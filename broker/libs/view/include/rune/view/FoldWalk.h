// libs/view/include/rune/view/FoldWalk.h
//
// The batch walk every fold shares (projection, aggregate, keyed, the key index).  P11-0 (R11b): a fold applies a committed
// transaction's records at its commit marker, in offset order among themselves, and drops an aborted one's.  A transactional batch
// opens a span in the instance's TxnSpanFence and is skipped; the pid's commit marker reads the span back through a SpanReader and
// applies it; its abort marker closes it.  P11-9: an application larger than the call's budget, or an atomic batch the pass cap
// cuts, continues next call with the fence raised (reads held, no checkpoint).
//
// A full fence cannot wait: no span closes while the fold stands below every open marker.  A transactional batch that finds no free
// slot applies at its own offsets instead (counted txnSpanTableFull).  The set of spans open at an offset is a function of the log
// alone (a span is open from its first batch to its marker, and a checkpoint never straddles one), so every fold path makes the
// same choice there.
//
// Hot path: a non-transactional data batch costs the one attribute test that already skipped control batches.  The reader is called
// only at a marker of an open span (cold); the fence is scanned only for a transactional or control batch.
#pragma once
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string_view>

#include <rune/protocol/RecordBatch.h>
#include <rune/storage/ControlMarkerLayout.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/FoldSkip.h>
#include <rune/view/FoldStats.h>
#include <rune/view/TxnSpanFence.h>

namespace rune::view {

/// Reads whole batches from `from` (at most to `to`, a hint the source may ignore) for a span read-back.  A function pointer, not a
/// template: the fold lives in rune_view and is called only at markers.
struct SpanReader {
    void* ctx = nullptr;
    rune::storage::LogReadView (*fn)(void* ctx, int64_t from, int64_t to) = nullptr;
    [[nodiscard]] rune::storage::LogReadView read(int64_t from, int64_t to) const { return fn(ctx, from, to); }
};
template <class ReadRangeFn>
[[nodiscard]] SpanReader spanReaderOf(ReadRangeFn& readRange) noexcept {
    return {&readRange, [](void* c, int64_t from, int64_t to) { return (*static_cast<ReadRangeFn*>(c))(from, to); }};
}

/// One fold call's transactional state: the instance's fence, its reader, and the call's budget in units (offsets the walk covers
/// plus records a marker application applies; 0 = no cap).  A fold without one (a transient source, D6) applies transactional
/// batches at their own offsets.
struct TxnFold {
    TxnSpanFence& fence;
    SpanReader    reader;
    int64_t       budget = 0;
};

/// The bytes one call may read back for a marker application (as kVoteScanBytesPerPass); the rest continues next call.
inline constexpr std::size_t kTxnReadBackBytesPerPass = std::size_t{1} << 20;

namespace detail {

enum class WalkEnd : uint8_t { Done, Corrupt, Marker };
struct WalkOut {
    WalkEnd end;
    int64_t walkedUpto;   // every batch below it was folded, skipped or held in a span
    int64_t pid = -1;     // Marker: the producer
    int64_t at  = -1;     // Marker: the marker's offset
};

enum class TxnStep : uint8_t { Skip, Apply, Stop, Marker };

// A control or transactional batch.  Control: a commit marker of an open span hands the walk back to apply it, an abort marker
// closes it, anything else is skipped.  Data: an aborted transaction's batch is skipped; with a fence, every other one opens (or
// extends) its producer's span and is skipped; without one, or with a full one, it is applied here (offset order).
inline TxnStep txnStep(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, int16_t attrs, int64_t last, int64_t bound,
                       const FoldSkip& skip, TxnSpanFence* fence, FoldStats& stats) noexcept {
    namespace b = rune::protocol::batch;
    namespace cm = rune::storage::control_marker;
    const int64_t base = r.baseOffset();
    if ((attrs & b::kAttrControl) != 0) {
        if (fence != nullptr && (attrs & b::kAttrTransactional) != 0 && base < bound && fence->count != 0 && fence->find(r.producerId()) != nullptr) {
            const int type = cm::controlType(batch, static_cast<std::size_t>(r.totalLength()));
            if (type == cm::kCommit) return TxnStep::Marker;
            if (type == cm::kAbort) fence->close(r.producerId());
        }
        ++stats.skippedControl;
        return TxnStep::Skip;
    }
    if (isAbortedBatch(skip, r, attrs)) {
        // #537: never applied.  A batch the bound cuts stops the walk; the next pass resumes and skips it again.
        if (last >= bound) return TxnStep::Stop;
        ++stats.skippedAborted;
        return TxnStep::Skip;
    }
    if (fence == nullptr) return TxnStep::Apply;
    if (base >= bound) return TxnStep::Stop;
    if (fence->find(r.producerId()) == nullptr && !fence->add(r.producerId(), base)) {
        ++stats.txnSpanTableFull;
        return TxnStep::Apply;
    }
    return last >= bound ? TxnStep::Stop : TxnStep::Skip;   // a cut batch: the cursor stops inside the span, which vetoes a checkpoint
}

template <class OnRecord>
WalkOut walkView(const rune::storage::LogReadView& view, int64_t from, int64_t bound, const FoldSkip& skip, TxnSpanFence* fence,
                 FoldStats& stats, OnRecord& onRecord) {
    namespace b = rune::protocol::batch;
    int64_t walkedUpto = view.baseOffset;
    std::size_t pos = 0;
    while (pos < view.len) {
        rune::protocol::RecordBatchReader r(view.data + pos, view.len - pos);
        if (!r.ensureValid()) return {WalkEnd::Corrupt, walkedUpto};   // torn/corrupt tail: stop, do not advance past it
        const int16_t attrs = r.attributes();
        const int64_t last  = r.baseOffset() + r.lastOffsetDelta();
        if ((attrs & (b::kAttrControl | b::kAttrTransactional)) != 0) [[unlikely]] {
            switch (txnStep(r, view.data + pos, attrs, last, bound, skip, fence, stats)) {
                case TxnStep::Skip:
                    pos += static_cast<std::size_t>(r.totalLength());
                    walkedUpto = last + 1;
                    continue;
                case TxnStep::Stop:   return {WalkEnd::Done, walkedUpto};
                case TxnStep::Marker: return {WalkEnd::Marker, walkedUpto, r.producerId(), r.baseOffset()};
                case TxnStep::Apply:  break;
            }
        }
        bool stopped = false;
        r.forEachRecord([&](std::string_view key, std::string_view value, int64_t offset, int64_t /*timestamp*/) {
            if (stopped) return;
            if (offset >= bound) { stopped = true; return; }   // never fold past the bound
            if (offset < from) return;                         // folded by a previous (mid-batch-stopped) pass
            if (isRejectedRecord(skip, offset)) [[unlikely]] {
                ++stats.skippedRejected;   // the judge rejected it: no view ever applies it
                return;
            }
            onRecord(key, value, offset);
        });
        if (stopped) {
            // P11-9: the pass cap cut an atomic batch; reads hold until the next pass applies the rest.
            if (fence != nullptr && (attrs & b::kAttrAtomic) != 0 && r.baseOffset() < bound) [[unlikely]] fence->atomicThrough = last;
            return {WalkEnd::Done, walkedUpto};
        }
        pos += static_cast<std::size_t>(r.totalLength());
        walkedUpto = last + 1;
    }
    return {WalkEnd::Done, walkedUpto};
}

// Applies the span of `pid` (open in the fence) at its marker: its batches in [first, marker), resuming above applyingThrough.  At
// most `left` records and kTxnReadBackBytesPerPass bytes; false when either ran out first (applyingThrough says where it stopped).
template <class OnRecord>
bool applySpan(TxnFold& txn, int64_t pid, int64_t marker, int64_t left, const FoldSkip& skip, FoldStats& stats, OnRecord& onRecord,
               int64_t& applied) {
    namespace b = rune::protocol::batch;
    TxnSpanFence& fence = txn.fence;
    int64_t at = fence.applyingThrough >= 0 ? fence.applyingThrough + 1 : fence.find(pid)->first;
    std::size_t bytes = 0;
    while (at < marker) {
        const rune::storage::LogReadView v = txn.reader.read(at, marker);
        if (v.empty()) return true;   // nothing left below the marker
        const int64_t start = at;
        for (std::size_t pos = 0; pos < v.len;) {
            rune::protocol::RecordBatchReader r(v.data + pos, v.len - pos);
            if (!r.ensureValid() || r.baseOffset() >= marker) return true;
            const auto len = static_cast<std::size_t>(r.totalLength());
            const int64_t last = r.baseOffset() + r.lastOffsetDelta();
            pos += len;
            if (last < at) continue;
            bytes += len;
            if (r.producerId() == pid && (r.attributes() & (b::kAttrControl | b::kAttrTransactional)) == b::kAttrTransactional) {
                bool out = false;
                r.forEachRecord([&](std::string_view key, std::string_view value, int64_t offset, int64_t) {
                    if (out || offset < at) return;
                    if (isRejectedRecord(skip, offset)) {
                        ++stats.skippedRejected;
                        return;
                    }
                    if (applied == left) {
                        out = true;
                        fence.applyingThrough = offset - 1;
                        return;
                    }
                    onRecord(key, value, offset);
                    ++applied;
                });
                if (out) return false;
            }
            at = last + 1;
            if (at < marker && bytes >= kTxnReadBackBytesPerPass) {
                fence.applyingThrough = at - 1;
                return false;
            }
        }
        if (at == start) return true;
    }
    return true;
}

} // namespace detail

/// The walk: `onRecord(key, value, offset)` sees every record in [cursor, bound) that is committed and not rejected, a transaction's
/// records at its commit marker.  `cursor` is IN/OUT exactly as foldBatch documents it; with `txn` it may also stop at a marker
/// whose application the budget cut (the fence then holds reads) or left for the next call.
template <class OnRecord>
void walkFold(const rune::storage::LogReadView& first, int64_t bound, int64_t& cursor, const FoldSkip& skipRef, TxnFold* txn,
              FoldStats& stats, OnRecord&& onRecord) {
    const FoldSkip skip = skipRef;   // #537: by value (see AbortedTxnFilter::skips)
    TxnSpanFence* fence = txn != nullptr ? &txn->fence : nullptr;
    const int64_t start = cursor;
    int64_t from = cursor;
    int64_t units = 0;   // records applied at markers this call
    rune::storage::LogReadView view = first;
    for (;;) {
        const detail::WalkOut w = detail::walkView(view, from, bound, skip, fence, stats, onRecord);
        if (w.end == detail::WalkEnd::Done) {
            cursor = std::max(from, std::min(view.nextOffset, bound));
            break;
        }
        if (w.end == detail::WalkEnd::Corrupt) {
            // Never advance past the last fully-walked batch (view.nextOffset describes bytes never validated), nor move back.
            cursor = std::max(from, w.walkedUpto);
            break;
        }
        cursor = std::max(from, w.at);
        const int64_t left = txn->budget > 0 ? txn->budget - (w.at - start) - units : INT64_MAX;
        if (left <= 0) break;   // the marker is applied next call, with that call's whole budget
        int64_t applied = 0;
        const bool done = detail::applySpan(*txn, w.pid, w.at, left, skip, stats, onRecord, applied);
        units += applied;
        if (!done) break;
        fence->close(w.pid);
        cursor = from = w.at + 1;
        if (txn->budget > 0) {
            const int64_t rest = txn->budget - (cursor - start) - units;
            if (rest <= 0) break;
            bound = std::min(bound, cursor + rest);
        }
        if (cursor >= bound) break;
        view = txn->reader.read(cursor, bound);
        if (view.empty()) break;
    }
    if (fence != nullptr && fence->atomicThrough >= 0 && cursor > fence->atomicThrough) fence->atomicThrough = -1;
}

} // namespace rune::view
