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
#include <rune/view/TxnSupersede.h>

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
    return {const_cast<void*>(static_cast<const void*>(&readRange)), [](void* c, int64_t from, int64_t to) { return (*static_cast<ReadRangeFn*>(c))(from, to); }};
}

/// A verdict source's decision point (D2): a transaction with a TxnJudge is applied at it, not at its marker.  The source decides each
/// non-transactional control batch: None (not a TxnJudge, or nothing to do), Apply (the span of `pid`, from `first` too when it is
/// not -1: a checkpoint inside the span holds none of its records), Drop (another path applies it: the span closes) or Wait (the fold
/// stops before the batch until the transaction is decided).  Called only for control batches (cold).
enum class JudgeStep : uint8_t { None, Apply, Drop, Wait };
struct JudgeDecision {
    JudgeStep step  = JudgeStep::None;
    int64_t   pid   = -1;
    int64_t   first = -1;
};
struct JudgeHook {
    void* ctx = nullptr;
    JudgeDecision (*fn)(void* ctx, const uint8_t* batch, std::size_t len, int64_t at) = nullptr;
};

/// One fold call's transactional state: the instance's fence, the worker's supersede keys, its reader, and the call's budget in units (offsets the walk covers
/// plus records a marker application applies; 0 = no cap).  A fold without one (a transient source, D6) applies transactional
/// batches at their own offsets.
struct TxnFold {
    TxnSpanFence&  fence;
    SupersedeKeys& keys;
    SpanReader     reader;
    int64_t        budget = 0;
    JudgeHook      judge{};   // a verdict source's replay; path A leaves it empty and applies at the marker
};

/// The bytes one call may read back for a marker application (as kVoteScanBytesPerPass); the rest continues next call.
inline constexpr std::size_t kTxnReadBackBytesPerPass = std::size_t{1} << 20;

namespace detail {

enum class WalkEnd : uint8_t { Done, Corrupt, Marker, Wait };
struct WalkOut {
    WalkEnd end;
    int64_t walkedUpto;   // every batch below it was folded, skipped or held in a span
    int64_t pid = -1;     // Marker: the producer
    int64_t at  = -1;     // Marker: the marker's offset
    bool judged = false;  // Marker: a TxnJudge, applied with no supersede notes (D2: P11-3 voted no on any superseder)
};

enum class TxnStep : uint8_t { Skip, Apply, Stop, Marker, Judged, Wait };

// A control or transactional batch.  Control: a commit marker of an open span hands the walk back to apply it, an abort marker
// closes it, anything else is skipped.  Data: an aborted transaction's batch is skipped; with a fence, every other one opens (or
// extends) its producer's span and is skipped; without one, or with a full one, it is applied here (offset order).
inline TxnStep txnStep(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, int16_t attrs, int64_t last, int64_t bound,
                       const FoldSkip& skip, TxnFold* txn, FoldStats& stats, int64_t& pid) noexcept {
    namespace b = rune::protocol::batch;
    namespace cm = rune::storage::control_marker;
    TxnSpanFence* fence = txn != nullptr ? &txn->fence : nullptr;
    const int64_t base = r.baseOffset();
    if ((attrs & b::kAttrControl) != 0) {
        if (fence != nullptr && (attrs & b::kAttrTransactional) != 0 && base < bound && fence->count != 0 && fence->find(r.producerId()) != nullptr) {
            const int type = cm::controlType(batch, static_cast<std::size_t>(r.totalLength()));
            if (type == cm::kCommit) {
                pid = r.producerId();
                return TxnStep::Marker;
            }
            if (type == cm::kAbort) fence->close(r.producerId());
        } else if (fence != nullptr && txn->judge.fn != nullptr && (attrs & b::kAttrTransactional) == 0 && base < bound) {
            const JudgeDecision d = txn->judge.fn(txn->judge.ctx, batch, static_cast<std::size_t>(r.totalLength()), base);
            if (d.step == JudgeStep::Wait) return TxnStep::Wait;
            if (d.step == JudgeStep::Drop) fence->close(d.pid);
            if (d.step == JudgeStep::Apply) {
                TxnSpan* open = fence->find(d.pid);
                if (open != nullptr && d.first >= 0) open->first = std::min(open->first, d.first);
                if (open != nullptr || (d.first >= 0 && fence->add(d.pid, d.first))) {
                    pid = d.pid;
                    return TxnStep::Judged;
                }
            }
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
WalkOut walkView(const rune::storage::LogReadView& view, int64_t from, int64_t bound, const FoldSkip& skip, TxnFold* txn,
                 FoldStats& stats, OnRecord& onRecord) {
    namespace b = rune::protocol::batch;
    TxnSpanFence* fence = txn != nullptr ? &txn->fence : nullptr;
    int64_t walkedUpto = view.baseOffset;
    std::size_t pos = 0;
    while (pos < view.len) {
        rune::protocol::RecordBatchReader r(view.data + pos, view.len - pos);
        if (!r.ensureValid()) return {WalkEnd::Corrupt, walkedUpto};   // torn/corrupt tail: stop, do not advance past it
        const int16_t attrs = r.attributes();
        const int64_t last  = r.baseOffset() + r.lastOffsetDelta();
        if ((attrs & (b::kAttrControl | b::kAttrTransactional)) != 0) [[unlikely]] {
            int64_t pid = -1;
            switch (txnStep(r, view.data + pos, attrs, last, bound, skip, txn, stats, pid)) {
                case TxnStep::Skip:
                    pos += static_cast<std::size_t>(r.totalLength());
                    walkedUpto = last + 1;
                    continue;
                case TxnStep::Stop:   return {WalkEnd::Done, walkedUpto};
                case TxnStep::Marker: return {WalkEnd::Marker, walkedUpto, pid, r.baseOffset()};
                case TxnStep::Judged: return {WalkEnd::Marker, walkedUpto, pid, r.baseOffset(), true};
                case TxnStep::Wait:   return {WalkEnd::Wait, walkedUpto};
                case TxnStep::Apply:  break;
            }
        }
        bool stopped = false;
        r.forEachRecord([&](std::string_view key, std::string_view value, int64_t offset, int64_t /*timestamp*/, uint8_t attributes) {
            if (stopped) return;
            if (offset >= bound) { stopped = true; return; }   // never fold past the bound
            if (offset < from) return;                         // folded by a previous (mid-batch-stopped) pass
            if (isRejectedRecord(skip, offset)) [[unlikely]] {
                ++stats.skippedRejected;   // the judge rejected it: no view ever applies it
                return;
            }
            // Lease topics (spec 6.6): a Checkpoint is invisible, a Revoke/Expire folds as an empty value (a delete).
            std::string_view v = value;
            if (!leaseDataView(skip.traits, attributes, v)) [[unlikely]] { ++stats.skippedControl; return; }
            onRecord(key, v, offset);
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

// A read-back below the marker.  A disk read stops at the next segment base at or above `to`, so an empty answer is read again to
// just past the marker, which holds at least the marker's batch: empty again is a failed read (an I/O error, a damaged segment,
// retention below the span), never "nothing left".
inline rune::storage::LogReadView readSpan(const TxnFold& txn, int64_t at, int64_t to, int64_t marker) {
    rune::storage::LogReadView v = txn.reader.read(at, to);
    if (v.empty()) v = txn.reader.read(at, marker + 1);
    return v;
}

enum class NoteEnd : uint8_t { Ready, Paused, Failed };

// The supersede notes of the segment of `pid`'s span from `at` (P11-3a), in the worker's key set: T's keys up to the first that does
// not fit, each with last_T over the rest of the span, and per key the last offset below the marker of another writer's record
// applied before it - plain and not rejected, or of a transaction not open in the fence at its offset (it closed below the marker; an
// aborted one is skipped by the partition's aborted index, which every fold path is handed).
//
// The note read reaches the marker, so it shares the call's byte budget and resumes where it stopped (SupersedeKeys' progress).  One
// fence holds the set at a time: another waits (Paused) while the holder keeps using it, and takes it once a whole call of its own
// saw no use (the holder is not driven: stalled, demoted, dropped).  A taken set is noted again from the holder's resume point.
inline NoteEnd noteSegment(TxnFold& txn, int64_t pid, int64_t at, int64_t marker, const FoldSkip& skip, std::size_t& bytes, FoldStats& stats) {
    namespace b = rune::protocol::batch;
    SupersedeKeys& k = txn.keys;
    TxnSpanFence& fence = txn.fence;
    const bool mine = fence.noteToken != 0 && k.owner == fence.noteToken;
    if (mine && k.pid == pid && k.marker == marker && at >= k.from && (!k.noted || at < k.segEnd)) {
        ++k.touches;
    } else {
        if (!mine && k.owner != 0 && fence.seenTouches != k.touches) {
            fence.seenTouches = k.touches;
            ++stats.txnNoteWaits;
            return NoteEnd::Paused;
        }
        fence.noteToken = k.claim(pid, marker, at);
        fence.seenTouches = SupersedeKeys::kNoTouches;
    }
    while (!k.noted) {
        if (bytes >= kTxnReadBackBytesPerPass) return NoteEnd::Paused;
        const rune::storage::LogReadView v = readSpan(txn, k.readTo, marker, marker);
        if (v.empty()) return NoteEnd::Failed;
        const int64_t start = k.readTo;
        for (std::size_t pos = 0; pos < v.len && !k.noted;) {
            rune::protocol::RecordBatchReader r(v.data + pos, v.len - pos);
            if (!r.ensureValid()) return NoteEnd::Failed;
            if (r.baseOffset() >= marker) {
                k.noted = true;
                break;
            }
            const auto len = static_cast<std::size_t>(r.totalLength());
            pos += len;
            const int16_t attrs = r.attributes();
            const int64_t last  = r.baseOffset() + r.lastOffsetDelta();
            if (last < k.readTo) continue;
            k.readTo = last + 1;
            k.noted = k.readTo >= marker;
            bytes += len;
            if ((attrs & b::kAttrControl) != 0) {
            } else if ((attrs & b::kAttrTransactional) != 0 && r.producerId() == pid) {
                r.forEachRecord([&](std::string_view key, std::string_view, int64_t offset, int64_t) {
                    if (offset < k.from) return;
                    if (!k.noteT(key, offset, k.segEnd == marker)) k.segEnd = offset;
                    k.lastT = offset;
                });
            } else if (!isAbortedBatch(skip, r, attrs)) {
                const TxnSpan* open = (attrs & b::kAttrTransactional) != 0 ? fence.find(r.producerId()) : nullptr;
                if (open == nullptr || open->first > r.baseOffset())   // an open one applies at its own marker, above this one
                    r.forEachRecord([&](std::string_view key, std::string_view, int64_t offset, int64_t) {
                        if (offset >= k.from && offset < marker && !isRejectedRecord(skip, offset)) k.noteOther(key, offset);
                    });
            }
            if (!k.noted && bytes >= kTxnReadBackBytesPerPass) return NoteEnd::Paused;
        }
        if (!k.noted && k.readTo == start) return NoteEnd::Failed;
    }
    return NoteEnd::Ready;
}

// Applies the span of `pid` (open in the fence) at its marker: its batches in [first, marker), resuming above applyingThrough, a
// segment of noted keys at a time, a record another writer superseded skipped.  The last segment ends after T's last record, not
// at the marker.  At a TxnJudge (`judged`) the span is one segment with no notes, up to the judge.  At most `left` records, and `bytes` (the call's read-back, notes included) up to kTxnReadBackBytesPerPass; false
// when either ran out first, the key set is another span's, or a read failed (applyingThrough says where it stopped once a record
// is applied).
template <class OnRecord>
bool applySpan(TxnFold& txn, int64_t pid, int64_t marker, int64_t left, const FoldSkip& skip, FoldStats& stats, OnRecord& onRecord,
               int64_t& applied, std::size_t& bytes, bool judged) {
    namespace b = rune::protocol::batch;
    TxnSpanFence& fence = txn.fence;
    int64_t at = fence.applyingThrough >= 0 ? fence.applyingThrough + 1 : fence.find(pid)->first;
    const int64_t spanFirst = fence.find(pid)->first;
    // A failed read stops the application where it stands: the span stays open, applied records stay fenced, and the next call
    // retries.  It never closes the span, which would let a checkpoint pass records never applied.  The key set is given up, so a
    // read that keeps failing never holds another instance's marker; the retry notes again from where it stands.
    const auto paused = [&] {
        if (at > spanFirst) fence.applyingThrough = at - 1;
        return false;
    };
    const auto release = [&] {
        if (fence.noteToken != 0 && txn.keys.owner == fence.noteToken) txn.keys.release();
        fence.noteToken = 0;
    };
    const auto failed = [&] {
        ++stats.txnReadFailed;
        release();
        return paused();
    };
    const auto done = [&] {
        release();
        return true;
    };
    while (at < marker) {
        const NoteEnd n = judged ? NoteEnd::Ready : noteSegment(txn, pid, at, marker, skip, bytes, stats);
        if (n == NoteEnd::Failed) return failed();
        if (n == NoteEnd::Paused) return paused();
        const int64_t segEnd = judged ? marker : txn.keys.segEnd;
        const int64_t end = judged || segEnd != marker ? segEnd : std::min(marker, txn.keys.lastT + 1);
        while (at < end) {
            const rune::storage::LogReadView v = readSpan(txn, at, end, marker);
            if (v.empty()) return failed();
            const int64_t start = at;
            for (std::size_t pos = 0; pos < v.len && at < end;) {
                rune::protocol::RecordBatchReader r(v.data + pos, v.len - pos);
                if (!r.ensureValid()) return failed();
                if (r.baseOffset() >= marker) return done();
                const auto len = static_cast<std::size_t>(r.totalLength());
                const int64_t last = r.baseOffset() + r.lastOffsetDelta();
                pos += len;
                if (last < at) continue;
                bytes += len;
                // An aborted batch is skipped: a span opened at a TxnJudge from its firstOffset (JudgeHook) was never filtered by the walk.
                if (r.producerId() == pid && (r.attributes() & (b::kAttrControl | b::kAttrTransactional)) == b::kAttrTransactional &&
                    !isAbortedBatch(skip, r, r.attributes())) {
                    enum class Out : uint8_t { No, Segment, Budget } out = Out::No;
                    r.forEachRecord([&](std::string_view key, std::string_view value, int64_t offset, int64_t) {
                        if (out != Out::No || offset < at) return;
                        if (offset >= end) {
                            out = Out::Segment;
                            return;
                        }
                        if (applied == left) {
                            out = Out::Budget;
                            fence.applyingThrough = offset - 1;
                            return;
                        }
                        if (isRejectedRecord(skip, offset)) {
                            ++stats.skippedRejected;
                            return;
                        }
                        if (!judged && txn.keys.superseded(key)) {
                            ++stats.txnSuperseded;
                            return;
                        }
                        onRecord(key, value, offset);
                        ++applied;
                    });
                    if (out == Out::Budget) return false;
                    if (out == Out::Segment) {   // the segment ends inside this batch
                        at = end;
                        break;
                    }
                }
                at = last + 1;
                if (at < end && bytes >= kTxnReadBackBytesPerPass) {
                    fence.applyingThrough = at - 1;
                    return false;
                }
            }
            if (at == start) return failed();
        }
        at = std::max(at, segEnd);
    }
    return done();
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
    std::size_t bytes = 0;   // read back at markers this call
    rune::storage::LogReadView view = first;
    for (;;) {
        const detail::WalkOut w = detail::walkView(view, from, bound, skip, txn, stats, onRecord);
        if (w.end == detail::WalkEnd::Done) {
            cursor = std::max(from, std::min(view.nextOffset, bound));
            break;
        }
        if (w.end == detail::WalkEnd::Corrupt || w.end == detail::WalkEnd::Wait) {
            // Never advance past the last fully-walked batch (view.nextOffset describes bytes never validated), nor move back.
            cursor = std::max(from, w.walkedUpto);
            break;
        }
        cursor = std::max(from, w.at);
        const int64_t left = txn->budget > 0 ? txn->budget - (w.at - start) - units : INT64_MAX;
        if (left <= 0) break;   // the marker is applied next call, with that call's whole budget
        int64_t applied = 0;
        const bool done = detail::applySpan(*txn, w.pid, w.at, left, skip, stats, onRecord, applied, bytes, w.judged);
        units += applied;
        if (!done) break;
        fence->close(w.pid);
        fence->applyingThrough = -1;
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
