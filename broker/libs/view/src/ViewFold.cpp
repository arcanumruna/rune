// libs/view/src/ViewFold.cpp
//
// Task 10: the single-source fold -- the heart of the Rune View Engine.
// Walks a committed prefix of a source topic's LogReadView batch-by-batch,
// decodes each record's codec-encoded VALUE against the WRITER's own schema
// version (never the latest), and applies Phase 1 Replace semantics into the
// view's ViewTable: upsert on a live record, erase on a tombstone. A source
// field the writer's version has dropped (or not yet added) decodes absent
// and writeSlot() leaves that output column NULL -- never garbage, never a
// stale carry-over from a previous row at that address.
//
// MERGE-KEY MODEL (post-review ruling): the table's identity key is the
// record's WIRE key (forEachRecord's `key` arg) -- the SAME bytes the log's
// own compaction (LogCleaner/OffsetMap) already keys "one current row per
// key" on. It is NOT the value's codec key-field. Those are two different
// key spaces that would only agree under an unenforced producer contract;
// if they ever diverged, an upsert keyed one way could never be erased by a
// tombstone keyed the other way (a permanently leaked row), or a tombstone
// could silently erase an unrelated row. Keying strictly on the wire/
// compaction key matches the log's own authority on "current row per key"
// and is the only key space that still exists for a tombstone (whose VALUE
// is empty by definition -- no codec payload to read a key-field from). The
// value's key-field is still decoded and written on upsert like any other
// CompiledField -- it is a display/projection output column, never consulted
// for merge identity.
//
// Hot path (sec14.0, <5us/record budget -- Task 11 benchmarks it): zero heap
// allocation in steady state. The only allocation is VersionDescriptorCache's
// first sight of a new writer version -- rare, not steady-state. No
// std::vector/std::function/std::string is built per record.
#include <rune/view/ViewFold.h>

#include <algorithm>
#include <cstring>

#include <rune/codec/Record.h>
#include <rune/protocol/RecordBatch.h>

namespace rune::view {

namespace {

using rune::codec::Descriptor;
using rune::codec::FieldValue;
using rune::codec::PrimType;

inline std::span<const uint8_t> asBytes(std::string_view sv) noexcept {
    return std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(sv.data()), sv.size());
}

// The view's key column's PrimType, resolved once per foldBatch() call (NOT
// per record): the CompiledField whose srcFieldId matches the per-source key
// field id. The compiler guarantees the key appears in the output
// (compileView step 3), so this is always found for a well-formed plan.
PrimType resolveKeyType(const CompiledViewPlan& plan) noexcept {
    const uint16_t keyFieldId = plan.keyFieldId[0];
    for (const auto& f : plan.output) {
        if (f.srcFieldId == keyFieldId) {
            return f.type;
        }
    }
    return PrimType::Int64; // unreachable for a compiler-produced plan
}

// Decodes the record's WIRE key -- the merge/compaction identity, used for
// EVERY record (upsert and tombstone alike; see the file-level doc above) --
// against `keyType`'s exact byte width. STRICT width match: a wire key that
// is not exactly `keyType`'s width is treated as malformed and rejected
// (returns false) rather than silently zero-padded/truncated, which could
// otherwise silently mis-key a row. A correctly-produced view-source record
// always has an exactly-right-width key. Signed types bit-cast through
// their two's-complement representation; unsigned types zero-extend.
bool decodeWireKey(std::string_view key, PrimType keyType, uint64_t& outKey) noexcept {
    size_t width;
    switch (keyType) {
        case PrimType::Int32:
        case PrimType::UInt32: width = 4; break;
        case PrimType::Int64:
        case PrimType::UInt64: width = 8; break;
        case PrimType::UInt16: width = 2; break;
        default: return false; // unreachable: Phase 1 keys are integer types
    }
    if (key.size() != width) {
        return false; // malformed: wrong-width wire key -- never zero-pad
    }

    uint64_t raw = 0;
    for (size_t i = 0; i < width; ++i) {
        raw |= static_cast<uint64_t>(static_cast<uint8_t>(key[i])) << (8 * i);
    }

    switch (keyType) {
        case PrimType::Int32: {
            const uint32_t u = static_cast<uint32_t>(raw);
            int32_t s;
            std::memcpy(&s, &u, sizeof s);
            outKey = static_cast<uint64_t>(static_cast<int64_t>(s));
            return true;
        }
        case PrimType::Int64: {
            int64_t s;
            std::memcpy(&s, &raw, sizeof s);
            outKey = static_cast<uint64_t>(s);
            return true;
        }
        default: // UInt16/UInt32/UInt64
            outKey = raw;
            return true;
    }
}

// Resolves the writer version of `value` and returns its materialized descriptor.
inline const Descriptor& writerOf(VersionDescriptorCache& vdc, std::string_view value) {
    const uint16_t writerVersion =
        value.size() >= 4
            ? static_cast<uint16_t>(static_cast<uint8_t>(value[2]) | (static_cast<uint8_t>(value[3]) << 8))
            : vdc.defaultVersion();
    return vdc.materializedFor(writerVersion);
}

// The key's own output column is decoded from the value and written like any other field -- it is a
// display/projection column, not the merge identity.
inline void writeProjectionRow(const CompiledViewPlan& plan, const RowLayout& layout, const Descriptor& d,
                               std::string_view value, uint8_t* row) noexcept {
    for (const auto& f : plan.output) {
        const FieldValue in = readField(d, asBytes(value), f.srcFieldId);
        writeSlot(row, layout, f.outSlot, in);
    }
}

} // namespace

PrepareStatus prepareProjection(ProjectionFold& f, const SourceRecord& rec, Prospect& p) noexcept {
    p.clear();
    uint64_t k = 0;
    if (!decodeWireKey(rec.key, f.keyType, k)) {
        p.stats.skippedMalformed = 1;
        return PrepareStatus::Malformed;
    }
    ProspectRow& r = p.rows[0];
    r.key     = ViewKey::fromU64(k);
    p.rowCount = 1;
    const uint8_t* cur = f.table.get(k);
    r.existed = cur != nullptr;
    if (rec.value.empty()) {
        p.op      = ProspectOp::Erase;
        r.removed = true;
        p.stats.deleted = 1;
        return PrepareStatus::Ok;
    }
    p.op    = ProspectOp::Upsert;
    p.stats.applied = 1;
    r.image = p.scratch.data();
    if (cur != nullptr) std::memcpy(r.image, cur, p.rowLen); else std::memset(r.image, 0, p.rowLen);
    const Descriptor& d = rec.writer != nullptr ? *rec.writer : writerOf(f.vdc, rec.value);
    writeProjectionRow(f.plan, f.layout, d, rec.value, r.image);
    return PrepareStatus::Ok;
}

void applyProjection(ProjectionFold& f, const SourceRecord&, const Prospect& p) noexcept {
    if (p.op == ProspectOp::None) return;
    const uint64_t k = p.rows[0].key.asU64();
    if (p.op == ProspectOp::Erase) f.table.erase(k);
    else std::memcpy(f.table.put(k), p.rows[0].image, p.rowLen);
}

FoldStats foldBatch(const CompiledViewPlan& plan, const RowLayout& layout,
                     VersionDescriptorCache& vdc, ViewTable& table,
                     const rune::storage::LogReadView& view, int64_t bound,
                     int64_t& cursor, const FoldSkip& skip) {
    FoldStats stats{};
    const PrimType keyType = resolveKeyType(plan);
    // Resume point: a LogReadView is WHOLE batches, so when the caller's cursor
    // sits mid-batch (a capped pass stopped there) the batch's records below it
    // were already folded and must not be re-applied.
    const FoldSkip foldSkip = skip;   // #537: by value (see AbortedTxnFilter::skips)
    const int64_t from = cursor;
    bool stopped = false;
    bool corrupt = false;

    // Highest offset this call has FULLY folded so far (every record of
    // every batch up to and including this point was folded or validly
    // skipped as control). Starts at view.baseOffset (nothing folded yet).
    // LogReadView's contract is WHOLE batches only, so this only ever
    // advances to a batch boundary -- never mid-batch.
    int64_t walkedUpto = view.baseOffset;

    size_t pos = 0;
    while (pos < view.len) {
        rune::protocol::RecordBatchReader r(view.data + pos, view.len - pos);
        if (!r.ensureValid()) {
            corrupt = true; // torn/corrupt tail: stop, do not advance past it
            break;
        }
        const int16_t attrs = r.attributes();
        if ((attrs & rune::protocol::batch::kAttrControl) != 0) {
            ++stats.skippedControl;
            pos += static_cast<size_t>(r.totalLength());
            walkedUpto = r.baseOffset() + r.lastOffsetDelta() + 1;
            continue;
        }
        if (isAbortedBatch(foldSkip, r, attrs)) [[unlikely]] {
            // #537: an aborted transaction's batch is never applied.  Below the
            // bound it is consumed whole, like a control batch; a batch the
            // bound cuts (a capped pass) stops the walk exactly as a record
            // past the bound does — the next pass resumes and skips it again.
            if (r.baseOffset() + r.lastOffsetDelta() >= bound) { stopped = true; break; }
            ++stats.skippedAborted;
            pos += static_cast<size_t>(r.totalLength());
            walkedUpto = r.baseOffset() + r.lastOffsetDelta() + 1;
            continue;
        }

        r.forEachRecord([&](std::string_view key, std::string_view value,
                             int64_t offset, int64_t /*timestamp*/) {
            if (stopped) {
                return; // already hit the uncommitted boundary this call
            }
            if (offset >= bound) {
                stopped = true; // don't fold uncommitted records
                return;
            }
            if (offset < from) {
                return; // already folded by a previous (mid-batch-stopped) pass
            }
            if (isRejectedRecord(foldSkip, offset)) [[unlikely]] {
                ++stats.skippedRejected;   // the judge rejected it: no view ever applies it
                return;
            }

            uint64_t k = 0;
            if (!decodeWireKey(key, keyType, k)) {
                ++stats.skippedMalformed;
                return;
            }

            if (value.empty()) {
                // Tombstone: erase by the same wire/compaction key.
                table.erase(k);
                ++stats.deleted;
                return;
            }

            const Descriptor& d = writerOf(vdc, value);
            writeProjectionRow(plan, layout, d, value, table.put(k));
            ++stats.applied;
        });

        if (stopped) {
            break; // bound reached mid-batch: this batch was NOT fully folded
        }
        pos += static_cast<size_t>(r.totalLength());
        walkedUpto = r.baseOffset() + r.lastOffsetDelta() + 1; // batch fully folded
    }

    // On a corrupt/torn break, never advance past the last fully-folded
    // batch -- view.nextOffset describes the WHOLE view (including bytes we
    // never validated), so using it here could skip records this call never
    // folded. Otherwise (normal completion or a bound-stop), the walk either
    // consumed the whole view or stopped exactly at a committed boundary, so
    // the existing min(nextOffset, bound) cap is correct and tighter than
    // walkedUpto would be when bound < nextOffset.
    // Never move the resume point backwards (a corrupt batch at/after `from`).
    cursor = std::max(from, corrupt ? walkedUpto : std::min(view.nextOffset, bound));
    return stats;
}

} // namespace rune::view
