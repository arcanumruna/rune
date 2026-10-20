// libs/view/include/rune/view/ViewFold.h
//
// Task 10: the single-source fold -- the heart of the Rune View Engine.
#pragma once
#include <cstdint>
#include <unordered_map>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewTable.h>

namespace rune::view {

// Per-writer-version materialized Descriptor cache for the fold hot path. A
// writer-version change is rare (a schema ALTER); the common case is a long
// run of records all written against the SAME version, so lookups must be
// steady-state-alloc-free. Only the FIRST sight of a given writerVersion
// pays for materializeAsOf() + an unordered_map node allocation; every later
// lookup for that version is a hash + compare, no allocation.
class VersionDescriptorCache {
public:
    explicit VersionDescriptorCache(rune::codec::Descriptor evolving)
        : evolving_(std::move(evolving)) {}

    const rune::codec::Descriptor& materializedFor(uint16_t writerVersion) {
        auto it = cache_.find(writerVersion);
        if (it != cache_.end()) {
            return it->second;
        }
        auto [ins, inserted] = cache_.emplace(
            writerVersion, rune::codec::materializeAsOf(evolving_, writerVersion));
        (void)inserted;
        return ins->second;
    }

    // Fallback writer version used only when a non-tombstone record's value
    // is too short to carry a [blockLength][version] header (defensive path;
    // never hit by a well-formed codec record). Defaults to the evolving
    // descriptor's own (latest) declared version.
    uint16_t defaultVersion() const noexcept { return evolving_.version; }

private:
    rune::codec::Descriptor evolving_;
    std::unordered_map<uint16_t, rune::codec::Descriptor> cache_;
};

struct FoldStats {
    uint64_t applied          = 0;
    uint64_t deleted          = 0;
    uint64_t skippedControl   = 0;
    // A record whose WIRE key is not exactly the view key column's byte
    // width (e.g. a truncated/corrupt producer-side key). Never silently
    // zero-padded -- counted and skipped instead, so a malformed key is
    // observable rather than silently mis-keying a row.
    uint64_t skippedMalformed = 0;
    // Phase 2a aggregate views: a live record whose group-by value is absent
    // (its old contribution, if any, is retracted; nothing is added).
    uint64_t skippedNoGroup   = 0;
    // Output values written NULL because a Sum did not fit its type (never
    // wrapped). Counted per emit, not per overflow event.
    uint64_t aggOverflow      = 0;
    uint64_t aggNonFinite     = 0;  // a NaN/+-Inf Float aggregate input was treated as NULL (counted per Float input)
    // A retraction found missing/zero state (group row absent or a count already
    // 0): counted and skipped with no mutation, never masked.
    uint64_t aggInconsistent  = 0;
    // Phase 2b: Min/Max caches rebuilt by one walk of the group's contributor
    // list (the cached extreme was retracted) -- the known O(group size) path.
    uint64_t aggRescans       = 0;
};

// Folds every COMMITTED record (offset < bound) in `view` -- a LogReadView
// over whole batches -- into `table`, per `plan`/`layout`. Control batches
// are skipped (counted in skippedControl). Merge-key model: the row's
// identity (table `put`/`erase` key) is the record's WIRE/compaction key --
// the same bytes the log's own compaction (OffsetMap) already keys "one
// current row per key" on -- NEVER the value's key-field. The value's
// key-field is a plain display/projection output column, decoded and
// written like any other CompiledField on upsert, and is NOT consulted for
// merge identity. A live (non-tombstone) record is decoded against the
// writer's OWN schema version (never the latest) and upserted (Phase 1: all
// CompiledFields are Replace); a tombstone (empty value) erases its wire key.
// `cursor` is IN/OUT: on entry, records with offset < cursor are skipped
// (already folded -- `view` is whole batches, so a pass that stopped at a
// mid-batch `bound` resumes inside that batch); on exit it is the offset the
// caller should resume from next time: the highest offset actually folded,
// capped at `bound` -- NEVER past a batch this call did not fully fold (see
// ViewFold.cpp for the early-break case). foldAggregateBatch: same contract.
FoldStats foldBatch(const CompiledViewPlan& plan, const RowLayout& layout,
                     VersionDescriptorCache& vdc, ViewTable& table,
                     const rune::storage::LogReadView& view, int64_t bound,
                     int64_t& cursor);

} // namespace rune::view
