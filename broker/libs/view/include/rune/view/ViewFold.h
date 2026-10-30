// libs/view/include/rune/view/ViewFold.h
//
// Task 10: the single-source fold -- the heart of the Rune View Engine.
#pragma once
#include <cstdint>
#include <unordered_map>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/FoldSkip.h>
#include <rune/view/FoldStats.h>
#include <rune/view/RecordFold.h>
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

// Folds every COMMITTED record (offset < bound) in `view` -- a LogReadView
// over whole batches -- into `table`, per `plan`/`layout`. Control batches
// are skipped (counted in skippedControl), and so is every data batch of an
// aborted transaction (`skip.aborted`, #537; counted in skippedAborted). Merge-key model: the row's
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
                     int64_t& cursor, const FoldSkip& skip);

// A projection view's fold inputs, built once per ViewInstance.
struct ProjectionFold {
    const CompiledViewPlan&  plan;
    const RowLayout&         layout;
    VersionDescriptorCache&  vdc;
    ViewTable&               table;
    rune::codec::PrimType    keyType;
};

// The view key column's type, for building a ProjectionFold / AggregateFold.
[[nodiscard]] rune::codec::PrimType viewKeyType(const CompiledViewPlan& plan) noexcept;

// Computes the record's prospective row into `p` without touching the table.
[[nodiscard]] PrepareStatus prepareProjection(ProjectionFold& f, const SourceRecord& rec, Prospect& p) noexcept;
// Commits a prepared record.
void applyProjection(ProjectionFold& f, const SourceRecord& rec, const Prospect& p) noexcept;

} // namespace rune::view
