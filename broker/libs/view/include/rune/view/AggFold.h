// libs/view/include/rune/view/AggFold.h — Phase 2a aggregate fold.
#pragma once
#include <cstdint>

#include <rune/storage/LogReadView.h>
#include <rune/view/AggState.h>
#include <rune/view/KeyedFold.h>
#include <rune/view/ViewFold.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewTable.h>

namespace rune::view {

// Folds every committed record (offset < bound) of `lv` into an aggregate view
// with TABLE semantics (AggState.h): contributor = 8-byte wire key, group = the
// group-by value. Same batch walk, control-batch skip, aborted-transaction skip
// (#537), bound stop, and cursor contract as foldBatch (ViewFold.h). Zero heap allocation in steady state.
// A layout with Min/Max (agg.numBlockCols > 0) also maintains each group's
// value block in `blocks` (rows of agg.blockRow); `blocks` may be nullptr only
// when agg.numBlockCols == 0 -- with Min/Max and no table, every block step is
// counted in aggInconsistent and skipped (never a crash).
FoldStats foldAggregateBatch(const CompiledViewPlan& plan, const RowLayout& layout,
                             const AggLayout& agg, VersionDescriptorCache& vdc,
                             ViewTable& view, ViewTable& contributors, ViewTable& groups,
                             ViewTable* blocks, const rune::storage::LogReadView& lv,
                             int64_t bound, int64_t& cursor, const FoldSkip& skip, TxnFold* txn = nullptr);

// 2a and Last-only layouts (no value block): blocks = nullptr.
FoldStats foldAggregateBatch(const CompiledViewPlan& plan, const RowLayout& layout,
                             const AggLayout& agg, VersionDescriptorCache& vdc,
                             ViewTable& view, ViewTable& contributors, ViewTable& groups,
                             const rune::storage::LogReadView& lv, int64_t bound,
                             int64_t& cursor, const FoldSkip& skip, TxnFold* txn = nullptr);

// An aggregate view's fold inputs, built once per ViewInstance.  `blocks` is nullptr iff agg.numBlockCols == 0.
struct AggregateFold {
    const CompiledViewPlan&  plan;
    const RowLayout&         layout;
    const AggLayout&         agg;
    VersionDescriptorCache&  vdc;
    ViewTable&               view;
    ViewTable&               contributors;
    ViewTable&               groups;
    ViewTable*               blocks;
    // Keyed view (agg.keyed): the spec, the group key -> gid table (row [gid u64]) and the next unused gid (apply advances it).
    const KeyedFoldSpec*     keyed       = nullptr;
    ViewTable*               groupIds    = nullptr;
    uint64_t*                nextGroupId = nullptr;
    uint8_t                  member = 0;         // this view's id within the judge's set, for TxnOverlay keys
    const TxnLockSet*        locks  = nullptr;   // rows held by other transactions: a prepare that touches one is Locked
};

// Computes the record's prospective group rows into `p` (rows[0] the new group, rows[1] the old group of a Move)
// without touching a table; CapacityExhausted when the overlay's fixed slots run out.  With `chain` the reads go chain-first and the
// record's rows are written into the chain (a keyed view's new group burns the next gid); a Locked or CapacityExhausted result leaves it unspecified.
[[nodiscard]] PrepareStatus prepareAggregate(AggregateFold& f, const SourceRecord& rec, Prospect& p, TxnOverlay* chain = nullptr) noexcept;
void applyAggregate(AggregateFold& f, const SourceRecord& rec, const Prospect& p) noexcept;

// The walk of foldProjectionBatch for an aggregate: each committed record prepared into p (reserved once per instance) and applied.
// The keyed view's fold (f.keyed set); a LegacyU64 view uses foldAggregateBatch.
FoldStats foldKeyedAggregateBatch(AggregateFold& f, Prospect& p, const rune::storage::LogReadView& lv, int64_t bound, int64_t& cursor,
                                  const FoldSkip& skip, TxnFold* txn = nullptr);

} // namespace rune::view
