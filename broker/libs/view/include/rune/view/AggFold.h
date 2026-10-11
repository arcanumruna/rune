// libs/view/include/rune/view/AggFold.h — Phase 2a aggregate fold.
#pragma once
#include <cstdint>

#include <rune/storage/LogReadView.h>
#include <rune/view/AggState.h>
#include <rune/view/ViewFold.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewTable.h>

namespace rune::view {

// Folds every committed record (offset < bound) of `lv` into an aggregate view
// with TABLE semantics (AggState.h): contributor = 8-byte wire key, group = the
// group-by value. Same batch walk, control-batch skip, bound stop, and cursor
// contract as foldBatch (ViewFold.h). Zero heap allocation in steady state.
FoldStats foldAggregateBatch(const CompiledViewPlan& plan, const RowLayout& layout,
                             const AggLayout& agg, VersionDescriptorCache& vdc,
                             ViewTable& view, ViewTable& contributors, ViewTable& groups,
                             const rune::storage::LogReadView& lv, int64_t bound,
                             int64_t& cursor);

} // namespace rune::view
