// Frozen copy of the pre-Part-04 foldBatch / foldAggregateBatch (see LegacyFoldReference.cpp).
#pragma once
#include <rune/view/AggFold.h>
#include <rune/view/ViewFold.h>

namespace legacy_fold {

rune::view::FoldStats foldBatch(const rune::view::CompiledViewPlan& plan, const rune::view::RowLayout& layout,
                                rune::view::VersionDescriptorCache& vdc, rune::view::ViewTable& table,
                                const rune::storage::LogReadView& view, int64_t bound, int64_t& cursor,
                                const rune::view::FoldSkip& skip);

rune::view::FoldStats foldAggregateBatch(const rune::view::CompiledViewPlan& plan, const rune::view::RowLayout& layout,
                                         const rune::view::AggLayout& agg, rune::view::VersionDescriptorCache& vdc,
                                         rune::view::ViewTable& view, rune::view::ViewTable& contributors,
                                         rune::view::ViewTable& groups, rune::view::ViewTable* blocks,
                                         const rune::storage::LogReadView& lv, int64_t bound, int64_t& cursor,
                                         const rune::view::FoldSkip& skip);

} // namespace legacy_fold
