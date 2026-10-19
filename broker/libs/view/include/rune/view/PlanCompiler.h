// libs/view/include/rune/view/PlanCompiler.h
#pragma once
#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <rune/codec/Descriptor.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>

namespace rune::view {

// Control-plane (CreateView-time) compilation error. Never thrown -- always
// returned via std::unexpected so a rejected view never produces a partial
// plan.
struct CompileError {
    std::string message;
};

// The result of compiling an authored ViewPlan: the resolved plan (names ->
// ids), the output schema's own Descriptor (so it can be registered/fanned
// out like any other topic schema), and the fixed-width RowLayout derived
// from it.
struct CompileResult {
    CompiledViewPlan plan;
    rune::codec::Descriptor outputDescriptor;
    RowLayout rowLayout;
};

// Resolves an authored `ViewPlan` against its upstream source schemas,
// validates every name/type constraint Phase 1 requires, and derives the
// compiled plan + output Descriptor + RowLayout. `sourceDescriptors[i]` must
// be the current Descriptor for `authored.sources[i]`.
//
// Never throws; every rejection path returns std::unexpected(CompileError)
// with a human-readable message. Phase 1 scope: fixed-width output columns
// only, single-source Replace (join/multi-source resolution is Phase 3, but
// the per-source vectors are already sized for it).
std::expected<CompileResult, CompileError> compileView(
    const ViewPlan& authored,
    std::span<const rune::codec::Descriptor* const> sourceDescriptors,
    int32_t viewTopicId);

} // namespace rune::view
