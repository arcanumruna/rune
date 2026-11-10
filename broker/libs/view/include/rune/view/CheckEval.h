// libs/view/include/rune/view/CheckEval.h
//
// Three-valued evaluation of a compiled `check` predicate on a view row image.
#pragma once
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <rune/codec/Descriptor.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>

namespace rune::view {

enum class PredicateResult : uint8_t { False = 0, True = 1, Unknown = 2 };

inline constexpr std::size_t kMaxPredicateNodes = 64;

// The flat node vector is evaluated in index order; the root is the last node. A comparison with a NULL or a
// non-finite Float64 operand is Unknown; and/or/not follow Kleene logic. Zero-alloc.
[[nodiscard]] PredicateResult evaluatePredicate(std::span<const CompiledPredicateNode> nodes, const RowLayout& layout,
                                                const uint8_t* row) noexcept;

// True when `before` is nullptr (a new row) or some column the predicate reads differs (null bit or bytes).
[[nodiscard]] bool predicateReadsChanged(std::span<const CompiledPredicateNode> nodes, const RowLayout& layout,
                                         const uint8_t* before, const uint8_t* after) noexcept;

// Cold. The compile-time shape rules: at most kMaxPredicateNodes nodes, and only columns of at most 16 bytes.
// `output` names the columns of `layout`. nullopt = fine.
[[nodiscard]] std::optional<std::string> constraintShapeError(const CompiledConstraint& c, const RowLayout& layout,
                                                              const rune::codec::Descriptor& output);

} // namespace rune::view
