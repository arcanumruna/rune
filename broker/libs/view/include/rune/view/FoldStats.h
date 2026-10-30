// libs/view/include/rune/view/FoldStats.h
#pragma once
#include <cstdint>

namespace rune::view {

struct FoldStats {
    uint64_t applied          = 0;
    uint64_t deleted          = 0;
    uint64_t skippedControl   = 0;
    // #537: data batches of aborted transactions skipped whole (never applied).
    uint64_t skippedAborted   = 0;
    // Part 04: records the judge rejected, skipped like a malformed one (never applied).
    uint64_t skippedRejected  = 0;
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
    // Phase 2b: Min/Max caches rebuilt by one scan of the group's value block
    // (the cached extreme was retracted) -- the known O(group size) path.
    uint64_t aggRescans       = 0;
};

} // namespace rune::view
