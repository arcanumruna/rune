// libs/view/include/rune/view/AggState.h
//
// Phase 2a — invertible aggregates (Count, Count(*), Sum, Avg) with TABLE
// semantics: a contributor is the source record's 8-byte WIRE key, its group
// the view key read from the record VALUE. Per (view, partition) the fold keeps
// two fixed-width auxiliary ViewTables next to the output rows:
//   contributors : contributorKey -> [groupKey u64][null bitmap][values]
//                  (the pre-image used to retract an update/delete)
//   groups       : groupKey -> [contributors u64][accumulators][non-null counts]
// Both are checkpointed with the output rows in one multi-table record (§9.2).
#pragma once
#include <cstdint>
#include <cstring>
#include <optional>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>

namespace rune::view {

// True iff any output column is an aggregate (anything but Replace).
bool isAggregateView(const CompiledViewPlan& plan) noexcept;

// Precision: integer Sum/Avg accumulate in int128 and are exact under
// retraction. Float Sum/Avg under retraction are approximate (catastrophic
// cancellation is possible when magnitudes are mixed); the accumulator resets
// to 0.0 when the last non-null input leaves, and NaN/+-Inf inputs are treated
// as NULL so accumulators stay finite.

// The numeric domain a Sum/Avg input accumulates in.
enum class AccKind : uint8_t { None, Signed, Unsigned, Float };

struct AggField {
    uint16_t  outSlot;     // output column written by this aggregate
    MergeRule rule;        // Count | CountAll | Sum | Avg
    uint16_t  srcFieldId;  // kNoSourceField for CountAll
    AccKind   kind;        // Sum/Avg: from the source field's type; None otherwise
    uint16_t  input;       // contributor value index; kNoSourceField for CountAll
};

struct AggLayout {
    uint16_t              keyOutSlot    = 0;   // the group-by output column
    uint16_t              keySrcFieldId = 0;   // group-by field read from the value
    rune::codec::PrimType keyType       = rune::codec::PrimType::Int64;
    std::vector<AggField> fields;              // one per non-key output column
    uint16_t              numInputs     = 0;   // fields with a source input
    RowLayout             contributorRow;      // only rowLen is meaningful
    uint32_t              contribNullOff = 8;
    uint32_t              contribValOff  = 8;
    RowLayout             groupRow;            // only rowLen is meaningful
    uint32_t              groupAccOff     = 8;
    uint32_t              groupNonNullOff = 8;
};

// Builds the layout for an aggregate view. `evolvingSrc` is the source's
// evolving descriptor; a field's type never changes for a given tag (a retype
// is a new tag), so it decides each Sum/Avg accumulator kind. nullopt if a
// referenced source field is not in `evolvingSrc` (compiler-guaranteed never).
std::optional<AggLayout> buildAggLayout(const CompiledViewPlan& plan,
                                        const rune::codec::Descriptor& evolvingSrc);

// Table ids inside a multi-table checkpoint record (ViewCheckpoint v2).
inline constexpr uint8_t kAggTableView         = 0;
inline constexpr uint8_t kAggTableContributors = 1;
inline constexpr uint8_t kAggTableGroups       = 2;

// Fixed-width little-endian byte helpers (memcpy: alignment-free, no UB).
inline uint64_t loadU64(const uint8_t* p) noexcept { uint64_t v; std::memcpy(&v, p, 8); return v; }
inline void     storeU64(uint8_t* p, uint64_t v) noexcept { std::memcpy(p, &v, 8); }
inline __int128 loadI128(const uint8_t* p) noexcept { __int128 v; std::memcpy(&v, p, 16); return v; }
inline void     storeI128(uint8_t* p, __int128 v) noexcept { std::memcpy(p, &v, 16); }
inline unsigned __int128 loadU128(const uint8_t* p) noexcept { unsigned __int128 v; std::memcpy(&v, p, 16); return v; }
inline void     storeU128(uint8_t* p, unsigned __int128 v) noexcept { std::memcpy(p, &v, 16); }
inline double   loadF64(const uint8_t* p) noexcept { double v; std::memcpy(&v, p, 8); return v; }
inline void     storeF64(uint8_t* p, double v) noexcept { std::memcpy(p, &v, 8); }

} // namespace rune::view
