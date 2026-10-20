
// broker/libs/view/include/rune/view/AggState.h
//
// Aggregate views with TABLE semantics: a contributor is the source record's
// 8-byte WIRE key, its group the view key read from the record VALUE.
//   Phase 2a — invertible: Count, Count(*), Sum, Avg.
//   Phase 2b — retractable: Min, Max, Last, LastNonNull.
// Per (view, partition) the fold keeps two fixed-width auxiliary ViewTables
// next to the output rows ({...} = only in a LINKED view, i.e. one using any
// Phase 2b rule; a 2a-only view keeps its 2a layout byte-for-byte):
//   contributors : contributorKey -> [groupKey u64]{[prev u64][next u64]}[null bitmap][values]
//   groups       : groupKey -> [contributors u64][accumulators][non-null counts]
//                              {[head u64][per Min/Max: value 8B, state u8]}
// prev/next/head form a doubly linked MRU list of the group's contributors,
// keys as links (kNoContributor ends it): every write of a contributor moves it
// to the head, so Last = head, LastNonNull = first non-null from the head.
// Both tables are checkpointed with the output rows in one v2 record (§9.2).
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

// True for the Phase 2b rules that need the per-group contributor list.
bool needsContributorList(MergeRule r) noexcept;

// Precision: integer Sum/Avg accumulate in int128 and are exact under
// retraction. Float Sum/Avg under retraction are approximate (catastrophic
// cancellation is possible when magnitudes are mixed); the accumulator resets
// to 0.0 when the last non-null input leaves, and NaN/+-Inf inputs are treated
// as NULL so accumulators stay finite.
//
// That NaN/+-Inf -> NULL rule applies to the inputs with a numeric AccKind:
// Sum/Avg/Min/Max over a Float input (AccKind::Float). Count(col), Last and
// LastNonNull have AccKind::None: a NaN/+-Inf value is NON-null for them
// (Count(col) counts it; Last/LastNonNull return it verbatim).

// The numeric domain a Sum/Avg accumulates in, and a Min/Max compares in.
enum class AccKind : uint8_t { None, Signed, Unsigned, Float };

// End-of-list / no-contributor link value. A contributor whose key equals it
// cannot be linked and is skipped (skippedMalformed) in a linked view.
inline constexpr uint64_t kNoContributor = UINT64_MAX;
// AggField::cache for a field without a Min/Max cache.
inline constexpr uint16_t kNoCache = 0xFFFF;
// One Min/Max cache entry in a group row: [value 8B][state u8].
inline constexpr uint32_t kCacheEntryBytes = 9;

// A Min/Max cache's state machine (the state byte). Empty: no non-null value
// (also what a zero-filled new group row reads as). Valid: value is the
// extreme. RescanPending: the extreme was retracted and value still holds that
// RETRACTED extreme (an upper/lower bound on the rest); resolved -- by an add
// that is at least as extreme, or by one list walk at emit -- before the record
// that set it finishes, so it is never checkpointed.
enum class CacheState : uint8_t { Empty = 0, Valid = 1, RescanPending = 2 };

struct AggField {
    uint16_t  outSlot;     // output column written by this aggregate
    MergeRule rule;        // Count | CountAll | Sum | Avg | Min | Max | Last | LastNonNull
    uint16_t  srcFieldId;  // kNoSourceField for CountAll
    AccKind   kind;        // Sum/Avg/Min/Max: from the source field's type; None otherwise
    uint16_t  input;       // contributor value index; kNoSourceField for CountAll
    uint16_t  cache;       // Min/Max: group cache index; kNoCache otherwise
    rune::codec::PrimType srcType;  // source field type (CountAll: Int64)
};

struct AggLayout {
    uint16_t              keyOutSlot    = 0;   // the group-by output column
    uint16_t              keySrcFieldId = 0;   // group-by field read from the value
    rune::codec::PrimType keyType       = rune::codec::PrimType::Int64;
    std::vector<AggField> fields;              // one per non-key output column
    uint16_t              numInputs     = 0;   // fields with a source input
    RowLayout             contributorRow;      // only rowLen is meaningful
    uint32_t              contribNullOff = 8;
    uint32_t              contribValOff  = 8;  // == inputOff[0] when numInputs > 0
    RowLayout             groupRow;            // only rowLen is meaningful
    uint32_t              groupAccOff     = 8;
    uint32_t              groupNonNullOff = 8;
    // --- Phase 2b ---
    bool                  linked          = false;  // any Min/Max/Last/LastNonNull
    uint32_t              contribPrevOff  = 0;      // linked only
    uint32_t              contribNextOff  = 0;      // linked only
    std::vector<uint32_t> inputOff;                 // per input: contributor-row byte offset
    std::vector<uint16_t> inputWidth;               // per input: 8, or the source width for Last/LastNonNull
    uint16_t              numCached       = 0;      // Min/Max fields
    uint32_t              groupHeadOff    = 0;      // linked only
    uint32_t              groupCacheOff   = 0;      // linked only
};

// Builds the layout for an aggregate view. `evolvingSrc` is the source's
// evolving descriptor; a field's type/width never changes for a given tag (a
// retype is a new tag), so it decides each accumulator kind and Last width.
// nullopt if a referenced source field is not in `evolvingSrc`.
std::optional<AggLayout> buildAggLayout(const CompiledViewPlan& plan,
                                        const rune::codec::Descriptor& evolvingSrc);

// Worst-case rows ONE source record can dirty in each table (= checkpoint
// delta entries). Linked: a group move dirties the contributor, both of its
// old neighbours and the new group's old head (4), both groups (2) and both
// view rows (2). 2a: the contributor, both groups, both view rows (1,2,2).
// A rescan only reads contributors (get() never dirties). Derivation: Phase
// 2b plan Task 4; pinned by AggRetractableRegression.WorstCaseMove... .
struct AggDirtyBound {
    uint32_t contributorRows;
    uint32_t groupRows;
    uint32_t viewRows;
};
AggDirtyBound aggDirtyBoundPerRecord(const AggLayout& a) noexcept;

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
