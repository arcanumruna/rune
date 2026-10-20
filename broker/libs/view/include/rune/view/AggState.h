// broker/libs/view/include/rune/view/AggState.h
//
// Aggregate views with TABLE semantics: a contributor is the source record's
// 8-byte WIRE key, its group the view key read from the record VALUE.
//   Phase 2a — invertible: Count, Count(*), Sum, Avg.
//   Phase 2b — retractable: Min, Max, Last, LastNonNull.
// Per (view, partition) the fold keeps fixed-width auxiliary ViewTables next
// to the output rows ({...} = only in a LINKED view, i.e. one using any Phase
// 2b rule; <...> = only when the view has a Min or Max; a 2a-only view keeps
// its 2a layout byte-for-byte):
//   contributors : contributorKey -> [groupKey u64]{[prev u64][next u64]}[null bitmap][values]<[blockPos u32]>
//   groups       : groupKey -> [contributors u64][accumulators][non-null counts]
//                              {[head u64][per Min/Max: value 8B, state u8]}<[blockId u32][blockChunks u32]>
//   blocks       : <blockChunkKey(blockId, chunk) -> [keys u64 x 32][per column: values 8B x 32][per column: nulls u8 x 32]>
// prev/next/head form a doubly linked MRU list of the group's contributors,
// keys as links (kNoContributor ends it): every write of a contributor moves it
// to the head, so Last = head, LastNonNull = first non-null from the head.
// The value block (Task 5b) holds each group's Min/Max inputs densely at
// positions 0..n-1 (32 per chunk row; every chunk but the tail full; at most one
// spare chunk), one column per distinct Min/Max source field: a rescan of a
// retracted extreme is a sequential scan of ceil(n/32) chunks, not a list walk.
// Add appends; an in-place update rewrites its entry; a remove swap-removes (the
// last entry moves into the hole, its contributor's blockPos is patched); the
// spare is freed once the tail holds <= 16 entries (hysteresis). A group's
// blockId is chosen at its first entry (blockIdCandidate, then linear probing);
// an id is in use iff its chunk 0 exists. All tables are checkpointed with the
// output rows in one v2 record (§9.2; blocks = table id 3).
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

// --- Min/Max value block (Task 5b) ---
inline constexpr uint32_t kBlockEntries     = 32;                 // entries per chunk row
inline constexpr uint32_t kBlockShift       = 5;
inline constexpr uint32_t kBlockMask        = kBlockEntries - 1;
static_assert((1u << kBlockShift) == kBlockEntries, "kBlockEntries must be 1 << kBlockShift");
inline constexpr uint32_t kBlockSpareFreeAt = kBlockEntries / 2;  // free the spare once the tail holds <= this
inline constexpr uint32_t kNoBlock          = 0;                  // group row blockId: no block
inline constexpr uint32_t kBlockIdProbes    = 64;                 // linear-probe limit for a free block id

// Exact chunk-row key: a 32-bit block id (per group) and a 32-bit chunk index.
inline constexpr uint64_t blockChunkKey(uint32_t blockId, uint32_t chunk) noexcept {
    return (static_cast<uint64_t>(blockId) << 32) | chunk;
}
// First block id tried for group key g (fixed 64-bit finalizer, low 32 bits;
// never kNoBlock). Collisions are resolved by linear probing in the fold.
inline constexpr uint32_t blockIdCandidate(uint64_t g) noexcept {
    g ^= g >> 33; g *= 0xff51afd7ed558ccdULL;
    g ^= g >> 33; g *= 0xc4ceb9fe1a85ec53ULL;
    g ^= g >> 33;
    const uint32_t id = static_cast<uint32_t>(g);
    return id == kNoBlock ? 1u : id;
}
// Chunk-row offsets (column-wise). slot = position & kBlockMask.
inline constexpr uint32_t blockKeyOff(uint32_t slot) noexcept { return 8u * slot; }
inline constexpr uint32_t blockValOff(uint16_t col, uint32_t slot) noexcept {
    return 8u * kBlockEntries * (1u + col) + 8u * slot;
}

// A Min/Max cache's state machine (the state byte). Empty: no non-null value
// (also what a zero-filled new group row reads as). Valid: value is the
// extreme. RescanPending: the extreme was retracted and value still holds that
// RETRACTED extreme (an upper/lower bound on the rest); resolved -- by an add
// that is at least as extreme, or by one value-block scan at emit -- before the record
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
    // --- Phase 2b Task 5b: Min/Max value block (all 0/empty when numBlockCols == 0) ---
    uint16_t              numBlockCols       = 0;   // distinct Min/Max source fields
    std::vector<uint16_t> blockColInput;            // per column: the input whose 8-byte value it copies
    std::vector<uint16_t> cacheCol;                 // per Min/Max cache index: its column
    uint32_t              contribBlockPosOff = 0;   // [blockPos u32] at the end of the contributor row
    uint32_t              groupBlockOff      = 0;   // [blockId u32][blockChunks u32] after the caches
    RowLayout             blockRow;                 // only rowLen: kBlockEntries * (8 + 9 * numBlockCols)
};

// Null-flag byte of (column, slot) in a chunk row.
inline uint32_t blockNullOff(const AggLayout& a, uint16_t col, uint32_t slot) noexcept {
    return 8u * kBlockEntries * (1u + a.numBlockCols) + kBlockEntries * col + slot;
}

// Builds the layout for an aggregate view. `evolvingSrc` is the source's
// evolving descriptor; a field's type/width never changes for a given tag (a
// retype is a new tag), so it decides each accumulator kind and Last width.
// nullopt if a referenced source field is not in `evolvingSrc`.
std::optional<AggLayout> buildAggLayout(const CompiledViewPlan& plan,
                                        const rune::codec::Descriptor& evolvingSrc);

// Worst-case rows ONE source record can dirty in each table (= checkpoint
// delta entries). Min/Max views: a group move dirties the contributor, both of
// its old neighbours, the new group's old head and the old group's swap-moved
// last entry (5); both groups (2); both view rows (2); the old group's hole
// chunk and freed spare plus the new group's append chunk (3). Last-only linked
// views {4,2,2,0}; 2a {1,2,2,0}. A rescan only reads (get() never dirties); a
// damaged-state branch never writes more than the healthy one. Derivation:
// Phase 2b plan Task 5b; pinned by AggRetractableRegression.WorstCaseMoveDirtiesExactlyTheBoundRegression.
struct AggDirtyBound {
    uint32_t contributorRows;
    uint32_t groupRows;
    uint32_t viewRows;
    uint32_t blockRows;
};
AggDirtyBound aggDirtyBoundPerRecord(const AggLayout& a) noexcept;

// Table ids inside a multi-table checkpoint record (ViewCheckpoint v2).
inline constexpr uint8_t kAggTableView         = 0;
inline constexpr uint8_t kAggTableContributors = 1;
inline constexpr uint8_t kAggTableGroups       = 2;
inline constexpr uint8_t kAggTableBlocks       = 3;   // only views with Min/Max (4 tables)

// Fixed-width little-endian byte helpers (memcpy: alignment-free, no UB).
inline uint64_t loadU64(const uint8_t* p) noexcept { uint64_t v; std::memcpy(&v, p, 8); return v; }
inline void     storeU64(uint8_t* p, uint64_t v) noexcept { std::memcpy(p, &v, 8); }
inline uint32_t loadU32(const uint8_t* p) noexcept { uint32_t v; std::memcpy(&v, p, 4); return v; }
inline void     storeU32(uint8_t* p, uint32_t v) noexcept { std::memcpy(p, &v, 4); }
inline __int128 loadI128(const uint8_t* p) noexcept { __int128 v; std::memcpy(&v, p, 16); return v; }
inline void     storeI128(uint8_t* p, __int128 v) noexcept { std::memcpy(p, &v, 16); }
inline unsigned __int128 loadU128(const uint8_t* p) noexcept { unsigned __int128 v; std::memcpy(&v, p, 16); return v; }
inline void     storeU128(uint8_t* p, unsigned __int128 v) noexcept { std::memcpy(p, &v, 16); }
inline double   loadF64(const uint8_t* p) noexcept { double v; std::memcpy(&v, p, 8); return v; }
inline void     storeF64(uint8_t* p, double v) noexcept { std::memcpy(p, &v, 8); }

} // namespace rune::view
