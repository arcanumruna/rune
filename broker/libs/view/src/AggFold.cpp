// broker/libs/view/src/AggFold.cpp — see AggFold.h / AggState.h.
#include <rune/view/AggFold.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <span>
#include <string_view>

#include <rune/codec/Record.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/view/FoldWalk.h>

namespace rune::view {

namespace {

using rune::codec::FieldValue;
using rune::codec::PrimType;

inline std::span<const uint8_t> asBytes(std::string_view sv) noexcept {
    return {reinterpret_cast<const uint8_t*>(sv.data()), sv.size()};
}

inline bool nullBit(const uint8_t* row, uint32_t off, uint16_t i) noexcept {
    return (row[off + i / 8] >> (i % 8)) & 1u;
}
inline void setNullBit(uint8_t* row, uint32_t off, uint16_t i, bool isNull) noexcept {
    const uint8_t m = static_cast<uint8_t>(1u << (i % 8));
    if (isNull) row[off + i / 8] |= m; else row[off + i / 8] &= static_cast<uint8_t>(~m);
}

// Min/Max comparisons in an input's widened 8-byte domain (int64 / uint64 /
// double bits -- exactly what a contributor value slot and a cache hold).
inline bool strictlyBetter(MergeRule rule, AccKind kind, const uint8_t* a, const uint8_t* b) noexcept {
    const bool max = rule == MergeRule::Max;
    switch (kind) {
        case AccKind::Signed: {
            int64_t x, y;
            std::memcpy(&x, a, 8);
            std::memcpy(&y, b, 8);
            return max ? x > y : x < y;
        }
        case AccKind::Unsigned: {
            const uint64_t x = loadU64(a), y = loadU64(b);
            return max ? x > y : x < y;
        }
        case AccKind::Float: {
            const double x = loadF64(a), y = loadF64(b);
            return max ? x > y : x < y;
        }
        case AccKind::None: return false;
    }
    return false;
}
inline bool sameValue(AccKind kind, const uint8_t* a, const uint8_t* b) noexcept {
    if (kind == AccKind::Float) return loadF64(a) == loadF64(b);
    return std::memcmp(a, b, 8) == 0;
}

constexpr uint8_t kEmpty   = static_cast<uint8_t>(CacheState::Empty);
constexpr uint8_t kValid   = static_cast<uint8_t>(CacheState::Valid);
constexpr uint8_t kPending = static_cast<uint8_t>(CacheState::RescanPending);
// Chunk lookups issued before scanning (independent keys: their misses overlap).
constexpr uint32_t kRescanBatch = 8;

// What `retract` does with the group the contributor leaves. One state per legal
// combination (this replaces a (dropIfEmpty, emitNow) bool pair, F8):
//   Delete         tombstone / missing group key: an emptied group loses its rows,
//                  a non-empty group is re-emitted now.
//   Move           update into ANOTHER group, or any 2a update: a non-empty old
//                  group is re-emitted now; an emptied one is dropped by the caller
//                  after the add.
//   InPlaceLinked  update within the SAME group of a linked view: no emit and no
//                  drop here. The caller's add + emit follows, so a new value that
//                  is at least as extreme skips the rescan.
enum class RetractMode : uint8_t { Delete, Move, InPlaceLinked };

// The tables as the step sees them.  DirectTable writes the ViewTable, so an ordinary view folds exactly as it
// always did; ProspectTable reads overlay-first and writes into the fixed slots of a Prospect.
struct DirectTable {
    ViewTable& t;
    [[nodiscard]] const uint8_t* get(uint64_t k) const noexcept { return t.get(k); }
    uint8_t* put(uint64_t k) { return t.put(k); }
    bool erase(uint64_t k) noexcept { return t.erase(k); }
};

struct ProspectTable {
    RowOverlay&        ov;
    RowOverlay::Table  which;
    TxnTable           base;   // the stored rows, seen through the chain and lock set; no table behind it for a keyed view's rows (found by gid here, apply maps them to their view keys; emit rewrites a whole row)

    [[nodiscard]] const uint8_t* get(uint64_t k) noexcept {
        if (RowOverlay::Slot* s = ov.find(which, k)) return s->erased ? nullptr : ov.imageOf(which, s);
        return base.get(k);
    }
    uint8_t* put(uint64_t k) noexcept {
        if (RowOverlay::Slot* s = ov.find(which, k)) {
            uint8_t* img = ov.imageOf(which, s);
            if (s->erased) { std::memset(img, 0, ov.rowLen(which)); s->erased = false; }
            return img;
        }
        uint8_t* img = nullptr;
        ov.add(which, k, img);
        if (const uint8_t* cur = base.get(k)) std::memcpy(img, cur, ov.rowLen(which));
        else std::memset(img, 0, ov.rowLen(which));
        return img;
    }
    bool erase(uint64_t k) noexcept {
        if (RowOverlay::Slot* s = ov.find(which, k)) {
            const bool was = !s->erased;
            s->erased = true;
            return was;
        }
        if (base.table() != nullptr && base.get(k) == nullptr) return false;
        uint8_t* img = nullptr;
        ov.add(which, k, img)->erased = true;
        return true;
    }
};

template <class T>
struct Ctx {
    const RowLayout& layout;
    const AggLayout& agg;
    T  view;
    T  contributors;
    T  groups;
    T* blocks;   // Min/Max value blocks; nullptr iff agg.numBlockCols == 0
    FoldStats& stats;
    // Keyed view (agg.keyed): the spec, the group key -> gid table (read only; apply writes it) and the gid a new group takes.
    const KeyedFoldSpec* keyed = nullptr;
    const TxnTable* groupIds = nullptr;
    uint64_t nextGid = 0;

    uint8_t* acc(uint8_t* g, size_t i) const noexcept { return g + agg.groupAccOff + 16 * i; }
    uint8_t* nn(uint8_t* g, size_t i) const noexcept { return g + agg.groupNonNullOff + 8 * i; }
    const uint8_t* val(const uint8_t* c, uint16_t input) const noexcept {
        return c + agg.inputOff[input];
    }
    uint8_t* cacheVal(uint8_t* g, uint16_t ci) const noexcept {
        return g + agg.groupCacheOff + kCacheEntryBytes * ci;
    }
    uint8_t* cacheState(uint8_t* g, uint16_t ci) const noexcept { return cacheVal(g, ci) + 8; }

    // Writable row of an EXISTING contributor (put() marks it dirty). A link
    // to a missing row is counted and never followed -- and never creates one.
    uint8_t* linkedContributor(uint64_t k) noexcept {
        if (k == kNoContributor || contributors.get(k) == nullptr) {
            ++stats.aggInconsistent;
            return nullptr;
        }
        return contributors.put(k);
    }

    void addToCache(uint8_t* gr, const AggField& f, const uint8_t* v, uint64_t nnNew) noexcept {
        uint8_t* cv = cacheVal(gr, f.cache);
        uint8_t* cs = cacheState(gr, f.cache);
        switch (*cs) {
            case kEmpty:
                if (nnNew == 1) { std::memcpy(cv, v, 8); *cs = kValid; }
                else { ++stats.aggInconsistent; *cs = kPending; }   // values without a cache: rebuild from the value block
                break;
            case kValid:
                if (strictlyBetter(f.rule, f.kind, v, cv)) std::memcpy(cv, v, 8);
                break;
            case kPending:
                // cv still holds the RETRACTED extreme, which bounds every
                // remaining value: one at least as extreme is the new extreme.
                if (strictlyBetter(f.rule, f.kind, v, cv) || sameValue(f.kind, v, cv)) {
                    std::memcpy(cv, v, 8);
                    *cs = kValid;
                }
                break;
            default:                                            // corrupt state byte
                ++stats.aggInconsistent;
                *cs = kPending;
                break;
        }
    }

    void retractFromCache(uint8_t* gr, const AggField& f, const uint8_t* v, uint64_t nnNew) noexcept {
        uint8_t* cv = cacheVal(gr, f.cache);
        uint8_t* cs = cacheState(gr, f.cache);
        if (nnNew == 0) { storeU64(cv, 0); *cs = kEmpty; return; }
        if (*cs == kValid && sameValue(f.kind, v, cv)) *cs = kPending;
    }

    // Applies one contributor row to its group: sign = +1 add, -1 retract.
    // Add: returns the group row (created if new). Retract: returns nullptr and
    // counts aggInconsistent -- touching nothing -- if the group row is missing
    // or any count would underflow (checked BEFORE mutating).
    uint8_t* applyContribution(const uint8_t* c, int sign) noexcept {
        const uint64_t g = loadU64(c);
        if (sign < 0) {
            const uint8_t* cur = groups.get(g);
            bool bad = cur == nullptr || loadU64(cur) == 0;
            for (size_t i = 0; !bad && i < agg.fields.size(); ++i) {
                const AggField& f = agg.fields[i];
                if (f.input == kNoSourceField || nullBit(c, agg.contribNullOff, f.input)) continue;
                if (loadU64(cur + agg.groupNonNullOff + 8 * i) == 0) bad = true;
            }
            if (bad) { ++stats.aggInconsistent; return nullptr; }
        }
        uint8_t* gr = groups.put(g);   // retract: returns the existing row
        const uint64_t n = loadU64(gr);
        storeU64(gr, sign > 0 ? n + 1 : n - 1);
        for (size_t i = 0; i < agg.fields.size(); ++i) {
            const AggField& f = agg.fields[i];
            if (f.input == kNoSourceField) continue;                 // CountAll
            if (nullBit(c, agg.contribNullOff, f.input)) continue;   // nulls contribute nothing
            const uint64_t nnv = loadU64(nn(gr, i));
            const uint64_t nnNew = sign > 0 ? nnv + 1 : nnv - 1;
            storeU64(nn(gr, i), nnNew);
            const uint8_t* v = val(c, f.input);
            if (f.cache != kNoCache) {                               // Min/Max
                if (sign > 0) addToCache(gr, f, v, nnNew); else retractFromCache(gr, f, v, nnNew);
                continue;
            }
            if (f.rule != MergeRule::Sum && f.rule != MergeRule::Avg) continue;   // Count(col), Last*
            switch (f.kind) {
                case AccKind::Signed: {
                    int64_t x; std::memcpy(&x, v, 8);
                    const __int128 a = loadI128(acc(gr, i));
                    storeI128(acc(gr, i), sign > 0 ? a + x : a - x);
                    break;
                }
                case AccKind::Unsigned: {
                    const uint64_t x = loadU64(v);
                    const unsigned __int128 a = loadU128(acc(gr, i));
                    storeU128(acc(gr, i), sign > 0 ? a + x : a - x);
                    break;
                }
                case AccKind::Float: {
                    const double x = loadF64(v);
                    const double a = loadF64(acc(gr, i));
                    // Last non-null contributor gone: reset to exactly 0.0 (no
                    // cancellation residual carried into the next contributor).
                    storeF64(acc(gr, i), nnNew == 0 ? 0.0 : (sign > 0 ? a + x : a - x));
                    break;
                }
                case AccKind::None: break;
            }
        }
        return gr;
    }

    // Unlinks contributor `c` (its pre-image row `cr`) from group `g`'s list.
    // `gr` is g's row, or nullptr if the retraction found g inconsistent (the
    // head is then still fixed when g's row exists).
    void unlink(uint64_t c, const uint8_t* cr, uint64_t g, uint8_t* gr) noexcept {
        const uint64_t prev = loadU64(cr + agg.contribPrevOff);
        const uint64_t next = loadU64(cr + agg.contribNextOff);
        if (prev != kNoContributor) {
            if (uint8_t* p = linkedContributor(prev)) storeU64(p + agg.contribNextOff, next);
        } else {
            uint8_t* hr = gr != nullptr ? gr : (groups.get(g) != nullptr ? groups.put(g) : nullptr);
            if (hr != nullptr && loadU64(hr + agg.groupHeadOff) == c) storeU64(hr + agg.groupHeadOff, next);
            else ++stats.aggInconsistent;   // no predecessor, yet not its group's head
        }
        if (next != kNoContributor) {
            if (uint8_t* nx = linkedContributor(next)) storeU64(nx + agg.contribPrevOff, prev);
        }
    }

    // Links contributor `c` (row `cr`) at the head of the group whose row is `gr`
    // (applyContribution(+1) has already counted c in).
    void linkAtHead(uint64_t c, uint8_t* cr, uint8_t* gr) noexcept {
        // c is the only contributor -> the list was empty (a new group row is
        // zero-filled, so its stored head is not a key then).
        uint64_t oldHead = loadU64(gr) == 1 ? kNoContributor : loadU64(gr + agg.groupHeadOff);
        if (oldHead == c) { ++stats.aggInconsistent; oldHead = kNoContributor; }
        storeU64(cr + agg.contribPrevOff, kNoContributor);
        storeU64(cr + agg.contribNextOff, oldHead);
        if (oldHead != kNoContributor) {
            if (uint8_t* h = linkedContributor(oldHead)) storeU64(h + agg.contribPrevOff, c);
            else storeU64(cr + agg.contribNextOff, kNoContributor);   // never link to a missing row
        }
        storeU64(gr + agg.groupHeadOff, c);
    }

    // --- Min/Max value block (Task 5b; layout in AggState.h) ---
    uint32_t blockIdOf(const uint8_t* gr) const noexcept { return loadU32(gr + agg.groupBlockOff); }
    uint32_t blockChunksOf(const uint8_t* gr) const noexcept { return loadU32(gr + agg.groupBlockOff + 4); }
    void setBlock(uint8_t* gr, uint32_t id, uint32_t chunks) const noexcept {
        storeU32(gr + agg.groupBlockOff, id);
        storeU32(gr + agg.groupBlockOff + 4, chunks);
    }

    // Writes entry `slot` of chunk `row` from contributor c's row `cr`.
    void writeEntry(uint8_t* row, uint32_t slot, uint64_t c, const uint8_t* cr) const noexcept {
        storeU64(row + blockKeyOff(slot), c);
        for (uint16_t col = 0; col < agg.numBlockCols; ++col) {
            const uint16_t in = agg.blockColInput[col];
            std::memcpy(row + blockValOff(col, slot), cr + agg.inputOff[in], 8);
            row[blockNullOff(agg, col, slot)] = nullBit(cr, agg.contribNullOff, in) ? 1 : 0;
        }
    }
    // Copies entry `from` of chunk `src` (key, values, nulls) to entry `to` of chunk `dst`.
    void moveEntry(uint8_t* dst, uint32_t to, const uint8_t* src, uint32_t from) const noexcept {
        std::memcpy(dst + blockKeyOff(to), src + blockKeyOff(from), 8);
        for (uint16_t col = 0; col < agg.numBlockCols; ++col) {
            std::memcpy(dst + blockValOff(col, to), src + blockValOff(col, from), 8);
            dst[blockNullOff(agg, col, to)] = src[blockNullOff(agg, col, from)];
        }
    }

    // A free block id for group g: its candidate, then linear probing (an id is
    // in use iff its chunk 0 exists; get() never dirties). kNoBlock if none.
    uint32_t allocBlockId(uint64_t g) const noexcept {
        uint32_t id = blockIdCandidate(g);
        for (uint32_t i = 0; i < kBlockIdProbes; ++i, ++id) {
            if (id == kNoBlock) id = 1;   // wrapped
            if (blocks->get(blockChunkKey(id, 0)) == nullptr) return id;
        }
        return kNoBlock;
    }

    // Appends contributor c (row cr, already counted into group g's row gr by
    // applyContribution(+1)) as the group's last entry, position n-1: into the
    // tail, the spare, or a new chunk at a boundary. Exactly one chunk row put.
    void blockAppend(uint64_t c, uint8_t* cr, uint64_t g, uint8_t* gr) noexcept {
        const uint64_t n = loadU64(gr);
        if (blocks == nullptr || n == 0 || n > UINT32_MAX) { ++stats.aggInconsistent; return; }
        const uint32_t pos = static_cast<uint32_t>(n - 1);
        uint32_t id = blockIdOf(gr);
        uint32_t chunks = blockChunksOf(gr);
        if (id == kNoBlock) {
            if (pos != 0) { ++stats.aggInconsistent; return; }   // entries without a block
            id = allocBlockId(g);
            if (id == kNoBlock) { ++stats.aggInconsistent; return; }
            chunks = 0;
        }
        const uint32_t idx = pos >> kBlockShift;
        uint8_t* row = nullptr;
        if (idx < chunks) {                                      // the tail, or the spare at a boundary
            if (blocks->get(blockChunkKey(id, idx)) == nullptr) ++stats.aggInconsistent;
            else row = blocks->put(blockChunkKey(id, idx));
        } else if (idx == chunks) {                              // a new chunk (zero-filled)
            row = blocks->put(blockChunkKey(id, idx));
            ++chunks;
        } else {
            ++stats.aggInconsistent;                             // a gap in the chunk sequence
        }
        setBlock(gr, id, chunks);
        if (row == nullptr) return;
        writeEntry(row, pos & kBlockMask, c, cr);
        storeU32(cr + agg.contribBlockPosOff, pos);
    }

    // Rewrites contributor c's entry in place (same group; position unchanged).
    void blockWrite(uint64_t c, const uint8_t* cr, const uint8_t* gr) noexcept {
        const uint32_t pos = loadU32(cr + agg.contribBlockPosOff);
        const uint32_t id = blockIdOf(gr);
        if (blocks == nullptr || id == kNoBlock || pos >= loadU64(gr)) { ++stats.aggInconsistent; return; }
        const uint64_t key = blockChunkKey(id, pos >> kBlockShift);
        const uint8_t* cur = blocks->get(key);
        const uint32_t slot = pos & kBlockMask;
        if (cur == nullptr || loadU64(cur + blockKeyOff(slot)) != c) { ++stats.aggInconsistent; return; }
        writeEntry(blocks->put(key), slot, c, cr);
    }

    // Removes contributor c (pre-image row cr) from group gr's block, AFTER
    // applyContribution(-1) (so loadU64(gr) is the post-remove count n'): the
    // old last entry (position n') moves into c's slot and its contributor's
    // blockPos is patched (the tail chunk is only read). Then the chunk policy:
    // n' == 0 frees the block and its id; a spare is freed once the tail holds
    // <= kBlockSpareFreeAt entries; an emptied tail simply becomes the spare.
    // At most: the hole chunk (put) + one erase -> 2 block rows, 1 contributor row.
    void blockRemove(uint64_t c, const uint8_t* cr, uint8_t* gr) noexcept {
        const uint64_t n = loadU64(gr);
        const uint32_t id = blockIdOf(gr);
        uint32_t chunks = blockChunksOf(gr);
        const uint32_t pos = loadU32(cr + agg.contribBlockPosOff);
        if (blocks == nullptr || id == kNoBlock || n > UINT32_MAX || pos > n) {
            ++stats.aggInconsistent;
            return;
        }
        const uint32_t last = static_cast<uint32_t>(n);
        if (pos != last) {
            const uint64_t holeKey = blockChunkKey(id, pos >> kBlockShift);
            const uint8_t* tail = blocks->get(blockChunkKey(id, last >> kBlockShift));
            const uint8_t* hole = blocks->get(holeKey);
            if (tail == nullptr || hole == nullptr ||
                loadU64(hole + blockKeyOff(pos & kBlockMask)) != c) {
                ++stats.aggInconsistent;
            } else {
                const uint64_t m = loadU64(tail + blockKeyOff(last & kBlockMask));
                moveEntry(blocks->put(holeKey), pos & kBlockMask, tail, last & kBlockMask);
                if (uint8_t* mr = linkedContributor(m)) storeU32(mr + agg.contribBlockPosOff, pos);
            }
        }
        if (n == 0) {
            if (chunks > 2) ++stats.aggInconsistent;              // a 1-entry block owns <= 1 chunk
            for (uint32_t k = 0; k < std::min<uint32_t>(chunks, 2); ++k)
                (void)blocks->erase(blockChunkKey(id, k));
            setBlock(gr, kNoBlock, 0);
            return;
        }
        const uint32_t used = static_cast<uint32_t>((n - 1) >> kBlockShift) + 1;
        const uint32_t tailFill = static_cast<uint32_t>(n - (static_cast<uint64_t>(used - 1) << kBlockShift));
        if (chunks == used + 1 && tailFill <= kBlockSpareFreeAt) {
            (void)blocks->erase(blockChunkKey(id, chunks - 1));
            --chunks;
        } else if (chunks != used && chunks != used + 1) {
            ++stats.aggInconsistent;
        }
        setBlock(gr, id, chunks);
    }

    // Rebuilds Min/Max field i's cache by ONE sequential scan of the group's
    // value block: ceil(n/32) chunk rows, looked up kRescanBatch at a time (the
    // keys are computed, not links, so their misses overlap), then n packed
    // values of the field's column. get() never dirties. A missing chunk, a
    // short scan or a non-null count mismatch is counted; the cache reports what
    // was actually found.
    void rescan(uint8_t* gr, size_t i, const AggField& f) noexcept {
        ++stats.aggRescans;
        uint8_t* cv = cacheVal(gr, f.cache);
        uint8_t* cs = cacheState(gr, f.cache);
        const uint64_t n = loadU64(gr);
        const uint64_t nonNull = loadU64(nn(gr, i));
        const uint16_t col = agg.cacheCol[f.cache];
        const uint32_t id = blocks != nullptr ? blockIdOf(gr) : kNoBlock;
        bool have = false;
        bool bad = id == kNoBlock;
        uint64_t seen = 0, scanned = 0;
        const uint64_t chunks = bad ? 0 : (n + kBlockEntries - 1) >> kBlockShift;
        const uint8_t* rows[kRescanBatch];
        for (uint64_t base = 0; base < chunks; base += kRescanBatch) {
            const uint32_t m = static_cast<uint32_t>(std::min<uint64_t>(kRescanBatch, chunks - base));
            for (uint32_t j = 0; j < m; ++j) {
                rows[j] = blocks->get(blockChunkKey(id, static_cast<uint32_t>(base + j)));
                if (rows[j] != nullptr) __builtin_prefetch(rows[j] + blockValOff(col, 0));
            }
            for (uint32_t j = 0; j < m; ++j) {
                const uint8_t* r = rows[j];
                if (r == nullptr) { bad = true; continue; }
                const uint64_t first = (base + j) << kBlockShift;
                const uint32_t cnt = static_cast<uint32_t>(std::min<uint64_t>(kBlockEntries, n - first));
                const uint8_t* nulls = r + blockNullOff(agg, col, 0);
                const uint8_t* vals = r + blockValOff(col, 0);
                for (uint32_t s = 0; s < cnt; ++s) {
                    if (nulls[s] != 0) continue;
                    const uint8_t* v = vals + 8u * s;
                    if (!have || strictlyBetter(f.rule, f.kind, v, cv)) { std::memcpy(cv, v, 8); have = true; }
                    ++seen;
                }
                scanned += cnt;
            }
        }
        if (bad || scanned != n || seen != nonNull) ++stats.aggInconsistent;
        if (!have) storeU64(cv, 0);
        *cs = have ? kValid : kEmpty;
    }

    // The most recently written contributor (emit runs only for a non-empty group).
    const uint8_t* headContributor(const uint8_t* gr) noexcept {
        const uint64_t h = loadU64(gr + agg.groupHeadOff);
        const uint8_t* r = h == kNoContributor ? nullptr : contributors.get(h);
        if (r == nullptr) ++stats.aggInconsistent;
        return r;
    }

    // First contributor from the head whose `input` is non-null; bounded walk.
    const uint8_t* firstNonNull(const uint8_t* gr, uint16_t input) noexcept {
        const uint64_t n = loadU64(gr);
        uint64_t k = loadU64(gr + agg.groupHeadOff);
        for (uint64_t steps = 0; k != kNoContributor; ++steps) {
            if (steps == n) break;
            const uint8_t* r = contributors.get(k);
            if (r == nullptr) break;
            if (!nullBit(r, agg.contribNullOff, input)) return r;
            k = loadU64(r + agg.contribNextOff);
        }
        ++stats.aggInconsistent;   // the non-null count said one exists
        return nullptr;
    }

    void dropGroup(uint64_t g) noexcept {
        view.erase(g);
        groups.erase(g);
    }

    // Rewrites the output row for group `g` from its accumulators, caches and list.
    void emit(uint64_t g, uint8_t* gr) noexcept {
        uint8_t* row = view.put(g);   // a keyed view's rows are found by gid here; apply maps them to the view key
        if (keyed != nullptr) {
            for (const KeyedFoldSpec::Component& c : keyed->components)
                writeSlot(row, layout, c.outSlot, rune::codec::decodeKeyComponent(c.type, c.keyFunction, {gr + agg.groupKeyOff + c.offset, c.width}));
        } else {
            FieldValue k{};
            k.type = agg.keyType;
            k.present = true;
            if (agg.keyType == PrimType::Int64) { int64_t s; std::memcpy(&s, &g, 8); k.i = s; }
            else { k.u = g; }
            writeSlot(row, layout, agg.keyOutSlot, k);
        }

        const uint64_t contributorsN = loadU64(gr);
        for (size_t i = 0; i < agg.fields.size(); ++i) {
            const AggField& f = agg.fields[i];
            const uint64_t nonNull = loadU64(nn(gr, i));
            FieldValue out{};
            out.present = true;
            switch (f.rule) {
                case MergeRule::CountAll:
                    out.type = PrimType::Int64; out.i = static_cast<int64_t>(contributorsN); break;
                case MergeRule::Count:
                    out.type = PrimType::Int64; out.i = static_cast<int64_t>(nonNull); break;
                case MergeRule::Sum:
                    if (nonNull == 0) { out.present = false; out.type = PrimType::Int64; break; }
                    if (f.kind == AccKind::Signed) {
                        out.type = PrimType::Int64;
                        const __int128 a = loadI128(acc(gr, i));
                        if (a > std::numeric_limits<int64_t>::max() ||
                            a < std::numeric_limits<int64_t>::min()) {
                            out.present = false; ++stats.aggOverflow;
                        } else {
                            out.i = static_cast<int64_t>(a);
                        }
                    } else if (f.kind == AccKind::Unsigned) {
                        out.type = PrimType::UInt64;
                        const unsigned __int128 a = loadU128(acc(gr, i));
                        if (a > std::numeric_limits<uint64_t>::max()) {
                            out.present = false; ++stats.aggOverflow;
                        } else {
                            out.u = static_cast<uint64_t>(a);
                        }
                    } else {
                        out.type = PrimType::Float64; out.f = loadF64(acc(gr, i));
                    }
                    break;
                case MergeRule::Avg:
                    out.type = PrimType::Float64;
                    if (nonNull == 0) { out.present = false; break; }
                    if (f.kind == AccKind::Signed)
                        out.f = static_cast<double>(loadI128(acc(gr, i))) / static_cast<double>(nonNull);
                    else if (f.kind == AccKind::Unsigned)
                        out.f = static_cast<double>(loadU128(acc(gr, i))) / static_cast<double>(nonNull);
                    else
                        out.f = loadF64(acc(gr, i)) / static_cast<double>(nonNull);
                    break;
                case MergeRule::Min:
                case MergeRule::Max: {
                    out.type = f.srcType;
                    if (nonNull == 0) { out.present = false; break; }
                    uint8_t* cs = cacheState(gr, f.cache);
                    if (*cs != kValid) {
                        if (*cs != kPending) ++stats.aggInconsistent;
                        rescan(gr, i, f);
                    }
                    if (*cs != kValid) { out.present = false; break; }
                    const uint8_t* cv = cacheVal(gr, f.cache);
                    if (f.kind == AccKind::Signed)        std::memcpy(&out.i, cv, 8);
                    else if (f.kind == AccKind::Unsigned) out.u = loadU64(cv);
                    else                                   out.f = loadF64(cv);
                    break;
                }
                case MergeRule::Last: {
                    const uint8_t* h = headContributor(gr);
                    if (h == nullptr || nullBit(h, agg.contribNullOff, f.input)) {
                        out.type = f.srcType; out.present = false; break;
                    }
                    out = decodeSlotValue(val(h, f.input), f.srcType, agg.inputWidth[f.input]);
                    break;
                }
                case MergeRule::LastNonNull: {
                    const uint8_t* h = nonNull == 0 ? nullptr : firstNonNull(gr, f.input);
                    if (h == nullptr) { out.type = f.srcType; out.present = false; break; }
                    out = decodeSlotValue(val(h, f.input), f.srcType, agg.inputWidth[f.input]);
                    break;
                }
                case MergeRule::Replace:
                    out.present = false; break;   // unreachable: the key column is not an AggField
            }
            writeSlot(row, layout, f.outSlot, out);
        }
    }

    // Retracts contributor `c` (pre-image row `cr`) from its group and, in a
    // linked view, unlinks it. `mode` says what then happens to the group (see
    // RetractMode above).
    uint64_t retract(uint64_t c, const uint8_t* cr, RetractMode mode) noexcept {
        const uint64_t g = loadU64(cr);
        uint8_t* gr = applyContribution(cr, -1);
        if (agg.linked) unlink(c, cr, g, gr);
        if (gr == nullptr) return g;   // inconsistent: counted, nothing else changed
        // Value block: an in-place update keeps its entry (rewritten after the add);
        // everything else leaves the group -- swap-remove BEFORE the emit below rescans.
        if (agg.numBlockCols > 0 && mode != RetractMode::InPlaceLinked) blockRemove(c, cr, gr);
        if (loadU64(gr) == 0) {
            if (mode == RetractMode::Delete) dropGroup(g);
        } else if (mode != RetractMode::InPlaceLinked) {
            emit(g, gr);
        }
        return g;
    }
};

enum class StepKind : uint8_t { None, Malformed, Erased, Upserted, Moved };
struct StepOut {
    StepKind kind = StepKind::None;
    uint64_t group = 0;      // Upserted / Moved: the record's new group
    uint64_t oldGroup = 0;   // Erased / Moved: the group the contributor left
};

inline const rune::codec::Descriptor& writerOf(VersionDescriptorCache& vdc, std::string_view value) {
    const uint16_t writerVersion =
        value.size() >= 4
            ? static_cast<uint16_t>(static_cast<uint8_t>(value[2]) | (static_cast<uint8_t>(value[3]) << 8))
            : vdc.defaultVersion();
    return vdc.materializedFor(writerVersion);
}

// One record against the aggregate tables -- the single body behind the direct fold and the prepare.
template <class T>
StepOut aggStep(Ctx<T>& cx, VersionDescriptorCache& vdc, const rune::codec::Descriptor* writer,
                std::string_view key, std::string_view value) noexcept {
    if (key.size() != cx.agg.sourceKeyWidth) { ++cx.stats.skippedMalformed; return {StepKind::Malformed}; }
    uint64_t c = 0;
    std::memcpy(&c, key.data(), key.size());   // little-endian identity bytes; a narrower integer key zero-extends
    // A linked view's lists end in kNoContributor: that key cannot be linked.
    if (cx.agg.linked && c == kNoContributor) { ++cx.stats.skippedMalformed; return {StepKind::Malformed}; }

    const uint8_t* old = cx.contributors.get(c);
    uint64_t oldGroup = 0;
    if (value.empty()) {               // tombstone: retract a known contributor
        if (old != nullptr) {
            oldGroup = cx.retract(c, old, RetractMode::Delete);
            cx.contributors.erase(c);
        }
        ++cx.stats.deleted;
        return old != nullptr ? StepOut{StepKind::Erased, 0, oldGroup} : StepOut{};
    }

    const rune::codec::Descriptor& d = writer != nullptr ? *writer : writerOf(vdc, value);
    uint64_t g = 0;
    ViewKey groupKey;
    bool hasGroup = false;
    if (cx.keyed != nullptr) {
        const DeriveKey dk = deriveViewKey(*cx.keyed, d, asBytes(value), groupKey);
        if (dk == DeriveKey::Malformed) { ++cx.stats.skippedMalformed; return {StepKind::Malformed}; }
        hasGroup = dk == DeriveKey::Ok;
        if (hasGroup) {
            const uint8_t* idRow = cx.groupIds->get(groupKey);
            g = idRow != nullptr ? loadU64(idRow) : cx.nextGid;
        }
    } else {
        const FieldValue gk = rune::codec::readField(d, asBytes(value), cx.agg.keySrcFieldId);
        hasGroup = gk.present;
        if (hasGroup) {
            if (cx.agg.keyType == PrimType::Int64) std::memcpy(&g, &gk.i, 8); else g = gk.u;
        }
    }
    if (!hasGroup) {
        if (old != nullptr) {
            oldGroup = cx.retract(c, old, RetractMode::Delete);
            cx.contributors.erase(c);
        }
        ++cx.stats.skippedNoGroup;
        return old != nullptr ? StepOut{StepKind::Erased, 0, oldGroup} : StepOut{};
    }
    bool hadOld = false;
    if (old != nullptr) {
        hadOld = true;
        const bool sameGroup = loadU64(old) == g;
        oldGroup = cx.retract(c, old, cx.agg.linked && sameGroup ? RetractMode::InPlaceLinked
                                                               : RetractMode::Move);
    }

    uint8_t* cr = cx.contributors.put(c);   // same row when it already existed
    storeU64(cr, g);
    for (const AggField& f : cx.agg.fields) {
        if (f.input == kNoSourceField) continue;
        FieldValue in = rune::codec::readField(d, asBytes(value), f.srcFieldId);
        if (in.present && f.kind == AccKind::Float && !std::isfinite(in.f)) {
            in.present = false;   // NaN/Inf is treated as NULL (Sum/Avg/Min/Max)
            ++cx.stats.aggNonFinite;
        }
        setNullBit(cr, cx.agg.contribNullOff, f.input, !in.present);
        uint8_t* dst = cr + cx.agg.inputOff[f.input];
        const uint16_t w = cx.agg.inputWidth[f.input];
        if (!in.present)                       { std::memset(dst, 0, w); }
        else if (f.rule == MergeRule::Last || f.rule == MergeRule::LastNonNull)
                                               { encodeSlotValue(dst, f.srcType, w, in); }
        else if (f.kind == AccKind::Signed)    { std::memcpy(dst, &in.i, 8); }
        else if (f.kind == AccKind::Unsigned)  { storeU64(dst, in.u); }
        else if (f.kind == AccKind::Float)     { storeF64(dst, in.f); }
        else                                   { storeU64(dst, 0); }   // Count(col)
    }
    uint8_t* gr = cx.applyContribution(cr, +1);
    if (cx.keyed != nullptr) std::memcpy(gr + cx.agg.groupKeyOff, groupKey.bytes.data(), cx.agg.groupKeyWidth);
    if (cx.agg.linked) cx.linkAtHead(c, cr, gr);
    if (cx.agg.numBlockCols > 0) {          // implies linked: same group == InPlaceLinked
        if (hadOld && oldGroup == g) cx.blockWrite(c, cr, gr);
        else cx.blockAppend(c, cr, g, gr);
    }
    cx.emit(g, gr);                      // a rescan here sees the block's new state
    if (hadOld && oldGroup != g) {
        const uint8_t* og = cx.groups.get(oldGroup);
        if (og != nullptr && loadU64(og) == 0) cx.dropGroup(oldGroup);
    }
    ++cx.stats.applied;
    return {hadOld && oldGroup != g ? StepKind::Moved : StepKind::Upserted, g, oldGroup};
}

// The record's touched rows into the chain, keyed the way apply keys them.  A keyed view's new group takes the next gid (burned when
// the chain is released) and its key goes into the chain's groupIds; a dropped group's key leaves them.
void chainRecord(AggregateFold& f, const RowOverlay& ov, const TxnTable& groups, const TxnTable& groupIds, TxnOverlay& chain) noexcept {
    using T = RowOverlay::Table;
    const uint32_t off = f.agg.groupKeyOff, width = f.agg.groupKeyWidth;
    if (f.keyed != nullptr) {
        TxnTable ids = groupIds;
        for (std::size_t i = 0; i < ov.used(T::Groups); ++i) {
            const RowOverlay::Slot& s = ov.slot(T::Groups, i);
            const uint8_t* stored = groups.get(s.key);
            if (s.erased) {
                if (stored != nullptr) ids.erase(ViewKey::fromBytes({stored + off, width}));
            } else if (stored == nullptr) {
                std::memcpy(ids.put(ViewKey::fromBytes({ov.image(T::Groups, i) + off, width})), &s.key, 8);
                *f.nextGroupId = std::max(*f.nextGroupId, s.key + 1);
            }
        }
    }
    const auto publish = [&](T t, OverlayTable table, std::size_t i, const ViewKey& key) {
        const RowOverlay::Slot& s = ov.slot(t, i);
        if (s.erased) chain.erase(f.member, table, key);
        else if (uint8_t* dst = chain.put(f.member, table, key, nullptr, ov.rowLen(t))) std::memcpy(dst, ov.image(t, i), ov.rowLen(t));
    };
    // The view rows go first, under their view keys (a keyed view's slots are gids): a dropped group's key is read from its stored row.
    for (std::size_t i = 0; i < ov.used(T::View); ++i) {
        const RowOverlay::Slot& s = ov.slot(T::View, i);
        if (f.keyed == nullptr) {
            publish(T::View, OverlayTable::View, i, ViewKey::fromU64(s.key));
            continue;
        }
        const uint8_t* keyed = groups.get(s.key);
        for (std::size_t j = 0; j < ov.used(T::Groups) && !s.erased; ++j)
            if (ov.slot(T::Groups, j).key == s.key && !ov.slot(T::Groups, j).erased) keyed = ov.image(T::Groups, j);
        if (keyed != nullptr) publish(T::View, OverlayTable::View, i, ViewKey::fromBytes({keyed + off, width}));
    }
    for (std::size_t i = 0; i < ov.used(T::Contributors); ++i) publish(T::Contributors, OverlayTable::Contributors, i, ViewKey::fromU64(ov.slot(T::Contributors, i).key));
    for (std::size_t i = 0; i < ov.used(T::Groups); ++i) publish(T::Groups, OverlayTable::Groups, i, ViewKey::fromU64(ov.slot(T::Groups, i).key));
    for (std::size_t i = 0; i < ov.used(T::Blocks); ++i) publish(T::Blocks, OverlayTable::Blocks, i, ViewKey::fromU64(ov.slot(T::Blocks, i).key));
}

} // namespace

FoldStats foldAggregateBatch(const CompiledViewPlan& /*plan*/, const RowLayout& layout,
                             const AggLayout& agg, VersionDescriptorCache& vdc,
                             ViewTable& view, ViewTable& contributors, ViewTable& groups,
                             ViewTable* blocks, const rune::storage::LogReadView& lv,
                             int64_t bound, int64_t& cursor, const FoldSkip& skip, TxnFold* txn) {
    FoldStats stats{};
    DirectTable dBlocks{blocks != nullptr ? *blocks : view};   // unused when blocks == nullptr
    Ctx<DirectTable> cx{layout, agg, DirectTable{view}, DirectTable{contributors}, DirectTable{groups},
                        blocks != nullptr ? &dBlocks : nullptr, stats};
    walkFold(lv, bound, cursor, skip, txn, stats, [&](std::string_view key, std::string_view value, int64_t) {
        (void)aggStep(cx, vdc, nullptr, key, value);
    });
    return stats;
}

FoldStats foldAggregateBatch(const CompiledViewPlan& plan, const RowLayout& layout,
                             const AggLayout& agg, VersionDescriptorCache& vdc,
                             ViewTable& view, ViewTable& contributors, ViewTable& groups,
                             const rune::storage::LogReadView& lv, int64_t bound,
                             int64_t& cursor, const FoldSkip& skip, TxnFold* txn) {
    return foldAggregateBatch(plan, layout, agg, vdc, view, contributors, groups, nullptr, lv,
                              bound, cursor, skip, txn);
}

FoldStats foldKeyedAggregateBatch(AggregateFold& f, Prospect& p, const rune::storage::LogReadView& lv, int64_t bound, int64_t& cursor,
                                  const FoldSkip& skip, TxnFold* txn) {
    FoldStats stats{};
    walkFold(lv, bound, cursor, skip, txn, stats, [&](std::string_view key, std::string_view value, int64_t offset) {
        const SourceRecord rec{key, value, offset, 0, nullptr};
        (void)prepareAggregate(f, rec, p);   // a malformed record leaves op None: apply does nothing and the stats say why
        applyAggregate(f, rec, p);
        stats.applied += p.stats.applied;
        stats.deleted += p.stats.deleted;
        stats.skippedMalformed += p.stats.skippedMalformed;
        stats.skippedNoGroup += p.stats.skippedNoGroup;
        stats.aggOverflow += p.stats.aggOverflow;
        stats.aggNonFinite += p.stats.aggNonFinite;
        stats.aggInconsistent += p.stats.aggInconsistent;
        stats.aggRescans += p.stats.aggRescans;
    });
    return stats;
}

PrepareStatus prepareAggregate(AggregateFold& f, const SourceRecord& rec, Prospect& p, TxnOverlay* chain) noexcept {
    p.clear();
    RowOverlay& ov = p.overlay;
    using T = RowOverlay::Table;
    if (chain != nullptr && !rowsFit(*chain, {&f.view, &f.contributors, &f.groups, f.blocks, f.groupIds})) return PrepareStatus::CapacityExhausted;
    TxnAccess x{chain, f.locks, f.member};
    TxnAccess* const xp = chain != nullptr || f.locks != nullptr ? &x : nullptr;
    const TxnTable groupsT(&f.groups, OverlayTable::Groups, xp);
    const TxnTable groupIdsT(f.groupIds, OverlayTable::GroupIds, xp);
    ProspectTable blocks{ov, T::Blocks, TxnTable(f.blocks, OverlayTable::Blocks, xp)};
    Ctx<ProspectTable> cx{f.layout, f.agg, ProspectTable{ov, T::View, TxnTable(f.keyed != nullptr ? nullptr : &f.view, OverlayTable::View, f.keyed != nullptr ? nullptr : xp)},
                         ProspectTable{ov, T::Contributors, TxnTable(&f.contributors, OverlayTable::Contributors, xp)},
                         ProspectTable{ov, T::Groups, groupsT}, f.blocks != nullptr ? &blocks : nullptr, p.stats,
                         f.keyed, f.groupIds != nullptr ? &groupIdsT : nullptr, f.nextGroupId != nullptr ? *f.nextGroupId : 0};
    const StepOut o = aggStep(cx, f.vdc, rec.writer, rec.key, rec.value);
    if (x.locked) return PrepareStatus::Locked;
    if (ov.full() || (chain != nullptr && chain->full())) return PrepareStatus::CapacityExhausted;   // a dropped absent-read lock is as bad as a dropped write
    if (o.kind == StepKind::Malformed) return PrepareStatus::Malformed;
    if (o.kind == StepKind::None) return PrepareStatus::Ok;
    const auto viewRow = [&](ProspectRow& row, uint64_t g) {
        if (f.keyed != nullptr) {   // the view key is the group row's key bytes: the overlay's image, else the stored row (a dropped group)
            const RowOverlay::Slot* gs = ov.find(T::Groups, g);
            const uint8_t* grow = gs != nullptr && !gs->erased ? ov.imageOf(T::Groups, gs) : groupsT.get(g);
            row.key = grow != nullptr ? ViewKey::fromBytes({grow + f.agg.groupKeyOff, f.agg.groupKeyWidth}) : ViewKey{};
        } else {
            row.key = ViewKey::fromU64(g);
        }
        bool chainErased = false;
        const uint8_t* held = chain != nullptr && row.key.len != 0 ? chain->get(f.member, OverlayTable::View, row.key, chainErased) : nullptr;
        const uint8_t* cur = held != nullptr ? held : chainErased ? nullptr : f.view.get(row.key);
        row.existed = cur != nullptr;
        if (chain != nullptr && cur != nullptr) {   // F11: chainRecord overwrites a held image in place
            uint8_t* b = p.before.data() + static_cast<std::size_t>(&row - p.rows.data()) * p.rowLen;
            std::memcpy(b, cur, p.rowLen);
            row.before = b;
        }
        const RowOverlay::Slot* s = ov.find(T::View, g);
        row.removed = s == nullptr || s->erased;
        row.image   = row.removed ? nullptr : ov.imageOf(T::View, s);
    };
    switch (o.kind) {
        case StepKind::Erased:   p.op = ProspectOp::Erase;  viewRow(p.rows[0], o.oldGroup); p.rowCount = 1; break;
        case StepKind::Upserted: p.op = ProspectOp::Upsert; viewRow(p.rows[0], o.group);    p.rowCount = 1; break;
        default:
            p.op = ProspectOp::Move;
            viewRow(p.rows[0], o.group);
            viewRow(p.rows[1], o.oldGroup);
            p.rowCount = 2;
            break;
    }
    if (chain != nullptr) {
        p.chained = true;
        chainRecord(f, ov, groupsT, groupIdsT, *chain);
        if (x.locked) return PrepareStatus::Locked;
        if (chain->full()) return PrepareStatus::CapacityExhausted;
    }
    return PrepareStatus::Ok;
}

// A keyed record's rows: the view rows are written under their view keys, a created group registers its key in groupIds and a
// dropped one removes it.  The view goes first -- a dropped group's key is read from its stored row.
static void applyKeyedAggregate(AggregateFold& f, const Prospect& p) noexcept {
    using T = RowOverlay::Table;
    const RowOverlay& ov = p.overlay;
    const uint32_t off = f.agg.groupKeyOff, width = f.agg.groupKeyWidth;
    for (std::size_t i = 0; i < ov.used(T::View); ++i) {
        const RowOverlay::Slot& s = ov.slot(T::View, i);
        const uint8_t* keyed = f.groups.get(s.key);   // the stored group row when it is erased or untouched
        for (std::size_t j = 0; j < ov.used(T::Groups) && !s.erased; ++j)
            if (ov.slot(T::Groups, j).key == s.key && !ov.slot(T::Groups, j).erased) keyed = ov.image(T::Groups, j);
        if (keyed == nullptr) continue;
        const ViewKey k = ViewKey::fromBytes({keyed + off, width});
        if (s.erased) f.view.erase(k);
        else std::memcpy(f.view.put(k), ov.image(T::View, i), ov.rowLen(T::View));
    }
    for (std::size_t i = 0; i < ov.used(T::Contributors); ++i) {
        const RowOverlay::Slot& s = ov.slot(T::Contributors, i);
        if (s.erased) f.contributors.erase(s.key);
        else std::memcpy(f.contributors.put(s.key), ov.image(T::Contributors, i), ov.rowLen(T::Contributors));
    }
    for (std::size_t i = 0; i < ov.used(T::Groups); ++i) {
        const RowOverlay::Slot& s = ov.slot(T::Groups, i);
        const uint8_t* stored = f.groups.get(s.key);
        if (s.erased) {
            if (stored != nullptr) f.groupIds->erase(ViewKey::fromBytes({stored + off, width}));
            f.groups.erase(s.key);
            continue;
        }
        if (stored == nullptr) {
            std::memcpy(f.groupIds->put(ViewKey::fromBytes({ov.image(T::Groups, i) + off, width})), &s.key, 8);
            *f.nextGroupId = std::max(*f.nextGroupId, s.key + 1);
        }
        std::memcpy(f.groups.put(s.key), ov.image(T::Groups, i), ov.rowLen(T::Groups));
    }
    if (f.blocks == nullptr) return;
    for (std::size_t i = 0; i < ov.used(T::Blocks); ++i) {
        const RowOverlay::Slot& s = ov.slot(T::Blocks, i);
        if (s.erased) f.blocks->erase(s.key);
        else std::memcpy(f.blocks->put(s.key), ov.image(T::Blocks, i), ov.rowLen(T::Blocks));
    }
}

void applyAggregate(AggregateFold& f, const SourceRecord&, const Prospect& p) noexcept {
    using T = RowOverlay::Table;
    if (f.keyed != nullptr) return applyKeyedAggregate(f, p);
    ViewTable* const tables[RowOverlay::kTables] = {&f.view, &f.contributors, &f.groups, f.blocks};
    // The order the direct step writes: contributors, groups, blocks, then the view.
    for (const T t : {T::Contributors, T::Groups, T::Blocks, T::View}) {
        ViewTable* tbl = tables[static_cast<std::size_t>(t)];
        if (tbl == nullptr) continue;
        for (std::size_t i = 0; i < p.overlay.used(t); ++i) {
            const RowOverlay::Slot& s = p.overlay.slot(t, i);
            if (s.erased) tbl->erase(s.key);
            else std::memcpy(tbl->put(s.key), p.overlay.image(t, i), p.overlay.rowLen(t));
        }
    }
}

} // namespace rune::view
