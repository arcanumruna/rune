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

struct Ctx {
    const RowLayout& layout;
    const AggLayout& agg;
    ViewTable& view;
    ViewTable& contributors;
    ViewTable& groups;
    FoldStats& stats;

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
                else { ++stats.aggInconsistent; *cs = kPending; }   // values without a cache: rebuild from the list
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

    // Rebuilds Min/Max field i's cache by ONE walk of the group's list, bounded
    // by its contributor count (a cycle or over-long list stops, counted).
    void rescan(uint8_t* gr, size_t i, const AggField& f) noexcept {
        ++stats.aggRescans;
        uint8_t* cv = cacheVal(gr, f.cache);
        uint8_t* cs = cacheState(gr, f.cache);
        const uint64_t n = loadU64(gr);
        const uint64_t nonNull = loadU64(nn(gr, i));
        uint64_t k = loadU64(gr + agg.groupHeadOff);
        uint64_t steps = 0, seen = 0;
        bool have = false, bad = false;
        while (k != kNoContributor) {
            if (steps == n) { bad = true; break; }
            const uint8_t* r = contributors.get(k);
            if (r == nullptr) { bad = true; break; }
            ++steps;
            if (!nullBit(r, agg.contribNullOff, f.input)) {
                const uint8_t* v = val(r, f.input);
                if (!have || strictlyBetter(f.rule, f.kind, v, cv)) { std::memcpy(cv, v, 8); have = true; }
                ++seen;
            }
            k = loadU64(r + agg.contribNextOff);
        }
        if (bad || steps != n || seen != nonNull) ++stats.aggInconsistent;
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
        uint8_t* row = view.put(g);
        FieldValue k{};
        k.type = agg.keyType;
        k.present = true;
        if (agg.keyType == PrimType::Int64) { int64_t s; std::memcpy(&s, &g, 8); k.i = s; }
        else { k.u = g; }
        writeSlot(row, layout, agg.keyOutSlot, k);

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
        if (loadU64(gr) == 0) {
            if (mode == RetractMode::Delete) dropGroup(g);
        } else if (mode != RetractMode::InPlaceLinked) {
            emit(g, gr);
        }
        return g;
    }
};

} // namespace

FoldStats foldAggregateBatch(const CompiledViewPlan& /*plan*/, const RowLayout& layout,
                             const AggLayout& agg, VersionDescriptorCache& vdc,
                             ViewTable& view, ViewTable& contributors, ViewTable& groups,
                             const rune::storage::LogReadView& lv, int64_t bound,
                             int64_t& cursor) {
    FoldStats stats{};
    Ctx cx{layout, agg, view, contributors, groups, stats};
    const int64_t from = cursor;   // records below it were folded by a prior (mid-batch) pass
    bool stopped = false;
    bool corrupt = false;
    int64_t walkedUpto = lv.baseOffset;

    size_t pos = 0;
    while (pos < lv.len) {
        rune::protocol::RecordBatchReader r(lv.data + pos, lv.len - pos);
        if (!r.ensureValid()) { corrupt = true; break; }
        if (r.isControl()) {
            ++stats.skippedControl;
            pos += static_cast<size_t>(r.totalLength());
            walkedUpto = r.baseOffset() + r.lastOffsetDelta() + 1;
            continue;
        }
        r.forEachRecord([&](std::string_view key, std::string_view value, int64_t offset,
                            int64_t /*ts*/) {
            if (stopped) return;
            if (offset >= bound) { stopped = true; return; }
            if (offset < from) return;         // already folded (resume mid-batch)
            if (key.size() != 8) { ++stats.skippedMalformed; return; }
            uint64_t c = 0;
            std::memcpy(&c, key.data(), 8);   // little-endian identity bytes
            // A linked view's lists end in kNoContributor: that key cannot be linked.
            if (agg.linked && c == kNoContributor) { ++stats.skippedMalformed; return; }

            const uint8_t* old = contributors.get(c);
            if (value.empty()) {               // tombstone: retract a known contributor
                if (old != nullptr) {
                    (void)cx.retract(c, old, RetractMode::Delete);
                    contributors.erase(c);
                }
                ++stats.deleted;
                return;
            }

            const uint16_t writerVersion =
                value.size() >= 4
                    ? static_cast<uint16_t>(static_cast<uint8_t>(value[2]) |
                                            (static_cast<uint8_t>(value[3]) << 8))
                    : vdc.defaultVersion();
            const rune::codec::Descriptor& d = vdc.materializedFor(writerVersion);
            const FieldValue gk = rune::codec::readField(d, asBytes(value), agg.keySrcFieldId);
            if (!gk.present) {
                if (old != nullptr) {
                    (void)cx.retract(c, old, RetractMode::Delete);
                    contributors.erase(c);
                }
                ++stats.skippedNoGroup;
                return;
            }
            uint64_t g = 0;
            if (agg.keyType == PrimType::Int64) std::memcpy(&g, &gk.i, 8); else g = gk.u;

            bool hadOld = false;
            uint64_t oldGroup = 0;
            if (old != nullptr) {
                hadOld = true;
                const bool sameGroup = loadU64(old) == g;
                oldGroup = cx.retract(c, old, agg.linked && sameGroup ? RetractMode::InPlaceLinked
                                                                       : RetractMode::Move);
            }

            uint8_t* cr = contributors.put(c);   // same row when it already existed
            storeU64(cr, g);
            for (const AggField& f : agg.fields) {
                if (f.input == kNoSourceField) continue;
                FieldValue in = rune::codec::readField(d, asBytes(value), f.srcFieldId);
                if (in.present && f.kind == AccKind::Float && !std::isfinite(in.f)) {
                    in.present = false;   // NaN/Inf is treated as NULL (Sum/Avg/Min/Max)
                    ++stats.aggNonFinite;
                }
                setNullBit(cr, agg.contribNullOff, f.input, !in.present);
                uint8_t* dst = cr + agg.inputOff[f.input];
                const uint16_t w = agg.inputWidth[f.input];
                if (!in.present)                       { std::memset(dst, 0, w); }
                else if (f.rule == MergeRule::Last || f.rule == MergeRule::LastNonNull)
                                                       { encodeSlotValue(dst, f.srcType, w, in); }
                else if (f.kind == AccKind::Signed)    { std::memcpy(dst, &in.i, 8); }
                else if (f.kind == AccKind::Unsigned)  { storeU64(dst, in.u); }
                else if (f.kind == AccKind::Float)     { storeF64(dst, in.f); }
                else                                   { storeU64(dst, 0); }   // Count(col)
            }
            uint8_t* gr = cx.applyContribution(cr, +1);
            if (agg.linked) cx.linkAtHead(c, cr, gr);
            cx.emit(g, gr);
            if (hadOld && oldGroup != g) {
                const uint8_t* og = groups.get(oldGroup);
                if (og != nullptr && loadU64(og) == 0) cx.dropGroup(oldGroup);
            }
            ++stats.applied;
        });
        if (stopped) break;
        pos += static_cast<size_t>(r.totalLength());
        walkedUpto = r.baseOffset() + r.lastOffsetDelta() + 1;
    }
    // Never move the resume point backwards (a corrupt batch at/after `from`).
    cursor = std::max(from, corrupt ? walkedUpto : std::min(lv.nextOffset, bound));
    return stats;
}

} // namespace rune::view
