// broker/libs/view/src/AggState.cpp — see AggState.h.
#include <rune/view/AggState.h>

#include <algorithm>

namespace rune::view {

namespace {

const rune::codec::FieldDescriptor* srcField(const rune::codec::Descriptor& d, uint16_t id) {
    for (const auto& f : d.fields) {
        if (f.id == id) return &f;
    }
    return nullptr;
}

AccKind accKindOf(rune::codec::PrimType t) noexcept {
    using P = rune::codec::PrimType;
    switch (t) {
        case P::Int32: case P::Int64:                   return AccKind::Signed;
        case P::UInt16: case P::UInt32: case P::UInt64: return AccKind::Unsigned;
        case P::Float64:                                return AccKind::Float;
        default:                                        return AccKind::None;
    }
}

bool isLastRule(MergeRule r) noexcept { return r == MergeRule::Last || r == MergeRule::LastNonNull; }
bool isExtremeRule(MergeRule r) noexcept { return r == MergeRule::Min || r == MergeRule::Max; }

} // namespace

bool isAggregateView(const CompiledViewPlan& plan) noexcept {
    return std::any_of(plan.output.begin(), plan.output.end(),
                       [](const CompiledField& f) { return f.rule != MergeRule::Replace; });
}

bool needsContributorList(MergeRule r) noexcept { return isExtremeRule(r) || isLastRule(r); }

std::optional<AggLayout> buildAggLayout(const CompiledViewPlan& plan,
                                        const rune::codec::Descriptor& evolvingSrc) {
    AggLayout a;
    for (const auto& f : plan.output) {
        if (f.rule == MergeRule::Replace) {   // compiler: exactly the group-by column
            a.keyOutSlot    = f.outSlot;
            a.keySrcFieldId = f.srcFieldId;
            a.keyType       = f.type;
            continue;
        }
        AggField af{};
        af.outSlot    = f.outSlot;
        af.rule       = f.rule;
        af.srcFieldId = f.srcFieldId;
        af.kind       = AccKind::None;
        af.input      = kNoSourceField;
        af.cache      = kNoCache;
        af.srcType    = f.type;
        if (f.rule != MergeRule::CountAll) {
            const rune::codec::FieldDescriptor* sd = srcField(evolvingSrc, f.srcFieldId);
            if (sd == nullptr) return std::nullopt;
            af.input   = a.numInputs++;
            af.srcType = sd->type;
            a.inputWidth.push_back(isLastRule(f.rule)
                                       ? static_cast<uint16_t>(rune::codec::fieldWidth(*sd))
                                       : uint16_t{8});
            if (f.rule == MergeRule::Sum || f.rule == MergeRule::Avg || isExtremeRule(f.rule))
                af.kind = accKindOf(sd->type);
            if (isExtremeRule(f.rule)) af.cache = a.numCached++;
            if (needsContributorList(f.rule)) a.linked = true;
        }
        a.fields.push_back(af);
    }
    // Task 5b: one value-block column per DISTINCT Min/Max source field, in
    // cache order (Min(x) and Max(x) read the same widened value and null bit).
    {
        std::vector<uint16_t> colSrc;
        for (const AggField& f : a.fields) {
            if (f.cache == kNoCache) continue;
            uint16_t col = 0;
            while (col < colSrc.size() && colSrc[col] != f.srcFieldId) ++col;
            if (col == colSrc.size()) {
                colSrc.push_back(f.srcFieldId);
                a.blockColInput.push_back(f.input);
            }
            a.cacheCol.push_back(col);
        }
        a.numBlockCols = static_cast<uint16_t>(colSrc.size());
    }
    const bool block = a.numBlockCols > 0;
    const uint32_t nFields = static_cast<uint32_t>(a.fields.size());
    a.contribPrevOff = a.linked ? 8u : 0u;
    a.contribNextOff = a.linked ? 16u : 0u;
    a.contribNullOff = a.linked ? 24u : 8u;
    a.contribValOff  = a.contribNullOff + (static_cast<uint32_t>(a.numInputs) + 7) / 8;
    uint32_t off = a.contribValOff;
    a.inputOff.reserve(a.inputWidth.size());
    for (const uint16_t w : a.inputWidth) {
        a.inputOff.push_back(off);
        off += w;
    }
    if (block) {
        a.contribBlockPosOff = off;
        off += 4;
    }
    a.contributorRow.rowLen = off;
    a.groupAccOff     = 8;
    a.groupNonNullOff = 8 + 16u * nFields;
    const uint32_t nnEnd = a.groupNonNullOff + 8u * nFields;
    if (a.linked) {
        a.groupHeadOff      = nnEnd;
        a.groupCacheOff     = nnEnd + 8;
        a.groupRow.rowLen   = a.groupCacheOff + kCacheEntryBytes * a.numCached;
        if (block) {
            a.groupBlockOff   = a.groupRow.rowLen;
            a.groupRow.rowLen += 8;
        }
    } else {
        a.groupRow.rowLen   = nnEnd;
    }
    a.blockRow.rowLen = block ? kBlockEntries * (8u + 9u * a.numBlockCols) : 0u;
    return a;
}

AggDirtyBound aggDirtyBoundPerRecord(const AggLayout& a) noexcept {
    const bool block = a.numBlockCols > 0;   // implies linked
    return AggDirtyBound{a.linked ? (block ? 5u : 4u) : 1u, 2u, 2u, block ? 3u : 0u};
}

} // namespace rune::view
