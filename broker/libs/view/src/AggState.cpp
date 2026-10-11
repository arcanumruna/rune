// libs/view/src/AggState.cpp — see AggState.h.
#include <rune/view/AggState.h>

#include <algorithm>

namespace rune::view {

namespace {

std::optional<rune::codec::PrimType> srcType(const rune::codec::Descriptor& d, uint16_t id) {
    for (const auto& f : d.fields) {
        if (f.id == id) return f.type;
    }
    return std::nullopt;
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

} // namespace

bool isAggregateView(const CompiledViewPlan& plan) noexcept {
    return std::any_of(plan.output.begin(), plan.output.end(),
                       [](const CompiledField& f) { return f.rule != MergeRule::Replace; });
}

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
        if (f.rule != MergeRule::CountAll) {
            const auto t = srcType(evolvingSrc, f.srcFieldId);
            if (!t) return std::nullopt;
            af.input = a.numInputs++;
            if (f.rule == MergeRule::Sum || f.rule == MergeRule::Avg) af.kind = accKindOf(*t);
        }
        a.fields.push_back(af);
    }
    a.contribNullOff        = 8;
    a.contribValOff         = 8 + (static_cast<uint32_t>(a.numInputs) + 7) / 8;
    a.contributorRow.rowLen = a.contribValOff + 8u * a.numInputs;
    a.groupAccOff           = 8;
    a.groupNonNullOff       = 8 + 16u * static_cast<uint32_t>(a.fields.size());
    a.groupRow.rowLen       = a.groupNonNullOff + 8u * static_cast<uint32_t>(a.fields.size());
    return a;
}

} // namespace rune::view
