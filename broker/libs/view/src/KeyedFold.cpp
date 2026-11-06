// libs/view/src/KeyedFold.cpp
#include <rune/view/KeyedFold.h>
#include <rune/codec/Record.h>
#include <rune/codec/ViewKeyEncoding.h>

namespace rune::view {

std::optional<KeyedFoldSpec> buildKeyedFoldSpec(const CompiledViewPlan& plan, const rune::codec::Descriptor& evolvingSrc) {
    if (keyModeOf(plan) != ViewKeyMode::Keyed || plan.keyFieldId.empty()) return std::nullopt;
    const auto fieldOf = [&](uint16_t id) -> const rune::codec::FieldDescriptor* {
        for (const auto& f : evolvingSrc.fields) {
            if (f.id == id) return &f;
        }
        return nullptr;
    };
    const auto* wireKey = fieldOf(plan.keyFieldId[0]);
    if (!wireKey) return std::nullopt;

    KeyedFoldSpec spec;
    uint16_t offset = 0;
    for (const CompiledKeyComponent& c : plan.keyComponents) {
        if (!fieldOf(c.srcFieldId)) return std::nullopt;
        spec.components.push_back({c.srcFieldId, c.type, c.keyFunction, c.width, offset, c.outSlot});
        offset = static_cast<uint16_t>(offset + c.width);
    }
    spec.keyWidth = offset;
    spec.prefixWidth = prefixWidthOf(plan);
    spec.wireKeyWidth = static_cast<uint8_t>(rune::codec::fieldWidth(*wireKey));
    spec.sourceKeyed = plan.keyComponents.size() == 1 && plan.keyComponents[0].keyFunction == 0 && plan.keyComponents[0].srcFieldId == plan.keyFieldId[0];
    spec.ownerMapped = ownerMappedOf(plan);
    return spec;
}

DeriveKey deriveViewKey(const KeyedFoldSpec& spec, const rune::codec::Descriptor& writer, std::span<const uint8_t> value, ViewKey& out) noexcept {
    for (const auto& c : spec.components) {
        const rune::codec::FieldValue v = rune::codec::readField(writer, value, c.srcFieldId);
        if (!v.present) return DeriveKey::Absent;
        if (!rune::codec::encodeKeyComponent(c.type, c.keyFunction, v, std::span<uint8_t>(out.bytes.data() + c.offset, c.width))) return DeriveKey::Malformed;
    }
    out.len = static_cast<uint8_t>(spec.keyWidth);
    return DeriveKey::Ok;
}

} // namespace rune::view
