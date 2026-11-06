// libs/view/include/rune/view/KeyedFold.h
//
// How a keyed view derives one view key from a source record: built once at install, read on the fold's hot path.
#pragma once
#include <cstdint>
#include <optional>
#include <span>
#include <vector>
#include <rune/codec/Descriptor.h>
#include <rune/codec/PrimType.h>
#include <rune/view/ViewKey.h>
#include <rune/view/ViewPlan.h>

namespace rune::view {

struct KeyedFoldSpec {
    struct Component {
        uint16_t srcFieldId;
        rune::codec::PrimType type;
        uint8_t keyFunction;
        uint16_t width;
        uint16_t offset;   // where the component's encoded bytes start in the key
        uint16_t outSlot;
    };
    std::vector<Component> components;
    uint16_t keyWidth = 0;
    uint16_t prefixWidth = 0;
    uint8_t  wireKeyWidth = 0;     // the source key field's width (owner-table key)
    bool     sourceKeyed = false;  // exactly one plain component on the source key field
    bool     ownerMapped = false;
};

// nullopt for a LegacyU64 plan or a key field missing from the source's evolving descriptor.
[[nodiscard]] std::optional<KeyedFoldSpec> buildKeyedFoldSpec(const CompiledViewPlan& plan, const rune::codec::Descriptor& evolvingSrc);

enum class DeriveKey : uint8_t { Ok, Absent, Malformed };

// Absent: a component field does not exist in the writer's version. Malformed: a component does not encode.
[[nodiscard]] DeriveKey deriveViewKey(const KeyedFoldSpec& spec, const rune::codec::Descriptor& writer,
                                      std::span<const uint8_t> value, ViewKey& out) noexcept;

} // namespace rune::view
