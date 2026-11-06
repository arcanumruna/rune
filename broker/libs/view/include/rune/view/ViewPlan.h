// libs/view/include/rune/view/ViewPlan.h
#pragma once
#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>
#include <rune/codec/PrimType.h>
#include <rune/codec/ViewPlanAuthored.h>
#include <rune/view/ViewTypes.h>

namespace rune::view {

// One resolved output field: field ids instead of names, output slot assigned.
struct CompiledField {
    uint16_t outSlot;
    rune::codec::PrimType type;
    uint16_t sourceIdx;
    uint16_t srcFieldId;
    MergeRule rule;
};

// One compiled key component. `type` is the source column type (FixedStr for lower()), `width` the encoded bytes,
// `outSlot` the view's key column; a lower() key column carries kDerivedKeyField as its srcFieldId.
struct CompiledKeyComponent {
    uint16_t srcFieldId;
    uint8_t keyFunction;
    uint16_t width;
    uint16_t outSlot;
    rune::codec::PrimType type;
};

// One resolved predicate node: a Column node carries the output slot it reads.
struct CompiledPredicateNode {
    PredicateOp op;
    uint16_t a = 0, b = 0;
    uint16_t outSlot = 0;
    rune::codec::PrimType constType{};
    std::array<uint8_t, 16> constBytes{};
};

struct CompiledConstraint {
    uint16_t ordinal;   // 1..n in declaration order
    std::string name;
    std::vector<CompiledPredicateNode> predicate;
};

// The compiled form of a view definition: names resolved to ids, ready for
// the admin->worker fan-out ring and the durable `__views` record. Note
// `keyFieldId` is PER-SOURCE (one entry per `sourceTopicId`/`SourceSpec`).
struct CompiledViewPlan {
    std::string viewName;
    int32_t viewTopicId;
    std::vector<uint16_t> keyFieldId;
    std::vector<int32_t> sourceTopicId;
    uint64_t outputFingerprint;
    std::vector<CompiledField> output;
    uint8_t persistence = 0;  // 0 = persisted, 1 = transient
    ViewSizing sizing;
    // v2. A v1 blob decodes with one synthesized component over keyFieldId[0].
    std::vector<CompiledKeyComponent> keyComponents;
    uint16_t keyWidth = 8;
    bool unique = false;
    std::vector<CompiledConstraint> constraints;
    uint32_t unicodeVersion = 0;
    uint64_t viewObjectId = 0;
};

// How a view's table is keyed: the uint64_t table every view has today, or a ViewKey table of keyWidth bytes.
enum class ViewKeyMode : uint8_t { LegacyU64, Keyed };

[[nodiscard]] ViewKeyMode keyModeOf(const CompiledViewPlan& plan) noexcept;
// A keyed projection whose key is not exactly the source key keeps an owner map.
[[nodiscard]] bool ownerMappedOf(const CompiledViewPlan& plan) noexcept;
// Encoded width of the first n-1 key components when n >= 2, else 0: the width of the prefix index.
[[nodiscard]] uint16_t prefixWidthOf(const CompiledViewPlan& plan) noexcept;
// True when the plan cannot be expressed in the v1 layout (the synthesized component does not count).
[[nodiscard]] bool needsV2(const CompiledViewPlan& plan) noexcept;

// Flat little-endian (de)serialization. Both forms lead with a 1-byte
// VERSION for forward-compat and length-prefix strings/vectors with a u32
// count. serialize writes v1 whenever needsV2 is false. Deserialization
// bounds-checks every read and returns std::nullopt on any truncation/
// overrun/bad-version/trailing byte -- never reads out of bounds.
std::vector<uint8_t> serialize(const CompiledViewPlan& plan);
std::optional<CompiledViewPlan> deserializeCompiledViewPlan(std::span<const uint8_t> buf);

} // namespace rune::view
