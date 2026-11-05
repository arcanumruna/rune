// libs/view/include/rune/view/ViewPlan.h
#pragma once
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
};

// Flat little-endian (de)serialization. Both forms lead with a 1-byte
// VERSION for forward-compat and length-prefix strings/vectors with a u32
// count. Deserialization bounds-checks every read and returns std::nullopt
// on any truncation/overrun/bad-version -- never reads out of bounds.
std::vector<uint8_t> serialize(const CompiledViewPlan& plan);
std::optional<CompiledViewPlan> deserializeCompiledViewPlan(std::span<const uint8_t> buf);

} // namespace rune::view
