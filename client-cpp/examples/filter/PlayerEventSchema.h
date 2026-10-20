#pragma once
// The player_event schema shared by the client tests, the broker slice (FilterBrokerRegressionTest) and
// filter_probe (docker/filter-e2e.sh) — ONE descriptor, so the generated header's kFingerprint matches the
// topic the probe defines.  codec/tools/make_player_event_blob.cpp and codec/tests/FilterTestSupport.h
// (filtertest::playerEvent(1)) describe the same v1 schema; a pinned literal fingerprint in the codec and the
// client tests keeps all three equal.
#include <rune/codec/Descriptor.h>
namespace rune::examples {
inline rune::codec::Descriptor playerEventDescriptor() {
    using rune::codec::DescriptorBuilder; using rune::codec::PrimType;
    return DescriptorBuilder("player_event")
        .addTagged("playerId", 0, PrimType::UInt64, 1, 0xFFFF, /*isKey=*/true, {})
        .addTagged("worldId", 1, PrimType::UInt32, 1, 0xFFFF, false, {})
        .addTagged("channel", 2, PrimType::Int32, 1, 0xFFFF, false, {})
        .addFixedStrTagged("zone", 3, 8, 1, 0xFFFF, false)
        .addTagged("kind", 4, PrimType::Enum, 1, 0xFFFF, false, {1, 2, 3})
        .addTagged("alive", 5, PrimType::Bool, 1, 0xFFFF, false, {})
        .addTagged("hp", 6, PrimType::Int64, 1, 0xFFFF, false, {})
        .addTagged("name", 7, PrimType::String, 1, 0xFFFF, false, {})
        .addTagged("pos", 8, PrimType::Float64, 1, 0xFFFF, false, {})
        .build();
}
}  // namespace rune::examples
