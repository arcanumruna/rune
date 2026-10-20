#pragma once
// Shared by FilterSchemaTest / FilterPredicateTest: the player_event fixture (Phase 2c Task 3).
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Flyweight.h>

#include <cstdint>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace filtertest {
using namespace rune::codec;

// tags: playerId 0 u64, worldId 1 u32, channel 2 i32, zone 3 str(8), kind 4 enum, alive 5 bool, hp 6 i64,
//       name 7 string, pos 8 f64; v2 ADD guild u32 (tag 9); v3 DROP channel.
inline Descriptor playerEvent(uint16_t uptoVersion = 3) {
    DescriptorBuilder b("player_event");
    b.addTagged("playerId", 0, PrimType::UInt64, 1, 0xFFFF, true, {});
    b.addTagged("worldId", 1, PrimType::UInt32, 1, 0xFFFF, false, {});
    b.addTagged("channel", 2, PrimType::Int32, 1, 0xFFFF, false, {});
    b.addFixedStrTagged("zone", 3, 8, 1, 0xFFFF, false);
    b.addTagged("kind", 4, PrimType::Enum, 1, 0xFFFF, false, {1, 2, 3});
    b.addTagged("alive", 5, PrimType::Bool, 1, 0xFFFF, false, {});
    b.addTagged("hp", 6, PrimType::Int64, 1, 0xFFFF, false, {});
    b.addTagged("name", 7, PrimType::String, 1, 0xFFFF, false, {});
    b.addTagged("pos", 8, PrimType::Float64, 1, 0xFFFF, false, {});
    Descriptor d = b.build();
    uint16_t nextTag = 9;
    if (uptoVersion >= 2) {
        auto r = applyOps(d, {{AlterOpType::Add, "guild", "", PrimType::UInt32, 0, {}}}, nextTag, 2);
        if (!r) throw std::runtime_error("add guild");
        d = *r;
    }
    if (uptoVersion >= 3) {
        auto r = applyOps(d, {{AlterOpType::Drop, "channel", "", PrimType::Int32, 0, {}}}, nextTag, 3);
        if (!r) throw std::runtime_error("drop channel");
        d = *r;
    }
    return d;
}

struct Vals { uint64_t playerId = 0; uint32_t worldId = 0; int32_t channel = 0; std::string_view zone; int32_t kind = 1;
              bool alive = false; int64_t hp = 0; uint32_t guild = 0; };

/// [blockLength u16][version u16][fixed block] at the materialized offsets of `version`; fields absent there are skipped.
inline std::vector<uint8_t> encodeRecord(const Descriptor& evolving, uint16_t version, const Vals& v) {
    const Descriptor m = materializeAsOf(evolving, version);
    std::vector<uint8_t> out(4 + m.blockLength, 0);
    fly::store<uint16_t>(out.data(), static_cast<uint16_t>(m.blockLength));
    fly::store<uint16_t>(out.data() + 2, version);
    uint8_t* blk = out.data() + 4;
    auto at = [&](const char* n) -> uint8_t* { const auto* f = m.find(n); return f ? blk + f->offset : nullptr; };
    if (auto* p = at("playerId")) fly::store<uint64_t>(p, v.playerId);
    if (auto* p = at("worldId")) fly::store<uint32_t>(p, v.worldId);
    if (auto* p = at("channel")) fly::store<int32_t>(p, v.channel);
    if (auto* p = at("zone")) fly::storeFixedStr(p, v.zone, 8);
    if (auto* p = at("kind")) fly::store<int32_t>(p, v.kind);
    if (auto* p = at("alive")) fly::store<bool>(p, v.alive);
    if (auto* p = at("hp")) fly::store<int64_t>(p, v.hp);
    if (auto* p = at("guild")) fly::store<uint32_t>(p, v.guild);
    return out;
}
}  // namespace filtertest
