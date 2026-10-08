#pragma once
// MetadataExtensions.h — D15: broker-owned additions to the __cluster_metadata
// record set that stay readable by OLDER binaries (rolling upgrades).
//
//   1. FeatureLevelRecord (recordType 0x8001, broker-owned id space) — the cluster's committed metadata
//      feature level (KRaft metadata.version).  An older MSM skips an unknown
//      record type deterministically (MetadataStateMachine::apply default).
//   2. Leaderless mark — a fixed-size TRAILER appended after an
//      UpdatePartitionAssignment record that makes a partition leaderless,
//      naming WHY (an empty memory-backed leader, an offline log dir) and the
//      replica / process incarnation it is about.  The rune-protocol decoder
//      ignores bytes past the replicas array (recordVersion 0 only — a later
//      protocol version may append its own fields), so an older MSM installs the
//      same assignment; a newer one also installs the mark, which a successor
//      controller re-derives its leaderless decisions from (it survives a
//      controller failover — it is replicated state, not controller memory).
//
// Wire (little-endian, like every metadata record):
//   FeatureLevelRecord: [0..1] type=0x8001  [2] version=0  [3..4] level int16
//   Leaderless trailer: [0] tag=0xD1  [1] kind uint8  [2..5] node int32
//                       [6..13] incarnation int64
#include <rune/metadata/PartitionAssignment.h>
#include <rune/protocol/MetadataRecords.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>

namespace rune::metadata {

/// Broker-owned record types live in the HIGH-BIT id space (review m4):
/// rune-protocol's MetadataRecordType owns the small ids (1..5 today) and adds
/// the next ones there — never a collision.  (Formal reservation of 0x8000+
/// in rune-protocol's enum docs: backlog D15b.)
inline constexpr uint16_t kFeatureLevelRecordType = 0x8001;

struct FeatureLevelRecord {
    int16_t level = 0;
    static constexpr std::size_t ENCODED_SIZE = 5;

    std::size_t encode(uint8_t* buf, std::size_t len) const noexcept {
        if (len < ENCODED_SIZE) return 0;
        buf[0] = static_cast<uint8_t>(kFeatureLevelRecordType & 0xff);
        buf[1] = static_cast<uint8_t>(kFeatureLevelRecordType >> 8);
        buf[2] = 0;
        buf[3] = static_cast<uint8_t>(static_cast<uint16_t>(level) & 0xff);
        buf[4] = static_cast<uint8_t>(static_cast<uint16_t>(level) >> 8);
        return ENCODED_SIZE;
    }
    static std::optional<FeatureLevelRecord> decode(const uint8_t* buf, std::size_t len) noexcept {
        if (len < ENCODED_SIZE) return std::nullopt;
        const uint16_t type = static_cast<uint16_t>(buf[0] | (buf[1] << 8));
        if (type != kFeatureLevelRecordType || buf[2] != 0) return std::nullopt;
        FeatureLevelRecord r;
        r.level = static_cast<int16_t>(static_cast<uint16_t>(buf[3] | (buf[4] << 8)));
        return r;
    }
};

namespace leaderless_trailer {
inline constexpr uint8_t     kTag  = 0xD1;
inline constexpr std::size_t kSize = 14;

/// Encoded size of the base UpdatePartitionAssignment record (its trailer, if
/// any, starts here).
inline std::size_t baseSize(const rune::protocol::metadata::UpdatePartitionAssignmentRecord& r) noexcept {
    return 3u + 4u + 4u + 4u + 2u + r.isr.size() * 4u + 4u + 4u + 2u + r.replicas.size() * 4u;
}

inline std::size_t encode(uint8_t* buf, std::size_t len, const LeaderlessMark& m) noexcept {
    if (len < kSize) return 0;
    buf[0] = kTag;
    buf[1] = static_cast<uint8_t>(m.kind);
    namespace d = rune::protocol::metadata::detail;   // explicit LE (D15b l3)
    d::putS32(buf, 2, static_cast<int32_t>(m.node));
    d::putS64(buf, 6, static_cast<int64_t>(m.incarnation));
    return kSize;
}

inline LeaderlessMark decode(const uint8_t* buf, std::size_t len) noexcept {
    LeaderlessMark m;
    if (len < kSize || buf[0] != kTag) return m;
    const uint8_t kind = buf[1];
    if (kind != static_cast<uint8_t>(LeaderlessMark::Kind::EmptyMemoryLeader) &&
        kind != static_cast<uint8_t>(LeaderlessMark::Kind::LogDirOffline))
        return m;   // unknown kind (a newer binary): ignored
    m.kind = static_cast<LeaderlessMark::Kind>(kind);
    namespace d = rune::protocol::metadata::detail;
    m.node        = static_cast<decltype(m.node)>(d::getS32(buf, 2));
    m.incarnation = static_cast<decltype(m.incarnation)>(d::getS64(buf, 6));
    return m;
}
} // namespace leaderless_trailer

} // namespace rune::metadata
