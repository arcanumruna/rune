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

// ---------------------------------------------------------------------------
// 3. TransientLineageRecord (recordType 0x8002) — E2 final fix wave, C1
//    multi-node (USER DECISION 2026-10-13, option b).  A TRANSIENT partition's
//    data LINEAGE: a token that names "this ring's data" across leader moves
//    that keep it (ISR handover, follower promotion) and changes exactly when
//    a leader takes over with no lineage data (a full-cluster restart, the
//    first-ever leadership; on a single node, every process start).  The
//    controller commits one record per confirmed leadership; the group
//    coordinator stamps committed offsets with the lineage and answers an
//    offset of another lineage as "none".  Internal, broker-to-broker; an older
//    MSM skips the unknown type (MetadataStateMachine::apply default).
//
//    Wire (LE): [0..1] type=0x8002 [2] version=0 [3..6] topicId int32
//               [7..10] partition int32 [11..18] lineage uint64
//               [19..22] mintEpoch int32 [23..26] confirmedEpoch int32
//               [27..30] confirmedLeader int32 [31..38] incarnation int64
// ---------------------------------------------------------------------------
inline constexpr uint16_t kTransientLineageRecordType = 0x8002;

struct TransientLineageRecord {
    int32_t          topicId   = 0;
    int32_t          partition = 0;
    TransientLineage l{};
    static constexpr std::size_t ENCODED_SIZE = 39;

    std::size_t encode(uint8_t* buf, std::size_t len) const noexcept {
        if (len < ENCODED_SIZE) return 0;
        namespace d = rune::protocol::metadata::detail;
        buf[0] = static_cast<uint8_t>(kTransientLineageRecordType & 0xff);
        buf[1] = static_cast<uint8_t>(kTransientLineageRecordType >> 8);
        buf[2] = 0;
        d::putS32(buf, 3, topicId);
        d::putS32(buf, 7, partition);
        d::putS64(buf, 11, static_cast<int64_t>(l.lineage));
        d::putS32(buf, 19, l.mintEpoch);
        d::putS32(buf, 23, l.confirmedEpoch);
        d::putS32(buf, 27, l.confirmedLeader);
        d::putS64(buf, 31, l.incarnation);
        return ENCODED_SIZE;
    }
    static std::optional<TransientLineageRecord> decode(const uint8_t* buf, std::size_t len) noexcept {
        if (len < ENCODED_SIZE) return std::nullopt;
        const uint16_t type = static_cast<uint16_t>(buf[0] | (buf[1] << 8));
        if (type != kTransientLineageRecordType || buf[2] != 0) return std::nullopt;
        namespace d = rune::protocol::metadata::detail;
        TransientLineageRecord r;
        r.topicId           = d::getS32(buf, 3);
        r.partition         = d::getS32(buf, 7);
        r.l.lineage         = static_cast<uint64_t>(d::getS64(buf, 11));
        r.l.mintEpoch       = d::getS32(buf, 19);
        r.l.confirmedEpoch  = d::getS32(buf, 23);
        r.l.confirmedLeader = d::getS32(buf, 27);
        r.l.incarnation     = d::getS64(buf, 31);
        if (r.l.lineage == 0) return std::nullopt;   // never a valid lineage
        return r;
    }
};

/// C1 multi-node: the lineage the controller commits for the leadership
/// (`leader`, `epoch`) whose ring holds data of leader epoch `src` (-1: the
/// ring holds no lineage data — unborn: a process start, the first leadership).
/// KEEP the committed lineage iff the data provably belongs to it — `src` lies
/// within [rec.mintEpoch, rec.confirmedEpoch] (an ISR handover / follower
/// promotion: the new leader fetched from a leadership of this lineage);
/// otherwise MINT `freshToken` (minted at `epoch`).  An unconfirmed `src`
/// (above confirmedEpoch: a leadership that never confirmed — it may have
/// minted a lineage that never committed) mints too: the safe side (an offset
/// of an older lineage is answered "none", never resumed into other data).
/// Pure; the controller supplies a fresh non-zero token.
/// Residual N3: `cleanElection` false (the leader was not in the replaced
/// assignment's ISR — an unclean / survivor election: it may lack committed
/// records) always MINTS.
[[nodiscard]] inline TransientLineage decideTransientLineage(const TransientLineage* rec, int32_t leader,
                                                             int32_t epoch, int32_t src, uint64_t freshToken,
                                                             int64_t incarnation = 0,
                                                             bool cleanElection = true) noexcept {
    TransientLineage out;
    const bool keep = cleanElection && rec != nullptr && rec->lineage != 0 && src >= 0 &&
                      src >= rec->mintEpoch && src <= rec->confirmedEpoch;
    out.lineage         = keep ? rec->lineage : freshToken;
    out.mintEpoch       = keep ? rec->mintEpoch : epoch;
    out.confirmedEpoch  = epoch;
    out.confirmedLeader = leader;
    out.incarnation     = incarnation;
    return out;
}

/// The coordinator's "not answerable yet" lineage (settling window): equal to
/// GroupCoordinationService::kLineagePending.
inline constexpr uint64_t kLineageUntrusted = UINT64_MAX;

/// C1 multi-node: the committed lineage `rec` is TRUSTED for the partition's
/// current committed leadership `pa` (the coordinator answers from it only
/// then): confirmed for exactly this leader and leader epoch, and — single node
/// — by this process (`selfIncarnation`; 0 = not checked, multi-node).
[[nodiscard]] inline bool lineageConfirmedFor(const TransientLineage* rec, const PartitionAssignment* pa,
                                              int64_t selfIncarnation) noexcept {
    return rec != nullptr && pa != nullptr && pa->leaderNode >= 0 && rec->confirmedLeader == pa->leaderNode &&
           rec->confirmedEpoch == pa->leaderEpoch && (selfIncarnation == 0 || rec->incarnation == selfIncarnation);
}

// ---------------------------------------------------------------------------
// 4. RemoveTopicPlacementRecord (recordType 0x8003) — catalog history Task 8.
//    A topic the catalog DROPPED: at commit every (topicId, p) assignment is
//    erased from the committed map (and its transient lineages), so each
//    replica of it sees a delete in its LocalRoleDelta.  An unknown topic id is
//    a no-op.  Appended once by the controller (MetadataService::
//    removeRuntimeTopic).  Topic ids are never reused (catalog ids), so a later
//    assignment for the same id never exists.  An older MSM skips the type.
//
//    Wire (LE): [0..1] type=0x8003 [2] version=0 [3..6] topicId int32
// ---------------------------------------------------------------------------
inline constexpr uint16_t kRemoveTopicPlacementRecordType = 0x8003;

struct RemoveTopicPlacementRecord {
    int32_t topicId = 0;
    static constexpr std::size_t ENCODED_SIZE = 7;

    std::size_t encode(uint8_t* buf, std::size_t len) const noexcept {
        if (len < ENCODED_SIZE) return 0;
        namespace d = rune::protocol::metadata::detail;
        buf[0] = static_cast<uint8_t>(kRemoveTopicPlacementRecordType & 0xff);
        buf[1] = static_cast<uint8_t>(kRemoveTopicPlacementRecordType >> 8);
        buf[2] = 0;
        d::putS32(buf, 3, topicId);
        return ENCODED_SIZE;
    }
    static std::optional<RemoveTopicPlacementRecord> decode(const uint8_t* buf, std::size_t len) noexcept {
        if (len < ENCODED_SIZE) return std::nullopt;
        const uint16_t type = static_cast<uint16_t>(buf[0] | (buf[1] << 8));
        if (type != kRemoveTopicPlacementRecordType || buf[2] != 0) return std::nullopt;
        namespace d = rune::protocol::metadata::detail;
        RemoveTopicPlacementRecord r;
        r.topicId = d::getS32(buf, 3);
        if (r.topicId < 0) return std::nullopt;   // internal partitions are never removed this way
        return r;
    }
};

// ---------------------------------------------------------------------------
// 5. NodeStorageRecord (recordType 0x8004) and ControllerTermRecord (0x8005) - ISR protocol v3.
//
//    NodeStorageRecord{node, storageId}: the controller COMMITS a node's storage id (feature level >= 2).  The
//    MSM installs it at commit like every record; nodeStorage(node) is the committed id (0 = none).
//    ControllerTermRecord{term}: the first record of a controller's claim (Raft section 5.4.2); the MSM
//    decodes and IGNORES it (it changes nothing, so followers apply it as a no-op).  Older MSMs skip both types.
//
//    NodeStorageRecord: [0..1] type=0x8004 [2] version=0 [3..6] node int32 [7..14] storageId int64
//    ControllerTermRecord: [0..1] type=0x8005 [2] version=0 [3..10] term int64
// ---------------------------------------------------------------------------
inline constexpr uint16_t kNodeStorageRecordType     = 0x8004;
inline constexpr uint16_t kControllerTermRecordType  = 0x8005;

struct NodeStorageRecord {
    int32_t node      = -1;
    int64_t storageId = 0;
    static constexpr std::size_t ENCODED_SIZE = 15;

    std::size_t encode(uint8_t* buf, std::size_t len) const noexcept {
        if (len < ENCODED_SIZE) return 0;
        namespace d = rune::protocol::metadata::detail;
        buf[0] = static_cast<uint8_t>(kNodeStorageRecordType & 0xff);
        buf[1] = static_cast<uint8_t>(kNodeStorageRecordType >> 8);
        buf[2] = 0;
        d::putS32(buf, 3, node);
        d::putS64(buf, 7, storageId);
        return ENCODED_SIZE;
    }
    static std::optional<NodeStorageRecord> decode(const uint8_t* buf, std::size_t len) noexcept {
        if (len < ENCODED_SIZE) return std::nullopt;
        const uint16_t type = static_cast<uint16_t>(buf[0] | (buf[1] << 8));
        if (type != kNodeStorageRecordType || buf[2] != 0) return std::nullopt;
        namespace d = rune::protocol::metadata::detail;
        NodeStorageRecord r;
        r.node      = d::getS32(buf, 3);
        r.storageId = d::getS64(buf, 7);
        if (r.node < 0 || r.storageId == 0) return std::nullopt;
        return r;
    }
};

struct ControllerTermRecord {
    int64_t term = 0;
    static constexpr std::size_t ENCODED_SIZE = 11;

    std::size_t encode(uint8_t* buf, std::size_t len) const noexcept {
        if (len < ENCODED_SIZE) return 0;
        namespace d = rune::protocol::metadata::detail;
        buf[0] = static_cast<uint8_t>(kControllerTermRecordType & 0xff);
        buf[1] = static_cast<uint8_t>(kControllerTermRecordType >> 8);
        buf[2] = 0;
        d::putS64(buf, 3, term);
        return ENCODED_SIZE;
    }
    static std::optional<ControllerTermRecord> decode(const uint8_t* buf, std::size_t len) noexcept {
        if (len < ENCODED_SIZE) return std::nullopt;
        const uint16_t type = static_cast<uint16_t>(buf[0] | (buf[1] << 8));
        if (type != kControllerTermRecordType || buf[2] != 0) return std::nullopt;
        namespace d = rune::protocol::metadata::detail;
        ControllerTermRecord r;
        r.term = d::getS64(buf, 3);
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
        kind != static_cast<uint8_t>(LeaderlessMark::Kind::LogDirOffline) &&
        kind != static_cast<uint8_t>(LeaderlessMark::Kind::StorageReset))
        return m;   // unknown kind (a newer binary): ignored
    m.kind = static_cast<LeaderlessMark::Kind>(kind);
    namespace d = rune::protocol::metadata::detail;
    m.node        = static_cast<decltype(m.node)>(d::getS32(buf, 2));
    m.incarnation = static_cast<decltype(m.incarnation)>(d::getS64(buf, 6));
    return m;
}
} // namespace leaderless_trailer

} // namespace rune::metadata
