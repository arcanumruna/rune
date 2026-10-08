#pragma once
// AdminChannels.h — Task 8a: shared-nothing cross-thread channels for the
// schema-registry DDL control plane (RULING-2).
//
// The AdminWorker runs on its OWN thread and SOLELY owns the __topic_schemas
// PartitionReplica.  Every cross-thread hop is an SPSC ring (HARD RULE #2,
// Aeron-style), never a shared mutable field.  Three ring channels connect the
// admin thread to the rest of the broker:
//
//   1. gateway  → admin   (AdminRequestRing)      — DefineTopic / GetSchema ops
//   2. admin    → control (CreateTopicRequestRing)— "create this physical topic"
//   3. control  → admin   (CreateTopicReplyRing)  — authoritative topicId back
//   4. control  → admin   (AdminRoleRing)         — Follower/Leader role events
//
// Because these are SPSC OneToOneByteRings, each ring has exactly ONE producer
// thread and ONE consumer thread:
//
//   AdminRequestRing      : producer = gateway/control duty-cycle thread; consumer = admin
//   CreateTopicRequestRing: producer = admin thread;                      consumer = control
//   CreateTopicReplyRing   : producer = control thread;                    consumer = admin
//   AdminRoleRing          : producer = control thread (bridge);           consumer = admin
//
// The messages are plain trivially-copyable structs carrying small ids plus
// heap-owned byte vectors for the descriptor/name.  Because a ring frame is a
// flat byte copy, we serialise a small fixed header and copy the variable-length
// bytes inline; the consumer reconstructs owning std::vectors/std::strings on
// its own thread (single-writer per side — no shared ownership).
//
// C++23.  Header-only wire helpers; no allocations on the drain path beyond the
// per-op owning containers the consumer must build (admin path is cold — DDL is
// rare — so a per-op allocation there is acceptable and off any hot path).

#include <rune/node/OneToOneByteRing.h>

#include <concurrent/AtomicBuffer.h>

#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace rune::node {

// ---------------------------------------------------------------------------
// Ring message type ids (the O2ORingBuffer msgTypeId field).
// ---------------------------------------------------------------------------
enum : int32_t {
    kMsgDefineTopic       = 1,
    kMsgGetSchema         = 2,
    kMsgCreateTopicReq    = 3,
    kMsgCreateTopicReply  = 4,
    kMsgAdminRole         = 5,
    kMsgEgressFrame       = 6,   // admin → control: [sessionId:i64][response frame]
    kMsgAlterTopic        = 7,   // gateway → admin: ALTER TOPIC (Task 11)
};

// ---------------------------------------------------------------------------
// Admin → control ring (AdminWorkerAgent::failureRing_; T10) message ids.
// One SPSC ring, producer = admin thread, consumer = control thread.
//   kA2cStorageFailure : [errno:i32][logDir bytes]   (Task 10 fix 3 / I1)
//   kA2cSchemaUpdate   : [topicId:i32][descriptor blob] (F3 round 6, item 3:
//                        the control thread — the ONLY producer of the worker
//                        SPSC ingress rings — fans it out to every worker)
// ---------------------------------------------------------------------------
enum : int32_t {
    kA2cStorageFailure = 1,
    kA2cSchemaUpdate   = 2,
};

// ---------------------------------------------------------------------------
// Ring capacities.  DDL is rare; a small ring is plenty.  Power-of-two payload.
// ---------------------------------------------------------------------------
inline constexpr int kAdminRequestRingBytes     = 1 << 20;  // 1 MiB (descriptor blobs)
inline constexpr int kCreateTopicRingBytes      = 1 << 16;  // 64 KiB
inline constexpr int kAdminRoleRingBytes        = 1 << 12;  // 4 KiB
// admin → control (T10 storage failure + F3 r6 SchemaUpdate).  Sized so that
// every descriptor the request ring can deliver fits ONE SchemaUpdate message
// (static_assert below, F3 round 7, item 4).
inline constexpr int kAdminToControlRingBytes   = kAdminRequestRingBytes;

// ---------------------------------------------------------------------------
// Wire layout helpers — little-endian fixed fields + length-prefixed byte runs.
// All hand-rolled (no protocol dependency): these frames never leave the process.
// ---------------------------------------------------------------------------
namespace admin_wire {

inline void putI64(uint8_t* p, int64_t v) { std::memcpy(p, &v, 8); }
inline void putI32(uint8_t* p, int32_t v) { std::memcpy(p, &v, 4); }
inline void putU16(uint8_t* p, uint16_t v){ std::memcpy(p, &v, 2); }
inline int64_t  getI64(const uint8_t* p){ int64_t v; std::memcpy(&v, p, 8); return v; }
inline int32_t  getI32(const uint8_t* p){ int32_t v; std::memcpy(&v, p, 4); return v; }
inline uint16_t getU16(const uint8_t* p){ uint16_t v; std::memcpy(&v, p, 2); return v; }

} // namespace admin_wire

// ---------------------------------------------------------------------------
// DefineTopic request (gateway → admin).  Owning form after drain.
// ---------------------------------------------------------------------------
struct DefineTopicMsg {
    int64_t              session        = 0;
    int64_t              corr           = 0;
    int32_t              partitionCount = 1;
    bool                 validate       = false;
    std::string          name;
    std::string          keyField;
    std::vector<uint8_t> blob;
};

// ---------------------------------------------------------------------------
// AlterTopic request (gateway → admin).  Owning form after drain (Task 11).
// Carries the client's CAS expectation (expectedVersion; 0 = don't check) and
// the opaque ALTER ops blob (rune::codec::encodeAlterOps wire format) that the
// admin thread decodes + applies against the topic's current evolving schema.
// ---------------------------------------------------------------------------
struct AlterTopicMsg {
    int64_t              session         = 0;
    int64_t              corr            = 0;
    uint16_t             expectedVersion = 0;   // 0 = unconditional (no CAS)
    std::string          topicName;
    std::vector<uint8_t> opsBlob;               // encodeAlterOps() wire bytes
};

// ---------------------------------------------------------------------------
// GetSchema request (gateway → admin).  Owning form after drain.
// ---------------------------------------------------------------------------
struct GetSchemaMsg {
    int64_t     session = 0;
    int64_t     corr    = 0;
    std::string name;
};

// ---------------------------------------------------------------------------
// CreateTopicRequest (admin → control).  Carries the pending DefineTopic corr
// so the control-thread reply can be matched back to the admin-side pending op.
// ---------------------------------------------------------------------------
struct CreateTopicReqMsg {
    int64_t     ticket         = 0;   // admin-assigned monotone id (matches reply)
    int32_t     partitionCount = 1;
    std::string name;
};

// ---------------------------------------------------------------------------
// CreateTopicReply (control → admin).  Authoritative id (or -1 on failure).
// ---------------------------------------------------------------------------
struct CreateTopicReplyMsg {
    int64_t ticket  = 0;
    int32_t topicId = -1;   // >=0 on success; -1 on failure
};

// ---------------------------------------------------------------------------
// AdminRole event (control → admin).  The bridge posts a Leader/Follower role
// transition for the schema replica; the admin thread applies it on its OWN
// thread (becomeLeader/becomeFollower) — no cross-thread role_ write.
// ---------------------------------------------------------------------------
struct AdminRoleMsg {
    bool     leader   = false;   // true = become Leader, false = become Follower
    int32_t  epoch    = 0;
    int32_t  leaderId = 0;       // used when leader==false (follower)
    // For leader: the ISR is the full replica set (schema replica minIsr gate
    // handles quorum); we do not carry it on the wire — the admin thread rebuilds
    // it from its own immutable replica-set knowledge.
};

// ===========================================================================
// Encoders / decoders
// ===========================================================================

// ---- DefineTopic ----
inline bool encodeDefineTopic(OneToOneByteRing& ring, const DefineTopicMsg& m) {
    using namespace admin_wire;
    const std::size_t len = 8 + 8 + 4 + 1
                          + 2 + m.name.size()
                          + 2 + m.keyField.size()
                          + 4 + m.blob.size();
    const auto idx = ring.tryClaim(kMsgDefineTopic, len);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    putI64(p, m.session);        p += 8;
    putI64(p, m.corr);           p += 8;
    putI32(p, m.partitionCount); p += 4;
    *p++ = m.validate ? 1 : 0;
    putU16(p, static_cast<uint16_t>(m.name.size())); p += 2;
    std::memcpy(p, m.name.data(), m.name.size());    p += m.name.size();
    putU16(p, static_cast<uint16_t>(m.keyField.size())); p += 2;
    std::memcpy(p, m.keyField.data(), m.keyField.size()); p += m.keyField.size();
    putI32(p, static_cast<int32_t>(m.blob.size())); p += 4;
    std::memcpy(p, m.blob.data(), m.blob.size());
    ring.commit(idx);
    return true;
}

inline DefineTopicMsg decodeDefineTopic(const uint8_t* buf, std::size_t len) {
    using namespace admin_wire;
    DefineTopicMsg m;
    std::size_t pos = 0;
    if (len < 21) return m;
    m.session = getI64(buf + pos); pos += 8;
    m.corr    = getI64(buf + pos); pos += 8;
    m.partitionCount = getI32(buf + pos); pos += 4;
    m.validate = buf[pos++] != 0;
    const uint16_t nl = getU16(buf + pos); pos += 2;
    if (pos + nl > len) return m;
    m.name.assign(reinterpret_cast<const char*>(buf + pos), nl); pos += nl;
    if (pos + 2 > len) return m;
    const uint16_t kl = getU16(buf + pos); pos += 2;
    if (pos + kl > len) return m;
    m.keyField.assign(reinterpret_cast<const char*>(buf + pos), kl); pos += kl;
    if (pos + 4 > len) return m;
    const int32_t bl = getI32(buf + pos); pos += 4;
    if (bl > 0 && pos + static_cast<std::size_t>(bl) <= len)
        m.blob.assign(buf + pos, buf + pos + bl);
    return m;
}

// ---- AlterTopic (Task 11) ----
inline bool encodeAlterTopic(OneToOneByteRing& ring, const AlterTopicMsg& m) {
    using namespace admin_wire;
    const std::size_t len = 8 + 8 + 2
                          + 2 + m.topicName.size()
                          + 4 + m.opsBlob.size();
    const auto idx = ring.tryClaim(kMsgAlterTopic, len);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    putI64(p, m.session);         p += 8;
    putI64(p, m.corr);            p += 8;
    putU16(p, m.expectedVersion); p += 2;
    putU16(p, static_cast<uint16_t>(m.topicName.size())); p += 2;
    std::memcpy(p, m.topicName.data(), m.topicName.size()); p += m.topicName.size();
    putI32(p, static_cast<int32_t>(m.opsBlob.size())); p += 4;
    if (!m.opsBlob.empty()) std::memcpy(p, m.opsBlob.data(), m.opsBlob.size());
    ring.commit(idx);
    return true;
}

inline AlterTopicMsg decodeAlterTopic(const uint8_t* buf, std::size_t len) {
    using namespace admin_wire;
    AlterTopicMsg m;
    std::size_t pos = 0;
    if (len < 20) return m;  // session(8)+corr(8)+expectedVersion(2)+nameLen(2)
    m.session = getI64(buf + pos); pos += 8;
    m.corr    = getI64(buf + pos); pos += 8;
    m.expectedVersion = getU16(buf + pos); pos += 2;
    const uint16_t nl = getU16(buf + pos); pos += 2;
    if (pos + nl > len) return m;
    m.topicName.assign(reinterpret_cast<const char*>(buf + pos), nl); pos += nl;
    if (pos + 4 > len) return m;
    const int32_t bl = getI32(buf + pos); pos += 4;
    if (bl > 0 && pos + static_cast<std::size_t>(bl) <= len)
        m.opsBlob.assign(buf + pos, buf + pos + bl);
    return m;
}

// ---- GetSchema ----
inline bool encodeGetSchema(OneToOneByteRing& ring, const GetSchemaMsg& m) {
    using namespace admin_wire;
    const std::size_t len = 8 + 8 + 2 + m.name.size();
    const auto idx = ring.tryClaim(kMsgGetSchema, len);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    putI64(p, m.session); p += 8;
    putI64(p, m.corr);    p += 8;
    putU16(p, static_cast<uint16_t>(m.name.size())); p += 2;
    std::memcpy(p, m.name.data(), m.name.size());
    ring.commit(idx);
    return true;
}

inline GetSchemaMsg decodeGetSchema(const uint8_t* buf, std::size_t len) {
    using namespace admin_wire;
    GetSchemaMsg m;
    if (len < 18) return m;
    m.session = getI64(buf);
    m.corr    = getI64(buf + 8);
    const uint16_t nl = getU16(buf + 16);
    if (18 + static_cast<std::size_t>(nl) <= len)
        m.name.assign(reinterpret_cast<const char*>(buf + 18), nl);
    return m;
}

// ---- CreateTopicReq ----
inline bool encodeCreateTopicReq(OneToOneByteRing& ring, const CreateTopicReqMsg& m) {
    using namespace admin_wire;
    const std::size_t len = 8 + 4 + 2 + m.name.size();
    const auto idx = ring.tryClaim(kMsgCreateTopicReq, len);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    putI64(p, m.ticket);         p += 8;
    putI32(p, m.partitionCount); p += 4;
    putU16(p, static_cast<uint16_t>(m.name.size())); p += 2;
    std::memcpy(p, m.name.data(), m.name.size());
    ring.commit(idx);
    return true;
}

inline CreateTopicReqMsg decodeCreateTopicReq(const uint8_t* buf, std::size_t len) {
    using namespace admin_wire;
    CreateTopicReqMsg m;
    if (len < 14) return m;
    m.ticket = getI64(buf);
    m.partitionCount = getI32(buf + 8);
    const uint16_t nl = getU16(buf + 12);
    if (14 + static_cast<std::size_t>(nl) <= len)
        m.name.assign(reinterpret_cast<const char*>(buf + 14), nl);
    return m;
}

// ---- CreateTopicReply ----
inline bool encodeCreateTopicReply(OneToOneByteRing& ring, const CreateTopicReplyMsg& m) {
    using namespace admin_wire;
    const std::size_t len = 8 + 4;
    const auto idx = ring.tryClaim(kMsgCreateTopicReply, len);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    putI64(p, m.ticket);  p += 8;
    putI32(p, m.topicId);
    ring.commit(idx);
    return true;
}

inline CreateTopicReplyMsg decodeCreateTopicReply(const uint8_t* buf, std::size_t len) {
    using namespace admin_wire;
    CreateTopicReplyMsg m;
    if (len < 12) return m;
    m.ticket  = getI64(buf);
    m.topicId = getI32(buf + 8);
    return m;
}

// ---- AdminRole ----
inline bool encodeAdminRole(OneToOneByteRing& ring, const AdminRoleMsg& m) {
    using namespace admin_wire;
    const std::size_t len = 1 + 4 + 4;
    const auto idx = ring.tryClaim(kMsgAdminRole, len);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    *p++ = m.leader ? 1 : 0;
    putI32(p, m.epoch);    p += 4;
    putI32(p, m.leaderId);
    ring.commit(idx);
    return true;
}

inline AdminRoleMsg decodeAdminRole(const uint8_t* buf, std::size_t len) {
    using namespace admin_wire;
    AdminRoleMsg m;
    if (len < 9) return m;
    m.leader   = buf[0] != 0;
    m.epoch    = getI32(buf + 1);
    m.leaderId = getI32(buf + 5);
    return m;
}

// ---- SchemaUpdate (admin → control; F3 round 6, item 3) ----
struct SchemaUpdateMsg {
    int32_t              topicId = -1;
    std::vector<uint8_t> blob;
};

/// Wire size of a SchemaUpdate message for `blobLen` descriptor bytes.
[[nodiscard]] inline constexpr std::size_t schemaUpdateWireLen(std::size_t blobLen) noexcept {
    return 4 + blobLen;
}

inline bool encodeSchemaUpdate(OneToOneByteRing& ring, int32_t topicId,
                               std::span<const uint8_t> blob) {
    using namespace admin_wire;
    const auto idx = ring.tryClaim(kA2cSchemaUpdate, schemaUpdateWireLen(blob.size()));
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    putI32(p, topicId);
    if (!blob.empty()) std::memcpy(p + 4, blob.data(), blob.size());
    ring.commit(idx);
    return true;
}

inline SchemaUpdateMsg decodeSchemaUpdate(const uint8_t* buf, std::size_t len) {
    using namespace admin_wire;
    SchemaUpdateMsg m;
    if (len < 4) return m;
    m.topicId = getI32(buf);
    m.blob.assign(buf + 4, buf + len);
    return m;
}

// ---- F3 round 7, item 4: descriptor size is bounded BY CONSTRUCTION ----
// A descriptor reaches the admin thread only inside a DefineTopic on the
// request ring (O2O ring: maxMsgLength = capacity / 8), whose fixed framing is
// session(8)+corr(8)+partitions(4)+validate(1)+nameLen(2)+keyLen(2)+blobLen(4).
// The SchemaUpdate that re-posts it on the admin→control ring carries only a
// 4-byte topicId in front, so every committed descriptor fits — the
// "descriptor too large → validator not pushed" path in
// AdminWorkerAgent::postSchemaUpdate is unreachable.
inline constexpr std::size_t kDefineTopicFixedBytes = 8 + 8 + 4 + 1 + 2 + 2 + 4;
inline constexpr std::size_t kMaxDescriptorViaRequestRing =
    static_cast<std::size_t>(kAdminRequestRingBytes) / 8 - kDefineTopicFixedBytes;
static_assert(schemaUpdateWireLen(kMaxDescriptorViaRequestRing) <=
                  static_cast<std::size_t>(kAdminToControlRingBytes) / 8,
              "every descriptor the admin request ring can deliver must fit one "
              "SchemaUpdate on the admin->control ring");

// ---- admin → control dispatch (F3 round 7, item 3) ----
// Explicit per-type dispatch: an unknown type or a too-short frame is
// reported (the caller logs it, rate-limited) and SKIPPED — never decoded as
// some other message (misreading a frame as a storage failure halts the broker).
enum class A2cDispatch : uint8_t { StorageFailure, SchemaUpdate, Unknown, Malformed };

template <typename OnStorageFailure, typename OnSchemaUpdate>
A2cDispatch dispatchAdminToControl(int32_t typeId, const uint8_t* p, std::size_t n,
                                   OnStorageFailure&& onStorageFailure,
                                   OnSchemaUpdate&& onSchemaUpdate) {
    switch (typeId) {
    case kA2cStorageFailure: {
        if (n < 4) return A2cDispatch::Malformed;
        const int32_t err = admin_wire::getI32(p);
        onStorageFailure(err, std::string_view(reinterpret_cast<const char*>(p) + 4, n - 4));
        return A2cDispatch::StorageFailure;
    }
    case kA2cSchemaUpdate:
        if (n < 4) return A2cDispatch::Malformed;
        onSchemaUpdate(decodeSchemaUpdate(p, n));
        return A2cDispatch::SchemaUpdate;
    default:
        return A2cDispatch::Unknown;
    }
}

} // namespace rune::node
