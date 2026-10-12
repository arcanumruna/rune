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
    kMsgCreateView        = 8,   // gateway → admin: CREATE VIEW (Task 19)
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
    kA2cViewPlanUpdate = 3,   // Task 19: admin → control, fan out a compiled view plan
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
    uint8_t              persistence    = 0;   // Phase 2b E: 0 = disk, 1 = transient
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
// CreateView request (gateway → admin).  Owning form after drain (Task 19).
// `planBlob` is the authored rune::view::ViewPlan (rune::view::serializeAuthored);
// the admin thread deserializes + compiles it against the source descriptor(s).
// ---------------------------------------------------------------------------
struct CreateViewMsg {
    int64_t              session     = 0;
    int64_t              corr        = 0;
    uint8_t              persistence = 0;   // 0 = persisted, 1 = transient
    std::string          viewName;
    std::string          keyField;
    std::vector<uint8_t> planBlob;          // authored ViewPlan (serializeAuthored)
};

// ---------------------------------------------------------------------------
// GetSchema request (gateway → admin).  Owning form after drain.
//
// requestedFingerprint (Task 13): 0 = latest (AdminWorker resolves via
// reg_.latest); non-zero = resolve the SPECIFIC version whose per-version
// fingerprint matches (reg_.byFingerprint) — lets a typed client fetch the
// older descriptor it was compiled against.  Appended as the LAST member so
// existing 3-arg aggregate inits (`GetSchemaMsg{session, corr, name}`) still
// compile and default to 0 (latest).
// ---------------------------------------------------------------------------
struct GetSchemaMsg {
    int64_t     session = 0;
    int64_t     corr    = 0;
    std::string name;
    uint64_t    requestedFingerprint = 0;
};

// ---------------------------------------------------------------------------
// CreateTopicRequest (admin → control).  Carries the pending DefineTopic corr
// so the control-thread reply can be matched back to the admin-side pending op.
// ---------------------------------------------------------------------------
// Task 19: `keyField` + `cleanupPolicy` extend the request so the control thread
// can create a VIEW SINK as a keyed + Compact "table" (not the default Delete
// topic).  DefineTopic leaves them default (empty keyField, cleanupPolicy 0 =
// Delete) — byte-compatible with the pre-Task-19 behaviour.
struct CreateTopicReqMsg {
    int64_t     ticket         = 0;   // admin-assigned monotone id (matches reply)
    int32_t     partitionCount = 1;
    std::string name;
    std::string keyField;             // Task 19: view sink key field (empty = none)
    uint8_t     cleanupPolicy  = 0;   // Task 19: 0=Delete, 1=Compact, 2=CompactDelete
    uint8_t     persistence    = 0;   // Phase 2b E: 0 = disk, 1 = transient (InMemoryLog)
};

// ---------------------------------------------------------------------------
// CreateTopicReply (control → admin).  Authoritative id (or -1 on failure).
//
// Phase 2b C final review I1: TopicRegistry::createTopic is idempotent by name —
// when the name is already taken (a RUNE_TOPICS boot topic, a recovered runtime
// topic) it returns the EXISTING topic's id and ignores the requested spec.  The
// reply therefore carries the AUTHORITATIVE partition count (read back from the
// registry's spec, never the request's) and an `existed` flag, so the admin
// thread records the real count in the schema registry (typed clients route by
// it) and can refuse to adopt an existing topic as a view sink.
// ---------------------------------------------------------------------------
struct CreateTopicReplyMsg {
    int64_t ticket         = 0;
    int32_t topicId        = -1;    // >=0 on success; -1 on failure
    int32_t partitionCount = 0;     // authoritative; <= 0 = not reported (keep requested)
    bool    existed        = false; // true = the name already named a topic (not created)
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
                          + 4 + m.blob.size()
                          // Phase 2b E: trailing persistence byte, only when non-zero —
                          // a disk frame stays byte-identical (a max-size descriptor
                          // still fits the ring: ProduceValidationRegression).
                          + (m.persistence != 0 ? 1 : 0);
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
    std::memcpy(p, m.blob.data(), m.blob.size()); p += m.blob.size();
    if (m.persistence != 0) *p = m.persistence;   // trailing; decode tolerates absence (=> disk)
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
    if (bl < 0 || pos + static_cast<std::size_t>(bl) > len) return m;
    if (bl > 0) m.blob.assign(buf + pos, buf + pos + bl);
    pos += static_cast<std::size_t>(bl);
    if (pos < len) {   // Phase 2b E; absent => disk
        if (buf[pos] > 1) {   // unknown mode: never cast it — drop the blob so the
            m.blob.clear();   // worker answers InvalidSchema (session/corr survive)
            return m;
        }
        m.persistence = buf[pos];
    }
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

// ---- CreateView (Task 19) ----
// Wire layout: session:i64, corr:i64, persistence:u8,
//   viewNameLen:u16 + name, keyFieldLen:u16 + keyField, planLen:i32 + planBlob.
inline bool encodeCreateView(OneToOneByteRing& ring, const CreateViewMsg& m) {
    using namespace admin_wire;
    const std::size_t len = 8 + 8 + 1
                          + 2 + m.viewName.size()
                          + 2 + m.keyField.size()
                          + 4 + m.planBlob.size();
    const auto idx = ring.tryClaim(kMsgCreateView, len);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    putI64(p, m.session); p += 8;
    putI64(p, m.corr);    p += 8;
    *p++ = m.persistence;
    putU16(p, static_cast<uint16_t>(m.viewName.size())); p += 2;
    std::memcpy(p, m.viewName.data(), m.viewName.size()); p += m.viewName.size();
    putU16(p, static_cast<uint16_t>(m.keyField.size())); p += 2;
    std::memcpy(p, m.keyField.data(), m.keyField.size()); p += m.keyField.size();
    putI32(p, static_cast<int32_t>(m.planBlob.size())); p += 4;
    if (!m.planBlob.empty()) std::memcpy(p, m.planBlob.data(), m.planBlob.size());
    ring.commit(idx);
    return true;
}

inline CreateViewMsg decodeCreateView(const uint8_t* buf, std::size_t len) {
    using namespace admin_wire;
    CreateViewMsg m;
    std::size_t pos = 0;
    if (len < 19) return m;  // session(8)+corr(8)+persistence(1)+viewNameLen(2)
    m.session = getI64(buf + pos); pos += 8;
    m.corr    = getI64(buf + pos); pos += 8;
    m.persistence = buf[pos++];
    const uint16_t nl = getU16(buf + pos); pos += 2;
    if (pos + nl > len) return m;
    m.viewName.assign(reinterpret_cast<const char*>(buf + pos), nl); pos += nl;
    if (pos + 2 > len) return m;
    const uint16_t kl = getU16(buf + pos); pos += 2;
    if (pos + kl > len) return m;
    m.keyField.assign(reinterpret_cast<const char*>(buf + pos), kl); pos += kl;
    if (pos + 4 > len) return m;
    const int32_t bl = getI32(buf + pos); pos += 4;
    if (bl > 0 && pos + static_cast<std::size_t>(bl) <= len)
        m.planBlob.assign(buf + pos, buf + pos + bl);
    return m;
}

// ---- GetSchema ----
// Wire layout: session:i64, corr:i64, requestedFingerprint:u64 (0=latest),
// nameLen:u16 + name bytes.
inline bool encodeGetSchema(OneToOneByteRing& ring, const GetSchemaMsg& m) {
    using namespace admin_wire;
    const std::size_t len = 8 + 8 + 8 + 2 + m.name.size();
    const auto idx = ring.tryClaim(kMsgGetSchema, len);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    putI64(p, m.session); p += 8;
    putI64(p, m.corr);    p += 8;
    putI64(p, static_cast<int64_t>(m.requestedFingerprint)); p += 8;
    putU16(p, static_cast<uint16_t>(m.name.size())); p += 2;
    std::memcpy(p, m.name.data(), m.name.size());
    ring.commit(idx);
    return true;
}

inline GetSchemaMsg decodeGetSchema(const uint8_t* buf, std::size_t len) {
    using namespace admin_wire;
    GetSchemaMsg m;
    if (len < 26) return m;
    m.session = getI64(buf);
    m.corr    = getI64(buf + 8);
    m.requestedFingerprint = static_cast<uint64_t>(getI64(buf + 16));
    const uint16_t nl = getU16(buf + 24);
    if (26 + static_cast<std::size_t>(nl) <= len)
        m.name.assign(reinterpret_cast<const char*>(buf + 26), nl);
    return m;
}

// ---- CreateTopicReq ----
// Layout: ticket:i64, partitionCount:i32, nameLen:u16 + name,
//         keyFieldLen:u16 + keyField, cleanupPolicy:u8   (Task 19 trailer),
//         persistence:u8 (Phase 2b E trailer; absent => disk).
inline bool encodeCreateTopicReq(OneToOneByteRing& ring, const CreateTopicReqMsg& m) {
    using namespace admin_wire;
    const std::size_t len = 8 + 4 + 2 + m.name.size()
                          + 2 + m.keyField.size() + 1 + 1;
    const auto idx = ring.tryClaim(kMsgCreateTopicReq, len);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    putI64(p, m.ticket);         p += 8;
    putI32(p, m.partitionCount); p += 4;
    putU16(p, static_cast<uint16_t>(m.name.size())); p += 2;
    std::memcpy(p, m.name.data(), m.name.size()); p += m.name.size();
    putU16(p, static_cast<uint16_t>(m.keyField.size())); p += 2;
    std::memcpy(p, m.keyField.data(), m.keyField.size()); p += m.keyField.size();
    *p++ = m.cleanupPolicy;
    *p   = m.persistence;
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
    std::size_t pos = 14;
    if (pos + nl > len) return m;
    m.name.assign(reinterpret_cast<const char*>(buf + pos), nl); pos += nl;
    // Task 19 trailer (keyField + cleanupPolicy) — tolerate its absence so a
    // decode of a legacy frame still yields name (defaults: empty / Delete).
    if (pos + 2 > len) return m;
    const uint16_t kl = getU16(buf + pos); pos += 2;
    if (pos + kl > len) return m;
    m.keyField.assign(reinterpret_cast<const char*>(buf + pos), kl); pos += kl;
    if (pos + 1 > len) return m;
    m.cleanupPolicy = buf[pos++];
    if (pos < len) {   // Phase 2b E; absent => disk
        if (buf[pos] > 1) {   // unknown mode: never cast it — a nameless request fails
            m.name.clear();   // on the control thread (createTopic rejects "")
            m.partitionCount = 0;
            return m;
        }
        m.persistence = buf[pos];
    }
    return m;
}

// ---- CreateTopicReply ----
inline bool encodeCreateTopicReply(OneToOneByteRing& ring, const CreateTopicReplyMsg& m) {
    using namespace admin_wire;
    const std::size_t len = 8 + 4 + 4 + 1;
    const auto idx = ring.tryClaim(kMsgCreateTopicReply, len);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    putI64(p, m.ticket);  p += 8;
    putI32(p, m.topicId); p += 4;
    putI32(p, m.partitionCount); p += 4;
    *p = m.existed ? 1 : 0;
    ring.commit(idx);
    return true;
}

inline CreateTopicReplyMsg decodeCreateTopicReply(const uint8_t* buf, std::size_t len) {
    using namespace admin_wire;
    CreateTopicReplyMsg m;
    if (len < 12) return m;
    m.ticket  = getI64(buf);
    m.topicId = getI32(buf + 8);
    if (len < 17) return m;   // pre-I1 frame: count not reported, not existed
    m.partitionCount = getI32(buf + 12);
    m.existed        = buf[16] != 0;
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

// ---- SchemaUpdate (admin → control; F3 round 6, item 3; Task 12: +version) ----
struct SchemaUpdateMsg {
    int32_t              topicId = -1;
    uint16_t              version = 0;
    std::vector<uint8_t> blob;
};

/// Wire size of a SchemaUpdate message for `blobLen` descriptor bytes.
/// Layout: [topicId:i32][version:u16][blob...].
[[nodiscard]] inline constexpr std::size_t schemaUpdateWireLen(std::size_t blobLen) noexcept {
    return 4 + 2 + blobLen;
}

inline bool encodeSchemaUpdate(OneToOneByteRing& ring, int32_t topicId, uint16_t version,
                               std::span<const uint8_t> blob) {
    using namespace admin_wire;
    const auto idx = ring.tryClaim(kA2cSchemaUpdate, schemaUpdateWireLen(blob.size()));
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    putI32(p, topicId);
    p[4] = static_cast<uint8_t>(version & 0xff);
    p[5] = static_cast<uint8_t>((version >> 8) & 0xff);
    if (!blob.empty()) std::memcpy(p + 6, blob.data(), blob.size());
    ring.commit(idx);
    return true;
}

inline SchemaUpdateMsg decodeSchemaUpdate(const uint8_t* buf, std::size_t len) {
    using namespace admin_wire;
    SchemaUpdateMsg m;
    if (len < 6) return m;
    m.topicId = getI32(buf);
    m.version = static_cast<uint16_t>(buf[4]) | (static_cast<uint16_t>(buf[5]) << 8);
    m.blob.assign(buf + 6, buf + len);
    return m;
}

// ---- ViewPlanUpdate (Task 19) ----
// Carries a compiled view plan + its output + source descriptors to install on a
// worker's WorkerViewSet.  Used on TWO rings with the SAME wire layout:
//   admin → control  (kA2cViewPlanUpdate): the admin thread, after commit, hands
//                     the compiled plan to the control thread for fan-out;
//   control → worker (ViewSnapshotChannel::kInstallMsgType): the control thread
//                     delivers it to each worker, whose WorkerViewSet installs it
//                     on its OWN thread (shared-nothing — installView is never
//                     called cross-thread).
// Wire layout: viewTopicId:i32, persistence:u8, recovery:u8,
//   planLen:i32 + plan, outDescLen:i32 + outDesc, srcDescLen:i32 + srcDesc.
//
// Task 19b: `recovery` distinguishes a RESTART-RECOVERY install (the startup-
// load loop, BrokerNode::loadViewsForRecovery_) from the Task-19 CREATE-time
// install.  On recovery the worker registers the plan template AND marks it so
// each owned (view, partition) instance is materialized via
// WorkerViewSet::installViewForRecovery (checkpoint load + RebuildingViews +
// serve-guard) instead of the plain-install lazy path (which would serve
// immediately).  Default false => the create path is byte-for-byte unchanged.
struct ViewPlanUpdateMsg {
    int32_t              viewTopicId = -1;
    uint8_t              persistence = 0;
    bool                 recovery    = false;  // Task 19b: restart-recovery install
    std::vector<uint8_t> planBlob;      // rune::view::serialize(CompiledViewPlan)
    std::vector<uint8_t> outDescBlob;   // rune::codec::serialize(outputDescriptor)
    std::vector<uint8_t> srcDescBlob;   // rune::codec::serialize(sourceDescriptor)
};

[[nodiscard]] inline std::size_t viewPlanUpdateWireLen(const ViewPlanUpdateMsg& m) noexcept {
    return 4 + 1 + 1 + 4 + m.planBlob.size() + 4 + m.outDescBlob.size() + 4 + m.srcDescBlob.size();
}

inline bool encodeViewPlanUpdate(OneToOneByteRing& ring, int32_t msgType,
                                 const ViewPlanUpdateMsg& m) {
    using namespace admin_wire;
    const auto idx = ring.tryClaim(msgType, viewPlanUpdateWireLen(m));
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    putI32(p, m.viewTopicId); p += 4;
    *p++ = m.persistence;
    *p++ = m.recovery ? uint8_t{1} : uint8_t{0};
    putI32(p, static_cast<int32_t>(m.planBlob.size())); p += 4;
    std::memcpy(p, m.planBlob.data(), m.planBlob.size()); p += m.planBlob.size();
    putI32(p, static_cast<int32_t>(m.outDescBlob.size())); p += 4;
    std::memcpy(p, m.outDescBlob.data(), m.outDescBlob.size()); p += m.outDescBlob.size();
    putI32(p, static_cast<int32_t>(m.srcDescBlob.size())); p += 4;
    std::memcpy(p, m.srcDescBlob.data(), m.srcDescBlob.size());
    ring.commit(idx);
    return true;
}

inline ViewPlanUpdateMsg decodeViewPlanUpdate(const uint8_t* buf, std::size_t len) {
    using namespace admin_wire;
    ViewPlanUpdateMsg m;
    std::size_t pos = 0;
    if (len < 10) return m;  // viewTopicId(4) + persistence(1) + recovery(1) + planLen(4)
    m.viewTopicId = getI32(buf + pos); pos += 4;
    m.persistence = buf[pos++];
    m.recovery    = buf[pos++] != 0;
    auto readBlob = [&](std::vector<uint8_t>& out) -> bool {
        if (pos + 4 > len) return false;
        const int32_t bl = getI32(buf + pos); pos += 4;
        if (bl < 0 || pos + static_cast<std::size_t>(bl) > len) return false;
        if (bl > 0) out.assign(buf + pos, buf + pos + bl);
        pos += static_cast<std::size_t>(bl);
        return true;
    };
    if (!readBlob(m.planBlob))    return m;
    if (!readBlob(m.outDescBlob)) return m;
    (void)readBlob(m.srcDescBlob);
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
        if (n < 6) return A2cDispatch::Malformed;
        onSchemaUpdate(decodeSchemaUpdate(p, n));
        return A2cDispatch::SchemaUpdate;
    default:
        return A2cDispatch::Unknown;
    }
}

} // namespace rune::node
