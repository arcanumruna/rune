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
#include <rune/protocol/CatalogTypes.h>

#include <concurrent/AtomicBuffer.h>

#include <array>
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
    // Catalog history Task 6 (control → admin, on the AdminRoleRing):
    kMsgCatalogRole       = 9,   // the __catalog replica's role (AdminRoleMsg, full form)
    kMsgCatalogAlterIsrResponse = 10,  // the controller's AlterIsrResponse for (-6, 0), verbatim SBE
};

// ---------------------------------------------------------------------------
// Admin → control ring (AdminWorkerAgent::failureRing_; T10) message ids.
// One SPSC ring, producer = admin thread, consumer = control thread.
//   kA2cStorageFailure : [errno:i32][logDir bytes]   (Task 10 fix 3 / I1)
//   kA2cSchemaUpdate   : [topicId:i32][descriptor blob] (F3 round 6, item 3:
//                        the control thread — the ONLY producer of the worker
//                        SPSC ingress rings — fans it out to every worker)
//   kA2cTopicCommitted : [topicId:i32] (B10 / D4 fix round 1: the topic's catalog
//                        record committed; the control thread rewrites its
//                        topic-id marker pending -> committed)
// ---------------------------------------------------------------------------
enum : int32_t {
    kA2cStorageFailure = 1,
    kA2cSchemaUpdate   = 2,
    kA2cViewPlanUpdate = 3,   // Task 19: admin → control, fan out a compiled view plan
    // B10 / D4 fix round 1: [topicId:i32] — a runtime topic's catalog record
    // (DefineTopic schema / CreateView record) COMMITTED; the control thread (owner
    // of TopicRegistry) rewrites its topic-id marker pending -> committed.
    kA2cTopicCommitted = 4,
    // Catalog history Task 6: the __catalog replica's AlterIsr proposal (verbatim
    // SBE AlterIsrMsg).  The control thread hands it to the controller channel
    // (replTransport_->sendAlterIsr) — the same one the coordinator partitions use.
    kA2cCatalogAlterIsr = 10,
    // Catalog history Task 7: the CatalogApplier's effects (admin → control).
    // Layouts and codecs below ("CatalogApplier effects").  Task 8 drains 5–8.
    kA2cCatalogTopicApplied = 5,    // a CreateTopic / view sink applied: build the topic at its catalog id
    kA2cCatalogTopicDropped = 6,    // a DropTopic / retired view sink applied
    kA2cCatalogTopicConfig  = 7,    // an AlterTopicConfig applied (cleaner policy override)
    kA2cViewUninstall       = 8,    // a DropView / AlterView retired a view plan
    kA2cCatalogHalted       = 9,    // the applier HALTED (metric + loud log on the control thread)
    kA2cCatalogApplied      = 11,   // the applier applied offset N (metric)
};

// ---------------------------------------------------------------------------
// Ring capacities.  DDL is rare; a small ring is plenty.  Power-of-two payload.
// ---------------------------------------------------------------------------
inline constexpr int kAdminRequestRingBytes     = 1 << 20;  // 1 MiB (descriptor blobs)
inline constexpr int kCreateTopicRingBytes      = 1 << 16;  // 64 KiB
inline constexpr int kAdminRoleRingBytes        = 1 << 12;  // 4 KiB (max message 512 B >= the largest role)
// admin → control (T10 storage failure + F3 r6 SchemaUpdate).  Sized so that
// every descriptor the request ring can deliver fits ONE SchemaUpdate message
// (static_assert below, F3 round 7, item 4).
inline constexpr int kAdminToControlRingBytes   = kAdminRequestRingBytes;
inline constexpr int kAdminEgressRingBytes     = kAdminRequestRingBytes;  // admin → control answer frames

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
    bool                 malformed   = false;   // ring decode saw an unknown persistence byte
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
    // Phase 2b E: the topic's REAL persistence wire byte (0/1), read back from the
    // registry — an existing schema-less topic keeps ITS mode, not the request's.
    // kPersistenceNotReported = frame without it (caller keeps the requested mode).
    static constexpr uint8_t kPersistenceNotReported = 0xff;
    uint8_t persistence    = kPersistenceNotReported;
    // DDL conformance B3: the requested name differs only in ASCII case from the
    // existing topic `topicId` (existed = true): nothing was created and the admin
    // thread answers TopicExists.  Ring trailer byte 18; absent => false.
    bool    caseConflict   = false;
    // OPTIONAL KL1 follow-up: the existing topic is a RUNTIME topic whose spec is
    // exactly the requested view sink's (Compact, key, partitions, mode) — the admin
    // thread may adopt it when no catalog record claims it (its own orphan sink).
    // Ring trailer byte 19; absent => false.
    // Final review I1: also requires the topic to be UNCOMMITTED (not servable — so
    // nothing could have been acknowledged into it) and every partition log EMPTY
    // (read by the control thread, which alone has touched an uncommitted topic).
    bool    sinkSpecMatches = false;
    // Final review I2: the existing topic is a RUNTIME topic with the Compact
    // cleanup policy — only a CREATE VIEW sink is one (DefineTopic creates Delete
    // topics).  The admin thread answers a DefineTopic over it TopicExists (25):
    // a view sink is never adopted as a typed topic (KL1b closed), so the two
    // catalogs never both claim one id.  Ring trailer byte 20; absent => false.
    bool    existingIsRuntimeCompact = false;
};

// ---------------------------------------------------------------------------
// AdminRole event (control → admin).  The bridge posts a Leader/Follower role
// transition for the schema replica; the admin thread applies it on its OWN
// thread (becomeLeader/becomeFollower) — no cross-thread role_ write.
// ---------------------------------------------------------------------------
struct AdminRoleMsg {
    /// The largest ISR a role carries (== AlterIsrMsg::kMaxIsr, FollowerSlots::kMaxReplicas).
    static constexpr int kMaxIsr = 16;
    bool     leader   = false;   // true = become Leader, false = become Follower
    int32_t  epoch    = 0;       // the leader epoch (the committed assignment's leaderEpoch)
    int32_t  leaderId = 0;       // used when leader==false (follower); -1 = no leader
    // Catalog history Task 6 (kMsgCatalogRole only): the committed assignment's
    // ISR and partitionEpoch (the AlterIsr compare-and-set version).  The
    // __topic_schemas role (kMsgAdminRole) leaves them empty / -1: its ISR is the
    // full replica set, rebuilt by the admin thread from its own immutable
    // replica-set knowledge.  Fixed-capacity (no allocation on the ring path).
    int32_t  partitionEpoch = -1;
    int32_t  isrCount       = 0;
    std::array<int32_t, kMaxIsr> isr{};
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
                          + 1;   // Phase 2b E: trailing persistence byte (always written;
                                 // accounted for in kDefineTopicFixedBytes)
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
    *p = m.persistence;   // trailing; decode tolerates absence (=> disk)
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
    if (buf[pos] > 1) {   // unknown mode: never cast it — session/corr survive so the
                          // worker answers InvalidSchema (see startCreateView_)
        m.malformed = true;
        return m;
    }
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
            m.name.clear();   // on the control thread (createTopicForAdmin refuses an invalid name)
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
    const std::size_t len = 8 + 4 + 4 + 1 + 1 + 1 + 1 + 1;
    const auto idx = ring.tryClaim(kMsgCreateTopicReply, len);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    putI64(p, m.ticket);  p += 8;
    putI32(p, m.topicId); p += 4;
    putI32(p, m.partitionCount); p += 4;
    *p++ = m.existed ? 1 : 0;
    *p++ = m.persistence;
    *p++ = m.caseConflict ? 1 : 0;      // DDL conformance B3 trailer
    *p++ = m.sinkSpecMatches ? 1 : 0;   // Task 14 (KL1 follow-up) trailer
    *p   = m.existingIsRuntimeCompact ? 1 : 0;   // final review I2 trailer
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
    if (len < 18) return m;   // pre-2b-E frame: persistence not reported
    if (len >= 19) m.caseConflict = buf[18] != 0;   // DDL conformance B3 trailer
    if (len >= 20) m.sinkSpecMatches = buf[19] != 0;   // Task 14 (KL1 follow-up) trailer
    if (len >= 21) m.existingIsRuntimeCompact = buf[20] != 0;   // final review I2 trailer
    if (buf[17] == CreateTopicReplyMsg::kPersistenceNotReported) return m;
    if (buf[17] > 1) {        // unknown mode: never cast — fail the create
        m.topicId      = -1;
        m.caseConflict = false;   // a failed create is Internal, never TopicExists
        m.sinkSpecMatches = false;   // nor an adoptable sink
        m.existingIsRuntimeCompact = false;
        return m;
    }
    m.persistence = buf[17];
    return m;
}

// ---- AdminRole ----
// Layout: [leader:u8][epoch:i32][leaderId:i32][partitionEpoch:i32][isrCount:i32][isr:i32 * isrCount].
inline constexpr std::size_t kAdminRoleFixedBytes = 1 + 4 + 4 + 4 + 4;

[[nodiscard]] inline constexpr std::size_t adminRoleWireLen(int32_t isrCount) noexcept {
    return kAdminRoleFixedBytes + 4u * static_cast<std::size_t>(isrCount < 0 ? 0 : isrCount);
}

/// `msgType` = kMsgAdminRole (the __topic_schemas / __views role) or
/// kMsgCatalogRole (the __catalog role).  false = ring full (the caller keeps
/// the role and retries) or an ISR beyond kMaxIsr (never encoded).
inline bool encodeAdminRole(OneToOneByteRing& ring, const AdminRoleMsg& m,
                            int32_t msgType = kMsgAdminRole) {
    using namespace admin_wire;
    if (m.isrCount < 0 || m.isrCount > AdminRoleMsg::kMaxIsr) return false;
    // tryClaim throws beyond the ring's max message (capacity / 8); the 4 KiB
    // role ring holds the largest role (17 + 16 * 4 B) by construction.
    if (adminRoleWireLen(m.isrCount) > static_cast<std::size_t>(ring.maxMsgLength())) return false;
    const auto idx = ring.tryClaim(msgType, adminRoleWireLen(m.isrCount));
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    *p++ = m.leader ? 1 : 0;
    putI32(p, m.epoch);          p += 4;
    putI32(p, m.leaderId);       p += 4;
    putI32(p, m.partitionEpoch); p += 4;
    putI32(p, m.isrCount);       p += 4;
    for (int i = 0; i < m.isrCount; ++i, p += 4) putI32(p, m.isr[static_cast<std::size_t>(i)]);
    ring.commit(idx);
    return true;
}

/// Checked decode: false on a short frame or an ISR count outside [0, kMaxIsr]
/// (the caller reports it — never applied as some other role).
[[nodiscard]] inline bool decodeAdminRole(const uint8_t* buf, std::size_t len, AdminRoleMsg& m) {
    using namespace admin_wire;
    m = AdminRoleMsg{};
    if (len < kAdminRoleFixedBytes) return false;
    m.leader         = buf[0] != 0;
    m.epoch          = getI32(buf + 1);
    m.leaderId       = getI32(buf + 5);
    m.partitionEpoch = getI32(buf + 9);
    m.isrCount       = getI32(buf + 13);
    if (m.isrCount < 0 || m.isrCount > AdminRoleMsg::kMaxIsr || len < adminRoleWireLen(m.isrCount)) {
        m = AdminRoleMsg{};
        return false;
    }
    for (int i = 0; i < m.isrCount; ++i)
        m.isr[static_cast<std::size_t>(i)] = getI32(buf + kAdminRoleFixedBytes + 4u * static_cast<std::size_t>(i));
    return true;
}

inline AdminRoleMsg decodeAdminRole(const uint8_t* buf, std::size_t len) {
    AdminRoleMsg m;
    (void)decodeAdminRole(buf, len, m);
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

/// Writes exactly schemaUpdateWireLen(blob.size()) bytes at `p`.
inline void writeSchemaUpdate(uint8_t* p, int32_t topicId, uint16_t version, std::span<const uint8_t> blob) noexcept {
    using namespace admin_wire;
    putI32(p, topicId);
    p[4] = static_cast<uint8_t>(version & 0xff);
    p[5] = static_cast<uint8_t>((version >> 8) & 0xff);
    if (!blob.empty()) std::memcpy(p + 6, blob.data(), blob.size());
}

inline bool encodeSchemaUpdate(OneToOneByteRing& ring, int32_t topicId, uint16_t version,
                               std::span<const uint8_t> blob) {
    const auto idx = ring.tryClaim(kA2cSchemaUpdate, schemaUpdateWireLen(blob.size()));
    if (idx < 0) return false;
    writeSchemaUpdate(ring.base() + idx, topicId, version, blob);
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

[[nodiscard]] inline std::size_t viewPlanUpdateWireLen(std::span<const uint8_t> plan, std::span<const uint8_t> outDesc,
                                                      std::span<const uint8_t> srcDesc) noexcept {
    return 4 + 1 + 1 + 4 + plan.size() + 4 + outDesc.size() + 4 + srcDesc.size();
}

/// Writes exactly viewPlanUpdateWireLen(plan, outDesc, srcDesc) bytes at `p`
/// (catalog history Task 7: the catalog effect sink writes it from spans).
inline void writeViewPlanUpdate(uint8_t* p, int32_t viewTopicId, uint8_t persistence, uint8_t recovery,
                                std::span<const uint8_t> plan, std::span<const uint8_t> outDesc,
                                std::span<const uint8_t> srcDesc) noexcept {
    using namespace admin_wire;
    putI32(p, viewTopicId); p += 4;
    *p++ = persistence;
    *p++ = recovery;
    putI32(p, static_cast<int32_t>(plan.size())); p += 4;
    if (!plan.empty()) std::memcpy(p, plan.data(), plan.size());
    p += plan.size();
    putI32(p, static_cast<int32_t>(outDesc.size())); p += 4;
    if (!outDesc.empty()) std::memcpy(p, outDesc.data(), outDesc.size());
    p += outDesc.size();
    putI32(p, static_cast<int32_t>(srcDesc.size())); p += 4;
    if (!srcDesc.empty()) std::memcpy(p, srcDesc.data(), srcDesc.size());
}

inline bool encodeViewPlanUpdate(OneToOneByteRing& ring, int32_t msgType,
                                 const ViewPlanUpdateMsg& m) {
    const auto idx = ring.tryClaim(msgType, viewPlanUpdateWireLen(m));
    if (idx < 0) return false;
    writeViewPlanUpdate(ring.base() + idx, m.viewTopicId, m.persistence, m.recovery ? uint8_t{1} : uint8_t{0},
                        m.planBlob, m.outDescBlob, m.srcDescBlob);
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

// ---- TopicCommitted (admin → control; B10 / D4 fix round 1) ----
// Layout: [topicId:i32].
inline constexpr std::size_t kTopicCommittedWireLen = 4;

inline bool encodeTopicCommitted(OneToOneByteRing& ring, int32_t topicId) {
    const auto idx = ring.tryClaim(kA2cTopicCommitted, kTopicCommittedWireLen);
    if (idx < 0) return false;
    admin_wire::putI32(ring.base() + idx, topicId);
    ring.commit(idx);
    return true;
}

/// The committed topic id, or -1 on a malformed (short) frame.
[[nodiscard]] inline int32_t decodeTopicCommitted(const uint8_t* buf, std::size_t len) {
    if (len < kTopicCommittedWireLen) return -1;
    return admin_wire::getI32(buf);
}

// ===========================================================================
// Catalog history Task 7 — CatalogApplier effects (admin → control, failureRing).
//
// Every layout is little-endian, fixed fields + u16-length-prefixed strings.
// Each message has a `...WireLen` (exact size), a `write...` (writes exactly
// that many bytes at a caller pointer: the ring claim, or the admin-owned
// preallocated stage of the ring-full backlog) and a CHECKED `decode...` that
// refuses a short / over-long / inconsistent frame (false — never a default
// value applied as a real effect).
//
//   kA2cCatalogTopicApplied : [topicId:i32][nameLen:u16 + name][partitions:i32][rf:i16]
//                             [persistence:u8][TopicConfig block]
//       TopicConfig block   : cleanupPolicy:u8 retentionMs:i64 retentionBytes:i64
//                             deleteRetentionMs:i64 minCleanableDirtyRatioPpm:i32
//                             segmentRollBytes:i64 minCompactionLagMs:i64
//                             maxCompactionLagMs:i64 retentionCheckMs:i64
//                             fileDeleteDelayMs:i64 durability:u8 durabilityTier:u8
//                             coPartitionGroupLen:u16 + bytes, partitionKeyLen:u16 + bytes
//   kA2cCatalogTopicDropped : [topicId:i32][nameLen:u16 + name]
//   kA2cCatalogTopicConfig  : [topicId:i32][count:u16][(key:u8, value:i64) * count]
//   kA2cViewUninstall       : [viewTopicId:i32]
//   kA2cCatalogHalted       : [offset:i64][expected:u64][actual:u64][detail bytes (rest)]
//   kA2cCatalogApplied      : [offset:i64][digest:u64]
// ===========================================================================
namespace admin_wire {
inline void putI16(uint8_t* p, int16_t v) { std::memcpy(p, &v, 2); }
inline void putU64(uint8_t* p, uint64_t v) { std::memcpy(p, &v, 8); }
inline int16_t  getI16(const uint8_t* p) { int16_t v; std::memcpy(&v, p, 2); return v; }
inline uint64_t getU64(const uint8_t* p) { uint64_t v; std::memcpy(&v, p, 8); return v; }
/// u16-length-prefixed string; the caller sized the buffer (len <= 0xFFFF checked by the WireLen user).
inline uint8_t* putStr16(uint8_t* p, std::string_view s) {
    putU16(p, static_cast<uint16_t>(s.size()));
    if (!s.empty()) std::memcpy(p + 2, s.data(), s.size());
    return p + 2 + s.size();
}
/// Checked read of a str16 at [pos, len): false on a short frame.
inline bool getStr16(const uint8_t* buf, std::size_t len, std::size_t& pos, std::string& out) {
    if (pos + 2 > len) return false;
    const uint16_t n = getU16(buf + pos);
    pos += 2;
    if (pos + n > len) return false;
    out.assign(reinterpret_cast<const char*>(buf + pos), n);
    pos += n;
    return true;
}
} // namespace admin_wire

inline constexpr std::size_t kTopicConfigFixedBytes = 1 + 8 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 8 + 1 + 1;
inline constexpr std::size_t kMaxStr16              = 0xFFFF;

[[nodiscard]] inline std::size_t topicConfigWireLen(const rune::protocol::TopicConfig& c) noexcept {
    return kTopicConfigFixedBytes + 2 + c.coPartitionGroup.size() + 2 + c.partitionKey.size();
}

struct CatalogTopicAppliedMsg {
    int32_t                     topicId           = -1;
    std::string                 name;
    int32_t                     partitions        = 0;
    int16_t                     replicationFactor = 0;
    uint8_t                     persistence       = 0;   // 0 disk, 1 transient
    rune::protocol::TopicConfig config;
};

/// 0 = cannot be encoded (a string longer than a u16 prefix holds).
[[nodiscard]] inline std::size_t catalogTopicAppliedWireLen(std::string_view name,
                                                            const rune::protocol::TopicConfig& c) noexcept {
    if (name.size() > kMaxStr16 || c.coPartitionGroup.size() > kMaxStr16 || c.partitionKey.size() > kMaxStr16)
        return 0;
    return 4 + 2 + name.size() + 4 + 2 + 1 + topicConfigWireLen(c);
}

inline void writeCatalogTopicApplied(uint8_t* p, int32_t topicId, std::string_view name, int32_t partitions,
                                     int16_t rf, uint8_t persistence, const rune::protocol::TopicConfig& c) {
    using namespace admin_wire;
    putI32(p, topicId); p += 4;
    p = putStr16(p, name);
    putI32(p, partitions); p += 4;
    putI16(p, rf); p += 2;
    *p++ = persistence;
    *p++ = c.cleanupPolicy;
    putI64(p, c.retentionMs); p += 8;
    putI64(p, c.retentionBytes); p += 8;
    putI64(p, c.deleteRetentionMs); p += 8;
    putI32(p, c.minCleanableDirtyRatioPpm); p += 4;
    putI64(p, c.segmentRollBytes); p += 8;
    putI64(p, c.minCompactionLagMs); p += 8;
    putI64(p, c.maxCompactionLagMs); p += 8;
    putI64(p, c.retentionCheckMs); p += 8;
    putI64(p, c.fileDeleteDelayMs); p += 8;
    *p++ = c.durability;
    *p++ = c.durabilityTier;
    p = putStr16(p, c.coPartitionGroup);
    (void)putStr16(p, c.partitionKey);
}

[[nodiscard]] inline bool decodeCatalogTopicApplied(const uint8_t* buf, std::size_t len, CatalogTopicAppliedMsg& m) {
    using namespace admin_wire;
    std::size_t pos = 0;
    if (len < 4) return false;
    m.topicId = getI32(buf); pos = 4;
    if (!getStr16(buf, len, pos, m.name)) return false;
    if (pos + 4 + 2 + 1 + kTopicConfigFixedBytes > len) return false;
    m.partitions        = getI32(buf + pos); pos += 4;
    m.replicationFactor = getI16(buf + pos); pos += 2;
    m.persistence       = buf[pos++];
    if (m.persistence > 1) return false;
    auto& c = m.config;
    c.cleanupPolicy             = buf[pos++];
    c.retentionMs               = getI64(buf + pos); pos += 8;
    c.retentionBytes            = getI64(buf + pos); pos += 8;
    c.deleteRetentionMs         = getI64(buf + pos); pos += 8;
    c.minCleanableDirtyRatioPpm = getI32(buf + pos); pos += 4;
    c.segmentRollBytes          = getI64(buf + pos); pos += 8;
    c.minCompactionLagMs        = getI64(buf + pos); pos += 8;
    c.maxCompactionLagMs        = getI64(buf + pos); pos += 8;
    c.retentionCheckMs          = getI64(buf + pos); pos += 8;
    c.fileDeleteDelayMs         = getI64(buf + pos); pos += 8;
    c.durability                = buf[pos++];
    c.durabilityTier            = buf[pos++];
    if (!getStr16(buf, len, pos, c.coPartitionGroup)) return false;
    if (!getStr16(buf, len, pos, c.partitionKey)) return false;
    return pos == len && m.topicId >= 0 && m.partitions > 0 && !m.name.empty();
}

struct CatalogTopicDroppedMsg {
    int32_t     topicId = -1;
    std::string name;
};
[[nodiscard]] inline std::size_t catalogTopicDroppedWireLen(std::string_view name) noexcept {
    return name.size() > kMaxStr16 ? 0 : 4 + 2 + name.size();
}
inline void writeCatalogTopicDropped(uint8_t* p, int32_t topicId, std::string_view name) {
    admin_wire::putI32(p, topicId);
    (void)admin_wire::putStr16(p + 4, name);
}
[[nodiscard]] inline bool decodeCatalogTopicDropped(const uint8_t* buf, std::size_t len, CatalogTopicDroppedMsg& m) {
    if (len < 4) return false;
    m.topicId = admin_wire::getI32(buf);
    std::size_t pos = 4;
    if (!admin_wire::getStr16(buf, len, pos, m.name)) return false;
    return pos == len && m.topicId >= 0;
}

/// One (key, value) of kA2cCatalogTopicConfig.  `key` = rune::protocol::ConfigKey's byte.
struct CatalogConfigEntry {
    uint8_t key   = 0;
    int64_t value = 0;
    bool operator==(const CatalogConfigEntry&) const = default;
};
inline constexpr std::size_t kCatalogConfigEntryBytes = 1 + 8;
/// The most entries one AlterTopicConfig can carry (one per alterable key, ruling 5).
inline constexpr std::size_t kMaxCatalogConfigEntries =
    static_cast<std::size_t>(rune::protocol::kLastConfigKey) - static_cast<std::size_t>(rune::protocol::kFirstConfigKey) + 1;
struct CatalogTopicConfigMsg {
    int32_t                                                  topicId = -1;
    uint16_t                                                 count   = 0;
    std::array<CatalogConfigEntry, kMaxCatalogConfigEntries> entries{};   // fixed: no allocation
};
[[nodiscard]] inline constexpr std::size_t catalogTopicConfigWireLen(std::size_t count) noexcept {
    return 4 + 2 + count * kCatalogConfigEntryBytes;
}
/// Writes the header; the caller then writes each entry with writeCatalogTopicConfigEntry.
inline void writeCatalogTopicConfigHeader(uint8_t* p, int32_t topicId, uint16_t count) noexcept {
    admin_wire::putI32(p, topicId);
    admin_wire::putU16(p + 4, count);
}
inline void writeCatalogTopicConfigEntry(uint8_t* p, std::size_t i, uint8_t key, int64_t value) noexcept {
    uint8_t* e = p + 6 + i * kCatalogConfigEntryBytes;
    e[0] = key;
    admin_wire::putI64(e + 1, value);
}
[[nodiscard]] inline bool decodeCatalogTopicConfig(const uint8_t* buf, std::size_t len, CatalogTopicConfigMsg& m) {
    if (len < 6) return false;
    m.topicId = admin_wire::getI32(buf);
    m.count   = admin_wire::getU16(buf + 4);
    if (m.count == 0 || m.count > kMaxCatalogConfigEntries) return false;
    if (len != catalogTopicConfigWireLen(m.count)) return false;
    for (std::size_t i = 0; i < m.count; ++i) {
        const uint8_t* e = buf + 6 + i * kCatalogConfigEntryBytes;
        if (e[0] < static_cast<uint8_t>(rune::protocol::kFirstConfigKey) ||
            e[0] > static_cast<uint8_t>(rune::protocol::kLastConfigKey))
            return false;
        m.entries[i] = CatalogConfigEntry{e[0], admin_wire::getI64(e + 1)};
    }
    return m.topicId >= 0;
}

inline constexpr std::size_t kViewUninstallWireLen = 4;
inline void writeViewUninstall(uint8_t* p, int32_t viewTopicId) noexcept { admin_wire::putI32(p, viewTopicId); }
/// The view's sink topic id, or -1 on a malformed frame.
[[nodiscard]] inline int32_t decodeViewUninstall(const uint8_t* buf, std::size_t len) noexcept {
    if (len != kViewUninstallWireLen) return -1;
    const int32_t id = admin_wire::getI32(buf);
    return id >= 0 ? id : -1;
}

struct CatalogHaltedMsg {
    int64_t     offset   = -1;
    uint64_t    expected = 0;
    uint64_t    actual   = 0;
    std::string detail;
};
inline constexpr std::size_t kCatalogHaltedFixedBytes = 8 + 8 + 8;
/// `detail` is truncated by the caller to fit the ring's max message.
inline void writeCatalogHalted(uint8_t* p, int64_t offset, uint64_t expected, uint64_t actual,
                               std::string_view detail) noexcept {
    admin_wire::putI64(p, offset);
    admin_wire::putU64(p + 8, expected);
    admin_wire::putU64(p + 16, actual);
    if (!detail.empty()) std::memcpy(p + kCatalogHaltedFixedBytes, detail.data(), detail.size());
}
[[nodiscard]] inline bool decodeCatalogHalted(const uint8_t* buf, std::size_t len, CatalogHaltedMsg& m) {
    if (len < kCatalogHaltedFixedBytes) return false;
    m.offset   = admin_wire::getI64(buf);
    m.expected = admin_wire::getU64(buf + 8);
    m.actual   = admin_wire::getU64(buf + 16);
    m.detail.assign(reinterpret_cast<const char*>(buf) + kCatalogHaltedFixedBytes, len - kCatalogHaltedFixedBytes);
    return true;
}

inline constexpr std::size_t kCatalogAppliedWireLen = 8 + 8;
inline void writeCatalogApplied(uint8_t* p, int64_t offset, uint64_t digest) noexcept {
    admin_wire::putI64(p, offset);
    admin_wire::putU64(p + 8, digest);
}
[[nodiscard]] inline bool decodeCatalogApplied(const uint8_t* buf, std::size_t len, int64_t& offset, uint64_t& digest) noexcept {
    if (len != kCatalogAppliedWireLen) return false;
    offset = admin_wire::getI64(buf);
    digest = admin_wire::getU64(buf + 8);
    return offset >= 0;
}

/// Claim `len` bytes of type `typeId` on `ring`, let `write(uint8_t*)` fill them,
/// commit.  false (nothing written) when the ring is full.
template <typename Write>
[[nodiscard]] inline bool encodeA2c(OneToOneByteRing& ring, int32_t typeId, std::size_t len, Write&& write) {
    const auto idx = ring.tryClaim(typeId, len);
    if (idx < 0) return false;
    write(ring.base() + idx);
    ring.commit(idx);
    return true;
}

// ---- F3 round 7, item 4: descriptor size is bounded BY CONSTRUCTION ----
// A descriptor reaches the admin thread only inside a DefineTopic on the
// request ring (O2O ring: maxMsgLength = capacity / 8), whose fixed framing is
// session(8)+corr(8)+partitions(4)+validate(1)+nameLen(2)+keyLen(2)+blobLen(4)
// + the trailing persistence byte (1, Phase 2b E: always written on this in-process
// ring, so a transient DefineTopic with a max-size descriptor still fits).
// The SchemaUpdate that re-posts it on the admin→control ring carries only a
// 4-byte topicId in front, so every committed descriptor fits — the
// "descriptor too large → validator not pushed" path in
// AdminWorkerAgent::postSchemaUpdate is unreachable.
inline constexpr std::size_t kDefineTopicFixedBytes = 8 + 8 + 4 + 1 + 2 + 2 + 4 + 1;
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
