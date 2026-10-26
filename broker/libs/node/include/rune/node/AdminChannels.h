#pragma once
// AdminChannels.h — shared-nothing cross-thread channels of the DDL control
// plane (RULING-2; catalog history Task 9).
//
// The AdminWorker runs on its OWN thread and SOLELY owns the __catalog replica,
// the CatalogApplier and the registries it projects.  Every cross-thread hop is
// an SPSC ring (HARD RULE #2, Aeron-style), never a shared mutable field:
//
//   1. gateway → admin   (AdminRequestRing)   — every DDL request + GetSchema
//   2. control → admin   (controlToAdminRing) — dropped-dir purges (Task 8)
//   3. control → admin   (AdminRoleRing)      — the __catalog role + its AlterIsr answers
//   4. admin   → control (failure ring)       — the applier's effects, storage failures
//   5. admin   → control (egress ring)        — DDL answer frames for the session egress
//
// Each ring has exactly ONE producer thread and ONE consumer thread.  The
// create-topic round-trip (admin → control "create this topic", control → admin
// "here is its id") is gone (Task 9): a topic's id comes from the catalog, and
// the control thread builds it from the applier's kA2cCatalogTopicApplied effect.
//
// A ring frame is a flat byte copy: a small fixed header plus the variable-length
// bytes inline; the consumer rebuilds owning strings / vectors on its own thread.
// Cold path (DDL is rare): the per-op owning containers are acceptable here.

#include <rune/node/OneToOneByteRing.h>
#include <rune/protocol/CatalogTypes.h>
#include <rune/protocol/NamespaceLimits.h>   // NamespaceDdlMsg (namespaces Task 7)
#include <rune/catalog/CatalogEvent.h>   // rune::catalog::ConfigChange (AlterTopicConfig)
#include <rune/catalog/NamespaceRegistry.h>   // NamespaceRecord (kA2cNamespaceUpserted, namespaces Task 9)
#include <rune/protocol/SecurityTypes.h>
#include <rune/security/SecurityEvent.h>   // ScramVerifier (the security DDL messages, authentication Task 10)
#include <rune/peerlink/PeerLinkKeys.h>      // LinkInstall (kMsgPeerLinkKeys, issue #241)

#include <concurrent/AtomicBuffer.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <optional>
#include <utility>
#include <variant>
#include <vector>

namespace rune::node {

// ---------------------------------------------------------------------------
// Ring message type ids (the O2ORingBuffer msgTypeId field).
// ---------------------------------------------------------------------------
enum : int32_t {
    kMsgDefineTopic       = 1,
    kMsgGetSchema         = 2,
    // 3, 4: the retired create-topic request / reply (Task 9) — never reused.
    kMsgAdminRole         = 5,   // RETIRED (Task 17): the __topic_schemas / __views role; nothing posts it, the admin thread refuses it
    kMsgEgressFrame       = 6,   // admin → control: [sessionId:i64][response frame]
    // Authentication Task 10 (D24): admin → control on the egress ring - a security DDL OK that may leave only once THIS
    // broker enforces the event: [sessionId:i64][eventOffset:i64][response frame] (the SecurityAnswerGate).
    kMsgEgressFrameGated  = 27,
    kMsgAlterTopic        = 7,   // gateway → admin: ALTER TOPIC (Task 11)
    kMsgCreateView        = 8,   // gateway → admin: CREATE VIEW (Task 19)
    // Catalog history Task 6 (control → admin, on the AdminRoleRing):
    kMsgCatalogRole       = 9,   // the __catalog replica's role (AdminRoleMsg, full form)
    kMsgCatalogAlterIsrResponse = 10,  // the controller's AlterIsrResponse for (-6, 0), verbatim SBE
    // Catalog history Task 8 (control → admin, on controlToAdminRing — the former
    // create-topic reply ring): [path bytes] — a dropped topic's renamed dir
    // (dataDir/dropped/<id>.<name>) to remove (cold; std::filesystem::remove_all on
    // the admin thread; a failure is logged and the dir stays for the boot purge).
    kMsgPurgeDroppedDir   = 16,
    // Catalog history Task 11 fix round 1 (I2), control → admin on
    // controlToAdminRing: the PLACEMENT VIEW (PlacementViewMsg) — how many
    // brokers a new partition may be placed on and which are catalog-halted.
    // Posted on change only; the leader's dry-run refuses rf above it.
    kMsgPlacementView     = 17,
    // Catalog history Task 12 fix round 0 — control → admin on controlToAdminRing:
    // catalog-read flow control (CatalogReadFlow → CatalogReadServer).
    kMsgCatalogReadPaused = 18,   // [session:i64][subscriptionId:i64][baseOffset:i64][droppedMarker:u8]
    kMsgCatalogReadResume = 19,   // [session:i64][subscriptionId:i64]
    kMsgCatalogSessionGone = 20,  // [session:i64] — fix round 3: a probe found the session gone
    // ISR v3 Step 10 (control → admin, on the AdminRoleRing): the __catalog leader role was HELD (the storage gate is
    // closed) - an AdminRoleMsg {leader false, leaderId -1}; the admin thread asks for a failover and demotes.
    kMsgCatalogHoldLeadership = 21,
    // Authentication Task 9 (D39) - control -> admin on the AdminRoleRing, the `__security` replica's counterparts of
    // 9 / 10 / 21: its role (AdminRoleMsg, full form), the controller's AlterIsrResponse for (-7, 0) verbatim, and the
    // held leader role (storage gate closed).
    kMsgSecurityRole             = 22,
    kMsgSecurityAlterIsrResponse = 23,
    kMsgSecurityHoldLeadership   = 24,
    // control -> admin on controlToAdminRing (Task 15 posts it): the peer link keys of the admin plane.
    kMsgPeerLinkKeys             = 25,
};

/// kMsgPlacementView: [placeable:i32][haltedCount:u16][halted ids: i32 * haltedCount].
struct PlacementViewMsg {
    static constexpr std::size_t kMaxHalted = 64;
    int32_t                            placeable   = 0;
    uint16_t                           haltedCount = 0;
    std::array<int32_t, kMaxHalted>    halted{};
    bool operator==(const PlacementViewMsg&) const = default;
};
[[nodiscard]] inline std::size_t placementViewWireLen(const PlacementViewMsg& m) noexcept {
    return 4u + 2u + 4u * static_cast<std::size_t>(m.haltedCount);
}
// Catalog history Task 9 — gateway → admin, on the AdminRequestRing (ring-scoped
// ids: 10 is kMsgCatalogAlterIsrResponse on the ROLE ring, a different ring).
enum : int32_t {
    kMsgDropTopic         = 10,
    kMsgDropView          = 11,
    kMsgAlterView         = 12,
    kMsgAlterTopicConfig  = 13,
    // Catalog history Task 12 — gateway → admin: catalog reads (CatalogReadServer).
    kMsgCatalogSubscribe   = 14,   // [session:i64][corr:i64][fromOffset:i64]
    kMsgCatalogUnsubscribe = 15,   // [session:i64][subscriptionId:i64]; -1 = the session closed (all of it)
    // Authentication Task 10 - gateway -> admin: the security write path and `__security` reads.
    kMsgSecurityDdl         = 30,
    kMsgSecurityBootstrap   = 31,
    kMsgSecuritySubscribe   = 32,  // the catalog layout: [session:i64][corr:i64][fromOffset:i64]
    kMsgSecurityUnsubscribe = 33,  // the catalog layout: [session:i64][subscriptionId:i64]; -1 = the session closed
    // Namespaces Task 7 - gateway -> admin: namespace DDL (81) and ListNamespaces (82).
    kMsgNamespaceDdl        = 34,
    kMsgListNamespaces      = 35,
};

/// kMsgCatalogSubscribe / kMsgCatalogUnsubscribe (fixed layouts).
struct CatalogSubscribeMsg   { int64_t session = 0; int64_t corr = 0; int64_t fromOffset = 0; };
struct CatalogUnsubscribeMsg { int64_t session = 0; int64_t subscriptionId = 0; };
inline constexpr std::size_t kCatalogSubscribeWireLen   = 24;
inline constexpr std::size_t kCatalogUnsubscribeWireLen = 16;

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
    // 4: the retired kA2cTopicCommitted (Task 9: a catalog topic is built from
    //    kA2cCatalogTopicApplied) — never reused.
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
    // Catalog history Task 12 (Task 8 review N5): a VIEW SINK applied — the
    // kA2cCatalogTopicApplied layout; the control thread also marks the sink
    // "awaiting its plan" until the view's plan reached every worker.
    kA2cCatalogSinkApplied  = 12,
    // Authentication Task 9 (D29/D39): the `__security` replica's AlterIsr proposal (verbatim SBE AlterIsrMsg), every
    // applied event's bytes for the control thread's SecurityView ([offset:i64][event bytes]) and the applier's halt
    // (the kA2cCatalogHalted layout).
    kA2cSecurityAlterIsr    = 20,
    kA2cSecurityApplied     = 21,
    kA2cSecurityHalted      = 22,
    // Authentication Task 16: the admin plane's cumulative dropped inter-broker frames, [count:u64] (paced, only on a
    // change) - the control thread folds it into counter 1'000'235.
    kA2cPeerFrameDrops      = 23,
    // Issues #15 / #111 (decision M7): the admin thread's verdict on whether this broker's `__security` state is caught
    // up and fresh (SecurityCatchUp), posted on change (layout below; #603 adds whether its view is behind the committed
    // HW it knows).  On the SAME FIFO as kA2cSecurityApplied, behind every applied event of the state it vouches for.
    kA2cSecurityServing     = 24,
    // Phase 2c (subscription filters): a typed topic's EVOLVING descriptor at its current version, [topicId:i32]
    // [version:u16][blob] (the kA2cSchemaUpdate layout).  The control thread hands it to the gateway, which fans it
    // out to every worker ahead of any filtered Subscribe.
    kA2cFilterSchema        = 25,
    // Namespaces Task 9: a namespace record the applier created or changed (live, and every namespace again at a boot
    // stash) -> the gateway's NamespaceMirror, which the login resolves `ns/user` against.  Layout below.
    kA2cNamespaceUpserted   = 26,
};

// ---------------------------------------------------------------------------
// Ring capacities.  DDL is rare; a small ring is plenty.  Power-of-two payload.
// ---------------------------------------------------------------------------
inline constexpr int kAdminRequestRingBytes     = 1 << 20;  // 1 MiB (descriptor blobs)
inline constexpr int kControlToAdminRingBytes   = 1 << 16;  // 64 KiB (dropped-dir purges)
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
inline void putU32(uint8_t* p, uint32_t v) { std::memcpy(p, &v, 4); }
inline uint32_t getU32(const uint8_t* p) { uint32_t v; std::memcpy(&v, p, 4); return v; }

} // namespace admin_wire

// ---------------------------------------------------------------------------
// Catalog history Task 9: what every DDL carries besides its operation (spec §4
// principal + statement + replayedFrom; ruling 13).  `requestId` is the client's
// 128-bit id of the logical DDL, stable across retries (F14); {0, 0} = a pre-v8
// client (no DDL extension on the wire).  `sessionId` / `sourceAddress` are the
// gateway's (the session id and the session's egress channel), never the
// client's word.  Owning form after drain (cold: DDL is rare).
// ---------------------------------------------------------------------------
struct DdlMeta {
    std::string                                statement;
    std::string                                clientName;
    std::string                                sourceAddress;
    std::string                                authenticated;   // the session's user (authentication Task 10; "" = anonymous)
    int64_t                                    sessionId = 0;
    rune::protocol::RequestId                  requestId{};
    std::optional<rune::catalog::ReplayedFrom> replayedFrom;   // set only by `rune catalog replay` (spec §7)
    bool operator==(const DdlMeta&) const = default;
};

// ---------------------------------------------------------------------------
// The DDL requests (gateway → admin, AdminRequestRing).  Owning forms after
// drain.  A frame the ring decode cannot read — short, over-long, or a byte out
// of the range it never casts — decodes to its own alternative, MalformedDdl
// (hard rule 3: no flag beside fields that would mean nothing): session / corr
// survive (when present) so the admin thread answers it, never dropped.
// ---------------------------------------------------------------------------

/// DefineTopic: an empty `blob` is an UNTYPED topic (ruling 3); a non-empty one
/// must deserialize.  v8 adds the replication factor (0 = every broker), the
/// creation config and the meta.
struct DefineTopicMsg {
    int64_t                     session        = 0;
    int64_t                     corr           = 0;
    int32_t                     partitionCount = 1;
    bool                        validate       = false;
    std::string                 name;
    std::string                 keyField;
    std::vector<uint8_t>        blob;
    uint8_t                     persistence    = 0;   // Phase 2b E: 0 = disk, 1 = transient
    int16_t                     replicationFactor = 0;
    rune::protocol::TopicConfig config;
    DdlMeta                     meta;
    /// Issue #129: the gateway's verdict on adopting a schema onto an EXISTING untyped topic (ALTER on it):
    /// "" = allowed (also every in-process / admin-level submit, which no session authorizes), otherwise the
    /// NotAuthorized detail the admin thread answers when the name turns out to be such a topic.
    std::string                 adoptDenial;
    uint32_t                    nsId = 0;   // namespaces Task 7: the namespace the object lives in (0 = default)
};

/// AlterTopic: the client's CAS expectation (expectedVersion = the topic's SCHEMA
/// version; 0 = don't check) and the opaque ALTER ops (encodeAlterOps wire bytes).
struct AlterTopicMsg {
    int64_t              session         = 0;
    int64_t              corr            = 0;
    uint16_t             expectedVersion = 0;   // 0 = unconditional (no CAS)
    std::string          topicName;
    std::vector<uint8_t> opsBlob;               // encodeAlterOps() wire bytes
    DdlMeta              meta;
    uint32_t                    nsId = 0;   // namespaces Task 7: the namespace the object lives in (0 = default)
};

/// CreateView: `planBlob` is the authored rune::view::ViewPlan (serializeAuthored).
struct CreateViewMsg {
    int64_t              session     = 0;
    int64_t              corr        = 0;
    uint8_t              persistence = 0;   // 0 = persisted, 1 = transient
    std::string          viewName;
    std::string          keyField;
    std::vector<uint8_t> planBlob;          // authored ViewPlan (serializeAuthored)
    DdlMeta              meta;
    uint32_t                    nsId = 0;   // namespaces Task 7: the namespace the object lives in (0 = default)
};

struct DropTopicMsg {
    int64_t     session = 0;
    int64_t     corr    = 0;
    std::string name;
    DdlMeta     meta;
    uint32_t    nsId    = 0;
};

struct DropViewMsg {
    int64_t     session = 0;
    int64_t     corr    = 0;
    std::string name;
    DdlMeta     meta;
    uint32_t    nsId    = 0;
};

/// AlterView (ruling 4): the view's plan and output replaced under the same
/// objectId with a fresh sink.  expectedVersion = the view's OBJECT version (0 = no check).
struct AlterViewMsg {
    int64_t              session         = 0;
    int64_t              corr            = 0;
    std::string          name;
    std::string          keyField;
    uint8_t              persistence     = 0;
    uint32_t             expectedVersion = 0;
    std::vector<uint8_t> planBlob;
    DdlMeta              meta;
    uint32_t             nsId            = 0;
};

/// AlterTopicConfig (ruling 5): expectedVersion = the topic's OBJECT version (0 = no check).
struct AlterTopicConfigMsg {
    int64_t                                  session         = 0;
    int64_t                                  corr            = 0;
    std::string                              name;
    uint32_t                                 expectedVersion = 0;
    std::vector<rune::catalog::ConfigChange> changes;
    DdlMeta                                  meta;
    uint32_t                                 nsId            = 0;
};

/// The client-protocol template id each DDL request arrived as (TemplateId values).
inline constexpr uint16_t kTidDefineTopic      = 64;
inline constexpr uint16_t kTidAlterTopic       = 68;
inline constexpr uint16_t kTidCreateView       = 70;
inline constexpr uint16_t kTidDropTopic        = 74;
inline constexpr uint16_t kTidAlterTopicConfig = 75;
inline constexpr uint16_t kTidDropView         = 76;
inline constexpr uint16_t kTidAlterView        = 77;
inline constexpr uint16_t kTidNamespaceDdl     = 81;
inline constexpr uint16_t kTidListNamespaces   = 82;

/// Namespaces Task 7: NamespaceDdl (81) — Create / SetLimits.  `mask` bit i = limits field i was given (the rest
/// resolve at the admin thread: Create from the cluster defaults, SetLimits from the current record).  `denial` is
/// the gateway's verdict ("" = cluster admin authorized); the admin thread answers a dedupe retry first, then 45.
/// `clientVersion` picks NamespaceLimitExceeded (52, v14+) or InvalidRequest (39) for an older client.
struct NamespaceDdlMsg {
    int64_t                          session = 0;
    int64_t                          corr    = 0;
    DdlMeta                          meta;            // the request id lives here
    uint8_t                          kind    = 0;     // rune::protocol::client::NamespaceDdlKind (1 Create, 2 SetLimits)
    std::string                      name;
    std::array<uint8_t, 4>           mask{};
    rune::protocol::NamespaceLimits  limits;
    std::string                      denial;
    uint16_t                         clientVersion = 0;
};

/// Namespaces Task 7: ListNamespaces (82) — a read, answered on the admin thread from its applied registry (no
/// append).  afterNsId is exclusive; max 0 = 100.  A caller that is not a cluster admin sees only `callerNsId`.
struct ListNamespacesMsg {
    int64_t  session      = 0;
    int64_t  corr         = 0;
    uint32_t afterNsId    = 0;
    uint16_t max          = 0;
    uint32_t callerNsId   = 0;
    uint8_t  clusterAdmin = 0;   // 0 / 1
};

/// A DDL frame the ring decode could not read: what can still be answered (the
/// request's template id, session and correlation id — 0 when even those are
/// missing) and why.  Answered once, in the request's own response type.
struct MalformedDdl {
    uint16_t    templateId = 0;
    int64_t     session    = 0;
    int64_t     corr       = 0;
    std::string reason;
    bool operator==(const MalformedDdl&) const = default;
};

/// One queued DDL: exactly one request message (the alternative IS the kind).
using DdlRequest = std::variant<DefineTopicMsg, AlterTopicMsg, CreateViewMsg, DropTopicMsg, DropViewMsg,
                                AlterViewMsg, AlterTopicConfigMsg, NamespaceDdlMsg, ListNamespacesMsg, MalformedDdl>;

// ---------------------------------------------------------------------------
// Authentication Task 10 (D36): the security DDL (79) and the one-time bootstrap (80), gateway → admin on the request
// ring (kMsgSecurityDdl / kMsgSecurityBootstrap).  The fields are the wire's (range-checked again on decode - never
// cast); `meta` carries the REDACTED statement, the session's user (`authenticated`), the request id.
// ---------------------------------------------------------------------------
inline constexpr uint16_t kTidSecurityDdl       = 79;
inline constexpr uint16_t kTidSecurityBootstrap = 80;

struct SecurityDdlMsg {
    int64_t                       session = 0;
    int64_t                       corr    = 0;
    DdlMeta                       meta;
    uint8_t                       kind = 0;               // rune::protocol::client::SecurityOpKind (1..9)
    std::string                   name;                   // the user / role the op targets (Grant/Revoke: the grantee)
    uint8_t                       change = 0;             // UserChangeKind (AlterUser)
    uint8_t                       passwordIndex = 0;      // DropPassword
    uint8_t                       terminateSessions = 0;  // SetPassword
    rune::security::ScramVerifier verifier;               // CreateUser, AddPassword, SetPassword
    uint8_t                       granteeKind = 0, op = 0, resourceType = 0, patternKind = 0;   // Grant / Revoke
    std::string                   pattern;
    std::string                   roleName;               // GrantRole / RevokeRole
    uint8_t                       force = 0;              // issue #121: FORCE - the last security admin may go (0 / 1)
    /// PR #410 review F1: the gateway's verdict ("" = authorized; otherwise the NotAuthorized detail).  The leader
    /// answers a retry of an applied request with its original result first, then refuses a denied one 45.
    std::string                   denial;
    /// PR #410 review F2: the gateway checked an own-credential proof for this request (whatever the outcome).
    uint8_t                       proofChecked = 0;
    /// Namespaces Task 8: the namespace of the session's user (0 = `default`).  An unqualified `name` / `roleName`
    /// resolves into it, and the leader's namespace guard judges the caller as a user of it.
    uint32_t                      callerNsId = 0;
};

/// SecurityBootstrap: the gateway already verified the MAC with its ClusterKeys; the nonce is kept as the request's
/// identity (a retried bootstrap dedupes on it).
struct SecurityBootstrapMsg {
    int64_t                       session = 0;
    int64_t                       corr    = 0;
    DdlMeta                       meta;                   // clientName, sourceAddress, sessionId
    std::string                   adminName;
    rune::security::ScramVerifier verifier;
    std::array<uint8_t, rune::security::kFakeSaltSeedBytes> fakeSaltSeed{};
    std::array<uint8_t, 16>       nonce{};
};

using SecurityRequest = std::variant<SecurityDdlMsg, SecurityBootstrapMsg, MalformedDdl>;

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
    uint32_t    nsId = 0;   // namespaces Task 7: the namespace the name resolves in (0 = default)
};


// ---------------------------------------------------------------------------
// AdminRole event (control → admin).  The bridge posts a Leader/Follower role
// transition for the __catalog replica (kMsgCatalogRole); the admin thread applies it on its OWN
// thread (becomeLeader/becomeFollower) — no cross-thread role_ write.
// ---------------------------------------------------------------------------
struct AdminRoleMsg {
    /// The largest ISR a role carries (== AlterIsrMsg::kMaxIsr, FollowerSlots::kMaxReplicas).
    static constexpr int kMaxIsr = 16;
    bool     leader   = false;   // true = become Leader, false = become Follower
    int32_t  epoch    = 0;       // the leader epoch (the committed assignment's leaderEpoch)
    int32_t  leaderId = 0;       // used when leader==false (follower); -1 = no leader
    // Catalog history Task 6 (kMsgCatalogRole only): the committed assignment's
    // ISR and partitionEpoch (the AlterIsr compare-and-set version).
    // Fixed-capacity (no allocation on the ring path).
    int32_t  partitionEpoch = -1;
    int32_t  isrCount       = 0;
    std::array<int32_t, kMaxIsr> isr{};
};

// ===========================================================================
// Encoders / decoders
// ===========================================================================

// ---- The DDL request codecs (catalog history Task 9) ----
// One LAYOUT per message, written once and used twice: by the Sizer (the exact
// frame length, and whether every length fits its prefix) and by the Writer.
// The decoders are checked (the Reader): a short / over-long frame, or a byte
// out of its range, decodes to MalformedDdl (with its reason) — never a default
// applied as a real request.
//
//   meta    : statement:u32+bytes, clientName:u16+bytes, sourceAddress:u16+bytes,
//             sessionId:i64, requestIdHi:u64, requestIdLo:u64, hasReplayedFrom:u8,
//             [clusterId:u16+bytes, offset:i64, principal:u16+bytes]
//   config  : the TopicConfig block (kTopicConfigFixedBytes + its two str16s)
//   Define  : session, corr, partitions:i32, validate:u8, name, keyField,
//             blob:u32+bytes, persistence:u8, rf:i16, config, meta
//   Alter   : session, corr, expectedVersion:u16, name, ops:u32+bytes, meta
//   View    : session, corr, persistence:u8, name, keyField, plan:u32+bytes, meta
//   DropT/V : session, corr, name, meta
//   AlterV  : session, corr, name, keyField, persistence:u8, expectedVersion:u32,
//             plan:u32+bytes, meta
//   Config  : session, corr, name, expectedVersion:u32, count:u16, (key:u8, value:i64)*, meta
namespace admin_wire {

inline constexpr std::size_t kStr16Max = 0xFFFF;

struct Sizer {
    std::size_t n    = 0;
    bool        fits = true;
    void i64(int64_t) noexcept { n += 8; }
    void u64(uint64_t) noexcept { n += 8; }
    void i32(int32_t) noexcept { n += 4; }
    void u32(uint32_t) noexcept { n += 4; }
    void i16(int16_t) noexcept { n += 2; }
    void u16(uint16_t) noexcept { n += 2; }
    void u8(uint8_t) noexcept { n += 1; }
    void str16(std::string_view s) noexcept { fits = fits && s.size() <= kStr16Max; n += 2 + s.size(); }
    void bytes32(std::span<const uint8_t> b) noexcept {
        fits = fits && b.size() <= 0x7FFF'FFFFu;
        n += 4 + b.size();
    }
    void str32(std::string_view s) noexcept { bytes32({reinterpret_cast<const uint8_t*>(s.data()), s.size()}); }
};

struct Writer {
    uint8_t* p;
    void i64(int64_t v) noexcept { putI64(p, v); p += 8; }
    void u64(uint64_t v) noexcept { std::memcpy(p, &v, 8); p += 8; }
    void i32(int32_t v) noexcept { putI32(p, v); p += 4; }
    void u32(uint32_t v) noexcept { std::memcpy(p, &v, 4); p += 4; }
    void i16(int16_t v) noexcept { std::memcpy(p, &v, 2); p += 2; }
    void u16(uint16_t v) noexcept { putU16(p, v); p += 2; }
    void u8(uint8_t v) noexcept { *p++ = v; }
    void str16(std::string_view s) noexcept {
        u16(static_cast<uint16_t>(s.size()));
        if (!s.empty()) std::memcpy(p, s.data(), s.size());
        p += s.size();
    }
    void bytes32(std::span<const uint8_t> b) noexcept {
        u32(static_cast<uint32_t>(b.size()));
        if (!b.empty()) std::memcpy(p, b.data(), b.size());
        p += b.size();
    }
    void str32(std::string_view s) noexcept { bytes32({reinterpret_cast<const uint8_t*>(s.data()), s.size()}); }
};

/// Checked reader: every short read clears `ok` (and reads nothing further).
struct Reader {
    const uint8_t* b;
    std::size_t    len;
    std::size_t    pos = 0;
    bool           ok  = true;
    const char*    why = nullptr;   // the first failure's reason
    void fail(const char* reason) noexcept {
        if (ok) why = reason;
        ok = false;
    }
    bool need(std::size_t k) noexcept {
        if (!ok || pos > len || len - pos < k) { fail("the frame is shorter than its fields"); return false; }
        return true;
    }
    int64_t  i64() noexcept { if (!need(8)) return 0; const int64_t v = getI64(b + pos); pos += 8; return v; }
    uint64_t u64() noexcept { if (!need(8)) return 0; uint64_t v; std::memcpy(&v, b + pos, 8); pos += 8; return v; }
    int32_t  i32() noexcept { if (!need(4)) return 0; const int32_t v = getI32(b + pos); pos += 4; return v; }
    uint32_t u32() noexcept { if (!need(4)) return 0; uint32_t v; std::memcpy(&v, b + pos, 4); pos += 4; return v; }
    int16_t  i16() noexcept { if (!need(2)) return 0; int16_t v; std::memcpy(&v, b + pos, 2); pos += 2; return v; }
    uint16_t u16() noexcept { if (!need(2)) return 0; const uint16_t v = getU16(b + pos); pos += 2; return v; }
    uint8_t  u8() noexcept { if (!need(1)) return 0; return b[pos++]; }
    void str16(std::string& out) {
        const uint16_t n = u16();
        if (!need(n)) return;
        out.assign(reinterpret_cast<const char*>(b + pos), n);
        pos += n;
    }
    void bytes32(std::vector<uint8_t>& out) {
        const uint32_t n = u32();
        if (!need(n)) return;
        out.assign(b + pos, b + pos + n);
        pos += n;
    }
    void str32(std::string& out) {
        const uint32_t n = u32();
        if (!need(n)) return;
        out.assign(reinterpret_cast<const char*>(b + pos), n);
        pos += n;
    }
    /// The whole frame was consumed exactly (false — with a reason — otherwise).
    [[nodiscard]] bool done() noexcept {
        if (ok && pos != len) fail("trailing bytes after the last field");
        return ok;
    }
};

template <class W>
void layoutMeta(W& w, const DdlMeta& m) {
    w.str32(m.statement);
    w.str16(m.clientName);
    w.str16(m.sourceAddress);
    w.str16(m.authenticated);
    w.i64(m.sessionId);
    w.u64(m.requestId.hi);
    w.u64(m.requestId.lo);
    w.u8(m.replayedFrom ? 1 : 0);
    if (m.replayedFrom) {
        w.str16(m.replayedFrom->clusterId);
        w.i64(m.replayedFrom->offset);
        w.str16(m.replayedFrom->principal);
    }
}
inline void readMeta(Reader& r, DdlMeta& m) {
    r.str32(m.statement);
    r.str16(m.clientName);
    r.str16(m.sourceAddress);
    r.str16(m.authenticated);
    m.sessionId    = r.i64();
    m.requestId.hi = r.u64();
    m.requestId.lo = r.u64();
    const uint8_t rf = r.u8();
    if (rf > 1) { r.fail("a replayedFrom flag other than 0/1"); return; }   // never cast
    if (rf == 1) {
        rune::catalog::ReplayedFrom from;
        r.str16(from.clusterId);
        from.offset = r.i64();
        r.str16(from.principal);
        m.replayedFrom = std::move(from);
    }
}

template <class W>
void layoutConfig(W& w, const rune::protocol::TopicConfig& c) {
    w.u8(c.cleanupPolicy);
    w.i64(c.retentionMs);
    w.i64(c.retentionBytes);
    w.i64(c.deleteRetentionMs);
    w.i32(c.minCleanableDirtyRatioPpm);
    w.i64(c.segmentRollBytes);
    w.i64(c.minCompactionLagMs);
    w.i64(c.maxCompactionLagMs);
    w.i64(c.retentionCheckMs);
    w.i64(c.fileDeleteDelayMs);
    w.u8(c.durability);
    w.u8(c.durabilityTier);
    w.str16(c.coPartitionGroup);
    w.str16(c.partitionKey);
}
inline void readConfig(Reader& r, rune::protocol::TopicConfig& c) {
    c.cleanupPolicy             = r.u8();
    c.retentionMs               = r.i64();
    c.retentionBytes            = r.i64();
    c.deleteRetentionMs         = r.i64();
    c.minCleanableDirtyRatioPpm = r.i32();
    c.segmentRollBytes          = r.i64();
    c.minCompactionLagMs        = r.i64();
    c.maxCompactionLagMs        = r.i64();
    c.retentionCheckMs          = r.i64();
    c.fileDeleteDelayMs         = r.i64();
    c.durability                = r.u8();
    c.durabilityTier            = r.u8();
    r.str16(c.coPartitionGroup);
    r.str16(c.partitionKey);
}

inline std::span<const uint8_t> span8(const std::vector<uint8_t>& v) noexcept { return {v.data(), v.size()}; }

template <class W> void layout(W& w, const DefineTopicMsg& m) {
    w.i64(m.session); w.i64(m.corr); w.i32(m.partitionCount); w.u8(m.validate ? 1 : 0);
    w.str16(m.name); w.str16(m.keyField); w.bytes32(span8(m.blob)); w.u8(m.persistence);
    w.i16(m.replicationFactor); layoutConfig(w, m.config); layoutMeta(w, m.meta); w.str16(m.adoptDenial);
    w.u32(m.nsId);
}
template <class W> void layout(W& w, const AlterTopicMsg& m) {
    w.i64(m.session); w.i64(m.corr); w.u16(m.expectedVersion); w.str16(m.topicName);
    w.bytes32(span8(m.opsBlob)); layoutMeta(w, m.meta); w.u32(m.nsId);
}
template <class W> void layout(W& w, const CreateViewMsg& m) {
    w.i64(m.session); w.i64(m.corr); w.u8(m.persistence); w.str16(m.viewName); w.str16(m.keyField);
    w.bytes32(span8(m.planBlob)); layoutMeta(w, m.meta); w.u32(m.nsId);
}
template <class W> void layout(W& w, const DropTopicMsg& m) {
    w.i64(m.session); w.i64(m.corr); w.str16(m.name); layoutMeta(w, m.meta); w.u32(m.nsId);
}
template <class W> void layout(W& w, const DropViewMsg& m) {
    w.i64(m.session); w.i64(m.corr); w.str16(m.name); layoutMeta(w, m.meta); w.u32(m.nsId);
}
template <class W> void layout(W& w, const AlterViewMsg& m) {
    w.i64(m.session); w.i64(m.corr); w.str16(m.name); w.str16(m.keyField); w.u8(m.persistence);
    w.u32(m.expectedVersion); w.bytes32(span8(m.planBlob)); layoutMeta(w, m.meta); w.u32(m.nsId);
}
template <class W> void layout(W& w, const AlterTopicConfigMsg& m) {
    w.i64(m.session); w.i64(m.corr); w.str16(m.name); w.u32(m.expectedVersion);
    w.u16(static_cast<uint16_t>(m.changes.size()));
    for (const auto& c : m.changes) { w.u8(static_cast<uint8_t>(c.key)); w.i64(c.value); }
    layoutMeta(w, m.meta); w.u32(m.nsId);
}
template <class W> void layout(W& w, const NamespaceDdlMsg& m) {
    w.i64(m.session); w.i64(m.corr); layoutMeta(w, m.meta); w.u8(m.kind); w.str16(m.name);
    w.bytes32({m.mask.data(), m.mask.size()});
    std::array<uint8_t, rune::protocol::kNamespaceLimitsBytes> lim{};
    rune::protocol::encodeLimits(m.limits, lim.data());
    w.bytes32({lim.data(), lim.size()});
    w.str16(m.denial); w.u16(m.clientVersion);
}
template <class W> void layout(W& w, const ListNamespacesMsg& m) {
    w.i64(m.session); w.i64(m.corr); w.u32(m.afterNsId); w.u16(m.max); w.u32(m.callerNsId); w.u8(m.clusterAdmin);
}

template <class W> void layoutVerifier(W& w, const rune::security::ScramVerifier& v) {
    w.bytes32({v.salt.data(), v.salt.size()});
    w.u32(v.iterations);
    w.bytes32({v.storedKey.data(), v.storedKey.size()});
    w.bytes32({v.serverKey.data(), v.serverKey.size()});
}
template <class W> void layout(W& w, const SecurityDdlMsg& m) {
    w.i64(m.session); w.i64(m.corr); layoutMeta(w, m.meta);
    w.u8(m.kind); w.str16(m.name); w.u8(m.change); w.u8(m.passwordIndex); w.u8(m.terminateSessions);
    layoutVerifier(w, m.verifier);
    w.u8(m.granteeKind); w.u8(m.op); w.u8(m.resourceType); w.u8(m.patternKind);
    w.str16(m.pattern); w.str16(m.roleName); w.u8(m.force); w.str16(m.denial); w.u8(m.proofChecked);
    w.u32(m.callerNsId);
}
template <class W> void layout(W& w, const SecurityBootstrapMsg& m) {
    w.i64(m.session); w.i64(m.corr); layoutMeta(w, m.meta);
    w.str16(m.adminName); layoutVerifier(w, m.verifier);
    w.bytes32({m.fakeSaltSeed.data(), m.fakeSaltSeed.size()});
    w.bytes32({m.nonce.data(), m.nonce.size()});
}

} // namespace admin_wire

template <class M> inline constexpr int32_t kDdlMsgType = 0;
template <> inline constexpr int32_t kDdlMsgType<SecurityDdlMsg>       = kMsgSecurityDdl;
template <> inline constexpr int32_t kDdlMsgType<SecurityBootstrapMsg> = kMsgSecurityBootstrap;
template <> inline constexpr int32_t kDdlMsgType<DefineTopicMsg>      = kMsgDefineTopic;
template <> inline constexpr int32_t kDdlMsgType<AlterTopicMsg>       = kMsgAlterTopic;
template <> inline constexpr int32_t kDdlMsgType<CreateViewMsg>       = kMsgCreateView;
template <> inline constexpr int32_t kDdlMsgType<DropTopicMsg>        = kMsgDropTopic;
template <> inline constexpr int32_t kDdlMsgType<DropViewMsg>         = kMsgDropView;
template <> inline constexpr int32_t kDdlMsgType<AlterViewMsg>        = kMsgAlterView;
template <> inline constexpr int32_t kDdlMsgType<AlterTopicConfigMsg> = kMsgAlterTopicConfig;
template <> inline constexpr int32_t kDdlMsgType<NamespaceDdlMsg>     = kMsgNamespaceDdl;
template <> inline constexpr int32_t kDdlMsgType<ListNamespacesMsg>   = kMsgListNamespaces;

/// The ring frame length of `m`, or 0 when a field does not fit its length prefix.
template <class M>
[[nodiscard]] inline std::size_t ddlWireLen(const M& m) noexcept {
    admin_wire::Sizer s;
    admin_wire::layout(s, m);
    return s.fits ? s.n : 0;
}

/// Encode a DDL request on the gateway → admin ring.  false = it cannot be
/// encoded (ddlWireLen 0, or longer than the ring's max message — the caller
/// tells the two apart with ddlWireLen) or the ring is full.
template <class M>
[[nodiscard]] inline bool encodeDdl(OneToOneByteRing& ring, const M& m) {
    const std::size_t len = ddlWireLen(m);
    if (len == 0 || len > static_cast<std::size_t>(ring.maxMsgLength())) return false;
    const auto idx = ring.tryClaim(kDdlMsgType<M>, len);
    if (idx < 0) return false;
    admin_wire::Writer w{ring.base() + idx};
    admin_wire::layout(w, m);
    ring.commit(idx);
    return true;
}

// Back-compat spellings (tests and the agent call these names).
inline bool encodeDefineTopic(OneToOneByteRing& ring, const DefineTopicMsg& m) { return encodeDdl(ring, m); }
inline bool encodeAlterTopic(OneToOneByteRing& ring, const AlterTopicMsg& m)   { return encodeDdl(ring, m); }
inline bool encodeCreateView(OneToOneByteRing& ring, const CreateViewMsg& m)   { return encodeDdl(ring, m); }

namespace admin_wire {
/// session + corr first (so a malformed frame can still be answered).
template <class M> inline void readHead(Reader& r, M& m) { m.session = r.i64(); m.corr = r.i64(); }
/// The decoded request, or — when the frame did not read exactly — its MalformedDdl.
template <class M> inline DdlRequest finish(Reader& r, M&& m, uint16_t templateId) {
    if (!r.done())
        return MalformedDdl{templateId, m.session, m.corr, r.why != nullptr ? r.why : "unreadable frame"};
    return DdlRequest{std::forward<M>(m)};
}
} // namespace admin_wire

inline DdlRequest decodeDefineTopic(const uint8_t* buf, std::size_t len) {
    admin_wire::Reader r{buf, len};
    DefineTopicMsg m;
    admin_wire::readHead(r, m);
    m.partitionCount = r.i32();
    m.validate       = r.u8() != 0;
    r.str16(m.name);
    r.str16(m.keyField);
    r.bytes32(m.blob);
    m.persistence = r.u8();
    if (r.ok && m.persistence > 1) r.fail("an unknown persistence byte");   // never cast
    m.replicationFactor = r.i16();
    admin_wire::readConfig(r, m.config);
    admin_wire::readMeta(r, m.meta);
    r.str16(m.adoptDenial);
    m.nsId = r.u32();
    return admin_wire::finish(r, std::move(m), kTidDefineTopic);
}
inline DdlRequest decodeAlterTopic(const uint8_t* buf, std::size_t len) {
    admin_wire::Reader r{buf, len};
    AlterTopicMsg m;
    admin_wire::readHead(r, m);
    m.expectedVersion = r.u16();
    r.str16(m.topicName);
    r.bytes32(m.opsBlob);
    admin_wire::readMeta(r, m.meta);
    m.nsId = r.u32();
    return admin_wire::finish(r, std::move(m), kTidAlterTopic);
}
inline DdlRequest decodeCreateView(const uint8_t* buf, std::size_t len) {
    admin_wire::Reader r{buf, len};
    CreateViewMsg m;
    admin_wire::readHead(r, m);
    m.persistence = r.u8();
    if (r.ok && m.persistence > 1) r.fail("an unknown persistence byte");   // never cast
    r.str16(m.viewName);
    r.str16(m.keyField);
    r.bytes32(m.planBlob);
    admin_wire::readMeta(r, m.meta);
    m.nsId = r.u32();
    return admin_wire::finish(r, std::move(m), kTidCreateView);
}
inline DdlRequest decodeDropTopic(const uint8_t* buf, std::size_t len) {
    admin_wire::Reader r{buf, len};
    DropTopicMsg m;
    admin_wire::readHead(r, m);
    r.str16(m.name);
    admin_wire::readMeta(r, m.meta);
    m.nsId = r.u32();
    return admin_wire::finish(r, std::move(m), kTidDropTopic);
}
inline DdlRequest decodeDropView(const uint8_t* buf, std::size_t len) {
    admin_wire::Reader r{buf, len};
    DropViewMsg m;
    admin_wire::readHead(r, m);
    r.str16(m.name);
    admin_wire::readMeta(r, m.meta);
    m.nsId = r.u32();
    return admin_wire::finish(r, std::move(m), kTidDropView);
}
inline DdlRequest decodeAlterView(const uint8_t* buf, std::size_t len) {
    admin_wire::Reader r{buf, len};
    AlterViewMsg m;
    admin_wire::readHead(r, m);
    r.str16(m.name);
    r.str16(m.keyField);
    m.persistence = r.u8();
    if (r.ok && m.persistence > 1) r.fail("an unknown persistence byte");   // never cast
    m.expectedVersion = r.u32();
    r.bytes32(m.planBlob);
    admin_wire::readMeta(r, m.meta);
    m.nsId = r.u32();
    return admin_wire::finish(r, std::move(m), kTidAlterView);
}
inline DdlRequest decodeAlterTopicConfig(const uint8_t* buf, std::size_t len) {
    admin_wire::Reader r{buf, len};
    AlterTopicConfigMsg m;
    admin_wire::readHead(r, m);
    r.str16(m.name);
    m.expectedVersion = r.u32();
    const uint16_t count = r.u16();
    if (r.ok && static_cast<std::size_t>(count) * 9u > len - r.pos) r.fail("the frame is shorter than its fields");
    if (r.ok) m.changes.reserve(count);
    for (uint16_t i = 0; i < count && r.ok; ++i) {
        const uint8_t key   = r.u8();
        const int64_t value = r.i64();
        if (key < static_cast<uint8_t>(rune::protocol::kFirstConfigKey) ||
            key > static_cast<uint8_t>(rune::protocol::kLastConfigKey)) {
            r.fail("an unknown config key");   // never cast
            break;
        }
        m.changes.push_back(rune::catalog::ConfigChange{static_cast<rune::catalog::ConfigKey>(key), value});
    }
    admin_wire::readMeta(r, m.meta);
    m.nsId = r.u32();
    return admin_wire::finish(r, std::move(m), kTidAlterTopicConfig);
}
inline DdlRequest decodeNamespaceDdl(const uint8_t* buf, std::size_t len) {
    admin_wire::Reader r{buf, len};
    NamespaceDdlMsg m;
    admin_wire::readHead(r, m);
    admin_wire::readMeta(r, m.meta);
    m.kind = r.u8();
    r.str16(m.name);
    std::vector<uint8_t> mask, lim;
    r.bytes32(mask);
    r.bytes32(lim);
    r.str16(m.denial);
    m.clientVersion = r.u16();
    if (r.ok) {
        if (m.kind < 1 || m.kind > 2) r.fail("an unknown namespace DDL kind");   // never cast
        else if (mask.size() != m.mask.size() || lim.size() != rune::protocol::kNamespaceLimitsBytes ||
                 !rune::protocol::decodeLimits(lim.data(), lim.size(), m.limits))
            r.fail("a malformed namespace limits record");
        else std::memcpy(m.mask.data(), mask.data(), mask.size());
    }
    return admin_wire::finish(r, std::move(m), kTidNamespaceDdl);
}
inline DdlRequest decodeListNamespaces(const uint8_t* buf, std::size_t len) {
    admin_wire::Reader r{buf, len};
    ListNamespacesMsg m;
    admin_wire::readHead(r, m);
    m.afterNsId    = r.u32();
    m.max          = r.u16();
    m.callerNsId   = r.u32();
    m.clusterAdmin = r.u8();
    if (r.ok && m.clusterAdmin > 1) r.fail("a cluster-admin flag other than 0/1");
    return admin_wire::finish(r, std::move(m), kTidListNamespaces);
}

// ---- Security DDL decode (authentication Task 10) ----
namespace admin_wire {
/// A verifier read with the library's value rules (salt 16..64, iterations 4096..1e6, 64-byte keys) - never cast.
inline void readVerifier(Reader& r, rune::security::ScramVerifier& v, bool required) {
    std::vector<uint8_t> salt, stored, server;
    r.bytes32(salt);
    v.iterations = r.u32();
    r.bytes32(stored);
    r.bytes32(server);
    if (!r.ok) return;
    if (!required) return;   // unused slot: its content is ignored
    if (salt.size() < rune::security::kSaltMin || salt.size() > rune::security::kSaltMax ||
        v.iterations < rune::security::kIterMin || v.iterations > rune::security::kIterMax ||
        stored.size() != rune::security::kKeyBytes || server.size() != rune::security::kKeyBytes) {
        r.fail("a malformed SCRAM verifier");
        return;
    }
    v.salt = std::move(salt);
    std::memcpy(v.storedKey.data(), stored.data(), rune::security::kKeyBytes);
    std::memcpy(v.serverKey.data(), server.data(), rune::security::kKeyBytes);
}
} // namespace admin_wire

inline SecurityRequest decodeSecurityDdl(const uint8_t* buf, std::size_t len) {
    admin_wire::Reader r{buf, len};
    SecurityDdlMsg m;
    admin_wire::readHead(r, m);
    admin_wire::readMeta(r, m.meta);
    m.kind = r.u8();
    r.str16(m.name);
    m.change = r.u8();
    m.passwordIndex = r.u8();
    m.terminateSessions = r.u8();
    const bool hasVerifier = m.kind == 1 || (m.kind == 2 && (m.change == 0 || m.change == 2));
    admin_wire::readVerifier(r, m.verifier, hasVerifier);
    m.granteeKind = r.u8(); m.op = r.u8(); m.resourceType = r.u8(); m.patternKind = r.u8();
    r.str16(m.pattern);
    r.str16(m.roleName);
    m.force = r.u8();
    r.str16(m.denial);
    m.proofChecked = r.u8();
    m.callerNsId = r.u32();
    if (r.ok) {
        if (m.kind < 1 || m.kind > 9) r.fail("an unknown security op kind");
        else if (m.force > 1) r.fail("a force flag other than 0/1");
        else if (m.proofChecked > 1) r.fail("a proof-checked flag other than 0/1");
        else if (m.kind == 2 && m.change > 4) r.fail("an unknown user change");
        else if (m.terminateSessions > 1) r.fail("a terminate flag other than 0/1");
        else if ((m.kind == 6 || m.kind == 7) &&
                 !(rune::protocol::validSecurityGranteeKind(m.granteeKind) && rune::protocol::validSecurityOperation(m.op) &&
                   rune::protocol::validSecurityResourceType(m.resourceType) &&
                   rune::protocol::validSecurityPatternKind(m.patternKind)))
            r.fail("an unknown grant enum byte");
    }
    if (!r.done()) return MalformedDdl{kTidSecurityDdl, m.session, m.corr, r.why != nullptr ? r.why : "unreadable frame"};
    return SecurityRequest{std::move(m)};
}
inline SecurityRequest decodeSecurityBootstrap(const uint8_t* buf, std::size_t len) {
    admin_wire::Reader r{buf, len};
    SecurityBootstrapMsg m;
    admin_wire::readHead(r, m);
    admin_wire::readMeta(r, m.meta);
    r.str16(m.adminName);
    admin_wire::readVerifier(r, m.verifier, true);
    std::vector<uint8_t> seed, nonce;
    r.bytes32(seed);
    r.bytes32(nonce);
    if (r.ok) {
        if (seed.size() != m.fakeSaltSeed.size() || nonce.size() != m.nonce.size()) r.fail("a malformed seed / nonce");
        else {
            std::memcpy(m.fakeSaltSeed.data(), seed.data(), seed.size());
            std::memcpy(m.nonce.data(), nonce.data(), nonce.size());
        }
    }
    if (!r.done()) return MalformedDdl{kTidSecurityBootstrap, m.session, m.corr, r.why != nullptr ? r.why : "unreadable frame"};
    return SecurityRequest{std::move(m)};
}

// ---- Catalog reads (catalog history Task 12) ----
/// false = ring full (the gateway answers the retriable NotLeader / retries).
[[nodiscard]] inline bool encodeCatalogSubscribe(OneToOneByteRing& ring, const CatalogSubscribeMsg& m) {
    const auto idx = ring.tryClaim(kMsgCatalogSubscribe, kCatalogSubscribeWireLen);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    admin_wire::putI64(p, m.session);
    admin_wire::putI64(p + 8, m.corr);
    admin_wire::putI64(p + 16, m.fromOffset);
    ring.commit(idx);
    return true;
}
/// Authentication Task 10: the `__security` reads - the catalog layouts under their own message types.
[[nodiscard]] inline bool encodeSecuritySubscribe(OneToOneByteRing& ring, const CatalogSubscribeMsg& m) {
    const auto idx = ring.tryClaim(kMsgSecuritySubscribe, kCatalogSubscribeWireLen);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    admin_wire::putI64(p, m.session);
    admin_wire::putI64(p + 8, m.corr);
    admin_wire::putI64(p + 16, m.fromOffset);
    ring.commit(idx);
    return true;
}
[[nodiscard]] inline bool encodeSecurityUnsubscribe(OneToOneByteRing& ring, const CatalogUnsubscribeMsg& m) {
    const auto idx = ring.tryClaim(kMsgSecurityUnsubscribe, kCatalogUnsubscribeWireLen);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    admin_wire::putI64(p, m.session);
    admin_wire::putI64(p + 8, m.subscriptionId);
    ring.commit(idx);
    return true;
}
[[nodiscard]] inline bool encodeCatalogUnsubscribe(OneToOneByteRing& ring, const CatalogUnsubscribeMsg& m) {
    const auto idx = ring.tryClaim(kMsgCatalogUnsubscribe, kCatalogUnsubscribeWireLen);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    admin_wire::putI64(p, m.session);
    admin_wire::putI64(p + 8, m.subscriptionId);
    ring.commit(idx);
    return true;
}
/// Fix round 0: catalog-read flow control (control → admin).  false = ring full (retried).
struct CatalogReadPausedMsg {
    int64_t session = 0; int64_t subscriptionId = 0; int64_t baseOffset = -1; bool droppedMarker = false;
};
struct CatalogReadResumeMsg { int64_t session = 0; int64_t subscriptionId = 0; };
[[nodiscard]] inline bool encodeCatalogReadPaused(OneToOneByteRing& ring, const CatalogReadPausedMsg& m) {
    const auto idx = ring.tryClaim(kMsgCatalogReadPaused, 25);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    admin_wire::putI64(p, m.session);
    admin_wire::putI64(p + 8, m.subscriptionId);
    admin_wire::putI64(p + 16, m.baseOffset);
    p[24] = m.droppedMarker ? 1 : 0;
    ring.commit(idx);
    return true;
}
[[nodiscard]] inline bool encodeCatalogReadResume(OneToOneByteRing& ring, const CatalogReadResumeMsg& m) {
    const auto idx = ring.tryClaim(kMsgCatalogReadResume, 16);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    admin_wire::putI64(p, m.session);
    admin_wire::putI64(p + 8, m.subscriptionId);
    ring.commit(idx);
    return true;
}
[[nodiscard]] inline bool decodeCatalogReadPaused(const uint8_t* p, std::size_t n, CatalogReadPausedMsg& m) noexcept {
    if (n != 25 || p[24] > 1) return false;
    m = CatalogReadPausedMsg{admin_wire::getI64(p), admin_wire::getI64(p + 8), admin_wire::getI64(p + 16), p[24] == 1};
    return true;
}
[[nodiscard]] inline bool decodeCatalogReadResume(const uint8_t* p, std::size_t n, CatalogReadResumeMsg& m) noexcept {
    if (n != 16) return false;
    m = CatalogReadResumeMsg{admin_wire::getI64(p), admin_wire::getI64(p + 8)};
    return true;
}

[[nodiscard]] inline bool encodeCatalogSessionGone(OneToOneByteRing& ring, int64_t session) {
    const auto idx = ring.tryClaim(kMsgCatalogSessionGone, 8);
    if (idx < 0) return false;
    admin_wire::putI64(ring.base() + idx, session);
    ring.commit(idx);
    return true;
}

/// Checked decodes: false on a frame of any other length (reported, never applied).
[[nodiscard]] inline bool decodeCatalogSubscribe(const uint8_t* p, std::size_t n, CatalogSubscribeMsg& m) noexcept {
    if (n != kCatalogSubscribeWireLen) return false;
    m = CatalogSubscribeMsg{admin_wire::getI64(p), admin_wire::getI64(p + 8), admin_wire::getI64(p + 16)};
    return true;
}
[[nodiscard]] inline bool decodeCatalogUnsubscribe(const uint8_t* p, std::size_t n, CatalogUnsubscribeMsg& m) noexcept {
    if (n != kCatalogUnsubscribeWireLen) return false;
    m = CatalogUnsubscribeMsg{admin_wire::getI64(p), admin_wire::getI64(p + 8)};
    return true;
}

// ---- GetSchema ----
// Wire layout: session:i64, corr:i64, requestedFingerprint:u64 (0=latest),
// nameLen:u16 + name bytes.
inline bool encodeGetSchema(OneToOneByteRing& ring, const GetSchemaMsg& m) {
    using namespace admin_wire;
    const std::size_t len = 8 + 8 + 8 + 2 + m.name.size() + 4;
    const auto idx = ring.tryClaim(kMsgGetSchema, len);
    if (idx < 0) return false;
    uint8_t* p = ring.base() + idx;
    putI64(p, m.session); p += 8;
    putI64(p, m.corr);    p += 8;
    putI64(p, static_cast<int64_t>(m.requestedFingerprint)); p += 8;
    putU16(p, static_cast<uint16_t>(m.name.size())); p += 2;
    std::memcpy(p, m.name.data(), m.name.size()); p += m.name.size();
    putU32(p, m.nsId);
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
    if (26 + static_cast<std::size_t>(nl) + 4 <= len) m.nsId = getU32(buf + 26 + nl);
    return m;
}

// ---- AdminRole ----
// Layout: [leader:u8][epoch:i32][leaderId:i32][partitionEpoch:i32][isrCount:i32][isr:i32 * isrCount].
inline constexpr std::size_t kAdminRoleFixedBytes = 1 + 4 + 4 + 4 + 4;

[[nodiscard]] inline constexpr std::size_t adminRoleWireLen(int32_t isrCount) noexcept {
    return kAdminRoleFixedBytes + 4u * static_cast<std::size_t>(isrCount < 0 ? 0 : isrCount);
}

/// `msgType` = kMsgCatalogRole (the __catalog role; kMsgAdminRole is retired).  false = ring full (the caller keeps
/// the role and retries) or an ISR beyond kMaxIsr (never encoded).
inline bool encodeAdminRole(OneToOneByteRing& ring, const AdminRoleMsg& m,
                            int32_t msgType) {
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

// ---- FilterSchema (admin → control; Phase 2c) ----
// Same layout as SchemaUpdate: [topicId:i32][version:u16][EVOLVING descriptor blob].
struct FilterSchemaMsg {
    int32_t              topicId = -1;
    uint16_t             version = 0;
    std::vector<uint8_t> blob;
};

[[nodiscard]] inline constexpr std::size_t filterSchemaWireLen(std::size_t blobLen) noexcept {
    return 4 + 2 + blobLen;
}

/// Writes exactly filterSchemaWireLen(blob.size()) bytes at `p`.
inline void writeFilterSchema(uint8_t* p, int32_t topicId, uint16_t version, std::span<const uint8_t> blob) noexcept {
    using namespace admin_wire;
    putI32(p, topicId);
    p[4] = static_cast<uint8_t>(version & 0xff);
    p[5] = static_cast<uint8_t>((version >> 8) & 0xff);
    if (!blob.empty()) std::memcpy(p + 6, blob.data(), blob.size());
}

/// False for a frame shorter than its 6-byte header (the caller logs it, rate-limited, and skips it).
[[nodiscard]] inline bool decodeFilterSchema(const uint8_t* buf, std::size_t len, FilterSchemaMsg& m) {
    using namespace admin_wire;
    if (len < 6) return false;
    m.topicId = getI32(buf);
    m.version = static_cast<uint16_t>(buf[4]) | static_cast<uint16_t>(static_cast<uint16_t>(buf[5]) << 8);
    m.blob.assign(buf + 6, buf + len);
    return true;
}

// ---- NamespaceUpserted (admin -> control; namespaces Task 9) ----
//   [nsId:u32][version:u32][state:u8 NsStateTag][fingerprint:u64][limits: kNamespaceLimitsBytes][nameLen:u16 + name]
inline constexpr std::size_t kNamespaceUpsertedFixedBytes = 4 + 4 + 1 + 8 + rune::protocol::kNamespaceLimitsBytes + 2;
[[nodiscard]] inline std::size_t namespaceUpsertedWireLen(std::string_view name) noexcept {
    return name.size() > 0xFFFF ? 0 : kNamespaceUpsertedFixedBytes + name.size();
}
/// Writes exactly namespaceUpsertedWireLen(rec.name) bytes at `p`.
inline void writeNamespaceUpserted(uint8_t* p, const rune::catalog::NamespaceRecord& rec) noexcept {
    using namespace admin_wire;
    putU32(p, rec.nsId);
    putU32(p + 4, rec.version);
    p[8] = static_cast<uint8_t>(rune::catalog::tagOf(rec.state));
    std::memcpy(p + 9, &rec.fingerprint, 8);
    rune::protocol::encodeLimits(rec.limits, p + 17);
    uint8_t* q = p + 17 + rune::protocol::kNamespaceLimitsBytes;
    putU16(q, static_cast<uint16_t>(rec.name.size()));
    if (!rec.name.empty()) std::memcpy(q + 2, rec.name.data(), rec.name.size());
}
/// False on a short / trailing-garbage frame or an unknown state tag (the caller logs it, rate-limited, and skips it).
[[nodiscard]] inline bool decodeNamespaceUpserted(const uint8_t* buf, std::size_t len, rune::catalog::NamespaceRecord& rec) {
    using namespace admin_wire;
    if (buf == nullptr || len < kNamespaceUpsertedFixedBytes) return false;
    rec.nsId = getU32(buf);
    rec.version = getU32(buf + 4);
    const uint8_t tag = buf[8];
    if (tag > static_cast<uint8_t>(rune::catalog::NsStateTag::Deleting)) return false;
    rec.state = rune::catalog::stateOf(static_cast<rune::catalog::NsStateTag>(tag));
    std::memcpy(&rec.fingerprint, buf + 9, 8);
    if (!rune::protocol::decodeLimits(buf + 17, len - 17, rec.limits)) return false;
    std::size_t pos = 17 + rune::protocol::kNamespaceLimitsBytes;
    const uint16_t n = getU16(buf + pos);
    pos += 2;
    if (pos + n != len) return false;
    rec.name.assign(reinterpret_cast<const char*>(buf + pos), n);
    return true;
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
// Catalog history Task 8: the same byte that carries `recovery` carries the
// UNINSTALL op (value kViewPlanUninstallByte; blobs empty) on the control →
// worker install ring — a DropView / AlterView retired the view.  One op byte:
// 0 = install, 1 = recovery install, 2 = uninstall.
inline constexpr uint8_t kViewPlanUninstallByte = 2;
struct ViewPlanUpdateMsg {
    int32_t              viewTopicId = -1;
    uint8_t              persistence = 0;
    bool                 recovery    = false;  // Task 19b: restart-recovery install
    bool                 uninstall   = false;  // Task 8: retire the view (op byte 2)
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
    const uint8_t op = m.uninstall ? kViewPlanUninstallByte : (m.recovery ? uint8_t{1} : uint8_t{0});
    writeViewPlanUpdate(ring.base() + idx, m.viewTopicId, m.persistence, op,
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
    const uint8_t op = buf[pos++];
    m.uninstall   = op == kViewPlanUninstallByte;
    m.recovery    = op == 1;
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
//                             [nsId:u32]   (namespaces Task 5: the owning namespace; appended LAST)
//   kA2cCatalogTopicDropped : [topicId:i32][nameLen:u16 + name][nsId:u32]
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
    uint32_t                    nsId              = 0;   // the owning namespace (names are per namespace)
};

/// 0 = cannot be encoded (a string longer than a u16 prefix holds).
[[nodiscard]] inline std::size_t catalogTopicAppliedWireLen(std::string_view name,
                                                            const rune::protocol::TopicConfig& c) noexcept {
    if (name.size() > kMaxStr16 || c.coPartitionGroup.size() > kMaxStr16 || c.partitionKey.size() > kMaxStr16)
        return 0;
    return 4 + 2 + name.size() + 4 + 2 + 1 + topicConfigWireLen(c) + 4;
}

inline void writeCatalogTopicApplied(uint8_t* p, int32_t topicId, std::string_view name, int32_t partitions,
                                     int16_t rf, uint8_t persistence, const rune::protocol::TopicConfig& c,
                                     uint32_t nsId = 0) {
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
    p = putStr16(p, c.partitionKey);
    putU32(p, nsId);
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
    if (pos + 4 > len) return false;
    m.nsId = getU32(buf + pos); pos += 4;
    return pos == len && m.topicId >= 0 && m.partitions > 0 && !m.name.empty();
}

struct CatalogTopicDroppedMsg {
    int32_t     topicId = -1;
    std::string name;
    uint32_t    nsId    = 0;   // namespaces plan Task 6: the dropped topic's namespace (its dir is under ns-<nsId>)
};
[[nodiscard]] inline std::size_t catalogTopicDroppedWireLen(std::string_view name) noexcept {
    return name.size() > kMaxStr16 ? 0 : 4 + 2 + name.size() + 4;
}
inline void writeCatalogTopicDropped(uint8_t* p, int32_t topicId, std::string_view name, uint32_t nsId = 0) {
    admin_wire::putI32(p, topicId);
    uint8_t* q = admin_wire::putStr16(p + 4, name);
    admin_wire::putU32(q, nsId);
}
[[nodiscard]] inline bool decodeCatalogTopicDropped(const uint8_t* buf, std::size_t len, CatalogTopicDroppedMsg& m) {
    if (len < 4) return false;
    m.topicId = admin_wire::getI32(buf);
    std::size_t pos = 4;
    if (!admin_wire::getStr16(buf, len, pos, m.name)) return false;
    if (pos + 4 > len) return false;
    m.nsId = admin_wire::getU32(buf + pos);
    pos += 4;
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

/// Writes `m` into `p` (placementViewWireLen(m) bytes).  No allocation.
inline void writePlacementView(uint8_t* p, const PlacementViewMsg& m) noexcept {
    admin_wire::putI32(p, m.placeable);
    p[4] = static_cast<uint8_t>(m.haltedCount & 0xff);
    p[5] = static_cast<uint8_t>(m.haltedCount >> 8);
    for (uint16_t i = 0; i < m.haltedCount; ++i) admin_wire::putI32(p + 6 + 4 * i, m.halted[i]);
}
/// False on a malformed frame (short, a count above kMaxHalted, a trailing byte, placeable < 0).
[[nodiscard]] inline bool decodePlacementView(const uint8_t* buf, std::size_t len, PlacementViewMsg& m) noexcept {
    if (len < 6) return false;
    m.placeable   = admin_wire::getI32(buf);
    m.haltedCount = static_cast<uint16_t>(buf[4] | (buf[5] << 8));
    if (m.placeable < 0 || m.haltedCount > PlacementViewMsg::kMaxHalted || len != placementViewWireLen(m)) return false;
    for (uint16_t i = 0; i < m.haltedCount; ++i) m.halted[i] = admin_wire::getI32(buf + 6 + 4 * i);
    return true;
}

/// kMsgPeerLinkKeys (Task 15, control -> admin on controlToAdminRing): the keys of an established inter-broker link,
/// [peer:i32][epoch:u32][sendMaster 32][recvMaster 32][install:u8] (issue #241: LinkInstall, receive-only or
/// send-and-receive; an unknown value is a malformed message).  Cold: one per link establishment / promotion.
inline constexpr std::size_t kPeerLinkKeysWireLen = 4 + 4 + 32 + 32 + 1;
struct PeerLinkKeysMsg {
    int32_t  peer  = -1;
    uint32_t epoch = 0;
    std::array<uint8_t, 32> sendMaster{}, recvMaster{};
    rune::peerlink::LinkInstall install = rune::peerlink::LinkInstall::SendAndReceive;
};
inline void writePeerLinkKeys(uint8_t* p, const PeerLinkKeysMsg& m) noexcept {
    admin_wire::putI32(p, m.peer);
    admin_wire::putI32(p + 4, static_cast<int32_t>(m.epoch));
    std::memcpy(p + 8, m.sendMaster.data(), 32);
    std::memcpy(p + 40, m.recvMaster.data(), 32);
    p[72] = static_cast<uint8_t>(m.install);
}
[[nodiscard]] inline bool decodePeerLinkKeys(const uint8_t* buf, std::size_t len, PeerLinkKeysMsg& m) noexcept {
    if (len != kPeerLinkKeysWireLen) return false;
    m.peer  = admin_wire::getI32(buf);
    m.epoch = static_cast<uint32_t>(admin_wire::getI32(buf + 4));
    std::memcpy(m.sendMaster.data(), buf + 8, 32);
    std::memcpy(m.recvMaster.data(), buf + 40, 32);
    if (buf[72] != static_cast<uint8_t>(rune::peerlink::LinkInstall::ReceiveOnly) &&
        buf[72] != static_cast<uint8_t>(rune::peerlink::LinkInstall::SendAndReceive)) return false;
    m.install = static_cast<rune::peerlink::LinkInstall>(buf[72]);
    return m.peer >= 0 && m.epoch != 0;
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

// ─── `__security` effects (authentication Task 9; D29) ───────────────────────────────────────────────────────────
//   kA2cSecurityApplied : [offset:i64][the applied event's record bytes]   (the control thread's SecurityView input)
//   kA2cSecurityHalted  : the kA2cCatalogHalted layout [offset:i64][expected:u64][actual:u64][detail]
inline constexpr std::size_t kSecurityAppliedFixedBytes = 8;
inline void writeSecurityApplied(uint8_t* p, int64_t offset, std::span<const uint8_t> eventBytes) noexcept {
    admin_wire::putI64(p, offset);
    if (!eventBytes.empty()) std::memcpy(p + kSecurityAppliedFixedBytes, eventBytes.data(), eventBytes.size());
}
/// `bytes` aliases `buf` (valid while the ring message is).  false = short / negative offset.
[[nodiscard]] inline bool decodeSecurityApplied(const uint8_t* buf, std::size_t len, int64_t& offset,
                                                std::span<const uint8_t>& bytes) noexcept {
    if (len <= kSecurityAppliedFixedBytes) return false;   // an event is never empty
    offset = admin_wire::getI64(buf);
    bytes  = std::span<const uint8_t>(buf + kSecurityAppliedFixedBytes, len - kSecurityAppliedFixedBytes);
    return offset >= 0;
}
//   kA2cSecurityServing : [caughtUp:u8][leaderHw:i64][epoch:i32][asLeader:u8][knownEnd:i64][behind:u8][held:u8]   (issues
//                         #15 / #111; PR #459 F2: the epoch / role the verdict is for, compared with the committed
//                         assignment on the control thread; #205: knownEnd = max(log end, leader HW) - the events this
//                         broker knows exist; #603: behind = the applier is below the committed HW this broker knows -
//                         every grant-checked operation answers AuthUnavailable until it is not; #460: held = the verdict is held through a
//                         leader failover - it counts only while this broker is in the committed ISR)
inline constexpr std::size_t kSecurityServingWireLen = 1 + 8 + 4 + 1 + 8 + 1 + 1;
inline void writeSecurityServing(uint8_t* p, bool caughtUp, int64_t leaderHw, int32_t epoch = -1,
                                 bool asLeader = false, int64_t knownEnd = -1, bool behind = false,
                                 bool held = false) noexcept {
    p[0] = caughtUp ? 1 : 0;
    admin_wire::putI64(p + 1, leaderHw);
    std::memcpy(p + 9, &epoch, 4);
    p[13] = asLeader ? 1 : 0;
    admin_wire::putI64(p + 14, knownEnd);
    p[22] = behind ? 1 : 0;
    p[23] = held ? 1 : 0;
}
[[nodiscard]] inline bool decodeSecurityServing(const uint8_t* buf, std::size_t len, bool& caughtUp, int64_t& leaderHw) noexcept {
    if (len != kSecurityServingWireLen || buf[0] > 1 || buf[13] > 1 || buf[22] > 1 || buf[23] > 1) return false;
    caughtUp = buf[0] == 1;
    leaderHw = admin_wire::getI64(buf + 1);
    return true;
}
[[nodiscard]] inline bool decodeSecurityServing(const uint8_t* buf, std::size_t len, bool& caughtUp, int64_t& leaderHw,
                                                int32_t& epoch, bool& asLeader) noexcept {
    if (!decodeSecurityServing(buf, len, caughtUp, leaderHw)) return false;
    std::memcpy(&epoch, buf + 9, 4);
    asLeader = buf[13] == 1;
    return true;
}
[[nodiscard]] inline bool decodeSecurityServing(const uint8_t* buf, std::size_t len, bool& caughtUp, int64_t& leaderHw,
                                                int32_t& epoch, bool& asLeader, int64_t& knownEnd) noexcept {
    if (!decodeSecurityServing(buf, len, caughtUp, leaderHw, epoch, asLeader)) return false;
    knownEnd = admin_wire::getI64(buf + 14);
    return true;
}
[[nodiscard]] inline bool decodeSecurityServing(const uint8_t* buf, std::size_t len, bool& caughtUp, int64_t& leaderHw,
                                                int32_t& epoch, bool& asLeader, int64_t& knownEnd, bool& behind) noexcept {
    if (!decodeSecurityServing(buf, len, caughtUp, leaderHw, epoch, asLeader, knownEnd)) return false;
    behind = buf[22] == 1;
    return true;
}
[[nodiscard]] inline bool decodeSecurityServing(const uint8_t* buf, std::size_t len, bool& caughtUp, int64_t& leaderHw,
                                                int32_t& epoch, bool& asLeader, int64_t& knownEnd, bool& behind,
                                                bool& held) noexcept {
    if (!decodeSecurityServing(buf, len, caughtUp, leaderHw, epoch, asLeader, knownEnd, behind)) return false;
    held = buf[23] == 1;
    return true;
}
using SecurityHaltedMsg = CatalogHaltedMsg;
inline constexpr std::size_t kSecurityHaltedFixedBytes = kCatalogHaltedFixedBytes;
inline void writeSecurityHalted(uint8_t* p, int64_t offset, uint64_t expected, uint64_t actual,
                                std::string_view detail) noexcept {
    writeCatalogHalted(p, offset, expected, actual, detail);
}
[[nodiscard]] inline bool decodeSecurityHalted(const uint8_t* buf, std::size_t len, SecurityHaltedMsg& m) {
    return decodeCatalogHalted(buf, len, m);
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
// A descriptor reaches the admin thread only inside a DefineTopic on the request
// ring (O2O ring: maxMsgLength = capacity / 8), whose fixed framing (catalog
// history Task 9 layout) is session + corr + partitions + validate + the three
// length prefixes + persistence + rf + the config block + the meta's fixed part.
// The SchemaUpdate that re-posts it on the admin→control ring carries only a
// 6-byte header in front, so every committed descriptor fits.
// statement(4) + clientName(2) + sourceAddress(2) + authenticated(2, auth Task 10) + sessionId + requestId hi/lo + flag.
inline constexpr std::size_t kDdlMetaFixedBytes     = 4 + 2 + 2 + 2 + 8 + 8 + 8 + 1;
// The trailing adoptDenial length prefix (2, issue #129).
inline constexpr std::size_t kDefineTopicFixedBytes =
    8 + 8 + 4 + 1 + 2 + 2 + 4 + 1 + 2 + kTopicConfigFixedBytes + 2 + 2 + kDdlMetaFixedBytes + 2;
inline constexpr std::size_t kMaxDescriptorViaRequestRing =
    static_cast<std::size_t>(kAdminRequestRingBytes) / 8 - kDefineTopicFixedBytes;
static_assert(schemaUpdateWireLen(kMaxDescriptorViaRequestRing) <=
                  static_cast<std::size_t>(kAdminToControlRingBytes) / 8,
              "every descriptor the admin request ring can deliver must fit one "
              "SchemaUpdate on the admin->control ring");

// A FilterSchema carries the same descriptor a SchemaUpdate does, so it fits the ring by the same bound.
static_assert(filterSchemaWireLen(kMaxDescriptorViaRequestRing) <=
                  static_cast<std::size_t>(kAdminToControlRingBytes) / 8,
              "every descriptor the admin request ring can deliver must fit one FilterSchema on the admin->control ring");

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
