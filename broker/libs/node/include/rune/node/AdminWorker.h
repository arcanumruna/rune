#pragma once
// AdminWorker.h — the catalog write path (catalog history spec §5.1, Task 9).
//
// Every broker runs one AdminWorker on its admin thread.  It SOLELY owns the
// __catalog replica, its replication plane, the CatalogApplier and the registries
// the applier projects (SchemaRegistry, ViewRegistry).  Every broker applies the
// committed catalog (CatalogApplier, Task 7); the catalog LEADER also writes it:
//
//   one DDL = one event.  The leader validates the request against its applied
//   catalog by DRY-RUNNING the shared library's frozen apply rules on it
//   (previewFingerprint — the same rules every broker applies, so no event the
//   rules refuse is ever written), assigns objectId / versions / topic ids from
//   the state, stamps the principal, statement and timestampMs, sizes every
//   effect frame the event will post (catalogEventEffectsRefusal), encodes it and
//   appends it acks=all.  Once COMMITTED and APPLIED LOCALLY it answers OK
//   (CreateTopic's OK is then held by the control thread until the topic serves —
//   DdlAnswerGate).
//
// Hard rules: admin-thread-owned (no locks, no atomics); every cross-thread
// effect leaves through AdminDeps (ring encoders) or the applier's sink.  The
// leadership is CatalogRole (one variant, rule 3); the in-flight DDL is InFlight
// (Idle | AwaitingCommit).  Cold path: DDL allocates per request; an idle pass
// allocates nothing.

#include <rune/storage/ZeroedBytes.h>
#include <cstdint>
#include <deque>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "rune/node/AdminChannels.h"
#include "rune/node/CatalogApplier.h"
#include "rune/node/CatalogPartition.h"
#include "rune/node/CatalogReadServer.h"
#include "rune/node/SecurityAdmin.h"
#include <rune/peerlink/PeerLinkKeyStore.h>
#include "rune/node/SecurityApplier.h"
#include "rune/node/SecurityPartition.h"
#include "rune/node/SchemaRegistry.h"
#include "rune/node/TopicDirReconcile.h"
#include "rune/node/ViewRegistry.h"

#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogState.h>

namespace rune::replication {
class PartitionReplica;
struct AlterIsrResponseMsg;
}

namespace rune::node {

// Error code integers (rune::protocol::client::ErrorCode; the AdminWorker passes
// ints to stay protocol-agnostic):
//   OK 0, NotLeader 2, Internal 6, UnknownTopic 7, InvalidSchema 23, TopicExists 25,
//   SchemaIncompatible 30, SchemaConflict 31, ViewExists 32, ViewSourceNotFound 33,
//   ViewFieldUnresolved 34, ViewKeyInvalid 35, ViewNotCoPartitioned 36,
//   ViewSourceTransient 37, ViewNotAlterable 38, InvalidRequest 39 (with a detail),
//   InvalidTopicName 40, ViewSourceIsView 41.

// ---------------------------------------------------------------------------
// AdminDeps — the answers the AdminWorker sends (every one resolved on the admin
// thread; production = encode + the admin→control egress ring).
// ---------------------------------------------------------------------------
struct AdminDeps {
    /// DefineTopicResponse (65).
    std::function<void(int64_t session, int64_t corr, int32_t topicId,
                       uint16_t version, uint16_t errorCode)> respondDefine;
    /// TopicSchemaResponse (67).  `partitionCount` (Phase 2b Part C) is the
    /// TOPIC's partition count (0 on any error).
    std::function<void(int64_t session, int64_t corr, int32_t topicId,
                       uint16_t version, int32_t partitionCount,
                       const std::vector<uint8_t>& blob,
                       uint16_t errorCode)> respondGet;
    /// AlterTopicResponse (69).  On OK `newVersion` is the new schema version; on
    /// an error, the current one (the client re-reads and retries its CAS).
    std::function<void(int64_t session, int64_t corr, int32_t topicId,
                       uint16_t newVersion, uint16_t errorCode)> respondAlter;
    /// CreateViewResponse (71).
    std::function<void(int64_t session, int64_t corr, int32_t viewTopicId,
                       uint64_t outputFingerprint, uint16_t errorCode)> respondCreateView;
    /// DdlResponse (78) — answers DropTopic / AlterTopicConfig / DropView / AlterView.
    /// `gateTopicId` = the topic the answer is gated on (the control thread holds
    /// the OK until it serves: AlterView's fresh sink), -1 = none.
    std::function<void(int64_t session, int64_t corr, uint16_t code, uint64_t objectId,
                       uint32_t newVersion, int32_t gateTopicId, std::string_view detail)> respondDdl;
    /// ErrorResponse (5) — a DefineTopic / AlterTopic / CreateView rejection that
    /// carries a detail (InvalidRequest 39).
    std::function<void(int64_t session, int64_t corr, uint16_t code, std::string_view message)> respondError;

    int32_t brokerCount = 1;   // config_.peers.size() (spec §8: rf <= broker count)
    int32_t topicMinIsr = 1;   // config_.gatewayMinIsr  (spec §8: rf >= minIsr, 0 = every broker)

    /// The largest admin→control effect frame (0 = catalogEffectMaxFrame(), the
    /// production ring).  The leader refuses an event any of whose effect frames
    /// would not fit (Task 7 C1).  Tests shrink it together with the applier sink's.
    std::size_t effectMaxFrameBytes = 0;

    /// Catalog history Task 12: the catalog read server's egress (production:
    /// the admin → control catalog-read ring, then the session's egress).
    /// false = full (the server keeps the position and retries).  Unset: catalog
    /// subscribes are answered nowhere — every production / rig AdminWorker sets it.
    CatalogReadEgress catalogRead;

    /// Authentication Task 10 (D24/D36): the DdlResponse (78) of a security DDL / bootstrap.  `gateOffset` >= 0 = an OK
    /// for the event at that offset, released by the control thread only once THIS broker enforces it (production posts
    /// kMsgEgressFrameGated); -1 = ungated (a refusal / error).  Unset: security answers go nowhere (rigs without one).
    SecurityRespond respondSecurity;
};

// ---------------------------------------------------------------------------
// The catalog leadership (rule 3: one variant, each state its own variables).
//   Follower        — not the catalog leader (leaderId -1 = leaderless).
//   LeaderCatchingUp— the replica leads but not every record of its log is both
//                     COMMITTED and APPLIED here (a fresh promotion — a previous
//                     epoch's tail included — or the tail of a DDL whose commit
//                     timed out): DDL answers the retriable NotLeader (Task 11:
//                     it never validates against a stale catalog).
//   LeaderServing   — leads, HW == LEO and the applier applied up to the HW: DDL
//                     is served.
//   Halted          — this broker's CatalogApplier halted at `offset` (Task 7):
//                     terminal for the process; every DDL answers Internal (spec
//                     §9), whatever the replica's role.  The control thread
//                     resigns the controllership of a halted broker (Task 11).
// Every transition logs "AdminWorker: __catalog role -> <roleName()> (epoch N)"
// at info (Task 24 finds the leader by it).
// ---------------------------------------------------------------------------
struct CatalogFollower         { int32_t leaderId = -1; int32_t epoch = 0; };
struct CatalogLeaderCatchingUp { int32_t epoch = 0; };
struct CatalogLeaderServing    { int32_t epoch = 0; };
struct CatalogHalted           { int64_t offset = -1; int32_t epoch = 0; };
using CatalogRole = std::variant<CatalogFollower, CatalogLeaderCatchingUp, CatalogLeaderServing, CatalogHalted>;

// DdlRequest (one queued DDL: one request message, or a MalformedDdl) lives in
// AdminChannels.h — its decoders produce it.

/// A DDL the leader refuses: nothing is appended; answered at once in the request's
/// own response type (or ErrorResponse / DdlResponse when it carries a detail).
struct DdlRejection {
    uint16_t    code    = 0;
    std::string detail;
    int32_t     topicId = -1;   // the existing object's topic id, when the answer names one
    uint16_t    version = 0;    // its (schema) version, when the answer names one
};

/// F14: FNV-1a-64 over the request's template id, object name, statement text and
/// every operation field (descriptor / ops / plan / config / changes / rf /
/// partitions / persistence / expectedVersion) — never the requestId, clientName,
/// session or replayedFrom.  Deterministic on every broker.
[[nodiscard]] uint64_t requestFingerprint(const DdlRequest& req) noexcept;

/// The client-protocol template id a request arrived as (64/68/70/74..77).
[[nodiscard]] uint16_t requestTemplateId(const DdlRequest& req) noexcept;

/// Binding 4: the answer to an event the encoder refused.  KindOpsMismatch /
/// UnknownKind / UnknownFormatVer are LEADER bugs (the leader built the event) →
/// Internal 6 (logged at error); every other refusal is the input's → InvalidRequest
/// 39 with a detail naming it.  (Oversize client fields are refused by name before
/// encoding; this is the backstop.)
[[nodiscard]] DdlRejection encodeErrorAnswer(rune::catalog::EncodeError e);

class AdminWorker {
public:
    /// \param reg            the SchemaRegistry the applier projects (admin-owned).
    /// \param selfId         this node's id.
    /// \param deps           the answers.
    /// \param viewRegistry   the ViewRegistry the applier projects (null: one is owned here).
    /// \param catalogReplica the __catalog replica (null: no catalog — every DDL answers Internal).
    /// \param catalogPlane   its replication plane (null: single node / no peers).
    AdminWorker(SchemaRegistry& reg,
                int32_t selfId,
                AdminDeps deps,
                ViewRegistry* viewRegistry = nullptr,
                rune::replication::PartitionReplica* catalogReplica = nullptr,
                rune::replication::WorkerReplicationTransportImpl<rune::replication::AeronPubRegistry>*
                    catalogPlane = nullptr);

    // ── Ingress (admin thread, after the request ring drain) ────────────────
    /// Queue a DDL (serialized: one in flight).  Beyond kMaxPendingDdl it is
    /// answered Internal at once (counted by pendingDdlRefused()).
    void submit(DdlRequest req);
    /// GetSchema is a pure read of the projected registry — answered at once.
    void submitGetSchema(const GetSchemaMsg& msg);
    /// Catalog history Task 12 (spec §7): a Subscribe / Unsubscribe / session close
    /// on `__catalog`, served by readServer_ from this broker's replica up to its
    /// HW — on any role, a halted applier included (it serves what it holds
    /// committed).  No catalog replica: the Subscribe is answered Internal.
    void catalogSubscribe(const CatalogSubscribeMsg& m);
    void catalogUnsubscribe(const CatalogUnsubscribeMsg& m);
    /// Authentication Task 10: a Subscribe / Unsubscribe on `__security` (topic -7), served by the SECOND read server
    /// (securityReadServer_) from this broker's `__security` replica - the same contract (backlog up to the HW, one
    /// caught-up marker, the history epoch).  The reader's ADMIN check is the gateway's (Task 12).
    void securitySubscribe(const CatalogSubscribeMsg& m);
    void securityUnsubscribe(const CatalogUnsubscribeMsg& m);
    /// Fix round 0: catalog-read flow control from the control thread - routed by the subscription's id space
    /// (kSecuritySubscriptionIdBit); a session close reaches both servers.
    void catalogReadPaused(const CatalogReadPausedMsg& m) {
        if (rune::gateway::isSecuritySubscriptionId(m.subscriptionId)) {
            if (securityReadServer_) securityReadServer_->onPaused(m.session, m.subscriptionId, m.baseOffset, m.droppedMarker);
        } else if (readServer_) {
            readServer_->onPaused(m.session, m.subscriptionId, m.baseOffset, m.droppedMarker);
        }
    }
    void catalogSessionGone(int64_t session) {
        if (readServer_) readServer_->onSessionGone(session);
        if (securityReadServer_) securityReadServer_->onSessionGone(session);
    }
    void catalogReadResume(const CatalogReadResumeMsg& m) {
        if (rune::gateway::isSecuritySubscriptionId(m.subscriptionId)) {
            if (securityReadServer_) securityReadServer_->onResume(m.session, m.subscriptionId);
        } else if (readServer_) {
            readServer_->onResume(m.session, m.subscriptionId);
        }
    }
    [[nodiscard]] const CatalogReadServer* securityReadServer() const noexcept {
        return securityReadServer_ ? &*securityReadServer_ : nullptr;
    }
    /// Security DDL / bootstrap (admin thread, after the request-ring drain).
    void submitSecurity(SecurityRequest req) { if (securityAdmin_) securityAdmin_->submit(std::move(req)); else rejectSecurityNoLog_(req); }
    [[nodiscard]] const CatalogReadServer* readServer() const noexcept { return readServer_ ? &*readServer_ : nullptr; }
    /// M3: catalog requests wait on the ring while the read server's owed answers are near capacity.
    [[nodiscard]] bool catalogIntakePaused() const noexcept {
        return (readServer_ && readServer_->intakePaused()) || (securityReadServer_ && securityReadServer_->intakePaused());
    }

    /// Task 20: resolve a view NAME for an open-for-read handshake.  Returns a
    /// ViewRowResponse status: 0 OK (out params set), 3 SchemaMismatch (a pin to
    /// a fingerprint other than the CURRENT output's), 4 UnknownView.
    [[nodiscard]] uint16_t openView(std::string_view name, uint64_t requestedFingerprint,
                                    int32_t& outViewTopicId, uint64_t& outFingerprint) const {
        outViewTopicId = -1;
        outFingerprint = 0;
        const ViewRecord* vrec = viewRegistry_->byName(name);
        if (vrec == nullptr) return 4;
        outViewTopicId = vrec->viewTopicId;
        const RegistryEntry* latest = reg_.latest(vrec->viewTopicId);
        if (latest == nullptr) return 4;
        outFingerprint = latest->fingerprint;
        if (requestedFingerprint != 0 && requestedFingerprint != latest->fingerprint) return 3;
        return 0;
    }

    // ── Boot (startup thread, BEFORE the admin thread starts — binding 7) ───
    /// Rigs without a BrokerNode only: the one boot replay of the local __catalog
    /// prefix (applier.replayLocal through `boot`), then applier.goLive(`live`).
    /// Throws std::runtime_error (refuse to start) when the replay refuses — a
    /// damaged or too-far checkpoint.  No catalog replica: goes live on an empty
    /// catalog.  Returns the events replayed.  A BrokerNode never calls it (Task 16,
    /// F4): its STARTUP phase ran the one replay before any builder and hands the
    /// booted applier over with adoptBootedApplier — a second replay is refused
    /// by the applier.
    int bootCatalog(BootEffects boot, LiveEffects live);

    /// Task 16: take over the applier the broker's startup phase booted (phase
    /// Booted — std::runtime_error otherwise: the broker refuses to start).  The
    /// admin thread's FIRST pass makes it Live through `live` (the ring encoders);
    /// nothing is replayed again.  The applier's registries must be the ones this
    /// worker was built over.  Startup thread, before the admin thread starts.
    void adoptBootedApplier(CatalogApplier booted, LiveEffects live);

    /// Task 16 (B6, fix round 1 R2): the broker booted with a local catalog it cannot trust (empty while
    /// catalog-made topic dirs exist).  Once this broker, as a FOLLOWER, applied the leader's committed
    /// history, the admin thread writes the durable catch-up sentinel (`caughtup <offset>`); the topic-dir
    /// reconcile itself runs at the NEXT boot, on the startup thread — never here.  A halt cancels it (the
    /// sentinel stays pending).  Startup thread, before the admin thread starts.
    void deferTopicDirReconcile(std::string dataDir);
    /// "None" | "AwaitingCatchUp" | "Done" — "Done" = the sentinel was written (or the wait was cancelled).
    [[nodiscard]] std::string_view bootReconcileStateName() const noexcept;

    // ── Roles (control → admin, admin thread) ───────────────────────────────
    /// The __catalog replica's role (multi-node: the committed (-6, 0) assignment;
    /// single node: the consensus leader).  Demotion answers an in-flight DDL the
    /// retriable NotLeader (spec §5.2): the retry dedupes on the new leader if the
    /// event committed.
    void onCatalogRole(const AdminRoleMsg& role);
    /// ISR v3 Step 10: the committed __catalog leadership is HELD (storage unconfirmed): never lead; ask the controller
    /// for a failover (the restart-fence bump) and serve as a follower of nobody.
    void onCatalogHoldLeadership(const AdminRoleMsg& role);
    /// control → admin (Task 11 fix round 1, I2): the placement view — the
    /// leader's dry-run refuses an explicit rf above `placeable` with
    /// InvalidRequest naming the shortfall and the halted brokers.  Until the
    /// first view arrives every configured broker is placeable.
    void onPlacementView(const PlacementViewMsg& v) noexcept { placement_ = v; placementKnown_ = true; }
    [[nodiscard]] int32_t placeableBrokers() const noexcept {
        return placementKnown_ ? placement_.placeable : deps_.brokerCount;
    }
    /// control -> admin (Task 15, D33): the keys of an established inter-broker link.  This thread's OWN store; Task 16
    /// hands them to the admin plane's PeerFrameCrypto.  Cold.
    void onPeerLinkKeys(const PeerLinkKeysMsg& m, int64_t nowNs) noexcept {
        rune::peerlink::LinkKeys k;
        k.epoch = m.epoch; k.sendMaster = m.sendMaster; k.recvMaster = m.recvMaster;
        if (peerKeys_.install(m.peer, k, nowNs)) ++peerLinkKeysSeen_;
    }
    [[nodiscard]] uint32_t peerLinkEpochForTest(int32_t peer) const noexcept { return peerKeys_.currentEpoch(peer); }
    [[nodiscard]] uint64_t peerLinkKeysSeenForTest() const noexcept { return peerLinkKeysSeen_; }
    /// control → admin: the controller's AlterIsrResponse for (-6, 0), verbatim.
    void onCatalogAlterIsrResponse(const rune::replication::AlterIsrResponseMsg& r);

    // ── `__security` (authentication Task 9, D39) ───────────────────────────
    // The admin thread owns the `__security` replica and its SecurityApplier beside `__catalog`'s: the same plane
    // (frames dispatched by topic id), the same pass.  Startup thread, before the admin thread starts:
    /// Take over the applier the startup phase booted (phase Booted — std::runtime_error otherwise: the broker
    /// refuses to start) and the `__security` replica (null: no security log).  The admin thread's FIRST pass
    /// makes the applier Live through `live` (the ring encoders); nothing is replayed again.
    void attachSecurity(SecurityApplier booted, rune::replication::PartitionReplica* replica,
                        SecurityLiveEffects live);
    /// Roles (control → admin, admin thread) - the SecurityAdmin's.
    void onSecurityRole(const AdminRoleMsg& role) { if (securityAdmin_) securityAdmin_->onRole(role); }
    void onSecurityHoldLeadership(const AdminRoleMsg& role) { if (securityAdmin_) securityAdmin_->onHoldLeadership(role); }
    void onSecurityAlterIsrResponse(const rune::replication::AlterIsrResponseMsg& r) {
        if (securityAdmin_) securityAdmin_->onAlterIsrResponse(r);
    }
    [[nodiscard]] rune::replication::PartitionReplica* securityReplica() const noexcept { return securityReplica_; }
    [[nodiscard]] const SecurityApplier* securityApplier() const noexcept {
        return securityApplier_ ? &*securityApplier_ : nullptr;
    }
    [[nodiscard]] SecurityApplier* securityApplierForTest() noexcept { return securityApplier_ ? &*securityApplier_ : nullptr; }
    [[nodiscard]] const SecurityAdmin* securityAdmin() const noexcept { return securityAdmin_ ? &*securityAdmin_ : nullptr; }
    /// Test seam (the 3-node suites): while held, doWork does not run the `__security` applier.
    void holdSecurityApplyForTest(bool on) noexcept { securityApplyHeldForTest_ = on; }

    /// Drive the plane, the replica and the applier; advance / start DDL.
    int doWork(int64_t nowNs);

    // ── Introspection ───────────────────────────────────────────────────────
    [[nodiscard]] rune::replication::PartitionReplica* catalogReplica() const noexcept { return catalogReplica_; }
    [[nodiscard]] const CatalogFrameStats& catalogFrameStats() const noexcept { return catalogFrameStats_; }
    [[nodiscard]] const CatalogApplier& applier() const noexcept { return applier_; }
    [[nodiscard]] CatalogApplier& applierForTest() noexcept { return applier_; }
    [[nodiscard]] const CatalogRole& role() const noexcept { return role_; }
    /// "Follower" | "LeaderCatchingUp" | "LeaderServing" | "Halted".
    [[nodiscard]] std::string_view roleName() const noexcept;
    /// Test seam (Task 11, CatalogLeadershipRegression): while held, doWork does
    /// not run the applier — a new leader stays CatchingUp with records it holds
    /// committed but unapplied.  Never set in production.
    void holdApplyForTest(bool on) noexcept { applyHeldForTest_ = on; }
    /// Test seam (Task 22, the 3-node slice): while held, an in-flight DDL is never ANSWERED (nor timed
    /// out) although its event commits and applies normally — the leader that is crashed meanwhile dies
    /// with a committed, unanswered DDL (the in-doubt client).  Never set in production.
    void holdAnswersForTest(bool on) noexcept { answersHeldForTest_ = on; }
    /// The catalog leader serving DDL.
    [[nodiscard]] bool isLeader() const noexcept { return std::holds_alternative<CatalogLeaderServing>(role_); }
    [[nodiscard]] std::size_t pendingCount() const noexcept { return pending_.size(); }
    static constexpr std::size_t kMaxPendingDdl = 256;
    /// Task 12: concurrent catalog subscriptions per broker (beyond: answered Internal).
    static constexpr std::size_t kMaxCatalogSubscriptions = 256;
    [[nodiscard]] uint64_t pendingDdlRefused() const noexcept { return pendingDdlRefused_; }
    [[nodiscard]] bool hasInFlight() const noexcept { return !std::holds_alternative<Idle>(inFlight_); }
    /// "Idle" | "AwaitingCommit".
    [[nodiscard]] std::string_view inFlightStateName() const noexcept;
    /// Dequeued DDLs (startNext_) and answers sent for them (answer_): at most one
    /// started DDL is unanswered — started - answered == (hasInFlight() ? 1 : 0).
    /// A queue-overflow refusal is neither (pendingDdlRefused()).
    [[nodiscard]] uint64_t startedCount() const noexcept { return started_; }
    [[nodiscard]] uint64_t answeredCount() const noexcept { return answered_; }
    /// Events appended to __catalog by this broker.
    [[nodiscard]] uint64_t eventsAppended() const noexcept { return eventsAppended_; }
    /// Task 23 fix round 2 (slice Recipe 3): in-flight DDLs answered with the retriable NotLeader, by CAUSE
    /// (admin-thread-owned plain counters; read ON the admin thread via BrokerNode::catalogNotLeaderCountsForTest).
    [[nodiscard]] uint64_t notLeaderBelowMinIsr() const noexcept { return notLeaderBelowMinIsr_; }
    [[nodiscard]] uint64_t notLeaderCommitTimeout() const noexcept { return notLeaderCommitTimeout_; }
    [[nodiscard]] uint64_t notLeaderDemoted() const noexcept { return notLeaderDemoted_; }
    /// Test seam (Task 23 fix round 3, T7): fabricate one "demoted" NotLeader count WITHOUT a demotion (product
    /// evidence with no stall behind it).  Admin thread only (BrokerNode::bumpCatalogNotLeaderDemotedForTest).
    void bumpNotLeaderDemotedForTest() noexcept { ++notLeaderDemoted_; stampEvidence_(); }
    /// Task 23 fix round 3 (R2-I1): the largest gap between two consecutive doWork passes (ns) since the last
    /// reset: "this thread was not scheduled".  Admin-thread-owned plain value, read ON the admin thread
    /// (BrokerNode::passGapsForTest).  The reset keeps the last pass time, so a gap spanning it still counts.
    [[nodiscard]] int64_t maxPassGapNs() const noexcept { return maxPassGapNs_; }
    void resetMaxPassGapForTest() noexcept { maxPassGapNs_ = 0; }
    /// Task 23 fix round 4 (R3-M1): when this admin thread produced topology evidence - a by-cause NotLeader answer or a
    /// catalog role-epoch change - since the last reset: the first and the last time (its pass clock, ns; 0 = none).
    /// Admin-thread-owned plain values, read ON the admin thread (BrokerNode::evidenceTimesForTest).
    struct EvidenceTimes { int64_t firstNs = 0, lastNs = 0; };
    [[nodiscard]] EvidenceTimes evidenceTimes() const noexcept { return evidence_; }
    void resetEvidenceTimesForTest() noexcept { evidence_ = {}; }
    /// Pre-v8 DDLs refused on a non-leader (F37).
    [[nodiscard]] uint64_t preV8Refused() const noexcept { return preV8Refused_; }
    /// DDLs refused because this broker leads __catalog without its (wiped) history (fix round 2, F2).
    [[nodiscard]] uint64_t ddlRefusedUntrustedLeader() const noexcept { return ddlRefusedUntrustedLeader_; }
    [[nodiscard]] static constexpr int64_t ddlTimeoutNs() noexcept { return kDdlTimeoutNs; }

    // ── The in-flight DDL (rule 3) ──────────────────────────────────────────
    struct Idle {};
    /// Appended at `offset`; answered once the applier applied it (OK), the commit
    /// timed out (NotLeader), the leader was demoted (NotLeader) or apply halted
    /// (Internal).
    struct AwaitingCommit {
        DdlRequest                  req;
        rune::catalog::CatalogEvent event;
        int64_t                     offset    = -1;
        int64_t                     startedNs = 0;
    };
    using InFlight = std::variant<Idle, AwaitingCommit>;

private:
    using Rejected = std::unexpected<DdlRejection>;
    using Built    = std::expected<rune::catalog::CatalogEvent, DdlRejection>;

    void startNext_(int64_t nowNs);
    bool advance_(int64_t nowNs);
    bool updateRole_();
    void demote_(int32_t leaderId, int32_t epoch);
    /// The ONE place role_ changes: logs the transition (Task 11 / Task 24).
    void setRole_(CatalogRole next);
    /// Leads, HW == LEO and every committed record applied (the Serving gate).
    [[nodiscard]] bool caughtUp_() const noexcept;

    // Event building — one function per request kind; each validates against the
    // applied state (the library's own checks, for the answer's CODE) and fills
    // the operation.  buildEvent_ adds the principal / statement / time and
    // dry-runs the library rules (the gate).
    [[nodiscard]] Built buildEvent_(const DdlRequest& req, const rune::catalog::CatalogState& s) const;
    [[nodiscard]] Built buildDefine_(const DefineTopicMsg& m, const rune::catalog::CatalogState& s) const;
    [[nodiscard]] Built buildAlter_(const AlterTopicMsg& m, const rune::catalog::CatalogState& s) const;
    [[nodiscard]] Built buildCreateView_(const CreateViewMsg& m, const rune::catalog::CatalogState& s) const;
    [[nodiscard]] Built buildAlterView_(const AlterViewMsg& m, const rune::catalog::CatalogState& s) const;
    [[nodiscard]] Built buildDropView_(const DropViewMsg& m, const rune::catalog::CatalogState& s) const;
    [[nodiscard]] Built buildDropTopic_(const DropTopicMsg& m, const rune::catalog::CatalogState& s) const;
    [[nodiscard]] Built buildAlterConfig_(const AlterTopicConfigMsg& m, const rune::catalog::CatalogState& s) const;
    /// A view definition (CreateView / AlterView) at `viewTopicId`.
    [[nodiscard]] std::expected<rune::catalog::ViewDef, DdlRejection>
        viewDef_(const std::string& name, const std::string& keyField, uint8_t persistence,
                 const std::vector<uint8_t>& planBlob, int32_t viewTopicId,
                 const rune::catalog::CatalogState& s) const;
    /// Encode + append `e` (key = objectId LE) — false: answered already.
    bool appendEvent_(const DdlRequest& req, rune::catalog::CatalogEvent e, int64_t nowNs);

    // Answers — every exit of a STARTED DDL goes through answer_(req, Response),
    // the ONE place answeredCount() moves; respond_ only encodes (a queue-overflow
    // refusal, never started, calls it directly).
    struct Response {
        uint16_t         code            = 0;
        std::string_view detail;
        int32_t          topicId         = -1;
        uint16_t         version         = 0;
        uint64_t         objectId        = 0;
        uint32_t         newVersion      = 0;
        int32_t          gateTopicId     = -1;
        uint64_t         viewFingerprint = 0;
    };
    void answer_(const DdlRequest& req, const Response& r);
    void answer_(const DdlRequest& req, uint16_t code, std::string_view detail = {}, int32_t topicId = -1,
                 uint16_t version = 0);
    void answerRejection_(const DdlRequest& req, const DdlRejection& r);
    void answerOk_(const DdlRequest& req, const rune::catalog::RequestRecord& r);
    void respond_(const DdlRequest& req, const Response& r);
    /// The RequestRecord of a requestId-less DDL (F28): from the applier's last
    /// applied event, which must be the in-flight one.
    [[nodiscard]] std::optional<rune::catalog::RequestRecord> recordOfLastApplied_(int64_t offset) const;
    /// A view answer's output fingerprint (the view at `r`'s version; else read
    /// from the event in the log).  nullopt: it cannot be recovered (answered Internal).
    [[nodiscard]] std::optional<uint64_t> viewOutputFingerprint_(const rune::catalog::RequestRecord& r) const;

    SchemaRegistry&                       reg_;
    int32_t                               selfId_;
    AdminDeps                             deps_;
    std::unique_ptr<ViewRegistry>         ownViews_;                  // when the caller passed none
    ViewRegistry*                         viewRegistry_  = nullptr;
    rune::replication::PartitionReplica*  catalogReplica_ = nullptr;
    rune::replication::WorkerReplicationTransportImpl<rune::replication::AeronPubRegistry>*
                                          catalogPlane_   = nullptr;
    CatalogFrameStats                     catalogFrameStats_;
    // D26 (mirrors the worker's replScratch_): the catalog replica's disk reads
    // pread into this preallocated buffer, never through the segment mmap.
    rune::storage::ZeroedBytes          catalogReadScratch_;   // ZeroedBytes: memset, not a per-byte vector fill (Debug-build cost)
    static constexpr int kCatalogFragmentLimit = 16;

    CatalogApplier                        applier_;
    // Authentication Task 9: the `__security` log (admin-owned beside the catalog's).
    rune::replication::PartitionReplica*  securityReplica_ = nullptr;
    std::optional<SecurityApplier>        securityApplier_;
    std::optional<SecurityAdmin>          securityAdmin_;
    std::optional<SecurityLiveEffects>    pendingSecurityLive_;   // the booted applier's goLive, owed to the first pass
    bool                                  securityApplyHeldForTest_ = false;
    CatalogRole                           role_{CatalogFollower{}};
    std::optional<CatalogReadServer>      readServer_;   // set iff catalogReplica_ (Task 12)
    std::optional<CatalogReadServer>      securityReadServer_;   // set iff the security log is attached (auth Task 10)
    void rejectSecurityNoLog_(const SecurityRequest& req);

    InFlight               inFlight_;
    std::deque<DdlRequest> pending_;            // <= kMaxPendingDdl
    std::vector<uint8_t>   eventBuf_;           // the encoded event (capacity reused)
    uint64_t pendingDdlRefused_ = 0;
    uint64_t started_           = 0;
    uint64_t answered_          = 0;
    uint64_t eventsAppended_    = 0;
    uint64_t notLeaderBelowMinIsr_ = 0, notLeaderCommitTimeout_ = 0, notLeaderDemoted_ = 0;   // by cause
    int64_t  lastPassNs_ = 0, maxPassGapNs_ = 0;   // R2-I1: the scheduling-gap signal (one subtract + compare per pass)
    EvidenceTimes evidence_{};                     // R3-M1: stamped (cold path) by every by-cause NotLeader / epoch change
    void stampEvidence_() noexcept {
        if (evidence_.firstNs == 0) evidence_.firstNs = lastPassNs_;
        evidence_.lastNs = lastPassNs_;
    }
    uint64_t preV8Refused_      = 0;
    uint64_t ddlRefusedUntrustedLeader_ = 0;
    bool     applyHeldForTest_  = false;   // holdApplyForTest (test seam only)
    bool     answersHeldForTest_ = false;  // holdAnswersForTest (test seam only)

    // The booted applier's goLive, owed to the admin thread's first pass (Task 16).
    std::optional<LiveEffects> pendingLive_;
    // The deferred boot reconcile (B6) — rule 3: one state, its own variables.
    struct NoBootReconcile {};
    struct AwaitingCatchUp { std::string dataDir; bool leaderWaitLogged = false; };
    struct BootReconcileDone {};
    std::variant<NoBootReconcile, AwaitingCatchUp, BootReconcileDone> bootReconcile_{NoBootReconcile{}};
    /// A FOLLOWER that applied the catalog leader's committed prefix: its leader told it its HW and
    /// answered its last fetch empty, it applied everything it holds, and that is not nothing.  A broker
    /// that LEADS the catalog is never "caught up" this way (its history is whatever its own log holds).
    [[nodiscard]] bool bootReconcileReady_() const noexcept;
    int stepBootReconcile_();
    rune::peerlink::PeerLinkKeyStore peerKeys_;   // Task 15: the admin plane's view of the inter-broker links
    uint64_t                         peerLinkKeysSeen_ = 0;
    PlacementViewMsg placement_{};         // the control thread's placement view (onPlacementView)
    bool             placementKnown_ = false;

    // An appended event that has not committed within this bound is answered the
    // retriable NotLeader (F13: it may still commit; the retry with the same
    // requestId dedupes to its result).  An error-path bound, never a convergence gate.  It MUST stay below
    // the client's default DDL deadline (ClientConfig::ddlTimeoutMs = 15 s), or the client times out
    // (no answer, no retry budget) before this NotLeader can reach it.
    static constexpr int64_t kDdlTimeoutNs = 10LL * 1'000'000'000LL;
};

} // namespace rune::node
