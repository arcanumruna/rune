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
#include "rune/node/SchemaRegistry.h"
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

    /// INTERIM (until Task 17 removes RUNE_TOPICS): the static topics of this
    /// broker, in id order.  They hold ids [0, n) in the TopicRegistry, so a catalog
    /// create whose id falls below n is refused explicitly (the two id spaces never
    /// share a process — binding 8).
    std::vector<std::string> staticTopicNames;

    /// The largest admin→control effect frame (0 = catalogEffectMaxFrame(), the
    /// production ring).  The leader refuses an event any of whose effect frames
    /// would not fit (Task 7 C1).  Tests shrink it together with the applier sink's.
    std::size_t effectMaxFrameBytes = 0;

    /// Catalog history Task 12: the catalog read server's egress (production:
    /// the admin → control catalog-read ring, then the session's egress).
    /// false = full (the server keeps the position and retries).  Unset: catalog
    /// subscribes are answered nowhere — every production / rig AdminWorker sets it.
    CatalogReadEgress catalogRead;
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
    /// \param replica        the retired __topic_schemas replica (Task 16 deletes it):
    ///                       its role is still driven by onRole, nothing reads or writes it.
    /// \param selfId         this node's id.
    /// \param replicaSet     every broker (onRole's ISR seed for the retired replicas).
    /// \param deps           the answers.
    /// \param viewsReplica   the retired __views replica (as `replica`).
    /// \param viewRegistry   the ViewRegistry the applier projects (null: one is owned here).
    /// \param catalogReplica the __catalog replica (null: no catalog — every DDL answers Internal).
    /// \param catalogPlane   its replication plane (null: single node / no peers).
    AdminWorker(SchemaRegistry& reg,
                rune::replication::PartitionReplica* replica,
                int32_t selfId,
                std::vector<int32_t> replicaSet,
                AdminDeps deps,
                rune::replication::PartitionReplica* viewsReplica = nullptr,
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
    /// Fix round 0: catalog-read flow control from the control thread.
    void catalogReadPaused(const CatalogReadPausedMsg& m) {
        if (readServer_) readServer_->onPaused(m.session, m.subscriptionId, m.baseOffset);
    }
    void catalogReadResume(const CatalogReadResumeMsg& m) {
        if (readServer_) readServer_->onResume(m.session, m.subscriptionId);
    }
    [[nodiscard]] const CatalogReadServer* readServer() const noexcept { return readServer_ ? &*readServer_ : nullptr; }

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
    /// The one boot replay of the local __catalog prefix (applier.replayLocal
    /// through `boot`), then applier.goLive(`live`).  Throws std::runtime_error
    /// (the broker refuses to start) when the replay refuses — a damaged or
    /// too-far checkpoint.  No catalog replica: goes live on an empty catalog.
    /// Returns the events replayed.  Task 16 moves the replay to the startup
    /// phase proper (F4).
    int bootCatalog(BootEffects boot, LiveEffects live);

    // ── Roles (control → admin, admin thread) ───────────────────────────────
    /// The retired __topic_schemas / __views replicas' role (nothing reads them).
    void onRole(const AdminRoleMsg& role);
    /// The __catalog replica's role (multi-node: the committed (-6, 0) assignment;
    /// single node: the consensus leader).  Demotion answers an in-flight DDL the
    /// retriable NotLeader (spec §5.2): the retry dedupes on the new leader if the
    /// event committed.
    void onCatalogRole(const AdminRoleMsg& role);
    /// control → admin (Task 11 fix round 1, I2): the placement view — the
    /// leader's dry-run refuses an explicit rf above `placeable` with
    /// InvalidRequest naming the shortfall and the halted brokers.  Until the
    /// first view arrives every configured broker is placeable.
    void onPlacementView(const PlacementViewMsg& v) noexcept { placement_ = v; placementKnown_ = true; }
    [[nodiscard]] int32_t placeableBrokers() const noexcept {
        return placementKnown_ ? placement_.placeable : deps_.brokerCount;
    }
    /// control → admin: the controller's AlterIsrResponse for (-6, 0), verbatim.
    void onCatalogAlterIsrResponse(const rune::replication::AlterIsrResponseMsg& r);

    /// Drive the plane, the replica and the applier; advance / start DDL.
    int doWork(int64_t nowNs);

    // ── Introspection ───────────────────────────────────────────────────────
    [[nodiscard]] rune::replication::PartitionReplica* catalogReplica() const noexcept { return catalogReplica_; }
    [[nodiscard]] rune::replication::PartitionReplica* replica() const noexcept { return replica_; }
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
    /// Pre-v8 DDLs refused on a non-leader (F37).
    [[nodiscard]] uint64_t preV8Refused() const noexcept { return preV8Refused_; }
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
    /// INTERIM (binding 8): a create at `topicId` below the static topics' ids.
    [[nodiscard]] std::optional<DdlRejection> staticIdCollision_(int32_t topicId) const;

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
    rune::replication::PartitionReplica*  replica_;          // retired __topic_schemas (role only)
    int32_t                               selfId_;
    std::vector<int32_t>                  replicaSet_;
    AdminDeps                             deps_;
    rune::replication::PartitionReplica*  viewsReplica_  = nullptr;   // retired __views (role only)
    std::unique_ptr<ViewRegistry>         ownViews_;                  // when the caller passed none
    ViewRegistry*                         viewRegistry_  = nullptr;
    rune::replication::PartitionReplica*  catalogReplica_ = nullptr;
    rune::replication::WorkerReplicationTransportImpl<rune::replication::AeronPubRegistry>*
                                          catalogPlane_   = nullptr;
    CatalogFrameStats                     catalogFrameStats_;
    // D26 (mirrors the worker's replScratch_): the catalog replica's disk reads
    // pread into this preallocated buffer, never through the segment mmap.
    std::vector<uint8_t>                  catalogReadScratch_;
    static constexpr int kCatalogFragmentLimit = 16;

    CatalogApplier                        applier_;
    CatalogRole                           role_{CatalogFollower{}};
    std::optional<CatalogReadServer>      readServer_;   // set iff catalogReplica_ (Task 12)

    InFlight               inFlight_;
    std::deque<DdlRequest> pending_;            // <= kMaxPendingDdl
    std::vector<uint8_t>   eventBuf_;           // the encoded event (capacity reused)
    uint64_t pendingDdlRefused_ = 0;
    uint64_t started_           = 0;
    uint64_t answered_          = 0;
    uint64_t eventsAppended_    = 0;
    uint64_t preV8Refused_      = 0;
    bool     applyHeldForTest_  = false;   // holdApplyForTest (test seam only)
    PlacementViewMsg placement_{};         // the control thread's placement view (onPlacementView)
    bool             placementKnown_ = false;

    // An appended event that has not committed within this bound is answered the
    // retriable NotLeader (F13: it may still commit; the retry with the same
    // requestId dedupes to its result).  An error-path bound, never a convergence gate.
    static constexpr int64_t kDdlTimeoutNs = 30LL * 1'000'000'000LL;
};

} // namespace rune::node
