#pragma once
#include <cstdint>
#include <deque>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>
#include "rune/node/SchemaRegistry.h"
#include "rune/node/AdminChannels.h"
#include "rune/node/ViewRegistry.h"
#include <rune/view/ViewPlan.h>
#include <rune/codec/Descriptor.h>

namespace rune::replication { class PartitionReplica; }

namespace rune::node {

// Error code integers (fixed by Task 1 ErrorCode enum; AdminWorker passes ints
// to stay protocol-agnostic):
//   OK                    = 0
//   Internal              = 6
//   UnknownTopic          = 7
//   InvalidSchema         = 23
//   SchemaValidationFailed= 24  (not used in this task)
//   TopicExists           = 25
//   SchemaIncompatible    = 30  (ALTER: ops rejected by the compatibility gate)
//   SchemaConflict        = 31  (ALTER: stale expectedVersion — optimistic CAS)
//   ViewSourceTransient   = 37  (CREATE VIEW: persisted view over a transient source)
//   ViewNotAlterable      = 38  (ALTER TOPIC naming a view's output topic)
//   InvalidTopicName      = 40  (DefineTopic/CreateView: name fails TopicName.h)
//   ViewSourceIsView      = 41  (CREATE VIEW whose source is another view's output)
//   (InvalidSchema 23 also answers DefineTopic.partitionCount < 1)

// ---------------------------------------------------------------------------
// AdminDeps — effects the AdminWorker needs, all resolved on the admin thread.
//
// RULING-2: the AdminWorker SOLELY owns the __topic_schemas replica.  It does
// NOT reach into TopicRegistry (control-thread-owned) or the WorkerPool
// directly across threads — every cross-thread effect is a ring hand-off:
//
//   requestCreateTopic — post a CreateTopicRequest to the CONTROL thread (which
//                        owns TopicRegistry + ReplicaRegistry) and returns a
//                        ticket; the authoritative id arrives later on the
//                        create-topic reply ring, drained in doWork().
//   pushValidators     — enqueue SchemaUpdate WorkerOps to each worker via their
//                        SPSC ingress (still a ring hand-off; workers apply on
//                        their own thread).
//   respondDefine/Get  — encode a response frame + send on the originating
//                        session's egress (via the gateway's control-frame path).
// ---------------------------------------------------------------------------
struct AdminDeps {
    // CREATING: post a create-topic request to the control thread.  Returns true
    // if the request was enqueued on the ring (false = ring full / no channel).
    // The authoritative topicId arrives later via CreateTopicReplyMsg.
    // Task 19: `keyField` + `cleanupPolicy` let the control thread create a VIEW
    // SINK as a keyed + Compact "table".  DefineTopic passes ("", 0) — the
    // pre-Task-19 Delete topic.  Phase 2b E: `persistence` (0 = disk, 1 = transient)
    // selects the topic's log backend on the control thread; a view sink is
    // Disk (0) in Part E; it follows the view's persistence once sinks carry change
    // events (parked).
    std::function<bool(int64_t ticket, int32_t partitionCount,
                       const std::string& name, const std::string& keyField,
                       uint8_t cleanupPolicy, uint8_t persistence)> requestCreateTopic;

    // PUBLISHING: push validators to each produce worker after commit (validate=true).
    std::function<void(const RegistryEntry&, int32_t topicId)> pushValidators;

    // Task 19: fan out a committed compiled view plan to the control thread,
    // which delivers it to each worker's WorkerViewSet (installView on the
    // worker's OWN thread — never cross-thread).  Admin → control ring hand-off.
    std::function<void(const ViewPlanUpdateMsg&)> pushViewPlan;

    // B10 / D4 fix round 1: a runtime topic's catalog record COMMITTED (DefineTopic
    // schema record / CreateView record past the HW gate).  Posted on the
    // admin→control ring (kA2cTopicCommitted); the control thread rewrites the
    // topic's dir marker pending -> committed, so boot never DELETES its data even
    // if the catalog is later damaged.  Optional (unset = no marker tracking).
    std::function<void(int32_t topicId)> topicCommitted;

    // Task 19: send the CreateView response back to the client (encode + egress).
    std::function<void(int64_t session, int64_t corr, int32_t viewTopicId,
                       uint64_t outputFingerprint, uint16_t errorCode)> respondCreateView;

    // Send the DefineTopic response back to the client (encode + egress).
    std::function<void(int64_t session, int64_t corr, int32_t topicId,
                       uint16_t version, uint16_t errorCode)> respondDefine;

    // Send the GetTopicSchema response back to the client (encode + egress).
    // `partitionCount` (Phase 2b Part C) is the TOPIC's partition count — the
    // typed client routes records by key over it; 0 on any error response.
    std::function<void(int64_t session, int64_t corr, int32_t topicId,
                       uint16_t version, int32_t partitionCount,
                       const std::vector<uint8_t>& blob,
                       uint16_t errorCode)> respondGet;

    // Send the AlterTopic response back to the client (encode + egress).  On OK,
    // `newVersion` is the freshly committed contiguous version; on any error it
    // carries the current version (so the client can re-read and retry CAS).
    std::function<void(int64_t session, int64_t corr, int32_t topicId,
                       uint16_t newVersion, uint16_t errorCode)> respondAlter;
};

/// Single-threaded, AdminWorker-owned DDL state machine (RULING-2).
///
/// SOLE owner of the __topic_schemas PartitionReplica: the admin thread is the
/// ONLY thread that calls schemaReplica_->doWork()/appendKeyed()/becomeLeader().
/// No locks, no atomics — the caller owns the thread (shared-nothing, rule #1).
///
/// The DDL path is ASYNC because createTopic is a ring round-trip to the control
/// thread (which owns TopicRegistry).  At most one DefineTopic is in flight; the
/// rest queue.  Each state holds exactly the variables it needs (rule #3).
class AdminWorker {
public:
    /// \param reg           in-memory schema registry (admin-thread-owned).
    /// \param replica       the __topic_schemas PartitionReplica (may be null in
    ///                      pure-unit tests that stub appendDurable; when null,
    ///                      appendDurable/rebuild are no-ops).
    /// \param selfId        this node's id (for becomeLeader ISR seeding).
    /// \param replicaSet    the full replica set (for becomeLeader ISR seeding).
    /// \param deps          effects.
    AdminWorker(SchemaRegistry& reg,
                rune::replication::PartitionReplica* replica,
                int32_t selfId,
                std::vector<int32_t> replicaSet,
                AdminDeps deps,
                // Task 19: the __views catalog + its replica.  Both admin-thread-
                // owned (the admin thread is the ONLY thread that drives/append
                // the views replica + mutates the ViewRegistry).  Null in pure
                // unit tests / deployments with no view catalog (CreateView then
                // responds kInternal).
                rune::replication::PartitionReplica* viewsReplica = nullptr,
                ViewRegistry* viewRegistry = nullptr);

    // -----------------------------------------------------------------------
    // Ingress — enqueue an op (called on the ADMIN thread by AdminWorkerAgent
    // after draining the request ring).
    // -----------------------------------------------------------------------
    void submitDefineTopic(DefineTopicMsg msg);
    void submitGetSchema(const GetSchemaMsg& msg);

    // CREATE VIEW (Task 19).  Queued onto the SAME pending_ deque as DefineTopic
    // so it is SERIALIZED with every other DDL op (one inFlight_ at a time).
    void submitCreateView(CreateViewMsg msg);

    // ALTER TOPIC (Task 11).  Queued onto the SAME pending_ deque as DefineTopic
    // so it is SERIALIZED with CREATE and other ALTERs (one inFlight_ at a time).
    void submitAlterTopic(AlterTopicMsg msg);

    // -----------------------------------------------------------------------
    // Task 20: openView — resolve an open-for-read handshake for a view NAME.
    // A pure read of the admin-owned catalogs (no round-trip), mirroring the
    // GetTopicSchema byFingerprint handshake: the view's output is a registered
    // typed topic (CreateView registered its output descriptor in the
    // SchemaRegistry under viewTopicId), so we resolve name -> viewTopicId via
    // the __views catalog, then confirm the client's pinned fingerprint against
    // the view's CURRENT output schema — drift is caught UP FRONT at open.
    //
    // Returns a ViewRowResponse status code:
    //   0 = OK            (outViewTopicId + outFingerprint carry the resolved id
    //                      and the current output fingerprint the client pins)
    //   3 = SchemaMismatch (a non-zero requestedFingerprint matches no live
    //                      version of the view's output schema)
    //   4 = UnknownView   (no __views catalog, name not found, or the view's
    //                      output descriptor is not registered)
    // -----------------------------------------------------------------------
    [[nodiscard]] uint16_t openView(std::string_view name, uint64_t requestedFingerprint,
                                    int32_t& outViewTopicId, uint64_t& outFingerprint) const {
        outViewTopicId = -1;
        outFingerprint = 0;
        if (viewRegistry_ == nullptr) return 4;                 // UnknownView
        const ViewRecord* vrec = viewRegistry_->byName(name);
        if (vrec == nullptr) return 4;                          // UnknownView
        outViewTopicId = vrec->viewTopicId;
        // Reuse GetTopicSchema's handshake against the view's output topic id.
        const RegistryEntry* latest = reg_.latest(vrec->viewTopicId);
        if (latest == nullptr) return 4;                        // output not registered
        outFingerprint = latest->fingerprint;
        // CURRENT-only contract: a view row is served encoded in the view's
        // CURRENT output layout, and executeGetViewRowWire_ accepts only the
        // current output fingerprint.  So a pin to any non-current fingerprint
        // (even a registered historical one) MUST be rejected here — otherwise a
        // client would pass open but fail every GetViewRow with SchemaMismatch.
        if (requestedFingerprint != 0 && requestedFingerprint != latest->fingerprint) {
            return 3;                                           // SchemaMismatch (drift)
        }
        return 0;                                               // OK
    }

    // -----------------------------------------------------------------------
    // control → admin: create-topic reply.  Matches the ticket of the in-flight
    // DefineTopic and advances the state machine (AWAITING_ID → AWAITING_COMMIT).
    // -----------------------------------------------------------------------
    void onCreateTopicReply(const CreateTopicReplyMsg& reply);

    // -----------------------------------------------------------------------
    // control → admin: role transition for the schema replica.  Applied on the
    // admin thread (becomeLeader/becomeFollower) — no cross-thread role_ write.
    // C: on a Follower→Leader edge, rebuild the SchemaRegistry from the log.
    // B4: demotion answers an in-flight DDL op Internal at once (never applied
    // live; an op demoted in AwaitingCommit is in doubt — KL2).
    // -----------------------------------------------------------------------
    void onRole(const AdminRoleMsg& role);

    // -----------------------------------------------------------------------
    // doWork — drive the replica + advance the in-flight DDL state machine.
    // Returns a work count (>0 if progress made).  Called every admin-thread tick.
    // -----------------------------------------------------------------------
    int doWork(int64_t nowNs);
    /// The __topic_schemas replica this (admin) thread solely drives.
    [[nodiscard]] rune::replication::PartitionReplica* replica() const noexcept { return replica_; }

    // -----------------------------------------------------------------------
    // Test / introspection.
    // -----------------------------------------------------------------------
    [[nodiscard]] bool isLeader() const noexcept { return leader_; }
    [[nodiscard]] std::size_t pendingCount() const noexcept { return pending_.size(); }
    /// No-hang fix round 3: the DDL queue bound; a request beyond it is answered Internal.
    static constexpr std::size_t kMaxPendingDdl = 256;
    [[nodiscard]] uint64_t pendingDdlRefused() const noexcept { return pendingDdlRefused_; }
    /// B4 fix round 1: in-flight ops answered Internal because their commit
    /// replica (or the view catalog) was missing in AwaitingCommit — never dropped.
    [[nodiscard]] uint64_t commitReplicaMissing() const noexcept { return commitReplicaMissing_; }
    /// The schema / __views replica pair (test access swaps it — final review M4:
    /// no public mutator; see AdminWorkerTestAccess in the tests' support headers).
    struct ReplicasForTest {
        rune::replication::PartitionReplica* schema = nullptr;
        rune::replication::PartitionReplica* views  = nullptr;
    };
    /// Final review M2: DDL ops refused Internal because the last promotion's
    /// catalog rebuild failed (__topic_schemas or __views).
    [[nodiscard]] uint64_t refusedForFailedRebuild() const noexcept { return refusedForFailedRebuild_; }

    // Fail-loud recovery signal (regression fix): true iff the most recent
    // rebuildFromLog_() (Follower->Leader promotion replay) hit a corrupt or
    // truncated __topic_schemas record and ABORTED rather than silently
    // skipping it. A silent skip would leave an UNDETECTED version gap in
    // SchemaRegistry's per-topic deque (indexed by version-1) — wrong/missing
    // schema versions served after recovery. Callers (tests, and any future
    // control-plane wiring) poll this to detect/halt instead of trusting a
    // silently-incomplete registry.
    [[nodiscard]] bool rebuildFailed() const noexcept { return rebuildFailed_; }
    /// The view-catalog counterpart (recoverViewsOnPromotion_ hit a corrupt record).
    [[nodiscard]] bool viewsRebuildFailed() const noexcept { return viewsRebuildFailed_; }

    // DDL conformance (tests): with startedCount() a test proves AT MOST ONE op is
    // ever started-but-unanswered — started - answered == (hasInFlight() ? 1 : 0).
    [[nodiscard]] bool hasInFlight() const noexcept {
        return !std::holds_alternative<Idle>(inFlight_);
    }
    /// Task 10b (hard rule 3): the in-flight state's name — "Idle",
    /// "DefineAwaitingId", "DefineAwaitingCommit", "AlterAwaitingCommit",
    /// "ViewAwaitingId" or "ViewAwaitingCommit".
    [[nodiscard]] std::string_view inFlightStateName() const noexcept;
    /// Queued DDL ops dequeued by startNext_ (each is answered exactly once,
    /// synchronously or later).  Monotone; admin-thread-owned.
    [[nodiscard]] uint64_t startedCount() const noexcept { return started_; }
    /// The in-flight error-path bound (see kDdlTimeoutNs).
    [[nodiscard]] static constexpr int64_t ddlTimeoutNs() noexcept { return kDdlTimeoutNs; }

    // ── In-flight DDL state machine (hard rule 3: each STATE holds exactly the
    //    variables that state needs — no flat op struct, no kind/state flags).
    //    Transitions construct the next state from the previous one (fields are
    //    moved across).  Every alternative but Idle is an op that has been started
    //    and not yet answered; every exit answers it and returns to Idle.
    //    Public so the Task 10b regression can pin the shape at compile time.
    struct Idle {};
    // DefineTopic: create-topic request posted to the control thread; waiting for
    // the reply ring (ticket match).  `fieldCount` (descriptor field count of
    // msg.blob, captured once while PARSING) seeds nextTag on commit.
    struct DefineAwaitingId {
        DefineTopicMsg msg;
        int64_t        ticket     = 0;
        int64_t        startedNs  = 0;
        uint16_t       fieldCount = 0;
    };
    // DefineTopic: TopicSchema appended to __topic_schemas; waiting for the HW to
    // cover `appendedLast`.  topicId / partitionCount / persistence are the
    // AUTHORITATIVE values from the create reply (final review I1, Phase 2b E).
    struct DefineAwaitingCommit {
        DefineTopicMsg  msg;
        int64_t         startedNs      = 0;
        uint16_t        fieldCount     = 0;
        int32_t         topicId        = -1;
        int32_t         partitionCount = 0;
        PersistenceMode persistence    = PersistenceMode::Disk;
        int64_t         appendedLast   = -1;
    };
    // AlterTopic: skips the id round-trip (the topic exists).  `alterSchema` is the
    // record applied on the HW gate; its topicId / version are the ALTER's topic id
    // and the contiguous new version computed ONCE in startAlter_.
    struct AlterAwaitingCommit {
        AlterTopicMsg alter;
        int64_t       startedNs    = 0;
        TopicSchema   alterSchema;
        int64_t       appendedLast = -1;
    };
    // CreateView: compiled + validated; keyed+Compact SINK create posted to the
    // control thread; waiting for the sink id.  The output fingerprint is
    // compiledPlan.outputFingerprint.
    struct ViewAwaitingId {
        CreateViewMsg                view;
        int64_t                      ticket    = 0;
        int64_t                      startedNs = 0;
        rune::view::CompiledViewPlan compiledPlan;   // viewTopicId stamped on the reply
        rune::codec::Descriptor      viewOutDesc;    // output descriptor (registered in reg_)
        rune::codec::Descriptor      viewSrcDesc;    // source descriptor (fan-out evolvingSrc)
    };
    // CreateView: ViewRecord appended to __views; waiting for the views replica's
    // HW to cover `appendedLast`.  The view's topic id is viewRecord.viewTopicId;
    // the compiled plan lives ONCE, serialized in viewRecord.compiledPlanBlob (final
    // review M3) — the commit fans out those bytes.  `partitionCount` is the sink's
    // partition count, resolved on the reply (authoritative, or the source's).
    struct ViewAwaitingCommit {
        CreateViewMsg                view;
        int64_t                      startedNs         = 0;
        uint64_t                     outputFingerprint = 0;
        rune::codec::Descriptor      viewOutDesc;
        rune::codec::Descriptor      viewSrcDesc;
        int32_t                      partitionCount    = 1;
        ViewRecord                   viewRecord;     // applied on the HW gate
        int64_t                      appendedLast      = -1;
    };
    using InFlight = std::variant<Idle, DefineAwaitingId, DefineAwaitingCommit,
                                  AlterAwaitingCommit, ViewAwaitingId, ViewAwaitingCommit>;

private:
    // Rebuild the SchemaRegistry from [0, HW) of the schema replica.
    void rebuildFromLog_();
    // B4 fix round 1: AwaitingCommit with no replica/catalog -> log, count, answer Internal.
    void answerMissingCommitReplica_(const char* catalog);

    // Task 19b (restart recovery): the view counterpart of rebuildFromLog_, on
    // the SAME Follower->Leader edge.  Rebuild the ViewRegistry from [0, HW) of
    // the __views replica, then — for every durable view — re-register its
    // output schema in reg_ and fan out a RECOVERY install to the workers via
    // deps_.pushViewPlan (the SAME admin->control->worker path CreateView uses).
    // __views has no other recovery hook (unlike __topic_schemas), and this runs
    // AFTER rebuildFromLog_ so the source descriptors already resolve in reg_.
    void recoverViewsOnPromotion_();

    // Advance the in-flight DDL if possible.  Returns true if work was done.
    bool advance_(int64_t nowNs);

    // Start the next queued DDL op (if idle).
    void startNext_(int64_t nowNs);

    // CREATE VIEW branch of startNext_ (admin thread, leader-guarded; Task 19).
    // Deserializes + compiles + validates the authored plan against the source
    // descriptor(s) in `reg_`, maps a CompileError to the right view error code,
    // ViewExists-guards against `viewRegistry_`, then posts a keyed+Compact sink
    // create request → AwaitingId.  On failure nothing is appended.
    void startCreateView_(CreateViewMsg msg, int64_t nowNs);

    // CREATE VIEW branch of onCreateTopicReply (Task 19): the sink id came back;
    // stamp it onto the compiled plan, append the durable ViewRecord to __views,
    // and move to AwaitingCommit (gated on the views replica's HW).
    void onCreateViewReply_(ViewAwaitingId& op, const CreateTopicReplyMsg& reply);

    // CREATE VIEW commit (Task 19), on the HW gate: apply the ViewRecord to the
    // ViewRegistry, register the view's output descriptor in the SchemaRegistry
    // (so GetTopicSchema/openView works — the view is a typed topic), respond OK,
    // and fan the compiled plan out to the workers (pushViewPlan).
    void commitCreateView_(const ViewAwaitingCommit& op);

    // ALTER branch of startNext_ (admin thread, leader-guarded).  Resolves the
    // topic, runs the optimistic-CAS + compatibility gate, applies the ops to
    // the current evolving descriptor and appends the new durable TopicSchema —
    // ATOMIC: on any failure nothing is appended and the version is not bumped.
    void startAlter_(AlterTopicMsg msg, int64_t nowNs);
    // Answer the in-flight op with `code` in its own response kind (Define /
    // CreateView / Alter) carrying `topicId`, then go Idle.  The exit for an
    // in-flight op that ends without committing (case conflict B3).
    void answerInFlight_(uint16_t code, int32_t topicId);
    // The topic id an uncommitted in-flight op is answered with on timeout /
    // demotion / missing commit replica: the ALTER's topic id, else -1.
    [[nodiscard]] int32_t inFlightAlterTopicId_() const noexcept;

    // ── Version-assignment helper (Task 10) ─────────────────────────────────
    // SOURCE OF TRUTH for the version assigned to a schema commit on
    // `topicId`, shared by DefineTopic (create, this task) and ALTER's own
    // commit path (Task 11). Versions MUST be CONTIGUOUS: SchemaRegistry
    // indexes a topic's versions by (version - 1) in a per-topic std::deque
    // (see SchemaRegistry.h), so skipping ahead leaves an unfilled gap slot.
    // No prior version for `topicId` (always true for DefineTopic, which only
    // ever targets a brand-new topicId just allocated by the control thread)
    // → the topic is being created for the first time → kInitialSchemaVersion.
    [[nodiscard]] uint16_t nextSchemaVersion_(int32_t topicId) const;

    static constexpr uint16_t kInitialSchemaVersion = 1;

    // A queued DDL op: exactly one request message (the alternative IS the kind).
    using PendingDdl = std::variant<DefineTopicMsg, AlterTopicMsg, CreateViewMsg>;

    SchemaRegistry&                       reg_;
    rune::replication::PartitionReplica*  replica_;
    int32_t                               selfId_;
    std::vector<int32_t>                  replicaSet_;
    AdminDeps                             deps_;

    // Task 19: the __views durable catalog (admin-thread-owned, like replica_).
    rune::replication::PartitionReplica*  viewsReplica_  = nullptr;
    ViewRegistry*                         viewRegistry_  = nullptr;

    // Leadership (admin-thread-owned; driven only by onRole()).
    bool     leader_      = false;
    int32_t  epoch_       = 0;
    bool     wasLeader_   = false;   // C: Follower→Leader edge detection

    // Set by rebuildFromLog_() on a corrupt/truncated TopicSchema record; see
    // rebuildFailed() above. Admin-thread-owned (no atomics — shared-nothing).
    bool     rebuildFailed_ = false;
    // M2: the __views counterpart.  While either is set, new DDL is refused
    // Internal: the registries may miss records past the corrupt one, so a create
    // could adopt (Task 14 / KL1b) a committed topic's sink.
    bool     viewsRebuildFailed_ = false;
    uint64_t refusedForFailedRebuild_ = 0;

    // Final review M4: tests swap the replica pointers through this friend only.
    friend struct AdminWorkerTestAccess;

    // In-flight DDL state machine.
    InFlight        inFlight_;              // Idle when no DDL op is in flight
    std::deque<PendingDdl> pending_;        // queued DDL ops (CREATE + ALTER, serialized); <= kMaxPendingDdl
    uint64_t pendingDdlRefused_ = 0;        // fix round 3: answered Internal because the queue was full
    uint64_t commitReplicaMissing_ = 0;     // B4 fix round 1: AwaitingCommit with no commit replica
    int64_t         nextTicket_ = 1;
    uint64_t        started_    = 0;    // ops dequeued by startNext_ (startedCount())

    // Timeout for a stuck in-flight op (createTopic reply lost, or HW never
    // advances because leadership was lost) — respond Internal and move on.
    // Time-independent CORRECTNESS is preserved: this is an ERROR path bound, not
    // a convergence gate (we never sleep to "let things settle").
    static constexpr int64_t kDdlTimeoutNs = 30LL * 1'000'000'000LL;
};

} // namespace rune::node
