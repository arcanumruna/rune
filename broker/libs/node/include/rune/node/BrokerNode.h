#pragma once
// BrokerNode.h — Broker node consensus runtime (BR-2 + BR-3 + BR-7a).
//
// Assembles the full consensus engine from a NodeConfig:
//   NodeStateFile + TermLog (durable, in config.dataDir)
//   1-node: NullSink → SbeConsensusPublisher (BR-2 path, unchanged)
//   N-node: UdpMessageSink (per-peer ExclusivePublications) + AeronIngress +
//           NodeConsensusAgent (composite: poll ingress + doWork) (BR-3 path)
//   ClientGateway (BR-7a): runs inside the duty-cycle via NodeRuntimeAgent.
//   ClusterMember vector (built from config.peers)
//   Election (startup values)
//   IngressRing
//   ConsensusAgent (inner duty-cycle agent)
//   NodeRuntimeAgent (wraps inner consensus agent + gateway, drives both)
//   MonotonicNanoClock + BackoffIdleStrategy
//   AgentRunner (one thread, no busy-spin)
//   Embedded Aeron media driver + Aeron client
//
// Single-node: self-elects via NullSink, no UDP.
// Multi-node: real per-peer UDP publications + ingress subscription.
// Both paths now serve clients via ClientGateway (single-writer duty-cycle).

#include <rune/node/NodeConfig.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/DriverPumpAgent.h>
#include <rune/node/UdpMessageSink.h>
#include <rune/node/NodeConsensusAgent.h>
#include <rune/node/NodeRuntimeAgent.h>
#include <rune/node/LogCleanerAgent.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/node/GroupCoordinationService.h>
#include <rune/node/TransactionCoordinationService.h>
#include <rune/node/MetadataService.h>
#include <rune/node/WorkerPool.h>
#include <rune/node/Worker.h>
#include <rune/node/ShardAgent.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/WorkerEgress.h>
#include <rune/node/WorkerReplicationTransport.h>
#include <rune/node/WorkerReplicationValidator.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/AdminWorker.h>
#include <rune/node/AdminChannels.h>
#include <rune/node/AdminControlPlane.h>
#include <rune/node/SchemaFanout.h>
#include <rune/node/OwnerThreadTestTask.h>
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/coordination/ReplicatedOffsetStore.h>
#include <rune/transaction/ReplicatedTxnStateStore.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/InMemoryLog.h>

#include <rune/consensus/NodeStateFile.h>
#include <rune/consensus/TermLog.h>
#include <rune/consensus/MessageSink.h>
#include <rune/consensus/NullSink.h>
#include <rune/consensus/SbeConsensusPublisher.h>
#include <rune/consensus/ClusterMember.h>
#include <rune/consensus/Election.h>
#include <rune/consensus/IngressRing.h>
#include <rune/consensus/ConsensusAgent.h>
#include <rune/consensus/AeronIngress.h>
#include <rune/consensus/NanoClock.h>
#include <rune/consensus/IdleStrategy.h>
#include <rune/consensus/AgentRunner.h>
#include <rune/consensus/Types.h>

// Aeron embedded driver (C API)
extern "C" {
#include <aeronmd/aeronmd.h>
}
#include <Aeron.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace rune::node {

// ---------------------------------------------------------------------------
// Max produce-batch size (F3 rounds 4-5).
//
// The broker derives the EFFECTIVE max produce-batch size it accepts from the
// exact per-message maximum of every transport/ring a produced batch must
// cross, and enforces it on EVERY produce path (gateway handleProduce AND
// each Worker's per-worker ingress handler) with MessageTooLarge — so a batch
// that could never be ingested or replicated is never accepted (an accepted-
// but-unreplicable batch would stall its follower forever).
//
// ONE formula (integration wave 2, F3 ⇄ F18 ⇄ F15 S3 ⇄ F19): the effective
// max IS NodeConfig::produceIntakeLimit() = min(explicit RUNE_MAX_PRODUCE_
// BATCH_BYTES if set, Worker::maxAcceptableBatchBytes(config)).  The helpers
// below are views of that formula's components, built on the SAME primitives
// the transports / rings enforce at send / enqueue time:
//   - replication: ReplicationFrameLimits.h (maxFetchRecordsViaOutboundRing /
//     ViaPublication) — what a FetchResponse/push can carry, legacy ring path
//     and per-worker publication path; ALWAYS applied, single-node too (the
//     limit must not change when a node joins a cluster);
//   - worker ingress ring: Worker::maxOpTailBytesFor() — what a WorkerOp tail
//     can carry through the O2O ring (ring maxMsgLength − WorkerOp framing);
//   - Aeron ingress: min(term/8, 16 MiB) − Produce framing — what a Produce
//     frame on this broker's own term can carry.  NOTE: on UDP the CLIENT's
//     term governs the image frame, so this does NOT by itself bound what
//     arrives — the explicit size checks at the gateway and in every worker do.
//   (the return ring and the client egress Records frame are formula terms too.)
//
// Startup refuses (before any side effect) when (a) the operator EXPLICITLY
// configured a max that exceeds the transport maximum, or (b) the effective
// max is below a sane minimum (cannot carry even one record).  The unset
// default otherwise always starts: the effective value is derived down.
// ---------------------------------------------------------------------------

/// Worker ingress ring: largest record batch a Produce WorkerOp can carry
/// (= Worker::maxOpTailBytesFor(workerIngressRingBytes)).
[[nodiscard]] std::size_t workerIngressMaxBatchBytes(const NodeConfig& config) noexcept;

/// Aeron ingress: largest record batch a key-less Produce frame can carry on
/// this broker's term (computeMaxMessageLength(term) − Produce framing).
[[nodiscard]] int32_t aeronIngressMaxBatchBytes(const NodeConfig& config) noexcept;

/// min(workerIngressMaxBatchBytes, aeronIngressMaxBatchBytes).
[[nodiscard]] int32_t ingressHardTransportMaxBytes(const NodeConfig& config) noexcept;

/// Replication: min over BOTH replication transport kinds of what a
/// FetchResponse/push can carry (ReplicationFrameLimits.h).
[[nodiscard]] int32_t replicationHardTransportMaxBytes(const NodeConfig& config) noexcept;

/// Replication fetch/push chunk for USER partitions: min(kDefaultMaxFetchBytes,
/// workerIngressMaxBatchBytes, and — multi-node — replicationHardTransportMaxBytes).
/// Replicated records are forwarded through the worker ingress ring (F3 r5 I1)
/// and a chunk over the replication ceiling is dropped by the transport (F3 r6).
[[nodiscard]] int32_t userTopicMaxFetchBytes(const NodeConfig& config) noexcept;

/// The CONFIGURED max: the operator's EXPLICIT override
/// (config.maxProduceBatchBytes) if set, else ClientGateway::kMaxProduceBatchBytes.
[[nodiscard]] std::size_t configuredMaxProduceBatchBytes(const NodeConfig& config) noexcept;

/// The EFFECTIVE max enforced by the gateway and every worker:
/// NodeConfig::produceIntakeLimit() (THE formula, lowered by an explicit
/// override).
[[nodiscard]] std::size_t effectiveMaxProduceBatchBytes(const NodeConfig& config) noexcept;

/// A record batch must at least hold its fixed header; an effective max at or
/// below this cannot carry a single record → startup refusal (M3).
inline constexpr std::size_t kMinSaneMaxProduceBatchBytes =
    static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) + 1;

/// Startup refusal when the EFFECTIVE max is below kMinSaneMaxProduceBatchBytes
/// (e.g. tiny rings/term) — names the numbers; never silently falls back.
[[nodiscard]] std::optional<std::string> validateEffectiveMaxProduceBatch(const NodeConfig& config);

/// Startup refusal ONLY when an EXPLICIT max exceeds the hard maximum it
/// would be capped to.  nullopt for the unset default.
[[nodiscard]] std::optional<std::string> validateExplicitMaxProduceBatch(const NodeConfig& config);

// ---------------------------------------------------------------------------
// NullSink — no-op outbound transport (sufficient for 1-node; BR-3 replaces)
// ---------------------------------------------------------------------------
using NullSink = rune::consensus::NullSink;   // moved to rune/consensus/NullSink.h (Task 11 fix 2)

// ---------------------------------------------------------------------------
// AdminWorkerAgent — wraps AdminWorker + its cross-thread SPSC rings into an
// Agent so it can be driven by an AgentRunner on a dedicated thread.
//
// Shared-nothing (Rule #1): AdminWorkerAgent owns the AdminWorker (which SOLELY
// owns the __topic_schemas replica).  Every cross-thread hop is an SPSC ring
// (Rule #2, Aeron-style):
//
//   requestRing_ (gateway/control → admin) : DefineTopic / GetSchema ops.
//   replyRing_   (control → admin)          : authoritative topicId back.
//   roleRing_    (control → admin)           : Follower/Leader role events.
//   createReqRing_ (admin → control)         : "create this physical topic".
//
// The dead unsynchronized linked-list enqueue path (Task 6) is REMOVED.
// ---------------------------------------------------------------------------
class AdminWorkerAgent final : public rune::consensus::Agent,
                               public rune::gateway::IAdminRequestSink {
public:
    AdminWorkerAgent(SchemaRegistry& reg,
                     rune::replication::PartitionReplica* replica,
                     int32_t selfId,
                     std::vector<int32_t> replicaSet,
                     AdminDeps deps)
        : worker_(reg, replica, selfId, std::move(replicaSet), std::move(deps))
        , requestRing_(kAdminRequestRingBytes)
        , replyRing_(kCreateTopicRingBytes)
        , roleRing_(kAdminRoleRingBytes) {}

    const char* name() const noexcept override { return "rune-admin"; }

    // ── IAdminRequestSink (gateway → admin ring adapter; RULING-2 F) ──────────
    // Called on the gateway/control thread; encodes onto requestRing_ (SPSC).
    bool submitDefineTopic(int64_t sessionId, int64_t correlationId,
                           std::string_view name, uint32_t partitionCount,
                           std::string_view keyField, bool validate,
                           const uint8_t* descriptorBlob,
                           uint32_t descriptorLen) override {
        DefineTopicMsg m;
        m.session        = sessionId;
        m.corr           = correlationId;
        m.partitionCount = static_cast<int32_t>(partitionCount);
        m.validate       = validate;
        m.name.assign(name);
        m.keyField.assign(keyField);
        if (descriptorBlob && descriptorLen)
            m.blob.assign(descriptorBlob, descriptorBlob + descriptorLen);
        return encodeDefineTopic(requestRing_, m);
    }

    bool submitGetSchema(int64_t sessionId, int64_t correlationId,
                         std::string_view name) override {
        GetSchemaMsg m;
        m.session = sessionId;
        m.corr    = correlationId;
        m.name.assign(name);
        return encodeGetSchema(requestRing_, m);
    }

    int doWork(int64_t nowNs) noexcept override {
        int work = 0;

        // 1. Drain role events (control → admin) FIRST so leadership is current
        //    before any DDL runs this tick.
        work += roleRing_.drain([this](int32_t /*tid*/,
                                       aeron::concurrent::AtomicBuffer& b,
                                       aeron::util::index_t off,
                                       aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
            worker_.onRole(decodeAdminRole(p, static_cast<std::size_t>(len)));
        }, 16);

        // 2. Drain create-topic replies (control → admin).
        work += replyRing_.drain([this](int32_t /*tid*/,
                                        aeron::concurrent::AtomicBuffer& b,
                                        aeron::util::index_t off,
                                        aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
            worker_.onCreateTopicReply(decodeCreateTopicReply(p, static_cast<std::size_t>(len)));
        }, 16);

        // 3. Drain gateway requests (DefineTopic / GetSchema).
        work += requestRing_.drain([this](int32_t tid,
                                          aeron::concurrent::AtomicBuffer& b,
                                          aeron::util::index_t off,
                                          aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
            const auto  n = static_cast<std::size_t>(len);
            if (tid == kMsgDefineTopic) {
                worker_.submitDefineTopic(decodeDefineTopic(p, n));
            } else if (tid == kMsgGetSchema) {
                worker_.submitGetSchema(decodeGetSchema(p, n));
            }
        }, 16);

        // 4. Advance the DDL state machine + drive the replica.
        work += worker_.doWork(nowNs);

        // 4b. F3 round 6, item 3: retry SchemaUpdates the admin→control ring
        //     could not take (full) — retry-on-full, never a drop.
        if (!schemaBacklog_.empty()) [[unlikely]] work += flushSchemaBacklog_();

        // 5. fsyncgate (Task 10 fix 3 / I1): the schema log this thread drives
        //    failed an fsync → hand it to the control thread (ring, no shared
        //    state), which halts the broker.  Signalled once (retried while the
        //    ring is full).
        if (!storageFailureSignalled_) {
            auto* r = worker_.replica();
            if (r && r->isDiskBacked() && r->log()->storageFailed()) [[unlikely]] {
                // M3 (Task 10 round 4): the ring's max message is capacity/8;
                // an over-long path would make write() THROW on this thread.
                // Keep the path's TAIL (the most specific part) behind "...".
                const std::string& dir = r->log()->dir();
                const std::size_t maxDir =
                    static_cast<std::size_t>(failureRing_.maxMsgLength()) - 4u;
                std::string shown = dir;
                if (shown.size() > maxDir) shown = "..." + dir.substr(dir.size() - (maxDir - 3u));
                std::vector<uint8_t> msg(4 + shown.size());
                const int32_t e = r->log()->storageErrno();
                std::memcpy(msg.data(), &e, 4);
                std::memcpy(msg.data() + 4, shown.data(), shown.size());   // M3: tail-truncated
                if (failureRing_.write(msg.data(), msg.size(), kA2cStorageFailure))   // F3 r6: typed
                    storageFailureSignalled_ = true;
            }
        }
        return work;
    }

    /// admin → control ring (T10; F3 r6 extended): kA2cStorageFailure
    /// ([errno:i32][dir]) and kA2cSchemaUpdate ([topicId:i32][blob]).
    OneToOneByteRing& failureRing() noexcept { return failureRing_; }

    /// F3 round 6, item 3 — ADMIN thread only (called from deps.pushValidators
    /// inside AdminWorker::doWork).  The admin thread must NEVER enqueue into a
    /// worker's SPSC ingress ring (the control thread is its single producer):
    /// post the update on the admin→control ring; the control thread fans it
    /// out (SchemaFanout).  Ring full → admin-owned backlog, retried every
    /// doWork.  A descriptor that can never fit the ring is refused loudly.
    void postSchemaUpdate(int32_t topicId, const std::vector<uint8_t>& blob) {
        // Unreachable by construction (static_assert in AdminChannels.h: every
        // descriptor the request ring can deliver fits one SchemaUpdate); kept
        // as a loud backstop rather than a silent tryClaim throw.
        if (schemaUpdateWireLen(blob.size()) > static_cast<std::size_t>(failureRing_.maxMsgLength()))
            [[unlikely]] {
            spdlog::error("AdminWorkerAgent: SchemaUpdate for topicId={} ({} B descriptor) "
                          "exceeds the admin→control ring max message ({} B); validator "
                          "NOT pushed", topicId, blob.size(), failureRing_.maxMsgLength());
            return;
        }
        if (!schemaBacklog_.empty() || !encodeSchemaUpdate(failureRing_, topicId, blob))
            schemaBacklog_.push_back(SchemaUpdateMsg{topicId, blob});   // keep FIFO order
    }
    [[nodiscard]] std::size_t schemaBacklogForTest() const noexcept { return schemaBacklog_.size(); }
    int flushSchemaBacklogForTest() { return flushSchemaBacklog_(); }

    // -----------------------------------------------------------------------
    // Producer-side accessors — the gateway/control thread enqueues here.
    // requestRing_ producer = gateway/control duty-cycle thread.
    // replyRing_ / roleRing_ producer = control thread.
    // createReqRing_ consumer = control thread; producer = admin (via deps).
    // -----------------------------------------------------------------------
    OneToOneByteRing& requestRing()   noexcept { return requestRing_; }
    OneToOneByteRing& replyRing()     noexcept { return replyRing_;   }
    OneToOneByteRing& roleRing()      noexcept { return roleRing_;    }
    OneToOneByteRing& createReqRing() noexcept { return createReqRing_; }
    OneToOneByteRing& egressRing()    noexcept { return egressRing_;   } // admin → control (response frames)

    AdminWorker& worker() noexcept { return worker_; }

private:
    AdminWorker      worker_;
    OneToOneByteRing requestRing_;     // gateway/control → admin (DefineTopic/GetSchema)
    OneToOneByteRing replyRing_;       // control → admin (createTopic reply)
    OneToOneByteRing roleRing_;        // control → admin (role events)
    OneToOneByteRing createReqRing_{kCreateTopicRingBytes}; // admin → control (createTopic req)
    OneToOneByteRing egressRing_{kAdminRequestRingBytes};   // admin → control (response frames: [sessionId:i64][frame])
    // admin → control (T10 storage failure + F3 r6 SchemaUpdate).  Sized like
    // the other blob-carrying admin rings so a descriptor fits one message.
    OneToOneByteRing failureRing_{kAdminToControlRingBytes};
    bool             storageFailureSignalled_ = false;
    std::vector<SchemaUpdateMsg> schemaBacklog_;   // admin-thread-owned (ring-full retry)

    int flushSchemaBacklog_() {
        std::size_t sent = 0;
        while (sent < schemaBacklog_.size() &&
               encodeSchemaUpdate(failureRing_, schemaBacklog_[sent].topicId, schemaBacklog_[sent].blob))
            ++sent;
        schemaBacklog_.erase(schemaBacklog_.begin(),
                             schemaBacklog_.begin() + static_cast<std::ptrdiff_t>(sent));
        return static_cast<int>(sent);
    }
};

// ---------------------------------------------------------------------------
// BrokerNode
// ---------------------------------------------------------------------------
class BrokerNode {
public:
    explicit BrokerNode(NodeConfig config);
    ~BrokerNode();

    // Non-copyable, non-movable (owns threads + Aeron state)
    BrokerNode(const BrokerNode&)            = delete;
    BrokerNode& operator=(const BrokerNode&) = delete;

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// Create dataDir if missing; start the embedded driver + Aeron client;
    /// launch the AgentRunner thread.
    void start();

    /// Bounded wait for this node to reach LEADER_READY or CLOSED.
    /// Returns true if leadership reached within timeout, false otherwise.
    bool awaitLeader(std::chrono::milliseconds timeout);

    /// Signal the agent thread to stop, join it, then close the Aeron client
    /// and embedded media driver.  Idempotent.
    void stop();

    // -----------------------------------------------------------------------
    // fsyncgate fail-stop (Task 10 fix 1; Kafka LogDirFailure / PostgreSQL
    // data_sync_retry=off).  When any log's fsync fails, the runtime logs
    // critical and HALTS the broker with kExitStorageFailure.  The process
    // owner (main) installs a HaltHandler before start(): it is called ONCE on
    // the runtime thread and must stop the node through the normal stop path
    // (from another thread) and exit with exitCode.  Without a handler the
    // node stops itself on a helper thread and _Exit()s with the code.
    // Restart recovery truncates the torn tail; replication re-heals it.
    // -----------------------------------------------------------------------
    static constexpr int kExitStorageFailure = 74;   // EX_IOERR
    static constexpr int kExitInvariantViolation = 70;   // EX_SOFTWARE (F7 ownership invariant)
    static_assert(kExitInvariantViolation == Worker::kExitInvariantViolation,
                  "a worker-thread invariant halt (F15 §6) exits with the same code");
    using HaltHandler = void (*)(void* ctx, int exitCode);
    void setHaltHandler(HaltHandler h, void* ctx) noexcept { haltHandler_ = h; haltCtx_ = ctx; }
    /// Test-only: drive the runtime's storage-failure path directly.
    void raiseStorageFailureForTest(const std::string& logDir, int err) { onStorageFailure_(logDir, err); }
    /// Test-only: during start(), re-run one user log's recovery with this msync
    /// errno injected (exercises the refuse-to-start path).
    void setRecoveryFaultForTest(int err) noexcept { recoveryFaultForTest_ = err; }
    /// Test-only: during start(), re-run the SCHEMA log's recovery with this msync errno.
    void setSchemaRecoveryFaultForTest(int err) noexcept { schemaRecoveryFaultForTest_ = err; }

    // -----------------------------------------------------------------------
    // Observers (thread-safe: atomic reads)
    // -----------------------------------------------------------------------

    bool   isLeader()  const noexcept;
    rune::consensus::NodeId leaderId() const noexcept;

    /// Return the current leadershipTermId from the election state.
    /// Observer-thread-safe: reads the atomic stateCode to decide the path,
    /// then reads the duty-cycle-written term from the Election.
    /// Used by main.cpp to annotate leadership log messages with the term so
    /// the DR-5 oracle can distinguish stale pre-election announcements from
    /// current-term announcements (term-aware assert_single_leader).
    rune::consensus::Term leadershipTermId() const noexcept;

    /// Publish this node's current role/term/leaderId into the Aeron node-role
    /// counter (RoleCounter.h) in the driver's CnC file.  Idempotent + cheap:
    /// call it from the main poll loop on every tick.  No-op until the driver
    /// has acked the counter allocation (findCounter returns the live Counter).
    /// Observer-thread-safe: reads the same atomic leaderId()/term accessors the
    /// leadership log uses; the counter is the machine-readable signal the
    /// dr-e2e harness reads (via rune_role) instead of scraping logs.
    void publishRole() noexcept;

    /// Return the leader's client Aeron channel for data-op redirect (GB-4).
    /// Observer-thread-safe: reads atomic leaderId() + immutable peer config.
    /// - If this node IS the leader → returns empty string (no self-redirect).
    /// - If leader id is known and that peer has a clientPort → returns its
    ///   clientChannel() ("aeron:udp?endpoint=host:port").
    /// - Otherwise (unknown leader, or leader peer has no clientPort) → empty.
    std::string leaderClientEndpoint() const noexcept;

    // -----------------------------------------------------------------------
    // F1: TEST-ONLY owner-thread hook.
    //
    // Replica / Log state is owned by exactly one thread (a worker for user
    // partitions when a WorkerPool exists; the control thread for internal
    // partitions and for pool-less nodes).  Tests MUST NOT touch it from the
    // test thread — the former replicaForTest()/topicRegistryForTest() raw
    // accessors were REMOVED because tests used them to append()/read LEO/HW/ISR
    // cross-thread (append() offers on the owner's single-writer
    // ExclusivePublication → corrupted/lost replication frames).
    //
    // onOwnerThreadForTest(topicId, partition, f) runs `f` on the replica's TRUE
    // owner thread for this node's configuration and returns its result (or
    // rethrows its exception) to the test thread:
    //   f(PartitionReplica&)  — missing replica = std::runtime_error.
    //   f(PartitionReplica*)  — nullptr passed through (tolerant polling).
    // Routing: test thread → control thread (SPSC test-task ring) → [user
    // partition + WorkerPool] owning worker via the existing control→worker
    // ingress ring (WorkerOp::TestTask).  See OwnerThreadTestTask.h.
    //
    // Timeout: if the owner has not STARTED the task within `timeout`, it is
    // cancelled (never runs) and std::runtime_error is thrown.
    // Preconditions (fail loudly with std::logic_error / invalid_argument):
    //   node started and not stopped; topicId != kSchemaTopicId (-4, the
    //   __topic_schemas replica is AdminWorker-owned — not routable here).
    // Callers may be on any test thread; test-side producers are serialised by
    // testHookMu_ (touched ONLY by test threads — never by a node thread).
    // -----------------------------------------------------------------------
    template <class F>
    auto onOwnerThreadForTest(int32_t topicId, int32_t partition, F&& f,
                              std::chrono::milliseconds timeout = std::chrono::seconds{10}) {
        if (topicId == kSchemaTopicId) {
            throw std::invalid_argument(
                "onOwnerThreadForTest: __topic_schemas (topicId=-4) is AdminWorker-owned; "
                "not routable via the owner-thread hook");
        }
        return submitOwnerThreadTestTask_<OwnerThreadTestTask::Target::Replica>(
            topicId, partition, std::forward<F>(f), timeout);
    }

    /// F1: run `f()` on the CONTROL (NodeRuntimeAgent) thread — for test reads of
    /// control-thread-owned state (e.g. ClientGateway::pendingAckCount()).
    template <class F>
    auto onControlThreadForTest(F&& f,
                                std::chrono::milliseconds timeout = std::chrono::seconds{10}) {
        return submitOwnerThreadTestTask_<OwnerThreadTestTask::Target::ControlThread>(
            -1, -1, std::forward<F>(f), timeout);
    }

    /// Observer-thread-safe: true if the replication metadata bridge has
    /// propagated this node's leadership to all its PartitionReplicas.
    /// Uses the atomic mirror in ReplicationMetadataBridge.
    bool replicationIsLeaderForTest() const noexcept {
        return replBridge_ && replBridge_->isLeaderAtomic();
    }

    /// Observer-thread-safe: the last epoch applied by the replication bridge.
    /// Uses the atomic mirror in ReplicationMetadataBridge.
    rune::replication::Epoch replicationEpochForTest() const noexcept {
        if (!replBridge_) return 0;
        return replBridge_->lastEpochAtomic();
    }

    /// Set the minimum ISR size required for acks=All produces on this node's gateway.
    /// Thread-safe (gateway::ClientGateway::setMinIsr uses std::atomic<int>).
    /// Typically called from tests to configure min-ISR before or after start().
    void setGatewayMinIsr(int minIsr) noexcept {
        if (gateway_) gateway_->setMinIsr(minIsr);
    }

    /// Return this node's ClientGateway (or nullptr).  Test-only: reaches into
    /// duty-cycle-owned state (e.g. pendingAckCount()); call only from a
    /// synchronized test context that knows the duty cycle is between doWork
    /// calls (or with the node stopped).  Used by GR-9 to observe the
    /// deferred-ack queue depth for the acks=All HW-gate boundary test.
    rune::gateway::ClientGateway* gatewayForTest() const noexcept {
        return gateway_.get();
    }

    /// Test-only: the node-level embedded driver (spec §3.6 one-pumper counters).
    EmbeddedDriver* driverForTest() const noexcept { return nodeDriver_.get(); }

    /// Test-only: does the runtime have a thread of its own?  `thread` placement:
    /// true while running; `fused` placement (spec §3.2): always false — the
    /// runtime duty cycle runs inside worker 0's pass.
    [[nodiscard]] bool runtimeThreadStartedForTest() const noexcept {
        return agentRunner_ && agentRunner_->isRunning();
    }

    /// Test/bench-only: override the runtime's slow-tick interval (ns; 0 = every
    /// cycle).  Set before start().  Default: placement decides (fused 10 ms).
    void setSlowTickIntervalForTest(int64_t ns) noexcept { slowTickIntervalOverrideNs_ = ns; }

    /// Test-only: the concrete consensus agent the runtime drives (never Generic).
    [[nodiscard]] NodeRuntimeAgent::ConsensusKind runtimeConsensusKindForTest() const noexcept {
        return runtimeAgent_ ? runtimeAgent_->consensusKind() : NodeRuntimeAgent::ConsensusKind::Generic;
    }
    /// Test-only: every runtime-cycle call is concrete (consensus agent, consensus
    /// publisher + sink, internal-replica transports) — no virtual fallback taken.
    [[nodiscard]] bool runtimePathIsConcreteForTest() const noexcept { return runtimePathIsConcrete_(); }

    /// Test-only, CONTROL thread only (call inside onControlThreadForTest):
    /// runtime duty cycles run / routing-image publishes so far.
    [[nodiscard]] uint64_t runtimeCyclesForTest() const noexcept {
        return runtimeAgent_ ? runtimeAgent_->cyclesForTest() : 0;
    }
    [[nodiscard]] uint64_t routingImagePublishesForTest() const noexcept {
        return runtimeAgent_ ? runtimeAgent_->routingImagePublishesForTest() : 0;
    }

    /// Test-only: return the MetadataService (for assignment() queries in tests).
    /// Non-null after start() if metadata service construction succeeded.
    /// NOT safe for concurrent use — call only when duty cycle is between
    /// doWork calls or when the node is stopped.
    MetadataService* metadataServiceForTest() const noexcept {
        return metadataService_.get();
    }

    /// Test-only: return the SchemaRegistry (for byName/byId queries in tests).
    /// Non-null after start() if schema persistence construction succeeded.
    /// NOT safe for concurrent use — call only when admin thread is quiesced or
    /// the node is stopped.
    SchemaRegistry* schemaRegistryForTest() const noexcept {
        return schemaRegistry_.get();
    }

    /// Test-only: return raw pointer to the __topic_schemas Log (for record-count
    /// assertions in SchemaPersistenceTest).
    rune::storage::Log* schemaLogForTest() const noexcept {
        return schemaLog_.get();
    }

    /// Test-only: return the AdminWorkerAgent (for direct onDefineTopic calls).
    /// Used by SchemaPersistenceTest to drive the AdminWorker without going
    /// through the gateway (Task 8).
    AdminWorkerAgent* adminWorkerAgentForTest() const noexcept {
        return adminWorkerAgent_.get();
    }

    /// Test-only: return the I/O throttle rate wired into the LogCleanerAgent.
    /// -1 = unlimited.  Valid after start().  Used by CP-10 Fix-1 wiring test.
    [[nodiscard]] int64_t cleanerThrottleRateForTest() const noexcept {
        if (!cleanerAgent_) return -2; // -2 = no cleaner (sentinel for test)
        return cleanerAgent_->throttleRateForTest();
    }

    /// Test-only: worker id owning (topicId, partition) per the WorkerPool router.
    /// Returns -1 if workerPool_ is null (no topics configured or E1 not wired).
    /// NOT concurrency-safe; call between doWork calls or when the node is stopped.
    /// Used by WorkerPoolParityTest to assert co-group affinity and N>1 worker usage.
    [[nodiscard]] int32_t workerForPartitionForTest(int32_t topicId,
                                                    int32_t partition) const noexcept {
        return workerPool_ ? workerPool_->ownerWorkerId(topicId, partition) : -1;
    }

    /// read-rework T3/T4 test-only: the per-worker dynamic-MDC egress publications
    /// created by buildWorkerPool (empty unless perWorkerIngressEnabled && aeron_).
    /// Lets a test assert per-worker egress creation + stream-id assignment.
    [[nodiscard]] const std::vector<std::unique_ptr<WorkerEgress>>&
    workerEgressesForTest() const noexcept {
        return workerEgresses_;
    }

    /// E5 Task 5 test-only: number of per-worker replication planes constructed
    /// in buildReplication() (0 when flag off or no peers).
    [[nodiscard]] std::size_t perWorkerPlanesConstructedForTest() const noexcept {
        return perWorkerReplTransports_.size();
    }

    /// Self-contained worker shards (spec §3.1): number of per-worker invoker
    /// drivers built (0 in legacy mode).  Startup/stop-time state (no worker access).
    [[nodiscard]] std::size_t shardDriverCountForTest() const noexcept { return shardDrivers_.size(); }
    /// Worker w (owned by its own thread — touch only inside onOwnerThreadForTest).
    [[nodiscard]] Worker* workerForTest(int32_t w) const noexcept {
        return workerPool_ && w >= 0 && w < workerPool_->workerCount() ? &workerPool_->worker(w) : nullptr;
    }

    /// Test-only fault injection: called (on the start() thread) at named startup
    /// phases ("build-gateway" = offset/txn/metadata rebuilds + admin build).
    /// Set before start().
    void setStartupPhaseHookForTest(std::function<void(std::string_view)> hook) {
        startupPhaseHookForTest_ = std::move(hook);
    }

    /// Test-only fault injection: while attaching shard `worker` at startup, run a
    /// resolve that cannot complete for `stall` (pumping like a slow real resolve).
    /// Set before start().
    void setShardResolveStallForTest(int32_t worker, std::chrono::milliseconds stall) noexcept {
        shardResolveStallWorkerForTest_ = worker;
        shardResolveStallForTest_       = stall;
    }

    /// Test-only: is worker w's shard Aeron client closed?  Call ONLY inside
    /// onOwnerThreadForTest on worker w (the client is worker-owned).
    [[nodiscard]] bool shardClientClosedForTest(int32_t w) const {
        return shardClients_.at(static_cast<std::size_t>(w))->isClosed();
    }

    /// Test-only: worker w's replication plane (nullptr if none).  The vector is
    /// startup-immutable; the transport's STATE is worker-owned — read it only
    /// inside onOwnerThreadForTest on that worker.
    [[nodiscard]] WorkerReplicationTransport* perWorkerPlaneForTest(int32_t w) const noexcept {
        return (w >= 0 && static_cast<std::size_t>(w) < perWorkerReplTransports_.size())
                   ? perWorkerReplTransports_[static_cast<std::size_t>(w)].get() : nullptr;
    }

private:
    // F1: test-side submit + await (see onOwnerThreadForTest).  Runs on the TEST
    // thread only; the node's threads only ever see the ring-carried ref.
    template <OwnerThreadTestTask::Target T, class F>
    auto submitOwnerThreadTestTask_(int32_t topicId, int32_t partition, F&& f,
                                    std::chrono::milliseconds timeout) {
        using Impl = detail::OwnerThreadTestTaskImpl<std::decay_t<F>, T>;
        if (!started_.load(std::memory_order_acquire) ||
            stopped_.load(std::memory_order_acquire) || !runtimeAgent_) {
            throw std::logic_error(
                "onOwnerThreadForTest: node is not running (start() not called, or "
                "already stopped) — no owner thread to run the task on");
        }
        auto task = std::make_shared<Impl>(topicId, partition, std::forward<F>(f));
        auto fut  = task->future();
        auto* ref = new OwnerThreadTestTaskRef(task);   // the ring's reference
        {
            std::lock_guard<std::mutex> lk(testHookMu_);
            if (!runtimeAgent_->offerOwnerThreadTestTask(ref)) {
                delete ref;
                throw std::runtime_error("onOwnerThreadForTest: control test-task ring full");
            }
        }
        return awaitOwnerThreadTestTask(*task, fut, timeout);
    }

    // F1: serialises TEST-thread producers onto the SPSC test-task ring.  Never
    // touched by any node thread (not on any production path).
    std::mutex testHookMu_;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------
    void startEmbeddedDriver();
    void stopEmbeddedDriver() noexcept;
    [[nodiscard]] bool runtimePathIsConcrete_() const noexcept;   // Task 11 fix 2 (review m4)
    void stopStartupPump_() noexcept;   // fused: join the interim runtime-driver pump (idempotent)
    void buildConsensusObjects();
    void buildSingleNodeConsensus();
    void buildMultiNodeConsensus();
    void buildReplication();   // GR-2/GR-3: replicas + transport + bridge
    void buildGateway();
    void buildWorkerPool();    // E1: shard→worker assignment (must call after replicaRegistry_ is fully built)
    void buildAdminWorker();   // Task 6: __topic_schemas log/replica + AdminWorker thread

    // Task 8b (RULING-4, SINGLE-NODE): create + register the user
    // PartitionReplica(s) for a RUNTIME-defined topic (created via
    // DefineTopic→AdminWorker→control-hook createTopic).  Mirrors the boot
    // buildReplication() user-topic path: reuses the dirs/log created by
    // TopicRegistry::createTopic (Task 5), constructs one PartitionReplica per
    // partition (Follower / epoch 0), registers each in replicaRegistry_ +
    // replTransport_ (which bumps the registry generation → WorkerPool::reconcile
    // assigns it next control cycle), and registers the topic with the
    // MetadataService so its assignment is emitted and the bridge promotes the
    // replica to Leader.  Control-thread only (same thread that owns these
    // registries — no cross-thread mutation).  Returns the number of partitions
    // registered.  Multi-node partition placement is OUT OF SCOPE (deferred).
    int32_t createRuntimeUserReplicas(const TopicSpec& spec);

    // -----------------------------------------------------------------------
    // Configuration (immutable after construction)
    // -----------------------------------------------------------------------
    NodeConfig config_;

    // -----------------------------------------------------------------------
    // Consensus objects (constructed in start())
    // -----------------------------------------------------------------------

    // Durable persistence (paths under config_.dataDir)
    std::unique_ptr<rune::consensus::NodeStateFile> nodeStateFile_;
    std::unique_ptr<rune::consensus::TermLog>       termLog_;

    // 1-node path: NullSink (BR-2)
    NullSink nullSink_;
    std::unique_ptr<rune::consensus::SbeConsensusPublisher> publisher_;

    // N-node path: UDP sink + ingress (BR-3)
    std::unique_ptr<UdpMessageSink>             udpSink_;
    std::unique_ptr<rune::consensus::AeronIngress> aeronIngress_;
    // Composite agent wrapping ingress poll + ConsensusAgent (multi-node only)
    std::unique_ptr<NodeConsensusAgent>         nodeAgent_;

    // Cluster membership
    std::vector<rune::consensus::ClusterMember> clusterMembers_;

    // Core consensus state machine (constructed last; holds references)
    std::unique_ptr<rune::consensus::IngressRing>    ingressRing_;
    std::unique_ptr<rune::consensus::Election>       election_;
    std::unique_ptr<rune::consensus::ConsensusAgent> agent_;

    // -----------------------------------------------------------------------
    // Topic registry (GB-1) — per-partition Logs, bootstrapped from RUNE_TOPICS.
    // Constructed in start() after dataDir creation; outlives the gateway.
    // Logs are single-writer: accessed only on the AgentRunner duty-cycle thread.
    // -----------------------------------------------------------------------
    std::unique_ptr<rune::gateway::TopicRegistry> topicRegistry_;

    // -----------------------------------------------------------------------
    // Replication (GR-2/GR-3) — per-partition PartitionReplica + transport.
    // Constructed in buildGateway() after aeron_ and topicRegistry_ are live.
    // All replication state is single-writer (AgentRunner duty-cycle thread).
    //
    // Destruction order in stop() (reverse of construction):
    //   runtimeAgent_ → gateway_ → replicaRegistry_ → replTransport_
    //   → replBridge_ → topicRegistry_
    //
    // memoryLogs_ is declared BEFORE replicaRegistry_ so that in the implicit
    // destructor (members destroyed in reverse declaration order) the replicas
    // are destroyed BEFORE the InMemoryLog instances they point at, preventing
    // dangling-ILog* accesses on the destructor-only teardown path.
    // -----------------------------------------------------------------------
    // InMemoryLog instances for memory-backed user-topic partitions (Task M1).
    // Owned here so their lifetime exceeds replicaRegistry_ (see ordering note).
    std::vector<std::unique_ptr<rune::storage::InMemoryLog>> memoryLogs_;
    std::unique_ptr<ReplicaRegistry>                        replicaRegistry_;
    std::unique_ptr<rune::replication::AeronPartitionTransport> replTransport_;
    std::unique_ptr<ReplicationMetadataBridge>              replBridge_;
    // Per-replica no-op NullPartitionTransport instances used when replTransport_ is null.
    // Prevents PartitionReplica assert(transport!=nullptr) in single-node / no-repl-port nodes.
    std::vector<std::unique_ptr<rune::replication::NullPartitionTransport>> nullTransports_;

    // -----------------------------------------------------------------------
    // Client gateway (BR-7a) — runs inside the duty-cycle via NodeRuntimeAgent.
    // Constructed after aeron_ in start(); destroyed in stop() before aeron_.
    // -----------------------------------------------------------------------
    std::unique_ptr<rune::gateway::ClientGateway> gateway_;

    // -----------------------------------------------------------------------
    // Consumer-group coordination (CG-5 wiring — created in buildGateway()).
    // offsetLog_     — durable Log for __consumer_offsets (one partition).
    // offsetReplica_ — PartitionReplica wrapping the offset Log (single-node
    //                  with InMemoryTransport; role transitions handled by bridge).
    // offsetStore_   — ReplicatedOffsetStore for committed-offset persistence.
    // groupCoordService_ — GroupCoordinationService (dispatches JoinGroup etc.).
    //
    // Destruction order (reverse of construction, in stop()):
    //   runtimeAgent_ → groupCoordService_ → offsetStore_ → offsetReplica_
    //   → offsetLog_ → gateway_ → …
    // -----------------------------------------------------------------------
    std::unique_ptr<rune::storage::Log>                        offsetLog_;
    std::unique_ptr<rune::replication::PartitionReplica>       offsetReplica_;
    std::unique_ptr<rune::coordination::ReplicatedOffsetStore> offsetStore_;
    std::unique_ptr<GroupCoordinationService>                  groupCoordService_;

    // -----------------------------------------------------------------------
    // Transaction coordination (TX-3 wiring — created in buildGateway()).
    // txnLog_          — durable Log for __transaction_state (one partition).
    // txnReplica_      — PartitionReplica wrapping the txn Log (majority-durable).
    // txnState_        — ReplicatedTxnStateStore for transaction metadata.
    // txnCoordService_ — TransactionCoordinationService (dispatches InitProducerId etc.).
    //
    // Destruction order (reverse of construction, in stop()):
    //   runtimeAgent_ → txnCoordService_ → txnState_ → txnReplica_ (in registry)
    //   → txnLog_ → …
    // -----------------------------------------------------------------------
    std::unique_ptr<rune::storage::Log>                             txnLog_;
    std::unique_ptr<rune::replication::PartitionReplica>            txnReplica_;
    std::unique_ptr<rune::transaction::ReplicatedTxnStateStore>     txnState_;
    std::unique_ptr<TransactionCoordinationService>                 txnCoordService_;

    // -----------------------------------------------------------------------
    // Cluster metadata (MD1 wiring — created in buildGateway()).
    // metadataLog_     — durable Log for __cluster_metadata (one partition).
    // metadataReplica_ — PartitionReplica wrapping the metadata Log.
    //                    Registered in ReplicaRegistry so the bridge drives
    //                    its leader/follower role from cluster leadership
    //                    (FINDING-1: DO NOT follow the assignment map).
    // metadataService_ — MetadataService: applies committed records on doWork,
    //                    exposes assignment(topicId, partition).
    //
    // topicId=-3 is the sentinel for __cluster_metadata (offsets=-1, txn=-2).
    //
    // Destruction order (reverse of construction, in stop()):
    //   runtimeAgent_ → metadataService_ → metadataReplica_ (in registry)
    //   → metadataLog_ → …
    // -----------------------------------------------------------------------
    std::unique_ptr<rune::storage::Log>                   metadataLog_;
    std::unique_ptr<rune::replication::PartitionReplica>  metadataReplica_;
    std::unique_ptr<MetadataService>                      metadataService_;

    // -----------------------------------------------------------------------
    // Schema registry (Task 6 — __topic_schemas persistence).
    // schemaLog_         — durable Log for __topic_schemas (one partition).
    // schemaReplica_     — PartitionReplica wrapping the schema Log.
    //                      topicId=-4 (kSchemaTopicId), cleanupPolicy=Compact.
    //                      Majority-durable (minIsr=2) like __transaction_state.
    //                      Registered in replicaRegistry_ so the bridge drives
    //                      its leader/follower role from cluster leadership.
    // schemaRegistry_    — In-memory schema registry (owned by admin thread).
    //                      Rebuilt from schemaLog_ on startup.
    // adminWorkerAgent_  — AdminWorkerAgent running on adminWorkerRunner_ thread.
    //                      Owns the SchemaRegistry; dispatches DefineTopic/Get ops.
    //
    // Destruction order (reverse of construction, in stop()):
    //   adminWorkerRunner_ → adminWorkerAgent_ → schemaRegistry_
    //   → schemaReplica_ (in registry) → schemaLog_ → …
    // -----------------------------------------------------------------------
    std::unique_ptr<rune::storage::Log>                   schemaLog_;
    std::unique_ptr<rune::replication::PartitionReplica>  schemaReplica_;
    std::unique_ptr<SchemaRegistry>                       schemaRegistry_;
    std::unique_ptr<AdminWorkerAgent>                     adminWorkerAgent_;
    // Runtime-cycle DDL control plane (direct call; see AdminControlPlane.h).
    friend class AdminControlPlane;
    int serviceAdminControl_();
    AdminControlPlane                                     adminControl_{*this};
    int64_t                                               slowTickIntervalOverrideNs_ = -1;
    // Task 8a: last leadership state posted to the admin role ring (control thread).
    bool                                                  adminLastLeader_ = false;
    int32_t                                               adminLastEpoch_  = -1;
    rune::consensus::BackoffIdleStrategy                  adminWorkerIdleStrategy_;
    rune::consensus::MonotonicNanoClock                   adminWorkerClock_;
    std::unique_ptr<rune::consensus::AgentRunner>         adminWorkerRunner_;

    // -----------------------------------------------------------------------
    // E1: WorkerPool — shard→worker ownership map (built after replicaRegistry_).
    // Destroyed AFTER runtimeAgent_ (which holds a non-owning ptr) and BEFORE
    // replicaRegistry_ (pool holds non-owning replica ptrs; registry owns them).
    // Declaration order enforces this via reverse-destruction in the destructor.
    // -----------------------------------------------------------------------
    std::unique_ptr<WorkerPool> workerPool_;  // E1: shard->worker ownership

    // Self-contained worker shards (spec §3.1).  Index = worker id.  Declared
    // BEFORE the per-worker Aeron resources below so that, even on a destructor
    // path, pubs/subs die before their clients and clients before their drivers.
    // Normally torn down explicitly in stop() (teardownShardRuntimes_) after the
    // worker runners are joined and each invoker driver is handed to the closing
    // thread (join → bindPumpThread → close), before the node driver.
    std::vector<std::unique_ptr<EmbeddedDriver>> shardDrivers_;
    std::vector<std::shared_ptr<aeron::Aeron>>   shardClients_;
    std::vector<std::unique_ptr<ShardAgent>>     shardAgents_;
    // F3 round 6, item 3: control-thread-owned SchemaUpdate fan-out (the
    // control thread is the single producer of every worker ingress ring).
    SchemaFanout                                 schemaFanout_;
    uint64_t                                     adminToControlRejects_ = 0;   // control thread only
    std::function<void(std::string_view)>        startupPhaseHookForTest_;
    HaltHandler                                  haltHandler_ = nullptr;
    void*                                        haltCtx_     = nullptr;
    bool                                         haltRaised_  = false;   // runtime thread only
    void onStorageFailure_(const std::string& logDir, int err);
    void refuseStartOnFailedRecovery_();
    void onInvariantViolation_(const std::string& what);
    int  recoveryFaultForTest_ = 0;
    int  schemaRecoveryFaultForTest_ = 0;
    int32_t                                      shardResolveStallWorkerForTest_ = -1;
    std::chrono::milliseconds                    shardResolveStallForTest_{0};
    void buildShardRuntimes_();
    void attachShardRuntimes_();
    void teardownShardRuntimes_() noexcept;

    // E5.1.3: per-worker Aeron ingress subscriptions.
    // One WorkerIngress per worker, allocated in buildWorkerPool() when
    // perWorkerIngressEnabled is true.  Each WorkerIngress subscribes to
    // (config_.clientEndpoint.channel, workerIngressBaseStreamId + workerId).
    // Owned here; workers hold non-owning raw pointers (via setWorkerIngress).
    // Empty when perWorkerIngressEnabled is false (default).
    std::vector<std::unique_ptr<WorkerIngress>> workerIngresses_;

    // read-rework T3/T4: per-worker dynamic-MDC egress publications.  One per worker
    // when perWorkerIngressEnabled && aeron_.  Owned here; workers hold non-owning
    // raw pointers (via setWorkerEgress).  Empty otherwise.
    std::vector<std::unique_ptr<WorkerEgress>> workerEgresses_;

    // E5 Task 5: per-worker replication transports.
    // One WorkerReplicationTransport per worker when perWorkerReplication && aeron_
    // AND there are replication peers (non-empty peer list).  Owned here; workers
    // hold non-owning pointers (via setReplicationTransport).  Empty otherwise.
    // Worker W uses perWorkerReplTransports_[W] for user (topicId>=0) partitions.
    // Internal partitions (topicId<0) always use the legacy replTransport_.
    std::vector<std::unique_ptr<WorkerReplicationTransport>> perWorkerReplTransports_;

    // Composite agent: wraps inner consensus agent + gateway + replication (single-writer).
    std::unique_ptr<NodeRuntimeAgent> runtimeAgent_;

    // -----------------------------------------------------------------------
    // CP-6: LogCleanerAgent — runs on its own AgentRunner thread (off the
    // duty-cycle / append hot path).  Wired after replicaRegistry_ is built
    // in buildGateway().  Stopped BEFORE runtimeAgent_ in stop() to avoid a
    // race where the cleaner enqueues a swap while the duty cycle has stopped
    // draining.  The cleaner's AgentRunner uses a BackoffIdleStrategy (same
    // as the main runner) but on a separate thread so compaction I/O (fsync)
    // never delays appends.
    // -----------------------------------------------------------------------
    std::unique_ptr<LogCleanerAgent>              cleanerAgent_;
    rune::consensus::BackoffIdleStrategy          cleanerIdleStrategy_;
    rune::consensus::MonotonicNanoClock           cleanerClock_;
    std::unique_ptr<rune::consensus::AgentRunner> cleanerRunner_;

    // -----------------------------------------------------------------------
    // Runtime (AgentRunner + clock + idle)
    // -----------------------------------------------------------------------
    rune::consensus::MonotonicNanoClock clock_;
    // Selected from config_.runtimeIdleStrategy (RUNE_RUNTIME_IDLE): Backoff
    // (default) or BusySpin (sub-100µs produce/ack round-trips; pegs the core).
    std::unique_ptr<rune::consensus::IdleStrategy> idleStrategy_;
    std::unique_ptr<rune::consensus::AgentRunner> agentRunner_;

    // -----------------------------------------------------------------------
    // Aeron embedded driver + client
    // -----------------------------------------------------------------------
    std::unique_ptr<EmbeddedDriver> nodeDriver_;   // node-level driver (runtime / legacy workers)
    // Fused placement only: services the runtime's invoker driver + client from
    // aeron_ connect until worker 0 takes them over (joined at the hand-off).
    std::unique_ptr<DriverPumpAgent>               startupPumpAgent_;
    std::unique_ptr<rune::consensus::IdleStrategy> startupPumpIdle_;
    std::unique_ptr<rune::consensus::AgentRunner>  startupPumpRunner_;

    std::shared_ptr<aeron::Aeron> aeron_;

    // -----------------------------------------------------------------------
    // Aeron node-role counter (RoleCounter.h) — machine-readable leadership.
    // Allocated in start() after aeron_ connects; updated by publishRole().
    // roleCounterRegId_ is the pending registration id until the driver acks
    // (findCounter returns the live Counter, cached in roleCounter_).
    // -----------------------------------------------------------------------
    std::int64_t                     roleCounterRegId_ = -1;
    std::int64_t                     termCounterRegId_ = -1;
    std::shared_ptr<aeron::Counter>  roleCounter_;
    std::shared_ptr<aeron::Counter>  termCounter_;

    // -----------------------------------------------------------------------
    // State flags
    // -----------------------------------------------------------------------
    std::atomic<bool> started_{false};
    std::atomic<bool> stopped_{false};
};

} // namespace rune::node
