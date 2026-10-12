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

#include <cstring>
#include <cassert>
#include <rune/protocol/client/SchemaMessages.h>
#include <cstdlib>
#include <limits>
#include <rune/gateway/SpillFifo.h>
#include <rune/node/NodeConfig.h>
#include <rune/consensus/ControlRing.h>   // Task 9: tlsReloadRing_ (SIGHUP trigger)
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/PeerResolverHub.h>
#include <rune/node/DriverPumpAgent.h>
#include <rune/node/UdpMessageSink.h>
#include <rune/node/NodeConsensusAgent.h>
#include <rune/node/NodeRuntimeAgent.h>
#include <rune/node/LogCleanerAgent.h>
#include <rune/node/SegmentProvisionerAgent.h>
#include <rune/node/ParkUntilWorkIdle.h>
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
#include <rune/node/ViewRegistry.h>
#include <rune/node/AdminWorker.h>
#include <rune/node/AdminChannels.h>
#include <rune/node/AdminControlPlane.h>
#include <rune/node/SchemaFanout.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/ViewSnapshotIoAgent.h>
#include <rune/node/OwnerThreadTestTask.h>
#include <rune/node/LeaderObserver.h>
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/PubAddBroker.h>
#include <rune/replication/InternalIsrKnowledge.h>
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

#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
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
                     AdminDeps deps,
                     rune::replication::PartitionReplica* viewsReplica = nullptr,
                     ViewRegistry* viewRegistry = nullptr)
        : worker_(reg, replica, selfId, std::move(replicaSet), std::move(deps),
                  viewsReplica, viewRegistry)
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
                           uint32_t descriptorLen,
                           uint8_t persistence) override {
        DefineTopicMsg m;
        m.session        = sessionId;
        m.corr           = correlationId;
        m.partitionCount = static_cast<int32_t>(partitionCount);
        m.validate       = validate;
        m.name.assign(name);
        m.keyField.assign(keyField);
        if (descriptorBlob && descriptorLen)
            m.blob.assign(descriptorBlob, descriptorBlob + descriptorLen);
        m.persistence = persistence;
        return encodeDefineTopic(requestRing_, m);
    }

    bool submitGetSchema(int64_t sessionId, int64_t correlationId,
                         std::string_view name,
                         uint64_t requestedFingerprint) override {
        GetSchemaMsg m;
        m.session = sessionId;
        m.corr    = correlationId;
        m.name.assign(name);
        m.requestedFingerprint = requestedFingerprint;
        return encodeGetSchema(requestRing_, m);
    }

    // Task 14: AlterTopic (templateId 68) — gateway → admin ring hand-off,
    // same discipline as submitDefineTopic/submitGetSchema above.
    bool submitAlterTopic(int64_t sessionId, int64_t correlationId,
                          std::string_view topicName,
                          uint16_t expectedVersion,
                          const uint8_t* opsBlob,
                          uint32_t opsLen) override {
        AlterTopicMsg m;
        m.session         = sessionId;
        m.corr            = correlationId;
        m.expectedVersion = expectedVersion;
        m.topicName.assign(topicName);
        if (opsBlob && opsLen)
            m.opsBlob.assign(opsBlob, opsBlob + opsLen);
        return encodeAlterTopic(requestRing_, m);
    }

    // Task 19: CreateView (templateId 70) — gateway → admin ring hand-off, same
    // discipline as the other DDL submits above.
    bool submitCreateView(int64_t sessionId, int64_t correlationId,
                          std::string_view viewName, std::string_view keyField,
                          uint8_t persistence, const uint8_t* planBlob,
                          uint32_t planLen) override {
        CreateViewMsg m;
        m.session     = sessionId;
        m.corr        = correlationId;
        m.persistence = persistence;
        m.viewName.assign(viewName);
        m.keyField.assign(keyField);
        if (planBlob && planLen)
            m.planBlob.assign(planBlob, planBlob + planLen);
        return encodeCreateView(requestRing_, m);
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
        //    Fix round 2: while DDL answers wait in the egress backlog, NO new
        //    request is taken — the request ring fills and the gateway answers
        //    the next DDL Internal (an explicit answer) instead of the backlog
        //    growing.  This bounds the backlog to one tick's answers (<= 16
        //    drained + the in-flight op), far below kEgressBacklogBytes.
        if (!egressBacklog_.empty()) [[unlikely]] work += flushEgressBacklog_();
        if (egressBacklog_.empty()) work += requestRing_.drain([this](int32_t tid,
                                          aeron::concurrent::AtomicBuffer& b,
                                          aeron::util::index_t off,
                                          aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
            const auto  n = static_cast<std::size_t>(len);
            if (tid == kMsgDefineTopic) {
                worker_.submitDefineTopic(decodeDefineTopic(p, n));
            } else if (tid == kMsgGetSchema) {
                worker_.submitGetSchema(decodeGetSchema(p, n));
            } else if (tid == kMsgAlterTopic) {
                worker_.submitAlterTopic(decodeAlterTopic(p, n));
            } else if (tid == kMsgCreateView) {
                worker_.submitCreateView(decodeCreateView(p, n));
            }
        }, 16);

        // 4. Advance the DDL state machine + drive the replica.
        work += worker_.doWork(nowNs);

        // 4b. F3 round 6, item 3: retry SchemaUpdates the admin→control ring
        //     could not take (full) — retry-on-full, never a drop.
        if (!schemaBacklog_.empty()) [[unlikely]] work += flushSchemaBacklog_();
        if (!viewPlanBacklog_.empty()) [[unlikely]] work += flushViewPlanBacklog_();  // Task 19
        if (!topicCommittedBacklog_.empty()) [[unlikely]] work += flushTopicCommittedBacklog_();  // B10
        if (!egressBacklog_.empty()) [[unlikely]] work += flushEgressBacklog_();       // no-hang R2 / fix round 2

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
    /// ([errno:i32][dir]) and kA2cSchemaUpdate ([topicId:i32][version:u16][blob]).
    OneToOneByteRing& failureRing() noexcept { return failureRing_; }

    /// F3 round 6, item 3 — ADMIN thread only (called from deps.pushValidators
    /// inside AdminWorker::doWork).  The admin thread must NEVER enqueue into a
    /// worker's SPSC ingress ring (the control thread is its single producer):
    /// post the update on the admin→control ring; the control thread fans it
    /// out (SchemaFanout).  Ring full → admin-owned backlog, retried every
    /// doWork.  A descriptor that can never fit the ring is refused loudly.
    /// Task 12: `version` is the schema version `blob` was materialized as of
    /// (the caller passes RegistryEntry.descriptor/version, never the raw
    /// evolving descriptorBlob).
    void postSchemaUpdate(int32_t topicId, uint16_t version, const std::vector<uint8_t>& blob) {
        // Unreachable by construction (static_assert in AdminChannels.h: every
        // descriptor the request ring can deliver fits one SchemaUpdate); kept
        // as a loud backstop rather than a silent tryClaim throw.
        if (schemaUpdateWireLen(blob.size()) > static_cast<std::size_t>(failureRing_.maxMsgLength()))
            [[unlikely]] {
            spdlog::error("AdminWorkerAgent: SchemaUpdate for topicId={} version={} ({} B descriptor) "
                          "exceeds the admin→control ring max message ({} B); validator "
                          "NOT pushed", topicId, version, blob.size(), failureRing_.maxMsgLength());
            return;
        }
        if (!schemaBacklog_.empty() || !encodeSchemaUpdate(failureRing_, topicId, version, blob))
            schemaBacklog_.push_back(SchemaUpdateMsg{topicId, version, blob});   // keep FIFO order
    }
    [[nodiscard]] std::size_t schemaBacklogForTest() const noexcept { return schemaBacklog_.size(); }

    /// No-hang wave (R2) / fix round 2 — ADMIN thread only (the deps.respond*
    /// callbacks).  Post one DDL response frame for `session` on the
    /// admin→control egress ring as [sessionId:i64][frame].  Ring full → the
    /// preallocated, bounded backlog (SpillFifo, FIFO), retried every doWork;
    /// while it is non-empty no new DDL request is taken (doWork step 3), which
    /// bounds it by construction.  Returns false ONLY when the frame can never
    /// fit one ring message (8 + n > maxMsgLength) — the caller answers
    /// MessageTooLarge instead.  Never allocates.
    [[nodiscard]] bool postEgressFrame(int64_t session, const uint8_t* frame, std::size_t n) {
        if (8 + n > static_cast<std::size_t>(egressRing_.maxMsgLength())) return false;
        if (egressBacklog_.empty() && tryPostEgress_(session, frame, n)) return true;
        if (!egressBacklog_.push(session, frame, n)) [[unlikely]] haltEgressBacklogFull_();
        return true;
    }
    /// ADMIN thread only (deps.respondGet).  Encode a TopicSchemaResponse in place
    /// in the egress ring (or, ring full / backlog ahead, via the preallocated
    /// stage into the bounded backlog).  A descriptor that can never fit one ring
    /// message is answered MessageTooLarge with no blob — never parked forever,
    /// never dropped.
    void postTopicSchemaResponse(int64_t session, int64_t corr, int32_t topicId, uint16_t version,
                                 int32_t partitionCount, const std::vector<uint8_t>& blob,
                                 uint16_t errorCode) {
        namespace pc = rune::protocol::client;
        pc::TopicSchemaResponse resp;
        resp.sessionId      = session;
        resp.correlationId  = corr;
        resp.topicId        = topicId;
        resp.version        = version;
        resp.errorCode      = errorCode;
        resp.partitionCount = partitionCount;   // Phase 2b Part C
        resp.descriptorBlob = blob.empty() ? nullptr : blob.data();
        resp.descriptorLen  = static_cast<uint32_t>(blob.size());
        if (resp.encodedSize() > maxEgressFrameBytes()) [[unlikely]] {
            resp.descriptorBlob = nullptr;
            resp.descriptorLen  = 0;
            resp.errorCode      = static_cast<uint16_t>(pc::ErrorCode::MessageTooLarge);
        }
        const std::size_t sz = resp.encodedSize();
        const auto idx = egressBacklog_.empty() ? egressRing_.tryClaim(kMsgEgressFrame, 8 + sz) : -1;
        if (idx < 0) [[unlikely]] {
            [[maybe_unused]] const std::size_t w = resp.encode(egressStage_.data());
            assert(w == sz && "TopicSchemaResponse encode() != encodedSize()");
            [[maybe_unused]] const bool posted = postEgressFrame(session, egressStage_.data(), sz);
            assert(posted);
            return;
        }
        uint8_t* p = egressRing_.base() + idx;
        std::memcpy(p, &session, 8);
        [[maybe_unused]] const std::size_t written = resp.encode(p + 8);
        assert(written == sz && "TopicSchemaResponse encode() != encodedSize()");
        egressRing_.commit(idx);
    }

    /// Admin-thread scratch for an answer encoded off-ring (preallocated to one
    /// ring message; sz <= maxEgressFrameBytes()).
    [[nodiscard]] uint8_t* egressStage(std::size_t /*sz*/) noexcept { return egressStage_.data(); }
    [[nodiscard]] std::size_t maxEgressFrameBytes() const noexcept {
        return static_cast<std::size_t>(egressRing_.maxMsgLength()) - 8u;
    }
    [[nodiscard]] bool egressBacklogEmpty() const noexcept { return egressBacklog_.empty(); }
    [[nodiscard]] std::size_t egressBacklogForTest() const noexcept { return egressBacklog_.count(); }
    int flushEgressBacklogForTest() { return flushEgressBacklog_(); }
    int flushSchemaBacklogForTest() { return flushSchemaBacklog_(); }

    /// Task 19 — ADMIN thread only (called from deps.pushViewPlan inside
    /// AdminWorker::commitCreateView_).  Post a compiled view plan on the
    /// admin→control ring (kA2cViewPlanUpdate); the control thread fans it out to
    /// every worker's install ring.  Ring full → admin-owned backlog, retried
    /// every doWork (FIFO, never dropped).
    void postViewPlan(const ViewPlanUpdateMsg& vp) {
        if (viewPlanUpdateWireLen(vp) > static_cast<std::size_t>(failureRing_.maxMsgLength()))
            [[unlikely]] {
            spdlog::error("AdminWorkerAgent: ViewPlanUpdate for viewTopicId={} ({} B) exceeds "
                          "the admin→control ring max message ({} B); view NOT installed",
                          vp.viewTopicId, viewPlanUpdateWireLen(vp), failureRing_.maxMsgLength());
            return;
        }
        if (!viewPlanBacklog_.empty() ||
            !encodeViewPlanUpdate(failureRing_, kA2cViewPlanUpdate, vp))
            viewPlanBacklog_.push_back(vp);   // keep FIFO order
    }

    /// B10 / D4 fix round 1 — ADMIN thread only (deps.topicCommitted).  Post
    /// kA2cTopicCommitted on the admin→control ring; the control thread rewrites
    /// the topic's dir marker pending -> committed.  Ring full → admin-owned
    /// backlog, retried every doWork (FIFO, never dropped).
    void postTopicCommitted(int32_t topicId) {
        if (!topicCommittedBacklog_.empty() || !encodeTopicCommitted(failureRing_, topicId))
            topicCommittedBacklog_.push_back(topicId);
    }
    [[nodiscard]] std::size_t topicCommittedBacklogForTest() const noexcept {
        return topicCommittedBacklog_.size();
    }
    int flushTopicCommittedBacklogForTest() { return flushTopicCommittedBacklog_(); }

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
    std::vector<ViewPlanUpdateMsg> viewPlanBacklog_; // Task 19: admin-owned (ring-full retry)
    std::vector<int32_t> topicCommittedBacklog_;     // B10 fix round 1: admin-owned (ring-full retry)
    // Fix round 2: preallocated, bounded (was an unbounded vector<vector> with
    // O(n) front erase).  4x the ring: one tick's answers fit by construction.
    static constexpr std::size_t kEgressBacklogBytes = 4u * static_cast<std::size_t>(kAdminRequestRingBytes);
    rune::gateway::SpillFifo egressBacklog_{kEgressBacklogBytes};
    std::vector<uint8_t> egressStage_ = std::vector<uint8_t>(static_cast<std::size_t>(kAdminRequestRingBytes) / 8);

    // The bound (doWork step 3 pauses intake while the backlog is non-empty) was
    // exceeded: an invariant breach — halt loudly, never drop a DDL answer.
    [[noreturn]] [[gnu::cold]] [[gnu::noinline]] void haltEgressBacklogFull_() const noexcept {
        spdlog::critical("AdminWorkerAgent: DDL egress backlog full ({} answers, {} B) beyond its "
                         "static bound — halting rather than dropping a DDL answer",
                         egressBacklog_.count(), egressBacklog_.usedBytes());
        std::abort();
    }

    bool tryPostEgress_(int64_t session, const uint8_t* frame, std::size_t n) {
        const auto idx = egressRing_.tryClaim(kMsgEgressFrame, 8 + n);
        if (idx < 0) return false;
        uint8_t* p = egressRing_.base() + idx;
        std::memcpy(p, &session, 8);
        std::memcpy(p + 8, frame, n);
        egressRing_.commit(idx);
        return true;
    }

    int flushEgressBacklog_() {
        int sent = 0;
        while (!egressBacklog_.empty()) {
            const auto r = egressBacklog_.front();
            if (!tryPostEgress_(r.key, r.data, r.len)) break;
            egressBacklog_.pop();
            ++sent;
        }
        return sent;
    }

    int flushSchemaBacklog_() {
        std::size_t sent = 0;
        while (sent < schemaBacklog_.size() &&
               encodeSchemaUpdate(failureRing_, schemaBacklog_[sent].topicId,
                                  schemaBacklog_[sent].version, schemaBacklog_[sent].blob))
            ++sent;
        schemaBacklog_.erase(schemaBacklog_.begin(),
                             schemaBacklog_.begin() + static_cast<std::ptrdiff_t>(sent));
        return static_cast<int>(sent);
    }

    int flushTopicCommittedBacklog_() {
        std::size_t sent = 0;
        while (sent < topicCommittedBacklog_.size() &&
               encodeTopicCommitted(failureRing_, topicCommittedBacklog_[sent]))
            ++sent;
        topicCommittedBacklog_.erase(topicCommittedBacklog_.begin(),
                                     topicCommittedBacklog_.begin() + static_cast<std::ptrdiff_t>(sent));
        return static_cast<int>(sent);
    }

    int flushViewPlanBacklog_() {
        std::size_t sent = 0;
        while (sent < viewPlanBacklog_.size() &&
               encodeViewPlanUpdate(failureRing_, kA2cViewPlanUpdate, viewPlanBacklog_[sent]))
            ++sent;
        viewPlanBacklog_.erase(viewPlanBacklog_.begin(),
                               viewPlanBacklog_.begin() + static_cast<std::ptrdiff_t>(sent));
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

    /// D2: the CPUs the broker's non-worker threads run on (resolved by start(); empty = unchanged).
    /// main() pins its own thread + the signal thread to them after start().
    [[nodiscard]] const std::vector<int32_t>& nonWorkerCpus() const noexcept { return config_.nonWorkerCpus; }

    /// Bounded wait for this node to reach LEADER_READY or CLOSED.
    /// Returns true if leadership reached within timeout, false otherwise.
    bool awaitLeader(std::chrono::milliseconds timeout);

    /// Signal the agent thread to stop, join it, then close the Aeron client
    /// and embedded media driver.  Idempotent.
    void stop();

    /// The configured controlled-shutdown budget (RUNE_CONTROLLED_SHUTDOWN_MS; 0 = off).
    [[nodiscard]] std::chrono::milliseconds controlledShutdownBudget() const noexcept {
        return std::chrono::milliseconds{config_.controlledShutdownMs};
    }

    /// Controlled shutdown (Kafka parity) — call BEFORE stop() on a graceful
    /// stop signal (SIGTERM/SIGINT; never on a fail-stop halt).  Observer
    /// thread only (the thread that called start()).  See ControlledShutdown.h.
    /// Returns true when every movable partition this node led is committed
    /// as led elsewhere (and, on the controller, consensus leadership moved);
    /// false when `budget` ran out first (logged — the caller then stops as
    /// before: the remaining partitions fail over the old way).
    bool controlledShutdown(std::chrono::milliseconds budget);

    /// Rolling-restart readiness gate (observer thread): how many partitions
    /// this node replicates but is NOT in the committed ISR of; -1 = unknown
    /// (no controller known yet, or the node is stopping).  0 = this node is
    /// fully back in sync — safe to restart the next node.  Also published as
    /// the Aeron counter kOutOfSyncCounterTypeId by publishRole() (read it
    /// out-of-band from cnc.dat, like the role counter).  Observer thread
    /// (the same single consumer as controlledShutdown()).
    int32_t outOfSyncPartitions() noexcept;

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
    /// Test-only (D19 L8): build the provisioner but not its runner thread —
    /// start() must refuse (every provisioned roll would wait forever).
    void skipProvisionerRunnerForTest() noexcept { skipProvisionerRunnerForTest_ = true; }
    /// Test-only: during start(), re-run the SCHEMA log's recovery with this msync errno.
    void setSchemaRecoveryFaultForTest(int err) noexcept { schemaRecoveryFaultForTest_ = err; }
    /// Test-only (DDL conformance): the control thread answers the FIRST admin
    /// create-topic request for `name` with a failure.  afterCreate=false fails
    /// before creating; true creates the topic first (the reply is lost after the
    /// work — an orphan).  Set before start(); afterwards only the control thread
    /// reads/writes it (single owner).
    void setAdminCreateFaultForTest(std::string name, bool afterCreate) {
        adminCreateFaultName_ = std::move(name);
        adminCreateFaultAfterCreate_ = afterCreate;
    }

    // -----------------------------------------------------------------------
    // Observers (F9a: ring-fed, no atomics)
    //
    // The leadership view is owned by the CONTROL thread, which publishes a
    // {leaderId, term, isLeader} snapshot on LeaderObserver's ring whenever it
    // changes.  isLeader()/leaderId()/leadershipTermId()/awaitLeader()/
    // publishRole()/leaderClientEndpoint() DRAIN that ring: they must be called
    // from the ONE observer thread: the thread that called start() (enforced —
    // any other thread is refused: critical log, abort in debug builds).  Node-internal (control-thread) code uses
    // LeaderObserver::computeOnControl() instead — never these.
    // -----------------------------------------------------------------------

    bool   isLeader()  const noexcept;
    rune::consensus::NodeId leaderId() const noexcept;

    /// One ring-drained snapshot {leaderId, term, isLeader} — use it when more
    /// than one field is needed together (never torn).  Observer thread only.
    LeaderSnapshot leaderSnapshot() const noexcept;

    /// Return the current leadershipTermId (from the same ring snapshot as
    /// leaderId(), so the pair is never torn).
    /// Used by main.cpp to annotate leadership log messages with the term so
    /// the DR-5 oracle can distinguish stale pre-election announcements from
    /// current-term announcements (term-aware assert_single_leader).
    rune::consensus::Term leadershipTermId() const noexcept;

    /// Publish this node's current role/term/leaderId into the Aeron node-role
    /// counter (RoleCounter.h) in the driver's CnC file.  Idempotent + cheap:
    /// call it from the main poll loop on every tick.  No-op until the driver
    /// has acked the counter allocation (findCounter returns the live Counter).
    /// Observer thread (see above): one ring-drained snapshot; the counter is the
    /// machine-readable signal the dr-e2e harness reads (via rune_role) instead
    /// of scraping logs.
    void publishRole() noexcept;

    /// Return the leader's client Aeron channel for data-op redirect (GB-4).
    /// Observer thread (see above): ring-drained snapshot + immutable peer config.
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
    // Timeout: if the owner reaches the task after `timeout`, it SKIPS it (the
    // callable never runs) and std::runtime_error is thrown.  The caller waits
    // for the owner's decision with no bound of its own (F9a ruling: the ctest
    // timeout is the bound), so the callable can never run against an unwound
    // frame.
    //
    // PRECONDITION (F9a): the calling thread is the controller, or a thread
    // started after start() returned and joined before stop() is called.  That
    // ordering (thread start / join) is what makes the controller-owned
    // started_/stopped_ check below race-free, and guarantees the control
    // thread is alive to drain the task.  stop() additionally SKIPS any task
    // still queued on the control ring when the runtime is joined.
    // Preconditions (fail loudly with std::logic_error / invalid_argument):
    //   node started and not stopped; topicId != kSchemaTopicId (-4, the
    //   __topic_schemas replica is AdminWorker-owned — not routable here).
    // Callers may be on any test thread: the control test-task ring is
    // many-to-one (F9a — no test-side mutex).
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

    /// Test-only, CONTROL thread only (call inside onControlThreadForTest):
    /// the durable __cluster_metadata ISR knowledge (round-4 LOW-B tests).
    [[nodiscard]] const rune::replication::InternalIsrKnowledge* internalIsrKnowledgeForTest() const noexcept {
        return internalIsrKnowledge_.get();
    }
    /// Test-only, CONTROL thread only: the consensus Election (nullptr when none).
    [[nodiscard]] const rune::consensus::Election* electionForTest() const noexcept { return election_.get(); }

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

    /// Task 9 (RUNE_TLS SIGHUP reload — Let's Encrypt renewal): request a
    /// cert/key reload. Callable from ANY thread (main(), from the signal
    /// thread's kTlsReloadEvent) — this only offers a TRIGGER onto
    /// tlsReloadRing_; the actual file re-read (config_.tlsCertFile/
    /// tlsKeyFile) and the gateway_->reloadTlsCert() swap happen on the
    /// control thread's next serviceAdminControl_() cycle (shared-nothing:
    /// only the trigger crosses threads, never the PEM text or gateway
    /// state). Returns false if the ring is momentarily full — harmless,
    /// since main()'s 100 ms poll loop re-delivers on the next SIGHUP or a
    /// caller may retry.
    bool requestTlsCertReload() noexcept {
        return tlsReloadRing_.offer(kTlsReloadTrigger);
    }

    /// Return this node's ClientGateway (or nullptr).  Test-only: reaches into
    /// duty-cycle-owned state (e.g. pendingAckCount()); call only from a
    /// synchronized test context that knows the duty cycle is between doWork
    /// calls (or with the node stopped).  Used by GR-9 to observe the
    /// deferred-ack queue depth for the acks=All HW-gate boundary test.
    rune::gateway::ClientGateway* gatewayForTest() const noexcept {
        return gateway_.get();
    }

    /// Test-only: the runtime thread's idle strategy (thread placement; nullptr before start / fused).
    [[nodiscard]] const rune::consensus::IdleStrategy* runtimeIdleForTest() const noexcept { return idleStrategy_.get(); }
    /// Test-only: the admin thread's idle strategy (nullptr when no admin worker).
    [[nodiscard]] const rune::consensus::IdleStrategy* adminIdleForTest() const noexcept { return adminWorkerIdleStrategy_.get(); }

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

    /// Test-only: return the ViewRegistry (for byName/byViewTopicId queries in
    /// tests). Non-null after start() if __views persistence construction
    /// succeeded. NOT safe for concurrent use — call only when the admin
    /// thread is quiesced or the node is stopped (mirrors schemaRegistryForTest).
    ViewRegistry* viewRegistryForTest() const noexcept {
        return viewRegistry_.get();
    }

    /// Test-only: return raw pointer to the __views Log (for record-count
    /// assertions), mirrors schemaLogForTest.
    rune::storage::Log* viewsLogForTest() const noexcept {
        return viewsLog_.get();
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
    /// D25: completed log-cleaner cycles (0 when there is no cleaner) — tests
    /// wait on cleaner progress instead of sleeping.
    [[nodiscard]] uint64_t cleanerCyclesForTest() const noexcept {
        return cleanerAgent_ ? cleanerAgent_->cyclesDoneForTest() : 0;
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
    /// Shard drivers this node OWNS (fused worker 0 rides the node driver: not counted).
    /// The config as resolved by start() (host-derived defaults applied).
    /// Start-thread / test use after start(); immutable afterwards.
    [[nodiscard]] const NodeConfig& effectiveConfigForTest() const noexcept { return config_; }
    [[nodiscard]] std::size_t shardDriverCountForTest() const noexcept {
        std::size_t n = 0;
        for (const auto& d : shardDrivers_) n += d ? 1u : 0u;
        return n;
    }
    /// Worker w (owned by its own thread — touch only inside onOwnerThreadForTest).
    [[nodiscard]] Worker* workerForTest(int32_t w) const noexcept {
        return workerPool_ && w >= 0 && w < workerPool_->workerCount() ? &workerPool_->worker(w) : nullptr;
    }

    /// Task 19b: node-level view readiness — true iff EVERY worker's views are
    /// ready (Worker::viewsReady(): no view set, or all hosted (view, partition)
    /// instances Ready).  A thin, INERT wrapper over the per-instance serve-guard
    /// (which is what the Phase-1 single-node restart e2e actually relies on):
    /// true when no durable view is installed-for-recovery anywhere, so it never
    /// gates the no-views case closed.  Read-only; call off the hot path.  NOT
    /// wired into the election path (see AdminWorker::recoverViewsOnPromotion_ / the Task-19b
    /// report: election-eligibility gating is a multi-node concern, DEFERRED).
    [[nodiscard]] bool viewsReady() const noexcept {
        if (!workerPool_) return true;
        for (int32_t w = 0; w < workerPool_->workerCount(); ++w) {
            if (!workerPool_->worker(w).viewsReady()) return false;
        }
        return true;
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

    /// Test-only (DR-15 fix round 1): the peer-hostname resolve function of this
    /// node's cold resolver thread (default getaddrinfo).  Set before start();
    /// called on the resolver thread (and the start thread).
    void setPeerResolveFnForTest(PeerResolverHub::ResolveFn fn) { resolveFnForTest_ = std::move(fn); }
    /// Test-only: legacy-plane add brokers still alive (0 after stop()).
    [[nodiscard]] std::size_t legacyPlaneBrokerCountForTest() const noexcept { return legacyPlaneBrokers_.size(); }

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
        if (!started_ || stopped_ || !runtimeAgent_) {
            throw std::logic_error(
                "onOwnerThreadForTest: node is not running (start() not called, or "
                "already stopped) — no owner thread to run the task on");
        }
        // Deadline fixed BEFORE the ref is enqueued: the owner decides run-or-skip
        // from it alone (see OwnerThreadTestTask.h, F9a).
        auto task = std::make_shared<Impl>(topicId, partition, std::forward<F>(f),
                                           OwnerThreadTestTask::Clock::now() + timeout);
        auto* ref = new OwnerThreadTestTaskRef(task);   // the ring's reference
        if (!runtimeAgent_->offerOwnerThreadTestTask(ref)) {
            delete ref;
            throw std::runtime_error("onOwnerThreadForTest: control test-task ring full");
        }
        return awaitOwnerThreadTestTask(*task, timeout);
    }

    // F9a: control-thread-side helper for the gateway leaderView — pure function
    // of a snapshot + the immutable peer config.
    std::string leaderClientEndpointFor_(const LeaderSnapshot& v) const noexcept;

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

    // Task 9 (RUNE_TLS SIGHUP reload): the trigger-only cross-thread ring —
    // requestTlsCertReload() (any thread) offers kTlsReloadTrigger;
    // serviceAdminControl_() (control thread) polls it and performs the
    // actual file re-read + gateway_->reloadTlsCert() swap itself, on its
    // own thread.  No PEM text or gateway state ever crosses the ring.
    static constexpr std::int32_t kTlsReloadTrigger = 1;
    rune::consensus::ControlRing<256> tlsReloadRing_;
    // DR-15 fix round 1: the cold peer-hostname resolver thread + the rings it
    // feeds (driver name caches, peer-publication owners).  Declared FIRST so
    // it is destroyed LAST — after every consumer of its rings.
    std::unique_ptr<PeerResolverHub>   resolverHub_;
    PeerResolverHub::ResolveFn         resolveFnForTest_;
    // Legacy per-worker planes' control-thread re-add brokers (outlive both
    // the planes and replTransport_, which polls them).
    std::vector<std::unique_ptr<rune::replication::PubAddBroker>> legacyPlaneBrokers_;

    // -----------------------------------------------------------------------
    // Consensus objects (constructed in start())
    // -----------------------------------------------------------------------

    // Durable persistence (paths under config_.dataDir)
    // Review 2026-10-04 MAJOR-2/3: this node's durable internal-ISR knowledge —
    // the consensus vote gate and the internal replicas' quorum store.  Declared
    // BEFORE election_ / replicaRegistry_ so it outlives both.
    std::unique_ptr<rune::replication::InternalIsrKnowledge> internalIsrKnowledge_;
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

    // -----------------------------------------------------------------------
    // View catalog (Task 17 — __views persistence, mirrors __topic_schemas).
    // viewsLog_         — durable Log for __views (one partition).
    // viewsReplica_     — PartitionReplica wrapping the views Log.
    //                      topicId=-5 (kViewsTopicId), cleanupPolicy=Compact.
    //                      Admin-thread-owned only (same RULING-2 A rationale
    //                      as schemaReplica_ above): NOT registered in
    //                      replicaRegistry_/replTransport_, backed by its own
    //                      NullPartitionTransport.
    // viewRegistry_     — In-memory view catalog (ViewRegistry), rebuilt from
    //                      viewsLog_ on startup. The CreateView DDL path that
    //                      APPENDS records to viewsReplica_ is Task 19 — this
    //                      wiring only creates the partition + replays it.
    //
    // Destruction order (reverse of construction, in stop()):
    //   viewRegistry_ → viewsReplica_ → viewsLog_ → …
    // -----------------------------------------------------------------------
    std::unique_ptr<rune::storage::Log>                   viewsLog_;
    std::unique_ptr<rune::replication::PartitionReplica>  viewsReplica_;
    std::unique_ptr<ViewRegistry>                         viewRegistry_;

    // -----------------------------------------------------------------------
    // Task 19: live view-fold wiring.  One ViewSnapshotChannel + one
    // WorkerViewSet per worker (index = worker id); the WorkerViewSet is wired
    // into the worker via setViewSet() before the runners start (single-
    // threaded, like setOwnedDirect).  A single per-NODE ViewSnapshotIoAgent
    // drains EVERY worker's channel on its own thread (mirrors the cleaner
    // agent).  fanoutViewPlan_/pumpViewFanout_ deliver a committed compiled plan
    // (admin→control) to each worker's install ring (control→worker) with
    // retry-on-full held in viewFanoutBacklog_ (never dropped).
    // -----------------------------------------------------------------------
    std::vector<std::unique_ptr<ViewSnapshotChannel>>     viewSnapshotChannels_;
    std::vector<std::unique_ptr<WorkerViewSet>>           workerViewSets_;
    std::unique_ptr<ViewSnapshotIoAgent>                  viewSnapshotIoAgent_;
    std::unique_ptr<rune::consensus::BackoffIdleStrategy> viewSnapshotIoIdleStrategy_;
    rune::consensus::MonotonicNanoClock                   viewSnapshotIoClock_;
    std::unique_ptr<rune::consensus::AgentRunner>         viewSnapshotIoRunner_;
    struct ViewFanoutPending { ViewPlanUpdateMsg msg; int32_t nextWorker = 0; };
    std::vector<ViewFanoutPending>                        viewFanoutBacklog_;  // control-thread-owned
    void fanoutViewPlan_(ViewPlanUpdateMsg msg);   // control thread
    int  pumpViewFanout_();                        // control thread (every cycle)
    // Task 19b (restart recovery): a restarted broker's durable view catalog is
    // recovered on the AdminWorker's Follower->Leader promotion edge
    // (AdminWorker::recoverViewsOnPromotion_), NOT at buildGateway — a fresh
    // __views Follower has highWatermark()==0 until promotion, so the catalog is
    // not replayable earlier.  The AdminWorker rebuilds the ViewRegistry and
    // fans out a RECOVERY install per view via deps_.pushViewPlan, which reaches
    // the same fanoutViewPlan_/pumpViewFanout_ path (co-locate + installForRecovery).

    // Runtime-cycle DDL control plane (direct call; see AdminControlPlane.h).
    friend class AdminControlPlane;
    void attachFusedRuntimeDoorbell_();   // A9: runtime's cross-thread rings → worker 0's doorbell (fused)
    int serviceAdminControl_();
    AdminControlPlane                                     adminControl_{*this};
    int64_t                                               slowTickIntervalOverrideNs_ = -1;
    // Task 8a: last leadership state posted to the admin role ring (control thread).
    bool                                                  adminLastLeader_ = false;
    int32_t                                               adminLastEpoch_  = -1;
    // Task 14: the static RUNE_TOPICS count, set once on the startup thread before the
    // recovery append (ids >= it are runtime topics); read by the control thread's
    // create drain (createTopicForAdmin's firstRuntimeId).  Immutable after start.
    // Defaults to "never matches" (fail-closed) until start() sets it.
    int32_t                                               firstRuntimeTopicId_ = std::numeric_limits<int32_t>::max();
    std::unique_ptr<rune::consensus::BackoffIdleStrategy> adminWorkerIdleStrategy_;   // NodeConfig::adminIdleShape
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
    std::vector<std::unique_ptr<EmbeddedDriver>> shardDrivers_;   ///< null = rides the node driver (fused w0)
    EmbeddedDriver& shardDriver_(std::size_t w) noexcept {
        return shardDrivers_[w] ? *shardDrivers_[w] : *nodeDriver_;
    }
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
    bool skipProvisionerRunnerForTest_ = false;
    int  schemaRecoveryFaultForTest_ = 0;
    std::string adminCreateFaultName_;              // control thread after start()
    bool        adminCreateFaultAfterCreate_ = false;
    bool        adminCreateFaultFired_       = false;
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
    // hotpath-A F2: the segment provisioner (see SegmentProvisionerAgent.h).
    // Destroyed after the replica registry (declared before it is NOT required:
    // the logs never call into it after the workers stopped).
    std::unique_ptr<SegmentProvisionerAgent>              provisionerAgent_;
    std::unique_ptr<ParkUntilWorkIdle>                    provisionerIdleStrategy_;
    rune::consensus::MonotonicNanoClock                   provisionerClock_;
    std::unique_ptr<rune::consensus::AgentRunner>         provisionerRunner_;
    std::array<std::int64_t, SegmentProvisionerAgent::kMetricCount>                    provCounterRegIds_{-1, -1, -1, -1, -1};
    std::array<std::shared_ptr<aeron::Counter>, SegmentProvisionerAgent::kMetricCount> provCounters_{};
    static void publishProvisionMetric_(void* self, int metric, std::int64_t value) noexcept;
    [[nodiscard]] rune::storage::ProvisionChannel* provisionChannel_() noexcept {
        return provisionerAgent_ ? provisionerAgent_->channel() : nullptr;
    }

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
    std::int64_t                     outOfSyncCounterRegId_ = -1;   ///< readiness gate (L5)
    std::shared_ptr<aeron::Counter>  outOfSyncCounter_;
    // D9 review MEDIUM-3: the TCS stuck-markers gauge counter.  Resolved and
    // set on the CONTROL (runtime) thread only, from the TCS sink below.
    std::int64_t                     stuckMarkersCounterRegId_ = -1;
    std::shared_ptr<aeron::Counter>  stuckMarkersCounter_;
    static bool publishStuckMarkers_(void* self, std::int64_t value);

    // -----------------------------------------------------------------------
    // State flags — CONTROLLER-owned (F9a: no atomics).  Written only by the
    // thread calling start()/stop(); the controller role may move to another
    // thread only across a happens-before edge (thread start/join), e.g.
    // ShardModeTest stops the node from a helper thread it then joins.
    // -----------------------------------------------------------------------
    bool started_ = false;
    bool stopped_ = false;

    // F9a: control thread → observer thread leadership ring (see Observers).
    // mutable: the const observer accessors drain it.
    mutable LeaderObserver leaderObserver_{static_cast<rune::consensus::NodeId>(config_.nodeId)};
};

} // namespace rune::node
