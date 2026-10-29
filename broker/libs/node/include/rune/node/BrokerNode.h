#pragma once
#include <rune/peerlink/ClusterKeys.h>
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

#include <rune/storage/ZeroedBytes.h>
#include <algorithm>
#include <cstring>
#include <cassert>
#include <optional>
#include <rune/protocol/client/NamespaceMessages.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/node/CatalogReadFlow.h>
#include <cstdlib>
#include <limits>
#include <rune/gateway/SpillFifo.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/StorageIdentity.h>
#include <rune/node/StorageGate.h>
#include <array>
#include <rune/node/HostDefaults.h>
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
#include <rune/node/AdminIsrChannel.h>
#include <rune/node/CatalogPartition.h>
#include <rune/node/CatalogApplier.h>
#include <rune/node/SecurityApplier.h>
#include <rune/node/SecurityPartition.h>
#include <rune/node/AuthDevMode.h>
#include <rune/gateway/SecurityView.h>
#include <rune/codec/Serialize.h>
#include <rune/node/AdminControlPlane.h>
#include <rune/node/DdlAnswerGate.h>
#include <rune/node/SecurityAnswerGate.h>
#include <rune/node/SecurityServing.h>
#include <rune/node/SchemaFanout.h>
#include <rune/node/TopicDroppedFanout.h>
#include <rune/node/NsSlotFanout.h>
#include <rune/node/NsSlotTable.h>
#include <rune/node/PeerLinkAgent.h>
#include <rune/node/PeerLinkFanout.h>
#include <rune/peerlink/ClusterKeys.h>
#include <rune/peerlink/PeerFrameCrypto.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/ViewSnapshotIoAgent.h>
#include <rune/node/OwnerThreadTestTask.h>
#include <rune/node/LeaderObserver.h>
#include <rune/node/RuneMetrics.h>
#include <metrics/MetricsFile.h>
#include <metrics/MetricsWriter.h>
#include <rune/node/RuneMetricSlot.h>
#include <metrics/SystemMetrics.h>
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
#include <variant>
#include <functional>
#include <memory>
#include <map>
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
// owns the __catalog replica).  Every cross-thread hop is an SPSC ring
// (Rule #2, Aeron-style):
//
//   requestRing_ (gateway/control → admin) : every DDL request + GetSchema.
//   controlToAdminRing_ (control → admin)   : dropped-dir purges (kMsgPurgeDroppedDir, Task 8).
//   roleRing_    (control → admin)           : the role events (+ the catalog's AlterIsr answers).
//   failureRing_ / egressRing_ (admin → control): the applier's effects / DDL answers.
//
// The dead unsynchronized linked-list enqueue path (Task 6) is REMOVED.
// ---------------------------------------------------------------------------
class AdminWorkerAgent final : public rune::consensus::Agent,
                               public rune::gateway::IAdminRequestSink {
public:
    AdminWorkerAgent(SchemaRegistry& reg,
                     int32_t selfId,
                     AdminDeps deps,
                     ViewRegistry* viewRegistry = nullptr,
                     rune::replication::PartitionReplica* catalogReplica = nullptr,
                     rune::replication::WorkerReplicationTransport* catalogPlane = nullptr)
        : worker_(reg, selfId, std::move(deps), viewRegistry, catalogReplica, catalogPlane)
        , catalogSchemas_(&reg)
        , requestRing_(kAdminRequestRingBytes)
        , controlToAdminRing_(kControlToAdminRingBytes)
        , roleRing_(kAdminRoleRingBytes) {}

    const char* name() const noexcept override { return "rune-admin"; }

    // ── IAdminRequestSink (gateway → admin ring adapter; RULING-2 F) ──────────
    // Called on the gateway/control thread; encodes onto requestRing_ (SPSC).
    // Every view is copied into the ring frame before return.
    rune::gateway::AdminSubmit submitDefineTopic(int64_t sessionId, int64_t correlationId,
                                                 std::string_view name, uint32_t partitionCount,
                                                 std::string_view keyField, bool validate,
                                                 const uint8_t* descriptorBlob, uint32_t descriptorLen,
                                                 uint8_t persistence, int16_t replicationFactor,
                                                 const rune::protocol::TopicConfigView& config,
                                                 const rune::gateway::DdlMetaView& meta,
                                                 std::string_view adoptDenial) override {
        DefineTopicMsg m;
        m.session        = sessionId;
        m.corr           = correlationId;
        m.partitionCount = static_cast<int32_t>(partitionCount);
        m.validate       = validate;
        m.name.assign(name);
        m.keyField.assign(keyField);
        if (descriptorBlob && descriptorLen) m.blob.assign(descriptorBlob, descriptorBlob + descriptorLen);
        m.persistence       = persistence;
        m.replicationFactor = replicationFactor;
        m.config = rune::protocol::convertTopicConfig<rune::protocol::TopicConfig>(config);
        m.meta   = ownMeta_(meta);
        m.nsId = meta.targetNsId;   // namespaces Task 10: the gateway-resolved namespace
        m.adoptDenial.assign(adoptDenial);
        return submitDdl_(m);
    }

    bool submitGetSchema(int64_t sessionId, int64_t correlationId,
                         std::string_view name,
                         uint64_t requestedFingerprint, uint32_t nsId) override {
        GetSchemaMsg m;
        m.session = sessionId;
        m.corr    = correlationId;
        m.nsId    = nsId;   // namespaces Task 10: the gateway-resolved namespace
        m.name.assign(name);
        m.requestedFingerprint = requestedFingerprint;
        return encodeGetSchema(requestRing_, m);
    }

    rune::gateway::AdminSubmit submitAlterTopic(int64_t sessionId, int64_t correlationId,
                                                std::string_view topicName, uint16_t expectedVersion,
                                                const uint8_t* opsBlob, uint32_t opsLen,
                                                const rune::gateway::DdlMetaView& meta) override {
        AlterTopicMsg m;
        m.session         = sessionId;
        m.corr            = correlationId;
        m.expectedVersion = expectedVersion;
        m.topicName.assign(topicName);
        if (opsBlob && opsLen) m.opsBlob.assign(opsBlob, opsBlob + opsLen);
        m.meta = ownMeta_(meta);
        m.nsId = meta.targetNsId;   // namespaces Task 10: the gateway-resolved namespace
        return submitDdl_(m);
    }

    rune::gateway::AdminSubmit submitCreateView(int64_t sessionId, int64_t correlationId,
                                                std::string_view viewName, std::string_view keyField,
                                                uint8_t persistence, const uint8_t* planBlob, uint32_t planLen,
                                                const rune::gateway::DdlMetaView& meta) override {
        CreateViewMsg m;
        m.session     = sessionId;
        m.corr        = correlationId;
        m.persistence = persistence;
        m.viewName.assign(viewName);
        m.keyField.assign(keyField);
        if (planBlob && planLen) m.planBlob.assign(planBlob, planBlob + planLen);
        m.meta = ownMeta_(meta);
        m.nsId = meta.targetNsId;   // namespaces Task 10: the gateway-resolved namespace
        return submitDdl_(m);
    }

    rune::gateway::AdminSubmit submitDropTopic(int64_t sessionId, int64_t correlationId, std::string_view name,
                                               const rune::gateway::DdlMetaView& meta) override {
        DropTopicMsg m;
        m.session = sessionId;
        m.corr    = correlationId;
        m.name.assign(name);
        m.meta = ownMeta_(meta);
        m.nsId = meta.targetNsId;   // namespaces Task 10: the gateway-resolved namespace
        return submitDdl_(m);
    }

    rune::gateway::AdminSubmit submitAlterTopicConfig(int64_t sessionId, int64_t correlationId,
                                                      std::string_view name, uint32_t expectedVersion,
                                                      std::span<const uint8_t> changes,
                                                      const rune::gateway::DdlMetaView& meta) override {
        namespace pc = rune::protocol::client;
        AlterTopicConfigMsg m;
        m.session         = sessionId;
        m.corr            = correlationId;
        m.expectedVersion = expectedVersion;
        m.name.assign(name);
        const uint16_t n = pc::configChangeCount(changes);
        m.changes.reserve(n);
        for (std::size_t i = 0; i < n; ++i) {
            const pc::ConfigChangeWire c = pc::configChangeAt(changes, i);
            m.changes.push_back(rune::catalog::ConfigChange{static_cast<rune::catalog::ConfigKey>(c.key), c.value});
        }
        m.meta = ownMeta_(meta);
        m.nsId = meta.targetNsId;   // namespaces Task 10: the gateway-resolved namespace
        return submitDdl_(m);
    }

    rune::gateway::AdminSubmit submitDropView(int64_t sessionId, int64_t correlationId, std::string_view name,
                                              const rune::gateway::DdlMetaView& meta) override {
        DropViewMsg m;
        m.session = sessionId;
        m.corr    = correlationId;
        m.name.assign(name);
        m.meta = ownMeta_(meta);
        m.nsId = meta.targetNsId;   // namespaces Task 10: the gateway-resolved namespace
        return submitDdl_(m);
    }

    rune::gateway::AdminSubmit submitAlterView(int64_t sessionId, int64_t correlationId, std::string_view name,
                                               std::string_view keyField, uint8_t persistence,
                                               uint32_t expectedVersion, const uint8_t* planBlob, uint32_t planLen,
                                               const rune::gateway::DdlMetaView& meta) override {
        AlterViewMsg m;
        m.session         = sessionId;
        m.corr            = correlationId;
        m.persistence     = persistence;
        m.expectedVersion = expectedVersion;
        m.name.assign(name);
        m.keyField.assign(keyField);
        if (planBlob && planLen) m.planBlob.assign(planBlob, planBlob + planLen);
        m.meta = ownMeta_(meta);
        m.nsId = meta.targetNsId;   // namespaces Task 10: the gateway-resolved namespace
        return submitDdl_(m);
    }

    // Catalog history Task 12: catalog reads (gateway → admin, the same request ring).
    bool submitCatalogSubscribe(int64_t sessionId, int64_t correlationId, int64_t fromOffset) override {
        return encodeCatalogSubscribe(requestRing_, CatalogSubscribeMsg{sessionId, correlationId, fromOffset});
    }
    bool submitCatalogUnsubscribe(int64_t sessionId, int64_t subscriptionId) override {
        return encodeCatalogUnsubscribe(requestRing_, CatalogUnsubscribeMsg{sessionId, subscriptionId});
    }

    // ── Authentication Task 10 (D36): security DDL / bootstrap / `__security` reads (gateway → admin) ───────────
    rune::gateway::AdminSubmit submitSecurityDdl(const rune::protocol::client::SecurityDdl& w,
                                                 const rune::gateway::DdlMetaView& meta, std::string_view denial,
                                                 bool proofChecked) override {
        SecurityDdlMsg m;
        m.session = w.sessionId;
        m.corr    = w.correlationId;
        m.meta    = ownMeta_(meta);
        m.kind    = static_cast<uint8_t>(w.kind);
        m.name.assign(w.name);
        m.change            = static_cast<uint8_t>(w.change);
        m.passwordIndex     = w.passwordIndex;
        m.terminateSessions = w.terminateSessions;
        if (w.hasVerifier()) ownVerifier_(w.verifier, m.verifier);
        m.granteeKind = w.granteeKind; m.op = w.op; m.resourceType = w.resourceType; m.patternKind = w.patternKind;
        m.pattern.assign(w.pattern);
        m.roleName.assign(w.roleName);
        m.force = (w.flags & rune::protocol::client::kSecurityDdlFlagForce) != 0 ? 1 : 0;
        m.denial.assign(denial);
        m.proofChecked = proofChecked ? 1 : 0;
        m.callerNsId   = meta.callerNsId;   // namespaces Task 9: the session's namespace (Task 8's resolution input)
        return submitDdl_(m);
    }
    rune::gateway::AdminSubmit submitSecurityBootstrap(const rune::protocol::client::SecurityBootstrap& w,
                                                       const rune::gateway::DdlMetaView& meta) override {
        SecurityBootstrapMsg m;
        m.session = w.sessionId;
        m.corr    = w.correlationId;
        m.meta    = ownMeta_(meta);
        m.adminName.assign(w.adminName);
        ownVerifier_(w.verifier, m.verifier);
        m.fakeSaltSeed = w.fakeSaltSeed;
        m.nonce        = w.nonce;
        return submitDdl_(m);
    }
    bool submitSecuritySubscribe(int64_t sessionId, int64_t correlationId, int64_t fromOffset) override {
        return encodeSecuritySubscribe(requestRing_, CatalogSubscribeMsg{sessionId, correlationId, fromOffset});
    }
    bool submitSecurityUnsubscribe(int64_t sessionId, int64_t subscriptionId) override {
        return encodeSecurityUnsubscribe(requestRing_, CatalogUnsubscribeMsg{sessionId, subscriptionId});
    }

    // ── Namespaces Task 7: NamespaceDdl (81) and ListNamespaces (82) (gateway → admin) ──────────────────────────
    rune::gateway::AdminSubmit submitNamespaceDdl(const rune::protocol::client::NamespaceDdlRequest& w,
                                                  const rune::gateway::DdlMetaView& meta, std::string_view denial,
                                                  uint16_t clientVersion) override {
        NamespaceDdlMsg m;
        m.session = w.sessionId;
        m.corr    = w.correlationId;
        m.meta    = ownMeta_(meta);
        m.kind    = static_cast<uint8_t>(w.kind);
        m.name.assign(w.name);
        m.mask    = w.presentMask;
        m.limits  = w.limits;
        m.denial.assign(denial);
        m.clientVersion = clientVersion;
        m.mode = static_cast<uint8_t>(w.mode);
        return submitDdl_(m);
    }
    rune::gateway::AdminSubmit submitListNamespaces(int64_t sessionId, int64_t correlationId, uint32_t afterNsId,
                                                    uint16_t max, uint32_t callerNsId, bool clusterAdmin) override {
        ListNamespacesMsg m;
        m.session      = sessionId;
        m.corr         = correlationId;
        m.afterNsId    = afterNsId;
        m.max          = max;
        m.callerNsId   = callerNsId;
        m.clusterAdmin = clusterAdmin ? 1 : 0;
        return submitDdl_(m);
    }

    int doWork(int64_t nowNs) noexcept override {
        int work = 0;

        // 1. Drain role events (control → admin) FIRST so leadership is current
        //    before any DDL runs this tick.
        //    Catalog history Task 6: the same ring carries the __catalog role
        //    (kMsgCatalogRole) and the controller's AlterIsrResponse for (-6, 0)
        //    (kMsgCatalogAlterIsrResponse, verbatim) — explicit per-type dispatch;
        //    a malformed / unknown frame is reported, never applied as another.
        work += roleRing_.drain([this](int32_t tid,
                                       aeron::concurrent::AtomicBuffer& b,
                                       aeron::util::index_t off,
                                       aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
            const auto  n = static_cast<std::size_t>(len);
            if (tid == kMsgCatalogRole) {
                // Fix round 1 (M4, fix-on-sight): checked decode — a bad frame is
                // reported, never applied as a default Follower@0 role.
                AdminRoleMsg m;
                if (decodeAdminRole(p, n, m)) worker_.onCatalogRole(m);
                else reportBadRoleFrame_(tid, n);
                return;
            }
            if (tid == kMsgCatalogHoldLeadership) {   // ISR v3 Step 10: the catalog leader role was held
                AdminRoleMsg m;
                if (decodeAdminRole(p, n, m)) worker_.onCatalogHoldLeadership(m);
                else reportBadRoleFrame_(tid, n);
                return;
            }
            if (tid == kMsgCatalogAlterIsrResponse) {
                rune::replication::AlterIsrResponseMsg r{};
                if (decodeCatalogAlterIsrResponse(p, n, r)) worker_.onCatalogAlterIsrResponse(r);
                else reportBadRoleFrame_(tid, n);
                return;
            }
            // Authentication Task 9 (D39): the `__security` replica's role, held role and AlterIsr answer.
            if (tid == kMsgSecurityRole) {
                AdminRoleMsg m;
                if (decodeAdminRole(p, n, m)) worker_.onSecurityRole(m);
                else reportBadRoleFrame_(tid, n);
                return;
            }
            if (tid == kMsgSecurityHoldLeadership) {
                AdminRoleMsg m;
                if (decodeAdminRole(p, n, m)) worker_.onSecurityHoldLeadership(m);
                else reportBadRoleFrame_(tid, n);
                return;
            }
            if (tid == kMsgSecurityAlterIsrResponse) {
                rune::replication::AlterIsrResponseMsg r{};
                if (decodeCatalogAlterIsrResponse(p, n, r)) worker_.onSecurityAlterIsrResponse(r);
                else reportBadRoleFrame_(tid, n);
                return;
            }
            reportBadRoleFrame_(tid, n);
        }, 16);

        // 1b. Catalog history Task 6: test-only tasks to run on THIS (admin)
        //     thread (BrokerNode::onAdminThreadForTest).  Empty in production:
        //     one acquire load.
        work += testTaskRing_.poll([](int32_t, std::int64_t a, std::int64_t) noexcept {
            runOwnerThreadTestTask(reinterpret_cast<void*>(static_cast<std::uintptr_t>(a)), nullptr);
        }, 16);

        // 2. Drain the control → admin ring: dropped-dir purges (catalog history
        //    Task 8) — explicit per-type dispatch.
        work += controlToAdminRing_.drain([this, nowNs](int32_t tid,
                                                 aeron::concurrent::AtomicBuffer& b,
                                                 aeron::util::index_t off,
                                                 aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
            const auto  n = static_cast<std::size_t>(len);
            if (tid == kMsgPlacementView) {   // Task 11 fix round 1 (I2)
                PlacementViewMsg v;
                if (decodePlacementView(p, n, v)) worker_.onPlacementView(v);
                else if (badControlFrames_++ % 1024 == 0)
                    spdlog::error("AdminWorkerAgent: malformed kMsgPlacementView (len={}) refused", n);
                return;
            }
            if (tid == kMsgCatalogReadPaused) {   // Task 12 fix round 0
                CatalogReadPausedMsg m;
                if (decodeCatalogReadPaused(p, n, m)) worker_.catalogReadPaused(m);
                else if (badControlFrames_++ % 1024 == 0)
                    spdlog::error("AdminWorkerAgent: malformed kMsgCatalogReadPaused (len={}) refused", n);
                return;
            }
            if (tid == kMsgCatalogSessionGone) {   // Task 12 fix round 3
                if (n == 8) worker_.catalogSessionGone(admin_wire::getI64(p));
                else if (badControlFrames_++ % 1024 == 0)
                    spdlog::error("AdminWorkerAgent: malformed kMsgCatalogSessionGone (len={}) refused", n);
                return;
            }
            if (tid == kMsgCatalogReadResume) {
                CatalogReadResumeMsg m;
                if (decodeCatalogReadResume(p, n, m)) worker_.catalogReadResume(m);
                else if (badControlFrames_++ % 1024 == 0)
                    spdlog::error("AdminWorkerAgent: malformed kMsgCatalogReadResume (len={}) refused", n);
                return;
            }
            if (tid == kMsgPeerLinkKeys) {   // Authentication Task 15 (D33)
                PeerLinkKeysMsg m;
                if (decodePeerLinkKeys(p, n, m)) worker_.onPeerLinkKeys(m);
                else if (badControlFrames_++ % 1024 == 0)
                    spdlog::error("AdminWorkerAgent: malformed kMsgPeerLinkKeys (len={}) refused", n);
                return;
            }
            if (tid == kMsgPurgeDroppedDir) {
                onPurgeDroppedDir_(std::string_view(reinterpret_cast<const char*>(p), n));
                return;
            }
            if (tid == kMsgNsReapResult) {   // Phase 2 Task 7: the control thread's answer to a reaper request
                ReapResultMsg m;
                if (decodeReapResult(p, n, m)) worker_.onNsReapResult(m);
                else if (badControlFrames_++ % 1024 == 0)
                    spdlog::error("AdminWorkerAgent: malformed kMsgNsReapResult (len={}) refused", n);
                return;
            }
            if (badControlFrames_++ % 1024 == 0)
                spdlog::error("AdminWorkerAgent: control→admin frame type={} len={} refused (unknown; refused "
                              "so far: {})", tid, n, badControlFrames_);
        }, 16);
        if (!holdDroppedDirPurge_ && !heldPurgeEnds_.empty()) [[unlikely]] work += releaseHeldPurges_();

        // 3. Drain gateway requests (every DDL + GetSchema).
        //    Fix round 2: while DDL answers wait in the egress backlog, NO new
        //    request is taken — the request ring fills and the gateway answers
        //    the next DDL Internal (an explicit answer) instead of the backlog
        //    growing.  This bounds the backlog to one tick's answers (<= 16
        //    drained + the in-flight op), far below kEgressBacklogBytes.
        if (!egressBacklog_.empty()) [[unlikely]] work += flushEgressBacklog_();
        //    Task 12 fix round 1 (M3): likewise while the catalog read server's owed
        //    answers near their preallocated capacity (the catalog-read ring is
        //    always drained, so this is transient) — bounded, never grown.
        if (egressBacklog_.empty() && !worker_.catalogIntakePaused()) work += requestRing_.drain([this](int32_t tid,
                                          aeron::concurrent::AtomicBuffer& b,
                                          aeron::util::index_t off,
                                          aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
            const auto  n = static_cast<std::size_t>(len);
            switch (tid) {
            case kMsgDefineTopic:      worker_.submit(decodeDefineTopic(p, n)); break;
            case kMsgGetSchema:        worker_.submitGetSchema(decodeGetSchema(p, n)); break;
            case kMsgAlterTopic:       worker_.submit(decodeAlterTopic(p, n)); break;
            case kMsgCreateView:       worker_.submit(decodeCreateView(p, n)); break;
            case kMsgDropTopic:        worker_.submit(decodeDropTopic(p, n)); break;
            case kMsgDropView:         worker_.submit(decodeDropView(p, n)); break;
            case kMsgAlterView:        worker_.submit(decodeAlterView(p, n)); break;
            case kMsgAlterTopicConfig: worker_.submit(decodeAlterTopicConfig(p, n)); break;
            case kMsgCatalogSubscribe: {   // Task 12
                CatalogSubscribeMsg m;
                if (decodeCatalogSubscribe(p, n, m)) worker_.catalogSubscribe(m);
                else reportBadRequestFrame_(tid, n);
                break;
            }
            case kMsgCatalogUnsubscribe: {
                CatalogUnsubscribeMsg m;
                if (decodeCatalogUnsubscribe(p, n, m)) worker_.catalogUnsubscribe(m);
                else reportBadRequestFrame_(tid, n);
                break;
            }
            // Authentication Task 10: the security write path and `__security` reads.
            case kMsgSecurityDdl:       worker_.submitSecurity(decodeSecurityDdl(p, n)); break;
            case kMsgSecurityBootstrap: worker_.submitSecurity(decodeSecurityBootstrap(p, n)); break;
            // Namespaces Task 7.
            case kMsgNamespaceDdl:      worker_.submit(decodeNamespaceDdl(p, n)); break;
            case kMsgListNamespaces:    worker_.submit(decodeListNamespaces(p, n)); break;
            case kMsgSecuritySubscribe: {
                CatalogSubscribeMsg m;
                if (decodeCatalogSubscribe(p, n, m)) worker_.securitySubscribe(m);
                else reportBadRequestFrame_(tid, n);
                break;
            }
            case kMsgSecurityUnsubscribe: {
                CatalogUnsubscribeMsg m;
                if (decodeCatalogUnsubscribe(p, n, m)) worker_.securityUnsubscribe(m);
                else reportBadRequestFrame_(tid, n);
                break;
            }
            default:
                if (badRequestFrames_++ % 1024 == 0)
                    spdlog::error("AdminWorkerAgent: request-ring frame type={} len={} refused (unknown; refused so "
                                  "far: {})", tid, n, badRequestFrames_);
                break;
            }
        }, 16);

        // 4. Advance the DDL state machine + drive the replica.
        work += worker_.doWork(nowNs);

        // 4b. F3 round 6, item 3: retry SchemaUpdates the admin→control ring
        //     could not take (full) — retry-on-full, never a drop.
        if (!egressBacklog_.empty()) [[unlikely]] work += flushEgressBacklog_();       // no-hang R2 / fix round 2
        // Catalog history Task 7: the CatalogApplier's effects that found the ring full.
        if (!catalogBacklog_.empty()) [[unlikely]] work += flushCatalogBacklog_();

        // 5. fsyncgate (Task 10 fix 3 / I1): the schema log this thread drives
        //    failed an fsync → hand it to the control thread (ring, no shared
        //    state), which halts the broker.  Signalled once (retried while the
        //    ring is full).
        if (!storageFailureSignalled_) {
            // Catalog history Task 6: the __catalog log is admin-driven too (authentication Task 9: and __security's).
            auto* r = worker_.catalogReplica();
            if (!(r && r->isDiskBacked() && r->log()->storageFailed()))
                r = worker_.securityReplica();
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
        // Authentication Task 16: the admin plane's dropped inter-broker frames, paced (<= 1 per second) and only on a
        // change, to the control thread (counter 1'000'235).  This thread owns the plane's crypto - nothing is shared.
        if (nowNs >= nextPeerDropReportNs_) {
            nextPeerDropReportNs_ = nowNs + 1'000'000'000LL;
            const uint64_t drops = worker_.peerFrameDrops();
            if (drops != reportedPeerDrops_) {
                uint8_t b[8];
                std::memcpy(b, &drops, 8);
                if (failureRing_.write(b, sizeof b, kA2cPeerFrameDrops)) reportedPeerDrops_ = drops;
            }
        }
        return work;
    }

    /// admin → control ring (T10; F3 r6 extended): kA2cStorageFailure
    /// ([errno:i32][dir]) and kA2cSchemaUpdate ([topicId:i32][version:u16][blob]).
    OneToOneByteRing& failureRing() noexcept { return failureRing_; }


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
    /// Authentication Task 10 (D24) - ADMIN thread only (deps.respondSecurity).  A DdlResponse for a security DDL /
    /// bootstrap.  `gateOffset` >= 0 (an OK): posted as kMsgEgressFrameGated [session][eventOffset][frame] - the control
    /// thread releases it only once ITS SecurityView applied that event and its refresh reached the workers; otherwise
    /// an ordinary egress frame.  The detail is cut to what one frame holds (never a refused answer).
    void postSecurityResponse(int64_t session, int64_t corr, uint16_t code, uint64_t objectId, uint32_t newVersion,
                              int64_t gateOffset, std::string_view detail) {
        namespace pc = rune::protocol::client;
        pc::DdlResponse r;
        r.sessionId     = session;
        r.correlationId = corr;
        r.errorCode     = code;
        r.objectId      = objectId;
        r.newVersion    = newVersion;
        r.gateTopicId   = -1;
        const std::size_t fixed = pc::MessageHeader::ENCODED_SIZE + pc::DdlResponse::BODY_FIXED + 2u;
        r.detail = detail.substr(0, std::min<std::size_t>({detail.size(), 0xFFFFu, 1024u, maxEgressFrameBytes() - fixed - 8u}));
        if (gateOffset < 0) {
            const std::size_t n = r.encode(egressStage_.data());
            [[maybe_unused]] const bool posted = n > 0 && postEgressFrame(session, egressStage_.data(), n);
            assert(posted && "a clamped DdlResponse always fits one admin egress message");
            return;
        }
        // [eventOffset:i64][frame], keyed with the gated bit; the ring/backlog prepend the session.
        admin_wire::putI64(egressStage_.data(), gateOffset);
        const std::size_t n = r.encode(egressStage_.data() + 8);
        const std::size_t total = 8 + n;
        if (n == 0 || 8 + total > static_cast<std::size_t>(egressRing_.maxMsgLength())) [[unlikely]] return;
        const int64_t key = session | kGatedKeyBit;
        if (egressBacklog_.empty() && tryPostEgress_(key, egressStage_.data(), total)) return;
        if (!egressBacklog_.push(key, egressStage_.data(), total)) [[unlikely]] haltEgressBacklogFull_();
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

    /// Catalog history Task 9 — ADMIN thread only (deps.respondDdl).  A DdlResponse
    /// (78) for DropTopic / AlterTopicConfig / DropView / AlterView.  The detail is
    /// informational: cut to what one frame holds (never a refused answer).
    void postDdlResponse(int64_t session, int64_t corr, uint16_t code, uint64_t objectId, uint32_t newVersion,
                         int32_t gateTopicId, std::string_view detail) {
        namespace pc = rune::protocol::client;
        pc::DdlResponse r;
        r.sessionId   = session;
        r.correlationId = corr;
        r.errorCode   = code;
        r.objectId    = objectId;
        r.newVersion  = newVersion;
        r.gateTopicId = gateTopicId;
        const std::size_t fixed = pc::MessageHeader::ENCODED_SIZE + pc::DdlResponse::BODY_FIXED + 2u;
        r.detail = detail.substr(0, std::min<std::size_t>({detail.size(), 0xFFFFu, maxEgressFrameBytes() - fixed}));
        const std::size_t n = r.encode(egressStage_.data());
        [[maybe_unused]] const bool posted = n > 0 && postEgressFrame(session, egressStage_.data(), n);
        assert(posted && "a clamped DdlResponse always fits one admin egress message");
    }
    /// Namespaces Task 7 — ADMIN thread only (deps.respondListNamespaces).  A ListNamespacesResponse (83): a page is
    /// at most 100 entries (~33 KB), well inside one admin egress message (ring/8 = 128 KiB); a page that still
    /// does not fit is answered ErrorResponse MessageTooLarge (never dropped).
    void postListNamespacesResponse(int64_t session, const rune::protocol::client::ListNamespacesResponse& r) {
        namespace pc = rune::protocol::client;
        if (r.encodedSize() > maxEgressFrameBytes()) [[unlikely]] {
            postErrorResponse(session, r.correlationId, static_cast<uint16_t>(pc::ErrorCode::MessageTooLarge),
                              "ListNamespaces page larger than one admin egress message");
            return;
        }
        const std::size_t n = r.encode(egressStage_.data());
        [[maybe_unused]] const bool posted = n > 0 && postEgressFrame(session, egressStage_.data(), n);
        assert(posted && "a ListNamespaces page within maxEgressFrameBytes always fits one admin egress message");
    }
    /// Catalog history Task 9 — ADMIN thread only (deps.respondError).  An
    /// ErrorResponse (5) carrying a DDL rejection's detail (InvalidRequest).
    void postErrorResponse(int64_t session, int64_t corr, uint16_t code, std::string_view message) {
        namespace pc = rune::protocol::client;
        pc::ErrorResponse r;
        r.sessionId     = session;
        r.correlationId = corr;
        r.code          = static_cast<int16_t>(code);
        const std::size_t fixed = pc::MessageHeader::ENCODED_SIZE + pc::ErrorResponse::BODY_FIXED + 2u;
        r.message = message.substr(0, std::min<std::size_t>({message.size(), 0xFFFFu, maxEgressFrameBytes() - fixed}));
        const std::size_t n = r.encode(egressStage_.data());
        [[maybe_unused]] const bool posted = postEgressFrame(session, egressStage_.data(), n);
        assert(posted && "a clamped ErrorResponse always fits one admin egress message");
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

    // -----------------------------------------------------------------------
    // Producer-side accessors — the gateway/control thread enqueues here.
    // requestRing_ producer = gateway/control duty-cycle thread.
    // controlToAdminRing_ / roleRing_ producer = control thread.
    // -----------------------------------------------------------------------
    OneToOneByteRing& requestRing()   noexcept { return requestRing_; }
    OneToOneByteRing& controlToAdminRing() noexcept { return controlToAdminRing_; }
    OneToOneByteRing& roleRing()      noexcept { return roleRing_;    }
    OneToOneByteRing& egressRing()    noexcept { return egressRing_;   } // admin → control (response frames)
    /// Catalog history Task 12: admin → control catalog-read frames ([sessionId:i64][frame]).
    OneToOneByteRing& catalogReadRing() noexcept { return catalogReadRing_; }

    /// ADMIN thread only (deps.catalogRead): one catalog-read frame
    /// ([CatalogReadFrameHead][frame]).  false = the ring is full — the read
    /// server keeps its position and retries (nothing is queued here).
    [[nodiscard]] bool postCatalogReadFrame(const CatalogReadFrameHead& h, std::span<const uint8_t> frame) {
        const std::size_t n = kCatalogReadHeadBytes + frame.size();
        if (n > static_cast<std::size_t>(catalogReadRing_.maxMsgLength())) [[unlikely]] return false;
        const auto idx = catalogReadRing_.tryClaim(kMsgEgressFrame, n);
        if (idx < 0) return false;
        uint8_t* p = catalogReadRing_.base() + idx;
        std::memcpy(p, &h.session, 8);
        std::memcpy(p + 8, &h.subscriptionId, 8);
        std::memcpy(p + 16, &h.baseOffset, 8);
        if (!frame.empty()) std::memcpy(p + kCatalogReadHeadBytes, frame.data(), frame.size());
        catalogReadRing_.commit(idx);
        return true;
    }

    AdminWorker& worker() noexcept { return worker_; }

    // ── Catalog history Task 7: the production CatalogApplier sinks ──────────
    // ADMIN thread only (the applier is admin-owned).  Every effect becomes one
    // admin→control message on failureRing_ (the control thread is the single
    // producer of every worker ring and the owner of TopicRegistry / placement /
    // the cleaner's policy ring).  Ring full → the preallocated, bounded FIFO
    // catalogBacklog_, flushed every doWork, never dropped; one FIFO for every
    // catalog effect, so their order on the ring is the applier's order.
    //
    // Boot vs live is the SINK, never a flag (F4): the live sink's view installs
    // carry the recovery byte 0 (a commit install), the boot sink's 1 (a recovery
    // install — the workers rebuild from their checkpoints).  Task 16 replaces
    // the boot sink.
    [[nodiscard]] LiveEffects catalogLiveEffects() { return LiveEffects{catalogSink_(/*viewInstallRecovery=*/0)}; }
    [[nodiscard]] BootEffects catalogBootEffects() { return BootEffects{catalogSink_(/*viewInstallRecovery=*/1)}; }
    // ── Authentication Task 9: the production SecurityApplier sinks (ADMIN thread only) ────────────────────────
    // Every applied event's bytes become one kA2cSecurityApplied message on the SAME admin→control FIFO as the
    // catalog's effects (one ring, one order); the applier's halt one kA2cSecurityHalted; a checkpoint storage
    // failure the broker's halt path.  The boot sink is empty by design: the booted state is handed to the control
    // thread's SecurityView directly (the startup thread, before any thread runs) and postBootStash delivers a
    // boot-time halt through the live sink.
    [[nodiscard]] SecurityLiveEffects securityLiveEffects() { return SecurityLiveEffects{securitySink_()}; }
    /// Issues #15 / #111 - ADMIN thread only (deps.securityServing): kA2cSecurityServing on the effect FIFO (never
    /// dropped: the backlog keeps it in order behind the applied events).
    void postSecurityServing(bool caughtUp, int64_t leaderHw, int32_t epoch, bool asLeader, int64_t knownEnd,
                             bool behind, bool held) {
        postCatalog_(kA2cSecurityServing, kSecurityServingWireLen, [&](uint8_t* p) {
            writeSecurityServing(p, caughtUp, leaderHw, epoch, asLeader, knownEnd, behind, held);
        });
    }
    /// Namespaces Phase 2 Task 7 - ADMIN thread only (deps.askControl): the reaper's request to the control thread, on the
    /// effect FIFO behind every applied event it follows (never dropped).
    bool postNsReapRequest(const ReapRequestMsg& m) {
        postCatalog_(kA2cNsReapRequest, kReapRequestWireLen, [&](uint8_t* p) { writeReapRequest(p, m); });
        return true;
    }
    [[nodiscard]] SecurityBootEffects securityBootEffects() const { return SecurityBootEffects{}; }
    [[nodiscard]] SecurityEffects securitySinkForBootStash() { return securitySink_(); }
    /// The `__security` replica's ISR channel (kA2cSecurityAlterIsr) - the control thread forwards it to the controller.
    AdminIsrChannel& securityIsrChannel() noexcept { return securityIsrChannel_; }
    [[nodiscard]] std::size_t catalogBacklogForTest() const noexcept { return catalogBacklog_.count(); }
    [[nodiscard]] std::size_t catalogBacklogBytesForTest() const noexcept { return catalogBacklog_.usedBytes(); }
    [[nodiscard]] uint64_t catalogEffectsRefusedForTest() const noexcept { return catalogEffectsRefused_; }
    int flushCatalogBacklogForTest() { return flushCatalogBacklog_(); }

    /// Catalog history Task 8 — ADMIN thread only (BrokerNode::postCatalogEffectForTest /
    /// holdDroppedDirPurgeForTest, inside onAdminThreadForTest): post one raw catalog
    /// effect frame through the production sink path; hold / release dropped-dir purges.
    void postCatalogFrameForTest(int32_t typeId, const std::vector<uint8_t>& payload) {
        postCatalog_(typeId, payload.size(), [&](uint8_t* p) {
            if (!payload.empty()) std::memcpy(p, payload.data(), payload.size());
        });
    }
    void setHoldDroppedDirPurgeForTest(bool on) noexcept { holdDroppedDirPurge_ = on; }
    [[nodiscard]] std::size_t heldDroppedDirPurgesForTest() const noexcept { return heldPurgeEnds_.size(); }
    [[nodiscard]] uint64_t droppedDirsPurgedForTest() const noexcept { return droppedDirsPurged_; }

    /// Catalog history Task 6: the __catalog replica's ISR channel (admin →
    /// control, kA2cCatalogAlterIsr).  BrokerNode wires it as the replica's
    /// setIsrChannel on the controller-ISR path, before the admin thread starts.
    AdminIsrChannel& catalogIsrChannel() noexcept { return catalogIsrChannel_; }
    /// Role-ring frames refused as malformed / of an unknown type.
    [[nodiscard]] uint64_t badRoleFramesForTest() const noexcept { return badRoleFrames_; }

    /// TEST-ONLY (BrokerNode::onAdminThreadForTest): offer a ring-carried
    /// OwnerThreadTestTask ref to THIS thread.  Many-to-one ring (any test
    /// thread may offer, no lock).  false = ring full.
    [[nodiscard]] bool offerAdminThreadTestTask(void* ref) noexcept {
        return testTaskRing_.offer(kAdminTestTaskMsgTypeId,
                                   static_cast<std::int64_t>(reinterpret_cast<std::uintptr_t>(ref)));
    }
    /// Shutdown (after the admin runner was joined): skip every queued test
    /// task so no test thread waits forever.
    void skipQueuedAdminThreadTestTasks() noexcept {
        (void)testTaskRing_.poll([](int32_t, std::int64_t a, std::int64_t) noexcept {
            skipOwnerThreadTestTask(reinterpret_cast<void*>(static_cast<std::uintptr_t>(a)));
        });
    }

private:
    /// The owning copy of a gateway DdlMetaView (cold: one DDL).
    static DdlMeta ownMeta_(const rune::gateway::DdlMetaView& v) {
        DdlMeta m;
        m.statement.assign(v.statement);
        m.clientName.assign(v.clientName);
        m.sourceAddress.assign(v.sourceAddress);
        m.authenticated.assign(v.authenticated);
        m.sessionId       = v.sessionId;
        m.requestId       = v.requestId;
        if (v.replayedFrom)
            m.replayedFrom = rune::catalog::ReplayedFrom{std::string(v.replayedFrom->clusterId),
                                                         v.replayedFrom->offset,
                                                         std::string(v.replayedFrom->principal)};
        return m;
    }
    static void ownVerifier_(const rune::protocol::client::VerifierWire& w, rune::security::ScramVerifier& out) {
        out.salt.assign(reinterpret_cast<const uint8_t*>(w.salt.data()),
                        reinterpret_cast<const uint8_t*>(w.salt.data()) + w.salt.size());
        out.iterations = w.iterations;
        if (w.storedKey.size() == out.storedKey.size()) std::memcpy(out.storedKey.data(), w.storedKey.data(), out.storedKey.size());
        if (w.serverKey.size() == out.serverKey.size()) std::memcpy(out.serverKey.data(), w.serverKey.data(), out.serverKey.size());
    }
    /// Encode a DDL on the request ring: TooLarge when it can never fit one ring
    /// message (an explicit, permanent answer), RingFull on back-pressure.
    template <class M>
    rune::gateway::AdminSubmit submitDdl_(const M& m) {
        const std::size_t len = ddlWireLen(m);
        if (len == 0 || len > static_cast<std::size_t>(requestRing_.maxMsgLength()))
            return rune::gateway::AdminSubmit::TooLarge;
        return encodeDdl(requestRing_, m) ? rune::gateway::AdminSubmit::Accepted : rune::gateway::AdminSubmit::RingFull;
    }
    [[gnu::cold]] [[gnu::noinline]] void reportBadRequestFrame_(int32_t tid, std::size_t n) noexcept {
        if (badRequestFrames_++ % 1024 == 0)
            spdlog::error("AdminWorkerAgent: request-ring frame type={} len={} refused (malformed; refused so far: {})",
                          tid, n, badRequestFrames_);
    }
    [[gnu::cold]] [[gnu::noinline]] void reportBadRoleFrame_(int32_t tid, std::size_t n) noexcept {
        if (badRoleFrames_++ % 1024 == 0)
            spdlog::error("AdminWorkerAgent: role-ring frame type={} len={} refused (malformed or unknown; "
                          "refused so far: {})", tid, n, badRoleFrames_);
    }

    AdminWorker      worker_;
    SchemaRegistry*  catalogSchemas_;  // admin-owned (the AdminWorker's registry): validator / view-source descriptors
    OneToOneByteRing requestRing_;     // gateway/control → admin (DefineTopic/GetSchema)
    OneToOneByteRing controlToAdminRing_;   // control → admin (Task 8: dropped-dir purges)
    // Catalog history Task 8 (F23): an admin-thread-owned hold under which purge
    // paths queue (preallocated) instead of being removed; releasing it purges
    // them on the next admin pass.
    bool                     holdDroppedDirPurge_ = false;
    // Held paths, packed (fix round 1, M8): one preallocated byte buffer + the
    // end offset of each path (capacity kept; no per-path allocation).
    std::vector<char>        heldPurgeBytes_ = [] { std::vector<char> v; v.reserve(64 * 1024); return v; }();
    std::vector<std::size_t> heldPurgeEnds_  = [] { std::vector<std::size_t> v; v.reserve(256); return v; }();
    uint64_t                 droppedDirsPurged_ = 0;   // admin-owned (test observability via onAdminThreadForTest)
    uint64_t                 badControlFrames_  = 0;
    void onPurgeDroppedDir_(std::string_view path) {
        if (holdDroppedDirPurge_) {
            heldPurgeBytes_.insert(heldPurgeBytes_.end(), path.begin(), path.end());
            heldPurgeEnds_.push_back(heldPurgeBytes_.size());
            return;
        }
        purgeDroppedDir_(path);
    }
    int releaseHeldPurges_() {
        std::size_t begin = 0;
        for (const std::size_t end : heldPurgeEnds_) {
            purgeDroppedDir_(std::string_view(heldPurgeBytes_.data() + begin, end - begin));
            begin = end;
        }
        const int n = static_cast<int>(heldPurgeEnds_.size());
        heldPurgeBytes_.clear();   // capacity kept
        heldPurgeEnds_.clear();
        return n;
    }
    void purgeDroppedDir_(std::string_view path) {
        std::error_code ec;
        const auto removed = std::filesystem::remove_all(std::filesystem::path(path), ec);
        if (ec) {
            spdlog::error("AdminWorkerAgent: purging the dropped topic dir '{}' failed: {} — it stays for the "
                          "boot purge", path, ec.message());
            return;
        }
        ++droppedDirsPurged_;
        spdlog::info("AdminWorkerAgent: dropped topic dir '{}' purged ({} entries)", path,
                     static_cast<uint64_t>(removed));
    }
    OneToOneByteRing roleRing_;        // control → admin (role events)
    OneToOneByteRing egressRing_{kAdminEgressRingBytes};   // admin → control (response frames: [sessionId:i64][frame])
    // Catalog history Task 12: the catalog read server's own egress — never behind
    // the DDL answer gate, never filling the DDL answers' ring.  One frame (a
    // Records of whole catalog batches) always fits the gateway's spill staging.
    OneToOneByteRing catalogReadRing_{kCatalogReadRingBytes};
    static_assert(static_cast<std::size_t>(kCatalogReadRingBytes) / 8 <=
                      rune::gateway::ClientGateway::spillSizing().stage,
                  "catalog read frames must be spillable");
    // Fix round 5: an admin answer (at most the ring's max message, capacity/8)
    // always fits the gateway spill's staging buffer.
    static_assert(static_cast<std::size_t>(kAdminEgressRingBytes) / 8 <=
                      rune::gateway::ClientGateway::spillSizing().stage,
                  "admin egress answers must be spillable");
    // admin → control (T10 storage failure + F3 r6 SchemaUpdate).  Sized like
    // the other blob-carrying admin rings so a descriptor fits one message.
    OneToOneByteRing failureRing_{kAdminToControlRingBytes};
    // Catalog history Task 6: the __catalog replica's AlterIsr → failureRing_
    // (declared after it: initialised with a reference to it).
    AdminIsrChannel  catalogIsrChannel_{failureRing_};
    AdminIsrChannel  securityIsrChannel_{failureRing_, kA2cSecurityAlterIsr};   // authentication Task 9 (D39)
    static constexpr int32_t kAdminTestTaskMsgTypeId = 0x7E58;
    rune::consensus::ControlRing<4096> testTaskRing_;   // test threads → admin (test-only)
    uint64_t         badRoleFrames_ = 0;
    bool             storageFailureSignalled_ = false;
    int64_t          nextPeerDropReportNs_ = 0;   // Task 16: paced report of the admin plane's dropped peer frames
    uint64_t         reportedPeerDrops_    = 0;
    uint64_t         badRequestFrames_ = 0;        // request-ring frames of an unknown type
    // Fix round 2: preallocated, bounded (was an unbounded vector<vector> with
    // O(n) front erase).  4x the ring: one tick's answers fit by construction.
    static constexpr std::size_t kEgressBacklogBytes = 4u * static_cast<std::size_t>(kAdminEgressRingBytes);
    rune::gateway::SpillFifo egressBacklog_{kEgressBacklogBytes};
    rune::storage::ZeroedBytes egressStage_{static_cast<std::size_t>(kAdminEgressRingBytes) / 8};   // ZeroedBytes: memset, not a per-byte vector fill (Debug-build cost)

    // Catalog history Task 7: catalog effects the ring could not take, FIFO.
    // GROWABLE (fix round 1, C1/I1): its volume is input-derived (a boot posts the
    // live catalog), so it never has a fixed bound that could abort; capacity
    // persists (steady state allocates nothing once warmed).
    static constexpr std::size_t kCatalogBacklogInitialBytes = 4u * static_cast<std::size_t>(kAdminToControlRingBytes);
    class CatalogFrameFifo {
    public:
        explicit CatalogFrameFifo(std::size_t initial) { buf_.reserve(initial); }
        [[nodiscard]] bool        empty() const noexcept { return count_ == 0; }
        [[nodiscard]] std::size_t count() const noexcept { return count_; }
        [[nodiscard]] std::size_t usedBytes() const noexcept { return buf_.size() - head_; }
        /// Reserve one frame of `len` bytes; the caller writes it at the returned pointer.
        uint8_t* append(int32_t typeId, std::size_t len) {
            const std::size_t at = buf_.size();
            buf_.resize(at + kHeader + len);
            const auto l = static_cast<uint32_t>(len);
            std::memcpy(buf_.data() + at, &typeId, 4);
            std::memcpy(buf_.data() + at + 4, &l, 4);
            ++count_;
            return buf_.data() + at + kHeader;
        }
        struct Front { int32_t typeId; const uint8_t* data; std::size_t len; };
        [[nodiscard]] Front front() const noexcept {
            int32_t t = 0;
            uint32_t l = 0;
            std::memcpy(&t, buf_.data() + head_, 4);
            std::memcpy(&l, buf_.data() + head_ + 4, 4);
            return Front{t, buf_.data() + head_ + kHeader, l};
        }
        void pop() noexcept {
            head_ += kHeader + front().len;
            if (--count_ == 0) { buf_.clear(); head_ = 0; }   // capacity kept
        }
    private:
        static constexpr std::size_t kHeader = 8;   // [typeId:i32][len:u32]
        std::vector<uint8_t> buf_;
        std::size_t          head_  = 0;
        std::size_t          count_ = 0;
    };
    CatalogFrameFifo catalogBacklog_{kCatalogBacklogInitialBytes};
    uint64_t         catalogEffectsRefused_ = 0;   // admin-owned: frames that could never fit (see postCatalog_)

    [[nodiscard]] std::size_t catalogMaxFrame_() const noexcept {
        return static_cast<std::size_t>(failureRing_.maxMsgLength());
    }

    /// Post one catalog effect frame of `len` bytes (written by `write(uint8_t*)`)
    /// on failureRing_, or into the FIFO backlog behind any earlier one.  Never
    /// aborts (C1): the applier sizes every frame first (CatalogEffectFrames.h —
    /// an oversize effect HALTS apply before it reaches here), so a frame that
    /// still cannot fit is refused loudly and counted, never posted half.
    template <typename Write>
    void postCatalog_(int32_t typeId, std::size_t len, Write&& write) {
        if (len == 0 || len > catalogMaxFrame_()) [[unlikely]] {
            ++catalogEffectsRefused_;
            spdlog::critical("AdminWorkerAgent: catalog effect type={} ({} B) cannot fit one admin→control ring "
                             "message ({} B) — NOT posted (the applier's frame sizing should have halted on it; "
                             "refused so far: {})", typeId, len, catalogMaxFrame_(), catalogEffectsRefused_);
            return;
        }
        if (catalogBacklog_.empty()) {
            const auto idx = failureRing_.tryClaim(typeId, len);
            if (idx >= 0) {
                write(failureRing_.base() + idx);
                failureRing_.commit(idx);
                return;
            }
        }
        write(catalogBacklog_.append(typeId, len));
    }

    int flushCatalogBacklog_() {
        int sent = 0;
        while (!catalogBacklog_.empty()) {
            const auto r = catalogBacklog_.front();
            const auto idx = failureRing_.tryClaim(r.typeId, r.len);
            if (idx < 0) break;
            std::memcpy(failureRing_.base() + idx, r.data, r.len);
            failureRing_.commit(idx);
            catalogBacklog_.pop();
            ++sent;
        }
        return sent;
    }

    void postCatalogTopicApplied_(int32_t topicId, std::string_view name, int32_t partitions, int16_t rf,
                                  rune::catalog::Persistence persistence, const rune::protocol::TopicConfig& c,
                                  uint32_t nsId, int32_t typeId = kA2cCatalogTopicApplied) {
        const std::size_t len = catalogTopicAppliedWireLen(name, c);
        const uint8_t pers = persistence == rune::catalog::Persistence::Transient ? 1 : 0;
        postCatalog_(typeId, len, [&](uint8_t* p) {
            writeCatalogTopicApplied(p, topicId, name, partitions, rf, pers, c, nsId);
        });
    }

    /// The validator fan-out of a typed topic: the materialized descriptor of each registered schema
    /// version fromVersion..current.  Workers select a validator by the record's OWN stamped version and
    /// hard-reject an unknown one, and the typed-transport gate accepts any known version, so a producer
    /// still writing an older version must keep validating after a restart.  A LIVE event passes the
    /// version it created or changed (the workers already hold the earlier ones from earlier posts); BOOT
    /// passes 1 (workers start empty and the ALTER history is never replayed).  Idempotent per worker
    /// (insert_or_assign); cold path (DDL / boot only).
    void postCatalogValidator_(const rune::catalog::TopicObject& t, uint16_t fromVersion) {
        if (!t.schema || !t.schema->validate) return;
        for (uint16_t v = fromVersion; v <= t.schema->schemaVersion; ++v) {
            const RegistryEntry* e = catalogSchemas_->byIdVersion(t.topicId, v);
            if (e == nullptr) [[unlikely]] {
                spdlog::error("AdminWorkerAgent: catalog topic '{}' (id={}) schema v{} not in the SchemaRegistry — "
                              "validator not pushed", t.name, t.topicId, v);
                continue;
            }
            const std::vector<uint8_t> blob = rune::codec::serialize(e->descriptor);
            postCatalog_(kA2cSchemaUpdate, schemaUpdateWireLen(blob.size()), [&](uint8_t* p) {
                writeSchemaUpdate(p, t.topicId, e->version, blob);
            });
        }
    }

    /// Phase 2c: a typed topic's EVOLVING descriptor at its current version, for the gateway's filter validation and
    /// the workers' filter engines.  Idempotent (a re-delivery extends to the same schema); cold (DDL / boot only).
    void postCatalogFilterSchema_(const rune::catalog::TopicObject& t) {
        if (!t.schema) return;
        const auto& b = t.schema->evolvingBlob;
        postCatalog_(kA2cFilterSchema, filterSchemaWireLen(b.size()), [&](uint8_t* p) {
            writeFilterSchema(p, t.topicId, t.schema->schemaVersion, std::span<const uint8_t>(b.data(), b.size()));
        });
    }

    CatalogEffects catalogSink_(uint8_t viewInstallRecovery) {
        CatalogEffects fx;
        fx.maxFrameBytes = catalogMaxFrame_();   // C1: the applier sizes every frame against it
        fx.topicApplied = [this](const rune::catalog::TopicObject& t) {
            // A typed topic's validator follows as schemaChanged (the applier posts both).  Its filter schema goes
            // FIRST: the gateway then validates (and the workers bind) a filter from the moment the topic serves.
            postCatalogFilterSchema_(t);
            postCatalogTopicApplied_(t.topicId, t.name, t.partitions, t.replicationFactor, t.persistence, t.config, t.nsId);
        };
        fx.sinkApplied = [this](const SinkSpec& s) {
            // A view sink is a keyed, compacted table (Task 19's sink spec), every broker (rf 0).
            rune::protocol::TopicConfig c;
            c.cleanupPolicy = 1;   // Compact
            c.partitionKey  = s.keyField;
            postCatalogTopicApplied_(s.topicId, s.name, s.partitions, /*rf*/0, s.persistence, c, s.nsId,
                                     kA2cCatalogSinkApplied);   // Task 12 (N5)
        };
        fx.namespaceUpserted = [this](const rune::catalog::NamespaceRecord& r) {   // namespaces Task 9: login mirror
            postCatalog_(kA2cNamespaceUpserted, namespaceUpsertedWireLen(r.name),
                         [&](uint8_t* p) { writeNamespaceUpserted(p, r); });
        };
        fx.namespaceErased = [this](uint32_t nsId) {   // namespaces Phase 2 Task 3: the mirror forgets it
            postCatalog_(kA2cNamespaceErased, kNamespaceErasedWireLen, [&](uint8_t* p) { writeNamespaceErased(p, nsId); });
        };
        fx.topicDropped = [this](int32_t topicId, uint32_t nsId, const std::string& name) {
            postCatalog_(kA2cCatalogTopicDropped, catalogTopicDroppedWireLen(name),
                         [&](uint8_t* p) { writeCatalogTopicDropped(p, topicId, name, nsId); });
        };
        fx.topicConfig = [this](int32_t topicId, std::span<const rune::catalog::ConfigChange> changes) {
            const std::size_t n = changes.size();
            postCatalog_(kA2cCatalogTopicConfig, n > kMaxCatalogConfigEntries ? 0 : catalogTopicConfigWireLen(n),
                         [&](uint8_t* p) {
                writeCatalogTopicConfigHeader(p, topicId, static_cast<uint16_t>(n));
                for (std::size_t i = 0; i < n; ++i)
                    writeCatalogTopicConfigEntry(p, i, static_cast<uint8_t>(changes[i].key), changes[i].value);
            });
        };
        fx.schemaChanged = [this](const rune::catalog::TopicObject& t, uint16_t fromVersion) {
            postCatalogValidator_(t, fromVersion);
            postCatalogFilterSchema_(t);
        };
        fx.viewInstalled = [this, viewInstallRecovery](const rune::catalog::ViewObject& v) {
            std::vector<uint8_t> src;   // the first source's materialized descriptor (empty: untyped)
            if (!v.def.sourceNames.empty())
                if (const RegistryEntry* e = catalogSchemas_->byName(v.nsId, v.def.sourceNames.front()))
                    src = rune::codec::serialize(e->descriptor);
            const std::span<const uint8_t> plan(v.def.compiledPlanBlob), out(v.def.outputDescriptorBlob), sd(src);
            const uint8_t pers = v.def.persistence == rune::catalog::Persistence::Transient ? 1 : 0;
            postCatalog_(kA2cViewPlanUpdate, viewPlanUpdateWireLen(plan, out, sd), [&](uint8_t* p) {
                writeViewPlanUpdate(p, v.def.viewTopicId, pers, viewInstallRecovery, plan, out, sd);
            });
        };
        fx.viewUninstalled = [this](int32_t viewTopicId) {
            postCatalog_(kA2cViewUninstall, kViewUninstallWireLen,
                         [&](uint8_t* p) { writeViewUninstall(p, viewTopicId); });
        };
        fx.applied = [this](int64_t offset, uint64_t digest) {
            postCatalog_(kA2cCatalogApplied, kCatalogAppliedWireLen,
                         [&](uint8_t* p) { writeCatalogApplied(p, offset, digest); });
        };
        fx.halted = [this](const rune::catalog::ApplyError& err) {
            std::string detail = std::string{rune::catalog::applyErrorName(err.code)} + ": " + err.detail;
            const std::size_t room = catalogMaxFrame_() - kCatalogHaltedFixedBytes;
            if (detail.size() > room) detail.resize(room);
            postCatalog_(kA2cCatalogHalted, kCatalogHaltedFixedBytes + detail.size(), [&](uint8_t* p) {
                writeCatalogHalted(p, err.offset, err.expected, err.actual, detail);
            });
        };
        fx.storageFailed = [this](int e, const std::string& dir) {
            std::string shown = dir;   // keep the path's TAIL (as the log storage-failure post)
            const std::size_t maxDir = catalogMaxFrame_() - 4u;
            if (shown.size() > maxDir) shown = "..." + dir.substr(dir.size() - (maxDir - 3u));
            postCatalog_(kA2cStorageFailure, 4 + shown.size(), [&](uint8_t* p) {
                std::memcpy(p, &e, 4);
                std::memcpy(p + 4, shown.data(), shown.size());
            });
        };
        return fx;
    }

    SecurityEffects securitySink_() {
        SecurityEffects fx;
        fx.maxFrameBytes = catalogMaxFrame_();   // D29: the applier halts on an event that cannot be mirrored in full
        fx.eventApplied = [this](int64_t offset, std::span<const uint8_t> bytes) {
            postCatalog_(kA2cSecurityApplied, kSecurityAppliedFixedBytes + bytes.size(),
                         [&](uint8_t* p) { writeSecurityApplied(p, offset, bytes); });
        };
        fx.halted = [this](const rune::security::ApplyError& err) {
            std::string detail = std::string{rune::security::applyErrorName(err.code)} + ": " + err.detail;
            const std::size_t room = catalogMaxFrame_() - kSecurityHaltedFixedBytes;
            if (detail.size() > room) detail.resize(room);
            postCatalog_(kA2cSecurityHalted, kSecurityHaltedFixedBytes + detail.size(), [&](uint8_t* p) {
                writeSecurityHalted(p, err.offset, err.expected, err.actual, detail);
            });
        };
        fx.storageFailed = [this](int e, const std::string& dir) {
            std::string shown = dir;   // keep the path's TAIL (as the log storage-failure post)
            const std::size_t maxDir = catalogMaxFrame_() - 4u;
            if (shown.size() > maxDir) shown = "..." + dir.substr(dir.size() - (maxDir - 3u));
            postCatalog_(kA2cStorageFailure, 4 + shown.size(), [&](uint8_t* p) {
                std::memcpy(p, &e, 4);
                std::memcpy(p + 4, shown.data(), shown.size());
            });
        };
        return fx;
    }

    // The bound (doWork step 3 pauses intake while the backlog is non-empty) was
    // exceeded: an invariant breach — halt loudly, never drop a DDL answer.
    [[noreturn]] [[gnu::cold]] [[gnu::noinline]] void haltEgressBacklogFull_() const noexcept {
        spdlog::critical("AdminWorkerAgent: DDL egress backlog full ({} answers, {} B) beyond its "
                         "static bound — halting rather than dropping a DDL answer",
                         egressBacklog_.count(), egressBacklog_.usedBytes());
        std::abort();
    }

    /// A backlog key with this bit set is a GATED frame (authentication Task 10): its data is [eventOffset:i64][frame]
    /// and it leaves as kMsgEgressFrameGated.  Session ids are cluster-unique and positive: the top bit is free.
    static constexpr int64_t kGatedKeyBit = std::numeric_limits<int64_t>::min();

    bool tryPostEgress_(int64_t sessionKey, const uint8_t* frame, std::size_t n) {
        const bool gated = (sessionKey & kGatedKeyBit) != 0;
        const int64_t session = sessionKey & std::numeric_limits<int64_t>::max();
        const auto idx = egressRing_.tryClaim(gated ? kMsgEgressFrameGated : kMsgEgressFrame, 8 + n);
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


};

// The drop sequence's two states (catalog history Task 8; BrokerNode::dropRuntimeTopic_).  Namespace
// scope on purpose: GCC cannot default-construct a class nested in BrokerNode that has default member
// initializers while BrokerNode is still incomplete, which made `DroppedTopic`'s default constructor
// (a std::variant of the two) ill-formed and the docker (gcc 13) build fail.
namespace drop_step {   // nested: the generic state names stay out of rune::node
struct AwaitingRetire {                              // step 2: holders still ack the removal
    uint64_t                                 retireGen = 0;
    std::vector<rune::storage::InMemoryLog*> memLogs;   // transient rings, freed with the replicas
};
struct AwaitingRename {                              // step 2: logs closed; the dir rename
    int64_t  retryAtNs = 0;                          // a failed rename backs off (never every pass)
    int64_t  backoffNs = 0;
    uint64_t failures  = 0;
};
}  // namespace drop_step
// Regression pin (docker gcc 13 build): the drop sequence's variant of these two must stay default-constructible.
static_assert(std::is_default_constructible_v<std::variant<drop_step::AwaitingRetire, drop_step::AwaitingRename>>,
              "BrokerNode::DroppedTopic default-constructs its step; keep the states at namespace scope");

// ---------------------------------------------------------------------------
// BrokerNode
// ---------------------------------------------------------------------------
class BrokerNode {
public:
    /// Task 9 fix round 1 (Minor 2): the DdlAnswerGate's "does the gated topic serve"
    /// — a topic the catalog has since DROPPED (a dedupe retry of a define whose topic
    /// was dropped, an AlterView whose sink was since replaced and retired) is a
    /// tombstone: its original OK is released at once, never held to the in-doubt
    /// Internal.  Otherwise the topic must serve (`catalogServes`: built + placed).
    template <class CatalogServes>
    [[nodiscard]] static bool ddlGatedTopicServes(const rune::gateway::TopicRegistry* registry, int32_t topicId,
                                                  CatalogServes&& catalogServes) noexcept {
        if (registry == nullptr) return true;
        if (registry->state(topicId) == rune::gateway::TopicRegistry::TopicState::Dropped) return true;
        return catalogServes(topicId);
    }
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
    /// the RuneMetric::OutOfSyncPartitions row of the metrics file by
    /// publishRole() (read it out-of-band, like the role row).  Observer thread
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
    /// Test-only: during start(), re-run the __catalog log's recovery with this msync errno.
    void setCatalogRecoveryFaultForTest(int err) noexcept { catalogRecoveryFaultForTest_ = err; }
    /// Test-only (concern 4): building the catalog topic `name` (its applied
    /// kA2cCatalogTopicApplied effect) fails — the topic never serves, so its held
    /// DDL OK must be answered Internal (in doubt).  Set before start();
    /// afterwards only the control thread reads it.
    void setCommitReplicaFaultForTest(std::string name) { commitReplicaFault_ = std::move(name); }

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

    /// Publish this node's current role, term and out-of-sync count into the
    /// observer's own rows of the metrics file (RuneMetric::Role / Term /
    /// OutOfSyncPartitions, owner MetricOwner::Observer).  Idempotent + cheap:
    /// call it from the main poll loop on every tick.  No-op before start()
    /// (the rows are allocated there, before any agent thread starts: D59).
    /// Observer thread (see above): one ring-drained snapshot; the rows are the
    /// machine-readable signal the dr-e2e harness reads (via rune_role) instead
    /// of scraping logs.
    void publishRole() noexcept;

    /// The metrics file this node publishes every RuneMetric in (D54); empty before start().  Immutable after start().
    [[nodiscard]] const std::string& metricsFilePath() const noexcept { return metricsPath_; }

    /// Test seam (D58): the owners that have written row `m` so far, one metricOwnerBit per MetricOwner.  Debug
    /// builds only (a release build compiles the record out: always 0).  Any thread (atomic_ref, relaxed).
    [[nodiscard]] std::uint8_t metricWrittenByForTest(RuneMetric m) const noexcept {
        auto& b = const_cast<std::uint8_t&>(metricWrittenBy_[static_cast<std::size_t>(runeMetricRow(m))]);
        return std::atomic_ref<std::uint8_t>(b).load(std::memory_order_relaxed);
    }

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
    //   node started and not stopped; topicId != kCatalogTopicId (-6, the
    //   __catalog replica is AdminWorker-owned — not routable here).
    // Callers may be on any test thread: the control test-task ring is
    // many-to-one (F9a — no test-side mutex).
    // -----------------------------------------------------------------------
    template <class F>
    auto onOwnerThreadForTest(int32_t topicId, int32_t partition, F&& f,
                              std::chrono::milliseconds timeout = std::chrono::seconds{10}) {
        if (topicId == kCatalogTopicId || topicId == kSecurityTopicId) {
            throw std::invalid_argument(
                "onOwnerThreadForTest: __catalog (topicId=-6) and __security (topicId=-7) are AdminWorker-owned; "
                "not routable via the owner-thread hook (use onAdminThreadForTest)");
        }
        return submitOwnerThreadTestTask_<OwnerThreadTestTask::Target::Replica>(
            topicId, partition, std::forward<F>(f), timeout);
    }

    /// Catalog history Task 6: run `f()` on the ADMIN (AdminWorkerAgent) thread —
    /// the owner of the __catalog replica, its Log and its plane.  Same contract as
    /// onOwnerThreadForTest (F9a): the admin thread decides run-or-skip from the
    /// deadline alone; the caller waits for its decision with no bound of its
    /// own.  Precondition: node started (admin worker running), not stopped.
    template <class F>
    auto onAdminThreadForTest(F&& f, std::chrono::milliseconds timeout = std::chrono::seconds{10}) {
        using Impl = detail::OwnerThreadTestTaskImpl<std::decay_t<F>, OwnerThreadTestTask::Target::AdminThread>;
        if (!started_ || stopped_ || !adminWorkerAgent_ || !adminWorkerRunner_) {
            throw std::logic_error(
                "onAdminThreadForTest: node is not running or has no admin worker — no admin "
                "thread to run the task on");
        }
        auto task = std::make_shared<Impl>(kCatalogTopicId, kCatalogPartition, std::forward<F>(f),
                                           OwnerThreadTestTask::Clock::now() + timeout);
        auto* ref = new OwnerThreadTestTaskRef(task);   // the ring's reference
        if (!adminWorkerAgent_->offerAdminThreadTestTask(ref)) {
            delete ref;
            throw std::runtime_error("onAdminThreadForTest: admin test-task ring full");
        }
        return awaitOwnerThreadTestTask(*task, timeout);
    }

    /// Test-only, ADMIN thread only (call inside onAdminThreadForTest): the
    /// __catalog replica (null: none built).
    [[nodiscard]] rune::replication::PartitionReplica* catalogReplicaForTest() const noexcept {
        return catalogReplica_.get();
    }
    /// Test-only: the __catalog Log — ONLY while the admin thread is quiesced
    /// (before start(), after stop()); otherwise read it inside onAdminThreadForTest.
    [[nodiscard]] rune::storage::Log* catalogLogForTest() const noexcept { return catalogLog_.get(); }

    // ── Authentication Task 9 test hooks (`__security`) ──────────────────────────────────────────────────────
    /// Test-only, ADMIN thread only (inside onAdminThreadForTest): the `__security` replica (null: none built).
    [[nodiscard]] rune::replication::PartitionReplica* securityReplicaForTest() const noexcept {
        return securityReplica_.get();
    }
    /// Test-only: the `__security` Log - while the admin thread is quiesced (before start(), after stop()) or inside
    /// onAdminThreadForTest.
    [[nodiscard]] rune::storage::Log* securityLogForTest() const noexcept { return securityLog_.get(); }
    /// What the CONTROL thread's SecurityView holds (read ON the control thread).
    struct SecurityViewSnap { bool present = false; int64_t appliedOffset = -1; uint64_t digest = 0; bool halted = false; bool hasSecurityAdmin = false; };
    [[nodiscard]] SecurityViewSnap securityViewForTest();
    /// Append one record to `__security` on THIS broker's admin thread iff it leads (acks=all); returns the record's
    /// base offset, or -1 (not the leader / below minIsr).  Test seam for the log's replication and mirroring - the
    /// write path proper is SecurityAdmin (Task 10).
    [[nodiscard]] int64_t appendSecurityRecordOnLeaderForTest(std::span<const uint8_t> eventBytes);
    /// Test-only: while held, this broker's `__security` applier applies nothing (the replica still replicates).
    void holdSecurityApplyForTest(bool on);
    /// Test-only: while held, the `__security` leader issues no NamespacePurged (a namespace stays DELETING with its grants).
    void holdSecurityPurgeForTest(bool on);
    /// Test-only (issue #111): cut this broker off the cluster's control plane - the consensus (nothing in, nothing
    /// out) and the control replication transport (nothing in: no metadata, no internal-ISR news) - or reconnect it.
    /// Its client links and admin plane stay up (a frame of the new leader's epoch is refused by its stale replica
    /// anyway): a broker the cluster moves past while it still believes its last metadata (a zombie leader).
    /// Multi-node only (posted to the control thread).
    void partitionFromPeersForTest(bool on);
    /// Test-only: the control thread's SecurityServing state ("CatchingUp" / "Serving" / "Stale"; "none" without auth).
    [[nodiscard]] std::string securityServingForTest();
    /// Test-only (#603), CONTROL thread: the gateway's "`__security` view behind held committed events" flag and the
    /// value it last shipped to the workers.
    struct SecurityBehindSnap { bool gateway = false; bool workers = false; uint64_t refusals = 0; };
    [[nodiscard]] SecurityBehindSnap securityBehindForTest();
    /// Test-only (#603), ADMIN thread: SecurityBehind's verdict and its posted edges.
    struct AdminBehindSnap { bool behind = false; uint64_t edges = 0; };
    [[nodiscard]] AdminBehindSnap adminSecurityBehindForTest();
    /// Test-only (review C I5), CONTROL thread: while held, the mirrored `__security` frames (kA2cSecurityApplied) are
    /// stashed unapplied - the SecurityView, and with it the D24 answer-gate release, stays where it was; on release
    /// they are applied in order.  Proves no OK leaves before THIS broker enforces the event.
    void holdSecurityMirrorForTest(bool on);
    /// Test-only (#203), CONTROL thread: hand `frame` to the kA2cSecurityApplied (or, for kA2cSecurityHalted /
    /// kA2cSecurityServing, that) handler as if it came off the admin-to-control ring, past any mirror hold (held frames
    /// stay held).
    void injectSecurityFrameOnControlForTest(uint16_t tid, std::vector<uint8_t> frame);
    // ── Authentication Task 15 test hooks (inter-broker links) ───────────────────────────────────────────────
    /// What the CONTROL thread's PeerLinkAgent / distribution holds (read ON the control thread).
    struct PeerLinkSnap { bool present = false; int64_t established = 0; uint64_t refusals = 0;
                          uint32_t epochOf[16] = {}; int32_t peerCount = 0; std::vector<int32_t> peers;
                          uint64_t frameDrops = 0; bool sealed = false;
                          uint64_t workerDropsReported = 0, adminDropsReported = 0;
                          uint64_t unknownEpochDrops = 0;   // issue #241: the control-thread transports' UnknownEpoch drops
                          uint64_t consensusAuthFramesOf[16] = {};   // issue #17: frames opened per peer on the consensus ingress
                        };
    [[nodiscard]] PeerLinkSnap peerLinksForTest();
    /// The link epoch worker `w` / the admin thread holds for `peer` (0 = none); read ON that thread.
    [[nodiscard]] uint32_t workerPeerLinkEpochForTest(int32_t w, int32_t peer);
    [[nodiscard]] uint32_t adminPeerLinkEpochForTest(int32_t peer);
    /// Issue #241: frames worker `w` / the admin plane dropped as UnknownEpoch; read ON that thread.
    [[nodiscard]] uint64_t workerPeerUnknownEpochDropsForTest(int32_t w);
    [[nodiscard]] uint64_t adminPeerUnknownEpochDropsForTest();
    /// Issue #241: this (initiator) node re-handshakes its link to `peer` now, same incarnation (control thread).
    /// False: no peer links, or not an Established initiator for `peer`.
    [[nodiscard]] bool forcePeerRekeyForTest(int32_t peer);
    /// Run `f(Worker&)` on worker `w`'s own thread (same contract as onOwnerThreadForTest).
    template <class F>
    auto onWorkerThreadForTest(int32_t w, F&& f, std::chrono::milliseconds timeout = std::chrono::seconds{10}) {
        if (!workerPool_ || w < 0 || w >= workerPool_->workerCount())
            throw std::invalid_argument("onWorkerThreadForTest: no such worker");
        return submitOwnerThreadTestTask_<OwnerThreadTestTask::Target::WorkerThread>(
            -1, w, [this, w, fn = std::forward<F>(f)]() mutable { return fn(workerPool_->worker(w)); }, timeout);
    }
    /// Authentication Task 10: the gateway's session table / auth-gate counters (read ON the control thread).
    [[nodiscard]] bool gatewayHasSessionForTest(int64_t sessionId);
    [[nodiscard]] uint64_t authGateRefusalsForTest();
    /// Test-only (Task 19): requests the gateway answered NotAuthorized (45) since start (control thread, read-only).
    [[nodiscard]] uint64_t gatewayNotAuthorizedForTest();
    /// Test-only (Task 19): the object id of user `name` in this broker's SecurityView (0 = none); control thread, read-only.
    [[nodiscard]] uint64_t securityUserIdForTest(const std::string& name);
    // ── Authentication Task 18 test hooks (each a snapshot taken ON the owning thread) ──
    /// The D24 answer gate's state (read ON the control thread).
    struct SecurityGateSnap { std::size_t held = 0; int64_t applied = -1, refreshed = -1, gatewayEnforced = -1, pendingEnforce = -1; std::size_t shipperPending = 0; };
    [[nodiscard]] SecurityGateSnap securityGateForTest();
    /// The offset the CONTROL thread's SecurityView has applied (-1: no view) - bounds each change's enforcement window.
    [[nodiscard]] int64_t securityAppliedOffsetOnControlForTest();
    /// The shipped permission epoch worker `w` holds for `sessionId` (0: none), read ON that worker's thread.
    [[nodiscard]] uint64_t workerPermissionEpochForTest(int32_t w, int64_t sessionId);
    /// Shard-produce frames worker `w` admitted and has not yet answered (read ON that worker's thread): 0 once every
    /// received frame was answered (the broker-side answered == received).
    [[nodiscard]] std::size_t workerDirectAcksInFlightForTest(int32_t w);
    /// Counter values the control thread publishes (read ON the control thread).
    struct SecurityMetricsSnap { int64_t halted = 0; int64_t applied = -1; int64_t authMode = 0; };
    [[nodiscard]] SecurityMetricsSnap securityMetricsForTest();
    /// Test-only (Task 16): the number of boot replays (CatalogApplier::replayLocal calls that
    /// returned >= 0) since construction — exactly 1 after every start(): the ONE boot replay,
    /// on the startup thread (F4).  Immutable once start() returned.
    [[nodiscard]] int bootReplaysForTest() const noexcept { return bootReplays_; }
    /// Test / bench only (catalog history Task 23, spec §10 gate): the BOOT STEP alone.  Builds a
    /// never-started BrokerNode over `dataDir` (nodeId 1, no peers) and runs bootFromCatalog_() —
    /// the exact function start() step 1b runs — timing that call only (not the node's construction
    /// or destruction).  Startup-thread semantics: no broker thread exists; nothing is shared.
    struct BootReplayForBench {
        int64_t     appliedEvents = 0;   ///< bootApplier_->lastApplied().offset + 1
        bool        halted        = false;
        std::size_t bootTopics    = 0;   ///< bootTopics_.size()  (live topics + live view sinks)
        std::size_t droppedIds    = 0;   ///< bootDroppedIds_.size()
        int64_t     bootNs        = 0;   ///< steady_clock around bootFromCatalog_() only
    };
    [[nodiscard]] static BootReplayForBench bootFromCatalogForBench(const std::string& dataDir);
    /// Test-only (Task 16): true iff the TopicRegistry served topic `topicId` when replication
    /// was built — the catalog's topics are in the registry BEFORE any boot builder runs.
    [[nodiscard]] bool registryHadTopicBeforeReplicationBuildForTest(int32_t topicId) const noexcept {
        return std::find(topicsAtReplicationBuild_.begin(), topicsAtReplicationBuild_.end(), topicId) !=
               topicsAtReplicationBuild_.end();
    }
    /// Test-only (Task 16): the topics / dropped ids the boot replay produced (immutable after start()).
    [[nodiscard]] const std::vector<TopicSpec>& bootTopicsForTest() const noexcept { return bootTopics_; }
    [[nodiscard]] std::size_t nsDirMismatchesAtBootForTest() const noexcept { return nsDirMismatchesAtBoot_; }
    [[nodiscard]] const std::vector<int32_t>& bootDroppedIdsForTest() const noexcept { return bootDroppedIds_; }
    /// Test-only (F6): the open-file need the boot budget estimated from bootTopics_ (immutable after start()).
    [[nodiscard]] uint64_t fdNeedAtBootForTest() const noexcept { return fdNeedAtBoot_; }
    /// Test-only (Task 16): the applier's effects phase ("Live" once the admin thread ran),
    /// read ON the admin thread.
    [[nodiscard]] std::string catalogApplierPhaseForTest() {
        return onAdminThreadForTest([this]() { return std::string{adminWorkerAgent_->worker().applier().phaseName()}; });
    }
    /// Test-only (Task 16, B6): set BEFORE start(), the __catalog applier applies nothing (the
    /// replica still replicates) until holdCatalogApplyForTest(false) — a broker that has received
    /// the leader's prefix but not applied it has NOT caught up, so a deferred boot reconcile
    /// must keep waiting.
    void holdCatalogApplyAtBootForTest(bool on) noexcept { holdCatalogApplyAtBootForTest_ = on; }
    /// Test seam (issue #15), before start(): the `__security` applier applies nothing from the admin thread's first
    /// pass on (the replica still fetches) until holdSecurityApplyForTest(false) - a restarted broker whose local
    /// state is behind the cluster's.
    void holdSecurityApplyAtBootForTest(bool on) noexcept { holdSecurityApplyAtBootForTest_ = on; }
    /// Test seam, before start(): the namespace reaper issues nothing until its budget is raised.
    void holdReaperAtBootForTest(bool on) noexcept { holdReaperAtBootForTest_ = on; }
    /// Catalog history Task 22 (checklist 1): a REAL crash for the 3-node slice - stop() without the
    /// controlled-shutdown protocol (no halt, no consensus hand-off, no goodbye to the members): the
    /// threads are joined and the transport torn down, nothing is answered or flushed on purpose.
    /// Test-thread only, like stop().  Production never calls it.
    /// NOT a transport-level crash (Task 22 fix rounds 1-2, M9/N-M5): the Aeron publications are closed normally, so
    /// PEERS see end-of-stream at once, but a CLIENT session still detects a dead broker only through its own stale-session
    /// threshold (SessionConfig::responseTimeoutNs, 10 s; measured ~13 s end to end by the seed-crash test).  The logs are
    /// destructed with a flush (the page cache survives, as it does for a killed process).  The liveness-timeout path of
    /// peers belongs to the docker e2e.
    void crashForTest() { crashForTest_ = true; stop(); }
    /// Task 22 seam (ADMIN thread via onAdminThreadForTest): hold / release this broker's DDL answers
    /// (AdminWorker::holdAnswersForTest).
    void holdCatalogAnswersForTest(bool on) {
        onAdminThreadForTest([this, on]() { adminWorkerAgent_->worker().holdAnswersForTest(on); return 0; });
    }
    /// Issue #197 seam (CONTROL thread): hold / release the gateway's permission shipper (queued, never sent while held;
    /// releasing ships the backlog at once).
    template <class = void>   // instantiated after onControlThreadForTest is defined
    void holdPermissionShippingForTest(bool on) {
        onControlThreadForTest([this, on]() { if (gateway_) gateway_->holdPermissionShippingForTest(on); return 0; });
    }
    /// Issue #189 seam (CONTROL thread): forget every login-throttle entry of this broker's gateway.
    template <class = void>   // instantiated after onControlThreadForTest is defined
    void resetLoginThrottleForTest() {
        onControlThreadForTest([this]() { if (gateway_) gateway_->resetLoginThrottleForTest(); return 0; });
    }
    /// Namespaces Phase 2 Task 18 seam (CONTROL thread): true iff neither the consumer-offset store nor the transaction-state
    /// store holds a live key of namespace `nsId` (the delete invariant).  Read-only: it counts the keys and writes nothing.
    /// Tri-state: Dirty = a live key; Unknown = a store this node could not read (not the coordinator / not loaded yet), so
    /// nothing is proven; Clean = every store was read and holds none.  Tests wait (bounded) until not Unknown.
    enum class KeyedStoresState : uint8_t { Clean, Dirty, Unknown };
    template <class = void>   // instantiated after onControlThreadForTest is defined
    KeyedStoresState nsKeyedStoresCleanForTest(uint32_t nsId) {
        return onControlThreadForTest([this, nsId]() -> KeyedStoresState {
            bool unknown = false;
            const auto fold = [&](const std::optional<std::size_t>& n) {
                if (!n) unknown = true;
                return n && *n != 0;
            };
            const bool dirty = (groupCoordService_ && fold(groupCoordService_->liveKeysOfNamespaceForTest(nsId))) |
                               (txnCoordService_ && fold(txnCoordService_->liveKeysOfNamespaceForTest(nsId)));
            return dirty ? KeyedStoresState::Dirty : unknown ? KeyedStoresState::Unknown : KeyedStoresState::Clean;
        });
    }
    /// Issues #188 / #5 seam (CONTROL thread): hold / release the gateway's record-egress resolve.
    template <class = void>   // instantiated after onControlThreadForTest is defined
    void holdRecordEgressResolveForTest(bool on) {
        onControlThreadForTest([this, on]() { if (gateway_) gateway_->holdRecordEgressResolveForTest(on); return 0; });
    }
    /// PR #528 review seam (CONTROL thread): this broker answers MetadataRequests as a pre-receiver-key (< 7) broker.
    template <class = void>   // instantiated after onControlThreadForTest is defined
    void emulatePreReceiverKeyMetadataForTest(bool on) {
        onControlThreadForTest([this, on]() { if (gateway_) gateway_->emulatePreReceiverKeyMetadataForTest(on); return 0; });
    }
    /// Task 22: hold / release this broker's catalog applier (a promoted leader stays CatchingUp).
    void holdCatalogApplyForTest(bool on) {
        onAdminThreadForTest([this, on]() { adminWorkerAgent_->worker().holdApplyForTest(on); return 0; });
    }
    /// Task 23 fix round 2 (slice Recipe 3): the NotLeader answers this broker's admin thread gave an in-flight DDL, by cause.
    struct CatalogNotLeaderCounts { uint64_t belowMinIsr = 0, commitTimeout = 0, demoted = 0; };
    [[nodiscard]] CatalogNotLeaderCounts catalogNotLeaderCountsForTest() {
        return onAdminThreadForTest([this]() -> CatalogNotLeaderCounts {
            const auto& w = adminWorkerAgent_->worker();
            return CatalogNotLeaderCounts{w.notLeaderBelowMinIsr(), w.notLeaderCommitTimeout(), w.notLeaderDemoted()};
        });
    }
    /// Task 23 fix round 3 (T7): fabricate one "demoted" NotLeader count on the admin thread (no demotion happened).
    void bumpCatalogNotLeaderDemotedForTest() {
        onAdminThreadForTest([this]() { adminWorkerAgent_->worker().bumpNotLeaderDemotedForTest(); return 0; });
    }
    /// Task 23 fix round 3 (R2-I1): the admin and control threads' max inter-pass gaps since the last reset, each read
    /// ON its owner thread — a harness-side stall signal (a thread unscheduled that long can lose a heartbeat).
    struct PassGaps { int64_t adminMaxGapNs = 0, controlMaxGapNs = 0; };
    template <class = void>   // instantiated after onControlThreadForTest is defined
    [[nodiscard]] PassGaps passGapsForTest() {
        PassGaps g;
        g.adminMaxGapNs   = onAdminThreadForTest([this]() -> int64_t { return adminWorkerAgent_->worker().maxPassGapNs(); });
        g.controlMaxGapNs = onControlThreadForTest([this]() -> int64_t { return adminControl_.maxPassGapNs(); });
        return g;
    }
    /// Resets the pass-gap maxima AND the evidence stamps (evidenceTimesForTest), each on its owner thread.
    template <class = void>
    void resetPassGapsForTest() {
        onAdminThreadForTest([this]() {
            adminWorkerAgent_->worker().resetMaxPassGapForTest();
            adminWorkerAgent_->worker().resetEvidenceTimesForTest();
            return 0;
        });
        onControlThreadForTest([this]() { adminControl_.resetMaxPassGapForTest(); ddlOverdueFirstNs_ = ddlOverdueLastNs_ = 0; return 0; });
    }
    /// Task 23 fix round 4 (R3-M1): when this broker produced topology evidence since the last reset (first / last, ns of the
    /// brokers' MonotonicNanoClock; 0 = none) - the admin thread's by-cause NotLeaders and role-epoch changes, the control
    /// thread's in-doubt gate answers - each read ON its owner thread.
    struct EvidenceTimes { int64_t firstNs = 0, lastNs = 0; };
    template <class = void>
    [[nodiscard]] EvidenceTimes evidenceTimesForTest() {
        const auto a = onAdminThreadForTest([this]() { return adminWorkerAgent_->worker().evidenceTimes(); });
        const auto c = onControlThreadForTest([this]() { return EvidenceTimes{ddlOverdueFirstNs_, ddlOverdueLastNs_}; });
        EvidenceTimes e{a.firstNs, a.lastNs};
        if (c.firstNs != 0 && (e.firstNs == 0 || c.firstNs < e.firstNs)) e.firstNs = c.firstNs;
        if (c.lastNs > e.lastNs) e.lastNs = c.lastNs;
        return e;
    }
    /// Held DDL OKs answered Internal (in doubt) by the DdlAnswerGate (read ON the control thread).
    template <class = void>   // instantiated after onControlThreadForTest is defined
    [[nodiscard]] uint64_t ddlAnswersOverdueForTest() {
        return onControlThreadForTest([this]() -> uint64_t { return ddlAnswersOverdue_; });
    }
    /// The DdlAnswerGate's hold cap (a DDL OK for a not-yet-serving topic is answered Internal "in doubt" after it).
    static constexpr int64_t kDdlAnswerMaxHoldNs = 5'000'000'000LL;
    /// Task 22: DDL events this broker's admin thread appended (read ON the admin thread).
    [[nodiscard]] uint64_t catalogEventsAppendedForTest() {
        return onAdminThreadForTest([this]() -> uint64_t { return adminWorkerAgent_->worker().eventsAppended(); });
    }
    /// Test-only (Task 16): the deferred boot reconcile's state ("None" | "AwaitingCatchUp" |
    /// "Done"), read ON the admin thread.
    [[nodiscard]] std::string bootReconcileStateForTest() {
        return onAdminThreadForTest([this]() { return std::string{adminWorkerAgent_->worker().bootReconcileStateName()}; });
    }
    /// Test-only (Task 16): true once no deferred boot reconcile is waiting (it never was, or it
    /// ran) — bounded by `timeout`.  Needs a running admin thread.
    [[nodiscard]] bool awaitBootReconcileSettledForTest(std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (bootReconcileStateForTest() != "AwaitingCatchUp") return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return bootReconcileStateForTest() != "AwaitingCatchUp";
    }
    /// Test-only: the control (runtime) thread's id — its own runner, or worker
    /// 0's in fused placement.  Recorded by start() on the controller, immutable
    /// until stop(); default id when not running.
    [[nodiscard]] std::thread::id controlThreadIdForTest() const noexcept { return controlThreadId_; }
    /// Test-only: this node's id (immutable config).
    [[nodiscard]] int32_t nodeIdForTest() const noexcept { return config_.nodeId; }

    /// F1: run `f()` on the CONTROL (NodeRuntimeAgent) thread — for test reads of
    /// control-thread-owned state (e.g. ClientGateway::pendingAckCount()).
    template <class F>
    auto onControlThreadForTest(F&& f,
                                std::chrono::milliseconds timeout = std::chrono::seconds{10}) {
        return submitOwnerThreadTestTask_<OwnerThreadTestTask::Target::ControlThread>(
            -1, -1, std::forward<F>(f), timeout);
    }

    // -----------------------------------------------------------------------
    // Catalog history Task 8 test hooks.
    // postCatalogEffectForTest: encode one catalog effect frame (an admin→control
    //   message type kA2c..., its payload) onto the admin→control ring FROM THE
    //   ADMIN THREAD, through the production sink path — exactly as the
    //   CatalogApplier's effects reach the control thread.
    // catalogTopicServesForTest: (control thread) the topic is applied here AND
    //   its placement is committed (every partition assigned in the MSM and its
    //   role routed) — spec §5.4.
    // holdDroppedDirPurgeForTest (F23): an admin-thread-owned hold under which
    //   kMsgPurgeDroppedDir paths queue instead of being removed; releasing it
    //   purges them on the next admin pass.
    // -----------------------------------------------------------------------
    void postCatalogEffectForTest(int32_t typeId, std::vector<uint8_t> payload);   // defined in BrokerNode.cpp
    bool catalogTopicServesForTest(int32_t topicId);
    void holdDroppedDirPurgeForTest(bool on);
    /// Fix round 1 (I2): the cleaner keeps swapping replica views but holds its
    /// ACK — the retire gate (a dropped topic's Logs, collectRetired) must hold.
    void holdCleanerViewAckForTest(bool on);
    /// Fix round 1 (M6): run `f()` on the CLEANER (LogCleanerAgent) thread —
    /// test reads of cleaner-owned state (its policy overrides).  Same contract
    /// as onAdminThreadForTest.
    template <class F>
    auto onCleanerThreadForTest(F&& f, std::chrono::milliseconds timeout = std::chrono::seconds{10}) {
        using Impl = detail::OwnerThreadTestTaskImpl<std::decay_t<F>, OwnerThreadTestTask::Target::CleanerThread>;
        if (!started_ || stopped_ || !cleanerAgent_ || !cleanerRunner_)
            throw std::logic_error("onCleanerThreadForTest: node is not running or has no cleaner");
        auto task = std::make_shared<Impl>(-1, -1, std::forward<F>(f), OwnerThreadTestTask::Clock::now() + timeout);
        auto* ref = new OwnerThreadTestTaskRef(task);
        if (!cleanerAgent_->offerCleanerThreadTestTask(ref)) {
            delete ref;
            throw std::runtime_error("onCleanerThreadForTest: cleaner test-task ring full");
        }
        return awaitOwnerThreadTestTask(*task, timeout);
    }


    /// E2 residual round 5 (R2), TEST-ONLY: a controller answers this node's
    /// node-control identity kNodeSuperseded (as after a late frame of an
    /// unseen older process) — the node ROTATES its process identity.  Runs on
    /// the control thread; returns the identity before the rotation.
    template <class = void>
    int64_t supersedeProcessIdentityForTest() {
        return onControlThreadForTest([this]() -> int64_t {
            if (!runtimeAgent_) return 0;
            auto& cs = runtimeAgent_->controlledShutdown();
            const int64_t id = cs.nonceOnControl();
            rune::replication::AlterIsrResponseMsg r{};
            r.leaderId       = config_.nodeId;
            r.leaderEpoch    = static_cast<rune::replication::Epoch>(static_cast<uint32_t>(static_cast<uint64_t>(id)));
            r.partitionEpoch = static_cast<int32_t>(static_cast<uint32_t>(static_cast<uint64_t>(id) >> 32));
            r.errorCode      = rune::replication::AlterIsrMsg::kNodeSuperseded;
            cs.onResponse(r);
            return id;
        });
    }
    /// Test-only: how many client sessions the gateway ever accepted (read on the control thread).
    template <class = void>
    uint64_t testConnectsAccepted() {
        return onControlThreadForTest([this]() -> uint64_t { return gateway_ ? gateway_->sessionsAllocated() : 0; });
    }
    template <class = void>
    int64_t processIdentityForTest() {
        return onControlThreadForTest([this]() -> int64_t {
            return runtimeAgent_ ? runtimeAgent_->controlledShutdown().nonceOnControl() : 0;
        });
    }

    /// Test-only, CONTROL thread only (call inside onControlThreadForTest):
    /// the durable __cluster_metadata ISR knowledge (round-4 LOW-B tests).
    [[nodiscard]] const rune::replication::InternalIsrKnowledge* internalIsrKnowledgeForTest() const noexcept {
        return internalIsrKnowledge_.get();
    }
    /// Test-only, CONTROL thread only: the consensus Election (nullptr when none).
    [[nodiscard]] const rune::consensus::Election* electionForTest() const noexcept { return election_.get(); }
    // ISR v3 Step 9 test seams (the first two: call inside onControlThreadForTest).
    /// The flags the next CanvassPosition carries (R12): 2 = valid, | 1 = this process has history; -1 = no publisher.
    /// Test-only, CONTROL thread: does this node abstain from other candidates' ballots right now?
    [[nodiscard]] int64_t foundingLongDwellNsForTest() const noexcept { return storageGate_ ? storageGate_->longDwellNs() : -1; }
    [[nodiscard]] bool abstainsForTest() noexcept { return abstainGateThunk_(this); }
    [[nodiscard]] int32_t canvassFlagsForTest() const noexcept { return publisher_ ? publisher_->canvassFlags() : -1; }
    /// StorageGate::stateCode(): 0 CONFIRMED, 1 UNCONFIRMED, 2 UNDECIDED (0 on a single node).
    [[nodiscard]] int storageGateStateForTest() const noexcept { return storageGate_ ? storageGate_->stateCode() : 0; }
    /// Hold / release the candidacy regardless of the gate (posted to the control thread).
    template <class = void>   // instantiated after onControlThreadForTest is defined
    void setTestCandidacyHold(bool hold) {
        (void)onControlThreadForTest([this, hold]() { if (runtimeAgent_) runtimeAgent_->setTestCandidacyHold(hold); return 0; });
    }
    /// ISR v3 Step 10: hold / release EVERY controller decision of this node (MetadataService::setHoldAllDecisionsForTest;
    /// posted to the control thread).  Needs NodeConfig::testHoldAllControllerDecisionsMs > 0.
    template <class = void>
    void setControllerDecisionHoldForTest(bool on) {
        (void)onControlThreadForTest([this, on]() { if (metadataService_) metadataService_->setHoldAllDecisionsForTest(on); return 0; });
    }
    /// PR #490 review: hold this broker's metadata apply (holdMetadataApplyForTest).
    template <class = void>
    void holdMetadataApplyForTest(bool on) {
        (void)onControlThreadForTest([this, on]() { if (metadataService_) metadataService_->holdMetadataApplyForTest(on); return 0; });
    }
    /// The consensus leader (this node, if it is) resigns (posted to the control thread).  Returns whether the resign
    /// took effect (false: not the leader, or no election).
    template <class = void>
    bool resignConsensusForTest() {
        return onControlThreadForTest([this]() -> bool {
            return election_ && election_->resign(std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count());
        });
    }
    /// The consensus leader (this node, if it is) stands down for `holdMs` (Election::standDown; posted to the
    /// control thread).  Unlike a resign it stands again once the hold ends (PR #601 review: leader flap).
    template <class = void>
    bool standDownConsensusForTest(int64_t holdMs) {
        return onControlThreadForTest([this, holdMs]() {
            return election_ && election_->standDown(std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count(), holdMs * 1'000'000);
        });
    }
    /// CP1-I1: the replication transport (control-thread state; read it through onControlThreadForTest).
    [[nodiscard]] rune::replication::AeronPartitionTransport* replTransportForTest() const noexcept { return replTransport_.get(); }

    /// Test-only: true if the replication metadata bridge has propagated this
    /// node's leadership to its internal PartitionReplicas.  Read ON the control
    /// thread (the bridge's owner) — fix-on-sight (catalog history Task 6, hard
    /// rule 1): no cross-thread atomic mirror.  Not running → false (the
    /// bridge observed nothing yet / is gone).  Never call it from a task that
    /// already runs on the control thread.
    template <class = void>   // instantiated after submitOwnerThreadTestTask_ is defined
    bool replicationIsLeaderForTest() {
        requireNotOnControlThread_("replicationIsLeaderForTest");
        if (!started_ || stopped_ || !runtimeAgent_ || !replBridge_) return false;
        return onControlThreadForTest([this] { return replBridge_ && replBridge_->isLeader(); });
    }

    /// Test-only: the last cluster epoch the bridge applied (control thread, as
    /// above).  Not running → 0.
    template <class = void>
    rune::replication::Epoch replicationEpochForTest() {
        requireNotOnControlThread_("replicationEpochForTest");
        if (!started_ || stopped_ || !runtimeAgent_ || !replBridge_) return 0;
        return onControlThreadForTest([this]() -> rune::replication::Epoch {
            return replBridge_ ? replBridge_->lastEpoch() : 0;
        });
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
    /// Task 12 fix round 0 — control thread only (onControlThreadForTest).
    [[nodiscard]] const CatalogReadFlow& catalogReadFlowForTest() const noexcept { return catalogReadFlow_; }
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
    /// Test-only (catalog history Task 8): the cleaner — CLEANER-thread-owned:
    /// read it only inside onCleanerThreadForTest.
    const LogCleanerAgent* cleanerForTest() const noexcept { return cleanerAgent_.get(); }

    /// Test-only: return the SchemaRegistry (for byName/byId queries in tests).
    /// Non-null after start() if schema persistence construction succeeded.
    /// NOT safe for concurrent use — call only when admin thread is quiesced or
    /// the node is stopped.
    SchemaRegistry* schemaRegistryForTest() const noexcept {
        return schemaRegistry_.get();
    }

    /// Test-only: return the ViewRegistry (for byName/byViewTopicId queries in
    /// tests). Non-null after start(). NOT safe for concurrent use — call only when the admin
    /// thread is quiesced or the node is stopped (mirrors schemaRegistryForTest).
    ViewRegistry* viewRegistryForTest() const noexcept {
        return viewRegistry_.get();
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
    /// Review B-I2 test seams (cold).  setMtuProbeForTest before start(): the watchdog probes `f` instead of the
    /// interface (and runs even on an all-loopback node).  runMtuWatchdogForTest: one forced probe ON the control
    /// thread; returns the deficit after it.  mtuWatchdogEdgesForTest: {below edges, restored edges}.
    void setMtuProbeForTest(std::function<int32_t()> f) { mtuProbeForTest_ = std::move(f); }
    /// Review B-M5 test seam (cold, before start()): edits the probed host facts before they are applied.
    void setHostNetFactsOverrideForTest(std::function<void(HostNetFacts&)> f) { hostFactsOverrideForTest_ = std::move(f); }
    /// Re-check I5 test seam (cold): while held, this node's transient lineage reads UNKNOWN (as right after a
    /// restart, before its metadata replica applied the new lineage).  Written on the control thread (hop).
    void holdTransientLineageForTest(bool hold);
    /// Re-check I4 test seam (cold, before start()): worker `w`'s egress is treated as unresolved AFTER its bind
    /// anchor bound (a driver timeout / channel clash) — reaches the buildWorkerPool / attachShardRuntimes_ backstops.
    void forceWorkerEgressUnresolvedForTest(int32_t w) noexcept { forceEgressUnresolvedForTest_ = w; }
    /// Re-check M2: the anchor channels resolved at start (subscriptions, then publications).
    [[nodiscard]] const std::vector<std::string>& bindAnchorChannelsForTest() const noexcept { return bindAnchorChannels_; }
    /// Review A-I5: the bind anchors resolved at start (subscriptions + MDC publications).
    [[nodiscard]] std::size_t bindAnchorsForTest() const noexcept { return bindAnchorSubs_.size() + bindAnchorPubs_.size(); }
    int32_t runMtuWatchdogForTest();
    std::pair<uint64_t, uint64_t> mtuWatchdogEdgesForTest();
    [[nodiscard]] std::size_t shardDriverCountForTest() const noexcept {
        std::size_t n = 0;
        for (const auto& d : shardDrivers_) n += d ? 1u : 0u;
        return n;
    }
    /// Test-only (R-M1): TopicDropped ops the control thread delivered to worker rings (one per worker per
    /// drop) / still pending.  Control-thread state: read inside onControlThreadForTest.
    [[nodiscard]] uint64_t topicDroppedEnqueuedForTest() const noexcept { return topicDroppedFanout_.enqueuedForTest(); }
    [[nodiscard]] std::size_t topicDroppedPendingForTest() const noexcept { return topicDroppedFanout_.pendingForTest(); }
    [[nodiscard]] int32_t workerCountForTest() const noexcept { return workerPool_ ? workerPool_->workerCount() : 0; }
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
    /// wired into the election path (views recover from the boot replay, see bootFromCatalog_ / the Task-19b
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

    /// Test-only fault: after shard `worker`'s driver is built, block the start thread for `stall` (a slow driver start).
    void setShardDriverBuildStallForTest(int32_t worker, std::chrono::milliseconds stall) noexcept {
        shardDriverBuildStallWorkerForTest_ = worker;
        shardDriverBuildStallForTest_       = stall;
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
    // Fix round 0 (Task 6): a control-thread round trip awaited ON the control
    // thread would wait for itself forever — refuse loudly instead.
    void requireNotOnControlThread_(const char* what) const {
        if (controlThreadId_ != std::thread::id{} && std::this_thread::get_id() == controlThreadId_) {
            spdlog::critical("BrokerNode[{}]: {} called ON the control thread — it posts a control-thread "
                             "task and would wait for itself forever (deadlock); refused",
                             config_.nodeId, what);
            throw std::logic_error(std::string(what) + ": called on the control thread (would deadlock)");
        }
    }
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
    /// Phase 2c: the NodeConfig filter knobs as the workers' FilterConfig.
    [[nodiscard]] rune::node::filter::FilterConfig filterConfig_() const noexcept {
        rune::node::filter::FilterConfig c;
        c.maxSubsPerPartition        = config_.filterMaxSubsPerPartition;
        c.maxIndexValuesPerPartition = config_.filterMaxIndexValuesPerPartition;
        c.noteIntervalNs             = static_cast<int64_t>(config_.filterNoteIntervalMs) * 1'000'000;
        c.noteRecords                = static_cast<uint64_t>(config_.filterNoteRecords);
        c.versionWaitNs              = static_cast<int64_t>(config_.filterVersionWaitMs) * 1'000'000;
        c.matchScratch               = config_.filterMatchScratch;
        return c;
    }
    void buildAdminWorker();   // Task 6 / 16: the __catalog replica + AdminWorker thread (adopts the booted applier)
    // Catalog history Task 6: dataDir/__catalog Log + replica + its replication
    // plane (admin-thread-owned).  Called from buildAdminWorker before the
    // schema block.  Throws on any failure (no silent fallback: a broker
    // without its catalog never starts).
    void buildCatalog_();
    // Authentication Task 9 (D39): dataDir/__security Log + replica on the SAME admin plane as __catalog's (built right
    // after it; shares its transport and controller-ISR decision).  The startup thread replays the local prefix once
    // (bootFromSecurity_) and hands the booted state to the control thread's SecurityView.
    void bootFromSecurity_();
    void buildSecurity_();
    void forwardSecurityAlterIsr_(const uint8_t* p, std::size_t n);
    static bool onAdminLogAlterIsrResponseThunk_(void* self, const rune::replication::AlterIsrResponseMsg& r) noexcept;
    void onSecurityApplied_(const uint8_t* p, std::size_t n);
    void failSecurityClosed_(int64_t offset, const std::string& describe);   // Task 14 (D28)
    void haltSecurityMirrorOnMalformed_(const char* what, std::size_t n);   // #203: an unparseable frame fails closed
    void onSecurityHalted_(const uint8_t* p, std::size_t n);
    // Issues #15 / #111 (decision M7): the SecurityServing state (control thread).  kA2cSecurityServing feeds the admin
    // thread's catch-up verdict; every control pass steps it with the consensus contact (the election CLOSED); a change
    // is published to the gateway (logins / -7 reads / security DDL answer AuthUnavailable unless Serving), and Stale
    // longer than R ends the authenticated sessions (48, session level).
    void onSecurityServing_(const uint8_t* p, std::size_t n);
    int stepSecurityServing_(int64_t nowNs);
    std::optional<SecurityServing> securityServing_;   // engaged iff the broker authenticates (gateway Required)
    int64_t controlNowNs_ = 0;   // PR #459 M2: the current control pass's clock (serviceAdminControl_), control thread
    void publishSecurityMetrics_() noexcept;
    void publishLoginTallies_() noexcept;   // control thread, every pass: the gateway's login tallies (D59)
    int  serviceAuthMode_(int64_t nowNs);
    // Authentication Task 15 (D33): the inter-broker link handshakes.  buildPeerLinks_ (startup thread) builds the
    // PeerLinkAgent when authMode == Enabled and there is more than one peer; servicePeerLinks_ (control thread, every
    // duty cycle) drives it, distributes newly established keys and publishes counters 233 / 234.
    void buildPeerLinks_();
    int  servicePeerLinks_(int64_t nowNs);
    void publishPeerLinkMetrics_() noexcept;
    static void onPeerLinkEstablishedThunk_(void* self, int32_t peer, const rune::peerlink::LinkKeys& k, int64_t nowNs);
    void onPeerLinkEstablished_(int32_t peer, const rune::peerlink::LinkKeys& k, int64_t nowNs);
    // Catalog history Task 6 (control thread): a kA2cCatalogAlterIsr frame →
    // the controller channel; the controller's (-6, 0) answer → the admin role ring.
    void forwardCatalogAlterIsr_(const uint8_t* p, std::size_t n);

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
    void    runPendingCreatesBehindDrop_();   ///< run, in arrival order, every waiting create whose name no drop holds

    // -----------------------------------------------------------------------
    // Catalog history Task 8 — the control thread executes the CatalogApplier's
    // effects (admin→control ring).  Every handler is IDEMPOTENT: a crash
    // between the applier's checkpoint and the effects re-runs them at the next
    // boot, and a repeated create / drop / config / uninstall must leave the
    // same final state (never a duplicate, an error or a double placement).
    //
    // The drop sequence, one explicit state per step (rule 3):
    //   1. dropRuntimeTopic_: TopicRegistry::dropTopic (serves nothing; the
    //      gateway answers UnknownTopic and ends the topic's subscriptions),
    //      MetadataService::removeRuntimeTopic (placement removal), every
    //      partition's replica fenced + removed (transient ring with it), the
    //      cleaner's override cleared → DroppedTopic{AwaitingRetire{retireGen}}.
    //   2. serviceDroppedTopics_ (each pass): once every worker and the cleaner /
    //      provisioner acked a generation >= retireGen (collectRetired's bound),
    //      the Logs are closed (releaseDroppedLogs) → AwaitingRename; then
    //      rename(ns-<nsId>/topics/<name>, ns-<nsId>/dropped/<id>.<name>) — atomic; a failure is
    //      logged and retried next pass, never deleted in place — and the path
    //      is posted to the admin thread (kMsgPurgeDroppedDir), which removes it
    //      (a failure leaves it for the boot purge: reconcileTopicDirs, Task 16).
    //   3. A create whose name a DroppedTopic still holds waits as a
    //      PendingCreateBehindDrop{spec} (FIFO per name) and runs right after
    //      that name's rename succeeded: the reused name starts EMPTY.
    // -----------------------------------------------------------------------
    void onCatalogTopicApplied_(const uint8_t* p, std::size_t n, bool isView = false);
    void runCatalogCreate_(const TopicSpec& spec);
    void dropRuntimeTopic_(int32_t topicId, uint32_t nsId, const std::string& name);
    void onCatalogTopicConfig_(const uint8_t* p, std::size_t n);
    void onViewUninstall_(int32_t viewTopicId);
    int  serviceDroppedTopics_();
    int  pumpCatalogOutboxes_();
    [[nodiscard]] bool catalogNameHeldByDrop_(uint32_t nsId, const std::string& name) const noexcept;
    [[nodiscard]] bool catalogTopicServes_(int32_t topicId) const noexcept;

    // -----------------------------------------------------------------------
    // Configuration (immutable after construction)
    // -----------------------------------------------------------------------
    NodeConfig config_;
    /// ISR design v3 §3.2 (Step 5): this data dir's storage identity, resolved in start() before anything
    /// writes into the data dir.  Owned by the starting thread until the threads exist; later steps hand it
    /// to its one owner (the storage gate).
    StorageIdentity::Loaded storage_;
    /// E2 final fix wave (C1, USER DECISION option b): a transient partition's
    /// data lineage is COMMITTED METADATA (TransientLineageRecord) — the
    /// coordinator answers from the lineage confirmed for the partition's
    /// current leadership, and "pending" (a retriable answer) until it is.
    /// processIncarnation_: a random non-zero token per process — the
    /// single-node controller's lineage authority (every transient ring there
    /// lives exactly as long as the process).
    int64_t processIncarnation_ = 0;
    /// E2 C1 residual N1: this process's node-control identity (multi-node) —
    /// registered with every controller (ControlledShutdown) and stamped on
    /// every replica's lineage report.  Drawn once at construction.
    int64_t lifecycleNonce_ = 0;
    void ensureProcessIncarnation_();
    static uint64_t transientLineageTrampoline_(void* ctx, int32_t topicId, int32_t partition) noexcept;
    static bool isTransientTopicTrampoline_(void* ctx, int32_t topicId) noexcept;
    /// ISR v3 Step 6: the storage manifest markers.  Each runs once, on the control thread (cold fsync), when the
    /// class first exists durably: the internal-ISR knowledge file / the first committed __cluster_metadata apply.
    void markConsensusWhenPresent_();
    bool    consensusUnmarked_ = true;          ///< the Consensus class is not in the manifest yet
    int64_t lastConsensusMarkCheckNs_ = 0;
    /// ISR v3 R17: may a claim over an EMPTY metadata log bootstrap a new one?  Read live at the claim edge (control
    /// thread; storage_ is the one Loaded object): only a Founder whose manifest never recorded a metadata log.
    static bool mayBootstrapThunk_(void* self) noexcept;
    static void markKnowledgeThunk_(void* self) noexcept;
    /// ISR v3 Step 9 (§3.6): the consensus vote gate - the storage gate first, then the metadata-ISR knowledge; with
    /// RUNE_STORAGE_RECOVERY_VOTE an unconfirmed node votes for what its knowledge ever learned (critical log).
    static bool voteGateThunk_(void* self, int32_t candidate) noexcept;
    static bool abstainGateThunk_(void* self) noexcept;
    [[nodiscard]] int64_t foundingShortDwellNs() const noexcept;
    [[nodiscard]] int64_t foundingLongDwellNs() const noexcept;
    void logRecoveryVote_(int32_t candidate) noexcept;
    int publishStorageMetrics_() noexcept;
    std::unique_ptr<StorageGate> storageGate_;   ///< multi-node only (null = open everywhere); control thread
    bool    bootHistory_ = false;                ///< R12: node-state.dat / term.log held history at boot
    int32_t lastRecoveryVoteCandidate_ = -1;
    int64_t lastRecoveryVoteTerm_      = -1;
    static void markMetadataLogThunk_(void* self) noexcept;
    static uint64_t transientLineageVersionTrampoline_(void* ctx) noexcept;   // R1 (round 5)

    // Task 9 (RUNE_TLS SIGHUP reload): the trigger-only cross-thread ring —
    // requestTlsCertReload() (any thread) offers kTlsReloadTrigger;
    // serviceAdminControl_() (control thread) polls it and performs the
    // actual file re-read + gateway_->reloadTlsCert() swap itself, on its
    // own thread.  No PEM text or gateway state ever crosses the ring.
    static constexpr std::int32_t kTlsReloadTrigger = 1;
    rune::consensus::ControlRing<256> tlsReloadRing_;
    // Task 6 fix round 1 (M5): stop() (controller) → control thread: the admin
    // runner is joined — the catalog answer relay stops waiting for it.
    static constexpr std::int32_t kAdminStoppedSignal = 1;
    rune::consensus::ControlRing<256> adminLifecycleRing_;
    // Control thread only (set from adminLifecycleRing_).
    bool                              adminDrainStopped_ = false;
    uint64_t                          catalogAnswersHeld_    = 0;   // ABORTed: role ahead / ring full
    uint64_t                          catalogAnswersDropped_ = 0;   // admin thread gone
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
    // Topic registry (GB-1) — per-partition Logs, built from the catalog at boot and by DDL.
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
    // Catalog history Task 6 — the __catalog partition (topic -6, partition 0).
    // catalogLog_          — durable Log at dataDir/__catalog (never InMemoryLog).
    // catalogPlane_        — its replication plane: a WorkerReplicationTransport
    //                        on stream config_.catalogReplStreamId over the
    //                        node's replication channel (multi-node only).
    // catalogAddBroker_    — the plane's control-thread re-add broker (DR-15).
    // catalogReplica_      — the PartitionReplica, owned HERE (never in
    //                        replicaRegistry_), driven ONLY by the admin thread.
    // catalogOnController_ — immutable after build: true = its role comes from
    //                        the controller-ISR path (multi-node), false = the
    //                        consensus leader (single node / no transport).
    // Declared before adminWorkerAgent_ (which holds raw pointers to them).
    // -----------------------------------------------------------------------
    std::unique_ptr<rune::storage::Log>                   catalogLog_;
    std::unique_ptr<rune::replication::WorkerReplicationTransport> catalogPlane_;
    std::unique_ptr<rune::replication::PubAddBroker>      catalogAddBroker_;
    std::unique_ptr<rune::replication::PartitionReplica>  catalogReplica_;
    bool                                                  catalogOnController_ = false;
    // Authentication Task 9 - the `__security` partition (topic -7, partition 0): the same ownership as __catalog's.
    // securityLog_/securityReplica_ are admin-thread-owned once the admin thread runs (never in replicaRegistry_);
    // bootSecurityApplier_ is the startup thread's booted applier until buildAdminWorker hands it to the AdminWorker;
    // securityView_ is the CONTROL thread's own mirror (constructed on the startup thread from a COPY of the booted
    // state, before any thread runs).
    std::unique_ptr<rune::storage::Log>                   securityLog_;
    std::unique_ptr<rune::replication::PartitionReplica>  securityReplica_;
    std::optional<SecurityApplier>                        bootSecurityApplier_;
    std::optional<rune::gateway::SecurityView>            securityView_;
    /// Review C I4: the cluster key(s), loaded ONCE in start() (nullopt unless authMode == Enabled); every component
    /// (gateway now, peer links / frame crypto in Tasks 15-16) takes a copy of THIS.
    std::optional<rune::peerlink::ClusterKeys>            clusterKeys_;
    struct HeldMirror { bool on = false; std::vector<std::pair<int32_t, std::vector<uint8_t>>> frames; };   // test seam (control thread): (ring type, bytes)
    HeldMirror                                            heldMirror_;
    uint64_t                                              securityAnswersHeld_    = 0;
    uint64_t                                              securityAnswersDropped_ = 0;
    uint64_t                                              securityAlterIsrRefused_ = 0;
    std::int64_t                     securityHaltedValue_  = 0;    // control-thread-owned (0 = applying)
    std::int64_t                     securityAppliedValue_ = -1;   // control-thread-owned
    DevModeWarnTimer                 devModeWarn_;                 // control thread
    std::unique_ptr<PeerLinkAgent>   peerLinkAgent_;               // control thread (null: no peer links)
    PeerLinkFanout                   peerLinkFanout_;              // control thread: keys -> workers + admin, over rings
    // Task 16 (D34): the inter-broker frame crypto of a transport this broker builds - PeerSealed for Enabled with more
    // than one peer (sender id: consensus 0, legacy replication 1, admin plane 2, worker w -> 3 + w), else PeerPlain.
    [[nodiscard]] bool sealsPeerFrames_() const noexcept {
        return config_.authMode == AuthMode::Enabled && config_.peers.size() > 1;
    }
    [[nodiscard]] rune::peerlink::PeerCryptoMode peerCrypto_(uint8_t senderId) const {
        if (sealsPeerFrames_()) return rune::peerlink::PeerFrameCrypto(config_.nodeId, senderId);
        return rune::peerlink::PeerPlain{};
    }
    [[nodiscard]] uint64_t peerFrameDropsTotal_() const noexcept;   // control thread: own transports + reported ones
    static void onWorkerPeerFrameDropsThunk_(void* self, int32_t worker, uint64_t drops);
    std::vector<uint64_t>            workerPeerDrops_;             // control thread: the workers' last reports
    uint64_t                         adminPeerDrops_ = 0;          // control thread: the admin plane's last report
    // Catalog history Task 16 — the boot from __catalog (startup thread; immutable after start()).
    /// Opens/replays the local __catalog on the startup thread and fills the members below.
    void bootFromCatalog_();
    rune::gateway::ObjectIndex                            bootObjects_;          // authentication Task 12: the same set as (id, name, kind), handed to the gateway
    std::vector<TopicSpec>                                bootTopics_;           // the catalog's live topics + view sinks (topic-id order)
    std::vector<int32_t>                                  bootDroppedIds_;       // every topic id the catalog dropped
    std::optional<CatalogApplier>                         bootApplier_;          // Booted; handed to the AdminWorker by buildAdminWorker
    uint64_t                                              fdNeedAtBoot_ = 0;     // estimateFdNeed(bootTopics_) at start()
    uint64_t                                              fdSoftLimit_ = 0;      // #296: soft RLIMIT_NOFILE after start()'s fd step (0 = unknown)
    int                                                   bootReplays_ = 0;      // replayLocal calls that returned >= 0
    bool                                                  bootReconcileDeferred_ = false;   // B6: the admin thread reconciles after catch-up
    bool                                                  holdCatalogApplyAtBootForTest_ = false;
    bool holdSecurityApplyAtBootForTest_ = false;   // issue #15 test seam (startup thread, before start)
    bool holdReaperAtBootForTest_ = false;          // test seam (startup thread, before start)
    std::vector<int32_t>                                  topicsAtReplicationBuild_;   // serving topic ids when replication was built
    std::thread::id                                       controlThreadId_;   // start() (controller); see controlThreadIdForTest
    // Control thread: the last consensus-driven catalog role posted (single node).
    struct PostedRole { bool leader = false; int32_t epoch = -1; };
    PostedRole                                            catalogLastRole_;
    PostedRole                                            securityLastRole_;   // auth Task 9: __security (single node: the consensus leader)
    // Catalog history Task 11: the DDL route last published to the gateway, as
    // its key (CatalogRoute index + the leader id / halt offset) — the endpoint
    // string is built only on a change (an idle control pass allocates nothing).
    struct PublishedCatalogRoute { std::optional<rune::gateway::RouteKind> kind; int64_t key = 0; };
    PublishedCatalogRoute                                 catalogRoutePublished_;
    // Authentication Task 10 (D24/D36), control thread: the `__security` DDL route last published to the gateway and the
    // answer gate that holds a security DDL's OK until THIS broker enforces the event.
    PublishedCatalogRoute                                 securityRoutePublished_;
    SecurityAnswerGate                                    securityGate_;
    int64_t                                               securityRefreshedOffset_ = -1;   // Task 13/14: the permission refresh of the event at this offset reached every worker
    int publishSecurityRoute_(bool consensusLeader);
    int pumpSecurityGate_();
    /// Control thread: recompute the DDL route; publish it on change.
    int publishCatalogRoute_(bool consensusLeader);
    /// Control thread (Task 11 fix round 1, I2): post the placement view to the
    /// admin thread (kMsgPlacementView) on change only; and the unplaceable-topics gauge.
    int publishPlacementView_();
    /// Control thread (Task 11 fix round 4): a user partition the committed map
    /// assigns HERE that this broker has not built is reported to the controller
    /// LIVE, every kUnservableReportPeriodNs (kBumpReplicaUnservable) — at once
    /// when the catalog apply halted, else after kUnbuiltReportGraceNs.  The
    /// controller only excludes it from that partition's leader election while
    /// the reports keep coming; it stops when the replica is built.
    int reportUnbuildableReplicas_();
    static constexpr int64_t kUnbuiltReportGraceNs    = 10'000'000'000LL;
    static constexpr int64_t kUnservableReportPeriodNs = 500'000'000LL;   // TTL on the controller: 4 periods
    struct UnbuiltReplica { int64_t firstSeenNs = 0; int64_t lastSentNs = 0; };
    std::map<int64_t, UnbuiltReplica> unbuilt_;   // (topic<<32|partition) → since
    uint64_t unbuiltScanVersion_ = ~0ull;
    bool     unbuiltScanHalted_  = false;
    int64_t  unbuiltLastPassNs_  = 0;
    int64_t  unbuiltResumeKey_   = 0;   // review M1: the next pass starts at the first refused key
    /// TEST-ONLY seam (control thread): hold this broker's catalog-halt broadcast
    /// (forces the placement race of fix round 2: a create placed on a broker that
    /// halts on it before any member knows).
    bool holdHaltReportForTest_ = false;
public:
    void holdCatalogHaltReportForTest(bool on) {
        onControlThreadForTest([this, on] { holdHaltReportForTest_ = on; return 0; });
    }
private:
    /// When resignIfHalted_ first saw this broker leading while halted (-1 = not yet; control thread).
    int64_t          haltedSinceNs_ = -1;
    PlacementViewMsg placementPosted_{};
    bool             placementPostedOnce_ = false;
    std::int64_t     unplaceablePublished_ = -1;
    // DR-bugs fix round 1 (review B-I2): link-MTU watchdog — CONTROL thread only.  The probe host is resolved
    // once at start (empty = every endpoint loopback: the watchdog is off); every kMtuProbePeriodNs the
    // interface MTU of its route is compared with the Aeron MTU; an ERROR on the below edge, a WARN on the
    // restore edge, never in between; the deficit is the live counter.
    static constexpr int64_t kMtuProbePeriodNs = 5'000'000'000LL;
    // Re-check I1: the probe target arrives from the resolver hub's cold thread over mtuProbeRing_ (drained here,
    // control thread, no allocation); the MTU is one ioctl on mtuProbeFd_.  mtuProbeDesc_: the host, for logs only.
    std::string      mtuProbeDesc_;
    MtuProbeTarget   mtuProbe_{};
    rune::consensus::PeerResolutionRing* mtuProbeRing_ = nullptr;
    int              mtuProbeFd_           = -1;
    int64_t          nextMtuProbeNs_       = 0;
    bool             mtuBelow_             = false;
    int32_t          lastLinkMtu_          = 0;
    int32_t          linkMtuDeficit_       = 0;
    uint64_t         mtuBelowEdges_        = 0;
    uint64_t         mtuRestoredEdges_     = 0;
    std::function<int32_t()> mtuProbeForTest_;   ///< cold test seam (setMtuProbeForTest)
    std::function<void(HostNetFacts&)> hostFactsOverrideForTest_;   ///< cold test seam (B-M5)
    bool holdTransientLineageForTest_ = false;   ///< control thread only (holdTransientLineageForTest)
    int32_t forceEgressUnresolvedForTest_ = -1;  ///< startup thread only (forceWorkerEgressUnresolvedForTest)
    std::vector<std::string> bindAnchorChannels_;   ///< startup thread only (bindAnchorChannelsForTest)
    int checkLinkMtu_(int64_t nowNs, bool force);
    // Review A-I5 (fix round 1): bind anchors — every fixed-port receive endpoint (and per-worker MDC egress
    // control endpoint) of a driver is BOUND before that driver's first publication auto-binds an ephemeral
    // port.  An anchor shares the component's exact channel (so its endpoint) on a reserved stream no
    // publisher ever uses: no image, no back-pressure.  Held until stop().
    static constexpr int32_t kBindAnchorStreamId = 0x7E5E0A7E;
    static const char* const kAnchorPubTerm;
    std::vector<std::shared_ptr<aeron::Subscription>>         bindAnchorSubs_;
    std::vector<std::shared_ptr<aeron::ExclusivePublication>> bindAnchorPubs_;
    void anchorFixedEndpoints_(const std::shared_ptr<aeron::Aeron>& client, const std::vector<std::string>& subs,
                               const std::vector<std::string>& pubs, rune::aeronutil::AeronPump& pump,
                               const char* where);
    void releaseBindAnchors_() noexcept { bindAnchorPubs_.clear(); bindAnchorSubs_.clear(); }
    std::int64_t     blockedPublished_     = -1;
    /// Control thread: a halted broker never drives catalog placement — it
    /// reports its halt to every member (never a placement target) and, while it
    /// leads the consensus, stands down to a live healthy peer when one exists
    /// (never leaving the cluster without a controller; fix round 0).
    int resignIfHalted_(bool consensusLeader);
    uint64_t                                              catalogAlterIsrRefused_ = 0;   // control thread

    // Catalog history Task 16: the SchemaRegistry / ViewRegistry are the CatalogApplier's
    // projections of __catalog (built by bootFromCatalog_ on the startup thread, then
    // owned by the admin thread); the retired __topic_schemas / __views logs and
    // replicas are gone.
    std::unique_ptr<SchemaRegistry>                       schemaRegistry_;
    std::unique_ptr<AdminWorkerAgent>                     adminWorkerAgent_;

    std::unique_ptr<ViewRegistry>                         viewRegistry_;   // (see schemaRegistry_)


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

    // Catalog history Task 8 (control-thread-owned): the drop sequence's states
    // (see dropRuntimeTopic_), the creates waiting behind a drop of their name,
    // and the frames a full ring could not take yet (retried, never dropped).
    static constexpr int64_t kRenameRetryMinNs = 100'000'000;     // 100 ms
    static constexpr int64_t kRenameRetryMaxNs = 10'000'000'000;  // 10 s
    struct DroppedTopic {
        int32_t                                      id = -1;
        uint32_t                                     nsId = 0;   // namespaces Task 6: its dir is under ns-<nsId>
        std::string                                  name;
        bool                                         diskBacked = true;
        std::variant<drop_step::AwaitingRetire, drop_step::AwaitingRename> step;
    };
    std::vector<DroppedTopic>                             droppedAwaitingRetire_;
    struct PendingCreateBehindDrop { TopicSpec spec; };
    std::vector<PendingCreateBehindDrop>                  pendingCreatesBehindDrop_;   // FIFO (per name)
    std::vector<std::string>                              purgeOutbox_;        // kMsgPurgeDroppedDir ring-full retry
    std::vector<std::vector<uint8_t>>                     cleanerPolicyOutbox_;  // kPolicyMsgType ring-full retry
    std::vector<std::array<uint8_t, CleanerViewChannel::kNsLimitBytes>> cleanerNsLimitOutbox_;   // kNsLimitMsgType ring-full retry (Task 13)
    void postCleanerNsLimit_(uint16_t slot, uint32_t nsId, uint64_t maxRetentionMs, uint64_t maxRetentionBytes);   // control thread
    std::vector<ReapResultMsg>                            reapResultOutbox_;     // kMsgNsReapResult ring-full retry (Task 7)
    void onNsReapRequest_(const ReapRequestMsg& m);                              // control thread (Task 7)
    void onNamespaceErasedDisk_(uint32_t nsId);                                  // control thread (Task 7, D18)
    // Namespaces Phase 2 Task 8 (D22): the node's AUTHORITATIVE slot table (control thread).  Slot 0 = `default`.  The
    // gateway, every worker and every worker's view set keep their own copy, fed from here (gateway: direct, same thread;
    // workers: nsSlotFanout_ on the worker rings).  Each namespace has one per-node gauge in the metrics file whose
    // owner tag is the nsId (a reader tells a reused slot from the old one).
    NsSlotTable                                           nsSlots_;
    std::vector<uint8_t>                                  nsDeletingAtBoot_;   // per slot: 1 = the booted catalog has the namespace DELETING
    NsSlotFanout                                          nsSlotFanout_;
    NsLimitAggregator                                     nsAggregator_;       // Task 14 (D28): control thread only
    std::vector<metrics::SlotWriter>                      nsMetricSlots_;       // by nsSlot; unbound (slot() < 0) = none
    uint64_t                                              nsSlotAssignFailures_ = 0;   // a bug if ever non-zero
    std::int64_t                                          lastNsMismatchPublished_ = 0;
    void bootAssignNsSlots_();                                                   // startup thread: from the booted catalog
    void onNsUpserted_(const rune::catalog::NamespaceRecord& rec);               // control thread: assign / refresh + broadcast
    void onNsErasedSlot_(uint32_t nsId);                                         // control thread: release + broadcast
    void registerNsMetric_(uint16_t slot, const rune::catalog::NamespaceRecord& rec);
    void freeNsMetric_(uint16_t slot);
    void publishNsSlotMetrics_() noexcept;
public:
    [[nodiscard]] uint16_t nsSlotOfForTest(uint32_t nsId);   // read ON the control thread
    /// The (nsId, slot) pairs of the authoritative table, ascending slot (read ON the control thread).
    [[nodiscard]] std::vector<std::pair<uint16_t, uint32_t>> nsSlotsForTest();
    [[nodiscard]] uint64_t nsSlotAssignFailuresForTest() const noexcept { return nsSlotAssignFailures_; }
    /// Namespaces Phase 2 Task 10 (D24, #236): this node's gateway counts (read ON the control thread).
    [[nodiscard]] uint32_t nsConnectionsForTest(uint32_t nsId);
    [[nodiscard]] uint32_t anonymousSessionsForTest();
private:
    int  serviceErasedNamespaces_();                                             // control thread: the rename, once its topics retired
    std::vector<uint32_t>                                 erasedNsAwaitingRename_;
    std::size_t                                           nsDirMismatchesAtBoot_ = 0;   // '*.deleting' dirs of a live namespace (fatal)
    CleanerViewChannel*                                   cleanerViewChannel_ = nullptr;   // owned by runtimeAgent_
    void fanoutViewPlan_(ViewPlanUpdateMsg msg);   // control thread
    int  pumpViewFanout_();                        // control thread (every cycle)
    // Restart recovery (catalog history Task 9): every live view is reinstalled
    // by the __catalog boot replay (AdminWorker::bootCatalog, the boot sink's
    // RECOVERY installs), through the same fanoutViewPlan_/pumpViewFanout_ path
    // (co-locate + installForRecovery).

    // Runtime-cycle DDL control plane (direct call; see AdminControlPlane.h).
    friend class AdminControlPlane;
    void attachFusedRuntimeDoorbell_();   // A9: runtime's cross-thread rings → worker 0's doorbell (fused)
    int serviceAdminControl_(int64_t nowNs);
    AdminControlPlane                                     adminControl_{*this};
    int64_t                                               slowTickIntervalOverrideNs_ = -1;
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
    TopicDroppedFanout                           topicDroppedFanout_;   // R-M1: drop -> every worker (control thread)
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
    int  catalogRecoveryFaultForTest_ = 0;
    std::optional<std::string>             commitReplicaFault_; // control thread after start()
    // C1 / M7: a DefineTopic / CreateView OK leaves only once its topic serves
    // (see DdlAnswerGate.h).  Control-thread-owned; one drain of the admin egress
    // ring (16 frames of at most a ring message each) always fits.
    // Catalog history Task 12 fix round 0: catalog-read flow control (control
    // thread) — one entry per paused catalog subscription, preallocated.
    CatalogReadFlow catalogReadFlow_{2u * AdminWorker::kMaxCatalogSubscriptions};
    static constexpr std::size_t kDdlAnswerGateBytes = 2u * static_cast<std::size_t>(kAdminRequestRingBytes) + 4096u;
    // PR #611 review (issue #209): drainAdminEgress reads a frame only while the gate holds nothing or has
    // 2 x (maxMsgLength + 32) bytes free (maxMsgLength = ring capacity / 8).  A gate smaller than that room plus one
    // frame would stop the drain whenever it held anything - the head-of-line block #209 removed.
    static_assert(kDdlAnswerGateBytes >= 3u * (static_cast<std::size_t>(kAdminEgressRingBytes) / 8u + 32u),
                  "the DDL answer gate must hold the drain's room plus a frame");
    DdlAnswerGate ddlAnswerGate_{kDdlAnswerGateBytes, kDdlAnswerMaxHoldNs};
    uint64_t      ddlAnswersOverdue_ = 0;   // control thread only: held OKs answered Internal (in doubt)
    int64_t       ddlOverdueFirstNs_ = 0, ddlOverdueLastNs_ = 0;   // control thread only: R3-M1 evidence stamps
    [[nodiscard]] bool ddlTopicServes_(int32_t topicId) const noexcept;
    int32_t                                      shardResolveStallWorkerForTest_ = -1;
    std::chrono::milliseconds                    shardResolveStallForTest_{0};
    int32_t                                      shardDriverBuildStallWorkerForTest_ = -1;
    std::chrono::milliseconds                    shardDriverBuildStallForTest_{0};
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

    // Catalog history Task 7: the CatalogApplier's metrics, set on the CONTROL thread only from the admin→control
    // drain (kA2cCatalogHalted / kA2cCatalogApplied) into the metrics file.
    std::int64_t                     catalogHaltedValue_  = 0;    // control-thread-owned (0 = applying)
    std::int64_t                     catalogAppliedValue_ = -1;   // control-thread-owned
    void publishCatalogMetrics_() noexcept;
    // D9 review MEDIUM-3 / #359: the TCS sinks (control thread) - stuck markers, stuck txn offset commits + oldest age.
    static bool publishStuckMarkers_(void* self, std::int64_t value);
    static bool publishStuckTxnOffsets_(void* self, std::int64_t stuck, std::int64_t oldestAgeMs);

    // -----------------------------------------------------------------------
    // arcanum-metrics (plan Task 8): the node's metrics file.  start() creates it, allocates EVERY kRuneMetrics row
    // (slots_[row]) and inits systemMetrics_ before any agent starts (D59); a failure throws out of start().  Each
    // slot has ONE writer thread (RuneMetricDef::owner); the control rows are written by the control thread only.
    // Task 9 (D58): the observer's rows are copied into observerSlots_ and the provisioner's into its constructor,
    // both before any thread starts; nothing else touches them.  Every store carries its writer's owner tag; a debug
    // build records it in metricWrittenBy_ (one bit per MetricOwner per row) and asserts it is the row's owner.
    // metricsFile_ owns the mapping (unmapped at destruction, after every agent joined).
    // -----------------------------------------------------------------------
    metrics::MetricsFile                                  metricsFile_;
    metrics::MetricsWriter                                metricsWriter_;
    std::vector<int32_t>                                  metricsFreeStack_;   // sized maxSlots once at start (cold)
    std::array<RuneMetricSlot, kNodeMetricCount>          slots_{};
    std::array<std::uint8_t, kNodeMetricCount>            metricWrittenBy_{};   // debug owner-tag record (D58)
    struct ObserverSlots { RuneMetricSlot role, term, outOfSync; };
    ObserverSlots                                         observerSlots_{};     // observer thread only (publishRole)
    std::optional<metrics::SystemMetrics>                 systemMetrics_;      // control thread (D62)
    bool                                                  metricsReady_ = false;   // set by start(), before agents
    std::string                                           metricsPath_;
    void createMetrics_();   // startup thread: steps 1-4 of the plan's start order
    /// A control-thread store into a MetricOwner::Control row.
    void controlSet_(RuneMetric m, std::int64_t v) noexcept {
        slots_[static_cast<std::size_t>(runeMetricRow(m))].set(v, MetricOwner::Control);
    }
    [[nodiscard]] std::int64_t controlGet_(RuneMetric m) const noexcept {
        return slots_[static_cast<std::size_t>(runeMetricRow(m))].get();
    }

    // -----------------------------------------------------------------------
    // State flags — CONTROLLER-owned (F9a: no atomics).  Written only by the
    // thread calling start()/stop(); the controller role may move to another
    // thread only across a happens-before edge (thread start/join), e.g.
    // ShardModeTest stops the node from a helper thread it then joins.
    // -----------------------------------------------------------------------
    bool started_ = false;
    bool stopped_ = false;
    bool crashForTest_ = false;   // crashForTest(): stop() skips the controlled-shutdown protocol

    // F9a: control thread → observer thread leadership ring (see Observers).
    // mutable: the const observer accessors drain it.
    mutable LeaderObserver leaderObserver_{static_cast<rune::consensus::NodeId>(config_.nodeId)};
};

} // namespace rune::node
