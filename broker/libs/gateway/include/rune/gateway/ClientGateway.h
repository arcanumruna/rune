#pragma once
#include <rune/consensus/PeerResolution.h>

#include <cstdio>
#include <cstdlib>
// ClientGateway.h — Client-facing session gateway (BR-4 / BR-5 / BR-7a / GR-4 / GR-5).
//
// Design:
//   - Subscribes to a single Aeron ingress stream (clientChannel/clientStreamId).
//   - FragmentAssembler reassembles multi-frame messages before dispatch.
//   - doWork(nowNs) polls ingress and dispatches:
//       Connect      → ASYNC: addExclusivePublication (non-blocking), record
//                      pending-connect entry; resolve + promote across doWork cycles
//       Keepalive    → touch session (update lastSeenNs)
//       Ping         → echo Pong on session egress
//       Disconnect   → remove session
//       Unknown/bad  → drop (never crash)
//   - Async Connect egress-resolve (BR-7a):
//       On Connect: addExclusivePublication returns a registrationId immediately —
//       no blocking. A PendingConnect is stored. Each doWork() polls
//       findExclusivePublication; on resolve → promote to SessionTable + send
//       ConnectResponse; on timeout (connectResolveTimeoutNs) → drop + log.
//       Never blocks the duty-cycle thread waiting for a client's egress.
//       Bounded in-flight cap: kMaxPendingConnects — oldest evicted beyond cap.
//       All PendingConnect state touched only in doWork (single-writer).
//   - Session timeout reaping: doWork(nowNs) reaps sessions idle beyond
//     sessionTimeoutNs (injected clock — deterministic, no wall-clock in reap).
//   - Leader-awareness: non-leader Connect → ErrorResponse{Redirect, leaderEndpoint}.
//   - GR-4: Produce is routed through the leader PartitionReplica (Mode A):
//       After appendNoSync, onLeaderLogAppended() reconciles the replica's tracked
//       LEO/HW so followers can fetch the new bytes.  Per-partition NotLeader
//       defence: if the replica is not in Leader role, produce is rejected BEFORE
//       any append (belt-and-suspenders beyond the cluster-level leaderView_ check).
//   - GR-5: acks=All deferred ack (non-blocking pending-ack queue):
//       When a Produce carries acks=1 (All), the ProduceResponse is NOT sent
//       immediately after append.  Instead a PendingAck entry is enqueued
//       (zero-alloc: pre-sized ring buffer).  Each doWork() cycle, after the
//       replica's HW may have advanced (replication fetch responses processed by
//       the duty-cycle thread BEFORE gateway.doWork), drainPendingAcks() checks
//       every entry: it re-resolves the replica through the ReplicaRegistry by
//       (topicId, partition, incarnation) — no pointer is held — and if the
//       published HW > entry.lastOffset the ProduceResponse is sent and the
//       entry removed; a removed / re-created partition fails it with NotLeader.
//       min-ISR gate: before enqueuing, canProduce(minIsr, acksAll=true) is
//       checked via the ISR manager; if ISR is too small, ProduceResponse
//       MinIsrViolation is sent immediately (no append).
//       Timeout: entries stuck beyond kPendingAckTimeoutNs receive
//       ProduceResponse{Internal} and are evicted (follower never advancing).
//       acks=Leader/None: unchanged — ack immediately after append.
//   - Single-writer: doWork() must be called from ONE thread only.
//   - Zero-alloc on the hot path: encode scratch is a fixed member buffer.
//   - NO busy-spin.

#include <rune/aeronutil/AeronPump.h>
#include <rune/gateway/SessionTable.h>
#include <rune/gateway/SpillPool.h>
#include <rune/gateway/IGroupRequestHandler.h>
#include <rune/gateway/ITxnRequestHandler.h>
#include <rune/gateway/IAdminRequestSink.h>
#include <rune/gateway/CatalogRoute.h>
// client<->broker TLS/AEAD (Task 7): server-role handshake driver. SessionTable.h
// (above) already pulls in rune/crypto/Aead.h + Cipher.h for CryptoState; this
// adds TlsSession itself for PendingConnect's in-flight handshake.
#include <rune/crypto/TlsSession.h>

// Forward-declared to avoid pulling the full WorkerPool header (and its heavy
// chain: Worker → SpscRing → WorkerOp → ReplicationMessages) into the gateway
// header.  All WorkerPool calls are in ClientGateway.cpp.
namespace rune::node { class WorkerPool; struct WorkerReturn; }

#include <Aeron.h>
#include <Subscription.h>
#include <ExclusivePublication.h>
#include <rune/aeronutil/FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/BufferClaim.h>
#include <util/Index.h>

#include <rune/storage/Log.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/transaction/ReadCommittedView.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/node/RecordsPush.h>
#include <rune/node/SessionAnnouncer.h>
// G0.1: ErrorCode enum for sendRespReliable() signature.
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/SchemaMessages.h>   // DdlExtension (catalog history Task 9)

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <map>
#include <unordered_map>
#include <utility>
#include <unordered_set>
#include <variant>
#include <vector>

namespace rune::node { class ReplicaRegistry; }
namespace rune::replication { class PartitionReplica; }

namespace rune::gateway {

// Forward declaration — full definition in TopicRegistry.h (avoids heavy include
// in this header; GB-2/GB-3 will include TopicRegistry.h in ClientGateway.cpp).
class TopicRegistry;

// ---------------------------------------------------------------------------
// LeaderView — snapshot of cluster leadership from the node's perspective.
// ---------------------------------------------------------------------------
struct LeaderView {
    bool        isLeader             = true;   // true if this node is the current leader
    std::string leaderClientEndpoint;           // leader's client-facing channel; empty if unknown
};

/// Shard mode (spec §3.3): a node's explicitly configured per-worker ingress base.
struct ShardIngressEndpoint {
    int32_t     nodeId   = -1;
    std::string host;
    int32_t     basePort = 0;
    /// The node's per-worker egress-control base (0 = unknown → this node's own).
    int32_t     egressControlBasePort = 0;
};

// ---------------------------------------------------------------------------
// ClientGateway
// ---------------------------------------------------------------------------
class ClientGateway {
public:
    // Default session timeout: 10 seconds.
    static constexpr int64_t kDefaultSessionTimeoutNs = 10LL * 1'000'000'000LL;
    /// D8b: a HELD session (paused by back-pressure while it has not read its acks) is reaped only after this
    /// long without activity or a worker Progress signal (the client reading its acks) — Kafka
    /// connections.max.idle.ms (10 min).  RUNE_MAX_HELD_IDLE_MS (NodeConfig bounds it to 1 ms .. 24 h).
    /// A session Lost on any worker is reaped on sessionTimeoutNs from its Lost edge instead (reapExpired).
    static constexpr int64_t kDefaultMaxHeldIdleNs = 600LL * 1'000'000'000LL;
    void setMaxHeldIdleNs(int64_t ns) noexcept { maxHeldIdleNs_ = ns > 0 ? ns : kDefaultMaxHeldIdleNs; }
    /// D8c (shard mode, G-A6 on the gateway-routed path) — two credit bounds,
    /// both counted from forward until the owning worker CLAIMS the ack
    /// (SessionSignal Credit returns it):
    ///  * per WORKER (fix round 1, MAJOR-1): at most rune::node::Worker::
    ///    kRoutedAckBudget (16384 = the worker's 32768-entry ack queue minus its
    ///    own 16384 reserve: owed ceiling 8192 + one drain 4096 + one poll 4096)
    ///    forwarded, unclaimed produces to one worker, summed over ALL sessions —
    ///    so no number of stalled gateway sessions can eat the reserve (the
    ///    worker's op drain / direct poll never run out of ack room for them).
    ///    Fairness inside that cap: in its last quarter (the congested band)
    ///    only LIGHT sessions — fewer than kLightSessionInFlight in flight to
    ///    that worker — are admitted.  A stalled session sits at its whole budget
    ///    (>= 64) so it is paused first, while up to 4096 / 64 = 64 light
    ///    sessions (a new client, an interactive producer) keep being served.
    ///  * per SESSION: sessionProduceCredits_ (default 1024 = 1/16 of the routed
    ///    share, so one session never takes more than that of a worker; >= 1024
    ///    batches in flight per session costs a gateway client nothing at sub-ms
    ///    RTTs — Little: 1024 / 1 ms = 1M batches/s).
    /// At either bound the session's next Produce frame is ABORTed (controlled
    /// poll — that client's image only): the pause engages BEFORE any worker's
    /// ack queue can fill, whatever its burst size, MTU or socket buffers.
    /// RUNE_GATEWAY_PRODUCE_CREDITS (NodeConfig bounds it to 1 .. kMaxSessionProduceCredits).
    static constexpr uint32_t kDefaultSessionProduceCredits = 1024;
    static constexpr uint32_t kMaxSessionProduceCredits     = 4096;   // 1/4 of the routed share (static_assert, .cpp)
    static constexpr uint32_t kLightSessionInFlight         = 64;
    static constexpr uint32_t kWorkerRoutedCap              = 16384;  // == Worker::kRoutedAckBudget (static_assert, .cpp)
    /// Normally set at construction (BrokerNode); safe at any time — the
    /// paused-session count is recomputed against the new budget (LOW-2).
    void setSessionProduceCredits(uint32_t n) noexcept {
        sessionProduceCredits_ = (n >= 1 && n <= kMaxSessionProduceCredits) ? n : kDefaultSessionProduceCredits;
        creditBlockedSessions_ = 0;
        sessions_.forEach([&](const Session& s) { creditBlockedSessions_ += s.produceInFlight >= sessionProduceCredits_; });
    }
    [[nodiscard]] uint32_t sessionProduceCredits() const noexcept { return sessionProduceCredits_; }

    // Async Connect egress-resolve timeout: 5 seconds.
    static constexpr int64_t kDefaultConnectResolveTimeoutNs = 5LL * 1'000'000'000LL;

    // Maximum in-flight pending connects.  Beyond this cap, the oldest is evicted.
    static constexpr std::size_t kMaxPendingConnects = 64;
    /// Task 13: most concurrent sessions in shard mode — one worker ack table's
    /// capacity (SessionAckSink::kMaxSessions − 1; static_assert in the .cpp).
    static constexpr std::size_t kShardMaxSessions = 4095;
    /// Task 13 fix round 3 (re-review 2, finding 2): width of Session::
    /// heldByWorkers — every shard worker must have a bit.  NodeConfig refuses
    /// more shard workers (kMaxShardWorkers; static_assert in BrokerNode.cpp).
    static constexpr int32_t kMaxHeldMaskWorkers = 64;

    // @param aeron                    Shared Aeron client (connected to a media driver).
    // @param clientChannel            Aeron channel the gateway listens on (clients publish here).
    // @param clientStreamId           Stream ID on clientChannel.
    // @param thisNodeId               This node's id; echoed in ConnectResponse.leaderMemberId.
    // @param sessionTimeoutNs         Idle session expiry threshold (default 10 s).
    // @param leaderView               Callable returning current LeaderView; default: always leader.
    // @param connectResolveTimeoutNs  How long before a pending-connect egress times out (default 5 s).
    // @param pump                     Startup resolve pump for the ingress subscription (spec §3.6):
    //                                 an invoker driver + conductor (fused runtime) is pumped by the
    //                                 constructing thread; the default pump sleep-polls a threaded driver.
    //                                 Production fused placement uses the default pump (BrokerNode's
    //                                 interim runner services the runtime driver during start()).
    // @param tlsCertPem / tlsKeyPem   client<->broker TLS/AEAD (Task 7): the broker's own TLS
    //                                 server cert + private key, as PEM TEXT (not file paths) —
    //                                 loaded once, at construction.  Both empty (the default) =
    //                                 crypto disabled: a Connect that offers cipherList/handshakeBytes
    //                                 silently degrades to the pre-Task-7 plaintext path, byte-
    //                                 identical to today (unless requireTls is set below).  Task 9
    //                                 owns the RUNE_TLS toggle / the file loading (BrokerNode) /
    //                                 SIGHUP reload (reloadTlsCert()); this constructor only takes
    //                                 the PEM text it's handed.
    // @param requireTls               Task 9 (downgrade safety, spec §11): when true, a Connect
    //                                 that does NOT end up with an established crypto session
    //                                 (plaintext client, no broker cert/key, or a local TLS init
    //                                 failure degrading to plaintext — see shouldEngageCrypto())
    //                                 is hard-rejected with ConnectResponse{HandshakeFailed} —
    //                                 NEVER served plaintext.  Default false (back-compat).  Task
    //                                 19: shard mode no longer refuses crypto outright (it seals
    //                                 its own per-worker Records egress now), so it is no longer
    //                                 part of this list.
    ClientGateway(std::shared_ptr<aeron::Aeron>       aeron,
                  std::string                         clientChannel,
                  int32_t                             clientStreamId,
                  int32_t                             thisNodeId,
                  int64_t                             sessionTimeoutNs         = kDefaultSessionTimeoutNs,
                  std::function<LeaderView()>         leaderView               = nullptr,
                  int64_t                             connectResolveTimeoutNs  = kDefaultConnectResolveTimeoutNs,
                  rune::aeronutil::AeronPump          pump                     = {},
                  std::string                         tlsCertPem               = {},
                  std::string                         tlsKeyPem                = {},
                  bool                                requireTls               = false);

    ~ClientGateway() = default;

    // Non-copyable, non-movable
    ClientGateway(const ClientGateway&)            = delete;
    ClientGateway& operator=(const ClientGateway&) = delete;

    // -----------------------------------------------------------------------
    // G0.1: Test-only factory — constructs a ClientGateway without Aeron.
    // Installs a TestEgressFn override so sendRespReliable / drainPendingResponses
    // use the callback rather than a real ExclusivePublication offer.
    // testEgressFn must return > 0 for "delivered" or <= 0 for back-pressure.
    // currentProduceSessionId is the session id used by sendRespReliable (public
    // 4-param version) — set to match the session id that the test operates on.
    // Single-writer: use only from tests on the test thread.
    // -----------------------------------------------------------------------
    using TestEgressFn = std::function<int64_t(const uint8_t* buf, std::size_t len)>;

    static ClientGateway makeForTest(TestEgressFn egressFn,
                                     int64_t currentProduceSessionId = 1);

    // G1.1: makeForTest overload that also wires a leaderView_ callback.
    // Used by ClientGatewayHotPathTest to guard against leaderView_ regression.
    static ClientGateway makeForTestWithLeaderView(TestEgressFn             egressFn,
                                                   std::function<LeaderView()> leaderView,
                                                   int64_t currentProduceSessionId = 1);

    // -----------------------------------------------------------------------
    // doWork — poll ingress (bounded fragment limit) + dispatch + reap expired.
    // Returns number of fragments processed (work count).
    // Must be called from ONE thread only.
    // -----------------------------------------------------------------------
    int doWork(int64_t nowNs);

    // -----------------------------------------------------------------------
    // reapExpired — remove sessions idle beyond sessionTimeoutNs.
    // Uses injected nowNs only (no wall-clock).  Bounded O(n) scan.
    // Callable directly from unit tests to drive reaping without live ingress.
    // -----------------------------------------------------------------------
    void reapExpired(int64_t nowNs);

    // -----------------------------------------------------------------------
    // setTopicRegistry — wire in the TopicRegistry (GB-1).
    // Must be called before doWork() if produce/subscribe dispatch is desired.
    // May be null (no topics configured). Pointer is non-owning; lifetime must
    // exceed the ClientGateway. Single-writer: call only on the duty-cycle thread.
    // GB-2/GB-3 will use this pointer for produce/subscribe dispatch.
    // -----------------------------------------------------------------------
    void setTopicRegistry(TopicRegistry* registry) noexcept {
        topicRegistry_ = registry;
        resetInlinePsm_();   // M4: a new registry = new partition identities
    }

    // -----------------------------------------------------------------------
    // topicRegistry — current registry pointer (may be null).
    // -----------------------------------------------------------------------
    [[nodiscard]] TopicRegistry* topicRegistry() const noexcept {
        return topicRegistry_;
    }

    // -----------------------------------------------------------------------
    // setReplicaRegistry — wire in the ReplicaRegistry (GR-4).
    // Must be called before doWork() if produce replication is desired.
    // May be null (no replication configured). Pointer is non-owning; lifetime
    // must exceed the ClientGateway. Single-writer: call only on the duty-cycle
    // thread (same restriction as setTopicRegistry).
    //
    // When set, handleProduce() will call replica->onLeaderLogAppended() after
    // each successful append so the leader replica's tracked LEO/HW is reconciled
    // and followers can fetch the new bytes.
    // -----------------------------------------------------------------------
    void setReplicaRegistry(rune::node::ReplicaRegistry* registry) noexcept {
        replicaRegistry_ = registry;
        resetInlinePsm_();   // M4: the facts' incarnations belong to the old registry
    }

    // -----------------------------------------------------------------------
    // replicaRegistry — current registry pointer (may be null).
    // -----------------------------------------------------------------------
    [[nodiscard]] rune::node::ReplicaRegistry* replicaRegistry() const noexcept {
        return replicaRegistry_;
    }

    // -----------------------------------------------------------------------
    // setGroupService — wire in the group-request handler (CG-3 / CG-4 fix).
    // Accepts any IGroupRequestHandler implementation (e.g. GroupCoordinationService).
    // Must be called before doWork() if group-coordination dispatch is desired.
    // May be null (group messages are dropped). Pointer is non-owning; lifetime
    // must exceed the ClientGateway. Single-writer: call only on the duty-cycle thread.
    // -----------------------------------------------------------------------
    void setGroupService(rune::gateway::IGroupRequestHandler* service) noexcept {
        groupService_ = service;
    }

    // -----------------------------------------------------------------------
    // groupService — current handler pointer (may be null).
    // -----------------------------------------------------------------------
    [[nodiscard]] rune::gateway::IGroupRequestHandler* groupService() const noexcept {
        return groupService_;
    }

    // -----------------------------------------------------------------------
    // setTxnService — wire in the txn-request handler (TX-3).
    // Accepts any ITxnRequestHandler implementation (e.g. TransactionCoordinationService).
    // Must be called before doWork() if txn-coordination dispatch is desired.
    // May be null (txn messages are dropped). Pointer is non-owning; lifetime
    // must exceed the ClientGateway. Single-writer: call only on the duty-cycle thread.
    // Dispatches templateIds 45 (InitProducerId), 47 (AddPartitionsToTxn),
    // 49 (AddOffsetsToTxn), 51 (TxnOffsetCommit), 53 (EndTxn).
    // -----------------------------------------------------------------------
    void setTxnService(rune::gateway::ITxnRequestHandler* service) noexcept {
        txnService_ = service;
    }

    // -----------------------------------------------------------------------
    // txnService — current txn handler pointer (may be null).
    // -----------------------------------------------------------------------
    [[nodiscard]] rune::gateway::ITxnRequestHandler* txnService() const noexcept {
        return txnService_;
    }

    // -----------------------------------------------------------------------
    // Task 8a: setAdminSink — wire in the schema DDL request sink (RULING-2 F).
    // Accepts any IAdminRequestSink (e.g. the AdminWorkerAgent ring adapter).
    // On DefineTopic (64) / GetTopicSchema (66), the gateway validates the frame,
    // requires a known session, decodes, copies the fields into the sink, and
    // RETURNS immediately — never blocks the produce hot path on DDL.
    // May be null (DDL messages are dropped).  Non-owning; must outlive the
    // gateway.  Single-writer: call only on the duty-cycle thread.
    // -----------------------------------------------------------------------
    void setAdminSink(rune::gateway::IAdminRequestSink* sink) noexcept {
        adminSink_ = sink;
    }

    [[nodiscard]] rune::gateway::IAdminRequestSink* adminSink() const noexcept {
        return adminSink_;
    }

    // -----------------------------------------------------------------------
    // Catalog history Task 11: the DDL route (CatalogRoute.h) — a CONTROL-thread
    // value BrokerNode publishes on change (same thread as the gateway's duty
    // cycle).  Every DDL handler (64, 68, 70, 74–77) consults it before
    // submitting; the reads (66, 72) are always served locally.  Default
    // RouteLocalLeader: the AdminWorker decides (a gateway with no node).
    // -----------------------------------------------------------------------
    void setCatalogRoute(rune::gateway::CatalogRoute r) noexcept { catalogRoute_ = std::move(r); }
    [[nodiscard]] const rune::gateway::CatalogRoute& catalogRoute() const noexcept { return catalogRoute_; }
    /// DDLs answered by the route without reaching the admin ring (Redirect /
    /// NotLeader / Internal / pre-v8 InvalidRequest).
    [[nodiscard]] uint64_t ddlRoutedAway() const noexcept { return ddlRoutedAway_; }
    /// F37: pre-v8 DDLs refused on a non-leader (InvalidRequest in their own type).
    [[nodiscard]] uint64_t preV8DdlRefused() const noexcept { return preV8DdlRefused_; }

    // -----------------------------------------------------------------------
    // MD1 Task 9: Per-partition routing image.
    //
    // setRoutingImageSnapshot — install a new routing image snapshot.
    // Called from BrokerNode::buildGateway() and MetadataService on every
    // duty cycle when the routing image is updated.
    //
    // The snapshot is a shared_ptr captured ONCE per duty cycle (capture-once
    // discipline) inside handleProduce / handleSubscribe / doWork.  The caller
    // MUST call this from the duty-cycle thread (single-writer).
    //
    // setNodeEndpointMap — wire in the nodeId → client endpoint map.
    // Maps each peer's nodeId to its clientChannel string
    // (e.g. "aeron:udp?endpoint=host:port").  Built from NodeConfig::peers.
    // Must be called before doWork() if per-partition routing is desired.
    // May be empty (no-op for routing — falls back to NotLeader error).
    // -----------------------------------------------------------------------
    void setRoutingImageSnapshot(
            std::shared_ptr<const rune::metadata::RoutingImage> img) noexcept {
        routingImage_ = std::move(img);
        // D9: a partition whose leadership moved fails its in-flight remote
        // markers NotLeader (event-driven; the TCS retry re-routes) — done by
        // the next marker pump of this same thread (review LOW-7: never calls
        // into the coordinator from inside the routing publish).
        routingChangedForMarkers_ = true;
    }

    [[nodiscard]]
    const rune::metadata::RoutingImage* routingImage() const noexcept {
        return routingImage_.get();
    }

    // -----------------------------------------------------------------------
    // setCommittedMetadataReady — signal that the committed assignment map is
    // non-empty (metadata log has replicated and at least one assignment is
    // committed).
    //
    // MD1-13 BOOTSTRAP FIX (Fix 5):
    //   With the static-routing-image bootstrap fix, routingImage_ is now set
    //   on EVERY duty cycle (even during the bootstrap window, using the
    //   StaticBalancer-derived image).  This means the old gate
    //   "if (routingImage_)" can NO LONGER be used to distinguish bootstrap
    //   from steady state in GR-4 (per-partition NotLeader defence) and GR-5
    //   (ISR gate / deferred-ack).
    //
    //   Use this flag instead:
    //     - false (default): bootstrap window — GR-4 and GR-5 ISR gate are
    //       SUPPRESSED.  User replicas start as Follower; bridge Step B
    //       (ReplicationMetadataBridge) has not yet fired.  During this window,
    //       the partition leader serves produces via Path A (static routing image)
    //       without the GR-4 role check or the ISR min-size gate.
    //     - true: steady state — GR-4 and GR-5 fire normally.  committedMap is
    //       non-empty → bridge Step B has promoted the correct replicas to Leader.
    //
    //   NodeRuntimeAgent calls setCommittedMetadataReady(true) on the first duty
    //   cycle where committedMap() is non-empty.  Once true it is never reset
    //   (committedMap only grows after the first assignment is committed).
    //
    //   Single-writer: must be called on the duty-cycle thread.
    // -----------------------------------------------------------------------
    void setCommittedMetadataReady(bool ready) noexcept {
        committedMetadataReady_ = ready;
    }

    // Task 11 fix 1: this node's leadership for the current duty cycle, pushed
    // by the runtime agent (same thread) right after consensus runs.  Once set,
    // doWork() reads it instead of calling the LeaderView std::function every
    // cycle (no indirect call, no std::string build on followers).  Unset (-1):
    // the LeaderView callable is used (gateway-only tests).
    void setLocalLeadership(bool leader) noexcept { localLeader_ = leader ? 1 : 0; }

    [[nodiscard]]
    bool committedMetadataReady() const noexcept {
        return committedMetadataReady_;
    }

    // -----------------------------------------------------------------------
    // Cluster-unique ids: every session id and subscription id this gateway
    // allocates carries ((nodeId + 1) << 40) in its high bits, so ids never
    // collide across the cluster's nodes (a client holding one session per
    // node demultiplexes Records by subscriptionId).  Always positive.  The
    // production constructor sets it from thisNodeId; test constructors keep
    // the historical small ids unless a test sets it.
    // -----------------------------------------------------------------------
    static constexpr int kClusterIdShift = 40;
    [[nodiscard]] static constexpr int64_t clusterIdPrefix(int32_t nodeId) noexcept {
        return static_cast<int64_t>(nodeId + 1) << kClusterIdShift;
    }
/// MAJOR-2 (CG routing): the ConnectRequest.clientVersion of a live session,
    /// -1 when the session is unknown.
    [[nodiscard]] int32_t sessionClientVersion(int64_t sessionId) {
        Session* s = sessions_.find(sessionId);
        return s != nullptr ? s->clientVersion : -1;
    }
    void setClusterIdNode(int32_t nodeId) noexcept {
        idPrefix_ = nodeId >= 0 ? clusterIdPrefix(nodeId) : 0;
        sessions_.setIdPrefix(idPrefix_);
    }

    void setNodeEndpointMap(
            std::unordered_map<int32_t, std::string> nodeEndpoints) noexcept {
        nodeEndpointMap_ = std::move(nodeEndpoints);
    }

    // -----------------------------------------------------------------------
    // SessionAdmission — which brokers host client sessions (set once, before
    // doWork; duty-cycle thread single-writer after that).
    //
    //   ClusterLeaderOnly (default — a bare gateway, BR-5): with no routing
    //     image, a Raft follower answers Connect with ErrorResponse{Redirect,
    //     the Raft leader's client endpoint} and creates no session.
    //   AnyNode (BrokerNode: metadata-driven routing): every broker hosts the
    //     session in every role (leader, follower, catching up, halted, below
    //     minIsr) — data ops are routed per partition, DDL per catalog leader;
    //     the Connect itself is never redirected.
    // -----------------------------------------------------------------------
    enum class SessionAdmission : uint8_t { ClusterLeaderOnly, AnyNode };
    void setSessionAdmission(SessionAdmission a) noexcept { sessionAdmission_ = a; }
    [[nodiscard]] SessionAdmission sessionAdmission() const noexcept { return sessionAdmission_; }

    // -----------------------------------------------------------------------
    // E2 inc2a: setWorkerPool — wire in the WorkerPool for per-partition SPSC
    // routing.  When set, produce-append and pushSubscriptions log-reads are
    // enqueued to the owning worker's ingress ring and the return ring is
    // drained at the start of doWork() to send deferred responses.
    //
    // When null (default), the gateway falls back to direct inline execution
    // (pre-E2 path — required for tests and single-node configurations that
    // do not wire a WorkerPool).
    //
    // Must be called before doWork() on the duty-cycle thread (single-writer).
    // The pointer is non-owning; the WorkerPool must outlive the gateway.
    // -----------------------------------------------------------------------
    // F15 S3 (re-review 2 B-1): also narrows the produce batch limit to the
    // pool's worker intake limit, so a gateway-routed batch the worker op /
    // return ring cannot carry is answered MessageTooLarge, never thrown.
    void setWorkerPool(rune::node::WorkerPool* pool) noexcept;

    /// E2 residual round 6 (F1, plain subscriptions): a TRANSIENT partition's
    /// current data lineage (0 = not transient, UINT64_MAX = not known yet,
    /// else the token) — the same provider the group coordinator uses (this
    /// thread).  Drives the lineage note (bind) and the claim check (resume),
    /// see rune-protocol kClientVersionLineageNote.
    using TransientLineageFn = uint64_t (*)(void* ctx, int32_t topicId, int32_t partition) noexcept;
    void setTransientLineage(TransientLineageFn fn, void* ctx) noexcept { lineageFn_ = fn; lineageCtx_ = ctx; }
    [[nodiscard]] uint64_t partitionDataResetsForTest() const noexcept { return partitionDataResets_; }
    [[nodiscard]] uint64_t lineageNotesSentForTest() const noexcept { return lineageNotesSent_; }
    [[nodiscard]] uint64_t subscribesAwaitingLineageForTest() const noexcept { return subscribesAwaitingLineage_; }
    /// F15 S3 (re-review 2 B-1) ⇄ F18: THE client-visible batch limit — the ONE
    /// gateway setter.  BrokerNode sets NodeConfig::produceIntakeLimit()
    /// (= rune::node::Worker::maxAcceptableBatchBytes(config): rings, replication
    /// frame, egress frame) before the gateway serves; clamped to the 8 MiB
    /// kMaxProduceBatchBytes.  The same value bounds every worker's guard.
    void setMaxProduceBatchBytes(std::size_t n) noexcept {
        maxProduceBatchBytes_ = std::min(n, kMaxProduceBatchBytes);
    }

    [[nodiscard]] rune::node::WorkerPool* workerPool() const noexcept {
        return workerPool_;
    }

    // -----------------------------------------------------------------------
    // E5.1.3 (e): setPerWorkerIngressActive — gate the gateway produce fanout.
    //
    // When active=true, the gateway's produce path (tryEnqueueOp for Produce)
    // is skipped entirely; workers poll their own Aeron subscriptions.
    // When active=false (default), the gateway fanout is the live path.
    //
    // Must be called before doWork() on the duty-cycle thread (single-writer).
    // -----------------------------------------------------------------------
    void setPerWorkerIngressActive(bool active) noexcept {
        perWorkerIngressActive_ = active;
    }

    [[nodiscard]] bool perWorkerIngressActive() const noexcept {
        return perWorkerIngressActive_;
    }

    // Self-contained worker shards (spec §3.3): every node's explicitly configured
    // per-worker ingress base (RUNE_PEERS, never derived).  handleMetadata advertises
    // ingressEndpoint = "<leader.host>:<leader.basePort + ownerWorker>" per partition;
    // a leader with no entry (or basePort 0) gets an empty endpoint.  Empty vector
    // (legacy default) → no endpoint, no trailer.  Set before doWork() runs.
    void setShardIngressEndpoints(std::vector<ShardIngressEndpoint> eps) noexcept {
        shardIngressEndpoints_ = std::move(eps);
    }

    // read-rework T3/T4 (I3b): setPerWorkerEgressActive — when active, the gateway
    // ROUTES Subscribe/Unsubscribe for user partitions (topicId >= 0) to the owning
    // worker via the ingress ring and STOPS pushing those partitions itself (the
    // worker reads+encodes+pushes on its own dynamic-MDC egress).  Internal
    // partitions (topicId < 0) stay gateway-pushed.  Default false (gateway push).
    // Must be called before doWork() on the duty-cycle thread (single-writer).
    void setPerWorkerEgressActive(bool active) noexcept {
        perWorkerEgressActive_ = active;
    }
    [[nodiscard]] bool perWorkerEgressActive() const noexcept {
        return perWorkerEgressActive_;
    }

    // -----------------------------------------------------------------------
    // E5.1.3 (d): addWorkerOwnedLog — register a Log* as exclusively owned by
    // a worker for the R7 fix.
    //
    // Called from BrokerNode::buildWorkerPool() for each user-partition log
    // before runners start (single-threaded startup, no race).
    // flushDirtyLogs() skips any log in this set, so the gateway never calls
    // Log::sync on a log the worker is appending to (R7 closed).
    // Internal partitions (topicId < 0) must NOT be registered here.
    // -----------------------------------------------------------------------
    void addWorkerOwnedLog(rune::storage::Log* log) {
        if (log) workerOwnedLogs_.insert(log);
    }
    [[nodiscard]] std::size_t workerOwnedLogCountForTest() const noexcept { return workerOwnedLogs_.size(); }
    /// Sanitizer triage T2 (2026-10-14): true iff `log` is read on THIS (gateway)
    /// thread — not registered as worker-owned.  A worker-owned Log's LEO /
    /// segment map belong to its worker and are never read here.
    [[nodiscard]] bool gatewayOwnsLog_(const rune::storage::Log* log) const noexcept {
        return log != nullptr &&
               (workerOwnedLogs_.empty() ||
                workerOwnedLogs_.count(const_cast<rune::storage::Log*>(log)) == 0);
    }

    // -----------------------------------------------------------------------
    // E5.1: setWorkerIngressBaseStreamId — configure the base Aeron stream id
    // for per-worker ingress routing.  The MetadataResponse fills each
    // PartitionEntry.ingressStreamId as base + workerForPartition(topicId, part).
    //
    // Must be called before the first MetadataRequest is handled.
    // -----------------------------------------------------------------------
    void setWorkerIngressBaseStreamId(int32_t base) noexcept {
        workerIngressBaseStreamId_ = base;
    }

    [[nodiscard]] int32_t workerIngressBaseStreamId() const noexcept {
        return workerIngressBaseStreamId_;
    }

    // Test-only: override the client ingress stream id (normally set via the
    // production constructor).  Used to verify that, with per-worker ingress
    // INACTIVE, the MetadataResponse advertises this stream (so the client can
    // route per-partition to the true leader on the shared gateway stream)
    // rather than the disabling sentinel 0.
    void setClientStreamIdForTest(int32_t streamId) noexcept {
        clientStreamId_ = streamId;
    }

    // read-rework T3/T4 (I4): configure the per-worker dynamic-MDC egress params so
    // handleMetadata can advertise each partition's egress control port + stream id
    // (control port = base + ownerWorker; stream id = base + ownerWorker).  Only
    // used when perWorkerEgressActive_.  Set by BrokerNode at activation.
    void setWorkerEgressParams(int32_t controlBasePort, int32_t baseStreamId) noexcept {
        workerEgressControlBasePort_ = controlBasePort;
        workerEgressBaseStreamId_    = baseStreamId;
    }

    // -----------------------------------------------------------------------
    // setMinIsr — configure the minimum ISR size gate for acks=All produces.
    // Default: 1 (single-node safe; for 3-node clusters, set to 2).
    // Thread-safe (atomic store): may be called from any thread.
    // -----------------------------------------------------------------------
    /// The effective client-visible batch limit (see setMaxProduceBatchBytes).
    [[nodiscard]] std::size_t maxProduceBatchBytes() const noexcept { return maxProduceBatchBytes_; }

    void setMinIsr(int minIsr) noexcept {
        minIsr_.store((minIsr >= 1) ? minIsr : 1, std::memory_order_relaxed);
    }

    // -----------------------------------------------------------------------
    // minIsr — current minIsr setting.
    // -----------------------------------------------------------------------
    [[nodiscard]] int minIsr() const noexcept {
        return minIsr_.load(std::memory_order_relaxed);
    }

    // -----------------------------------------------------------------------
    // G0.3: setFragmentLimit — configure the maximum fragments polled per
    // doWork() cycle (RUNE_GATEWAY_FRAGMENT_LIMIT).  Default 256 (raised from
    // the hard-coded 10 to avoid head-of-line blocking on batched ingress).
    // Must be called before doWork() on the duty-cycle thread (single-writer).
    // -----------------------------------------------------------------------
    void setFragmentLimit(int32_t limit) noexcept {
        fragmentLimit_ = (limit >= 1) ? limit : 256;
    }

    // -----------------------------------------------------------------------
    // fragmentLimit — current fragment poll limit setting.
    // -----------------------------------------------------------------------
    [[nodiscard]] int32_t fragmentLimit() const noexcept {
        return fragmentLimit_;
    }

    // (F3 round 4/5's verbatim setMaxProduceBatchBytes is merged into the ONE
    //  setter above — S3's, clamped to kMaxProduceBatchBytes; BrokerNode refuses
    //  to start when the effective limit cannot carry one record, F3 M3.)
    [[nodiscard]] uint64_t oversizeProduceRejectLogsForTest() const noexcept {
        return oversizeProduceRejectLogs_;
    }
    /// EOS-DUP defense (inline path): produces rejected because the frame's
    /// producer identity disagreed with the RecordBatch header.
    [[nodiscard]] uint64_t identityMismatchRejects() const noexcept { return identityMismatchRejects_; }

    // -----------------------------------------------------------------------
    // pendingAckCount — number of deferred acks=All entries waiting for HW.
    // For tests and monitoring.
    // -----------------------------------------------------------------------
    [[nodiscard]] std::size_t pendingAckCount() const noexcept {
        return pendingAcks_.size();
    }

    // -----------------------------------------------------------------------
    // G0.1: sendRespReliable — encode a ProduceResponse and try to deliver it.
    // On back-pressure (BACK_PRESSURED / ADMIN_ACTION from the egress offer):
    //   enqueues the encoded bytes on the pre-allocated pendingResponses_ ring.
    //   Returns true  — accepted for delivery (will be retried by drainPendingResponses).
    //   Returns false — only on genuine encode failure (scratch too small).
    //
    // This public 4-param overload is intended for test use (it targets the
    // session whose id was set in currentProduceSessionId_).  Production code
    // inside handleProduce calls the private sendRespReliable_(Session&, ...).
    //
    // Single-writer: call only on the duty-cycle thread (or test thread).
    // -----------------------------------------------------------------------
    bool sendRespReliable(rune::protocol::client::ErrorCode ec,
                          int32_t                           partition,
                          int64_t                           baseOffset,
                          int64_t                           correlationId);

    // -----------------------------------------------------------------------
    // G0.1: drainPendingResponses — re-offer queued egress frames from the head
    // of pendingResponses_.  Stops on the first back-pressure (preserves FIFO
    // order).  Returns number of frames successfully delivered this call.
    //
    // Called at the TOP of doWork() (before ingress poll) so buffered acks are
    // sent as soon as the publication clears back-pressure.
    //
    // Single-writer: call only on the duty-cycle thread.
    // -----------------------------------------------------------------------
    int drainPendingResponses(int64_t nowNs);

    // -----------------------------------------------------------------------
    // G0.1: pendingResponseCount — number of egress frames queued in the retry
    // ring.  For tests and monitoring.
    // -----------------------------------------------------------------------
    [[nodiscard]] std::size_t pendingResponseCount() const noexcept {
        return egressSpill_.records();
    }

    // -----------------------------------------------------------------------
    // TX-4 Task 8: appendControlRecord — append a control marker (COMMIT/ABORT)
    // to the leader replica for (topicId, partition) with acks=All durability.
    //
    // Called from TransactionCoordinationService::doWork to write markers to
    // each participant partition and the __consumer_offsets partition.
    //
    // Feeds the per-partition LsoTracker + AbortedTxnIndex (Task 9) so that
    // pushSubscriptions() can apply the correct LSO cap immediately after.
    //
    // \param topicId   Topic identifier (routed via TopicRegistry).
    // \param partition Partition index.
    // \param marker    Raw control batch bytes (already CRC-covered by build()).
    //
    // \param producerId / commit  the marker's producer + COMMIT/ABORT, used when
    //          the append is asynchronous (see kControlRecordAsync).
    //
    // \returns baseOffset of the appended marker (synchronous paths: the caller
    //          then calls notifyControlMarker); kControlRecordAsync when the
    //          marker was handed to the owning worker — the offset is NOT known
    //          yet and is never pre-computed (Task 10 round 4, P1): the gateway
    //          itself calls notifyControlMarker with the offset the worker
    //          ACTUALLY appended at, or surfaces a failure to the txn
    //          coordinator; or -1 if the topic/partition is unknown or the
    //          replica refused the append.
    //
    // Single-writer: must be called on the duty-cycle thread.
    // ---------------------------------------------------------------------------
    static constexpr int64_t kControlRecordAsync = -2;
    /// \param coordinatorEpoch  the epoch of the coordinator dispatching the
    ///          marker (F15 m3): queued entries are tagged with it, purged by
    ///          purgeQueuedMarkers() when that coordinator unloads, and echoed
    ///          to onControlMarkerLanded / onControlMarkerRefused.
    int64_t appendControlRecord(int32_t                    topicId,
                                int32_t                    partition,
                                std::span<const uint8_t>   marker,
                                int64_t                    producerId,
                                bool                       commit,
                                int32_t                    coordinatorEpoch);

    // -----------------------------------------------------------------------
    // D9: cross-node transaction markers.
    //
    // Coordinator side: appendControlRecord resolves the partition's leader
    // from the routing image; a REMOTE leader gets the marker as a
    // WriteTxnMarker over the cluster plane (same markersInFlight_ token table,
    // kControlRecordAsync); its WriteTxnMarkerResponse completes it through the
    // SAME callbacks as a local marker (onControlMarkerLanded / Refused).  A
    // routing-image leader change fails in-flight remote markers NotLeader.
    //
    // Leader side: appendRemoteControlRecord takes the same worker op-ring
    // path as a local marker; the result is answered to the ORIGIN node
    // instead of this node's own coordinator.
    //
    // The channel is plain function pointers + ctx (no virtual, no
    // std::function); BrokerNode wires it to the AeronPartitionTransport.
    // Unset (single node / unit rigs) = no forwarding (today's behaviour).
    // All of it runs on the duty-cycle (runtime) thread.  Cold path.
    // -----------------------------------------------------------------------
    struct TxnMarkerChannel {
        bool (*sendMarker)(void* ctx, int32_t toNode, const rune::replication::WriteTxnMarkerMsg& m) = nullptr;
        bool (*sendResponse)(void* ctx, int32_t toNode,
                             const rune::replication::WriteTxnMarkerResponseMsg& r) = nullptr;
        void* ctx = nullptr;
    };
    void setTxnMarkerChannel(const TxnMarkerChannel& ch) noexcept { txnMarkerChannel_ = ch; }
    /// Leader side: a coordinator's marker for a partition this node leads.
    void appendRemoteControlRecord(const rune::replication::WriteTxnMarkerMsg& m);
    /// Coordinator side: the partition leader's answer.
    void onRemoteControlRecordResponse(const rune::replication::WriteTxnMarkerResponseMsg& r);
    /// AeronPartitionTransport handler thunks (ctx = the gateway).
    static void onWriteTxnMarkerThunk(void* self, const rune::replication::WriteTxnMarkerMsg& m) {
        static_cast<ClientGateway*>(self)->appendRemoteControlRecord(m);
    }
    static void onWriteTxnMarkerResponseThunk(void* self, const rune::replication::WriteTxnMarkerResponseMsg& r) {
        static_cast<ClientGateway*>(self)->onRemoteControlRecordResponse(r);
    }
    /// Review MAJOR-1: the cluster plane's link to `peer` came back (down→up
    /// edge; -1 = a new inbound image whose peer is unknown) — every remote
    /// marker in flight to it (to everyone for -1) is RE-SENT with the SAME
    /// token by the next marker pump.  A remote marker completes only on a real
    /// answer or a leadership change — never on a timer.
    void onPlaneReconnect(int32_t peer) {
        if (peer < 0) resendAllRemoteMarkers_ = true;
        else if (std::find(reconnectedPeers_.begin(), reconnectedPeers_.end(), peer) == reconnectedPeers_.end())
            reconnectedPeers_.push_back(peer);
    }
    static void onPlaneReconnectThunk(void* self, int32_t peer) {
        static_cast<ClientGateway*>(self)->onPlaneReconnect(peer);
    }
    /// Test-only: this node's id (the test factories use 0).
    void setNodeIdForTest(int32_t id) noexcept { thisNodeId_ = id; }
    /// Test-only: the token of the last marker issued (tokens carry a boot incarnation).
    [[nodiscard]] int64_t lastMarkerTokenForTest() const noexcept { return nextMarkerToken_ - 1; }
    /// Test-only: doWork's marker step at `nowNs` (test gateways have no Aeron to poll).
    int pumpTxnMarkersForTest(int64_t nowNs) { nowNs_ = nowNs; return pumpTxnMarkers_(); }
    [[nodiscard]] std::size_t remoteMarkerIndexSizeForTest() const noexcept { return lastRemoteMarker_.size(); }
    /// Re-review 2 MEDIUM-2: a producer expired on this node — forget its
    /// remote-marker idempotence entries (it can have no open transaction left
    /// for a stale copy to close).  Control thread.  Cold.
    void onProducerExpired(int64_t producerId);
    /// Test-only: remote markers that came back LANDED (re-review LOW-1).
    [[nodiscard]] uint64_t remoteMarkersLandedForTest() const noexcept { return remoteMarkersLanded_; }
    [[nodiscard]] std::size_t remoteMarkersInFlightForTest() const noexcept {
        std::size_t n = 0;
        for (const auto& [t, m] : markersInFlight_) n += m.remoteNode >= 0 ? 1u : 0u;
        return n;
    }

    /// F15 m3: drop every marker still waiting in markerSendQueue_ (never
    /// handed to a worker) whose coordinatorEpoch < @p olderThan.  Markers
    /// already on a worker ring land (they carry a durable decision and are
    /// idempotent); their replies are then ignored by the coordinator.
    /// \returns the number of markers purged.  Control thread only.
    std::size_t purgeQueuedMarkers(int32_t olderThan);

    // -----------------------------------------------------------------------
    // TX-4 Task 9 test hooks — feed the per-partition LSO tracker from external
    // code (e.g. integration tests that append batches directly to the log
    // rather than going through handleProduce / appendControlRecord).
    //
    // In production these are called automatically from handleProduce and
    // appendControlRecord.  In tests, call them after appendToLog() to keep
    // the tracker in sync.
    //
    // Both must be called on the duty-cycle thread (single-writer).
    // -----------------------------------------------------------------------

    // Feed the LsoTracker: a transactional data batch (producerId >= 0) was
    // appended at baseOffset on (topicId, partition).
    void notifyTransactionalBatch(int32_t topicId, int32_t partition,
                                  int64_t producerId, int64_t baseOffset) noexcept;

    // Feed the LsoTracker + AbortedTxnIndex: a control marker was appended for
    // producerId at markerOffset on (topicId, partition).
    // commit=true → COMMIT marker, commit=false → ABORT marker.
    void notifyControlMarker(int32_t topicId, int32_t partition,
                             int64_t producerId, int64_t markerOffset,
                             bool commit) noexcept;

    // -----------------------------------------------------------------------
    // CG-4: Group-push API — called by GroupCoordinationService on rebalance.
    // These are rebalance-time (cold-ish) operations; NOT on the hot push path.
    // All three must be called from the single duty-cycle thread (single-writer).
    // -----------------------------------------------------------------------

    // addGroupSubscription — create an internal push subscription for a group
    // member, seeded from fromOffset.  Reuses the SAME Subscription struct and
    // pushSubscriptions() machinery.  Marks the subscription as a group sub so
    // revokeGroupSubscriptions() removes it without touching client subs.
    // Returns subscriptionId (> 0) on success, or -1 if sessionId is unknown.
    //
    // TX-6b: `pendingCarrier` — set to true only when GroupCoordinationService
    // creates the sub with the conservative default isolation=1 and expects a
    // Subscribe carrier frame to arrive with the true isolation level.  When
    // true, the push loop stalls at the first aborted batch until the carrier
    // arrives (preventing nextOffset from advancing past aborted records under
    // the wrong isolation).  Pass false (the default) for all other callers
    // (explicit isolation, tests, etc.) — the sub behaves normally from the start.
    int64_t addGroupSubscription(int64_t memberSessionId,
                                 int32_t topicId,
                                 int32_t partition,
                                 int64_t fromOffset,
                                 uint8_t isolation,
                                 bool    pendingCarrier = false);

    // revokeGroupSubscriptions — remove ALL push subscriptions that were created
    // via addGroupSubscription for a member session.  Normal client subscriptions
    // are unaffected.  No-op if the session has no group subscriptions.
    void revokeGroupSubscriptions(int64_t memberSessionId);

    // -----------------------------------------------------------------------
    // Catalog history Task 8: the topic `topicId` was DROPPED (control thread,
    // BrokerNode's drop effect, right after TopicRegistry::dropTopic).  Every
    // live subscription of it — plain and group-created — ends EXPLICITLY: its
    // Subscribe correlation id is answered ErrorResponse{UnknownTopic} (queued +
    // retried, never best-effort), it is dropped here and on its owning worker.
    // Producers learn it on their next Produce (UnknownTopic).  Cold.  Returns
    // the number of subscriptions ended.
    // -----------------------------------------------------------------------
    std::size_t endTopicSubscriptions(int32_t topicId);

    /// Catalog history Task 8 fix round 1 (M5): forget every per-partition
    /// state the gateway keeps for the dropped topic — its txn / LSO state, its
    /// inline producer-state proofs, and its Logs in the worker-owned / dirty
    /// sets (a freed Log's address can be reused by a new Log).  `logs` are the
    /// topic's Logs, taken before the registry dropped it.  Cold.
    void forgetDroppedTopic(int32_t topicId, int32_t partitions, std::span<rune::storage::Log* const> logs);

    // sendControlFrame — send an already-encoded frame (e.g. Assign) on a
    // session's egress.  Reuses the same sendTo path as all other responses.
    // Returns true if the frame was accepted by the egress publication.
    bool sendControlFrame(int64_t sessionId, const uint8_t* buf, std::size_t len);

    // -----------------------------------------------------------------------
    // fsyncgate fail-stop (Task 10 fix 1): invoked on THIS duty-cycle thread when
    // a worker signals StorageFailure over its return ring, or when the gateway's
    // own flushDirtyLogs sync fails.  BrokerNode wires it to its halt path.
    // Cold path (std::function is fine).  Called at most once.
    // -----------------------------------------------------------------------
    using StorageFailureHandler = std::function<void(const std::string& logDir, int err)>;
    void setStorageFailureHandler(StorageFailureHandler h) { storageFailureHandler_ = std::move(h); }
    /// Invariant violations detected on the duty-cycle thread (halt, exit 70).
    using InvariantViolationHandler = std::function<void(const std::string& what)>;
    void setInvariantViolationHandler(InvariantViolationHandler h) { invariantViolationHandler_ = std::move(h); }

    // -----------------------------------------------------------------------
    // CG-4 test / introspection hooks — not used in production code paths.
    // -----------------------------------------------------------------------

    // setOnControlFrame — optional callback invoked by sendControlFrame BEFORE
    // the actual egress send (regardless of whether the send succeeds).  Useful
    // in tests to capture Assign frames without a live Aeron egress publication.
    // Pass nullptr to clear.  Single-writer: call only on the duty-cycle thread.
    using ControlFrameCallback = std::function<void(int64_t sessionId,
                                                    const uint8_t* buf,
                                                    std::size_t len)>;
    void setOnControlFrame(ControlFrameCallback cb) noexcept {
        onControlFrame_ = std::move(cb);
    }

    // setOnEgressFrame — optional callback invoked by sendTo() when the session
    // egress publication is null (i.e. an injected test session).  The callback
    // receives (sessionId, encoded frame bytes, length) BEFORE the null-egress
    // early-return fires, so tests can capture Records pushes and Assign frames
    // that would otherwise be silently dropped because there is no real Aeron
    // publication.  Pass nullptr to clear.  Single-writer: call only on the
    // duty-cycle thread.  The callback fires and sendTo returns true (as if
    // successfully sent) — callers (pushSubscriptions) advance nextOffset.
    using EgressFrameCallback = std::function<void(int64_t sessionId,
                                                   const uint8_t* buf,
                                                   std::size_t len)>;
    void setOnEgressFrame(EgressFrameCallback cb) noexcept {
        onEgressFrame_ = std::move(cb);
    }

    // groupMemberSubCount — number of group push subscriptions tracked for a
    // given member session.  Returns 0 if the session has no group subs.
    // For tests to assert that addGroupSubscription added the right entries.
    [[nodiscard]] std::size_t groupMemberSubCount(int64_t memberSessionId) const noexcept {
        const auto it = groupMemberSubs_.find(memberSessionId);
        return (it != groupMemberSubs_.end()) ? it->second.size() : 0u;
    }

    // injectTestSession — install a fake session with no real egress publication.
    // The ExclusivePublication shared_ptr is null; sendTo will call pub.isConnected()
    // which will crash unless sendControlFrame uses the onControlFrame_ callback
    // only path (or the session has a real pub).  Tests must install onControlFrame_
    // if they want to capture frames, and set topicRegistry_ before calling
    // addGroupSubscription.
    //
    // Returns the allocated sessionId.
    // Single-writer: call only on the duty-cycle thread or before doWork() starts.
    // D6: clientVersion = the session's ConnectRequest.clientVersion (default:
    // the current client, which understands UnknownProducerId).
    int64_t injectTestSession(int64_t nowNs = 0,
                              int32_t clientVersion = rune::protocol::client::kClientVersion) {
        const int64_t sid = sessions_.nextSessionId();
        sessions_.add(sid, /*egress=*/nullptr, nowNs, {}, 0, clientVersion);
        return sid;
    }
    /// Catalog history Task 9: a test session whose egress CHANNEL is known (the
    /// DDL principal's source address is the session's egress channel).
    int64_t injectTestSessionOnChannel(std::string egressChannel, int64_t nowNs = 0) {
        const int64_t sid = sessions_.nextSessionId();
        sessions_.add(sid, /*egress=*/nullptr, nowNs, std::move(egressChannel), 0,
                      rune::protocol::client::kClientVersion);
        return sid;
    }

    // -----------------------------------------------------------------------
    // sessionCount — number of active sessions.
    // -----------------------------------------------------------------------
    [[nodiscard]] std::size_t sessionCount() const {
        return sessions_.size();
    }

    // -----------------------------------------------------------------------
    // subscriptionCount — number of active subscriptions (for tests).
    // -----------------------------------------------------------------------
    /// Catalog history Task 12 fix round 0 — control thread: catalog-read
    /// flow control.  A Records frame of a catalog subscription is OFFERED, never
    /// spilled: BackPressured when answers wait in the session's spill (order) or
    /// the publication cannot take it now; the caller pauses the subscription.
    enum class CatalogOffer : uint8_t { Sent, BackPressured, NoSession };
    CatalogOffer tryOfferCatalogFrame(int64_t sessionId, const uint8_t* buf, std::size_t len);
    /// Whether a paused catalog reader's session has room again: its spill is
    /// empty and its publication's window holds at least catalogResumeThreshold
    /// (or it is gone).
    enum class CatalogRoom : uint8_t { Ready, NotYet, Gone };
    static constexpr int64_t kCatalogResumeWindowBytes = 16 * 1024;
    /// Fix round 1 (C2): min(16 KiB, term / 2, the largest window observed / 2),
    /// at least 1 — reachable by ANY receiver window (a constant could exceed it: a hang).
    [[nodiscard]] static constexpr int64_t catalogResumeThreshold(int64_t termLength, int64_t windowSeen) noexcept {
        int64_t t = kCatalogResumeWindowBytes;
        if (termLength > 0) t = std::min(t, termLength / 2);
        if (windowSeen > 0) t = std::min(t, windowSeen / 2);
        return std::max<int64_t>(t, 1);
    }
    CatalogRoom catalogEgressRoom(int64_t sessionId);
    /// Test seam (egress-less sessions): while set, every catalog offer to `sid`
    /// is back-pressured and its room NotYet.
    void setCatalogBackPressureForTest(int64_t sid, bool on) { catalogBackPressureForTest_ = on ? sid : 0; }

    /// Catalog history Task 12: catalog Subscribes forwarded to the admin thread /
    /// catalog Unsubscribes waiting for room on its request ring.
    [[nodiscard]] uint64_t catalogSubscribesForwarded() const noexcept { return catalogSubscribesForwarded_; }
    [[nodiscard]] std::size_t pendingCatalogUnsubscribes() const noexcept { return pendingCatalogUnsubs_.size(); }
    [[nodiscard]] uint64_t catalogUnsubscribesDropped() const noexcept { return catalogUnsubsDropped_; }
    [[nodiscard]] uint64_t catalogClosesLeftToTheSweep() const noexcept { return catalogClosesToSweep_; }
    /// Catalog history Task 12 (N5) — control thread: a view sink was applied here
    /// (kA2cCatalogSinkApplied); its plan is not on every worker yet.  Then: its
    /// plan reached every worker's install ring (or the view was dropped).
    void noteViewSinkAwaitingPlan(int32_t viewTopicId);
    void noteViewPlanDelivered(int32_t viewTopicId);
    [[nodiscard]] bool viewSinkAwaitingPlan(int32_t viewTopicId) const noexcept {
        for (const int32_t id : viewSinksAwaitingPlan_) if (id == viewTopicId) return true;
        return false;
    }
    [[nodiscard]] std::size_t subscriptionCount() const {
        return subscriptions_.size();
    }

    // -----------------------------------------------------------------------
    // applySubscribeForTest — feed a raw Subscribe frame through the real
    // handleSubscribe() path (test seam).  Lets integration tests exercise the
    // group-consumer isolation-carrier merge without a live Aeron client.
    // Single-writer: call only on the duty-cycle thread or before doWork().
    // -----------------------------------------------------------------------
    void applySubscribeForTest(const uint8_t* buf, std::size_t len) {
        handleSubscribe(buf, len);
    }
    /// Test seam: feed a raw Unsubscribe frame through handleUnsubscribe().
    void applyUnsubscribeForTest(const uint8_t* buf, std::size_t len) {
        handleUnsubscribe(buf, len);
    }
    /// Test seam: one gateway push pass (pushSubscriptions) without doWork's
    /// Aeron polling.
    void pushSubscriptionsForTest() { pushSubscriptions(); }
    /// Plain subscriptions answered NotLeader + dropped because this node
    /// stopped serving them (read_committed after a demotion; any isolation
    /// after the replica was removed) — retireReadCommittedMovedAway_.
    [[nodiscard]] uint64_t readCommittedSubscriptionsMovedAway() const noexcept { return readCommittedMovedAway_; }
    static constexpr int64_t kMovedAwayScanEveryCyclesForTest = 16;   // == kMovedAwayScanEveryCycles
    static constexpr int     kMovedAwayLookupsPerSliceForTest = 32;   // == kMovedAwayLookupsPerSlice
    static constexpr int64_t kMovedAwaySummaryEveryNsForTest  = 1'000'000'000;   // == kMovedAwaySummaryEveryNs
    static constexpr int64_t kRoutedSweepMinGapCyclesForTest  = 64;   // == kRoutedSweepMinGapCycles
    /// M6: transient-partition classifications the push loop made (per registry change, not per cycle).
    [[nodiscard]] uint64_t transientClassificationsForTest() const noexcept { return transientClassifications_; }
    /// M4: routed moved-away sweeps run so far (coalesced role changes).
    [[nodiscard]] uint64_t routedMovedAwaySweepsForTest() const noexcept { return routedMovedAwaySweeps_; }
    /// N2: moved-away summaries logged, and the count the last one reported.
    [[nodiscard]] uint64_t movedAwaySummariesForTest() const noexcept { return movedAwaySummaries_; }
    [[nodiscard]] uint64_t lastMovedAwaySummaryCountForTest() const noexcept { return lastMovedAwaySummaryCount_; }
    /// N1: the most movedAway_ probes any fallback slice made so far.
    [[nodiscard]] int movedAwayMaxProbesPerSliceForTest() const noexcept { return movedAwayMaxProbesPerSlice_; }
    /// N1 test seam: put every subscription in as few buckets as possible (a
    /// pathological collision pattern the fallback slice must stay bounded on).
    void squeezeSubscriptionBucketsForTest() {
        subscriptions_.max_load_factor(1.0e9f);
        subscriptions_.rehash(0);
    }

    // -----------------------------------------------------------------------
    // applyProduceForTest — feed a raw Produce frame through the real
    // handleProduce() path (test seam).  Lets integration tests exercise the
    // full produce routing, acks=All bootstrap rejection, and GR-4/GR-5 gates
    // without a live Aeron client publishing ingress frames.
    // Single-writer: call only on the duty-cycle thread or before doWork().
    // -----------------------------------------------------------------------
    void applyProduceForTest(const uint8_t* buf, std::size_t len) {
        (void)handleProduce(buf, len);
    }

    // -----------------------------------------------------------------------
    // E5.1: applyMetadataForTest — feed a raw MetadataRequest frame through
    // the real handleMetadata() path (test seam), parallel to applyProduceForTest.
    // Single-writer: call only on the duty-cycle thread or before doWork().
    // -----------------------------------------------------------------------
    void applyDisconnectForTest(const uint8_t* buf, std::size_t len) { handleDisconnect(buf, len); }
    void applyGroupForTest(const uint8_t* buf, std::size_t len, uint16_t tid, int64_t nowNs = 0) {
        handleGroupMessage(buf, len, nowNs, tid);
    }
    void applyTxnForTest(const uint8_t* buf, std::size_t len, uint16_t tid, int64_t nowNs = 0) {
        handleTxnMessage(buf, len, nowNs, tid);
    }
    [[nodiscard]] uint64_t coordinatorRedirectsForTest() const noexcept { return coordinatorRedirects_; }
    // No-hang wave (R2) test seams: the full ingress dispatch (checkFrame +
    // every handler) and the reject / unanswerable counters.
    bool applyClientFrameForTest(const uint8_t* buf, std::size_t len) { return onClientFrame_(buf, len); }
    void applyGetViewRowForTest(const uint8_t* buf, std::size_t len) { handleGetViewRow(buf, len); }
    [[nodiscard]] uint64_t rejectedRequestsForTest() const noexcept { return rejectedRequests_; }
    // No-hang fix round 2: inject a session with a NON-NULL placeholder egress
    // (never dereferenced: makeForTest's TestEgressFn intercepts every offer of
    // the reliable answer path), so tests exercise the production answer path
    // (spill, per-session FIFO, spill-full expiry) — no test flag on it.
    // Fix round 2 (I5): a handshake in progress (awaitingHandshake, placeholder
    // reply publication intercepted by the TestEgressFn).
    void injectPendingHandshakeForTest(int64_t sessionId);
    void pollPendingConnectsForTest(int64_t nowNs) { pollPendingConnects(nowNs); }
    int64_t injectReliableTestSessionForTest(int64_t nowNs = 0,
                                             int32_t clientVersion = rune::protocol::client::kClientVersion);
    [[nodiscard]] std::size_t egressSpillUsedBytesForTest() const noexcept { return egressSpill_.usedBytes(); }
    /// Fix round 4: a session's spill state / pool bytes held (test seam; Normal / 0 when unknown).
    [[nodiscard]] SpillState spillStateForTest(int64_t sid) noexcept {
        const Session* s = sessions_.find(sid);
        return s ? s->spill.state : SpillState::Normal;
    }
    [[nodiscard]] std::size_t spilledBytesForTest(int64_t sid) noexcept {
        const Session* s = sessions_.find(sid);
        return s ? s->spill.bytes : 0;
    }
    [[nodiscard]] std::size_t spillReadySessionsForTest() const noexcept { return spillReadySessions_; }
    /// Fix round 4 (N12): the reply-host string a pending connect still owns (null once the ring took it).
    [[nodiscard]] const void* pendingResolveHostForTest(std::size_t i) const noexcept {
        return i < pendingConnects_.size() ? pendingConnects_[i].resolveHost.get() : nullptr;
    }
    /// Fix round 4: the spill's sizing (tests / benches).
    struct SpillSizing { std::size_t total, pool, stage, sessionQuota; };
    static constexpr SpillSizing spillSizing() noexcept;
    /// Fix round 5 (N20): a worker reports `sid` Held (as a SessionSignal Held does).
    void markSessionHeldForTest(int64_t sid) {
        if (Session* s = sessions_.find(sid); s && s->heldByWorkers == 0) { s->heldByWorkers = 1; ++heldSessions_; }
    }
    /// Fix round 4 (N9): put a session in the Expiring state exactly as a full spill does.
    void expireSessionForSpillForTest(int64_t sid) {
        if (Session* s = sessions_.find(sid)) expireForSpill_(*s);
    }
    [[nodiscard]] uint64_t oldClientUnknownMemberAnswersForTest() const noexcept { return oldClientUnknownMemberAnswers_; }
    [[nodiscard]] uint64_t oldClientSessionWideEvictionsForTest() const noexcept { return oldClientSessionWideEvictions_; }
    [[nodiscard]] bool sessionExistsForTest(int64_t sid) noexcept { return sessions_.find(sid) != nullptr; }
    [[nodiscard]] uint64_t unanswerableFramesForTest() const noexcept { return unanswerableFrames_; }
    void applyMetadataForTest(const uint8_t* buf, std::size_t len) {
        handleMetadata(buf, len);
    }

    // -----------------------------------------------------------------------
    // Task 8a: DDL test seams — feed a raw DefineTopic (64) / GetTopicSchema (66)
    // frame through the real handler path (route to the admin sink).
    // Single-writer: call only on the duty-cycle thread or before doWork().
    // -----------------------------------------------------------------------
    void applyDefineTopicForTest(const uint8_t* buf, std::size_t len) {
        handleDefineTopic(buf, len);
    }
    void applyGetTopicSchemaForTest(const uint8_t* buf, std::size_t len) {
        handleGetTopicSchema(buf, len);
    }
    // Task 14: AlterTopic (68) test seam — mirrors applyDefineTopicForTest.
    void applyAlterTopicForTest(const uint8_t* buf, std::size_t len) {
        handleAlterTopic(buf, len);
    }
    // Task 19: CreateView (70) test seam — mirrors applyDefineTopicForTest.
    void applyCreateViewForTest(const uint8_t* buf, std::size_t len) {
        handleCreateView(buf, len);
    }
    /// Catalog history Task 9 fix round 1 (Minor 1): a topic a consumer group may be
    /// assigned — served here and, a catalog topic, PLACED (its committed routing
    /// image names a leader for every partition; C1 generalized, spec §5.4).
    /// Control thread; cold (a rebalance).
    [[nodiscard]] bool topicAssignable(int32_t topicId) const noexcept;
    // Catalog history Task 9: any DDL frame through the real dispatch path.
    void applyDdlFrameForTest(const uint8_t* buf, std::size_t len) {
        const auto h = rune::protocol::client::MessageHeader::decode(buf, len);
        if (!h) return;
        namespace pc = rune::protocol::client;
        switch (static_cast<pc::TemplateId>(h->templateId)) {
        case pc::TemplateId::DefineTopic:      handleDefineTopic(buf, len); break;
        case pc::TemplateId::AlterTopic:       handleAlterTopic(buf, len); break;
        case pc::TemplateId::CreateView:       handleCreateView(buf, len); break;
        case pc::TemplateId::DropTopic:        handleDropTopic(buf, len); break;
        case pc::TemplateId::AlterTopicConfig: handleAlterTopicConfig(buf, len); break;
        case pc::TemplateId::DropView:         handleDropView(buf, len); break;
        case pc::TemplateId::AlterView:        handleAlterView(buf, len); break;
        default: break;
        }
    }

    // -----------------------------------------------------------------------
    // subscriptionIsolationForTest — return the isolation level (0/1) of the
    // subscription matching (sessionId, topicId, partition), or -1 if none.
    // For tests to assert the carrier merge updated the group sub in place.
    // -----------------------------------------------------------------------
    [[nodiscard]] int subscriptionIsolationForTest(int64_t sessionId, int32_t topicId,
                                                   int32_t partition) const {
        for (const auto& [subId, sub] : subscriptions_) {
            if (sub.sessionId == sessionId && sub.topicId == topicId &&
                sub.partition == partition) {
                return static_cast<int>(sub.isolation);
            }
        }
        return -1;
    }

    // F19: the push cursor (nextOffset) of subscription `subscriptionId`, or -1
    // if no such subscription.  Tests assert a non-deliverable batch is never
    // silently skipped (the cursor does not move past it).
    [[nodiscard]] int64_t subscriptionNextOffsetForTest(int64_t subscriptionId) const {
        const auto it = subscriptions_.find(subscriptionId);
        return it == subscriptions_.end() ? -1 : it->second.nextOffset;
    }

    // -----------------------------------------------------------------------
    // pendingConnectCount — number of in-flight async egress-resolve entries.
    // Includes both the main pending table and the draining-release table.
    // Used by tests to assert that dropped/timed-out entries drain to zero.
    // -----------------------------------------------------------------------
    [[nodiscard]] std::size_t pendingConnectCount() const {
        return pendingConnects_.size() + drainingRegistrations_.size();
    }

private:
    // -----------------------------------------------------------------------
    // PendingAck — one deferred acks=All produce waiting for ISR HW to cover it.
    //
    // Enqueued in pendingAcks_ (pre-sized vector used as a ring — zero heap
    // alloc on the hot path).  Drained in drainPendingAcks() each doWork()
    // cycle after the replica's HW has advanced for this cycle.
    //
    // Safety invariants (Task 10 round 4, Important 3):
    //   - NO replica pointer is held.  The entry names its replica by
    //     (topicId, partition, incarnation) and drainPendingAcks resolves it
    //     through the ReplicaRegistry on EVERY check.  A miss or a different
    //     incarnation (the partition was removed / re-created) = stale: the
    //     ack fails RETRIABLY with NotLeader — nothing freed is dereferenced.
    //   - incarnation 0 = the replica was not registered (unit-test fixtures):
    //     resolved by (topicId, partition) alone.
    //   - All fields set once on enqueue; read-only during drain.
    //   - Single-writer: only touched on the duty-cycle thread.
    // -----------------------------------------------------------------------
    struct PendingAck {
        int64_t      sessionId     = 0;   ///< owning session
        int64_t      correlationId = 0;   ///< echoed back in ProduceResponse
        int32_t      topicId       = 0;   ///< replica identity (with partition + incarnation)
        int32_t      partition     = 0;   ///< partition (echoed in ProduceResponse)
        uint32_t     incarnation   = 0;   ///< replica incarnation at enqueue (0 = unregistered)
        int64_t      baseOffset    = 0;   ///< first offset of the batch
        int64_t      lastOffset    = 0;   ///< last offset of the batch (HW must reach this)
        int64_t      enqueuedAtNs  = 0;   ///< for timeout eviction
        /// The replica's logGeneration() the waiter last validated against
        /// (truncation / reconcile: the worker path's §3.2 rule, see
        /// inlineWaiterStillValid_).
        uint64_t     logGen        = 0;
    };

    // -----------------------------------------------------------------------
    // No-hang fix rounds 2-4 (I1 / N8): the egress SPILL — every back-pressured
    // client answer is kept, in per-session order, in a preallocated block pool
    // (SpillPool: one intrusive chain per Session) and re-offered by
    // drainPendingResponses, which walks only the READY list of sessions that
    // hold spilled answers (O(1) per spilling session per duty cycle; a session
    // whose publication has no window costs one compare).  There is NO
    // best-effort fallback: a session the spill cannot hold is EXPIRED
    // explicitly (SessionExpired, then closed — every pending client op
    // completes SessionLost), never silently short of an answer.
    //
    // Memory: kEgressSpillBytes in total, allocated once — the block pool plus
    // the staging buffer a multi-block answer is gathered into for its offer.
    // -----------------------------------------------------------------------
    static constexpr std::size_t kEgressSpillBytes     = 4u << 20;     // 4 MiB in total
    static constexpr std::size_t kMaxSpillRecordBytes  = 256u << 10;   // the staging buffer (largest spillable answer)
    static constexpr std::size_t kEgressSpillPoolBytes = kEgressSpillBytes - kMaxSpillRecordBytes;
    // Fix round 3 (I1 fairness) / round 4 (N10): one session may hold at most
    // this many POOL bytes (blocks, headers included) — 8 x quota == the pool;
    // the session over quota is the one expired (never an innocent pusher).
    static constexpr std::size_t kSessionSpillQuota = kEgressSpillPoolBytes / 8;
    // Fix round 3 (I1 SessionExpired delivery): the explicit EXPIRING state of a
    // session the spill could not hold — only what that state needs.  Its
    // SessionExpired is re-offered every drain until delivered (then the session
    // is closed) or until the lease deadline (closed anyway, counted lost).
    // Preallocated; a full table leaves the session marked (answers refused) and
    // adopts it as soon as a slot frees.
    struct SpillExpiring {
        int64_t sessionId  = 0;
        int64_t deadlineNs = 0;
        bool    subscriptionsDropped = false;   // fix round 4 (N9): its push subscriptions ended
    };
    static constexpr std::size_t kMaxSpillExpiring = 256;

    // -----------------------------------------------------------------------
    // Subscription — per-session push subscription (GB-3 / GR-6).
    //
    // nextOffset tracks the exact offset to push next.  The push loop advances
    // it ONLY after a successful tryClaim (exactly-once delivery guarantee).
    // isolation: 0 = read_uncommitted, 1 = read_committed.
    //   GR-6: BOTH modes are bounded by replica->highWatermark() when a replica
    //   is wired.  This prevents delivery of truncatable (uncommitted) records
    //   that a leader-failover would truncate.  "read_uncommitted" here means
    //   "allow open-transaction records" (Kafka semantics), NOT "allow records
    //   above majority-HW" — those are never safe to deliver.
    //   TODO(Milestone C): read_committed additionally uses the real LSO (log-
    //   stable-offset) once transactions land; LSO ≤ HW always.
    // CG-4: isGroup marks subscriptions created via addGroupSubscription so
    // that revokeGroupSubscriptions() can remove exactly those and leave
    // normal client-initiated subscriptions untouched.
    // -----------------------------------------------------------------------
    struct Subscription {
        int64_t subscriptionId = 0;
        int64_t sessionId      = 0;
        // The correlationId of the client's Subscribe that created it: stamped
        // on every Records push so the client binds the push to ITS request
        // (exact instance identity, before and after the SubscribeResponse).
        int64_t subscribeCorrId = 0;
        int32_t topicId        = 0;
        int32_t partition      = 0;
        int64_t nextOffset     = 0;
        uint8_t isolation      = 0;   // 0=read_uncommitted, 1=read_committed
        bool    isGroup        = false; // CG-4: true = created by addGroupSubscription
        // TX-6b: group sub starts with the conservative default isolation=1.
        // groupCarrierReceived tracks whether the Subscribe carrier (which carries
        // the true isolation) has arrived from the client.  Until it arrives, the
        // push loop stalls at the first transactional batch that would be abort-
        // filtered so that nextOffset does NOT advance past aborted records before
        // the true isolation is known.  Once the carrier arrives (flag=true) the
        // correct isolation is applied on the next duty cycle — no data loss, no
        // premature skip under a wrong isolation assumption.
        bool    groupCarrierReceived = false;
        // ONE failure state for a push subscription (F15 S3 re-review 1 I-4 ⇄
        // F19, merged in integration wave 2), two error paths:
        //   - the owning worker answered a PushRead MessageTooLarge (a stored
        //     batch larger than its return ring), or
        //   - the next stored batch's Records frame exceeds this session's
        //     egress publication maxMessageLength (F19).
        // Either way the subscription is parked AT that batch (nextOffset not
        // advanced, no further PushRead / reads / pushes: no spin) until the
        // client unsubscribes, and owes the client one non-retriable
        // ErrorResponse{MessageTooLarge, correlationId 0, message naming the
        // subscription} (rune::node::encodePushTooLargeError) — re-offered each
        // cycle until sent (errorOwed).
        bool    failed         = false;
        bool    errorOwed      = false;
        rune::node::PushTooLargeCause failedCause = rune::node::PushTooLargeCause::EgressFrame;
        std::size_t failedFrameLen = 0;   // EgressFrame: the undeliverable Records frame size
        // F15 S4 addendum R-1 / R-3 (worker-pool PushRead path): the cursor and
        // the HW snapshot of the PushRead in flight, and the HW snapshot at
        // which the owning worker answered "nothing below its bound" (-1: not
        // parked).  The subscription is not re-read until the gateway's HW
        // snapshot moves past parkedAtHw, or its isolation carrier arrives.
        int64_t issuedNextOffset = -1;
        int64_t issuedHw         = -1;
        int64_t parkedAtHw       = -1;
        // Review I-2 fallback: push-cycle backoff after a notReady answer.
        int64_t notReadyBackoff  = 0;
        int64_t retryAtCycle     = 0;
        // E2 Task 13 fix round 1: a replica of this partition was registered
        // here when it was created — its later REMOVAL (the partition moved off
        // this node) retires it NotLeader (retireMovedAway_), any isolation.
        bool    hadReplica       = false;
        // E2 final fix wave (M6): a partition with no disk Log is classified
        // (servedPartition_: Transient → its replica) ONCE per registry change
        // — keyed by the replica registry's generation (any add / remove) and
        // the catalog's — never re-probed every push cycle.  transientReplica
        // is valid only while both generations still match (checked first).
        // E2 final fix wave (M2): the cursor is still the bind-time resolved
        // start (no worker answer adopted yet) — sent with every PushRead
        // (WorkerOp::freshCursor) so a transient ring start above it is not
        // counted / warned as a reader that fell behind.
        bool cursorFresh = true;
        const rune::replication::PartitionReplica* transientReplica = nullptr;
        uint64_t transientClassRegGen = UINT64_MAX;
        uint64_t transientClassCatGen = UINT64_MAX;
    };

    // -----------------------------------------------------------------------
    // PendingConnect — tracks an in-flight async egress publication resolve.
    // Stored in pendingConnects_ until resolved or timed out.
    // All fields captured at Connect-request time; leaderView sampled at connect.
    // -----------------------------------------------------------------------
    struct PendingConnect {
        int64_t     registrationId  = 0;   // from addExclusivePublication
        int64_t     sessionId       = 0;   // pre-allocated
        int64_t     correlationId   = 0;
        int64_t     requestedAtNs   = 0;   // for timeout
        bool        isLeader        = true; // captured at Connect time
        // leaderClientEndpoint captured at Connect time (for redirect path).
        // If !isLeader && !empty → send Redirect instead of OK.
        std::string leaderClientEndpoint;

        // DR-BUG-2 FIX: once the egress ExclusivePublication resolves it is
        // stashed here so subsequent doWork() cycles can retry the
        // ConnectResponse offer while the Aeron UDP link is still warming up.
        // null = publication not yet resolved; non-null = resolved, retrying send.
        std::shared_ptr<aeron::ExclusivePublication> resolvedPub;

        // Hybrid egress: the client's egress endpoint (from the Connect request),
        // carried to the Session so a per-consumer unicast Records publication can
        // be created lazily on the first subscribe.
        std::string egressChannel;
        int32_t     egressStreamId = 0;
        int32_t     clientVersion  = 0;   ///< D6: ConnectRequest.clientVersion

        // Task 13 (spec §3.4): every worker has this session's reply destination
        // queued (announce-before-accept) — set once, so a ConnectResponse retry
        // never re-announces.  announceBegun (fix round 1, I2): at least one
        // worker may hold it — it counts against the shard session cap, and
        // EVERY drop of this entry (timeout, eviction, error) closes it on every
        // worker.
        bool        announced     = false;
        bool        announceBegun = false;
        // Fix round 2 (LOW-4): the client's reply host is a NAME — resolved off
        // the duty thread (PeerResolverHub); no publication registered yet
        // (registrationId unused) until the numeric endpoint is known.
        bool        awaitingResolve = false;
        // Fix round 3: the resolve ring refused the reply-host offer — retried
        // each poll until accepted or the connect timeout (never a silent drop).
        bool        resolveOfferPending = false;
        // Fix round 4 (N12): the reply host handed to the resolver, allocated
        // ONCE per pending connect (owned here until the ring accepts it) — a
        // refused offer is retried without a new allocation.
        std::unique_ptr<std::string> resolveHost;
        // Fix round 3 (N4): the explicit FailingHandshake state — HandshakeFailed
        // (on handshakeFailCorr) re-offered each poll until delivered / timeout.
        bool        failingHandshake  = false;
        int64_t     handshakeFailCorr = 0;

        // ---------------------------------------------------------------
        // client<->broker TLS/AEAD (Task 7) — server-role handshake state.
        // Engaged only when the client's ConnectRequest offered crypto
        // (non-empty cipherList + handshakeBytes) AND this gateway was
        // configured with a cert/key; nullopt for the whole plaintext path
        // (byte-identical to pre-Task-7).  `chosenCipher`/`hsBytes` mirror
        // the client's own SessionMachine fields (task-7-interfaces.md
        // points at them as the canonical shape): hsBytes holds the
        // server's own pending outbound handshake bytes, drained into the
        // NEXT ConnectResponse/HandshakeContinueResponse leg and cleared
        // right after — never grows unbounded across legs.
        // ---------------------------------------------------------------
        std::optional<rune::crypto::TlsSession> tls;
        rune::crypto::Cipher                    chosenCipher = rune::crypto::Cipher::None;
        std::vector<uint8_t>                    hsBytes;
        // Set once the first ConnectResponse (carrying chosenCipher +
        // the server's first flight) has been sent: this entry stays alive
        // in pendingConnects_ (not erased, no Session yet) until the
        // client's HandshakeContinue legs drive tls->done() — see
        // handleHandshakeContinue_.  Mutually exclusive with `tls ==
        // nullopt` being erased immediately (plaintext path, unchanged).
        bool        awaitingHandshake = false;
        // Set when the server-role TlsSession rejected the client's
        // opening handshake bytes (malformed/invalid ClientHello, a bad
        // token embedded in it, cert/SNI mismatch, ...) or (later, in
        // handleHandshakeContinue_) exportKeys() returned the
        // Cipher::None fail-closed sentinel.  pollPendingConnects answers
        // ConnectResponse{HandshakeFailed} and creates no session — never
        // installs all-zero keys.
        bool        cryptoFailed = false;
        // Fix round 2: non-zero = answer ConnectResponse{refuseCode} on the
        // resolved reply publication and create NO session (explicit refusal).
        uint16_t    refuseCode = 0;
    };

    // -----------------------------------------------------------------------
    // Fragment handler — called by FragmentAssembler for each reassembled frame.
    // -----------------------------------------------------------------------
    void onFragment(aeron::concurrent::AtomicBuffer& buffer,
                    aeron::util::index_t             offset,
                    aeron::util::index_t             length,
                    aeron::Header&                   header);

    // -----------------------------------------------------------------------
    // Dispatch helpers
    // -----------------------------------------------------------------------
    void handleConnect   (const uint8_t* buf, std::size_t len, int64_t nowNs);
    // client<->broker TLS/AEAD (Task 7): a later leg of a multi-round TLS
    // handshake, after the initial Connect/ConnectResponse leg (handleConnect)
    // started it.  Looks up the in-flight PendingConnect by sessionId (not yet
    // promoted to sessions_), drives TlsSession::readInbound/writeOutbound, and
    // on done() exports keys (direction-mirrored — see task-7-interfaces.md)
    // and promotes the session.  Unknown sessionId / decode failure → drop.
    void handleHandshakeContinue_(const uint8_t* buf, std::size_t len, int64_t nowNs);
    // Pull every pending outbound handshake byte out of pending.tls into
    // pending.hsBytes (growable — cold path, once per handshake leg; clears
    // first).  No-op if pending.tls is empty.
    static void drainHandshakeOutbound_(PendingConnect& pending);
    void handleKeepalive (const uint8_t* buf, std::size_t len, int64_t nowNs);
    void handlePing      (const uint8_t* buf, std::size_t len, int64_t nowNs);
    void handleDisconnect(const uint8_t* buf, std::size_t len);
    // MD1 Task 10: Metadata request — answerable by any node (reads routingImage_ + nodeEndpointMap_).
    void handleMetadata     (const uint8_t* buf, std::size_t len);
    // GB-2: Produce handler — offset-assignment + leader-local append.
    /// false = deferred, not consumed (D8c per-worker cap; shard mode only).
    bool handleProduce      (const uint8_t* buf, std::size_t len);
    // read-rework T3/T4 (I3b): route subscription lifecycle to the owning worker
    // (per-worker egress).  Look up the Log/replica for (topicId, partition) and
    // enqueue a Subscribe/Unsubscribe WorkerOp via workerPool_->tryEnqueueOp.
    void forwardSubscribeToWorker_(int64_t subId, int64_t sessionId, int32_t topicId,
                                   int32_t partition, int64_t nextOffset, uint8_t isolation,
                                   bool isGroup, bool groupCarrierReceived,
                                   int64_t subscribeCorrId = 0);
    void forwardUnsubscribeToWorker_(int64_t subId, int32_t topicId, int32_t partition);

    // Hybrid egress: a Subscribe deferred until its session's per-consumer UNICAST
    // Records publication resolves (NON-BLOCKING — never sleeps on the control
    // thread).  Buffered in pendingRecordEgress_ and drained by
    // pollPendingRecordEgress_ once the pub is ready.
    struct BufferedSubscribe {
        int64_t subId              = 0;
        int64_t subscribeCorrId    = 0;  // stamped on the worker's Records pushes
        int64_t sessionId          = 0;
        int32_t topicId            = 0;
        int32_t partition          = 0;
        int64_t nextOffset         = 0;
        uint8_t isolation          = 0;
        bool    isGroup            = false;
        bool    groupCarrierReceived = false;
    };
    struct PendingRecordEgress {
        int64_t regId         = -1;  // addExclusivePublication registration (in flight)
        int64_t requestedAtNs = 0;   // for the resolve timeout
        std::vector<BufferedSubscribe> waiters;
    };
    // (sessionId, ownerWorkerId) → in-flight record-egress pub resolve + its
    // buffered subscribes.  ONE pub per worker (single-writer ExclusivePublication).
    std::map<std::pair<int64_t, int32_t>, PendingRecordEgress> pendingRecordEgress_;

    // Build + route a Subscribe WorkerOp with the resolved egress pub (may be null
    // → MDC fallback).
    rune::node::WorkerOp buildSubscribeOp_(const BufferedSubscribe& s, void* egressPub) const;
    void enqueueSubscribeOp_(const BufferedSubscribe& s, void* egressPub);
    // Resolve in-flight record-egress pubs (non-blocking) + drain buffered
    // subscribes.  Called from doWork().
    void pollPendingRecordEgress_(int64_t nowNs);
    // A resolved record-egress pub: cache it on its Session (never replacing a
    // cached one) and forward the buffered subscribes with the CACHED pointer.
    void onRecordEgressResolved_(int64_t sessionId, int32_t workerId, PendingRecordEgress& pend,
                                 std::shared_ptr<aeron::ExclusivePublication> pub);

    // -----------------------------------------------------------------------
    // Record-egress publication lifetime — RETIRE-ON-ACK (2026-10-05 fix).
    //
    // A per-(session, worker) record-egress pub is handed to worker w as a RAW
    // pointer (WorkerOp::egressPub); the worker stores it in its subscription
    // table and writes through it on every push pass.  The gateway must
    // therefore never destroy it while w may still hold it.  When the session
    // closes, each of its pubs moves here (still owned — the shared_ptr never
    // crosses threads) and w is sent a release op on its op ring — the SAME
    // FIFO ring as every Subscribe op that carried the pointer, so it executes
    // after them: the worker drops every subscription referencing the pointer
    // and answers SessionSignal::EgressReleased(token) on its return ring.
    // Only that acknowledgement destroys the pub (onEgressReleased_).  A
    // release op a full ring refuses is retried every doWork (never dropped).
    // Event-driven: no timer decides when a pub is freed.  Cold (close rate).
    // States per entry: Unsent (enqueued=false) → AwaitingAck (enqueued=true)
    // → destroyed on the ack.
    // -----------------------------------------------------------------------
    struct RetiringEgress {
        uint64_t                                      token     = 0;
        int64_t                                       sessionId = 0;
        int32_t                                       worker    = 0;
        std::shared_ptr<aeron::ExclusivePublication>  pub;
        bool                                          enqueued  = false;
    };
    void retireRecordEgress_(int64_t sid, int32_t worker, std::shared_ptr<aeron::ExclusivePublication> pub);
    bool tryEnqueueEgressRelease_(const RetiringEgress& r);
    int  pumpEgressReleases_();
    void onEgressReleased_(int32_t worker, uint64_t token);
    // GB-3: Subscribe/Unsubscribe handlers.
    void handleSubscribe    (const uint8_t* buf, std::size_t len);
    void handleUnsubscribe  (const uint8_t* buf, std::size_t len);
    // CG-3: Group coordination handler — routes templateIds 32–43 to GroupCoordinationService.
    // tid is the raw uint16_t template id (avoids including Protocol.h in this header).
    void handleGroupMessage (const uint8_t* buf, std::size_t len, int64_t nowNs, uint16_t tid);
    // TX-3: Transaction coordination handler — routes templateIds 45,47,49,51,53
    // to TransactionCoordinationService.
    void handleTxnMessage   (const uint8_t* buf, std::size_t len, int64_t nowNs, uint16_t tid);
    // D4: a follower redirects a group / txn coordinator request to the leader.
    bool redirectToCoordinator_(Session& session, int64_t correlationId);
    uint64_t coordinatorRedirects_ = 0;
    // Task 8a: schema DDL handlers — route DefineTopic (64) / GetTopicSchema (66)
    // to the admin sink (ring hand-off).  Validate frame + require known session,
    // decode, copy fields into the sink, RETURN immediately (no hot-path blocking).
    void handleDefineTopic  (const uint8_t* buf, std::size_t len);
    void handleGetTopicSchema(const uint8_t* buf, std::size_t len);
    // Task 14: AlterTopic (68) — same discipline (decode, copy-and-return onto
    // the admin sink); the AlterTopicResponse (69) comes back asynchronously
    // on the session egress via the admin→control egress ring (respondAlter).
    void handleAlterTopic   (const uint8_t* buf, std::size_t len);
    void handleCreateView   (const uint8_t* buf, std::size_t len);
    // Catalog history Task 9: the catalog DDL ops (74–77) and their shared parts.
    void handleDropTopic       (const uint8_t* buf, std::size_t len);
    void handleAlterTopicConfig(const uint8_t* buf, std::size_t len);
    void handleDropView        (const uint8_t* buf, std::size_t len);
    void handleAlterView       (const uint8_t* buf, std::size_t len);
    template <class Msg, class Submit>
    void handleCatalogDdl_(const uint8_t* buf, std::size_t len, const char* what, Submit&& submit);
    [[nodiscard]] static rune::gateway::DdlMetaView
        ddlMetaOf_(const rune::protocol::client::DdlExtension* ext, int64_t sessionId, const Session& session) noexcept;
    void answerAdminSubmit_(const uint8_t* buf, std::size_t len, rune::gateway::AdminSubmit r,
                            const char* ringFull, const char* tooLarge);
    /// Task 11: the DDL route gate — true = answered here (never submitted):
    /// RouteRemote → Redirect, RouteUnknown → NotLeader, RouteHalted → Internal
    /// (ErrorResponse; a pre-v8 session gets its own response type, F37).
    /// false = RouteLocalLeader, or no header / no session (the handler answers).
    [[nodiscard]] bool ddlRouted_(const uint8_t* buf, std::size_t len);
    // Task 20: GetViewRow (72) — build a GetViewRow WorkerOp routed to the
    // owning worker (same op/return plumbing as PushRead); the worker's
    // ViewRowResult is encoded as a ViewRowResponse (73) in drainWorkerReturns_.
    void handleGetViewRow   (const uint8_t* buf, std::size_t len);
    // GB-3: Push delivery — called from doWork after ingress poll.
    void pushSubscriptions  ();
    void pushSubscriptionsPass_();   ///< N2: the pass body (pushSubscriptions logs the summary after it)
    // GB-5p2: Group-commit flush — called from doWork() once per cycle AFTER all
    // handleProduce() calls have completed.  Flushes each dirty partition Log
    // exactly once (one fsync per touched partition = group-commit under load).
    // Zero per-produce heap; dirtyLogs_ is a pre-allocated inline set.
    // Single-writer: called only on the duty-cycle thread.
    void flushDirtyLogs();

    // GR-5: Drain deferred acks=All pending-ack queue.
    // Called from doWork() AFTER the replica's HW may have advanced for this cycle.
    // For each entry: if replica->highWatermark() >= lastOffset → send ProduceResponse{OK}.
    //                 if enqueuedAtNs + kPendingAckTimeoutNs < nowNs_ → timeout → ProduceResponse{Internal}.
    // Never blocks; never spins.
    void drainPendingAcks();

    // -----------------------------------------------------------------------
    // G0.1: sendReliable_ — THE single reliable-egress primitive.
    //
    // Tries to deliver already-encoded bytes via offerBytes_(session).  On
    // back-pressure (offer <= 0) copies the bytes onto the pendingResponses_
    // ring for retry by drainPendingResponses().  When the ring is full, logs a
    // warning and falls back to a best-effort sendTo (bounded last resort — the
    // only path that can still drop, and it is documented + logged, never silent).
    //
    // EVERY ack / redirect / error response in the produce path routes through
    // this function so the "no silent drop on back-pressure" invariant holds on
    // all paths (inline, worker, deferred acks=All, redirects).
    //
    // No-hang fix round 2 (I1): any size; per-session FIFO (a session with
    // spilled answers spills every new one behind them); spill full → the
    // session is expired explicitly (expireForSpill_), never best-effort.
    //
    // Single-writer: duty-cycle thread only.
    // -----------------------------------------------------------------------
    void sendReliable_(Session& session, const uint8_t* buf, std::size_t len);
    void expireForSpill_(Session& session);
    void serviceSpillExpiring_(int64_t nowNs);
    [[nodiscard]] Session* largestSpiller_();
    // Fix round 4 (N8): the intrusive READY list of Spilled sessions.
    void linkSpillReady_(Session& s) noexcept;
    void unlinkSpillReady_(Session& s) noexcept;
    // Fix round 4 (N8): O(1) "would an offer be accepted now?" — Aeron refuses
    // an offer only at position >= positionLimit (or not connected).
    [[nodiscard]] bool spillWritable_(const Session& s) const noexcept;
    // Fix round 4 (N9): a frame of an Expiring session is dropped (counted) —
    // never executed; true = dropped.
    [[nodiscard]] bool dropExpiringFrame_(const uint8_t* buf, std::size_t len);
    // GB-3 / fix round 4 (N9): a session's subscriptions (and the owning
    // workers' cursors) end — at close, and once its Expiring state is serviced.
    void dropSessionSubscriptions_(int64_t sid);
    // An answer to a client request: reliable (sendReliable_) on a real
    // session; an egress-less injected test session goes through sendTo (its
    // capture hook).
    void sendAnswer_(Session& session, const uint8_t* buf, std::size_t len);
    // No-hang wave (R2): answer the request in buf[0..len) with its OWN
    // response type carrying `code` (RequestRejection.h) on the header's
    // correlationId — a header-decodable request on a known session is never
    // dropped.  Produce goes through sendRespReliable_ (version-gated frame);
    // every other answer through sendAnswer_ (queued under
    // back-pressure).  Returns false — after noteUnanswerable_ — only when the
    // header does not decode, the session is unknown, or the request type has
    // no response (fire-and-forget).  Cold path; zero allocation.
    bool rejectRequest_(const uint8_t* buf, std::size_t len, uint16_t code, const char* why);
    bool answerRejection_(Session& session, const rune::protocol::client::MessageHeader& hdr,
                          uint16_t code, const char* why, const uint8_t* buf, std::size_t len);
    // No-hang fix rounds 4-5 (item 8 / N23): make a pre-v5 client's
    // UnknownMember true — evict the request's own (group, member) (the group
    // for OffsetFetch); session-wide only when the body is undecodable.
    void evictOldClientMember_(Session& session, uint16_t tid, const uint8_t* buf, std::size_t len,
                               int64_t nowNs);
    // The only frames that may go unanswered (undecodable header, unknown
    // session, no response type): counted, warn-logged rate-limited (first,
    // then every 1024th — never a per-frame log on the produce path).
    void noteUnanswerable_(const uint8_t* buf, std::size_t len, const char* why);
    // Task 13 fix round 1: shard-mode session lifecycle helpers.
    /// Worker ack-table slots in use or about to be (I2): accepted sessions +
    /// closes not yet queued on every worker + pending connects whose
    /// announcement has begun.
    [[nodiscard]] std::size_t shardSessionSlotsInUse_() const noexcept;
    /// A dropped pending connect whose announcement began → close everywhere.
    void closeDroppedPendingConnect_(const PendingConnect& pending);
    void failPendingHandshake_(std::deque<PendingConnect>::iterator it, int64_t correlationId);
    bool offerOnPub_(aeron::ExclusivePublication& pub, const uint8_t* buf, std::size_t len);
    bool offerHostResolve_(PendingConnect& pending);
    bool offerHandshakeFailed_(PendingConnect& pending);
    /// Task 19: retry the per-worker crypto-key SessionAnnounce (second
    /// wave) for every session in pendingCryptoAnnounces_ a full worker ring
    /// refused earlier. A session closed/expired in the meantime is dropped
    /// from the list (nothing to announce). Returns work done.
    int retryPendingCryptoAnnounces_();
    /// Close an accepted session (Disconnect / SessionFailed): table, workers,
    /// subscriptions, group-member tracking.  False = unknown session (no-op).
    bool closeSession_(int64_t sid, bool notifyExpired = false);
    /// Fix round 2 (finding 1) + D8c: a Produce frame of a session some worker
    /// holds acks for (Held) or that has no produce credit left — the
    /// shard-mode poll ABORTs it (that client's image only).
    /// Lows batch C (D8c R3): WorkerCap* = the session's head Produce was
    /// deferred by a full worker share and that worker (`*capWorker`) still
    /// refuses it — ABORTed before decrypt / decode (Session::capDeferredOn).
    enum class ProducePause : uint8_t { None, Held, NoCredit, WorkerCapFull, WorkerCapBand };
    [[nodiscard]] ProducePause produceFramePause_(const uint8_t* buf, std::size_t len, int32_t* capWorker = nullptr);
    void clearCapDeferred_(Session& s) noexcept;
    /// Lows A/B/C review L1: the controlled subscription's image count after a
    /// poll.  A drop (a client image lost) clears every cap-deferral memo —
    /// a lost image's deferred frame never comes back to clear its own.  Cold.
    void noteControlledImages_(int images) noexcept;
    int  lastControlledImages_ = 0;
    [[nodiscard]] bool produceFrameHeld_(const uint8_t* buf, std::size_t len) {
        return produceFramePause_(buf, len) != ProducePause::None;
    }
    /// Shard-mode controlled-poll decision for one reassembled client frame:
    /// false = ABORT (counted), true = dispatch it.  One branch when no session
    /// is paused.
    [[nodiscard]] bool admitClientFrame_(const uint8_t* buf, std::size_t len);
    /// D8c: one produce of `s` was forwarded to a worker / `n` of its acks were claimed.
    void takeProduceCredit_(Session& s, int32_t worker) noexcept;
    void returnProduceCredits_(Session& s, int32_t worker, int64_t n) noexcept;
    /// D8c fix round 1: may `s` forward one more produce to `worker` (the
    /// per-worker routed cap + its congested-band fairness rule)?
    enum class WorkerAdmit : uint8_t { Admit, Band, Full };
    [[nodiscard]] WorkerAdmit workerAdmitsProduce_(int32_t worker, const Session& s) const noexcept;
    /// D8c fix round 2 (re-review MEDIUM-R1): the worker's routed share is FULL
    /// and a frame was deferred — advance its cap-full episode; may expire the
    /// held session with the largest share on that worker (see the .cpp).
    void onWorkerCapFull_(int32_t worker);
    /// Leaving a cap-full episode: the worker's routed share dropped below the
    /// congested band (congestion resolved).  Credits inside the band keep the
    /// episode (and the reader marks made in it).
    void noteWorkerShareFreed_(int32_t worker) noexcept {
        if (routedInFlight_[static_cast<std::size_t>(worker)] < workerRoutedCap_ - workerRoutedCap_ / 4)
            capEpisode_[static_cast<std::size_t>(worker)] = CapEpisode{};
    }
    /// Validate + dispatch one reassembled client frame (onFragment's body).
    /// false = the frame was NOT consumed (a produce deferred by the per-worker
    /// cap): the shard-mode controlled poll ABORTs it and redelivers it later.
    bool onClientFrame_(const uint8_t* buf, std::size_t len);
    void onSessionSignal_(const rune::node::WorkerReturn& ret);
    void sendSessionExpired_(aeron::ExclusivePublication* pub, Session* session, int64_t sid);
    /// Evict the oldest pending connect (the kMaxPendingConnects cap).
    void evictOldestPendingConnect_();
    /// Encode an ErrorResponse{code} into scratch_ and send it reliably.
    void sendErrorResponse_(Session& session, int64_t sessionId, int64_t correlationId,
                            rune::protocol::client::ErrorCode code);

    // F19: fail push subscription `sub` once — its next batch (at
    // sub.nextOffset, Records frame `frameLen` bytes) exceeds the session
    // egress maxMessageLength.  Sets sub.failed + errorOwed, logs, and tries the
    // error (trySendOwedPushError_).  The cursor is NOT advanced.  Cold path.
    // Single-writer: duty-cycle thread only.
    void failPushSubscriptionTooLarge_(Subscription& sub, Session& session, std::size_t frameLen);

    // F19: offer the owed ErrorResponse{MessageTooLarge, correlationId 0,
    // message "subscription S topic T partition P offset O: ..."} directly on
    // the session egress; clears errorOwed only once sent (retry-until-sent,
    // like the worker's ErrorOwed — never best-effort).  Cold path.
    void trySendOwedPushError_(Subscription& sub, Session& session);

    // F19: test-only capture of a Records frame for a null-egress session
    // (injectTestSession) via onEgressFrame_.  Kept out of pushRecords_ so the
    // production send helper never allocates.
    [[gnu::cold]] void captureTestRecordsFrame_(Session& session,
                                                const rune::protocol::client::Records& rec);

    // F19: send one Records frame for `rec` on `session` (tryClaim fast path /
    // vectored offer / null-egress test hook).  Returns the RecordsSendResult.
    rune::node::RecordsSendResult pushRecords_(Session& session,
                                               const rune::protocol::client::Records& rec);

    // -----------------------------------------------------------------------
    // G0.1: sendRespReliable_ — private production implementation.
    // Encodes a ProduceResponse into scratch_ and routes it through
    // sendReliable_.  Always returns true (never silently drops).
    // Returns false only on encode error (shouldn't happen given static_assert).
    //
    // NOTE: The public 4-param sendRespReliable() delegates here using
    // currentProduceSessionId_.  All internal callers (handleProduce) should
    // call sendRespReliable_() directly for clarity.
    // -----------------------------------------------------------------------
    // D6: `ec` is mapped for the session's client version
    // (produceErrorForClient); logStartOffset is carried on the wire (-1 =
    // not carried — set only with UnknownProducerId).
    bool sendRespReliable_(Session& session,
                           rune::protocol::client::ErrorCode ec,
                           int32_t                           partition,
                           int64_t                           baseOffset,
                           int64_t                           correlationId,
                           int64_t                           logStartOffset = -1);

    // G0.1: offerBytes_ — try to offer encoded bytes on the egress.
    // Returns > 0 on success, <= 0 on back-pressure.
    // Checks testEgressFn_ first (non-null only in tests), then real sendTo.
    int64_t offerBytes_(Session* session, const uint8_t* buf, std::size_t len);

    // -----------------------------------------------------------------------
    // Egress send helper — tryClaim first, fallback to offer.
    // Returns true if the frame was sent (or offered).
    // -----------------------------------------------------------------------
    bool sendTo(Session& session, const uint8_t* buf, std::size_t len);

    // -----------------------------------------------------------------------
    // sendOnPub — send a frame on a raw ExclusivePublication (for redirect path
    // where no Session is yet created).
    // -----------------------------------------------------------------------
    bool sendOnPub(aeron::ExclusivePublication& pub, const uint8_t* buf, std::size_t len);

    // -----------------------------------------------------------------------
    // pollPendingConnects — called in doWork; for each pending connect, try
    // findExclusivePublication. On resolve → promote to session + send
    // ConnectResponse. On timeout → drop + log. Never blocks.
    // -----------------------------------------------------------------------
    void pollPendingConnects(int64_t nowNs);

    // -----------------------------------------------------------------------
    // drainPendingReleases — called in pollPendingConnects; for each
    // registration in drainingRegistrations_, attempt findExclusivePublication.
    // On resolve (shared_ptr non-null): let it drop immediately — destructor
    // calls ClientConductor::releaseExclusivePublication(registrationId)
    // (Aeron C++ 1.41.4: ExclusivePublication::~ExclusivePublication() calls
    // m_conductor.releaseExclusivePublication(m_registrationId)).
    // On error (throws): conductor already removed the entry — no leak.
    // On still-AWAITING (nullptr): keep polling until it resolves.
    // Never blocks.
    // -----------------------------------------------------------------------
    void drainPendingReleases();

    // -----------------------------------------------------------------------
    // Members
    // -----------------------------------------------------------------------
    std::shared_ptr<aeron::Aeron>        aeron_;
    std::string                          clientChannel_;
    int32_t                              clientStreamId_;
    int32_t                              thisNodeId_;
    int32_t                              workerIngressBaseStreamId_ = 0;  // E5.1
    int32_t                              workerEgressControlBasePort_ = 0; // read-rework I4
    int32_t                              workerEgressBaseStreamId_    = 0; // read-rework I4
    int64_t                              sessionTimeoutNs_;
    int64_t                              maxHeldIdleNs_ = kDefaultMaxHeldIdleNs;
    int64_t                              connectResolveTimeoutNs_;
    std::function<LeaderView()>          leaderView_;
    // client<->broker TLS/AEAD (Task 7): this broker's TLS server cert/key,
    // as PEM text, loaded once at construction.  Both empty = crypto disabled
    // (see the ctor's tlsCertPem/tlsKeyPem doc comment).  Task 9: ALSO swapped
    // by reloadTlsCert() on SIGHUP renewal — single-writer, duty-cycle thread
    // only (see reloadTlsCert()'s precondition).
    std::string                          tlsCertPem_;
    std::string                          tlsKeyPem_;
    // Task 9 (require_tls, spec §11 downgrade safety): see the ctor doc comment.
    bool                                  requireTls_ = false;

    std::shared_ptr<aeron::Subscription> sub_;
    rune::aeronutil::FragmentAssembler   assembler_;
    rune::aeronutil::ImageLossWatch      imageWatch_;   // review m-c
    // Fix round 2 (finding 1): shard mode polls the client subscription with
    // controlledPoll (per-session produce pause); separate reassembly state.
    rune::aeronutil::ControlledFragmentAssembler controlledAssembler_;
    rune::aeronutil::ImageLossWatch              controlledImageWatch_;
    std::size_t                                  heldSessions_ = 0;   // sessions with heldByWorkers != 0
    uint64_t                                     heldProduceAborts_ = 0;   // every paused-produce ABORT (Held or no credit)
    // D8c: per-session produce credits (shard mode).
    uint32_t                                     sessionProduceCredits_ = kDefaultSessionProduceCredits;
    std::size_t                                  creditBlockedSessions_ = 0;   // sessions with produceInFlight >= budget
    std::size_t                                  capDeferredSessions_ = 0;     // sessions with capDeferredOn >= 0 (R3)
    std::size_t                                  expiringSessions_ = 0;        // fix round 4/5 (N9, N20): sessions Expiring (exact)
    uint64_t                                     creditProduceAborts_   = 0;   // ABORTs for want of credit
    uint64_t                                     produceCreditUnderflows_ = 0; // Credit beyond in-flight (invariant breach; clamped)
    // D8c fix round 1 (MAJOR-1): forwarded, unclaimed produces per worker (all sessions).
    std::array<uint32_t, kMaxHeldMaskWorkers>    routedInFlight_{};
    uint32_t                                     workerRoutedCap_ = kWorkerRoutedCap;
    uint64_t                                     workerCapProduceAborts_ = 0;  // produces deferred by the per-worker cap
    uint64_t                                     capDeferMemoAborts_ = 0;      // ... of them ABORTed by the memo (R3)
    // D8c fix round 2: per-worker cap-full episode (rich state: Idle = tag 0;
    // Congested{tag, deferredPolls}).  An episode ends below the congested band
    // or with its (single) expiry.
    struct CapEpisode {
        uint16_t tag = 0;            ///< 0 = Idle
        uint32_t deferredPolls = 0;  ///< frames deferred by the FULL share in this episode
    };
    std::array<CapEpisode, kMaxHeldMaskWorkers>  capEpisode_{};
    uint16_t                                     capEpisodeTags_ = 0;          // global, never 0 when issued; one per episode / scan window (wraps per 65535 windows)
    uint64_t                                     workerCapEvictions_ = 0;
    uint64_t                                     directShareExpiries_ = 0;   // D8d: worker Expire signals acted on
    int64_t                                      lastCapEvictedSid_ = 0;
    /// Deferred polls of one cap-full episode before an expiry is considered:
    /// a READING session's claims return a Credit within a worker pass + one
    /// ret_ hop, i.e. many times over in 64 gateway duty cycles; a reader that
    /// credits in the episode is never expired.
    static constexpr uint32_t kCapEvictAfterDeferredPolls = 64;

    SessionTable                         sessions_;
    // Retire-on-ack record-egress pubs (see RetiringEgress).  Declared after
    // sessions_ / aeron_ so it is destroyed before them (pubs before client).
    std::vector<RetiringEgress>          retiringEgress_;
    uint64_t                             nextRetireToken_  = 0;
    std::size_t                          retiringUnsent_   = 0;

    // Async pending-connect table (BR-7a).
    // Bounded to kMaxPendingConnects; all touched only in doWork (single-writer).
    // std::deque gives O(1) pop_front (evict-oldest) while still supporting
    // erase(it) for mid-sequence resolve/timeout removals.
    std::deque<PendingConnect>           pendingConnects_;
    rune::consensus::HostResolvePort     hostResolver_{};   // fix round 2 (LOW-4)
    /// Apply the hub's answers to pending connects awaiting their reply host.
    int pollHostResolutions_();
    /// Register the egress pub of `pending` (its channel final); false = dropped.
    bool registerConnectEgress_(PendingConnect& pending);

    // Draining-release table (BR-7a fix): registrationIds that were logically
    // dropped (timeout / eviction / error) before their ExclusivePublication
    // resolved.  We must keep polling findExclusivePublication until the
    // conductor transitions from AWAITING_MEDIA_DRIVER to either
    // REGISTERED_MEDIA_DRIVER (shared_ptr returned → drop it, destructor
    // releases) or ERRORED_MEDIA_DRIVER (throws → conductor already cleaned up).
    // Not structurally capped: a hard cap could evict an entry before it
    // releases and re-introduce the leak this table exists to prevent. It is
    // instead expected-bounded — an Aeron UDP/IPC ExclusivePublication always
    // resolves within a few driver cycles (publication creation needs no
    // subscriber), and drainPendingReleases() clears each entry the cycle it
    // resolves, so the table stays small (≈ pending drop rate × resolve latency).
    std::vector<int64_t>                 drainingRegistrations_;

    // Scratch buffer for reap ID collection (avoids heap allocation per frame).
    // Populated and drained entirely within reapExpired() — single-writer, safe.
    std::vector<int64_t>                 reapScratch_;

    // Reusable encode scratch — 68 KiB covers any Milestone-A/B frame.
    // (ConnectResponse=38 B, Pong=26+echo, max echo=65535+26 = ~65 KiB;
    //  ProduceResponse=46 B; restamp scratch = max RecordBatch = 65535 B)
    static constexpr std::size_t kScratchSize = 68 * 1024;
    std::array<uint8_t, kScratchSize>    scratch_{};

    // client<->broker TLS/AEAD (Task 8): reusable, grow-once scratch buffers
    // for the data-plane AEAD codec (rune/gateway/CryptoFrame.h) — single-
    // writer (duty-cycle thread only), never shrink, resize() is contained in
    // a try/catch at the call site so a bad_alloc drops the frame instead of
    // throwing out of a noexcept-adjacent hot path (mirrors the client's
    // egressDecryptScratch_ / AeronTransport::sealScratch).
    //   decryptScratch_ — onClientFrame_'s openIngressFrame() destination.
    //     Always sized to the wire frame's length: the reconstructed
    //     plaintext frame is kCryptoOverhead (28) bytes SHORTER than the
    //     wire frame it came from, so `len` bytes of capacity is always enough.
    //   sealScratch_    — sendTo()'s sealResponseFrame() destination. Sized
    //     to plainLen + kCryptoOverhead on demand.
    std::vector<uint8_t>                 decryptScratch_;
    std::vector<uint8_t>                 sealScratch_;

    // client<->broker TLS/AEAD (Task 8b): pushRecords_'s own grow-once seal
    // scratch for an ENCRYPTED session's Records push (rune::node::
    // RecordsSealScratch — see RecordsPush.h's file header comment for the
    // encrypted path). Separate from sealScratch_ above (that one is
    // sendTo()'s, sized plainLen + kCryptoOverhead for ordinary responses;
    // Records frames can be far larger — up to maxMessageLength — and need
    // their own contiguous plaintext-assembly buffer too, which ordinary
    // responses never do). Single-writer (duty-cycle thread only), never
    // shared across threads (shared-nothing).
    rune::node::RecordsSealScratch       recordsSealScratch_;

    // Max accepted produce-batch WIRE size.  A batch above this is rejected LOUDLY
    // with MessageTooLarge (never silently dropped).  The zero-copy append path no
    // longer needs a fixed restamp scratch — the base offset is patched into the
    // page cache / ring slot via pwritev — so this bound only guards against an
    // absurd/malformed length overrunning the worker ingress ring or a segment.
    // Default 8 MiB (>> typical batches, << the 32 MiB default worker ingress ring).
    // kMaxProduceBatchBytes (public, below the accessors) == rune::node::Worker::
    // kMaxProduceBatchBytes (F15 S3 static_assert in ClientGateway.cpp).
    std::size_t maxProduceBatchBytes_ = kMaxProduceBatchBytes;
    // F3 round 6, item 2: oversize-reject log rate limiting (control thread only).
    uint64_t oversizeProduceRejects_    = 0;
    uint64_t identityMismatchRejects_   = 0;   // inline path: frame identity != batch header
    uint64_t oversizeProduceRejectLogs_ = 0;

    // Topic registry (GB-1) — non-owning; set via setTopicRegistry().
    // Null until wired in; GB-2/GB-3 will use it for produce/subscribe dispatch.
    // Single-writer: only accessed on the duty-cycle thread.
    TopicRegistry*                       topicRegistry_ = nullptr;

    // Replica registry (GR-4) — non-owning; set via setReplicaRegistry().
    // Null until wired in; handleProduce() uses it to call onLeaderLogAppended()
    // after each successful append so the leader PartitionReplica's tracked LEO/HW
    // covers the new bytes (Mode A seam — no double-append).
    // Single-writer: only accessed on the duty-cycle thread.
    rune::node::ReplicaRegistry*         replicaRegistry_ = nullptr;

    // E2 inc2a: WorkerPool for per-partition SPSC routing (non-owning).
    // Null until wired via setWorkerPool(). When set, handleProduce() and
    // pushSubscriptions() route log work through the owning worker's ring;
    // drainWorkerReturns_() sends deferred responses at the top of doWork().
    rune::node::WorkerPool* workerPool_ = nullptr;

    // E2 residual round 6 (F1): see setTransientLineage().  Cold state (bind /
    // resume rate); pendingLineageNotes_ is non-empty only while a bound
    // transient subscription's lineage is still unknown.
    static constexpr uint64_t kLineageUnknown = UINT64_MAX;
    TransientLineageFn lineageFn_  = nullptr;
    void*              lineageCtx_ = nullptr;
    uint64_t           partitionDataResets_ = 0;
    uint64_t           lineageNotesSent_    = 0;
    uint64_t           subscribesAwaitingLineage_ = 0;   ///< FU1: answered NotLeader (lineage unknown)
    std::vector<int64_t> pendingLineageNotes_;   ///< subscriptionIds awaiting their lineage
    /// Send (or queue until known) subscription `subId`'s lineage note.
    void noteLineage_(int64_t subId);
    void sendLineageNote_(Session& session, int64_t subId, const Subscription& sub, uint64_t lineage);
    int  flushPendingLineageNotes_();

    // Catalog history Task 12: catalog Unsubscribes / session closes the admin
    // request ring could not take (full) — retried every duty cycle, in order,
    // never dropped (a lost close would keep the admin pushing to a dead session
    // and hold one of its bounded subscription slots).  Reserved; grows only on a
    // pathological backlog.
    struct PendingCatalogUnsubscribe { int64_t sessionId; int64_t subscriptionId; };
    // Fix round 1 (M3): fixed capacity; a session close supersedes that
    // session's pending single unsubscribes (one entry per closed session).
    static constexpr std::size_t kPendingCatalogUnsubsCap = 1024;
    std::vector<PendingCatalogUnsubscribe> pendingCatalogUnsubs_ = [] {
        std::vector<PendingCatalogUnsubscribe> v; v.reserve(kPendingCatalogUnsubsCap); return v;
    }();
    uint64_t catalogUnsubsDropped_ = 0;
    uint64_t catalogClosesToSweep_ = 0;   // fix round 3: closes left to the admin's liveness sweep
    uint64_t catalogSubscribesForwarded_ = 0;
    void forwardCatalogUnsubscribe_(int64_t sid, int64_t subscriptionId);
    int  flushPendingCatalogUnsubs_();
    /// handleSubscribe for topicId == kCatalogTopicId: answered here or forwarded.
    void handleCatalogSubscribe_(Session& session, const rune::protocol::client::Subscribe& msg);

    // Catalog history Task 12 (Task 8 review N5): view sinks APPLIED here whose
    // compiled plan has not reached every worker yet — a GetViewRow of one is
    // the retriable NotReady, never the worker's permanent UnknownView.  Cold
    // (one entry per view in creation); usually empty — one size check per read.
    std::vector<int32_t> viewSinksAwaitingPlan_;
    int64_t catalogBackPressureForTest_ = 0;   // setCatalogBackPressureForTest (0 = none)

    // E5.1.3 (e): per-worker ingress active flag.
    //
    // When true, the gateway produce fanout (tryEnqueueOp for Produce) is SKIPPED.
    // Workers poll their own Aeron subscriptions for produce frames directly.
    // This prevents double-processing when per-worker ingress is active.
    //
    // Default false — the gateway fanout is the live production path.
    // Set via setPerWorkerIngressActive() from BrokerNode::buildWorkerPool()
    // when RUNE_PER_WORKER_INGRESS=true.
    bool perWorkerIngressActive_ = false;
    bool shardMode_ = false;   // Task 13 (spec §3.4); set before the runtime runs
    rune::node::SessionAnnouncer sessionAnnouncer_;   // Task 13: runtime thread only
    // Task 19: sessions whose per-worker crypto-key SessionAnnounce (second
    // wave) didn't fully land on every worker on the first attempt (a ring
    // was momentarily full) — retried each doWork() cycle
    // (retryPendingCryptoAnnounces_) until every worker has the real key.
    // Bounded by concurrently-connecting encrypted shard sessions (cold,
    // connect-time only); empty in legacy mode and for every plaintext
    // session.
    std::vector<int64_t> pendingCryptoAnnounces_;
    std::vector<ShardIngressEndpoint> shardIngressEndpoints_;  // spec §3.3; empty = legacy

    // read-rework T3/T4 (I3b): when true, Subscribe/Unsubscribe for user partitions
    // are routed to the owning worker (which pushes on its own egress) and the
    // gateway's pushSubscriptions skips user partitions.  Set via
    // setPerWorkerEgressActive() from BrokerNode when RUNE_PER_WORKER_EGRESS=true.
    bool perWorkerEgressActive_ = false;

    // E5.1.3 (d) R7 fix: set of Log* pointers owned by workers.
    //
    // Populated by addWorkerOwnedLog() called from BrokerNode::buildWorkerPool()
    // when per-worker ingress is active.  flushDirtyLogs() skips any log in this
    // set to prevent the control-thread gateway from calling Log::sync on a log
    // that the owning worker thread is appending to (TSan race R7).
    //
    // Internal partitions (topicId < 0) are never added here; they remain on
    // the gateway sync path.
    //
    // Shared-nothing ([[rune-shared-nothing-law]]): this set is built once at
    // startup (before runners start, single-threaded) and is read-only at runtime.
    std::unordered_set<rune::storage::Log*> workerOwnedLogs_;

    // E2 inc2a: Set of subscriptionIds that have an outstanding PushRead op in
    // the worker's ingress ring.  A subscription must not get a second PushRead
    // until the first one's return has been processed (exactly-once ordering).
    // Cleared in handlePushReadReturn_ after the send completes (or fails with
    // back-pressure — in the back-pressure case we clear it so the next cycle
    // re-enqueues from the same nextOffset).
    std::unordered_set<int64_t> pendingWorkerReads_;

    // Group request handler (CG-3/CG-4 fix) — non-owning; set via setGroupService().
    // Null until wired in; group messages (templateIds 32–43) are dispatched here.
    // IGroupRequestHandler is defined in this lib (gateway); the concrete impl
    // (GroupCoordinationService) lives in rune_node — no circular dependency.
    // Single-writer: only accessed on the duty-cycle thread.
    rune::gateway::IGroupRequestHandler* groupService_ = nullptr;

    // Transaction request handler (TX-3) — non-owning; set via setTxnService().
    // Null until wired in; txn messages (templateIds 45,47,49,51,53) dispatched here.
    // ITxnRequestHandler is defined in this lib (gateway); the concrete impl
    // (TransactionCoordinationService) lives in rune_node — no circular dependency.
    // Single-writer: only accessed on the duty-cycle thread.
    rune::gateway::ITxnRequestHandler* txnService_ = nullptr;

    // Task 8a: schema DDL request sink (RULING-2 F) — non-owning; set via
    // setAdminSink().  Null → DefineTopic/GetTopicSchema are dropped.
    rune::gateway::IAdminRequestSink* adminSink_ = nullptr;
    // Catalog history Task 11: the DDL route (setCatalogRoute) + its counters.
    rune::gateway::CatalogRoute catalogRoute_{rune::gateway::RouteLocalLeader{}};
    uint64_t                    ddlRoutedAway_   = 0;
    uint64_t                    preV8DdlRefused_ = 0;

    // MD1 Task 9: Per-partition RoutingImage (capture-once per duty cycle).
    //
    // routingImage_: the current routing image snapshot, captured once at the
    //   start of doWork() (or set directly by tests via setRoutingImageSnapshot).
    //   All per-produce/subscribe lookup() calls use this raw pointer (no
    //   atomic_load per produce — the single capture-once discipline ensures
    //   zero per-produce lock/alloc on Apple libc++ and Linux alike).
    //   Null until the first MetadataService emit (or test injection).
    //
    // committedMetadataReady_: set to true when committedMap() is first non-empty.
    //   Used as the gate for GR-4 (per-partition NotLeader defence) and GR-5
    //   (ISR min-size gate / deferred-ack) instead of routingImage_ (see Fix 5 /
    //   MD1-13 comment on setCommittedMetadataReady()).
    //
    // nodeEndpointMap_: maps nodeId → client endpoint string for REDIRECT.
    //   Built from NodeConfig::peers in BrokerNode::buildGateway().
    //   Immutable after construction; single-writer (duty-cycle thread).
    //   Keyed by int32_t nodeId; value is the "aeron:udp?endpoint=…" string.
    //   Empty until wired in (no routing image = no redirects).
    std::shared_ptr<const rune::metadata::RoutingImage> routingImage_;
    bool                                                 committedMetadataReady_ = false;
    int8_t                                               localLeader_ = -1;   // see setLocalLeadership()
    std::unordered_map<int32_t, std::string>             nodeEndpointMap_;
    SessionAdmission                                     sessionAdmission_ = SessionAdmission::ClusterLeaderOnly;

    // Cached nowNs for the current doWork() invocation (passed into handlers).
    int64_t                              nowNs_ = 0;

    // GB-3: Subscription registry — keyed by subscriptionId.
    // Iterated each doWork() to push new Records to each subscriber's egress.
    // Subscriptions are reaped when the owning session is removed/reaped.
    // Single-writer: only accessed on the duty-cycle thread.
    std::unordered_map<int64_t, Subscription> subscriptions_;

    // GB-3: Monotonic subscription ID generator (starts at 1).
    int64_t nextSubscriptionId_ = 0;
    int64_t idPrefix_           = 0;  // cluster-unique id prefix (setClusterIdNode)
    [[nodiscard]] int64_t allocSubscriptionId_() noexcept {
        // LOW-2: the counter must never carry into the node prefix (another
        // node's ids) — 2^40 subscriptions per process lifetime.
        if (++nextSubscriptionId_ >= (int64_t{1} << kClusterIdShift)) [[unlikely]] {
            std::fprintf(stderr, "ClientGateway: subscription id space exhausted (2^40) — aborting\n");
            std::abort();
        }
        return idPrefix_ | nextSubscriptionId_;
    }


    // CG-4: Group-member push subscription tracking.
    // Maps memberSessionId → set of subscriptionIds that were created via
    // addGroupSubscription() for that member.  Enables O(n_subs) eager revoke
    // that removes ONLY group subs, leaving normal client subs untouched.
    // Single-writer (duty-cycle thread only).
    std::unordered_map<int64_t, std::vector<int64_t>> groupMemberSubs_;

    // CG-4: Optional control-frame capture callback (for tests without live Aeron egress).
    // Invoked by sendControlFrame BEFORE the actual egress send.  Null in production.
    ControlFrameCallback onControlFrame_;

    // CG-4: Optional egress-frame capture callback (for tests with null-egress sessions).
    // Invoked by sendTo() when session.egress is null; fires instead of the
    // null-egress early-return.  Returns true so callers (pushSubscriptions) advance
    // nextOffset.  Null in production.
    EgressFrameCallback onEgressFrame_;

    // GB-5p2: Dirty-log set + group-commit flush cadence.
    //
    // handleProduce() uses appendNoSync() (page-cache write, no fsync) and
    // records the Log* in dirtyLogs_.  flushDirtyLogs() is called every
    // kGroupCommitIntervalNs nanoseconds (time-based cadence) from doWork():
    // it fsyncs each dirty Log once and clears the set.
    //
    // Cadence rationale:
    //   - Per-cycle flush (every doWork()): one fsync per produce under light
    //     load — latency still dominated by fsync (~400µs).
    //   - Time-based flush (every kGroupCommitIntervalNs): produces between
    //     flushes land in page cache at ~1-10µs; fsync is amortised over all
    //     records accumulated since the last flush.  Under load many records
    //     share one fsync.  Under idle the flush fires on a timer even with no
    //     produces (keeps dirty data from sitting un-fsynced indefinitely).
    //
    // kGroupCommitIntervalNs = 10 ms: matches Kafka's default log.flush.interval.ms.
    // Durability trade-off: data written since the last flush may be lost on a
    // leader crash — acks=Leader (acks=1) semantics.
    //
    // Pointer identity: Log objects are owned by TopicRegistry (lifetime >
    // gateway); raw pointer equality is the correct identity key.
    static constexpr int64_t kGroupCommitIntervalNs = 10LL * 1'000'000LL; // 10 ms
    std::unordered_set<rune::storage::Log*> dirtyLogs_;
    StorageFailureHandler storageFailureHandler_;
    InvariantViolationHandler invariantViolationHandler_;
    bool                  storageFailureRaised_ = false;
    void raiseStorageFailure_(const std::string& logDir, int err);
    void handleWorkerReturn_(rune::node::WorkerReturn ret);   // by value: WorkerReturn complete in the .cpp
    void onControlRecordRefused_(const rune::node::WorkerReturn& ret);
    void onControlRecordAppended_(const rune::node::WorkerReturn& ret);
    uint64_t controlRecordsRefused_ = 0;
    uint64_t produceBackPressured_  = 0;   // M5: produces answered NotLeader on back-pressure

    // -----------------------------------------------------------------------
    // Task 10 round 4 (P1): asynchronous control markers.  A marker handed to
    // a worker gets a token (the op's correlationId); its offset arrives in
    // the worker's ControlRecordAppended reply and only THEN feeds the LSO /
    // aborted-txn bookkeeping.  A marker the worker ring could not take waits
    // in markerSendQueue_ (FIFO, retried every doWork — never dropped); while
    // one waits for a partition, produces to that partition answer the
    // retriable NotLeader so no later batch can overtake the marker.  A
    // refused marker for a partition still on this node is re-routed (safe:
    // no offset was pre-computed) — a bounded number of times, then it is an
    // invariant violation (halt).  Cold path (allocation is fine).
    // -----------------------------------------------------------------------
    struct MarkerInFlight {
        int32_t              topicId    = 0;
        int32_t              partition  = 0;
        int64_t              producerId = -1;
        bool                 commit     = false;
        uint32_t             reroutes   = 0;
        int32_t              coordinatorEpoch = 0;   ///< F15 m3
        // D9 coordinator side: forwarded to this leader node (-1 = local marker)
        // under this routing-image leader epoch; sent = handed to the plane
        // (false = waiting in markerSendQueue_).
        int32_t              remoteNode        = -1;
        int32_t              remoteLeaderEpoch = -1;
        bool                 sent              = false;
        // D9 leader side: appended on behalf of this coordinator node (-1 = own).
        int32_t              originNode  = -1;
        int64_t              originToken = 0;
        /// re-review MEDIUM-3: leader side, the target leader epoch the origin
        /// routed it under (-1 = not checked).
        int32_t              targetLeaderEpoch = -1;
        std::vector<uint8_t> bytes;
    };
    static constexpr uint32_t kMaxMarkerReroutes = 4;
    std::unordered_map<int64_t, MarkerInFlight> markersInFlight_;
    std::deque<int64_t>                         markerSendQueue_;
    /// Review MEDIUM-1: per-boot incarnation in the high bits (bits 32..62) so
    /// a pre-restart answer never matches a post-restart token.
    int64_t                                     nextMarkerToken_ = initialMarkerToken_();
    static int64_t initialMarkerToken_();
    bool trySendMarker_(int64_t token);
    /// F15 K3: THE routing predicate for worker-owned appends — used by the
    /// produce path AND marker routing (appendControlRecord), in every mode,
    /// so a marker is queued on the same op ring as the produces it must
    /// follow (L4) and the control thread never appends to a worker-owned log.
    [[nodiscard]] bool routesToWorker_(int32_t topicId) const noexcept {
        return workerPool_ != nullptr && (!perWorkerIngressActive_ || topicId >= 0);
    }

    // Phase 2b E2: what this gateway can serve for (topic, partition) — ONE
    // explicit kind each (CLAUDE.md rule 3), C1 first:
    //   NotPlaced      — a catalog-applied topic whose placement is not committed
    //                    yet: retriable NotLeader, hidden from metadata.
    //   Disk           — its servable disk Log (`log`).
    //   Transient      — committed, persistence=transient, in range, replica
    //                    registered: NO disk Log; its InMemoryLog is owned by the
    //                    replica's worker and is NEVER touched here (its
    //                    published HW is the subscribe clamp).
    //   Unknown        — nothing servable here.
    struct ServedPartition {
        // Catalog history Task 8 (C1 generalized, spec §5.4); Task 9 removed the
        // old runtime path's AwaitingCommit (an entry exists only once applied).
        enum class Kind : uint8_t { Unknown, NotPlaced, Disk, Transient };
        Kind                kind = Kind::Unknown;
        rune::storage::Log* log  = nullptr;   ///< Kind::Disk only
    };
    using RoutedEntry = rune::metadata::RoutingImage::Entry;
    /// `routed`: the routing-image entry the caller already looked up for this
    /// (topic, partition) in the CURRENT image (step 3b), or nullptr.
    [[nodiscard]] ServedPartition servedPartition_(int32_t topicId, int32_t partition,
                                                   const RoutedEntry* routed = nullptr) const noexcept;
    /// Catalog history Task 8 (C1 generalized): the MSM holds a COMMITTED
    /// placement for (topicId, partition) — the current routing image was built
    /// from the committed assignment map (RoutingImage::committed(); the
    /// static-balancer image never is) and names a leader for it.
    [[nodiscard]] bool placementCommitted_(int32_t topicId, int32_t partition,
                                           const RoutedEntry* routed = nullptr) const noexcept;
    /// C1 generalized: the code for a topic id this broker has not applied —
    /// NotLeader (retriable) when its placement is already committed (a producer
    /// learned it from another broker, this one applies it next), UnknownTopic
    /// otherwise and always for a Dropped id.
    /// Fix round 1 (I5): the C1 gate on a view read — a ViewRowResponse status
    /// to answer, or kViewRowNoGate (forward to the worker).
    static constexpr uint16_t kViewRowNoGate = 0xFFFF;
    [[nodiscard]] uint16_t viewRowGate_(int32_t viewTopicId, int32_t partition) const noexcept;
    [[nodiscard]] rune::protocol::client::ErrorCode unappliedTopicCode_(int32_t topicId, int32_t partition) const noexcept;
    /// E2 Task 11 ruling: may a read_committed subscription to (topicId,
    /// partition) be accepted HERE?  read_committed is served from a LEADER's RC
    /// state only (RcState I2): a follower answered OK and then delivered nothing
    /// (a silent stall).  False only for a registered replica that is not a
    /// published leader (GatewayReplicaView::publishedLeader); no replica
    /// (unreplicated / internal) → true, unchanged.  Cold (subscribe time, and
    /// the demotion sweep on a routed role change — retireReadCommittedMovedAway_).
    [[nodiscard]] bool readCommittedServableHere_(int32_t topicId, int32_t partition) const noexcept;
    /// Task 11 carry (E2 Task 13): a plain read_committed subscription accepted
    /// while this node led the partition, which it no longer leads (demoted:
    /// controlled shutdown, failover, a higher epoch) — no RC state serves it
    /// here any more, so it would push nothing, forever (a silent stall, disk
    /// and transient, both worker modes).  Answer its Subscribe correlation id
    /// with the bare ErrorResponse{NotLeader} (the retriable re-route every
    /// unservable-here read_committed subscribe gets; the client re-subscribes
    /// from its position and follows the leader) and drop it here and on the
    /// owning worker.  True = retired (the caller erases the record).
    bool retireReadCommittedMovedAway_(int64_t subId, Subscription& sub);
    uint64_t readCommittedMovedAway_ = 0;   ///< cold counter: subscriptions retired by retireReadCommittedMovedAway_
    /// The registry's roleChanges() the last moved-away sweep saw.  The sweep
    /// runs on every routed role change / replica removal (ReplicationMetadata
    /// Bridge::recordIntendedRole_, ReplicaRegistry::remove) and, as the
    /// fallback for a demotion no control-plane route reports (a leader fenced
    /// by a higher epoch in onFetchRequest), a bounded slice every
    /// kMovedAwayScanEveryCycles push passes (scanMovedAwaySlice_) — never a
    /// per-pass lookup per subscription, never an O(subs) spike.
    uint64_t seenRcSweepRoleChanges_ = 0;
    /// M4: a routed change is waiting for its (coalesced) sweep.
    bool     routedSweepPending_     = false;
    int64_t  nextRoutedSweepCycle_   = 0;     ///< M4: the earliest pass the next routed sweep may run
    uint64_t routedMovedAwaySweeps_  = 0;     ///< M4: routed sweeps run (tests)
    uint64_t transientClassifications_ = 0;   ///< M6: see Subscription::transientReplica (tests)
    static constexpr int64_t kRoutedSweepMinGapCycles = kRoutedSweepMinGapCyclesForTest;
    static constexpr int64_t     kMovedAwayScanEveryCycles   = kMovedAwayScanEveryCyclesForTest;   ///< power of two
    static constexpr int         kMovedAwayLookupsPerSlice   = kMovedAwayLookupsPerSliceForTest;
    static constexpr std::size_t kMovedAwayBucketsPerSlice   = 256;   ///< empty buckets cost no lookup, but bounded
    /// N2: the moved-away summary's rate limit (wall time, the duty clock nowNs_).
    static constexpr int64_t     kMovedAwaySummaryEveryNs    = kMovedAwaySummaryEveryNsForTest;
    std::size_t movedAwayBucket_           = 0;   ///< fallback scan cursor (bucket index)
    std::size_t movedAwayBucketPos_        = 0;   ///< N1: survivors of that bucket already visited (mid-bucket resume)
    int         movedAwayMaxProbesPerSlice_ = 0;  ///< N1: the most movedAway_ probes one slice made (tests)
    uint64_t    movedAwaySinceSummary_     = 0;
    int64_t     nextMovedAwaySummaryNs_    = 0;
    uint64_t    movedAwaySummaries_        = 0;   ///< N2: summaries logged (tests)
    uint64_t    lastMovedAwaySummaryCount_ = 0;   ///< N2: the count the last summary reported (tests)
    /// The fallback slice: at most kMovedAwayLookupsPerSlice movedAway_ probes,
    /// resuming at (movedAwayBucket_, movedAwayBucketPos_) — mid-bucket (N1).
    void scanMovedAwaySlice_();
    void logMovedAwaySummary_();
    [[nodiscard]] static bool movedAwayEligible_(const Subscription& sub) noexcept {
        return !sub.isGroup && sub.topicId >= 0 && !sub.failed;
    }
    /// Moved-away verdict for one plain subscription during a sweep: true when
    /// this node can no longer serve it — its replica was removed (any
    /// isolation) or, read_committed, it is no longer a published leader.
    /// M5: also records hadReplica the first time it sees a registered replica.
    [[nodiscard]] bool movedAway_(Subscription& sub) const noexcept;
    int  pumpMarkerQueue_();
    // D9 (see TxnMarkerChannel).
    TxnMarkerChannel txnMarkerChannel_{};
    std::deque<std::pair<int32_t, rune::replication::WriteTxnMarkerResponseMsg>> pendingMarkerResponses_;
    uint64_t remoteMarkersLanded_ = 0;           ///< D9: cold counter (tests / logs)
    // Re-review NEW MAJOR-1: leader side, the last remote marker taken per
    // (topic, partition, producer) and its outcome (cold; see appendRemoteControlRecord).
    struct RemoteMarkerKey {
        int32_t topicId = 0; int32_t partition = 0; int64_t producerId = -1;
        bool operator==(const RemoteMarkerKey& o) const noexcept {
            return topicId == o.topicId && partition == o.partition && producerId == o.producerId;
        }
    };
    struct RemoteMarkerKeyHash {
        std::size_t operator()(const RemoteMarkerKey& k) const noexcept {
            return std::hash<int64_t>{}(k.producerId) ^ (static_cast<std::size_t>(static_cast<uint32_t>(k.topicId)) << 20) ^
                   static_cast<std::size_t>(static_cast<uint32_t>(k.partition));
        }
    };
    struct LastRemoteMarker {
        int32_t origin = -1; int32_t coordinatorEpoch = -1; int64_t token = 0;
        bool answered = false; int16_t errorCode = 0; int64_t offset = -1;
    };
    std::unordered_map<RemoteMarkerKey, LastRemoteMarker, RemoteMarkerKeyHash> lastRemoteMarker_;
    uint64_t seenRegistryRoleChanges_ = 0;
    void pruneRemoteMarkerIndex_();
    bool routingChangedForMarkers_ = false;      ///< D9: leader-change check due (LOW-7)
    bool resendAllRemoteMarkers_   = false;      ///< D9 MAJOR-1: re-send every remote marker
    std::vector<int32_t> reconnectedPeers_;      ///< D9 MAJOR-1: re-send markers to these peers
    int  resendRemoteMarkers_();
    /// The remote leader of (topicId, partition) from the routing image, or -1
    /// when the marker is local (no channel / image, unassigned, or led here).
    [[nodiscard]] int32_t remoteMarkerLeader_(int32_t topicId, int32_t partition, int32_t& leaderEpoch) const noexcept;
    bool sendRemoteMarker_(int64_t token, MarkerInFlight& m);
    void sendMarkerResponse_(int32_t toNode, const rune::replication::WriteTxnMarkerResponseMsg& r);
    void answerRemoteOrigin_(const MarkerInFlight& m, int16_t errorCode, int64_t offset);
    int  pumpMarkerResponses_();
    /// doWork's marker step: queued markers (op ring / cluster plane), queued
    /// answers, the unanswered-remote-marker sweep.
    int  pumpTxnMarkers_();
    void failRemoteMarkersOnLeaderChange_();
    /// Complete a coordinator-side marker as refused (TCS retry) and forget it.
    void refuseMarker_(int64_t token, int16_t errorCode);
    /// The inline (no worker pool) append of appendControlRecord.
    int64_t appendControlRecordInline_(int32_t topicId, int32_t partition, std::span<const uint8_t> marker,
                                       rune::storage::Log* log);
    [[nodiscard]] bool markerQueuedFor_(int32_t topicId, int32_t partition) const noexcept;

    // M5: subscription lifecycle ops the owning worker ring could not take are
    // queued per worker (FIFO: a Subscribe/Unsubscribe pair keeps its order)
    // and retried every doWork — never dropped.  A queued Subscribe re-resolves
    // its session's egress publication at retry time (never a dangling pointer;
    // a session gone meanwhile drops it).
    struct PendingSubOp {
        bool              unsubscribe = false;
        bool              mdc         = false;   // forwarded with the MDC pub (no unicast)
        BufferedSubscribe sub;                   // subscribe: everything; unsubscribe: subId/topic/partition
    };
    std::vector<std::deque<PendingSubOp>> pendingSubOps_;   // indexed by owning worker
    bool trySubOp_(const PendingSubOp& p);
    void queueSubOp_(PendingSubOp p);
    int  pumpSubOps_();
public:
    [[nodiscard]] uint64_t controlRecordsRefusedForTest() const noexcept { return controlRecordsRefused_; }
    [[nodiscard]] std::size_t markersInFlightForTest() const noexcept { return markersInFlight_.size(); }
    [[nodiscard]] std::size_t markersQueuedForTest() const noexcept { return markerSendQueue_.size(); }
    [[nodiscard]] std::size_t pendingSubOpsForTest() const noexcept {
        std::size_t n = 0;
        for (const auto& q : pendingSubOps_) n += q.size();
        return n;
    }
    [[nodiscard]] std::size_t pendingAcksForTest() const noexcept { return pendingAcks_.size(); }
    /// Test-only (single-threaded): take the per-cycle min-ISR snapshot now
    /// (doWork does it at the top of every cycle).
    void syncMinIsrForTest() noexcept { cachedMinIsr_ = minIsr_.load(std::memory_order_relaxed); }
    /// Test-only: run the deferred-ack drain once at `nowNs`.
    void drainPendingAcksForTest(int64_t nowNs) { nowNs_ = nowNs; drainPendingAcks(); }
    /// Test-only: the node-level Follower→Leader edge doWork reacts to.
    void onNodeLeadershipGainedForTest() { rebuildProducerStateFromLogs(); }
    /// Test-only: inline producer-state rebuilds begun / one doWork-style drive.
    [[nodiscard]] uint64_t inlinePsmRebuildsForTest() const noexcept { return inlinePsmRebuilds_; }
    int advanceInlinePsmScansForTest(int64_t nowNs) { nowNs_ = nowNs; return advanceInlinePsmScans_(); }
    void setNowForTest(int64_t nowNs) noexcept { nowNs_ = nowNs; }
    /// Test-only: the subscription lifecycle forwarding (worker routing).
    void forwardSubscribeForTest(int64_t subId, int64_t sessionId, int32_t topicId, int32_t partition,
                                 int64_t nextOffset, uint8_t isolation, bool isGroup, bool carrier,
                                 int64_t subscribeCorrId = 0) {
        forwardSubscribeToWorker_(subId, sessionId, topicId, partition, nextOffset, isolation, isGroup, carrier,
                                  subscribeCorrId);
    }
    void forwardUnsubscribeForTest(int64_t subId, int32_t topicId, int32_t partition) {
        forwardUnsubscribeToWorker_(subId, topicId, partition);
    }
    /// Test-only: run the retry pumps (markers + subscription ops) once.
    int pumpRetryQueuesForTest() { return pumpMarkerQueue_() + pumpSubOps_() + pumpEgressReleases_(); }
    // Record-egress lifetime (retire-on-ack) test hooks.
    void injectRecordEgressForTest(int64_t sid, int32_t worker, std::shared_ptr<aeron::ExclusivePublication> pub) {
        if (Session* s = sessions_.find(sid)) s->recordEgressByWorker[worker] = std::move(pub);
    }
    void closeSessionForTest(int64_t sid) { (void)closeSession_(sid); }
    /// Task 12: the doWork step that retries catalog Unsubscribes the admin ring refused.
    int pumpCatalogUnsubscribesForTest() { return pendingCatalogUnsubs_.empty() ? 0 : flushPendingCatalogUnsubs_(); }
    /// Drive the resolve-completion path for (sid, worker) with `pub` and one
    /// buffered subscribe `subId` on (topicId, partition).
    void resolveRecordEgressForTest(int64_t sid, int32_t worker, std::shared_ptr<aeron::ExclusivePublication> pub,
                                    int64_t subId, int32_t topicId, int32_t partition) {
        PendingRecordEgress pend;
        BufferedSubscribe b;
        b.subId = subId; b.sessionId = sid; b.topicId = topicId; b.partition = partition;
        pend.waiters.push_back(b);
        onRecordEgressResolved_(sid, worker, pend, std::move(pub));
    }
    [[nodiscard]] std::size_t retiringRecordEgressCountForTest() const noexcept { return retiringEgress_.size(); }
    [[nodiscard]] std::size_t retiringRecordEgressUnsentForTest() const noexcept { return retiringUnsent_; }
    /// Test-only: feed a worker return through the real drain handler.
    void handleWorkerReturnForTest(const rune::node::WorkerReturn& ret);
    // Task 13 (spec §3.4): shard mode — announce-before-accept (every worker
    // learns a session's reply destination before ConnectResponse), worker-side
    // produce acks (the runtime consumes post-ack notifications, never relays
    // answers), worker psm_ authority for user partitions, SessionClose on every
    // worker at Disconnect / reap / connect timeout.  Set before the runtime runs.
    void setShardMode(bool on) noexcept { shardMode_ = on; }

    // Task 9 (require_tls, spec §11 downgrade safety). Set before the runtime
    // runs (mirrors setShardMode); also usable from makeForTest()-built
    // gateways where the ctor's requireTls param isn't reachable.
    void setRequireTls(bool on) noexcept { requireTls_ = on; }
    [[nodiscard]] bool requireTls() const noexcept { return requireTls_; }

    // Task 9 (SIGHUP cert/key reload — Let's Encrypt renewal): swap in newly
    // re-read PEM text so the NEXT handshake uses the renewed cert; any
    // session already mid-handshake or established keeps its existing keys
    // (tlsCertPem_/tlsKeyPem_ are read only at a NEW Connect's
    // TlsSession::makeServer() call in handleConnect).
    //
    // Shared-nothing precondition: call ONLY from this gateway's own
    // duty-cycle thread (the same thread that calls doWork() /
    // handleConnect()) — e.g. BrokerNode::serviceAdminControl_(), which
    // already runs there, polling a trigger-only ControlRing a signal/main
    // thread offered on SIGHUP.  Never call this from a signal handler or
    // any other thread: there is no lock here, by design (rule 1 — the
    // gateway's state is single-writer, and the cross-thread boundary is
    // the ring, not this setter).
    void reloadTlsCert(std::string certPem, std::string keyPem) noexcept {
        tlsCertPem_ = std::move(certPem);
        tlsKeyPem_  = std::move(keyPem);
    }

    /// DR-15 fix round 2 (LOW-4): a client's reply-channel HOSTNAME is resolved
    /// by the cold resolver thread through `port` and the egress publication is
    /// added on the numeric endpoint — no driver conductor (worker 0's thread
    /// under fused placement) ever resolves a client-supplied name.  Unset: the
    /// channel is added as given (Aeron resolves it).  Before the gateway runs.
    void setHostResolver(rune::consensus::HostResolvePort port) noexcept { hostResolver_ = port; }
    [[nodiscard]] bool shardMode() const noexcept { return shardMode_; }
    [[nodiscard]] std::size_t pendingSessionClosesForTest() const noexcept { return sessionAnnouncer_.pendingCloses(); }
    void drainWorkerReturnsForTest(int64_t nowNs) { nowNs_ = nowNs; drainWorkerReturns_(); }
    rune::storage::ProducerStateManager& producerStateManagerForTest() noexcept { return producerStateManager_; }
    [[nodiscard]] int64_t sessionLastSeenNsForTest(int64_t sid) noexcept {
        const Session* s = sessions_.find(sid);
        return s ? s->lastSeenNs : -1;
    }
    void reapExpiredForTest(int64_t nowNs) { reapExpired(nowNs); }
    /// D8b round 2: some worker reports `sid`'s ack publication Lost (NOT_CONNECTED).
    [[nodiscard]] bool sessionLostForTest(int64_t sid) noexcept {
        const Session* s = sessions_.find(sid);
        return s && s->lostByWorkers != 0;
    }
    /// D8b: some worker reports `sid` Held (acks queued its publication refuses).
    [[nodiscard]] bool sessionHeldForTest(int64_t sid) noexcept {
        const Session* s = sessions_.find(sid);
        return s && s->heldByWorkers != 0;
    }
    // Fix round 1 (I2) seams: a resolved-or-not pending connect with the given
    // announce state (no Aeron publication), the cap accounting, eviction.
    void injectPendingConnectForTest(int64_t sessionId, bool announceBegun) {
        PendingConnect p;
        p.sessionId = sessionId; p.registrationId = -1; p.announceBegun = announceBegun;
        p.announced = announceBegun; p.egressChannel = "aeron:ipc";
        pendingConnects_.push_back(std::move(p));
    }
    [[nodiscard]] std::size_t shardSessionSlotsInUseForTest() const noexcept { return shardSessionSlotsInUse_(); }
    void evictOldestPendingConnectForTest() { evictOldestPendingConnect_(); }
    /// Fix round 3: Produce frames ABORTed because their session was held
    /// (control thread only — read via BrokerNode::onControlThreadForTest).
    [[nodiscard]] uint64_t heldProduceAbortsForTest() const noexcept { return heldProduceAborts_; }
    /// D8b-L (LOW-4): images on the client ingress subscription (control thread) — a dead client's image must be
    /// released, never pinned by an aborted (held) Produce frame.
    [[nodiscard]] int clientImageCountForTest() const { return sub_ ? sub_->imageCount() : 0; }
    /// D8c: one shutdown line with the paused-produce counters (only when any
    /// pause happened; warn so it shows at the perf log level).  Call once the
    /// control thread is joined.
    void logProducePauseStats() const;
    /// Fix round 3: the periodic no-hang stats line (duty thread; on change).
    bool logNoHangStatsIfChanged_(int64_t nowNs);
    bool logNoHangStatsForTest(int64_t nowNs) { return logNoHangStatsIfChanged_(nowNs); }
    static constexpr int64_t kNoHangStatsLogNs = 10'000'000'000LL;   // 10 s
    [[nodiscard]] int64_t nextNoHangStatsLogNsForTest() const noexcept { return nextNoHangStatsLogNs_; }
    // No-hang wave stats (fix round 2): requests answered with an error instead
    // of dropped, frames that could not be answered, spill expiries.  Read on
    // the duty thread (or after it joined); logged with logProducePauseStats.
    struct NoHangStats {
        uint64_t    rejectedRequests   = 0;
        uint64_t    unanswerableFrames = 0;
        uint64_t    spillExpiries      = 0;
        uint64_t    spillExpiryDrops   = 0;
        std::size_t spilledAnswers     = 0;
        std::size_t spillUsedBytes     = 0;
        uint64_t    spillExpiryNotified   = 0;
        uint64_t    spillExpiryNotifyLost = 0;
        std::size_t spillExpiring         = 0;
        uint64_t    expiringIngressDrops  = 0;   // fix round 4 (N9): frames of an Expiring session dropped
        // Fix round 5 (N21 / N23 / Disconnect): pre-v5 group errors answered
        // UnknownMember (+ the session-wide evictions among them), and Expiring
        // sessions finalized by the client's own Disconnect.
        uint64_t    oldClientUnknownMember       = 0;
        uint64_t    oldClientSessionWideEvictions = 0;
        uint64_t    spillExpiryClientLeft        = 0;
    };
    /// Fix round 4 (item 6 / N11): the periodic line's change detector — the
    /// COUNTERS compared one by one (the round-3 XOR signature missed changes
    /// that cancelled out); gauges (spilledAnswers / bytes / expiring) excluded.
    [[nodiscard]] static bool noHangCountersDiffer(const NoHangStats& a, const NoHangStats& b) noexcept {
        return a.rejectedRequests != b.rejectedRequests || a.unanswerableFrames != b.unanswerableFrames ||
               a.spillExpiries != b.spillExpiries || a.spillExpiryDrops != b.spillExpiryDrops ||
               a.spillExpiryNotified != b.spillExpiryNotified ||
               a.spillExpiryNotifyLost != b.spillExpiryNotifyLost ||
               a.expiringIngressDrops != b.expiringIngressDrops ||
               a.oldClientUnknownMember != b.oldClientUnknownMember ||
               a.oldClientSessionWideEvictions != b.oldClientSessionWideEvictions ||
               a.spillExpiryClientLeft != b.spillExpiryClientLeft;
    }
    [[nodiscard]] NoHangStats noHangStats() const noexcept {
        return NoHangStats{rejectedRequests_, unanswerableFrames_, spillExpiries_, spillExpiryDrops_,
                           egressSpill_.records(), egressSpill_.usedBytes(), spillExpiryNotified_,
                           spillExpiryNotifyLost_, expiringSessions_, expiringIngressDrops_,
                           oldClientUnknownMemberAnswers_, oldClientSessionWideEvictions_, spillExpiryClientLeft_};
    }
    /// D8c test seams (control thread only).
    [[nodiscard]] uint64_t creditProduceAbortsForTest() const noexcept { return creditProduceAborts_; }
    [[nodiscard]] uint64_t produceCreditUnderflowsForTest() const noexcept { return produceCreditUnderflows_; }
    [[nodiscard]] std::size_t creditBlockedSessionsForTest() const noexcept { return creditBlockedSessions_; }
    [[nodiscard]] uint32_t routedInFlightForTest(int32_t worker) const noexcept { return routedInFlight_[worker]; }
    [[nodiscard]] uint64_t workerCapProduceAbortsForTest() const noexcept { return workerCapProduceAborts_; }
    void setWorkerRoutedCapForTest(uint32_t cap) noexcept { workerRoutedCap_ = cap; }
    [[nodiscard]] uint64_t workerCapEvictionsForTest() const noexcept { return workerCapEvictions_; }
    [[nodiscard]] uint64_t directShareExpiriesForTest() const noexcept { return directShareExpiries_; }
    [[nodiscard]] int64_t lastCapEvictedSessionForTest() const noexcept { return lastCapEvictedSid_; }
    [[nodiscard]] int64_t produceInFlightForTest(int64_t sid) noexcept {
        const Session* s = sessions_.find(sid);
        return s ? static_cast<int64_t>(s->produceInFlight) : -1;
    }
    /// D8c: ONE shard-mode controlled poll over `frames` (each a whole client
    /// frame on one image, in order) through the REAL per-frame decision
    /// (admitClientFrame_) and dispatch: stops at the first ABORT, exactly as
    /// Aeron's controlledPoll does.  Returns the frames consumed.
    std::size_t controlledPollFramesForTest(const std::vector<std::vector<uint8_t>>& frames, std::size_t from = 0) {
        std::size_t i = from;
        for (; i < frames.size(); ++i) {
            if (!admitClientFrame_(frames[i].data(), frames[i].size())) break;
            if (!onClientFrame_(frames[i].data(), frames[i].size())) break;   // deferred: redelivered next poll
        }
        return i - from;
    }
    bool clientFrameHeldForTest(const uint8_t* buf, std::size_t len) { return produceFrameHeld_(buf, len); }
    /// Test-only: a session's crypto state (install a c2b key to drive encrypted ingress); null = unknown.
    [[nodiscard]] CryptoState* sessionCryptoForTest(int64_t sid) noexcept {
        Session* s = sessions_.find(sid);
        return s ? &s->crypto : nullptr;
    }
    /// Lows batch C (D8c R3): redeliveries of a cap-deferred Produce ABORTed by the per-session memo, before any
    /// decrypt / decode (control thread).
    [[nodiscard]] uint64_t capDeferMemoAbortsForTest() const noexcept { return capDeferMemoAborts_; }
    /// Lows A/B/C review L1: sessions holding a cap-deferral memo; the controlled
    /// subscription's image count after a poll (what the poll loop reports).
    [[nodiscard]] std::size_t capDeferredSessionsForTest() const noexcept { return capDeferredSessions_; }
    void noteControlledImagesForTest(int images) noexcept { noteControlledImages_(images); }
    void injectPendingRecordEgressForTest(int64_t sid, int32_t worker) { pendingRecordEgress_[{sid, worker}]; }
    [[nodiscard]] std::size_t pendingRecordEgressCountForTest() const noexcept { return pendingRecordEgress_.size(); }
private:
    int64_t                                 lastFlushNs_ = 0;

    // GB-3: Per-cycle fairness cap — max record batches pushed per subscription
    // per doWork() cycle.  Prevents one busy subscription from starving others.
    // Default is the production value; the effective cap is the member below so a
    // test can lower it to deterministically exercise the pause/resume path
    // independent of the production default.
    static constexpr int kMaxBatchesPerSubPerCycle = 1024;
    int maxBatchesPerSubPerCycle_ = kMaxBatchesPerSubPerCycle;

public:
    /// Test-only: override the per-cycle per-subscription batch cap.  Lets a test
    /// force a partial first cycle (pause) at a small, cap-value-independent bound
    /// while production keeps the 1024 default.  Not for hot-path use.
    void setMaxBatchesPerSubPerCycleForTest(int n) noexcept {
        if (n > 0) maxBatchesPerSubPerCycle_ = n;
    }
    [[nodiscard]] int maxBatchesPerSubPerCycleForTest() const noexcept {
        return maxBatchesPerSubPerCycle_;
    }

    // The 8 MiB ceiling of the client-visible batch limit (F3: the CONFIGURED
    // default when RUNE_MAX_PRODUCE_BATCH_BYTES is unset; F15 S3: == rune::node::
    // Worker::kMaxProduceBatchBytes).  Never enforced as-is in a running broker:
    // BrokerNode installs NodeConfig::produceIntakeLimit() (= min(configured,
    // Worker::maxAcceptableBatchBytes(config))) via setMaxProduceBatchBytes().
    static constexpr std::size_t kMaxProduceBatchBytes = 8u * 1024u * 1024u;

private:

    // GB-3: Max bytes fetched from Log::read per subscription per cycle.
    // Bounded to avoid reading unbounded data from a fast-filling log.
    // The first whole batch is always returned even if larger (F19: batches are
    // not size-capped by the Records frame — its length prefix is u32).
    static constexpr int32_t kMaxChunkBytes = static_cast<int32_t>(512 * 1024); // 512 KiB

    // GB-3: push encode scratch — scratch_ (kScratchSize) holds one encoded
    // single-fragment Records frame (tryClaim fast path), or only the 52-byte
    // fixed part of a larger frame sent as a vectored [fixed | batch] offer
    // (F19: the scratch size does not bound the deliverable batch size).
    // We encode into scratch_ during push; this is safe because push happens
    // AFTER the ingress poll (handlers that use scratch_ are done for this cycle).

    // GR-5: Deferred acks=All pending-ack queue.
    //
    // Pre-allocated to kMaxPendingAcks entries (zero heap alloc on the hot path).
    // Stored as a vector used in insertion order; drainPendingAcks() removes
    // entries that either: (a) HW >= lastOffset → send OK, or (b) timed out → send Internal.
    // std::vector with erase-by-swap-and-pop avoids per-cycle allocation; entries
    // are individually removed as they resolve.  All state touched only on the
    // duty-cycle thread (single-writer).
    //
    // Timeout: kPendingAckTimeoutNs (same order as the produce timeout seen by clients).
    // Cap: kMaxPendingAcks (power-of-two so the check is fast); a new acks=All produce
    // that would overflow the cap is rejected with MinIsrViolation to prevent unbounded
    // queue growth when followers are persistently lagging.
    static constexpr std::size_t kMaxPendingAcks     = 1024;          ///< bounded queue
    static constexpr int64_t kPendingAckTimeoutNs     = 30LL * 1'000'000'000LL; ///< 30 s
    std::vector<PendingAck>         pendingAcks_;

    // Default minIsr for the pending-ack gate.  Overridable via setMinIsr().
    // For a 3-node cluster: 2 (majority).  For a 1-node cluster: 1 (leader only).
    // std::atomic so setMinIsr() can safely be called from a different thread
    // (e.g. a test thread) without data racing the duty-cycle read in handleProduce().
    std::atomic<int>                minIsr_{1};

    // G1.1: Per-cycle snapshot of minIsr_.  Refreshed once at the top of doWork()
    // so handleProduce and drainPendingAcks use a plain int load instead of a
    // per-record atomic load.  Plain int: accessed only on the duty-cycle thread.
    int                             cachedMinIsr_{1};

    // G0.3: Maximum fragments polled per doWork() cycle.
    // Default 256 (raised from hard-coded 10 to avoid head-of-line blocking on
    // batched ingress).  Set via setFragmentLimit() from BrokerNode::buildGateway
    // (sourced from NodeConfig::gatewayFragmentLimit).  Plain int32_t — accessed
    // only on the duty-cycle thread (single-writer).
    int32_t                         fragmentLimit_ = 256;

    // TX-2: Idempotent-producer dedup state.
    //
    // Owned by the gateway (one per gateway instance).  Keyed by
    // (topicId, producerId, partition).  Consulted in handleProduce() when msg.producerId != -1.
    // TX-2c: topicId added to the key to prevent cross-topic state collision for the
    // same producerId+partition across different topics.
    //
    // Ownership rationale: Log and PartitionReplica do NOT own a
    // ProducerStateManager in the current codebase; the ExactlyOnceHarness
    // (integration tests) also keeps PSM separate from the Log.  The gateway is
    // the correct single-writer home: handleProduce() is the only call-site and
    // is always on the duty-cycle thread (no locks needed).
    //
    // It answers a sequence only while PROVEN consistent with the partition's
    // log: rebuilt from logStart to LEO per partition, and again whenever the
    // log / leadership it was built for changes (inlinePsm_ / proveInlinePsm_
    // below).  The node-level leader edge (rebuildProducerStateFromLogs) only
    // invalidates it.  Without a proven state a producer that retries an
    // already-durable batch would be re-appended or answered OOS.
    //
    // No per-op heap allocation: ProducerStateManager::validate() is noexcept,
    // allocation-free; update() inserts into the internal unordered_map only on
    // the first Accept for a new (producerId, partition) pair.
    //
    // F15 S5 (§2.5, Q3/I6): ONLY for partitions this gateway appends to itself
    // (no worker pool — unit rigs — or an internal partition in shard mode).
    // A worker-owned partition's producer state lives in its OWNING worker's
    // psm_ alone, in every mode: this map is never validated, updated or
    // rebuilt for it.
    rune::storage::ProducerStateManager producerStateManager_;

    // -----------------------------------------------------------------------
    // PSM <-> log consistency (review of b4594f8 + fix round 1).  The contract
    // (Kafka's): an OutOfOrderSequence answer means "NOT appended"; a stored
    // batch is answered OK (its offset in the window, -1 beyond it) or duplicate
    // and is never appended again.  producerStateManager_ can vouch for that
    // only while it is PROVEN consistent with the partition's log: built from
    // logStart to LEO (every stored batch, the leader's kept tail above HW
    // included — Kafka loads producer state to the log end) for exactly the log
    // content and leadership it now serves.  One state per (topicId,
    // partition) — a stable identity (TopicRegistry never deletes or reuses
    // one; a registry swap resets every state) — plus the FACTS it was built
    // for (M4: no Log* — the replica's incarnation + log / leadership
    // generations, or a passthrough log's own content generation):
    //   Unproven — nothing built (never, invalidated by a node-level leader
    //              edge, or the replica is not a COMPLETE leader: answered
    //              NotLeader, nothing appended — a deficient leader may still
    //              reconcile-append old-term batches).
    //   Scanning — a budgeted rebuild (I3: kInlinePsmStepBytes per step, one
    //              step per idempotent batch and one per doWork cycle — never an
    //              O(log) scan on the duty thread); answered NotLeader.
    //   Proven   — validate against producerStateManager_.  Live appends keep
    //              it exact (handleProduceBatch's update, markers' fenceEpoch).
    //   Failed   — the rebuild hit an unreadable batch (I2: LATCHED — answered
    //              Internal, outcome unknown, without rescanning) until the facts
    //              change or retryAtNs passes (doubling backoff, capped).
    // Any change of the facts (per-partition promotion, demotion, KIP-101
    // truncation, reconcile append, partition re-creation) restarts the build.
    // Inline path only (the replica is control-owned: its role / generations
    // are this thread's state).  Cold map insert; the Proven hot path is one
    // hash lookup + a compare, no allocation.
    // -----------------------------------------------------------------------
    struct InlinePsmFacts {
        uint32_t incarnation = 0;   ///< replica registry incarnation (0 = passthrough)
        uint64_t logGen      = 0;   ///< replica logGeneration() / passthrough Log::contentGeneration()
        uint64_t leadGen     = 0;   ///< replica leadershipGeneration() (0 = passthrough)
        bool operator==(const InlinePsmFacts&) const noexcept = default;
    };
    struct InlinePsmUnproven {};
    struct InlinePsmScanning {
        InlinePsmFacts facts;
        rune::storage::ProducerStateManager::Rebuild cursor;
        int64_t backoffNs = 0;      ///< carried into Failed (doubling)
    };
    struct InlinePsmProven {
        InlinePsmFacts facts;
    };
    struct InlinePsmFailed {
        InlinePsmFacts facts;
        int64_t retryAtNs = 0;
        int64_t backoffNs = 0;
    };
    using InlinePsmState = std::variant<InlinePsmUnproven, InlinePsmScanning, InlinePsmProven, InlinePsmFailed>;
    std::unordered_map<uint64_t, InlinePsmState> inlinePsm_;
    std::size_t inlinePsmScanning_  = 0;   ///< states in Scanning (doWork drives them; 0 = no work)
    uint64_t    inlinePsmRebuilds_  = 0;   ///< rebuilds begun (I2 test: never one per produce)
    static constexpr std::size_t kInlinePsmStepBytes     = 1u << 20;              // I3: per step
    static constexpr int64_t     kInlinePsmRetryMinNs    = 100'000'000;           // I2: 100 ms
    static constexpr int64_t     kInlinePsmRetryMaxNs    = 30'000'000'000;        // I2: 30 s
    enum class InlinePsmGate : uint8_t {
        Proven,       ///< validate against producerStateManager_
        NotLeader,    ///< not a complete leader, or still building: retriable, nothing appended
        Unprovable,   ///< the rebuild failed (latched): outcome unknown (Internal)
    };
    [[nodiscard]] static uint64_t inlinePsmKey_(int32_t topicId, int32_t partition) noexcept {
        return (static_cast<uint64_t>(static_cast<uint32_t>(topicId)) << 32) |
               static_cast<uint64_t>(static_cast<uint32_t>(partition));
    }
    [[nodiscard]] InlinePsmGate proveInlinePsm_(int32_t topicId, int32_t partition,
                                                rune::storage::Log& log,
                                                const rune::replication::PartitionReplica* replica);
    void setInlinePsmState_(InlinePsmState& slot, InlinePsmState next) noexcept;
    /// One budgeted step for every partition whose inline state is Scanning
    /// (doWork; a no-op branch while none is).
    int advanceInlinePsmScans_();
    void resetInlinePsm_() noexcept {
        inlinePsm_.clear();
        inlinePsmScanning_ = 0;
    }
    /// An inline acks=all waiter may still fire OK (the worker path's §3.2
    /// rule): the replica is Leader and its log generation is unchanged — or
    /// changed by exactly one cut strictly above the waiter's last offset (the
    /// waiter re-bases).  Otherwise the data it waits on may be gone.
    [[nodiscard]] static bool inlineWaiterStillValid_(const rune::replication::PartitionReplica& rep,
                                                      PendingAck& entry) noexcept;

    // TX-2b: Tracks previous-cycle leadership to detect Follower→Leader transitions.
    // Same pattern as TransactionCoordinationService::wasLeader_.
    // true  = was the leader on the previous doWork() call.
    // false = was a follower (or leadership unknown).
    // Initialized to false so the very first doWork() as leader fires the rebuild.
    bool wasLeader_ = false;

    // TX-2b / TX-7: node-level Follower→Leader edge — invalidates the inline
    // producer state of every gateway-appended partition (rebuilt lazily, to
    // LEO, by proveInlinePsm_) and rebuilds the inline read_committed state.
    //
    // Cold path: allocations acceptable.  Single-writer: duty-cycle thread only.
    void rebuildProducerStateFromLogs();

    // -----------------------------------------------------------------------
    // E2 inc2a: drainWorkerReturns_ — drain all workers' return rings and
    // process each result.
    //
    // Called at the TOP of doWork() (before ingress poll) so that produce
    // responses and push-read chunks from the PREVIOUS cycle are sent before
    // any new ingress is processed.  This preserves strictly-ordered delivery:
    // a produce response is never reordered past subsequent ingress frames.
    //
    // ProduceResult: complete the PSM update, send ProduceResponse (or enqueue
    //   PendingAck for acks=All).
    // PushReadResult: iterate the chunk's batches, encode Records frames, send
    //   them on the session egress (same back-pressure logic as pushSubscriptions).
    //
    // Only called when workerPool_ is set (guarded in doWork).
    // WorkerReturn type is fully defined in the .cpp (WorkerOp.h included there).
    // -----------------------------------------------------------------------
    void drainWorkerReturns_();
    void noteWorkerReadAnswer_(Subscription& sub, int64_t effectiveBound, bool notReady) noexcept;
    int64_t pushCycle_ = 0;   ///< pushSubscriptions passes (F15 S4 backoff clock)

    // TX-4 Task 9: Per-partition LSO tracking + aborted-txn index — F15 S4:
    // ONLY for the no-worker-pool inline path (unit rigs: the control thread
    // appends and reads those logs itself).  With a worker pool (every
    // production mode) the OWNING worker holds and serves the read_committed
    // state (RcState, design note §2.1-§2.4); this map is never fed or read.
    //
    // Maintains read_committed isolation state for each (topicId, partition)
    // that has received at least one transactional data batch or control marker.
    //
    // PartitionTxnState holds the LsoTracker + AbortedTxnIndex for one partition.
    // Updated incrementally on the single-writer thread:
    //   - handleProduce: when producerId >= 0 → lsoTracker.onProduceBatch(pid, base)
    //   - appendControlRecord: → lsoTracker.onControlMarker + abortedIndex.onAbortMarker
    //
    // Key: (topicId, partition) encoded as int64_t: (topicId << 32) | partition.
    // Created on first transactional access; never removed (partition count bounded).
    struct PartitionTxnState {
        rune::transaction::LsoTracker   lsoTracker;
        rune::transaction::AbortedTxnIndex abortedIndex;
    };

    // Encode (topicId, partition) to a unique int64 key for the map.
    static int64_t partitionKey(int32_t topicId, int32_t partition) noexcept {
        return (static_cast<int64_t>(topicId) << 32) |
               static_cast<int64_t>(static_cast<uint32_t>(partition));
    }

    // Get-or-create the PartitionTxnState for (topicId, partition).
    // Called only on the duty-cycle thread (single-writer). No locks.
    PartitionTxnState& getTxnState(int32_t topicId, int32_t partition) noexcept {
        return partitionTxnStates_[partitionKey(topicId, partition)];
    }

    // Returns nullptr if no state exists yet for this partition.
    const PartitionTxnState* getTxnStateRO(int32_t topicId, int32_t partition) const noexcept {
        auto it = partitionTxnStates_.find(partitionKey(topicId, partition));
        return (it != partitionTxnStates_.end()) ? &it->second : nullptr;
    }

    // Map from partition key → LsoTracker + AbortedTxnIndex.
    // Single-writer: only accessed on the duty-cycle thread.
    std::unordered_map<int64_t, PartitionTxnState> partitionTxnStates_;

    // No-hang fix round 2 (I1): the egress spill (see kEgressSpillBytes).
    // Single-writer: duty-cycle thread only.  Allocated once at construction.
    rune::gateway::SpillPool egressSpill_{kEgressSpillPoolBytes};
    std::vector<uint8_t>     spillStage_ = std::vector<uint8_t>(kMaxSpillRecordBytes);   // multi-block gather
    Session*    spillReadyHead_     = nullptr;   // fix round 4: Spilled sessions (intrusive, Session::spill)
    Session*    spillReadyTail_     = nullptr;
    std::size_t spillReadySessions_ = 0;
    std::array<SpillExpiring, kMaxSpillExpiring> spillExpiring_{};
    std::size_t spillExpiringCount_ = 0;
    std::size_t spillExpiringOverflow_ = 0;   // marked sessions not yet in the table (adopted on a free slot)

    // G0.1: session id captured at the start of handleProduce; used by the
    // public 4-param sendRespReliable() (test seam) to locate the session.
    // Only valid during a handleProduce() call.  Zero otherwise.
    int64_t currentProduceSessionId_ = 0;

    // G0.1: Test-only egress override.  When non-null, sendRespReliable_() and
    // drainPendingResponses() call this function instead of the real session
    // egress publication.  Null (= nullptr) in production — zero overhead on
    // the hot path (one null-check).
    TestEgressFn testEgressFn_;

    // No-hang wave (R2): cold-path counters + the test seam flag — kept at the
    // END of the object so no hot member's offset moves.
    uint64_t rejectedRequests_      = 0;       // answered with an error instead of dropped
    uint64_t unanswerableFrames_    = 0;       // dropped: no header / no session / no response type
    uint64_t spillExpiries_         = 0;       // sessions expired because the egress spill was full
    uint64_t spillExpiryDrops_      = 0;       // answers to an expiring session (it gets SessionExpired)
    uint64_t spillExpiryNotified_   = 0;       // ... whose SessionExpired was delivered
    uint64_t spillExpiryNotifyLost_ = 0;       // ... closed at the lease deadline without delivery
    int64_t  nextNoHangStatsLogNs_  = 0;       // fix round 3: periodic no-hang stats line
    uint64_t expiringIngressDrops_  = 0;       // fix round 4 (N9): frames of an Expiring session dropped
    uint64_t oldClientUnknownMemberAnswers_ = 0;   // fix round 4 (item 8): pre-v5 group errors → UnknownMember
    uint64_t oldClientSessionWideEvictions_ = 0;   // fix round 5 (N23): ... of them with an undecodable body
    uint64_t spillExpiryClientLeft_         = 0;   // fix round 5: Expiring sessions the client Disconnected
    NoHangStats lastNoHangStats_{};            // fix round 4 (N11): the last logged counters

    // G0.1: Private test constructor (skips Aeron setup).
    // Accessed via makeForTest() only.
    struct TestConstructTag {};
    explicit ClientGateway(TestConstructTag,
                           TestEgressFn  egressFn,
                           int64_t       currentSessionId);

    // G1.1: Private test constructor with leaderView_.
    // Accessed via makeForTestWithLeaderView() only.
    explicit ClientGateway(TestConstructTag,
                           TestEgressFn                egressFn,
                           std::function<LeaderView()> leaderView,
                           int64_t                     currentSessionId);
};

constexpr ClientGateway::SpillSizing ClientGateway::spillSizing() noexcept {
    return SpillSizing{kEgressSpillBytes, kEgressSpillPoolBytes, kMaxSpillRecordBytes, kSessionSpillQuota};
}

} // namespace rune::gateway
