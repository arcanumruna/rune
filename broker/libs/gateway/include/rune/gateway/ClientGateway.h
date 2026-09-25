#pragma once
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
//       every entry: if replica->highWatermark() >= entry.lastOffset, the
//       ProduceResponse is sent and the entry removed.
//       min-ISR gate: before enqueuing, canProduce(minIsr, acksAll=true) is
//       checked via the ISR manager; if ISR is too small, ProduceResponse
//       MinIsrViolation is sent immediately (no append).
//       Timeout: entries stuck beyond kPendingAckTimeoutNs receive
//       ProduceResponse{Internal} and are evicted (follower never advancing).
//       acks=Leader/None: unchanged — ack immediately after append.
//   - Single-writer: doWork() must be called from ONE thread only.
//   - Zero-alloc on the hot path: encode scratch is a fixed member buffer.
//   - NO busy-spin.

#include <rune/gateway/SessionTable.h>
#include <rune/gateway/IGroupRequestHandler.h>
#include <rune/gateway/ITxnRequestHandler.h>

// Forward-declared to avoid pulling the full WorkerPool header (and its heavy
// chain: Worker → SpscRing → WorkerOp → ReplicationMessages) into the gateway
// header.  All WorkerPool calls are in ClientGateway.cpp.
namespace rune::node { class WorkerPool; }

#include <Aeron.h>
#include <Subscription.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/BufferClaim.h>
#include <util/Index.h>

#include <rune/storage/Log.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/transaction/ReadCommittedView.h>
#include <rune/metadata/RoutingImage.h>
// G0.1: ErrorCode enum for sendRespReliable() signature.
#include <rune/protocol/client/Protocol.h>

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
#include <unordered_map>
#include <unordered_set>
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

// ---------------------------------------------------------------------------
// ClientGateway
// ---------------------------------------------------------------------------
class ClientGateway {
public:
    // Default session timeout: 10 seconds.
    static constexpr int64_t kDefaultSessionTimeoutNs = 10LL * 1'000'000'000LL;

    // Async Connect egress-resolve timeout: 5 seconds.
    static constexpr int64_t kDefaultConnectResolveTimeoutNs = 5LL * 1'000'000'000LL;

    // Maximum in-flight pending connects.  Beyond this cap, the oldest is evicted.
    static constexpr std::size_t kMaxPendingConnects = 64;

    // @param aeron                    Shared Aeron client (connected to a media driver).
    // @param clientChannel            Aeron channel the gateway listens on (clients publish here).
    // @param clientStreamId           Stream ID on clientChannel.
    // @param thisNodeId               This node's id; echoed in ConnectResponse.leaderMemberId.
    // @param sessionTimeoutNs         Idle session expiry threshold (default 10 s).
    // @param leaderView               Callable returning current LeaderView; default: always leader.
    // @param connectResolveTimeoutNs  How long before a pending-connect egress times out (default 5 s).
    ClientGateway(std::shared_ptr<aeron::Aeron>       aeron,
                  std::string                         clientChannel,
                  int32_t                             clientStreamId,
                  int32_t                             thisNodeId,
                  int64_t                             sessionTimeoutNs         = kDefaultSessionTimeoutNs,
                  std::function<LeaderView()>         leaderView               = nullptr,
                  int64_t                             connectResolveTimeoutNs  = kDefaultConnectResolveTimeoutNs);

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

    [[nodiscard]]
    bool committedMetadataReady() const noexcept {
        return committedMetadataReady_;
    }

    void setNodeEndpointMap(
            std::unordered_map<int32_t, std::string> nodeEndpoints) noexcept {
        nodeEndpointMap_ = std::move(nodeEndpoints);
    }

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
    void setWorkerPool(rune::node::WorkerPool* pool) noexcept {
        workerPool_ = pool;
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

    // -----------------------------------------------------------------------
    // setMinIsr — configure the minimum ISR size gate for acks=All produces.
    // Default: 1 (single-node safe; for 3-node clusters, set to 2).
    // Thread-safe (atomic store): may be called from any thread.
    // -----------------------------------------------------------------------
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
        return pendingRespCount_;
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
    // \returns baseOffset of the appended marker, or -1 if the topic/partition
    //          is unknown or the replica is not in leader role.
    //
    // Single-writer: must be called on the duty-cycle thread.
    // ---------------------------------------------------------------------------
    int64_t appendControlRecord(int32_t                    topicId,
                                int32_t                    partition,
                                std::span<const uint8_t>   marker);

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

    // sendControlFrame — send an already-encoded frame (e.g. Assign) on a
    // session's egress.  Reuses the same sendTo path as all other responses.
    // Returns true if the frame was accepted by the egress publication.
    bool sendControlFrame(int64_t sessionId, const uint8_t* buf, std::size_t len);

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
    int64_t injectTestSession(int64_t nowNs = 0) {
        const int64_t sid = sessions_.nextSessionId();
        sessions_.add(sid, /*egress=*/nullptr, nowNs);
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

    // -----------------------------------------------------------------------
    // applyProduceForTest — feed a raw Produce frame through the real
    // handleProduce() path (test seam).  Lets integration tests exercise the
    // full produce routing, acks=All bootstrap rejection, and GR-4/GR-5 gates
    // without a live Aeron client publishing ingress frames.
    // Single-writer: call only on the duty-cycle thread or before doWork().
    // -----------------------------------------------------------------------
    void applyProduceForTest(const uint8_t* buf, std::size_t len) {
        handleProduce(buf, len);
    }

    // -----------------------------------------------------------------------
    // E5.1: applyMetadataForTest — feed a raw MetadataRequest frame through
    // the real handleMetadata() path (test seam), parallel to applyProduceForTest.
    // Single-writer: call only on the duty-cycle thread or before doWork().
    // -----------------------------------------------------------------------
    void applyMetadataForTest(const uint8_t* buf, std::size_t len) {
        handleMetadata(buf, len);
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
    // Safety invariants:
    //   - replica pointer is non-owning; outlives the gateway (same lifetime
    //     contract as replicaRegistry_ pointer).
    //   - All fields set once on enqueue; read-only during drain.
    //   - Single-writer: only touched on the duty-cycle thread.
    // -----------------------------------------------------------------------
    struct PendingAck {
        int64_t      sessionId     = 0;   ///< owning session
        int64_t      correlationId = 0;   ///< echoed back in ProduceResponse
        int32_t      partition     = 0;   ///< partition (echoed in ProduceResponse)
        int64_t      baseOffset    = 0;   ///< first offset of the batch
        int64_t      lastOffset    = 0;   ///< last offset of the batch (HW must reach this)
        int64_t      enqueuedAtNs  = 0;   ///< for timeout eviction
        // Non-owning: null only when replicaRegistry_ is not set (passthrough mode,
        // which does not produce acks=All entries).
        rune::replication::PartitionReplica* replica = nullptr;
    };

    // -----------------------------------------------------------------------
    // G0.1: PendingResp — one already-encoded response frame waiting for the
    // egress publication to clear back-pressure.
    //
    // Pre-allocated inline (no hot-path heap alloc).  The encoded bytes are
    // copied into `bytes` at enqueue time (encode-once strategy); `sessionId`
    // identifies the egress session on drain.
    //
    // kMaxBytes = 512 covers ProduceResponse (38 B) and ErrorResponse with a
    // typical redirect URL (< ~450 B).  Responses that exceed kMaxBytes are
    // sent best-effort and NOT enqueued (same as pre-fix behaviour for those
    // rare oversized frames).  A static_assert in sendRespReliable_ ensures the
    // produce-ack path (ProduceResponse::ENCODED_SIZE = 38) always fits.
    //
    // Single-writer: all accesses are on the duty-cycle thread only.
    // -----------------------------------------------------------------------
    struct PendingResp {
        static constexpr std::size_t kMaxBytes = 512;
        std::array<uint8_t, kMaxBytes> bytes{};
        std::size_t len       = 0;
        int64_t     sessionId = 0;
    };

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
    void handleKeepalive (const uint8_t* buf, std::size_t len, int64_t nowNs);
    void handlePing      (const uint8_t* buf, std::size_t len, int64_t nowNs);
    void handleDisconnect(const uint8_t* buf, std::size_t len);
    // MD1 Task 10: Metadata request — answerable by any node (reads routingImage_ + nodeEndpointMap_).
    void handleMetadata     (const uint8_t* buf, std::size_t len);
    // GB-2: Produce handler — offset-assignment + leader-local append.
    void handleProduce      (const uint8_t* buf, std::size_t len);
    // GB-3: Subscribe/Unsubscribe handlers.
    void handleSubscribe    (const uint8_t* buf, std::size_t len);
    void handleUnsubscribe  (const uint8_t* buf, std::size_t len);
    // CG-3: Group coordination handler — routes templateIds 32–43 to GroupCoordinationService.
    // tid is the raw uint16_t template id (avoids including Protocol.h in this header).
    void handleGroupMessage (const uint8_t* buf, std::size_t len, int64_t nowNs, uint16_t tid);
    // TX-3: Transaction coordination handler — routes templateIds 45,47,49,51,53
    // to TransactionCoordinationService.
    void handleTxnMessage   (const uint8_t* buf, std::size_t len, int64_t nowNs, uint16_t tid);
    // GB-3: Push delivery — called from doWork after ingress poll.
    void pushSubscriptions  ();
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
    // G0.1: enqueueOrBestEffort_ — THE single reliable-egress primitive.
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
    // `len` must be <= PendingResp::kMaxBytes; oversized frames (rare long
    // redirect URLs) are sent best-effort (they cannot fit the fixed ring slot).
    //
    // Single-writer: duty-cycle thread only.
    // -----------------------------------------------------------------------
    void enqueueOrBestEffort_(Session& session, const uint8_t* buf, std::size_t len);

    // -----------------------------------------------------------------------
    // G0.1: sendRespReliable_ — private production implementation.
    // Encodes a ProduceResponse into scratch_ and routes it through
    // enqueueOrBestEffort_.  Always returns true (never silently drops).
    // Returns false only on encode error (shouldn't happen given static_assert).
    //
    // NOTE: The public 4-param sendRespReliable() delegates here using
    // currentProduceSessionId_.  All internal callers (handleProduce) should
    // call sendRespReliable_() directly for clarity.
    // -----------------------------------------------------------------------
    bool sendRespReliable_(Session& session,
                           rune::protocol::client::ErrorCode ec,
                           int32_t                           partition,
                           int64_t                           baseOffset,
                           int64_t                           correlationId);

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
    // Resolve a new ExclusivePublication (bounded wait).
    // Returns nullptr if the publication cannot be resolved within the deadline.
    // NOTE: kept for the redirect path (non-leader with known leader endpoint),
    // which still needs a synchronous resolve to send the ErrorResponse.
    // The regular (leader-accept) Connect path is now async via pendingConnects_.
    // -----------------------------------------------------------------------
    std::shared_ptr<aeron::ExclusivePublication>
    resolvePublication(const std::string& channel, int32_t streamId);

    // -----------------------------------------------------------------------
    // Members
    // -----------------------------------------------------------------------
    std::shared_ptr<aeron::Aeron>        aeron_;
    std::string                          clientChannel_;
    int32_t                              clientStreamId_;
    int32_t                              thisNodeId_;
    int32_t                              workerIngressBaseStreamId_ = 0;  // E5.1
    int64_t                              sessionTimeoutNs_;
    int64_t                              connectResolveTimeoutNs_;
    std::function<LeaderView()>          leaderView_;

    std::shared_ptr<aeron::Subscription> sub_;
    aeron::FragmentAssembler             assembler_;

    SessionTable                         sessions_;

    // Async pending-connect table (BR-7a).
    // Bounded to kMaxPendingConnects; all touched only in doWork (single-writer).
    // std::deque gives O(1) pop_front (evict-oldest) while still supporting
    // erase(it) for mid-sequence resolve/timeout removals.
    std::deque<PendingConnect>           pendingConnects_;

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
    //  ProduceResponse=38 B; restamp scratch = max RecordBatch = 65535 B)
    static constexpr std::size_t kScratchSize = 68 * 1024;
    std::array<uint8_t, kScratchSize>    scratch_{};

    // GB-2: Reusable restamp scratch for offset-assignment (single per-produce
    // copy of the incoming RecordBatch bytes with baseOffset rewritten in place).
    // Sized to the same max frame — avoids per-produce heap allocation.
    // Single-writer: only accessed on the duty-cycle thread via handleProduce().
    std::array<uint8_t, kScratchSize>    restampScratch_{};

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
    std::unordered_map<int32_t, std::string>             nodeEndpointMap_;

    // Cached nowNs for the current doWork() invocation (passed into handlers).
    int64_t                              nowNs_ = 0;

    // GB-3: Subscription registry — keyed by subscriptionId.
    // Iterated each doWork() to push new Records to each subscriber's egress.
    // Subscriptions are reaped when the owning session is removed/reaped.
    // Single-writer: only accessed on the duty-cycle thread.
    std::unordered_map<int64_t, Subscription> subscriptions_;

    // GB-3: Monotonic subscription ID generator (starts at 1).
    int64_t nextSubscriptionId_ = 0;

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
    int64_t                                 lastFlushNs_ = 0;

    // GB-3: Per-cycle fairness cap — max record batches pushed per subscription
    // per doWork() cycle.  Prevents one busy subscription from starving others.
    static constexpr int kMaxBatchesPerSubPerCycle = 1024;

    // GB-3: Max bytes fetched from Log::read per subscription per cycle.
    // Bounded to avoid reading unbounded data from a fast-filling log.
    // Each batch is at most 65535 bytes (u16 var field cap) + overhead ≈ 64 KiB.
    static constexpr int32_t kMaxChunkBytes = static_cast<int32_t>(512 * 1024); // 512 KiB

    // GB-3: Reusable push scratch — sized to hold one encoded Records frame.
    // Records header = 50 bytes fixed + up to 65535 bytes of recordBatch.
    // 68 KiB covers that; share kScratchSize constant.
    // NOTE: scratch_ is already kScratchSize = 68 KiB and is reused here.
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
    // TX-2b: On leader transition (Follower→Leader) or on startup as leader, the
    // PSM is rebuilt from each partition Log via rebuildProducerStateFromLogs().
    // This mirrors the TransactionCoordinationService::doWork pattern: wasLeader_
    // tracks the previous cycle's leadership so the rebuild fires exactly once on
    // the first duty cycle after becoming leader.  Without this rebuild, a
    // producer that retries an already-durable batch after failover would be
    // incorrectly accepted (re-appended), violating exactly-once semantics.
    //
    // No per-op heap allocation: ProducerStateManager::validate() is noexcept,
    // allocation-free; update() inserts into the internal unordered_map only on
    // the first Accept for a new (producerId, partition) pair.
    rune::storage::ProducerStateManager producerStateManager_;

    // TX-2b: Tracks previous-cycle leadership to detect Follower→Leader transitions.
    // Same pattern as TransactionCoordinationService::wasLeader_.
    // true  = was the leader on the previous doWork() call.
    // false = was a follower (or leadership unknown).
    // Initialized to false so the very first doWork() as leader fires the rebuild.
    bool wasLeader_ = false;

    // TX-2b: Rebuild the PSM from all partition Logs in the TopicRegistry.
    //
    // Called from doWork() exactly once on Follower→Leader transition.  For each
    // (topicId, partition) in topicRegistry_, reads the partition Log and calls
    // ProducerStateManager::rebuildFromLog() to reconstruct the dedup window.
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

    // TX-4 Task 9: Per-partition LSO tracking + aborted-txn index.
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

    // -----------------------------------------------------------------------
    // G0.1: Egress retry queue — bounded, pre-allocated, no hot-path alloc.
    //
    // pendingResponses_: ring buffer of encoded frames waiting for back-pressure
    //   to clear.  Head/tail/count are maintained as plain indices (single-writer,
    //   no CAS/atomics needed).  Capacity: kMaxPendingResponses.
    //
    // pendingRespHead_:  index of the first (oldest) entry; popped on drain.
    // pendingRespCount_: number of live entries (0 = empty).
    //
    // When the ring is full and a new response must be enqueued, the frame is
    // sent best-effort via sendTo (same as pre-fix behaviour) and a warning is
    // logged.  This is a hard-stop guard — the ring should never fill in practice
    // (back-pressure clears within a few doWork cycles under normal conditions).
    //
    // Single-writer: all accesses on the duty-cycle thread.
    // -----------------------------------------------------------------------
    static constexpr std::size_t kMaxPendingResponses = 64;
    std::array<PendingResp, kMaxPendingResponses> pendingResponses_{};
    std::size_t pendingRespHead_  = 0;
    std::size_t pendingRespCount_ = 0;

    // G0.1: session id captured at the start of handleProduce; used by the
    // public 4-param sendRespReliable() (test seam) to locate the session.
    // Only valid during a handleProduce() call.  Zero otherwise.
    int64_t currentProduceSessionId_ = 0;

    // G0.1: Test-only egress override.  When non-null, sendRespReliable_() and
    // drainPendingResponses() call this function instead of the real session
    // egress publication.  Null (= nullptr) in production — zero overhead on
    // the hot path (one null-check).
    TestEgressFn testEgressFn_;

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

} // namespace rune::gateway
