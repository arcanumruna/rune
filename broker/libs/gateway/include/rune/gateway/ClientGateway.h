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

#include <Aeron.h>
#include <Subscription.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/BufferClaim.h>
#include <util/Index.h>

#include <rune/storage/Log.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
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
    // pendingAckCount — number of deferred acks=All entries waiting for HW.
    // For tests and monitoring.
    // -----------------------------------------------------------------------
    [[nodiscard]] std::size_t pendingAckCount() const noexcept {
        return pendingAcks_.size();
    }

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
    int64_t addGroupSubscription(int64_t memberSessionId,
                                 int32_t topicId,
                                 int32_t partition,
                                 int64_t fromOffset,
                                 uint8_t isolation);

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
    // GB-2: Produce handler — offset-assignment + leader-local append.
    void handleProduce      (const uint8_t* buf, std::size_t len);
    // GB-3: Subscribe/Unsubscribe handlers.
    void handleSubscribe    (const uint8_t* buf, std::size_t len);
    void handleUnsubscribe  (const uint8_t* buf, std::size_t len);
    // CG-3: Group coordination handler — routes templateIds 32–43 to GroupCoordinationService.
    // tid is the raw uint16_t template id (avoids including Protocol.h in this header).
    void handleGroupMessage (const uint8_t* buf, std::size_t len, int64_t nowNs, uint16_t tid);
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

    // Group request handler (CG-3/CG-4 fix) — non-owning; set via setGroupService().
    // Null until wired in; group messages (templateIds 32–43) are dispatched here.
    // IGroupRequestHandler is defined in this lib (gateway); the concrete impl
    // (GroupCoordinationService) lives in rune_node — no circular dependency.
    // Single-writer: only accessed on the duty-cycle thread.
    rune::gateway::IGroupRequestHandler* groupService_ = nullptr;

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
    static constexpr int kMaxBatchesPerSubPerCycle = 8;

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
};

} // namespace rune::gateway
