#pragma once
// ClientGateway.h — Client-facing session gateway (BR-4 / BR-5 / BR-7a / GR-4).
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
//   - Single-writer: doWork() must be called from ONE thread only.
//   - Zero-alloc on the hot path: encode scratch is a fixed member buffer.
//   - NO busy-spin.

#include <rune/gateway/SessionTable.h>

#include <Aeron.h>
#include <Subscription.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/BufferClaim.h>
#include <util/Index.h>

#include <rune/storage/Log.h>

#include <array>
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
    // Subscription — per-session push subscription (GB-3).
    //
    // nextOffset tracks the exact offset to push next.  The push loop advances
    // it ONLY after a successful tryClaim (exactly-once delivery guarantee).
    // isolation: 0 = read_uncommitted, 1 = read_committed.
    //   M-B: both modes use logEndOffset() as the isolation bound; the real LSO
    //   will be used for read_committed once transactions land (Milestone C).
    // -----------------------------------------------------------------------
    struct Subscription {
        int64_t subscriptionId = 0;
        int64_t sessionId      = 0;
        int32_t topicId        = 0;
        int32_t partition      = 0;
        int64_t nextOffset     = 0;
        uint8_t isolation      = 0;   // 0=read_uncommitted, 1=read_committed
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
    // GB-3: Push delivery — called from doWork after ingress poll.
    void pushSubscriptions  ();
    // GB-5p2: Group-commit flush — called from doWork() once per cycle AFTER all
    // handleProduce() calls have completed.  Flushes each dirty partition Log
    // exactly once (one fsync per touched partition = group-commit under load).
    // Zero per-produce heap; dirtyLogs_ is a pre-allocated inline set.
    // Single-writer: called only on the duty-cycle thread.
    void flushDirtyLogs();

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

    // Cached nowNs for the current doWork() invocation (passed into handlers).
    int64_t                              nowNs_ = 0;

    // GB-3: Subscription registry — keyed by subscriptionId.
    // Iterated each doWork() to push new Records to each subscriber's egress.
    // Subscriptions are reaped when the owning session is removed/reaped.
    // Single-writer: only accessed on the duty-cycle thread.
    std::unordered_map<int64_t, Subscription> subscriptions_;

    // GB-3: Monotonic subscription ID generator (starts at 1).
    int64_t nextSubscriptionId_ = 0;

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
};

} // namespace rune::gateway
