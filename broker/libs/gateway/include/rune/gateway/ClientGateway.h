#pragma once
// ClientGateway.h — Client-facing session gateway (BR-4 / BR-5 / BR-7a).
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

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace rune::gateway {

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
    // sessionCount — number of active sessions.
    // -----------------------------------------------------------------------
    [[nodiscard]] std::size_t sessionCount() const {
        return sessions_.size();
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
    void handlePing      (const uint8_t* buf, std::size_t len);
    void handleDisconnect(const uint8_t* buf, std::size_t len);

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
    std::vector<PendingConnect>          pendingConnects_;

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

    // Reusable encode scratch — 64 KiB covers any Milestone-A frame.
    // (ConnectResponse=38 B, Pong=26+echo, max echo=65535+26 = ~65 KiB)
    static constexpr std::size_t kScratchSize = 68 * 1024;
    std::array<uint8_t, kScratchSize>    scratch_{};

    // Cached nowNs for the current doWork() invocation (passed into handlers).
    int64_t                              nowNs_ = 0;
};

} // namespace rune::gateway
