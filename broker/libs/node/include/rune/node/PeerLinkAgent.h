#pragma once
// PeerLinkAgent.h — the inter-broker link handshakes of one broker (authentication plan Task 15, D33, D35).
//
// OWNED BY THE CONTROL THREAD (the runtime duty cycle).  One Aeron subscription on <self consensus endpoint>, stream
// peerAuthStreamId, and one publication per peer to that peer's consensus endpoint (the UdpMessageSink's lazy,
// self-healing resolution); one pure PeerHandshake per peer.  The handshake frames travel in the clear by design:
// nonces and MACs only, never a key.  When a link is established the agent hands the keys to the established sink
// (BrokerNode distributes them: the control-thread transports directly, every worker and the admin thread over
// rings — shared-nothing; nothing here is read by another thread).
//
// Runs only for AuthMode::Enabled with more than one peer; InProcessUnauthenticated / DevDisabled build none.
#include <rune/aeronutil/AeronPump.h>
#include <rune/aeronutil/FragmentAssembler.h>
#include <rune/consensus/PeerResolution.h>
#include <rune/consensus/UdpMessageSink.h>
#include <rune/peerlink/ClusterKeys.h>
#include <rune/peerlink/PeerHandshake.h>

#include <Aeron.h>
#include <Subscription.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace rune::node {

class PeerLinkAgent {
public:
    struct PeerEndpoint { int32_t id = -1; std::string channel; };   // the peer's CONSENSUS endpoint channel
    struct Params {
        int32_t                      selfId = -1;
        uint64_t                     incarnation = 0;      // strictly higher on every restart (the process incarnation)
        int32_t                      streamId = 0;         // RUNE_PEER_AUTH_STREAM_ID
        std::string                  bindChannel;          // this node's consensus bind channel
        std::vector<PeerEndpoint>    peers;                // every OTHER node
        rune::peerlink::ClusterKeys  keys;                 // a COPY: the file is never re-read here
    };
    /// (peer, keys, nowNs) of a link just established — a new epoch included.  Control thread.
    using EstablishedFn = void (*)(void* ctx, int32_t peer, const rune::peerlink::LinkKeys& keys, int64_t nowNs);

    /// Startup thread.  Throws std::runtime_error when the subscription does not resolve (the broker does not start).
    PeerLinkAgent(std::shared_ptr<aeron::Aeron> aeron, Params params,
                  rune::consensus::PeerResolutionSource resolution = {}, rune::aeronutil::AeronPump pump = {});
    PeerLinkAgent(const PeerLinkAgent&) = delete;
    PeerLinkAgent& operator=(const PeerLinkAgent&) = delete;

    void setEstablishedSink(EstablishedFn fn, void* ctx) noexcept { sinkFn_ = fn; sinkCtx_ = ctx; }

    /// One duty pass: drain the subscription, tick every handshake.  Returns work done.
    int doWork(int64_t nowNs);

    /// The consensus ingress saw a frame from `peer` naming an epoch we do not hold (Task 16).  Unauthenticated: it
    /// only matters once the peer has been silent (see PeerHandshake::onUnknownEpoch).
    void onUnknownEpoch(int32_t peer, uint32_t epoch, int64_t nowNs);
    /// Issue #241: a transport of this thread opened an AUTHENTICATED frame from `peer` under `epoch`, an epoch it holds
    /// receive-only - the peer holds it, so a responder awaiting the Confirm promotes it (a legacy initiator sends none).
    void onPeerUsedEpoch(int32_t peer, uint32_t epoch, int64_t nowNs);
    /// TEST ONLY (issue #241): this initiator re-handshakes the link to `peer` now (same incarnation).  False: not an
    /// Established initiator for `peer`.
    bool forceRekeyForTest(int32_t peer, int64_t nowNs);
    /// The transports' authenticated-frame counters for `peer` (monotonic sum); a rise means the peer is alive.
    void observeAuthFrames(int32_t peer, uint64_t total, int64_t nowNs) noexcept;
    [[nodiscard]] std::size_t peerCount() const noexcept { return peers_.size(); }
    [[nodiscard]] int32_t peerIdAt(std::size_t i) const noexcept { return peers_[i].id; }

    /// Peers whose link is currently established (counter 1'000'233).
    [[nodiscard]] int64_t establishedPeers() const noexcept;
    /// Refusals logged by this broker, both directions (counter 1'000'234); monotonic.
    [[nodiscard]] uint64_t refusals() const noexcept { return refusals_; }
    [[nodiscard]] uint64_t malformedFrames() const noexcept { return malformed_; }
    [[nodiscard]] const rune::peerlink::LinkState* stateOf(int32_t peer) const noexcept;

private:
    struct PeerSlot {
        int32_t                         id = -1;
        rune::peerlink::PeerHandshake   hs;
        uint64_t                        authSeen = 0;   // the transports' authenticated-frame total last observed
        uint64_t                        refusals = 0;   // this peer's: the log is rate-limited (first, then every 64th ~ 13 s)
    };

    void onFrame_(const uint8_t* b, std::size_t len, int64_t nowNs);
    void absorb_(PeerSlot& p, rune::peerlink::PeerHandshake::Out&& o, int64_t nowNs);
    [[nodiscard]] PeerSlot* slot_(int32_t id) noexcept;

    int32_t                                  self_;
    std::shared_ptr<aeron::Aeron>            aeron_;
    std::shared_ptr<aeron::Subscription>     sub_;
    rune::consensus::UdpMessageSink          sink_;       // handshake frames out (PeerPlain: never sealed)
    std::vector<PeerSlot>                    peers_;
    EstablishedFn                            sinkFn_ = nullptr;
    void*                                    sinkCtx_ = nullptr;
    uint64_t                                 refusals_ = 0;
    uint64_t                                 malformed_ = 0;
    int64_t                                  lastNowNs_ = 0;   // the pass's clock, for the fragment handler
};

} // namespace rune::node
