#pragma once
// UdpMessageSink.h — MessageSink implementation for aeron:udp multi-node.
// (Lives in rune_consensus since Task 11 fix 2 so SbeConsensusPublisher can
// call it DIRECTLY — final class, no MessageSink vtable on the duty cycle.
// rune::node::UdpMessageSink is an alias.)
//
// Design (BR-3):
//   AeronMessageSink uses ONE channel + per-peer stream IDs (fine for IPC).
//   For UDP, each peer has a different endpoint (channel). This sink holds a
//   NodeId → ExclusivePublication map where each publication is on
//   peer.consensusChannel() with the shared consensusStreamId.
//
//   Mirrors AeronMessageSink's tryClaim→offer logic exactly.
//
// Peer resolution (DR-BUG-4):
//   Peer publications are resolved LAZILY and SELF-HEALINGLY, never fatally.
//   addExclusivePublication() is async: it returns a registrationId immediately
//   and the media-driver conductor resolves the endpoint (incl. DNS) later.
//   findExclusivePublication() THROWS if the driver returned an error for that
//   registration — e.g. a peer hostname that does not yet resolve because the
//   peer container is still (re)starting.
//
//   The OLD design resolved all peers synchronously in the constructor and let
//   that exception escape → BrokerNode::start() aborted → the node exited.  In a
//   quorum-loss recovery (two nodes restarted at once) each restarted node tried
//   to resolve the OTHER still-down peer, threw "unknown host", and crashed —
//   so the cluster never re-formed its ISR and acks=All could not reach minIsr.
//
//   The fix: NEVER let a peer-resolution error abort startup or a send.  Instead:
//     * The constructor issues addExclusivePublication for every peer and does a
//       bounded best-effort resolve, swallowing per-peer errors.
//     * A peer whose registration ERRORED is RE-REGISTERED (fresh
//       addExclusivePublication) so the driver retries DNS on the next probe —
//       this is what lets a peer that comes up later actually reconnect.
//     * resolvePending() runs one non-blocking probe per unresolved peer; it is
//       invoked at the top of every send() (the single consensus duty-cycle
//       thread), so resolution is retried continuously with no spin, no block,
//       and no fatal path.
//
// DR-15 fix round 1 (2026-10-05): with a PeerResolutionSource wired (every
// BrokerNode), a peer whose registration errored is re-added ONLY on its host's
// resolution event from the cold PeerResolverHub (the driver answers peer hosts
// from that off-thread cache — no getaddrinfo on this duty thread, no re-add
// per send).  Without one (unmanaged / numeric host), the DR-BUG-4 re-register
// below is unchanged.
//
// Thread-safety: ExclusivePublication is NOT thread-safe. Only the single
// duty-cycle thread (NodeConsensusAgent) must call send().  addXxx/findXxx on
// the Aeron ClientConductor ARE thread-safe, but resolvePending() is only ever
// called from send() on that same duty-cycle thread anyway.

#include <rune/consensus/IngressRing.h>   // kMaxConsensusFrameBytes (#254)
#include <rune/consensus/MessageSink.h>
#include <rune/consensus/Types.h>

#include <rune/aeronutil/AeronPump.h>
#include <rune/consensus/PeerResolution.h>
#include <rune/peerlink/PeerFrameCrypto.h>   // authentication Task 16: AEAD on every inter-broker frame
#include <spdlog/spdlog.h>

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <concurrent/logbuffer/BufferClaim.h>

#include <cstdio>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <chrono>
#include <thread>

namespace rune::consensus {

// ---------------------------------------------------------------------------
// UdpMessageSink — per-peer UDP ExclusivePublication
// ---------------------------------------------------------------------------
class UdpMessageSink final : public rune::consensus::MessageSink {
public:
    struct PeerEndpoint {
        std::string channel;   // "aeron:udp?endpoint=host:port"
        int32_t     streamId;
    };

    // @param aeron        Shared Aeron client (already connected to a media driver).
    // @param peerEndpoints  Map NodeId → {channel, streamId} for each peer (not self).
    UdpMessageSink(std::shared_ptr<aeron::Aeron>                            aeron,
                   const std::unordered_map<rune::consensus::NodeId,
                                             PeerEndpoint>&                 peerEndpoints,
                   rune::aeronutil::AeronPump                               pump = {},
                   rune::consensus::PeerResolutionSource                    resolution = {},
                   rune::peerlink::PeerCryptoMode                           crypto = rune::peerlink::PeerPlain{})
        : aeron_(std::move(aeron))
        , crypto_(std::move(crypto))
    {
        if (std::holds_alternative<rune::peerlink::PeerFrameCrypto>(crypto_))
            sealScratch_.resize(kMaxSealedPayload + rune::peerlink::kPeerFrameOverhead);   // COLD, once
        // DR-15 fix round 1: off-thread resolution events (PeerResolverHub).
        inbox_.attach(resolution);
        (void)inbox_.poll();   // prefilled addresses first: the adds below go out numeric (DR-4)
        // Register all peer slots.  addExclusivePublication is async — it never
        // throws for an unresolvable host here; the error (if any) surfaces later
        // in findExclusivePublication().
        for (const auto& [nodeId, ep] : peerEndpoints) {
            PeerSlot slot;
            slot.channel  = ep.channel;
            slot.streamId = ep.streamId;
            slot.hostIdx  = inbox_.hostIndexOfChannel(ep.channel);
            registerPeer(slot, /*retry=*/false);
            peers_.emplace(nodeId, std::move(slot));
        }

        // Bounded best-effort initial resolve — NEVER fatal.  Peers that are not
        // yet resolvable (e.g. a co-restarting node whose DNS entry is not up)
        // are left pending and retried lazily from send().  This is the DR-BUG-4
        // fix: a temporarily-unresolvable peer must not abort node startup.
        // Default pump: 1 ms sleep between probes (legacy); invoker pump: drive
        // this shard's own driver + conductor between probes (spec §3.6).
        // 2026-10-05 (DR-15 audit): stop as soon as Aeron has ANSWERED every
        // registration (resolved or errored) — an unresolvable peer no longer
        // holds node startup for the whole 5 s budget.
        (void)pump.resolve([&] { (void)resolvePending(); return everyPeerAnswered_(); }, std::chrono::seconds{5});
        // Partial (or zero) resolution is OK — resolvePending() runs on every
        // send() until the remaining peers come up.
    }

    ~UdpMessageSink() override = default;

    // ----- Authentication Task 16: this sink's frame crypto (sender id 0, D33).  Control thread only. -----
    [[nodiscard]] bool peerSealed() const noexcept { return std::holds_alternative<rune::peerlink::PeerFrameCrypto>(crypto_); }
    bool installPeerLink(int32_t peer, const rune::peerlink::LinkKeys& keys) noexcept {
        auto* c = std::get_if<rune::peerlink::PeerFrameCrypto>(&crypto_);
        if (c == nullptr) return false;
        try { return c->installLink(peer, keys, rune::peerlink::steadyNowNs()); } catch (...) { return false; }
    }
    [[nodiscard]] uint32_t peerLinkEpoch(int32_t peer) const noexcept {
        const auto* c = std::get_if<rune::peerlink::PeerFrameCrypto>(&crypto_);
        return c ? c->currentEpoch(peer) : 0u;
    }
    [[nodiscard]] uint64_t peerFrameDrops() const noexcept {
        const auto* c = std::get_if<rune::peerlink::PeerFrameCrypto>(&crypto_);
        return c ? c->drops() : 0u;
    }
    /// The largest consensus frame a sealed sink carries: the receiver's ingress ring maximum (#254).
    static constexpr std::size_t kMaxSealedPayload = rune::consensus::kMaxConsensusFrameBytes;

    /// Test-only: number of peer publications still unresolved (runs one resolve probe).
    [[nodiscard]] int unresolvedForTest() noexcept { return resolvePending(); }
    /// Test-only: registrations issued after the first one per peer.
    [[nodiscard]] uint64_t reAddsForTest() const noexcept { return reAdds_; }
    /// Test seam (issue #111), the sending thread only: while set nothing is sent to any peer.
    void setPartitionedForTest(bool on) noexcept { partitionedForTest_ = on; }

    UdpMessageSink(const UdpMessageSink&)            = delete;
    UdpMessageSink& operator=(const UdpMessageSink&) = delete;

    // -----------------------------------------------------------------------
    // MessageSink::send — zero-copy tryClaim, fallback offer
    // -----------------------------------------------------------------------
    void send(rune::consensus::NodeId toMember,
              const uint8_t*          buf,
              std::size_t             len) override
    {
        if (partitionedForTest_) [[unlikely]] return;   // issue #111 seam: this node reaches no peer
        // Self-healing resolution: one non-blocking probe per unresolved peer.
        // Cheap when everything is already resolved (early-outs on peer.pub).
        resolvePending();

        auto it = peers_.find(toMember);
        if (it == peers_.end()) return;        // unknown peer — silent drop
        auto& pub = it->second.pub;
        if (!pub) return;                      // not yet resolved — drop (canvass retries)
        if (!pub->isConnected()) return;       // not connected — drop (will retry)

        // Task 16 (D34): a sealed sink seals the WHOLE frame before Aeron fragments it; no established link to the
        // destination -> nothing is sent (counted, rate-limited log).  PeerPlain: one predicted branch, today's bytes.
        if (auto* c = std::get_if<rune::peerlink::PeerFrameCrypto>(&crypto_)) [[unlikely]] {
            if (!c->hasLink(toMember)) {
                c->noteNoLink();
                if ((noLinkLogged_++ % 1024) == 0)
                    spdlog::warn("UdpMessageSink: peer frames to node {} dropped: link not established ({} dropped so far)",
                                 toMember, noLinkLogged_);
                return;
            }
            if (len > kMaxSealedPayload) [[unlikely]] {
                c->noteNoLink();   // counted with the other undeliverable frames
                if ((noLinkLogged_++ % 1024) == 0)
                    spdlog::error("UdpMessageSink: consensus frame {} B exceeds the {} B seal scratch - dropped", len, kMaxSealedPayload);
                return;
            }
            const std::size_t n = c->seal(toMember, buf, len, sealScratch_.data(), sealScratch_.size());
            if (n == 0) [[unlikely]] return;   // a cipher error: never transmitted
            buf = sealScratch_.data();
            len = n;
        }

        auto* mutableBuf = const_cast<uint8_t*>(buf);
        aeron::concurrent::AtomicBuffer atomicBuf(
            mutableBuf, static_cast<aeron::util::index_t>(len));

        // Prefer tryClaim for zero-copy (works when len <= mtu).
        if (len <= static_cast<std::size_t>(pub->maxPayloadLength())) {
            aeron::concurrent::logbuffer::BufferClaim claim;
            int64_t pos = pub->tryClaim(
                static_cast<aeron::util::index_t>(len), claim);
            if (pos > 0) {
                claim.buffer().putBytes(
                    claim.offset(), atomicBuf, 0,
                    static_cast<aeron::util::index_t>(len));
                claim.commit();
                return;
            }
            // tryClaim returned BACK_PRESSURED / ADMIN_ACTION / NOT_CONNECTED:
            // fall through to offer.
        }

        // Fallback: offer (copies internally).
        pub->offer(atomicBuf, 0, static_cast<aeron::util::index_t>(len));
        // No retry — upper layers have their own retry/timeout logic (canvass repeats).
    }

private:
    struct PeerSlot {
        std::string channel;
        int32_t     streamId       = 0;
        int64_t     registrationId = -1;
        bool        registered     = false;    // addExclusivePublication issued
        int         hostIdx        = -1;       // managed peer host (-1: unmanaged / no resolver)
        uint32_t    addGen         = 0;        // host resolution generation at the last add
        uint32_t    failures       = 0;        // errored registrations so far
        uint32_t    boundIp        = 0;        // the host's address at the last add (network order)
        bool        boundKnown     = false;    // boundIp valid (the host had resolved at that add)
        std::shared_ptr<aeron::ExclusivePublication> pub; // null until resolved
    };

    // Issue addExclusivePublication for a peer slot (async; does not resolve).
    void registerPeer(PeerSlot& slot, bool retry = true) noexcept {
        if (retry) ++reAdds_;
        slot.addGen     = inbox_.generation(slot.hostIdx);
        slot.boundKnown = inbox_.addressOf(slot.hostIdx, slot.boundIp);
        try {
            slot.registrationId = aeron_->addExclusivePublication(inbox_.addChannel(slot.channel, slot.hostIdx),
                                                                  slot.streamId);
            slot.registered     = true;
        } catch (const std::exception&) {
            // addExclusivePublication is normally async and does not throw for a
            // bad host, but guard anyway so construction can never abort.
            onFailed_(slot);
        }
    }

    // A registration errored (DR-15: the peer's host is not resolvable yet).
    // Managed host: re-add at once if it resolved since that add, else on its
    // next resolution event (fix round 1, MAJOR-1: event-driven — never a
    // send/pass count, and the driver answers peer hosts from its off-thread
    // cache, so a re-add never blocks this thread on DNS).  Unmanaged host (no
    // resolver wired — tests / numeric): re-added on the next probe (DR-BUG-4).
    void onFailed_(PeerSlot& slot) noexcept {
        ++slot.failures;
        slot.pub.reset();
        slot.registered     = false;
        slot.registrationId = -1;
        if (slot.hostIdx >= 0 && inbox_.generation(slot.hostIdx) != slot.addGen) registerPeer(slot);
    }

    void onHostResolved_(int idx) noexcept {
        for (auto& [nodeId, slot] : peers_) {
            if (slot.hostIdx != idx) continue;
            if (!slot.pub && !slot.registered) { registerPeer(slot); continue; }
            // An add still in flight is never superseded (the Aeron client only
            // releases a publication it has FOUND — overwriting the id would leak
            // one on the old address): once it completes, the hub's next
            // confirmation (~1 s) finds it bound to the stale address and re-adds.
            if (!slot.pub) continue;
            // DR-4 (2026-10-07): the peer's host now resolves to a DIFFERENT
            // address (a restarted container / pod got a new IP — two restarted
            // peers can even swap theirs).  The live publication still sends to
            // the old address, where ANOTHER member's subscription (same port +
            // stream) accepts the frames and answers the status messages, so
            // Aeron never re-resolves it: consensus traffic for this peer reaches
            // the wrong member forever.  Re-add it on the new address (the
            // driver's name cache got the change first — PeerResolverHub order).
            std::uint32_t ip = 0;
            if (!inbox_.addressOf(idx, ip)) continue;
            if (!slot.boundKnown) { slot.boundIp = ip; slot.boundKnown = true; continue; }
            if (ip == slot.boundIp) continue;   // a confirmation of the same address
            std::fprintf(stderr, "UdpMessageSink: peer %d host of %s changed address — re-adding its "
                                 "publication\n", static_cast<int>(nodeId), slot.channel.c_str());
            slot.pub.reset();
            slot.registered     = false;
            slot.registrationId = -1;
            ++unresolved_;   // resolvePending() must probe the new registration (it early-outs at 0)
            registerPeer(slot);
        }
    }

    // Non-blocking: resolution events (a few loads when empty), then one
    // findExclusivePublication probe per in-flight registration.  NEVER throws.
    // Returns the number of peers still unresolved.
    int resolvePending() noexcept {
        (void)inbox_.poll([this](int idx) { onHostResolved_(idx); });
        if (unresolved_ == 0) [[likely]] return 0;
        int unresolved = 0;
        for (auto& [nodeId, slot] : peers_) {
            if (slot.pub) continue;            // already resolved
            ++unresolved;

            if (!slot.registered) {
                // Unmanaged host: DR-BUG-4 re-register.  Managed: wait for the event.
                if (slot.hostIdx < 0) registerPeer(slot);
                continue;                      // give the driver a tick to work
            }

            try {
                auto pub = aeron_->findExclusivePublication(slot.registrationId);
                if (pub) {
                    slot.pub = std::move(pub);
                    --unresolved;              // now resolved
                }
                // else: still pending; keep probing on the next call.
            } catch (const std::exception&) {
                // Driver returned an error for this registration (commonly an
                // unresolved host for a peer that is still (re)starting) — do
                // NOT propagate (would abort startup / send).
                onFailed_(slot);
            }
        }
        unresolved_ = unresolved;
        return unresolved;
    }

    /// Aeron answered every peer's registration at least once (ready or error).
    [[nodiscard]] bool everyPeerAnswered_() const noexcept {
        for (const auto& [nodeId, slot] : peers_)
            if (!slot.pub && slot.failures == 0) return false;
        return true;
    }

    int      unresolved_ = 1;   // > 0 until a resolvePending pass finds every peer resolved
    uint64_t reAdds_     = 0;
    bool     partitionedForTest_ = false;   // issue #111 test seam (sending thread)
    rune::consensus::PeerResolutionInbox inbox_;   // consensus duty thread: resolution events

    std::shared_ptr<aeron::Aeron> aeron_;
    rune::peerlink::PeerCryptoMode crypto_{rune::peerlink::PeerPlain{}};   // Task 16: chosen at construction
    std::vector<uint8_t>           sealScratch_;                            // sealed frame (preallocated, sealed only)
    uint64_t                       noLinkLogged_ = 0;
    std::unordered_map<rune::consensus::NodeId, PeerSlot> peers_;
};

} // namespace rune::consensus
