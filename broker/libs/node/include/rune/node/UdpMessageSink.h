#pragma once
// UdpMessageSink.h — MessageSink implementation for aeron:udp multi-node.
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
// Thread-safety: ExclusivePublication is NOT thread-safe. Only the single
// duty-cycle thread (NodeConsensusAgent) must call send().  addXxx/findXxx on
// the Aeron ClientConductor ARE thread-safe, but resolvePending() is only ever
// called from send() on that same duty-cycle thread anyway.

#include <rune/consensus/MessageSink.h>
#include <rune/consensus/Types.h>

#include <rune/aeronutil/AeronPump.h>

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <concurrent/logbuffer/BufferClaim.h>

#include <string>
#include <unordered_map>
#include <memory>
#include <chrono>
#include <thread>

namespace rune::node {

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
                   rune::aeronutil::AeronPump                               pump = {})
        : aeron_(std::move(aeron))
    {
        // Register all peer slots.  addExclusivePublication is async — it never
        // throws for an unresolvable host here; the error (if any) surfaces later
        // in findExclusivePublication().
        for (const auto& [nodeId, ep] : peerEndpoints) {
            PeerSlot slot;
            slot.channel  = ep.channel;
            slot.streamId = ep.streamId;
            registerPeer(slot);
            peers_.emplace(nodeId, std::move(slot));
        }

        // Bounded best-effort initial resolve — NEVER fatal.  Peers that are not
        // yet resolvable (e.g. a co-restarting node whose DNS entry is not up)
        // are left pending and retried lazily from send().  This is the DR-BUG-4
        // fix: a temporarily-unresolvable peer must not abort node startup.
        // Default pump: 1 ms sleep between probes (legacy); invoker pump: drive
        // this shard's own driver + conductor between probes (spec §3.6).
        (void)pump.resolve([&] { return resolvePending() == 0; }, std::chrono::seconds{5});
        // Partial (or zero) resolution is OK — resolvePending() runs on every
        // send() until the remaining peers come up.
    }

    ~UdpMessageSink() override = default;

    /// Test-only: number of peer publications still unresolved (runs one resolve probe).
    [[nodiscard]] int unresolvedForTest() noexcept { return resolvePending(); }

    UdpMessageSink(const UdpMessageSink&)            = delete;
    UdpMessageSink& operator=(const UdpMessageSink&) = delete;

    // -----------------------------------------------------------------------
    // MessageSink::send — zero-copy tryClaim, fallback offer
    // -----------------------------------------------------------------------
    void send(rune::consensus::NodeId toMember,
              const uint8_t*          buf,
              std::size_t             len) override
    {
        // Self-healing resolution: one non-blocking probe per unresolved peer.
        // Cheap when everything is already resolved (early-outs on peer.pub).
        resolvePending();

        auto it = peers_.find(toMember);
        if (it == peers_.end()) return;        // unknown peer — silent drop
        auto& pub = it->second.pub;
        if (!pub) return;                      // not yet resolved — drop (canvass retries)
        if (!pub->isConnected()) return;       // not connected — drop (will retry)

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
        std::shared_ptr<aeron::ExclusivePublication> pub; // null until resolved
    };

    // Issue addExclusivePublication for a peer slot (async; does not resolve).
    void registerPeer(PeerSlot& slot) noexcept {
        try {
            slot.registrationId = aeron_->addExclusivePublication(slot.channel, slot.streamId);
            slot.registered     = true;
        } catch (const std::exception&) {
            // addExclusivePublication is normally async and does not throw for a
            // bad host, but guard anyway so construction can never abort.  The
            // slot stays unregistered and is retried by resolvePending().
            slot.registered = false;
        }
    }

    // Non-blocking: one findExclusivePublication probe per unresolved peer.
    // On a driver error (e.g. unresolvable host while a peer is (re)starting) the
    // errored registration is dropped and RE-ISSUED so the driver retries DNS on
    // a later probe — this is what allows a peer that comes up later to reconnect.
    // NEVER throws.  Returns the number of peers still unresolved.
    int resolvePending() noexcept {
        int unresolved = 0;
        for (auto& [nodeId, slot] : peers_) {
            if (slot.pub) continue;            // already resolved
            ++unresolved;

            if (!slot.registered) {
                // Registration previously failed/errored — try to re-register so
                // a peer that has since come up can be resolved.
                registerPeer(slot);
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
                // unresolved host for a peer that is still (re)starting).  Drop
                // the dead registration and re-issue a fresh one so DNS is
                // retried — do NOT propagate (would abort startup / send).
                slot.pub.reset();
                slot.registered     = false;
                slot.registrationId = -1;
                registerPeer(slot);
            }
        }
        return unresolved;
    }

    std::shared_ptr<aeron::Aeron> aeron_;
    std::unordered_map<rune::consensus::NodeId, PeerSlot> peers_;
};

} // namespace rune::node
