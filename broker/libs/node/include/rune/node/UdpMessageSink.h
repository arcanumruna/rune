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
//   Bounded findExclusivePublication resolution (5 s, same as AeronMessageSink).
//
// Thread-safety: ExclusivePublication is NOT thread-safe. Only the single
// duty-cycle thread (NodeConsensusAgent) must call send().

#include <rune/consensus/MessageSink.h>
#include <rune/consensus/Types.h>

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
                                             PeerEndpoint>&                 peerEndpoints)
        : aeron_(std::move(aeron))
    {
        // Register all publications.
        for (auto& [nodeId, ep] : peerEndpoints) {
            int64_t regId = aeron_->addExclusivePublication(ep.channel, ep.streamId);
            regIds_[nodeId] = regId;
        }

        // Wait up to 5 s for all publications to be resolved by the conductor.
        resolvePublications();
    }

    ~UdpMessageSink() override = default;

    UdpMessageSink(const UdpMessageSink&)            = delete;
    UdpMessageSink& operator=(const UdpMessageSink&) = delete;

    // -----------------------------------------------------------------------
    // MessageSink::send — zero-copy tryClaim, fallback offer
    // -----------------------------------------------------------------------
    void send(rune::consensus::NodeId toMember,
              const uint8_t*          buf,
              std::size_t             len) override
    {
        auto it = pubs_.find(toMember);
        if (it == pubs_.end()) return; // unknown peer — silent drop

        auto& pub = *it->second;
        if (!pub.isConnected()) return; // not yet connected — drop (will retry on next canvass)

        auto* mutableBuf = const_cast<uint8_t*>(buf);
        aeron::concurrent::AtomicBuffer atomicBuf(
            mutableBuf, static_cast<aeron::util::index_t>(len));

        // Prefer tryClaim for zero-copy (works when len <= mtu).
        if (len <= static_cast<std::size_t>(pub.maxPayloadLength())) {
            aeron::concurrent::logbuffer::BufferClaim claim;
            int64_t pos = pub.tryClaim(
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
        pub.offer(atomicBuf, 0, static_cast<aeron::util::index_t>(len));
        // No retry — upper layers have their own retry/timeout logic (canvass repeats).
    }

private:
    void resolvePublications() {
        using namespace std::chrono;
        auto deadline = steady_clock::now() + seconds{5};

        while (steady_clock::now() < deadline) {
            bool allResolved = true;
            for (auto& [nodeId, regId] : regIds_) {
                if (pubs_.count(nodeId)) continue; // already resolved
                auto pub = aeron_->findExclusivePublication(regId);
                if (pub) {
                    pubs_[nodeId] = std::move(pub);
                } else {
                    allResolved = false;
                }
            }
            if (allResolved) return;
            std::this_thread::sleep_for(milliseconds{1});
        }
        // Partial resolution is OK — unresolved peers silently dropped in send()
        // until their publication becomes available on the next attempt.
    }

    std::shared_ptr<aeron::Aeron> aeron_;
    std::unordered_map<rune::consensus::NodeId, int64_t>                                    regIds_;
    std::unordered_map<rune::consensus::NodeId, std::shared_ptr<aeron::ExclusivePublication>> pubs_;
};

} // namespace rune::node
