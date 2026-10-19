#pragma once
// AeronMessageSink.h — MessageSink implementation backed by one
// aeron::ExclusivePublication per peer.
//
// Design (§4.5, P2P-8):
//   - Holds a map NodeId → std::shared_ptr<ExclusivePublication>.
//   - send(toMember, buf, len): uses ExclusivePublication::tryClaim for
//     fixed-small messages (zero-copy into the term buffer); falls back to
//     offer() when tryClaim returns ADMIN_ACTION or NOT_CONNECTED.
//   - For IPC tests each peer gets its own stream ID derived from its NodeId
//     so they can share a single Aeron channel ("aeron:ipc").
//
// Thread-safety: ExclusivePublication is NOT thread-safe for offer/tryClaim.
//   Only one thread (the ConsensusAgent's duty-cycle thread) must call send().
//   The underlying Aeron::ClientConductor IS thread-safe for addXxx()/findXxx().

#include <rune/consensus/MessageSink.h>
#include <rune/consensus/Types.h>

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <concurrent/logbuffer/BufferClaim.h>

#include <unordered_map>
#include <memory>
#include <stdexcept>
#include <chrono>
#include <thread>

namespace rune::consensus {

// ---------------------------------------------------------------------------
// AeronMessageSink
// ---------------------------------------------------------------------------
class AeronMessageSink final : public MessageSink {
public:
    // @param aeron   Shared Aeron client (already connected to a media driver).
    // @param channel Aeron channel URI, e.g. "aeron:ipc".
    // @param peers   Map NodeId → stream ID on that channel.
    //                For IPC: use (baseStreamId + peerId) as the per-peer stream.
    AeronMessageSink(std::shared_ptr<aeron::Aeron>              aeron,
                     const std::string&                         channel,
                     const std::unordered_map<NodeId, int32_t>& peerStreams)
        : aeron_(std::move(aeron))
        , channel_(channel)
    {
        // Register all publications.  We poll findExclusivePublication until
        // the conductor has resolved each one (bounded wait).
        for (auto& [nodeId, streamId] : peerStreams) {
            int64_t regId = aeron_->addExclusivePublication(channel_, streamId);
            regIds_[nodeId] = regId;
        }

        // Wait for all publications to become connected (bounded: 5 s).
        resolvePublications();
    }

    ~AeronMessageSink() override = default;

    AeronMessageSink(const AeronMessageSink&)            = delete;
    AeronMessageSink& operator=(const AeronMessageSink&) = delete;

    // -----------------------------------------------------------------------
    // MessageSink::send
    // -----------------------------------------------------------------------
    void send(NodeId toMember, const uint8_t* buf, std::size_t len) override {
        auto it = pubs_.find(toMember);
        if (it == pubs_.end()) return; // unknown peer — silent drop

        auto& pub = *it->second;
        if (!pub.isConnected()) return; // not yet connected — drop

        auto* mutableBuf = const_cast<uint8_t*>(buf);
        aeron::concurrent::AtomicBuffer atomicBuf(mutableBuf, static_cast<aeron::util::index_t>(len));

        // Prefer tryClaim for zero-copy (works when len <= mtu).
        if (len <= static_cast<std::size_t>(pub.maxPayloadLength())) {
            aeron::concurrent::logbuffer::BufferClaim claim;
            int64_t pos = pub.tryClaim(static_cast<aeron::util::index_t>(len), claim);
            if (pos > 0) {
                claim.buffer().putBytes(claim.offset(), atomicBuf, 0,
                                        static_cast<aeron::util::index_t>(len));
                claim.commit();
                return;
            }
            // tryClaim returned BACK_PRESSURED, ADMIN_ACTION, or NOT_CONNECTED:
            // fall through to offer.
        }

        // Fallback: offer (copies into term buffer internally).
        pub.offer(atomicBuf, 0, static_cast<aeron::util::index_t>(len));
        // We deliberately do not retry on back-pressure; upper layers have
        // their own retry / timeout logic.
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
        // Partial resolution is OK — unresolved peers will be silently dropped
        // in send() until the publication becomes available.
    }

    std::shared_ptr<aeron::Aeron>                           aeron_;
    std::string                                             channel_;
    std::unordered_map<NodeId, int64_t>                    regIds_;
    std::unordered_map<NodeId, std::shared_ptr<aeron::ExclusivePublication>> pubs_;
};

} // namespace rune::consensus
