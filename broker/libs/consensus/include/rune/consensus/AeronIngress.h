#pragma once
// AeronIngress.h — Subscribes to one Aeron stream and feeds incoming frames
// into a node's IngressRing for ConsensusAgent to drain.
//
// Design (§4.5, P2P-8):
//   - Holds a single aeron::Subscription on a per-node channel+stream.
//   - poll(fragmentLimit) drives the subscription; each fragment is
//     delivered to FragmentAssembler which reassembles multi-frame messages
//     and calls the handler, which writes the bytes into ingressRing_.
//   - Called from the same thread as ConsensusAgent::doWork() (or from
//     an AgentRunner that interleaves poll + doWork).
//
// Thread-safety: poll() must be called from ONE thread only.

#include <rune/aeronutil/AeronPump.h>
#include <rune/consensus/IngressRing.h>
#include <rune/peerlink/PeerFrameCrypto.h>   // authentication Task 16
#include <spdlog/spdlog.h>

#include <Aeron.h>
#include <Subscription.h>
#include <rune/aeronutil/FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>
#include <util/Index.h>

#include <memory>
#include <vector>
#include <stdexcept>
#include <chrono>

namespace rune::consensus {

// ---------------------------------------------------------------------------
// AeronIngress
// ---------------------------------------------------------------------------
class AeronIngress {
public:
    // @param aeron        Shared Aeron client.
    // @param channel      Aeron channel URI, e.g. "aeron:ipc".
    // @param streamId     The stream this node listens on.
    // @param ingressRing  Ring to write decoded frames into.
    // @param pump         Startup resolve pump (default: sleep-poll a threaded driver).
    //                     (Production fused placement services the runtime driver from
    //                     an interim runner and uses the default pump — BrokerNode.)
    AeronIngress(std::shared_ptr<aeron::Aeron> aeron,
                 const std::string&            channel,
                 int32_t                       streamId,
                 IngressRing&                  ingressRing,
                 rune::aeronutil::AeronPump    pump = {},
                 rune::peerlink::PeerCryptoMode crypto = rune::peerlink::PeerPlain{})
        : aeron_(std::move(aeron))
        , ingressRing_(ingressRing)
        , maxFrame_(ingressRing.maxMsgLength())
        , crypto_(std::move(crypto))
    {
        if (std::holds_alternative<rune::peerlink::PeerFrameCrypto>(crypto_))
            recvScratch_.resize(maxFrame_ + rune::peerlink::kPeerFrameOverhead);   // COLD, once
        int64_t regId = aeron_->addSubscription(channel, streamId);

        // Until Aeron answers (bounded by Aeron's driverTimeoutMs, default 10 s).  Spec §3.6: an invoker driver/conductor (fused runtime) is
        // pumped by this thread between probes; the default pump sleeps 1 ms.
        sub_ = pump.resolve([&] { return aeron_->findSubscription(regId); }, rune::aeronutil::AeronPump::kUntilAnswered);   // D8: until Aeron answers
        if (!sub_) {
            throw std::runtime_error("AeronIngress: subscription not resolved within timeout");
        }
    }

    ~AeronIngress() = default;

    AeronIngress(const AeronIngress&)            = delete;
    AeronIngress& operator=(const AeronIngress&) = delete;

    // -----------------------------------------------------------------------
    // poll — drive the subscription; returns fragment count.
    // Must be called from ONE thread only.
    // -----------------------------------------------------------------------
    // Task 11 fix 2 (B3): templated assembler + handler — no std::function.
    int poll(int fragmentLimit = 10) {
        const int n = sub_->poll(
            [this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l,
                   aeron::Header& h) {
                assembler_.onFragment(
                    [this](aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                           aeron::util::index_t length, aeron::Header&) {
                        if (partitionedForTest_) [[unlikely]] { ++droppedPartitionedForTest_; return; }   // #111 seam
                        const auto* frame = reinterpret_cast<const uint8_t*>(buffer.buffer() + offset);
                        const auto len = static_cast<std::size_t>(length);
                        if (auto* c = std::get_if<rune::peerlink::PeerFrameCrypto>(&crypto_)) [[unlikely]] {
                            // #254: a frame whose plaintext cannot fit the ring is dropped before any cipher work.
                            if (len > maxFrame_ + rune::peerlink::kPeerFrameOverhead) [[unlikely]] { noteOversize_(len); return; }
                            // Task 16 (D34): open AFTER reassembly, BEFORE the ring - a frame that does not open
                            // never reaches the consensus agent (dropped, counted, rate-limited log).
                            std::size_t plainLen = 0; int32_t from = -1; uint32_t epoch = 0;
                            const auto r = c->open(frame, static_cast<std::size_t>(length), recvScratch_.data(),
                                                   recvScratch_.size(), plainLen, from, epoch);
                            if (r != rune::peerlink::PeerFrameCrypto::OpenResult::Ok) [[unlikely]] { noteOpenDrop_(r, from, epoch); return; }
                            if (plainLen > maxFrame_) [[unlikely]] { noteOversize_(plainLen); return; }
                            ingressRing_.write(recvScratch_.data(), plainLen);
                            return;
                        }
                        // #254: the ring refuses (throws on) a message above its maximum; peer input never reaches
                        // it - an oversized frame is dropped, counted and logged rate-limited.
                        if (len > maxFrame_) [[unlikely]] { noteOversize_(len); return; }
                        // Write the (reassembled) frame into the ingress ring.
                        ingressRing_.write(frame, len);
                    },
                    b, o, l, h);
            },
            fragmentLimit);
        imageWatch_.afterPoll(*sub_, assembler_);   // free buffers of lost images (m-c)
        return n;
    }

    aeron::Subscription& subscription() { return *sub_; }

    /// Test seam (issue #111), the polling thread only: while set, every inbound consensus frame is consumed and
    /// dropped - this node hears no peer (with UdpMessageSink::setPartitionedForTest: a node cut off the consensus).
    void setPartitionedForTest(bool on) noexcept { partitionedForTest_ = on; }
    [[nodiscard]] uint64_t droppedPartitionedForTest() const noexcept { return droppedPartitionedForTest_; }

    /// #254: inbound frames dropped because they exceed the ingress ring's maximum message (monotonic).
    [[nodiscard]] uint64_t oversizeDrops() const noexcept { return oversizeDrops_; }
    /// The largest consensus frame (plaintext) this ingress delivers: the ring's maximum message.
    [[nodiscard]] std::size_t maxFrameLength() const noexcept { return maxFrame_; }

    // ----- Authentication Task 16: this ingress's frame crypto (the receive side of the consensus link).  Control
    //       thread only; the PeerLinkAgent's keys are installed here by the same thread. -----
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
    [[nodiscard]] uint64_t authFrames(int32_t peer) const noexcept {
        const auto* c = std::get_if<rune::peerlink::PeerFrameCrypto>(&crypto_);
        return c ? c->authFrames(peer) : 0u;
    }
    [[nodiscard]] uint64_t peerFrameDrops() const noexcept {
        const auto* c = std::get_if<rune::peerlink::PeerFrameCrypto>(&crypto_);
        return c ? c->drops() : 0u;
    }
    /// The (peer, epoch) of the last frame naming an epoch this ingress does not hold - consumed once.  The peer's
    /// heartbeats arrive continuously, so the PeerLinkAgent (same thread) learns of a stale link within a beat.
    [[nodiscard]] bool takeUnknownEpoch(int32_t& peer, uint32_t& epoch) noexcept {
        if (!pendingUnknown_.valid) return false;
        peer = pendingUnknown_.peer; epoch = pendingUnknown_.epoch; pendingUnknown_.valid = false;
        return true;
    }
    /// Issue #241: the (peer, epoch) of the last authenticated frame under an epoch this ingress holds receive-only -
    /// consumed once; the PeerLinkAgent (same thread) promotes it (a legacy initiator sends no Confirm).
    [[nodiscard]] bool takePeerUsedEpoch(int32_t& peer, uint32_t& epoch) noexcept {
        auto* c = std::get_if<rune::peerlink::PeerFrameCrypto>(&crypto_);
        return c != nullptr && c->takePeerUsedEpoch(peer, epoch);
    }
    /// Frames this ingress dropped as UnknownEpoch (monotonic; 0 when not sealed).
    [[nodiscard]] uint64_t unknownEpochDrops() const noexcept {
        const auto* c = std::get_if<rune::peerlink::PeerFrameCrypto>(&crypto_);
        return c ? c->dropsOf(rune::peerlink::PeerFrameCrypto::OpenResult::UnknownEpoch) : 0u;
    }
private:
    void noteOversize_(std::size_t len) noexcept {
        if ((oversizeDrops_++ % 1024) == 0)
            spdlog::warn("AeronIngress: inbound consensus frame of {} B exceeds the {} B maximum - dropped ({} dropped so far)",
                         len, maxFrame_, oversizeDrops_);
    }
    void noteOpenDrop_(rune::peerlink::PeerFrameCrypto::OpenResult r, int32_t from, uint32_t epoch) noexcept {
        if (r == rune::peerlink::PeerFrameCrypto::OpenResult::UnknownEpoch) pendingUnknown_ = PendingUnknown{true, from, epoch};
        if ((openDropsLogged_++ % 1024) == 0)
            spdlog::warn("AeronIngress: frame from node {} dropped: {} (epoch {}; {} dropped so far)", from,
                         rune::peerlink::PeerFrameCrypto::name(r), epoch, openDropsLogged_);
    }
    struct PendingUnknown { bool valid = false; int32_t peer = -1; uint32_t epoch = 0; };

    std::shared_ptr<aeron::Aeron>          aeron_;
    std::shared_ptr<aeron::Subscription>   sub_;
    IngressRing&                           ingressRing_;
    bool                                   partitionedForTest_ = false;   // test seam (polling thread)
    uint64_t                               droppedPartitionedForTest_ = 0;
    std::size_t                            maxFrame_;                              // ingressRing_.maxMsgLength()
    rune::peerlink::PeerCryptoMode         crypto_{rune::peerlink::PeerPlain{}};   // Task 16: chosen at construction
    std::vector<uint8_t>                   recvScratch_;                           // an opened frame (preallocated)
    PendingUnknown                         pendingUnknown_{};
    uint64_t                               openDropsLogged_ = 0;
    uint64_t                               oversizeDrops_ = 0;
    rune::aeronutil::FragmentAssembler     assembler_;
    rune::aeronutil::ImageLossWatch        imageWatch_;
};

} // namespace rune::consensus
