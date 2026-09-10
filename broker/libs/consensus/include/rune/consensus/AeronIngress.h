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

#include <rune/consensus/IngressRing.h>

#include <Aeron.h>
#include <Subscription.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>
#include <util/Index.h>

#include <memory>
#include <stdexcept>
#include <chrono>
#include <thread>

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
    AeronIngress(std::shared_ptr<aeron::Aeron> aeron,
                 const std::string&            channel,
                 int32_t                       streamId,
                 IngressRing&                  ingressRing)
        : aeron_(std::move(aeron))
        , ingressRing_(ingressRing)
        , assembler_([this](aeron::concurrent::AtomicBuffer& buffer,
                            aeron::util::index_t             offset,
                            aeron::util::index_t             length,
                            aeron::Header&                   /*header*/)
          {
              // Write fragment bytes into the ingress ring.
              // IngressRing::write() accepts uint8_t* and size_t.
              ingressRing_.write(
                  reinterpret_cast<const uint8_t*>(buffer.buffer() + offset),
                  static_cast<std::size_t>(length));
          })
    {
        int64_t regId = aeron_->addSubscription(channel, streamId);

        // Wait up to 5 s for the subscription to be resolved.
        using namespace std::chrono;
        auto deadline = steady_clock::now() + seconds{5};
        while (steady_clock::now() < deadline) {
            sub_ = aeron_->findSubscription(regId);
            if (sub_) break;
            std::this_thread::sleep_for(milliseconds{1});
        }
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
    int poll(int fragmentLimit = 10) {
        return sub_->poll(assembler_.handler(), fragmentLimit);
    }

    aeron::Subscription& subscription() { return *sub_; }

private:
    std::shared_ptr<aeron::Aeron>          aeron_;
    std::shared_ptr<aeron::Subscription>   sub_;
    IngressRing&                           ingressRing_;
    aeron::FragmentAssembler               assembler_;
};

} // namespace rune::consensus
