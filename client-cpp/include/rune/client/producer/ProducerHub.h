// include/rune/client/producer/ProducerHub.h
//
// CB-2 — ProducerHub: single point that ClientAgent calls on the agent thread
// for all producer duty-cycle work.
//
// Design:
//   ProducerHub holds a list of ProducerImpl instances created by Client::producer().
//   On each doWork cycle the agent calls:
//     hub.drainAndFlush(nowNs, sessionId, sendFn)
//   When a ProduceResponse arrives (dispatchEgressFrame), the agent calls:
//     hub.onProduceResponse(correlationId, baseOffset, topicId, partition)
//
//   All calls are on the AGENT thread (single-writer); no mutex required here.
//   Producers are added only at setup time (before start()); they are never
//   removed during operation, so the vector is safe to iterate without locking.

#pragma once

#include <rune/client/producer/ProducerImpl.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace rune::client::producer {

class ProducerHub {
public:
    ProducerHub() = default;

    // Non-copyable, non-movable.
    ProducerHub(const ProducerHub&) = delete;
    ProducerHub& operator=(const ProducerHub&) = delete;
    ProducerHub(ProducerHub&&) = delete;
    ProducerHub& operator=(ProducerHub&&) = delete;

    // ---- Setup-phase (called before agent start()) ---------------------------

    void add(std::shared_ptr<ProducerImpl> impl) {
        impls_.push_back(std::move(impl));
    }

    // ---- Agent-thread duty cycle (implemented in ProducerHub.cpp) -----------

    /// Drain all producer rings and flush ready batches.
    /// sendFn: the agent's transport send function.
    /// Returns total work count (drained + flushed).
    int drainAndFlush(int64_t nowNs,
                      int64_t sessionId,
                      std::function<bool(const uint8_t*, std::size_t)> sendFn);

    /// Route a ProduceResponse to all producers (the one that owns the
    /// correlationId will resolve the ack; others ignore it).
    void onProduceResponse(int64_t correlationId, int64_t baseOffset);

    /// Fail all pending acks on close.
    void failAll(const std::string& reason);

    /// Sweep timed-out acks (called from Client::sweepTimeouts).
    void sweepTimedOut(const std::chrono::steady_clock::time_point& now);

    bool empty() const noexcept { return impls_.empty(); }

    /// CB-5 B: attempt to resend a redirected Produce identified by correlationId.
    /// Returns true if one producer handled it (resent or failed).
    bool retryRedirectedProduce(int64_t corrId,
                                std::function<bool(const uint8_t*, std::size_t)> sendFn);

    /// CB-5 B: resend every in-flight Produce across all producers after a
    /// leader redirect + reconnect, re-stamped with the new sessionId.
    /// Returns the total number of frames resent.
    int resendAllRedirected(int64_t newSessionId,
                            std::function<bool(const uint8_t*, std::size_t)> sendFn);

    /// CB-5 B: true if any producer has an in-flight (unacked) batch.
    [[nodiscard]] bool hasInFlight() const noexcept;


private:
    std::vector<std::shared_ptr<ProducerImpl>> impls_;
};

} // namespace rune::client::producer
