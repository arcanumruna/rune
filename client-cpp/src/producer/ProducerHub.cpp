// src/producer/ProducerHub.cpp
//
// CB-2 — ProducerHub non-inline method implementations.
//
// Shared-nothing, single-owner model (NO atomics, NO locks):
//   The producer list is owned by the runner (agent) thread.  add() and every
//   duty-cycle iterator run on that one thread, so the list is a plain vector
//   iterated without any synchronization.  Registration crosses the app→runner
//   boundary through the agent's M2O command ring (kMsgRegisterProducer), the
//   sanctioned cross-thread mechanism — never a shared atomic or mutex here.

#include <rune/client/producer/ProducerHub.h>

namespace rune::client::producer {

// ---------------------------------------------------------------------------
// add() — RUNNER-THREAD ONLY (invoked from the kMsgRegisterProducer drain).
// A plain push_back on the single-owner vector.
// ---------------------------------------------------------------------------

void ProducerHub::add(std::shared_ptr<ProducerImpl> impl)
{
    producers_.push_back(std::move(impl));
}

// ---------------------------------------------------------------------------
// Duty-cycle methods — runner thread only; iterate the single-owner vector.
// ---------------------------------------------------------------------------

int ProducerHub::drainAndFlush(int64_t nowNs,
                                int64_t sessionId,
                                std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> sendFn)
{
    int work = 0;
    for (auto& impl : producers_) {
        // Drain + flush in a tight loop: drain one message at a time so that
        // flush boundaries (kMsgFlush or batchBytes triggers) are honoured
        // between individual records.  This ensures that a flush() call between
        // two sends produces two separate batches (the expected user-facing
        // semantic).  We cap the loop at 256 iterations to bound CPU.
        static constexpr int kMaxInnerIters = 256;
        for (int iter = 0; iter < kMaxInnerIters; ++iter) {
            const int drained = impl->drainRing(nowNs);
            const int flushed = impl->flushBatches(nowNs, sessionId, sendFn);
            work += drained + flushed;
            if (drained == 0) break; // no more messages in the ring
        }
        // Reliability: retransmit any in-flight batch whose attempt timed out (O(1)
        // when nothing is stalled — checks the oldest batch and returns).
        work += impl->retransmitStalled(nowNs, sendFn);
    }
    return work;
}

void ProducerHub::onProduceResponse(int64_t correlationId, int64_t baseOffset) {
    for (auto& impl : producers_) {
        impl->onProduceResponse(correlationId, baseOffset);
    }
}

void ProducerHub::failAll(const std::string& reason) {
    for (auto& impl : producers_) {
        impl->failAllPending(reason);
    }
}

void ProducerHub::failPending(int64_t corrId, const std::string& reason) {
    for (auto& impl : producers_) {
        impl->failPending(corrId, reason);
    }
}

bool ProducerHub::retryRedirectedProduce(
    int64_t corrId,
    std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> sendFn)
{
    for (auto& impl : producers_) {
        if (impl->retryRedirectedProduce(corrId, sendFn)) {
            return true;
        }
    }
    return false;
}

int ProducerHub::resendAllRedirected(
    int64_t newSessionId,
    std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> sendFn)
{
    int resent = 0;
    for (auto& impl : producers_) {
        resent += impl->resendAllRedirected(newSessionId, sendFn);
    }
    return resent;
}

int ProducerHub::resendAllViaRoute(
    std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> sendFn)
{
    int resent = 0;
    for (auto& impl : producers_) {
        resent += impl->resendAllViaRoute(sendFn);
    }
    return resent;
}

bool ProducerHub::hasInFlight() const noexcept {
    for (const auto& impl : producers_) {
        if (impl->hasInFlight()) return true;
    }
    return false;
}

void ProducerHub::sweepTimedOut(const std::chrono::steady_clock::time_point& now) {
    for (auto& impl : producers_) {
        impl->sweepTimedOut(now);
    }
}

bool ProducerHub::partitionForCorrId(int64_t corrId,
                                     int32_t& topicId,
                                     int32_t& partition) const noexcept
{
    for (const auto& impl : producers_) {
        if (impl->partitionForCorrId(corrId, topicId, partition)) {
            return true;
        }
    }
    return false;
}

bool ProducerHub::firstPendingPartition(int32_t& topicId,
                                        int32_t& partition) const noexcept
{
    for (const auto& impl : producers_) {
        if (impl->firstPendingPartition(topicId, partition)) {
            return true;
        }
    }
    return false;
}

void ProducerHub::updateIngressRoute(int32_t topicId, int32_t partition,
                                     int32_t ingressStreamId) noexcept
{
    for (auto& impl : producers_) {
        impl->updateIngressRoute(topicId, partition, ingressStreamId);
    }
}

} // namespace rune::client::producer
