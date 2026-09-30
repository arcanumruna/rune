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

#include <stdexcept>

namespace rune::client::producer {

// ---------------------------------------------------------------------------
// add() — RUNNER-THREAD ONLY (invoked from the kMsgRegisterProducer drain).
// A plain push_back on the single-owner vector; the producer's position is its
// ordinal, encoded into every batch correlation id it emits.
// ---------------------------------------------------------------------------

void ProducerHub::add(std::shared_ptr<ProducerImpl> impl)
{
    if (producers_.size() > batchcorr::kMaxOrdinal) {
        throw std::length_error("ProducerHub: at most 4096 producers per client");
    }
    impl->setOrdinal(static_cast<uint32_t>(producers_.size()));
    producers_.push_back(std::move(impl));
}

// ---------------------------------------------------------------------------
// Duty-cycle methods — runner thread only; iterate the single-owner vector.
// ---------------------------------------------------------------------------

void ProducerHub::onProduceResponse(int64_t correlationId, int64_t baseOffset) {
    // O(1): the batch id names its producer.  Matching is by correlation id only,
    // never by the publisher the response arrived on.
    if (!batchcorr::isBatch(correlationId)) return;       // txn / non-producer id
    const uint32_t o = batchcorr::ordinal(correlationId);
    if (o < producers_.size()) producers_[o]->onProduceResponse(correlationId, baseOffset);
}

void ProducerHub::failAll(const std::string& reason) {
    for (auto& impl : producers_) {
        impl->failAllPending(reason);
    }
}

void ProducerHub::failPending(int64_t corrId, const std::string& reason) {
    for (auto& impl : producers_) {
        impl->failBatch(corrId, reason);
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
                                     int32_t& partition) noexcept
{
    for (auto& impl : producers_) {
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
