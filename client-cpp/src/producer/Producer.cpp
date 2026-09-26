// src/producer/Producer.cpp
//
// CB-2/CB-3 — Producer public API: thin wrapper forwarding to ProducerImpl.

#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/producer/BatchBuilder.h>

#include <cstdlib>
#include <stdexcept>

namespace rune::client {

// ---------------------------------------------------------------------------
// ProducerConfig::fromEnv
// ---------------------------------------------------------------------------

ProducerConfig ProducerConfig::fromEnv() noexcept {
    ProducerConfig cfg{};
    if (const char* v = std::getenv("RUNE_LINGER_US")) {
        const long val = std::strtol(v, nullptr, 10);
        if (val >= 0) cfg.lingerUs = static_cast<int64_t>(val);
    }
    if (const char* v = std::getenv("RUNE_BATCH_BYTES")) {
        const long val = std::strtol(v, nullptr, 10);
        if (val > 0) cfg.batchBytes = static_cast<int32_t>(val);
    }
    if (const char* v = std::getenv("RUNE_BATCH_RECORDS")) {
        const long val = std::strtol(v, nullptr, 10);
        if (val > 0) cfg.batchRecords = static_cast<int32_t>(val);
    }
    if (const char* v = std::getenv("RUNE_MAX_FRAME_BYTES")) {
        const long val = std::strtol(v, nullptr, 10);
        if (val > 0) cfg.maxFrameBytes = static_cast<int32_t>(val);
    }
    if (const char* v = std::getenv("RUNE_DELIVERY_TIMEOUT_MS")) {
        const long val = std::strtol(v, nullptr, 10);
        if (val > 0) cfg.deliveryTimeoutMs = static_cast<int64_t>(val);
    }
    return cfg;
}

Producer::Producer(std::shared_ptr<producer::ProducerImpl> impl) noexcept
    : impl_(std::move(impl))
{}

// ---------------------------------------------------------------------------
// Hot path — void send (ZERO per-send heap allocation)
// ---------------------------------------------------------------------------

void Producer::send(TopicHandle            topic,
                    const Key&             key,
                    std::span<const uint8_t> value) noexcept
{
    if (!impl_ || !topic.valid()) return;
    // Compute partition client-side (same hash as the broker).
    const int32_t part = key.partition(topic.partitionCount);
    // Submit to the ring — zero alloc, no lock.
    impl_->submitSendRecord(topic.topicId, part, key, value,
                             /*wantAck=*/false, /*correlationId=*/0);
}

// ---------------------------------------------------------------------------
// Opt-in ack paths
// ---------------------------------------------------------------------------

void Producer::send(TopicHandle              topic,
                    const Key&               key,
                    std::span<const uint8_t> value,
                    std::function<void(int64_t)> callback)
{
    if (!impl_ || !topic.valid()) return;
    const int32_t part   = key.partition(topic.partitionCount);
    const int64_t corrId = impl_->registerCallback(std::move(callback));
    // Back-pressure: if the send ring is full the record never reaches the agent,
    // so the ack would never arrive — fail the pending callback immediately
    // (callback fires with offset=-1) rather than stranding it forever.
    if (!impl_->submitSendRecord(topic.topicId, part, key, value,
                                 /*wantAck=*/true, corrId)) {
        impl_->failPending(corrId, "producer send ring full — back-pressure (retry)");
    }
}

std::future<int64_t> Producer::send(TopicHandle              topic,
                                     const Key&               key,
                                     std::span<const uint8_t> value,
                                     want_future_t)
{
    if (!impl_ || !topic.valid()) {
        std::promise<int64_t> p;
        p.set_exception(std::make_exception_ptr(
            std::runtime_error("Producer: invalid topic handle")));
        return p.get_future();
    }
    const int32_t part = key.partition(topic.partitionCount);
    auto [corrId, fut] = impl_->registerFuture();
    // Back-pressure: if the send ring is full the record never reaches the agent,
    // so the ack would never arrive and future.get() would hang forever. Reject
    // the future immediately with a retriable error instead of stranding it.
    if (!impl_->submitSendRecord(topic.topicId, part, key, value,
                                 /*wantAck=*/true, corrId)) {
        impl_->failPending(corrId, "producer send ring full — back-pressure (retry)");
    }
    return std::move(fut);
}

// ---------------------------------------------------------------------------
// Tier-2: explicit atomic batch
// ---------------------------------------------------------------------------

BatchBuilder Producer::batch(TopicHandle topic, const Key& key) {
    if (!impl_ || !topic.valid()) {
        // Return a "dead" BatchBuilder; send() will throw (empty batch guard).
        // Use a null impl so add() and send() are still safely callable.
        // Actually: for an invalid topic, return a BatchBuilder that will throw
        // on send().  We satisfy this by returning a builder with impl_ set and
        // topicId=-1; the submitTier2Batch call will fail gracefully.
        // Best: throw here so the caller knows immediately.
        throw std::invalid_argument("Producer::batch: invalid topic handle");
    }
    const int32_t part = key.partition(topic.partitionCount);
    return BatchBuilder(impl_, topic.topicId, part);
}

// ---------------------------------------------------------------------------
// Control
// ---------------------------------------------------------------------------

void Producer::flush() noexcept {
    if (impl_) impl_->submitFlush();
}

// ---------------------------------------------------------------------------
// Instrumentation counters
// ---------------------------------------------------------------------------

uint64_t Producer::framesSent() const noexcept {
    return impl_ ? impl_->framesSent() : 0u;
}

uint64_t Producer::recordsSent() const noexcept {
    return impl_ ? impl_->recordsSent() : 0u;
}

} // namespace rune::client
