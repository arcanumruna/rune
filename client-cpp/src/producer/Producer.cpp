// src/producer/Producer.cpp
//
// CB-2/CB-3 — Producer public API: thin wrapper forwarding to ProducerImpl.

#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/producer/BatchBuilder.h>

#include <stdexcept>

namespace rune::client {

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
    impl_->submitSendRecord(topic.topicId, part, key, value,
                             /*wantAck=*/true, corrId);
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
    impl_->submitSendRecord(topic.topicId, part, key, value,
                             /*wantAck=*/true, corrId);
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

} // namespace rune::client
