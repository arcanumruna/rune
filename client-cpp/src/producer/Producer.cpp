// src/producer/Producer.cpp
//
// CB-2 — Producer public API: thin wrapper forwarding to ProducerImpl.

#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>

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
// Control
// ---------------------------------------------------------------------------

void Producer::flush() noexcept {
    if (impl_) impl_->submitFlush();
}

} // namespace rune::client
