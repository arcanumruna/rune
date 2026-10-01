// src/producer/Producer.cpp
//
// CB-2/CB-3 — Producer public API: thin wrapper forwarding to ProducerImpl.

#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/producer/BatchBuilder.h>

#include <cstdlib>
#include <stdexcept>

namespace rune::client {

namespace {
// fix1 minor 13: a distinct failUnsubmitted() reason per SendResult, instead
// of one generic "back-pressure or invalid value" string for every case.
const char* sendFailureReason(SendResult r) noexcept {
    switch (r) {
    case SendResult::Ok:            return "producer send accepted"; // unreachable (caller checks Ok first)
    case SendResult::BackPressured: return "producer send ring full — back-pressure (retry)";
    case SendResult::ValueTooLarge: return "producer send rejected — value too large for this producer's ring";
    case SendResult::InvalidTopic:  return "producer send rejected — invalid topic handle";
    case SendResult::WrongThread:   return "producer send rejected — wrong thread (ClientShard)";
    case SendResult::WrongShard:    return "producer send rejected — wrong shard (ClientShard)";
    case SendResult::Closed:        return "producer send rejected — client closed";
    }
    return "producer send not accepted";
}

// Task 14 fix1: Direct (Invoker) ingress — a send made from a callback the
// producer fired inside its own duty work is deferred; when that deferred
// buffer is full the rejection gets its own reason (not "ring full").
const char* rejectReason(const producer::ProducerImpl& impl, SendResult r) noexcept {
    if (r == SendResult::BackPressured && impl.lastRejectWasDeferredFull()) {
        return "producer send rejected — deferred buffer full (send from inside the producer's "
               "own duty-cycle callback; raise ProducerConfig::directDeferredBytes or retry later)";
    }
    return sendFailureReason(r);
}
} // namespace

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

SendResult Producer::trySend(TopicHandle            topic,
                             const Key&             key,
                             std::span<const uint8_t> value) noexcept
{
    if (!impl_ || !topic.valid()) return SendResult::InvalidTopic;
    return impl_->trySubmit(topic.topicId, key.partition(topic.partitionCount), key, value, 0);
}

void Producer::send(TopicHandle            topic,
                    const Key&             key,
                    std::span<const uint8_t> value)
{
    if (!impl_ || !topic.valid()) {
        throw std::invalid_argument("Producer::send: invalid topic handle");
    }
    const int32_t part = key.partition(topic.partitionCount);
    const SendResult r = impl_->trySubmit(topic.topicId, part, key, value, 0);
    if (r == SendResult::Ok) [[likely]] return;
    impl_->sendBlocking(r, topic.topicId, part, key, value);
}

// ---------------------------------------------------------------------------
// Opt-in ack paths
// ---------------------------------------------------------------------------

void Producer::send(TopicHandle              topic,
                    const Key&               key,
                    std::span<const uint8_t> value,
                    std::function<void(int64_t)> callback)
{
    if (!impl_ || !topic.valid()) {
        // fix1 I8: never destroy the callback uncalled — fire it with -1 (the
        // same "not accepted" signal failUnsubmitted uses below), consistent
        // with every other invalid-input path reporting rather than dropping.
        if (callback) callback(-1);
        return;
    }
    const int32_t part   = key.partition(topic.partitionCount);
    const int64_t ackHandle = impl_->registerCallback(std::move(callback));
    // Back-pressure / rejection: if the send ring is full (or the value/topic is
    // invalid) the record never reaches the agent, so the ack would never arrive
    // — fail the pending callback immediately (callback fires with offset=-1)
    // rather than stranding it forever.
    const SendResult r = impl_->trySubmit(topic.topicId, part, key, value, ackHandle);
    if (r != SendResult::Ok) {
        impl_->failUnsubmitted(ackHandle, rejectReason(*impl_, r));
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
    auto [ackHandle, fut] = impl_->registerFuture();
    // Back-pressure / rejection: if the send ring is full (or the value/topic is
    // invalid) the record never reaches the agent, so the ack would never
    // arrive and future.get() would hang forever. Reject the future
    // immediately with a specific error instead of stranding it.
    const SendResult r = impl_->trySubmit(topic.topicId, part, key, value, ackHandle);
    if (r != SendResult::Ok) {
        impl_->failUnsubmitted(ackHandle, rejectReason(*impl_, r));
    }
    return std::move(fut);
}

void Producer::onBatchAck(std::function<void(const BatchAck&)> cb) {
    if (impl_) impl_->onBatchAck(std::move(cb));
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

uint64_t Producer::testCallbackExceptionCount() const noexcept {
    return impl_ ? impl_->testCallbackExceptionCount() : 0u;
}

uint64_t Producer::recordsSent() const noexcept {
    return impl_ ? impl_->recordsSent() : 0u;
}

} // namespace rune::client
