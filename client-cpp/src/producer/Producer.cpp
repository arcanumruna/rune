// src/producer/Producer.cpp
//
// CB-2/CB-3 — Producer public API: thin wrapper forwarding to ProducerImpl.

#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/Completions.h>

#include <cstdlib>
#include <stdexcept>
#include <string>

namespace rune::client {

namespace {
SendResult mapReserve(OpResult r) noexcept {
    switch (r) {
    case OpResult::QueueFull:   return SendResult::QueueFull;
    case OpResult::Closed:      return SendResult::Closed;
    case OpResult::WrongThread: return SendResult::WrongThread;
    default:                    return SendResult::InvalidArgument;
    }
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

// F8c C6: the batch-ack credit unit every send reserves when batchAcksTo() is
// bound (cold: only producers bound to a batch-ack queue reach this).
namespace {
[[nodiscard]] SendResult reserveBatchUnit(Completions& bq) noexcept {
    if (!bq.onOwnerThread()) return SendResult::WrongThread;
    const OpResult r = bq.reserve(producer::kAckRecordBytes);
    return r == OpResult::Accepted ? SendResult::Ok : mapReserve(r);
}
} // namespace

SendResult Producer::trySend(TopicHandle            topic,
                             const Key&             key,
                             std::span<const uint8_t> value) noexcept
{
    if (!impl_ || !topic.valid()) return SendResult::InvalidTopic;
    Completions* bq = impl_->batchAckCq();
    if (bq == nullptr) [[likely]] {
        return impl_->trySubmit(topic.topicId, key.partition(topic.partitionCount), key, value, 0);
    }
    const SendResult u = reserveBatchUnit(*bq);
    if (u != SendResult::Ok) return u;
    const SendResult r = impl_->trySubmit(topic.topicId, key.partition(topic.partitionCount), key, value, 0);
    if (r == SendResult::Ok) {
        ++bq->producerIssued_;
    } else {
        bq->unreserve(producer::kAckRecordBytes);
    }
    return r;
}

void Producer::send(TopicHandle            topic,
                    const Key&             key,
                    std::span<const uint8_t> value)
{
    if (!impl_ || !topic.valid()) {
        throw std::invalid_argument("Producer::send: invalid topic handle");
    }
    if (impl_->batchAckCq() != nullptr) [[unlikely]] {
        // Credit-reserving path: retry back-pressure only (QueueFull is the
        // caller's to resolve — it must poll its queue).
        producer::ProducerImpl::BlockingWait w = impl_->beginBlockingWait();
        for (;;) {
            const SendResult r = trySend(topic, key, value);
            if (r == SendResult::Ok) return;
            producer::ProducerImpl::throwForSendResult(r);  // QueueFull included: poll it
            impl_->blockingWaitStep(w);
        }
    }
    const int32_t part = key.partition(topic.partitionCount);
    const SendResult r = impl_->trySubmit(topic.topicId, part, key, value, 0);
    if (r == SendResult::Ok) [[likely]] return;
    impl_->sendBlocking(r, topic.topicId, part, key, value);
}

// ---------------------------------------------------------------------------
// Opt-in ack paths (F8c C6: tagged tokens, zero allocation)
// ---------------------------------------------------------------------------

SendResult Producer::trySend(TopicHandle topic, const Key& key, std::span<const uint8_t> value,
                             Completions& cq, Token token) noexcept {
    if (!impl_ || !topic.valid()) return SendResult::InvalidTopic;
    if (token > kMaxToken || cq.client_ == nullptr || cq.client_ != impl_->hooks().ctx ||
        cq.ring_ == nullptr) {
        return SendResult::InvalidArgument;
    }
    // Owner check in EVERY build (review m8): the credit counters are the
    // owner thread's plain state; a foreign thread must never touch them.
    if (!cq.onOwnerThread()) return SendResult::WrongThread;
    const OpResult rr = cq.reserve(producer::kAckRecordBytes);
    if (rr != OpResult::Accepted) return mapReserve(rr);
    Completions* bq = impl_->batchAckCq();
    if (bq != nullptr) [[unlikely]] {
        const SendResult u = reserveBatchUnit(*bq);
        if (u != SendResult::Ok) {
            cq.unreserve(producer::kAckRecordBytes);
            return u;
        }
    }
    const uint64_t h = producer::ackhandle::forCq(cq.cqRef_, token);
    const SendResult r = impl_->trySubmit(topic.topicId, key.partition(topic.partitionCount), key, value,
                                          static_cast<int64_t>(h));
    if (r == SendResult::Ok) [[likely]] {
        ++cq.producerIssued_;
        if (bq != nullptr) ++bq->producerIssued_;
        return r;
    }
    // Rejected: no completion will ever arrive — return the credit here.
    cq.unreserve(producer::kAckRecordBytes);
    if (bq != nullptr) bq->unreserve(producer::kAckRecordBytes);
    return r;
}

SendResult Producer::trySend(TopicHandle topic, const Key& key, std::span<const uint8_t> value,
                             AckToken token) noexcept {
    if (!impl_ || !topic.valid()) return SendResult::InvalidTopic;
    if (token.token > kMaxToken) return SendResult::InvalidArgument;
    Completions* bq = impl_->batchAckCq();
    if (bq != nullptr) [[unlikely]] {
        const SendResult u = reserveBatchUnit(*bq);
        if (u != SendResult::Ok) return u;
    }
    const uint64_t h = producer::ackhandle::forHandler(token.token);
    const SendResult r = impl_->trySubmit(topic.topicId, key.partition(topic.partitionCount), key, value,
                                          static_cast<int64_t>(h));
    if (bq != nullptr) {
        if (r == SendResult::Ok) ++bq->producerIssued_;
        else bq->unreserve(producer::kAckRecordBytes);
    }
    return r;
}

namespace {
// The blocking forms: retry BackPressured with the void send()'s waiting rules
// (sendBlocking decides pump / wait / throw); everything else is final.
template <class Try>
void sendRetrying(producer::ProducerImpl& impl, Try&& attempt) {
    producer::ProducerImpl::BlockingWait w = impl.beginBlockingWait();
    for (;;) {
        const SendResult r = attempt();
        if (r == SendResult::Ok) return;
        producer::ProducerImpl::throwForSendResult(r);  // QueueFull: only the caller can poll
        impl.blockingWaitStep(w);
    }
}
} // namespace

void Producer::send(TopicHandle topic, const Key& key, std::span<const uint8_t> value, Completions& cq,
                    Token token) {
    if (!impl_ || !topic.valid()) throw std::invalid_argument("Producer::send: invalid topic handle");
    sendRetrying(*impl_, [&] { return trySend(topic, key, value, cq, token); });
}

void Producer::send(TopicHandle topic, const Key& key, std::span<const uint8_t> value, AckToken token) {
    if (!impl_ || !topic.valid()) throw std::invalid_argument("Producer::send: invalid topic handle");
    sendRetrying(*impl_, [&] { return trySend(topic, key, value, token); });
}

void Producer::onAck(AckFn fn, void* ctx) noexcept {
    if (impl_) impl_->setOnAck(fn, ctx);
}

void Producer::onBatchAck(BatchAckFn fn, void* ctx) {
    if (impl_) impl_->setOnBatchAck(fn, ctx);
}

void Producer::batchAcksTo(Completions& cq) {
    if (!impl_) return;
    if (cq.client_ == nullptr || cq.client_ != impl_->hooks().ctx || cq.cqRef_ == kNoCqRef) {
        throw std::logic_error("Producer::batchAcksTo: the queue is not registered with this producer's Client");
    }
    impl_->setBatchAckCq(&cq, cq.cqRef_);
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
// F8c C6a: close
// ---------------------------------------------------------------------------

OpResult Producer::close(const CompletionTarget& target, Token token) {
    if (!impl_) return OpResult::InvalidArgument;
    return impl_->beginClose(target, token);
}

OpStatus Producer::close() { return closeAndWait(std::chrono::nanoseconds::max()); }

OpStatus Producer::closeAndWait(std::chrono::nanoseconds timeout) {
    if (!impl_) return OpStatus::Closed;
    if (impl_->clientClosed()) return OpStatus::Closed;  // never touch a gone Client
    const auto& hk = impl_->hooks();
    if (hk.andWait == nullptr) {
        throw std::logic_error("Producer::close(): no Client to wait on (use close(target, token))");
    }
    struct IssueCtx {
        producer::ProducerImpl* impl;
        OpResult                last = OpResult::InvalidArgument;
    } ic{impl_.get()};
    const detail::AndWaitIssue issue = [](void* c, const CompletionTarget& t, Token tok) {
        auto* x = static_cast<IssueCtx*>(c);
        x->last = x->impl->beginClose(t, tok);
        return x->last;
    };
    try {
        const Completion c = hk.andWait(hk.ctx, "Producer::close()", timeout, issue, &ic, nullptr);
        return c.status;
    } catch (const std::runtime_error&) {
        // The intake was already closed (second close / closeImmediately /
        // Client closed): nothing to wait for.
        if (ic.last == OpResult::Closed) return OpStatus::Closed;
        throw;
    }
}

OpResult Producer::closeImmediately() {
    if (!impl_) return OpResult::InvalidArgument;
    return impl_->closeImmediately();
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
