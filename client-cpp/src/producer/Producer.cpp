// src/producer/Producer.cpp
//
// CB-2/CB-3 — Producer public API: thin wrapper forwarding to ProducerImpl.

#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/Completions.h>

#include <cstdio>
#include <cstdlib>
#include <exception>
#include <stdexcept>
#include <string>
#include <utility>

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

Producer::Producer(producer::ProducerImpl* impl) noexcept
    : impl_(impl)
{}

Producer::Producer(Producer&& o) noexcept
    : impl_(std::exchange(o.impl_, nullptr))
{}

Producer& Producer::operator=(Producer&& o) noexcept {
    if (this != &o) {
        release_();  // the current producer: closeImmediately + released
        impl_ = std::exchange(o.impl_, nullptr);
    }
    return *this;
}

Producer::~Producer() { release_(); }

// F8c C7: the §3.3 release.  Never throws, never waits for the duty side.
void Producer::release_() noexcept {
    producer::ProducerImpl* p = std::exchange(impl_, nullptr);
    if (p == nullptr) return;  // moved-from
    const producer::ProducerImpl::Hooks& hk = p->hooks();
    if (hk.release == nullptr) {
        // Bare impl (no Client — unit rigs): the handle is the only owner and
        // the caller is its duty thread.  Destroying it from inside one of its
        // own handlers would free it under the walk that fired the handler.
        if (p->inDutyWork()) {
            std::fprintf(stderr, "rune-client: FATAL: a bare Producer was destroyed inside one of its "
                                 "own handlers (use-after-free); aborting\n");
            std::fflush(stderr);
            std::abort();
        }
        p->applyHandleRelease();
        delete p;
        return;
    }
    // The duty side already detached (Client close handed it back, or the
    // registration was refused): never touch the Client — it may be gone.
    if (!p->dutyDetached()) {
        detail::HubCommand cmd;
        cmd.op     = detail::HubCommand::Op::HandleRelease;
        cmd.handle = reinterpret_cast<uint64_t>(p);
        cmd.a      = static_cast<int64_t>(detail::HandleKind::Producer);
        try {
            // Accepted: the duty side applies closeImmediately (unless a close
            // already started) and detaches its share once the producer is
            // quiescent.  Closed: a close() on another thread hands it back.
            // Orphaned: the sink handed the share over itself (F8c C7 F1).
            if (hk.release(hk.ctx, cmd) == detail::HubSubmit::Orphaned) return;
        } catch (const std::exception& e) {
            std::fprintf(stderr, "rune-client: ~Producer: release not delivered: %s\n", e.what());
        } catch (...) {
            std::fprintf(stderr, "rune-client: ~Producer: release not delivered (unknown error)\n");
        }
    }
    // The handle's last access: the side that detaches last frees the impl.
    if (p->detachHandle()) delete p;
}

// ---------------------------------------------------------------------------
// Hot path — void send (ZERO per-send heap allocation)
// ---------------------------------------------------------------------------

// F8c C6: the batch-ack credit unit every send reserves when batchAcksTo() is
// bound (cold: only producers bound to a batch-ack queue reach this).
namespace {
[[nodiscard]] SendResult reserveBatchUnit(Completions& bq) noexcept {
    if (!bq.valid()) return SendResult::InvalidArgument;  // review m7: moved-from queue
    if (!bq.onOwnerThread()) return SendResult::WrongThread;
    const OpResult r = bq.reserve(producer::kAckRecordBytes);
    return r == OpResult::Accepted ? SendResult::Ok : mapReserve(r);
}
} // namespace

// The one send body (T15b M3): the partition is computed by the caller, once.
SendResult Producer::trySendPart_(TopicHandle topic, int32_t part, const Key& key,
                                  std::span<const uint8_t> value) noexcept {
    producer::ProducerImpl* impl = impl_;
    Completions* bq = impl->batchAckCq();
    if (bq == nullptr) [[likely]] {
        return impl->trySubmit(topic.topicId, part, key, value, 0);
    }
    const SendResult u = reserveBatchUnit(*bq);
    if (u != SendResult::Ok) return u;
    const SendResult r = impl->trySubmit(topic.topicId, part, key, value, 0);
    if (r == SendResult::Ok) {
        ++bq->producerIssued_;
    } else {
        bq->unreserve(producer::kAckRecordBytes);
    }
    return r;
}

SendResult Producer::trySend(TopicHandle            topic,
                             const Key&             key,
                             std::span<const uint8_t> value) noexcept
{
    if (!impl_ || !topic.valid()) return SendResult::InvalidTopic;
    return trySendPart_(topic, key.partition(topic.partitionCount), key, value);
}

SendResult Producer::trySendTo(TopicHandle topic, int32_t partition, const Key& key,
                               std::span<const uint8_t> value) noexcept {
    if (!impl_ || !topic.valid()) return SendResult::InvalidTopic;
    if (partition < 0 || partition >= topic.partitionCount) [[unlikely]] return SendResult::InvalidArgument;
    return trySendPart_(topic, partition, key, value);
}

void Producer::send(TopicHandle            topic,
                    const Key&             key,
                    std::span<const uint8_t> value)
{
    if (!impl_ || !topic.valid()) {
        throw std::invalid_argument("Producer::send: invalid topic handle");
    }
    sendTo(topic, key.partition(topic.partitionCount), key, value);
}

void Producer::sendTo(TopicHandle topic, int32_t part, const Key& key, std::span<const uint8_t> value) {
    if (!impl_ || !topic.valid()) {
        throw std::invalid_argument("Producer::send: invalid topic handle");
    }
    if (part < 0 || part >= topic.partitionCount) {
        throw std::invalid_argument("Producer::sendTo: partition out of range");
    }
    if (impl_->batchAckCq() != nullptr) [[unlikely]] {
        // Credit-reserving path: retry back-pressure only (QueueFull is the
        // caller's to resolve — it must poll its queue).
        producer::ProducerImpl::BlockingWait w = impl_->beginBlockingWait();
        for (;;) {
            const SendResult r = trySendPart_(topic, part, key, value);
            if (r == SendResult::Ok) return;
            producer::ProducerImpl::throwForSendResult(r);  // QueueFull included: poll it
            impl_->blockingWaitStep(w);
        }
    }
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
// Phase 2b Part B: zero-copy produce
// ---------------------------------------------------------------------------

namespace {
// The batch-ack credit of a claim: `units` reservations of ONE unit each — the
// units a Tier-1 BatchAck returns per record.  All or none.
[[nodiscard]] SendResult reserveBatchUnits(Completions& bq, uint32_t units) noexcept {
    if (!bq.valid()) return SendResult::InvalidArgument;
    if (!bq.onOwnerThread()) return SendResult::WrongThread;
    for (uint32_t i = 0; i < units; ++i) {
        const OpResult r = bq.reserve(producer::kAckRecordBytes);
        if (r != OpResult::Accepted) {
            for (uint32_t j = 0; j < i; ++j) bq.unreserve(producer::kAckRecordBytes);
            return mapReserve(r);
        }
    }
    return SendResult::Ok;
}
}  // namespace

Claim Producer::claimPart_(TopicHandle topic, int32_t part, const Key& key, uint32_t valueLen,
                           uint64_t ackHandle, Completions* cq) noexcept {
    const producer::ProducerImpl::ClaimGrant g =
        impl_->claimRecord(topic.topicId, part, key, valueLen, ackHandle);
    if (g.result != SendResult::Ok) {
        if (cq != nullptr) cq->unreserve(producer::kAckRecordBytes);  // nothing will ever complete
        return Claim(g.result);
    }
    detail::ClaimOpen core{impl_, g.token, cq, nullptr, 0};
    if (Completions* bq = impl_->batchAckCq(); bq != nullptr) [[unlikely]] {
        const SendResult u = reserveBatchUnits(*bq, 1);
        if (u != SendResult::Ok) {
            detail::ClaimCredit::abort(core);  // discards the record, returns cq's credit
            return Claim(u);
        }
        core.bq    = bq;
        core.units = 1;
    }
    return Claim(core, g.base, valueLen);
}

Claim Producer::tryClaim(TopicHandle topic, const Key& key, uint32_t valueLen) noexcept {
    if (!impl_ || !topic.valid()) return Claim(SendResult::InvalidTopic);
    return claimPart_(topic, key.partition(topic.partitionCount), key, valueLen, 0, nullptr);
}

Claim Producer::tryClaim(TopicHandle topic, const Key& key, uint32_t valueLen, Completions& cq,
                         Token token) noexcept {
    if (!impl_ || !topic.valid()) return Claim(SendResult::InvalidTopic);
    if (token > kMaxToken || cq.client_ == nullptr || cq.client_ != impl_->hooks().ctx || cq.ring_ == nullptr) {
        return Claim(SendResult::InvalidArgument);
    }
    if (!cq.onOwnerThread()) return Claim(SendResult::WrongThread);
    const OpResult rr = cq.reserve(producer::kAckRecordBytes);
    if (rr != OpResult::Accepted) return Claim(mapReserve(rr));
    return claimPart_(topic, key.partition(topic.partitionCount), key, valueLen,
                      producer::ackhandle::forCq(cq.cqRef_, token), &cq);
}

Claim Producer::tryClaim(TopicHandle topic, const Key& key, uint32_t valueLen, AckToken token) noexcept {
    if (!impl_ || !topic.valid()) return Claim(SendResult::InvalidTopic);
    if (token.token > kMaxToken) return Claim(SendResult::InvalidArgument);
    return claimPart_(topic, key.partition(topic.partitionCount), key, valueLen,
                      producer::ackhandle::forHandler(token.token), nullptr);
}

BatchClaim Producer::claimBatch(TopicHandle topic, const Key& key, uint32_t n, uint32_t recLen) noexcept {
    if (!impl_ || !topic.valid()) return BatchClaim(SendResult::InvalidTopic);
    const producer::ProducerImpl::ClaimGrant g =
        impl_->claimBatch(topic.topicId, key.partition(topic.partitionCount), key, n, recLen);
    if (g.result != SendResult::Ok) return BatchClaim(g.result);
    detail::ClaimOpen core{impl_, g.token, nullptr, nullptr, 0};
    if (Completions* bq = impl_->batchAckCq(); bq != nullptr) [[unlikely]] {
        const SendResult u = reserveBatchUnits(*bq, n);  // one unit per record, like n sends
        if (u != SendResult::Ok) {
            detail::ClaimCredit::abort(core);
            return BatchClaim(u);
        }
        core.bq    = bq;
        core.units = n;
    }
    return BatchClaim(core, g.base, g.offsets, n, recLen);
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
    return BatchBuilder(impl_, topic.topicId, part);  // borrows: must not outlive this Producer
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

// No wait bound of its own: the drain is bounded by deliveryTimeoutMs (the
// token completes Timeout then).  ~100 years keeps the deadline arithmetic
// (now + timeout) from overflowing.
OpStatus Producer::close() { return closeAndWait(std::chrono::hours(24 * 365 * 100)); }

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
    } ic{impl_};
    // Invoker owner: this thread is about to block inside the wait and cannot
    // commit a claim it holds - abandon it now (no-op for Thread mode / no claim).
    impl_->abandonOpenClaimForClose();
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

long Producer::testClaimRecord(TopicHandle topic, int32_t partition, const Key& key,
                               std::span<const uint8_t> value) noexcept {
    if (!impl_ || !topic.valid()) return -1;
    return static_cast<long>(impl_->testClaimSendRecord(topic.topicId, partition, key, value, 0));
}

void Producer::testCommitClaim(long claimIndex) noexcept {
    if (impl_ && claimIndex >= 0) impl_->testCommitClaim(static_cast<aeron::util::index_t>(claimIndex));
}

void Producer::testSetNextSequence(TopicHandle topic, int32_t partition, int32_t seq) noexcept {
    if (impl_) impl_->debugSetNextSequenceForTest(topic.topicId, partition, seq);
}

} // namespace rune::client
