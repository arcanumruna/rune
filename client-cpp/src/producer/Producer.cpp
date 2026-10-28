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
// The batch-ack credit of a send / claim: `units` reservations of ONE unit each - the units a Tier-1 BatchAck
// returns per record.  All or none (#329: the single-unit form is the same body).
[[nodiscard]] SendResult reserveBatchUnits(Completions& bq, uint32_t units) noexcept {
    if (!bq.valid()) return SendResult::InvalidArgument;  // review m7: moved-from queue
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
[[nodiscard]] SendResult reserveBatchUnit(Completions& bq) noexcept { return reserveBatchUnits(bq, 1); }
} // namespace

// The one send body (T15b M3): the partition is computed by the caller, once.
SendResult Producer::trySendPart_(TopicHandle topic, int32_t part, const Key& key,
                                  std::span<const uint8_t> value) noexcept {
    producer::ProducerImpl* impl = impl_;
    Completions* bq = impl->batchAckCq();
    if (bq == nullptr) [[likely]] {
        return impl->trySubmit(topic.topicId, part, key, value, 0);
    }
    if (impl->gateRefusesBeforeCredit()) [[unlikely]] return SendResult::NotInTransaction;  // #339: before any credit
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
    if (impl_->gateRefusesBeforeCredit()) [[unlikely]] return SendResult::NotInTransaction;  // #339: before any credit
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
        if (impl_->gateRefusesBeforeCredit()) return SendResult::NotInTransaction;  // #339: before any credit
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


Claim Producer::claimPart_(TopicHandle topic, int32_t part, const Key& key, uint32_t valueLen,
                           uint64_t ackHandle, Completions* cq) noexcept {
    // Credit BEFORE the ring slot (#328): exactly a send's order, so with the ring and the batch-ack queue both
    // full a claim reports QueueFull like a send does - and a ring refusal below returns the credit.
    Completions* bq = impl_->batchAckCq();
    if (bq != nullptr) [[unlikely]] {
        SendResult u = impl_->gateRefusesBeforeCredit() ? SendResult::NotInTransaction : reserveBatchUnit(*bq);
        if (u != SendResult::Ok) {
            if (cq != nullptr) cq->unreserve(producer::kAckRecordBytes);
            return Claim(u);
        }
    }
    const producer::ProducerImpl::ClaimGrant g =
        impl_->claimRecord(topic.topicId, part, key, valueLen, ackHandle);
    if (g.result != SendResult::Ok) {
        if (bq != nullptr) bq->unreserve(producer::kAckRecordBytes);
        if (cq != nullptr) cq->unreserve(producer::kAckRecordBytes);  // nothing will ever complete
        return Claim(g.result);
    }
    return Claim(detail::ClaimOpen{impl_, g.token, cq, bq, bq != nullptr ? 1u : 0u}, g.base, valueLen);
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
    if (impl_->gateRefusesBeforeCredit()) [[unlikely]] return Claim(SendResult::NotInTransaction);  // #339
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

BatchClaim Producer::claimBatchPart_(TopicHandle topic, int32_t part, const Key& key, uint32_t n,
                                     uint32_t recLen, uint64_t ackHandle, Completions* cq) noexcept {
    // Credit before the ring slot, as claimPart_: n batch-ack units (one per record, like n sends), all or none.
    // The never-retryable size rules come first: an impossible claim is not QueueFull (polling cannot help) and
    // must not walk a reservation loop of n units.
    Completions* bq = impl_->batchAckCq();
    if (bq != nullptr) [[unlikely]] {
        if (const SendResult shape = impl_->claimBatchShape(key, n, recLen); shape != SendResult::Ok) {
            if (cq != nullptr) cq->unreserve(producer::kAckRecordBytes);
            return BatchClaim(shape);
        }
        SendResult u = impl_->gateRefusesBeforeCredit() ? SendResult::NotInTransaction : reserveBatchUnits(*bq, n);
        if (u != SendResult::Ok) {
            if (cq != nullptr) cq->unreserve(producer::kAckRecordBytes);
            return BatchClaim(u);
        }
    }
    const producer::ProducerImpl::ClaimGrant g = impl_->claimBatch(topic.topicId, part, key, n, recLen, ackHandle);
    if (g.result != SendResult::Ok) {
        if (bq != nullptr) {
            for (uint32_t i = 0; i < n; ++i) bq->unreserve(producer::kAckRecordBytes);
        }
        if (cq != nullptr) cq->unreserve(producer::kAckRecordBytes);  // nothing will ever complete
        return BatchClaim(g.result);
    }
    return BatchClaim(detail::ClaimOpen{impl_, g.token, cq, bq, bq != nullptr ? n : 0u}, g.base, g.offsets, n, recLen);
}

BatchClaim Producer::claimBatch(TopicHandle topic, const Key& key, uint32_t n, uint32_t recLen) noexcept {
    if (!impl_ || !topic.valid()) return BatchClaim(SendResult::InvalidTopic);
    return claimBatchPart_(topic, key.partition(topic.partitionCount), key, n, recLen, 0, nullptr);
}

BatchClaim Producer::claimBatch(TopicHandle topic, const Key& key, uint32_t n, uint32_t recLen, Completions& cq,
                                Token token) noexcept {
    if (!impl_ || !topic.valid()) return BatchClaim(SendResult::InvalidTopic);
    if (token > kMaxToken || cq.client_ == nullptr || cq.client_ != impl_->hooks().ctx || cq.ring_ == nullptr) {
        return BatchClaim(SendResult::InvalidArgument);
    }
    if (!cq.onOwnerThread()) return BatchClaim(SendResult::WrongThread);
    if (impl_->gateRefusesBeforeCredit()) [[unlikely]] return BatchClaim(SendResult::NotInTransaction);  // #339
    const OpResult rr = cq.reserve(producer::kAckRecordBytes);  // ONE completion for the whole batch
    if (rr != OpResult::Accepted) return BatchClaim(mapReserve(rr));
    return claimBatchPart_(topic, key.partition(topic.partitionCount), key, n, recLen,
                           producer::ackhandle::forCq(cq.cqRef_, token), &cq);
}

BatchClaim Producer::claimBatch(TopicHandle topic, const Key& key, uint32_t n, uint32_t recLen,
                                AckToken token) noexcept {
    if (!impl_ || !topic.valid()) return BatchClaim(SendResult::InvalidTopic);
    if (token.token > kMaxToken) return BatchClaim(SendResult::InvalidArgument);
    return claimBatchPart_(topic, key.partition(topic.partitionCount), key, n, recLen,
                           producer::ackhandle::forHandler(token.token), nullptr);
}

uint64_t Producer::claimsAbandoned() const noexcept { return impl_ != nullptr ? impl_->claimsAbandonedChecked() : 0u; }

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
// Phase 2b F: transactions (Task 1: the API surface and its refusals; Task 8: the lifecycle)
// ---------------------------------------------------------------------------

bool Producer::transactional() const noexcept {
    return impl_ != nullptr && !impl_->config().transactionalId.empty();
}

namespace {
[[noreturn]] void throwNoTransactionalId(const char* op) {
    throw std::logic_error(std::string(op) + ": producer has no transactionalId");
}
}  // namespace

// Token forms.  A plain producer is refused explicitly (InvalidArgument, no completion).  A transactional one
// issues a control op: Thread mode writes it to the producer ring (FIFO with the records before it, FD1),
// Invoker mode applies it at once on the owner thread (WrongThread elsewhere).  Accepted: exactly one completion.
OpResult Producer::issueTxn_(std::uint8_t kindByte, const CompletionTarget& target, Token token,
                             std::span<const OffsetEntry> offsets, std::string_view groupId,
                             int32_t generationId, std::string_view memberId) {
    if (!impl_ || !transactional()) return OpResult::InvalidArgument;
    using Kind = producer::TxnControl::Kind;
    const Kind kind = static_cast<Kind>(kindByte);
    producer::TxnControl c;
    c.kind    = kind;
    c.token   = token;
    c.offsets = offsets;
    c.groupId = groupId;
    c.generationId = generationId;   // #125
    c.memberId     = memberId;
    if (token > kMaxToken) return OpResult::InvalidArgument;
    Completions* q = target.cq;
    if (q == nullptr && target.handler.fn == nullptr) return OpResult::InvalidArgument;
    if (q != nullptr) {
        if (q->client_ == nullptr || q->client_ != impl_->hooks().ctx || q->ring_ == nullptr) return OpResult::InvalidArgument;
        if (!q->onOwnerThread()) return OpResult::WrongThread;
        // The widest txn completion carries error text; a flush's too.
        const uint32_t r  = q->reservationFor(OpKind::InitTransactions);
        const OpResult rr = q->reserve(r);
        if (rr != OpResult::Accepted) return rr;
        c.target.reservedBytes = r;
        c.target.cqRef         = q->cqRef_;
    } else {
        c.target.handler = target.handler;
    }
    const OpResult r = impl_->issueTxnControl(c);
    if (r != OpResult::Accepted && q != nullptr) q->unreserve(c.target.reservedBytes);   // nothing will complete
    // C6: a ring op is a producer op of this queue (like trySend(cq)): a release before its completion is
    // delivered keeps the registry slot quarantined until then.
    if (r == OpResult::Accepted && q != nullptr && impl_->txnOpsOnRing()) ++q->producerIssued_;
    return r;
}

OpResult Producer::initTransactions(const CompletionTarget& t, Token k) {
    return issueTxn_(static_cast<std::uint8_t>(producer::TxnControl::Kind::Init), t, k, {}, {});
}

OpResult Producer::sendOffsetsToTransaction(std::span<const OffsetEntry> offsets, std::string_view groupId,
                                            const CompletionTarget& t, Token k) {
    return issueTxn_(static_cast<std::uint8_t>(producer::TxnControl::Kind::SendOffsets), t, k, offsets, groupId);
}

OpResult Producer::sendOffsetsToTransaction(std::span<const OffsetEntry> offsets, const GroupMetadata& group,
                                            const CompletionTarget& t, Token k) {
    return issueTxn_(static_cast<std::uint8_t>(producer::TxnControl::Kind::SendOffsets), t, k, offsets, group.groupId,
                     group.generationId, group.memberId);
}

OpResult Producer::commitTransaction(const CompletionTarget& t, Token k) {
    return issueTxn_(static_cast<std::uint8_t>(producer::TxnControl::Kind::Commit), t, k, {}, {});
}

OpResult Producer::abortTransaction(const CompletionTarget& t, Token k) {
    return issueTxn_(static_cast<std::uint8_t>(producer::TxnControl::Kind::Abort), t, k, {}, {});
}

OpResult Producer::flush(const CompletionTarget& t, Token k) {
    return issueTxn_(static_cast<std::uint8_t>(producer::TxnControl::Kind::Flush), t, k, {}, {});
}

void Producer::beginTransaction() {
    if (!transactional()) throwNoTransactionalId("beginTransaction()");
    // A full ring is waited out like a blocking send (the duty cycle drains it); every other refusal throws.
    producer::ProducerImpl::BlockingWait w = impl_->beginBlockingWait();
    for (;;) {
        producer::TxnControl c;   // Begin: void, no target
        c.kind = producer::TxnControl::Kind::Begin;
        const OpResult r = impl_->issueTxnControl(c);
        if (r == OpResult::Accepted) return;
        if (r == OpResult::BackPressured) {
            impl_->blockingWaitStep(w);
            continue;
        }
        if (r == OpResult::WrongThread) {
            throw std::logic_error("beginTransaction(): an Invoker producer is used on its owner thread only");
        }
        throw std::logic_error("beginTransaction(): producer closed");
    }
}

namespace {
// Maps a completed op to the *AndWait contract: Ok returns, everything else throws with the outcome.
void throwForTxnCompletion(const char* what, const Completion& c, const std::string& text) {
    if (c.status == OpStatus::Ok) return;
    std::string msg(what);
    switch (c.status) {
    case OpStatus::Fenced:         msg += ": ProducerFenced - re-initialize"; break;
    case OpStatus::Timeout:        msg += ": timed out"; break;
    case OpStatus::Closed:         msg += ": producer closed"; break;
    case OpStatus::DeliveryFailed: msg += ": a data batch was not durably acknowledged"; break;
    case OpStatus::BrokerError:    msg += ": broker error code " + std::to_string(c.errorCode); break;
    default: msg += ": failed (status " + std::to_string(static_cast<int>(c.status)) + ")"; break;
    }
    if (!text.empty()) msg += " (" + text + ")";
    if (c.status == OpStatus::InvalidArgument) throw std::invalid_argument(msg);
    throw std::runtime_error(msg);
}
}  // namespace

void Producer::txnAndWait_(const char* what, std::chrono::nanoseconds timeout, std::uint8_t kind,
                           std::span<const OffsetEntry> offsets, std::string_view groupId,
                           int32_t generationId, std::string_view memberId) {
    if (!transactional()) throwNoTransactionalId(what);
    if (impl_->clientClosed()) throw std::runtime_error(std::string(what) + ": producer closed");
    const auto& hk = impl_->hooks();
    if (hk.andWait == nullptr) {
        throw std::logic_error(std::string(what) + ": no Client to wait on (use the token form)");
    }
    struct Ctx {
        Producer*                    self;
        std::uint8_t                 kind;
        std::span<const OffsetEntry> offsets;
        std::string_view             groupId;
        int32_t                      generationId;
        std::string_view             memberId;
    } ctx{this, kind, offsets, groupId, generationId, memberId};
    const detail::AndWaitIssue issue = [](void* x, const CompletionTarget& t, Token k) {
        auto* c = static_cast<Ctx*>(x);
        return c->self->issueTxn_(c->kind, t, k, c->offsets, c->groupId, c->generationId, c->memberId);
    };
    std::string      text;
    const Completion c = hk.andWait(hk.ctx, what, timeout, issue, &ctx, &text);
    throwForTxnCompletion(what, c, text);
}

void Producer::initTransactionsAndWait(std::chrono::nanoseconds timeout) {
    txnAndWait_("initTransactions()", timeout, static_cast<std::uint8_t>(producer::TxnControl::Kind::Init), {}, {});
}

void Producer::sendOffsetsToTransactionAndWait(std::span<const OffsetEntry> offsets, std::string_view groupId,
                                               std::chrono::nanoseconds timeout) {
    txnAndWait_("sendOffsetsToTransaction()", timeout, static_cast<std::uint8_t>(producer::TxnControl::Kind::SendOffsets), offsets, groupId);
}

void Producer::sendOffsetsToTransactionAndWait(std::span<const OffsetEntry> offsets, const GroupMetadata& group,
                                               std::chrono::nanoseconds timeout) {
    txnAndWait_("sendOffsetsToTransaction()", timeout, static_cast<std::uint8_t>(producer::TxnControl::Kind::SendOffsets),
                offsets, group.groupId, group.generationId, group.memberId);
}

void Producer::commitTransactionAndWait(std::chrono::nanoseconds timeout) {
    txnAndWait_("commitTransaction()", timeout, static_cast<std::uint8_t>(producer::TxnControl::Kind::Commit), {}, {});
}

void Producer::abortTransactionAndWait(std::chrono::nanoseconds timeout) {
    txnAndWait_("abortTransaction()", timeout, static_cast<std::uint8_t>(producer::TxnControl::Kind::Abort), {}, {});
}

void Producer::flushAndWait(std::chrono::nanoseconds timeout) {
    txnAndWait_("flush()", timeout, static_cast<std::uint8_t>(producer::TxnControl::Kind::Flush), {}, {});
}

// Status (FD9): the bound thread only (the creating thread, or bindBatchThread()'s); anywhere else "not available".
TxnState Producer::state() const noexcept { return impl_ != nullptr ? impl_->appTxnState() : TxnState::Uninitialized; }
int64_t  Producer::producerId() const noexcept { return impl_ != nullptr ? impl_->appProducerId() : -1; }
int16_t  Producer::producerEpoch() const noexcept { return impl_ != nullptr ? impl_->appProducerEpoch() : int16_t{-1}; }
bool     Producer::transactionActive() const noexcept { return impl_ != nullptr && impl_->appTxnActive(); }

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
    const detail::AndWaitIssue issue = [](void* c, const CompletionTarget& t, Token tok) {
        auto* x = static_cast<IssueCtx*>(c);
        x->last = x->impl->beginClose(t, tok);
        // Invoker owner: this thread now blocks inside the wait and cannot
        // commit a claim it holds - abandon it (no-op for Thread mode / no
        // claim).  Final review I1: only once the close is issued; a refused
        // close (a duty-callback caller, a closed intake) leaves it open.
        if (x->last == OpResult::Accepted) x->impl->abandonOpenClaimForClose();
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

uint64_t Producer::framesResent() const noexcept {
    return impl_ ? impl_->framesResent() : 0u;
}

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

namespace rune::client {
void Producer::bindBatchThread() {
    if (!impl_) throw std::logic_error("Producer::bindBatchThread: empty producer");
    impl_->bindBatchThread();
}
} // namespace rune::client
