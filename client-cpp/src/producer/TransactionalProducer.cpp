// src/producer/TransactionalProducer.cpp
//
// TX-5 / F8c C5 — TransactionalProducer: the app-side handle.  Every operation
// is a HubCommand for the duty thread (TransactionalProducerImpl::applyCommand);
// results are completion tokens.  The handle never touches duty-owned state.

#include <rune/client/producer/TransactionalProducer.h>
#include <rune/client/producer/TransactionalProducerImpl.h>

#include <memory>
#include <stdexcept>
#include <string>

namespace rune::client {

TransactionalProducer::TransactionalProducer(
    std::shared_ptr<producer::TransactionalProducerImpl> impl,
    detail::HubCommandSink sink, void* sinkCtx, std::thread::id dutyThread,
    detail::TargetedOpSink opSink, detail::AndWaitSink waitSink) noexcept
    : impl_(std::move(impl))
    , sink_(sink)
    , sinkCtx_(sinkCtx)
    , dutyThread_(dutyThread)
    , opSink_(opSink)
    , waitSink_(waitSink)
{}

bool TransactionalProducer::finished_() const noexcept {
    // Closed by the duty side (Client close) or abandoned (never registered):
    // the Client may be gone — never touch the sinks again.
    return !impl_ || impl_->status.closed();
}

OpResult TransactionalProducer::issue_(detail::HubCommand& cmd, const CompletionTarget& target,
                                       Token token, OpKind op) {
    if (finished_() || opSink_ == nullptr) return OpResult::Closed;
    cmd.handle = reinterpret_cast<uint64_t>(impl_.get());
    return opSink_(sinkCtx_, cmd, target, token, op);
}

OpResult TransactionalProducer::initTransactions(CompletionTarget target, Token token) {
    detail::HubCommand cmd;
    cmd.op = detail::HubCommand::Op::TxnInit;
    return issue_(cmd, target, token, OpKind::InitTransactions);
}

OpResult TransactionalProducer::sendOffsetsToTransaction(const std::map<TopicPartitionKey, int64_t>& offsets,
                                                         const std::string& groupId,
                                                         CompletionTarget target, Token token) {
    if (finished_() || opSink_ == nullptr) return OpResult::Closed;
    auto payload = std::make_unique<producer::TxnOffsetsPayload>();  // cold path
    payload->groupId = groupId;
    payload->offsets = offsets;
    detail::HubCommand cmd;
    cmd.op = detail::HubCommand::Op::TxnSendOffsets;
    cmd.d  = static_cast<int64_t>(reinterpret_cast<uintptr_t>(payload.get()));
    const OpResult r = issue_(cmd, target, token, OpKind::SendOffsets);
    if (r == OpResult::Accepted) (void)payload.release();  // the duty side owns it now
    return r;
}

OpResult TransactionalProducer::commitTransaction(CompletionTarget target, Token token) {
    detail::HubCommand cmd;
    cmd.op = detail::HubCommand::Op::TxnEnd;
    cmd.d  = 1;
    return issue_(cmd, target, token, OpKind::CommitTransaction);
}

OpResult TransactionalProducer::abortTransaction(CompletionTarget target, Token token) {
    detail::HubCommand cmd;
    cmd.op = detail::HubCommand::Op::TxnEnd;
    cmd.d  = 0;
    return issue_(cmd, target, token, OpKind::AbortTransaction);
}

// ---- *AndWait --------------------------------------------------------------

namespace {
struct OffsetsIssue {
    TransactionalProducer*                        self;
    const std::map<TopicPartitionKey, int64_t>*   offsets;
    const std::string*                            groupId;
};
} // namespace

void TransactionalProducer::andWait_(const char* what, std::chrono::nanoseconds timeout,
                                     detail::AndWaitIssue issue, void* ctx) {
    if (finished_() || waitSink_ == nullptr) throw std::runtime_error(std::string(what) + ": producer closed");
    std::string text;
    const Completion c = waitSink_(sinkCtx_, what, timeout, issue, ctx, &text);
    if (c.status == OpStatus::Ok) return;
    std::string msg(what);
    switch (c.status) {
    case OpStatus::Fenced:         msg += ": ProducerFenced — re-initialize"; break;
    case OpStatus::Timeout:        msg += ": timed out"; break;
    case OpStatus::Closed:         msg += ": client closed"; break;
    case OpStatus::DeliveryFailed: msg += ": a data batch was not durably acknowledged"; break;
    case OpStatus::BrokerError:    msg += ": broker error code " + std::to_string(c.errorCode); break;
    default: msg += ": failed (status " + std::to_string(static_cast<int>(c.status)) + ")"; break;
    }
    if (!text.empty()) msg += " (" + text + ")";
    throw std::runtime_error(msg);
}

void TransactionalProducer::initTransactionsAndWait(std::chrono::nanoseconds timeout) {
    andWait_("initTransactions()", timeout,
             [](void* s, const CompletionTarget& t, Token k) {
                 return static_cast<TransactionalProducer*>(s)->initTransactions(t, k);
             },
             this);
}

void TransactionalProducer::sendOffsetsToTransactionAndWait(const std::map<TopicPartitionKey, int64_t>& offsets,
                                                            const std::string& groupId,
                                                            std::chrono::nanoseconds timeout) {
    OffsetsIssue ctx{this, &offsets, &groupId};
    andWait_("sendOffsetsToTransaction()", timeout,
             [](void* c, const CompletionTarget& t, Token k) {
                 auto* o = static_cast<OffsetsIssue*>(c);
                 return o->self->sendOffsetsToTransaction(*o->offsets, *o->groupId, t, k);
             },
             &ctx);
}

void TransactionalProducer::commitTransactionAndWait(std::chrono::nanoseconds timeout) {
    andWait_("commitTransaction()", timeout,
             [](void* s, const CompletionTarget& t, Token k) {
                 return static_cast<TransactionalProducer*>(s)->commitTransaction(t, k);
             },
             this);
}

void TransactionalProducer::abortTransactionAndWait(std::chrono::nanoseconds timeout) {
    andWait_("abortTransaction()", timeout,
             [](void* s, const CompletionTarget& t, Token k) {
                 return static_cast<TransactionalProducer*>(s)->abortTransaction(t, k);
             },
             this);
}

// ---- No-completion commands ---------------------------------------------------

void TransactionalProducer::beginTransaction() {
    if (finished_() || sink_ == nullptr) return;
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::TxnBegin;
    cmd.handle = reinterpret_cast<uint64_t>(impl_.get());
    (void)sink_(sinkCtx_, cmd);  // Closed: nothing to begin any more
}

void TransactionalProducer::send(TopicHandle topic, const Key& key, std::span<const uint8_t> value) {
    // fix1 I8: never silently drop an invalid topic — report via throw.
    if (!topic.valid()) {
        throw std::invalid_argument("TransactionalProducer::send: invalid topic handle");
    }
    // fix5: never silently accept a send nothing will process.
    if (finished_() || sink_ == nullptr) throw std::logic_error("client closed");
    auto rec = std::make_unique<producer::TxnSendRecord>();
    rec->topicId   = topic.topicId;
    rec->partition = key.partition(topic.partitionCount);
    rec->key       = key;
    rec->value.assign(value.begin(), value.end());
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::TxnSend;
    cmd.handle = reinterpret_cast<uint64_t>(impl_.get());
    cmd.d      = static_cast<int64_t>(reinterpret_cast<uintptr_t>(rec.get()));
    switch (sink_(sinkCtx_, cmd)) {  // may throw (ring full): rec frees itself
    case detail::HubSubmit::Accepted:
        (void)rec.release();  // the duty side owns it now
        return;
    case detail::HubSubmit::Closed:
        break;
    }
    throw std::logic_error("client closed");
}

// ---- Accessors -----------------------------------------------------------------

int64_t TransactionalProducer::producerId() const noexcept {
    if (!impl_) return producer::TransactionalProducerImpl::kNoPid;
    if (onDutyThread_()) return impl_->producerIdDuty();
    (void)impl_->status.drainLatest(impl_->appStatusCache);
    return impl_->appStatusCache.producerId;
}

int16_t TransactionalProducer::producerEpoch() const noexcept {
    if (!impl_) return 0;
    if (onDutyThread_()) return impl_->producerEpochDuty();
    (void)impl_->status.drainLatest(impl_->appStatusCache);
    return impl_->appStatusCache.producerEpoch;
}

bool TransactionalProducer::transactionActive() const noexcept {
    if (!impl_) return false;
    if (onDutyThread_()) return impl_->transactionActiveDuty();
    (void)impl_->status.drainLatest(impl_->appStatusCache);
    return impl_->appStatusCache.txnActive != 0;
}

} // namespace rune::client
