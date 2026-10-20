// src/producer/BatchBuilder.cpp
//
// CB-3 — BatchBuilder: Tier-2 explicit atomic batch implementation.

#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/Completions.h>
#include <rune/client/producer/Producer.h>

#include <chrono>
#include <stdexcept>

namespace rune::client {

namespace {
OpResult tier2Result(producer::RejectReason why) noexcept {
    using producer::RejectReason;
    switch (why) {
    case RejectReason::None:          return OpResult::Accepted;
    case RejectReason::Closed:        return OpResult::Closed;
    case RejectReason::WrongThread:   return OpResult::WrongThread;
    case RejectReason::Unaddressable: return OpResult::InvalidArgument;
    default:                          return OpResult::BackPressured;  // ring / arena full, partition deferred
    }
}
} // namespace

namespace {

// Current wall-clock timestamp: epoch MILLISECONDS (the record-timestamp wire contract, Kafka parity — D7).
inline int64_t wallMs() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

BatchBuilder::BatchBuilder(producer::ProducerImpl* impl,
                           int32_t topicId,
                           int32_t partition) noexcept
    : impl_(impl)
    , topicId_(topicId)
    , partition_(partition)
{}

BatchBuilder::BatchBuilder(BatchBuilder&& o) noexcept
    : impl_(o.impl_), topicId_(o.topicId_), partition_(o.partition_), slot_(o.slot_), state_(o.state_)
{
    o.state_ = State::Sent;  // inert: the slot (if any) is ours now
}

BatchBuilder& BatchBuilder::operator=(BatchBuilder&& o) noexcept {
    if (this != &o) {
        releaseSlot_();
        impl_ = o.impl_; topicId_ = o.topicId_; partition_ = o.partition_;
        slot_ = o.slot_; state_ = o.state_;
        o.state_ = State::Sent;
    }
    return *this;
}

BatchBuilder::~BatchBuilder() { releaseSlot_(); }

void BatchBuilder::releaseSlot_() noexcept {
    if ((state_ == State::Open || state_ == State::Built) && impl_ != nullptr && !impl_->handleDetached()) {
        // Drop the records (the next user of the slot starts empty) and hand it back.
        producer::Tier2Slot& t = impl_->tier2Slot(slot_);
        t.builder.reset(0, 0);
        impl_->tier2Release(slot_);
    }
    state_ = State::Sent;
}

// ---------------------------------------------------------------------------
// add() - accumulate records
// ---------------------------------------------------------------------------

OpResult BatchBuilder::add(std::span<const uint8_t> value) {
    return add(value, std::string_view{});
}

OpResult BatchBuilder::add(std::span<const uint8_t> value,
                           std::string_view         record_key)
{
    if (state_ == State::Sent || state_ == State::Built) {
        throw std::logic_error("BatchBuilder: add after send");
    }
    // Backlog B6: the per-record limit, one O(1) compare at append.
    if (record_key.size() + value.size() > Producer::kMaxRecordBytes) {
        throw std::length_error("BatchBuilder::add: MessageTooLarge: key + value exceeds "
                                "Producer::kMaxRecordBytes (16 KiB)");
    }
    if (state_ == State::Idle || state_ == State::Starved) {
        const uint32_t s = impl_->tier2Acquire();
        if (s == producer::kNoTier2Slot) {
            state_ = State::Starved;
            return OpResult::BackPressured;  // every slot is in use: nothing was added
        }
        slot_  = s;
        state_ = State::Open;
    }
    producer::Tier2Slot& t = impl_->tier2Slot(slot_);

    // Slot capacity: the worst-case encoded record (10-byte length reserve + attrs + tsDelta +
    // offsetDelta + two length varints + header count = 37 bytes of framing) must still fit
    // with the batch header - checked BEFORE appending, so a refused record leaves the batch
    // intact and append() never grows the preallocated body.
    constexpr std::size_t kRecordFraming = 37;
    if (t.builder.bodyBytes() + kRecordFraming + record_key.size() + value.size() +
            static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) > impl_->tier2SlotBytes()) {
        throw std::length_error("BatchBuilder::add: MessageTooLarge: the batch would exceed "
                                "ProducerConfig::tier2SlotBytes");
    }
    // Reset (keeping capacity) on the first record: the batch timestamp base is taken here.
    if (t.builder.recordCount() == 0) t.builder.reset(0, wallMs());

    // Append record: record_key + value.
    const std::string_view vv(
        reinterpret_cast<const char*>(value.data()), value.size());

    t.builder.append(record_key, vv, wallMs());
    return OpResult::Accepted;
}

// ---------------------------------------------------------------------------
// send() - flush as one atomic RecordBatch
// ---------------------------------------------------------------------------

OpResult BatchBuilder::submit_(uint64_t ackHandle) {
    if (state_ == State::Sent) {
        throw std::logic_error("BatchBuilder: send called more than once");
    }
    if (state_ == State::Starved) return OpResult::BackPressured;  // add() found no free slot
    if (state_ == State::Idle) {
        throw std::invalid_argument(
            "BatchBuilder: empty batch - must add() at least one record before send()");
    }
    producer::Tier2Slot& t = impl_->tier2Slot(slot_);
    if (state_ == State::Open) {
        // Seal the RecordBatch bytes ONCE into the slot (kept across a rejected send for a
        // retry).  baseSequence / producerId / epoch are patched by the agent.
        t.recordCount = t.builder.recordCount();
        t.builder.buildInto(t.batchBytes);
        t.builder.reset(0, 0);
        state_ = State::Built;
    }
    // Submit: the slot INDEX travels through the ring (kMsgTier2Batch token), in
    // submission order with Tier-1 records.  The slot is the duty side's only when accepted.
    const producer::RejectReason why =
        impl_->submitTier2Batch(topicId_, partition_, ackHandle, t.recordCount, slot_);
    // Phase 2b F: a transactional producer outside a transaction.  OpResult has no such value;
    // like every other misuse of a Tier-2 batch it throws (the callers return their credit).
    if (why == producer::RejectReason::NotInTransaction) throw std::logic_error("produce outside a transaction");
    if (why == producer::RejectReason::None) {
        state_ = State::Sent;  // the duty side owns the slot now
    }
    return tier2Result(why);
}

namespace {
// The batch-ack credit unit of a batchAcksTo() producer (one per Tier-2 batch).
OpResult reserveUnit(Completions* bq) noexcept {
    if (bq == nullptr) return OpResult::Accepted;
    if (!bq->onOwnerThread()) return OpResult::WrongThread;
    return bq->reserve(producer::kAckRecordBytes);
}
} // namespace

// Review m3: a deliberate deviation from the spec's "debug check" — it runs in
// every build: one acquire load of a cache line written once per lifetime,
// per batch (cold relative to the per-record path).
void BatchBuilder::checkProducer_() const {
    // F8c C7 (spec §3.1 rule 1): a BatchBuilder borrows its producer and must
    // not outlive the Producer handle.  Caught while the duty side still holds
    // the impl (the handle's share is gone); after the impl is freed this read
    // itself is the bug (ASan reports it).
    if (impl_ == nullptr || impl_->handleDetached()) {
        throw std::logic_error("BatchBuilder: used after its Producer was destroyed");
    }
}

OpResult BatchBuilder::send() {
    checkProducer_();
    Completions* bq = impl_->batchAckCq();
    const OpResult u = reserveUnit(bq);
    if (u != OpResult::Accepted) return u;
    OpResult r = OpResult::InvalidArgument;
    try {
        r = submit_(0);
    } catch (...) {
        if (bq != nullptr) bq->unreserve(producer::kAckRecordBytes);
        throw;
    }
    if (bq != nullptr) {
        if (r == OpResult::Accepted) ++bq->producerIssued_;
        else bq->unreserve(producer::kAckRecordBytes);
    }
    return r;
}

OpResult BatchBuilder::send(Completions& cq, Token token) {
    checkProducer_();
    if (token > kMaxToken || cq.client_ == nullptr || cq.client_ != impl_->hooks().ctx ||
        cq.ring_ == nullptr) {
        return OpResult::InvalidArgument;
    }
    if (!cq.onOwnerThread()) return OpResult::WrongThread;
    const OpResult rr = cq.reserve(producer::kAckRecordBytes);
    if (rr != OpResult::Accepted) return rr;
    Completions* bq = impl_->batchAckCq();
    const OpResult u = reserveUnit(bq);
    if (u != OpResult::Accepted) {
        cq.unreserve(producer::kAckRecordBytes);
        return u;
    }
    OpResult r = OpResult::InvalidArgument;
    try {
        r = submit_(producer::ackhandle::forCq(cq.cqRef_, token));
    } catch (...) {
        cq.unreserve(producer::kAckRecordBytes);
        if (bq != nullptr) bq->unreserve(producer::kAckRecordBytes);
        throw;
    }
    if (r == OpResult::Accepted) {
        ++cq.producerIssued_;
        if (bq != nullptr) ++bq->producerIssued_;
    } else {
        cq.unreserve(producer::kAckRecordBytes);
        if (bq != nullptr) bq->unreserve(producer::kAckRecordBytes);
    }
    return r;
}

OpResult BatchBuilder::send(AckToken token) {
    checkProducer_();
    if (token.token > kMaxToken) return OpResult::InvalidArgument;
    Completions* bq = impl_->batchAckCq();
    const OpResult u = reserveUnit(bq);
    if (u != OpResult::Accepted) return u;
    OpResult r = OpResult::InvalidArgument;
    try {
        r = submit_(producer::ackhandle::forHandler(token.token));
    } catch (...) {
        if (bq != nullptr) bq->unreserve(producer::kAckRecordBytes);
        throw;
    }
    if (bq != nullptr) {
        if (r == OpResult::Accepted) ++bq->producerIssued_;
        else bq->unreserve(producer::kAckRecordBytes);
    }
    return r;
}

} // namespace rune::client
