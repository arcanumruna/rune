// src/producer/BatchBuilder.cpp
//
// CB-3 — BatchBuilder: Tier-2 explicit atomic batch implementation.

#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/Completions.h>

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

// Current wall-clock timestamp (ns) for batch baseTimestamp.
inline int64_t wallNs() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
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
    , sent_(false)
{}

// ---------------------------------------------------------------------------
// add() — accumulate records
// ---------------------------------------------------------------------------

void BatchBuilder::add(std::span<const uint8_t> value) {
    add(value, std::string_view{});
}

void BatchBuilder::add(std::span<const uint8_t> value,
                       std::string_view         record_key)
{
    if (sent_ || !batchBytes_.empty()) {
        throw std::logic_error("BatchBuilder: add after send");
    }

    // Lazy-init builder on first add.
    if (!builder_) {
        builder_ = std::make_unique<rune::protocol::RecordBatchBuilder>(
            0, wallNs());
    }

    // Append record: record_key + value.
    const std::string_view vv(
        reinterpret_cast<const char*>(value.data()), value.size());

    builder_->append(record_key, vv, wallNs());
}

// ---------------------------------------------------------------------------
// send() — flush as one atomic RecordBatch
// ---------------------------------------------------------------------------

OpResult BatchBuilder::submit_(uint64_t ackHandle) {
    if (sent_) {
        throw std::logic_error("BatchBuilder: send called more than once");
    }
    if (batchBytes_.empty()) {
        if (!builder_ || builder_->recordCount() == 0) {
            throw std::invalid_argument(
                "BatchBuilder: empty batch — must add() at least one record before send()");
        }
        // Build the RecordBatch bytes ONCE (kept across a rejected send for a
        // retry).  baseSequence / producerId / epoch are patched by the agent.
        recordCount_ = builder_->recordCount();
        builder_->buildInto(batchBytes_);
        builder_.reset();  // single-use
    }
    // Submit: the heap Tier2Entry's address travels through the ring
    // (kMsgTier2Batch token), in submission order with Tier-1 records.  The
    // bytes are moved only when accepted.
    const producer::RejectReason why =
        impl_->submitTier2Batch(topicId_, partition_, ackHandle, recordCount_, batchBytes_);
    if (why == producer::RejectReason::None) {
        sent_ = true;
        batchBytes_.clear();
        batchBytes_.shrink_to_fit();
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
