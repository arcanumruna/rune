// src/producer/BatchBuilder.cpp
//
// CB-3 — BatchBuilder: Tier-2 explicit atomic batch implementation.

#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/producer/ProducerImpl.h>

#include <chrono>
#include <stdexcept>

namespace rune::client {

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

BatchBuilder::BatchBuilder(std::shared_ptr<producer::ProducerImpl> impl,
                           int32_t topicId,
                           int32_t partition) noexcept
    : impl_(std::move(impl))
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
    if (sent_) {
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

std::future<int64_t> BatchBuilder::send() {
    if (sent_) {
        throw std::logic_error("BatchBuilder: send called more than once");
    }

    if (!builder_ || builder_->recordCount() == 0) {
        throw std::invalid_argument("BatchBuilder: empty batch — must add() at least one record before send()");
    }

    sent_ = true;

    // Register a future for the ack (Tier-2 always returns a future).
    auto [ackHandle, fut] = impl_->registerFuture();

    const int32_t recordCount = builder_->recordCount();

    // Build the RecordBatch bytes into a local vector.
    // NOTE: baseSequence is left as 0 here.  The agent thread will patch the
    // correct baseSequence (from PartitionState.nextBaseSequence) before
    // encoding the Produce wire frame.  Producer ID and epoch are also left 0
    // here; the agent patches them from the shared producerId_/producerEpoch_.
    std::vector<uint8_t> batchBytes;
    builder_->buildInto(batchBytes);
    builder_.reset(); // release memory; builder is single-use

    // Submit to ProducerImpl: the heap Tier2Entry's address travels through the
    // ring (kMsgTier2Batch token) so the agent processes it in submission order
    // relative to Tier-1 records on the same partition.
    const bool ok = impl_->submitTier2Batch(
        topicId_, partition_, ackHandle, recordCount, std::move(batchBytes));

    if (!ok) {
        // CB-5 A3: ring full — fail the specific promise immediately.
        // The ring token was never written so the agent will never see this
        // batch; the future would otherwise hang until produceTimeoutMs.
        // Fail it now so the caller gets an error without waiting.
        const char* specific = impl_->tier2RejectReason();  // Direct: partition deferred / unaddressable
        impl_->failUnsubmitted(ackHandle,
            specific != nullptr ? specific
            : impl_->lastRejectWasDeferredFull()
                ? "Tier-2 batch rejected — deferred buffer full (sent from inside the producer's "
                  "own duty-cycle callback) — batch dropped"
                : "Tier-2 batch ring full — batch dropped");
    }

    return std::move(fut);
}

} // namespace rune::client
