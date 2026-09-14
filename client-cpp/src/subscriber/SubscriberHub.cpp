// src/subscriber/SubscriberHub.cpp
//
// CB-4 — SubscriberHub: subscription routing + record dispatch.
//
// Threading model recap (see header for full details):
//   mtx_ guards the routing tables on the CONTROL PLANE.
//   Record dispatch (hot path) acquires mtx_ only to look up and copy the
//   shared_ptr, then RELEASES the lock before calling the handler.
//   No lock is held during RecordBatchReader iteration or handler invocation.

#include <rune/client/subscriber/SubscriberHub.h>
#include <rune/protocol/RecordBatch.h>

#include <cstdint>
#include <cstring>

namespace rune::client::subscriber {

// ---------------------------------------------------------------------------
// registerSubscribe — app thread
// ---------------------------------------------------------------------------

void SubscriberHub::registerSubscribe(
        std::shared_ptr<SubscriptionImpl> impl,
        int64_t                          sessionId,
        int64_t                          correlationId,
        int32_t                          topicId,
        int32_t                          partition,
        uint8_t                          partitionKnown,
        int64_t                          fromOffset,
        uint8_t                          isolation,
        std::span<const uint8_t>         keyBytes,
        const std::function<bool(const uint8_t*, std::size_t)>& submitFn)
{
    // ---- Encode the Subscribe frame -----------------------------------------
    // Max frame size: header(24) + fixed(~30) + key(≤16) = ~70 bytes.
    // Use a stack buffer for zero-alloc encoding.
    uint8_t frameBuf[256]{};

    const std::size_t frameLen = codec::encodeSubscribe(
        frameBuf, sizeof(frameBuf),
        topicId,
        partition,
        partitionKnown,
        fromOffset,
        isolation,
        keyBytes,
        sessionId,
        correlationId);

    // ---- Register the impl under correlationId (control-plane lock) ---------
    {
        std::lock_guard<std::mutex> lk(mtx_);
        pendingByCorrelation_.emplace(correlationId, impl);
    }

    // ---- Submit to the agent ring -------------------------------------------
    // If submission fails (ring full), we leave the impl registered; it will
    // be cleaned up by deactivateAll() on close or by a timeout in the future.
    if (frameLen > 0) {
        submitFn(frameBuf, frameLen);
    }
}

// ---------------------------------------------------------------------------
// sendUnsubscribe — any thread
// ---------------------------------------------------------------------------

void SubscriberHub::sendUnsubscribe(
        int64_t subscriptionId,
        int64_t sessionId,
        int64_t correlationId,
        const std::function<bool(const uint8_t*, std::size_t)>& submitFn)
{
    // subscriptionId == 0 means the SubscribeResponse has not arrived yet.
    // Still encode and send — the broker will ignore an Unsubscribe for an
    // unknown id, which is harmless.

    // Remove from activeById_ under lock so the agent thread won't dispatch
    // further records for this subscription after unsubscribe() returns.
    {
        std::lock_guard<std::mutex> lk(mtx_);
        activeById_.erase(subscriptionId);
        // Also clean up from pending (if unsubscribed before SubscribeResponse).
        // We cannot easily correlate from subscriptionId back to correlationId
        // here — just leave the pending entry; it will be skipped on
        // onSubscribeResponse because active == false.
    }

    if (subscriptionId == 0) {
        return; // No valid id to unsubscribe on the broker side yet.
    }

    uint8_t frameBuf[64]{};
    const std::size_t frameLen = codec::encodeUnsubscribe(
        frameBuf, sizeof(frameBuf),
        subscriptionId,
        sessionId,
        correlationId);

    if (frameLen > 0) {
        submitFn(frameBuf, frameLen);
    }
}

// ---------------------------------------------------------------------------
// onSubscribeResponse — agent thread
// ---------------------------------------------------------------------------

void SubscriberHub::onSubscribeResponse(const codec::SubscribeResponseView& rsp)
{
    std::shared_ptr<SubscriptionImpl> impl;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = pendingByCorrelation_.find(rsp.correlationId);
        if (it == pendingByCorrelation_.end()) {
            return; // unknown correlationId — race with close/unsubscribe; safe
        }
        impl = std::move(it->second);
        pendingByCorrelation_.erase(it);

        if (!impl->active.load(std::memory_order_acquire)) {
            // Already unsubscribed before SubscribeResponse arrived; discard.
            return;
        }

        // Store the broker-assigned subscriptionId (atomic store; release
        // so that any thread reading subscriptionId() sees a consistent value).
        impl->subscriptionId.store(rsp.subscriptionId, std::memory_order_release);

        // Move to the active routing table.
        activeById_.emplace(rsp.subscriptionId, impl);
    }
}

// ---------------------------------------------------------------------------
// onRecords — agent thread
// ---------------------------------------------------------------------------

void SubscriberHub::onRecords(const codec::RecordsView& rv)
{
    // ---- Look up the subscription (acquire lock for the pointer copy only) --
    std::shared_ptr<SubscriptionImpl> impl;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = activeById_.find(rv.subscriptionId);
        if (it == activeById_.end()) {
            return; // unknown / already-removed subscription
        }
        impl = it->second; // copy the shared_ptr; will release lock immediately
    }

    // ---- Check active flag (no lock held) -----------------------------------
    if (!impl->active.load(std::memory_order_acquire)) {
        return; // unsubscribed after the look-up; drop
    }

    // ---- Decode the RecordBatch and dispatch per record ---------------------
    // RecordBatchReader iterates the batch in zero-copy fashion.
    // The spans are views into the Aeron fragment buffer, which is valid for
    // the duration of this call (i.e. during the RecordHandler callback).
    const auto& rb = rv.recordBatch;
    if (rb.empty()) return;

    rune::protocol::RecordBatchReader reader(rb.data(), rb.size());

    // Validate (CRC + bounds).
    if (!reader.ensureValid()) {
        // Malformed batch — drop (never crash).
        return;
    }

    // forEach calls fn(key, value, absoluteOffset, timestamp) per record.
    // key and value are string_views into the Aeron buffer — zero-copy.
    reader.forEach([&](std::string_view keyView,
                       std::string_view valView,
                       int64_t          absOffset,
                       int64_t          timestamp)
    {
        // Re-check active before each record (handler could theoretically
        // unsubscribe a sibling subscription, but not THIS one from within
        // the handler — defensive check anyway).
        if (!impl->active.load(std::memory_order_acquire)) return;

        Record rec;
        rec.offset    = absOffset;
        rec.timestamp = timestamp;
        rec.key   = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(keyView.data()), keyView.size());
        rec.value = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(valView.data()), valView.size());

        // Invoke the handler (agent thread; must not block).
        impl->handler(rec);

        // Update offset tracking: nextOffset = absOffset + 1.
        // Release store so app-thread reads via position() are consistent.
        impl->nextOffset.store(absOffset + 1, std::memory_order_release);
    });
}

// ---------------------------------------------------------------------------
// deactivateAll — called on close()
// ---------------------------------------------------------------------------

void SubscriberHub::deactivateAll()
{
    std::lock_guard<std::mutex> lk(mtx_);

    for (auto& [corrId, impl] : pendingByCorrelation_) {
        impl->active.store(false, std::memory_order_release);
    }
    for (auto& [subId, impl] : activeById_) {
        impl->active.store(false, std::memory_order_release);
    }
    pendingByCorrelation_.clear();
    activeById_.clear();
}

// ---------------------------------------------------------------------------
// Diagnostics
// ---------------------------------------------------------------------------

std::size_t SubscriberHub::pendingCount() const
{
    std::lock_guard<std::mutex> lk(mtx_);
    return pendingByCorrelation_.size();
}

std::size_t SubscriberHub::activeCount() const
{
    std::lock_guard<std::mutex> lk(mtx_);
    return activeById_.size();
}

} // namespace rune::client::subscriber
