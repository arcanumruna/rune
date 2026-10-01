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

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>

namespace rune::client::subscriber {

// ---------------------------------------------------------------------------
// registerSubscribe — app thread
// ---------------------------------------------------------------------------

void SubscriberHub::registerSubscribe(
        std::shared_ptr<SubscriptionImpl>             impl,
        int64_t                                       sessionId,
        int64_t                                       correlationId,
        int32_t                                       topicId,
        int32_t                                       partition,
        uint8_t                                       partitionKnown,
        int64_t                                       fromOffset,
        uint8_t                                       isolation,
        std::span<const uint8_t>                      keyBytes,
        std::chrono::steady_clock::time_point         deadline,
        const SendFn&                                  submitFn)
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
    // Store the encoded Subscribe frame alongside the impl so it can be
    // re-stamped + resent on a leader redirect (CB-5 B).
    {
        std::lock_guard<std::mutex> lk(mtx_);
        PendingSub ps;
        ps.impl     = impl;
        ps.deadline = deadline;
        if (frameLen > 0) {
            ps.subscribeFrame.assign(frameBuf, frameBuf + frameLen);
        }
        pendingByCorrelation_.emplace(correlationId, std::move(ps));
    }

    // ---- Submit to the agent ring -------------------------------------------
    // If submission fails (ring full), we leave the impl registered; it will
    // be cleaned up by deactivateAll() on close or by a timeout sweep.
    // Stream id 0 = default ingress stream; Task 1.3 will supply the correct
    // coordinator/session stream id here once per-session routing is wired.
    if (frameLen > 0) {
        submitFn(0, frameBuf, frameLen);
    }
}

// ---------------------------------------------------------------------------
// sendUnsubscribe — any thread
// ---------------------------------------------------------------------------

void SubscriberHub::sendUnsubscribe(
        int64_t subscriptionId,
        int64_t sessionId,
        int64_t correlationId,
        const SendFn& submitFn)
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
        submitFn(0, frameBuf, frameLen);
    }
}

// ---------------------------------------------------------------------------
// onSubscribeResponse — agent thread
// ---------------------------------------------------------------------------

void SubscriberHub::onSubscribeResponse(
        const codec::SubscribeResponseView& rsp,
        int64_t                             unsubCorrId,
        int64_t                             sessionId,
        const SendFn&                       submitFn)
{
    std::shared_ptr<SubscriptionImpl> impl;
    bool needUnsubscribe = false;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = pendingByCorrelation_.find(rsp.correlationId);
        if (it == pendingByCorrelation_.end()) {
            return; // unknown correlationId — race with close/unsubscribe; safe
        }
        impl = std::move(it->second.impl);
        pendingByCorrelation_.erase(it);

        // Store the broker-assigned subscriptionId regardless of active state,
        // so that any pending unsubscribe() on the app thread that already ran
        // (setting active=false before this response) can now find the id.
        impl->subscriptionId.store(rsp.subscriptionId, std::memory_order_release);

        if (!impl->active.load(std::memory_order_acquire)) {
            // CB-5 A2 FIX: unsubscribe() ran before SubscribeResponse arrived.
            // The earlier sendUnsubscribe() call sent subscriptionId=0 (no-op on
            // broker side).  Now that we know the real id, send a proper Unsubscribe.
            // Do NOT insert into activeById_ — the impl is already logically dead.
            needUnsubscribe = true;
        } else {
            // Move to the active routing table.
            activeById_.emplace(rsp.subscriptionId, impl);
        }
    }

    if (needUnsubscribe && submitFn) {
        // Encode + send an Unsubscribe with the now-known subscriptionId.
        uint8_t frameBuf[64]{};
        const std::size_t frameLen = codec::encodeUnsubscribe(
            frameBuf, sizeof(frameBuf),
            rsp.subscriptionId,
            sessionId,
            unsubCorrId);
        if (frameLen > 0) {
            submitFn(0, frameBuf, frameLen);
        }
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

        // Invoke the handler (agent thread; must not block). fix1 C1: this runs
        // inside the noexcept ClientAgent::doWork chain (Client::onEgressFrame ->
        // SubscriberHub::onRecords) — a throwing handler must never escape it.
        try {
            impl->handler(rec);
        } catch (const std::exception& e) {
            ++callbackExceptions_;
            std::fprintf(stderr,
                "rune-client: subscription record handler threw: %s (caught; the duty "
                "cycle continues)\n", e.what());
        } catch (...) {
            ++callbackExceptions_;
            std::fprintf(stderr,
                "rune-client: subscription record handler threw a non-std::exception "
                "(caught; the duty cycle continues)\n");
        }

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

    for (auto& [corrId, ps] : pendingByCorrelation_) {
        if (ps.impl) ps.impl->active.store(false, std::memory_order_release);
    }
    for (auto& [subId, impl] : activeById_) {
        impl->active.store(false, std::memory_order_release);
    }
    pendingByCorrelation_.clear();
    activeById_.clear();
}

// ---------------------------------------------------------------------------
// resendPending — agent thread (CB-5 B leader-redirect resend)
// ---------------------------------------------------------------------------

int SubscriberHub::resendPending(
        int64_t newSessionId,
        const SendFn& submitFn)
{
    // Re-stamp the MessageHeader sessionId (int64 LE at byte offset 8) of every
    // pending Subscribe frame with the leader's freshly assigned sessionId, then
    // resend.  Frames are kept pending (still awaiting SubscribeResponse from the
    // leader); we only mutate the stored bytes so a later redirect re-stamps
    // from the last-known frame.
    constexpr std::size_t kHeaderSessionIdOff = 8;

    int resent = 0;
    std::lock_guard<std::mutex> lk(mtx_);
    for (auto& [corrId, ps] : pendingByCorrelation_) {
        if (!ps.impl) continue;
        if (!ps.impl->active.load(std::memory_order_acquire)) continue; // unsubscribed
        auto& frame = ps.subscribeFrame;
        if (frame.size() < kHeaderSessionIdOff + 8) continue;

        const uint64_t u = static_cast<uint64_t>(newSessionId);
        for (int i = 0; i < 8; ++i)
            frame[kHeaderSessionIdOff + i] = static_cast<uint8_t>(u >> (i * 8));

        if (submitFn && submitFn(0, frame.data(), frame.size())) {
            ++resent;
        }
    }
    return resent;
}

bool SubscriberHub::hasPending() const
{
    std::lock_guard<std::mutex> lk(mtx_);
    return !pendingByCorrelation_.empty();
}

// ---------------------------------------------------------------------------
// M2 FIX: failPendingSubscribes — fail all unbound pending subscribes
// ---------------------------------------------------------------------------

void SubscriberHub::failPendingSubscribes(const std::string& reason)
{
    // Collect the impls to fail under the lock, then deactivate outside.
    std::vector<std::shared_ptr<SubscriptionImpl>> toFail;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        for (auto& [corrId, ps] : pendingByCorrelation_) {
            if (ps.impl) {
                // Mark inactive so a late SubscribeResponse / Records push is
                // treated as an already-unsubscribed response and dropped.
                ps.impl->active.store(false, std::memory_order_release);
                toFail.push_back(ps.impl);
            }
        }
        pendingByCorrelation_.clear();
    }
    // Note: SubscriptionImpl does not currently hold a promise / future — the
    // Subscription handle is the app's only reference.  Marking active=false
    // is the error signal; the app polls subscriptionId() / active.  This
    // mirrors the deactivateAll() semantics (no unsubscribe frame needed — the
    // broker never bound the id).  If a future per-subscribe promise is added
    // (Milestone-C), resolve it with an exception here.
    (void)reason;
}

// ---------------------------------------------------------------------------
// M2 FIX: sweepTimedOut — timeout pending subscribes that got no response
// ---------------------------------------------------------------------------

void SubscriberHub::sweepTimedOut(const std::chrono::steady_clock::time_point& now)
{
    std::lock_guard<std::mutex> lk(mtx_);
    for (auto it = pendingByCorrelation_.begin();
         it != pendingByCorrelation_.end(); )
    {
        if (now >= it->second.deadline) {
            if (it->second.impl) {
                it->second.impl->active.store(false, std::memory_order_release);
            }
            it = pendingByCorrelation_.erase(it);
        } else {
            ++it;
        }
    }
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
