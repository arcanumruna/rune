// src/subscriber/SubscriberHub.cpp
//
// CB-4 — SubscriberHub: subscription routing + record dispatch.
//
// F8a threading recap (see header for full details): every table and every
// agent-side SubscriptionImpl field is owned by the duty thread.  App threads
// reach the hub only through HubCommands drained from the agent ring
// (applyCommand), and learn duty-side state only through each subscription's
// StatusRing.  No mutex, no atomics.

#include <rune/client/subscriber/SubscriberHub.h>
#include <rune/protocol/RecordBatch.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>

namespace rune::client::subscriber {

// ---------------------------------------------------------------------------
// Status publication helpers — duty thread
// ---------------------------------------------------------------------------

void SubscriberHub::publishStatus_(SubscriptionImpl& impl)
{
    const SubscriptionStatus st{impl.subscriptionId, impl.nextOffset};
    impl.statusDirty = !impl.status.tryPublish(st) && !impl.status.closed();
}

void SubscriberHub::finish_(SubscriptionImpl& impl)
{
    impl.active = false;
    if (impl.status.closed()) return;
    // Final snapshot (uses the ring's reserve — always fits), THEN close: an
    // app thread that observes closed() and drains afterwards sees it.
    (void)impl.status.publishFinal(SubscriptionStatus{impl.subscriptionId, impl.nextOffset});
    impl.statusDirty = false;
    impl.status.close();
}

// ---------------------------------------------------------------------------
// makeSubscribeRequest — app thread, pure (no hub state)
// ---------------------------------------------------------------------------

std::unique_ptr<SubscribeRequest> SubscriberHub::makeSubscribeRequest(
        std::shared_ptr<SubscriptionImpl>     impl,
        int64_t                               sessionId,
        uint8_t                               partitionKnown,
        int64_t                               fromOffset,
        uint8_t                               isolation,
        std::span<const uint8_t>              keyBytes,
        std::chrono::steady_clock::time_point deadline)
{
    // Max frame size: header(24) + fixed(~30) + key(≤16) = ~70 bytes.
    uint8_t frameBuf[256]{};
    const std::size_t frameLen = codec::encodeSubscribe(
        frameBuf, sizeof(frameBuf),
        impl->topicId,
        impl->partition,
        partitionKnown,
        fromOffset,
        isolation,
        keyBytes,
        sessionId,
        impl->correlationId);

    auto req = std::make_unique<SubscribeRequest>();
    req->impl     = std::move(impl);
    req->deadline = deadline;
    if (frameLen > 0) req->subscribeFrame.assign(frameBuf, frameBuf + frameLen);
    return req;
}

// ---------------------------------------------------------------------------
// applyCommand / adoptSubscribe / unsubscribe — duty thread
// ---------------------------------------------------------------------------

void SubscriberHub::applyCommand(const detail::HubCommand& cmd,
                                 int64_t                   unsubCorrId,
                                 int64_t                   sessionId,
                                 const SendFn&             submitFn)
{
    switch (cmd.op) {
    case detail::HubCommand::Op::SubscribeAdd:
        adoptSubscribe(std::unique_ptr<SubscribeRequest>(
                           reinterpret_cast<SubscribeRequest*>(cmd.handle)),
                       submitFn);
        break;
    case detail::HubCommand::Op::Unsubscribe:
        unsubscribe(cmd.a, unsubCorrId, sessionId, submitFn);
        break;
    default:
        break;  // not a subscriber op
    }
}

void SubscriberHub::adoptSubscribe(std::unique_ptr<SubscribeRequest> req,
                                   const SendFn&                     submitFn)
{
    if (!req || !req->impl) return;
    SubscribeRequest& r = *req;
    const int64_t corrId = r.impl->correlationId;

    // The frame is sent from the request's own storage, which the map now owns
    // (no copy, no extra allocation on the duty thread).
    auto [it, inserted] = pendingByCorrelation_.emplace(corrId, std::move(req));
    if (!inserted) return;  // duplicate correlationId: impossible (unique counter)

    // Stream id 0 = default ingress stream.  A failed submit (ring full) leaves
    // the entry pending; the timeout sweep / close path finishes it.
    const auto& frame = it->second->subscribeFrame;
    if (!frame.empty() && submitFn) {
        (void)submitFn(0, frame.data(), frame.size());
    }
}

void SubscriberHub::unsubscribe(int64_t       subscribeCorrId,
                                int64_t       unsubCorrId,
                                int64_t       sessionId,
                                const SendFn& submitFn)
{
    // Still pending (SubscribeResponse not yet seen): keep the entry so the
    // late response runs the CB-5 A2 path (Unsubscribe with the real id), but
    // finish it for the app now.
    if (auto pit = pendingByCorrelation_.find(subscribeCorrId);
        pit != pendingByCorrelation_.end()) {
        if (pit->second->impl) finish_(*pit->second->impl);
        return;
    }

    // Bound: find it by its correlationId (cold path; a handful of entries).
    for (auto it = activeById_.begin(); it != activeById_.end(); ++it) {
        if (it->second->correlationId != subscribeCorrId) continue;
        const int64_t subId = it->first;
        finish_(*it->second);
        activeById_.erase(it);

        uint8_t frameBuf[64]{};
        const std::size_t frameLen = codec::encodeUnsubscribe(
            frameBuf, sizeof(frameBuf), subId, sessionId, unsubCorrId);
        if (frameLen > 0 && submitFn) {
            (void)submitFn(0, frameBuf, frameLen);
        }
        return;
    }
    // Unknown: already finished (timed out / failed / A2) — nothing to do.
}

// ---------------------------------------------------------------------------
// onSubscribeResponse — duty thread
// ---------------------------------------------------------------------------

void SubscriberHub::onSubscribeResponse(
        const codec::SubscribeResponseView& rsp,
        int64_t                             unsubCorrId,
        int64_t                             sessionId,
        const SendFn&                       submitFn)
{
    auto it = pendingByCorrelation_.find(rsp.correlationId);
    if (it == pendingByCorrelation_.end()) {
        return; // unknown correlationId — race with close/timeout; safe
    }
    std::shared_ptr<SubscriptionImpl> impl = std::move(it->second->impl);
    pendingByCorrelation_.erase(it);
    if (!impl) return;

    // Store the broker-assigned subscriptionId regardless of active state.
    impl->subscriptionId = rsp.subscriptionId;

    if (!impl->active) {
        // CB-5 A2 FIX: unsubscribe() ran before SubscribeResponse arrived.  Now
        // that we know the real id, send a proper Unsubscribe.  Do NOT insert
        // into activeById_ — the impl is already logically dead (its status
        // ring was closed by unsubscribe()).
        if (submitFn) {
            uint8_t frameBuf[64]{};
            const std::size_t frameLen = codec::encodeUnsubscribe(
                frameBuf, sizeof(frameBuf), rsp.subscriptionId, sessionId, unsubCorrId);
            if (frameLen > 0) {
                (void)submitFn(0, frameBuf, frameLen);
            }
        }
        return;
    }

    // Bind: lifecycle snapshot (uses the reserve — always fits).
    (void)impl->status.publishFinal(SubscriptionStatus{impl->subscriptionId, impl->nextOffset});
    impl->statusDirty = false;
    activeById_.emplace(rsp.subscriptionId, std::move(impl));
}

// ---------------------------------------------------------------------------
// onRecords — duty thread (hot path: map lookup + handler; one publish/frame)
// ---------------------------------------------------------------------------

void SubscriberHub::onRecords(const codec::RecordsView& rv)
{
    auto it = activeById_.find(rv.subscriptionId);
    if (it == activeById_.end()) {
        return; // unknown / already-removed subscription
    }
    SubscriptionImpl& impl = *it->second;
    if (!impl.active) return;

    // ---- Decode the RecordBatch and dispatch per record ---------------------
    // The spans are views into the Aeron fragment buffer, which is valid for
    // the duration of this call (i.e. during the RecordHandler callback).
    const auto& rb = rv.recordBatch;
    if (rb.empty()) return;

    rune::protocol::RecordBatchReader reader(rb.data(), rb.size());
    if (!reader.ensureValid()) {
        return; // Malformed batch — drop (never crash).
    }

    const int64_t before = impl.nextOffset;
    reader.forEach([&](std::string_view keyView,
                       std::string_view valView,
                       int64_t          absOffset,
                       int64_t          timestamp)
    {
        Record rec;
        rec.offset    = absOffset;
        rec.timestamp = timestamp;
        rec.key   = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(keyView.data()), keyView.size());
        rec.value = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(valView.data()), valView.size());

        // Invoke the handler (duty thread; must not block). fix1 C1: this runs
        // inside the noexcept ClientAgent::doWork chain — a throwing handler
        // must never escape it.
        try {
            impl.handler(rec);
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

        impl.nextOffset = absOffset + 1;
    });

    // One status snapshot per Records frame (not per record).
    if (impl.nextOffset != before || impl.statusDirty) publishStatus_(impl);
}

// ---------------------------------------------------------------------------
// deactivateAll — called on close()
// ---------------------------------------------------------------------------

void SubscriberHub::deactivateAll()
{
    for (auto& [corrId, req] : pendingByCorrelation_) {
        if (req && req->impl) finish_(*req->impl);
    }
    for (auto& [subId, impl] : activeById_) {
        finish_(*impl);
    }
    pendingByCorrelation_.clear();
    activeById_.clear();
}

// ---------------------------------------------------------------------------
// resendPending — duty thread (CB-5 B leader-redirect resend)
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
    for (auto& [corrId, req] : pendingByCorrelation_) {
        if (!req || !req->impl) continue;
        if (!req->impl->active) continue; // unsubscribed
        auto& frame = req->subscribeFrame;
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
    return !pendingByCorrelation_.empty();
}

// ---------------------------------------------------------------------------
// M2 FIX: failPendingSubscribes — fail all unbound pending subscribes
// ---------------------------------------------------------------------------

void SubscriberHub::failPendingSubscribes(const std::string& reason)
{
    // Finishing the impl (inactive + closed status ring) is the error signal:
    // the app's Subscription::active() turns false.  No unsubscribe frame is
    // needed — the broker never bound the id.
    for (auto& [corrId, req] : pendingByCorrelation_) {
        if (req && req->impl) finish_(*req->impl);
    }
    pendingByCorrelation_.clear();
    (void)reason;
}

// ---------------------------------------------------------------------------
// M2 FIX: sweepTimedOut — timeout pending subscribes that got no response
// ---------------------------------------------------------------------------

void SubscriberHub::sweepTimedOut(const std::chrono::steady_clock::time_point& now)
{
    for (auto it = pendingByCorrelation_.begin();
         it != pendingByCorrelation_.end(); )
    {
        if (now >= it->second->deadline) {
            if (it->second->impl) finish_(*it->second->impl);
            it = pendingByCorrelation_.erase(it);
        } else {
            ++it;
        }
    }

    // F8a: retry snapshots a near-full status ring refused earlier.
    for (auto& [subId, impl] : activeById_) {
        if (impl->statusDirty) publishStatus_(*impl);
    }
}

// ---------------------------------------------------------------------------
// Diagnostics (duty thread / quiescent tests)
// ---------------------------------------------------------------------------

std::size_t SubscriberHub::pendingCount() const
{
    return pendingByCorrelation_.size();
}

std::size_t SubscriberHub::activeCount() const
{
    return activeById_.size();
}

} // namespace rune::client::subscriber
