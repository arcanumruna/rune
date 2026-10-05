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

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>

namespace rune::client::subscriber {

// ---------------------------------------------------------------------------
// Status publication helpers — duty thread
// ---------------------------------------------------------------------------

void SubscriberHub::markDirty_(SubscriptionImpl& impl) noexcept
{
    if (impl.statusDirty) return;
    impl.statusDirty = true;
    impl.prevDirty   = nullptr;
    impl.nextDirty   = dirtyHead_;
    if (dirtyHead_) dirtyHead_->prevDirty = &impl;
    dirtyHead_ = &impl;
    ++dirtyCount_;
}

void SubscriberHub::clearDirty_(SubscriptionImpl& impl) noexcept
{
    if (!impl.statusDirty) return;
    if (impl.prevDirty) impl.prevDirty->nextDirty = impl.nextDirty;
    else                dirtyHead_ = impl.nextDirty;
    if (impl.nextDirty) impl.nextDirty->prevDirty = impl.prevDirty;
    impl.prevDirty = impl.nextDirty = nullptr;
    impl.statusDirty = false;
    --dirtyCount_;
}

void SubscriberHub::publishStatus_(SubscriptionImpl& impl)
{
    if (impl.status.tryPublish(SubscriptionStatus{impl.subscriptionId, impl.nextOffset})) {
        clearDirty_(impl);
    } else if (!impl.status.closed()) {
        markDirty_(impl);
    }
}

int SubscriberHub::republishDirty()
{
    int published = 0;
    for (SubscriptionImpl* p = dirtyHead_; p != nullptr; ) {
        SubscriptionImpl* next = p->nextDirty;
        if (p->status.tryPublish(SubscriptionStatus{p->subscriptionId, p->nextOffset})) {
            clearDirty_(*p);
            ++published;
        } else if (p->status.closed()) {
            clearDirty_(*p);  // abandoned by the app: nobody will read it
        }
        p = next;
    }
    return published;
}

/*static*/
void SubscriberHub::closeStatus_(SubscriptionImpl& impl)
{
    if (impl.status.producerClosed()) return;
    // Final snapshot (uses the ring's reserve — always fits), THEN close: an
    // app thread that observes finished() and drains afterwards sees it.
    (void)impl.status.publishFinal(SubscriptionStatus{impl.subscriptionId, impl.nextOffset, impl.failReason});
    impl.status.close();
}

void SubscriberHub::finish_(SubscriptionImpl& impl)
{
    impl.active = false;
    clearDirty_(impl);
    if (&impl == dispatching_) return;  // closed by onRecords once the handler returns
    closeStatus_(impl);
}

// ---------------------------------------------------------------------------
// makeSubscribeRequest — app thread, pure (no hub state)
// ---------------------------------------------------------------------------

std::unique_ptr<SubscribeRequest> SubscriberHub::makeSubscribeRequest(
        SubscriptionImpl*                     impl,
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
    req->impl     = impl;
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
    case detail::HubCommand::Op::SubscribeAdd: {
        std::unique_ptr<SubscribeRequest> req(reinterpret_cast<SubscribeRequest*>(cmd.handle));
        // F8c C7: from here the duty side holds its share of the impl (even
        // if it was unsubscribed before this adoption) until release().
        if (req && req->impl) owned_.push_back(req->impl);
        // F8a fix2: stamp the frame with the CURRENT session (the app thread's
        // snapshot may be stale); a frame of a dead session is dropped by the
        // ring drain, and pending subscribes are re-sent on every connect.
        if (req && req->subscribeFrame.size() >= 16) {
            const auto u = static_cast<uint64_t>(sessionId);
            for (int i = 0; i < 8; ++i)
                req->subscribeFrame[8 + i] = static_cast<uint8_t>(u >> (i * 8));
        }
        adoptSubscribe(std::move(req), submitFn);
        break;
    }
    case detail::HubCommand::Op::Unsubscribe: {
        // F8c C3: identified by the impl address (handle) — resolved against
        // this hub's own tables only, never dereferenced unless found; the
        // correlationId form (a) is kept for hub-level callers.
        const int64_t corr = (cmd.handle != 0)
            ? correlationIdOf_(reinterpret_cast<const SubscriptionImpl*>(cmd.handle))
            : cmd.a;
        if (corr != 0) unsubscribe(corr, unsubCorrId, sessionId, submitFn);
        break;
    }
    default:
        break;  // not a subscriber op
    }
}

void SubscriberHub::adoptSubscribe(std::unique_ptr<SubscribeRequest> req,
                                   const SendFn&                     submitFn)
{
    if (!req || !req->impl) return;
    // fix3 I3: unsubscribed (directly, on this thread) before its adoption —
    // honour it: never register, never send the Subscribe.
    if (!req->impl->active) return;
    SubscribeRequest& r = *req;
    const int64_t corrId = r.impl->correlationId;

    // The frame is sent from the request's own storage, which the map now owns
    // (no copy, no extra allocation on the duty thread).
    auto [it, inserted] = pendingByCorrelation_.emplace(corrId, std::move(req));
    if (!inserted) return;  // duplicate correlationId: impossible (unique counter)

    SubscribeRequest& pr = *it->second;
    if (directSend_ == nullptr) {
        // Hub-level tests without a transport: the given SendFn.
        if (!pr.subscribeFrame.empty() && submitFn) (void)submitFn(0, pr.subscribeFrame.data(), pr.subscribeFrame.size());
        return;
    }
    // fix3 I4: straight to the transport (never the command ring).  While the
    // session is down the next ConnectResponse re-sends every pending one
    // (resendPending); a refused offer is retried every duty cycle.
    // fix4 I-1 (belt and braces): "down" also marks it UNSENT, so even if no
    // further ConnectResponse arrives the per-cycle retry offers it as soon
    // as the hub learns the session is connected — never silently dropped.
    if (!sessionConnected_) { markUnsent_(pr); return; }
    switch (sendSubscribeFrame_(pr)) {
    case detail::SendResult::Accepted: break;
    case detail::SendResult::Retry:    markUnsent_(pr); break;
    case detail::SendResult::Fatal:    failRequest_(it); break;  // fix4 M1: never re-offered
    }
}

detail::SendResult SubscriberHub::sendSubscribeFrame_(const SubscribeRequest& req)
{
    if (req.subscribeFrame.empty()) return detail::SendResult::Accepted;  // nothing to send
    assert(directSend_ != nullptr && "SubscriberHub: direct send not wired");
    if (directSend_ == nullptr) return detail::SendResult::Retry;  // never claim "sent"
    return directSend_(directSendCtx_, req.subscribeFrame.data(), req.subscribeFrame.size());
}

SubscriberHub::PendingMap::iterator SubscriberHub::failRequest_(PendingMap::iterator it)
{
    // fix4 M1: the transport can never carry this Subscribe — fail the
    // subscription (the handle sees finished()) and forget the request.
    clearUnsent_(*it->second);
    if (it->second->impl) finish_(*it->second->impl);
    ++subscribesFailed_;
    return pendingByCorrelation_.erase(it);
}

void SubscriberHub::markUnsent_(SubscribeRequest& req) noexcept
{
    if (req.unsent) return;
    req.unsent = true;
    ++unsentSubscribes_;
}

void SubscriberHub::clearUnsent_(SubscribeRequest& req) noexcept
{
    if (!req.unsent) return;
    req.unsent = false;
    --unsentSubscribes_;
}

int SubscriberHub::retryUnsentSubscribes()
{
    if (!sessionConnected_) return 0;  // the next connect re-sends every pending one
    int sent = 0;
    for (auto it = pendingByCorrelation_.begin();
         it != pendingByCorrelation_.end() && unsentSubscribes_ != 0; ) {
        SubscribeRequest& req = *it->second;
        if (!req.unsent) { ++it; continue; }
        if (!req.impl || !req.impl->active) { clearUnsent_(req); ++it; continue; }
        switch (sendSubscribeFrame_(req)) {
        case detail::SendResult::Accepted: clearUnsent_(req); ++sent; ++it; break;
        case detail::SendResult::Retry:    ++it; break;
        case detail::SendResult::Fatal:    it = failRequest_(it); break;
        }
    }
    return sent;
}

int64_t SubscriberHub::correlationIdOf_(const SubscriptionImpl* key) const noexcept
{
    for (const auto& [corr, req] : pendingByCorrelation_) {
        if (req->impl == key) return corr;
    }
    for (const auto& [id, impl] : activeById_) {
        if (impl == key) return impl->correlationId;
    }
    return 0;  // not (or no longer) held: nothing to unsubscribe
}

bool SubscriberHub::unsubscribe(int64_t       subscribeCorrId,
                                int64_t       unsubCorrId,
                                int64_t       sessionId,
                                const SendFn& submitFn)
{
    // Still pending (SubscribeResponse not yet seen): keep the entry so the
    // late response runs the CB-5 A2 path (Unsubscribe with the real id), but
    // finish it for the app now.
    if (auto pit = pendingByCorrelation_.find(subscribeCorrId);
        pit != pendingByCorrelation_.end()) {
        if (pit->second->impl) {
            pit->second->impl->unsubCorrId = unsubCorrId;  // used by the late Unsubscribe
            finish_(*pit->second->impl);
        }
        return true;
    }

    // Bound: find it by its correlationId (cold path; a handful of entries).
    for (auto it = activeById_.begin(); it != activeById_.end(); ++it) {
        if (it->second->correlationId != subscribeCorrId) continue;
        if (!it->second->active) return true;  // already unsubscribed (erase deferred / pending)
        SubscriptionImpl& impl = *it->second;
        const bool running = &impl == dispatching_;
        finish_(impl);
        // F8a fix2: the Unsubscribe goes straight to the transport from this
        // (duty) thread; if refused, the entry stays (active=false,
        // unsubPending) and the duty cycle retries it — never dropped.
        impl.unsubCorrId    = unsubCorrId;
        impl.unsubSessionId = sessionId;
        // fix4 M1: Fatal = never deliverable: finished locally, not retried.
        const bool sent = trySendUnsubscribe_(impl, submitFn) != detail::SendResult::Retry;
        if (!sent) markUnsubPending_(impl);
        // Its handler is running (it unsubscribed itself): onRecords erases the
        // entry after the handler returns — erasing now could free the
        // std::function mid-call.
        if (sent && !running) activeById_.erase(it);
        return true;
    }
    // Unknown: not adopted yet, or already finished (timed out / failed / A2).
    return false;
}

void SubscriberHub::unsubscribeDirect(SubscriptionImpl& impl, int64_t unsubCorrId,
                                      int64_t sessionId, const SendFn& submitFn)
{
    // fix3 I3: act on the impl itself.  Not (yet) in the hub: its SubscribeAdd
    // is still queued in the ring — finish it now; adoptSubscribe() honours it.
    if (!unsubscribe(impl.correlationId, unsubCorrId, sessionId, submitFn) && impl.active) {
        finish_(impl);
    }
}

detail::SendResult SubscriberHub::trySendUnsubscribe_(const SubscriptionImpl& impl,
                                                      const SendFn&           submitFn)
{
    uint8_t frameBuf[64]{};
    const std::size_t frameLen = codec::encodeUnsubscribe(
        frameBuf, sizeof(frameBuf), impl.subscriptionId, impl.unsubSessionId, impl.unsubCorrId);
    if (frameLen == 0) return detail::SendResult::Accepted;  // cannot encode: nothing to retry
    if (directSend_ != nullptr) return directSend_(directSendCtx_, frameBuf, frameLen);
    return (!submitFn || submitFn(0, frameBuf, frameLen)) ? detail::SendResult::Accepted
                                                          : detail::SendResult::Retry;
}

detail::SendResult SubscriberHub::retrySendUnsubscribe_(const SubscriptionImpl& impl)
{
    assert(directSend_ != nullptr && "SubscriberHub: direct send not wired");
    if (directSend_ == nullptr) return detail::SendResult::Retry;  // never claim "sent"
    uint8_t frameBuf[64]{};
    const std::size_t frameLen = codec::encodeUnsubscribe(
        frameBuf, sizeof(frameBuf), impl.subscriptionId, impl.unsubSessionId, impl.unsubCorrId);
    if (frameLen == 0) return detail::SendResult::Accepted;
    return directSend_(directSendCtx_, frameBuf, frameLen);
}

void SubscriberHub::markUnsubPending_(SubscriptionImpl& impl) noexcept
{
    if (impl.unsubPending) return;
    impl.unsubPending = true;
    ++pendingUnsubCount_;
}

int SubscriberHub::retryPendingUnsubscribes(int64_t currentSessionId)
{
    int sent = 0;
    for (auto it = activeById_.begin(); it != activeById_.end() && pendingUnsubCount_ != 0; ) {
        SubscriptionImpl& impl = *it->second;
        if (!impl.unsubPending || &impl == dispatching_) { ++it; continue; }
        if (impl.unsubSessionId != currentSessionId) {
            // Its session is gone: the broker dropped the subscription with it.
            ++unsubscribesDropped_;
            std::fprintf(stderr,
                "rune-client: pending Unsubscribe for subscription %lld dropped: session %lld "
                "no longer exists\n",
                static_cast<long long>(impl.subscriptionId),
                static_cast<long long>(impl.unsubSessionId));
        } else {
            const detail::SendResult r = retrySendUnsubscribe_(impl);
            if (r == detail::SendResult::Retry) { ++it; continue; }
            if (r == detail::SendResult::Accepted) ++sent;
            // Fatal (fix4 M1): never deliverable — finished locally, dropped.
        }
        impl.unsubPending = false;
        --pendingUnsubCount_;
        it = activeById_.erase(it);
    }
    return sent;
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
    // fix3: a response of a session that is no longer current must not bind
    // (its subscription died with that session; binding would leak it).  The
    // pending entry stays and is re-sent on the current session.
    if (rsp.sessionId != sessionId) {
        ++staleResponsesIgnored_;
        return;
    }
    auto it = pendingByCorrelation_.find(rsp.correlationId);
    if (it == pendingByCorrelation_.end()) {
        return; // unknown correlationId — race with close/timeout; safe
    }
    clearUnsent_(*it->second);
    SubscriptionImpl* impl = it->second->impl;
    pendingByCorrelation_.erase(it);
    if (!impl) return;

    // Store the broker-assigned subscriptionId regardless of active state.
    impl->subscriptionId = rsp.subscriptionId;
    impl->boundPartition = rsp.partition;

    if (!impl->active) {
        // CB-5 A2 FIX: unsubscribe() ran before SubscribeResponse arrived.  Now
        // that we know the real id, send a proper Unsubscribe (direct, with
        // duty-cycle retry: an inactive entry is kept only while it is pending).
        if (impl->unsubCorrId == 0) impl->unsubCorrId = unsubCorrId;
        impl->unsubSessionId = sessionId;
        if (trySendUnsubscribe_(*impl, submitFn) == detail::SendResult::Retry) {  // Fatal: dropped
            markUnsubPending_(*impl);
            activeById_.emplace(rsp.subscriptionId, impl);  // cold path
        }
        return;
    }

    // Bind: lifecycle snapshot (uses the reserve — always fits).
    (void)impl->status.publishFinal(SubscriptionStatus{impl->subscriptionId, impl->nextOffset, impl->failReason});
    activeById_.emplace(rsp.subscriptionId, impl);
}

// ---------------------------------------------------------------------------
// onRecords — duty thread (hot path: map lookup + handler; one publish/frame)
// ---------------------------------------------------------------------------

void SubscriberHub::onRecords(const codec::RecordsView& rv, int64_t currentSessionId)
{
    auto it = activeById_.find(rv.subscriptionId);
    if (it == activeById_.end()) {
        // CG round 3 (MAJOR-1): a broker stamps every push with the
        // correlationId of the Subscribe that created it — the first Records
        // frame binds a still-pending subscription (its SubscribeResponse may
        // arrive later, on another publication).  Cold: once per subscription.
        if (rv.correlationId == 0) return;  // unknown / already-removed subscription
        const auto p = pendingByCorrelation_.find(rv.correlationId);
        if (p == pendingByCorrelation_.end() || p->second->impl == nullptr) return;
        SubscriptionImpl* pi = p->second->impl;
        if (!pi->active || rv.topicId != pi->topicId) return;
        if (currentSessionId != 0 && rv.sessionId != currentSessionId) return;  // another session's push
        clearUnsent_(*p->second);
        pendingByCorrelation_.erase(p);
        pi->subscriptionId = rv.subscriptionId;
        pi->boundPartition = rv.partition;
        (void)pi->status.publishFinal(SubscriptionStatus{pi->subscriptionId, pi->nextOffset, pi->failReason});
        it = activeById_.emplace(rv.subscriptionId, pi).first;
    }
    SubscriptionImpl& impl = *it->second;
    if (!impl.active) return;
    // Exact instance: the push names the Subscribe that created it.
    if (rv.correlationId != 0 && rv.correlationId != impl.correlationId) return;
    // Only its own topic / partition (a belt-and-braces compare on a branch
    // already taken: subscription ids are cluster-unique).
    if (rv.topicId != impl.topicId || (impl.boundPartition >= 0 && rv.partition != impl.boundPartition)) return;

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
    dispatching_ = &impl;
    reader.forEach([&](std::string_view keyView,
                       std::string_view valView,
                       int64_t          absOffset,
                       int64_t          timestamp)
    {
        // The handler may have unsubscribed this subscription (directly, on
        // this thread): deliver nothing more of the batch.
        if (!impl.active) return;

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
    dispatching_ = nullptr;

    if (!impl.active) {
        // Finished from inside its own handler: now that no handler is running,
        // publish the final snapshot, close the ring, and drop the entry (this
        // may free impl — nothing below touches it).
        closeStatus_(impl);
        if (!impl.unsubPending) activeById_.erase(it);  // pending: the retry erases it
        return;
    }
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
    pendingUnsubCount_ = 0;
    unsentSubscribes_  = 0;  // their sessions end with the Client
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
    for (auto it = pendingByCorrelation_.begin(); it != pendingByCorrelation_.end(); ) {
        auto& req = it->second;
        if (!req || !req->impl || !req->impl->active) { ++it; continue; }  // unsubscribed
        auto& frame = req->subscribeFrame;
        if (frame.size() < kHeaderSessionIdOff + 8) { ++it; continue; }

        const uint64_t u = static_cast<uint64_t>(newSessionId);
        for (int i = 0; i < 8; ++i)
            frame[kHeaderSessionIdOff + i] = static_cast<uint8_t>(u >> (i * 8));

        if (directSend_ != nullptr) {
            // fix3 I4: direct, with per-cycle retry on refusal; fix4 M1: a
            // Fatal one fails its subscription (never re-offered).
            switch (sendSubscribeFrame_(*req)) {
            case detail::SendResult::Accepted: clearUnsent_(*req); ++resent; break;
            case detail::SendResult::Retry:    markUnsent_(*req); break;
            case detail::SendResult::Fatal:    it = failRequest_(it); continue;
            }
        } else if (submitFn && submitFn(0, frame.data(), frame.size())) {
            ++resent;
        }
        ++it;
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

bool SubscriberHub::failPendingSubscribe(int64_t correlationId, SubscriptionFailReason reason)
{
    const auto it = pendingByCorrelation_.find(correlationId);
    if (it == pendingByCorrelation_.end()) return false;
    clearUnsent_(*it->second);
    if (it->second->impl) {
        it->second->impl->failReason = reason;
        finish_(*it->second->impl);
    }
    pendingByCorrelation_.erase(it);
    return true;
}

void SubscriberHub::failPendingSubscribes(const std::string& reason)
{
    // Finishing the impl (inactive + closed status ring) is the error signal:
    // the app's Subscription::active() turns false.  No unsubscribe frame is
    // needed — the broker never bound the id.
    for (auto& [corrId, req] : pendingByCorrelation_) {
        if (req && req->impl) finish_(*req->impl);
    }
    pendingByCorrelation_.clear();
    unsentSubscribes_ = 0;
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
            clearUnsent_(*it->second);
            it = pendingByCorrelation_.erase(it);
        } else {
            ++it;
        }
    }
}

// ---------------------------------------------------------------------------
// F8c C7: release — forget an impl its handle released, once unreferenced
// ---------------------------------------------------------------------------

bool SubscriberHub::release(SubscriptionImpl* impl) noexcept
{
    // A running handler of it (its own record handler destroyed the handle),
    // a dirty-list link, a pending request (the CB-5 A2 late-response path) or
    // a routing entry (bound, or an Unsubscribe still being retried) all keep
    // referencing it: not yet.
    if (impl == dispatching_ || impl->statusDirty) return false;
    for (const auto& [corr, req] : pendingByCorrelation_) {
        if (req->impl == impl) return false;
    }
    for (const auto& [id, p] : activeById_) {
        if (p == impl) return false;
    }
    for (std::size_t i = 0; i < owned_.size(); ++i) {
        if (owned_[i] == impl) {
            owned_[i] = owned_.back();
            owned_.pop_back();
            break;
        }
    }
    return true;
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
