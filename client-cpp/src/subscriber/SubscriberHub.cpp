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
#include <rune/client/group/GroupConsumerHub.h>  // isFetchCorrId (header-only constexpr)
#include <rune/client/TopicHandle.h>            // isEventLogTopic
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>  // kCatalogTopicId

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
    if (impl.status.tryPublish(
            SubscriptionStatus{impl.subscriptionId, impl.nextOffset, SubscriptionFailReason::None, 0, impl.dataResets, impl.batchBytes})) {
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
        if (p->status.tryPublish(
                SubscriptionStatus{p->subscriptionId, p->nextOffset, SubscriptionFailReason::None, 0, p->dataResets, p->batchBytes})) {
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
    (void)impl.status.publishFinal(
        SubscriptionStatus{impl.subscriptionId, impl.nextOffset, impl.failReason, impl.brokerErrorCode,
                           impl.dataResets, impl.batchBytes});
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
    // Max frame size: header(24) + fixed(~30) + key(≤16) + the optional filter trailer.
    uint8_t frameBuf[codec::kMaxSubscribeFrameBytes];
    const std::size_t frameLen = codec::encodeSubscribe(
        frameBuf, sizeof(frameBuf),
        impl->topicId,
        impl->partition,
        partitionKnown,
        fromOffset,
        isolation,
        keyBytes,
        sessionId,
        impl->correlationId,
        impl->filterSpan());

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

bool SubscriberHub::brokerTooOldForFilter_(SubscriptionImpl& impl) noexcept
{
    if (!impl.filtered() || brokerVersion_ == nullptr) return false;
    // 0 = that session is not connected yet: the frame stays pending, as today.
    const int32_t v = brokerVersion_(brokerVersionCtx_, impl.homeSession);
    if (v < 1 || v >= rune::protocol::client::kBrokerVersionFilter) return false;
    impl.failReason = SubscriptionFailReason::BrokerTooOld;   // before the finish: the final snapshot carries it
    return true;
}

detail::SendResult SubscriberHub::sendSubscribeFrame_(const SubscribeRequest& req)
{
    if (req.subscribeFrame.empty()) return detail::SendResult::Accepted;  // nothing to send
    // Phase 2c (SD3): a broker below kBrokerVersionFilter would ignore the filter and deliver everything — send
    // nothing; Fatal makes the caller finish the subscription (failReason BrokerTooOld) and forget the request.
    if (req.impl != nullptr && brokerTooOldForFilter_(*req.impl)) [[unlikely]] return detail::SendResult::Fatal;
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
        impl.unsubSessionId = homeSidOf_(impl, sessionId);   // I2: a moved one's own node session
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
    if (impl.homeSession != 0 && routed_())   // I2: on its own node session
        return router_.offer(router_.ctx, impl.homeSession, frameBuf, frameLen);
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
    if (impl.homeSession != 0 && routed_())   // I2: on its own node session
        return router_.offer(router_.ctx, impl.homeSession, frameBuf, frameLen);
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
        const int64_t homeNow = (impl.homeSession != 0 && routed_())
                                    ? router_.sessionId(router_.ctx, impl.homeSession)
                                    : currentSessionId;
        if (impl.unsubSessionId != homeNow) {
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
    auto it = pendingByCorrelation_.find(rsp.correlationId);
    if (it == pendingByCorrelation_.end()) {
        if (rsp.sessionId != sessionId) ++staleResponsesIgnored_;
        return; // unknown correlationId — race with close/timeout; safe
    }
    // I2: a moved subscription's Subscribe went out on its own node session.
    const int64_t expectSid = it->second->impl ? homeSidOf_(*it->second->impl, sessionId) : sessionId;
    if (rsp.sessionId != expectSid || expectSid == 0) {
        ++staleResponsesIgnored_;
        return;
    }
    clearUnsent_(*it->second);
    SubscriptionImpl* impl = it->second->impl;
    pendingByCorrelation_.erase(it);
    if (!impl) return;

    // No-hang wave: an error answer binds NOTHING broker-side.  It used to be
    // bound like an OK (subscriptionId 0, active, never delivering — a silent
    // hang).  Finish it with the broker's code; no Unsubscribe is owed.
    if (rsp.errorCode != 0) [[unlikely]] {
        // Authentication: a refused / revoked Subscribe is named, not a generic broker rejection.
        impl->failReason =
            rsp.errorCode == static_cast<int16_t>(rune::protocol::client::ErrorCode::NotAuthorized)
                ? SubscriptionFailReason::NotAuthorized
            : rsp.errorCode == static_cast<int16_t>(rune::protocol::client::ErrorCode::SessionRevoked)
                ? SubscriptionFailReason::SessionRevoked
                : SubscriptionFailReason::RejectedByBroker;
        impl->brokerErrorCode = static_cast<uint16_t>(rsp.errorCode);
        finish_(*impl);
        return;
    }

    // Store the broker-assigned subscriptionId regardless of active state.
    impl->subscriptionId = rsp.subscriptionId;
    impl->boundPartition = rsp.partition;

    if (!impl->active) {
        // CB-5 A2 FIX: unsubscribe() ran before SubscribeResponse arrived.  Now
        // that we know the real id, send a proper Unsubscribe (direct, with
        // duty-cycle retry: an inactive entry is kept only while it is pending).
        if (impl->unsubCorrId == 0) impl->unsubCorrId = unsubCorrId;
        impl->unsubSessionId = expectSid;
        if (trySendUnsubscribe_(*impl, submitFn) == detail::SendResult::Retry) {  // Fatal: dropped
            markUnsubPending_(*impl);
            activeById_.emplace(rsp.subscriptionId, impl);  // cold path
        }
        return;
    }

    // Bind: lifecycle snapshot (uses the reserve — always fits).
    (void)impl->status.publishFinal(
        SubscriptionStatus{impl->subscriptionId, impl->nextOffset, impl->failReason, 0, impl->dataResets, impl->batchBytes});
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
        // LOW-1 (round 4): a group fetch push (bit-61 correlationId) is never a
        // plain subscription's — one compare instead of a map lookup per batch.
        if (rv.correlationId == 0 || group::GroupConsumerHub::isFetchCorrId(rv.correlationId)) return;
        // A-MEDIUM-1 (round 4): no session yet (a fresh connect) = no binding by
        // push: another client's push on a shared publication could carry the
        // same small correlationId.  The SubscribeResponse still binds.
        const auto p = pendingByCorrelation_.find(rv.correlationId);
        if (p == pendingByCorrelation_.end() || p->second->impl == nullptr) return;
        SubscriptionImpl* pi = p->second->impl;
        const int64_t expectSid = homeSidOf_(*pi, currentSessionId);   // I2: a moved one's node session
        if (expectSid == 0 || rv.sessionId != expectSid) return;
        if (!pi->active || rv.topicId != pi->topicId) return;
        clearUnsent_(*p->second);
        pendingByCorrelation_.erase(p);
        pi->subscriptionId = rv.subscriptionId;
        pi->boundPartition = rv.partition;
        (void)pi->status.publishFinal(
            SubscriptionStatus{pi->subscriptionId, pi->nextOffset, pi->failReason, 0, pi->dataResets, pi->batchBytes});
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
    if (rb.size() == 8) [[unlikely]] {
        uint64_t l = 0;
        for (int i = 0; i < 8; ++i) l |= static_cast<uint64_t>(rb[static_cast<std::size_t>(i)]) << (8 * i);
        if (isEventLogTopic(impl.topicId)) {
            // Catalog history (client version 8, Task 13 fix round 2): the stream's
            // HISTORY-EPOCH NOTE (baseOffset −1, before any record) or the CAUGHT-UP
            // MARKER (baseOffset = the broker's HW, the same epoch).  Never a
            // lineage note (catalog subscriptions are never transient).
            dispatching_ = &impl;
            try {
                if (rv.baseOffset < 0) { if (impl.onHistoryEpoch) impl.onHistoryEpoch(l); }
                else if (impl.onCaughtUp) impl.onCaughtUp(rv.baseOffset, l);
            } catch (const std::exception& e) {
                ++callbackExceptions_;
                std::fprintf(stderr, "rune-client: catalog epoch/caught-up callback threw: %s (caught; the duty "
                                     "cycle continues)\n", e.what());
            } catch (...) {
                ++callbackExceptions_;
                std::fprintf(stderr, "rune-client: catalog epoch/caught-up callback threw a non-std::exception "
                                     "(caught; the duty cycle continues)\n");
            }
            dispatching_ = nullptr;
            if (!impl.active) {   // finished from inside the callback (as for a record handler)
                closeStatus_(impl);
                if (!impl.unsubPending) activeById_.erase(it);
            }
            return;
        }
        // E2 round 6: the TRANSIENT LINEAGE NOTE (client version 7) — the data
        // lineage this subscription's records belong to (claimed on a resume).
        impl.lineage = l;
        return;
    }
    if (rb.empty()) {
        // A catalog frame with no epoch is not a version-8 marker: never taken as
        // "caught up" (the reader's deadline ends a read that never gets one).
        if (isEventLogTopic(impl.topicId)) [[unlikely]] { noteMalformed_(); return; }
        if (impl.filtered()) [[unlikely]] {
            // Phase 2c (SD11): a filtered subscription's POSITION NOTE — every record below it was evaluated and
            // every match below it delivered.  Monotonic.
            if (rv.baseOffset > impl.nextOffset) { impl.nextOffset = rv.baseOffset; publishStatus_(impl); }
            return;
        }
        // A POSITION NOTE (client version 6): the broker's cursor for this
        // subscription, sent right before it answers NotLeader (moved).  Kept
        // only while nothing was delivered (rearmMovedAway resumes there).
        if (impl.nextOffset == 0 && rv.baseOffset >= 0) impl.resumeHint = rv.baseOffset;
        return;
    }

    // A frame carries ONE batch, or SEVERAL whole batches back to back (the catalog
    // read server coalesces; DataMessages.h): every one is decoded.  A control
    // batch's offsets are consumed, never delivered.  No allocation: the visitor is
    // a template (RecordBatchReader::forEachRecord), never a std::function.
    const int32_t partitionOfRecords = impl.boundPartition >= 0 ? impl.boundPartition : impl.partition;
    impl.batchBytes += rb.size();   // FD13: egress bytes delivered (the status is published below)
    dispatching_ = &impl;
    std::span<const uint8_t> rest(rb.data(), rb.size());
    bool first = true;
    while (!rest.empty() && impl.active) {
        rune::protocol::RecordBatchReader reader(rest.data(), rest.size());
        if (!reader.ensureValid()) {   // also: shorter than a batch header
            noteMalformed_();          // counted + logged (rate-limited), never silent
            if (first) { dispatching_ = nullptr; return; }   // Malformed batch — drop (never crash).
            break;                                           // a malformed tail: what was decoded stands
        }
        first = false;
        const auto batchLen = static_cast<std::size_t>(reader.totalLength());
        const int64_t last  = reader.baseOffset() + static_cast<int64_t>(reader.lastOffsetDelta());
        // E2 final fix wave (I1): a moved subscription resumes at its position,
        // but the new leader may push from BELOW it — a batch straddling it
        // (brokers push whole batches), or whole batches below it while the new
        // leader's HW still lags the old one's (the gateway clamps the resume
        // offset to its HW).  Every record below the position was delivered
        // before the move: never twice.  The floor is STICKY until a batch at or
        // above it arrives (a re-armed subscription never meets a restarted
        // log: a restart ends the session, so the move fails explicitly).  No
        // allocation; one predicted compare per batch while no floor is set.
        int64_t dropBelow = -1;
        bool skip = false;
        if (impl.resumeFloor >= 0) [[unlikely]] {
            if (last < impl.resumeFloor) {   // wholly below: duplicates only
                ++movedDuplicateBatchesDropped_;
                skip = true;
            } else {
                dropBelow        = impl.resumeFloor;   // this batch carries a record at/above it
                impl.resumeFloor = -1;
            }
        }
        if (!skip && reader.isControl()) [[unlikely]] {
            // A control batch (a transaction marker): its offsets are consumed.
            ++controlBatchesSkipped_;
            if (last + 1 > impl.nextOffset) impl.nextOffset = last + 1;
            skip = true;
        }
        if (!skip) {
            reader.forEachRecord([&](std::string_view keyView,
                                     std::string_view valView,
                                     int64_t          absOffset,
                                     int64_t          timestamp)
            {
                // The handler may have unsubscribed this subscription (directly, on
                // this thread): deliver nothing more of the batch.
                if (!impl.active) return;
                if (absOffset < dropBelow) [[unlikely]] return;   // delivered before the move

                Record rec;
                rec.partition = partitionOfRecords;
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
            if (impl.active && impl.filtered() && last + 1 > impl.nextOffset) [[unlikely]] impl.nextOffset = last + 1;   // the rebuilt batch spans its source; not past records an unsubscribing handler never got
        }
        if (batchLen >= rest.size()) break;
        rest = rest.subspan(batchLen);
    }
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
    publishStatus_(impl);   // rb is non-empty here: batchBytes grew
}

// A Records frame whose batch (the first, or one after decoded ones) is not a
// valid RecordBatch: counted, and logged at most once per 1024 (duty thread).
void SubscriberHub::noteMalformed_() noexcept
{
    if (malformedBatches_++ % 1024 == 0)
        std::fprintf(stderr, "rune-client: a Records frame carried a malformed batch — dropped from there "
                             "(malformed so far: %llu)\n", static_cast<unsigned long long>(malformedBatches_));
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
        if (req->impl->homeSession != 0) { ++it; continue; }  // I2: lives on its node session
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

bool SubscriberHub::isPending(int64_t correlationId) const
{
    return pendingByCorrelation_.find(correlationId) != pendingByCorrelation_.end();
}

bool SubscriberHub::resendOne(int64_t correlationId, int64_t sessionId, const SendFn& submitFn,
                              std::chrono::steady_clock::time_point newDeadline)
{
    constexpr std::size_t kHeaderSessionIdOff = 8;
    auto it = pendingByCorrelation_.find(correlationId);
    if (it == pendingByCorrelation_.end()) return false;
    auto& req = it->second;
    if (!req || !req->impl || !req->impl->active) return false;
    auto& frame = req->subscribeFrame;
    if (frame.size() < kHeaderSessionIdOff + 8) return false;
    req->deadline = newDeadline;
    if (req->impl->moved && routed_()) {
        // I2: a moved subscription goes wherever its partition leader is now
        // (its node's own session, or the seed) — never by moving the seed.
        routeMovedPending_(it, sessionId);
        return true;
    }
    const uint64_t u = static_cast<uint64_t>(sessionId);
    for (int i = 0; i < 8; ++i) frame[kHeaderSessionIdOff + i] = static_cast<uint8_t>(u >> (i * 8));
    if (directSend_ != nullptr) {
        switch (sendSubscribeFrame_(*req)) {
        case detail::SendResult::Accepted: clearUnsent_(*req); return true;
        case detail::SendResult::Retry:    markUnsent_(*req); return true;
        case detail::SendResult::Fatal:    (void)failRequest_(it); return false;
        }
    }
    return submitFn && submitFn(0, frame.data(), frame.size());
}

// ---------------------------------------------------------------------------
// M2 FIX: failPendingSubscribes — fail all unbound pending subscribes
// ---------------------------------------------------------------------------

bool SubscriberHub::rearmMovedAway(int64_t correlationId, int64_t newCorrId, int64_t unsubCorrId,
                                   int64_t answerSid, int64_t seedSid,
                                   std::chrono::steady_clock::time_point deadline)
{
    if (correlationId == 0 || newCorrId == 0) return false;
    for (auto it = activeById_.begin(); it != activeById_.end(); ++it) {
        SubscriptionImpl& impl = *it->second;
        if (impl.correlationId != correlationId) continue;
        // Unsubscribed (erase deferred / Unsubscribe pending) or its handler is
        // running: nothing to move.
        if (!impl.active || impl.unsubPending || &impl == dispatching_) return false;
        // Only an answer on the subscription's OWN session moves it (a stale
        // one of an earlier session concerns an instance that died with it).
        const int64_t homeSid = homeSidOf_(impl, seedSid);
        if (homeSid == 0 || answerSid != homeSid) return false;
        const int64_t sessionId = seedSid;   // the re-armed request starts on the seed (see homeMoved)
        const int32_t part = impl.boundPartition >= 0 ? impl.boundPartition : impl.partition;
        // Nothing delivered (nextOffset 0): the broker's position note (its
        // bind-time resolved start / cursor), else the requested From.
        const int64_t from = impl.nextOffset > 0  ? impl.nextOffset
                           : impl.resumeHint >= 0 ? impl.resumeHint
                                                  : impl.fromWire;
        uint8_t frameBuf[codec::kMaxSubscribeFrameBytes];
        uint8_t claim[8];
        const std::size_t frameLen = codec::encodeSubscribe(frameBuf, sizeof(frameBuf), impl.topicId, part,
                                                            /*partitionKnown=*/1, from, impl.isolation,
                                                            lineageClaim_(impl, claim), sessionId, newCorrId,
                                                            impl.filterSpan());
        if (frameLen == 0) return false;
        auto req = std::make_unique<SubscribeRequest>();   // cold: once per leadership move
        req->impl     = &impl;
        req->deadline = deadline;
        req->subscribeFrame.assign(frameBuf, frameBuf + frameLen);
        // Belt and braces: release the old instance on this session too.  The
        // node that answered NotLeader already dropped it (an Unsubscribe of an
        // unknown id is ignored); should the answer have been a stray one, this
        // keeps a second broker-side stream from leaking until the session
        // ends.  Best-effort by design: a refused offer is moot either way.
        {
            uint8_t unsubBuf[64]{};
            const std::size_t unsubLen =
                codec::encodeUnsubscribe(unsubBuf, sizeof(unsubBuf), impl.subscriptionId, homeSid, unsubCorrId);
            if (unsubLen > 0) {
                if (impl.homeSession != 0 && routed_())   // I2: its old node session
                    (void)router_.offer(router_.ctx, impl.homeSession, unsubBuf, unsubLen);
                else if (directSend_ != nullptr)
                    (void)directSend_(directSendCtx_, unsubBuf, unsubLen);
            }
        }
        activeById_.erase(it);
        impl.correlationId  = newCorrId;
        impl.subscriptionId = 0;   // pending again until the leader binds it
        impl.moved          = true;
        impl.homeSession    = 0;   // routed by the caller (homeMoved / the pending paths)
        impl.homeSid        = 0;
        impl.homeEpoch      = 0;
        impl.resumeFloor    = impl.nextOffset > 0 ? impl.nextOffset : -1;
        impl.resumeHint     = -1;
        pendingByCorrelation_.emplace(newCorrId, std::move(req));
        ++subscriptionsMoved_;
        publishStatus_(impl);
        return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// E2 final fix wave (I2): moved subscriptions on their leader's own session
// ---------------------------------------------------------------------------

void SubscriberHub::stampSession_(std::vector<uint8_t>& frame, int64_t sid) noexcept
{
    constexpr std::size_t kHeaderSessionIdOff = 8;
    if (frame.size() < kHeaderSessionIdOff + 8) return;
    const auto u = static_cast<uint64_t>(sid);
    for (int i = 0; i < 8; ++i) frame[kHeaderSessionIdOff + i] = static_cast<uint8_t>(u >> (i * 8));
}

bool SubscriberHub::movedPendingPartition(int64_t correlationId, int32_t& topicId, int32_t& partition) const
{
    const auto it = pendingByCorrelation_.find(correlationId);
    if (it == pendingByCorrelation_.end() || !it->second || it->second->impl == nullptr) return false;
    const SubscriptionImpl& impl = *it->second->impl;
    if (!impl.moved || !impl.active) return false;
    topicId   = impl.topicId;
    partition = impl.boundPartition >= 0 ? impl.boundPartition : impl.partition;
    return true;
}

/*static*/ std::span<const uint8_t> SubscriberHub::lineageClaim_(const SubscriptionImpl& impl, uint8_t (&buf)[8]) noexcept
{
    // A claim only for a resume FROM A POSITION (something delivered, or the
    // broker's cursor note) of a subscription the broker noted a lineage for.
    if (impl.lineage == 0 || (impl.nextOffset <= 0 && impl.resumeHint < 0)) return {};
    for (int i = 0; i < 8; ++i) buf[i] = static_cast<uint8_t>(impl.lineage >> (8 * i));
    return std::span<const uint8_t>(buf, 8);
}

bool SubscriberHub::restartFromLogStart(int64_t correlationId, int64_t seedSid,
                                        std::chrono::steady_clock::time_point deadline)
{
    const auto it = pendingByCorrelation_.find(correlationId);
    if (it == pendingByCorrelation_.end() || !it->second || it->second->impl == nullptr) return false;
    SubscriptionImpl& impl = *it->second->impl;
    if (!impl.active) return false;
    const int32_t part = impl.boundPartition >= 0 ? impl.boundPartition : impl.partition;
    uint8_t frameBuf[codec::kMaxSubscribeFrameBytes];
    constexpr int64_t kFromEarliest = -2;   // From::earliest() on the wire: the log start
    const std::size_t frameLen = codec::encodeSubscribe(frameBuf, sizeof(frameBuf), impl.topicId, part,
                                                        /*partitionKnown=*/1, kFromEarliest, impl.isolation,
                                                        std::span<const uint8_t>{}, seedSid, correlationId,
                                                        impl.filterSpan());
    if (frameLen == 0) return false;
    try {
        it->second->subscribeFrame.assign(frameBuf, frameBuf + frameLen);
    } catch (...) {
        return false;   // allocation failure (cold): the subscribe deadline fails it explicitly
    }
    // The data it had a position in is gone: restart at the log start, no
    // floor, no claim — and tell the application (never silent).
    ++impl.dataResets;
    ++partitionDataResets_;
    impl.lineage     = 0;
    impl.nextOffset  = 0;
    impl.resumeFloor = -1;
    impl.resumeHint  = -1;
    impl.fromWire    = kFromEarliest;
    publishStatus_(impl);
    return resendOne(correlationId, seedSid, SendFn{}, deadline);
}

bool SubscriberHub::answeredOnItsSession(int64_t correlationId, int64_t answerSid, int64_t seedSid) const
{
    if (answerSid == 0) return true;
    const auto it = pendingByCorrelation_.find(correlationId);
    if (it == pendingByCorrelation_.end() || !it->second || it->second->impl == nullptr) return true;
    const SubscriptionImpl& impl = *it->second->impl;
    if (!impl.active) return true;
    if (impl.homeSession != 0 && impl.homeSid == 0) return false;   // held: nothing outstanding
    return answerSid == homeSidOf_(impl, seedSid);
}

bool SubscriberHub::isNewPendingSubscribe(int64_t correlationId) const
{
    const auto it = pendingByCorrelation_.find(correlationId);
    if (it == pendingByCorrelation_.end() || !it->second || it->second->impl == nullptr) return false;
    const SubscriptionImpl& impl = *it->second->impl;
    return impl.active && !impl.moved && impl.subscriptionId == 0;
}

bool SubscriberHub::adoptRedirectedSubscribe(int64_t correlationId)
{
    if (!routed_()) return false;
    const auto it = pendingByCorrelation_.find(correlationId);
    if (it == pendingByCorrelation_.end() || !it->second || it->second->impl == nullptr) return false;
    SubscriptionImpl& impl = *it->second->impl;
    if (!impl.active || impl.subscriptionId != 0) return false;
    impl.moved = true;   // per-partition routed from now on (no position: nothing delivered yet)
    return true;
}

bool SubscriberHub::homeMoved(int64_t correlationId, int64_t seedSid)
{
    if (!routed_()) return false;
    const auto it = pendingByCorrelation_.find(correlationId);
    if (it == pendingByCorrelation_.end() || !it->second || it->second->impl == nullptr) return false;
    if (!it->second->impl->moved || !it->second->impl->active) return false;
    routeMovedPending_(it, seedSid);
    return true;
}

void SubscriberHub::routeMovedPending_(PendingMap::iterator it, int64_t seedSid)
{
    SubscribeRequest& req  = *it->second;
    SubscriptionImpl& impl = *req.impl;
    const int32_t part     = impl.boundPartition >= 0 ? impl.boundPartition : impl.partition;
    const uint32_t session = router_.leaderSession(router_.ctx, impl.topicId, part);
    impl.homeSid   = 0;
    impl.homeEpoch = 0;
    if (session == 0) {
        // The seed's node leads it: the historical seed path (sent now when
        // the seed is Connected, else re-sent on its next connect).
        impl.homeSession = 0;
        stampSession_(req.subscribeFrame, seedSid);
        if (!sessionConnected_ || seedSid == 0 || directSend_ == nullptr) { markUnsent_(req); return; }
        switch (sendSubscribeFrame_(req)) {
        case detail::SendResult::Accepted: clearUnsent_(req); break;
        case detail::SendResult::Retry:    markUnsent_(req); break;
        case detail::SendResult::Fatal:    (void)failRequest_(it); break;
        }
        return;
    }
    clearUnsent_(req);   // never the seed's retry list: driveHomed owns it now
    impl.homeSession = session;
    ++homedCount_;
    sendHomed_(it);
}

void SubscriberHub::sendHomed_(PendingMap::iterator it)
{
    SubscribeRequest& req  = *it->second;
    SubscriptionImpl& impl = *req.impl;
    if (brokerTooOldForFilter_(impl)) [[unlikely]] { (void)failRequest_(it); return; }   // SD3: that node's broker
    const int64_t sid = router_.sessionId(router_.ctx, impl.homeSession);
    if (sid == 0) return;   // that node's session is (re)connecting: held, flushed by driveHomed
    stampSession_(req.subscribeFrame, sid);
    switch (router_.offer(router_.ctx, impl.homeSession, req.subscribeFrame.data(), req.subscribeFrame.size())) {
    case detail::SendResult::Accepted:
        impl.homeSid   = sid;
        impl.homeEpoch = router_.epoch(router_.ctx, impl.homeSession);
        break;
    case detail::SendResult::Retry:
        break;   // held: re-offered by driveHomed
    case detail::SendResult::Fatal:
        (void)failRequest_(it);
        break;
    }
}

bool SubscriberHub::movedRoutesToANode(int64_t correlationId)
{
    int32_t t = -1, p = -1;
    if (!routed_() || !movedPendingPartition(correlationId, t, p)) return false;
    return router_.leaderSession(router_.ctx, t, p) != 0;
}

bool SubscriberHub::holdMovedForRetry(int64_t correlationId, std::chrono::steady_clock::time_point notBefore)
{
    if (!routed_()) return false;
    const auto it = pendingByCorrelation_.find(correlationId);
    if (it == pendingByCorrelation_.end() || !it->second || it->second->impl == nullptr) return false;
    SubscriptionImpl& impl = *it->second->impl;
    if (!impl.moved || !impl.active) return false;
    const int32_t  part    = impl.boundPartition >= 0 ? impl.boundPartition : impl.partition;
    const uint32_t session = router_.leaderSession(router_.ctx, impl.topicId, part);
    if (session == 0) return false;   // the seed's node leads: the seed's own retry path
    clearUnsent_(*it->second);
    impl.homeSession       = session;
    impl.homeSid           = 0;   // held: driveHomed re-offers it after notBefore
    impl.homeEpoch         = 0;
    it->second->notBefore  = notBefore;
    ++homedCount_;
    return true;
}

int16_t SubscriberHub::homeRejectCode_(const SubscriptionImpl& impl) const noexcept
{
    if (impl.homeSession == 0 || router_.rejectCode == nullptr) return 0;
    return router_.rejectCode(router_.ctx, impl.homeSession);
}

void SubscriberHub::endRejected_(SubscriptionImpl& impl, int16_t code)
{
    // As the subscription's own refusal would end it (endRefused): 46 is
    // SessionRevoked, anything else (44 a refused login, ...) RejectedByBroker.
    impl.failReason      = code == static_cast<int16_t>(rune::protocol::client::ErrorCode::SessionRevoked)
                               ? SubscriptionFailReason::SessionRevoked
                               : SubscriptionFailReason::RejectedByBroker;
    impl.brokerErrorCode = static_cast<uint16_t>(code);
    finish_(impl);
}

int SubscriberHub::driveHomed(int64_t seedSid, std::chrono::steady_clock::time_point deadline,
                              std::chrono::steady_clock::time_point now)
{
    if (!routed_()) { homedCount_ = 0; unansweredHomed_ = 0; return 0; }
    int work = 0;
    std::size_t homed = 0;
    std::size_t unanswered = 0;
    // Pending ones: held (not sent) -> re-resolve the leader and send; sent but
    // their session changed under them (reconnect / drop) -> route again.
    for (auto it = pendingByCorrelation_.begin(); it != pendingByCorrelation_.end();) {
        auto next = std::next(it);
        SubscribeRequest* req = it->second.get();
        if (req != nullptr && req->impl != nullptr && req->impl->active && req->impl->homeSession != 0) {
            SubscriptionImpl& impl = *req->impl;
            if (const int16_t code = homeRejectCode_(impl); code != 0) {   // #290: never sendable there
                endRejected_(impl, code);
                clearUnsent_(*req);
                pendingByCorrelation_.erase(it);
                ++work;
                it = next;
                continue;
            }
            const bool held = impl.homeSid == 0;
            const bool lost = !held && router_.epoch(router_.ctx, impl.homeSession) != impl.homeEpoch;
            const bool due  = !held || now >= req->notBefore;   // a NotLeader backoff (holdMovedForRetry)
            // #365: fresh metadata moved the partition off its home session (a
            // crashed leader's session is not yet seen lost): follow the route.
            const int32_t part = impl.boundPartition >= 0 ? impl.boundPartition : impl.partition;
            const bool moved = !held && router_.leaderSession(router_.ctx, impl.topicId, part) != impl.homeSession;
            if ((held && due) || lost || moved) {
                if (lost) ++homedSessionsLost_;
                routeMovedPending_(it, seedSid);   // homedCount_ is recounted below
                ++work;
            }
            if (pendingByCorrelation_.find(impl.correlationId) != pendingByCorrelation_.end() &&
                impl.homeSession != 0) {
                ++homed;
                if (impl.homeSid != 0) ++unanswered;   // sent there, no answer yet
            }
        }
        it = next;
    }
    // Bound ones whose node session was lost (the broker ended the
    // subscription with that session), or whose partition fresh metadata moved
    // to another node (#365: a crashed leader) — re-arm from the position.
    homedScratch_.clear();
    for (const auto& [id, impl] : activeById_) {
        if (impl->homeSession == 0 || !impl->active || impl->unsubPending || impl == dispatching_) continue;
        const int32_t part = impl->boundPartition >= 0 ? impl->boundPartition : impl->partition;
        if (router_.epoch(router_.ctx, impl->homeSession) == impl->homeEpoch &&
            router_.leaderSession(router_.ctx, impl->topicId, part) == impl->homeSession) {   // #365: route moved
            ++homed;
            continue;
        }
        try {
            homedScratch_.push_back(id);
        } catch (...) {
            ++homed;   // allocation failure (cold): the next pass retries
        }
    }
    for (const int64_t id : homedScratch_) {
        const auto ait = activeById_.find(id);
        if (ait == activeById_.end()) continue;
        SubscriptionImpl& impl = *ait->second;
        if (const int16_t code = homeRejectCode_(impl); code != 0) {   // #290: its node session was Rejected
            endRejected_(impl, code);
            activeById_.erase(ait);
            ++work;
            continue;
        }
        const int64_t newCorr = router_.nextCorrId(router_.ctx);
        const int32_t part    = impl.boundPartition >= 0 ? impl.boundPartition : impl.partition;
        const int64_t from    = impl.nextOffset > 0  ? impl.nextOffset
                              : impl.resumeHint >= 0 ? impl.resumeHint
                                                     : impl.fromWire;
        uint8_t frameBuf[codec::kMaxSubscribeFrameBytes];
        uint8_t claim[8];
        const std::size_t frameLen = codec::encodeSubscribe(frameBuf, sizeof(frameBuf), impl.topicId, part,
                                                            /*partitionKnown=*/1, from, impl.isolation,
                                                            lineageClaim_(impl, claim), seedSid, newCorr,
                                                            impl.filterSpan());
        if (frameLen == 0) { ++homed; continue; }
        std::unique_ptr<SubscribeRequest> req;
        try {
            req = std::make_unique<SubscribeRequest>();   // cold: once per lost node session
            req->subscribeFrame.assign(frameBuf, frameBuf + frameLen);
        } catch (...) {
            ++homed;
            continue;   // the next pass retries
        }
        req->impl     = &impl;
        req->deadline = deadline;
        if (router_.epoch(router_.ctx, impl.homeSession) == impl.homeEpoch) {
            // #365: its session is still up (only the route moved): release the
            // old instance there, best-effort as on a NotLeader move.
            uint8_t unsubBuf[64]{};
            const std::size_t unsubLen = codec::encodeUnsubscribe(unsubBuf, sizeof(unsubBuf), impl.subscriptionId,
                                                                  impl.homeSid, router_.nextCorrId(router_.ctx));
            if (unsubLen > 0) (void)router_.offer(router_.ctx, impl.homeSession, unsubBuf, unsubLen);
        }
        ++homedSessionsLost_;
        activeById_.erase(ait);
        impl.correlationId  = newCorr;
        impl.subscriptionId = 0;
        impl.resumeFloor    = impl.nextOffset > 0 ? impl.nextOffset : -1;
        impl.resumeHint     = -1;
        impl.homeSession    = 0;
        impl.homeSid        = 0;
        impl.homeEpoch      = 0;
        auto [pit, ok] = pendingByCorrelation_.emplace(newCorr, std::move(req));
        if (!ok) continue;
        ++subscriptionsMoved_;
        publishStatus_(impl);
        routeMovedPending_(pit, seedSid);
        if (impl.homeSession != 0) ++homed;
        ++work;
    }
    homedCount_      = homed;
    unansweredHomed_ = unanswered;
    return work;
}

bool SubscriberHub::chargeNotLeaderRetry(int64_t correlationId, int maxRetries)
{
    const auto it = pendingByCorrelation_.find(correlationId);
    if (it == pendingByCorrelation_.end() || !it->second) return false;
    if (it->second->nlRetries >= maxRetries) return false;
    ++it->second->nlRetries;
    return true;
}

bool SubscriberHub::failPendingSubscribe(int64_t correlationId, SubscriptionFailReason reason,
                                         uint16_t brokerErrorCode)
{
    const auto it = pendingByCorrelation_.find(correlationId);
    if (it == pendingByCorrelation_.end()) return false;
    clearUnsent_(*it->second);
    if (it->second->impl) {
        it->second->impl->failReason = reason;
        it->second->impl->brokerErrorCode = brokerErrorCode;
        finish_(*it->second->impl);
    }
    pendingByCorrelation_.erase(it);
    return true;
}

bool SubscriberHub::failActiveSubscription(int64_t subscriptionId, SubscriptionFailReason reason,
                                           int64_t unsubCorrId, int64_t sessionId, const SendFn& submitFn)
{
    const auto it = activeById_.find(subscriptionId);
    if (it == activeById_.end() || !it->second->active) return false;
    it->second->failReason = reason;  // before the finish: the final snapshot carries it
    return unsubscribe(it->second->correlationId, unsubCorrId, sessionId, submitFn);
}

bool SubscriberHub::pendingSubscribeTarget(int64_t correlationId, int32_t& topicId, int32_t& partition) const
{
    const auto it = pendingByCorrelation_.find(correlationId);
    if (it == pendingByCorrelation_.end() || !it->second || it->second->impl == nullptr) return false;
    topicId   = it->second->impl->topicId;
    partition = it->second->impl->partition;
    return true;
}

bool SubscriberHub::failActiveByCorrelation(int64_t correlationId, SubscriptionFailReason why, uint16_t code,
                                            int64_t unsubCorrId, int64_t sessionId, const SendFn& submitFn)
{
    if (correlationId == 0) return false;
    for (const auto& [id, impl] : activeById_) {
        if (impl == nullptr || impl->correlationId != correlationId) continue;
        if (!impl->active) return false;
        impl->failReason      = why;   // before the finish: the final snapshot carries them
        impl->brokerErrorCode = code;
        return unsubscribe(correlationId, unsubCorrId, sessionId, submitFn);
    }
    return false;
}

void SubscriberHub::failPendingSubscribes(const std::string& reason)
{
    // Finishing the impl (inactive + closed status ring) is the error signal:
    // the app's Subscription::active() turns false.  No unsubscribe frame is
    // needed — the broker never bound the id.
    // I2: a moved subscription homed on a leader node's own session does not
    // depend on the seed's redirect episode — it stays (its own deadline and
    // driveHomed bound it).  Client close finishes everything (deactivateAll).
    for (auto it = pendingByCorrelation_.begin(); it != pendingByCorrelation_.end();) {
        auto& req = it->second;
        if (req && req->impl && req->impl->homeSession != 0) { ++it; continue; }
        if (req) clearUnsent_(*req);
        if (req && req->impl) finish_(*req->impl);
        it = pendingByCorrelation_.erase(it);
    }
    (void)reason;
}

bool SubscriberHub::endCatalogRead(int64_t correlationId, int64_t answerSid, int64_t seedSid)
{
    if (correlationId == 0) return false;
    for (auto it = activeById_.begin(); it != activeById_.end(); ++it) {
        SubscriptionImpl* impl = it->second;
        if (impl == nullptr || impl->correlationId != correlationId) continue;
        if (!isEventLogTopic(impl->topicId)) return false;   // not an event-log (catalog / security) read
        if (!impl->active || impl->unsubPending) return false;
        const int64_t homeSid = homeSidOf_(*impl, seedSid);
        if (homeSid == 0 || answerSid != homeSid) return false;   // a stale answer of an earlier session
        impl->failReason = SubscriptionFailReason::CatalogReadEnded;
        finish_(*impl);   // inactive + final status (closed by onRecords if dispatching)
        ++catalogReadsEnded_;
        if (impl != dispatching_) activeById_.erase(it);   // else onRecords erases it once its handler returns
        return true;
    }
    return false;
}

bool SubscriberHub::endRefused(int64_t correlationId, int64_t answerSid, int64_t seedSid,
                               SubscriptionFailReason why, uint16_t code)
{
    if (correlationId == 0) return false;
    if (pendingByCorrelation_.find(correlationId) != pendingByCorrelation_.end()) {
        if (!answeredOnItsSession(correlationId, answerSid, seedSid)) {
            ++staleResponsesIgnored_;   // an earlier session's answer: the re-sent Subscribe will be answered itself
            return true;
        }
        return failPendingSubscribe(correlationId, why, code);
    }
    for (auto it = activeById_.begin(); it != activeById_.end(); ++it) {
        SubscriptionImpl* impl = it->second;
        if (impl == nullptr || impl->correlationId != correlationId) continue;
        if (!impl->active || impl->unsubPending) return false;
        const int64_t homeSid = homeSidOf_(*impl, seedSid);
        if (homeSid == 0 || answerSid != homeSid) return true;   // a stale answer of an earlier session
        impl->failReason      = why;
        impl->brokerErrorCode = code;
        finish_(*impl);   // inactive + final status (closed by onRecords if dispatching)
        if (impl != dispatching_) activeById_.erase(it);   // else onRecords erases it once its handler returns
        return true;
    }
    return false;
}

int SubscriberHub::endAllLive(SubscriptionFailReason why, uint16_t code)
{
    int ended = 0;
    for (auto it = pendingByCorrelation_.begin(); it != pendingByCorrelation_.end();) {
        auto& req = it->second;
        if (req) clearUnsent_(*req);
        if (req && req->impl) {
            if (req->impl->active) {
                req->impl->failReason      = why;
                req->impl->brokerErrorCode = code;
                ++ended;
            }
            finish_(*req->impl);
        }
        it = pendingByCorrelation_.erase(it);
    }
    for (auto it = activeById_.begin(); it != activeById_.end();) {
        SubscriptionImpl* impl = it->second;
        if (impl == nullptr || !impl->active) { ++it; continue; }
        impl->failReason      = why;
        impl->brokerErrorCode = code;
        finish_(*impl);
        ++ended;
        if (impl == dispatching_) { ++it; continue; }   // onRecords erases it once its handler returns
        it = activeById_.erase(it);
    }
    return ended;
}

int SubscriberHub::endSeedBound()
{
    int ended = 0;
    for (auto it = activeById_.begin(); it != activeById_.end();) {
        SubscriptionImpl* impl = it->second;
        if (impl == nullptr || !impl->active || impl->homeSession != 0) { ++it; continue; }
        impl->failReason = SubscriptionFailReason::SessionLost;
        finish_(*impl);   // inactive + final status (closed by onRecords if dispatching)
        ++ended;
        ++seedBoundEnded_;
        if (impl == dispatching_) { ++it; continue; }   // onRecords erases it once its handler returns
        it = activeById_.erase(it);
    }
    return ended;
}

// ---------------------------------------------------------------------------
// M2 FIX: sweepTimedOut — timeout pending subscribes that got no response
// ---------------------------------------------------------------------------

int SubscriberHub::sweepTimedOut(const std::chrono::steady_clock::time_point& now)
{
    int timedOut = 0;
    for (auto it = pendingByCorrelation_.begin();
         it != pendingByCorrelation_.end(); )
    {
        if (now >= it->second->deadline) {
            if (SubscriptionImpl* impl = it->second->impl) {
                // An app-unsubscribed (inactive) one times out quietly; an active
                // one is an explicit TimedOut failure the app must see.
                if (impl->active) {
                    impl->failReason = SubscriptionFailReason::TimedOut;
                    ++timedOut;
                }
                finish_(*impl);
            }
            clearUnsent_(*it->second);
            it = pendingByCorrelation_.erase(it);
        } else {
            ++it;
        }
    }
    return timedOut;
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
