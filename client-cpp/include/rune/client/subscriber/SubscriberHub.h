// include/rune/client/subscriber/SubscriberHub.h
//
// CB-4 — SubscriberHub: manages all active subscriptions.
//
// Threading model (F8a — shared-nothing, no mutex, no atomics):
//   - The hub and every table in it are OWNED by the duty thread (the Thread-
//     mode runner, or the Invoker owner).  Every method below except the static
//     makeSubscribeRequest() runs on that thread only.
//   - App-thread API calls never touch the hub: Client::subscribe() builds a
//     SubscribeRequest (makeSubscribeRequest, a pure function) and hands it to
//     the duty thread as a HubCommand{SubscribeAdd} on the agent command ring;
//     Subscription::unsubscribe() sends HubCommand{Unsubscribe}.  The duty
//     thread applies them via applyCommand().
//   - Duty-thread state the app needs (subscriptionId, resume position,
//     liveness) is PUBLISHED to each subscription's StatusRing; the app-side
//     Subscription handle drains it.  Invariant: whenever the hub drops a
//     subscription (unsubscribed, timed out, failed, closed) it publishes the
//     final snapshot and closes that ring.
//   - Record dispatch (onRecords) is a plain map lookup + handler call; one
//     status publish per Records frame (never per record).

#pragma once

#include <rune/client/subscriber/Subscription.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/detail/HubCommand.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace rune::client::subscriber {

/// App-thread -> duty-thread hand-off for one subscribe() call (heap-allocated
/// by the app thread, ownership passed through the agent command ring).
struct SubscribeRequest {
    SubscriptionImpl*                     impl = nullptr;  // F8c C7: not owning (see owned_)
    std::vector<uint8_t>                  subscribeFrame;  // encoded Subscribe (resend on redirect)
    std::chrono::steady_clock::time_point deadline;        // M2: subscribe-request timeout
    bool                                  unsent = false;  // fix3 I4: refused, retried per cycle
    uint8_t                               nlRetries = 0;   // bare-NotLeader retries spent (bounded budget)
};

class SubscriberHub {
public:
    /// SendFn: (ingressStreamId, frame, len) -> bool.
    /// ingressStreamId=0 means "use the default ingress stream".
    /// Matches the ClientAgent::SendFn signature so Task 1.3 can pass the
    /// stream id through without an adapter at every call site.
    using SendFn = std::function<bool(int32_t, const uint8_t*, std::size_t)>;

    SubscriberHub() = default;

    // Non-copyable, non-movable.
    SubscriberHub(const SubscriberHub&) = delete;
    SubscriberHub& operator=(const SubscriberHub&) = delete;
    SubscriberHub(SubscriberHub&&) = delete;
    SubscriberHub& operator=(SubscriberHub&&) = delete;

    // -------------------------------------------------------------------------
    // App thread: build the duty-thread hand-off (pure — touches no hub state)
    // -------------------------------------------------------------------------

    /// Encode the Subscribe frame for `impl` (stack-encoded, then copied into
    /// the request) and package it with the subscribe-request deadline.  The
    /// caller hands the returned request to the duty thread as
    /// HubCommand{SubscribeAdd, handle = request.release()}.
    ///
    ///   fromOffset     — wire-encoded From value (see From::toWire())
    ///   isolation      — isolation byte
    ///   keyBytes       — key bytes (valid only during this call)
    ///   partitionKnown — 1 if partition is explicit, 0 if resolved from key
    ///   deadline       — subscribe-request timeout (sweepTimedOut fails it)
    [[nodiscard]] static std::unique_ptr<SubscribeRequest>
    makeSubscribeRequest(SubscriptionImpl*                     impl,
                         int64_t                               sessionId,
                         uint8_t                               partitionKnown,
                         int64_t                               fromOffset,
                         uint8_t                               isolation,
                         std::span<const uint8_t>              keyBytes,
                         std::chrono::steady_clock::time_point deadline);

    // -------------------------------------------------------------------------
    // Duty thread: commands drained from the agent ring
    // -------------------------------------------------------------------------

    /// Apply one subscriber HubCommand (SubscribeAdd / Unsubscribe).
    /// `unsubCorrId` is a fresh correlationId for an Unsubscribe frame (used
    /// only by Op::Unsubscribe on a bound subscription).  Ops that are not
    /// subscriber ops are ignored.
    void applyCommand(const detail::HubCommand& cmd,
                      int64_t                   unsubCorrId,
                      int64_t                   sessionId,
                      const SendFn&             submitFn);

    /// Adopt a SubscribeRequest: register it as pending (keyed by its
    /// correlationId) and send its Subscribe frame.
    void adoptSubscribe(std::unique_ptr<SubscribeRequest> req, const SendFn& submitFn);

    /// Unsubscribe the subscription whose Subscribe correlationId is
    /// `subscribeCorrId`.  Bound: drop the routing entry, send Unsubscribe
    /// (unsubCorrId), close its status ring.  Still pending: mark it inactive
    /// and close its status ring; the late SubscribeResponse then triggers the
    /// CB-5 A2 Unsubscribe with the real id.  Unknown: no-op.
    /// Returns false if no such subscription is in the hub (not adopted yet,
    /// or already finished).
    bool unsubscribe(int64_t subscribeCorrId, int64_t unsubCorrId,
                     int64_t sessionId, const SendFn& submitFn);

    /// fix3 I3: unsubscribe applied DIRECTLY on the duty thread for a handle
    /// whose impl the caller holds: acts on the impl even if its SubscribeAdd
    /// is still queued in the ring (adoption then drops it).
    void unsubscribeDirect(SubscriptionImpl& impl, int64_t unsubCorrId,
                           int64_t sessionId, const SendFn& submitFn);

    // -------------------------------------------------------------------------
    // Agent-thread callbacks (called from ClientAgent's egress observer)
    // -------------------------------------------------------------------------

    /// Called on the duty thread when a SubscribeResponse arrives.
    ///
    /// Binds the broker-assigned subscriptionId to the pending impl, and moves
    /// the entry from pendingByCorrelation_ to activeById_.
    /// Unknown correlationId → no-op (race with close/unsubscribe; safe).
    ///
    /// CB-5 A2 FIX: if the impl was already unsubscribed before the response
    /// arrived, erases it from activeById_ and sends an Unsubscribe frame via
    /// submitFn (using the newly known subscriptionId).
    void onSubscribeResponse(const codec::SubscribeResponseView& rsp,
                             int64_t                             unsubCorrId,
                             int64_t                             sessionId,
                             const SendFn&                       submitFn);

    /// Called on the agent thread when a Records push arrives.
    ///
    /// Looks up the Subscription by subscriptionId.  If not found or inactive,
    /// drops the batch.  Otherwise decodes the RecordBatch via RecordBatchReader
    /// (zero-copy spans into the Aeron buffer) and calls the handler once per
    /// record.  Updates nextOffset after each delivered record.
    ///
    /// The recordBatch span MUST be valid for the duration of this call
    /// (Aeron guarantees the fragment buffer is live for the callback duration).
    /// Matched by subscriptionId (cluster-unique: the broker puts its node id in
    /// the high bits), the creating Subscribe's correlationId (stamped on every
    /// push; a pending subscription is bound by its first Records frame), and
    /// the subscription's own topic / resolved partition.  `currentSessionId`:
    /// the seed session's broker id — a pending subscription is bound by a push
    /// only when it is non-zero and equals the push's session (0 = no session:
    /// only the SubscribeResponse binds).
    void onRecords(const codec::RecordsView& rv, int64_t currentSessionId = 0);

    /// Fail / deactivate all subscriptions (called on close, after the duty
    /// thread is gone).  Publishes each final snapshot and closes each status
    /// ring.  No Unsubscribe frames are sent (transport is shutting down).
    void deactivateAll();

    /// M2 FIX [CB-5]: Fail all PENDING (unbound) subscriptions with a clear
    /// error.  Called when a redirect reaches a dead endpoint, retarget fails,
    /// or kMaxRedirects is exhausted — so pending subscribes are not orphaned
    /// in pendingByCorrelation_ forever.  Also called from close() to guarantee
    /// no subscribe future can hang.
    ///
    /// Each pending impl's active flag is cleared (so a late SubscribeResponse
    /// is treated as an already-unsubscribed response and a late Unsubscribe is
    /// emitted via the CB-5 A2 path rather than routing records to a dead impl).
    void failPendingSubscribes(const std::string& reason);
    /// Fail the ONE pending subscription whose Subscribe carried
    /// `correlationId` (its handle sees finished()).  False if none.
    bool failPendingSubscribe(int64_t correlationId, SubscriptionFailReason reason,
                              uint16_t brokerErrorCode = 0);

    /// Rune E2 Task 13: the broker answered the Subscribe `correlationId` of a
    /// BOUND subscription with NotLeader / Redirect — its node stopped leading
    /// the partition and dropped the subscription.  Re-arm it as PENDING under
    /// `newCorrId`: its Subscribe is re-encoded from the resume position
    /// (nextOffset; the requested From while nothing was delivered), the same
    /// isolation, its bound partition (partitionKnown), stamped `sessionId`,
    /// with `deadline` as its subscribe timeout.  The Subscribe is NOT sent
    /// here: the caller routes it like any refused pending Subscribe.  The old
    /// instance gets a best-effort Unsubscribe (`unsubCorrId`) on `sessionId`;
    /// its Records (old id / correlationId) are dropped from now on.  False (no
    /// change) when `correlationId` names no active bound subscription.  Cold.
    ///
    /// E2 final fix wave (I2): `answerSid` is the session the answer came on —
    /// it must be the subscription's own home session (`seedSid` for one on the
    /// seed, its node session's id for a moved one homed on a leader node);
    /// the old instance's Unsubscribe goes to that home.  The re-armed request
    /// is stamped `seedSid` and homed on the seed until routed (homeMoved).
    bool rearmMovedAway(int64_t correlationId, int64_t newCorrId, int64_t unsubCorrId, int64_t answerSid,
                        int64_t seedSid, std::chrono::steady_clock::time_point deadline);

    // ---- E2 final fix wave (I2): moved subscriptions live on their leader's
    // own node session (never by retargeting the seed) -------------------------

    /// How a moved subscription reaches its partition leader's node session
    /// (plain function pointers + context, wired once before start by the
    /// Client to its ClientAgent — the group fetch router's trampolines).
    struct NodeRouter {
        void* ctx = nullptr;
        /// Session index leading (topicId, partition) now (0 = the seed); may
        /// lazily open that node's session.
        uint32_t (*leaderSession)(void* ctx, int32_t topicId, int32_t partition) = nullptr;
        /// That session's broker session id while Connected, else 0.
        int64_t (*sessionId)(void* ctx, uint32_t session) = nullptr;
        /// That session's connect epoch while Connected, else 0 (a reconnect
        /// that got the same broker id is a new epoch).
        uint64_t (*epoch)(void* ctx, uint32_t session) = nullptr;
        /// Offer a frame (already stamped with that session's id) on its node.
        detail::SendResult (*offer)(void* ctx, uint32_t session, const uint8_t* buf, std::size_t len) = nullptr;
        /// A fresh correlationId from the Client's duty-owned counter.
        int64_t (*nextCorrId)(void* ctx) = nullptr;
    };
    void setNodeRouter(const NodeRouter& r) noexcept { router_ = r; }

    /// The PENDING Subscribe `correlationId` belongs to a moved subscription:
    /// its (topicId, bound partition).  False otherwise.
    bool movedPendingPartition(int64_t correlationId, int32_t& topicId, int32_t& partition) const;
    /// Route the pending moved Subscribe `correlationId` to its partition's
    /// leader as the agent now routes it (the caller installed the route the
    /// Redirect named): that node's own session — sent now when Connected,
    /// else held and flushed by driveHomed() — or the seed (stamped `seedSid`)
    /// when the seed's node leads it.  False when not a pending moved one.
    bool homeMoved(int64_t correlationId, int64_t seedSid);
    /// Subscriptions living on a leader node's session (an upper bound,
    /// recounted by every driveHomed pass; the duty cycle calls driveHomed
    /// only while non-zero — one branch otherwise).
    [[nodiscard]] std::size_t homedCount() const noexcept { return homedCount_; }
    /// Duty cycle (while homedCount() != 0): flush held moved Subscribes whose
    /// node session is Connected (re-resolving the leader each attempt); a
    /// moved subscription whose node session was LOST (dropped / reconnected:
    /// the broker ended the subscription with it) is re-armed from its
    /// position under a fresh correlationId (`deadline` = its subscribe
    /// timeout) and routed again — never a silent stall.  Returns work done.
    int driveHomed(int64_t seedSid, std::chrono::steady_clock::time_point deadline);
    /// Moved subscriptions re-armed because their node session was lost.
    [[nodiscard]] uint64_t homedSessionsLost() const noexcept { return homedSessionsLost_; }
    /// Bound subscriptions moved by rearmMovedAway.
    [[nodiscard]] uint64_t subscriptionsMoved() const noexcept { return subscriptionsMoved_; }
    /// E2 final fix wave (I1): batches wholly below a moved subscription's
    /// resume floor (re-deliveries from a new leader whose HW lagged), dropped.
    [[nodiscard]] uint64_t movedDuplicateBatchesDropped() const noexcept { return movedDuplicateBatchesDropped_; }

    /// Spend one bare-NotLeader retry of the pending Subscribe `correlationId`.
    /// True while within `maxRetries` (re-send it); false when the budget is
    /// exhausted or the corr is not pending (caller fails it explicitly).
    bool chargeNotLeaderRetry(int64_t correlationId, int maxRetries);

    /// M2 FIX [CB-5]: Sweep pending subscriptions whose subscribe-request
    /// deadline has expired.  Mirrors the produce-ack timeout sweep.
    /// Called from Client::sweepTimeouts() on the runner thread.
    /// Each still-active timed-out Subscribe finishes with failReason TimedOut;
    /// the return value is how many, so the caller can fire onError for each.
    int sweepTimedOut(const std::chrono::steady_clock::time_point& now);

    /// CB-5 B: resend every PENDING Subscribe frame after a leader redirect +
    /// reconnect.  A Subscribe rejected by a follower with ErrorResponse
    /// {Redirect} was never bound (no SubscribeResponse), so it still lives in
    /// pendingByCorrelation_.  Each stored Subscribe frame is re-stamped with
    /// the NEW leader-assigned sessionId (MessageHeader int64 LE at byte 8) and
    /// resent so the leader — which just assigned a fresh session — accepts it.
    /// Returns the number of Subscribe frames resent.
    int resendPending(int64_t newSessionId,
                      const SendFn& submitFn);

    /// F8a fix1 I4: number of subscriptions whose latest status snapshot a
    /// near-full status ring refused.  The duty cycle calls republishDirty()
    /// whenever it is non-zero (one branch when zero) — progress-based: the
    /// snapshot lands as soon as the app has drained, not on a timer.
    [[nodiscard]] std::size_t dirtyCount() const noexcept { return dirtyCount_; }
    /// Retry every dirty snapshot; returns how many were published.
    int republishDirty();

    /// F8a fix2: duty-thread direct send on the default ingress publication
    /// (plain function pointer + context; set once before start).  Used for
    /// Unsubscribe frames so they never depend on command-ring space.  Unset
    /// (hub-level tests): the SendFn passed to the call is used.
    using DirectSendFn = detail::SendResult (*)(void* ctx, const uint8_t* buf, std::size_t len);
    void setDirectSend(DirectSendFn fn, void* ctx) noexcept { directSend_ = fn; directSendCtx_ = ctx; }

    /// F8a fix2: Unsubscribe frames the transport refused, retried by the duty
    /// cycle (gated: one branch when zero).  A retry for a session that is no
    /// longer the current one is dropped (logged + counted) — the broker ended
    /// the subscription with that session.
    [[nodiscard]] std::size_t pendingUnsubscribes() const noexcept { return pendingUnsubCount_; }
    int retryPendingUnsubscribes(int64_t currentSessionId);
    [[nodiscard]] uint64_t unsubscribesDropped() const noexcept { return unsubscribesDropped_; }

    /// fix3 I4: adoption-time / reconnect Subscribe frames the transport
    /// refused, retried every duty cycle while Connected (gated: one branch
    /// when zero).  On a session change they are re-stamped by resendPending.
    [[nodiscard]] std::size_t unsentSubscribes() const noexcept { return unsentSubscribes_; }
    int retryUnsentSubscribes();

    /// fix3/fix4: session state (duty thread; default connected for hub-level
    /// tests).  The Client updates it AT THE TRANSITION (the ConnectResponse
    /// handler, before any callback of that poll; and before applying any hub
    /// command), so an adoption later in the same poll sees the truth.
    void setSessionConnected(bool connected) noexcept { sessionConnected_ = connected; }
    [[nodiscard]] uint64_t staleResponsesIgnored() const noexcept { return staleResponsesIgnored_; }
    /// fix4 M1: subscriptions failed because their Subscribe frame was Fatal.
    [[nodiscard]] uint64_t subscribesFailed() const noexcept { return subscribesFailed_; }

    // -------------------------------------------------------------------------
    // F8c C7: ownership of the impls (spec §3.3) — duty thread
    // -------------------------------------------------------------------------

    /// The handle released `impl` (its HandleRelease was applied and the
    /// subscription unsubscribed): if no table references it any more and no
    /// handler of it is running, forget it and return true — the caller then
    /// detaches the duty side's share (and frees the impl if the handle
    /// already detached).  False: still referenced; ask again later.
    [[nodiscard]] bool release(SubscriptionImpl* impl) noexcept;

    /// Client close (after deactivateAll): every impl this hub still owns is
    /// handed back — `detach(impl)` detaches the duty side's share (and frees
    /// it if the handle already detached).  The hub holds nothing afterwards.
    template <class Detach>
    void handBackAll(Detach&& detach) noexcept {
        std::vector<SubscriptionImpl*> owned;
        owned.swap(owned_);
        for (SubscriptionImpl* impl : owned) detach(impl);
    }

    /// Finish `impl` locally (inactive, final snapshot, status closed) — no
    /// frame sent (Client close: the transport is going away).
    void finishLocal(SubscriptionImpl& impl) { finish_(impl); }

    /// F8c C7 F1: visit every impl this hub owns (cold: an orphan scan).
    template <class F>
    void forEachOwned(F&& f) {
        for (std::size_t i = 0; i < owned_.size(); ++i) f(*owned_[i]);
    }

    /// Impls this hub owns (adopted, not yet released and forgotten).
    [[nodiscard]] std::size_t ownedCount() const noexcept { return owned_.size(); }

    /// CB-5 B: true if there is at least one pending (unbound) subscription.
    [[nodiscard]] bool hasPending() const;
    /// DR-15: the Subscribe `correlationId` is still pending (no answer bound it).
    [[nodiscard]] bool isPending(int64_t correlationId) const;
    /// DR-15: re-send ONLY that pending Subscribe, re-stamped with `sessionId`, and
    /// restart its deadline (a re-routed request is progressing, not silent).
    /// False when it is no longer pending.
    bool resendOne(int64_t correlationId, int64_t sessionId, const SendFn& submitFn,
                   std::chrono::steady_clock::time_point newDeadline);

    // -------------------------------------------------------------------------
    // Helpers (accessible so SubscriberHub is testable standalone)
    // -------------------------------------------------------------------------

    /// Return the number of pending (unbound) subscriptions.
    [[nodiscard]] std::size_t pendingCount() const;

    /// Return the number of active (bound) subscriptions.
    [[nodiscard]] std::size_t activeCount() const;

    /// fix1 C1: record handlers run inside the noexcept ClientAgent::doWork
    /// chain (onRecords is called from Client::onEgressFrame); a throwing
    /// handler is caught, counted here, and logged instead of terminating the
    /// process. PLAIN counter (fix2 minor-5 — shared-nothing rule, no
    /// atomics): written only on the agent/duty-cycle thread. Only safe to
    /// read once no concurrent writer remains — see
    /// Client::testCallbackExceptionCount()'s doc for the exact contract.
    [[nodiscard]] uint64_t testCallbackExceptionCount() const noexcept {
        return callbackExceptions_;
    }

private:
    // ---- Routing tables (duty-thread owned; no lock) ---------------------------

    // Pending subscriptions: Subscribe sent, SubscribeResponse not yet received.
    // Key: correlationId.  The request keeps the encoded Subscribe frame so it
    // can be re-stamped + resent on a leader redirect (CB-5 B).
    using PendingMap = std::map<int64_t, std::unique_ptr<SubscribeRequest>>;
    PendingMap pendingByCorrelation_;

    // Active subscriptions: SubscribeResponse received, subscriptionId bound.
    // Key: subscriptionId.
    std::map<int64_t, SubscriptionImpl*> activeById_;

    // F8c C7: every impl whose SubscribeAdd this hub adopted (even one already
    // unsubscribed / finished), until release() forgets it or Client close
    // hands it back.  The tables above only reference impls listed here.
    std::vector<SubscriptionImpl*> owned_;

    // fix1 C1 / fix2 minor-5: plain counter, written only on the agent thread.
    uint64_t callbackExceptions_ = 0;

    // F8a fix1 C1(a): the subscription whose handler is running right now
    // (null outside onRecords' dispatch).  Finishing it is deferred: its
    // routing entry is erased and its status ring closed only AFTER the
    // handler returns, so a handler that unsubscribes (or destroys its own
    // Subscription) never frees the std::function it is running, and
    // finished() never turns true while a callback is still executing.
    SubscriptionImpl* dispatching_ = nullptr;

    // F8a fix1 I4: intrusive dirty list (no allocation on the dispatch path).
    SubscriptionImpl* dirtyHead_  = nullptr;
    std::size_t       dirtyCount_ = 0;
    void markDirty_(SubscriptionImpl& impl) noexcept;
    void clearDirty_(SubscriptionImpl& impl) noexcept;

    /// Publish impl's current snapshot (ordinary update; a refusal marks it dirty).
    void publishStatus_(SubscriptionImpl& impl);

    // F8a fix2: direct Unsubscribe send + retry.
    DirectSendFn directSend_    = nullptr;
    void*        directSendCtx_ = nullptr;
    std::size_t  pendingUnsubCount_  = 0;
    uint64_t     unsubscribesDropped_ = 0;
    detail::SendResult trySendUnsubscribe_(const SubscriptionImpl& impl, const SendFn& submitFn);
    detail::SendResult retrySendUnsubscribe_(const SubscriptionImpl& impl);
    // fix3 I4.
    bool        sessionConnected_      = true;
    std::size_t unsentSubscribes_      = 0;
    uint64_t    staleResponsesIgnored_ = 0;
    uint64_t    subscribesFailed_      = 0;  // fix4 M1: Fatal Subscribe offers
    uint64_t    subscriptionsMoved_    = 0;  // E2 Task 13: rearmMovedAway
    uint64_t    movedDuplicateBatchesDropped_ = 0;  // E2 final fix wave (I1)
    // E2 final fix wave (I2): moved subscriptions homed on leader node sessions.
    NodeRouter  router_{};
    std::size_t homedCount_        = 0;
    uint64_t    homedSessionsLost_ = 0;
    std::vector<int64_t> homedScratch_;   // driveHomed: lost bound ones (cold; capacity reused)
    [[nodiscard]] bool routed_() const noexcept { return router_.offer != nullptr; }
    /// The broker session id `impl` lives on: its node session's (homed) or `seedSid`.
    [[nodiscard]] static int64_t homeSidOf_(const SubscriptionImpl& impl, int64_t seedSid) noexcept {
        return impl.homeSession != 0 ? impl.homeSid : seedSid;
    }
    /// Route the pending request at `it` to its partition leader now (see homeMoved).
    void routeMovedPending_(PendingMap::iterator it, int64_t seedSid);
    /// Send a homed pending request on its node session when Connected (held otherwise).
    void sendHomed_(PendingMap::iterator it);
    static void stampSession_(std::vector<uint8_t>& frame, int64_t sid) noexcept;
    detail::SendResult sendSubscribeFrame_(const SubscribeRequest& req);
    /// F8c C3: the Subscribe correlationId of the subscription whose impl is
    /// `key`, found by address in this hub's own tables (0 = not held).
    [[nodiscard]] int64_t correlationIdOf_(const SubscriptionImpl* key) const noexcept;
    /// fix4 M1: fail + erase a pending request whose Subscribe is never deliverable.
    PendingMap::iterator failRequest_(PendingMap::iterator it);
    void markUnsent_(SubscribeRequest& req) noexcept;
    void clearUnsent_(SubscribeRequest& req) noexcept;
    void markUnsubPending_(SubscriptionImpl& impl) noexcept;
    /// Mark impl finished: inactive, final snapshot, status ring closed
    /// (deferred to the end of its dispatch if its handler is running).
    void finish_(SubscriptionImpl& impl);
    /// Publish the final snapshot and close the status ring.
    static void closeStatus_(SubscriptionImpl& impl);
};

} // namespace rune::client::subscriber
