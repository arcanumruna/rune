// include/rune/client/subscriber/SubscriberHub.h
//
// CB-4 — SubscriberHub: manages all active subscriptions.
//
// Threading model:
//   - The routing table (pendingByCorrelation_ + activeById_) is guarded by
//     mtx_ — a control-plane mutex.  It is NEVER held during record dispatch;
//     the dispatch hot path is lock-free (the impl's atomic active flag is
//     checked before calling the handler).
//
//   - subscribe() [app thread]: encodes the Subscribe frame, registers
//     the impl keyed by correlationId (under mtx_), submits via the agent
//     ring, and returns the Subscription immediately.  Records begin flowing
//     once the SubscribeResponse binds the subscriptionId.
//
//   - onSubscribeResponse() [agent thread]: moves the pending entry from
//     pendingByCorrelation_ to activeById_, storing the broker-assigned
//     subscriptionId.
//
//   - onRecords() [agent thread]: looks up the Subscription by subscriptionId,
//     checks active flag, decodes the RecordBatch via RecordBatchReader, and
//     calls the handler once per record.  Updates nextOffset after each record.
//     Lock is NOT held during handler dispatch.
//
//   - unsubscribe() [app/any thread]: sends Unsubscribe frame; the hub
//     removes the entry from activeById_ on the agent thread when the frame
//     has been processed (or on the next Records look-up for an inactive sub).
//
// Design choice (from the brief): routing table lives under a mutex (control-
// plane).  The record-dispatch hot path does NOT take the mutex; it copies the
// shared_ptr out under lock then drops the lock before calling the handler.

#pragma once

#include <rune/client/subscriber/Subscription.h>
#include <rune/client/codec/DataCodec.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace rune::client::subscriber {

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
    // App-thread API
    // -------------------------------------------------------------------------

    /// Build and submit a Subscribe message.
    ///
    /// Encodes the Subscribe frame, registers the impl under correlationId
    /// (mutex-guarded), and writes the frame to the agent ring via submitFn.
    ///
    /// Returns the Subscription handle immediately.  The broker-assigned
    /// subscriptionId will be bound asynchronously when SubscribeResponse arrives.
    ///
    /// Parameters:
    ///   impl         — shared subscription state (created by Client::subscribe())
    ///   sessionId    — current session id
    ///   correlationId— unique id for this subscribe request
    ///   topicId      — topic id
    ///   partition    — partition number
    ///   fromOffset   — wire-encoded From value (see From::toWire())
    ///   isolation    — isolation byte
    ///   keyBytes     — key bytes (zero-copy span; valid only during this call)
    ///   partitionKnown — 1 if partition is explicit, 0 if resolved from key
    ///   deadline     — subscribe-request timeout (M2: sweepTimedOut fails it if no response)
    ///   submitFn     — calls agent_.submitSend(frame, len)
    void registerSubscribe(std::shared_ptr<SubscriptionImpl>             impl,
                           int64_t                                       sessionId,
                           int64_t                                       correlationId,
                           int32_t                                       topicId,
                           int32_t                                       partition,
                           uint8_t                                       partitionKnown,
                           int64_t                                       fromOffset,
                           uint8_t                                       isolation,
                           std::span<const uint8_t>                      keyBytes,
                           std::chrono::steady_clock::time_point         deadline,
                           const SendFn&                                  submitFn);

    /// Submit an Unsubscribe frame for a given subscriptionId.
    ///
    /// Called from Subscription::unsubscribe() (any thread).
    /// Marks the impl inactive and writes Unsubscribe to the agent ring.
    /// The routing entry will be cleaned up on the next agent-thread access.
    void sendUnsubscribe(int64_t subscriptionId,
                         int64_t sessionId,
                         int64_t correlationId,
                         const SendFn& submitFn);

    // -------------------------------------------------------------------------
    // Agent-thread callbacks (called from ClientAgent's egress observer)
    // -------------------------------------------------------------------------

    /// Called on the agent thread when a SubscribeResponse arrives.
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
    void onRecords(const codec::RecordsView& rv);

    /// Fail / deactivate all active subscriptions (called on close).
    /// Marks all impls inactive.  No Unsubscribe frames are sent (transport
    /// is shutting down anyway).
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

    /// M2 FIX [CB-5]: Sweep pending subscriptions whose subscribe-request
    /// deadline has expired.  Mirrors the produce-ack timeout sweep.
    /// Called from Client::sweepTimeouts() on the runner thread.
    void sweepTimedOut(const std::chrono::steady_clock::time_point& now);

    /// CB-5 B: resend every PENDING Subscribe frame after a leader redirect +
    /// reconnect.  A Subscribe rejected by a follower with ErrorResponse
    /// {Redirect} was never bound (no SubscribeResponse), so it still lives in
    /// pendingByCorrelation_.  Each stored Subscribe frame is re-stamped with
    /// the NEW leader-assigned sessionId (MessageHeader int64 LE at byte 8) and
    /// resent so the leader — which just assigned a fresh session — accepts it.
    /// Returns the number of Subscribe frames resent.
    int resendPending(int64_t newSessionId,
                      const SendFn& submitFn);

    /// CB-5 B: true if there is at least one pending (unbound) subscription.
    [[nodiscard]] bool hasPending() const;

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
    /// process. A relaxed atomic (not the single-writer plain counters
    /// elsewhere in this class): a test thread polling this has no other way
    /// to establish happens-before with the exception that is itself the
    /// event being waited for.
    [[nodiscard]] uint64_t testCallbackExceptionCount() const noexcept {
        return callbackExceptions_.load(std::memory_order_relaxed);
    }

private:
    // ---- Routing tables (guarded by mtx_) ------------------------------------
    mutable std::mutex mtx_;

    // Pending subscriptions: Subscribe sent, SubscribeResponse not yet received.
    // Key: correlationId.  Stores the impl plus the encoded Subscribe frame so
    // the frame can be re-stamped + resent on a leader redirect (CB-5 B).
    struct PendingSub {
        std::shared_ptr<SubscriptionImpl>     impl;
        std::vector<uint8_t>                  subscribeFrame; // encoded Subscribe (for redirect resend)
        std::chrono::steady_clock::time_point deadline;       // M2: subscribe-request timeout
    };
    std::map<int64_t, PendingSub> pendingByCorrelation_;

    // Active subscriptions: SubscribeResponse received, subscriptionId bound.
    // Key: subscriptionId.
    std::map<int64_t, std::shared_ptr<SubscriptionImpl>> activeById_;

    // fix1 C1: written on the agent thread (onRecords), read from a test thread.
    std::atomic<uint64_t> callbackExceptions_ {0};
};

} // namespace rune::client::subscriber
