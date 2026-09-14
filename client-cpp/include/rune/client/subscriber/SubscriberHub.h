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
    /// SendFn: (frame, len) -> bool.  Provided by the agent as its transport send.
    using SendFn = std::function<bool(const uint8_t*, std::size_t)>;

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
    ///   submitFn     — calls agent_.submitSend(frame, len)
    void registerSubscribe(std::shared_ptr<SubscriptionImpl> impl,
                           int64_t                          sessionId,
                           int64_t                          correlationId,
                           int32_t                          topicId,
                           int32_t                          partition,
                           uint8_t                          partitionKnown,
                           int64_t                          fromOffset,
                           uint8_t                          isolation,
                           std::span<const uint8_t>         keyBytes,
                           const std::function<bool(const uint8_t*, std::size_t)>& submitFn);

    /// Submit an Unsubscribe frame for a given subscriptionId.
    ///
    /// Called from Subscription::unsubscribe() (any thread).
    /// Marks the impl inactive and writes Unsubscribe to the agent ring.
    /// The routing entry will be cleaned up on the next agent-thread access.
    void sendUnsubscribe(int64_t subscriptionId,
                         int64_t sessionId,
                         int64_t correlationId,
                         const std::function<bool(const uint8_t*, std::size_t)>& submitFn);

    // -------------------------------------------------------------------------
    // Agent-thread callbacks (called from ClientAgent's egress observer)
    // -------------------------------------------------------------------------

    /// Called on the agent thread when a SubscribeResponse arrives.
    ///
    /// Binds the broker-assigned subscriptionId to the pending impl, and moves
    /// the entry from pendingByCorrelation_ to activeById_.
    /// Unknown correlationId → no-op (race with close/unsubscribe; safe).
    void onSubscribeResponse(const codec::SubscribeResponseView& rsp);

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

    // -------------------------------------------------------------------------
    // Helpers (accessible so SubscriberHub is testable standalone)
    // -------------------------------------------------------------------------

    /// Return the number of pending (unbound) subscriptions.
    [[nodiscard]] std::size_t pendingCount() const;

    /// Return the number of active (bound) subscriptions.
    [[nodiscard]] std::size_t activeCount() const;

private:
    // ---- Routing tables (guarded by mtx_) ------------------------------------
    mutable std::mutex mtx_;

    // Pending subscriptions: Subscribe sent, SubscribeResponse not yet received.
    // Key: correlationId.
    std::map<int64_t, std::shared_ptr<SubscriptionImpl>> pendingByCorrelation_;

    // Active subscriptions: SubscribeResponse received, subscriptionId bound.
    // Key: subscriptionId.
    std::map<int64_t, std::shared_ptr<SubscriptionImpl>> activeById_;
};

} // namespace rune::client::subscriber
