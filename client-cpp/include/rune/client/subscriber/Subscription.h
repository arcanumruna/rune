// include/rune/client/subscriber/Subscription.h
//
// CB-4 — Subscribe surface: Subscription handle + SubscribeOptions.
// Namespace: rune::client
//
// Threading contract:
//   - The RecordHandler fires on the AGENT THREAD (the runner thread that owns
//     the doWork() duty cycle).  The handler must NOT block the agent thread —
//     doing so delays all subsequent record dispatch and keepalive ticks.
//   - The record spans (Record::key, Record::value) are ZERO-COPY views into
//     the Aeron fragment buffer.  They are valid ONLY during the callback; the
//     handler must copy any data it needs to retain beyond the callback.
//   - Subscription::unsubscribe() and the RAII dtor submit an Unsubscribe
//     command via the agent command ring — the actual local-routing removal
//     happens on the agent thread when the ring is drained (F8a: no shared
//     state, no atomics, no locks).
//   - position() is advanced on the agent thread after each delivered Records
//     frame and PUBLISHED to the handle through a per-subscription status ring;
//     reads are eventually consistent.

#pragma once

#include <rune/client/detail/HubCommand.h>
#include <rune/client/detail/StatusRing.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <span>

namespace rune::client {

// ---------------------------------------------------------------------------
// From — determines which offset the subscription starts delivering from.
// ---------------------------------------------------------------------------
struct From {
    enum class Kind : uint8_t {
        Earliest = 0,  // deliver from offset 0 (beginning of log)
        Latest   = 1,  // deliver only records produced AFTER subscribe (LEO)
        Offset   = 2,  // deliver from an explicit offset (resume / mid-stream)
    };

    Kind    kind      = Kind::Latest;
    int64_t startAt   = 0; // meaningful only when kind == Offset

    // Factories.
    static From earliest() noexcept { return {Kind::Earliest, 0}; }
    static From latest()   noexcept { return {Kind::Latest,   0}; }
    static From offset(int64_t o) noexcept { return {Kind::Offset, o}; }

    // Wire encoding: maps Kind → int64_t fromOffset for the Subscribe message.
    //   Earliest → -2  (broker convention; matches Kafka semantics)
    //   Latest   → -1  (LEO sentinel)
    //   Offset   → startAt (explicit offset)
    [[nodiscard]] int64_t toWire() const noexcept {
        switch (kind) {
        case Kind::Earliest: return -2;
        case Kind::Latest:   return -1;
        case Kind::Offset:   return startAt;
        }
        return -1; // unreachable
    }
};

// ---------------------------------------------------------------------------
// Isolation — read-isolation level.
// Milestone-B: both values are treated as ReadUncommitted on the broker side
// (LEO is the only read frontier).  Passed through verbatim so the wire is
// correct for future milestone support.
// ---------------------------------------------------------------------------
enum class Isolation : uint8_t {
    ReadUncommitted = 0,
    ReadCommitted   = 1,
};

// ---------------------------------------------------------------------------
// SubscribeOptions — parameters for Client::subscribe().
// ---------------------------------------------------------------------------
struct SubscribeOptions {
    From      from      = From::latest();
    Isolation isolation = Isolation::ReadUncommitted;
};

// ---------------------------------------------------------------------------
// Record — a zero-copy view of a single delivered record.
//
// IMPORTANT: key and value spans are views into the Aeron fragment buffer.
// They are valid ONLY during the RecordHandler callback.  Copy any data you
// need to retain beyond the callback.
// ---------------------------------------------------------------------------
struct Record {
    int64_t                  offset;    // absolute offset in the partition log
    int64_t                  timestamp; // producer timestamp (nanoseconds)
    std::span<const uint8_t> key;       // zero-copy — valid only in-callback
    std::span<const uint8_t> value;     // zero-copy — valid only in-callback
};

// ---------------------------------------------------------------------------
// RecordHandler — called once per record on the AGENT thread.
//
// Contract:
//   - Must NOT block (sleep, mutex wait, I/O, call close()).
//   - Must NOT call unsubscribe() from within the handler (that would attempt
//     a ring write from the agent thread — deadlock).  Unsubscribe from the
//     app thread instead.
//   - The record's key/value spans are valid only during this call.
// ---------------------------------------------------------------------------
using RecordHandler = std::function<void(const Record&)>;

// ---------------------------------------------------------------------------
// SubscriptionStatus — one snapshot of the duty-thread-owned subscription state,
// published to the app-side handle through the subscription's StatusRing.
// ---------------------------------------------------------------------------
struct SubscriptionStatus {
    int64_t subscriptionId = 0;  // broker-assigned id (0 = SubscribeResponse not yet seen)
    int64_t nextOffset     = 0;  // resume position (lastDelivered + 1; 0 = nothing delivered)
};

// ---------------------------------------------------------------------------
// SubscriptionImpl — internal state for one subscription (F8a: shared-nothing).
//
// Held by the Subscription handle (app thread) and the SubscriberHub routing
// table (duty thread), but NO field is ever read or written by both:
//   - immutable after construction (written by the creating app thread BEFORE
//     the SubscribeAdd command is put on the agent ring, whose release/acquire
//     publishes them): correlationId, topicId, partition, handler;
//   - duty-thread owned (plain fields, never touched by an app thread):
//     subscriptionId, nextOffset, active, statusDirty;
//   - the cross-thread channel: `status` (StatusRing — the ring's own memory
//     ordering is the only synchronisation; its single consumer is the
//     Subscription handle).
// ---------------------------------------------------------------------------
struct SubscriptionImpl {
    // ---- Immutable after construction -----------------------------------------
    int64_t       correlationId = 0;   // the Subscribe correlationId (subscription identity)
    int32_t       topicId       = 0;
    int32_t       partition     = 0;
    RecordHandler handler;             // always valid (non-null after subscribe())

    // ---- Duty-thread owned ------------------------------------------------------
    int64_t subscriptionId = 0;  // bound by SubscribeResponse (0 = pending)
    int64_t nextOffset     = 0;  // lastDelivered + 1 (CB-5 resume point)
    bool    active         = true;  // false once unsubscribed / timed out / closed
    bool    statusDirty    = false; // a snapshot newer than the last published one exists

    // ---- Duty thread -> app handle ------------------------------------------------
    detail::StatusRing<SubscriptionStatus> status;

    SubscriptionImpl() = default;

    // Non-copyable, non-movable (owned by shared_ptr; the ring is pinned).
    SubscriptionImpl(const SubscriptionImpl&) = delete;
    SubscriptionImpl& operator=(const SubscriptionImpl&) = delete;
    SubscriptionImpl(SubscriptionImpl&&) = delete;
    SubscriptionImpl& operator=(SubscriptionImpl&&) = delete;
};

// ---------------------------------------------------------------------------
// Subscription — user-facing RAII handle returned by Client::subscribe().
//
// Threading (F8a):
//   - The handle is OWNED by one app thread at a time (it is move-only).  All
//     of its methods must be called by that owner — never concurrently from
//     two threads on the same handle.
//   - unsubscribe() / the dtor hand an Unsubscribe command to the duty thread
//     through the agent command ring; the duty thread removes the routing
//     entry and sends the Unsubscribe frame.  No shared state is touched.
//   - position() / subscriptionId() / active() drain the subscription's status
//     ring and answer from the handle's own cache (eventually consistent: the
//     duty thread publishes after each delivered Records frame and on every
//     lifecycle change).
//   - Do NOT call unsubscribe() from within the RecordHandler.  Unsubscribe
//     from the app thread.
//   - Once the subscription is finished on the duty side (unsubscribed, timed
//     out, or the Client closed), the handle never touches the Client again —
//     a handle may safely outlive its Client.
//
// Records start arriving once the broker acknowledges (SubscribeResponse).
// There is no wait for the ack in subscribe() itself; the Subscription is
// returned immediately and records flow once the ack arrives.
// ---------------------------------------------------------------------------
class Subscription {
public:
    // ---- Construction (called by Client::subscribe()) -----------------------

    /// Constructed by Client::subscribe().  `sink`/`sinkCtx` deliver the
    /// Unsubscribe command to the duty thread (the Client's command ring).
    Subscription(std::shared_ptr<SubscriptionImpl> impl,
                 detail::HubCommandSink            sink,
                 void*                             sinkCtx) noexcept;

    // RAII: dtor calls unsubscribe() if still active (never throws).
    ~Subscription();

    // Non-copyable (unique ownership of the unsubscribe side-effect).
    // Movable so Client::subscribe() can return it by value.
    Subscription(const Subscription&) = delete;
    Subscription& operator=(const Subscription&) = delete;

    Subscription(Subscription&&) noexcept;
    Subscription& operator=(Subscription&&) noexcept;

    // ---- API ----------------------------------------------------------------

    /// Ask the duty thread to unsubscribe (Unsubscribe frame + stop local
    /// dispatch).  Idempotent.  After it returns, active() is false; the
    /// handler may still fire for records already being dispatched on the duty
    /// thread before it applies the command.
    /// Throws only if the command ring stays full past the bounded wait (see
    /// Client::producer()); the destructor swallows (and logs) that.
    void unsubscribe();

    /// Return the next expected offset (lastDelivered + 1); 0 before any record.
    /// The resume point: re-subscribe with From::offset(position()).
    [[nodiscard]] int64_t position() const;

    /// True until unsubscribe(), or until the duty thread finished the
    /// subscription (subscribe timed out / failed, or the Client closed).
    [[nodiscard]] bool active() const;

    /// The subscriptionId assigned by the broker (0 until the SubscribeResponse
    /// has been processed — records cannot arrive before then).
    [[nodiscard]] int64_t subscriptionId() const;

private:
    void drainStatus_() const;

    std::shared_ptr<SubscriptionImpl> impl_;
    detail::HubCommandSink            sink_    = nullptr;
    void*                             sinkCtx_ = nullptr;
    bool                              moved_        = false;
    bool                              unsubscribed_ = false;  // handle-local (app-owned)
    // App-owned cache of the latest status snapshot drained from impl_->status.
    mutable SubscriptionStatus        cache_{};
};

} // namespace rune::client
