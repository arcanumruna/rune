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
//   - Subscription::unsubscribe() and the RAII dtor are safe to call from any
//     thread (including the app thread).  Internally they submit an Unsubscribe
//     command via the agent ring — the actual local-routing removal happens on
//     the agent thread when the ring is drained.
//   - position() is updated on the agent thread immediately after each record
//     is delivered to the handler.  Reads from other threads are eventually
//     consistent (atomic acquire load); the value is always >= the last
//     delivered offset.

#pragma once

#include <atomic>
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
// SubscriptionImpl — internal shared state for Subscription.
//
// Held by the Subscription handle (app thread) and the SubscriberHub routing
// table (agent thread).  Thread-safe fields documented per-member.
// ---------------------------------------------------------------------------
struct SubscriptionImpl {
    // Immutable after construction (set up before any agent access).
    int64_t     correlationId = 0;   // the Subscribe correlationId
    int32_t     topicId       = 0;
    int32_t     partition     = 0;
    RecordHandler handler;           // always valid (non-null after subscribe())

    // subscriptionId: assigned by SubscribeResponse (0 = pending).
    // Written once on the agent thread; read on any thread.
    std::atomic<int64_t> subscriptionId{0};

    // ---- Mutable (agent thread writes; any thread may read) ------------------

    // nextOffset: the offset of the NEXT record we expect.
    //   Initialised to 0; updated after each delivered record to
    //   (lastDeliveredOffset + 1).  Atomically stored by the agent thread;
    //   any thread may read (acquire load → eventually consistent).
    //
    // For CB-5 reconnect resume: pass nextOffset as the fromOffset in the
    // re-subscribe after reconnect.
    std::atomic<int64_t> nextOffset{0};

    // active: cleared (release store) by unsubscribe(); the hub checks this
    // before dispatching and drops late Records for inactive subscriptions.
    // Also cleared by the SubscriberHub when an Unsubscribe response arrives
    // or when the client is closed.
    std::atomic<bool> active{true};

    SubscriptionImpl() = default;

    // Non-copyable, non-movable (owned by shared_ptr; atomics cannot copy).
    SubscriptionImpl(const SubscriptionImpl&) = delete;
    SubscriptionImpl& operator=(const SubscriptionImpl&) = delete;
    SubscriptionImpl(SubscriptionImpl&&) = delete;
    SubscriptionImpl& operator=(SubscriptionImpl&&) = delete;
};

// ---------------------------------------------------------------------------
// Subscription — user-facing RAII handle returned by Client::subscribe().
//
// Threading:
//   - unsubscribe() is safe to call from any thread.
//   - position() is safe to call from any thread (atomic acquire load).
//   - Dtor calls unsubscribe() if still active — safe from any thread.
//   - Do NOT call unsubscribe() from within the RecordHandler (that would
//     attempt a ring write from the agent thread, which owns the ring drain;
//     this would be a no-op lock contention at best, or deadlock at worst).
//     Unsubscribe from the app thread.
//
// Records start arriving once the broker acknowledges (SubscribeResponse).
// There is no wait for the ack in subscribe() itself; the Subscription is
// returned immediately and records flow once the ack arrives.
// ---------------------------------------------------------------------------
class Subscription {
public:
    // ---- Construction (called by Client::subscribe()) -----------------------

    // Constructed by Client::subscribe().  Not intended for direct use.
    explicit Subscription(std::shared_ptr<SubscriptionImpl> impl,
                          std::function<void(int64_t /*subscriptionId*/)> unsubFn) noexcept;

    // RAII: dtor calls unsubscribe() if still active.
    ~Subscription();

    // Non-copyable (unique ownership of the unsubscribe side-effect).
    // Movable so Client::subscribe() can return it by value.
    Subscription(const Subscription&) = delete;
    Subscription& operator=(const Subscription&) = delete;

    Subscription(Subscription&&) noexcept;
    Subscription& operator=(Subscription&&) noexcept;

    // ---- API ----------------------------------------------------------------

    /// Send Unsubscribe to the broker and stop local dispatch.
    ///
    /// Idempotent: safe to call multiple times (subsequent calls are no-ops).
    /// Thread-safe: safe to call from the app thread concurrently with records
    /// being dispatched on the agent thread.
    ///
    /// After unsubscribe() returns, the handler may still fire once or twice
    /// for records already in-flight on the agent thread.  The SubscriberHub
    /// performs the final active-check on the agent thread before dispatching;
    /// records arriving after the Unsubscribe is processed on the agent thread
    /// are silently dropped.
    void unsubscribe();

    /// Return the next expected offset (lastDelivered + 1).
    ///
    /// Thread-safe (atomic acquire load; eventually consistent).
    ///
    /// Semantics: if no records have been delivered yet, returns 0.
    ///   After the first record at offset O is delivered, returns O+1.
    ///   This value is the resume point: re-subscribe with From::offset(position())
    ///   to receive records from exactly where this subscription left off.
    ///   (Full reconnect-resume is implemented in CB-5.)
    [[nodiscard]] int64_t position() const noexcept;

    /// Return true if the subscription is still active (not yet unsubscribed).
    [[nodiscard]] bool active() const noexcept;

    /// Return the subscriptionId assigned by the broker (0 if the
    /// SubscribeResponse has not yet arrived — records cannot arrive before
    /// this is set, so a 0 here means no records have been dispatched yet).
    [[nodiscard]] int64_t subscriptionId() const noexcept;

private:
    std::shared_ptr<SubscriptionImpl>              impl_;
    std::function<void(int64_t /*subscriptionId*/)> unsubFn_;
    bool                                            moved_ = false;
};

} // namespace rune::client
