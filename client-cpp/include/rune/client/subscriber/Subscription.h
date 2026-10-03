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
//   - Subscription::unsubscribe() and the RAII dtor (F8c C7: a HandleRelease,
//     which unsubscribes) submit a command via the agent command ring — the
//     actual local-routing removal happens on the agent thread when the ring
//     is drained (F8a: no shared state, no atomics, no locks).
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
#include <thread>

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
    int64_t                  timestamp; // producer timestamp, epoch MILLISECONDS (wire contract, Kafka parity)
    std::span<const uint8_t> key;       // zero-copy — valid only in-callback
    std::span<const uint8_t> value;     // zero-copy — valid only in-callback
};

// ---------------------------------------------------------------------------
// RecordHandler — called once per record on the AGENT thread.
//
// Contract:
//   - Must NOT block (sleep, mutex wait, I/O, call Client::close()).
//   - MAY call unsubscribe() (or destroy the Subscription) — on the duty thread
//     the unsubscribe is applied directly: no further record of the batch is
//     delivered, and the routing entry is removed once this handler returns.
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
// F8c C7 ownership (spec §3.3, no shared_ptr): the impl is owned jointly by
// the Subscription handle and the duty side (the SubscriberHub, from the
// SubscribeAdd's adoption until the handle's HandleRelease is applied and no
// hub table references it any more — or until Client close hands it back).
// Each side detaches its share of `status`'s ownership word exactly once; the
// side that detaches LAST frees the impl.  Nobody waits, nobody refcounts.
//
// Neither side ever reads or writes a field the other owns:
//   - immutable after construction (written by the creating app thread BEFORE
//     the SubscribeAdd command is put on the agent ring, whose release/acquire
//     publishes them): topicId, partition, handler;
//   - correlationId: F8c C3 — assigned by the duty thread when it adopts the
//     SubscribeAdd (the duty-owned correlation counter), never read by an app
//     thread (an app-side Unsubscribe identifies the impl by address);
//   - duty-thread owned (plain fields, never touched by an app thread):
//     subscriptionId, nextOffset, active, statusDirty, the dirty-list links,
//     the pending-Unsubscribe fields;
//   - the cross-thread channel: `status` (StatusRing — the ring's own memory
//     ordering is the only synchronisation; its single consumer is the
//     Subscription handle).
// ---------------------------------------------------------------------------
struct SubscriptionImpl {
    // ---- Immutable after construction -----------------------------------------
    int64_t       correlationId = 0;   // the Subscribe correlationId (duty-assigned at adoption)
    int32_t       topicId       = 0;
    int32_t       partition     = 0;
    RecordHandler handler;             // always valid (non-null after subscribe())

    // ---- Duty-thread owned ------------------------------------------------------
    int64_t subscriptionId = 0;  // bound by SubscribeResponse (0 = pending)
    int64_t nextOffset     = 0;  // lastDelivered + 1 (CB-5 resume point)
    bool    active         = true;  // false once unsubscribed / timed out / closed
    bool    statusDirty    = false; // a snapshot newer than the last published one exists
    // F8a fix2: an Unsubscribe the transport refused, retried by the duty cycle.
    bool    unsubPending   = false;
    int64_t unsubCorrId    = 0;
    int64_t unsubSessionId = 0;
    // Intrusive links of the hub's dirty list (no allocation on the dispatch path).
    SubscriptionImpl* prevDirty = nullptr;
    SubscriptionImpl* nextDirty = nullptr;

    // ---- F8c C7 fix round 2 (F1): orphaned release -------------------------------
    int64_t orphanFence  = 0;      // written by the handle BEFORE its orphan RMW (published by it)
    bool    releaseNoted = false;  // duty thread: its release was applied / queued

    // ---- Duty thread -> app handle ------------------------------------------------
    detail::StatusRing<SubscriptionStatus> status;

    SubscriptionImpl() = default;
    // F8c C7: freed by whichever of {the handle, the duty side} detaches its
    // share of `status`'s ownership word last (see Subscription).
    ~SubscriptionImpl() {
        if (detail::testOnImplFree != nullptr) detail::testOnImplFree(detail::HandleKind::Subscription, this);
    }

    // Non-copyable, non-movable (raw single-owner hand-off; the ring is pinned).
    SubscriptionImpl(const SubscriptionImpl&) = delete;
    SubscriptionImpl& operator=(const SubscriptionImpl&) = delete;
    SubscriptionImpl(SubscriptionImpl&&) = delete;
    SubscriptionImpl& operator=(SubscriptionImpl&&) = delete;
};

// ---------------------------------------------------------------------------
// Subscription — user-facing RAII handle returned by Client::subscribe().
//
// Threading (F8a):
//   - The handle is OWNED by one app thread at a time (it is move-only).  Its
//     app-side methods must be called by that owner — never concurrently from
//     two app threads on the same handle.
//   - Calls from INSIDE a callback (i.e. on the duty thread, Thread mode) are
//     allowed: they act directly on duty-owned state and never touch the
//     handle's app-side cache.  In Invoker mode the owner thread is the duty
//     thread and every call is the owner's.
//   - unsubscribe() hands an Unsubscribe command to the duty thread (applied
//     directly when already on it).  Delivery is GUARANTEED: a full command
//     ring is waited out on duty-thread progress, never a timeout, never a
//     throw.  The destructor (F8c C7) hands over the handle's share of the
//     impl in one HandleRelease command, which unsubscribes (applied directly
//     on the duty thread, through the command ring elsewhere — orphaned and
//     applied by the duty side in FIFO order when the ring is full), and the
//     impl is freed by whichever side lets go last — never
//     while one of its handlers runs.
//   - position() / subscriptionId() drain the subscription's status ring and
//     answer from the handle's own cache (eventually consistent).
//   - finished(): the duty side is done with this subscription.  Once it
//     returns true, NO further handler call will run and position() is final.
//     Resume pattern: unsubscribe(); wait until finished(); then read
//     position() and re-subscribe with From::offset(position()).
//   - Once finished, the handle never touches the Client again — a handle may
//     safely outlive its Client (but must not be used CONCURRENTLY with the
//     Client's destruction).
//   - HANDLE LIFETIME (fix4 M6): the handle must NOT be destroyed, moved from
//     or moved into while ANY thread may still be calling it — its owner app
//     thread, or a runner callback (e.g. a record handler of another
//     subscription that unsubscribes this one).  The direct (duty-thread) path
//     acts on the impl through the caller's handle for the whole call; a
//     concurrent destroy/move is a data race on the handle itself.  Hand a
//     handle to a callback only if it outlives every callback that may use it
//     (e.g. unsubscribe it, wait for finished() of the callback's own
//     subscription, then destroy it).
//
// Records start arriving once the broker acknowledges (SubscribeResponse).
// There is no wait for the ack in subscribe() itself; the Subscription is
// returned immediately and records flow once the ack arrives.
// ---------------------------------------------------------------------------
class Subscription {
public:
    // ---- Construction (called by Client::subscribe()) -----------------------

    /// Constructed by Client::subscribe(): takes the handle's share of `impl`.
    /// `sink`/`sinkCtx` deliver commands to the duty thread (nullptr: a
    /// standalone handle — no duty side, the handle alone frees the impl).
    /// `dutyThread`: the Thread-mode runner (calls from it take the duty
    /// path); a default id (Invoker mode, tests) disables that.
    Subscription(SubscriptionImpl*      impl,
                 detail::HubCommandSink sink,
                 void*                  sinkCtx,
                 std::thread::id        dutyThread = {}) noexcept;

    /// F8c C7 release (§3.3): unsubscribes (the duty side applies it) and
    /// gives the handle's share of the impl to the duty side.  Never throws,
    /// never waits (a full command ring: the release is orphaned and the duty
    /// side applies it in FIFO order — see ~Producer).  After Client
    /// close the impl was handed back: the handle frees it, never touching
    /// the Client.
    ~Subscription();

    // Move-only (unique ownership of the handle's share).
    Subscription(const Subscription&) = delete;
    Subscription& operator=(const Subscription&) = delete;

    Subscription(Subscription&&) noexcept;
    Subscription& operator=(Subscription&&) noexcept;

    // ---- API ----------------------------------------------------------------

    /// Unsubscribe (Unsubscribe frame + stop local dispatch).  Idempotent.
    /// After it returns, active() is false; the handler may still fire for
    /// records the duty thread dispatches before it applies the command — wait
    /// for finished() to know it no longer can.  Never throws for a full ring.
    void unsubscribe();

    /// Next expected offset (lastDelivered + 1); 0 before any record.  Final
    /// once finished() is true.
    [[nodiscard]] int64_t position() const;

    /// True until unsubscribe(), or until the duty side finished the
    /// subscription (unsubscribed, subscribe timed out / failed, Client closed).
    [[nodiscard]] bool active() const;

    /// True once the duty side finished this subscription: no further handler
    /// call will run, and position() is final.
    [[nodiscard]] bool finished() const noexcept;

    /// The subscriptionId assigned by the broker (0 until the SubscribeResponse
    /// has been processed — records cannot arrive before then).
    [[nodiscard]] int64_t subscriptionId() const;

private:
    [[nodiscard]] bool onDutyThread_() const noexcept {
        return dutyThread_ != std::thread::id{} && std::this_thread::get_id() == dutyThread_;
    }
    void drainStatus_() const;
    /// F8c C7: the §3.3 release of the handle's share (see the destructor).
    void release_() noexcept;

    SubscriptionImpl*                 impl_    = nullptr;  // the handle's share (§3.3)
    detail::HubCommandSink            sink_    = nullptr;
    void*                             sinkCtx_ = nullptr;
    std::thread::id                   dutyThread_{};
    bool                              unsubscribed_ = false;  // handle-local (app-owned)
    // App-owned cache of the latest status snapshot drained from impl_->status.
    mutable SubscriptionStatus        cache_{};
};

} // namespace rune::client
