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
    /// Catalog history (Task 13): a subscription of TopicHandle::catalog() only.
    /// The serving broker pushes ONE caught-up marker after the committed backlog
    /// — Records{baseOffset = its high watermark, the 8-byte HISTORY EPOCH}
    /// (DataMessages.h, client version 8).  It fires this, on the RUNNER thread
    /// (must not block), with that high watermark and epoch.  Ignored for every
    /// other topic.
    std::function<void(int64_t highWatermark, uint64_t historyEpoch)> onCaughtUp;
    /// Catalog history (Task 13 fix round 2): TopicHandle::catalog() only.  Every
    /// catalog stream OPENS with the broker's history-epoch note (Records{-1, the
    /// epoch}, before any record of it): every record that follows belongs to
    /// the committed history it names.  Runner thread; must not block.
    std::function<void(uint64_t historyEpoch)> onHistoryEpoch;
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
/// Why a subscription finished without being unsubscribed (Subscription::failReason()).
enum class SubscriptionFailReason : int32_t {
    None = 0,
    /// MEDIUM-3: the partition is led by another node while a consumer group is
    /// active on this Client.  Plain subscriptions are sent on the seed session,
    /// which is the group coordinator's and is never moved to follow a
    /// redirect then; subscribe on a Client without a group consumer (or wait
    /// for per-partition routing of plain subscriptions).
    LeaderElsewhereWhileGroupActive = 1,
    /// No-hang wave: the broker answered the Subscribe with an error
    /// (SubscribeResponse.errorCode != 0 — UnknownTopic, UnknownPartition,
    /// InvalidRequest, ...).  Nothing was bound broker-side; the code is
    /// Subscription::brokerErrorCode().  (Such an answer used to BIND the
    /// subscription with id 0 — active, never delivering: a silent hang.)
    RejectedByBroker = 2,
    /// Silent-failure gap: the Subscribe got no SubscribeResponse within
    /// subscribeTimeoutMs (it used to finish with None and no onError).
    TimedOut = 3,
    /// E2 FU1 (seed-session check): the subscription was bound on the SEED
    /// session and that session was lost (a resume always gets a new broker
    /// session, and the broker ends a session's subscriptions with it).  It is
    /// ended explicitly — never left active and silent.  Subscribe again
    /// (from position()) to continue.
    SessionLost = 4,
    /// Catalog history (Task 13): a CATALOG subscription (TopicHandle::catalog())
    /// the broker ended with the retriable NotLeader — its replica truncated
    /// below a high watermark it had already served (an unclean / deficient
    /// leader upstream).  What this subscription delivered may no longer be
    /// the committed history: it is never resumed silently from position();
    /// re-read from 0 and re-validate (rune::client::CatalogReader does).
    CatalogReadEnded = 5,
    /// Authentication (client version 9): the broker refused the Subscribe — or ended the live subscription when
    /// its READ grant was revoked — with NotAuthorized (45), on the Subscribe's correlation id or in the
    /// SubscribeResponse.  Never re-armed: a grant is an admin's act.  brokerErrorCode() is 45.
    NotAuthorized = 6,
    /// Authentication (D26): the session's user was dropped / disabled (SessionRevoked, 46) — on this
    /// subscription's correlation id, or as the session-level notice that ends everything of the session.
    /// The session is closed and not reconnected.  brokerErrorCode() is 46.
    SessionRevoked = 7,
    /// C-F21: the broker cannot deliver the subscription's next batch (it is
    /// larger than the egress max message or the broker's return ring —
    /// ErrorResponse{MessageTooLarge} naming this subscription) and parked it;
    /// the client released it.  Re-subscribing from the same offset meets the
    /// same batch: raise the broker's / client's term (max message) size.
    UndeliverableBatch = 8,
};

struct SubscriptionStatus {
    int64_t subscriptionId = 0;  // broker-assigned id (0 = SubscribeResponse not yet seen)
    int64_t nextOffset     = 0;  // resume position (lastDelivered + 1; 0 = nothing delivered)
    SubscriptionFailReason failReason = SubscriptionFailReason::None;  // final snapshot only
    uint16_t brokerErrorCode = 0;  // RejectedByBroker: the SubscribeResponse errorCode (final snapshot only)
    uint32_t dataResets      = 0;  // E2 round 6: times the partition's data was reset under it (see dataResets())
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
//     publishes them): topicId, partition, handler, onCaughtUp, onHistoryEpoch;
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
    // correlationId: duty-assigned at adoption; re-assigned (duty thread) when a
    // bound subscription is moved to the partition's new leader
    // (SubscriberHub::rearmMovedAway) — the duty side's identity, never the handle's.
    int64_t       correlationId = 0;   // the Subscribe correlationId
    int32_t       topicId       = 0;
    int32_t       partition     = 0;
    int64_t       fromWire      = -1;  // the requested From (From::toWire): the resume point while nothing was delivered
    uint8_t       isolation     = 0;   // the requested Isolation (wire byte)
    RecordHandler handler;             // always valid (non-null after subscribe())
    std::function<void(int64_t, uint64_t)> onCaughtUp;   // catalog subscriptions only (may be empty)
    std::function<void(uint64_t)> onHistoryEpoch;        // catalog subscriptions only (may be empty)

    // ---- Duty-thread owned ------------------------------------------------------
    int64_t subscriptionId = 0;  // bound by SubscribeResponse (0 = pending)
    int32_t boundPartition = -1; // the partition the SubscribeResponse resolved
    SubscriptionFailReason failReason = SubscriptionFailReason::None;  // set before a failing finish
    uint16_t brokerErrorCode = 0;  // RejectedByBroker: the broker's error code
    int64_t nextOffset     = 0;  // lastDelivered + 1 (CB-5 resume point)
    // E2 Task 13: the resume position of a moved subscription (rearmMovedAway).
    // Records below it were delivered before the move: dropped (a straddling
    // batch's lower records, whole batches below it while the new leader's HW
    // lags).  Sticky until a batch carrying a record at/above it arrives
    // (E2 final fix wave, I1); -1 = none.
    int64_t resumeFloor    = -1;
    // E2 Task 13 fix round 2: the broker's POSITION NOTE (an empty-batch
    // Records frame sent before it answers a moved subscription NotLeader —
    // client version 6): its cursor.  A subscription that delivered nothing
    // resumes there (never re-resolves Latest).  -1 = none.
    int64_t resumeHint     = -1;
    // E2 final fix wave (I2): WHERE the subscription lives.  0 = the SEED
    // session (every new subscription).  A MOVED subscription (rearmMovedAway)
    // answered Redirect follows its partition's leader on that node's OWN
    // session (homeSession = the agent's session index), as produce and group
    // fetch do — the seed is never retargeted by it, so the other subscriptions
    // bound on the seed stay alive.  homeSid / homeEpoch: that session's broker
    // id and connect epoch when its Subscribe went out (homeSid 0 = held, not
    // sent yet).  moved: re-armed by a leadership move at least once, or a NEW
    // Subscribe answered Redirect (O1) — routed per partition from then on.
    uint32_t homeSession   = 0;
    int64_t  homeSid       = 0;
    uint64_t homeEpoch     = 0;
    bool     moved         = false;
    // E2 residual round 6 (F1): the TRANSIENT partition's data lineage the
    // position belongs to (the broker's lineage note; 0 = unknown / not
    // transient) — claimed on a resume; and how many times the broker answered
    // a resume PartitionDataReset (the subscription restarted at the log start).
    uint64_t lineage       = 0;
    uint32_t dataResets    = 0;
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

    /// Why the duty side finished this subscription on its own (None when it
    /// was unsubscribed, timed out or closed — see SubscriptionFailReason).
    [[nodiscard]] SubscriptionFailReason failReason() const;

    /// The broker's error code when failReason() == RejectedByBroker (an
    /// rune::protocol::client::ErrorCode value), else 0.
    [[nodiscard]] uint16_t brokerErrorCode() const;

    /// How many times this subscription's TRANSIENT partition lost its data
    /// under it (its node restarted with an empty ring — a new data lineage):
    /// each time the subscription restarted at the partition's log start, so
    /// record offsets start over and records it had not consumed before the
    /// reset are gone.  0 for a disk partition.  Never silent: this counter is
    /// the application's signal.
    [[nodiscard]] uint32_t dataResets() const;

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
