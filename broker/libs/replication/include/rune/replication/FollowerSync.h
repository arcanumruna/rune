#pragma once
// ---------------------------------------------------------------------------
// FollowerSync.h — follower replication-sync state machine.
//
// Namespace: rune::replication
// C++23
//
// SINGLE SOURCE OF TRUTH for a follower's fetch/ack cadence.  Replaces the
// former scattered flags (heardFromLeader_ / lastResponseEmpty_ /
// lastKnownLeaderHw_ / leoAtLastPeriodicFetch_ / dataFetchInFlight_ /
// dataFetchSentNs_) with one explicit two-state machine so every edge case
// — bootstrap, catch-up, in-sync keepalive, lost-response timeout, role
// change — has exactly one well-defined code path.
//
// It is pure logic: no clock, no I/O, no Aeron, no allocation.  The caller
// supplies `nowNs` and the cadence constants; the machine returns what to do
// this duty tick.  That purity is deliberate — it makes the machine
// exhaustively unit-testable in isolation (see test/FollowerSyncTest.cpp).
//
// ── Design (why two states) ────────────────────────────────────────────────
// The "am I caught up?" signal is the emptiness of the last leader response,
// NOT a high-watermark comparison: a min-ISR-gated leader HW can read 0 while
// THIS follower is the one gating it, so an HW compare gives false "behind"
// forever.  An empty response is unambiguous — the leader had nothing beyond
// our fetch offset.
//
//   Behind — bootstrap (never heard a leader) OR the last response carried
//            records (more to pull).  Emits a SINGLE-OUTSTANDING data-fetch on
//            the fast cadence: it will NOT re-request the same [LEO,…] range
//            while one is already in flight — that redundant re-fetch (the
//            follower's LEO hasn't advanced only because the response is still
//            in transit / unapplied) is exactly the flood that head-of-line-
//            blocks the useful applies.  A timeout re-sends if the response was
//            lost.  Bulk data also arrives independently via the GR-10 push.
//   InSync — the last response was empty.  Emits only a slow PURE-ACK
//            keepalive (maxBytes=0) for ISR liveness; real data still arrives
//            via push, which flips the machine back to Behind on apply.
//
// The latency-critical fast-ack (emitted on every apply, pure, maxBytes=0) is
// INDEPENDENT of this machine — it advances the leader's follower-offset → HW
// for acks=majority.  FollowerSync governs only the periodic/catch-up fetch.
// ---------------------------------------------------------------------------

#include <cstdint>

#include "rune/replication/Types.h"

namespace rune::replication {

class FollowerSync {
public:
    /// TruncatePending (fix round 5): a KIP-279 divergence truncation to
    /// truncateTarget() is decided but the log cannot run it yet (the log
    /// cleaner still has posted renames to perform — the owner never waits).
    /// No fetch is emitted and no fetched data may be applied until the owner
    /// completes the truncation on a later duty pass (completeTruncation).
    enum class State : uint8_t { Behind, InSync, TruncatePending };

    static constexpr const char* toString(State s) noexcept {
        return s == State::InSync ? "InSync" : s == State::Behind ? "Behind" : "TruncatePending";
    }

    /// What the follower should do on this duty tick.
    struct Action {
        bool emit;      ///< emit a FetchRequest at all this tick?
        bool wantData;  ///< true = catch-up data fetch; false = pure-ack (maxBytes=0)
    };

    /// Role change (become-follower / self-demotion): start over in bootstrap.
    /// A fresh follower must confirm its position with the new leader before it
    /// can be considered caught up, even if its LEO happens to match.
    void reset() noexcept {
        state_           = State::Behind;
        truncateTarget_  = -1;
        leaderHw_        = 0;
        fetchDeadlineNs_ = 0;
        lastEmitNs_      = 0;
        prevEmitNs_      = 0;
        ackOwed_         = false;
    }

    /// A FetchResponse or GR-10 push from the current (or a newer) leader was
    /// applied.  `hadRecords` is the caught-up signal: non-empty ⟺ still Behind
    /// (keep pulling); empty ⟺ InSync (keepalive only).  Either way the leader
    /// answered, so any outstanding single-outstanding data-fetch is cleared —
    /// the next Behind tick may issue a fresh one from the advanced LEO.
    void onResponse(bool hadRecords, Offset leaderHw) noexcept {
        if (leaderHw > leaderHw_) leaderHw_ = leaderHw;  // monotonic, observational
        fetchDeadlineNs_ = 0;                            // response arrived → not outstanding
        if (state_ == State::TruncatePending) return;    // only completeTruncation leaves it
        state_ = hadRecords ? State::Behind : State::InSync;
    }

    /// Enter TruncatePending: the state holds the truncation target.
    void enterTruncatePending(Offset target) noexcept {
        state_           = State::TruncatePending;
        truncateTarget_  = target;
        fetchDeadlineNs_ = 0;
    }
    [[nodiscard]] bool   truncatePending() const noexcept { return state_ == State::TruncatePending; }
    [[nodiscard]] Offset truncateTarget()  const noexcept { return truncateTarget_; }

    /// F3 fix-round-2, item A: the leader replied with a non-recoverable error
    /// (kErrRecordTooLarge — a batch that exceeds even the transport's hard
    /// ceiling and can NEVER be delivered at this size).  This is emphatically
    /// NOT "caught up": force Behind (never let a stale InSync linger past an
    /// error response) and clear the outstanding-fetch deadline so the next
    /// duty tick issues a fresh fetch at the FAST cadence rather than waiting
    /// out a resend timer for a response that already arrived (as an error).
    /// Does not touch leaderHw_ (still a valid, monotonic observation).
    void forceBehind() noexcept {
        fetchDeadlineNs_ = 0;
        if (state_ != State::TruncatePending) state_ = State::Behind;
    }

    /// Decide this duty tick's fetch action.
    ///   nowNs    — monotonic clock (ns).
    ///   fastNs   — fast catch-up cadence (min spacing between data fetches).
    ///   keepNs   — InSync keepalive cadence (must be < replicaLagMaxMs so the
    ///              follower is never evicted from the ISR — and small enough that a
    ///              LOST push is rediscovered promptly, see below).
    ///   resendNs — how long to wait for an in-flight data-fetch's response
    ///              before assuming it was lost and re-sending.  Effective
    ///              resend interval is max(fastNs, resendNs); pass resendNs ≥
    ///              fastNs for the timeout to dominate.
    [[nodiscard]] Action decide(int64_t nowNs, int64_t fastNs, int64_t keepNs,
                                int64_t resendNs) noexcept {
        if (state_ == State::TruncatePending) return {false, false};   // nothing until truncated
        if (state_ == State::InSync) {
            if (nowNs - lastEmitNs_ < keepNs) return {false, false};
            prevEmitNs_ = lastEmitNs_;
            lastEmitNs_ = nowNs;
            // DATA keepalive, NOT a pure ack.  A caught-up follower that goes InSync
            // must still periodically PULL: the leader's GR-10 push is best-effort and
            // a dropped/oversized/back-pressured push leaves the follower silently
            // behind.  A pure-ack keepalive (wantData=false) never pulls that gap, so
            // the follower stalls at its last offset until it is evicted — the
            // "InSync stall" collapse (HW frozen mid-run, ISR still full).  A data
            // keepalive returns empty when genuinely caught up (cheap, stays InSync)
            // and returns the missed tail otherwise → onResponse flips back to Behind
            // and the fast path drains it.  Single-outstanding still applies.
            fetchDeadlineNs_ = nowNs + resendNs;
            return {true, true};
        }
        // Behind.
        if (nowNs - lastEmitNs_ < fastNs) return {false, false};
        const bool outstanding = fetchDeadlineNs_ != 0 && nowNs < fetchDeadlineNs_;
        if (outstanding) return {false, false};    // one data-fetch already in flight
        prevEmitNs_      = lastEmitNs_;
        lastEmitNs_      = nowNs;
        fetchDeadlineNs_ = nowNs + resendNs;        // arm resend; cleared by onResponse
        return {true, true};                        // single-outstanding catch-up fetch
    }

    /// F27: the fetch decide() just emitted was REFUSED by the transport (Aeron
    /// back-pressure / not connected).  It never left, so nothing is outstanding:
    /// undo the emit — the next tick re-sends it, whatever the clock says
    /// (time-independent; never wait out resendNs for a frame that was not sent).
    void onFetchRefused() noexcept {
        lastEmitNs_      = prevEmitNs_;
        fetchDeadlineNs_ = 0;
    }

    /// F27: a pure fast-ack (LEO report) was refused by the transport — owe it;
    /// the next tick re-sends it.  Any fetch that IS sent carries the same LEO
    /// and discharges it (onFetchSent).
    void onAckRefused() noexcept { ackOwed_ = true; }
    void onFetchSent()  noexcept { ackOwed_ = false; }
    [[nodiscard]] bool ackOwed() const noexcept { return ackOwed_; }

    // ── Observability / test accessors (also keep fields "used" for -Werror) ──
    [[nodiscard]] State   state()     const noexcept { return state_; }
    [[nodiscard]] bool    inSync()    const noexcept { return state_ == State::InSync; }
    [[nodiscard]] Offset  leaderHw()  const noexcept { return leaderHw_; }
    /// True when a single-outstanding data-fetch is currently armed (awaiting
    /// its response or its resend deadline).
    [[nodiscard]] bool    fetchArmed() const noexcept { return fetchDeadlineNs_ != 0; }

private:
    State   state_           = State::Behind;  ///< bootstrap in Behind (fetch until first empty)
    Offset  leaderHw_        = 0;              ///< highest leader HW heard (observational)
    int64_t fetchDeadlineNs_ = 0;              ///< 0 = no outstanding fetch; else resend deadline
    int64_t lastEmitNs_      = 0;              ///< last fetch emit (throttles fast/keepalive)
    int64_t prevEmitNs_      = 0;              ///< lastEmitNs_ before the latest emit (F27 undo)
    bool    ackOwed_         = false;          ///< F27: a refused fast-ack awaits re-send
    Offset  truncateTarget_  = -1;             ///< TruncatePending: the decided truncation offset
};

} // namespace rune::replication
