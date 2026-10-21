#pragma once
// SecurityServing.h — issues #15 / #111 (combined fix programme, decision M7): a broker serves logins, `__security`
// reads and security DDL only while its `__security` state is PROVABLY current; otherwise it answers the retriable
// AuthUnavailable (48), and one that stays stale too long ends its authenticated sessions (48, session level) so the
// clients move to a current broker.  Two pure machines (no clock, no I/O, no allocation - the owners pass `nowNs`):
//
//   SecurityCatchUp   — ADMIN thread, one step per pass over the `__security` replica + applier.  "Caught up" means:
//                       not halted, and either
//                         * the replica LEADS and SecurityAdmin reached LeaderServing in this epoch (the whole log is
//                           committed and applied: a promotion's previous tail included), or
//                         * since boot it once held everything its current leader had (caughtUpWithLeader: that
//                           leader answered with nothing beyond our log end) with every committed record applied,
//                       and ever since then the CONTACT stayed fresh: the current leader answered (or we led) within F
//                       AND the applier was at the high watermark within F.  A lapse of either for longer than F drops
//                       it (the latch is re-earned from scratch, never resumed): a restarted, wiped, new or partitioned
//                       follower answers 48 until it caught up again.  Posted to the control thread on change
//                       (kA2cSecurityServing), ordered behind every kA2cSecurityApplied of the state it vouches for.
//   SecurityServing   — CONTROL thread: CatchingUp (boot: never served) | Serving | Stale{sinceNs, sessionsEnded}.
//                       Current ⇔ the admin thread's latest "caught up" AND the consensus was in contact (the election
//                       CLOSED with a leader) within F - the second condition is what stops a partitioned ex-leader
//                       (a zombie `__security` leader on a minority keeps "leading" with nobody to tell it otherwise): a
//                       minority consensus leader steps down (inactive follower quorum) and a cut-off follower
//                       re-canvasses, so neither stays CLOSED.  Stale for longer than R ends the authenticated sessions
//                       once (they reconnect to a current broker).
//
// The staleness of what a broker serves is therefore bounded by F everywhere: a follower's state is at most F older
// than its leader's last answer, and a cut-off leader stops F after its last consensus contact (a new leader can only
// be elected - and commit a change - after the broker liveness timeout, which F exceeds).
#include <cstdint>
#include <variant>

namespace rune::node {

class SecurityCatchUp {
public:
    struct Inputs {
        int64_t  nowNs              = 0;
        bool     present            = false;   ///< a `__security` replica + applier exist
        bool     halted             = false;   ///< the applier halted (D28 - its own path)
        bool     leader             = false;   ///< the replica leads
        bool     leaderServing      = false;   ///< SecurityAdmin is LeaderServing in the replica's current epoch
        int32_t  epoch              = 0;       ///< the replica's epoch
        uint64_t leaderAnswers      = 0;       ///< PartitionReplica::leaderAnswers()
        bool     caughtUpWithLeader = false;   ///< PartitionReplica::caughtUpWithLeader()
        int64_t  appliedNext        = -1;      ///< the applier's next offset (every record below it is applied)
        int64_t  highWatermark      = 0;       ///< the replica's committed prefix
    };

    explicit SecurityCatchUp(int64_t maxStalenessNs) noexcept : stalenessNs_(maxStalenessNs) {}

    /// One pass.  Returns the verdict (also caughtUp()).
    bool step(const Inputs& in) noexcept {
        if (in.leaderAnswers != answersSeen_) { answersSeen_ = in.leaderAnswers; lastAnswerNs_ = in.nowNs; }
        if (in.present && !in.halted && in.appliedNext >= in.highWatermark) lastAppliedNs_ = in.nowNs;
        if (!in.present || in.halted) { st_ = NotCaughtUp{}; return false; }
        if (in.leader && in.leaderServing) {   // the leader's own log is the truth (and the contact is itself)
            lastAnswerNs_ = in.nowNs;
            st_ = CaughtUp{in.epoch, true};
            return true;
        }
        const bool fresh = in.nowNs - lastAnswerNs_ <= stalenessNs_ && in.nowNs - lastAppliedNs_ <= stalenessNs_;
        if (std::holds_alternative<CaughtUp>(st_)) {
            // Kept across a leader change while the contact is fresh (a failover is shorter than F); dropped the
            // moment it is not - from then on only a full catch-up earns it back.
            if (!fresh) st_ = NotCaughtUp{};
            return std::holds_alternative<CaughtUp>(st_);
        }
        if (!in.leader && fresh && in.caughtUpWithLeader && in.appliedNext >= in.highWatermark) {
            st_ = CaughtUp{in.epoch, false};
            return true;
        }
        return false;
    }

    [[nodiscard]] bool caughtUp() const noexcept { return std::holds_alternative<CaughtUp>(st_); }

private:
    struct NotCaughtUp {};
    struct CaughtUp { int32_t epoch = 0; bool asLeader = false; };   // what earned it (diagnostics)
    std::variant<NotCaughtUp, CaughtUp> st_{NotCaughtUp{}};
    int64_t  stalenessNs_;
    uint64_t answersSeen_   = 0;
    int64_t  lastAnswerNs_  = INT64_MIN / 2;   // never: always older than F
    int64_t  lastAppliedNs_ = INT64_MIN / 2;
};

class SecurityServing {
public:
    struct CatchingUp {};
    struct Serving {};
    struct Stale { int64_t sinceNs = 0; bool sessionsEnded = false; };
    using State = std::variant<CatchingUp, Serving, Stale>;

    /// What the caller does after a step: tell the gateway the new serving verdict / end the authenticated sessions.
    struct Actions { bool changed = false; bool endSessions = false; };

    SecurityServing(int64_t maxStalenessNs, int64_t staleSessionEndNs) noexcept
        : stalenessNs_(maxStalenessNs), sessionEndNs_(staleSessionEndNs) {}

    /// kA2cSecurityServing: the admin thread's latest verdict.
    void onAdminCaughtUp(bool caughtUp, int64_t leaderHw) noexcept { adminCaughtUp_ = caughtUp; leaderHw_ = leaderHw; }

    /// One control pass: `consensusClosed` = the election is CLOSED with a leader right now.
    Actions step(int64_t nowNs, bool consensusClosed) noexcept {
        if (consensusClosed) lastConsensusNs_ = nowNs;
        const bool current = adminCaughtUp_ && nowNs - lastConsensusNs_ <= stalenessNs_;
        Actions a;
        if (current) {
            if (!std::holds_alternative<Serving>(st_)) { st_ = Serving{}; a.changed = true; }
            return a;
        }
        if (std::holds_alternative<Serving>(st_)) { st_ = Stale{nowNs, false}; a.changed = true; return a; }
        if (auto* s = std::get_if<Stale>(&st_); s != nullptr && !s->sessionsEnded && nowNs - s->sinceNs > sessionEndNs_) {
            s->sessionsEnded = true;
            a.endSessions = true;
        }
        return a;
    }

    [[nodiscard]] bool serving() const noexcept { return std::holds_alternative<Serving>(st_); }
    [[nodiscard]] const State& state() const noexcept { return st_; }
    [[nodiscard]] bool adminCaughtUp() const noexcept { return adminCaughtUp_; }
    [[nodiscard]] int64_t leaderHw() const noexcept { return leaderHw_; }
    /// Counter value: 0 CatchingUp, 1 Serving, 2 Stale.
    [[nodiscard]] int64_t code() const noexcept { return static_cast<int64_t>(st_.index()); }
    [[nodiscard]] const char* name() const noexcept {
        return serving() ? "Serving" : std::holds_alternative<Stale>(st_) ? "Stale" : "CatchingUp";
    }

private:
    State   st_{CatchingUp{}};
    int64_t stalenessNs_;
    int64_t sessionEndNs_;
    bool    adminCaughtUp_   = false;
    int64_t leaderHw_        = -1;
    int64_t lastConsensusNs_ = INT64_MIN / 2;
};

} // namespace rune::node
