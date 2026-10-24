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
//   SecurityBehind    — ADMIN thread, issue #603: is the applier below the committed high watermark this broker KNOWS
//                       (the highest leaderCommittedHw it has seen; its own HW when it leads)?  Stepped once per pass
//                       AFTER the applier ran in that pass, so a record that arrived and was applied in the same pass
//                       never flips it - only a held, slow or halted apply leaves it set.  Posted on its edges inside
//                       kA2cSecurityServing (the same FIFO, behind the kA2cSecurityApplied of the state it vouches
//                       for).  While it is set, every grant-checked operation (the gateway's request gate and routed
//                       produce, logins, `__security` reads and security DDL, admin-log frames, the workers' ingress
//                       WRITE check and every subscription push, the gateway's and the workers') answers the retriable
//                       AuthUnavailable (48) or waits, and nothing is appended or pushed; the control thread clears the
//                       workers' copy only once every applied event's enforcement fence was acknowledged (the view's
//                       effects - revoked bitmaps, ended subscriptions - reached them first).  The R session end below
//                       is unchanged: a broker stuck behind still ends its sessions R after it went Stale.
//
// The staleness of what a broker serves is therefore bounded by F everywhere: a follower's state is at most F older
// than its leader's last answer, and a cut-off leader stops F after its last consensus contact (a new leader can only
// be elected - and commit a change - after the broker liveness timeout, which F exceeds).  Issue #603 narrows it: a
// committed event this broker HOLDS (its leader told it the HW covers it) is never served around - the grants it
// would change are refused (48) until it is applied.  F remains the bound only for events this broker has not heard
// of yet (not received, or received but its leader's HW advertisement covering them not arrived yet): nothing local
// can tell those apart from "nothing happened", which is inherent to a replica that learns of commits by message.
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
        int64_t  highWatermark      = 0;       ///< the leader's committed HW (PartitionReplica::leaderCommittedHw: a
                                               ///< follower's is the leader's advertised HW, never capped at its LEO)
    };

    explicit SecurityCatchUp(int64_t maxStalenessNs) noexcept : stalenessNs_(maxStalenessNs) {}

    /// One pass.  Returns the verdict (also caughtUp()).
    bool step(const Inputs& in) noexcept {
        const bool answered = in.leaderAnswers != answersSeen_;
        if (answered) { answersSeen_ = in.leaderAnswers; lastAnswerNs_ = in.nowNs; }
        if (in.present && !in.halted && in.appliedNext >= in.highWatermark) lastAppliedNs_ = in.nowNs;
        if (!in.present || in.halted) { st_ = NotCaughtUp{}; return false; }
        if (in.leader && in.leaderServing) {   // the leader's own log is the truth (and the contact is itself)
            lastAnswerNs_ = in.nowNs;
            st_ = CaughtUp{in.epoch, true};
            return true;
        }
        const bool fresh = in.nowNs - lastAnswerNs_ <= stalenessNs_ && in.nowNs - lastAppliedNs_ <= stalenessNs_;
        if (auto* c = std::get_if<CaughtUp>(&st_)) {
            // PR #459 F2: a verdict earned as LEADER says nothing about a new leader's log - a demoted broker (a healed
            // zombie) re-earns it as a follower.  A follower's verdict is kept across a leader change while the contact
            // is fresh (a failover is shorter than F) and follows the new epoch; dropped the moment it is not fresh -
            // from then on only a full catch-up earns it back.
            if (c->asLeader && !in.leader) {
                st_ = NotCaughtUp{};
                if (!answered) lastAnswerNs_ = INT64_MIN / 2;   // its own "contact" as leader is no leader answer
            } else if (!fresh) {
                st_ = NotCaughtUp{};
            } else {
                c->epoch = in.epoch;
                return true;
            }
        }
        const bool freshNow = in.nowNs - lastAnswerNs_ <= stalenessNs_ && in.nowNs - lastAppliedNs_ <= stalenessNs_;
        if (!in.leader && freshNow && in.caughtUpWithLeader && in.appliedNext >= in.highWatermark) {
            st_ = CaughtUp{in.epoch, false};
            return true;
        }
        return false;
    }

    [[nodiscard]] bool caughtUp() const noexcept { return std::holds_alternative<CaughtUp>(st_); }
    /// PR #459 F2: the epoch / role the verdict is for (meaningful while caughtUp()).
    [[nodiscard]] int32_t verdictEpoch() const noexcept { const auto* c = std::get_if<CaughtUp>(&st_); return c ? c->epoch : -1; }
    [[nodiscard]] bool verdictAsLeader() const noexcept { const auto* c = std::get_if<CaughtUp>(&st_); return c && c->asLeader; }

private:
    struct NotCaughtUp {};
    struct CaughtUp { int32_t epoch = 0; bool asLeader = false; };   // what earned it (diagnostics)
    std::variant<NotCaughtUp, CaughtUp> st_{NotCaughtUp{}};
    int64_t  stalenessNs_;
    uint64_t answersSeen_   = 0;
    int64_t  lastAnswerNs_  = INT64_MIN / 2;   // never: always older than F
    int64_t  lastAppliedNs_ = INT64_MIN / 2;
};

class SecurityBehind {
public:
    struct Current {};
    struct Behind { int64_t appliedNext = -1; int64_t committedHw = 0; };   // what the verdict saw (diagnostics)
    using State = std::variant<Current, Behind>;

    /// One pass, after the applier: `present` = a `__security` replica + applier exist; `halted` = the applier halted
    /// (never current again: D28); `appliedNext` = the applier's next offset; `committedHw` = the replica's
    /// leaderCommittedHw (a follower's is the leader's advertised HW, 0 after a leader change until the new leader
    /// answers - so the highest seen is kept: a committed offset never moves back).  Returns true on an edge.
    bool step(bool present, bool halted, int64_t appliedNext, int64_t committedHw) noexcept {
        if (committedHw > knownCommittedHw_) knownCommittedHw_ = committedHw;
        const bool behind = present && (halted || appliedNext < knownCommittedHw_);
        if (behind == std::holds_alternative<Behind>(st_)) {
            if (auto* b = std::get_if<Behind>(&st_)) { b->appliedNext = appliedNext; b->committedHw = knownCommittedHw_; }
            return false;
        }
        if (behind) st_ = Behind{appliedNext, knownCommittedHw_};
        else st_ = Current{};
        return true;
    }

    [[nodiscard]] bool behind() const noexcept { return std::holds_alternative<Behind>(st_); }
    [[nodiscard]] int64_t knownCommittedHw() const noexcept { return knownCommittedHw_; }
    [[nodiscard]] const State& state() const noexcept { return st_; }

private:
    State   st_{Current{}};
    int64_t knownCommittedHw_ = 0;
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

    /// kA2cSecurityServing: the admin thread's latest verdict, for `epoch` of the `__security` replica in the role
    /// `asLeader` (-1: not checked - single node).
    void onAdminCaughtUp(bool caughtUp, int64_t leaderHw, int32_t epoch = -1, bool asLeader = false) noexcept {
        adminCaughtUp_ = caughtUp; leaderHw_ = leaderHw; adminEpoch_ = epoch; adminAsLeader_ = asLeader;
    }

    /// PR #459 F2: the committed `__security` assignment as this broker's metadata knows it, and whether that metadata
    /// map is current (a ready controller, or a follower caught up with its metadata leader and applied through its HW)
    /// with the count of metadata-leader answers (monotonic) - after a lost consensus contact the map is trusted again
    /// only once an answer arrived AFTER the contact returned and it is current: a healed zombie must not vouch for its
    /// old epoch from a map that has not learned the cluster moved on yet.
    /// `mapControls`: this node is the ready controller now (it won a majority election after the contact returned:
    /// its log is complete) - its map needs no leader answer.
    struct Committed { int32_t epoch = -1; bool selfLeads = false; bool mapCurrent = true; uint64_t mapAnswers = 0;
                       bool mapControls = false; };

    /// One control pass: `consensusClosed` = the election is CLOSED with a leader right now; `committed` (multi-node)
    /// = the committed assignment - the admin verdict counts only while it is for that epoch and role, so a healed
    /// zombie (a leader-era verdict of an old epoch) serves nothing until its admin thread vouches for the new one.
    Actions step(int64_t nowNs, bool consensusClosed, const Committed* committed = nullptr) noexcept {
        if (!consensusClosed && nowNs - lastConsensusNs_ > stalenessNs_) { mapRefreshOwed_ = true; baselineSet_ = false; }
        if (consensusClosed) lastConsensusNs_ = nowNs;
        if (committed != nullptr && mapRefreshOwed_ && consensusClosed) {
            if (committed->mapControls) mapRefreshOwed_ = false;
            else if (!baselineSet_) { mapAnswersBaseline_ = committed->mapAnswers; baselineSet_ = true; }
            else if (committed->mapAnswers > mapAnswersBaseline_ && committed->mapCurrent) mapRefreshOwed_ = false;
        }
        // A LEADER verdict counts only for the committed epoch with this node as its leader (a healed zombie's
        // leader-era verdict never does); a FOLLOWER verdict is bounded-stale on its own (SecurityCatchUp drops it F
        // after the leader's last answer), so a failover's epoch bump / a promotion in flight does not interrupt it.
        const bool forCommitted =
            committed == nullptr ||
            (!mapRefreshOwed_ &&
             (!adminAsLeader_ || (adminEpoch_ == committed->epoch && committed->selfLeads)));
        const bool current = adminCaughtUp_ && forCommitted && nowNs - lastConsensusNs_ <= stalenessNs_;
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
    int32_t adminEpoch_      = -1;
    bool     mapRefreshOwed_     = false;   // PR #459 F2: the consensus contact was lost past F
    bool     baselineSet_        = false;
    uint64_t mapAnswersBaseline_ = 0;
    bool    adminAsLeader_   = false;
    int64_t leaderHw_        = -1;
    int64_t lastConsensusNs_ = INT64_MIN / 2;
};

} // namespace rune::node
