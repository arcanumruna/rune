#pragma once
#include <rune/consensus/Types.h>
#include <rune/consensus/Messages.h>
#include <rune/consensus/PersistentState.h>
#include <rune/consensus/RaftLog.h>
#include <cstdint>
#include <random>

namespace rune::consensus {

// ---------------------------------------------------------------------------
// RaftNode — single-writer Raft state machine (Aeron Cluster model).
//
// Driven by ONE thread via doWork(nowNs, out).  All time is the injected
// nowNs parameter — no wall-clock.  Outbound messages are written into the
// caller-provided Outbox which is REUSED across ticks (caller clears).
// ---------------------------------------------------------------------------

struct Options {
    int64_t  electionTimeoutBaseNs;   ///< Base election timeout (nanoseconds)
    int64_t  electionTimeoutJitterNs; ///< Max random jitter added to base
    int64_t  heartbeatIntervalNs;     ///< Leader heartbeat interval (nanoseconds)
    uint64_t rngSeed;                 ///< Seed for the randomized election timeout RNG
};

class RaftNode {
public:
    /// Construct a RaftNode.  ps and log are owned externally; RaftNode holds refs.
    /// Starts as Follower with an election deadline set from nowNs=0 (caller must
    /// call doWork(nowNs, out) to drive the node).
    RaftNode(ClusterConfig cfg, PersistentState& ps, RaftLog& log, Options opts);

    // ------------------------------------------------------------------
    // Observers (hot path — no allocation)
    // ------------------------------------------------------------------
    RaftRole role()        const noexcept { return role_; }
    Term     currentTerm() const noexcept { return ps_.currentTerm(); }
    NodeId   self()        const noexcept { return cfg_.self; }

    /// Exposed for tests to verify deterministic jitter.
    int64_t electionDeadlineNs() const noexcept { return electionDeadlineNs_; }

    // ------------------------------------------------------------------
    // Single-writer agent entry point.
    // Returns workCount = (messages emitted) + (role transitions performed).
    // Time is ONLY the nowNs parameter — no wall-clock.
    // ------------------------------------------------------------------
    int doWork(int64_t nowNs, Outbox& out);

    // ------------------------------------------------------------------
    // Role transitions (also usable directly; doWork calls them internally)
    // ------------------------------------------------------------------

    /// Step down to Follower.  If term > currentTerm, persist via ps.setCurrentTerm
    /// (which resets votedFor).  Resets randomized election deadline.
    void becomeFollower(Term term, int64_t nowNs);

    /// Start a new election: bump+persist term, vote self, become Candidate,
    /// reset election deadline, emit RequestVote to every peer != self.
    void becomeCandidate(int64_t nowNs, Outbox& out);

    /// Become Leader: set heartbeatDeadlineNs = nowNs so next doWork heartbeats
    /// immediately.
    void becomeLeader(int64_t nowNs, Outbox& out);

    /// If term > currentTerm, step down to Follower and adopt the new term.
    /// Handlers in P2-d/P2-e call this on every inbound message.
    void observeTerm(Term term, int64_t nowNs);

private:
    /// Draw a fresh randomized election deadline: nowNs + base + U[0, jitter].
    void resetElectionDeadline(int64_t nowNs);

    /// Emit empty AppendEntries heartbeats to all peers.
    int emitHeartbeats(int64_t nowNs, Outbox& out);

    ClusterConfig   cfg_;
    PersistentState& ps_;
    RaftLog&         log_;
    Options          opts_;

    RaftRole role_              { RaftRole::Follower };
    int64_t  electionDeadlineNs_{ 0 };
    int64_t  heartbeatDeadlineNs_{ 0 };

    std::mt19937_64 rng_;
    std::uniform_int_distribution<int64_t> jitterDist_;
};

} // namespace rune::consensus
