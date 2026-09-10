#pragma once
#include <rune/consensus/Types.h>
#include <rune/consensus/Messages.h>
#include <rune/consensus/PersistentState.h>
#include <rune/consensus/RaftLog.h>
#include <cstdint>
#include <random>
#include <unordered_map>
#include <vector>

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

    /// Number of votes granted to this node in the current term (including self).
    int grantedVoteCount() const noexcept { return static_cast<int>(votesGranted_.size()); }

    /// Per-peer leader state accessors (valid only when role==Leader).
    Index nextIndexFor(NodeId peer)  const;
    Index matchIndexFor(NodeId peer) const;

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

    /// Become Leader: initializes nextIndex/matchIndex per peer, emits first
    /// AppendEntries (with any entries the peer might be missing) to all peers.
    void becomeLeader(int64_t nowNs, Outbox& out);

    /// If term > currentTerm, step down to Follower and adopt the new term.
    /// Handlers in P2-d/P2-e call this on every inbound message.
    void observeTerm(Term term, int64_t nowNs);

    // ------------------------------------------------------------------
    // P2-d: Vote handlers
    // ------------------------------------------------------------------

    /// Handle an inbound RequestVote RPC.
    /// Grants or denies per §A.3; persists vote before replying (§A.4).
    /// Appends a RequestVoteResult to `out` addressed to rv.candidateId,
    /// and also returns the result struct for convenient testing.
    RequestVoteResult handleRequestVote(const RequestVote& rv, int64_t nowNs, Outbox& out);

    /// Handle an inbound RequestVoteResult (response to our RequestVote).
    /// Tallies granted votes; transitions to Leader when majority reached.
    void handleRequestVoteResult(NodeId from, const RequestVoteResult& res,
                                 int64_t nowNs, Outbox& out);

    // ------------------------------------------------------------------
    // P2-e: Replication handlers
    // ------------------------------------------------------------------

    /// Leader: append a command to the log and replicate to all peers.
    /// Returns the new lastIndex, or -1 if not leader (no-op).
    Index propose(std::string command, int64_t nowNs, Outbox& out);

    /// Follower: handle an AppendEntries RPC from the leader.
    /// Appends result to `out` (to=ae.leaderId) and also returns it.
    AppendEntriesResult handleAppendEntries(const AppendEntries& ae,
                                            int64_t nowNs, Outbox& out);

    /// Leader: handle a response to a previously-sent AppendEntries.
    void handleAppendEntriesResult(NodeId from, const AppendEntriesResult& res,
                                   int64_t nowNs, Outbox& out);

private:
    /// Draw a fresh randomized election deadline: nowNs + base + U[0, jitter].
    void resetElectionDeadline(int64_t nowNs);

    /// Build and emit an AppendEntries to a single peer (carries entries if
    /// the peer is behind; otherwise an empty heartbeat).
    void emitAppendEntriesTo(NodeId peer, int64_t nowNs, Outbox& out);

    /// Emit AppendEntries to all peers (replaces old empty-heartbeat helper).
    int emitHeartbeats(int64_t nowNs, Outbox& out);

    /// Advance commitIndex as leader when a majority of nodes (incl. self)
    /// have matchIndex >= N AND log.termAt(N) == currentTerm (Figure-8 rule).
    void tryAdvanceCommitIndex();

    ClusterConfig   cfg_;
    PersistentState& ps_;
    RaftLog&         log_;
    Options          opts_;

    RaftRole role_              { RaftRole::Follower };
    int64_t  electionDeadlineNs_{ 0 };
    int64_t  heartbeatDeadlineNs_{ 0 };

    std::mt19937_64 rng_;
    std::uniform_int_distribution<int64_t> jitterDist_;

    /// NodeIds that granted a vote to us in the current term (including self).
    /// Cleared and re-seeded with self on becomeCandidate; cleared on step-down.
    std::vector<NodeId> votesGranted_;

    /// Per-peer leader replication state (valid while role==Leader).
    /// nextIndex[peer]  — next log index to send to that peer (init: lastIndex+1).
    /// matchIndex[peer] — highest index known to be replicated on that peer (init: 0).
    std::unordered_map<NodeId, Index> nextIndex_;
    std::unordered_map<NodeId, Index> matchIndex_;
};

} // namespace rune::consensus
