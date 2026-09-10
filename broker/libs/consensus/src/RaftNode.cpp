#include <rune/consensus/RaftNode.h>
#include <cassert>

namespace rune::consensus {

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------
RaftNode::RaftNode(ClusterConfig cfg, PersistentState& ps, RaftLog& log, Options opts)
    : cfg_(std::move(cfg))
    , ps_(ps)
    , log_(log)
    , opts_(opts)
    , rng_(opts.rngSeed)
    , jitterDist_(0, opts.electionTimeoutJitterNs)
{
    // Set the initial election deadline from t=0 so the first doWork
    // call with the real nowNs value triggers an election at the right time.
    resetElectionDeadline(0);
}

// ---------------------------------------------------------------------------
// doWork — single-writer agent tick
// ---------------------------------------------------------------------------
int RaftNode::doWork(int64_t nowNs, Outbox& out)
{
    int work = 0;

    switch (role_) {
    case RaftRole::Follower:
    case RaftRole::Candidate:
        if (nowNs >= electionDeadlineNs_) {
            becomeCandidate(nowNs, out);
            // becomeCandidate emits (members-1) RequestVotes + 1 transition
            work += static_cast<int>(cfg_.members.size() - 1) + 1;
        }
        break;

    case RaftRole::Leader:
        if (nowNs >= heartbeatDeadlineNs_) {
            work += emitHeartbeats(nowNs, out);
            heartbeatDeadlineNs_ = nowNs + opts_.heartbeatIntervalNs;
            work += 1; // the heartbeat-send "tick" itself
        }
        break;
    }

    return work;
}

// ---------------------------------------------------------------------------
// becomeFollower
// ---------------------------------------------------------------------------
void RaftNode::becomeFollower(Term term, int64_t nowNs)
{
    if (term > ps_.currentTerm()) {
        ps_.setCurrentTerm(term); // resets votedFor to kNoVote
    }
    role_ = RaftRole::Follower;
    votesGranted_.clear(); // no longer a candidate or leader
    resetElectionDeadline(nowNs);
}

// ---------------------------------------------------------------------------
// becomeCandidate
// ---------------------------------------------------------------------------
void RaftNode::becomeCandidate(int64_t nowNs, Outbox& out)
{
    Term newTerm = ps_.currentTerm() + 1;
    // Persist new term and vote for self atomically before sending any message.
    ps_.update(newTerm, cfg_.self);

    role_ = RaftRole::Candidate;
    resetElectionDeadline(nowNs);

    // Seed vote set with self (a candidate always votes for itself).
    votesGranted_.clear();
    votesGranted_.push_back(cfg_.self);

    // Emit RequestVote to every peer != self.
    RequestVote rv{
        .candidateTerm  = newTerm,
        .candidateId    = cfg_.self,
        .lastLogIndex   = log_.lastIndex(),
        .lastLogTerm    = log_.lastTerm(),
    };

    for (NodeId peer : cfg_.members) {
        if (peer != cfg_.self) {
            out.push_back(OutboundMessage{ .to = peer, .msg = rv });
        }
    }
}

// ---------------------------------------------------------------------------
// becomeLeader
// ---------------------------------------------------------------------------
void RaftNode::becomeLeader(int64_t nowNs, Outbox& out)
{
    role_ = RaftRole::Leader;
    // Set deadline to nowNs so the very next doWork call emits heartbeats.
    heartbeatDeadlineNs_ = nowNs;
    // Emit initial heartbeats immediately.
    emitHeartbeats(nowNs, out);
    heartbeatDeadlineNs_ = nowNs + opts_.heartbeatIntervalNs;
}

// ---------------------------------------------------------------------------
// observeTerm
// ---------------------------------------------------------------------------
void RaftNode::observeTerm(Term term, int64_t nowNs)
{
    if (term > ps_.currentTerm()) {
        becomeFollower(term, nowNs);
    }
}

// ---------------------------------------------------------------------------
// handleRequestVote (P2-d) — §A.3 decision order, §A.4 persist before reply
// ---------------------------------------------------------------------------
RequestVoteResult RaftNode::handleRequestVote(const RequestVote& rv,
                                              int64_t nowNs, Outbox& out)
{
    // Step 1: observe the candidate's term — if higher, step down and adopt it
    // (this clears votedFor so we may vote for the candidate in this term).
    observeTerm(rv.candidateTerm, nowNs);

    RequestVoteResult result{ .term = ps_.currentTerm(), .voteGranted = false };

    // Step 2: build grant decision.
    bool grantVote = false;

    // candidateTerm must equal currentTerm (stale/lower → deny).
    if (rv.candidateTerm == ps_.currentTerm()) {
        // votedFor must be kNoVote or the same candidate (idempotency).
        NodeId vf = ps_.votedFor();
        bool canVote = (vf == kNoVote || vf == rv.candidateId);

        if (canVote) {
            // Candidate log must be at-least-as-up-to-date (compareLog >= 0).
            Index myLastIndex = log_.lastIndex();
            Term  myLastTerm  = log_.lastTerm();
            bool upToDate =
                rv.lastLogTerm > myLastTerm ||
                (rv.lastLogTerm == myLastTerm && rv.lastLogIndex >= myLastIndex);

            if (upToDate) {
                grantVote = true;
            }
        }
    }

    if (grantVote) {
        // §A.4 — persist before replying.
        ps_.update(ps_.currentTerm(), rv.candidateId);
        // Reset election deadline: we heard from a viable candidate.
        resetElectionDeadline(nowNs);
        result.voteGranted = true;
    }

    // Append to outbox so the transport can deliver the reply.
    out.push_back(OutboundMessage{ .to = rv.candidateId, .msg = result });

    return result;
}

// ---------------------------------------------------------------------------
// handleRequestVoteResult (P2-d) — tally votes; promote to Leader at majority
// ---------------------------------------------------------------------------
void RaftNode::handleRequestVoteResult(NodeId from, const RequestVoteResult& res,
                                       int64_t nowNs, Outbox& out)
{
    // Observe the responder's term — may step us down.
    observeTerm(res.term, nowNs);

    // Ignore if we're no longer a Candidate or the result is for a stale term.
    if (role_ != RaftRole::Candidate || res.term != ps_.currentTerm()) {
        return;
    }

    if (!res.voteGranted) {
        return;
    }

    // Record the grant, but only once per node (de-duplicate).
    bool alreadyCounted = false;
    for (NodeId id : votesGranted_) {
        if (id == from) { alreadyCounted = true; break; }
    }
    if (!alreadyCounted) {
        votesGranted_.push_back(from);
    }

    // Promote to Leader if we have a majority.
    if (static_cast<int>(votesGranted_.size()) >= cfg_.majority()) {
        becomeLeader(nowNs, out);
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------
void RaftNode::resetElectionDeadline(int64_t nowNs)
{
    int64_t jitter = jitterDist_(rng_);
    electionDeadlineNs_ = nowNs + opts_.electionTimeoutBaseNs + jitter;
}

int RaftNode::emitHeartbeats(int64_t /*nowNs*/, Outbox& out)
{
    int count = 0;
    AppendEntries hb{
        .term         = ps_.currentTerm(),
        .leaderId     = cfg_.self,
        .prevLogIndex = log_.lastIndex(),
        .prevLogTerm  = log_.lastTerm(),
        .entries      = {},            // empty heartbeat
        .leaderCommit = log_.commitIndex(),
    };

    for (NodeId peer : cfg_.members) {
        if (peer != cfg_.self) {
            out.push_back(OutboundMessage{ .to = peer, .msg = hb });
            ++count;
        }
    }
    return count;
}

} // namespace rune::consensus
