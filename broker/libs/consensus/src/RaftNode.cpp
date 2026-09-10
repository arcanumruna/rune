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
