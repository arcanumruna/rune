#include <rune/consensus/RaftNode.h>
#include <algorithm>
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

    // Initialize per-peer replication state (Raft §5.3).
    // nextIndex[peer]  = leader's lastIndex + 1
    // matchIndex[peer] = 0 (nothing confirmed yet)
    // matchIndex[self] = leader's lastIndex (leader already has everything)
    Index leaderLast = log_.lastIndex();
    nextIndex_.clear();
    matchIndex_.clear();
    for (NodeId peer : cfg_.members) {
        if (peer != cfg_.self) {
            nextIndex_[peer]  = leaderLast + 1;
            matchIndex_[peer] = 0;
        }
    }
    // Leader's own match is its lastIndex (used for commit majority counting).
    matchIndex_[cfg_.self] = leaderLast;

    // Set deadline to nowNs so the very next doWork call emits heartbeats.
    heartbeatDeadlineNs_ = nowNs;
    // Emit initial AppendEntries to all peers immediately.
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
// handleAppendEntries (P2-e) — follower side, Raft §5.3 / notes §A.5
// ---------------------------------------------------------------------------
AppendEntriesResult RaftNode::handleAppendEntries(const AppendEntries& ae,
                                                   int64_t nowNs, Outbox& out)
{
    // 1. Always observe the sender's term first (may step us down).
    observeTerm(ae.term, nowNs);

    AppendEntriesResult reply{ .term = ps_.currentTerm(), .success = false, .matchIndex = 0 };

    // 2. Stale leader → reject.
    if (ae.term < ps_.currentTerm()) {
        out.push_back(OutboundMessage{ .to = ae.leaderId, .msg = reply });
        return reply;
    }

    // 3. Valid current-term leader: reset election deadline; step down if Candidate.
    resetElectionDeadline(nowNs);
    if (role_ == RaftRole::Candidate) {
        // Same-term leader beats us — revert to follower without changing term.
        role_ = RaftRole::Follower;
        votesGranted_.clear();
    }

    // 4. Consistency check: prevLogIndex must exist and term must match.
    if (ae.prevLogIndex > 0) {
        if (ae.prevLogIndex > log_.lastIndex() ||
            log_.termAt(ae.prevLogIndex) != ae.prevLogTerm) {
            out.push_back(OutboundMessage{ .to = ae.leaderId, .msg = reply });
            return reply;
        }
    }

    // 5. Append with conflict resolution (idempotent: do NOT truncate on match).
    Index lastNewIndex = ae.prevLogIndex + static_cast<Index>(ae.entries.size());
    for (Index i = 0; i < static_cast<Index>(ae.entries.size()); ++i) {
        Index idx = ae.prevLogIndex + 1 + i;
        if (idx <= log_.lastIndex()) {
            if (log_.termAt(idx) != ae.entries[i].term) {
                // Conflict: truncate this and all following entries, then append.
                log_.truncateFrom(idx);
                for (Index j = i; j < static_cast<Index>(ae.entries.size()); ++j) {
                    log_.append(ae.entries[j].term, ae.entries[j].command);
                }
                break;
            }
            // Terms match at idx — skip (idempotent re-delivery, preserve entry).
        } else {
            // Beyond current log end — append this and remaining entries.
            for (Index j = i; j < static_cast<Index>(ae.entries.size()); ++j) {
                log_.append(ae.entries[j].term, ae.entries[j].command);
            }
            break;
        }
    }

    // 6. Advance commit index.
    if (ae.leaderCommit > log_.commitIndex()) {
        log_.setCommitIndex(std::min(ae.leaderCommit, lastNewIndex));
    }

    reply.success    = true;
    reply.matchIndex = lastNewIndex;
    out.push_back(OutboundMessage{ .to = ae.leaderId, .msg = reply });
    return reply;
}

// ---------------------------------------------------------------------------
// handleAppendEntriesResult (P2-e) — leader side
// ---------------------------------------------------------------------------
void RaftNode::handleAppendEntriesResult(NodeId from, const AppendEntriesResult& res,
                                          int64_t nowNs, Outbox& out)
{
    // Observe term — if higher, we step down (observeTerm calls becomeFollower).
    observeTerm(res.term, nowNs);

    // Ignore if we've stepped down or term mismatch.
    if (role_ != RaftRole::Leader || res.term != ps_.currentTerm()) {
        return;
    }

    if (res.success) {
        // Update peer's replication progress.
        matchIndex_[from] = std::max(matchIndex_[from], res.matchIndex);
        nextIndex_[from]  = matchIndex_[from] + 1;

        // Update our own matchIndex (leader always has everything it proposed).
        matchIndex_[cfg_.self] = log_.lastIndex();

        // Try to advance commitIndex (Leader Completeness / Figure-8 rule).
        tryAdvanceCommitIndex();
    } else {
        // Rejected: back off nextIndex and retry with lower prevLogIndex.
        nextIndex_[from] = std::max(Index{1}, nextIndex_[from] - 1);
        emitAppendEntriesTo(from, nowNs, out);
    }
}

// ---------------------------------------------------------------------------
// propose (P2-e) — leader appends + replicates a command
// ---------------------------------------------------------------------------
Index RaftNode::propose(std::string command, int64_t nowNs, Outbox& out)
{
    if (role_ != RaftRole::Leader) {
        return -1; // not leader — no-op
    }

    // Append to own log.
    Index newIndex = log_.append(ps_.currentTerm(), std::move(command));

    // Update leader's own matchIndex immediately.
    matchIndex_[cfg_.self] = newIndex;

    // Replicate to all peers.
    for (NodeId peer : cfg_.members) {
        if (peer != cfg_.self) {
            emitAppendEntriesTo(peer, nowNs, out);
        }
    }

    return newIndex;
}

// ---------------------------------------------------------------------------
// Observers for per-peer leader state
// ---------------------------------------------------------------------------
Index RaftNode::nextIndexFor(NodeId peer) const
{
    auto it = nextIndex_.find(peer);
    return (it != nextIndex_.end()) ? it->second : 0;
}

Index RaftNode::matchIndexFor(NodeId peer) const
{
    auto it = matchIndex_.find(peer);
    return (it != matchIndex_.end()) ? it->second : 0;
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------
void RaftNode::resetElectionDeadline(int64_t nowNs)
{
    int64_t jitter = jitterDist_(rng_);
    electionDeadlineNs_ = nowNs + opts_.electionTimeoutBaseNs + jitter;
}

void RaftNode::emitAppendEntriesTo(NodeId peer, int64_t /*nowNs*/, Outbox& out)
{
    Index  ni        = nextIndex_.count(peer) ? nextIndex_[peer] : log_.lastIndex() + 1;
    Index  prevIdx   = ni - 1;
    Term   prevTerm  = log_.termAt(prevIdx); // returns 0 for idx<=0

    AppendEntries ae{
        .term         = ps_.currentTerm(),
        .leaderId     = cfg_.self,
        .prevLogIndex = prevIdx,
        .prevLogTerm  = prevTerm,
        .entries      = log_.entriesFrom(ni),  // empty if peer is caught up
        .leaderCommit = log_.commitIndex(),
    };
    out.push_back(OutboundMessage{ .to = peer, .msg = std::move(ae) });
}

int RaftNode::emitHeartbeats(int64_t nowNs, Outbox& out)
{
    int count = 0;
    for (NodeId peer : cfg_.members) {
        if (peer != cfg_.self) {
            emitAppendEntriesTo(peer, nowNs, out);
            ++count;
        }
    }
    return count;
}

void RaftNode::tryAdvanceCommitIndex()
{
    // Figure-8 rule: only advance commit to an entry of the CURRENT term.
    // Walk from the highest possible index down to commitIndex+1 and find
    // the highest N where:
    //   (a) log.termAt(N) == currentTerm
    //   (b) a majority of nodes (leader included) have matchIndex >= N
    Index lastIdx = log_.lastIndex();
    Term  curTerm = ps_.currentTerm();

    for (Index n = lastIdx; n > log_.commitIndex(); --n) {
        if (log_.termAt(n) != curTerm) {
            continue; // Figure-8: only commit own-term entries by count
        }
        // Count nodes (including self) with matchIndex >= n.
        int count = 0;
        for (NodeId node : cfg_.members) {
            auto it = matchIndex_.find(node);
            if (it != matchIndex_.end() && it->second >= n) {
                ++count;
            }
        }
        if (count >= cfg_.majority()) {
            log_.setCommitIndex(n);
            break; // highest such N found; prior entries commit transitively
        }
    }
}

} // namespace rune::consensus
