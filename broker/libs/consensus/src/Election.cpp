// Election.cpp — Faithful C++ port of io.aeron.cluster.Election.
// P2P-5a: INIT, CANVASS, NOMINATE, CANDIDATE_BALLOT, FOLLOWER_BALLOT,
//         onCanvassPosition, onRequestVote, onVote.
// P2P-5b: LeaderLogReplication..Closed; onNewLeadershipTerm, onAppendPosition,
//         onCommitPosition. Archive parts are pass-throughs (no Aeron Archive in Phase 2).

#include <bit>
#include <rune/consensus/Election.h>
#include <rune/consensus/ClusterMember.h>
#include <rune_fr/FlightRecorder.h>
#include <algorithm>
#include <cassert>
#include <stdexcept>
#include <cstdio>
#include <cstdlib>

namespace rune::consensus {

// TEMP consensus diagnostics (RUNE_CTRL_LOG=1): trace election state transitions
// and AppendPosition receipt/drops so we can see why the eligible set collapses
// after a node death.  Remove once the failover-eligibility bug is fixed.
static bool electLog() noexcept {
    static const bool on = ::getenv("RUNE_CTRL_LOG") != nullptr;
    return on;
}

// ---------------------------------------------------------------------------
// Construction — Election.java:110-149
// ---------------------------------------------------------------------------
Election::Election(bool             isNodeStartup,
                   NodeId           gracefulClosedLeaderId,
                   Term             leadershipTermId,
                   Term             logLeadershipTermId,
                   int64_t          logPosition,
                   int64_t          appendPosition,
                   std::vector<ClusterMember> clusterMembers,
                   NodeId           thisMemberId,
                   ConsensusPublisher& publisher,
                   NodeStateFile&   nodeStateFile,
                   TermLog&         termLog,
                   int64_t          electionTimeoutNs,
                   int64_t          electionStatusIntervalNs,
                   int64_t          startupCanvassTimeoutNs,
                   uint64_t         rngSeed)
    : clusterMembers_(std::move(clusterMembers))
    , thisMemberId_(thisMemberId)
    , publisher_(PublisherRef::of(publisher))
    , nodeStateFile_(nodeStateFile)
    , termLog_(termLog)
    , logPosition_(logPosition)
    , appendPosition_(appendPosition)
    , leadershipTermId_(leadershipTermId)
    , logLeadershipTermId_(logLeadershipTermId)
    , candidateTermId_(leadershipTermId)
    , notifiedCommitPosition_(logPosition)
    , isNodeStartup_(isNodeStartup)
    , isLeaderStartup_(isNodeStartup)
    , isExtendedCanvass_(isNodeStartup)
    , gracefulClosedLeaderId_(gracefulClosedLeaderId)
    , electionTimeoutNs_(electionTimeoutNs)
    , electionStatusIntervalNs_(electionStatusIntervalNs)
    , startupCanvassTimeoutNs_(startupCanvassTimeoutNs)
    , rng_(rngSeed)
{
    // Election.java:142-143: initialTimeOfLastUpdateNs = nowNs - DAYS.toNanos(1)
    // We initialise to a value that guarantees the first interval check fires
    // on the first doWork call. Use INT64_MIN/2 as a safe "very far in the past".
    initialTimeOfLastUpdateNs_ = INT64_MIN / 2;
    timeOfLastUpdateNs_        = initialTimeOfLastUpdateNs_;

    // D24 review MAJOR-1: the durable follow floor (and the ack term it
    // restores) is in force from construction — never only from init(): a
    // message handled before the first doWork must already see it.
    followFloorTermId_ = std::max(followFloorTermId_, nodeStateFile_.followFloorTermId());
    votedTermId_       = std::max(votedTermId_, followFloorTermId_);

    // Follower leader-liveness timeout (mirrors Aeron's leaderHeartbeatTimeoutNs).
    // A follower re-enters CANVASS if it hears no leader heartbeat within this
    // window.  The leader broadcasts NLT+CommitPosition every
    // electionStatusIntervalNs_; we require several missed heartbeats before
    // declaring the leader dead, and never less than one election timeout, so a
    // transient scheduling hiccup does not trigger a spurious re-election.
    //   timeout = max(electionTimeoutNs, 3 * electionStatusIntervalNs)
    leaderHeartbeatTimeoutNs_ = std::max<int64_t>(
        electionTimeoutNs_, 3 * electionStatusIntervalNs_);

    // Steady-state follower duties: send our AppendPosition on the status interval;
    // detect a dead leader after leaderHeartbeatTimeoutNs_.
    followerLiveness_.configure(electionStatusIntervalNs_, leaderHeartbeatTimeoutNs_);

    // Pre-allocate rankedPositions for quorumPosition — no alloc in the hot path.
    const int threshold = ClusterMember::quorumThreshold(
        static_cast<int>(clusterMembers_.size()));
    rankedPositions_.assign(threshold, 0);

    // Election.java:147: ctx.electionStateCounter().setRelease(INIT.code())
    stateCode_ = code(ElectionState::Init);
}

// ---------------------------------------------------------------------------
// Public accessors
// ---------------------------------------------------------------------------

ElectionState Election::state()    const noexcept { return state_; }
int32_t       Election::stateCode() const noexcept { return stateCode_; }
Term          Election::leadershipTermId()       const noexcept { return leadershipTermId_; }
Term          Election::candidateTermId()        const noexcept { return candidateTermId_; }
Term          Election::logLeadershipTermId()    const noexcept { return logLeadershipTermId_; }
int64_t       Election::logPosition()            const noexcept { return logPosition_; }
int64_t       Election::appendPosition()         const noexcept { return appendPosition_; }
int64_t       Election::notifiedCommitPosition() const noexcept { return notifiedCommitPosition_; }
NodeId        Election::thisMemberId()           const noexcept { return thisMemberId_; }
NodeId        Election::leaderMemberId()         const noexcept {
    return leaderMember_ ? leaderMember_->id : -1;
}

bool Election::isClosed() const noexcept {
    return state_ == ElectionState::Closed;
}

// eligibleNodeIds — live cluster members for partition-failover reassignment.
// A member is live if it appended a position within timeoutNs (Aeron liveness).
// The leader itself is always included when this node is the leader (it never
// records its own AppendPosition).  ALLOCATION-FREE: fills out[0..cap) and returns
// the count — the per-duty-cycle liveness poll must not heap-allocate.  Sorted
// ascending, de-duplicated.
std::size_t Election::eligibleNodeIds(int64_t nowNs, int64_t timeoutNs,
                                      NodeId* out, std::size_t cap) const noexcept {
    if (timeoutNs <= 0) {
        timeoutNs = leaderHeartbeatTimeoutNs_;
    }

    const NodeId selfId   = thisMemberId_;
    const bool   selfLeads =
        (leaderMember_ != nullptr) && (leaderMember_->id == selfId);

    // Become-leader grace (controlled-shutdown review D2): a freshly elected
    // leader has no AppendPosition from its peers in the new term yet (their
    // clocks were reset at canvass) — for one window from its first leader
    // state every peer counts as live, so it never fails partitions over to
    // itself with ISR={self}.  A dead peer is dropped after the window.
    const bool leaderGrace = selfLeads && leaderSinceNs_ != INT64_MIN && nowNs - leaderSinceNs_ < timeoutNs;
    std::size_t n = 0;
    for (const ClusterMember& m : clusterMembers_) {
        const bool isSelfLeaderEntry = selfLeads && (m.id == selfId);
        if (isSelfLeaderEntry || leaderGrace || m.isActive(nowNs, timeoutNs)) {
            if (n < cap) out[n++] = m.id;
        }
    }

    std::sort(out, out + n);
    n = static_cast<std::size_t>(std::unique(out, out + n) - out);
    return n;
}

// nextDeadlineNs — returns the next clock deadline this state is waiting on.
// Tests jump nowNs to exactly this value to avoid busy-spin.
int64_t Election::nextDeadlineNs() const noexcept {
    switch (state_) {
        case ElectionState::Init:
            // Init transitions immediately — no deadline.
            return INT64_MIN;

        case ElectionState::Canvass: {
            // Two potential fires: interval publish and timeout.
            int64_t intervalDeadline = timeOfLastUpdateNs_ + electionStatusIntervalNs_;
            int64_t timeoutDeadline  = timeOfLastStateChangeNs_ +
                (isExtendedCanvass_ ? startupCanvassTimeoutNs_ : electionTimeoutNs_);
            return std::min(intervalDeadline, timeoutDeadline);
        }

        case ElectionState::Nominate:
            // Wait for nominationDeadlineNs, or interval publish.
            // r1 LOW-2: a won pre-ballot is due at once (nominationDeadlineNs_ passed).
            return std::min(preBallot_.active && !preBallotWon_() ? preBallot_.deadlineNs : nominationDeadlineNs_,
                            timeOfLastUpdateNs_ + electionStatusIntervalNs_);

        case ElectionState::CandidateBallot:
            return timeOfLastStateChangeNs_ + electionTimeoutNs_;

        case ElectionState::FollowerBallot:
            return timeOfLastStateChangeNs_ + electionTimeoutNs_;

        case ElectionState::LeaderLogReplication:
        case ElectionState::LeaderReplay:
        case ElectionState::LeaderReady:
            // Heartbeat interval.
            return timeOfLastUpdateNs_ + electionStatusIntervalNs_;

        case ElectionState::LeaderInit:
        case ElectionState::FollowerLogReplication:
        case ElectionState::FollowerReplay:
        case ElectionState::FollowerCatchupInit:
        case ElectionState::FollowerCatchupAwait:
        case ElectionState::FollowerCatchup:
        case ElectionState::FollowerLogInit:
        case ElectionState::FollowerLogAwait:
        case ElectionState::FollowerReady:
            // These transition immediately (pass-through) — report no future deadline.
            return INT64_MIN;

        case ElectionState::Closed:
            // Multi-node leader in Closed state continues heartbeat for late joiners.
            if (leaderMember_ != nullptr &&
                leaderMember_->id == thisMemberId_ &&
                clusterMembers_.size() > 1) {
                return timeOfLastUpdateNs_ + electionStatusIntervalNs_;
            }
            // Multi-node FOLLOWER in Closed: the FollowerLiveness SM owns the
            // wake-up deadline — min(next AppendPosition send, leader-liveness
            // expiry).  Keeps the leader's view of us fresh and detects a dead
            // leader promptly.
            if (clusterMembers_.size() > 1 &&
                leaderMember_ != nullptr &&
                leaderMember_->id != thisMemberId_) {
                return followerLiveness_.nextDeadlineNs();
            }
            return INT64_MIN;
    }
    return INT64_MIN;
}

// ---------------------------------------------------------------------------
// doWork — Election.java:181-260
// ---------------------------------------------------------------------------
int Election::doWork(int64_t nowNs)
{
    RUNE_FR_SPAN_CONSENSUS(rune_fr::Site::ElectionStep, 0);
    int workCount = 0;
    switch (state_) {
        case ElectionState::Init:
            workCount += init(nowNs);
            break;
        case ElectionState::Canvass:
            workCount += canvass(nowNs);
            break;
        case ElectionState::Nominate:
            workCount += nominate(nowNs);
            break;
        case ElectionState::CandidateBallot:
            workCount += candidateBallot(nowNs);
            break;
        case ElectionState::FollowerBallot:
            workCount += followerBallot(nowNs);
            break;
        // P2P-5b: leader-log states
        case ElectionState::LeaderLogReplication:
            workCount += leaderLogReplication(nowNs);
            break;
        case ElectionState::LeaderReplay:
            workCount += leaderReplay(nowNs);
            break;
        case ElectionState::LeaderInit:
            workCount += leaderInit(nowNs);
            break;
        case ElectionState::LeaderReady:
            workCount += leaderReady(nowNs);
            break;
        // P2P-5b: follower-log states
        case ElectionState::FollowerLogReplication:
            // No archive replication in Phase 2 — fall through to replay immediately.
            state(ElectionState::FollowerReplay, nowNs, "no-archive skip log replication");
            workCount++;
            break;
        case ElectionState::FollowerReplay:
            workCount += followerReplay(nowNs);
            break;
        case ElectionState::FollowerCatchupInit:
            workCount += followerCatchupInit(nowNs);
            break;
        case ElectionState::FollowerCatchupAwait:
            workCount += followerCatchupAwait(nowNs);
            break;
        case ElectionState::FollowerCatchup:
            workCount += followerCatchup(nowNs);
            break;
        case ElectionState::FollowerLogInit:
            workCount += followerLogInit(nowNs);
            break;
        case ElectionState::FollowerLogAwait:
            workCount += followerLogAwait(nowNs);
            break;
        case ElectionState::FollowerReady:
            workCount += followerReady(nowNs);
            break;
        case ElectionState::Closed:
            workCount += closedLeaderHeartbeat(nowNs);
            break;
    }
    return workCount;
}

// ---------------------------------------------------------------------------
// INIT — Election.java:653-698
// ---------------------------------------------------------------------------
int Election::init(int64_t nowNs)
{
    // Election.java:681: candidateTermId = max(nodeStateFile.candidateTerm().candidateTermId(), leadershipTermId)
    Term persistedTerm = nodeStateFile_.candidateTermId();
    // D24: the durable follow floor survives the restart; the ack term with it
    // (a grant / lead / follow before the crash still refuses an older term).
    followFloorTermId_ = std::max(followFloorTermId_, nodeStateFile_.followFloorTermId());
    votedTermId_       = std::max(votedTermId_, followFloorTermId_);
    // Review LOW-2: never stand at (or below) a term already granted / followed.
    candidateTermId_   = std::max({persistedTerm, leadershipTermId_, followFloorTermId_});

    const int memberCount = static_cast<int>(clusterMembers_.size());

    if (memberCount == 1) {
        // Election.java:683-691: single-node shortcut — win immediately.
        candidateTermId_++;
        raiseFollowFloor_(candidateTermId_);   // D24: leads it (durable first — review LOW-1)

        // Point leaderMember at the single (self) member.
        leaderMember_     = memberById(thisMemberId_);
        leadershipTermId_ = candidateTermId_;

        // Election.java:690: state(LEADER_LOG_REPLICATION, nowNs, "single-node cluster leader")
        state(ElectionState::LeaderLogReplication, nowNs, "single-node cluster leader");
    } else {
        // Election.java:694: state(CANVASS, nowNs, "")
        state(ElectionState::Canvass, nowNs, "");
    }

    return 1;
}

// ---------------------------------------------------------------------------
// CANVASS — Election.java:700-729
// ---------------------------------------------------------------------------
int Election::canvass(int64_t nowNs)
{
    int workCount = 0;

    // Election.java:703-705: deadline
    // NOTE: Java uses consensusModuleAgent.timeOfLastLeaderUpdateNs() for non-startup;
    // we use timeOfLastStateChangeNs_ + electionTimeoutNs_ as a standalone approximation.
    const int64_t deadlineNs = isExtendedCanvass_
        ? timeOfLastStateChangeNs_ + startupCanvassTimeoutNs_
        : timeOfLastStateChangeNs_ + electionTimeoutNs_;

    // Election.java:707-712: publish CanvassPosition on interval
    if (hasUpdateIntervalExpired(nowNs, electionStatusIntervalNs_)) {
        timeOfLastUpdateNs_ = nowNs;
        publishCanvassPosition();   // a resigned leader too: its position is what lets
                                    // the others count its vote (a quorum without it may not exist)
        workCount++;
    }

    // Election.java:720-727: check candidate readiness
    // The candidacy rule (setVoteGate) is applied by VOTERS only (re-review
    // 2026-10-04 MAJOR-A): this node may stand whatever its own — possibly
    // stale — internal-ISR knowledge says; a candidate outside the newest
    // committed ISR is denied by the voters (any grant majority intersects the
    // commit majority).  A self-gate turned stale self-knowledge into a
    // permanently headless consensus.
    // Controlled shutdown: a resigned leader (or a node that withdrew its
    // candidacy) never stands again — it still canvasses and votes, so the
    // others can elect without it.  A resigned leader re-sends its StandDown
    // on the status interval (a lost one would leave a follower waiting for
    // the heartbeat timeout) and waits to follow the successor.
    if (candidacyBlocks_(nowNs, workCount)) return workCount;

    ClusterMember* thisM = memberById(thisMemberId_);
    if (thisM != nullptr) {
        // Election.java:720: isUnanimousCandidate
        if (ClusterMember::isUnanimousCandidate(clusterMembers_, *thisM, gracefulClosedLeaderId_)) {
            // Election.java:723-725: random backoff
            std::uniform_real_distribution<double> dist(0.0, 1.0);
            int64_t delayNs = static_cast<int64_t>(dist(rng_) * static_cast<double>(electionTimeoutNs_ >> 1));
            nominationDeadlineNs_ = nowNs + delayNs;
            state(ElectionState::Nominate, nowNs, "unanimous candidate");
            workCount++;
        } else if (nowNs >= deadlineNs &&
                   ClusterMember::isQuorumCandidate(clusterMembers_, *thisM)) {
            // Election.java:721: deadline + isQuorumCandidate
            std::uniform_real_distribution<double> dist(0.0, 1.0);
            int64_t delayNs = static_cast<int64_t>(dist(rng_) * static_cast<double>(electionTimeoutNs_ >> 1));
            nominationDeadlineNs_ = nowNs + delayNs;
            state(ElectionState::Nominate, nowNs, "quorum candidate after deadline");
            workCount++;
        }
    }

    return workCount;
}

// ---------------------------------------------------------------------------
// NOMINATE — Election.java:732-751
// ---------------------------------------------------------------------------
int Election::nominate(int64_t nowNs)
{
    // Election.java:734-741: if deadline passed → become candidate
    if (nowNs >= nominationDeadlineNs_) {
        // D29 PreVote (Raft §9.6, a Rune extension): before raising the durable
        // candidate term, a NON-BINDING pre-ballot at candidateTermId_ + 1 — a
        // member that hears a live leader denies it, so a node cut off from the
        // leader never bumps a real term (no term inflation, and a bridge member
        // never grants it a binding vote that would raise its follow floor).
        if (clusterMembers_.size() > 1) {
            // r1 LOW-3: a deny-with-raise lifted candidateTermId_ past the running
            // pre-ballot's term — ask again at the new term + 1.
            if (!preBallot_.active || preBallot_.term <= candidateTermId_) {
                startPreBallot_(nowNs);
                return 1;
            }
            // r1 LOW-1: the self pre-vote follows the CURRENT gate, like the real
            // self-vote (gateSelfVote_) — our knowledge may have changed since.
            if (!mayVoteFor(thisMemberId_)) preBallot_.grantedMask &= ~selfBit_();
            if (preBallot_.denied || !preBallotWon_()) {
                if (nowNs >= preBallot_.deadlineNs || preBallot_.denied) {
                    preBallot_ = PreBallot{};
                    ++preBallotsLost_;
                    state(ElectionState::Canvass, nowNs, "pre-ballot without a majority");
                    return 1;
                }
                if (hasUpdateIntervalExpired(nowNs, electionStatusIntervalNs_)) {   // lost requests: re-ask
                    timeOfLastUpdateNs_ = nowNs;
                    publishCanvassPosition();   // review MEDIUM-2: peers keep counting us while we pre-ballot
                    sendPreVoteRequests_();
                    return 1;
                }
                return 0;
            }
            preBallot_ = PreBallot{};
        }
        // Election.java:736-737: proposeMaxCandidateTermId(candidateTermId+1, logPosition, time)
        candidateTermId_ = nodeStateFile_.proposeMaxCandidateTermId(
            candidateTermId_ + 1, logPosition_, /*timestampMs=*/0);

        // Election.java:738: ClusterMember.becomeCandidate(clusterMembers, candidateTermId, thisMember.id())
        ClusterMember::becomeCandidate(clusterMembers_, candidateTermId_, thisMemberId_);
        gateSelfVote_();   // CRITICAL-R3: the self-vote counts only if our knowledge admits us

        // Election.java:739: state(CANDIDATE_BALLOT, nowNs, "")
        state(ElectionState::CandidateBallot, nowNs, "nomination deadline reached");
        return 1;
    }

    // Election.java:742-748: still waiting — publish canvass position on interval
    if (hasUpdateIntervalExpired(nowNs, electionStatusIntervalNs_)) {
        timeOfLastUpdateNs_ = nowNs;
        publishCanvassPosition();
        return 1;
    }

    return 0;
}

// ---------------------------------------------------------------------------
// CANDIDATE_BALLOT — Election.java:754-793
// ---------------------------------------------------------------------------
int Election::candidateBallot(int64_t nowNs)
{
    int workCount = 0;
    // CRITICAL-R3: re-evaluate our own vote — knowledge learned from a peer
    // during the ballot may admit (or exclude) us.
    gateSelfVote_();

    // Election.java:758-763: unanimous check (no timeout needed)
    if (ClusterMember::isUnanimousLeader(clusterMembers_, candidateTermId_, gracefulClosedLeaderId_)) {
        raiseFollowFloor_(candidateTermId_);   // D24: leads it (durable before any NLT; review LOW-1: first)
        leaderMember_     = memberById(thisMemberId_);
        leadershipTermId_ = candidateTermId_;
        // Election.java:762: state(LEADER_LOG_REPLICATION, nowNs, "unanimous leader")
        state(ElectionState::LeaderLogReplication, nowNs, "unanimous leader");
        workCount++;
        return workCount;
    }

    // Election.java:765-779: timeout check
    if (nowNs >= (timeOfLastStateChangeNs_ + electionTimeoutNs_)) {
        if (ClusterMember::isQuorumLeader(clusterMembers_, candidateTermId_)) {
            // Election.java:769-771: quorum leader
            raiseFollowFloor_(candidateTermId_);   // D24: leads it (durable before any NLT; review LOW-1: first)
            leaderMember_     = memberById(thisMemberId_);
            leadershipTermId_ = candidateTermId_;
            state(ElectionState::LeaderLogReplication, nowNs, "quorum leader after timeout");
        } else {
            // Election.java:775: no quorum → back to CANVASS
            state(ElectionState::Canvass, nowNs, "timeout without quorum");
        }
        workCount++;
        return workCount;
    }

    // Election.java:781-790: send RequestVote to peers not yet balloted
    for (auto& member : clusterMembers_) {
        if (member.id == thisMemberId_) continue; // skip self (already voted for self)
        if (!member.isBallotSent) {
            publishCandidacyState_(member.id);   // CRITICAL-R3: our knowledge first
            // Election.java:787-789: requestVote
            publisher_.requestVote(
                member.id,
                logLeadershipTermId_,
                appendPosition_,
                candidateTermId_,
                thisMemberId_);
            member.isBallotSent = true;
            workCount++;
        }
    }

    return workCount;
}

// ---------------------------------------------------------------------------
// FOLLOWER_BALLOT — Election.java:796-807
// ---------------------------------------------------------------------------
int Election::followerBallot(int64_t nowNs)
{
    int workCount = 0;
    // Election.java:800-803
    if (nowNs >= (timeOfLastStateChangeNs_ + electionTimeoutNs_)) {
        state(ElectionState::Canvass, nowNs, "follower ballot timeout");
        workCount++;
    }
    return workCount;
}

// ---------------------------------------------------------------------------
// LEADER_LOG_REPLICATION — Election.java:809-825
// New leader broadcasts NLT + CommitPosition on heartbeat interval and waits
// for quorumPosition >= appendPosition (followers report via onAppendPosition).
// ARCHIVE PASS-THROUGH: no real log publication — logPosition already == appendPosition.
// ---------------------------------------------------------------------------
int Election::leaderLogReplication(int64_t nowNs)
{
    int workCount = 0;

    // D29b (review LOW-6 / r2 MEDIUM-3): leader-state CheckQuorum.
    if (leaderStatesTimedOut_(nowNs)) {
        standDownFromLeaderStates_(nowNs);
        return 1;
    }

    // Election.java:812-817: publish NLT + CommitPosition on interval
    if (hasUpdateIntervalExpired(nowNs, electionStatusIntervalNs_)) {
        timeOfLastUpdateNs_ = nowNs;
        publishNewLeadershipTermToAll(nowNs);
        workCount++;
    }

    // Election.java:818: thisMember.logPosition(appendPosition)
    ClusterMember* thisM = memberById(thisMemberId_);
    if (thisM != nullptr) {
        thisM->logPosition              = appendPosition_;
        thisM->leadershipTermId         = leadershipTermId_;
        thisM->timeOfLastAppendPositionNs = nowNs;
    }

    // Election.java:820-824: if quorumPosition >= appendPosition → LEADER_REPLAY
    int64_t qPos = ClusterMember::quorumPosition(
        clusterMembers_, rankedPositions_, nowNs, electionTimeoutNs_);
    if (qPos >= appendPosition_) {
        // Election.java:822: logPosition = appendPosition (archive pass-through)
        logPosition_ = appendPosition_;
        state(ElectionState::LeaderReplay, nowNs, "quorum reached appendPosition");
        workCount++;
    }

    return workCount;
}

// ---------------------------------------------------------------------------
// LEADER_REPLAY — Election.java:828-865
// Replay log from logPosition to appendPosition.
// ARCHIVE PASS-THROUGH: nothing to replay (already done above), advance immediately.
// ---------------------------------------------------------------------------
int Election::leaderReplay(int64_t nowNs)
{
    // Election.java:831-833: if logPosition == appendPosition → LEADER_INIT immediately
    // PHASE-2-ARCHIVE-DEFERRED: Aeron Archive log replication is not wired in Phase 2.
    // We assert the Phase-2 precondition that no log gap exists between logPosition_
    // and appendPosition_. If this assertion fires, wire the real FollowerLogReplication
    // path before enabling Archive integration (Phase 3).
    // Audit finding C5: without this guard a leader with appendPosition > logPosition
    // would publish NLT at appendPosition, over-counting quorum at a position followers
    // don't actually hold. In Phase 2 all nodes start from position 0; this holds.
    assert(logPosition_ == appendPosition_
        && "PHASE-2-DEFERRED: leader has un-replayed log gap; wire Archive before enabling");
    state(ElectionState::LeaderInit, nowNs, "leader replay pass-through");
    return 1;
}

// ---------------------------------------------------------------------------
// LEADER_INIT — Election.java:868-874
// One-shot: record term in TermLog (joinLogAsLeader in real impl), → LEADER_READY.
// ---------------------------------------------------------------------------
int Election::leaderInit(int64_t nowNs)
{
    // Election.java:870-872: joinLogAsLeader(leadershipTermId, logPosition, logSessionId, isLeaderStartup)
    // Phase 2 equivalent: record the term in TermLog.
    // Only append if we don't already have a valid entry for this term.
    if (!termLog_.getTermEntry(leadershipTermId_)) {
        termLog_.appendTerm(
            /*recordingId=*/0,
            leadershipTermId_,
            /*termBaseLogPosition=*/logPosition_,
            /*timestamp=*/0);
    }

    state(ElectionState::LeaderReady, nowNs, "leader init done");
    return 1;
}

// ---------------------------------------------------------------------------
// LEADER_READY — Election.java:877-894
// Publishes NLT on heartbeat interval; checks hasQuorumAtPosition.
// When quorum confirms logPosition → electionComplete → CLOSED.
// ---------------------------------------------------------------------------
int Election::leaderReady(int64_t nowNs)
{
    int workCount = 0;

    // D29b (review LOW-6 / r2 MEDIUM-3): leader-state CheckQuorum.
    if (leaderStatesTimedOut_(nowNs)) {
        standDownFromLeaderStates_(nowNs);
        return 1;
    }

    // Election.java:880-887: publish NLT on interval
    if (hasUpdateIntervalExpired(nowNs, electionStatusIntervalNs_)) {
        timeOfLastUpdateNs_ = nowNs;
        publishNewLeadershipTermToAll(nowNs);
        workCount++;
    }

    // Election.java:888: updateLeaderPosition (update self logPosition)
    ClusterMember* thisM = memberById(thisMemberId_);
    if (thisM != nullptr) {
        thisM->logPosition              = logPosition_;
        thisM->leadershipTermId         = leadershipTermId_;
        thisM->timeOfLastAppendPositionNs = nowNs;
    }

    // Election.java:890-893: hasQuorumAtPosition → appendNewLeadershipTermEvent → electionComplete
    // Aeron-faithful: check leadershipTermId + logPosition only (ref spec line ~125).
    // logLeadershipTermId_ is intentionally NOT passed — AppendPosition wire messages
    // do not carry that field, so followers never have it set; including it would cause
    // permanent quorum deadlock once logLeadershipTermId_ != 0 (audit N1, 2026-09-11).
    if (ClusterMember::hasQuorumAtPosition(
            clusterMembers_, leadershipTermId_,
            logPosition_, nowNs, electionTimeoutNs_)) {
        // Election.java:891: appendNewLeadershipTermEvent (Phase 2: no-op / already in TermLog)
        electionComplete(nowNs);
        workCount++;
    }

    return workCount;
}

// ---------------------------------------------------------------------------
// FOLLOWER_REPLAY — Election.java:960-1017
// Follower replays local log from logPosition up to min(appendPosition, notifiedCommitPosition).
// ARCHIVE PASS-THROUGH: logPosition == appendPosition already → skip to FollowerLogInit.
// ---------------------------------------------------------------------------
int Election::followerReplay(int64_t nowNs)
{
    // Election.java:967-968: if logPosition == appendPosition (nothing to replay)
    //   → if no catchup needed: FOLLOWER_LOG_INIT
    // PHASE-2-ARCHIVE-DEFERRED: always nothing to replay (Archive not wired).
    // Assert the Phase-2 precondition to prevent silent quorum over-count (audit C5):
    // a follower with logPosition < appendPosition would need catchup replication
    // before being counted toward quorum — but without Archive it would be counted
    // at its local appendPosition anyway. Guard this by asserting no gap exists.
    // In Phase 2 all nodes start from position 0; this precondition holds.
    assert(logPosition_ == appendPosition_
        && "PHASE-2-DEFERRED: follower has un-replayed gap; wire Archive catchup before enabling");
    state(ElectionState::FollowerLogInit, nowNs, "follower replay pass-through");
    return 1;
}

// ---------------------------------------------------------------------------
// FOLLOWER_CATCHUP_INIT — Election.java:1020-1055  (Archive-dependent, pass-through)
// ---------------------------------------------------------------------------
int Election::followerCatchupInit(int64_t nowNs)
{
    // Phase 2 pass-through: no Aeron Archive catchup.
    state(ElectionState::FollowerLogInit, nowNs, "follower catchup init pass-through");
    return 1;
}

// ---------------------------------------------------------------------------
// FOLLOWER_CATCHUP_AWAIT — Election.java:1058-1086  (pass-through)
// ---------------------------------------------------------------------------
int Election::followerCatchupAwait(int64_t nowNs)
{
    state(ElectionState::FollowerLogInit, nowNs, "follower catchup await pass-through");
    return 1;
}

// ---------------------------------------------------------------------------
// FOLLOWER_CATCHUP — Election.java:1089-1112  (pass-through)
// ---------------------------------------------------------------------------
int Election::followerCatchup(int64_t nowNs)
{
    state(ElectionState::FollowerLogInit, nowNs, "follower catchup pass-through");
    return 1;
}

// ---------------------------------------------------------------------------
// FOLLOWER_LOG_INIT — Election.java:1115-1131
// If logSubscription == null (no prior catchup): add follower subscription → FOLLOWER_LOG_AWAIT.
// Else (post-catchup): → FOLLOWER_READY.
// Phase 2 pass-through: no Aeron log subscription needed — go directly to FOLLOWER_READY.
// ---------------------------------------------------------------------------
int Election::followerLogInit(int64_t nowNs)
{
    // Election.java:1128: else (subscription exists from catchup) → FOLLOWER_READY
    // Phase 2: always go directly to FOLLOWER_READY (no subscription).
    state(ElectionState::FollowerReady, nowNs, "follower log init pass-through");
    return 1;
}

// ---------------------------------------------------------------------------
// FOLLOWER_LOG_AWAIT — Election.java:1134-1163
// Waits for live log image by logSessionId; pass-through in Phase 2.
// ---------------------------------------------------------------------------
int Election::followerLogAwait(int64_t nowNs)
{
    // Phase 2 pass-through: no Aeron image wait.
    state(ElectionState::FollowerReady, nowNs, "follower log await pass-through");
    return 1;
}

// ---------------------------------------------------------------------------
// FOLLOWER_READY — Election.java:1166-1179
// Publishes AppendPosition(leadershipTermId, logPosition, thisMemberId, FLAG_NONE) to leader once.
// Then calls electionComplete → CLOSED.
// ---------------------------------------------------------------------------
int Election::followerReady(int64_t nowNs)
{
    // Election.java:1170-1178: publish AppendPosition to leader, then electionComplete
    if (leaderMember_ != nullptr) {
        publisher_.appendPosition(
            leaderMember_->id,
            leadershipTermId_,
            logPosition_,
            thisMemberId_,
            /*flags=*/0);
    }
    electionComplete(nowNs);
    return 1;
}

// ---------------------------------------------------------------------------
// onCanvassPosition — Election.java:284-332
// ---------------------------------------------------------------------------
void Election::onCanvassPosition(Term    logLeadershipTermId,
                                 int64_t logPosition,
                                 Term    leadershipTermId,
                                 NodeId  followerMemberId,
                                 int32_t flags)
{
    // Election.java:291-294
    if (state_ == ElectionState::Init) return;

    // Skip messages from self
    if (followerMemberId == thisMemberId_) return;

    // ISR v3 R12: founding evidence.  "Fresh" = the sender says (valid flags) it has no history; an invalid-flags
    // canvass (an old sender's 0) or a history flag is history.
    noteHeard_(followerMemberId);
    if ((flags & 2) != 0 && (flags & 1) == 0) {
        if (followerMemberId >= 0 && followerMemberId < 32) heardFreshMask_ |= 1u << followerMemberId;
    } else {
        anyHistory_ = true;
        peerWithHistory_ = followerMemberId;
    }
    if (leadershipTermId > maxPeerLeadershipTermId_) maxPeerLeadershipTermId_ = leadershipTermId;

    // Election.java:296-299: clear gracefulClosedLeaderId if it was this member
    // — except a leader that RESIGNED to us (StandDown, controlled shutdown):
    // it keeps canvassing (its vote may be needed for a quorum), and the skip
    // must hold for this election (review LOW-1).
    if (followerMemberId == gracefulClosedLeaderId_ && !skipFromStandDown_) {
        gracefulClosedLeaderId_ = -1; // NULL_VALUE
    }

    // Election.java:301-304: update the follower's known log position
    ClusterMember* follower = memberById(followerMemberId);
    if (follower != nullptr) {
        follower->logLeadershipTermId = logLeadershipTermId;
        follower->logPosition         = logPosition;
        follower->leadershipTermId    = leadershipTermId;
        // Note: updateMemberLogPosition in Java also sets timeOfLastAppendPositionNs;
        // we leave that for P2P-5b when we have a real clock to pass.
    }
}

// ---------------------------------------------------------------------------
// onRequestVote — Election.java:334-380
// ---------------------------------------------------------------------------
void Election::onRequestVote(Term    logLeadershipTermId,
                              int64_t logPosition,
                              Term    candidateTermId,
                              NodeId  candidateId,
                              int64_t nowNs)
{
    // Election.java:341-344
    if (state_ == ElectionState::Init) return;

    // Election.java:346-349: ignore vote requests from self
    if (candidateId == thisMemberId_) return;
    noteHeard_(candidateId);   // ISR v3 R12: the heard bit only - a candidate's term is NEVER founding evidence
    if (abstains_()) return;   // ISR v3: an untrusted storage takes no part in another node's ballot (no Against veto)

    // ClosedLeaderStepDownRegression: Aeron ConsensusModuleAgent.onRequestVote —
    // outside an election a candidate with a higher term than ours means the
    // cluster moved on → enterElection().  We then evaluate THIS request like any
    // Canvass node would (grant iff its log is at least as fresh), instead of
    // dropping it: a CLOSED node that never answers withholds the vote the
    // survivors need for a majority (DR-10 flap: headless 4+ min).
    // Only a candidate we would GRANT pulls us out (log at least as fresh as ours
    // and allowed by the candidacy gate): a denied candidacy — stale log or a
    // member excluded from the internal ISR — must never unseat a live leader
    // (MAJOR-R3); the deny below still raises our candidate term as before.
    if (state_ == ElectionState::Closed && clusterMembers_.size() > 1 &&
        candidateTermId > leadershipTermId_ && candidateTermId > candidateTermId_ &&
        memberById(candidateId) != nullptr &&
        ClusterMember::compareLog(logLeadershipTermId_, appendPosition_,
                                  logLeadershipTermId, logPosition) <= 0 &&
        mayVoteFor(candidateId)) {
        // DR-10 review MEDIUM-1 — leader stickiness (Raft §9.6 / etcd CheckQuorum):
        // while this CLOSED node still has a working leadership (as leader: an
        // active follower quorum; as follower: a leader heard within the
        // heartbeat timeout) a higher-term candidate is answered with a DENY and
        // never pulls it out — else a partial partition ping-pongs leadership
        // (the cut-off node campaigns, the bridge node grants, repeat).  A dead /
        // paused leader (DR-10) has neither, so the zombie path is unchanged.
        const bool selfLeads = leaderMember_ != nullptr && leaderMember_->id == thisMemberId_;
        // D29: a higher-term candidacy from the leader WE follow means it stepped
        // down (TermFloorNotice / lost quorum) — never sticky against it.
        const bool ownLeaderAsks = !selfLeads && leaderMember_ != nullptr && leaderMember_->id == candidateId;
        const bool sticky = !ownLeaderAsks && (selfLeads
            ? hasActiveFollowerQuorum_(nowNs)
            : (followerLiveness_.armed() &&
               nowNs - followerLiveness_.lastLeaderUpdateNs() < leaderHeartbeatTimeoutNs_));
        if (sticky) {
            placeVote(candidateTermId, candidateId, false);
            return;
        }
        leaveClosedForElection_(leadershipTermId_, nowNs,
                                "higher-term RequestVote in CLOSED → re-election");
    }

    if (candidateTermId <= candidateTermId_) {
        // Election.java:351-353: stale candidate → deny
        placeVote(candidateTermId, candidateId, false);
    } else if (state_ != ElectionState::Closed && leaderLeaseLive_(nowNs, candidateId)) {
        // D29 review MEDIUM-1 (etcd: votes are ignored in lease): we heard a live
        // leader within the heartbeat timeout — a binding grant now would raise
        // our floor above it (the ratchet PreVote removes; old-version candidates
        // skip the pre-ballot).  Deny WITHOUT adopting the term.
        placeVote(candidateTermId, candidateId, false);
    } else if (ClusterMember::compareLog(logLeadershipTermId_, appendPosition_,
                                         logLeadershipTermId, logPosition) > 0 ||
               !mayVoteFor(candidateId)) {
        // MAJOR-2/3: a candidate outside the newest durable internal ISR is
        // denied exactly like one with a stale log.
        // Election.java:355-371: our log is more recent → deny but update our candidateTermId
        // Election.java:357-358: proposeMaxCandidateTermId to update our persisted term
        candidateTermId_ = nodeStateFile_.proposeMaxCandidateTermId(
            candidateTermId, logPosition, /*timestampMs=*/0);
        placeVote(candidateTermId, candidateId, false);
    } else if (state_ == ElectionState::Canvass  ||
               state_ == ElectionState::Nominate ||
               state_ == ElectionState::CandidateBallot ||
               state_ == ElectionState::FollowerBallot) {
        // Election.java:373-380: candidate is up-to-date and we can vote
        // Election.java:376-377: proposeMaxCandidateTermId
        raiseFollowFloor_(candidateTermId);   // D24: a GRANT (candidate term + floor, one sync)
        votedTermId_ = std::max(votedTermId_, candidateTermId);   // MAJOR-R3: a GRANT
        placeVote(candidateTermId, candidateId, true);
        // Election.java:379: state(FOLLOWER_BALLOT, nowNs, "")
        // nowNs is passed in from the caller (duty-cycle clock).
        // In tests, nowNs defaults to 0; followerBallot timeout checks against
        // timeOfLastStateChangeNs_ which will be 0 in that case.
        state(ElectionState::FollowerBallot, nowNs, "voted for candidate");
    }
}

// ---------------------------------------------------------------------------
// onVote — Election.java:383-409
// ---------------------------------------------------------------------------
void Election::onVote(Term    logLeadershipTermId,
                      int64_t logPosition,
                      Term    candidateTermId,
                      NodeId  candidateId,
                      NodeId  voterId,
                      bool    vote)
{
    // Election.java:394-397: ignore unless we are in CANDIDATE_BALLOT
    if (state_ != ElectionState::CandidateBallot) return;
    if (candidateTermId != candidateTermId_)       return;
    if (candidateId     != thisMemberId_)          return;

    // Election.java:400-407: record vote on the member
    ClusterMember* follower = memberById(voterId);
    if (follower != nullptr) {
        follower->candidateTermId     = candidateTermId;
        follower->vote                = vote ? Vote::For : Vote::Against;
        follower->logLeadershipTermId = logLeadershipTermId;
        follower->logPosition         = logPosition;
    }
}

// ---------------------------------------------------------------------------
// onNewLeadershipTerm — Election.java:411-533
// Received from a leader during FOLLOWER_BALLOT, CANVASS, or CANDIDATE_BALLOT.
// Sets leaderMember, leadershipTermId, notifiedCommitPosition and drives the
// follower replay path.
// ---------------------------------------------------------------------------
void Election::onNewLeadershipTerm(int64_t nowNs,
                                   Term    logLeadershipTermIdArg,
                                   Term    nextLeadershipTermId,
                                   int64_t nextTermBaseLogPosition,
                                   int64_t /*nextLogPosition*/,
                                   Term    leadershipTermIdArg,
                                   int64_t /*termBaseLogPosition*/,
                                   int64_t logPositionArg,
                                   int64_t /*leaderRecordingId*/,
                                   int64_t /*timestamp*/,
                                   NodeId  leaderMemberIdArg,
                                   int32_t logSessionIdArg,
                                   bool    /*isStartup*/,
                                   int64_t commitPositionArg)
{
    // D24 review MAJOR-1: nothing is decided before init() (as onCanvassPosition /
    // onRequestVote) — the leader's next heartbeat repeats it.
    if (state_ == ElectionState::Init) return;
    noteLeaderHeard_(leaderMemberIdArg, leadershipTermIdArg, nowNs);   // D29: even one refused below (by floor)
    noteTermEvidence_(leaderMemberIdArg, leadershipTermIdArg);          // ISR v3 R12

    // Election.java:430: if CLOSED ignore — EXCEPT a same-term NLT from the leader
    // we already follow is a LIVENESS HEARTBEAT, not an election message.  The
    // leader's periodic Closed-state broadcast is publishNewLeadershipTermToAll()
    // (an NLT), so a Closed follower that blindly dropped it would never refresh
    // its follower-liveness clock from the leader's heartbeats, falsely time out a
    // LIVE leader after leaderHeartbeatTimeoutNs, and flap Canvass↔Closed.  That
    // flapping collapses eligibleNodeIds() to just the leader (a live survivor is
    // never credited), which stalls partition failover forever → committed records
    // become unreadable after a leader partition (dr5_leader_isolation committed-loss).
    // Refresh liveness on the heartbeat, then still ignore it for election purposes.
    if (state_ == ElectionState::Closed) {
        // ClosedLeaderStepDownRegression: a strictly-higher term from another
        // member means an election concluded without us (we were paused or
        // partitioned).  Aeron ConsensusModuleAgent.onNewLeadershipTerm: a
        // higher leadershipTermId outside an election → enterElection().  Holding
        // CLOSED here kept a resumed ex-leader a zombie of the old term forever.
        if (leadershipTermIdArg > leadershipTermId_ &&
            leaderMemberIdArg != thisMemberId_ &&
            memberById(leaderMemberIdArg) != nullptr) {
            leaveClosedForElection_(leadershipTermIdArg, nowNs,
                                    "higher-term NLT in CLOSED → re-election");
            return;
        }
        if (leaderMember_ != nullptr &&
            leaderMemberIdArg == leaderMember_->id &&
            leaderMemberIdArg != thisMemberId_ &&
            leadershipTermIdArg == leadershipTermId_) {
            followerLiveness_.onLeaderHeartbeat(nowNs);
            if (commitPositionArg > notifiedCommitPosition_) {
                notifiedCommitPosition_ = commitPositionArg;
            }
        }
        return;
    }

    // Election.java:440-450: state gate — only accept from ballot/canvass states
    bool inAcceptingState =
        (state_ == ElectionState::FollowerBallot   ||
         state_ == ElectionState::CandidateBallot  ||
         state_ == ElectionState::Canvass          ||
         state_ == ElectionState::Nominate);

    if (!inAcceptingState) {
        // Election.java:517-533: in a non-accepting (LEADER) state, a strictly-higher
        // leadershipTermId means another node won a new election while this node was
        // leading.  The Aeron step-down rule requires this node to abandon leadership
        // and re-enter election with the new term (two-leaders-window close).
        // Ref: docs/specs/…-reference.md line 146 "if LEADER_READY and higher termId →
        // new election"; Election.java:517-533.
        //
        // A1 fix (audit 2026-09-11): previously, a higher-term NLT/commit in a LEADER
        // state was silently dropped.  Now we adopt the higher term and transition back
        // to CANVASS so the node participates in the new election rather than holding a
        // stale leadership.
        if (leadershipTermIdArg > leadershipTermId_ && memberById(leaderMemberIdArg) != nullptr) {   // review LOW-5
            // Adopt the new term durably (mirrors Aeron proposeMaxCandidateTermId);
            // D24: a leader announced it — the floor rises with it.
            raiseFollowFloor_(leadershipTermIdArg);
            leadershipTermId_ = leadershipTermIdArg;
            // Step down: return to CANVASS to re-participate in the new election.
            state(ElectionState::Canvass, nowNs, "A1: higher-term NLT in LEADER state → step down");
            return;
        }
        // Matching term: update notifiedCommitPosition from the commit field.
        if (leadershipTermIdArg == leadershipTermId_) {
            if (commitPositionArg > notifiedCommitPosition_) {
                notifiedCommitPosition_ = commitPositionArg;
            }
            // An NLT heartbeat for our current term from the current leader
            // refreshes the follower liveness clock (leader still alive).
            if (leaderMemberIdArg == thisMemberId_) {
                // Defensive: never arm the timeout for ourselves.
            } else if (leaderMember_ != nullptr &&
                       leaderMember_->id == leaderMemberIdArg &&
                       leaderMember_->id != thisMemberId_) {
                followerLiveness_.onLeaderHeartbeat(nowNs);
            }
        }
        return;
    }

    // Election.java:452-454: stale NLT — leadershipTermId < our current leadershipTermId
    if (leadershipTermIdArg < leadershipTermId_) {
        return;
    }

    // B2 fix: Aeron Election.java:433-437 — gate on candidateTermId.
    // A follower only follows the leader of the term it participated in.
    // If nextLeadershipTermId > candidateTermId_, the node was partitioned and
    // missed a ballot round; adopt the new term via proposeMaxCandidateTermId
    // (same as Aeron's logic at Election.java:443-448).
    // If nextLeadershipTermId < candidateTermId_, the NLT is for a stale ballot
    // this node already advanced past — ignore.
    if (nextLeadershipTermId > candidateTermId_) {
        // Adopt the higher term — node missed its ballot round.  D24: a leader
        // announced it — the follow floor rises with it.
        raiseFollowFloor_(nextLeadershipTermId);
    } else if (nextLeadershipTermId < candidateTermId_) {
        // D24 (DR-4 review MAJOR-1 / MEDIUM-3): a candidate term above the
        // leader's is a stale ballot only if this node GRANTED, LED, FOLLOWED or
        // adopted from a leader a newer term (the durable follow floor) — a
        // newer term's leader may exist, elected with this node's help, so the
        // old one is a zombie.  A candidate term raised only by our own failed
        // candidacies (or by denials) helped no leader: following the live
        // leader of the older term is safe, and refusing it strands this node
        // (the leader's stickiness denies our RequestVotes) until it dies.
        // Review MAJOR-2: only while CANVASSING / NOMINATING — a candidate in
        // its ballot may hold a winning quorum (a peer granted it: that peer's
        // floor is already above this term); abandoning it would strand that peer.
        const bool canvassing = state_ == ElectionState::Canvass || state_ == ElectionState::Nominate;
        // (A member that GRANTED a newer term to a candidacy that then failed
        // stays out until the leader changes — closing that needs PreVote,
        // backlog D29; a step-down notice reopened partial-partition ping-pong.)
        if (leadershipTermIdArg < followFloorTermId_) {
            // D29: tell that leader — a term at or above our floor may already
            // have been decided with our help, so it must step down to a newer
            // one (etcd's lower-term answer, narrowed to the floor).  Safe with
            // PreVote: a cut-off candidate can no longer ratchet our floor.
            if (canvassing && leaderMemberIdArg != thisMemberId_ && memberById(leaderMemberIdArg) != nullptr)
                publisher_.termFloorNotice(leaderMemberIdArg, thisMemberId_, followFloorTermId_, leadershipTermIdArg);
            return;
        }
        if (!canvassing) return;
    }
    // nextLeadershipTermId == candidateTermId_: this node participated; continue.
    // (Review LOW-2, defence in depth: never below the floor, whatever the branch.)
    if (leadershipTermIdArg < followFloorTermId_) return;

    // Election.java:456-458: locate the sending leader member
    ClusterMember* leaderM = memberById(leaderMemberIdArg);
    if (leaderM == nullptr) return;

    // B2 fix: leader log-freshness check — Aeron Election.java:460-470.
    // Reject the NLT if the leader's log is strictly behind this node's log.
    // compareLog > 0 means our log is more recent than the leader's → reject.
    // This prevents following a leader whose log is shorter than our own
    // (Leader-Completeness violation). Audit finding B2 / ref §3.1 row NLT.
    if (ClusterMember::compareLog(logLeadershipTermId_, appendPosition_,
                                  logLeadershipTermIdArg, logPositionArg) > 0) {
        // Leader's log is behind ours. Do not follow.
        return;
    }

    // Election.java:460-470: set leader member + leadership term
    raiseFollowFloor_(leadershipTermIdArg);   // D24: follows it (durable before any ack; review LOW-1: first)
    leaderMember_     = leaderM;
    leadershipTermId_ = leadershipTermIdArg;
    logSessionId_     = logSessionIdArg;

    // Election.java:472-476: update notifiedCommitPosition
    if (commitPositionArg > notifiedCommitPosition_) {
        notifiedCommitPosition_ = commitPositionArg;
    }

    // Election.java:478-490: decide follower path based on appendPosition vs termBaseLogPosition
    // Phase 2 pass-through: always go FOLLOWER_REPLAY (archive replication skipped).
    // In real Aeron: if appendPosition < termBaseLogPosition → FollowerLogReplication
    //                else → FollowerReplay.
    // We always go to FollowerReplay (which immediately passes through to FollowerLogInit).
    state(ElectionState::FollowerReplay, nowNs, "onNewLeadershipTerm follower path");

    (void)nextTermBaseLogPosition;
}

// ---------------------------------------------------------------------------
// onAppendPosition — Election.java:535-554
// Called when a follower sends its AppendPosition to the leader.
// Updates that member's logPosition so quorumPosition advances.
// ---------------------------------------------------------------------------
void Election::onAppendPosition(int64_t nowNs,
                                Term    leadershipTermIdArg,
                                int64_t logPositionArg,
                                NodeId  followerMemberIdArg,
                                uint8_t /*flags*/)
{
    noteTermEvidence_(followerMemberIdArg, leadershipTermIdArg);   // ISR v3 R12
    // Election.java:541-543: ignore if wrong leadershipTermId
    if (leadershipTermIdArg != leadershipTermId_) {
        if (electLog()) {
            std::fprintf(stderr,
                "[APPOS node=%d] DROP from follower=%d: msgTerm=%lld != myTerm=%lld "
                "(state=%d)\n",
                (int)thisMemberId_, (int)followerMemberIdArg,
                (long long)leadershipTermIdArg, (long long)leadershipTermId_,
                (int)state_);
        }
        return;
    }
    if (electLog()) {
        std::fprintf(stderr,
            "[APPOS node=%d] recv from follower=%d pos=%lld term=%lld (state=%d)\n",
            (int)thisMemberId_, (int)followerMemberIdArg, (long long)logPositionArg,
            (long long)leadershipTermIdArg, (int)state_);
    }

    // Election.java:545-549: update the follower's logPosition
    ClusterMember* follower = memberById(followerMemberIdArg);
    if (follower != nullptr) {
        follower->logPosition              = logPositionArg;
        follower->leadershipTermId         = leadershipTermIdArg;
        follower->timeOfLastAppendPositionNs = nowNs;
    }
}

// ---------------------------------------------------------------------------
// onCommitPosition — Election.java:557-587
// Called when a follower receives a CommitPosition from the leader.
// Updates notifiedCommitPosition_ and extends any pending replication deadline.
// ---------------------------------------------------------------------------
void Election::onCommitPosition(int64_t nowNs,
                                Term    leadershipTermIdArg,
                                int64_t logPositionArg,
                                NodeId  leaderMemberIdArg)
{
    if (state_ == ElectionState::Init) return;   // D24 review MAJOR-1 (see onNewLeadershipTerm)
    noteLeaderHeard_(leaderMemberIdArg, leadershipTermIdArg, nowNs);   // D29
    noteTermEvidence_(leaderMemberIdArg, leadershipTermIdArg);          // ISR v3 R12

    // A1 fix (audit 2026-09-11): a CommitPosition with a strictly-higher term while
    // this node is in a LEADER state signals a new election has concluded elsewhere.
    // Step down and re-enter CANVASS to participate, per Aeron Election.java:517-533
    // and ref docs/specs/…-reference.md line 146.
    if (leadershipTermIdArg > leadershipTermId_) {
        // ClosedLeaderStepDownRegression: Aeron ConsensusModuleAgent.onCommitPosition
        // enters an election on a higher term outside an election as well.
        if (state_ == ElectionState::Closed && clusterMembers_.size() > 1) {
            leaveClosedForElection_(leadershipTermIdArg, nowNs,
                                    "higher-term CommitPosition in CLOSED → re-election");
            return;
        }
        const bool inLeaderState =
            (state_ == ElectionState::LeaderLogReplication ||
             state_ == ElectionState::LeaderReplay         ||
             state_ == ElectionState::LeaderInit           ||
             state_ == ElectionState::LeaderReady);
        if (inLeaderState) {
            raiseFollowFloor_(leadershipTermIdArg);   // D24: a leader announced it
            leadershipTermId_ = leadershipTermIdArg;
            state(ElectionState::Canvass, nowNs, "A1: higher-term CommitPosition in LEADER state → step down");
            return;
        }
    }

    // Election.java:563-565: ignore if wrong term
    if (leadershipTermIdArg != leadershipTermId_) return;

    // Leader-liveness refresh: a CommitPosition for our current term IS a leader
    // heartbeat.  Refresh the follower liveness clock so a live leader is never
    // spuriously timed out.  Only refresh for a follower that is actually
    // following someone else (the leader must not arm its own timeout).
    if (leaderMember_ != nullptr && leaderMember_->id != thisMemberId_) {
        followerLiveness_.onLeaderHeartbeat(nowNs);
    }

    // Election.java:567: update notifiedCommitPosition
    if (logPositionArg > notifiedCommitPosition_) {
        notifiedCommitPosition_ = logPositionArg;
    }
}

// ---------------------------------------------------------------------------
// Controlled shutdown — leadership hand-off (Raft leadership transfer; Aeron's
// graceful leader close: followers elect at once, skipping the closed leader).
// ---------------------------------------------------------------------------
bool Election::resign(int64_t nowNs)
{
    if (state_ != ElectionState::Closed || leaderMember_ == nullptr ||
        leaderMember_->id != thisMemberId_ || clusterMembers_.size() <= 1)
        return false;
    candidacy_ = Resigned{leadershipTermId_};
    for (const ClusterMember& m : clusterMembers_)
        if (m.id != thisMemberId_) publisher_.standDown(m.id, leadershipTermId_, thisMemberId_);
    followerLiveness_.reset();
    // Canvass entry resets the members and drops leaderMember_ (no heartbeat,
    // no leader reported) — the node now only votes and follows.
    state(ElectionState::Canvass, nowNs, "controlled shutdown: leader resigned");
    timeOfLastUpdateNs_ = nowNs;   // StandDown just went out: re-send on the interval
    return true;
}

bool Election::standDown(int64_t nowNs, int64_t holdNs)
{
    if (state_ != ElectionState::Closed || leaderMember_ == nullptr ||
        leaderMember_->id != thisMemberId_ || clusterMembers_.size() <= 1)
        return false;
    // A withdrawn node never takes a (bounded) hold that would mask it: it
    // resigns instead (controlled shutdown owns that path).
    if (std::holds_alternative<Resigned>(candidacy_)) return false;
    if (std::holds_alternative<AwaitingStorage>(candidacy_))   // the storage hold stays; the stand-down is remembered
        candidacy_ = AwaitingStorage{nowNs + std::max<int64_t>(holdNs, 0), leadershipTermId_};
    else
        candidacy_ = StoodDown{nowNs + std::max<int64_t>(holdNs, 0), leadershipTermId_};
    ++standDowns_;
    for (const ClusterMember& m : clusterMembers_)
        if (m.id != thisMemberId_) publisher_.standDown(m.id, leadershipTermId_, thisMemberId_);
    followerLiveness_.reset();
    state(ElectionState::Canvass, nowNs, "leader stood down (a healthier member should lead)");
    timeOfLastUpdateNs_ = nowNs;
    return true;
}

void Election::withdrawCandidacy() noexcept
{
    // Any state → Resigned; a pending stand-down's StandDown keeps being re-sent.
    if (const auto* sd = std::get_if<StoodDown>(&candidacy_)) { candidacy_ = Resigned{sd->term}; return; }
    if (const auto* as = std::get_if<AwaitingStorage>(&candidacy_)) { candidacy_ = Resigned{as->standDownTerm}; return; }
    if (!std::holds_alternative<Resigned>(candidacy_)) candidacy_ = Resigned{-1};
}

void Election::setStorageHold(bool hold, int64_t nowNs) noexcept
{
    if (hold) {
        if (std::holds_alternative<Candidate>(candidacy_)) candidacy_ = AwaitingStorage{};
        else if (const auto* sd = std::get_if<StoodDown>(&candidacy_)) candidacy_ = AwaitingStorage{sd->untilNs, sd->term};
        return;   // Resigned / AwaitingStorage: no-op
    }
    if (const auto* as = std::get_if<AwaitingStorage>(&candidacy_)) {
        if (as->standDownUntilNs > nowNs) candidacy_ = StoodDown{as->standDownUntilNs, as->standDownTerm};
        else candidacy_ = Candidate{};
    }
}

bool Election::candidacyBlocks_(int64_t nowNs, int workCount)
{
    auto resend = [&](Term term) {
        if (workCount > 0 && term >= 0)
            for (const ClusterMember& m : clusterMembers_)
                if (m.id != thisMemberId_) publisher_.standDown(m.id, term, thisMemberId_);
    };
    if (std::holds_alternative<AwaitingStorage>(candidacy_)) return true;   // ISR v3: held, no StandDown resend
    if (const auto* r = std::get_if<Resigned>(&candidacy_)) {
        resend(r->term);
        return true;
    }
    if (const auto* sd = std::get_if<StoodDown>(&candidacy_)) {
        if (nowNs < sd->untilNs) {
            resend(sd->term);
            return true;
        }
        candidacy_ = Candidate{};   // the hold ended: a candidate again
    }
    return false;
}

void Election::onStandDown(int64_t nowNs, Term leadershipTermIdArg, NodeId leaderMemberIdArg)
{
    if (state_ != ElectionState::Closed) return;
    if (leaderMember_ == nullptr || leaderMember_->id != leaderMemberIdArg ||
        leaderMemberIdArg == thisMemberId_ || leadershipTermIdArg != leadershipTermId_)
        return;   // not from the leader we follow, for our term: stale / foreign
    gracefulClosedLeaderId_ = leaderMemberIdArg;   // canvass completes without it
    skipFromStandDown_      = true;
    if (leaderHeardId_ == leaderMemberIdArg) leaderHeardId_ = -1;   // D29 review MAJOR-1: the lease ends with it
    followerLiveness_.reset();
    state(ElectionState::Canvass, nowNs, "leader stood down (controlled shutdown) → election now");
}

// ---------------------------------------------------------------------------
// CLOSED leader heartbeat — post-election NLT broadcast.
//
// After the election reaches CLOSED, the leader continues broadcasting
// NewLeadershipTerm on the heartbeat interval so late-joining nodes that
// start their own Canvass phase can receive an NLT, call
// onNewLeadershipTerm(), and converge to the existing leader without waiting
// for a full re-election.  Followers in CLOSED are silent (no heartbeat).
//
// Safety: this does NOT re-open the election.  The NLT carries the SAME
// leadershipTermId_; late-joiners accept it because nextLeadershipTermId >
// their initial candidateTermId_(0) → they adopt the existing leader via the
// B2 path in onNewLeadershipTerm().  Single-leader invariant is unaffected.
// ---------------------------------------------------------------------------
int Election::closedLeaderHeartbeat(int64_t nowNs)
{
    // Only the leader broadcasts; followers in Closed are silent EXCEPT they run
    // leader-liveness detection so a dead leader triggers a fresh election.
    if (leaderMember_ == nullptr || leaderMember_->id != thisMemberId_) {
        // Follower path: detect a dead leader.  In Aeron this lives in the
        // ConsensusModuleAgent (timeOfLastLeaderUpdateNs + leaderHeartbeatTimeoutNs);
        // it is folded into the Election here because the agent is not ported.
        //
        // Only multi-node followers that have actually observed a leader arm the
        // timeout (sentinel guard).  When the current leader has been silent for
        // longer than leaderHeartbeatTimeoutNs_, re-enter CANVASS so the surviving
        // nodes elect a new leader.  This does NOT weaken safety: the re-election
        // runs the full canvass/ballot/quorum path with a strictly-higher term.
        const bool haveLeader = (leaderMember_ != nullptr);
        const bool isFollower  = haveLeader && leaderMember_->id != thisMemberId_;
        const bool multiNode   = clusterMembers_.size() > 1;

        if (!(multiNode && isFollower)) return 0;

        // The FollowerLiveness state machine owns both steady-state duties and
        // their clocks (see FollowerLiveness.h): (1) republish our AppendPosition
        // to the leader on the status interval so the leader's per-member liveness
        // (eligibleNodeIds → partition failover) never goes stale, and (2) detect a
        // dead leader after leaderHeartbeatTimeoutNs_.
        const FollowerLiveness::Action act = followerLiveness_.decide(nowNs);

        if (act.triggerReElection) {
            // (Observability: the leadership-change is logged by the caller —
            //  NodeConsensusAgent::updateObservedLeader — when leaderMemberId()
            //  transitions away from the dead node.  rune_consensus has no spdlog.)
            // Drop the stale leader so leaderMemberId() no longer reports the dead
            // node while we re-elect, and disarm until we accept a new leader.
            leaderMember_ = nullptr;
            followerLiveness_.reset();
            state(ElectionState::Canvass, nowNs, "leader heartbeat timeout → re-election");
            return 1;
        }

        if (act.sendAppendPosition) {
            publisher_.appendPosition(leaderMember_->id, leadershipTermId_,
                                      logPosition_, thisMemberId_, /*flags=*/0);
            return 1;
        }
        return 0;
    }

    // Single-node clusters have no peers to broadcast to — no work.
    const bool hasPeers = (clusterMembers_.size() > 1);
    if (!hasPeers) {
        return 0;
    }

    if (!hasUpdateIntervalExpired(nowNs, electionStatusIntervalNs_)) {
        return 0;
    }

    // ClosedLeaderStepDownRegression: Aeron ConsensusModuleAgent (leader slow
    // tick): !ClusterMember.hasActiveQuorum(...) → "inactive follower quorum" →
    // enterElection().  The leader counts itself (Aeron: member.isLeader()).  A
    // leader paused/partitioned past the heartbeat timeout resumes here, sees no
    // follower reported in the window, and rejoins the cluster's election instead
    // of lingering as a zombie leader of the old term.
    if (!hasActiveFollowerQuorum_(nowNs)) {
        leaveClosedForElection_(leadershipTermId_, nowNs,
                                "inactive follower quorum → re-election");
        return 1;
    }

    timeOfLastUpdateNs_ = nowNs;
    publishNewLeadershipTermToAll(nowNs);
    return 1;
}

// ClosedLeaderStepDownRegression: the leader itself is always active
// (Aeron ClusterMember.hasActiveQuorum: member.isLeader() || isActive).  One
// implementation: ClusterMember::hasActiveQuorum with this node as the leader
// (DR-10 review LOW-3).  Callers invoke it only while this node leads.
bool Election::hasActiveFollowerQuorum_(int64_t nowNs) const noexcept
{
    return ClusterMember::hasActiveQuorum(clusterMembers_, nowNs, leaderHeartbeatTimeoutNs_,
                                          thisMemberId_);
}

// D29b — leader-state CheckQuorum (documented Raft tightening, like the PreVote
// lease it protects).  Aeron's leader states have no quorum timeout: its
// CheckQuorum runs in the agent's slow tick only while no election is in
// progress.  That is harmless in Aeron, but here every NewLeadershipTerm a
// would-be leader broadcasts stamps the D29 lease on its peers — one that won
// a ballot and then lost its INBOUND links (no AppendPosition ever arrives)
// would keep the others denying each other's pre-votes for as long as the link
// stays one-way.  So: in LeaderLogReplication / LeaderReady, after a heartbeat
// timeout without an active follower quorum, stand down (StandDown drops the
// peers' lease at once) and canvass.  Time-independent: a leader-elect that
// hears its followers never trips it, however slow the election.
// CONTRACT (review MEDIUM-3): every follower election state must publish
// AppendPosition to the leader at least every status interval — as Aeron's
// updateFollowerLogReplication does — or this trips on a live quorum.  Today
// the follower states are pass-throughs (PHASE-2-ARCHIVE-DEFERRED) and a
// follower reaches CLOSED, where FollowerLiveness sends it, within one pass;
// wiring real archive catch-up must keep that cadence.
bool Election::leaderStatesTimedOut_(int64_t nowNs) const noexcept
{
    return clusterMembers_.size() > 1 &&
           nowNs - leaderSinceNs_ >= leaderHeartbeatTimeoutNs_ &&
           !hasActiveFollowerQuorum_(nowNs);
}

void Election::standDownFromLeaderStates_(int64_t nowNs)
{
    ++leaderStateStandDowns_;
    for (const ClusterMember& m : clusterMembers_)
        if (m.id != thisMemberId_) publisher_.standDown(m.id, leadershipTermId_, thisMemberId_);
    leaderMember_ = nullptr;
    state(ElectionState::Canvass, nowNs, "leader state without an active follower quorum → stand down");
}

// ---------------------------------------------------------------------------
// D29 PreVote (Raft thesis §9.6) + TermFloorNotice.
// ---------------------------------------------------------------------------
void Election::noteLeaderHeard_(NodeId leaderId, Term term, int64_t nowNs) noexcept
{
    if (leaderId == thisMemberId_ || memberById(leaderId) == nullptr) return;
    // Review MAJOR-2: a leader of a term below the one we follow / adopted is
    // provably superseded — a zombie stuck in a leader state must never hold
    // our lease (the survivors could then never elect after the real leader dies).
    // Re-review M3: also below our durable floor — after a restart the Election
    // starts at leadershipTermId 0, but the floor survives.
    if (term < leadershipTermId_ || term < followFloorTermId_) return;
    leaderHeardId_ = leaderId;
    leaderHeardNs_ = nowNs;
}

bool Election::leaderLeaseLive_(int64_t nowNs, NodeId candidateId) const noexcept
{
    const bool selfLeads = leaderMember_ != nullptr && leaderMember_->id == thisMemberId_;
    if (selfLeads) {
        // A working leadership denies: CLOSED with an active follower quorum, or
        // still establishing (leader states).
        return state_ == ElectionState::Closed ? hasActiveFollowerQuorum_(nowNs) : true;
    }
    // Heard a live leader within the heartbeat timeout — unless the candidate IS
    // that leader, or the leader we follow (it stepped down: TermFloorNotice /
    // lost its quorum; review MEDIUM-3).
    if (leaderMember_ != nullptr && leaderMember_->id == candidateId) return false;
    return leaderHeardId_ >= 0 && leaderHeardId_ != candidateId &&
           nowNs - leaderHeardNs_ < leaderHeartbeatTimeoutNs_;
}

void Election::startPreBallot_(int64_t nowNs)
{
    preBallot_ = PreBallot{};
    preBallot_.active     = true;
    preBallot_.term       = candidateTermId_ + 1;
    preBallot_.deadlineNs = nowNs + electionTimeoutNs_;
    if (mayVoteFor(thisMemberId_)) markPreVote_(thisMemberId_);   // the self-vote, gated like the real one
    timeOfLastUpdateNs_ = nowNs;
    ++preBallotsStarted_;
    sendPreVoteRequests_();
}

void Election::sendPreVoteRequests_()
{
    for (std::size_t i = 0; i < clusterMembers_.size(); ++i) {
        const NodeId id = clusterMembers_[i].id;
        if (id == thisMemberId_ || (i < 32 && (preBallot_.grantedMask & (1u << i)))) continue;
        publishCandidacyState_(id);   // review MEDIUM-2: our knowledge first (as the real ballot does)
        publisher_.preVoteRequest(id, preBallot_.term, logLeadershipTermId_, appendPosition_, thisMemberId_);
    }
}

void Election::markPreVote_(NodeId voterId) noexcept
{
    for (std::size_t i = 0; i < clusterMembers_.size() && i < 32; ++i)
        if (clusterMembers_[i].id == voterId) { preBallot_.grantedMask |= (1u << i); return; }
}

bool Election::preBallotWon_() const noexcept
{
    const int granted = std::popcount(preBallot_.grantedMask);
    return granted >= ClusterMember::quorumThreshold(static_cast<int>(clusterMembers_.size()));
}

void Election::onPreVoteRequest(int64_t nowNs, Term term, Term logLeadershipTermId, int64_t logPosition,
                                NodeId candidateId)
{
    if (state_ == ElectionState::Init) return;
    if (candidateId == thisMemberId_ || memberById(candidateId) == nullptr) return;
    if (abstains_()) return;   // ISR v3: see setAbstainGate
    // Non-binding: grant iff we WOULD vote for it at `term` and no live leader
    // holds us — and change nothing (no candidate term, no floor, no state).
    const bool grant = term > candidateTermId_ && term > followFloorTermId_ &&
                       ClusterMember::compareLog(logLeadershipTermId_, appendPosition_,
                                                 logLeadershipTermId, logPosition) <= 0 &&
                       mayVoteFor(candidateId) && !leaderLeaseLive_(nowNs, candidateId);
    if (grant) ++preVotesGranted_; else ++preVotesDenied_;
    // D29b (review M4, measured): a NOMINATE node that grants a peer's pre-vote
    // abandons its own unwon pre-ballot and re-arms its nomination one fresh
    // random back-off later.  Non-binding (no term, no floor), so only our own
    // candidacy is delayed; it stops simultaneous pre-ballot winners from all
    // standing at the same term and splitting the real ballot (5-node hand-off,
    // 200 seeds: lost rounds 13→1 at 0 ms jitter, 36→12 at 5 ms, worst 751→257 ms).
    // Review LOW-1: once per peer term — a re-asked pre-vote (status interval)
    // does not push our candidacy out again.
    if (grant && state_ == ElectionState::Nominate && term != deferredForTerm_) {
        deferredForTerm_ = term;
        if (preBallot_.active && !preBallotWon_()) preBallot_ = PreBallot{};
        if (!preBallot_.active) {
            std::uniform_real_distribution<double> dist(0.0, 1.0);
            nominationDeadlineNs_ = std::max(nominationDeadlineNs_,
                nowNs + (electionTimeoutNs_ >> 1) +
                static_cast<int64_t>(dist(rng_) * static_cast<double>(electionTimeoutNs_ >> 1)));
        }
    }
    publisher_.preVoteResponse(candidateId, term, thisMemberId_, grant,
                               std::max(candidateTermId_, followFloorTermId_));
}

void Election::onPreVoteResponse(Term term, NodeId voterId, bool granted, Term voterTermId, int64_t nowNs)
{
    if (state_ != ElectionState::Nominate || !preBallot_.active || term != preBallot_.term) return;
    if (voterId == thisMemberId_ || memberById(voterId) == nullptr) return;
    if (granted) { markPreVote_(voterId); return; }
    // Re-review M1: the real ballot fails on ANY Against (Aeron isQuorumLeader) —
    // a pre-ballot that met an explicit denial for our term (lease, log, gate)
    // would only raise durable terms for a certain loss: it is lost too (the term
    // catch-up below re-asks instead).
    if (voterTermId < preBallot_.term) { preBallot_.denied = true; return; }
    // Review CRITICAL-1 (etcd: a rejected pre-vote carries the voter's term): a
    // live member already holds a term >= ours durably — adopt it as our
    // CANDIDATE term (never the floor: we took part in nothing) and re-ask above
    // it.  Aeron's term catch-up ran through the stale node's real RequestVote,
    // which a lost pre-ballot never sends; without this, a term/eligibility
    // inversion (the eligible node behind in term) wedges the election forever.
    if (voterTermId >= preBallot_.term) {
        candidateTermId_ = std::max(candidateTermId_,
                                    nodeStateFile_.proposeMaxCandidateTermId(voterTermId, logPosition_, 0));
        ++preBallotTermCatchUps_;
        const uint32_t self = preBallot_.grantedMask & selfBit_();
        preBallot_.term        = candidateTermId_ + 1;
        preBallot_.grantedMask = self;
        preBallot_.denied      = false;
        if (nowNs != INT64_MIN) preBallot_.deadlineNs = nowNs + electionTimeoutNs_;   // re-review M2: a fresh window
        sendPreVoteRequests_();
    }
}

uint32_t Election::selfBit_() const noexcept
{
    for (std::size_t i = 0; i < clusterMembers_.size() && i < 32; ++i)
        if (clusterMembers_[i].id == thisMemberId_) return 1u << i;
    return 0;
}

void Election::onTermFloorNotice(int64_t nowNs, NodeId fromMember, Term floorTermId, Term refusedTermId)
{
    // A member refused OUR NewLeadershipTerm because it granted, led, followed
    // or adopted a newer term (its durable floor).  Its own candidacies never
    // raise a floor, and with PreVote a cut-off candidate cannot obtain its
    // binding vote while it hears us — so the floor is real: step down to an
    // election above it (our followers grant our own re-candidacy).
    if (state_ == ElectionState::Init) return;
    if (leaderMember_ == nullptr || leaderMember_->id != thisMemberId_) return;   // only the leader
    if (fromMember == thisMemberId_ || memberById(fromMember) == nullptr) return;
    if (refusedTermId != leadershipTermId_ || floorTermId <= leadershipTermId_) return;
    ++termFloorStepDowns_;
    // Review MEDIUM-4: stand ABOVE the floor (candidate term) without adopting it
    // as our own floor — no leader of `floorTermId` need exist (a failed grant).
    candidateTermId_ = std::max(candidateTermId_,
                                nodeStateFile_.proposeMaxCandidateTermId(floorTermId, logPosition_, 0));
    // Review MEDIUM-3 / MAJOR-1: our followers canvass at once and drop our lease.
    for (const ClusterMember& m : clusterMembers_)
        if (m.id != thisMemberId_) publisher_.standDown(m.id, leadershipTermId_, thisMemberId_);
    if (state_ == ElectionState::Closed) {
        leaveClosedForElection_(leadershipTermId_, nowNs, "a member's follow floor is above our term → re-election");
        return;
    }
    leaderMember_ = nullptr;
    state(ElectionState::Canvass, nowNs, "a member's follow floor is above our term → step down");
}

void Election::raiseFollowFloor_(Term term)
{
    if (term <= followFloorTermId_ && term <= candidateTermId_) return;
    candidateTermId_   = std::max(candidateTermId_,
                                  nodeStateFile_.proposeMaxFollowFloorTermId(term, logPosition_, /*timestampMs=*/0));
    followFloorTermId_ = std::max(followFloorTermId_, term);
}

// ClosedLeaderStepDownRegression: leave the CLOSED steady state for a fresh
// election (Aeron enterElection()).  Adopts `term` durably when it is newer,
// forgets the leader (leaderMemberId() → -1 while re-electing) and disarms the
// follower liveness clock until a new leader is accepted.
void Election::leaveClosedForElection_(Term term, int64_t nowNs, const char* reason)
{
    if (term > leadershipTermId_) {
        raiseFollowFloor_(term);   // D24: a leader announced it (NLT / CommitPosition)
        leadershipTermId_ = term;
    }
    leaderMember_ = nullptr;
    followerLiveness_.reset();
    state(ElectionState::Canvass, nowNs, reason);
}

// ---------------------------------------------------------------------------
// Private: state transition — Election.java:1385-1438
// ---------------------------------------------------------------------------
void Election::state(ElectionState next, int64_t nowNs, const char* /*reason*/)
{
    if (next == state_) return;
    if (state_ == ElectionState::Nominate) preBallot_ = PreBallot{};   // D29
    if (electLog()) {
        std::fprintf(stderr,
            "[ELECT node=%d] state %d -> %d term=%lld leader=%d nowNs=%lld\n",
            (int)thisMemberId_, (int)state_, (int)next,
            (long long)leadershipTermId_,
            (int)(leaderMember_ ? leaderMember_->id : -1), (long long)nowNs);
    }

    // Election.java:1389-1391: leaving CANVASS clears isExtendedCanvass
    if (state_ == ElectionState::Canvass) {
        isExtendedCanvass_ = false;
    }

    // Election.java:1394-1417: entry actions
    switch (next) {
        case ElectionState::Canvass:
            // Election.java:1397-1399
            resetMembers();
            break;
        case ElectionState::CandidateBallot:
            // Election.java:1401-1402: role = CANDIDATE (CMA wired in full impl)
            break;
        case ElectionState::LeaderLogReplication:
            // Election.java:1404-1407: role = LEADER; set thisMember logPosition
            // Phase 2: no addLogPublication (no Aeron log publication yet).
            isLeaderStartup_ = isNodeStartup_; // capture startup flag for NLT isStartup field
            {
                ClusterMember* thisM = memberById(thisMemberId_);
                if (thisM != nullptr) {
                    thisM->logPosition              = appendPosition_;
                    thisM->leadershipTermId         = leadershipTermId_;
                    thisM->timeOfLastAppendPositionNs = nowNs;
                }
            }
            // Failover-liveness grace from the FIRST leader state (see
            // eligibleNodeIds): this node reports itself leader — and its
            // controller duties start — here, not at electionComplete.  A
            // separate clock: the members' append-position clocks (the
            // replication quorum, Election.java:820) stay exactly Aeron's.
            leaderSinceNs_ = nowNs;
            break;
        case ElectionState::FollowerLogReplication:
        case ElectionState::FollowerReplay:
            // Election.java:1410-1412: role = FOLLOWER
            break;
        default:
            break;
    }

    state_ = next;

    // Election.java:1433: electionStateCounter.setRelease(newState.code())
    stateCode_ = code(next);

    // Election.java:1434-1436: reset timers
    timeOfLastStateChangeNs_ = nowNs;
    timeOfLastUpdateNs_      = initialTimeOfLastUpdateNs_;
}

// ---------------------------------------------------------------------------
// Private: electionComplete — Election.java:1200+ (called from leaderReady / followerReady)
// ---------------------------------------------------------------------------
void Election::electionComplete(int64_t nowNs)
{
    // Aeron Election.java:1200+, electionComplete: leader appends term in leaderInit,
    // but follower must persist the accepted leadership term here for Phase-3 recovery.
    // See: Election.java:1200 (electionComplete), tryJoinLogAsFollower comment at 1116.
    if (!termLog_.getTermEntry(leadershipTermId_)) {
        // Follower path: append the term if not already present (leader appended in leaderInit).
        // Use phase-2 sentinels: recordingId=-1, termBaseLogPosition=0 if not captured.
        termLog_.appendTerm(
            /*recordingId=*/-1,
            leadershipTermId_,
            /*termBaseLogPosition=*/0,
            /*timestamp=*/nowNs);
    }

    // Commit the term's log position (both paths).
    termLog_.commitLogPosition(leadershipTermId_, logPosition_);

    // Arm the follower leader-liveness timeout: a follower that just accepted a
    // leader must start expecting heartbeats from it.  The leader itself does
    // NOT arm the timeout (it never times itself out).  Sentinel INT64_MIN keeps
    // a leader (or a not-yet-following node) from being force-re-canvassed.
    if (leaderMember_ != nullptr && leaderMember_->id != thisMemberId_) {
        followerLiveness_.onBecomeFollower(nowNs);  // arm liveness + schedule 1st send
    } else {
        followerLiveness_.reset();                  // leader / not-yet-following: disarm
    }

    // BECOME-LEADER liveness grace (Aeron ConsensusModuleAgent.becomeLeader):
    // when THIS node becomes leader, seed every OTHER member's append-position
    // clock to `nowNs`.  A freshly-elected leader has never received an
    // AppendPosition from its peers in this term, so their clocks sit at the
    // INT64_MIN sentinel → isActive()==false → eligibleNodeIds() reports them
    // dead.  The DynamicBalancer then fails partitions over to the new leader
    // with the still-alive prior leader wrongly excluded — the acks=all
    // leadership-churn / throughput-dip bug.  Seeding the clocks gives every
    // peer one full timeout window to report before it can be judged dead.
    // Time-INDEPENDENT: correct for ANY window — a live peer always reports
    // within the window; a genuinely dead one never does, so real failover
    // still fires.  Mirrors Aeron seeding member liveness at leader handover.
    if (leaderMember_ != nullptr && leaderMember_->id == thisMemberId_) {
        for (ClusterMember& m : clusterMembers_) {
            if (m.id != thisMemberId_) m.timeOfLastAppendPositionNs = nowNs;
        }
    }

    // The hand-off election is over.  Aeron builds a new Election per election
    // and passes the graceful-close leader id to that ONE election only; this
    // Election is long-lived, so drop it here — later elections must hear from
    // every member again (DR-10 review LOW-2).
    skipFromStandDown_      = false;
    gracefulClosedLeaderId_ = -1;   // NULL_VALUE
    state(ElectionState::Closed, nowNs, "electionComplete");
}

// ---------------------------------------------------------------------------
// Private: publishNewLeadershipTermToAll — leader heartbeat helper
// Broadcasts NLT + CommitPosition to every peer on the heartbeat interval.
// Election.java:809-825 (leaderLogReplication), 877-894 (leaderReady).
// ---------------------------------------------------------------------------
void Election::publishNewLeadershipTermToAll(int64_t nowNs)
{
    for (const auto& member : clusterMembers_) {
        if (member.id == thisMemberId_) continue;
        // ConsensusPublisher.java:172-232  newLeadershipTerm
        publisher_.newLeadershipTerm(
            member.id,
            /*logLeadershipTermId=*/logLeadershipTermId_,
            /*nextLeadershipTermId=*/leadershipTermId_,
            /*nextTermBaseLogPosition=*/logPosition_,
            /*nextLogPosition=*/appendPosition_,
            /*leadershipTermId=*/leadershipTermId_,
            /*termBaseLogPosition=*/logPosition_,
            /*logPosition=*/logPosition_,
            /*leaderRecordingId=*/0,
            /*timestamp=*/nowNs,
            /*leaderMemberId=*/thisMemberId_,
            /*logSessionId=*/logSessionId_,
            /*isStartup=*/isLeaderStartup_,
            /*commitPosition=*/logPosition_);

        // ConsensusPublisher.java:273-306  commitPosition
        publisher_.commitPosition(
            member.id,
            leadershipTermId_,
            logPosition_,
            thisMemberId_);
    }
}

// ---------------------------------------------------------------------------
// Private: resetMembers — Election.java:1446-1451
// ---------------------------------------------------------------------------
void Election::resetMembers()
{
    // Election.java:1448: ClusterMember.reset(clusterMembers)
    for (auto& m : clusterMembers_) m.reset();

    // Election.java:1449: thisMember.leadershipTermId(leadershipTermId).logPosition(appendPosition)
    ClusterMember* thisM = memberById(thisMemberId_);
    if (thisM != nullptr) {
        thisM->leadershipTermId    = leadershipTermId_;
        thisM->logPosition         = appendPosition_;
        thisM->logLeadershipTermId = logLeadershipTermId_;
    }

    // Election.java:1450: leaderMember = null
    leaderMember_ = nullptr;
}

// ---------------------------------------------------------------------------
// Private: publishCanvassPosition — Election.java:1232-1251
// ---------------------------------------------------------------------------
void Election::publishCanvassPosition()
{
    for (const auto& member : clusterMembers_) {
        if (member.id == thisMemberId_) continue;
        publishCandidacyState_(member.id);   // CRITICAL-R3: our knowledge with the canvass
        // Election.java:1247-1248: canvassPosition(member.publication(), ...)
        publisher_.canvassPosition(
            member.id,
            logLeadershipTermId_,
            appendPosition_,
            leadershipTermId_,
            thisMemberId_);
    }
}

// ---------------------------------------------------------------------------
// CRITICAL-R3 — knowledge exchange + gated self-vote (see Election.h).
// ---------------------------------------------------------------------------
void Election::publishCandidacyState_(NodeId toMember)
{
    if (candidacySrc_ == nullptr) return;
    CandidacyState st;
    if (!candidacySrc_(candidacyCtx_, st)) return;   // none / unprovable: nothing to share
    publisher_.candidacyState(toMember, thisMemberId_, st);
}

void Election::gateSelfVote_() noexcept
{
    ClusterMember* me = memberById(thisMemberId_);
    if (me == nullptr || me->candidateTermId != candidateTermId_) return;
    // The candidate's own vote is a vote like any other: it counts only if the
    // candidacy rule admits it under this node's own durable knowledge (a node
    // that acked its own removal is in the commit majority — its vote is the
    // intersection).  None, not Against: the peers' votes still count.
    me->vote = mayVoteFor(thisMemberId_) ? Vote::For : Vote::None;
}

// ---------------------------------------------------------------------------
// Private: placeVote — Election.java:1182-1196
// ---------------------------------------------------------------------------
void Election::placeVote(Term candidateTermId, NodeId candidateId, bool vote)
{
    // Election.java:1184: clusterMemberByIdMap.get(candidateId)
    ClusterMember* candidate = memberById(candidateId);
    if (candidate != nullptr) {
        if (vote) ++votesGranted_;             // ISR v3 (votesGrantedForTest)
        publishCandidacyState_(candidateId);   // CRITICAL-R3: our knowledge with the vote
        // Election.java:1187-1194: consensusPublisher.placeVote(...)
        publisher_.placeVote(
            candidateId,
            candidateTermId,
            logLeadershipTermId_,
            appendPosition_,
            candidateId,
            thisMemberId_,
            vote);
    }
}

// ---------------------------------------------------------------------------
// Private: helpers
// ---------------------------------------------------------------------------
bool Election::hasUpdateIntervalExpired(int64_t nowNs, int64_t intervalNs) const noexcept
{
    return nowNs >= (timeOfLastUpdateNs_ + intervalNs);
}

ClusterMember* Election::memberById(NodeId id) noexcept
{
    for (auto& m : clusterMembers_) {
        if (m.id == id) return &m;
    }
    return nullptr;
}

} // namespace rune::consensus
