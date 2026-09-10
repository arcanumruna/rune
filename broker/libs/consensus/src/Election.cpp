// Election.cpp — Faithful C++ port of io.aeron.cluster.Election.
// Implements ballot-phase states: INIT, CANVASS, NOMINATE, CANDIDATE_BALLOT,
// FOLLOWER_BALLOT and handlers onCanvassPosition, onRequestVote, onVote.
// States LeaderLogReplication onward are reached but not driven (P2P-5b).

#include <rune/consensus/Election.h>
#include <rune/consensus/ClusterMember.h>
#include <algorithm>
#include <stdexcept>

namespace rune::consensus {

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
    , publisher_(publisher)
    , nodeStateFile_(nodeStateFile)
    , termLog_(termLog)
    , logPosition_(logPosition)
    , appendPosition_(appendPosition)
    , leadershipTermId_(leadershipTermId)
    , logLeadershipTermId_(logLeadershipTermId)
    , candidateTermId_(leadershipTermId)
    , isNodeStartup_(isNodeStartup)
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

    // Election.java:147: ctx.electionStateCounter().setRelease(INIT.code())
    stateCode_.store(code(ElectionState::Init), std::memory_order_release);
}

// ---------------------------------------------------------------------------
// Public accessors
// ---------------------------------------------------------------------------

ElectionState Election::state()    const noexcept { return state_; }
int32_t       Election::stateCode() const noexcept { return stateCode_.load(std::memory_order_acquire); }
Term          Election::leadershipTermId()   const noexcept { return leadershipTermId_; }
Term          Election::candidateTermId()    const noexcept { return candidateTermId_; }
Term          Election::logLeadershipTermId() const noexcept { return logLeadershipTermId_; }
int64_t       Election::logPosition()        const noexcept { return logPosition_; }
int64_t       Election::appendPosition()     const noexcept { return appendPosition_; }
NodeId        Election::thisMemberId()       const noexcept { return thisMemberId_; }

// ---------------------------------------------------------------------------
// doWork — Election.java:181-260
// ---------------------------------------------------------------------------
int Election::doWork(int64_t nowNs)
{
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
        case ElectionState::Closed:
            break;
        default:
            // LeaderLogReplication and beyond: reached in P2P-5b.
            // For now do nothing — the state was set; caller can inspect it.
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
    candidateTermId_   = std::max(persistedTerm, leadershipTermId_);

    const int memberCount = static_cast<int>(clusterMembers_.size());

    if (memberCount == 1) {
        // Election.java:683-691: single-node shortcut — win immediately.
        candidateTermId_++;
        nodeStateFile_.proposeMaxCandidateTermId(candidateTermId_, logPosition_, /*timestampMs=*/0);

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
        publishCanvassPosition();
        workCount++;
    }

    // Election.java:720-727: check candidate readiness
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
        // Election.java:736-737: proposeMaxCandidateTermId(candidateTermId+1, logPosition, time)
        candidateTermId_ = nodeStateFile_.proposeMaxCandidateTermId(
            candidateTermId_ + 1, logPosition_, /*timestampMs=*/0);

        // Election.java:738: ClusterMember.becomeCandidate(clusterMembers, candidateTermId, thisMember.id())
        ClusterMember::becomeCandidate(clusterMembers_, candidateTermId_, thisMemberId_);

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

    // Election.java:758-763: unanimous check (no timeout needed)
    if (ClusterMember::isUnanimousLeader(clusterMembers_, candidateTermId_, gracefulClosedLeaderId_)) {
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
// onCanvassPosition — Election.java:284-332
// ---------------------------------------------------------------------------
void Election::onCanvassPosition(Term    logLeadershipTermId,
                                 int64_t logPosition,
                                 Term    leadershipTermId,
                                 NodeId  followerMemberId)
{
    // Election.java:291-294
    if (state_ == ElectionState::Init) return;

    // Skip messages from self
    if (followerMemberId == thisMemberId_) return;

    // Election.java:296-299: clear gracefulClosedLeaderId if it was this member
    if (followerMemberId == gracefulClosedLeaderId_) {
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

    if (candidateTermId <= candidateTermId_) {
        // Election.java:351-353: stale candidate → deny
        placeVote(candidateTermId, candidateId, false);
    } else if (ClusterMember::compareLog(logLeadershipTermId_, appendPosition_,
                                         logLeadershipTermId, logPosition) > 0) {
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
        candidateTermId_ = nodeStateFile_.proposeMaxCandidateTermId(
            candidateTermId, logPosition, /*timestampMs=*/0);
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
// Private: state transition — Election.java:1385-1438
// ---------------------------------------------------------------------------
void Election::state(ElectionState next, int64_t nowNs, const char* /*reason*/)
{
    if (next == state_) return;

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
            // Election.java:1401-1402: role = CANDIDATE (P2P-5b wires CMA)
            break;
        case ElectionState::LeaderLogReplication:
            // Election.java:1404-1407: role = LEADER; addLogPublication (P2P-5b)
            break;
        case ElectionState::FollowerLogReplication:
        case ElectionState::FollowerReplay:
            // Election.java:1410-1412: role = FOLLOWER (P2P-5b)
            break;
        default:
            break;
    }

    state_ = next;

    // Election.java:1433: electionStateCounter.setRelease(newState.code())
    stateCode_.store(code(next), std::memory_order_release);

    // Election.java:1434-1436: reset timers
    timeOfLastStateChangeNs_ = nowNs;
    timeOfLastUpdateNs_      = initialTimeOfLastUpdateNs_;
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
// Private: placeVote — Election.java:1182-1196
// ---------------------------------------------------------------------------
void Election::placeVote(Term candidateTermId, NodeId candidateId, bool vote)
{
    // Election.java:1184: clusterMemberByIdMap.get(candidateId)
    ClusterMember* candidate = memberById(candidateId);
    if (candidate != nullptr) {
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
