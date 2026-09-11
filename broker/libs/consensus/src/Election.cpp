// Election.cpp — Faithful C++ port of io.aeron.cluster.Election.
// P2P-5a: INIT, CANVASS, NOMINATE, CANDIDATE_BALLOT, FOLLOWER_BALLOT,
//         onCanvassPosition, onRequestVote, onVote.
// P2P-5b: LeaderLogReplication..Closed; onNewLeadershipTerm, onAppendPosition,
//         onCommitPosition. Archive parts are pass-throughs (no Aeron Archive in Phase 2).

#include <rune/consensus/Election.h>
#include <rune/consensus/ClusterMember.h>
#include <algorithm>
#include <cassert>
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

    // Pre-allocate rankedPositions for quorumPosition — no alloc in the hot path.
    const int threshold = ClusterMember::quorumThreshold(
        static_cast<int>(clusterMembers_.size()));
    rankedPositions_.assign(threshold, 0);

    // Election.java:147: ctx.electionStateCounter().setRelease(INIT.code())
    stateCode_.store(code(ElectionState::Init), std::memory_order_release);
}

// ---------------------------------------------------------------------------
// Public accessors
// ---------------------------------------------------------------------------

ElectionState Election::state()    const noexcept { return state_; }
int32_t       Election::stateCode() const noexcept { return stateCode_.load(std::memory_order_acquire); }
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
            return std::min(nominationDeadlineNs_,
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
            return INT64_MIN;
    }
    return INT64_MIN;
}

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
// LEADER_LOG_REPLICATION — Election.java:809-825
// New leader broadcasts NLT + CommitPosition on heartbeat interval and waits
// for quorumPosition >= appendPosition (followers report via onAppendPosition).
// ARCHIVE PASS-THROUGH: no real log publication — logPosition already == appendPosition.
// ---------------------------------------------------------------------------
int Election::leaderLogReplication(int64_t nowNs)
{
    int workCount = 0;

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
    // Election.java:430: if CLOSED ignore
    if (state_ == ElectionState::Closed) return;

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
        if (leadershipTermIdArg > leadershipTermId_) {
            // Adopt the new term durably (mirrors Aeron proposeMaxCandidateTermId).
            candidateTermId_ = nodeStateFile_.proposeMaxCandidateTermId(
                leadershipTermIdArg, logPosition_, /*timestampMs=*/0);
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
        // Adopt the higher term — node missed its ballot round.
        candidateTermId_ = nodeStateFile_.proposeMaxCandidateTermId(
            nextLeadershipTermId, logPosition_, /*timestampMs=*/0);
    } else if (nextLeadershipTermId < candidateTermId_) {
        // Stale ballot — this node has already advanced past nextLeadershipTermId.
        return;
    }
    // nextLeadershipTermId == candidateTermId_: this node participated; continue.

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
    // Election.java:541-543: ignore if wrong leadershipTermId
    if (leadershipTermIdArg != leadershipTermId_) return;

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
                                NodeId  /*leaderMemberIdArg*/)
{
    // A1 fix (audit 2026-09-11): a CommitPosition with a strictly-higher term while
    // this node is in a LEADER state signals a new election has concluded elsewhere.
    // Step down and re-enter CANVASS to participate, per Aeron Election.java:517-533
    // and ref docs/specs/…-reference.md line 146.
    if (leadershipTermIdArg > leadershipTermId_) {
        const bool inLeaderState =
            (state_ == ElectionState::LeaderLogReplication ||
             state_ == ElectionState::LeaderReplay         ||
             state_ == ElectionState::LeaderInit           ||
             state_ == ElectionState::LeaderReady);
        if (inLeaderState) {
            candidateTermId_ = nodeStateFile_.proposeMaxCandidateTermId(
                leadershipTermIdArg, logPosition_, /*timestampMs=*/0);
            leadershipTermId_ = leadershipTermIdArg;
            state(ElectionState::Canvass, nowNs, "A1: higher-term CommitPosition in LEADER state → step down");
            return;
        }
    }

    // Election.java:563-565: ignore if wrong term
    if (leadershipTermIdArg != leadershipTermId_) return;

    // Election.java:567: update notifiedCommitPosition
    if (logPositionArg > notifiedCommitPosition_) {
        notifiedCommitPosition_ = logPositionArg;
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
    stateCode_.store(code(next), std::memory_order_release);

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
