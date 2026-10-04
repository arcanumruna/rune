// ElectionBallotTest.cpp — P2P-5a: deterministic tests for Election ballot core.
// Tests: INIT, CANVASS, NOMINATE, CANDIDATE_BALLOT, FOLLOWER_BALLOT, and
//        onRequestVote / onVote / onCanvassPosition handlers.
//
// Design principles:
//   - Manual nano clock (int64_t nowNs, advanced by caller)
//   - Seeded RNG (passed to Election constructor, deterministic)
//   - FakeConsensusPublisher records all emitted calls
//   - NodeStateFile on unique temp paths (cleaned up in TempFile destructor)
//   - TermLog on unique temp paths
//   - No tautologies: always assert concrete expected values

#include <gtest/gtest.h>
#include <rune/consensus/Election.h>
#include <rune/consensus/ConsensusPublisher.h>
#include <rune/consensus/ClusterMember.h>
#include <rune/consensus/ElectionState.h>
#include <rune/consensus/NodeStateFile.h>
#include <rune/consensus/TermLog.h>
#include <rune/consensus/Types.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <vector>
#include <unistd.h>

using namespace rune::consensus;
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// TempFile — unique path, cleaned up on destruction
// ---------------------------------------------------------------------------
struct TempFile {
    fs::path path;
    explicit TempFile(std::string_view suffix = ".dat") {
        static std::atomic<uint64_t> g_tempSeq{0};
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        auto uniq = std::to_string(::getpid()) + "_" +
                    std::to_string(g_tempSeq.fetch_add(1, std::memory_order_relaxed)) + "_" +
                    std::to_string(ts);
        path = fs::temp_directory_path() /
               ("rune_ebt_" + uniq + std::string(suffix));
        fs::remove(path);
    }
    ~TempFile() { std::error_code ec; fs::remove(path, ec); }
    std::string str() const { return path.string(); }
};

// ---------------------------------------------------------------------------
// FakeConsensusPublisher — records emitted messages for assertion
// ---------------------------------------------------------------------------
struct CanvassCall {
    NodeId  toMember;
    Term    logLeadershipTermId;
    int64_t logPosition;
    Term    leadershipTermId;
    NodeId  followerMemberId;
};
struct RequestVoteCall {
    NodeId  toMember;
    Term    logLeadershipTermId;
    int64_t logPosition;
    Term    candidateTermId;
    NodeId  candidateMemberId;
};
struct PlaceVoteCall {
    NodeId  toMember;
    Term    candidateTermId;
    Term    logLeadershipTermId;
    int64_t logPosition;
    NodeId  candidateMemberId;
    NodeId  followerMemberId;
    bool    vote;
};
struct NewLeadershipTermCall {
    NodeId  toMember;
    Term    logLeadershipTermId;
    Term    nextLeadershipTermId;
    int64_t nextTermBaseLogPosition;
    int64_t nextLogPosition;
    Term    leadershipTermId;
    int64_t termBaseLogPosition;
    int64_t logPosition;
    int64_t leaderRecordingId;
    int64_t timestamp;
    NodeId  leaderMemberId;
    int32_t logSessionId;
    bool    isStartup;
    int64_t commitPosition;
};
struct AppendPositionCall {
    NodeId  toMember;
    Term    leadershipTermId;
    int64_t logPosition;
    NodeId  followerMemberId;
    uint8_t flags;
};
struct CommitPositionCall {
    NodeId  toMember;
    Term    leadershipTermId;
    int64_t logPosition;
    NodeId  leaderMemberId;
};

struct FakeConsensusPublisher : ConsensusPublisher {
    std::vector<CanvassCall>           canvassCalls;
    std::vector<RequestVoteCall>       requestVoteCalls;
    std::vector<PlaceVoteCall>         placeVoteCalls;
    std::vector<NewLeadershipTermCall> newLeadershipTermCalls;
    std::vector<AppendPositionCall>    appendPositionCalls;
    std::vector<CommitPositionCall>    commitPositionCalls;
    std::vector<std::pair<NodeId, CandidacyState>> candidacyCalls;   ///< (to, state)

    void candidacyState(NodeId toMember, NodeId, const CandidacyState& st) override {
        candidacyCalls.emplace_back(toMember, st);
    }

    void canvassPosition(NodeId toMember, Term logLeadershipTermId, int64_t logPosition,
                         Term leadershipTermId, NodeId followerMemberId) override {
        canvassCalls.push_back({toMember, logLeadershipTermId, logPosition, leadershipTermId, followerMemberId});
    }
    void requestVote(NodeId toMember, Term logLeadershipTermId, int64_t logPosition,
                     Term candidateTermId, NodeId candidateMemberId) override {
        requestVoteCalls.push_back({toMember, logLeadershipTermId, logPosition, candidateTermId, candidateMemberId});
    }
    void placeVote(NodeId toMember, Term candidateTermId, Term logLeadershipTermId, int64_t logPosition,
                   NodeId candidateMemberId, NodeId followerMemberId, bool vote) override {
        placeVoteCalls.push_back({toMember, candidateTermId, logLeadershipTermId, logPosition,
                                   candidateMemberId, followerMemberId, vote});
    }
    void newLeadershipTerm(NodeId toMember, Term logLeadershipTermId, Term nextLeadershipTermId,
                           int64_t nextTermBaseLogPosition, int64_t nextLogPosition,
                           Term leadershipTermId, int64_t termBaseLogPosition, int64_t logPosition,
                           int64_t leaderRecordingId, int64_t timestamp, NodeId leaderMemberId,
                           int32_t logSessionId, bool isStartup, int64_t commitPosition) override {
        newLeadershipTermCalls.push_back({toMember, logLeadershipTermId, nextLeadershipTermId,
            nextTermBaseLogPosition, nextLogPosition, leadershipTermId, termBaseLogPosition,
            logPosition, leaderRecordingId, timestamp, leaderMemberId, logSessionId,
            isStartup, commitPosition});
    }
    void appendPosition(NodeId toMember, Term leadershipTermId, int64_t logPosition,
                        NodeId followerMemberId, uint8_t flags) override {
        appendPositionCalls.push_back({toMember, leadershipTermId, logPosition, followerMemberId, flags});
    }
    void commitPosition(NodeId toMember, Term leadershipTermId, int64_t logPosition,
                        NodeId leaderMemberId) override {
        commitPositionCalls.push_back({toMember, leadershipTermId, logPosition, leaderMemberId});
    }
    void clear() {
        canvassCalls.clear();
        requestVoteCalls.clear();
        placeVoteCalls.clear();
        newLeadershipTermCalls.clear();
        appendPositionCalls.clear();
        commitPositionCalls.clear();
    }
};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------
static constexpr int64_t kElectionTimeoutNs       = 150'000'000LL; // 150ms
static constexpr int64_t kStatusIntervalNs         =  50'000'000LL; // 50ms
static constexpr int64_t kStartupCanvassTimeoutNs  = 300'000'000LL; // 300ms

static std::vector<ClusterMember> makeMembers1(NodeId self = 0) {
    ClusterMember m; m.id = self; m.logPosition = 0; m.logLeadershipTermId = 0; m.leadershipTermId = 0;
    return {m};
}
static std::vector<ClusterMember> makeMembers3(NodeId self = 0) {
    std::vector<ClusterMember> ms(3);
    for (int i = 0; i < 3; i++) {
        ms[i].id = i; ms[i].logPosition = 0; ms[i].logLeadershipTermId = 0; ms[i].leadershipTermId = 0;
    }
    return ms;
}
static std::vector<ClusterMember> makeMembers5(NodeId self = 0) {
    std::vector<ClusterMember> ms(5);
    for (int i = 0; i < 5; i++) {
        ms[i].id = i; ms[i].logPosition = 0; ms[i].logLeadershipTermId = 0; ms[i].leadershipTermId = 0;
    }
    return ms;
}

static Election makeElection(
    std::vector<ClusterMember> members,
    NodeId                     self,
    FakeConsensusPublisher&    pub,
    NodeStateFile&             nsf,
    TermLog&                   tl,
    bool                       isNodeStartup = true,
    uint64_t                   rngSeed       = 42)
{
    return Election(
        isNodeStartup,
        /*gracefulClosedLeaderId=*/-1,
        /*leadershipTermId=*/0,
        /*logLeadershipTermId=*/0,
        /*logPosition=*/0,
        /*appendPosition=*/0,
        std::move(members),
        self,
        pub,
        nsf,
        tl,
        kElectionTimeoutNs,
        kStatusIntervalNs,
        kStartupCanvassTimeoutNs,
        rngSeed);
}

// ============================================================================
// TEST GROUP 1: Single-member cluster — immediate win on INIT
// ============================================================================

// T1: Single-member init → LeaderLogReplication; candidateTermId incremented.
TEST(ElectionBallot, SingleMember_Init_WinsImmediately) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers1(0), 0, pub, nsf, tl);

    int64_t nowNs = 1'000'000LL;
    int work = e.doWork(nowNs);

    EXPECT_EQ(e.state(), ElectionState::LeaderLogReplication);
    EXPECT_EQ(e.stateCode(), static_cast<int32_t>(ElectionState::LeaderLogReplication));
    EXPECT_EQ(e.candidateTermId(), 1); // leadershipTermId(0) + 1
    EXPECT_EQ(e.leadershipTermId(), 1);
    EXPECT_GT(work, 0);
}

// T2: Single-member — candidateTermId persisted to NodeStateFile.
TEST(ElectionBallot, SingleMember_CandidateTermId_Persisted) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    {
        NodeStateFile nsf(nsfFile.str());
        TermLog       tl(tlFile.str());
        FakeConsensusPublisher pub;

        auto e = makeElection(makeMembers1(0), 0, pub, nsf, tl);
        e.doWork(1'000'000LL);
        EXPECT_EQ(e.candidateTermId(), 1);
    }
    // Reopen NodeStateFile — candidateTermId must be >= 1
    NodeStateFile nsf2(nsfFile.str());
    EXPECT_GE(nsf2.candidateTermId(), 1);
}

// T3: Single-member — no canvass or vote messages emitted.
TEST(ElectionBallot, SingleMember_NoMessagesEmitted) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers1(0), 0, pub, nsf, tl);
    e.doWork(1'000'000LL);

    EXPECT_TRUE(pub.canvassCalls.empty());
    EXPECT_TRUE(pub.requestVoteCalls.empty());
    EXPECT_TRUE(pub.placeVoteCalls.empty());
}

// T4: Single-member — stateCode atomic reflects LeaderLogReplication code.
TEST(ElectionBallot, SingleMember_StateCodeAtomic_MatchesLeaderLogReplication) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers1(0), 0, pub, nsf, tl);
    e.doWork(1'000'000LL);

    EXPECT_EQ(e.stateCode(), static_cast<int32_t>(ElectionState::LeaderLogReplication));
    EXPECT_EQ(e.stateCode(), 5); // LeaderLogReplication ordinal == 5
}

// ============================================================================
// TEST GROUP 2: 3-member cluster — INIT → CANVASS
// ============================================================================

// T5: 3-member init → CANVASS state.
TEST(ElectionBallot, ThreeMember_Init_GoesToCanvass) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    e.doWork(1'000'000LL);

    EXPECT_EQ(e.state(), ElectionState::Canvass);
    EXPECT_EQ(e.stateCode(), static_cast<int32_t>(ElectionState::Canvass));
}

// T6: CANVASS — CanvassPosition emitted to both peers on first doWork.
TEST(ElectionBallot, Canvass_BroadcastsToAllPeers) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    e.doWork(1'000'000LL); // INIT → CANVASS
    pub.clear();

    // Advance past the status interval
    e.doWork(1'000'000LL + kStatusIntervalNs + 1LL);

    // Should have sent CanvassPosition to peers 1 and 2 (not self 0)
    ASSERT_EQ(pub.canvassCalls.size(), 2u);
    std::vector<NodeId> targets;
    for (auto& c : pub.canvassCalls) targets.push_back(c.toMember);
    std::sort(targets.begin(), targets.end());
    EXPECT_EQ(targets[0], 1);
    EXPECT_EQ(targets[1], 2);
}

// T7: CANVASS — CanvassPosition fields correct (logLeadershipTermId, followerMemberId).
TEST(ElectionBallot, Canvass_CanvassPositionFields_Correct) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    e.doWork(1'000'000LL); // INIT → CANVASS
    pub.clear();
    e.doWork(1'000'000LL + kStatusIntervalNs + 1LL);

    ASSERT_GE(pub.canvassCalls.size(), 1u);
    auto& c = pub.canvassCalls[0];
    EXPECT_EQ(c.followerMemberId, 0);     // self
    EXPECT_EQ(c.logLeadershipTermId, 0);  // initial
    EXPECT_EQ(c.leadershipTermId, 0);     // initial
    EXPECT_EQ(c.logPosition, 0);          // appendPosition (initial)
}

// T8: CANVASS — CanvassPosition NOT emitted a second time before interval expires.
// The first canvass() call always publishes (initialTimeOfLastUpdateNs ensures this).
// After that, the interval must expire before another publish happens.
TEST(ElectionBallot, Canvass_NoCanvassBeforeInterval) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    e.doWork(1'000'000LL); // INIT → CANVASS
    // First canvass doWork always publishes due to initialTimeOfLastUpdateNs.
    int64_t firstCanvassNs = 1'000'000LL + kStatusIntervalNs + 1LL;
    e.doWork(firstCanvassNs);         // first interval fires, sets timeOfLastUpdateNs = firstCanvassNs
    pub.clear();

    // doWork 1ns later — interval (50ms) has NOT expired yet
    e.doWork(firstCanvassNs + 1LL);
    EXPECT_TRUE(pub.canvassCalls.empty());
}

// ============================================================================
// TEST GROUP 3: CANVASS → NOMINATE (feeding peer canvass positions)
// ============================================================================

// T9: Feed canvass positions from both peers → isQuorumCandidate → NOMINATE
//     (both peers at same logPosition=0 as self → willVoteFor=true).
TEST(ElectionBallot, Canvass_PeerPositions_QuorumCandidate_GoesToNominate) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // INIT → CANVASS

    // Feed canvass positions from peers (logPosition=0 means they will vote for self)
    e.onCanvassPosition(/*logLeadershipTermId=*/0, /*logPosition=*/0, /*leadershipTermId=*/0, /*from=*/1);
    e.onCanvassPosition(/*logLeadershipTermId=*/0, /*logPosition=*/0, /*leadershipTermId=*/0, /*from=*/2);

    // Advance past startup canvass timeout
    int64_t afterTimeout = nowNs + kStartupCanvassTimeoutNs + 1LL;
    e.doWork(afterTimeout);

    EXPECT_EQ(e.state(), ElectionState::Nominate);
}

// T10: Unanimous canvass positions → NOMINATE immediately (before deadline).
TEST(ElectionBallot, Canvass_UnanimousPositions_NominateImmediate) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // INIT → CANVASS

    // Feed canvass from peers
    e.onCanvassPosition(0, 0, 0, 1);
    e.onCanvassPosition(0, 0, 0, 2);

    // doWork well before deadline — should still transition (unanimous)
    e.doWork(nowNs + 1'000LL);
    EXPECT_EQ(e.state(), ElectionState::Nominate);
}

// ============================================================================
// TEST GROUP 4: NOMINATE → CANDIDATE_BALLOT
// ============================================================================

// T11: After backoff → CANDIDATE_BALLOT; candidateTermId incremented + persisted.
TEST(ElectionBallot, Nominate_AfterBackoff_GoesToCandidateBallot) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    // Seed=0 → distribution yields a very small (possibly 0) backoff
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, /*rngSeed=*/0);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // INIT → CANVASS

    e.onCanvassPosition(0, 0, 0, 1);
    e.onCanvassPosition(0, 0, 0, 2);
    e.doWork(nowNs + kStartupCanvassTimeoutNs + 1LL); // → NOMINATE

    EXPECT_EQ(e.state(), ElectionState::Nominate);

    // Drive past nomination deadline (use a very large nowNs to exceed any backoff)
    int64_t farFuture = nowNs + kStartupCanvassTimeoutNs + kElectionTimeoutNs * 10;
    e.doWork(farFuture); // → CANDIDATE_BALLOT

    EXPECT_EQ(e.state(), ElectionState::CandidateBallot);
    EXPECT_EQ(e.candidateTermId(), 1); // was 0, incremented to 1
}

// T12: NOMINATE → CANDIDATE_BALLOT — candidateTermId persisted to NodeStateFile.
TEST(ElectionBallot, Nominate_CandidateTermId_Persisted) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    {
        NodeStateFile nsf(nsfFile.str());
        TermLog       tl(tlFile.str());
        FakeConsensusPublisher pub;
        auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0);
        int64_t nowNs = 1'000'000LL;
        e.doWork(nowNs);
        e.onCanvassPosition(0, 0, 0, 1);
        e.onCanvassPosition(0, 0, 0, 2);
        e.doWork(nowNs + kStartupCanvassTimeoutNs + 1LL);
        e.doWork(nowNs + kStartupCanvassTimeoutNs + kElectionTimeoutNs * 10);
        EXPECT_EQ(e.state(), ElectionState::CandidateBallot);
        // candidateTermId should be persisted
    }
    NodeStateFile nsf2(nsfFile.str());
    EXPECT_GE(nsf2.candidateTermId(), 1);
}

// T13: CANDIDATE_BALLOT — RequestVote emitted to peers (not self).
TEST(ElectionBallot, CandidateBallot_RequestVote_EmittedToPeers) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);
    e.onCanvassPosition(0, 0, 0, 1);
    e.onCanvassPosition(0, 0, 0, 2);
    e.doWork(nowNs + kStartupCanvassTimeoutNs + 1LL);
    int64_t farFuture = nowNs + kStartupCanvassTimeoutNs + kElectionTimeoutNs * 10;
    e.doWork(farFuture);
    EXPECT_EQ(e.state(), ElectionState::CandidateBallot);

    pub.clear();
    // One more doWork in CANDIDATE_BALLOT — should send RequestVote to peers
    e.doWork(farFuture + 1LL);

    // Should have sent to members 1 and 2 (not self 0)
    EXPECT_EQ(pub.requestVoteCalls.size(), 2u);
    for (auto& rv : pub.requestVoteCalls) {
        EXPECT_NE(rv.toMember, 0);          // not self
        EXPECT_EQ(rv.candidateMemberId, 0); // from self
        EXPECT_EQ(rv.candidateTermId, 1);   // incremented
    }
}

// T14: CANDIDATE_BALLOT — RequestVote only sent once per peer (isBallotSent).
TEST(ElectionBallot, CandidateBallot_RequestVote_SentOnlyOnce) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);
    e.onCanvassPosition(0, 0, 0, 1);
    e.onCanvassPosition(0, 0, 0, 2);
    e.doWork(nowNs + kStartupCanvassTimeoutNs + 1LL);
    int64_t farFuture = nowNs + kStartupCanvassTimeoutNs + kElectionTimeoutNs * 10;
    e.doWork(farFuture);

    pub.clear();
    e.doWork(farFuture + 1LL);  // sends to peers 1 and 2
    size_t firstCount = pub.requestVoteCalls.size();

    pub.clear();
    e.doWork(farFuture + 2LL);  // no re-send
    EXPECT_EQ(pub.requestVoteCalls.size(), 0u);
    EXPECT_EQ(firstCount, 2u);
}

// ============================================================================
// TEST GROUP 5: onVote → quorum → win
// ============================================================================

// Helper to bring election into CANDIDATE_BALLOT
static void bringToCandidateBallot(Election& e, int64_t& nowNs) {
    e.doWork(nowNs);
    e.onCanvassPosition(0, 0, 0, 1);
    e.onCanvassPosition(0, 0, 0, 2);
    nowNs += 50'000'000LL + kStartupCanvassTimeoutNs + 1LL;
    e.doWork(nowNs);
    nowNs += kElectionTimeoutNs * 10;
    e.doWork(nowNs);
    nowNs += 1LL;
    e.doWork(nowNs); // sends RequestVotes
}

// T15: onVote from one peer (quorum = 2/3) → quorum leader after timeout → LeaderLogReplication.
TEST(ElectionBallot, OnVote_QuorumGrant_AfterTimeout_WinsLeadership) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0);

    int64_t nowNs = 1'000'000LL;
    bringToCandidateBallot(e, nowNs);
    ASSERT_EQ(e.state(), ElectionState::CandidateBallot);

    Term cTerm = e.candidateTermId();

    // Feed vote from peer 1 (vote=true) — now quorum of 2/3 (self + peer 1)
    e.onVote(/*logLeadershipTermId=*/0, /*logPosition=*/0, cTerm, /*candidateId=*/0, /*voterId=*/1, /*vote=*/true);

    // Advance past election timeout → quorum check
    e.doWork(nowNs + kElectionTimeoutNs + 1LL);

    EXPECT_EQ(e.state(), ElectionState::LeaderLogReplication);
    EXPECT_EQ(e.leadershipTermId(), cTerm);
}

// T16: onVote — unanimous (all 3 members) → wins immediately without timeout.
TEST(ElectionBallot, OnVote_Unanimous_WinsWithoutTimeout) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0);

    int64_t nowNs = 1'000'000LL;
    bringToCandidateBallot(e, nowNs);
    ASSERT_EQ(e.state(), ElectionState::CandidateBallot);
    Term cTerm = e.candidateTermId();

    // Feed votes from both peers
    e.onVote(0, 0, cTerm, 0, 1, true);
    e.onVote(0, 0, cTerm, 0, 2, true);

    // doWork — unanimous check should fire immediately
    e.doWork(nowNs + 1LL); // before timeout

    EXPECT_EQ(e.state(), ElectionState::LeaderLogReplication);
}

// T17: No votes received → timeout → back to CANVASS.
TEST(ElectionBallot, CandidateBallot_Timeout_NoQuorum_GoesBackToCanvass) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0);

    int64_t nowNs = 1'000'000LL;
    bringToCandidateBallot(e, nowNs);
    ASSERT_EQ(e.state(), ElectionState::CandidateBallot);

    // No votes at all — advance past timeout
    e.doWork(nowNs + kElectionTimeoutNs + 1LL);

    EXPECT_EQ(e.state(), ElectionState::Canvass);
}

// T18: onVote with wrong candidateTermId is ignored.
TEST(ElectionBallot, OnVote_WrongCandidateTermId_Ignored) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0);

    int64_t nowNs = 1'000'000LL;
    bringToCandidateBallot(e, nowNs);
    ASSERT_EQ(e.state(), ElectionState::CandidateBallot);
    Term cTerm = e.candidateTermId();

    // Send vote with wrong candidateTermId
    e.onVote(0, 0, cTerm + 99, 0, 1, true);

    // Advance past timeout — should be CANVASS (no quorum)
    e.doWork(nowNs + kElectionTimeoutNs + 1LL);
    EXPECT_EQ(e.state(), ElectionState::Canvass);
}

// T19: onVote with wrong candidateId (not self) is ignored.
TEST(ElectionBallot, OnVote_WrongCandidateId_Ignored) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0);

    int64_t nowNs = 1'000'000LL;
    bringToCandidateBallot(e, nowNs);
    Term cTerm = e.candidateTermId();

    // vote directed at candidateId=1 (not self=0)
    e.onVote(0, 0, cTerm, /*candidateId=*/1, /*voterId=*/2, true);

    // No quorum → timeout → CANVASS
    e.doWork(nowNs + kElectionTimeoutNs + 1LL);
    EXPECT_EQ(e.state(), ElectionState::Canvass);
}

// ============================================================================
// TEST GROUP 6: onRequestVote handler
// ============================================================================

// T20: onRequestVote — grant when candidate up-to-date; placeVote(true) emitted.
TEST(ElectionBallot, OnRequestVote_UpToDate_GrantsVote_PlaceVoteEmitted) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // INIT → CANVASS

    pub.clear();
    // Candidate 1 with term=1 (higher than our candidateTermId=0), log equal to us
    e.onRequestVote(/*logLeadershipTermId=*/0, /*logPosition=*/0, /*candidateTermId=*/1, /*candidateId=*/1, nowNs);

    ASSERT_EQ(pub.placeVoteCalls.size(), 1u);
    auto& pv = pub.placeVoteCalls[0];
    EXPECT_TRUE(pv.vote);
    EXPECT_EQ(pv.toMember, 1);
    EXPECT_EQ(pv.candidateTermId, 1);
    EXPECT_EQ(pv.followerMemberId, 0); // self
}

// T21: onRequestVote grant → state transitions to FOLLOWER_BALLOT.
TEST(ElectionBallot, OnRequestVote_Grant_GoesToFollowerBallot) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);

    e.onRequestVote(0, 0, 1, 1, nowNs);

    EXPECT_EQ(e.state(), ElectionState::FollowerBallot);
}

// T22: onRequestVote grant → candidateTermId updated + persisted.
TEST(ElectionBallot, OnRequestVote_Grant_CandidateTermId_PersistsAcrossReopen) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    {
        NodeStateFile nsf(nsfFile.str());
        TermLog       tl(tlFile.str());
        FakeConsensusPublisher pub;
        auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
        int64_t nowNs = 1'000'000LL;
        e.doWork(nowNs);
        e.onRequestVote(0, 0, /*candidateTermId=*/3, 1, nowNs);
        EXPECT_EQ(e.candidateTermId(), 3);
    }
    // Reopen — persisted term must be >= 3
    NodeStateFile nsf2(nsfFile.str());
    EXPECT_GE(nsf2.candidateTermId(), 3);
}

// T23: onRequestVote — deny when our log is more recent (compareLog > 0); placeVote(false).
TEST(ElectionBallot, OnRequestVote_OurLogMoreRecent_Denies) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    // Self has logLeadershipTermId=5, appendPosition=100
    std::vector<ClusterMember> ms(3);
    for (int i = 0; i < 3; i++) { ms[i].id = i; ms[i].logPosition = 0; ms[i].logLeadershipTermId = 0; ms[i].leadershipTermId = 0; }
    Election e(true, -1,
               /*leadershipTermId=*/5, /*logLeadershipTermId=*/5,
               /*logPosition=*/100, /*appendPosition=*/100,
               std::move(ms), 0, pub, nsf, tl,
               kElectionTimeoutNs, kStatusIntervalNs, kStartupCanvassTimeoutNs, 42);
    e.doWork(1'000'000LL); // INIT → CANVASS

    pub.clear();
    int64_t nowNs = 1'000'000LL;
    // Candidate 1 with lower log (logLeadershipTermId=3, logPosition=50)
    e.onRequestVote(/*logLeadershipTermId=*/3, /*logPosition=*/50, /*candidateTermId=*/6, 1, nowNs);

    ASSERT_EQ(pub.placeVoteCalls.size(), 1u);
    EXPECT_FALSE(pub.placeVoteCalls[0].vote);
}

// T24: onRequestVote — deny when candidateTermId <= our candidateTermId; placeVote(false).
TEST(ElectionBallot, OnRequestVote_StaleCandidateTerm_Denies) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    nsf.proposeMaxCandidateTermId(5, 0, 0); // pre-seed: our candidateTermId = 5
    TermLog tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);

    pub.clear();
    // Candidate with term=5 (== ours) → deny
    e.onRequestVote(0, 0, /*candidateTermId=*/5, 1, nowNs);

    ASSERT_EQ(pub.placeVoteCalls.size(), 1u);
    EXPECT_FALSE(pub.placeVoteCalls[0].vote);
}

// T25: onRequestVote — INIT state → no response.
TEST(ElectionBallot, OnRequestVote_InInitState_Ignored) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);

    // Don't call doWork — stays in INIT
    ASSERT_EQ(e.state(), ElectionState::Init);
    int64_t nowNs = 1'000'000LL;
    e.onRequestVote(0, 0, 1, 1, nowNs);

    EXPECT_TRUE(pub.placeVoteCalls.empty());
}

// ============================================================================
// TEST GROUP 7: FOLLOWER_BALLOT timeout
// ============================================================================

// T26: FOLLOWER_BALLOT — timeout → CANVASS.
TEST(ElectionBallot, FollowerBallot_Timeout_GoesToCanvass) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // INIT → CANVASS
    e.onRequestVote(0, 0, 1, 1, nowNs); // → FOLLOWER_BALLOT
    ASSERT_EQ(e.state(), ElectionState::FollowerBallot);

    // Advance past election timeout (timeOfLastStateChangeNs = nowNs from onRequestVote)
    e.doWork(nowNs + kElectionTimeoutNs + 1LL);
    EXPECT_EQ(e.state(), ElectionState::Canvass);
}

// T27: FOLLOWER_BALLOT — before timeout, stays in FOLLOWER_BALLOT.
TEST(ElectionBallot, FollowerBallot_BeforeTimeout_StaysInState) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);
    e.onRequestVote(0, 0, 1, 1, nowNs);
    ASSERT_EQ(e.state(), ElectionState::FollowerBallot);

    e.doWork(nowNs + 1LL); // well before timeout (timeOfLastStateChangeNs=nowNs, timeout=150ms; 1ns < 150ms)
    EXPECT_EQ(e.state(), ElectionState::FollowerBallot);
}

// ============================================================================
// TEST GROUP 8: State atomic tracking
// ============================================================================

// T28: State code is updated atomically on each transition.
TEST(ElectionBallot, StateCode_UpdatedOnEveryTransition) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);

    // Initial: INIT = code 0
    EXPECT_EQ(e.stateCode(), 0);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // → CANVASS = code 1
    EXPECT_EQ(e.stateCode(), 1);

    // Trigger vote to FOLLOWER_BALLOT = code 4
    e.onRequestVote(0, 0, 1, 1, nowNs);
    EXPECT_EQ(e.stateCode(), 4);

    // Timeout → CANVASS = code 1
    e.doWork(nowNs + kElectionTimeoutNs + 1LL);
    EXPECT_EQ(e.stateCode(), 1);
}

// T29: onCanvassPosition — updates the peer member's logPosition.
TEST(ElectionBallot, OnCanvassPosition_UpdatesPeerLogPosition) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    e.doWork(1'000'000LL); // → CANVASS

    // Feed canvass from peer 1 with non-zero logPosition (higher leadershipTermId)
    // leadershipTermId=2 > candidate's leadershipTermId=0 → peer 1 won't vote for self
    e.onCanvassPosition(/*logLeadershipTermId=*/2, /*logPosition=*/500, /*leadershipTermId=*/2, /*from=*/1);
    // Add peer 2 at same position as self
    e.onCanvassPosition(0, 0, 0, 2);

    // After timeout: peer 1 ahead (leadershipTermId=2 > self 0) → won't vote for self
    // peer 2 at leadershipTermId=0, logPosition=0 = self → will vote
    // self votes for self → 2 votes (self + peer2) >= threshold(3)=2 → isQuorumCandidate → NOMINATE
    int64_t afterTimeout = 1'000'000LL + kStartupCanvassTimeoutNs + 1LL;
    e.doWork(afterTimeout);
    EXPECT_EQ(e.state(), ElectionState::Nominate);
}

// T30: 5-member cluster — needs 3 votes for quorum; 2 votes not enough.
TEST(ElectionBallot, FiveMember_TwoVotes_NotEnoughQuorum_Canvass) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers5(0), 0, pub, nsf, tl, true, 0);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // INIT → CANVASS

    // Feed all 4 peers at same position → unanimous → NOMINATE
    for (int i = 1; i <= 4; i++) e.onCanvassPosition(0, 0, 0, i);
    e.doWork(nowNs + kStartupCanvassTimeoutNs + 1LL);
    ASSERT_EQ(e.state(), ElectionState::Nominate);

    // Drive to CANDIDATE_BALLOT
    e.doWork(nowNs + kStartupCanvassTimeoutNs + kElectionTimeoutNs * 10);
    ASSERT_EQ(e.state(), ElectionState::CandidateBallot);

    Term cTerm = e.candidateTermId();
    // Feed only 1 vote (self + 1 peer = 2 total, quorum=3 for 5-member)
    e.onVote(0, 0, cTerm, 0, 1, true);

    // Advance past timeout → not quorum → back to CANVASS
    e.doWork(nowNs + kStartupCanvassTimeoutNs + kElectionTimeoutNs * 20);
    EXPECT_EQ(e.state(), ElectionState::Canvass);
}

// ============================================================================
// C2: onRequestVote same-term shorter-log boundary (audit T3/C2)
// The classic off-by-one "elect a leader with a shorter log" must be denied.
// ============================================================================

// C2: onRequestVote — same logLeadershipTermId, candidate position < our appendPosition → deny.
// This is the off-by-one case: candidate in same term but shorter log must not win our vote.
TEST(ElectionBallot, OnRequestVote_SameTerm_CandidateShorterLog_Denies) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    // Self has logLeadershipTermId=5, appendPosition=100
    std::vector<ClusterMember> ms(3);
    for (int i = 0; i < 3; i++) {
        ms[i].id = i;
        ms[i].logPosition = 0;
        ms[i].logLeadershipTermId = 0;
        ms[i].leadershipTermId = 0;
    }
    Election e(true, -1,
               /*leadershipTermId=*/5, /*logLeadershipTermId=*/5,
               /*logPosition=*/100, /*appendPosition=*/100,
               std::move(ms), 0, pub, nsf, tl,
               kElectionTimeoutNs, kStatusIntervalNs, kStartupCanvassTimeoutNs, 42);
    e.doWork(1'000'000LL); // INIT → CANVASS

    pub.clear();
    int64_t nowNs = 1'000'000LL;
    // Candidate 1: SAME logLeadershipTermId=5 but logPosition=99 (one entry behind)
    // Election.cpp compareLog(5,100, 5,99) = +1 → our log is more recent → deny.
    e.onRequestVote(/*logLeadershipTermId=*/5, /*logPosition=*/99,
                    /*candidateTermId=*/6, /*candidateId=*/1, nowNs);

    ASSERT_EQ(pub.placeVoteCalls.size(), 1u);
    EXPECT_FALSE(pub.placeVoteCalls[0].vote)
        << "Candidate with same term but shorter log (pos=99 < 100) must be denied";
}

// C2b: onRequestVote — same logLeadershipTermId, equal position → grant.
TEST(ElectionBallot, OnRequestVote_SameTerm_EqualPosition_Grants) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    // Self has logLeadershipTermId=5, appendPosition=100
    std::vector<ClusterMember> ms(3);
    for (int i = 0; i < 3; i++) {
        ms[i].id = i;
        ms[i].logPosition = 0;
        ms[i].logLeadershipTermId = 0;
        ms[i].leadershipTermId = 0;
    }
    Election e(true, -1,
               /*leadershipTermId=*/5, /*logLeadershipTermId=*/5,
               /*logPosition=*/100, /*appendPosition=*/100,
               std::move(ms), 0, pub, nsf, tl,
               kElectionTimeoutNs, kStatusIntervalNs, kStartupCanvassTimeoutNs, 42);
    e.doWork(1'000'000LL); // INIT → CANVASS

    pub.clear();
    int64_t nowNs = 1'000'000LL;
    // Candidate 1: SAME logLeadershipTermId=5, SAME logPosition=100 → grant.
    e.onRequestVote(/*logLeadershipTermId=*/5, /*logPosition=*/100,
                    /*candidateTermId=*/6, /*candidateId=*/1, nowNs);

    ASSERT_EQ(pub.placeVoteCalls.size(), 1u);
    EXPECT_TRUE(pub.placeVoteCalls[0].vote)
        << "Candidate with same term and equal log position must be granted";
}

// ============================================================================
// Review 2026-10-04 MAJOR-2/3 — candidacy rule (setVoteGate): a node outside
// the newest durable internal ISR never gathers a majority.
// ============================================================================
namespace {
struct GateCtx { NodeId denied = -1; };
bool denyOne(void* ctx, NodeId candidate) noexcept {
    return candidate != static_cast<GateCtx*>(ctx)->denied;
}
} // namespace

// A gated-out candidate's RequestVote is denied (like a stale log).
TEST(ElectionBallotRegression, VoteGate_DeniesCandidateOutsideInternalIsr) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    GateCtx g{/*denied=*/1};
    e.setVoteGate(&denyOne, &g);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);                             // INIT → CANVASS
    pub.clear();
    e.onRequestVote(0, 0, /*candidateTermId=*/1, /*candidateId=*/1, nowNs);
    ASSERT_EQ(pub.placeVoteCalls.size(), 1u);
    EXPECT_FALSE(pub.placeVoteCalls[0].vote) << "voted for a candidate outside the internal ISR";
    EXPECT_EQ(e.state(), ElectionState::Canvass);
    pub.clear();
    e.onRequestVote(0, 0, /*candidateTermId=*/2, /*candidateId=*/2, nowNs);
    ASSERT_EQ(pub.placeVoteCalls.size(), 1u);
    EXPECT_TRUE(pub.placeVoteCalls[0].vote) << "an eligible candidate must still get the vote";
}

// Re-review 2026-10-04 (fix round 2) MAJOR-A: the candidacy rule is applied
// by VOTERS only.  A node whose OWN knowledge excludes itself (stale: it missed
// the frame that re-admitted it) still stands — the voters decide (any grant
// majority intersects the commit majority).  The old self-gate turned stale
// self-knowledge into a permanently headless consensus.  (Replaces
// VoteGate_IneligibleNodeNeverNominates, which pinned the self-gate.)
TEST(ElectionBallotRegression, VoteGate_StaleSelfKnowledgeStillStands) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    GateCtx g{/*denied=*/0};                     // self's own knowledge excludes self
    e.setVoteGate(&denyOne, &g);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);                             // INIT → CANVASS
    e.onCanvassPosition(0, 0, 0, 1);
    e.onCanvassPosition(0, 0, 0, 2);
    for (int i = 0; i < 20 && e.state() != ElectionState::CandidateBallot; ++i) {
        nowNs += kElectionTimeoutNs;
        e.doWork(nowNs);
    }
    EXPECT_EQ(e.state(), ElectionState::CandidateBallot)
        << "a node with stale self-knowledge refused to stand (headless consensus)";
    EXPECT_GE(e.candidateTermId(), 1);
}

// Re-review 2026-10-04 (fix round 3) MAJOR-R3: the internal-ISR ack term is
// the term this node GRANTED a vote in or followed a leader in.  A DENIED
// RequestVote raises candidateTermId (Aeron) but must never raise the ack term
// — otherwise a live leader and its followers refuse their own metadata ISR
// states forever after one denied candidacy.
TEST(ElectionBallotRegression, DeniedRequestVoteNeverRaisesAckTerm_GrantDoes) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    GateCtx g{/*denied=*/1};
    e.setVoteGate(&denyOne, &g);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);                             // INIT → CANVASS
    const Term before = e.ackTermId();
    e.onRequestVote(0, 0, /*candidateTermId=*/5, /*candidateId=*/1, nowNs);   // denied (gate)
    ASSERT_EQ(pub.placeVoteCalls.size(), 1u);
    ASSERT_FALSE(pub.placeVoteCalls[0].vote);
    EXPECT_EQ(e.candidateTermId(), 5) << "(Aeron: a denial still raises candidateTermId)";
    EXPECT_EQ(e.ackTermId(), before) << "a DENIED vote raised the internal-ISR ack term";
    e.onRequestVote(0, 0, /*candidateTermId=*/6, /*candidateId=*/2, nowNs);   // granted
    ASSERT_EQ(pub.placeVoteCalls.size(), 2u);
    ASSERT_TRUE(pub.placeVoteCalls[1].vote);
    EXPECT_EQ(e.ackTermId(), 6) << "a GRANTED vote must raise the ack term";
}

// Re-review 2026-10-04 (fix round 3) CRITICAL-R3: the candidate's OWN vote is
// gated by its own durable knowledge.  A node that acked its own removal may
// still stand (MAJOR-A), but its self-vote does not count: one stale peer's
// grant is not a majority.  (Before: becomeCandidate recorded the self-vote
// unconditionally and the excluded node won with one stale grant.)
TEST(ElectionBallotRegression, SelfVoteGatedByOwnKnowledge_OneStaleGrantIsNotAMajority) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    GateCtx g{/*denied=*/0};                     // self's own knowledge excludes self
    e.setVoteGate(&denyOne, &g);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);                             // INIT → CANVASS
    e.onCanvassPosition(0, 0, 0, 1);
    e.onCanvassPosition(0, 0, 0, 2);
    for (int i = 0; i < 20 && e.state() != ElectionState::CandidateBallot; ++i) {
        nowNs += kElectionTimeoutNs;
        e.doWork(nowNs);
    }
    ASSERT_EQ(e.state(), ElectionState::CandidateBallot) << "MAJOR-A: the node must still stand";
    const Term term = e.candidateTermId();
    e.onVote(/*logLeadershipTermId=*/0, /*logPosition=*/0, term, /*candidateId=*/0, /*voterId=*/1, /*vote=*/true);   // one stale grant
    for (int i = 0; i < 3; ++i) { nowNs += kElectionTimeoutNs; e.doWork(nowNs); }
    EXPECT_NE(e.state(), ElectionState::LeaderLogReplication)
        << "won with its own ungated self-vote + one stale grant";
    EXPECT_NE(e.state(), ElectionState::LeaderReplay);
    EXPECT_NE(e.leaderMemberId(), 0);
}

// CRITICAL-R3 liveness: the knowledge is exchanged while canvassing / voting —
// every CanvassPosition, RequestVote and Vote is accompanied by the sender's
// newest durable metadata-ISR state, and a received one is handed to the
// learn hook (which adopts it when newer, bypassing the ack term rule).
namespace {
struct ExchangeCtx {
    CandidacyState mine{};
    bool           have = true;
    int            learned = 0;
    CandidacyState last{};
};
bool exSrc(void* c, CandidacyState& out) noexcept {
    auto* x = static_cast<ExchangeCtx*>(c);
    if (!x->have) return false;
    out = x->mine;
    return true;
}
void exLearn(void* c, const CandidacyState& s) noexcept {
    auto* x = static_cast<ExchangeCtx*>(c);
    ++x->learned;
    x->last = s;
}
} // namespace

TEST(ElectionBallotRegression, CandidacyStateExchangedWhileCanvassingAndVoting) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    ExchangeCtx x;
    x.mine.leaderEpoch = 1; x.mine.version = 4; x.mine.count = 2; x.mine.members[0] = 1; x.mine.members[1] = 3;
    e.setCandidacyExchange(&exSrc, &exLearn, &x);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);                             // INIT → CANVASS (publishes canvass)
    nowNs += kStatusIntervalNs;
    e.doWork(nowNs);
    ASSERT_FALSE(pub.canvassCalls.empty());
    bool sentTo1 = false, sentTo2 = false;
    for (const auto& [to, st] : pub.candidacyCalls) {
        EXPECT_EQ(st.version, 4);
        sentTo1 = sentTo1 || to == 1;
        sentTo2 = sentTo2 || to == 2;
    }
    EXPECT_TRUE(sentTo1 && sentTo2) << "the newest metadata-ISR state did not accompany the canvass";
    // A vote is accompanied too.
    pub.clear(); pub.candidacyCalls.clear();
    e.onRequestVote(0, 0, /*candidateTermId=*/3, /*candidateId=*/1, nowNs);
    ASSERT_EQ(pub.placeVoteCalls.size(), 1u);
    bool withVote = false;
    for (const auto& c : pub.candidacyCalls) withVote = withVote || c.first == 1;
    EXPECT_TRUE(withVote) << "the newest metadata-ISR state did not accompany the vote";
    // Received: handed to the learn hook.
    CandidacyState peer;
    peer.leaderEpoch = 1; peer.version = 6; peer.count = 1; peer.members[0] = 2;
    e.onCandidacyState(2, peer);
    EXPECT_EQ(x.learned, 1);
    EXPECT_EQ(x.last.version, 6);
}
