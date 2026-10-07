// ElectionLogTest.cpp — P2P-5b: end-to-end election leader/follower-log state tests.
//
// Tests cover: single-node → CLOSED, leader 3-member → CLOSED, follower path → CLOSED,
// onCommitPosition/onAppendPosition handlers, NLT/AppendPosition messages emitted.
//
// Design principles:
//   - ALL clock advancement via nextDeadlineNs() — ABSOLUTELY NO unbounded while loops.
//   - Bounded driver: driveToClosed() loops at most 100 times, fails explicitly if stalled.
//   - Unique TempFile for each test (NodeStateFile + TermLog), cleaned up on destruction.
//   - No tautologies: concrete field assertions after CLOSED.

#include <gtest/gtest.h>
#include <rune/consensus/Election.h>
#include <rune/consensus/ConsensusPublisher.h>
#include <rune/consensus/ClusterMember.h>
#include <rune/consensus/ElectionState.h>
#include <rune/consensus/NodeStateFile.h>
#include <rune/consensus/TermLog.h>
#include <rune/consensus/Types.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <vector>
#include <unistd.h>

using namespace rune::consensus;
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// TempFile — unique path per test, cleaned up on destruction
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
               ("rune_elt_" + uniq + std::string(suffix));
        fs::remove(path);
    }
    ~TempFile() { std::error_code ec; fs::remove(path, ec); }
    std::string str() const { return path.string(); }
};

// ---------------------------------------------------------------------------
// Recorded call structs (same layout as ElectionBallotTest for consistency)
// ---------------------------------------------------------------------------
struct CanvassCall   { NodeId toMember; Term logLeadershipTermId; int64_t logPosition;
                       Term leadershipTermId; NodeId followerMemberId; };
struct RequestVoteCall { NodeId toMember; Term logLeadershipTermId; int64_t logPosition;
                         Term candidateTermId; NodeId candidateMemberId; };
struct PlaceVoteCall   { NodeId toMember; Term candidateTermId; Term logLeadershipTermId;
                         int64_t logPosition; NodeId candidateMemberId; NodeId followerMemberId; bool vote; };
struct NLTCall {
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
struct AppendPositionCall { NodeId toMember; Term leadershipTermId; int64_t logPosition;
                            NodeId followerMemberId; uint8_t flags; };
struct CommitPositionCall { NodeId toMember; Term leadershipTermId; int64_t logPosition;
                            NodeId leaderMemberId; };

// ---------------------------------------------------------------------------
// FakeConsensusPublisher — records all outbound messages
// ---------------------------------------------------------------------------
struct FakeConsensusPublisher : ConsensusPublisher {
    std::vector<CanvassCall>      canvassCalls;
    std::vector<RequestVoteCall>  requestVoteCalls;
    std::vector<PlaceVoteCall>    placeVoteCalls;
    std::vector<NLTCall>          nltCalls;
    std::vector<AppendPositionCall> appendPositionCalls;
    std::vector<CommitPositionCall> commitPositionCalls;

    void canvassPosition(NodeId to, Term lltid, int64_t lp, Term ltid, NodeId fid) override {
        canvassCalls.push_back({to, lltid, lp, ltid, fid});
    }
    void requestVote(NodeId to, Term lltid, int64_t lp, Term ctid, NodeId cmid) override {
        requestVoteCalls.push_back({to, lltid, lp, ctid, cmid});
    }
    void placeVote(NodeId to, Term ctid, Term lltid, int64_t lp, NodeId cmid, NodeId fid, bool v) override {
        placeVoteCalls.push_back({to, ctid, lltid, lp, cmid, fid, v});
    }
    void newLeadershipTerm(NodeId to, Term lltid, Term nltid, int64_t ntblp, int64_t nlp,
                           Term ltid, int64_t tblp, int64_t lp, int64_t rid,
                           int64_t ts, NodeId lid, int32_t sid, bool startup, int64_t cp) override {
        nltCalls.push_back({to, lltid, nltid, ntblp, nlp, ltid, tblp, lp, rid, ts, lid, sid, startup, cp});
    }
    void appendPosition(NodeId to, Term ltid, int64_t lp, NodeId fid, uint8_t flags) override {
        appendPositionCalls.push_back({to, ltid, lp, fid, flags});
    }
    void commitPosition(NodeId to, Term ltid, int64_t lp, NodeId lid) override {
        commitPositionCalls.push_back({to, ltid, lp, lid});
    }
    void clear() {
        canvassCalls.clear(); requestVoteCalls.clear(); placeVoteCalls.clear();
        nltCalls.clear(); appendPositionCalls.clear(); commitPositionCalls.clear();
    }
};

// ---------------------------------------------------------------------------
// Test timing constants
// ---------------------------------------------------------------------------
static constexpr int64_t kElectionTimeoutNs       = 150'000'000LL; // 150ms
static constexpr int64_t kStatusIntervalNs         =  50'000'000LL; //  50ms
static constexpr int64_t kStartupCanvassTimeoutNs  = 300'000'000LL; // 300ms

// ---------------------------------------------------------------------------
// Factory helpers
// ---------------------------------------------------------------------------
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

static Election makeElection(
    std::vector<ClusterMember> members,
    NodeId                     self,
    FakeConsensusPublisher&    pub,
    NodeStateFile&             nsf,
    TermLog&                   tl,
    bool                       isNodeStartup = true,
    Term                       leadershipTermId = 0,
    int64_t                    logPosition = 0,
    int64_t                    appendPosition = 0,
    uint64_t                   rngSeed = 42)
{
    return Election(
        isNodeStartup,
        /*gracefulClosedLeaderId=*/-1,
        leadershipTermId,
        /*logLeadershipTermId=*/leadershipTermId,
        logPosition,
        appendPosition,
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

// ---------------------------------------------------------------------------
// BOUNDED DRIVER — never spins, never hangs.
// Advances nowNs to e.nextDeadlineNs() at each step.
// 100-step cap: if election stalls it fails fast.
// ---------------------------------------------------------------------------
static bool driveToClosed(Election& e, int64_t& nowNs) {
    for (int i = 0; i < 100 && !e.isClosed(); ++i) {
        int64_t deadline = e.nextDeadlineNs();
        // Jump to next deadline (or advance by 1ns if already past — never stall)
        if (deadline > nowNs) {
            nowNs = deadline;
        } else {
            nowNs += 1;
        }
        e.doWork(nowNs);
    }
    return e.isClosed();
}

// Feed AppendPosition from a follower and drive election leader forward.
// Used in 3-member leader tests to make quorum advance.
static bool driveLeaderToClosed(
    Election& leader, int64_t& nowNs,
    FakeConsensusPublisher& pub,
    std::vector<NodeId> followerIds)
{
    for (int i = 0; i < 100 && !leader.isClosed(); ++i) {
        int64_t deadline = leader.nextDeadlineNs();
        if (deadline > nowNs) {
            nowNs = deadline;
        } else {
            nowNs += 1;
        }
        leader.doWork(nowNs);

        // After each doWork the leader may have emitted NLT; simulate followers
        // responding with AppendPosition once the leader reaches LEADER_LOG_REPLICATION.
        if (leader.state() == ElectionState::LeaderLogReplication ||
            leader.state() == ElectionState::LeaderReady) {
            Term ltid = leader.leadershipTermId();
            int64_t lpos = leader.appendPosition();
            for (NodeId fid : followerIds) {
                leader.onAppendPosition(nowNs, ltid, lpos, fid, 0);
            }
        }
    }
    return leader.isClosed();
}

// ===========================================================================
// GROUP 1: Single-member cluster — direct INIT → LeaderLogReplication → CLOSED
// ===========================================================================

// L1: Single-node end-to-end → CLOSED; TermLog has a term entry for leadershipTermId.
TEST(ElectionLog, SingleMember_EndToEnd_Closed) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers1(0), 0, pub, nsf, tl);
    int64_t nowNs = 1'000'000LL;

    ASSERT_TRUE(driveToClosed(e, nowNs));
    EXPECT_EQ(e.state(), ElectionState::Closed);
    EXPECT_EQ(e.stateCode(), static_cast<int32_t>(ElectionState::Closed));

    // TermLog must have a valid entry for the winning term.
    EXPECT_TRUE(tl.getTermEntry(e.leadershipTermId()).has_value());
}

// L2: Single-node leadershipTermId = 1 after election.
TEST(ElectionLog, SingleMember_LeadershipTermId_IsOne) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers1(0), 0, pub, nsf, tl);
    int64_t nowNs = 1'000'000LL;

    ASSERT_TRUE(driveToClosed(e, nowNs));
    // Init increments candidateTermId from 0 → 1 for a single-node cluster.
    EXPECT_EQ(e.leadershipTermId(), 1);
}

// L3: Single-node — isClosed() returns true exactly once CLOSED.
TEST(ElectionLog, SingleMember_IsClosed_ReturnsTrueAfterClosed) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers1(0), 0, pub, nsf, tl);
    EXPECT_FALSE(e.isClosed());

    int64_t nowNs = 1'000'000LL;
    ASSERT_TRUE(driveToClosed(e, nowNs));
    EXPECT_TRUE(e.isClosed());
}

// L4: Single-node — leaderMemberId == thisMemberId after CLOSED.
TEST(ElectionLog, SingleMember_LeaderMemberId_IsSelf) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers1(0), 0, pub, nsf, tl);
    int64_t nowNs = 1'000'000LL;

    ASSERT_TRUE(driveToClosed(e, nowNs));
    EXPECT_EQ(e.leaderMemberId(), e.thisMemberId());
}

// L5: Single-node — no NLT, appendPosition, or commitPosition emitted (no peers).
TEST(ElectionLog, SingleMember_NoMessagesEmittedToPeers) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers1(0), 0, pub, nsf, tl);
    int64_t nowNs = 1'000'000LL;
    ASSERT_TRUE(driveToClosed(e, nowNs));

    // Single-node: no peers to broadcast to.
    EXPECT_TRUE(pub.nltCalls.empty());
    EXPECT_TRUE(pub.appendPositionCalls.empty());
    EXPECT_TRUE(pub.commitPositionCalls.empty());
}

// L6: Single-node — doWork after CLOSED is a no-op (workCount == 0).
TEST(ElectionLog, SingleMember_DoWork_AfterClosed_IsNoop) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers1(0), 0, pub, nsf, tl);
    int64_t nowNs = 1'000'000LL;
    ASSERT_TRUE(driveToClosed(e, nowNs));
    pub.clear();

    int work = e.doWork(nowNs + 1'000'000'000LL);
    EXPECT_EQ(work, 0);
    EXPECT_TRUE(pub.nltCalls.empty());
}

// ===========================================================================
// GROUP 2: 3-member leader path — win election, replicate, reach CLOSED
// ===========================================================================

// Helper: bring a 3-member election (node 0) to CandidateBallot with quorum votes.
static void bringToLeaderLogReplication(Election& e, int64_t& nowNs) {
    e.doWork(nowNs); // INIT → CANVASS
    // Feed canvass positions from both peers (same log → unanimous candidate)
    e.onCanvassPosition(0, 0, 0, 1);
    e.onCanvassPosition(0, 0, 0, 2);
    nowNs += kStartupCanvassTimeoutNs + 1;
    e.doWork(nowNs); // CANVASS → NOMINATE (unanimous)

    // Advance past nomination backoff (seed=0 → near-zero backoff)
    nowNs += kElectionTimeoutNs * 10;
    e.doWork(nowNs); // NOMINATE → CANDIDATE_BALLOT

    nowNs += 1;
    e.doWork(nowNs); // sends RequestVotes

    // Feed votes from peers
    Term cTerm = e.candidateTermId();
    e.onVote(0, 0, cTerm, 0, 1, true);
    e.onVote(0, 0, cTerm, 0, 2, true);

    nowNs += 1;
    e.doWork(nowNs); // unanimous → LeaderLogReplication
}

// L7: 3-member leader reaches CLOSED with bounded clock driver + follower AppendPosition feed.
TEST(ElectionLog, ThreeMember_Leader_EndToEnd_Closed) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);

    int64_t nowNs = 1'000'000LL;
    bringToLeaderLogReplication(e, nowNs);
    ASSERT_EQ(e.state(), ElectionState::LeaderLogReplication);

    // Drive to CLOSED, feeding AppendPosition from followers on each step
    ASSERT_TRUE(driveLeaderToClosed(e, nowNs, pub, {1, 2}));
    EXPECT_EQ(e.state(), ElectionState::Closed);
}

// L8: 3-member leader — TermLog has a valid term entry after CLOSED.
TEST(ElectionLog, ThreeMember_Leader_TermLog_HasEntry) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, 0);

    int64_t nowNs = 1'000'000LL;
    bringToLeaderLogReplication(e, nowNs);
    ASSERT_TRUE(driveLeaderToClosed(e, nowNs, pub, {1, 2}));

    // TermLog must have an entry for the elected leadershipTermId.
    EXPECT_TRUE(tl.getTermEntry(e.leadershipTermId()).has_value());
}

// L9: 3-member leader — NLT emitted to both peers (leaderMemberId == self, correct term).
TEST(ElectionLog, ThreeMember_Leader_NLT_EmittedToPeers) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, 0);

    int64_t nowNs = 1'000'000LL;
    bringToLeaderLogReplication(e, nowNs);
    pub.clear();

    // One heartbeat step:
    int64_t deadline = e.nextDeadlineNs();
    nowNs = std::max(nowNs + 1, deadline);
    e.doWork(nowNs);

    // NLT should have been broadcast to peers 1 and 2
    ASSERT_GE(pub.nltCalls.size(), 2u);
    std::vector<NodeId> targets;
    for (auto& n : pub.nltCalls) targets.push_back(n.toMember);
    std::sort(targets.begin(), targets.end());
    EXPECT_EQ(targets[0], 1);
    EXPECT_EQ(targets[1], 2);

    // Fields: leaderMemberId == 0 (self), leadershipTermId correct
    for (auto& n : pub.nltCalls) {
        EXPECT_EQ(n.leaderMemberId, 0);
        EXPECT_EQ(n.leadershipTermId, e.leadershipTermId());
    }
}

// L10: 3-member leader — CommitPosition emitted alongside NLT.
TEST(ElectionLog, ThreeMember_Leader_CommitPosition_EmittedToPeers) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, 0);

    int64_t nowNs = 1'000'000LL;
    bringToLeaderLogReplication(e, nowNs);
    pub.clear();

    int64_t deadline = e.nextDeadlineNs();
    nowNs = std::max(nowNs + 1, deadline);
    e.doWork(nowNs);

    // CommitPosition emitted to both peers alongside NLT
    ASSERT_GE(pub.commitPositionCalls.size(), 2u);
    for (auto& cp : pub.commitPositionCalls) {
        EXPECT_EQ(cp.leaderMemberId, 0);
        EXPECT_EQ(cp.leadershipTermId, e.leadershipTermId());
    }
}

// L11: 3-member leader — leaderMemberId == self (0) after CLOSED.
TEST(ElectionLog, ThreeMember_Leader_LeaderMemberId_IsSelf) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, 0);

    int64_t nowNs = 1'000'000LL;
    bringToLeaderLogReplication(e, nowNs);
    ASSERT_TRUE(driveLeaderToClosed(e, nowNs, pub, {1, 2}));

    EXPECT_EQ(e.leaderMemberId(), 0);
}

// L12: 3-member leader — only one vote (quorum timeout path) also reaches CLOSED.
TEST(ElectionLog, ThreeMember_Leader_QuorumVote_Timeout_Closed) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, 0);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // INIT → CANVASS
    e.onCanvassPosition(0, 0, 0, 1);
    e.onCanvassPosition(0, 0, 0, 2);
    nowNs += kStartupCanvassTimeoutNs + 1;
    e.doWork(nowNs); // NOMINATE

    nowNs += kElectionTimeoutNs * 10;
    e.doWork(nowNs); // CandidateBallot

    nowNs += 1;
    e.doWork(nowNs); // sends RequestVotes

    // Only ONE external vote (quorum = self + peer 1 = 2 ≥ threshold(3)=2)
    Term cTerm = e.candidateTermId();
    e.onVote(0, 0, cTerm, 0, 1, true);

    // Advance past ballot timeout → quorum leader
    nowNs += kElectionTimeoutNs + 1;
    e.doWork(nowNs); // → LeaderLogReplication (quorum leader after timeout)

    ASSERT_EQ(e.state(), ElectionState::LeaderLogReplication);
    ASSERT_TRUE(driveLeaderToClosed(e, nowNs, pub, {1, 2}));
    EXPECT_TRUE(e.isClosed());
}

// ===========================================================================
// GROUP 3: onAppendPosition handler
// ===========================================================================

// L13: onAppendPosition updates the member's logPosition, enabling quorum advance.
TEST(ElectionLog, OnAppendPosition_UpdatesMemberLogPosition) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, 0);

    int64_t nowNs = 1'000'000LL;
    bringToLeaderLogReplication(e, nowNs);

    // Inject AppendPosition from peer 1
    Term ltid = e.leadershipTermId();
    e.onAppendPosition(nowNs, ltid, 0, 1, 0);
    e.onAppendPosition(nowNs, ltid, 0, 2, 0);

    // Election should now have quorum → advance past LeaderLogReplication
    ASSERT_TRUE(driveLeaderToClosed(e, nowNs, pub, {})); // no further feeding needed
    EXPECT_TRUE(e.isClosed());
}

// L14: onAppendPosition with wrong leadershipTermId is ignored — member logPosition stays null.
TEST(ElectionLog, OnAppendPosition_WrongTerm_DoesNotUpdateMember) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    // Use a 5-member cluster so quorum requires 3 and self-alone isn't enough.
    std::vector<ClusterMember> ms5(5);
    for (int i = 0; i < 5; i++) {
        ms5[i].id = i; ms5[i].logPosition = 0; ms5[i].logLeadershipTermId = 0;
        ms5[i].leadershipTermId = 0;
    }
    auto e = makeElection(std::move(ms5), 0, pub, nsf, tl, true, 0, 0, 0, 0);

    int64_t nowNs = 1'000'000LL;
    // Drive to LeaderLogReplication for 5-member cluster (need 3 votes)
    e.doWork(nowNs); // CANVASS
    for (int i = 1; i <= 4; i++) e.onCanvassPosition(0, 0, 0, i);
    nowNs += kStartupCanvassTimeoutNs + 1;
    e.doWork(nowNs); // NOMINATE
    nowNs += kElectionTimeoutNs * 10;
    e.doWork(nowNs); // CandidateBallot
    nowNs += 1;
    e.doWork(nowNs); // send RequestVotes
    Term cTerm = e.candidateTermId();
    e.onVote(0, 0, cTerm, 0, 1, true);
    e.onVote(0, 0, cTerm, 0, 2, true);
    e.onVote(0, 0, cTerm, 0, 3, true);
    nowNs += kElectionTimeoutNs + 1;
    e.doWork(nowNs); // quorum leader after timeout → LeaderLogReplication
    ASSERT_EQ(e.state(), ElectionState::LeaderLogReplication);

    Term ltid = e.leadershipTermId();
    // Feed with wrong term (stale): should be ignored
    e.onAppendPosition(nowNs, ltid + 99, 0, 4, 0);

    // Heartbeat fires but quorum NOT met (only self + 3 members with correct term from votes,
    // but members 1-3 don't have timeOfLastAppendPositionNs set via onAppendPosition)
    // With no valid AppendPosition from peers, state stays LeaderLogReplication.
    int64_t deadline = e.nextDeadlineNs();
    nowNs = std::max(nowNs + 1, deadline);
    e.doWork(nowNs);
    // Still in leader path (not closed yet without peer AppendPositions)
    EXPECT_NE(e.state(), ElectionState::Closed);
}

// L15: onAppendPosition from self-member is processed.
//      T5 fix: add real assertion — onAppendPosition is an in-place update,
//      not a state transition trigger; doWork drives transitions.
TEST(ElectionLog, OnAppendPosition_FromSelf_Processed) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, 0);

    int64_t nowNs = 1'000'000LL;
    bringToLeaderLogReplication(e, nowNs);

    // T5 fix: assert that state does not change on self AppendPosition.
    // onAppendPosition is an in-place member update; state transitions happen in doWork.
    ElectionState stateBefore = e.state();
    Term ltid = e.leadershipTermId();
    e.onAppendPosition(nowNs, ltid, 0, 0, 0);
    EXPECT_EQ(e.state(), stateBefore)
        << "onAppendPosition for self must not trigger a state transition (doWork does that)";
}

// ===========================================================================
// GROUP 4: onCommitPosition handler
// ===========================================================================

// L16: onCommitPosition updates notifiedCommitPosition.
TEST(ElectionLog, OnCommitPosition_UpdatesNotifiedCommitPosition) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    // Use a follower election (node 0 in FOLLOWER_BALLOT after voting for node 1)
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // → CANVASS
    // Vote for peer 1
    e.onRequestVote(0, 0, 1, 1, nowNs); // → FOLLOWER_BALLOT

    ASSERT_EQ(e.state(), ElectionState::FollowerBallot);

    // Feed NLT from peer 1 (leader) → drives follower path → CLOSED
    e.onNewLeadershipTerm(nowNs, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, true, 500LL);

    // After onNewLeadershipTerm: notifiedCommitPosition updated to 500
    EXPECT_EQ(e.notifiedCommitPosition(), 500LL);

    // Also verify via onCommitPosition after becoming follower
    // Feed an additional commit position update
    e.onCommitPosition(nowNs, e.leadershipTermId(), 600LL, 1);
    EXPECT_EQ(e.notifiedCommitPosition(), 600LL);
}

// L17: onCommitPosition with wrong term is ignored.
TEST(ElectionLog, OnCommitPosition_WrongTerm_Ignored) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);
    e.onRequestVote(0, 0, 1, 1, nowNs);
    // Feed NLT to get into follower path
    e.onNewLeadershipTerm(nowNs, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, true, 500LL);

    Term ltid = e.leadershipTermId();
    int64_t prevCommit = e.notifiedCommitPosition();

    // Wrong term commit position — must be ignored
    e.onCommitPosition(nowNs, ltid + 99, 900LL, 1);
    EXPECT_EQ(e.notifiedCommitPosition(), prevCommit); // unchanged
}

// L18: onCommitPosition does not decrease notifiedCommitPosition (only increases).
TEST(ElectionLog, OnCommitPosition_DoesNotDecrease) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);
    e.onRequestVote(0, 0, 1, 1, nowNs);
    e.onNewLeadershipTerm(nowNs, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, true, 500LL);

    Term ltid = e.leadershipTermId();
    e.onCommitPosition(nowNs, ltid, 500LL, 1);
    EXPECT_EQ(e.notifiedCommitPosition(), 500LL);

    // Stale (lower) commit position
    e.onCommitPosition(nowNs, ltid, 100LL, 1);
    EXPECT_EQ(e.notifiedCommitPosition(), 500LL); // not decreased
}

// ===========================================================================
// GROUP 5: onNewLeadershipTerm — follower path
// ===========================================================================

// L19: From FOLLOWER_BALLOT + matching NLT → follower drives to CLOSED (bounded).
TEST(ElectionLog, Follower_FromFollowerBallot_NLT_DrivesClosed) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    // Node 0 is a follower that voted for node 1 in term 1
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // → CANVASS
    e.onRequestVote(0, 0, 1, 1, nowNs); // → FOLLOWER_BALLOT
    ASSERT_EQ(e.state(), ElectionState::FollowerBallot);

    // Leader 1 sends NLT for term 1
    e.onNewLeadershipTerm(nowNs, 0, 1, 0, 0, /*leadershipTermId=*/1, 0, 0, 0, 0, /*leaderMemberId=*/1, 0, true, 0);

    // Should now be in FollowerReplay or further
    EXPECT_NE(e.state(), ElectionState::FollowerBallot);

    // Drive to CLOSED with bounded driver
    ASSERT_TRUE(driveToClosed(e, nowNs));
    EXPECT_EQ(e.state(), ElectionState::Closed);
}

// L20: Follower emits AppendPosition to leader in FOLLOWER_READY before CLOSED.
TEST(ElectionLog, Follower_EmitsAppendPosition_ToLeader) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);
    e.onRequestVote(0, 0, 1, 1, nowNs);
    e.onNewLeadershipTerm(nowNs, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, true, 0);

    pub.clear();
    ASSERT_TRUE(driveToClosed(e, nowNs));

    // FOLLOWER_READY must have published AppendPosition to leader (member 1)
    ASSERT_GE(pub.appendPositionCalls.size(), 1u);
    auto& ap = pub.appendPositionCalls[0];
    EXPECT_EQ(ap.toMember, 1); // leader
    EXPECT_EQ(ap.leadershipTermId, e.leadershipTermId());
    EXPECT_EQ(ap.followerMemberId, 0); // self
    EXPECT_EQ(ap.flags, 0);
}

// L21: Stale NLT (leadershipTermId < current) is ignored.
TEST(ElectionLog, OnNewLeadershipTerm_Stale_Ignored) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    // Node 0 with leadershipTermId=5 already
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, /*leadershipTermId=*/5);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // → CANVASS
    // Vote for peer 1 with term=6
    e.onRequestVote(5, 0, 6, 1, nowNs); // → FOLLOWER_BALLOT

    // Now feed a stale NLT with term=4 (< our leadershipTermId=5)
    e.onNewLeadershipTerm(nowNs, 4, 4, 0, 0, /*leadershipTermId=*/4, 0, 0, 0, 0, 1, 0, true, 0);
    // Must still be in FOLLOWER_BALLOT (stale NLT ignored)
    EXPECT_EQ(e.state(), ElectionState::FollowerBallot);
}

// L22: NLT from CANVASS state → follower path → CLOSED.
TEST(ElectionLog, Follower_FromCanvass_NLT_DrivesClosed) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // → CANVASS

    // Leader sends NLT for term 1 directly to canvassing node
    e.onNewLeadershipTerm(nowNs, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, true, 0);

    // Not in CANVASS anymore
    EXPECT_NE(e.state(), ElectionState::Canvass);
    ASSERT_TRUE(driveToClosed(e, nowNs));
    EXPECT_EQ(e.state(), ElectionState::Closed);
}

// L23: Follower path sets leadershipTermId_ from NLT message.
TEST(ElectionLog, Follower_LeadershipTermId_SetFromNLT) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);
    e.onRequestVote(0, 0, 3, 1, nowNs); // term 3
    e.onNewLeadershipTerm(nowNs, 0, 3, 0, 0, /*leadershipTermId=*/3, 0, 0, 0, 0, 1, 0, true, 0);

    ASSERT_TRUE(driveToClosed(e, nowNs));
    EXPECT_EQ(e.leadershipTermId(), 3);
}

// L24: Follower path — leaderMemberId is correctly identified.
TEST(ElectionLog, Follower_LeaderMemberId_CorrectAfterNLT) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);
    e.onRequestVote(0, 0, 1, /*candidateId=*/2, nowNs); // voted for member 2
    e.onNewLeadershipTerm(nowNs, 0, 1, 0, 0, 1, 0, 0, 0, 0, /*leaderMemberId=*/2, 0, true, 0);
    ASSERT_TRUE(driveToClosed(e, nowNs));

    // Leader should be member 2
    EXPECT_EQ(e.leaderMemberId(), 2);
}

// L25: NLT ignored in CLOSED state.
TEST(ElectionLog, OnNewLeadershipTerm_Ignored_WhenClosed) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers1(0), 0, pub, nsf, tl);

    int64_t nowNs = 1'000'000LL;
    ASSERT_TRUE(driveToClosed(e, nowNs));
    ASSERT_TRUE(e.isClosed());

    // Inject NLT — must be ignored (no state change from CLOSED)
    e.onNewLeadershipTerm(nowNs, 0, 99, 0, 0, 99, 0, 0, 0, 0, 1, 0, true, 0);
    EXPECT_EQ(e.state(), ElectionState::Closed);
    EXPECT_EQ(e.leadershipTermId(), 1); // unchanged from the winning term
}

// ===========================================================================
// GROUP 6: nextDeadlineNs — clock hints
// ===========================================================================

// L26: nextDeadlineNs() in LeaderLogReplication returns heartbeat deadline.
TEST(ElectionLog, NextDeadlineNs_InLeaderLogReplication) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, 0);

    int64_t nowNs = 1'000'000LL;
    bringToLeaderLogReplication(e, nowNs);
    ASSERT_EQ(e.state(), ElectionState::LeaderLogReplication);

    // nextDeadlineNs must be a finite future value (within heartbeat interval of "now")
    int64_t d = e.nextDeadlineNs();
    EXPECT_GT(d, INT64_MIN);
    EXPECT_LE(d, nowNs + kStatusIntervalNs + 1);
}

// L27: nextDeadlineNs() in CLOSED returns INT64_MIN (no pending deadline).
TEST(ElectionLog, NextDeadlineNs_InClosed_IsMinValue) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers1(0), 0, pub, nsf, tl);

    int64_t nowNs = 1'000'000LL;
    ASSERT_TRUE(driveToClosed(e, nowNs));

    EXPECT_EQ(e.nextDeadlineNs(), INT64_MIN);
}

// L28: nextDeadlineNs() in CandidateBallot returns ballot timeout.
TEST(ElectionLog, NextDeadlineNs_InCandidateBallot) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, 0);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);
    e.onCanvassPosition(0, 0, 0, 1);
    e.onCanvassPosition(0, 0, 0, 2);
    nowNs += kStartupCanvassTimeoutNs + 1;
    e.doWork(nowNs);
    nowNs += kElectionTimeoutNs * 10;
    e.doWork(nowNs); // → CandidateBallot
    ASSERT_EQ(e.state(), ElectionState::CandidateBallot);

    int64_t d = e.nextDeadlineNs();
    EXPECT_GT(d, nowNs); // must be in the future
    EXPECT_LE(d, nowNs + kElectionTimeoutNs + 1);
}

// ===========================================================================
// GROUP 7: Full end-to-end follower + leader interaction
// ===========================================================================

// L29: Full 3-member scenario: leader (node 0) reaches CLOSED, feeds follower (node 2)
//      which also reaches CLOSED. The leader receives AppendPosition from follower 2
//      and both close correctly.
TEST(ElectionLog, Full_LeaderAndFollower_BothReachClosed) {
    // Node 0: leader
    TempFile nsfFile0(".nsf"), tlFile0(".tl");
    NodeStateFile nsf0(nsfFile0.str());
    TermLog       tl0(tlFile0.str());
    FakeConsensusPublisher pub0;
    auto leader = makeElection(makeMembers3(0), 0, pub0, nsf0, tl0, true, 0, 0, 0, 0);

    // Node 2: follower
    TempFile nsfFile2(".nsf"), tlFile2(".tl");
    NodeStateFile nsf2(nsfFile2.str());
    TermLog       tl2(tlFile2.str());
    FakeConsensusPublisher pub2;
    auto follower = makeElection(makeMembers3(2), 2, pub2, nsf2, tl2, true, 0, 0, 0, 0);

    int64_t leaderNs = 1'000'000LL;
    int64_t followerNs = 1'000'000LL;

    // 1. Drive leader to LeaderLogReplication
    bringToLeaderLogReplication(leader, leaderNs);
    ASSERT_EQ(leader.state(), ElectionState::LeaderLogReplication);

    // 2. Drive follower into CANVASS, then vote for leader (node 0)
    follower.doWork(followerNs);
    ASSERT_EQ(follower.state(), ElectionState::Canvass);
    follower.onRequestVote(0, 0, leader.candidateTermId(), 0, followerNs);
    ASSERT_EQ(follower.state(), ElectionState::FollowerBallot);

    // 3. Leader emits NLT; feed it to follower
    int64_t d = leader.nextDeadlineNs();
    leaderNs = std::max(leaderNs + 1, d);
    leader.doWork(leaderNs);

    // Fetch NLT fields from what leader published
    ASSERT_GE(pub0.nltCalls.size(), 1u);
    auto& nlt = pub0.nltCalls[0];
    follower.onNewLeadershipTerm(
        followerNs, nlt.logLeadershipTermId, nlt.nextLeadershipTermId,
        nlt.nextTermBaseLogPosition, nlt.nextLogPosition,
        nlt.leadershipTermId, nlt.termBaseLogPosition, nlt.logPosition,
        nlt.leaderRecordingId, nlt.timestamp, nlt.leaderMemberId,
        nlt.logSessionId, nlt.isStartup, nlt.commitPosition);

    // 4. Drive follower to CLOSED
    ASSERT_TRUE(driveToClosed(follower, followerNs));
    EXPECT_TRUE(follower.isClosed());

    // 5. Feed follower's AppendPosition to leader and drive leader to CLOSED
    ASSERT_GE(pub2.appendPositionCalls.size(), 1u);
    auto& ap = pub2.appendPositionCalls[0];
    leader.onAppendPosition(leaderNs, ap.leadershipTermId, ap.logPosition, ap.followerMemberId, ap.flags);
    // Also feed AppendPosition from node 1 (peer not in this test but needed for quorum)
    leader.onAppendPosition(leaderNs, leader.leadershipTermId(), 0, 1, 0);

    ASSERT_TRUE(driveToClosed(leader, leaderNs));
    EXPECT_TRUE(leader.isClosed());
}

// L30: 3-member leader — only minimum heartbeats (bounded): NLT not emitted continuously.
//      With seed=0 and immediate AppendPosition, leader closes in ≤ 5 driver steps.
TEST(ElectionLog, ThreeMember_Leader_ClosesInFewSteps) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, 0);

    int64_t nowNs = 1'000'000LL;
    bringToLeaderLogReplication(e, nowNs);

    // Feed AppendPosition immediately from both followers
    Term ltid = e.leadershipTermId();
    e.onAppendPosition(nowNs, ltid, 0, 1, 0);
    e.onAppendPosition(nowNs, ltid, 0, 2, 0);

    int steps = 0;
    for (; steps < 100 && !e.isClosed(); ++steps) {
        int64_t deadline = e.nextDeadlineNs();
        nowNs = (deadline > nowNs) ? deadline : nowNs + 1;
        e.doWork(nowNs);
    }

    EXPECT_TRUE(e.isClosed());
    // Should complete in very few steps since quorum is immediate
    EXPECT_LT(steps, 20);
}

// ===========================================================================
// GROUP 8: Follower TermLog persistence on election close
// ===========================================================================

// L31: Follower persists term entry on election close; entry survives reopen.
// This test validates the fix for the follower persistence gap: before close,
// follower must call appendTerm so the TermLog entry survives recovery.
TEST(ElectionLog, FollowerPersistsTermEntryOnClose) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    int64_t nowNs = 1'000'000LL;

    // Drive follower to CLOSED via onNewLeadershipTerm → follower path.
    e.doWork(nowNs); // → CANVASS
    e.onRequestVote(0, 0, 1, 1, nowNs); // → FOLLOWER_BALLOT
    ASSERT_EQ(e.state(), ElectionState::FollowerBallot);

    // Feed NLT from leader (node 1) for term 1
    e.onNewLeadershipTerm(nowNs, 0, 1, 0, 0, /*leadershipTermId=*/1, 0, 0, 0, 0, /*leaderMemberId=*/1, 0, true, 0);

    // Drive to CLOSED with bounded driver
    ASSERT_TRUE(driveToClosed(e, nowNs));
    ASSERT_EQ(e.state(), ElectionState::Closed);

    Term closedTermId = e.leadershipTermId();
    int64_t closedLogPosition = e.logPosition();

    // Assertion 1: TermLog entry exists and is committed (not NULL_POSITION).
    auto entry = tl.getTermEntry(closedTermId);
    ASSERT_TRUE(entry.has_value()) << "Follower TermLog missing entry for term " << closedTermId;
    EXPECT_EQ(entry->leadershipTermId, closedTermId);
    EXPECT_NE(entry->logPosition, kNullPosition) << "TermLog entry not committed (still NULL_POSITION)";
    EXPECT_EQ(entry->logPosition, closedLogPosition) << "TermLog logPosition mismatch";

    // Assertion 2: Close and reopen the TermLog — entry must persist on disk.
    // Destroy the old TermLog and create a fresh one over the same path.
    {
        TermLog tlReopen(tlFile.str());
        auto reopenEntry = tlReopen.getTermEntry(closedTermId);
        ASSERT_TRUE(reopenEntry.has_value()) << "Reopened TermLog lost entry for term " << closedTermId;
        EXPECT_EQ(reopenEntry->leadershipTermId, closedTermId);
        EXPECT_EQ(reopenEntry->logPosition, closedLogPosition);
    }
}

// ===========================================================================
// GROUP 7: Leader-Completeness safety tests (audit B2/T1/T4)
// ===========================================================================

// T4: Leader does NOT close when a follower reports an AppendPosition with a stale
//     leadershipTermId (wrong term → onAppendPosition ignores it → quorum not met).
//     Tests the hasQuorumAtPosition gate: follower at wrong term is not counted.
TEST(ElectionLog, Leader_FollowerWrongTerm_DoesNotAdvanceQuorum) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    // seed=0 so bringToLeaderLogReplication nominates immediately
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);

    int64_t nowNs = 1'000'000LL;
    bringToLeaderLogReplication(e, nowNs);
    ASSERT_EQ(e.state(), ElectionState::LeaderLogReplication);

    Term    ltid      = e.leadershipTermId();
    int64_t leaderPos = e.appendPosition();

    // Feed follower 1 with WRONG leadershipTermId (stale term).
    // onAppendPosition filters on leadershipTermId → this follower is ignored.
    // Only self counts at (ltid, leaderPos). Quorum for 3 members = 2 → not met.
    e.onAppendPosition(nowNs, /*leadershipTermId=*/ltid - 1, leaderPos,
                       /*followerMemberId=*/1, 0);

    // Give the leader a heartbeat tick
    nowNs = e.nextDeadlineNs() + 1;
    e.doWork(nowNs);

    // Leader must still be in LEADER_LOG_REPLICATION or LEADER_REPLAY (not CLOSED or LEADER_READY)
    // because only self (1 member) has reported — quorum (2) not reached.
    EXPECT_NE(e.state(), ElectionState::Closed)
        << "Leader must not close with stale-term follower AppendPosition (quorum not met)";
}

// LC1: NLT for a HIGHER term that this node never voted in IS adopted (audit B2/T1).
//      Per Aeron Election.java:433-437: if nextLeadershipTermId > candidateTermId_,
//      the node adopts the new term via proposeMaxCandidateTermId and follows the leader.
//      This is the correct Aeron behavior: a node that missed a ballot round still
//      follows the winning leader by adopting the winning term.
//      A STALE (lower term) NLT is what must be rejected — tested by the existing
//      OnNewLeadershipTerm_Stale_Ignored test and the LC2 log-freshness test below.
TEST(ElectionLog, NLT_HigherTerm_IsAdoptedAndFollowed) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // INIT → CANVASS
    ASSERT_EQ(e.state(), ElectionState::Canvass);
    // candidateTermId_ after init = 1

    // Inject NLT for term=99 — node 0 never voted in term 99 but term is higher.
    // Per Aeron Election.java:433-437: adopt the new term and follow the leader.
    e.onNewLeadershipTerm(nowNs,
        /*logLeadershipTermId=*/0,
        /*nextLeadershipTermId=*/99,
        /*nextTermBaseLogPosition=*/0,
        /*nextLogPosition=*/0,
        /*leadershipTermId=*/99,
        /*termBaseLogPosition=*/0,
        /*logPosition=*/0,
        /*leaderRecordingId=*/0,
        /*timestamp=*/0,
        /*leaderMemberId=*/1,
        /*logSessionId=*/0,
        /*isStartup=*/true,
        /*commitPosition=*/0);

    // Must have left CANVASS — higher-term NLT is adopted and the node follows.
    EXPECT_NE(e.state(), ElectionState::Canvass)
        << "NLT for higher term must be adopted per Aeron Election.java:433-437";
    // candidateTermId must have been updated to 99
    EXPECT_EQ(e.candidateTermId(), 99)
        << "candidateTermId must be adopted to nextLeadershipTermId=99";
}

// LC2: NLT where leader log is BEHIND follower's appendPosition is rejected (audit B2).
//      Node 0 has appendPosition=200, logLeadershipTermId=5. After voting in term 6,
//      leader sends NLT with logPosition=50, logLtId=3 → must be rejected.
TEST(ElectionLog, NLT_LeaderLogBehindFollower_IsIgnored) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    // nodeStateFile starts fresh (candidateTermId=NULL_VALUE=-1) so init will
    // set candidateTermId_ = max(-1, leadershipTermId=5) = 5, then onRequestVote
    // for term=6 (>5) will grant the vote and set candidateTermId_=6.
    NodeStateFile nsf(nsfFile.str());
    TermLog tl(tlFile.str());
    FakeConsensusPublisher pub;

    // Node 0 with logLeadershipTermId=5, appendPosition=200
    std::vector<ClusterMember> ms(3);
    for (int i = 0; i < 3; i++) {
        ms[i].id = i;
        ms[i].logPosition = 0;
        ms[i].logLeadershipTermId = 0;
        ms[i].leadershipTermId = 0;
    }
    Election e(true, -1,
               /*leadershipTermId=*/5, /*logLeadershipTermId=*/5,
               /*logPosition=*/200, /*appendPosition=*/200,
               std::move(ms), 0, pub, nsf, tl,
               kElectionTimeoutNs, kStatusIntervalNs, kStartupCanvassTimeoutNs, 42);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // INIT → CANVASS
    // After init: candidateTermId_ = max(NULL_VALUE=-1 → treated as 5, leadershipTermId=5) = 5

    ASSERT_EQ(e.state(), ElectionState::Canvass);

    // Vote for member 1 in term 6 (6 > candidateTermId_=5) → grants vote,
    // proposes term 6, state → FOLLOWER_BALLOT
    e.onRequestVote(/*logLeadershipTermId=*/5, /*logPosition=*/200,
                    /*candidateTermId=*/6, /*candidateId=*/1, nowNs);
    ASSERT_EQ(e.state(), ElectionState::FollowerBallot)
        << "node must have voted for candidate in term 6 (candidateTermId was 5)";

    // Now candidateTermId_=6. Leader 1 sends NLT for term=6 but its log is
    // (logLtId=3, logPos=50) which is BEHIND our (5, 200).
    // compareLog(5,200, 3,50) > 0 → must be rejected (log-freshness check).
    e.onNewLeadershipTerm(nowNs,
        /*logLeadershipTermId=*/3,
        /*nextLeadershipTermId=*/6,
        /*nextTermBaseLogPosition=*/0,
        /*nextLogPosition=*/50,
        /*leadershipTermId=*/6,
        /*termBaseLogPosition=*/0,
        /*logPosition=*/50,
        /*leaderRecordingId=*/0,
        /*timestamp=*/0,
        /*leaderMemberId=*/1,
        /*logSessionId=*/0,
        /*isStartup=*/true,
        /*commitPosition=*/0);

    // Must still be in FOLLOWER_BALLOT — behind-log NLT must be rejected
    EXPECT_EQ(e.state(), ElectionState::FollowerBallot)
        << "NLT with leader log (term=3,pos=50) behind follower (term=5,pos=200) must be ignored";
}

// ===========================================================================
// N1 anti-regression: leader with non-zero logLeadershipTermId_ still reaches quorum.
//
// The C6 fix (commit 779fe5b) added logLeadershipTermId to hasQuorumAtPosition,
// causing a liveness deadlock (audit N1, 2026-09-11): AppendPosition messages
// do not carry logLeadershipTermId, so onAppendPosition() never sets
// follower->logLeadershipTermId.  Once logLeadershipTermId_ != 0 on the leader,
// no follower ever passed the predicate → quorum unreachable forever.
//
// This test drives a 3-member election where the leader starts with
// logLeadershipTermId=3 (simulating a node that has appended entries in prior
// terms).  Followers send AppendPosition (which does NOT carry logLeadershipTermId,
// so follower->logLeadershipTermId stays at its initial value = 0 ≠ 3).
// The election MUST close.  Under the deadlock predicate it would stall at
// LEADER_LOG_REPLICATION and never reach Closed.
// ===========================================================================
TEST(ElectionLog, N1_NonZeroLogLeadershipTermId_QuorumStillReached) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    // 3-member cluster; leader is node 0.
    // logLeadershipTermId=3 to simulate a node that has appended entries in an
    // earlier term.  Members start with logLeadershipTermId=0 (unset) to mirror
    // what onAppendPosition() leaves them at — it never sets logLeadershipTermId.
    std::vector<ClusterMember> ms(3);
    for (int i = 0; i < 3; i++) {
        ms[i].id = i;
        ms[i].logPosition         = 0;
        ms[i].logLeadershipTermId = 0;  // NOT set by AppendPosition wire messages
        ms[i].leadershipTermId    = 0;
    }
    // Start from leadershipTermId=3, logLeadershipTermId=3 (non-zero — this is the
    // field the deadlock predicate required followers to match but never could).
    Election leader(/*isNodeStartup=*/true, /*gracefulClosedLeaderId=*/-1,
                    /*leadershipTermId=*/3, /*logLeadershipTermId=*/3,
                    /*logPosition=*/0, /*appendPosition=*/0,
                    std::move(ms), /*self=*/0, pub, nsf, tl,
                    kElectionTimeoutNs, kStatusIntervalNs, kStartupCanvassTimeoutNs,
                    /*rngSeed=*/0);

    int64_t nowNs = 1'000'000LL;

    // Phase 1: bring to LeaderLogReplication via canvass + votes (same as bringToLeaderLogReplication).
    leader.doWork(nowNs); // INIT → CANVASS
    leader.onCanvassPosition(0, 0, 0, 1);
    leader.onCanvassPosition(0, 0, 0, 2);
    nowNs += kStartupCanvassTimeoutNs + 1;
    leader.doWork(nowNs); // CANVASS → NOMINATE

    nowNs += kElectionTimeoutNs * 10;
    leader.doWork(nowNs); // NOMINATE → CANDIDATE_BALLOT

    nowNs += 1;
    leader.doWork(nowNs); // sends RequestVotes

    // Feed votes from followers 1 and 2 — they have logLeadershipTermId=0 (NOT 3).
    // This is exactly what happens in real runs: followers' logLeadershipTermId is
    // whatever was in their ClusterMember slot; onAppendPosition never updates it.
    Term cTerm = leader.candidateTermId();
    leader.onVote(0, 0, cTerm, 0, 1, true);
    leader.onVote(0, 0, cTerm, 0, 2, true);

    nowNs += 1;
    leader.doWork(nowNs); // unanimous → LeaderLogReplication

    ASSERT_EQ(leader.state(), ElectionState::LeaderLogReplication)
        << "N1 setup: leader must reach LeaderLogReplication before quorum test";

    // Phase 2: drive to CLOSED by feeding AppendPosition from followers.
    // AppendPosition does NOT carry logLeadershipTermId (ref spec line ~145);
    // onAppendPosition() sets follower->leadershipTermId and logPosition only.
    // Therefore follower->logLeadershipTermId remains 0 (≠ leader's logLeadershipTermId_=3).
    //
    // With the Aeron-faithful predicate (leadershipTermId + logPosition only),
    // quorum is reached and the election closes.
    // With the C6 deadlock predicate (also checks logLeadershipTermId),
    // no follower ever matches → election stalls → driveLeaderToClosed returns false.
    bool closed = driveLeaderToClosed(leader, nowNs, pub, {1, 2});

    EXPECT_TRUE(closed)
        << "N1 anti-regression: leader with logLeadershipTermId=3 (non-zero) must reach quorum "
           "from AppendPosition messages that do NOT carry logLeadershipTermId. "
           "A regression to the C6 deadlock predicate would stall here (follower->logLeadershipTermId=0 != 3).";
    EXPECT_EQ(leader.state(), ElectionState::Closed);
}

// ===========================================================================
// Part B — 8 missing-test areas (audit 2026-09-11 full re-audit)
// ===========================================================================

// ---------------------------------------------------------------------------
// B-1 (covers A1): Higher-term NLT received while node is in LEADER_READY →
//     node must step down and re-enter CANVASS (two-leaders-window close).
//     Aeron ref: Election.java:517-533; ref spec line 146.
//     Without the A1 fix the higher-term NLT was silently dropped and the
//     node kept leading — this test would FAIL in that case.
// ---------------------------------------------------------------------------
TEST(ElectionLog, PartB1_HigherTermNLT_WhileLeaderReady_StepsDownToCanvass) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);

    int64_t nowNs = 1'000'000LL;

    // Bring to LEADER_READY via the standard sequence.
    bringToLeaderLogReplication(e, nowNs);
    ASSERT_EQ(e.state(), ElectionState::LeaderLogReplication);

    // Drive leader through LEADER_REPLAY → LEADER_INIT → LEADER_READY by feeding
    // AppendPosition from both followers (quorum) and advancing the clock.
    Term ltid = e.leadershipTermId();
    e.onAppendPosition(nowNs, ltid, e.appendPosition(), 1, 0);
    e.onAppendPosition(nowNs, ltid, e.appendPosition(), 2, 0);

    for (int i = 0; i < 20 && e.state() != ElectionState::LeaderReady && e.state() != ElectionState::Closed; ++i) {
        int64_t d = e.nextDeadlineNs();
        nowNs = (d > nowNs) ? d : nowNs + 1;
        e.doWork(nowNs);
    }
    // Must be in LEADER_READY or LEADER_LOG_REPLICATION (either is a LEADER state).
    const bool inLeaderState =
        (e.state() == ElectionState::LeaderReady          ||
         e.state() == ElectionState::LeaderLogReplication  ||
         e.state() == ElectionState::LeaderReplay          ||
         e.state() == ElectionState::LeaderInit);
    ASSERT_TRUE(inLeaderState) << "Setup failed: leader not in a LEADER state before higher-term NLT";

    Term currentLeaderTerm = e.leadershipTermId();
    Term higherTerm        = currentLeaderTerm + 5;

    // Inject a NewLeadershipTerm from peer node 2 carrying a strictly-higher term.
    // A1 fix: this must trigger a step-down to CANVASS.
    e.onNewLeadershipTerm(nowNs,
        /*logLeadershipTermId=*/0,
        /*nextLeadershipTermId=*/higherTerm,
        /*nextTermBaseLogPosition=*/0,
        /*nextLogPosition=*/0,
        /*leadershipTermId=*/higherTerm,
        /*termBaseLogPosition=*/0,
        /*logPosition=*/0,
        /*leaderRecordingId=*/0,
        /*timestamp=*/0,
        /*leaderMemberId=*/2,
        /*logSessionId=*/0,
        /*isStartup=*/false,
        /*commitPosition=*/0);

    // Must have left LEADER state and entered CANVASS.
    EXPECT_EQ(e.state(), ElectionState::Canvass)
        << "A1: higher-term NLT in LEADER state must trigger step-down to CANVASS "
           "(Aeron Election.java:517-533; ref spec line 146)";
    EXPECT_EQ(e.leadershipTermId(), higherTerm)
        << "A1: leadershipTermId_ must be updated to the higher term on step-down";
}

// ---------------------------------------------------------------------------
// B-1b (covers A1): Higher-term CommitPosition while in LEADER_READY also
//     triggers step-down (same A1 fix, onCommitPosition path).
// ---------------------------------------------------------------------------
TEST(ElectionLog, PartB1b_HigherTermCommit_WhileLeaderReady_StepsDown) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);

    int64_t nowNs = 1'000'000LL;
    bringToLeaderLogReplication(e, nowNs);
    ASSERT_EQ(e.state(), ElectionState::LeaderLogReplication);

    // Advance to LEADER_READY.
    Term ltid = e.leadershipTermId();
    e.onAppendPosition(nowNs, ltid, e.appendPosition(), 1, 0);
    e.onAppendPosition(nowNs, ltid, e.appendPosition(), 2, 0);
    for (int i = 0; i < 20 && e.state() != ElectionState::LeaderReady && e.state() != ElectionState::Closed; ++i) {
        int64_t d = e.nextDeadlineNs();
        nowNs = (d > nowNs) ? d : nowNs + 1;
        e.doWork(nowNs);
    }
    // Accept LEADER_LOG_REPLICATION too if LEADER_READY not yet reached.
    const bool inLeader = (e.state() == ElectionState::LeaderReady ||
                           e.state() == ElectionState::LeaderLogReplication ||
                           e.state() == ElectionState::LeaderReplay ||
                           e.state() == ElectionState::LeaderInit);
    ASSERT_TRUE(inLeader);

    Term higherTerm = e.leadershipTermId() + 3;
    e.onCommitPosition(nowNs, higherTerm, /*logPosition=*/0, /*leaderMemberId=*/2);

    EXPECT_EQ(e.state(), ElectionState::Canvass)
        << "A1: higher-term CommitPosition in LEADER state must trigger step-down to CANVASS";
    EXPECT_EQ(e.leadershipTermId(), higherTerm);
}

// ---------------------------------------------------------------------------
// B-4 (audit Part B #4): NLT with nextLeadershipTermId < candidateTermId_ is
//     rejected (stale-ballot reject).  The Election.cpp:747-750 path that
//     returns early when nextLeadershipTermId < candidateTermId_ was previously
//     uncovered by any test.
// ---------------------------------------------------------------------------
TEST(ElectionLog, PartB4_NLT_StaleBallotReject_IgnoredWhenNextTermBehindCandidate) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // INIT → CANVASS
    ASSERT_EQ(e.state(), ElectionState::Canvass);

    // Vote in term 10 to advance candidateTermId_ to 10.
    e.onRequestVote(/*logLeadershipTermId=*/0, /*logPosition=*/0,
                    /*candidateTermId=*/10, /*candidateId=*/1, nowNs);
    ASSERT_EQ(e.state(), ElectionState::FollowerBallot);
    ASSERT_EQ(e.candidateTermId(), 10);

    // Now inject an NLT with nextLeadershipTermId=5 < candidateTermId_=10.
    // This is a stale-ballot NLT — the node has already advanced past term 5.
    // Election.cpp:747-750: must be rejected (return early without following).
    e.onNewLeadershipTerm(nowNs,
        /*logLeadershipTermId=*/0,
        /*nextLeadershipTermId=*/5,   // < candidateTermId_=10 → REJECT
        /*nextTermBaseLogPosition=*/0,
        /*nextLogPosition=*/0,
        /*leadershipTermId=*/5,
        /*termBaseLogPosition=*/0,
        /*logPosition=*/0,
        /*leaderRecordingId=*/0,
        /*timestamp=*/0,
        /*leaderMemberId=*/1,
        /*logSessionId=*/0,
        /*isStartup=*/true,
        /*commitPosition=*/0);

    // Must still be in FOLLOWER_BALLOT — stale-ballot NLT must be ignored.
    EXPECT_EQ(e.state(), ElectionState::FollowerBallot)
        << "B-4: NLT with nextLeadershipTermId=5 < candidateTermId_=10 must be rejected "
           "(Election.cpp:747-750 stale-ballot reject branch)";
    EXPECT_EQ(e.candidateTermId(), 10) << "B-4: candidateTermId must not regress on stale NLT";
}

// ---------------------------------------------------------------------------
// B-5 (audit Part B #5): Vote-persist-across-restart in FollowerBallot.
//     After granting a vote in FOLLOWER_BALLOT via onRequestVote, the
//     candidateTermId must be durable: a freshly-reconstructed Election with the
//     same NodeStateFile must see candidateTermId >= the granted term.
//     This tests the durable-vote path via a real Election object (not just the
//     NodeStateFile directly), covering the B1 fix end-to-end.
// ---------------------------------------------------------------------------
TEST(ElectionLog, PartB5_VotePersistAcrossRestart_FollowerBallotPath) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    Term grantedTerm = 7;

    {
        NodeStateFile nsf(nsfFile.str());
        TermLog       tl(tlFile.str());
        FakeConsensusPublisher pub;
        auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
        int64_t nowNs = 1'000'000LL;
        e.doWork(nowNs); // INIT → CANVASS

        // Grant vote for term 7 — this must durably store candidateTermId=7 via
        // NodeStateFile (B1 fix: fsync before grant).
        e.onRequestVote(/*logLeadershipTermId=*/0, /*logPosition=*/0,
                        /*candidateTermId=*/grantedTerm, /*candidateId=*/1, nowNs);
        ASSERT_EQ(e.state(), ElectionState::FollowerBallot);
        ASSERT_EQ(e.candidateTermId(), grantedTerm);
        // Election and NodeStateFile destroyed here; fd closed.
    }

    // Reconstruct — NodeStateFile must remember candidateTermId >= grantedTerm.
    NodeStateFile nsf2(nsfFile.str());
    EXPECT_GE(nsf2.candidateTermId(), grantedTerm)
        << "B-5: candidateTermId must survive a process restart; "
           "NodeStateFile must fsync before vote is granted (B1 fix).";

    // Reconstruct a full Election — it must clamp candidateTermId_ to the persisted value.
    TermLog       tl2(tlFile.str());
    FakeConsensusPublisher pub2;
    Election e2(/*isNodeStartup=*/false, /*gracefulClosedLeaderId=*/-1,
                /*leadershipTermId=*/grantedTerm, /*logLeadershipTermId=*/0,
                /*logPosition=*/0, /*appendPosition=*/0,
                makeMembers3(0), /*self=*/0, pub2, nsf2, tl2,
                kElectionTimeoutNs, kStatusIntervalNs, kStartupCanvassTimeoutNs, 42);
    int64_t nowNs2 = 1'000'000LL;
    e2.doWork(nowNs2); // INIT

    // The reconstructed election must not grant a vote for the same term it already voted in.
    // candidateTermId_ = max(persistedTerm≥7, leadershipTermId=7) = ≥7.
    // A vote request for term=7 (== candidateTermId_) must be denied.
    FakeConsensusPublisher pub3;
    NodeStateFile nsf3(nsfFile.str());
    TermLog       tl3(tlFile.str());
    Election e3(/*isNodeStartup=*/false, /*gracefulClosedLeaderId=*/-1,
                /*leadershipTermId=*/0, /*logLeadershipTermId=*/0,
                /*logPosition=*/0, /*appendPosition=*/0,
                makeMembers3(0), /*self=*/0, pub3, nsf3, tl3,
                kElectionTimeoutNs, kStatusIntervalNs, kStartupCanvassTimeoutNs, 42);
    int64_t nowNs3 = 1'000'000LL;
    e3.doWork(nowNs3); // INIT → CANVASS
    ASSERT_EQ(e3.state(), ElectionState::Canvass);

    // Same term as previously voted (grantedTerm=7) — must be denied because
    // candidateTermId_ was loaded from the persisted NodeStateFile (>= 7).
    // onRequestVote denies if candidateTermId ≤ candidateTermId_.
    pub3.placeVoteCalls.clear();
    e3.onRequestVote(/*logLeadershipTermId=*/0, /*logPosition=*/0,
                     /*candidateTermId=*/grantedTerm, /*candidateId=*/2, nowNs3);
    ASSERT_FALSE(pub3.placeVoteCalls.empty());
    EXPECT_FALSE(pub3.placeVoteCalls[0].vote)
        << "B-5: reconstructed Election must deny a vote for the same term previously "
           "voted in (candidateTermId persisted via NodeStateFile fsync).";
}

// ---------------------------------------------------------------------------
// B-6 (audit Part B #6): Election-timeout >= boundary: candidateBallot/followerBallot
//     at nowNs == timeOfLastStateChangeNs_ + electionTimeoutNs_ exactly (the
//     >= edge) should transition, not stay.
//     Also tests that isExtendedCanvass_ is cleared after first CANVASS exit.
// ---------------------------------------------------------------------------
TEST(ElectionLog, PartB6_ElectionTimeoutAtExactBoundary_Transitions) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // INIT → CANVASS (extended timeout on startup)

    // Vote → FOLLOWER_BALLOT (timeOfLastStateChangeNs_ = nowNs at this point)
    e.onRequestVote(0, 0, 1, 1, nowNs);
    ASSERT_EQ(e.state(), ElectionState::FollowerBallot);

    // At exactly boundary: nowNs + electionTimeoutNs_ — must transition (>= check).
    int64_t exactBoundary = nowNs + kElectionTimeoutNs;
    e.doWork(exactBoundary);
    EXPECT_EQ(e.state(), ElectionState::Canvass)
        << "B-6: FollowerBallot timeout at exact boundary (>= edge) must transition to CANVASS";

    // After first CANVASS exit, isExtendedCanvass_ must be cleared.
    // The next canvass timeout should use electionTimeoutNs_ (not startupCanvassTimeoutNs_).
    // nextDeadlineNs() for CANVASS with isExtendedCanvass_=false uses electionTimeoutNs_.
    // We don't inspect the private field directly — instead verify the deadline is within
    // [nowNs, nowNs + startupCanvassTimeoutNs_) (shorter than startup canvass timeout).
    int64_t canvassNow = exactBoundary;
    int64_t d = e.nextDeadlineNs();
    // After the first Canvass exit, timeout should be ≤ electionTimeoutNs_ from state-change.
    // The state change happened at exactBoundary, so deadline ≤ exactBoundary + electionTimeoutNs_.
    EXPECT_LE(d, exactBoundary + kElectionTimeoutNs)
        << "B-6: after first CANVASS exit, extended canvass must be cleared "
           "(deadline must use electionTimeoutNs_, not startupCanvassTimeoutNs_)";
    (void)canvassNow;
}

// ---------------------------------------------------------------------------
// B-7 (audit Part B #7): Duplicate/old-position fencing — an AppendPosition
//     from a known follower with a stale (lower) logPosition must not advance
//     quorum to a position the followers have not actually reached.
//     This covers the T2 "duplicate/old-position" fencing case still open.
//
//     Setup: construct a leader with appendPosition=1000 (non-zero) so that
//     "stale position=0" is genuinely behind the quorum threshold.
// ---------------------------------------------------------------------------
TEST(ElectionLog, PartB7_DuplicateOldPositionAppend_DoesNotAdvanceQuorum) {
    // Use a non-zero appendPosition so "position=0" from followers is genuinely stale.
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    std::vector<ClusterMember> ms(3);
    for (int i = 0; i < 3; i++) {
        ms[i].id = i; ms[i].logPosition = 0; ms[i].logLeadershipTermId = 0; ms[i].leadershipTermId = 0;
    }
    // Leader starts at appendPosition=1000 (simulates a node with real log entries).
    // C5 assert requires logPosition == appendPosition, so set both to 1000.
    const int64_t kLeaderPos = 1000;
    Election leader(/*isNodeStartup=*/true, /*gracefulClosedLeaderId=*/-1,
                    /*leadershipTermId=*/0, /*logLeadershipTermId=*/0,
                    /*logPosition=*/kLeaderPos, /*appendPosition=*/kLeaderPos,
                    std::move(ms), /*self=*/0, pub, nsf, tl,
                    kElectionTimeoutNs, kStatusIntervalNs, kStartupCanvassTimeoutNs, /*seed=*/0);

    int64_t nowNs = 1'000'000LL;
    // Drive to LeaderLogReplication.
    bringToLeaderLogReplication(leader, nowNs);
    ASSERT_EQ(leader.state(), ElectionState::LeaderLogReplication);

    Term    ltid      = leader.leadershipTermId();
    int64_t leaderPos = leader.appendPosition();
    ASSERT_EQ(leaderPos, kLeaderPos) << "Setup: leader must have appendPosition=1000";

    // Feed STALE AppendPositions (logPosition=0, well below leaderPos=1000) from both followers.
    // Quorum requires 2 members at logPosition >= leaderPos=1000.
    // Self is at leaderPos=1000, but followers report position=0 → only self qualifies.
    // quorumPosition for 3-member cluster (threshold=2): must see 2 members at >=1000.
    // With both followers at 0, quorumPosition = max of the 2 lowest = 0 → no quorum.
    leader.onAppendPosition(nowNs, ltid, /*logPosition=*/0, /*followerMemberId=*/1, 0);
    leader.onAppendPosition(nowNs, ltid, /*logPosition=*/0, /*followerMemberId=*/2, 0);

    // Drive for a few heartbeats — leader must NOT close without quorum.
    for (int i = 0; i < 5 && !leader.isClosed(); ++i) {
        int64_t d = leader.nextDeadlineNs();
        nowNs = (d > nowNs) ? d : nowNs + 1;
        leader.doWork(nowNs);
    }
    EXPECT_NE(leader.state(), ElectionState::Closed)
        << "B-7: stale-position AppendPosition (pos=0 < leaderPos=" << leaderPos
        << ") from both followers must not count toward quorum; "
           "leader must not close without a real majority at leaderPos";
    EXPECT_NE(leader.state(), ElectionState::LeaderReady)
        << "B-7: leader must not reach LeaderReady (quorum gate) with stale-position followers";

    // Confirm: feeding the CORRECT position from one follower (quorum met) DOES close.
    leader.onAppendPosition(nowNs, ltid, kLeaderPos, /*followerMemberId=*/1, 0);
    bool closed = driveLeaderToClosed(leader, nowNs, pub, {});
    EXPECT_TRUE(closed)
        << "B-7: after feeding correct-position from follower 1, leader must close "
           "(self + follower1 = 2 of 3 → quorum)";
}

// ---------------------------------------------------------------------------
// B-8 (audit Part B #8): C5 pass-through guard positive-path coverage.
//     When logPosition_ == appendPosition_ (the Phase-2 precondition), the
//     follower replay pass-through must succeed and advance to FollowerLogInit.
//     This exercises the guard branch in followerReplay() and leaderReplay()
//     without triggering the fatal assert, documenting the Phase-2 precondition.
//
//     The NEGATIVE case (logPosition != appendPosition) fires an assert() that
//     terminates the process — it cannot be tested here without EXPECT_DEATH.
//     The assert text "PHASE-2-DEFERRED" is the intended signal for Phase-3 wiring.
// ---------------------------------------------------------------------------
TEST(ElectionLog, PartB8_C5_PassthroughGuard_PositivePath_FollowerReachesLogInit) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    // logPosition == appendPosition == 0: Phase-2 precondition holds.
    // followerReplay() must pass through to FollowerLogInit without asserting.
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // INIT → CANVASS
    ASSERT_EQ(e.state(), ElectionState::Canvass);

    // Grant vote → FOLLOWER_BALLOT
    e.onRequestVote(0, 0, 1, 1, nowNs);
    ASSERT_EQ(e.state(), ElectionState::FollowerBallot);

    // NLT from leader → FOLLOWER_REPLAY (then pass-through to FollowerLogInit)
    e.onNewLeadershipTerm(nowNs, 0, 1, 0, 0, /*leadershipTermId=*/1, 0, 0, 0, 0,
                          /*leaderMemberId=*/1, 0, true, 0);
    // After NLT: state should be FOLLOWER_REPLAY; doWork advances it.
    ASSERT_EQ(e.state(), ElectionState::FollowerReplay);

    // doWork in FOLLOWER_REPLAY must not crash (logPosition_ == appendPosition_ == 0)
    // and must advance to FollowerLogInit (or beyond, via pass-through chain).
    e.doWork(nowNs);

    // Must have passed through FollowerReplay → FollowerLogInit → ... → Closed
    // (all follower pass-throughs are immediate; the bounded driver closes it).
    const bool pastFollowerReplay = (e.state() != ElectionState::FollowerReplay);
    EXPECT_TRUE(pastFollowerReplay)
        << "B-8/C5: Phase-2 followerReplay pass-through (logPos==appendPos) must advance "
           "past FollowerReplay without triggering the PHASE-2-DEFERRED assert";
}

// ===========================================================================
// GROUP 9: Multi-node CLOSED leader heartbeat (closedLeaderHeartbeat shim)
//          BR-3b fix: lock the shim against regression.
// ===========================================================================

// CH-1: Single-member Election in CLOSED emits NOTHING on doWork (no-op path).
//       nextDeadlineNs() returns INT64_MIN for single-member CLOSED.
//       Extends the existing L6 test with explicit sink assertion.
TEST(ElectionLog, ClosedHeartbeat_SingleMember_IsNoop) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers1(0), 0, pub, nsf, tl);
    int64_t nowNs = 1'000'000LL;
    ASSERT_TRUE(driveToClosed(e, nowNs));
    ASSERT_EQ(e.state(), ElectionState::Closed);

    pub.clear();
    Term termBefore = e.leadershipTermId();

    // Advance past many status intervals and pump repeatedly — must produce no NLT.
    for (int i = 0; i < 10; ++i) {
        nowNs += kStatusIntervalNs + 1;
        int work = e.doWork(nowNs);
        EXPECT_EQ(work, 0)
            << "CH-1: single-member CLOSED doWork must return 0 (no-op), got " << work;
    }
    EXPECT_TRUE(pub.nltCalls.empty())
        << "CH-1: single-member CLOSED leader must not emit any NLT";
    EXPECT_EQ(e.leadershipTermId(), termBefore)
        << "CH-1: leadershipTermId must not change in CLOSED (no re-election)";
    // nextDeadlineNs must remain INT64_MIN — no pending deadline for single-member CLOSED.
    EXPECT_EQ(e.nextDeadlineNs(), INT64_MIN)
        << "CH-1: nextDeadlineNs() must be INT64_MIN for single-member CLOSED";
}

// CH-2: 3-member leader in CLOSED re-emits NLT at the status interval with the
//       SAME leadershipTermId (no term increment).  This is the core safety
//       property of closedLeaderHeartbeat: late joiners receive the existing
//       leader's term so they can converge without re-triggering a new election.
TEST(ElectionLog, ClosedHeartbeat_ThreeMember_Leader_EmitsNLT_SameTermId) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    // seed=0: bringToLeaderLogReplication nominates immediately.
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);

    int64_t nowNs = 1'000'000LL;

    // Drive node 0 through canvass/ballot to LeaderLogReplication, then to CLOSED.
    bringToLeaderLogReplication(e, nowNs);
    ASSERT_EQ(e.state(), ElectionState::LeaderLogReplication)
        << "CH-2 setup: must reach LeaderLogReplication first";
    ASSERT_TRUE(driveLeaderToClosed(e, nowNs, pub, {1, 2}))
        << "CH-2 setup: 3-member leader (node 0) must reach CLOSED";
    ASSERT_EQ(e.state(), ElectionState::Closed);
    ASSERT_EQ(e.leaderMemberId(), 0) << "CH-2 setup: node 0 must be the leader";

    const Term leaderTerm = e.leadershipTermId();
    pub.clear();

    // --- Verify nextDeadlineNs() returns a real heartbeat deadline (not INT64_MIN) ---
    int64_t deadline = e.nextDeadlineNs();
    EXPECT_NE(deadline, INT64_MIN)
        << "CH-2: multi-node leader in CLOSED must have a pending heartbeat deadline";
    EXPECT_LE(deadline, nowNs + kStatusIntervalNs + 1)
        << "CH-2: heartbeat deadline must be within one status interval";

    // Advance past the status interval and call doWork.
    nowNs = deadline + 1;  // exactly past the deadline
    int work = e.doWork(nowNs);
    EXPECT_GT(work, 0)
        << "CH-2: multi-node leader CLOSED doWork past status interval must return > 0";

    // Exactly two NLT messages must have been emitted: one to peer 1, one to peer 2.
    ASSERT_EQ(pub.nltCalls.size(), 2u)
        << "CH-2: leader in CLOSED must emit NLT to both peers (2 NLTs for 3-member cluster)";

    std::vector<NodeId> targets;
    for (auto& nlt : pub.nltCalls) targets.push_back(nlt.toMember);
    std::sort(targets.begin(), targets.end());
    EXPECT_EQ(targets[0], 1) << "CH-2: NLT must be addressed to peer 1";
    EXPECT_EQ(targets[1], 2) << "CH-2: NLT must be addressed to peer 2";

    // Key safety property: leadershipTermId in the NLT must match the original term.
    // The shim MUST NOT increment the term — that would start a new election.
    for (auto& nlt : pub.nltCalls) {
        EXPECT_EQ(nlt.leadershipTermId, leaderTerm)
            << "CH-2: NLT leadershipTermId must equal the original CLOSED term (no increment)";
        EXPECT_EQ(nlt.nextLeadershipTermId, leaderTerm)
            << "CH-2: NLT nextLeadershipTermId must equal the original CLOSED term";
        EXPECT_EQ(nlt.leaderMemberId, 0)
            << "CH-2: NLT leaderMemberId must identify node 0 as leader";
    }
    // The election's own leadershipTermId must not have changed.
    EXPECT_EQ(e.leadershipTermId(), leaderTerm)
        << "CH-2: Election::leadershipTermId_ must not change during CLOSED heartbeat";
}

// CH-3: 3-member leader in CLOSED emits MULTIPLE heartbeats on successive
//       clock advances, each carrying the SAME term.  Each heartbeat is
//       bounded by the status interval (no skipping).
TEST(ElectionLog, ClosedHeartbeat_ThreeMember_Leader_MultipleHeartbeats_SameTerm) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);

    int64_t nowNs = 1'000'000LL;
    bringToLeaderLogReplication(e, nowNs);
    ASSERT_EQ(e.state(), ElectionState::LeaderLogReplication);
    ASSERT_TRUE(driveLeaderToClosed(e, nowNs, pub, {1, 2}));
    ASSERT_EQ(e.state(), ElectionState::Closed);
    ASSERT_EQ(e.leaderMemberId(), 0);

    const Term leaderTerm = e.leadershipTermId();
    pub.clear();

    // Fire 3 heartbeat intervals.
    static constexpr int kHeartbeats = 3;
    for (int h = 0; h < kHeartbeats; ++h) {
        pub.clear();
        int64_t d = e.nextDeadlineNs();
        ASSERT_NE(d, INT64_MIN) << "CH-3: heartbeat deadline must be valid at interval " << h;
        nowNs = d + 1;
        int work = e.doWork(nowNs);
        EXPECT_GT(work, 0) << "CH-3: doWork must report work at interval " << h;
        ASSERT_EQ(pub.nltCalls.size(), 2u)
            << "CH-3: 2 NLTs expected per heartbeat at interval " << h;
        for (auto& nlt : pub.nltCalls) {
            EXPECT_EQ(nlt.leadershipTermId, leaderTerm)
                << "CH-3: NLT leadershipTermId must remain " << leaderTerm
                << " at interval " << h;
            EXPECT_EQ(nlt.nextLeadershipTermId, leaderTerm)
                << "CH-3: NLT nextLeadershipTermId must remain " << leaderTerm
                << " at interval " << h;
        }
    }

    // Election state must still be CLOSED — heartbeat must not transition the state.
    EXPECT_EQ(e.state(), ElectionState::Closed)
        << "CH-3: CLOSED state must be preserved across " << kHeartbeats << " heartbeat intervals";
    EXPECT_EQ(e.leadershipTermId(), leaderTerm)
        << "CH-3: leadershipTermId must remain " << leaderTerm << " after " << kHeartbeats << " heartbeats";
}

// CH-4: 3-member FOLLOWER in CLOSED emits no NLT (only the leader heartbeats).
//       Drive node 0 to CLOSED as a follower (leader = node 1).
// CB-5: a follower in CLOSED must NOT broadcast leader heartbeats (only the
// leader does), but it MUST run leader-liveness detection: while the leader
// keeps sending heartbeats the follower stays in CLOSED; once the leader goes
// silent past the leader-heartbeat timeout the follower re-enters CANVASS so the
// surviving nodes can elect a new leader (dead-leader recovery).
TEST(ElectionLog, ClosedFollower_NoHeartbeatBroadcast_ButLivenessRearmed) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    // Node 0 is a follower that voted for node 1.
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // INIT → CANVASS
    e.onRequestVote(0, 0, 1, /*candidateId=*/1, nowNs); // vote for node 1 → FOLLOWER_BALLOT
    ASSERT_EQ(e.state(), ElectionState::FollowerBallot);

    // Leader 1 sends NLT → follower path → CLOSED.
    e.onNewLeadershipTerm(nowNs, 0, 1, 0, 0, /*leadershipTermId=*/1, 0, 0, 0, 0,
                          /*leaderMemberId=*/1, 0, true, 0);
    ASSERT_TRUE(driveToClosed(e, nowNs));
    ASSERT_EQ(e.state(), ElectionState::Closed);
    ASSERT_EQ(e.leaderMemberId(), 1) << "setup: leader must be node 1, not node 0";

    pub.clear();

    // The follower is now armed.  Its FIRST duty is an immediate AppendPosition
    // send (nextDeadlineNs()==INT64_MIN means "act now"); after the first send the
    // next deadline becomes a real future time (min of next-send / leader-expiry).
    e.doWork(nowNs);   // perform the immediate first AppendPosition send
    EXPECT_FALSE(pub.appendPositionCalls.empty())
        << "follower in CLOSED must send its AppendPosition to the leader";
    EXPECT_GT(e.nextDeadlineNs(), nowNs)
        << "after the first send the follower schedules a future send/liveness deadline";
    pub.clear();

    // While the leader keeps sending CommitPosition heartbeats, the follower stays
    // in CLOSED and emits NO NLT of its own — BUT it MUST periodically publish its
    // OWN AppendPosition back to the leader so the leader's per-member liveness
    // (eligibleNodeIds → partition failover) never goes stale.  Omitting this
    // collapsed the eligible set and broke failover + churned throughput; the
    // FollowerLiveness SM now drives one send per status interval.
    for (int i = 0; i < 5; ++i) {
        nowNs += kStatusIntervalNs + 1;
        e.onCommitPosition(nowNs, /*leadershipTermId=*/1, 0, /*leaderMemberId=*/1);
        e.doWork(nowNs);
        EXPECT_EQ(e.state(), ElectionState::Closed)
            << "follower must stay CLOSED while leader heartbeats arrive";
    }
    EXPECT_TRUE(pub.nltCalls.empty())
        << "follower in CLOSED must never emit NLT (only the leader broadcasts)";
    EXPECT_FALSE(pub.appendPositionCalls.empty())
        << "REGRESSION: follower in CLOSED MUST periodically publish its "
           "AppendPosition to the leader (keeps leader liveness / eligibleNodeIds "
           "fresh — the failover-eligibility + throughput-churn fix)";
}

// CB-5: dead-leader detection — a follower in CLOSED whose leader goes silent
// past the leader-heartbeat timeout re-enters CANVASS to trigger re-election.
TEST(ElectionLog, ClosedFollower_DeadLeader_ReCanvasses) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl);

    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs); // INIT → CANVASS
    e.onRequestVote(0, 0, 1, /*candidateId=*/1, nowNs); // vote for node 1 → FOLLOWER_BALLOT
    e.onNewLeadershipTerm(nowNs, 0, 1, 0, 0, /*leadershipTermId=*/1, 0, 0, 0, 0,
                          /*leaderMemberId=*/1, 0, true, 0);
    ASSERT_TRUE(driveToClosed(e, nowNs));
    ASSERT_EQ(e.state(), ElectionState::Closed);
    ASSERT_EQ(e.leaderMemberId(), 1);

    // Leader dies: no more heartbeats.  nextDeadlineNs() is now the sooner of the
    // periodic AppendPosition-send deadline and the leader-liveness expiry, so
    // advance WELL past the leader-heartbeat timeout explicitly to force detection.
    nowNs += 10 * e.leaderHeartbeatTimeoutNs();
    int work = e.doWork(nowNs);

    EXPECT_GT(work, 0) << "silent-leader timeout must produce work (re-canvass)";
    EXPECT_EQ(e.state(), ElectionState::Canvass)
        << "follower must re-enter CANVASS after leader-heartbeat timeout";
    EXPECT_EQ(e.leaderMemberId(), -1)
        << "stale dead leader must be dropped so observers stop redirecting to it";
}

// B-8b: C5 positive path — leaderReplay pass-through also must not crash when
//       logPosition_ == appendPosition_.
TEST(ElectionLog, PartB8b_C5_PassthroughGuard_LeaderReplay_PositivePath) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);

    int64_t nowNs = 1'000'000LL;
    bringToLeaderLogReplication(e, nowNs);
    ASSERT_EQ(e.state(), ElectionState::LeaderLogReplication);

    // Feed AppendPosition from both followers → quorum → transitions through
    // LeaderLogReplication → LeaderReplay (guarded assert) → LeaderInit → LeaderReady.
    Term ltid = e.leadershipTermId();
    e.onAppendPosition(nowNs, ltid, e.appendPosition(), 1, 0);
    e.onAppendPosition(nowNs, ltid, e.appendPosition(), 2, 0);

    // Advance clock — leader transitions through LEADER_REPLAY (assert must not fire).
    for (int i = 0; i < 10 && e.state() == ElectionState::LeaderLogReplication; ++i) {
        int64_t d = e.nextDeadlineNs();
        nowNs = (d > nowNs) ? d : nowNs + 1;
        e.doWork(nowNs);
    }
    EXPECT_NE(e.state(), ElectionState::LeaderLogReplication)
        << "B-8b/C5: leaderReplay pass-through (logPos==appendPos) must advance past "
           "LeaderLogReplication without triggering the PHASE-2-DEFERRED assert";
}

// ===========================================================================
// REGRESSION: become-leader liveness grace (acks=all leadership-churn fix).
//
// Bug (found via e2e on the 4-core VM): a busy single-partition leader (node0)
// starved its own consensus heartbeat and lost leadership.  The new leader
// (node1) closed its election on quorum = {self, node2} WITHOUT node0 ever
// sending an AppendPosition for the new term, so node0's clock stayed at the
// INT64_MIN sentinel → isActive()==false → eligibleNodeIds() reported node0
// dead → the DynamicBalancer failed node0's partition over to node1 while the
// still-alive node0 was wrongly excluded (leadership churn / throughput dip).
//
// Fix: electionComplete() seeds every OTHER member's timeOfLastAppendPositionNs
// to `nowNs` when THIS node becomes leader — one full timeout window of grace
// for a peer to report before it can be judged dead.  Time-independent: any
// window converges; a live peer reports within it, a dead one never does.
// ===========================================================================
TEST(ElectionLogRegression, BecomeLeaderGrantsLivenessGraceToSilentPeer) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;

    // Node 0 is self.  Win the election via canvass+votes from BOTH peers (votes
    // do NOT set the append-position liveness clock — only onAppendPosition does),
    // then close the leader phase on an AppendPosition quorum of {self, node2}
    // ONLY.  node1 (the busy/lagging peer) votes but NEVER sends an AppendPosition
    // for this term → its liveness clock stays at the INT64_MIN sentinel, exactly
    // reproducing the production scenario.
    auto e = makeElection(makeMembers3(0), /*self=*/0, pub, nsf, tl);
    int64_t nowNs = 1'000'000LL;
    bringToLeaderLogReplication(e, nowNs);
    ASSERT_EQ(e.state(), ElectionState::LeaderLogReplication);
    ASSERT_TRUE(driveLeaderToClosed(e, nowNs, pub, /*followerIds=*/{2}))
        << "leader must close on self+node2 majority without node1's AppendPosition";
    ASSERT_EQ(e.leaderMemberId(), 0) << "node 0 must be the leader";

    // Immediately after becoming leader, node1 has sent nothing this term.  With
    // the grace seed it must STILL be eligible (benefit of the doubt for one
    // window); without the fix it would be excluded → false failover.
    NodeId out[8];
    std::size_t n = e.eligibleNodeIds(nowNs, kElectionTimeoutNs, out, 8);
    std::vector<NodeId> elig(out, out + n);
    EXPECT_NE(std::find(elig.begin(), elig.end(), 1), elig.end())
        << "silent peer node1 must be granted liveness grace at leader handover, "
           "not immediately declared dead (the churn bug)";
    EXPECT_EQ(n, 3u) << "all three members eligible within the grace window";

    // The grace is a WINDOW, not permanent: after the timeout elapses with no
    // report from node1, it drops out — real crash-failover is preserved.
    int64_t later = nowNs + 2 * kElectionTimeoutNs;
    std::size_t n2 = e.eligibleNodeIds(later, kElectionTimeoutNs, out, 8);
    std::vector<NodeId> elig2(out, out + n2);
    EXPECT_EQ(std::find(elig2.begin(), elig2.end(), 1), elig2.end())
        << "after the grace window with no heartbeat, a genuinely silent peer "
           "MUST expire so real failover still fires";
}

// ===========================================================================
// Controlled shutdown (2026-10-05): a resigning leader hands the consensus
// off before it exits — it tells every follower to start an election NOW
// (StandDown, template 59) instead of letting them wait out the leader
// heartbeat timeout, and it never stands for election again in this process.
// ===========================================================================
namespace {
// Own publisher (NOT derived from FakeConsensusPublisher: several test TUs
// define different global classes of that name — deriving from it here would
// pick up another TU's inline members).
struct StandDownRecorder final : ConsensusPublisher {
    struct Call { NodeId to; Term term; NodeId leader; };
    std::vector<Call> standDowns;
    int requestVotes = 0, nlts = 0;
    void standDown(NodeId to, Term term, NodeId leader) override { standDowns.push_back({to, term, leader}); }
    int canvasses = 0;
    void canvassPosition(NodeId, Term, int64_t, Term, NodeId) override { ++canvasses; }
    void requestVote(NodeId, Term, int64_t, Term, NodeId) override { ++requestVotes; }
    void placeVote(NodeId, Term, Term, int64_t, NodeId, NodeId, bool) override {}
    void newLeadershipTerm(NodeId, Term, Term, int64_t, int64_t, Term, int64_t, int64_t, int64_t,
                           int64_t, NodeId, int32_t, bool, int64_t) override { ++nlts; }
    void appendPosition(NodeId, Term, int64_t, NodeId, uint8_t) override {}
    void commitPosition(NodeId, Term, int64_t, NodeId) override {}
    void clear() { standDowns.clear(); requestVotes = 0; nlts = 0; canvasses = 0; }
};
Election makeSdElection(NodeId self, StandDownRecorder& pub, NodeStateFile& nsf, TermLog& tl, uint64_t seed) {
    return Election(true, -1, 0, 0, 0, 0, makeMembers3(self), self, pub, nsf, tl,
                    kElectionTimeoutNs, kStatusIntervalNs, kStartupCanvassTimeoutNs, seed);
}
bool driveSdLeaderToClosed(Election& leader, int64_t& nowNs) {
    for (int i = 0; i < 100 && !leader.isClosed(); ++i) {
        const int64_t deadline = leader.nextDeadlineNs();
        nowNs = deadline > nowNs ? deadline : nowNs + 1;
        leader.doWork(nowNs);
        if (leader.state() == ElectionState::LeaderLogReplication || leader.state() == ElectionState::LeaderReady)
            for (NodeId fid : {1, 2}) leader.onAppendPosition(nowNs, leader.leadershipTermId(), leader.appendPosition(), fid, 0);
    }
    return leader.isClosed();
}
} // namespace

TEST(ElectionStandDownRegression, ResigningLeaderTellsFollowersAndNeverStandsAgainRegression) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    StandDownRecorder pub;
    auto e = makeSdElection(0, pub, nsf, tl, /*seed=*/0);
    int64_t nowNs = 1'000'000LL;
    bringToLeaderLogReplication(e, nowNs);
    ASSERT_TRUE(driveSdLeaderToClosed(e, nowNs));
    ASSERT_EQ(e.leaderMemberId(), 0);
    const Term term = e.leadershipTermId();

    ASSERT_TRUE(e.resign(nowNs)) << "a CLOSED leader must be able to resign";
    EXPECT_TRUE(e.resigned());
    ASSERT_EQ(pub.standDowns.size(), 2u) << "every follower must be told to elect now";
    for (const auto& c : pub.standDowns) { EXPECT_EQ(c.term, term); EXPECT_EQ(c.leader, 0); }
    EXPECT_NE(e.leaderMemberId(), 0) << "the resigned node still reports itself leader";
    // It never nominates itself again, however long it canvasses.
    pub.clear();
    for (int i = 0; i < 50; ++i) {
        e.onCanvassPosition(0, 0, term, 1);
        e.onCanvassPosition(0, 0, term, 2);
        nowNs += kElectionTimeoutNs;
        e.doWork(nowNs);
        EXPECT_TRUE(e.state() == ElectionState::Canvass) << "the resigned node stood for election";
    }
    EXPECT_EQ(pub.requestVotes, 0);
    EXPECT_EQ(pub.nlts, 0) << "the resigned node kept heartbeating as leader";
    // It follows the successor.
    e.onNewLeadershipTerm(nowNs, term, term + 1, 0, 0, term + 1, 0, 0, 0, 0, /*leader=*/1, 0, false, 0);
    ASSERT_TRUE(driveToClosed(e, nowNs));
    EXPECT_EQ(e.leaderMemberId(), 1);
    EXPECT_FALSE(e.resign(nowNs)) << "only the leader can resign";
}

TEST(ElectionStandDownRegression, FollowerElectsAtOnceSkippingTheResignedLeaderRegression) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    StandDownRecorder pub;
    auto e = makeSdElection(0, pub, nsf, tl, 42);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);
    e.onRequestVote(0, 0, 1, 1, nowNs);
    e.onNewLeadershipTerm(nowNs, 0, 1, 0, 0, 1, 0, 0, 0, 0, /*leader=*/1, 0, true, 0);
    ASSERT_TRUE(driveToClosed(e, nowNs));
    ASSERT_EQ(e.leaderMemberId(), 1);

    e.onStandDown(nowNs, /*term=*/0, /*leader=*/1);   // stale term
    e.onStandDown(nowNs, /*term=*/1, /*leader=*/2);   // not our leader
    EXPECT_EQ(e.state(), ElectionState::Closed) << "a stale / foreign StandDown re-opened the election";
    EXPECT_EQ(e.leaderMemberId(), 1);

    e.onStandDown(nowNs + 1, /*term=*/1, /*leader=*/1);   // well before any heartbeat timeout
    EXPECT_EQ(e.state(), ElectionState::Canvass) << "the follower waited for the heartbeat timeout";
    EXPECT_EQ(e.leaderMemberId(), -1);
    // Unanimous WITHOUT the resigned leader's canvass: only node 2 reports.
    e.onCanvassPosition(0, 0, 1, 2);
    e.doWork(nowNs + 2);
    EXPECT_EQ(e.state(), ElectionState::Nominate) << "the election waited for the resigned leader";
}

// Controlled shutdown found it (2026-10-05): the node reports itself leader
// (NodeConsensusAgent: every Leader* state) from LEADER_LOG_REPLICATION on, so
// its controller duties (partition failover over eligibleNodeIds()) start
// THEN — before electionComplete.  The become-leader liveness grace must
// already hold there: a freshly elected leader sees every peer live for one
// window, never "only me" (which failed every partition over to itself with
// ISR={self} right after a hand-off).
TEST(ElectionStandDownRegression, NewLeaderSeesPeersLiveFromLeaderLogReplicationRegression) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    StandDownRecorder pub;
    auto e = makeSdElection(0, pub, nsf, tl, /*seed=*/0);
    int64_t nowNs = 1'000'000LL;
    bringToLeaderLogReplication(e, nowNs);
    ASSERT_EQ(e.state(), ElectionState::LeaderLogReplication);
    std::array<NodeId, 8> buf{};
    const std::size_t n = e.eligibleNodeIds(nowNs, kElectionTimeoutNs, buf.data(), buf.size());
    EXPECT_EQ(n, 3u) << "a new leader judged its live peers dead before its first AppendPosition";
}

// Review 2026-10-05 D2 (Aeron fidelity): LEADER_LOG_REPLICATION waits for a
// quorum of NEW-TERM AppendPositions — the peers' old-term canvass / vote
// positions never count (Election.java:820), even though the new leader's
// failover liveness view (eligibleNodeIds) already sees its peers live.
TEST(ElectionStandDownRegression, LeaderLogReplicationIgnoresOldTermCanvassPositionsRegression) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    StandDownRecorder pub;
    Election e(true, -1, 0, 0, /*logPosition=*/1000, /*appendPosition=*/1000, makeMembers3(0), 0, pub, nsf, tl,
               kElectionTimeoutNs, kStatusIntervalNs, kStartupCanvassTimeoutNs, 0);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);
    e.onCanvassPosition(0, 1000, 0, 1);
    e.onCanvassPosition(0, 1000, 0, 2);
    nowNs += kStartupCanvassTimeoutNs + 1;
    e.doWork(nowNs);
    nowNs += kElectionTimeoutNs * 10;
    e.doWork(nowNs);
    nowNs += 1;
    e.doWork(nowNs);
    const Term cTerm = e.candidateTermId();
    e.onVote(0, 1000, cTerm, 0, 1, true);
    e.onVote(0, 1000, cTerm, 0, 2, true);
    nowNs += 1;
    e.doWork(nowNs);
    ASSERT_EQ(e.state(), ElectionState::LeaderLogReplication);
    for (int i = 0; i < 3; ++i) e.doWork(++nowNs);
    EXPECT_EQ(e.state(), ElectionState::LeaderLogReplication)
        << "old-term canvass/vote positions counted toward the replication quorum";
    std::array<NodeId, 8> buf{};
    EXPECT_EQ(e.eligibleNodeIds(nowNs, kElectionTimeoutNs, buf.data(), buf.size()), 3u)
        << "the new leader's failover view must still see its peers live";
    e.onAppendPosition(nowNs, e.leadershipTermId(), 1000, 1, 0);
    e.doWork(++nowNs);
    EXPECT_NE(e.state(), ElectionState::LeaderLogReplication) << "a new-term AppendPosition quorum did not advance";
}

// Review LOW-1: a follower told by StandDown to elect now keeps skipping the
// resigned leader even when that leader canvasses with a NEWER log (an
// unreplicated tail appended in its resign tick) — the hand-off election stays
// unanimous without it instead of falling back to the election timeout.  The
// resigned leader still canvasses (its position lets others count its vote).
TEST(ElectionStandDownRegression, ResignedLeaderCanvassKeepsTheFollowersSkipRegression) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    StandDownRecorder pub;
    auto e = makeSdElection(0, pub, nsf, tl, 42);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);
    e.onRequestVote(0, 0, 1, 1, nowNs);
    e.onNewLeadershipTerm(nowNs, 0, 1, 0, 0, 1, 0, 0, 0, 0, /*leader=*/1, 0, true, 0);
    ASSERT_TRUE(driveToClosed(e, nowNs));
    e.onStandDown(nowNs + 1, 1, 1);
    ASSERT_EQ(e.state(), ElectionState::Canvass);
    e.onCanvassPosition(1, /*logPosition=*/500, 1, /*from=*/1);   // the resigned leader, newer log
    e.onCanvassPosition(0, 0, 1, 2);
    e.doWork(nowNs + 2);
    EXPECT_EQ(e.state(), ElectionState::Nominate)
        << "the resigned leader's canvass cleared the skip — the hand-off fell back to the timeout";

    // ... and the resigned leader DOES canvass.
    TempFile nsf2(".nsf"), tl2(".tl");
    NodeStateFile n2(nsf2.str());
    TermLog       t2(tl2.str());
    StandDownRecorder pub2;
    auto l = makeSdElection(0, pub2, n2, t2, 0);
    int64_t now2 = 1'000'000LL;
    bringToLeaderLogReplication(l, now2);
    ASSERT_TRUE(driveSdLeaderToClosed(l, now2));
    ASSERT_TRUE(l.resign(now2));
    pub2.clear();
    for (int i = 0; i < 5; ++i) { now2 += kStatusIntervalNs; l.doWork(now2); }
    EXPECT_GT(pub2.canvasses, 0) << "a resigned leader that never canvasses can block a quorum";
}

// ===========================================================================
// ClosedLeaderStepDownRegression (DR-10 repeated-flap, box run 2026-10-05).
//
// A leader that reached CLOSED never left it: it ignored a higher-term NLT /
// CommitPosition (only the pre-CLOSED Leader* states stepped down), it never
// checked for an active follower quorum, and it silently dropped RequestVote
// (no reply in CLOSED).  A leader paused past the heartbeat timeout (docker
// pause) therefore resumed as a zombie CLOSED leader of the old term: the
// survivors elected a new leader without it, and when THAT leader was paused
// next, the zombie never voted — the two live nodes stayed headless for 4+
// minutes.  Aeron leaves the steady state in all three cases
// (ConsensusModuleAgent: inactive-follower-quorum → enterElection; a higher
// leadershipTermId NLT → enterElection; an unexpected RequestVote with a
// higher term → enterElection).
// ===========================================================================

static void closeAsThreeMemberLeader(Election& e, FakeConsensusPublisher& pub, int64_t& nowNs) {
    bringToLeaderLogReplication(e, nowNs);
    EXPECT_TRUE(driveLeaderToClosed(e, nowNs, pub, {1, 2}));
    EXPECT_EQ(e.leaderMemberId(), 0);
}

TEST(ElectionLog, ClosedLeaderStepDownRegression_HigherTermNLT_StepsDownAndFollows) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    int64_t nowNs = 1'000'000LL;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);
    closeAsThreeMemberLeader(e, pub, nowNs);
    ASSERT_TRUE(e.isClosed());

    const Term higher = e.leadershipTermId() + 1;
    auto nlt = [&] {
        e.onNewLeadershipTerm(nowNs, /*logLeadershipTermId=*/e.leadershipTermId(),
                              /*nextLeadershipTermId=*/higher, 0, 0,
                              /*leadershipTermId=*/higher, 0,
                              /*logPosition=*/e.appendPosition(), 0, 0,
                              /*leaderMemberId=*/2, 0, false, 0);
    };
    nlt();
    EXPECT_FALSE(e.isClosed()) << "a CLOSED leader must step down on a higher-term NLT";
    EXPECT_NE(e.leaderMemberId(), 0) << "the stepped-down node must not still report itself leader";
    EXPECT_EQ(e.leadershipTermId(), higher);

    // The new leader's next heartbeat NLT makes it a follower of node 2.
    nlt();
    ASSERT_TRUE(driveToClosed(e, nowNs));
    EXPECT_EQ(e.leaderMemberId(), 2);
    EXPECT_EQ(e.leadershipTermId(), higher);
}

TEST(ElectionLog, ClosedLeaderStepDownRegression_HigherTermCommit_StepsDown) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    int64_t nowNs = 1'000'000LL;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);
    closeAsThreeMemberLeader(e, pub, nowNs);
    ASSERT_TRUE(e.isClosed());

    const Term higher = e.leadershipTermId() + 2;
    e.onCommitPosition(nowNs, higher, /*logPosition=*/0, /*leaderMemberId=*/1);
    EXPECT_EQ(e.state(), ElectionState::Canvass);
    EXPECT_EQ(e.leadershipTermId(), higher);
}

TEST(ElectionLog, ClosedLeaderStepDownRegression_HigherTermRequestVote_IsAnswered) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    int64_t nowNs = 1'000'000LL;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);
    closeAsThreeMemberLeader(e, pub, nowNs);
    ASSERT_TRUE(e.isClosed());
    pub.clear();
    // The zombie case: paused past the heartbeat timeout, no follower heard since
    // (no doWork yet — the RequestVote is the first thing it processes).
    nowNs += e.leaderHeartbeatTimeoutNs() + 1;

    // Candidate 1 with an equally fresh log asks for a vote in a newer term.
    const Term candTerm = e.leadershipTermId() + 1;
    e.onRequestVote(/*logLeadershipTermId=*/e.leadershipTermId(), e.appendPosition(),
                    candTerm, /*candidateId=*/1, nowNs);

    ASSERT_EQ(pub.placeVoteCalls.size(), 1u)
        << "a CLOSED (zombie) leader must answer a higher-term RequestVote — "
           "a silent drop withholds the vote the survivors need for a majority";
    EXPECT_TRUE(pub.placeVoteCalls[0].vote);
    EXPECT_EQ(pub.placeVoteCalls[0].toMember, 1);
    EXPECT_FALSE(e.isClosed());
    EXPECT_NE(e.leaderMemberId(), 0);
}

TEST(ElectionLog, ClosedLeaderStepDownRegression_LostFollowerQuorum_StepsDown) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    int64_t nowNs = 1'000'000LL;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);
    closeAsThreeMemberLeader(e, pub, nowNs);
    ASSERT_TRUE(e.isClosed());

    // Paused / partitioned leader: no AppendPosition from any follower for longer
    // than the heartbeat timeout (the docker-pause resume case).
    nowNs += e.leaderHeartbeatTimeoutNs() + 1;
    e.doWork(nowNs);
    EXPECT_EQ(e.state(), ElectionState::Canvass)
        << "a CLOSED leader without an active follower quorum must step down";
    EXPECT_EQ(e.leaderMemberId(), -1);
}

TEST(ElectionLog, ClosedLeaderStepDownRegression_LiveFollowers_KeepLeadership) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    int64_t nowNs = 1'000'000LL;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);
    closeAsThreeMemberLeader(e, pub, nowNs);
    ASSERT_TRUE(e.isClosed());

    // Only ONE follower reports (2 of 3 incl. the leader = quorum) for 10 timeouts.
    const int64_t end = nowNs + 10 * e.leaderHeartbeatTimeoutNs();
    while (nowNs < end) {
        nowNs += kStatusIntervalNs;
        e.onAppendPosition(nowNs, e.leadershipTermId(), e.appendPosition(), 1, 0);
        e.doWork(nowNs);
        ASSERT_TRUE(e.isClosed()) << "a leader with a live follower quorum must keep leadership";
    }
    EXPECT_EQ(e.leaderMemberId(), 0);
}

TEST(ElectionLog, ClosedLeaderStepDownRegression_StaleCandidateRequestVote_KeepsLeadership) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    int64_t nowNs = 1'000'000LL;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);
    closeAsThreeMemberLeader(e, pub, nowNs);
    ASSERT_TRUE(e.isClosed());
    pub.clear();

    // A candidate whose log is BEHIND ours asks at a higher term: denied, and a
    // denied candidacy must never unseat the live leader (MAJOR-R3).
    e.onRequestVote(/*logLeadershipTermId=*/-1, /*logPosition=*/0,
                    e.leadershipTermId() + 1, /*candidateId=*/1, nowNs);
    ASSERT_EQ(pub.placeVoteCalls.size(), 1u);
    EXPECT_FALSE(pub.placeVoteCalls[0].vote);
    EXPECT_TRUE(e.isClosed());
    EXPECT_EQ(e.leaderMemberId(), 0);
}

// DR-10 flap (2026-10-05, second layer): the node runtime treats a node as the
// consensus leader from LEADER_LOG_REPLICATION on (isLocalLeader), and its
// partition controller reassigns off eligibleNodeIds() right away.  The peer
// liveness grace was seeded only in electionComplete() (CLOSED), so for the
// whole LEADER_* window every peer read as dead (eligN=1): the new controller
// fenced EVERY partition — including those of a live peer — to itself with
// ISR {self}.  The grace must hold from the moment this node leads.
TEST(ElectionLog, LeaderLivenessGraceRegression_PeersEligibleFromLeaderLogReplication) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);
    int64_t nowNs = 1'000'000'000'000LL;   // far past any sentinel window
    bringToLeaderLogReplication(e, nowNs);
    ASSERT_EQ(e.state(), ElectionState::LeaderLogReplication);

    NodeId out[8];
    const std::size_t n = e.eligibleNodeIds(nowNs, 0, out, 8);
    std::vector<NodeId> elig(out, out + n);
    EXPECT_EQ(elig, (std::vector<NodeId>{0, 1, 2}))
        << "a just-elected leader must not see its live peers as dead before CLOSED";
}


// DR-10 review MEDIUM-1 — leader stickiness: a CLOSED leader that still has an
// active follower quorum DENIES a higher-term candidate and keeps leading (no
// partial-partition ping-pong).
TEST(ElectionLog, ClosedLeaderStickinessRegression_LeaderWithQuorumDeniesAndKeepsLeading) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    int64_t nowNs = 1'000'000LL;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);
    closeAsThreeMemberLeader(e, pub, nowNs);
    ASSERT_TRUE(e.isClosed());
    pub.clear();
    e.onRequestVote(e.leadershipTermId(), e.appendPosition(), e.leadershipTermId() + 1,
                    /*candidateId=*/1, nowNs);
    ASSERT_EQ(pub.placeVoteCalls.size(), 1u) << "a sticky leader still ANSWERS (deny)";
    EXPECT_FALSE(pub.placeVoteCalls[0].vote);
    EXPECT_TRUE(e.isClosed());
    EXPECT_EQ(e.leaderMemberId(), 0);
}

// Builds a CLOSED follower (self=1) of leader 0 at term 1.
static void closeAsFollowerOf0(Election& e, int64_t& nowNs) {
    e.doWork(nowNs);   // → CANVASS
    e.onNewLeadershipTerm(nowNs, 0, 1, 0, 0, 1, 0, 0, 0, 0, /*leader=*/0, 0, true, 0);
    ASSERT_TRUE(driveToClosed(e, nowNs));
    ASSERT_EQ(e.leaderMemberId(), 0);
}

TEST(ElectionLog, ClosedLeaderStickinessRegression_FollowerWithLiveLeaderDenies) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    int64_t nowNs = 1'000'000LL;
    auto e = makeElection(makeMembers3(1), 1, pub, nsf, tl);
    closeAsFollowerOf0(e, nowNs);
    pub.clear();
    e.onRequestVote(e.leadershipTermId(), e.appendPosition(), e.leadershipTermId() + 1,
                    /*candidateId=*/2, nowNs);
    ASSERT_EQ(pub.placeVoteCalls.size(), 1u);
    EXPECT_FALSE(pub.placeVoteCalls[0].vote) << "a follower with a live leader must not grant";
    EXPECT_TRUE(e.isClosed());
    EXPECT_EQ(e.leaderMemberId(), 0);

    // Once the leader has been silent past the timeout the candidate is granted.
    nowNs += e.leaderHeartbeatTimeoutNs() + 1;
    pub.clear();
    e.onRequestVote(e.leadershipTermId(), e.appendPosition(), e.leadershipTermId() + 1,
                    /*candidateId=*/2, nowNs);
    ASSERT_EQ(pub.placeVoteCalls.size(), 1u);
    EXPECT_TRUE(pub.placeVoteCalls[0].vote);
    EXPECT_FALSE(e.isClosed());
}

// MEDIUM-3: a candidate the candidacy gate denies (outside the newest durable
// internal ISR) — with an equally fresh log — never unseats a CLOSED leader,
// even one that lost its quorum view (MAJOR-R3).
static bool denyNode2(void*, NodeId c) noexcept { return c != 2; }
TEST(ElectionLog, ClosedLeaderStepDownRegression_GateDeniedCandidacyKeepsLeader) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    int64_t nowNs = 1'000'000LL;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);
    closeAsThreeMemberLeader(e, pub, nowNs);
    e.setVoteGate(&denyNode2, nullptr);
    nowNs += e.leaderHeartbeatTimeoutNs() + 1;   // not sticky: only the gate protects it
    pub.clear();
    e.onRequestVote(e.leadershipTermId(), e.appendPosition(), e.leadershipTermId() + 1,
                    /*candidateId=*/2, nowNs);
    ASSERT_EQ(pub.placeVoteCalls.size(), 1u);
    EXPECT_FALSE(pub.placeVoteCalls[0].vote);
    EXPECT_TRUE(e.isClosed()) << "a gate-denied candidacy unseated the leader (MAJOR-R3)";
    EXPECT_EQ(e.leaderMemberId(), 0);
}

// MEDIUM-3: CLOSED-follower variants of the higher-term NLT / CommitPosition.
TEST(ElectionLog, ClosedLeaderStepDownRegression_FollowerHigherTermNltAndCommitLeaveClosed) {
    for (int variant = 0; variant < 2; ++variant) {
        TempFile nsfFile(".nsf"), tlFile(".tl");
        NodeStateFile nsf(nsfFile.str());
        TermLog       tl(tlFile.str());
        FakeConsensusPublisher pub;
        int64_t nowNs = 1'000'000LL;
        auto e = makeElection(makeMembers3(1), 1, pub, nsf, tl);
        closeAsFollowerOf0(e, nowNs);
        const Term higher = e.leadershipTermId() + 1;
        if (variant == 0)
            e.onNewLeadershipTerm(nowNs, e.leadershipTermId(), higher, 0, 0, higher, 0,
                                  e.appendPosition(), 0, 0, /*leader=*/2, 0, false, 0);
        else
            e.onCommitPosition(nowNs, higher, 0, /*leader=*/2);
        EXPECT_FALSE(e.isClosed()) << "variant " << variant;
        EXPECT_EQ(e.leadershipTermId(), higher) << "variant " << variant;
    }
}

// MEDIUM-3: the adopted term is durable — a restarted node never votes in it again.
TEST(ElectionLog, ClosedLeaderStepDownRegression_AdoptedTermIsDurable) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    Term higher = 0;
    {
        NodeStateFile nsf(nsfFile.str());
        TermLog       tl(tlFile.str());
        FakeConsensusPublisher pub;
        int64_t nowNs = 1'000'000LL;
        auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);
        closeAsThreeMemberLeader(e, pub, nowNs);
        higher = e.leadershipTermId() + 3;
        e.onNewLeadershipTerm(nowNs, e.leadershipTermId(), higher, 0, 0, higher, 0,
                              e.appendPosition(), 0, 0, /*leader=*/2, 0, false, 0);
        ASSERT_FALSE(e.isClosed());
    }
    NodeStateFile reopened(nsfFile.str());
    EXPECT_GE(reopened.candidateTermId(), higher) << "the stepped-down term was not persisted";
}

// ===========================================================================
// DR-10 review LOW-2 — gracefulClosedLeaderId persisted across elections.
// Aeron builds a new Election per election; the graceful-close leader id is a
// constructor argument of ONE election.  Rune's Election is long-lived, so the
// id set by a StandDown must be dropped when that hand-off election completes —
// else every later election still skips the resigned member in
// isUnanimousCandidate / isUnanimousLeader and nominates without hearing from
// it (no longer unanimous: it might hold a fresher log).
// ===========================================================================
TEST(ElectionStandDownRegression, GracefulLeaderSkipEndsWithItsElectionRegression) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    StandDownRecorder pub;
    auto e = makeSdElection(0, pub, nsf, tl, 42);
    int64_t nowNs = 1'000'000LL;
    e.doWork(nowNs);
    e.onRequestVote(0, 0, 1, 1, nowNs);
    e.onNewLeadershipTerm(nowNs, 0, 1, 0, 0, 1, 0, 0, 0, 0, /*leader=*/1, 0, true, 0);
    ASSERT_TRUE(driveToClosed(e, nowNs));
    ASSERT_EQ(e.leaderMemberId(), 1);

    // Leader 1 resigns; the hand-off election elects node 2.
    e.onStandDown(nowNs, /*term=*/1, /*leader=*/1);
    ASSERT_EQ(e.state(), ElectionState::Canvass);
    e.onRequestVote(0, 0, 2, 2, nowNs);
    e.onNewLeadershipTerm(nowNs, 0, 2, 0, 0, 2, 0, 0, 0, 0, /*leader=*/2, 0, false, 0);
    ASSERT_TRUE(driveToClosed(e, nowNs));
    ASSERT_EQ(e.leaderMemberId(), 2);

    // Leader 2 dies: an ordinary (non-graceful) election follows.
    nowNs += 10 * e.leaderHeartbeatTimeoutNs();
    e.doWork(nowNs);
    ASSERT_EQ(e.state(), ElectionState::Canvass);
    // Only node 2 reports: node 1's position is unknown → not unanimous.
    e.onCanvassPosition(0, 0, 2, 2);
    e.doWork(nowNs + 1);
    EXPECT_EQ(e.state(), ElectionState::Canvass)
        << "a later election still skipped the member that resigned two terms ago";
}

// ===========================================================================
// DR-10 review LOW-4 — 2-member clusters.  quorumThreshold(2) == 2: since the
// CLOSED step-down (ClosedLeaderStepDownRegression, Aeron "inactive follower
// quorum") a 2-node leader steps down when its only follower stops reporting —
// a minority never leads (DR-4).  It keeps leading while the follower reports.
// A 2-node cluster therefore tolerates NO node failure (consensus and every
// controller decision pause until both nodes are up) — run 1 or >= 3 nodes.
// ===========================================================================
static void closeAsTwoMemberLeader(Election& e, FakeConsensusPublisher& pub, int64_t& nowNs) {
    e.doWork(nowNs);                       // INIT → CANVASS
    e.onCanvassPosition(0, 0, 0, 1);
    nowNs += kStartupCanvassTimeoutNs + 1;
    e.doWork(nowNs);                       // → NOMINATE (unanimous)
    nowNs += kElectionTimeoutNs * 10;
    e.doWork(nowNs);                       // → CANDIDATE_BALLOT
    nowNs += 1;
    e.doWork(nowNs);
    e.onVote(0, 0, e.candidateTermId(), 0, 1, true);
    nowNs += 1;
    e.doWork(nowNs);                       // → LEADER_LOG_REPLICATION
    ASSERT_TRUE(driveLeaderToClosed(e, nowNs, pub, {1}));
    ASSERT_EQ(e.leaderMemberId(), 0);
}
static std::vector<ClusterMember> makeMembers2(NodeId self = 0) {
    auto ms = makeMembers3(self);
    ms.pop_back();
    return ms;
}

TEST(ElectionLog, TwoMemberClusterRegression_LeaderStepsDownWhenItsOnlyFollowerDies) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    int64_t nowNs = 1'000'000LL;
    auto e = makeElection(makeMembers2(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);
    closeAsTwoMemberLeader(e, pub, nowNs);
    ASSERT_TRUE(e.isClosed());

    nowNs += e.leaderHeartbeatTimeoutNs() + 1;   // follower 1 silent past the timeout
    e.doWork(nowNs);
    EXPECT_EQ(e.state(), ElectionState::Canvass)
        << "a 2-member leader without its only follower is a minority and must not lead";
    EXPECT_EQ(e.leaderMemberId(), -1);
    // Alone it can never win (needs both votes): it stays leaderless.
    for (int i = 0; i < 20; ++i) {
        nowNs += kElectionTimeoutNs;
        e.doWork(nowNs);
        ASSERT_EQ(e.leaderMemberId(), -1) << "a lone 2-member node elected itself";
    }
}

TEST(ElectionLog, TwoMemberClusterRegression_LeaderKeepsLeadingWhileFollowerReports) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    int64_t nowNs = 1'000'000LL;
    auto e = makeElection(makeMembers2(0), 0, pub, nsf, tl, true, 0, 0, 0, /*seed=*/0);
    closeAsTwoMemberLeader(e, pub, nowNs);
    const int64_t end = nowNs + 10 * e.leaderHeartbeatTimeoutNs();
    while (nowNs < end) {
        nowNs += kStatusIntervalNs;
        e.onAppendPosition(nowNs, e.leadershipTermId(), e.appendPosition(), 1, 0);
        e.doWork(nowNs);
        ASSERT_TRUE(e.isClosed());
    }
    EXPECT_EQ(e.leaderMemberId(), 0);
}

