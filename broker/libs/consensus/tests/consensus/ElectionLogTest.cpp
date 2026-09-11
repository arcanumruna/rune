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

// L15: onAppendPosition from self-member is processed (self always counts in quorum).
TEST(ElectionLog, OnAppendPosition_FromSelf_Processed) {
    TempFile nsfFile(".nsf"), tlFile(".tl");
    NodeStateFile nsf(nsfFile.str());
    TermLog       tl(tlFile.str());
    FakeConsensusPublisher pub;
    auto e = makeElection(makeMembers3(0), 0, pub, nsf, tl, true, 0, 0, 0, 0);

    int64_t nowNs = 1'000'000LL;
    bringToLeaderLogReplication(e, nowNs);

    // onAppendPosition doesn't crash or corrupt state for self-member id
    Term ltid = e.leadershipTermId();
    e.onAppendPosition(nowNs, ltid, 0, 0, 0);
    // No assertion on state; just confirm no crash.
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
// GROUP 7: Leader-Completeness safety tests (audit B2/T1)
// ===========================================================================

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
