#include <gtest/gtest.h>
#include <rune/consensus/RaftNode.h>
#include <rune/consensus/PersistentState.h>
#include <rune/consensus/RaftLog.h>
#include <rune/consensus/Types.h>
#include <rune/consensus/Messages.h>

#include <filesystem>
#include <chrono>
#include <algorithm>
#include <set>
#include <atomic>
#include <unistd.h>

using namespace rune::consensus;
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// RAII temp file for PersistentState.
struct TempFile {
    fs::path path;
    explicit TempFile(std::string_view suffix = ".bin") {
        static std::atomic<uint64_t> g_tempSeq{0};
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        auto uniq = std::to_string(::getpid()) + "_" +
                    std::to_string(g_tempSeq.fetch_add(1, std::memory_order_relaxed)) + "_" +
                    std::to_string(ts);
        path = fs::temp_directory_path() /
               ("rune_raft_test_" + uniq + std::string(suffix));
        fs::remove(path);
    }
    ~TempFile() {
        std::error_code ec;
        fs::remove(path, ec);
        fs::remove(fs::path(path.string() + ".tmp"), ec);
    }
    std::string str() const { return path.string(); }
};

/// Build a 3-member ClusterConfig with self = members[0].
static ClusterConfig makeConfig3(NodeId self = 0) {
    return ClusterConfig{ .self = self, .members = {0, 1, 2} };
}

/// Build a 5-member ClusterConfig with self = members[0].
static ClusterConfig makeConfig5(NodeId self = 0) {
    return ClusterConfig{ .self = self, .members = {0, 1, 2, 3, 4} };
}

/// Default Options with deterministic seed.
static Options makeOpts(uint64_t seed = 42) {
    return Options{
        .electionTimeoutBaseNs   = 150'000'000LL,  // 150 ms
        .electionTimeoutJitterNs = 50'000'000LL,   // 50 ms
        .heartbeatIntervalNs     = 50'000'000LL,   // 50 ms
        .rngSeed                 = seed,
    };
}

// ---------------------------------------------------------------------------
// 1. Fresh node is Follower with term from PersistentState and correct self().
// ---------------------------------------------------------------------------
TEST(RaftNode, FreshNodeIsFollowerWithCorrectIdentity) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    EXPECT_EQ(node.role(),        RaftRole::Follower);
    EXPECT_EQ(node.currentTerm(), 0);
    EXPECT_EQ(node.self(),        0);
}

// ---------------------------------------------------------------------------
// 2. Fresh node with non-zero existing PersistentState reflects that term.
// ---------------------------------------------------------------------------
TEST(RaftNode, FreshNodeReflectsExistingPersistedTerm) {
    TempFile tmp;
    {
        PersistentState ps(tmp.str());
        ps.setCurrentTerm(7);
    }
    PersistentState ps2(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps2, log, makeOpts());

    EXPECT_EQ(node.currentTerm(), 7);
    EXPECT_EQ(node.role(),        RaftRole::Follower);
}

// ---------------------------------------------------------------------------
// 3. doWork before election deadline: no messages, role stays Follower.
// ---------------------------------------------------------------------------
TEST(RaftNode, DoWorkBeforeDeadlineNoMessages) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto opts = makeOpts();
    auto node = RaftNode(makeConfig3(), ps, log, opts);

    Outbox out;
    int64_t nowNs = node.electionDeadlineNs() - 1; // just before deadline
    int work = node.doWork(nowNs, out);

    EXPECT_EQ(work, 0);
    EXPECT_TRUE(out.empty());
    EXPECT_EQ(node.role(), RaftRole::Follower);
}

// ---------------------------------------------------------------------------
// 4. doWork at election deadline: becomes Candidate, term incremented.
// ---------------------------------------------------------------------------
TEST(RaftNode, DoWorkAtDeadlineBecomesCandidateIncrementsTerm) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig3(), ps, log, makeOpts());

    Outbox out;
    int64_t nowNs = node.electionDeadlineNs(); // exactly at deadline
    node.doWork(nowNs, out);

    EXPECT_EQ(node.role(),        RaftRole::Candidate);
    EXPECT_EQ(node.currentTerm(), 1); // was 0, incremented by 1
}

// ---------------------------------------------------------------------------
// 5. Term increment is PERSISTED (reopen PersistentState after election).
// ---------------------------------------------------------------------------
TEST(RaftNode, ElectionPersistsTermAcrossReopen) {
    TempFile tmp;
    {
        PersistentState ps(tmp.str());
        RaftLog log;
        auto node = RaftNode(makeConfig3(), ps, log, makeOpts());

        Outbox out;
        node.doWork(node.electionDeadlineNs(), out);
        EXPECT_EQ(ps.currentTerm(), 1);
    }
    // Reopen and verify persistence.
    PersistentState ps2(tmp.str());
    EXPECT_EQ(ps2.currentTerm(), 1);
}

// ---------------------------------------------------------------------------
// 6. votedFor is set to self and PERSISTED after becoming Candidate.
// ---------------------------------------------------------------------------
TEST(RaftNode, ElectionVotedForSelfPersisted) {
    TempFile tmp;
    const NodeId selfId = 2;
    {
        PersistentState ps(tmp.str());
        RaftLog log;
        auto node = RaftNode(makeConfig3(selfId), ps, log, makeOpts());

        Outbox out;
        node.doWork(node.electionDeadlineNs(), out);
        EXPECT_EQ(ps.votedFor(), selfId);
    }
    // Persisted across reopen.
    PersistentState ps2(tmp.str());
    EXPECT_EQ(ps2.votedFor(), selfId);
}

// ---------------------------------------------------------------------------
// 7. Candidate emits exactly (members-1) RequestVote messages, one per peer.
// ---------------------------------------------------------------------------
TEST(RaftNode, CandidateEmitsOneRequestVotePerPeer_3Members) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    node.doWork(node.electionDeadlineNs(), out);

    ASSERT_EQ(out.size(), 2u);  // 3-member cluster, 2 peers

    std::set<NodeId> targets;
    for (auto& m : out) {
        EXPECT_TRUE(std::holds_alternative<RequestVote>(m.msg));
        targets.insert(m.to);
    }
    // Must cover peers 1 and 2 (not self=0).
    EXPECT_EQ(targets.count(0), 0u); // not to self
    EXPECT_EQ(targets.count(1), 1u);
    EXPECT_EQ(targets.count(2), 1u);
}

// ---------------------------------------------------------------------------
// 8. RequestVote fields are correct for empty log (lastLogIndex=0, lastLogTerm=0).
// ---------------------------------------------------------------------------
TEST(RaftNode, RequestVoteFieldsCorrectEmptyLog) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    node.doWork(node.electionDeadlineNs(), out);

    for (auto& m : out) {
        auto& rv = std::get<RequestVote>(m.msg);
        EXPECT_EQ(rv.candidateTerm,  1);
        EXPECT_EQ(rv.candidateId,    0);
        EXPECT_EQ(rv.lastLogIndex,   0);
        EXPECT_EQ(rv.lastLogTerm,    0);
    }
}

// ---------------------------------------------------------------------------
// 9. RequestVote fields are correct after appending log entries.
// ---------------------------------------------------------------------------
TEST(RaftNode, RequestVoteFieldsCorrectWithLogEntries) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    log.append(1, "cmd1");
    log.append(1, "cmd2");
    log.append(2, "cmd3");

    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());
    Outbox out;
    node.doWork(node.electionDeadlineNs(), out);

    for (auto& m : out) {
        auto& rv = std::get<RequestVote>(m.msg);
        EXPECT_EQ(rv.candidateTerm,  1);
        EXPECT_EQ(rv.lastLogIndex,   3); // lastIndex()
        EXPECT_EQ(rv.lastLogTerm,    2); // lastTerm()
    }
}

// ---------------------------------------------------------------------------
// 10. Candidate whose election times out again increments term a second time.
// ---------------------------------------------------------------------------
TEST(RaftNode, CandidateTimeoutStartsFreshElectionIncrementsTerm) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    // First election.
    node.doWork(node.electionDeadlineNs(), out);
    EXPECT_EQ(node.currentTerm(), 1);
    EXPECT_EQ(node.role(), RaftRole::Candidate);

    out.clear();
    // Second election (deadline already passed for the new candidate deadline).
    node.doWork(node.electionDeadlineNs(), out);
    EXPECT_EQ(node.currentTerm(), 2);
    EXPECT_EQ(node.role(), RaftRole::Candidate);
    EXPECT_EQ(out.size(), 2u); // still 2 peers in 3-member config
}

// ---------------------------------------------------------------------------
// 11. Randomized election deadline lies in [base, base+jitter].
// ---------------------------------------------------------------------------
TEST(RaftNode, ElectionDeadlineWithinExpectedRange) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto opts = makeOpts(99);
    const int64_t base   = opts.electionTimeoutBaseNs;
    const int64_t jitter = opts.electionTimeoutJitterNs;
    const int64_t nowNs  = 1'000'000'000LL; // 1 second

    auto node = RaftNode(makeConfig3(), ps, log, opts);
    // Force a becomeFollower to reset the deadline at a known nowNs.
    node.becomeFollower(0, nowNs);

    int64_t dl = node.electionDeadlineNs();
    EXPECT_GE(dl, nowNs + base);
    EXPECT_LE(dl, nowNs + base + jitter);
}

// ---------------------------------------------------------------------------
// 12. Two nodes with different RNG seeds get different deadlines (determinism).
// ---------------------------------------------------------------------------
TEST(RaftNode, DifferentSeedsProduceDifferentDeadlines) {
    TempFile tmp1, tmp2;
    PersistentState ps1(tmp1.str()), ps2(tmp2.str());
    RaftLog log1, log2;
    const int64_t nowNs = 5'000'000'000LL;

    auto opts1 = makeOpts(1);
    auto opts2 = makeOpts(2);

    auto node1 = RaftNode(makeConfig3(0), ps1, log1, opts1);
    auto node2 = RaftNode(makeConfig3(1), ps2, log2, opts2);

    node1.becomeFollower(0, nowNs);
    node2.becomeFollower(0, nowNs);

    EXPECT_NE(node1.electionDeadlineNs(), node2.electionDeadlineNs());
}

// ---------------------------------------------------------------------------
// 13. becomeLeader: role becomes Leader; next doWork emits heartbeats.
// ---------------------------------------------------------------------------
TEST(RaftNode, BecomeLeaderThenDoWorkEmitsHeartbeats) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    // First become candidate to get a valid term.
    Outbox out;
    node.doWork(node.electionDeadlineNs(), out);
    ASSERT_EQ(node.role(), RaftRole::Candidate);
    out.clear();

    // Become leader.
    int64_t nowNs = node.electionDeadlineNs() + 1'000;
    node.becomeLeader(nowNs, out);
    EXPECT_EQ(node.role(), RaftRole::Leader);
    // becomeLeader emits immediate heartbeats to all peers.
    EXPECT_EQ(out.size(), 2u); // 3-member, 2 peers
    for (auto& m : out) {
        EXPECT_TRUE(std::holds_alternative<AppendEntries>(m.msg));
        auto& ae = std::get<AppendEntries>(m.msg);
        EXPECT_EQ(ae.term,     node.currentTerm());
        EXPECT_EQ(ae.leaderId, 0);
        EXPECT_TRUE(ae.entries.empty());
    }
}

// ---------------------------------------------------------------------------
// 14. Heartbeat fields: prevLogIndex/prevLogTerm/leaderCommit correct.
// ---------------------------------------------------------------------------
TEST(RaftNode, HeartbeatFieldsCorrectWithLogAndCommit) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    log.append(1, "cmd1");
    log.append(1, "cmd2");
    log.setCommitIndex(1);

    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());
    Outbox out;
    // Elect.
    node.doWork(node.electionDeadlineNs(), out);
    out.clear();
    node.becomeLeader(1'000'000LL, out);

    for (auto& m : out) {
        auto& ae = std::get<AppendEntries>(m.msg);
        EXPECT_EQ(ae.prevLogIndex, 2); // log.lastIndex()
        EXPECT_EQ(ae.prevLogTerm,  1); // log.lastTerm()
        EXPECT_EQ(ae.leaderCommit, 1); // log.commitIndex()
        EXPECT_TRUE(ae.entries.empty());
    }
}

// ---------------------------------------------------------------------------
// 15. doWork before heartbeat interval elapses emits nothing.
// ---------------------------------------------------------------------------
TEST(RaftNode, DoWorkBeforeHeartbeatIntervalEmitsNothing) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto opts = makeOpts();
    auto node = RaftNode(makeConfig3(0), ps, log, opts);

    Outbox out;
    node.doWork(node.electionDeadlineNs(), out);
    out.clear();

    int64_t leaderNow = node.electionDeadlineNs() + 1'000;
    node.becomeLeader(leaderNow, out);
    out.clear();

    // Before heartbeat interval elapses — should emit nothing.
    int64_t tooEarlyNs = leaderNow + opts.heartbeatIntervalNs - 1;
    int work = node.doWork(tooEarlyNs, out);
    EXPECT_EQ(out.size(), 0u);
    EXPECT_EQ(work, 0);
}

// ---------------------------------------------------------------------------
// 16. doWork after heartbeat interval elapses emits another round.
// ---------------------------------------------------------------------------
TEST(RaftNode, DoWorkAfterHeartbeatIntervalEmitsAgain) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto opts = makeOpts();
    auto node = RaftNode(makeConfig3(0), ps, log, opts);

    Outbox out;
    node.doWork(node.electionDeadlineNs(), out);
    out.clear();

    int64_t leaderNow = node.electionDeadlineNs() + 1'000;
    node.becomeLeader(leaderNow, out);
    out.clear();

    // Advance clock beyond heartbeat interval.
    int64_t laterNs = leaderNow + opts.heartbeatIntervalNs + 1;
    node.doWork(laterNs, out);

    EXPECT_EQ(out.size(), 2u); // 2 peers in 3-member config
    for (auto& m : out) {
        EXPECT_TRUE(std::holds_alternative<AppendEntries>(m.msg));
    }
}

// ---------------------------------------------------------------------------
// 17. observeTerm(higher): steps down to Follower, adopts+persists new term.
// ---------------------------------------------------------------------------
TEST(RaftNode, ObserveHigherTermStepsDownAndPersistsTerm) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    // Start election -> Candidate term=1.
    node.doWork(node.electionDeadlineNs(), out);
    ASSERT_EQ(node.currentTerm(), 1);

    // Observe a higher term.
    node.observeTerm(5, node.electionDeadlineNs() + 1'000);
    EXPECT_EQ(node.role(),        RaftRole::Follower);
    EXPECT_EQ(node.currentTerm(), 5);
    EXPECT_EQ(ps.currentTerm(),   5);
    EXPECT_EQ(ps.votedFor(),      kNoVote); // setCurrentTerm resets votedFor
}

// ---------------------------------------------------------------------------
// 18. observeTerm(lower-or-equal): no change.
// ---------------------------------------------------------------------------
TEST(RaftNode, ObserveLowerOrEqualTermNoChange) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    node.doWork(node.electionDeadlineNs(), out);
    ASSERT_EQ(node.currentTerm(), 1);
    ASSERT_EQ(node.role(), RaftRole::Candidate);

    // Same term.
    node.observeTerm(1, 0);
    EXPECT_EQ(node.role(),        RaftRole::Candidate);
    EXPECT_EQ(node.currentTerm(), 1);

    // Lower term.
    node.observeTerm(0, 0);
    EXPECT_EQ(node.role(),        RaftRole::Candidate);
    EXPECT_EQ(node.currentTerm(), 1);
}

// ---------------------------------------------------------------------------
// 19. Outbox reuse: pass the same Outbox across two doWork ticks.
// ---------------------------------------------------------------------------
TEST(RaftNode, OutboxReuse_CallerClearsBetweenTicks) {
    TempFile tmp1, tmp2;
    PersistentState ps1(tmp1.str()), ps2(tmp2.str());
    RaftLog log1, log2;
    auto node1 = RaftNode(makeConfig3(0), ps1, log1, makeOpts(1));
    auto node2 = RaftNode(makeConfig3(1), ps2, log2, makeOpts(2));

    Outbox sharedOut;

    // First tick: node1 election.
    node1.doWork(node1.electionDeadlineNs(), sharedOut);
    EXPECT_EQ(sharedOut.size(), 2u);

    sharedOut.clear(); // caller clears

    // Second tick: node2 election into the same Outbox.
    node2.doWork(node2.electionDeadlineNs(), sharedOut);
    EXPECT_EQ(sharedOut.size(), 2u); // still 2, not 4

    // All messages should be from node2 (candidateId=1).
    for (auto& m : sharedOut) {
        auto& rv = std::get<RequestVote>(m.msg);
        EXPECT_EQ(rv.candidateId, 1);
    }
}

// ---------------------------------------------------------------------------
// 20. 5-member config: candidate emits 4 RequestVotes, majority()==3.
// ---------------------------------------------------------------------------
TEST(RaftNode, FiveMemberConfig_FourRequestVotes_MajorityThree) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig5(0), ps, log, makeOpts());

    EXPECT_EQ(makeConfig5().majority(), 3);

    Outbox out;
    node.doWork(node.electionDeadlineNs(), out);

    EXPECT_EQ(out.size(), 4u); // 5 members - 1 self = 4 peers
    std::set<NodeId> targets;
    for (auto& m : out) {
        EXPECT_TRUE(std::holds_alternative<RequestVote>(m.msg));
        targets.insert(m.to);
    }
    EXPECT_EQ(targets.count(0), 0u); // not to self
    EXPECT_EQ(targets.size(),   4u); // one per peer
}

// ---------------------------------------------------------------------------
// 21. becomeFollower with equal term does NOT reset votedFor.
// ---------------------------------------------------------------------------
TEST(RaftNode, BecomeFollowerSameTermDoesNotResetVotedFor) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    // Become candidate (sets votedFor=self, term=1).
    Outbox out;
    node.doWork(node.electionDeadlineNs(), out);
    ASSERT_EQ(ps.votedFor(), 0);

    // becomeFollower with the SAME term — votedFor must be preserved.
    node.becomeFollower(1, node.electionDeadlineNs() + 1'000);
    EXPECT_EQ(ps.currentTerm(), 1);
    EXPECT_EQ(ps.votedFor(),    0); // unchanged
    EXPECT_EQ(node.role(),      RaftRole::Follower);
}

// ---------------------------------------------------------------------------
// 22. Successive elections accumulate term correctly (no gap).
// ---------------------------------------------------------------------------
TEST(RaftNode, SuccessiveElectionsIncrementTermMonotonically) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    for (int i = 1; i <= 5; ++i) {
        out.clear();
        node.doWork(node.electionDeadlineNs(), out);
        EXPECT_EQ(node.currentTerm(), i);
        EXPECT_EQ(node.role(), RaftRole::Candidate);
    }
}

// ---------------------------------------------------------------------------
// 23. Randomized deadline is deterministic for the same seed across resets.
// ---------------------------------------------------------------------------
TEST(RaftNode, SameSeedProducesDeterministicDeadlineSequence) {
    // Two nodes constructed with the same seed should produce the same
    // sequence of deadlines across multiple becomeFollower resets.
    TempFile tmp1, tmp2;
    PersistentState ps1(tmp1.str()), ps2(tmp2.str());
    RaftLog log1, log2;
    const uint64_t seed = 7777;
    auto node1 = RaftNode(makeConfig3(0), ps1, log1, makeOpts(seed));
    auto node2 = RaftNode(makeConfig3(0), ps2, log2, makeOpts(seed));

    // Both should have the same initial deadline (computed from nowNs=0).
    EXPECT_EQ(node1.electionDeadlineNs(), node2.electionDeadlineNs());

    // Reset several times; deadlines should still match.
    for (int64_t t = 1'000'000LL; t <= 5'000'000LL; t += 1'000'000LL) {
        node1.becomeFollower(0, t);
        node2.becomeFollower(0, t);
        EXPECT_EQ(node1.electionDeadlineNs(), node2.electionDeadlineNs());
    }
}

// ---------------------------------------------------------------------------
// 24. Leader heartbeats: correct targets (all peers, not self).
// ---------------------------------------------------------------------------
TEST(RaftNode, HeartbeatTargetsAllPeersNotSelf) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig5(2), ps, log, makeOpts()); // self=2

    Outbox out;
    node.doWork(node.electionDeadlineNs(), out);
    out.clear();
    node.becomeLeader(node.electionDeadlineNs() + 1'000, out);

    EXPECT_EQ(out.size(), 4u); // 5 members, 4 peers
    std::set<NodeId> targets;
    for (auto& m : out) { targets.insert(m.to); }
    EXPECT_EQ(targets.count(2), 0u); // not to self
    EXPECT_EQ(targets.size(),   4u);
}

// ---------------------------------------------------------------------------
// 25. Outbox not cleared by node: appending across two consecutive ticks
//     without caller clearing accumulates both rounds.
// ---------------------------------------------------------------------------
TEST(RaftNode, OutboxAccumulatesAcrossTicksIfCallerDoesNotClear) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto opts = makeOpts();
    auto node = RaftNode(makeConfig3(0), ps, log, opts);

    // Elect to become candidate.
    Outbox out;
    node.doWork(node.electionDeadlineNs(), out);
    // out now has 2 RequestVotes. Do NOT clear.

    // Trigger a second election.
    size_t before = out.size();
    node.doWork(node.electionDeadlineNs(), out); // second election
    EXPECT_GT(out.size(), before); // messages accumulated
}
