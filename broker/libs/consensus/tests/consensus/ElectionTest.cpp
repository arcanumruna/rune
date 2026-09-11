// ElectionTest.cpp — P2-d: handleRequestVote + handleRequestVoteResult tests.
// Deterministic: manual nano clock, fixed seed, PersistentState on unique temp paths.

#include <gtest/gtest.h>
#include <rune/consensus/RaftNode.h>
#include <rune/consensus/PersistentState.h>
#include <rune/consensus/RaftLog.h>
#include <rune/consensus/Types.h>
#include <rune/consensus/Messages.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <set>
#include <unistd.h>

using namespace rune::consensus;
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

struct TempFile {
    fs::path path;
    explicit TempFile(std::string_view suffix = ".bin") {
        static std::atomic<uint64_t> g_tempSeq{0};
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        auto uniq = std::to_string(::getpid()) + "_" +
                    std::to_string(g_tempSeq.fetch_add(1, std::memory_order_relaxed)) + "_" +
                    std::to_string(ts);
        path = fs::temp_directory_path() /
               ("rune_elec_test_" + uniq + std::string(suffix));
        fs::remove(path);
    }
    ~TempFile() {
        std::error_code ec;
        fs::remove(path, ec);
        fs::remove(fs::path(path.string() + ".tmp"), ec);
    }
    std::string str() const { return path.string(); }
};

static ClusterConfig makeConfig3(NodeId self = 0) {
    return ClusterConfig{ .self = self, .members = {0, 1, 2} };
}

static ClusterConfig makeConfig5(NodeId self = 0) {
    return ClusterConfig{ .self = self, .members = {0, 1, 2, 3, 4} };
}

static Options makeOpts(uint64_t seed = 42) {
    return Options{
        .electionTimeoutBaseNs   = 150'000'000LL,
        .electionTimeoutJitterNs =  50'000'000LL,
        .heartbeatIntervalNs     =  50'000'000LL,
        .rngSeed                 = seed,
    };
}

/// Advance node to Candidate.
static void electToCandidate(RaftNode& node, Outbox& out) {
    out.clear();
    node.doWork(node.electionDeadlineNs(), out); // triggers becomeCandidate
    out.clear();
}

/// Count RequestVoteResult messages in the outbox with voteGranted==true.
static int countGranted(const Outbox& out) {
    int n = 0;
    for (auto& m : out) {
        if (auto* r = std::get_if<RequestVoteResult>(&m.msg)) {
            if (r->voteGranted) ++n;
        }
    }
    return n;
}

static bool hasLeaderHeartbeat(const Outbox& out) {
    for (auto& m : out) {
        if (std::holds_alternative<AppendEntries>(m.msg)) return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// 1. Lower candidateTerm → deny, reply term == currentTerm
// ---------------------------------------------------------------------------
TEST(Election, LowerCandidateTerm_Deny) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.setCurrentTerm(5);
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    RequestVote rv{ .candidateTerm = 4, .candidateId = 1,
                    .lastLogIndex = 0, .lastLogTerm = 0 };
    auto res = node.handleRequestVote(rv, 1'000'000LL, out);

    EXPECT_FALSE(res.voteGranted);
    EXPECT_EQ(res.term, 5);
    EXPECT_EQ(node.role(), RaftRole::Follower);
    EXPECT_EQ(ps.votedFor(), kNoVote);
}

// ---------------------------------------------------------------------------
// 2. Higher candidateTerm → step down then grant (if log up-to-date)
// ---------------------------------------------------------------------------
TEST(Election, HigherCandidateTerm_StepDownAndGrant) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.setCurrentTerm(3);
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    RequestVote rv{ .candidateTerm = 7, .candidateId = 2,
                    .lastLogIndex = 0, .lastLogTerm = 0 };
    auto res = node.handleRequestVote(rv, 1'000'000LL, out);

    EXPECT_TRUE(res.voteGranted);
    EXPECT_EQ(res.term, 7);
    EXPECT_EQ(node.role(), RaftRole::Follower);  // stepped down
    EXPECT_EQ(ps.currentTerm(), 7);
    EXPECT_EQ(ps.votedFor(), 2);
}

// ---------------------------------------------------------------------------
// 3. Already voted for a DIFFERENT candidate in same term → deny
// ---------------------------------------------------------------------------
TEST(Election, AlreadyVotedOtherCandidate_SameTerm_Deny) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.update(4, 1); // voted for node 1 in term 4
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    RequestVote rv{ .candidateTerm = 4, .candidateId = 2,
                    .lastLogIndex = 0, .lastLogTerm = 0 };
    auto res = node.handleRequestVote(rv, 1'000'000LL, out);

    EXPECT_FALSE(res.voteGranted);
    EXPECT_EQ(ps.votedFor(), 1); // unchanged
}

// ---------------------------------------------------------------------------
// 4. Already voted for the SAME candidate in same term → grant (idempotent)
// ---------------------------------------------------------------------------
TEST(Election, AlreadyVotedSameCandidate_SameTerm_Grant) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.update(4, 2); // voted for node 2 in term 4
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    RequestVote rv{ .candidateTerm = 4, .candidateId = 2,
                    .lastLogIndex = 0, .lastLogTerm = 0 };
    auto res = node.handleRequestVote(rv, 1'000'000LL, out);

    EXPECT_TRUE(res.voteGranted);
    EXPECT_EQ(ps.votedFor(), 2); // still 2
}

// ---------------------------------------------------------------------------
// 5. Up-to-date check: candidate lastLogTerm < mine → deny
// ---------------------------------------------------------------------------
TEST(Election, UpToDate_CandidateTermLower_Deny) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.setCurrentTerm(3);
    RaftLog log;
    log.append(3, "cmd1"); // lastTerm=3, lastIndex=1
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    RequestVote rv{ .candidateTerm = 3, .candidateId = 1,
                    .lastLogIndex = 5, .lastLogTerm = 2 }; // term < mine(3)
    auto res = node.handleRequestVote(rv, 1'000'000LL, out);

    EXPECT_FALSE(res.voteGranted);
    EXPECT_EQ(ps.votedFor(), kNoVote);
}

// ---------------------------------------------------------------------------
// 6. Up-to-date check: candidate lastLogTerm == mine, index < mine → deny
// ---------------------------------------------------------------------------
TEST(Election, UpToDate_SameTermShorterLog_Deny) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.setCurrentTerm(3);
    RaftLog log;
    log.append(3, "cmd1");
    log.append(3, "cmd2"); // lastTerm=3, lastIndex=2
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    RequestVote rv{ .candidateTerm = 3, .candidateId = 1,
                    .lastLogIndex = 1, .lastLogTerm = 3 }; // index 1 < mine 2
    auto res = node.handleRequestVote(rv, 1'000'000LL, out);

    EXPECT_FALSE(res.voteGranted);
    EXPECT_EQ(ps.votedFor(), kNoVote);
}

// ---------------------------------------------------------------------------
// 7. Up-to-date check: candidate lastLogTerm > mine → grant
// ---------------------------------------------------------------------------
TEST(Election, UpToDate_HigherLastLogTerm_Grant) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.setCurrentTerm(4);
    RaftLog log;
    log.append(2, "cmd1"); // lastTerm=2, lastIndex=1
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    RequestVote rv{ .candidateTerm = 4, .candidateId = 1,
                    .lastLogIndex = 1, .lastLogTerm = 3 }; // term 3 > mine 2
    auto res = node.handleRequestVote(rv, 1'000'000LL, out);

    EXPECT_TRUE(res.voteGranted);
    EXPECT_EQ(ps.votedFor(), 1);
}

// ---------------------------------------------------------------------------
// 8. Up-to-date check: same lastLogTerm, index >= mine → grant
// ---------------------------------------------------------------------------
TEST(Election, UpToDate_SameTermEqualIndex_Grant) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.setCurrentTerm(3);
    RaftLog log;
    log.append(3, "cmd1"); // lastTerm=3, lastIndex=1
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    RequestVote rv{ .candidateTerm = 3, .candidateId = 1,
                    .lastLogIndex = 1, .lastLogTerm = 3 }; // equal
    auto res = node.handleRequestVote(rv, 1'000'000LL, out);

    EXPECT_TRUE(res.voteGranted);
    EXPECT_EQ(ps.votedFor(), 1);
}

// ---------------------------------------------------------------------------
// 9. Up-to-date check: same lastLogTerm, index > mine → grant
// ---------------------------------------------------------------------------
TEST(Election, UpToDate_SameTermLongerLog_Grant) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.setCurrentTerm(3);
    RaftLog log;
    log.append(3, "cmd1"); // lastTerm=3, lastIndex=1
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    RequestVote rv{ .candidateTerm = 3, .candidateId = 1,
                    .lastLogIndex = 3, .lastLogTerm = 3 }; // longer
    auto res = node.handleRequestVote(rv, 1'000'000LL, out);

    EXPECT_TRUE(res.voteGranted);
    EXPECT_EQ(ps.votedFor(), 1);
}

// ---------------------------------------------------------------------------
// 10. Valid grant: outbox gets exactly one RequestVoteResult with correct fields
// ---------------------------------------------------------------------------
TEST(Election, ValidGrant_OutboxContainsCorrectResult) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.setCurrentTerm(2);
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    RequestVote rv{ .candidateTerm = 2, .candidateId = 1,
                    .lastLogIndex = 0, .lastLogTerm = 0 };
    node.handleRequestVote(rv, 1'000'000LL, out);

    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0].to, 1);
    auto& r = std::get<RequestVoteResult>(out[0].msg);
    EXPECT_TRUE(r.voteGranted);
    EXPECT_EQ(r.term, 2);
}

// ---------------------------------------------------------------------------
// 11. Denied vote: outbox still gets a result (voteGranted=false)
// ---------------------------------------------------------------------------
TEST(Election, Deny_OutboxContainsDenialResult) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.setCurrentTerm(5);
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    RequestVote rv{ .candidateTerm = 3, .candidateId = 1,
                    .lastLogIndex = 0, .lastLogTerm = 0 };
    node.handleRequestVote(rv, 1'000'000LL, out);

    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0].to, 1);
    auto& r = std::get<RequestVoteResult>(out[0].msg);
    EXPECT_FALSE(r.voteGranted);
    EXPECT_EQ(r.term, 5); // our current term
}

// ---------------------------------------------------------------------------
// 12. Persistence: after a grant, reopen PersistentState → votedFor+term correct
// ---------------------------------------------------------------------------
TEST(Election, GrantPersistsVoteAcrossReopen) {
    TempFile tmp;
    {
        PersistentState ps(tmp.str());
        ps.setCurrentTerm(6);
        RaftLog log;
        auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

        Outbox out;
        RequestVote rv{ .candidateTerm = 6, .candidateId = 2,
                        .lastLogIndex = 0, .lastLogTerm = 0 };
        auto res = node.handleRequestVote(rv, 1'000'000LL, out);
        ASSERT_TRUE(res.voteGranted);
    }
    // Reopen: persistence must have survived
    PersistentState ps2(tmp.str());
    EXPECT_EQ(ps2.currentTerm(), 6);
    EXPECT_EQ(ps2.votedFor(), 2);
}

// ---------------------------------------------------------------------------
// 13. One vote per term: grant to A, then B in same term → deny, votedFor stays A
// ---------------------------------------------------------------------------
TEST(Election, OneVotePerTerm_SecondCandidateDenied) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.setCurrentTerm(5);
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    RequestVote rvA{ .candidateTerm = 5, .candidateId = 1,
                     .lastLogIndex = 0, .lastLogTerm = 0 };
    auto resA = node.handleRequestVote(rvA, 1'000'000LL, out);
    ASSERT_TRUE(resA.voteGranted);
    EXPECT_EQ(ps.votedFor(), 1);

    out.clear();
    RequestVote rvB{ .candidateTerm = 5, .candidateId = 2,
                     .lastLogIndex = 0, .lastLogTerm = 0 };
    auto resB = node.handleRequestVote(rvB, 2'000'000LL, out);
    EXPECT_FALSE(resB.voteGranted);
    EXPECT_EQ(ps.votedFor(), 1); // still A
}

// ---------------------------------------------------------------------------
// 13b. Dueling candidates: node becomes Candidate, denies same-term RequestVote
//      from another candidate (votedFor==self != candidateId).
// ---------------------------------------------------------------------------
TEST(Election, CandidateReceivesSameTermRequestVoteDenies) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    electToCandidate(node, out);
    ASSERT_EQ(node.role(), RaftRole::Candidate);
    Term candidateTerm = node.currentTerm();
    EXPECT_EQ(ps.votedFor(), 0); // self-vote

    // A same-term RequestVote from candidate 1, with its log that would otherwise be grantable
    out.clear();
    RequestVote rv{ .candidateTerm = candidateTerm, .candidateId = 1,
                    .lastLogIndex = 0, .lastLogTerm = 0 };
    auto res = node.handleRequestVote(rv, 2'000'000LL, out);

    EXPECT_FALSE(res.voteGranted); // denied: votedFor==0 (self) != 1
    EXPECT_EQ(res.term, candidateTerm);
    EXPECT_EQ(node.role(), RaftRole::Candidate); // stays Candidate
    EXPECT_EQ(ps.votedFor(), 0); // self-vote unchanged
}

// ---------------------------------------------------------------------------
// 14. Election deadline resets on grant, NOT on denial
// ---------------------------------------------------------------------------
TEST(Election, ElectionDeadlineResetOnGrant_NotOnDeny) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.setCurrentTerm(3);
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    int64_t before = node.electionDeadlineNs();

    // Grant: deadline should move forward from nowNs=large
    Outbox out;
    int64_t grantNs = 1'000'000'000LL; // 1s
    RequestVote rvGrant{ .candidateTerm = 3, .candidateId = 1,
                         .lastLogIndex = 0, .lastLogTerm = 0 };
    node.handleRequestVote(rvGrant, grantNs, out);
    int64_t afterGrant = node.electionDeadlineNs();
    // deadline must be at least grantNs + base
    EXPECT_GE(afterGrant, grantNs + makeOpts().electionTimeoutBaseNs);
    EXPECT_GT(afterGrant, before);

    // Deny: deadline should NOT change
    int64_t deadlineBeforeDeny = node.electionDeadlineNs();
    out.clear();
    // votedFor is already 1 so a vote for 2 gets denied
    RequestVote rvDeny{ .candidateTerm = 3, .candidateId = 2,
                        .lastLogIndex = 0, .lastLogTerm = 0 };
    node.handleRequestVote(rvDeny, grantNs + 1, out);
    EXPECT_EQ(node.electionDeadlineNs(), deadlineBeforeDeny);
}

// ---------------------------------------------------------------------------
// 15. Vote tally 3-member: self + 1 granted result → becomeLeader
// ---------------------------------------------------------------------------
TEST(Election, VoteTally_3Member_OneExternalGrant_BecomesLeader) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    electToCandidate(node, out);
    ASSERT_EQ(node.role(), RaftRole::Candidate);
    // Self-vote already counted (1/3). Need 2 more for majority=2.
    // Actually majority=2; self=1, so one external grant suffices.
    EXPECT_EQ(node.grantedVoteCount(), 1); // just self

    RequestVoteResult res{ .term = node.currentTerm(), .voteGranted = true };
    node.handleRequestVoteResult(1, res, 1'000'000LL, out);

    EXPECT_EQ(node.role(), RaftRole::Leader);
    EXPECT_TRUE(hasLeaderHeartbeat(out));
}

// ---------------------------------------------------------------------------
// 16. Vote tally 5-member: needs self+2; one grant → still Candidate
// ---------------------------------------------------------------------------
TEST(Election, VoteTally_5Member_OneGrant_StillCandidate) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig5(0), ps, log, makeOpts());

    Outbox out;
    electToCandidate(node, out);
    ASSERT_EQ(node.role(), RaftRole::Candidate);

    RequestVoteResult res{ .term = node.currentTerm(), .voteGranted = true };
    node.handleRequestVoteResult(1, res, 1'000'000LL, out);

    EXPECT_EQ(node.role(), RaftRole::Candidate); // 5-member majority=3; self + 1 grant = 2 < 3, still Candidate
    EXPECT_EQ(node.grantedVoteCount(), 2);
}

// ---------------------------------------------------------------------------
// 17. Vote tally 5-member: self+2 grants → Leader
// ---------------------------------------------------------------------------
TEST(Election, VoteTally_5Member_TwoGrants_BecomesLeader) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig5(0), ps, log, makeOpts());

    Outbox out;
    electToCandidate(node, out);
    ASSERT_EQ(node.role(), RaftRole::Candidate);
    Term term = node.currentTerm();

    RequestVoteResult res{ .term = term, .voteGranted = true };
    node.handleRequestVoteResult(1, res, 1'000'000LL, out);
    EXPECT_EQ(node.role(), RaftRole::Candidate);

    node.handleRequestVoteResult(2, res, 1'000'001LL, out);
    EXPECT_EQ(node.role(), RaftRole::Leader);
    EXPECT_TRUE(hasLeaderHeartbeat(out));
}

// ---------------------------------------------------------------------------
// 18. Duplicate grant from same node counts once (no double-promotion)
// ---------------------------------------------------------------------------
TEST(Election, DuplicateGrantFromSameNode_CountedOnce) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig5(0), ps, log, makeOpts());

    Outbox out;
    electToCandidate(node, out);
    ASSERT_EQ(node.role(), RaftRole::Candidate);
    Term term = node.currentTerm();

    RequestVoteResult res{ .term = term, .voteGranted = true };
    node.handleRequestVoteResult(1, res, 1'000'000LL, out);
    EXPECT_EQ(node.grantedVoteCount(), 2);
    // Send duplicate from same node (1)
    node.handleRequestVoteResult(1, res, 1'000'001LL, out);
    EXPECT_EQ(node.grantedVoteCount(), 2); // still 2, not 3
    EXPECT_EQ(node.role(), RaftRole::Candidate);
}

// ---------------------------------------------------------------------------
// 19. Stale result (res.term < currentTerm) is ignored
// ---------------------------------------------------------------------------
TEST(Election, StaleResult_LowerTerm_Ignored) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    electToCandidate(node, out);
    ASSERT_EQ(node.role(), RaftRole::Candidate);
    Term term = node.currentTerm();

    RequestVoteResult stale{ .term = term - 1, .voteGranted = true };
    node.handleRequestVoteResult(1, stale, 1'000'000LL, out);

    // The result has a lower term and should be ignored.
    // (observeTerm won't step down for a lower term; the stale check fires
    // because res.term != currentTerm)
    EXPECT_EQ(node.role(), RaftRole::Candidate);
    EXPECT_EQ(node.grantedVoteCount(), 1); // only self
}

// ---------------------------------------------------------------------------
// 20. Result with res.term > currentTerm → step down to Follower
// ---------------------------------------------------------------------------
TEST(Election, ResultHigherTerm_StepDown) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    electToCandidate(node, out);
    ASSERT_EQ(node.role(), RaftRole::Candidate);
    Term term = node.currentTerm();

    RequestVoteResult higher{ .term = term + 5, .voteGranted = true };
    node.handleRequestVoteResult(1, higher, 1'000'000LL, out);

    EXPECT_EQ(node.role(), RaftRole::Follower);
    EXPECT_EQ(node.currentTerm(), term + 5);
}

// ---------------------------------------------------------------------------
// 21. handleRequestVoteResult ignored when role != Candidate (e.g. Follower)
// ---------------------------------------------------------------------------
TEST(Election, ResultIgnored_WhenNotCandidate) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.setCurrentTerm(3);
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());
    // Node is Follower
    ASSERT_EQ(node.role(), RaftRole::Follower);

    Outbox out;
    RequestVoteResult res{ .term = 3, .voteGranted = true };
    node.handleRequestVoteResult(1, res, 1'000'000LL, out);

    // Should remain Follower, not become Leader
    EXPECT_EQ(node.role(), RaftRole::Follower);
}

// ---------------------------------------------------------------------------
// 22. becomeCandidate clears vote set from prior term and seeds with self
// ---------------------------------------------------------------------------
TEST(Election, BecomeCandidate_ClearsPriorVotes_AddsSelf) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig5(0), ps, log, makeOpts());

    Outbox out;
    // First election: become candidate, get two more grants
    electToCandidate(node, out);
    Term firstTerm = node.currentTerm();
    RequestVoteResult res{ .term = firstTerm, .voteGranted = true };
    node.handleRequestVoteResult(1, res, 1'000'000LL, out);
    EXPECT_EQ(node.grantedVoteCount(), 2);
    // Don't promote — just trigger second election (timeout)
    // Advance to next candidate state without majority
    out.clear();
    node.doWork(node.electionDeadlineNs(), out); // second becomeCandidate
    EXPECT_EQ(node.grantedVoteCount(), 1); // reset: only self
    EXPECT_GT(node.currentTerm(), firstTerm);
}

// ---------------------------------------------------------------------------
// 23. End-to-end: node times out → Candidate → receives grants → Leader → heartbeats
// ---------------------------------------------------------------------------
TEST(Election, EndToEnd_TimeoutThenGrants_BecomesLeader_EmitsHeartbeats) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts(99));

    // doWork at deadline → Candidate, emits RequestVotes
    Outbox out;
    int64_t electionNs = node.electionDeadlineNs();
    node.doWork(electionNs, out);
    ASSERT_EQ(node.role(), RaftRole::Candidate);

    Term term = node.currentTerm();
    // Count RequestVote messages
    int rvCount = 0;
    for (auto& m : out) { if (std::holds_alternative<RequestVote>(m.msg)) ++rvCount; }
    EXPECT_EQ(rvCount, 2); // peers 1 and 2

    out.clear();
    // Feed a granted result (one is enough for 3-member majority=2 with self)
    RequestVoteResult grant{ .term = term, .voteGranted = true };
    node.handleRequestVoteResult(1, grant, electionNs + 1'000, out);
    ASSERT_EQ(node.role(), RaftRole::Leader);

    // becomeLeader emits immediate heartbeats
    EXPECT_TRUE(hasLeaderHeartbeat(out));
    std::set<NodeId> targets;
    for (auto& m : out) {
        if (std::holds_alternative<AppendEntries>(m.msg)) {
            targets.insert(m.to);
        }
    }
    EXPECT_EQ(targets.count(0), 0u); // not to self
    EXPECT_GE(targets.size(), 1u);

    // Subsequent doWork (after interval) emits another heartbeat round
    out.clear();
    int64_t afterHb = electionNs + makeOpts(99).heartbeatIntervalNs + 2'000;
    node.doWork(afterHb, out);
    EXPECT_TRUE(hasLeaderHeartbeat(out));
}

// ---------------------------------------------------------------------------
// 24. handleRequestVote from self is handled normally (no special-case needed)
// ---------------------------------------------------------------------------
TEST(Election, RequestVoteFromSelf_EmptyLog_Grant) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.setCurrentTerm(1);
    ps.setVotedFor(kNoVote);
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    // Simulating self-vote message (unusual but must not crash)
    RequestVote rv{ .candidateTerm = 1, .candidateId = 0,
                    .lastLogIndex = 0, .lastLogTerm = 0 };
    auto res = node.handleRequestVote(rv, 1'000'000LL, out);
    // votedFor is kNoVote; log empty matches; must grant
    EXPECT_TRUE(res.voteGranted);
}

// ---------------------------------------------------------------------------
// 25. Step-down via handleRequestVote clears votesGranted_
// ---------------------------------------------------------------------------
TEST(Election, StepDownViaRequestVote_ClearsVoteSet) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig5(0), ps, log, makeOpts());

    Outbox out;
    electToCandidate(node, out);
    ASSERT_EQ(node.role(), RaftRole::Candidate);
    EXPECT_EQ(node.grantedVoteCount(), 1); // self

    // A RequestVote with a much higher term causes step-down
    Term highTerm = node.currentTerm() + 10;
    RequestVote rv{ .candidateTerm = highTerm, .candidateId = 2,
                    .lastLogIndex = 0, .lastLogTerm = 0 };
    out.clear();
    node.handleRequestVote(rv, 2'000'000LL, out);

    EXPECT_EQ(node.role(), RaftRole::Follower);
    EXPECT_EQ(node.grantedVoteCount(), 0); // cleared on step-down
}

// ---------------------------------------------------------------------------
// 26. Equal candidateTerm, no prior vote, empty log on both sides → grant
// ---------------------------------------------------------------------------
TEST(Election, EqualTerm_NoVote_EmptyLog_Grant) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.setCurrentTerm(2);
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    RequestVote rv{ .candidateTerm = 2, .candidateId = 1,
                    .lastLogIndex = 0, .lastLogTerm = 0 };
    auto res = node.handleRequestVote(rv, 1'000'000LL, out);

    EXPECT_TRUE(res.voteGranted);
    EXPECT_EQ(res.term, 2);
    EXPECT_EQ(ps.votedFor(), 1);
}

// ---------------------------------------------------------------------------
// 27. Denied vote is still outboxed with correct term and voteGranted=false
// ---------------------------------------------------------------------------
TEST(Election, DeniedVote_OutboxHasCorrectFields) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.setCurrentTerm(5);
    RaftLog log;
    log.append(5, "x"); // lastTerm=5, lastIndex=1
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    // Stale log (lastLogTerm=4 < mine=5)
    RequestVote rv{ .candidateTerm = 5, .candidateId = 1,
                    .lastLogIndex = 10, .lastLogTerm = 4 };
    auto res = node.handleRequestVote(rv, 1'000'000LL, out);

    EXPECT_FALSE(res.voteGranted);
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0].to, 1);
    auto& r = std::get<RequestVoteResult>(out[0].msg);
    EXPECT_FALSE(r.voteGranted);
    EXPECT_EQ(r.term, 5);
}

// ---------------------------------------------------------------------------
// 28. becomeLeader after result: votesGranted reset on step-down to follower
// ---------------------------------------------------------------------------
TEST(Election, AfterLeader_StepDown_VoteSetCleared) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    auto node = RaftNode(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    electToCandidate(node, out);
    Term term = node.currentTerm();

    // Promote to Leader
    RequestVoteResult grant{ .term = term, .voteGranted = true };
    node.handleRequestVoteResult(1, grant, 1'000'000LL, out);
    ASSERT_EQ(node.role(), RaftRole::Leader);
    EXPECT_GT(node.grantedVoteCount(), 0);

    // Step down via observeTerm
    node.observeTerm(term + 1, 2'000'000LL);
    EXPECT_EQ(node.role(), RaftRole::Follower);
    EXPECT_EQ(node.grantedVoteCount(), 0);
}
