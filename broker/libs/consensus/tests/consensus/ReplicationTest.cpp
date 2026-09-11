// ReplicationTest.cpp — P2-e: AppendEntries + commit (Leader Completeness)
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
// Helpers
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
               ("rune_repl_test_" + uniq + std::string(suffix));
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

/// Elect a node as leader in a single step (no real peers needed).
static void electAsLeader(RaftNode& node, int64_t nowNs, Outbox& out) {
    node.doWork(node.electionDeadlineNs(), out); // become candidate
    out.clear();
    node.becomeLeader(nowNs, out);
    out.clear();
}

/// Find the first AppendEntries in the outbox addressed to `to`.
static const AppendEntries* findAE(const Outbox& out, NodeId to) {
    for (auto& msg : out) {
        if (msg.to == to && std::holds_alternative<AppendEntries>(msg.msg)) {
            return &std::get<AppendEntries>(msg.msg);
        }
    }
    return nullptr;
}

/// Find the first AppendEntriesResult in the outbox addressed to `to`.
static const AppendEntriesResult* findAEResult(const Outbox& out, NodeId to) {
    for (auto& msg : out) {
        if (msg.to == to && std::holds_alternative<AppendEntriesResult>(msg.msg)) {
            return &std::get<AppendEntriesResult>(msg.msg);
        }
    }
    return nullptr;
}

// ===========================================================================
// 1. Follower consistency check — matching prevLog succeeds
// ===========================================================================
TEST(Replication, FollowerAppendSucceedsMatchingPrevLog) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    log.append(1, "cmd1"); // idx=1, term=1

    RaftNode node(makeConfig3(1), ps, log, makeOpts());
    Outbox out;

    AppendEntries ae{
        .term         = 1,
        .leaderId     = 0,
        .prevLogIndex = 1,
        .prevLogTerm  = 1,
        .entries      = { LogEntry{1, "cmd2"} },
        .leaderCommit = 0,
    };
    auto res = node.handleAppendEntries(ae, 1'000, out);

    EXPECT_TRUE(res.success);
    EXPECT_EQ(res.matchIndex, 2);
    EXPECT_EQ(log.lastIndex(), 2);
    EXPECT_EQ(log.termAt(2), 1);

    const AppendEntriesResult* r = findAEResult(out, 0);
    ASSERT_NE(r, nullptr);
    EXPECT_TRUE(r->success);
    EXPECT_EQ(r->matchIndex, 2);
}

// ===========================================================================
// 2. Follower consistency check — prevLogIndex beyond lastIndex → reject
// ===========================================================================
TEST(Replication, FollowerRejectsPrevLogIndexBeyondLastIndex) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode node(makeConfig3(1), ps, log, makeOpts());

    Outbox out;
    AppendEntries ae{
        .term         = 1,
        .leaderId     = 0,
        .prevLogIndex = 5,   // log is empty
        .prevLogTerm  = 1,
        .entries      = {},
        .leaderCommit = 0,
    };
    auto res = node.handleAppendEntries(ae, 1'000, out);

    EXPECT_FALSE(res.success);
    EXPECT_EQ(res.matchIndex, 0);
    EXPECT_EQ(log.lastIndex(), 0);
}

// ===========================================================================
// 3. Follower consistency check — prevLogTerm mismatch → reject
// ===========================================================================
TEST(Replication, FollowerRejectsPrevLogTermMismatch) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    log.append(1, "cmd1"); // idx=1, term=1
    RaftNode node(makeConfig3(1), ps, log, makeOpts());

    Outbox out;
    AppendEntries ae{
        .term         = 2,
        .leaderId     = 0,
        .prevLogIndex = 1,
        .prevLogTerm  = 2,  // mismatch: actual is term=1
        .entries      = { LogEntry{2, "cmd2"} },
        .leaderCommit = 0,
    };
    auto res = node.handleAppendEntries(ae, 1'000, out);

    EXPECT_FALSE(res.success);
    EXPECT_EQ(res.matchIndex, 0);
    EXPECT_EQ(log.lastIndex(), 1); // log unchanged
}

// ===========================================================================
// 4. First append on empty log (prevLogIndex=0)
// ===========================================================================
TEST(Replication, FollowerAcceptsFirstEntryOnEmptyLog) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode node(makeConfig3(1), ps, log, makeOpts());

    Outbox out;
    AppendEntries ae{
        .term         = 1,
        .leaderId     = 0,
        .prevLogIndex = 0,
        .prevLogTerm  = 0,
        .entries      = { LogEntry{1, "first"} },
        .leaderCommit = 0,
    };
    auto res = node.handleAppendEntries(ae, 1'000, out);

    EXPECT_TRUE(res.success);
    EXPECT_EQ(res.matchIndex, 1);
    EXPECT_EQ(log.lastIndex(), 1);
    EXPECT_EQ(log.at(1).command, "first");
}

// ===========================================================================
// 5. Conflict truncation: follower has [t1,t1,t2], leader sends term-3 at idx=3
// ===========================================================================
TEST(Replication, ConflictTruncationAtIndex3) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    log.append(1, "a"); // idx=1, term=1
    log.append(1, "b"); // idx=2, term=1
    log.append(2, "c"); // idx=3, term=2 — will conflict
    RaftNode node(makeConfig3(1), ps, log, makeOpts());

    Outbox out;
    AppendEntries ae{
        .term         = 3,
        .leaderId     = 0,
        .prevLogIndex = 2,
        .prevLogTerm  = 1,
        .entries      = { LogEntry{3, "d"} }, // idx=3 now term=3
        .leaderCommit = 0,
    };
    auto res = node.handleAppendEntries(ae, 1'000, out);

    EXPECT_TRUE(res.success);
    EXPECT_EQ(res.matchIndex, 3);
    EXPECT_EQ(log.lastIndex(), 3);
    EXPECT_EQ(log.termAt(3), 3);
    EXPECT_EQ(log.at(3).command, "d");
    EXPECT_EQ(log.termAt(1), 1); // earlier entries preserved
    EXPECT_EQ(log.termAt(2), 1);
}

// ===========================================================================
// 6. Idempotent re-delivery: same entries do NOT trigger truncation
// ===========================================================================
TEST(Replication, IdempotentRedeliveryDoesNotTruncate) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    log.append(1, "a");
    log.append(1, "b");
    log.append(1, "c");
    log.setCommitIndex(2);
    RaftNode node(makeConfig3(1), ps, log, makeOpts());

    AppendEntries ae{
        .term         = 1,
        .leaderId     = 0,
        .prevLogIndex = 2,
        .prevLogTerm  = 1,
        .entries      = { LogEntry{1, "c"} }, // idx=3 matches existing term
        .leaderCommit = 2,
    };
    Outbox out;
    auto res1 = node.handleAppendEntries(ae, 1'000, out);
    out.clear();
    auto res2 = node.handleAppendEntries(ae, 2'000, out);

    EXPECT_TRUE(res1.success);
    EXPECT_TRUE(res2.success);
    EXPECT_EQ(log.lastIndex(), 3);   // not grown beyond 3
    EXPECT_EQ(log.commitIndex(), 2); // commit preserved
    EXPECT_EQ(log.at(3).command, "c");
}

// ===========================================================================
// 7. Commit advancement (follower): leaderCommit advances commitIndex
// ===========================================================================
TEST(Replication, CommitAdvancementFollower) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode node(makeConfig3(1), ps, log, makeOpts());

    Outbox out;
    AppendEntries ae{
        .term         = 1,
        .leaderId     = 0,
        .prevLogIndex = 0,
        .prevLogTerm  = 0,
        .entries      = { LogEntry{1, "x"}, LogEntry{1, "y"} },
        .leaderCommit = 2,
    };
    node.handleAppendEntries(ae, 1'000, out);

    EXPECT_EQ(log.commitIndex(), 2); // min(2, lastNewIndex=2) = 2
}

// ===========================================================================
// 8. Commit capped at lastNewIndex when leaderCommit > lastNewIndex
// ===========================================================================
TEST(Replication, CommitCappedAtLastNewIndex) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode node(makeConfig3(1), ps, log, makeOpts());

    Outbox out;
    AppendEntries ae{
        .term         = 1,
        .leaderId     = 0,
        .prevLogIndex = 0,
        .prevLogTerm  = 0,
        .entries      = { LogEntry{1, "x"} },
        .leaderCommit = 99,  // far ahead
    };
    node.handleAppendEntries(ae, 1'000, out);

    EXPECT_EQ(log.commitIndex(), 1); // capped at lastNewIndex=1
}

// ===========================================================================
// 9. Stale AE: ae.term < currentTerm → reject, reply carries currentTerm
// ===========================================================================
TEST(Replication, StaleAERejectedWithCurrentTerm) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    ps.setCurrentTerm(5);
    RaftLog log;
    RaftNode node(makeConfig3(1), ps, log, makeOpts());

    Outbox out;
    AppendEntries ae{
        .term         = 3, // stale
        .leaderId     = 0,
        .prevLogIndex = 0,
        .prevLogTerm  = 0,
        .entries      = {},
        .leaderCommit = 0,
    };
    auto res = node.handleAppendEntries(ae, 1'000, out);

    EXPECT_FALSE(res.success);
    EXPECT_EQ(res.term, 5);
    EXPECT_EQ(res.matchIndex, 0);
    EXPECT_EQ(log.lastIndex(), 0); // nothing appended
}

// ===========================================================================
// 10. Higher-term AE: node adopts term, becomes follower, processes successfully
// ===========================================================================
TEST(Replication, HigherTermAEAdoptsTermAndBecomesFollower) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode node(makeConfig3(1), ps, log, makeOpts());

    // First become candidate (term=1).
    Outbox out;
    node.doWork(node.electionDeadlineNs(), out);
    ASSERT_EQ(node.role(), RaftRole::Candidate);
    ASSERT_EQ(node.currentTerm(), 1);
    out.clear();

    // Receive AE with term=5 — higher than current term=1.
    AppendEntries ae{
        .term         = 5,
        .leaderId     = 0,
        .prevLogIndex = 0,
        .prevLogTerm  = 0,
        .entries      = { LogEntry{5, "hello"} },
        .leaderCommit = 0,
    };
    auto res = node.handleAppendEntries(ae, 2'000, out);

    EXPECT_EQ(node.role(), RaftRole::Follower);
    EXPECT_EQ(node.currentTerm(), 5);
    EXPECT_TRUE(res.success);
    EXPECT_EQ(res.term, 5);
    EXPECT_EQ(log.lastIndex(), 1);
}

// ===========================================================================
// 11. Candidate receiving valid same-term AE steps down to Follower
// ===========================================================================
TEST(Replication, CandidateReceivesSameTermAEStepsDownToFollower) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode node(makeConfig3(1), ps, log, makeOpts());

    Outbox out;
    node.doWork(node.electionDeadlineNs(), out);
    ASSERT_EQ(node.role(), RaftRole::Candidate);
    ASSERT_EQ(node.currentTerm(), 1);
    out.clear();

    AppendEntries ae{
        .term         = 1,
        .leaderId     = 0,
        .prevLogIndex = 0,
        .prevLogTerm  = 0,
        .entries      = {},
        .leaderCommit = 0,
    };
    auto res = node.handleAppendEntries(ae, 2'000, out);

    EXPECT_EQ(node.role(), RaftRole::Follower);
    EXPECT_EQ(node.currentTerm(), 1);
    EXPECT_TRUE(res.success);
}

// ===========================================================================
// 12. propose returns -1 when not leader
// ===========================================================================
TEST(Replication, ProposeReturnsMinusOneWhenNotLeader) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode node(makeConfig3(1), ps, log, makeOpts()); // starts as follower

    Outbox out;
    Index idx = node.propose("cmd", 1'000, out);

    EXPECT_EQ(idx, -1);
    EXPECT_TRUE(out.empty());
    EXPECT_EQ(log.lastIndex(), 0);
}

// ===========================================================================
// 13. propose appends to own log and emits AE to all peers
// ===========================================================================
TEST(Replication, ProposeAppendsAndEmitsToAllPeers) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode leader(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    electAsLeader(leader, 1'000, out);

    Index idx = leader.propose("set x=1", 2'000, out);

    EXPECT_EQ(idx, 1);
    EXPECT_EQ(log.lastIndex(), 1);
    EXPECT_EQ(log.at(1).command, "set x=1");
    EXPECT_EQ(log.termAt(1), leader.currentTerm());

    const AppendEntries* ae1 = findAE(out, 1);
    const AppendEntries* ae2 = findAE(out, 2);
    ASSERT_NE(ae1, nullptr);
    ASSERT_NE(ae2, nullptr);
    EXPECT_EQ(ae1->entries.size(), 1u);
    EXPECT_EQ(ae1->entries[0].command, "set x=1");
    EXPECT_EQ(ae2->entries.size(), 1u);
}

// ===========================================================================
// 14. Commit advances after one peer success (majority=2 in 3-member cluster)
// ===========================================================================
TEST(Replication, CommitAdvancesAfterOneSuccessResult_3Members) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode leader(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    electAsLeader(leader, 1'000, out);
    leader.propose("cmd1", 2'000, out);
    out.clear();

    EXPECT_EQ(log.commitIndex(), 0); // not yet replicated to majority

    // Peer 1 acknowledges: matchIndex=1.
    AppendEntriesResult res{ .term = leader.currentTerm(), .success = true, .matchIndex = 1 };
    leader.handleAppendEntriesResult(1, res, 3'000, out);

    // self(match=1) + peer1(match=1) = 2 = majority(2). Current term. Commit.
    EXPECT_EQ(log.commitIndex(), 1);
}

// ===========================================================================
// 15. 5-member cluster: single peer success does NOT commit
// ===========================================================================
TEST(Replication, FiveMemberOnePeerSuccessDoesNotCommit) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode leader(makeConfig5(0), ps, log, makeOpts());

    Outbox out;
    electAsLeader(leader, 1'000, out);
    leader.propose("cmd1", 2'000, out);
    out.clear();

    AppendEntriesResult res{ .term = leader.currentTerm(), .success = true, .matchIndex = 1 };
    leader.handleAppendEntriesResult(1, res, 3'000, out);

    EXPECT_EQ(log.commitIndex(), 0); // only 2/5 — not a majority
}

// ===========================================================================
// 16. 5-member cluster: two peer successes = majority (3 incl self) → commit
// ===========================================================================
TEST(Replication, FiveMemberTwoPeerSuccessesCommit) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode leader(makeConfig5(0), ps, log, makeOpts());

    Outbox out;
    electAsLeader(leader, 1'000, out);
    leader.propose("cmd1", 2'000, out);
    out.clear();

    AppendEntriesResult res{ .term = leader.currentTerm(), .success = true, .matchIndex = 1 };
    leader.handleAppendEntriesResult(1, res, 3'000, out);
    EXPECT_EQ(log.commitIndex(), 0); // only 2/5

    leader.handleAppendEntriesResult(2, res, 3'000, out);
    EXPECT_EQ(log.commitIndex(), 1); // now 3/5 = majority
}

// ===========================================================================
// 17. Figure-8 / Leader Completeness: must NOT commit prior-term entry by count
//
// Scenario (3 members, majority=2):
//   Log at leader (term=3): [idx=1,term=1 | idx=2,term=1]
//   matchIndex[self]=2, matchIndex[1]=0, matchIndex[2]=0
//
//   Step A: Peer 1 reports matchIndex=2. Both self and peer1 have idx=2.
//     - log.termAt(2)=1 != currentTerm=3 → Figure-8: must NOT commit.
//
//   Step B: Leader proposes a current-term (term=3) entry → idx=3, term=3.
//     Peer 1 reports matchIndex=3. Now majority has idx=3 with term=3.
//     → setCommitIndex(3). Prior entries commit transitively.
// ===========================================================================
TEST(Replication, Figure8_PriorTermEntryNotCommittedByCountAlone) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    log.append(1, "prior1"); // idx=1, term=1
    log.append(1, "prior2"); // idx=2, term=1

    RaftNode leader(makeConfig3(0), ps, log, makeOpts());

    Outbox out;

    // Become candidate at term=1, win election.
    leader.doWork(leader.electionDeadlineNs(), out);
    ASSERT_EQ(leader.currentTerm(), 1);
    out.clear();

    // Simulate receiving a vote from peer 1 — majority reached → leader.
    RequestVoteResult grant{ .term = 1, .voteGranted = true };
    leader.handleRequestVoteResult(1, grant, 1'000, out);
    ASSERT_EQ(leader.role(), RaftRole::Leader);
    ASSERT_EQ(leader.currentTerm(), 1);
    out.clear();

    // Leader is term=1, log has [t1,t1] — both are current-term entries.
    // We need a higher term leader with prior-term entries.
    // Observe a higher term to step down, then re-elect at term=2.
    leader.observeTerm(2, 2'000);
    ASSERT_EQ(leader.role(), RaftRole::Follower);
    ASSERT_EQ(leader.currentTerm(), 2);
    out.clear();

    // becomeCandidate from term=2 bumps to term=3.
    leader.doWork(leader.electionDeadlineNs(), out);
    ASSERT_EQ(leader.currentTerm(), 3);
    out.clear();

    RequestVoteResult grant3{ .term = 3, .voteGranted = true };
    leader.handleRequestVoteResult(1, grant3, 3'000, out);
    ASSERT_EQ(leader.role(), RaftRole::Leader);
    ASSERT_EQ(leader.currentTerm(), 3);
    out.clear();

    // Now: leader is term=3, log=[idx=1 t1, idx=2 t1].
    // matchIndex[self]=2 (set by becomeLeader), matchIndex[1]=0, matchIndex[2]=0.
    EXPECT_EQ(log.commitIndex(), 0);
    EXPECT_EQ(log.termAt(1), 1);
    EXPECT_EQ(log.termAt(2), 1);

    // --- Step A ---
    // Peer 1 reports matchIndex=2 (it has both prior-term entries).
    // Count nodes with match>=2: self(2)=yes, peer1(2)=yes → count=2 = majority.
    // BUT log.termAt(2)=1 != currentTerm=3 → Figure-8: do NOT commit.
    AppendEntriesResult resPeer1a{ .term = 3, .success = true, .matchIndex = 2 };
    leader.handleAppendEntriesResult(1, resPeer1a, 4'000, out);

    EXPECT_EQ(log.commitIndex(), 0)
        << "Figure-8 violated: committed prior-term entry by matchIndex count alone";

    // --- Step B ---
    // Leader proposes a current-term (term=3) entry.
    out.clear();
    leader.propose("current_term_cmd", 5'000, out);
    ASSERT_EQ(log.lastIndex(), 3);
    ASSERT_EQ(log.termAt(3), 3); // current term

    // Peer 1 responds with matchIndex=3 (has all three entries).
    // Count with match>=3: self(3)=yes, peer1(3)=yes → count=2 = majority.
    // log.termAt(3)=3 == currentTerm=3 → SAFE to commit index=3 (and transitively 1,2).
    out.clear();
    AppendEntriesResult resPeer1b{ .term = 3, .success = true, .matchIndex = 3 };
    leader.handleAppendEntriesResult(1, resPeer1b, 6'000, out);

    EXPECT_EQ(log.commitIndex(), 3)
        << "Commit did not advance when own-term entry reached majority";
}

// ===========================================================================
// 18. nextIndex backoff on rejection: decrements and retries with lower prevLogIndex
// ===========================================================================
TEST(Replication, NextIndexBackoffOnRejection) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    log.append(1, "a");
    log.append(1, "b");
    log.append(1, "c");
    RaftNode leader(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    electAsLeader(leader, 1'000, out);
    out.clear();

    // Initial nextIndex for peer 1 = lastIndex+1 = 4.
    EXPECT_EQ(leader.nextIndexFor(1), 4);

    AppendEntriesResult rej{ .term = leader.currentTerm(), .success = false, .matchIndex = 0 };

    // First rejection: nextIndex[1] = max(1, 4-1) = 3.
    leader.handleAppendEntriesResult(1, rej, 2'000, out);
    EXPECT_EQ(leader.nextIndexFor(1), 3);
    const AppendEntries* ae1 = findAE(out, 1);
    ASSERT_NE(ae1, nullptr);
    EXPECT_EQ(ae1->prevLogIndex, 2); // nextIndex=3 → prevLogIndex=2
    out.clear();

    // Second rejection: nextIndex[1] = 2.
    leader.handleAppendEntriesResult(1, rej, 3'000, out);
    EXPECT_EQ(leader.nextIndexFor(1), 2);
    const AppendEntries* ae2 = findAE(out, 1);
    ASSERT_NE(ae2, nullptr);
    EXPECT_EQ(ae2->prevLogIndex, 1);
    out.clear();

    // Third rejection: nextIndex[1] = 1.
    leader.handleAppendEntriesResult(1, rej, 4'000, out);
    EXPECT_EQ(leader.nextIndexFor(1), 1);
    const AppendEntries* ae3 = findAE(out, 1);
    ASSERT_NE(ae3, nullptr);
    EXPECT_EQ(ae3->prevLogIndex, 0); // nextIndex=1 → prevLogIndex=0
}

// ===========================================================================
// 19. nextIndex is floored at 1 (never goes below 1)
// ===========================================================================
TEST(Replication, NextIndexFloorAtOne) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode leader(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    electAsLeader(leader, 1'000, out);
    out.clear();

    EXPECT_EQ(leader.nextIndexFor(1), 1); // empty log → lastIndex+1=1

    AppendEntriesResult rej{ .term = leader.currentTerm(), .success = false, .matchIndex = 0 };
    leader.handleAppendEntriesResult(1, rej, 2'000, out);
    EXPECT_EQ(leader.nextIndexFor(1), 1); // max(1, 1-1=0) = 1
}

// ===========================================================================
// 20. Heartbeat carries entries for a lagging follower
// ===========================================================================
TEST(Replication, HeartbeatCarriesEntriesForLaggingFollower) {
    // Leader has 3 entries. Peer 1 is backed off to nextIndex=2.
    // The AE sent to peer 1 should carry entries from index 2 onwards.
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    log.append(1, "e1");
    log.append(1, "e2");
    log.append(1, "e3");
    RaftNode leader(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    electAsLeader(leader, 1'000, out);
    out.clear();

    // Back off peer 1 via two rejections: nextIndex[1]: 4 → 3 → 2.
    AppendEntriesResult rej{ .term = leader.currentTerm(), .success = false, .matchIndex = 0 };
    leader.handleAppendEntriesResult(1, rej, 2'000, out);
    out.clear();
    leader.handleAppendEntriesResult(1, rej, 3'000, out);
    out.clear();

    ASSERT_EQ(leader.nextIndexFor(1), 2);

    // Trigger heartbeat via doWork after interval.
    int64_t t = 1'000 + makeOpts().heartbeatIntervalNs + 1;
    leader.doWork(t, out);

    const AppendEntries* ae = findAE(out, 1);
    ASSERT_NE(ae, nullptr);
    EXPECT_EQ(ae->prevLogIndex, 1);
    EXPECT_EQ(ae->entries.size(), 2u); // e2 and e3
    EXPECT_EQ(ae->entries[0].command, "e2");
    EXPECT_EQ(ae->entries[1].command, "e3");
}

// ===========================================================================
// 21. Heartbeat is empty for a caught-up follower
// ===========================================================================
TEST(Replication, HeartbeatIsEmptyForCaughtUpFollower) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    log.append(1, "e1");
    RaftNode leader(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    electAsLeader(leader, 1'000, out);
    out.clear();

    // nextIndex[1] = lastIndex+1 = 2; peer is "caught up".
    int64_t t = 1'000 + makeOpts().heartbeatIntervalNs + 1;
    leader.doWork(t, out);

    const AppendEntries* hb = findAE(out, 1);
    ASSERT_NE(hb, nullptr);
    EXPECT_TRUE(hb->entries.empty());
}

// ===========================================================================
// 22. Leader steps down when AE result carries higher term
// ===========================================================================
TEST(Replication, LeaderStepsDownOnHigherTermInAEResult) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode leader(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    electAsLeader(leader, 1'000, out);
    ASSERT_EQ(leader.role(), RaftRole::Leader);
    out.clear();

    AppendEntriesResult res{ .term = leader.currentTerm() + 5, .success = false, .matchIndex = 0 };
    leader.handleAppendEntriesResult(1, res, 2'000, out);

    EXPECT_EQ(leader.role(), RaftRole::Follower);
    EXPECT_EQ(leader.currentTerm(), res.term);
}

// ===========================================================================
// 23. Stale-term AE result is ignored (no state change)
// ===========================================================================
TEST(Replication, StaleTermAEResultIgnored) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode leader(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    electAsLeader(leader, 1'000, out);
    leader.propose("cmd", 2'000, out);
    out.clear();

    AppendEntriesResult stale{ .term = leader.currentTerm() - 1, .success = true, .matchIndex = 1 };
    leader.handleAppendEntriesResult(1, stale, 3'000, out);

    EXPECT_EQ(log.commitIndex(), 0); // stale result must not advance commit
}

// ===========================================================================
// 24. Duplicate success result is idempotent
// ===========================================================================
TEST(Replication, DuplicateSuccessResultIsIdempotent) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode leader(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    electAsLeader(leader, 1'000, out);
    leader.propose("cmd1", 2'000, out);
    out.clear();

    AppendEntriesResult res{ .term = leader.currentTerm(), .success = true, .matchIndex = 1 };
    leader.handleAppendEntriesResult(1, res, 3'000, out);
    EXPECT_EQ(log.commitIndex(), 1);

    // Deliver same result again — must be idempotent.
    leader.handleAppendEntriesResult(1, res, 4'000, out);
    EXPECT_EQ(log.commitIndex(), 1);
}

// ===========================================================================
// 25. Empty heartbeat to empty follower succeeds (matchIndex=0)
// ===========================================================================
TEST(Replication, EmptyHeartbeatOnEmptyFollowerSucceeds) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode node(makeConfig3(1), ps, log, makeOpts());

    Outbox out;
    AppendEntries ae{
        .term         = 1,
        .leaderId     = 0,
        .prevLogIndex = 0,
        .prevLogTerm  = 0,
        .entries      = {},
        .leaderCommit = 0,
    };
    auto res = node.handleAppendEntries(ae, 1'000, out);

    EXPECT_TRUE(res.success);
    EXPECT_EQ(res.matchIndex, 0); // prevLogIndex + 0 entries = 0
    EXPECT_EQ(log.lastIndex(), 0);
}

// ===========================================================================
// 26. Multiple entries in a single AE are all appended correctly
// ===========================================================================
TEST(Replication, MultipleEntriesInSingleAEAppendedCorrectly) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode node(makeConfig3(1), ps, log, makeOpts());

    Outbox out;
    AppendEntries ae{
        .term         = 1,
        .leaderId     = 0,
        .prevLogIndex = 0,
        .prevLogTerm  = 0,
        .entries      = { LogEntry{1,"a"}, LogEntry{1,"b"}, LogEntry{1,"c"} },
        .leaderCommit = 3,
    };
    auto res = node.handleAppendEntries(ae, 1'000, out);

    EXPECT_TRUE(res.success);
    EXPECT_EQ(res.matchIndex, 3);
    EXPECT_EQ(log.lastIndex(), 3);
    EXPECT_EQ(log.at(1).command, "a");
    EXPECT_EQ(log.at(2).command, "b");
    EXPECT_EQ(log.at(3).command, "c");
    EXPECT_EQ(log.commitIndex(), 3);
}

// ===========================================================================
// 27. Conflict in the middle of a batch: truncate + append rest
// ===========================================================================
TEST(Replication, ConflictMidBatchTruncatesAndAppendsRest) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    log.append(1, "a"); // idx=1
    log.append(1, "b"); // idx=2 — will conflict
    log.append(1, "c"); // idx=3 — will be replaced
    RaftNode node(makeConfig3(1), ps, log, makeOpts());

    Outbox out;
    AppendEntries ae{
        .term         = 2,
        .leaderId     = 0,
        .prevLogIndex = 1,
        .prevLogTerm  = 1,
        .entries      = { LogEntry{2,"B"}, LogEntry{2,"C"} }, // overwrite idx=2,3
        .leaderCommit = 0,
    };
    auto res = node.handleAppendEntries(ae, 1'000, out);

    EXPECT_TRUE(res.success);
    EXPECT_EQ(res.matchIndex, 3);
    EXPECT_EQ(log.lastIndex(), 3);
    EXPECT_EQ(log.termAt(2), 2);
    EXPECT_EQ(log.at(2).command, "B");
    EXPECT_EQ(log.termAt(3), 2);
    EXPECT_EQ(log.at(3).command, "C");
    EXPECT_EQ(log.termAt(1), 1); // first entry preserved
}

// ===========================================================================
// 28. becomeLeader initializes per-peer state correctly
// ===========================================================================
TEST(Replication, BecomeLeaderInitializesPerPeerState) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    log.append(1, "x");
    log.append(1, "y"); // lastIndex = 2

    RaftNode leader(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    electAsLeader(leader, 1'000, out);

    EXPECT_EQ(leader.nextIndexFor(1),  3); // lastIndex+1
    EXPECT_EQ(leader.nextIndexFor(2),  3);
    EXPECT_EQ(leader.matchIndexFor(1), 0); // unconfirmed
    EXPECT_EQ(leader.matchIndexFor(2), 0);
    EXPECT_EQ(leader.matchIndexFor(0), 2); // leader's own match = lastIndex
}

// ===========================================================================
// 29. Valid AE resets the follower's election deadline
// ===========================================================================
TEST(Replication, ValidAEResetsElectionDeadline) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode node(makeConfig3(1), ps, log, makeOpts());

    int64_t before = node.electionDeadlineNs();
    int64_t nowNs = before - 1; // just before it expires

    Outbox out;
    AppendEntries ae{
        .term         = 1,
        .leaderId     = 0,
        .prevLogIndex = 0,
        .prevLogTerm  = 0,
        .entries      = {},
        .leaderCommit = 0,
    };
    node.handleAppendEntries(ae, nowNs, out);

    int64_t after = node.electionDeadlineNs();
    // Deadline must be reset from nowNs (not from the previous value).
    EXPECT_GE(after, nowNs + makeOpts().electionTimeoutBaseNs);
    EXPECT_NE(after, before); // actually changed
}

// ===========================================================================
// 30. matchIndex never goes backwards (out-of-order late result ignored)
// ===========================================================================
TEST(Replication, MatchIndexNeverGoesBackwards) {
    TempFile tmp;
    PersistentState ps(tmp.str());
    RaftLog log;
    RaftNode leader(makeConfig3(0), ps, log, makeOpts());

    Outbox out;
    electAsLeader(leader, 1'000, out);
    leader.propose("a", 2'000, out);
    leader.propose("b", 2'000, out);
    out.clear();

    // Peer 1 responds with matchIndex=2.
    AppendEntriesResult res2{ .term = leader.currentTerm(), .success = true, .matchIndex = 2 };
    leader.handleAppendEntriesResult(1, res2, 3'000, out);
    EXPECT_EQ(leader.matchIndexFor(1), 2);

    // Late/duplicate result with matchIndex=1 (out-of-order).
    AppendEntriesResult res1{ .term = leader.currentTerm(), .success = true, .matchIndex = 1 };
    leader.handleAppendEntriesResult(1, res1, 4'000, out);

    EXPECT_EQ(leader.matchIndexFor(1), 2); // monotonic — stays at 2
}
