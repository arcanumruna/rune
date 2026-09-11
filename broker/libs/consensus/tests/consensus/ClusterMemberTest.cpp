// ClusterMemberTest.cpp — Faithful port tests for ClusterMember algorithms.
// All tests are pure/in-memory; no I/O, no temp dirs, no allocation inside algorithms.
// Tests mirror the Java source exactly (ClusterMember.java).

#include <gtest/gtest.h>
#include <rune/consensus/ClusterMember.h>
#include <array>
#include <vector>

using namespace rune::consensus;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static ClusterMember makeMember(NodeId id, Term ltId, int64_t logPos,
                                 int64_t tolanNs = kNullValue,
                                 Vote vote = Vote::None,
                                 Term candidateTerm = kNullValue)
{
    ClusterMember m;
    m.id                         = id;
    m.leadershipTermId           = ltId;
    m.logPosition                = logPos;
    m.timeOfLastAppendPositionNs = tolanNs;
    m.vote                       = vote;
    m.candidateTermId            = candidateTerm;
    return m;
}

// Active member: timeOfLastAppendPositionNs + timeoutNs > nowNs
// => timeOfLastAppendPositionNs = nowNs - timeoutNs + 1  (just inside window)
static ClusterMember makeActiveMember(NodeId id, Term ltId, int64_t logPos,
                                       int64_t nowNs, int64_t timeoutNs,
                                       Vote vote = Vote::None,
                                       Term candidateTerm = kNullValue)
{
    ClusterMember m;
    m.id                         = id;
    m.leadershipTermId           = ltId;
    // logLeadershipTermId defaults to the same as leadershipTermId (same branch).
    // C6 tests that need a different branch set this field explicitly.
    m.logLeadershipTermId        = ltId;
    m.logPosition                = logPos;
    m.timeOfLastAppendPositionNs = nowNs - timeoutNs + 1; // active: last + timeout > now
    m.vote                       = vote;
    m.candidateTermId            = candidateTerm;
    return m;
}

static ClusterMember makeInactiveMember(NodeId id, Term ltId, int64_t logPos,
                                         int64_t nowNs, int64_t timeoutNs)
{
    ClusterMember m;
    m.id                         = id;
    m.leadershipTermId           = ltId;
    m.logPosition                = logPos;
    m.timeOfLastAppendPositionNs = nowNs - timeoutNs - 1; // inactive: last + timeout <= now
    return m;
}

// ---------------------------------------------------------------------------
// 1. compareLog — 4-arg form
//    ClusterMember.java:1173-1197
// ---------------------------------------------------------------------------

// Higher lhsLogLeadershipTermId wins regardless of logPosition.
TEST(ClusterMember, CompareLog_HigherTermWins_RegarlessOfPosition) {
    // lhs has higher term but smaller position → lhs still wins
    EXPECT_EQ(ClusterMember::compareLog(5, 100, 3, 999), 1);
}
TEST(ClusterMember, CompareLog_LowerTermLoses_RegarlessOfPosition) {
    EXPECT_EQ(ClusterMember::compareLog(2, 9999, 7, 1), -1);
}

// Equal term → higher logPosition wins.
TEST(ClusterMember, CompareLog_EqualTerm_HigherPositionWins) {
    EXPECT_EQ(ClusterMember::compareLog(4, 200, 4, 100), 1);
}
TEST(ClusterMember, CompareLog_EqualTerm_LowerPositionLoses) {
    EXPECT_EQ(ClusterMember::compareLog(4, 50, 4, 200), -1);
}

// Fully equal → 0.
TEST(ClusterMember, CompareLog_FullyEqual_ReturnsZero) {
    EXPECT_EQ(ClusterMember::compareLog(3, 100, 3, 100), 0);
}
TEST(ClusterMember, CompareLog_BothZero_ReturnsZero) {
    EXPECT_EQ(ClusterMember::compareLog(0, 0, 0, 0), 0);
}

// Symmetry: compareLog(a,b) == -compareLog(b,a)
TEST(ClusterMember, CompareLog_Symmetry_Term) {
    EXPECT_EQ(ClusterMember::compareLog(5, 100, 3, 100),
              -ClusterMember::compareLog(3, 100, 5, 100));
}
TEST(ClusterMember, CompareLog_Symmetry_Position) {
    EXPECT_EQ(ClusterMember::compareLog(3, 200, 3, 100),
              -ClusterMember::compareLog(3, 100, 3, 200));
}
TEST(ClusterMember, CompareLog_Symmetry_Equal) {
    EXPECT_EQ(ClusterMember::compareLog(3, 100, 3, 100),
              -ClusterMember::compareLog(3, 100, 3, 100));
}

// ---------------------------------------------------------------------------
// 2. compareLog — 2-arg (member overload)
//    ClusterMember.java:1208-1211 — uses leadershipTermId + logPosition
// ---------------------------------------------------------------------------
TEST(ClusterMember, CompareLogMember_HigherTermWins) {
    auto lhs = makeMember(0, 5, 100);
    auto rhs = makeMember(1, 3, 999);
    EXPECT_EQ(ClusterMember::compareLog(lhs, rhs), 1);
}
TEST(ClusterMember, CompareLogMember_EqualTermHigherPosWins) {
    auto lhs = makeMember(0, 4, 200);
    auto rhs = makeMember(1, 4, 100);
    EXPECT_EQ(ClusterMember::compareLog(lhs, rhs), 1);
}
TEST(ClusterMember, CompareLogMember_Equal) {
    auto lhs = makeMember(0, 3, 100);
    auto rhs = makeMember(1, 3, 100);
    EXPECT_EQ(ClusterMember::compareLog(lhs, rhs), 0);
}

// ---------------------------------------------------------------------------
// 3. quorumThreshold
//    ClusterMember.java:853-856: (memberCount >> 1) + 1
// ---------------------------------------------------------------------------
TEST(ClusterMember, QuorumThreshold_1) { EXPECT_EQ(ClusterMember::quorumThreshold(1), 1); }
TEST(ClusterMember, QuorumThreshold_2) { EXPECT_EQ(ClusterMember::quorumThreshold(2), 2); }
TEST(ClusterMember, QuorumThreshold_3) { EXPECT_EQ(ClusterMember::quorumThreshold(3), 2); }
TEST(ClusterMember, QuorumThreshold_4) { EXPECT_EQ(ClusterMember::quorumThreshold(4), 3); }
TEST(ClusterMember, QuorumThreshold_5) { EXPECT_EQ(ClusterMember::quorumThreshold(5), 3); }
TEST(ClusterMember, QuorumThreshold_6) { EXPECT_EQ(ClusterMember::quorumThreshold(6), 4); }
TEST(ClusterMember, QuorumThreshold_7) { EXPECT_EQ(ClusterMember::quorumThreshold(7), 4); }

// ---------------------------------------------------------------------------
// 4. quorumPosition
//    ClusterMember.java:867-895
// ---------------------------------------------------------------------------

// 3 members all active, positions {100, 50, 80}, K=2 → 2nd-highest = 80.
TEST(ClusterMember, QuorumPosition_3Members_AllActive_Returns2ndHighest) {
    int64_t nowNs     = 1'000'000'000LL;
    int64_t timeoutNs = 500'000'000LL;
    std::array<ClusterMember, 3> members = {
        makeActiveMember(0, 1, 100, nowNs, timeoutNs),
        makeActiveMember(1, 1,  50, nowNs, timeoutNs),
        makeActiveMember(2, 1,  80, nowNs, timeoutNs),
    };
    // K = quorumThreshold(3) = 2
    std::array<int64_t, 2> ranked{};
    int64_t pos = ClusterMember::quorumPosition(members, ranked, nowNs, timeoutNs);
    EXPECT_EQ(pos, 80);
}

// 3 members, one inactive (old timestamp), positions {100, 50, 80}.
// Only {100, 80} active → K=2, 2nd-highest = 80.
TEST(ClusterMember, QuorumPosition_3Members_OneInactive_ExcludedFromRanking) {
    int64_t nowNs     = 1'000'000'000LL;
    int64_t timeoutNs = 500'000'000LL;
    std::array<ClusterMember, 3> members = {
        makeActiveMember(0,  1, 100, nowNs, timeoutNs),
        makeInactiveMember(1, 1,  50, nowNs, timeoutNs), // inactive
        makeActiveMember(2,  1,  80, nowNs, timeoutNs),
    };
    std::array<int64_t, 2> ranked{};
    int64_t pos = ClusterMember::quorumPosition(members, ranked, nowNs, timeoutNs);
    // Active positions: {100, 80}. K=2 → 2nd-highest = 80.
    EXPECT_EQ(pos, 80);
}

// 5 members all active, positions {100, 50, 80, 60, 90}.
// Sorted descending: 100, 90, 80, 60, 50. K=3 → 3rd-highest = 80.
TEST(ClusterMember, QuorumPosition_5Members_AllActive_Returns3rdHighest) {
    int64_t nowNs     = 1'000'000'000LL;
    int64_t timeoutNs = 500'000'000LL;
    std::array<ClusterMember, 5> members = {
        makeActiveMember(0, 1, 100, nowNs, timeoutNs),
        makeActiveMember(1, 1,  50, nowNs, timeoutNs),
        makeActiveMember(2, 1,  80, nowNs, timeoutNs),
        makeActiveMember(3, 1,  60, nowNs, timeoutNs),
        makeActiveMember(4, 1,  90, nowNs, timeoutNs),
    };
    // K = quorumThreshold(5) = 3
    std::array<int64_t, 3> ranked{};
    int64_t pos = ClusterMember::quorumPosition(members, ranked, nowNs, timeoutNs);
    EXPECT_EQ(pos, 80);
}

// All members inactive → rankedPositions all 0 → returns 0.
TEST(ClusterMember, QuorumPosition_AllInactive_Returns0) {
    int64_t nowNs     = 1'000'000'000LL;
    int64_t timeoutNs = 500'000'000LL;
    std::array<ClusterMember, 3> members = {
        makeInactiveMember(0, 1, 100, nowNs, timeoutNs),
        makeInactiveMember(1, 1,  50, nowNs, timeoutNs),
        makeInactiveMember(2, 1,  80, nowNs, timeoutNs),
    };
    std::array<int64_t, 2> ranked{};
    int64_t pos = ClusterMember::quorumPosition(members, ranked, nowNs, timeoutNs);
    EXPECT_EQ(pos, 0);
}

// Ties: all members same position → quorumPosition == that position.
TEST(ClusterMember, QuorumPosition_AllTied_ReturnsTiedPosition) {
    int64_t nowNs     = 1'000'000'000LL;
    int64_t timeoutNs = 500'000'000LL;
    std::array<ClusterMember, 3> members = {
        makeActiveMember(0, 1, 42, nowNs, timeoutNs),
        makeActiveMember(1, 1, 42, nowNs, timeoutNs),
        makeActiveMember(2, 1, 42, nowNs, timeoutNs),
    };
    std::array<int64_t, 2> ranked{};
    int64_t pos = ClusterMember::quorumPosition(members, ranked, nowNs, timeoutNs);
    EXPECT_EQ(pos, 42);
}

// Caller's rankedPositions array is used (no internal allocation).
// Verify it is populated correctly after the call.
TEST(ClusterMember, QuorumPosition_UsesCallerArray_NoInternalAllocation) {
    int64_t nowNs     = 1'000'000'000LL;
    int64_t timeoutNs = 500'000'000LL;
    std::array<ClusterMember, 3> members = {
        makeActiveMember(0, 1, 200, nowNs, timeoutNs),
        makeActiveMember(1, 1, 150, nowNs, timeoutNs),
        makeActiveMember(2, 1, 100, nowNs, timeoutNs),
    };
    std::array<int64_t, 2> ranked{0LL, 0LL};
    ClusterMember::quorumPosition(members, ranked, nowNs, timeoutNs);
    // Top 2: [200, 150]. ranked[0]=200, ranked[1]=150.
    EXPECT_EQ(ranked[0], 200);
    EXPECT_EQ(ranked[1], 150);
}

// ---------------------------------------------------------------------------
// 5. hasActiveQuorum
//    ClusterMember.java:828-845
// ---------------------------------------------------------------------------

// 2 of 3 active → threshold=2 → true.
TEST(ClusterMember, HasActiveQuorum_2of3_True) {
    int64_t nowNs     = 1'000'000'000LL;
    int64_t timeoutNs = 500'000'000LL;
    std::array<ClusterMember, 3> members = {
        makeActiveMember(0, 1, 0, nowNs, timeoutNs),
        makeActiveMember(1, 1, 0, nowNs, timeoutNs),
        makeInactiveMember(2, 1, 0, nowNs, timeoutNs),
    };
    EXPECT_TRUE(ClusterMember::hasActiveQuorum(members, nowNs, timeoutNs));
}

// 1 of 3 active → threshold=2 → false.
TEST(ClusterMember, HasActiveQuorum_1of3_False) {
    int64_t nowNs     = 1'000'000'000LL;
    int64_t timeoutNs = 500'000'000LL;
    std::array<ClusterMember, 3> members = {
        makeActiveMember(0, 1, 0, nowNs, timeoutNs),
        makeInactiveMember(1, 1, 0, nowNs, timeoutNs),
        makeInactiveMember(2, 1, 0, nowNs, timeoutNs),
    };
    EXPECT_FALSE(ClusterMember::hasActiveQuorum(members, nowNs, timeoutNs));
}

// 3 of 5 active → threshold=3 → true (boundary).
TEST(ClusterMember, HasActiveQuorum_3of5_True) {
    int64_t nowNs     = 1'000'000'000LL;
    int64_t timeoutNs = 500'000'000LL;
    std::array<ClusterMember, 5> members = {
        makeActiveMember(0, 1, 0, nowNs, timeoutNs),
        makeActiveMember(1, 1, 0, nowNs, timeoutNs),
        makeActiveMember(2, 1, 0, nowNs, timeoutNs),
        makeInactiveMember(3, 1, 0, nowNs, timeoutNs),
        makeInactiveMember(4, 1, 0, nowNs, timeoutNs),
    };
    EXPECT_TRUE(ClusterMember::hasActiveQuorum(members, nowNs, timeoutNs));
}

// 2 of 5 active → threshold=3 → false.
TEST(ClusterMember, HasActiveQuorum_2of5_False) {
    int64_t nowNs     = 1'000'000'000LL;
    int64_t timeoutNs = 500'000'000LL;
    std::array<ClusterMember, 5> members = {
        makeActiveMember(0, 1, 0, nowNs, timeoutNs),
        makeActiveMember(1, 1, 0, nowNs, timeoutNs),
        makeInactiveMember(2, 1, 0, nowNs, timeoutNs),
        makeInactiveMember(3, 1, 0, nowNs, timeoutNs),
        makeInactiveMember(4, 1, 0, nowNs, timeoutNs),
    };
    EXPECT_FALSE(ClusterMember::hasActiveQuorum(members, nowNs, timeoutNs));
}

// ---------------------------------------------------------------------------
// 6. hasQuorumAtPosition
//    ClusterMember.java:907-925
// ---------------------------------------------------------------------------

// Majority at/above position with correct term → true.
TEST(ClusterMember, HasQuorumAtPosition_MajorityAtPosition_True) {
    int64_t nowNs     = 1'000'000'000LL;
    int64_t timeoutNs = 500'000'000LL;
    Term    ltId      = 3;
    int64_t pos       = 100;
    std::array<ClusterMember, 3> members = {
        makeActiveMember(0, ltId, 100, nowNs, timeoutNs),
        makeActiveMember(1, ltId, 200, nowNs, timeoutNs),
        makeActiveMember(2, ltId,  50, nowNs, timeoutNs), // below pos
    };
    // All members have logLeadershipTermId=ltId (same branch via makeActiveMember).
    EXPECT_TRUE(ClusterMember::hasQuorumAtPosition(members, ltId, ltId, pos, nowNs, timeoutNs));
}

// Only one at/above position → threshold=2 → false.
TEST(ClusterMember, HasQuorumAtPosition_MinorityAtPosition_False) {
    int64_t nowNs     = 1'000'000'000LL;
    int64_t timeoutNs = 500'000'000LL;
    Term    ltId      = 3;
    int64_t pos       = 100;
    std::array<ClusterMember, 3> members = {
        makeActiveMember(0, ltId, 100, nowNs, timeoutNs),
        makeActiveMember(1, ltId,  50, nowNs, timeoutNs),
        makeActiveMember(2, ltId,  30, nowNs, timeoutNs),
    };
    EXPECT_FALSE(ClusterMember::hasQuorumAtPosition(members, ltId, ltId, pos, nowNs, timeoutNs));
}

// Members at position but wrong leadershipTermId → not counted.
TEST(ClusterMember, HasQuorumAtPosition_WrongTerm_NotCounted) {
    int64_t nowNs     = 1'000'000'000LL;
    int64_t timeoutNs = 500'000'000LL;
    Term    ltId      = 3;
    int64_t pos       = 100;
    std::array<ClusterMember, 3> members = {
        makeActiveMember(0, ltId, 200, nowNs, timeoutNs),  // correct term
        makeActiveMember(1, 99,   200, nowNs, timeoutNs),  // wrong term (ltId=99, logLtId=99)
        makeActiveMember(2, 99,   200, nowNs, timeoutNs),  // wrong term (ltId=99, logLtId=99)
    };
    EXPECT_FALSE(ClusterMember::hasQuorumAtPosition(members, ltId, ltId, pos, nowNs, timeoutNs));
}

// Inactive members at position → not counted (isActive check inside hasReachedPosition).
TEST(ClusterMember, HasQuorumAtPosition_InactiveNotCounted) {
    int64_t nowNs     = 1'000'000'000LL;
    int64_t timeoutNs = 500'000'000LL;
    Term    ltId      = 3;
    int64_t pos       = 100;
    std::array<ClusterMember, 3> members = {
        makeActiveMember(0, ltId, 200, nowNs, timeoutNs),
        makeInactiveMember(1, ltId, 200, nowNs, timeoutNs), // inactive
        makeInactiveMember(2, ltId, 200, nowNs, timeoutNs), // inactive
    };
    EXPECT_FALSE(ClusterMember::hasQuorumAtPosition(members, ltId, ltId, pos, nowNs, timeoutNs));
}

// ---------------------------------------------------------------------------
// 7. isUnanimousLeader
//    ClusterMember.java:981-1002
// ---------------------------------------------------------------------------

// All 3 members have correct candidateTermId and vote=For → unanimous.
TEST(ClusterMember, IsUnanimousLeader_AllFor_True) {
    Term cTerm = 7;
    std::array<ClusterMember, 3> members = {
        makeMember(0, 0, 0, 0, Vote::For, cTerm),
        makeMember(1, 0, 0, 0, Vote::For, cTerm),
        makeMember(2, 0, 0, 0, Vote::For, cTerm),
    };
    EXPECT_TRUE(ClusterMember::isUnanimousLeader(members, cTerm, -1));
}

// 2 of 3 For → not unanimous (one has no vote).
TEST(ClusterMember, IsUnanimousLeader_OneNoVote_False) {
    Term cTerm = 7;
    std::array<ClusterMember, 3> members = {
        makeMember(0, 0, 0, 0, Vote::For,  cTerm),
        makeMember(1, 0, 0, 0, Vote::For,  cTerm),
        makeMember(2, 0, 0, 0, Vote::None, cTerm), // no vote
    };
    EXPECT_FALSE(ClusterMember::isUnanimousLeader(members, cTerm, -1));
}

// One member excluded (gracefulClosedLeaderId) → only the remaining 2 must be For;
// with quorumThreshold(3)=2, this should still be true.
TEST(ClusterMember, IsUnanimousLeader_GracefulClosedExcluded_OthersFor_True) {
    Term cTerm = 7;
    // Member 2 is the gracefully closed leader; skip it.
    // Members 0 and 1 both For. Votes accumulated = 2 >= quorumThreshold(3)=2.
    std::array<ClusterMember, 3> members = {
        makeMember(0, 0, 0, 0, Vote::For,  cTerm),
        makeMember(1, 0, 0, 0, Vote::For,  cTerm),
        makeMember(2, 0, 0, 0, Vote::None, cTerm), // excluded
    };
    EXPECT_TRUE(ClusterMember::isUnanimousLeader(members, cTerm, 2));
}

// Wrong candidateTermId on one member → returns false (term mismatch).
TEST(ClusterMember, IsUnanimousLeader_WrongTermOnOne_False) {
    Term cTerm = 7;
    std::array<ClusterMember, 3> members = {
        makeMember(0, 0, 0, 0, Vote::For, cTerm),
        makeMember(1, 0, 0, 0, Vote::For, cTerm),
        makeMember(2, 0, 0, 0, Vote::For, 99),  // wrong term
    };
    EXPECT_FALSE(ClusterMember::isUnanimousLeader(members, cTerm, -1));
}

// ---------------------------------------------------------------------------
// 8. isQuorumLeader
//    ClusterMember.java:1015-1036
// ---------------------------------------------------------------------------

// Majority For, no Against → true.
TEST(ClusterMember, IsQuorumLeader_MajorityFor_NoAgainst_True) {
    Term cTerm = 5;
    std::array<ClusterMember, 3> members = {
        makeMember(0, 0, 0, 0, Vote::For,  cTerm),
        makeMember(1, 0, 0, 0, Vote::For,  cTerm),
        makeMember(2, 0, 0, 0, Vote::None, cTerm),
    };
    EXPECT_TRUE(ClusterMember::isQuorumLeader(members, cTerm));
}

// All 3 For → both unanimous and quorum leader.
TEST(ClusterMember, IsQuorumLeader_AllFor_True) {
    Term cTerm = 5;
    std::array<ClusterMember, 3> members = {
        makeMember(0, 0, 0, 0, Vote::For, cTerm),
        makeMember(1, 0, 0, 0, Vote::For, cTerm),
        makeMember(2, 0, 0, 0, Vote::For, cTerm),
    };
    EXPECT_TRUE(ClusterMember::isQuorumLeader(members, cTerm));
}

// Any Against vote → false even if majority For.
TEST(ClusterMember, IsQuorumLeader_AnyAgainst_False) {
    Term cTerm = 5;
    std::array<ClusterMember, 3> members = {
        makeMember(0, 0, 0, 0, Vote::For,     cTerm),
        makeMember(1, 0, 0, 0, Vote::For,     cTerm),
        makeMember(2, 0, 0, 0, Vote::Against, cTerm), // veto
    };
    EXPECT_FALSE(ClusterMember::isQuorumLeader(members, cTerm));
}

// Wrong candidateTermId members don't count toward the vote total.
TEST(ClusterMember, IsQuorumLeader_WrongTermMembersNotCounted) {
    Term cTerm = 5;
    // Only member 0 has the right term and votes For; threshold=2 → false.
    std::array<ClusterMember, 3> members = {
        makeMember(0, 0, 0, 0, Vote::For, cTerm),
        makeMember(1, 0, 0, 0, Vote::For, 99),   // wrong term
        makeMember(2, 0, 0, 0, Vote::For, 99),   // wrong term
    };
    EXPECT_FALSE(ClusterMember::isQuorumLeader(members, cTerm));
}

// Unanimous→both true; quorum leader→quorum true, unanimous false when one abstains.
TEST(ClusterMember, IsQuorumLeader_MajorityFor_IsUnanimous_False) {
    Term cTerm = 5;
    std::array<ClusterMember, 3> members = {
        makeMember(0, 0, 0, 0, Vote::For,  cTerm),
        makeMember(1, 0, 0, 0, Vote::For,  cTerm),
        makeMember(2, 0, 0, 0, Vote::None, cTerm), // abstain
    };
    EXPECT_TRUE(ClusterMember::isQuorumLeader(members, cTerm));
    EXPECT_FALSE(ClusterMember::isUnanimousLeader(members, cTerm, -1));
}

// ---------------------------------------------------------------------------
// 9. willVoteFor
//    ClusterMember.java:1292-1298
//    Returns: logPosition != NULL_POSITION && compareLog(voter, candidate) <= 0
// ---------------------------------------------------------------------------

// Candidate more up-to-date (higher term) → voter votes For.
TEST(ClusterMember, WillVoteFor_CandidateHigherTerm_True) {
    auto voter     = makeMember(0, 3, 100);
    voter.logPosition = 100; // not NULL_POSITION
    auto candidate = makeMember(1, 5, 50);
    EXPECT_TRUE(ClusterMember::willVoteFor(voter, candidate));
}

// Candidate equal log → voter votes For.
TEST(ClusterMember, WillVoteFor_EqualLog_True) {
    auto voter     = makeMember(0, 3, 100);
    auto candidate = makeMember(1, 3, 100);
    EXPECT_TRUE(ClusterMember::willVoteFor(voter, candidate));
}

// Candidate stale (voter has higher term) → false.
TEST(ClusterMember, WillVoteFor_CandidateStale_False) {
    auto voter     = makeMember(0, 7, 50);
    auto candidate = makeMember(1, 3, 200);
    EXPECT_FALSE(ClusterMember::willVoteFor(voter, candidate));
}

// Voter logPosition == NULL_POSITION → false regardless.
TEST(ClusterMember, WillVoteFor_NullPosition_False) {
    ClusterMember voter;
    voter.id               = 0;
    voter.leadershipTermId = 0;
    voter.logPosition      = kNullPosition; // NULL_POSITION
    auto candidate = makeMember(1, 5, 200);
    EXPECT_FALSE(ClusterMember::willVoteFor(voter, candidate));
}

// ---------------------------------------------------------------------------
// 10. reset() — clears vote + positions
//     ClusterMember.java:140-149
// ---------------------------------------------------------------------------
TEST(ClusterMember, Reset_ClearsFields) {
    ClusterMember m;
    m.id                         = 1;
    m.vote                       = Vote::For;
    m.candidateTermId            = 42;
    m.leadershipTermId           = 10;
    m.logPosition                = 999;
    m.timeOfLastAppendPositionNs = 12345;

    m.reset();

    EXPECT_EQ(m.vote,              Vote::None);
    EXPECT_EQ(m.candidateTermId,   kNullValue);
    EXPECT_EQ(m.leadershipTermId,  kNullValue);
    EXPECT_EQ(m.logPosition,       kNullPosition);
    // id and timeOfLastAppendPositionNs are NOT reset by Java reset()
    EXPECT_EQ(m.id, 1);
}

// ---------------------------------------------------------------------------
// P2P-5a additions — Task 1
// ---------------------------------------------------------------------------

// becomeCandidate: self gets vote=For+candidateTermId+isBallotSent=true; others cleared
TEST(ClusterMember, BecomeCandidate_SetsCorrectState) {
    std::vector<ClusterMember> members(3);
    members[0].id = 0; members[1].id = 1; members[2].id = 2;
    for (auto& m : members) m.logPosition = 0;

    ClusterMember::becomeCandidate(members, /*candidateTermId=*/5, /*candidateMemberId=*/0);

    EXPECT_EQ(members[0].vote, Vote::For);
    EXPECT_EQ(members[0].candidateTermId, 5);
    EXPECT_TRUE(members[0].isBallotSent);

    EXPECT_EQ(members[1].vote, Vote::None);
    EXPECT_EQ(members[1].candidateTermId, kNullValue);
    EXPECT_FALSE(members[1].isBallotSent);

    EXPECT_EQ(members[2].vote, Vote::None);
    EXPECT_EQ(members[2].candidateTermId, kNullValue);
    EXPECT_FALSE(members[2].isBallotSent);
}

// reset() clears isBallotSent
TEST(ClusterMember, Reset_ClearsBallotSent) {
    ClusterMember m;
    m.id = 1; m.isBallotSent = true; m.vote = Vote::For; m.candidateTermId = 3; m.logPosition = 10;
    m.reset();
    EXPECT_FALSE(m.isBallotSent);
    EXPECT_EQ(m.vote, Vote::None);
    EXPECT_EQ(m.candidateTermId, kNullValue);
    EXPECT_EQ(m.logPosition, kNullPosition);
}

// isUnanimousCandidate: all willVoteFor candidate → true
TEST(ClusterMember, IsUnanimousCandidate_AllWillVote_True) {
    std::vector<ClusterMember> members(3);
    for (int i = 0; i < 3; i++) {
        members[i].id = i;
        members[i].logPosition = 0;
        members[i].leadershipTermId = 0;
        members[i].logLeadershipTermId = 0;
    }
    ClusterMember candidate; candidate.id = 0; candidate.logPosition = 0; candidate.leadershipTermId = 0; candidate.logLeadershipTermId = 0;
    EXPECT_TRUE(ClusterMember::isUnanimousCandidate(members, candidate, /*gracefulClosedLeaderId=*/-1));
}

// isUnanimousCandidate: one member has higher log → false
TEST(ClusterMember, IsUnanimousCandidate_OneMemberAhead_False) {
    std::vector<ClusterMember> members(3);
    for (int i = 0; i < 3; i++) { members[i].id = i; members[i].logPosition = 0; members[i].logLeadershipTermId = 0; }
    members[1].logPosition = 100;    // member 1 is ahead via leadershipTermId comparison (but leadershipTermId=kNullValue default)
    members[1].leadershipTermId = 1; // higher leadershipTermId → won't vote for candidate at term=0
    ClusterMember candidate; candidate.id = 0; candidate.logPosition = 0; candidate.leadershipTermId = 0; candidate.logLeadershipTermId = 0;
    EXPECT_FALSE(ClusterMember::isUnanimousCandidate(members, candidate, -1));
}

// isQuorumCandidate: majority willVoteFor → true
TEST(ClusterMember, IsQuorumCandidate_Majority_True) {
    std::vector<ClusterMember> members(3);
    for (int i = 0; i < 3; i++) { members[i].id = i; members[i].logPosition = 0; members[i].leadershipTermId = 0; members[i].logLeadershipTermId = 0; }
    // member[2] has higher leadershipTermId → won't vote; but 0 and 1 will (majority=2 of 3)
    members[2].leadershipTermId = 5; // higher term → compareLog returns +1 → won't vote
    ClusterMember candidate; candidate.id = 0; candidate.logPosition = 0; candidate.leadershipTermId = 0; candidate.logLeadershipTermId = 0;
    EXPECT_TRUE(ClusterMember::isQuorumCandidate(members, candidate));
}

// isQuorumCandidate: minority willVoteFor → false
TEST(ClusterMember, IsQuorumCandidate_Minority_False) {
    std::vector<ClusterMember> members(3);
    for (int i = 0; i < 3; i++) { members[i].id = i; members[i].logPosition = 0; members[i].leadershipTermId = 0; members[i].logLeadershipTermId = 0; }
    members[0].logPosition = kNullPosition; // won't vote (logPosition == NULL)
    members[1].logPosition = kNullPosition; // won't vote
    // only member[2] would vote (logPosition=0 >= candidate 0); but 1 < quorumThreshold(3)=2
    ClusterMember candidate; candidate.id = 3; candidate.logPosition = 0; candidate.leadershipTermId = 0; candidate.logLeadershipTermId = 0;
    EXPECT_FALSE(ClusterMember::isQuorumCandidate(members, candidate));
}

// ===========================================================================
// C6: hasQuorumAtPosition must NOT count a member on a different log branch.
// Two members can share leadershipTermId yet have divergent log tails
// (different logLeadershipTermId). Counting them together over-counts quorum.
// ===========================================================================

// C6a: member with wrong logLeadershipTermId is excluded from quorum count.
TEST(ClusterMember, HasQuorumAtPosition_DifferentLogBranch_NotCounted) {
    // 3-member cluster: quorum threshold = 2
    std::array<ClusterMember, 3> members{};
    members[0].id = 0; members[0].leadershipTermId = 5; members[0].logLeadershipTermId = 5;
    members[0].logPosition = 100; members[0].timeOfLastAppendPositionNs = 1000;

    members[1].id = 1; members[1].leadershipTermId = 5; members[1].logLeadershipTermId = 4; // WRONG branch
    members[1].logPosition = 100; members[1].timeOfLastAppendPositionNs = 1000;

    members[2].id = 2; members[2].leadershipTermId = 5; members[2].logLeadershipTermId = 5;
    members[2].logPosition = 50;  members[2].timeOfLastAppendPositionNs = 1000; // behind

    // Only member 0 matches leadershipTermId=5 AND logLeadershipTermId=5 AND logPosition>=100.
    // Member 1 has wrong logLeadershipTermId=4 (different log branch — must not count).
    // Member 2 is behind (position 50 < 100).
    // Quorum threshold = 2; only 1 matching → false.
    bool q = ClusterMember::hasQuorumAtPosition(
        std::span<const ClusterMember>(members.data(), members.size()),
        /*leadershipTermId=*/5, /*logLeadershipTermId=*/5,
        /*position=*/100,
        /*nowNs=*/2000, /*timeoutNs=*/5000);
    EXPECT_FALSE(q) << "Member on wrong log branch (logLtId=4) must not count toward quorum";
}

// C6b: hasQuorumAtPosition returns true when quorum of members match both term fields.
TEST(ClusterMember, HasQuorumAtPosition_AllMatchingBranch_True) {
    std::array<ClusterMember, 3> members{};
    for (int i = 0; i < 3; i++) {
        members[i].id = i;
        members[i].leadershipTermId    = 5;
        members[i].logLeadershipTermId = 5;
        members[i].logPosition = 100;
        members[i].timeOfLastAppendPositionNs = 1000;
    }
    // All 3 match — quorum = 2, count = 3 → true.
    bool q = ClusterMember::hasQuorumAtPosition(
        std::span<const ClusterMember>(members.data(), members.size()),
        /*leadershipTermId=*/5, /*logLeadershipTermId=*/5,
        /*position=*/100,
        /*nowNs=*/2000, /*timeoutNs=*/5000);
    EXPECT_TRUE(q);
}

// ===========================================================================
// C2: compareLog same-term boundary tests (audit T3/C2)
// The classic off-by-one "elect a shorter log" case was untested.
// ===========================================================================

// C2a: same term, candidate strictly behind on position → compareLog returns +1 (voter more recent).
TEST(ClusterMember, CompareLog_SameTerm_ShorterCandidateIsBehind) {
    // Voter (term=5, pos=100) vs candidate (term=5, pos=99) — one behind.
    EXPECT_EQ(ClusterMember::compareLog(5, 100, 5, 99),  1)
        << "Same term, voter longer: must return +1";
    EXPECT_EQ(ClusterMember::compareLog(5,  99, 5, 100), -1)
        << "Same term, voter shorter: must return -1";
    EXPECT_EQ(ClusterMember::compareLog(5, 100, 5, 100),  0)
        << "Same term, equal position: must return 0";
}

// C2b: willVoteFor — same term, candidate strictly behind → willVoteFor = false.
TEST(ClusterMember, WillVoteFor_SameTerm_ShorterCandidate_ReturnsFalse) {
    ClusterMember voter;
    voter.leadershipTermId = 5;
    voter.logPosition      = 100;
    voter.logLeadershipTermId = 5;

    ClusterMember candidate;
    candidate.leadershipTermId = 5;
    candidate.logPosition      = 99; // one entry shorter
    candidate.logLeadershipTermId = 5;

    EXPECT_FALSE(ClusterMember::willVoteFor(voter, candidate))
        << "Voter with longer log must NOT vote for a same-term shorter-log candidate";
}

// C2c: willVoteFor — same term, equal position → willVoteFor = true.
TEST(ClusterMember, WillVoteFor_SameTerm_EqualPosition_ReturnsTrue) {
    ClusterMember voter;
    voter.leadershipTermId = 5;
    voter.logPosition      = 100;
    voter.logLeadershipTermId = 5;

    ClusterMember candidate;
    candidate.leadershipTermId = 5;
    candidate.logPosition      = 100;
    candidate.logLeadershipTermId = 5;

    EXPECT_TRUE(ClusterMember::willVoteFor(voter, candidate));
}
