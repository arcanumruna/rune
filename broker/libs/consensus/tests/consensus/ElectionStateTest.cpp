// ElectionStateTest.cpp — Faithful port: ElectionState enum + code()/fromCode().
// All 18 ordinals, round-trips, and error cases.

#include <gtest/gtest.h>
#include <rune/consensus/ElectionState.h>
#include <stdexcept>

using namespace rune::consensus;

// ---------------------------------------------------------------------------
// 1. Ordinal values — every state must match the Java enum code.
//    ElectionState.java:26-115: code == ordinal for all 18 states.
// ---------------------------------------------------------------------------
TEST(ElectionState, OrdinalInit) {
    EXPECT_EQ(code(ElectionState::Init), 0);
}
TEST(ElectionState, OrdinalCanvass) {
    EXPECT_EQ(code(ElectionState::Canvass), 1);
}
TEST(ElectionState, OrdinalNominate) {
    EXPECT_EQ(code(ElectionState::Nominate), 2);
}
TEST(ElectionState, OrdinalCandidateBallot) {
    EXPECT_EQ(code(ElectionState::CandidateBallot), 3);
}
TEST(ElectionState, OrdinalFollowerBallot) {
    EXPECT_EQ(code(ElectionState::FollowerBallot), 4);
}
TEST(ElectionState, OrdinalLeaderLogReplication) {
    EXPECT_EQ(code(ElectionState::LeaderLogReplication), 5);
}
TEST(ElectionState, OrdinalLeaderReplay) {
    EXPECT_EQ(code(ElectionState::LeaderReplay), 6);
}
TEST(ElectionState, OrdinalLeaderInit) {
    EXPECT_EQ(code(ElectionState::LeaderInit), 7);
}
TEST(ElectionState, OrdinalLeaderReady) {
    EXPECT_EQ(code(ElectionState::LeaderReady), 8);
}
TEST(ElectionState, OrdinalFollowerLogReplication) {
    EXPECT_EQ(code(ElectionState::FollowerLogReplication), 9);
}
TEST(ElectionState, OrdinalFollowerReplay) {
    EXPECT_EQ(code(ElectionState::FollowerReplay), 10);
}
TEST(ElectionState, OrdinalFollowerCatchupInit) {
    EXPECT_EQ(code(ElectionState::FollowerCatchupInit), 11);
}
TEST(ElectionState, OrdinalFollowerCatchupAwait) {
    EXPECT_EQ(code(ElectionState::FollowerCatchupAwait), 12);
}
TEST(ElectionState, OrdinalFollowerCatchup) {
    EXPECT_EQ(code(ElectionState::FollowerCatchup), 13);
}
TEST(ElectionState, OrdinalFollowerLogInit) {
    EXPECT_EQ(code(ElectionState::FollowerLogInit), 14);
}
TEST(ElectionState, OrdinalFollowerLogAwait) {
    EXPECT_EQ(code(ElectionState::FollowerLogAwait), 15);
}
TEST(ElectionState, OrdinalFollowerReady) {
    EXPECT_EQ(code(ElectionState::FollowerReady), 16);
}
TEST(ElectionState, OrdinalClosed) {
    EXPECT_EQ(code(ElectionState::Closed), 17);
}

// ---------------------------------------------------------------------------
// 2. fromCode() round-trip for all 18 states.
// ---------------------------------------------------------------------------
TEST(ElectionState, RoundTripAll18) {
    for (int32_t c = 0; c <= 17; ++c) {
        ElectionState s = fromCode(c);
        EXPECT_EQ(code(s), c) << "round-trip failed for code=" << c;
    }
}

// ---------------------------------------------------------------------------
// 3. fromCode() throws for out-of-range codes.
//    Mirrors ElectionState.java:148-151: throws ClusterException.
// ---------------------------------------------------------------------------
TEST(ElectionState, FromCode_Negative_Throws) {
    EXPECT_THROW(fromCode(-1), std::out_of_range);
}
TEST(ElectionState, FromCode_TooLarge_Throws) {
    EXPECT_THROW(fromCode(18), std::out_of_range);
}
TEST(ElectionState, FromCode_WayTooLarge_Throws) {
    EXPECT_THROW(fromCode(1000), std::out_of_range);
}

// ---------------------------------------------------------------------------
// 4. Specific fromCode() correctness checks (not just round-trip).
// ---------------------------------------------------------------------------
TEST(ElectionState, FromCode_0_IsInit) {
    EXPECT_EQ(fromCode(0), ElectionState::Init);
}
TEST(ElectionState, FromCode_17_IsClosed) {
    EXPECT_EQ(fromCode(17), ElectionState::Closed);
}
TEST(ElectionState, FromCode_5_IsLeaderLogReplication) {
    EXPECT_EQ(fromCode(5), ElectionState::LeaderLogReplication);
}
TEST(ElectionState, FromCode_8_IsLeaderReady) {
    EXPECT_EQ(fromCode(8), ElectionState::LeaderReady);
}
