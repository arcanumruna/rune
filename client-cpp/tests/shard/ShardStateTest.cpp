// tests/shard/ShardStateTest.cpp — T15b fix round 1: the shard state is DERIVED
// each duty cycle from the Client's own sessions (design note §4): a pure
// function of a DutyView, tested row by row against the transition table.
#include <rune/client/shard/ShardState.h>

#include <gtest/gtest.h>

namespace rc = rune::client;
namespace rs = rune::client::shard;
using SS = rune::client::session::SessionState;

namespace {
rc::DutyView view(SS seed, bool everLive) {
    rc::DutyView v;
    v.seedState     = seed;
    v.seedEverLive  = everLive;
    v.seedSessionId = everLive ? 42 : 0;
    return v;
}
} // namespace

TEST(ShardStateRegression, NeverConnectedIsIdle) {
    EXPECT_EQ(rs::deriveShardStatus(view(SS::Disconnected, false), 0).state, rs::ShardState::Idle);
    EXPECT_EQ(rs::deriveShardStatus(view(SS::Closed, false), 0).state, rs::ShardState::Idle);
}

TEST(ShardStateRegression, ConnectIssuedIsConnecting) {
    EXPECT_EQ(rs::deriveShardStatus(view(SS::Connecting, false), 0).state, rs::ShardState::Connecting);
    EXPECT_EQ(rs::deriveShardStatus(view(SS::Resuming, false), 0).state, rs::ShardState::Connecting);
}

TEST(ShardStateRegression, SeedAndEveryLeaderConnectedIsLive) {
    rc::DutyView v   = view(SS::Connected, true);
    v.leaderSessions = 2;
    const auto s     = rs::deriveShardStatus(v, 3);
    EXPECT_EQ(s.state, rs::ShardState::Live);
    EXPECT_EQ(s.shard, 3u);
    EXPECT_EQ(s.sessionId, 42);
    EXPECT_EQ(s.downMask, 0u);
}

TEST(ShardStateRegression, LeaderDownIsDegradedWithItsMask) {
    rc::DutyView v    = view(SS::Connected, true);
    v.leaderSessions  = 3;
    v.leadersDownMask = 0b010;
    const auto s      = rs::deriveShardStatus(v, 0);
    EXPECT_EQ(s.state, rs::ShardState::Degraded);
    EXPECT_EQ(s.downMask, 0b010u);
}

TEST(ShardStateRegression, SeedLostAfterLiveIsReconnectingNeverIdleOrFailed) {
    for (const SS st : {SS::Connecting, SS::Resuming, SS::Disconnected, SS::Backoff}) {
        rc::DutyView v      = view(st, true);
        v.seedBackoffAttempt = 4;
        const auto s        = rs::deriveShardStatus(v, 0);
        EXPECT_EQ(s.state, rs::ShardState::Reconnecting) << static_cast<int>(st);
        EXPECT_EQ(s.attempt, 4);
    }
}

TEST(ShardStateRegression, SeedRejectedIsFailedRejectedBeforeOrAfterLive) {
    for (const bool everLive : {false, true}) {
        rc::DutyView v   = view(SS::Rejected, everLive);
        v.seedRejectCode = 9;
        const auto s     = rs::deriveShardStatus(v, 0);
        EXPECT_EQ(s.state, rs::ShardState::Failed);
        EXPECT_EQ(s.failure, rs::ShardFailure::Rejected);
        EXPECT_EQ(s.rejectCode, 9);
    }
}

TEST(ShardStateRegression, TransportFailedWinsOverEverything) {
    for (const SS st : {SS::Connected, SS::Connecting, SS::Backoff, SS::Rejected}) {
        rc::DutyView v      = view(st, true);
        v.transportFailed   = true;
        v.producerClosing   = true;
        const auto s        = rs::deriveShardStatus(v, 0);
        EXPECT_EQ(s.state, rs::ShardState::Failed);
        EXPECT_EQ(s.failure, rs::ShardFailure::TransportClosed);
    }
}

TEST(ShardStateRegression, BeginCloseIsDrainingInAnyRunningState) {
    for (const SS st : {SS::Connected, SS::Connecting, SS::Backoff, SS::Disconnected}) {
        rc::DutyView v    = view(st, st == SS::Connected);
        v.producerClosing = true;
        EXPECT_EQ(rs::deriveShardStatus(v, 0).state, rs::ShardState::Draining) << static_cast<int>(st);
    }
}

TEST(ShardStateRegression, PublishOnlyOnChange) {
    rc::DutyView v = view(SS::Connected, true);
    const auto a   = rs::deriveShardStatus(v, 0);
    EXPECT_TRUE(rs::sameStatus(a, rs::deriveShardStatus(v, 0)));
    v.leaderSessions  = 1;
    v.leadersDownMask = 1;
    EXPECT_FALSE(rs::sameStatus(a, rs::deriveShardStatus(v, 0)));
    EXPECT_STREQ(rs::toString(rs::ShardState::Degraded), "Degraded");
}
