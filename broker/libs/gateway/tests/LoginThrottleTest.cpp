// LoginThrottleTest.cpp - authentication Task 11 (D14): the brute-force curve.  Pure unit: no Aeron, no clock.
#include <rune/gateway/LoginThrottle.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <string>

using rune::gateway::LoginThrottle;
constexpr int64_t ms = 1'000'000;

TEST(LoginThrottleRegression, TheFirstTwoFailuresAreFreeThenTheDelayDoublesToTheCap) {
    LoginThrottle t; int64_t now = 0;
    t.onFailure("ws1", "10.0.0.9", now); t.onFailure("ws1", "10.0.0.9", now);
    EXPECT_TRUE(t.check("ws1", "10.0.0.9", now).allowed);
    uint32_t expected = 250;
    for (int k = 0; k < 12; ++k) {
        t.onFailure("ws1", "10.0.0.9", now);
        const auto v = t.check("ws1", "10.0.0.9", now);
        ASSERT_FALSE(v.allowed) << k;
        EXPECT_EQ(v.retryAfterMs, std::min<uint32_t>(expected, 30'000)) << k;
        now += static_cast<int64_t>(v.retryAfterMs) * ms;          // wait it out
        EXPECT_TRUE(t.check("ws1", "10.0.0.9", now).allowed);
        expected *= 2;
    }
}

TEST(LoginThrottleRegression, AttackerAtOneAddressNeverDelaysTheUserAtAnotherRegression) {
    LoginThrottle t;
    for (int i = 0; i < 1000; ++i) t.onFailure("ws1", "203.0.113.7", 0);
    EXPECT_FALSE(t.check("ws1", "203.0.113.7", 0).allowed);
    EXPECT_TRUE(t.check("ws1", "10.0.0.9", 0).allowed) << "no hard lockout (spec §5)";
}

TEST(LoginThrottleRegression, ManyUsersFromOneAddressAreThrottledPerAddress) {
    LoginThrottle t;
    for (int i = 0; i < 11; ++i) t.onFailure("user" + std::to_string(i), "203.0.113.7", 0);
    EXPECT_FALSE(t.check("fresh-user", "203.0.113.7", 0).allowed) << "the host key (10 free) throttles a sweep";
    for (int i = 0; i < 10; ++i) t.onFailure("u" + std::to_string(i), "198.51.100.1", 0);
    EXPECT_TRUE(t.check("fresh-user", "198.51.100.1", 0).allowed) << "10 failures from an address are still free";
}

TEST(LoginThrottleRegression, SuccessResetsThePairAndIdleEntriesAreForgotten) {
    LoginThrottle t;
    for (int i = 0; i < 5; ++i) t.onFailure("ws1", "h", 0);
    t.onSuccess("ws1", "h");
    EXPECT_TRUE(t.check("ws1", "h", 0).allowed);
    for (int i = 0; i < 5; ++i) t.onFailure("ws2", "h2", 0);
    EXPECT_FALSE(t.check("ws2", "h2", 0).allowed);
    EXPECT_TRUE(t.check("ws2", "h2", LoginThrottle::kForgetAfterNs + 1).allowed);
    // A forgotten entry starts again from a free failure, not from where it left off.
    t.onFailure("ws2", "h2", LoginThrottle::kForgetAfterNs + 1);
    EXPECT_TRUE(t.check("ws2", "h2", LoginThrottle::kForgetAfterNs + 1).allowed);
}

TEST(LoginThrottleRegression, AFullTableEvictsTheOldestNeverAllocatesNorRefuses) {
    LoginThrottle t;
    // 10 000 distinct (user, host) pairs, each failing 3 times (past the 2 free), time moving forward.
    int64_t now = 1;
    for (int i = 0; i < 10'000; ++i) {
        const std::string u = "user" + std::to_string(i), h = "10." + std::to_string(i / 250) + "." + std::to_string(i % 250) + ".1";
        for (int k = 0; k < 3; ++k) t.onFailure(u, h, now);
        ++now;
    }
    // The newest entries are throttled (the table evicted the oldest to hold them) ...
    for (int i = 9'990; i < 10'000; ++i) {
        const std::string u = "user" + std::to_string(i), h = "10." + std::to_string(i / 250) + "." + std::to_string(i % 250) + ".1";
        EXPECT_FALSE(t.check(u, h, now).allowed) << i;
    }
    // ... and a brand-new pair is never refused by a full table.
    EXPECT_TRUE(t.check("brand-new", "192.0.2.77", now).allowed);
    t.onFailure("brand-new", "192.0.2.77", now);
    EXPECT_TRUE(t.check("brand-new", "192.0.2.77", now).allowed) << "one failure is free";
}

TEST(LoginThrottleRegression, AnAttemptInsideTheDelayReportsTheRemainingTime) {
    LoginThrottle t;
    for (int i = 0; i < 3; ++i) t.onFailure("a", "h", 0);   // 250 ms
    const auto v = t.check("a", "h", 100 * ms);
    ASSERT_FALSE(v.allowed);
    EXPECT_EQ(v.retryAfterMs, 150u);
}

// Regression (Task 20, found by the docker e2e): the login log lines printed `source=<binary garbage>` - the gateway bound a
// std::string_view to Aeron's by-value Image::sourceIdentity() and read freed memory, so the throttle's host key was random.
TEST(LoginThrottleRegression, SourceHostOfAnAeronImageIsTheHostNeverGarbageRegression) {
    using rune::gateway::hostOfSourceIdentity;
    EXPECT_EQ(hostOfSourceIdentity("172.18.0.4:51234"), "172.18.0.4");
    EXPECT_EQ(hostOfSourceIdentity("[::1]:5678"), "::1");
    EXPECT_EQ(hostOfSourceIdentity("[fe80::1]:40200"), "fe80::1");
    EXPECT_EQ(hostOfSourceIdentity("10.0.0.9"), "10.0.0.9");          // no port
    EXPECT_EQ(hostOfSourceIdentity("aeron:ipc"), "");                 // IPC: the caller falls back to the reply endpoint
    EXPECT_EQ(hostOfSourceIdentity(""), "");
    // The result owns its bytes: it survives the source string (the by-value temporary the Aeron accessor returns).
    std::string host;
    {
        std::string temporary = "192.168.7.7:9999";
        host = hostOfSourceIdentity(temporary);
        temporary.assign(temporary.size(), 'X');                       // clobber the source
    }
    EXPECT_EQ(host, "192.168.7.7");
    EXPECT_EQ(hostOfSourceIdentity(std::string("203.0.113.5:1")), "203.0.113.5");   // a temporary argument
}
