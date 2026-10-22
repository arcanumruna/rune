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
    t.onSuccess("ws1", "10.0.0.9", 0);   // the user's own address: a known pair
    for (int i = 0; i < 1000; ++i) t.onFailure("ws1", "203.0.113.7", 0);
    EXPECT_FALSE(t.check("ws1", "203.0.113.7", 0).allowed);
    EXPECT_TRUE(t.check("ws1", "10.0.0.9", 0).allowed) << "no hard lockout (spec §5): a known pair is never delayed";
    // #131: from an address the user never logged in from, the per-user component delays it - by its low cap only.
    const auto fresh = t.check("ws1", "192.0.2.1", 0);
    EXPECT_FALSE(fresh.allowed);
    EXPECT_LE(fresh.retryAfterMs, 2'000u);
}

// Issue #131: the source host is attacker-influenced (an IPv6 /64, a forged reply endpoint), so (user, host) and host
// alone gave every address its own free guesses and its own exponential schedule against one user.
TEST(LoginThrottleRegression, PerUserBackoffAppliesAcrossSourceHostsRegression) {
    LoginThrottle t;
    t.onSuccess("u", "10.0.0.9", 0);   // the real user's own address
    for (int h = 0; h < 100; ++h)
        for (int k = 0; k < 3; ++k) t.onFailure("u", "2001:db8::" + std::to_string(h), 0);
    const auto v = t.check("u", "2001:db8::fresh", 0);
    ASSERT_FALSE(v.allowed) << "100 hosts x 3 guesses: the next attempt from a fresh host is delayed";
    EXPECT_GT(v.retryAfterMs, 0u);
    EXPECT_LE(v.retryAfterMs, static_cast<uint32_t>(LoginThrottle::kMaxUserDelayNs / ms)) << "a low ceiling, not a lockout";
    EXPECT_TRUE(t.check("u", "2001:db8::fresh", LoginThrottle::kMaxUserDelayNs).allowed) << "waiting the cap out works";
    EXPECT_TRUE(t.check("u", "10.0.0.9", 0).allowed) << "a success from a known (user, host) pair bypasses it";
    EXPECT_TRUE(t.check("other", "2001:db8::fresh", 0).allowed) << "per user: another user is untouched";
    // The first kFreeUserFailures of a user are free across hosts.
    LoginThrottle f;
    for (uint32_t i = 0; i < LoginThrottle::kFreeUserFailures; ++i) f.onFailure("v", "198.51.100." + std::to_string(i), 0);
    EXPECT_TRUE(f.check("v", "192.0.2.200", 0).allowed);
    f.onFailure("v", "198.51.100.250", 0);
    EXPECT_FALSE(f.check("v", "192.0.2.200", 0).allowed);
}

// Issue #131: the tables were keyed by an unkeyed FNV, so an attacker could grind names into one 8-slot probe window
// and evict its own entries (a fresh schedule).  The hash is keyed per process: two throttles place names differently.
TEST(LoginThrottleRegression, TableKeysAreNotPredictableRegression) {
    LoginThrottle a, b;
    int differ = 0;
    for (int i = 0; i < 32; ++i) {
        const std::string u = "user" + std::to_string(i);
        differ += a.pairSlotForTest(u, "10.0.0.9") != b.pairSlotForTest(u, "10.0.0.9");
    }
    EXPECT_GE(differ, 28) << "random keys: the same names land in unrelated slots (a 1/4096 chance each to agree)";
    // A fixed key is deterministic, and the two parts of a key never run into each other.
    LoginThrottle::HashKey k{};
    k[0] = 1;
    LoginThrottle c(k), d(k);
    EXPECT_EQ(c.pairSlotForTest("ws1", "10.0.0.9"), d.pairSlotForTest("ws1", "10.0.0.9"));
    for (int i = 0; i < 3; ++i) c.onFailure("ab", "c", 0);
    EXPECT_FALSE(c.check("ab", "c", 0).allowed);
    EXPECT_TRUE(c.check("a", "bc", 0).allowed) << "user|host is not ambiguous";
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
    t.onSuccess("ws1", "h", 0);
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
