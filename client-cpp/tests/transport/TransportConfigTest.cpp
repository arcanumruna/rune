// tests/transport/TransportConfigTest.cpp — explicit Aeron knob validation (no driver).
#include <rune/client/transport/TransportConfig.h>

#include <gtest/gtest.h>

#include <cstdlib>

using rune::client::transport::TransportConfig;
using rune::client::transport::validate;
using rune::client::transport::applyEnvOverrides;

// F12: the default MTU is Aeron's 1408 (fits a 1500-byte path unfragmented);
// 8 KiB jumbo is opt-in only (project rule: jumbo MTU never the default).
TEST(TransportConfig, DefaultsAreValidAndMtuIsAeronDefaultRegression) {
    TransportConfig c;
    EXPECT_EQ(c.mtuLength, 1408u);
    EXPECT_EQ(c.mtuLength, rune::client::transport::kAeronDefaultMtuLength);
    EXPECT_EQ(rune::client::transport::kJumboMtuLength, 8192u);
    EXPECT_EQ(c.rcvInitialWindowLength, 0u);
    EXPECT_EQ(c.socketSndBufBytes, 2u * 1024 * 1024);
    EXPECT_EQ(c.socketRcvBufBytes, 2u * 1024 * 1024);
    EXPECT_EQ(c.driverIdleStrategy, "sleep-ns");
    EXPECT_FALSE(validate(c).has_value());
}

TEST(TransportConfig, RejectsMtuNotMultipleOf32) {
    TransportConfig c; c.mtuLength = 8190;
    ASSERT_TRUE(validate(c).has_value());
    EXPECT_NE(validate(c)->find("multiple of 32"), std::string::npos);
}

TEST(TransportConfig, RejectsMtuOutsideAeronBounds) {
    TransportConfig c; c.mtuLength = 32;
    EXPECT_TRUE(validate(c).has_value());
    c.mtuLength = 65536;
    EXPECT_TRUE(validate(c).has_value());
    c.mtuLength = 65504;
    EXPECT_FALSE(validate(c).has_value());
}

TEST(TransportConfig, RejectsTermLengthNotPowerOfTwoOrOutOfRange) {
    TransportConfig c; c.termBufferLength = 3u * 1024 * 1024;
    EXPECT_TRUE(validate(c).has_value());
    c.termBufferLength = 32u * 1024;
    EXPECT_TRUE(validate(c).has_value());
    c.termBufferLength = 256u * 1024;
    EXPECT_FALSE(validate(c).has_value());
}

// B6 review: Aeron's max message is term/8.  A term whose max message cannot carry one maximum record
// (16 KiB + batch/frame headers) accepted the record at append and then failed every offer (a fatal
// max-message throw that looked like back-pressure) until the delivery timeout.  Refuse it at config time.
TEST(TransportConfigRegression, TermTooSmallForTheMaxRecordIsRejected) {
    TransportConfig c;
    c.termBufferLength = 64u * 1024;    // max message 8 KiB
    ASSERT_TRUE(validate(c).has_value());
    EXPECT_NE(validate(c)->find("16 KiB"), std::string::npos) << *validate(c);
    c.termBufferLength = 128u * 1024;   // max message 16 KiB: no room for the headers
    EXPECT_TRUE(validate(c).has_value());
    c.termBufferLength = 256u * 1024;   // max message 32 KiB
    EXPECT_FALSE(validate(c).has_value());
}

TEST(TransportConfig, RejectsSocketBufferSmallerThanMtu) {
    TransportConfig c;
    c.mtuLength = 8192;  // opt-in jumbo: a 4 KiB socket buffer cannot hold one datagram
    c.socketSndBufBytes = 4096;
    ASSERT_TRUE(validate(c).has_value());
    EXPECT_NE(validate(c)->find("socketSndBufBytes"), std::string::npos);
    c.socketSndBufBytes = 0;  // 0 = OS default, allowed
    c.socketRcvBufBytes = 4096;
    ASSERT_TRUE(validate(c).has_value());
    EXPECT_NE(validate(c)->find("socketRcvBufBytes"), std::string::npos);
}

TEST(TransportConfig, EnvOverridesMtuSocketBuffersAndIdle) {
    ::setenv("RUNE_CLIENT_AERON_MTU", "16384", 1);
    ::setenv("RUNE_AERON_SOCKET_BUFFER_BYTES", "4194304", 1);
    ::setenv("RUNE_CLIENT_AERON_DRIVER_IDLE", "backoff", 1);
    TransportConfig c;
    const auto warnings = applyEnvOverrides(c);
    ::unsetenv("RUNE_CLIENT_AERON_MTU");
    ::unsetenv("RUNE_AERON_SOCKET_BUFFER_BYTES");
    ::unsetenv("RUNE_CLIENT_AERON_DRIVER_IDLE");
    EXPECT_TRUE(warnings.empty());
    EXPECT_EQ(c.mtuLength, 16384u);
    EXPECT_EQ(c.socketSndBufBytes, 4194304u);
    EXPECT_EQ(c.socketRcvBufBytes, 4194304u);
    EXPECT_EQ(c.driverIdleStrategy, "backoff");
}

TEST(TransportConfig, EnvOverridesMtuSocketBuffersAndIdleWarnNothing) {
    ::setenv("RUNE_CLIENT_AERON_MTU", "8192", 1);
    ::setenv("RUNE_AERON_SOCKET_BUFFER_BYTES", "0", 1);  // 0 = OS default: valid
    TransportConfig c;
    const auto w = applyEnvOverrides(c);
    ::unsetenv("RUNE_CLIENT_AERON_MTU");
    ::unsetenv("RUNE_AERON_SOCKET_BUFFER_BYTES");
    EXPECT_TRUE(w.empty());
    EXPECT_EQ(c.mtuLength, 8192u);
    EXPECT_EQ(c.socketSndBufBytes, 0u);
    EXPECT_EQ(c.socketRcvBufBytes, 0u);
}

// F12: a malformed value is not applied AND is reported (never silently dropped).
TEST(TransportConfig, MalformedEnvIsReportedNotSilentlyDroppedRegression) {
    ::setenv("RUNE_CLIENT_AERON_MTU", "8k", 1);
    TransportConfig c;
    const auto w = applyEnvOverrides(c);
    ::unsetenv("RUNE_CLIENT_AERON_MTU");
    EXPECT_EQ(c.mtuLength, 1408u);
    ASSERT_EQ(w.size(), 1u);
    EXPECT_NE(w[0].find("RUNE_CLIENT_AERON_MTU"), std::string::npos);
    EXPECT_NE(w[0].find("8k"), std::string::npos);
}

// F12: "-1" used to parse (strtoull) as ULLONG_MAX == the "unset" sentinel and
// vanish silently; any other negative wrapped to a huge buffer and was APPLIED.
// Negative values are now rejected with a warning and never applied.
TEST(TransportConfig, NegativeSocketBufferEnvIsRejectedRegression) {
    for (const char* bad : {"-1", "-5", "+4096", " 4096", "4096 ", "99999999999999999999999"}) {
        ::setenv("RUNE_AERON_SOCKET_BUFFER_BYTES", bad, 1);
        TransportConfig c;
        const auto w = applyEnvOverrides(c);
        ::unsetenv("RUNE_AERON_SOCKET_BUFFER_BYTES");
        EXPECT_EQ(c.socketSndBufBytes, 2u * 1024 * 1024) << bad;
        EXPECT_EQ(c.socketRcvBufBytes, 2u * 1024 * 1024) << bad;
        ASSERT_EQ(w.size(), 1u) << bad;
        EXPECT_NE(w[0].find("RUNE_AERON_SOCKET_BUFFER_BYTES"), std::string::npos) << bad;
    }
}

TEST(TransportConfig, UnsetOrEmptyEnvIsNotAWarning) {
    ::setenv("RUNE_CLIENT_AERON_MTU", "", 1);
    ::unsetenv("RUNE_AERON_SOCKET_BUFFER_BYTES");
    TransportConfig c;
    const auto w = applyEnvOverrides(c);
    ::unsetenv("RUNE_CLIENT_AERON_MTU");
    EXPECT_TRUE(w.empty());
    EXPECT_EQ(c.mtuLength, 1408u);
}

// F12: Aeron's driver refuses to start when the receiver window exceeds the
// socket's SO_RCVBUF (aeron_driver.c "Window length greater than socket
// SO_RCVBUF") — validate() says so up front, against the 128 KiB default window
// or the configured one.
TEST(TransportConfig, RejectsSocketRcvBufBelowReceiveWindowRegression) {
    TransportConfig c;
    c.socketRcvBufBytes = 64u * 1024;  // >= MTU, but below the 128 KiB window
    ASSERT_TRUE(validate(c).has_value());
    EXPECT_NE(validate(c)->find("receive window"), std::string::npos) << *validate(c);
    EXPECT_NE(validate(c)->find("131072"), std::string::npos) << *validate(c);
    c.socketRcvBufBytes = 128u * 1024;
    EXPECT_FALSE(validate(c).has_value());
    c.socketRcvBufBytes = 0;  // OS default: not checkable here, allowed
    EXPECT_FALSE(validate(c).has_value());

    c.rcvInitialWindowLength = 512u * 1024;  // configured window
    c.socketRcvBufBytes      = 256u * 1024;
    ASSERT_TRUE(validate(c).has_value());
    EXPECT_NE(validate(c)->find("rcvInitialWindowLength"), std::string::npos) << *validate(c);
    c.socketRcvBufBytes = 512u * 1024;
    EXPECT_FALSE(validate(c).has_value());
}

TEST(TransportConfig, RejectsReceiveWindowBelowMtu) {
    TransportConfig c;
    c.mtuLength              = 8192;
    c.rcvInitialWindowLength = 4096;
    ASSERT_TRUE(validate(c).has_value());
    EXPECT_NE(validate(c)->find("rcvInitialWindowLength"), std::string::npos);
}

// Regression (Task 6 review I1): the Aeron C++ client sends a driver keepalive
// every 500 ms (ClientConductor.h:47) and takes its inter-service timeout from
// the driver's liveness value, so liveness <= ~500 ms times out a HEALTHY,
// properly pumped client.  validate() must reject it (floor 1 s = 2x keepalive).
TEST(TransportConfig, RejectsLivenessBelowKeepaliveFloorRegression) {
    using rune::client::transport::kMinClientLivenessTimeoutNs;
    static_assert(kMinClientLivenessTimeoutNs == 1'000'000'000LL);
    TransportConfig c;
    for (int64_t bad : {int64_t{0}, int64_t{1}, int64_t{200'000'000}, int64_t{500'000'000},
                        kMinClientLivenessTimeoutNs - 1}) {
        c.clientLivenessTimeoutNs = bad;
        ASSERT_TRUE(validate(c).has_value()) << bad;
        EXPECT_NE(validate(c)->find("clientLivenessTimeoutNs"), std::string::npos);
        EXPECT_NE(validate(c)->find("keepalive"), std::string::npos);
    }
    c.clientLivenessTimeoutNs = kMinClientLivenessTimeoutNs;
    EXPECT_FALSE(validate(c).has_value());
}

// Regression (Task 6 review I2): the transport derives the driver's
// publication_unblock_timeout as 1.5x liveness; the driver conductor casts it
// to int64, which overflows above ~6.1e18 ns.  Cap liveness at 1 hour.
TEST(TransportConfig, RejectsLivenessAboveOneHourCapRegression) {
    using rune::client::transport::kMaxClientLivenessTimeoutNs;
    static_assert(kMaxClientLivenessTimeoutNs == 3'600'000'000'000LL);
    TransportConfig c;
    c.clientLivenessTimeoutNs = kMaxClientLivenessTimeoutNs;
    EXPECT_FALSE(validate(c).has_value());
    for (int64_t bad : {kMaxClientLivenessTimeoutNs + 1, int64_t{6'200'000'000'000'000'000LL},
                        INT64_MAX}) {
        c.clientLivenessTimeoutNs = bad;
        ASSERT_TRUE(validate(c).has_value()) << bad;
        EXPECT_NE(validate(c)->find("clientLivenessTimeoutNs"), std::string::npos);
    }
}

// UDP GSO/GRO (rune-broker spec 2026-10-03-aeron-udp-gso-gro): default on; env forces it off / sets batching.
TEST(TransportConfig, UdpGsoDefaultsOnEnvTurnsOffAndSetsMsgsPerSend) {
    EXPECT_TRUE(TransportConfig{}.udpGso);
    EXPECT_EQ(TransportConfig{}.maxMessagesPerSend, 0u);
    ::setenv("RUNE_CLIENT_AERON_UDP_GSO", "0", 1);
    ::setenv("RUNE_CLIENT_AERON_MAX_MSGS_PER_SEND", "8", 1);
    TransportConfig c;
    const auto w = applyEnvOverrides(c);
    ::unsetenv("RUNE_CLIENT_AERON_UDP_GSO");
    ::unsetenv("RUNE_CLIENT_AERON_MAX_MSGS_PER_SEND");
    EXPECT_TRUE(w.empty());
    EXPECT_FALSE(c.udpGso);
    EXPECT_EQ(c.maxMessagesPerSend, 8u);

    ::setenv("RUNE_CLIENT_AERON_UDP_GSO", "1", 1);
    TransportConfig on; on.udpGso = false;
    (void)applyEnvOverrides(on);
    ::unsetenv("RUNE_CLIENT_AERON_UDP_GSO");
    EXPECT_TRUE(on.udpGso);

    ::setenv("RUNE_CLIENT_AERON_MAX_MSGS_PER_SEND", "17", 1);
    TransportConfig d;
    const auto w2 = applyEnvOverrides(d);
    ::unsetenv("RUNE_CLIENT_AERON_MAX_MSGS_PER_SEND");
    EXPECT_EQ(d.maxMessagesPerSend, 0u);
    ASSERT_EQ(w2.size(), 1u);
    EXPECT_NE(w2[0].find("RUNE_CLIENT_AERON_MAX_MSGS_PER_SEND"), std::string::npos);
}

// A7 (rune-broker findings §4.12): epoll-first transport polling in the embedded driver — default on, env off.
TEST(TransportConfig, EpollPollDefaultsOnAndEnvTurnsItOff) {
    EXPECT_TRUE(TransportConfig{}.epollPoll);
    ::setenv("RUNE_CLIENT_AERON_EPOLL_POLL", "0", 1);
    TransportConfig c;
    const auto w = applyEnvOverrides(c);
    ::unsetenv("RUNE_CLIENT_AERON_EPOLL_POLL");
    EXPECT_TRUE(w.empty());
    EXPECT_FALSE(c.epollPoll);
    ::setenv("RUNE_CLIENT_AERON_EPOLL_POLL", "1", 1);
    TransportConfig on; on.epollPoll = false;
    (void)applyEnvOverrides(on);
    ::unsetenv("RUNE_CLIENT_AERON_EPOLL_POLL");
    EXPECT_TRUE(on.epollPoll);
}

// Review D3: the on/off knobs take the broker's 0/1/false/true.  "false" used to be parsed as a number, rejected
// with a warning, and the knob silently stayed ON (an A/B off arm that measured the on arm).
TEST(TransportConfigRegression, OnOffKnobsAcceptFalseTrueAndWarnOnGarbageRegression) {
    for (const char* name : {"RUNE_CLIENT_AERON_EPOLL_POLL", "RUNE_CLIENT_AERON_UDP_GSO"}) {
        for (const auto& [v, want] : {std::pair{"false", false}, std::pair{"0", false},
                                      std::pair{"true", true}, std::pair{"1", true}}) {
            ::setenv(name, v, 1);
            TransportConfig c;
            c.epollPoll = !want;
            c.udpGso = !want;
            const auto w = applyEnvOverrides(c);
            ::unsetenv(name);
            EXPECT_TRUE(w.empty()) << name << "=" << v;
            EXPECT_EQ(std::string(name) == "RUNE_CLIENT_AERON_EPOLL_POLL" ? c.epollPoll : c.udpGso, want)
                << name << "=" << v;
        }
        for (const char* bad : {"off", "2", "yes"}) {
            ::setenv(name, bad, 1);
            TransportConfig c;
            const auto w = applyEnvOverrides(c);
            ::unsetenv(name);
            ASSERT_EQ(w.size(), 1u) << name << "=" << bad;
            EXPECT_NE(w[0].find("must be 0/1/false/true"), std::string::npos) << w[0];
            EXPECT_TRUE(c.epollPoll && c.udpGso) << "garbage keeps the configured value";
        }
    }
}
