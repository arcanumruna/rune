// tests/transport/TransportConfigTest.cpp — explicit Aeron knob validation (no driver).
#include <rune/client/transport/TransportConfig.h>

#include <gtest/gtest.h>

#include <cstdlib>

using rune::client::transport::TransportConfig;
using rune::client::transport::validate;
using rune::client::transport::applyEnvOverrides;

TEST(TransportConfig, DefaultsAreValidAndMtuIsEightKiB) {
    TransportConfig c;
    EXPECT_EQ(c.mtuLength, 8192u);
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
    c.termBufferLength = 64u * 1024;
    EXPECT_FALSE(validate(c).has_value());
}

TEST(TransportConfig, RejectsSocketBufferSmallerThanMtu) {
    TransportConfig c; c.socketSndBufBytes = 4096;
    EXPECT_TRUE(validate(c).has_value());
    c.socketSndBufBytes = 0;  // 0 = OS default, allowed
    c.socketRcvBufBytes = 4096;
    EXPECT_TRUE(validate(c).has_value());
}

TEST(TransportConfig, EnvOverridesMtuSocketBuffersAndIdle) {
    ::setenv("RUNE_CLIENT_AERON_MTU", "16384", 1);
    ::setenv("RUNE_AERON_SOCKET_BUFFER_BYTES", "4194304", 1);
    ::setenv("RUNE_CLIENT_AERON_DRIVER_IDLE", "backoff", 1);
    TransportConfig c;
    applyEnvOverrides(c);
    ::unsetenv("RUNE_CLIENT_AERON_MTU");
    ::unsetenv("RUNE_AERON_SOCKET_BUFFER_BYTES");
    ::unsetenv("RUNE_CLIENT_AERON_DRIVER_IDLE");
    EXPECT_EQ(c.mtuLength, 16384u);
    EXPECT_EQ(c.socketSndBufBytes, 4194304u);
    EXPECT_EQ(c.socketRcvBufBytes, 4194304u);
    EXPECT_EQ(c.driverIdleStrategy, "backoff");
}

TEST(TransportConfig, MalformedEnvIsIgnored) {
    ::setenv("RUNE_CLIENT_AERON_MTU", "8k", 1);
    TransportConfig c;
    applyEnvOverrides(c);
    ::unsetenv("RUNE_CLIENT_AERON_MTU");
    EXPECT_EQ(c.mtuLength, 8192u);
}
