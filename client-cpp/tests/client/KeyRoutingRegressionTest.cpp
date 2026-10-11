// tests/client/KeyRoutingRegressionTest.cpp
//
// Phase 2b Part C: rune::client::Key::partition must be EXACTLY
// rune::protocol::partitionForKey — the function the broker's
// TopicRegistry::partitionForKey calls for every partitionKnown=0 request
// (produce / subscribe / GetViewRow).  A client-only special case (the first
// Task 5 cut returned 0 for an empty key in Key::partition while the broker
// still hashed fnv1a64("") % 8 = 5) splits a Key{} producer from a Key{}
// subscriber.  The empty-key rule lives in PartitionHash.h.

#include <gtest/gtest.h>
#include <rune/client/Key.h>
#include <rune/protocol/PartitionHash.h>

#include <array>
#include <cstdint>
#include <span>

namespace rc = rune::client;

TEST(KeyRoutingRegression, EmptyKeyIsPartitionZeroForAnyPartitionCount) {
    const rc::Key emptyDefault{};
    const rc::Key emptySpan(std::span<const uint8_t>{});
    for (int32_t n = 1; n <= 64; ++n) {
        EXPECT_EQ(emptyDefault.partition(n), 0) << "partitionCount=" << n;
        EXPECT_EQ(emptySpan.partition(n), 0) << "partitionCount=" << n;
    }
}

TEST(KeyRoutingRegression, ClientKeyPartitionMatchesProtocolForAllKeys) {
    std::array<uint8_t, 16> buf{};
    for (std::size_t i = 0; i < buf.size(); ++i) buf[i] = static_cast<uint8_t>(0x30 + i * 7);
    for (std::size_t len : {std::size_t{0}, std::size_t{1}, std::size_t{8}, std::size_t{16}}) {
        const std::span<const uint8_t> b(buf.data(), len);
        const rc::Key k(b);
        ASSERT_EQ(k.size(), len);
        for (int32_t n : {1, 8}) {
            EXPECT_EQ(k.partition(n), rune::protocol::partitionForKey(b, n))
                << "len=" << len << " n=" << n;
        }
    }
    // The int64 constructor (8 LE bytes) too.
    const rc::Key k64(int64_t{100});
    EXPECT_EQ(k64.partition(8), rune::protocol::partitionForKey(k64.bytes(), 8));
}
