// O2ORingBufferTest.cpp — direct edge-case tests for the internal SPSC ring
// (rune::node::detail::O2ORingBuffer): zero-copy tryClaim/commit/abort, the write()
// convenience, back-pressure on full, wrap-around, and controlledRead ABORT.
#include <rune/node/detail/O2ORingBuffer.h>

#include <concurrent/AtomicBuffer.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

using rune::node::detail::O2ORingBuffer;
using rune::node::detail::ControlledAction;

namespace {

// Backing storage = pow2 payload + trailer.
struct Backing {
    std::vector<std::uint8_t> bytes;
    explicit Backing(int payloadPow2)
        : bytes(static_cast<std::size_t>(payloadPow2) +
                static_cast<std::size_t>(O2ORingBuffer::trailerLength()), 0) {}
};

constexpr int32_t kType = 7;

// Claim + write `msg` in place + commit.  Returns false on back-pressure.
bool claimWrite(O2ORingBuffer& ring, const std::string& msg) {
    const auto idx = ring.tryClaim(kType, msg.size());
    if (idx < 0) return false;
    std::memcpy(ring.base() + idx, msg.data(), msg.size());
    ring.commit(idx);
    return true;
}

TEST(O2ORingBuffer, ClaimCommitReadRoundTrip) {
    Backing b(1024);
    O2ORingBuffer ring(b.bytes.data(), b.bytes.size());

    ASSERT_TRUE(claimWrite(ring, "hello"));
    ASSERT_TRUE(claimWrite(ring, "world"));

    std::vector<std::string> got;
    const int n = ring.read([&](int32_t type, aeron::concurrent::AtomicBuffer& buf,
                                aeron::util::index_t off, aeron::util::index_t len) {
        EXPECT_EQ(type, kType);
        got.emplace_back(reinterpret_cast<const char*>(buf.buffer()) + off,
                         static_cast<std::size_t>(len));
    }, 16);
    EXPECT_EQ(n, 2);
    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[0], "hello");
    EXPECT_EQ(got[1], "world");
}

TEST(O2ORingBuffer, WriteConvenienceMatchesClaim) {
    Backing b(1024);
    O2ORingBuffer ring(b.bytes.data(), b.bytes.size());
    const char* payload = "abcd";
    aeron::concurrent::AtomicBuffer src(const_cast<uint8_t*>(
        reinterpret_cast<const uint8_t*>(payload)), 4);
    ASSERT_TRUE(ring.write(kType, src, 0, 4));

    std::string got;
    ring.read([&](int32_t, aeron::concurrent::AtomicBuffer& buf,
                  aeron::util::index_t off, aeron::util::index_t len) {
        got.assign(reinterpret_cast<const char*>(buf.buffer()) + off,
                   static_cast<std::size_t>(len));
    }, 16);
    EXPECT_EQ(got, "abcd");
}

TEST(O2ORingBuffer, AbortBecomesPaddingAndIsSkipped) {
    Backing b(1024);
    O2ORingBuffer ring(b.bytes.data(), b.bytes.size());

    ASSERT_TRUE(claimWrite(ring, "keep1"));
    // Claim then ABORT — must not be delivered, but the slot is reclaimed as padding.
    const auto idx = ring.tryClaim(kType, 5);
    ASSERT_GE(idx, 0);
    ring.abort(idx);
    ASSERT_TRUE(claimWrite(ring, "keep2"));

    std::vector<std::string> got;
    ring.read([&](int32_t, aeron::concurrent::AtomicBuffer& buf,
                  aeron::util::index_t off, aeron::util::index_t len) {
        got.emplace_back(reinterpret_cast<const char*>(buf.buffer()) + off,
                         static_cast<std::size_t>(len));
    }, 16);
    ASSERT_EQ(got.size(), 2u) << "aborted message must not be delivered";
    EXPECT_EQ(got[0], "keep1");
    EXPECT_EQ(got[1], "keep2");
}

TEST(O2ORingBuffer, BackpressureWhenFullThenDrains) {
    Backing b(256);  // tiny ring
    O2ORingBuffer ring(b.bytes.data(), b.bytes.size());

    // Fill until tryClaim reports insufficient capacity (< 0).
    const std::string msg(32, 'x');
    int written = 0;
    while (claimWrite(ring, msg)) ++written;
    EXPECT_GT(written, 0);
    EXPECT_LT(ring.tryClaim(kType, msg.size()), 0) << "ring must report back-pressure when full";

    // Drain everything, then we can write again.
    int drained = ring.read([](int32_t, aeron::concurrent::AtomicBuffer&,
                               aeron::util::index_t, aeron::util::index_t) {}, 1000);
    EXPECT_EQ(drained, written);
    EXPECT_TRUE(claimWrite(ring, msg)) << "space available after drain";
}

TEST(O2ORingBuffer, WrapAroundPreservesOrder) {
    Backing b(256);
    O2ORingBuffer ring(b.bytes.data(), b.bytes.size());

    // Write+drain many messages so the tail/head wrap the ring multiple times.
    int64_t expected = 0, seen = 0;
    for (int round = 0; round < 200; ++round) {
        const std::string m = "m" + std::to_string(round);
        while (!claimWrite(ring, m)) {
            // Fully drain (loop past wrap) to free space, then retry the claim.
            for (;;) {
                const int k = ring.read([&](int32_t, aeron::concurrent::AtomicBuffer& buf,
                                            aeron::util::index_t off, aeron::util::index_t len) {
                    std::string got(reinterpret_cast<const char*>(buf.buffer()) + off,
                                    static_cast<std::size_t>(len));
                    EXPECT_EQ(got, "m" + std::to_string(seen));
                    ++seen;
                }, 1000);
                if (k == 0) break;
            }
        }
        ++expected;
    }
    // Drain the rest.  read() returns one CONTIGUOUS run (agrona semantics — the
    // same per-cycle drain the worker uses), so loop until it yields nothing.
    for (;;) {
        const int k = ring.read([&](int32_t, aeron::concurrent::AtomicBuffer& buf,
                                    aeron::util::index_t off, aeron::util::index_t len) {
            std::string got(reinterpret_cast<const char*>(buf.buffer()) + off,
                            static_cast<std::size_t>(len));
            EXPECT_EQ(got, "m" + std::to_string(seen));
            ++seen;
        }, 1000);
        if (k == 0) break;
    }
    EXPECT_EQ(seen, expected);
}

TEST(O2ORingBuffer, ControlledReadAbortLeavesMessage) {
    Backing b(1024);
    O2ORingBuffer ring(b.bytes.data(), b.bytes.size());
    ASSERT_TRUE(claimWrite(ring, "a"));
    ASSERT_TRUE(claimWrite(ring, "b"));

    // First controlledRead: ABORT immediately → consumes nothing.
    int n1 = ring.controlledRead([&](int32_t, aeron::concurrent::AtomicBuffer&,
                                     aeron::util::index_t, aeron::util::index_t) {
        return ControlledAction::ABORT;
    }, 16);
    EXPECT_EQ(n1, 0) << "ABORT must leave the message unconsumed";

    // Second: CONTINUE both → consumes both, in order.
    std::vector<std::string> got;
    int n2 = ring.controlledRead([&](int32_t, aeron::concurrent::AtomicBuffer& buf,
                                     aeron::util::index_t off, aeron::util::index_t len) {
        got.emplace_back(reinterpret_cast<const char*>(buf.buffer()) + off,
                         static_cast<std::size_t>(len));
        return ControlledAction::CONTINUE;
    }, 16);
    EXPECT_EQ(n2, 2);
    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[0], "a");
    EXPECT_EQ(got[1], "b");
}

TEST(O2ORingBuffer, TooLargeMessageThrows) {
    Backing b(256);
    O2ORingBuffer ring(b.bytes.data(), b.bytes.size());
    // maxMsgLength = capacity / 8; a message beyond it must be rejected (throw).
    EXPECT_THROW((void)ring.tryClaim(kType, static_cast<std::size_t>(ring.maxMsgLength()) + 1),
                 aeron::util::IllegalArgumentException);
}

} // namespace
