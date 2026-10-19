// IngressRingDrainTest.cpp — Task 11 fix 2 (B1): IngressRing::drain is templated
// on the handler (inlined, like OneToOneByteRing) — never a std::function per
// drain.  Proof: a MOVE-ONLY handler (not convertible to std::function) drains.
#include <rune/consensus/IngressRing.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>

using rune::consensus::IngressRing;

TEST(IngressRingRegression, DrainTakesAMoveOnlyHandlerNoStdFunction) {
    IngressRing ring(64 * 1024);
    const uint8_t a[3] = {1, 2, 3}, b[2] = {9, 8};
    ASSERT_TRUE(ring.write(a, sizeof a));
    ASSERT_TRUE(ring.write(b, sizeof b));
    auto sum = std::make_unique<int>(0);     // move-only capture
    int frames = 0;
    const int n = ring.drain(
        [s = std::move(sum), &frames](int32_t, aeron::concurrent::AtomicBuffer& buf,
                                      aeron::util::index_t off, aeron::util::index_t len) mutable {
            for (aeron::util::index_t i = 0; i < len; ++i) *s += buf.getUInt8(off + i);
            ++frames;
        },
        10);
    EXPECT_EQ(n, 2);
    EXPECT_EQ(frames, 2);
}
