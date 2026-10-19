// tests/producer/InFlightRingTest.cpp — batch correlation ids + in-flight ring (pure).
#include <rune/client/producer/InFlightRing.h>

#include <gtest/gtest.h>

using namespace rune::client::producer;

TEST(BatchCorrId, RoundTripsAndIsDistinctFromClientCounterIds) {
    const int64_t c = batchcorr::make(4095, 262143, 0xFFFFFFFFu, 0xAB);
    EXPECT_TRUE(batchcorr::isBatch(c));
    EXPECT_EQ(batchcorr::ordinal(c), 4095u);
    EXPECT_EQ(batchcorr::slot(c), 262143u);
    EXPECT_EQ(batchcorr::seq(c), batchcorr::kSeqMask) << "24-bit sequence";
    EXPECT_EQ(batchcorr::attempt(c), 0xAB);
    EXPECT_EQ(batchcorr::base(c), batchcorr::make(4095, 262143, 0xFFFFFFFFu));
    EXPECT_EQ(batchcorr::withAttempt(batchcorr::base(c), 0xAB), c);
    EXPECT_GT(c, 0);
    EXPECT_FALSE(batchcorr::isBatch(1));                   // Client::nextCorrId_ range
    EXPECT_FALSE(batchcorr::isBatch(3'000'000'000'000LL)); // SessionMachine corr bases
    EXPECT_FALSE(batchcorr::isBatch(0));
    EXPECT_FALSE(batchcorr::isBatch(-5));
}

TEST(InFlightRing, CapacityRoundsUpToPowerOfTwo) {
    EXPECT_EQ(InFlightRing(1).capacity(), 1u);
    EXPECT_EQ(InFlightRing(3).capacity(), 4u);
    EXPECT_EQ(InFlightRing(32).capacity(), 32u);
}

TEST(InFlightRing, ClaimFindReleaseFifo) {
    InFlightRing r(4);
    InFlightBatch& a = r.claim(1, 7);
    InFlightBatch& b = r.claim(1, 7);
    EXPECT_EQ(batchcorr::seq(a.corrId), 0u);
    EXPECT_EQ(batchcorr::seq(b.corrId), 1u);
    EXPECT_EQ(r.live(), 2u);
    EXPECT_EQ(r.find(a.corrId), &a);
    r.release(a);
    EXPECT_EQ(r.find(a.corrId), nullptr);
    EXPECT_EQ(r.oldestLive(), &b);
    r.release(b);
    EXPECT_TRUE(r.empty());
    EXPECT_EQ(r.oldestLive(), nullptr);
}

TEST(InFlightRing, OutOfOrderReleaseKeepsWindowFullUntilOldestAcked) {
    InFlightRing r(2);
    InFlightBatch& a = r.claim(0, 0);
    InFlightBatch& b = r.claim(0, 0);
    r.release(b);
    EXPECT_EQ(r.live(), 1u);
    EXPECT_TRUE(r.full()) << "a claim now would overwrite live batch a";
    r.release(a);
    EXPECT_FALSE(r.full());
    EXPECT_TRUE(r.empty());
}

// Regression (Review Focus 4): a duplicate ack, or an ack for an id whose slot was
// reused, must never match a live batch or double-release.
TEST(InFlightRing, DuplicateAndStaleAcksAreIgnoredRegression) {
    InFlightRing r(2);
    const int64_t first = r.claim(0, 3).corrId;
    InFlightBatch* f = r.find(first);
    ASSERT_NE(f, nullptr);
    r.release(*f);
    r.release(*f);                        // duplicate release: no effect
    EXPECT_EQ(r.live(), 0u);
    r.claim(0, 3);                        // seq 1
    const int64_t reused = r.claim(0, 3).corrId;  // seq 2 -> same slot as seq 0
    EXPECT_EQ(batchcorr::seq(reused) & 1u, batchcorr::seq(first) & 1u);
    EXPECT_EQ(r.find(first), nullptr) << "stale id must not match the reused slot";
    EXPECT_NE(r.find(reused), nullptr);
    EXPECT_EQ(r.live(), 2u);
}

// EOS follow-ups I-2: every attempt of a live batch is found (find ignores the
// attempt tag); the slot keeps the attempt-free id.
TEST(InFlightRing, FindMatchesEveryAttemptOfALiveBatchRegression) {
    InFlightRing r(4);
    InFlightBatch& b = r.claim(2, 7);
    EXPECT_EQ(batchcorr::attempt(b.corrId), 0);
    for (uint8_t a : {uint8_t{0}, uint8_t{1}, uint8_t{255}}) {
        EXPECT_EQ(r.find(batchcorr::withAttempt(b.corrId, a)), &b) << int(a);
    }
    const int64_t id = b.corrId;
    r.release(b);
    EXPECT_EQ(r.find(batchcorr::withAttempt(id, 3)), nullptr);
}
