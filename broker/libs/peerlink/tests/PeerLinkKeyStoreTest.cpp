// PeerLinkKeyStoreTest.cpp — authentication plan Task 15: an owner's view of its peer links.
#include <rune/peerlink/PeerLinkKeyStore.h>

#include <gtest/gtest.h>

namespace pl = rune::peerlink;

namespace {
pl::LinkKeys keysAt(uint32_t epoch, uint8_t fill) { pl::LinkKeys k; k.epoch = epoch; k.sendMaster.fill(fill); k.recvMaster.fill(static_cast<uint8_t>(fill + 1)); return k; }
constexpr int64_t kSec = 1'000'000'000;
} // namespace

TEST(PeerLinkKeyStoreRegression, TheOldEpochStaysFindableForTwoSecondsThenIsDropped) {
    pl::PeerLinkKeyStore s;
    EXPECT_EQ(s.currentEpoch(2), 0u);
    ASSERT_TRUE(s.install(2, keysAt(1, 10), 0));
    EXPECT_EQ(s.currentEpoch(2), 1u);
    ASSERT_TRUE(s.install(2, keysAt(2, 20), 5 * kSec));
    EXPECT_EQ(s.currentEpoch(2), 2u);
    ASSERT_NE(s.find(2, 2, 5 * kSec), nullptr);
    ASSERT_NE(s.find(2, 1, 5 * kSec + 1'900'000'000), nullptr) << "in flight under the old epoch";
    EXPECT_EQ(s.find(2, 1, 5 * kSec + 2'100'000'000), nullptr) << "dropped after 2 s";
    EXPECT_EQ(s.find(2, 3, 5 * kSec), nullptr);
    EXPECT_EQ(s.find(9, 2, 5 * kSec), nullptr) << "another peer";
}

TEST(PeerLinkKeyStoreRegression, TheSameEpochNumberIsReplacedNotKept) {
    pl::PeerLinkKeyStore s;
    ASSERT_TRUE(s.install(2, keysAt(1, 10), 0));
    ASSERT_TRUE(s.install(2, keysAt(1, 99), kSec));      // a restarted responder reuses epoch 1 with other keys
    EXPECT_EQ(s.find(2, 1, kSec)->sendMaster[0], 99);
    EXPECT_EQ(s.find(2, 1, 2 * kSec)->sendMaster[0], 99) << "no stale copy of the old keys";
}

TEST(PeerLinkKeyStoreRegression, PeersAreIndependentAndTheTableIsBounded) {
    pl::PeerLinkKeyStore s;
    for (int32_t p = 0; p < static_cast<int32_t>(pl::PeerLinkKeyStore::kMaxPeers); ++p) ASSERT_TRUE(s.install(p, keysAt(1, 1), 0));
    EXPECT_FALSE(s.install(1000, keysAt(1, 1), 0)) << "full: refused, never reallocated";
    EXPECT_TRUE(s.install(3, keysAt(2, 1), 0)) << "an existing peer still updates";
}
