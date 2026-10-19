// PeerLinkKeysTest.cpp — authentication plan Task 15: directional, nonce-bound link keys (D33).
#include <rune/peerlink/PeerLinkKeys.h>

#include <gtest/gtest.h>

#include <string>

namespace pl = rune::peerlink;

namespace {
rune::crypto::ClusterKeyMaterial key(char c) {
    std::string k(48, c);
    return *rune::crypto::deriveClusterKey({reinterpret_cast<const uint8_t*>(k.data()), k.size()});
}
std::array<uint8_t, 32> nonce(uint8_t v) { std::array<uint8_t, 32> n{}; n.fill(v); return n; }
} // namespace

TEST(PeerLinkKeysRegression, DerivationIsDirectionalAndNonceBound) {
    const auto k = key('k');
    const auto a = nonce(1), b = nonce(2);
    auto forA = pl::deriveLinkKeys(k, 1, 2, true, a, b, 7);
    auto forB = pl::deriveLinkKeys(k, 2, 1, false, a, b, 7);
    ASSERT_TRUE(forA && forB);
    EXPECT_EQ(forA->epoch, 7u);
    EXPECT_EQ(forA->sendMaster, forB->recvMaster) << "A's send key is B's receive key";
    EXPECT_EQ(forA->recvMaster, forB->sendMaster);
    EXPECT_NE(forA->sendMaster, forA->recvMaster) << "one key per direction";

    auto swapped = pl::deriveLinkKeys(k, 1, 2, true, b, a, 7);                 // the nonces are ordered
    EXPECT_NE(swapped->sendMaster, forA->sendMaster);
    auto otherNonce = pl::deriveLinkKeys(k, 1, 2, true, a, nonce(3), 7);       // a replayed handshake gets other keys
    EXPECT_NE(otherNonce->sendMaster, forA->sendMaster);
    EXPECT_NE(otherNonce->recvMaster, forA->recvMaster);
    auto otherKey = pl::deriveLinkKeys(key('x'), 1, 2, true, a, b, 7);
    EXPECT_NE(otherKey->sendMaster, forA->sendMaster);
    auto otherPair = pl::deriveLinkKeys(k, 1, 3, true, a, b, 7);               // the labels bind the node ids
    EXPECT_NE(otherPair->sendMaster, forA->sendMaster);
    EXPECT_EQ(pl::deriveLinkKeys(k, 1, 2, true, a, b, 7), forA) << "deterministic";
}
