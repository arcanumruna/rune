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
    auto forA = pl::deriveLinkKeys(k, 1, 2, true, a, b, 7, std::nullopt);
    auto forB = pl::deriveLinkKeys(k, 2, 1, false, a, b, 7, std::nullopt);
    ASSERT_TRUE(forA && forB);
    EXPECT_EQ(forA->epoch, 7u);
    EXPECT_EQ(forA->sendMaster, forB->recvMaster) << "A's send key is B's receive key";
    EXPECT_EQ(forA->recvMaster, forB->sendMaster);
    EXPECT_NE(forA->sendMaster, forA->recvMaster) << "one key per direction";

    auto swapped = pl::deriveLinkKeys(k, 1, 2, true, b, a, 7, std::nullopt);                 // the nonces are ordered
    EXPECT_NE(swapped->sendMaster, forA->sendMaster);
    auto otherNonce = pl::deriveLinkKeys(k, 1, 2, true, a, nonce(3), 7, std::nullopt);       // a replayed handshake gets other keys
    EXPECT_NE(otherNonce->sendMaster, forA->sendMaster);
    EXPECT_NE(otherNonce->recvMaster, forA->recvMaster);
    auto otherKey = pl::deriveLinkKeys(key('x'), 1, 2, true, a, b, 7, std::nullopt);
    EXPECT_NE(otherKey->sendMaster, forA->sendMaster);
    auto otherPair = pl::deriveLinkKeys(k, 1, 3, true, a, b, 7, std::nullopt);               // the labels bind the node ids
    EXPECT_NE(otherPair->sendMaster, forA->sendMaster);
    EXPECT_EQ(pl::deriveLinkKeys(k, 1, 2, true, a, b, 7, std::nullopt), forA) << "deterministic";
}

// Issue #242: the link keys mix in an ephemeral X25519 secret, so a recording plus a later-stolen cluster key cannot
// re-derive them; the v1 derivation (a legacy initiator) is byte-identical to the pre-#242 one.
TEST(PeerLinkKeysRegression, LinkKeysDependOnAnEphemeralDhSecretRegression) {
    const auto k = key('k');
    const auto a = nonce(1), b = nonce(2);
    const auto s1 = nonce(0x31), s2 = nonce(0x32);
    auto v1 = pl::deriveLinkKeys(k, 1, 2, true, a, b, 7, std::nullopt);
    auto d1 = pl::deriveLinkKeys(k, 1, 2, true, a, b, 7, std::span<const uint8_t, 32>(s1));
    auto d2 = pl::deriveLinkKeys(k, 1, 2, true, a, b, 7, std::span<const uint8_t, 32>(s2));
    auto d1Peer = pl::deriveLinkKeys(k, 2, 1, false, a, b, 7, std::span<const uint8_t, 32>(s1));
    ASSERT_TRUE(v1 && d1 && d2 && d1Peer);
    EXPECT_NE(d1->sendMaster, d2->sendMaster) << "another shared secret, other keys";
    EXPECT_NE(d1->recvMaster, d2->recvMaster);
    EXPECT_NE(d1->sendMaster, v1->sendMaster) << "v2 keys are never the v1 keys";
    EXPECT_NE(d1->recvMaster, v1->recvMaster);
    EXPECT_EQ(d1->sendMaster, d1Peer->recvMaster) << "still the mirror pair";
    EXPECT_EQ(d1->recvMaster, d1Peer->sendMaster);
    EXPECT_EQ(pl::deriveLinkKeys(k, 1, 2, true, a, b, 7, std::span<const uint8_t, 32>(s1)), d1) << "deterministic";
    // Pinned from the pre-#242 code (key 'k' x48, nonces 0x01.. / 0x02.., 1 -> 2, epoch 7): v1 never changes.
    const std::array<uint8_t, 32> pinnedSend = {0x91, 0x49, 0xf1, 0xcd, 0x90, 0x11, 0x18, 0x2c, 0x76, 0x25, 0x7a, 0xf9, 0xf3, 0xa1, 0x4d, 0xd0,
                                                0x33, 0xfd, 0xbe, 0x7f, 0x52, 0x2e, 0xca, 0x88, 0x6d, 0x0c, 0xe7, 0xe3, 0x15, 0x84, 0x27, 0xf5};
    const std::array<uint8_t, 32> pinnedRecv = {0x48, 0x73, 0xa9, 0xe1, 0x47, 0x00, 0x4f, 0xb7, 0x21, 0x71, 0x04, 0x65, 0xd9, 0xae, 0x03, 0x29,
                                                0x0d, 0x2d, 0x50, 0x4f, 0xb1, 0x20, 0xaa, 0xbd, 0x75, 0xda, 0x80, 0xcb, 0x41, 0xe8, 0x2c, 0xd4};
    EXPECT_EQ(v1->sendMaster, pinnedSend);
    EXPECT_EQ(v1->recvMaster, pinnedRecv);
}
