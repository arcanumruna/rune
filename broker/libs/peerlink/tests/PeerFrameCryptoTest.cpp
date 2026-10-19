// PeerFrameCryptoTest.cpp — authentication plan Task 16: AEAD on every inter-broker frame.
#include <rune/peerlink/PeerFrameCrypto.h>

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace pl = rune::peerlink;

namespace {
pl::LinkKeys linkFor(bool aToB, uint32_t epoch = 1, uint8_t base = 1) {
    pl::LinkKeys k; k.epoch = epoch;
    k.sendMaster.fill(aToB ? base : static_cast<uint8_t>(base + 1));
    k.recvMaster.fill(aToB ? static_cast<uint8_t>(base + 1) : base);
    return k;
}
const uint8_t* bytes(const std::string& s) { return reinterpret_cast<const uint8_t*>(s.data()); }
constexpr int64_t kSec = 1'000'000'000;
} // namespace

TEST(PeerFrameRegression, SealedFramesOpenOnlyAtTheirPeerAndOnlyOnce) {
    pl::PeerFrameCrypto a(1, /*sender*/ 3), b(2, /*sender*/ 3);
    ASSERT_TRUE(a.installLink(2, linkFor(true), 0)); ASSERT_TRUE(b.installLink(1, linkFor(false), 0));
    const std::string msg = "FetchResponse bytes";
    std::vector<uint8_t> wire(msg.size() + pl::kPeerFrameOverhead), plain(msg.size());
    const auto n = a.seal(2, bytes(msg), msg.size(), wire.data(), wire.size());
    ASSERT_EQ(n, msg.size() + 32);
    std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
    ASSERT_EQ(b.open(wire.data(), n, plain.data(), plain.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::Ok);
    EXPECT_EQ(from, 1); EXPECT_EQ(epoch, 1u); EXPECT_EQ(std::string(plain.begin(), plain.begin() + outLen), msg);
    EXPECT_EQ(b.open(wire.data(), n, plain.data(), plain.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::Replay);
    wire[20] ^= 1;
    EXPECT_EQ(b.open(wire.data(), n, plain.data(), plain.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::BadTag);
}

TEST(PeerFrameRegression, FramesFromAnUnauthenticatedPeerAreDroppedAndCountedRegression) {
    pl::PeerFrameCrypto rogue(9, 0), b(2, 0);
    ASSERT_TRUE(rogue.installLink(2, linkFor(true), 0));           // keys b never agreed to
    std::vector<uint8_t> wire(64), out(64);
    const auto n = rogue.seal(2, bytes("hello"), 5, wire.data(), wire.size());
    std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
    EXPECT_EQ(b.open(wire.data(), n, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::UnknownPeer);
    EXPECT_EQ(b.drops(), 1u);
    EXPECT_EQ(b.seal(5, bytes("x"), 1, wire.data(), wire.size()), 0u) << "no link -> never sent";
    EXPECT_EQ(b.noLinkDrops(), 1u);
}

TEST(PeerFrameRegression, AWrongKeyPeerWithALinkEntryIsBadTagNotDispatchedRegression) {
    pl::PeerFrameCrypto a(1, 0), b(2, 0);
    ASSERT_TRUE(a.installLink(2, linkFor(true, 1, 1), 0));
    ASSERT_TRUE(b.installLink(1, linkFor(false, 1, 50), 0));      // b holds a different master for node 1
    std::vector<uint8_t> wire(64), out(64);
    const auto n = a.seal(2, bytes("hello"), 5, wire.data(), wire.size());
    std::size_t outLen = 7; int32_t from = -1; uint32_t epoch = 0;
    EXPECT_EQ(b.open(wire.data(), n, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::BadTag);
    EXPECT_EQ(outLen, 0u) << "nothing to dispatch";
}

TEST(PeerFrameRegression, PlaintextFramesAreNotPeerFrames) {
    pl::PeerFrameCrypto b(2, 0);
    const uint8_t sbe[40] = {0x10, 0x00};             // an SBE header, magic byte != 0xB7
    std::vector<uint8_t> out(64); std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
    EXPECT_EQ(b.open(sbe, sizeof sbe, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::NotPeerFrame);
    const uint8_t shortSbe[8] = {0x10, 0x00};
    EXPECT_EQ(b.open(shortSbe, sizeof shortSbe, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::NotPeerFrame);
}

TEST(PeerFrameRegression, SealInPlaceEqualsSeal) {
    pl::PeerFrameCrypto a(1, 3), a2(1, 3), b(2, 3);
    ASSERT_TRUE(a.installLink(2, linkFor(true), 0)); ASSERT_TRUE(a2.installLink(2, linkFor(true), 0));
    ASSERT_TRUE(b.installLink(1, linkFor(false), 0));
    const std::string msg = "the same plaintext sealed two ways";
    std::vector<uint8_t> w1(msg.size() + 32), w2(msg.size() + 32), out(msg.size());
    const auto n1 = a.seal(2, bytes(msg), msg.size(), w1.data(), w1.size());
    std::memcpy(w2.data() + pl::kPeerHeaderBytes, msg.data(), msg.size());
    const auto n2 = a2.sealInPlace(2, w2.data(), msg.size(), w2.size());
    ASSERT_EQ(n1, msg.size() + 32); ASSERT_EQ(n2, msg.size() + 32);
    EXPECT_EQ(w1, w2) << "same key, same seq 1: byte-identical frames";
    std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
    ASSERT_EQ(b.open(w2.data(), n2, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::Ok);
    EXPECT_EQ(std::string(out.begin(), out.begin() + outLen), msg);
}

TEST(PeerFrameRegression, ABogusSenderIsRefusedBeforeCryptoRegression) {
    pl::PeerFrameCrypto a(1, 3), b(2, 3);
    ASSERT_TRUE(a.installLink(2, linkFor(true), 0)); ASSERT_TRUE(b.installLink(1, linkFor(false), 0));
    std::vector<uint8_t> w(8 + 32), out(32);
    const auto n = a.seal(2, bytes("payload"), 7, w.data(), w.size());
    ASSERT_GT(n, 0u);
    std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
    for (int bogus = 0; bogus < 256; ++bogus) {
        if (bogus == 3) continue;
        w[1] = static_cast<uint8_t>(bogus);   // 255 different bogus sender ids
        EXPECT_EQ(b.open(w.data(), n, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::BadSender) << bogus;
    }
    EXPECT_EQ(b.dropsOf(pl::PeerFrameCrypto::OpenResult::BadSender), 255u);
    EXPECT_EQ(b.dropsOf(pl::PeerFrameCrypto::OpenResult::BadTag), 0u) << "refused before any crypto ran";
    EXPECT_EQ(b.authFrames(1), 0u);
    w[1] = 3;
    EXPECT_EQ(b.open(w.data(), n, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::Ok) << "the role's sender still opens";
    EXPECT_EQ(b.authFrames(1), 1u);
}

TEST(PeerFrameRegression, AFrameReplayedOntoAnotherPlaneFailsRegression) {
    // The admin plane's frame (sender 2) copied onto the legacy plane (sender 1) and a worker plane (sender 4).
    pl::PeerFrameCrypto admin(1, 2), legacyRx(2, 1), workerRx(2, 4), adminRx(2, 2);
    ASSERT_TRUE(admin.installLink(2, linkFor(true), 0));
    ASSERT_TRUE(legacyRx.installLink(1, linkFor(false), 0)); ASSERT_TRUE(workerRx.installLink(1, linkFor(false), 0));
    ASSERT_TRUE(adminRx.installLink(1, linkFor(false), 0));
    std::vector<uint8_t> w(8 + 32), out(32);
    const auto n = admin.seal(2, bytes("catalog"), 7, w.data(), w.size());
    std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
    EXPECT_EQ(legacyRx.open(w.data(), n, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::BadSender);
    EXPECT_EQ(workerRx.open(w.data(), n, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::BadSender);
    EXPECT_EQ(adminRx.open(w.data(), n, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::Ok);
}

TEST(PeerFrameRegression, ADeferredCommitReopensUntilCommittedRegression) {
    pl::PeerFrameCrypto a(1, 1), b(2, 1);
    ASSERT_TRUE(a.installLink(2, linkFor(true), 0)); ASSERT_TRUE(b.installLink(1, linkFor(false), 0));
    std::vector<uint8_t> w(8 + 32), out(32);
    const auto n = a.seal(2, bytes("fetch"), 5, w.data(), w.size());
    std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
    for (int i = 0; i < 3; ++i)   // dispatch ABORTed: the same frame opens again each time
        ASSERT_EQ(b.open(w.data(), n, out.data(), out.size(), outLen, from, epoch, pl::PeerFrameCrypto::kClockNow, false),
                  pl::PeerFrameCrypto::OpenResult::Ok);
    b.commitOpened(from, epoch, 1);
    EXPECT_EQ(b.open(w.data(), n, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::Replay);
}

TEST(PeerFrameRegression, AnOldEpochStaysValidForTwoSecondsThenIsRefused) {
    pl::PeerFrameCrypto a(1, 0), b(2, 0);
    ASSERT_TRUE(a.installLink(2, linkFor(true, 1, 1), 0));
    ASSERT_TRUE(b.installLink(1, linkFor(false, 1, 1), 0));
    std::vector<uint8_t> w1(40), w2(40), w3(40), out(8);
    const auto n1 = a.seal(2, bytes("old1"), 4, w1.data(), w1.size());
    const auto n2 = a.seal(2, bytes("old2"), 4, w2.data(), w2.size());
    const auto n3 = a.seal(2, bytes("old3"), 4, w3.data(), w3.size());
    const int64_t t = 10 * kSec;
    ASSERT_TRUE(b.installLink(1, linkFor(false, 2, 77), t));        // a new epoch with other keys
    std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
    EXPECT_EQ(b.currentEpoch(1), 2u);
    EXPECT_EQ(b.open(w1.data(), n1, out.data(), out.size(), outLen, from, epoch, t + 1'900'000'000),
              pl::PeerFrameCrypto::OpenResult::Ok) << "in flight under epoch 1";
    EXPECT_EQ(epoch, 1u);
    EXPECT_EQ(b.open(w2.data(), n2, out.data(), out.size(), outLen, from, epoch, t + 2'100'000'000),
              pl::PeerFrameCrypto::OpenResult::UnknownEpoch) << "dropped after 2 s";
    EXPECT_EQ(epoch, 1u) << "the unknown epoch is reported (the control thread re-handshakes)";
    EXPECT_EQ(from, 1);
    EXPECT_EQ(b.open(w3.data(), n3, out.data(), out.size(), outLen, from, epoch, t + 2'200'000'000),
              pl::PeerFrameCrypto::OpenResult::UnknownEpoch);
}

TEST(PeerFrameRegression, ANewEpochStartsAFreshSendSequenceAndWindow) {
    pl::PeerFrameCrypto a(1, 0), b(2, 0);
    ASSERT_TRUE(a.installLink(2, linkFor(true, 1, 1), 0)); ASSERT_TRUE(b.installLink(1, linkFor(false, 1, 1), 0));
    std::vector<uint8_t> w(40), out(8);
    std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
    for (int i = 0; i < 5; ++i) { const auto n = a.seal(2, bytes("abcd"), 4, w.data(), w.size()); ASSERT_EQ(b.open(w.data(), n, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::Ok); }
    ASSERT_TRUE(a.installLink(2, linkFor(true, 2, 9), kSec)); ASSERT_TRUE(b.installLink(1, linkFor(false, 2, 9), kSec));
    const auto n = a.seal(2, bytes("abcd"), 4, w.data(), w.size());
    EXPECT_EQ(b.open(w.data(), n, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::Ok) << "seq restarted at 1 under the new keys";
    EXPECT_EQ(epoch, 2u);
}

TEST(PeerFrameRegression, TheHeaderAndTheDestinationAreAuthenticated) {
    pl::PeerFrameCrypto a(1, 0), b(2, 0), c(3, 0);
    ASSERT_TRUE(a.installLink(2, linkFor(true), 0)); ASSERT_TRUE(b.installLink(1, linkFor(false), 0));
    ASSERT_TRUE(c.installLink(1, linkFor(false), 0));   // c shares the keys (a compromised broker): still toNode differs
    std::vector<uint8_t> w(40), out(8);
    const auto n = a.seal(2, bytes("abcd"), 4, w.data(), w.size());
    std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
    EXPECT_EQ(c.open(w.data(), n, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::BadTag)
        << "a frame sealed for node 2 never opens as one for node 3";
    auto forged = w; forged[1] ^= 3;                     // another sender id: another key
    EXPECT_NE(b.open(forged.data(), n, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::Ok);
    forged = w; forged[9] ^= 1;                           // seq bits (1 -> 257)
    EXPECT_EQ(b.open(forged.data(), n, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::BadTag);
    EXPECT_EQ(b.open(w.data(), n, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::Ok) << "the genuine frame still opens";
}

TEST(PeerFrameRegression, SealRefusesATooSmallBufferAndAnOversizedNodeId) {
    pl::PeerFrameCrypto a(1, 0);
    ASSERT_TRUE(a.installLink(2, linkFor(true), 0));
    std::vector<uint8_t> w(40);
    EXPECT_EQ(a.seal(2, bytes("abcd"), 4, w.data(), 35), 0u) << "cap < len + 32";
    EXPECT_FALSE(a.installLink(70000, linkFor(true), 0)) << "a node id must fit the u16 header field";
    EXPECT_FALSE(a.installLink(2, linkFor(true, /*epoch*/ 0), 0)) << "epoch 0 means no link";
}

TEST(PeerFrameRegression, AnOutOfOrderFrameInsideTheWindowOpensOnceAndAnAncientOneNever) {
    pl::PeerFrameCrypto a(1, 0), b(2, 0);
    ASSERT_TRUE(a.installLink(2, linkFor(true), 0)); ASSERT_TRUE(b.installLink(1, linkFor(false), 0));
    std::vector<std::vector<uint8_t>> f;
    for (int i = 0; i < 100; ++i) { std::vector<uint8_t> w(40); const auto n = a.seal(2, bytes("abcd"), 4, w.data(), w.size()); w.resize(n); f.push_back(w); }
    std::vector<uint8_t> out(8); std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
    auto open = [&](int i) { return b.open(f[static_cast<std::size_t>(i)].data(), f[static_cast<std::size_t>(i)].size(), out.data(), out.size(), outLen, from, epoch); };
    EXPECT_EQ(open(99), pl::PeerFrameCrypto::OpenResult::Ok);       // seq 100 first
    EXPECT_EQ(open(60), pl::PeerFrameCrypto::OpenResult::Ok);       // seq 61: 39 behind, inside the 64-wide window
    EXPECT_EQ(open(60), pl::PeerFrameCrypto::OpenResult::Replay);
    EXPECT_EQ(open(10), pl::PeerFrameCrypto::OpenResult::Replay);   // seq 11: more than 64 behind
}
