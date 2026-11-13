// BootstrapProofTest.cpp - the bootstrap proof (authentication plan D4, issue #234) in isolation: the MAC is keyed by a
// subkey of the cluster master and covers a one-use challenge, so a captured proof verifies under no other challenge
// and a MAC made with the raw master (or the v1 input) never verifies.
#include <rune/gateway/BootstrapProof.h>
#include <rune/crypto/Kdf.h>
#include <rune/peerlink/ClusterKeys.h>
#include <rune/protocol/client/SecurityMessages.h>

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <string>
#include <vector>

using namespace rune::gateway;
namespace pc = rune::protocol::client;

namespace {

rune::peerlink::ClusterKey keyOf(const std::string& contents) {
    auto k = rune::crypto::deriveClusterKey(
        std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(contents.data()), contents.size()));
    return *k;
}

struct Fixture {
    std::array<uint8_t, 16> salt{};
    std::array<uint8_t, 64> stored{}, server{};
    pc::SecurityBootstrap msg;
    Fixture() {
        salt.fill(7); stored.fill(1); server.fill(2);
        msg.sessionId = 1; msg.correlationId = 2; msg.adminName = "admin";
        msg.verifier = pc::VerifierWire{std::string_view(reinterpret_cast<const char*>(salt.data()), salt.size()), 4096,
                                        std::string_view(reinterpret_cast<const char*>(stored.data()), 64),
                                        std::string_view(reinterpret_cast<const char*>(server.data()), 64)};
        msg.fakeSaltSeed.fill(5); msg.nonce.fill(6);
    }
    /// A MAC over `input` under `key` (any 64-byte key), written into msg.mac.
    void signWith(std::span<const uint8_t> key, std::span<const uint8_t> input) {
        rune::crypto::DigestBytes mac;
        ASSERT_TRUE(rune::crypto::hmac(rune::crypto::Digest::Sha512, key, input, mac));
        std::memcpy(msg.mac.data(), mac.bytes.data(), 64);
    }
    void signV2(const rune::peerlink::ClusterKey& k, std::span<const uint8_t, 32> challenge) {
        std::array<uint8_t, 64> sub{};
        ASSERT_TRUE(rune::crypto::deriveBootstrapMacKey(k.master, sub));
        std::vector<uint8_t> in;
        pc::bootstrapMacInput(msg, challenge, in);
        signWith(sub, in);
    }
};

}  // namespace

TEST(BootstrapProofRegression, AProofVerifiesOnlyWithItsChallengeRegression) {
    rune::peerlink::ClusterKeys keys;
    keys.current = keyOf(std::string(40, 'a'));
    keys.next = keyOf(std::string(40, 'b'));
    std::array<uint8_t, 32> ch{}, other{};
    ch.fill(0x11); other.fill(0x11); other[31] = 0x12;
    std::vector<uint8_t> scratch;

    Fixture f;
    f.signV2(keys.current, ch);
    EXPECT_EQ(verifyBootstrapProof(f.msg, ch, &keys, scratch), BootstrapProof::Ok) << "right challenge";
    EXPECT_EQ(verifyBootstrapProof(f.msg, other, &keys, scratch), BootstrapProof::Mismatch) << "another challenge";
    EXPECT_EQ(verifyBootstrapProof(f.msg, ch, nullptr, scratch), BootstrapProof::NoKey);

    Fixture n;   // the NEXT key verifies (rotation window)
    n.signV2(*keys.next, ch);
    EXPECT_EQ(verifyBootstrapProof(n.msg, ch, &keys, scratch), BootstrapProof::Ok) << "the next key";

    Fixture raw;   // the v2 input keyed with the RAW master (no subkey)
    {
        std::vector<uint8_t> in;
        pc::bootstrapMacInput(raw.msg, ch, in);
        raw.signWith(keys.current.master, in);
    }
    EXPECT_EQ(verifyBootstrapProof(raw.msg, ch, &keys, scratch), BootstrapProof::Mismatch) << "raw master over the v2 input";

    Fixture v1;   // a v1-style MAC: raw master over "rune-bootstrap-v1" without a challenge
    {
        std::vector<uint8_t> in;
        const std::string tag = "rune-bootstrap-v1";
        in.assign(tag.begin(), tag.end());
        std::vector<uint8_t> v2;
        pc::bootstrapMacInput(v1.msg, ch, v2);
        // the v1 input is the v2 input without the 32 challenge bytes after the tag
        in.insert(in.end(), v2.begin() + static_cast<std::ptrdiff_t>(tag.size() + 32), v2.end());
        v1.signWith(keys.current.master, in);
    }
    EXPECT_EQ(verifyBootstrapProof(v1.msg, ch, &keys, scratch), BootstrapProof::Mismatch) << "v1 proof";

    Fixture v1sub;   // even the subkey over the v1 input is refused
    {
        std::array<uint8_t, 64> sub{};
        ASSERT_TRUE(rune::crypto::deriveBootstrapMacKey(keys.current.master, sub));
        std::vector<uint8_t> v2, in;
        const std::string tag = "rune-bootstrap-v1";
        pc::bootstrapMacInput(v1sub.msg, ch, v2);
        in.assign(tag.begin(), tag.end());
        in.insert(in.end(), v2.begin() + static_cast<std::ptrdiff_t>(tag.size() + 32), v2.end());
        v1sub.signWith(sub, in);
    }
    EXPECT_EQ(verifyBootstrapProof(v1sub.msg, ch, &keys, scratch), BootstrapProof::Mismatch);
}

TEST(BootstrapProofRegression, TheBootstrapSubkeyIsNotTheMasterAndIsDeterministicRegression) {
    const auto k = keyOf(std::string(40, 'a'));
    std::array<uint8_t, 64> a{}, b{};
    ASSERT_TRUE(rune::crypto::deriveBootstrapMacKey(k.master, a));
    ASSERT_TRUE(rune::crypto::deriveBootstrapMacKey(k.master, b));
    EXPECT_EQ(a, b);
    EXPECT_NE(a, k.master);
    const auto k2 = keyOf(std::string(40, 'b'));
    ASSERT_TRUE(rune::crypto::deriveBootstrapMacKey(k2.master, b));
    EXPECT_NE(a, b);
    std::array<uint8_t, 64> z{};
    z.fill(0x55);
    EXPECT_FALSE(rune::crypto::deriveBootstrapMacKey({}, z));
    for (const uint8_t c : z) EXPECT_EQ(c, 0) << "a failed derivation leaves no partial key";
}
