// tests/GeneratedRoundTripTest.cpp
//
// This test includes a header generated at build time by rune_codec_gen
// from tests/generated/characters.blob.  It constructs a Characters record
// via the generated encode() and then asserts readField() (the runtime
// reader) returns the same values — proving generated codec ⇄ runtime reader
// agreement.

#include <gtest/gtest.h>
#include <rune/codec/Record.h>

// Generated at build time: tests/generated/characters.gen.h
#include "generated/characters.gen.h"

using namespace rune::codec;

TEST(GeneratedRoundTrip, EncodeDecodeMathes) {
    // Construct a Characters record using the generated struct
    rune::gen::Characters c;
    c.id       = 1001;
    c.userId   = 42;
    c.strength = 7;
    c.name     = "Alice";

    // Encode into a buffer
    std::vector<uint8_t> buf(c.encodedSize());
    size_t written = c.encode(buf.data());
    ASSERT_EQ(written, buf.size());

    // Use the runtime reader to read each field back
    const Descriptor& d = rune::gen::Characters::descriptor();

    auto fId       = readField(d, buf, d.find("id")->id);
    auto fUserId   = readField(d, buf, d.find("userId")->id);
    auto fStrength = readField(d, buf, d.find("strength")->id);
    auto fName     = readField(d, buf, d.find("name")->id);

    EXPECT_TRUE(fId.present);
    EXPECT_EQ(fId.i, 1001);

    EXPECT_TRUE(fUserId.present);
    EXPECT_EQ(fUserId.i, 42);

    EXPECT_TRUE(fStrength.present);
    EXPECT_EQ(fStrength.i, 7);

    EXPECT_TRUE(fName.present);
    EXPECT_EQ(fName.bytes, "Alice");
}

TEST(GeneratedRoundTrip, FingerprintMatchesRuntimeDescriptor) {
    // The generated kFingerprint must match fingerprint(descriptor())
    const Descriptor& d = rune::gen::Characters::descriptor();
    EXPECT_EQ(rune::gen::Characters::kFingerprint, fingerprint(d));
}
