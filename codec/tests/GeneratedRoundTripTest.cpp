// tests/GeneratedRoundTripTest.cpp
//
// This test includes a header generated at build time by rune_codec_gen
// from tests/generated/characters.blob.  It constructs a Characters record
// via the generated encode() and then asserts readField() (the runtime
// reader) returns the same values — proving generated codec ⇄ runtime reader
// agreement.

#include <gtest/gtest.h>
#include <rune/codec/Record.h>
#include <rune/codec/Generator.h>

// Generated at build time: tests/generated/characters.gen.h
#include "generated/characters.gen.h"

using namespace rune::codec;

TEST(GeneratedRoundTrip, EncodeDecodeMatches) {
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

// I-1 / M-2: the generated encode() must stamp the descriptor's real version,
// and the generated struct must expose kVersion. A v2 schema with a
// sinceVersion=2 field must round-trip so that readField() sees the v2 field
// as PRESENT — proving the version stamp is honored end-to-end (a v1 stamp
// would gate the field out as absent: silent data loss).
TEST(GeneratedRoundTrip, VersionStampHonoredForV2Field) {
    // A v2 descriptor: v1 fixed field "id", plus a sinceVersion=2 fixed field
    // "level" and a sinceVersion=2 var field "title".
    auto d = DescriptorBuilder("heroes", /*version=*/2)
                 .add("id",    PrimType::Int64,  /*sinceVersion=*/1)
                 .add("level", PrimType::Int32,  /*sinceVersion=*/2)
                 .add("title", PrimType::String, /*sinceVersion=*/2)
                 .build();

    // 1. The generator must emit kVersion=2 and stamp it (not a hard-coded 1).
    const std::string hdr = generateHeader(d);
    EXPECT_NE(hdr.find("kVersion     = 2u"), std::string::npos);
    EXPECT_NE(hdr.find("wr16(kVersion);"), std::string::npos);
    EXPECT_EQ(hdr.find("wr16(uint16_t(1)); // version"), std::string::npos);

    // 2. Hand-frame a record with the correct v2 header stamp and confirm the
    //    reader honors it: the sinceVersion=2 fields read as PRESENT.
    //    Layout: [blockLength:u16][version:u16][id:int64][level:int32][var: title]
    const uint16_t blockLength = static_cast<uint16_t>(d.blockLength); // 8 + 4 = 12
    ASSERT_EQ(blockLength, 12u);

    std::vector<uint8_t> rec;
    auto w16 = [&](uint16_t v){ rec.push_back(uint8_t(v)); rec.push_back(uint8_t(v>>8)); };
    auto w32 = [&](uint32_t v){ for(int i=0;i<4;i++) rec.push_back(uint8_t(v>>(8*i))); };
    auto w64 = [&](uint64_t v){ for(int i=0;i<8;i++) rec.push_back(uint8_t(v>>(8*i))); };

    w16(blockLength);
    w16(2);                       // version = 2 (the real stamp)
    w64(uint64_t(777));           // id
    w32(uint32_t(9));             // level (sinceVersion=2)
    const char* title = "Knight";
    w32(uint32_t(6));             // var length prefix
    for (const char* p = title; *p; ++p) rec.push_back(uint8_t(*p));

    auto fId    = readField(d, rec, d.find("id")->id);
    auto fLevel = readField(d, rec, d.find("level")->id);
    auto fTitle = readField(d, rec, d.find("title")->id);

    EXPECT_TRUE(fId.present);
    EXPECT_EQ(fId.i, 777);
    EXPECT_TRUE(fLevel.present);          // honored because header says v2
    EXPECT_EQ(fLevel.i, 9);
    EXPECT_TRUE(fTitle.present);
    EXPECT_EQ(fTitle.bytes, "Knight");

    // Control: the same bytes but stamped v1 (the OLD buggy behavior) would
    // gate the v2 fields out as absent — proving the stamp is what matters.
    std::vector<uint8_t> recV1 = rec;
    recV1[2] = 1; recV1[3] = 0;           // rewrite version stamp to 1
    EXPECT_FALSE(readField(d, recV1, d.find("level")->id).present);
    EXPECT_FALSE(readField(d, recV1, d.find("title")->id).present);
}
