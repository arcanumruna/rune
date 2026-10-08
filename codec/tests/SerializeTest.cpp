// tests/SerializeTest.cpp
#include <gtest/gtest.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Fingerprint.h>
using namespace rune::codec;

TEST(Serialize, RoundTrip) {
  auto d = DescriptorBuilder("characters")
    .add("id",PrimType::Int64).add("userId",PrimType::Int64,1,true)
    .add("type",PrimType::Enum,1,false,{0,1,2}).add("name",PrimType::String)
    .add("strength",PrimType::Int32).build();
  auto blob = serialize(d);
  auto d2 = deserialize(blob);
  EXPECT_EQ(fingerprint(d), fingerprint(d2));      // structurally identical
  EXPECT_EQ(d2.blockLength, d.blockLength);
  EXPECT_EQ(d2.find("type")->enumValues, (std::vector<int32_t>{0,1,2}));
}

TEST(Serialize, RejectsTruncated) {
  auto blob = serialize(DescriptorBuilder("t").add("a",PrimType::Int64).build());
  blob.resize(blob.size()/2);
  EXPECT_THROW(deserialize(blob), std::runtime_error);
}

TEST(Serialize, Rcd2RoundTripPreservesTagsAndUntil) {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("id", 5, rune::codec::PrimType::Int64, 1, 0xFFFF, true, {});
    b.addTagged("age",2, rune::codec::PrimType::Int32, 1, 3,      false,{});
    auto d = b.build();
    auto blob = rune::codec::serialize(d);
    EXPECT_EQ(blob[0], 0x52); EXPECT_EQ(blob[1], 0x43); EXPECT_EQ(blob[2], 0x44); EXPECT_EQ(blob[3], 0x32); // "RCD2"
    auto d2 = rune::codec::deserialize(blob);
    EXPECT_EQ(d2.find("id")->id, 5);
    EXPECT_EQ(d2.find("age")->untilVersion, 3);
}

TEST(Serialize, ReadsLegacyRcd1AsInfiniteUntilAndPositionalTag) {
    // Hand-crafted minimal RCD1 blob per the documented RCD1 layout:
    //   [magic u32 "RCD1"][nameLen u16][name][version u16][fieldCount u16]
    //   per field: [nameLen u16][name][id u16][type u8][sinceVersion u16][isKey u8][enumCount u16]
    // One field "id" of type Int64 (=1), id=0, sinceVersion=1, isKey=0, no enum values.
    std::vector<uint8_t> blob;
    auto pushU16 = [&](uint16_t v) {
        blob.push_back(static_cast<uint8_t>(v & 0xff));
        blob.push_back(static_cast<uint8_t>((v >> 8) & 0xff));
    };
    auto pushU32 = [&](uint32_t v) {
        blob.push_back(static_cast<uint8_t>(v & 0xff));
        blob.push_back(static_cast<uint8_t>((v >> 8) & 0xff));
        blob.push_back(static_cast<uint8_t>((v >> 16) & 0xff));
        blob.push_back(static_cast<uint8_t>((v >> 24) & 0xff));
    };
    auto pushStr = [&](const std::string& s) {
        pushU16(static_cast<uint16_t>(s.size()));
        for (unsigned char c : s) blob.push_back(c);
    };

    pushU32(0x31444352u); // "RCD1"
    pushStr("t");         // schema name
    pushU16(1);           // version
    pushU16(1);           // fieldCount

    pushStr("id");                                   // field name
    pushU16(0);                                       // id (positional)
    blob.push_back(static_cast<uint8_t>(rune::codec::PrimType::Int64)); // type
    pushU16(1);                                        // sinceVersion
    blob.push_back(0);                                 // isKey
    pushU16(0);                                        // enumCount

    auto d = rune::codec::deserialize(blob);
    ASSERT_NE(d.find("id"), nullptr);
    EXPECT_EQ(d.find("id")->id, 0u);
    EXPECT_EQ(d.find("id")->untilVersion, 0xFFFFu);
}

TEST(Serialize, Rcd2TruncatedBlobThrows) {
    rune::codec::DescriptorBuilder b("t"); b.add("id", rune::codec::PrimType::Int64);
    auto blob = rune::codec::serialize(b.build());
    blob.resize(blob.size()/2);
    EXPECT_THROW(rune::codec::deserialize(blob), std::runtime_error);
}

// Regression: RCD2 drops RCD1's sequential-id check (tags are explicit and
// durable, not positional), but it must still reject a blob with two fields
// sharing the same tag. Left unchecked, readField()'s first-match-by-id
// lookup and fingerprint()'s sort-by-id become ambiguous for the same bytes.
TEST(SerializeRegression, Rcd2DuplicateTagsRejectedOnDeserialize) {
    // Hand-crafted RCD2 blob, same field layout as
    // ReadsLegacyRcd1AsInfiniteUntilAndPositionalTag but with the RCD2 magic
    // and a trailing untilVersion per field. Two fields "a" and "b" both
    // carry tag=0.
    std::vector<uint8_t> blob;
    auto pushU16 = [&](uint16_t v) {
        blob.push_back(static_cast<uint8_t>(v & 0xff));
        blob.push_back(static_cast<uint8_t>((v >> 8) & 0xff));
    };
    auto pushU32 = [&](uint32_t v) {
        blob.push_back(static_cast<uint8_t>(v & 0xff));
        blob.push_back(static_cast<uint8_t>((v >> 8) & 0xff));
        blob.push_back(static_cast<uint8_t>((v >> 16) & 0xff));
        blob.push_back(static_cast<uint8_t>((v >> 24) & 0xff));
    };
    auto pushStr = [&](const std::string& s) {
        pushU16(static_cast<uint16_t>(s.size()));
        for (unsigned char c : s) blob.push_back(c);
    };
    auto pushField = [&](const std::string& name, uint16_t tag) {
        pushStr(name);                                                         // name
        pushU16(tag);                                                          // tag (duplicate across fields)
        blob.push_back(static_cast<uint8_t>(rune::codec::PrimType::Int64));    // type
        pushU16(1);                                                            // sinceVersion
        blob.push_back(0);                                                    // isKey
        pushU16(0);                                                            // enumCount
        pushU16(0xFFFF);                                                       // untilVersion (RCD2 only)
    };

    pushU32(0x32444352u); // "RCD2"
    pushStr("t");         // schema name
    pushU16(1);           // version
    pushU16(2);           // fieldCount

    pushField("a", 0);
    pushField("b", 0);    // same tag as "a" -- malformed/malicious

    EXPECT_THROW(rune::codec::deserialize(blob), std::runtime_error);
}
