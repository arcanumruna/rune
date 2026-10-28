// tests/FixedStrSerializeTest.cpp
#include <gtest/gtest.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Fingerprint.h>
using namespace rune::codec;

static Descriptor mk(uint16_t len) {
    return DescriptorBuilder("t",1).add("id",PrimType::Int64).addFixedStr("code",len).build();
}

TEST(FixedStrSerialize, RoundTripPreservesWidth) {
    Descriptor d = mk(8);
    Descriptor back = deserialize(serialize(d));
    ASSERT_NE(back.find("code"), nullptr);
    EXPECT_EQ(back.find("code")->type, PrimType::FixedStr);
    EXPECT_EQ(back.find("code")->fixedLen, 8u);
    EXPECT_EQ(back.blockLength, d.blockLength);
}

TEST(FixedStrSerialize, FingerprintDistinguishesWidth) {
    EXPECT_NE(fingerprint(mk(8)), fingerprint(mk(16)));
}

TEST(FixedStrSerialize, ExistingSchemasFingerprintUnchanged_Regression) {
    // A schema with NO FixedStr field must fingerprint exactly as before this change.
    // Golden value captured from pre-0c rune-codec main (commit 82d2135):
    //   DescriptorBuilder("chars",1).add("id",Int64).add("userId",Int64,1,true).add("name",String).build()
    constexpr uint64_t kGolden = 0x8FC60841B8667539ULL;
    Descriptor d = DescriptorBuilder("chars",1)
        .add("id",PrimType::Int64)
        .add("userId",PrimType::Int64,1,true)
        .add("name",PrimType::String)
        .build();
    EXPECT_EQ(fingerprint(d), kGolden);
}

TEST(FixedStrSerialize, ToTextRendersFixedWidth) {
    std::string t = toText(mk(8));
    EXPECT_NE(t.find("string(8)"), std::string::npos);
}

TEST(FixedStrSerialize, Rcd2RoundTripPreservesTagAndUntilForFixedStr) {
    DescriptorBuilder b("t");
    b.addFixedStrTagged("code", 7, 8, 1, 4, false);
    auto d = b.build();
    auto blob = serialize(d);
    auto d2 = deserialize(blob);
    auto* f = d2.find("code");
    ASSERT_NE(f, nullptr);
    EXPECT_EQ(f->id, 7u);
    EXPECT_EQ(f->fixedLen, 8u);
    EXPECT_EQ(f->untilVersion, 4u);
}

// #667: a FixedStr of width 0 has no storage; refused when the column is defined.
TEST(DescriptorBuilderRegression, FixedStrOfWidthZeroIsRefusedRegression) {
    EXPECT_THROW(DescriptorBuilder("t", 1).addFixedStr("code", 0), std::invalid_argument);
    EXPECT_THROW(DescriptorBuilder("t", 1).addFixedStrTagged("code", 3, 0, 1, 0xFFFF, false), std::invalid_argument);
    EXPECT_NO_THROW(mk(1));
    EXPECT_NO_THROW(mk(65535));
    EXPECT_EQ(mk(1).find("code")->fixedLen, 1u);
    EXPECT_EQ(mk(65535).find("code")->fixedLen, 65535u);
}

// The decoder reports a zero-width FixedStr like every other malformed field (std::runtime_error), never the
// builder's invalid_argument.  The FixedStr is the LAST field, so its fixedLen (u16 LE) sits just before the
// trailing untilVersion (u16 LE) of the RCD2 blob.
TEST(DescriptorBuilderRegression, DecodingAHistoricalZeroWidthFixedStrSucceedsRegression) {
    std::vector<uint8_t> blob = serialize(mk(8));
    ASSERT_GE(blob.size(), 4u);
    ASSERT_EQ(blob[blob.size() - 4], 8u);
    ASSERT_EQ(blob[blob.size() - 3], 0u);
    blob[blob.size() - 4] = 0;
    Descriptor d;
    ASSERT_NO_THROW(d = deserialize(blob));   // legal when written (#667): replay must accept it
    const FieldDescriptor* zf = zeroWidthFixedStr(d);
    ASSERT_NE(zf, nullptr);
    EXPECT_EQ(zf->fixedLen, 0u);
    EXPECT_EQ(zeroWidthFixedStr(deserialize(serialize(mk(8)))), nullptr);
}
