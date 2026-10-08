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
