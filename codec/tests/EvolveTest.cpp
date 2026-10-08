#include <gtest/gtest.h>
#include <cstring>
#include <vector>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Record.h>

static rune::codec::Descriptor evolving() {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("id",   1, rune::codec::PrimType::Int64, 1, 0xFFFF, true,  {});
    b.addTagged("age",  2, rune::codec::PrimType::Int32, 1, 3,      false, {}); // dropped at v3
    b.addTagged("score",3, rune::codec::PrimType::Int32, 2, 0xFFFF, false, {}); // added at v2
    return b.build();
}
TEST(Evolve, V1HasIdAndAgeNotScore) {
    auto d = rune::codec::materializeAsOf(evolving(), 1);
    EXPECT_NE(d.find("id"), nullptr);
    EXPECT_NE(d.find("age"), nullptr);
    EXPECT_EQ(d.find("score"), nullptr);
    EXPECT_EQ(d.find("age")->offset, 8u);        // after int64 id
    EXPECT_EQ(d.blockLength, 12u);               // 8 + 4
}
TEST(Evolve, V3DropsAgeAndReclaimsItsSlot) {
    auto d = rune::codec::materializeAsOf(evolving(), 3);
    EXPECT_EQ(d.find("age"), nullptr);           // dropped
    EXPECT_NE(d.find("score"), nullptr);
    EXPECT_EQ(d.find("score")->offset, 8u);      // reclaims age's slot
    EXPECT_EQ(d.blockLength, 12u);
}
TEST(Evolve, ReadFieldDecodesOldRecordAgainstItsVersion) {
    // a v1 record (version=1) carrying id + age; decode age via v1 view
    auto v1 = rune::codec::materializeAsOf(evolving(), 1);
    std::vector<uint8_t> rec(4 + 12, 0);
    rec[0]=12; rec[1]=0; rec[2]=1; rec[3]=0;          // blockLength=12, version=1
    int64_t id=42; std::memcpy(&rec[4], &id, 8);
    int32_t age=7; std::memcpy(&rec[12], &age, 4);
    auto fv = rune::codec::readField(v1, rec, /*tag=*/2);
    EXPECT_TRUE(fv.present); EXPECT_EQ(fv.i, 7);
}
