#include <gtest/gtest.h>
#include <algorithm>
#include <cstring>
#include <vector>
#include <rune/codec/AlterOps.h>
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

TEST(Evolve, CheckCompatibilityRejectsDropUnderForward) {
    std::vector<rune::codec::AlterOp> ops = {{rune::codec::AlterOpType::Drop,"age","",{},0,{}}};
    EXPECT_FALSE(rune::codec::checkCompatibility(ops, rune::codec::CompatMode::Forward).has_value());
    EXPECT_TRUE (rune::codec::checkCompatibility(ops, rune::codec::CompatMode::Backward).has_value());
}
TEST(Evolve, AddAllocatesFreshTagAndDropRetiresTag) {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("id", 1, rune::codec::PrimType::Int64, 1, 0xFFFF, true, {});
    b.addTagged("age",2, rune::codec::PrimType::Int32, 1, 0xFFFF, false,{});
    auto cur = b.build(); uint16_t nextTag = 3;
    auto d1 = rune::codec::applyOps(cur, {{rune::codec::AlterOpType::Drop,"age","",{},0,{}}}, nextTag, 2);
    ASSERT_TRUE(d1.has_value());
    EXPECT_EQ(d1->find("age")->untilVersion, 2);     // retired at v2
    auto d2 = rune::codec::applyOps(*d1, {{rune::codec::AlterOpType::Add,"age","",rune::codec::PrimType::Int64,0,{}}}, nextTag, 3);
    ASSERT_TRUE(d2.has_value());
    // retired name reused -> FRESH tag 3, old retired tag 2 still present & retired
    EXPECT_EQ(nextTag, 4);
    EXPECT_EQ(std::count_if(d2->fields.begin(), d2->fields.end(), [](auto&f){return f.name=="age";}), 2);
}
TEST(Evolve, DropKeyFieldRejected) {
    rune::codec::DescriptorBuilder b("t"); b.addTagged("id",1,rune::codec::PrimType::Int64,1,0xFFFF,true,{});
    auto cur=b.build(); uint16_t nt=2;
    EXPECT_FALSE(rune::codec::applyOps(cur, {{rune::codec::AlterOpType::Drop,"id","",{},0,{}}}, nt, 2).has_value());
}
TEST(Evolve, AddRejectsCollisionWithLiveField) {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("id", 1, rune::codec::PrimType::Int64, 1, 0xFFFF, true, {});
    auto cur = b.build(); uint16_t nt = 2;
    auto r = rune::codec::applyOps(
        cur, {{rune::codec::AlterOpType::Add,"id","",rune::codec::PrimType::Int32,0,{}}}, nt, 2);
    EXPECT_FALSE(r.has_value());
    EXPECT_EQ(nt, 2); // nextTag untouched on rejection
}
TEST(Evolve, DropRejectsAbsentField) {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("id", 1, rune::codec::PrimType::Int64, 1, 0xFFFF, true, {});
    auto cur = b.build(); uint16_t nt = 2;
    EXPECT_FALSE(rune::codec::applyOps(
        cur, {{rune::codec::AlterOpType::Drop,"nope","",{},0,{}}}, nt, 2).has_value());
}
TEST(Evolve, DropRejectsAlreadyDropped) {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("id", 1, rune::codec::PrimType::Int64, 1, 0xFFFF, true, {});
    b.addTagged("age",2, rune::codec::PrimType::Int32, 1, 0xFFFF, false,{});
    auto cur = b.build(); uint16_t nt = 3;
    auto d1 = rune::codec::applyOps(cur, {{rune::codec::AlterOpType::Drop,"age","",{},0,{}}}, nt, 2);
    ASSERT_TRUE(d1.has_value());
    EXPECT_FALSE(rune::codec::applyOps(
        *d1, {{rune::codec::AlterOpType::Drop,"age","",{},0,{}}}, nt, 3).has_value());
}
TEST(Evolve, DropRejectsLastRemainingLiveField) {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("age", 1, rune::codec::PrimType::Int32, 1, 0xFFFF, false, {});
    auto cur = b.build(); uint16_t nt = 2;
    EXPECT_FALSE(rune::codec::applyOps(
        cur, {{rune::codec::AlterOpType::Drop,"age","",{},0,{}}}, nt, 2).has_value());
}
TEST(Evolve, RenameRenamesLiveFieldKeepingTagAndType) {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("id", 1, rune::codec::PrimType::Int64, 1, 0xFFFF, true, {});
    b.addTagged("age",2, rune::codec::PrimType::Int32, 1, 0xFFFF, false,{});
    auto cur = b.build(); uint16_t nt = 3;
    auto d = rune::codec::applyOps(
        cur, {{rune::codec::AlterOpType::Rename,"age","years",{},0,{}}}, nt, 2);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->find("age"), nullptr);
    ASSERT_NE(d->find("years"), nullptr);
    EXPECT_EQ(d->find("years")->id, 2);
    EXPECT_EQ(d->find("years")->type, rune::codec::PrimType::Int32);
    EXPECT_EQ(nt, 3); // RENAME allocates no tag
}
TEST(Evolve, RenameRejectsAbsentOldName) {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("id", 1, rune::codec::PrimType::Int64, 1, 0xFFFF, true, {});
    auto cur = b.build(); uint16_t nt = 2;
    EXPECT_FALSE(rune::codec::applyOps(
        cur, {{rune::codec::AlterOpType::Rename,"nope","other",{},0,{}}}, nt, 2).has_value());
}
TEST(Evolve, RenameRejectsCollisionWithLiveField) {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("id", 1, rune::codec::PrimType::Int64, 1, 0xFFFF, true, {});
    b.addTagged("age",2, rune::codec::PrimType::Int32, 1, 0xFFFF, false,{});
    auto cur = b.build(); uint16_t nt = 3;
    EXPECT_FALSE(rune::codec::applyOps(
        cur, {{rune::codec::AlterOpType::Rename,"age","id",{},0,{}}}, nt, 2).has_value());
}
TEST(Evolve, SetCompatibilityIsNoopInApplyOps) {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("id", 1, rune::codec::PrimType::Int64, 1, 0xFFFF, true, {});
    auto cur = b.build(); uint16_t nt = 2;
    auto d = rune::codec::applyOps(
        cur, {{rune::codec::AlterOpType::SetCompatibility,"","",{},0,rune::codec::CompatMode::Forward}},
        nt, 2);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->fields.size(), cur.fields.size());
    EXPECT_EQ(nt, 2);
}
TEST(Evolve, ApplyOpsIsAtomicOnFirstError) {
    // The ADD would succeed in isolation, but the DROP that follows fails
    // (dropping the key field) -- the whole call must be rejected as one
    // unit, and nextTag must not advance, so the caller can safely discard
    // the result and retry without having burned a tag.
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("id", 1, rune::codec::PrimType::Int64, 1, 0xFFFF, true, {});
    auto cur = b.build(); uint16_t nt = 2;
    auto r = rune::codec::applyOps(
        cur,
        {{rune::codec::AlterOpType::Add,"age","",rune::codec::PrimType::Int32,0,{}},
         {rune::codec::AlterOpType::Drop,"id","",{},0,{}}},
        nt, 2);
    EXPECT_FALSE(r.has_value());
    EXPECT_EQ(nt, 2); // tag allocation is rolled back on failure, not leaked
}
