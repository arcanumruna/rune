#include <gtest/gtest.h>
#include "rune/ql/Parser.h"
#include "rune/ql/Backends.h"
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Generator.h>
#include <rune/codec/AlterOps.h>
using namespace rune::ql;
TEST(Backends, ToDescriptorMatchesFields) {
    auto t = std::get<TopicDef>(parse("topic characters { id int64, userId int64, name string, code string(8) } partitions 16 key userId;").value());
    auto d = toDescriptor(t);
    EXPECT_EQ(d.name, "characters");
    EXPECT_TRUE(d.find("userId")->isKey);
    EXPECT_EQ(d.find("code")->type, rune::codec::PrimType::FixedStr);
    EXPECT_EQ(d.find("code")->fixedLen, 8u);
}
TEST(Backends, ApplyPlanBlobDeserializes) {
    auto t = std::get<TopicDef>(parse("topic t { id int64, name string } partitions 4 key id;").value());
    auto plan = applyPlan(t);
    EXPECT_EQ(plan.name, "t");
    EXPECT_EQ(plan.partitions, 4u);
    EXPECT_EQ(plan.keyField, "id");
    auto back = rune::codec::deserialize(plan.descriptorBlob);
    EXPECT_EQ(rune::codec::fingerprint(back), rune::codec::fingerprint(toDescriptor(t)));
}
TEST(Backends, CodegenCompilesFromDescriptor) {
    auto t = std::get<TopicDef>(parse("topic t { id int64, code string(8) } partitions 1 key id;").value());
    std::string hdr = rune::codec::generateHeader(toDescriptor(t));
    EXPECT_NE(hdr.find("struct T"), std::string::npos);
    EXPECT_NE(hdr.find("std::array<char, 8>"), std::string::npos);
}

TEST(Backends, ApplyAlterPlanRoundTripsAddDropRename) {
    auto a = std::get<AlterTopicDef>(
        parse("alter topic users add column score int32, drop column age, "
              "rename column name to fullName;")
            .value());

    auto plan = applyAlterPlan(a, 7);
    EXPECT_EQ(plan.name, "users");
    EXPECT_EQ(plan.expectedVersion, 7u);

    auto decoded = rune::codec::decodeAlterOps(plan.opsBlob);
    ASSERT_TRUE(decoded.has_value());
    ASSERT_EQ(decoded->size(), 3u);

    EXPECT_EQ((*decoded)[0].type, rune::codec::AlterOpType::Add);
    EXPECT_EQ((*decoded)[0].name, "score");
    EXPECT_EQ((*decoded)[0].primType, rune::codec::PrimType::Int32);
    EXPECT_EQ((*decoded)[0].fixedLen, 0u);

    EXPECT_EQ((*decoded)[1].type, rune::codec::AlterOpType::Drop);
    EXPECT_EQ((*decoded)[1].name, "age");

    EXPECT_EQ((*decoded)[2].type, rune::codec::AlterOpType::Rename);
    EXPECT_EQ((*decoded)[2].name, "name");
    EXPECT_EQ((*decoded)[2].newName, "fullName");
}

TEST(Backends, ApplyAlterPlanRoundTripsAddFixedStr) {
    auto a = std::get<AlterTopicDef>(
        parse("alter topic users add column code string(8);").value());

    auto plan = applyAlterPlan(a);
    EXPECT_EQ(plan.expectedVersion, 0u);

    auto decoded = rune::codec::decodeAlterOps(plan.opsBlob);
    ASSERT_TRUE(decoded.has_value());
    ASSERT_EQ(decoded->size(), 1u);
    EXPECT_EQ((*decoded)[0].type, rune::codec::AlterOpType::Add);
    EXPECT_EQ((*decoded)[0].name, "code");
    EXPECT_EQ((*decoded)[0].primType, rune::codec::PrimType::FixedStr);
    EXPECT_EQ((*decoded)[0].fixedLen, 8u);
}

TEST(Backends, ApplyAlterPlanRoundTripsSetCompatibility) {
    auto fwd = std::get<AlterTopicDef>(
        parse("alter topic users set compatibility forward;").value());
    auto fwdPlan = applyAlterPlan(fwd);
    auto fwdDecoded = rune::codec::decodeAlterOps(fwdPlan.opsBlob);
    ASSERT_TRUE(fwdDecoded.has_value());
    ASSERT_EQ(fwdDecoded->size(), 1u);
    EXPECT_EQ((*fwdDecoded)[0].type, rune::codec::AlterOpType::SetCompatibility);
    EXPECT_EQ((*fwdDecoded)[0].mode, rune::codec::CompatMode::Forward);

    auto bwd = std::get<AlterTopicDef>(
        parse("alter topic users set compatibility backward;").value());
    auto bwdPlan = applyAlterPlan(bwd);
    auto bwdDecoded = rune::codec::decodeAlterOps(bwdPlan.opsBlob);
    ASSERT_TRUE(bwdDecoded.has_value());
    ASSERT_EQ(bwdDecoded->size(), 1u);
    EXPECT_EQ((*bwdDecoded)[0].type, rune::codec::AlterOpType::SetCompatibility);
    EXPECT_EQ((*bwdDecoded)[0].mode, rune::codec::CompatMode::Backward);
}

// ── Regression ───────────────────────────────────────────────────────────────
// Guards the AlterTopicPlan backend (Task 17): every AlterOpKind must map to
// its codec AlterOpType and round-trip through encode/decode unchanged.
TEST(BackendsRegression, ApplyAlterPlanAllOpKindsRoundTrip) {
    auto a = std::get<AlterTopicDef>(
        parse("alter topic users add column score int32, drop column age, "
              "rename column name to fullName, set compatibility forward;")
            .value());
    auto plan = applyAlterPlan(a, 42);
    auto decoded = rune::codec::decodeAlterOps(plan.opsBlob);
    ASSERT_TRUE(decoded.has_value());
    ASSERT_EQ(decoded->size(), 4u);
    EXPECT_EQ((*decoded)[0].type, rune::codec::AlterOpType::Add);
    EXPECT_EQ((*decoded)[1].type, rune::codec::AlterOpType::Drop);
    EXPECT_EQ((*decoded)[2].type, rune::codec::AlterOpType::Rename);
    EXPECT_EQ((*decoded)[3].type, rune::codec::AlterOpType::SetCompatibility);
    EXPECT_EQ((*decoded)[3].mode, rune::codec::CompatMode::Forward);
    EXPECT_EQ(plan.expectedVersion, 42u);
    EXPECT_EQ(plan.name, "users");
}
