#include <gtest/gtest.h>
#include "rune/ql/Parser.h"
#include "rune/ql/Backends.h"
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Generator.h>
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
