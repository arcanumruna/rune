// tests/ToTextTest.cpp
#include <gtest/gtest.h>
#include <rune/codec/Serialize.h>
using namespace rune::codec;
TEST(ToText, Golden) {
  auto d = DescriptorBuilder("characters").add("id",PrimType::Int64)
    .add("userId",PrimType::Int64,1,true).add("strength",PrimType::Int32)
    .add("name",PrimType::String).build();
  const std::string t = toText(d);
  EXPECT_NE(t.find("schema characters"), std::string::npos);
  EXPECT_NE(t.find("id        int64   @0"), std::string::npos);
  EXPECT_NE(t.find("userId    int64   @8    key"), std::string::npos);
  EXPECT_NE(t.find("name      string  vardata#0"), std::string::npos);
}
