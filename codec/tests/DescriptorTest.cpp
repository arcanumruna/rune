#include <gtest/gtest.h>
#include <rune/codec/Descriptor.h>
using namespace rune::codec;

TEST(Descriptor, LayoutFixedThenVar) {
  auto d = DescriptorBuilder("characters")
    .add("id", PrimType::Int64)
    .add("userId", PrimType::Int64, 1, /*isKey=*/true)
    .add("name", PrimType::String)         // var
    .add("strength", PrimType::Int32)
    .build();
  EXPECT_EQ(d.name, "characters");
  EXPECT_EQ(d.blockLength, 20u);           // int64+int64+int32 = 8+8+4
  EXPECT_EQ(d.find("id")->offset, 0u);
  EXPECT_EQ(d.find("userId")->offset, 8u);
  EXPECT_TRUE(d.find("userId")->isKey);
  EXPECT_EQ(d.find("strength")->offset, 16u);
  EXPECT_EQ(d.find("name")->offset, 0u);   // var index 0
  EXPECT_TRUE(isVarLen(d.find("name")->type));
  EXPECT_EQ(d.find("id")->id, 0u);
  EXPECT_EQ(d.find("strength")->id, 3u);
}
