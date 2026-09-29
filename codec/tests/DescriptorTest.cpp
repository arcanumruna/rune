#include <gtest/gtest.h>
#include <stdexcept>
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

// I-2 regression: the additive-only var-index invariant must be enforced by
// build(). Var fields whose sinceVersion decreases in var-index order would
// cause readField/Validator to mis-parse the var-data section across versions,
// so build() must reject such a descriptor outright.
TEST(DescriptorRegression, RejectsDecreasingVarSinceVersion) {
  DescriptorBuilder b("bad", /*version=*/3);
  b.add("early", PrimType::String, /*sinceVersion=*/2)   // var index 0
   .add("late",  PrimType::String, /*sinceVersion=*/1);  // var index 1 (decreasing!)
  EXPECT_THROW(b.build(), std::invalid_argument);
}

// Sibling: a non-decreasing var-index order (including equal versions) is fine.
TEST(DescriptorRegression, AcceptsNonDecreasingVarSinceVersion) {
  DescriptorBuilder b("good", /*version=*/3);
  b.add("a", PrimType::String, /*sinceVersion=*/1)
   .add("b", PrimType::String, /*sinceVersion=*/1)
   .add("c", PrimType::String, /*sinceVersion=*/3);
  EXPECT_NO_THROW(b.build());
}
