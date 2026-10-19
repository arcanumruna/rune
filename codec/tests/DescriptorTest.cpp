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

TEST(Descriptor, UntilVersionDefaultsToInfinity) {
    rune::codec::FieldDescriptor f;
    EXPECT_EQ(f.untilVersion, 0xFFFF);
}
TEST(Descriptor, IsLiveAtRespectsSinceAndUntil) {
    rune::codec::FieldDescriptor f; f.sinceVersion = 2; f.untilVersion = 4;
    EXPECT_FALSE(isLiveAt(f, 1));
    EXPECT_TRUE (isLiveAt(f, 2));
    EXPECT_TRUE (isLiveAt(f, 3));
    EXPECT_FALSE(isLiveAt(f, 4));   // until is exclusive
}
TEST(Descriptor, AddTaggedPreservesExplicitTagThroughBuild) {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("a", 7, rune::codec::PrimType::Int32, 1, 0xFFFF, false, {});
    b.addTagged("b", 3, rune::codec::PrimType::Int64, 1, 0xFFFF, false, {});
    auto d = b.build();
    EXPECT_EQ(d.find("a")->id, 7);   // NOT 0
    EXPECT_EQ(d.find("b")->id, 3);   // NOT 1
}

// Regression: mixing add() (auto-tag from fields_.size() at call time) with
// addTagged() in one builder is a footgun that can silently produce
// duplicate tags, corrupting the durable-tag invariant fingerprint() and
// readField() both rely on. build() must fail loudly instead.
TEST(DescriptorRegression, BuildRejectsDuplicateTagsFromAddTaggedCollision) {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("a", 5, rune::codec::PrimType::Int32, 1, 0xFFFF, false, {});
    b.addTagged("b", 5, rune::codec::PrimType::Int64, 1, 0xFFFF, false, {}); // same tag as "a"
    EXPECT_THROW(b.build(), std::invalid_argument);
}

// Sibling: a RETIRED field (untilVersion set) legitimately coexists with a
// LIVE field of the same NAME as long as the TAG differs -- that's the
// ordinary rename/replace evolution pattern and must still be allowed. Only
// tag collisions are rejected, never name collisions.
TEST(DescriptorRegression, RetiredFieldSameNameDifferentTagStillBuilds) {
    rune::codec::DescriptorBuilder b("t", /*version=*/3);
    b.addTagged("score", 1, rune::codec::PrimType::Int32, /*sinceVersion=*/1,
                /*untilVersion=*/2, false, {});   // retired at v2
    b.addTagged("score", 2, rune::codec::PrimType::Int64, /*sinceVersion=*/2,
                /*untilVersion=*/0xFFFF, false, {}); // live, different tag, same name
    Descriptor d;
    EXPECT_NO_THROW(d = b.build());
    EXPECT_EQ(d.fields.size(), 2u);
}
