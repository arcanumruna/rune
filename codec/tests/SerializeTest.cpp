// tests/SerializeTest.cpp
#include <gtest/gtest.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Fingerprint.h>
using namespace rune::codec;

TEST(Serialize, RoundTrip) {
  auto d = DescriptorBuilder("characters")
    .add("id",PrimType::Int64).add("userId",PrimType::Int64,1,true)
    .add("type",PrimType::Enum,1,false,{0,1,2}).add("name",PrimType::String)
    .add("strength",PrimType::Int32).build();
  auto blob = serialize(d);
  auto d2 = deserialize(blob);
  EXPECT_EQ(fingerprint(d), fingerprint(d2));      // structurally identical
  EXPECT_EQ(d2.blockLength, d.blockLength);
  EXPECT_EQ(d2.find("type")->enumValues, (std::vector<int32_t>{0,1,2}));
}

TEST(Serialize, RejectsTruncated) {
  auto blob = serialize(DescriptorBuilder("t").add("a",PrimType::Int64).build());
  blob.resize(blob.size()/2);
  EXPECT_THROW(deserialize(blob), std::runtime_error);
}
