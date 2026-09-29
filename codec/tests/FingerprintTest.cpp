#include <gtest/gtest.h>
#include <rune/codec/Fingerprint.h>
using namespace rune::codec;
static Descriptor base() {
  return DescriptorBuilder("t").add("a", PrimType::Int64).add("b", PrimType::Int32).build();
}
TEST(Fingerprint, DeterministicAndSensitive) {
  EXPECT_EQ(fingerprint(base()), fingerprint(base()));                 // deterministic
  auto added = DescriptorBuilder("t").add("a",PrimType::Int64)
                 .add("b",PrimType::Int32).add("c",PrimType::Bool,2).build();
  EXPECT_NE(fingerprint(base()), fingerprint(added));                  // additive change => new fp
  auto retyped = DescriptorBuilder("t").add("a",PrimType::Int32).add("b",PrimType::Int32).build();
  EXPECT_NE(fingerprint(base()), fingerprint(retyped));                // type change => new fp
}
