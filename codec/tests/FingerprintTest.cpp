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

// I-3: name length and enum count are hashed as delimiters, so the name/enum
// byte boundary can no longer be slid between structurally distinct schemas.
// Before the fix these two descriptors produced identical fingerprints.
TEST(Fingerprint, NameEnumBoundaryNotSlidable) {
  // Field name "A" with an enum whitelist of one zero value.
  auto withEnum = DescriptorBuilder("s")
                    .add("A", PrimType::Enum, 1, false, {0})
                    .build();
  // A different field: name "A\0\0\0\0" with an empty enum whitelist.
  // Without length/count delimiters both hash the same trailing bytes.
  std::string slidName = std::string("A") + std::string(4, '\0');
  auto withoutEnum = DescriptorBuilder("s")
                       .add(slidName, PrimType::Enum, 1, false, {})
                       .build();
  EXPECT_NE(fingerprint(withEnum), fingerprint(withoutEnum));
}
