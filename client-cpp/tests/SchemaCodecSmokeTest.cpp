#include <gtest/gtest.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
TEST(SchemaCodecSmoke, FingerprintAndRoundTrip) {
    using namespace rune::codec;
    Descriptor d = DescriptorBuilder("characters", 1)
        .add("id", PrimType::Int64)
        .add("userId", PrimType::Int64, 1, /*isKey=*/true)
        .add("name", PrimType::String)
        .build();
    const uint64_t fp = fingerprint(d);
    EXPECT_NE(fp, 0u);
    auto blob = serialize(d);
    Descriptor back = deserialize(blob);
    EXPECT_EQ(fingerprint(back), fp);
}
