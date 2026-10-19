#include <gtest/gtest.h>
#include <rune/codec/Descriptor.h>
using namespace rune::codec;

TEST(FixedStr, LaidInFixedBlockWithWidth) {
    Descriptor d = DescriptorBuilder("t", 1)
        .add("id", PrimType::Int64)          // offset 0, 8 bytes
        .addFixedStr("code", 8)              // offset 8, 8 bytes (FixedStr)
        .add("n", PrimType::Int32)           // offset 16, 4 bytes
        .build();
    const FieldDescriptor* code = d.find("code");
    ASSERT_NE(code, nullptr);
    EXPECT_EQ(code->type, PrimType::FixedStr);
    EXPECT_EQ(code->fixedLen, 8u);
    EXPECT_FALSE(isVarLen(code->type));      // block field, not var-data
    EXPECT_EQ(code->offset, 8u);             // after int64
    EXPECT_EQ(fieldWidth(*code), 8u);
    EXPECT_EQ(d.blockLength, 20u);           // 8 + 8 + 4
    EXPECT_EQ(d.find("n")->offset, 16u);
}
