// tests/FixedStrReadValidateTest.cpp
#include <gtest/gtest.h>
#include <rune/codec/Record.h>
#include <rune/codec/Validator.h>
#include <cstring>
using namespace rune::codec;

TEST(FixedStrRead, ReadsBlockBytesAsView) {
    Descriptor d = DescriptorBuilder("t",1).add("id",PrimType::Int64).addFixedStr("code",8).build();
    ASSERT_EQ(d.blockLength, 16u);
    std::vector<uint8_t> rec(4 + 16, 0);
    rec[0] = 16; rec[1] = 0; rec[2] = 1; rec[3] = 0;   // blockLength=16, version=1
    std::memcpy(rec.data()+4+8, "ABCD\0\0\0\0", 8);      // code at block offset 8
    FieldValue fv = readField(d, rec, d.find("code")->id);
    ASSERT_TRUE(fv.present);
    EXPECT_EQ(fv.type, PrimType::FixedStr);
    EXPECT_EQ(fv.bytes.size(), 8u);
    EXPECT_EQ(std::string_view(fv.bytes.data(), 4), "ABCD");   // caller trims trailing NULs
    Validator v(d);
    EXPECT_TRUE(v.validate(rec).ok);
}
TEST(FixedStrRead, ValidatorRejectsShortBlock) {
    Descriptor d = DescriptorBuilder("t",1).add("id",PrimType::Int64).addFixedStr("code",8).build();
    std::vector<uint8_t> rec(4 + 10, 0);   // blockLength claims 16 but only 10 present
    rec[0] = 16; rec[1] = 0; rec[2] = 1; rec[3] = 0;
    Validator v(d);
    EXPECT_FALSE(v.validate(rec).ok);
}
