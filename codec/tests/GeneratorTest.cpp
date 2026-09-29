// tests/GeneratorTest.cpp
#include <gtest/gtest.h>
#include <rune/codec/Generator.h>
#include <rune/codec/Record.h>
using namespace rune::codec;

TEST(Generator, EmitsCompilableStructWithFingerprint) {
    auto d = DescriptorBuilder("characters").add("id",PrimType::Int64)
               .add("name",PrimType::String).build();
    const std::string hdr = generateHeader(d);
    EXPECT_NE(hdr.find("struct Characters"), std::string::npos);
    EXPECT_NE(hdr.find("kFingerprint"), std::string::npos);
    EXPECT_NE(hdr.find(std::to_string(fingerprint(d))), std::string::npos);
}
