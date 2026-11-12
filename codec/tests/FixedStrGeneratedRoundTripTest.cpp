// tests/FixedStrGeneratedRoundTripTest.cpp
//
// Build-time generated round-trip test for a descriptor containing a FixedStr
// field.  The header is generated at build time by rune_codec_gen from
// tests/generated/fixedstr_sample.blob.  It constructs a FixedstrSample record
// via the generated encode() and asserts that readField() (the runtime reader)
// and Validator return consistent results — proving generated codec ⇄ runtime
// reader agreement for FixedStr fields.

#include <gtest/gtest.h>
#include "generated/fixedstr_sample.gen.h"   // generated at build time
#include <rune/codec/Record.h>
#include <rune/codec/Validator.h>
#include <cstring>
#include <type_traits>
#include <vector>

using namespace rune::codec;

TEST(FixedStrGen, EncodeRoundTripsThroughReadField) {
    rune::gen::FixedstrSample s;
    s.id = 42;
    s.code = "ABCD";   // rune::FixedString<8>, rest zero
    s.name = "hello";

    std::vector<uint8_t> buf(s.encodedSize());
    size_t n = s.encode(buf.data());
    buf.resize(n);

    const Descriptor& d = rune::gen::FixedstrSample::descriptor();
    EXPECT_TRUE(Validator(d).validate(buf).ok);

    FieldValue fId = readField(d, buf, d.find("id")->id);
    ASSERT_TRUE(fId.present);
    EXPECT_EQ(fId.i, 42);

    FieldValue code = readField(d, buf, d.find("code")->id);
    ASSERT_TRUE(code.present);
    EXPECT_EQ(code.bytes.size(), 8u);
    EXPECT_EQ(std::string_view(code.bytes.data(), 4), "ABCD");

    FieldValue name = readField(d, buf, d.find("name")->id);
    ASSERT_TRUE(name.present);
    EXPECT_EQ(name.bytes, "hello");
}

TEST(FixedStrGen, FingerprintMatchesRuntimeDescriptor) {
    const Descriptor& d = rune::gen::FixedstrSample::descriptor();
    EXPECT_EQ(rune::gen::FixedstrSample::kFingerprint, fingerprint(d));
}

TEST(FixedStrGen, MemberIsFixedStringRegression) {
    static_assert(std::is_same_v<decltype(rune::gen::FixedstrSample{}.code), rune::FixedString<8>>);
    rune::gen::FixedstrSample s{.id = 1, .code = "ABCDEFGH"};
    std::vector<uint8_t> buf(s.encodedSize());
    buf.resize(s.encode(buf.data()));
    rune::gen::FixedstrSample out;
    ASSERT_TRUE(out.decode(buf));
    EXPECT_EQ(out.code.view(), "ABCDEFGH");
}
