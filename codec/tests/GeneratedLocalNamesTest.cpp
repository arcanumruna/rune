// tests/GeneratedLocalNamesTest.cpp — fields named like the generator's own locals, parameters and private members
// (n, r, bl, ver, row, pos, len, at, record, p, buf, wr16/32/64, _tmp, v, s, Rebind, ...): the generated headers
// compile and every field round-trips through encode() / decode() / decodeAny_() and the Encoder / Decoder.
#include <gtest/gtest.h>
#include "generated/shadow_locals.gen.h"
#include "generated/shadow_locals_fixed.gen.h"
#include "generated/shadow_locals_str_key.gen.h"

#include <array>
#include <cstring>
#include <string_view>
#include <vector>

namespace {
using L  = rune::gen::ShadowLocals;
using LF = rune::gen::ShadowLocalsFixed;
using LS = rune::gen::ShadowLocalsStrKey;

template <class T> std::vector<uint8_t> encodeOf(const T& t) {
    std::vector<uint8_t> b(t.encodedSize());
    b.resize(t.encode(b.data()));
    return b;
}

L locals() {
    L x;
    x.record = 0x0102030405060708ull; x.n = -9; x.r = 7u; x.bl = 11; x.ver = 12; std::memcpy(x.row.data(), "rowval", 6);
    x.p = -1234567890123LL; x.buf = 13u; x.wr16 = 14; x.wr32 = 15u; x.wr64 = 16ull; x._tmp = -2.5; x.at = -17;
    x.v = 18; x.i = 19; x.w = 20u; x.out = 21u; x.b = true; x.pos = "position"; x.len = "lengthy";
    return x;
}
void expectLocals(const L& y) {
    const L x = locals();
    EXPECT_EQ(y.record, x.record); EXPECT_EQ(y.n, x.n); EXPECT_EQ(y.r, x.r); EXPECT_EQ(y.bl, x.bl); EXPECT_EQ(y.ver, x.ver);
    EXPECT_EQ(y.row, x.row); EXPECT_EQ(y.p, x.p); EXPECT_EQ(y.buf, x.buf); EXPECT_EQ(y.wr16, x.wr16);
    EXPECT_EQ(y.wr32, x.wr32); EXPECT_EQ(y.wr64, x.wr64); EXPECT_EQ(y._tmp, x._tmp); EXPECT_EQ(y.at, x.at);
    EXPECT_EQ(y.v, x.v); EXPECT_EQ(y.i, x.i); EXPECT_EQ(y.w, x.w); EXPECT_EQ(y.out, x.out); EXPECT_EQ(y.b, x.b);
    EXPECT_EQ(y.pos, x.pos); EXPECT_EQ(y.len, x.len);
}

LF localsFixed() {
    LF x;
    x.v = 0xA1A2A3A4A5A6A7A8ull; x.n = -3; x.r = 4u; x.bl = 5; x.ver = 6; std::memcpy(x.row.data(), "fixedrow", 8);
    x.p = -7; x._tmp = 8.75; x.buf = 9u; x.rec = 10u; x.keyValue = 11u; x.Rebind = 12u; x.off = 13u; x.w = 14u;
    x.i = 15u; x.s = -16;
    return x;
}
}  // namespace

TEST(GeneratedLocalNamesRegression, FieldsNamedLikeDecodeAndEncodeLocalsRoundTripRegression) {
    const L x = locals();
    const auto bytes = encodeOf(x);
    EXPECT_EQ(bytes.size(), x.encodedSize());
    EXPECT_EQ(x.key(), x.record);

    L fast;
    ASSERT_TRUE(fast.decode(bytes));
    expectLocals(fast);
    L any;
    ASSERT_TRUE(any.decodeAny_(bytes, nullptr));   // the lineage-row path decode() takes for any other version
    expectLocals(any);
    EXPECT_EQ(encodeOf(any), bytes);
}

TEST(GeneratedLocalNamesRegression, AllFixedFieldsNamedLikeEncoderAndDecoderLocalsRoundTripRegression) {
    const LF x = localsFixed();
    const auto bytes = encodeOf(x);
    ASSERT_EQ(bytes.size(), LF::Encoder::kEncodedLength);

    std::vector<uint8_t> claimed(LF::Encoder::kEncodedLength, 0xEE);
    LF::Encoder(claimed).key(x.v).n(x.n).r(x.r).bl(x.bl).ver(x.ver).row("fixedrow").p(x.p)._tmp(x._tmp).buf(x.buf)
        .rec(x.rec).keyValue(x.keyValue).Rebind(x.Rebind).off(x.off).w(x.w).i(x.i).s(x.s);
    EXPECT_EQ(claimed, bytes);
    std::vector<uint8_t> viaClaim(LF::Encoder::kEncodedLength, 0xEE);
    LF::ClaimEncoder(viaClaim, x.v).n(x.n).r(x.r).bl(x.bl).ver(x.ver).row("fixedrow").p(x.p)._tmp(x._tmp).buf(x.buf)
        .rec(x.rec).keyValue(x.keyValue).Rebind(x.Rebind).off(x.off).w(x.w).i(x.i).s(x.s);
    EXPECT_EQ(viaClaim, bytes);

    LF y;
    ASSERT_TRUE(y.decode(bytes));
    EXPECT_EQ(encodeOf(y), bytes);
    LF z;
    ASSERT_TRUE(z.decodeAny_(bytes, nullptr));
    EXPECT_EQ(encodeOf(z), bytes);

    const LF::Decoder d(bytes);
    ASSERT_TRUE(d.known());
    EXPECT_EQ(d.key(), x.v); EXPECT_EQ(d.v(), x.v); EXPECT_EQ(d.n(), x.n); EXPECT_EQ(d.r(), x.r); EXPECT_EQ(d.bl(), x.bl);
    EXPECT_EQ(d.ver(), x.ver); EXPECT_EQ(d.row(), "fixedrow"); EXPECT_EQ(d.p(), x.p); EXPECT_EQ(d._tmp(), x._tmp);
    EXPECT_EQ(d.buf(), x.buf); EXPECT_EQ(d.rec(), x.rec); EXPECT_EQ(d.keyValue(), x.keyValue);
    EXPECT_EQ(d.Rebind(), x.Rebind); EXPECT_EQ(d.off(), x.off); EXPECT_EQ(d.w(), x.w); EXPECT_EQ(d.i(), x.i);
    EXPECT_EQ(d.s(), x.s);
}

TEST(GeneratedLocalNamesRegression, AFixedStrKeyNamedLikeTheDecoderKeyLocalsRoundTripsRegression) {
    LS x;
    std::memcpy(x.s.data(), "skey0001", 8);
    x.v = -42;
    const auto bytes = encodeOf(x);

    std::vector<uint8_t> claimed(LS::Encoder::kEncodedLength, 0xEE);
    LS::Encoder(claimed).key(x.s).v(x.v);
    EXPECT_EQ(claimed, bytes);
    const LS::Decoder d(bytes);
    EXPECT_EQ(d.key(), x.s);
    EXPECT_EQ(d.s(), "skey0001");
    EXPECT_EQ(d.v(), x.v);
    LS y;
    ASSERT_TRUE(y.decode(bytes));
    EXPECT_EQ(y.s, x.s);
    EXPECT_EQ(y.v, x.v);
}
