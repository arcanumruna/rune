// tests/CheckedWritesViewKeyEncodingTest.cpp
#include "rune/codec/ViewKeyEncoding.h"
#include <gtest/gtest.h>
#include <array>
#include <climits>
#include <cstring>
#include <random>
#include <string>
#include <vector>

using namespace rune::codec;

namespace {

FieldValue uval(PrimType t, uint64_t u) { FieldValue v; v.type = t; v.present = true; v.u = u; return v; }
FieldValue ival(PrimType t, int64_t i)  { FieldValue v; v.type = t; v.present = true; v.i = i; return v; }
FieldValue bval(bool b)                 { FieldValue v; v.type = PrimType::Bool; v.present = true; v.b = b; v.i = b; return v; }
FieldValue sval(std::string_view s)     { FieldValue v; v.type = PrimType::FixedStr; v.present = true; v.bytes = s; return v; }

std::vector<uint8_t> enc(PrimType t, uint8_t fn, const FieldValue& v, uint16_t fixedLen = 0) {
    std::vector<uint8_t> out(viewKeyComponentWidth(t, fixedLen, fn));
    EXPECT_TRUE(encodeKeyComponent(t, fn, v, out));
    return out;
}

FieldValue randomValue(PrimType t, std::mt19937_64& rng) {
    switch (t) {
        case PrimType::Int32:  return ival(t, int32_t(rng()));
        case PrimType::Int64:  return ival(t, int64_t(rng()));
        case PrimType::UInt16: return uval(t, uint16_t(rng()));
        case PrimType::UInt32: return uval(t, uint32_t(rng()));
        case PrimType::Enum:   return uval(t, uint32_t(rng()));
        default:               return uval(t, rng());
    }
}

} // namespace

TEST(CheckedWritesViewKeyEncoding, UnsignedIsBigEndian) {
    EXPECT_EQ(enc(PrimType::UInt16, 0, uval(PrimType::UInt16, 0x0102)), (std::vector<uint8_t>{1, 2}));
    EXPECT_EQ(enc(PrimType::UInt32, 0, uval(PrimType::UInt32, 1)), (std::vector<uint8_t>{0, 0, 0, 1}));
    EXPECT_EQ(enc(PrimType::UInt64, 0, uval(PrimType::UInt64, 0x0102030405060708ull)),
              (std::vector<uint8_t>{1, 2, 3, 4, 5, 6, 7, 8}));
}

TEST(CheckedWritesViewKeyEncoding, SignedFlipsTheSignBit) {
    EXPECT_EQ(enc(PrimType::Int64, 0, ival(PrimType::Int64, -1)),
              (std::vector<uint8_t>{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}));
    EXPECT_EQ(enc(PrimType::Int64, 0, ival(PrimType::Int64, 0)),
              (std::vector<uint8_t>{0x80, 0, 0, 0, 0, 0, 0, 0}));
    EXPECT_EQ(enc(PrimType::Int32, 0, ival(PrimType::Int32, INT32_MIN)), (std::vector<uint8_t>{0, 0, 0, 0}));
}

TEST(CheckedWritesViewKeyEncoding, OrderIsPreserved) {
    std::mt19937_64 rng(42);
    for (PrimType t : {PrimType::Int32, PrimType::Int64, PrimType::UInt16, PrimType::UInt32, PrimType::UInt64, PrimType::Enum}) {
        const bool isSigned = t == PrimType::Int32 || t == PrimType::Int64;
        for (int n = 0; n < 10000; ++n) {
            const FieldValue a = randomValue(t, rng), b = randomValue(t, rng);
            const auto ea = enc(t, 0, a), eb = enc(t, 0, b);
            const int  c  = std::memcmp(ea.data(), eb.data(), ea.size());
            const bool lt = isSigned ? a.i < b.i : a.u < b.u;
            const bool gt = isSigned ? a.i > b.i : a.u > b.u;
            ASSERT_EQ(lt, c < 0);
            ASSERT_EQ(gt, c > 0);
        }
    }
}

TEST(CheckedWritesViewKeyEncoding, BoolAndFixedStr) {
    EXPECT_EQ(enc(PrimType::Bool, 0, bval(true)), (std::vector<uint8_t>{1}));
    EXPECT_EQ(enc(PrimType::Bool, 0, bval(false)), (std::vector<uint8_t>{0}));
    EXPECT_EQ(enc(PrimType::FixedStr, 0, sval("ab"), 8), (std::vector<uint8_t>{0x61, 0x62, 0, 0, 0, 0, 0, 0}));
}

TEST(CheckedWritesViewKeyEncoding, Widths) {
    EXPECT_EQ(viewKeyComponentWidth(PrimType::UInt16, 0, kKeyFunctionNone), 2u);
    EXPECT_EQ(viewKeyComponentWidth(PrimType::Int32, 0, kKeyFunctionNone), 4u);
    EXPECT_EQ(viewKeyComponentWidth(PrimType::UInt64, 0, kKeyFunctionNone), 8u);
    EXPECT_EQ(viewKeyComponentWidth(PrimType::Bool, 0, kKeyFunctionNone), 1u);
    EXPECT_EQ(viewKeyComponentWidth(PrimType::Enum, 0, kKeyFunctionNone), 4u);
    EXPECT_EQ(viewKeyComponentWidth(PrimType::FixedStr, 64, kKeyFunctionNone), 64u);
    EXPECT_EQ(viewKeyComponentWidth(PrimType::FixedStr, 16, kKeyFunctionLower), 24u);
    EXPECT_EQ(viewKeyComponentWidth(PrimType::FixedStr, 64, kKeyFunctionLower), 96u);
    EXPECT_EQ(viewKeyComponentWidth(PrimType::UInt64, 0, kKeyFunctionLower), 0u);
    EXPECT_EQ(viewKeyComponentWidth(PrimType::FixedStr, 16, kKeyFunctionCasefold), 0u);
    EXPECT_EQ(viewKeyComponentWidth(PrimType::FixedStr, 16, 3), 0u);
    EXPECT_EQ(viewKeyComponentWidth(PrimType::Float64, 0, kKeyFunctionNone), 0u);
    EXPECT_EQ(viewKeyComponentWidth(PrimType::String, 0, kKeyFunctionNone), 0u);
    EXPECT_EQ(viewKeyComponentWidth(PrimType::Bytes, 0, kKeyFunctionNone), 0u);
    // (provider uint16, subject string(64))
    EXPECT_EQ(viewKeyComponentWidth(PrimType::UInt16, 0, kKeyFunctionNone) + viewKeyComponentWidth(PrimType::FixedStr, 64, kKeyFunctionNone), 66u);
}

TEST(CheckedWritesViewKeyEncoding, LowerMatchesLowerUtf8) {
    const std::string src("FoO\0\0\0\0\0\0\0\0\0\0\0\0\0", 16);
    const auto        got = enc(PrimType::FixedStr, kKeyFunctionLower, sval(src), 16);
    std::array<uint8_t, 24> want{};
    ASSERT_TRUE(lowerUtf8({reinterpret_cast<const uint8_t*>(src.data()), src.size()}, want).ok);
    EXPECT_EQ(got, std::vector<uint8_t>(want.begin(), want.end()));
    EXPECT_EQ(got[0], 'f');
}

TEST(CheckedWritesViewKeyEncodingRegression, LoweredComponentNeverExceedsItsWidthRegression) {
    const std::vector<std::string> alphabet = {"a", "Z", "\xC8\xBA", "\xC8\xBE", "\xC4\xB0", "\xC3\xA9", "\xF0\x9E\xA4\xA1", "\xE2\xB1\xA2"};
    std::mt19937 rng(7);
    for (uint32_t n = 1; n <= 85; ++n) {
        const uint32_t w = maxLoweredWidth(n);
        for (int it = 0; it < 2000; ++it) {
            std::string s;
            for (;;) {
                const std::string& c = alphabet[rng() % alphabet.size()];
                if (s.size() + c.size() > n) break;
                s += c;
                if (rng() % 8 == 0) break;
            }
            std::array<uint8_t, 256> buf;
            buf.fill(0xAB);
            const std::size_t off = 16;
            ASSERT_TRUE(encodeKeyComponent(PrimType::FixedStr, kKeyFunctionLower, sval(s), std::span<uint8_t>(buf).subspan(off, w)))
                << "n=" << n << " it=" << it;
            for (std::size_t k = 0; k < buf.size(); ++k)
                if (k < off || k >= off + w) ASSERT_EQ(buf[k], 0xAB) << "guard byte " << k << " touched, n=" << n;
        }
    }
}

TEST(CheckedWritesViewKeyEncoding, DecodeInvertsEncode) {
    std::mt19937_64 rng(11);
    for (PrimType t : {PrimType::Int32, PrimType::Int64, PrimType::UInt16, PrimType::UInt32, PrimType::UInt64, PrimType::Enum}) {
        for (int n = 0; n < 1000; ++n) {
            const FieldValue v   = randomValue(t, rng);
            const auto       e   = enc(t, 0, v);
            const FieldValue got = decodeKeyComponent(t, 0, e);
            EXPECT_EQ(got.i, v.i);
            EXPECT_EQ(got.u, v.u);
        }
    }
    for (bool b : {false, true}) {
        const auto e = enc(PrimType::Bool, 0, bval(b));
        EXPECT_EQ(decodeKeyComponent(PrimType::Bool, 0, e).b, b);
    }
    const auto plain = enc(PrimType::FixedStr, 0, sval("hello"), 8);
    EXPECT_EQ(decodeKeyComponent(PrimType::FixedStr, 0, plain).bytes, std::string_view("hello\0\0\0", 8));
    const auto low = enc(PrimType::FixedStr, kKeyFunctionLower, sval("HeLLo"), 8);
    const auto d   = decodeKeyComponent(PrimType::FixedStr, kKeyFunctionLower, low);
    EXPECT_EQ(d.bytes.size(), low.size());
    EXPECT_EQ(d.bytes.substr(0, 5), "hello");
}

TEST(CheckedWritesViewKeyEncoding, AbsentOrWrongKindIsFalse) {
    std::array<uint8_t, 8> out{};
    FieldValue absent = uval(PrimType::UInt64, 5);
    absent.present = false;
    EXPECT_FALSE(encodeKeyComponent(PrimType::UInt64, 0, absent, out));
    EXPECT_FALSE(encodeKeyComponent(PrimType::UInt64, 0, sval("x"), out));
}
