// libs/view/tests/view/ViewRowTest.cpp
#include <gtest/gtest.h>
#include <climits>
#include <cstring>
#include <string>
#include <rune/view/ViewRow.h>
#include <rune/codec/Descriptor.h>
using namespace rune::view;
using rune::codec::DescriptorBuilder; using rune::codec::PrimType; using rune::codec::FieldValue;

static rune::codec::Descriptor twoColDesc() {
    return DescriptorBuilder("v", 1)
        .add("id", PrimType::Int64, 1, /*isKey*/true)
        .add("hp", PrimType::Int32).build();
}

TEST(ViewRow, LayoutWidthAndRoundTrip) {
    auto d = twoColDesc();
    RowLayout L = buildRowLayout(d);
    EXPECT_EQ(L.bitmapBytes, 1u);           // 2 fields -> 1 byte bitmap
    EXPECT_EQ(L.rowLen, 1u + 8u + 4u);      // bitmap + i64 + i32
    std::vector<uint8_t> row(L.rowLen, 0);
    FieldValue v{}; v.type = PrimType::Int64; v.present = true; v.i = 42;
    writeSlot(row.data(), L, 0, v);
    EXPECT_FALSE(isNull(row.data(), L, 0));
    EXPECT_EQ(readSlot(row.data(), L, 0).i, 42);
}

TEST(ViewRow, AbsentSetsNullBitNotZeroSentinel) {
    auto d = twoColDesc();
    RowLayout L = buildRowLayout(d);
    std::vector<uint8_t> row(L.rowLen, 0);
    FieldValue absent{}; absent.present = false; absent.type = PrimType::Int32;
    writeSlot(row.data(), L, 1, absent);
    EXPECT_TRUE(isNull(row.data(), L, 1));
    EXPECT_FALSE(readSlot(row.data(), L, 1).present); // distinct from a real 0
}

TEST(ViewRow, VarLenColumnRejected) {
    auto d = DescriptorBuilder("v",1).add("id",PrimType::Int64,1,true)
             .add("name",PrimType::String).build();
    EXPECT_THROW(buildRowLayout(d), std::invalid_argument);
}

// #667: a descriptor that bypasses the builder (hand-assembled fields) still never yields a zero-width slot.
TEST(ViewRowRegression, BuildRowLayoutRefusesAZeroWidthFixedStrRegression) {
    auto d = DescriptorBuilder("v",1).add("id",PrimType::Int64,1,true).addFixedStr("code",4).build();
    EXPECT_NO_THROW(buildRowLayout(d));
    d.fields[1].fixedLen = 0;
    try {
        buildRowLayout(d);
        FAIL() << "a zero-width FixedStr produced a layout";
    } catch (const std::invalid_argument& e) {
        EXPECT_NE(std::string(e.what()).find("FixedStr column 'code' has width 0"), std::string::npos);
    }
}

TEST(ViewRow, SlotValueCodecRoundTripsEveryFixedType) {
    using P = rune::codec::PrimType;
    uint8_t buf[8];
    auto rt = [&](P t, uint16_t w, const rune::codec::FieldValue& v) {
        std::memset(buf, 0xAB, sizeof buf);
        rune::view::encodeSlotValue(buf, t, w, v);
        return rune::view::decodeSlotValue(buf, t, w);
    };
    rune::codec::FieldValue v{};
    v.i = -5;          EXPECT_EQ(rt(P::Int32, 4, v).i, -5);
    v.i = INT64_MIN;   EXPECT_EQ(rt(P::Int64, 8, v).i, INT64_MIN);
    v.u = 65535;       EXPECT_EQ(rt(P::UInt16, 2, v).u, 65535u);
    v.u = 4000000000u; EXPECT_EQ(rt(P::UInt32, 4, v).u, 4000000000u);
    v.u = 7;           EXPECT_EQ(rt(P::Enum, 4, v).u, 7u);
    v.u = UINT64_MAX;  EXPECT_EQ(rt(P::UInt64, 8, v).u, UINT64_MAX);
    v.f = -2.5;        EXPECT_DOUBLE_EQ(rt(P::Float64, 8, v).f, -2.5);
    v.b = true;        EXPECT_TRUE(rt(P::Bool, 1, v).b);
    v.bytes = "AB";
    const auto s = rt(P::FixedStr, 4, v);
    EXPECT_EQ(std::string(s.bytes), std::string("AB\0\0", 4)) << "short FixedStr is zero-padded to width";
    EXPECT_TRUE(s.present);
    EXPECT_EQ(s.type, P::FixedStr);
}

// F6: golden bytes. Comparing against encodeSlotValue would be a tautology,
// because writeSlot now calls it.
TEST(ViewRow, WriteSlotWritesGoldenBytesAtTheSlotOffset) {
    using P = rune::codec::PrimType;
    const auto d = rune::codec::DescriptorBuilder("t", 1).add("a", P::Int32).addFixedStr("s", 3).build();
    const auto L = rune::view::buildRowLayout(d);
    std::vector<uint8_t> row(L.rowLen, 0xEE);   // a stale-byte canary: padding must really be written
    rune::codec::FieldValue a{}; a.type = P::Int32; a.present = true; a.i = -7;
    rune::codec::FieldValue s{}; s.type = P::FixedStr; s.present = true; s.bytes = "xy";
    rune::view::writeSlot(row.data(), L, 0, a);
    rune::view::writeSlot(row.data(), L, 1, s);
    const uint8_t goldenA[] = {0xF9, 0xFF, 0xFF, 0xFF};   // Int32 -7, little-endian
    const uint8_t goldenS[] = {0x78, 0x79, 0x00};         // FixedStr "xy" at width 3, zero-padded
    EXPECT_EQ(std::memcmp(row.data() + L.slotOffset[0], goldenA, 4), 0);
    EXPECT_EQ(std::memcmp(row.data() + L.slotOffset[1], goldenS, 3), 0);
    EXPECT_EQ(rune::view::readSlot(row.data(), L, 0).i, -7);
}

// The min(width, size) clamp is the bounds guard for Last-value storage: a
// longer value is truncated at the width and the byte past it stays intact.
TEST(ViewRow, SlotValueCodecTruncatesFixedStrAtWidthAndWritesBoolFalse) {
    using P = rune::codec::PrimType;
    uint8_t buf[8];
    std::memset(buf, 0xAB, sizeof buf);
    rune::codec::FieldValue v{};
    v.bytes = "ABCDEF";
    rune::view::encodeSlotValue(buf, P::FixedStr, 4, v);
    EXPECT_EQ(std::string(rune::view::decodeSlotValue(buf, P::FixedStr, 4).bytes), "ABCD");
    EXPECT_EQ(buf[4], 0xAB) << "nothing is written past the slot width";

    std::memset(buf, 0xAB, sizeof buf);
    v.b = false;
    rune::view::encodeSlotValue(buf, P::Bool, 1, v);
    EXPECT_EQ(buf[0], 0x00);
    EXPECT_FALSE(rune::view::decodeSlotValue(buf, P::Bool, 1).b);

    std::memset(buf, 0xAB, sizeof buf);
    v.f = -2.5;
    rune::view::encodeSlotValue(buf, P::Float64, 8, v);
    const uint8_t goldenF[] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0xC0};   // -2.5, little-endian IEEE-754
    EXPECT_EQ(std::memcmp(buf, goldenF, 8), 0);
}
