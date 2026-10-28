// tests/client/ViewRowCodecTest.cpp — buildRowLayout never guesses a FixedStr width (#162).
#include <gtest/gtest.h>

#include <rune/client/view/ViewRowCodec.h>
#include <rune/codec/PrimType.h>

#include <array>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace rcv = rune::client::view;
using rune::codec::PrimType;

TEST(ViewRowCodecRegression, FixedStrColumnWithoutWidthsIsRefusedNotMisdecodedRegression) {
    const std::array<PrimType, 3> types{PrimType::Int64, PrimType::FixedStr, PrimType::Int64};
    EXPECT_THROW(rcv::buildRowLayout(types), std::invalid_argument);
}

TEST(ViewRowCodecRegression, FixedStrColumnWithAZeroWidthIsRefusedRegression) {
    const std::array<PrimType, 3> types{PrimType::Int64, PrimType::FixedStr, PrimType::Int64};
    const std::array<uint16_t, 3> widths{0, 0, 0};
    EXPECT_THROW(rcv::buildRowLayout(types, widths), std::invalid_argument);
}

TEST(ViewRowCodecRegression, FixedStrColumnWithItsWidthDecodesEveryLaterColumnAtItsOffsetRegression) {
    const std::array<PrimType, 3> types{PrimType::Int64, PrimType::FixedStr, PrimType::Int64};
    const std::array<uint16_t, 3> widths{0, 5, 0};
    rcv::RowLayout layout;
    ASSERT_NO_THROW(layout = rcv::buildRowLayout(types, widths));
    EXPECT_EQ(layout.bitmapBytes, 1u);
    EXPECT_EQ(layout.slotOffset, (std::vector<uint32_t>{1, 9, 14}));
    EXPECT_EQ(layout.slotWidth, (std::vector<uint16_t>{8, 5, 8}));
    EXPECT_EQ(layout.rowLen, 22u);

    std::array<uint8_t, 22> row{};
    const int64_t a = 7, c = -3;
    for (int k = 0; k < 8; ++k) {
        row[1 + k]  = static_cast<uint8_t>(static_cast<uint64_t>(a) >> (8 * k));
        row[14 + k] = static_cast<uint8_t>(static_cast<uint64_t>(c) >> (8 * k));
    }
    const char* s = "abcde";
    for (int k = 0; k < 5; ++k) row[9 + k] = static_cast<uint8_t>(s[k]);

    EXPECT_EQ(rcv::readSlot(row, layout, 0).i, 7);
    EXPECT_EQ(rcv::readSlot(row, layout, 1).bytes, "abcde");
    EXPECT_EQ(rcv::readSlot(row, layout, 2).i, -3);
}

// ---- Adversarial review of batch C9 (#162): every FixedStr column needs its own width, at every position. ----

// The widths span is positional: a width given for a DIFFERENT slot (or a span that stops short) is not a width.
TEST(ViewRowCodecRegression, FixedStrWidthsShortOrMisplacedAreRefusedAtEveryPositionAdversarialRegression) {
    const std::array<PrimType, 4> types{PrimType::FixedStr, PrimType::Int32, PrimType::FixedStr, PrimType::Bool};
    // Span stops before the second FixedStr (idx 2): refused, naming column 2.
    {
        const std::array<uint16_t, 2> widths{4, 0};
        try {
            (void)rcv::buildRowLayout(types, widths);
            ADD_FAILURE() << "a FixedStr past the end of the widths span was given a guessed width";
        } catch (const std::invalid_argument& e) {
            EXPECT_NE(std::string(e.what()).find("column 2"), std::string::npos) << e.what();
        }
    }
    // Width on the wrong slot (the Int32's): FixedStr idx 0 has none.
    {
        const std::array<uint16_t, 4> widths{0, 4, 6, 0};
        try {
            (void)rcv::buildRowLayout(types, widths);
            ADD_FAILURE() << "a FixedStr at slot 0 borrowed another slot's width";
        } catch (const std::invalid_argument& e) {
            EXPECT_NE(std::string(e.what()).find("column 0"), std::string::npos) << e.what();
        }
    }
    // Only the first FixedStr has a width: the second still refuses.
    {
        const std::array<uint16_t, 4> widths{4, 0, 0, 0};
        EXPECT_THROW((void)rcv::buildRowLayout(types, widths), std::invalid_argument);
    }
}

// Bounds 1 and max (65535) at the first and the last slot; 9 columns so the bitmap is 2 bytes and slot 8's null bit
// lives in the second byte.  Every later column lands at its exact offset and a NULL slot reads absent.
TEST(ViewRowCodecRegression, FixedStrWidthOneAndMaxAcrossATwoByteBitmapDecodeAtExactOffsetsAdversarialRegression) {
    std::array<PrimType, 9> types{PrimType::FixedStr, PrimType::Int64, PrimType::Int32,  PrimType::UInt16,
                                  PrimType::Bool,     PrimType::UInt64, PrimType::Float64, PrimType::Int32,
                                  PrimType::FixedStr};
    std::array<uint16_t, 9> widths{1, 0, 0, 0, 0, 0, 0, 0, 0xFFFF};
    const rcv::RowLayout L = rcv::buildRowLayout(types, widths);
    EXPECT_EQ(L.bitmapBytes, 2u);
    EXPECT_EQ(L.slotOffset, (std::vector<uint32_t>{2, 3, 11, 15, 17, 18, 26, 34, 38}));
    EXPECT_EQ(L.slotWidth.front(), 1u);
    EXPECT_EQ(L.slotWidth.back(), 0xFFFFu);
    EXPECT_EQ(L.rowLen, 38u + 0xFFFFu);

    std::vector<uint8_t> row(L.rowLen, 0);
    row[2] = 'z';
    const int64_t i64 = -0x0102030405060708LL;
    for (int k = 0; k < 8; ++k) row[3 + k] = static_cast<uint8_t>(static_cast<uint64_t>(i64) >> (8 * k));
    row[34] = 0x2A;                 // Int32 slot 7 = 42
    row[38] = 'A'; row[38 + 0xFFFE] = 'Z';
    row[0] |= static_cast<uint8_t>(1u << 4);   // slot 4 (Bool) NULL
    EXPECT_EQ(rcv::readSlot(row, L, 0).bytes, "z");
    EXPECT_EQ(rcv::readSlot(row, L, 1).i, i64);
    EXPECT_EQ(rcv::readSlot(row, L, 7).i, 42);
    EXPECT_FALSE(rcv::readSlot(row, L, 4).present);
    const auto last = rcv::readSlot(row, L, 8);
    ASSERT_TRUE(last.present);
    ASSERT_EQ(last.bytes.size(), 0xFFFFu);
    EXPECT_EQ(last.bytes.front(), 'A');
    EXPECT_EQ(last.bytes.back(), 'Z');
    row[1] |= 1u;                   // slot 8's null bit is in the SECOND bitmap byte
    EXPECT_FALSE(rcv::readSlot(row, L, 8).present);
    EXPECT_TRUE(rcv::readSlot(row, L, 0).present);
}

// A width given for a non-FixedStr slot is ignored (the type's own size wins), so a caller that passes the view's
// whole fixedLen column (0 or not) never shifts a numeric slot.
TEST(ViewRowCodecRegression, AWidthOnANumericSlotNeverOverridesItsTypeSizeAdversarialRegression) {
    const std::array<PrimType, 3> types{PrimType::Int64, PrimType::FixedStr, PrimType::Int32};
    const std::array<uint16_t, 3> widths{3, 2, 9};
    const rcv::RowLayout L = rcv::buildRowLayout(types, widths);
    EXPECT_EQ(L.slotWidth, (std::vector<uint16_t>{8, 2, 4}));
    EXPECT_EQ(L.slotOffset, (std::vector<uint32_t>{1, 9, 11}));
    EXPECT_EQ(L.rowLen, 15u);
}

// C9 review: a row shorter than the layout (a broker on another layout or version) or a slot outside the layout is
// never read out of bounds: readSlot returns valid=false, present=false at every short length.
// RED: drop the bounds check at the top of readSlot (valid stays true; ASan reports the over-read).
TEST(ViewRowCodecRegression, AShortRowOrAnOutOfRangeSlotReadsNothingAdversarialRegression) {
    const std::array<PrimType, 3> types{PrimType::Int64, PrimType::FixedStr, PrimType::Int64};
    const std::array<uint16_t, 3> widths{0, 5, 0};
    const rcv::RowLayout L = rcv::buildRowLayout(types, widths);
    ASSERT_EQ(L.rowLen, 22u);
    std::vector<uint8_t> full(L.rowLen, 0);
    for (uint16_t slot = 0; slot < 3; ++slot) EXPECT_TRUE(rcv::readSlot(full, L, slot).valid) << slot;
    for (uint32_t len = 0; len < L.rowLen; ++len) {
        const std::vector<uint8_t> shortRow(full.begin(), full.begin() + len);
        for (uint16_t slot = 0; slot < 3; ++slot) {
            const rcv::FieldValue fv = rcv::readSlot(shortRow, L, slot);
            EXPECT_FALSE(fv.valid) << "len " << len << " slot " << slot;
            EXPECT_FALSE(fv.present) << "len " << len << " slot " << slot;
        }
    }
    for (const uint16_t slot : {uint16_t{3}, uint16_t{8}, uint16_t{65535}}) {
        const rcv::FieldValue fv = rcv::readSlot(full, L, slot);
        EXPECT_FALSE(fv.valid) << slot;
        EXPECT_FALSE(fv.present) << slot;
    }
}
