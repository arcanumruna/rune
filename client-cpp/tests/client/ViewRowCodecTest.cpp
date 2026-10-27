// tests/client/ViewRowCodecTest.cpp — buildRowLayout never guesses a FixedStr width (#162).
#include <gtest/gtest.h>

#include <rune/client/view/ViewRowCodec.h>
#include <rune/codec/PrimType.h>

#include <array>
#include <cstdint>
#include <stdexcept>
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
