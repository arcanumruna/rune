// tests/client/LeasesRowDecodeTest.cpp - lease topics Task 14b: the generated <T>LeasesRow::decode against a row image encoded by
// hand per the broker's lease view layout (null bitmap, one fixed slot per field ever declared - dropped ones too - then
// lease_granted_offset and lease_updated_offset).  lease_evo.gen.h (rune_gen_claim_schemas) is version 2 of a lease schema with a
// field dropped between live ones.
#include "lease_evo.gen.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace {

using rune::gen::LeaseEvo;
using Row = LeaseEvo::LeasesRow;

/// Appends little-endian values; the layout is spelled out slot by slot, never derived from the generated constants.
struct RowImage {
    std::vector<uint8_t> b;
    explicit RowImage(std::size_t bitmapBytes) : b(bitmapBytes, 0) {}
    template <class V> void put(V v) { const auto at = b.size(); b.resize(at + sizeof v); std::memcpy(b.data() + at, &v, sizeof v); }
    void putStr(const char (&s)[5]) { b.insert(b.end(), s, s + 4); }
    void setNull(std::size_t slot) { b[slot >> 3] |= static_cast<uint8_t>(1u << (slot & 7)); }
};

/// 4 lease columns, a, gone (dropped), tag, flag, ratio, kind, w, granted, updated = 13 slots -> a 2-byte bitmap.
RowImage fullImage() {
    RowImage r(2);
    r.put<int64_t>(500);              // lease_version
    r.put<uint64_t>(60000);           // lease_ttl_ms
    r.put<uint64_t>(9);               // lease_holder
    r.put<uint64_t>(1234);            // lease_remaining_ms
    r.put<uint32_t>(0xAABBCCDDu);     // a
    r.put<int64_t>(0x1111222233334444);   // gone: the dropped slot, present in the row, never surfaced
    r.putStr("wxyz");                 // tag
    r.put<uint8_t>(1);                // flag
    r.put<double>(2.5);               // ratio
    r.put<int32_t>(2);                // kind
    r.put<uint16_t>(0xBEEF);          // w
    r.put<int64_t>(500);              // lease_granted_offset
    r.put<int64_t>(777);              // lease_updated_offset
    return r;
}

}  // namespace

TEST(LeasesRowDecodeRegression, EveryLiveMemberAndBothOffsetsPastADroppedSlot) {
    const RowImage img = fullImage();
    ASSERT_EQ(img.b.size(), 81u);
    EXPECT_EQ(Row::kRowLen, 81u);
    const auto r = Row::decode(img.b);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->lease_version, 500);
    EXPECT_EQ(r->lease_ttl_ms, 60000u);
    EXPECT_EQ(r->lease_holder, 9u);
    EXPECT_EQ(r->lease_remaining_ms, 1234u);
    EXPECT_EQ(r->a, 0xAABBCCDDu);
    EXPECT_EQ(std::string(r->tag.data(), 4), "wxyz");
    EXPECT_TRUE(r->flag);
    EXPECT_EQ(r->ratio, 2.5);
    EXPECT_EQ(r->kind, 2);
    EXPECT_EQ(r->w, 0xBEEF);
    EXPECT_EQ(r->lease_granted_offset, 500);
    EXPECT_EQ(r->lease_updated_offset, 777);
}

TEST(LeasesRowDecodeRegression, WrongLengthIsNullopt) {
    RowImage img = fullImage();
    img.b.pop_back();
    EXPECT_FALSE(Row::decode(img.b).has_value()) << "one byte short";
    img = fullImage();
    img.b.push_back(0);
    EXPECT_FALSE(Row::decode(img.b).has_value()) << "one byte long";
    EXPECT_FALSE(Row::decode({}).has_value());
}

TEST(LeasesRowDecodeRegression, NullLiveSlotReadsAsZeroAndLaterSlotsStayAligned) {
    RowImage img = fullImage();
    img.setNull(4);    // a
    img.setNull(11);   // lease_granted_offset
    const auto r = Row::decode(img.b);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->a, 0u);
    EXPECT_EQ(r->lease_granted_offset, 0);
    EXPECT_EQ(r->w, 0xBEEF) << "slots after a null one are not shifted";
    EXPECT_EQ(r->lease_updated_offset, 777);
}
