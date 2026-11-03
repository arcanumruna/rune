// Part 04 Task 3: control record types 16-19 (C10).
#include <gtest/gtest.h>

#include <vector>

#include <rune/storage/ControlRecordLayout.h>
#include <rune/transaction/ControlMarker.h>

namespace cr = rune::storage::control_record;

namespace {
std::vector<uint8_t> marker(int64_t from, int64_t through, uint32_t setId, const std::vector<cr::MarkerEntry>& e) {
    std::vector<uint8_t> b(cr::markerBatchBytes(static_cast<uint32_t>(e.size())));
    const cr::MarkerHeader h{from, through, setId, static_cast<uint32_t>(e.size())};
    EXPECT_EQ(cr::buildVerdictMarker(b, h, e), b.size());
    return b;
}
}  // namespace

TEST(CheckedWritesControlRecord, MarkerRoundTrips) {
    const auto b = marker(100, 140, 3, {{5, 55, 0x00070001}, {12, 56, 0}});
    EXPECT_EQ(cr::recordType(b.data(), b.size()), cr::kVerdictMarker);
    const auto h = cr::parseMarkerHeader(b.data(), b.size());
    ASSERT_TRUE(h.has_value());
    EXPECT_EQ(h->fromOffset, 100);
    EXPECT_EQ(h->finalThrough, 140);
    EXPECT_EQ(h->constraintSetId, 3u);
    EXPECT_EQ(h->count, 2u);
    std::vector<std::tuple<int64_t, uint16_t, uint32_t>> got;
    EXPECT_TRUE(cr::forEachMarkerEntry(b.data(), b.size(), [&](int64_t o, uint16_t c, uint32_t id) { got.emplace_back(o, c, id); }));
    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[0], std::make_tuple(int64_t{105}, uint16_t{55}, uint32_t{0x00070001}));
    EXPECT_EQ(got[1], std::make_tuple(int64_t{112}, uint16_t{56}, uint32_t{0}));
}

TEST(CheckedWritesControlRecord, ChangeRoundTrips) {
    const std::vector<cr::ChangeMember> m{{0, 1, 0}, {7, 200, 0xF00D}};
    std::vector<uint8_t> b(cr::changeBatchBytes(2));
    ASSERT_EQ(cr::buildConstraintSetChange(b, 2, m), b.size());
    EXPECT_EQ(cr::recordType(b.data(), b.size()), cr::kConstraintSetChange);
    uint32_t setId = 0;
    uint16_t count = 0;
    std::vector<cr::ChangeMember> out(8);
    ASSERT_TRUE(cr::parseConstraintSetChange(b.data(), b.size(), setId, out, count));
    EXPECT_EQ(setId, 2u);
    ASSERT_EQ(count, 2);
    EXPECT_EQ(out[1].viewObjectId, 7u);
    EXPECT_EQ(out[1].kind, 200);
    EXPECT_EQ(out[1].planFingerprint, 0xF00Du);
    EXPECT_FALSE(cr::parseConstraintSetChange(b.data(), b.size(), setId, std::span<cr::ChangeMember>(out.data(), 1), count));
}

TEST(CheckedWritesControlRecord, MaxMarkerFitsAndOneMoreIsRefused) {
    std::vector<cr::MarkerEntry> e(cr::kMaxMarkerEntries);
    for (uint32_t i = 0; i < e.size(); ++i) e[i] = {i, 55, 1};
    const auto b = marker(0, 5000, 1, e);
    EXPECT_EQ(cr::recordType(b.data(), b.size()), cr::kVerdictMarker);
    e.push_back({4096, 55, 1});
    std::vector<uint8_t> big(cr::markerBatchBytes(4097) + 64);
    EXPECT_EQ(cr::buildVerdictMarker(big, {0, 5000, 1, 4097}, e), 0u);
}

TEST(CheckedWritesControlRecord, BuilderRefusesAMismatchOrASmallBuffer) {
    std::vector<uint8_t> b(cr::markerBatchBytes(1));
    EXPECT_EQ(cr::buildVerdictMarker(b, {0, 1, 1, 2}, std::vector<cr::MarkerEntry>{{0, 55, 1}}), 0u);
    std::vector<uint8_t> small(10);
    EXPECT_EQ(cr::buildVerdictMarker(small, {0, 1, 1, 1}, std::vector<cr::MarkerEntry>{{0, 55, 1}}), 0u);
}

TEST(CheckedWritesControlRecord, RecordTypeRefusesLookalikes) {
    const auto good = marker(0, 10, 1, {});
    ASSERT_EQ(cr::recordType(good.data(), good.size()), cr::kVerdictMarker);

    const auto txn = rune::transaction::ControlMarker::build(5, 0, rune::transaction::ControlType::Commit, 1);
    EXPECT_EQ(cr::recordType(txn.data(), txn.size()), cr::kUnparseable);

    auto reseal = [](std::vector<uint8_t> b) {
        const uint32_t crc = rune::protocol::crc32c(b.data() + 21, b.size() - 21);
        std::memcpy(b.data() + 17, &crc, 4);
        return b;
    };
    auto withAttrs = good; withAttrs[21] = 0x30;                       // transactional bit set
    EXPECT_EQ(cr::recordType(reseal(withAttrs).data(), good.size()), cr::kUnparseable);
    auto withPid = good; withPid[43] = 0; for (int i = 1; i < 8; ++i) withPid[43 + i] = 0;   // producerId 0
    EXPECT_EQ(cr::recordType(reseal(withPid).data(), good.size()), cr::kUnparseable);
    auto flipped = good; flipped[30] ^= 0x01;                            // CRC no longer matches
    EXPECT_EQ(cr::recordType(flipped.data(), flipped.size()), cr::kUnparseable);
    auto type20 = good;
    for (std::size_t i = 61; i + 1 < type20.size(); ++i)
        if (type20[i] == 17 && type20[i + 1] == 0 && type20[i - 1] == 0x04) { type20[i] = 20; break; }
    EXPECT_EQ(cr::recordType(reseal(type20).data(), type20.size()), cr::kUnparseable);
    auto type19 = good;
    for (std::size_t i = 61; i + 1 < type19.size(); ++i)
        if (type19[i] == 17 && type19[i + 1] == 0 && type19[i - 1] == 0x04) { type19[i] = 19; break; }
    EXPECT_EQ(cr::recordType(reseal(type19).data(), type19.size()), cr::kTxnJudge);
}
