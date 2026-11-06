// Part 04 Task 3: control record types 16-19 (C10); Part 09 Task 6: type 20 (kVerdictPlan).
#include <gtest/gtest.h>

#include <cstring>
#include <optional>
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
    ASSERT_EQ(cr::buildConstraintSetChange(b, 2, 41, m), b.size());
    EXPECT_EQ(cr::recordType(b.data(), b.size()), cr::kConstraintSetChange);
    uint32_t setId = 0;
    int64_t catalogThrough = 0;
    uint16_t count = 0;
    std::vector<cr::ChangeMember> out(8);
    ASSERT_TRUE(cr::parseConstraintSetChange(b.data(), b.size(), setId, catalogThrough, out, count));
    EXPECT_EQ(setId, 2u);
    EXPECT_EQ(catalogThrough, 41);
    ASSERT_EQ(count, 2);
    EXPECT_EQ(out[1].viewObjectId, 7u);
    EXPECT_EQ(out[1].kind, 200);
    EXPECT_EQ(out[1].planFingerprint, 0xF00Du);
    EXPECT_FALSE(cr::parseConstraintSetChange(b.data(), b.size(), setId, catalogThrough, std::span<cr::ChangeMember>(out.data(), 1), count));
}

TEST(CheckedWritesControlRecord, VerdictPlanRoundTrips) {
    const std::vector<uint8_t> plan(300, 0xA1), out(120, 0xB2), src(90, 0xC3);
    const cr::VerdictPlanHeader h{7, 0xF00D, 2};
    std::vector<uint8_t> b(cr::verdictPlanBatchBytes(cr::verdictPlanPayloadBytes(plan.size(), out.size(), src.size())));
    ASSERT_EQ(cr::buildVerdictPlan(b, h, plan, out, src), b.size());
    EXPECT_EQ(cr::recordType(b.data(), b.size()), cr::kVerdictPlan);
    EXPECT_EQ(cr::kVerdictPlan, 20);
    cr::VerdictPlanHeader got{};
    std::span<const uint8_t> gotPlan, gotOut, gotSrc;
    ASSERT_TRUE(cr::parseVerdictPlan(b.data(), b.size(), got, gotPlan, gotOut, gotSrc));
    EXPECT_EQ(got.viewObjectId, 7u);
    EXPECT_EQ(got.planFingerprint, 0xF00Du);
    EXPECT_EQ(got.kind, 2);
    EXPECT_EQ(std::vector<uint8_t>(gotPlan.begin(), gotPlan.end()), plan);
    EXPECT_EQ(std::vector<uint8_t>(gotOut.begin(), gotOut.end()), out);
    EXPECT_EQ(std::vector<uint8_t>(gotSrc.begin(), gotSrc.end()), src);
    const auto c = cr::parseControl(b.data(), b.size());
    ASSERT_TRUE(c.has_value());
    EXPECT_FALSE(cr::parseMarkerHeader(*c).has_value()) << "a view of another type";
    for (std::size_t n = 0; n < b.size(); ++n) EXPECT_FALSE(cr::parseControl(b.data(), n).has_value()) << "truncated at " << n;
    auto flipped = b;
    flipped[flipped.size() - 5] ^= 0x01;
    EXPECT_FALSE(cr::parseVerdictPlan(flipped.data(), flipped.size(), got, gotPlan, gotOut, gotSrc));
}

TEST(CheckedWritesControlRecord, VerdictPlanOverTheCapIsRefused) {
    const cr::VerdictPlanHeader h{7, 0xF00D, 2};
    const std::size_t fixed = cr::verdictPlanPayloadBytes(0, 0, 0);
    const std::vector<uint8_t> big(cr::kMaxVerdictPlanBytes, 0x5A), none;
    std::vector<uint8_t> b(cr::verdictPlanBatchBytes(cr::kMaxVerdictPlanBytes + 1));
    EXPECT_EQ(cr::buildVerdictPlan(b, h, std::span<const uint8_t>(big.data(), cr::kMaxVerdictPlanBytes + 1 - fixed), none, none), 0u) << "65,537 bytes";
    ASSERT_EQ(cr::verdictPlanPayloadBytes(cr::kMaxVerdictPlanBytes - fixed, 0, 0), cr::kMaxVerdictPlanBytes);
    const std::size_t n = cr::buildVerdictPlan(b, h, std::span<const uint8_t>(big.data(), cr::kMaxVerdictPlanBytes - fixed), none, none);
    EXPECT_EQ(n, cr::verdictPlanBatchBytes(cr::kMaxVerdictPlanBytes)) << "65,536 bytes builds";
    cr::VerdictPlanHeader got{};
    std::span<const uint8_t> gotPlan, gotOut, gotSrc;
    EXPECT_TRUE(cr::parseVerdictPlan(b.data(), n, got, gotPlan, gotOut, gotSrc));
    EXPECT_EQ(gotPlan.size(), cr::kMaxVerdictPlanBytes - fixed);
    std::vector<uint8_t> small(10);
    EXPECT_EQ(cr::buildVerdictPlan(small, h, none, none, none), 0u) << "too small";
}

TEST(CheckedWritesControlRecordRegression, VersionOneChangeIsUnparseableRegression) {
    // Type 18 was never released at version 1 (#788): a v1 payload, CRC-valid or not, is not a change record.
    std::vector<uint8_t> b(cr::changeBatchBytes(0));
    ASSERT_EQ(cr::buildConstraintSetChange(b, 1, 5, {}), b.size());
    ASSERT_TRUE(cr::parseControl(b.data(), b.size()).has_value());
    const std::size_t versionAt = b.size() - 1 - cr::kChangeFixedBytes;
    ASSERT_EQ(b[versionAt], cr::kChangePayloadVersion);
    b[versionAt] = 1;
    const uint32_t crc = rune::protocol::crc32c(b.data() + rune::protocol::batch::kAttributesOffset, b.size() - rune::protocol::batch::kAttributesOffset);
    std::memcpy(b.data() + rune::protocol::batch::kCrcOffset, &crc, 4);
    EXPECT_FALSE(cr::parseControl(b.data(), b.size()).has_value());
    EXPECT_EQ(cr::recordType(b.data(), b.size()), cr::kUnparseable);
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
    auto type21 = good;
    for (std::size_t i = 61; i + 1 < type21.size(); ++i)
        if (type21[i] == 17 && type21[i + 1] == 0 && type21[i - 1] == 0x04) { type21[i] = 21; break; }
    EXPECT_EQ(cr::recordType(reseal(type21).data(), type21.size()), cr::kUnparseable);
    auto type19 = good;
    for (std::size_t i = 61; i + 1 < type19.size(); ++i)
        if (type19[i] == 17 && type19[i + 1] == 0 && type19[i - 1] == 0x04) { type19[i] = 19; break; }
    EXPECT_EQ(cr::recordType(reseal(type19).data(), type19.size()), cr::kTxnJudge);
}

// #804: parseControl validates (framing + CRC) once and hands out a view; the view overloads do no CRC of their own.
TEST(CheckedWritesControlRecordRegression, ParseControlOnceGivesTypeAndViewsRegression) {
    const auto marker1 = marker(100, 140, 3, {{5, 55, 0x00070001}, {12, 56, 0}});
    const std::vector<cr::ChangeMember> members{{0, 1, 0}, {7, 200, 0xF00D}};
    std::vector<uint8_t> change(cr::changeBatchBytes(2));
    ASSERT_EQ(cr::buildConstraintSetChange(change, 2, -1, members), change.size());

    // type 17 and 18: the type matches, and the view parse equals the raw parse
    const auto cm = cr::parseControl(marker1.data(), marker1.size());
    ASSERT_TRUE(cm.has_value());
    EXPECT_EQ(cm->type, cr::kVerdictMarker);
    const auto hv = cr::parseMarkerHeader(*cm), hr = cr::parseMarkerHeader(marker1.data(), marker1.size());
    ASSERT_TRUE(hv && hr);
    EXPECT_EQ(std::memcmp(&*hv, &*hr, sizeof(cr::MarkerHeader)), 0);
    const auto mv = cr::parseMarker(*cm), mr = cr::parseMarker(marker1.data(), marker1.size());
    ASSERT_TRUE(mv && mr);
    ASSERT_EQ(mv->header.count, 2u);
    for (uint32_t i = 0; i < 2; ++i) {
        EXPECT_EQ(mv->offsetAt(i), mr->offsetAt(i));
        EXPECT_EQ(mv->codeAt(i), mr->codeAt(i));
        EXPECT_EQ(mv->constraintIdAt(i), mr->constraintIdAt(i));
    }
    EXPECT_EQ(mv->offsetAt(1), 112);
    std::vector<std::tuple<int64_t, uint16_t, uint32_t>> viaView, viaRaw;
    EXPECT_TRUE(cr::forEachMarkerEntry(*cm, [&](int64_t o, uint16_t c, uint32_t id) { viaView.emplace_back(o, c, id); }));
    EXPECT_TRUE(cr::forEachMarkerEntry(marker1.data(), marker1.size(), [&](int64_t o, uint16_t c, uint32_t id) { viaRaw.emplace_back(o, c, id); }));
    EXPECT_EQ(viaView, viaRaw);
    EXPECT_EQ(viaView.size(), 2u);

    const auto cc = cr::parseControl(change.data(), change.size());
    ASSERT_TRUE(cc.has_value());
    EXPECT_EQ(cc->type, cr::kConstraintSetChange);
    uint32_t idV = 0, idR = 0;
    int64_t ctV = 0, ctR = 0;
    uint16_t nV = 0, nR = 0;
    std::vector<cr::ChangeMember> outV(8), outR(8);
    ASSERT_TRUE(cr::parseConstraintSetChange(*cc, idV, ctV, outV, nV));
    ASSERT_TRUE(cr::parseConstraintSetChange(change.data(), change.size(), idR, ctR, outR, nR));
    EXPECT_EQ(idV, idR);
    EXPECT_EQ(ctV, ctR);
    ASSERT_EQ(nV, nR);
    EXPECT_EQ(outV[1].planFingerprint, outR[1].planFingerprint);
    EXPECT_EQ(outV[1].viewObjectId, 7u);

    // a view of the wrong type is refused, with no CRC involved
    EXPECT_FALSE(cr::parseMarkerHeader(*cc).has_value());
    EXPECT_FALSE(cr::parseMarker(*cc).has_value());
    EXPECT_FALSE(cr::forEachMarkerEntry(*cc, [](int64_t, uint16_t, uint32_t) {}));
    EXPECT_FALSE(cr::parseConstraintSetChange(*cm, idV, ctV, outV, nV));

    // types 16 and 19 are control records too, and parse to their type
    for (const int16_t t : {cr::kReservation, cr::kTxnJudge}) {
        auto b = marker(0, 10, 1, {});
        for (std::size_t i = 61; i + 1 < b.size(); ++i)
            if (b[i] == 17 && b[i + 1] == 0 && b[i - 1] == 0x04) { b[i] = static_cast<uint8_t>(t); break; }
        const uint32_t crc = rune::protocol::crc32c(b.data() + 21, b.size() - 21);
        std::memcpy(b.data() + 17, &crc, 4);
        const auto c = cr::parseControl(b.data(), b.size());
        ASSERT_TRUE(c.has_value()) << t;
        EXPECT_EQ(c->type, t);
        EXPECT_FALSE(cr::parseMarkerHeader(*c).has_value()) << "a view of another type";
    }

    // type 21 is not a control record of this family
    auto type21 = marker(0, 10, 1, {});
    for (std::size_t i = 61; i + 1 < type21.size(); ++i)
        if (type21[i] == 17 && type21[i + 1] == 0 && type21[i - 1] == 0x04) { type21[i] = 21; break; }
    { const uint32_t crc = rune::protocol::crc32c(type21.data() + 21, type21.size() - 21); std::memcpy(type21.data() + 17, &crc, 4); }
    EXPECT_FALSE(cr::parseControl(type21.data(), type21.size()).has_value());

    // one flipped value byte: the CRC refuses it
    auto flipped = marker1;
    flipped[flipped.size() - 3] ^= 0x01;
    EXPECT_FALSE(cr::parseControl(flipped.data(), flipped.size()).has_value());

    // a transaction marker and a data-less buffer are not control records of this family
    const auto txn = rune::transaction::ControlMarker::build(5, 0, rune::transaction::ControlType::Commit, 1);
    EXPECT_FALSE(cr::parseControl(txn.data(), txn.size()).has_value());
    EXPECT_FALSE(cr::parseControl(nullptr, 0).has_value());

    // truncated at every offset, for both shapes
    for (const std::vector<uint8_t>* whole : {&marker1, static_cast<const std::vector<uint8_t>*>(&change)})
        for (std::size_t n = 0; n < whole->size(); ++n)
            EXPECT_FALSE(cr::parseControl(whole->data(), n).has_value()) << "truncated at " << n;
}

TEST(CheckedWritesControlRecord, ReservationRoundTrips) {
    std::vector<uint8_t> b(cr::reservationBatchBytes());
    ASSERT_EQ(cr::buildReservation(b, 8192), b.size());
    EXPECT_EQ(cr::recordType(b.data(), b.size()), cr::kReservation);
    EXPECT_EQ(cr::parseReservation(b.data(), b.size()), std::optional<uint64_t>(8192));
    int64_t producerId = 0;
    std::memcpy(&producerId, b.data() + 43, 8);
    EXPECT_EQ(producerId, -1);

    std::vector<uint8_t> shortOut(b.size() - 1);
    EXPECT_EQ(cr::buildReservation(shortOut, 8192), 0u);

    const auto m = marker(0, 10, 1, {});
    EXPECT_FALSE(cr::parseReservation(m.data(), m.size()).has_value());
}
