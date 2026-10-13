// Catalog history (spec §6): the verifying replayer — every event is decoded, checked to
// follow the previous offset with no gap, applied under its formatVer's frozen rules, and
// its fingerprint verified.  Any failure stops the replay with an error naming the offset.
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace rc = rune::catalog;

TEST(CatalogReplayerRegression, ReplaysTheGoldenScriptAndVerifiesEveryFingerprint) {
    const auto events = rc::testing::goldenScriptV1();
    rc::CatalogReplayer r;
    for (std::size_t i = 0; i < events.size(); ++i) {
        auto a = r.feed(static_cast<int64_t>(i), rc::encode(events[i]).value());
        ASSERT_TRUE(a.has_value()) << rc::describe(a.error());
        EXPECT_EQ(a->fingerprint, events[i].fingerprint) << "offset " << i;
    }
    EXPECT_EQ(r.nextOffset(), static_cast<int64_t>(events.size()));
}

TEST(CatalogReplayerRegression, AGapIsAnErrorNamingTheOffset) {
    const auto events = rc::testing::goldenScriptV1();
    rc::CatalogReplayer r;
    ASSERT_TRUE(r.feed(0, rc::encode(events[0]).value()));
    auto a = r.feed(2, rc::encode(events[1]).value());
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ReplayError::Code::Gap);
    EXPECT_EQ(a.error().offset, 2);
    EXPECT_EQ(r.nextOffset(), 1);
    EXPECT_NE(rc::describe(a.error()).find("offset 2: Gap"), std::string::npos) << rc::describe(a.error());
}

TEST(CatalogReplayerRegression, ARepeatedOffsetIsAGapToo) {
    const auto events = rc::testing::goldenScriptV1();
    rc::CatalogReplayer r;
    ASSERT_TRUE(r.feed(0, rc::encode(events[0]).value()));
    auto a = r.feedDecoded(0, events[1]);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ReplayError::Code::Gap);
    EXPECT_EQ(r.nextOffset(), 1);
}

TEST(CatalogReplayerRegression, UnknownFormatVersionStopsAtItsOffsetRegression) {
    const auto events = rc::testing::goldenScriptV1();
    rc::CatalogReplayer r;
    ASSERT_TRUE(r.feed(0, rc::encode(events[0]).value()));
    auto bytes = rc::encode(events[1]).value();
    bytes[0] = 9;
    auto a = r.feed(1, bytes);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ReplayError::Code::Decode);
    EXPECT_EQ(*a.error().decode, rc::DecodeError::UnknownFormatVer);
    EXPECT_EQ(a.error().offset, 1);
    EXPECT_EQ(r.nextOffset(), 1) << "nothing past the bad event is applied";
    EXPECT_NE(rc::describe(a.error()).find("offset 1"), std::string::npos);
}

TEST(CatalogReplayerRegression, AnEventPastTheBadOneIsRefusedNeverSkippedTo) {
    // "Never continue silently": after a refused event the replayer still waits for THAT
    // offset — feeding the one after it is a Gap, not a skip.
    const auto events = rc::testing::goldenScriptV1();
    rc::CatalogReplayer r;
    ASSERT_TRUE(r.feed(0, rc::encode(events[0]).value()));
    auto bad = rc::encode(events[1]).value();
    bad.pop_back();   // truncated
    auto a = r.feed(1, bad);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(*a.error().decode, rc::DecodeError::Truncated);
    auto b = r.feed(2, rc::encode(events[2]).value());
    ASSERT_FALSE(b.has_value());
    EXPECT_EQ(b.error().code, rc::ReplayError::Code::Gap);
    ASSERT_TRUE(r.feed(1, rc::encode(events[1]).value())) << "the good bytes for the waited-for offset apply";
}

TEST(CatalogReplayerRegression, TamperedFingerprintIsAnExplicitErrorNamingTheOffset) {
    auto events = rc::testing::goldenScriptV1();
    events[3].fingerprint ^= 0x10;
    rc::CatalogReplayer r;
    for (int64_t i = 0; i < 3; ++i) ASSERT_TRUE(r.feedDecoded(i, events[static_cast<std::size_t>(i)]));
    const uint64_t before = r.state().digest();
    auto a = r.feedDecoded(3, events[3]);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ReplayError::Code::Apply);
    EXPECT_EQ(a.error().apply->code, rc::ApplyError::Code::FingerprintMismatch);
    EXPECT_EQ(a.error().offset, 3);
    EXPECT_EQ(r.state().digest(), before) << "a refused event changes nothing";
    const std::string d = rc::describe(a.error());
    EXPECT_NE(d.find("offset 3: Apply FingerprintMismatch expected=0x"), std::string::npos) << d;
    EXPECT_NE(d.find(" actual=0x"), std::string::npos) << d;
}

TEST(CatalogReplayerRegression, TamperedFingerprintInTheBytesStopsTheByteReplayToo) {
    auto events = rc::testing::goldenScriptV1();
    rc::CatalogReplayer r;
    for (std::size_t i = 0; i < 5; ++i) ASSERT_TRUE(r.feed(static_cast<int64_t>(i), rc::encode(events[i]).value()));
    events[5].fingerprint += 1;
    auto a = r.feed(5, rc::encode(events[5]).value());
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().apply->code, rc::ApplyError::Code::FingerprintMismatch);
    EXPECT_EQ(a.error().apply->expected, events[5].fingerprint);
    EXPECT_EQ(a.error().offset, 5);
}

TEST(CatalogReplayerRegression, APreconditionFailureIsAnApplyErrorNamingTheOffset) {
    const auto events = rc::testing::goldenScriptV1();
    rc::CatalogReplayer r;
    ASSERT_TRUE(r.feedDecoded(0, events[0]));
    auto a = r.feedDecoded(1, events[0]);   // CreateTopic 'players' again: name taken
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ReplayError::Code::Apply);
    EXPECT_EQ(a.error().apply->code, rc::ApplyError::Code::Precondition);
    EXPECT_EQ(a.error().offset, 1);
    EXPECT_NE(rc::describe(a.error()).find("offset 1: Apply Precondition"), std::string::npos) << rc::describe(a.error());
}

TEST(CatalogReplayerRegression, ResumingFromACachedPrefixEqualsAFullReplay) {
    const auto events = rc::testing::goldenScriptV1();
    rc::CatalogReplayer full;
    for (std::size_t i = 0; i < events.size(); ++i) ASSERT_TRUE(full.feedDecoded(static_cast<int64_t>(i), events[i]));
    rc::CatalogReplayer head;
    const std::size_t mid = events.size() / 2;
    for (std::size_t i = 0; i < mid; ++i) ASSERT_TRUE(head.feedDecoded(static_cast<int64_t>(i), events[i]));
    rc::CatalogReplayer tail(head.state());
    EXPECT_EQ(tail.nextOffset(), static_cast<int64_t>(mid));
    for (std::size_t i = mid; i < events.size(); ++i) ASSERT_TRUE(tail.feedDecoded(static_cast<int64_t>(i), events[i]));
    EXPECT_EQ(tail.state().digest(), full.state().digest());
}

// ── the golden script itself: comprehensive enough to freeze formatVer 1 ─────────
TEST(CatalogReplayerRegression, TheGoldenScriptCoversEveryKindTwiceAndBothReplayedFromLayouts) {
    const auto events = rc::testing::goldenScriptV1();
    std::map<rc::EventKind, int> perKind;
    int withReplay = 0, withoutReplay = 0, authenticated = 0;
    for (const auto& e : events) {
        ++perKind[e.kind];
        (e.replayedFrom ? withReplay : withoutReplay) += 1;
        if (!e.principal.authenticated.empty()) ++authenticated;
    }
    for (uint16_t k = 1; k <= 9; ++k)
        EXPECT_GE(perKind[static_cast<rc::EventKind>(k)], 2) << rc::kindName(static_cast<rc::EventKind>(k));
    EXPECT_GE(withReplay, 1) << "the replayedFrom-present layout is pinned";
    EXPECT_GE(withoutReplay, 1) << "the replayedFrom-absent layout is pinned";
    EXPECT_GE(authenticated, 1);
}

TEST(CatalogReplayerRegression, TheGoldenScriptReusesATopicNameAndAViewName) {
    const auto events = rc::testing::goldenScriptV1();
    std::map<std::string, std::vector<uint64_t>> topicCreates, viewCreates;
    for (const auto& e : events) {
        if (e.kind == rc::EventKind::CreateTopic) topicCreates[e.objectName].push_back(e.objectId);
        if (e.kind == rc::EventKind::CreateView) viewCreates[e.objectName].push_back(e.objectId);
    }
    ASSERT_EQ(topicCreates["players"].size(), 2u);
    EXPECT_NE(topicCreates["players"][0], topicCreates["players"][1]);
    ASSERT_EQ(topicCreates["presence"].size(), 2u);
    EXPECT_NE(topicCreates["presence"][0], topicCreates["presence"][1]);
    ASSERT_EQ(viewCreates["ev_view"].size(), 2u);
    EXPECT_NE(viewCreates["ev_view"][0], viewCreates["ev_view"][1]);
}

TEST(CatalogReplayerRegression, TheGoldenScriptIsDeterministic) {
    const auto a = rc::testing::goldenScriptV1();
    const auto b = rc::testing::goldenScriptV1();
    ASSERT_EQ(a.size(), b.size());
    for (std::size_t i = 0; i < a.size(); ++i) EXPECT_EQ(a[i], b[i]) << "offset " << i;
}
