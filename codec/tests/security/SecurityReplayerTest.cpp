// Authentication spec §3: the verifying replayer — decode, gap check, apply under the frozen rules of
// the event's formatVer, fingerprint verified; any failure names the offset and stops the replay.
#include <rune/security/SecurityReplayer.h>
#include <rune/security/testing/SecurityGoldenScript.h>
#include <gtest/gtest.h>

#include <string>

namespace rs = rune::security;

TEST(SecurityReplayerRegression, ReplaysTheGoldenScriptVerifyingEveryFingerprint) {
    const auto events = rs::testing::goldenScriptV1();
    ASSERT_EQ(events.size(), 16u);
    rs::SecurityReplayer r;
    for (std::size_t i = 0; i < events.size(); ++i) {
        auto a = r.feed(static_cast<int64_t>(i), rs::encode(events[i]).value());
        ASSERT_TRUE(a.has_value()) << rs::describe(a.error());
        EXPECT_EQ(a->fingerprint, events[i].fingerprint) << "offset " << i;
    }
    EXPECT_EQ(r.nextOffset(), static_cast<int64_t>(events.size()));
}

TEST(SecurityReplayerRegression, AGapIsAnErrorNamingTheOffset) {
    const auto events = rs::testing::goldenScriptV1();
    rs::SecurityReplayer r;
    ASSERT_TRUE(r.feed(0, rs::encode(events[0]).value()));
    auto a = r.feed(2, rs::encode(events[1]).value());
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().kind, rs::ReplayError::Kind::Gap);
    EXPECT_EQ(a.error().offset, 2);
    EXPECT_EQ(r.nextOffset(), 1);
    EXPECT_NE(rs::describe(a.error()).find("offset 2: Gap"), std::string::npos) << rs::describe(a.error());
    auto again = r.feedDecoded(0, events[1]);   // a repeated offset is a gap too
    EXPECT_EQ(again.error().kind, rs::ReplayError::Kind::Gap);
}

TEST(SecurityReplayerRegression, UnknownFormatVersionStopsAtItsOffset) {
    const auto events = rs::testing::goldenScriptV1();
    rs::SecurityReplayer r;
    ASSERT_TRUE(r.feed(0, rs::encode(events[0]).value()));
    auto bytes = rs::encode(events[1]).value();
    bytes[0] = 3;   // 1 and 2 are the known formats
    auto a = r.feed(1, bytes);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().kind, rs::ReplayError::Kind::Decode);
    EXPECT_EQ(*a.error().decode, rs::DecodeError::UnknownFormatVer);
    EXPECT_EQ(a.error().offset, 1);
    EXPECT_EQ(r.nextOffset(), 1) << "nothing past the bad event is applied";
    EXPECT_EQ(r.state().appliedOffset(), 0) << "the state is the prefix";
    auto skip = r.feed(2, rs::encode(events[2]).value());
    EXPECT_EQ(skip.error().kind, rs::ReplayError::Kind::Gap) << "a later event is never applied past a refused one";
}

TEST(SecurityReplayerRegression, TamperedFingerprintStopsAtItsOffsetRegression) {
    auto events = rs::testing::goldenScriptV1();
    events[3].fingerprint ^= 1;
    rs::SecurityReplayer r;
    for (int64_t i = 0; i < 3; ++i) ASSERT_TRUE(r.feedDecoded(i, events[static_cast<std::size_t>(i)]));
    const uint64_t digestBefore = r.state().digest();
    auto a = r.feed(3, rs::encode(events[3]).value());
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().kind, rs::ReplayError::Kind::Apply);
    EXPECT_EQ(a.error().apply->code, rs::ApplyError::Code::FingerprintMismatch);
    EXPECT_EQ(a.error().offset, 3);
    EXPECT_EQ(r.state().digest(), digestBefore);
    EXPECT_NE(rs::describe(a.error()).find("expected=0x"), std::string::npos) << rs::describe(a.error());
}

TEST(SecurityReplayerRegression, ARuleRefusalNamesItsOffsetAndRefusal) {
    auto events = rs::testing::goldenScriptV1();
    rs::SecurityReplayer r;
    ASSERT_TRUE(r.feedDecoded(0, events[0]));
    auto a = r.feedDecoded(1, events[0]);   // a second bootstrap
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().apply->refusal, rs::Refusal::AlreadyBootstrapped);
    EXPECT_NE(rs::describe(a.error()).find("AlreadyBootstrapped"), std::string::npos);
}

TEST(SecurityReplayerRegression, ResumesFromACachedPrefix) {
    const auto events = rs::testing::goldenScriptV1();
    rs::SecurityReplayer first;
    for (int64_t i = 0; i < 5; ++i) ASSERT_TRUE(first.feedDecoded(i, events[static_cast<std::size_t>(i)]));
    rs::SecurityReplayer resumed(first.state());
    EXPECT_EQ(resumed.nextOffset(), 5);
    EXPECT_EQ(resumed.feedDecoded(4, events[4]).error().kind, rs::ReplayError::Kind::Gap);
    EXPECT_EQ(resumed.feedDecoded(6, events[6]).error().kind, rs::ReplayError::Kind::Gap);
    for (int64_t i = 5; i < 16; ++i) ASSERT_TRUE(resumed.feedDecoded(i, events[static_cast<std::size_t>(i)])) << i;
    rs::SecurityReplayer whole;
    for (int64_t i = 0; i < 16; ++i) ASSERT_TRUE(whole.feedDecoded(i, events[static_cast<std::size_t>(i)]));
    EXPECT_EQ(resumed.state().digest(), whole.state().digest());
}
