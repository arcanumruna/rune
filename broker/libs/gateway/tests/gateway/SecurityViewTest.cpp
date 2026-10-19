// SecurityViewTest — the control thread's mirror of __security (authentication plan D29): it applies the admin
// applier's event bytes in offset order with the same library, reaches the same digest, and halts (fail closed) on a
// gap or an apply error.
#include <rune/gateway/SecurityView.h>

#include <rune/security/SecurityReplayer.h>
#include <rune/security/testing/SecurityGoldenScript.h>

#include <gtest/gtest.h>

#include <vector>

namespace sec = rune::security;
using rune::gateway::SecurityView;

namespace {
std::vector<std::vector<uint8_t>> goldenBytes() {
    std::vector<std::vector<uint8_t>> out;
    for (const auto& e : sec::testing::goldenScriptV1()) {
        auto b = sec::encode(e);
        EXPECT_TRUE(b.has_value());
        out.push_back(std::move(*b));
    }
    return out;
}
}  // namespace

TEST(SecurityViewRegression, MirrorsEveryEventInOffsetOrderToTheSameDigest) {
    const auto bytes = goldenBytes();
    sec::SecurityReplayer lib;
    SecurityView view{sec::SecurityState{}};
    EXPECT_EQ(view.appliedOffset(), -1);
    for (std::size_t i = 0; i < bytes.size(); ++i) {
        ASSERT_TRUE(lib.feed(static_cast<int64_t>(i), bytes[i]).has_value());
        auto a = view.onApplied(static_cast<int64_t>(i), bytes[i]);
        ASSERT_TRUE(a.has_value()) << "offset " << i;
        EXPECT_EQ(view.appliedOffset(), static_cast<int64_t>(i));
    }
    EXPECT_EQ(view.state().digest(), lib.state().digest());
    EXPECT_FALSE(view.halted());
    EXPECT_EQ(view.stateName(), "Mirroring");
    EXPECT_TRUE(view.state().hasSecurityAdmin() == lib.state().hasSecurityAdmin());
}

TEST(SecurityViewRegression, ResumesFromTheBootedStateHandedOverByTheStartupThread) {
    const auto bytes = goldenBytes();
    sec::SecurityReplayer boot;
    for (std::size_t i = 0; i < 6; ++i) ASSERT_TRUE(boot.feed(static_cast<int64_t>(i), bytes[i]).has_value());
    SecurityView view{boot.state()};   // a COPY: the booted applier keeps its own
    EXPECT_EQ(view.appliedOffset(), 5);
    ASSERT_TRUE(view.onApplied(6, bytes[6]).has_value());
    EXPECT_EQ(view.appliedOffset(), 6);
}

TEST(SecurityViewRegression, AGapHaltsTheMirrorAndAppliesNothingMore) {
    const auto bytes = goldenBytes();
    SecurityView view{sec::SecurityState{}};
    ASSERT_TRUE(view.onApplied(0, bytes[0]).has_value());
    auto gap = view.onApplied(2, bytes[2]);   // offset 1 never arrived
    ASSERT_FALSE(gap.has_value());
    EXPECT_EQ(gap.error().code, sec::ApplyError::Code::OutOfOrder);
    EXPECT_TRUE(view.halted());
    EXPECT_EQ(view.appliedOffset(), 0);
    // Even the right next event is refused once halted: the first cause stays.
    auto after = view.onApplied(1, bytes[1]);
    ASSERT_FALSE(after.has_value());
    EXPECT_EQ(after.error().code, sec::ApplyError::Code::OutOfOrder);
    EXPECT_EQ(view.appliedOffset(), 0);
}

TEST(SecurityViewRegression, ARepeatedOffsetHaltsTheMirror) {
    const auto bytes = goldenBytes();
    SecurityView view{sec::SecurityState{}};
    ASSERT_TRUE(view.onApplied(0, bytes[0]).has_value());
    ASSERT_FALSE(view.onApplied(0, bytes[0]).has_value());
    EXPECT_TRUE(view.halted());
}

TEST(SecurityViewRegression, AnUndecodableOrTamperedRecordHaltsTheMirror) {
    const auto bytes = goldenBytes();
    {
        SecurityView view{sec::SecurityState{}};
        auto r = view.onApplied(0, std::vector<uint8_t>{1, 0, 2});   // formatVer 1, then truncated
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error().code, sec::ApplyError::Code::BadOps);
        EXPECT_TRUE(view.halted());
    }
    {
        auto events = sec::testing::goldenScriptV1();
        events[0].fingerprint ^= 1;
        auto b = sec::encode(events[0]);
        SecurityView view{sec::SecurityState{}};
        auto r = view.onApplied(0, *b);
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error().code, sec::ApplyError::Code::FingerprintMismatch);
        EXPECT_EQ(view.appliedOffset(), -1) << "nothing of the refused event is applied";
    }
}

TEST(SecurityViewRegression, AnAdminReportedHaltStopsTheMirrorToo) {
    const auto bytes = goldenBytes();
    SecurityView view{sec::SecurityState{}};
    ASSERT_TRUE(view.onApplied(0, bytes[0]).has_value());
    sec::ApplyError e;
    e.offset = 1;
    e.code = sec::ApplyError::Code::FingerprintMismatch;
    view.onHalted(e);
    EXPECT_TRUE(view.halted());
    ASSERT_NE(view.haltError(), nullptr);
    EXPECT_EQ(view.haltError()->offset, 1);
    EXPECT_FALSE(view.onApplied(1, bytes[1]).has_value());
    view.onHalted(sec::ApplyError{});   // idempotent: the first cause stays
    EXPECT_EQ(view.haltError()->offset, 1);
}
