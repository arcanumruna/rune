// tests/client/ViewPlanAuthorTest.cpp — the client's MergeRule must mirror the
// broker's wire values byte-for-byte (append-only enum).
#include <gtest/gtest.h>

#include <rune/client/view/ViewPlanAuthor.h>

namespace rcv = rune::client::view;

TEST(ViewPlanAuthorRegression, LastNonNullIsAppendedAtWireValue8) {
    EXPECT_EQ(static_cast<uint8_t>(rcv::MergeRule::CountAll), 7u);
    EXPECT_EQ(static_cast<uint8_t>(rcv::MergeRule::LastNonNull), 8u);

    rcv::ViewPlan p;
    p.viewName = "v"; p.key = "k";
    p.sources.push_back(rcv::SourceSpec{5, "k"});
    p.output.push_back(rcv::AuthoredField{"k", rune::codec::PrimType::Int64, 0, "k", rcv::MergeRule::Replace});
    p.output.push_back(rcv::AuthoredField{"b", rune::codec::PrimType::Int32, 0, "bonus", rcv::MergeRule::LastNonNull});
    const auto blob = rcv::serializeAuthored(p);
    // Tail = [last field's rule u8][persistence u8][sizing: u32 + u32 + u64].
    ASSERT_GE(blob.size(), 18u);
    EXPECT_EQ(blob[blob.size() - 18], 8u) << "LastNonNull must serialize as rule byte 8";

    // F1: the client's own decoder (used by `rune catalog replay`) must round-trip
    // rule 8 and still reject one past it.
    const auto back = rcv::deserializeAuthored(blob);
    ASSERT_TRUE(back.has_value()) << "deserializeAuthored must accept rule byte 8";
    ASSERT_EQ(back->output.size(), 2u);
    EXPECT_EQ(back->output[1].rule, rcv::MergeRule::LastNonNull);
    auto bad = blob;
    bad[bad.size() - 18] = 9;
    EXPECT_FALSE(rcv::deserializeAuthored(bad).has_value())
        << "rule byte 9 (one past LastNonNull) must be rejected, never cast";
}
