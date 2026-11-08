// Part 11 Task 8: the transaction span fence (contract addition 13) and the fold that applies a transaction at its commit marker
// (P11-0, P11-9).
#include <gtest/gtest.h>

#include <rune/view/TxnSpanFence.h>

namespace {

using rune::view::TxnSpanFence;

TEST(CheckedWritesTxnSpanFence, AddFindClose) {
    TxnSpanFence f;
    EXPECT_EQ(f.find(7), nullptr);
    ASSERT_TRUE(f.add(7, 10));
    ASSERT_TRUE(f.add(9, 13));
    ASSERT_NE(f.find(7), nullptr);
    EXPECT_EQ(f.find(7)->first, 10);
    EXPECT_EQ(f.find(9)->first, 13);
    f.close(7);
    EXPECT_EQ(f.find(7), nullptr);
    EXPECT_EQ(f.find(9)->first, 13);
    EXPECT_EQ(f.count, 1);
    f.close(42);   // not open: nothing changes
    EXPECT_EQ(f.count, 1);
}

TEST(CheckedWritesTxnSpanFence, The65thSpanIsRefusedUntilOneCloses) {
    TxnSpanFence f;
    for (int64_t pid = 0; pid < 64; ++pid) ASSERT_TRUE(f.add(pid, 100 + pid));
    EXPECT_FALSE(f.add(64, 200));
    EXPECT_EQ(f.find(64), nullptr);
    f.close(5);
    EXPECT_TRUE(f.add(64, 200));
    EXPECT_EQ(f.find(64)->first, 200);
    for (int64_t pid = 0; pid < 64; ++pid)
        if (pid != 5) EXPECT_NE(f.find(pid), nullptr) << pid;
}

TEST(CheckedWritesTxnSpanFence, StraddlesOnlyAboveAnOpenFirst) {
    TxnSpanFence f;
    EXPECT_FALSE(f.straddles(1'000'000));
    ASSERT_TRUE(f.add(3, 10));
    EXPECT_FALSE(f.straddles(10));   // a checkpoint AT first holds nothing of the span: a replay from it meets every batch
    EXPECT_TRUE(f.straddles(11));
    f.close(3);
    EXPECT_FALSE(f.straddles(11));
}

TEST(CheckedWritesTxnSpanFence, FencedWhileAnApplicationIsInProgress) {
    TxnSpanFence f;
    EXPECT_FALSE(f.fenced());
    ASSERT_TRUE(f.add(3, 10));
    EXPECT_FALSE(f.fenced());   // an open span alone never holds reads (P11-9): its rows are simply not there yet
    f.applyingThrough = 12;
    EXPECT_TRUE(f.fenced());
    f.close(3);   // the application completed with the span
    EXPECT_FALSE(f.fenced());
    f.atomicThrough = 40;
    EXPECT_TRUE(f.fenced());
    f.clear();
    EXPECT_FALSE(f.fenced());
    EXPECT_EQ(f.count, 0);
}

} // namespace
