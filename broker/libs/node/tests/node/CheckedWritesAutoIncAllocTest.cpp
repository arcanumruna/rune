// CheckedWritesAutoIncAllocTest.cpp - checked writes Part 06 Task 7 (hard rule): the side window's record allocates
// nothing once reserved.  Lives in the alloc-regression binary: AllocWindow.cpp replaces the global operator new.
#include "support/AllocWindow.h"

#include <gtest/gtest.h>

#include <rune/node/AutoInc.h>

TEST(CheckedWritesAutoIncWindow, RecordAllocatesNothing) {
    rune::node::AssignedBaseWindow w;
    w.reserve(1024);
    rune::testing::AllocWindow window;
    for (int64_t i = 0; i < 100'000; ++i) w.record(i % 512, i, static_cast<uint64_t>(i) + 1);
    EXPECT_EQ(window.count(), 0u);
}
