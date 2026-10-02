#include <gtest/gtest.h>

#include "rune/replication/DiagFlags.h"

using rune::replication::diagFlagFromEnv;

// A default deployment (env unset) must not run the per-fetch FollowerSync
// trace: it formats an info line on every follower fetch emission.
TEST(DiagFlagsRegression, FollowerSyncTraceIsOffByDefaultRegression) {
    EXPECT_FALSE(diagFlagFromEnv(nullptr)) << "unset = off";
    EXPECT_FALSE(diagFlagFromEnv("")) << "empty = off";
    EXPECT_FALSE(diagFlagFromEnv("0")) << "\"0\" = off";
    EXPECT_TRUE(diagFlagFromEnv("1"));
    EXPECT_TRUE(diagFlagFromEnv("yes"));
    EXPECT_TRUE(diagFlagFromEnv("01"));
}
