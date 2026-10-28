// tests/detail/CoordRouteTest.cpp - #362: the coordinator route variant and its down-grace tracking.
#include <rune/client/detail/CoordRoute.h>

#include <gtest/gtest.h>

#include <cstdint>

using namespace rune::client::detail;

namespace {
// The logic Client.cpp had before #362: a 0 down-since stamp meant "up", so "first seen down at t=0" was lost.
bool oldDownTooLong(int64_t& downSince, bool connected, int64_t now, int64_t grace) {
    if (connected) {
        downSince = 0;
    } else if (downSince == 0) {
        downSince = now;
    }
    return downSince != 0 && now - downSince > grace;
}
}  // namespace

TEST(CoordRouteRegression, DownAtClockZeroStillCountsTheGraceFromZeroRegression) {
    CoordRemote r{3, "ch", 0, std::nullopt};
    EXPECT_FALSE(coordDownTooLong(r, false, 0, 5'000));   // first seen down at t=0
    EXPECT_FALSE(coordDownTooLong(r, false, 5'000, 5'000));
    EXPECT_TRUE(coordDownTooLong(r, false, 5'001, 5'000)) << "down since t=0, not restarted by a 0 sentinel";
    EXPECT_FALSE(coordDownTooLong(r, true, 5'002, 5'000));
    EXPECT_FALSE(r.downSinceNs.has_value());
    EXPECT_FALSE(coordDownTooLong(r, false, 6'000, 5'000));
    EXPECT_FALSE(coordDownTooLong(r, false, 11'000, 5'000));
    EXPECT_TRUE(coordDownTooLong(r, false, 11'001, 5'000)) << "re-armed from the new down edge";
}

TEST(CoordRouteRegression, SeedIsIndexZeroRegression) {
    EXPECT_EQ(coordIdxOf(CoordRoute{}), 0u);
    EXPECT_EQ(coordIdxOf(CoordRoute{CoordRemote{7, "c", 0, {}}}), 7u);
}

TEST(CoordRouteRegression, TheOldSentinelLostTheDownEdgeAtClockZeroRegression) {
    int64_t downSince = 0;
    EXPECT_FALSE(oldDownTooLong(downSince, false, 0, 5'000));
    EXPECT_FALSE(oldDownTooLong(downSince, false, 5'000, 5'000));
    EXPECT_FALSE(oldDownTooLong(downSince, false, 5'001, 5'000)) << "documents #362: the old logic restarts the grace";
}
