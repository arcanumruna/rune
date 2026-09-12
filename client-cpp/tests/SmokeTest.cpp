#include <gtest/gtest.h>
#include "rune/client/Version.h"

TEST(Smoke, VersionIsSet)
{
    EXPECT_EQ(std::string(rune::client::kVersion), "0.1.0-snapshot");
}

TEST(Smoke, VersionIsNotEmpty)
{
    EXPECT_FALSE(rune::client::kVersion.empty());
}
