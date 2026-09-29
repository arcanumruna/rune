// tests/SmokeTest.cpp
#include <gtest/gtest.h>
#include <rune/codec/Version.h>
TEST(Smoke, VersionIsSet) { EXPECT_GE(rune::codec::kVersion, 1); }
