// tests/lease/LeaseScanHashTest.cpp - the client's copy of the broker's ScanLeases order hash is the broker's function (spec 8.3).
#include <rune/protocol/LeaseScanHash.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

// The same three golden values as the broker's ScanLeasesRegression.ScanHashGoldenRegression (broker/libs/node/tests/node/LeaseScanTest.cpp).
// Mutation: change the seed or the function in protocol/LeaseScanHash.h -> red (and the broker's golden test too).
TEST(LeaseScanHashRegression, LeaseScanHashGoldenRegression) {
    const std::vector<uint8_t> a = {1, 0, 0, 0, 0, 0, 0, 0};
    const std::vector<uint8_t> b = {'a', 'b', 'c'};
    const std::vector<uint8_t> c(16, 0xAB);
    EXPECT_EQ(rune::protocol::scanHash(a, 6), 17450080259330277671ULL) << "golden 1";
    EXPECT_EQ(rune::protocol::scanHash(b, 7), 10202830245010477438ULL) << "golden 2";
    EXPECT_EQ(rune::protocol::scanHash(c, 1000), 16726008355911138569ULL) << "golden 3";
    EXPECT_NE(rune::protocol::scanHash(a, 6), rune::protocol::scanHash(a, 7)) << "the topic id is the seed";
}
