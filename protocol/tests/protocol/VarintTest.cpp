#include <gtest/gtest.h>
#include <rune/protocol/Varint.h>
using namespace rune::protocol;

TEST(Varint, RoundTripLong) {
    for (int64_t v : {0LL, 1LL, -1LL, 127LL, -128LL, 300LL, -300LL,
                      9223372036854775807LL, -9223372036854775807LL - 1}) {
        uint8_t buf[10]; size_t n = writeVarlong(v, buf);
        size_t pos = 0; int64_t got = readVarlong(buf, pos);
        EXPECT_EQ(got, v);
        EXPECT_EQ(pos, n);
    }
}
