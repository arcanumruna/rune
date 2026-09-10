#include <gtest/gtest.h>
#include <rune/protocol/Crc32c.h>
#include <string>

using rune::protocol::crc32c;

TEST(Crc32c, KnownVectors) {
    // CRC-32C ("Castagnoli") reference values.
    EXPECT_EQ(crc32c(nullptr, 0), 0x00000000u);
    const std::string a = "123456789";
    EXPECT_EQ(crc32c(reinterpret_cast<const uint8_t*>(a.data()), a.size()), 0xE3069283u);
}

TEST(Crc32c, IncrementalMatchesOneShot) {
    const std::string s = "the quick brown fox";
    const auto* p = reinterpret_cast<const uint8_t*>(s.data());
    uint32_t seed = crc32c(p, 9);
    uint32_t inc  = rune::protocol::crc32c(seed, p + 9, s.size() - 9);
    EXPECT_EQ(inc, crc32c(p, s.size()));
}
