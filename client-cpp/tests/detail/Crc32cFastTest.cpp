// tests/detail/Crc32cFastTest.cpp — backlog B3: the 3-way interleaved CRC-32C
// must equal rune::protocol::crc32c (and the software table reference) for
// every length around the block boundaries and for random lengths/alignments.
#include <rune/client/detail/Crc32cFast.h>
#include <rune/protocol/Crc32c.h>

#include <gtest/gtest.h>

#include <random>
#include <vector>

using rune::client::detail::crc32cFast;
namespace fast = rune::client::detail::crc32c_fast;

namespace {
uint32_t reference(const uint8_t* p, std::size_t n) {
    return ~rune::protocol::detail::crc32c_sw(~0u, p, n);
}
}  // namespace

TEST(Crc32cFast, KnownVector) {
    const char* s = "123456789";
    EXPECT_EQ(crc32cFast(reinterpret_cast<const uint8_t*>(s), 9), 0xE3069283u);  // CRC-32C check value
    EXPECT_EQ(crc32cFast(nullptr, 0), rune::protocol::crc32c(nullptr, 0));
}

TEST(Crc32cFast, EveryLengthAroundTheBlockBoundariesMatches) {
    std::mt19937_64 rng(3);
    std::vector<uint8_t> buf(3 * fast::kBigBlock * 2 + 64);
    for (auto& b : buf) b = static_cast<uint8_t>(rng());
    std::vector<std::size_t> lens;
    for (std::size_t base : {std::size_t{0}, 3 * fast::kSmallBlock, 3 * fast::kBigBlock, 6 * fast::kBigBlock}) {
        for (std::size_t d = 0; d < 80; ++d) {
            if (base + d < buf.size() - 16) lens.push_back(base + d);
            if (base >= d + 1) lens.push_back(base - d - 1);
        }
    }
    for (std::size_t len : lens) {
        for (std::size_t off = 0; off < 9; ++off) {
            ASSERT_EQ(crc32cFast(buf.data() + off, len), reference(buf.data() + off, len))
                << "len=" << len << " off=" << off;
        }
    }
}

TEST(Crc32cFast, RandomLengthsAndAlignmentsMatch) {
    std::mt19937_64 rng(11);
    std::vector<uint8_t> buf(256 * 1024 + 16);
    for (auto& b : buf) b = static_cast<uint8_t>(rng());
    for (int i = 0; i < 2000; ++i) {
        const std::size_t off = rng() % 16;
        const std::size_t len = rng() % (buf.size() - off);
        ASSERT_EQ(crc32cFast(buf.data() + off, len), rune::protocol::crc32c(buf.data() + off, len))
            << "len=" << len << " off=" << off;
    }
}
