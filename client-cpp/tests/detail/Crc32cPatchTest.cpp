// tests/detail/Crc32cPatchTest.cpp — backlog B2: the incremental CRC-32C patch
// must be BIT-EXACT with a full recompute (randomized property tests: random
// regions of 0..64 KiB, every patch offset of small regions, random patch
// widths, the RecordBatch identity fields at their real offsets).
#include <rune/client/detail/Crc32cPatch.h>
#include <rune/protocol/Crc32c.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <vector>

using rune::client::detail::crc32cPatch;
using rune::protocol::crc32c;

namespace {

// Patches buf[off, off+n) with random bytes and checks the incremental CRC
// against a full recompute.  Returns false on mismatch.
bool patchMatches(std::vector<uint8_t>& buf, std::size_t off, std::size_t n, std::mt19937_64& rng) {
    const uint32_t before = crc32c(buf.data(), buf.size());
    std::vector<uint8_t> old(buf.begin() + static_cast<std::ptrdiff_t>(off),
                             buf.begin() + static_cast<std::ptrdiff_t>(off + n));
    for (std::size_t i = 0; i < n; ++i) buf[off + i] = static_cast<uint8_t>(rng());
    const uint32_t patched = crc32cPatch(before, buf.size(), off, old.data(), buf.data() + off, n);
    return patched == crc32c(buf.data(), buf.size());
}

}  // namespace

TEST(Crc32cPatch, EveryOffsetAndWidthOfSmallRegionsIsBitExact) {
    std::mt19937_64 rng(0xC0FFEE);
    for (std::size_t len = 1; len <= 96; ++len) {
        std::vector<uint8_t> buf(len);
        for (auto& b : buf) b = static_cast<uint8_t>(rng());
        for (std::size_t off = 0; off < len; ++off) {
            for (std::size_t n = 0; off + n <= len && n <= 20; ++n) {
                ASSERT_TRUE(patchMatches(buf, off, n, rng)) << "len=" << len << " off=" << off << " n=" << n;
            }
        }
    }
}

TEST(Crc32cPatch, RandomRegionsUpTo64KiBAreBitExact) {
    std::mt19937_64 rng(42);
    for (int iter = 0; iter < 400; ++iter) {
        const std::size_t len = 1 + rng() % (64 * 1024);
        std::vector<uint8_t> buf(len);
        for (auto& b : buf) b = static_cast<uint8_t>(rng());
        const std::size_t n   = rng() % std::min<std::size_t>(len + 1, 64);
        const std::size_t off = (len - n == 0) ? 0 : rng() % (len - n + 1);
        ASSERT_TRUE(patchMatches(buf, off, n, rng)) << "len=" << len << " off=" << off << " n=" << n;
    }
}

TEST(Crc32cPatch, EmptyRegionAndUnchangedPatchKeepTheCrc) {
    std::vector<uint8_t> buf(100, 0x5A);
    const uint32_t c = crc32c(buf.data(), buf.size());
    EXPECT_EQ(crc32cPatch(c, buf.size(), 10, buf.data() + 10, buf.data() + 10, 14), c);
    EXPECT_EQ(crc32cPatch(c, buf.size(), 100, buf.data(), buf.data(), 0), c);
    const uint32_t e = crc32c(buf.data(), 0);
    EXPECT_EQ(crc32cPatch(e, 0, 0, nullptr, nullptr, 0), e);
}

TEST(Crc32cPatch, RecordBatchIdentityRestampIsBitExact) {
    // The real use: re-stamp producerId/epoch/baseSequence (14 contiguous bytes)
    // of a RecordBatch and patch the stored CRC over [kAttributesOffset, end).
    namespace rb = rune::protocol::batch;
    std::mt19937_64 rng(7);
    for (int iter = 0; iter < 200; ++iter) {
        rune::protocol::RecordBatchBuilder b(0, 1000);
        const int recs = 1 + static_cast<int>(rng() % 50);
        std::vector<char> v(1 + rng() % 2000, 'x');
        for (int r = 0; r < recs; ++r) b.append("k", std::string_view(v.data(), v.size()), 1000 + r);
        b.setProducerId(static_cast<int64_t>(rng()));
        b.setProducerEpoch(static_cast<int16_t>(rng()));
        b.setBaseSequence(static_cast<int32_t>(rng()));
        std::vector<uint8_t> batch = b.build();
        const std::size_t region = batch.size() - rb::kAttributesOffset;
        const uint32_t before = rune::protocol::detail::getLE32(batch.data() + rb::kCrcOffset);
        std::vector<uint8_t> old(batch.begin() + rb::kProducerIdOffset, batch.begin() + rb::kRecordCountOffset);
        for (int i = rb::kProducerIdOffset; i < rb::kRecordCountOffset; ++i) batch[i] = static_cast<uint8_t>(rng());
        const uint32_t patched = crc32cPatch(before, region, rb::kProducerIdOffset - rb::kAttributesOffset,
                                             old.data(), batch.data() + rb::kProducerIdOffset,
                                             rb::kRecordCountOffset - rb::kProducerIdOffset);
        ASSERT_EQ(patched, crc32c(batch.data() + rb::kAttributesOffset, region));
        rune::protocol::detail::putLE(batch, rb::kCrcOffset, patched);
        rune::protocol::RecordBatchReader rd(batch.data(), batch.size());
        ASSERT_TRUE(rd.ensureValid());
    }
}
