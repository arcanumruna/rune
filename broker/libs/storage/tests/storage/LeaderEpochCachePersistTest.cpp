#include <rune/storage/LeaderEpochCache.h>
#include <gtest/gtest.h>
#include <filesystem>
using namespace rune::storage;

TEST(LeaderEpochCachePersist, RoundTripsThroughFile) {
    LeaderEpochCache c;
    c.assign(0, 0); c.assign(1, 100); c.assign(3, 250);
    const auto path = std::filesystem::temp_directory_path() / "lec_ckpt.bin";
    c.checkpointToFile(path.string());

    LeaderEpochCache c2;
    ASSERT_TRUE(c2.restoreFromFile(path.string()));
    EXPECT_EQ(c2.latestEpoch(), 3);
    EXPECT_EQ(c2.startOffsetFor(1), 100);
    EXPECT_EQ(c2.startOffsetFor(3), 250);
    // KIP-101 walk-back still works after restore:
    auto [ep, end] = c2.endOffsetForEpoch(1, /*leo*/400);
    EXPECT_EQ(ep, 1);
    EXPECT_EQ(end, 250) << "epoch 1 ended where epoch 3 began";
    std::filesystem::remove(path);
}

TEST(LeaderEpochCachePersist, MissingFileReturnsFalse) {
    LeaderEpochCache c;
    EXPECT_FALSE(c.restoreFromFile("/tmp/lec_does_not_exist_xyz.bin"));
}

TEST(LeaderEpochCachePersist, TruncatedFileReturnsFalse) {
    const auto path = std::filesystem::temp_directory_path() / "lec_truncated.bin";
    // Write 3 bytes — clearly truncated
    {
        int fd = ::open(path.string().c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        ASSERT_GE(fd, 0);
        uint8_t garbage[3] = {1, 2, 3};
        ::write(fd, garbage, 3);
        ::close(fd);
    }
    LeaderEpochCache c;
    EXPECT_FALSE(c.restoreFromFile(path.string()));
    std::filesystem::remove(path);
}

TEST(LeaderEpochCachePersist, CorruptMagicReturnsFalse) {
    LeaderEpochCache c;
    c.assign(0, 0);
    c.assign(1, 100);
    const auto path = std::filesystem::temp_directory_path() / "lec_corrupt.bin";
    c.checkpointToFile(path.string());

    // Corrupt the first byte of the magic
    {
        int fd = ::open(path.string().c_str(), O_RDWR);
        ASSERT_GE(fd, 0);
        uint8_t bad = 0xFF;
        ::write(fd, &bad, 1);
        ::close(fd);
    }
    LeaderEpochCache c2;
    EXPECT_FALSE(c2.restoreFromFile(path.string()));
    std::filesystem::remove(path);
}

TEST(LeaderEpochCachePersist, EmptyCacheRoundTrips) {
    const auto path = std::filesystem::temp_directory_path() / "lec_empty.bin";
    LeaderEpochCache c;
    c.checkpointToFile(path.string());

    LeaderEpochCache c2;
    ASSERT_TRUE(c2.restoreFromFile(path.string()));
    EXPECT_EQ(c2.latestEpoch(), -1);
    std::filesystem::remove(path);
}

TEST(LeaderEpochCachePersist, EntriesAccessor) {
    LeaderEpochCache c;
    c.assign(0, 0);
    c.assign(2, 100);
    c.assign(5, 300);
    auto v = c.entries();
    ASSERT_EQ(v.size(), 3u);
    EXPECT_EQ(v[0].first, 0);  EXPECT_EQ(v[0].second, 0);
    EXPECT_EQ(v[1].first, 2);  EXPECT_EQ(v[1].second, 100);
    EXPECT_EQ(v[2].first, 5);  EXPECT_EQ(v[2].second, 300);
}
