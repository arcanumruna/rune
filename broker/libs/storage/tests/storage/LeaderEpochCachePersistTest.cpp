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

// FIX 2 regression: a checkpoint to a non-existent/unwritable directory must
// NOT install a partial file and must leave any pre-existing checkpoint intact.
TEST(LeaderEpochCachePersist, BadPathLeavesNoPollutingFile) {
    // Write into a directory that does not exist — open() will fail → no rename.
    const std::string badPath = "/tmp/lec_nonexistent_dir_xyz/lec_ckpt.bin";
    LeaderEpochCache c;
    c.assign(0, 0); c.assign(1, 100);
    // Must not crash; tmp must not remain.
    c.checkpointToFile(badPath);
    // Neither the checkpoint nor the .tmp should exist.
    EXPECT_FALSE(std::filesystem::exists(badPath));
    EXPECT_FALSE(std::filesystem::exists(badPath + ".tmp"));
}

// FIX 2 regression: a successful checkpoint survives a restore (write-path
// integrity end-to-end including the directory fsync branch).
TEST(LeaderEpochCachePersist, CheckpointIntegrityAfterFixes) {
    const auto path = std::filesystem::temp_directory_path() / "lec_fix2_ckpt.bin";
    LeaderEpochCache c;
    c.assign(0, 0); c.assign(2, 200); c.assign(4, 400);
    c.checkpointToFile(path.string());

    // The .tmp must be gone (renamed to canonical path).
    EXPECT_FALSE(std::filesystem::exists(path.string() + ".tmp"));

    LeaderEpochCache c2;
    ASSERT_TRUE(c2.restoreFromFile(path.string()));
    EXPECT_EQ(c2.latestEpoch(), 4);
    EXPECT_EQ(c2.startOffsetFor(2), 200);
    EXPECT_EQ(c2.startOffsetFor(4), 400);
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

// ---------------------------------------------------------------------------
// D21 review L1: the leader-epoch-state file is pre-sized so its history
// practically never outgrows a slot — outgrowing it re-creates the file
// (fsync + rename + directory fsync), which can land on a worker.  Sparse: the
// pre-size costs address space, not blocks.
// ---------------------------------------------------------------------------
#include <rune/storage/LeaderEpochState.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

TEST(LeaderEpochStateRegression, FreshFileIsPresizedAndSparseRegression) {
    using ES = rune::storage::LeaderEpochState;
    const auto dir = std::filesystem::temp_directory_path() / ("rune_les_presize_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    const std::size_t stride = ES::strideFor(ES::kMinEntries);
    EXPECT_GE(ES::capacityOf(stride), 4096u) << "a slot must hold thousands of epochs";
    const std::vector<std::pair<int32_t, int64_t>> e{{1, 0}, {2, 100}};
    const int fd = ES::create(dir.string(), stride, 1, 2, e.begin(), e.end());
    ASSERT_GE(fd, 0);
    struct stat st{};
    ASSERT_EQ(::fstat(fd, &st), 0);
    EXPECT_EQ(static_cast<std::size_t>(st.st_size), 2 * stride);
#if defined(__linux__)   // ext4 / xfs: a hole costs no blocks (APFS allocates small extended files)
    EXPECT_LE(static_cast<int64_t>(st.st_blocks) * 512, int64_t{16} << 10) << "pre-size must not cost blocks";
#endif
    ::close(fd);
    const auto rd = ES::read(dir.string());
    EXPECT_EQ(rd.state, ES::FileState::Valid);
    EXPECT_EQ(rd.entries, e);
    EXPECT_EQ(rd.fence, 2);
    std::filesystem::remove_all(dir);
}
