// CleanerOffsetCheckpointTest.cpp — dedicated unit tests for the compaction
// cleaner's on-disk first-dirty-offset checkpoint.
//
// SkimpyOffsetMapTest (SOM-3/4) exercises persistence + truncation indirectly;
// these isolate the four load/store paths, including the magic-validation and
// uninitialized-file paths that had no direct coverage.

#include <gtest/gtest.h>
#include <rune/storage/CleanerOffsetCheckpoint.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <unistd.h>

using rune::storage::CleanerOffsetCheckpoint;
namespace fs = std::filesystem;

namespace {

// Unique checkpoint file path inside a fresh temp dir.
struct TmpCkpt {
    fs::path dir;
    fs::path file;
    TmpCkpt() {
        static std::atomic<uint32_t> seq{0};
        auto n  = seq.fetch_add(1, std::memory_order_relaxed);
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        dir = fs::temp_directory_path() /
              ("rune_ckpt_" + std::to_string(static_cast<long>(::getpid())) +
               "_" + std::to_string(n) + "_" + std::to_string(ts));
        fs::create_directories(dir);
        file = dir / "cleaner.ckpt";
    }
    ~TmpCkpt() { std::error_code ec; fs::remove_all(dir, ec); }
};

} // namespace

TEST(CleanerOffsetCheckpoint, UninitializedFileReadsZero) {
    TmpCkpt t;
    CleanerOffsetCheckpoint cp(t.file.string(), /*logEndOffset=*/1000);
    EXPECT_EQ(cp.value(), 0) << "fresh file with no magic → uninitialized (0)";
}

TEST(CleanerOffsetCheckpoint, StorePersistsAcrossReopen) {
    TmpCkpt t;
    {
        CleanerOffsetCheckpoint cp(t.file.string(), /*logEndOffset=*/1000);
        cp.store(42);
        EXPECT_EQ(cp.value(), 42);
    }
    CleanerOffsetCheckpoint reopened(t.file.string(), /*logEndOffset=*/1000);
    EXPECT_EQ(reopened.value(), 42) << "stored value must survive reopen";
}

TEST(CleanerOffsetCheckpoint, LoadClampsToLogEndOffsetOnTruncation) {
    TmpCkpt t;
    {
        CleanerOffsetCheckpoint cp(t.file.string(), /*logEndOffset=*/1000);
        cp.store(100);
    }
    // Log truncated below the checkpoint (failover) → clamp to logEndOffset.
    CleanerOffsetCheckpoint reopened(t.file.string(), /*logEndOffset=*/50);
    EXPECT_EQ(reopened.value(), 50) << "checkpoint above LEO must clamp to LEO";
}

TEST(CleanerOffsetCheckpoint, CorruptMagicResetsToZero) {
    TmpCkpt t;
    {
        CleanerOffsetCheckpoint cp(t.file.string(), /*logEndOffset=*/1000);
        cp.store(77);
    }
    // Corrupt the magic sentinel (bytes [8..15]) directly on disk.
    {
        std::fstream f(t.file.string(),
                       std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(f.is_open());
        f.seekp(8);
        const char garbage[8] = {1, 2, 3, 4, 5, 6, 7, 8};
        f.write(garbage, sizeof(garbage));
    }
    CleanerOffsetCheckpoint reopened(t.file.string(), /*logEndOffset=*/1000);
    EXPECT_EQ(reopened.value(), 0) << "bad magic → treated as uninitialized";
}
