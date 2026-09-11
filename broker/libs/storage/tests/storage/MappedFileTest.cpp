#include <gtest/gtest.h>
#include <rune/storage/MappedFile.h>
#include <filesystem>
#include <stdexcept>
#include <atomic>
#include <chrono>
#include <unistd.h>
using rune::storage::MappedFile;

// ---------------------------------------------------------------------------
// Helper: unique temp file path per test instance
// ---------------------------------------------------------------------------
static std::filesystem::path makeTempPath(const char* stem) {
    static std::atomic<uint64_t> g_tempSeq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto uniq = std::to_string(::getpid()) + "_" +
                std::to_string(g_tempSeq.fetch_add(1, std::memory_order_relaxed)) + "_" +
                std::to_string(ts);
    return std::filesystem::temp_directory_path() / (std::string(stem) + "_" + uniq + ".bin");
}

TEST(MappedFile, WriteReadPersistAcrossReopen) {
    auto p = makeTempPath("rune_mf");
    std::filesystem::remove(p);
    { MappedFile mf(p.string(), 4096, true); mf.data()[0] = 42; mf.data()[4095] = 7; mf.sync(); }
    { MappedFile mf(p.string(), 4096, false); EXPECT_EQ(mf.data()[0], 42); EXPECT_EQ(mf.data()[4095], 7); }
    std::filesystem::remove(p);
}

// Edge: write then read within the same instance (no reopen needed)
TEST(MappedFile, WriteReadWithinSingleInstance) {
    auto p = makeTempPath("rune_mf_single");
    std::filesystem::remove(p);
    {
        MappedFile mf(p.string(), 1024, true);
        mf.data()[0]    = 0xAB;
        mf.data()[512]  = 0xCD;
        mf.data()[1023] = 0xEF;
        EXPECT_EQ(mf.data()[0],    0xAB);
        EXPECT_EQ(mf.data()[512],  0xCD);
        EXPECT_EQ(mf.data()[1023], 0xEF);
    }
    std::filesystem::remove(p);
}

// Edge: resize() preserves earlier bytes and exposes new zeroed space; sync() + reopen sees it
TEST(MappedFile, ResizePreservesAndExposesZeroedSpace) {
    auto p = makeTempPath("rune_mf_resize");
    std::filesystem::remove(p);
    {
        MappedFile mf(p.string(), 4096, true);
        mf.data()[0]    = 0x11;
        mf.data()[4095] = 0x22;
        mf.resize(8192);
        // Earlier bytes preserved
        EXPECT_EQ(mf.data()[0],    0x11);
        EXPECT_EQ(mf.data()[4095], 0x22);
        // Newly exposed region should be zero (ftruncate zero-fills)
        EXPECT_EQ(mf.data()[4096], 0x00);
        EXPECT_EQ(mf.data()[8191], 0x00);
        // Write into new region
        mf.data()[5000] = 0x55;
        mf.sync();
    }
    // Reopen and verify everything persisted
    {
        MappedFile mf(p.string(), 8192, false);
        EXPECT_EQ(mf.data()[0],    0x11);
        EXPECT_EQ(mf.data()[4095], 0x22);
        EXPECT_EQ(mf.data()[5000], 0x55);
    }
    std::filesystem::remove(p);
}

// Edge: resize to larger then write into the newly-mapped region; verify persistence after reopen
TEST(MappedFile, ResizeWriteIntoNewRegionPersists) {
    auto p = makeTempPath("rune_mf_resize2");
    std::filesystem::remove(p);
    {
        MappedFile mf(p.string(), 4096, true);
        mf.data()[100] = 0xAA;
        mf.resize(16384);
        // Write well into the new region
        mf.data()[8000] = 0xBB;
        mf.data()[16383] = 0xCC;
        mf.sync();
    }
    {
        MappedFile mf(p.string(), 16384, false);
        EXPECT_EQ(mf.data()[100],   0xAA);
        EXPECT_EQ(mf.data()[8000],  0xBB);
        EXPECT_EQ(mf.data()[16383], 0xCC);
    }
    std::filesystem::remove(p);
}

// Edge: two sequential writable instances over the same path (second sees first's data)
TEST(MappedFile, TwoSequentialWritableInstancesSamePath) {
    auto p = makeTempPath("rune_mf_seq");
    std::filesystem::remove(p);
    {
        MappedFile mf(p.string(), 4096, true);
        mf.data()[0] = 0x01;
        mf.data()[1] = 0x02;
        mf.sync();
    }
    {
        // Second writable instance: ftruncate to same size preserves content, then we write
        MappedFile mf(p.string(), 4096, true);
        // Overwrite byte 0 to a different value
        mf.data()[0] = 0xFF;
        mf.data()[2] = 0x03;
        mf.sync();
    }
    {
        MappedFile mf(p.string(), 4096, false);
        EXPECT_EQ(mf.data()[0], 0xFF);
        EXPECT_EQ(mf.data()[2], 0x03);
    }
    std::filesystem::remove(p);
}

// Edge: opening a non-existent path read-only throws
TEST(MappedFile, OpenNonExistentReadOnlyThrows) {
    auto p = makeTempPath("rune_mf_noexist_xyz123");
    std::filesystem::remove(p); // ensure it doesn't exist
    EXPECT_THROW(MappedFile(p.string(), 4096, false), std::runtime_error);
}

// Edge: boundary — writing the last byte of the mapping and reading it back
TEST(MappedFile, BoundaryLastByteWriteRead) {
    auto p = makeTempPath("rune_mf_boundary");
    std::filesystem::remove(p);
    constexpr size_t sz = 4096;
    {
        MappedFile mf(p.string(), sz, true);
        mf.data()[sz - 1] = 0xDE;
        mf.sync();
    }
    {
        MappedFile mf(p.string(), sz, false);
        EXPECT_EQ(mf.data()[sz - 1], 0xDE);
    }
    std::filesystem::remove(p);
}

// Edge: sync() actually persists (read-only reopen after sync sees correct data)
TEST(MappedFile, SyncPersistsBeforeReopen) {
    auto p = makeTempPath("rune_mf_sync");
    std::filesystem::remove(p);
    {
        MappedFile mf(p.string(), 4096, true);
        for (int i = 0; i < 16; ++i) mf.data()[i] = static_cast<uint8_t>(i);
        mf.sync();
    }
    {
        MappedFile mf(p.string(), 4096, false);
        for (int i = 0; i < 16; ++i)
            EXPECT_EQ(mf.data()[i], static_cast<uint8_t>(i)) << "at index " << i;
    }
    std::filesystem::remove(p);
}
