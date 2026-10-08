#include <gtest/gtest.h>
#include <rune/storage/MappedFile.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
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

// F3.2: a read-only MappedFile must not retain a POSIX fd once mapped — the
// mapping itself keeps the page-cache binding alive, so the fd is dead weight
// (one fewer fd per retained segment).  A writable mapping still needs its fd
// (ftruncate on resize()), so it must keep it.
TEST(MappedFileRegression, ReadOnlyMappingRetainsNoFd) {
    auto p = makeTempPath("rune_mf_nofd");
    std::filesystem::remove(p);
    {
        MappedFile writer(p.string(), 4096, true);
        writer.data()[0]    = 0xAA;
        writer.data()[4095] = 0xBB;
        writer.sync();
    }

    MappedFile ro(p.string(), 4096, false);
    EXPECT_EQ(ro.fdForTest(), -1) << "read-only MappedFile must close its fd right after mmap";
    EXPECT_EQ(ro.data()[0],    0xAA);
    EXPECT_EQ(ro.data()[4095], 0xBB);

    // A larger resize() must remap (via a brief reopen) but end up fd-less again.
    ro.resize(8192);
    EXPECT_EQ(ro.fdForTest(), -1) << "resize() must close the fd again after remapping";
    EXPECT_EQ(ro.data()[0],    0xAA) << "data must survive the resize-remap";
    EXPECT_EQ(ro.data()[4095], 0xBB);

    // Writable mode is unchanged: it must still hold its fd (needed for ftruncate).
    MappedFile writable(p.string(), 4096, true);
    EXPECT_NE(writable.fdForTest(), -1) << "writable MappedFile must keep its fd open";

    std::filesystem::remove(p);
}

// F3 fix-round-1, item 1 (IMPORTANT): the MappedFile ctor must not leak fd_
// when ftruncate/mmap fails AFTER open() already succeeded.  Before the fix,
// a persistently-failing lazy-map retry (e.g. ensureReadMap() hammering a
// backing path whose mmap always fails) leaked one fd per attempt and would
// eventually exhaust the process's fd table.
//
// /dev/null: open() always succeeds, but mmap() on it fails with ENODEV (not
// seekable/mappable) — the exact "open succeeded, mmap failed" shape named
// in the review.  For the LogSegment half, a sealed segment's .log path is
// replaced with a symlink to /dev/null so the LAZY read-map's open() (by
// pathname) succeeds but its mmap() fails the same way, every retry.
static int countOpenFdsForTest() {
    int n = 0;
    std::error_code ec;
    for (auto& e : std::filesystem::directory_iterator("/dev/fd", ec)) { (void)e; ++n; }
    return n;
}

TEST(MappedFileRegression, FailedMmapAfterOpenLeaksNoFd) {
    // --- Part 1: direct MappedFile construction against /dev/null. ---
    int before1 = countOpenFdsForTest();
    for (int i = 0; i < 50; ++i) {
        EXPECT_THROW(MappedFile(std::string("/dev/null"), 4096, /*writable*/false),
                     std::runtime_error);
    }
    int after1 = countOpenFdsForTest();
    EXPECT_EQ(after1, before1)
        << "50 failed MappedFile(/dev/null) constructions must leak no fds";

    // --- Part 2: 50 failing LogSegment::readView() calls via the lazy
    // read-map path, where the backing path EXISTS (open succeeds) but mmap
    // fails every time. ---
    using namespace rune::storage; using namespace rune::protocol;
    auto dir = makeTempPath("rune_mf_leak_seg_dir");
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);

    std::vector<int64_t> bases;
    int64_t sealedBase = -1;
    {
        Log log(dir.string(), /*rollBytes=*/256);
        int64_t off = 0;
        for (int i = 0; i < 30; ++i) {
            RecordBatchBuilder b(off, 7000 + i); b.append("k", "v" + std::to_string(i), 7000 + i);
            off = log.append(b.build(), off);
        }
        bases = log.segmentBaseOffsets();
        ASSERT_GE(bases.size(), 3u);
        sealedBase = bases[1];
    }
    Log log2(dir.string(), /*rollBytes=*/256);
    log2.recover();
    LogSegment* sealedSeg = log2.segmentForTest(sealedBase);
    ASSERT_NE(sealedSeg, nullptr);
    ASSERT_FALSE(sealedSeg->hasReadMapForTest()) << "sealed segment must still be unmapped";

    // D3b r2: the lazy read map is made through the segment's own fd (never by
    // re-opening the path), so a path swap can no longer fail it; inject a
    // persistent, retried map failure via the seam instead.
    sealedSeg->setReadMapFaultForTest(true);

    int before2 = countOpenFdsForTest();
    for (int i = 0; i < 50; ++i) {
        LogReadView v = log2.readView(sealedBase, 1 << 20);
        EXPECT_TRUE(v.empty());
    }
    EXPECT_EQ(sealedSeg->readMapFailures(), 50u);
    int after2 = countOpenFdsForTest();
    EXPECT_EQ(after2, before2)
        << "50 failed lazy LogSegment readView() map attempts must leak no fds";

    std::filesystem::remove_all(dir);
}

// F3 fix-round-1, item 4: a resize() failure (after the old mapping is
// munmap'd) must leave the MappedFile in a fully consistent "nothing mapped"
// state: data()==nullptr, size()==0, and no leaked fd.  Injected cheaply via
// the same open-succeeds/mmap-fails symlink-to-/dev/null trick used above.
TEST(MappedFileRegression, ResizeFailureLeavesConsistentState) {
    auto p = makeTempPath("rune_mf_resize_fail");
    std::filesystem::remove(p);
    {
        MappedFile mf(p.string(), 4096, true);
        mf.data()[0] = 0x42;
        mf.sync();
    }

    MappedFile ro(p.string(), 4096, false);
    ASSERT_EQ(ro.data()[0], 0x42);
    ASSERT_EQ(ro.fdForTest(), -1);

    // Redirect the backing path: resize()'s read-only reopen (by pathname)
    // will now succeed (symlink resolves to /dev/null) but mmap() will fail.
    std::filesystem::remove(p);
    std::filesystem::create_symlink("/dev/null", p);

    EXPECT_THROW(ro.resize(8192), std::runtime_error);
    EXPECT_EQ(ro.data(), nullptr) << "a failed resize() must leave data() null";
    EXPECT_EQ(ro.size(), 0u)      << "a failed resize() must leave size() at 0, not the attempted new size";
    EXPECT_EQ(ro.fdForTest(), -1) << "a failed resize() must not leak the reopened fd";

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
