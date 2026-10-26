#include <gtest/gtest.h>
#include <rune/storage/MappedFile.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <cstdio>
#include <filesystem>
#include <stdexcept>
#include <atomic>
#include <chrono>
#include <fcntl.h>
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

// D28 (box perf capture 2026-10-08): the prefault loop `data_[off] = data_[off]`
// is a no-op self-assignment the optimizer DELETED — in every Release build the
// provisioner's "prefaulted" index was never touched, and the worker's first
// OffsetIndex::append into each page took a page fault (mmap_lock, queued
// behind the provisioner / cleaner mmap+munmap).  Probed with /proc/self/pagemap
// (bit 63 = the page is mapped in this process) — exact, unlike fault counts
// (large folios map several pages per fault).  Must hold in Release.
namespace {
#ifdef __linux__
size_t mappedPages(const uint8_t* base, size_t pages, size_t page) {
    const int fd = ::open("/proc/self/pagemap", O_RDONLY);
    if (fd < 0) return SIZE_MAX;
    size_t n = 0;
    for (size_t i = 0; i < pages; ++i) {
        uint64_t e = 0;
        const off_t at = static_cast<off_t>((reinterpret_cast<uintptr_t>(base) / page + i) * sizeof e);
        if (::pread(fd, &e, sizeof e, at) == static_cast<ssize_t>(sizeof e) && (e >> 63) != 0) ++n;
    }
    ::close(fd);
    return n;
}
#endif
}
TEST(MappedFileRegression, PrefaultMapsEveryPageEvenOptimizedRegression) {
#ifndef __linux__
    GTEST_SKIP() << "needs /proc/self/pagemap";
#else
    const size_t page = static_cast<size_t>(::sysconf(_SC_PAGESIZE));
    const size_t pages = 64;
    {   // control: without a prefault nothing is mapped (the probe is sensitive)
        const auto ctl = makeTempPath("rune_mf_prefault_ctl");
        MappedFile f(ctl.string(), pages * page, /*writable*/true, /*prefault*/false);
        const size_t n = mappedPages(f.data(), pages, page);
        if (n == SIZE_MAX) GTEST_SKIP() << "/proc/self/pagemap unreadable";
        EXPECT_LT(n, pages) << "probe insensitive: an untouched mapping reads as mapped";
        std::filesystem::remove(ctl);
    }
    const auto path = makeTempPath("rune_mf_prefault");
    {   // fresh file: write-prefaulted
        MappedFile f(path.string(), pages * page, /*writable*/true, /*prefault*/true);
        EXPECT_EQ(mappedPages(f.data(), pages, page), pages) << "a prefaulted fresh mapping left pages unmapped";
        for (size_t i = 0; i < pages; ++i) f.data()[i * page + 8] = static_cast<uint8_t>(i + 1);
    }
    {   // reopened existing file: prefaulted again
        MappedFile f(path.string(), pages * page, /*writable*/true, /*prefault*/true);
        EXPECT_EQ(mappedPages(f.data(), pages, page), pages) << "a prefaulted reopened mapping left pages unmapped";
        unsigned sum = 0;
        for (size_t i = 0; i < pages; ++i) sum += f.data()[i * page + 8];
        EXPECT_EQ(sum, pages * (pages + 1) / 2);
    }
    std::filesystem::remove(path);
#endif
}

// D28: the recovered ACTIVE segment's index is mapped before the owner appends.
TEST(MappedFileRegression, RecoveredActiveSegmentIndexIsPrefaultedRegression) {
#ifndef __linux__
    GTEST_SKIP() << "needs /proc/self/pagemap";
#else
    const auto dir = makeTempPath("rune_mf_recovered_active");
    std::filesystem::create_directories(dir);
    {
        rune::storage::Log log(dir.string(), 64 * 1024 * 1024);
        rune::protocol::RecordBatchBuilder b(0, 1000);
        b.append("k", "v", 1000);
        log.append(b.build(), 0);
        log.sync();
    }
    rune::storage::Log log(dir.string(), 64 * 1024 * 1024);
    (void)log.recover();
    const auto* idx = log.activeIndexForTest();
    ASSERT_NE(idx, nullptr);
    const size_t page = static_cast<size_t>(::sysconf(_SC_PAGESIZE));
    const size_t pages = idx->mappedBytesForTest() / page;
    ASSERT_GT(pages, 0u);
    EXPECT_EQ(mappedPages(idx->dataForTest(), pages, page), pages) << "the recovered active index is not prefaulted";
    std::filesystem::remove_all(dir);
#endif
}

// ---------------------------------------------------------------------------
// Full disk: a writable map must be backed by real blocks when it is made, so a
// full filesystem answers with an exception at open/resize instead of a SIGBUS
// on the first store into a page that has no block behind it.  Mutation: drop
// the fallocate in MappedFile.cpp → each child dies on SIGBUS.
// ---------------------------------------------------------------------------
#include <rune/storage/testing/DiskFullTmpfs.h>

TEST(MappedFileRegression, WritableMapOnAFullDiskThrowsInsteadOfSigbusRegression) {
    rune::test::runOnTinyTmpfs(64 * 1024, [](const std::string& dir) {
        try {
            MappedFile mf(dir + "/big.index", 1 << 20, true, /*prefault=*/true);
            std::fprintf(stderr, "prefaulted 1 MiB map on a 64 KiB fs did not throw\n");
            return false;
        } catch (const std::runtime_error&) {}
        try {
            MappedFile mf(dir + "/big2.index", 1 << 20, true, /*prefault=*/false);
            for (int i = 0; i < (1 << 20); i += 4096) mf.data()[i] = 1;   // SIGBUS without the fix
            std::fprintf(stderr, "unprefaulted 1 MiB map on a 64 KiB fs did not throw\n");
            return false;
        } catch (const std::runtime_error&) {}
        return true;
    });
}

TEST(MappedFileRegression, ResizeGrowthOnAFullDiskThrowsInsteadOfSigbusRegression) {
    rune::test::runOnTinyTmpfs(64 * 1024, [](const std::string& dir) {
        MappedFile mf(dir + "/grow.index", 16 * 1024, true);
        mf.data()[0] = 7;
        try {
            mf.resize(1 << 20);
            for (int i = 0; i < (1 << 20); i += 4096) mf.data()[i] = 1;   // SIGBUS without the fix
            std::fprintf(stderr, "resize to 1 MiB on a 64 KiB fs did not throw\n");
            return false;
        } catch (const std::runtime_error&) {}
        return true;
    });
}

TEST(MappedFileRegression, WritableMapOnAFilledDiskThrowsRegression) {
    rune::test::runOnTinyTmpfs(64 * 1024, [](const std::string& dir) {
        if (!rune::test::fillDisk(dir)) { std::fprintf(stderr, "could not fill fs\n"); return false; }
        try {
            MappedFile mf(dir + "/small.index", 4096, true);
            mf.data()[0] = 1;   // SIGBUS without the fix
            std::fprintf(stderr, "4 KiB map on a full fs did not throw\n");
            return false;
        } catch (const std::runtime_error&) {}
        return true;
    });
}
