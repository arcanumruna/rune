// SegmentFdScaleTest.cpp — backlog D20: per-segment open-file cost.
//
// Every LogSegment used to keep a std::fstream (a libc FILE*) open for its
// whole life next to its raw .log fd.  glibc links every FILE into ONE
// process-wide list and fclose() unlinks it with a linear walk (_IO_un_link),
// so destroying a segment cost O(open segments): at ~40k segments the cleaner
// needed >1M owner passes to drain 256 retention drops.  The fix routes all
// segment I/O through the raw fd (pread / pwrite / pwritev / ftruncate /
// fsync): a live segment holds exactly TWO descriptors (its .log fd and the
// writable .index mapping's fd, kept for grow-on-demand ftruncate) and NO
// stdio stream, so segment destruction is plain close(2)s — O(1).  Before the
// fix it was THREE (the fstream's own fd + FILE on top).
//
// Deterministic: counts open descriptors (an fstream holds one of its own),
// never wall-clock.  Every test here is a regression test.
#include <gtest/gtest.h>
#include <rune/storage/LogSegment.h>
#include <rune/protocol/RecordBatch.h>

#include <cerrno>
#include <filesystem>
#include <system_error>
#include <memory>
#include <string>
#include <vector>
#include <fcntl.h>
#include <sys/resource.h>
#include <unistd.h>

using namespace rune::storage;
using namespace rune::protocol;
namespace fs = std::filesystem;

namespace {

constexpr int kSegments = 256;
constexpr int kFdsPerSegment = 2;   // .log fd + .index mapping fd; 3 with the fstream

fs::path freshDir(const char* stem) {
    auto d = fs::temp_directory_path() /
             (std::string(stem) + "_" + std::to_string(::getpid()));
    fs::remove_all(d);
    fs::create_directories(d);
    return d;
}

/// Room for 3 fds per segment (the pre-fix cost) so the RED run fails on the
/// count, not on EMFILE (macOS's default soft limit is 256).
void raiseFdLimit() {
    rlimit rl{};
    if (::getrlimit(RLIMIT_NOFILE, &rl) != 0) return;
    const rlim_t want = 8192;
    if (rl.rlim_cur < want) {
        rl.rlim_cur = (rl.rlim_max == RLIM_INFINITY || rl.rlim_max > want) ? want : rl.rlim_max;
        (void)::setrlimit(RLIMIT_NOFILE, &rl);
    }
}

int openFdCount() {
    rlimit rl{};
    ::getrlimit(RLIMIT_NOFILE, &rl);
    const int hi = rl.rlim_cur == RLIM_INFINITY || rl.rlim_cur > 65536 ? 65536 : static_cast<int>(rl.rlim_cur);
    int n = 0;
    for (int fd = 0; fd < hi; ++fd)
        if (::fcntl(fd, F_GETFD) != -1) ++n;
    return n;
}

std::vector<uint8_t> batchAt(int64_t base) {
    RecordBatchBuilder b(base, 1000);
    b.append("k", "v", 1000);
    return b.build();
}

} // namespace

// Each live segment (created, durable-appended, no-sync-appended, synced, read)
// costs exactly kFdsPerSegment descriptors — no stdio stream beside the raw fd.
TEST(SegmentFdScaleRegression, LiveSegmentHoldsNoStdioStreamRegression) {
    raiseFdLimit();
    const fs::path dir = freshDir("rune_d20_fd");
    const int before = openFdCount();
    {
        std::vector<std::unique_ptr<LogSegment>> segs;
        segs.reserve(kSegments);
        for (int i = 0; i < kSegments; ++i) {
            const int64_t base = static_cast<int64_t>(i) * 10;
            auto s = std::make_unique<LogSegment>(dir.string(), base, 4096, 64 * 1024);
            s->append(batchAt(base), base);                     // durable path (was fstream)
            const auto b2 = batchAt(base + 1);
            s->appendNoSync(b2.data(), b2.size(), base + 1);    // pwrite path
            s->sync();
            ASSERT_EQ(s->read(base, 1 << 16).size(), batchAt(base).size() + b2.size());
            segs.push_back(std::move(s));
        }
        const int live = openFdCount() - before;
        EXPECT_EQ(live, kFdsPerSegment * kSegments)
            << "per-segment descriptors = " << static_cast<double>(live) / kSegments
            << " (3 = a FILE*-backed fstream per segment: O(open segments) fclose on glibc)";
    }
    EXPECT_EQ(openFdCount(), before) << "segment destruction leaked descriptors";
    fs::remove_all(dir);
}

// The recovery path (reopen + recover over existing files) and the background
// staging path keep the same per-segment descriptor cost.
TEST(SegmentFdScaleRegression, RecoveredAndStagedSegmentsHoldNoStdioStreamRegression) {
    raiseFdLimit();
    const fs::path dir = freshDir("rune_d20_fd_rec");
    for (int i = 0; i < kSegments; ++i) {
        const int64_t base = static_cast<int64_t>(i) * 10;
        LogSegment s(dir.string(), base, 4096, 64 * 1024);
        s.append(batchAt(base), base);
    }
    const int before = openFdCount();
    {
        std::vector<std::unique_ptr<LogSegment>> segs;
        for (int i = 0; i < kSegments; ++i) {
            const int64_t base = static_cast<int64_t>(i) * 10;
            auto s = std::make_unique<LogSegment>(dir.string(), base, 4096, 64 * 1024);
            ASSERT_EQ(s->recover(/*syncIndex*/false), base + 1);
            ASSERT_EQ(s->sizeBytes(), static_cast<int64_t>(batchAt(base).size()));
            segs.push_back(std::move(s));
        }
        for (int i = 0; i < kSegments; ++i) {
            const std::string stem = (dir / ("staging-" + std::to_string(i))).string();
            segs.push_back(LogSegment::prepareStaging(stem + ".slog", stem + ".sidx", i, 4096, 64 * 1024));
        }
        const int live = openFdCount() - before;
        EXPECT_EQ(live, kFdsPerSegment * 2 * kSegments)
            << "per-segment descriptors = " << static_cast<double>(live) / (2 * kSegments);
    }
    EXPECT_EQ(openFdCount(), before) << "segment destruction leaked descriptors";
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// D20 review M1: the durable append's pwrite retry loop (writeAll_).
// ---------------------------------------------------------------------------
namespace {
int gCalls = 0;
ssize_t shortThenFull(int fd, const void* b, std::size_t n, off_t off) {
    ++gCalls;
    return ::pwrite(fd, b, gCalls == 1 && n > 1 ? n / 2 : n, off);   // first call: half
}
ssize_t eintrThenFull(int fd, const void* b, std::size_t n, off_t off) {
    if (++gCalls == 1) { errno = EINTR; return -1; }
    return ::pwrite(fd, b, n, off);
}
ssize_t hardEio(int, const void*, std::size_t, off_t) { ++gCalls; errno = EIO; return -1; }

off_t fileEnd(int fd) { return ::lseek(fd, 0, SEEK_END); }
} // namespace

TEST(SegmentFdScaleRegression, DurableAppendCompletesAShortWriteRegression) {
    const fs::path dir = freshDir("rune_d20_short");
    LogStorageState st;
    LogSegment seg(dir.string(), 0, 4096, 64 * 1024);
    seg.setStorageState(&st);
    gCalls = 0; st.pwriteForTest = &shortThenFull;
    const auto b = batchAt(0);
    seg.append(b, 0);
    EXPECT_EQ(gCalls, 2) << "the short write must be continued, not failed";
    EXPECT_EQ(st.failedErrno, 0);
    EXPECT_EQ(fileEnd(seg.logFd()), static_cast<off_t>(b.size()));
    EXPECT_EQ(seg.read(0, 1 << 16), b);
    fs::remove_all(dir);
}

TEST(SegmentFdScaleRegression, DurableAppendRetriesEintrRegression) {
    const fs::path dir = freshDir("rune_d20_eintr");
    LogStorageState st;
    LogSegment seg(dir.string(), 0, 4096, 64 * 1024);
    seg.setStorageState(&st);
    gCalls = 0; st.pwriteForTest = &eintrThenFull;
    const auto b = batchAt(0);
    EXPECT_EQ(seg.append(b, 0), 1);
    EXPECT_EQ(gCalls, 2);
    EXPECT_EQ(st.failedErrno, 0);
    EXPECT_EQ(fileEnd(seg.logFd()), static_cast<off_t>(b.size()));
    EXPECT_EQ(seg.read(0, 1 << 16), b);
    fs::remove_all(dir);
}

TEST(SegmentFdScaleRegression, DurableAppendHardErrorIsFailStopWithErrnoRegression) {
    const fs::path dir = freshDir("rune_d20_eio");
    LogStorageState st;
    LogSegment seg(dir.string(), 0, 4096, 64 * 1024);
    seg.setStorageState(&st);
    gCalls = 0; st.pwriteForTest = &hardEio;
    const auto b = batchAt(0);
    try {
        seg.append(b, 0);
        FAIL() << "a hard write error must throw";
    } catch (const std::system_error& e) {
        EXPECT_EQ(e.code().value(), EIO);
    }
    EXPECT_EQ(gCalls, 1) << "a hard errno is never retried";
    EXPECT_EQ(st.failedErrno, EIO) << "sticky fail-stop";
    EXPECT_EQ(seg.sizeBytes(), 0);
    st.pwriteForTest = nullptr;
    EXPECT_THROW(seg.sync(), std::system_error) << "a failed log never syncs again";
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// D20 review M2: recover() over a torn tail (pread + ftruncate through fd_),
// then append — with and without preallocated (KEEP_SIZE) blocks.  The append
// must land right after the cut (O_APPEND EOF == logSize_: review L1), and a
// second recovery must read every valid batch.
// ---------------------------------------------------------------------------
namespace {
void tornTailThenAppend(bool prealloc) {
    const fs::path dir = freshDir(prealloc ? "rune_d20_torn_pre" : "rune_d20_torn");
    const auto b0 = batchAt(0), b1 = batchAt(1), b2 = batchAt(2);
    std::string logPath;
    {
        LogSegment s(dir.string(), 0, 4096, 64 * 1024);
        s.append(b0, 0);
        s.append(b1, 1);
        logPath = s.logPath();
    }
    {   // torn tail: half of a third batch
        const int fd = ::open(logPath.c_str(), O_WRONLY | O_APPEND);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(::write(fd, b2.data(), b2.size() / 2), static_cast<ssize_t>(b2.size() / 2));
        ::close(fd);
    }
    const off_t valid = static_cast<off_t>(b0.size() + b1.size());
    {
        LogSegment s(dir.string(), 0, 4096, 64 * 1024);
        if (prealloc) s.preallocate(1 << 20);
        ASSERT_EQ(s.recover(/*syncIndex*/false), 2);
        ASSERT_EQ(s.sizeBytes(), static_cast<int64_t>(valid));
        if (prealloc) s.preallocate(1 << 20);
        EXPECT_EQ(fileEnd(s.logFd()), valid) << "O_APPEND EOF must equal logSize_ after the cut";
        s.append(b2, 2);
        EXPECT_EQ(fileEnd(s.logFd()), valid + static_cast<off_t>(b2.size()));
        EXPECT_EQ(s.read(2, 1 << 16), b2) << "the append landed right after the cut";
    }
    {
        LogSegment s(dir.string(), 0, 4096, 64 * 1024);
        EXPECT_EQ(s.recover(/*syncIndex*/false), 3) << "every valid batch survives a second recovery";
        std::vector<uint8_t> all(b0);
        all.insert(all.end(), b1.begin(), b1.end());
        all.insert(all.end(), b2.begin(), b2.end());
        EXPECT_EQ(s.read(0, 1 << 16), all);
    }
    fs::remove_all(dir);
}
} // namespace

TEST(SegmentFdScaleRegression, RecoverTornTailThenAppendRegression) { tornTailThenAppend(false); }
TEST(SegmentFdScaleRegression, RecoverTornTailThenAppendPreallocatedRegression) { tornTailThenAppend(true); }

// ---------------------------------------------------------------------------
// D20 review L2: the page-cache appends (appendNoSync / appendNoSyncRestamp)
// resume a short write like the durable path, instead of failing the log with
// a fabricated ENOSPC.  kShortWriteOnceForTest makes the first syscall write
// only part of the batch.
// ---------------------------------------------------------------------------
TEST(SegmentFdScaleRegression, AppendNoSyncResumesAShortWriteRegression) {
    const fs::path dir = freshDir("rune_d20b_l2_nosync");
    LogStorageState st;
    LogSegment seg(dir.string(), 0, 4096, 64 * 1024);
    seg.setStorageState(&st);
    const auto b0 = batchAt(0), b1 = batchAt(1);
    st.writeFaultForTest = LogStorageState::kShortWriteOnceForTest;
    EXPECT_EQ(seg.appendNoSync(b0.data(), b0.size(), 0), 1);
    EXPECT_EQ(st.failedErrno, 0) << "a short write is resumed, not a fail-stop";
    EXPECT_EQ(seg.appendNoSync(b1.data(), b1.size(), 1), 2);
    EXPECT_EQ(fileEnd(seg.logFd()), static_cast<off_t>(b0.size() + b1.size()));
    EXPECT_EQ(seg.read(0, 1 << 16).size(), b0.size() + b1.size());
    fs::remove_all(dir);
}

TEST(SegmentFdScaleRegression, AppendNoSyncRestampResumesAShortWriteRegression) {
    for (const int32_t epoch : {-1, 7}) {
        const fs::path dir = freshDir("rune_d20b_l2_restamp");
        LogStorageState st;
        LogSegment seg(dir.string(), 0, 4096, 64 * 1024);
        seg.setStorageState(&st);
        const auto b = batchAt(999);   // producer's base; restamped to 0
        st.writeFaultForTest = LogStorageState::kShortWriteOnceForTest;
        EXPECT_EQ(seg.appendNoSyncRestamp(b.data(), b.size(), 0, 0, epoch), 1);
        EXPECT_EQ(st.failedErrno, 0) << "epoch " << epoch;
        EXPECT_EQ(fileEnd(seg.logFd()), static_cast<off_t>(b.size()));
        const auto got = seg.read(0, 1 << 16);
        ASSERT_EQ(got.size(), b.size());
        RecordBatchReader r(got.data(), got.size());
        EXPECT_TRUE(r.ensureValid()) << "every byte after the cut landed in order";
        EXPECT_EQ(r.baseOffset(), 0);
        if (epoch >= 0) EXPECT_EQ(r.partitionLeaderEpoch(), epoch);
        fs::remove_all(dir);
    }
}
