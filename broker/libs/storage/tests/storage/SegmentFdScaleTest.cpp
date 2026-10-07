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

#include <filesystem>
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
