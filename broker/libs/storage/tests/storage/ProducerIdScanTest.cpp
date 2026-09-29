// ProducerIdScanTest.cpp — dedicated storage-level tests for scanMaxProducerId
// over a REAL on-disk rune::storage::Log.
//
// The existing coverage (transaction/ProducerIdWatermarkRecoveryTest) drives the
// scan over an InMemoryLog.  This suite exercises the same contract over the
// real Log — the segment-backed read path (CP-3 first-live-segment start,
// chunked read, high-watermark enforcement).

#include <gtest/gtest.h>
#include <rune/storage/ProducerIdScan.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

using rune::storage::Log;
using rune::storage::scanMaxProducerId;
namespace fs = std::filesystem;

namespace {

struct TmpDir {
    fs::path path;
    TmpDir() {
        static std::atomic<uint32_t> seq{0};
        auto n  = seq.fetch_add(1, std::memory_order_relaxed);
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        path = fs::temp_directory_path() /
               ("rune_pidscan_" + std::to_string(static_cast<long>(::getpid())) +
                "_" + std::to_string(n) + "_" + std::to_string(ts));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};

// Append one single-record batch at `off` with the given producerId (pid < 0 →
// legacy/non-idempotent).  Returns the base offset used.
int64_t appendPid(Log& log, int64_t off, int64_t pid) {
    rune::protocol::RecordBatchBuilder b(off, 1000LL + off);
    if (pid >= 0) b.setProducerId(pid);
    b.append("k", "v", 1000LL + off);
    auto data = b.build();
    log.append(data, off); // single-record → lastOffset == off
    return off;
}

} // namespace

TEST(ProducerIdScanLog, EmptyLogReturnsMinusOne) {
    TmpDir d;
    Log log(d.path.string());
    EXPECT_EQ(scanMaxProducerId(log, log.logEndOffset()), -1);
}

TEST(ProducerIdScanLog, ReturnsMaxPidBelowHighWatermark) {
    TmpDir d;
    Log log(d.path.string());
    appendPid(log, 0, 10);
    appendPid(log, 1, 50);
    appendPid(log, 2, 30);
    // All three committed (hw = 3) → highest observed pid is 50.
    EXPECT_EQ(scanMaxProducerId(log, /*highWatermark=*/3), 50);
}

TEST(ProducerIdScanLog, HighWatermarkExcludesUncommittedTail) {
    TmpDir d;
    Log log(d.path.string());
    appendPid(log, 0, 10);
    appendPid(log, 1, 20);
    appendPid(log, 2, 9999); // uncommitted tail
    // hw = 2 → only offsets [0,1] scanned; the 9999 pid at offset 2 is excluded.
    EXPECT_EQ(scanMaxProducerId(log, /*highWatermark=*/2), 20);
}

TEST(ProducerIdScanLog, NonIdempotentBatchesYieldMinusOne) {
    TmpDir d;
    Log log(d.path.string());
    appendPid(log, 0, -1);
    appendPid(log, 1, -1);
    EXPECT_EQ(scanMaxProducerId(log, /*highWatermark=*/2), -1);
}
