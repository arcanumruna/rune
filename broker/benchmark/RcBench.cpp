// RcBench.cpp — F15 S4 (design note §9): the worker-owned read_committed state.
//
//   BM_RcPushNonTxn/<iso>  — one gateway-path PushRead executed by the OWNING
//                            worker (executePushReadWire_: bound, read, clamp +
//                            strip walk, return encode) over non-transactional
//                            data.  iso 0 = read_uncommitted, 1 = read_committed.
//                            Budget: read_committed matches read_uncommitted
//                            within noise (one LSO lookup per read, one attribute
//                            test per batch — both already paid by the walk).
//   BM_RcBuildStep/<budget> — ONE §2.3 incremental build step (RcState::advance)
//                            of <budget> bytes over a 32 MiB log with a cold page
//                            cache: before every build round the segments are
//                            dropped from the process (madvise DONTNEED) and, on
//                            Linux, from the page cache (posix_fadvise DONTNEED),
//                            so each step faults its bytes in.  Reports p50 /
//                            p99 / max per step (counters, µs).  Target: p99 <=
//                            20 µs at the default kRcScanBytesPerPass.  macOS has
//                            no page-cache drop without root: there the step is
//                            mapping-cold (page faults) but page-cache-warm.
#include <rune/node/RcState.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/transaction/ControlMarker.h>

#include <benchmark/benchmark.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace {

namespace fs = std::filesystem;
using rune::node::RcFacts;
using rune::node::RcState;
using rune::node::Worker;
using rune::node::WorkerOp;
using rune::node::WorkerReturn;
using rune::replication::PartitionReplica;

struct BenchDir {
    fs::path path;
    BenchDir() {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() / ("rune_rcbench_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~BenchDir() { std::error_code ec; fs::remove_all(path, ec); }
};

class NullTransport final : public rune::replication::PartitionTransport {
public:
    void sendFetchRequest(const rune::replication::FetchRequestMsg&) override {}
    void sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override {}
};

std::vector<uint8_t> dataBatch(std::size_t valueBytes) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.append("k", std::string(valueBytes, 'v'), 1000);
    return b.build();
}
std::vector<uint8_t> txnBatch(int64_t pid, std::size_t valueBytes) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.setTransactional(true); b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(0);
    b.append("k", std::string(valueBytes, 't'), 1000);
    return b.build();
}
void appendAt(rune::storage::Log& log, const std::vector<uint8_t>& b) {
    int64_t base = 0;
    (void)log.appendNoSyncRestamp(b.data(), b.size(),
                                  rune::protocol::RecordBatchReader(b.data(), b.size()).lastOffsetDelta(), base);
}

// Drop every segment's pages from this process and (Linux) the page cache.
void dropCache(rune::storage::Log& log, const fs::path& dir) {
    const long page = ::sysconf(_SC_PAGESIZE);
    for (const int64_t base : log.segmentBaseOffsets()) {
        const auto v = log.readView(base, INT32_MAX);
        if (v.empty()) continue;
        const auto start = reinterpret_cast<uintptr_t>(v.data) & ~static_cast<uintptr_t>(page - 1);
        const auto end   = reinterpret_cast<uintptr_t>(v.data) + v.len;
        (void)::madvise(reinterpret_cast<void*>(start), end - start, MADV_DONTNEED);
    }
#if defined(__linux__)
    for (const auto& e : fs::directory_iterator(dir)) {
        if (e.path().extension() != ".log") continue;
        const int fd = ::open(e.path().c_str(), O_RDONLY);
        if (fd < 0) continue;
        (void)::fdatasync(fd);
        (void)::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
        ::close(fd);
    }
#else
    (void)dir;
#endif
}

void BM_RcPushNonTxn(benchmark::State& state) {
    const auto iso = static_cast<uint8_t>(state.range(0));
    BenchDir d;
    rune::storage::Log log(d.path.string(), 64LL * 1024 * 1024);
    log.recover();
    for (int i = 0; i < 64; ++i) appendAt(log, dataBatch(1000));   // 64 x ~1 KiB, non-transactional
    rune::replication::PartitionConfig c;
    c.topicId = 1; c.partitionId = 0; c.selfId = 1; c.minIsr = 1; c.pushOnAppend = false;
    c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000LL;
    NullTransport t;
    PartitionReplica rep(c, rune::replication::Role::Leader, 1, {1}, {1}, &log, &t);
    rep.onLeaderLogAppended();
    Worker w(0, 1 << 22, 1 << 23, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    int64_t now = 1;
    for (int i = 0; i < 8 && w.rcStatusForTest(1, 0) != Worker::kRcReady; ++i) w.doWork(++now);
    if (w.rcStatusForTest(1, 0) != Worker::kRcReady) { state.SkipWithError("RC never Ready"); return; }
    const int64_t hw = log.logEndOffset();
    std::size_t bytes = 0;
    for (auto _ : state) {
        WorkerOp op;
        op.type = WorkerOp::Type::PushRead; op.topicId = 1; op.partition = 0; op.replica = &rep;
        op.logPtr = &log; op.sessionId = 1; op.subscriptionId = 7; op.nextOffset = 0; op.hwBound = hw;
        op.isolation = iso;
        (void)w.tryEnqueueOp(std::move(op));
        w.doWork(++now);
        w.drainReturn([&](WorkerReturn r) { bytes += r.chunk.size(); benchmark::DoNotOptimize(r.chunk.data()); });
    }
    state.SetBytesProcessed(static_cast<int64_t>(bytes));
    state.SetItemsProcessed(state.iterations());
}

void BM_RcBuildStep(benchmark::State& state) {
    const auto budget = static_cast<int32_t>(state.range(0));
    BenchDir d;
    rune::storage::Log log(d.path.string(), 64LL * 1024 * 1024);
    log.recover();
    // 32 MiB: ~1 KiB data batches; one txn opened every 64 batches, its ABORT
    // marker 32 batches later (classification work on the build path).
    int64_t pid = 1;
    for (int i = 0; i < 32 * 1024; ++i) {
        if (i % 64 == 0) {
            appendAt(log, txnBatch(pid, 1000));
        } else if (i % 64 == 32) {
            appendAt(log, rune::transaction::ControlMarker::build(pid, 0, rune::transaction::ControlType::Abort, 0));
            ++pid;
        } else {
            appendAt(log, dataBatch(1000));
        }
    }
    log.sync();
    const RcFacts f{true, true, 0, 0, 1};
    std::vector<double> us;
    us.reserve(1 << 20);
    auto rc = std::make_unique<RcState>();
    rc->reserve(1024);
    dropCache(log, d.path);
    for (auto _ : state) {
        if (rc->ready()) {
            state.PauseTiming();
            rc = std::make_unique<RcState>();
            rc->reserve(1024);
            dropCache(log, d.path);
            state.ResumeTiming();
        }
        const auto t0 = std::chrono::steady_clock::now();
        benchmark::DoNotOptimize(rc->advance(f, &log, budget));
        const auto t1 = std::chrono::steady_clock::now();
        us.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
    }
    if (us.empty()) return;
    std::sort(us.begin(), us.end());
    state.counters["p50_us"] = us[us.size() / 2];
    state.counters["p99_us"] = us[std::min(us.size() - 1, us.size() * 99 / 100)];
    state.counters["max_us"] = us.back();
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * budget);
}

} // namespace

BENCHMARK(BM_RcPushNonTxn)->Arg(0)->Arg(1)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_RcBuildStep)->Arg(16 * 1024)->Arg(32 * 1024)->Arg(64 * 1024)->Arg(256 * 1024)
    ->Unit(benchmark::kMicrosecond)->Iterations(20000);
