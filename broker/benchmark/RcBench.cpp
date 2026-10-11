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
//                            Fix round 1: the step parses only RESIDENT bytes
//                            (mincore gate + MADV_WILLNEED read-ahead), so a cold
//                            step returns at once; nonres_frac = steps that found
//                            nothing resident (waiting on read-ahead).
//                            Fix round 2 (RR1-4): hints go through the same
//                            policy as the worker (async IORING_OP_FADVISE on
//                            io_uring, else sync MADV_WILLNEED); reported split
//                            per step: hint_ns / probe_ns (mincore) / parse_ns,
//                            minflt / majflt per step (whole run incl. drops),
//                            async_hints / sync_hints totals.
//                            Fix round 3 (m6): p999_us and max_step_minflt (the
//                            worst minor-fault count inside ONE timed step,
//                            getrusage outside the timed interval).  Regression
//                            threshold (controller ticket): p99 <= 20 µs at the
//                            default 32 KiB budget.
//   BM_RcWorkerPush/<iso>  — the worker-push egress arm (pushOwnedSubscriptions_)
//                            of 64 x ~1 KiB non-txn batches on an embedded-driver
//                            IPC publication (review I-3).  iso 0 / 1 as above.
//
// Base comparison (review I-3): the RU arms (BM_RcPushNonTxn/0,
// BM_RcWorkerPush/0) compile on the pre-S4 base 614ae96 too (every S4-only API
// is behind __has_include(<rune/node/RcState.h>)): copy this file into a base
// checkout's benchmark/ (add it to CMakeLists) and run ABAB against S4.
#if __has_include(<rune/node/RcState.h>)
#include <rune/node/RcState.h>
#define RUNE_RCBENCH_S4 1
#endif
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/transaction/ControlMarker.h>

#include <benchmark/benchmark.h>
#include <Aeron.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <climits>
#include <unistd.h>

namespace {

namespace fs = std::filesystem;
#if defined(RUNE_RCBENCH_S4)
using rune::node::RcFacts;
using rune::node::RcState;
#endif
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
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
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
#if defined(RUNE_RCBENCH_S4)
        const auto v = log.segmentBytesAt(base);   // the mapping, no page touched
        if (v.data == nullptr) continue;
        const auto start = reinterpret_cast<uintptr_t>(v.data) & ~static_cast<uintptr_t>(page - 1);
        const auto end   = reinterpret_cast<uintptr_t>(v.data) + v.size;
#else
        const auto v = log.readView(base, INT32_MAX);
        if (v.empty()) continue;
        const auto start = reinterpret_cast<uintptr_t>(v.data) & ~static_cast<uintptr_t>(page - 1);
        const auto end   = reinterpret_cast<uintptr_t>(v.data) + v.len;
#endif
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
#if defined(RUNE_RCBENCH_S4)
    for (int i = 0; i < 8 && w.rcStatusForTest(1, 0) != Worker::kRcReady; ++i) w.doWork(++now);
    if (w.rcStatusForTest(1, 0) != Worker::kRcReady) { state.SkipWithError("RC never Ready"); return; }
#else
    for (int i = 0; i < 8; ++i) w.doWork(++now);
#endif
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

void BM_RcWorkerPush(benchmark::State& state) {
    const auto iso = static_cast<uint8_t>(state.range(0));
    BenchDir d, ad;
    rune::node::EmbeddedDriverConfig dc;
    dc.dir = (ad.path / "aeron").string();
    dc.termBufferBytes = 16 * 1024 * 1024;
    std::unique_ptr<rune::node::EmbeddedDriver> driver;
    try {
        driver = std::make_unique<rune::node::EmbeddedDriver>(dc, rune::node::DriverMode::Threaded);
    } catch (const std::exception& e) { state.SkipWithError(e.what()); return; }
    aeron::Context ctx;
    ctx.aeronDir(dc.dir);
    auto aeron = aeron::Aeron::connect(ctx);
    const int64_t pubReg = aeron->addExclusivePublication("aeron:ipc", 9101);
    const int64_t subReg = aeron->addSubscription("aeron:ipc", 9101);
    std::shared_ptr<aeron::ExclusivePublication> pub;
    std::shared_ptr<aeron::Subscription> sub;
    for (int i = 0; i < 1'000'000 && (!pub || !sub || !pub->isConnected()); ++i) {
        if (!pub) pub = aeron->findExclusivePublication(pubReg);
        if (!sub) sub = aeron->findSubscription(subReg);
        std::this_thread::yield();
    }
    if (!pub || !sub || !pub->isConnected()) { state.SkipWithError("IPC not connected"); return; }

    rune::storage::Log log(d.path.string(), 64LL * 1024 * 1024);
    log.recover();
    for (int i = 0; i < 64; ++i) appendAt(log, dataBatch(1000));
    rune::replication::PartitionConfig c;
    c.topicId = 1; c.partitionId = 0; c.selfId = 1; c.minIsr = 1; c.pushOnAppend = false;
    c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000LL;
    NullTransport t;
    PartitionReplica rep(c, rune::replication::Role::Leader, 1, {1}, {1}, &log, &t);
    rep.onLeaderLogAppended();
    Worker w(0, 1 << 22, 1 << 23, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    int64_t now = 1;
    for (int i = 0; i < 8; ++i) w.doWork(++now);
    auto subscribe = [&] {
        WorkerOp op;
        op.type = WorkerOp::Type::Subscribe; op.subscriptionId = 1; op.sessionId = 1; op.topicId = 1;
        op.partition = 0; op.nextOffset = 0; op.logPtr = &log; op.replica = &rep; op.isolation = iso;
        op.egressPub = pub.get();
        (void)w.tryEnqueueOp(std::move(op));
    };
    std::size_t frames = 0;
    auto drain = [&] {
        while (sub->poll([&](aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t,
                             aeron::Header&) { ++frames; }, 256) > 0) {}
    };
    for (auto _ : state) {
        subscribe();            // rewinds the cursor to 0 (updates the existing entry)
        w.doWork(++now);        // drain op + push the 64 batches
        state.PauseTiming();
        drain();
        state.ResumeTiming();
    }
    state.counters["frames_per_iter"] = static_cast<double>(frames) / static_cast<double>(state.iterations());
    state.SetItemsProcessed(state.iterations() * 64);
}

#if defined(RUNE_RCBENCH_S4)
// Read-ahead issuer for the bench: exactly the worker's policy — an async
// IORING_OP_FADVISE(WILLNEED) where the io backend is io_uring, else the
// synchronous MADV_WILLNEED.
#if defined(__linux__)
constexpr int kRusageWho = RUSAGE_THREAD;   // the worker-thread view
#else
constexpr int kRusageWho = RUSAGE_SELF;
#endif

struct BenchHints {
    rune::node::IoBackend* io = nullptr;
    uint64_t asyncIssued = 0, syncIssued = 0, skipped = 0;
    static rune::node::RcHint fn(void* c, const rune::node::RcReadAheadReq& r) {
        auto* h = static_cast<BenchHints*>(c);
        if (h->io->asyncHints() && r.fd >= 0) {
            const auto t = h->io->submitWillNeed(r.fd, r.fileOff,
                                                 static_cast<uint32_t>(std::min<std::size_t>(r.len, UINT32_MAX)));
            if (t == rune::node::kNoToken) { ++h->skipped; return rune::node::RcHint::Skipped; }
            ++h->asyncIssued;
            return rune::node::RcHint::InFlight;
        }
        ++h->syncIssued;
        RcState::syncWillNeed(r);
        return rune::node::RcHint::Done;
    }
};

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
    const RcFacts f{/*leader*/ true, /*logGeneration*/ 0, /*leadershipGeneration*/ 0, /*epoch*/ 1};
    rune::node::IoBackend io(1024, 1024);
    BenchHints hints;
    hints.io = &io;
    RcState::StepProfile prof;
    std::vector<double> us;
    us.reserve(1 << 20);
    uint64_t nonRes = 0, steps = 0;
    long maxStepMinflt = 0;   // review m6: worst-case minor faults inside ONE step
    auto fresh = [&] {
        auto rc = std::make_unique<RcState>();
        rc->reserve(1024);
        rc->setReadAhead(&BenchHints::fn, &hints);
        rc->setProfile(&prof);
        return rc;
    };
    auto rc = fresh();
    dropCache(log, d.path);
    struct rusage ru0{}, ru1{};
    ::getrusage(RUSAGE_SELF, &ru0);
    for (auto _ : state) {
        if (rc->ready()) {
            state.PauseTiming();
            nonRes += rc->nonResidentSteps(); steps += rc->scanSteps();
            while (rc->hintInFlight() && io.reap([&](const rune::node::FlushCompletion&) { rc->onHintCompleted(); }) >= 0) {
                if (!rc->hintInFlight()) break;
            }
            rc = fresh();
            dropCache(log, d.path);
            state.ResumeTiming();
        }
        struct rusage s0{}, s1{};
        ::getrusage(kRusageWho, &s0);   // outside the timed interval
        const auto t0 = std::chrono::steady_clock::now();
        benchmark::DoNotOptimize(rc->advance(f, &log, budget));
        const auto t1 = std::chrono::steady_clock::now();
        ::getrusage(kRusageWho, &s1);
        us.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
        maxStepMinflt = std::max<long>(maxStepMinflt, s1.ru_minflt - s0.ru_minflt);
        // The worker reaps completions every pass (its existing drain).
        state.PauseTiming();
        (void)io.reap([&](const rune::node::FlushCompletion&) { rc->onHintCompleted(); });
        state.ResumeTiming();
    }
    ::getrusage(RUSAGE_SELF, &ru1);
    nonRes += rc->nonResidentSteps(); steps += rc->scanSteps();
    if (us.empty()) return;
    std::sort(us.begin(), us.end());
    const double n = static_cast<double>(us.size());
    state.counters["p50_us"] = us[us.size() / 2];
    state.counters["p99_us"] = us[std::min(us.size() - 1, us.size() * 99 / 100)];
    state.counters["p999_us"] = us[std::min(us.size() - 1, us.size() * 999 / 1000)];
    state.counters["max_us"] = us.back();
    state.counters["max_step_minflt"] = static_cast<double>(maxStepMinflt);
    state.counters["nonres_frac"] = static_cast<double>(nonRes) / static_cast<double>(std::max<uint64_t>(1, steps));
    state.counters["hint_ns"]  = static_cast<double>(prof.hintNs) / n;     // mean per step
    state.counters["probe_ns"] = static_cast<double>(prof.probeNs) / n;
    state.counters["parse_ns"] = static_cast<double>(prof.parseNs) / n;
    state.counters["minflt"]   = static_cast<double>(ru1.ru_minflt - ru0.ru_minflt) / n;
    state.counters["majflt"]   = static_cast<double>(ru1.ru_majflt - ru0.ru_majflt) / n;
    state.counters["async_hints"] = static_cast<double>(hints.asyncIssued);
    state.counters["sync_hints"]  = static_cast<double>(hints.syncIssued);
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * budget);
}
#endif

} // namespace

BENCHMARK(BM_RcPushNonTxn)->Arg(0)->Arg(1)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_RcWorkerPush)->Arg(0)->Arg(1)->Unit(benchmark::kMicrosecond);
#if defined(RUNE_RCBENCH_S4)
BENCHMARK(BM_RcBuildStep)->Arg(16 * 1024)->Arg(32 * 1024)->Arg(64 * 1024)->Arg(256 * 1024)
    ->Unit(benchmark::kMicrosecond)->Iterations(20000);
#endif
