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
//   BM_RcRingRebuildStep/<budget> — E2 final fix wave (I3): ONE budgeted step of
//                            the MEMORY-BACKED rebuild (RingScanning) over a FULL
//                            8 MiB InMemoryLog of minimum-size batches (~110k,
//                            transactions interleaved), producer state fed too —
//                            the per-partition cost a failover adds to each
//                            worker pass.  p50 / p99 / max per step (µs).
//   BM_RcRingRebuildFull/<budget> — the WHOLE ring rebuild (every step until
//                            Ready).  <budget> = 2147483647 is the pre-I3
//                            behaviour: the whole ring in ONE pass.
//   BM_RcTransientFailover/<K> — the K-partition failover: ONE shard-mode worker
//                            owns K transient follower partitions with full 8 MiB
//                            rings; BecomeLeader for all K lands in one pass; every
//                            Worker::doWork is timed until all K RC states are
//                            Ready.  max_pass_us / p99_pass_us / all_ready_us /
//                            passes (the Release twin of the
//                            TransientFailoverLatencyGateRegression test gate).
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
#include <rune/metadata/ShardRouter.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/WorkerPool.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>
#include <rune/storage/ProducerStateManager.h>
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

#if defined(RUNE_RCBENCH_S4)
// ---- E2 final fix wave (I3): the memory-backed (transient) RC rebuild -------
struct FullRing {
    rune::storage::InMemoryLog mem{8u << 20, 1'000'000};
    FullRing() {
        auto build = [](int64_t pid, int32_t seq, bool txn) {
            rune::protocol::RecordBatchBuilder b(0, 1000);
            if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq); }
            if (txn) b.setTransactional(true);
            b.append("", "", 1000);
            return b.build();
        };
        auto put = [&](const std::vector<uint8_t>& b) {
            int64_t base = -1;
            (void)mem.appendNoSyncRestamp(b.data(), b.size(),
                                          rune::protocol::RecordBatchReader(b.data(), b.size()).lastOffsetDelta(),
                                          base);
        };
        const auto plain = build(-1, -1, false);
        int32_t seq[8] = {};
        for (int64_t i = 0; mem.logStartOffset() == 0 || i < 150'000; ++i) {
            if (i % 1000 == 0) {
                const int k = static_cast<int>((i / 1000) % 8);
                put(build(1000 + k, seq[k]++, true));
                if ((i / 1000) % 3 != 2)
                    put(rune::transaction::ControlMarker::build(
                        1000 + k, 0,
                        ((i / 1000) % 3) == 0 ? rune::transaction::ControlType::Commit
                                              : rune::transaction::ControlType::Abort, 0));
            } else {
                put(plain);
            }
            if (i > 2'000'000) break;
        }
    }
};
FullRing& fullRing() { static FullRing r; return r; }

void BM_RcRingRebuildStep(benchmark::State& state) {
    auto& ring = fullRing();
    const auto budget = static_cast<int32_t>(state.range(0));
    rune::storage::ProducerStateManager psm;
    rune::node::RcState rc;
    rc.reserve(256);
    rc.reserveRingRebuild();
    rc.bindProducerState(&psm, 1, 0);
    rune::node::RcFacts f;
    f.leader = true; f.epoch = 1;
    std::vector<double> us;
    us.reserve(1u << 20);
    std::size_t bytes = 0;
    for (auto _ : state) {
        if (rc.ready()) ++f.logGeneration;   // the next step starts a fresh build (reset + first step)
        const auto t0 = std::chrono::steady_clock::now();
        bytes += rc.advance(f, nullptr, budget, &ring.mem);
        const auto t1 = std::chrono::steady_clock::now();
        us.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
    }
    std::sort(us.begin(), us.end());
    state.counters["p50_us"] = us[us.size() / 2];
    state.counters["p99_us"] = us[std::min(us.size() - 1, us.size() * 99 / 100)];
    state.counters["max_us"] = us.back();
    state.counters["ring_batches"] = static_cast<double>(ring.mem.logEndOffset() - ring.mem.logStartOffset());
    state.SetBytesProcessed(static_cast<int64_t>(bytes));
}

void BM_RcRingRebuildFull(benchmark::State& state) {
    auto& ring = fullRing();
    const auto budget = static_cast<int32_t>(state.range(0));
    rune::storage::ProducerStateManager psm;
    rune::node::RcState rc;
    rc.reserve(256);
    rc.reserveRingRebuild();
    rc.bindProducerState(&psm, 1, 0);
    rune::node::RcFacts f;
    f.leader = true; f.epoch = 1;
    uint64_t steps = 0;
    double maxStepUs = 0;
    for (auto _ : state) {
        ++f.logGeneration;   // a fresh build
        do {
            const auto t0 = std::chrono::steady_clock::now();
            (void)rc.advance(f, nullptr, budget, &ring.mem);
            const auto t1 = std::chrono::steady_clock::now();
            maxStepUs = std::max(maxStepUs, std::chrono::duration<double, std::micro>(t1 - t0).count());
            ++steps;
        } while (!rc.ready());
    }
    state.counters["steps_per_rebuild"] = static_cast<double>(steps) / static_cast<double>(state.iterations());
    state.counters["max_step_us"] = maxStepUs;
}

void BM_RcTransientFailover(benchmark::State& state) {
    const int k = static_cast<int>(state.range(0));
    auto fill = [](rune::storage::InMemoryLog& mem, int64_t pidBase) {
        auto build = [](int64_t pid, int32_t seq, bool txn) {
            rune::protocol::RecordBatchBuilder b(0, 1000);
            if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq); }
            if (txn) b.setTransactional(true);
            b.append("", "", 1000);
            return b.build();
        };
        auto put = [&](const std::vector<uint8_t>& b) {
            int64_t base = -1;
            (void)mem.appendNoSyncRestamp(b.data(), b.size(),
                                          rune::protocol::RecordBatchReader(b.data(), b.size()).lastOffsetDelta(),
                                          base);
        };
        const auto plain = build(-1, -1, false);
        int32_t seq[8] = {};
        for (int64_t i = 0; mem.logStartOffset() == 0 || i < 150'000; ++i) {
            if (i % 1000 == 0) {
                const int j = static_cast<int>((i / 1000) % 8);
                put(build(pidBase + j, seq[j]++, true));
                if ((i / 1000) % 3 != 2)
                    put(rune::transaction::ControlMarker::build(
                        pidBase + j, 0,
                        ((i / 1000) % 3) == 0 ? rune::transaction::ControlType::Commit
                                              : rune::transaction::ControlType::Abort, 0));
            } else {
                put(plain);
            }
            if (i > 2'000'000) break;
        }
    };
    double maxPass = 0, p99Pass = 0, allReady = 0, passes = 0;
    for (auto _ : state) {
        state.PauseTiming();
        std::vector<std::unique_ptr<rune::storage::InMemoryLog>> mems;
        rune::replication::NullPartitionTransport transport;
        rune::node::ReplicaRegistry reg;
        rune::node::WorkerPool pool{1, rune::metadata::ShardRouter(1)};
        for (int p = 0; p < k; ++p) {
            mems.push_back(std::make_unique<rune::storage::InMemoryLog>(8u << 20, 1'000'000));
            fill(*mems.back(), 1000 + 10 * p);
            rune::replication::PartitionConfig c;
            c.topicId = 1; c.partitionId = p; c.selfId = 1; c.minIsr = 1;
            c.replicaLagMaxOffset = 1'000'000'000; c.replicaLagMaxMs = 600'000; c.followerFlushIntervalNs = 0;
            auto r = std::make_unique<rune::replication::PartitionReplica>(
                c, rune::replication::Role::Follower, 1, std::vector<int32_t>{1, 2}, std::vector<int32_t>{1, 2},
                mems.back().get(), &transport);
            r->setExternalFollowerFlush(true);
            reg.add(1, p, std::move(r));
        }
        pool.setRegistry(&reg);
        rune::node::Worker& w = pool.worker(0);
        w.setWorkerIngress(nullptr, true);
        w.setShardMode(true);
        pool.assignFromRegistry(reg);
        int64_t now = 1'000'000'000;
        for (int i = 0; i < 8; ++i) { now += 1'000'000; (void)w.doWork(now); pool.drainAllReturns([](rune::node::WorkerReturn) {}); }
        for (int p = 0; p < k; ++p) {
            rune::node::WorkerOp op;
            op.type = rune::node::WorkerOp::Type::BecomeLeader;
            op.topicId = 1; op.partition = p; op.roleEpoch = 2; op.isr = {1};
            (void)pool.tryEnqueueOp(1, p, std::move(op));
        }
        std::vector<double> us;
        us.reserve(1u << 16);
        state.ResumeTiming();
        const auto t0 = std::chrono::steady_clock::now();
        for (int i = 0; i < 200'000; ++i) {
            now += 1'000;
            const auto a = std::chrono::steady_clock::now();
            (void)w.doWork(now);
            const auto b = std::chrono::steady_clock::now();
            us.push_back(std::chrono::duration<double, std::micro>(b - a).count());
            pool.drainAllReturns([](rune::node::WorkerReturn) {});
            bool all = true;
            for (int p = 0; p < k && all; ++p) all = w.rcStatusForTest(1, p) == 2;
            if (all) { allReady = std::chrono::duration<double, std::micro>(b - t0).count(); break; }
        }
        state.PauseTiming();
        std::sort(us.begin(), us.end());
        maxPass = us.back();
        p99Pass = us[std::min(us.size() - 1, us.size() * 99 / 100)];
        passes  = static_cast<double>(us.size());
        state.ResumeTiming();
    }
    state.counters["max_pass_us"]  = maxPass;
    state.counters["p99_pass_us"]  = p99Pass;
    state.counters["all_ready_us"] = allReady;
    state.counters["passes"]       = passes;
}
#endif

} // namespace

BENCHMARK(BM_RcPushNonTxn)->Arg(0)->Arg(1)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_RcWorkerPush)->Arg(0)->Arg(1)->Unit(benchmark::kMicrosecond);
#if defined(RUNE_RCBENCH_S4)
BENCHMARK(BM_RcBuildStep)->Arg(16 * 1024)->Arg(32 * 1024)->Arg(64 * 1024)->Arg(256 * 1024)
    ->Unit(benchmark::kMicrosecond)->Iterations(20000);
BENCHMARK(BM_RcRingRebuildStep)->Arg(32 * 1024)->Unit(benchmark::kMicrosecond)->Iterations(20000);
BENCHMARK(BM_RcRingRebuildFull)->Arg(32 * 1024)->Arg(2147483647)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_RcTransientFailover)->Arg(1)->Arg(10)->Unit(benchmark::kMillisecond)->Iterations(3);
#endif
