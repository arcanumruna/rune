// FilterPushBench.cpp - Phase 2c Task 16: the cost of broker-side subscription filters.
//
//   BM_FilterIndexLookup/<subs>/<hit>  - the per-record cost of the shared scan's probe: the record accessor (version +
//                              field slot), the key load and FilterIndex::forEach, over a 64 Ki-record batch of
//                              player_event values.  subs distinct worldIds are registered; hit 1 = every record's
//                              world is registered, hit 0 = none is.  Gates: ns_per_record <= 5.0 per arm, and the 500
//                              arm within 10% of the 1 arm (the probe is O(1) in the number of subscribers).
//   BM_FilterRebuild/<pct>             - rebuildBatch over a 256-record, 1 KiB-value batch keeping pct% of the records:
//                              ns_per_matched_byte (reported) and allocs (gate: 0).
//   BM_FilterWorkerPush/<subs>         - a real Worker with an embedded-driver IPC egress, one partition, subs filtered
//                              subscriptions (distinct worldIds) and 1 unfiltered control.  Each iteration appends a
//                              64-record batch and times append -> the first matching frame polled on the subscriber.
//                              Gates: p50_us <= 50, p99_us <= 200, allocs == 0, and ns_per_record_broker(500) within 10%
//                              of ns_per_record_broker(1).
//
// The worker arm keeps the MATCHED work constant across arms (half of each batch belongs to subscription 1's world, the
// other half to a world nobody follows), so what varies between 1, 50 and 500 is only the number of idle subscribers:
// the flatness gate measures exactly the claim "cost is O(matches), not O(subscribers)".
#include "BenchGate.h"

#include "FilterFakes.h"

#include <rune/codec/Filter.h>
#include <rune/codec/FilterPredicate.h>
#include <rune/codec/Serialize.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/filter/FilterIndex.h>
#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <benchmark/benchmark.h>
#include <Aeron.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

extern std::atomic<int64_t> g_allocCount;
extern std::atomic<bool>    g_allocEnabled;

namespace {

namespace fs = std::filesystem;
namespace pc = rune::protocol::client;
using namespace rune::node::filter::fakes;
using rune::node::Worker;
using rune::node::WorkerOp;

[[nodiscard]] int64_t steadyNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

constexpr int32_t  kTopic     = 10;
constexpr uint16_t kVer       = 2;
constexpr uint32_t kMissWorld = 1'000'000;   // a world nobody follows

// ---------------------------------------------------------------------------------------------------------------
// BM_FilterIndexLookup
// ---------------------------------------------------------------------------------------------------------------
double gLookupNs1[2] = {-1.0, -1.0};   // ns_per_record of the 1-subscriber arm, per hit (thread 0 only)

void BM_FilterIndexLookup(benchmark::State& state) {
    const auto subs = static_cast<uint32_t>(state.range(0));
    const bool hit  = state.range(1) != 0;
    constexpr std::size_t kRecords = 64 * 1024;

    rune::node::filter::FilterIndex idx;
    idx.init(4096, 4096);
    for (uint32_t s = 0; s < subs; ++s) {
        if (!idx.add(rune::codec::FilterKey{0, s}, s)) { state.SkipWithError("index add refused"); return; }
    }
    const rune::codec::FilterSchema schema = playerSchema(kVer);
    const int32_t field = schema.indexOfTag(1);   // worldId
    const rune::codec::FieldSlot slot = schema.slot(field, kVer);
    PlayerRow row;
    row.playerId = 1; row.worldId = 0; row.channel = 1; row.guild = 3;
    const std::vector<uint8_t> tmpl = encodePlayerValue(kVer, row);
    const std::size_t stride = tmpl.size();
    std::vector<uint8_t> buf(stride * kRecords);
    for (std::size_t i = 0; i < kRecords; ++i) {
        uint8_t* v = buf.data() + i * stride;
        std::memcpy(v, tmpl.data(), stride);
        const uint32_t world = hit ? static_cast<uint32_t>(i % subs) : kMissWorld + static_cast<uint32_t>(i);
        std::memcpy(v + slot.offset, &world, sizeof world);
    }

    uint64_t sink = 0;
    auto pass = [&] {
        const uint8_t* p = buf.data();
        for (std::size_t i = 0; i < kRecords; ++i, p += stride) {
            const uint16_t ver = rune::codec::recordVersion(p);
            const rune::codec::FieldSlot sl = schema.slot(field, ver);
            if (!sl.present || static_cast<std::size_t>(sl.offset) + sl.width > stride) continue;
            const rune::codec::FilterKey key = rune::codec::loadFilterKey(p + sl.offset, sl.width);
            idx.forEach(key, [&](uint32_t member) { sink += member + 1; });
        }
    };
    pass();   // warm
    int64_t bestPassNs = INT64_MAX;   // the fastest single pass: the flatness gate compares these (immune to load spikes)
    const int64_t t0 = steadyNs();
    for (auto _ : state) {
        const int64_t a = steadyNs();
        pass();
        bestPassNs = std::min(bestPassNs, steadyNs() - a);
    }
    const int64_t t1 = steadyNs();
    benchmark::DoNotOptimize(sink);

    const double records = static_cast<double>(state.iterations()) * static_cast<double>(kRecords);
    const double ns = static_cast<double>(t1 - t0) / records;
    const double bestNs = static_cast<double>(bestPassNs) / static_cast<double>(kRecords);
    state.counters["ns_per_record"] = ns;
    state.counters["best_ns_per_record"] = bestNs;
    state.SetItemsProcessed(static_cast<int64_t>(records));

    const bool ok = ns <= 5.0;
    if (subs == 1) gLookupNs1[hit ? 1 : 0] = bestNs;
    bool flat = true;
    if (subs == 500 && gLookupNs1[hit ? 1 : 0] > 0.0) flat = bestNs <= 1.10 * gLookupNs1[hit ? 1 : 0];
    if (state.thread_index() == 0)
        rune::bench::gateCheck(state, ok && flat, !ok ? "ns_per_record > 5.0" : "ns_per_record(500) > 1.10 x ns_per_record(1)");
}

// ---------------------------------------------------------------------------------------------------------------
// BM_FilterRebuild
// ---------------------------------------------------------------------------------------------------------------
void BM_FilterRebuild(benchmark::State& state) {
    const auto pct = static_cast<int>(state.range(0));
    constexpr int kRecs = 256;
    rune::protocol::RecordBatchBuilder b(0, 1000);
    for (int i = 0; i < kRecs; ++i) b.append("", std::string(1024, static_cast<char>('a' + i % 26)), 1000);
    const std::vector<uint8_t> src = b.build();

    std::vector<rune::protocol::RecordSpan> spans;
    spans.reserve(kRecs);
    if (!rune::protocol::forEachRecordSpan(src.data(), src.size(),
                                           [&](const rune::protocol::RecordSpan& r) { spans.push_back(r); })) {
        state.SkipWithError("batch walk failed");
        return;
    }
    std::vector<uint32_t> keep;
    keep.reserve(kRecs);
    std::size_t matchedBytes = 0;
    for (uint32_t i = 0; i < spans.size(); ++i)
        if (static_cast<int>(i % 100) < pct) { keep.push_back(i); matchedBytes += spans[i].len; }
    if (keep.empty()) { state.SkipWithError("empty keep"); return; }
    std::vector<uint8_t> out(src.size() + 64);

    std::size_t sink = 0;
    for (int i = 0; i < 64; ++i) sink += rune::protocol::rebuildBatch(src, spans, keep, out);   // warm
    g_allocCount.store(0);
    g_allocEnabled.store(true, std::memory_order_release);
    const int64_t t0 = steadyNs();
    for (auto _ : state) {
        sink += rune::protocol::rebuildBatch(src, spans, keep, out);
        benchmark::ClobberMemory();
    }
    const int64_t t1 = steadyNs();
    g_allocEnabled.store(false, std::memory_order_release);
    const int64_t allocs = g_allocCount.load(std::memory_order_acquire);
    benchmark::DoNotOptimize(sink);

    state.counters["ns_per_matched_byte"] =
        static_cast<double>(t1 - t0) / (static_cast<double>(state.iterations()) * static_cast<double>(matchedBytes));
    state.counters["allocs"] = static_cast<double>(allocs);
    state.counters["matched_bytes"] = static_cast<double>(matchedBytes);
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * matchedBytes));
    if (state.thread_index() == 0) rune::bench::gateCheck(state, allocs == 0, "rebuildBatch allocated");
}

// ---------------------------------------------------------------------------------------------------------------
// BM_FilterWorkerPush
// ---------------------------------------------------------------------------------------------------------------
struct BenchDir {
    fs::path path;
    BenchDir() {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() / ("rune_filterbench_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~BenchDir() { std::error_code ec; fs::remove_all(path, ec); }
};

class NullTransport final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};

struct FilterBuf {
    rune::codec::FilterBytes fb;
    std::size_t n = 0;
    [[nodiscard]] std::span<const uint8_t> view() const { return {fb.bytes.data(), n}; }
};
FilterBuf worldEq(uint32_t w) {
    FilterBuf f;
    rune::codec::FilterWriter fw(std::span<uint8_t>(f.fb.bytes.data(), f.fb.bytes.size()));
    fw.term(1, rune::codec::PrimType::UInt32, 4, rune::Match<uint32_t>(w));
    auto n = fw.finish();
    f.n = n ? *n : 0;
    return f;
}

double gPushNs1 = -1.0;   // ns_per_record_broker of the 1-subscriber arm (thread 0 only)

void BM_FilterWorkerPush(benchmark::State& state) {
    const auto subs = static_cast<uint32_t>(state.range(0));
    constexpr int kRows = 64;
    constexpr int64_t kControlSub = 1'000'000;
    constexpr int kWarm = 400;
    constexpr int kMaxPasses = 200'000;

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
    const int64_t pubReg = aeron->addExclusivePublication("aeron:ipc", 9301);
    const int64_t subReg = aeron->addSubscription("aeron:ipc", 9301);
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
    rune::replication::PartitionConfig c;
    c.topicId = kTopic; c.partitionId = 0; c.selfId = 1; c.minIsr = 1; c.pushOnAppend = false;
    c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000LL;
    NullTransport t;
    rune::replication::PartitionReplica rep(c, rune::replication::Role::Leader, 1, {1}, {1}, &log, &t);
    rep.onLeaderLogAppended();
    Worker w(0, 1 << 22, 1 << 23, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    for (int i = 0; i < 8; ++i) w.doWork(steadyNs());

    auto enqueue = [&](auto&& make) {
        for (int i = 0; i < 100'000; ++i) {
            if (w.tryEnqueueOp(make())) return true;
            w.doWork(steadyNs());
        }
        return false;
    };
    if (!enqueue([&] {
            WorkerOp op;
            op.type = WorkerOp::Type::FilterSchema;
            op.topicId = kTopic;
            op.clientVersion = kVer;
            op.schemaUpdateBlob = rune::codec::serialize(playerEvent(kVer));
            return op;
        })) { state.SkipWithError("schema op never enqueued"); return; }
    w.doWork(steadyNs());

    std::vector<FilterBuf> filters;
    filters.reserve(subs);
    for (uint32_t s = 0; s < subs; ++s) filters.push_back(worldEq(s));
    auto subscribeOp = [&](int64_t id, const FilterBuf* f) {
        WorkerOp op;
        op.type = WorkerOp::Type::Subscribe;
        op.subscriptionId = id; op.sessionId = 4200 + id; op.correlationId = 9000 + id;
        op.topicId = kTopic; op.partition = 0; op.nextOffset = 0;
        op.logPtr = &log; op.replica = &rep; op.isolation = 0;
        op.egressPub = pub.get();
        if (f != nullptr) op.filterBytes = f->view();
        return op;
    };
    for (uint32_t s = 0; s < subs; ++s)
        if (!enqueue([&] { return subscribeOp(static_cast<int64_t>(s) + 1, &filters[s]); })) {
            state.SkipWithError("subscribe op never enqueued");
            return;
        }
    if (!enqueue([&] { return subscribeOp(kControlSub, nullptr); })) { state.SkipWithError("control subscribe never enqueued"); return; }
    for (int i = 0; i < 16; ++i) w.doWork(steadyNs());

    // The batch: half subscription 1's world (worldId 0), half a world nobody follows.  Pre-built; the append restamps the base.
    std::vector<PlayerRow> rows;
    for (int i = 0; i < kRows; ++i) {
        PlayerRow r;
        r.playerId = static_cast<uint64_t>(i) + 1; r.channel = 1; r.guild = 3;
        r.worldId = (i % 2 == 0) ? 0u : kMissWorld + static_cast<uint32_t>(i);
        rows.push_back(r);
    }
    const std::vector<uint8_t> batch = makePlayerBatch(0, kVer, rows);

    bool gotMatch = false, gotControl = false;
    int64_t tMatch = 0;
    // IPC fragments at the MTU: reassemble whole messages (frames of ~1 KiB and more).
    aeron::FragmentAssembler assembler([&](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off, aeron::util::index_t len, aeron::Header&) {
        const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + off);
        const auto r = pc::Records::decode(p, static_cast<std::size_t>(len));
        if (!r) return;
        if (r->recordBatch.empty()) return;   // a position note
        if (r->subscriptionId == 1 && !gotMatch) { gotMatch = true; tMatch = steadyNs(); }
        else if (r->subscriptionId == kControlSub) gotControl = true;
    });
    auto handler = assembler.handler();
    auto drain = [&] { while (sub->poll(handler, 256) > 0) {} };

    int64_t passNs = 0;
    uint64_t passes = 0;
    int64_t latencyNs = 0;
    uint64_t allocs = 0;
    // One iteration; false when the frames never arrived.  Accumulates the worker's pass time (and, measured, its allocations).
    auto iteration = [&](bool measured) -> bool {
        gotMatch = gotControl = false;
        tMatch = 0;
        const int64_t t0 = steadyNs();
        int64_t base = 0;
        (void)log.appendNoSyncRestamp(batch.data(), batch.size(), kRows - 1, base);
        rep.onLeaderLogAppended();
        for (int p = 0; p < kMaxPasses && !(gotMatch && gotControl); ++p) {
            const int64_t before = g_allocCount.load(std::memory_order_relaxed);
            if (measured) g_allocEnabled.store(true, std::memory_order_relaxed);
            const int64_t a = steadyNs();
            w.doWork(a);
            const int64_t b = steadyNs();
            if (measured) {
                g_allocEnabled.store(false, std::memory_order_relaxed);
                allocs += static_cast<uint64_t>(g_allocCount.load(std::memory_order_relaxed) - before);
            }
            passNs += b - a;
            ++passes;
            sub->poll(handler, 64);
        }
        if (!(gotMatch && gotControl)) return false;
        latencyNs = tMatch - t0;
        drain();
        return true;
    };

    for (int i = 0; i < kWarm; ++i)
        if (!iteration(false)) { state.SkipWithError("warm-up: frames never arrived"); return; }

    std::vector<double> lat;
    lat.reserve(1 << 20);
    passNs = 0;
    passes = 0;
    allocs = 0;
    uint64_t iters = 0;
    for (auto _ : state) {
        if (!iteration(true)) { state.SkipWithError("frames never arrived"); break; }
        lat.push_back(static_cast<double>(latencyNs) / 1e3);
        ++iters;
    }
    if (iters == 0) return;
    std::sort(lat.begin(), lat.end());
    const double p50 = lat[lat.size() / 2];
    const double p99 = lat[std::min(lat.size() - 1, static_cast<std::size_t>(static_cast<double>(lat.size()) * 0.99))];
    const double nsPerRec = static_cast<double>(passNs) / (static_cast<double>(iters) * kRows);
    state.counters["p50_us"] = p50;
    state.counters["p99_us"] = p99;
    state.counters["ns_per_record_broker"] = nsPerRec;
    state.counters["allocs"] = static_cast<double>(allocs);
    state.counters["passes_per_iter"] = static_cast<double>(passes) / static_cast<double>(iters);
    state.SetItemsProcessed(static_cast<int64_t>(iters) * kRows);

    if (subs == 1) gPushNs1 = nsPerRec;
    const char* msg = nullptr;
    if (p50 > 50.0) msg = "p50_us > 50";
    else if (p99 > 200.0) msg = "p99_us > 200";
    else if (allocs != 0) msg = "allocs != 0";
    else if (subs == 500 && gPushNs1 > 0.0 && nsPerRec > 1.10 * gPushNs1)
        msg = "ns_per_record_broker(500) > 1.10 x ns_per_record_broker(1)";
    if (state.thread_index() == 0) rune::bench::gateCheck(state, msg == nullptr, msg != nullptr ? msg : "");
}

}  // namespace

BENCHMARK(BM_FilterIndexLookup)
    ->Args({1, 0})->Args({1, 1})->Args({50, 0})->Args({50, 1})->Args({500, 0})->Args({500, 1})
    ->Iterations(2000)->Unit(benchmark::kMicrosecond);   // fixed: the flatness gate compares whole runs, never a calibration probe
BENCHMARK(BM_FilterRebuild)->Arg(1)->Arg(10)->Arg(50)->Unit(benchmark::kNanosecond);
BENCHMARK(BM_FilterWorkerPush)->Arg(1)->Arg(50)->Arg(500)->Iterations(20000)->Unit(benchmark::kMicrosecond);
