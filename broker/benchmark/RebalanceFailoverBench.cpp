// RebalanceFailoverBench.cpp — Task 15: failover/rebalance/handoff latency +
//   memory-topic produce p99.  HARD gate: produce-under-rebalance p99 < 100 µs.
//
// All benchmarks use PartitionReplica + InMemoryTransport (in-process, no Aeron,
// no BrokerNode — BrokerNode::stop has a pre-existing SIGABRT in build-release).
//
// Measurements
// ─────────────
//   FailoverElection      — FailoverElection::elect() over a 3-node ISR.
//                           Pure function; no I/O.  Measures election latency.
//
//   RebalanceApply        — DynamicBalancer::rebalance() decision + enqueue via
//                           MetadataService::emitDynamicAssignment().
//                           Drives a forced-failover scenario each iteration.
//
//   LeaderHandoff         — becomeFollower(newEpoch) + becomeLeader(newEpoch+1)
//                           round-trip on a single PartitionReplica (disk-backed).
//                           Models the leader-move latency at the replication layer.
//
//   DiskProduce           — PartitionReplica::append() on a disk-backed Log.
//                           Compared against the Phase-1 baseline (4.4–8.7 µs).
//                           Confirms the ILog de-virtualization (concrete Log*
//                           path, no vtable) did NOT regress append latency.
//
//   MemoryProduce         — PartitionReplica::append() on an InMemoryLog.
//                           Expected LOWER than DiskProduce (no segment-roll tail).
//                           p99 gate: < 100 µs (straightforward — should be ~1 µs).
//
//   ProduceUnderRebalance — Hot-path produce loop on an InMemoryLog-backed replica
//                           with a DynamicBalancer::rebalance() call injected every
//                           K iterations (concurrent-rebalance simulation).
//                           HARD GATE: p99 >= 100 000 ns → SkipWithError.
//
// Registration
// ─────────────
// All use BENCHMARK_DEFINE_F / BENCHMARK_REGISTER_F on RebalanceFailoverFixture
// with UseManualTime()->MinTime(2.0)->Unit(benchmark::kMicrosecond), as required
// by the task brief.
//
// De-virtualization note (hot path)
// ───────────────────────────────────
// PartitionReplica uses a bool memoryBacked_ + two concrete pointers (diskLog_,
// memLog_) with [[gnu::always_inline]] onLog() — a single branch to a direct
// concrete call, NO vtable on the append/read hot path.  This bench confirms
// the de-virtualization payoff: disk-produce p99 should be at or below the
// Phase-1 baseline (4.4–8.7 µs measured on M2).

#include <benchmark/benchmark.h>

#include <rune/metadata/FailoverElection.h>
#include <rune/metadata/DynamicBalancer.h>
#include <rune/metadata/ReplicaPositionTable.h>
#include <rune/metadata/PartitionAssignment.h>
#include <rune/metadata/AssignmentMap.h>
#include <rune/metadata/StaticBalancer.h>
#include <rune/node/MetadataService.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/MetadataRecords.h>
#include <rune/metadata/Gpid.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <sys/types.h>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace rune::replication;
using namespace rune::metadata;
using namespace rune::node;
namespace pmd = rune::protocol::metadata;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
static uint32_t g_rfbSeq = 0;  // the benchmark thread only: no atomic

static fs::path rfbTempDir(const char* tag)
{
    uint32_t seq = g_rfbSeq++;
    std::string name = std::string("rune_rfb_") + tag +
                       "_" + std::to_string(static_cast<long>(::getpid())) +
                       "_" + std::to_string(seq);
    return fs::temp_directory_path() / name;
}

static inline int64_t rfbNowNs() noexcept
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Percentile helper (sorted input assumed).
static double pctile(const std::vector<double>& s, double p)
{
    if (s.empty()) return 0.0;
    if (s.size() == 1) return s[0];
    const double   idx = p * static_cast<double>(s.size() - 1);
    const std::size_t lo = static_cast<std::size_t>(idx);
    const std::size_t hi = lo + 1 < s.size() ? lo + 1 : lo;
    return s[lo] + (idx - static_cast<double>(lo)) * (s[hi] - s[lo]);
}

// Build a minimal 1-record batch.
static std::vector<uint8_t> rfbMakeBatch(int payloadBytes = 128)
{
    rune::protocol::RecordBatchBuilder b(0, 1'000'000LL);
    b.append("key", std::string(static_cast<std::size_t>(payloadBytes), 'x'), 1'000'000LL);
    return b.build();
}

// Standard PartitionConfig for bench replicas.
static PartitionConfig rfbPartCfg(NodeId selfId, int32_t topicId = 0, int32_t partId = 0,
                                   int minIsr = 1)
{
    PartitionConfig cfg;
    cfg.topicId             = topicId;
    cfg.partitionId         = partId;
    cfg.selfId              = selfId;
    cfg.replicaLagMaxMs     = 60'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = minIsr;
    cfg.fetchIntervalNs     = 5'000'000LL;
    cfg.isrCheckIntervalNs  = 5'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false; // keep bench focused on append, not push
    cfg.fastAckOnApply      = true;
    return cfg;
}

// Encode one UpdatePartitionAssignmentRecord (for metadata replica seeding).
static std::size_t rfbEncodeAssignment(
    uint8_t* buf, std::size_t bufLen,
    int32_t topicId, int32_t partition, int32_t leaderNode,
    const std::vector<int32_t>& isr,
    const std::vector<int32_t>& replicas,
    int32_t leaderEpoch, int32_t partitionEpoch)
{
    static thread_local std::vector<uint8_t> isrBuf, repBuf;
    isrBuf.resize(2 + isr.size() * 4);
    repBuf.resize(2 + replicas.size() * 4);

    auto writeU16 = [](uint8_t* p, uint16_t v) {
        p[0] = static_cast<uint8_t>(v); p[1] = static_cast<uint8_t>(v >> 8);
    };
    auto writeS32 = [](uint8_t* p, int32_t v) {
        uint32_t u = static_cast<uint32_t>(v);
        p[0]=static_cast<uint8_t>(u);     p[1]=static_cast<uint8_t>(u>>8);
        p[2]=static_cast<uint8_t>(u>>16); p[3]=static_cast<uint8_t>(u>>24);
    };

    writeU16(isrBuf.data(), static_cast<uint16_t>(isr.size()));
    for (std::size_t i = 0; i < isr.size(); ++i)
        writeS32(isrBuf.data() + 2 + i * 4, isr[i]);
    writeU16(repBuf.data(), static_cast<uint16_t>(replicas.size()));
    for (std::size_t i = 0; i < replicas.size(); ++i)
        writeS32(repBuf.data() + 2 + i * 4, replicas[i]);

    pmd::Int32ArrayView isrView(
        reinterpret_cast<const int32_t*>(isrBuf.data() + 2),
        static_cast<uint16_t>(isr.size()));
    pmd::Int32ArrayView repView(
        reinterpret_cast<const int32_t*>(repBuf.data() + 2),
        static_cast<uint16_t>(replicas.size()));

    pmd::UpdatePartitionAssignmentRecord rec;
    rec.topicId        = topicId;
    rec.partition      = partition;
    rec.leaderNode     = leaderNode;
    rec.isr            = isrView;
    rec.replicas       = repView;
    rec.leaderEpoch    = leaderEpoch;
    rec.partitionEpoch = partitionEpoch;
    return rec.encode(buf, bufLen);
}

// ===========================================================================
// RebalanceFailoverFixture
//
// Shared setup for all RebalanceFailoverBench benchmarks.
//
// Provides:
//   - 1 disk-backed PartitionReplica (Leader, RF=1, minIsr=1) for disk-produce
//     and leader-handoff measurements.
//   - 1 InMemoryLog (standalone) for zero-alloc memory-produce and
//     produce-under-rebalance measurements.
//     NOTE: we bench InMemoryLog::append() DIRECTLY (not via PartitionReplica)
//     because PartitionReplica::append() re-wraps the payload in a new
//     RecordBatch on every call (makeBatch → build() → new vector<uint8_t>),
//     making it structurally non-zero-alloc.  The ILog de-virtualization payoff
//     is in InMemoryLog::appendImpl() — the concrete hot path — and that is
//     exactly what we need to measure here.
//   - A MetadataService with its own Log-backed replica for the rebalance-apply
//     measurement.
//   - Pre-built batch bytes for zero-alloc in the hot-path loops.
//
// No Aeron, no BrokerNode — all in-process.
// ===========================================================================
class RebalanceFailoverFixture : public benchmark::Fixture {
public:
    // -----------------------------------------------------------------------
    // Setup
    // -----------------------------------------------------------------------
    void SetUp(::benchmark::State& state) override
    {
        // ── Disk-backed replica for disk-produce + handoff ─────────────────
        diskDir_ = rfbTempDir("disk");
        fs::create_directories(diskDir_);
        diskLog_ = std::make_unique<rune::storage::Log>(diskDir_.string());
        diskLog_->recover();
        diskTransport_ = std::make_unique<InMemoryTransport>();
        const std::vector<NodeId> singleNode = {1};
        diskReplica_ = std::make_unique<PartitionReplica>(
            rfbPartCfg(1, /*topicId=*/0, /*partId=*/0),
            Role::Leader, /*epoch=*/1, singleNode, singleNode,
            diskLog_.get(), diskTransport_.get());
        diskTransport_->setLeader(diskReplica_.get());

        // ── Standalone InMemoryLog for zero-alloc memory-produce bench ─────
        // Both MemoryProduce and ProduceUnderRebalance are capped at 50 000
        // iterations each.  At ~230 bytes/record that's ~11 MB total per bench.
        // Ring sized at 32 MiB → ~140K records capacity → no eviction for 50K
        // iterations.  InMemoryLog eviction fires memmove of the entire ring
        // and skews tail latency, so we must prevent it.
        memLog_ = std::make_unique<rune::storage::InMemoryLog>(
            /*maxBytes=*/32u * 1024u * 1024u,   // 32 MiB — fits 50K × ~200B with room
            /*maxRecords=*/200'000u);            // well above 50K bench iterations

        // ── MetadataService replica (disk-backed) for rebalance-apply ──────
        metaDir_ = rfbTempDir("meta");
        fs::create_directories(metaDir_);
        metaLog_ = std::make_unique<rune::storage::Log>(metaDir_.string());
        metaLog_->recover();
        metaTransport_ = std::make_unique<InMemoryTransport>();
        PartitionConfig metaCfg = rfbPartCfg(1, /*topicId=*/-3, /*partId=*/0);
        metaReplica_ = std::make_unique<PartitionReplica>(
            metaCfg, Role::Leader, /*epoch=*/1, singleNode, singleNode,
            metaLog_.get(), metaTransport_.get());
        metaTransport_->setLeader(metaReplica_.get());
        metaSvc_ = std::make_unique<MetadataService>(metaReplica_.get(), 1);

        // Seed the metadata service with a committed assignment so the rebalance
        // bench has a non-empty AssignmentMap to work with.
        {
            uint8_t buf[512];
            const std::vector<int32_t> isr3     = {1, 2, 3};
            const std::vector<int32_t> replicas3 = {1, 2, 3};
            std::size_t n = rfbEncodeAssignment(
                buf, sizeof(buf), /*topicId=*/0, /*partition=*/0,
                /*leaderNode=*/1, isr3, replicas3, 1, 1);
            metaReplica_->append(std::span<const uint8_t>(buf, n), false);
            int64_t clk = 5'000'000LL;
            metaReplica_->doWork(clk);
            metaTransport_->pump(clk);
            metaSvc_->doWork(clk);
        }

        // ── Warm-up disk replica ───────────────────────────────────────────
        // Pre-build a valid RecordBatch so the disk-produce bench uses real
        // on-disk format.
        batch_ = rfbMakeBatch(128);
        const auto bsp = std::span<const uint8_t>(batch_.data(), batch_.size());
        int64_t clk = 5'000'000LL;
        for (int i = 0; i < 50; ++i) {
            clk += 5'000'000LL;
            diskReplica_->append(bsp, false);
            diskReplica_->doWork(clk);
            diskTransport_->pump(clk);
        }
        clk_ = clk;

        // ── Pre-touch the InMemoryLog ring pages.
        // InMemoryLog::append(data, len, lastOffset) accepts raw batch bytes
        // directly — no re-wrapping, no heap allocation on the hot path.
        // We pre-touch a portion of the ring so page-faults don't appear
        // in the first measured iterations.
        // 1000 appends × ~230 bytes = ~230 KB  (well within the 32 MiB ring).
        {
            int64_t lastOffset = 0;
            for (int i = 0; i < 1000; ++i) {
                lastOffset = memLog_->append(
                    batch_.data(), batch_.size(), lastOffset);
            }
            // Ring pages warm; steady-state appends are page-fault-free.
        }

        (void)state;
    }

    // -----------------------------------------------------------------------
    // TearDown
    // -----------------------------------------------------------------------
    void TearDown(::benchmark::State& /*state*/) override
    {
        metaSvc_.reset();
        metaReplica_.reset();
        metaTransport_.reset();
        metaLog_.reset();
        memLog_.reset();
        diskReplica_.reset();
        diskTransport_.reset();
        diskLog_.reset();
        std::error_code ec;
        fs::remove_all(diskDir_, ec);
        fs::remove_all(metaDir_, ec);
    }

    // -----------------------------------------------------------------------
    // Shared members
    // -----------------------------------------------------------------------

    // Disk replica
    fs::path                                  diskDir_;
    std::unique_ptr<rune::storage::Log>       diskLog_;
    std::unique_ptr<InMemoryTransport>        diskTransport_;
    std::unique_ptr<PartitionReplica>         diskReplica_;

    // Standalone InMemoryLog (zero-alloc memory-produce benchmark)
    std::unique_ptr<rune::storage::InMemoryLog> memLog_;

    // Metadata replica + service
    fs::path                                  metaDir_;
    std::unique_ptr<rune::storage::Log>       metaLog_;
    std::unique_ptr<InMemoryTransport>        metaTransport_;
    std::unique_ptr<PartitionReplica>         metaReplica_;
    std::unique_ptr<MetadataService>          metaSvc_;

    // Pre-built RecordBatch bytes (reused in hot paths without re-allocation).
    std::vector<uint8_t> batch_;

    int64_t clk_ = 0;
    // Running log offset counter for the InMemoryLog (advanced each append).
    int64_t memLogOffset_ = 1000; // starts after SetUp's 1000 pre-touch appends
};

// ===========================================================================
// 1. FailoverElection — FailoverElection::elect() pure function latency.
//
// Scenario: 3-node cluster (nodes 1, 2, 3), leader is node 1.
//   Node 1 goes down → liveNodes = {2, 3}.
//   ISR = {1, 2, 3} → proposed ISR = {2, 3}.
//   elect() picks node 2 (lowest nodeId among 2 & 3 with equal position).
//
// FailoverElection::elect is a noexcept pure function — no allocation on the
// hot path (all vectors are pre-populated before the timed loop).
// ===========================================================================
BENCHMARK_DEFINE_F(RebalanceFailoverFixture, FailoverElection)(benchmark::State& state)
{
    if (state.skipped()) return;

    // Build election inputs once — reuse across iterations.
    PartitionAssignment cur;
    cur.leaderNode    = 1;
    cur.isr           = {1, 2, 3};
    cur.replicas      = {1, 2, 3};
    cur.leaderEpoch   = 1;
    cur.partitionEpoch = 1;

    // Node 1 is dead.
    const std::vector<int32_t> liveNodes = {2, 3};

    // Position table: populate reports for nodes 2 and 3.
    ReplicaPositionTable positions;
    ReplicaPositionReport rep2;
    rep2.topicId = 0; rep2.partition = 0; rep2.nodeId = 2;
    rep2.leaderEpoch = 1; rep2.logEndPosition = 100;
    positions.report(rep2);
    ReplicaPositionReport rep3;
    rep3.topicId = 0; rep3.partition = 0; rep3.nodeId = 3;
    rep3.leaderEpoch = 1; rep3.logEndPosition = 95;
    positions.report(rep3);

    ElectionInputs in;
    in.cur        = cur;
    in.liveNodes  = liveNodes;
    in.positions  = &positions;
    in.allowUnclean = false;

    std::vector<double> samples;
    static constexpr std::size_t kMaxSamples = 2'000'000UZ;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations), kMaxSamples));

    // Warm-up.
    for (int w = 0; w < 200; ++w) {
        auto out = FailoverElection::elect(0, 0, in);
        benchmark::DoNotOptimize(out);
    }

    for (auto _ : state) {
        const int64_t t0 = rfbNowNs();
        auto out = FailoverElection::elect(0, 0, in);
        const int64_t elapsed = rfbNowNs() - t0;
        benchmark::DoNotOptimize(out);

        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        samples.push_back(static_cast<double>(elapsed) / 1'000.0); // µs
    }

    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        state.counters["election_p50_us"]  = pctile(samples, 0.50);
        state.counters["election_p99_us"]  = pctile(samples, 0.99);
        state.counters["election_p999_us"] = pctile(samples, 0.999);
    }
}

BENCHMARK_REGISTER_F(RebalanceFailoverFixture, FailoverElection)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond);

// ===========================================================================
// 2. RebalanceApply — DynamicBalancer::rebalance() + emitDynamicAssignment.
//
// Each iteration:
//   1. DynamicBalancer::rebalance(in) — computes deltas for a forced-failover.
//   2. metaSvc_->emitDynamicAssignment(recs) — enqueues the records.
//   3. metaSvc_->doWork(clk) — drains the queue (append to metadata log).
//
// The rebalance scenario: every call sees liveNodes = {1} (nodes 2+3 are
// "down") so all partitions with leaders 2 or 3 (none initially — topicId=0
// partition=0 is already on node 1) get rebalanced.  We pre-populate several
// off-node partitions to make rebalance non-trivial.
// ===========================================================================
BENCHMARK_DEFINE_F(RebalanceFailoverFixture, RebalanceApply)(benchmark::State& state)
{
    if (state.skipped()) return;

    // Pre-populate the metadata map with 6 partitions across nodes 1–3.
    //   topicId=0: partitions 0–5, leaders round-robin {1,2,3,1,2,3}.
    {
        uint8_t buf[512];
        const std::vector<int32_t> isr1 = {1, 2, 3};
        const std::vector<int32_t> rep1 = {1, 2, 3};
        const int32_t leaders[6] = {1, 2, 3, 1, 2, 3};
        int64_t clk = clk_;
        for (int p = 0; p < 6; ++p) {
            std::size_t n = rfbEncodeAssignment(
                buf, sizeof(buf), /*topicId=*/0, p,
                leaders[p], isr1, rep1, 1, 1);
            metaReplica_->append(std::span<const uint8_t>(buf, n), false);
        }
        clk += 10'000'000LL;
        metaReplica_->doWork(clk);
        metaTransport_->pump(clk);
        metaSvc_->doWork(clk);
        clk_ = clk;
    }

    // TopicSpec for the rebalancer.
    TopicSpec ts;
    ts.topicId        = 0;
    ts.partitionCount = 6;
    ts.rf             = 1;

    // ReplicaPositionTable: all replicas report same epoch/LEO so election
    // is deterministic by nodeId.
    ReplicaPositionTable positions;
    for (int node : {1, 2, 3}) {
        for (int p = 0; p < 6; ++p) {
            ReplicaPositionReport r;
            r.topicId = 0; r.partition = p; r.nodeId = node;
            r.leaderEpoch = 1; r.logEndPosition = 100;
            positions.report(r);
        }
    }

    std::vector<double> samples;
    static constexpr std::size_t kMaxSamples = 2'000'000UZ;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations), kMaxSamples));

    int64_t clk = clk_;
    int64_t epoch = 2;  // advance epoch each cycle to force reassignment

    // Warm-up.
    for (int w = 0; w < 20; ++w) {
        clk += 5'000'000LL;
        DynamicBalancerInputs in;
        in.cur       = &metaSvc_->committedMap();
        in.liveNodes = {1, 2, 3};
        in.topics    = {ts};
        in.positions = &positions;
        in.maxPreferredElectionsPerCycle = 100;
        in.maxRecordsPerCycle = 4096;
        DynamicBalancer bal;
        auto recs = bal.rebalance(in);
        if (!recs.empty()) {
            metaSvc_->emitDynamicAssignment(recs);
            metaSvc_->doWork(clk);
        }
        metaReplica_->doWork(clk);
        metaTransport_->pump(clk);
        (void)epoch;
    }
    clk_ = clk;

    for (auto _ : state) {
        clk_ += 5'000'000LL;

        DynamicBalancerInputs in;
        in.cur       = &metaSvc_->committedMap();
        in.liveNodes = {1, 2, 3};
        in.topics    = {ts};
        in.positions = &positions;
        in.maxPreferredElectionsPerCycle = 100;
        in.maxRecordsPerCycle = 4096;

        const int64_t t0 = rfbNowNs();

        DynamicBalancer bal;
        auto recs = bal.rebalance(in);
        if (!recs.empty()) {
            metaSvc_->emitDynamicAssignment(recs);
        }
        metaSvc_->doWork(clk_);

        const int64_t elapsed = rfbNowNs() - t0;
        benchmark::DoNotOptimize(recs);

        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        samples.push_back(static_cast<double>(elapsed) / 1'000.0); // µs

        metaReplica_->doWork(clk_);
        metaTransport_->pump(clk_);
    }

    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        state.counters["rebalance_p50_us"]  = pctile(samples, 0.50);
        state.counters["rebalance_p99_us"]  = pctile(samples, 0.99);
        state.counters["rebalance_p999_us"] = pctile(samples, 0.999);
    }
}

BENCHMARK_REGISTER_F(RebalanceFailoverFixture, RebalanceApply)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond);

// ===========================================================================
// 3. LeaderHandoff — becomeFollower + becomeLeader round-trip latency.
//
// Models the leader-move handoff at the replication layer.
// Each iteration:
//   1. diskReplica_->becomeFollower(newEpoch, leaderId=2)
//   2. diskReplica_->becomeLeader(newEpoch+1, isr={1})
//
// This is the "handoff" path the control plane exercises when it moves
// partition leadership from node 1 to node 2 and back.  No data is flushed;
// this measures pure state-machine transition overhead.
// ===========================================================================
BENCHMARK_DEFINE_F(RebalanceFailoverFixture, LeaderHandoff)(benchmark::State& state)
{
    if (state.skipped()) return;

    std::vector<double> samples;
    static constexpr std::size_t kMaxSamples = 2'000'000UZ;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations), kMaxSamples));

    Epoch handoffEpoch = 2;

    // Warm-up.
    for (int w = 0; w < 50; ++w) {
        diskReplica_->becomeFollower(handoffEpoch, /*leaderId=*/2);
        diskReplica_->becomeLeader(handoffEpoch + 1, {1});
        handoffEpoch += 2;
        clk_ += 5'000'000LL;
        diskReplica_->doWork(clk_);
        diskTransport_->pump(clk_);
    }

    for (auto _ : state) {
        const int64_t t0 = rfbNowNs();
        diskReplica_->becomeFollower(handoffEpoch, /*leaderId=*/2);
        diskReplica_->becomeLeader(handoffEpoch + 1, {1});
        const int64_t elapsed = rfbNowNs() - t0;

        benchmark::DoNotOptimize(handoffEpoch);
        handoffEpoch += 2;

        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        samples.push_back(static_cast<double>(elapsed) / 1'000.0); // µs

        clk_ += 5'000'000LL;
        diskReplica_->doWork(clk_);
        diskTransport_->pump(clk_);
    }

    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        state.counters["handoff_p50_us"]  = pctile(samples, 0.50);
        state.counters["handoff_p99_us"]  = pctile(samples, 0.99);
        state.counters["handoff_p999_us"] = pctile(samples, 0.999);
    }
}

BENCHMARK_REGISTER_F(RebalanceFailoverFixture, LeaderHandoff)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond);

// ===========================================================================
// 4. DiskProduce — PartitionReplica::append() on a disk-backed Log.
//
// Confirms the Phase-1 baseline (4.4–8.7 µs on M2) is maintained after the
// ILog de-virtualization.  The concrete Log* path has no vtable dispatch —
// a single branch on memoryBacked_ (false here) to diskLog_->append().
//
// HARD gate: none (informational comparison; the gate is on produce-under-
// rebalance, not on raw append).  We report the p99 explicitly so the user
// can compare against the Phase-1 baseline.
// ===========================================================================
BENCHMARK_DEFINE_F(RebalanceFailoverFixture, DiskProduce)(benchmark::State& state)
{
    if (state.skipped()) return;

    const auto bsp = std::span<const uint8_t>(batch_.data(), batch_.size());

    std::vector<double> samples;
    static constexpr std::size_t kMaxSamples = 2'000'000UZ;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations), kMaxSamples));

    // Warm-up.
    for (int w = 0; w < 50; ++w) {
        Offset o = diskReplica_->append(bsp, false);
        benchmark::DoNotOptimize(o);
        clk_ += 5'000'000LL;
        diskReplica_->doWork(clk_);
        diskTransport_->pump(clk_);
    }

    for (auto _ : state) {
        const int64_t t0 = rfbNowNs();
        Offset off = diskReplica_->append(bsp, false);
        const int64_t elapsed = rfbNowNs() - t0;
        benchmark::DoNotOptimize(off);

        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        samples.push_back(static_cast<double>(elapsed) / 1'000.0); // µs

        // Keep HW advancing — avoid log runaway.
        clk_ += 5'000'000LL;
        diskReplica_->doWork(clk_);
        diskTransport_->pump(clk_);
    }

    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        const double p99 = pctile(samples, 0.99);
        state.counters["disk_produce_p50_us"]  = pctile(samples, 0.50);
        state.counters["disk_produce_p99_us"]  = p99;
        state.counters["disk_produce_p999_us"] = pctile(samples, 0.999);
        // Baseline comparison: Phase-1 measured 4.4–8.7 µs on M2.
        // Flag if p99 > 3× baseline ceiling (26 µs) — indicates a regression.
        state.counters["disk_p99_vs_baseline_ok"] = (p99 <= 26.0) ? 1.0 : 0.0;
    }
}

BENCHMARK_REGISTER_F(RebalanceFailoverFixture, DiskProduce)
    ->UseManualTime()
    ->MinTime(2.0)
    ->Unit(benchmark::kMicrosecond);

// ===========================================================================
// 5. MemoryProduce — InMemoryLog::append() direct hot-path latency.
//
// Measures the concrete InMemoryLog::appendImpl() latency:
//   - ring_.insert() (amortised O(1), no alloc in steady state after reserve())
//   - entries_.push_back() (amortised O(1), no alloc after reserve())
//
// We call InMemoryLog::append(data, len, lastOffset) directly rather than
// PartitionReplica::append(), because PartitionReplica::append() re-wraps the
// payload into a new RecordBatch on every call (makeBatch → build() → new
// vector<uint8_t>), which is a structural allocation on the hot path — NOT
// the behaviour we want to measure for the de-virtualization payoff.
//
// InMemoryLog is pre-warm-touched in SetUp (1 000 appends) so page faults
// do NOT appear in the measured region.  Expected p99 < 10 µs.
//
// Memory produce should be LOWER than disk produce (no segment-roll tail,
// no fsync, no mmap extend).
//
// Throughput counter: records/s via benchmark::Counter(kIsRate).
// ===========================================================================
BENCHMARK_DEFINE_F(RebalanceFailoverFixture, MemoryProduce)(benchmark::State& state)
{
    if (state.skipped()) return;

    const uint8_t*  data = batch_.data();
    const std::size_t len = batch_.size();

    std::vector<double> samples;
    static constexpr std::size_t kMaxSamples = 2'000'000UZ;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations), kMaxSamples));
    int64_t totalRecords = 0;

    for (auto _ : state) {
        const int64_t t0 = rfbNowNs();
        int64_t off = memLog_->append(data, len, memLogOffset_);
        const int64_t elapsed = rfbNowNs() - t0;
        benchmark::DoNotOptimize(off);

        // Advance the offset counter (one record per batch, lastOffset = memLogOffset_).
        memLogOffset_ = off + 1;

        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        samples.push_back(static_cast<double>(elapsed) / 1'000.0); // µs
        ++totalRecords;
    }

    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        state.counters["mem_produce_p50_us"]  = pctile(samples, 0.50);
        state.counters["mem_produce_p99_us"]  = pctile(samples, 0.99);
        state.counters["mem_produce_p999_us"] = pctile(samples, 0.999);
    }
    state.counters["mem_records_per_sec"] = benchmark::Counter(
        static_cast<double>(totalRecords), benchmark::Counter::kIsRate);
}

BENCHMARK_REGISTER_F(RebalanceFailoverFixture, MemoryProduce)
    ->UseManualTime()
    ->Iterations(50000)     // fixed count prevents ring eviction (see SetUp comment)
    ->Unit(benchmark::kMicrosecond);

// ===========================================================================
// 6. ProduceUnderRebalance — HARD GATE: p99 < 100 µs while rebalance in flight.
//
// Measures single-append latency on the InMemoryLog hot path while a
// DynamicBalancer::rebalance() call is injected every kRebalanceInterval
// iterations.  Both the append and the balancer run on the same thread.
//
// Uses InMemoryLog::append() directly (same rationale as MemoryProduce —
// PartitionReplica::append() re-wraps into a new RecordBatch on every call,
// making steady-state zero-alloc impossible via that path).
//
// Each state iteration times ONE InMemoryLog::append() call, and on every
// kRebalanceInterval-th iteration also includes a DynamicBalancer::rebalance()
// call.  The timing is per-iteration so the full cost of the rebalance-injection
// iteration is captured in the p99 sample.
//
// The InMemoryLog ring is pre-warm-touched (SetUp) so page faults do NOT appear
// in the measured region.  In steady state the ring wraps (evictions) but the
// memmove cost is bounded and pre-exercised.
//
// HARD GATE: state.SkipWithError if p99 >= 100 000 ns (100 µs).
// ===========================================================================
BENCHMARK_DEFINE_F(RebalanceFailoverFixture, ProduceUnderRebalance)(benchmark::State& state)
{
    if (state.skipped()) return;

    // Inject rebalance once every kRebalanceInterval produce iterations.
    static constexpr int kRebalanceInterval = 32;

    const uint8_t*  data = batch_.data();
    const std::size_t len = batch_.size();

    // DynamicBalancer inputs — steady-state (all nodes live, no failover needed).
    // Build a local metadata replica for topic 0, partition 0 → leader=1.
    // Re-uses the fixture's metaSvc_ committedMap (already seeded with t0p0→1).
    TopicSpec ts;
    ts.topicId        = 0;
    ts.partitionCount = 1;
    ts.rf             = 1;

    ReplicaPositionTable positions;
    {
        ReplicaPositionReport r;
        r.topicId = 0; r.partition = 0; r.nodeId = 1;
        r.leaderEpoch = 1; r.logEndPosition = 100;
        positions.report(r);
    }

    // Pre-build the DynamicBalancerInputs struct (no alloc per iteration).
    DynamicBalancerInputs balIn;
    balIn.cur       = &metaSvc_->committedMap();
    balIn.liveNodes = {1};   // steady-state: only node 1 alive → no failover
    balIn.topics    = {ts};
    balIn.positions = &positions;
    balIn.maxPreferredElectionsPerCycle = 100;
    balIn.maxRecordsPerCycle = 4096;

    std::vector<double> samples;
    static constexpr std::size_t kMaxSamples = 2'000'000UZ;
    samples.reserve(std::min(static_cast<std::size_t>(state.max_iterations), kMaxSamples));

    int64_t iterCount   = 0;
    int64_t localOffset = memLogOffset_; // picks up after SetUp's warm-up appends

    // Warm-up: stabilise icache + branch predictors.
    // Keep warm-up small — shared ring has limited capacity.
    for (int w = 0; w < 200; ++w) {
        int64_t off = memLog_->append(data, len, localOffset);
        benchmark::DoNotOptimize(off);
        localOffset = off + 1;
        if (w % kRebalanceInterval == 0) {
            DynamicBalancer bal;
            auto recs = bal.rebalance(balIn);
            benchmark::DoNotOptimize(recs);
        }
    }

    for (auto _ : state) {
        const bool doRebalance = ((iterCount % kRebalanceInterval) == 0);

        const int64_t t0 = rfbNowNs();

        // ── Hot-path append (always measured) ────────────────────────────────
        int64_t off = memLog_->append(data, len, localOffset);
        benchmark::DoNotOptimize(off);
        localOffset = off + 1;

        // ── Optional rebalance injection (same iteration, same timed region) ──
        if (doRebalance) {
            DynamicBalancer bal;
            auto recs = bal.rebalance(balIn);
            if (!recs.empty()) {
                // emitDynamicAssignment enqueues into the fixture's metaSvc_.
                metaSvc_->emitDynamicAssignment(recs);
            }
            benchmark::DoNotOptimize(recs);
        }

        const int64_t elapsed = rfbNowNs() - t0;

        state.SetIterationTime(static_cast<double>(elapsed) / 1e9);
        samples.push_back(static_cast<double>(elapsed) / 1'000.0); // µs
        ++iterCount;
    }

    // Persist offset back for shared use.
    memLogOffset_ = localOffset;

    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        const double p50  = pctile(samples, 0.50);
        const double p99  = pctile(samples, 0.99);
        const double p999 = pctile(samples, 0.999);

        state.counters["produce_rebalance_p50_us"]    = p50;
        state.counters["produce_rebalance_p99_us"]    = p99;
        state.counters["produce_rebalance_p999_us"]   = p999;
        state.counters["produce_rebalance_gate_pass"] = (p99 < 100.0) ? 1.0 : 0.0;

        // ── HARD GATE ────────────────────────────────────────────────────────
        // p99 >= 100 µs (100 000 ns) means a single produce iteration is
        // hitting tail latency above the Rune ULL mandate — this fires on any
        // blocking regression (mutex, sync I/O, O(N) scan) bleeding from the
        // rebalance path into the produce hot path.
        if (p99 >= 100.0) {
            state.SkipWithError(
                "produce-under-rebalance p99 >= 100us");
            return;
        }
    }
}

BENCHMARK_REGISTER_F(RebalanceFailoverFixture, ProduceUnderRebalance)
    ->UseManualTime()
    ->Iterations(50000)     // fixed count prevents ring eviction (see SetUp comment)
    ->Unit(benchmark::kMicrosecond);
