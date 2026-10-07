// DutyPassLatencyGateTest.cpp — automated hot-path latency regression gate for
// the worker DUTY PASS.
//
// Why: a 620 ms stall of the worker duty pass (compaction install recovering a
// 64 MiB segment on the owner thread) was caught only by a reviewer's manual
// probe — correctness tests cannot see latency.  This gate drives the REAL
// production duty pass while each background operation happens and fails when
// the pass's MAX or p99.9 exceeds a per-build-type budget.
//
// What one measured pass is (production default = shard mode + fused runtime
// placement, NodeConfig::workerMode / runtimePlacement):
//     intake (produce frames → Worker's ingress frame handler — the body of
//             pollWorkerIngress_, the Aeron poll itself excluded)
//   + Worker::doWork(now)          (shard order: op ring, appends, replica duty,
//                                   RC build, background disk work, acks, io reap,
//                                   cleaner swap / retention / staging drains)
//   + NodeRuntimeAgent::doWork(now) (the fused runtime's agent work: reconcile,
//                                   cleaner view publication, retired-replica gate)
// Not measured: the embedded Aeron driver + client conductor duty cycles (no
// driver in this rig) and the gateway's drain of worker returns (done by the
// test between passes, as the runtime thread / fused gateway would).
//
// Background: the REAL LogCleanerAgent runs on its own AgentRunner thread
// (production wiring: ring-delivered replica view, coalescing on, 128 MiB cycle
// budget, BackoffIdleStrategy) doing segment pre-provisioning, staging renames,
// retention proposals, compaction passes and every posted file op — exactly
// what it does in BrokerNode.  The test thread IS the worker thread (single
// owner); it never sleeps: every wait is a bounded condition loop of passes.
//
// Scenarios (each single- and/or multi-partition):
//   1. steady produce (baseline)
//   2. segment roll every few appends (pre-provisioned adoption, seal submit,
//      sealed-snapshot publish)
//   3. time + size retention deleting segments (cleaner proposes, owner drops)
//   4. compaction of a compacted topic, 1 / 16 / 64 MiB groups, installed by
//      the owner (the 620 ms bug class)
//   5. follower divergence truncation (TruncatePending) while a throttled
//      cleaner pass runs on another log
//   6. consumer reads (PushRead: Log::read of sealed + active segments) during 2-4
//   7. leadership change: becomeFollower / becomeLeader with epoch bump
//      (controller-managed: leader-epoch fence)
//   8. idempotent + transactional produce with COMMIT/ABORT control markers
//
// Budgets: see kReleaseBudget / kDebugBudget (named constants, documented).
// Cold operations (explicitly allowed above the hot budget, own ceiling):
//   * a pass that executes a BecomeLeader / BecomeFollower op — the leader-epoch
//     checkpoint (write + fsync + rename + dir fsync, LeaderEpochCache::
//     checkpointToFile) and, controller-managed, the leader-epoch fence
//     (PartitionReplica::persistLedEpoch_) run synchronously on the owner;
//   * the pass that completes a follower divergence truncation (Log::truncateTo:
//     recovery-point persist, file removal / ftruncate).
// Everything else — rolls, compaction installs, retention drops, reads,
// markers, TruncatePending passes, RC rebuild after a promotion — is HOT.
//
// Determinism: each scenario runs enough passes for a stable p99.9; a scenario
// that misses its budget is re-run (up to kAttempts) and only the BEST attempt
// is judged — a real regression (a stall in the pass) reproduces every run, a
// scheduler preemption does not.  ctest runs the binary RUN_SERIAL.
//
// Benchmark mode: RUNE_LATENCY_GATE_BENCH=1 prints the numbers, never fails;
// RUNE_LATENCY_GATE_SCALE=<n> multiplies the pass counts.
//
// Known-failing: a scenario whose budget cannot be met today is listed in
// kKnownFailures with its measured number (a real hot-path bug to fix).  It is
// judged against a regression ceiling instead (it must not get worse), and the
// gate prints a notice when it starts meeting the real budget.
#include <rune/consensus/ConsensusAgent.h>   // defines Agent (before AgentRunner.h)
#include <rune/consensus/AgentRunner.h>
#include <rune/consensus/IdleStrategy.h>
#include <rune/consensus/NanoClock.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/LogCleanerAgent.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/NodeRuntimeAgent.h>
#include <rune/node/ParkUntilWorkIdle.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/SegmentProvisionerAgent.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/storage/Log.h>
#include <rune/transaction/ControlMarker.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <sys/resource.h>

namespace fs = std::filesystem;
namespace pc = rune::protocol::client;
using rune::node::LogCleanerAgent;
using rune::node::NodeRuntimeAgent;
using rune::node::ReplicaRegistry;
using rune::node::Worker;
using rune::node::WorkerOp;
using rune::node::WorkerPool;
using rune::node::WorkerReturn;
using rune::replication::CleanupPolicy;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;
using rune::storage::Log;

// Segment-heavy scenarios hold thousands of live segments (fds + mmaps).
static const bool g_gate_fd_limit_raised = [] {
    struct ::rlimit rl{};
    if (::getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur < rl.rlim_max) {
        rl.rlim_cur = rl.rlim_max;
        ::setrlimit(RLIMIT_NOFILE, &rl);
    }
    return true;
}();

namespace {

constexpr int64_t kUs  = 1'000;
constexpr int64_t kMs  = 1'000'000;
constexpr int64_t kMiB = 1024 * 1024;

// ---------------------------------------------------------------------------
// Build kind + budgets
// ---------------------------------------------------------------------------
#if defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(address_sanitizer)
#define RUNE_GATE_SANITIZED 1
#endif
#endif
#if defined(__SANITIZE_THREAD__) || defined(__SANITIZE_ADDRESS__)
#define RUNE_GATE_SANITIZED 1
#endif
#ifndef RUNE_GATE_SANITIZED
#define RUNE_GATE_SANITIZED 0
#endif

#ifdef NDEBUG
constexpr bool kReleaseBuild = true;
#else
constexpr bool kReleaseBuild = false;
#endif

struct Budget {
    int64_t hotMaxNs;    ///< worst HOT pass
    int64_t hotP999Ns;   ///< p99.9 of HOT passes
    int64_t coldMaxNs;   ///< worst allowed-cold pass (role change / truncation completion)
};

// RELEASE (-O3, gcc-13, Linux arm64 — the broker image's builder stage):
//   hot max 100 µs  = BasicShardAgent::kSlowPassNs, the spec §8 slow-pass alarm
//                     and the 100 µs p99 produce target: one pass over budget
//                     delays every partition the worker owns by that much.
//   hot p99.9 50 µs = half the max; ~6x the measured steady-state p99.9 (7.7 µs,
//                     1 partition), so a common path getting slower trips it
//                     before the max does.
//   cold max 250 ms = a catastrophe ceiling only (the cold ops are synchronous
//                     fsyncs by design — 2-63 ms measured in the Docker VM; it
//                     catches O(log size) work sneaking into a role change, not
//                     fsync jitter).  The cold cost itself is reported (finding F7).
constexpr Budget kReleaseBudget{100 * kUs, 50 * kUs, 250 * kMs};
// DEBUG (-O0 -g; build-fast on macOS = x86_64 under Rosetta 2).  Measured on
// build-fast: hot paths run ~7x slower than Release (p50 4.0 vs 0.5 µs steady
// 1p, 28 vs 4.8 µs steady 8p, 4.3 vs 0.6 µs compaction), and macOS + Rosetta add
// ms-scale scheduling outliers in long runs (3.6-4.0 ms worst passes with no
// background op in the 2.4M-pass 64 MiB compaction runs).  Set at ~3x headroom
// over the worst clean Debug scenario (best-of-3: max 1.9 ms, p99.9 0.48 ms):
// max 6 ms, p99.9 1 ms — it catches regressions of the 620 ms class (orders of
// magnitude); Release is the authoritative latency gate.
constexpr Budget kDebugBudget{6'000 * kUs, 1'000 * kUs, 250 * kMs};

constexpr const Budget& budget() noexcept { return kReleaseBuild ? kReleaseBudget : kDebugBudget; }
constexpr const char* buildName() noexcept { return kReleaseBuild ? "Release" : "Debug"; }

// Scenarios that cannot meet the budget today — REAL hot-path bugs (findings
// F1.. in the report).  Never a silent loosening: each entry lists the number
// measured when it was added and is judged against a regression CEILING (the
// worst attempt seen x ~3) instead of the budget, so it cannot get worse
// unnoticed; the gate prints a notice once the real budget is met.
struct KnownFailure {
    const char* scenario;
    bool        release;          ///< build type the entry applies to
    int64_t     measuredMaxNs;    ///< best-of-attempts HOT max when listed
    int64_t     measuredP999Ns;   ///< best-of-attempts HOT p99.9 when listed
    int64_t     ceilingMaxNs;     ///< regression ceilings
    int64_t     ceilingP999Ns;
    const char* why;              ///< finding id + root cause
};
// Root causes (docs/superpowers/briefs/2026-10-05-latency-gate-report.md):
//   F1 Log::maybeRoll calls LogSegment::preallocate(rollBytes) — fallocate of
//      the whole next segment (1-64 MiB) on the worker at every roll (~150-330 µs).
//   F2 no pre-provisioned segment at roll → inline create (open + ftruncate +
//      mmap) on the worker (0.2-17 ms): the cleaner builds <= 1 staging segment
//      per log per cycle, parks up to 1 ms, and none during a compaction pass.
//   F3 > Log::kMaxPendingSeals (16) seals in flight → inline seal fsync on the worker.
//   F4 publishSealedSnapshot is O(segments) + a heap allocation, twice per roll
//      (maybeRoll + the recovery-point completion in the pass): ~130 µs at 5000 segments.
//   F5 PushRead serves via Log::read: a heap vector + up to 512 KiB copy per op
//      on the worker (Release p99 45 µs, max 200-870 µs).
//   F6 plain pwrite appends stall 100-230 µs on ext4 with 8 partitions written
//      per pass (absent on tmpfs) — filesystem/journal; confirm on bare metal.
// Ceilings = 3x the worst best-of-3 seen over the listing runs (p99.9 that met
// the budget keeps the budget as its ceiling).
constexpr KnownFailure kKnownFailures[] = {
    // ---- Release (Linux arm64, Docker builder stage, ext4 volume) ----
    {"SteadyProduce/8p",               true,    150 * kUs,    100 * kUs,   2'000 * kUs,    750 * kUs, "F6"},
    {"SegmentRoll/1p",                 true,  2'000 * kUs,  1'500 * kUs,  20'000 * kUs,  5'000 * kUs, "F1 F2 F3 F4"},
    {"SegmentRoll/8p",                 true,  8'000 * kUs,  4'200 * kUs,  30'000 * kUs, 22'000 * kUs, "F1 F2 F3 F4"},
    {"Retention/time+size/2p",         true,  3'800 * kUs,  3'300 * kUs,  20'000 * kUs, 14'000 * kUs, "F1 F2 F3 F4"},
    {"Compaction/1MiB/1p",             true,    336 * kUs,    159 * kUs,   1'600 * kUs,    500 * kUs, "F1 F2"},
    {"Compaction/16MiB/1p",            true,    313 * kUs,     18 * kUs,   1'250 * kUs,     50 * kUs, "F1 F2"},
    {"Compaction/64MiB/1p",            true,    260 * kUs,     16 * kUs,   1'700 * kUs,     50 * kUs, "F1 F2"},
    {"Compaction/1MiB/4p",             true,    772 * kUs,    370 * kUs,   2'700 * kUs,  1'100 * kUs, "F1 F2"},
    {"FollowerTruncation/cleanerPass/2p", true, 3'900 * kUs, 1'900 * kUs,  16'000 * kUs,  6'000 * kUs, "F1 F2 F3 F4"},
    {"Reads+SegmentRoll/1p",           true,  4'000 * kUs,  3'100 * kUs,  13'000 * kUs, 10'000 * kUs, "F1-F5"},
    {"Reads+Retention/2p",             true,  6'100 * kUs,  4'000 * kUs,  19'000 * kUs, 12'500 * kUs, "F1-F5"},
    {"Reads+Compaction/16MiB/1p",      true,    216 * kUs,     61 * kUs,     700 * kUs,    200 * kUs, "F5 F1"},
    {"LeadershipChange/1p",            true,    271 * kUs,     27 * kUs,   1'400 * kUs,     50 * kUs, "F1 F2"},
    {"LeadershipChange/8p",            true,  1'330 * kUs,    384 * kUs,   4'500 * kUs,  1'300 * kUs, "F1 F2"},
    {"TxnMarkers/1p",                  true,    206 * kUs,     33 * kUs,     700 * kUs,     50 * kUs, "F1"},
    {"TxnMarkers/4p",                  true,  1'290 * kUs,     39 * kUs,   6'000 * kUs,     50 * kUs, "F1 F2"},
    // ---- Debug (macOS build-fast, x86_64 Rosetta, APFS) ----
    {"SegmentRoll/1p",                 false, 7'200 * kUs,  1'520 * kUs,  27'000 * kUs,  5'000 * kUs, "F1 F2 F4"},
    {"SegmentRoll/8p",                 false, 18'900 * kUs, 12'400 * kUs, 90'000 * kUs, 45'000 * kUs, "F1 F2 F4"},
    {"Retention/time+size/2p",         false, 7'300 * kUs,  1'620 * kUs,  33'000 * kUs,  5'000 * kUs, "F1 F2 F4"},
    {"Reads+SegmentRoll/1p",           false, 4'500 * kUs,  1'560 * kUs,  30'000 * kUs,  5'000 * kUs, "F1 F2 F4 F5"},
    {"Reads+Compaction/16MiB/1p",      false, 6'600 * kUs,  6'160 * kUs,  22'000 * kUs, 20'000 * kUs,
     "F5 (Debug: one unoptimised 512 KiB Log::read costs ~6 ms)"},
};

const KnownFailure* knownFailure(const std::string& scenario) noexcept {
    for (const auto& k : kKnownFailures)
        if (scenario == k.scenario && k.release == kReleaseBuild) return &k;
    return nullptr;
}

bool benchMode() noexcept {
    const char* v = std::getenv("RUNE_LATENCY_GATE_BENCH");
    return v != nullptr && v[0] != '\0' && v[0] != '0';
}
int scale() noexcept {
    const char* v = std::getenv("RUNE_LATENCY_GATE_SCALE");
    const int s = v ? std::atoi(v) : 0;
    return s > 0 ? s : (benchMode() ? 4 : 1);
}
constexpr int kAttempts = 3;

// ---------------------------------------------------------------------------
// Pass flags (what happened in a pass — attribution of the worst passes)
// ---------------------------------------------------------------------------
enum PassFlag : uint32_t {
    kProduce      = 1u << 0,
    kRoll         = 1u << 1,
    kInstall      = 1u << 2,   ///< compaction group installed (owner)
    kRetention    = 1u << 3,   ///< retention dropped segment(s) (owner)
    kRoleOp       = 1u << 4,   ///< BecomeLeader / BecomeFollower executed  (COLD)
    kTruncate     = 1u << 5,   ///< follower truncation completed            (COLD)
    kTruncPending = 1u << 6,   ///< a replica sat in TruncatePending
    kRead         = 1u << 7,   ///< PushRead served
    kMarker       = 1u << 8,   ///< txn control marker appended
    kDiverge      = 1u << 9,   ///< diverging-epoch response applied
    kInlineFsync  = 1u << 10,  ///< an inline LogSegment::sync ran on the owner (Log::syncCallsForTest)
    kSyncCreate   = 1u << 11,  ///< a roll created the next segment inline (no pre-provisioned one)
};
constexpr uint32_t kColdFlags = kRoleOp | kTruncate;
/// Background operations: a pass with exactly ONE of these is attributable to
/// it ("solo" maxima — e.g. roll alone = the adopt path, no inline create/fsync).
constexpr uint32_t kBgFlags = kRoll | kInstall | kRetention | kRoleOp | kTruncate | kTruncPending | kDiverge |
                              kInlineFsync | kSyncCreate | kRead;

std::string flagsStr(uint32_t f) {
    static constexpr std::pair<uint32_t, const char*> names[] = {
        {kProduce, "produce"}, {kRoll, "roll"}, {kInstall, "install"}, {kRetention, "retention"},
        {kRoleOp, "role-op"}, {kTruncate, "truncate"}, {kTruncPending, "trunc-pending"},
        {kRead, "read"}, {kMarker, "marker"}, {kDiverge, "diverge"}, {kInlineFsync, "INLINE-FSYNC"},
        {kSyncCreate, "SYNC-CREATE"}};
    std::string s;
    for (const auto& [bit, n] : names)
        if (f & bit) { if (!s.empty()) s += '+'; s += n; }
    return s.empty() ? "idle" : s;
}

// ---------------------------------------------------------------------------
// Statistics (test-only allocation: samples are stored, sorted at the end)
// ---------------------------------------------------------------------------
/// Where a pass spent its time: intake (frame handler = appends), the Worker
/// pass, the fused runtime agent.
struct Split {
    int64_t intakeNs = 0, workerNs = 0, runtimeNs = 0;
};
struct Worst {
    int64_t  ns    = 0;
    uint32_t flags = 0;
    uint64_t pass  = 0;
    Split    split{};
};
struct Summary {
    uint64_t n = 0;
    int64_t  p50 = 0, p99 = 0, p999 = 0, max = 0;
};
struct Stats {
    std::vector<int64_t> hot, cold;
    std::array<Worst, 3> worstHot{};
    Worst                worstCold{};
    std::array<int64_t, 16> flagMax{};   ///< worst pass that had flag bit i set
    std::array<int64_t, 16> soloMax{};   ///< worst pass whose ONLY background flag was bit i
    /// ... its Worker::doWork + runtime part only (where installs / retention
    /// drops / TruncatePending run) — excludes the intake (append pwrite) noise.
    std::array<int64_t, 16> soloWorkMax{};
    int64_t  hotNoRollMax = 0;           ///< worst HOT pass that did not roll a segment
    uint64_t passes = 0;
    Stats() { hot.reserve(1 << 18); cold.reserve(1024); }
    void add(int64_t ns, uint32_t flags, const Split& split = {}) {
        ++passes;
        noteFlags(ns, flags, split.workerNs + split.runtimeNs);
        if (!(flags & (kColdFlags | kRoll)) && ns > hotNoRollMax) hotNoRollMax = ns;
        if (flags & kColdFlags) {
            cold.push_back(ns);
            if (ns > worstCold.ns) worstCold = {ns, flags, passes, split};
            return;
        }
        hot.push_back(ns);
        if (ns > worstHot.back().ns) {
            worstHot.back() = {ns, flags, passes, split};
            std::sort(worstHot.begin(), worstHot.end(),
                      [](const Worst& a, const Worst& b) { return a.ns > b.ns; });
        }
    }
    /// A driven pass with no input and nothing observed (only its flag maxima
    /// and the idle max are kept — it would dilute the percentiles).
    void noteIdle(int64_t ns, uint32_t flags) {
        noteFlags(ns, flags, ns);
        if (ns > idleMax) idleMax = ns;
    }
    void noteFlags(int64_t ns, uint32_t flags, int64_t workNs) {
        const uint32_t bg = flags & kBgFlags;
        const bool solo = bg != 0 && (bg & (bg - 1)) == 0;
        for (std::size_t b = 0; b < flagMax.size(); ++b) {
            if (!(flags & (1u << b))) continue;
            if (ns > flagMax[b]) flagMax[b] = ns;
            if (solo && (bg & (1u << b))) {
                if (ns > soloMax[b]) soloMax[b] = ns;
                if (workNs > soloWorkMax[b]) soloWorkMax[b] = workNs;
            }
        }
    }
    int64_t idleMax = 0;
    static Summary summarize(std::vector<int64_t> v) {
        Summary s;
        s.n = v.size();
        if (v.empty()) return s;
        std::sort(v.begin(), v.end());
        auto at = [&](double q) {
            const std::size_t i = static_cast<std::size_t>(q * static_cast<double>(v.size() - 1));
            return v[i];
        };
        s.p50 = at(0.50); s.p99 = at(0.99); s.p999 = at(0.999); s.max = v.back();
        return s;
    }
    [[nodiscard]] Summary hotSummary()  const { return summarize(hot); }
    [[nodiscard]] Summary coldSummary() const { return summarize(cold); }
};

double us(int64_t ns) noexcept { return static_cast<double>(ns) / 1000.0; }

// ---------------------------------------------------------------------------
// Frames + batches (built OUTSIDE the timed window)
// ---------------------------------------------------------------------------
int64_t wallMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

struct BatchShape {
    int         records    = 16;
    std::size_t valueBytes = 48;
    int64_t     keySpace   = 1 << 30;   ///< distinct keys (compaction: duplicates when small)
    bool        oldTimestamps = false;  ///< ts = 1000 epoch-ms: time retention deletes it
};

std::vector<uint8_t> buildBatch(const BatchShape& s, int64_t& keySeq, int64_t pid = -1,
                                int16_t epoch = 0, int32_t seq = -1, bool txn = false) {
    const int64_t ts = s.oldTimestamps ? 1000 : wallMs();
    rune::protocol::RecordBatchBuilder b(0, ts);
    if (pid >= 0) { b.setProducerId(pid); b.setProducerEpoch(epoch); b.setBaseSequence(seq); }
    if (txn) b.setTransactional(true);
    const std::string v(s.valueBytes, 'v');
    for (int i = 0; i < s.records; ++i) {
        const int64_t k = keySeq++ % s.keySpace;
        b.append("key-" + std::to_string(k), v, ts + i);
    }
    return b.build();
}

std::vector<uint8_t> produceFrame(int64_t corr, const std::vector<uint8_t>& batch, int32_t topicId,
                                  int32_t part, int64_t pid = -1, int16_t epoch = 0, int32_t seq = -1) {
    pc::Produce msg;
    msg.sessionId = 1; msg.correlationId = corr; msg.topicId = topicId; msg.partition = part;
    msg.partitionKnown = 1; msg.acks = 1; msg.producerId = pid; msg.producerEpoch = epoch;
    msg.baseSequence = seq; msg.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> frame(msg.encodedSize());
    msg.encode(frame.data());
    return frame;
}

// ---------------------------------------------------------------------------
// The rig: production worker pass + real off-thread cleaner
// ---------------------------------------------------------------------------
struct PartSpec {
    int32_t       topicId        = 1;
    int32_t       part           = 0;
    int64_t       rollBytes      = 64 * kMiB;
    CleanupPolicy policy         = CleanupPolicy::Delete;
    double        dirtyRatio     = 0.5;
    int64_t       retentionBytes = -1;
    int64_t       retentionMs    = -1;
    bool          controllerManaged = false;   ///< multi-node: ISR channel + leader-epoch fence
};

struct Part {
    PartSpec            spec;
    std::unique_ptr<Log> log;
    PartitionReplica*   rep = nullptr;
    int32_t             epoch = 1;
    // last observed state (flags attribution)
    std::size_t segs = 0;
    uint64_t    gen = 0, truncs = 0;
    int64_t     logStart = 0, activeBase = 0;
    uint64_t    syncCalls = 0;
};

class GateRig {
public:
    explicit GateRig(int64_t cleanerThrottleBytesPerSec = -1)
        : pool_(1, rune::metadata::ShardRouter(1)),
          rt_(noop_, nullptr, nullptr, nullptr, &reg_, nullptr, nullptr, nullptr, nullptr, &pool_),
          // BrokerNode's production cleaner wiring (cycle budget 128 MiB, dedupe =
          // budget, coalescing on (NodeConfig default), 64 MiB fallback roll).
          cleaner_(&reg_, 128 * kMiB, -1, cleanerThrottleBytesPerSec, /*useCoalescing=*/true, 64 * kMiB),
          // hotpath-A F2: BrokerNode's segment provisioner (NodeConfig defaults).
          provisioner_(static_cast<std::size_t>(rune::node::NodeConfig{}.segmentProvisionDepth),
                       rune::node::NodeConfig{}.segmentProvisionCapBytes, fs::temp_directory_path().string(),
                       rune::node::NodeConfig{}.segmentProvisionCapFreePct),
          provisionerIdle_(rune::node::IdleShape{10, 20, 1'000,
                                                 rune::node::NodeConfig{}.segmentProvisionerMaxParkUs * 1'000},
                           nullptr, 0, provisioner_.doorbell()) {
        static int seq = 0;
        dir_ = fs::temp_directory_path() /
               ("rune_latgate_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::remove_all(dir_);
        fs::create_directories(dir_);
        cleaner_.attachViewChannel(rt_.enableCleanerView());
        cleaner_.setExternalProvisioner(true);                       // BrokerNode (hotpath-A)
        provisioner_.attachViewChannel(rt_.enableProvisionerView());
        pool_.setRegistry(&reg_);                   // BrokerNode: routed ops are stamped from the registry
        Worker& w = pool_.worker(0);
        w.setWorkerIngress(nullptr, /*active=*/true);
        w.setShardMode(true);                       // production default (WorkerMode::Shard)
    }
    ~GateRig() {
        stop();
        std::error_code ec;
        fs::remove_all(dir_, ec);
    }

    Part& add(const PartSpec& s) {
        auto p = std::make_unique<Part>();
        p->spec = s;
        const fs::path d = dir_ / (std::to_string(s.topicId) + "_" + std::to_string(s.part));
        fs::create_directories(d);
        p->log = std::make_unique<Log>(d.string(), s.rollBytes);
        p->log->recover();
        PartitionConfig c;
        c.topicId = s.topicId; c.partitionId = s.part; c.selfId = 1; c.minIsr = 1;
        c.replicaLagMaxOffset = 1'000'000'000;
        c.cleanupPolicy = s.policy;
        c.minCleanableDirtyRatio = s.dirtyRatio;
        c.retentionBytes = s.retentionBytes;
        c.retentionMs = s.retentionMs;
        c.segmentRollBytes = s.rollBytes;
        c.retentionCheckMs = 1;     // retention every cleaner cycle (test cadence)
        c.fileDeleteDelayMs = 0;
        c.followerFlushIntervalNs = 0;   // BrokerNode default (durability via replication)
        auto r = std::make_unique<PartitionReplica>(c, Role::Leader, 1, std::vector<int32_t>{1},
                                                    std::vector<int32_t>{1}, p->log.get(), &transport_);
        r->setExternalFollowerFlush(true);          // BrokerNode::applyWorkerDrivenDurability
        p->log->setProvisioning(provisioner_.channel(),
                                rune::node::NodeConfig{}.segmentProvisionThresholdPct);   // ... + hotpath-A F2 / M3
        if (s.controllerManaged) {
            r->setLeaderEpochFenceFile((d / "leader-epoch-fence").string());
            r->setIsrChannel(&transport_);
        }
        p->rep = r.get();
        reg_.add(s.topicId, s.part, std::move(r));
        parts_.push_back(std::move(p));
        return *parts_.back();
    }

    /// Ownership (startup, before the runner — as BrokerNode), first passes, cleaner thread.
    void start() {
        pool_.assignFromRegistry(reg_);
        const int64_t now = clock_.nowNs();
        (void)pool_.worker(0).doWork(now);
        (void)rt_.doWork(now);            // publishes the cleaner's replica view
        drainReturns();
        observe(0);
        runner_ = std::make_unique<rune::consensus::AgentRunner>(cleaner_, cleanerIdle_, cleanerClock_);
        runner_->start();
        provRunner_ = std::make_unique<rune::consensus::AgentRunner>(provisioner_, provisionerIdle_, provClock_);
        provRunner_->start();
        // Review M3: provisioning is demand-driven — nothing is pre-built at
        // startup (an idle partition reserves nothing).
    }
    void stop() {
        if (provRunner_) { provRunner_->stopAndJoin(); provRunner_.reset(); }
        if (runner_) { runner_->stopAndJoin(); runner_.reset(); }
    }

    /// ONE measured duty pass.  `frames` are fed through the worker's ingress
    /// frame handler inside the timed window.  Returns the pass duration.
    int64_t pass(const std::vector<uint8_t>* const* frames, int nFrames) {
        Worker& w = pool_.worker(0);
        const int64_t t0 = clock_.nowNs();
        // hotpath-A: a HELD frame (its partition's roll is deferred — the
        // production controlled poll ABORTs it) is re-delivered first on the
        // next pass, ahead of the new ones (image order); a held frame stops
        // its image, so later frames of the same partition queue behind it.
        heldPtrs_.clear();
        // (The image key is decoded only while something is held — rare.)
        auto feed = [&](const std::vector<uint8_t>* f, int64_t key) {
            if (!heldPtrs_.empty()) {
                if (key == 0) key = imageKey(*f);
                for (const auto& hk : heldPtrs_) if (hk.second == key) { heldPtrs_.emplace_back(f, key); return; }
            }
            if (w.handleIngressFrameForTest(f->data(), f->size(), t0) < 0) {
                heldPtrs_.emplace_back(f, key != 0 ? key : imageKey(*f));
                ++holds;
            }
        };
        for (std::size_t i = 0; i < carried_.size(); ++i) feed(&carried_[i], carriedKeys_[i]);
        for (int i = 0; i < nFrames; ++i) feed(frames[i], 0);
        const int64_t tIntake = clock_.nowNs();
        (void)w.doWork(t0);
        const int64_t tWorker = clock_.nowNs();
        (void)rt_.doWork(t0);
        const int64_t t1 = clock_.nowNs();
        lastSplit = {tIntake - t0, tWorker - tIntake, t1 - tWorker};
        // Outside the timed window: own copies of the frames still held.
        if (!heldPtrs_.empty() || !carried_.empty()) {
            std::vector<std::vector<uint8_t>> next;
            std::vector<int64_t> keys;
            for (const auto& [hp, hk] : heldPtrs_) { next.push_back(*hp); keys.push_back(hk); }
            carried_.swap(next);
            carriedKeys_.swap(keys);
        }
        drainReturns();
        return t1 - t0;
    }
    Split lastSplit{};
    uint64_t holds = 0;   ///< frames held (re-delivered later) — back-pressure, not loss
    /// Review M3: rolls deferred (a roll arrived before its ready segment) and
    /// rolls made inline after a provisioning failure / the cap, summed.
    uint64_t rollDeferrals() const {
        uint64_t n = 0;
        for (const auto& p : parts_) n += p->log->rollDeferralsForTest();
        return n;
    }
    uint64_t failureInlineRolls() const {
        uint64_t n = 0;
        for (const auto& p : parts_) n += p->log->failureInlineRollsForTest();
        return n;
    }
    std::size_t heldFrames() const noexcept { return carried_.size(); }

    /// Flags describing what the last pass did (compared with the previous one).
    uint32_t observe(uint32_t extra) {
        uint32_t f = extra;
        for (auto& p : parts_) {
            const auto snap = p->log->sealedSegmentSnapshot();
            const std::size_t segs = snap ? snap->size() : 0;
            const int64_t active = (snap && !snap->empty()) ? snap->back().endOffset : 0;   // active base
            const uint64_t gen = p->log->installGeneration();
            const uint64_t tr = p->log->contentGeneration();
            const int64_t ls = p->log->logStartOffset();
            const bool truncated = tr != p->truncs;
            const bool rolled = !truncated && active > p->activeBase;
            if (truncated) f |= kTruncate;
            if (rolled) {
                f |= kRoll;
                ++rolls;
                // An adopted pre-provisioned segment carries its staging sequence
                // (LogSegment::prepareStaging); the inline fallback create has -1.
                if (const auto* seg = p->log->segmentForTest(active); seg != nullptr && seg->stagingSeq() < 0) {
                    f |= kSyncCreate;
                    ++syncCreates;
                }
            }
            if (const uint64_t sc = p->log->syncCallsForTest(); sc != p->syncCalls) {
                f |= kInlineFsync;
                inlineFsyncs += sc - p->syncCalls;
                p->syncCalls = sc;
            }
            // Every install bumps the generation: a roll installs the new active
            // segment (one), a compaction install re-installs a sealed group;
            // retention only erases.  The very first append of an empty log
            // installs its first segment (no sealed segment yet — not an install).
            if (!truncated && p->segs > 0 && gen - p->gen > (rolled ? 1u : 0u)) {
                f |= kInstall;
                ++installs;
            }
            if (ls > p->logStart) { f |= kRetention; ++retentionDrops; }
            if (p->rep->truncatePendingForTest()) f |= kTruncPending;
            p->segs = segs; p->gen = gen; p->truncs = tr; p->logStart = ls; p->activeBase = active;
        }
        return f;
    }

    void drainReturns() {
        pool_.drainAllReturns([this](WorkerReturn r) {
            switch (r.type) {
            case WorkerReturn::Type::ProduceResult:
                ++produceAnswers;
                if (r.errorCode != 0) { ++produceErrors; lastProduceError = r.errorCode; }
                break;
            case WorkerReturn::Type::PushReadResult:
                ++readAnswers;
                readBytes += r.chunk.size();
                break;
            case WorkerReturn::Type::ControlRecordAppended:
                if (r.errorCode != 0) ++markerErrors; else ++markersAppended;
                break;
            default: break;
            }
        });
    }

    bool enqueue(WorkerOp op) {
        const int32_t t = op.topicId, p = op.partition;
        return pool_.tryEnqueueOp(t, p, std::move(op));
    }
    void becomeFollower(Part& p) {
        WorkerOp op;
        op.type = WorkerOp::Type::BecomeFollower; op.topicId = p.spec.topicId; op.partition = p.spec.part;
        op.roleEpoch = ++p.epoch; op.roleLeaderId = 2;
        ASSERT_TRUE(enqueue(std::move(op)));
    }
    void becomeLeader(Part& p) {
        WorkerOp op;
        op.type = WorkerOp::Type::BecomeLeader; op.topicId = p.spec.topicId; op.partition = p.spec.part;
        op.roleEpoch = ++p.epoch; op.isr = {1};
        ASSERT_TRUE(enqueue(std::move(op)));
    }
    void pushRead(const Part& p, int64_t fromOffset, int64_t subId) {
        WorkerOp op;
        op.type = WorkerOp::Type::PushRead; op.topicId = p.spec.topicId; op.partition = p.spec.part;
        op.subscriptionId = subId; op.sessionId = 1; op.nextOffset = fromOffset;
        op.hwBound = INT64_MAX; op.isolation = 0; op.logPtr = p.log.get();
        ASSERT_TRUE(enqueue(std::move(op)));
    }

    Worker& worker() { return pool_.worker(0); }
    /// After stop() only.
    [[nodiscard]] uint64_t provisionedForTest() const noexcept { return provisioner_.builtForTest(); }
    std::vector<std::unique_ptr<Part>>& parts() { return parts_; }

    // counters (test thread)
    uint64_t produceAnswers = 0, produceErrors = 0, readAnswers = 0, readBytes = 0;
    uint64_t markersAppended = 0, markerErrors = 0, installs = 0, retentionDrops = 0;
    uint64_t rolls = 0, syncCreates = 0, inlineFsyncs = 0;
    int16_t  lastProduceError = 0;

private:
    class Noop final : public rune::consensus::Agent {
    public:
        int doWork(int64_t) override { return 0; }
    };
    fs::path                               dir_;
    rune::replication::NullPartitionTransport transport_;
    ReplicaRegistry                        reg_;
    WorkerPool                             pool_;
    Noop                                   noop_;
    NodeRuntimeAgent                       rt_;
    LogCleanerAgent                        cleaner_;
    rune::consensus::BackoffIdleStrategy   cleanerIdle_;   // BrokerNode::cleanerIdleStrategy_
    rune::consensus::MonotonicNanoClock    cleanerClock_;
    rune::consensus::MonotonicNanoClock    clock_;
    std::unique_ptr<rune::consensus::AgentRunner> runner_;
    rune::node::SegmentProvisionerAgent    provisioner_;
    rune::node::ParkUntilWorkIdle          provisionerIdle_;   // BrokerNode: doorbell park (review m5)
    rune::consensus::MonotonicNanoClock    provClock_;
    std::unique_ptr<rune::consensus::AgentRunner> provRunner_;
    std::vector<std::unique_ptr<Part>>     parts_;
    std::vector<std::pair<const std::vector<uint8_t>*, int64_t>> heldPtrs_;   // this pass's holds
    std::vector<std::vector<uint8_t>> carried_;                                // owned, re-fed next pass
    std::vector<int64_t>              carriedKeys_;
    /// The image of a frame in this rig = its partition (one client).
    static int64_t imageKey(const std::vector<uint8_t>& f) {
        const auto m = pc::Produce::decode(f.data(), f.size());
        return m ? ((static_cast<int64_t>(m->topicId) << 32) | static_cast<uint32_t>(m->partition)) + 1 : -1;
    }
};

/// One measured pass + its attribution (sequenced: pass, then observe).
void step(GateRig& rig, Stats& st, const std::vector<uint8_t>* const* frames, int n, uint32_t extra) {
    const int64_t ns = rig.pass(frames, n);
    const uint32_t f = rig.observe(extra);
    st.add(ns, f, rig.lastSplit);
}

// ---------------------------------------------------------------------------
// Judging
// ---------------------------------------------------------------------------
struct Outcome {
    Summary hot, cold;
    Stats   stats;
    std::string extra;
};

struct OpGate {
    uint32_t    flag;
    const char* what;
};
constexpr OpGate kOpGates[] = {
    {kInstall,      "a compaction-group install (owner, O(1) since fix round 4)"},
    {kRetention,    "a retention drop (owner validates + posts)"},
    {kTruncPending, "a TruncatePending pass (no fetch / apply, no file op)"},
};
constexpr std::size_t bitOf(uint32_t flag) noexcept {
    std::size_t b = 0;
    while ((flag >> b) != 1u) ++b;
    return b;
}
bool opGatesPass(const std::array<int64_t, 16>& soloBest) {
    for (const auto& g : kOpGates)
        if (soloBest[bitOf(g.flag)] > budget().hotMaxNs) return false;
    return true;
}

bool withinBudget(const std::string& name, const Outcome& o) {
    const Budget& b = budget();
    if (const KnownFailure* k = knownFailure(name))
        return o.hot.max <= k->ceilingMaxNs && o.hot.p999 <= k->ceilingP999Ns && o.cold.max <= b.coldMaxNs;
    return o.hot.max <= b.hotMaxNs && o.hot.p999 <= b.hotP999Ns && o.cold.max <= b.coldMaxNs;
}

void printOutcome(const std::string& name, int attempt, const Outcome& o) {
    std::string worst;
    for (const auto& w : o.stats.worstHot) {
        if (w.ns == 0) continue;
        char buf[192];
        std::snprintf(buf, sizeof buf, " %.1f@%" PRIu64 "(%s; i/w/r=%.0f/%.0f/%.0f)", us(w.ns), w.pass,
                      flagsStr(w.flags).c_str(), us(w.split.intakeNs), us(w.split.workerNs), us(w.split.runtimeNs));
        worst += buf;
    }
    std::string byFlag, solo;
    for (std::size_t b = 1; b < o.stats.flagMax.size(); ++b) {   // bit 0 (produce) = nearly every pass
        char buf[64];
        if (o.stats.flagMax[b] != 0) {
            std::snprintf(buf, sizeof buf, " %s=%.1f", flagsStr(1u << b).c_str(), us(o.stats.flagMax[b]));
            byFlag += buf;
        }
        if (o.stats.soloMax[b] != 0) {
            std::snprintf(buf, sizeof buf, " %s=%.1f(w%.1f)", flagsStr(1u << b).c_str(), us(o.stats.soloMax[b]),
                          us(o.stats.soloWorkMax[b]));
            solo += buf;
        }
    }
    std::printf("[LatencyGate] %-34s %s try=%d passes=%" PRIu64
                " | HOT n=%" PRIu64 " p50=%.1f p99=%.1f p99.9=%.1f max=%.1f us | COLD n=%" PRIu64
                " max=%.1f us (%s) | hot-no-roll max=%.1f | by-flag max:%s | solo max:%s | worst:%s | %s\n",
                name.c_str(), buildName(), attempt, o.stats.passes, o.hot.n, us(o.hot.p50), us(o.hot.p99),
                us(o.hot.p999), us(o.hot.max), o.cold.n, us(o.cold.max),
                o.cold.n ? flagsStr(o.stats.worstCold.flags).c_str() : "-", us(o.stats.hotNoRollMax),
                byFlag.c_str(), solo.c_str(), worst.c_str(), o.extra.c_str());
    std::fflush(stdout);
}

/// Runs `scenario` (one fresh rig per attempt) and judges the best attempt.
void gate(const std::string& name, const std::function<Outcome()>& scenario) {
    if (RUNE_GATE_SANITIZED) GTEST_SKIP() << "latency budgets are meaningless under a sanitizer";
    Outcome best;
    bool have = false;
    // Per-operation gates: the best (lowest) solo max seen per operation over
    // the attempts — a slow operation is slow in every attempt.
    std::array<int64_t, 16> soloBest{};
    for (int attempt = 1; attempt <= kAttempts; ++attempt) {
        Outcome o = scenario();
        if (::testing::Test::HasFatalFailure()) return;
        o.hot = o.stats.hotSummary();
        o.hot.max = std::max(o.hot.max, o.stats.idleMax);   // an uncounted idle pass is still a pass
        o.cold = o.stats.coldSummary();
        printOutcome(name, attempt, o);
        for (std::size_t bit = 0; bit < soloBest.size(); ++bit)
            if (o.stats.soloMax[bit] != 0 && (soloBest[bit] == 0 || o.stats.soloWorkMax[bit] < soloBest[bit]))
                soloBest[bit] = std::max<int64_t>(o.stats.soloWorkMax[bit], 1);
        const bool ok = withinBudget(name, o) && opGatesPass(soloBest);
        if (ok || !have || o.hot.max < best.hot.max) { best = std::move(o); have = true; }
        if (benchMode() || ok) break;
    }
    if (benchMode()) return;
    const Budget& b = budget();
    // The operation each scenario exists for (compaction install, retention
    // drop, a TruncatePending pass, a control-marker append) must fit the HOT
    // budget on its own — judged even when the scenario is known-failing for
    // another reason (the roll path), so the 620 ms install class stays guarded.
    for (const auto& g : kOpGates) {
        const int64_t v = soloBest[bitOf(g.flag)];
        if (v == 0) continue;   // not cleanly attributable in any attempt (always coincided)
        EXPECT_LE(v, b.hotMaxNs) << name << ": the worker part of a pass whose only background operation was "
                                 << g.what << " took " << us(v) << " us > HOT budget " << us(b.hotMaxNs)
                                 << " us (" << buildName() << ")";
        std::printf("[LatencyGate] %s op-gate %s: worker part %.1f us (budget %.1f us)\n", name.c_str(), g.what,
                    us(v), us(b.hotMaxNs));
    }
    if (const KnownFailure* k = knownFailure(name)) {
        std::printf("[LatencyGate] %s KNOWN-FAILING (%s): hot max %.1f us / p99.9 %.1f us vs budget %.1f / %.1f us "
                    "(listed at %.1f / %.1f us; ceiling %.1f / %.1f us) — %s\n",
                    name.c_str(), buildName(), us(best.hot.max), us(best.hot.p999), us(b.hotMaxNs),
                    us(b.hotP999Ns), us(k->measuredMaxNs), us(k->measuredP999Ns), us(k->ceilingMaxNs),
                    us(k->ceilingP999Ns), k->why);
        if (best.hot.max <= b.hotMaxNs && best.hot.p999 <= b.hotP999Ns)
            std::printf("[LatencyGate] %s now MEETS the budget — remove it from kKnownFailures\n", name.c_str());
        EXPECT_LE(best.hot.max, k->ceilingMaxNs)
            << name << ": known-failing scenario's HOT max regressed past its ceiling (" << buildName() << ")";
        EXPECT_LE(best.hot.p999, k->ceilingP999Ns)
            << name << ": known-failing scenario's HOT p99.9 regressed past its ceiling (" << buildName() << ")";
        EXPECT_LE(best.cold.max, b.coldMaxNs)
            << name << ": worst allowed-COLD pass " << us(best.cold.max) << " us > ceiling " << us(b.coldMaxNs);
        return;
    }
    EXPECT_LE(best.hot.max, b.hotMaxNs)
        << name << ": worst HOT duty pass " << us(best.hot.max) << " us > budget " << us(b.hotMaxNs)
        << " us (" << buildName() << "); worst pass did: " << flagsStr(best.stats.worstHot[0].flags);
    EXPECT_LE(best.hot.p999, b.hotP999Ns)
        << name << ": HOT p99.9 " << us(best.hot.p999) << " us > budget " << us(b.hotP999Ns) << " us ("
        << buildName() << ")";
    EXPECT_LE(best.cold.max, b.coldMaxNs)
        << name << ": worst allowed-COLD pass " << us(best.cold.max) << " us > ceiling " << us(b.coldMaxNs)
        << " us (" << buildName() << "); it did: " << flagsStr(best.stats.worstCold.flags);
}

// ---------------------------------------------------------------------------
// Scenario drivers
// ---------------------------------------------------------------------------

/// Pre-encoded produce frames per partition (plain, non-idempotent): reused
/// every pass (the log copies + restamps at append; the frame is never written).
struct FrameSet {
    std::vector<std::vector<uint8_t>> frames;   // one per partition (index = part order)
};
FrameSet framesFor(GateRig& rig, const BatchShape& shape) {
    FrameSet fset;
    int64_t keySeq = 0;
    for (auto& p : rig.parts()) {
        const auto b = buildBatch(shape, keySeq);
        fset.frames.push_back(produceFrame(1, b, p->spec.topicId, p->spec.part));
    }
    return fset;
}

/// Produce-every-pass loop: one frame per partition per pass; `perPass` may add
/// ops (reads) before the pass and returns extra flags; `done` ends the loop
/// (after at least `minPasses`); `maxPasses` caps it (the caller asserts the
/// scenario's own non-vacuity condition afterwards).
struct LoopCfg {
    uint64_t minPasses = 20'000;
    uint64_t maxPasses = 2'000'000;
};
void produceLoop(GateRig& rig, Stats& st, const FrameSet& fset, const LoopCfg& lc,
                 const std::function<uint32_t(uint64_t)>& perPass,
                 const std::function<bool()>& done) {
    std::vector<const std::vector<uint8_t>*> ptrs;
    for (const auto& f : fset.frames) ptrs.push_back(&f);
    for (uint64_t i = 0; i < lc.maxPasses; ++i) {
        uint32_t extra = kProduce;
        if (perPass) extra |= perPass(i);
        const int64_t ns = rig.pass(ptrs.data(), static_cast<int>(ptrs.size()));
        st.add(ns, rig.observe(extra), rig.lastSplit);
        if (i + 1 >= lc.minPasses && (!done || done())) return;
    }
}

uint64_t passes(uint64_t n) { return n * static_cast<uint64_t>(scale()); }

std::string counters(GateRig& rig) {
    // Review M2: fsync attribution per scenario.  The background threads are
    // stopped first (their counters are read only after they stopped).
    rig.stop();
    uint64_t inlineSyncs = 0, roleSyncs = 0, cleanerFsyncs = 0, cleanerDirFsyncs = 0, inPlace = 0, dirMut = 0;
    for (auto& p : rig.parts()) {
        inlineSyncs += p->log->syncCallsForTest();
        roleSyncs += p->log->roleChangeSyncCallsForTest();
        cleanerFsyncs += p->log->cleanerFsyncsForTest();
        cleanerDirFsyncs += p->log->cleanerDirFsyncsForTest();
        inPlace += p->log->inPlaceTruncationsForTest();
        dirMut += p->log->directoryMutationsForTest();
    }
    Worker& w = rig.worker();
    char buf[1024];
    std::snprintf(buf, sizeof buf,
                  "acks=%" PRIu64 " errs=%" PRIu64 "(last %d) installs=%" PRIu64 " retentionDrops=%" PRIu64
                  " reads=%" PRIu64 " readMiB=%.1f markers=%" PRIu64 "/%" PRIu64 "err rolls=%" PRIu64
                  " syncCreates=%" PRIu64 " inlineFsyncs=%" PRIu64 " holds=%" PRIu64 " deferrals=%" PRIu64
                  " failureInlineRolls=%" PRIu64
                  " | FSYNC io: seal=%" PRIu64 " bg=%" PRIu64 " rp=%" PRIu64 " dir=%" PRIu64
                  " epoch=%" PRIu64 " | worker-inline: sync=%" PRIu64 " role=%" PRIu64
                  " | cleaner: fsync=%" PRIu64 " dir=%" PRIu64 " | provisioned=%" PRIu64
                  " | dirMutations=%" PRIu64 " inPlaceTrunc=%" PRIu64,
                  rig.produceAnswers, rig.produceErrors, static_cast<int>(rig.lastProduceError), rig.installs, rig.retentionDrops, rig.readAnswers,
                  static_cast<double>(rig.readBytes) / kMiB, rig.markersAppended, rig.markerErrors, rig.rolls,
                  rig.syncCreates, rig.inlineFsyncs, rig.holds, rig.rollDeferrals(), rig.failureInlineRolls(),
                  w.sealSubmitsForTest(), w.bgFlushSubmitsCountForTest(),
                  w.recoveryPointSubmitsForTest(), w.dirSyncSubmitsForTest(), w.epochStateSubmitsForTest(),
                  inlineSyncs, roleSyncs, cleanerFsyncs, cleanerDirFsyncs, rig.provisionedForTest(), dirMut, inPlace);
    return buf;
}

// ---- 1. steady produce -----------------------------------------------------
Outcome steadyProduce(int nParts) {
    Outcome o;
    GateRig rig;
    for (int i = 0; i < nParts; ++i) rig.add(PartSpec{.topicId = 1, .part = i});
    rig.start();
    const FrameSet fset = framesFor(rig, BatchShape{});
    produceLoop(rig, o.stats, fset, {.minPasses = passes(20'000)}, nullptr, nullptr);
    EXPECT_EQ(rig.produceErrors, 0u) << "last error " << rig.lastProduceError;
    EXPECT_EQ(rig.produceAnswers, o.stats.passes * static_cast<uint64_t>(nParts));
    o.extra = counters(rig);
    return o;
}

// ---- 2. segment roll every few appends (+ 6. reads) ------------------------
Outcome segmentRoll(int nParts, bool reads) {
    Outcome o;
    GateRig rig;
    // ~1 KiB batches, 4 KiB roll: a roll every ~4 appends per partition.
    for (int i = 0; i < nParts; ++i) rig.add(PartSpec{.topicId = 1, .part = i, .rollBytes = 4 * 1024});
    rig.start();
    const FrameSet fset = framesFor(rig, BatchShape{});
    uint64_t rollsBefore = 0;
    for (auto& p : rig.parts()) rollsBefore += p->segs;
    const uint64_t n = passes(nParts > 1 ? 5'000 : 20'000);
    std::function<uint32_t(uint64_t)> perPass;
    if (reads) {
        perPass = [&rig](uint64_t i) -> uint32_t {
            Part& p = *rig.parts()[i % rig.parts().size()];
            const int64_t leo = p.log->logEndOffset();
            // alternate: oldest sealed data / the active tail
            const int64_t from = (i & 1) ? std::max<int64_t>(p.log->logStartOffset(), 0) : std::max<int64_t>(leo - 32, 0);
            rig.pushRead(p, from, static_cast<int64_t>(i));
            return kRead;
        };
    }
    produceLoop(rig, o.stats, fset, {.minPasses = n, .maxPasses = n}, perPass, nullptr);
    uint64_t segs = 0;
    for (auto& p : rig.parts()) segs += p->segs;
    EXPECT_GT(segs, rollsBefore + n / 8) << "precondition: segments rolled every few appends";
    EXPECT_EQ(rig.produceErrors, 0u) << "last error " << rig.lastProduceError;
    if (reads) EXPECT_GT(rig.readBytes, 0u) << "precondition: reads returned data";
    o.extra = counters(rig) + " sealedSegments=" + std::to_string(segs);
    return o;
}

// ---- 3. time + size retention (+ 6. reads) ---------------------------------
Outcome retention(bool reads) {
    Outcome o;
    GateRig rig;
    // T: time retention (old record timestamps, retention.ms = 1).
    rig.add(PartSpec{.topicId = 1, .part = 0, .rollBytes = 8 * 1024, .retentionMs = 1});
    // S: size retention (keep 64 KiB).
    rig.add(PartSpec{.topicId = 1, .part = 1, .rollBytes = 8 * 1024, .retentionBytes = 64 * 1024});
    rig.start();
    FrameSet fset;
    {
        int64_t k = 0;
        fset.frames.push_back(produceFrame(1, buildBatch(BatchShape{.oldTimestamps = true}, k), 1, 0));
        fset.frames.push_back(produceFrame(1, buildBatch(BatchShape{}, k), 1, 1));
    }
    std::function<uint32_t(uint64_t)> perPass;
    if (reads) {
        perPass = [&rig](uint64_t i) -> uint32_t {
            Part& p = *rig.parts()[i % 2];
            const int64_t from = (i & 2) ? p.log->logStartOffset() : std::max<int64_t>(p.log->logEndOffset() - 32, 0);
            rig.pushRead(p, from, static_cast<int64_t>(i));
            return kRead;
        };
    }
    const uint64_t n = passes(20'000);
    produceLoop(rig, o.stats, fset, {.minPasses = n, .maxPasses = n * 20}, perPass,
                [&rig] { return rig.parts()[0]->logStart > 0 && rig.parts()[1]->logStart > 0; });
    EXPECT_GT(rig.parts()[0]->logStart, 0) << "precondition: time retention deleted segments";
    EXPECT_GT(rig.parts()[1]->logStart, 0) << "precondition: size retention deleted segments";
    EXPECT_EQ(rig.produceErrors, 0u) << "last error " << rig.lastProduceError;
    o.extra = counters(rig);
    return o;
}

// ---- 4. compaction (+ 6. reads) -------------------------------------------
Outcome compaction(int64_t groupBytes, int nParts, bool reads) {
    Outcome o;
    GateRig rig;
    for (int i = 0; i < nParts; ++i)
        rig.add(PartSpec{.topicId = 1, .part = i, .rollBytes = groupBytes, .policy = CleanupPolicy::Compact,
                         .dirtyRatio = 0.01});
    rig.start();
    // Fill: 64 KiB batches (8 x 8 KiB records) until every partition holds
    // >= 1.5 sealed rolls; the last ~10% of the fill re-uses the first keys, so
    // the first group shrinks a little and every output group is ~= one roll.
    // Then 1 KiB batches every pass while the cleaner compacts and the owner
    // installs.
    int64_t keySeq = 0;
    const int64_t fillBytes = groupBytes * 5 / 2;
    const BatchShape bigShape{.records = 8, .valueBytes = 8 * 1024,
                              .keySpace = std::max<int64_t>(fillBytes / (8 * 1024) * 9 / 10, 16)};
    FrameSet small;
    {
        int64_t smallKeys = 0;
        for (auto& p : rig.parts())
            small.frames.push_back(produceFrame(1, buildBatch(BatchShape{.keySpace = 4096}, smallKeys),
                                                p->spec.topicId, p->spec.part));
    }
    auto filled = [&] {
        int64_t minBytes = INT64_MAX;
        for (auto& p : rig.parts()) {
            const auto snap = p->log->sealedSegmentSnapshot();
            int64_t b = 0;
            if (snap) for (const auto& si : *snap) b += si.sizeBytes;
            minBytes = std::min(minBytes, b);
        }
        return minBytes >= fillBytes - groupBytes;   // >= 1.5 sealed rolls (+ the active)
    };
    std::vector<std::vector<uint8_t>> fill(rig.parts().size());
    std::vector<const std::vector<uint8_t>*> ptrs(rig.parts().size());
    uint64_t fillPasses = 0;
    while (!filled() && fillPasses < 1'000'000) {
        for (std::size_t i = 0; i < rig.parts().size(); ++i) {
            fill[i] = produceFrame(1, buildBatch(bigShape, keySeq), rig.parts()[i]->spec.topicId,
                                   rig.parts()[i]->spec.part);
            ptrs[i] = &fill[i];
        }
        const int64_t ns = rig.pass(ptrs.data(), static_cast<int>(ptrs.size()));
        o.stats.add(ns, rig.observe(kProduce), rig.lastSplit);
        ++fillPasses;
    }
    EXPECT_TRUE(filled()) << "fill did not complete";
    if (!filled()) return o;
    // Wait (passes, no sleep) for the owner to install compaction groups.
    const uint64_t wantInstalls = static_cast<uint64_t>(nParts);
    std::function<uint32_t(uint64_t)> perPass;
    if (reads) {
        perPass = [&rig](uint64_t i) -> uint32_t {
            Part& p = *rig.parts()[i % rig.parts().size()];
            const int64_t from = (i & 1) ? p.log->logStartOffset() : std::max<int64_t>(p.log->logEndOffset() - 32, 0);
            rig.pushRead(p, from, static_cast<int64_t>(i));
            return kRead;
        };
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::minutes(10);
    // Debug: a 512 KiB Log::read costs ms unoptimised — fewer read passes.
    const uint64_t minPasses = passes(reads && !kReleaseBuild ? 5'000 : 20'000);
    produceLoop(rig, o.stats, small, {.minPasses = minPasses, .maxPasses = 50'000'000}, perPass,
                [&] { return rig.installs >= wantInstalls || std::chrono::steady_clock::now() > deadline; });
    EXPECT_GE(rig.installs, wantInstalls) << "precondition: the owner installed compaction group(s)";
    EXPECT_EQ(rig.produceErrors, 0u) << "last error " << rig.lastProduceError;
    o.extra = counters(rig) + " fillPasses=" + std::to_string(fillPasses);
    return o;
}

// ---- 5. follower truncation (TruncatePending) during a cleaner pass --------
Outcome truncationDuringCleanerPass() {
    Outcome o;
    // A slow (throttled) cleaner: a compaction pass over A lasts seconds, so
    // B's posted file ops are performed only at A's pass boundaries.
    GateRig rig(/*cleanerThrottleBytesPerSec=*/2 * kMiB);
    Part& A = rig.add(PartSpec{.topicId = 1, .part = 0, .rollBytes = 256 * 1024,
                               .policy = CleanupPolicy::Compact, .dirtyRatio = 0.01});
    Part& B = rig.add(PartSpec{.topicId = 2, .part = 0, .rollBytes = 8 * 1024, .retentionBytes = 16 * 1024});
    rig.start();
    (void)A;
    // B leads at a real epoch (epoch cache entry) before it can diverge.
    rig.becomeLeader(B);
    {
        const std::vector<uint8_t>* none[] = {nullptr};
        step(rig, o.stats, none, 0, kRoleOp);
    }
    int64_t keySeq = 0;
    const auto aFrame = produceFrame(1, buildBatch(BatchShape{.records = 4, .valueBytes = 256, .keySpace = 64}, keySeq), 1, 0);
    const auto bFrame = produceFrame(1, buildBatch(BatchShape{}, keySeq), 2, 0);
    const std::vector<uint8_t>* aAndB[] = {&aFrame, &bFrame};
    const std::vector<uint8_t>* onlyA[] = {&aFrame};
    const std::vector<uint8_t>* onlyB[] = {&bFrame};
    // A grows at ~1 MiB/s of wall time (half the cleaner's 2 MiB/s throttle), so
    // the cleaner spends about half its time inside a pass over A, and B's
    // posted file ops are performed at A's pass boundaries.  Pre-fill ~1.5 MiB.
    const auto t0 = std::chrono::steady_clock::now();
    int64_t aBytes = 0;
    const int64_t aPreBytes = 3 * kMiB / 2;
    auto aDue = [&] {
        const double s = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
        return aBytes < aPreBytes + static_cast<int64_t>(s * static_cast<double>(kMiB));
    };
    for (int i = 0; i < 4'000 && aBytes < aPreBytes; ++i) {
        step(rig, o.stats, onlyA, 1, kProduce);
        aBytes += static_cast<int64_t>(aFrame.size());
    }

    enum class St { Leading, Demoting, Diverging, Pending, Promoting } st = St::Leading;
    uint64_t truncations = 0, pendingPasses = 0, pendingEpisodes = 0, iterations = 0, idlePasses = 0;
    bool wasPending = false;
    int64_t lsoAtLead = B.logStart;
    const uint64_t maxPasses = passes(4'000'000);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::minutes(5);
    rune::replication::ErrorPayloadBuf epb;
    uint64_t i = 0;
    for (; i < maxPasses && iterations < 12 && std::chrono::steady_clock::now() < deadline; ++i) {
        uint32_t extra = 0;
        const bool bLeads = st == St::Leading;
        switch (st) {
        case St::Leading:
            // B leads until retention dropped segments (the owner posted
            // their renames to the cleaner) — then demote it at once.
            if (B.logStart > lsoAtLead) { rig.becomeFollower(B); extra |= kRoleOp; st = St::Demoting; }
            break;
        case St::Demoting: {
            // Hot-path B review C1: a follower whose last divergence truncation's
            // history is not durable yet neither fetches nor applies — the new
            // leader's answer can only come after its next fetch.
            if (B.rep->refetchAwaitsEpochStateForTest()) break;
            // Diverging-epoch answer from the new leader: truncate the tail
            // B wrote in its last epoch.  Deferred (TruncatePending) while B's
            // posted file ops are still queued for the cleaner.
            const int64_t leo = B.log->logEndOffset();
            WorkerOp op;
            op.type = WorkerOp::Type::ApplyReplicated; op.topicId = 2; op.partition = 0;
            op.replicatedPayload.leaderEpoch = B.epoch;
            op.replicatedPayload.partition = 0;
            op.replicatedPayload.topicId = 2;
            op.replicatedPayload.errorCode = rune::replication::kErrDivergingEpoch;
            const auto pl = epb.set(B.epoch - 1, std::max<int64_t>(leo - 3, B.logStart));
            op.replicatedPayload.records.assign(pl.begin(), pl.end());
            EXPECT_TRUE(rig.enqueue(std::move(op)));
            extra |= kDiverge;
            st = St::Diverging;
            break;
        }
        case St::Diverging:
        case St::Pending:
            break;
        case St::Promoting:
            rig.becomeLeader(B); extra |= kRoleOp;
            lsoAtLead = B.logStart;
            st = St::Leading;
            ++iterations;
            break;
        }
        const bool a = aDue();
        if (a) aBytes += static_cast<int64_t>(aFrame.size());
        if (a || bLeads) extra |= kProduce;
        const int64_t ns = bLeads ? (a ? rig.pass(aAndB, 2) : rig.pass(onlyB, 1))
                                  : (a ? rig.pass(onlyA, 1) : rig.pass(onlyA, 0));
        const uint32_t f = rig.observe(extra);
        // A pass with no input and nothing observed (A not due, B follower) is
        // still driven — it is what completes the truncation — but not
        // counted: it would only dilute the percentiles with empty passes.
        if ((f & ~kTruncPending) != 0) {
            o.stats.add(ns, f, rig.lastSplit);
        } else {
            ++idlePasses;
            o.stats.noteIdle(ns, f);
        }
        const bool pending = B.rep->truncatePendingForTest();
        if (pending) { ++pendingPasses; if (!wasPending) ++pendingEpisodes; }
        wasPending = pending;
        if (f & kTruncate) ++truncations;
        if ((st == St::Diverging || st == St::Pending) && (f & kTruncate)) st = St::Promoting;
        else if (st == St::Diverging && pending) st = St::Pending;
    }
    EXPECT_GE(truncations, 3u) << "precondition: follower divergence truncations completed";
    EXPECT_GE(pendingEpisodes, 1u) << "precondition: a truncation waited in TruncatePending";
    char buf[256];
    std::snprintf(buf, sizeof buf, " truncations=%" PRIu64 " pendingEpisodes=%" PRIu64 " pendingPasses=%" PRIu64
                  " iterations=%" PRIu64 " idlePasses(uncounted)=%" PRIu64 " idleMax=%.1fus", truncations,
                  pendingEpisodes, pendingPasses, iterations, idlePasses, us(o.stats.idleMax));
    o.extra = counters(rig) + buf;
    return o;
}

// ---- 7. leadership change with epoch bump ----------------------------------
Outcome leadershipChange(int nParts) {
    Outcome o;
    GateRig rig;
    for (int i = 0; i < nParts; ++i)
        rig.add(PartSpec{.topicId = 1, .part = i, .rollBytes = 1 * kMiB, .controllerManaged = true});
    rig.start();
    // Controller-managed partitions start at a NEW epoch (fence persisted).
    // Hot-path B F7: the fence is written through the worker's io backend and
    // the leadership acts once it completed — drive passes until it did
    // (bounded; the completion pass is measured as a HOT pass).
    for (auto& p : rig.parts()) rig.becomeLeader(*p);
    auto allLead = [&] {
        for (auto& p : rig.parts()) if (!p->rep->acceptsProduce()) return false;
        return true;
    };
    {
        const std::vector<uint8_t>* none[] = {nullptr};
        step(rig, o.stats, none, 0, kRoleOp);
        for (int k = 0; k < 2'000'000 && !allLead(); ++k) step(rig, o.stats, none, 0, 0);
        EXPECT_TRUE(allLead()) << "precondition: every partition leads";
        if (!allLead()) return o;
    }
    const FrameSet fset = framesFor(rig, BatchShape{});
    std::vector<const std::vector<uint8_t>*> ptrs;
    std::vector<bool> leading(rig.parts().size(), true);
    uint64_t flips = 0;
    const uint64_t n = passes(30'000);
    // Event-driven cycle (hot-path B F7: a promotion leads once its durable
    // fence completed through the io backend — milliseconds, not passes):
    // demote the victim after it led 200 passes, promote it 50 passes later,
    // move to the next partition once the promotion actually leads.
    enum class Ph { Leading, Demoted, Promoting } ph = Ph::Leading;
    std::size_t victim = 0;
    uint64_t t = 0;
    // At least n passes AND 24 flips (a promotion waits for an fdatasync:
    // milliseconds of idle passes), bounded.
    for (uint64_t i = 0; (i < n || flips < 24) && i < 200 * n; ++i) {
        uint32_t extra = kProduce;
        ++t;
        Part& vp = *rig.parts()[victim];
        if (ph == Ph::Leading && t >= 200 && vp.rep->acceptsProduce()) {
            rig.becomeFollower(vp); leading[victim] = false; extra |= kRoleOp; ++flips; ph = Ph::Demoted; t = 0;
        } else if (ph == Ph::Demoted && t >= 50) {
            rig.becomeLeader(vp); leading[victim] = true; extra |= kRoleOp; ph = Ph::Promoting;
        } else if (ph == Ph::Promoting && vp.rep->acceptsProduce()) {
            ph = Ph::Leading; t = 0; victim = (victim + 1) % rig.parts().size();
        }
        ptrs.clear();
        // Produce only where the partition accepts it (a promotion leads once
        // its durable fence completed — F7): never a refused produce.
        for (std::size_t k = 0; k < rig.parts().size(); ++k)
            if (leading[k] && rig.parts()[k]->rep->acceptsProduce() && !((extra & kRoleOp) != 0 && k == victim))
                ptrs.push_back(&fset.frames[k]);
        const int64_t ns = rig.pass(ptrs.data(), static_cast<int>(ptrs.size()));
        o.stats.add(ns, rig.observe(extra), rig.lastSplit);
    }
    if (ph == Ph::Demoted) {   // end in leadership: promote the demoted victim
        rig.becomeLeader(*rig.parts()[victim]);
        leading[victim] = true;
    }
    {
        const std::vector<uint8_t>* none[] = {nullptr};
        for (int k = 0; k < 2'000'000 && !allLead(); ++k) step(rig, o.stats, none, 0, 0);
    }
    EXPECT_GT(flips, 10u);
    for (auto& p : rig.parts()) EXPECT_EQ(p->rep->role(), Role::Leader);
    o.extra = counters(rig) + " flips=" + std::to_string(flips);
    return o;
}

// ---- 8. idempotent + transactional produce with control markers ------------
Outcome txnMarkers(int nParts) {
    Outcome o;
    GateRig rig;
    for (int i = 0; i < nParts; ++i) rig.add(PartSpec{.topicId = 1, .part = i, .rollBytes = 1 * kMiB});
    rig.start();
    // Let every partition's RC (read_committed / producer-state) build complete.
    {
        const std::vector<uint8_t>* none[] = {nullptr};
        for (int k = 0; k < 64; ++k) step(rig, o.stats, none, 0, 0);
    }
    constexpr int64_t kIdemPid = 1001, kTxnPidBase = 2001;
    std::vector<int32_t> idemSeq(rig.parts().size(), 0), txnSeq(rig.parts().size(), 0);
    std::vector<std::vector<uint8_t>> frames;
    std::vector<const std::vector<uint8_t>*> ptrs;
    int64_t keySeq = 0;
    const BatchShape shape{.records = 4};
    const uint64_t n = passes(20'000);
    int64_t corr = 0;
    for (uint64_t i = 0; i < n; ++i) {
        uint32_t extra = kProduce;
        frames.clear();
        for (std::size_t k = 0; k < rig.parts().size(); ++k) {
            const Part& p = *rig.parts()[k];
            frames.push_back(produceFrame(++corr, buildBatch(shape, keySeq, kIdemPid, 0, idemSeq[k]), 1,
                                          p.spec.part, kIdemPid, 0, idemSeq[k]));
            idemSeq[k] += shape.records;
            const int64_t tpid = kTxnPidBase + static_cast<int64_t>(k);
            frames.push_back(produceFrame(++corr, buildBatch(shape, keySeq, tpid, 0, txnSeq[k], /*txn=*/true), 1,
                                          p.spec.part, tpid, 0, txnSeq[k]));
            txnSeq[k] += shape.records;
            if (i % 8 == 7) {   // end the transaction: COMMIT (3 of 4) / ABORT
                WorkerOp op;
                op.type = WorkerOp::Type::AppendControlRecord; op.topicId = 1; op.partition = p.spec.part;
                op.correlationId = ++corr; op.producerId = tpid;
                op.recordBatchBytes = rune::transaction::ControlMarker::build(
                    tpid, 0, (i % 32 == 31) ? rune::transaction::ControlType::Abort
                                            : rune::transaction::ControlType::Commit, 0);
                EXPECT_TRUE(rig.enqueue(std::move(op)));
                extra |= kMarker;
            }
        }
        ptrs.clear();
        for (const auto& f : frames) ptrs.push_back(&f);
        const int64_t ns = rig.pass(ptrs.data(), static_cast<int>(ptrs.size()));
        o.stats.add(ns, rig.observe(extra), rig.lastSplit);
    }
    // Drain the parked marker answers (they park on the HW).
    {
        const std::vector<uint8_t>* none[] = {nullptr};
        for (int k = 0; k < 64; ++k) step(rig, o.stats, none, 0, 0);
    }
    EXPECT_EQ(rig.produceErrors, 0u) << "last error " << rig.lastProduceError;
    EXPECT_EQ(rig.markersAppended, n / 8 * static_cast<uint64_t>(nParts)) << "every control marker appended";
    EXPECT_EQ(rig.markerErrors, 0u);
    o.extra = counters(rig);
    return o;
}

} // namespace

// ===========================================================================
// The gate.  Suite name carries both filters: *LatencyGate* and *Regression*.
// ===========================================================================
TEST(DutyPassLatencyGateRegression, SteadyProduceSinglePartition) {
    gate("SteadyProduce/1p", [] { return steadyProduce(1); });
}
TEST(DutyPassLatencyGateRegression, SteadyProduceEightPartitions) {
    gate("SteadyProduce/8p", [] { return steadyProduce(8); });
}
TEST(DutyPassLatencyGateRegression, SegmentRollSinglePartition) {
    gate("SegmentRoll/1p", [] { return segmentRoll(1, false); });
}
TEST(DutyPassLatencyGateRegression, SegmentRollEightPartitions) {
    gate("SegmentRoll/8p", [] { return segmentRoll(8, false); });
}
TEST(DutyPassLatencyGateRegression, RetentionTimeAndSize) {
    gate("Retention/time+size/2p", [] { return retention(false); });
}
TEST(DutyPassLatencyGateRegression, Compaction1MiBGroup) {
    gate("Compaction/1MiB/1p", [] { return compaction(1 * kMiB, 1, false); });
}
TEST(DutyPassLatencyGateRegression, Compaction16MiBGroup) {
    gate("Compaction/16MiB/1p", [] { return compaction(16 * kMiB, 1, false); });
}
TEST(DutyPassLatencyGateRegression, Compaction64MiBGroup) {
    gate("Compaction/64MiB/1p", [] { return compaction(64 * kMiB, 1, false); });
}
TEST(DutyPassLatencyGateRegression, Compaction1MiBGroupFourPartitions) {
    gate("Compaction/1MiB/4p", [] { return compaction(1 * kMiB, 4, false); });
}
TEST(DutyPassLatencyGateRegression, FollowerTruncationDuringCleanerPass) {
    gate("FollowerTruncation/cleanerPass/2p", [] { return truncationDuringCleanerPass(); });
}
TEST(DutyPassLatencyGateRegression, ConsumerReadsDuringSegmentRoll) {
    gate("Reads+SegmentRoll/1p", [] { return segmentRoll(1, true); });
}
TEST(DutyPassLatencyGateRegression, ConsumerReadsDuringRetention) {
    gate("Reads+Retention/2p", [] { return retention(true); });
}
TEST(DutyPassLatencyGateRegression, ConsumerReadsDuringCompaction) {
    gate("Reads+Compaction/16MiB/1p", [] { return compaction(16 * kMiB, 1, true); });
}
TEST(DutyPassLatencyGateRegression, LeadershipChangeSinglePartition) {
    gate("LeadershipChange/1p", [] { return leadershipChange(1); });
}
TEST(DutyPassLatencyGateRegression, LeadershipChangeEightPartitions) {
    gate("LeadershipChange/8p", [] { return leadershipChange(8); });
}
TEST(DutyPassLatencyGateRegression, TxnMarkersSinglePartition) {
    gate("TxnMarkers/1p", [] { return txnMarkers(1); });
}
TEST(DutyPassLatencyGateRegression, TxnMarkersFourPartitions) {
    gate("TxnMarkers/4p", [] { return txnMarkers(4); });
}
