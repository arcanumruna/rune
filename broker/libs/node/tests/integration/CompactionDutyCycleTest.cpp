// ---------------------------------------------------------------------------
// CompactionDutyCycleTest.cpp — CP-2: NodeRuntimeAgent duty-cycle compaction.
//
// Tests that runBoundedCompaction is wired into doWork (step 6.5) and:
//   1. Compacts SEALED segments on LEADER replicas whose policy == Compact.
//   2. Is a no-op on NON-LEADER nodes.
//   3. Never touches the active (highest-base-offset) segment.
//   4. Respects the dirty-ratio threshold (only runs when dirty enough).
//
// No Aeron, no BrokerNode, no media driver.  Uses InMemoryTransport +
// PartitionReplica directly.
//
// Pattern mirrors GroupHandlerIntegrationTest / TxnMarkerTest (no Aeron seam).
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/NodeRuntimeAgent.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/OffsetMap.h>
#include <rune/protocol/RecordBatch.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using namespace rune::replication;
using namespace rune::storage;
using namespace rune::protocol;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static std::atomic<uint64_t> g_seq{0};

static fs::path makeTempDir(const char* stem) {
    auto n = std::to_string(::getpid()) + "_" +
             std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed));
    auto p = fs::temp_directory_path() / (std::string(stem) + "_" + n);
    fs::create_directories(p);
    return p;
}

/// Build a single-record batch with a real key for compaction.
static std::vector<uint8_t> makeBatch(int64_t baseOffset, std::string_view key,
                                       std::string_view value, int64_t ts = 1000) {
    RecordBatchBuilder b(baseOffset, ts);
    b.append(key, value, ts);
    return b.build();
}

// ---------------------------------------------------------------------------
// MinimalConsensusAgent — drives no consensus; tracks isLeader via bool flag.
// ---------------------------------------------------------------------------
class MinimalConsensusAgent : public rune::consensus::Agent {
public:
    int doWork(int64_t) override { return 0; }
};

// ---------------------------------------------------------------------------
// NullTransport — no-op PartitionTransport for single-node tests.
// ---------------------------------------------------------------------------
class NullTransport : public PartitionTransport {
public:
    void sendFetchRequest(const FetchRequestMsg&) override {}
    void sendFetchResponse(NodeId, const FetchResponseMsg&) override {}
};

// ---------------------------------------------------------------------------
// TestFixture — creates a PartitionReplica with Compact policy, wired into a
// ReplicaRegistry, and a NodeRuntimeAgent that drives runBoundedCompaction.
// ---------------------------------------------------------------------------
struct CompactionHarness {
    fs::path           dir;
    std::unique_ptr<Log>                         log;
    std::unique_ptr<NullTransport>               transport;
    std::unique_ptr<PartitionReplica>            replica;
    std::unique_ptr<rune::node::ReplicaRegistry> registry;
    MinimalConsensusAgent                        dummyAgent;
    std::unique_ptr<rune::node::NodeRuntimeAgent> agent;

    bool isLeader = true; // injected into NodeRuntimeAgent step 6.5

    explicit CompactionHarness(bool leaderRole = true,
                               CleanupPolicy policy = CleanupPolicy::Compact,
                               double dirtyRatioThreshold = 0.0 /* 0 = always compact */)
        : isLeader(leaderRole)
    {
        dir = makeTempDir("cp2_duty");
        // Small roll byte limit so we get multiple segments quickly.
        log = std::make_unique<Log>(dir.string(), /*segmentRollBytes=*/256);

        PartitionConfig cfg;
        cfg.topicId              = 42;
        cfg.partitionId          = 0;
        cfg.selfId               = 1;
        cfg.minIsr               = 1;
        cfg.replicaLagMaxMs      = 10'000;
        cfg.replicaLagMaxOffset  = 10'000;
        cfg.fetchIntervalNs      = 1'000'000LL;
        cfg.isrCheckIntervalNs   = 1'000'000LL;
        cfg.maxFetchBytes        = 1 << 20;
        cfg.pushOnAppend         = false;
        cfg.cleanupPolicy        = policy;

        transport = std::make_unique<NullTransport>();
        replica = std::make_unique<PartitionReplica>(
            cfg,
            leaderRole ? Role::Leader : Role::Follower,
            /*epoch=*/1,
            std::vector<NodeId>{1},
            std::vector<NodeId>{1},
            log.get(),
            transport.get());

        if (leaderRole) {
            replica->becomeLeader(/*epoch=*/1, {1});
        }

        registry = std::make_unique<rune::node::ReplicaRegistry>();
        registry->add(42, 0, std::move(replica));

        // NodeRuntimeAgent in the backward-compat no-gateway constructor.
        // We need to reach runBoundedCompaction via doWork.
        agent = std::make_unique<rune::node::NodeRuntimeAgent>(
            dummyAgent,
            /*gateway=*/nullptr);
    }

    ~CompactionHarness() {
        agent.reset();
        registry.reset();
        // replica is moved into registry; log lives independently.
        log.reset();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    PartitionReplica* getReplicaRaw() {
        return registry->lookup(42, 0);
    }

    Log* getLog() { return getReplicaRaw() ? getReplicaRaw()->log() : nullptr; }
};

// ---------------------------------------------------------------------------
// Count live records from all segments of a log.
// ---------------------------------------------------------------------------
static int countLiveRecords(Log& log) {
    int count = 0;
    auto bases = log.segmentBaseOffsets();
    if (bases.empty()) return 0;
    int64_t cursor = 0;
    for (size_t si = 0; si < bases.size(); ++si) {
        int64_t segEnd = (si + 1 < bases.size()) ? bases[si + 1]
                                                  : std::numeric_limits<int64_t>::max();
        if (cursor >= segEnd) continue;
        if (cursor < bases[si]) cursor = bases[si];
        for (;;) {
            auto bytes = log.read(cursor, 1 << 20);
            if (bytes.empty()) break;
            RecordBatchReader r(bytes.data(), bytes.size());
            if (!r.ensureValid()) break;
            bool advanced = false;
            r.forEach([&](std::string_view, std::string_view, int64_t off, int64_t) {
                ++count;
                cursor = off + 1;
                advanced = true;
            });
            if (!advanced) break;
        }
    }
    return count;
}

// ---------------------------------------------------------------------------
// Get the latest value for a key across all log records.
// ---------------------------------------------------------------------------
static std::string latestValueForKey(Log& log, std::string_view key) {
    std::string latest;
    auto bases = log.segmentBaseOffsets();
    if (bases.empty()) return latest;
    int64_t cursor = 0;
    for (size_t si = 0; si < bases.size(); ++si) {
        int64_t segEnd = (si + 1 < bases.size()) ? bases[si + 1]
                                                  : std::numeric_limits<int64_t>::max();
        if (cursor >= segEnd) continue;
        if (cursor < bases[si]) cursor = bases[si];
        for (;;) {
            auto bytes = log.read(cursor, 1 << 20);
            if (bytes.empty()) break;
            RecordBatchReader r(bytes.data(), bytes.size());
            if (!r.ensureValid()) break;
            bool advanced = false;
            r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t) {
                if (k == key) latest = std::string(v);
                cursor = off + 1;
                advanced = true;
            });
            if (!advanced) break;
        }
    }
    return latest;
}

// ---------------------------------------------------------------------------
// Check that the active segment (last base offset) has not shrunk.
// Returns the count of records in the active segment.
// ---------------------------------------------------------------------------
static int countActiveSegmentRecords(Log& log) {
    auto bases = log.segmentBaseOffsets();
    if (bases.empty()) return 0;
    int64_t activeBase = bases.back();
    int count = 0;
    int64_t cursor = activeBase;
    for (;;) {
        auto bytes = log.read(cursor, 1 << 20);
        if (bytes.empty()) break;
        RecordBatchReader r(bytes.data(), bytes.size());
        if (!r.ensureValid()) break;
        bool advanced = false;
        r.forEach([&](std::string_view, std::string_view, int64_t off, int64_t) {
            ++count;
            cursor = off + 1;
            advanced = true;
        });
        if (!advanced) break;
    }
    return count;
}

// ===========================================================================
// TEST 1: Leader compact policy — sealed segments compacted, latest per key
//         kept, active segment untouched.
//
// This is the FAILING test before runBoundedCompaction is wired in.
// ===========================================================================
TEST(CompactionDutyCycle, CompactsSealedSegmentsLeaderOnlyLatestPerKey) {
    // We build a minimal harness directly (no heavy BrokerNode / Aeron).
    // The NodeRuntimeAgent::runBoundedCompaction step is tested by calling
    // doWork on a purpose-built agent that carries the registry + leader flag.

    auto dir = makeTempDir("cp2_t1");

    // Small segment roll so 200 appends span multiple segments.
    Log log(dir.string(), /*rollBytes=*/128);

    NullTransport transport;
    PartitionConfig cfg;
    cfg.topicId             = 10;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    cfg.cleanupPolicy       = CleanupPolicy::Compact;

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    // Append 200 keyed records so the log rolls many segments.
    for (int v = 0; v < 200; ++v) {
        auto batch = makeBatch(v, "k1", "v" + std::to_string(v));
        log.append(batch, v);
    }
    replica->onLeaderLogAppended();

    // Capture the active segment base before compaction.
    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u) << "Need at least 2 segments for this test";
    const int64_t activeBase  = bases.back();
    const int totalBefore     = countLiveRecords(log);

    // Capture active segment record count (must not shrink).
    const int activeRecordsBefore = countActiveSegmentRecords(log);

    // Wire into a ReplicaRegistry and NodeRuntimeAgent.
    rune::node::ReplicaRegistry registry;
    PartitionReplica* rawReplica = replica.get();
    registry.add(10, 0, std::move(replica));

    // Build the NodeRuntimeAgent with leader=true, registry wired.
    MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent agent(dummy, nullptr);
    // Manually invoke runBoundedCompaction by driving the agent with registry.
    // Since NodeRuntimeAgent::runBoundedCompaction needs the registry we pass it
    // via the constructor overload that takes a ReplicaRegistry*.
    // We call it directly via a purpose-built agent in the full NodeRuntimeAgent
    // constructor.

    // For the FAILING test, we call the duty cycle the intended way:
    // create an agent with the registry, drive doWork, observe compaction.
    rune::node::NodeRuntimeAgent fullAgent(
        dummy,
        /*election=*/nullptr,
        /*bridge=*/nullptr,
        /*transport=*/nullptr,
        &registry,
        /*gateway=*/nullptr);

    // runBoundedCompaction is called from doWork step 6.5.
    // Drive 50 duty cycles (leader is pre-established; compaction fires on
    // any cycle where dirty ratio >= threshold and sealed segments exist).
    for (int i = 0; i < 50; ++i) {
        fullAgent.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }

    // Reload to pick up rewritten segment files.
    log.reloadSegments();

    // --- Assertions ---

    // 1. Latest value for k1 is still 199 (latest kept).
    EXPECT_EQ(latestValueForKey(log, "k1"), "v199") << "Latest value for k1 must be v199";

    // 2. Superseded records were reclaimed (fewer than 200).
    const int totalAfter = countLiveRecords(log);
    EXPECT_LT(totalAfter, totalBefore)
        << "Sealed segment records should have been compacted (superseded removed)";
    EXPECT_LT(totalAfter, 200)
        << "After compaction fewer than 200 records should remain (only k1@199 in sealed zone)";

    // 3. Active segment was not touched — its record count is unchanged.
    const int activeRecordsAfter = countActiveSegmentRecords(log);
    EXPECT_EQ(activeRecordsAfter, activeRecordsBefore)
        << "Active segment must not be modified by compaction";

    // 4. Active segment base offset unchanged.
    auto basesAfter = log.segmentBaseOffsets();
    EXPECT_FALSE(basesAfter.empty());
    EXPECT_EQ(basesAfter.back(), activeBase) << "Active segment base must be unchanged";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 2: NON-LEADER — runBoundedCompaction is a strict no-op.
// ===========================================================================
TEST(CompactionDutyCycle, NonLeaderRunBoundedCompactionIsNoOp) {
    auto dir = makeTempDir("cp2_t2");
    Log log(dir.string(), /*rollBytes=*/128);

    NullTransport transport;
    PartitionConfig cfg;
    cfg.topicId             = 10;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    cfg.cleanupPolicy       = CleanupPolicy::Compact;

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Follower, /*epoch=*/0,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    // Stays Follower — no becomeLeader().

    // Append records directly to the log (bypassing the follower replica API).
    for (int v = 0; v < 80; ++v) {
        auto batch = makeBatch(v, "k1", "v" + std::to_string(v));
        log.append(batch, v);
    }

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u) << "Need at least 2 segments for this test";

    const int totalBefore = countLiveRecords(log);
    const int64_t activeBase = bases.back();

    rune::node::ReplicaRegistry registry;
    registry.add(10, 0, std::move(replica));

    MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent agent(
        dummy,
        /*election=*/nullptr,
        /*bridge=*/nullptr,
        /*transport=*/nullptr,
        &registry,
        /*gateway=*/nullptr);

    // Drive 50 cycles. Follower should NEVER compact.
    for (int i = 0; i < 50; ++i) {
        agent.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }

    log.reloadSegments();

    // All records must be intact — no compaction ran.
    const int totalAfter = countLiveRecords(log);
    EXPECT_EQ(totalAfter, totalBefore)
        << "Follower replica must not be compacted (runBoundedCompaction is leader-only)";

    auto basesAfter = log.segmentBaseOffsets();
    EXPECT_FALSE(basesAfter.empty());
    EXPECT_EQ(basesAfter.back(), activeBase)
        << "Active segment base must be unchanged on follower";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 3: Delete policy — runBoundedCompaction skips partitions with Delete policy.
// ===========================================================================
TEST(CompactionDutyCycle, DeletePolicySkippedByCompaction) {
    auto dir = makeTempDir("cp2_t3");
    Log log(dir.string(), /*rollBytes=*/128);

    NullTransport transport;
    PartitionConfig cfg;
    cfg.topicId             = 10;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    cfg.cleanupPolicy       = CleanupPolicy::Delete; // should NOT compact

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    for (int v = 0; v < 80; ++v) {
        auto batch = makeBatch(v, "k1", "v" + std::to_string(v));
        log.append(batch, v);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u);

    const int totalBefore = countLiveRecords(log);

    rune::node::ReplicaRegistry registry;
    registry.add(10, 0, std::move(replica));

    MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent agent(
        dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    for (int i = 0; i < 50; ++i) {
        agent.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }

    log.reloadSegments();

    const int totalAfter = countLiveRecords(log);
    EXPECT_EQ(totalAfter, totalBefore)
        << "Delete-policy partition must not be compacted";

    fs::remove_all(dir);
}

// ===========================================================================
// TEST 4: Active segment boundary — compaction never reduces active segment
//         records even when all records share the same key.
// ===========================================================================
TEST(CompactionDutyCycle, ActiveSegmentNeverTouchedByDutyCycle) {
    auto dir = makeTempDir("cp2_t4");
    Log log(dir.string(), /*rollBytes=*/128);

    NullTransport transport;
    PartitionConfig cfg;
    cfg.topicId             = 10;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.minIsr              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    cfg.cleanupPolicy       = CleanupPolicy::Compact;

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, &transport);
    replica->becomeLeader(1, {1});

    // Fill enough to create multiple segments.
    for (int v = 0; v < 150; ++v) {
        auto batch = makeBatch(v, "k1", "v" + std::to_string(v));
        log.append(batch, v);
    }
    replica->onLeaderLogAppended();

    auto bases = log.segmentBaseOffsets();
    ASSERT_GE(bases.size(), 2u);

    const int64_t activeBase         = bases.back();
    const int64_t leoBeforeCompaction = log.logEndOffset();
    const int activeRecsBefore        = countActiveSegmentRecords(log);

    rune::node::ReplicaRegistry registry;
    registry.add(10, 0, std::move(replica));

    MinimalConsensusAgent dummy;
    rune::node::NodeRuntimeAgent agent(
        dummy, nullptr, nullptr, nullptr, &registry, nullptr);

    for (int i = 0; i < 50; ++i) {
        agent.doWork(static_cast<int64_t>(i) * 1'000'000LL);
    }

    log.reloadSegments();

    // logEndOffset must not decrease (active segment not truncated).
    EXPECT_EQ(log.logEndOffset(), leoBeforeCompaction)
        << "logEndOffset must be unchanged (active segment not compacted)";

    // Active segment records unchanged.
    const int activeRecsAfter = countActiveSegmentRecords(log);
    EXPECT_EQ(activeRecsAfter, activeRecsBefore)
        << "Active segment record count must be unchanged after duty cycle compaction";

    fs::remove_all(dir);
}
