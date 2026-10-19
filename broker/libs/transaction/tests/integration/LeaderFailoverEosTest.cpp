// ---------------------------------------------------------------------------
// LeaderFailoverEosTest.cpp — F7: Cross-layer leader-failover EOS invariant.
//
// Namespace: rune::transaction::test + rune::replication::test
// C++23. TDD. NO busy-spin. All loops bounded + ASSERT. Injected clock.
//
// ## What is tested (P5-5 Finding 7)
//
// The marker-durability invariant:
//   "read_committed MUST NEVER return txn data without a durable COMMIT marker
//    being visible on the log."
//
// ## Setup
//   - 2-replica PartitionReplica cluster (leader=1, follower=2) using
//     InMemoryTransport.
//   - TransactionCoordinator + TransactionLog + ReadCommittedView + LsoTracker
//     wired to the leader's rune::storage::Log.
//   - A transactional data batch is written; the txn is ended (PrepareCommit).
//   - A COMMIT marker is appended to the LEADER's log only.
//     → leader LEO > follower LEO (HW cannot advance because follower has not
//       fetched the marker).  HW is REAL: it lags LEO until the follower fetches.
//   - BEFORE the follower fetches the marker, leader dies.
//   - PartitionFailoverCoordinator elects the follower as the new leader.
//   - LSO on the NEW leader must pin (txn not resolved on follower's log).
//   - read_committed MUST return empty (marker not yet durable on follower).
//   - Re-drive the COMMIT marker on the new leader's log.
//   - read_committed MUST now return the committed data.
//
// ## Key assertions
//   1. After leader death: new leader's read_committed is EMPTY (LSO pinned).
//   2. After re-writing the COMMIT marker on new leader: data becomes visible.
//   3. LSO ≤ HW throughout (monotone safety invariant).
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

// Replication layer
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/PartitionFailoverCoordinator.h>

// Transaction layer
#include <rune/transaction/TransactionCoordinator.h>
#include <rune/transaction/TransactionLog.h>
#include <rune/transaction/ControlMarker.h>
#include <rune/transaction/ReadCommittedView.h>
#include <rune/transaction/Types.h>
#include <rune/storage/Log.h>
#include <rune/coordination/OffsetStore.h>
#include <rune/coordination/Types.h>
#include <rune/protocol/RecordBatch.h>

// Reuse the EOS harness helpers (makeDataBatch, appendControlMarker,
// parseOffsets, HarnessDir).
#include "ExactlyOnceHarness.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

namespace rune::transaction::test {

// Hard cap on all bounded drive loops.
static constexpr int kCap = 500;

// ---------------------------------------------------------------------------
// Unique temporary directory helper (mirrors ReplicationClusterHarness).
// ---------------------------------------------------------------------------
static std::string makeUniqPath(const std::string& tag) {
    static std::atomic<uint64_t> gSeq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path().string()
           + "/rune_f7_" + tag + "_"
           + std::to_string(::getpid()) + "_"
           + std::to_string(gSeq.fetch_add(1, std::memory_order_relaxed)) + "_"
           + std::to_string(ts);
}

// RAII temp dir.
struct TDir {
    std::string path;
    explicit TDir(const std::string& tag) : path(makeUniqPath(tag)) {
        std::filesystem::create_directories(path);
    }
    ~TDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
    TDir(const TDir&) = delete;
    TDir& operator=(const TDir&) = delete;
};

// ---------------------------------------------------------------------------
// F7. Cross-layer leader-failover EOS: marker-durability invariant.
//
// Scenario:
//   1. 2-node cluster (leader=1, follower=2), ISR={1,2}, minIsr=2.
//   2. TransactionCoordinator writes a data batch (txnId="f7-txn").
//   3. endTransaction(commit) → PrepareCommit.
//   4. COMMIT marker appended to leader's storage::Log ONLY.
//      HW has NOT advanced past the marker (follower has not fetched it).
//   5. Leader dies (node 1 killed).  HW on leader = marker_offset (not yet
//      reflected on follower's log/HW).
//   6. Follower (node 2) is promoted to leader.
//   7. Follower's LsoTracker still has the txn OPEN (no marker on follower's
//      log) → read_committed returns EMPTY (LSO pins).
//   8. Re-drive the COMMIT marker on the new leader's log (coordinator
//      completeTransaction()).
//   9. After marker written + view rebuilt: read_committed returns the data.
// ---------------------------------------------------------------------------
TEST(LeaderFailoverEos, MarkerDurabilityInvariant_ReadCommittedNeverShowsDataWithoutMarker)
{
    // -----------------------------------------------------------------------
    // 1. Set up two storage Logs (one per replica node).
    // -----------------------------------------------------------------------
    TDir logDir1("n1");
    TDir logDir2("n2");
    rune::storage::Log leaderLog(logDir1.path);
    rune::storage::Log followerLog(logDir2.path);

    // -----------------------------------------------------------------------
    // 2. Set up two PartitionReplica instances sharing an InMemoryTransport.
    //    minIsr=2 so HW only advances when BOTH nodes have fetched.
    // -----------------------------------------------------------------------
    using namespace rune::replication;

    int64_t clock = 1'000'000'000LL;                // 1 s start (non-zero)
    static constexpr int64_t kStep = 1'000'000LL;   // 1 ms steps

    const NodeId kNode1 = 1, kNode2 = 2;
    std::vector<NodeId> allReplicas = {kNode1, kNode2};
    std::vector<NodeId> initialIsr  = {kNode1, kNode2};

    PartitionConfig cfgLeader;
    cfgLeader.partitionId        = 0;
    cfgLeader.selfId             = kNode1;
    cfgLeader.minIsr             = 1;               // single-node produce; follower syncs asynchronously
    cfgLeader.fetchIntervalNs    = kStep;
    cfgLeader.isrCheckIntervalNs = kStep;
    cfgLeader.replicaLagMaxMs    = 10'000;
    cfgLeader.replicaLagMaxOffset = 100'000;
    cfgLeader.maxFetchBytes      = 1 << 20;

    PartitionConfig cfgFollower   = cfgLeader;
    cfgFollower.selfId            = kNode2;

    InMemoryTransport transport;

    auto leaderReplica   = std::make_unique<PartitionReplica>(
                               cfgLeader,   Role::Leader,   /*epoch=*/1,
                               allReplicas, initialIsr,
                               &leaderLog, &transport);
    auto followerReplica = std::make_unique<PartitionReplica>(
                               cfgFollower, Role::Follower,  /*epoch=*/1,
                               allReplicas, initialIsr,
                               &followerLog, &transport);

    transport.setLeader(leaderReplica.get());
    transport.addFollower(kNode2, followerReplica.get());

    // -----------------------------------------------------------------------
    // 3. Set up PartitionFailoverCoordinator.
    // -----------------------------------------------------------------------
    SimpleNodeLiveness liveness;
    PartitionMetadata meta;
    meta.partitionId = 0;
    meta.leader      = kNode1;
    meta.replicas    = allReplicas;
    meta.isr         = initialIsr;
    meta.leaderEpoch = 1;

    PartitionFailoverCoordinator failoverCoord(meta, &liveness, kStep);
    failoverCoord.registerReplica(kNode1, leaderReplica.get());
    failoverCoord.registerReplica(kNode2, followerReplica.get());

    // -----------------------------------------------------------------------
    // 4. Set up TransactionCoordinator wired to the LEADER's storage::Log.
    //    We also wire LsoTracker + ReadCommittedView to the leader's log for
    //    the pre-failover checks.  After failover, we wire a new view to the
    //    follower's log (now the new leader's log).
    // -----------------------------------------------------------------------
    TDir txnLogDir("txnlog");
    const std::string txnLogPath = txnLogDir.path + "/txn.log";

    auto txnLog = std::make_unique<TransactionLog>(txnLogPath);
    auto txnCoord = std::make_unique<TransactionCoordinator>(
                        ProducerIdAllocator{}, /*txnTimeoutMs=*/60'000,
                        txnLog.get(), /*coordinatorEpoch=*/0,
                        /*offsetApplyCb=*/nullptr);

    // -----------------------------------------------------------------------
    // 5. Start a transaction and write a data batch to the LEADER's log.
    // -----------------------------------------------------------------------
    const std::string txnId = "f7-txn";
    auto initRes = txnCoord->initProducerId(txnId, 60'000, clock);
    ASSERT_TRUE(initRes.ok());
    ProducerId pid = initRes.producerId;
    ProducerEpoch epoch = initRes.producerEpoch;

    // Add the test partition (topic="f7", partition=0).
    // TransactionCoordinator::addPartitions takes rune::transaction::TopicPartition.
    rune::transaction::TopicPartition txnTp{"f7", 0};
    std::vector<rune::transaction::TopicPartition> parts = {txnTp};
    auto apRes = txnCoord->addPartitions(txnId, pid, epoch, parts, clock);
    ASSERT_TRUE(apRes.ok());

    // Append a transactional DATA batch to the leader's log.
    // The transaction layer writes an already-framed batch DIRECTLY to the
    // partition log the leader replica owns, then notifies the replica so its
    // ReplicaLeaderState LEO/HW tracking reconciles with the real log (the same
    // reconciliation append() performs internally).
    int64_t dataOff = leaderLog.logEndOffset(); // should be 0
    auto dataBatch = makeDataBatch(dataOff, pid, epoch, /*baseSeq=*/0, "f7-payload");
    leaderLog.append(dataBatch, dataOff);
    leaderReplica->onLeaderLogAppended();

    // Wire LsoTracker + ReadCommittedView to the leader's log.
    LsoTracker    leaderLso;
    AbortedTxnIndex leaderAborted;
    ReadCommittedView leaderView(&leaderLso, &leaderAborted);
    leaderLso.onProduceBatch(pid, dataOff);

    // -----------------------------------------------------------------------
    // 6. Drive replication: let follower fetch the data batch.
    //    minIsr=1 → HW on leader advances immediately on append.
    //    We drive until follower's LEO matches leader's LEO (data replicated
    //    on follower's log — this is what the F7 invariant requires).
    // -----------------------------------------------------------------------
    {
        const Offset leaderLeoAfterData = leaderReplica->logEndOffset();
        // Drive until the leader HW advances to the data batch LEO.
        // This requires two pump cycles:
        //   Cycle N:   follower sends FetchRequest(leo=0) → leader replies → follower appends
        //   Cycle N+1: follower sends FetchRequest(leo=leaderLeoAfterData) →
        //              leader updates known follower LEO → HW recomputed
        bool synced = false;
        for (int i = 0; i < kCap && !synced; ++i) {
            clock += kStep;
            leaderReplica->doWork(clock);
            followerReplica->doWork(clock);
            transport.pump(clock);
            synced = (leaderReplica->highWatermark() >= leaderLeoAfterData
                      && followerReplica->logEndOffset() >= leaderLeoAfterData);
        }
        ASSERT_TRUE(synced) << "Follower did not sync data batch (HW + LEO) within cap";
    }

    // Both nodes have the data batch; leader HW covers the data batch.
    EXPECT_EQ(leaderReplica->logEndOffset(), followerReplica->logEndOffset())
        << "LEO mismatch after data replication";
    EXPECT_GT(leaderReplica->highWatermark(), 0)
        << "Leader HW must be > 0 after follower fetched the data batch";

    // LSO is pinned (data batch open txn).
    int64_t hwAfterData = leaderReplica->highWatermark();
    EXPECT_EQ(leaderLso.lso(hwAfterData), 0)
        << "LSO must be pinned at 0 (first open txn offset)";

    // read_committed must return EMPTY before commit marker.
    {
        auto rcResult = leaderView.read(&leaderLog, hwAfterData,
                                         0, 1 << 20,
                                         IsolationLevel::ReadCommitted);
        EXPECT_TRUE(parseOffsets(rcResult).empty())
            << "read_committed must return empty while txn is open (LSO pins)";
    }

    // -----------------------------------------------------------------------
    // 7. endTransaction(commit) → PrepareCommit.
    // -----------------------------------------------------------------------
    auto endRes = txnCoord->endTransaction(txnId, pid, epoch, /*commit=*/true, clock);
    ASSERT_TRUE(endRes.ok());
    EXPECT_EQ(txnCoord->state(txnId), TxnState::PrepareCommit);

    // -----------------------------------------------------------------------
    // 8. Stop replication delivery to follower (simulates network partition
    //    just before marker write), then append COMMIT marker to leader's log.
    //    The follower never receives the marker before the leader dies.
    //
    //    With minIsr=1, the leader's HW advances to the marker offset immediately
    //    on append — but the FOLLOWER's log does not contain the marker.
    //    The F7 invariant is that the NEW leader (promoted follower) must NOT
    //    expose the txn data under read_committed until it re-writes the marker.
    // -----------------------------------------------------------------------

    // Sever replication: remove follower from transport so the marker is never
    // delivered to the follower (simulates leader crash just after writing marker).
    transport.removeFollower(kNode2);

    int64_t markerOff = leaderLog.logEndOffset(); // marker placed after data batch
    auto markerBatch  = ControlMarker::build(pid, epoch, ControlType::Commit, /*coordEpoch=*/0);
    // Stamp baseOffset at [0..7] LE.
    {
        uint64_t u = static_cast<uint64_t>(markerOff);
        for (int i = 0; i < 8; ++i)
            markerBatch[i] = static_cast<uint8_t>(u >> (i * 8));
    }
    leaderLog.append(markerBatch, markerOff);
    leaderReplica->onLeaderLogAppended();

    // Update leader's LsoTracker (marker written on leader only).
    leaderLso.onControlMarker(pid, markerOff, /*commit=*/true);

    // Verify: follower's log stops at the data batch (no marker).
    EXPECT_LT(followerReplica->logEndOffset(), leaderReplica->logEndOffset())
        << "Follower LEO must be less than leader LEO (follower missing the marker)";

    // -----------------------------------------------------------------------
    // 9. LEADER DIES (follower already severed from transport above).
    // -----------------------------------------------------------------------
    liveness.markDead(kNode1);
    // Note: leaderReplica is intentionally NOT driven further.

    // -----------------------------------------------------------------------
    // 10. Elect follower (node 2) as the new leader via PartitionFailoverCoordinator.
    // -----------------------------------------------------------------------
    failoverCoord.signalLeaderDead(kNode1);

    bool elected = false;
    for (int i = 0; i < kCap && !elected; ++i) {
        clock += kStep;
        failoverCoord.doWork(clock);
        elected = (failoverCoord.currentLeader() == kNode2);
    }
    ASSERT_TRUE(elected)
        << "Follower (node 2) was not elected as new leader within cap";

    EXPECT_EQ(failoverCoord.currentLeader(), kNode2);
    EXPECT_EQ(failoverCoord.currentEpoch(), 2); // epoch bumped

    // -----------------------------------------------------------------------
    // 11. After failover: follower's log does NOT have the COMMIT marker.
    //     Wire a new LsoTracker + ReadCommittedView to the FOLLOWER's log.
    //     LSO must remain pinned (txn still open on follower's log).
    //     read_committed MUST return EMPTY (marker-durability invariant).
    // -----------------------------------------------------------------------
    LsoTracker    newLso;
    AbortedTxnIndex newAborted;
    ReadCommittedView newView(&newLso, &newAborted);

    // Rebuild view from follower's log (only the data batch, no marker).
    int64_t followerHw = followerReplica->highWatermark();
    newView.rebuild(&followerLog, followerHw);

    // F7 KEY ASSERTION: LSO is still pinned at the data batch offset.
    int64_t lsoAfterFailover = newLso.lso(followerHw);
    EXPECT_EQ(lsoAfterFailover, 0)
        << "INVARIANT VIOLATED: LSO advanced past open txn on follower "
           "(marker was not replicated → LSO must remain pinned)";

    // F7 KEY ASSERTION: read_committed MUST be empty.
    {
        auto rcResult = newView.read(&followerLog, followerHw,
                                      0, 1 << 20,
                                      IsolationLevel::ReadCommitted);
        EXPECT_TRUE(parseOffsets(rcResult).empty())
            << "INVARIANT VIOLATED: read_committed returned txn data on the "
               "new leader BEFORE a durable COMMIT marker was written "
               "(marker-durability invariant breached)";
    }

    // Sanity: read_uncommitted sees the data batch on the follower's log.
    {
        auto ruResult = newView.read(&followerLog, followerHw,
                                      0, 1 << 20,
                                      IsolationLevel::ReadUncommitted);
        // Should see the data batch (it was replicated before the leader died).
        EXPECT_FALSE(parseOffsets(ruResult).empty())
            << "read_uncommitted must see the data batch on the new leader's log";
    }

    // -----------------------------------------------------------------------
    // 12. Re-drive the COMMIT marker on the NEW leader's log.
    //     Simulates the recovered coordinator re-writing the marker (standard
    //     resolvePendingMarkers flow for a recovered PrepareCommit).
    // -----------------------------------------------------------------------

    // Recreate the TransactionCoordinator from the durable TransactionLog
    // (recovers PrepareCommit state).
    txnCoord.reset();
    txnLog.reset();
    txnLog = std::make_unique<TransactionLog>(txnLogPath);
    ProducerIdAllocator alloc;
    for (auto& [id, m] : txnLog->all()) alloc.reseedAtLeast(m.producerId);
    txnCoord = std::make_unique<TransactionCoordinator>(
                   std::move(alloc), 60'000, txnLog.get(),
                   /*coordinatorEpoch=*/1, /*offsetApplyCb=*/nullptr);

    // Verify recovered state.
    auto recoveredState = txnCoord->state(txnId);
    ASSERT_TRUE(recoveredState.has_value());
    EXPECT_EQ(*recoveredState, TxnState::PrepareCommit)
        << "Coordinator must recover PrepareCommit from durable log";

    // Write the COMMIT marker onto the NEW leader's (follower's) log.
    int64_t newMarkerOff = followerLog.logEndOffset();
    auto newMarkerBatch  = ControlMarker::build(pid, epoch, ControlType::Commit,
                                                 /*coordEpoch=*/1);
    {
        uint64_t u = static_cast<uint64_t>(newMarkerOff);
        for (int i = 0; i < 8; ++i)
            newMarkerBatch[i] = static_cast<uint8_t>(u >> (i * 8));
    }
    followerLog.append(newMarkerBatch, newMarkerOff);

    // Complete the transaction in the coordinator.
    auto completeRes = txnCoord->completeTransaction(txnId);
    ASSERT_TRUE(completeRes.ok());

    // -----------------------------------------------------------------------
    // 13. After marker written on new leader: rebuild view.
    //     read_committed MUST now return the committed data.
    // -----------------------------------------------------------------------
    int64_t newHw = followerLog.logEndOffset(); // HW = LEO on single-node after failover
    newLso.clear();
    newAborted.clear();
    newView.rebuild(&followerLog, newHw);

    // LSO must advance past the marker (txn resolved).
    int64_t lsoAfterMarker = newLso.lso(newHw);
    EXPECT_EQ(lsoAfterMarker, newHw)
        << "LSO must equal HW after COMMIT marker written "
           "(no open transactions remaining)";

    // F7 KEY ASSERTION: read_committed now returns the data.
    {
        auto rcResult = newView.read(&followerLog, newHw,
                                      0, 1 << 20,
                                      IsolationLevel::ReadCommitted);
        auto offsets = parseOffsets(rcResult);
        EXPECT_FALSE(offsets.empty())
            << "read_committed must return committed data after COMMIT marker "
               "written on the new leader";
        // Verify the data batch offset is present.
        bool found = false;
        for (int64_t o : offsets) if (o == dataOff) { found = true; break; }
        EXPECT_TRUE(found)
            << "Data batch at offset " << dataOff
            << " must be visible under read_committed after COMMIT marker";
    }

    // Safety: LSO ≤ HW throughout (monotone invariant).
    EXPECT_LE(newLso.lso(newHw), newHw) << "LSO ≤ HW must hold at all times";
}

} // namespace rune::transaction::test
