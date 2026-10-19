// ---------------------------------------------------------------------------
// F15TxnRegressionTest.cpp — F15 step S0: failing *Regression* tests that pin
// the EOS marker / LSO defects at the fastest deterministic layer (pure unit:
// ReadCommittedView, LsoTracker, PartitionReplica, TransactionCoordinator).
//
// Design note: docs/superpowers/specs/2026-10-01-f15-eos-marker-lso-design.md
//
// These tests are EXPECTED RED on b04f871 (S0 lands tests only; fixes come in
// S1-S8 after the design review).  No sleeps, no spins.
// ---------------------------------------------------------------------------

#include <rune/transaction/ControlMarker.h>
#include <rune/transaction/ReadCommittedView.h>
#include <rune/transaction/TransactionCoordinator.h>
#include <rune/transaction/TransactionLog.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <gtest/gtest.h>

#include <atomic>
#include <climits>
#include <cstdint>
#include <filesystem>
#include <map>
#include <random>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::transaction;
namespace fs  = std::filesystem;
namespace rep = rune::replication;

namespace {

struct TmpDir {
    fs::path path;
    TmpDir() {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() /
               ("rune_f15tx_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};

class NullTransport final : public rep::PartitionTransport {
public:
    bool sendFetchRequest(const rep::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rep::NodeId, const rep::FetchResponseMsg&) override { return true; }
};

rep::PartitionConfig singleNodeCfg() {
    rep::PartitionConfig c;
    c.topicId = 1; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
    c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000LL;
    c.replicaLagMaxMs = 1'000'000; c.pushOnAppend = false;
    return c;
}

// A data batch with a producerId; `transactional` sets ONLY the TRANSACTIONAL
// attribute (0x10) — an idempotent (non-transactional) producer leaves it clear.
std::vector<uint8_t> dataBatch(int64_t base, int64_t pid, bool transactional) {
    rune::protocol::RecordBatchBuilder b(base, 0);
    b.setProducerId(pid);
    b.setProducerEpoch(0);
    b.setBaseSequence(0);
    b.setTransactional(transactional);
    b.append({}, "v", 0);
    return b.build();
}

int64_t appendRaw(rune::storage::Log& log, const std::vector<uint8_t>& batchAtLeo) {
    const int64_t off = log.logEndOffset();
    auto b = batchAtLeo;
    for (int i = 0; i < 8; ++i) b[i] = static_cast<uint8_t>(static_cast<uint64_t>(off) >> (8 * i));
    log.append(b, off);
    return off;
}

// Count the batches a read returned, and whether any of them is a control batch.
struct BatchScan { int batches = 0; int control = 0; };
BatchScan scanBatches(const std::vector<uint8_t>& raw) {
    BatchScan s;
    std::size_t pos = 0;
    while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= raw.size()) {
        rune::protocol::RecordBatchReader r(raw.data() + pos, raw.size() - pos);
        const int32_t total = r.totalLength();
        if (total <= 0 || pos + static_cast<std::size_t>(total) > raw.size()) break;
        ++s.batches;
        if (ControlMarker::isControlBatch(raw.data(), pos, raw.size() - pos)) ++s.control;
        pos += static_cast<std::size_t>(total);
    }
    return s;
}

} // namespace

// ===========================================================================
// N3 — ReadCommittedView::rebuild must require the TRANSACTIONAL attribute.
// ===========================================================================

TEST(ControlBatchRegression, RebuildIgnoresIdempotentNonTxnBatches) {
    // An IDEMPOTENT (pid >= 0) but NON-transactional producer never opens a
    // transaction.  The incremental gateway path already checks the 0x10 bit;
    // rebuild() must too, or an idempotent producer pins the LSO forever after
    // any rebuild (and read_committed consumers stall).
    TmpDir d;
    rune::storage::Log log(d.path.string(), 64LL * 1024 * 1024);
    log.recover();
    appendRaw(log, dataBatch(0, /*pid=*/42, /*transactional=*/false));
    appendRaw(log, dataBatch(0, /*pid=*/42, /*transactional=*/false));
    LsoTracker tracker;
    AbortedTxnIndex aborted;
    ReadCommittedView view(&tracker, &aborted);
    view.rebuild(&log, /*hw=*/2);
    EXPECT_EQ(tracker.openCount(), 0u) << "an idempotent (non-transactional) producer opens no transaction";
    EXPECT_EQ(view.lso(2), 2) << "LSO == HW: nothing is pending";
}

// ===========================================================================
// N2 — a marker appended through PartitionReplica::append must land as a REAL
// control batch (CONTROL|TRANSACTIONAL, the marker's producerId), never as the
// value of a wrapper data batch (pid -1, no CONTROL bit).
// ===========================================================================

TEST(ControlBatchRegression, ReplicaAppendedMarkerIsAControlBatchInTheLog) {
    TmpDir d;
    rune::storage::Log log(d.path.string(), 64LL * 1024 * 1024);
    log.recover();
    NullTransport t;
    rep::PartitionReplica leader(singleNodeCfg(), rep::Role::Leader, 1, {1}, {1}, &log, &t);
    const auto marker = ControlMarker::build(/*pid=*/7, /*epoch=*/0, ControlType::Abort, /*coordEpoch=*/0);
    ASSERT_TRUE(ControlMarker::isControlBatch(marker.data(), 0, marker.size())) << "precondition: input is a control batch";
    const auto base = leader.append(std::span<const uint8_t>(marker), /*acksAll=*/true);
    ASSERT_EQ(base, 0);
    const auto raw = log.read(0, 1 << 20);
    ASSERT_FALSE(raw.empty());
    rune::protocol::RecordBatchReader r(raw.data(), raw.size());
    EXPECT_TRUE(ControlMarker::isControlBatch(raw.data(), 0, raw.size()))
        << "the log must hold a control batch (got attrs=0x" << std::hex << r.attributes() << std::dec
        << " producerId=" << r.producerId() << ")";
    EXPECT_EQ(r.producerId(), 7) << "the marker's producerId, not a wrapper's -1";
}

TEST(ControlBatchRegression, ReplicaAppendedAbortMarkerClosesTheTxnOnRebuild) {
    // txn data (pid 7, TRANSACTIONAL) then an ABORT marker through the replica
    // append path.  A rebuild must see the marker: txn closed, range recorded.
    TmpDir d;
    rune::storage::Log log(d.path.string(), 64LL * 1024 * 1024);
    log.recover();
    NullTransport t;
    rep::PartitionReplica leader(singleNodeCfg(), rep::Role::Leader, 1, {1}, {1}, &log, &t);
    appendRaw(log, dataBatch(0, 7, /*transactional=*/true));
    leader.onLeaderLogAppended();
    const auto marker = ControlMarker::build(7, 0, ControlType::Abort, 0);
    ASSERT_EQ(leader.append(std::span<const uint8_t>(marker), true), 1);
    LsoTracker tracker;
    AbortedTxnIndex aborted;
    ReadCommittedView view(&tracker, &aborted);
    view.rebuild(&log, log.logEndOffset());
    EXPECT_EQ(tracker.openCount(), 0u) << "the ABORT marker closes pid 7's transaction";
    ASSERT_EQ(aborted.size(), 1u) << "the aborted range is recorded";
    EXPECT_TRUE(aborted.isAborted(7, 0));
}

TEST(ControlBatchRegression, ConsumerNeverReceivesMarkerBytes) {
    // read_uncommitted: control batches are NEVER returned.  A marker wrapped as
    // a data batch is delivered to the consumer as one record holding the
    // marker's bytes.
    TmpDir d;
    rune::storage::Log log(d.path.string(), 64LL * 1024 * 1024);
    log.recover();
    NullTransport t;
    rep::PartitionReplica leader(singleNodeCfg(), rep::Role::Leader, 1, {1}, {1}, &log, &t);
    const auto marker = ControlMarker::build(7, 0, ControlType::Commit, 0);
    ASSERT_EQ(leader.append(std::span<const uint8_t>(marker), true), 0);
    LsoTracker tracker;
    AbortedTxnIndex aborted;
    ReadCommittedView view(&tracker, &aborted);
    const auto out = view.read(&log, log.logEndOffset(), 0, 1 << 20, IsolationLevel::ReadUncommitted);
    const auto s = scanBatches(out);
    EXPECT_EQ(s.batches, 0) << "the marker must not reach a read_uncommitted consumer ("
                            << s.batches << " batch(es), " << out.size() << " bytes delivered)";
}

// ===========================================================================
// Found on sight (N4): LsoTracker::clear() clears openTxns_ but NOT the
// byFirstOffset_ min-index, so after a rebuild (which calls clear()) a stale
// first-offset pins the LSO below a live transaction's real first offset.
// ===========================================================================

TEST(LsoTrackerRegression, ClearDropsTheFirstOffsetIndex) {
    LsoTracker tracker;
    tracker.onProduceBatch(/*pid=*/1, /*base=*/10);
    tracker.clear();
    tracker.onProduceBatch(/*pid=*/2, /*base=*/50);
    EXPECT_EQ(tracker.openCount(), 1u);
    EXPECT_EQ(tracker.lso(100), 50) << "pid 1 was cleared: its first offset (10) must not pin the LSO";
}

// ===========================================================================
// m2 — epoch-overflow fence: the NEW identity the client already holds gets
// ConcurrentTransactions (retriable) while the old txn's PrepareAbort resolves,
// not InvalidPid (fatal).
// ===========================================================================

namespace {
struct OverflowFenced {
    TmpDir d;
    fs::path path = d.path / "txn.log";
    std::unique_ptr<TransactionLog> log;
    std::unique_ptr<TransactionCoordinator> coord;
    ProducerId newPid = -1;
    ProducerEpoch newEpoch = -1;
    OverflowFenced() {
        {
            TransactionLog seed(path.string());
            TxnLogEntry e;
            e.txnId = "t-ovf"; e.producerId = 42; e.producerEpoch = INT16_MAX;
            e.state = TxnState::Ongoing; e.txnTimeoutMs = 60'000; e.txnStartNs = 1'000;
            e.partitions = {TopicPartition{"t", 0}};
            seed.append(e);
        }
        log   = std::make_unique<TransactionLog>(path.string());
        coord = std::make_unique<TransactionCoordinator>(ProducerIdAllocator{100}, 60'000, log.get(), 3);
        const auto r = coord->initProducerId(std::string("t-ovf"), 60'000, 0);
        if (r.ok()) { newPid = r.producerId; newEpoch = r.producerEpoch; }
    }
};
constexpr int kCoordConcurrentTransactions = InitProducerIdResult::kConcurrentTransactions;   // 6
} // namespace

TEST(TxnCoordinatorFailoverRegression, OverflowFenceNewPidGetsConcurrentTransactionsOnAddPartitions) {
    OverflowFenced f;
    ASSERT_NE(f.newPid, 42) << "precondition: overflow allocated a fresh pid";
    ASSERT_EQ(f.coord->state("t-ovf"), TxnState::PrepareAbort) << "precondition: old txn still aborting";
    const auto r = f.coord->addPartitions("t-ovf", f.newPid, f.newEpoch, {TopicPartition{"t", 1}}, 0);
    EXPECT_EQ(r.errorCode, kCoordConcurrentTransactions)
        << "the client holds the NEW identity; while the fence's PrepareAbort resolves it must retry "
           "(ConcurrentTransactions), not be told its pid is invalid";
}

TEST(TxnCoordinatorFailoverRegression, OverflowFenceNewPidGetsConcurrentTransactionsOnEndTxn) {
    OverflowFenced f;
    ASSERT_EQ(f.coord->state("t-ovf"), TxnState::PrepareAbort);
    const auto r = f.coord->endTransaction("t-ovf", f.newPid, f.newEpoch, /*commit=*/false, 0);
    EXPECT_EQ(r.errorCode, kCoordConcurrentTransactions);
}

// ===========================================================================
// I4 (EndTxn idempotency, Kafka): Complete* is the resting state, so a client
// that lost the EndTxn response and retries the SAME direction gets OK.
// ===========================================================================

TEST(TxnCoordinatorFailoverRegression, EndTxnRetryAfterCompleteSameDirectionIsOkAtCoordinator) {
    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000);
    const auto ip = coord.initProducerId(std::string("t-idem"), 60'000, 0);
    ASSERT_TRUE(ip.ok());
    ASSERT_TRUE(coord.addPartitions("t-idem", ip.producerId, ip.producerEpoch, {TopicPartition{"t", 0}}, 0).ok());
    ASSERT_TRUE(coord.endTransaction("t-idem", ip.producerId, ip.producerEpoch, true, 0).ok());
    ASSERT_TRUE(coord.completeTransaction("t-idem").ok());
    EXPECT_EQ(coord.state("t-idem"), TxnState::CompleteCommit) << "Complete* is the resting state (Kafka)";
    const auto retry = coord.endTransaction("t-idem", ip.producerId, ip.producerEpoch, /*commit=*/true, 0);
    EXPECT_EQ(retry.errorCode, EndTransactionResult::kOk)
        << "a same-direction EndTxn retry after completion is idempotent OK (got " << retry.errorCode << ")";
}

// ===========================================================================
// F15 S1 (N4, §2.2): the compact tracker (flat pid table + first-offset FIFO)
// must match a reference model under churn: growth, in-place compaction behind
// a long-open front, out-of-order registration, re-open after close, clear().
// ===========================================================================

TEST(LsoTrackerRegression, CompactTrackerMatchesReferenceModelUnderChurn) {
    LsoTracker tracker(/*openTxnCapacity=*/4);       // tiny: forces compaction + growth
    std::map<ProducerId, int64_t> model;             // pid -> firstOffset
    std::mt19937_64 rng(12345);
    int64_t offset = 0;
    auto modelLso = [&](int64_t hw) {
        int64_t m = hw;
        for (const auto& [p, f] : model) m = std::min(m, f);
        return std::max<int64_t>(0, std::min(m, hw));
    };
    // A long-open transaction at the front pins the FIFO head while many others churn.
    tracker.onProduceBatch(1'000'000, offset); model[1'000'000] = offset; ++offset;
    for (int step = 0; step < 20'000; ++step) {
        const auto op = rng() % 10;
        const ProducerId pid = static_cast<ProducerId>(rng() % 64);
        if (op < 5) {
            // In-order (L1) registration; ~1/16 of them out of order.
            const bool ooo = (rng() % 16) == 0 && offset > 10;
            const int64_t at = ooo ? offset - 1 - static_cast<int64_t>(rng() % 10) : offset++;
            tracker.onProduceBatch(pid, at);
            if (!model.count(pid)) model[pid] = at;
        } else if (op < 9) {
            const int64_t got = tracker.onControlMarker(pid, offset++, (rng() & 1) != 0);
            const auto it = model.find(pid);
            ASSERT_EQ(got, it == model.end() ? -1 : it->second) << "step " << step;
            if (it != model.end()) model.erase(it);
        } else if (step % 997 == 0) {
            tracker.clear();
            model.clear();
        }
        ASSERT_EQ(tracker.openCount(), model.size()) << "step " << step;
        ASSERT_EQ(tracker.lso(offset), modelLso(offset)) << "step " << step;
        ASSERT_EQ(tracker.firstOffsetOf(pid), model.count(pid) ? model[pid] : -1) << "step " << step;
    }
    EXPECT_GT(tracker.growths(), 0u) << "the churn exercised the cold doubling";
    EXPECT_GT(tracker.outOfOrderInserts(), 0u) << "the churn exercised out-of-order placement";
}

TEST(LsoTrackerRegression, LongOpenFrontCompactsInPlaceWithoutGrowth) {
    // One open txn pins the front; thousands of short txns open and close behind
    // it.  Steady state must stay within the reserved capacity (no doubling).
    LsoTracker tracker(/*openTxnCapacity=*/64);
    tracker.onProduceBatch(1, 0);
    for (int64_t i = 1; i < 10'000; ++i) {
        tracker.onProduceBatch(2, i);
        ASSERT_EQ(tracker.onControlMarker(2, i + 1, true), i);
    }
    EXPECT_EQ(tracker.lso(20'000), 0);
    EXPECT_EQ(tracker.openCount(), 1u);
    EXPECT_EQ(tracker.growths(), 0u) << "dead entries behind the front must be compacted, not grown";
    EXPECT_EQ(tracker.capacity(), 64u);
}

// ===========================================================================
// F15 S1 (fix-on-sight in the rebuild → RcBuilder::step loop): rebuild must
// scan EVERY segment up to HW.  The old rebuild did one Log::read(0, …), which
// returns a single segment, so a marker in a later segment was never seen.
// ===========================================================================

TEST(ControlBatchRegression, RebuildSpansEverySegment) {
    TmpDir d;
    rune::storage::Log log(d.path.string(), /*segmentBytes=*/1024);   // tiny segments
    log.recover();
    appendRaw(log, dataBatch(0, /*pid=*/7, /*transactional=*/true));
    while (log.segmentBaseOffsets().size() < 3)
        appendRaw(log, dataBatch(0, /*pid=*/-1, /*transactional=*/false));
    auto marker = ControlMarker::build(7, 0, ControlType::Abort, 0);
    const int64_t markerOff = appendRaw(log, marker);
    ASSERT_GE(log.segmentBaseOffsets().size(), 3u) << "precondition: the marker is not in the first segment";
    LsoTracker tracker;
    AbortedTxnIndex aborted;
    ReadCommittedView view(&tracker, &aborted);
    view.rebuild(&log, log.logEndOffset());
    EXPECT_EQ(tracker.openCount(), 0u) << "the ABORT marker at " << markerOff << " (a later segment) was never scanned";
    ASSERT_EQ(aborted.size(), 1u);
    EXPECT_TRUE(aborted.isAborted(7, 0));
}

// ===========================================================================
// F15 S7 (§5.4) — the EndTxn idempotency table rows beyond the S0 tests, and
// the I7 allocator gate at the coordinator layer.
// ===========================================================================

namespace {
struct Begun {
    TransactionCoordinator coord{ProducerIdAllocator{}, 60'000};
    InitProducerIdResult ip;
    explicit Begun(const std::string& id) {
        ip = coord.initProducerId(id, 60'000, 0);
        (void)coord.addPartitions(id, ip.producerId, ip.producerEpoch, {TopicPartition{"t", 0}}, 0);
    }
};
} // namespace

TEST(TxnEndTxnTableRegression, CompleteAbortSameDirectionOkOppositeInvalid) {
    Begun b("t-ca");
    ASSERT_TRUE(b.coord.endTransaction("t-ca", b.ip.producerId, b.ip.producerEpoch, false, 0).ok());
    ASSERT_TRUE(b.coord.completeTransaction("t-ca").ok());
    ASSERT_EQ(b.coord.state("t-ca"), TxnState::CompleteAbort);
    const auto same = b.coord.endTransaction("t-ca", b.ip.producerId, b.ip.producerEpoch, false, 0);
    EXPECT_TRUE(same.ok());
    EXPECT_TRUE(same.alreadyComplete) << "no transition, no markers";
    EXPECT_EQ(b.coord.endTransaction("t-ca", b.ip.producerId, b.ip.producerEpoch, true, 0).errorCode,
              EndTransactionResult::kInvalidTransition);
    EXPECT_EQ(b.coord.endTransaction("t-ca", b.ip.producerId, static_cast<ProducerEpoch>(b.ip.producerEpoch - 1), false, 0).errorCode,
              EndTransactionResult::kInvalidProducerEpoch) << "stale epoch → 17 before the table";
}

TEST(TxnEndTxnTableRegression, FenceAbortCallerGetsConcurrentThenProducerFencedOnCommit) {
    Begun b("t-f");
    const auto fence = b.coord.initProducerId(std::string("t-f"), 60'000, 0);   // fences the Ongoing txn
    ASSERT_TRUE(fence.ok());
    ASSERT_EQ(b.coord.state("t-f"), TxnState::PrepareAbort);
    EXPECT_EQ(b.coord.endTransaction("t-f", fence.producerId, fence.producerEpoch, true, 0).errorCode,
              EndTransactionResult::kConcurrentTransactions) << "the fence caller never ended that txn";
    EXPECT_EQ(b.coord.endTransaction("t-f", fence.producerId, fence.producerEpoch, false, 0).errorCode,
              EndTransactionResult::kConcurrentTransactions);
    ASSERT_TRUE(b.coord.completeTransaction("t-f").ok());
    ASSERT_EQ(b.coord.state("t-f"), TxnState::CompleteAbort);
    EXPECT_EQ(b.coord.endTransaction("t-f", fence.producerId, fence.producerEpoch, true, 0).errorCode,
              EndTransactionResult::kInvalidPid) << "COMMIT after a fence's abort: ProducerFenced (18)";
}

TEST(TxnEndTxnTableRegression, CompleteIsRestingAndReusable) {
    Begun b("t-r");
    ASSERT_TRUE(b.coord.endTransaction("t-r", b.ip.producerId, b.ip.producerEpoch, true, 0).ok());
    ASSERT_TRUE(b.coord.completeTransaction("t-r").ok());
    ASSERT_EQ(b.coord.state("t-r"), TxnState::CompleteCommit);
    EXPECT_EQ(b.coord.epochOf("t-r"), b.ip.producerEpoch) << "Complete* keeps the producer epoch";
    // The next transaction: Complete* → Ongoing.
    ASSERT_TRUE(b.coord.addPartitions("t-r", b.ip.producerId, b.ip.producerEpoch, {TopicPartition{"t", 1}}, 5).ok());
    EXPECT_EQ(b.coord.state("t-r"), TxnState::Ongoing);
    EXPECT_EQ(b.coord.partitionsOf("t-r").size(), 1u) << "the previous txn's partitions are gone";
    ASSERT_TRUE(b.coord.endTransaction("t-r", b.ip.producerId, b.ip.producerEpoch, false, 0).ok());
    ASSERT_TRUE(b.coord.completeTransaction("t-r").ok());
    // InitProducerId treats Complete* like Empty: bump.
    const auto re = b.coord.initProducerId(std::string("t-r"), 60'000, 0);
    ASSERT_TRUE(re.ok()) << "Complete* must not answer ConcurrentTransactions";
    EXPECT_EQ(re.producerEpoch, b.ip.producerEpoch + 1);
    EXPECT_EQ(b.coord.state("t-r"), TxnState::Empty);
}

TEST(TxnPidBlockRegression, GatedAllocatorRefusesAboveTheCommittedCeiling) {
    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000);
    coord.allocator().commitLimit(2);                         // block [0, 2) committed
    EXPECT_TRUE(coord.initProducerId(std::nullopt, 60'000, 0).ok());
    EXPECT_TRUE(coord.initProducerId(std::string("a"), 60'000, 0).ok());
    const auto refused = coord.initProducerId(std::string("b"), 60'000, 0);
    EXPECT_EQ(refused.errorCode, InitProducerIdResult::kConcurrentTransactions) << "retriable, never pid -2";
    EXPECT_FALSE(coord.metadata("b")) << "nothing created for a refused allocation";
    coord.allocator().commitLimit(4);
    const auto ok = coord.initProducerId(std::string("b"), 60'000, 0);
    ASSERT_TRUE(ok.ok());
    EXPECT_EQ(ok.producerId, 2);
}

// F15 S7 fix round 1 (concern 3): the fence flag survives a disk-log replay —
// the fence caller's answers are the same with and without a restart.
TEST(TxnEndTxnTableRegression, FenceAnswersSurviveTransactionLogReplay) {
    TmpDir d;
    const auto path = (d.path / "txn.log").string();
    ProducerId pid = -1; ProducerEpoch epoch = -1;
    {
        TransactionLog log(path);
        TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log);
        const auto ip = coord.initProducerId(std::string("t-fl"), 60'000, 0);
        ASSERT_TRUE(coord.addPartitions("t-fl", ip.producerId, ip.producerEpoch, {TopicPartition{"t", 0}}, 0).ok());
        const auto fence = coord.initProducerId(std::string("t-fl"), 60'000, 0);
        ASSERT_TRUE(fence.ok());
        pid = fence.producerId; epoch = fence.producerEpoch;
    }
    {
        TransactionLog log(path);
        TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log);
        ASSERT_EQ(coord.state("t-fl"), TxnState::PrepareAbort);
        EXPECT_EQ(coord.endTransaction("t-fl", pid, epoch, true, 0).errorCode,
                  EndTransactionResult::kConcurrentTransactions) << "fence caller after replay: 27, not 19";
        ASSERT_TRUE(coord.completeTransaction("t-fl").ok());
    }
    TransactionLog log(path);
    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log);
    ASSERT_EQ(coord.state("t-fl"), TxnState::CompleteAbort);
    EXPECT_EQ(coord.endTransaction("t-fl", pid, epoch, true, 0).errorCode,
              EndTransactionResult::kInvalidPid) << "COMMIT after a fence's abort, after replay: 18, not 19";
}

TEST(TxnPidBlockRegression, ExhaustedPidSpaceIsNotRetriable) {
    TransactionCoordinator coord(ProducerIdAllocator{INT64_MAX}, 60'000);
    EXPECT_EQ(coord.initProducerId(std::nullopt, 60'000, 0).errorCode, InitProducerIdResult::kProducerIdsExhausted);
    EXPECT_EQ(coord.initProducerId(std::string("x"), 60'000, 0).errorCode, InitProducerIdResult::kProducerIdsExhausted);
}

TEST(TxnPidBlockRegression, ResetGateDropsAnEarlierLoadsLimit) {
    ProducerIdAllocator a;
    a.commitLimit(1000);
    ASSERT_EQ(a.allocate(), 0);
    a.resetGate();                                            // a new coordinator load
    EXPECT_EQ(a.allocate(), ProducerIdAllocator::kBlockNotCommitted)
        << "only a ceiling committed by THIS load may be used";
    a.commitLimit(2000);
    EXPECT_EQ(a.allocate(), 1);
}

// ---------------------------------------------------------------------------
// F15 S4 (design note §2.2): AbortedTxnIndex in marker (lastOffset) order with
// an O(log R + k) lookup.  Structure / model tests (no base behaviour to RED:
// they pin the new lookup against a brute-force reference under churn).
// ---------------------------------------------------------------------------
TEST(AbortedTxnIndexRegression, LookupMatchesReferenceUnderChurn) {
    std::mt19937_64 rng(1515);
    AbortedTxnIndex idx;
    std::vector<AbortedTxnRange> ref;
    int64_t marker = 0;
    for (int i = 0; i < 2000; ++i) {
        marker += 1 + static_cast<int64_t>(rng() % 5);
        const int64_t span = static_cast<int64_t>(rng() % 300);        // long + short txns interleave
        const int64_t pid  = static_cast<int64_t>(rng() % 16);
        idx.onAbortMarker(pid, std::max<int64_t>(0, marker - span), marker);
        ref.push_back({pid, std::max<int64_t>(0, marker - span), marker});
    }
    for (int q = 0; q < 20000; ++q) {
        const int64_t pid = static_cast<int64_t>(rng() % 16);
        const int64_t off = static_cast<int64_t>(rng() % static_cast<uint64_t>(marker + 10));
        bool want = false;
        for (const auto& r : ref) want = want || (r.producerId == pid && r.contains(off));
        ASSERT_EQ(idx.isAborted(pid, off), want) << "pid=" << pid << " off=" << off;
    }
}

TEST(AbortedTxnIndexRegression, OutOfOrderInsertKeepsMarkerOrderAndPruneDropsBelowLogStart) {
    AbortedTxnIndex idx;
    idx.reserve(4);
    idx.onAbortMarker(1, 0, 10);
    idx.onAbortMarker(2, 20, 30);
    idx.onAbortMarker(3, 12, 15);                                     // out of order (cold)
    ASSERT_EQ(idx.size(), 3u);
    EXPECT_EQ(idx.ranges()[1].lastOffset, 15);
    EXPECT_TRUE(idx.isAborted(3, 13));
    EXPECT_TRUE(idx.isAborted(2, 25));
    EXPECT_FALSE(idx.isAborted(1, 11));
    EXPECT_EQ(idx.growths(), 0u);
    idx.pruneBelow(16);                                               // markers 10 and 15 are gone
    ASSERT_EQ(idx.size(), 1u);
    EXPECT_FALSE(idx.isAborted(1, 5));
    EXPECT_TRUE(idx.isAborted(2, 20));
    idx.clear();
    EXPECT_FALSE(idx.isAborted(2, 20));
    EXPECT_EQ(idx.capacity(), 4u) << "clear keeps the reservation";
}
