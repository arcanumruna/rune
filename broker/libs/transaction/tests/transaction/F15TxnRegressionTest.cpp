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
    void sendFetchRequest(const rep::FetchRequestMsg&) override {}
    void sendFetchResponse(rep::NodeId, const rep::FetchResponseMsg&) override {}
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
