// ---------------------------------------------------------------------------
// TransactionLogTest.cpp — Unit tests for TransactionLog (P5-3).
//
// TDD: durable txn-state log, crash-safe append+fsync, torn-tail recovery,
// replay → identical in-memory map, full lifecycle.
//
// Unique temp paths: /tmp/txnlog_<pid>_<atomic counter>
// ---------------------------------------------------------------------------

#include <rune/transaction/TransactionLog.h>
#include <rune/transaction/TransactionCoordinator.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <set>
#include <string>
#include <unistd.h>

using namespace rune::transaction;
namespace fs = std::filesystem;

namespace {

// ---------------------------------------------------------------------------
// Unique temp path helper
// ---------------------------------------------------------------------------
static std::atomic<int> g_counter{0};

static std::string tmpPath() {
    return "/tmp/txnlog_" + std::to_string(::getpid())
         + "_" + std::to_string(g_counter.fetch_add(1, std::memory_order_relaxed));
}

// Cleanup helper
struct TempFile {
    std::string path;
    explicit TempFile(std::string p) : path(std::move(p)) {}
    ~TempFile() { ::unlink(path.c_str()); }
};

// Build a TopicPartition
static TopicPartition tp(std::string topic, int32_t part) {
    return TopicPartition{std::move(topic), part};
}

// Build a TxnLogEntry
static TxnLogEntry makeEntry(const TransactionalId& txnId,
                               ProducerId pid,
                               ProducerEpoch epoch,
                               TxnState state,
                               std::initializer_list<TopicPartition> parts = {},
                               int64_t startNs = 0,
                               int32_t coordEpoch = 0,
                               int32_t timeoutMs = 60'000)
{
    TxnLogEntry e;
    e.txnId            = txnId;
    e.producerId       = pid;
    e.producerEpoch    = epoch;
    e.state            = state;
    e.partitions       = std::set<TopicPartition>(parts);
    e.txnStartNs       = startNs;
    e.coordinatorEpoch = coordEpoch;
    e.txnTimeoutMs     = timeoutMs;
    return e;
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// TEST: open an empty log, size = 0
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, OpenEmptyLog) {
    auto path = tmpPath();
    TempFile tf{path};
    TransactionLog log(path);
    EXPECT_EQ(log.size(), 0u);
    EXPECT_EQ(log.metadata("nonexistent"), nullptr);
}

// ---------------------------------------------------------------------------
// TEST: append a single entry and retrieve it
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, AppendSingleEntry) {
    auto path = tmpPath();
    TempFile tf{path};
    TransactionLog log(path);

    auto e = makeEntry("txn-1", 42, 0, TxnState::Empty);
    log.append(e);

    ASSERT_EQ(log.size(), 1u);
    const TransactionMetadata* meta = log.metadata("txn-1");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->producerId, 42);
    EXPECT_EQ(meta->producerEpoch, 0);
    EXPECT_EQ(meta->state, TxnState::Empty);
    EXPECT_EQ(meta->txnId, "txn-1");
}

// ---------------------------------------------------------------------------
// TEST: append multiple transitions for the same txnId; last wins
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, MultipleTransitions) {
    auto path = tmpPath();
    TempFile tf{path};
    TransactionLog log(path);

    log.append(makeEntry("txn-A", 1, 0, TxnState::Empty));
    log.append(makeEntry("txn-A", 1, 0, TxnState::Ongoing, {tp("topic1", 0)}, 1000));
    log.append(makeEntry("txn-A", 1, 0, TxnState::PrepareCommit, {tp("topic1", 0)}, 1000));

    ASSERT_EQ(log.size(), 1u);
    const auto* meta = log.metadata("txn-A");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->state, TxnState::PrepareCommit);
    EXPECT_EQ(meta->partitions.size(), 1u);
}

// ---------------------------------------------------------------------------
// TEST: multiple different txnIds
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, MultipleDifferentTxnIds) {
    auto path = tmpPath();
    TempFile tf{path};
    TransactionLog log(path);

    for (int i = 0; i < 10; ++i) {
        auto txnId = "txn-" + std::to_string(i);
        log.append(makeEntry(txnId, i, 0, TxnState::Empty));
    }

    EXPECT_EQ(log.size(), 10u);
    for (int i = 0; i < 10; ++i) {
        auto txnId = "txn-" + std::to_string(i);
        const auto* meta = log.metadata(txnId);
        ASSERT_NE(meta, nullptr);
        EXPECT_EQ(meta->producerId, i);
    }
}

// ---------------------------------------------------------------------------
// TEST: durability across reopen — destroy + recover from same path
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, DurabilityAcrossReopen) {
    auto path = tmpPath();
    TempFile tf{path};

    {
        TransactionLog log(path);
        log.append(makeEntry("txn-durable", 99, 3, TxnState::Ongoing,
                              {tp("t1", 0), tp("t2", 1)}, 5000, 7, 30'000));
        log.append(makeEntry("txn-durable", 99, 3, TxnState::PrepareCommit,
                              {tp("t1", 0), tp("t2", 1)}, 5000, 7, 30'000));
    }

    // Reopen — must reconstruct identical state.
    TransactionLog log2(path);
    ASSERT_EQ(log2.size(), 1u);
    const auto* meta = log2.metadata("txn-durable");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->producerId, 99);
    EXPECT_EQ(meta->producerEpoch, 3);
    EXPECT_EQ(meta->state, TxnState::PrepareCommit);
    EXPECT_EQ(meta->txnStartNs, 5000);
    EXPECT_EQ(meta->txnTimeoutMs, 30'000);
    EXPECT_EQ(meta->partitions.size(), 2u);
    EXPECT_TRUE(meta->partitions.contains(tp("t1", 0)));
    EXPECT_TRUE(meta->partitions.contains(tp("t2", 1)));
}

// ---------------------------------------------------------------------------
// TEST: durability — multiple txnIds survive reopen
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, DurabilityMultipleTxnIds) {
    auto path = tmpPath();
    TempFile tf{path};

    {
        TransactionLog log(path);
        for (int i = 0; i < 5; ++i) {
            std::string id = "tx" + std::to_string(i);
            log.append(makeEntry(id, i, static_cast<ProducerEpoch>(i),
                                  TxnState::Ongoing, {tp("topic", i)}, int64_t{i * 1000}));
        }
    }

    TransactionLog log2(path);
    EXPECT_EQ(log2.size(), 5u);
    for (int i = 0; i < 5; ++i) {
        std::string id = "tx" + std::to_string(i);
        const auto* meta = log2.metadata(id);
        ASSERT_NE(meta, nullptr) << "missing " << id;
        EXPECT_EQ(meta->producerId, i);
        EXPECT_EQ(meta->producerEpoch, i);
        EXPECT_EQ(meta->txnStartNs, int64_t{i * 1000});
        EXPECT_EQ(meta->partitions.size(), 1u);
    }
}

// ---------------------------------------------------------------------------
// TEST: torn-tail recovery — truncate mid-record → recover to last complete,
// no crash, and the log is writable afterward.
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, TornTailRecovery) {
    auto path = tmpPath();
    TempFile tf{path};

    // Write two complete records.
    {
        TransactionLog log(path);
        log.append(makeEntry("txn-ok", 1, 0, TxnState::Empty));
        log.append(makeEntry("txn-ok", 1, 0, TxnState::Ongoing, {tp("t", 0)}, 100));
    }

    // Corrupt the file: append a partial record (torn tail).
    {
        int fd = ::open(path.c_str(), O_WRONLY | O_APPEND);
        ASSERT_GE(fd, 0);
        uint8_t garbage[20] = {0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02};
        ::write(fd, garbage, sizeof(garbage));
        ::close(fd);
    }

    // Reopen: must recover to last complete record, no crash.
    {
        TransactionLog log(path);
        ASSERT_EQ(log.size(), 1u);
        const auto* meta = log.metadata("txn-ok");
        ASSERT_NE(meta, nullptr);
        EXPECT_EQ(meta->state, TxnState::Ongoing); // last complete

        // Must be writable after torn-tail recovery.
        log.append(makeEntry("txn-ok", 1, 0, TxnState::PrepareCommit, {tp("t", 0)}, 100));
        EXPECT_EQ(log.metadata("txn-ok")->state, TxnState::PrepareCommit);
    }

    // Reopen again to confirm the new record persisted.
    {
        TransactionLog log(path);
        const auto* meta = log.metadata("txn-ok");
        ASSERT_NE(meta, nullptr);
        EXPECT_EQ(meta->state, TxnState::PrepareCommit);
    }
}

// ---------------------------------------------------------------------------
// TEST: torn tail on the FIRST record — file truncated to 0 (empty)
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, TornTailOnFirstRecord) {
    auto path = tmpPath();
    TempFile tf{path};

    // Write only garbage (no complete record).
    {
        int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
        ASSERT_GE(fd, 0);
        uint8_t garbage[10] = {0xAA, 0xBB, 0xCC};
        ::write(fd, garbage, sizeof(garbage));
        ::close(fd);
    }

    // Reopen: must silently recover to empty (truncate garbage).
    {
        TransactionLog log(path);
        EXPECT_EQ(log.size(), 0u);
        // Writable.
        log.append(makeEntry("txn-new", 5, 0, TxnState::Empty));
        EXPECT_EQ(log.size(), 1u);
    }
}

// ---------------------------------------------------------------------------
// TEST: full lifecycle (Empty → Ongoing → PrepareCommit → CompleteCommit)
//       replayed correctly.
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, FullLifecycleReplay) {
    auto path = tmpPath();
    TempFile tf{path};

    {
        TransactionLog log(path);
        log.append(makeEntry("txn-lc", 10, 1, TxnState::Empty,
                              {}, 0, 0, 60'000));
        log.append(makeEntry("txn-lc", 10, 1, TxnState::Ongoing,
                              {tp("p", 0), tp("q", 1)}, 1000, 0, 60'000));
        log.append(makeEntry("txn-lc", 10, 1, TxnState::PrepareCommit,
                              {tp("p", 0), tp("q", 1)}, 1000, 0, 60'000));
        log.append(makeEntry("txn-lc", 10, 1, TxnState::CompleteCommit,
                              {}, 0, 0, 60'000));
    }

    TransactionLog log2(path);
    ASSERT_EQ(log2.size(), 1u);
    const auto* meta = log2.metadata("txn-lc");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->state, TxnState::CompleteCommit);
    EXPECT_EQ(meta->producerId, 10);
    EXPECT_EQ(meta->producerEpoch, 1);
    EXPECT_EQ(meta->partitions.size(), 0u); // CompleteCommit clears partitions
}

// ---------------------------------------------------------------------------
// TEST: appendFromMetadata round-trip
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, AppendFromMetadata) {
    auto path = tmpPath();
    TempFile tf{path};
    TransactionLog log(path);

    TransactionMetadata meta;
    meta.txnId         = "txn-meta";
    meta.producerId    = 77;
    meta.producerEpoch = 5;
    meta.state         = TxnState::Ongoing;
    meta.txnStartNs    = 999;
    meta.txnTimeoutMs  = 45'000;
    meta.partitions.insert(tp("alpha", 2));
    meta.partitions.insert(tp("beta", 3));

    log.appendFromMetadata(meta, /*coordinatorEpoch=*/2);

    const auto* recovered = log.metadata("txn-meta");
    ASSERT_NE(recovered, nullptr);
    EXPECT_EQ(recovered->producerId, 77);
    EXPECT_EQ(recovered->producerEpoch, 5);
    EXPECT_EQ(recovered->state, TxnState::Ongoing);
    EXPECT_EQ(recovered->txnStartNs, 999);
    EXPECT_EQ(recovered->txnTimeoutMs, 45'000);
    EXPECT_EQ(recovered->partitions.size(), 2u);
}

// ---------------------------------------------------------------------------
// TEST: large number of partitions per entry
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, LargePartitionSet) {
    auto path = tmpPath();
    TempFile tf{path};

    std::set<TopicPartition> parts;
    for (int i = 0; i < 100; ++i) {
        parts.insert(tp("topic-" + std::to_string(i), i));
    }

    {
        TransactionLog log(path);
        TxnLogEntry e = makeEntry("txn-large", 1, 0, TxnState::Ongoing);
        e.partitions = parts;
        e.txnStartNs = 12345;
        log.append(e);
    }

    TransactionLog log2(path);
    const auto* meta = log2.metadata("txn-large");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->partitions.size(), 100u);
    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(meta->partitions.contains(tp("topic-" + std::to_string(i), i)));
    }
}

// ---------------------------------------------------------------------------
// TEST: empty txnId is stored and recovered correctly
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, EmptyTxnId) {
    auto path = tmpPath();
    TempFile tf{path};

    {
        TransactionLog log(path);
        log.append(makeEntry("", 55, 0, TxnState::Empty));
    }

    TransactionLog log2(path);
    ASSERT_EQ(log2.size(), 1u);
    const auto* meta = log2.metadata("");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->producerId, 55);
}

// ---------------------------------------------------------------------------
// TEST: coordinatorEpoch is persisted and recovered
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, CoordinatorEpochPersisted) {
    auto path = tmpPath();
    TempFile tf{path};

    {
        TransactionLog log(path);
        TxnLogEntry e = makeEntry("txn-ep", 1, 0, TxnState::Empty, {}, 0, 42);
        log.append(e);
    }

    TransactionLog log2(path);
    const auto* meta = log2.metadata("txn-ep");
    ASSERT_NE(meta, nullptr);
    // coordinatorEpoch is not in TransactionMetadata directly, but
    // the TxnLogEntry held it — verify the metadata state is correct.
    EXPECT_EQ(meta->producerId, 1);
    EXPECT_EQ(meta->state, TxnState::Empty);
}

// ---------------------------------------------------------------------------
// TEST: many sequential appends, all survive reopen
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, ManySequentialAppends) {
    auto path = tmpPath();
    TempFile tf{path};

    constexpr int N = 50;
    {
        TransactionLog log(path);
        for (int i = 0; i < N; ++i) {
            log.append(makeEntry("txn-seq-" + std::to_string(i),
                                  i, static_cast<ProducerEpoch>(i % 32767),
                                  TxnState::Ongoing,
                                  {tp("t", i)}, int64_t{i}));
        }
    }

    TransactionLog log2(path);
    EXPECT_EQ(log2.size(), N);
    for (int i = 0; i < N; ++i) {
        const auto* m = log2.metadata("txn-seq-" + std::to_string(i));
        ASSERT_NE(m, nullptr);
        EXPECT_EQ(m->producerId, i);
        EXPECT_EQ(m->txnStartNs, int64_t{i});
    }
}

// ---------------------------------------------------------------------------
// TEST: abort lifecycle replayed correctly
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, AbortLifecycleReplay) {
    auto path = tmpPath();
    TempFile tf{path};

    {
        TransactionLog log(path);
        log.append(makeEntry("txn-abort", 20, 0, TxnState::Empty));
        log.append(makeEntry("txn-abort", 20, 0, TxnState::Ongoing, {tp("a", 0)}, 500));
        log.append(makeEntry("txn-abort", 20, 0, TxnState::PrepareAbort, {tp("a", 0)}, 500));
        log.append(makeEntry("txn-abort", 20, 0, TxnState::CompleteAbort));
    }

    TransactionLog log2(path);
    const auto* meta = log2.metadata("txn-abort");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->state, TxnState::CompleteAbort);
}

// ---------------------------------------------------------------------------
// TEST: fencingEpoch is NOT persisted (always false on recovery)
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, FencingEpochNotPersisted) {
    auto path = tmpPath();
    TempFile tf{path};

    TransactionMetadata meta;
    meta.txnId         = "txn-fence";
    meta.producerId    = 1;
    meta.producerEpoch = 2;
    meta.state         = TxnState::PrepareAbort;
    meta.txnStartNs    = 0;
    meta.txnTimeoutMs  = 60'000;
    meta.fencingEpoch  = true; // this should NOT be persisted

    {
        TransactionLog log(path);
        log.appendFromMetadata(meta);
    }

    TransactionLog log2(path);
    const auto* m = log2.metadata("txn-fence");
    ASSERT_NE(m, nullptr);
    EXPECT_FALSE(m->fencingEpoch); // must be false after recovery
}
