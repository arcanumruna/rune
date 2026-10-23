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
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <set>
#include <string>
#include <unistd.h>

using namespace rune::transaction;
namespace fs = std::filesystem;

namespace {

// ---------------------------------------------------------------------------
// Local CRC32C (Castagnoli) — matches TransactionLog's internal implementation.
// Used in tests that need to re-stamp the checksum after mutating a raw record.
// ---------------------------------------------------------------------------
static uint32_t testLogCrc32c(const uint8_t* data, std::size_t len) noexcept {
    static uint32_t tbl[256];
    static bool init = false;
    if (!init) {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int j = 0; j < 8; ++j) c = (c >> 1) ^ (0x82F63B78u & -(c & 1u));
            tbl[i] = c;
        }
        init = true;
    }
    uint32_t crc = 0xFFFFFFFFu;
    for (std::size_t i = 0; i < len; ++i)
        crc = (crc >> 8) ^ tbl[(crc ^ data[i]) & 0xFFu];
    return crc ^ 0xFFFFFFFFu;
}

// ---------------------------------------------------------------------------
// Unique temp path helper
// ---------------------------------------------------------------------------
static std::atomic<int> g_counter{0};

static std::string tmpPath() {
    return std::filesystem::temp_directory_path().string() + "/txnlog_" + std::to_string(::getpid())
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
// TEST: coordinatorEpoch is persisted and RECOVERED (FIX 2)
// Destroy the log and reopen — recovered coordinatorEpoch must equal what was written.
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, CoordinatorEpochPersisted) {
    auto path = tmpPath();
    TempFile tf{path};

    {
        TransactionLog log(path);
        TxnLogEntry e = makeEntry("txn-ep", 1, 0, TxnState::PrepareCommit, {tp("t", 0)}, 500, 42);
        log.append(e);
    }

    // Reopen — coordinatorEpoch 42 must survive.
    TransactionLog log2(path);
    const auto* meta = log2.metadata("txn-ep");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->producerId, 1);
    EXPECT_EQ(meta->state, TxnState::PrepareCommit);
    // FIX 2: coordinatorEpoch must be stored in TransactionMetadata and recovered.
    EXPECT_EQ(meta->coordinatorEpoch, 42);
}

// ---------------------------------------------------------------------------
// TEST: crash-seam recovery — PrepareCommit survives coordinator restart (FIX 4)
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, CrashSeamRecoveryPrepareCommit) {
    auto path = tmpPath();
    TempFile tf{path};

    ProducerId    pid    = 77;
    ProducerEpoch epoch  = 3;
    int32_t       coordE = 7;

    // Phase 1: drive txn to PrepareCommit and durably append that state.
    {
        TransactionLog log(path);
        // Simulate coordinator appending each transition.
        log.append(makeEntry("txn-crash", pid, epoch, TxnState::Empty,
                              {}, 0, coordE, 30'000));
        log.append(makeEntry("txn-crash", pid, epoch, TxnState::Ongoing,
                              {tp("orders", 0), tp("payments", 1)}, 1000, coordE, 30'000));
        log.append(makeEntry("txn-crash", pid, epoch, TxnState::PrepareCommit,
                              {tp("orders", 0), tp("payments", 1)}, 1000, coordE, 30'000));
        // Crash here — completeTransaction is NEVER called.
    }

    // Phase 2: a new coordinator reconstructs from the log.
    TransactionLog log2(path);
    ASSERT_EQ(log2.size(), 1u);
    const auto* meta = log2.metadata("txn-crash");
    ASSERT_NE(meta, nullptr);

    // Must recover exact PrepareCommit state.
    EXPECT_EQ(meta->state,           TxnState::PrepareCommit);
    EXPECT_EQ(meta->producerId,       pid);
    EXPECT_EQ(meta->producerEpoch,    epoch);
    EXPECT_EQ(meta->coordinatorEpoch, coordE);
    EXPECT_EQ(meta->txnTimeoutMs,     30'000);
    EXPECT_EQ(meta->partitions.size(), 2u);
    EXPECT_TRUE(meta->partitions.contains(tp("orders", 0)));
    EXPECT_TRUE(meta->partitions.contains(tp("payments", 1)));

    // Can resolve: complete the commit from recovered state.
    log2.append(makeEntry("txn-crash", pid, epoch, TxnState::CompleteCommit,
                           {}, 0, coordE, 30'000));
    const auto* meta2 = log2.metadata("txn-crash");
    ASSERT_NE(meta2, nullptr);
    EXPECT_EQ(meta2->state, TxnState::CompleteCommit);
}

// ---------------------------------------------------------------------------
// TEST: crash-seam recovery — PrepareAbort survives coordinator restart (FIX 4)
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, CrashSeamRecoveryPrepareAbort) {
    auto path = tmpPath();
    TempFile tf{path};

    ProducerId    pid    = 55;
    ProducerEpoch epoch  = 1;
    int32_t       coordE = 3;

    {
        TransactionLog log(path);
        log.append(makeEntry("txn-crash-abort", pid, epoch, TxnState::Empty,
                              {}, 0, coordE, 60'000));
        log.append(makeEntry("txn-crash-abort", pid, epoch, TxnState::Ongoing,
                              {tp("alpha", 0)}, 2000, coordE, 60'000));
        log.append(makeEntry("txn-crash-abort", pid, epoch, TxnState::PrepareAbort,
                              {tp("alpha", 0)}, 2000, coordE, 60'000));
        // Crash — completeTransaction never called.
    }

    TransactionLog log2(path);
    const auto* meta = log2.metadata("txn-crash-abort");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->state,           TxnState::PrepareAbort);
    EXPECT_EQ(meta->producerId,       pid);
    EXPECT_EQ(meta->producerEpoch,    epoch);
    EXPECT_EQ(meta->coordinatorEpoch, coordE);
    EXPECT_EQ(meta->partitions.size(), 1u);
    EXPECT_TRUE(meta->partitions.contains(tp("alpha", 0)));

    // Resolve by completing the abort.
    log2.append(makeEntry("txn-crash-abort", pid, epoch, TxnState::CompleteAbort,
                           {}, 0, coordE, 60'000));
    EXPECT_EQ(log2.metadata("txn-crash-abort")->state, TxnState::CompleteAbort);
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
// TEST: fencingEpoch IS persisted (F15: header byte 27, bit0)
// ---------------------------------------------------------------------------
TEST(TransactionLogTest, FencingEpochPersistedInFlagsByte) {
    auto path = tmpPath();
    TempFile tf{path};

    TransactionMetadata meta;
    meta.txnId         = "txn-fence";
    meta.producerId    = 1;
    meta.producerEpoch = 2;
    meta.state         = TxnState::PrepareAbort;
    meta.txnStartNs    = 0;
    meta.txnTimeoutMs  = 60'000;
    meta.fencingEpoch  = true; // F15 S7 fix round 1: persisted (header byte 27, bit0)

    {
        TransactionLog log(path);
        log.appendFromMetadata(meta);
    }

    TransactionLog log2(path);
    const auto* m = log2.metadata("txn-fence");
    ASSERT_NE(m, nullptr);
    EXPECT_TRUE(m->fencingEpoch); // F15 concern 3: the §5.4 fence answers survive recovery
}

// ===========================================================================
// Part B (durable log) — additional edge-case coverage
// ===========================================================================

// #23 — CRC corruption in a MIDDLE record (not the tail): a good record, a
// CRC-flipped record, then a good record. Replay must stop at the first bad one
// and truncate everything after (recover only the leading good record).
TEST(TransactionLogTest, MiddleRecordCrcCorruptionTruncatesRest) {
    auto path = tmpPath();
    TempFile tf{path};

    // Write three complete records; capture the byte offset of record #2's start.
    int64_t rec2Start = 0, rec2Len = 0;
    {
        TransactionLog log(path);
        log.append(makeEntry("txn-a", 1, 0, TxnState::Empty));               // rec0
        rec2Start = static_cast<int64_t>(fs::file_size(path));
        log.append(makeEntry("txn-b", 2, 0, TxnState::Ongoing, {tp("t", 0)}, 5)); // rec1 (the middle)
        rec2Len = static_cast<int64_t>(fs::file_size(path)) - rec2Start;
        log.append(makeEntry("txn-c", 3, 0, TxnState::Empty));               // rec2
    }

    // Flip a byte in the MIDDLE record's payload (before its CRC) to break CRC.
    {
        int fd = ::open(path.c_str(), O_RDWR);
        ASSERT_GE(fd, 0);
        // Corrupt a byte well inside the middle record's fixed header (offset +16
        // is producerId, definitely covered by CRC and not the magic/length).
        off_t corruptAt = static_cast<off_t>(rec2Start + 16);
        uint8_t b = 0;
        ASSERT_EQ(::pread(fd, &b, 1, corruptAt), 1);
        b ^= 0xFF;
        ASSERT_EQ(::pwrite(fd, &b, 1, corruptAt), 1);
        ::close(fd);
    }
    (void)rec2Len;

    // Reopen: only the FIRST good record survives; the corrupt middle and the
    // trailing good record are truncated away.
    {
        TransactionLog log(path);
        EXPECT_EQ(log.size(), 1u);
        EXPECT_NE(log.metadata("txn-a"), nullptr);
        EXPECT_EQ(log.metadata("txn-b"), nullptr);
        EXPECT_EQ(log.metadata("txn-c"), nullptr);
        // Writable after truncation.
        log.append(makeEntry("txn-d", 4, 0, TxnState::Empty));
        EXPECT_NE(log.metadata("txn-d"), nullptr);
    }
}

// #22 — Torn tail straddling a Complete* transition: Ongoing + PrepareCommit
// fully written, then a partial CompleteCommit record. Reopen → recovers to
// PrepareCommit (resolvable), never a half-applied Complete.
TEST(TransactionLogTest, TornTailMidCompleteRecoversToPrepare) {
    auto path = tmpPath();
    TempFile tf{path};
    {
        TransactionLog log(path);
        log.append(makeEntry("txn-p", 9, 2, TxnState::Empty));
        log.append(makeEntry("txn-p", 9, 2, TxnState::Ongoing, {tp("x", 0)}, 100));
        log.append(makeEntry("txn-p", 9, 2, TxnState::PrepareCommit, {tp("x", 0)}, 100));
    }
    // Append a partial CompleteCommit record (torn — only a few bytes).
    {
        int fd = ::open(path.c_str(), O_WRONLY | O_APPEND);
        ASSERT_GE(fd, 0);
        // Valid magic but truncated body (not a full record).
        uint8_t partial[8] = {0x4C, 0x4E, 0x4B, 0x54, 0xFF, 0xFF, 0x00, 0x00};
        ::write(fd, partial, sizeof(partial));
        ::close(fd);
    }
    {
        TransactionLog log(path);
        const auto* m = log.metadata("txn-p");
        ASSERT_NE(m, nullptr);
        EXPECT_EQ(m->state, TxnState::PrepareCommit);
    }
}

// A5 — over-long txnId is REJECTED loudly (std::length_error), never silently
// truncated/dropped.
TEST(TransactionLogTest, AppendRejectsOverLongTxnId) {
    auto path = tmpPath();
    TempFile tf{path};
    TransactionLog log(path);
    TxnLogEntry e = makeEntry(std::string(65536, 'z'), 1, 0, TxnState::Empty);
    EXPECT_THROW(log.append(e), std::length_error);
    // Nothing persisted.
    EXPECT_EQ(log.size(), 0u);
}

// A5 — over-long partition count is REJECTED loudly. (Constructing 65536 real
// partitions is wasteful; assert the topic-length guard instead, same code path
// class: an over-long length-prefixed field is rejected, not truncated.)
TEST(TransactionLogTest, AppendRejectsOverLongTopicName) {
    auto path = tmpPath();
    TempFile tf{path};
    TransactionLog log(path);
    TxnLogEntry e = makeEntry("txn-x", 1, 0, TxnState::Ongoing);
    e.partitions.insert(TopicPartition{std::string(65536, 't'), 0});
    EXPECT_THROW(log.append(e), std::length_error);
}

// #24 — partitionCount > 0 with an EMPTY txnId round-trips (length-prefixed
// empty txnId is handled correctly alongside partitions).
TEST(TransactionLogTest, EmptyTxnIdWithPartitionsRoundTrips) {
    auto path = tmpPath();
    TempFile tf{path};
    {
        TransactionLog log(path);
        log.append(makeEntry("", 55, 0, TxnState::Ongoing, {tp("t", 0), tp("t", 1)}, 7));
    }
    TransactionLog log2(path);
    const auto* m = log2.metadata("");
    ASSERT_NE(m, nullptr);
    EXPECT_EQ(m->producerId, 55);
    EXPECT_EQ(m->partitions.size(), 2u);
}

// #24 — a topic string containing embedded NUL bytes survives the round-trip
// (length-prefixed, not NUL-terminated).
TEST(TransactionLogTest, TopicWithEmbeddedNulRoundTrips) {
    auto path = tmpPath();
    TempFile tf{path};
    std::string binTopic("a\0b\0c", 5);
    {
        TransactionLog log(path);
        log.append(makeEntry("txn-bin", 1, 0, TxnState::Ongoing,
                              {TopicPartition{binTopic, 3}}, 1));
    }
    TransactionLog log2(path);
    const auto* m = log2.metadata("txn-bin");
    ASSERT_NE(m, nullptr);
    ASSERT_EQ(m->partitions.size(), 1u);
    EXPECT_EQ(m->partitions.begin()->topic.size(), 5u);
    EXPECT_EQ(m->partitions.begin()->topic, binTopic);
}

// A2 — pendingNewProducerId/Epoch survive a persist→replay round-trip in the log.
TEST(TransactionLogTest, PendingNewProducerIdRoundTrips) {
    auto path = tmpPath();
    TempFile tf{path};
    {
        TransactionLog log(path);
        TxnLogEntry e = makeEntry("txn-swap", 100, INT16_MAX, TxnState::PrepareAbort,
                                  {tp("t", 0)}, 42);
        e.pendingNewProducerId    = 777;
        e.pendingNewProducerEpoch = 0;
        log.append(e);
    }
    TransactionLog log2(path);
    const auto* m = log2.metadata("txn-swap");
    ASSERT_NE(m, nullptr);
    EXPECT_EQ(m->producerId, 100);              // OLD pid persisted on the abort
    EXPECT_EQ(m->pendingNewProducerId, 777);    // deferred fresh pid recovered
    EXPECT_EQ(m->pendingNewProducerEpoch, 0);
}

// ===========================================================================
// L1 — invalid state byte (CRC recomputed to be valid) → recovery rejects it
//
// A CRC-valid record with stateRaw > 7 (out-of-range for TxnState) must be
// treated as corrupt and cause replay to stop, just like a CRC failure. The
// record must NOT be applied to the in-memory map, and the log must be
// truncated at the last known-good position.
// ===========================================================================
TEST(TransactionLogTest, InvalidStateByteRejectedEvenWithValidCrc) {
    auto path = tmpPath();
    TempFile tf{path};

    // Write a good record first so we can verify it survives the re-open.
    {
        TransactionLog log(path);
        log.append(makeEntry("txn-good", 1, 0, TxnState::Ongoing,
                              {tp("t", 0)}, /*startNs=*/100));
    }

    // Append a second raw record with stateRaw = 0xFF (invalid), but with a
    // correctly recomputed CRC so the CRC check passes.  We do this by:
    //   1. Writing the record through the normal API (valid state),
    //   2. Reading the raw file,
    //   3. Flipping the state byte to 0xFF,
    //   4. Recomputing the CRC over the modified bytes, and
    //   5. Writing the modified record directly.
    {
        TransactionLog log(path);
        log.append(makeEntry("txn-bad-state", 2, 0, TxnState::Empty));
    }

    // Find the second record and corrupt its state byte, recomputing the CRC.
    {
        // Read the file.
        int fd = ::open(path.c_str(), O_RDWR);
        ASSERT_GE(fd, 0);

        // File = record1 + record2.  Skip record1.
        // Record header: magic(4)+totalLen(4)+txnStartNs(8)+pid(8)+epoch(2)+state(1)+...
        // Read totalLength of record1.
        uint8_t hdr4[4];
        ASSERT_EQ(::pread(fd, hdr4, 4, 4), 4); // totalLength of record1
        uint32_t rec1Len;
        std::memcpy(&rec1Len, hdr4, sizeof(rec1Len));

        // Record2 starts at rec1Len. State byte is at offset 26 within the record.
        off_t rec2Start = static_cast<off_t>(rec1Len);

        // Read totalLength of record2.
        ASSERT_EQ(::pread(fd, hdr4, 4, rec2Start + 4), 4);
        uint32_t rec2Len;
        std::memcpy(&rec2Len, hdr4, sizeof(rec2Len));

        // Read the entire second record.
        std::vector<uint8_t> rec2(rec2Len);
        ASSERT_EQ(::pread(fd, rec2.data(), rec2Len, rec2Start),
                  static_cast<ssize_t>(rec2Len));

        // Corrupt state byte (offset 26 within the record).
        rec2[26] = 0xFF; // out-of-range (max valid = 7 = PrepareEpochFence)

        // Recompute CRC over bytes [0 .. totalLength - 4).
        // Use TransactionLog's own crc32c (accessible via static public interface).
        uint32_t newCrc = testLogCrc32c(rec2.data(), rec2Len - 4);
        std::memcpy(rec2.data() + rec2Len - 4, &newCrc, 4);

        // Write the modified record back.
        ASSERT_EQ(::pwrite(fd, rec2.data(), rec2Len, rec2Start),
                  static_cast<ssize_t>(rec2Len));
        ::close(fd);
    }

    // Reopen: the invalid-state record must be rejected; only the first good
    // record survives (the log is truncated at the first-record boundary).
    TransactionLog log3(path);
    EXPECT_EQ(log3.size(), 1u)
        << "The corrupted-state record must be rejected by replay";
    EXPECT_NE(log3.metadata("txn-good"), nullptr)
        << "The good record before the corrupt one must still be present";
    EXPECT_EQ(log3.metadata("txn-bad-state"), nullptr)
        << "The record with an invalid state byte must not be applied";

    // The log must remain writable after truncation.
    log3.append(makeEntry("txn-after", 3, 0, TxnState::Empty));
    EXPECT_EQ(log3.size(), 2u);
    EXPECT_NE(log3.metadata("txn-after"), nullptr);
}

// ===========================================================================
// Round-2 Residual-7: CRC-valid header with an in-body topicLen that overruns
// totalLength → rejected as corrupt (no over-read / no bogus metadata applied).
//
// Strategy: write a record with one partition (short topic "x"), then in the
// raw bytes change the topicLen in the variable body to 65535 (far beyond the
// actual record size), recompute the CRC over the modified bytes (same total
// size), and write it back. Replay must detect the overrun and stop.
// ===========================================================================
TEST(TransactionLogTest, TopicLenOverrunsTotalLengthRejected) {
    auto path = tmpPath();
    TempFile tf{path};

    // Write a "good" record first so we can confirm it survives.
    {
        TransactionLog log(path);
        log.append(makeEntry("txn-pre", 1, 0, TxnState::Empty));
    }

    // Append a record with a single short-topic partition; we will corrupt the
    // topicLen in the body but keep totalLength (and CRC) consistent.
    const std::string kTxnId = "txn-tlen";
    const std::string kTopic  = "x";  // length 1 → topicLen = 1

    {
        TransactionLog log(path);
        log.append(makeEntry(kTxnId, 2, 0, TxnState::Ongoing,
                              {tp(kTopic, 7)}, /*startNs=*/1));
    }

    // Now corrupt the appended record's topicLen field.
    {
        // Read the file to find record2.
        int fd = ::open(path.c_str(), O_RDWR);
        ASSERT_GE(fd, 0);

        // totalLength of record1 tells us where record2 starts.
        uint8_t tmp4[4];
        ASSERT_EQ(::pread(fd, tmp4, 4, 4), 4);
        uint32_t rec1Len;
        std::memcpy(&rec1Len, tmp4, sizeof(rec1Len));
        off_t rec2Start = static_cast<off_t>(rec1Len);

        // Read totalLength of record2.
        ASSERT_EQ(::pread(fd, tmp4, 4, rec2Start + 4), 4);
        uint32_t rec2Len;
        std::memcpy(&rec2Len, tmp4, sizeof(rec2Len));

        // Read the full record.
        std::vector<uint8_t> rec(rec2Len);
        ASSERT_EQ(::pread(fd, rec.data(), rec2Len, rec2Start),
                  static_cast<ssize_t>(rec2Len));

        // The variable body starts at kFixedHeaderSize (52) within the record.
        // Layout of the variable body:
        //   [0 .. txnIdLen)   : txnId bytes
        //   [txnIdLen .. +2)  : topicLen (uint16) of first partition
        //   ...
        constexpr std::size_t kFixedHdr = 52;
        const std::size_t txnIdOff = kFixedHdr; // txnId starts here
        const std::size_t txnIdLen = kTxnId.size();
        const std::size_t topicLenOff = txnIdOff + txnIdLen; // topicLen of partition0

        // Sanity: verify we are at the right position (existing topic is "x" = 0x0001).
        ASSERT_LT(topicLenOff + 2, rec2Len);
        uint16_t existingTopicLen;
        std::memcpy(&existingTopicLen, rec.data() + topicLenOff, 2);
        ASSERT_EQ(existingTopicLen, static_cast<uint16_t>(kTopic.size()))
            << "Sanity: topicLen in the record must match the written topic length";

        // Set topicLen to 0xFFFF (65535) — way beyond totalLength.
        uint16_t bigLen = 0xFFFFu;
        std::memcpy(rec.data() + topicLenOff, &bigLen, 2);

        // Recompute the CRC: covers bytes [0 .. rec2Len-4).
        uint32_t newCrc = testLogCrc32c(rec.data(), rec2Len - 4);
        std::memcpy(rec.data() + rec2Len - 4, &newCrc, 4);

        // Write the modified record back.
        ASSERT_EQ(::pwrite(fd, rec.data(), rec2Len, rec2Start),
                  static_cast<ssize_t>(rec2Len));
        ::close(fd);
    }

    // Reopen: the corrupt record (topicLen overrun) must be rejected.
    // Only the first good record must survive.
    TransactionLog log3(path);
    EXPECT_EQ(log3.size(), 1u)
        << "Record with topicLen overrunning totalLength must be rejected";
    EXPECT_NE(log3.metadata("txn-pre"), nullptr)
        << "The preceding good record must still be present";
    EXPECT_EQ(log3.metadata(kTxnId), nullptr)
        << "The corrupt record must not be applied to the metadata map";
}

// E1 review (fsyncgate): a failed pwrite/fsync of the transaction log must be STICKY.  It threw but left the
// log usable: a retry after a failed fsync could report success for pages the kernel had already dropped,
// and the in-memory txn state would then claim a durable transition that is not on disk.
TEST(TransactionLogRegression, WriteOrFsyncFailureIsStickyFailStop) {
    auto path = tmpPath();
    TempFile tf{path};
    TransactionLog log(path);
    log.append(makeEntry("txn-1", 42, 0, TxnState::Empty));
    log.setWriteFaultForTest(EIO);
    EXPECT_THROW(log.append(makeEntry("txn-1", 42, 0, TxnState::Ongoing)), std::system_error);
    EXPECT_TRUE(log.storageFailed());
    EXPECT_EQ(log.metadata("txn-1")->state, TxnState::Empty) << "a failed append never reaches the in-memory state";
    log.setWriteFaultForTest(0);
    EXPECT_THROW(log.append(makeEntry("txn-1", 42, 0, TxnState::Ongoing)), std::system_error)
        << "sticky: no later 'success' once the log failed";
    EXPECT_EQ(log.metadata("txn-1")->state, TxnState::Empty);
}
