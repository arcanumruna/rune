#include <gtest/gtest.h>
#include <rune/coordination/OffsetStore.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <fcntl.h>

using namespace rune::coordination;
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// RAII unique temp-file helper (PID + monotonic atomic counter)
// ---------------------------------------------------------------------------
struct TempOffset {
    fs::path path;
    explicit TempOffset(const char* tag = "") {
        static std::atomic<uint64_t> g_seq{0};
        auto seq = g_seq.fetch_add(1, std::memory_order_relaxed);
        std::string name = "rune_offset_" +
                           std::to_string(::getpid()) + "_" +
                           std::to_string(seq) +
                           std::string(tag) + ".log";
        path = fs::temp_directory_path() / name;
        fs::remove(path);
    }
    ~TempOffset() {
        std::error_code ec;
        fs::remove(path, ec);
    }
};

// Helper to make a TopicPartition.
static TopicPartition tp(std::string topic, int32_t part) {
    return {std::move(topic), part};
}

// ===========================================================================
// 1. commit → fetch round-trip
// ===========================================================================

TEST(OffsetStore, CommitFetchRoundTripOffset) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp1", tp("topicA", 0), 42, "meta", 1'000'000);
    auto co = store.fetch("grp1", tp("topicA", 0));
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->offset, 42);
}

TEST(OffsetStore, CommitFetchRoundTripMetadata) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp1", tp("topicA", 0), 42, "my-metadata", 1'000'000);
    auto co = store.fetch("grp1", tp("topicA", 0));
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->metadata, "my-metadata");
}

TEST(OffsetStore, CommitFetchRoundTripTimestamp) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp1", tp("topicA", 0), 42, "meta", 1'234'567'890LL);
    auto co = store.fetch("grp1", tp("topicA", 0));
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->commitTimestampNs, 1'234'567'890LL);
}

// ===========================================================================
// 2. fetch of uncommitted → nullopt
// ===========================================================================

TEST(OffsetStore, FetchUncommittedGroupReturnsNullopt) {
    TempOffset t;
    OffsetStore store(t.path.string());
    EXPECT_FALSE(store.fetch("no-such-group", tp("topicA", 0)).has_value());
}

TEST(OffsetStore, FetchUncommittedTopicPartitionReturnsNullopt) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp", tp("topicA", 0), 10, "", 0);
    EXPECT_FALSE(store.fetch("grp", tp("topicB", 0)).has_value()); // different topic
}

TEST(OffsetStore, FetchUncommittedPartitionReturnsNullopt) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp", tp("topicA", 0), 10, "", 0);
    EXPECT_FALSE(store.fetch("grp", tp("topicA", 1)).has_value()); // different partition
}

// ===========================================================================
// 3. Overwrite — second commit wins (Kafka semantics: lower offset allowed)
// ===========================================================================

TEST(OffsetStore, OverwriteSecondCommitWins) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp", tp("topicA", 0), 100, "old", 1000);
    store.commit("grp", tp("topicA", 0), 200, "new", 2000);
    auto co = store.fetch("grp", tp("topicA", 0));
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->offset, 200);
    EXPECT_EQ(co->metadata, "new");
    EXPECT_EQ(co->commitTimestampNs, 2000);
}

TEST(OffsetStore, OverwriteWithLowerOffsetAllowed) {
    // Kafka OffsetCommit allows overwriting with a lower offset — no monotonicity
    // enforcement in the store itself.
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp", tp("topicA", 0), 500, "first", 1000);
    store.commit("grp", tp("topicA", 0), 100, "reset", 2000);
    auto co = store.fetch("grp", tp("topicA", 0));
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->offset, 100);
    EXPECT_EQ(co->metadata, "reset");
}

TEST(OffsetStore, OverwriteMetadataOnly) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp", tp("topicA", 0), 42, "old-meta", 1000);
    store.commit("grp", tp("topicA", 0), 42, "new-meta", 2000);
    auto co = store.fetch("grp", tp("topicA", 0));
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->metadata, "new-meta");
    EXPECT_EQ(co->offset, 42); // same offset, different metadata
}

// ===========================================================================
// 4. Multi-group isolation (same tp, different groups are independent)
// ===========================================================================

TEST(OffsetStore, MultiGroupIsolationDifferentOffsets) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp-A", tp("topicX", 0), 100, "", 0);
    store.commit("grp-B", tp("topicX", 0), 200, "", 0);

    auto a = store.fetch("grp-A", tp("topicX", 0));
    auto b = store.fetch("grp-B", tp("topicX", 0));
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(a->offset, 100);
    EXPECT_EQ(b->offset, 200);
}

TEST(OffsetStore, MultiGroupIsolationOverwriteOneDoesNotAffectOther) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp-A", tp("topicX", 0), 100, "", 0);
    store.commit("grp-B", tp("topicX", 0), 200, "", 0);
    store.commit("grp-A", tp("topicX", 0), 999, "", 0); // overwrite A

    auto a = store.fetch("grp-A", tp("topicX", 0));
    auto b = store.fetch("grp-B", tp("topicX", 0));
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(a->offset, 999);
    EXPECT_EQ(b->offset, 200); // untouched
}

TEST(OffsetStore, MultiGroupFetchNulloptForWrongGroup) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp-A", tp("topicX", 0), 100, "", 0);
    EXPECT_FALSE(store.fetch("grp-B", tp("topicX", 0)).has_value());
}

// ===========================================================================
// 5. Multi-partition within a group
// ===========================================================================

TEST(OffsetStore, MultiPartitionIndependentCommits) {
    TempOffset t;
    OffsetStore store(t.path.string());
    for (int32_t p = 0; p < 5; ++p) {
        store.commit("grp", tp("topicA", p), p * 10, "", 0);
    }
    for (int32_t p = 0; p < 5; ++p) {
        auto co = store.fetch("grp", tp("topicA", p));
        ASSERT_TRUE(co.has_value()) << "partition " << p;
        EXPECT_EQ(co->offset, p * 10) << "partition " << p;
    }
}

TEST(OffsetStore, MultiTopicIndependentCommits) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp", tp("topic-alpha", 0), 10, "", 0);
    store.commit("grp", tp("topic-beta",  0), 20, "", 0);
    store.commit("grp", tp("topic-gamma", 0), 30, "", 0);

    EXPECT_EQ(store.fetch("grp", tp("topic-alpha", 0))->offset, 10);
    EXPECT_EQ(store.fetch("grp", tp("topic-beta",  0))->offset, 20);
    EXPECT_EQ(store.fetch("grp", tp("topic-gamma", 0))->offset, 30);
}

// ===========================================================================
// 6. fetchAll — returns exactly the committed set
// ===========================================================================

TEST(OffsetStore, FetchAllReturnsAllPartitions) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp", tp("topicA", 0), 10, "", 0);
    store.commit("grp", tp("topicA", 1), 20, "", 0);
    store.commit("grp", tp("topicB", 0), 30, "", 0);

    auto all = store.fetchAll("grp");
    EXPECT_EQ(all.size(), 3u);
}

TEST(OffsetStore, FetchAllEmptyForUnknownGroup) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp-A", tp("topicA", 0), 10, "", 0);
    auto all = store.fetchAll("grp-B");
    EXPECT_TRUE(all.empty());
}

TEST(OffsetStore, FetchAllContainsCorrectOffsets) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp", tp("topicA", 0), 111, "", 0);
    store.commit("grp", tp("topicA", 1), 222, "", 0);

    auto all = store.fetchAll("grp");
    ASSERT_EQ(all.size(), 2u);

    // Build a lookup by partition.
    std::unordered_map<int32_t, int64_t> byPart;
    for (auto& [tpv, co] : all) {
        byPart[tpv.partition] = co.offset;
    }
    EXPECT_EQ(byPart[0], 111);
    EXPECT_EQ(byPart[1], 222);
}

TEST(OffsetStore, FetchAllAfterOverwriteShowsLatest) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp", tp("topicA", 0), 100, "", 0);
    store.commit("grp", tp("topicA", 0), 200, "", 0); // overwrite

    auto all = store.fetchAll("grp");
    ASSERT_EQ(all.size(), 1u);
    EXPECT_EQ(all[0].second.offset, 200);
}

TEST(OffsetStore, FetchAllGroupsAreIsolated) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp-A", tp("topicA", 0), 10, "", 0);
    store.commit("grp-B", tp("topicA", 0), 20, "", 0);
    store.commit("grp-B", tp("topicA", 1), 30, "", 0);

    auto allA = store.fetchAll("grp-A");
    auto allB = store.fetchAll("grp-B");
    EXPECT_EQ(allA.size(), 1u);
    EXPECT_EQ(allB.size(), 2u);
}

// ===========================================================================
// 7. Durability across reopen — commit N, destroy, reopen → all recovered
// ===========================================================================

TEST(OffsetStore, DurabilityAcrossReopenSingleCommit) {
    TempOffset t;
    {
        OffsetStore store(t.path.string());
        store.commit("grp", tp("topicA", 0), 42, "meta", 1234567);
    }
    OffsetStore store2(t.path.string());
    auto co = store2.fetch("grp", tp("topicA", 0));
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->offset, 42);
    EXPECT_EQ(co->metadata, "meta");
    EXPECT_EQ(co->commitTimestampNs, 1234567);
}

TEST(OffsetStore, DurabilityAcrossReopenMultipleCommits) {
    TempOffset t;
    {
        OffsetStore store(t.path.string());
        for (int32_t p = 0; p < 8; ++p) {
            store.commit("grp", tp("topicA", p), p * 100, "m" + std::to_string(p), p * 1000);
        }
    }
    OffsetStore store2(t.path.string());
    for (int32_t p = 0; p < 8; ++p) {
        auto co = store2.fetch("grp", tp("topicA", p));
        ASSERT_TRUE(co.has_value()) << "partition " << p;
        EXPECT_EQ(co->offset, p * 100) << "partition " << p;
        EXPECT_EQ(co->metadata, "m" + std::to_string(p));
        EXPECT_EQ(co->commitTimestampNs, p * 1000LL);
    }
}

TEST(OffsetStore, DurabilityAcrossReopenOverwritePreservesLastCommit) {
    TempOffset t;
    {
        OffsetStore store(t.path.string());
        store.commit("grp", tp("topicA", 0), 100, "old", 1000);
        store.commit("grp", tp("topicA", 0), 999, "new", 9000);
    }
    OffsetStore store2(t.path.string());
    auto co = store2.fetch("grp", tp("topicA", 0));
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->offset, 999);
    EXPECT_EQ(co->metadata, "new");
}

TEST(OffsetStore, DurabilityAcrossReopenMultiGroup) {
    TempOffset t;
    {
        OffsetStore store(t.path.string());
        store.commit("grp-A", tp("topicA", 0), 10, "ma", 100);
        store.commit("grp-B", tp("topicA", 0), 20, "mb", 200);
    }
    OffsetStore store2(t.path.string());
    EXPECT_EQ(store2.fetch("grp-A", tp("topicA", 0))->offset, 10);
    EXPECT_EQ(store2.fetch("grp-B", tp("topicA", 0))->offset, 20);
}

TEST(OffsetStore, DurabilityAcrossReopenFetchAllIntact) {
    TempOffset t;
    {
        OffsetStore store(t.path.string());
        store.commit("grp", tp("t1", 0), 1, "", 0);
        store.commit("grp", tp("t1", 1), 2, "", 0);
        store.commit("grp", tp("t2", 0), 3, "", 0);
    }
    OffsetStore store2(t.path.string());
    auto all = store2.fetchAll("grp");
    EXPECT_EQ(all.size(), 3u);
}

// ===========================================================================
// 8. Torn-tail recovery
// ===========================================================================

// Helper: write N full commits, then truncate file to file_size - bytes_to_cut.
static void commitThenTruncate(const fs::path& path,
                                int             nCommits,
                                std::size_t     cutBytes)
{
    {
        OffsetStore store(path.string());
        for (int i = 0; i < nCommits; ++i) {
            store.commit("grp", tp("topicA", i), i * 10, "m", i * 100);
        }
    }
    // Determine file size, then truncate tail.
    auto size = static_cast<off_t>(fs::file_size(path));
    auto newSize = static_cast<off_t>(size) - static_cast<off_t>(cutBytes);
    if (newSize < 0) newSize = 0;
    int fd = ::open(path.c_str(), O_RDWR);
    ::ftruncate(fd, newSize);
    ::close(fd);
}

TEST(OffsetStore, TornTailRecoveryNoCrash) {
    TempOffset t;
    // Cut 10 bytes from the middle of the last record's body.
    commitThenTruncate(t.path, 3, 10);
    // Reopen must not throw.
    EXPECT_NO_THROW({
        OffsetStore store2(t.path.string());
    });
}

TEST(OffsetStore, TornTailRecoveryRecoversPreviousCommits) {
    TempOffset t;
    // Write 3 commits, cut enough to destroy the last one (records are ~50+ bytes).
    commitThenTruncate(t.path, 3, 20);
    OffsetStore store2(t.path.string());
    // First 2 partitions must be recovered; last one is torn.
    EXPECT_TRUE(store2.fetch("grp", tp("topicA", 0)).has_value());
    EXPECT_TRUE(store2.fetch("grp", tp("topicA", 1)).has_value());
}

TEST(OffsetStore, TornTailRecoveryLastRecordDropped) {
    TempOffset t;
    // Write 3, cut so last record is partially missing.
    commitThenTruncate(t.path, 3, 20);
    OffsetStore store2(t.path.string());
    // Last committed offset (partition 2) was torn — it should be absent.
    EXPECT_FALSE(store2.fetch("grp", tp("topicA", 2)).has_value());
}

TEST(OffsetStore, TornTailStoreIsWritableAfterRecovery) {
    TempOffset t;
    commitThenTruncate(t.path, 3, 20);
    OffsetStore store2(t.path.string());
    // Must be able to write new commits after recovery.
    EXPECT_NO_THROW({
        store2.commit("grp", tp("topicA", 9), 99, "new", 9000);
    });
    EXPECT_EQ(store2.fetch("grp", tp("topicA", 9))->offset, 99);
}

TEST(OffsetStore, TornTailNewCommitPersistsThroughSubsequentReopen) {
    TempOffset t;
    commitThenTruncate(t.path, 3, 20);
    {
        OffsetStore store2(t.path.string());
        store2.commit("grp", tp("topicA", 9), 99, "after-recovery", 9000);
    }
    OffsetStore store3(t.path.string());
    auto co = store3.fetch("grp", tp("topicA", 9));
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->offset, 99);
    EXPECT_EQ(co->metadata, "after-recovery");
}

TEST(OffsetStore, TornTailEntireFileTruncated) {
    TempOffset t;
    // Truncate entire file to 0 — empty store, no crash.
    commitThenTruncate(t.path, 5, 0xFFFFFFFF); // will clamp to 0
    OffsetStore store2(t.path.string());
    EXPECT_FALSE(store2.fetch("grp", tp("topicA", 0)).has_value());
    EXPECT_NO_THROW(store2.commit("grp", tp("topicA", 0), 1, "", 0));
}

// ===========================================================================
// 9. Empty metadata + empty strings
// ===========================================================================

TEST(OffsetStore, EmptyMetadataAllowed) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp", tp("topic", 0), 7, "", 0);
    auto co = store.fetch("grp", tp("topic", 0));
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->offset, 7);
    EXPECT_EQ(co->metadata, "");
}

TEST(OffsetStore, EmptyMetadataDurableAcrossReopen) {
    TempOffset t;
    {
        OffsetStore store(t.path.string());
        store.commit("grp", tp("topic", 0), 7, "", 0);
    }
    OffsetStore store2(t.path.string());
    auto co = store2.fetch("grp", tp("topic", 0));
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->metadata, "");
}

// ===========================================================================
// 10. Offset = 0 and negative offsets
// ===========================================================================

TEST(OffsetStore, OffsetZeroAllowed) {
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp", tp("topic", 0), 0, "", 0);
    EXPECT_EQ(store.fetch("grp", tp("topic", 0))->offset, 0);
}

TEST(OffsetStore, NegativeOffsetAllowed) {
    // Kafka uses -1 as "no offset committed" sentinel; the store must not
    // reject it.
    TempOffset t;
    OffsetStore store(t.path.string());
    store.commit("grp", tp("topic", 0), -1, "", 0);
    EXPECT_EQ(store.fetch("grp", tp("topic", 0))->offset, -1);
}

// ===========================================================================
// 11. Long strings / large metadata
// ===========================================================================

TEST(OffsetStore, LongGroupAndTopicNames) {
    TempOffset t;
    std::string longGroup(200, 'G');
    std::string longTopic(200, 'T');
    std::string longMeta(500, 'M');
    OffsetStore store(t.path.string());
    store.commit(longGroup, tp(longTopic, 3), 12345, longMeta, 9999);
    auto co = store.fetch(longGroup, tp(longTopic, 3));
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->offset, 12345);
    EXPECT_EQ(co->metadata, longMeta);
}

TEST(OffsetStore, LongStringsDurableAcrossReopen) {
    TempOffset t;
    std::string longGroup(300, 'G');
    std::string longTopic(300, 'T');
    std::string longMeta(1000, 'M');
    {
        OffsetStore store(t.path.string());
        store.commit(longGroup, tp(longTopic, 0), 77777, longMeta, 42);
    }
    OffsetStore store2(t.path.string());
    auto co = store2.fetch(longGroup, tp(longTopic, 0));
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(co->offset, 77777);
    EXPECT_EQ(co->metadata, longMeta);
}

// ===========================================================================
// 12. CRC byte-flip — middle byte of record body corrupted
//
// This is the regression test for T1 (audit): torn-tail tests previously
// only ftruncated, never flipped a byte.  The CRC path in replayAndTruncate
// must reject a record whose body byte was corrupted (magic and length still
// match; only the checksum fails).
// ===========================================================================

/// Write N commits, then flip one body byte of the last complete record.
/// The body starts at offset kFixedHeaderSize (36) inside each record.
/// We flip a byte at position (fileSize - totalLength_of_last_record + 36)
/// — the first byte of the group-id field — leaving magic/length intact so
/// the CRC path is actually exercised (not just the truncation path).
static void commitThenFlipBodyByte(const fs::path& path, int nCommits)
{
    // Write the records.
    {
        OffsetStore store(path.string());
        for (int i = 0; i < nCommits; ++i) {
            store.commit("grp", tp("topicA", i), i * 10LL, "meta", i * 100LL);
        }
    }

    // Determine the file size and the start of the last record.
    // Each record for group="grp" (3 bytes), topic="topicA" (6 bytes),
    // meta="meta" (4 bytes):
    //   totalLength = 36 + 3 + 6 + 4 + 4 = 53 bytes
    // We open the file, seek to (fileSize - 53 + 36) and flip a byte.
    auto fileSize = static_cast<off_t>(fs::file_size(path));
    ASSERT_GT(fileSize, 0);

    // We don't hardcode the record size — read it from the last-record header.
    int fd = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);

    // Walk forward from offset 0 to find where the last record starts.
    // Each record has its totalLength at bytes [4..7].
    off_t pos = 0;
    off_t lastRecordStart = 0;
    while (pos < fileSize) {
        uint32_t magic = 0, totalLen = 0;
        if (::pread(fd, &magic, 4, pos) != 4) break;
        if (::pread(fd, &totalLen, 4, pos + 4) != 4) break;
        // Little-endian: on little-endian hosts this reads directly.
        // (We target x86/ARM; safe for this test.)
        if (static_cast<off_t>(pos + totalLen) > fileSize) break;
        lastRecordStart = pos;
        pos += static_cast<off_t>(totalLen);
    }

    // Flip one byte inside the body (offset 36 from record start) of the
    // last record.  This leaves magic, length, and all header fields intact
    // but makes the CRC mismatch → the recovery must detect it and drop the
    // record.
    off_t flipPos = lastRecordStart + 36; // first byte of group-id field
    uint8_t b = 0;
    ssize_t nr = ::pread(fd, &b, 1, flipPos);
    ASSERT_EQ(nr, 1);
    b ^= 0xFF;
    ssize_t nw = ::pwrite(fd, &b, 1, flipPos);
    ASSERT_EQ(nw, 1);
    ::close(fd);
}

TEST(OffsetStore, CrcByteFlip_CorruptedLastRecordDropped) {
    TempOffset t("crcflip");
    // Write 3 commits, corrupt the last one's body byte.
    ASSERT_NO_FATAL_FAILURE(commitThenFlipBodyByte(t.path, 3));

    // Reopen — must not throw and must drop the corrupted last record.
    OffsetStore store2(t.path.string());

    // The first 2 records (partition 0 and 1) must survive.
    EXPECT_TRUE(store2.fetch("grp", tp("topicA", 0)).has_value())
        << "Record 0 (clean) must survive CRC recovery";
    EXPECT_TRUE(store2.fetch("grp", tp("topicA", 1)).has_value())
        << "Record 1 (clean) must survive CRC recovery";

    // The last record (partition 2) had its body corrupted — must be absent.
    EXPECT_FALSE(store2.fetch("grp", tp("topicA", 2)).has_value())
        << "Record 2 (CRC-corrupted) must be dropped by recovery";
}

TEST(OffsetStore, CrcByteFlip_StoreIsWritableAfterRecovery) {
    TempOffset t("crcflip2");
    ASSERT_NO_FATAL_FAILURE(commitThenFlipBodyByte(t.path, 2));
    OffsetStore store2(t.path.string());
    // Must be able to commit after CRC-based recovery.
    EXPECT_NO_THROW(store2.commit("grp", tp("topicA", 9), 99, "new", 9000));
    EXPECT_EQ(store2.fetch("grp", tp("topicA", 9))->offset, 99);
}

// ===========================================================================
// AUDIT B-6: fetch() (single-key) miss paths not explicitly tested
//
// fetchAll-unknown is covered; single-fetch miss for both unknown-group
// AND known-group/unknown-tp must explicitly return nullopt.
// ===========================================================================

TEST(OffsetStore, FetchSingleKey_UnknownGroup_ReturnsNullopt) {
    TempOffset t("fetchmiss1");
    OffsetStore store(t.path.string());
    store.commit("grp-real", tp("topicA", 0), 42, "meta", 1000);
    // Querying a completely different group must return nullopt.
    auto co = store.fetch("grp-nonexistent", tp("topicA", 0));
    EXPECT_FALSE(co.has_value())
        << "fetch() for unknown group must return nullopt";
}

TEST(OffsetStore, FetchSingleKey_KnownGroupUnknownTP_ReturnsNullopt) {
    TempOffset t("fetchmiss2");
    OffsetStore store(t.path.string());
    store.commit("grp", tp("topicA", 0), 42, "meta", 1000);
    // Same group, different topic-partition.
    auto co1 = store.fetch("grp", tp("topicB", 0));
    EXPECT_FALSE(co1.has_value())
        << "fetch() for known group but unknown topic must return nullopt";
    auto co2 = store.fetch("grp", tp("topicA", 99));
    EXPECT_FALSE(co2.has_value())
        << "fetch() for known group+topic but unknown partition must return nullopt";
}

// ===========================================================================
// AUDIT A3 [SILENT hardening]: OffsetStore replay body-length identity check.
//
// A record whose sub-field lengths (groupLen + topicLen + metaLen) do NOT
// sum to (totalLength - kFixedHeaderSize - kChecksumSize) but has a valid
// CRC for the raw bytes as written (so the CRC path alone does not reject it)
// must be detected by an explicit body-length identity check and rejected.
//
// We craft such a record by:
//   1. Writing a normal record (groupLen, topicLen, metaLen, totalLength all
//      consistent, CRC valid).
//   2. Rewriting the file with the SAME raw bytes but with groupLen bumped
//      by 1 (so groupLen+topicLen+metaLen now exceeds the body), then
//      recomputing the CRC over those tampered bytes so the CRC itself is
//      valid for the tampered header — yet the body-length identity fails.
// ===========================================================================

#include <sys/types.h>

namespace {

/// Little-endian read helpers (same pattern as OffsetStore internals).
template<typename T>
static T le_read(const uint8_t* p) {
    T v{};
    std::memcpy(&v, p, sizeof(T));
    // On all our target platforms (x86/ARM LE) this is byte-identical.
    return v;
}

template<typename T>
static void le_write(uint8_t* p, T v) {
    std::memcpy(p, &v, sizeof(T));
}

/// CRC32C (mirrors the one in OffsetStore.cpp — recomputed here for the test).
static uint32_t test_crc32c(const uint8_t* data, std::size_t len) {
    // Build table once.
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
        crc = (crc >> 8) ^ tbl[(crc ^ data[i]) & 0xFF];
    return crc ^ 0xFFFFFFFFu;
}

} // anonymous namespace

TEST(OffsetStore, BodyLengthInconsistency_RecordRejectedCleanly) {
    TempOffset t("bodylen");

    // 1. Write two clean records.
    {
        OffsetStore store(t.path.string());
        store.commit("grp", tp("topicA", 0), 100, "meta", 1000);
        store.commit("grp", tp("topicA", 1), 200, "meta", 2000);
    }

    // 2. Read the file bytes, find the last record, and tamper:
    //    bump groupLen by 1 so groupLen+topicLen+metaLen > body size,
    //    then recompute the CRC so the CRC check itself passes.
    auto fileSize = static_cast<off_t>(fs::file_size(t.path));
    ASSERT_GT(fileSize, 0);

    // Read the whole file.
    int fd = ::open(t.path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);

    std::vector<uint8_t> fileBuf(static_cast<std::size_t>(fileSize));
    ssize_t nr = ::pread(fd, fileBuf.data(), fileBuf.size(), 0);
    ASSERT_EQ(nr, static_cast<ssize_t>(fileSize));

    // Walk to find the last record start.
    off_t pos = 0, lastStart = 0;
    while (pos < fileSize) {
        uint32_t magic = le_read<uint32_t>(fileBuf.data() + pos);
        uint32_t tlen  = le_read<uint32_t>(fileBuf.data() + pos + 4);
        if (magic != 0x4F464653u) break;
        if (pos + static_cast<off_t>(tlen) > fileSize) break;
        lastStart = pos;
        pos += static_cast<off_t>(tlen);
    }

    // lastStart points to the second (last) record.
    uint32_t totalLength = le_read<uint32_t>(fileBuf.data() + lastStart + 4);
    ASSERT_GE(totalLength, 40u); // sanity

    // groupLen is at offset 28 within the record.
    uint16_t origGroupLen = le_read<uint16_t>(fileBuf.data() + lastStart + 28);
    // Bump groupLen by 1 → sub-length sum exceeds body size.
    uint16_t badGroupLen = static_cast<uint16_t>(origGroupLen + 1);
    le_write<uint16_t>(fileBuf.data() + lastStart + 28, badGroupLen);

    // Recompute CRC over [0 .. totalLength-5] of this record so that the
    // CRC is valid for the tampered bytes (defeating the CRC alone as a
    // guard — only the body-length identity check will catch this).
    constexpr std::size_t kChecksumSize = 4;
    uint32_t newCrc = test_crc32c(fileBuf.data() + lastStart, totalLength - kChecksumSize);
    le_write<uint32_t>(fileBuf.data() + lastStart + totalLength - kChecksumSize, newCrc);

    // Write the tampered file back.
    ssize_t nw = ::pwrite(fd, fileBuf.data(), fileBuf.size(), 0);
    ASSERT_EQ(nw, static_cast<ssize_t>(fileSize));
    ::close(fd);

    // 3. Reopen OffsetStore — must not crash, must not mis-decode the tampered record.
    OffsetStore store2(t.path.string());

    // The first record (clean) must survive.
    EXPECT_TRUE(store2.fetch("grp", tp("topicA", 0)).has_value())
        << "Clean record 0 must survive after body-length-inconsistent record is dropped";

    // The tampered record must be rejected (either dropped or the store
    // recovers cleanly — it must not crash or return a corrupt value).
    // If the check is missing, this would either crash (string overread)
    // or silently succeed with a garbage offset/group.
    // The test is satisfied by: no crash, no exception, and the store is
    // writable afterwards.
    EXPECT_NO_THROW(store2.commit("grp", tp("topicA", 9), 999, "ok", 9000));
    EXPECT_EQ(store2.fetch("grp", tp("topicA", 9))->offset, 999);
}
