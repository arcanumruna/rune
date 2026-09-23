#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>
#include <gtest/gtest.h>
#include <filesystem>
using namespace rune::storage;

TEST(InMemoryLog, AppendReadRoundTrip) {
    InMemoryLog log(/*maxBytes*/1<<20, /*maxRecords*/1024);
    std::vector<uint8_t> b = {1,2,3,4};
    log.append(b.data(), b.size(), /*lastOffset*/0);
    EXPECT_EQ(log.logEndOffset(), 1);
    auto out = log.read(0, 1<<20);
    ASSERT_FALSE(out.empty());
}
TEST(InMemoryLog, EvictionAdvancesLogStartOffset) {
    InMemoryLog log(/*maxBytes*/64, /*maxRecords*/2);
    uint8_t b[8]{};
    log.append(b, sizeof b, 0); // off 0
    log.append(b, sizeof b, 1); // off 1
    log.append(b, sizeof b, 2); // off 2 -> evicts off 0
    EXPECT_EQ(log.logStartOffset(), 1);
    EXPECT_TRUE(log.read(0, 1024).empty()) << "evicted read -> out-of-range -> empty";
}
TEST(InMemoryLog, TruncateMidRing) {
    InMemoryLog log(1<<20, 1024);
    uint8_t b[4]{}; for (int i=0;i<5;++i) log.append(b,sizeof b,i);
    log.truncateTo(3);
    EXPECT_EQ(log.logEndOffset(), 3);
}
TEST(InMemoryLog, NoDiskAndSyncIsNoOp) {
    InMemoryLog log(1<<20, 1024);
    EXPECT_NO_THROW(log.sync());              // no fsync path
    EXPECT_FALSE(log.dir().empty());          // synthetic name, no real dir created
}
TEST(InMemoryLog, IsDiskBackedTag) {
    // InMemoryLog must report false; a real on-disk Log must report true.
    InMemoryLog iml(1<<20, 1024);
    EXPECT_FALSE(iml.isDiskBacked());

    auto tmpDir = std::filesystem::temp_directory_path() / "rune_isdiskbacked_test";
    std::filesystem::create_directories(tmpDir);
    {
        Log diskLog(tmpDir.string());
        EXPECT_TRUE(diskLog.isDiskBacked());
    }
    std::filesystem::remove_all(tmpDir);
}
TEST(InMemoryLog, ReadReturnsAtLeastFirstBatchOverCap) {
    // Construct a log with a generous byte cap (won't evict anything).
    // Use a tiny read maxBytes budget — smaller than the batch itself.
    // read() must still return the first (and only) batch to guarantee
    // consumer forward progress (matches LogSegment::read semantics).
    InMemoryLog log(1<<20, 1024);
    std::vector<uint8_t> bigBatch(256, 0xAB);
    log.append(bigBatch.data(), bigBatch.size(), /*lastOffset*/0);

    // Budget is 1 byte — smaller than the 256-byte batch.
    auto out = log.read(0, /*maxBytes*/1);
    ASSERT_FALSE(out.empty()) << "read() must return at least the first batch even if it exceeds maxBytes";
    EXPECT_EQ(out.size(), bigBatch.size());
}
