#include <rune/storage/InMemoryLog.h>
#include <gtest/gtest.h>
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
