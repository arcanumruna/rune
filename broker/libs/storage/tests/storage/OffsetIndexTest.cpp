#include <gtest/gtest.h>
#include <rune/storage/OffsetIndex.h>
#include <filesystem>
#include <string>
using namespace rune::storage;

TEST(OffsetIndex, AppendAndLookupLargestLowerBound) {
    auto p = (std::filesystem::temp_directory_path() / "rune.index").string();
    std::filesystem::remove(p);
    OffsetIndex idx(p, 100 /*baseOffset*/, 8 * 1024);
    idx.append(105, 0);
    idx.append(110, 200);
    idx.append(120, 512);
    EXPECT_EQ(idx.entries(), 3);
    EXPECT_EQ(idx.lookup(104).position, 0);     // below first entry -> base
    EXPECT_EQ(idx.lookup(105).offset, 105);
    EXPECT_EQ(idx.lookup(115).offset, 110);     // largest <= 115
    EXPECT_EQ(idx.lookup(999).offset, 120);
    std::filesystem::remove(p);
}

TEST(OffsetIndex, WarmSectionBoundaryCorrectness) {
    auto p = (std::filesystem::temp_directory_path() / "rune_warm.index").string();
    std::filesystem::remove(p);
    OffsetIndex idx(p, 0, 64 * 1024);
    for (int i = 0; i < 4000; ++i) idx.append(i * 10, i * 32); // 4000 entries > 1024 warm
    EXPECT_EQ(idx.lookup(5).offset, 0);
    EXPECT_EQ(idx.lookup(10 * 500 + 3).offset, 10 * 500);     // cold section
    EXPECT_EQ(idx.lookup(10 * 3999 + 1).offset, 10 * 3999);   // warm section (tail)
    EXPECT_EQ(idx.lookup(10 * 3000).offset, 10 * 3000);       // near boundary
    std::filesystem::remove(p);
}

TEST(OffsetIndex, WarmLookupSearchesTailOnly) {
    auto p = (std::filesystem::temp_directory_path() / "rune_warm2.index").string();
    std::filesystem::remove(p);
    OffsetIndex idx(p, 0, 64 * 1024);
    for (int i = 0; i < 4000; ++i) idx.append(i * 10, i);
    (void)idx.lookup(10 * 3999);
    EXPECT_GE(idx.lastSearchLow(), 4000 - 1024 - 1); // searched only the warm tail
    std::filesystem::remove(p);
}

// Edge: empty index — entries()==0 and lookup(anything) returns {baseOffset, 0}
TEST(OffsetIndex, EmptyIndexLookupReturnsBase) {
    auto p = (std::filesystem::temp_directory_path() / "rune_idx_empty.index").string();
    std::filesystem::remove(p);
    OffsetIndex idx(p, 500, 8 * 1024);
    EXPECT_EQ(idx.entries(), 0);
    auto r0 = idx.lookup(0);
    EXPECT_EQ(r0.offset, 500);
    EXPECT_EQ(r0.position, 0);
    auto r1 = idx.lookup(500);
    EXPECT_EQ(r1.offset, 500);
    EXPECT_EQ(r1.position, 0);
    auto r2 = idx.lookup(999999);
    EXPECT_EQ(r2.offset, 500);
    EXPECT_EQ(r2.position, 0);
    std::filesystem::remove(p);
}

// Edge: single entry — below base returns base, exact match, above returns the single entry
TEST(OffsetIndex, SingleEntryLookup) {
    auto p = (std::filesystem::temp_directory_path() / "rune_idx_single.index").string();
    std::filesystem::remove(p);
    OffsetIndex idx(p, 200, 8 * 1024);
    idx.append(205, 100);
    EXPECT_EQ(idx.entries(), 1);
    // Below first entry -> base
    auto rb = idx.lookup(201);
    EXPECT_EQ(rb.offset, 200);
    EXPECT_EQ(rb.position, 0);
    // Exact match
    auto re = idx.lookup(205);
    EXPECT_EQ(re.offset, 205);
    EXPECT_EQ(re.position, 100);
    // Above -> returns the single entry (largest <= target)
    auto ra = idx.lookup(9999);
    EXPECT_EQ(ra.offset, 205);
    EXPECT_EQ(ra.position, 100);
    std::filesystem::remove(p);
}

// Edge: non-zero baseOffset — verify relative-offset math produces correct absolute offsets
TEST(OffsetIndex, NonZeroBaseOffsetMath) {
    auto p = (std::filesystem::temp_directory_path() / "rune_idx_nonzero.index").string();
    std::filesystem::remove(p);
    const int64_t base = 1000;
    OffsetIndex idx(p, base, 8 * 1024);
    idx.append(1000, 0);
    idx.append(1010, 200);
    idx.append(1020, 500);
    // Lookup below all -> base
    EXPECT_EQ(idx.lookup(999).offset, base);
    EXPECT_EQ(idx.lookup(999).position, 0);
    // Exact first
    EXPECT_EQ(idx.lookup(1000).offset, 1000);
    EXPECT_EQ(idx.lookup(1000).position, 0);
    // Between first and second
    EXPECT_EQ(idx.lookup(1005).offset, 1000);
    EXPECT_EQ(idx.lookup(1005).position, 0);
    // Exact second
    EXPECT_EQ(idx.lookup(1010).offset, 1010);
    EXPECT_EQ(idx.lookup(1010).position, 200);
    // Between second and third
    EXPECT_EQ(idx.lookup(1015).offset, 1010);
    EXPECT_EQ(idx.lookup(1015).position, 200);
    // Exact third
    EXPECT_EQ(idx.lookup(1020).offset, 1020);
    EXPECT_EQ(idx.lookup(1020).position, 500);
    // Above all -> last entry
    EXPECT_EQ(idx.lookup(9999).offset, 1020);
    EXPECT_EQ(idx.lookup(9999).position, 500);
    std::filesystem::remove(p);
}

// Edge: strictly-increasing offsets; lookup picks the largest <= target at exact and between points
TEST(OffsetIndex, StrictlyIncreasingOffsetsLookup) {
    auto p = (std::filesystem::temp_directory_path() / "rune_idx_strict.index").string();
    std::filesystem::remove(p);
    OffsetIndex idx(p, 0, 8 * 1024);
    // Append offsets 0, 5, 10, 15, 20 with positions 0,50,100,150,200
    for (int i = 0; i < 5; ++i) idx.append(i * 5, i * 50);
    // Exact matches
    for (int i = 0; i < 5; ++i) {
        auto r = idx.lookup(i * 5);
        EXPECT_EQ(r.offset, i * 5)   << "exact at i=" << i;
        EXPECT_EQ(r.position, i * 50) << "exact at i=" << i;
    }
    // Between: lookup(3) -> largest <= 3 is 0
    EXPECT_EQ(idx.lookup(3).offset, 0);
    // Between: lookup(7) -> largest <= 7 is 5
    EXPECT_EQ(idx.lookup(7).offset, 5);
    // Between: lookup(12) -> largest <= 12 is 10
    EXPECT_EQ(idx.lookup(12).offset, 10);
    // Between: lookup(18) -> largest <= 18 is 15
    EXPECT_EQ(idx.lookup(18).offset, 15);
    std::filesystem::remove(p);
}

// Helper to build a unique index path with a tag
static std::string uniqueIdx(const char* tag) {
    return (std::filesystem::temp_directory_path() / (std::string("rune_idx_warm_") + tag + ".index")).string();
}

// Edge: kWarmEntries boundary — indexes of sizes 1023, 1024, 1025, 2048 entries
// For each: verify lookup in cold region, at the warm boundary, and in the warm tail.
TEST(OffsetIndex, WarmBoundary1023Entries) {
    auto p = uniqueIdx("1023");
    std::filesystem::remove(p);
    const int N = 1023;
    OffsetIndex idx(p, 0, 64 * 1024);
    for (int i = 0; i < N; ++i) idx.append(i * 10LL, i * 4);
    EXPECT_EQ(idx.entries(), N);
    // Cold region (well below warm boundary)
    EXPECT_EQ(idx.lookup(10LL * 100).offset, 10LL * 100);
    // Near end
    EXPECT_EQ(idx.lookup(10LL * (N-1)).offset, 10LL * (N-1));
    // Between entries near end
    EXPECT_EQ(idx.lookup(10LL * (N-1) + 3).offset, 10LL * (N-1));
    std::filesystem::remove(p);
}

TEST(OffsetIndex, WarmBoundary1024Entries) {
    auto p = uniqueIdx("1024");
    std::filesystem::remove(p);
    const int N = 1024; // exactly kWarmEntries
    OffsetIndex idx(p, 0, 64 * 1024);
    for (int i = 0; i < N; ++i) idx.append(i * 10LL, i * 4);
    EXPECT_EQ(idx.entries(), N);
    // Boundary entry
    EXPECT_EQ(idx.lookup(10LL * (N-1)).offset, 10LL * (N-1));
    // Cold region
    EXPECT_EQ(idx.lookup(10LL * 200).offset, 10LL * 200);
    // Above all
    EXPECT_EQ(idx.lookup(10LL * N + 100).offset, 10LL * (N-1));
    std::filesystem::remove(p);
}

TEST(OffsetIndex, WarmBoundary1025Entries) {
    auto p = uniqueIdx("1025");
    std::filesystem::remove(p);
    const int N = 1025; // one beyond kWarmEntries — warm tail starts at entry 1
    OffsetIndex idx(p, 0, 64 * 1024);
    for (int i = 0; i < N; ++i) idx.append(i * 10LL, i * 4);
    EXPECT_EQ(idx.entries(), N);
    // Cold region (entry 0)
    EXPECT_EQ(idx.lookup(3LL).offset, 0LL);
    // Warm boundary
    EXPECT_EQ(idx.lookup(10LL * (N - 1025)).offset, 10LL * (N - 1025));
    // Warm tail
    EXPECT_EQ(idx.lookup(10LL * (N-1)).offset, 10LL * (N-1));
    std::filesystem::remove(p);
}

TEST(OffsetIndex, WarmBoundary2048Entries) {
    auto p = uniqueIdx("2048");
    std::filesystem::remove(p);
    const int N = 2048;
    OffsetIndex idx(p, 0, 128 * 1024);
    for (int i = 0; i < N; ++i) idx.append(i * 10LL, i * 4);
    EXPECT_EQ(idx.entries(), N);
    // Cold region
    EXPECT_EQ(idx.lookup(10LL * 100).offset, 10LL * 100);
    // At warm boundary (first warm entry = N - kWarmEntries = 1024)
    EXPECT_EQ(idx.lookup(10LL * 1024).offset, 10LL * 1024);
    // Warm tail
    EXPECT_EQ(idx.lookup(10LL * (N-1) + 5).offset, 10LL * (N-1));
    std::filesystem::remove(p);
}

// Edge: lastSearchLow() — warm-tail lookup starts at firstWarm; cold lookup starts at 0
TEST(OffsetIndex, LastSearchLowWarmTailVsColdRegion) {
    auto p = (std::filesystem::temp_directory_path() / "rune_idx_lsl.index").string();
    std::filesystem::remove(p);
    const int N = 4000;
    OffsetIndex idx(p, 0, 64 * 1024);
    for (int i = 0; i < N; ++i) idx.append(i * 10LL, i);
    // kWarmEntries = 1024; firstWarm = N - 1 - 1024 = 2975
    const int kWarmEntries = 1024;
    const int firstWarm = N - 1 - kWarmEntries;

    // Lookup in warm tail (target > entry[firstWarm])
    (void)idx.lookup(10LL * (N - 1));
    EXPECT_GE(idx.lastSearchLow(), firstWarm);

    // Lookup in cold region (target < entry[firstWarm])
    (void)idx.lookup(10LL * 100); // well into cold region
    EXPECT_EQ(idx.lastSearchLow(), 0);

    std::filesystem::remove(p);
}
