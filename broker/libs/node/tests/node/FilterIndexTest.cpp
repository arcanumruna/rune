// FilterIndexTest.cpp — Phase 2c Task 6: FilterIndex (value -> members, open addressing, capped).
#include "rune/node/filter/FilterIndex.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <new>
#include <random>
#include <vector>

#ifdef RUNE_FILTER_ENGINE_ALLOC_COUNT
namespace {
bool     gArmed  = false;   // single-threaded test binary: plain globals
uint64_t gAllocs = 0;
}  // namespace
void* operator new(std::size_t n) {
    if (gArmed) ++gAllocs;
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t n) {
    if (gArmed) ++gAllocs;
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
#endif

using rune::codec::FilterKey;
using rune::node::filter::FilterIndex;

namespace {
std::vector<uint32_t> members(const FilterIndex& ix, FilterKey k) {
    std::vector<uint32_t> v;
    ix.forEach(k, [&](uint32_t m) { v.push_back(m); });
    std::sort(v.begin(), v.end());
    return v;
}
}  // namespace

TEST(FilterIndex, AddForEachRemove) {
    FilterIndex ix;
    ix.init(8, 8);
    const FilterKey a{0, 7}, b{0, 9};
    ASSERT_TRUE(ix.add(a, 1));
    ASSERT_TRUE(ix.add(a, 2));
    ASSERT_TRUE(ix.add(b, 3));
    EXPECT_EQ(ix.distinctValues(), 2u);
    EXPECT_EQ(members(ix, a), (std::vector<uint32_t>{1, 2}));
    EXPECT_EQ(members(ix, b), (std::vector<uint32_t>{3}));
    ix.remove(a, 1);
    EXPECT_EQ(members(ix, a), (std::vector<uint32_t>{2}));
    ix.remove(a, 2);
    EXPECT_TRUE(members(ix, a).empty());
    EXPECT_EQ(ix.distinctValues(), 1u);
    EXPECT_EQ(members(ix, b), (std::vector<uint32_t>{3}));
}

TEST(FilterIndex, MissVisitsNothing) {
    FilterIndex ix;
    EXPECT_FALSE(ix.initialized());
    ix.init(4, 4);
    EXPECT_TRUE(ix.initialized());
    ASSERT_TRUE(ix.add(FilterKey{0, 1}, 5));
    EXPECT_TRUE(members(ix, FilterKey{0, 2}).empty());
    ix.remove(FilterKey{0, 2}, 5);   // removing an absent registration is a no-op
    EXPECT_EQ(members(ix, FilterKey{0, 1}), (std::vector<uint32_t>{5}));
}

TEST(FilterIndex, FixedStrSizedKeysCollideOnlyWhenEqual) {
    FilterIndex ix;
    ix.init(8, 8);
    ASSERT_TRUE(ix.add(FilterKey{1, 5}, 10));
    ASSERT_TRUE(ix.add(FilterKey{2, 5}, 20));
    ASSERT_TRUE(ix.add(FilterKey{3, 5}, 30));
    EXPECT_EQ(members(ix, FilterKey{1, 5}), (std::vector<uint32_t>{10}));
    EXPECT_EQ(members(ix, FilterKey{2, 5}), (std::vector<uint32_t>{20}));
    EXPECT_EQ(members(ix, FilterKey{3, 5}), (std::vector<uint32_t>{30}));
    EXPECT_TRUE(members(ix, FilterKey{4, 5}).empty());
}

TEST(FilterIndexRegression, CapRefusesTheNextDistinctValueNotAnExistingOneRegression) {
    FilterIndex ix;
    ix.init(4, 16);
    for (uint64_t i = 0; i < 4; ++i) ASSERT_TRUE(ix.add(FilterKey{0, i}, static_cast<uint32_t>(i)));
    EXPECT_FALSE(ix.add(FilterKey{0, 100}, 99));
    EXPECT_EQ(ix.distinctValues(), 4u);
    EXPECT_TRUE(members(ix, FilterKey{0, 100}).empty());
    EXPECT_TRUE(ix.add(FilterKey{0, 2}, 50));
    EXPECT_EQ(members(ix, FilterKey{0, 2}), (std::vector<uint32_t>{2, 50}));
    EXPECT_EQ(ix.distinctValues(), 4u);
}

TEST(FilterIndexRegression, BackwardShiftDeleteKeepsEveryOtherKeyReachableRegression) {
    constexpr uint64_t kSeed = 4242;
    std::mt19937_64 rng(kSeed);
    FilterIndex ix;
    ix.init(1024, 1024);
    std::map<FilterKey, uint32_t> live;
    while (live.size() < 1000) {
        const FilterKey k{rng() % 7, rng()};
        if (live.count(k)) continue;
        const auto m = static_cast<uint32_t>(live.size());
        ASSERT_TRUE(ix.add(k, m)) << "seed " << kSeed;
        live[k] = m;
    }
    std::vector<FilterKey> keys;
    for (auto& [k, m] : live) keys.push_back(k);
    std::shuffle(keys.begin(), keys.end(), rng);
    for (size_t i = 0; i < keys.size() / 2; ++i) {
        ix.remove(keys[i], live[keys[i]]);
        live.erase(keys[i]);
    }
    EXPECT_EQ(ix.distinctValues(), live.size()) << "seed " << kSeed;
    for (auto& [k, m] : live) EXPECT_EQ(members(ix, k), (std::vector<uint32_t>{m})) << "seed " << kSeed;
    for (size_t i = 0; i < keys.size() / 2; ++i) EXPECT_TRUE(members(ix, keys[i]).empty()) << "seed " << kSeed;
}

class FilterIndexModel : public ::testing::TestWithParam<uint32_t> {};

TEST_P(FilterIndexModel, ModelCheckedAgainstAMultimap) {
    const uint32_t seed = GetParam();
    std::mt19937_64 rng(seed);
    constexpr uint32_t kMaxValues = 64;
    FilterIndex ix;
    ix.init(kMaxValues, 64);
    std::multimap<FilterKey, uint32_t> model;
    auto distinct = [&] {
        uint32_t n = 0;
        for (auto it = model.begin(); it != model.end(); it = model.upper_bound(it->first)) ++n;
        return n;
    };
    auto modelMembers = [&](FilterKey k) {
        std::vector<uint32_t> v;
        auto [lo, hi] = model.equal_range(k);
        for (auto it = lo; it != hi; ++it) v.push_back(it->second);
        std::sort(v.begin(), v.end());
        return v;
    };
    auto pick = [&] { return FilterKey{rng() % 3, rng() % 90}; };   // small domain: forces collisions and the cap
    for (int step = 0; step < 5000; ++step) {
        const FilterKey k = pick();
        switch (rng() % 3) {
            case 0: {
                const auto m = static_cast<uint32_t>(rng() % 1000);
                bool dup = false;
                auto [lo, hi] = model.equal_range(k);
                for (auto it = lo; it != hi; ++it) dup |= it->second == m;
                if (dup) break;   // one (k, member) registration at a time
                const bool isNew = model.count(k) == 0;
                const bool expect = !(isNew && distinct() >= kMaxValues);
                EXPECT_EQ(ix.add(k, m), expect) << "seed " << seed << " step " << step;
                if (expect) model.emplace(k, m);
                break;
            }
            case 1: {
                auto [lo, hi] = model.equal_range(k);
                if (lo == hi) { ix.remove(k, 12345); break; }
                auto it = lo;
                std::advance(it, rng() % std::distance(lo, hi));
                ix.remove(k, it->second);
                model.erase(it);
                break;
            }
            default:
                EXPECT_EQ(members(ix, k), modelMembers(k)) << "seed " << seed << " step " << step;
        }
        ASSERT_EQ(ix.distinctValues(), distinct()) << "seed " << seed << " step " << step;
    }
    for (uint64_t h = 0; h < 3; ++h)
        for (uint64_t l = 0; l < 90; ++l)
            ASSERT_EQ(members(ix, FilterKey{h, l}), modelMembers(FilterKey{h, l})) << "seed " << seed;
}
INSTANTIATE_TEST_SUITE_P(Seeds, FilterIndexModel, ::testing::Range(1u, 17u));

#ifdef RUNE_FILTER_ENGINE_ALLOC_COUNT
TEST(FilterIndex, LookupAllocatesNothing) {
    FilterIndex ix;
    ix.init(256, 256);
    for (uint64_t i = 0; i < 200; ++i) ASSERT_TRUE(ix.add(FilterKey{0, i}, static_cast<uint32_t>(i)));
    uint64_t sink = 0;
    gAllocs = 0;
    gArmed  = true;
    for (uint64_t i = 0; i < 100000; ++i) ix.forEach(FilterKey{0, i % 400}, [&](uint32_t m) { sink += m; });
    gArmed = false;
    EXPECT_EQ(gAllocs, 0u);
    EXPECT_GT(sink, 0u);
}
#endif
