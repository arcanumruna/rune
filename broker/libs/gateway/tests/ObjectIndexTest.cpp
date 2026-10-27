// ObjectIndexTest.cpp - the control-thread (nsId, name) -> id index (namespaces Task 5, fixes #128).
//
// Replaces the global operator new (AllocWindow.cpp), so it is its own binary.
#include "support/AllocWindow.h"

#include <rune/gateway/ObjectIndex.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <map>
#include <random>
#include <string>
#include <utility>
#include <vector>

namespace rg = rune::gateway;
using rune::security::ResourceType;

namespace {
std::vector<std::string> namesOf(int n, const char* prefix = "obj_") {
    std::vector<std::string> v;
    v.reserve(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) v.push_back(prefix + std::to_string(i));
    return v;
}
rg::ObjectIndex fill(const std::vector<std::string>& names) {
    rg::ObjectIndex idx;
    for (std::size_t i = 0; i < names.size(); ++i) idx.onTopic(0, static_cast<int32_t>(i), names[i]);
    return idx;
}
} // namespace

TEST(ObjectIndexRegression, HashLookupZeroAllocRegression) {
    const auto names = namesOf(10'000);
    const auto misses = namesOf(1000, "absent_");
    const rg::ObjectIndex idx = fill(names);
    uint64_t sink = 0;
    {
        rune::testing::AllocWindow w;
        for (int rep = 0; rep < 1'000'000; ++rep) {
            const std::size_t i = (static_cast<std::size_t>(rep) * 7919u) % names.size();
            const auto id = idx.byName(0, names[i]);
            sink += id ? static_cast<uint64_t>(*id) : 0;
            if ((rep & 1023) == 0) sink += idx.byName(0, misses[(rep >> 10) % misses.size()]).has_value();
            sink += idx.byName(1, names[i]).has_value();   // right name, wrong namespace
        }
        EXPECT_EQ(w.count(), 0u) << "byName allocated";
    }
    EXPECT_GT(sink, 0u);
}

// Issue #128: byName was a linear scan.  Lookup time must not grow with the index: 10 000 entries cost less than 4x
// 10 entries (the median of 101 runs of the same lookup count).  A scan would be ~1000x.
TEST(ObjectIndexRegression, Issue128LinearScanRegression) {
    auto medianNs = [](int entries) {
        const auto names = namesOf(entries);
        const rg::ObjectIndex idx = fill(names);
        std::vector<int64_t> runs;
        uint64_t sink = 0;
        for (int r = 0; r < 101; ++r) {
            const auto t0 = std::chrono::steady_clock::now();
            for (int i = 0; i < 20'000; ++i) {
                const auto id = idx.byName(0, names[static_cast<std::size_t>(i * 7919) % names.size()]);
                sink += id ? static_cast<uint64_t>(*id) : 0;
            }
            const auto t1 = std::chrono::steady_clock::now();
            runs.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
        }
        EXPECT_GT(sink, 0u);
        std::nth_element(runs.begin(), runs.begin() + 50, runs.end());
        return runs[50];
    };
    const int64_t small = medianNs(10);
    const int64_t large = medianNs(10'000);
    EXPECT_LT(large, 4 * small) << "10 entries: " << small << " ns, 10000 entries: " << large << " ns";
}

TEST(ObjectIndexRegression, SameNameTwoNsRegression) {
    rg::ObjectIndex idx;
    idx.onTopic(0, 1, "t");
    idx.onTopic(1, 2, "t");
    idx.onView(2, 3, "t_view");
    ASSERT_TRUE(idx.byName(0, "t"));
    ASSERT_TRUE(idx.byName(1, "t"));
    EXPECT_EQ(*idx.byName(0, "t"), 1);
    EXPECT_EQ(*idx.byName(1, "t"), 2);
    EXPECT_FALSE(idx.byName(2, "t"));
    EXPECT_FALSE(idx.byName(0, "t_view"));
    ASSERT_TRUE(idx.byName(2, "t_view"));
    EXPECT_EQ(idx.at(*idx.byName(2, "t_view"))->type, ResourceType::View);
    EXPECT_EQ(idx.at(2)->nsId, 1u);

    std::vector<int32_t> seen;
    idx.forEachInNs(1, [&](int32_t id, const rg::ObjectEntry& e) { seen.push_back(id); EXPECT_EQ(e.nsId, 1u); });
    EXPECT_EQ(seen, std::vector<int32_t>{2});

    idx.onDropped(1);   // dropping (0,t) leaves (1,t)
    EXPECT_FALSE(idx.byName(0, "t"));
    ASSERT_TRUE(idx.byName(1, "t"));
    EXPECT_EQ(*idx.byName(1, "t"), 2);
}

// An AlterView's fresh sink takes the view's name while the retired sink is still live; the retired sink's drop
// must not unlink the name the new sink holds.
TEST(ObjectIndexRegression, RetiredSinkDropKeepsNewHolderRegression) {
    rg::ObjectIndex idx;
    idx.onView(0, 5, "board");
    idx.onView(0, 9, "board");   // the new sink, same (nsId, name)
    EXPECT_EQ(*idx.byName(0, "board"), 9);
    idx.onDropped(5);
    ASSERT_TRUE(idx.byName(0, "board"));
    EXPECT_EQ(*idx.byName(0, "board"), 9);
    idx.onDropped(9);
    EXPECT_FALSE(idx.byName(0, "board"));
}

// Seeded model check against std::map: create / re-apply / drop in random order across namespaces; deletion must keep
// every other probe chain reachable (backward-shift), and the table must agree with the model after every step.
TEST(ObjectIndexRegression, RandomizedModelRegression) {
    for (uint32_t seed = 1; seed <= 20; ++seed) {
        std::mt19937 rng(seed);
        rg::ObjectIndex idx;
        std::map<std::pair<uint32_t, std::string>, int32_t> model;   // live key -> id
        std::map<int32_t, std::pair<uint32_t, std::string>> byId;
        for (int step = 0; step < 4000; ++step) {
            const int32_t id = static_cast<int32_t>(rng() % 300);
            if (rng() % 3 == 0) {
                idx.onDropped(id);
                if (auto it = byId.find(id); it != byId.end()) {
                    if (auto m = model.find(it->second); m != model.end() && m->second == id) model.erase(m);
                    byId.erase(it);
                }
            } else {
                const uint32_t ns = rng() % 4;
                const std::string name = "n" + std::to_string(rng() % 60);
                if (auto it = byId.find(id); it != byId.end()) {   // re-apply under another key
                    if (auto m = model.find(it->second); m != model.end() && m->second == id) model.erase(m);
                }
                idx.onTopic(ns, id, name);
                byId[id] = {ns, name};
                model[{ns, name}] = id;
            }
            if (step % 97 == 0 || step > 3990) {
                for (uint32_t ns = 0; ns < 4; ++ns)
                    for (int n = 0; n < 60; ++n) {
                        const std::string name = "n" + std::to_string(n);
                        const auto got = idx.byName(ns, name);
                        const auto m = model.find({ns, name});
                        ASSERT_EQ(got.has_value(), m != model.end()) << "seed " << seed << " step " << step;
                        if (got) ASSERT_EQ(*got, m->second) << "seed " << seed << " step " << step;
                    }
            }
        }
    }
}
