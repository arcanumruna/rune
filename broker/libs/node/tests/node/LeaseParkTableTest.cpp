// Lease topics Task 11a: the park table (LeaseParkTable.h) as a pure data structure.
#include "AllocWindow.h"

#include <rune/node/lease/LeaseParkTable.h>

#include <gtest/gtest.h>

#include <memory>
#include <vector>

using namespace rune::node::lease;

namespace {
struct Got { uint32_t rid; uint16_t index; uint16_t code; uint8_t final; uint64_t ttl; int64_t session; int64_t corr; };
std::vector<Got> drainAll(LeaseParkTable& t) {
    std::vector<Got> out;
    t.drain([&](const LeaseParkSlot& s, const LeaseParkAnswer& a) { out.push_back({s.rid, s.index, a.code, a.final, a.ttlMs, s.sessionId, s.correlationId}); });
    return out;
}
}  // namespace

TEST(LeaseParkTableRegression, FullAt4096Regression) {
    auto t = std::make_unique<LeaseParkTable>();
    uint32_t heads[16];
    for (auto& h : heads) h = kLeaseNil;
    uint32_t n = 0;
    for (int r = 0; r < 16; ++r) {
        const uint32_t req = t->openRequest(100 + r, 200 + r);
        ASSERT_NE(req, kLeaseNil);
        for (uint16_t i = 0; i < 256; ++i, ++n) {
            ASSERT_FALSE(t->full()) << n;
            ASSERT_NE(t->park(req, 5, 0, uint32_t(r), i, heads[r]), kLeaseNil);
        }
    }
    EXPECT_EQ(n, kLeaseParkTableEntries);
    EXPECT_TRUE(t->full());
    EXPECT_EQ(t->inUse(), kLeaseParkTableEntries);
    uint32_t extra = kLeaseNil;
    EXPECT_EQ(t->park(0, 5, 0, 99, 0, extra), kLeaseNil) << "the 4097th park is refused";
    EXPECT_EQ(extra, kLeaseNil);
    // free one chain: every slot returns
    t->answerChain(heads[0], 7, 0);
    EXPECT_EQ(drainAll(*t).size(), 256u);
    EXPECT_FALSE(t->full());
    EXPECT_EQ(t->inUse(), kLeaseParkTableEntries - 256);
}

TEST(LeaseParkTableRegression, FinalOnLastOfRequestRegression) {
    LeaseParkTable t;
    const uint32_t req = t.openRequest(1, 2);
    uint32_t a = kLeaseNil, b = kLeaseNil;   // two chains (two keys) of ONE request
    ASSERT_NE(t.park(req, 5, 0, 10, 0, a), kLeaseNil);
    ASSERT_NE(t.park(req, 5, 0, 11, 1, b), kLeaseNil);
    ASSERT_NE(t.park(req, 5, 0, 10, 2, a), kLeaseNil);
    EXPECT_EQ(t.pendingOf(req), 3u);
    t.answerChain(a, 0, 5000);   // indexes 0 then 2
    auto g = drainAll(t);
    ASSERT_EQ(g.size(), 2u);
    EXPECT_EQ(g[0].index, 0); EXPECT_EQ(g[0].final, 0);
    EXPECT_EQ(g[1].index, 2); EXPECT_EQ(g[1].final, 0) << "index 1 is still parked";
    EXPECT_EQ(t.requestsInUse(), 1u);
    t.answerChain(b, 64, 0);
    g = drainAll(t);
    ASSERT_EQ(g.size(), 1u);
    EXPECT_EQ(g[0].index, 1); EXPECT_EQ(g[0].final, 1); EXPECT_EQ(g[0].code, 64);
    EXPECT_EQ(g[0].session, 1); EXPECT_EQ(g[0].corr, 2);
    EXPECT_EQ(t.requestsInUse(), 0u) << "the request is released with its final answer";
    EXPECT_EQ(t.inUse(), 0u);
}

TEST(LeaseParkTableRegression, AnswerWhereOnlyThatPartitionRegression) {
    LeaseParkTable t;
    const uint32_t req = t.openRequest(1, 2);
    uint32_t p0 = kLeaseNil, p1 = kLeaseNil, q0 = kLeaseNil;
    ASSERT_NE(t.park(req, 5, 0, 1, 0, p0), kLeaseNil);
    ASSERT_NE(t.park(req, 5, 1, 2, 1, p1), kLeaseNil);
    ASSERT_NE(t.park(req, 6, 0, 3, 2, q0), kLeaseNil);
    t.answerWhere(5, 0, 2);
    auto g = drainAll(t);
    ASSERT_EQ(g.size(), 1u);
    EXPECT_EQ(g[0].index, 0); EXPECT_EQ(g[0].code, 2); EXPECT_EQ(g[0].final, 0);
    EXPECT_EQ(t.inUse(), 2u) << "(5,1) and (6,0) are still parked";
    t.answerWhere(5, 1, 7);
    t.answerWhere(6, 0, 7);
    g = drainAll(t);
    ASSERT_EQ(g.size(), 2u);
    EXPECT_EQ(g[0].final + g[1].final, 1) << "exactly the request's last answer is final";
}

TEST(LeaseParkTableRegression, ChainOrderOldestFirstRegression) {
    LeaseParkTable t;
    const uint32_t req = t.openRequest(1, 2);
    uint32_t h = kLeaseNil;
    for (uint16_t i = 0; i < 5; ++i) ASSERT_NE(t.park(req, 5, 0, 9, i, h), kLeaseNil);
    t.answerChain(h, 0, 11);
    EXPECT_EQ(h, kLeaseNil);
    const auto g = drainAll(t);
    ASSERT_EQ(g.size(), 5u);
    for (uint16_t i = 0; i < 5; ++i) { EXPECT_EQ(g[i].index, i); EXPECT_EQ(g[i].final, i == 4 ? 1 : 0); EXPECT_EQ(g[i].ttl, 11u); }
}

TEST(LeaseParkTableRegression, AnswerOneUnlinksMiddleRegression) {
    LeaseParkTable t;
    const uint32_t req = t.openRequest(1, 2);
    uint32_t h = kLeaseNil;
    uint32_t slots[4];
    for (uint16_t i = 0; i < 4; ++i) slots[i] = t.park(req, 5, 0, 9, i, h);
    t.answerOne(h, slots[1], 3, 1);   // a middle one
    t.answerOne(h, slots[3], 4, 2);   // the head (newest)
    auto g = drainAll(t);
    ASSERT_EQ(g.size(), 2u);
    EXPECT_EQ(g[0].index, 1); EXPECT_EQ(g[0].code, 3);
    EXPECT_EQ(g[1].index, 3); EXPECT_EQ(g[1].code, 4);
    std::vector<uint16_t> left;
    t.forEachInChain(h, [&](uint32_t, const LeaseParkSlot& s) { left.push_back(s.index); });
    ASSERT_EQ(left.size(), 2u);
    EXPECT_EQ(left[0], 0); EXPECT_EQ(left[1], 2) << "oldest first";
    t.answerChain(h, 0, 0);
    g = drainAll(t);
    ASSERT_EQ(g.size(), 2u);
    EXPECT_EQ(g[1].final, 1);
}

TEST(LeaseParkTableAllocRegression, NoAllocAfterConstructionRegression) {
    auto t = std::make_unique<LeaseParkTable>();
    rune::testing::AllocWindow w;
    uint32_t h = kLeaseNil;
    for (int round = 0; round < 50; ++round) {
        const uint32_t req = t->openRequest(1, round);
        for (uint16_t i = 0; i < 200; ++i) (void)t->park(req, 5, 0, 9, i, h);
        t->forEachInChain(h, [](uint32_t, const LeaseParkSlot&) {});
        t->answerChain(h, 0, 1);
        t->drain([](const LeaseParkSlot&, const LeaseParkAnswer&) {});
    }
    EXPECT_EQ(w.count(), 0u);
}
