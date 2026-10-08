// SessionAnnouncerTest.cpp — announce-before-accept over the runtime→worker rings.
#include <rune/node/SessionAnnouncer.h>
#include <rune/node/WorkerPool.h>
#include <rune/metadata/ShardRouter.h>

#include <gtest/gtest.h>

using namespace rune::node;

namespace {
std::unique_ptr<WorkerPool> pool(int32_t n, std::size_t ringBytes = 1 << 16) {
    return std::make_unique<WorkerPool>(n, rune::metadata::ShardRouter(n), ringBytes, 4096, 16, 16);
}
// A ring read stops at the buffer's wrap point (Agrona OneToOneRingBuffer), so a
// ring that wrapped needs a second pass — bounded, never "until idle".
void drainAll(WorkerPool& p) {
    for (int pass = 0; pass < 2; ++pass)
        for (int32_t w = 0; w < p.workerCount(); ++w) p.worker(w).doWork(1);
}
}

TEST(SessionAnnouncerRegression, AnnouncesOnceToEveryWorker) {
    auto p = pool(3);
    SessionAnnouncer a;
    EXPECT_TRUE(a.announce(*p, 5, "aeron:udp?endpoint=h:1", 9));
    drainAll(*p);
    for (int32_t w = 0; w < 3; ++w) EXPECT_EQ(p->worker(w).sessionAnnouncesSeenForTest(), 1u);
}

TEST(SessionAnnouncerRegression, FullRingDefersAndRetriesOnlyTheMissingWorker) {
    auto p = pool(2, /*ringBytes=*/4096);
    WorkerOp filler; filler.type = WorkerOp::Type::SessionClose; filler.sessionId = 1;
    while (p->worker(1).tryEnqueueOp(filler)) {}             // worker 1's ring is now full
    SessionAnnouncer a;
    EXPECT_FALSE(a.announce(*p, 5, "c", 9)) << "no ConnectResponse until every worker knows the session";
    p->worker(1).doWork(1);                                  // drains worker 1
    EXPECT_TRUE(a.announce(*p, 5, "c", 9));
    drainAll(*p);
    EXPECT_EQ(p->worker(0).sessionAnnouncesSeenForTest(), 1u) << "worker 0 must not get a duplicate";
    EXPECT_EQ(p->worker(1).sessionAnnouncesSeenForTest(), 1u);
}

TEST(SessionAnnouncerRegression, CloseRetriesUntilEveryWorkerAccepted) {
    auto p = pool(2, /*ringBytes=*/4096);
    WorkerOp filler; filler.type = WorkerOp::Type::SessionClose; filler.sessionId = 1;
    while (p->worker(0).tryEnqueueOp(filler)) {}
    SessionAnnouncer a;
    a.close(*p, 5);
    EXPECT_EQ(a.pendingClosesForTest(), 1u);
    p->worker(0).doWork(1);
    EXPECT_EQ(a.retryCloses(*p), 1);
    EXPECT_EQ(a.pendingClosesForTest(), 0u);
}

// D6: the announcement carries the session's ConnectRequest.clientVersion to
// every worker (a shard worker acks directly and gates UnknownProducerId on it).
TEST(SessionAnnouncerRegression, AnnouncementCarriesTheClientVersionRegression) {
    auto p = pool(2);
    SessionAnnouncer a;
    EXPECT_TRUE(a.announce(*p, 5, "c", 9, /*clientVersion=*/1));
    drainAll(*p);
    for (int32_t w = 0; w < 2; ++w) EXPECT_EQ(p->worker(w).lastAnnouncedClientVersionForTest(), 1);
    EXPECT_TRUE(a.announce(*p, 6, "c", 9, /*clientVersion=*/2));
    drainAll(*p);
    for (int32_t w = 0; w < 2; ++w) EXPECT_EQ(p->worker(w).lastAnnouncedClientVersionForTest(), 2);
}
