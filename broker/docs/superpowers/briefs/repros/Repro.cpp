#include "DrHarness.h"
using namespace rune::replication;
using namespace rune::replication::dr;
namespace { constexpr int kCap = 3000; }

// Same as LeaderMovedToLaggingIsrReplica_ReconcilesFromSurvivors, but the
// committed range the lagging replica misses was written under an epoch it
// never saw (one extra leader move while it lagged).
TEST(Review, DeficientLeaderMissingWholeEpoch) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    c.moveLeader(2);                 // epoch 2 on {1,2}; 3 lags, never sees epoch-2 data
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    c.moveLeader(3);                 // lagging ISR replica (controller ISR {1,2,3})
    c.healAll();
    auto pc=[&](const char* tag){ for (NodeId id=1;id<=3;++id){ auto& k=c.rep(id).leaderEpochCacheForTest(); std::cerr<<tag<<" n"<<id<<":"; for(int e=0;e<=4;++e) if(k.startOffsetFor(e)>=0) std::cerr<<" "<<e<<"@"<<k.startOffsetFor(e); std::cerr<<"\n";} };
    pc("pre-move-heal");
    c.run(3); pc("after3");
    bool w = c.waitWritable(kCap);
    pc("end");
    std::cerr << "writable=" << w << " leo3=" << c.rep(3).logEndOffset() << "\n" << c.dump();
    if (w) { (void)c.produceAcked(25); }
    c.run(300);
    std::cerr << "after: leo1=" << c.rep(1).logEndOffset() << " leo2=" << c.rep(2).logEndOffset()
              << " leo3=" << c.rep(3).logEndOffset() << "\n" << c.dump();
    for (NodeId id = 1; id <= 3; ++id) c.expectAckedPresent(id, "missing-epoch");
    EXPECT_TRUE(c.converged());
    auto a = c.ids(1), b = c.ids(3);
    size_t n = std::min(a.size(), b.size()), diff = 0;
    for (size_t i = 0; i < n; ++i) if (a[i] != b[i]) ++diff;
    std::cerr << "ids differ at " << diff << " of " << n << " common positions (n1 vs n3)\n";
}

// Leader-moved-to-lagging with followers ahead by DIFFERENT amounts.
TEST(Review, TwoFollowersAheadDifferentAmounts) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    c.cut(1, 2);                     // 2 stops receiving
    (void)c.produce(5);              // uncommitted tail on 1 only (min-ISR 2: refused? see count)
    c.run(5);
    c.moveLeader(3);
    c.healAll();
    bool w = c.waitWritable(kCap);
    std::cerr << "writable=" << w << "\n" << c.dump();
    if (w) (void)c.produceAcked(5);
    ASSERT_TRUE(c.driveUntil([&] { return c.converged(); }, kCap)) << c.dump();
    for (NodeId id = 1; id <= 3; ++id) { c.expectAckedPresent(id, "two-ahead"); c.expectNoDuplicates(id); }
}

// A live follower holding committed records is left out of the new ISR
// (controller could not reach it); the elected replica lacks them.
TEST(Review, FloorClampedFollowerCountedAsHolder) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);   // on {1,2}
    c.run(20);
    c.crash(1, CrashKind::ProcessKill);
    c.healAll();
    NodeId nl = c.failover({2});         // 2 alive but unreachable to the controller
    std::cerr << "elected " << nl << "\n";
    c.applyMetadata(2);                  // 2 learns the assignment later
    bool w = c.waitWritable(kCap);
    std::cerr << "writable=" << w << "\n" << c.dump();
    if (w) (void)c.produceAcked(25);
    c.run(200);
    std::cerr << c.dump();
    auto a = c.ids(2), b = c.ids(nl);
    size_t n = std::min(a.size(), b.size()), diff = 0;
    for (size_t i = 0; i < n; ++i) if (a[i] != b[i]) ++diff;
    std::cerr << "n2 vs leader: " << diff << " differing positions of " << n << "; converged=" << c.converged() << "\n";
    for (NodeId id : {NodeId(2), nl}) c.expectAckedPresent(id, "floor-clamp");
}

// Production produce gate for pid -1 (Worker::produceRoleGatePasses_ / gateway
// inline path): role == Leader only, NO isLeaderComplete() check.  Emulate one
// non-idempotent produce that lands on the deficient leader before it completes.
TEST(Review, NonIdempotentProduceOnIncompleteDeficientLeader) {
    DrCluster c(3);
    c.start();
    ASSERT_EQ(c.produceAcked(10), 10);
    c.isolate(3);
    ASSERT_EQ(c.produceAcked(20), 20);
    c.run(20);
    c.moveLeader(3);
    c.healAll();
    ASSERT_FALSE(c.rep(3).isLeaderComplete());
    for (uint64_t id = 1000; id < 1005; ++id) {
        rune::protocol::RecordBatchBuilder b(0, 0);
        char v[sizeof id]; std::memcpy(v, &id, sizeof id);
        b.append(std::string_view{}, std::string_view(v, sizeof v), 0);
        const auto bytes = b.build();
        Offset base = -1;
        c.node(3).log->appendNoSyncRestamp(bytes.data(), bytes.size(), 0, base);
        c.rep(3).onLeaderLogAppended();
    }
    c.run(300);
    std::cerr << c.dump();
    std::cerr << "complete=" << c.rep(3).isLeaderComplete() << " hw3=" << c.rep(3).highWatermark() << "\n";
    auto a = c.ids(1), b = c.ids(3);
    size_t n = std::min(a.size(), b.size()), diff = 0;
    for (size_t i = 0; i < n; ++i) if (a[i] != b[i]) ++diff;
    std::cerr << "n1 vs n3 differing positions: " << diff << " of " << n << "\n";
    for (NodeId id = 1; id <= 3; ++id) c.expectAckedPresent(id, "pid-1 on incomplete leader");
}
