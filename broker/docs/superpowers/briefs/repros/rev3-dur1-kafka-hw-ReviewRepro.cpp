#include "/Users/vradu/Projects/rune-broker-wt31/libs/replication/tests/replication/PartitionReplicaTest.cpp"

namespace {
FetchRequestMsg fr(Epoch ep, NodeId f, Offset off, int32_t lfe) {
    FetchRequestMsg r{}; r.leaderEpoch = ep; r.partition = 0; r.followerId = f;
    r.fetchOffset = off; r.maxBytes = 0; r.topicId = 0; r.lastFetchedEpoch = lfe; return r;
}
}

// R1: the leader's per-follower LEO map survives across leaderships; a member
// that has NOT fetched in the new leadership is counted at its OLD position.
TEST(ReviewRepro, StaleFollowerLeoAcrossLeaderships) {
    TempDir d("rv1"); rune::storage::Log log(d.path);
    InMemoryTransport t;
    constexpr NodeId kB = 1, kA = 2, kC = 3;
    std::vector<NodeId> all = {kB, kA, kC};
    PartitionReplica b(makeConfig(kB, 10'000, 1'000'000, 2), Role::Leader, 1, all, all, &log, &t);
    for (int i = 0; i < 20; ++i) ASSERT_GE(b.append(makePayload(1)), 0);
    int64_t clock = 1'000'000;
    b.doWork(clock);
    ASSERT_EQ(b.onFetchRequest(fr(1, kA, 20, 1), clock), kErrNone);
    ASSERT_EQ(b.onFetchRequest(fr(1, kC, 5, 1), clock), kErrNone);
    EXPECT_EQ(b.highWatermark(), 5);
    b.becomeFollower(2, kC);            // C leads epoch 2 (A is truncated there)
    b.becomeLeader(3, {kB, kA, kC});     // leadership back to B; A has not fetched yet
    for (int i = 0; i < 3; ++i) ASSERT_GE(b.append(makePayload(3)), 0);
    clock += 1'000'000; b.doWork(clock);
    ASSERT_EQ(b.onFetchRequest(fr(3, kC, b.logEndOffset(), 3), clock), kErrNone);
    std::printf("R1: HW=%lld followerLeo(A)=%lld (A never fetched in epoch 3)\n",
                (long long)b.highWatermark(), (long long)b.followerLeoForTest(kA));
    EXPECT_LE(b.highWatermark(), 5) << "HW counted A at its previous-leadership position";
}

// R2: ISR expansion admits a follower below the leader-epoch start offset
// (Kafka isFollowerInSync requires followerEnd >= leaderEpochStartOffset).
TEST(ReviewRepro, ExpansionBelowLeaderEpochStart) {
    TempDir d("rv2"); rune::storage::Log log(d.path);
    InMemoryTransport t;
    constexpr NodeId kA = 2, kB = 1, kC = 3;
    std::vector<NodeId> all = {kB, kA, kC};
    PartitionReplica a(makeConfig(kA, 10'000, 1'000'000, 2), Role::Follower, 1, all, all, &log, &t);
    seedStamped(a, 0, 10, 1, /*hw=*/5);   // holds 10 committed (ISR {B,A}), stale HW view 5
    a.becomeLeader(2, {kA, kB});           // preferred election; C outside the ISR
    int64_t clock = 1'000'000;
    a.doWork(clock);
    ASSERT_EQ(a.onFetchRequest(fr(2, kC, 7, 1), clock), kErrNone);   // C holds only [0,7)
    for (int i = 0; i < 5; ++i) { clock += 1'000'000; a.doWork(clock); }
    const auto& isr = a.isr();
    const bool cIn = std::find(isr.begin(), isr.end(), kC) != isr.end();
    std::printf("R2: HW=%lld cInIsr=%d electionLeo=10 cLeo=7\n", (long long)a.highWatermark(), (int)cIn);
    EXPECT_FALSE(cIn) << "C admitted to the ISR while missing records the new leader held at election";
}

// R1b: end to end — A truncates under C, B regains leadership; B acks records A lacks.
TEST(ReviewRepro, StaleFollowerLeoAcrossLeaderships_EndToEnd) {
    TempDir db("rv1b"), da("rv1a"), dc("rv1c");
    rune::storage::Log lb(db.path), la(da.path), lc(dc.path);
    InMemoryTransport t;
    constexpr NodeId kB = 1, kA = 2, kC = 3;
    std::vector<NodeId> all = {kB, kA, kC};
    auto cfg = [](NodeId id) { return makeConfig(id, 10'000, 1'000'000, 2); };
    PartitionReplica b(cfg(kB), Role::Leader, 1, all, all, &lb, &t);
    PartitionReplica a(cfg(kA), Role::Follower, 1, all, all, &la, &t);
    PartitionReplica c(cfg(kC), Role::Follower, 1, all, all, &lc, &t);
    a.becomeFollower(1, kB); c.becomeFollower(1, kB);
    t.setLeader(&b); t.addFollower(kA, &a); t.addFollower(kC, &c);
    int64_t clock = 0;
    for (int i = 0; i < 5; ++i) ASSERT_GE(b.append(makePayload(1)), 0);
    ASSERT_TRUE(driveReplicas(t, clock, kDriverCap, [&]{ return a.logEndOffset()==5 && c.logEndOffset()==5 && b.highWatermark()==5; }, b, a, c));
    t.removeFollower(kC);                                  // C stalls at 5
    for (int i = 0; i < 15; ++i) ASSERT_GE(b.append(makePayload(1)), 0);
    ASSERT_TRUE(driveReplicas(t, clock, kDriverCap, [&]{ return a.logEndOffset()==20 && b.followerLeoForTest(kA)==20; }, b, a));
    ASSERT_EQ(b.highWatermark(), 5);
    std::printf("dbg e1: fl(A)=%lld fl(C)=%lld\n",(long long)b.followerLeoForTest(kA),(long long)b.followerLeoForTest(kC));
    // Epoch 2: C leads (in ISR, holds the committed prefix). A and B truncate to 5.
    t.removeFollower(kA);
    c.becomeLeader(2, {kC, kA, kB}); a.becomeFollower(2, kC); b.becomeFollower(2, kC);
    t.setLeader(&c); t.addFollower(kA, &a); t.addFollower(kB, &b);
    for (int i = 0; i < 3; ++i) ASSERT_GE(c.append(makePayload(2)), 0);
    ASSERT_TRUE(driveReplicas(t, clock, kDriverCap, [&]{ return a.logEndOffset()==8 && b.logEndOffset()==8 && c.highWatermark()==8; }, c, a, b))
        << "a=" << a.logEndOffset() << " b=" << b.logEndOffset();
    t.removeFollower(kA);                                  // A slow from here
    for (int i = 0; i < 4; ++i) ASSERT_GE(c.append(makePayload(2)), 0);
    ASSERT_TRUE(driveReplicas(t, clock, kDriverCap, [&]{ return b.logEndOffset()==12; }, c, b));
    // Epoch 3: preferred election back to B (ISR carried over); A not fetching yet.
    t.removeFollower(kB);
    b.becomeLeader(3, {kB, kA, kC}); c.becomeFollower(3, kB);
    t.setLeader(&b); t.addFollower(kC, &c);
    std::printf("dbg e3 start: fl(A)=%lld fl(C)=%lld hw=%lld\n",(long long)b.followerLeoForTest(kA),(long long)b.followerLeoForTest(kC),(long long)b.highWatermark());
    for (int i = 0; i < 3; ++i) ASSERT_GE(b.append(makePayload(3)), 0);   // offsets 12..14
    driveReplicas(t, clock, 20, [&]{ return c.logEndOffset()==15 && b.highWatermark()>=15; }, b, c);
    std::printf("dbg end: fl(A)=%lld fl(C)=%lld\n",(long long)b.followerLeoForTest(kA),(long long)b.followerLeoForTest(kC));
    std::printf("R1b: B.HW=%lld  A.LEO=%lld (A in ISR=%d)  C.LEO=%lld\n", (long long)b.highWatermark(),
                (long long)a.logEndOffset(),
                (int)(std::find(b.isr().begin(), b.isr().end(), kA) != b.isr().end()), (long long)c.logEndOffset());
    EXPECT_LE(b.highWatermark(), a.logEndOffset()) << "committed past an ISR member's actual LEO";
}
