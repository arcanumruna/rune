// HotPathBAllocRegressionTest.cpp — the hot-path B regressions that REPLACE
// the global operator new / operator delete.  D23 (D19-D21 review L2): a
// replacement is binary-wide, so these tests live in their own binary
// (rune_hotpath_b_alloc_regression_tests) — every other hot-path B test runs
// against the stock allocator.
#include "HotPathBRig.h"
#include <execinfo.h>
#include <cstdio>

// The same-address replacement test: the block of one deleted object is kept
// (not freed) so the next object is built at the SAME address (deterministic
// pointer reuse, no allocator luck).  Everything else frees normally.
void* gKeepFreedBlock = nullptr;
bool  gKeptBlockFreed = false;
void operator delete(void* p) noexcept {
    if (p != nullptr && p == gKeepFreedBlock) { gKeptBlockFreed = true; return; }
    std::free(p);
}
void operator delete(void* p, std::size_t) noexcept { ::operator delete(p); }
// D28b: heap allocations made by THIS thread while armed (the worker's duty
// pass); the first few print a backtrace so the culprit is named.
thread_local bool     gAllocArmed = false;
thread_local uint64_t gArmedAllocs = 0;
void* operator new(std::size_t n) {
    if (gAllocArmed) [[unlikely]] {
        if (++gArmedAllocs <= 3) {
            gAllocArmed = false;   // backtrace itself may allocate
            void* frames[32];
            const int k = ::backtrace(frames, 32);
            std::fprintf(stderr, "D28b hot-path allocation of %zu bytes:\n", n);
            ::backtrace_symbols_fd(frames, k, 2);
            gAllocArmed = true;
        }
    }
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}


// Same, memory-backed replicas: the route held the replica's raw pointer and
// ownership was checked by pointer — a replacement replica allocated at the
// SAME address passed as the owner of the old one's completion.
TEST(HotPathBRegression, StaleMemoryFenceCompletionNeverConfirmsAReplicaAtTheSameAddressRegression) {
    TmpDir d;
    rune::replication::NullPartitionTransport transport;
    rune::node::ReplicaRegistry reg;
    WorkerPool pool{1, rune::metadata::ShardRouter(1)};
    rune::storage::InMemoryLog logA(1 << 20, 10'000), logB(1 << 20, 10'000);
    PartitionConfig c;
    c.topicId = 1; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
    c.replicaLagMaxOffset = 1'000'000'000;
    auto make = [&](void* mem, rune::storage::InMemoryLog& log, const fs::path& stateDir) {
        std::unique_ptr<PartitionReplica> r(new (mem) PartitionReplica(c, Role::Follower, 1, std::vector<int32_t>{1},
                                                                       std::vector<int32_t>{1}, &log, &transport));
        r->setExternalFollowerFlush(true);
        r->setIsrChannel(&transport);
        r->setMemoryEpochStateDir(stateDir.string(), (stateDir.string() + ".fence"));
        return r;
    };
    void* memA = ::operator new(sizeof(PartitionReplica));
    gKeepFreedBlock = memA;   // A's block survives A's delete: B is built at the SAME address
    reg.add(1, 0, make(memA, logA, d.p / "a.state"));
    pool.setRegistry(&reg);
    Worker& w = pool.worker(0);
    w.setWorkerIngress(nullptr, /*active=*/true);
    w.setShardMode(true);
    pool.assignFromRegistry(reg);
    int64_t now = 1'000'000'000;
    auto pass = [&] { now += 1'000'000; (void)w.doWork(now); };
    auto lead = [&] {
        WorkerOp op;
        op.type = WorkerOp::Type::BecomeLeader;
        op.topicId = 1; op.partition = 0; op.roleEpoch = 3; op.isr = {1};
        ASSERT_TRUE(pool.tryEnqueueOp(1, 0, std::move(op)));
    };
    pass();
    w.setReapPausedForTest(true);
    lead();
    pass();   // A submits its fence write; the completion is held
    const auto tA = w.lastEpochStateTokenForTest();
    reg.remove(1, 0);   // A retired, dropped by the worker, then freed (every worker acked)
    pool.assignFromRegistry(reg);
    pass();
    ASSERT_EQ(reg.collectRetired(UINT64_MAX), 1u);
    // B allocated at the same address (fresh state dir: the same write seq).
    ASSERT_TRUE(gKeptBlockFreed) << "precondition: A was deleted";
    gKeepFreedBlock = nullptr;   // B's own delete frees normally
    void* memB = memA;
    reg.add(1, 0, make(memB, logB, d.p / "b.state"));
    auto* B = static_cast<PartitionReplica*>(memB);
    pool.assignFromRegistry(reg);
    lead();
    pass();   // B submits its own fence write (held)
    ASSERT_TRUE(B->leadershipPendingForTest());
    ASSERT_NE(w.lastEpochStateTokenForTest(), tA);
    w.deliverCompletionForTest({tA, 0, 0});   // A's stale completion
    EXPECT_TRUE(B->leadershipPendingForTest()) << "A's fsync confirmed B's fence";
    w.setReapPausedForTest(false);
    const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (B->role() != Role::Leader && std::chrono::steady_clock::now() < end) { pass(); std::this_thread::yield(); }
    EXPECT_EQ(B->role(), Role::Leader) << "B leads on its OWN completion";
}

// D28b (box perf capture 2026-10-08: 29 worker brk waits on mmap_lock): a
// steady-state duty pass — produce intake, leader push to an in-sync follower,
// follower fetches — never allocates on the worker (heap growth = brk / mmap =
// mmap_lock, and allocator latency, on the hot path).
TEST(HotPathBRegression, SteadyStateDutyPassesNeverAllocateRegression) {
    Rig rig(false, 64 * 1024 * 1024, nullptr, false, /*withFollower=*/true);
    const auto frame = produceFrame(1, 1, 0, 1000, 4);
    auto armedPass = [&](bool produce) {
        rig.now += 1'000'000;
        gAllocArmed = true;
        if (produce) (void)rig.worker().handleIngressFrameForTest(frame.data(), frame.size(), rig.now);
        (void)rig.worker().doWork(rig.now);
        gAllocArmed = false;
    };
    // Warm-up: every container reaches its steady size.
    for (int i = 0; i < 64; ++i) rig.pass(&frame);
    for (int i = 0; i < 256; ++i) {
        rig.fetch(rig.log->logEndOffset());
        rig.pass(&frame);
        rig.pool.drainAllReturns([](WorkerReturn) {});
    }
    gArmedAllocs = 0;
    for (int i = 0; i < 512; ++i) {
        if ((i & 3) == 0) rig.fetch(rig.log->logEndOffset());   // enqueue: test side, disarmed
        armedPass(true);
        rig.pool.drainAllReturns([](WorkerReturn) {});
    }
    EXPECT_EQ(gArmedAllocs, 0u) << "the worker's steady-state duty pass allocated (see the D28b backtraces above)";
}
