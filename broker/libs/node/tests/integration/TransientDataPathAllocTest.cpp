// TransientDataPathAllocTest.cpp — Phase 2b E2: a steady-state worker pass on a
// TRANSIENT partition — produce intake (the ring wraps and evicts), push reads
// served from the ring — never allocates.  Replaces the global operator new
// (binary-wide), so it lives in its own binary (rune_transient_alloc_regression_tests).
#include "TransientRig.h"

#include <rune/node/RcState.h>

#include <execinfo.h>
#include <cstdio>
#include <cstdlib>
#include <new>

// Heap allocations made while armed; the first few print a backtrace so the
// culprit is named.  The worker is driven by hand on the test thread (no other
// thread runs while armed), so plain globals suffice (no thread_local).
bool     gAllocArmed  = false;
uint64_t gArmedAllocs = 0;
void* operator new(std::size_t n) {
    if (gAllocArmed) [[unlikely]] {
        if (++gArmedAllocs <= 3) {
            gAllocArmed = false;   // backtrace itself may allocate
            void* frames[32];
            const int k = ::backtrace(frames, 32);
            std::fprintf(stderr, "E2 transient hot-path allocation of %zu bytes:\n", n);
            ::backtrace_symbols_fd(frames, k, 2);
            gAllocArmed = true;
        }
    }
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

using namespace transient_rig;

TEST(TransientDataPathRegression, SteadyStateProduceEvictAndPushReadNeverAllocateRegression) {
    Rig rig(/*ringBytes=*/256u << 10, /*maxBatches=*/100'000);
    const auto batch = batchOf(4, 200);
    const auto frame = produceFrame(1, batch, /*acks=*/0);
    int64_t next = 0;
    int64_t sub  = 1;
    auto drainReads = [&] {   // test side, disarmed (the copy-mode drain allocates the chunk)
        rig.pool.drainAllReturns([&](WorkerReturn r) {
            if (r.type == WorkerReturn::Type::PushReadResult && r.newNextOffset > next) next = r.newNextOffset;
        });
    };
    // Warm-up: every container reaches its steady size; the ring wraps and evicts many times.
    for (int i = 0; i < 2048; ++i) {
        rig.pass(&frame);
        if ((i & 3) == 3) { rig.pushRead(sub++, next); rig.pass(); }
        drainReads();
    }
    ASSERT_GT(rig.mem.logStartOffset(), 0) << "precondition: the ring evicts in steady state";
    ASSERT_GT(next, 0) << "precondition: the reads are answered from the ring";
    gArmedAllocs = 0;
    for (int i = 0; i < 512; ++i) {
        if ((i & 3) == 0) rig.pushRead(sub++, next);   // enqueue: test side, disarmed
        rig.now += 1'000'000;
        gAllocArmed = true;
        (void)rig.worker().handleIngressFrameForTest(frame.data(), frame.size(), rig.now);
        (void)rig.worker().doWork(rig.now);
        gAllocArmed = false;
        drainReads();
    }
    EXPECT_EQ(gArmedAllocs, 0u) << "a steady-state transient pass allocated (see the backtraces above)";
}

// E2 Task 12 fix round 2 (review I1): the memory-backed RC rebuild remembers the
// producers with an in-ring marker in a FIXED-capacity O(1) set — never a
// growing vector scanned linearly.  More producers than it holds: the rebuild
// allocates nothing, the ones it could not remember are pinned at the ring start
// (the conservative side of the finding-B ruling) and counted; the others open
// at their own batch.
TEST(TransientDataPathRegression, RingRebuildWithMoreProducersThanTheMarkerSetNeverAllocatesRegression) {
    constexpr int     kProducers = static_cast<int>(rune::node::RcState::kRingMarkedPidsCapacity) + 76;
    constexpr int64_t kPid0      = 1000;
    rune::storage::InMemoryLog mem(16u << 20, 100'000);
    mem.resetTo(10);   // the ring starts above 0: an unmarked producer's first batch is not provably its txn's first
    auto put = [&](const std::vector<uint8_t>& b) {
        int64_t base = -1;
        const int32_t d = rune::protocol::RecordBatchReader(b.data(), b.size()).lastOffsetDelta();
        EXPECT_GE(mem.appendNoSyncRestamp(b.data(), b.size(), d, base), 0);
        return base;
    };
    for (int p = 0; p < kProducers; ++p) {   // every producer: a whole committed txn ...
        put(batchOf(1, 10, kPid0 + p, 0, 0, /*txn=*/true));
        put(rune::transaction::ControlMarker::build(kPid0 + p, 0, rune::transaction::ControlType::Commit, 0));
    }
    std::vector<int64_t> second(kProducers);
    for (int p = 0; p < kProducers; ++p)      // ... then a new, open txn
        second[static_cast<std::size_t>(p)] = put(batchOf(1, 10, kPid0 + p, 0, 1, true));
    const int64_t ringStart = mem.logStartOffset();
    ASSERT_EQ(ringStart, 10);

    rune::node::RcState rc;
    rc.reserve(4 * kProducers);
    rc.reserveRingRebuild();
    const std::size_t setBytes = rc.ringMarkedPids().slotBytes();
    rune::node::RcFacts f;
    f.leader = true; f.epoch = 1;
    gArmedAllocs = 0;
    gAllocArmed  = true;
    // E2 final fix wave (I3): the ring build is budgeted — many advance steps,
    // every one of them allocation-free.
    int steps = 0;
    for (; steps < 4096 && !rc.ready(); ++steps)
        (void)rc.advance(f, nullptr, rune::node::RcState::kDefaultScanBytesPerPass, &mem);
    gAllocArmed  = false;
    ASSERT_TRUE(rc.ready()) << "the budgeted ring build never finished";
    EXPECT_GT(steps, 1) << "precondition: the ring is larger than one step's budget";
    EXPECT_EQ(gArmedAllocs, 0u) << "the ring rebuild allocated (see the backtraces above)";
    EXPECT_EQ(rc.ringMarkedPids().slotBytes(), setBytes) << "the marker set grew";
    EXPECT_EQ(rc.ringMarkedPids().size(), rune::node::RcState::kRingMarkedPidsCapacity);
    EXPECT_EQ(rc.ringMarkedPidsOverflows(), 76u);
    int own = 0, pinned = 0;
    for (int p = 0; p < kProducers; ++p) {
        const int64_t first = rc.tracker().firstOffsetOf(kPid0 + p);
        if (first == second[static_cast<std::size_t>(p)]) ++own;
        else if (first == ringStart) ++pinned;
        else ADD_FAILURE() << "producer " << p << " opened at " << first;
    }
    EXPECT_EQ(own, static_cast<int>(rune::node::RcState::kRingMarkedPidsCapacity))
        << "a remembered producer's txn opens at its own batch";
    EXPECT_EQ(pinned, 76) << "an overflowed producer is pinned at the ring start (conservative)";
    EXPECT_EQ(rc.lso(INT64_MAX), ringStart);
}
