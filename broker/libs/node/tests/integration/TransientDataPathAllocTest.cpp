// TransientDataPathAllocTest.cpp — Phase 2b E2: a steady-state worker pass on a
// TRANSIENT partition — produce intake (the ring wraps and evicts), push reads
// served from the ring — never allocates.  Replaces the global operator new
// (binary-wide), so it lives in its own binary (rune_transient_alloc_regression_tests).
#include "TransientRig.h"

#include <execinfo.h>
#include <cstdio>
#include <cstdlib>
#include <new>

// Heap allocations made by THIS thread while armed; the first few print a
// backtrace so the culprit is named.
thread_local bool     gAllocArmed  = false;
thread_local uint64_t gArmedAllocs = 0;
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
