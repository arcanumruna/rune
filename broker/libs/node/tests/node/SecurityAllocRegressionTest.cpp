// SecurityAllocRegressionTest.cpp — authentication Task 9: the admin thread's __security steady state never
// allocates (hard rule 4).  Replaces the global operator new (binary-wide), so it lives in its own binary
// (rune_security_alloc_regression_tests) — the CatalogAllocRegressionTest (F12) pattern.
#include "support/SecurityRig.h"

#include <gtest/gtest.h>

#include <execinfo.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>

namespace {
bool     gArmed       = false;   // single-threaded test binary: plain globals, no atomics
uint64_t gArmedAllocs = 0;

void report(std::size_t n) {
    if (++gArmedAllocs <= 3) {      // name the culprit
        gArmed = false;             // backtrace itself may allocate
        void* frames[32];
        const int k = ::backtrace(frames, 32);
        std::fprintf(stderr, "security alloc gate: allocation of %zu bytes:\n", n);
        ::backtrace_symbols_fd(frames, k, 2);
        gArmed = true;
    }
}
}  // namespace
void* operator new(std::size_t n) {
    if (gArmed) [[unlikely]] report(n);
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t n) {
    if (gArmed) [[unlikely]] report(n);
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

using rune::node::test::SecurityRig;

// The steady idle pass: one compare against the HW, no read, no allocation.
TEST(SecurityAllocRegression, IdleApplierPassAllocatesNothing) {
    SecurityRig rig("alloc_idle");
    rig.appendEvents(0, 16);
    rig.applier.applyCommitted(rig.replica());
    ASSERT_EQ(rig.applier.nextOffset(), 16);
    gArmedAllocs = 0;
    gArmed = true;
    for (int i = 0; i < 1000; ++i) (void)rig.applier.applyCommitted(rig.replica());
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u);
}

// A halted applier is idle too: it answers 0 without touching the log or allocating.
TEST(SecurityAllocRegression, HaltedApplierAllocatesNothing) {
    SecurityRig rig("alloc_halt");
    rig.appendEvents(0, 3);
    rig.appendTampered(3);
    rig.applier.applyCommitted(rig.replica());
    ASSERT_TRUE(rig.applier.halted());
    gArmedAllocs = 0;
    gArmed = true;
    for (int i = 0; i < 1000; ++i) (void)rig.applier.applyCommitted(rig.replica());
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u);
}
