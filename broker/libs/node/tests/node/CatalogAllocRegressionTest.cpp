// CatalogAllocRegressionTest.cpp — catalog history: the admin thread's catalog steady
// state never allocates.  Replaces the global operator new (binary-wide), so it lives in
// its own binary (rune_catalog_alloc_regression_tests).  Tasks 12 and 23 append here.
#include "support/CatalogRig.h"
#include <gtest/gtest.h>
#include <cstdlib>
#include <new>

namespace {
bool     gArmed       = false;   // single-threaded test binary: plain globals, no atomics
uint64_t gArmedAllocs = 0;
}
void* operator new(std::size_t n) {
    if (gArmed) ++gArmedAllocs;
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t n) {
    if (gArmed) ++gArmedAllocs;
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

using rune::node::test::CatalogRig;

TEST(CatalogAllocRegression, SteadyIdleApplyAllocatesNothingRegression) {
    CatalogRig rig("ap_alloc");
    rig.appendEvents(0, 18);
    rig.applier.applyCommitted(rig.replica());
    gArmedAllocs = 0;
    gArmed = true;
    for (int i = 0; i < 1000; ++i) (void)rig.applier.applyCommitted(rig.replica());
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u);
}

// A halted applier is idle too: it answers 0 without touching the log or allocating.
TEST(CatalogAllocRegression, HaltedApplierAllocatesNothingRegression) {
    CatalogRig rig("ap_alloc_halt");
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
