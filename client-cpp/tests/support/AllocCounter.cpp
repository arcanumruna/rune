// tests/support/AllocCounter.cpp — replacement global new/delete for the
// rune_invoker_tests binary.  Counting is off except between start()/stop().
#include "AllocCounter.h"

#include <atomic>
#include <cstdlib>
#include <new>

namespace {
std::atomic<bool> g_on{false};
std::atomic<long> g_count{0};
thread_local bool t_on    = false;  // test instrumentation (startThisThread)
thread_local long t_count = 0;
thread_local long t_ext   = 0;  // the part of t_count that came from noteExternal()
thread_local long t_failIn = 0;      // failNthAllocationThisThread: 0 = disarmed
thread_local bool t_failFired = false;
/// True when this allocation is the injected failure (counts down the armed n).
bool injectFailure() noexcept {
    if (t_failIn <= 0 || --t_failIn != 0) return false;
    t_failFired = true;
    return true;
}
} // namespace

void* operator new(std::size_t n) {
    if (g_on.load(std::memory_order_relaxed)) g_count.fetch_add(1, std::memory_order_relaxed);
    if (t_on) ++t_count;
    if (injectFailure()) throw std::bad_alloc{};
    if (void* p = std::malloc(n != 0 ? n : 1)) return p;
    throw std::bad_alloc{};
}
void* operator new[](std::size_t n) { return ::operator new(n); }
// Over-aligned types (alignas > __STDCPP_DEFAULT_NEW_ALIGNMENT__) go through the
// align_val_t overloads, which the standard library does NOT route through the
// plain operator new above — so they are counted here too, or every zero-alloc
// gate is blind to them.  The nothrow forms forward to these in libc++/libstdc++.
void* operator new(std::size_t n, std::align_val_t al) {
    if (g_on.load(std::memory_order_relaxed)) g_count.fetch_add(1, std::memory_order_relaxed);
    if (t_on) ++t_count;
    if (injectFailure()) throw std::bad_alloc{};
    const std::size_t a = static_cast<std::size_t>(al);
    const std::size_t sz = ((n != 0 ? n : 1) + a - 1) / a * a;  // aligned_alloc: size % align == 0
    if (void* p = std::aligned_alloc(a, sz)) return p;
    throw std::bad_alloc{};
}
void* operator new[](std::size_t n, std::align_val_t al) { return ::operator new(n, al); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
// The nothrow forms route through the replaced throwing forms. Otherwise a nothrow allocation (libstdc++'s
// get_temporary_buffer under std::stable_sort, e.g. in gtest) comes from the default/ASan allocator but is released
// by the replaced delete: ASan alloc-dealloc-mismatch, and the allocation goes uncounted.
void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
    try { return ::operator new(n); } catch (...) { return nullptr; }
}
void* operator new[](std::size_t n, const std::nothrow_t&) noexcept {
    try { return ::operator new[](n); } catch (...) { return nullptr; }
}
void operator delete(void* p, const std::nothrow_t&) noexcept { ::operator delete(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { ::operator delete[](p); }

namespace alloc_counter {
void start() noexcept {
    g_count.store(0, std::memory_order_relaxed);
    g_on.store(true, std::memory_order_seq_cst);
}
long stop() noexcept {
    g_on.store(false, std::memory_order_seq_cst);
    return g_count.load(std::memory_order_relaxed);
}
void startThisThread() noexcept {
    t_count = 0;
    t_ext   = 0;
    t_on    = true;
}
long stopThisThread() noexcept {
    t_on = false;
    return t_count;
}
void noteExternal() noexcept {
    if (g_on.load(std::memory_order_relaxed)) g_count.fetch_add(1, std::memory_order_relaxed);
    if (t_on) {
        ++t_count;
        ++t_ext;
    }
}
long externalThisThread() noexcept { return t_ext; }
void failNthAllocationThisThread(long n) noexcept {
    t_failIn    = n > 0 ? n : 0;
    t_failFired = false;
}
bool injectedFailureFiredThisThread() noexcept { return t_failFired; }
} // namespace alloc_counter
