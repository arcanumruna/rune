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
} // namespace

void* operator new(std::size_t n) {
    if (g_on.load(std::memory_order_relaxed)) g_count.fetch_add(1, std::memory_order_relaxed);
    if (t_on) ++t_count;
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
    t_on    = true;
}
long stopThisThread() noexcept {
    t_on = false;
    return t_count;
}
} // namespace alloc_counter
