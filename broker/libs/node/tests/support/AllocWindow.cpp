// AllocWindow.cpp — the counting global operator new behind rune::testing::AllocWindow (test support only).
#include "AllocWindow.h"

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <functional>
#include <new>
#include <thread>

namespace {
// The window's owner thread (hash of its id + 1; 0 = no open window).  Only the owner increments the count, so
// the atomics are for the other threads' reads of the owner word, not for coordination.
std::atomic<std::size_t> gOwner{0};
std::atomic<uint64_t>    gCount{0};

inline std::size_t selfKey() noexcept { return std::hash<std::thread::id>{}(std::this_thread::get_id()) + 1; }
inline void        note() noexcept {
    const std::size_t owner = gOwner.load(std::memory_order_relaxed);
    if (owner != 0 && owner == selfKey()) gCount.fetch_add(1, std::memory_order_relaxed);
}
void* allocate(std::size_t n) {
    note();
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void* allocateAligned(std::size_t n, std::size_t a) {
    note();
    const std::size_t sz = ((n ? n : 1) + a - 1) / a * a;
    if (void* p = std::aligned_alloc(a, sz)) return p;
    throw std::bad_alloc();
}
}  // namespace

namespace rune::testing {
AllocWindow::AllocWindow() noexcept {
    assert(gOwner.load() == 0 && "AllocWindow does not nest");
    gCount.store(0);
    gOwner.store(selfKey());
}
AllocWindow::~AllocWindow() { gOwner.store(0); }
uint64_t AllocWindow::count() const noexcept { return gCount.load(std::memory_order_relaxed); }
}  // namespace rune::testing

void* operator new(std::size_t n) { return allocate(n); }
void* operator new[](std::size_t n) { return allocate(n); }
void* operator new(std::size_t n, std::align_val_t a) { return allocateAligned(n, static_cast<std::size_t>(a)); }
void* operator new[](std::size_t n, std::align_val_t a) { return allocateAligned(n, static_cast<std::size_t>(a)); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
