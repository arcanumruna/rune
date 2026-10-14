#pragma once
// RingAtomics — the ring control words (record length, head, head cache, tail) as
// C++ atomics, for the in-tree M2O / O2O rings and the A9 doorbell watch only.
//
// Why (sanitizer triage 2026-10-14, cluster T1): the rings used Aeron's
// AtomicBuffer::getInt32Volatile / putInt32Ordered / getInt64Volatile /
// putInt64Ordered.  Those are a PLAIN volatile load/store plus a standalone
// std::atomic_thread_fence.  ThreadSanitizer models neither volatile nor
// standalone fences, so every empty poll (consumer loads the length word, sees 0)
// followed by a producer's claim (stores -length into the same word) was a
// "data race" — there is no happens-before edge between those two accesses, and
// none is needed: in the C++ model both are atomic accesses.  RUNE_TSAN_ACQUIRE /
// RELEASE cannot help there (they order the PAYLOAD after an observed store; they
// cannot make two unordered accesses to the control word itself atomic), and the
// tsan-aeron.supp function-name entries stopped matching once -O1 inlined the
// accessors into rune frames.  So the control words are now real atomics.
//
// Cost: identical machine code on x86-64 (acquire load = mov, release store = mov,
// CAS = lock cmpxchg — exactly what Aeron's Atomic64_gcc_x86_64.h emits); on
// AArch64 ldar/stlr instead of ldr+dmb / dmb+str (equal or cheaper).  No allocation,
// no virtual call, header-only, inlined.  Payload bytes stay plain (published by
// the release store of the length word, consumed after its acquire load).
//
// Alignment: every word sits at an 8-aligned record offset or in the cache-line
// aligned trailer of a buffer that is at least 16-aligned (operator new /
// alignas(64) storage), which satisfies std::atomic_ref's required_alignment.

#include <atomic>
#include <cstdint>

#include <concurrent/AtomicBuffer.h>
#include <util/Index.h>

namespace rune::consensus::detail::ring_atomic {

static_assert(std::atomic_ref<std::int32_t>::is_always_lock_free);
static_assert(std::atomic_ref<std::int64_t>::is_always_lock_free);

template <typename T>
[[nodiscard]] inline std::atomic_ref<T> word(const aeron::concurrent::AtomicBuffer& b,
                                              aeron::util::index_t offset) noexcept {
    return std::atomic_ref<T>(*reinterpret_cast<T*>(b.buffer() + offset));
}

[[nodiscard]] inline std::int32_t getInt32Acquire(const aeron::concurrent::AtomicBuffer& b,
                                                aeron::util::index_t offset) noexcept {
    return word<std::int32_t>(b, offset).load(std::memory_order_acquire);
}
inline void putInt32Release(const aeron::concurrent::AtomicBuffer& b, aeron::util::index_t offset,
                           std::int32_t v) noexcept {
    word<std::int32_t>(b, offset).store(v, std::memory_order_release);
}
[[nodiscard]] inline std::int64_t getInt64Acquire(const aeron::concurrent::AtomicBuffer& b,
                                                aeron::util::index_t offset) noexcept {
    return word<std::int64_t>(b, offset).load(std::memory_order_acquire);
}
inline void putInt64Release(const aeron::concurrent::AtomicBuffer& b, aeron::util::index_t offset,
                           std::int64_t v) noexcept {
    word<std::int64_t>(b, offset).store(v, std::memory_order_release);
}
/// Multi-producer tail claim (Aeron compareAndSetInt64 = __sync CAS, seq_cst — kept).
[[nodiscard]] inline bool compareAndSetInt64(const aeron::concurrent::AtomicBuffer& b, aeron::util::index_t offset,
                                   std::int64_t expected, std::int64_t desired) noexcept {
    return word<std::int64_t>(b, offset).compare_exchange_strong(expected, desired,
                                                                 std::memory_order_seq_cst);
}

} // namespace rune::consensus::detail::ring_atomic
