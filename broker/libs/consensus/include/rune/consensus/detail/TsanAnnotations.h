#pragma once
// TSan happens-before annotations for ring-buffer internals (F16).
//
// The rings publish records and free space with Aeron's ordered stores /
// volatile loads (AtomicBuffer::putInt32Ordered / getInt32Volatile, implemented
// with inline-asm fences on x86).  ThreadSanitizer cannot see those fences, so
// every payload handed through a ring looked like a data race, burying real
// races in noise.  These annotations state the edges the fences already
// guarantee: release on the record (or head) before the ordered store, acquire
// after the volatile load observes it.  Ring internals only; they compile to
// nothing outside TSan builds (zero hot-path cost).

#if defined(__SANITIZE_THREAD__)
#  define RUNE_TSAN_ENABLED 1
#elif defined(__has_feature)
#  if __has_feature(thread_sanitizer)
#    define RUNE_TSAN_ENABLED 1
#  endif
#endif

#if defined(RUNE_TSAN_ENABLED)
extern "C" void __tsan_acquire(void* addr);
extern "C" void __tsan_release(void* addr);
#  define RUNE_TSAN_ACQUIRE(addr) __tsan_acquire(static_cast<void*>(addr))
#  define RUNE_TSAN_RELEASE(addr) __tsan_release(static_cast<void*>(addr))
#else
#  define RUNE_TSAN_ACQUIRE(addr) ((void)0)
#  define RUNE_TSAN_RELEASE(addr) ((void)0)
#endif
