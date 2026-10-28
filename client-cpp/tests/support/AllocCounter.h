// tests/support/AllocCounter.h — counts global operator new calls in a window.
#pragma once
#include <cstddef>
namespace alloc_counter {
void start() noexcept;
long stop() noexcept;
/// Count only the calling thread's allocations (other threads — runners, a
/// fake broker — are not in the window).
void startThisThread() noexcept;
long stopThisThread() noexcept;
/// C10 (#535): an allocation made OUTSIDE operator new (OpenSSL's CRYPTO_malloc, via
/// OpenSslAllocCounter.cpp) counts into the same windows; externalThisThread() is how many of the calling
/// thread's counted allocations were such external ones (so a failure can say which counter fired).
void  noteExternal() noexcept;
long  externalThisThread() noexcept;
/// C10 review: allocation-failure injection.  The n-th operator new on the calling thread from now (n >= 1) throws
/// std::bad_alloc (once); 0 disarms.  injectedFailureFiredThisThread() says whether the armed failure fired, so a
/// test proves it reached the branch it targets.
void failNthAllocationThisThread(long n) noexcept;
bool injectedFailureFiredThisThread() noexcept;
/// C6 review (#257 / #305): freed-memory scan.  While armed on the calling thread, every block released through the
/// replaced operator delete is searched (its whole usable size) for `needle` before it is freed;
/// freedHitsThisThread() is how many blocks still held it - an unwiped plaintext copy.  A null needle disarms.
/// While armed, every new block starts zeroed, so stale bytes of a reused chunk never count.
/// `needle` must outlive the window (a string literal).
void scanFreedForThisThread(const char* needle, std::size_t len) noexcept;
long freedHitsThisThread() noexcept;
} // namespace alloc_counter

/// OpenSslAllocCounter.cpp (linked into rune_client_crypto_regression_tests only): true when the CRYPTO_set_mem_functions
/// wrappers were installed before OpenSSL's first allocation.
bool openSslAllocCounterInstalled() noexcept;
