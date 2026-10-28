// tests/support/AllocCounter.h — counts global operator new calls in a window.
#pragma once
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
} // namespace alloc_counter

/// OpenSslAllocCounter.cpp (linked into rune_client_crypto_regression_tests only): true when the CRYPTO_set_mem_functions
/// wrappers were installed before OpenSSL's first allocation.
bool openSslAllocCounterInstalled() noexcept;
