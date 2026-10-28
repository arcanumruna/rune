// tests/support/OpenSslAllocCounter.cpp — C10 (#535): makes OpenSSL's allocations visible to the zero-allocation gates.
//
// AllocCounter.cpp replaces global operator new, but OpenSSL allocates through its own CRYPTO_malloc, which a
// replaced operator new never sees: a gate could pass while every call allocated inside libcrypto.  This file installs
// CRYPTO_set_mem_functions wrappers that count into the SAME window (alloc_counter::noteExternal) and forward to
// malloc/realloc/free.  OpenSSL accepts the replacement only before its first allocation, so the install is a
// namespace-scope static initializer; the gate test ASSERTs openSslAllocCounterInstalled().  Linked ONLY into
// rune_client_crypto_regression_tests (no other binary pays for the wrappers or can have its OpenSSL swapped).
#include "AllocCounter.h"

#include <openssl/crypto.h>

#include <cstdlib>

namespace {
void* countingMalloc(std::size_t n, const char*, int) {
    alloc_counter::noteExternal();
    return std::malloc(n != 0 ? n : 1);
}
void* countingRealloc(void* p, std::size_t n, const char*, int) {
    alloc_counter::noteExternal();
    if (n == 0) {   // #675: libc / OpenSSL semantics - realloc(p, 0) frees p
        std::free(p);
        return nullptr;
    }
    return std::realloc(p, n);
}
void countingFree(void* p, const char*, int) { std::free(p); }

const bool g_installed = CRYPTO_set_mem_functions(&countingMalloc, &countingRealloc, &countingFree) == 1;
}  // namespace

bool openSslAllocCounterInstalled() noexcept { return g_installed; }
