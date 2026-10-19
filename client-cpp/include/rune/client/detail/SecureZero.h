#pragma once
// Zeroing the optimiser may not elide (password / key material is wiped after use).
#include <cstddef>

namespace rune::client::detail {
inline void secureZero(void* p, std::size_t n) noexcept {
    volatile unsigned char* v = static_cast<volatile unsigned char*>(p);
    while (n-- != 0) *v++ = 0;
}
} // namespace rune::client::detail
