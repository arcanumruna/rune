// include/rune/client/producer/CountingSink.h — concrete sink for in-process
// benches and tests: counts frames/bytes, always accepts.  Single-threaded.
#pragma once
#include <cstddef>
#include <cstdint>

namespace rune::client::producer {
struct CountingSink {
    uint64_t* frames = nullptr;
    uint64_t* bytes  = nullptr;
    bool operator()(int32_t, int32_t, int32_t, const uint8_t*, std::size_t len) const noexcept {
        ++*frames;
        *bytes += len;
        return true;
    }
};
} // namespace rune::client::producer
