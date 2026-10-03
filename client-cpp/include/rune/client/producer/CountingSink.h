// include/rune/client/producer/CountingSink.h — concrete sink for in-process
// benches and tests: counts frames/bytes, accepts (optionally after refusing
// `refusals` offers per accepted frame — models transport back-pressure).
// Single-threaded.
#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace rune::client::producer {
struct CountingSink {
    uint64_t* frames = nullptr;
    uint64_t* bytes  = nullptr;
    /// Back-pressure model (benches): refuse `refusals` offers before each
    /// accepted one.  `left` counts down the current run; nullptr = never refuse.
    int       refusals = 0;
    int*      left     = nullptr;
    /// When set, the accepted frame's correlation id (MessageHeader int64 at
    /// byte 16) is stored here (benches ack the batch with it).
    int64_t*  lastCorr = nullptr;

    bool operator()(int32_t, int32_t, int32_t, const uint8_t* buf, std::size_t len) const noexcept {
        if (left != nullptr) {
            if (*left > 0) {
                --*left;
                return false;
            }
            *left = refusals;
        }
        if (lastCorr != nullptr && len >= 24) std::memcpy(lastCorr, buf + 16, 8);
        ++*frames;
        *bytes += len;
        return true;
    }
};
} // namespace rune::client::producer
