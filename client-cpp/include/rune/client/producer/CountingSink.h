// include/rune/client/producer/CountingSink.h — concrete sink for in-process
// benches and tests: counts frames/bytes, accepts (optionally after refusing
// `refusals` offers per accepted frame — models transport back-pressure).
// Single-threaded.
#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include <rune/client/transport/FrameCrc.h>

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
    /// Tests: CRCs this sink sealed on accept; refused offers that arrived with
    /// the CRC still pending; a copy of the last accepted frame.
    uint64_t*             seals            = nullptr;
    uint64_t*             unsealedRefusals = nullptr;
    std::vector<uint8_t>* lastFrame        = nullptr;

    bool operator()(int32_t, int32_t, int32_t, const uint8_t* buf, std::size_t len) const noexcept {
        if (left != nullptr) {
            if (*left > 0) {
                --*left;
                return false;
            }
            *left = refusals;
        }
        if (lastCorr != nullptr && len >= 24) std::memcpy(lastCorr, buf + 16, 8);
        if (lastFrame != nullptr) lastFrame->assign(buf, buf + len);
        ++*frames;
        *bytes += len;
        return true;
    }
    /// Backlog B1: behaves like the Aeron transport — a refused offer computes
    /// nothing; the CRC is sealed (into `buf`) only on accept.
    bool operator()(int32_t t, int32_t p, int32_t s, uint8_t* buf, std::size_t len,
                    transport::FrameCrc crc) const noexcept {
        if (left != nullptr && *left > 0) {
            if (unsealedRefusals != nullptr && crc.pending()) ++*unsealedRefusals;
            --*left;
            return false;
        }
        if (crc.pending()) {
            (void)transport::sealFrame(buf, len, crc);
            if (seals != nullptr) ++*seals;
        }
        return (*this)(t, p, s, static_cast<const uint8_t*>(buf), len);
    }
};
} // namespace rune::client::producer
