// include/rune/client/transport/FrameCrc.h — CRC-on-accept (backlog B1 / LAT-1 h).
//
// The producer hands the transport a Produce frame whose RecordBatch CRC field is
// NOT yet computed, plus a FrameCrc naming where it goes.  The transport seals it
// only once Aeron has accepted the frame: inside a successful tryClaim (frame <=
// max payload) or, for a frame spanning several MTUs, inside the offer's
// reserved-value supplier — which Aeron calls after its position < limit check
// and before the fragment's commit.  A back-pressured (refused) offer therefore
// never computes a CRC, and the next attempt offers the same unsealed bytes.
//
// Sealing writes the CRC into the producer's source buffer too: that buffer is
// the retained retransmit frame, so later re-offers are already sealed (and an
// identity re-stamp patches the CRC incrementally — detail/Crc32cPatch.h).
#pragma once

#include <rune/protocol/Crc32c.h>
#include <rune/protocol/RecordBatch.h>

#include <cstddef>
#include <cstdint>

namespace rune::client::transport {

struct FrameCrc {
    /// Frame offset of the RecordBatch CRC field (uint32 LE); 0 = nothing to seal.
    uint32_t crcAt = 0;
    /// Frame offset where the CRC-covered region begins (it runs to the frame end).
    uint32_t from = 0;

    [[nodiscard]] constexpr bool pending() const noexcept { return crcAt != 0; }

    /// The FrameCrc of a RecordBatch that starts at frame offset `batchStart`.
    [[nodiscard]] static constexpr FrameCrc forBatchAt(std::size_t batchStart) noexcept {
        return FrameCrc{
            static_cast<uint32_t>(batchStart + ::rune::protocol::batch::kCrcOffset),
            static_cast<uint32_t>(batchStart + ::rune::protocol::batch::kAttributesOffset)};
    }
};

/// The CRC of `frame`'s covered region (nothing written).
[[nodiscard]] inline uint32_t frameCrcValue(const uint8_t* frame, std::size_t len, FrameCrc c) noexcept {
    return ::rune::protocol::crc32c(frame + c.from, len - c.from);
}

/// Writes `crc` (LE) at `dst`.
inline void putCrc(uint8_t* dst, uint32_t crc) noexcept {
    dst[0] = static_cast<uint8_t>(crc);
    dst[1] = static_cast<uint8_t>(crc >> 8);
    dst[2] = static_cast<uint8_t>(crc >> 16);
    dst[3] = static_cast<uint8_t>(crc >> 24);
}

/// Computes and stores the frame's CRC in place (no-op when nothing is pending).
/// For sinks that accept unconditionally (in-process benches/tests) or that
/// cannot seal inside an Aeron claim.  Returns the CRC (0 when not pending).
inline uint32_t sealFrame(uint8_t* frame, std::size_t len, FrameCrc c) noexcept {
    if (!c.pending()) return 0;
    const uint32_t crc = frameCrcValue(frame, len, c);
    putCrc(frame + c.crcAt, crc);
    return crc;
}

}  // namespace rune::client::transport
