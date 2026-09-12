#pragma once
// Dispatch.h — safe frame-dispatch primitive for the Rune client protocol.
//
// checkFrame(buf, avail) inspects the raw bytes of an incoming frame and
// returns a FrameCheck describing its validity.  The function is:
//   - read-only  (const uint8_t*)
//   - bounds-safe (never reads past avail)
//   - zero-alloc
//   - noexcept
//   - forward-compatible: unknown template ids / newer-than-us versions are
//     cleanly rejected rather than mis-parsed or crashed on.
//
// Ordering guarantee (checked in this order):
//   1. TooShort          — avail < MessageHeader::ENCODED_SIZE
//   2. WrongSchema       — header.schemaId != kSchemaId
//   3. UnsupportedVersion— header.version  >  kProtocolVersion
//   4. UnknownTemplate   — templateId not in the known set (1..7 MA; 16..17 MB)
//   5. Ok
//
// Future tickets extend isKnownTemplate() as new template ids are assigned
// from the reserved ranges documented in Protocol.h.

#include <cstddef>
#include <cstdint>
#include <optional>

#include "MessageHeader.h"
#include "Protocol.h"

namespace rune::protocol::client {

// ---------------------------------------------------------------------------
// FrameStatus — result of checkFrame
// ---------------------------------------------------------------------------
enum class FrameStatus : uint8_t {
    Ok = 0,
    TooShort,           // avail < MessageHeader::ENCODED_SIZE
    WrongSchema,        // header.schemaId != kSchemaId
    UnsupportedVersion, // header.version > kProtocolVersion (newer than we speak)
    UnknownTemplate,    // templateId not a known/handled id
};

// ---------------------------------------------------------------------------
// FrameCheck — returned by checkFrame
// ---------------------------------------------------------------------------
struct FrameCheck {
    FrameStatus status    = FrameStatus::TooShort;
    uint16_t    templateId = 0;   // valid only when status == Ok
    uint16_t    version    = 0;   // decoded version (for diagnostics, always set if not TooShort)
};

// ---------------------------------------------------------------------------
// isKnownTemplate — true for templateIds handled in Milestone A (1..7)
//                   and Milestone B produce/consume range (16..17).
//
// Reserved ranges (from Protocol.h):
//   control       1-15   (Connect=1 .. Pong=7 assigned so far)
//   produce/consume 16-31 (Produce=16, ProduceResponse=17 added in PB-1)
//   groups/txn    32-63
//   view          64-95
//
// Extend this function when future tickets add template ids.
// ---------------------------------------------------------------------------
[[nodiscard]] constexpr bool isKnownTemplate(uint16_t tid) noexcept {
    // Milestone A control range: 1..7
    if (tid >= static_cast<uint16_t>(TemplateId::Connect) &&
        tid <= static_cast<uint16_t>(TemplateId::Pong)) {
        return true;
    }
    // Milestone B produce/consume: 16..17
    if (tid >= static_cast<uint16_t>(TemplateId::Produce) &&
        tid <= static_cast<uint16_t>(TemplateId::ProduceResponse)) {
        return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// checkFrame — validate and classify an incoming raw frame.
//
// Never reads past buf[avail-1].  Safe to call on any buffer including
// exact-size allocations (ASAN-friendly).
// ---------------------------------------------------------------------------
[[nodiscard]] inline FrameCheck checkFrame(const uint8_t* buf, std::size_t avail) noexcept {
    // Step 1: length check — must have at least one full header before any field reads.
    if (avail < MessageHeader::ENCODED_SIZE) {
        return FrameCheck{FrameStatus::TooShort, 0, 0};
    }

    // Step 2: decode the header (guaranteed to succeed — avail >= ENCODED_SIZE).
    // MessageHeader::decode returns nullopt only when avail < 24, already guarded above.
    const auto hdrOpt = MessageHeader::decode(buf, avail);
    // hdrOpt is always valid here; treat the rare impossible branch defensively.
    if (!hdrOpt) [[unlikely]] {
        return FrameCheck{FrameStatus::TooShort, 0, 0};
    }
    const MessageHeader& hdr = *hdrOpt;

    // Step 3: schema check.
    if (hdr.schemaId != kSchemaId) {
        return FrameCheck{FrameStatus::WrongSchema, hdr.templateId, hdr.version};
    }

    // Step 4: version check — accept equal or older, reject strictly newer.
    if (hdr.version > kProtocolVersion) {
        return FrameCheck{FrameStatus::UnsupportedVersion, hdr.templateId, hdr.version};
    }

    // Step 5: template id check.
    if (!isKnownTemplate(hdr.templateId)) {
        return FrameCheck{FrameStatus::UnknownTemplate, hdr.templateId, hdr.version};
    }

    // All checks passed.
    return FrameCheck{FrameStatus::Ok, hdr.templateId, hdr.version};
}

// ---------------------------------------------------------------------------
// peekTemplateId — convenience wrapper.
//
// Returns the templateId iff checkFrame returns Ok, else nullopt.
// ---------------------------------------------------------------------------
[[nodiscard]] inline std::optional<uint16_t> peekTemplateId(const uint8_t* buf,
                                                             std::size_t    avail) noexcept {
    const FrameCheck fc = checkFrame(buf, avail);
    if (fc.status == FrameStatus::Ok) {
        return fc.templateId;
    }
    return std::nullopt;
}

} // namespace rune::protocol::client
