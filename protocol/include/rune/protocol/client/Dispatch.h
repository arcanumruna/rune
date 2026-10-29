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
//   4. UnknownTemplate   — templateId not in the known set (1..7 MA; 8-9 MD1; 16..21 MB; 32..44 MC; 12..15 auth; 64..83)
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
// isKnownTemplate — true for templateIds handled in Milestone A (1..7),
//                   MD1 Task 10 Metadata (8..9),
//                   Milestone B produce/subscribe range (16..21),
//                   Milestone C consumer-group range (32..44), and
//                   Milestone C-B txn range (45..54).
//
// Reserved ranges (from Protocol.h):
//   control         1-15   (Connect=1 .. Pong=7; MetadataRequest=8, MetadataResponse=9; handshake 10-11;
//                            auth 12-15 — all assigned)
//   produce/consume 16-31  (Produce=16 .. Unsubscribe=21 assigned; 22-31 gap → UnknownTemplate)
//   groups/txn      32-63  (JoinGroup=32 .. Assign=44; InitProducerId=45..EndTxnResponse=54)
//   view            64-95  (DefineTopic=64..TopicSchemaResponse=67; AlterTopic=68, AlterTopicResponse=69;
//                            CreateView=70, CreateViewResponse=71, GetViewRow=72, ViewRowResponse=73;
//                            catalog history: DropTopic=74, AlterTopicConfig=75, DropView=76,
//                            AlterView=77, DdlResponse=78; SecurityDdl=79, SecurityBootstrap=80; 81-95 gap → UnknownTemplate)
//
// Extend this function when future tickets add template ids.
// ---------------------------------------------------------------------------
[[nodiscard]] constexpr bool isKnownTemplate(uint16_t tid) noexcept {
    // Milestone A control range: 1..7
    if (tid >= static_cast<uint16_t>(TemplateId::Connect) &&
        tid <= static_cast<uint16_t>(TemplateId::Pong)) {
        return true;
    }
    // MD1 Task 10: Metadata request/response: 8..9
    if (tid == static_cast<uint16_t>(TemplateId::MetadataRequest) ||
        tid == static_cast<uint16_t>(TemplateId::MetadataResponse)) {
        return true;
    }
    // client<->broker TLS/AEAD (Task 4/5): HandshakeContinue=10, HandshakeContinueResponse=11.
    // Added here because Task 4 defined these ids in Protocol.h but didn't extend
    // isKnownTemplate() for them — found while wiring the Task 5 client-side drive
    // (ClientAgent::dispatchEgressFrame's checkFrame() silently dropped every
    // HandshakeContinueResponse as UnknownTemplate without this).
    if (tid == static_cast<uint16_t>(TemplateId::HandshakeContinue) ||
        tid == static_cast<uint16_t>(TemplateId::HandshakeContinueResponse)) {
        return true;
    }
    // authentication (client version 9): AuthClientFirst..AuthServerFinal = 12..15
    if (tid >= static_cast<uint16_t>(TemplateId::AuthClientFirst) &&
        tid <= static_cast<uint16_t>(TemplateId::AuthServerFinal)) {
        return true;
    }
    // Milestone B produce/subscribe range: 16..21
    if (tid >= static_cast<uint16_t>(TemplateId::Produce) &&
        tid <= static_cast<uint16_t>(TemplateId::Unsubscribe)) {
        return true;
    }
    // Milestone C consumer-group range: 32..44
    if (tid >= static_cast<uint16_t>(TemplateId::JoinGroup) &&
        tid <= static_cast<uint16_t>(TemplateId::Assign)) {
        return true;
    }
    // Milestone C-B txn range: 45..54
    if (tid >= static_cast<uint16_t>(TemplateId::InitProducerId) &&
        tid <= static_cast<uint16_t>(TemplateId::EndTxnResponse)) {
        return true;
    }
    // 0b/0c schema + view-engine range + catalog-history DDL + security DDL + namespaces: 64..85 (...NamespaceDdl=81, ListNamespaces=82,
    // ListNamespacesResponse=83, NamespaceUsage=84, NamespaceUsageResponse=85; 86-95 gap -> UnknownTemplate)
    if (tid >= static_cast<uint16_t>(TemplateId::DefineTopic) &&
        tid <= static_cast<uint16_t>(TemplateId::NamespaceUsageResponse)) {
        return true;
    }
    // NAT-safe egress (client version 16): WorkerBind = 86 (88-95 gap -> UnknownTemplate).
    // WorkerBindAck = 87 is broker -> client only: it stays UNKNOWN here (ingress), the client recognizes it by id.
    if (tid == static_cast<uint16_t>(TemplateId::WorkerBind)) return true;
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
