// include/rune/client/agent/AgentRingMessages.h
//
// Internal (app→runner) ClientAgent command-ring messages, encoded in the
// project's SBE style: a MessageHeader followed by a fixed little-endian body.
//
// These messages NEVER reach the wire — they travel only over the ClientAgent
// M2O command ring (app threads → the single runner thread).  They reuse the
// client MessageHeader + detail LE helpers for consistency with the wire codecs,
// but carry a DISTINCT internal schema id so they can never be confused with a
// client-facing frame (schema 120).
//
// Rationale (shared-nothing / event-loop): every app→runner event is a message
// on the ring; the runner reacts to it in its duty cycle.  No shared mutable
// state, no locks — the payload is self-describing and versioned like every
// other ring message in the project.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>

#include <rune/protocol/client/MessageHeader.h>

#include <rune/client/detail/HubCommand.h>

namespace rune::client::agent::ring_msg {

namespace pc = ::rune::protocol::client;

/// Distinct internal schema id (client-facing wire schema is 120).
inline constexpr uint16_t kInternalSchemaId = 121;
inline constexpr uint16_t kInternalVersion  = 1;

// ---------------------------------------------------------------------------
// RegisterProducer — app thread hands a producer to the runner for registration.
//
// Frame layout (all little-endian):
//   [0..23]  MessageHeader (blockLength=8, templateId=1, schemaId=121)
//   [24..31] handle : uint64
//
// `handle` is the address of a heap-allocated std::shared_ptr<ProducerImpl>
// created by Client::producer().  The runner decodes this message, adopts the
// shared_ptr into the single-owner ProducerHub list, and frees the holder.  The
// value is a process-local pointer (the message never leaves the process); SBE
// gives it a self-describing, versioned envelope on the ring like every other
// message, instead of a raw memcpy of pointer bytes.
// ---------------------------------------------------------------------------
struct RegisterProducer {
    uint64_t handle = 0;

    static constexpr uint16_t    TEMPLATE_ID  = 1;
    static constexpr uint16_t    BODY_FIXED   = 8;
    static constexpr std::size_t ENCODED_SIZE =
        pc::MessageHeader::ENCODED_SIZE + BODY_FIXED;  // 32

    /// Encode the full frame into buf (must be ≥ ENCODED_SIZE). Returns bytes written.
    std::size_t encode(uint8_t* buf) const noexcept {
        pc::MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kInternalSchemaId;
        hdr.version       = kInternalVersion;
        hdr.sessionId     = 0;
        hdr.correlationId = 0;
        hdr.encode(buf);
        pc::detail::putLE64(buf,
                            static_cast<int>(pc::MessageHeader::ENCODED_SIZE),
                            static_cast<int64_t>(handle));
        return ENCODED_SIZE;
    }

    /// Decode a full frame from buf[0..avail-1]. Returns nullopt on any mismatch.
    static std::optional<RegisterProducer> decode(const uint8_t* buf,
                                                  std::size_t avail) noexcept {
        auto hdrOpt = pc::MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const pc::MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kInternalSchemaId) return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID)       return std::nullopt;
        if (avail < ENCODED_SIZE)                return std::nullopt;

        RegisterProducer msg;
        msg.handle = static_cast<uint64_t>(
            pc::detail::getLE64(buf,
                                static_cast<int>(pc::MessageHeader::ENCODED_SIZE)));
        return msg;
    }
};

// ---------------------------------------------------------------------------
// HubCommandMsg — F8a: an app-thread subscriber / group-consumer operation for
// the duty thread (see detail::HubCommand for the ops and payload ownership).
//
// Frame layout (all little-endian):
//   [0..23]  MessageHeader (blockLength=32, templateId=2, schemaId=121)
//   [24..25] op     : uint16
//   [26..31] reserved (zero)
//   [32..39] handle : uint64   (process-local pointer / identity; never leaves the process)
//   [40..47] a      : int64
//   [48..55] b      : int64
//   [56..63] c      : int64   (F8c C4: completion-target word)
//   [64..71] d      : int64   (F8c C5: op payload)
// ---------------------------------------------------------------------------
struct HubCommandMsg {
    static constexpr uint16_t    TEMPLATE_ID  = 2;
    static constexpr uint16_t    BODY_FIXED   = 48;
    static constexpr std::size_t ENCODED_SIZE =
        pc::MessageHeader::ENCODED_SIZE + BODY_FIXED;  // 72

    static std::size_t encode(const ::rune::client::detail::HubCommand& c, uint8_t* buf) noexcept {
        pc::MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kInternalSchemaId;
        hdr.version       = kInternalVersion;
        hdr.sessionId     = 0;
        hdr.correlationId = 0;
        hdr.encode(buf);
        constexpr int B = static_cast<int>(pc::MessageHeader::ENCODED_SIZE);
        pc::detail::putLE16(buf, B + 0, static_cast<uint16_t>(c.op));
        for (int i = 2; i < 8; ++i) buf[B + i] = 0;
        pc::detail::putLE64(buf, B + 8,  static_cast<int64_t>(c.handle));
        pc::detail::putLE64(buf, B + 16, c.a);
        pc::detail::putLE64(buf, B + 24, c.b);
        pc::detail::putLE64(buf, B + 32, c.c);
        pc::detail::putLE64(buf, B + 40, c.d);
        return ENCODED_SIZE;
    }

    static std::optional<::rune::client::detail::HubCommand>
    decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = pc::MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        if (hdrOpt->schemaId   != kInternalSchemaId) return std::nullopt;
        if (hdrOpt->templateId != TEMPLATE_ID)       return std::nullopt;
        if (avail < ENCODED_SIZE)                    return std::nullopt;
        constexpr int B = static_cast<int>(pc::MessageHeader::ENCODED_SIZE);
        ::rune::client::detail::HubCommand c;
        c.op     = static_cast<::rune::client::detail::HubCommand::Op>(pc::detail::getLE16(buf, B + 0));
        c.handle = static_cast<uint64_t>(pc::detail::getLE64(buf, B + 8));
        c.a      = pc::detail::getLE64(buf, B + 16);
        c.b      = pc::detail::getLE64(buf, B + 24);
        c.c      = pc::detail::getLE64(buf, B + 32);
        c.d      = pc::detail::getLE64(buf, B + 40);
        return c;
    }
};

} // namespace rune::client::agent::ring_msg
