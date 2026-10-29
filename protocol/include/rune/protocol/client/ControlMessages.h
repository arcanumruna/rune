#pragma once
#include <array>
#include <cstddef>
#include <cstring>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include "Codec.h"
#include "MessageHeader.h"
#include "Protocol.h"

namespace rune::protocol::client {

// ---------------------------------------------------------------------------
// ConnectRequest  (templateId 1)
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (8)
//     templateId  = 1
//   [24..27]  egressStreamId : int32
//   [28..31]  clientVersion  : int32
//   [32..33]  egressChannel length : uint16
//   [34..]    egressChannel bytes
//
// Total = 24 + 8 + 2 + egressChannel.size() = 34 + egressChannel.size()
//
// client<->broker TLS/AEAD (Task 4) — additive trailing crypto fields:
//   [..+2]   cipherList length : uint16  (var prefix, putVarBytes)
//   [..+n]   cipherList bytes
//   [..+4]   handshakeBytes length : uint32  (var prefix, putVarBytes32)
//   [..+m]   handshakeBytes bytes
//   [..+2]   authToken length : uint16  (var prefix, putVarStr)
//   [..+k]   authToken bytes
// These three fields are written ONLY when at least one is non-empty (i.e.
// the client is offering crypto); an old (<v4) peer that never sets them
// sees byte-identical frames to before this task. On decode they are only
// attempted when clientVersion >= kClientVersionCrypto AND trailing bytes
// remain — absence of either ⇒ "no crypto offered" (fields stay default).
// This mirrors the additive blockLength/avail-gated idiom used elsewhere in
// this codec (see Produce's BODY_FIXED_V0/V1 in DataMessages.h) without
// bumping kProtocolVersion.
//
// v16 (NAT-safe egress, kClientVersionResponseEgress): the crypto block is ALWAYS present (empty fields encode as
// zero-length var fields: 2+4+2 bytes), followed by one byte responseCorrelated (0 or 1; anything else is rejected).
// A frame with clientVersion < 16 is byte-identical to before.  On decode a v16 frame that ends right after the
// crypto block has responseCorrelated = false; bytes after the flag are ignored (later additive fields).
// ---------------------------------------------------------------------------
struct ConnectRequest {
    int64_t      sessionId       = 0;
    int64_t      correlationId   = 0;
    int32_t      egressStreamId  = 0;
    int32_t      clientVersion   = 0;
    std::string_view egressChannel;   // e.g. "aeron:udp?endpoint=host:port"

    // v4 additive crypto fields — default-empty ⇒ no crypto offered.
    std::span<const uint8_t> cipherList;    // client-preferred cipher ids, in order
    std::string_view         handshakeBytes; // opening handshake blob (e.g. TLS ClientHello)
    std::string_view         authToken;      // optional bearer/auth token
    bool responseCorrelated = false;  // v16: replies ride an Aeron response publication (see kClientVersionResponseEgress)

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::Connect); // 1
    static constexpr uint16_t BODY_FIXED  = 8;  // fixed body bytes (not counting header or var fields)

    /// Exact encoded frame size; mirrors encode() (every hand-sized buffer must use this).
    ///   header + BODY_FIXED + 2 + egressChannel.size()
    ///   [+ 2+cipherList.size() + 4+handshakeBytes.size() + 2+authToken.size() if offering crypto or clientVersion >= 16]
    ///   [+ 1 flag byte at clientVersion >= 16]
    std::size_t encodedSize() const noexcept {
        std::size_t n = MessageHeader::ENCODED_SIZE + BODY_FIXED + 2u + egressChannel.size();
        const bool offersCrypto = !cipherList.empty() || !handshakeBytes.empty() || !authToken.empty();
        if (offersCrypto || clientVersion >= kClientVersionResponseEgress)
            n += 2u + cipherList.size() + 4u + handshakeBytes.size() + 2u + authToken.size();
        if (clientVersion >= kClientVersionResponseEgress) n += 1u;
        return n;
    }

    /// Encode the full frame into buf.  buf must hold encodedSize() bytes.
    /// Returns total bytes written.
    std::size_t encode(uint8_t* buf) const noexcept {
        // Write header
        MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);

        // Fixed body starting at byte 24
        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        detail::putLE32(buf, fixedBase + 0, egressStreamId);
        detail::putLE32(buf, fixedBase + 4, clientVersion);

        // Var field starting at byte 32
        const std::size_t varOffset = fixedBase + BODY_FIXED;
        std::size_t pos = varOffset + putVarStr(buf + varOffset, egressChannel);

        // v4 additive crypto fields — omitted entirely when nothing is offered,
        // so an old-style ConnectRequest (no crypto fields set) encodes to
        // byte-identical output as before this task.
        const bool offersCrypto = !cipherList.empty() || !handshakeBytes.empty() || !authToken.empty();
        const bool writeCryptoBlock = offersCrypto || clientVersion >= kClientVersionResponseEgress;
        if (writeCryptoBlock) {
            pos += putVarBytes(buf + pos, cipherList.data(), static_cast<uint16_t>(cipherList.size()));
            pos += putVarBytes32(buf + pos,
                                 reinterpret_cast<const uint8_t*>(handshakeBytes.data()),
                                 static_cast<uint32_t>(handshakeBytes.size()));
            pos += putVarStr(buf + pos, authToken);
        }
        if (clientVersion >= kClientVersionResponseEgress) buf[pos++] = responseCorrelated ? 1 : 0;

        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<ConnectRequest> decode(const uint8_t* buf, std::size_t avail) noexcept {
        // Decode and validate header
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        // Fixed body
        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        ConnectRequest msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        msg.egressStreamId = detail::getLE32(buf, static_cast<int>(fixedBase) + 0);
        msg.clientVersion  = detail::getLE32(buf, static_cast<int>(fixedBase) + 4);

        // Var field
        std::size_t pos = fixedBase + BODY_FIXED;
        if (!getVarStr(buf, avail, pos, msg.egressChannel)) return std::nullopt;

        // v4 additive crypto fields (trailing) — only attempted when the
        // sender declares clientVersion >= kClientVersionCrypto AND there are
        // bytes left to read. Absence of either ⇒ no crypto offered (fields
        // stay default-empty); an old (<v4) client's frame decodes exactly
        // as before this task.
        // At clientVersion >= 16 the crypto block is mandatory (an empty one is 2+4+2 bytes).
        const bool v16 = msg.clientVersion >= kClientVersionResponseEgress;
        if (v16 && pos >= avail) return std::nullopt;
        if (msg.clientVersion >= kClientVersionCrypto && pos < avail) {
            const uint8_t* clData = nullptr;
            uint16_t       clLen  = 0;
            if (!getVarBytes(buf, avail, pos, clData, clLen)) return std::nullopt;
            msg.cipherList = std::span<const uint8_t>(clData, clLen);

            const uint8_t* hbData = nullptr;
            uint32_t       hbLen  = 0;
            if (!getVarBytes32(buf, avail, pos, hbData, hbLen)) return std::nullopt;
            msg.handshakeBytes = std::string_view(reinterpret_cast<const char*>(hbData), hbLen);

            if (!getVarStr(buf, avail, pos, msg.authToken)) return std::nullopt;
        }

        // v16: the response-correlated flag follows the crypto block and is mandatory.
        if (v16) {
            if (pos >= avail) return std::nullopt;
            const uint8_t f = buf[pos++];
            if (f > 1) return std::nullopt;
            msg.responseCorrelated = f == 1;
        }

        return msg;
    }
};

// ---------------------------------------------------------------------------
// WorkerBind  (templateId 86) — NAT-safe egress (client version 16), the FIRST frame (plaintext) a response-mode
// client sends on its ingress publication to a shard worker.
//
//   [0..23]   MessageHeader (24 bytes)   blockLength = 16, templateId = 86,
//                                        sessionId (bytes 8..15, where Produce carries it) = the connect's session,
//                                        correlationId = the client's bind correlation
//   [24..39]  token : 16 B               the bindToken of the ConnectResponse
//
// Total = 40 bytes.  The worker pins the session to the Aeron image the frame arrived on and answers on a response
// publication correlated with it.  A wrong token, a repeated bind and an unknown session are dropped without a reply.
// ---------------------------------------------------------------------------
struct WorkerBind {
    int64_t                 sessionId     = 0;
    int64_t                 correlationId = 0;
    std::array<uint8_t, 16> token{};

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::WorkerBind);   // 86
    static constexpr uint16_t BODY_FIXED  = 16;
    static constexpr std::size_t ENCODED_SIZE = MessageHeader::ENCODED_SIZE + BODY_FIXED;    // 40

    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);
        std::memcpy(buf + MessageHeader::ENCODED_SIZE, token.data(), token.size());
        return ENCODED_SIZE;
    }

    static std::optional<WorkerBind> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE) return std::nullopt;
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId != kSchemaId || hdr.templateId != TEMPLATE_ID) return std::nullopt;
        if (hdr.blockLength < BODY_FIXED) return std::nullopt;
        WorkerBind m;
        m.sessionId     = hdr.sessionId;
        m.correlationId = hdr.correlationId;
        std::memcpy(m.token.data(), buf + MessageHeader::ENCODED_SIZE, m.token.size());
        return m;
    }
};

// ---------------------------------------------------------------------------
// WorkerBindAck  (templateId 87) — broker -> client only.  The worker's acknowledgement of a WorkerBind, sent
// PLAINTEXT on the session's response publication (a TLS client accepts exactly this template unsealed: it carries no
// secret, only the session id and the bind's correlation id).
//
//   [0..23]   MessageHeader (24 bytes)   blockLength = 0, templateId = 87,
//                                        sessionId (bytes 8..15) = the bound session,
//                                        correlationId (bytes 16..23) = the WorkerBind's correlationId
//
// Total = 24 bytes, no body.  Until the client sees it the bind is not established: it resends the WorkerBind.
// ---------------------------------------------------------------------------
struct WorkerBindAck {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::WorkerBindAck);   // 87
    static constexpr std::size_t ENCODED_SIZE = MessageHeader::ENCODED_SIZE;                    // 24

    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr;
        hdr.blockLength   = 0;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);
        return ENCODED_SIZE;
    }

    static std::optional<WorkerBindAck> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE) return std::nullopt;
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId != kSchemaId || hdr.templateId != TEMPLATE_ID) return std::nullopt;
        WorkerBindAck m;
        m.sessionId     = hdr.sessionId;
        m.correlationId = hdr.correlationId;
        return m;
    }
};

// ---------------------------------------------------------------------------
// ConnectResponse  (templateId 2)
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (18)
//     templateId  = 2
//   [24..25]  code              : int16
//   [26..29]  leaderMemberId    : int32
//   [30..37]  assignedSessionId : int64
//   [38..41]  brokerVersion     : int32  (additive; absent in the 38-byte
//                                         frame of an older broker = 0)
//
// Total = 24 + 18 = 42 bytes (no var fields); decode accepts the 38-byte form.
//
// client<->broker TLS/AEAD (Task 4) — additive trailing crypto fields,
// layered AFTER brokerVersion:
//   [42]      chosenCipher : uint8   (rune::crypto::Cipher value; 0 = none chosen)
//   [43..46]  handshakeBytes length : uint32 (var prefix, putVarBytes32)
//   [47..]    handshakeBytes bytes
// Written ONLY when the broker actually chose a cipher or has handshake
// bytes to return (chosenCipher != 0 || !handshakeBytes.empty()); otherwise
// encode() produces the exact main 42-byte brokerVersion frame, byte-for-byte.
// On decode, the trailing fields are only read when hdr.blockLength >=
// BODY_FIXED_V4 AND avail >= ENCODED_SIZE_V4 — the same blockLength/avail-
// gated idiom used by Produce's BODY_FIXED_V0/V1 in DataMessages.h.
// kProtocolVersion is not bumped; absence of the gate ⇒ no crypto chosen
// (fields stay default).
//
// authentication (client version 9) — BODY_FIXED_V9 = 21, written iff `v9` is set
// (the broker sets it for clientVersion >= 9):
//   [42]      chosenCipher : uint8   (0 = none)
//   [43]      authMode     : uint8   (AuthMode)
//   [44]      authState    : uint8   (AuthState)
//   [45..48]  handshakeBytes length : uint32 (ALWAYS present in a v9 frame, possibly 0)
//   [49..]    handshakeBytes bytes
// NAT-safe egress (client version 16) — BODY_FIXED_V16 = 37, written iff `v16` is set (implies the v9 fields):
//   [45..60]  bindToken : 16 B  (all-zero = none).  A response-mode shard session's secret for its WorkerBind.
// The handshake var bytes still sit at fixedBase + blockLength, so a pre-v16 decoder skips the token.
// decode(): blockLength >= 21 reads authMode/authState; the handshake var bytes
// always sit at fixedBase + blockLength.  A pre-v9 frame decodes as
// authMode = InProcess, authState = Anonymous: a pre-v9 broker has no authentication.
// ---------------------------------------------------------------------------
struct ConnectResponse {
    int64_t sessionId         = 0;
    int64_t correlationId     = 0;
    int16_t code              = 0;   // rune::protocol::client::ErrorCode value
    int32_t leaderMemberId    = -1;
    int64_t assignedSessionId = 0;
    int32_t brokerVersion     = kBrokerVersion;  // capability gate (Protocol.h)

    // v4 additive crypto fields — default ⇒ no crypto chosen.
    uint8_t           chosenCipher = 0;    // rune::crypto::Cipher value; 0 = none
    std::string_view  handshakeBytes;      // handshake continuation blob (e.g. TLS ServerHello)

    // v9 additive authentication fields — default ⇒ a broker without authentication.
    bool      v9        = false;                  // encode the v9 frame (the broker sets it for clientVersion >= 9)
    AuthMode  authMode  = AuthMode::InProcess;
    AuthState authState = AuthState::Anonymous;

    // v16 additive NAT-safe egress field (implies v9): the worker bind token of a response-mode shard session.
    bool                    v16       = false;
    std::array<uint8_t, 16> bindToken{};

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::ConnectResponse); // 2
    static constexpr uint16_t BODY_FIXED    = 18;  // incl. brokerVersion (main)
    static constexpr uint16_t BODY_FIXED_V0 = 14;  // an older broker's body (no brokerVersion)
    static constexpr uint16_t BODY_FIXED_V4 = 19;  // + chosenCipher byte, AFTER brokerVersion
    static constexpr uint16_t BODY_FIXED_V9 = 21;  // + authMode, authState bytes, AFTER chosenCipher
    static constexpr uint16_t BODY_FIXED_V16 = 37; // + 16 B bindToken, AFTER authState

    static constexpr std::size_t ENCODED_SIZE    = MessageHeader::ENCODED_SIZE + BODY_FIXED;     // 42
    static constexpr std::size_t ENCODED_SIZE_V0 = MessageHeader::ENCODED_SIZE + BODY_FIXED_V0;  // 38
    static constexpr std::size_t ENCODED_SIZE_V4 = MessageHeader::ENCODED_SIZE + BODY_FIXED_V4;  // 43
    static constexpr std::size_t ENCODED_SIZE_V9 = MessageHeader::ENCODED_SIZE + BODY_FIXED_V9;  // 45
    static constexpr std::size_t ENCODED_SIZE_V16 = MessageHeader::ENCODED_SIZE + BODY_FIXED_V16; // 61

    /// Encode the full frame into buf. Returns ENCODED_SIZE (42) when no
    /// crypto was chosen — byte-identical to main's brokerVersion frame — or
    /// ENCODED_SIZE_V4 + 4 + handshakeBytes.size() when the broker chose a
    /// cipher and/or has handshake bytes to send.
    std::size_t encode(uint8_t* buf) const noexcept {
        const bool chosenCrypto = (chosenCipher != 0) || !handshakeBytes.empty();
        const bool withV9 = v9 || v16;

        MessageHeader hdr;
        hdr.blockLength   = v16 ? BODY_FIXED_V16 : (withV9 ? BODY_FIXED_V9 : (chosenCrypto ? BODY_FIXED_V4 : BODY_FIXED));
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        detail::putLE16s(buf, fixedBase + 0, code);
        detail::putLE32 (buf, fixedBase + 2, leaderMemberId);
        detail::putLE64 (buf, fixedBase + 6, assignedSessionId);
        detail::putLE32 (buf, fixedBase + 14, brokerVersion);

        if (!withV9 && !chosenCrypto) return ENCODED_SIZE;

        buf[fixedBase + 18] = chosenCipher;  // @42, AFTER brokerVersion [14..17]
        if (withV9) {
            buf[fixedBase + 19] = static_cast<uint8_t>(authMode);
            buf[fixedBase + 20] = static_cast<uint8_t>(authState);
        }
        if (v16) std::memcpy(buf + fixedBase + 21, bindToken.data(), bindToken.size());
        std::size_t pos = static_cast<std::size_t>(fixedBase) +
                          (v16 ? BODY_FIXED_V16 : (withV9 ? BODY_FIXED_V9 : BODY_FIXED_V4));
        pos += putVarBytes32(buf + pos,
                             reinterpret_cast<const uint8_t*>(handshakeBytes.data()),
                             static_cast<uint32_t>(handshakeBytes.size()));
        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<ConnectResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE_V0) return std::nullopt;

        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;
        // #194: the header states the body length - a frame shorter than it is truncated, never a shorter (older) layout.
        if (hdr.blockLength < BODY_FIXED_V0 || avail < MessageHeader::ENCODED_SIZE + hdr.blockLength) return std::nullopt;

        ConnectResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        msg.code              = detail::getLE16s(buf, fixedBase + 0);
        msg.leaderMemberId    = detail::getLE32 (buf, fixedBase + 2);
        msg.assignedSessionId = detail::getLE64 (buf, fixedBase + 6);
        // Additive field: present when the sender's body is long enough.
        msg.brokerVersion = (hdr.blockLength >= BODY_FIXED)
                                ? static_cast<int32_t>(detail::getLE32(buf, fixedBase + 14))
                                : 0;

        // v4 additive crypto fields — gated on blockLength/avail (mirrors the
        // Produce BODY_FIXED_V0/V1 idiom). Absence ⇒ no crypto chosen.
        if (hdr.blockLength >= BODY_FIXED_V4) {
            msg.chosenCipher = buf[fixedBase + 18];  // @42, AFTER brokerVersion [14..17]
            if (hdr.blockLength >= BODY_FIXED_V9) {
                const uint8_t mode  = buf[fixedBase + 19];
                const uint8_t state = buf[fixedBase + 20];
                if (mode > static_cast<uint8_t>(AuthMode::InProcess) ||
                    state > static_cast<uint8_t>(AuthState::Anonymous)) return std::nullopt;
                msg.v9        = true;
                msg.authMode  = static_cast<AuthMode>(mode);
                msg.authState = static_cast<AuthState>(state);
                if (hdr.blockLength >= BODY_FIXED_V16) {
                    msg.v16 = true;
                    std::memcpy(msg.bindToken.data(), buf + fixedBase + 21, msg.bindToken.size());
                }
            }
            std::size_t pos = static_cast<std::size_t>(fixedBase) + hdr.blockLength;
            const uint8_t* hbData = nullptr;
            uint32_t       hbLen  = 0;
            if (!getVarBytes32(buf, avail, pos, hbData, hbLen)) return std::nullopt;
            msg.handshakeBytes = std::string_view(reinterpret_cast<const char*>(hbData), hbLen);
        }

        return msg;
    }
};

// ---------------------------------------------------------------------------
// Keepalive  (templateId 3, BODY_FIXED = 0)
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (0)
//     templateId  = 3
//
// Total = 24 bytes (header only, no body, no var fields)
// ---------------------------------------------------------------------------
struct Keepalive {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::Keepalive); // 3
    static constexpr uint16_t BODY_FIXED  = 0;

    static constexpr std::size_t ENCODED_SIZE = MessageHeader::ENCODED_SIZE + BODY_FIXED; // 24

    /// Encode the full frame (24 bytes) into buf. Returns 24.
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);
        return ENCODED_SIZE;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<Keepalive> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE) return std::nullopt;

        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        Keepalive msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        return msg;
    }
};

// ---------------------------------------------------------------------------
// Disconnect  (templateId 4, BODY_FIXED = 2)
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (2)
//     templateId  = 4
//   [24..25]  reason : int16
//
// Total = 24 + 2 = 26 bytes (no var fields)
// ---------------------------------------------------------------------------
struct Disconnect {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    int16_t reason        = 0;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::Disconnect); // 4
    static constexpr uint16_t BODY_FIXED  = 2;

    static constexpr std::size_t ENCODED_SIZE = MessageHeader::ENCODED_SIZE + BODY_FIXED; // 26

    /// Encode the full frame (26 bytes) into buf. Returns 26.
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        detail::putLE16s(buf, fixedBase + 0, reason);

        return ENCODED_SIZE;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<Disconnect> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE) return std::nullopt;

        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        Disconnect msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        msg.reason = detail::getLE16s(buf, fixedBase + 0);

        return msg;
    }
};

// ---------------------------------------------------------------------------
// ErrorResponse  (templateId 5, BODY_FIXED = 2)
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (2)
//     templateId  = 5
//   [24..25]  code    : int16  (ErrorCode value)
//   [26..27]  message length : uint16  (var prefix)
//   [28..]    message bytes
//
//   [..+8]    retryAfterNs : uint64 — ONLY when code == ErrorCode::Throttled (trailer after the message; older
//             decoders ignore trailing bytes, a Throttled frame without it decodes retryAfterNs 0)
//
// Total = 24 + 2 + 2 + message.size() = 28 + message.size()  (+ 8 when code == Throttled)
// ---------------------------------------------------------------------------
struct ErrorResponse {
    int64_t          sessionId     = 0;
    int64_t          correlationId = 0;
    int16_t          code          = 0;   // rune::protocol::client::ErrorCode value
    std::string_view message;
    uint64_t         retryAfterNs  = 0;   // Throttled only: how long the client should wait before retrying

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::ErrorResponse); // 5
    static constexpr uint16_t BODY_FIXED  = 2;

    /// Encode the full frame into buf.  buf must be large enough:
    ///   MessageHeader::ENCODED_SIZE + BODY_FIXED + 2 + message.size()  (+ 8 when code == Throttled)
    /// Returns total bytes written.
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        detail::putLE16s(buf, fixedBase + 0, code);

        const std::size_t varOffset = fixedBase + BODY_FIXED;
        const std::size_t varWritten = putVarStr(buf + varOffset, message);

        std::size_t end = varOffset + varWritten;
        if (code == static_cast<int16_t>(ErrorCode::Throttled)) {
            detail::putLE64(buf, static_cast<int>(end), static_cast<int64_t>(retryAfterNs));
            end += 8;
        }
        return end;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<ErrorResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        // Decode and validate header
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        // Fixed body
        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        ErrorResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        msg.code          = detail::getLE16s(buf, static_cast<int>(fixedBase) + 0);

        // Var field
        std::size_t pos = fixedBase + BODY_FIXED;
        if (!getVarStr(buf, avail, pos, msg.message)) return std::nullopt;

        if (msg.code == static_cast<int16_t>(ErrorCode::Throttled) && pos + 8u <= avail)
            msg.retryAfterNs = static_cast<uint64_t>(detail::getLE64(buf, static_cast<int>(pos)));
        return msg;
    }
};

// ---------------------------------------------------------------------------
// HandshakeContinue  (templateId 10, BODY_FIXED = 0)
//
// client<->broker TLS/AEAD (Task 4) — a continuation leg of a multi-round
// crypto handshake (sent after the initial Connect/ConnectResponse exchange
// when the chosen cipher needs more than one round-trip, e.g. TLS).
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (0)
//     templateId  = 10
//   [24..27]  handshakeBytes length : uint32 (var prefix, putVarBytes32)
//   [28..]    handshakeBytes bytes
//
// Total = 24 + 4 + handshakeBytes.size() = 28 + handshakeBytes.size()
// ---------------------------------------------------------------------------
struct HandshakeContinue {
    int64_t           sessionId     = 0;
    int64_t           correlationId = 0;
    std::string_view  handshakeBytes;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::HandshakeContinue); // 10
    static constexpr uint16_t BODY_FIXED  = 0;

    /// Total encoded size: header + 4-byte var32 prefix + handshakeBytes.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 4u + handshakeBytes.size();
    }

    /// Encode the full frame into buf. buf must be >= encodedSize() bytes.
    /// Returns total bytes written.
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        std::size_t pos = fixedBase + BODY_FIXED;
        pos += putVarBytes32(buf + pos,
                             reinterpret_cast<const uint8_t*>(handshakeBytes.data()),
                             static_cast<uint32_t>(handshakeBytes.size()));
        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<HandshakeContinue> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        HandshakeContinue msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        std::size_t pos = fixedBase + BODY_FIXED;
        const uint8_t* hbData = nullptr;
        uint32_t       hbLen  = 0;
        if (!getVarBytes32(buf, avail, pos, hbData, hbLen)) return std::nullopt;
        msg.handshakeBytes = std::string_view(reinterpret_cast<const char*>(hbData), hbLen);

        return msg;
    }
};

/// Free-function encode helper: bounds-checks against bufSize before calling
/// HandshakeContinue::encode. Returns 0 (no write) if bufSize is too small.
inline std::size_t encodeHandshakeContinue(uint8_t* buf, std::size_t bufSize,
                                           const HandshakeContinue& msg) noexcept {
    if (bufSize < msg.encodedSize()) return 0;
    return msg.encode(buf);
}

// ---------------------------------------------------------------------------
// HandshakeContinueResponse  (templateId 11, BODY_FIXED = 2)
//
// client<->broker TLS/AEAD (Task 4) — the broker's reply to a
// HandshakeContinue leg: either more handshake bytes to send back (code ==
// ErrorCode::OK, continue) or a terminal code (e.g.
// ErrorCode::HandshakeFailed) ending the handshake.
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (2)
//     templateId  = 11
//   [24..25]  code : int16  (rune::protocol::client::ErrorCode value)
//   [26..29]  handshakeBytes length : uint32 (var prefix, putVarBytes32)
//   [30..]    handshakeBytes bytes
//
// Total = 24 + 2 + 4 + handshakeBytes.size() = 30 + handshakeBytes.size()
// ---------------------------------------------------------------------------
struct HandshakeContinueResponse {
    int64_t           sessionId     = 0;
    int64_t           correlationId = 0;
    int16_t           code          = 0;  // rune::protocol::client::ErrorCode value
    std::string_view  handshakeBytes;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::HandshakeContinueResponse); // 11
    static constexpr uint16_t BODY_FIXED  = 2;

    /// Total encoded size: header + code + 4-byte var32 prefix + handshakeBytes.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 4u + handshakeBytes.size();
    }

    /// Encode the full frame into buf. buf must be >= encodedSize() bytes.
    /// Returns total bytes written.
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        detail::putLE16s(buf, fixedBase + 0, code);

        std::size_t pos = static_cast<std::size_t>(fixedBase) + BODY_FIXED;
        pos += putVarBytes32(buf + pos,
                             reinterpret_cast<const uint8_t*>(handshakeBytes.data()),
                             static_cast<uint32_t>(handshakeBytes.size()));
        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<HandshakeContinueResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        HandshakeContinueResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        msg.code          = detail::getLE16s(buf, static_cast<int>(fixedBase) + 0);

        std::size_t pos = fixedBase + BODY_FIXED;
        const uint8_t* hbData = nullptr;
        uint32_t       hbLen  = 0;
        if (!getVarBytes32(buf, avail, pos, hbData, hbLen)) return std::nullopt;
        msg.handshakeBytes = std::string_view(reinterpret_cast<const char*>(hbData), hbLen);

        return msg;
    }
};

/// Free-function encode helper: bounds-checks against bufSize before calling
/// HandshakeContinueResponse::encode. Returns 0 (no write) if bufSize is too small.
inline std::size_t encodeHandshakeContinueResponse(uint8_t* buf, std::size_t bufSize,
                                                   const HandshakeContinueResponse& msg) noexcept {
    if (bufSize < msg.encodedSize()) return 0;
    return msg.encode(buf);
}

} // namespace rune::protocol::client
