#pragma once
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include "Codec.h"
#include "MessageHeader.h"
#include "Protocol.h"

namespace rune::protocol::client {

// ---------------------------------------------------------------------------
// Produce  (templateId 16)
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (10)
//     templateId  = 16
//   [24..27]  topicId        : int32
//   [28..31]  partition      : int32   (used when partitionKnown==1; else ignore)
//   [32]      partitionKnown : uint8   (1 => use partition; 0 => broker resolves from key)
//   [33]      acks           : uint8   (0 = Leader/None, 1 = All — GR-5)
//   BODY_FIXED = 10, so var fields start at offset 34 (header 24 + 10)
//   [34..35]  key length     : uint16  (var prefix; may be 0 when partitionKnown==1)
//   [36..]    key bytes
//   [36+keyLen..37+keyLen]  recordBatch length : uint16  (var prefix)
//   [38+keyLen..]           recordBatch bytes   (opaque — never parsed by the codec)
//
// Backward compatibility: old clients (BODY_FIXED==9) omit the acks byte.
// The decoder accepts both: if the frame only carries 9 fixed bytes, acks defaults to 0.
//
// Note: the u16 length prefix caps a single var field at 65535 bytes.
// Producing batches larger than 65535 bytes requires multi-frame framing,
// which is deferred to a later milestone.
//
// Total = 24 + 10 + 2 + key.size() + 2 + recordBatch.size()
//       = 38 + key.size() + recordBatch.size()
// ---------------------------------------------------------------------------
struct Produce {
    int64_t  sessionId      = 0;
    int64_t  correlationId  = 0;
    int32_t  topicId        = 0;
    int32_t  partition      = 0;
    uint8_t  partitionKnown = 0;   // 1 => explicit partition; 0 => resolve from key
    uint8_t  acks           = 0;   // 0 = Leader/None (immediate ack), 1 = All (deferred until ISR HW)
    std::span<const uint8_t> key;          // zero-copy view; may be empty
    std::span<const uint8_t> recordBatch;  // opaque blob; zero-copy view

    static constexpr uint16_t TEMPLATE_ID   = static_cast<uint16_t>(TemplateId::Produce); // 16
    static constexpr uint16_t BODY_FIXED    = 10; // topicId(4) + partition(4) + partitionKnown(1) + acks(1)
    static constexpr uint16_t BODY_FIXED_V0 = 9;  // old clients without the acks byte

    /// Total encoded size accounting for the two var fields.
    /// key and recordBatch are each capped at 65535 bytes by the u16 prefix.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 2u + key.size()
             + 2u + recordBatch.size();
    }

    /// Encode the full frame into buf.  buf must be ≥ encodedSize() bytes.
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
        detail::putLE32(buf, fixedBase + 0, topicId);
        detail::putLE32(buf, fixedBase + 4, partition);
        buf[fixedBase + 8] = partitionKnown;
        buf[fixedBase + 9] = acks;

        std::size_t pos = static_cast<std::size_t>(fixedBase) + BODY_FIXED;

        // key var field
        pos += putVarBytes(buf + pos,
                           key.data(),
                           static_cast<uint16_t>(key.size()));

        // recordBatch var field
        pos += putVarBytes(buf + pos,
                           recordBatch.data(),
                           static_cast<uint16_t>(recordBatch.size()));

        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    /// The returned spans are zero-copy views into buf — buf must outlive the result.
    /// Backward compat: accepts both BODY_FIXED==9 (old, acks=0) and BODY_FIXED==10 (new).
    static std::optional<Produce> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        // Accept both v0 (BODY_FIXED=9, no acks byte) and v1 (BODY_FIXED=10).
        // The blockLength in the header tells us which variant the sender used.
        const uint16_t declaredFixed = hdr.blockLength;
        if (declaredFixed < BODY_FIXED_V0) return std::nullopt; // too short
        if (avail < MessageHeader::ENCODED_SIZE + declaredFixed) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;

        Produce msg;
        msg.sessionId      = hdr.sessionId;
        msg.correlationId  = hdr.correlationId;
        msg.topicId        = detail::getLE32(buf, static_cast<int>(fixedBase) + 0);
        msg.partition      = detail::getLE32(buf, static_cast<int>(fixedBase) + 4);
        msg.partitionKnown = buf[fixedBase + 8];
        // acks field only present in v1 (declaredFixed >= 10)
        msg.acks = (declaredFixed >= BODY_FIXED) ? buf[fixedBase + 9] : uint8_t{0};

        // Var fields start after the declared fixed block (forward-compat: skip unknown fields).
        std::size_t pos = fixedBase + declaredFixed;

        // key var field
        const uint8_t* keyData = nullptr;
        uint16_t       keyLen  = 0;
        if (!getVarBytes(buf, avail, pos, keyData, keyLen)) return std::nullopt;
        msg.key = std::span<const uint8_t>(keyData, keyLen);

        // recordBatch var field
        const uint8_t* rbData = nullptr;
        uint16_t       rbLen  = 0;
        if (!getVarBytes(buf, avail, pos, rbData, rbLen)) return std::nullopt;
        msg.recordBatch = std::span<const uint8_t>(rbData, rbLen);

        return msg;
    }
};

// ---------------------------------------------------------------------------
// ProduceResponse  (templateId 17)
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (14)
//     templateId  = 17
//   [24..25]  errorCode  : int16   (ErrorCode value; 0 = OK)
//   [26..29]  partition  : int32   (assigned/confirmed partition)
//   [30..37]  baseOffset : int64   (base offset of the batch; ignore when errorCode != 0)
//   BODY_FIXED = 14 (2 + 4 + 8)
//
// Total = 24 + 14 = 38 bytes (no var fields)
// ---------------------------------------------------------------------------
struct ProduceResponse {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    int16_t errorCode     = 0;   // rune::protocol::client::ErrorCode value
    int32_t partition     = 0;
    int64_t baseOffset    = 0;   // meaningful only when errorCode == 0

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::ProduceResponse); // 17
    static constexpr uint16_t BODY_FIXED  = 14;  // errorCode(2) + partition(4) + baseOffset(8)

    static constexpr std::size_t ENCODED_SIZE = MessageHeader::ENCODED_SIZE + BODY_FIXED; // 38

    /// Encode the full frame (38 bytes) into buf.  Returns 38.
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
        detail::putLE16s(buf, fixedBase + 0, errorCode);
        detail::putLE32 (buf, fixedBase + 2, partition);
        detail::putLE64 (buf, fixedBase + 6, baseOffset);

        return ENCODED_SIZE;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<ProduceResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE) return std::nullopt;

        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        ProduceResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        msg.errorCode  = detail::getLE16s(buf, fixedBase + 0);
        msg.partition  = detail::getLE32 (buf, fixedBase + 2);
        msg.baseOffset = detail::getLE64 (buf, fixedBase + 6);

        return msg;
    }
};

// ---------------------------------------------------------------------------
// Subscribe  (templateId 18)  client→broker
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (18)
//     templateId  = 18
//   [24..27]  topicId        : int32
//   [28..31]  partition      : int32   (used when partitionKnown==1; else ignore)
//   [32]      partitionKnown : uint8   (1 => use partition; 0 => broker resolves from key)
//   [33..40]  fromOffset     : int64
//   [41]      isolation      : uint8   (0=read_uncommitted, 1=read_committed)
//   BODY_FIXED = 18 (4+4+1+8+1), var fields start at offset 42 (header 24 + 18)
//   [42..43]  key length     : uint16  (var prefix; empty when partitionKnown==1)
//   [44..]    key bytes
//
// Total = 24 + 18 + 2 + key.size()
//       = 44 + key.size()
// ---------------------------------------------------------------------------
struct Subscribe {
    int64_t  sessionId      = 0;
    int64_t  correlationId  = 0;
    int32_t  topicId        = 0;
    int32_t  partition      = 0;
    uint8_t  partitionKnown = 0;   // 1 => explicit partition; 0 => resolve from key
    int64_t  fromOffset     = 0;
    uint8_t  isolation      = 0;   // 0=read_uncommitted, 1=read_committed
    std::span<const uint8_t> key;  // zero-copy view; empty when partitionKnown==1

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::Subscribe); // 18
    static constexpr uint16_t BODY_FIXED  = 18; // topicId(4)+partition(4)+partitionKnown(1)+fromOffset(8)+isolation(1)

    /// Total encoded size accounting for the var field.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 2u + key.size();
    }

    /// Encode the full frame into buf.  buf must be ≥ encodedSize() bytes.
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
        detail::putLE32(buf, fixedBase + 0, topicId);
        detail::putLE32(buf, fixedBase + 4, partition);
        buf[fixedBase + 8] = partitionKnown;
        detail::putLE64(buf, fixedBase + 9, fromOffset);
        buf[fixedBase + 17] = isolation;

        std::size_t pos = static_cast<std::size_t>(fixedBase) + BODY_FIXED;

        // key var field
        pos += putVarBytes(buf + pos,
                           key.data(),
                           static_cast<uint16_t>(key.size()));
        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    /// The returned span is a zero-copy view into buf — buf must outlive the result.
    static std::optional<Subscribe> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        Subscribe msg;
        msg.sessionId      = hdr.sessionId;
        msg.correlationId  = hdr.correlationId;
        msg.topicId        = detail::getLE32(buf, static_cast<int>(fixedBase) + 0);
        msg.partition      = detail::getLE32(buf, static_cast<int>(fixedBase) + 4);
        msg.partitionKnown = buf[fixedBase + 8];
        msg.fromOffset     = detail::getLE64(buf, static_cast<int>(fixedBase) + 9);
        msg.isolation      = buf[fixedBase + 17];

        std::size_t pos = fixedBase + BODY_FIXED;

        // key var field
        const uint8_t* keyData = nullptr;
        uint16_t       keyLen  = 0;
        if (!getVarBytes(buf, avail, pos, keyData, keyLen)) return std::nullopt;
        msg.key = std::span<const uint8_t>(keyData, keyLen);

        return msg;
    }
};

// ---------------------------------------------------------------------------
// SubscribeResponse  (templateId 19)  broker→client
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (14)
//     templateId  = 19
//   [24..25]  errorCode      : int16   (ErrorCode value; 0 = OK)
//   [26..29]  partition      : int32   (assigned/confirmed partition)
//   [30..37]  subscriptionId : int64
//   BODY_FIXED = 14 (2+4+8)
//
// Total = 24 + 14 = 38 bytes (no var fields)
// ---------------------------------------------------------------------------
struct SubscribeResponse {
    int64_t sessionId      = 0;
    int64_t correlationId  = 0;
    int16_t errorCode      = 0;   // rune::protocol::client::ErrorCode value
    int32_t partition      = 0;
    int64_t subscriptionId = 0;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::SubscribeResponse); // 19
    static constexpr uint16_t BODY_FIXED  = 14; // errorCode(2)+partition(4)+subscriptionId(8)

    static constexpr std::size_t ENCODED_SIZE = MessageHeader::ENCODED_SIZE + BODY_FIXED; // 38

    /// Encode the full frame (38 bytes) into buf.  Returns 38.
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
        detail::putLE16s(buf, fixedBase + 0, errorCode);
        detail::putLE32 (buf, fixedBase + 2, partition);
        detail::putLE64 (buf, fixedBase + 6, subscriptionId);

        return ENCODED_SIZE;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<SubscribeResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE) return std::nullopt;

        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        SubscribeResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        msg.errorCode      = detail::getLE16s(buf, fixedBase + 0);
        msg.partition      = detail::getLE32 (buf, fixedBase + 2);
        msg.subscriptionId = detail::getLE64 (buf, fixedBase + 6);

        return msg;
    }
};

// ---------------------------------------------------------------------------
// Records  (templateId 20)  broker→client PUSH
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (24)
//     templateId  = 20
//   [24..31]  subscriptionId : int64
//   [32..35]  topicId        : int32
//   [36..39]  partition      : int32
//   [40..47]  baseOffset     : int64
//   BODY_FIXED = 24 (8+4+4+8), var field starts at offset 48 (header 24 + 24)
//   [48..49]  recordBatch length : uint16  (var prefix)
//   [50..]    recordBatch bytes   (opaque — never parsed by the codec)
//
// Total = 24 + 24 + 2 + recordBatch.size()
//       = 50 + recordBatch.size()
// ---------------------------------------------------------------------------
struct Records {
    int64_t sessionId      = 0;
    int64_t correlationId  = 0;
    int64_t subscriptionId = 0;
    int32_t topicId        = 0;
    int32_t partition      = 0;
    int64_t baseOffset     = 0;
    std::span<const uint8_t> recordBatch; // opaque blob; zero-copy view

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::Records); // 20
    static constexpr uint16_t BODY_FIXED  = 24; // subscriptionId(8)+topicId(4)+partition(4)+baseOffset(8)

    /// Total encoded size accounting for the var field.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 2u + recordBatch.size();
    }

    /// Encode the full frame into buf.  buf must be ≥ encodedSize() bytes.
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
        detail::putLE64(buf, fixedBase + 0,  subscriptionId);
        detail::putLE32(buf, fixedBase + 8,  topicId);
        detail::putLE32(buf, fixedBase + 12, partition);
        detail::putLE64(buf, fixedBase + 16, baseOffset);

        std::size_t pos = static_cast<std::size_t>(fixedBase) + BODY_FIXED;

        // recordBatch var field
        pos += putVarBytes(buf + pos,
                           recordBatch.data(),
                           static_cast<uint16_t>(recordBatch.size()));
        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    /// The returned span is a zero-copy view into buf — buf must outlive the result.
    static std::optional<Records> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        Records msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        msg.subscriptionId = detail::getLE64(buf, static_cast<int>(fixedBase) + 0);
        msg.topicId        = detail::getLE32(buf, static_cast<int>(fixedBase) + 8);
        msg.partition      = detail::getLE32(buf, static_cast<int>(fixedBase) + 12);
        msg.baseOffset     = detail::getLE64(buf, static_cast<int>(fixedBase) + 16);

        std::size_t pos = fixedBase + BODY_FIXED;

        // recordBatch var field
        const uint8_t* rbData = nullptr;
        uint16_t       rbLen  = 0;
        if (!getVarBytes(buf, avail, pos, rbData, rbLen)) return std::nullopt;
        msg.recordBatch = std::span<const uint8_t>(rbData, rbLen);

        return msg;
    }
};

// ---------------------------------------------------------------------------
// Unsubscribe  (templateId 21)  client→broker
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (8)
//     templateId  = 21
//   [24..31]  subscriptionId : int64
//   BODY_FIXED = 8
//
// Total = 24 + 8 = 32 bytes (no var fields)
// ---------------------------------------------------------------------------
struct Unsubscribe {
    int64_t sessionId      = 0;
    int64_t correlationId  = 0;
    int64_t subscriptionId = 0;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::Unsubscribe); // 21
    static constexpr uint16_t BODY_FIXED  = 8; // subscriptionId(8)

    static constexpr std::size_t ENCODED_SIZE = MessageHeader::ENCODED_SIZE + BODY_FIXED; // 32

    /// Encode the full frame (32 bytes) into buf.  Returns 32.
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
        detail::putLE64(buf, fixedBase + 0, subscriptionId);

        return ENCODED_SIZE;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<Unsubscribe> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE) return std::nullopt;

        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        Unsubscribe msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        msg.subscriptionId = detail::getLE64(buf, fixedBase + 0);

        return msg;
    }
};

} // namespace rune::protocol::client
