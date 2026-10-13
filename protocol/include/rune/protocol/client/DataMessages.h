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
//     blockLength = BODY_FIXED (24)
//     templateId  = 16
//   [24..27]  topicId         : int32
//   [28..31]  partition       : int32   (used when partitionKnown==1; else ignore)
//   [32]      partitionKnown  : uint8   (1 => use partition; 0 => broker resolves from key)
//   [33]      acks            : uint8   (0 = Leader/None, 1 = All — GR-5)
//   [34..41]  producerId      : int64   (-1 = non-idempotent legacy produce)
//   [42..43]  producerEpoch   : int16
//   [44..47]  baseSequence    : int32   (-1 = non-idempotent)
//   BODY_FIXED = 24, so var fields start at offset 48 (header 24 + 24)
//   [48..49]  key length      : uint16  (var prefix; may be 0 when partitionKnown==1)
//   [50..]    key bytes
//   [..+2]    recordBatch length : uint16  (var prefix)
//   [..+recordBatch.size()]  recordBatch bytes   (opaque — never parsed by the codec)
//
// Backward compatibility:
//   BODY_FIXED_V0 = 9  (old clients without the acks byte)
//   BODY_FIXED_V1 = 10 (acks byte, no idempotent fields)
//   BODY_FIXED    = 24 (TX-1: adds producerId/producerEpoch/baseSequence)
// Decoders that read only the prefix still parse via the blockLength-based skip.
// Sentinel producerId == -1 means non-idempotent (legacy) produce.
//
// Note: the u16 length prefix caps a single var field at 65535 bytes.
// Producing batches larger than 65535 bytes requires multi-frame framing,
// which is deferred to a later milestone.
//
// Total = 24 + 24 + 2 + key.size() + 2 + recordBatch.size()
//       = 52 + key.size() + recordBatch.size()
// ---------------------------------------------------------------------------
struct Produce {
    int64_t  sessionId      = 0;
    int64_t  correlationId  = 0;
    int32_t  topicId        = 0;
    int32_t  partition      = 0;
    uint8_t  partitionKnown = 0;    // 1 => explicit partition; 0 => resolve from key
    uint8_t  acks           = 0;    // 0 = Leader/None (immediate ack), 1 = All (deferred until ISR HW)
    int64_t  producerId     = -1;   // -1 = non-idempotent legacy produce (TX-1 sentinel)
    int16_t  producerEpoch  = 0;
    int32_t  baseSequence   = -1;   // -1 = non-idempotent
    std::span<const uint8_t> key;          // zero-copy view; may be empty
    std::span<const uint8_t> recordBatch;  // opaque blob; zero-copy view

    static constexpr uint16_t TEMPLATE_ID   = static_cast<uint16_t>(TemplateId::Produce); // 16
    static constexpr uint16_t BODY_FIXED    = 24; // topicId(4)+partition(4)+partitionKnown(1)+acks(1)+producerId(8)+producerEpoch(2)+baseSequence(4)
    static constexpr uint16_t BODY_FIXED_V1 = 10; // acks byte, no idempotent fields
    static constexpr uint16_t BODY_FIXED_V0 = 9;  // old clients without the acks byte

    /// Total encoded size accounting for the two var fields.
    /// key uses a u16 length prefix (≤65535); recordBatch uses a u32 prefix so a
    /// single Produce can carry Kafka-scale batches (≫64KB) in one frame.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 2u + key.size()
             + 4u + recordBatch.size();
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
        detail::putLE32(buf, fixedBase + 0,  topicId);
        detail::putLE32(buf, fixedBase + 4,  partition);
        buf[fixedBase + 8] = partitionKnown;
        buf[fixedBase + 9] = acks;
        detail::putLE64(buf, fixedBase + 10, producerId);
        detail::putLE16s(buf, fixedBase + 18, producerEpoch);
        detail::putLE32(buf, fixedBase + 20, baseSequence);

        std::size_t pos = static_cast<std::size_t>(fixedBase) + BODY_FIXED;

        // key var field
        pos += putVarBytes(buf + pos,
                           key.data(),
                           static_cast<uint16_t>(key.size()));

        // recordBatch var field (u32 length prefix — may exceed 64KB)
        pos += putVarBytes32(buf + pos,
                             recordBatch.data(),
                             static_cast<uint32_t>(recordBatch.size()));

        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    /// The returned spans are zero-copy views into buf — buf must outlive the result.
    /// Backward compat: accepts BODY_FIXED_V0==9, BODY_FIXED_V1==10, and BODY_FIXED==24.
    /// Unknown extra fixed bytes are skipped via blockLength (forward-compat).
    static std::optional<Produce> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

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
        // acks field only present in v1+ (declaredFixed >= 10)
        msg.acks = (declaredFixed >= BODY_FIXED_V1) ? buf[fixedBase + 9] : uint8_t{0};
        // No-hang wave: an unknown acks byte is rejected, never read as one of
        // the two modes (the gateway tested == 1, the worker != 0 — a 2 was a
        // silent durability downgrade on one path).  Answered InvalidRequest.
        if (msg.acks > 1) return std::nullopt;
        // idempotent fields only present in v2 (declaredFixed >= 24)
        if (declaredFixed >= BODY_FIXED) {
            msg.producerId    = detail::getLE64(buf, static_cast<int>(fixedBase) + 10);
            msg.producerEpoch = detail::getLE16s(buf, static_cast<int>(fixedBase) + 18);
            msg.baseSequence  = detail::getLE32(buf, static_cast<int>(fixedBase) + 20);
        }
        // else: producerId = -1, producerEpoch = 0, baseSequence = -1 (defaults = non-idempotent)

        // Var fields start after the declared fixed block (forward-compat: skip unknown fields).
        std::size_t pos = fixedBase + declaredFixed;

        // key var field
        const uint8_t* keyData = nullptr;
        uint16_t       keyLen  = 0;
        if (!getVarBytes(buf, avail, pos, keyData, keyLen)) return std::nullopt;
        msg.key = std::span<const uint8_t>(keyData, keyLen);

        // recordBatch var field (u32 length prefix — may exceed 64KB)
        const uint8_t* rbData = nullptr;
        uint32_t       rbLen  = 0;
        if (!getVarBytes32(buf, avail, pos, rbData, rbLen)) return std::nullopt;
        msg.recordBatch = std::span<const uint8_t>(rbData, rbLen);

        return msg;
    }
};

// ---------------------------------------------------------------------------
// ProduceResponse  (templateId 17)
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (22)
//     templateId  = 17
//   [24..25]  errorCode      : int16   (ErrorCode value; 0 = OK)
//   [26..29]  partition      : int32   (assigned/confirmed partition)
//   [30..37]  baseOffset     : int64   (base offset of the batch; ignore when errorCode != 0)
//   [38..45]  logStartOffset : int64   (D6, Kafka PartitionProduceResponse.logStartOffset:
//                                       the partition's log start; -1 = not carried.
//                                       Set with ErrorCode::UnknownProducerId)
//   BODY_FIXED = 22 (2 + 4 + 8 + 8)
//
// Backward compatibility (blockLength-versioned, additive — the header version
// is NOT bumped):
//   BODY_FIXED_V0 = 14 (pre-D6, no logStartOffset): decoded with
//   logStartOffset = -1.  A pre-D6 decoder reads fixed offsets 24..37 and
//   ignores the trailing 8 bytes, so a new frame decodes unchanged there.
//   A longer future block is skipped via blockLength (forward-compat).
//
// Total = 24 + 22 = 46 bytes (no var fields)
// ---------------------------------------------------------------------------
struct ProduceResponse {
    int64_t sessionId      = 0;
    int64_t correlationId  = 0;
    int16_t errorCode      = 0;   // rune::protocol::client::ErrorCode value
    int32_t partition      = 0;
    int64_t baseOffset     = 0;   // meaningful only when errorCode == 0
    int64_t logStartOffset = -1;  // D6: -1 = not carried (old broker / not set)

    static constexpr uint16_t TEMPLATE_ID   = static_cast<uint16_t>(TemplateId::ProduceResponse); // 17
    static constexpr uint16_t BODY_FIXED    = 22;  // errorCode(2) + partition(4) + baseOffset(8) + logStartOffset(8)
    static constexpr uint16_t BODY_FIXED_V0 = 14;  // pre-D6: errorCode(2) + partition(4) + baseOffset(8)

    static constexpr std::size_t ENCODED_SIZE    = MessageHeader::ENCODED_SIZE + BODY_FIXED;    // 46
    static constexpr std::size_t ENCODED_SIZE_V0 = MessageHeader::ENCODED_SIZE + BODY_FIXED_V0; // 38

    /// Encode the full frame (46 bytes) into buf.  Returns 46.
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
        detail::putLE16s(buf, fixedBase + 0,  errorCode);
        detail::putLE32 (buf, fixedBase + 2,  partition);
        detail::putLE64 (buf, fixedBase + 6,  baseOffset);
        detail::putLE64 (buf, fixedBase + 14, logStartOffset);

        return ENCODED_SIZE;
    }

    /// D6 review M4: the frame size a session that connected with
    /// `clientVersion` is sent — the pre-D6 38 B frame (blockLength 14) below
    /// kClientVersionUnknownProducerId, the 46 B frame from it on.
    [[nodiscard]] static constexpr std::size_t encodedSizeFor(int32_t clientVersion) noexcept {
        return clientUnderstandsUnknownProducerId(clientVersion) ? ENCODED_SIZE : ENCODED_SIZE_V0;
    }

    /// D6 review M4: encode the frame for a session that connected with
    /// `clientVersion` — every broker answer site uses this.  Below
    /// kClientVersionUnknownProducerId the frame is EXACTLY the pre-D6 one
    /// (38 B, blockLength 14, no logStartOffset) and UnknownProducerId is
    /// answered OutOfOrderSequence (produceErrorForClient), so even a strict
    /// pre-D6 decoder (exact length / exact blockLength) reads it.  From that
    /// version on it is the 46 B frame.  buf must hold encodedSizeFor(version).
    /// Returns the bytes written.
    std::size_t encodeFor(uint8_t* buf, int32_t clientVersion) const noexcept {
        const bool full = clientUnderstandsUnknownProducerId(clientVersion);
        MessageHeader hdr;
        hdr.blockLength   = full ? BODY_FIXED : BODY_FIXED_V0;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        detail::putLE16s(buf, fixedBase + 0, static_cast<int16_t>(produceErrorForClient(
                                                 static_cast<ErrorCode>(static_cast<uint16_t>(errorCode)),
                                                 clientVersion)));
        detail::putLE32 (buf, fixedBase + 2, partition);
        detail::putLE64 (buf, fixedBase + 6, baseOffset);
        if (!full) return ENCODED_SIZE_V0;
        detail::putLE64 (buf, fixedBase + 14, logStartOffset);
        return ENCODED_SIZE;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation (header + declared block), a declared
    /// block shorter than the v0 block, or a header mismatch.
    static std::optional<ProduceResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE_V0) return std::nullopt;

        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;
        if (hdr.blockLength < BODY_FIXED_V0) return std::nullopt;
        if (avail < MessageHeader::ENCODED_SIZE + hdr.blockLength) return std::nullopt;

        ProduceResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        msg.errorCode  = detail::getLE16s(buf, fixedBase + 0);
        msg.partition  = detail::getLE32 (buf, fixedBase + 2);
        msg.baseOffset = detail::getLE64 (buf, fixedBase + 6);
        if (hdr.blockLength >= BODY_FIXED)
            msg.logStartOffset = detail::getLE64(buf, fixedBase + 14);

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
//
// Catalog history (client version 8): Subscribe{topicId = kCatalogTopicId
// (−6), partition 0, partitionKnown 1} reads `__catalog` from ANY broker,
// served from that broker's replica up to its high watermark.  After the
// committed backlog the broker pushes ONE Records frame with baseOffset = its
// HW and an EMPTY records payload — the caught-up marker (catalog
// subscriptions only); live events follow as they commit.
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
        // No-hang wave: an unknown isolation byte is rejected, never read as
        // read_uncommitted (every consumer tests == read_committed).
        if (msg.isolation > 1) return std::nullopt;

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
//   [48..51]  recordBatch length : uint32  (var prefix — batches may exceed 64 KiB)
//   [52..]    recordBatch bytes   (opaque — never parsed by the codec)
//
// Total = 24 + 24 + 4 + recordBatch.size()
//       = 52 + recordBatch.size()
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
    /// recordBatch uses a u32 length prefix (may exceed 64KB).
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 4u + recordBatch.size();
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

        // recordBatch var field (u32 length prefix — may exceed 64KB)
        pos += putVarBytes32(buf + pos,
                             recordBatch.data(),
                             static_cast<uint32_t>(recordBatch.size()));
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

        // recordBatch var field (u32 length prefix — may exceed 64KB)
        const uint8_t* rbData = nullptr;
        uint32_t       rbLen  = 0;
        if (!getVarBytes32(buf, avail, pos, rbData, rbLen)) return std::nullopt;
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

// ---------------------------------------------------------------------------
// GetViewRow  (templateId 72)  client->broker — point lookup into a
// materialized view's output topic by key.
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (20)
//     templateId  = 72
//   [24..27]  viewTopicId          : int32
//   [28..35]  keyU64               : uint64
//   [36..43]  requestedFingerprint : uint64  (0 = latest; non-zero pins the
//                                              caller to a specific output
//                                              schema fingerprint)
//   BODY_FIXED = 20 (4+8+8)
//
// Total = 24 + 20 = 44 bytes (no var fields)
// ---------------------------------------------------------------------------
struct GetViewRow {
    int64_t  sessionId           = 0;
    int64_t  correlationId       = 0;
    int32_t  viewTopicId         = -1;
    uint64_t keyU64              = 0;
    uint64_t requestedFingerprint = 0;  // 0 = latest

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::GetViewRow); // 72
    static constexpr uint16_t BODY_FIXED  = 20; // viewTopicId(4)+keyU64(8)+requestedFingerprint(8)

    static constexpr std::size_t ENCODED_SIZE = MessageHeader::ENCODED_SIZE + BODY_FIXED; // 44

    /// Encode the full frame (44 bytes) into buf.  Returns 44.
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
        detail::putLE32(buf, fixedBase + 0,  viewTopicId);
        detail::putLE64(buf, fixedBase + 4,  static_cast<int64_t>(keyU64));
        detail::putLE64(buf, fixedBase + 12, static_cast<int64_t>(requestedFingerprint));

        return ENCODED_SIZE;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<GetViewRow> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE) return std::nullopt;

        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        GetViewRow msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        msg.viewTopicId          = detail::getLE32(buf, fixedBase + 0);
        msg.keyU64               = static_cast<uint64_t>(detail::getLE64(buf, fixedBase + 4));
        msg.requestedFingerprint = static_cast<uint64_t>(detail::getLE64(buf, fixedBase + 12));

        return msg;
    }
};

// ---------------------------------------------------------------------------
// ViewRowResponse  (templateId 73)  broker->client
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (6)
//     templateId  = 73
//   [24..25]  status   : uint16  (0=OK,1=AbsentKey,2=NotReady,3=SchemaMismatch,4=UnknownView)
//   [26..29]  rowLen   : uint32  (== rowBytes.size(); 0 when status != OK)
//   BODY_FIXED = 6 (2+4), var field starts at offset 30 (header 24 + 6)
//   [30..33]  rowBytes length : uint32  (var prefix)
//   [34..]    rowBytes bytes   (opaque row payload; empty when status != OK)
//
// Total = 24 + 6 + 4 + rowBytes.size() = 34 + rowBytes.size()
// ---------------------------------------------------------------------------
struct ViewRowResponse {
    int64_t sessionId      = 0;
    int64_t correlationId  = 0;
    uint16_t status        = 0;   // 0=OK,1=AbsentKey,2=NotReady,3=SchemaMismatch,4=UnknownView
    uint32_t rowLen        = 0;   // == rowBytes.size()
    const uint8_t* rowBytes = nullptr;
    uint32_t rowBytesLen    = 0;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::ViewRowResponse); // 73
    static constexpr uint16_t BODY_FIXED  = 6; // status(2)+rowLen(4)

    // Append-only.  5 = the GetViewRow body did not decode (non-retriable);
    // 6 = the owning worker's ingress ring was full this cycle (retriable —
    // the read was not performed; re-issue it); 7 = the row exists but its
    // ViewRowResponse would exceed the broker's egress frame (non-retriable;
    // no row bytes are carried).
    enum Status : uint16_t { kOk = 0, kAbsentKey = 1, kNotReady = 2, kSchemaMismatch = 3, kUnknownView = 4,
                             kInvalidRequest = 5, kBusy = 6, kRowTooLarge = 7 };

    /// Total encoded size accounting for the var field.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 4u + rowBytesLen;
    }

    /// Encode the full frame into buf.  buf must be >= encodedSize() bytes.
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
        detail::putLE16(buf, fixedBase + 0, status);
        detail::putLE32(buf, fixedBase + 2, static_cast<int32_t>(rowLen));

        std::size_t pos = static_cast<std::size_t>(fixedBase) + BODY_FIXED;
        pos += putVarBytes32(buf + pos, rowBytes, rowBytesLen);
        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    /// The returned span (rowBytes/rowBytesLen) is a zero-copy view into buf —
    /// buf must outlive the result.
    static std::optional<ViewRowResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        ViewRowResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        msg.status = detail::getLE16(buf, static_cast<int>(fixedBase) + 0);
        msg.rowLen = static_cast<uint32_t>(detail::getLE32(buf, static_cast<int>(fixedBase) + 2));

        std::size_t pos = fixedBase + BODY_FIXED;
        if (!getVarBytes32(buf, avail, pos, msg.rowBytes, msg.rowBytesLen)) return std::nullopt;

        return msg;
    }
};

} // namespace rune::protocol::client
