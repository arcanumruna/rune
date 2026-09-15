#pragma once
// OffsetMessages.h — Offset commit/fetch protocol messages (CG-1, templateIds 40–43).
//
// Frame layout convention (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (message-specific)
//     templateId  = <message-specific>
//   [24..24+BODY_FIXED-1]   fixed-width fields
//   [24+BODY_FIXED..]       var fields + array fields
//
// groupId and memberId are encoded as uint16 length-prefixed var bytes.
// Entry arrays are encoded as a uint16 count prefix followed by fixed-stride
// or variable-stride elements.  Per-entry metadata fields use the same
// uint16 length-prefix convention.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>
#include "Codec.h"
#include "GroupMessages.h"   // for TopicPartition32
#include "MessageHeader.h"
#include "Protocol.h"

namespace rune::protocol::client {

// ---------------------------------------------------------------------------
// OffsetCommitEntry — one entry within an OffsetCommit request.
//   topicId(int32) + partition(int32) + offset(int64) + metadata(var bytes)
// Wire stride per entry: 4 + 4 + 8 + 2 + metadata.size() (variable)
// ---------------------------------------------------------------------------
struct OffsetCommitEntry {
    int32_t topicId   = 0;
    int32_t partition = 0;
    int64_t offset    = 0;
    std::vector<uint8_t> metadata;  // opaque client metadata; may be empty
};

// ---------------------------------------------------------------------------
// OffsetCommit  (templateId 40)  client→broker
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (4)
//     templateId  = 40
//   [24..27]  generationId : int32
//   BODY_FIXED = 4, var/array fields start at offset 28
//   [28..29]  groupId length  : uint16
//   [30..]    groupId bytes
//   [..+2]    memberId length : uint16
//   [..+memberId.size()]  memberId bytes
//   [..+2]    entries count   : uint16
//   per entry:
//     topicId(int32) + partition(int32) + offset(int64) +
//     metadata length(uint16) + metadata bytes
//
// Total = 24 + 4 + 2 + groupId.size() + 2 + memberId.size() + 2 +
//         sum_entries(4 + 4 + 8 + 2 + metadata.size())
// ---------------------------------------------------------------------------
struct OffsetCommit {
    int64_t  sessionId     = 0;
    int64_t  correlationId = 0;
    int32_t  generationId  = 0;
    std::span<const uint8_t> groupId;   // zero-copy view
    std::span<const uint8_t> memberId;  // zero-copy view
    std::vector<OffsetCommitEntry> entries;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::OffsetCommit); // 40
    static constexpr uint16_t BODY_FIXED  = 4;  // generationId(4)

    /// Total encoded size.
    std::size_t encodedSize() const noexcept {
        std::size_t entriesSize = 0;
        for (const auto& e : entries) {
            entriesSize += 4u + 4u + 8u + 2u + e.metadata.size(); // topicId+partition+offset+metaLen+meta
        }
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 2u + groupId.size()
             + 2u + memberId.size()
             + 2u + entriesSize;
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
        detail::putLE32(buf, fixedBase + 0, generationId);

        std::size_t pos = static_cast<std::size_t>(fixedBase) + BODY_FIXED;

        pos += putVarBytes(buf + pos, groupId.data(),  static_cast<uint16_t>(groupId.size()));
        pos += putVarBytes(buf + pos, memberId.data(), static_cast<uint16_t>(memberId.size()));

        // entries array: uint16 count + variable-stride entries
        detail::putLE16(buf + pos, 0, static_cast<uint16_t>(entries.size()));
        pos += 2;
        for (const auto& e : entries) {
            detail::putLE32(buf, static_cast<int>(pos),      e.topicId);
            detail::putLE32(buf, static_cast<int>(pos) + 4,  e.partition);
            detail::putLE64(buf, static_cast<int>(pos) + 8,  e.offset);
            pos += 16;
            // metadata var field
            pos += putVarBytes(buf + pos,
                               e.metadata.data(),
                               static_cast<uint16_t>(e.metadata.size()));
        }

        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    /// groupId and memberId are zero-copy spans into buf — buf must outlive the result.
    static std::optional<OffsetCommit> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        OffsetCommit msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        msg.generationId  = detail::getLE32(buf, static_cast<int>(fixedBase) + 0);

        std::size_t pos = fixedBase + BODY_FIXED;

        const uint8_t* groupIdData = nullptr;
        uint16_t       groupIdLen  = 0;
        if (!getVarBytes(buf, avail, pos, groupIdData, groupIdLen)) return std::nullopt;
        msg.groupId = std::span<const uint8_t>(groupIdData, groupIdLen);

        const uint8_t* memberIdData = nullptr;
        uint16_t       memberIdLen  = 0;
        if (!getVarBytes(buf, avail, pos, memberIdData, memberIdLen)) return std::nullopt;
        msg.memberId = std::span<const uint8_t>(memberIdData, memberIdLen);

        // entries array
        if (pos + 2 > avail) return std::nullopt;
        const uint16_t count = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;

        msg.entries.resize(count);
        for (uint16_t i = 0; i < count; ++i) {
            // Need at least 16 bytes (topicId + partition + offset) before metadata
            if (pos + 16 > avail) return std::nullopt;
            msg.entries[i].topicId   = detail::getLE32(buf, static_cast<int>(pos));
            msg.entries[i].partition = detail::getLE32(buf, static_cast<int>(pos) + 4);
            msg.entries[i].offset    = detail::getLE64(buf, static_cast<int>(pos) + 8);
            pos += 16;

            // metadata var field
            const uint8_t* metaData = nullptr;
            uint16_t       metaLen  = 0;
            if (!getVarBytes(buf, avail, pos, metaData, metaLen)) return std::nullopt;
            msg.entries[i].metadata.assign(metaData, metaData + metaLen);
        }

        return msg;
    }
};

// ---------------------------------------------------------------------------
// OffsetCommitResult — one result within an OffsetCommitResponse.
//   topicId(int32) + partition(int32) + errorCode(int16)
// Wire stride per entry: 4 + 4 + 2 = 10 bytes (fixed)
// ---------------------------------------------------------------------------
struct OffsetCommitResult {
    int32_t topicId   = 0;
    int32_t partition = 0;
    int16_t errorCode = 0;
};

// ---------------------------------------------------------------------------
// OffsetCommitResponse  (templateId 41)  broker→client
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (0)
//     templateId  = 41
//   BODY_FIXED = 0, array starts at offset 24
//   [24..25]  results count : uint16
//   per result (10 bytes each):
//     topicId(int32) + partition(int32) + errorCode(int16)
//
// Total = 24 + 2 + 10*results.size()
// ---------------------------------------------------------------------------
struct OffsetCommitResponse {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    std::vector<OffsetCommitResult> results;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::OffsetCommitResponse); // 41
    static constexpr uint16_t BODY_FIXED  = 0;  // no fixed body beyond header

    /// Total encoded size.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 2u + 10u * results.size();
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

        std::size_t pos = MessageHeader::ENCODED_SIZE + BODY_FIXED;

        // results array: uint16 count + 10-byte entries
        detail::putLE16(buf + pos, 0, static_cast<uint16_t>(results.size()));
        pos += 2;
        for (const auto& r : results) {
            detail::putLE32(buf, static_cast<int>(pos),     r.topicId);
            detail::putLE32(buf, static_cast<int>(pos) + 4, r.partition);
            detail::putLE16s(buf, static_cast<int>(pos) + 8, r.errorCode);
            pos += 10;
        }

        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<OffsetCommitResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        // BODY_FIXED = 0: array starts right after header
        std::size_t pos = MessageHeader::ENCODED_SIZE;

        OffsetCommitResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        if (pos + 2 > avail) return std::nullopt;
        const uint16_t count = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;
        if (pos + static_cast<std::size_t>(count) * 10u > avail) return std::nullopt;
        msg.results.resize(count);
        for (uint16_t i = 0; i < count; ++i) {
            msg.results[i].topicId   = detail::getLE32 (buf, static_cast<int>(pos));
            msg.results[i].partition = detail::getLE32 (buf, static_cast<int>(pos) + 4);
            msg.results[i].errorCode = detail::getLE16s(buf, static_cast<int>(pos) + 8);
            pos += 10;
        }

        return msg;
    }
};

// ---------------------------------------------------------------------------
// OffsetFetchEntry — one entry within an OffsetFetch request.
//   topicId(int32) + partition(int32) — fixed 8 bytes
// ---------------------------------------------------------------------------
struct OffsetFetchEntry {
    int32_t topicId   = 0;
    int32_t partition = 0;
};

// ---------------------------------------------------------------------------
// OffsetFetch  (templateId 42)  client→broker
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (0)
//     templateId  = 42
//   BODY_FIXED = 0, var/array fields start at offset 24
//   [24..25]  groupId length : uint16
//   [26..]    groupId bytes
//   [..+2]    entries count  : uint16   (0 = fetch all committed offsets)
//   per entry (8 bytes each):
//     topicId(int32) + partition(int32)
//
// Total = 24 + 2 + groupId.size() + 2 + 8*entries.size()
// An empty entries list (count=0) means "fetch all committed offsets".
// ---------------------------------------------------------------------------
struct OffsetFetch {
    int64_t  sessionId     = 0;
    int64_t  correlationId = 0;
    std::span<const uint8_t> groupId;  // zero-copy view
    std::vector<OffsetFetchEntry> entries;  // empty = fetch all

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::OffsetFetch); // 42
    static constexpr uint16_t BODY_FIXED  = 0;  // no fixed body beyond header

    /// Total encoded size.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 2u + groupId.size()
             + 2u + 8u * entries.size();
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

        std::size_t pos = MessageHeader::ENCODED_SIZE + BODY_FIXED;

        pos += putVarBytes(buf + pos, groupId.data(), static_cast<uint16_t>(groupId.size()));

        // entries array: uint16 count + 8-byte entries
        detail::putLE16(buf + pos, 0, static_cast<uint16_t>(entries.size()));
        pos += 2;
        for (const auto& e : entries) {
            detail::putLE32(buf, static_cast<int>(pos),     e.topicId);
            detail::putLE32(buf, static_cast<int>(pos) + 4, e.partition);
            pos += 8;
        }

        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    /// groupId is a zero-copy span into buf — buf must outlive the result.
    static std::optional<OffsetFetch> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        std::size_t pos = MessageHeader::ENCODED_SIZE;

        OffsetFetch msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        const uint8_t* groupIdData = nullptr;
        uint16_t       groupIdLen  = 0;
        if (!getVarBytes(buf, avail, pos, groupIdData, groupIdLen)) return std::nullopt;
        msg.groupId = std::span<const uint8_t>(groupIdData, groupIdLen);

        // entries array
        if (pos + 2 > avail) return std::nullopt;
        const uint16_t count = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;
        if (pos + static_cast<std::size_t>(count) * 8u > avail) return std::nullopt;
        msg.entries.resize(count);
        for (uint16_t i = 0; i < count; ++i) {
            msg.entries[i].topicId   = detail::getLE32(buf, static_cast<int>(pos));
            msg.entries[i].partition = detail::getLE32(buf, static_cast<int>(pos) + 4);
            pos += 8;
        }

        return msg;
    }
};

// ---------------------------------------------------------------------------
// OffsetFetchResult — one result within an OffsetFetchResponse.
//   topicId(int32) + partition(int32) + offset(int64) + errorCode(int16)
// Wire stride: 4 + 4 + 8 + 2 = 18 bytes (fixed)
// ---------------------------------------------------------------------------
struct OffsetFetchResult {
    int32_t topicId   = 0;
    int32_t partition = 0;
    int64_t offset    = 0;
    int16_t errorCode = 0;
};

// ---------------------------------------------------------------------------
// OffsetFetchResponse  (templateId 43)  broker→client
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (0)
//     templateId  = 43
//   BODY_FIXED = 0, array starts at offset 24
//   [24..25]  results count : uint16
//   per result (18 bytes each):
//     topicId(int32) + partition(int32) + offset(int64) + errorCode(int16)
//
// Total = 24 + 2 + 18*results.size()
// ---------------------------------------------------------------------------
struct OffsetFetchResponse {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    std::vector<OffsetFetchResult> results;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::OffsetFetchResponse); // 43
    static constexpr uint16_t BODY_FIXED  = 0;  // no fixed body beyond header

    /// Total encoded size.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 2u + 18u * results.size();
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

        std::size_t pos = MessageHeader::ENCODED_SIZE + BODY_FIXED;

        // results array: uint16 count + 18-byte entries
        detail::putLE16(buf + pos, 0, static_cast<uint16_t>(results.size()));
        pos += 2;
        for (const auto& r : results) {
            detail::putLE32 (buf, static_cast<int>(pos),      r.topicId);
            detail::putLE32 (buf, static_cast<int>(pos) + 4,  r.partition);
            detail::putLE64 (buf, static_cast<int>(pos) + 8,  r.offset);
            detail::putLE16s(buf, static_cast<int>(pos) + 16, r.errorCode);
            pos += 18;
        }

        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<OffsetFetchResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        // BODY_FIXED = 0: array starts right after header
        std::size_t pos = MessageHeader::ENCODED_SIZE;

        OffsetFetchResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        if (pos + 2 > avail) return std::nullopt;
        const uint16_t count = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;
        if (pos + static_cast<std::size_t>(count) * 18u > avail) return std::nullopt;
        msg.results.resize(count);
        for (uint16_t i = 0; i < count; ++i) {
            msg.results[i].topicId   = detail::getLE32 (buf, static_cast<int>(pos));
            msg.results[i].partition = detail::getLE32 (buf, static_cast<int>(pos) + 4);
            msg.results[i].offset    = detail::getLE64 (buf, static_cast<int>(pos) + 8);
            msg.results[i].errorCode = detail::getLE16s(buf, static_cast<int>(pos) + 16);
            pos += 18;
        }

        return msg;
    }
};

} // namespace rune::protocol::client
