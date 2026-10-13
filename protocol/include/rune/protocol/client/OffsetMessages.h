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
//
// Lifetime contract:
//   - All span and view fields are zero-copy into the caller-supplied buffer.
//   - buf MUST outlive every decoded result.  No heap allocation on decode.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include "ArrayView.h"
#include "Codec.h"
#include "GroupMessages.h"   // for TopicPartition32
#include "MessageHeader.h"
#include "Protocol.h"

namespace rune::protocol::client {

// ---------------------------------------------------------------------------
// OffsetCommitEntry — one entry within an OffsetCommit request (encode path).
//   topicId(int32) + partition(int32) + offset(int64) + metadata(var bytes)
// Wire stride per entry: 4 + 4 + 8 + 2 + metadata.size() (variable)
//
// metadata is a zero-copy span — the span must remain valid for the duration
// of the encode() call.
// ---------------------------------------------------------------------------
struct OffsetCommitEntry {
    int32_t topicId   = 0;
    int32_t partition = 0;
    int64_t offset    = 0;
    std::span<const uint8_t> metadata;  // zero-copy; may be empty
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

    // Decoded: zero-copy forward-iterable view — buf must outlive this.
    // Iterate with range-for; each element is an OffsetCommitEntryRef with zero-copy metadata.
    OffsetCommitEntriesView entries;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::OffsetCommit); // 40
    static constexpr uint16_t BODY_FIXED  = 4;  // generationId(4)

    /// Total encoded size from a span of OffsetCommitEntry structs.
    static std::size_t encodedSizeFor(std::span<const uint8_t> grp,
                                      std::span<const uint8_t> mbr,
                                      std::span<const OffsetCommitEntry> entrs) noexcept {
        std::size_t entriesSize = 0;
        for (const auto& e : entrs)
            entriesSize += 4u + 4u + 8u + 2u + e.metadata.size();
        return MessageHeader::ENCODED_SIZE + BODY_FIXED
             + 2u + grp.size()
             + 2u + mbr.size()
             + 2u + entriesSize;
    }

    /// Encode the full frame into buf.  entrs is a span of OffsetCommitEntry.
    /// buf must be >= encodedSizeFor(groupId, memberId, entrs) bytes.
    /// Returns total bytes written.
    std::size_t encode(uint8_t* buf,
                       std::span<const OffsetCommitEntry> entrs) const noexcept {
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
        detail::putLE16(buf + pos, 0, static_cast<uint16_t>(entrs.size()));
        pos += 2;
        for (const auto& e : entrs) {
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
    /// groupId, memberId, and all entry metadata are zero-copy spans into buf —
    /// buf must outlive the decoded result.  No heap allocation.
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

        // entries array — variable stride; validate count * minEntrySize first.
        if (pos + 2 > avail) return std::nullopt;
        const uint16_t count = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;
        // Minimum entry size = 18 bytes (topicId4 + partition4 + offset8 + metaLen2 + empty metadata).
        if (pos + static_cast<std::size_t>(count) * 18u > avail) return std::nullopt;

        // Build a zero-copy view (no allocation).
        msg.entries = OffsetCommitEntriesView(buf, pos, avail, count);

        return msg;
    }
};

// ---------------------------------------------------------------------------
// OffsetCommitResult — one result within an OffsetCommitResponse (encode path).
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

    // Decoded: zero-copy fixed-stride view — buf must outlive this.
    // Access: results[i].topicId, results[i].partition, results[i].errorCode
    OffsetCommitResultView results;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::OffsetCommitResponse); // 41
    static constexpr uint16_t BODY_FIXED  = 0;  // no fixed body beyond header

    /// Encoded size for a given result count.
    static std::size_t encodedSizeFor(std::size_t count) noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 2u + 10u * count;
    }

    /// Encode the full frame into buf.  resSlice is a span of OffsetCommitResult.
    /// buf must be >= encodedSizeFor(resSlice.size()) bytes.
    /// Returns total bytes written.
    std::size_t encode(uint8_t* buf,
                       std::span<const OffsetCommitResult> resSlice) const noexcept {
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
        detail::putLE16(buf + pos, 0, static_cast<uint16_t>(resSlice.size()));
        pos += 2;
        for (const auto& r : resSlice) {
            detail::putLE32(buf, static_cast<int>(pos),     r.topicId);
            detail::putLE32(buf, static_cast<int>(pos) + 4, r.partition);
            detail::putLE16s(buf, static_cast<int>(pos) + 8, r.errorCode);
            pos += 10;
        }

        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    /// results is a zero-copy view into buf — buf must outlive the result.
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
        msg.results = OffsetCommitResultView(buf + pos, count);

        return msg;
    }
};

// ---------------------------------------------------------------------------
// OffsetFetchEntry — one entry within an OffsetFetch request (encode path).
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

    // Decoded: zero-copy fixed-stride view — buf must outlive this.
    // Empty (size()==0) means "fetch all".
    OffsetFetchEntryView entries;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::OffsetFetch); // 42
    static constexpr uint16_t BODY_FIXED  = 0;  // no fixed body beyond header

    /// Encoded size for given groupId + entry count.
    static std::size_t encodedSizeFor(std::span<const uint8_t> grp,
                                      std::size_t entryCount) noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED
             + 2u + grp.size()
             + 2u + 8u * entryCount;
    }

    /// Encode the full frame into buf.  entrs is a span of OffsetFetchEntry.
    /// buf must be >= encodedSizeFor(groupId, entrs.size()) bytes.
    /// Returns total bytes written.
    std::size_t encode(uint8_t* buf,
                       std::span<const OffsetFetchEntry> entrs) const noexcept {
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
        detail::putLE16(buf + pos, 0, static_cast<uint16_t>(entrs.size()));
        pos += 2;
        for (const auto& e : entrs) {
            detail::putLE32(buf, static_cast<int>(pos),     e.topicId);
            detail::putLE32(buf, static_cast<int>(pos) + 4, e.partition);
            pos += 8;
        }

        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    /// groupId and entries are zero-copy views into buf — buf must outlive the result.
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

        // entries array (zero-copy view)
        if (pos + 2 > avail) return std::nullopt;
        const uint16_t count = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;
        if (pos + static_cast<std::size_t>(count) * 8u > avail) return std::nullopt;
        msg.entries = OffsetFetchEntryView(buf + pos, count);

        return msg;
    }
};

// ---------------------------------------------------------------------------
// OffsetFetchResult — one result within an OffsetFetchResponse (encode path).
//   topicId(int32) + partition(int32) + offset(int64) + errorCode(int16)
// Wire stride: 4 + 4 + 8 + 2 = 18 bytes (fixed)
// offset -1 = no committed offset (the member applies its reset policy).
// Phase 2b E2 (final fix wave C1): a broker also answers -1 (errorCode OK) for
// a TRANSIENT partition whose committed offset belongs to data that did not
// survive (a restart emptied the ring): an offset is a data position, and
// resuming at it would skip the refilled records.  A fetch-all response omits
// such an entry.  No format change.
// (E2 final fix wave, C1 multi-node) The broker knows a transient partition's
// data LINEAGE from the cluster metadata, confirmed per leadership.  Until the
// partition's current leadership has confirmed it (briefly after a restart or
// a leader move), an OffsetFetch entry is answered errorCode NotLeader (2),
// offset -1, and an OffsetCommit entry NotLeader: neither the offset nor
// "none" can be answered yet.  NotLeader is the existing RETRIABLE code — a
// client re-fetches / re-commits after a backoff, within its budget.
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

    // Decoded: zero-copy fixed-stride view — buf must outlive this.
    OffsetFetchResultView results;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::OffsetFetchResponse); // 43
    static constexpr uint16_t BODY_FIXED  = 0;  // no fixed body beyond header

    /// Encoded size for a given result count.
    static std::size_t encodedSizeFor(std::size_t count) noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 2u + 18u * count;
    }

    /// Encode the full frame into buf.  resSlice is a span of OffsetFetchResult.
    /// buf must be >= encodedSizeFor(resSlice.size()) bytes.
    /// Returns total bytes written.
    std::size_t encode(uint8_t* buf,
                       std::span<const OffsetFetchResult> resSlice) const noexcept {
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
        detail::putLE16(buf + pos, 0, static_cast<uint16_t>(resSlice.size()));
        pos += 2;
        for (const auto& r : resSlice) {
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
    /// results is a zero-copy view into buf — buf must outlive the result.
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
        msg.results = OffsetFetchResultView(buf + pos, count);

        return msg;
    }
};

} // namespace rune::protocol::client
