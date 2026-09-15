#pragma once
// GroupMessages.h — Consumer-group protocol messages (CG-1, templateIds 32–44).
//
// Frame layout convention (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (message-specific)
//     templateId  = <message-specific>
//   [24..24+BODY_FIXED-1]   fixed-width fields (int32, int64, uint8, int16)
//   [24+BODY_FIXED..]       var fields: groupId, memberId, etc. (uint16 length prefix)
//                           array fields: uint16 count prefix + fixed-stride elements
//
// All spans are zero-copy views into the caller-supplied buffer — buf must outlive
// the decoded result.  Array fields (subscribedTopicIds, assignment) are decoded
// into caller-owned vectors.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>
#include "Codec.h"
#include "MessageHeader.h"
#include "Protocol.h"

namespace rune::protocol::client {

// ---------------------------------------------------------------------------
// TopicPartition32 — shared helper for assignment/entry arrays.
// 8 bytes: topicId(int32) + partition(int32), little-endian.
// ---------------------------------------------------------------------------
struct TopicPartition32 {
    int32_t topicId   = 0;
    int32_t partition = 0;
};

// ---------------------------------------------------------------------------
// JoinGroup  (templateId 32)  client→broker
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (4)
//     templateId  = 32
//   [24..27]  sessionTimeoutMs : int32
//   BODY_FIXED = 4, var fields start at offset 28 (header 24 + 4)
//   [28..29]  groupId length    : uint16  (var prefix)
//   [30..]    groupId bytes
//   [..+2]    memberId length   : uint16  (empty = new member)
//   [..+memberId.size()]  memberId bytes
//   [..+2]    subscribedTopicIds count : uint16
//   [..+count*4]  subscribedTopicIds entries (each int32 LE)
//
// Total = 24 + 4 + 2 + groupId.size() + 2 + memberId.size() + 2 + 4*topicCount
// ---------------------------------------------------------------------------
struct JoinGroup {
    int64_t  sessionId        = 0;
    int64_t  correlationId    = 0;
    int32_t  sessionTimeoutMs = 0;
    std::span<const uint8_t> groupId;   // zero-copy view; must not be empty
    std::span<const uint8_t> memberId;  // zero-copy view; empty = new member
    std::vector<int32_t>     subscribedTopicIds;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::JoinGroup); // 32
    static constexpr uint16_t BODY_FIXED  = 4;  // sessionTimeoutMs(4)

    /// Total encoded size.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 2u + groupId.size()
             + 2u + memberId.size()
             + 2u + 4u * subscribedTopicIds.size();
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
        detail::putLE32(buf, fixedBase + 0, sessionTimeoutMs);

        std::size_t pos = static_cast<std::size_t>(fixedBase) + BODY_FIXED;

        // groupId var field
        pos += putVarBytes(buf + pos,
                           groupId.data(),
                           static_cast<uint16_t>(groupId.size()));

        // memberId var field (empty = new member)
        pos += putVarBytes(buf + pos,
                           memberId.data(),
                           static_cast<uint16_t>(memberId.size()));

        // subscribedTopicIds: uint16 count + int32 entries
        detail::putLE16(buf + pos, 0, static_cast<uint16_t>(subscribedTopicIds.size()));
        pos += 2;
        for (int32_t tid : subscribedTopicIds) {
            detail::putLE32(buf, static_cast<int>(pos), tid);
            pos += 4;
        }

        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    /// Spans are zero-copy views into buf — buf must outlive the result.
    static std::optional<JoinGroup> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        JoinGroup msg;
        msg.sessionId        = hdr.sessionId;
        msg.correlationId    = hdr.correlationId;
        msg.sessionTimeoutMs = detail::getLE32(buf, static_cast<int>(fixedBase) + 0);

        std::size_t pos = fixedBase + BODY_FIXED;

        // groupId var field
        const uint8_t* groupIdData = nullptr;
        uint16_t       groupIdLen  = 0;
        if (!getVarBytes(buf, avail, pos, groupIdData, groupIdLen)) return std::nullopt;
        msg.groupId = std::span<const uint8_t>(groupIdData, groupIdLen);

        // memberId var field
        const uint8_t* memberIdData = nullptr;
        uint16_t       memberIdLen  = 0;
        if (!getVarBytes(buf, avail, pos, memberIdData, memberIdLen)) return std::nullopt;
        msg.memberId = std::span<const uint8_t>(memberIdData, memberIdLen);

        // subscribedTopicIds: uint16 count + int32 entries
        if (pos + 2 > avail) return std::nullopt;
        const uint16_t count = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;
        if (pos + static_cast<std::size_t>(count) * 4u > avail) return std::nullopt;
        msg.subscribedTopicIds.resize(count);
        for (uint16_t i = 0; i < count; ++i) {
            msg.subscribedTopicIds[i] = detail::getLE32(buf, static_cast<int>(pos));
            pos += 4;
        }

        return msg;
    }
};

// ---------------------------------------------------------------------------
// JoinGroupResponse  (templateId 33)  broker→client
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (6)
//     templateId  = 33
//   [24..27]  generationId : int32
//   [28]      isLeader     : uint8   (1 = this member is the group leader)
//   [29..30]  errorCode    : int16
//   BODY_FIXED = 7 (int32 + uint8 + int16), var fields start at offset 31
//   [31..32]  memberId length : uint16
//   [33..]    memberId bytes
//
// Total = 24 + 7 + 2 + memberId.size()
// ---------------------------------------------------------------------------
struct JoinGroupResponse {
    int64_t  sessionId     = 0;
    int64_t  correlationId = 0;
    std::span<const uint8_t> memberId;  // zero-copy view; assigned memberId
    int32_t  generationId  = 0;
    uint8_t  isLeader      = 0;         // 1 = this member is the group leader
    int16_t  errorCode     = 0;         // ErrorCode value; 0 = OK

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::JoinGroupResponse); // 33
    static constexpr uint16_t BODY_FIXED  = 7;  // generationId(4) + isLeader(1) + errorCode(2)

    /// Total encoded size.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 2u + memberId.size();
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
        buf[fixedBase + 4] = isLeader;
        detail::putLE16s(buf, fixedBase + 5, errorCode);

        std::size_t pos = static_cast<std::size_t>(fixedBase) + BODY_FIXED;

        // memberId var field
        pos += putVarBytes(buf + pos,
                           memberId.data(),
                           static_cast<uint16_t>(memberId.size()));
        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<JoinGroupResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        JoinGroupResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        msg.generationId  = detail::getLE32(buf, static_cast<int>(fixedBase) + 0);
        msg.isLeader      = buf[fixedBase + 4];
        msg.errorCode     = detail::getLE16s(buf, fixedBase + 5);

        std::size_t pos = fixedBase + BODY_FIXED;

        // memberId var field
        const uint8_t* memberIdData = nullptr;
        uint16_t       memberIdLen  = 0;
        if (!getVarBytes(buf, avail, pos, memberIdData, memberIdLen)) return std::nullopt;
        msg.memberId = std::span<const uint8_t>(memberIdData, memberIdLen);

        return msg;
    }
};

// ---------------------------------------------------------------------------
// SyncGroup  (templateId 34)  client→broker
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (4)
//     templateId  = 34
//   [24..27]  generationId : int32
//   BODY_FIXED = 4, var fields start at offset 28
//   [28..29]  groupId length  : uint16
//   [30..]    groupId bytes
//   [..+2]    memberId length : uint16
//   [..+memberId.size()]  memberId bytes
//
// Total = 24 + 4 + 2 + groupId.size() + 2 + memberId.size()
// ---------------------------------------------------------------------------
struct SyncGroup {
    int64_t  sessionId     = 0;
    int64_t  correlationId = 0;
    std::span<const uint8_t> groupId;   // zero-copy view
    std::span<const uint8_t> memberId;  // zero-copy view
    int32_t  generationId  = 0;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::SyncGroup); // 34
    static constexpr uint16_t BODY_FIXED  = 4;  // generationId(4)

    /// Total encoded size.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 2u + groupId.size()
             + 2u + memberId.size();
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

        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<SyncGroup> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        SyncGroup msg;
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

        return msg;
    }
};

// ---------------------------------------------------------------------------
// SyncGroupResponse  (templateId 35)  broker→client
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (2)
//     templateId  = 35
//   [24..25]  errorCode : int16
//   BODY_FIXED = 2, var/array fields start at offset 26
//   [26..27]  assignment count : uint16
//   [28..]    assignment entries (each 8 bytes: topicId(int32) + partition(int32))
//
// Total = 24 + 2 + 2 + 8*assignment.size()
// ---------------------------------------------------------------------------
struct SyncGroupResponse {
    int64_t  sessionId     = 0;
    int64_t  correlationId = 0;
    int16_t  errorCode     = 0;         // ErrorCode value; 0 = OK
    std::vector<TopicPartition32> assignment;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::SyncGroupResponse); // 35
    static constexpr uint16_t BODY_FIXED  = 2;  // errorCode(2)

    /// Total encoded size.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 2u + 8u * assignment.size();
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
        detail::putLE16s(buf, fixedBase + 0, errorCode);

        std::size_t pos = static_cast<std::size_t>(fixedBase) + BODY_FIXED;

        // assignment array: uint16 count + 8-byte entries
        detail::putLE16(buf + pos, 0, static_cast<uint16_t>(assignment.size()));
        pos += 2;
        for (const auto& tp : assignment) {
            detail::putLE32(buf, static_cast<int>(pos),     tp.topicId);
            detail::putLE32(buf, static_cast<int>(pos) + 4, tp.partition);
            pos += 8;
        }

        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<SyncGroupResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        SyncGroupResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        msg.errorCode     = detail::getLE16s(buf, fixedBase + 0);

        std::size_t pos = fixedBase + BODY_FIXED;

        // assignment array
        if (pos + 2 > avail) return std::nullopt;
        const uint16_t count = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;
        if (pos + static_cast<std::size_t>(count) * 8u > avail) return std::nullopt;
        msg.assignment.resize(count);
        for (uint16_t i = 0; i < count; ++i) {
            msg.assignment[i].topicId   = detail::getLE32(buf, static_cast<int>(pos));
            msg.assignment[i].partition = detail::getLE32(buf, static_cast<int>(pos) + 4);
            pos += 8;
        }

        return msg;
    }
};

// ---------------------------------------------------------------------------
// Heartbeat  (templateId 36)  client→broker
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (4)
//     templateId  = 36
//   [24..27]  generationId : int32
//   BODY_FIXED = 4, var fields start at offset 28
//   [28..29]  groupId length  : uint16
//   [30..]    groupId bytes
//   [..+2]    memberId length : uint16
//   [..+memberId.size()]  memberId bytes
//
// Total = 24 + 4 + 2 + groupId.size() + 2 + memberId.size()
// ---------------------------------------------------------------------------
struct Heartbeat {
    int64_t  sessionId     = 0;
    int64_t  correlationId = 0;
    std::span<const uint8_t> groupId;   // zero-copy view
    std::span<const uint8_t> memberId;  // zero-copy view
    int32_t  generationId  = 0;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::Heartbeat); // 36
    static constexpr uint16_t BODY_FIXED  = 4;  // generationId(4)

    /// Total encoded size.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 2u + groupId.size()
             + 2u + memberId.size();
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

        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<Heartbeat> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        Heartbeat msg;
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

        return msg;
    }
};

// ---------------------------------------------------------------------------
// HeartbeatResponse  (templateId 37)  broker→client
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (2)
//     templateId  = 37
//   [24..25]  errorCode : int16
//   BODY_FIXED = 2  (no var fields)
//
// Total = 24 + 2 = 26 bytes
// ---------------------------------------------------------------------------
struct HeartbeatResponse {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    int16_t errorCode     = 0;   // ErrorCode value; 0 = OK

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::HeartbeatResponse); // 37
    static constexpr uint16_t BODY_FIXED  = 2;  // errorCode(2)

    static constexpr std::size_t ENCODED_SIZE = MessageHeader::ENCODED_SIZE + BODY_FIXED; // 26

    /// Encode the full frame (26 bytes) into buf.  Returns 26.
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

        return ENCODED_SIZE;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<HeartbeatResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE) return std::nullopt;

        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        HeartbeatResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        msg.errorCode = detail::getLE16s(buf, fixedBase + 0);

        return msg;
    }
};

// ---------------------------------------------------------------------------
// LeaveGroup  (templateId 38)  client→broker
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (0)
//     templateId  = 38
//   BODY_FIXED = 0, var fields start at offset 24
//   [24..25]  groupId length  : uint16
//   [26..]    groupId bytes
//   [..+2]    memberId length : uint16
//   [..+memberId.size()]  memberId bytes
//
// Total = 24 + 0 + 2 + groupId.size() + 2 + memberId.size()
// ---------------------------------------------------------------------------
struct LeaveGroup {
    int64_t  sessionId     = 0;
    int64_t  correlationId = 0;
    std::span<const uint8_t> groupId;   // zero-copy view
    std::span<const uint8_t> memberId;  // zero-copy view

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::LeaveGroup); // 38
    static constexpr uint16_t BODY_FIXED  = 0;  // no fixed body beyond header

    /// Total encoded size.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 2u + groupId.size()
             + 2u + memberId.size();
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

        pos += putVarBytes(buf + pos, groupId.data(),  static_cast<uint16_t>(groupId.size()));
        pos += putVarBytes(buf + pos, memberId.data(), static_cast<uint16_t>(memberId.size()));

        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<LeaveGroup> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        // BODY_FIXED = 0: var fields start right after the header
        std::size_t pos = MessageHeader::ENCODED_SIZE;

        LeaveGroup msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        const uint8_t* groupIdData = nullptr;
        uint16_t       groupIdLen  = 0;
        if (!getVarBytes(buf, avail, pos, groupIdData, groupIdLen)) return std::nullopt;
        msg.groupId = std::span<const uint8_t>(groupIdData, groupIdLen);

        const uint8_t* memberIdData = nullptr;
        uint16_t       memberIdLen  = 0;
        if (!getVarBytes(buf, avail, pos, memberIdData, memberIdLen)) return std::nullopt;
        msg.memberId = std::span<const uint8_t>(memberIdData, memberIdLen);

        return msg;
    }
};

// ---------------------------------------------------------------------------
// LeaveGroupResponse  (templateId 39)  broker→client
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (2)
//     templateId  = 39
//   [24..25]  errorCode : int16
//   BODY_FIXED = 2  (no var fields)
//
// Total = 24 + 2 = 26 bytes
// ---------------------------------------------------------------------------
struct LeaveGroupResponse {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    int16_t errorCode     = 0;   // ErrorCode value; 0 = OK

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::LeaveGroupResponse); // 39
    static constexpr uint16_t BODY_FIXED  = 2;  // errorCode(2)

    static constexpr std::size_t ENCODED_SIZE = MessageHeader::ENCODED_SIZE + BODY_FIXED; // 26

    /// Encode the full frame (26 bytes) into buf.  Returns 26.
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

        return ENCODED_SIZE;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<LeaveGroupResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE) return std::nullopt;

        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        LeaveGroupResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        msg.errorCode = detail::getLE16s(buf, fixedBase + 0);

        return msg;
    }
};

// ---------------------------------------------------------------------------
// Assign  (templateId 44)  broker→client egress control frame
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (4)
//     templateId  = 44
//   [24..27]  generationId : int32
//   BODY_FIXED = 4, array field starts at offset 28
//   [28..29]  assignment count : uint16   (0 = revoke all)
//   [30..]    assignment entries (each 8 bytes: topicId(int32) + partition(int32))
//
// Total = 24 + 4 + 2 + 8*assignment.size()
// An empty assignment (count=0) signals a full revoke.
// ---------------------------------------------------------------------------
struct Assign {
    int64_t  sessionId     = 0;
    int64_t  correlationId = 0;
    int32_t  generationId  = 0;
    std::vector<TopicPartition32> assignment;  // empty = revoke

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::Assign); // 44
    static constexpr uint16_t BODY_FIXED  = 4;  // generationId(4)

    /// Total encoded size.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 2u + 8u * assignment.size();
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

        // assignment array: uint16 count + 8-byte entries
        detail::putLE16(buf + pos, 0, static_cast<uint16_t>(assignment.size()));
        pos += 2;
        for (const auto& tp : assignment) {
            detail::putLE32(buf, static_cast<int>(pos),     tp.topicId);
            detail::putLE32(buf, static_cast<int>(pos) + 4, tp.partition);
            pos += 8;
        }

        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    /// An empty assignment (count=0) means revoke all.
    static std::optional<Assign> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        Assign msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        msg.generationId  = detail::getLE32(buf, static_cast<int>(fixedBase) + 0);

        std::size_t pos = fixedBase + BODY_FIXED;

        // assignment array
        if (pos + 2 > avail) return std::nullopt;
        const uint16_t count = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;
        if (pos + static_cast<std::size_t>(count) * 8u > avail) return std::nullopt;
        msg.assignment.resize(count);
        for (uint16_t i = 0; i < count; ++i) {
            msg.assignment[i].topicId   = detail::getLE32(buf, static_cast<int>(pos));
            msg.assignment[i].partition = detail::getLE32(buf, static_cast<int>(pos) + 4);
            pos += 8;
        }

        return msg;
    }
};

} // namespace rune::protocol::client
