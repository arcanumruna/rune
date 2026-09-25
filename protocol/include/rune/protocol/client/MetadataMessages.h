#pragma once
// MetadataMessages.h — MD1 Task 10: client Metadata request/response codecs.
//
// MetadataRequest  (templateId 8, client→broker)
// MetadataResponse (templateId 9, broker→client)
//
// These messages belong to the control range (1-15).
// ---------------------------------------------------------------------------
// MetadataRequest
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (0)
//     templateId  = 8
//   BODY_FIXED = 0 (no fixed body fields)
//   [24..25]  topicIds length : uint16   (count of int32 topic ids)
//   [26..]    topicIds bytes             (count * 4 bytes, LE int32 each)
//
// An empty topicIds array (length=0) means "all topics".
// ---------------------------------------------------------------------------
// MetadataResponse
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (0)
//     templateId  = 9
//   BODY_FIXED = 0 (no fixed body)
//   [24..25]  brokerCount  : uint16
//   [26..]    brokers[]:    (each entry = nodeId(4) + port(4) + host var-str)
//               [+0]  nodeId : int32
//               [+4]  port   : int32
//               [+8..9] host length : uint16
//               [+10..]  host bytes
//   [after brokers]
//             partitionCount : uint16
//   [after count]
//             partitions[]:  (each entry = fixed 20 bytes)
//               topicId(4) + partition(4) + leaderId(4) + leaderEpoch(4) + ingressStreamId(4)
// ---------------------------------------------------------------------------

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <vector>
#include "Codec.h"
#include "MessageHeader.h"
#include "Protocol.h"

namespace rune::protocol::client {

// ---------------------------------------------------------------------------
// MetadataRequest  (templateId 8)
// ---------------------------------------------------------------------------
struct MetadataRequest {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    /// Topic ids to query.  Empty = all topics.
    ///
    /// ENCODING: Set this span before calling encode().  The span must stay
    ///   valid until encode() returns.  Typically points into a caller-owned
    ///   vector or a static array.
    ///
    /// DECODING: decode() populates ownedTopicIds_ (below) by copying each
    ///   int32 via getLE32() to avoid a misaligned aliasing load (pos=26 is
    ///   NOT 4-byte aligned in the wire buffer, so reinterpret_cast<int32_t*>
    ///   would be UB — a misaligned load that UBSan traps).  topicIds is then
    ///   set to point at ownedTopicIds_ so callers iterate topicIds as before.
    ///   ownedTopicIds_ is an implementation detail; callers should only read
    ///   through topicIds.
    std::span<const int32_t> topicIds;

    /// Owned storage for decoded topicIds (populated by decode()).
    /// Non-empty only when this struct was produced by decode().
    std::vector<int32_t> ownedTopicIds_;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::MetadataRequest); // 8
    static constexpr uint16_t BODY_FIXED  = 0;

    /// Total encoded size: header + 2 (count field) + topicIds.size()*4.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 2u + topicIds.size() * 4u;
    }

    /// Encode into buf.  buf must be >= encodedSize() bytes.
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
        // Write count as uint16 (LE).
        const uint16_t count = static_cast<uint16_t>(topicIds.size());
        detail::putLE16(buf, static_cast<int>(pos), count);
        pos += 2;
        for (const int32_t tid : topicIds) {
            detail::putLE32(buf, static_cast<int>(pos), tid);
            pos += 4;
        }
        return pos;
    }

    /// Decode from buf[0..avail-1].
    /// Returns nullopt on truncation or header mismatch.
    /// The returned span is a zero-copy view into buf — buf must outlive the result.
    static std::optional<MetadataRequest> decode(const uint8_t* buf,
                                                  std::size_t    avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedEnd = MessageHeader::ENCODED_SIZE + BODY_FIXED;
        if (avail < fixedEnd + 2u) return std::nullopt;  // need at least the count u16

        MetadataRequest msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        std::size_t pos = fixedEnd;
        const uint16_t count = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;
        // Validate: count * 4 bytes must be present.
        if (avail < pos + static_cast<std::size_t>(count) * 4u) return std::nullopt;
        // MISALIGNMENT FIX: pos here (fixedEnd + 2 = 26) is NOT 4-byte aligned,
        // so reinterpret_cast<const int32_t*>(buf + pos) is undefined behaviour
        // (a misaligned load that UBSan traps and that the broker previously
        // dereferenced).  Decode each topicId via getLE32() into an owned vector
        // and point the span at that vector, so callers iterate topicIds safely.
        if (count > 0) {
            msg.ownedTopicIds_.reserve(count);
            for (uint16_t i = 0; i < count; ++i) {
                msg.ownedTopicIds_.push_back(
                    detail::getLE32(buf, static_cast<int>(pos + i * 4u)));
            }
            msg.topicIds = std::span<const int32_t>(msg.ownedTopicIds_);
        }
        return msg;
    }
};

// ---------------------------------------------------------------------------
// BrokerEntry and PartitionEntry — wire-format value types for MetadataResponse.
// ---------------------------------------------------------------------------

/// One broker entry as decoded from the response.
struct BrokerEntry {
    int32_t      nodeId = 0;
    int32_t      port   = 0;
    std::string_view host;  // zero-copy into source buffer
};

/// One partition entry as decoded from the response (fixed 28 bytes).
/// E5.1: ingressStreamId added as the 5th field (per-worker ingress routing).
/// read-rework T3/T4: egressControlPort + egressStreamId added (per-worker dynamic-
/// MDC egress).  The client builds `aeron:udp?control-mode=dynamic|control=<leader
/// host>:<egressControlPort>|fc=min` on stream egressStreamId to receive this
/// partition's pushed Records.  Both 0 when per-worker egress is inactive.
struct PartitionEntry {
    int32_t topicId          = 0;
    int32_t partition        = 0;
    int32_t leaderId         = 0;
    int32_t leaderEpoch      = 0;
    int32_t ingressStreamId  = 0;  ///< E5.1: Aeron stream id for per-worker ingress.
    int32_t egressControlPort = 0; ///< per-worker dynamic-MDC control port (0=off).
    int32_t egressStreamId   = 0;  ///< per-worker egress stream id (0=off).
};

// ---------------------------------------------------------------------------
// MetadataResponse  (templateId 9)
// ---------------------------------------------------------------------------
struct MetadataResponse {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;

    // ---- Encode-side fields (set by the broker before calling encode()) ------
    // These are spans / collections of wire structs for the two arrays.
    // Kept as spans for zero-copy: the broker builds into a scratch area and
    // passes spans into encode().  Tests may also set them directly.
    std::span<const BrokerEntry>    brokers;
    std::span<const PartitionEntry> partitions;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::MetadataResponse); // 9
    static constexpr uint16_t BODY_FIXED  = 0;

    // ---- Encode into buf.
    //
    // Wire format after the header (BODY_FIXED=0):
    //   uint16 brokerCount
    //   for each broker:
    //     int32 nodeId  (LE)
    //     int32 port    (LE)
    //     uint16 hostLen (LE)
    //     hostLen bytes
    //   uint16 partitionCount
    //   for each partition:
    //     int32 topicId        (LE)
    //     int32 partition      (LE)
    //     int32 leaderId       (LE)
    //     int32 leaderEpoch    (LE)
    //     int32 ingressStreamId (LE)   E5.1: per-worker Aeron stream id
    //
    // Returns total bytes written.
    // buf must be large enough; use encodedSize() to pre-allocate.
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

        // Broker array.
        detail::putLE16(buf, static_cast<int>(pos),
                        static_cast<uint16_t>(brokers.size()));
        pos += 2;
        for (const auto& b : brokers) {
            detail::putLE32(buf, static_cast<int>(pos),     b.nodeId);
            detail::putLE32(buf, static_cast<int>(pos) + 4, b.port);
            pos += 8;
            pos += putVarStr(buf + pos, b.host);
        }

        // Partition array.
        detail::putLE16(buf, static_cast<int>(pos),
                        static_cast<uint16_t>(partitions.size()));
        pos += 2;
        for (const auto& p : partitions) {
            detail::putLE32(buf, static_cast<int>(pos),      p.topicId);
            detail::putLE32(buf, static_cast<int>(pos) + 4,  p.partition);
            detail::putLE32(buf, static_cast<int>(pos) + 8,  p.leaderId);
            detail::putLE32(buf, static_cast<int>(pos) + 12, p.leaderEpoch);
            detail::putLE32(buf, static_cast<int>(pos) + 16, p.ingressStreamId);   // E5.1
            detail::putLE32(buf, static_cast<int>(pos) + 20, p.egressControlPort); // read-rework
            detail::putLE32(buf, static_cast<int>(pos) + 24, p.egressStreamId);    // read-rework
            pos += 28;
        }
        return pos;
    }

    // ---- Compute encoded size (useful for scratch-buffer sizing) -------------
    std::size_t encodedSize() const noexcept {
        std::size_t sz = MessageHeader::ENCODED_SIZE + BODY_FIXED
                       + 2u;   // brokerCount
        for (const auto& b : brokers) {
            sz += 4u + 4u + 2u + b.host.size();
        }
        sz += 2u;              // partitionCount
        sz += partitions.size() * 28u;  // read-rework: 28 bytes/entry (E5.1 was 20)
        return sz;
    }

    // ---- Decode result ----------------------------------------------------------
    //
    // Because broker host strings are variable-length, the decoded response
    // holds vector-of-structs (BrokerEntry.host is a string_view into buf).
    // buf must outlive the result.
    struct Decoded {
        int64_t                   sessionId     = 0;
        int64_t                   correlationId = 0;
        std::vector<BrokerEntry>    brokers;
        std::vector<PartitionEntry> partitions;
    };

    static std::optional<Decoded> decode(const uint8_t* buf,
                                          std::size_t    avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedEnd = MessageHeader::ENCODED_SIZE + BODY_FIXED;
        std::size_t pos = fixedEnd;

        // Broker array.
        if (pos + 2u > avail) return std::nullopt;
        const uint16_t brokerCount = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;

        Decoded result;
        result.sessionId     = hdr.sessionId;
        result.correlationId = hdr.correlationId;
        result.brokers.reserve(brokerCount);

        for (uint16_t i = 0; i < brokerCount; ++i) {
            if (pos + 8u > avail) return std::nullopt;
            BrokerEntry b;
            b.nodeId = detail::getLE32(buf, static_cast<int>(pos));
            b.port   = detail::getLE32(buf, static_cast<int>(pos) + 4);
            pos += 8;
            if (!getVarStr(buf, avail, pos, b.host)) return std::nullopt;
            result.brokers.push_back(b);
        }

        // Partition array.
        if (pos + 2u > avail) return std::nullopt;
        const uint16_t partCount = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;
        result.partitions.reserve(partCount);

        if (pos + static_cast<std::size_t>(partCount) * 28u > avail) return std::nullopt;  // 28 bytes/entry
        for (uint16_t i = 0; i < partCount; ++i) {
            PartitionEntry p;
            p.topicId          = detail::getLE32(buf, static_cast<int>(pos));
            p.partition        = detail::getLE32(buf, static_cast<int>(pos) + 4);
            p.leaderId         = detail::getLE32(buf, static_cast<int>(pos) + 8);
            p.leaderEpoch      = detail::getLE32(buf, static_cast<int>(pos) + 12);
            p.ingressStreamId  = detail::getLE32(buf, static_cast<int>(pos) + 16);  // E5.1
            p.egressControlPort = detail::getLE32(buf, static_cast<int>(pos) + 20); // read-rework
            p.egressStreamId   = detail::getLE32(buf, static_cast<int>(pos) + 24);  // read-rework
            pos += 28;
            result.partitions.push_back(p);
        }
        return result;
    }
};

} // namespace rune::protocol::client
