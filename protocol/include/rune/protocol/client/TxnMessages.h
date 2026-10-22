#pragma once
// TxnMessages.h — Transaction protocol messages (TX-1, templateIds 45–54).
//
// Frame layout convention (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (message-specific)
//     templateId  = <message-specific>
//   [24..24+BODY_FIXED-1]   fixed-width fields
//   [24+BODY_FIXED..]       var fields (uint16 length prefix) + array fields
//
// Lifetime contract (mirrors GroupMessages.h and OffsetMessages.h):
//   - Span fields (transactionalId, groupId) and ArrayView fields are
//     zero-copy views into the caller-supplied buffer.
//   - buf MUST outlive every decoded result.  No heap allocation on decode.
//   - Encode accepts span/range inputs and writes into a caller-provided buffer.
//
// Sentinel conventions:
//   - Empty transactionalId => idempotent-only producer (no coordinator).
//   - producerId == -1 => non-idempotent (legacy); not used directly in these
//     messages but mirrored for consistency with Produce.
//
// Array wire format (AddPartitionsToTxn, AddPartitionsToTxnResponse,
//                    TxnOffsetCommitResponse):
//   uint16 count prefix + fixed-stride elements (zero-copy ArrayView on decode).
//
// Variable-stride array (TxnOffsetCommit entries):
//   uint16 count prefix + per-entry (topicId4+partition4+offset8+metaLen2+meta).
//   Decoded as TxnOffsetCommitEntriesView (forward-iterable, zero-copy).

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

#include "ArrayView.h"
#include "Codec.h"
#include "MessageHeader.h"
#include "Protocol.h"

namespace rune::protocol::client {

// ---------------------------------------------------------------------------
// TxnTopicPartition — one (topicId, partition) pair for AddPartitionsToTxn.
// Wire stride: 8 bytes (topicId int32 + partition int32), LE.
// ---------------------------------------------------------------------------
struct TxnTopicPartition {
    int32_t topicId   = 0;
    int32_t partition = 0;
};

// Decoded element from a TxnTopicPartition array (stride 8).
// Reuses the same physical layout as TopicPartition32Decoded from ArrayView.h.
// We define a separate decode function to keep things self-contained.
inline TxnTopicPartition decodeTxnTP(const uint8_t* base, std::size_t idx) noexcept {
    const int off = static_cast<int>(idx * 8u);
    return { detail::getLE32(base, off), detail::getLE32(base, off + 4) };
}
using TxnTopicPartitionView = FixedArrayView<TxnTopicPartition, 8, decodeTxnTP>;

// ---------------------------------------------------------------------------
// TxnPartitionResult — one (topicId, partition, errorCode) result entry.
// Wire stride: 10 bytes (topicId4 + partition4 + errorCode2), LE.
// Used in AddPartitionsToTxnResponse and TxnOffsetCommitResponse.
// ---------------------------------------------------------------------------
struct TxnPartitionResult {
    int32_t topicId   = 0;
    int32_t partition = 0;
    int16_t errorCode = 0;
};

inline TxnPartitionResult decodeTxnResult(const uint8_t* base, std::size_t idx) noexcept {
    const int off = static_cast<int>(idx * 10u);
    return { detail::getLE32(base, off),
             detail::getLE32(base, off + 4),
             detail::getLE16s(base, off + 8) };
}
using TxnPartitionResultView = FixedArrayView<TxnPartitionResult, 10, decodeTxnResult>;

// ---------------------------------------------------------------------------
// TxnOffsetCommitEntry — one entry for a TxnOffsetCommit request (encode path).
//   topicId(4) + partition(4) + offset(8) + metadata(var)
// Wire stride: 4 + 4 + 8 + 2 + metadata.size() (variable).
// ---------------------------------------------------------------------------
struct TxnOffsetCommitEntry {
    int32_t topicId   = 0;
    int32_t partition = 0;
    int64_t offset    = 0;
    std::span<const uint8_t> metadata;  // zero-copy; may be empty
};

// ---------------------------------------------------------------------------
// TxnOffsetCommitEntryRef — zero-copy decoded view of one TxnOffsetCommit entry.
// ---------------------------------------------------------------------------
struct TxnOffsetCommitEntryRef {
    int32_t topicId   = 0;
    int32_t partition = 0;
    int64_t offset    = 0;
    std::span<const uint8_t> metadata;  // zero-copy; may be empty
};

// ---------------------------------------------------------------------------
// TxnOffsetCommitEntriesView — forward-iterable zero-copy view over
// variable-stride TxnOffsetCommit entries (mirrors OffsetCommitEntriesView).
//
// Each entry: topicId(4) + partition(4) + offset(8) + metaLen(2) + metaBytes
// Minimum entry size: 18 bytes (empty metadata).
// ---------------------------------------------------------------------------
class TxnOffsetCommitEntriesView {
public:
    TxnOffsetCommitEntriesView() noexcept = default;
    TxnOffsetCommitEntriesView(const uint8_t* buf, std::size_t startPos,
                               std::size_t avail, uint16_t count) noexcept
        : buf_(buf), startPos_(startPos), avail_(avail), count_(count) {}

    std::size_t size()  const noexcept { return count_; }
    bool        empty() const noexcept { return count_ == 0; }

    /// O(n) random access via linear scan.  Prefer range-for on the hot path.
    TxnOffsetCommitEntryRef operator[](std::size_t idx) const noexcept {
        auto it = begin();
        for (std::size_t i = 0; i < idx && it.remaining > 0; ++i) ++it;
        return *it;
    }

    struct Iterator {
        using iterator_category = std::forward_iterator_tag;
        using value_type        = TxnOffsetCommitEntryRef;
        using difference_type   = std::ptrdiff_t;
        using pointer           = const TxnOffsetCommitEntryRef*;
        using reference         = TxnOffsetCommitEntryRef;

        const uint8_t* buf;
        std::size_t    pos;
        std::size_t    avail;
        uint16_t       remaining;

        TxnOffsetCommitEntryRef operator*() const noexcept {
            if (pos + 18u > avail) return {};
            TxnOffsetCommitEntryRef ref;
            ref.topicId   = detail::getLE32(buf, static_cast<int>(pos));
            ref.partition = detail::getLE32(buf, static_cast<int>(pos) + 4);
            ref.offset    = detail::getLE64(buf, static_cast<int>(pos) + 8);
            const uint16_t metaLen = detail::getLE16(buf, static_cast<int>(pos) + 16);
            const std::size_t dataOff = pos + 18u;
            if (dataOff + metaLen <= avail)
                ref.metadata = std::span<const uint8_t>(buf + dataOff, metaLen);
            return ref;
        }

        Iterator& operator++() noexcept {
            if (pos + 18u > avail) { remaining = 0; return *this; }
            const uint16_t metaLen = detail::getLE16(buf, static_cast<int>(pos) + 16);
            pos += static_cast<std::size_t>(18u) + metaLen;
            --remaining;
            return *this;
        }

        Iterator operator++(int) noexcept { auto c = *this; ++(*this); return c; }
        bool operator==(const Iterator& o) const noexcept { return remaining == o.remaining; }
        bool operator!=(const Iterator& o) const noexcept { return remaining != o.remaining; }
    };

    Iterator begin() const noexcept { return {buf_, startPos_, avail_, count_}; }
    Iterator end()   const noexcept { return {buf_, 0,         avail_, 0u}; }

private:
    const uint8_t* buf_      = nullptr;
    std::size_t    startPos_ = 0;
    std::size_t    avail_    = 0;
    uint16_t       count_    = 0;
};

// ===========================================================================
// InitProducerId  (templateId 45)  client→broker
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (4)
//     templateId  = 45
//   [24..27]  transactionTimeoutMs : int32
//   BODY_FIXED = 4, var fields start at offset 28 (header 24 + 4)
//   [28..29]  transactionalId length : uint16  (0 = idempotent-only producer)
//   [30..]    transactionalId bytes
//
// Total = 24 + 4 + 2 + transactionalId.size()
// Empty transactionalId => idempotent-only producer (no transaction coordinator).
// ===========================================================================
struct InitProducerId {
    int64_t  sessionId            = 0;
    int64_t  correlationId        = 0;
    int32_t  transactionTimeoutMs = 0;
    std::span<const uint8_t> transactionalId;  // zero-copy view; empty = idempotent-only

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::InitProducerId); // 45
    static constexpr uint16_t BODY_FIXED  = 4;  // transactionTimeoutMs(4)

    /// Total encoded size.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 2u + transactionalId.size();
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
        detail::putLE32(buf, fixedBase + 0, transactionTimeoutMs);

        std::size_t pos = static_cast<std::size_t>(fixedBase) + BODY_FIXED;
        pos += putVarBytes(buf + pos,
                           transactionalId.data(),
                           static_cast<uint16_t>(transactionalId.size()));
        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    /// transactionalId is a zero-copy span into buf — buf must outlive the result.
    static std::optional<InitProducerId> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        InitProducerId msg;
        msg.sessionId            = hdr.sessionId;
        msg.correlationId        = hdr.correlationId;
        msg.transactionTimeoutMs = detail::getLE32(buf, static_cast<int>(fixedBase) + 0);

        std::size_t pos = fixedBase + BODY_FIXED;

        const uint8_t* txnIdData = nullptr;
        uint16_t       txnIdLen  = 0;
        if (!getVarBytes(buf, avail, pos, txnIdData, txnIdLen)) return std::nullopt;
        msg.transactionalId = std::span<const uint8_t>(txnIdData, txnIdLen);

        return msg;
    }
};

// ===========================================================================
// InitProducerIdResponse  (templateId 46)  broker→client
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (12)
//     templateId  = 46
//   [24..31]  producerId    : int64
//   [32..33]  producerEpoch : int16
//   [34..35]  errorCode     : int16
//   BODY_FIXED = 12 (no var fields)
//
// Total = 24 + 12 = 36 bytes
// ===========================================================================
struct InitProducerIdResponse {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    int64_t producerId    = -1;
    int16_t producerEpoch = 0;
    int16_t errorCode     = 0;  // ErrorCode value; 0 = OK

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::InitProducerIdResponse); // 46
    static constexpr uint16_t BODY_FIXED  = 12;  // producerId(8) + producerEpoch(2) + errorCode(2)

    static constexpr std::size_t ENCODED_SIZE = MessageHeader::ENCODED_SIZE + BODY_FIXED; // 36

    /// Encode the full frame (36 bytes) into buf.  Returns 36.
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED; // producerId(8)+producerEpoch(2)+errorCode(2)
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        detail::putLE64(buf, fixedBase + 0, producerId);
        detail::putLE16s(buf, fixedBase + 8, producerEpoch);
        detail::putLE16s(buf, fixedBase + 10, errorCode);

        return ENCODED_SIZE;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    static std::optional<InitProducerIdResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE) return std::nullopt;

        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        InitProducerIdResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        msg.producerId    = detail::getLE64(buf, fixedBase + 0);
        msg.producerEpoch = detail::getLE16s(buf, fixedBase + 8);
        msg.errorCode     = detail::getLE16s(buf, fixedBase + 10);

        return msg;
    }
};

// ===========================================================================
// EndTxn  (templateId 53)  client→broker
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (11)
//     templateId  = 53
//   [24..31]  producerId    : int64
//   [32..33]  producerEpoch : int16
//   [34]      commit        : uint8   (1 = commit, 0 = abort)
//   BODY_FIXED = 11 (int64+int16+uint8), var fields start at offset 35
//   [35..36]  transactionalId length : uint16
//   [37..]    transactionalId bytes
//
// Total = 24 + 11 + 2 + transactionalId.size()
// ===========================================================================
struct EndTxn {
    int64_t  sessionId       = 0;
    int64_t  correlationId   = 0;
    int64_t  producerId      = -1;
    int16_t  producerEpoch   = 0;
    uint8_t  commit          = 0;   // 1 = commit, 0 = abort
    std::span<const uint8_t> transactionalId;  // zero-copy view

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::EndTxn); // 53
    static constexpr uint16_t BODY_FIXED  = 11;  // producerId(8) + producerEpoch(2) + commit(1)

    /// Total encoded size.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE
             + BODY_FIXED
             + 2u + transactionalId.size();
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
        detail::putLE64(buf, fixedBase + 0, producerId);
        detail::putLE16s(buf, fixedBase + 8, producerEpoch);
        buf[fixedBase + 10] = commit;

        std::size_t pos = static_cast<std::size_t>(fixedBase) + BODY_FIXED;
        pos += putVarBytes(buf + pos,
                           transactionalId.data(),
                           static_cast<uint16_t>(transactionalId.size()));
        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    /// transactionalId is a zero-copy span into buf — buf must outlive the result.
    static std::optional<EndTxn> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        EndTxn msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        msg.producerId    = detail::getLE64(buf, static_cast<int>(fixedBase) + 0);
        msg.producerEpoch = detail::getLE16s(buf, static_cast<int>(fixedBase) + 8);
        msg.commit        = buf[fixedBase + 10];

        std::size_t pos = fixedBase + BODY_FIXED;

        const uint8_t* txnIdData = nullptr;
        uint16_t       txnIdLen  = 0;
        if (!getVarBytes(buf, avail, pos, txnIdData, txnIdLen)) return std::nullopt;
        msg.transactionalId = std::span<const uint8_t>(txnIdData, txnIdLen);

        return msg;
    }
};

// ===========================================================================
// EndTxnResponse  (templateId 54)  broker→client
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (2)
//     templateId  = 54
//   [24..25]  errorCode : int16
//   BODY_FIXED = 2  (no var fields)
//
// Total = 24 + 2 = 26 bytes
// ===========================================================================
struct EndTxnResponse {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    int16_t errorCode     = 0;  // ErrorCode value; 0 = OK

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::EndTxnResponse); // 54
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
    static std::optional<EndTxnResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE) return std::nullopt;

        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        EndTxnResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        msg.errorCode = detail::getLE16s(buf, fixedBase + 0);

        return msg;
    }
};

// ===========================================================================
// AddPartitionsToTxn  (templateId 47)  client→broker
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (10)
//     templateId  = 47
//   [24..31]  producerId    : int64
//   [32..33]  producerEpoch : int16
//   BODY_FIXED = 10, var/array fields start at offset 34
//   [34..35]  transactionalId length : uint16
//   [36..]    transactionalId bytes
//   [..+2]    partitions count : uint16
//   [..+count*8]  partitions entries (each 8 bytes: topicId(int32)+partition(int32))
//
// Total = 24 + 10 + 2 + transactionalId.size() + 2 + 8*partitionCount
// ===========================================================================
struct AddPartitionsToTxn {
    int64_t  sessionId       = 0;
    int64_t  correlationId   = 0;
    int64_t  producerId      = -1;
    int16_t  producerEpoch   = 0;
    std::span<const uint8_t> transactionalId;  // zero-copy view

    // Decoded: zero-copy fixed-stride view — buf must outlive this.
    TxnTopicPartitionView partitions;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::AddPartitionsToTxn); // 47
    static constexpr uint16_t BODY_FIXED  = 10;  // producerId(8) + producerEpoch(2)

    /// Total encoded size for given transactionalId + partition count.
    static std::size_t encodedSizeFor(std::span<const uint8_t> txnId,
                                      std::size_t partitionCount) noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED
             + 2u + txnId.size()
             + 2u + 8u * partitionCount;
    }

    /// Encode the full frame into buf.  partSpan is a span of TxnTopicPartition.
    /// buf must be >= encodedSizeFor(transactionalId, partSpan.size()) bytes.
    /// Returns total bytes written.
    std::size_t encode(uint8_t* buf,
                       std::span<const TxnTopicPartition> partSpan) const noexcept {
        MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        detail::putLE64(buf, fixedBase + 0, producerId);
        detail::putLE16s(buf, fixedBase + 8, producerEpoch);

        std::size_t pos = static_cast<std::size_t>(fixedBase) + BODY_FIXED;

        pos += putVarBytes(buf + pos,
                           transactionalId.data(),
                           static_cast<uint16_t>(transactionalId.size()));

        // partitions array: uint16 count + 8-byte entries
        detail::putLE16(buf + pos, 0, static_cast<uint16_t>(partSpan.size()));
        pos += 2;
        for (const auto& tp : partSpan) {
            detail::putLE32(buf, static_cast<int>(pos),     tp.topicId);
            detail::putLE32(buf, static_cast<int>(pos) + 4, tp.partition);
            pos += 8;
        }

        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    /// All span/view fields are zero-copy into buf — buf must outlive the result.
    static std::optional<AddPartitionsToTxn> decode(const uint8_t* buf,
                                                     std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        AddPartitionsToTxn msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        msg.producerId    = detail::getLE64(buf, static_cast<int>(fixedBase) + 0);
        msg.producerEpoch = detail::getLE16s(buf, static_cast<int>(fixedBase) + 8);

        std::size_t pos = fixedBase + BODY_FIXED;

        const uint8_t* txnIdData = nullptr;
        uint16_t       txnIdLen  = 0;
        if (!getVarBytes(buf, avail, pos, txnIdData, txnIdLen)) return std::nullopt;
        msg.transactionalId = std::span<const uint8_t>(txnIdData, txnIdLen);

        // partitions array (zero-copy view)
        if (pos + 2 > avail) return std::nullopt;
        const uint16_t count = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;
        if (pos + static_cast<std::size_t>(count) * 8u > avail) return std::nullopt;
        msg.partitions = TxnTopicPartitionView(buf + pos, count);

        return msg;
    }
};

// ===========================================================================
// AddPartitionsToTxnResponse  (templateId 48)  broker→client
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (0)
//     templateId  = 48
//   BODY_FIXED = 0, array starts at offset 24
//   [24..25]  results count : uint16
//   per result (10 bytes each):
//     topicId(int32) + partition(int32) + errorCode(int16)
//
// Total = 24 + 2 + 10*results.size()
// ===========================================================================
struct AddPartitionsToTxnResponse {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;

    // Decoded: zero-copy fixed-stride view — buf must outlive this.
    TxnPartitionResultView results;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::AddPartitionsToTxnResponse); // 48
    static constexpr uint16_t BODY_FIXED  = 0;  // no fixed body beyond header

    /// Encoded size for a given result count.
    static std::size_t encodedSizeFor(std::size_t count) noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 2u + 10u * count;
    }

    /// Encode the full frame into buf.  resSlice is a span of TxnPartitionResult.
    /// buf must be >= encodedSizeFor(resSlice.size()) bytes.
    /// Returns total bytes written.
    std::size_t encode(uint8_t* buf,
                       std::span<const TxnPartitionResult> resSlice) const noexcept {
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
    static std::optional<AddPartitionsToTxnResponse> decode(const uint8_t* buf,
                                                             std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        // BODY_FIXED = 0: array starts right after header
        std::size_t pos = MessageHeader::ENCODED_SIZE;

        AddPartitionsToTxnResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        if (pos + 2 > avail) return std::nullopt;
        const uint16_t count = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;
        if (pos + static_cast<std::size_t>(count) * 10u > avail) return std::nullopt;
        msg.results = TxnPartitionResultView(buf + pos, count);

        return msg;
    }
};

// ===========================================================================
// AddOffsetsToTxn  (templateId 49)  client→broker
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (10)
//     templateId  = 49
//   [24..31]  producerId    : int64
//   [32..33]  producerEpoch : int16
//   BODY_FIXED = 10, var fields start at offset 34
//   [34..35]  transactionalId length : uint16
//   [36..]    transactionalId bytes
//   [..+2]    groupId length : uint16
//   [..+groupId.size()]  groupId bytes
//
// Total = 24 + 10 + 2 + transactionalId.size() + 2 + groupId.size()
// ===========================================================================
struct AddOffsetsToTxn {
    int64_t  sessionId       = 0;
    int64_t  correlationId   = 0;
    int64_t  producerId      = -1;
    int16_t  producerEpoch   = 0;
    std::span<const uint8_t> transactionalId;  // zero-copy view
    std::span<const uint8_t> groupId;           // zero-copy view

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::AddOffsetsToTxn); // 49
    static constexpr uint16_t BODY_FIXED  = 10;  // producerId(8) + producerEpoch(2)

    /// Total encoded size.
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED
             + 2u + transactionalId.size()
             + 2u + groupId.size();
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
        detail::putLE64(buf, fixedBase + 0, producerId);
        detail::putLE16s(buf, fixedBase + 8, producerEpoch);

        std::size_t pos = static_cast<std::size_t>(fixedBase) + BODY_FIXED;

        pos += putVarBytes(buf + pos,
                           transactionalId.data(),
                           static_cast<uint16_t>(transactionalId.size()));
        pos += putVarBytes(buf + pos,
                           groupId.data(),
                           static_cast<uint16_t>(groupId.size()));
        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    /// All span fields are zero-copy into buf — buf must outlive the result.
    static std::optional<AddOffsetsToTxn> decode(const uint8_t* buf,
                                                  std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        AddOffsetsToTxn msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        msg.producerId    = detail::getLE64(buf, static_cast<int>(fixedBase) + 0);
        msg.producerEpoch = detail::getLE16s(buf, static_cast<int>(fixedBase) + 8);

        std::size_t pos = fixedBase + BODY_FIXED;

        const uint8_t* txnIdData = nullptr;
        uint16_t       txnIdLen  = 0;
        if (!getVarBytes(buf, avail, pos, txnIdData, txnIdLen)) return std::nullopt;
        msg.transactionalId = std::span<const uint8_t>(txnIdData, txnIdLen);

        const uint8_t* grpIdData = nullptr;
        uint16_t       grpIdLen  = 0;
        if (!getVarBytes(buf, avail, pos, grpIdData, grpIdLen)) return std::nullopt;
        msg.groupId = std::span<const uint8_t>(grpIdData, grpIdLen);

        return msg;
    }
};

// ===========================================================================
// AddOffsetsToTxnResponse  (templateId 50)  broker→client
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (2)
//     templateId  = 50
//   [24..25]  errorCode : int16
//   BODY_FIXED = 2  (no var fields)
//
// Total = 24 + 2 = 26 bytes
// ===========================================================================
struct AddOffsetsToTxnResponse {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    int16_t errorCode     = 0;  // ErrorCode value; 0 = OK

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::AddOffsetsToTxnResponse); // 50
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
    static std::optional<AddOffsetsToTxnResponse> decode(const uint8_t* buf,
                                                          std::size_t avail) noexcept {
        if (avail < ENCODED_SIZE) return std::nullopt;

        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        AddOffsetsToTxnResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        msg.errorCode = detail::getLE16s(buf, fixedBase + 0);

        return msg;
    }
};

// ===========================================================================
// TxnOffsetCommit  (templateId 51)  client→broker
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (10)
//     templateId  = 51
//   [24..31]  producerId    : int64
//   [32..33]  producerEpoch : int16
//   BODY_FIXED = 10, var/array fields start at offset 34
//   [34..35]  transactionalId length : uint16
//   [36..]    transactionalId bytes
//   [..+2]    groupId length : uint16
//   [..+groupId.size()]  groupId bytes
//   [..+2]    entries count : uint16
//   per entry (variable stride):
//     topicId(int32) + partition(int32) + offset(int64) + metaLen(uint16) + meta bytes
//
// Total = 24 + 10 + 2 + txnId.size() + 2 + groupId.size() + 2 +
//         sum_entries(4+4+8+2+meta.size())
//
// Client version 10 (issue #125, KIP-447) — an OPTIONAL trailing member block right after the last entry:
//   [..+4]    generationId   : int32
//   [..+2]    memberId length : uint16
//   [..+len]  memberId bytes
// Present = the consumer-group member the offsets belong to; the coordinator fences it like an OffsetCommit.
// Absent (the frame ends with the last entry) = legacy: generationId -1, memberId empty, no member check.  The
// encoder writes the block only when generationId >= 0.  The decoder accepts both; a block truncated at any offset
// is a malformed frame (nullopt); bytes after a complete block are ignored (forward compatible).  An older
// decoder reads only `count` entries and ignores the block (a v10 client may send it to any broker).
// ===========================================================================
struct TxnOffsetCommit {
    int64_t  sessionId       = 0;
    int64_t  correlationId   = 0;
    int64_t  producerId      = -1;
    int16_t  producerEpoch   = 0;
    std::span<const uint8_t> transactionalId;  // zero-copy view
    std::span<const uint8_t> groupId;           // zero-copy view
    /// KIP-447 member block (version 10): encoded only when generationId >= 0.  Decoded: hasMemberBlock tells
    /// whether the frame carried it (a present block is always checked, whatever its values).
    int32_t  generationId    = -1;
    std::span<const uint8_t> memberId;          // zero-copy view; empty = none
    bool     hasMemberBlock  = false;           // decode only

    // Decoded: zero-copy forward-iterable view — buf must outlive this.
    TxnOffsetCommitEntriesView entries;

    static constexpr std::size_t kMemberBlockFixed = 4u + 2u;   // generationId + memberId length

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::TxnOffsetCommit); // 51
    static constexpr uint16_t BODY_FIXED  = 10;  // producerId(8) + producerEpoch(2)

    /// Total encoded size from spans and entries.
    static std::size_t encodedSizeFor(std::span<const uint8_t> txnId,
                                      std::span<const uint8_t> grpId,
                                      std::span<const TxnOffsetCommitEntry> entrs) noexcept {
        std::size_t entriesSize = 0;
        for (const auto& e : entrs)
            entriesSize += 4u + 4u + 8u + 2u + e.metadata.size();
        return MessageHeader::ENCODED_SIZE + BODY_FIXED
             + 2u + txnId.size()
             + 2u + grpId.size()
             + 2u + entriesSize;
    }

    /// Bytes the member block adds (0 when generationId < 0: no block).
    std::size_t memberBlockSize() const noexcept {
        return generationId >= 0 ? kMemberBlockFixed + memberId.size() : 0u;
    }

    /// Total encoded size of THIS message (ids, the member block when present) with `entrs`.
    std::size_t encodedSize(std::span<const TxnOffsetCommitEntry> entrs) const noexcept {
        return encodedSizeFor(transactionalId, groupId, entrs) + memberBlockSize();
    }

    /// Write the member block (when generationId >= 0) at buf[pos..]; returns the bytes written.  Exposed for
    /// encoders that write the entries themselves (the client splits entries across frames).
    std::size_t encodeMemberBlock(uint8_t* buf, std::size_t pos) const noexcept {
        if (generationId < 0) return 0;
        detail::putLE32(buf, static_cast<int>(pos), generationId);
        return 4u + putVarBytes(buf + pos + 4, memberId.data(), static_cast<uint16_t>(memberId.size()));
    }

    /// Encode the full frame into buf.  buf must be >= encodedSize(entrs) bytes.
    /// Returns total bytes written.
    std::size_t encode(uint8_t* buf,
                       std::span<const TxnOffsetCommitEntry> entrs) const noexcept {
        MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        detail::putLE64(buf, fixedBase + 0, producerId);
        detail::putLE16s(buf, fixedBase + 8, producerEpoch);

        std::size_t pos = static_cast<std::size_t>(fixedBase) + BODY_FIXED;

        pos += putVarBytes(buf + pos,
                           transactionalId.data(),
                           static_cast<uint16_t>(transactionalId.size()));
        pos += putVarBytes(buf + pos,
                           groupId.data(),
                           static_cast<uint16_t>(groupId.size()));

        // entries array: uint16 count + variable-stride entries
        detail::putLE16(buf + pos, 0, static_cast<uint16_t>(entrs.size()));
        pos += 2;
        for (const auto& e : entrs) {
            detail::putLE32(buf, static_cast<int>(pos),      e.topicId);
            detail::putLE32(buf, static_cast<int>(pos) + 4,  e.partition);
            detail::putLE64(buf, static_cast<int>(pos) + 8,  e.offset);
            pos += 16;
            pos += putVarBytes(buf + pos,
                               e.metadata.data(),
                               static_cast<uint16_t>(e.metadata.size()));
        }
        pos += encodeMemberBlock(buf, pos);

        return pos;
    }

    /// Decode a full frame from buf[0..avail-1].
    /// Returns nullopt on any truncation or header mismatch.
    /// All span/view fields are zero-copy into buf — buf must outlive the result.
    static std::optional<TxnOffsetCommit> decode(const uint8_t* buf,
                                                  std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        TxnOffsetCommit msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        msg.producerId    = detail::getLE64(buf, static_cast<int>(fixedBase) + 0);
        msg.producerEpoch = detail::getLE16s(buf, static_cast<int>(fixedBase) + 8);

        std::size_t pos = fixedBase + BODY_FIXED;

        const uint8_t* txnIdData = nullptr;
        uint16_t       txnIdLen  = 0;
        if (!getVarBytes(buf, avail, pos, txnIdData, txnIdLen)) return std::nullopt;
        msg.transactionalId = std::span<const uint8_t>(txnIdData, txnIdLen);

        const uint8_t* grpIdData = nullptr;
        uint16_t       grpIdLen  = 0;
        if (!getVarBytes(buf, avail, pos, grpIdData, grpIdLen)) return std::nullopt;
        msg.groupId = std::span<const uint8_t>(grpIdData, grpIdLen);

        // entries array — variable stride; validate count * minEntrySize first.
        if (pos + 2 > avail) return std::nullopt;
        const uint16_t count = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;
        // Minimum entry size = 18 bytes (empty metadata).
        if (pos + static_cast<std::size_t>(count) * 18u > avail) return std::nullopt;

        msg.entries = TxnOffsetCommitEntriesView(buf, pos, avail, count);

        // Walk the entries to their end (bounded: count is u16): an entry that overruns the frame is a truncation,
        // and the version-10 member block, if any, starts right after the last one.
        std::size_t end = pos;
        for (uint16_t i = 0; i < count; ++i) {
            if (end + 18u > avail) return std::nullopt;
            end += 18u + detail::getLE16(buf, static_cast<int>(end) + 16);
            if (end > avail) return std::nullopt;
        }
        if (end == avail) return msg;   // legacy: no member block
        if (end + kMemberBlockFixed > avail) return std::nullopt;   // a truncated block
        msg.generationId = detail::getLE32(buf, static_cast<int>(end));
        std::size_t mpos = end + 4u;
        const uint8_t* memberData = nullptr;
        uint16_t       memberLen  = 0;
        if (!getVarBytes(buf, avail, mpos, memberData, memberLen)) return std::nullopt;
        msg.memberId       = std::span<const uint8_t>(memberData, memberLen);
        msg.hasMemberBlock = true;
        return msg;
    }
};

// ===========================================================================
// TxnOffsetCommitResponse  (templateId 52)  broker→client
//
// Frame layout (all little-endian):
//   [0..23]   MessageHeader (24 bytes)
//     blockLength = BODY_FIXED (0)
//     templateId  = 52
//   BODY_FIXED = 0, array starts at offset 24
//   [24..25]  results count : uint16
//   per result (10 bytes each):
//     topicId(int32) + partition(int32) + errorCode(int16)
//
// Total = 24 + 2 + 10*results.size()
// ===========================================================================
struct TxnOffsetCommitResponse {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;

    // Decoded: zero-copy fixed-stride view — buf must outlive this.
    TxnPartitionResultView results;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::TxnOffsetCommitResponse); // 52
    static constexpr uint16_t BODY_FIXED  = 0;  // no fixed body beyond header

    /// Encoded size for a given result count.
    static std::size_t encodedSizeFor(std::size_t count) noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 2u + 10u * count;
    }

    /// Encode the full frame into buf.  resSlice is a span of TxnPartitionResult.
    /// buf must be >= encodedSizeFor(resSlice.size()) bytes.
    /// Returns total bytes written.
    std::size_t encode(uint8_t* buf,
                       std::span<const TxnPartitionResult> resSlice) const noexcept {
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
    static std::optional<TxnOffsetCommitResponse> decode(const uint8_t* buf,
                                                          std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        // BODY_FIXED = 0: array starts right after header
        std::size_t pos = MessageHeader::ENCODED_SIZE;

        TxnOffsetCommitResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;

        if (pos + 2 > avail) return std::nullopt;
        const uint16_t count = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;
        if (pos + static_cast<std::size_t>(count) * 10u > avail) return std::nullopt;
        msg.results = TxnPartitionResultView(buf + pos, count);

        return msg;
    }
};

} // namespace rune::protocol::client
