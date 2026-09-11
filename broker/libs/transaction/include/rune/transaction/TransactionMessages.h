#pragma once
// ---------------------------------------------------------------------------
// TransactionMessages.h — Flat LE SBE codecs for the Rune transaction
// protocol (schema id = 114, version = 1).
//
// Messages:
//   InitProducerIdRequest   (templateId = 1) — client → coordinator
//   InitProducerIdResponse  (templateId = 2) — coordinator → client
//   AddPartitionsToTxnRequest  (templateId = 3) — client → coordinator
//   AddPartitionsToTxnResponse (templateId = 4) — coordinator → client
//   EndTxnRequest           (templateId = 5) — client → coordinator
//   EndTxnResponse          (templateId = 6) — coordinator → client
//   WriteTxnMarkersRequest  (templateId = 7) — coordinator → broker
//
// Wire format: 8-byte SBE message header + fixed block + variable-length
// fields (uint16 length prefix + raw UTF-8 bytes for strings; partition
// arrays use uint16 count + { topicLen:uint16, topic:bytes, partition:int32 }
// tuples).
//
// All integers are little-endian (static_assert enforced).
// NO internal allocation — encode/decode work on caller-provided buffers.
//
// 8-byte SBE header:
//   blockLength : uint16   (fixed-field payload bytes, excluding header)
//   templateId  : uint16
//   schemaId    : uint16   (= 114)
//   version     : uint16   (= 1)
// ---------------------------------------------------------------------------

#include <rune/transaction/Types.h>

#include <bit>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>
#include <vector>

namespace rune::transaction {

// ---------------------------------------------------------------------------
// Schema constants
// ---------------------------------------------------------------------------
inline constexpr uint16_t kTxnSchemaId      = 114;
inline constexpr uint16_t kTxnSchemaVersion = 1;

// ---------------------------------------------------------------------------
// Template IDs
// ---------------------------------------------------------------------------
enum class TxnTemplateId : uint16_t {
    InitProducerIdRequest      = 1,
    InitProducerIdResponse     = 2,
    AddPartitionsToTxnRequest  = 3,
    AddPartitionsToTxnResponse = 4,
    EndTxnRequest              = 5,
    EndTxnResponse             = 6,
    WriteTxnMarkersRequest     = 7,
};

// ---------------------------------------------------------------------------
// ControlType — commit or abort (mirrors Kafka's EndTransactionMarker)
// ---------------------------------------------------------------------------
enum class ControlType : int16_t {
    Commit = 0,
    Abort  = 1,
};

// ---------------------------------------------------------------------------
// Internal little-endian helpers
// ---------------------------------------------------------------------------
namespace txn_detail {

static_assert(std::endian::native == std::endian::little,
    "rune::transaction SBE codecs assume a little-endian host "
    "(schema 114 is little-endian SBE)");

template<typename T>
inline void write_le(uint8_t* p, T v) noexcept {
    std::memcpy(p, &v, sizeof(T));
}

template<typename T>
inline T read_le(const uint8_t* p) noexcept {
    T v; std::memcpy(&v, p, sizeof(T)); return v;
}

/// Write a uint16-prefixed string.
inline void write_str(uint8_t* buf, std::size_t& pos, std::string_view s) noexcept {
    auto len = static_cast<uint16_t>(s.size());
    write_le<uint16_t>(buf + pos, len); pos += 2;
    std::memcpy(buf + pos, s.data(), len); pos += len;
}

/// Read a uint16-prefixed string (zero-copy view into buf).
inline std::string_view read_str(const uint8_t* buf, std::size_t& pos) noexcept {
    auto len = read_le<uint16_t>(buf + pos); pos += 2;
    std::string_view sv{reinterpret_cast<const char*>(buf + pos),
                        static_cast<std::size_t>(len)};
    pos += len;
    return sv;
}

/// Write a partition array: uint16 count + { topicLen:uint16, topic, partition:int32 }×N.
inline void write_partitions(uint8_t* buf, std::size_t& pos,
                              const std::vector<TopicPartition>& parts) noexcept
{
    write_le<uint16_t>(buf + pos, static_cast<uint16_t>(parts.size())); pos += 2;
    for (const auto& tp : parts) {
        auto tlen = static_cast<uint16_t>(tp.topic.size());
        write_le<uint16_t>(buf + pos, tlen);       pos += 2;
        std::memcpy(buf + pos, tp.topic.data(), tlen); pos += tlen;
        write_le<int32_t>(buf + pos, tp.partition); pos += 4;
    }
}

/// Read a partition array into a vector.
inline std::vector<TopicPartition> read_partitions(const uint8_t* buf, std::size_t& pos) {
    uint16_t count = read_le<uint16_t>(buf + pos); pos += 2;
    std::vector<TopicPartition> parts;
    parts.reserve(count);
    for (uint16_t i = 0; i < count; ++i) {
        uint16_t tlen = read_le<uint16_t>(buf + pos); pos += 2;
        std::string topic{reinterpret_cast<const char*>(buf + pos),
                          static_cast<std::size_t>(tlen)};
        pos += tlen;
        int32_t partition = read_le<int32_t>(buf + pos); pos += 4;
        parts.push_back(TopicPartition{std::move(topic), partition});
    }
    return parts;
}

} // namespace txn_detail

// ---------------------------------------------------------------------------
// TxnMessageHeader — 8 bytes, schema 114
// ---------------------------------------------------------------------------
struct TxnMessageHeader {
    uint16_t blockLength{0};
    uint16_t templateId{0};
    uint16_t schemaId{0};
    uint16_t version{0};

    static constexpr std::size_t ENCODED_SIZE = 8;

    static std::size_t encode(uint8_t* buf, std::size_t offset,
                               uint16_t blockLen, uint16_t tmplId) noexcept
    {
        uint8_t* p = buf + offset;
        txn_detail::write_le<uint16_t>(p + 0, blockLen);
        txn_detail::write_le<uint16_t>(p + 2, tmplId);
        txn_detail::write_le<uint16_t>(p + 4, kTxnSchemaId);
        txn_detail::write_le<uint16_t>(p + 6, kTxnSchemaVersion);
        return ENCODED_SIZE;
    }

    static TxnMessageHeader decode(const uint8_t* buf, std::size_t offset) noexcept {
        const uint8_t* p = buf + offset;
        TxnMessageHeader h{};
        h.blockLength = txn_detail::read_le<uint16_t>(p + 0);
        h.templateId  = txn_detail::read_le<uint16_t>(p + 2);
        h.schemaId    = txn_detail::read_le<uint16_t>(p + 4);
        h.version     = txn_detail::read_le<uint16_t>(p + 6);
        return h;
    }
};

static_assert(TxnMessageHeader::ENCODED_SIZE == 8,
              "TxnMessageHeader must be exactly 8 bytes");

inline uint16_t peekTxnTemplateId(const uint8_t* buf, std::size_t offset = 0) noexcept {
    return txn_detail::read_le<uint16_t>(buf + offset + 2);
}

// ===========================================================================
// 1. InitProducerIdRequest  (templateId = 1)
//
// Fixed block (4 bytes):
//   txnTimeoutMs : int32  @ +0
//
// Var-length: transactionalId (uint16 len prefix + bytes)
// ===========================================================================
struct InitProducerIdRequestMsg {
    int32_t          txnTimeoutMs{0};
    std::string_view transactionalId; ///< zero-copy view

    static constexpr uint16_t TEMPLATE_ID  = 1;
    static constexpr uint16_t BLOCK_LENGTH = 4; // int32

    static_assert(BLOCK_LENGTH == 4, "InitProducerIdRequest fixed block must be 4 bytes");

    static std::size_t encode(uint8_t*         buf,
                               std::size_t      bufOffset,
                               int32_t          txnTimeoutMs_,
                               std::string_view transactionalId_) noexcept
    {
        std::size_t pos = bufOffset;
        pos += TxnMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        txn_detail::write_le<int32_t>(buf + pos, txnTimeoutMs_); pos += 4;
        txn_detail::write_str(buf, pos, transactionalId_);
        return pos - bufOffset;
    }

    static InitProducerIdRequestMsg decode(const uint8_t* buf,
                                            std::size_t    bufOffset) noexcept
    {
        std::size_t pos = bufOffset + TxnMessageHeader::ENCODED_SIZE;
        InitProducerIdRequestMsg m{};
        m.txnTimeoutMs    = txn_detail::read_le<int32_t>(buf + pos); pos += 4;
        m.transactionalId = txn_detail::read_str(buf, pos);
        return m;
    }
};

// ===========================================================================
// 2. InitProducerIdResponse  (templateId = 2)
//
// Fixed block (12 bytes):
//   producerId    : int64  @ +0
//   producerEpoch : int16  @ +8
//   errorCode     : int16  @ +10
//
// No var-length fields.
// ===========================================================================
struct InitProducerIdResponseMsg {
    int64_t producerId{-1};
    int16_t producerEpoch{-1};
    int16_t errorCode{0};

    static constexpr uint16_t TEMPLATE_ID  = 2;
    static constexpr uint16_t BLOCK_LENGTH = 12; // int64 + int16 + int16

    static_assert(BLOCK_LENGTH == 12, "InitProducerIdResponse fixed block must be 12 bytes");

    static std::size_t encode(uint8_t*    buf,
                               std::size_t bufOffset,
                               int64_t     producerId_,
                               int16_t     producerEpoch_,
                               int16_t     errorCode_) noexcept
    {
        std::size_t pos = bufOffset;
        pos += TxnMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        txn_detail::write_le<int64_t>(buf + pos, producerId_);    pos += 8;
        txn_detail::write_le<int16_t>(buf + pos, producerEpoch_); pos += 2;
        txn_detail::write_le<int16_t>(buf + pos, errorCode_);     pos += 2;
        return pos - bufOffset;
    }

    static InitProducerIdResponseMsg decode(const uint8_t* buf,
                                             std::size_t    bufOffset) noexcept
    {
        std::size_t pos = bufOffset + TxnMessageHeader::ENCODED_SIZE;
        InitProducerIdResponseMsg m{};
        m.producerId    = txn_detail::read_le<int64_t>(buf + pos); pos += 8;
        m.producerEpoch = txn_detail::read_le<int16_t>(buf + pos); pos += 2;
        m.errorCode     = txn_detail::read_le<int16_t>(buf + pos); pos += 2;
        return m;
    }
};

// ===========================================================================
// 3. AddPartitionsToTxnRequest  (templateId = 3)
//
// Fixed block (10 bytes):
//   producerId    : int64  @ +0
//   producerEpoch : int16  @ +8
//
// Var-length:
//   transactionalId : uint16-prefixed string
//   partitions      : uint16 count + { topicLen:uint16, topic, partition:int32 }×N
// ===========================================================================
struct AddPartitionsToTxnRequestMsg {
    int64_t                    producerId{-1};
    int16_t                    producerEpoch{0};
    std::string_view           transactionalId;
    std::vector<TopicPartition> partitions; // decoded form

    static constexpr uint16_t TEMPLATE_ID  = 3;
    static constexpr uint16_t BLOCK_LENGTH = 10; // int64 + int16

    static_assert(BLOCK_LENGTH == 10, "AddPartitionsToTxnRequest fixed block must be 10 bytes");

    static std::size_t encode(uint8_t*                            buf,
                               std::size_t                         bufOffset,
                               int64_t                             producerId_,
                               int16_t                             producerEpoch_,
                               std::string_view                    transactionalId_,
                               const std::vector<TopicPartition>&  partitions_) noexcept
    {
        std::size_t pos = bufOffset;
        pos += TxnMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        txn_detail::write_le<int64_t>(buf + pos, producerId_);    pos += 8;
        txn_detail::write_le<int16_t>(buf + pos, producerEpoch_); pos += 2;
        txn_detail::write_str(buf, pos, transactionalId_);
        txn_detail::write_partitions(buf, pos, partitions_);
        return pos - bufOffset;
    }

    static AddPartitionsToTxnRequestMsg decode(const uint8_t* buf,
                                                std::size_t    bufOffset)
    {
        std::size_t pos = bufOffset + TxnMessageHeader::ENCODED_SIZE;
        AddPartitionsToTxnRequestMsg m{};
        m.producerId    = txn_detail::read_le<int64_t>(buf + pos); pos += 8;
        m.producerEpoch = txn_detail::read_le<int16_t>(buf + pos); pos += 2;
        m.transactionalId = txn_detail::read_str(buf, pos);
        m.partitions    = txn_detail::read_partitions(buf, pos);
        return m;
    }
};

// ===========================================================================
// 4. AddPartitionsToTxnResponse  (templateId = 4)
//
// Fixed block (2 bytes):
//   errorCode : int16  @ +0
//
// No var-length fields.
// ===========================================================================
struct AddPartitionsToTxnResponseMsg {
    int16_t errorCode{0};

    static constexpr uint16_t TEMPLATE_ID  = 4;
    static constexpr uint16_t BLOCK_LENGTH = 2; // int16

    static_assert(BLOCK_LENGTH == 2, "AddPartitionsToTxnResponse fixed block must be 2 bytes");

    static std::size_t encode(uint8_t*    buf,
                               std::size_t bufOffset,
                               int16_t     errorCode_) noexcept
    {
        std::size_t pos = bufOffset;
        pos += TxnMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        txn_detail::write_le<int16_t>(buf + pos, errorCode_); pos += 2;
        return pos - bufOffset;
    }

    static AddPartitionsToTxnResponseMsg decode(const uint8_t* buf,
                                                 std::size_t    bufOffset) noexcept
    {
        std::size_t pos = bufOffset + TxnMessageHeader::ENCODED_SIZE;
        AddPartitionsToTxnResponseMsg m{};
        m.errorCode = txn_detail::read_le<int16_t>(buf + pos); pos += 2;
        return m;
    }
};

// ===========================================================================
// 5. EndTxnRequest  (templateId = 5)
//
// Fixed block (12 bytes):
//   producerId    : int64  @ +0
//   producerEpoch : int16  @ +8
//   commit        : int16  @ +10  (1 = commit, 0 = abort)
//
// Var-length: transactionalId (uint16 len prefix + bytes)
// ===========================================================================
struct EndTxnRequestMsg {
    int64_t          producerId{-1};
    int16_t          producerEpoch{0};
    int16_t          commit{0};   ///< 1 = commit, 0 = abort
    std::string_view transactionalId;

    static constexpr uint16_t TEMPLATE_ID  = 5;
    static constexpr uint16_t BLOCK_LENGTH = 12; // int64 + int16 + int16

    static_assert(BLOCK_LENGTH == 12, "EndTxnRequest fixed block must be 12 bytes");

    static std::size_t encode(uint8_t*         buf,
                               std::size_t      bufOffset,
                               int64_t          producerId_,
                               int16_t          producerEpoch_,
                               bool             commit_,
                               std::string_view transactionalId_) noexcept
    {
        std::size_t pos = bufOffset;
        pos += TxnMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        txn_detail::write_le<int64_t>(buf + pos, producerId_);         pos += 8;
        txn_detail::write_le<int16_t>(buf + pos, producerEpoch_);      pos += 2;
        txn_detail::write_le<int16_t>(buf + pos, commit_ ? 1 : 0);     pos += 2;
        txn_detail::write_str(buf, pos, transactionalId_);
        return pos - bufOffset;
    }

    static EndTxnRequestMsg decode(const uint8_t* buf,
                                    std::size_t    bufOffset) noexcept
    {
        std::size_t pos = bufOffset + TxnMessageHeader::ENCODED_SIZE;
        EndTxnRequestMsg m{};
        m.producerId    = txn_detail::read_le<int64_t>(buf + pos); pos += 8;
        m.producerEpoch = txn_detail::read_le<int16_t>(buf + pos); pos += 2;
        m.commit        = txn_detail::read_le<int16_t>(buf + pos); pos += 2;
        m.transactionalId = txn_detail::read_str(buf, pos);
        return m;
    }

    [[nodiscard]] bool isCommit() const noexcept { return commit != 0; }
};

// ===========================================================================
// 6. EndTxnResponse  (templateId = 6)
//
// Fixed block (2 bytes):
//   errorCode : int16  @ +0
//
// No var-length fields.
// ===========================================================================
struct EndTxnResponseMsg {
    int16_t errorCode{0};

    static constexpr uint16_t TEMPLATE_ID  = 6;
    static constexpr uint16_t BLOCK_LENGTH = 2; // int16

    static_assert(BLOCK_LENGTH == 2, "EndTxnResponse fixed block must be 2 bytes");

    static std::size_t encode(uint8_t*    buf,
                               std::size_t bufOffset,
                               int16_t     errorCode_) noexcept
    {
        std::size_t pos = bufOffset;
        pos += TxnMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        txn_detail::write_le<int16_t>(buf + pos, errorCode_); pos += 2;
        return pos - bufOffset;
    }

    static EndTxnResponseMsg decode(const uint8_t* buf,
                                     std::size_t    bufOffset) noexcept
    {
        std::size_t pos = bufOffset + TxnMessageHeader::ENCODED_SIZE;
        EndTxnResponseMsg m{};
        m.errorCode = txn_detail::read_le<int16_t>(buf + pos); pos += 2;
        return m;
    }
};

// ===========================================================================
// 7. WriteTxnMarkersRequest  (templateId = 7)
//
// Fixed block (14 bytes):
//   producerId        : int64  @ +0
//   producerEpoch     : int16  @ +8
//   controlType       : int16  @ +10  (0 = Commit, 1 = Abort)
//   coordinatorEpoch  : int32  @ +12
//
// Var-length:
//   transactionalId : uint16-prefixed string
//   partitions      : uint16 count + { topicLen:uint16, topic, partition:int32 }×N
// ===========================================================================
struct WriteTxnMarkersRequestMsg {
    int64_t                    producerId{-1};
    int16_t                    producerEpoch{0};
    int16_t                    controlType{0};   ///< 0 = Commit, 1 = Abort
    int32_t                    coordinatorEpoch{0};
    std::string_view           transactionalId;
    std::vector<TopicPartition> partitions;

    static constexpr uint16_t TEMPLATE_ID  = 7;
    static constexpr uint16_t BLOCK_LENGTH = 14; // int64 + int16 + int16 + int32

    static_assert(BLOCK_LENGTH == 14, "WriteTxnMarkersRequest fixed block must be 14 bytes");

    static std::size_t encode(uint8_t*                            buf,
                               std::size_t                         bufOffset,
                               int64_t                             producerId_,
                               int16_t                             producerEpoch_,
                               ControlType                         controlType_,
                               int32_t                             coordinatorEpoch_,
                               std::string_view                    transactionalId_,
                               const std::vector<TopicPartition>&  partitions_) noexcept
    {
        std::size_t pos = bufOffset;
        pos += TxnMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        txn_detail::write_le<int64_t>(buf + pos, producerId_);                   pos += 8;
        txn_detail::write_le<int16_t>(buf + pos, producerEpoch_);                pos += 2;
        txn_detail::write_le<int16_t>(buf + pos, static_cast<int16_t>(controlType_)); pos += 2;
        txn_detail::write_le<int32_t>(buf + pos, coordinatorEpoch_);             pos += 4;
        txn_detail::write_str(buf, pos, transactionalId_);
        txn_detail::write_partitions(buf, pos, partitions_);
        return pos - bufOffset;
    }

    static WriteTxnMarkersRequestMsg decode(const uint8_t* buf,
                                             std::size_t    bufOffset)
    {
        std::size_t pos = bufOffset + TxnMessageHeader::ENCODED_SIZE;
        WriteTxnMarkersRequestMsg m{};
        m.producerId       = txn_detail::read_le<int64_t>(buf + pos); pos += 8;
        m.producerEpoch    = txn_detail::read_le<int16_t>(buf + pos); pos += 2;
        m.controlType      = txn_detail::read_le<int16_t>(buf + pos); pos += 2;
        m.coordinatorEpoch = txn_detail::read_le<int32_t>(buf + pos); pos += 4;
        m.transactionalId  = txn_detail::read_str(buf, pos);
        m.partitions       = txn_detail::read_partitions(buf, pos);
        return m;
    }

    [[nodiscard]] ControlType controlTypeEnum() const noexcept {
        return static_cast<ControlType>(controlType);
    }
};

} // namespace rune::transaction
