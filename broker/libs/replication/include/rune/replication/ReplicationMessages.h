#pragma once
// ---------------------------------------------------------------------------
// ReplicationMessages.h — SBE-style flat little-endian codecs for the Rune
// data-plane replication protocol (schema id = 112, version = 1).
//
// Protocol model: Kafka-style FETCH replication.
//   • Follower → Leader: FetchRequest  (follower asks for records from fetchOffset)
//   • Leader  → Follower: FetchResponse (leader sends records + high-watermark)
//
// Wire format: 8-byte SBE message header followed by a fixed-size payload.
// FetchResponse additionally carries a variable-length records blob
// (int32 length prefix + raw bytes).
//
// All integers are little-endian (native on x86; static_assert enforced).
// NO internal allocation — encode/decode work on caller-provided buffers.
// Zero-copy: FetchResponseMsg::records is a std::span<const uint8_t> view
// into the decode buffer.
//
// 8-byte SBE message header (identical structure to consensus schema 111):
//   blockLength : uint16   (fixed-field payload bytes, excluding header)
//   templateId  : uint16
//   schemaId    : uint16   (= 112)
//   version     : uint16   (= 1)
// ---------------------------------------------------------------------------

#include <cstdint>
#include <cstring>
#include <span>
#include <cassert>
#include <bit>

#include <rune/replication/Types.h>

// SBE-generated flyweight codecs (schema rune-replication.xml, schemaId 112).
// The records-carrying messages (FetchRequest/FetchResponse and their reconcile
// twins) delegate encode/decode/sizing to these so the outbound send path sizes
// its buffer by the GENERATED computeLength() — no hand-rolled magic constants,
// no fixed-cap stack buffer, no buffer overflow when a batch is large.
#include <rune_protocol_replication_sbe/MessageHeader.h>
#include <rune_protocol_replication_sbe/FetchRequest.h>
#include <rune_protocol_replication_sbe/FetchResponse.h>

namespace rune::replication {

namespace rsbe = ::rune::protocol::replication::sbe;

// ---------------------------------------------------------------------------
// Schema constants
// ---------------------------------------------------------------------------
inline constexpr uint16_t kSchemaId      = 112;
inline constexpr uint16_t kSchemaVersion = 2; // bumped: DR-BUG-5 added topicId to FetchRequest/FetchResponse

// ---------------------------------------------------------------------------
// Template IDs (assigned for the replication protocol)
// ---------------------------------------------------------------------------
enum class ReplicationTemplateId : uint16_t {
    FetchRequest             = 1,
    FetchResponse            = 2,
    OffsetsForLeaderEpochReq = 3,   // DR-BUG-6: KIP-101 epoch-boundary query
    OffsetsForLeaderEpochResp= 4,   // DR-BUG-6: KIP-101 epoch-boundary answer
    ReconcileFetchReq        = 5,   // DR-BUG-6: deficient-leader reverse pull (req)
    ReconcileFetchResp       = 6,   // DR-BUG-6: deficient-leader reverse pull (resp)
};

// DR-BUG-6: reconcile-pull template ids.  The reconcile request/response reuse
// the FetchRequest/FetchResponse WIRE LAYOUT but carry a DISTINCT templateId so
// the transport routes them to onReconcileRequest()/applyReconcileResponse()
// instead of the normal onFetchRequest()/applyReplicated() path.
inline constexpr uint16_t kReconcileFetchReqTemplateId  = 5;
inline constexpr uint16_t kReconcileFetchRespTemplateId = 6;

// ---------------------------------------------------------------------------
// Internal little-endian helpers (UB-free via memcpy; optimises to mov)
// ---------------------------------------------------------------------------
namespace detail {

// Enforce little-endian at compile time: schema 112 is little-endian SBE.
static_assert(std::endian::native == std::endian::little,
    "rune::replication SBE codecs assume a little-endian host "
    "(schema 112 is little-endian SBE)");

template<typename T>
inline void write_le(uint8_t* p, T v) noexcept {
    std::memcpy(p, &v, sizeof(T));
}

template<typename T>
inline T read_le(const uint8_t* p) noexcept {
    T v; std::memcpy(&v, p, sizeof(T)); return v;
}

} // namespace detail

// ---------------------------------------------------------------------------
// ReplicationMessageHeader — 8 bytes, little-endian
// Identical layout to ConsensusMessageHeader (schema 111) but with
// schemaId = 112.
// ---------------------------------------------------------------------------
struct ReplicationMessageHeader {
    uint16_t blockLength;
    uint16_t templateId;
    uint16_t schemaId;
    uint16_t version;

    static constexpr std::size_t ENCODED_SIZE = 8;

    /// Encode header into buf[offset..offset+8). Returns 8 (bytes written).
    static std::size_t encode(uint8_t* buf, std::size_t offset,
                              uint16_t blockLen,
                              uint16_t tmplId) noexcept
    {
        uint8_t* p = buf + offset;
        detail::write_le<uint16_t>(p + 0, blockLen);
        detail::write_le<uint16_t>(p + 2, tmplId);
        detail::write_le<uint16_t>(p + 4, kSchemaId);
        detail::write_le<uint16_t>(p + 6, kSchemaVersion);
        return ENCODED_SIZE;
    }

    /// Decode header from buf[offset..offset+8).
    static ReplicationMessageHeader decode(const uint8_t* buf,
                                           std::size_t offset) noexcept
    {
        const uint8_t* p = buf + offset;
        ReplicationMessageHeader h{};
        h.blockLength = detail::read_le<uint16_t>(p + 0);
        h.templateId  = detail::read_le<uint16_t>(p + 2);
        h.schemaId    = detail::read_le<uint16_t>(p + 4);
        h.version     = detail::read_le<uint16_t>(p + 6);
        return h;
    }
};

/// Peek the templateId from a buffer without decoding the full header.
inline uint16_t peekTemplateId(const uint8_t* buf,
                                std::size_t offset = 0) noexcept
{
    return detail::read_le<uint16_t>(buf + offset + 2);
}

/// DR-BUG-6: overwrite the templateId of an already-encoded frame (SBE header
/// byte +2).  Used to re-stamp a Fetch frame as a Reconcile frame so it routes
/// to the reconcile handlers, reusing the Fetch codec without duplication.
inline void stampTemplateId(uint8_t* buf, std::size_t offset,
                            uint16_t tid) noexcept
{
    detail::write_le<uint16_t>(buf + offset + 2, tid);
}

// ===========================================================================
// 1. FetchRequest  (templateId = 1)
//
// A follower sends FetchRequest to the partition leader to:
//   (a) Fetch records it is missing (from fetchOffset onward, up to maxBytes).
//   (b) Report its own logEndOffset (= fetchOffset) so the leader can
//       compute the high-watermark.
//
// Wire layout (fixed block, 32 bytes):  [schema version 2: DR-BUG-5 adds topicId]
//   leaderEpoch   : Epoch       (int64)  @ +0   leader epoch the follower expects
//   partition     : PartitionId (int32)  @ +8
//   followerId    : NodeId      (int32)  @ +12
//   fetchOffset   : Offset      (int64)  @ +16  follower logEndOffset / next wanted offset
//   maxBytes      : int32       @ +24   max bytes of records wanted (0 = position-only)
//   topicId       : int32       @ +28   topic identifier (DR-BUG-5: multi-topic routing)
// Total: 8 + 4 + 4 + 8 + 4 + 4 = 32 bytes
// ===========================================================================
struct FetchRequestMsg {
    Epoch       leaderEpoch;
    PartitionId partition;
    NodeId      followerId;
    Offset      fetchOffset;
    int32_t     maxBytes;
    int32_t     topicId;    ///< DR-BUG-5: distinguishes replicas that share a partitionId

    static constexpr uint16_t TEMPLATE_ID  = 1;
    static constexpr uint16_t BLOCK_LENGTH = 32; // 2×int64 + 4×int32 = 16 + 16 = 32
    static constexpr std::size_t TOTAL_SIZE =
        ReplicationMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    /// Encode FetchRequest into buf[offset]. Returns total bytes written.
    static std::size_t encode(uint8_t* buf, std::size_t offset,
                              Epoch       leaderEpoch_,
                              PartitionId partition_,
                              NodeId      followerId_,
                              Offset      fetchOffset_,
                              int32_t     maxBytes_,
                              int32_t     topicId_ = 0) noexcept
    {
        std::size_t pos = offset;
        pos += ReplicationMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p +  0, leaderEpoch_);
        detail::write_le<int32_t>(p +  8, partition_);
        detail::write_le<int32_t>(p + 12, followerId_);
        detail::write_le<int64_t>(p + 16, fetchOffset_);
        detail::write_le<int32_t>(p + 24, maxBytes_);
        detail::write_le<int32_t>(p + 28, topicId_);
        return TOTAL_SIZE;
    }

    /// Decode FetchRequest from buf[offset].
    static FetchRequestMsg decode(const uint8_t* buf,
                                  std::size_t offset) noexcept
    {
        const uint8_t* p = buf + offset + ReplicationMessageHeader::ENCODED_SIZE;
        FetchRequestMsg m{};
        m.leaderEpoch  = detail::read_le<int64_t>(p +  0);
        m.partition    = detail::read_le<int32_t>(p +  8);
        m.followerId   = detail::read_le<int32_t>(p + 12);
        m.fetchOffset  = detail::read_le<int64_t>(p + 16);
        m.maxBytes     = detail::read_le<int32_t>(p + 24);
        m.topicId      = detail::read_le<int32_t>(p + 28);
        return m;
    }
};

// ===========================================================================
// 2. FetchResponse  (templateId = 2)  — variable-length
//
// The leader sends FetchResponse to a follower in reply to FetchRequest.
// It carries the high-watermark (= committed offset, safe to deliver to
// consumers), the logStartOffset (earliest offset available in this epoch),
// an error code, and a variable-length records blob.
//
// Fixed block (34 bytes):  [schema version 2: DR-BUG-5 adds topicId]
//   leaderEpoch    : Epoch       (int64)  @ +0
//   partition      : PartitionId (int32)  @ +8
//   highWatermark  : Offset      (int64)  @ +12
//   logStartOffset : Offset      (int64)  @ +20
//   errorCode      : int16       @ +28
//   _pad           : int16       @ +30   (alignment padding; must be 0)
//   topicId        : int32       @ +32   (DR-BUG-5: multi-topic routing)
// Total fixed: 8 + 4 + 8 + 8 + 2 + 2 + 4 = 36 bytes... wait:
//   leaderEpoch(8) + partition(4) + highWatermark(8) + logStartOffset(8) +
//   errorCode(2) + _pad(2) + topicId(4) = 36 bytes
//
// Followed by variable-length records blob:
//   recordsLength  : int32       @ +36  (can be 0 for HW-only updates)
//   recordsData    : uint8[]     @ +40  (recordsLength bytes)
//
// Total on-wire = 8 (header) + 36 (fixed) + 4 (length prefix) + recordsLength
// ===========================================================================
struct FetchResponseMsg {
    Epoch                    leaderEpoch;
    PartitionId              partition;
    Offset                   highWatermark;
    Offset                   logStartOffset;
    int16_t                  errorCode;
    int32_t                  topicId;   ///< DR-BUG-5: distinguishes replicas that share a partitionId
    std::span<const uint8_t> records;   ///< Zero-copy view into the decode buf.

    static constexpr uint16_t TEMPLATE_ID = 2;
    // Layout constants DERIVED from the generated SBE codec (not hand-magic):
    // the fixed block length and the var-data (records) length-prefix width.
    static constexpr uint16_t    BLOCK_LENGTH_FIXED = rsbe::FetchResponse::sbeBlockLength();
    static constexpr std::size_t VAR_HEADER_SIZE    = 4; // varDataEncoding uint32 length prefix

    /// Total encoded frame size (SBE header + block + var-header + records),
    /// computed by the GENERATED codec — the single source of truth the send path
    /// uses to size its buffer.  No hand-rolled constants; overflow-proof.
    static std::size_t encodedSize(std::size_t recordsLen) noexcept
    {
        return rsbe::MessageHeader::encodedLength()
             + rsbe::FetchResponse::computeLength(recordsLen);
    }

    /// Encode via the generated SBE flyweight into buf[offset].  The caller MUST
    /// provide buf with at least encodedSize(records_.size()) bytes at offset.
    /// records_ may be empty (high-watermark-only update).  Returns bytes written.
    static std::size_t encode(uint8_t* buf, std::size_t offset,
                              Epoch                    leaderEpoch_,
                              PartitionId              partition_,
                              Offset                   highWatermark_,
                              Offset                   logStartOffset_,
                              int16_t                  errorCode_,
                              std::span<const uint8_t> records_,
                              int32_t                  topicId_ = 0) noexcept
    {
        const std::size_t frame = encodedSize(records_.size());
        rsbe::FetchResponse enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf), offset, offset + frame);
        enc.leaderEpoch(leaderEpoch_)
           .partition(partition_)
           .highWatermark(highWatermark_)
           .logStartOffset(logStartOffset_)
           .errorCode(errorCode_)
           .topicId(topicId_);
        enc.putRecords(reinterpret_cast<const char*>(records_.data()),
                       static_cast<std::uint32_t>(records_.size()));
        return frame;
    }

    /// Decode via the generated SBE flyweight.  records is a zero-copy view into
    /// buf — caller keeps buf alive while using the returned msg.  fragmentSize is
    /// the bytes available at buf[offset] (bounds-check); SIZE_MAX = legacy/skip.
    static FetchResponseMsg decode(const uint8_t* buf,
                                   std::size_t offset,
                                   std::size_t fragmentSize = SIZE_MAX) noexcept
    {
        char* b = reinterpret_cast<char*>(const_cast<uint8_t*>(buf));
        const std::uint64_t cap =
            (fragmentSize == SIZE_MAX) ? (offset + (std::uint64_t{1} << 30))
                                       : static_cast<std::uint64_t>(fragmentSize);
        rsbe::MessageHeader mh;
        mh.wrap(b, offset, 0, cap);
        rsbe::FetchResponse dec;
        dec.wrapForDecode(b, offset + rsbe::MessageHeader::encodedLength(),
                          mh.blockLength(), mh.version(), cap);
        FetchResponseMsg m{};
        m.leaderEpoch    = dec.leaderEpoch();
        m.partition      = dec.partition();
        m.highWatermark  = dec.highWatermark();
        m.logStartOffset = dec.logStartOffset();
        m.errorCode      = dec.errorCode();
        m.topicId        = dec.topicId();
        // Records tail: guard against a malformed/truncated length prefix (a
        // corrupt frame claiming more bytes than the fragment holds) BEFORE reading
        // the view — decode is noexcept and must never over-read or let an SBE
        // bounds-check throw escape.  recordsLength() only reads the 4-byte length
        // field (within the block), which is always present here.
        const std::uint32_t rlen = dec.recordsLength();
        const std::size_t framing = encodedSize(0);          // header + block + len-prefix
        const std::size_t avail   = (cap >= framing) ? static_cast<std::size_t>(cap - framing) : 0;
        if (rlen == 0 || rlen > avail) {
            m.records = {};                                   // empty or malformed → safe empty span
        } else {
            const auto sv = dec.getRecordsAsStringView();
            m.records = std::span<const uint8_t>{
                reinterpret_cast<const uint8_t*>(sv.data()), sv.size()};
        }
        return m;
    }
};

// ===========================================================================
// 3. OffsetsForLeaderEpochRequest  (templateId = 3)   [DR-BUG-6, KIP-101]
//
// A follower that has just learned of a new leader/epoch sends this to the NEW
// leader to discover the true divergence point BEFORE truncating.  It carries
// the follower's LATEST leader-epoch (the largest epoch it has records for) and
// its own log-end-offset.  The leader answers from its LeaderEpochCache.
//
// Wire layout (fixed block, 24 bytes):
//   currentEpoch   : Epoch       (int64)  @ +0   epoch this fence request is issued under
//   partition      : PartitionId (int32)  @ +8
//   followerId     : NodeId      (int32)  @ +12
//   requestedEpoch : Epoch       (int64)  @ +16  follower's latest leader-epoch
//   followerLeo    : Offset      (int64)  @ +24  follower's log-end-offset
//   topicId        : int32       @ +32
// Total block: 8+4+4+8+8+4 = 36 bytes
// ===========================================================================
struct OffsetsForLeaderEpochReqMsg {
    Epoch       currentEpoch;
    PartitionId partition;
    NodeId      followerId;
    Epoch       requestedEpoch;
    Offset      followerLeo;
    int32_t     topicId;

    static constexpr uint16_t TEMPLATE_ID  = 3;
    static constexpr uint16_t BLOCK_LENGTH = 36;
    static constexpr std::size_t TOTAL_SIZE =
        ReplicationMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static std::size_t encode(uint8_t* buf, std::size_t offset,
                              Epoch       currentEpoch_,
                              PartitionId partition_,
                              NodeId      followerId_,
                              Epoch       requestedEpoch_,
                              Offset      followerLeo_,
                              int32_t     topicId_ = 0) noexcept
    {
        std::size_t pos = offset;
        pos += ReplicationMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p +  0, currentEpoch_);
        detail::write_le<int32_t>(p +  8, partition_);
        detail::write_le<int32_t>(p + 12, followerId_);
        detail::write_le<int64_t>(p + 16, requestedEpoch_);
        detail::write_le<int64_t>(p + 24, followerLeo_);
        detail::write_le<int32_t>(p + 32, topicId_);
        return TOTAL_SIZE;
    }

    static OffsetsForLeaderEpochReqMsg decode(const uint8_t* buf,
                                              std::size_t offset) noexcept
    {
        const uint8_t* p = buf + offset + ReplicationMessageHeader::ENCODED_SIZE;
        OffsetsForLeaderEpochReqMsg m{};
        m.currentEpoch   = detail::read_le<int64_t>(p +  0);
        m.partition      = detail::read_le<int32_t>(p +  8);
        m.followerId     = detail::read_le<int32_t>(p + 12);
        m.requestedEpoch = detail::read_le<int64_t>(p + 16);
        m.followerLeo    = detail::read_le<int64_t>(p + 24);
        m.topicId        = detail::read_le<int32_t>(p + 32);
        return m;
    }
};

// ===========================================================================
// 4. OffsetsForLeaderEpochResponse  (templateId = 4)   [DR-BUG-6, KIP-101]
//
// The NEW leader's answer: for the requested epoch, the leader reports the
// epoch it and the follower actually share (responseEpoch, the largest cached
// epoch <= requestedEpoch, or -1 if none) and the end offset of that shared
// epoch on the leader's log (the start of the next epoch, or the leader's LEO
// if the shared epoch is still open).
//
// Wire layout (fixed block, 28 bytes):
//   leaderEpoch    : Epoch       (int64)  @ +0   leader's current epoch (for fencing)
//   partition      : PartitionId (int32)  @ +8
//   responseEpoch  : Epoch       (int64)  @ +12  shared epoch, or -1
//   endOffset      : Offset      (int64)  @ +20  end offset of the shared epoch
//   errorCode      : int16       @ +28
//   _pad           : int16       @ +30
//   topicId        : int32       @ +32
// Total block: 8+4+8+8+2+2+4 = 36 bytes
// ===========================================================================
struct OffsetsForLeaderEpochRespMsg {
    Epoch       leaderEpoch;
    PartitionId partition;
    Epoch       responseEpoch;
    Offset      endOffset;
    int16_t     errorCode;
    int32_t     topicId;

    static constexpr uint16_t TEMPLATE_ID  = 4;
    static constexpr uint16_t BLOCK_LENGTH = 36;
    static constexpr std::size_t TOTAL_SIZE =
        ReplicationMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static std::size_t encode(uint8_t* buf, std::size_t offset,
                              Epoch       leaderEpoch_,
                              PartitionId partition_,
                              Epoch       responseEpoch_,
                              Offset      endOffset_,
                              int16_t     errorCode_,
                              int32_t     topicId_ = 0) noexcept
    {
        std::size_t pos = offset;
        pos += ReplicationMessageHeader::encode(buf, pos, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + pos;
        detail::write_le<int64_t>(p +  0, leaderEpoch_);
        detail::write_le<int32_t>(p +  8, partition_);
        detail::write_le<int64_t>(p + 12, responseEpoch_);
        detail::write_le<int64_t>(p + 20, endOffset_);
        detail::write_le<int16_t>(p + 28, errorCode_);
        detail::write_le<int16_t>(p + 30, 0);     // alignment pad
        detail::write_le<int32_t>(p + 32, topicId_);
        return TOTAL_SIZE;
    }

    static OffsetsForLeaderEpochRespMsg decode(const uint8_t* buf,
                                               std::size_t offset) noexcept
    {
        const uint8_t* p = buf + offset + ReplicationMessageHeader::ENCODED_SIZE;
        OffsetsForLeaderEpochRespMsg m{};
        m.leaderEpoch   = detail::read_le<int64_t>(p +  0);
        m.partition     = detail::read_le<int32_t>(p +  8);
        m.responseEpoch = detail::read_le<int64_t>(p + 12);
        m.endOffset     = detail::read_le<int64_t>(p + 20);
        m.errorCode     = detail::read_le<int16_t>(p + 28);
        // p+30: alignment pad (skip)
        m.topicId       = detail::read_le<int32_t>(p + 32);
        return m;
    }
};

} // namespace rune::replication
