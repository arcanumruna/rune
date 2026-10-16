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

#include <algorithm>
#include <array>
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
#include <rune_protocol_replication_sbe/AlterIsr.h>
#include <rune_protocol_replication_sbe/AlterIsrResponse.h>
#include <rune_protocol_replication_sbe/InternalIsr.h>
#include <rune_protocol_replication_sbe/InternalIsrAck.h>

namespace rune::replication {

namespace rsbe = ::rune::protocol::replication::sbe;

// ---------------------------------------------------------------------------
// Schema constants
// ---------------------------------------------------------------------------
inline constexpr uint16_t kSchemaId      = 112;
inline constexpr uint16_t kSchemaVersion = 4; // v2: DR-BUG-5 topicId; v3: KIP-320/595 lastFetchedEpoch, AlterIsr; v4: ISR protocol (storage id, correlated AlterIsr answers)
/// The oldest schema version whose FetchResponse this binary still applies (v3 leaders send v3 headers during a roll).
inline constexpr uint16_t kMinReplicationVersion = 3;
/// Mixed v2/v3 replication is UNSUPPORTED (fail closed): a decoded v2
/// FetchRequest carries this lastFetchedEpoch sentinel (the leader refuses it
/// unless the follower's log is empty) and a decoded v2 FetchResponse carries
/// errorCode kErrUnsupportedWireVersion (the follower drops it).
inline constexpr int32_t kLastFetchedEpochV2Frame = -2;
inline constexpr int16_t kErrUnsupportedWireVersion = -9;

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
    AlterIsr                 = 7,   // partition leader → controller (Kafka AlterPartition)
    AlterIsrResponse         = 8,   // controller → partition leader
    InternalIsr              = 9,   // internal-partition leader → replicas
    InternalIsrAck           = 10,  // replica → internal-partition leader
    WriteTxnMarker           = 11,  // D9: txn coordinator → partition leader (hand-rolled)
    WriteTxnMarkerResponse   = 12,  // D9: partition leader → txn coordinator (hand-rolled)
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
// Wire layout (fixed block, 44 bytes):  [v2: DR-BUG-5 topicId; v3: lastFetchedEpoch @ +32,
//                                        incarnation @ +36 (KIP-903)]
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
    /// v3 (KIP-320/595): the epoch of the last batch in the follower's log
    /// (-1 = none, or a v2 sender: no lineage validation).  The leader checks
    /// (fetchOffset, lastFetchedEpoch) against its own epoch lineage on EVERY
    /// fetch and answers DivergingEpoch when it is not on it.
    int32_t     lastFetchedEpoch = -1;
    /// v3 (KIP-903): the follower's PROCESS incarnation (monotonic per node,
    /// bumped on every start; 0 = unknown / not tracked).  A leader ignores a
    /// fetch from an incarnation older than one it already saw from that
    /// follower (a frame of a dead process).
    int64_t     incarnation = 0;
    /// v4: the follower's storage id (0 = unknown / a v3 sender).
    int64_t     storageId = 0;
    /// v4: the start offset of the epoch of the follower's last batch (-1 = unknown / a v3 sender).
    int64_t     lastEpochStartOffset = -1;

    static constexpr uint16_t TEMPLATE_ID  = 1;
    static constexpr uint16_t BLOCK_LENGTH = 60; // 5×int64 + 5×int32 (v4)
    static_assert(BLOCK_LENGTH == rsbe::FetchRequest::sbeBlockLength());
    static constexpr uint16_t BLOCK_LENGTH_V3 = 44;
    static constexpr uint16_t BLOCK_LENGTH_V2 = 32;
    static constexpr std::size_t TOTAL_SIZE =
        ReplicationMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;
    /// Smallest decodable frame (a v2 sender): receivers bound-check against this.
    static constexpr std::size_t MIN_SIZE =
        ReplicationMessageHeader::ENCODED_SIZE + BLOCK_LENGTH_V2;

    /// Encode FetchRequest into buf[offset]. Returns total bytes written.
    static std::size_t encode(uint8_t* buf, std::size_t offset,
                              Epoch       leaderEpoch_,
                              PartitionId partition_,
                              NodeId      followerId_,
                              Offset      fetchOffset_,
                              int32_t     maxBytes_,
                              int32_t     topicId_ = 0,
                              int32_t     lastFetchedEpoch_ = -1,
                              int64_t     incarnation_ = 0,
                              int64_t     storageId_ = 0,
                              int64_t     lastEpochStartOffset_ = -1) noexcept
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
        detail::write_le<int32_t>(p + 32, lastFetchedEpoch_);
        detail::write_le<int64_t>(p + 36, incarnation_);
        detail::write_le<int64_t>(p + 44, storageId_);
        detail::write_le<int64_t>(p + 52, lastEpochStartOffset_);
        return TOTAL_SIZE;
    }

    /// Decode FetchRequest from buf[offset] (at least MIN_SIZE bytes of a frame of `len`
    /// bytes).  A v2 frame (blockLength 32) carries no lastFetchedEpoch: decoded as
    /// kLastFetchedEpochV2Frame (the leader fails closed on it).  Each later field is read
    /// only when the header's block covers it AND the frame holds it (a header may claim more
    /// than the frame carries): a v3 frame reads storageId 0 / lastEpochStartOffset -1.
    static FetchRequestMsg decode(const uint8_t* buf,
                                  std::size_t offset,
                                  std::size_t len) noexcept
    {
        const std::size_t avail = len > offset + ReplicationMessageHeader::ENCODED_SIZE
                                      ? len - offset - ReplicationMessageHeader::ENCODED_SIZE : 0;
        const std::size_t blockLen = std::min<std::size_t>(detail::read_le<uint16_t>(buf + offset), avail);
        const uint8_t* p = buf + offset + ReplicationMessageHeader::ENCODED_SIZE;
        FetchRequestMsg m{};
        m.leaderEpoch  = detail::read_le<int64_t>(p +  0);
        m.partition    = detail::read_le<int32_t>(p +  8);
        m.followerId   = detail::read_le<int32_t>(p + 12);
        m.fetchOffset  = detail::read_le<int64_t>(p + 16);
        m.maxBytes     = detail::read_le<int32_t>(p + 24);
        m.topicId      = detail::read_le<int32_t>(p + 28);
        m.lastFetchedEpoch = blockLen >= 36 ? detail::read_le<int32_t>(p + 32)
                                            : kLastFetchedEpochV2Frame;
        m.incarnation = blockLen >= BLOCK_LENGTH_V3 ? detail::read_le<int64_t>(p + 36) : 0;
        m.storageId   = blockLen >= 52 ? detail::read_le<int64_t>(p + 44) : 0;
        m.lastEpochStartOffset = blockLen >= BLOCK_LENGTH ? detail::read_le<int64_t>(p + 52) : -1;
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
// Fixed block (34 bytes, sbeBlockLength=34; unchanged in v3 — data frames keep
// their 46-byte framing):  [schema version 2: DR-BUG-5 adds topicId]
// v3: a record-less ERROR answer may carry a 12-byte payload in `records` — see
// ErrorPayload (DivergingEpoch / OffsetOutOfRange).
//   leaderEpoch    : Epoch       (int64)  @ +0
//   partition      : PartitionId (int32)  @ +8
//   highWatermark  : Offset      (int64)  @ +12
//   logStartOffset : Offset      (int64)  @ +20
//   errorCode      : int16       @ +28
//   topicId        : int32       @ +30   (DR-BUG-5: multi-topic routing; NO alignment pad)
// Total fixed: leaderEpoch(8) + partition(4) + highWatermark(8) + logStartOffset(8) +
//   errorCode(2) + topicId(4) = 34 bytes  (matches generated SBE sbeBlockLength=34;
//   topicIdEncodingOffset()=30, errorCodeEncodingOffset()=28)
//
// Followed by variable-length records blob:
//   recordsLength  : uint32      @ +34  (can be 0 for HW-only updates)
//   recordsData    : uint8[]     @ +38  (recordsLength bytes)
//
// Total on-wire = 8 (header) + 34 (fixed) + 4 (length prefix) + recordsLength
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
    /// Smallest decodable frame: a v2 sender's (34-byte block) empty answer.
    static constexpr std::size_t MIN_SIZE = 8 + 34 + 4;
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
        // header + the frame's OWN block (v2: 34, v3: 46) + len-prefix
        const std::size_t framing = rsbe::MessageHeader::encodedLength() + mh.blockLength()
                                  + VAR_HEADER_SIZE;
        const std::size_t avail   = (cap >= framing) ? static_cast<std::size_t>(cap - framing) : 0;
        if (rlen == 0 || rlen > avail) {
            m.records = {};                                   // empty or malformed → safe empty span
        } else {
            const auto sv = dec.getRecordsAsStringView();
            m.records = std::span<const uint8_t>{
                reinterpret_cast<const uint8_t*>(sv.data()), sv.size()};
        }
        // Mixed v2/v3 is unsupported: a v2 leader's frame is never applied.
        if (mh.version() < kMinReplicationVersion) {
            m.errorCode = kErrUnsupportedWireVersion;
            m.records   = {};
        }
        return m;
    }
};

// ---------------------------------------------------------------------------
// v3 error payload — a record-less FetchResponse ERROR answer may carry
//   int32 epoch | int64 endOffset   (12 bytes, LE) in its `records` field:
//   DivergingEpoch (-7): the leader's endOffsetForEpoch(lastFetchedEpoch);
//   OffsetOutOfRange (-4): epoch -1, endOffset = the leader's log end offset.
// Data frames are untouched (their framing stays 46 bytes).
// ---------------------------------------------------------------------------
struct ErrorPayload {
    int32_t epoch     = -1;
    Offset  endOffset = -1;
    static constexpr std::size_t SIZE = 12;
    void encode(uint8_t* out) const noexcept {
        detail::write_le<int32_t>(out, epoch);
        detail::write_le<int64_t>(out + 4, endOffset);
    }
    [[nodiscard]] static bool decode(std::span<const uint8_t> in, ErrorPayload& out) noexcept {
        if (in.size() != SIZE) return false;
        out.epoch     = detail::read_le<int32_t>(in.data());
        out.endOffset = detail::read_le<int64_t>(in.data() + 4);
        return true;
    }
};

/// Owns an ErrorPayload's bytes for a FetchResponseMsg::records view (tests,
/// harnesses).
struct ErrorPayloadBuf {
    std::array<uint8_t, ErrorPayload::SIZE> bytes{};
    std::span<const uint8_t> set(int32_t epoch, Offset endOffset) noexcept {
        ErrorPayload{epoch, endOffset}.encode(bytes.data());
        return {bytes.data(), bytes.size()};
    }
};

// ---------------------------------------------------------------------------
// AlterIsr (template 7, v3) — Kafka AlterPartition: the partition leader
// proposes an ISR change to the controller, which commits it to the metadata
// log only while `partitionEpoch` (the epoch of the committed assignment the
// proposal was derived from) is still the committed partitionEpoch —
// compare-and-set on a monotonic version (no ABA).  Generated SBE codec.
// ---------------------------------------------------------------------------
/// D15 — the cluster's METADATA FEATURE LEVEL (KRaft metadata.version): the
/// level is committed to __cluster_metadata (FeatureLevelRecord) by the
/// controller only once EVERY cluster member advertised support for it
/// (AlterIsrMsg::kNodeFeatureLevelSlot), or at the bootstrap of a fresh
/// metadata log.  It gates ONLY request / record kinds an older peer cannot
/// handle — never a safety mechanism that already shipped (review M1): the
/// leader-epoch fence protocol (refuse + bump + leaderless reports, 9c80eab)
/// is part of kBase and is unconditional.  Monotonic.
namespace MetadataFeature {
/// main as of 9c80eab (the epoch fence protocol included).
inline constexpr int16_t kBase = 0;
/// D15: leaderless marks on NO_LEADER records (trailer) — older binaries
/// ignore them, so nothing is gated on it yet; future request kinds an older
/// controller would drop are gated on a level above it.
inline constexpr int16_t kLeaderlessMarks = 1;
/// ISR protocol (storage identity): gates the storage strip, the NodeStorageRecord
/// confirmation and the StorageReset leaderless mark.
inline constexpr int16_t kStorageIdentity = 2;
/// The level this binary supports.
inline constexpr int16_t kLatest = kStorageIdentity;
} // namespace MetadataFeature

struct AlterIsrMsg {
    static constexpr uint16_t TEMPLATE_ID = 7;
    static constexpr int kMaxIsr = 16;
    /// isrCount == kLeaderEpochBump: not an ISR change — "the committed
    /// (leaderId, leaderEpoch, partitionEpoch) assignment names me leader at an
    /// epoch I already led at in an earlier incarnation: re-elect me at a NEW
    /// epoch" (PartitionReplica leader-epoch restart fence).  The controller
    /// commits the same assignment at leaderEpoch + 1.  An older controller
    /// ignores it (isrCount <= 0 was always dropped unanswered).
    static constexpr int32_t kLeaderEpochBump = 0;
    /// Bump request payload (isrCount == kLeaderEpochBump): isr[0] = the
    /// requester's fence epoch, isr[1] = flags, isr[2] = its log end offset
    /// (clamped to int32).  kBumpFenceValid: isr[0] is meaningful.
    /// kBumpMemoryBacked: a memory log (empty after a restart).
    static constexpr int32_t kBumpFenceValid   = 1;
    static constexpr int32_t kBumpMemoryBacked = 2;
    /// Fix round 2 N1 (KIP-112): the requester's log dir is OFFLINE (a fence
    /// write failed) — never (re-)elect it until a new process incarnation.
    static constexpr int32_t kBumpLogDirFailed = 4;
    /// Fix round 2 N1/N2: not a bump — a FOLLOWER REPORT of a replica of a
    /// LEADERLESS partition (sent once per leaderless epoch, re-sent until
    /// answered): isr[2] = its LEO, isr[3]/isr[4] = its process incarnation
    /// (low/high 32 bits).  `leaderId` is the reporting replica.
    static constexpr int32_t kBumpFollowerReport = 8;
    /// Catalog history Task 11 (fix round 4): not a bump — a LIVE, periodic
    /// report by node `leaderId` that it is in (topicId, partition)'s replicas but
    /// cannot serve it (its catalog never applied the create, or its apply
    /// halted).  The controller keeps it only while refreshed and excludes the
    /// node from that partition's leader ELECTION — nothing else (no replica-set
    /// or ISR edit, no metadata record).
    static constexpr int32_t kBumpReplicaUnservable = 1024;
    /// Controlled shutdown (Kafka parity) — NODE-level requests on the same
    /// channel: topicId == kNodeControlTopicId (no partition), leaderId = the
    /// requesting node, isrCount == kLeaderEpochBump, isr[1] = flags,
    /// isr[3]/isr[4] = its process incarnation (low/high 32 bits).
    ///   kNodeShutdown: "I am stopping gracefully — move every partition I
    ///     lead to another in-sync replica, never elect me, drop me from the
    ///     ISRs" (re-sent until this controller acked it).
    ///   kNodeRegister: "I am incarnation X and NOT stopping" (sent once per
    ///     start until acked) — clears a mark left by an earlier incarnation.
    /// The controller answers with an AlterIsrResponse carrying the same
    /// topicId, leaderId = the requester and the incarnation in
    /// (leaderEpoch = low 32 bits, partitionEpoch = high 32 bits).  An older
    /// controller drops it unanswered (topicId < 0, not a coordinator key).
    static constexpr int32_t kNodeControlTopicId = -100;
    static constexpr int32_t kNodeShutdown       = 16;
    static constexpr int32_t kNodeRegister       = 32;
    /// kNodeShutdown is sent to EVERY member (each records the mark — a
    /// successor controller already knows it).  kNodeShutdownUserOnly: the
    /// requester is the controller — completion counts user partitions only
    /// (its coordinator partitions move after it resigned).
    static constexpr int32_t kNodeShutdownUserOnly = 64;
    /// A member's CONFIRMATION to the controller (sent when its committed map
    /// changes, and on every request it receives): leaderId = the stopping
    /// node, isr[2] = the confirming member, isr[3]/isr[4] = the stopping
    /// node's incarnation, isr[5] = kConfirmUserClear | kConfirmAllClear —
    /// in ITS committed map the stopping node leads / is in-sync for nothing
    /// movable (so its routing no longer names it).
    static constexpr int32_t kNodeShutdownConfirm = 128;
    /// E2 final fix wave (C1 multi-node): not a bump — a TRANSIENT partition
    /// LEADER's data-lineage report for its leadership (`leaderId`,
    /// `leaderEpoch`): isr[2] = the leader epoch of the data its ring holds
    /// (-1: none — its ring is unborn); isr[3]/isr[4] = the reporting
    /// process's node-control identity (low/high 32 bits; residual N1).  Re-sent on the AlterIsr cadence until
    /// answered kAlterIsrLineageConfirmed (committed) or FencedLeaderEpoch.
    /// An older controller drops it (no answer; the leader keeps re-sending).
    static constexpr int32_t kLineageReport = 256;
    /// Catalog history Task 11 (fix round 0): a node-control REGISTER from a
    /// process whose CatalogApplier HALTED ("I am halted") — sent to EVERY member
    /// (each records the mark for that process; a new process clears it).  The
    /// controller never places a new partition on a halted node, and a halted
    /// controller hands the controllership to a healthy peer when one is live.
    static constexpr int32_t kNodeCatalogHalted = 512;
    static constexpr int32_t kConfirmUserClear = 1;
    static constexpr int32_t kConfirmAllClear  = 2;
    /// The controller's answer (responder id in `partition`): kAlterIsrOk =
    /// COMPLETE — clear in its own map AND confirmed clear by every other live
    /// member; kNodeShutdownPending = marked, not complete yet.
    static constexpr int16_t kNodeShutdownPending = 6;
    /// Any member's answer to a request from a SUPERSEDED process identity
    /// (fix round 2, N1): the receiving process is alive by definition — it
    /// rotates to a fresh identity and re-sends at once.
    static constexpr int16_t kNodeSuperseded = 7;
    /// D15 (metadata.version gate): every node-control request carries the
    /// highest metadata feature level its binary supports in this slot (an
    /// older binary leaves it 0 = MetadataFeature::kBase).
    static constexpr int kNodeFeatureLevelSlot = 6;
    /// ISR protocol (storage identity): a node-control request carries the sender's storage id
    /// in slots 7 (low 32 bits) / 8 (high 32 bits) and its origin in slot 9.
    static constexpr int kNodeStorageLoSlot     = 7;
    static constexpr int kNodeStorageHiSlot     = 8;
    static constexpr int kNodeStorageOriginSlot = 9;
    static constexpr int32_t kStorageOriginNone      = 0;
    static constexpr int32_t kStorageOriginFounder   = 1;
    static constexpr int32_t kStorageOriginLegacy    = 2;
    static constexpr int32_t kStorageOriginJoiner    = 3;
    static constexpr int32_t kStorageOriginUndecided = 4;
    /// Not a bump: a follower's below-committed report (its log end is below the HW it learned
    /// in its leadership).  isr[1] = this flag, `leaderId` = the reporter, isr[2] = the floor
    /// (clamped to int32), isr[3]/isr[4] = the reporter's incarnation, `proposalId` = reportId.
    static constexpr int32_t kBumpLeaderBelowCommitted = 2048;
    int32_t topicId        = 0;
    int32_t partition      = 0;
    NodeId  leaderId       = -1;
    Epoch   leaderEpoch    = -1;
    int32_t isrCount       = 0;
    std::array<int32_t, kMaxIsr> isr{};
    int32_t partitionEpoch = -1;
    /// v4: correlation only - never part of the CAS; 0 = a v3 sender.
    int64_t proposalId = 0;
    /// v4: the storage id of each proposed ISR member, parallel to `isr`.
    std::array<int64_t, kMaxIsr> isrStorage{};
    /// v4: 1-based ordinal of this send of `proposalId`; 0 = a v3 sender.
    int32_t sendSeq = 0;

    static constexpr std::size_t V3_BLOCK = 92;
    static constexpr std::size_t TOTAL_SIZE =
        rsbe::MessageHeader::encodedLength() + rsbe::AlterIsr::sbeBlockLength();

    std::size_t encode(uint8_t* buf, std::size_t cap) const noexcept {
        if (cap < TOTAL_SIZE) return 0;
        rsbe::AlterIsr enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf), 0, cap);
        enc.topicId(topicId).partition(partition).leaderId(leaderId)
           .leaderEpoch(leaderEpoch).isrCount(isrCount).partitionEpoch(partitionEpoch)
           .proposalId(proposalId).sendSeq(sendSeq);
        for (int i = 0; i < kMaxIsr; ++i) {
            enc.isr(static_cast<std::uint64_t>(i), isr[static_cast<std::size_t>(i)]);
            enc.isrStorage(static_cast<std::uint64_t>(i), isrStorage[static_cast<std::size_t>(i)]);
        }
        return TOTAL_SIZE;
    }
    /// Accepts a v3 frame (block 92) as well as v4+ (block >= 232): the v4 fields read 0 when the
    /// sender's acting version predates them.
    [[nodiscard]] static bool decode(const uint8_t* buf, std::size_t len, AlterIsrMsg& m) noexcept {
        if (len < rsbe::MessageHeader::encodedLength() + V3_BLOCK) return false;
        char* b = reinterpret_cast<char*>(const_cast<uint8_t*>(buf));
        rsbe::MessageHeader mh;
        mh.wrap(b, 0, 0, len);
        if (mh.templateId() != TEMPLATE_ID || mh.blockLength() < V3_BLOCK) return false;
        if (len < rsbe::MessageHeader::encodedLength() + mh.blockLength()) return false;
        rsbe::AlterIsr dec;
        dec.wrapForDecode(b, rsbe::MessageHeader::encodedLength(), mh.blockLength(), mh.version(), len);
        m.topicId        = dec.topicId();
        m.partition      = dec.partition();
        m.leaderId       = dec.leaderId();
        m.leaderEpoch    = dec.leaderEpoch();
        m.isrCount       = dec.isrCount();
        m.partitionEpoch = dec.partitionEpoch();
        if (m.isrCount < 0 || m.isrCount > kMaxIsr) return false;
        for (int i = 0; i < kMaxIsr; ++i)
            m.isr[static_cast<std::size_t>(i)] = dec.isr(static_cast<std::uint64_t>(i));
        // A v4 field is read only when the sender's acting version has it AND its header block covers it: a
        // header claiming version 4 with a v3-sized block would otherwise read past the frame.
        const std::size_t bl = mh.blockLength();
        m.proposalId = (dec.proposalIdInActingVersion() && bl >= 100) ? dec.proposalId() : 0;
        m.sendSeq    = (dec.sendSeqInActingVersion() && bl >= 232) ? dec.sendSeq() : 0;
        for (int i = 0; i < kMaxIsr; ++i)
            m.isrStorage[static_cast<std::size_t>(i)] =
                (dec.isrStorageInActingVersion() && bl >= 228) ? dec.isrStorage(static_cast<std::uint64_t>(i)) : 0;
        return true;
    }
};

// AlterIsrResponse error codes (Kafka AlterPartition errors).
inline constexpr int16_t kAlterIsrOk                   = 0;  ///< committed / already committed at partitionEpoch
inline constexpr int16_t kAlterIsrIneligibleReplica    = 1;  ///< every added replica is fenced (KIP-841): nothing to commit
inline constexpr int16_t kAlterIsrFencedLeaderEpoch    = 2;  ///< not the current leader / leader epoch
inline constexpr int16_t kAlterIsrInvalidUpdateVersion = 3;  ///< partitionEpoch is not the committed one
inline constexpr int16_t kAlterIsrInvalidRequest       = 4;  ///< malformed (leader missing, non-replica)
/// E2 final fix wave (C1 multi-node): the leader's TransientLineage report for
/// its leader epoch is COMMITTED (the leader stops re-sending it).
inline constexpr int16_t kAlterIsrLineageConfirmed     = 5;
/// E2 residual round 5 (R2): NODE-LOCAL only, never on the wire — the node's
/// own runtime tells a user replica (on its owner thread, through the existing
/// AlterIsrResponse op; the identity rides in the op's nextOffset slot) that
/// this process's node-control identity ROTATED.  The replica adopts it and,
/// leading a transient partition, re-reports its lineage under it.
inline constexpr int16_t kAlterIsrLocalLineageIdentity = 100;
/// ISR protocol: NODE-LOCAL only, never on the wire - the node's own runtime tells a held
/// leader role (storage not confirmed) to ask for a failover through the restart-fence bump.
inline constexpr int16_t kAlterIsrLocalHoldLeadership = 101;

/// A controller answer stamped with a term below the newest term this node has learned
/// comes from a deposed controller.  answer 0 = a v3 sender (no term): never stale.
inline bool isStaleControllerAnswer(int64_t answer, int64_t known) noexcept { return answer > 0 && answer < known; }

// ---------------------------------------------------------------------------
// AlterIsrResponse (template 8, v3) — Kafka AlterPartitionResponse.  The leader
// clears its pending proposal only on a commit (the applied assignment) or on
// an explicit reject here; a transient refusal (another change in flight)
// gets no answer and the proposal is re-sent.
// ---------------------------------------------------------------------------
struct AlterIsrResponseMsg {
    static constexpr uint16_t TEMPLATE_ID = 8;
    int32_t topicId        = 0;
    int32_t partition      = 0;
    NodeId  leaderId       = -1;
    Epoch   leaderEpoch    = -1;
    int32_t partitionEpoch = -1;   ///< the controller's committed partitionEpoch
    int16_t errorCode      = kAlterIsrOk;
    /// v4: echoes of the request's proposalId / sendSeq (0 = answering a v3 request), and the
    /// term of the answering controller (0 = a v3 controller).
    int64_t proposalId      = 0;
    int64_t controllerEpoch = 0;
    int32_t sendSeq         = 0;

    static constexpr std::size_t V3_BLOCK = 26;
    static constexpr std::size_t TOTAL_SIZE =
        rsbe::MessageHeader::encodedLength() + rsbe::AlterIsrResponse::sbeBlockLength();

    std::size_t encode(uint8_t* buf, std::size_t cap) const noexcept {
        if (cap < TOTAL_SIZE) return 0;
        rsbe::AlterIsrResponse enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf), 0, cap);
        enc.topicId(topicId).partition(partition).leaderId(leaderId)
           .leaderEpoch(leaderEpoch).partitionEpoch(partitionEpoch).errorCode(errorCode)
           .proposalId(proposalId).controllerEpoch(controllerEpoch).sendSeq(sendSeq);
        return TOTAL_SIZE;
    }
    [[nodiscard]] static bool decode(const uint8_t* buf, std::size_t len, AlterIsrResponseMsg& m) noexcept {
        if (len < rsbe::MessageHeader::encodedLength() + V3_BLOCK) return false;
        char* b = reinterpret_cast<char*>(const_cast<uint8_t*>(buf));
        rsbe::MessageHeader mh;
        mh.wrap(b, 0, 0, len);
        if (mh.templateId() != TEMPLATE_ID || mh.blockLength() < V3_BLOCK) return false;
        if (len < rsbe::MessageHeader::encodedLength() + mh.blockLength()) return false;
        rsbe::AlterIsrResponse dec;
        dec.wrapForDecode(b, rsbe::MessageHeader::encodedLength(), mh.blockLength(), mh.version(), len);
        m.topicId        = dec.topicId();
        m.partition      = dec.partition();
        m.leaderId       = dec.leaderId();
        m.leaderEpoch    = dec.leaderEpoch();
        m.partitionEpoch = dec.partitionEpoch();
        m.errorCode      = dec.errorCode();
        const std::size_t bl = mh.blockLength();   // see AlterIsrMsg::decode: acting version AND block coverage
        m.proposalId      = (dec.proposalIdInActingVersion() && bl >= 34) ? dec.proposalId() : 0;
        m.controllerEpoch = (dec.controllerEpochInActingVersion() && bl >= 42) ? dec.controllerEpoch() : 0;
        m.sendSeq         = (dec.sendSeqInActingVersion() && bl >= 46) ? dec.sendSeq() : 0;
        return true;
    }
};

// ---------------------------------------------------------------------------
// InternalIsr (template 9, v3) — the ISR of an INTERNAL partition (led by the
// consensus leader), published by its leader to every replica.  Each replica
// persists the newest (leaderEpoch, version) it sees (InternalIsrKnowledge)
// and acks it; the change commits once a majority of the cluster persisted it.
// ---------------------------------------------------------------------------
struct InternalIsrMsg {
    static constexpr uint16_t TEMPLATE_ID = 9;
    static constexpr int kMaxIsr = 16;
    int32_t topicId     = 0;
    int32_t partition   = 0;
    NodeId  leaderId    = -1;
    Epoch   leaderEpoch = -1;
    int64_t version     = 0;
    int32_t isrCount    = 0;
    std::array<int32_t, kMaxIsr> isr{};

    static constexpr std::size_t TOTAL_SIZE =
        rsbe::MessageHeader::encodedLength() + rsbe::InternalIsr::sbeBlockLength();

    std::size_t encode(uint8_t* buf, std::size_t cap) const noexcept {
        if (cap < TOTAL_SIZE) return 0;
        rsbe::InternalIsr enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf), 0, cap);
        enc.topicId(topicId).partition(partition).leaderId(leaderId)
           .leaderEpoch(leaderEpoch).version(version).isrCount(isrCount);
        for (int i = 0; i < kMaxIsr; ++i)
            enc.isr(static_cast<std::uint64_t>(i), isr[static_cast<std::size_t>(i)]);
        return TOTAL_SIZE;
    }
    [[nodiscard]] static bool decode(const uint8_t* buf, std::size_t len, InternalIsrMsg& m) noexcept {
        if (len < TOTAL_SIZE) return false;
        char* b = reinterpret_cast<char*>(const_cast<uint8_t*>(buf));
        rsbe::MessageHeader mh;
        mh.wrap(b, 0, 0, len);
        if (mh.templateId() != TEMPLATE_ID || mh.blockLength() < rsbe::InternalIsr::sbeBlockLength()) return false;
        rsbe::InternalIsr dec;
        dec.wrapForDecode(b, rsbe::MessageHeader::encodedLength(), mh.blockLength(), mh.version(), len);
        m.topicId     = dec.topicId();
        m.partition   = dec.partition();
        m.leaderId    = dec.leaderId();
        m.leaderEpoch = dec.leaderEpoch();
        m.version     = dec.version();
        m.isrCount    = dec.isrCount();
        if (m.isrCount < 0 || m.isrCount > kMaxIsr) return false;
        for (int i = 0; i < kMaxIsr; ++i)
            m.isr[static_cast<std::size_t>(i)] = dec.isr(static_cast<std::uint64_t>(i));
        return true;
    }
};

// ---------------------------------------------------------------------------
// InternalIsrAck (template 10, v3) — "I durably hold InternalIsr (leaderEpoch,
// version) or newer."
// ---------------------------------------------------------------------------
struct InternalIsrAckMsg {
    static constexpr uint16_t TEMPLATE_ID = 10;
    int32_t topicId     = 0;
    int32_t partition   = 0;
    NodeId  followerId  = -1;
    Epoch   leaderEpoch = -1;
    int64_t version     = 0;

    static constexpr std::size_t TOTAL_SIZE =
        rsbe::MessageHeader::encodedLength() + rsbe::InternalIsrAck::sbeBlockLength();

    std::size_t encode(uint8_t* buf, std::size_t cap) const noexcept {
        if (cap < TOTAL_SIZE) return 0;
        rsbe::InternalIsrAck enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf), 0, cap);
        enc.topicId(topicId).partition(partition).followerId(followerId)
           .leaderEpoch(leaderEpoch).version(version);
        return TOTAL_SIZE;
    }
    [[nodiscard]] static bool decode(const uint8_t* buf, std::size_t len, InternalIsrAckMsg& m) noexcept {
        if (len < TOTAL_SIZE) return false;
        char* b = reinterpret_cast<char*>(const_cast<uint8_t*>(buf));
        rsbe::MessageHeader mh;
        mh.wrap(b, 0, 0, len);
        if (mh.templateId() != TEMPLATE_ID || mh.blockLength() < rsbe::InternalIsrAck::sbeBlockLength()) return false;
        rsbe::InternalIsrAck dec;
        dec.wrapForDecode(b, rsbe::MessageHeader::encodedLength(), mh.blockLength(), mh.version(), len);
        m.topicId     = dec.topicId();
        m.partition   = dec.partition();
        m.followerId  = dec.followerId();
        m.leaderEpoch = dec.leaderEpoch();
        m.version     = dec.version();
        return true;
    }
};

// ===========================================================================
// D9 — cross-node transaction markers (templates 11 / 12).
//
// The transaction coordinator writes a COMMIT/ABORT control marker to a
// partition led by ANOTHER node by sending it WriteTxnMarker over this cluster
// plane; the partition leader appends it on the owning worker (same op-ring
// path as a local marker) and answers WriteTxnMarkerResponse (Kafka
// WriteTxnMarkers, one partition per request).  Hand-rolled fixed blocks (like
// PlaneHello) — cold control traffic, little-endian, no allocation; the
// marker bytes are a zero-copy view into the frame on decode.
//
// WriteTxnMarker (template 11), fixed block 44 bytes + marker bytes:
//   coordinatorNodeId : int32 @ +0
//   coordinatorEpoch  : int32 @ +4
//   token             : int64 @ +8    coordinator-side marker token (echoed back)
//   topicId           : int32 @ +16
//   partition         : int32 @ +20
//   producerId        : int64 @ +24
//   commit            : uint8 @ +32   (+33..35 pad)
//   markerLen         : uint32 @ +36  (<= kMaxMarkerBytes)
//   leaderEpoch       : int32 @ +40   the TARGET partition's leader epoch the
//                                     coordinator routed it under (re-review
//                                     MEDIUM-3: the leader refuses an older one;
//                                     -1 = unknown, not checked)
//   marker            : markerLen bytes @ +44 (the encoded control batch)
// ===========================================================================
struct WriteTxnMarkerMsg {
    static constexpr uint16_t    TEMPLATE_ID     = 11;
    static constexpr uint16_t    BLOCK_LENGTH    = 44;
    static constexpr std::size_t kFixedSize      = ReplicationMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;
    static constexpr std::size_t kMaxMarkerBytes = 128;
    static constexpr std::size_t kMaxSize        = kFixedSize + kMaxMarkerBytes;

    int32_t coordinatorNodeId = -1;
    int32_t coordinatorEpoch  = -1;
    int64_t token             = 0;
    int32_t topicId           = 0;
    int32_t partition         = 0;
    int64_t producerId        = -1;
    uint8_t commit            = 0;
    int32_t leaderEpoch       = -1;
    std::span<const uint8_t> marker{};   ///< encode: source; decode: view into the frame

    /// Bytes written, or 0 (marker above kMaxMarkerBytes / cap too small).
    std::size_t encode(uint8_t* buf, std::size_t cap) const noexcept {
        if (marker.size() > kMaxMarkerBytes) return 0;
        const std::size_t total = kFixedSize + marker.size();
        if (cap < total) return 0;
        ReplicationMessageHeader::encode(buf, 0, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + ReplicationMessageHeader::ENCODED_SIZE;
        detail::write_le<int32_t>(p + 0,  coordinatorNodeId);
        detail::write_le<int32_t>(p + 4,  coordinatorEpoch);
        detail::write_le<int64_t>(p + 8,  token);
        detail::write_le<int32_t>(p + 16, topicId);
        detail::write_le<int32_t>(p + 20, partition);
        detail::write_le<int64_t>(p + 24, producerId);
        p[32] = commit; p[33] = 0; p[34] = 0; p[35] = 0;
        detail::write_le<uint32_t>(p + 36, static_cast<uint32_t>(marker.size()));
        detail::write_le<int32_t>(p + 40, leaderEpoch);
        if (!marker.empty()) std::memcpy(p + BLOCK_LENGTH, marker.data(), marker.size());
        return total;
    }
    /// False on a truncated frame, a foreign template id or a marker length
    /// above kMaxMarkerBytes / beyond the frame.
    [[nodiscard]] static bool decode(const uint8_t* buf, std::size_t len, WriteTxnMarkerMsg& m) noexcept {
        if (len < kFixedSize) return false;
        const auto h = ReplicationMessageHeader::decode(buf, 0);
        if (h.templateId != TEMPLATE_ID || h.blockLength < BLOCK_LENGTH) return false;
        const uint8_t* p = buf + ReplicationMessageHeader::ENCODED_SIZE;
        const uint32_t markerLen = detail::read_le<uint32_t>(p + 36);
        if (markerLen > kMaxMarkerBytes) return false;
        const std::size_t start = ReplicationMessageHeader::ENCODED_SIZE + h.blockLength;
        if (len < start || len - start < markerLen) return false;
        m.coordinatorNodeId = detail::read_le<int32_t>(p + 0);
        m.coordinatorEpoch  = detail::read_le<int32_t>(p + 4);
        m.token             = detail::read_le<int64_t>(p + 8);
        m.topicId           = detail::read_le<int32_t>(p + 16);
        m.partition         = detail::read_le<int32_t>(p + 20);
        m.producerId        = detail::read_le<int64_t>(p + 24);
        m.commit            = p[32];
        m.leaderEpoch       = detail::read_le<int32_t>(p + 40);
        m.marker            = std::span<const uint8_t>(buf + start, markerLen);
        return true;
    }
};

// ---------------------------------------------------------------------------
// WriteTxnMarkerResponse (template 12), fixed block 40 bytes:
//   token            : int64 @ +0    the coordinator's token (from the request)
//   topicId          : int32 @ +8
//   partition        : int32 @ +12
//   producerId       : int64 @ +16
//   coordinatorEpoch : int32 @ +24
//   errorCode        : int16 @ +28   client ErrorCode (0 = landed)  (+30..31 pad)
//   offset           : int64 @ +32   where the marker landed (-1 when refused)
// ---------------------------------------------------------------------------
struct WriteTxnMarkerResponseMsg {
    static constexpr uint16_t    TEMPLATE_ID  = 12;
    static constexpr uint16_t    BLOCK_LENGTH = 40;
    static constexpr std::size_t TOTAL_SIZE   = ReplicationMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    int64_t token            = 0;
    int32_t topicId          = 0;
    int32_t partition        = 0;
    int64_t producerId       = -1;
    int32_t coordinatorEpoch = -1;
    int16_t errorCode        = 0;
    int64_t offset           = -1;

    std::size_t encode(uint8_t* buf, std::size_t cap) const noexcept {
        if (cap < TOTAL_SIZE) return 0;
        ReplicationMessageHeader::encode(buf, 0, BLOCK_LENGTH, TEMPLATE_ID);
        uint8_t* p = buf + ReplicationMessageHeader::ENCODED_SIZE;
        detail::write_le<int64_t>(p + 0,  token);
        detail::write_le<int32_t>(p + 8,  topicId);
        detail::write_le<int32_t>(p + 12, partition);
        detail::write_le<int64_t>(p + 16, producerId);
        detail::write_le<int32_t>(p + 24, coordinatorEpoch);
        detail::write_le<int16_t>(p + 28, errorCode);
        detail::write_le<int16_t>(p + 30, 0);
        detail::write_le<int64_t>(p + 32, offset);
        return TOTAL_SIZE;
    }
    [[nodiscard]] static bool decode(const uint8_t* buf, std::size_t len, WriteTxnMarkerResponseMsg& m) noexcept {
        if (len < TOTAL_SIZE) return false;
        const auto h = ReplicationMessageHeader::decode(buf, 0);
        if (h.templateId != TEMPLATE_ID || h.blockLength < BLOCK_LENGTH) return false;
        const uint8_t* p = buf + ReplicationMessageHeader::ENCODED_SIZE;
        m.token            = detail::read_le<int64_t>(p + 0);
        m.topicId          = detail::read_le<int32_t>(p + 8);
        m.partition        = detail::read_le<int32_t>(p + 12);
        m.producerId       = detail::read_le<int64_t>(p + 16);
        m.coordinatorEpoch = detail::read_le<int32_t>(p + 24);
        m.errorCode        = detail::read_le<int16_t>(p + 28);
        m.offset           = detail::read_le<int64_t>(p + 32);
        return true;
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

// ===========================================================================
// PlaneHello (templateId = 90) — shard-mode replication-plane handshake.
//
// Self-contained worker shards (spec §3.3): worker w of every node exchanges a
// hello on its replication plane w carrying the sender's node id, plane index
// and worker count.  A receiver whose worker count (or plane index) differs
// refuses the peer loudly instead of silently splitting replication planes
// (partition P owned by worker a here, by worker b there).  Hand-rolled, fixed
// 24 bytes, never sent in legacy mode.  Pre-handshake receivers drop the unknown
// templateId (WorkerReplicationTransport::dispatchFrame_ default branch).
//    nodeId      : int32 @ +0
//    workerId    : int32 @ +4   (sender's plane index)
//    workerCount : int32 @ +8   (sender's RUNE_WORKERS)
//    flags       : int32 @ +12  (bit0 = reply requested)
// ===========================================================================
inline constexpr uint16_t kPlaneHelloTemplateId = 90;

struct PlaneHelloMsg {
    int32_t nodeId      = -1;
    int32_t workerId    = -1;
    int32_t workerCount = 0;
    int32_t flags       = 0;
    // F3 round 6, item 6: the cluster-wide max produce-batch constraint — the
    // node's effective max produce batch and worker ingress ring size; a
    // mismatch refuses the plane exactly like a worker-count mismatch.
    int64_t maxProduceBatchBytes   = 0;
    int32_t workerIngressRingBytes = 0;

    static constexpr int32_t     kReplyRequested = 1;
    static constexpr uint16_t    BLOCK_LENGTH    = 32;   // 16 + 8 + 4 + 4 pad
    static constexpr std::size_t TOTAL_SIZE      = ReplicationMessageHeader::ENCODED_SIZE + BLOCK_LENGTH;

    static std::size_t encode(uint8_t* buf, const PlaneHelloMsg& m) noexcept {
        ReplicationMessageHeader::encode(buf, 0, BLOCK_LENGTH, kPlaneHelloTemplateId);
        uint8_t* p = buf + ReplicationMessageHeader::ENCODED_SIZE;
        detail::write_le<int32_t>(p + 0,  m.nodeId);
        detail::write_le<int32_t>(p + 4,  m.workerId);
        detail::write_le<int32_t>(p + 8,  m.workerCount);
        detail::write_le<int32_t>(p + 12, m.flags);
        detail::write_le<int64_t>(p + 16, m.maxProduceBatchBytes);
        detail::write_le<int32_t>(p + 24, m.workerIngressRingBytes);
        detail::write_le<int32_t>(p + 28, 0);   // pad
        return TOTAL_SIZE;
    }

    /// nullopt-free decode: returns false if the frame is too short.
    static bool decode(const uint8_t* buf, std::size_t len, PlaneHelloMsg& out) noexcept {
        if (len < TOTAL_SIZE) return false;
        const uint8_t* p = buf + ReplicationMessageHeader::ENCODED_SIZE;
        out.nodeId      = detail::read_le<int32_t>(p + 0);
        out.workerId    = detail::read_le<int32_t>(p + 4);
        out.workerCount = detail::read_le<int32_t>(p + 8);
        out.flags       = detail::read_le<int32_t>(p + 12);
        out.maxProduceBatchBytes   = detail::read_le<int64_t>(p + 16);
        out.workerIngressRingBytes = detail::read_le<int32_t>(p + 24);
        return true;
    }
};

} // namespace rune::replication
