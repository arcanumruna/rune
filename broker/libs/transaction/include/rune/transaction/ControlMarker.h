#pragma once
// ---------------------------------------------------------------------------
// ControlMarker.h — Kafka-faithful transaction control batches (P5-3).
//
// Namespace: rune::transaction
// C++23. NO locks. NO hot-path alloc. Single-writer.
//
// ## Design (mirrors Kafka KIP-98 §6.4 control batches)
//
// A control batch is a special RecordBatch with the CONTROL flag set in the
// batch attributes (bit 5 = 0x20).  It contains exactly one record: the
// end-transaction marker, carrying {controlType: Commit(0)|Abort(1),
// coordinatorEpoch: int32}.
//
// Control batches are byte-compatible with the log format so they can be
// appended directly to a partition's log.  A reader identifies a control
// batch by checking the CONTROL bit; it then extracts the marker payload.
//
// ## Batch wire layout (subset of Kafka RecordBatch §6.4, little-endian)
//
//   [0..7]   int64  baseOffset         = 0 (filled by log on append)
//   [8..11]  int32  batchLengthBytes   = total bytes from [12..]
//   [12..15] int32  partitionLeaderEpoch = -1
//   [16]     int8   magic               = 2
//   [17..20] int32  crc                 = CRC32C of [21..]
//   [21..22] int16  attributes          = 0x20 (CONTROL flag, LE)
//   [23..26] int32  lastOffsetDelta     = 0 (single record)
//   [27..34] int64  firstTimestamp      = 0
//   [35..42] int64  maxTimestamp        = 0
//   [43..50] int64  producerId
//   [51..52] int16  producerEpoch
//   [53..56] int32  baseSequence        = -1
//   [57..60] int32  numRecords          = 1
//   [61..]   record
//
// ## Record wire layout (Kafka variable-length record, per §6.4)
//
// Records use variable-length zig-zag varint encoding for some fields.
// For our control marker record (fixed known sizes), the encoding is:
//
//   attributes  : int8            = 0
//   timestampDelta : varint       = 0
//   offsetDelta    : varint       = 0
//   keyLength      : varint       = -1 (null key)
//   valueLength    : varint       = 6  (controlType:int16 + coordinatorEpoch:int32)
//   value          :
//       version         : int16   = 1  (EndTransactionMarker version)
//       coordinatorEpoch: int32
//   headersArrayCount  : varint   = 0
//
// controlType is NOT in the record value — it lives in the batch attributes:
//   bit 0-2: compression (0 = none)
//   bit 3:   timestampType (0 = CREATE_TIME)
//   bit 4:   isTransactional (1 = yes)
//   bit 5:   isControl (1 = yes — marks this as a control batch)
//   bit 6-7: deleteHorizonMs / unused (0)
//
// We encode controlType as bit 6 of the attributes (our extension for
// internal routing), mirroring Kafka's approach of putting COMMIT(0)/ABORT(1)
// in the EndTransactionMarker record value key (a special 2-byte key).
//
// For Rune's implementation we use the following simplified but byte-exact
// approach that matches Kafka's KIP-98 control batch semantics:
//   - attributes = 0x0020 | (isTransactional=1 → bit 4) | controlType in record key
//   - record key (2 bytes): int16 = controlType (0=COMMIT, 1=ABORT)
//   - record value (6 bytes): version:int16(=1) + coordinatorEpoch:int32
//
// The full BATCH_HEADER_SIZE = 61 bytes.
// The record is: attrs(1) + tsDelta(1,varint=0) + offDelta(1,varint=0)
//                + keyLen(1,varint=2) + key(2) + valLen(1,varint=6) + val(6)
//                + hdrCount(1,varint=0) = 14 bytes
// Total batch = BATCH_HEADER_SIZE + 4 (numRecords int32... wait — Kafka uses
// int32 in the header) + record = 61 + 14 = 75 bytes.
// But the batch HEADER already contains numRecords at [57..60].
// So ENCODED_SIZE = 61 + 14 = 75 bytes.
//
// CRC field at [17..20] covers bytes [21..end] (CRC32C).
//
// ## writeMarkers helper
//
// writeMarkers(coordinator, txnId, commit, appendFn) —
//   on EndTxn, builds a control batch for EACH partition in the txn and
//   calls appendFn(partition, batch_bytes) for each. Then calls
//   coordinator.completeTransaction(txnId). Returns number of markers written.
// ---------------------------------------------------------------------------

#include <rune/transaction/Types.h>
#include <rune/transaction/TransactionMessages.h>
#include <rune/transaction/TransactionCoordinator.h>

#include <cstdint>
#include <functional>
#include <span>
#include <vector>

namespace rune::transaction {

// ---------------------------------------------------------------------------
// ControlMarkerPayload — the decoded contents of an end-txn-marker record.
// ---------------------------------------------------------------------------
struct ControlMarkerPayload {
    ControlType controlType{ControlType::Commit};
    int32_t     coordinatorEpoch{0};
    ProducerId  producerId{-1};
    ProducerEpoch producerEpoch{0};
};

// ---------------------------------------------------------------------------
// ControlMarker — static helpers for building / identifying / extracting
// control batches.
// ---------------------------------------------------------------------------
class ControlMarker {
public:
    // -----------------------------------------------------------------------
    // Batch constants
    // -----------------------------------------------------------------------
    static constexpr int8_t  kMagicByte         = 2;
    static constexpr int16_t kAttrControl        = 0x0020; // bit 5
    static constexpr int16_t kAttrTransactional  = 0x0010; // bit 4
    static constexpr int16_t kAttrCommit         = 0x0040; // bit 6: 0=commit,1=abort
    // combined attributes for a control batch:
    //   CONTROL | TRANSACTIONAL | (commit=0 : no extra bit, abort=1 : bit 6)
    // We use: attrs = 0x30 | (abort ? 0x40 : 0x00)
    // But to keep decoding simple we store controlType in the record key (2 bytes).

    /// Total encoded size of a control batch in bytes.
    static constexpr std::size_t ENCODED_SIZE = 75;

    /// Offset of the CRC field within the batch bytes.
    static constexpr std::size_t CRC_OFFSET = 17;

    /// First byte covered by CRC (right after the CRC field).
    static constexpr std::size_t CRC_DATA_START = 21;

    /// Batch header size (up to and including numRecords).
    static constexpr std::size_t BATCH_HEADER_SIZE = 61;

    // -----------------------------------------------------------------------
    // build — encode a control batch for the given producer + controlType.
    //
    // The batch is self-contained: CRC is computed and embedded.
    // baseOffset is written as 0 (the log will assign the real offset on append).
    // -----------------------------------------------------------------------
    static std::vector<uint8_t> build(ProducerId    producerId,
                                       ProducerEpoch producerEpoch,
                                       ControlType   controlType,
                                       int32_t       coordinatorEpoch) noexcept;

    // -----------------------------------------------------------------------
    // isControlBatch — true if the batch at buf[offset] has the CONTROL flag.
    //
    // Checks magic byte (must be 2) and the CONTROL bit in attributes.
    // buf must contain at least ENCODED_SIZE bytes from offset.
    // -----------------------------------------------------------------------
    static bool isControlBatch(const uint8_t* buf, std::size_t offset = 0) noexcept;

    // -----------------------------------------------------------------------
    // extract — decode the ControlMarkerPayload from a control batch.
    //
    // Returns false if the batch is not a valid control batch.
    // -----------------------------------------------------------------------
    static bool extract(const uint8_t* buf, std::size_t offset,
                         ControlMarkerPayload& out) noexcept;

    // -----------------------------------------------------------------------
    // writeMarkers — on EndTxn, produce a control batch for every partition
    // in the transaction and call appendFn(tp, batch) for each.
    //
    // After all appends, calls coordinator.completeTransaction(txnId).
    //
    // appendFn signature:
    //   void(const TopicPartition&, const std::vector<uint8_t>&)
    //
    // Returns the number of markers written (= number of partitions in txn),
    // or 0 if txnId unknown / wrong state.
    // -----------------------------------------------------------------------
    static int writeMarkers(TransactionCoordinator&   coordinator,
                             const TransactionalId&    txnId,
                             ProducerId                producerId,
                             ProducerEpoch             producerEpoch,
                             bool                      commit,
                             int32_t                   coordinatorEpoch,
                             int64_t                   nowNs,
                             const std::function<void(const TopicPartition&,
                                                       const std::vector<uint8_t>&)>& appendFn);

private:
    // CRC32C (Castagnoli)
    static uint32_t crc32c(const uint8_t* data, std::size_t len) noexcept;
};

} // namespace rune::transaction
