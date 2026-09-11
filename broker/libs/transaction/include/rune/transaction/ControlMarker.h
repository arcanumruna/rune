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
// ## Batch wire layout (rune-protocol RecordBatch, LITTLE-endian — all-LE)
//
// Control batches are encoded in the SAME little-endian RecordBatch format
// as data batches so that RecordBatchReader can walk them correctly.
// This removes the last BE outlier; Rune is now uniformly LE.
//
//   [0..7]   int64 LE  baseOffset         = 0 (log stamps on append)
//   [8..11]  int32 LE  batchLengthBytes   = total bytes from [12..]
//   [12..15] int32 LE  partitionLeaderEpoch = -1
//   [16]     int8      magic               = 2
//   [17..20] uint32 LE crc                 = CRC32C of [21..]
//   [21..22] int16 LE  attributes          = 0x0030 (CONTROL|TRANSACTIONAL)
//   [23..26] int32 LE  lastOffsetDelta     = 0 (single record)
//   [27..34] int64 LE  firstTimestamp      = 0
//   [35..42] int64 LE  maxTimestamp        = 0
//   [43..50] int64 LE  producerId
//   [51..52] int16 LE  producerEpoch
//   [53..56] int32 LE  baseSequence        = -1
//   [57..60] int32 LE  numRecords          = 1
//   [61..]   record (varint-encoded, endian-neutral)
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
    static constexpr int16_t kAttrControl        = 0x0020; // bit 5: isControl
    static constexpr int16_t kAttrTransactional  = 0x0010; // bit 4: isTransactional
    // S2 fix: kAttrCommit (bit 6) is NOT set during build and NOT checked during
    // extract.  controlType is stored ONLY in the record key (2-byte int16).
    // Using both would create two sources of truth — we pick the record key
    // exclusively (matches the authoritative Kafka KIP-98 definition).

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
    // isControlBatch — true if the batch at buf[offset] has the CONTROL flag
    // AND passes CRC32C verification AND the buffer has sufficient length.
    //
    // S1 fix: verifies magic byte, available length, the CRC32C, and then
    // the CONTROL bit so a corrupt/truncated data batch cannot be mistaken
    // for a control batch.
    //
    // \param buf     Buffer containing the batch bytes.
    // \param offset  Byte offset within buf where the batch starts.
    // \param avail   Number of bytes available starting at buf+offset.
    //                Must be >= ENCODED_SIZE; returns false otherwise.
    //
    // LOW fix (audit re-audit): `avail` is a REQUIRED parameter — no default.
    // A default of ENCODED_SIZE invited an over-read when a caller passed a short
    // buffer but omitted the length; every caller must now pass the real available
    // bytes.
    // -----------------------------------------------------------------------
    static bool isControlBatch(const uint8_t* buf,
                                std::size_t    offset,
                                std::size_t    avail) noexcept;

    // -----------------------------------------------------------------------
    // extract — decode the ControlMarkerPayload from a control batch.
    //
    // S1 fix: performs full bounds-checking on every field and varint read.
    // Returns false if the batch fails isControlBatch() or any field would
    // read past avail bytes.
    //
    // \param buf     Buffer.
    // \param offset  Start of batch in buf.
    // \param out     Decoded payload on success.
    // \param avail   Available bytes from buf+offset.
    //
    // LOW fix (audit re-audit): `avail` is a REQUIRED parameter — no default — so a
    // caller can never accidentally over-read a short buffer by omitting the length.
    // -----------------------------------------------------------------------
    static bool extract(const uint8_t*       buf,
                         std::size_t          offset,
                         ControlMarkerPayload& out,
                         std::size_t          avail) noexcept;

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

    // -----------------------------------------------------------------------
    // resolvePendingMarkers — B1/B2: resolve all internally-initiated aborts.
    //
    // Iterates coordinator.pendingMarkerTransactions(), builds a COMMIT or
    // ABORT control batch for each partition using the PERSISTED epochs from
    // the coordinator metadata (not a live client request), and calls appendFn.
    // After all markers are written, calls completeTransaction() for each txnId.
    //
    // This is the correct resolution path for:
    //   - epoch-fence aborts (initProducerId on Ongoing)
    //   - timeout aborts (doWork)
    //   - recovered Prepare* after coordinator failover (B2)
    //
    // Returns the total number of markers written across all resolved txns.
    // -----------------------------------------------------------------------
    static int resolvePendingMarkers(
        TransactionCoordinator& coordinator,
        const std::function<void(const TopicPartition&,
                                  const std::vector<uint8_t>&)>& appendFn);

private:
    // CRC32C (Castagnoli)
    static uint32_t crc32c(const uint8_t* data, std::size_t len) noexcept;
};

} // namespace rune::transaction
