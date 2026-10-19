// ---------------------------------------------------------------------------
// ControlMarker.cpp — Kafka-faithful transaction control batch implementation.
//
// Namespace: rune::transaction
// C++23. NO locks. NO hot-path alloc. Single-writer.
//
// All-LE: control batches are now encoded in the SAME little-endian
// RecordBatch format as data batches (rune-protocol RecordBatch.h unified).
// Under the old BE+LE mismatch, RecordBatchReader::totalLength() misread the
// LE batchLength as ~1 GiB, making data after a marker unreachable and
// recover() treating markers as torn tails (silent truncation).
// ---------------------------------------------------------------------------

#include <rune/transaction/ControlMarker.h>
#include <rune/transaction/detail/Crc32c.h>
#include <rune/protocol/RecordBatch.h>

#include <cassert>
#include <cstring>

namespace rune::transaction {

namespace {

// ---------------------------------------------------------------------------
// Little-endian helpers — matching rune-protocol RecordBatch (LE) layout.
// ---------------------------------------------------------------------------

static void write_le64(uint8_t* p, int64_t v) noexcept {
    uint64_t u = static_cast<uint64_t>(v);
    for (int i = 0; i < 8; ++i) p[i] = static_cast<uint8_t>(u >> (i * 8));
}

static void write_le32(uint8_t* p, int32_t v) noexcept {
    uint32_t u = static_cast<uint32_t>(v);
    for (int i = 0; i < 4; ++i) p[i] = static_cast<uint8_t>(u >> (i * 8));
}

static void write_le16(uint8_t* p, int16_t v) noexcept {
    uint16_t u = static_cast<uint16_t>(v);
    p[0] = static_cast<uint8_t>(u);
    p[1] = static_cast<uint8_t>(u >> 8);
}

static int64_t read_le64(const uint8_t* p) noexcept {
    uint64_t u = 0;
    for (int i = 0; i < 8; ++i) u |= static_cast<uint64_t>(p[i]) << (i * 8);
    return static_cast<int64_t>(u);
}

static int32_t read_le32(const uint8_t* p) noexcept {
    uint32_t u = uint32_t(p[0]) | (uint32_t(p[1]) << 8)
               | (uint32_t(p[2]) << 16) | (uint32_t(p[3]) << 24);
    return static_cast<int32_t>(u);
}

static int16_t read_le16(const uint8_t* p) noexcept {
    return static_cast<int16_t>(uint16_t(p[0]) | (uint16_t(p[1]) << 8));
}

// ---------------------------------------------------------------------------
// Zig-zag varint encoding helpers (for Kafka record format)
// ---------------------------------------------------------------------------

/// Encode a signed value as zig-zag varint into buf[pos]; advance pos.
/// Returns bytes written.
static std::size_t write_varint_zigzag(uint8_t* buf, std::size_t pos, int64_t v) noexcept {
    uint64_t uv = static_cast<uint64_t>((v << 1) ^ (v >> 63));
    std::size_t written = 0;
    do {
        uint8_t b = static_cast<uint8_t>(uv & 0x7Fu);
        uv >>= 7;
        if (uv != 0) b |= 0x80u;
        buf[pos + written] = b;
        ++written;
    } while (uv != 0);
    return written;
}

/// Read a zig-zag varint from buf[pos..end]; advance pos.
/// S1 fix: bounds-checked — returns false via out-parameter if end is exceeded.
/// \param buf  Buffer start.
/// \param pos  Current position (advanced on success).
/// \param end  One-past-the-last-valid byte index.
/// \param out  Decoded value on success.
/// \returns true on success, false if buffer would be over-read.
static bool read_varint_zigzag_safe(const uint8_t* buf,
                                     std::size_t&   pos,
                                     std::size_t    end,
                                     int64_t&       out) noexcept {
    uint64_t uv = 0;
    int shift = 0;
    // Maximum 10 bytes for a 64-bit varint (7 bits per byte).
    constexpr int kMaxVarintBytes = 10;
    for (int i = 0; i < kMaxVarintBytes; ++i) {
        if (pos >= end) return false;
        uint8_t b = buf[pos++];
        uv |= static_cast<uint64_t>(b & 0x7Fu) << shift;
        if ((b & 0x80u) == 0) {
            out = static_cast<int64_t>((uv >> 1) ^ -(uv & 1u));
            return true;
        }
        shift += 7;
    }
    return false; // varint too long (malformed)
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// CRC32C (Castagnoli) — software table
// ---------------------------------------------------------------------------
/*static*/
uint32_t ControlMarker::crc32c(const uint8_t* data, std::size_t len) noexcept {
    // F15 S1: constexpr table (no std::call_once — shared-nothing law).
    return detail::crc32c(data, len);
}

// ---------------------------------------------------------------------------
// build — encode a control batch in LITTLE-ENDIAN RecordBatch format.
//
// All-LE unification: control batches now use the SAME little-endian byte
// layout as data batches (rune-protocol RecordBatch), so RecordBatchReader
// can walk them without misreading the batchLength field.
//
// Batch wire layout (all LE, 75 bytes total — same offsets as Kafka v2):
//   [0..7]   int64 LE  baseOffset         = 0 (log stamps on append)
//   [8..11]  int32 LE  batchLengthBytes   = 63  (ENCODED_SIZE - 12)
//   [12..15] int32 LE  partitionLeaderEpoch = -1
//   [16]     int8      magic              = 2
//   [17..20] uint32 LE crc (CRC32C of bytes [21..74], same coverage as RecordBatch)
//   [21..22] int16 LE  attributes         = 0x0030 (CONTROL|TRANSACTIONAL)
//   [23..26] int32 LE  lastOffsetDelta    = 0
//   [27..34] int64 LE  firstTimestamp     = 0
//   [35..42] int64 LE  maxTimestamp       = 0
//   [43..50] int64 LE  producerId
//   [51..52] int16 LE  producerEpoch
//   [53..56] int32 LE  baseSequence       = -1
//   [57..60] int32 LE  numRecords         = 1
//   [61..74] record (14 bytes, varint-encoded — endian-neutral)
//
// Record layout (14 bytes, same varint scheme as RecordBatchBuilder::append):
//   [0]     int8   attributes  = 0
//   [1]     varint timestampDelta = 0
//   [2]     varint offsetDelta   = 0
//   [3]     varint keyLength     = 2  (zig-zag(2) = 0x04)
//   [4..5]  int16 LE  key       = controlType (0=Commit, 1=Abort)
//   [6]     varint valueLength  = 6  (zig-zag(6) = 0x0C)
//   [7..8]  int16 LE  version   = 1
//   [9..12] int32 LE  coordinatorEpoch
//   [13]    varint headersCount = 0
//
// Total = 61 + 14 = 75 bytes = ENCODED_SIZE. ✓
//
// attributes (bit field, same as RecordBatchBuilder):
//   bit 4 (0x0010) = isTransactional
//   bit 5 (0x0020) = isControl  ← identifies this as a control batch
//   => 0x0030
// ---------------------------------------------------------------------------
/*static*/
std::vector<uint8_t> ControlMarker::build(ProducerId    producerId,
                                            ProducerEpoch producerEpoch,
                                            ControlType   controlType,
                                            int32_t       coordinatorEpoch) noexcept
{
    std::vector<uint8_t> buf(ENCODED_SIZE, 0u);
    uint8_t* p = buf.data();

    // Batch header — LITTLE-ENDIAN (matches rune-protocol RecordBatch layout).
    write_le64(p +  0, int64_t{0});                           // baseOffset (log stamps)
    write_le32(p +  8, int32_t(ENCODED_SIZE - 12));           // batchLengthBytes = 63
    write_le32(p + 12, int32_t{-1});                          // partitionLeaderEpoch
    p[16] = static_cast<uint8_t>(kMagicByte);                 // magic = 2
    // CRC at [17..20] — filled after body is encoded.
    int16_t attrs = kAttrControl | kAttrTransactional;        // 0x0030
    write_le16(p + 21, attrs);                                 // attributes (LE)
    write_le32(p + 23, int32_t{0});                           // lastOffsetDelta
    write_le64(p + 27, int64_t{0});                           // firstTimestamp
    write_le64(p + 35, int64_t{0});                           // maxTimestamp
    write_le64(p + 43, producerId);                            // producerId
    write_le16(p + 51, static_cast<int16_t>(producerEpoch));  // producerEpoch
    write_le32(p + 53, int32_t{-1});                          // baseSequence
    write_le32(p + 57, int32_t{1});                           // numRecords

    // Record at [61..74] — varint fields are byte-stream (endian-neutral).
    std::size_t rpos = BATCH_HEADER_SIZE; // = 61
    p[rpos++] = 0u;                                            // record attributes

    rpos += write_varint_zigzag(p, rpos, 0);  // timestampDelta = 0
    rpos += write_varint_zigzag(p, rpos, 0);  // offsetDelta = 0
    rpos += write_varint_zigzag(p, rpos, 2);  // keyLength = 2 (zig-zag(2)=4=0x04)

    // key = controlType as int16 LE
    write_le16(p + rpos, static_cast<int16_t>(controlType));
    rpos += 2;

    rpos += write_varint_zigzag(p, rpos, 6);  // valueLength = 6 (zig-zag(6)=12=0x0C)

    // value: version(int16 LE = 1) + coordinatorEpoch(int32 LE)
    write_le16(p + rpos, int16_t{1});          rpos += 2;
    write_le32(p + rpos, coordinatorEpoch);    rpos += 4;

    rpos += write_varint_zigzag(p, rpos, 0);  // headersCount = 0

    assert(rpos == ENCODED_SIZE);
    // D3b r2: the layout the log cleaner parses (ControlMarkerLayout.h) is THIS one.
    namespace cml = rune::storage::control_marker;
    assert(p[cml::kKeyLengthOffset] == cml::kKeyLengthByte && p[cml::kValueLengthOffset] == cml::kValueLengthByte);
    assert(cml::controlType(p, ENCODED_SIZE) == static_cast<int>(controlType));

    // CRC32C over bytes [CRC_DATA_START..ENCODED_SIZE) — same range as
    // RecordBatchBuilder::build() which covers [kAttributesOffset..end].
    uint32_t cksum = crc32c(p + CRC_DATA_START, ENCODED_SIZE - CRC_DATA_START);
    // Store CRC little-endian (matching RecordBatch).
    p[CRC_OFFSET + 0] = static_cast<uint8_t>(cksum);
    p[CRC_OFFSET + 1] = static_cast<uint8_t>(cksum >>  8);
    p[CRC_OFFSET + 2] = static_cast<uint8_t>(cksum >> 16);
    p[CRC_OFFSET + 3] = static_cast<uint8_t>(cksum >> 24);

    return buf;
}

// ---------------------------------------------------------------------------
// isControlBatch — reads the LE RecordBatch format (all-LE unification).
//
// Checks: avail >= ENCODED_SIZE, magic == 2, CRC32C valid (LE-stored),
// CONTROL attribute bit set in LE int16 at [21..22].
// ---------------------------------------------------------------------------
/*static*/
bool ControlMarker::isControlBatch(const uint8_t* buf,
                                    std::size_t    offset,
                                    std::size_t    avail) noexcept {
    // Must have at least ENCODED_SIZE bytes available.
    if (avail < ENCODED_SIZE) return false;
    const uint8_t* p = buf + offset;
    // Check magic byte at [16].
    if (p[16] != static_cast<uint8_t>(kMagicByte)) return false;
    // Verify CRC32C before trusting any other field.
    // CRC stored as LE uint32 at [17..20]; covers bytes [21..ENCODED_SIZE).
    uint32_t stored   = uint32_t(p[CRC_OFFSET])
                      | (uint32_t(p[CRC_OFFSET + 1]) <<  8)
                      | (uint32_t(p[CRC_OFFSET + 2]) << 16)
                      | (uint32_t(p[CRC_OFFSET + 3]) << 24);
    uint32_t computed = crc32c(p + CRC_DATA_START, ENCODED_SIZE - CRC_DATA_START);
    if (stored != computed) return false;
    // Check CONTROL bit (bit 5 = 0x0020) in LE int16 attributes at [21..22].
    int16_t attrs = read_le16(p + 21);
    return (attrs & kAttrControl) != 0;
}

// ---------------------------------------------------------------------------
// extract — decode ControlMarkerPayload from a LE control batch.
//
// Reads batch header fields as LE (producerId at [43..50], producerEpoch at
// [51..52]) and record key/value using bounds-checked varint reads.
// The record key (int16 LE) encodes controlType; the record value (6 bytes)
// encodes version(int16 LE) + coordinatorEpoch(int32 LE).
// ---------------------------------------------------------------------------
/*static*/
bool ControlMarker::extract(const uint8_t*       buf,
                              std::size_t          offset,
                              ControlMarkerPayload& out,
                              std::size_t          avail) noexcept
{
    if (!isControlBatch(buf, offset, avail)) return false;

    const uint8_t* p   = buf + offset;
    // avail >= ENCODED_SIZE (isControlBatch verified).
    const std::size_t end = ENCODED_SIZE; // relative to p

    // Read batch header fields — LITTLE-ENDIAN.
    out.producerId    = read_le64(p + 43);
    out.producerEpoch = read_le16(p + 51);

    // Decode the record starting at [BATCH_HEADER_SIZE = 61].
    std::size_t rpos = BATCH_HEADER_SIZE;
    // record attributes (1 byte)
    if (rpos + 1 > end) return false;
    rpos += 1;

    // timestampDelta (varint)
    int64_t dummy{};
    if (!read_varint_zigzag_safe(p, rpos, end, dummy)) return false;
    // offsetDelta (varint)
    if (!read_varint_zigzag_safe(p, rpos, end, dummy)) return false;
    // keyLength (varint): must be 2
    int64_t keyLen{};
    if (!read_varint_zigzag_safe(p, rpos, end, keyLen)) return false;
    if (keyLen == 2) {
        if (rpos + 2 > end) return false;
        // key = controlType as int16 LE
        int16_t ct = read_le16(p + rpos);
        out.controlType = static_cast<ControlType>(ct);
        rpos += 2;
    } else {
        return false;
    }
    // valueLength (varint): must be 6
    int64_t valLen{};
    if (!read_varint_zigzag_safe(p, rpos, end, valLen)) return false;
    if (valLen != 6) return false;
    if (rpos + 6 > end) return false;
    // value: version(int16 LE) + coordinatorEpoch(int32 LE)
    rpos += 2; // skip version
    out.coordinatorEpoch = read_le32(p + rpos);
    rpos += 4;

    return true;
}

// ---------------------------------------------------------------------------
// writeMarkers
//
// Handles TWO entry scenarios:
//   (a) txn is Ongoing → calls endTransaction first to move to PrepareCommit /
//       PrepareAbort, then writes markers + completeTransaction.
//   (b) txn is already in PrepareCommit / PrepareAbort (e.g. doWork sees a
//       client-driven txn that handleEndTxn already prepared) → skips the
//       endTransaction call and reads partitions directly from coordinator
//       metadata, then writes markers + completeTransaction.
//
// In both cases the outcome is: markers written to all partitions, then
// coordinator advanced to CompleteCommit/CompleteAbort → Empty.
// ---------------------------------------------------------------------------
/*static*/
int ControlMarker::writeMarkers(
    TransactionCoordinator&   coordinator,
    const TransactionalId&    txnId,
    ProducerId                producerId,
    ProducerEpoch             producerEpoch,
    bool                      commit,
    int32_t                   coordinatorEpoch,
    int64_t                   nowNs,
    const std::function<void(const TopicPartition&,
                              const std::vector<uint8_t>&)>& appendFn)
{
    // Determine current state from coordinator metadata.
    const TransactionMetadata* meta = coordinator.metadata(txnId);
    if (!meta) return 0;

    std::vector<TopicPartition> partitions;

    if (meta->state == TxnState::Ongoing) {
        // Scenario (a): transition Ongoing → PrepareCommit/PrepareAbort first.
        auto result = coordinator.endTransaction(txnId, producerId, producerEpoch,
                                                  commit, nowNs);
        if (!result.ok()) return 0;
        partitions = std::move(result.partitions);
    } else if (meta->state == TxnState::PrepareCommit ||
               meta->state == TxnState::PrepareAbort) {
        // Scenario (b): already in Prepare* state (e.g. doWork driving a
        // client-prepared txn or recovering a Prepare* after failover).
        // Get partitions directly from metadata.
        partitions.assign(meta->partitions.begin(), meta->partitions.end());
        // Verify the commit/abort intent matches the current state.
        // (PrepareCommit → must write COMMIT marker; PrepareAbort → ABORT.)
        commit = (meta->state == TxnState::PrepareCommit);
    } else {
        // Wrong state (e.g. Empty, CompleteCommit, Dead) → nothing to do.
        return 0;
    }

    const ControlType ct = commit ? ControlType::Commit : ControlType::Abort;

    // Write a control batch to each involved partition.
    int count = 0;
    for (const auto& tp : partitions) {
        auto batch = ControlMarker::build(producerId, producerEpoch, ct, coordinatorEpoch);
        appendFn(tp, batch);
        ++count;
    }

    // Signal the coordinator that all markers have been written.
    coordinator.completeTransaction(txnId);

    return count;
}

// ---------------------------------------------------------------------------
// resolvePendingMarkers — B1/B2: resolve all internally-initiated aborts.
// ---------------------------------------------------------------------------
/*static*/
int ControlMarker::resolvePendingMarkers(
    TransactionCoordinator& coordinator,
    const std::function<void(const TopicPartition&,
                              const std::vector<uint8_t>&)>& appendFn)
{
    // Snapshot the pending list — we must not iterate while completing txns
    // (completing modifies markersPending and coordinator state).
    auto pending = coordinator.pendingMarkerTransactions();

    int total = 0;
    for (const auto& p : pending) {
        const ControlType ct = p.commit ? ControlType::Commit : ControlType::Abort;
        // Write a control batch for each partition using the PERSISTED epochs.
        for (const auto& tp : p.partitions) {
            auto batch = ControlMarker::build(p.producerId, p.producerEpoch,
                                               ct, p.coordinatorEpoch);
            appendFn(tp, batch);
            ++total;
        }
        // Advance state machine: PrepareAbort/PrepareCommit → Complete* → Empty.
        coordinator.completeTransaction(p.txnId);
    }
    return total;
}

} // namespace rune::transaction
