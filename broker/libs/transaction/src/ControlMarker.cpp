// ---------------------------------------------------------------------------
// ControlMarker.cpp — Kafka-faithful transaction control batch implementation.
//
// Namespace: rune::transaction
// C++23. NO locks. NO hot-path alloc. Single-writer.
// ---------------------------------------------------------------------------

#include <rune/transaction/ControlMarker.h>

#include <cassert>
#include <cstring>
#include <mutex>

namespace rune::transaction {

namespace {

// ---------------------------------------------------------------------------
// Little-endian helpers (UB-free via memcpy)
// ---------------------------------------------------------------------------
template<typename T>
static void write_le(uint8_t* p, T v) noexcept { std::memcpy(p, &v, sizeof(T)); }

template<typename T>
static T read_le(const uint8_t* p) noexcept { T v; std::memcpy(&v, p, sizeof(T)); return v; }

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
static uint32_t     cm_crc32c_tbl[256];
static std::once_flag cm_crc32c_once;

static void cm_build_table() noexcept {
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j)
            crc = (crc >> 1) ^ (0x82F63B78u & -(crc & 1u));
        cm_crc32c_tbl[i] = crc;
    }
}

// A6: thread-safe one-time table init (see TransactionLog.cpp rationale).
static void cm_init_table() noexcept {
    std::call_once(cm_crc32c_once, cm_build_table);
}

/*static*/
uint32_t ControlMarker::crc32c(const uint8_t* data, std::size_t len) noexcept {
    cm_init_table();
    uint32_t crc = 0xFFFFFFFFu;
    for (std::size_t i = 0; i < len; ++i)
        crc = (crc >> 8) ^ cm_crc32c_tbl[(crc ^ data[i]) & 0xFFu];
    return crc ^ 0xFFFFFFFFu;
}

// ---------------------------------------------------------------------------
// build — encode a control batch.
//
// Batch wire layout (all LE, 75 bytes total):
//   [0..7]   int64  baseOffset         = 0
//   [8..11]  int32  batchLengthBytes   = 63  (bytes from [12..74])
//   [12..15] int32  partitionLeaderEpoch = -1
//   [16]     int8   magic              = 2
//   [17..20] int32  crc (CRC32C of bytes [21..74])
//   [21..22] int16  attributes
//   [23..26] int32  lastOffsetDelta    = 0
//   [27..34] int64  firstTimestamp     = 0
//   [35..42] int64  maxTimestamp       = 0
//   [43..50] int64  producerId
//   [51..52] int16  producerEpoch
//   [53..56] int32  baseSequence       = -1
//   [57..60] int32  numRecords         = 1
//   [61..74] record (14 bytes)
//
// Record layout (14 bytes):
//   [0]     int8   attributes  = 0
//   [1]     varint timestampDelta = 0  (encodes to 0x00, 1 byte)
//   [2]     varint offsetDelta   = 0  (encodes to 0x00, 1 byte)
//   [3]     varint keyLength     = 2  (encodes to 0x04, zig-zag(2)=4, 1 byte)
//   [4..5]  int16  key          = controlType (0=commit, 1=abort)
//   [6]     varint valueLength  = 6  (encodes to 0x0C, zig-zag(6)=12, 1 byte)
//   [7..8]  int16  version      = 1
//   [9..12] int32  coordinatorEpoch
//   [13]    varint headersCount = 0  (encodes to 0x00, 1 byte)
//
// Total record = 14 bytes. ENCODED_SIZE = 61 + 14 = 75. ✓
//
// attributes:
//   bit 4 (0x0010) = isTransactional
//   bit 5 (0x0020) = isControl
//   => base = 0x0030
// ---------------------------------------------------------------------------
/*static*/
std::vector<uint8_t> ControlMarker::build(ProducerId    producerId,
                                            ProducerEpoch producerEpoch,
                                            ControlType   controlType,
                                            int32_t       coordinatorEpoch) noexcept
{
    std::vector<uint8_t> buf(ENCODED_SIZE, 0u);
    uint8_t* p = buf.data();

    // Batch header
    write_le<int64_t>(p +  0, int64_t{0});                      // baseOffset
    write_le<int32_t>(p +  8, int32_t{ENCODED_SIZE - 12});       // batchLengthBytes = 75-12=63
    write_le<int32_t>(p + 12, int32_t{-1});                     // partitionLeaderEpoch
    p[16] = static_cast<uint8_t>(kMagicByte);                   // magic = 2
    // CRC at [17..20] — filled after body is encoded
    int16_t attrs = kAttrControl | kAttrTransactional;           // 0x0030
    write_le<int16_t>(p + 21, attrs);                            // attributes
    write_le<int32_t>(p + 23, int32_t{0});                      // lastOffsetDelta
    write_le<int64_t>(p + 27, int64_t{0});                      // firstTimestamp
    write_le<int64_t>(p + 35, int64_t{0});                      // maxTimestamp
    write_le<int64_t>(p + 43, producerId);                       // producerId
    write_le<int16_t>(p + 51, producerEpoch);                    // producerEpoch
    write_le<int32_t>(p + 53, int32_t{-1});                     // baseSequence
    write_le<int32_t>(p + 57, int32_t{1});                      // numRecords

    // Record at [61..]
    std::size_t rpos = BATCH_HEADER_SIZE; // = 61
    p[rpos++] = 0u;                                              // attributes

    // timestampDelta = 0 (zig-zag varint)
    rpos += write_varint_zigzag(p, rpos, 0);
    // offsetDelta = 0 (zig-zag varint)
    rpos += write_varint_zigzag(p, rpos, 0);
    // keyLength = 2 (zig-zag varint; zig-zag(2) = 4 = 0x04)
    rpos += write_varint_zigzag(p, rpos, 2);
    // key = controlType as int16
    write_le<int16_t>(p + rpos, static_cast<int16_t>(controlType)); rpos += 2;
    // valueLength = 6 (zig-zag varint; zig-zag(6) = 12 = 0x0C)
    rpos += write_varint_zigzag(p, rpos, 6);
    // value: version(int16=1) + coordinatorEpoch(int32)
    write_le<int16_t>(p + rpos, int16_t{1});           rpos += 2;
    write_le<int32_t>(p + rpos, coordinatorEpoch);     rpos += 4;
    // headersCount = 0 (zig-zag varint)
    rpos += write_varint_zigzag(p, rpos, 0);

    assert(rpos == ENCODED_SIZE);

    // Compute CRC32C over bytes [CRC_DATA_START..ENCODED_SIZE).
    uint32_t cksum = crc32c(p + CRC_DATA_START, ENCODED_SIZE - CRC_DATA_START);
    write_le<uint32_t>(p + CRC_OFFSET, cksum);

    return buf;
}

// ---------------------------------------------------------------------------
// isControlBatch — S1 fix: bounds check + CRC verify + magic + CONTROL bit.
// ---------------------------------------------------------------------------
/*static*/
bool ControlMarker::isControlBatch(const uint8_t* buf,
                                    std::size_t    offset,
                                    std::size_t    avail) noexcept {
    // S1: must have at least ENCODED_SIZE bytes available.
    if (avail < ENCODED_SIZE) return false;
    const uint8_t* p = buf + offset;
    // Check magic byte at [16].
    if (p[16] != static_cast<uint8_t>(kMagicByte)) return false;
    // S1: verify CRC32C before trusting any other field (including the CONTROL bit).
    // CRC covers bytes [CRC_DATA_START..ENCODED_SIZE).
    uint32_t stored   = read_le<uint32_t>(p + CRC_OFFSET);
    uint32_t computed = crc32c(p + CRC_DATA_START, ENCODED_SIZE - CRC_DATA_START);
    if (stored != computed) return false;
    // Check CONTROL bit (bit 5) in attributes at [21..22].
    int16_t attrs = read_le<int16_t>(p + 21);
    return (attrs & kAttrControl) != 0;
}

// ---------------------------------------------------------------------------
// extract — S1 fix: bounds-checked varint reads + isControlBatch check.
// ---------------------------------------------------------------------------
/*static*/
bool ControlMarker::extract(const uint8_t*       buf,
                              std::size_t          offset,
                              ControlMarkerPayload& out,
                              std::size_t          avail) noexcept
{
    if (!isControlBatch(buf, offset, avail)) return false;

    const uint8_t* p   = buf + offset;
    // avail is already >= ENCODED_SIZE at this point (isControlBatch checked).
    const std::size_t end = ENCODED_SIZE; // relative to p

    out.producerId    = read_le<int64_t>(p + 43);
    out.producerEpoch = read_le<int16_t>(p + 51);

    // Decode the record starting at [61].
    std::size_t rpos = BATCH_HEADER_SIZE; // 61
    // attributes (1 byte)
    if (rpos + 1 > end) return false;
    rpos += 1;

    // timestampDelta (varint) — bounds-checked
    int64_t dummy{};
    if (!read_varint_zigzag_safe(p, rpos, end, dummy)) return false;
    // offsetDelta (varint)
    if (!read_varint_zigzag_safe(p, rpos, end, dummy)) return false;
    // keyLength (varint)
    int64_t keyLen{};
    if (!read_varint_zigzag_safe(p, rpos, end, keyLen)) return false;
    // key = controlType (int16)
    if (keyLen == 2) {
        if (rpos + 2 > end) return false;
        int16_t ct = read_le<int16_t>(p + rpos);
        out.controlType = static_cast<ControlType>(ct);
        rpos += 2;
    } else {
        return false;
    }
    // valueLength (varint)
    int64_t valLen{};
    if (!read_varint_zigzag_safe(p, rpos, end, valLen)) return false;
    if (valLen != 6) return false;
    if (rpos + 6 > end) return false;
    // value: version(int16) + coordinatorEpoch(int32)
    rpos += 2; // skip version
    out.coordinatorEpoch = read_le<int32_t>(p + rpos);
    rpos += 4;

    return true;
}

// ---------------------------------------------------------------------------
// writeMarkers
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
    // Drive the coordinator state machine to PrepareCommit / PrepareAbort.
    auto result = coordinator.endTransaction(txnId, producerId, producerEpoch,
                                              commit, nowNs);
    if (!result.ok()) return 0;

    const ControlType ct = commit ? ControlType::Commit : ControlType::Abort;

    // Write a control batch to each involved partition.
    int count = 0;
    for (const auto& tp : result.partitions) {
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
