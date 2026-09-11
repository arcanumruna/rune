// ---------------------------------------------------------------------------
// TransactionLog.cpp — Durable __transaction_state log implementation.
//
// Namespace: rune::transaction
// C++23. NO locks. Single-writer.
// ---------------------------------------------------------------------------

#include <rune/transaction/TransactionLog.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <stdexcept>
#include <system_error>

namespace rune::transaction {

namespace {

// ---------------------------------------------------------------------------
// Little-endian helpers
// ---------------------------------------------------------------------------
template<typename T>
static void write_le(uint8_t* p, T v) noexcept { std::memcpy(p, &v, sizeof(T)); }

template<typename T>
static T read_le(const uint8_t* p) noexcept { T v; std::memcpy(&v, p, sizeof(T)); return v; }

} // anonymous namespace

// ---------------------------------------------------------------------------
// CRC32C software table (Castagnoli)
// ---------------------------------------------------------------------------
static uint32_t crc32c_tbl[256];
static bool     crc32c_tbl_init = false;

static void init_crc32c_table() {
    if (crc32c_tbl_init) return;
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ (0x82F63B78u & -(crc & 1u));
        }
        crc32c_tbl[i] = crc;
    }
    crc32c_tbl_init = true;
}

/*static*/
uint32_t TransactionLog::crc32c(const uint8_t* data, std::size_t len, uint32_t crc) noexcept {
    init_crc32c_table();
    for (std::size_t i = 0; i < len; ++i) {
        crc = (crc >> 8) ^ crc32c_tbl[(crc ^ data[i]) & 0xFFu];
    }
    return crc ^ 0xFFFFFFFFu;
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------
TransactionLog::TransactionLog(std::string path)
    : path_(std::move(path))
{
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) {
        throw std::system_error(errno, std::system_category(),
                                "TransactionLog: open " + path_);
    }
    replayAndTruncate();
}

TransactionLog::~TransactionLog() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

// ---------------------------------------------------------------------------
// append
// ---------------------------------------------------------------------------
void TransactionLog::append(const TxnLogEntry& entry) {
    appendRecord(entry);
    applyEntry(entry);
}

// ---------------------------------------------------------------------------
// appendFromMetadata
// ---------------------------------------------------------------------------
void TransactionLog::appendFromMetadata(const TransactionMetadata& meta,
                                         int32_t coordinatorEpoch)
{
    TxnLogEntry e;
    e.txnId            = meta.txnId;
    e.producerId       = meta.producerId;
    e.producerEpoch    = meta.producerEpoch;
    e.state            = meta.state;
    e.partitions       = meta.partitions;
    e.txnStartNs       = meta.txnStartNs;
    e.coordinatorEpoch = coordinatorEpoch;
    e.txnTimeoutMs     = meta.txnTimeoutMs;
    append(e);
}

// ---------------------------------------------------------------------------
// metadata
// ---------------------------------------------------------------------------
const TransactionMetadata* TransactionLog::metadata(const TransactionalId& txnId) const noexcept {
    auto it = txnMap_.find(txnId);
    if (it == txnMap_.end()) return nullptr;
    return &it->second;
}

// ---------------------------------------------------------------------------
// applyEntry — update in-memory map
// ---------------------------------------------------------------------------
void TransactionLog::applyEntry(const TxnLogEntry& entry) {
    auto& meta = txnMap_[entry.txnId];
    meta.txnId             = entry.txnId;
    meta.producerId        = entry.producerId;
    meta.producerEpoch     = entry.producerEpoch;
    meta.state             = entry.state;
    meta.partitions        = entry.partitions;
    meta.txnStartNs        = entry.txnStartNs;
    meta.txnTimeoutMs      = entry.txnTimeoutMs;
    meta.coordinatorEpoch  = entry.coordinatorEpoch;   // FIX 2: persist+recover coordinatorEpoch
    // fencingEpoch is not persisted; reset to false on recover
    meta.fencingEpoch      = false;
}

// ---------------------------------------------------------------------------
// appendRecord — encode + write + fsync
//
// Wire layout:
//   [0..3]    uint32  magic
//   [4..7]    uint32  totalLength
//   [8..15]   int64   txnStartNs
//   [16..23]  int64   producerId
//   [24..25]  int16   producerEpoch
//   [26]      uint8   state
//   [27]      uint8   _pad = 0
//   [28..31]  int32   coordinatorEpoch
//   [32..35]  int32   txnTimeoutMs
//   [36..37]  uint16  txnIdLen
//   [38..39]  uint16  partitionCount
//   [40..41]  uint16  _pad = 0
//   [42..43]  uint16  _pad2 = 0     → kFixedHeaderSize = 44
//   [44..]    bytes   txnId
//   [..]      tuples  { topicLen:uint16, topic bytes, partition:int32 } × partitionCount
//   [..]      uint32  CRC32C
// ---------------------------------------------------------------------------
void TransactionLog::appendRecord(const TxnLogEntry& entry) {
    // Compute variable-length sizes.
    const auto txnIdLen      = static_cast<uint16_t>(entry.txnId.size());
    const auto partCount     = static_cast<uint16_t>(entry.partitions.size());

    // Compute total partition bytes.
    std::size_t partBytes = 0;
    for (const auto& tp : entry.partitions) {
        partBytes += 2 + tp.topic.size() + 4; // topicLen:uint16 + topic + partition:int32
    }

    const uint32_t totalLength = kFixedHeaderSize
                                 + txnIdLen
                                 + static_cast<uint32_t>(partBytes)
                                 + kChecksumSize;

    std::vector<uint8_t> buf(totalLength);
    uint8_t* p = buf.data();

    write_le<uint32_t>(p +  0, kMagic);
    write_le<uint32_t>(p +  4, totalLength);
    write_le<int64_t> (p +  8, entry.txnStartNs);
    write_le<int64_t> (p + 16, entry.producerId);
    write_le<int16_t> (p + 24, entry.producerEpoch);
    p[26] = static_cast<uint8_t>(entry.state);
    p[27] = 0u; // padding
    write_le<int32_t> (p + 28, entry.coordinatorEpoch);
    write_le<int32_t> (p + 32, entry.txnTimeoutMs);
    write_le<uint16_t>(p + 36, txnIdLen);
    write_le<uint16_t>(p + 38, partCount);
    write_le<uint16_t>(p + 40, 0u); // pad
    write_le<uint16_t>(p + 42, 0u); // pad2

    std::size_t pos = kFixedHeaderSize;
    std::memcpy(p + pos, entry.txnId.data(), txnIdLen);
    pos += txnIdLen;

    for (const auto& tp : entry.partitions) {
        auto topicLen = static_cast<uint16_t>(tp.topic.size());
        write_le<uint16_t>(p + pos, topicLen); pos += 2;
        std::memcpy(p + pos, tp.topic.data(), topicLen); pos += topicLen;
        write_le<int32_t>(p + pos, tp.partition); pos += 4;
    }

    assert(pos + kChecksumSize == totalLength);

    // CRC covers all bytes before the checksum field.
    uint32_t cksum = crc32c(p, pos);
    write_le<uint32_t>(p + pos, cksum);
    pos += kChecksumSize;
    assert(pos == totalLength);

    ssize_t written = ::pwrite(fd_, buf.data(), totalLength,
                                static_cast<off_t>(writePos_));
    if (written != static_cast<ssize_t>(totalLength)) {
        throw std::system_error(errno, std::system_category(),
                                "TransactionLog: pwrite " + path_);
    }
    if (::fsync(fd_) != 0) {
        throw std::system_error(errno, std::system_category(),
                                "TransactionLog: fsync " + path_);
    }
    writePos_ += static_cast<int64_t>(totalLength);
}

// ---------------------------------------------------------------------------
// replayAndTruncate
// ---------------------------------------------------------------------------
void TransactionLog::replayAndTruncate() {
    struct stat st{};
    if (::fstat(fd_, &st) != 0) {
        throw std::system_error(errno, std::system_category(),
                                "TransactionLog: fstat " + path_);
    }
    const int64_t fileSize = static_cast<int64_t>(st.st_size);

    int64_t pos        = 0;
    int64_t lastGoodPos = 0;

    uint8_t fixedHdr[kFixedHeaderSize];

    while (pos < fileSize) {
        const int64_t remaining = fileSize - pos;
        if (remaining < static_cast<int64_t>(kFixedHeaderSize)) break;

        ssize_t n = ::pread(fd_, fixedHdr, kFixedHeaderSize, static_cast<off_t>(pos));
        if (n != static_cast<ssize_t>(kFixedHeaderSize)) break;

        uint32_t magic = read_le<uint32_t>(fixedHdr + 0);
        if (magic != kMagic) break;

        uint32_t totalLength = read_le<uint32_t>(fixedHdr + 4);
        if (totalLength < kFixedHeaderSize + kChecksumSize) break;
        if (remaining < static_cast<int64_t>(totalLength)) break;

        std::vector<uint8_t> recBuf(totalLength);
        n = ::pread(fd_, recBuf.data(), totalLength, static_cast<off_t>(pos));
        if (n != static_cast<ssize_t>(totalLength)) break;

        // Verify CRC32C.
        uint32_t storedCksum = read_le<uint32_t>(recBuf.data() + totalLength - kChecksumSize);
        uint32_t computed    = crc32c(recBuf.data(), totalLength - kChecksumSize);
        if (storedCksum != computed) break;

        // Decode fixed header.
        const uint8_t* rb = recBuf.data();
        int64_t txnStartNs       = read_le<int64_t> (rb +  8);
        int64_t producerId        = read_le<int64_t> (rb + 16);
        int16_t producerEpoch     = read_le<int16_t> (rb + 24);
        uint8_t stateRaw          = rb[26];
        int32_t coordinatorEpoch  = read_le<int32_t> (rb + 28);
        int32_t txnTimeoutMs      = read_le<int32_t> (rb + 32);
        uint16_t txnIdLen         = read_le<uint16_t>(rb + 36);
        uint16_t partCount        = read_le<uint16_t>(rb + 38);

        // Decode variable body.
        std::size_t vpos = kFixedHeaderSize;

        // Bounds check for txnId.
        if (vpos + txnIdLen > totalLength - kChecksumSize) break;
        std::string txnId{reinterpret_cast<const char*>(rb + vpos), txnIdLen};
        vpos += txnIdLen;

        std::set<TopicPartition> partitions;
        bool bad = false;
        for (uint16_t i = 0; i < partCount; ++i) {
            if (vpos + 2 > totalLength - kChecksumSize) { bad = true; break; }
            uint16_t topicLen = read_le<uint16_t>(rb + vpos); vpos += 2;
            if (vpos + topicLen + 4 > totalLength - kChecksumSize) { bad = true; break; }
            std::string topic{reinterpret_cast<const char*>(rb + vpos), topicLen};
            vpos += topicLen;
            int32_t partition = read_le<int32_t>(rb + vpos); vpos += 4;
            partitions.insert(TopicPartition{std::move(topic), partition});
        }
        if (bad) break;

        // Apply to in-memory map.
        TxnLogEntry entry;
        entry.txnId            = std::move(txnId);
        entry.producerId       = producerId;
        entry.producerEpoch    = static_cast<ProducerEpoch>(producerEpoch);
        entry.state            = static_cast<TxnState>(stateRaw);
        entry.partitions       = std::move(partitions);
        entry.txnStartNs       = txnStartNs;
        entry.coordinatorEpoch = coordinatorEpoch;
        entry.txnTimeoutMs     = txnTimeoutMs;
        applyEntry(entry);

        pos         += static_cast<int64_t>(totalLength);
        lastGoodPos  = pos;
    }

    // Truncate torn tail.
    if (lastGoodPos < fileSize) {
        if (::ftruncate(fd_, static_cast<off_t>(lastGoodPos)) != 0) {
            throw std::system_error(errno, std::system_category(),
                                    "TransactionLog: ftruncate " + path_);
        }
    }
    writePos_ = lastGoodPos;
}

} // namespace rune::transaction
