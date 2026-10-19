// ---------------------------------------------------------------------------
// OffsetStore.cpp — Durable consumer committed-offset store implementation.
//
// Log format (per record, all little-endian):
//   [0..3]   uint32  magic        = kMagic ("OFFS" = 0x4F464653)
//   [4..7]   uint32  totalLength  = kFixedHeaderSize + groupLen + topicLen + metadataLen + 4
//                                   (the +4 is the trailing uint32 checksum)
//   [8..15]  int64   commitTs     (nanoseconds)
//   [16..23] int64   offset
//   [24..27] int32   partition
//   [28..29] uint16  groupLen
//   [30..31] uint16  topicLen
//   [32..33] uint16  metadataLen
//   [34..35] uint16  _pad = 0     (align body to 36 = kFixedHeaderSize)
//   [36..]   bytes   group (groupLen bytes)
//   [..]     bytes   topic (topicLen bytes)
//   [..]     bytes   metadata (metadataLen bytes)
//   [..]     uint32  checksum  (CRC32C of bytes [0 .. totalLength-5])
//
// kFixedHeaderSize = 36
// totalLength = 36 + groupLen + topicLen + metadataLen + 4
//
// Crash-recovery: on open we scan the file record by record. When we find
// a truncated header, a magic mismatch, or a checksum mismatch we stop and
// ftruncate the file to the last good position.
// ---------------------------------------------------------------------------

#include <rune/coordination/OffsetStore.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <bit>
#include <cassert>
#include <cstring>
#include <stdexcept>
#include <system_error>
#include <vector>

namespace rune::coordination {

namespace {

// ---------------------------------------------------------------------------
// CRC32C — software implementation (no intrinsics needed for correctness;
// fast enough for the sizes we handle here).
// ---------------------------------------------------------------------------
static uint32_t crc32c_table[256];
static bool     crc32c_table_init = false;

static void init_crc32c_table() {
    if (crc32c_table_init) return;
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ (0x82F63B78u & -(crc & 1u));
        }
        crc32c_table[i] = crc;
    }
    crc32c_table_init = true;
}

static uint32_t crc32c(const uint8_t* data, std::size_t len, uint32_t crc = 0xFFFFFFFFu) {
    init_crc32c_table();
    for (std::size_t i = 0; i < len; ++i) {
        crc = (crc >> 8) ^ crc32c_table[(crc ^ data[i]) & 0xFFu];
    }
    return crc ^ 0xFFFFFFFFu;
}

// ---------------------------------------------------------------------------
// Little-endian helpers
// ---------------------------------------------------------------------------
template<typename T>
static void write_le(uint8_t* p, T v) noexcept { std::memcpy(p, &v, sizeof(T)); }

template<typename T>
static T read_le(const uint8_t* p) noexcept { T v; std::memcpy(&v, p, sizeof(T)); return v; }

} // anonymous namespace

// ---------------------------------------------------------------------------
// Record layout constants
// ---------------------------------------------------------------------------
// kMagic and kFixedHeaderSize are class members (OffsetStore::kMagic etc.)
// kChecksumSize is local to this translation unit.
static constexpr uint32_t kChecksumSize = 4u;

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

OffsetStore::OffsetStore(std::string path)
    : path_(std::move(path))
{
    // Open or create the log file.
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) {
        throw std::system_error(errno, std::system_category(),
                                "OffsetStore: open " + path_);
    }

    // Replay existing records; sets writePos_ and truncates any torn tail.
    replayAndTruncate();
}

OffsetStore::~OffsetStore() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

// ---------------------------------------------------------------------------
// commit
// ---------------------------------------------------------------------------

void OffsetStore::commit(const GroupId&        group,
                          const TopicPartition& tp,
                          Offset                offset,
                          std::string_view      metadata,
                          int64_t               commitTimestampNs)
{
    appendRecord(group, tp, offset, metadata, commitTimestampNs);
    applyRecord(group, tp, offset, std::string{metadata}, commitTimestampNs);
}

// ---------------------------------------------------------------------------
// fetch
// ---------------------------------------------------------------------------

std::optional<CommittedOffset> OffsetStore::fetch(const GroupId&        group,
                                                    const TopicPartition& tp) const
{
    auto git = index_.find(group);
    if (git == index_.end()) return std::nullopt;
    auto tit = git->second.find(tp);
    if (tit == git->second.end()) return std::nullopt;
    return tit->second;
}

// ---------------------------------------------------------------------------
// fetchAll
// ---------------------------------------------------------------------------

std::vector<std::pair<TopicPartition, CommittedOffset>>
OffsetStore::fetchAll(const GroupId& group) const
{
    std::vector<std::pair<TopicPartition, CommittedOffset>> result;
    auto git = index_.find(group);
    if (git == index_.end()) return result;
    result.reserve(git->second.size());
    for (const auto& [tp, co] : git->second) {
        result.emplace_back(tp, co);
    }
    return result;
}

// ---------------------------------------------------------------------------
// applyRecord — update in-memory index
// ---------------------------------------------------------------------------

void OffsetStore::applyRecord(std::string    group,
                               TopicPartition tp,
                               Offset         offset,
                               std::string    metadata,
                               int64_t        commitTimestampNs)
{
    auto& tpMap = index_[group];
    tpMap[tp] = CommittedOffset{offset, std::move(metadata), commitTimestampNs};
}

// ---------------------------------------------------------------------------
// appendRecord — encode + write + fsync
// ---------------------------------------------------------------------------

void OffsetStore::appendRecord(const GroupId&        group,
                                const TopicPartition& tp,
                                Offset                offset,
                                std::string_view      metadata,
                                int64_t               commitTimestampNs)
{
    const auto groupLen    = static_cast<uint16_t>(group.size());
    const auto topicLen    = static_cast<uint16_t>(tp.topic.size());
    const auto metadataLen = static_cast<uint16_t>(metadata.size());

    // Total on-disk size for this record.
    const uint32_t totalLength = kFixedHeaderSize
                                 + groupLen + topicLen + metadataLen
                                 + kChecksumSize;

    // Stack-allocate for small records; heap for large (metadata can be big).
    std::vector<uint8_t> buf(totalLength);
    uint8_t* p = buf.data();

    // Fixed header
    write_le<uint32_t>(p +  0, kMagic);
    write_le<uint32_t>(p +  4, totalLength);
    write_le<int64_t> (p +  8, commitTimestampNs);
    write_le<int64_t> (p + 16, offset);
    write_le<int32_t> (p + 24, tp.partition);
    write_le<uint16_t>(p + 28, groupLen);
    write_le<uint16_t>(p + 30, topicLen);
    write_le<uint16_t>(p + 32, metadataLen);
    write_le<uint16_t>(p + 34, 0u); // padding

    // Variable body
    std::size_t pos = kFixedHeaderSize;
    std::memcpy(p + pos, group.data(), groupLen);   pos += groupLen;
    std::memcpy(p + pos, tp.topic.data(), topicLen); pos += topicLen;
    std::memcpy(p + pos, metadata.data(), metadataLen); pos += metadataLen;

    // Checksum covers everything before the checksum field.
    uint32_t cksum = crc32c(p, pos);
    write_le<uint32_t>(p + pos, cksum);
    pos += kChecksumSize;

    assert(pos == totalLength);

    // Write to file.
    ssize_t written = ::pwrite(fd_, buf.data(), totalLength,
                                static_cast<off_t>(writePos_));
    if (written != static_cast<ssize_t>(totalLength)) {
        throw std::system_error(errno, std::system_category(),
                                "OffsetStore: pwrite " + path_);
    }

    // Flush to disk for crash durability.
    if (::fsync(fd_) != 0) {
        throw std::system_error(errno, std::system_category(),
                                "OffsetStore: fsync " + path_);
    }

    writePos_ += static_cast<int64_t>(totalLength);
}

// ---------------------------------------------------------------------------
// replayAndTruncate — read the log, apply all complete records, truncate torn tail
// ---------------------------------------------------------------------------

void OffsetStore::replayAndTruncate() {
    // Determine file size.
    struct stat st{};
    if (::fstat(fd_, &st) != 0) {
        throw std::system_error(errno, std::system_category(),
                                "OffsetStore: fstat " + path_);
    }
    const int64_t fileSize = static_cast<int64_t>(st.st_size);

    int64_t pos = 0;           // read cursor
    int64_t lastGoodPos = 0;   // end of last complete record

    // Scratch buffer for reading fixed headers.
    uint8_t fixedHdr[kFixedHeaderSize];

    while (pos < fileSize) {
        const int64_t remaining = fileSize - pos;

        // Need at least the fixed header.
        if (remaining < static_cast<int64_t>(kFixedHeaderSize)) {
            // Torn header — truncate here.
            break;
        }

        ssize_t n = ::pread(fd_, fixedHdr, kFixedHeaderSize,
                             static_cast<off_t>(pos));
        if (n != static_cast<ssize_t>(kFixedHeaderSize)) {
            break; // I/O error → treat as torn
        }

        // Check magic.
        uint32_t magic = read_le<uint32_t>(fixedHdr + 0);
        if (magic != kMagic) {
            break; // corruption/torn
        }

        uint32_t totalLength = read_le<uint32_t>(fixedHdr + 4);

        // Sanity: totalLength must be at least kFixedHeaderSize + kChecksumSize.
        if (totalLength < kFixedHeaderSize + kChecksumSize) {
            break;
        }

        // Check we have the full record.
        if (remaining < static_cast<int64_t>(totalLength)) {
            break; // torn tail
        }

        // Read the full record.
        std::vector<uint8_t> recBuf(totalLength);
        n = ::pread(fd_, recBuf.data(), totalLength, static_cast<off_t>(pos));
        if (n != static_cast<ssize_t>(totalLength)) {
            break;
        }

        // Verify checksum.
        uint32_t storedCksum = read_le<uint32_t>(recBuf.data() + totalLength - kChecksumSize);
        uint32_t computed    = crc32c(recBuf.data(), totalLength - kChecksumSize);
        if (storedCksum != computed) {
            break; // checksum mismatch → torn or corrupted
        }

        // Decode fields.
        int64_t  commitTs   = read_le<int64_t> (recBuf.data() +  8);
        int64_t  off        = read_le<int64_t> (recBuf.data() + 16);
        int32_t  partition  = read_le<int32_t> (recBuf.data() + 24);
        uint16_t groupLen   = read_le<uint16_t>(recBuf.data() + 28);
        uint16_t topicLen   = read_le<uint16_t>(recBuf.data() + 30);
        uint16_t metaLen    = read_le<uint16_t>(recBuf.data() + 32);
        // [34..35] is padding

        // Defense-in-depth: verify body-length identity before slicing.
        // The CRC catches random bit-flips in well-formed records, but an
        // adversarially-crafted record can have a valid CRC for the tampered
        // header bytes while sub-lengths sum to more than the body (audit A3).
        // totalLength = kFixedHeaderSize + groupLen + topicLen + metaLen + kChecksumSize
        // → body size = totalLength - kFixedHeaderSize - kChecksumSize
        // → must equal groupLen + topicLen + metaLen.
        uint32_t bodySize = totalLength - kFixedHeaderSize - kChecksumSize;
        uint32_t subLenSum = static_cast<uint32_t>(groupLen)
                           + static_cast<uint32_t>(topicLen)
                           + static_cast<uint32_t>(metaLen);
        if (subLenSum != bodySize) {
            break; // body-length inconsistency — reject as corrupt
        }

        const uint8_t* body = recBuf.data() + kFixedHeaderSize;
        std::string group_str{reinterpret_cast<const char*>(body),                   groupLen};
        std::string topic_str{reinterpret_cast<const char*>(body + groupLen),        topicLen};
        std::string meta_str {reinterpret_cast<const char*>(body + groupLen + topicLen), metaLen};

        // Apply to in-memory index.
        TopicPartition tp{topic_str, partition};
        applyRecord(std::move(group_str), std::move(tp),
                    off, std::move(meta_str), commitTs);

        pos        += static_cast<int64_t>(totalLength);
        lastGoodPos = pos;
    }

    // If there was a torn tail, truncate the file.
    if (lastGoodPos < fileSize) {
        if (::ftruncate(fd_, static_cast<off_t>(lastGoodPos)) != 0) {
            throw std::system_error(errno, std::system_category(),
                                    "OffsetStore: ftruncate (torn tail) " + path_);
        }
    }

    writePos_ = lastGoodPos;
}

} // namespace rune::coordination
