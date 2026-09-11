// ---------------------------------------------------------------------------
// ProducerStateManager.cpp — Kafka-style idempotent producer dedup.
//
// On-disk snapshot format (all little-endian):
//
// File header:
//   [0..3]   uint32  magic        = kFileMagic (0x50534D47 "PSMG")
//   [4..7]   uint32  version      = 1
//   [8..11]  uint32  entryCount
//   [12..15] uint32  _pad = 0
//
// Per-entry (fixed size = kEntrySize):
//   [0..7]   int64   producerId
//   [8..11]  int32   partition
//   [12..13] int16   producerEpoch
//   [14..15] int16   _pad = 0
//   [16..19] int32   lastSequence
//   [20..23] int32   windowHead
//   [24..27] int32   windowCount
//   [28..31] int32   _pad = 0
//   Then kRetryWindowSize * kBatchDescSize bytes of window slots:
//     slot: int32 baseSequence, int32 lastSequence, int64 baseOffset (16 bytes)
//   Trailing uint32 CRC32C of everything in the entry before the checksum.
//
// kEntrySize = 32 + kRetryWindowSize * 16 + 4
//            = 32 + 5 * 16 + 4 = 116 bytes
//
// The file is written atomically: entries, then file header, then fsync.
// Replay: read header, then read each entry verbatim.
// A torn entry (file too short, CRC mismatch) stops replay; we keep what we
// decoded so far — the last snapshot is always consistent.
// ---------------------------------------------------------------------------

#include <rune/storage/ProducerStateManager.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstring>
#include <stdexcept>
#include <system_error>
#include <vector>

namespace rune::storage {

namespace {

// ---------------------------------------------------------------------------
// CRC32C (software, same polynomial as OffsetStore)
// ---------------------------------------------------------------------------
static uint32_t g_crc_table[256];
static bool     g_crc_init = false;

static void init_crc() noexcept {
    if (g_crc_init) return;
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int j = 0; j < 8; ++j)
            c = (c >> 1) ^ (0x82F63B78u & -(c & 1u));
        g_crc_table[i] = c;
    }
    g_crc_init = true;
}

static uint32_t crc32c(const uint8_t* d, std::size_t n, uint32_t crc = 0xFFFFFFFFu) noexcept {
    init_crc();
    for (std::size_t i = 0; i < n; ++i)
        crc = (crc >> 8) ^ g_crc_table[(crc ^ d[i]) & 0xFFu];
    return crc ^ 0xFFFFFFFFu;
}

// ---------------------------------------------------------------------------
// Little-endian helpers
// ---------------------------------------------------------------------------
template<typename T>
static void wle(uint8_t* p, T v) noexcept { std::memcpy(p, &v, sizeof(T)); }

template<typename T>
static T rle(const uint8_t* p) noexcept { T v{}; std::memcpy(&v, p, sizeof(T)); return v; }

// ---------------------------------------------------------------------------
// Layout constants
// ---------------------------------------------------------------------------
static constexpr uint32_t kFileMagic   = 0x50534D47u; // "PSMG"
static constexpr uint32_t kFileVersion = 1u;
static constexpr uint32_t kFileHdrSize = 16u;         // magic+version+count+pad

static constexpr uint32_t kBatchDescSize = 16u;       // int32+int32+int64
// Per-entry fixed header: producerId(8) + partition(4) + epoch(2) + pad(2)
//   + lastSequence(4) + windowHead(4) + windowCount(4) + pad(4) = 32
static constexpr uint32_t kEntryFixedSize = 32u;
static constexpr uint32_t kWindowBytes    = kRetryWindowSize * kBatchDescSize; // 80
static constexpr uint32_t kChecksumSize   = 4u;
static constexpr uint32_t kEntrySize      = kEntryFixedSize + kWindowBytes + kChecksumSize; // 116

// -------------------------------------------------------------------------
// Encode one BatchDescriptor into p (16 bytes, LE)
// -------------------------------------------------------------------------
static void encodeBatch(uint8_t* p, const BatchDescriptor& bd) noexcept {
    wle<int32_t>(p,     bd.baseSequence);
    wle<int32_t>(p + 4, bd.lastSequence);
    wle<int64_t>(p + 8, bd.baseOffset);
}

static BatchDescriptor decodeBatch(const uint8_t* p) noexcept {
    BatchDescriptor bd;
    bd.baseSequence = rle<int32_t>(p);
    bd.lastSequence = rle<int32_t>(p + 4);
    bd.baseOffset   = rle<int64_t>(p + 8);
    return bd;
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// validate — pure, no allocation, no locks
// ---------------------------------------------------------------------------
ValidationResult
ProducerStateManager::validate(int64_t producerId,
                               int32_t partition,
                               int16_t producerEpoch,
                               int32_t baseSequence,
                               int32_t lastSequenceInBatch) const noexcept
{
    // Protocol invariant: sequence numbers are non-negative.
    assert(baseSequence >= 0 && "baseSequence must be >= 0 per protocol");
    assert(lastSequenceInBatch >= baseSequence && "lastSequence must be >= baseSequence");

    const ProducerPartitionKey key{producerId, partition};
    auto it = map_.find(key);

    if (it == map_.end()) {
        // Brand-new producer on this partition — trust-on-first-use.
        // Epoch is accepted as-is for the first appearance of a (producerId, partition)
        // pair on this node. Cross-node epoch fencing requires a coordinator.
        if (baseSequence != 0) return ValidationResult::oos();
        return ValidationResult::accept();
    }

    const ProducerStateEntry& e = it->second;

    // --- epoch checks ---
    if (producerEpoch < e.producerEpoch) {
        return ValidationResult::fenced();  // zombie: lower epoch
    }

    if (producerEpoch > e.producerEpoch) {
        // New session: epoch bumped. The new session MUST start at baseSequence 0.
        if (baseSequence != 0) return ValidationResult::oos();
        return ValidationResult::accept();
    }

    // Same epoch — check sequence.
    // First, scan the retry window for an exact duplicate.
    for (int i = 0; i < e.windowCount; ++i) {
        // Window is stored oldest-first from windowHead when full, or from [0]
        // when not full. The easiest approach: iterate all filled slots.
        // Slots 0..windowCount-1 if not yet wrapped; circular otherwise.
        int slot = (e.windowHead - e.windowCount + kRetryWindowSize + i) % kRetryWindowSize;
        const auto& bd = e.window[slot];
        if (bd.baseSequence == baseSequence && bd.lastSequence == lastSequenceInBatch) {
            return ValidationResult::dup(bd.baseOffset);
        }
    }

    // Compute the expected next baseSequence.
    // lastSequence == -1 means no batch appended yet in this epoch.
    int32_t expected = (e.lastSequence < 0) ? 0 : e.lastSequence + 1;

    if (baseSequence == expected) {
        return ValidationResult::accept();
    }

    // baseSequence < expected and not in retry window → stale / gap
    // baseSequence > expected → forward gap
    return ValidationResult::oos();
}

// ---------------------------------------------------------------------------
// update — called after a successful append
// ---------------------------------------------------------------------------
void ProducerStateManager::update(int64_t producerId,
                                  int32_t partition,
                                  int16_t producerEpoch,
                                  int32_t baseSequence,
                                  int32_t lastSequence,
                                  int64_t baseOffset)
{
    ProducerStateEntry& e = getOrCreate(producerId, partition);

    if (producerEpoch > e.producerEpoch) {
        // New epoch: clear retry window + reset sequence state.
        e.producerEpoch = producerEpoch;
        e.lastSequence  = -1;
        e.windowHead    = 0;
        e.windowCount   = 0;
        e.window        = {};
    }

    // Record the batch in the retry window (circular, evicts oldest when full).
    BatchDescriptor bd{baseSequence, lastSequence, baseOffset};
    e.window[e.windowHead] = bd;
    e.windowHead = (e.windowHead + 1) % kRetryWindowSize;
    if (e.windowCount < kRetryWindowSize) ++e.windowCount;

    e.lastSequence = lastSequence;
}

// ---------------------------------------------------------------------------
// getOrCreate
// ---------------------------------------------------------------------------
ProducerStateEntry&
ProducerStateManager::getOrCreate(int64_t producerId, int32_t partition)
{
    return map_[ProducerPartitionKey{producerId, partition}];
}

// ---------------------------------------------------------------------------
// entry (read-only accessor)
// ---------------------------------------------------------------------------
const ProducerStateEntry*
ProducerStateManager::entry(int64_t producerId, int32_t partition) const noexcept
{
    auto it = map_.find(ProducerPartitionKey{producerId, partition});
    if (it == map_.end()) return nullptr;
    return &it->second;
}

// ---------------------------------------------------------------------------
// takeSnapshot — encode all entries + fsync
// ---------------------------------------------------------------------------
void ProducerStateManager::takeSnapshot(const std::string& path) const
{
    // Build the full file in memory first.
    // S4: use 64-bit arithmetic to avoid uint32 overflow for large producer counts.
    const uint64_t count     = static_cast<uint64_t>(map_.size());
    const uint64_t totalSize = static_cast<uint64_t>(kFileHdrSize) +
                               count * static_cast<uint64_t>(kEntrySize);
    // Guard against pathological producer counts (> 1 billion would be a bug).
    if (count > 1'000'000'000ULL)
        throw std::runtime_error("ProducerStateManager: snapshot count unreasonably large");

    std::vector<uint8_t> buf(static_cast<std::size_t>(totalSize), 0u);
    uint8_t* p = buf.data();

    // File header.
    wle<uint32_t>(p,      kFileMagic);
    wle<uint32_t>(p + 4,  kFileVersion);
    wle<uint32_t>(p + 8,  static_cast<uint32_t>(count));
    wle<uint32_t>(p + 12, 0u); // pad
    p += kFileHdrSize;

    // Entries.
    for (const auto& [key, e] : map_) {
        uint8_t* ep = p;

        wle<int64_t>(ep,      key.producerId);
        wle<int32_t>(ep + 8,  key.partition);
        wle<int16_t>(ep + 12, e.producerEpoch);
        wle<int16_t>(ep + 14, 0); // pad
        wle<int32_t>(ep + 16, e.lastSequence);
        wle<int32_t>(ep + 20, e.windowHead);
        wle<int32_t>(ep + 24, e.windowCount);
        wle<int32_t>(ep + 28, 0); // pad

        uint8_t* wp = ep + kEntryFixedSize;
        for (int i = 0; i < kRetryWindowSize; ++i) {
            encodeBatch(wp + static_cast<std::size_t>(i) * kBatchDescSize, e.window[i]);
        }

        // CRC over the entry body (excluding the trailing checksum itself).
        uint32_t cksum = crc32c(ep, kEntryFixedSize + kWindowBytes);
        wle<uint32_t>(ep + kEntryFixedSize + kWindowBytes, cksum);

        p += kEntrySize;
    }

    assert(p == buf.data() + totalSize);

    // Atomic write: write to a temp file, fsync, rename.
    std::string tmp = path + ".tmp";
    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw std::system_error(errno, std::system_category(),
                                "ProducerStateManager::takeSnapshot open " + tmp);
    }

    ssize_t written = ::write(fd, buf.data(), static_cast<std::size_t>(totalSize));
    if (written != static_cast<ssize_t>(totalSize)) {
        ::close(fd);
        throw std::system_error(errno, std::system_category(),
                                "ProducerStateManager::takeSnapshot write " + tmp);
    }

    if (::fsync(fd) != 0) {
        ::close(fd);
        throw std::system_error(errno, std::system_category(),
                                "ProducerStateManager::takeSnapshot fsync " + tmp);
    }
    ::close(fd);

    if (::rename(tmp.c_str(), path.c_str()) != 0) {
        throw std::system_error(errno, std::system_category(),
                                "ProducerStateManager::takeSnapshot rename " + path);
    }
}

// ---------------------------------------------------------------------------
// recover — read snapshot, decode entries
// ---------------------------------------------------------------------------
int ProducerStateManager::recover(const std::string& path)
{
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) return 0; // no snapshot → start fresh
        throw std::system_error(errno, std::system_category(),
                                "ProducerStateManager::recover open " + path);
    }

    // Determine file size.
    struct stat st{};
    if (::fstat(fd, &st) != 0) {
        ::close(fd);
        throw std::system_error(errno, std::system_category(),
                                "ProducerStateManager::recover fstat " + path);
    }
    const auto fileSize = static_cast<std::size_t>(st.st_size);

    if (fileSize < kFileHdrSize) {
        ::close(fd);
        return 0; // truncated/empty
    }

    std::vector<uint8_t> buf(fileSize);
    ssize_t n = ::read(fd, buf.data(), fileSize);
    ::close(fd);
    if (n != static_cast<ssize_t>(fileSize)) {
        return 0; // I/O error
    }

    const uint8_t* p = buf.data();

    // Validate file header.
    if (rle<uint32_t>(p) != kFileMagic || rle<uint32_t>(p + 4) != kFileVersion) {
        return 0; // wrong magic/version
    }
    const uint32_t count = rle<uint32_t>(p + 8);
    p += kFileHdrSize;

    // S4: 64-bit arithmetic to prevent overflow for large count values from disk.
    const uint64_t needed = static_cast<uint64_t>(kFileHdrSize) +
                            static_cast<uint64_t>(count) * static_cast<uint64_t>(kEntrySize);
    if (static_cast<uint64_t>(fileSize) < needed) {
        return 0; // file truncated
    }
    // Bound count against fileSize to prevent over-read if count is forged.
    const uint32_t maxEntries = static_cast<uint32_t>((fileSize - kFileHdrSize) / kEntrySize);

    map_.clear();
    int loaded = 0;
    // Use min(count, maxEntries) to guard against forged count on disk (S4).
    const uint32_t safeCount = std::min(count, maxEntries);

    for (uint32_t i = 0; i < safeCount; ++i) {
        const uint8_t* ep = p + static_cast<std::size_t>(i) * kEntrySize;

        // CRC check.
        uint32_t storedCksum = rle<uint32_t>(ep + kEntryFixedSize + kWindowBytes);
        uint32_t computed    = crc32c(ep, kEntryFixedSize + kWindowBytes);
        if (storedCksum != computed) {
            break; // torn/corrupt entry — stop here, keep what we have
        }

        ProducerPartitionKey key;
        key.producerId = rle<int64_t>(ep);
        key.partition  = rle<int32_t>(ep + 8);

        ProducerStateEntry e;
        e.producerEpoch = rle<int16_t>(ep + 12);
        // [14..15] pad
        e.lastSequence  = rle<int32_t>(ep + 16);
        e.windowHead    = rle<int32_t>(ep + 20);
        e.windowCount   = rle<int32_t>(ep + 24);
        // [28..31] pad

        // S5: clamp windowHead/windowCount to valid ranges (defense against corrupt snapshots).
        if (e.windowCount < 0 || e.windowCount > kRetryWindowSize)
            e.windowCount = std::clamp(e.windowCount, 0, kRetryWindowSize);
        if (e.windowHead < 0 || e.windowHead >= kRetryWindowSize)
            e.windowHead = 0;

        const uint8_t* wp = ep + kEntryFixedSize;
        for (int j = 0; j < kRetryWindowSize; ++j) {
            e.window[j] = decodeBatch(wp + static_cast<std::size_t>(j) * kBatchDescSize);
        }

        map_.emplace(key, e);
        ++loaded;
    }

    return loaded;
}

} // namespace rune::storage
