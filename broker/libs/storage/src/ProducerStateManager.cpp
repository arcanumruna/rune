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
#include <rune/storage/Log.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/protocol/RecordBatch.h>

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
// TX-2c: bumped to version 2 to include topicId in the per-entry header.
// Version 1 snapshots (no topicId) are rejected cleanly in recover().
static constexpr uint32_t kFileVersion = 2u;
static constexpr uint32_t kFileHdrSize = 16u;         // magic+version+count+pad

static constexpr uint32_t kBatchDescSize = 16u;       // int32+int32+int64
// Per-entry fixed header (TX-2c, version 2):
//   producerId(8) + partition(4) + epoch(2) + pad(2)
//   + lastSequence(4) + windowHead(4) + windowCount(4) + topicId(4) = 32
// topicId replaces the former trailing pad(4) at [28..31].
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
ProducerStateManager::validate(int32_t topicId,
                               int64_t producerId,
                               int32_t partition,
                               int16_t producerEpoch,
                               int32_t baseSequence,
                               int32_t lastSequenceInBatch) const noexcept
{
    // A1: Protocol invariant enforced at runtime (not just via assert which is a no-op
    // under NDEBUG).  A negative baseSequence or a lastSequence below baseSequence is
    // protocol-illegal; reject with OutOfOrderSequence in all build modes.
    if (baseSequence < 0 || lastSequenceInBatch < baseSequence)
        return ValidationResult::oos();

    const ProducerPartitionKey key{topicId, producerId, partition};
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
void ProducerStateManager::update(int32_t topicId,
                                  int64_t producerId,
                                  int32_t partition,
                                  int16_t producerEpoch,
                                  int32_t baseSequence,
                                  int32_t lastSequence,
                                  int64_t baseOffset)
{
    ProducerStateEntry& e = getOrCreate(topicId, producerId, partition);

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
ProducerStateManager::getOrCreate(int32_t topicId, int64_t producerId, int32_t partition)
{
    return map_[ProducerPartitionKey{topicId, producerId, partition}];
}

// ---------------------------------------------------------------------------
// entry (read-only accessor)
// ---------------------------------------------------------------------------
const ProducerStateEntry*
ProducerStateManager::entry(int32_t topicId, int64_t producerId, int32_t partition) const noexcept
{
    auto it = map_.find(ProducerPartitionKey{topicId, producerId, partition});
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
        wle<int32_t>(ep + 28, key.topicId); // TX-2c: topicId (replaces former pad)

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
        key.topicId     = rle<int32_t>(ep + 28); // TX-2c: topicId (was former pad)

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

// ---------------------------------------------------------------------------
// rebuildFromLog — reconstruct producer dedup state by scanning a partition Log.
//
// Mirrors ReplicatedTxnStateStore::rebuildFromLog() (and
// ReplicatedOffsetStore::rebuildFromLog()):
//   - Clear the existing map first (idempotent rebuild).
//   - Scan [0, highWatermark) batch by batch using the multi-segment loop that
//     advances fetchOffset so Log::read() is called once per segment boundary.
//   - For each RecordBatch whose producerId >= 0 (idempotent producer), call
//     update() to record the batch in the retry window.
//   - Scanning forward from offset 0 naturally reproduces the correct window
//     state at the log end: the circular window evicts oldest-first, matching
//     the real produce sequence.
//
// Non-idempotent batches (producerId == -1) are silently skipped.
// Control batches are also skipped (their producerId is set but baseSequence
// is typically -1 — the PSM only tracks data batches).
//
// TX-2b: highWatermark MUST be the committed HW (replica->highWatermark()),
// not the log-end-offset (LEO).  Batches above HW may be truncated by KIP-101
// divergence resolution; seeding PSM state from them would produce a false-
// Duplicate for a producer retry of a never-committed sequence, with the
// duplicate-reported offset pointing at a truncated-away (non-existent) log
// position — acknowledged data loss.
//
// takeSnapshot / recover: not called in the current codebase hot path.  They
// are a future Phase-5 optimisation for fast restart on large logs (skip the
// full scan with a pre-built PSM image).
//
// Production callers of rebuildFromLog:
//   • ClientGateway::rebuildProducerStateFromLogs (ClientGateway.cpp:212) —
//     called on leader election / restart over every (topicId, partition).
//   • PartitionReplica::truncateLog (Task 11) — re-rebuild fired atomically
//     when a KIP-101 divergence truncation lands at or below the rebuild HW.
//
// Cold path: allocations acceptable.
// ---------------------------------------------------------------------------
template<class LogT>
void ProducerStateManager::rebuildFromLog(int32_t topicId,
                                           LogT&    log,
                                           int64_t highWatermark,
                                           int32_t partition)
{
    // Task 11: record the HW bound for this rebuild so PartitionReplica::truncateLog
    // can detect when a subsequent KIP-101 divergence truncation falls below it and
    // must trigger a re-rebuild to keep the dedup window consistent.
    rebuildHw_ = highWatermark;

    // TX-2c multi-partition fix: clear ONLY the keys for this (topicId, partition)
    // instead of the whole map.  ClientGateway::rebuildProducerStateFromLogs calls
    // this method in a loop over every (topicId, partition) on ONE shared PSM.
    // The old map_.clear() wiped every prior partition's state on each iteration —
    // only the LAST partition iterated survived, making dedup silently broken for
    // any topic with >1 partition or any node hosting >1 (topicId, partition).
    // With erase_if, each call is still idempotent for its own (topicId, partition)
    // (RebuildIsIdempotent passes), but state from other partitions is preserved.
    std::erase_if(map_, [&](const auto& kv) noexcept {
        return kv.first.topicId == topicId && kv.first.partition == partition;
    });

    if (highWatermark <= 0) return;

    static constexpr int32_t kChunkBytes = 64 * 1024 * 1024; // 64 MiB per read

    // CP-3: start the scan at the log's FIRST surviving segment base, not a
    // hardcoded 0.  Size retention (Delete / CompactDelete via
    // Log::deleteOldSegments) can age out the front of the log, advancing the
    // first segment's base past 0.  In that case log.read(0) returns empty and
    // a fetchOffset==0 start would bail immediately, reconstructing NO producer
    // state — silently breaking idempotent dedup after any retention on the
    // producer-bearing log (e.g. __transaction_state).  Beginning at the first
    // live segment base makes rebuild robust to a trimmed log prefix.
    int64_t fetchOffset = 0;
    {
        const auto segBases = log.segmentBaseOffsets();
        if (!segBases.empty() && segBases.front() > 0) {
            fetchOffset = segBases.front();
        }
    }

    while (fetchOffset < highWatermark) {
        // Log::read() resolves only ONE segment per call (the segment that
        // contains fetchOffset).  Loop until we reach highWatermark.
        std::vector<uint8_t> data =
            log.read(fetchOffset, kChunkBytes);
        if (data.empty()) {
            // CP-4: log.read() can return empty even when more segments exist.
            // After compaction (Compact / CompactDelete policy via
            // LogCleaner::cleanSegment), records within a segment are removed
            // and the surviving records may not start at the segment's logical
            // base offset.  If fetchOffset falls in a gap (e.g. offset N was
            // compacted away, but the segment's base is < N), read() returns
            // empty because no batch covers offset N.  Simply breaking here
            // would stop the rebuild prematurely, leaving producer state for
            // later segments unseen — yielding spurious OOS errors on retries.
            //
            // Fix: when read() returns empty, try to advance fetchOffset to
            // the start of the next segment whose base is strictly greater
            // than the current fetchOffset.  If no such segment exists, the
            // log truly has no more data and we stop.
            const auto segBases = log.segmentBaseOffsets();
            auto it = std::upper_bound(segBases.begin(), segBases.end(), fetchOffset);
            if (it == segBases.end()) break; // no segment after fetchOffset
            fetchOffset = *it;
            continue;
        }

        std::size_t pos = 0;
        int64_t     lastBatchEnd = fetchOffset; // progress sentinel

        while (pos < data.size()) {
            const uint8_t*    batchStart = data.data() + pos;
            const std::size_t remaining  = data.size() - pos;

            if (remaining < static_cast<std::size_t>(
                    rune::protocol::batch::kRecordBatchOverhead))
                break;

            rune::protocol::RecordBatchReader reader(batchStart, remaining);

            const int32_t tl = reader.totalLength();
            if (tl <= 0 || static_cast<std::size_t>(tl) > remaining) break;

            // Only CRC-valid batches are accepted.
            if (!reader.ensureValid()) break;

            const int64_t batchBase = reader.baseOffset();
            const int32_t lastDelta = reader.lastOffsetDelta();
            const int64_t batchEnd  = batchBase + lastDelta + 1;

            // Skip control batches (COMMIT/ABORT markers) — they are tagged
            // with the CONTROL attribute bit; their baseSequence is meaningless
            // for idempotent-producer dedup (and is typically -1 when built by
            // ControlMarker::build which doesn't stamp a sequence).
            const bool isControl =
                (reader.attributes() & rune::protocol::batch::kAttrControl) != 0;

            // TX-2b: stop processing batches once we reach or exceed the HW bound.
            // The outer while(fetchOffset < highWatermark) controls which LOG
            // SEGMENTS are fetched, but within a single read() chunk the segment
            // may contain batches beyond HW (e.g. the uncommitted tail in the same
            // segment as the last committed batch).  We must stop here — otherwise
            // we would seed PSM state from uncommitted batches that may later be
            // truncated by KIP-101, producing a false-Duplicate for a real produce.
            if (batchBase >= highWatermark) break;

            const int64_t pid   = reader.producerId();
            const int16_t epoch = reader.producerEpoch();
            const int32_t baseSeq = reader.baseSequence();

            // Record idempotent data batches only:
            //   producerId >= 0 — idempotent producer
            //   baseSequence >= 0 — valid sequence (non-idempotent default is -1)
            //   not a control batch
            if (!isControl && pid >= 0 && baseSeq >= 0) {
                // lastSequence = baseSequence + lastOffsetDelta
                // (for a multi-record batch, lastOffsetDelta == recordCount - 1,
                //  matching how handleProduce computes lastSeqInBatch).
                const int32_t lastSeq = baseSeq + lastDelta;
                update(topicId, pid, partition, epoch, baseSeq, lastSeq, batchBase);
            }

            if (batchEnd > lastBatchEnd) lastBatchEnd = batchEnd;
            pos += static_cast<std::size_t>(tl);
        }

        if (lastBatchEnd <= fetchOffset) break; // no progress — stop
        fetchOffset = lastBatchEnd;
    }
}

// De-virtualized seam (Milestone D Phase 2): explicit instantiations for the two
// concrete log backends.  These are the only log types rebuildFromLog is ever
// used with; each instantiation calls read()/segmentBaseOffsets() directly (no
// vtable).  Keeping the definition out-of-line avoids pulling the segment/
// filesystem machinery into every translation unit that includes the header.
template void ProducerStateManager::rebuildFromLog<Log>(
        int32_t, Log&, int64_t, int32_t);
template void ProducerStateManager::rebuildFromLog<InMemoryLog>(
        int32_t, InMemoryLog&, int64_t, int32_t);

} // namespace rune::storage
