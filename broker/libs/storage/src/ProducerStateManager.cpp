// ---------------------------------------------------------------------------
// ProducerStateManager.cpp — Kafka-style idempotent producer dedup.
//
// On-disk snapshot format (all little-endian), VERSION 3:
//
// File header:
//   [0..3]   uint32  magic        = kFileMagic (0x50534D47 "PSMG")
//   [4..7]   uint32  version      = 3
//   [8..11]  uint32  entryCount
//   [12..15] uint32  windowSlots  = W (kRetryWindowSize at write time)
//
// Per-entry (fixed size = 32 + W*16 + 4):
//   [0..7]   int64   producerId
//   [8..11]  int32   partition
//   [12..13] int16   producerEpoch
//   [14..15] int16   _pad = 0
//   [16..19] int32   lastSequence
//   [20..23] int32   windowHead
//   [24..27] int32   windowCount
//   [28..31] int32   topicId
//   Then W * 16 bytes of window slots:
//     slot: int32 baseSequence, int32 lastSequence, int64 baseOffset (16 bytes)
//   Trailing uint32 CRC32C of everything in the entry before the checksum.
//
// Version 2 (TX-2c .. EOS-OOS): identical except [12..15] was pad (0) and W was
// implicitly 5.  recover() reads both; a recorded W different from the current
// kRetryWindowSize is re-windowed (slots replayed oldest-first, the newest
// min(W, kRetryWindowSize) kept).  Version 1 (no topicId) is rejected.
//
// The file is written atomically: temp file, fsync, rename.
// A torn entry (file too short, CRC mismatch) stops replay; we keep what we
// decoded so far — the last snapshot is always consistent.
// ---------------------------------------------------------------------------

#include <rune/storage/ProducerStateManager.h>
#include <rune/storage/Log.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/ControlMarkerLayout.h>
#include <rune/protocol/RecordBatch.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstring>
#include <limits>
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
// TX-2c: version 2 added topicId to the per-entry header.
// EOS-OOS: version 3 records the window size in the file header (the window
// grew 5 -> kIdempotentMaxInFlightBatches).  Version 1 is rejected.
static constexpr uint32_t kFileVersion   = 3u;
static constexpr uint32_t kFileVersionV2 = 2u;
static constexpr uint32_t kV2WindowSlots = 5u;
static constexpr uint32_t kFileHdrSize   = ProducerStateManager::kSnapshotFileHeaderBytes;

static constexpr uint32_t kBatchDescSize = 16u;       // int32+int32+int64
// Per-entry fixed header:
//   producerId(8) + partition(4) + epoch(2) + pad(2)
//   + lastSequence(4) + windowHead(4) + windowCount(4) + topicId(4) = 32
static constexpr uint32_t kEntryFixedSize = 32u;
static constexpr uint32_t kChecksumSize   = 4u;
static constexpr uint32_t entrySizeFor(uint32_t windowSlots) noexcept {
    return kEntryFixedSize + windowSlots * kBatchDescSize + kChecksumSize;
}
static constexpr uint32_t kWindowBytes = static_cast<uint32_t>(kRetryWindowSize) * kBatchDescSize;
static constexpr uint32_t kEntrySize   = entrySizeFor(static_cast<uint32_t>(kRetryWindowSize));
static_assert(kEntrySize == ProducerStateManager::kSnapshotEntryBytes);

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
        // D6 (Kafka UNKNOWN_PRODUCER_ID): a non-zero first sequence means this
        // state does not know the producer — typically retention removed all its
        // batches before a rebuild.  NOT appended; the caller answers it with
        // the partition's logStartOffset (OutOfOrderSequence to old clients).
        if (baseSequence != 0) return ValidationResult::unknownProducer();
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

    // Same epoch.  Hot path: the next in-order batch — header only, no scan.
    if (baseSequence > e.lastSequence) {
        // lastSequence == -1 means no batch appended yet in this epoch → expect 0.
        if (baseSequence == e.lastSequence + 1) return ValidationResult::accept();
        // D6 L1: a rebuilt marker-only entry knows the epoch, not the producer's sequence.
        if (e.markerOnly) return ValidationResult::unknownProducer();
        return ValidationResult::oos();   // forward gap
    }

    // Review I2: sequences wrap INT32_MAX -> 0 (Kafka incrementSequence).  The
    // batch after lastSequence == INT32_MAX starts at 0 and is IN ORDER — never
    // a known-old duplicate (that would answer OK without appending: loss).
    if (e.lastSequence == std::numeric_limits<int32_t>::max() && baseSequence == 0)
        return ValidationResult::accept();

    // baseSequence <= lastSequence: a retransmit (rare).  An exact match in the
    // window answers with the original offset.  Filled slots are [0, count)
    // (circular only once full; the match is order-independent).
    for (int i = 0; i < e.windowCount; ++i) {
        const auto& r = e.seqs[static_cast<std::size_t>(i)];
        if (r.base == baseSequence && r.last == lastSequenceInBatch) {
            const int64_t off = e.baseOffsets[static_cast<std::size_t>(i)];
            return ValidationResult::dup(off, off + (r.last - r.base));
        }
    }

    // EOS-OOS (Kafka DUPLICATE_SEQUENCE_NUMBER parity): entirely at or below
    // lastSequence but not in the window → a KNOWN-OLD duplicate.  Every
    // sequence <= lastSequence of this epoch was appended (in order, from 0), so
    // the records are stored: OK with an unknown offset, never re-appended.  Its
    // answer waits on the batch holding lastSequence (the newest slot).
    if (lastSequenceInBatch <= e.lastSequence) {
        const int n = e.newestSlot();
        const int64_t covered = n < 0 ? int64_t{-1}
            : e.baseOffsets[static_cast<std::size_t>(n)] +
              (e.seqs[static_cast<std::size_t>(n)].last - e.seqs[static_cast<std::size_t>(n)].base);
        return ValidationResult::dup(-1, covered);
    }

    // Partially overlaps lastSequence (base <= last < end) → OutOfOrderSequence.
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

    // F15 S5: a replayed batch older than an already-applied fence (or a
    // later epoch) never re-enters the window — it would make a new session's
    // sequence look like a duplicate of zombie data.
    if (producerEpoch < e.producerEpoch) return;

    if (producerEpoch > e.producerEpoch) {
        // New epoch: clear retry window + reset sequence state.
        e.producerEpoch = producerEpoch;
        e.lastSequence  = -1;
        e.resetWindow();
    }

    // Record the batch in the retry window (circular, evicts oldest when full).
    e.push(baseSequence, lastSequence, baseOffset);
    e.lastSequence = lastSequence;
    e.markerOnly   = false;   // D6 L1: a sequence of this epoch is known now
}

// ---------------------------------------------------------------------------
// fenceEpoch — F15 S5 (C1+, I3): raise only
// ---------------------------------------------------------------------------
void ProducerStateManager::fenceEpoch(int32_t topicId, int64_t producerId, int32_t partition,
                                      int16_t producerEpoch, bool fromRebuild, int32_t coordinatorEpoch)
{
    if (producerId < 0 || producerEpoch < 0) return;
    ProducerStateEntry& e = getOrCreate(topicId, producerId, partition);
    if (coordinatorEpoch > e.coordinatorEpoch) e.coordinatorEpoch = coordinatorEpoch;   // D9 MAJOR-2
    if (producerEpoch <= e.producerEpoch) return;
    e.producerEpoch = producerEpoch;
    e.lastSequence  = -1;
    e.markerOnly    = fromRebuild;   // D6 L1: no sequence of this epoch survives in the log
    e.resetWindow();
}

bool ProducerStateManager::markerCoordinatorFenced(int32_t topicId, int64_t producerId, int32_t partition,
                                                   int32_t coordinatorEpoch) const
{
    const ProducerStateEntry* e = entry(topicId, producerId, partition);
    return e != nullptr && coordinatorEpoch < e->coordinatorEpoch;
}

// ---------------------------------------------------------------------------
// clearPartition — F15 S5 (I3): before a rebuild of (topicId, partition)
// ---------------------------------------------------------------------------
void ProducerStateManager::clearPartition(int32_t topicId, int32_t partition)
{
    std::erase_if(map_, [&](const auto& kv) noexcept {
        return kv.first.topicId == topicId && kv.first.partition == partition;
    });
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
    wle<uint32_t>(p + 12, static_cast<uint32_t>(kRetryWindowSize)); // v3: window slots
    p += kFileHdrSize;

    // Entries.
    for (const auto& [key, e] : map_) {
        uint8_t* ep = p;

        wle<int64_t>(ep,      key.producerId);
        wle<int32_t>(ep + 8,  key.partition);
        wle<int16_t>(ep + 12, e.producerEpoch);
        wle<int16_t>(ep + 14, 0); // pad
        wle<int32_t>(ep + 16, e.lastSequence);
        wle<int32_t>(ep + 20, static_cast<int32_t>(e.windowHead));
        wle<int32_t>(ep + 24, static_cast<int32_t>(e.windowCount));
        wle<int32_t>(ep + 28, key.topicId); // TX-2c: topicId (replaces former pad)

        uint8_t* wp = ep + kEntryFixedSize;
        for (int i = 0; i < kRetryWindowSize; ++i) {
            encodeBatch(wp + static_cast<std::size_t>(i) * kBatchDescSize, e.slot(i));
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

    // Validate file header (v3: recorded window size; v2: implicit 5 slots).
    if (rle<uint32_t>(p) != kFileMagic) return 0;
    const uint32_t version = rle<uint32_t>(p + 4);
    uint32_t slots = 0;
    if (version == kFileVersion) {
        slots = rle<uint32_t>(p + 12);
        if (slots == 0 || slots > 255u) return 0;           // corrupt header
    } else if (version == kFileVersionV2) {
        slots = kV2WindowSlots;
    } else {
        return 0;                                           // v1 / unknown
    }
    const uint32_t entrySize   = entrySizeFor(slots);
    const uint32_t windowBytes = slots * kBatchDescSize;
    const uint32_t count = rle<uint32_t>(p + 8);
    p += kFileHdrSize;

    // S4: 64-bit arithmetic to prevent overflow for large count values from disk.
    const uint64_t needed = static_cast<uint64_t>(kFileHdrSize) +
                            static_cast<uint64_t>(count) * static_cast<uint64_t>(entrySize);
    if (static_cast<uint64_t>(fileSize) < needed) {
        return 0; // file truncated
    }
    // Bound count against fileSize to prevent over-read if count is forged.
    const uint32_t maxEntries = static_cast<uint32_t>((fileSize - kFileHdrSize) / entrySize);

    map_.clear();
    int loaded = 0;
    // Use min(count, maxEntries) to guard against forged count on disk (S4).
    const uint32_t safeCount = std::min(count, maxEntries);

    for (uint32_t i = 0; i < safeCount; ++i) {
        const uint8_t* ep = p + static_cast<std::size_t>(i) * entrySize;

        // CRC check.
        uint32_t storedCksum = rle<uint32_t>(ep + kEntryFixedSize + windowBytes);
        uint32_t computed    = crc32c(ep, kEntryFixedSize + windowBytes);
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
        int32_t head    = rle<int32_t>(ep + 20);
        int32_t cnt     = rle<int32_t>(ep + 24);
        key.topicId     = rle<int32_t>(ep + 28); // TX-2c: topicId (was former pad)

        // S5: clamp windowHead/windowCount to valid ranges (defense against corrupt snapshots).
        const auto islots = static_cast<int32_t>(slots);
        cnt = std::clamp(cnt, 0, islots);
        if (head < 0 || head >= islots) head = 0;

        // Replay the recorded slots oldest-first into the current window: the
        // newest min(slots, kRetryWindowSize) survive, in order (re-windowing
        // when the recorded size differs; identity when it is the same).
        const uint8_t* wp = ep + kEntryFixedSize;
        const int32_t oldest = (cnt < islots) ? 0 : head;
        for (int32_t j = 0; j < cnt; ++j) {
            const int32_t s = (oldest + j) % islots;
            const BatchDescriptor bd = decodeBatch(wp + static_cast<std::size_t>(s) * kBatchDescSize);
            e.push(bd.baseSequence, bd.lastSequence, bd.baseOffset);
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
// Control batches are applied as epoch raises (fenceEpoch, F15 S5 / I3) —
// never as sequences.
//
// PSM <-> log consistency (review of b4594f8; supersedes TX-2b's HW bound): a
// state that answers produces is built to the LEO — every stored batch, the
// leader's kept tail above HW included (Kafka loads producer state to the log
// end).  Built only to HW it ACCEPTED a resend of the first tail batch (a
// silent duplicate) and answered OOS for later stored ones.  The tail's
// possible KIP-101 truncation is handled by invalidation: the owner rebuilds
// on every log-generation / leadership change (RcState, proveInlinePsm_), so
// no window ever covers a truncated-away offset.
//
// takeSnapshot / recover: not called by the broker (no production caller).
// recover() restores whatever was snapshotted without checking it against the
// log, so a future caller must re-validate (rebuild the suffix past the
// snapshot and drop it on a log-generation change) before answering produces.
//
// Production callers of rebuildFromLog:
//   • ClientGateway::proveInlinePsm_ — the inline path, per partition, to LEO.
//   • Worker::provePassthroughPsm_ — replica-less partitions, to LEO.
//   • RcState::advance — a memory-backed partition (InMemoryLog), to LEO.
//   • PartitionReplica::truncateLog (Task 11) — its own (test-only) state.
//
// Cold path: allocations acceptable.
// ---------------------------------------------------------------------------
namespace {
// The first segment base strictly above `offset` (a compaction gap below the
// bound), or -1.  An InMemoryLog has no gaps: every offset in [logStart, LEO)
// is held by a batch.
int64_t nextBaseAfter(Log& log, int64_t offset) noexcept { return log.nextSegmentBaseAfter(offset); }
int64_t nextBaseAfter(InMemoryLog&, int64_t) noexcept { return -1; }
// One past the last written offset of the segment holding `offset` (-1 = unknown / no segment).
int64_t segmentEndAt(Log& log, int64_t offset) noexcept { return log.segmentNextOffsetAt(offset); }
int64_t segmentEndAt(InMemoryLog&, int64_t) noexcept { return -1; }
// Per-step budget of the synchronous rebuildFromLog loop.
constexpr std::size_t kSyncStepBytes = 4u * 1024u * 1024u;
} // namespace

template<class LogT>
bool ProducerStateManager::rebuildFromLog(int32_t topicId,
                                           LogT&    log,
                                           int64_t highWatermark,
                                           int32_t partition)
{
    Rebuild r;
    beginRebuild(r, topicId, log, highWatermark, partition);
    Rebuild::Status st = Rebuild::Status::InProgress;
    while (st == Rebuild::Status::InProgress) st = rebuildStep(r, log, kSyncStepBytes);
    return st == Rebuild::Status::Done;
}

template<class LogT>
void ProducerStateManager::beginRebuild(Rebuild& r, int32_t topicId, LogT& log, int64_t bound,
                                        int32_t partition)
{
    // Task 11: record the bound for PartitionReplica::truncateLog's re-rebuild test.
    rebuildHw_ = bound;
    // TX-2c: clear ONLY this (topicId, partition) — the map is shared.
    clearPartition(topicId, partition);
    r.topicId   = topicId;
    r.partition = partition;
    // CP-3: start at the first surviving offset (retention advances it).
    r.next      = log.logStartOffset();
    r.target    = bound;
}

template<class LogT>
ProducerStateManager::Rebuild::Status
ProducerStateManager::rebuildStep(Rebuild& r, LogT& log, std::size_t budgetBytes)
{
    using S = Rebuild::Status;
    const int64_t logStart = log.logStartOffset();
    if (r.next < logStart) r.next = logStart;   // retention between steps: those batches are gone
    std::size_t consumed = 0;
    while (r.next < r.target) {
        if (consumed >= budgetBytes && consumed > 0) return S::InProgress;
        const std::size_t room = budgetBytes > consumed ? budgetBytes - consumed : 1;
        const LogReadView v = log.readView(r.next, static_cast<int32_t>(
            std::min<std::size_t>(room, static_cast<std::size_t>(std::numeric_limits<int32_t>::max()))));
        if (v.empty()) {
            // Re-review C1: an empty view BELOW the segment's written end means bytes exist that
            // cannot be read (corrupt length, failed mapping) — never a gap: fail the proof.
            if (const int64_t segEnd = segmentEndAt(log, r.next); segEnd > r.next) return S::Failed;
            // CP-4: a compaction gap (a sealed segment's superseded tail) — or
            // the end of the data.  Skip to the next segment base.
            const int64_t nb = nextBaseAfter(log, r.next);
            if (nb < 0 || nb >= r.target) return S::Done;
            if (nb <= r.next) return S::Failed;   // no progress
            r.next = nb;
            continue;
        }
        // readView yields WHOLE batches only (the first one always): a batch is
        // never cut at the window edge.
        std::size_t pos = 0;
        const int64_t before = r.next;
        constexpr std::size_t kOverhead = static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead);
        while (pos + kOverhead <= v.len) {
            rune::protocol::RecordBatchReader reader(v.data + pos, v.len - pos);
            const int32_t tl = reader.totalLength();
            if (tl <= 0 || static_cast<std::size_t>(tl) > v.len - pos) return S::Failed;
            const int64_t batchBase = reader.baseOffset();
            if (batchBase >= r.target) return S::Done;   // past the bound (before its CRC matters)
            if (!reader.ensureValid()) return S::Failed;
            const int32_t lastDelta = reader.lastOffsetDelta();
            const int64_t batchLast = batchBase + lastDelta;
            if (batchLast >= r.next) {   // below the cursor: already applied (re-framed by compaction)
                const int64_t pid = reader.producerId();
                const bool isControl =
                    (reader.attributes() & rune::protocol::batch::kAttrControl) != 0;
                // F15 S5 (I3): a control batch is an epoch raise, never a sequence.
                if (isControl && pid >= 0) {
                    fenceEpoch(r.topicId, pid, r.partition, reader.producerEpoch(), /*fromRebuild=*/true,
                               control_marker::coordinatorEpoch(v.data + pos, static_cast<std::size_t>(tl)));
                } else if (!isControl && pid >= 0 && reader.baseSequence() >= 0) {
                    const int32_t baseSeq = reader.baseSequence();
                    update(r.topicId, pid, r.partition, reader.producerEpoch(), baseSeq, baseSeq + lastDelta,
                           batchBase);
                }
                r.next = batchLast + 1;
            }
            pos += static_cast<std::size_t>(tl);
        }
        if (pos != v.len) return S::Failed;   // a fragment inside the written bytes
        consumed += v.len;
        if (r.next <= before) {
            // The whole view lay below the cursor: continue after it.
            if (v.nextOffset <= r.next) return S::Failed;
            r.next = v.nextOffset;
        }
    }
    return S::Done;
}

// De-virtualized seam (Milestone D Phase 2): explicit instantiations for the two
// concrete log backends.  These are the only log types rebuildFromLog is ever
// used with; each instantiation calls read()/segmentBaseOffsets() directly (no
// vtable).  Keeping the definition out-of-line avoids pulling the segment/
// filesystem machinery into every translation unit that includes the header.
template bool ProducerStateManager::rebuildFromLog<Log>(
        int32_t, Log&, int64_t, int32_t);
template bool ProducerStateManager::rebuildFromLog<InMemoryLog>(
        int32_t, InMemoryLog&, int64_t, int32_t);
template void ProducerStateManager::beginRebuild<Log>(Rebuild&, int32_t, Log&, int64_t, int32_t);
template void ProducerStateManager::beginRebuild<InMemoryLog>(Rebuild&, int32_t, InMemoryLog&, int64_t, int32_t);
template ProducerStateManager::Rebuild::Status
ProducerStateManager::rebuildStep<Log>(Rebuild&, Log&, std::size_t);
template ProducerStateManager::Rebuild::Status
ProducerStateManager::rebuildStep<InMemoryLog>(Rebuild&, InMemoryLog&, std::size_t);

} // namespace rune::storage
