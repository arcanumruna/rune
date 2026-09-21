#include <rune/storage/LogCleaner.h>
#include <rune/protocol/RecordBatch.h>

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <climits>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

namespace rune::storage {

namespace {

// Mirror LogSegment::fname — produce the on-disk path for a segment.
std::string segmentLogPath(const std::string& dir, int64_t baseOffset) {
    std::ostringstream os;
    os << dir << "/" << std::setw(20) << std::setfill('0') << baseOffset << ".log";
    return os.str();
}

std::string segmentIndexPath(const std::string& dir, int64_t baseOffset) {
    std::ostringstream os;
    os << dir << "/" << std::setw(20) << std::setfill('0') << baseOffset << ".index";
    return os.str();
}

// Read the entire contents of a file into a byte vector.
std::vector<uint8_t> readFile(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) return {};
    f.seekg(0, std::ios::end);
    auto sz = f.tellg();
    if (sz <= 0) return {};
    std::vector<uint8_t> buf(static_cast<size_t>(sz));
    f.seekg(0, std::ios::beg);
    f.read(reinterpret_cast<char*>(buf.data()), sz);
    return buf;
}

// Write bytes to path (overwrite) and fsync the fd before returning.
// Uses POSIX open/write/fsync so we can get the fd for fsync — matching
// the idiom in libs/coordination/OffsetStore.cpp (pwrite+fsync pattern).
void writeFileDurable(const std::string& path, const std::vector<uint8_t>& data) {
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw std::system_error(errno, std::system_category(),
                                "LogCleaner: open " + path);
    }
    if (!data.empty()) {
        const char* ptr = reinterpret_cast<const char*>(data.data());
        size_t remaining = data.size();
        while (remaining > 0) {
            ssize_t n = ::write(fd, ptr, remaining);
            if (n < 0) {
                int saved = errno;
                ::close(fd);
                throw std::system_error(saved, std::system_category(),
                                        "LogCleaner: write " + path);
            }
            ptr += n;
            remaining -= static_cast<size_t>(n);
        }
    }
    // Flush to disk before rename — prevents partial/zero-byte file on power loss.
    if (::fsync(fd) != 0) {
        int saved = errno;
        ::close(fd);
        throw std::system_error(saved, std::system_category(),
                                "LogCleaner: fsync " + path);
    }
    ::close(fd);
}

// Fsync the directory so the rename is durable (the directory entry survives
// a power loss).  Mirrors the Kafka/NodeStateFile pattern.
void fsyncDir(const std::string& dir) {
    int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dfd < 0) {
        throw std::system_error(errno, std::system_category(),
                                "LogCleaner: open dir " + dir);
    }
    if (::fsync(dfd) != 0) {
        int saved = errno;
        ::close(dfd);
        throw std::system_error(saved, std::system_category(),
                                "LogCleaner: fsync dir " + dir);
    }
    ::close(dfd);
}

// Build a sparse .index from a contiguous log byte buffer.
// Uses the same 8-byte entry format as OffsetIndex:
//   [int32 relativeOffset (native order) | int32 physPosition (native order)]
// Writes an entry every ~indexIntervalBytes (matching LogSegment behaviour).
void buildIndex(const std::string& indexPath, int64_t baseOffset,
                const std::vector<uint8_t>& logData,
                int32_t indexIntervalBytes = 4096) {
    // Remove stale index.
    std::filesystem::remove(indexPath);
    std::ofstream idx(indexPath, std::ios::binary | std::ios::trunc);
    if (!idx.is_open()) throw std::runtime_error("LogCleaner: cannot open index " + indexPath);

    size_t pos = 0;
    // C2: seed bytesSince = 0 (matching the shouldIndex() rule in LogSegment):
    //   index the first batch (bytesSince == 0) and then every indexIntervalBytes.
    int64_t bytesSince = 0;
    while (pos + static_cast<size_t>(rune::protocol::batch::kRecordBatchOverhead) <= logData.size()) {
        rune::protocol::RecordBatchReader r(logData.data() + pos, logData.size() - pos);
        int32_t total = r.totalLength();
        if (total <= 0 || pos + static_cast<size_t>(total) > logData.size()) break;

        // C2: unified shouldIndex predicate (bytesSince==0 || bytesSince>=interval).
        if (bytesSince == 0 || bytesSince >= static_cast<int64_t>(indexIntervalBytes)) {
            // C4: guard 2GiB position limit before cast.
            if (pos > static_cast<size_t>(INT32_MAX))
                throw std::runtime_error("LogCleaner: buildIndex position exceeds 2GiB limit");
            int32_t relOff  = static_cast<int32_t>(r.baseOffset() - baseOffset);
            int32_t physPos = static_cast<int32_t>(pos);
            // Native byte order — matches OffsetIndex::append() which uses memcpy.
            idx.write(reinterpret_cast<const char*>(&relOff),  4);
            idx.write(reinterpret_cast<const char*>(&physPos), 4);
            bytesSince = 0;
        }

        bytesSince += static_cast<int64_t>(total);
        pos += static_cast<size_t>(total);
    }
    idx.flush();
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// buildOffsetMap
// ---------------------------------------------------------------------------
void LogCleaner::buildOffsetMap(Log& log, Offset fromOffset, Offset toOffset,
                                OffsetMap& map) {
    if (fromOffset >= toOffset) return;

    // Walk segments that overlap [fromOffset, toOffset).
    auto bases = log.segmentBaseOffsets();
    const std::string& dir = log.dir();

    // S6: The final segment's "end" is toOffset. Records in the active segment
    // at offsets < toOffset are scanned and their keys entered into the map; these
    // map entries match nothing during rewrite (the active segment is never
    // compacted), so they are harmlessly wasteful. The record-level filter below
    // (`offset >= toOffset`) already correctly gates on the compaction boundary.
    // Note: segEnd is only used for the "skip segment entirely" range check below;
    // the record filter uses the toOffset parameter directly.
    for (size_t i = 0; i < bases.size(); ++i) {
        int64_t segBase = bases[i];
        int64_t segEnd = (i + 1 < bases.size()) ? bases[i + 1] : toOffset;

        // Skip segments entirely outside [fromOffset, toOffset).
        if (segEnd <= fromOffset) continue;
        if (segBase >= toOffset) break;

        // Read segment from disk.
        auto buf = readFile(segmentLogPath(dir, segBase));
        if (buf.empty()) continue;

        size_t pos = 0;
        while (pos + static_cast<size_t>(rune::protocol::batch::kRecordBatchOverhead)
               <= buf.size()) {
            rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
            int32_t total = r.totalLength();
            // A3: bounds check before CRC; a corrupt totalLength must not misparse the walk.
            if (total <= 0 || pos + static_cast<size_t>(total) > buf.size()) break;
            // A3: CRC-gate the walk — stop on the first corrupt batch (same as recover()).
            if (!r.ensureValid()) break;

            r.forEach([&](std::string_view key, std::string_view /*val*/,
                          int64_t offset, int64_t /*ts*/) {
                if (offset < fromOffset || offset >= toOffset) return;
                if (key.empty()) return; // skip null-key records (not compactable)
                map.put(key, offset);
            });

            pos += static_cast<size_t>(total);
        }
    }
}

// ---------------------------------------------------------------------------
// rewriteSegment  (internal)
// ---------------------------------------------------------------------------
void LogCleaner::rewriteSegment(Log& log, int64_t segBase, OffsetMap& map,
                                Offset fromOffset, Offset cleanBoundaryOffset,
                                int64_t nowNs, int64_t deleteRetentionMs) {
    const std::string& dir = log.dir();
    std::string logPath   = segmentLogPath(dir, segBase);
    std::string newPath   = logPath + ".new";
    std::string idxPath   = segmentIndexPath(dir, segBase);

    // Remove stale temp from a prior crashed compaction.
    std::filesystem::remove(newPath);

    // Read the current segment.
    auto buf = readFile(logPath);
    if (buf.empty()) return;

    std::vector<uint8_t> outBuf;
    outBuf.reserve(buf.size());

    size_t pos = 0;
    int64_t deleteRetentionNs = deleteRetentionMs * 1'000'000LL;

    while (pos + static_cast<size_t>(rune::protocol::batch::kRecordBatchOverhead)
           <= buf.size()) {
        rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
        int32_t total = r.totalLength();
        // A3: bounds check before CRC; a corrupt totalLength must not misparse the walk.
        if (total <= 0 || pos + static_cast<size_t>(total) > buf.size()) break;
        // A3: CRC-gate the walk — stop on the first corrupt batch.
        if (!r.ensureValid()) break;

        // A2: Extract producer identity from the original batch so that surviving
        // records can carry the same producerId / producerEpoch / baseSequence.
        // Kafka's LogCleaner preserves these fields when rewriting compacted segments
        // so idempotent/transactional dedup state remains valid after compaction.
        // All-LE: use RecordBatchReader accessors (LE) for producerId/epoch/seq.
        int64_t origProducerId    = r.producerId();
        int16_t origProducerEpoch = r.producerEpoch();
        int32_t origBaseSequence  = static_cast<int32_t>(
            rune::protocol::detail::getLE32(buf.data() + pos + rune::protocol::batch::kBaseSequenceOffset));
        int64_t batchBaseOffset   = r.baseOffset();

        // Collect records to keep from this batch.
        // We rebuild the batch only with the surviving records, preserving
        // original offsets via offsetDelta (single-record batches so each
        // record retains its exact original offset).
        //
        // Strategy: emit each surviving record as its own 1-record batch
        // (same technique Kafka uses for compacted segments — each kept
        // record becomes a batch with baseOffset == that record's offset).
        // A2: After building, patch in the original producer fields and
        // recompute CRC to preserve idempotent-producer identity.
        r.forEach([&](std::string_view key, std::string_view val,
                      int64_t offset, int64_t timestamp) {
            // Helper: emit this record as its own single-record batch,
            // preserving the original batch's producer identity (A2).
            auto keep = [&]() {
                rune::protocol::RecordBatchBuilder b(offset, timestamp);
                b.append(key, val, timestamp);
                auto batch = b.build();

                // A2: Patch producerId, producerEpoch, baseSequence into the
                // rebuilt batch.  The record's sequence number within the
                // original batch is: origBaseSequence + (offset - batchBaseOffset).
                // For single-record batches origBaseSequence == offset's sequence.
                // We always patch (even when producerId == -1) to faithfully
                // preserve whatever producer identity the original batch carried.
                {
                    // All-LE: patch producerId/producerEpoch/baseSequence as LE.
                    // producerId at offset 43 (int64 little-endian)
                    {
                        uint64_t u = static_cast<uint64_t>(origProducerId);
                        for (int i = 0; i < 8; ++i)
                            batch[rune::protocol::batch::kProducerIdOffset + i] =
                                static_cast<uint8_t>(u >> (i * 8));
                    }
                    // producerEpoch at offset 51 (int16 little-endian)
                    {
                        uint16_t u = static_cast<uint16_t>(origProducerEpoch);
                        batch[rune::protocol::batch::kProducerEpochOffset]     = static_cast<uint8_t>(u);
                        batch[rune::protocol::batch::kProducerEpochOffset + 1] = static_cast<uint8_t>(u >> 8);
                    }
                    // baseSequence at offset 53 (int32 little-endian):
                    // record's sequence = origBaseSequence + offsetDelta within original batch.
                    {
                        int32_t recSeq = origBaseSequence + static_cast<int32_t>(offset - batchBaseOffset);
                        uint32_t u = static_cast<uint32_t>(recSeq);
                        for (int i = 0; i < 4; ++i)
                            batch[rune::protocol::batch::kBaseSequenceOffset + i] =
                                static_cast<uint8_t>(u >> (i * 8));
                    }
                    // Recompute CRC over [attributes..end] since we changed fields
                    // in the CRC-covered region.  Store CRC as LE uint32.
                    uint32_t newCrc = rune::protocol::crc32c(
                        batch.data() + rune::protocol::batch::kAttributesOffset,
                        batch.size() - rune::protocol::batch::kAttributesOffset);
                    batch[rune::protocol::batch::kCrcOffset]     = static_cast<uint8_t>(newCrc);
                    batch[rune::protocol::batch::kCrcOffset + 1] = static_cast<uint8_t>(newCrc >> 8);
                    batch[rune::protocol::batch::kCrcOffset + 2] = static_cast<uint8_t>(newCrc >> 16);
                    batch[rune::protocol::batch::kCrcOffset + 3] = static_cast<uint8_t>(newCrc >> 24);
                }

                outBuf.insert(outBuf.end(), batch.begin(), batch.end());
            };

            // Records before the dirty range (in the already-clean region)
            // are always kept verbatim.
            if (offset < fromOffset) { keep(); return; }

            // Records at/past the clean boundary (HW) are always kept verbatim.
            if (offset >= cleanBoundaryOffset) { keep(); return; }

            // Null-key records: always keep (not compactable).
            if (key.empty()) { keep(); return; }

            // Record is inside the dirty range [fromOffset, cleanBoundaryOffset).
            // Keep it only if its offset == the latest offset for this key.
            auto latest = map.get(key);
            if (!latest.has_value() || *latest != offset) {
                return; // not in map or superseded — drop
            }

            // This is the latest.  If it's a tombstone, check retention.
            if (isTombstone(val)) {
                int64_t tombstoneTs = timestamp; // timestamp in the record (nanoseconds)
                if ((nowNs - tombstoneTs) > deleteRetentionNs) {
                    return; // past retention — fully remove key + tombstone
                }
            }

            // Keep this record.
            keep();
        });

        pos += static_cast<size_t>(total);
    }

    // A4: If compaction removed ALL records from a non-active segment, outBuf is
    // empty.  Writing a 0-byte .log would leave a permanent dangling file that
    // never gets garbage-collected.  Instead, delete the segment and its index
    // directly and fsync the directory so the deletions are durable.
    // Log::reloadSegments() (called by compact() after all segments are
    // rewritten) only discovers segments with on-disk .log files, so a deleted
    // segment is simply absent — Log remains consistent and LEO is unaffected.
    if (outBuf.empty()) {
        std::filesystem::remove(logPath);
        std::filesystem::remove(idxPath);
        fsyncDir(dir);
        return;
    }

    // Durable atomic swap: write → fsync file → rename → fsync dir.
    //
    // 1. Write all bytes to the .new temp file.
    // 2. fsync the .new file (data reaches persistent storage before rename).
    // 3. Atomically rename .new → .log (POSIX guarantees atomicity).
    // 4. fsync the parent directory (the new directory entry survives power loss).
    //
    // Without step 2, a power-loss between flush and rename can leave a
    // partially-written .new file replacing the good .log.
    // Without step 4, the renamed directory entry may not survive a crash.
    //
    // If this process crashes between step 2 and step 3, we leave the fsynced
    // .new file.  On next startup, LogCleaner::compact() will clean it up
    // (see compact() preamble).
    writeFileDurable(newPath, outBuf);

    // 3. Atomic rename: .new → .log.
    std::filesystem::rename(newPath, logPath);

    // 4. fsync the parent directory so the renamed entry is durable.
    fsyncDir(dir);

    // Remove the stale .index so Log::recover() (called after all segments are
    // rewritten) rebuilds a correct sparse index from the compacted bytes.
    // Also write a fresh .index directly so crash recovery has a valid index.
    std::filesystem::remove(idxPath);
    buildIndex(idxPath, segBase, outBuf);
}

// ---------------------------------------------------------------------------
// compact
// ---------------------------------------------------------------------------
void LogCleaner::compact(Log& log, OffsetMap& map,
                         Offset firstDirtyOffset, Offset cleanBoundaryOffset,
                         int64_t nowNs, int64_t deleteRetentionMs) {
    const std::string& dir = log.dir();

    // Clean up any stale .log.new temp files left by a previous crashed run.
    for (auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.path().extension() == ".new") {
            std::filesystem::remove(e.path());
        }
    }

    auto bases = log.segmentBaseOffsets();
    if (bases.empty()) return;

    // The ACTIVE segment is always the last one — never compact it.
    // Also never compact segments whose base is >= cleanBoundaryOffset.
    int64_t activeBase = bases.back();

    for (int64_t segBase : bases) {
        if (segBase == activeBase) continue;       // active segment — skip
        if (segBase >= cleanBoundaryOffset) break; // at/past HW — stop
        rewriteSegment(log, segBase, map, firstDirtyOffset, cleanBoundaryOffset, nowNs, deleteRetentionMs);
    }

    // Reload the log's in-memory state from the rewritten files.
    // We must use reloadSegments() (not recover()) because the in-memory
    // LogSegment objects still hold open file handles to the OLD inodes
    // (pre-rename). recover() would read stale data through those old handles.
    // reloadSegments() closes all handles, re-discovers segment files on disk,
    // and runs recover() fresh — picking up the atomically renamed new content.
    log.reloadSegments();
}

// ---------------------------------------------------------------------------
// clean  (convenience: build map + compact in one call)
// ---------------------------------------------------------------------------
void LogCleaner::clean(Log& log, Offset firstDirtyOffset, Offset cleanBoundaryOffset,
                       int64_t nowNs, int64_t deleteRetentionMs) {
    OffsetMap map;
    buildOffsetMap(log, firstDirtyOffset, cleanBoundaryOffset, map);
    compact(log, map, firstDirtyOffset, cleanBoundaryOffset, nowNs, deleteRetentionMs);
}

// ---------------------------------------------------------------------------
// cleanSegment  (CP-2: bounded single-segment compaction — O(1) reload)
// ---------------------------------------------------------------------------
bool LogCleaner::cleanSegment(Log& log, Offset segBase, Offset segEnd,
                              Offset globalDirtyStart,
                              int64_t nowNs, int64_t deleteRetentionMs) {
    // Guard: the target segment must exist and must NOT be the active segment
    // (the active segment is always the highest-base segment and owns the LEO).
    auto bases = log.segmentBaseOffsets();
    if (bases.empty()) return false;
    const int64_t activeBase = bases.back();
    if (segBase == activeBase) return false;   // never compact the active segment
    bool found = false;
    for (int64_t b : bases) { if (b == segBase) { found = true; break; } }
    if (!found) return false;
    (void)segEnd; // retained for API symmetry / future byte-budgeted variants

    // Clean up any stale .log.new temp files left by a previous crashed run.
    for (auto& e : std::filesystem::directory_iterator(log.dir())) {
        if (e.path().extension() == ".new") {
            std::filesystem::remove(e.path());
        }
    }

    // Build the offset map GLOBALLY over the whole dirty range
    // [globalDirtyStart, activeBase).  This is the correctness fix: a record in
    // THIS segment that is superseded by a newer record in a LATER sealed segment
    // must be dropped, so latest-per-key has to be computed across ALL dirty
    // segments, not just this one.  (Scanning the whole dirty prefix each cycle is
    // O(dirty bytes); the expensive part that was O(n^2) — reopening every segment
    // via reloadSegments() — is avoided by reloadSegment() below.)
    if (globalDirtyStart < 0) globalDirtyStart = 0;
    OffsetMap map;
    buildOffsetMap(log, globalDirtyStart, activeBase, map);

    // Rewrite exactly the ONE segment at segBase.  We pass the GLOBAL bounds
    // (fromOffset = globalDirtyStart, cleanBoundaryOffset = activeBase) so the
    // per-record filter inside rewriteSegment consults the global map and drops
    // globally-superseded records.  rewriteSegment only ever reads+writes the
    // single segBase .log file, so passing global bounds does not touch any other
    // segment.
    rewriteSegment(log, segBase, map, globalDirtyStart, activeBase, nowNs, deleteRetentionMs);

    // Reload ONLY this segment in place — O(1) in the number of segments.  This
    // avoids the O(n) full-log reloadSegments() (re-recover() of EVERY segment)
    // that made a per-segment duty-cycle loop O(n^2) (~22s for 100 segments;
    // ~128ms after this change).
    log.reloadSegment(segBase);
    return true;
}

} // namespace rune::storage
