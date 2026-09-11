#include <rune/storage/LogCleaner.h>
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
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

// Write bytes to path (overwrite).
void writeFile(const std::string& path, const std::vector<uint8_t>& data) {
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    if (!f.is_open()) throw std::runtime_error("LogCleaner: cannot open " + path);
    if (!data.empty())
        f.write(reinterpret_cast<const char*>(data.data()),
                static_cast<std::streamsize>(data.size()));
    f.flush();
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
    int32_t bytesSince = indexIntervalBytes; // trigger first entry immediately
    while (pos + static_cast<size_t>(rune::protocol::batch::kRecordBatchOverhead) <= logData.size()) {
        rune::protocol::RecordBatchReader r(logData.data() + pos, logData.size() - pos);
        int32_t total = r.totalLength();
        if (total <= 0 || pos + static_cast<size_t>(total) > logData.size()) break;

        if (bytesSince >= indexIntervalBytes) {
            int32_t relOff = static_cast<int32_t>(r.baseOffset() - baseOffset);
            int32_t physPos = static_cast<int32_t>(pos);
            // Native byte order — matches OffsetIndex::append() which uses memcpy.
            idx.write(reinterpret_cast<const char*>(&relOff),  4);
            idx.write(reinterpret_cast<const char*>(&physPos), 4);
            bytesSince = 0;
        }

        bytesSince += total;
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
            if (total <= 0 || pos + static_cast<size_t>(total) > buf.size()) break;

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
        if (total <= 0 || pos + static_cast<size_t>(total) > buf.size()) break;

        // Collect records to keep from this batch.
        // We rebuild the batch only with the surviving records, preserving
        // original offsets via offsetDelta (single-record batches so each
        // record retains its exact original offset).
        //
        // Strategy: emit each surviving record as its own 1-record batch
        // (same technique Kafka uses for compacted segments — each kept
        // record becomes a batch with baseOffset == that record's offset).
        r.forEach([&](std::string_view key, std::string_view val,
                      int64_t offset, int64_t timestamp) {
            // Helper: emit this record as its own single-record batch.
            auto keep = [&]() {
                rune::protocol::RecordBatchBuilder b(offset, timestamp);
                b.append(key, val, timestamp);
                auto batch = b.build();
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

    // Write the temp file.
    writeFile(newPath, outBuf);

    // Atomically rename: newPath → logPath.
    // If this process crashes between write and rename, we leave the .new file.
    // On next startup, LogCleaner::compact() will clean up stale .new files
    // (see compact() preamble).
    std::filesystem::rename(newPath, logPath);

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

} // namespace rune::storage
