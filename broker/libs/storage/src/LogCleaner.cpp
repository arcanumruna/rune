#include <rune/storage/LogCleaner.h>
#include <rune/protocol/RecordBatch.h>

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <climits>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <limits>
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


namespace {
// D3 (I4): a COMMIT/ABORT marker batch.  Never key-compacted, never indexed in the
// offset map (Kafka: control batches are not subject to key compaction).
bool isControlBatch(const rune::protocol::RecordBatchReader& r) {
    return (static_cast<uint16_t>(r.attributes()) & static_cast<uint16_t>(rune::protocol::batch::kAttrControl)) != 0;
}
bool isTransactionalBatch(const rune::protocol::RecordBatchReader& r) {
    return (static_cast<uint16_t>(r.attributes()) &
            static_cast<uint16_t>(rune::protocol::batch::kAttrTransactional)) != 0;
}
// Kafka LogCleaner parity (ProducerTails.h): note the batch if it is an
// idempotent DATA batch starting below `toOffset`.
void noteProducerTail(const uint8_t* batch, const rune::protocol::RecordBatchReader& r, Offset toOffset,
                      ProducerTails& tails) {
    if (r.producerId() < 0 || r.baseSequence() < 0) return;
    if ((static_cast<uint16_t>(r.attributes()) & static_cast<uint16_t>(rune::protocol::batch::kAttrControl)) != 0)
        return;
    if (r.baseOffset() >= toOffset) return;
    tails.note(r.producerId(), r.baseOffset() + r.lastOffsetDelta(),
               static_cast<int64_t>(rune::protocol::detail::getLE64(batch + rune::protocol::batch::kMaxTimestampOffset)));
}
// C2 (Kafka BatchRetention.RETAIN_EMPTY): the RECORD-LESS producer-tail batch —
// covers `lastOffset`, carries the producer id / epoch / the batch's last
// sequence / its maxTimestamp, holds no record (no key can resurrect).
void appendEmptyTailBatch(std::vector<uint8_t>& out, std::vector<uint8_t>& tmp, int64_t lastOffset,
                          int64_t maxTimestampMs, int64_t producerId, int16_t producerEpoch, int32_t lastSequence,
                          bool transactional) {
    rune::protocol::RecordBatchBuilder b(lastOffset, maxTimestampMs);
    b.setTransactional(transactional);
    b.setProducerId(producerId);
    b.setProducerEpoch(producerEpoch);
    b.setBaseSequence(lastSequence);
    b.buildInto(tmp);   // recordCount 0 (the builder writes lastOffsetDelta -1)
    rune::protocol::detail::putLE(tmp, rune::protocol::batch::kLastOffsetDeltaOffset, static_cast<uint32_t>(0));
    const uint32_t crc = rune::protocol::crc32c(tmp.data() + rune::protocol::batch::kAttributesOffset,
                                                tmp.size() - rune::protocol::batch::kAttributesOffset);
    rune::protocol::detail::putLE(tmp, rune::protocol::batch::kCrcOffset, crc);
    out.insert(out.end(), tmp.begin(), tmp.end());
}
// Walk the batch headers of one segment file, noting producer tails only (the
// clean prefix below a pass's dirty range: its producers' last batches must be
// known too, or a prefix copy superseded by the dirty section would drop one).
void noteProducerTailsOfFile(const std::vector<uint8_t>& buf, Offset toOffset, ProducerTails& tails) {
    std::size_t pos = 0;
    while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= buf.size()) {
        rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
        const int32_t total = r.totalLength();
        if (total <= 0 || pos + static_cast<std::size_t>(total) > buf.size()) break;
        if (!r.ensureValid()) break;
        noteProducerTail(buf.data() + pos, r, toOffset, tails);
        pos += static_cast<std::size_t>(total);
    }
}
} // namespace

// ---------------------------------------------------------------------------
// buildOffsetMap  (APPEND-THREAD ONLY — derives bases from segments_)
// ---------------------------------------------------------------------------
void LogCleaner::buildOffsetMap(Log& log, Offset fromOffset, Offset toOffset,
                                OffsetMap& map) {
    // segmentBaseOffsets() reads the writer-owned segments_ map; only safe on the
    // single-writer append thread.  Delegate the pure-file scan to the shared core.
    buildOffsetMapFromBases(log.dir(), log.segmentBaseOffsets(),
                            fromOffset, toOffset, map);
}

// ---------------------------------------------------------------------------
// buildOffsetMapFromBases  (CP-7 BOUNDED overload — SkimpyOffsetMap)
// ---------------------------------------------------------------------------
Offset LogCleaner::buildOffsetMapFromBases(const std::string& dir,
                                           const std::vector<int64_t>& bases,
                                           Offset fromOffset, Offset toOffset,
                                           SkimpyOffsetMap& map) {
    prefixBytesRead_ = 0;
    if (fromOffset >= toOffset) return -1;

    for (size_t i = 0; i < bases.size(); ++i) {
        int64_t segBase = bases[i];
        int64_t segEnd  = (i + 1 < bases.size()) ? bases[i + 1] : toOffset;

        if (segBase >= toOffset)  break;
        if (segEnd <= fromOffset) {
            // Clean prefix: producer tails only (ProducerTails.h).
            const auto prefix = readFile(segmentLogPath(dir, segBase));
            prefixBytesRead_ += static_cast<int64_t>(prefix.size());   // M3: throttled by the caller
            noteProducerTailsOfFile(prefix, toOffset, map.producerTails());
            continue;
        }

        auto buf = readFile(segmentLogPath(dir, segBase));
        if (buf.empty()) continue;

        size_t pos = 0;
        while (pos + static_cast<size_t>(rune::protocol::batch::kRecordBatchOverhead)
               <= buf.size()) {
            rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
            int32_t total = r.totalLength();
            if (total <= 0 || pos + static_cast<size_t>(total) > buf.size()) break;
            if (!r.ensureValid()) break;
            noteProducerTail(buf.data() + pos, r, toOffset, map.producerTails());

            bool full = false;
            if (!isControlBatch(r)) r.forEachRecord([&](std::string_view key, std::string_view /*val*/,
                          int64_t offset, int64_t /*ts*/) {
                if (full) return;
                if (offset < fromOffset || offset >= toOffset) return;
                if (key.empty()) return; // null-key: not compactable
                if (!map.put(key, offset)) {
                    // Map is full — stop the pass.  latestOffset() holds the
                    // highest offset that made it into the map.
                    full = true;
                }
            });

            if (full) return map.latestOffset();
            pos += static_cast<size_t>(total);
        }
    }
    return map.latestOffset();
}

// ---------------------------------------------------------------------------
// buildOffsetMapFromBases  (CLEANER-SAFE — pure file work, no segments_)
// ---------------------------------------------------------------------------
void LogCleaner::buildOffsetMapFromBases(const std::string& dir,
                                         const std::vector<int64_t>& bases,
                                         Offset fromOffset, Offset toOffset,
                                         OffsetMap& map) {
    prefixBytesRead_ = 0;
    if (fromOffset >= toOffset) return;

    // Walk segments that overlap [fromOffset, toOffset).  `bases` and `dir` are
    // supplied explicitly (from the sealed-segment snapshot on the cleaner thread,
    // or from segmentBaseOffsets() on the append thread) so we never read segments_.

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

        // Skip segments entirely outside [fromOffset, toOffset) — except that a
        // clean-prefix segment's producer tails are still noted (ProducerTails.h).
        if (segBase >= toOffset) break;
        if (segEnd <= fromOffset) {
            const auto prefix = readFile(segmentLogPath(dir, segBase));
            prefixBytesRead_ += static_cast<int64_t>(prefix.size());   // M3: throttled by the caller
            noteProducerTailsOfFile(prefix, toOffset, map.producerTails());
            continue;
        }

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
            noteProducerTail(buf.data() + pos, r, toOffset, map.producerTails());

            if (!isControlBatch(r)) r.forEachRecord([&](std::string_view key, std::string_view /*val*/,
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
// rewriteSegmentImpl  (internal — type-erased core)
// ---------------------------------------------------------------------------
bool LogCleaner::rewriteSegmentImpl(
    const std::string& dir, int64_t segBase,
    const std::function<std::optional<Offset>(std::string_view)>& getLatest,
    Offset fromOffset, Offset cleanBoundaryOffset,
    int64_t nowNs, int64_t deleteRetentionMs,
    int64_t nowMs, bool* changed, const ProducerTails* tails)
{
    std::string logPath   = segmentLogPath(dir, segBase);
    std::string newPath   = logPath + ".new";
    std::string idxPath   = segmentIndexPath(dir, segBase);

    // Remove stale temp from a prior crashed compaction.
    std::filesystem::remove(newPath);

    // Read the current segment.
    auto buf = readFile(logPath);
    if (buf.empty()) return false;

    std::vector<uint8_t> outBuf;
    outBuf.reserve(buf.size());

    // CP-10 ULL: reusable single-batch build buffer — avoids one heap allocation
    // per kept record (the inner keepWithHorizon lambda now calls buildInto() which
    // clears and reuses this buffer instead of returning a new vector each time).
    // Typical single-record batch is ~100-200 B; pre-reserve 512 B covers most cases.
    std::vector<uint8_t> tmpBatch;
    tmpBatch.reserve(512);

    size_t pos = 0;
    int64_t deleteRetentionNs = deleteRetentionMs * 1'000'000LL;

    // Change-tracking: whether this pass actually altered the segment's logical
    // content (dropped a record, dropped/first-stamped a tombstone).  Re-encoding
    // multi-record batches into single-record batches is NOT a logical change, so
    // a byte-diff would spuriously flag every re-copy.  When `modified` stays
    // false the original .log is byte-valid and we skip the durable rewrite +
    // swap enqueue entirely (avoids churning already-clean prefix segments that
    // the Kafka-faithful from-log-start sweep re-visits every pass).
    bool modified = false;

    while (pos + static_cast<size_t>(rune::protocol::batch::kRecordBatchOverhead)
           <= buf.size()) {
        rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
        int32_t total = r.totalLength();
        // A3: bounds check before CRC; a corrupt totalLength must not misparse the walk.
        if (total <= 0 || pos + static_cast<size_t>(total) > buf.size()) break;
        // A3: CRC-gate the walk — stop on the first corrupt batch.
        if (!r.ensureValid()) break;

        // CP-8: KIP-534 delete-horizon gate.
        // CONTRACT: when hasDeleteHorizon() is true, baseTimestamp() IS the horizon
        // (epoch-ms), NOT a real batch timestamp.  NEVER treat baseTimestamp() as a
        // wall-clock timestamp on a horizon-stamped batch.
        //
        // If the batch has a delete-horizon stamp AND nowMs >= deleteHorizonMs,
        // all tombstones in this batch have expired their horizon and should be dropped
        // during the tombstone-check below (we signal this with horizonExpired=true).
        // If the batch does NOT have a horizon stamp, tombstone retention is by the
        // legacy nowNs path (backward compat).
        const bool batchHasHorizon = r.hasDeleteHorizon();
        const int64_t batchHorizonMs = batchHasHorizon ? r.deleteHorizonMs() : -1;
        const bool horizonExpired = (batchHasHorizon && nowMs >= 0
                                     && nowMs >= batchHorizonMs);

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
        const int64_t batchLastOffset = batchBaseOffset + r.lastOffsetDelta();
        const bool batchIsData = origProducerId >= 0 && origBaseSequence >= 0 &&
            (static_cast<uint16_t>(r.attributes()) & static_cast<uint16_t>(rune::protocol::batch::kAttrControl)) == 0;
        const int64_t batchMaxTs = static_cast<int64_t>(
            rune::protocol::detail::getLE64(buf.data() + pos + rune::protocol::batch::kMaxTimestampOffset));
        // C2 / I1: does this batch carry its producer's last metadata (Kafka
        // isBatchLastRecordOfProducer, unexpired)?  Then it is never removed
        // whole: kept records carry it, else a record-less batch does.
        const bool tailNeeded = batchIsData && tails != nullptr && batchLastOffset < cleanBoundaryOffset &&
            tails->mustRetain(origProducerId, batchLastOffset, batchMaxTs, nowMs, producerIdExpirationMs_);
        if (batchIsData && r.recordCount() == 0) {
            // A record-less producer-tail batch of an earlier pass: kept while
            // it is still the (unexpired) tail, dropped once superseded.
            if (batchLastOffset >= cleanBoundaryOffset || tailNeeded)
                outBuf.insert(outBuf.end(), buf.begin() + static_cast<std::ptrdiff_t>(pos),
                              buf.begin() + static_cast<std::ptrdiff_t>(pos + static_cast<size_t>(total)));
            else
                modified = true;
            pos += static_cast<size_t>(total);
            continue;
        }
        if (isControlBatch(r)) {   // D3: markers are copied verbatim, never key-compacted
            outBuf.insert(outBuf.end(), buf.begin() + static_cast<std::ptrdiff_t>(pos),
                          buf.begin() + static_cast<std::ptrdiff_t>(pos + static_cast<size_t>(total)));
            pos += static_cast<size_t>(total);
            continue;
        }
        const bool batchIsTxn = isTransactionalBatch(r);   // D3: rebuilt records keep the txn bit
        bool lastKept = false;   // the record at batchLastOffset was emitted

        // CP-8: does this batch contain any tombstone (for horizon stamping)?
        // We track this as we iterate records and then stamp the batch if needed.
        // Since we emit each record as its own 1-record batch, we stamp the
        // individual output batch (not the source batch as a whole).
        // horizonStampNeeded: for a tombstone that is the LATEST for its key and is
        // being RETAINED (not yet past horizon), if it doesn't have a horizon yet,
        // stamp one.  `nowMs + deleteRetentionMs` is the horizon epoch-ms.

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
        r.forEachRecord([&](std::string_view key, std::string_view val,
                      int64_t offset, int64_t timestamp) {
            // Helper: emit this record as its own single-record batch,
            // preserving the original batch's producer identity (A2).
            // @param horizonMs  If >= 0, stamp this as a delete-horizon batch.
            auto keepWithHorizon = [&](int64_t horizonMs) {
                rune::protocol::RecordBatchBuilder b(offset, timestamp);
                b.setTransactional(batchIsTxn);
                if (horizonMs >= 0) {
                    // CP-8: Stamp the output batch with the delete horizon.
                    // This sets kAttrDeleteHorizon and puts horizonMs into baseTimestamp.
                    b.setDeleteHorizonMs(horizonMs);
                }
                b.append(key, val, timestamp);
                // CP-10 ULL: use buildInto(tmpBatch) — reuses capacity, avoids per-record alloc.
                b.buildInto(tmpBatch);
                auto& batch = tmpBatch;

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
                if (offset == batchLastOffset) lastKept = true;
            };

            auto keep = [&]() { keepWithHorizon(-1); };

            // Records at/past the clean boundary (HW / passEnd) are always kept
            // verbatim.  For bounded-range passes, cleanBoundaryOffset == passEnd
            // which may be less than activeBase, so records beyond the pass boundary
            // are preserved for the next pass.
            if (offset >= cleanBoundaryOffset) { keep(); return; }

            // Null-key records: always keep (not compactable).
            if (key.empty()) { keep(); return; }


            // UNIFIED RETAIN RULE (Kafka LogCleaner.shouldRetainRecord).
            //
            // The dirty-section offset map holds the HIGHEST offset seen for each
            // key in [globalDirtyStart, activeBase).  A record is SUPERSEDED — and
            // therefore dropped — iff the map holds a strictly higher offset for
            // its key.  This rule is applied UNIFORMLY to:
            //   • the already-clean prefix (offset < fromOffset): a prefix copy
            //     whose key reappears later in the dirty section is dropped.  This
            //     is the cross-round CONVERGENCE FIX — Kafka re-copies segments
            //     from log-start [0, endOffset) against the dirty map on every
            //     round, so a key written before the cleaner watermark and
            //     rewritten after it never strands an old copy below the watermark
            //     (the drcp1/3/4 "key delivered seq 37 AND 197" DR failure).
            //   • the dirty range [fromOffset, cleanBoundaryOffset): a copy that is
            //     not the latest for its key is dropped.
            // A record whose key is ABSENT from the map (its only occurrence, or a
            // clean-prefix-only key not rewritten in the dirty section) is RETAINED
            // — never dropped (no data loss).  For a dirty-range record the map
            // always contains the key, so "absent" implies a clean-prefix-only key.
            auto latest = getLatest(key);
            if (latest.has_value() && offset < *latest) {
                modified = true;
                return; // superseded by a higher-offset copy in the dirty map — drop
            }

            // This is the latest.  If it's a tombstone, handle delete-horizon.
            if (isTombstone(val)) {
                // CP-8: KIP-534 delete-horizon stamping/dropping.
                //
                // Case A: batch already has a horizon stamp (from a prior clean pass).
                //   - If nowMs >= horizonMs (horizon expired): drop the tombstone.
                //   - Otherwise: keep the tombstone, preserve the existing horizon stamp.
                //
                // Case B: batch does NOT have a horizon stamp (first time we see this tombstone).
                //   - If nowMs is available: stamp a horizon (nowMs + deleteRetentionMs).
                //   - Otherwise: fall back to legacy nowNs retention.
                if (batchHasHorizon) {
                    if (horizonExpired) {
                        modified = true;
                        return; // horizon passed — fully remove key + tombstone (CP-8)
                    }
                    // Horizon not yet passed — keep with existing horizon stamp.
                    keepWithHorizon(batchHorizonMs);
                    return;
                }

                // No horizon stamp yet.
                if (nowMs >= 0) {
                    // CP-8: first time compacting this tombstone — stamp the horizon.
                    // The tombstone is RETAINED in this pass.  It will be dropped
                    // once nowMs >= (nowMs_at_stamp + deleteRetentionMs).
                    // Stamping a fresh horizon changes the segment's content.
                    int64_t horizonMs = nowMs + deleteRetentionMs;
                    modified = true;
                    keepWithHorizon(horizonMs);
                    return;
                }

                // Legacy nowNs-only path (nowMs not provided — backward compat).
                int64_t tombstoneTs = timestamp; // timestamp in the record (nanoseconds)
                if ((nowNs - tombstoneTs) > deleteRetentionNs) {
                    modified = true;
                    return; // past retention — fully remove key + tombstone
                }
            }

            // Keep this record (non-tombstone, or tombstone within retention).
            keep();
        });
        // C2: the producer's last record was dropped — keep its METADATA only.
        if (tailNeeded && !lastKept) {
            appendEmptyTailBatch(outBuf, tmpBatch, batchLastOffset, batchMaxTs, origProducerId, origProducerEpoch,
                                 origBaseSequence + static_cast<int32_t>(batchLastOffset - batchBaseOffset),
                                 batchIsTxn);
        }

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
        if (changed) *changed = true; // segment removed — content changed
        return true;
    }

    // Nothing was dropped or stamped this pass: the existing .log is byte-valid
    // (re-framing single-record batches is not a logical change).  Skip the
    // durable rewrite AND signal *changed=false so the caller does NOT enqueue a
    // swap for this segment.  This keeps the Kafka-faithful from-log-start sweep
    // from churning already-clean prefix segments on every pass.
    if (!modified) {
        if (changed) *changed = false;
        return true; // file present + valid, unchanged
    }
    if (changed) *changed = true;

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
    return true;
}

// ---------------------------------------------------------------------------
// rewriteSegment  (OffsetMap overload — delegates to impl)
// ---------------------------------------------------------------------------
bool LogCleaner::rewriteSegment(const std::string& dir, int64_t segBase, OffsetMap& map,
                                Offset fromOffset, Offset cleanBoundaryOffset,
                                int64_t nowNs, int64_t deleteRetentionMs,
                                int64_t nowMs) {
    return rewriteSegmentImpl(dir, segBase,
        [&map](std::string_view key) -> std::optional<Offset> { return map.get(key); },
        fromOffset, cleanBoundaryOffset, nowNs, deleteRetentionMs, nowMs, nullptr,
        &map.producerTails());
}

// ---------------------------------------------------------------------------
// rewriteSegment  (SkimpyOffsetMap overload — delegates to impl)
// ---------------------------------------------------------------------------
bool LogCleaner::rewriteSegment(const std::string& dir, int64_t segBase, SkimpyOffsetMap& map,
                                Offset fromOffset, Offset cleanBoundaryOffset,
                                int64_t nowNs, int64_t deleteRetentionMs,
                                int64_t nowMs) {
    return rewriteSegmentImpl(dir, segBase,
        [&map](std::string_view key) -> std::optional<Offset> { return map.get(key); },
        fromOffset, cleanBoundaryOffset, nowNs, deleteRetentionMs, nowMs, nullptr,
        &map.producerTails());
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
        rewriteSegment(dir, segBase, map, firstDirtyOffset, cleanBoundaryOffset, nowNs, deleteRetentionMs);
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
// produceCleanedFile  (CP-6 race-fix: CLEANER-THREAD path — PURE FILE WORK)
//
// No Log& parameter and no segments_ access whatsoever.  Everything comes from
// the caller-supplied immutable snapshot-derived `sealedBases` + `dir`.  The
// in-memory reload is deferred to the append thread (drainCleanerSwaps →
// reloadSegment); this function ONLY writes the compacted file to disk.
// ---------------------------------------------------------------------------
bool LogCleaner::produceCleanedFile(const std::string& dir,
                                    const std::vector<int64_t>& sealedBases,
                                    Offset segBase, Offset activeBase,
                                    Offset globalDirtyStart,
                                    int64_t nowNs, int64_t deleteRetentionMs) {
    if (sealedBases.empty()) return false;

    // Guard: segBase must be one of the sealed segments in the snapshot AND must
    // be strictly below the active/uncleanable boundary.  We NEVER read segments_
    // to validate this — the snapshot IS the source of truth for the cleaner.
    if (segBase >= activeBase) return false;
    bool found = false;
    for (int64_t b : sealedBases) { if (b == segBase) { found = true; break; } }
    if (!found) return false;

    // Clean up any stale .log.new temp files left by a previous crashed run.
    // Pure directory I/O — does not touch segments_.
    for (auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.path().extension() == ".new") {
            std::filesystem::remove(e.path());
        }
    }

    // Build the offset map GLOBALLY over the whole dirty range
    // [globalDirtyStart, activeBase) using the SNAPSHOT bases — reads segment
    // bytes straight from the .log files, never via segments_.  This is the
    // cross-segment latest-per-key correctness (CP-2) computed off-thread.
    if (globalDirtyStart < 0) globalDirtyStart = 0;
    OffsetMap map;
    buildOffsetMapFromBases(dir, sealedBases, globalDirtyStart, activeBase, map);

    // Rewrite exactly the ONE segment at segBase (pure file I/O: atomic write→
    // rename, or delete when emptied).  Passing the GLOBAL bounds makes the
    // per-record filter drop globally-superseded records.
    return rewriteSegment(dir, segBase, map, globalDirtyStart, activeBase,
                          nowNs, deleteRetentionMs);
}

// ---------------------------------------------------------------------------
// produceCleanedFileBounded  (CP-7: CLEANER-THREAD bounded path)
//
// Uses a SkimpyOffsetMap sized by dedupeBufferBytes for the dirty-range scan.
// Bounded-range-per-pass: the map scan stops when the map is full; the pass
// covers only [globalDirtyStart, passEnd) where passEnd = map.latestOffset()+1.
// The segment is rewritten with cleanBoundaryOffset = passEnd so records beyond
// passEnd are preserved verbatim for subsequent passes.
//
// Returns CleanPassResult { compacted, passEnd }.  The caller MUST advance the
// cleaner watermark to passEnd (not to activeBase) and store the checkpoint.
// ---------------------------------------------------------------------------
CleanPassResult LogCleaner::produceCleanedFileBounded(
    const std::string& dir,
    const std::vector<int64_t>& sealedBases,
    Offset segBase, Offset activeBase,
    Offset globalDirtyStart,
    int64_t nowNs, int64_t deleteRetentionMs,
    int64_t dedupeBufferBytes,
    Throttler* throttler,
    int64_t nowMs)
{
    CleanPassResult result{false, activeBase};

    if (sealedBases.empty()) return result;
    if (segBase >= activeBase) return result;
    bool found = false;
    for (int64_t b : sealedBases) { if (b == segBase) { found = true; break; } }
    if (!found) return result;

    // Clean up stale .log.new temp files.
    for (auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.path().extension() == ".new") {
            std::filesystem::remove(e.path());
        }
    }

    if (globalDirtyStart < 0) globalDirtyStart = 0;

    // Build the bounded SkimpyOffsetMap over [globalDirtyStart, activeBase).
    // The map scan stops when the map is full; latestOffset() is the boundary.
    std::size_t maxSlots = SkimpyOffsetMap::maxSlotsFromBudget(dedupeBufferBytes);
    SkimpyOffsetMap map(maxSlots > 0 ? maxSlots : 1);

    // CP-8: throttle the offset-map build (read phase).
    // We read each segment file during buildOffsetMapFromBases; accumulate bytes
    // and call maybeThrottle after the full map build.
    {
        // Read bytes can be estimated from sealedBases sizes (file reads).
        // For simplicity, throttle after the full map-build pass.
        buildOffsetMapFromBases(dir, sealedBases, globalDirtyStart, activeBase, map);
        if (throttler && prefixBytesRead_ > 0) throttler->maybeThrottle(prefixBytesRead_);   // M3
        if (throttler) {
            // Approximate read bytes: size of the segment file being processed.
            // For the cleaner-thread path we throttle after the map is built.
            // We use the segment file size as the read-bytes approximation.
            const std::string segPath = segmentLogPath(dir, segBase);
            std::error_code ec;
            auto sz = static_cast<int64_t>(
                std::filesystem::file_size(segPath, ec));
            if (!ec && sz > 0) throttler->maybeThrottle(sz);
        }
    }

    // passEnd: the first offset NOT covered by this pass.
    // If the map filled up: latestOffset() < activeBase − 1 → partial pass.
    // If the map covered everything: latestOffset() == the last-offset-before-activeBase.
    Offset latestMapped = map.latestOffset();
    if (latestMapped < 0) {
        // Map got nothing (no records in range) — nothing to compact.
        // passEnd stays as activeBase so the watermark advances.
        return result;
    }
    Offset passEnd = latestMapped + 1;  // next pass starts here
    // Clamp: passEnd can't exceed activeBase.
    if (passEnd > activeBase) passEnd = activeBase;
    result.passEnd = passEnd;

    // Only rewrite the segment if its base is within [globalDirtyStart, passEnd).
    // If segBase >= passEnd, the segment is entirely beyond this pass — skip.
    if (segBase >= passEnd) {
        // The segment is past the boundary of this pass.
        // Return passEnd so the caller advances the watermark correctly.
        return result;
    }

    // Rewrite the segment with cleanBoundaryOffset = passEnd (bounded pass).
    // Records at/past passEnd are preserved verbatim for future passes.
    //
    // CP-7 missing-file handling: if the segment .log was already deleted (by a
    // prior pass that emptied it), rewriteSegmentImpl returns false because
    // readFile() returns empty.  In that case the segment is already clean —
    // treat it as successfully compacted so Phase 5 enqueues the base and lets
    // drainCleanerSwaps / reloadSegment drop the entry from segments_.
    // This is the correct behaviour when the ring was full on a prior cycle:
    // the file was written/deleted but the enqueue failed, so we retry the
    // enqueue next cycle.  We do NOT advance the watermark without a successful
    // enqueue (preserves the CP-6 ring-full invariant).
    const bool wrote = rewriteSegment(dir, segBase, map, globalDirtyStart,
                                      passEnd, nowNs, deleteRetentionMs, nowMs);
    // CP-8: throttle the write phase.
    if (wrote && throttler) {
        const std::string logPath2 = segmentLogPath(dir, segBase);
        std::error_code ec2;
        auto wsz = static_cast<int64_t>(
            std::filesystem::file_size(logPath2, ec2));
        if (!ec2 && wsz > 0) throttler->maybeThrottle(wsz);
    }
    // Also treat a missing-file (wrote==false but file absent) as compacted so
    // Phase 5 will enqueue and drain it.  Check explicitly: if the .log is gone
    // the segment is already clean (either emptied on a prior pass, or never existed).
    const std::string logPath = segmentLogPath(dir, segBase);
    if (wrote || !std::filesystem::exists(logPath)) {
        result.compacted = true;
    }
    // else: wrote==false AND file still exists — genuine I/O error or missing segment;
    // leave compacted=false so Phase 4's !compacted path fires.
    return result;
}

// ---------------------------------------------------------------------------
// produceCleanedRangeBounded  (CONVERGENCE FIX: Kafka doClean-from-log-start)
//
// Build ONE bounded map over the dirty section [globalDirtyStart, activeBase),
// then re-copy EVERY sealed segment with base < passEnd against that map so a key
// rewritten in the dirty section supersedes its stale clean-prefix copy.  Only
// the segments whose content actually changed are reported in rewrittenBases.
// ---------------------------------------------------------------------------
RangeCleanResult LogCleaner::produceCleanedRangeBounded(
    const std::string& dir,
    const std::vector<int64_t>& sealedBases,
    const std::vector<int64_t>& sealedSizes,
    Offset activeBase,
    Offset globalDirtyStart,
    int64_t nowNs, int64_t deleteRetentionMs,
    SkimpyOffsetMap& map,
    bool useCoalescing,
    int64_t groupRollBytes,
    Throttler* throttler,
    int64_t nowMs)
{
    RangeCleanResult result;
    result.passEnd = activeBase;   // default: nothing dirty → advance to activeBase

    if (sealedBases.empty()) return result;
    if (globalDirtyStart < 0) globalDirtyStart = 0;

    // Clean up stale .log.new temp files left by a previous crashed run.
    for (auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.path().extension() == ".new") std::filesystem::remove(e.path());
    }

    // Fill the caller-owned dedupe map ONCE over the whole dirty section (clear()
    // reuses its backing array — no per-pass allocation).  The map holds the
    // HIGHEST offset per key in [globalDirtyStart, activeBase); it stops early (map
    // full) at latestOffset(), which becomes the pass boundary.  Building it once
    // (not per segment/group) is what lets several coalesced groups be processed in
    // one pass without a member's keys vanishing after it is deleted.
    map.clear();
    buildOffsetMapFromBases(dir, sealedBases, globalDirtyStart, activeBase, map);
    // M3: the clean-prefix reads for producer tails count against the I/O throttle.
    if (throttler && prefixBytesRead_ > 0) throttler->maybeThrottle(prefixBytesRead_);

    const Offset latestMapped = map.latestOffset();
    if (latestMapped < 0) {
        // No compactable (non-null-key) records in the dirty range.  Nothing to
        // rewrite; the caller advances the watermark to activeBase.
        return result; // compacted=false, passEnd=activeBase, rewrittenBases empty
    }

    Offset passEnd = latestMapped + 1;
    if (passEnd > activeBase) passEnd = activeBase;
    result.passEnd = passEnd;

    // The in-pass segments are those whose byte range overlaps [logStart, passEnd),
    // i.e. base < passEnd (sealedBases is sorted ascending).  Every one of them is
    // re-copied against the SAME dirty map, so a stale prefix copy whose key
    // reappears in the dirty section is dropped (the convergence fix); records at or
    // beyond passEnd are preserved verbatim for the next pass.
    std::vector<int64_t> inPassBases;
    std::vector<int64_t> inPassSizes;
    for (std::size_t i = 0; i < sealedBases.size(); ++i) {
        if (sealedBases[i] >= passEnd) break;
        inPassBases.push_back(sealedBases[i]);
        inPassSizes.push_back(i < sealedSizes.size() ? sealedSizes[i] : 0);
    }
    if (inPassBases.empty()) return result;

    if (useCoalescing && inPassBases.size() > 1) {
        // Kafka segment grouping: coalesce adjacent in-pass segments into one output
        // per group.  All members of a merged group need a swap enqueue (the front
        // is reloaded from the merged file; the rest are dropped once their .log is
        // gone).  Groups are formed only over the in-pass bases so we never merge a
        // segment that straddles/exceeds passEnd.
        const int64_t roll = (groupRollBytes > 0) ? groupRollBytes
                                                   : (64LL * 1024 * 1024);
        auto groups = groupSegmentsBySize(inPassBases, inPassSizes, roll);
        for (const auto& g : groups) {
            if (g.bases.empty()) continue;
            bool changed = false;
            mergeGroupWithMap(dir, g.bases, map, globalDirtyStart, passEnd,
                              nowNs, deleteRetentionMs, nowMs, throttler, &changed);
            if (changed) {
                result.compacted = true;
                for (int64_t b : g.bases) result.rewrittenBases.push_back(b);
            }
        }
        return result;
    }

    // Non-coalescing: rewrite each in-pass segment in place against the shared map.
    auto getLatest = [&map](std::string_view key) -> std::optional<Offset> {
        return map.get(key);
    };
    for (int64_t b : inPassBases) {
        bool changed = false;
        rewriteSegmentImpl(dir, b, getLatest, globalDirtyStart, passEnd,
                           nowNs, deleteRetentionMs, nowMs, &changed, &map.producerTails());

        // A segment needs a swap enqueue iff its on-disk content changed (records
        // dropped / tombstone stamped / emptied), OR its .log is now absent (emptied
        // on a prior pass whose enqueue was dropped when the ring was full — this
        // re-enqueue is idempotent: reloadSegment drops it and the next snapshot no
        // longer lists it, so it never loops).  Unchanged prefix segments are left on
        // disk and NOT enqueued — no churn (one exists() stat covers both branches).
        std::error_code ec;
        const bool present = std::filesystem::exists(segmentLogPath(dir, b), ec);
        if (changed || !present) {
            result.rewrittenBases.push_back(b);
            result.compacted = true;
        }

        // Background I/O accounting (never on the append hot path): throttle by the
        // segment's on-disk size.  One stat, only when a throttle is configured.
        if (throttler && present) {
            auto sz = static_cast<int64_t>(
                std::filesystem::file_size(segmentLogPath(dir, b), ec));
            if (!ec && sz > 0) throttler->maybeThrottle(sz);
        }
    }

    return result;
}

// ---------------------------------------------------------------------------
// groupSegmentsBySize  (CP-8: Kafka-faithful segment coalescing)
// ---------------------------------------------------------------------------
std::vector<SegmentGroup> LogCleaner::groupSegmentsBySize(
    const std::vector<int64_t>& sealedBases,
    const std::vector<int64_t>& sealedSizes,
    int64_t segmentRollBytes,
    int32_t maxIndexEntries)
{
    std::vector<SegmentGroup> groups;
    if (sealedBases.empty()) return groups;

    // Estimate index entries for a segment of `bytes` bytes:
    //   ~ ceil(bytes / kIndexIntervalBytes) entries.
    constexpr int64_t kIndexIntervalBytes = 4096;
    auto estimateIndexEntries = [&](int64_t bytes) -> int64_t {
        if (bytes <= 0) return 0;
        return (bytes + kIndexIntervalBytes - 1) / kIndexIntervalBytes;
    };

    SegmentGroup cur;
    cur.bases.push_back(sealedBases[0]);
    cur.combinedBytes = sealedSizes[0];
    int64_t curIndexEntries = estimateIndexEntries(sealedSizes[0]);
    // Offset span: last-offset-in-group - first-offset-in-group.
    // first offset of a segment = its baseOffset.
    // last offset of a segment ≈ next segment's base - 1 (we use the base as span proxy).
    int64_t groupFirstBase = sealedBases[0];

    for (std::size_t i = 1; i < sealedBases.size(); ++i) {
        int64_t nextBase  = sealedBases[i];
        int64_t nextSize  = sealedSizes[i];
        int64_t addedIdx  = estimateIndexEntries(nextSize);
        int64_t newBytes  = cur.combinedBytes + nextSize;
        int64_t newIdx    = curIndexEntries + addedIdx;
        // Offset span = (next segment's last offset - groupFirstBase).
        // We approximate with nextBase - groupFirstBase (the span of base offsets).
        // The actual span can be slightly larger but this is conservative enough.
        int64_t offsetSpan = nextBase - groupFirstBase;
        // Check all three coalescing bounds:
        bool fitsBytes  = (newBytes <= segmentRollBytes);
        bool fitsIndex  = (newIdx   <= static_cast<int64_t>(maxIndexEntries));
        bool fitsOffset = (offsetSpan <= static_cast<int64_t>(std::numeric_limits<int32_t>::max()));

        if (fitsBytes && fitsIndex && fitsOffset) {
            cur.bases.push_back(nextBase);
            cur.combinedBytes = newBytes;
            curIndexEntries   = newIdx;
        } else {
            // Start a new group.
            groups.push_back(std::move(cur));
            cur = SegmentGroup{};
            cur.bases.push_back(nextBase);
            cur.combinedBytes = nextSize;
            curIndexEntries   = addedIdx;
            groupFirstBase    = nextBase;
        }
    }
    groups.push_back(std::move(cur));
    return groups;
}

// ---------------------------------------------------------------------------
// produceCleanedGroupBounded  (CP-8: coalesced group rewrite)
//
// Compacts an entire SegmentGroup into ONE output segment at group.bases[0].
// The SkimpyOffsetMap is built over [globalDirtyStart, activeBase) (same as
// the single-segment path).  All input segments in the group are merged into
// a single cleaned output file at the group's lowest base offset.
// Non-group segments (those in sealedBases but NOT in the group) are not
// touched.  After the write, all group segments EXCEPT the first are deleted
// on disk (they've been absorbed into the first).
// The caller must enqueue ALL group.bases via enqueueCleanedSegment — the
// append thread's drainCleanerSwaps → reloadSegment will:
//   (a) reload group.bases[0] from the new merged .log
//   (b) drop group.bases[1..] since their .log files no longer exist.
// ---------------------------------------------------------------------------
// mergeGroupWithMap — coalesce the in-pass segments of ONE group into the group's
// lowest base, filtering every record against the PREBUILT dirty `map`.  The map
// is built once by the caller over the whole dirty range, so cross-segment (and
// cross-prefix) supersession is honoured even when several groups are processed in
// the same pass (a per-group rebuild would miss keys in already-deleted members).
//
// Sets *changed=true iff the group's on-disk layout changed — records were dropped,
// a tombstone was first-stamped, or >1 member was merged into one.  When a group
// has a single member and nothing was dropped/stamped, the original .log is left
// untouched (byte-valid) and *changed=false, so the caller does not enqueue it.
// PURE FILE WORK — never touches Log::segments_.
void LogCleaner::mergeGroupWithMap(
    const std::string& dir,
    const std::vector<int64_t>& groupInPass,
    SkimpyOffsetMap& map,
    Offset globalDirtyStart, Offset passEnd,
    int64_t nowNs, int64_t deleteRetentionMs, int64_t nowMs,
    Throttler* throttler, bool* changed)
{
    if (changed) *changed = false;
    if (groupInPass.empty()) return;

    const int64_t outBase = groupInPass.front();
    const std::string outLogPath = segmentLogPath(dir, outBase);
    const std::string outNewPath = outLogPath + ".new";
    const std::string outIdxPath = segmentIndexPath(dir, outBase);
    std::filesystem::remove(outNewPath);

    // A >1-member group is always a layout change (members merged into the front).
    bool modified = (groupInPass.size() > 1);
    const int64_t deleteRetentionNs = deleteRetentionMs * 1'000'000LL;

    std::vector<uint8_t> outBuf;
    // CP-10 ULL: reusable single-batch build buffer (buildInto reuses capacity) —
    // avoids one heap allocation per kept record in the coalescing path.
    std::vector<uint8_t> tmpBatch;
    tmpBatch.reserve(512);
    for (int64_t segBase : groupInPass) {
        auto buf = readFile(segmentLogPath(dir, segBase));
        if (buf.empty()) continue;
        outBuf.reserve(outBuf.size() + buf.size());

        std::size_t pos = 0;
        while (pos + static_cast<size_t>(rune::protocol::batch::kRecordBatchOverhead)
               <= buf.size()) {
            rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
            int32_t total = r.totalLength();
            if (total <= 0 || pos + static_cast<size_t>(total) > buf.size()) break;
            if (!r.ensureValid()) break;

            // CP-8: horizon gate (same logic as rewriteSegmentImpl).
            const bool batchHasHorizon  = r.hasDeleteHorizon();
            const int64_t batchHorizonMs = batchHasHorizon ? r.deleteHorizonMs() : -1;
            const bool horizonExpired   = (batchHasHorizon && nowMs >= 0
                                           && nowMs >= batchHorizonMs);

            int64_t origProducerId    = r.producerId();
            int16_t origProducerEpoch = r.producerEpoch();
            int32_t origBaseSequence  = static_cast<int32_t>(
                rune::protocol::detail::getLE32(buf.data() + pos
                    + rune::protocol::batch::kBaseSequenceOffset));
            int64_t batchBaseOffset   = r.baseOffset();
            const int64_t batchLastOffset = batchBaseOffset + r.lastOffsetDelta();
            const bool batchIsData = origProducerId >= 0 && origBaseSequence >= 0 &&
                (static_cast<uint16_t>(r.attributes()) & static_cast<uint16_t>(rune::protocol::batch::kAttrControl)) == 0;
            const int64_t batchMaxTs = static_cast<int64_t>(
                rune::protocol::detail::getLE64(buf.data() + pos + rune::protocol::batch::kMaxTimestampOffset));
            // C2 / I1 (see rewriteSegmentImpl): producer-tail metadata retention.
            const bool tailNeeded = batchIsData && batchLastOffset < passEnd &&
                map.producerTails().mustRetain(origProducerId, batchLastOffset, batchMaxTs, nowMs,
                                               producerIdExpirationMs_);
            if (batchIsData && r.recordCount() == 0) {
                if (batchLastOffset >= passEnd || tailNeeded)
                    outBuf.insert(outBuf.end(), buf.begin() + static_cast<std::ptrdiff_t>(pos),
                                  buf.begin() + static_cast<std::ptrdiff_t>(pos + static_cast<size_t>(total)));
                else
                    modified = true;
                pos += static_cast<size_t>(total);
                continue;
            }
            if (isControlBatch(r)) {   // D3: markers are copied verbatim, never key-compacted
                outBuf.insert(outBuf.end(), buf.begin() + static_cast<std::ptrdiff_t>(pos),
                              buf.begin() + static_cast<std::ptrdiff_t>(pos + static_cast<size_t>(total)));
                pos += static_cast<size_t>(total);
                continue;
            }
            const bool batchIsTxn = isTransactionalBatch(r);   // D3: rebuilt records keep the txn bit
            bool lastKept = false;

            r.forEachRecord([&](std::string_view key, std::string_view val,
                          int64_t offset, int64_t timestamp) {
                auto keepWithHorizon2 = [&](int64_t horizonMs2) {
                    rune::protocol::RecordBatchBuilder b2(offset, timestamp);
                    b2.setTransactional(batchIsTxn);
                    if (horizonMs2 >= 0) b2.setDeleteHorizonMs(horizonMs2);
                    b2.append(key, val, timestamp);
                    b2.buildInto(tmpBatch);
                    auto& batch = tmpBatch;
                    // A2: Patch producer identity.
                    {
                        uint64_t u = static_cast<uint64_t>(origProducerId);
                        for (int ii = 0; ii < 8; ++ii)
                            batch[rune::protocol::batch::kProducerIdOffset + ii] =
                                static_cast<uint8_t>(u >> (ii * 8));
                    }
                    {
                        uint16_t u = static_cast<uint16_t>(origProducerEpoch);
                        batch[rune::protocol::batch::kProducerEpochOffset]     = static_cast<uint8_t>(u);
                        batch[rune::protocol::batch::kProducerEpochOffset + 1] = static_cast<uint8_t>(u >> 8);
                    }
                    {
                        int32_t recSeq = origBaseSequence
                            + static_cast<int32_t>(offset - batchBaseOffset);
                        uint32_t u = static_cast<uint32_t>(recSeq);
                        for (int ii = 0; ii < 4; ++ii)
                            batch[rune::protocol::batch::kBaseSequenceOffset + ii] =
                                static_cast<uint8_t>(u >> (ii * 8));
                    }
                    uint32_t newCrc = rune::protocol::crc32c(
                        batch.data() + rune::protocol::batch::kAttributesOffset,
                        batch.size() - rune::protocol::batch::kAttributesOffset);
                    batch[rune::protocol::batch::kCrcOffset]     = static_cast<uint8_t>(newCrc);
                    batch[rune::protocol::batch::kCrcOffset + 1] = static_cast<uint8_t>(newCrc >> 8);
                    batch[rune::protocol::batch::kCrcOffset + 2] = static_cast<uint8_t>(newCrc >> 16);
                    batch[rune::protocol::batch::kCrcOffset + 3] = static_cast<uint8_t>(newCrc >> 24);
                    outBuf.insert(outBuf.end(), batch.begin(), batch.end());
                    if (offset == batchLastOffset) lastKept = true;
                };
                auto keep2 = [&]() { keepWithHorizon2(-1); };

                // Records at/past the pass boundary are kept verbatim.
                if (offset >= passEnd) { keep2(); return; }
                if (key.empty())       { keep2(); return; }

                // UNIFIED RETAIN RULE (see rewriteSegmentImpl): drop iff the dirty
                // map holds a strictly higher offset for this key; retain records
                // whose key is absent from the map (clean-prefix-only keys).  This
                // re-cleans the clean prefix [logStart, globalDirtyStart) against
                // the dirty section — the cross-round convergence fix.
                auto latest = map.get(key);
                if (latest.has_value() && offset < *latest) { modified = true; return; }

                if (isTombstone(val)) {
                    if (batchHasHorizon) {
                        if (horizonExpired) { modified = true; return; }
                        keepWithHorizon2(batchHorizonMs); return;
                    }
                    if (nowMs >= 0) {
                        modified = true;
                        keepWithHorizon2(nowMs + deleteRetentionMs); return;
                    }
                    if ((nowNs - timestamp) > deleteRetentionNs) { modified = true; return; }
                }
                keep2();
            });
            if (tailNeeded && !lastKept) {   // C2: metadata only
                appendEmptyTailBatch(outBuf, tmpBatch, batchLastOffset, batchMaxTs, origProducerId,
                                     origProducerEpoch,
                                     origBaseSequence + static_cast<int32_t>(batchLastOffset - batchBaseOffset),
                                 batchIsTxn);
            }

            pos += static_cast<size_t>(total);
        }
    }
    (void)globalDirtyStart; // dirty range is captured by the map bounds

    // Single-member group with no drops/stamps: leave the original .log untouched
    // (re-framing alone is not a logical change) — no rewrite, no enqueue.
    if (!modified) return;
    if (changed) *changed = true;

    if (outBuf.empty()) {
        // All records removed — delete every member segment.
        for (int64_t b : groupInPass) {
            std::filesystem::remove(segmentLogPath(dir, b));
            std::filesystem::remove(segmentIndexPath(dir, b));
        }
        fsyncDir(dir);
        return;
    }

    // Durable write of the merged output to the group's lowest base, then delete
    // the absorbed members.
    writeFileDurable(outNewPath, outBuf);
    std::filesystem::rename(outNewPath, outLogPath);
    fsyncDir(dir);
    std::filesystem::remove(outIdxPath);
    buildIndex(outIdxPath, outBase, outBuf);
    for (std::size_t gi = 1; gi < groupInPass.size(); ++gi) {
        std::filesystem::remove(segmentLogPath(dir, groupInPass[gi]));
        std::filesystem::remove(segmentIndexPath(dir, groupInPass[gi]));
    }
    fsyncDir(dir);

    if (throttler) throttler->maybeThrottle(static_cast<int64_t>(outBuf.size()));
}

CleanPassResult LogCleaner::produceCleanedGroupBounded(
    const std::string& dir,
    const std::vector<int64_t>& sealedBases,
    const SegmentGroup& group,
    Offset activeBase,
    Offset globalDirtyStart,
    int64_t nowNs, int64_t deleteRetentionMs,
    int64_t dedupeBufferBytes,
    Throttler* throttler,
    int64_t nowMs)
{
    CleanPassResult result{false, activeBase};
    if (group.bases.empty()) return result;

    // All bases in the group must be < activeBase.
    for (int64_t b : group.bases) {
        if (b >= activeBase) return result;
    }

    // Clean up stale .log.new temp files.
    for (auto& e : std::filesystem::directory_iterator(dir)) {
        if (e.path().extension() == ".new") {
            std::filesystem::remove(e.path());
        }
    }

    if (globalDirtyStart < 0) globalDirtyStart = 0;

    // Build offset map over the full dirty range.
    std::size_t maxSlots = SkimpyOffsetMap::maxSlotsFromBudget(dedupeBufferBytes);
    SkimpyOffsetMap map(maxSlots > 0 ? maxSlots : 1);
    buildOffsetMapFromBases(dir, sealedBases, globalDirtyStart, activeBase, map);
    if (throttler && prefixBytesRead_ > 0) throttler->maybeThrottle(prefixBytesRead_);   // M3

    // Throttle the map-build read phase (approx from combined group size).
    if (throttler && group.combinedBytes > 0) {
        throttler->maybeThrottle(group.combinedBytes);
    }

    Offset latestMapped = map.latestOffset();
    if (latestMapped < 0) return result; // nothing to compact

    Offset passEnd = latestMapped + 1;
    if (passEnd > activeBase) passEnd = activeBase;
    result.passEnd = passEnd;

    // Determine which segments in the group fall within [globalDirtyStart, passEnd).
    std::vector<int64_t> groupInPass;
    for (int64_t b : group.bases) {
        if (b < passEnd) groupInPass.push_back(b);
    }
    if (groupInPass.empty()) return result;

    bool changed = false;
    mergeGroupWithMap(dir, groupInPass, map, globalDirtyStart, passEnd,
                      nowNs, deleteRetentionMs, nowMs, throttler, &changed);
    result.compacted = changed;
    return result;
}

// ---------------------------------------------------------------------------
// cleanSegment  (CP-2 APPEND-THREAD path: produce file + reload in place)
//
// This overload IS allowed to touch segments_ because it runs on the single-
// writer append/duty-cycle thread (NodeRuntimeAgent::runBoundedCompaction and
// synchronous tests).  It derives bases from segmentBaseOffsets(), produces the
// cleaned file via the shared pure-file core, then reloads the segment in place.
// The dedicated off-thread cleaner (LogCleanerAgent) must NOT call this — it
// calls produceCleanedFile() and defers reloadSegment() to drainCleanerSwaps().
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
    (void)segEnd; // retained for API symmetry / future byte-budgeted variants

    // Produce the compacted file off the map — same pure-file core the cleaner
    // thread uses.  `bases` here is the append-thread's live view (safe on this
    // thread); the dirty range upper bound is the active-segment base.
    const bool produced = produceCleanedFile(log.dir(), bases, segBase,
                                             /*activeBase=*/activeBase,
                                             globalDirtyStart, nowNs,
                                             deleteRetentionMs);
    if (!produced) return false;

    // Reload ONLY this segment in place — O(1) in the number of segments.  This
    // is a segments_ mutation, but we are on the single-writer append thread.
    log.reloadSegment(segBase);
    return true;
}

} // namespace rune::storage
