#pragma once
// ---------------------------------------------------------------------------
// FileRecordingStore.h — File-backed RecordingStore.
//
// Namespace: rune::replication
// C++23. No locks. No busy-spin. Single-writer.
//
// ## Format
//
// Each batch is framed as:
//   [uint64_t length   (8 bytes, LE)] — byte count of the batch payload
//   [int64_t  baseOffset (8 bytes, LE)] — first offset in this batch
//   [uint8_t  bytes[length]           ] — raw batch payload
//
// The file is append-only.  On open we scan to find the last complete frame
// (torn-tail recovery) and reset the write pointer there.
//
// ## Durability
//
// append() writes atomically to the file (no partial frame visible on disk
// after a crash if the OS delivers a torn write after the length+offset header
// because we detect this in scan/recover).  We do NOT fsync per-append — same
// trade-off as the Log (majority-in-memory durability; Archive is async).
// ---------------------------------------------------------------------------

#include <rune/replication/RecordingStore.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>
#include <unistd.h>

namespace rune::replication {

// ---------------------------------------------------------------------------
// FileRecordingStore
// ---------------------------------------------------------------------------
class FileRecordingStore : public RecordingStore {
public:
    // Header preceding each batch in the file.
    struct BatchHeader {
        uint64_t length;      ///< payload byte count
        int64_t  baseOffset;  ///< first offset in this batch
    };
    static constexpr std::size_t kHeaderSize = sizeof(BatchHeader);

    // -----------------------------------------------------------------------
    // Construction / destruction
    // -----------------------------------------------------------------------

    /// Open (or create) a recording store at the given path.
    ///
    /// If the file exists it is reopened and the tail is scanned to recover
    /// the last complete batch position.  A torn tail (incomplete frame) is
    /// silently discarded — the file is physically truncated to the last
    /// complete frame boundary.
    explicit FileRecordingStore(const std::string& path);

    ~FileRecordingStore() override;


    // Non-copyable, movable.
    FileRecordingStore(const FileRecordingStore&)            = delete;
    FileRecordingStore& operator=(const FileRecordingStore&) = delete;
    FileRecordingStore(FileRecordingStore&&)                 = default;
    FileRecordingStore& operator=(FileRecordingStore&&)      = default;

    // -----------------------------------------------------------------------
    // RecordingStore interface
    // -----------------------------------------------------------------------

    /// Append a batch.  Thread-safety: single-writer assumed.
    void append(int64_t baseOffset, std::span<const uint8_t> bytes) override;

    /// Last recorded batch's end position (exclusive).
    /// = baseOffset-of-last-batch + 1 if we model it as "next expected offset".
    ///
    /// Actually: after append(baseOffset, bytes), recordingPosition() ==
    /// baseOffset + 1  (i.e. we track the OFFSET count, not byte position).
    /// This mirrors Aeron Archive's getRecordingPosition() semantics.
    [[nodiscard]] int64_t recordingPosition() const noexcept override {
        return recordingPosition_;
    }

    /// Scan batches in order, filtered by [fromOffset, toOffset).
    void scan(int64_t fromOffset, int64_t toOffset,
              const BatchSink& sink) const override;

    /// Number of batches in the store.
    [[nodiscard]] int64_t batchCount() const noexcept override {
        return batchCount_;
    }

    /// Path to the underlying file.
    [[nodiscard]] const std::string& path() const noexcept { return path_; }

private:
    // -----------------------------------------------------------------------
    // Recovery helper: scan the file, find last complete frame, return
    // the byte offset of the end of the last complete frame (or 0 if none).
    // Also populates batchCount_ and recordingPosition_.
    // -----------------------------------------------------------------------
    int64_t recoverFromFile();

    // -----------------------------------------------------------------------
    // Write helpers
    // -----------------------------------------------------------------------
    void writeLE64(uint64_t v);
    void writeLE64s(int64_t v);

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------
    std::string   path_;
    mutable std::fstream file_;          ///< read+write, binary
    int           fd_ = -1;             ///< POSIX fd for fsync (A-NEW-3)
    int64_t recordingPosition_ = 0;      ///< next expected offset (exclusive)
    int64_t batchCount_        = 0;
};

} // namespace rune::replication
