#pragma once
// ---------------------------------------------------------------------------
// CleanerOffsetCheckpoint — CP-7: Persisted cleaner watermark.
//
// Stores a SINGLE int64_t value (the first-dirty-offset = end of last cleaned
// range) in a mmap-backed file so that `lastCleanOffset` survives broker
// restarts.  Uses MappedFile for the mmap backing.
//
// ## Format (file size = 16 bytes, fixed)
//   Bytes [0..7]  : int64_t value (little-endian) — the checkpoint offset
//   Bytes [8..15] : uint64_t magic/version (kMagic) — integrity sentinel
//
// The magic is checked on load; if absent or mismatched the file is treated
// as uninitialised and the checkpoint returns 0 (start from the beginning).
//
// ## Threading / single-writer discipline
// The checkpoint is WRITTEN on the CLEANER THREAD (in LogCleanerAgent::doWork
// after a successful compaction pass).  This is safe because:
//   (a) The checkpoint mmap file is ONLY touched by the cleaner — no other
//       thread reads or writes the file after initial construction.
//   (b) The cleaner is a single dedicated agent (or at most N cleaners with
//       per-log claims, so only one writer per log at a time).
//   (c) The checkpoint stores ONLY the lastCleanOffset, which is advanced
//       monotonically on the cleaner thread.  It does NOT touch any field
//       owned by the append thread (the in-memory segment map, logEndOffset,
//       or any other append-thread-owned state).
// The read (load) happens in ensureCheckpoint(), also on the cleaner thread
// (first compaction pass for this log after agent construction / restart).
// The INITIAL construction (and its mmap open) may happen on any thread but
// is protected by the checkpoints_ map's per-log lazy-init guard.
// NOTE: the old comment "append/drain thread ONLY" was incorrect — corrected
// in CP-7 to reflect actual ownership (cleaner thread).  The safety argument
// above explains why cleaner-thread ownership is correct.
//
// ## Truncation-awareness
// On load, the checkpoint is CLAMPED to `logEndOffset` so a truncated log
// (failover scenario) never skips live data: if the log was truncated below
// the checkpoint, the checkpoint is lowered to the log's end.
// ---------------------------------------------------------------------------

#include <rune/storage/MappedFile.h>
#include <cstdint>
#include <cstring>
#include <string>

namespace rune::storage {

class CleanerOffsetCheckpoint {
public:
    static constexpr uint64_t kMagic     = 0x52554E455F435037ULL; // "RUNE_CP7"
    static constexpr size_t   kFileSize  = 16; // 8 bytes offset + 8 bytes magic

    /// Open (or create) the checkpoint file at `path`.
    /// @param logEndOffset  Current log end offset (for truncation-aware load).
    CleanerOffsetCheckpoint(std::string path, int64_t logEndOffset)
        : file_(path, kFileSize, /*writable=*/true)
    {
        // Ensure the backing file is at least kFileSize bytes.
        if (file_.size() < kFileSize) {
            file_.resize(kFileSize);
        }
        // Load and validate.
        load(logEndOffset);
    }

    /// Return the persisted first-dirty-offset (0 if uninitialised or invalid).
    [[nodiscard]] int64_t value() const noexcept { return value_; }

    /// Persist a new checkpoint offset.
    /// Called on the CLEANER THREAD after a successful compaction pass.
    /// Writes the value + magic, then msync()s.
    /// Thread-safety: only one cleaner agent writes per log at a time (per-log
    /// inProgress claim); the file is not shared with the append thread.
    void store(int64_t offset) noexcept {
        value_ = offset;
        uint8_t* d = file_.data();
        // Write offset as little-endian int64.
        uint64_t u = static_cast<uint64_t>(offset);
        for (int i = 0; i < 8; ++i) d[i] = static_cast<uint8_t>(u >> (i * 8));
        // Write magic.
        u = kMagic;
        for (int i = 0; i < 8; ++i) d[8 + i] = static_cast<uint8_t>(u >> (i * 8));
        file_.sync();
    }

private:
    void load(int64_t logEndOffset) noexcept {
        const uint8_t* d = file_.data();
        if (file_.size() < kFileSize) { value_ = 0; return; }

        // Read magic.
        uint64_t magic = 0;
        for (int i = 0; i < 8; ++i) magic |= static_cast<uint64_t>(d[8 + i]) << (i * 8);
        if (magic != kMagic) { value_ = 0; return; } // uninitialised file

        // Read offset (LE int64).
        uint64_t u = 0;
        for (int i = 0; i < 8; ++i) u |= static_cast<uint64_t>(d[i]) << (i * 8);
        int64_t saved = static_cast<int64_t>(u);

        // Truncation-aware: clamp to [0, logEndOffset].
        if (saved < 0)            saved = 0;
        if (saved > logEndOffset) saved = logEndOffset;
        value_ = saved;
    }

    MappedFile file_;
    int64_t    value_ = 0;
};

} // namespace rune::storage
