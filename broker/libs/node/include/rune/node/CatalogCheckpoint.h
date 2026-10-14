#pragma once
// CatalogCheckpoint.h — the applied-offset checkpoint of `__catalog` (catalog
// history ruling 8).
//
// There is no persisted high watermark.  What survives a restart is
// `dataDir/__catalog/applied` = (last applied offset, CatalogState digest),
// written by the CatalogApplier BEFORE it posts any side effect of the event at
// that offset.  Boot replays exactly [0, appliedOffset] (always committed: it was
// applied, so it was below the HW) and catches the rest up live.  Hence:
//   * a crash between the checkpoint and the effects re-runs those effects
//     through the BOOT sink (recovery installs), never through the live one a
//     second time — no event is applied twice;
//   * a crash before the checkpoint re-applies the event live — none is skipped.
//
// File layout (little-endian, 32 bytes, fixed):
//   magic "RCATAPL1" (8) | appliedOffset i64 | digest u64 | check u64
// where `check` is FNV-1a-64 over the 24 bytes before it.  Written as
// tmp + fsync + rename + directory fsync, so a reader sees the old or the new
// checkpoint, never a torn one.  A file that is present but does not verify is
// local damage: readCatalogCheckpoint THROWS (it never answers "none", which would
// re-apply the whole history live).
//
// Owned by the admin thread (the CatalogApplier) — and read once at boot.

#include <cstdint>
#include <optional>
#include <string>

namespace rune::node {

struct CatalogCheckpoint {
    int64_t  appliedOffset = -1;
    uint64_t digest        = 0;
    bool operator==(const CatalogCheckpoint&) const = default;
};

/// The checkpoint file's name inside the catalog directory.
inline constexpr const char* kCatalogCheckpointFile = "applied";

/// nullopt = none (no file).  Throws std::runtime_error (naming the path) on an
/// unreadable, short, foreign or corrupt file.
[[nodiscard]] std::optional<CatalogCheckpoint> readCatalogCheckpoint(const std::string& catalogDir);

/// tmp + fsync + rename + directory fsync.  false = not durable (errno is left
/// set by the failing call); the previous checkpoint, if any, is intact.
[[nodiscard]] bool writeCatalogCheckpoint(const std::string& catalogDir, const CatalogCheckpoint& c);

/// Review M8 (Task 6 carry-over): a checkpoint at or past the local log end means
/// the log lost events this broker already applied (and effected) — replaying the
/// shorter log would silently fork this broker's catalog.  nullopt = consistent;
/// otherwise the explicit refusal message.  `logEndOffset` = the recovered LEO.
[[nodiscard]] std::optional<std::string> catalogCheckpointBeyondLog(const std::optional<CatalogCheckpoint>& c,
                                                                    int64_t logEndOffset,
                                                                    const std::string& catalogDir);

// ─── The catalog HISTORY EPOCH (Task 13 fix round 2) ────────────────────────────
// `dataDir/__catalog/history_epoch` — what a catalog reader's cache is checked
// against (CatalogReadServer: every catalog stream opens with an epoch note and
// every caught-up marker carries it).  The epoch names one committed history of
// THIS broker's `__catalog`: it changes on every truncation below a served high
// watermark, and a fresh one (random 64-bit start, so two brokers never share a
// value) is drawn whenever the file is absent or damaged — a client whose cache
// carries another value re-reads from 0.
//
// Persisted with what was served under it: `servedFloor` (every offset below it
// may have been served under `epoch`) and the ANCHOR — the batch holding
// servedFloor-1 (base offset, partition leader epoch, CRC).  Boot re-reads that
// batch: a log that lost it or holds a different one (lost unsynced bytes, a
// truncate-and-regrow the read server never saw before a crash) bumps the epoch.
//
// File layout (little-endian, 48 bytes, fixed):
//   magic "RCATHEP1" (8) | epoch u64 | servedFloor i64 | anchorBase i64 |
//   anchorLeaderEpoch i32 | anchorCrc u32 | check u64 (FNV-1a-64 of the 40 before)
// Written like the checkpoint (tmp + fsync + rename + directory fsync).  Owned by
// the admin thread (the CatalogReadServer).
//
// OPERATIONS (fix round 3): restoring a data dir from a backup, or cloning it to
// another broker, MUST delete `__catalog/history_epoch` — a self-consistent
// rollback (log and file restored together) cannot be detected at boot, and the
// restored broker would serve another history under an epoch clients cached.
// A missing file only costs every client one re-read.  Every change of the epoch
// is a fresh random draw (never +1), so lineages that once shared a value part.
struct CatalogHistoryEpoch {
    uint64_t epoch             = 0;
    int64_t  servedFloor       = 0;
    int64_t  anchorBase        = -1;   // -1: no anchor (servedFloor 0)
    int32_t  anchorLeaderEpoch = -1;
    uint32_t anchorCrc         = 0;
    bool operator==(const CatalogHistoryEpoch&) const = default;
};

inline constexpr const char* kCatalogHistoryEpochFile = "history_epoch";

/// nullopt = absent, short, foreign or corrupt — the caller starts a fresh epoch
/// (always safe: every cached prefix is re-read).  `path` = dir/history_epoch.
[[nodiscard]] std::optional<CatalogHistoryEpoch> readCatalogHistoryEpoch(const std::string& path) noexcept;

/// tmp + fsync + rename + directory fsync over PRECOMPUTED paths (no allocation:
/// the read server writes it on its pass).  false = not durable (errno set); the
/// previous file, if any, is intact.
[[nodiscard]] bool writeCatalogHistoryEpoch(const std::string& path, const std::string& tmpPath,
                                            const std::string& dir, const CatalogHistoryEpoch& e) noexcept;

} // namespace rune::node
