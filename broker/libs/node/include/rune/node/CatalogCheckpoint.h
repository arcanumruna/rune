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

} // namespace rune::node
