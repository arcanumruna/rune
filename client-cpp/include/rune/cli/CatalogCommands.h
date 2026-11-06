// include/rune/cli/CatalogCommands.h
//
// Catalog history Task 15 — `rune ql` and `rune catalog export | replay | diff`.
//
// The pure helpers below are unit-tested offline; the commands (cmdQl / cmdCatalog)
// are dispatched from rune::cli::run() (Cli.cpp).
//
// Exit codes: 0 success, 1 an explicit error (a message on stderr), 2 a usage error,
// 3 only `catalog diff` when the two catalogs differ.
#pragma once

#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogQueries.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/CatalogState.h>

#ifdef RUNE_CLI_LIVE_ENABLED
#include <rune/client/Client.h>
#endif

#include <cstdint>
#include <istream>
#include <optional>
#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace rune::cli {

/// F18: one history row — `offset  time(UTC)  kind  object(objectId)  version prev→new  principal
/// replayedFrom  statement`, columns separated by two spaces.  replayedFrom is
/// `<clusterId>@<offset>` or `-`; any other empty column is `-` too.  One line, no newline.
[[nodiscard]] std::string formatHistoryRow(const rune::catalog::RecordedEvent&);

/// `rune catalog replay`: is the target event the replay of `source`?  nullopt = ok; otherwise the
/// message `replay aborted at source offset N (<kind> '<name>'): …` (no replayedFrom / a different
/// source offset / a different kind / a different fingerprint).
[[nodiscard]] std::optional<std::string> verifyReplayStep(const rune::catalog::RecordedEvent& source,
                                                          const rune::catalog::RecordedEvent& target);

/// As above, plus: the target event must replay THIS run's cluster id.
[[nodiscard]] std::optional<std::string> verifyReplayStep(const rune::catalog::RecordedEvent& source,
                                                          const rune::catalog::RecordedEvent& target,
                                                          std::string_view clusterId);

/// The deterministic id of replaying `sourceOffset` of `sourceClusterId`:
/// {fnv1a64(sourceClusterId), uint64(sourceOffset)} — a re-run issues the same ids.
[[nodiscard]] rune::catalog::RequestId replayRequestId(std::string_view sourceClusterId, int64_t sourceOffset) noexcept;

/// Exactly 32 hex digits (either case): hi = the first 16, lo = the last 16.  Anything else: nullopt.
[[nodiscard]] std::optional<rune::catalog::RequestId> parseRequestId(std::string_view hex32) noexcept;

/// Repeats refresh() + lookup() until lookup() yields a value, refresh() fails, or nowMs() reaches
/// deadlineMs (checked after each lookup, so at least one refresh+lookup always runs).  pause() runs
/// between attempts.  nullopt = not visible; `why` says why ("<refresh error>" or "not visible before
/// the deadline").  Bounded: the loop ends at the deadline.
template <class RefreshFn, class LookupFn, class NowFn, class PauseFn>
[[nodiscard]] std::optional<rune::catalog::RequestRecord> awaitRequestVisible(RefreshFn&& refresh, LookupFn&& lookup,
                                                                              NowFn&& nowMs, PauseFn&& pause,
                                                                              int64_t deadlineMs, std::string& why) {
    for (;;) {
        if (std::optional<std::string> e = refresh()) { why = std::move(*e); return std::nullopt; }
        if (std::optional<rune::catalog::RequestRecord> r = lookup()) return r;
        if (nowMs() >= deadlineMs) { why = "not visible before the deadline"; return std::nullopt; }
        pause();
    }
}

/// `SHOW TOPIC / SCHEMA / VIEW … AS OF` bodies and `DIFF` lines (pure, multi-line, '\n'-terminated).
[[nodiscard]] std::string formatTopicDefinition(const rune::catalog::TopicObject&);
[[nodiscard]] std::string formatSchemaDefinition(const rune::catalog::TopicObject&);
[[nodiscard]] std::string formatViewDefinition(const rune::catalog::ViewObject&);
/// `+ topic NAME` | `- view NAME` | `~ topic NAME: field, field`  (no newline)
[[nodiscard]] std::string formatDiffEntry(const rune::catalog::DiffEntry&);

#ifdef RUNE_CLI_LIVE_ENABLED
/// One partition's counters, and the label of each of `result.constraints` (`<view>.<constraint>`, `<view>.unique`).
struct PartitionStats {
    int32_t                   partition = 0;
    rune::client::StatsResult result;
    std::vector<std::string>  constraintLabels;
};

/// Per partition `  partition <p>`, `    <stat> <value>` in id order, `    constraint <label> <rejections>`; `    (no counters)` for none.
[[nodiscard]] std::string formatStatsText(std::span<const PartitionStats>);
/// `{"<objectKind>": "<name>", "partitions": [{"partition": 0, "stats": {...}, "constraints": [...]}]}` on one line, no newline.
[[nodiscard]] std::string formatStatsJson(std::string_view objectKind, std::string_view name, std::span<const PartitionStats>);
#endif

/// Prints the `rune ql` / `rune catalog` usage lines.
void printCatalogUsage(std::ostream& err);

/// `rune ql …`: args are everything after "ql".
int cmdQl(const std::vector<std::string>& args, std::istream& in, std::ostream& out, std::ostream& err);
/// `rune stats (topic | view) <name> …`: args are everything after "stats".
int cmdStats(const std::vector<std::string>& args, std::ostream& out, std::ostream& err);
/// `rune catalog export|replay|diff …`: args are everything after "catalog".
int cmdCatalog(const std::vector<std::string>& args, std::ostream& out, std::ostream& err);

} // namespace rune::cli
