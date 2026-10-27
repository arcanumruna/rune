#pragma once
// libs/node/include/rune/node/TopicDirReconcile.h
//
// Catalog history Task 16 (spec §3, §5.3): boot's topic-directory reconcile.
//
// `__catalog` is the single source of truth for which topics exist, so the topic
// directories under dataDir are reconciled against the BOOTED catalog — the live
// topics (and live view sinks) it names and the topic ids it dropped.
//
// Every disk-backed CATALOG topic dir carries a topic-id marker (rune/gateway/
// TopicDirMarker.h): "<id>\n", written before any partition dir exists.  A dir whose
// marker names id X holds only X's data; a markerless dir with no record bytes holds
// nothing.
//
// Namespaces (Task 6, D14; NamespacePaths.h): the steps below run once per namespace root
// <ns> = dataDir/ns-<nsId> — every ns-<u32> dir under dataDir, plus every namespace a catalog
// topic names — against that namespace's catalog topics only.  An entry directly under dataDir
// that is not ns-<u32> (a stray dir, a pre-namespace dataDir/topics, ns-<id>.deleting) is logged
// and left untouched: never read, moved or deleted (no migration).  Below, "dataDir/" means <ns>/.
//
//   0. every dataDir/topics/<name> claimed (exactly or by ASCII case fold) by a CATALOG
//      topic t is judged by its marker:
//        keep        marker == t's id (t not transient); or no valid marker and no record
//                    bytes (TopicRegistry::createTopicAt adopts it);
//        retire      marker names ANOTHER id the catalog marks dropped (an older
//                    incarnation of a re-created name, or an AlterView-retired sink):
//                    moved to dataDir/dropped/<id>.<name> (result.retired) — the live
//                    topic starts empty — and purged by step 1 in the same run;
//        quarantine  anything else (a marker naming a live / unknown id; record bytes
//                    with no valid marker).
//   1. dataDir/dropped/<id>.<name> — a dropped topic's data, left there by a purge the
//      admin thread could not finish — is REMOVED, but only when <id> is a topic id the
//      catalog marks dropped; any other entry is left alone and logged.
//   2. every dataDir/topics/<name> that no catalog topic claims is
//      QUARANTINED: moved, atomically, to dataDir/quarantine/<name>-<nowUnixSeconds>[-N].
//      A topic dir is NEVER deleted.  Symlinks are never followed; a directory that does
//      not have the layout of a topic dir (<digits>/<regular files>) is left alone.
//
// Idempotent: a crash anywhere re-runs to the same end state.
//
// The retired `dataDir/__topic_schemas` and `dataDir/__views` dirs are never read
// and never touched (no migration, spec §1); warnRetiredCatalogDirs logs them once
// per boot.
//
// A boot whose catalog replay HALTED must NOT reconcile (the caller skips it).  A broker
// whose local catalog cannot be trusted (wiped while it holds catalog-made dirs) reconciles
// only at a LATER boot, after it proved as a follower that it applied the leader's history
// (decideBootReconcile / the catch-up sentinel).  Runs on the STARTUP thread only.
#include <rune/node/NodeConfig.h>

#include <cstdint>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace rune::node {

struct TopicDirReconciliation {
    std::vector<std::filesystem::path> quarantined;   // destinations under ns-<id>/quarantine/
    std::vector<std::filesystem::path> purged;        // ns-<id>/dropped/ entries removed
    std::vector<std::filesystem::path> retired;       // ns-<id>/dropped/ destinations of older incarnations (step 0)
    std::vector<std::filesystem::path> removedSinkDirs;   // VLS B.2: empty dirs of former view-sink replicas, removed
};

/// `catalogTopics`: every topic the catalog holds live (view sinks included): name, id, persistence.
/// `droppedIds`: the topic ids the catalog dropped.  Never throws (every failure is logged and leaves the entry in place).
[[nodiscard]] TopicDirReconciliation
reconcileTopicDirs(const std::string& dataDir, const std::vector<TopicSpec>& catalogTopics,
                   std::span<const int32_t> droppedIds, int64_t nowUnixSeconds);

// ─── The catch-up sentinel (fix round 1, R2) ──────────────────────────────────
// `dataDir/.catalog-catchup`: "pending\n" = a broker booted with a local catalog it could not trust
// (empty while catalog-made topic dirs exist) and its catch-up has not completed; "caughtup <offset>\n"
// = as a FOLLOWER it applied the leader's committed history up to <offset>.  The deferred reconcile runs
// at the boot AFTER the sentinel reads caughtup, on the startup thread.
inline constexpr const char* kCatchUpSentinelFile = ".catalog-catchup";
struct CatchUpSentinel {
    enum class Kind : uint8_t { Absent, Pending, CaughtUp };
    Kind    kind   = Kind::Absent;
    int64_t offset = -1;
};
/// "pending\n" | "caughtup <dec>\n"; any other content reads as Pending (logged).  Never throws.
[[nodiscard]] CatchUpSentinel readCatchUpSentinel(const std::string& dataDir) noexcept;
/// caughtUpAt < 0 writes "pending\n"; else "caughtup <caughtUpAt>\n".  tmp + fsync + rename + dir fsync.
bool writeCatchUpSentinel(const std::string& dataDir, int64_t caughtUpAt) noexcept;
/// unlink + dir fsync; absent -> true.
bool removeCatchUpSentinel(const std::string& dataDir) noexcept;
/// Any non-symlink child of dataDir/ns-<u32>/topics (any namespace) whose topic-id marker is Valid (a catalog-made dir).
[[nodiscard]] bool topicsHoldCatalogMarkedDir(const std::string& dataDir) noexcept;
/// Any entry in dataDir/ns-<u32>/dropped, in any namespace (a purge an earlier run did not finish).
[[nodiscard]] bool droppedHoldsEntries(const std::string& dataDir) noexcept;

enum class BootReconcile : uint8_t { Skip, RunNow, RunNowAndClearSentinel, Defer, DeferAndWritePending };
/// Pure.  First match wins:
///   halted                                               -> Skip
///   sentinel CaughtUp && lastApplied >= s.offset         -> RunNowAndClearSentinel
///   sentinel CaughtUp && lastApplied <  s.offset         -> DeferAndWritePending
///   sentinel Pending                                     -> Defer
///   lastApplied < 0 && (markedDirs || droppedHasEntries) -> DeferAndWritePending
///   otherwise                                            -> RunNow
/// Only MARKED dirs defer: markerless dirs are legacy / static and reconcile at once.
[[nodiscard]] BootReconcile decideBootReconcile(bool halted, int64_t lastApplied, bool markedDirs,
                                                bool droppedHasEntries, CatchUpSentinel s) noexcept;

/// Logs, once, a retired `__topic_schemas` / `__views` dir that holds data: it is
/// ignored (never read, never modified) — `__catalog` is the catalog.
void warnRetiredCatalogDirs(const std::string& dataDir);

} // namespace rune::node
