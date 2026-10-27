// ---------------------------------------------------------------------------
// rune_role — standalone reader for the node-role metrics in the broker's arcanum-metrics file.
//
// Reads the machine-readable leadership signal that BrokerNode publishes into its metrics file (type ids and the
// 4-byte LE node-id key: RuneMetrics.h, arcanum-metrics plan D54/D61): the node-role metric, the leadership-term
// metric and the storage-gate metrics.  Prints two lines to stdout:
//
//     ROLE nodeId=<n> role=<LEADER|FOLLOWER|CANDIDATE|NONE> term=<t>
//     STORAGE nodeId=<n> state=<CONFIRMED|UNCONFIRMED|UNDECIDED|UNKNOWN> heldLeaderRoles=<n> pendingNodes=<n> \
//             outOfSync=<n> lostCommittedReports=<n>        (ISR v3 section 3.10; one line; a metric that is
//                                                            absent prints -1, the state prints UNKNOWN)
//
// The output is byte-identical to the cnc.dat reader it replaces (RuneRoleRegression.GoldenOutputTable): the dr-e2e
// harness runs `docker exec <svc> rune_role` and parses these lines, so leader detection does not depend on
// RUNE_LOG_LEVEL.
//
// Usage:
//     rune_role [<metricsFile>]
//   - If <metricsFile> is given, reads it.
//   - Else ${RUNE_METRICS_FILE}, else ${RUNE_DATA_DIR}/metrics/rune-metrics.dat (RUNE_DATA_DIR default
//     /var/lib/rune) — the broker's own default (NodeConfig::effectiveMetricsFile).
//
// The node is the one the first role metric names (one broker per file); the term and storage metrics are looked up
// by (typeId, that node's key).
//
// Exit codes:
//   0  role metric found and printed
//   1  metrics file missing / unreadable / not ready yet (the open status is printed on stderr)
//   2  file readable but the role metric is not present yet
// ---------------------------------------------------------------------------
#include <rune/node/RuneMetrics.h>

#include <metrics/MetricsFile.h>
#include <metrics/MetricsReader.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <span>
#include <string>

using rune::node::RuneMetric;
using rune::node::typeIdOf;
using rune::node::RoleCode;
using rune::node::roleCodeFromValue;
using rune::node::roleCodeName;
using rune::node::decodeRoleKey;

namespace {

/// Polls of the header's version word before an open reports NotReady (plan Task 10).
constexpr std::int32_t kMaxVersionPolls = 100;

std::string resolveMetricsFile(int argc, char** argv) {
    if (argc > 1 && argv[1] != nullptr && argv[1][0] != '\0') {
        return std::string(argv[1]);
    }
    if (const char* mf = std::getenv("RUNE_METRICS_FILE"); mf != nullptr && mf[0] != '\0') {
        return std::string(mf);
    }
    const char* dataDir = std::getenv("RUNE_DATA_DIR");
    const std::string base = (dataDir && dataDir[0] != '\0')
        ? std::string(dataDir) : std::string("/var/lib/rune");
    return base + "/metrics/rune-metrics.dat";
}

const char* fileStatusName(metrics::FileStatus s) noexcept {
    switch (s) {
        case metrics::FileStatus::Ok:               return "Ok";
        case metrics::FileStatus::PathTooLong:      return "PathTooLong";
        case metrics::FileStatus::CapacityTooSmall: return "CapacityTooSmall";
        case metrics::FileStatus::CapacityTooLarge: return "CapacityTooLarge";
        case metrics::FileStatus::IoError:          return "IoError";
        case metrics::FileStatus::NotReady:         return "NotReady";
        case metrics::FileStatus::VersionMismatch:  return "VersionMismatch";
        case metrics::FileStatus::BadLength:        return "BadLength";
        case metrics::FileStatus::TooLarge:         return "TooLarge";
    }
    return "Unknown";
}

/// One consistent read of `slot` (a slot reused mid-read is read once more, as MetricsReader's scans do).
bool readLive(const metrics::MetricsReader& reader, std::int32_t slot, metrics::Live& out) noexcept {
    if (slot < 0) return false;
    metrics::ReadStatus st = reader.read(slot, out);
    if (st == metrics::ReadStatus::Reused) st = reader.read(slot, out);
    return st == metrics::ReadStatus::Live;
}

/// The value of (typeId of `m`, `key`), or -1 when the metric is absent.
std::int64_t valueOr(const metrics::MetricsReader& reader, RuneMetric m, std::span<const std::byte> key) noexcept {
    metrics::Live live;
    return readLive(reader, reader.findFirst(typeIdOf(m), key).slot, live) ? live.value : -1;
}

} // namespace

int main(int argc, char** argv) {
    const std::string path = resolveMetricsFile(argc, argv);

    metrics::MetricsFile file;
    if (const metrics::FileStatus st = metrics::MetricsFile::open(path.c_str(), kMaxVersionPolls, file);
        st != metrics::FileStatus::Ok) {
        std::cerr << "rune_role: cannot open metrics file '" << path << "': " << fileStatusName(st) << "\n";
        return 1;
    }
    const metrics::MetricsReader reader{file};

    // The role metric: the first live slot of its type (one broker per file).  Its key names the node, and every
    // other metric is matched by (typeId, that same key).
    std::int32_t roleSlot = -1;
    reader.forEachLive([&](const metrics::Live& l) {
        if (roleSlot < 0 && l.typeId == typeIdOf(RuneMetric::Role)) roleSlot = l.slot;
    });
    metrics::Live roleLive;
    if (!readLive(reader, roleSlot, roleLive)) {
        std::cerr << "rune_role: role metric (typeId=" << typeIdOf(RuneMetric::Role)
                  << ") not present in '" << path << "' yet\n";
        return 2;
    }
    const std::span<const std::byte> key = roleLive.key();
    std::int32_t nodeId = -1;
    std::int32_t nid = -1;
    if (decodeRoleKey(reinterpret_cast<const std::uint8_t*>(key.data()), key.size(), nid)) nodeId = nid;
    const RoleCode role = roleCodeFromValue(roleLive.value);
    const std::int64_t term = valueOr(reader, RuneMetric::Term, key);   // -1 pre-election (or absent)

    const std::int64_t gateState     = valueOr(reader, RuneMetric::StorageGateState, key);
    const std::int64_t heldRoles     = valueOr(reader, RuneMetric::LeaderRolesHeld, key);
    const std::int64_t pendingNodes  = valueOr(reader, RuneMetric::StoragePendingNodes, key);
    const std::int64_t outOfSync     = valueOr(reader, RuneMetric::OutOfSyncPartitions, key);
    const std::int64_t lostCommitted = valueOr(reader, RuneMetric::LostCommittedReports, key);

    std::cout << "ROLE nodeId=" << nodeId
              << " role=" << roleCodeName(role)
              << " term=" << term << "\n";
    const char* state = gateState == 0 ? "CONFIRMED" : gateState == 1 ? "UNCONFIRMED" : gateState == 2 ? "UNDECIDED" : "UNKNOWN";
    std::cout << "STORAGE nodeId=" << nodeId
              << " state=" << state
              << " heldLeaderRoles=" << heldRoles
              << " pendingNodes=" << pendingNodes
              << " outOfSync=" << outOfSync
              << " lostCommittedReports=" << lostCommitted << "\n";
    return 0;
}
