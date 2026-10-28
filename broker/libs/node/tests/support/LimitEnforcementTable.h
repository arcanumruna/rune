#pragma once
// LimitEnforcementTable.h - the namespace limit enforcement table (namespaces Phase 2, D23-D25; plan Tasks 9-14).
//
// A constexpr table of rows, one per NamespaceLimits field a task enforces.  Every row is exercised in three cells:
//   Below : the operation succeeds with usage under the limit;
//   At    : the operation that brings usage TO the limit succeeds (usage == limit afterwards);
//   Above : the next operation is refused with `codeAbove`, nothing is applied (no catalog / security event written).
// (For a per-request size - partitions per topic, schema versions - Below/At/Above are L-1 / L / L+1.)
// The tests read their limit value and expectations from the row; `kCellsPerRow` pins the cell count.
// Tasks 9, 10 and 11 add the rows they enforce; Tasks 12-14 append theirs (view memory, retention, rates).

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace rune::node::test {

enum class LimitArea : uint8_t {
    Ddl,          // AdminWorker, from CatalogState (counts, durability, features)
    Security,     // SecurityAdmin (maxUsers)
    Group,        // group coordinator (maxGroups)
    Gateway,      // gateway, per request (lowestAcks, features)
    Connections,  // gateway, per node (Task 10)
    Txn,          // transaction coordinator (Task 10)
    Disk,         // worker, per node (Task 11)
    ViewMemory,   // worker's view set (Task 12)
    Retention,    // AdminWorker refusal + the cleaner's clamp (Task 13)
    ProduceSize,  // worker produce intake, per batch / per record
    Rate,         // the limit aggregator's throttle deadline (Task 14, D28); codeAbove 53 = Throttled
};

enum class LimitCell : uint8_t { Below = 0, At = 1, Above = 2 };
inline constexpr std::size_t kCellsPerRow = 3;

inline constexpr uint16_t kCodeNamespaceLimitExceeded = 52;

struct LimitRow {
    std::string_view field;        // the CLI name (NamespaceLimitFields.h)
    std::size_t      index;        // the field's index in NamespaceLimits (0..28)
    LimitArea        area;
    uint64_t         limit;        // the value the test sets
    uint16_t         codeAbove;    // the refusal at Above
    std::string_view operation;    // what the cells run
    std::string_view atObservation;
};

// clang-format off
inline constexpr std::array kLimitRows = {
    // ---- Task 9: counts, durability, features ---------------------------------------------------------------
    LimitRow{"max-topics",               0,  LimitArea::Ddl,      3, 52, "DefineTopic",                "the 3rd topic is created"},
    LimitRow{"max-partitions-per-topic", 1,  LimitArea::Ddl,      4, 52, "DefineTopic(partitions=L-1/L/L+1)", "a 4-partition topic is created"},
    LimitRow{"max-partitions-total",     2,  LimitArea::Ddl,      6, 52, "DefineTopic(2 partitions)",  "the partitions sum to exactly 6"},
    LimitRow{"max-views",                3,  LimitArea::Ddl,      2, 52, "CreateView",                 "the 2nd view is created"},
    LimitRow{"max-schemas",              4,  LimitArea::Ddl,      2, 52, "DefineTopic(typed)",         "the 2nd typed topic is created"},
    LimitRow{"max-versions-per-schema",  5,  LimitArea::Ddl,      3, 52, "AlterTopic(add field)",      "schema version 3 is reached"},
    LimitRow{"max-groups",               6,  LimitArea::Group,    2, 52, "JoinGroup(new group)",       "the 2nd group is created"},
    LimitRow{"max-users",                7,  LimitArea::Security, 2, 52, "CreateUser",                 "the 2nd user is created"},
    LimitRow{"min-replication-factor",  24,  LimitArea::Ddl,      2, 52, "DefineTopic(rf)",            "rf == 2 is created"},
    LimitRow{"max-replication-factor",  25,  LimitArea::Ddl,      2, 52, "DefineTopic(rf)",            "rf == 2 is created"},
    LimitRow{"lowest-acks",             26,  LimitArea::Gateway,  2, 52, "Produce(acks)",              "acks=All is served"},
    LimitRow{"lowest-min-insync",       27,  LimitArea::Ddl,      2, 52, "DefineTopic (cluster min ISR)", "min ISR == 2 is created"},
    LimitRow{"feature-bits",            28,  LimitArea::Ddl,      0, 52, "DefineTopic(compacted) / CreateView / InitProducerId / login", "the allowed feature is served"},
    // ---- Task 10: connections, open txns, txn time -----------------------------------------------------------
    LimitRow{"max-connections",          18, LimitArea::Connections, 2, 52, "login after the SCRAM proof", "the 2nd session of the namespace on this node"},
    LimitRow{"max-connections-per-user", 19, LimitArea::Connections, 2, 52, "login after the SCRAM proof", "the user's 2nd session on this node"},
    LimitRow{"max-sessions-per-user",    20, LimitArea::Connections, 2, 52, "login after the SCRAM proof", "the user's 2nd session on this node"},
    LimitRow{"max-open-txns",            21, LimitArea::Txn,         2, 52, "AddPartitions opens a txn",   "the 2nd open transaction"},
    LimitRow{"max-txn-time-ms",          22, LimitArea::Txn,     30000, 52, "InitProducerId(timeout)",     "a timeout == the maximum"},
    // ---- Task 11: disk ---------------------------------------------------------------------------------------
    LimitRow{"max-disk-bytes",            8, LimitArea::Disk, 16384, 52, "leader produce",            "the produce that brings usage to the limit"},
    // ---- max-message-bytes / max-batch-bytes: the worker's produce intake (Below/At/Above = L-1 / L / L+1 bytes) ----------
    LimitRow{"max-message-bytes",        11, LimitArea::ProduceSize, 1024, 52, "leader produce (one record of L-1 / L / L+1 bytes)", "a record of exactly the limit is appended"},
    LimitRow{"max-batch-bytes",          12, LimitArea::ProduceSize, 4096, 52, "leader produce (a batch of L-1 / L / L+1 bytes)", "a batch of exactly the limit is appended"},
    // ---- Task 12: view memory (limit = 2 table chunks; codeAbove 54 = ViewPaused, the read answer of a paused view) ------
    // ---- Task 13: retention maximums (Above = an explicit topic retention over the maximum is refused at DDL; a lowered maximum
    //      clamps the cleaner's effective retention to min(topic, maximum) on its next pass) ---------------------------------
    LimitRow{"max-retention-ms",          9, LimitArea::Retention, 3600000, 52, "DefineTopic / AlterTopicConfig(retention.ms)", "retention.ms == the maximum is accepted"},
    LimitRow{"max-retention-bytes",      10, LimitArea::Retention, 1048576, 52, "DefineTopic / AlterTopicConfig(retention.bytes)", "retention.bytes == the maximum is accepted"},
    // ---- Task 14: rates (the aggregator compares the excess over limit x elapsed; Above = Throttled 53 with a retry-after) ----
    LimitRow{"produce-bytes-per-sec",    13, LimitArea::Rate,  10000, 53, "leader produce (bytes)",   "usage at limit x elapsed is not throttled"},
    LimitRow{"produce-records-per-sec",  14, LimitArea::Rate,   1000, 53, "leader produce (records)", "usage at limit x elapsed is not throttled"},
    LimitRow{"fetch-bytes-per-sec",      15, LimitArea::Rate,  10000, 53, "push read (bytes)",        "usage at limit x elapsed is not throttled"},
    LimitRow{"request-ops-per-sec",      16, LimitArea::Rate,   1000, 53, "produce / fetch / view read (ops)", "usage at limit x elapsed is not throttled"},
    LimitRow{"query-cycles-per-sec",     17, LimitArea::Rate, 1000000, 53, "GetViewRow (cycles)",     "usage at limit x elapsed is not throttled"},
    LimitRow{"max-view-memory-bytes",    23, LimitArea::ViewMemory,   2, 54, "GetViewRow after the fold", "the fold that brings the view to exactly the limit still serves"},
};
// clang-format on

[[nodiscard]] constexpr const LimitRow* rowOf(std::string_view field) noexcept {
    for (const LimitRow& r : kLimitRows) if (r.field == field) return &r;
    return nullptr;
}

[[nodiscard]] constexpr std::size_t rowsOfArea(LimitArea a) noexcept {
    std::size_t n = 0;
    for (const LimitRow& r : kLimitRows) n += r.area == a ? 1 : 0;
    return n;
}

}  // namespace rune::node::test
