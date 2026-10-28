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
