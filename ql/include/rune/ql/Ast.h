#pragma once
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>
#include <rune/codec/PrimType.h>
namespace rune::ql {
struct FieldDef { std::string name; rune::codec::PrimType type = rune::codec::PrimType::Int32; uint16_t fixedLen = 0; };
// Topic persistence (Phase 2b E).  Values are the DefineTopic wire byte and are
// APPEND-ONLY: Disk = durable log (default), Transient = replicated in memory,
// never written to disk (metadata survives a restart, data does not).
enum class Persistence : uint8_t { Disk = 0, Transient = 1 };

// One `key = value` of a `with (…)` / `set …` clause.  Integer and enum settings
// carry `value` (the ratio is already in ppm; an enum is its wire byte); enum and
// string settings also carry `text` (the canonical enum word / the string).
struct ConfigSetting { std::string key; int64_t value = 0; std::string text; };

struct TopicDef {
    std::string name;
    std::vector<FieldDef> fields;      // EMPTY → an untyped topic (then keyField is empty too)
    uint32_t partitions = 1;
    std::string keyField;
    Persistence persistence = Persistence::Disk;   // `transient topic …;` → Transient
    // Appended after the original members so existing positional aggregate init keeps compiling.
    int16_t replicas = 0;              // 0 = broker default; parse bound 0..32767 (F31)
    std::vector<ConfigSetting> with;   // creation-time config, parse order
};
struct ParseError { std::size_t line = 0; std::size_t col = 0; std::string message; };

enum class Compatibility { Backward, Forward };
enum class AlterOpKind { Add, Drop, Rename, SetCompatibility };
struct AlterOp {
    AlterOpKind kind = AlterOpKind::Add;
    FieldDef field;          // used by Add
    std::string name;        // used by Drop (column to drop) / Rename (old name)
    std::string newName;     // used by Rename (new name)
    Compatibility mode = Compatibility::Backward;  // used by SetCompatibility
};
struct AlterTopicDef { std::string name; std::vector<AlterOp> ops; };

struct AlterTopicConfigDef { std::string name; std::vector<ConfigSetting> settings; };
struct DropTopicDef { std::string name; };
struct DropViewDef  { std::string name; };

enum class ObjectKind { Topic, View, Schema };
struct ShowHistoryDef { std::optional<ObjectKind> kind; std::string name; std::optional<std::string> sinceIso; };
struct AsOfPoint { enum class By { Version, Time, Offset } by = By::Version; int64_t number = 0; std::string timeIso; };
struct ShowAsOfDef { ObjectKind kind = ObjectKind::Topic; std::string name; AsOfPoint at; };
struct DiffDef {
    enum class Scope { Schema, Catalog } scope = Scope::Schema;
    std::string name;            // Schema only
    int64_t from = 0, to = 0;    // Schema: versions; Catalog: offsets
};

using Statement = std::variant<TopicDef, AlterTopicDef, AlterTopicConfigDef, DropTopicDef, DropViewDef,
                               ShowHistoryDef, ShowAsOfDef, DiffDef>;
} // namespace rune::ql
