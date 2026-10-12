#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <rune/codec/PrimType.h>
namespace rune::ql {
struct FieldDef { std::string name; rune::codec::PrimType type = rune::codec::PrimType::Int32; uint16_t fixedLen = 0; };
// Topic persistence (Phase 2b E).  Values are the DefineTopic wire byte and are
// APPEND-ONLY: Disk = durable log (default), Transient = replicated in memory,
// never written to disk (metadata survives a restart, data does not).
enum class Persistence : uint8_t { Disk = 0, Transient = 1 };

struct TopicDef {
    std::string name;
    std::vector<FieldDef> fields;
    uint32_t partitions = 1;
    std::string keyField;
    Persistence persistence = Persistence::Disk;   // `transient topic …;` → Transient
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
} // namespace rune::ql
