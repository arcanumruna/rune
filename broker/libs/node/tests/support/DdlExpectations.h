#pragma once
// DdlExpectations.h — DDL conformance plan: THE expectation table.
//
// Occupant (what the target name names right now) x Op -> the exact error code and
// the number of requestCreateTopic calls the op makes.  Every unit matrix, the
// stress model and the BrokerNode burst read THIS table; the plan's markdown table
// is a copy of it.  A "control-detected" rejection (the admin thread cannot see
// schema-less topics, the control thread can) makes exactly one request and
// creates nothing.  Test-only.
#include <cstdint>

namespace rune::node::test {

// rune::protocol::client::ErrorCode values (ints: protocol-agnostic, like AdminWorker).
inline constexpr uint16_t kOk                  = 0;
inline constexpr uint16_t kInternal            = 6;
inline constexpr uint16_t kUnknownTopic        = 7;
inline constexpr uint16_t kInvalidSchema       = 23;
inline constexpr uint16_t kTopicExists         = 25;
inline constexpr uint16_t kSchemaConflict      = 31;
inline constexpr uint16_t kViewExists          = 32;
inline constexpr uint16_t kViewSourceNotFound  = 33;
inline constexpr uint16_t kViewFieldUnresolved = 34;
inline constexpr uint16_t kViewSourceTransient = 37;
inline constexpr uint16_t kViewNotAlterable    = 38;
inline constexpr uint16_t kInvalidTopicName    = 40;
inline constexpr uint16_t kViewSourceIsView     = 41;

enum class Op : uint8_t {
    DefineDisk, DefineTransient, ViewPersisted, ViewTransient,
    AlterAdd, AlterDrop, AlterRename, AlterRenameKey, AlterSetCompat, AlterCasCurrent, AlterCasStale,
    GetSchema,
};
inline constexpr int kOpCount = 12;
inline constexpr Op kAllOps[kOpCount] = {
    Op::DefineDisk, Op::DefineTransient, Op::ViewPersisted, Op::ViewTransient,
    Op::AlterAdd, Op::AlterDrop, Op::AlterRename, Op::AlterRenameKey, Op::AlterSetCompat,
    Op::AlterCasCurrent, Op::AlterCasStale, Op::GetSchema,
};

[[nodiscard]] inline const char* opName(Op o) noexcept {
    switch (o) {
        case Op::DefineDisk:      return "DefineDisk";
        case Op::DefineTransient: return "DefineTransient";
        case Op::ViewPersisted:   return "ViewPersisted";
        case Op::ViewTransient:   return "ViewTransient";
        case Op::AlterAdd:        return "AlterAdd";
        case Op::AlterDrop:       return "AlterDrop";
        case Op::AlterRename:     return "AlterRename";
        case Op::AlterRenameKey:  return "AlterRenameKey";
        case Op::AlterSetCompat:  return "AlterSetCompat";
        case Op::AlterCasCurrent: return "AlterCasCurrent";
        case Op::AlterCasStale:   return "AlterCasStale";
        case Op::GetSchema:       return "GetSchema";
    }
    return "?";
}
[[nodiscard]] constexpr bool isDefine(Op o) noexcept { return o == Op::DefineDisk || o == Op::DefineTransient; }
[[nodiscard]] constexpr bool isView(Op o) noexcept { return o == Op::ViewPersisted || o == Op::ViewTransient; }
[[nodiscard]] constexpr bool isCreate(Op o) noexcept { return isDefine(o) || isView(o); }
[[nodiscard]] constexpr bool isAlter(Op o) noexcept { return o >= Op::AlterAdd && o <= Op::AlterCasStale; }

enum class Occupant : uint8_t {
    Free, TypedDisk, TypedTransient, ViewPersisted, ViewTransient,
    SchemaLessDisk, SchemaLessTransient, OrphanSink, CaseVariant, BadName,
};
inline constexpr int kOccupantCount = 10;

[[nodiscard]] inline const char* occupantName(Occupant o) noexcept {
    switch (o) {
        case Occupant::Free:                return "Free";
        case Occupant::TypedDisk:           return "TypedDisk";
        case Occupant::TypedTransient:      return "TypedTransient";
        case Occupant::ViewPersisted:       return "ViewPersisted";
        case Occupant::ViewTransient:       return "ViewTransient";
        case Occupant::SchemaLessDisk:      return "SchemaLessDisk";
        case Occupant::SchemaLessTransient: return "SchemaLessTransient";
        case Occupant::OrphanSink:          return "OrphanSink";
        case Occupant::CaseVariant:         return "CaseVariant";
        case Occupant::BadName:             return "BadName";
    }
    return "?";
}

struct Cell { uint16_t code; uint8_t createRequests; };

// Columns: DefD DefT ViewP ViewT Add Drop Rename RenameKey SetCompat CasCur CasStale Get
inline constexpr uint16_t kExpectedCode[kOccupantCount][kOpCount] = {
    /* Free                */ { 0,  0,  0,  0,  7,  7,  7,  7,  7,  7,  7,  7},
    /* TypedDisk           */ {25, 25, 25, 25,  0,  0,  0,  0,  0,  0, 31,  0},
    /* TypedTransient      */ {25, 25, 25, 25,  0,  0,  0,  0,  0,  0, 31,  0},
    /* ViewPersisted       */ {25, 25, 32, 32, 38, 38, 38, 38, 38, 38, 38,  0},
    /* ViewTransient       */ {25, 25, 32, 32, 38, 38, 38, 38, 38, 38, 38,  0},
    /* SchemaLessDisk      */ { 0, 25, 25, 25,  7,  7,  7,  7,  7,  7,  7,  7},
    /* SchemaLessTransient */ { 0,  0, 25, 25,  7,  7,  7,  7,  7,  7,  7,  7},
    /* OrphanSink (KL1)    */ { 0, 25,  0,  0,  7,  7,  7,  7,  7,  7,  7,  7},
    /* CaseVariant (B3)    */ {25, 25, 25, 25,  7,  7,  7,  7,  7,  7,  7,  7},
    /* BadName (B1/B2)     */ {40, 40, 40, 40,  7,  7,  7,  7,  7,  7,  7,  7},
};
inline constexpr uint8_t kExpectedCreateRequests[kOccupantCount][kOpCount] = {
    /* Free                */ {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0},
    /* TypedDisk           */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
    /* TypedTransient      */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
    /* ViewPersisted       */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
    /* ViewTransient       */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
    /* SchemaLessDisk      */ {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0},
    /* SchemaLessTransient */ {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0},
    /* OrphanSink (KL1)    */ {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0},
    /* CaseVariant (B3)    */ {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0},
    /* BadName (B1/B2)     */ {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
};

[[nodiscard]] constexpr Cell expectedCell(Occupant o, Op op) noexcept {
    return Cell{kExpectedCode[static_cast<int>(o)][static_cast<int>(op)],
                kExpectedCreateRequests[static_cast<int>(o)][static_cast<int>(op)]};
}

/// What the name names after a SUCCESSFUL `op` on occupant `before`.  A define over
/// a schema-less transient topic adopts it and records Transient; alters and reads
/// keep the kind.
[[nodiscard]] constexpr Occupant occupantAfterSuccess(Occupant before, Op op) noexcept {
    switch (op) {
        case Op::DefineDisk:
            return before == Occupant::SchemaLessTransient ? Occupant::TypedTransient : Occupant::TypedDisk;
        case Op::DefineTransient: return Occupant::TypedTransient;
        case Op::ViewPersisted:   return Occupant::ViewPersisted;
        case Op::ViewTransient:   return Occupant::ViewTransient;
        default:                  return before;
    }
}

/// What a create leaves behind when the control thread created its topic but the
/// op never committed (reply failed after create / timeout / demotion).
[[nodiscard]] constexpr Occupant orphanOf(Op op) noexcept {
    switch (op) {
        case Op::DefineDisk:      return Occupant::SchemaLessDisk;
        case Op::DefineTransient: return Occupant::SchemaLessTransient;
        default:                  return Occupant::OrphanSink;   // a view sink is created Disk (Part E)
    }
}

} // namespace rune::node::test
