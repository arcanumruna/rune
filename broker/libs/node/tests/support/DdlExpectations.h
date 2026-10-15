#pragma once
// DdlExpectations.h — DDL conformance plan: THE expectation table (catalog
// history Task 10: answered by the __catalog write path).
//
// Occupant (what the target name names right now) x Op -> the exact error code and
// the number of __catalog events the op appends.  Every unit matrix, the stress
// model and the BrokerNode burst read THIS table; the plan's markdown table is a
// copy of it.  Every rejection is decided on the admin thread from the applied
// catalog (the former "detected by the control thread" rows are gone): a rejected
// op appends 0 events, an accepted one exactly 1, a read 0.  Test-only.
#include <cstdint>

namespace rune::node::test {

// rune::protocol::client::ErrorCode values (ints: protocol-agnostic, like AdminWorker).
inline constexpr uint16_t kOk                  = 0;
inline constexpr uint16_t kNotLeader           = 2;
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
inline constexpr uint16_t kInvalidRequest      = 39;
inline constexpr uint16_t kInvalidTopicName    = 40;
inline constexpr uint16_t kViewSourceIsView     = 41;

enum class Op : uint8_t {
    DefineDisk, DefineTransient, ViewPersisted, ViewTransient,
    AlterAdd, AlterDrop, AlterRename, AlterRenameKey, AlterSetCompat, AlterCasCurrent, AlterCasStale,
    GetSchema,
    // Catalog history (Task 9's new wire ops 74-77).
    DropTopic, DropView, AlterView, AlterTopicConfig,
};
inline constexpr int kOpCount = 16;
inline constexpr Op kAllOps[kOpCount] = {
    Op::DefineDisk, Op::DefineTransient, Op::ViewPersisted, Op::ViewTransient,
    Op::AlterAdd, Op::AlterDrop, Op::AlterRename, Op::AlterRenameKey, Op::AlterSetCompat,
    Op::AlterCasCurrent, Op::AlterCasStale, Op::GetSchema,
    Op::DropTopic, Op::DropView, Op::AlterView, Op::AlterTopicConfig,
};

[[nodiscard]] inline const char* opName(Op o) noexcept {
    switch (o) {
        case Op::DefineDisk:       return "DefineDisk";
        case Op::DefineTransient:  return "DefineTransient";
        case Op::ViewPersisted:    return "ViewPersisted";
        case Op::ViewTransient:    return "ViewTransient";
        case Op::AlterAdd:         return "AlterAdd";
        case Op::AlterDrop:        return "AlterDrop";
        case Op::AlterRename:      return "AlterRename";
        case Op::AlterRenameKey:   return "AlterRenameKey";
        case Op::AlterSetCompat:   return "AlterSetCompat";
        case Op::AlterCasCurrent:  return "AlterCasCurrent";
        case Op::AlterCasStale:    return "AlterCasStale";
        case Op::GetSchema:        return "GetSchema";
        case Op::DropTopic:        return "DropTopic";
        case Op::DropView:         return "DropView";
        case Op::AlterView:        return "AlterView";
        case Op::AlterTopicConfig: return "AlterTopicConfig";
    }
    return "?";
}
[[nodiscard]] constexpr bool isDefine(Op o) noexcept { return o == Op::DefineDisk || o == Op::DefineTransient; }
[[nodiscard]] constexpr bool isView(Op o) noexcept { return o == Op::ViewPersisted || o == Op::ViewTransient; }
[[nodiscard]] constexpr bool isCreate(Op o) noexcept { return isDefine(o) || isView(o); }
[[nodiscard]] constexpr bool isAlter(Op o) noexcept { return o >= Op::AlterAdd && o <= Op::AlterCasStale; }
[[nodiscard]] constexpr bool isDrop(Op o) noexcept { return o == Op::DropTopic || o == Op::DropView; }
/// The ops answered by a DdlResponse (templates 74-77): their version is the OBJECT version.
[[nodiscard]] constexpr bool isCatalogOp(Op o) noexcept { return o >= Op::DropTopic; }
/// The ops whose plan names sources (CREATE VIEW, ALTER VIEW).
[[nodiscard]] constexpr bool readsSources(Op o) noexcept { return isView(o) || o == Op::AlterView; }

// OrphanSink removed (catalog history): a create whose event never committed leaves no topic, dir or id.
enum class Occupant : uint8_t {
    Free, TypedDisk, TypedTransient, ViewPersisted, ViewTransient,
    SchemaLessDisk, SchemaLessTransient, CaseVariant, BadName,
};
inline constexpr int kOccupantCount = 9;

[[nodiscard]] inline const char* occupantName(Occupant o) noexcept {
    switch (o) {
        case Occupant::Free:                return "Free";
        case Occupant::TypedDisk:           return "TypedDisk";
        case Occupant::TypedTransient:      return "TypedTransient";
        case Occupant::ViewPersisted:       return "ViewPersisted";
        case Occupant::ViewTransient:       return "ViewTransient";
        case Occupant::SchemaLessDisk:      return "SchemaLessDisk";
        case Occupant::SchemaLessTransient: return "SchemaLessTransient";
        case Occupant::CaseVariant:         return "CaseVariant";
        case Occupant::BadName:             return "BadName";
    }
    return "?";
}

struct Cell { uint16_t code; uint8_t events; };

// Columns: DefD DefT ViewP ViewT Add Drop Rename RenameKey SetCompat CasCur CasStale Get
//          | DropTopic DropView AlterView AlterTopicConfig   (Task 9's table)
// SchemaLess* are UNTYPED topics created by DDL (an empty descriptor — formerly
// static topics).  SchemaLess* x every Alter* stays 7 (F3: an untyped topic has
// no schema to alter).  DropTopic of a typed / untyped topic assumes no live view
// reads it (a view reader is 39 with a detail — pinned apart); DropTopic of a view
// name is 39 ("is a view; use DROP VIEW").
inline constexpr uint16_t kExpectedCode[kOccupantCount][kOpCount] = {
    /* Free                */ { 0,  0,  0,  0,  7,  7,  7,  7,  7,  7,  7,  7,    7,  7,  7,  7},
    /* TypedDisk           */ {25, 25, 25, 25,  0,  0,  0,  0,  0,  0, 31,  0,    0,  7,  7,  0},
    /* TypedTransient      */ {25, 25, 25, 25,  0,  0,  0,  0,  0,  0, 31,  0,    0,  7,  7,  0},
    /* ViewPersisted       */ {25, 25, 32, 32, 38, 38, 38, 38, 38, 38, 38,  0,   39,  0,  0, 38},
    /* ViewTransient       */ {25, 25, 32, 32, 38, 38, 38, 38, 38, 38, 38,  0,   39,  0,  0, 38},
    /* SchemaLessDisk      */ { 0, 25, 25, 25,  7,  7,  7,  7,  7,  7,  7,  7,    0,  7,  7,  0},
    /* SchemaLessTransient */ { 0,  0, 25, 25,  7,  7,  7,  7,  7,  7,  7,  7,    0,  7,  7,  0},
    /* CaseVariant (B3)    */ {25, 25, 25, 25,  7,  7,  7,  7,  7,  7,  7,  7,    7,  7,  7,  7},
    /* BadName (B1/B2)     */ {40, 40, 40, 40,  7,  7,  7,  7,  7,  7,  7,  7,    7,  7,  7,  7},
};

/// Events appended: 0 for every rejection and every read, 1 for an accepted op.
[[nodiscard]] constexpr uint8_t expectedEvents(Occupant o, Op op) noexcept {
    return op != Op::GetSchema && kExpectedCode[static_cast<int>(o)][static_cast<int>(op)] == kOk ? 1 : 0;
}

[[nodiscard]] constexpr Cell expectedCell(Occupant o, Op op) noexcept {
    return Cell{kExpectedCode[static_cast<int>(o)][static_cast<int>(op)], expectedEvents(o, op)};
}

/// What the name names after a SUCCESSFUL `op` on occupant `before`.  A define over
/// an untyped transient topic adopts it and records Transient; a drop frees the
/// name; alters, ALTER VIEW (same persistence), config and reads keep the kind.
[[nodiscard]] constexpr Occupant occupantAfterSuccess(Occupant before, Op op) noexcept {
    switch (op) {
        case Op::DefineDisk:
            return before == Occupant::SchemaLessTransient ? Occupant::TypedTransient : Occupant::TypedDisk;
        case Op::DefineTransient: return Occupant::TypedTransient;
        case Op::ViewPersisted:   return Occupant::ViewPersisted;
        case Op::ViewTransient:   return Occupant::ViewTransient;
        case Op::DropTopic:
        case Op::DropView:        return Occupant::Free;
        default:                  return before;
    }
}

} // namespace rune::node::test
