#pragma once
#include <rune/protocol/SecurityTypes.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>
#include <rune/codec/PrimType.h>
namespace rune::ql {
struct FieldDef { std::string name; rune::codec::PrimType type = rune::codec::PrimType::Int32; uint16_t fixedLen = 0; bool autoIncrement = false; };
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
    bool lease = false;                // `lease topic …` (lease.* settings allowed)
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

// ── Security statements (authentication phase, Task 6) ──────────────────────
// The enum values are the wire / event bytes (the SecurityDdl `op` / `resourceType` / `granteeKind`
// bytes and rune::security::Operation / ResourceType / GranteeKind): ONE definition, in rune-protocol
// (SecurityTypes.h), aliased here, in rune-codec and on the wire.
using SecOp       = rune::protocol::SecurityOperation;
using SecResource = rune::protocol::SecurityResourceType;
struct SecPattern { SecResource type = SecResource::Topic; bool prefix = false; std::string name; };
using Grantee = rune::protocol::SecurityGranteeKind;
struct CreateUserDef { std::string name; std::string password; };      // password: in memory only, never rendered
struct AlterUserDef {
    enum class Change : uint8_t { AddPassword, DropPassword, SetPassword, Disable, Enable } change = Change::Disable;
    std::string name;
    std::string password;              // AddPassword / SetPassword
    uint8_t index = 0;                 // DropPassword: 1 (oldest) or 2
    bool terminateSessions = false;    // SetPassword
    bool force = false;                // Disable: FORCE (issue #121: the last security admin may go)
};
// `force` (issue #121): the statement's trailing FORCE - it may remove the last enabled security admin.
struct DropUserDef   { std::string name; bool force = false; };
struct CreateRoleDef { std::string name; };
struct DropRoleDef   { std::string name; bool force = false; };
struct GrantDef      { SecOp op = SecOp::Read; SecPattern resource; Grantee grantee = Grantee::User; std::string granteeName; };
struct RevokeDef     { SecOp op = SecOp::Read; SecPattern resource; Grantee grantee = Grantee::User; std::string granteeName;
                       bool force = false; };
struct GrantRoleDef  { std::string role; std::string user; };
struct RevokeRoleDef { std::string role; std::string user; bool force = false; };
struct ShowUsersDef {};
struct ShowRolesDef {};
struct ShowGrantsDef { Grantee who = Grantee::User; std::string name; std::optional<AsOfPoint> at; };   // at: Offset | Time
struct ShowSecurityHistoryDef { std::optional<Grantee> who; std::string name; std::optional<std::string> sinceIso; };

// ── create view (Checked Writes Part 08) ─────────────────────────────────────
// createView := "create" ["or" "replace"] ["transient"] ["unique"] "view" IDENT "key" keyspec "as" "select" items
//               "from" IDENT ("constraint" IDENT "check" "(" orExpr ")")* ";"
// keyspec    := keyItem | "(" keyItem ("," keyItem)* ")"
// keyItem    := IDENT | "lower" "(" IDENT ")" "as" IDENT
// items      := "*" | item ("," item)*
// item       := AGG "(" ("*" | IDENT) ")" "as" IDENT | IDENT ["as" IDENT]      AGG := count|sum|avg|min|max|last, only before "("
// An aggregate view is grouped by its key: there is no "group by" (the parser refuses it).
// orExpr := andExpr ("or" andExpr)*   andExpr := notExpr ("and" notExpr)*   notExpr := "not" notExpr | "(" orExpr ")" | IDENT CMP literal
// CMP := "=" | "<>" | "<" | "<=" | ">" | ">="          literal := ["-"] INT | STRING | "true" | "false"
// select := "select" ("*" | IDENT ("," IDENT)*) "from" IDENT "where" IDENT "=" selLit ("and" IDENT "=" selLit)* ["limit" INT] ";"
// selLit := literal | "lower" "(" STRING ")"            show := "show" ("topic" | "view") IDENT ";"
enum class KeyFunction : uint8_t { None = 0, Lower = 1 };          // the plan's keyFunction byte
struct KeyItem { std::string column; KeyFunction fn = KeyFunction::None; std::string outName; };   // outName = column when None
enum class ItemKind : uint8_t { Star, Column, CountAll, Count, Sum, Avg, Min, Max, Last };
struct SelectItem { ItemKind kind = ItemKind::Column; std::string column; std::string alias; };    // alias "" = none
// Sign and magnitude are kept apart so -9223372036854775808 and 18446744073709551615 both parse; the column range check is the planner's.
struct Literal { enum class Kind : uint8_t { Integer, String, Bool } kind = Kind::Integer;
                 bool negative = false; uint64_t magnitude = 0; std::string text; bool boolean = false;
                 bool lowered = false; };                                                        // lowered: lower('…'), select only
enum class PredOp : uint8_t { Column = 1, Const = 2, Eq = 3, Ne = 4, Lt = 5, Le = 6, Gt = 7, Ge = 8, And = 9, Or = 10, Not = 11 };   // = rune::view::PredicateOp values
struct PredNode { PredOp op = PredOp::Column; uint16_t a = 0, b = 0; std::string column; Literal value; };   // flat, root last
struct ConstraintDef { std::string name; std::vector<PredNode> predicate; };
struct CreateViewDef { std::string name; bool orReplace = false; bool transient = false; bool unique = false;
                       std::vector<KeyItem> key; std::vector<SelectItem> items; std::string source;
                       std::vector<ConstraintDef> constraints; };

struct WhereTerm { std::string column; Literal value; };
struct SelectDef { std::vector<std::string> columns; /* empty = * */ std::string view; std::vector<WhereTerm> where;
                   std::optional<uint32_t> limit; };
struct ShowTopicDef { std::string name; };
struct ShowViewDef  { std::string name; };

// ── lease statements (lease topics, spec section 5) ──────────────────────────
// show leases := "show" "leases" NAME ["where" IDENT "like" STRING] ["limit" INT] ";"   (keyColumn / prefix only with a where)
// show lease  := "show" "lease" NAME "key" KEYLIT ";"       revoke lease := "revoke" "lease" NAME "key" KEYLIT ";"
struct ShowLeasesDef { std::string topic; std::string keyColumn; std::optional<std::string> prefix; uint32_t limit = 100; };
struct ShowLeaseDef  { std::string topic; Literal key; };
struct RevokeLeaseDef { std::string topic; Literal key; };

// Security statements, then CreateViewDef, SelectDef, ShowTopicDef, ShowViewDef, then the three lease statements, are APPENDED: the existing indices never move.
using Statement = std::variant<TopicDef, AlterTopicDef, AlterTopicConfigDef, DropTopicDef, DropViewDef,
                               ShowHistoryDef, ShowAsOfDef, DiffDef,
                               CreateUserDef, AlterUserDef, DropUserDef, CreateRoleDef, DropRoleDef, GrantDef, RevokeDef,
                               GrantRoleDef, RevokeRoleDef, ShowUsersDef, ShowRolesDef, ShowGrantsDef,
                               ShowSecurityHistoryDef, CreateViewDef, SelectDef, ShowTopicDef, ShowViewDef,
                               ShowLeasesDef, ShowLeaseDef, RevokeLeaseDef>;

/// The canonical text of `s` with every password literal replaced by '<redacted>' (D10) — the ONLY statement
/// text a client sends for a security statement.  Keywords upper-case, names quoted when not a plain IDENT.
/// Catalog statements (topic / alter / drop topic|view / show history / as of / diff) are never re-rendered:
/// their text is the caller's source, so this returns an empty string for them.
[[nodiscard]] std::string redactedText(const Statement& s);
[[nodiscard]] bool isSecurityStatement(const Statement& s) noexcept;
[[nodiscard]] bool carriesPassword(const Statement& s) noexcept;
} // namespace rune::ql
