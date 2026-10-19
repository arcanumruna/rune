#pragma once
// AuthMatrixExpectations.h — authentication spec §11: the request × grant × timing matrix (720 cells).
// Read by the unit matrix (Task 17), the stress (Task 18), the 3-node slice (Task 19) and the docker e2e (Task 20).
#include <array>
#include <cstdint>
#include <optional>
#include <string_view>

namespace rune::node::test::amatrix {

enum class Op : uint8_t { Produce, ProduceRouted, TxnProduce, TxnInit, TxnEnd, Subscribe, GroupJoin, OffsetCommit,
                          OffsetFetch, ViewRead, ViewOpen, Metadata, CreateTopic, CreateView, CreateSchema, AlterSchema,
                          SetCompatibility, AlterTopicConfig, AlterView, DropTopic, DropView, CatalogRead, SecurityDdl,
                          SecurityRead, kCount };
enum class Grant : uint8_t { None, Exact, Prefix, ViaRole, Cluster, kCount };
enum class Timing : uint8_t { Steady, RevokedMid, CreatedAfterLogin, GrantedAfterLogin, ObjectMissing, UserDisabledMid, kCount };
enum class Outcome : uint8_t { NotApplicable, Denied, FilteredOut, Missing, EndedNotAuthorized, SessionRevoked, DeniedThenOk, Ok };
enum class NaRule : uint8_t { None, NA1, NA2, NA3, NA4, NA5, NA6 };
inline constexpr int16_t kNone = -1;

[[nodiscard]] constexpr bool needsReadOrWrite(Op o) noexcept { return static_cast<uint8_t>(o) <= static_cast<uint8_t>(Op::ViewOpen); }
[[nodiscard]] constexpr bool clusterOnly(Op o) noexcept { return o == Op::SecurityDdl || o == Op::SecurityRead; }
[[nodiscard]] constexpr bool noTarget(Op o) noexcept { return o == Op::CatalogRead || clusterOnly(o); }
[[nodiscard]] constexpr bool streaming(Op o) noexcept {
    return o == Op::Subscribe || o == Op::GroupJoin || o == Op::CatalogRead || o == Op::SecurityRead;
}

[[nodiscard]] constexpr NaRule naRule(Op o, Grant g, Timing t) noexcept {
    if (g == Grant::Cluster && needsReadOrWrite(o)) return NaRule::NA1;
    if ((g == Grant::Exact || g == Grant::Prefix) && clusterOnly(o)) return NaRule::NA2;
    if (g == Grant::None && (t == Timing::RevokedMid || t == Timing::GrantedAfterLogin || t == Timing::UserDisabledMid)) return NaRule::NA3;
    if ((t == Timing::CreatedAfterLogin || t == Timing::ObjectMissing) && noTarget(o)) return NaRule::NA4;
    if (t == Timing::CreatedAfterLogin && (o == Op::CreateTopic || o == Op::CreateView)) return NaRule::NA5;
    if (t == Timing::ObjectMissing && (o == Op::TxnEnd || o == Op::CreateSchema)) return NaRule::NA6;
    return NaRule::None;
}

/// R3's per-op answer for a granted request on a missing object.
struct MissingAnswer { Outcome outcome; int16_t code; int16_t viewStatus; };
[[nodiscard]] constexpr MissingAnswer missingAnswer(Op o) noexcept {
    switch (o) {
        case Op::Produce: case Op::ProduceRouted: case Op::TxnProduce: case Op::Subscribe: case Op::ViewRead:
            return {Outcome::Denied, 45, kNone};                 // id-addressed (D22)
        case Op::TxnInit: case Op::GroupJoin: case Op::OffsetCommit: case Op::OffsetFetch:
            return {Outcome::Ok, 0, kNone};                      // implicit objects
        case Op::ViewOpen: return {Outcome::Missing, 0, 4};      // ViewRowResponse status 4 UnknownView
        case Op::Metadata: return {Outcome::FilteredOut, 0, kNone};
        case Op::CreateTopic: case Op::CreateView: return {Outcome::Ok, 0, kNone};
        default: return {Outcome::Missing, 7, kNone};             // UnknownTopic (name-addressed alter / drop)
    }
}

[[nodiscard]] constexpr Outcome expect(Op o, Grant g, Timing t) noexcept {
    if (naRule(o, g, t) != NaRule::None) return Outcome::NotApplicable;
    if (g == Grant::None) return o == Op::Metadata ? Outcome::FilteredOut : Outcome::Denied;                // R1
    if (o == Op::CatalogRead && (g == Grant::Exact || g == Grant::Prefix)) return Outcome::Denied;           // R2
    if (t == Timing::ObjectMissing) return missingAnswer(o).outcome;                                        // R3
    if (t == Timing::RevokedMid) return Outcome::EndedNotAuthorized;                                        // R4
    if (t == Timing::UserDisabledMid) return Outcome::SessionRevoked;                                       // R5
    if (t == Timing::GrantedAfterLogin) return Outcome::DeniedThenOk;                                       // R6
    return Outcome::Ok;                                                                                     // R7
}

/// Every code a cell asserts.  first = the first answer; second = the answer after the change (RevokedMid,
/// GrantedAfterLogin, UserDisabledMid) or kNone; reconnect = the code of a reconnect attempt (UserDisabledMid) or kNone.
struct ExpectedCodes { int16_t first = kNone; int16_t second = kNone; int16_t reconnect = kNone; int16_t viewStatus = kNone;
                       bool presentFirst = false, presentSecond = false; };   // Metadata presence
[[nodiscard]] constexpr ExpectedCodes expectedCodes(Op o, Grant g, Timing t) noexcept {
    const Outcome out = expect(o, g, t);
    const bool meta = o == Op::Metadata;
    switch (out) {
        case Outcome::NotApplicable:      return {};
        case Outcome::Denied:             return {.first = meta ? int16_t{0} : int16_t{45}};
        case Outcome::FilteredOut:        return {.first = 0, .presentFirst = false};
        case Outcome::Missing:            { const auto m = missingAnswer(o); return {.first = m.code, .viewStatus = m.viewStatus}; }
        case Outcome::EndedNotAuthorized: return {.first = 0, .second = meta ? int16_t{0} : int16_t{45}, .presentFirst = meta, .presentSecond = false};
        case Outcome::SessionRevoked:     return {.first = 0, .second = 46, .reconnect = 44, .presentFirst = meta};
        case Outcome::DeniedThenOk:       return {.first = meta ? int16_t{0} : int16_t{45}, .second = 0, .presentFirst = false, .presentSecond = meta};
        case Outcome::Ok:                 return {.first = 0, .presentFirst = meta};
    }
    return {};
}

[[nodiscard]] constexpr std::string_view name(Op v) noexcept {
    switch (v) {
        case Op::Produce: return "Produce";
        case Op::ProduceRouted: return "ProduceRouted";
        case Op::TxnProduce: return "TxnProduce";
        case Op::TxnInit: return "TxnInit";
        case Op::TxnEnd: return "TxnEnd";
        case Op::Subscribe: return "Subscribe";
        case Op::GroupJoin: return "GroupJoin";
        case Op::OffsetCommit: return "OffsetCommit";
        case Op::OffsetFetch: return "OffsetFetch";
        case Op::ViewRead: return "ViewRead";
        case Op::ViewOpen: return "ViewOpen";
        case Op::Metadata: return "Metadata";
        case Op::CreateTopic: return "CreateTopic";
        case Op::CreateView: return "CreateView";
        case Op::CreateSchema: return "CreateSchema";
        case Op::AlterSchema: return "AlterSchema";
        case Op::SetCompatibility: return "SetCompatibility";
        case Op::AlterTopicConfig: return "AlterTopicConfig";
        case Op::AlterView: return "AlterView";
        case Op::DropTopic: return "DropTopic";
        case Op::DropView: return "DropView";
        case Op::CatalogRead: return "CatalogRead";
        case Op::SecurityDdl: return "SecurityDdl";
        case Op::SecurityRead: return "SecurityRead";
        default: return "?";
    }
}
[[nodiscard]] constexpr std::string_view name(Grant v) noexcept {
    switch (v) {
        case Grant::None: return "None";
        case Grant::Exact: return "Exact";
        case Grant::Prefix: return "Prefix";
        case Grant::ViaRole: return "ViaRole";
        case Grant::Cluster: return "Cluster";
        default: return "?";
    }
}
[[nodiscard]] constexpr std::string_view name(Timing v) noexcept {
    switch (v) {
        case Timing::Steady: return "Steady";
        case Timing::RevokedMid: return "RevokedMid";
        case Timing::CreatedAfterLogin: return "CreatedAfterLogin";
        case Timing::GrantedAfterLogin: return "GrantedAfterLogin";
        case Timing::ObjectMissing: return "ObjectMissing";
        case Timing::UserDisabledMid: return "UserDisabledMid";
        default: return "?";
    }
}
[[nodiscard]] constexpr std::string_view name(Outcome v) noexcept {
    switch (v) {
        case Outcome::NotApplicable: return "NotApplicable";
        case Outcome::Denied: return "Denied";
        case Outcome::FilteredOut: return "FilteredOut";
        case Outcome::Missing: return "Missing";
        case Outcome::EndedNotAuthorized: return "EndedNotAuthorized";
        case Outcome::SessionRevoked: return "SessionRevoked";
        case Outcome::DeniedThenOk: return "DeniedThenOk";
        case Outcome::Ok: return "Ok";
        default: return "?";
    }
}
[[nodiscard]] constexpr std::string_view name(NaRule v) noexcept {
    switch (v) {
        case NaRule::None: return "None";
        case NaRule::NA1: return "NA1";
        case NaRule::NA2: return "NA2";
        case NaRule::NA3: return "NA3";
        case NaRule::NA4: return "NA4";
        case NaRule::NA5: return "NA5";
        case NaRule::NA6: return "NA6";
        default: return "?";
    }
}

} // namespace rune::node::test::amatrix
