#pragma once
// CatalogMatrixExpectations.h — catalog history plan: THE interaction-matrix table.
// Kind x Entity x Timing -> Outcome.  The unit matrix, the seeded stress, the
// 3-node slice and the docker e2e read this; the plan's markdown table is a copy.
#include <cstdint>
#include <string_view>

namespace rune::node::test::cmatrix {

enum class Kind : uint8_t { CreateTopic, DropTopic, AlterTopicConfig, CreateSchema, AlterSchema, SetCompatibility,
                            CreateView, AlterView, DropView };
enum class Via : uint8_t { Leader, Follower };
enum class Persist : uint8_t { Disk, Transient };
struct Entity { Via via; Persist persist; };
enum class Timing : uint8_t { Steady, LeaderChangeMidDdl, RestartMidApply, NewBrokerJoins, CatalogBelowMinIsr,
                              ConcurrentDdlDifferentBrokers, DropAndReuseName, CreateRacesPlacement,
                              FingerprintMismatch };
enum class Outcome : uint8_t { NotApplicable, Ok, RedirectThenOk, NotLeaderThenOkOnce, NotLeaderUntilBudgetThenOk,
                               OneWinsOneConflict, OkNewObjectId, OkAfterPlacement, HaltedBrokerInternalOthersOk };
inline constexpr int kKinds = 9, kEntities = 4, kTimings = 9;

[[nodiscard]] constexpr bool createsATopic(Kind k) noexcept {
    return k == Kind::CreateTopic || k == Kind::CreateView || k == Kind::AlterView;
}
[[nodiscard]] constexpr int naRule(Kind k, Entity, Timing t) noexcept {
    return (t == Timing::CreateRacesPlacement && !createsATopic(k)) ? 1 : 0;   // R1
}
[[nodiscard]] constexpr Outcome expect(Kind k, Entity e, Timing t) noexcept {
    if (naRule(k, e, t) != 0) return Outcome::NotApplicable;                          // R1
    switch (t) {
    case Timing::FingerprintMismatch:           return Outcome::HaltedBrokerInternalOthersOk;   // R2
    case Timing::CatalogBelowMinIsr:            return Outcome::NotLeaderUntilBudgetThenOk;     // R3
    case Timing::LeaderChangeMidDdl:            return Outcome::NotLeaderThenOkOnce;            // R4
    case Timing::ConcurrentDdlDifferentBrokers: return Outcome::OneWinsOneConflict;             // R5
    case Timing::DropAndReuseName:              return Outcome::OkNewObjectId;                  // R6
    case Timing::CreateRacesPlacement:          return Outcome::OkAfterPlacement;               // R7
    case Timing::Steady: case Timing::RestartMidApply: case Timing::NewBrokerJoins:
        return e.via == Via::Follower ? Outcome::RedirectThenOk : Outcome::Ok;                  // R8 / R9
    }
    return Outcome::NotApplicable;
}
/// R5: the loser's code.
[[nodiscard]] constexpr uint16_t conflictCode(Kind k) noexcept {
    switch (k) {
    case Kind::CreateTopic: case Kind::CreateSchema: return 25;   // TopicExists
    case Kind::CreateView:                           return 32;   // ViewExists
    case Kind::DropTopic: case Kind::DropView:       return 7;    // UnknownTopic
    default:                                         return 31;   // SchemaConflict (expectedVersion)
    }
}

/// Every answer code a cell asserts (F15/F16) — the ONLY place a matrix code is written down.
///   first          the first answer to the cell's DDL
///   afterRedirect  the first answer after following a Redirect (-1: the first answer was not a Redirect)
///   final          the client-visible terminal result of the cell's DDL
///   other          R5: the concurrent DDL's terminal result
///   viaLeader      R2: the same op sent via the leader
///   retried        R3: the op retried after one broker returned
/// A DDL sent via a follower answers Redirect (3) first in every rule except R2 (broker 3 is halted
/// and answers Internal, never a redirect).  The DDL sent to the leader is applied first (R5).
struct ExpectedCodes { int first, afterRedirect, final, other, viaLeader, retried; };
[[nodiscard]] constexpr ExpectedCodes expectedCodes(Kind k, Entity e, Timing t) noexcept {
    const bool viaF = e.via == Via::Follower;   // a follower answers Redirect (3) first
    auto viaLeaderThen = [&](int leaderAnswer, int final) {
        return viaF ? ExpectedCodes{3, leaderAnswer, final, -1, -1, -1} : ExpectedCodes{leaderAnswer, -1, final, -1, -1, -1};
    };
    switch (expect(k, e, t)) {
    case Outcome::NotApplicable:                return {-1, -1, -1, -1, -1, -1};
    case Outcome::HaltedBrokerInternalOthersOk: return {6, -1, 6, -1, 0, -1};                       // R2
    case Outcome::NotLeaderUntilBudgetThenOk: {                                                    // R3
        ExpectedCodes c = viaLeaderThen(2, 2); c.retried = 0; return c;
    }
    case Outcome::NotLeaderThenOkOnce:          return viaLeaderThen(2, 0);                         // R4
    case Outcome::OneWinsOneConflict: {                                                            // R5
        const int conflict = conflictCode(k);
        return viaF ? ExpectedCodes{3, conflict, conflict, 0, -1, -1} : ExpectedCodes{0, -1, 0, conflict, -1, -1};
    }
    case Outcome::OkNewObjectId: case Outcome::OkAfterPlacement:                                   // R6 / R7
    case Outcome::RedirectThenOk: case Outcome::Ok:                                                // R8 / R9
        return viaLeaderThen(0, 0);
    }
    return {-1, -1, -1, -1, -1, -1};
}
/// M4: the response TEMPLATE of a cell's first answer (named like CatalogDdlRig::Answer::Kind; the test maps them).
/// AdminWorker::respond_ keys on the REQUEST's template id: DefineTopic -> Define, AlterTopic -> Alter,
/// CreateView -> CreateView, every other kind -> Ddl.  The gateway's own answers (Redirect, the halted broker's
/// Internal) are plain ErrorResponses.
enum class Template : uint8_t { Define, Alter, CreateView, Ddl, Error };
[[nodiscard]] constexpr Template adminTemplate(Kind k) noexcept {
    switch (k) {
    case Kind::CreateTopic: case Kind::CreateSchema:      return Template::Define;
    case Kind::AlterSchema: case Kind::SetCompatibility:  return Template::Alter;
    case Kind::CreateView:                                return Template::CreateView;
    case Kind::DropTopic: case Kind::AlterTopicConfig: case Kind::AlterView: case Kind::DropView: return Template::Ddl;
    }
    return Template::Ddl;
}
[[nodiscard]] constexpr Template expectedFirstTemplate(Kind k, Entity e, Timing t) noexcept {
    if (expect(k, e, t) == Outcome::NotApplicable) return Template::Error;
    // A follower answers Redirect (gateway); the halted broker of R2 answers Internal (gateway).  Every other
    // first answer is the admin worker's, in the request's template.
    if (t == Timing::FingerprintMismatch || e.via == Via::Follower) return Template::Error;
    return adminTemplate(k);
}
[[nodiscard]] constexpr std::string_view name(Kind k) noexcept {
    constexpr std::string_view n[] = {"CreateTopic", "DropTopic", "AlterTopicConfig", "CreateSchema", "AlterSchema",
                                      "SetCompatibility", "CreateView", "AlterView", "DropView"};
    return n[static_cast<int>(k)];
}
[[nodiscard]] constexpr std::string_view name(Timing t) noexcept {
    constexpr std::string_view n[] = {"Steady", "LeaderChangeMidDdl", "RestartMidApply", "NewBrokerJoins",
                                      "CatalogBelowMinIsr", "ConcurrentDdlDifferentBrokers", "DropAndReuseName",
                                      "CreateRacesPlacement", "FingerprintMismatch"};
    return n[static_cast<int>(t)];
}
[[nodiscard]] constexpr std::string_view name(Outcome o) noexcept {
    constexpr std::string_view n[] = {"NotApplicable", "Ok", "RedirectThenOk", "NotLeaderThenOkOnce",
                                      "NotLeaderUntilBudgetThenOk", "OneWinsOneConflict", "OkNewObjectId",
                                      "OkAfterPlacement", "HaltedBrokerInternalOthersOk"};
    return n[static_cast<int>(o)];
}

} // namespace rune::node::test::cmatrix
