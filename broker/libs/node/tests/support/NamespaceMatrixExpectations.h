// NamespaceMatrixExpectations.h - the single source of truth for the namespaces interaction matrix (plan
// 2026-10-26-rune-namespaces, "Interaction matrix"; spec 2026-10-24 §8).  The 1-node matrix (NamespaceMatrixTest),
// the seeded stress test (NamespaceStressTest) and the 3-node slice (NamespaceTrioTest) all read it.
//
// The matrix is Op (20) x Caller (7) x Target (4) sequential cells, plus the timing cells (the 7 DDL ops x
// {ClusterAdmin, NsAdminSame} x OwnNs x the 4 non-sequential timings).  expect() evaluates the plan's rules in
// order; the comments at each rule record how the rule reads for the rig the tests build (NamespaceRig.h).
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

namespace rune::node::test::nsmatrix {

enum class Op : uint8_t {
    CreateNamespace, SetNamespaceLimits, ListNamespaces,
    CreateTopic, AlterTopic, DropTopic, Produce, Fetch, Metadata, GetTopicSchema,
    CreateView, DropView, GetViewRow,
    CreateUser, DropUser, CreateRole, Grant, Revoke,
    GroupJoin, TxnInit,
    kCount
};
enum class Caller : uint8_t {
    ClusterAdmin, NsAdminSame, NsAdminForeign, UserGranted, UserUngranted, UserForeign, Anonymous, kCount
};
enum class Target : uint8_t { OwnNs, ForeignNs, Missing, SameNameBoth, kCount };
enum class Timing : uint8_t { Sequential, BackToBack, ConcurrentSameName, LeaderChange, Restart, kCount };

inline constexpr std::size_t kOps = static_cast<std::size_t>(Op::kCount);
inline constexpr std::size_t kCallers = static_cast<std::size_t>(Caller::kCount);
inline constexpr std::size_t kTargets = static_cast<std::size_t>(Target::kCount);
inline constexpr std::size_t kTimings = static_cast<std::size_t>(Timing::kCount);

inline const char* name(Op o) {
    static constexpr const char* n[] = {"CreateNamespace", "SetNamespaceLimits", "ListNamespaces", "CreateTopic",
                                        "AlterTopic", "DropTopic", "Produce", "Fetch", "Metadata", "GetTopicSchema",
                                        "CreateView", "DropView", "GetViewRow", "CreateUser", "DropUser",
                                        "CreateRole", "Grant", "Revoke", "GroupJoin", "TxnInit"};
    return n[static_cast<std::size_t>(o)];
}
inline const char* name(Caller c) {
    static constexpr const char* n[] = {"ClusterAdmin", "NsAdminSame", "NsAdminForeign", "UserGranted",
                                        "UserUngranted", "UserForeign", "Anonymous"};
    return n[static_cast<std::size_t>(c)];
}
inline const char* name(Target t) {
    static constexpr const char* n[] = {"OwnNs", "ForeignNs", "Missing", "SameNameBoth"};
    return n[static_cast<std::size_t>(t)];
}
inline const char* name(Timing t) {
    static constexpr const char* n[] = {"Sequential", "BackToBack", "ConcurrentSameName", "LeaderChange", "Restart"};
    return n[static_cast<std::size_t>(t)];
}

// ── error codes the matrix names (rune::protocol::client::ErrorCode values) ──────────────────────────────────────
inline constexpr int kOk = 0;
inline constexpr int kUnknownTopic = 7;
inline constexpr int kTopicExists = 25;       ///< also CREATE NAMESPACE / CREATE USER / CREATE ROLE on an existing name
inline constexpr int kViewExists = 32;
inline constexpr int kViewSourceNotFound = 33;
inline constexpr int kInvalidRequest = 39;    ///< nsGuard: a security target in a namespace the caller cannot see
inline constexpr int kNotAuthorized = 45;
inline constexpr int kNsLimitExceeded = 52;

enum class Kind : uint8_t { NotApplicable, Ok, Missing, NotAuthorized, Exists, LimitExceeded, Refused };

/// An expected outcome.  `code` is the exact answer code for every kind but Missing, whose codes are
/// expectedCodes(op) (and whose bytes the test compares with the same op on a truly missing name).
struct Outcome {
    Kind kind = Kind::NotApplicable;
    int code = -1;
    static Outcome na() { return {Kind::NotApplicable, -1}; }
    static Outcome ok() { return {Kind::Ok, kOk}; }
    static Outcome missing() { return {Kind::Missing, -1}; }
    static Outcome notAuthorized() { return {Kind::NotAuthorized, kNotAuthorized}; }
    static Outcome exists(int c) { return {Kind::Exists, c}; }
    static Outcome refused(int c) { return {Kind::Refused, c}; }
    bool operator==(const Outcome&) const = default;
};
inline std::string describe(const Outcome& o) {
    static constexpr const char* k[] = {"n/a", "Ok", "Missing", "NotAuthorized", "Exists", "LimitExceeded", "Refused"};
    std::string s = k[static_cast<std::size_t>(o.kind)];
    if (o.kind != Kind::Missing && o.kind != Kind::NotApplicable) s += "(" + std::to_string(o.code) + ")";
    return s;
}

inline constexpr bool isNamespaceOp(Op o) {
    return o == Op::CreateNamespace || o == Op::SetNamespaceLimits || o == Op::ListNamespaces;
}
inline constexpr bool isSecurityOp(Op o) {
    return o == Op::CreateUser || o == Op::DropUser || o == Op::CreateRole || o == Op::Grant || o == Op::Revoke;
}
/// The ops that make a new object (rule 6: a Missing target is created when the caller is granted).
inline constexpr bool isCreateOp(Op o) {
    return o == Op::CreateNamespace || o == Op::CreateTopic || o == Op::CreateView || o == Op::CreateUser ||
           o == Op::CreateRole;
}
/// The ops addressed by an id (topic or view id): a missing or foreign id is refused with 45 (spec §5), not
/// "unknown", so a foreign id is indistinguishable from a missing one.
inline constexpr bool isIdOp(Op o) {
    return o == Op::Produce || o == Op::Fetch || o == Op::Metadata || o == Op::GetViewRow || o == Op::GroupJoin;
}
/// The read ops an anonymous session may reach in `default` (rule 2).  Describe-only (D-anon): the broker gives an
/// anonymous session DESCRIBE on `default` and nothing else, so Metadata and GetTopicSchema answer and Fetch /
/// GetViewRow are refused like any other ungranted op.
inline constexpr bool isAnonymousReadOp(Op o) {
    return o == Op::Metadata || o == Op::GetTopicSchema;
}
/// The 7 DDL ops the timing cells cover.
inline constexpr bool isTimingOp(Op o) {
    return o == Op::CreateNamespace || o == Op::SetNamespaceLimits || o == Op::CreateTopic || o == Op::DropTopic ||
           o == Op::CreateView || o == Op::CreateUser || o == Op::Grant;
}

/// The answer codes a Missing outcome may carry, per op (-1 pads).  Hidden-as-missing answers are byte-equal to
/// the truly-missing ones (the test compares them), so this set is the same for both.
///   - name-addressed topic / view DDL and schema reads: UnknownTopic;
///   - CreateView: a missing source or name is ViewSourceNotFound or UnknownTopic;
///   - id-addressed ops (Produce, Fetch, GetViewRow, GroupJoin): NotAuthorized (spec §5: a missing or foreign id);
///     Metadata answers OK with the id simply not listed (the test maps "not listed" to this code);
///   - security DDL: InvalidRequest ("no namespace '<asked>'" / no such principal);
///   - namespace ops: UnknownTopic (SetLimits on a missing namespace); TxnInit: NotAuthorized.
inline std::array<int, 3> expectedCodes(Op o) {
    switch (o) {
        case Op::CreateNamespace: case Op::SetNamespaceLimits: case Op::ListNamespaces: return {kUnknownTopic, -1, -1};
        case Op::CreateTopic: case Op::AlterTopic: case Op::DropTopic: case Op::GetTopicSchema: case Op::DropView:
            return {kUnknownTopic, -1, -1};
        case Op::CreateView: return {kUnknownTopic, kViewSourceNotFound, -1};
        case Op::Produce: case Op::Fetch: case Op::GetViewRow: case Op::GroupJoin: case Op::TxnInit:
        case Op::Metadata:
            return {kNotAuthorized, -1, -1};
        case Op::CreateUser: case Op::DropUser: case Op::CreateRole: case Op::Grant: case Op::Revoke:
            return {kInvalidRequest, -1, -1};
        case Op::kCount: break;
    }
    return {-1, -1, -1};
}
inline bool isExpectedMissingCode(Op o, int code) {
    for (const int c : expectedCodes(o)) if (c >= 0 && c == code) return true;
    return false;
}
/// The cluster admin holds every permission bit, so a missing id is not refused (spec: cross-namespace access by
/// id is closed by permissions only): it gets the path's own unknown answer.  Nothing is hidden from root.
inline constexpr int kUnknownView = 4;   ///< GetViewRowResponse status
inline bool isExpectedMissingCode(Op o, Caller c, int code) {
    if (c == Caller::ClusterAdmin) {
        if (o == Op::Produce || o == Op::Fetch) return code == kUnknownTopic;
        if (o == Op::GetViewRow) return code == kUnknownView;
    }
    return isExpectedMissingCode(o, code);
}

/// Rule 1: SameNameBoth is not applicable to the namespace ops (a namespace has no "same local name").
inline constexpr bool naRule(Op o, Target t) { return t == Target::SameNameBoth && isNamespaceOp(o); }

/// "Today's result" of an op on an existing object by a caller allowed on it: the create ops answer Exists (the
/// rig names an existing object), every other op is Ok (the rig gives drop / revoke / alter a fresh per-cell object).
inline Outcome todays(Op o) {
    switch (o) {
        case Op::CreateNamespace: case Op::CreateTopic: return Outcome::exists(kTopicExists);
        // The security DDL's convention (auth phase): a duplicate principal is InvalidRequest with a
        // "UserExists" / "RoleExists" detail.  Only a duplicate namespace reuses TopicExists (plan, codes).
        case Op::CreateUser: case Op::CreateRole: return Outcome::exists(kInvalidRequest);
        case Op::CreateView: return Outcome::exists(kViewExists);
        default: return Outcome::ok();
    }
}
/// Whether the caller holds the op's grant in its own namespace (the rig's grant sets, NamespaceRig.h).  nsGuard
/// gives the security DDL to the cluster admin and a namespace admin only, so UserGranted is not granted on them.
inline constexpr bool granted(Op o, Caller c) {
    switch (c) {
        case Caller::ClusterAdmin: case Caller::NsAdminSame: case Caller::NsAdminForeign: return true;
        case Caller::UserGranted: case Caller::UserForeign: return !isSecurityOp(o);
        default: return false;
    }
}

/// The expected outcome of one cell.  The rules are the plan's, in its order.
inline Outcome expect(Op o, Caller c, Target t, Timing = Timing::Sequential) {
    // 1. not applicable
    if (naRule(o, t)) return Outcome::na();
    const bool root = c == Caller::ClusterAdmin;
    // 2. Anonymous (RUNE_ANONYMOUS_READ on; reaches `default` only; the namespace under test is `default`, so
    //    OwnNs and SameNameBoth both name a `default` object).  Only the read ops are reachable at all: a write or
    //    DDL is refused before any name resolution.
    if (c == Caller::Anonymous) {
        if (!isAnonymousReadOp(o)) return Outcome::notAuthorized();
        if (t == Target::ForeignNs || t == Target::Missing) return Outcome::missing();
        return Outcome::ok();
    }
    // 3. namespace ops (the target is the namespace itself; OwnNs = `a`, ForeignNs = `b`, Missing = a fresh name)
    if (isNamespaceOp(o)) {
        if (o == Op::ListNamespaces) return Outcome::ok();   // filtered: exactly the caller's namespace (root: all)
        if (!root) return Outcome::notAuthorized();
        if (o == Op::CreateNamespace) return t == Target::Missing ? Outcome::ok() : Outcome::exists(kTopicExists);
        return t == Target::Missing ? Outcome::missing() : Outcome::ok();
    }
    // 4. groups and transactions (Phase 2 Task 5, D19: they work in every namespace).  A transactional id is a name
    //    resolved in the caller's namespace (a qualifier the caller cannot see is an opaque id of its own), so
    //    InitProducerId answers Ok for every caller holding the write grant, whatever the target.  A join is
    //    id-addressed (its topic id): the general rules apply, except that root's join names a topic id it does not
    //    check (a member may subscribe before a topic exists, as in Kafka), so it answers Ok on a Missing target.
    if (o == Op::TxnInit) return granted(o, c) ? Outcome::ok() : Outcome::notAuthorized();
    if (o == Op::GroupJoin && t == Target::Missing && root) return Outcome::ok();
    // 5. ForeignNs: root and the target namespace's own admin (logged into it, naming it unqualified)
    if (t == Target::ForeignNs) {
        if (root || c == Caller::NsAdminSame) return todays(o);
        // A data op checks the caller's own grants on the LOCAL name before the name resolves, so an ungranted
        // caller is refused for a foreign namespace exactly as for a missing one.  The security DDL checks the
        // namespace first (nsGuard), so it answers Missing either way.
        if (!granted(o, c) && !isSecurityOp(o)) return Outcome::notAuthorized();
        return Outcome::missing();
    }
    // 6. Missing: created when granted; otherwise missing.  The missing name lives in the namespace under test, so a
    //    caller from another namespace sees a hidden namespace: Missing even for a create.
    if (t == Target::Missing) {
        if (c == Caller::NsAdminForeign || c == Caller::UserForeign) return Outcome::missing();
        if (!granted(o, c)) return Outcome::notAuthorized();
        return isCreateOp(o) ? Outcome::ok() : Outcome::missing();
    }
    // 7. OwnNs / SameNameBoth
    if (c == Caller::UserUngranted) return Outcome::notAuthorized();
    if (c == Caller::NsAdminForeign || c == Caller::UserForeign) {
        if (t == Target::OwnNs) return Outcome::missing();
        // SameNameBoth: the caller's own namespace's object (the test checks its content)
    }
    if (!granted(o, c)) return Outcome::notAuthorized();
    return todays(o);
}

/// Whether (op, caller, target, timing) is one of the 56 timing cells.
inline constexpr bool isTimingCell(Op o, Caller c, Target t, Timing tm) {
    return tm != Timing::Sequential && isTimingOp(o) && (c == Caller::ClusterAdmin || c == Caller::NsAdminSame) &&
           t == Target::OwnNs;
}

struct Counts { std::size_t sequential = 0, na = 0, applicable = 0, timing = 0, total = 0; };
inline Counts countCells() {
    Counts k;
    for (std::size_t o = 0; o < kOps; ++o)
        for (std::size_t c = 0; c < kCallers; ++c)
            for (std::size_t t = 0; t < kTargets; ++t) {
                ++k.sequential;
                if (naRule(Op(o), Target(t))) ++k.na; else ++k.applicable;
                for (std::size_t tm = 1; tm < kTimings; ++tm)
                    if (isTimingCell(Op(o), Caller(c), Target(t), Timing(tm))) ++k.timing;
            }
    k.total = k.applicable + k.timing;
    return k;
}

}  // namespace rune::node::test::nsmatrix
