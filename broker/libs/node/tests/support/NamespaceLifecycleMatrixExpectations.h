// NamespaceLifecycleMatrixExpectations.h - the single source of truth for the namespace LIFECYCLE interaction matrix
// (plan 2026-10-26-rune-namespace-lifecycle-limits, "Interaction matrix"; spec 2026-10-24 §8).  The 1-node matrix
// (NamespaceLifecycleMatrixTest), the seeded stress test (NamespaceStressTest) and the 3-node slice
// (NamespaceLifecycleTrioRegressionTest) all read it.
//
//   9 ops x 6 callers x 6 targets = 324 sequential cells, 24 not applicable (Recreate applies only to Deleting and
//   Missing), 300 applicable; plus 36 timing cells; 336 executed.
//
// expect() evaluates the plan's rules in this order:
//   1. naRule;
//   2. login time: on SuspendedRA a NsAdminSame / UserInNs cannot log in (NamespaceSuspended); on Deleting they get
//      AuthenticationFailed; the op is never sent;
//   3. every caller other than ClusterAdmin gets NotAuthorized, checked before any lookup (a non-admin learns nothing
//      about which names exist);
//   4. ClusterAdmin: per op, below.
#pragma once

#include <rune/protocol/client/Protocol.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>

namespace rune::node::test::nslc {

using rune::protocol::client::ErrorCode;

enum class LcOp : uint8_t { SuspendRO, SuspendRA, SwitchMode, Resume, Delete, Recreate,
                            SetLimitsRaise, SetLimitsLowerBelowUsage, SetLimitsLowerRetention };   // 9
enum class LcCaller : uint8_t { ClusterAdmin, NsAdminSame, NsAdminForeign, UserInNs, UserForeign, Anonymous };  // 6
enum class LcTarget : uint8_t { Active, SuspendedRO, SuspendedRA, Deleting, Missing, Default };    // 6
enum class LcTiming : uint8_t { Sequential, BackToBack, CrashMidDelete, LeaderChangeMidDelete,
                                FollowerCatchUp, LimitChangeMidTick, RecreateUnderThrottle, TxnOpen };

inline constexpr std::size_t kLcOps = 9, kLcCallers = 6, kLcTargets = 6;

struct LcOutcome {
    enum Kind : uint8_t { Ok, OkWithWarning, Missing, Refused } kind;
    ErrorCode code;     ///< exact for Ok (OK), OkWithWarning (OK) and Refused; Missing: the answer for an unknown name
    LcTarget after;     ///< the target state a follow-up ListNamespaces (cluster admin) shows, by name
};

inline const char* name(LcOp o) {
    static constexpr const char* n[] = {"SuspendRO", "SuspendRA", "SwitchMode", "Resume", "Delete", "Recreate",
                                        "SetLimitsRaise", "SetLimitsLowerBelowUsage", "SetLimitsLowerRetention"};
    return n[static_cast<std::size_t>(o)];
}
inline const char* name(LcCaller c) {
    static constexpr const char* n[] = {"ClusterAdmin", "NsAdminSame", "NsAdminForeign", "UserInNs", "UserForeign",
                                        "Anonymous"};
    return n[static_cast<std::size_t>(c)];
}
inline const char* name(LcTarget t) {
    static constexpr const char* n[] = {"Active", "SuspendedRO", "SuspendedRA", "Deleting", "Missing", "Default"};
    return n[static_cast<std::size_t>(t)];
}
inline const char* name(LcTiming t) {
    static constexpr const char* n[] = {"Sequential", "BackToBack", "CrashMidDelete", "LeaderChangeMidDelete",
                                        "FollowerCatchUp", "LimitChangeMidTick", "RecreateUnderThrottle", "TxnOpen"};
    return n[static_cast<std::size_t>(t)];
}
inline std::string describe(const LcOutcome& o) {
    static constexpr const char* k[] = {"Ok", "OkWithWarning", "Missing", "Refused"};
    std::string s = k[o.kind];
    if (o.kind == LcOutcome::Refused) s += "(" + std::to_string(static_cast<int>(o.code)) + ")";
    return s + " after=" + name(o.after);
}

/// Rule 1: Recreate (Create of the same name) applies only to a Deleting or a Missing name.
inline constexpr bool naRule(LcOp o, LcCaller, LcTarget t) {
    return o == LcOp::Recreate && t != LcTarget::Deleting && t != LcTarget::Missing;
}

inline constexpr bool isSuspended(LcTarget t) { return t == LcTarget::SuspendedRO || t == LcTarget::SuspendedRA; }
/// The targets whose name resolves to a live namespace (ClusterAdmin ops find it).
inline constexpr bool isLive(LcTarget t) { return t == LcTarget::Active || isSuspended(t) || t == LcTarget::Default; }

/// The state a SwitchMode lands in (RO <-> RA; RA from Active).
inline constexpr LcTarget switchedTo(LcTarget t) {
    return t == LcTarget::SuspendedRA ? LcTarget::SuspendedRO : LcTarget::SuspendedRA;
}

inline LcOutcome expect(LcOp o, LcCaller c, LcTarget t) {
    // 1. not applicable
    if (naRule(o, c, t)) return {LcOutcome::Refused, ErrorCode::InvalidRequest, t};
    // 2. login time
    if ((c == LcCaller::NsAdminSame || c == LcCaller::UserInNs)) {
        if (t == LcTarget::SuspendedRA) return {LcOutcome::Refused, ErrorCode::NamespaceSuspended, t};
        if (t == LcTarget::Deleting) return {LcOutcome::Refused, ErrorCode::AuthenticationFailed, t};
    }
    // 3. only the cluster admin runs a lifecycle op, checked before any lookup
    if (c != LcCaller::ClusterAdmin) return {LcOutcome::Refused, ErrorCode::NotAuthorized, t};
    // 4. ClusterAdmin
    if (!isLive(t) && o != LcOp::Recreate) return {LcOutcome::Missing, ErrorCode::UnknownTopic, t};
    switch (o) {
        case LcOp::SuspendRO: return {LcOutcome::Ok, ErrorCode::OK, LcTarget::SuspendedRO};
        case LcOp::SuspendRA: return {LcOutcome::Ok, ErrorCode::OK, LcTarget::SuspendedRA};
        case LcOp::SwitchMode: return {LcOutcome::Ok, ErrorCode::OK, switchedTo(t)};
        case LcOp::Resume: return {LcOutcome::Ok, ErrorCode::OK, t == LcTarget::Default ? LcTarget::Default : LcTarget::Active};
        case LcOp::Delete:
            if (t == LcTarget::Default) return {LcOutcome::Refused, ErrorCode::InvalidRequest, LcTarget::Default};
            return {LcOutcome::Ok, ErrorCode::OK, LcTarget::Missing};
        case LcOp::Recreate: return {LcOutcome::Ok, ErrorCode::OK, LcTarget::Active};
        case LcOp::SetLimitsRaise:
        case LcOp::SetLimitsLowerBelowUsage: return {LcOutcome::Ok, ErrorCode::OK, t};
        case LcOp::SetLimitsLowerRetention: return {LcOutcome::OkWithWarning, ErrorCode::OK, t};
    }
    return {LcOutcome::Refused, ErrorCode::Internal, t};
}

/// Every code the matrix can emit.
inline std::span<const ErrorCode> expectedCodes() {
    static constexpr std::array<ErrorCode, 8> k{ErrorCode::OK, ErrorCode::UnknownTopic, ErrorCode::TopicExists,
                                                ErrorCode::InvalidRequest, ErrorCode::AuthenticationFailed,
                                                ErrorCode::NotAuthorized, ErrorCode::NamespaceSuspended,
                                                ErrorCode::NamespaceLimitExceeded};
    return k;
}

// ── the 36 timing cells ───────────────────────────────────────────────────────────────────────────────────────────
/// How many variants of each timing (ClusterAdmin, with a seeded workload in the target namespace).
inline constexpr std::size_t timingVariants(LcTiming t) {
    switch (t) {
        case LcTiming::Sequential: return 0;
        case LcTiming::BackToBack: return 9;             // one per op
        case LcTiming::CrashMidDelete: return 7;         // one per delete step
        case LcTiming::LeaderChangeMidDelete: return 7;  // the same seven points
        case LcTiming::FollowerCatchUp: return 3;        // suspend, delete, recreate
        case LcTiming::LimitChangeMidTick: return 5;     // one per rate
        case LcTiming::RecreateUnderThrottle: return 2;  // a worker ring, the gateway
        case LcTiming::TxnOpen: return 3;                // SuspendRO, SuspendRA, Delete
    }
    return 0;
}

/// The seven points of a delete at which a crash / leader change lands.  `reaperEvents` = how many catalog events the
/// reaper may issue before it is stopped (the standard workload: 2 topics, no views = step 1, drop, drop, steps 2, 4, 8):
///   0 Deleted applied, sessions fenced   1 txns aborted (step 1)        2 first object dropped
///   3 objects dropped                    4 groups tombstoned (step 2)   5 txn state tombstoned (step 4)
///   6 security purged (step 8): the namespace is erased, its directory renamed and its slot freed.
inline constexpr int kDeletePoints = 7;
inline const char* deletePointName(int p) {
    static constexpr const char* n[] = {"DeletedApplied", "TxnsAborted", "FirstObjectDropped", "ObjectsDropped",
                                        "GroupsTombstoned", "TxnStateTombstoned", "SecurityPurged"};
    return n[p];
}
inline constexpr int reaperEventsAtPoint(int p) { return p; }

/// The five rates a LimitChangeMidTick cell covers, in NamespaceLimits field order.
inline const char* rateField(int i) {
    static constexpr const char* n[] = {"produce-bytes-per-sec", "produce-records-per-sec", "fetch-bytes-per-sec",
                                        "request-ops-per-sec", "query-cycles-per-sec"};
    return n[i];
}

/// BackToBack: the same op sent twice (distinct request ids) before the first reply; the second sees the state the
/// first left.  Target: Active for every op but Recreate (Missing).
inline LcOutcome backToBackSecond(LcOp o) {
    switch (o) {
        case LcOp::Delete: return {LcOutcome::Missing, ErrorCode::UnknownTopic, LcTarget::Missing};
        case LcOp::Recreate: return {LcOutcome::Refused, ErrorCode::TopicExists, LcTarget::Active};
        default: return expect(o, LcCaller::ClusterAdmin, LcTarget::Active);
    }
}

struct LcCounts { std::size_t cells = 0, na = 0, applicable = 0, timing = 0, executed = 0; };
inline LcCounts countCells() {
    LcCounts k;
    for (std::size_t o = 0; o < kLcOps; ++o)
        for (std::size_t c = 0; c < kLcCallers; ++c)
            for (std::size_t t = 0; t < kLcTargets; ++t) {
                ++k.cells;
                if (naRule(LcOp(o), LcCaller(c), LcTarget(t))) ++k.na; else ++k.applicable;
            }
    for (const LcTiming tm : {LcTiming::BackToBack, LcTiming::CrashMidDelete, LcTiming::LeaderChangeMidDelete,
                              LcTiming::FollowerCatchUp, LcTiming::LimitChangeMidTick, LcTiming::RecreateUnderThrottle,
                              LcTiming::TxnOpen})
        k.timing += timingVariants(tm);
    k.executed = k.applicable + k.timing;
    return k;
}

}  // namespace rune::node::test::nslc
