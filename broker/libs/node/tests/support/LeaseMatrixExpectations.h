#pragma once
// LeaseMatrixExpectations.h - lease topics Task 17: the single source of truth of the lease interaction matrix (spec §21, plan
// "Interaction matrix").  The unit table (LeaseMatrixUnitTest.cpp), the 1-node broker matrix (LeaseMatrixTest.cpp), the trio slice
// (LeaseTrioTest.cpp), the seeded stress test (Task 18) and the e2e script (Tasks 19, 20) all read it.
//
//   22 ops x 9 entities = 198 sequential cells; 48 are n/a (naRule), 150 apply; 89 timing cells (timingsOf); 239 executed.
//   expect() encodes the plan's rules 1-6 in order, on top of spec §21's table.
//
// Header-only and standard library only: pure functions and constexpr tables, no allocation outside the test helpers.
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace rune::node::test::leasematrix {

enum class Op : uint8_t {
    Grant, Update, Revoke, KeepAlive1, KeepAliveBatch, TimeToLive, PointRead, ScanLeases, WatchStart,   // the 9 client-surface ops
    BrokerExpire, BrokerCheckpoint, ClearCheckpoint0,                                                   // broker records
    AlterBounds, AlterCheckpoint, DropTopic,                                                           // DDL
    OwnClaim, OwnTryClaim, OwnRelease,                                                              // Task 20
    ElectCampaign, ElectProclaim, ElectResign, ElectObserve,                                           // Task 20
    kCount
};
enum class Entity : uint8_t {
    SameKeyVer, StaleVer, OtherKeySamePart, OtherPart, OtherLeaseTopic, NonLeaseSameWorker, UserView, OtherNs, OldSession, kCount
};
enum class Timing : uint8_t {
    Sequential, BackToBack,
    AroundExpireBeforePoll, AroundExpireAfterBuild, AroundExpireAfterCommit, AroundExpireAfterFold,
    KeepAliveInClearing, KeepAliveInCheckpointing, KeepAliveInEmitting, KeepAliveInDueNotEmitted,
    ContenderGrants, RevokeBehindUpdate, KnownOldDuplicate, HandoffOverflow, AtRiskPendingUnsent,
    LeaderChangeUncommitted, LeaderChangeCommittedNotFolded, LeaderChangeFolded,
    LeaderChangeGrantAck, LeaderChangeMidScan, Zombie, RestartOne, RestartAll, CompactionMid,
    SuspendReadOnly, SuspendRejectAll, AlterMidLife, DropWithParked,
    kCount
};

inline constexpr std::size_t kOps = std::size_t(Op::kCount), kEntities = std::size_t(Entity::kCount), kTimings = std::size_t(Timing::kCount);

[[nodiscard]] constexpr const char* name(Op o) noexcept {
    constexpr const char* n[] = {"Grant", "Update", "Revoke", "KeepAlive1", "KeepAliveBatch", "TimeToLive", "PointRead", "ScanLeases",
                                 "WatchStart", "BrokerExpire", "BrokerCheckpoint", "ClearCheckpoint0", "AlterBounds", "AlterCheckpoint",
                                 "DropTopic", "OwnClaim", "OwnTryClaim", "OwnRelease", "ElectCampaign", "ElectProclaim", "ElectResign",
                                 "ElectObserve"};
    return n[std::size_t(o)];
}
[[nodiscard]] constexpr const char* name(Entity e) noexcept {
    constexpr const char* n[] = {"SameKeyVer", "StaleVer", "OtherKeySamePart", "OtherPart", "OtherLeaseTopic", "NonLeaseSameWorker",
                                 "UserView", "OtherNs", "OldSession"};
    return n[std::size_t(e)];
}
[[nodiscard]] constexpr const char* name(Timing t) noexcept {
    constexpr const char* n[] = {"Sequential", "BackToBack", "AroundExpireBeforePoll", "AroundExpireAfterBuild", "AroundExpireAfterCommit",
                                 "AroundExpireAfterFold", "KeepAliveInClearing", "KeepAliveInCheckpointing", "KeepAliveInEmitting",
                                 "KeepAliveInDueNotEmitted", "ContenderGrants", "RevokeBehindUpdate", "KnownOldDuplicate", "HandoffOverflow",
                                 "AtRiskPendingUnsent", "LeaderChangeUncommitted", "LeaderChangeCommittedNotFolded", "LeaderChangeFolded",
                                 "LeaderChangeGrantAck", "LeaderChangeMidScan", "Zombie", "RestartOne", "RestartAll", "CompactionMid",
                                 "SuspendReadOnly", "SuspendRejectAll", "AlterMidLife", "DropWithParked"};
    return n[std::size_t(t)];
}

/// The 9 client-surface ops (Grant ... WatchStart).
[[nodiscard]] constexpr bool isClientSurface(Op o) noexcept { return std::size_t(o) <= std::size_t(Op::WatchStart); }
/// Ops that carry a version (a StaleVer cell exists for them).
[[nodiscard]] constexpr bool carriesVersion(Op o) noexcept {
    switch (o) {
    case Op::Update: case Op::Revoke: case Op::KeepAlive1: case Op::KeepAliveBatch: case Op::BrokerExpire: case Op::BrokerCheckpoint:
    case Op::ClearCheckpoint0: case Op::OwnRelease: case Op::ElectProclaim: case Op::ElectResign:
        return true;
    default: return false;
    }
}
[[nodiscard]] constexpr bool isBrokerOp(Op o) noexcept { return o == Op::BrokerExpire || o == Op::BrokerCheckpoint || o == Op::ClearCheckpoint0; }
/// Ops answered only on a lease topic (keepalive, TimeToLive, ScanLeases, watch).
[[nodiscard]] constexpr bool isLeaseOnly(Op o) noexcept {
    return o == Op::KeepAlive1 || o == Op::KeepAliveBatch || o == Op::TimeToLive || o == Op::ScanLeases || o == Op::WatchStart ||
           o == Op::AlterBounds || o == Op::AlterCheckpoint;   // the lease.* keys of a plain topic are refused
}
/// False for the 7 Own / Elect ops: Task 20 builds them and flips this.
[[nodiscard]] constexpr bool implemented(Op o) noexcept { return std::size_t(o) < std::size_t(Op::OwnClaim); }

/// True when the (op, entity) cell does not exist (the plan's 48 n/a cells).
[[nodiscard]] constexpr bool naRule(Op o, Entity e) noexcept {
    switch (e) {
    case Entity::StaleVer: return !carriesVersion(o);
    case Entity::UserView:
        switch (o) {
        case Op::Grant: case Op::Update: case Op::Revoke: case Op::KeepAlive1: case Op::KeepAliveBatch: case Op::BrokerExpire:
        case Op::BrokerCheckpoint: case Op::ClearCheckpoint0: case Op::DropTopic: return false;
        default: return true;
        }
    case Entity::OtherNs: return !(isClientSurface(o) || o == Op::AlterBounds || o == Op::AlterCheckpoint || o == Op::DropTopic);
    case Entity::OldSession: return !isClientSurface(o);
    default: return false;
    }
}

inline constexpr std::array<Timing, 10> kGrantT{Timing::BackToBack, Timing::LeaderChangeGrantAck, Timing::KnownOldDuplicate, Timing::ContenderGrants,
                                                Timing::RestartOne, Timing::RestartAll, Timing::CompactionMid, Timing::SuspendReadOnly,
                                                Timing::SuspendRejectAll, Timing::AlterMidLife};
inline constexpr std::array<Timing, 8> kUpdateT{Timing::BackToBack, Timing::AroundExpireBeforePoll, Timing::AroundExpireAfterBuild,
                                                Timing::AroundExpireAfterCommit, Timing::AroundExpireAfterFold, Timing::KnownOldDuplicate,
                                                Timing::SuspendReadOnly, Timing::SuspendRejectAll};
inline constexpr std::array<Timing, 10> kRevokeT{Timing::AroundExpireBeforePoll, Timing::AroundExpireAfterBuild, Timing::AroundExpireAfterCommit,
                                                 Timing::AroundExpireAfterFold, Timing::RevokeBehindUpdate, Timing::KnownOldDuplicate,
                                                 Timing::LeaderChangeUncommitted, Timing::LeaderChangeCommittedNotFolded,
                                                 Timing::LeaderChangeFolded, Timing::SuspendRejectAll};
inline constexpr std::array<Timing, 17> kKeepAlive1T{
    Timing::AroundExpireBeforePoll, Timing::AroundExpireAfterBuild, Timing::AroundExpireAfterCommit, Timing::AroundExpireAfterFold,
    Timing::KeepAliveInClearing, Timing::KeepAliveInCheckpointing, Timing::KeepAliveInEmitting, Timing::KeepAliveInDueNotEmitted,
    Timing::LeaderChangeUncommitted, Timing::LeaderChangeCommittedNotFolded, Timing::LeaderChangeFolded, Timing::Zombie,
    Timing::SuspendReadOnly, Timing::SuspendRejectAll, Timing::DropWithParked, Timing::RestartOne, Timing::AtRiskPendingUnsent};
inline constexpr std::array<Timing, 5> kKeepAliveBatchT{Timing::KeepAliveInClearing, Timing::KeepAliveInCheckpointing, Timing::KeepAliveInEmitting,
                                                        Timing::KeepAliveInDueNotEmitted, Timing::DropWithParked};
inline constexpr std::array<Timing, 2> kTtlT{Timing::LeaderChangeFolded, Timing::Zombie};
inline constexpr std::array<Timing, 2> kPointReadT{Timing::BackToBack, Timing::LeaderChangeFolded};
inline constexpr std::array<Timing, 2> kScanT{Timing::LeaderChangeMidScan, Timing::CompactionMid};
inline constexpr std::array<Timing, 3> kWatchT{Timing::HandoffOverflow, Timing::LeaderChangeMidScan, Timing::CompactionMid};
inline constexpr std::array<Timing, 7> kBrokerExpireT{Timing::LeaderChangeUncommitted, Timing::LeaderChangeCommittedNotFolded,
                                                      Timing::LeaderChangeFolded, Timing::RestartAll, Timing::SuspendReadOnly,
                                                      Timing::SuspendRejectAll, Timing::ContenderGrants};
inline constexpr std::array<Timing, 7> kBrokerCheckpointT{Timing::LeaderChangeUncommitted, Timing::LeaderChangeCommittedNotFolded,
                                                          Timing::LeaderChangeFolded, Timing::RestartOne, Timing::RestartAll,
                                                          Timing::CompactionMid, Timing::AlterMidLife};
inline constexpr std::array<Timing, 3> kClearT{Timing::LeaderChangeUncommitted, Timing::LeaderChangeCommittedNotFolded, Timing::LeaderChangeFolded};
inline constexpr std::array<Timing, 1> kAlterT{Timing::AlterMidLife};
inline constexpr std::array<Timing, 1> kDropT{Timing::DropWithParked};
inline constexpr std::array<Timing, 2> kOwnClaimT{Timing::LeaderChangeFolded, Timing::KnownOldDuplicate};
inline constexpr std::array<Timing, 1> kBackToBackT{Timing::BackToBack};
inline constexpr std::array<Timing, 1> kAfterFoldT{Timing::AroundExpireAfterFold};
inline constexpr std::array<Timing, 3> kCampaignT{Timing::Zombie, Timing::KnownOldDuplicate, Timing::AtRiskPendingUnsent};
inline constexpr std::array<Timing, 1> kFoldedT{Timing::LeaderChangeFolded};

/// The timing cells of `o` (on SameKeyVer unless the plan says otherwise): 89 over all ops.
[[nodiscard]] constexpr std::span<const Timing> timingsOf(Op o) noexcept {
    switch (o) {
    case Op::Grant: return kGrantT;
    case Op::Update: return kUpdateT;
    case Op::Revoke: return kRevokeT;
    case Op::KeepAlive1: return kKeepAlive1T;
    case Op::KeepAliveBatch: return kKeepAliveBatchT;
    case Op::TimeToLive: return kTtlT;
    case Op::PointRead: return kPointReadT;
    case Op::ScanLeases: return kScanT;
    case Op::WatchStart: return kWatchT;
    case Op::BrokerExpire: return kBrokerExpireT;
    case Op::BrokerCheckpoint: return kBrokerCheckpointT;
    case Op::ClearCheckpoint0: return kClearT;
    case Op::AlterBounds: case Op::AlterCheckpoint: return kAlterT;
    case Op::DropTopic: return kDropT;
    case Op::OwnClaim: return kOwnClaimT;
    case Op::OwnTryClaim: return kBackToBackT;
    case Op::OwnRelease: return kAfterFoldT;
    case Op::ElectCampaign: return kCampaignT;
    case Op::ElectProclaim: return kAfterFoldT;
    case Op::ElectResign: return kBackToBackT;
    case Op::ElectObserve: return kFoldedT;
    case Op::kCount: break;
    }
    return {};
}

/// What an op answers.  The client codes are the wire ErrorCode names; the rest are cell-level verdicts.
enum class Result : uint8_t {
    Ok,                  ///< accepted / served
    KeyExists,           ///< a grant of a live key
    LeaseNotFound,       ///< definite: the lease is not (or no longer) this version
    InvalidRequest,      ///< rule 1: an old session (39), or an upsert
    MissingObject,       ///< rule 2: byte-equal to the answer for a truly missing name
    NotALeaseTopic,      ///< rule 3: a lease-only op on a non-lease topic (UnknownTopic / NotAuthorized / local InvalidArgument)
    NoEffect,            ///< rule 3: a broker op on a non-lease topic: the partition is byte-identical to a run without it
    StaleVerdict,        ///< rule 6: a broker record of a lease that moved on: the defensive stale verdict
    Absent,              ///< the cell is n/a
};
/// What an op does to the lease it addresses.
enum class Effect : uint8_t {
    None,        ///< unchanged
    Created,     ///< a new row (grant)
    Replaced,    ///< columns replaced, deadline unchanged (update)
    Removed,     ///< the row ends (revoke, expire, drop)
    Rearmed,     ///< the deadline moves to now + ttl (keepalive)
    Checkpointed ///< a Checkpoint(remaining) row folded
};
struct Outcome {
    Result result = Result::Ok;
    Effect target = Effect::None;   ///< the effect on the lease the op ADDRESSES (the entity's own lease)
    bool   lChanged = false;        ///< does L itself (the SameKeyVer lease) change?
    bool   userViewChanged = false; ///< does U change?
    bool operator==(const Outcome&) const = default;
};

/// The effect an op has on the lease it addresses, when it succeeds.
[[nodiscard]] constexpr Effect okEffect(Op o) noexcept {
    switch (o) {
    case Op::Grant: return Effect::Created;
    case Op::Update: return Effect::Replaced;
    case Op::Revoke: case Op::BrokerExpire: case Op::DropTopic: return Effect::Removed;
    case Op::KeepAlive1: case Op::KeepAliveBatch: return Effect::Rearmed;
    case Op::BrokerCheckpoint: case Op::ClearCheckpoint0: return Effect::Checkpointed;   // the row's remaining column changes (a clear writes 0)
    default: return Effect::None;                      // reads, scans, watch, alter (a bounds change touches no row)
    }
}

/// The expected outcome of (op, entity, timing).  Rules, in order (plan "Expected outcomes"):
///  1. OldSession x any -> InvalidRequest naming the version, before any lease state is touched; L unchanged.
///  2. OtherNs x any -> the answer of a missing object (byte-equal to a truly missing name); L unchanged.
///  3. NonLeaseSameWorker x a lease-only op -> NotALeaseTopic; x a broker op -> NoEffect (log and views byte-identical);
///     x grant / update / revoke / alter / drop -> the op's own result on that topic (it is a plain topic: not a lease op).
///  4. OtherKeySamePart / OtherPart / OtherLeaseTopic x any -> that entity's own expected result; L unchanged.
///  5. UserView x Expire / Revoke -> the row is deleted in the same fold batch as the lease view's; x Checkpoint, Clear, keepalive -> U is
///     byte-identical.
///  6. StaleVer x client op -> LeaseNotFound (definite), L unchanged; x broker op -> the defensive stale verdict.
/// Sequential is the base; timings that change the verdict are encoded after the rules.
[[nodiscard]] constexpr Outcome expect(Op o, Entity e, Timing t) noexcept {
    if (naRule(o, e)) return {Result::Absent, Effect::None, false, false};
    // 1
    if (e == Entity::OldSession) return {Result::InvalidRequest, Effect::None, false, false};
    // 2
    if (e == Entity::OtherNs) return {Result::MissingObject, Effect::None, false, false};
    // 6
    if (e == Entity::StaleVer) {
        if (isBrokerOp(o)) return {Result::StaleVerdict, Effect::None, false, false};
        return {Result::LeaseNotFound, Effect::None, false, false};
    }
    // 3
    if (e == Entity::NonLeaseSameWorker) {
        if (isLeaseOnly(o)) return {Result::NotALeaseTopic, Effect::None, false, false};
        if (isBrokerOp(o)) return {Result::NoEffect, Effect::None, false, false};
        // grant / update / revoke on a plain topic are plain produces; alter / drop address the topic itself
        return {Result::Ok, Effect::None, false, false};
    }
    // A topic drop is topic-wide: for a cell that names a lease of the dropped topic itself, the whole topic (L, every key, U) goes.
    if (o == Op::DropTopic && (e == Entity::SameKeyVer || e == Entity::OtherKeySamePart || e == Entity::OtherPart || e == Entity::UserView))
        return {Result::Ok, Effect::Removed, true, true};
    // 4: the entity's own lease is addressed; L stays unchanged.  A grant addresses a fresh key (Ok); every other op a live lease.
    if (e == Entity::OtherKeySamePart || e == Entity::OtherPart || e == Entity::OtherLeaseTopic)
        return {Result::Ok, okEffect(o), false, false};
    // SameKeyVer / UserView
    Outcome out{Result::Ok, okEffect(o), false, false};
    if (o == Op::Grant) out.result = Result::KeyExists, out.target = Effect::None;   // L is live
    // SameKeyVer and UserView both act on L; UserView additionally asserts rule 5 on U (the same flags: U loses the row with the lease on
    // Expire / Revoke, has its columns replaced on Update, and is byte-identical for Checkpoint, Clear and keepalive).
    out.lChanged         = out.target != Effect::None;
    out.userViewChanged  = out.target == Effect::Removed || out.target == Effect::Replaced;
    (void)t;
    return out;
}

// ---- counts -------------------------------------------------------------------------------------------------------------------
struct Counts { std::size_t sequential = 0, na = 0, applicable = 0, timing = 0, executed = 0; };
[[nodiscard]] constexpr Counts countCells(bool implementedOnly) noexcept {
    Counts c;
    for (std::size_t oi = 0; oi < kOps; ++oi) {
        const Op o = Op(oi);
        if (implementedOnly && !implemented(o)) continue;
        for (std::size_t ei = 0; ei < kEntities; ++ei) {
            ++c.sequential;
            if (naRule(o, Entity(ei))) ++c.na; else ++c.applicable;
        }
        c.timing += timingsOf(o).size();
    }
    c.executed = c.applicable + c.timing;
    return c;
}

}  // namespace rune::node::test::leasematrix
