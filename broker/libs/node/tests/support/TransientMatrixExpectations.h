#pragma once
// TransientMatrixExpectations.h — Phase 2b E2 interaction matrix: THE single
// source of truth (plan section "Interaction matrix").  expect(op, entity,
// timing) -> Outcome, rules R1-R12 applied top-down.  The unit matrix (Task
// 11), the seeded stress (Task 12), the BrokerNode slice (Task 13) and the e2e
// leg (Task 14) read it; no test hard-codes an outcome.  Header-only,
// constexpr, no allocation.  Test-only.
#include <array>
#include <cstdint>
#include <string_view>

namespace rune::node::test::tmatrix {

enum class Op : uint8_t {
    ProduceAcks0, ProduceAcks1, ProduceAcksAll,
    SubEarliest, SubLatest, SubOffset, GroupConsume, ReadCommitted,
    TxnMarker,
};
enum class Persist : uint8_t { Transient, Disk };
enum class Mode    : uint8_t { Shard, Legacy };
enum class Role    : uint8_t { Leader, Follower };
enum class Commit  : uint8_t { Committed, Uncommitted };
enum class Timing  : uint8_t {
    Steady, RingFullUncommitted, EvictedCursor, LeaderMovesMidOp, Restart, FollowerBelowRingStart,
    CommitLandsMidOp,
};

struct Entity {
    Persist persist;
    Mode    mode;
    Role    role;
    Commit  commit;
};

enum class Outcome : uint8_t {
    Ok,                      // acked / delivered exactly the expected batches
    OkFromRingStart,         // read resumed at the ring start; skipped records counted
    OkEmptyAfterRestart,     // transient after a restart: offsets from 0, only post-restart records
    OkAfterCommit,           // C1: refused NotLeader, then Ok once the topic committed
    RetriableNotLeader,      // 2 (subscribe: bare ErrorResponse; group: -1); nothing appended / registered
                             // (also read_committed on a follower — R7, ruling 2026-10-13: never OK-then-nothing)
    RetriableLogFull,        // 42 to a v6 session (NotLeader to v5); nothing appended; Ok after catch-up
    MarkerRetried,           // marker refused retriably; lands once the condition clears (leader cells)
    UnansweredThenReissued,  // no Records while disowned; served again once ownership returns
    ParityWithDisk,          // the transient topic's answer + delivery == the disk topic's
    NotApplicable,
};

inline constexpr std::array<Op, 9> kOps{Op::ProduceAcks0, Op::ProduceAcks1, Op::ProduceAcksAll, Op::SubEarliest,
                                        Op::SubLatest, Op::SubOffset, Op::GroupConsume, Op::ReadCommitted,
                                        Op::TxnMarker};
inline constexpr std::array<Timing, 7> kTimings{Timing::Steady, Timing::RingFullUncommitted, Timing::EvictedCursor,
                                                Timing::LeaderMovesMidOp, Timing::Restart,
                                                Timing::FollowerBelowRingStart, Timing::CommitLandsMidOp};

[[nodiscard]] constexpr bool isProduce(Op o) noexcept {
    return o == Op::ProduceAcks0 || o == Op::ProduceAcks1 || o == Op::ProduceAcksAll;
}
[[nodiscard]] constexpr bool isRead(Op o) noexcept { return !isProduce(o) && o != Op::TxnMarker; }
/// The Produce `acks` byte: 0 = Leader/None (the client decides whether it waits), 1 = All.
[[nodiscard]] constexpr uint8_t wireAcks(Op o) noexcept { return o == Op::ProduceAcksAll ? 1 : 0; }

[[nodiscard]] constexpr Outcome leaderMoves_(Op op) noexcept {   // R9
    if (isProduce(op)) return Outcome::RetriableNotLeader;
    if (op == Op::TxnMarker) return Outcome::MarkerRetried;
    return Outcome::UnansweredThenReissued;
}

[[nodiscard]] constexpr Outcome expect(Op op, Entity e, Timing t) noexcept {
    const bool marker = op == Op::TxnMarker;
    if (e.commit == Commit::Uncommitted) {                                           // R1-R3
        if (t == Timing::Steady)           return marker ? Outcome::NotApplicable : Outcome::RetriableNotLeader;
        if (t == Timing::CommitLandsMidOp)                                           // a follower then answers as R7
            return marker || e.role == Role::Follower ? Outcome::NotApplicable : Outcome::OkAfterCommit;
        return Outcome::NotApplicable;
    }
    if (t == Timing::CommitLandsMidOp) return Outcome::NotApplicable;                // R3
    const bool ringOnly = t == Timing::RingFullUncommitted || t == Timing::EvictedCursor ||
                          t == Timing::FollowerBelowRingStart;
    if (e.persist == Persist::Disk && ringOnly) return Outcome::NotApplicable;      // R4
    if (e.role == Role::Follower) {
        if (ringOnly) return Outcome::NotApplicable;                                 // R5
        // R7 (controller ruling 2026-10-13, Task 11 fix round 2): read_committed is
        // served from a leader's RC state only — a follower refuses the subscribe
        // NotLeader (retriable: the client re-routes), never OK-then-nothing.
        if (op == Op::ReadCommitted) return Outcome::RetriableNotLeader;
        if (t == Timing::LeaderMovesMidOp) return leaderMoves_(op);                  // R6
        if (isProduce(op)) return Outcome::RetriableNotLeader;                       // R7
        return marker ? Outcome::MarkerRetried : Outcome::ParityWithDisk;
    }
    switch (t) {
    case Timing::Steady: return Outcome::Ok;                                         // R8
    case Timing::LeaderMovesMidOp: return leaderMoves_(op);                          // R9
    case Timing::RingFullUncommitted:                                                // R10
        if (isProduce(op)) return Outcome::RetriableLogFull;
        return marker ? Outcome::MarkerRetried : Outcome::Ok;
    case Timing::EvictedCursor:                                                      // R11
    case Timing::FollowerBelowRingStart:
        if (isProduce(op) || marker || op == Op::SubLatest) return Outcome::Ok;
        return Outcome::OkFromRingStart;
    case Timing::Restart:                                                            // R12
        return e.persist == Persist::Transient ? Outcome::OkEmptyAfterRestart : Outcome::Ok;
    case Timing::CommitLandsMidOp: break;
    }
    return Outcome::NotApplicable;
}

[[nodiscard]] constexpr std::string_view naRule(Op op, Entity e, Timing t) noexcept {
    if (e.commit == Commit::Uncommitted) {
        if (op == Op::TxnMarker && (t == Timing::Steady || t == Timing::CommitLandsMidOp))
            return "R1/R2: no transaction can include a partition whose produce was refused";
        if (t == Timing::CommitLandsMidOp) return "R2: after the commit a follower answers as R7 (Committed rows)";
        return "R3: this timing needs a serving topic";
    }
    if (t == Timing::CommitLandsMidOp) return "R3: the topic is already committed";
    if (e.persist == Persist::Disk) return "R4: a disk log has no ring (retention: CP-5.2; follower: ruling 4)";
    return "R5: a leader-side ring state (the lagging follower is Tasks 3 and 12)";
}

[[nodiscard]] constexpr std::string_view name(Op o) noexcept {
    constexpr std::array<std::string_view, 9> n{"ProduceAcks0", "ProduceAcks1", "ProduceAcksAll", "SubEarliest",
                                                "SubLatest", "SubOffset", "GroupConsume", "ReadCommitted",
                                                "TxnMarker"};
    return n[static_cast<std::size_t>(o)];
}
[[nodiscard]] constexpr std::string_view name(Timing t) noexcept {
    constexpr std::array<std::string_view, 7> n{"Steady", "RingFullUncommitted", "EvictedCursor",
                                                "LeaderMovesMidOp", "Restart", "FollowerBelowRingStart",
                                                "CommitLandsMidOp"};
    return n[static_cast<std::size_t>(t)];
}
[[nodiscard]] constexpr std::string_view name(Outcome o) noexcept {
    constexpr std::array<std::string_view, 10> n{"Ok", "OkFromRingStart", "OkEmptyAfterRestart", "OkAfterCommit",
                                                 "RetriableNotLeader", "RetriableLogFull", "MarkerRetried",
                                                 "UnansweredThenReissued", "ParityWithDisk", "NotApplicable"};
    return n[static_cast<std::size_t>(o)];
}

} // namespace rune::node::test::tmatrix
