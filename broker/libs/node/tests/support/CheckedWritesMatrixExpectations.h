#pragma once
// CheckedWritesMatrixExpectations.h - the checked-writes interaction matrix, shared by the parts (plan 04 P4-15, R03d).
// Every part owns an id range of Op and Entity; Timing is append-only.  expect() chains the per-part expectNN by the op's range.
#include <cstdint>
#include <vector>

namespace rune::node::cw::mx {   // own namespace: cw::Entity is Part 02's

enum class Op : uint16_t {
    AcceptedWrite = 0, RejectedWrite = 1, RejectedOverAccepted = 2, CapRace = 3, DeleteFreesCap = 4,   // 04: 0-19
    Insert = 20, Update = 21, Upsert = 22, Remove = 23, RemoveExisting = 24,                           // 05: 20-39
    /* 06: 40-59, 07: 60-79, 09: 80-99, 11: 100-119 */
    ViewAckLV = 120, ViewAckAV = 121,                                                                   // 03: 120-139
};
enum class Entity : uint16_t {
    RawSubscription = 0, GroupConsumer = 1, FilteredSubscription = 2, ReplayFromStart = 3,
    OtherViews = 4, TwoVerdictViews = 5, ViewAckWriter = 6, OldWriters = 7,                              // 04: 0-19
    KeyNew = 20, KeyExisting = 21, KeyEarlierInBatch = 22, KeyRemovedEarlier = 23, KeyRejectedEarlier = 24, KeyBeforeIndex = 25,   // 05: 20-39
    AckNoViews = 120, AckOneView = 121, AckTwoViewsLate = 122, AckTransientSource = 123,                 // 03: 120-139
};
enum class Timing : uint16_t {
    Single, BackToBack, SelfCollidingBatch, Racing, RetryBeforeJudge, RetryAfterJudge,
    FailoverAppendHw, FailoverHwFold, FailoverFoldMarker, TruncateTail, RestartDuringRebuild,
    CreateOnNonEmpty, ReplaceDuringWrites, DropDuringWrites,
    TxnCommitBehind, TxnAbortBehind, LeaderLossParked, AckDeadline, StrictTier, GatewayRouted,           // appended for 03
    RetryAfterWindow,                                                                                    // appended for 04 (R04e)
};
enum class Part : uint8_t { P03 = 3, P04 = 4, P05 = 5, P06 = 6, P07 = 7, P09 = 9, P11 = 11 };

struct Cell {
    Op op;
    Entity entity;
    Timing timing;
};

/// What a cell checks beyond (a)-(g), which every applicable cell asserts.
enum class Check : uint8_t {
    None,
    RetryOutsideWindow,   ///< RetryAfterWindow x ViewAckWriter: DuplicateSequence, no trailer, verdictUnknownDuplicates == 1
    NotLeaderFirst,       ///< ViewAckWriter x FailoverAppendHw: the first View ack is NotLeader, the re-sent batch carries the model's verdicts
    Trailer,              ///< ViewAckWriter: the trailer is exactly the model's rejections
    OldFrames,            ///< OldWriters: 46-byte frames, no trailer
    Delivered,            ///< readers: the delivered records are the model's accepted records
    Tables,               ///< OtherViews, TwoVerdictViews: every table equals a fresh refold of the accepted records
    KeyIndexTable,        ///< 05 Single: the trailer is the P5-3 table's verdict (keyIndexVerdict05), checked against the model
    NotLeaderParked,      ///< 05 LeaderLossParked: the parked View ack is NotLeader
    DeadlineInternal,     ///< 05 AckDeadline: Internal, and once the fold resumes the log holds the model's verdict
    RetryReplays,         ///< 05 RetryBeforeJudge / RetryAfterJudge: the duplicate's ack is byte-identical to the original's
    SelfColliding,        ///< 05 SelfCollidingBatch: the op twice in one batch (selfCollidingCodes05)
    RacingOneWins,        ///< 05 Racing x Insert x KeyNew: one Ok and one KeyExists, the Ok at the lower offset
};

struct Expected {
    bool applicable = true;
    const char* naReason = nullptr;
    Check check = Check::None;
};

// ---- Part 03 (View acks): 2 ops x 4 entities x 10 timings ----
[[nodiscard]] inline bool isP03(const Cell& c) noexcept { return c.op >= Op::ViewAckLV; }
[[nodiscard]] inline bool isP05(const Cell& c) noexcept { return c.op >= Op::Insert && c.op < static_cast<Op>(40); }

[[nodiscard]] inline Expected expect03(const Cell& c) {
    if (c.entity == Entity::AckTransientSource && c.timing == Timing::StrictTier)
        return {false, "a transient ring has no fd: Strict acks as Majority there, the cell equals Single"};
    return {};
}

// ---- Part 04 (the verdict pipeline): 5 ops x 8 entities x 15 timings ----
[[nodiscard]] inline Expected expect04(const Cell& c) {
    const bool retry = c.timing == Timing::RetryBeforeJudge || c.timing == Timing::RetryAfterJudge || c.timing == Timing::RetryAfterWindow;
    if (c.entity == Entity::OldWriters && retry) return {false, "old writers make no idempotent retry"};
    if (c.timing == Timing::RetryAfterWindow && c.entity == Entity::ViewAckWriter) return {true, nullptr, Check::RetryOutsideWindow};
    if (c.timing == Timing::FailoverAppendHw && c.entity == Entity::ViewAckWriter) return {true, nullptr, Check::NotLeaderFirst};
    switch (c.entity) {
    case Entity::ViewAckWriter: return {true, nullptr, Check::Trailer};
    case Entity::OldWriters: return {true, nullptr, Check::OldFrames};
    case Entity::OtherViews: case Entity::TwoVerdictViews: return {true, nullptr, Check::Tables};
    default: return {true, nullptr, Check::Delivered};
    }
}

// ---- Part 05 (intents and the key index): 5 ops x 6 entities x 21 timings ----
/// The P5-3 table at the Single timing: 0 = Ok, else the code the op's record is rejected with (constraint id 0).
[[nodiscard]] inline uint16_t keyIndexVerdict05(Op op, Entity e) {
    const bool exists = e == Entity::KeyExisting || e == Entity::KeyEarlierInBatch || e == Entity::KeyBeforeIndex;
    switch (op) {
    case Op::Insert: return exists ? 56 : 0;
    case Op::Update: case Op::RemoveExisting: return exists ? 0 : 57;
    default: return 0;
    }
}

/// SelfCollidingBatch: the codes of the op sent twice in one batch where the key state is known (New, Existing); empty = follow the model.
[[nodiscard]] inline std::vector<uint16_t> selfCollidingCodes05(Op op, Entity e) {
    if (op == Op::Insert && e == Entity::KeyNew) return {0, 56};
    if (op == Op::Update && e == Entity::KeyExisting) return {0, 0};
    if (op == Op::RemoveExisting && e == Entity::KeyExisting) return {0, 57};
    if (op == Op::Remove || op == Op::Upsert) return {0, 0};
    return {};
}

[[nodiscard]] inline Expected expect05(const Cell& c) {
    if (c.timing == Timing::ReplaceDuringWrites || c.timing == Timing::DropDuringWrites) return {false, "key.index is immutable (no replace or drop)"};
    if (c.entity == Entity::KeyRejectedEarlier && (c.timing == Timing::TxnCommitBehind || c.timing == Timing::TxnAbortBehind))
        return {false, "the TestReject member makes the source constrained, and transactions are refused there before Part G"};
    switch (c.timing) {
    case Timing::FailoverAppendHw: case Timing::TruncateTail: return {true, nullptr, Check::NotLeaderFirst};
    case Timing::LeaderLossParked: return {true, nullptr, Check::NotLeaderParked};
    case Timing::AckDeadline: return {true, nullptr, Check::DeadlineInternal};
    case Timing::RetryAfterWindow: return {true, nullptr, Check::RetryOutsideWindow};
    case Timing::RetryBeforeJudge: case Timing::RetryAfterJudge: return {true, nullptr, Check::RetryReplays};
    case Timing::SelfCollidingBatch: return {true, nullptr, Check::SelfColliding};
    case Timing::Racing: return {true, nullptr, c.op == Op::Insert && c.entity == Entity::KeyNew ? Check::RacingOneWins : Check::Trailer};
    case Timing::Single: return {true, nullptr, Check::KeyIndexTable};
    default: return {true, nullptr, Check::Trailer};
    }
}

[[nodiscard]] inline Expected expect(const Cell& c) { return isP05(c) ? expect05(c) : isP03(c) ? expect03(c) : expect04(c); }

[[nodiscard]] inline std::vector<Cell> cellsFor(Part part) {
    std::vector<Cell> out;
    if (part == Part::P03) {
        const Timing t03[] = {Timing::Single, Timing::BackToBack, Timing::RetryBeforeJudge, Timing::RetryAfterJudge, Timing::TxnCommitBehind,
                              Timing::TxnAbortBehind, Timing::LeaderLossParked, Timing::AckDeadline, Timing::StrictTier, Timing::GatewayRouted};
        for (const Op op : {Op::ViewAckLV, Op::ViewAckAV})
            for (const Entity e : {Entity::AckNoViews, Entity::AckOneView, Entity::AckTwoViewsLate, Entity::AckTransientSource})
                for (const Timing t : t03) out.push_back({op, e, t});
    } else if (part == Part::P05) {
        const Timing t05[] = {Timing::Single, Timing::BackToBack, Timing::SelfCollidingBatch, Timing::Racing, Timing::RetryBeforeJudge, Timing::RetryAfterJudge,
                              Timing::FailoverAppendHw, Timing::FailoverHwFold, Timing::FailoverFoldMarker, Timing::TruncateTail, Timing::RestartDuringRebuild,
                              Timing::CreateOnNonEmpty, Timing::ReplaceDuringWrites, Timing::DropDuringWrites,
                              Timing::TxnCommitBehind, Timing::TxnAbortBehind, Timing::LeaderLossParked, Timing::AckDeadline, Timing::StrictTier, Timing::GatewayRouted,
                              Timing::RetryAfterWindow};
        for (const Op op : {Op::Insert, Op::Update, Op::Upsert, Op::Remove, Op::RemoveExisting})
            for (const Entity e : {Entity::KeyNew, Entity::KeyExisting, Entity::KeyEarlierInBatch, Entity::KeyRemovedEarlier, Entity::KeyRejectedEarlier, Entity::KeyBeforeIndex})
                for (const Timing t : t05) out.push_back({op, e, t});
    } else if (part == Part::P04) {
        for (uint16_t op = 0; op <= static_cast<uint16_t>(Op::DeleteFreesCap); ++op)
            for (uint16_t e = 0; e <= static_cast<uint16_t>(Entity::OldWriters); ++e) {
                for (uint16_t t = 0; t <= static_cast<uint16_t>(Timing::DropDuringWrites); ++t)
                    out.push_back({static_cast<Op>(op), static_cast<Entity>(e), static_cast<Timing>(t)});
                out.push_back({static_cast<Op>(op), static_cast<Entity>(e), Timing::RetryAfterWindow});
            }
    }
    return out;
}

[[nodiscard]] inline std::size_t applicable(const std::vector<Cell>& cells) {
    std::size_t n = 0;
    for (const Cell& c : cells) n += expect(c).applicable ? 1 : 0;
    return n;
}
[[nodiscard]] inline std::size_t skipped(const std::vector<Cell>& cells) { return cells.size() - applicable(cells); }

inline const char* nameOf(Op o) {
    switch (o) {
    case Op::AcceptedWrite: return "AcceptedWrite"; case Op::RejectedWrite: return "RejectedWrite";
    case Op::RejectedOverAccepted: return "RejectedOverAccepted"; case Op::CapRace: return "CapRace";
    case Op::DeleteFreesCap: return "DeleteFreesCap";
    case Op::Insert: return "Insert"; case Op::Update: return "Update"; case Op::Upsert: return "Upsert";
    case Op::Remove: return "Remove"; case Op::RemoveExisting: return "RemoveExisting"; case Op::ViewAckLV: return "ViewAckLV"; case Op::ViewAckAV: return "ViewAckAV";
    }
    return "?";
}
inline const char* nameOf(Entity e) {
    switch (e) {
    case Entity::RawSubscription: return "RawSubscription"; case Entity::GroupConsumer: return "GroupConsumer";
    case Entity::FilteredSubscription: return "FilteredSubscription"; case Entity::ReplayFromStart: return "ReplayFromStart";
    case Entity::OtherViews: return "OtherViews"; case Entity::TwoVerdictViews: return "TwoVerdictViews";
    case Entity::ViewAckWriter: return "ViewAckWriter"; case Entity::OldWriters: return "OldWriters";
    case Entity::AckNoViews: return "AckNoViews"; case Entity::AckOneView: return "AckOneView";
    case Entity::AckTwoViewsLate: return "AckTwoViewsLate"; case Entity::AckTransientSource: return "AckTransientSource";
    case Entity::KeyNew: return "KeyNew"; case Entity::KeyExisting: return "KeyExisting"; case Entity::KeyEarlierInBatch: return "KeyEarlierInBatch";
    case Entity::KeyRemovedEarlier: return "KeyRemovedEarlier"; case Entity::KeyRejectedEarlier: return "KeyRejectedEarlier";
    case Entity::KeyBeforeIndex: return "KeyBeforeIndex";
    }
    return "?";
}
inline const char* nameOf(Timing t) {
    switch (t) {
    case Timing::Single: return "Single"; case Timing::BackToBack: return "BackToBack";
    case Timing::SelfCollidingBatch: return "SelfCollidingBatch"; case Timing::Racing: return "Racing";
    case Timing::RetryBeforeJudge: return "RetryBeforeJudge"; case Timing::RetryAfterJudge: return "RetryAfterJudge";
    case Timing::FailoverAppendHw: return "FailoverAppendHw"; case Timing::FailoverHwFold: return "FailoverHwFold";
    case Timing::FailoverFoldMarker: return "FailoverFoldMarker"; case Timing::TruncateTail: return "TruncateTail";
    case Timing::RestartDuringRebuild: return "RestartDuringRebuild"; case Timing::CreateOnNonEmpty: return "CreateOnNonEmpty";
    case Timing::ReplaceDuringWrites: return "ReplaceDuringWrites"; case Timing::DropDuringWrites: return "DropDuringWrites";
    case Timing::TxnCommitBehind: return "TxnCommitBehind"; case Timing::TxnAbortBehind: return "TxnAbortBehind";
    case Timing::LeaderLossParked: return "LeaderLossParked"; case Timing::AckDeadline: return "AckDeadline";
    case Timing::StrictTier: return "StrictTier"; case Timing::GatewayRouted: return "GatewayRouted";
    case Timing::RetryAfterWindow: return "RetryAfterWindow";
    }
    return "?";
}

}  // namespace rune::node::cw::mx
