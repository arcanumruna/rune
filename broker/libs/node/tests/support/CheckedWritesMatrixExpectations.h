#pragma once
// CheckedWritesMatrixExpectations.h - the checked-writes interaction matrix, shared by the parts (plan 04 P4-15, R03d).
// Every part owns an id range of Op and Entity; Timing is append-only.  expect() chains the per-part expectNN by the op's range.
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace rune::node::cw::mx {   // own namespace: cw::Entity is Part 02's

enum class Op : uint16_t {
    AcceptedWrite = 0, RejectedWrite = 1, RejectedOverAccepted = 2, CapRace = 3, DeleteFreesCap = 4,   // 04: 0-19
    Insert = 20, Update = 21, Upsert = 22, Remove = 23, RemoveExisting = 24,                           // 05: 20-39
    AutoCreate1 = 40, AutoCreate8 = 41, ImportBelow = 42, ImportAbove = 43, UpdateAssigned = 44, RemoveExistingAssigned = 45,   // 06: 40-59
    ViewUpsertNew = 60, ViewUpdateNonKey = 61, ViewRename = 62, ViewMoveGroup = 63, ViewDelete = 64, ViewDeleteThenReuse = 65,   // 07: 60-79
    ViewDuplicateRetry = 66, ViewPointRead = 67, ViewPrefixScan = 68,
    CCreate = 80, CUpdateNonKey = 81, CRename = 82, CMove = 83, CRemove = 84, CRemoveExisting = 85, CDeleteThenReuse = 86,   // 09: 80-99
    TxnCreate = 100, TxnRename = 101, TxnMove = 102, TxnRemoveExisting = 103, TxnSelfCollidingCreates = 104, TxnUpdateNonKey = 105, AtomicBatch = 106,   // 11: 100-119
    ViewAckLV = 120, ViewAckAV = 121,                                                                   // 03: 120-139
};
enum class Entity : uint16_t {
    RawSubscription = 0, GroupConsumer = 1, FilteredSubscription = 2, ReplayFromStart = 3,
    OtherViews = 4, TwoVerdictViews = 5, ViewAckWriter = 6, OldWriters = 7,                              // 04: 0-19
    KeyNew = 20, KeyExisting = 21, KeyEarlierInBatch = 22, KeyRemovedEarlier = 23, KeyRejectedEarlier = 24, KeyBeforeIndex = 25,   // 05: 20-39
    P1U64 = 40, P4U64 = 41, P1U32 = 42, P2I64 = 43, AtReservationEdge = 44, NearMax = 45, Constrained = 46,                  // 06: 40-59
    KeyU16 = 60, KeyU32 = 61, KeyU64 = 62, KeyI64 = 63, KeyStr16 = 64, KeyComposite = 65, KeyCompositeWide = 66, KeyLower = 67, KeyMaxWidth = 68,   // 07: 60-79
    G11Free = 80, G11Taken = 81, G12Free = 82, G12Taken = 83, OverFree = 84, OverTaken = 85, G11Self = 86, G12Self = 87, OverSelf = 88,   // 09: 80-99
    DupOwner = 89, DupExtra = 90,
    AllPass = 100, FailsUnique = 101, FailsCheck = 102, SecondPartitionNo = 103, KeyAbsent = 104, OutsideWriteSameKey = 105,   // 11: 100-119
    OutsideWriteLockedRow = 106, RacingTxnSameName = 107,
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
    NotLeaderThenExhausted,   ///< 06 NearMax x AutoCreate1 x FailoverAppendHw / TruncateTail: the first ack is NotLeader, the re-send finds the counter above the cap (the reservation was clamped to it)
    AutoExhausted,        ///< 06 NearMax x AutoCreate8: every answer is AutoIncrementExhausted and nothing is stored
    ImportOutOfRange,     ///< 06 NearMax x ImportAbove: every answer is InvalidRequest and nothing is stored
    Burned,               ///< 06 Constrained x AutoCreate8: the rejected creates' ids are burned, never handed out again
    RacingIds,            ///< 06 Racing x a create: the two batches' ids are disjoint and contiguous
    CollidingPair,        ///< 06 Racing / SelfCollidingBatch x an import, update or remove: the pair of verdicts of collidingCodes06
    AutoVerdict,          ///< 06 an import, update or remove: the trailer is verdict06
    ConstraintTable,      ///< 09 Single: the trailer is the Single table's verdicts (single09), checked against the model
};

struct Expected {
    bool applicable = true;
    const char* naReason = nullptr;
    Check check = Check::None;
};

// ---- Part 03 (View acks): 2 ops x 4 entities x 10 timings ----
[[nodiscard]] inline bool isP03(const Cell& c) noexcept { return c.op >= Op::ViewAckLV; }
[[nodiscard]] inline bool isP05(const Cell& c) noexcept { return c.op >= Op::Insert && c.op < Op::AutoCreate1; }
[[nodiscard]] inline bool isP06(const Cell& c) noexcept { return c.op >= Op::AutoCreate1 && c.op < static_cast<Op>(60); }

[[nodiscard]] inline bool isP09(const Cell& c) noexcept { return c.op >= Op::CCreate && c.op <= Op::CDeleteThenReuse; }
[[nodiscard]] inline bool isP07(const Cell& c) noexcept { return c.op >= Op::ViewUpsertNew && c.op <= Op::ViewPrefixScan; }

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

// ---- Part 06 (autoincrement): 6 ops x 7 entities x 21 timings ----
/// The constraint id of the Constrained entity's TestReject member (object id 7).
inline constexpr uint32_t kConstraintId06 = (7u << 16) | 1u;
[[nodiscard]] inline bool isCreate06(Op op) noexcept { return op == Op::AutoCreate1 || op == Op::AutoCreate8; }

/// The code (0 = Ok) the record of an import, update or remove is rejected with at the Single timing; `constrained`: the TestReject{TagEquals{9}} member is in
/// the constraint set.  Only the update (tag 9) meets the member.  A create has no verdict of its own: its trailer is the member's, from the model.
[[nodiscard]] inline uint16_t verdict06(Op op, bool constrained) {
    switch (op) {
    case Op::ImportBelow: return 56;
    case Op::UpdateAssigned: return constrained ? 55 : 0;
    default: return 0;   // ImportAbove, RemoveExistingAssigned
    }
}

/// The codes of an import, update or remove sent twice, in offset order: as two records of one batch (SelfCollidingBatch) or as batches of two producers in
/// one pass (Racing).  The second meets what the first did: the id imported, or removed.  NearMax x ImportAbove is refused first.
[[nodiscard]] inline std::pair<uint16_t, uint16_t> collidingCodes06(Op op, Entity e) {
    switch (op) {
    case Op::ImportBelow: return {56, 56};
    case Op::ImportAbove: return {0, 56};
    case Op::UpdateAssigned: return e == Entity::Constrained ? std::pair<uint16_t, uint16_t>{55, 55} : std::pair<uint16_t, uint16_t>{0, 0};
    default: return {0, 57};   // RemoveExistingAssigned
    }
}

[[nodiscard]] inline Expected expect06(const Cell& c) {
    if ((c.timing == Timing::ReplaceDuringWrites || c.timing == Timing::DropDuringWrites) && c.entity != Entity::Constrained)
        return {false, "no constraint member to replace or drop"};
    if (c.entity == Entity::NearMax && c.op == Op::AutoCreate8) return {true, nullptr, Check::AutoExhausted};
    if (c.entity == Entity::NearMax && c.op == Op::ImportAbove) return {true, nullptr, Check::ImportOutOfRange};
    switch (c.timing) {
    case Timing::FailoverAppendHw: case Timing::TruncateTail:
        return {true, nullptr, c.entity == Entity::NearMax && c.op == Op::AutoCreate1 ? Check::NotLeaderThenExhausted : Check::NotLeaderFirst};
    case Timing::LeaderLossParked: return {true, nullptr, Check::NotLeaderParked};
    case Timing::AckDeadline: return {true, nullptr, Check::DeadlineInternal};
    case Timing::RetryAfterWindow: return {true, nullptr, Check::RetryOutsideWindow};
    case Timing::RetryBeforeJudge: case Timing::RetryAfterJudge: return {true, nullptr, Check::RetryReplays};
    default: break;
    }
    if (c.entity == Entity::Constrained && c.op == Op::AutoCreate8) return {true, nullptr, Check::Burned};
    if (c.timing == Timing::Racing) return {true, nullptr, isCreate06(c.op) ? Check::RacingIds : Check::CollidingPair};
    if (c.timing == Timing::SelfCollidingBatch) return {true, nullptr, isCreate06(c.op) ? Check::Trailer : Check::CollidingPair};
    return {true, nullptr, isCreate06(c.op) ? Check::Trailer : Check::AutoVerdict};
}

// ---- Part 07 (wide view keys): 9 ops x 9 key shapes, the timings of the op's kind ----
[[nodiscard]] inline bool isComposite07(Entity e) noexcept { return e >= Entity::KeyComposite && e <= Entity::KeyMaxWidth; }
[[nodiscard]] inline bool isLegacy07(Entity e) noexcept { return e >= Entity::KeyU16 && e <= Entity::KeyI64; }
[[nodiscard]] inline bool isRead07(Op op) noexcept { return op == Op::ViewPointRead || op == Op::ViewPrefixScan; }

[[nodiscard]] inline Expected expect07(const Cell& c) {
    if (c.op == Op::ViewRename && !(c.entity == Entity::KeyCompositeWide || c.entity == Entity::KeyLower || c.entity == Entity::KeyMaxWidth))
        return {false, "only a derived or wide key renames: the other keys are the source key or a column that moves the group"};
    if (c.op == Op::ViewMoveGroup && !(c.entity == Entity::KeyComposite || c.entity == Entity::KeyCompositeWide))
        return {false, "only a composite key with a leading column moves a group"};
    if (c.op == Op::ViewPrefixScan && !isComposite07(c.entity)) return {false, "a prefix scan needs a composite key"};
    return {};
}

// ---- Part 09 (view constraints): 7 ops x 11 entities x 15 timings ----
/// A character of the Part 09 scenarios.  id 0 = a key-0 create (the broker assigns the id); intent 1 = create, 2 = update, 0 = upsert.
struct Char09 { uint64_t id = 0; uint64_t userId = 0; std::string name; int64_t lastPlayed = 0; uint8_t intent = 0; bool del = false; };
/// `setup` is written before the constrained views exist (unjudged below their change records); `op` is the batch the cell sends.
struct Scenario09 { std::vector<Char09> setup; std::vector<Char09> op; };

inline constexpr uint64_t kUserU09 = 1, kUserW09 = 2, kUserV09 = 3;
/// Source keys sit above the counter's first reservation, so the first setup write moves the counter past all of them.
inline constexpr uint64_t kIdBase09 = 100000;
inline constexpr uint32_t kNameCid09 = 7u << 16, kStatsCid09 = (8u << 16) | 1u;   ///< character_name (ordinal 0), account_stats (ordinal 1)

[[nodiscard]] inline bool isDup09(Entity e) noexcept { return e == Entity::DupOwner || e == Entity::DupExtra; }
[[nodiscard]] inline bool isSelf09(Entity e) noexcept { return e == Entity::G11Self || e == Entity::G12Self || e == Entity::OverSelf; }
[[nodiscard]] inline bool isTaken09(Entity e) noexcept { return e == Entity::G11Taken || e == Entity::G12Taken || e == Entity::OverTaken; }

[[nodiscard]] inline Scenario09 scenario09(Op op, Entity e) {
    const auto id = [](uint64_t k) { return kIdBase09 + k; };
    const int n = (e == Entity::G11Free || e == Entity::G11Taken || e == Entity::G11Self) ? 11
                : (e == Entity::G12Free || e == Entity::G12Taken || e == Entity::G12Self) ? 12
                : isDup09(e) ? 5 : 14;
    Scenario09 s;
    for (int k = 1; k <= n; ++k) s.setup.push_back({id(static_cast<uint64_t>(k)), kUserU09, "u" + std::to_string(k)});
    if (isDup09(e)) {
        s.setup[4].name = "Dup";
        s.setup.push_back({id(9), kUserW09, "dup"});
        s.setup.push_back({id(101), kUserW09, "w2"});
        s.setup.push_back({id(102), kUserW09, "w3"});
        if (e == Entity::DupOwner) s.setup.push_back({id(12), kUserW09, "DUP"});   // a second extra, so that the promoted one is the smallest of two
    } else {
        s.setup.push_back({id(101), kUserW09, isTaken09(e) ? "foo" : "w1"});
        s.setup.push_back({id(102), kUserW09, "w2"});
        s.setup.push_back({id(103), kUserW09, "w3"});
    }
    if (op == Op::CMove)
        for (uint64_t k = 1; k <= 5; ++k) s.setup.push_back({id(200 + k), kUserV09, "v" + std::to_string(k)});

    const Char09 c = e == Entity::DupOwner ? s.setup[4] : e == Entity::DupExtra ? s.setup[static_cast<std::size_t>(n)] : s.setup.front();
    const Char09 moved = s.setup.size() > static_cast<std::size_t>(n) + 3 ? s.setup[static_cast<std::size_t>(n) + 3] : c;   // CMove: the first of V's
    std::string x = isTaken09(e) ? (e == Entity::G11Taken ? "Foo" : "foo") : "Xnew";
    if (isSelf09(e)) x = "U1";
    if (isDup09(e)) x = op == Op::CRename ? "Renamed" : "DUP";
    switch (op) {
    case Op::CCreate: s.op = {{0, kUserU09, x, 0, 1}}; break;
    case Op::CUpdateNonKey: s.op = {{c.id, c.userId, c.name, 777, 2}}; break;
    case Op::CRename: s.op = {{c.id, c.userId, x, 0, 2}}; break;
    case Op::CMove: s.op = {{moved.id, kUserU09, moved.name, 0, 2}}; break;
    case Op::CRemove: s.op = {{c.id, 0, "", 0, 0, true}}; break;
    case Op::CRemoveExisting: s.op = {{c.id, 0, "", 0, 2, true}}; break;
    default: s.op = {{c.id, 0, "", 0, 0, true}, {0, kUserU09, c.name, 0, 1}}; break;   // CDeleteThenReuse
    }
    return s;
}

/// The verdict of one op record at the Single timing: Ok, N (character_name, ordinal 0) or M (account_stats, ordinal 1).
enum class V09 : uint8_t { Ok, N, M };

/// The Single table of the plan's Interaction matrix A, one entry per record of the op's batch; empty = the pair is not applicable.
[[nodiscard]] inline std::vector<V09> single09(Op op, Entity e) {
    constexpr V09 O = V09::Ok, N = V09::N, M = V09::M;
    switch (op) {
    case Op::CCreate:
        switch (e) {
        case Entity::G11Free: return {O};
        case Entity::G12Free: case Entity::OverFree: return {M};
        case Entity::G11Taken: case Entity::G12Taken: case Entity::OverTaken: case Entity::DupOwner: return {N};
        default: return {};
        }
    case Op::CUpdateNonKey:
        return isSelf09(e) || isDup09(e) ? std::vector<V09>{O} : std::vector<V09>{};
    case Op::CRename:
        switch (e) {
        case Entity::G11Taken: case Entity::G12Taken: case Entity::OverTaken: return {N};
        default: return {O};
        }
    case Op::CMove:
        switch (e) {
        case Entity::G11Self: return {O};
        case Entity::G12Self: case Entity::OverSelf: return {M};
        default: return {};
        }
    case Op::CRemove: case Op::CRemoveExisting:
        return isSelf09(e) || isDup09(e) ? std::vector<V09>{O} : std::vector<V09>{};
    case Op::CDeleteThenReuse:
        switch (e) {
        case Entity::G11Self: case Entity::G12Self: return {O, O};
        case Entity::OverSelf: return {O, M};
        case Entity::DupOwner: return {O, N};
        default: return {};
        }
    default: return {};
    }
}

// ---- Part 11 (transaction votes): the Single table ----
/// A verdict of the Single table: Ok, N (character_name), M (account_stats), K (KeyNotFound) or C (TxnConflict).
enum class V11 : uint8_t { Ok, N, M, K, C };

/// The Single table of the plan's Interaction matrix A: `character`'s verdict of the transaction (or atomic batch) and, for OutsideWriteLockedRow, of
/// producer 78's create after the vote; for RacingTxnSameName, of the second transaction (the second batch).  Empty = the pair is not applicable.
/// The ledger partition's own vote (SecondPartitionNo) is not `character`'s.
[[nodiscard]] inline std::vector<V11> single11(Op op, Entity e) {
    constexpr V11 O = V11::Ok, N = V11::N, M = V11::M, K = V11::K, C = V11::C;
    const bool create = op == Op::TxnCreate || op == Op::TxnSelfCollidingCreates;
    switch (e) {
    case Entity::AllPass: return {op == Op::TxnSelfCollidingCreates ? N : O};
    case Entity::SecondPartitionNo: return op == Op::AtomicBatch ? std::vector<V11>{} : std::vector<V11>{op == Op::TxnSelfCollidingCreates ? N : O};
    case Entity::FailsUnique: return {op == Op::TxnCreate || op == Op::TxnRename || op == Op::AtomicBatch || op == Op::TxnSelfCollidingCreates ? N : O};
    case Entity::FailsCheck: return {create || op == Op::TxnMove || op == Op::AtomicBatch ? M : O};
    case Entity::KeyAbsent: return create || op == Op::AtomicBatch ? std::vector<V11>{} : std::vector<V11>{K};
    case Entity::OutsideWriteSameKey: return create || op == Op::AtomicBatch ? std::vector<V11>{} : std::vector<V11>{C};
    case Entity::OutsideWriteLockedRow:
        switch (op) {
        case Op::TxnCreate: case Op::TxnMove: return {O, M};
        case Op::TxnSelfCollidingCreates: return {N, O};
        case Op::AtomicBatch: return {M, O};
        default: return {O, O};
        }
    default:   // RacingTxnSameName
        switch (op) {
        case Op::TxnCreate: case Op::TxnRename: return {O, N};
        case Op::TxnMove: return {O, M};
        case Op::TxnRemoveExisting: return {O, K};
        case Op::TxnSelfCollidingCreates: return {N, O};
        case Op::AtomicBatch: return {O, N};
        default: return {O, O};
        }
    }
}

[[nodiscard]] inline Expected expect09(const Cell& c) {
    if (c.op == Op::CCreate && (isSelf09(c.entity) || c.entity == Entity::DupExtra))
        return {false, c.entity == Entity::DupExtra ? "the duplicate's name is the DupOwner cell" : "a new record owns no name yet"};
    if ((c.op == Op::CUpdateNonKey || c.op == Op::CRemove || c.op == Op::CRemoveExisting || c.op == Op::CDeleteThenReuse) && c.entity <= Entity::OverTaken)
        return {false, "the op keeps C's own name"};
    if (c.op == Op::CDeleteThenReuse && c.entity == Entity::DupExtra) return {false, "covered by CRemove x DupExtra"};
    if (c.op == Op::CMove && !isSelf09(c.entity)) return {false, "a move keeps the name; the entity names the target group"};
    if (c.timing == Timing::RetryAfterWindow) return {true, nullptr, Check::RetryOutsideWindow};
    return {true, nullptr, c.timing == Timing::Single ? Check::ConstraintTable : Check::Trailer};
}

[[nodiscard]] inline Expected expect(const Cell& c) { return isP09(c) ? expect09(c) : isP07(c) ? expect07(c) : isP06(c) ? expect06(c) : isP05(c) ? expect05(c) : isP03(c) ? expect03(c) : expect04(c); }

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
    } else if (part == Part::P06) {
        const Timing t06[] = {Timing::Single, Timing::BackToBack, Timing::SelfCollidingBatch, Timing::Racing, Timing::RetryBeforeJudge, Timing::RetryAfterJudge,
                              Timing::FailoverAppendHw, Timing::FailoverHwFold, Timing::FailoverFoldMarker, Timing::TruncateTail, Timing::RestartDuringRebuild,
                              Timing::CreateOnNonEmpty, Timing::ReplaceDuringWrites, Timing::DropDuringWrites,
                              Timing::TxnCommitBehind, Timing::TxnAbortBehind, Timing::LeaderLossParked, Timing::AckDeadline, Timing::StrictTier, Timing::GatewayRouted,
                              Timing::RetryAfterWindow};
        for (const Op op : {Op::AutoCreate1, Op::AutoCreate8, Op::ImportBelow, Op::ImportAbove, Op::UpdateAssigned, Op::RemoveExistingAssigned})
            for (const Entity e : {Entity::P1U64, Entity::P4U64, Entity::P1U32, Entity::P2I64, Entity::AtReservationEdge, Entity::NearMax, Entity::Constrained})
                for (const Timing t : t06) out.push_back({op, e, t});
    } else if (part == Part::P07) {
        const Timing writes[] = {Timing::Single, Timing::BackToBack, Timing::SelfCollidingBatch, Timing::FailoverHwFold, Timing::RestartDuringRebuild,
                                 Timing::CreateOnNonEmpty, Timing::ReplaceDuringWrites, Timing::DropDuringWrites};
        const Timing retries[] = {Timing::RetryBeforeJudge, Timing::RetryAfterJudge, Timing::FailoverHwFold};
        const Timing reads[] = {Timing::Single, Timing::BackToBack, Timing::FailoverHwFold, Timing::RestartDuringRebuild, Timing::CreateOnNonEmpty};
        for (uint16_t op = static_cast<uint16_t>(Op::ViewUpsertNew); op <= static_cast<uint16_t>(Op::ViewPrefixScan); ++op)
            for (uint16_t e = static_cast<uint16_t>(Entity::KeyU16); e <= static_cast<uint16_t>(Entity::KeyMaxWidth); ++e) {
                const Op o = static_cast<Op>(op);
                const std::vector<Timing> ts = o == Op::ViewDuplicateRetry ? std::vector<Timing>(std::begin(retries), std::end(retries))
                                              : isRead07(o) ? std::vector<Timing>(std::begin(reads), std::end(reads))
                                                            : std::vector<Timing>(std::begin(writes), std::end(writes));
                for (const Timing t : ts) out.push_back({o, static_cast<Entity>(e), t});
            }
    } else if (part == Part::P09) {
        const Timing t09[] = {Timing::Single, Timing::BackToBack, Timing::SelfCollidingBatch, Timing::Racing, Timing::RetryBeforeJudge, Timing::RetryAfterJudge,
                              Timing::FailoverAppendHw, Timing::FailoverHwFold, Timing::FailoverFoldMarker, Timing::TruncateTail, Timing::RestartDuringRebuild,
                              Timing::CreateOnNonEmpty, Timing::ReplaceDuringWrites, Timing::DropDuringWrites, Timing::RetryAfterWindow};
        for (uint16_t op = static_cast<uint16_t>(Op::CCreate); op <= static_cast<uint16_t>(Op::CDeleteThenReuse); ++op)
            for (uint16_t e = static_cast<uint16_t>(Entity::G11Free); e <= static_cast<uint16_t>(Entity::DupExtra); ++e)
                for (const Timing t : t09) out.push_back({static_cast<Op>(op), static_cast<Entity>(e), t});
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

/// The group-by block of Part 07: the aggregate column under test (the rules of the device_history view, one at a time).
enum class AggRule07 : uint8_t { CountAll, SumBytes, MinLastSeen, MaxLastSeen, LastPlatform };
struct AggCell07 { Cell cell; AggRule07 rule; };

[[nodiscard]] inline std::vector<AggCell07> aggCellsFor07() {
    std::vector<AggCell07> out;
    const Timing writes[] = {Timing::Single, Timing::BackToBack, Timing::RestartDuringRebuild, Timing::FailoverHwFold};
    const Timing scans[] = {Timing::Single, Timing::RestartDuringRebuild, Timing::FailoverHwFold};
    for (const AggRule07 r : {AggRule07::CountAll, AggRule07::SumBytes, AggRule07::MinLastSeen, AggRule07::MaxLastSeen, AggRule07::LastPlatform})
        for (const Entity e : {Entity::KeyComposite, Entity::KeyLower}) {
            for (const Op o : {Op::ViewUpsertNew, Op::ViewUpdateNonKey, Op::ViewMoveGroup, Op::ViewDelete})
                for (const Timing t : writes) out.push_back({{o, e, t}, r});
            for (const Timing t : scans) out.push_back({{Op::ViewPrefixScan, e, t}, r});
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
    case Op::Remove: return "Remove"; case Op::RemoveExisting: return "RemoveExisting";
    case Op::AutoCreate1: return "AutoCreate1"; case Op::AutoCreate8: return "AutoCreate8"; case Op::ImportBelow: return "ImportBelow";
    case Op::ViewUpsertNew: return "ViewUpsertNew"; case Op::ViewUpdateNonKey: return "ViewUpdateNonKey"; case Op::ViewRename: return "ViewRename";
    case Op::ViewMoveGroup: return "ViewMoveGroup"; case Op::ViewDelete: return "ViewDelete"; case Op::ViewDeleteThenReuse: return "ViewDeleteThenReuse";
    case Op::ViewDuplicateRetry: return "ViewDuplicateRetry"; case Op::ViewPointRead: return "ViewPointRead"; case Op::ViewPrefixScan: return "ViewPrefixScan";
    case Op::ImportAbove: return "ImportAbove"; case Op::UpdateAssigned: return "UpdateAssigned"; case Op::RemoveExistingAssigned: return "RemoveExistingAssigned"; case Op::CCreate: return "CCreate"; case Op::CUpdateNonKey: return "CUpdateNonKey"; case Op::CRename: return "CRename"; case Op::CMove: return "CMove";
    case Op::CRemove: return "CRemove"; case Op::CRemoveExisting: return "CRemoveExisting"; case Op::CDeleteThenReuse: return "CDeleteThenReuse";
    case Op::TxnCreate: return "TxnCreate"; case Op::TxnRename: return "TxnRename"; case Op::TxnMove: return "TxnMove";
    case Op::TxnRemoveExisting: return "TxnRemoveExisting"; case Op::TxnSelfCollidingCreates: return "TxnSelfCollidingCreates";
    case Op::TxnUpdateNonKey: return "TxnUpdateNonKey"; case Op::AtomicBatch: return "AtomicBatch";
    case Op::ViewAckLV: return "ViewAckLV"; case Op::ViewAckAV: return "ViewAckAV";
    }
    return "?";
}
inline const char* nameOf(Entity e) {
    switch (e) {
    case Entity::RawSubscription: return "RawSubscription"; case Entity::GroupConsumer: return "GroupConsumer";
    case Entity::FilteredSubscription: return "FilteredSubscription"; case Entity::ReplayFromStart: return "ReplayFromStart";
    case Entity::OtherViews: return "OtherViews"; case Entity::TwoVerdictViews: return "TwoVerdictViews";
    case Entity::ViewAckWriter: return "ViewAckWriter"; case Entity::OldWriters: return "OldWriters";
    case Entity::G11Free: return "G11Free"; case Entity::G11Taken: return "G11Taken"; case Entity::G12Free: return "G12Free"; case Entity::G12Taken: return "G12Taken";
    case Entity::OverFree: return "OverFree"; case Entity::OverTaken: return "OverTaken"; case Entity::G11Self: return "G11Self"; case Entity::G12Self: return "G12Self";
    case Entity::OverSelf: return "OverSelf"; case Entity::DupOwner: return "DupOwner"; case Entity::DupExtra: return "DupExtra";
    case Entity::AllPass: return "AllPass"; case Entity::FailsUnique: return "FailsUnique"; case Entity::FailsCheck: return "FailsCheck";
    case Entity::SecondPartitionNo: return "SecondPartitionNo"; case Entity::KeyAbsent: return "KeyAbsent";
    case Entity::OutsideWriteSameKey: return "OutsideWriteSameKey"; case Entity::OutsideWriteLockedRow: return "OutsideWriteLockedRow";
    case Entity::RacingTxnSameName: return "RacingTxnSameName";
    case Entity::AckNoViews: return "AckNoViews"; case Entity::AckOneView: return "AckOneView";
    case Entity::AckTwoViewsLate: return "AckTwoViewsLate"; case Entity::AckTransientSource: return "AckTransientSource";
    case Entity::KeyNew: return "KeyNew"; case Entity::KeyExisting: return "KeyExisting"; case Entity::KeyEarlierInBatch: return "KeyEarlierInBatch";
    case Entity::KeyRemovedEarlier: return "KeyRemovedEarlier"; case Entity::KeyRejectedEarlier: return "KeyRejectedEarlier";
    case Entity::KeyBeforeIndex: return "KeyBeforeIndex";
    case Entity::KeyU16: return "KeyU16"; case Entity::KeyU32: return "KeyU32"; case Entity::KeyU64: return "KeyU64"; case Entity::KeyI64: return "KeyI64";
    case Entity::KeyStr16: return "KeyStr16"; case Entity::KeyComposite: return "KeyComposite"; case Entity::KeyCompositeWide: return "KeyCompositeWide";
    case Entity::KeyLower: return "KeyLower"; case Entity::KeyMaxWidth: return "KeyMaxWidth";
    case Entity::P1U64: return "P1U64"; case Entity::P4U64: return "P4U64"; case Entity::P1U32: return "P1U32"; case Entity::P2I64: return "P2I64";
    case Entity::AtReservationEdge: return "AtReservationEdge"; case Entity::NearMax: return "NearMax"; case Entity::Constrained: return "Constrained";
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
