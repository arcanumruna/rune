// tests/support/TxnMatrixExpectations.h - Phase 2b F Task 12: the transaction interaction matrix, as data.
//
// SINGLE SOURCE OF TRUTH for "what must happen when <op> is issued on <entity> at <timing>".  The unit matrix
// (TxnMatrixTest, Task 12), the seeded stress model (Task 13) and the real-broker slice (Task 11) all read
// expect() / naRule(); no test hard-codes an outcome.  The rules are the plan's "Interaction matrix" section
// (NA1-NA3, R1-R11) verbatim, top-down; header-only and constexpr so the cell counts are pinned at compile time
// too.  Pure data: no dependency on the client.
#pragma once

#include <cstdint>
#include <optional>

namespace rune::client::txnmatrix {

enum class Op : uint8_t {
    Send,         ///< p.send(t, key, value)                                 (void)
    SendTo,       ///< p.sendTo(t, partition, key, value)
    TrySendCq,    ///< p.trySend(t, key, value, cq, token)
    TrySendAck,   ///< p.trySend(t, key, value, AckToken{token})
    Batch,        ///< p.batch(t, key).add(...)x3.send()                     (Tier-2)
    TryClaim,     ///< p.tryClaim(t, key, len)->commit()
    ClaimBatch,   ///< p.claimBatch(t, key, 3, len)->commit()
    TypedSend,    ///< TypedTopic<Position, ProducerRef>::send(rec)
    TypedClaim,   ///< TypedTopic<Position, ProducerRef>::claim(k).commit()
    SendOffsets,  ///< p.sendOffsetsToTransaction(offsets, group, cq, token)
};
inline constexpr uint8_t kOps = 10;

enum class Entity : uint8_t {
    PlainNoAck,         ///< no transactionalId, acks = None
    PlainAcked,         ///< no transactionalId, acks = All, broker-assigned id (FD4)
    TxnNewPartition,    ///< transactional; the op's partition is not yet in the current transaction
    TxnAddedPartition,  ///< transactional; already added this transaction
};
inline constexpr uint8_t kEntities = 4;

enum class Timing : uint8_t {
    Steady,             ///< initialized (txn: init OK, begin); op; (txn: commit)
    BeforeInit,         ///< txn producer created, initTransactions not called; op
    OutsideTxn,         ///< init OK, no begin (or after a completed commit); op
    AfterCommitIssued,  ///< begin; one record; commit issued (not completed); op
    AbortableError,     ///< begin; the fake answers an AddPartitions 19; op; commit; abort
    Fenced,             ///< begin; a second init with the same transactionalId bumps the epoch; the next coordinator answer is 18
    SessionLostMid,     ///< op issued; the fake drops the session with the frame unanswered; the client reconnects; (txn: commit)
    AfterAbortRewind,   ///< begin; op; abort; seekToCommitted on the group consumer; begin; the same op again; commit
};
inline constexpr uint8_t kTimings = 8;

enum class NaRule : uint8_t {
    NA1,  ///< a plain producer has no transaction phases (every timing except Steady / SessionLostMid)
    NA2,  ///< an offsets leg exists only in a transaction
    NA3,  ///< offsets carry no data partition; TxnAddedPartition stands for "the transactional producer"
};

enum class Outcome : uint8_t {
    Delivered,             ///< R1
    DeliveredOnce,         ///< R2
    AtMostOnce,            ///< R3
    Refused,               ///< R4-R6
    CommittedVisible,      ///< R7
    FailsTxn,              ///< R8
    Fenced,                ///< R9
    CommittedVisibleOnce,  ///< R10
    RedoneOnce,            ///< R11
};

[[nodiscard]] constexpr bool isPlain(Entity e) noexcept { return e == Entity::PlainNoAck || e == Entity::PlainAcked; }

/// The not-applicable table, top-down: the first matching rule names the reason.
[[nodiscard]] constexpr std::optional<NaRule> naRule(Op op, Entity e, Timing t) noexcept {
    if (isPlain(e) && t != Timing::Steady && t != Timing::SessionLostMid) return NaRule::NA1;
    if (op == Op::SendOffsets && isPlain(e)) return NaRule::NA2;
    if (op == Op::SendOffsets && e == Entity::TxnNewPartition) return NaRule::NA3;
    return std::nullopt;
}

/// The outcome of an APPLICABLE cell (R1..R11, top-down).  Undefined for a not-applicable cell (returns Delivered).
[[nodiscard]] constexpr Outcome expect(Op, Entity e, Timing t) noexcept {
    if (isPlain(e)) {
        if (t == Timing::Steady) return Outcome::Delivered;                                         // R1
        return e == Entity::PlainAcked ? Outcome::DeliveredOnce : Outcome::AtMostOnce;              // R2 / R3
    }
    switch (t) {
    case Timing::BeforeInit:        return Outcome::Refused;               // R4
    case Timing::OutsideTxn:        return Outcome::Refused;               // R5
    case Timing::AfterCommitIssued: return Outcome::Refused;               // R6
    case Timing::Steady:            return Outcome::CommittedVisible;      // R7
    case Timing::AbortableError:    return Outcome::FailsTxn;              // R8
    case Timing::Fenced:            return Outcome::Fenced;                // R9
    case Timing::SessionLostMid:    return Outcome::CommittedVisibleOnce;  // R10
    case Timing::AfterAbortRewind:  return Outcome::RedoneOnce;            // R11
    }
    return Outcome::Delivered;
}

[[nodiscard]] constexpr const char* name(Op o) noexcept {
    constexpr const char* n[] = {"Send", "SendTo", "TrySendCq", "TrySendAck", "Batch",
                                 "TryClaim", "ClaimBatch", "TypedSend", "TypedClaim", "SendOffsets"};
    return n[static_cast<uint8_t>(o)];
}
[[nodiscard]] constexpr const char* name(Entity e) noexcept {
    constexpr const char* n[] = {"PlainNoAck", "PlainAcked", "TxnNewPartition", "TxnAddedPartition"};
    return n[static_cast<uint8_t>(e)];
}
[[nodiscard]] constexpr const char* name(Timing t) noexcept {
    constexpr const char* n[] = {"Steady", "BeforeInit", "OutsideTxn", "AfterCommitIssued",
                                 "AbortableError", "Fenced", "SessionLostMid", "AfterAbortRewind"};
    return n[static_cast<uint8_t>(t)];
}
[[nodiscard]] constexpr const char* name(NaRule r) noexcept {
    constexpr const char* n[] = {"NA1", "NA2", "NA3"};
    return n[static_cast<uint8_t>(r)];
}
[[nodiscard]] constexpr const char* name(Outcome o) noexcept {
    constexpr const char* n[] = {"Delivered", "DeliveredOnce", "AtMostOnce", "Refused", "CommittedVisible",
                                 "FailsTxn", "Fenced", "CommittedVisibleOnce", "RedoneOnce"};
    return n[static_cast<uint8_t>(o)];
}

/// Every data op writes records; SendOffsets writes an offsets leg only.
[[nodiscard]] constexpr bool isDataOp(Op o) noexcept { return o != Op::SendOffsets; }

}  // namespace rune::client::txnmatrix
