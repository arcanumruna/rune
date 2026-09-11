#pragma once
// ---------------------------------------------------------------------------
// Types.h — Core type aliases for the rune::transaction library.
//
// Namespace: rune::transaction
// C++23
//
// Mirrors Kafka's transaction types (KIP-98):
//   - ProducerId, ProducerEpoch, TransactionalId
//   - TxnState: the transaction state machine states
//   - TopicPartition: compatible with rune::coordination::TopicPartition
// ---------------------------------------------------------------------------

#include <cstdint>
#include <functional>
#include <string>

namespace rune::transaction {

// ---------------------------------------------------------------------------
// Producer identity types (mirror Kafka)
// ---------------------------------------------------------------------------

/// Unique identifier for a producer (int64, matches Kafka producerId).
using ProducerId = int64_t;

/// Epoch for a producer session. Incremented on InitProducerId to fence
/// the previous (zombie) producer. int16 matches Kafka's producerEpoch.
using ProducerEpoch = int16_t;

/// Transactional identifier string (group.id-like, unique per transactional producer).
using TransactionalId = std::string;

// ---------------------------------------------------------------------------
// TxnState — Kafka-faithful transaction lifecycle states (KIP-98).
//
//  Empty          — initial state, no ongoing transaction.
//  Ongoing        — addPartitions/addOffsets have been called; txn is open.
//  PrepareCommit  — endTransaction(commit) called; coordinator is writing markers.
//  PrepareAbort   — endTransaction(abort) or timeout abort triggered.
//  CompleteCommit — all COMMIT markers written; transitioning back to Empty.
//  CompleteAbort  — all ABORT markers written; transitioning back to Empty.
//  Dead           — transactionalId expired / coordinator has deleted the metadata.
//  PrepareEpochFence — bumping epoch to fence a zombie; intermediate state.
// ---------------------------------------------------------------------------
enum class TxnState : uint8_t {
    Empty,
    Ongoing,
    PrepareCommit,
    PrepareAbort,
    CompleteCommit,
    CompleteAbort,
    // A7 (justified): Dead is the KIP-98 terminal state for an EXPIRED
    // transactionalId. Rune does not yet implement txnId-expiry reaping, so no
    // code path transitions INTO Dead. It is retained (a) for on-disk/enum
    // compatibility with the persisted `state` byte and Kafka's lifecycle, and
    // (b) because initProducerId already handles a recovered/injected Dead state
    // as "re-create" (fall-through to the Empty re-init branch). Removing it now
    // would renumber the persisted enum. Wiring the expiry path is a separate
    // lifecycle feature, not a bug fix.
    Dead,
    // PrepareEpochFence: reserved transient state named in initProducerId's
    // concurrent-txn guard; not assigned by any current path (the Ongoing-fence
    // uses PrepareAbort directly). Kept for the same enum-stability reason.
    PrepareEpochFence,  // used internally during epoch-bump for zombie fencing
};

/// Human-readable name for a TxnState (useful in error messages and tests).
inline const char* txnStateName(TxnState s) noexcept {
    switch (s) {
        case TxnState::Empty:             return "Empty";
        case TxnState::Ongoing:           return "Ongoing";
        case TxnState::PrepareCommit:     return "PrepareCommit";
        case TxnState::PrepareAbort:      return "PrepareAbort";
        case TxnState::CompleteCommit:    return "CompleteCommit";
        case TxnState::CompleteAbort:     return "CompleteAbort";
        case TxnState::Dead:              return "Dead";
        case TxnState::PrepareEpochFence: return "PrepareEpochFence";
    }
    return "Unknown";
}

// ---------------------------------------------------------------------------
// TopicPartition — a (topic, partition) pair, compatible with the coordination
// lib's definition but in the rune::transaction namespace.
// ---------------------------------------------------------------------------
struct TopicPartition {
    std::string topic;
    int32_t     partition{0};

    bool operator==(const TopicPartition& o) const noexcept {
        return partition == o.partition && topic == o.topic;
    }
    bool operator<(const TopicPartition& o) const noexcept {
        if (topic != o.topic) return topic < o.topic;
        return partition < o.partition;
    }
};

} // namespace rune::transaction

// ---------------------------------------------------------------------------
// std::hash specialisation — allows TopicPartition as unordered_map key.
// ---------------------------------------------------------------------------
template<>
struct std::hash<rune::transaction::TopicPartition> {
    std::size_t operator()(const rune::transaction::TopicPartition& tp) const noexcept {
        std::size_t h = std::hash<std::string>{}(tp.topic);
        h ^= std::hash<int32_t>{}(tp.partition) + 0x9e3779b9u + (h << 6) + (h >> 2);
        return h;
    }
};
