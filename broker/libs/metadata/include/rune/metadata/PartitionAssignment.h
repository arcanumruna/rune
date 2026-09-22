// PartitionAssignment.h — two-epoch in-memory assignment model.
// Consumed by: MSM apply, balancer, replication bridge, routing image.
//
// STUB — tests will be compiled against this; implementation follows TDD RED phase.

#pragma once

#include <cstdint>
#include <vector>

namespace rune::metadata {

// ─────────────────────────────────────────────────────────────────────────────
// Sentinel values
// ─────────────────────────────────────────────────────────────────────────────

/// leaderNode value meaning "no current leader".
inline constexpr int32_t NO_LEADER        = -1;

/// newLeader value in AssignmentChange meaning "leader field is unchanged".
/// Using -2 avoids collision with NO_LEADER (-1) and any valid node ID (≥ 0).
inline constexpr int32_t NO_LEADER_CHANGE = -2;

/// Empty newReplicas vector in AssignmentChange means "replica set unchanged".
/// This constant is provided for documentation; the actual check is `.empty()`.
// (no separate sentinel needed — we rely on the empty vector convention)

// ─────────────────────────────────────────────────────────────────────────────
// Core types
// ─────────────────────────────────────────────────────────────────────────────

/// In-memory snapshot of a single partition's assignment at a point in time.
struct PartitionAssignment {
    int32_t              leaderNode;      ///< Current leader; NO_LEADER (-1) if none.
    std::vector<int32_t> isr;             ///< In-sync replica set (ordered by convention).
    std::vector<int32_t> replicas;        ///< Full replica set.
    int32_t              leaderEpoch;     ///< Bumps on leader change or replica removal.
    int32_t              partitionEpoch;  ///< Bumps on every applied change.
};

/// Describes a requested mutation.  Fields use sentinel values to mean "unchanged".
struct AssignmentChange {
    /// New leader node ID, or NO_LEADER (-1) to make leaderless,
    /// or NO_LEADER_CHANGE (-2) to leave leader field untouched.
    int32_t              newLeader   = NO_LEADER_CHANGE;

    /// Replacement ISR.  Any non-no-op ISR value triggers a partitionEpoch bump.
    std::vector<int32_t> newIsr;

    /// Replacement replica set.  Empty ⟹ "unchanged" (NO_REPLICA_CHANGE convention).
    /// A non-empty vector with a member *removed* from cur.replicas forces a
    /// leaderEpoch bump even when newLeader == NO_LEADER_CHANGE.
    std::vector<int32_t> newReplicas;
};

// ─────────────────────────────────────────────────────────────────────────────
// NO_REPLICA_CHANGE sentinel alias (empty vector)
// ─────────────────────────────────────────────────────────────────────────────

/// Helper — use in test/call-site code to make intent explicit.
/// Assign this to AssignmentChange::newReplicas to mean "do not change replicas".
inline const std::vector<int32_t> NO_REPLICA_CHANGE = {};

// ─────────────────────────────────────────────────────────────────────────────
// mergeChange — pure function; no I/O, no side-effects.
//
// Bump rules:
//   partitionEpoch  bumps on EVERY applied change (i.e. at least one field differs).
//   leaderEpoch     bumps when ANY of:
//       a) newLeader != NO_LEADER_CHANGE  (includes going to NO_LEADER)
//       b) a replica is *removed* from the replica set
//   leaderEpoch does NOT bump on plain ISR shrink/expand.
//   Invariant: leaderEpoch ≤ partitionEpoch always.
// ─────────────────────────────────────────────────────────────────────────────
inline PartitionAssignment mergeChange(
    const PartitionAssignment& cur,
    const AssignmentChange&    change)
{
    PartitionAssignment next = cur;  // start from current state

    // ── Apply leader field ──────────────────────────────────────────────────
    bool leaderChanged = false;
    if (change.newLeader != NO_LEADER_CHANGE) {
        if (change.newLeader != cur.leaderNode) {
            next.leaderNode = change.newLeader;
            leaderChanged   = true;
        }
        // Even if same value passed explicitly, treat it as a no-leader-change
        // (the sentinel path is the intended way; explicit same value is a no-op
        //  for leaderEpoch just like the sentinel).
    }

    // ── Apply ISR ───────────────────────────────────────────────────────────
    bool isrChanged = (change.newIsr != cur.isr);
    if (isrChanged) {
        next.isr = change.newIsr;
    }

    // ── Apply replica set ───────────────────────────────────────────────────
    bool replicaRemoved = false;
    bool replicasChanged = false;
    if (!change.newReplicas.empty()) {
        // Check whether any member was removed
        for (int32_t r : cur.replicas) {
            bool found = false;
            for (int32_t nr : change.newReplicas) {
                if (nr == r) { found = true; break; }
            }
            if (!found) { replicaRemoved = true; break; }
        }
        replicasChanged = (change.newReplicas != cur.replicas);
        next.replicas = change.newReplicas;
    }

    // ── Epoch bumps ─────────────────────────────────────────────────────────
    bool anyChange = leaderChanged || isrChanged || replicasChanged;
    if (!anyChange) {
        return next;  // no-op: neither epoch bumps
    }

    next.partitionEpoch = cur.partitionEpoch + 1;

    if (leaderChanged || replicaRemoved) {
        next.leaderEpoch = cur.leaderEpoch + 1;
    }

    return next;
}

} // namespace rune::metadata
