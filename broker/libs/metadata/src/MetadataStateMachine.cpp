// MetadataStateMachine.cpp — deterministic fold over metadata-log records.
//
// Records are decoded from the wire format defined in MetadataRecords.h.
// Only committed records (pos <= committedPos) are visible via assignment().

#include "rune/metadata/MetadataStateMachine.h"
#include "rune/protocol/MetadataRecords.h"

#include <algorithm>

namespace rune::metadata {

using namespace rune::protocol::metadata;

// ─────────────────────────────────────────────────────────────────────────────
// apply
// ─────────────────────────────────────────────────────────────────────────────

void MetadataStateMachine::apply(const uint8_t* buf, std::size_t len, int64_t recordPos)
{
    if (len < 2) return;

    // Peek at type
    auto typeOpt = peekRecordType(buf, len);
    if (!typeOpt.has_value()) return;

    switch (*typeOpt) {
    case MetadataRecordType::CreateTopic: {
        // CreateTopicRecord: we register the topic but don't pre-create partition
        // assignments here — partitions become live only when an
        // UpdatePartitionAssignmentRecord arrives for them.
        // We still decode to validate and guard on version.
        auto rec = CreateTopicRecord::decode(buf, len);
        if (!rec.has_value()) return;  // bad version or truncated → skip
        // No partition-map entry until an assignment record arrives.
        break;
    }

    case MetadataRecordType::UpdatePartitionAssignment: {
        auto rec = UpdatePartitionAssignmentRecord::decode(buf, len);
        if (!rec.has_value()) {
            // handleLoss: unknown version or truncated — count but do not crash.
            ++lossCount_;
            return;
        }

        // Build a PartitionAssignment directly from the authoritative record values.
        // The controller already carries the correct leaderEpoch/partitionEpoch —
        // we install them verbatim (no double-bump).
        PartitionAssignment pa;
        pa.leaderNode      = rec->leaderNode;
        pa.leaderEpoch     = rec->leaderEpoch;
        pa.partitionEpoch  = rec->partitionEpoch;

        pa.isr.reserve(rec->isr.size());
        for (int32_t v : rec->isr) pa.isr.push_back(v);

        pa.replicas.reserve(rec->replicas.size());
        for (int32_t v : rec->replicas) pa.replicas.push_back(v);

        // If pos is already committed, install directly.
        if (recordPos <= committedPos_) {
            install_(rec->topicId, rec->partition, pa);
        } else {
            // Queue as pending
            pending_.push_back(PendingEntry{
                recordPos,
                rec->topicId,
                rec->partition,
                std::move(pa)
            });
        }
        break;
    }

    case MetadataRecordType::NodeJoin: {
        auto rec = NodeJoinRecord::decode(buf, len);
        if (!rec.has_value()) return;
        // NodeJoin is informational at this layer — no partition-map effect.
        // We validate and accept it to avoid crashing on it.
        (void)rec;
        break;
    }

    case MetadataRecordType::NodeLeave: {
        auto rec = NodeLeaveRecord::decode(buf, len);
        if (!rec.has_value()) return;
        // NodeLeave is informational — no partition-map effect.
        (void)rec;
        break;
    }

    default:
        // Unknown record type — deterministically skip.
        break;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// commit
// ─────────────────────────────────────────────────────────────────────────────

void MetadataStateMachine::commit(int64_t committedPos)
{
    if (committedPos <= committedPos_) return;
    committedPos_ = committedPos;
    flushPending(committedPos);
}

void MetadataStateMachine::install_(int32_t topicId, int32_t partition,
                                    const PartitionAssignment& pa)
{
    if (const PartitionAssignment* cur = committed_.get(topicId, partition)) {
        const bool regress =
            pa.leaderEpoch < cur->leaderEpoch ||
            (pa.leaderEpoch == cur->leaderEpoch && pa.leaderNode != cur->leaderNode) ||
            pa.partitionEpoch < cur->partitionEpoch;
        if (regress) { ++regressionsRejected_; return; }
    }
    committed_.put(topicId, partition, pa);
    ++committedVersion_;
}

void MetadataStateMachine::flushPending(int64_t upToPos)
{
    if (pending_.empty()) return;

    // Sort by log position to ensure deterministic installation order
    std::sort(pending_.begin(), pending_.end(),
              [](const PendingEntry& a, const PendingEntry& b) {
                  return a.pos < b.pos;
              });

    std::size_t kept = 0;
    for (auto& e : pending_) {
        if (e.pos <= upToPos) {
            install_(e.topicId, e.partition, e.assignment);
        } else {
            // Move back to the kept region
            pending_[kept++] = std::move(e);
        }
    }
    pending_.resize(kept);
}

// ─────────────────────────────────────────────────────────────────────────────
// revert
// ─────────────────────────────────────────────────────────────────────────────

void MetadataStateMachine::revert(int64_t toCommittedPos)
{
    // Discard every pending entry with pos > toCommittedPos.
    // The committed map is untouched — only uncommitted speculation is pruned.
    // O(n) in pending_ size, which is bounded by the max number of in-flight
    // (uncommitted) records.
    if (pending_.empty()) return;

    std::size_t kept = 0;
    for (auto& e : pending_) {
        if (e.pos <= toCommittedPos) {
            // Keep this entry — it was committed before the revert point.
            pending_[kept++] = std::move(e);
        }
        // else: drop (discard uncommitted speculation)
    }
    pending_.resize(kept);
}

// ─────────────────────────────────────────────────────────────────────────────
// assignment
// ─────────────────────────────────────────────────────────────────────────────

const PartitionAssignment* MetadataStateMachine::assignment(
    int32_t topicId, int32_t partition) const noexcept
{
    return committed_.get(topicId, partition);
}

// ─────────────────────────────────────────────────────────────────────────────
// deriveLocalDelta
//
// For each partition in the CURRENT committed map:
//   Look up (topicId, partition) in prev.
//   Classify for selfNode:
//     - New in current, self is leader                         → electedLeaders
//     - leaderEpoch increased, self is leader                  → electedLeaders
//     - leaderEpoch same, partitionEpoch increased, self leader → leaderUpdates
//     - partitionEpoch increased, self is replica (not leader)  → followers
//
// For each partition in PREV that is ABSENT in current:
//   If selfNode was in prev.replicas                           → deletes
// ─────────────────────────────────────────────────────────────────────────────

static bool containsNode(const std::vector<int32_t>& vec, int32_t node) noexcept {
    for (int32_t v : vec)
        if (v == node) return true;
    return false;
}

LocalRoleDelta MetadataStateMachine::deriveLocalDelta(
    int32_t selfNode, const AssignmentMap& prev) const
{
    LocalRoleDelta delta;

    // Walk current committed map
    committed_.forEach([&](int32_t topicId, int32_t partition, const PartitionAssignment& cur) {
        const PartitionAssignment* prevPa = prev.get(topicId, partition);
        const PartitionKey key{topicId, partition};

        if (prevPa == nullptr) {
            // Brand-new partition — check selfNode's role
            if (cur.leaderNode == selfNode) {
                delta.electedLeaders.push_back(key);
            } else if (containsNode(cur.replicas, selfNode)) {
                // New partition, self is a follower replica
                delta.followers.push_back(key);
            }
            return;
        }

        // Partition existed before — compare epochs
        const bool leaderEpochBumped    = (cur.leaderEpoch     > prevPa->leaderEpoch);
        const bool partitionEpochBumped = (cur.partitionEpoch  > prevPa->partitionEpoch);

        // Gate: either epoch must have advanced.  leaderEpoch alone is sufficient
        // to detect a new election (brief: electedLeader keyed on "leaderEpoch changed").
        if (!leaderEpochBumped && !partitionEpochBumped) {
            // No change — nothing to report
            return;
        }

        if (cur.leaderNode == selfNode) {
            if (leaderEpochBumped) {
                // New election: leaderEpoch advanced (partitionEpoch may or may not have)
                delta.electedLeaders.push_back(key);
            } else {
                // Self stays leader, only partitionEpoch bumped (ISR change, no re-election)
                delta.leaderUpdates.push_back(key);
            }
        } else if (containsNode(cur.replicas, selfNode)) {
            delta.followers.push_back(key);
        } else if (containsNode(prevPa->replicas, selfNode)) {
            // selfNode was a replica but has been removed from the replica set
            delta.deletes.push_back(key);
        }
    });

    // Walk prev map for partitions that were removed (absent in current)
    prev.forEach([&](int32_t topicId, int32_t partition, const PartitionAssignment& prevPa) {
        if (committed_.get(topicId, partition) == nullptr) {
            // Partition removed — if self was a replica, it's a delete
            if (containsNode(prevPa.replicas, selfNode)) {
                delta.deletes.push_back(PartitionKey{topicId, partition});
            }
        }
    });

    return delta;
}

} // namespace rune::metadata
