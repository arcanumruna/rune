#pragma once
// ---------------------------------------------------------------------------
// MetadataService.h — MD1 Task 4: thin driver that owns the MSM
// (rune::metadata::MetadataStateMachine) and applies committed metadata-log
// records on the duty cycle.
//
// Design (mirrors GroupCoordinationService / TransactionCoordinationService):
//   - owns a MetadataStateMachine (the committed partition-assignment map)
//   - on construction: caller supplies the metadata PartitionReplica* (topicId=-3)
//   - doWork(nowNs): reads the replica's highWatermark() and calls
//       msm_.commit(hw) so all pending records with pos <= hw become visible.
//     Also replays any new batches from the committed log tail on the
//     leader-transition edge (wasLeader_ gate, mirrors TCS::doWork).
//   - rebuildFromLog(): replays the entire committed log [0, HW) and rebuilds
//     the MSM from scratch — called on restart or leader failover
//     (mirrors ReplicatedTxnStateStore::rebuildFromLog).
//   - assignment(topicId, partition): delegates to msm_.assignment().
//
// HW-gated apply:
//   The MSM uses a pendingPos buffer.  A record is NOT visible via assignment()
//   until commit(hw) is called with hw >= record's log position.  doWork()
//   calls commit(replica->highWatermark()) each cycle — so assignment() reflects
//   only COMMITTED records (the HW-gate invariant).
//
// Single-writer: all methods must be called on the duty-cycle thread.
//   No locks.  No hot-path allocation beyond the first doWork after a burst.
//
// CONTROLLER RULING (FINDING-1):
//   The __cluster_metadata partition's OWN leadership follows cluster-consensus
//   leadership, NOT the assignment map.  The existing ReplicationMetadataBridge
//   broadcasts cluster leadership to ALL registered replicas, so simply
//   registering metadataReplica_ in ReplicaRegistry makes the bridge drive its
//   leader/follower role automatically.  MetadataService does NOT attempt to
//   drive its own replica's role.
//
// Namespace: rune::node
// C++23. Single-writer. No locks.
// ---------------------------------------------------------------------------

#include <rune/metadata/MetadataStateMachine.h>
#include <rune/metadata/PartitionAssignment.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/protocol/RecordBatch.h>  // RecordBatchReader

#include <cstdint>
#include <vector>

namespace rune::node {

// ---------------------------------------------------------------------------
// MetadataService
// ---------------------------------------------------------------------------
class MetadataService {
public:
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// @param replica  Non-owning pointer to the __cluster_metadata
    ///                 PartitionReplica (topicId=-3, partitionId=0).
    ///                 Must outlive this object.
    /// @param selfId   This node's NodeId (for logging / delta derivation).
    MetadataService(rune::replication::PartitionReplica* replica,
                    int32_t selfId)
        : replica_(replica)
        , selfId_(selfId)
    {}

    MetadataService(const MetadataService&)            = delete;
    MetadataService& operator=(const MetadataService&) = delete;

    // -----------------------------------------------------------------------
    // Duty-cycle entry point (single-writer thread)
    // -----------------------------------------------------------------------

    /// Apply committed records from the metadata log.
    ///
    /// On every cycle:
    ///   1. Read highWatermark() from the replica.
    ///   2. Read new batches in (lastApplied_, hw] from the log.
    ///   3. For each record in those batches, call msm_.apply().
    ///   4. Call msm_.commit(hw) to move pending→committed.
    ///
    /// On leader-transition edge (wasLeader_ gate):
    ///   - calls rebuildFromLog() to rebuild from the committed tail.
    ///
    /// \returns non-zero if any records were applied this cycle.
    int doWork(int64_t nowNs) {
        if (!replica_) return 0;

        // Detect Follower→Leader transition (same pattern as TCS::doWork).
        const bool amLeader = (replica_->role() == rune::replication::Role::Leader);
        if (amLeader && !wasLeader_) {
            // Became leader: rebuild the MSM from the committed log so we have
            // the full assignment map before serving requests.
            rebuildFromLog();
            wasLeader_ = true;
        } else if (!amLeader && wasLeader_) {
            wasLeader_ = false;
            // Follower: reset lastApplied_ so we re-apply from scratch if we
            // become leader again later.
            lastApplied_ = -1;
        }

        const rune::replication::Offset hw = replica_->highWatermark();
        if (hw <= 0 || hw <= lastApplied_) {
            // Nothing new to apply.  Still call commit() so pending records
            // already decoded in a prior partial cycle get committed.
            msm_.commit(hw - 1);
            return 0;
        }

        // Read new batches [lastApplied_+1 .. hw) from the log.
        const rune::replication::Offset fetchFrom =
            (lastApplied_ < 0) ? 0 : lastApplied_;
        const int32_t kChunkBytes = 64 * 1024 * 1024; // 64 MiB per chunk

        int applied = 0;
        rune::replication::Offset cursor = fetchFrom;

        while (cursor < hw) {
            const std::vector<uint8_t> data =
                replica_->read(cursor, kChunkBytes);
            if (data.empty()) break;

            std::size_t pos = 0;
            rune::replication::Offset lastBatchEnd = cursor;

            while (pos < data.size()) {
                const uint8_t*    batchStart = data.data() + pos;
                const std::size_t remaining  = data.size() - pos;

                if (remaining < static_cast<std::size_t>(
                        rune::protocol::batch::kRecordBatchOverhead))
                    break;

                rune::protocol::RecordBatchReader reader(batchStart, remaining);
                if (!reader.ensureValid()) break;

                const int32_t tl = reader.totalLength();
                if (tl <= 0 || static_cast<std::size_t>(tl) > remaining) break;

                // Compute the log position of this batch.
                reader.forEach([this, &applied](
                        std::string_view /*key*/,
                        std::string_view val,
                        int64_t logOffset,  // base + offDelta (absolute log position)
                        int64_t /*ts*/) {
                    msm_.apply(
                        reinterpret_cast<const uint8_t*>(val.data()),
                        val.size(),
                        logOffset);
                    ++applied;
                });

                const rune::replication::Offset batchNextOffset =
                    reader.baseOffset() + reader.lastOffsetDelta() + 1;
                if (batchNextOffset > lastBatchEnd)
                    lastBatchEnd = batchNextOffset;

                pos += static_cast<std::size_t>(tl);
            }

            if (lastBatchEnd <= cursor) break; // no progress
            cursor = lastBatchEnd;
        }

        // Commit all records up to hw-1 (log offsets are 0-indexed;
        // hw is the next-unwritten offset so the last committed is hw-1).
        msm_.commit(hw - 1);
        if (cursor > lastApplied_) lastApplied_ = cursor;

        return applied;
    }

    // -----------------------------------------------------------------------
    // Rebuild (called on restart or leader-transition edge)
    // -----------------------------------------------------------------------

    /// Replay the replica Log [0, highWatermark()) and reconstruct the MSM
    /// from scratch.  Mirrors ReplicatedTxnStateStore::rebuildFromLog().
    ///
    /// @returns true if any records were replayed (HW > 0).
    bool rebuildFromLog() {
        if (!replica_) return false;

        const rune::replication::Offset hw = replica_->highWatermark();

        // Reset state.
        msm_ = rune::metadata::MetadataStateMachine{};
        lastApplied_ = -1;

        if (hw <= 0) return false;

        static constexpr int32_t kChunkBytes = 64 * 1024 * 1024;
        rune::replication::Offset cursor = 0;

        while (cursor < hw) {
            const std::vector<uint8_t> data =
                replica_->read(cursor, kChunkBytes);
            if (data.empty()) break;

            std::size_t pos = 0;
            rune::replication::Offset lastBatchEnd = cursor;

            while (pos < data.size()) {
                const uint8_t*    batchStart = data.data() + pos;
                const std::size_t remaining  = data.size() - pos;

                if (remaining < static_cast<std::size_t>(
                        rune::protocol::batch::kRecordBatchOverhead))
                    break;

                rune::protocol::RecordBatchReader reader(batchStart, remaining);
                if (!reader.ensureValid()) break;

                const int32_t tl = reader.totalLength();
                if (tl <= 0 || static_cast<std::size_t>(tl) > remaining) break;

                reader.forEach([this](
                        std::string_view /*key*/,
                        std::string_view val,
                        int64_t logOffset,  // absolute log position
                        int64_t /*ts*/) {
                    msm_.apply(
                        reinterpret_cast<const uint8_t*>(val.data()),
                        val.size(),
                        logOffset);
                });

                const rune::replication::Offset batchNextOffset =
                    reader.baseOffset() + reader.lastOffsetDelta() + 1;
                if (batchNextOffset > lastBatchEnd)
                    lastBatchEnd = batchNextOffset;

                pos += static_cast<std::size_t>(tl);
            }

            if (lastBatchEnd <= cursor) break;
            cursor = lastBatchEnd;
        }

        // Commit everything up to hw-1.
        msm_.commit(hw - 1);
        lastApplied_ = cursor;
        return true;
    }

    // -----------------------------------------------------------------------
    // Query
    // -----------------------------------------------------------------------

    /// Return the committed assignment for (topicId, partition).
    /// Returns nullptr if unknown or not yet committed.
    [[nodiscard]]
    const rune::metadata::PartitionAssignment*
    assignment(int32_t topicId, int32_t partition) const noexcept {
        return msm_.assignment(topicId, partition);
    }

    /// Return the full committed AssignmentMap (read-only).
    [[nodiscard]]
    const rune::metadata::AssignmentMap& committedMap() const noexcept {
        return msm_.committedMap();
    }

    // -----------------------------------------------------------------------
    // Test-only / diagnostic
    // -----------------------------------------------------------------------

    /// Return the raw MSM (for snapshot / delta derivation in tests).
    rune::metadata::MetadataStateMachine& msmForTest() noexcept { return msm_; }

private:
    rune::replication::PartitionReplica* replica_;   ///< non-owning
    int32_t                               selfId_;

    rune::metadata::MetadataStateMachine  msm_;

    // Last log offset we have read batches from.  Initialized to -1 (nothing applied).
    rune::replication::Offset lastApplied_ = -1;

    // Follower→Leader transition gate (mirrors TCS / GCS patterns).
    bool wasLeader_ = false;
};

} // namespace rune::node
