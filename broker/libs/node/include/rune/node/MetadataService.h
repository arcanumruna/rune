#pragma once
// ---------------------------------------------------------------------------
// MetadataService.h — MD1 Task 4 + Task 6: thin driver that owns the MSM
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
// MD1 Task 6 — Controller emits static assignment on leader claim:
//   - configure(topics, liveNodes): supplies the topic + node context for
//     StaticBalancer::assign().  Must be called before start().
//   - emitStaticAssignmentIfNeeded(): called by doWork() on the Follower→Leader
//     edge.  Runs StaticBalancer::assign() and appends one record per
//     replica_->append() call — each metadata record occupies its own log slot.
//     This is the ONLY correct design: concatenating N encoded records into one
//     payload would cause only the first record to be decoded on replay.
//     Idempotency: if the MSM already has assignments for ALL user topics in
//     topics_, emitting is skipped.  The "assigned_" flag latches after the
//     first successful emit so re-observing leadership is also a no-op.
//   - balancerErrorCount(): error counter; incremented when BalancerError is
//     caught (misconfigured group, RF > nodes, etc.).  Non-zero = silent failure;
//     use this to detect bad config in tests or monitoring.
//
// Only USER topics (topicId >= 0) are assigned.
// Internal partitions (__cluster_metadata=-3, __consumer_offsets=-1,
//   __transaction_state=-2) follow cluster leadership and are NOT assigned.
//
// Namespace: rune::node
// C++23. Single-writer. No locks.
// ---------------------------------------------------------------------------

#include <rune/metadata/MetadataStateMachine.h>
#include <rune/metadata/PartitionAssignment.h>
#include <rune/metadata/StaticBalancer.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/protocol/RecordBatch.h>  // RecordBatchReader
#include <rune/protocol/MetadataRecords.h>

#include <cstdint>
#include <span>
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
    // MD1 Task 6: configure topic + node context for static assignment emit.
    // -----------------------------------------------------------------------

    /// Supply the topic + live-node context used by emitStaticAssignmentIfNeeded().
    ///
    /// @param topics      User topic specs (topicId >= 0; internal topics are
    ///                    filtered out automatically).
    /// @param liveNodes   Live broker NodeIds (sorted ascending internally by
    ///                    StaticBalancer::assign).
    ///
    /// Must be called on the duty-cycle thread before start().
    void configure(const std::vector<rune::metadata::TopicSpec>& topics,
                   const std::vector<rune::metadata::NodeId>&    liveNodes,
                   int32_t /*batchSize_unused*/ = 256)
    {
        // Filter: only user topics (topicId >= 0).
        configuredTopics_.clear();
        configuredTopics_.reserve(topics.size());
        for (const auto& ts : topics) {
            if (ts.topicId >= 0) {
                configuredTopics_.push_back(ts);
            }
        }
        liveNodes_ = liveNodes;
        assigned_  = false;
    }

    /// Emit static assignment records to the metadata log if needed.
    ///
    /// This is the leader-claim action.  It:
    ///   1. Checks the idempotency guard (assigned_ flag + MSM already-has-all).
    ///   2. Runs StaticBalancer::assign(configuredTopics_, liveNodes_).
    ///   3. Appends one record per replica_->append() call — each metadata record
    ///      occupies its own log slot (the ONLY correct design; see comment below).
    ///
    /// No-op if:
    ///   - This replica is not the Leader.
    ///   - configuredTopics_ is empty or liveNodes_ is empty.
    ///   - The MSM already has committed assignments for all user topics.
    ///   - assigned_ is true (latched after first successful emit).
    ///
    /// Public for test-only direct invocation; normally called by doWork().
    /// Single-writer: must be called on the duty-cycle thread.
    void emitStaticAssignmentIfNeeded() {
        if (!replica_) return;
        if (replica_->role() != rune::replication::Role::Leader) return;
        if (configuredTopics_.empty() || liveNodes_.empty()) return;

        // Idempotency guard: if already assigned (flag latched), skip.
        if (assigned_) return;

        // Secondary idempotency check: if MSM already has ALL user-topic
        // partition assignments committed, skip re-emission.
        if (msmAlreadyHasAllAssignments()) {
            assigned_ = true;
            return;
        }

        // Run StaticBalancer.
        std::vector<rune::metadata::BalancerRecord> records;
        try {
            records = rune::metadata::StaticBalancer::assign(configuredTopics_, liveNodes_);
        } catch (const rune::metadata::BalancerError& ex) {
            // Balancer validation failure (e.g. RF > nodes, co-partition mismatch).
            // Increment the error counter so callers / tests can detect silent failure.
            // Do NOT crash the duty-cycle — this must not happen in production when
            // config is validated at startup.
            (void)ex;
            ++balancerErrors_;
            return;
        }

        if (records.empty()) {
            assigned_ = true;
            return;
        }

        // Encode + append records to the metadata log.
        //
        // One-record-per-append invariant (required by the read path):
        //   Each replica_->append() stores its payload as the VALUE of exactly
        //   ONE log record (one RecordBatch entry on disk).  Concatenating N
        //   metadata records' bytes into a single payload would make all N look
        //   like ONE apply() call in doWork() — only the first record would be
        //   decoded; the rest would be silently ignored.  Therefore we MUST call
        //   append() exactly once per metadata record so each occupies its own
        //   log slot and advances logEndOffset() by exactly 1.
        //
        // Per-record encode buffer.
        //   Max size: prefix(3) + topicId(4) + partition(4) + leaderNode(4)
        //   + isr(2+n*4) + leaderEpoch(4) + partitionEpoch(4) + replicas(2+n*4).
        //   With RF≤1024 per side: ~8.2 KiB.  The 16 KiB stack buffer bounds
        //   per-record size for Phase-1 scale.  A record that encodes larger
        //   than kEncBufSize (very large ISR/replica array) returns n==0 and is
        //   skipped — flagged for a future guard that adds a hard startup check.
        static constexpr std::size_t kEncBufSize = 16 * 1024;
        uint8_t encBuf[kEncBufSize];

        for (const auto& br : records) {
            const std::size_t n = br.rec.encode(encBuf, kEncBufSize);
            if (n == 0) continue; // encode overflow (should not occur at Phase-1 scale)

            replica_->append(
                std::span<const uint8_t>(encBuf, n),
                /*acksAll=*/false);
        }

        assigned_ = true;
    }

    // -----------------------------------------------------------------------
    // Test-only / diagnostic: error counter for BalancerError catch path.
    // Non-zero indicates a misconfigured topic set (RF > nodes, co-partition
    // mismatch, etc.) — no assignment was emitted when this is non-zero.
    // -----------------------------------------------------------------------
    [[nodiscard]] uint64_t balancerErrorCount() const noexcept { return balancerErrors_; }

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
            // MD1 Task 6: on the leader-claim edge, emit static partition
            // assignment records if not already assigned (idempotent).
            // This is the only point at which the controller appends assignment
            // records; followers never emit.
            emitStaticAssignmentIfNeeded();
        } else if (!amLeader && wasLeader_) {
            wasLeader_ = false;
            assigned_  = false;  // re-arm emit if we regain leadership later
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
    // -----------------------------------------------------------------------
    // MSM-already-assigned check (idempotency §2)
    //
    // Returns true if the committed MSM already has an assignment for every
    // (topicId, partition) pair in configuredTopics_.  If it does, the leader
    // must not emit duplicates — the metadata log is already complete for this
    // topic set (e.g. after a restart where rebuildFromLog() replayed an
    // earlier assignment epoch).
    // -----------------------------------------------------------------------
    bool msmAlreadyHasAllAssignments() const noexcept {
        for (const auto& ts : configuredTopics_) {
            for (int32_t p = 0; p < ts.partitionCount; ++p) {
                if (msm_.assignment(ts.topicId, p) == nullptr) {
                    return false;
                }
            }
        }
        return !configuredTopics_.empty(); // empty → treat as "not assigned"
    }

    rune::replication::PartitionReplica* replica_;   ///< non-owning
    int32_t                               selfId_;

    rune::metadata::MetadataStateMachine  msm_;

    // Last log offset we have read batches from.  Initialized to -1 (nothing applied).
    rune::replication::Offset lastApplied_ = -1;

    // Follower→Leader transition gate (mirrors TCS / GCS patterns).
    bool wasLeader_ = false;

    // -----------------------------------------------------------------------
    // MD1 Task 6: static assignment emit state.
    // -----------------------------------------------------------------------

    /// User topic specs for static assignment (topicId >= 0 only).
    std::vector<rune::metadata::TopicSpec>  configuredTopics_;
    /// Live node IDs for the balancer.
    std::vector<rune::metadata::NodeId>     liveNodes_;
    /// Idempotency latch: true after the first successful emit.
    bool assigned_ = false;
    /// Error counter: incremented when BalancerError is caught.
    uint64_t balancerErrors_ = 0;
};

} // namespace rune::node
