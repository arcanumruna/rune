// MetadataStateMachine.h — deterministic fold over metadata-log records.
//
// Produces the partition→assignment map for the cluster's control plane.
// Records are applied via apply(buf, len, recordPos) and become visible via
// assignment() only when recordPos <= committedPos (set by commit()).
//
// Design:
//   - All state lives in an AssignmentMap (open-addressing, no per-op alloc).
//   - apply() decodes the record, stores it in a pending buffer, and moves
//     pending records into the committed map when commit() is called.
//   - Deterministic: applying the same stream from empty twice yields the
//     identical map.
//   - snapshot()/restore(): full-image I/O; restore() resets then replays,
//     pruning any assignment absent from the snapshot.

#pragma once

#include "rune/metadata/AssignmentMap.h"
#include "rune/metadata/PartitionAssignment.h"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace rune::metadata {

// ─────────────────────────────────────────────────────────────────────────────
// PartitionKey — (topicId, partition) pair, used in delta lists.
// ─────────────────────────────────────────────────────────────────────────────

struct PartitionKey {
    int32_t topicId;
    int32_t partition;
};

// ─────────────────────────────────────────────────────────────────────────────
// LocalRoleDelta — what changed from selfNode's perspective vs a previous map.
//
// Classification (for partition P, comparing prev vs current):
//   delete        : self was a replica in prev, now absent in current
//   electedLeader : self is leader in current AND leaderEpoch > prev.leaderEpoch
//                   (includes brand-new partitions where prev had no entry)
//   leaderUpdate  : self is leader in current AND leaderEpoch == prev.leaderEpoch
//                   but partitionEpoch > prev.partitionEpoch
//   follower      : self is NOT the leader in current, is a replica, AND
//                   partitionEpoch > prev.partitionEpoch
// ─────────────────────────────────────────────────────────────────────────────

struct LocalRoleDelta {
    std::vector<PartitionKey> deletes;
    std::vector<PartitionKey> electedLeaders;
    std::vector<PartitionKey> leaderUpdates;
    std::vector<PartitionKey> followers;
};

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot I/O interfaces — simple length-prefixed binary format.
//
// SnapshotWriter must provide:   void writeInt32(int32_t); void writeInt64(int64_t);
// SnapshotReader must provide:   int32_t readInt32();       int64_t readInt64();
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// MetadataStateMachine
// ─────────────────────────────────────────────────────────────────────────────

class MetadataStateMachine {
public:
    MetadataStateMachine() = default;

    // ── Apply a raw log record buffer.  ───────────────────────────────────────
    // recordPos is the log position of this record.
    // The record is decoded and stored; it becomes visible via assignment()
    // only after commit(recordPos) is called.
    void apply(const uint8_t* buf, std::size_t len, int64_t recordPos);

    // ── Advance the committed position. ──────────────────────────────────────
    // All pending records with pos <= committedPos are moved into the committed map.
    void commit(int64_t committedPos);

    // ── Query committed state. ────────────────────────────────────────────────
    // Returns nullptr if the partition is unknown or not yet committed.
    const PartitionAssignment* assignment(int32_t topicId, int32_t partition) const noexcept;

    // ── Derive per-node role delta vs a previous assignment map. ─────────────
    LocalRoleDelta deriveLocalDelta(int32_t selfNode, const AssignmentMap& prev) const;

    // ── Snapshot / restore. ───────────────────────────────────────────────────
    // snapshot(w): write full image to writer w.
    // restore(r):  reset state, then read image from reader r (prunes missing).
    template<typename Writer>
    void snapshot(Writer& w) const;

    template<typename Reader>
    void restore(Reader& r);

    // ── Expose the committed map (read-only) for delta derivation. ────────────
    const AssignmentMap& committedMap() const noexcept { return committed_; }

    // ── Bumped on every committed-map mutation (install / restore).  Lets a
    //    consumer rebuild derived state (the RoutingImage) only on change. ────
    [[nodiscard]] uint64_t committedVersion() const noexcept { return committedVersion_; }

    // ── MVCC revert: discard pending records with pos > toCommittedPos. ───────
    // Used when a commit is lost (e.g. leader failover).  The committed map is
    // untouched — only uncommitted speculation in pending_ is pruned.
    void revert(int64_t toCommittedPos);

    // ── handleLoss counter: records skipped due to unknown recordVersion. ─────
    // Incremented in apply() when a record's recordVersion is unsupported.
    // The record is NOT applied and does NOT crash apply().
    [[nodiscard]] uint64_t lossCount() const noexcept { return lossCount_; }

    // ── Epoch-regression guard (KRaft invariant; epoch-restart fix round 1):
    //    an assignment record never moves a partition BACK — it is ignored
    //    (counted here) when its leaderEpoch is lower than the installed one,
    //    equal with a different leader (one epoch, one leader), or its
    //    partitionEpoch is lower.  Every controller decision bumps; only a
    //    re-emitted stale decision (e.g. a static epoch-0 assignment) regresses.
    [[nodiscard]] uint64_t regressionsRejected() const noexcept { return regressionsRejected_; }

    // ── D15: the committed metadata feature level (KRaft metadata.version;
    //    FeatureLevelRecord).  Monotonic: a lower record is ignored.  0 = no
    //    record yet (a log written by pre-D15 binaries). ─────────────────────
    [[nodiscard]] int16_t featureLevel() const noexcept { return featureLevel_; }

private:
    // Committed assignments (pos <= committedPos)
    AssignmentMap committed_;

    // Pending records: encoded as a compact struct so we don't need to
    // re-decode on commit.
    struct PendingEntry {
        int64_t          pos;
        int32_t          topicId;
        int32_t          partition;
        PartitionAssignment assignment;
    };

    std::vector<PendingEntry> pending_;
    /// D15: uncommitted FeatureLevelRecords (pos, level).
    std::vector<std::pair<int64_t, int16_t>> pendingFeature_;
    int16_t featureLevel_ = 0;

    int64_t committedPos_ = -1;

    // See committedVersion().
    uint64_t committedVersion_ = 0;

    // Count of records skipped due to unknown recordVersion (handleLoss).
    uint64_t lossCount_ = 0;
    uint64_t regressionsRejected_ = 0;

    void flushPending(int64_t upToPos);
    void install_(int32_t topicId, int32_t partition, const PartitionAssignment& pa);
};

// ─────────────────────────────────────────────────────────────────────────────
// Template implementations (snapshot / restore)
// ─────────────────────────────────────────────────────────────────────────────

// Binary snapshot format (all LE int32/int64):
//   int64  committedPos
//   int32  count         — number of (topicId, partition, assignment) entries
//   per entry:
//     int32 topicId
//     int32 partition
//     int32 leaderNode
//     int32 leaderEpoch
//     int32 partitionEpoch
//     int32 isrCount
//     int32 isr[0..isrCount-1]
//     int32 replicaCount
//     int32 replicas[0..replicaCount-1]
//
// D15b m4 (documented gap, no production caller today): this image does NOT
// carry the D15 replicated state — the committed featureLevel() (a
// FeatureLevelRecord, type 0x8001) and each leaderless assignment's
// LeaderlessMark (the 0xD1 trailer).  A restore from it would read level 0
// and no marks, i.e. a successor would lose the leaderless reasons exactly as
// after a pre-D15 log (see restoreLeaderlessMarks_).  Before any caller
// (metadata snapshotting / log compaction) lands, bump the image to carry:
//   int16 featureLevel                     — after committedPos
//   per entry: uint8 markKind, int32 markNode, int64 markIncarnation
// with a format version so an older image restores as level 0 / no marks.

template<typename Writer>
void MetadataStateMachine::snapshot(Writer& w) const
{
    w.writeInt64(committedPos_);

    // Count committed entries
    int32_t count = committed_.size();
    w.writeInt32(count);

    committed_.forEach([&](int32_t topicId, int32_t partition, const PartitionAssignment& pa) {
        w.writeInt32(topicId);
        w.writeInt32(partition);
        w.writeInt32(pa.leaderNode);
        w.writeInt32(pa.leaderEpoch);
        w.writeInt32(pa.partitionEpoch);
        w.writeInt32(static_cast<int32_t>(pa.isr.size()));
        for (int32_t n : pa.isr)      w.writeInt32(n);
        w.writeInt32(static_cast<int32_t>(pa.replicas.size()));
        for (int32_t n : pa.replicas) w.writeInt32(n);
    });
}

template<typename Reader>
void MetadataStateMachine::restore(Reader& r)
{
    // Full reset — pending discarded, committed cleared
    committed_.clear();
    pending_.clear();
    ++committedVersion_;

    committedPos_ = r.readInt64();
    const int32_t count = r.readInt32();

    for (int32_t i = 0; i < count; ++i) {
        int32_t topicId        = r.readInt32();
        int32_t partition      = r.readInt32();
        PartitionAssignment pa;
        pa.leaderNode          = r.readInt32();
        pa.leaderEpoch         = r.readInt32();
        pa.partitionEpoch      = r.readInt32();
        int32_t isrCount       = r.readInt32();
        pa.isr.resize(static_cast<std::size_t>(isrCount));
        for (int32_t j = 0; j < isrCount; ++j) pa.isr[j] = r.readInt32();
        int32_t repCount       = r.readInt32();
        pa.replicas.resize(static_cast<std::size_t>(repCount));
        for (int32_t j = 0; j < repCount; ++j) pa.replicas[j] = r.readInt32();
        committed_.put(topicId, partition, pa);
    }
    // Snapshot is a full image — any entry not in snapshot is absent (pruned by reset+reload).
}

} // namespace rune::metadata
