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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <unordered_map>
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
    MetadataStateMachine() { nodeStorage_.reserve(128); pendingStorage_.reserve(16); }

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

    // ── E2 final fix wave (C1 multi-node): the committed data lineage of a
    //    TRANSIENT partition (TransientLineageRecord), nullptr when none was
    //    committed.  A record that would move confirmedEpoch BACK is ignored
    //    (a re-emitted stale decision). ─────────────────────────────────────
    [[nodiscard]] const TransientLineage* lineage(int32_t topicId, int32_t partition) const noexcept {
        const auto it = lineages_.find(lineageKey_(topicId, partition));
        return it == lineages_.end() ? nullptr : &it->second;
    }
    [[nodiscard]] uint64_t lineageVersion() const noexcept { return lineageVersion_; }

    // ── ISR v3 (level >= 2): the committed storage id of a node (NodeStorageRecord), 0 = none.  Installed at
    //    COMMIT like every record, and rebuilt by replay (the MSM is rebuilt from the log, never snapshotted
    //    with it).  A ControllerTermRecord is decoded and ignored. ─────────────────────────────────────────
    [[nodiscard]] int64_t nodeStorage(int32_t node) const noexcept {
        for (const auto& e : nodeStorage_) if (e.first == node) return e.second;
        return 0;
    }

private:
    /// ISR v3: committed node → storage id (a handful of nodes: a flat vector, capacity reserved once).
    std::vector<std::pair<int32_t, int64_t>> nodeStorage_;
    struct PendingStorage { int64_t pos; int32_t node; int64_t storageId; };
    std::vector<PendingStorage> pendingStorage_;
    void installStorage_(int32_t node, int64_t storageId);
    // Committed assignments (pos <= committedPos)
    AssignmentMap committed_;

    // Pending records: encoded as a compact struct so we don't need to
    // re-decode on commit.
    // Catalog history Task 8: a pending entry is an assignment to install OR a
    // whole-topic placement removal (RemoveTopicPlacementRecord) — one kind
    // field, applied in log-position order with the assignments.
    enum class PendingKind : uint8_t { Assign, RemoveTopic };
    struct PendingEntry {
        int64_t          pos;
        int32_t          topicId;
        int32_t          partition;
        PartitionAssignment assignment;
        PendingKind      kind = PendingKind::Assign;
    };

    std::vector<PendingEntry> pending_;
    /// D15: uncommitted FeatureLevelRecords (pos, level).
    std::vector<std::pair<int64_t, int16_t>> pendingFeature_;
    int16_t featureLevel_ = 0;
    /// C1 multi-node: committed lineages + uncommitted records (pos, key, lineage).
    static int64_t lineageKey_(int32_t t, int32_t p) noexcept {
        return (static_cast<int64_t>(t) << 32) | static_cast<int64_t>(static_cast<uint32_t>(p));
    }
    std::unordered_map<int64_t, TransientLineage> lineages_;
    struct PendingLineage { int64_t pos; int64_t key; TransientLineage l; };
    std::vector<PendingLineage> pendingLineage_;
    uint64_t lineageVersion_ = 0;
    void installLineage_(int64_t key, const TransientLineage& l);

    int64_t committedPos_ = -1;

    // See committedVersion().
    uint64_t committedVersion_ = 0;

    // Count of records skipped due to unknown recordVersion (handleLoss).
    uint64_t lossCount_ = 0;
    uint64_t regressionsRejected_ = 0;

    void flushPending(int64_t upToPos);
    void install_(int32_t topicId, int32_t partition, const PartitionAssignment& pa);
    /// Task 8: erase every committed (topicId, *) assignment + lineage.
    void removeTopic_(int32_t topicId);
};

// ─────────────────────────────────────────────────────────────────────────────
// Template implementations (snapshot / restore)
// ─────────────────────────────────────────────────────────────────────────────

// Binary snapshot format (all LE int32/int64).
//
// Image v0 (legacy, still restored):
//   int64  committedPos   (>= -1)
//   int32  count
//   per entry: int32 topicId, partition, leaderNode, leaderEpoch,
//              partitionEpoch, isrCount, isr[..], replicaCount, replicas[..]
//
// Image v1 (written; E2 residual N4 — also closes the D15b m4 gap):
//   int64  kImageV1Tag    (negative: a v0 image's first int64 is a
//                          committedPos >= -1, so the tag is unambiguous)
//   int64  committedPos
//   int32  featureLevel   (D15 FeatureLevelRecord)
//   int32  count
//   per entry: the v0 fields, then
//              int32 cleanElection (N3), int32 markKind, int32 markNode,
//              int64 markIncarnation (D15 LeaderlessMark)
//   int32  lineageCount   (C1 TransientLineage records, sorted by key)
//   per lineage: int32 topicId, int32 partition, int64 lineage,
//                int32 mintEpoch, int32 confirmedEpoch, int32 confirmedLeader,
//                int64 incarnation
//
// A v0 image restores as feature level 0, no marks, clean elections and NO
// lineages (a transient group offset then answers "pending"/"none" until a
// leadership re-confirms — never a stale offset).

inline constexpr int64_t kMsmImageV1Tag = -0x52554E45'00000001LL;   // "RUNE" v1

template<typename Writer>
void MetadataStateMachine::snapshot(Writer& w) const
{
    w.writeInt64(kMsmImageV1Tag);
    w.writeInt64(committedPos_);
    w.writeInt32(featureLevel_);

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
        w.writeInt32(pa.cleanElection ? 1 : 0);
        w.writeInt32(static_cast<int32_t>(pa.mark.kind));
        w.writeInt32(pa.mark.node);
        w.writeInt64(pa.mark.incarnation);
    });

    // Sorted: the image bytes are a function of the state, not of hash order.
    std::vector<int64_t> keys;
    keys.reserve(lineages_.size());
    for (const auto& [k, l] : lineages_) keys.push_back(k);
    std::sort(keys.begin(), keys.end());
    w.writeInt32(static_cast<int32_t>(keys.size()));
    for (const int64_t k : keys) {
        const TransientLineage& l = lineages_.at(k);
        w.writeInt32(static_cast<int32_t>(k >> 32));
        w.writeInt32(static_cast<int32_t>(static_cast<uint32_t>(k)));
        w.writeInt64(static_cast<int64_t>(l.lineage));
        w.writeInt32(l.mintEpoch);
        w.writeInt32(l.confirmedEpoch);
        w.writeInt32(l.confirmedLeader);
        w.writeInt64(l.incarnation);
    }
}

template<typename Reader>
void MetadataStateMachine::restore(Reader& r)
{
    // Full reset — pending discarded, committed cleared
    committed_.clear();
    pending_.clear();
    pendingFeature_.clear();
    lineages_.clear();
    pendingLineage_.clear();
    nodeStorage_.clear();     // ISR v3: an image carries no storage ids (like a v0 image's lineages)
    pendingStorage_.clear();
    featureLevel_ = 0;
    ++lineageVersion_;
    ++committedVersion_;

    const int64_t first = r.readInt64();
    const bool v1 = first == kMsmImageV1Tag;
    committedPos_ = v1 ? r.readInt64() : first;
    if (v1) featureLevel_ = static_cast<int16_t>(r.readInt32());
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
        if (v1) {
            pa.cleanElection    = r.readInt32() != 0;
            {   // Review M6b (round 5): only the known kinds — an image from another
                // (dev) binary with an unknown kind restores as no mark.
                const int32_t kind = r.readInt32();
                pa.mark.kind = (kind == static_cast<int32_t>(LeaderlessMark::Kind::EmptyMemoryLeader) ||
                                kind == static_cast<int32_t>(LeaderlessMark::Kind::LogDirOffline) ||
                                kind == static_cast<int32_t>(LeaderlessMark::Kind::StorageReset))
                                   ? static_cast<LeaderlessMark::Kind>(kind)
                                   : LeaderlessMark::Kind::None;
            }
            pa.mark.node        = r.readInt32();
            pa.mark.incarnation = r.readInt64();
        }
        committed_.put(topicId, partition, pa);
    }
    if (v1) {
        const int32_t n = r.readInt32();
        for (int32_t i = 0; i < n; ++i) {
            const int32_t t = r.readInt32();
            const int32_t p = r.readInt32();
            TransientLineage l;
            l.lineage         = static_cast<uint64_t>(r.readInt64());
            l.mintEpoch       = r.readInt32();
            l.confirmedEpoch  = r.readInt32();
            l.confirmedLeader = r.readInt32();
            l.incarnation     = r.readInt64();
            lineages_[lineageKey_(t, p)] = l;
        }
    }
    // Snapshot is a full image — any entry not in snapshot is absent (pruned by reset+reload).
}

} // namespace rune::metadata
