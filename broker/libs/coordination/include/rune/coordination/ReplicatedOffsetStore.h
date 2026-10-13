#pragma once
// ---------------------------------------------------------------------------
// ReplicatedOffsetStore.h — Consumer committed-offset store whose commits
// are durable via the replicated `__consumer_offsets` partition Log.
//
// Design (CG-2):
//   Each offset commit = one single-record RecordBatch appended to the
//   replica Log with acksAll=true (majority-durable).  On coordinator
//   failover the new leader calls rebuildFromLog() to replay the replicated
//   Log and reconstruct the in-memory committed-offset map — zero data loss.
//
// Key/value encoding (compact little-endian, no padding):
//   key   = groupLen(uint16) + group(bytes) + topicLen(uint16) + topic(bytes)
//           + partition(int32)                                  [total: variable]
//   value = offset(int64) + tsNs(int64) + metaLen(uint16) + meta(bytes)
//           [ + lineage(uint64) — E2 final fix wave C1: only when non-zero (a
//             TRANSIENT partition's data lineage); older records lack it (0)
//             and older readers ignore the trailing bytes ]
//           [total: 18 + metaLen (+ 8) bytes]
//
// Allocation behavior:
//   commit(): CG-2's own encoding path is close-to-zero-alloc.  The key_bytes
//   and val_bytes are encoded into keyScratch_ and valScratch_ (member vectors
//   pre-reserved in the constructor and reused across calls).  The compact
//   payload is assembled into batchScratch_ (also a member vector reused across
//   calls) — no per-commit heap allocation on these three buffers after the
//   first call that causes them to reach their steady-state size.
//   However, PartitionReplica::append() internally calls makeBatch() which
//   returns a fresh std::vector<uint8_t> by value (one heap allocation per
//   commit, in the replication layer, beyond our control).  A future zero-alloc
//   pass would need a PartitionReplica::appendInto() variant (using the existing
//   RecordBatchBuilder::buildInto seam) to write the wrapper batch into a
//   caller-supplied buffer.  Deferred to the DR-9 M1 zero-copy card.
//   fetch() / fetchAll(): cold path; allocations are acceptable.
//   rebuildFromLog(): replay path (cold); allocations are acceptable.
//
// Single-writer: no locks; no hot-path allocation beyond the above.
// Namespace: rune::coordination
// C++23
// ---------------------------------------------------------------------------

#include <rune/coordination/OffsetStore.h>   // CommittedOffset, GroupId, TopicPartition, Offset, Types.h
#include <rune/replication/PartitionReplica.h>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace rune::coordination {

// ---------------------------------------------------------------------------
// ReplicatedOffsetStore
// ---------------------------------------------------------------------------
class ReplicatedOffsetStore {
public:
    /// Construct over an existing PartitionReplica (non-owning).
    /// The replica must outlive the store.
    explicit ReplicatedOffsetStore(rune::replication::PartitionReplica* replica);

    // -----------------------------------------------------------------------
    // Write path (hot — called on every OffsetCommit RPC).
    // -----------------------------------------------------------------------

    /// Commit/overwrite the offset for (group, tp).
    ///
    /// Builds a single-record RecordBatch and appends it to the replica Log
    /// with acksAll=true (majority-durable).  Updates the in-memory map ONLY
    /// if the append succeeds (returned base offset >= 0).
    ///
    /// \returns true  on success (appended + in-memory map updated).
    /// \returns false if the replica is minIsr-gated (append returned -1).
    ///
    /// Kafka semantics: a later commit with a LOWER offset is allowed and
    /// replaces the previous entry.
    /// `lineage` (C1): the transient partition's data lineage the offset is a
    /// position in (0 = none: a disk partition — the record is byte-identical
    /// to before).
    bool commit(const GroupId&        group,
                const TopicPartition& tp,
                Offset                offset,
                std::string_view      metadata,
                int64_t               tsNs,
                uint64_t              lineage = 0);

    // -----------------------------------------------------------------------
    // Read path (cold — O(1) or O(n) over the group's partitions).
    // -----------------------------------------------------------------------

    /// Fetch the committed offset for (group, tp).
    /// Returns nullopt if no commit has been recorded for this pair.
    [[nodiscard]]
    std::optional<CommittedOffset> fetch(const GroupId&        group,
                                         const TopicPartition& tp) const;

    /// Return all committed offsets for @p group (ordering unspecified).
    [[nodiscard]]
    std::vector<std::pair<TopicPartition, CommittedOffset>>
    fetchAll(const GroupId& group) const;

    // -----------------------------------------------------------------------
    // Rebuild (called after leader election, before serving any commits).
    // -----------------------------------------------------------------------

    /// Replay the replica Log [0, highWatermark()) and reconstruct the
    /// in-memory committed-offset map.  Last-write-wins per (group, tp) key,
    /// including a lower-offset overwrite.
    ///
    /// Typical use: new coordinator leader calls this once after becoming
    /// leader before accepting any new OffsetCommit requests.
    ///
    /// @returns true if the rebuild was EFFECTIVE (the replica's high-watermark
    ///          was > 0, so the log was actually scanned).  Returns false when
    ///          hw <= 0 (nothing to replay yet) — the caller should RETRY on a
    ///          later duty cycle (see CP-5.2 cold-restart fix in
    ///          GroupCoordinationService::doWork): on a cold cluster restart the
    ///          HW is briefly gated at 0 while the ISR re-expands, and a one-shot
    ///          rebuild here would silently recover no committed offsets.
    bool rebuildFromLog();

    /// The __consumer_offsets replica this store appends to / rebuilds from.
    [[nodiscard]] rune::replication::PartitionReplica* replica() const noexcept { return replica_; }

private:
    // -----------------------------------------------------------------------
    // Key/value encode helpers (write into caller-supplied vectors).
    // -----------------------------------------------------------------------

    /// Encode (group, tp) → keyScratch_.
    void encodeKey(const GroupId& group, const TopicPartition& tp);

    /// Encode (offset, tsNs, metadata) → valScratch_.
    void encodeVal(Offset offset, int64_t tsNs, std::string_view metadata, uint64_t lineage = 0);

    /// Apply one decoded record to the in-memory map (last-write-wins).
    void applyRecord(std::string_view key, std::string_view val);

    // -----------------------------------------------------------------------
    // In-memory index: group → { tp → CommittedOffset }
    // -----------------------------------------------------------------------
    using TpMap = std::unordered_map<TopicPartition, CommittedOffset>;
    std::unordered_map<std::string, TpMap> index_;

    rune::replication::PartitionReplica* replica_;  ///< non-owning

    // Per-store reusable scratch buffers (avoids per-commit heap allocation).
    std::vector<uint8_t> keyScratch_;   ///< encoded key_bytes section
    std::vector<uint8_t> valScratch_;   ///< encoded val_bytes section
    std::vector<uint8_t> batchScratch_; ///< compact payload (keyLen+key+valLen+val) passed to append()
};

} // namespace rune::coordination
