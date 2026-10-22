#pragma once
// ---------------------------------------------------------------------------
// ReplicatedTxnStateStore.h — Transaction state store whose commits are
// majority-durable via the replicated `__transaction_state` partition Log.
//
// Design (TX-3):
//   Each state transition = one single-record RecordBatch appended to the
//   replica Log with acksAll=true (majority-durable — differs from
//   ReplicatedOffsetStore which uses minIsr=1).  On coordinator failover the
//   new leader calls rebuildFromLog() to replay the replicated Log and
//   reconstruct the in-memory transaction metadata map.
//
// Record body encoding:
//   Reuses the exact TransactionLog on-disk record layout (magic 0x544B4E4C,
//   kFixedHeaderSize=52, CRC32C tail) so one decoder serves both the
//   disk-based TransactionLog and this replicated store.
//
// Key correctness properties:
//   - put() uses replica->append(acksAll=TRUE) — majority durable.
//   - In-memory map updated ONLY after append succeeds (base offset >= 0).
//   - rebuildFromLog() reads [0, HW) across ALL segments (the CG-2 multi-
//     segment durability fix — do NOT read only the first segment).
//   - Last-write-wins per txnId (fold semantics).
//
// Circular-dependency note:
//   rune_transaction already links rune_coordination (for
//   coordination::TopicPartition and OffsetStore used by addOffsetsToTxn).
//   ReplicatedTxnStateStore depends on TransactionMetadata (this lib) and
//   PartitionReplica (rune_replication, transitively via rune_coordination).
//   Therefore ReplicatedTxnStateStore lives in rune_transaction, NOT in
//   rune_coordination (which would create a cycle).
//   This is an intentional deviation from the brief's
//   "libs/coordination/..." location to prevent a circular dependency.
//
// Allocation behavior:
//   put(): encodes into recordScratch_ (member vector, pre-reserved);
//   PartitionReplica::append() internally allocates once in makeBatch().
//   rebuildFromLog(): cold path; allocations acceptable.
//   get(): O(1) map lookup; no allocation.
//
// Single-writer: no locks; no hot-path allocation beyond the above.
// Namespace: rune::transaction
// C++23
// ---------------------------------------------------------------------------

#include <rune/transaction/TransactionCoordinator.h>  // TransactionMetadata, TransactionalId
#include <rune/transaction/Types.h>                   // TxnState, ProducerId, ProducerEpoch
#include <rune/replication/PartitionReplica.h>

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace rune::transaction {

// ---------------------------------------------------------------------------
// ReplicatedTxnStateStore
// ---------------------------------------------------------------------------
class ReplicatedTxnStateStore {
public:
    /// Construct over an existing PartitionReplica (non-owning).
    /// The replica must outlive the store.
    explicit ReplicatedTxnStateStore(rune::replication::PartitionReplica* replica);

    // -----------------------------------------------------------------------
    // Write path — called on every coordinator state transition.
    // -----------------------------------------------------------------------

    /// Append a txn-state record via replica.append(acksAll=true).
    ///
    /// Encodes the metadata using the TransactionLog on-disk layout and
    /// appends to the replica Log as a single-record RecordBatch with
    /// acksAll=true (majority-durable).  Updates the in-memory map ONLY
    /// if the append succeeds (returned base offset >= 0).
    ///
    /// \returns true  on success (appended + in-memory map updated).
    /// \returns false if the replica is minIsr-gated (append returned -1).
    bool put(const TransactionMetadata& meta, int32_t coordinatorEpoch) { return putAt(meta, coordinatorEpoch) >= 0; }
    /// #358 class: put(), returning the record's log offset (-1: refused).  The record is durable - it survives a
    /// failover of the coordinator - once the replica's high watermark has passed it; an acks-all append
    /// replicates asynchronously, so an answer that depends on the record waits for that.
    rune::replication::Offset putAt(const TransactionMetadata& meta, int32_t coordinatorEpoch);

    // -----------------------------------------------------------------------
    // Read path — O(1) map lookup.
    // -----------------------------------------------------------------------

    /// Return the last-seen metadata for a transactionalId.
    /// Returns nullptr if not found.
    [[nodiscard]]
    const TransactionMetadata* get(const TransactionalId& txnId) const noexcept;

    // -----------------------------------------------------------------------
    // Rebuild — called after leader election, before serving any requests.
    // -----------------------------------------------------------------------

    /// Replay the replica Log [0, highWatermark()) and reconstruct the
    /// in-memory metadata map.  Last-write-wins per txnId.
    /// Clears existing in-memory state before replay.
    ///
    /// Iterates ALL segments by looping with advancing fetchOffset (avoids
    /// the CG-2 bug of reading only the first segment).
    void rebuildFromLog();

    // -----------------------------------------------------------------------
    // F15 §5.1 / §5.6 — coordinator load + persisted pid block ceiling.
    // -----------------------------------------------------------------------

    /// The __transaction_state replica this store writes (control-owned).
    [[nodiscard]] rune::replication::PartitionReplica* replica() const noexcept { return replica_; }

    /// The folded index (txnId → last metadata) — the coordinator load snapshot.
    [[nodiscard]] const std::map<TransactionalId, TransactionMetadata, std::less<>>& all() const noexcept {
        return index_;
    }

    /// F15 I7 (Kafka ProducerIdManager): append a pid BLOCK CEILING record —
    /// every producerId below @p ceiling may have been handed out.  The record
    /// is recognised by its own record-type tag (kPidBlockMagic in the value
    /// header) and written under the RESERVED compaction key kPidBlockKey, so
    /// the LogCleaner keeps only the latest ceiling (= the max: ceilings are
    /// monotone) and the load scan stays bounded.  kPidBlockKey can never be a
    /// transactional id: its leading 0xFF byte is never valid UTF-8 (Kafka
    /// STRING) and InitProducerId rejects it explicitly
    /// (isReservedKey) — metadata is only ever created there.
    /// \returns the record's offset (commit it by HW > offset), or -1 if the
    ///          replica is min-ISR-gated.
    rune::replication::Offset putPidBlockCeiling(ProducerId ceiling);

    /// Highest pid block ceiling folded by rebuildFromLog() (-1 if none).
    [[nodiscard]] ProducerId persistedPidCeiling() const noexcept { return persistedPidCeiling_; }

    /// Value-header tag of a pid block ceiling record ("PIDB", little-endian).
    static constexpr uint32_t kPidBlockMagic = 0x42444950u;

    /// Reserved compaction key of the pid block ceiling record.
    static constexpr std::string_view kPidBlockKey{"\xFF__rune_pid_block", 17};

    /// True for a key reserved by this store (never a valid transactional id).
    [[nodiscard]] static bool isReservedKey(std::string_view txnId) noexcept { return txnId == kPidBlockKey; }

private:
    // -----------------------------------------------------------------------
    // Encode helpers
    // -----------------------------------------------------------------------

    /// Encode a full record for @p meta into recordScratch_.
    void encodeRecord(const TransactionMetadata& meta, int32_t coordinatorEpoch);

    /// Apply one decoded record payload (the value of a wrapper RecordBatch
    /// record) to the in-memory map — last-write-wins per txnId.
    void applyRecord(std::string_view payload);

    // -----------------------------------------------------------------------
    // CRC32C (software Castagnoli — same as TransactionLog's crc32c)
    // -----------------------------------------------------------------------
    static uint32_t crc32c(const uint8_t* data, std::size_t len,
                            uint32_t init = 0xFFFFFFFFu) noexcept;

    // -----------------------------------------------------------------------
    // In-memory index: txnId → TransactionMetadata.
    // std::map with std::less<> (transparent comparator) so hot-path get()
    // can be called with string_view without constructing a std::string.
    // -----------------------------------------------------------------------
    std::map<TransactionalId, TransactionMetadata, std::less<>> index_;

    rune::replication::PartitionReplica* replica_;  ///< non-owning

    /// F15 I7: highest pid block ceiling seen by rebuildFromLog() (-1 = none).
    ProducerId persistedPidCeiling_ = -1;

    // Per-store reusable scratch buffer — avoids per-put heap allocation.
    std::vector<uint8_t> recordScratch_;
};

} // namespace rune::transaction
