#pragma once
// ---------------------------------------------------------------------------
// TransactionLog.h — Durable __transaction_state log for the transaction
// coordinator (P5-3).
//
// Namespace: rune::transaction
// C++23. NO locks. Single-writer. Crash-safe (fsync on append, CRC per
// record, torn-tail recovery to last complete entry).
//
// ## Purpose (mirrors Kafka's __transaction_state internal topic)
//
// Every time the TransactionCoordinator transitions a txn (initProducerId,
// addPartitions, beginCommit, beginAbort, completeCommit, completeAbort), it
// appends a TxnLogEntry describing the new state.  On coordinator restart,
// TransactionLog::open() replays the log and rebuilds the exact in-memory
// metadata map — coordinator-failover foundation (P5-5).
//
// ## On-disk record layout (all fields little-endian)
//
//   [0..3]    uint32  magic         = kMagic (0x54584E4C = "TXNL")
//   [4..7]    uint32  totalLength   = kFixedHeaderSize + varFields + 4(CRC)
//   [8..15]   int64   txnStartNs    (0 when state is not Ongoing)
//   [16..23]  int64   producerId
//   [24..25]  int16   producerEpoch
//   [26..26]  uint8   state         (TxnState enum cast to uint8_t)
//   [27..27]  uint8   coordinatorEpoch (truncated to uint8 for log; full int32 in P5-3)
//   [28..31]  int32   coordinatorEpoch (full 32-bit)
//   [32..35]  int32   txnTimeoutMs
//   [36..37]  uint16  txnIdLen
//   [38..39]  uint16  partitionCount  (number of {topicLen:uint16, topic:bytes, partition:int32} tuples)
//   [40..41]  uint16  _pad = 0
//   [42..43]  uint16  _pad2 = 0      (align to kFixedHeaderSize = 44)
//   [44..]    bytes   txnId (txnIdLen bytes)
//   [..]      tuples  { topicLen:uint16, topic:topicLen bytes, partition:int32 } × partitionCount
//   [..]      uint32  CRC32C of all bytes before this field
//
// kFixedHeaderSize = 44
//
// ## Recovery
// On open: scan records; stop on torn header / magic mismatch / CRC mismatch;
// ftruncate to last good position; replay all good records to build txnMap.
// ---------------------------------------------------------------------------

#include <rune/transaction/Types.h>
#include <rune/transaction/TransactionCoordinator.h>

#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace rune::transaction {

// ---------------------------------------------------------------------------
// TxnLogEntry — one persisted transition
// ---------------------------------------------------------------------------
struct TxnLogEntry {
    TransactionalId        txnId;
    ProducerId             producerId{-1};
    ProducerEpoch          producerEpoch{0};
    TxnState               state{TxnState::Empty};
    std::set<TopicPartition> partitions;   ///< snapshot of partitions at this transition
    int64_t                txnStartNs{0};
    int32_t                coordinatorEpoch{0};
    int32_t                txnTimeoutMs{60'000};
};

// ---------------------------------------------------------------------------
// TransactionLog
// ---------------------------------------------------------------------------
class TransactionLog {
public:
    // -----------------------------------------------------------------------
    // Open/create a log at @p path; replay any existing records.
    // On torn tail: ftruncate to last complete record (no crash, writable).
    // -----------------------------------------------------------------------
    explicit TransactionLog(std::string path);
    ~TransactionLog();

    TransactionLog(const TransactionLog&)            = delete;
    TransactionLog& operator=(const TransactionLog&) = delete;

    // -----------------------------------------------------------------------
    // append — persist a transaction state transition.
    // Encodes the entry, writes, and fsyncs to disk.
    // Also updates the in-memory txnMap.
    // -----------------------------------------------------------------------
    void append(const TxnLogEntry& entry);

    // -----------------------------------------------------------------------
    // Convenience: build and append a TxnLogEntry from TransactionMetadata.
    // -----------------------------------------------------------------------
    void appendFromMetadata(const TransactionMetadata& meta, int32_t coordinatorEpoch = 0);

    // -----------------------------------------------------------------------
    // Accessors (rebuilt from replay)
    // -----------------------------------------------------------------------

    /// Fetch the last-seen metadata for a transactionalId.
    /// Returns nullptr if not found.
    [[nodiscard]] const TransactionMetadata* metadata(const TransactionalId& txnId) const noexcept;

    /// All known transactionalIds (replay result).
    [[nodiscard]] const std::unordered_map<TransactionalId, TransactionMetadata>& all() const noexcept {
        return txnMap_;
    }

    /// Number of known transactionalIds.
    [[nodiscard]] std::size_t size() const noexcept { return txnMap_.size(); }

    // -----------------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------------
    static constexpr uint32_t kMagic           = 0x544B4E4Cu; // "TKNL" little-endian
    static constexpr uint32_t kFixedHeaderSize  = 44u;
    static constexpr uint32_t kChecksumSize     = 4u;

private:
    // -----------------------------------------------------------------------
    // Disk I/O
    // -----------------------------------------------------------------------
    void replayAndTruncate();
    void appendRecord(const TxnLogEntry& entry);
    void applyEntry(const TxnLogEntry& entry);

    // -----------------------------------------------------------------------
    // CRC32C
    // -----------------------------------------------------------------------
    static uint32_t crc32c(const uint8_t* data, std::size_t len, uint32_t crc = 0xFFFFFFFFu) noexcept;

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------
    std::string path_;
    int         fd_{-1};
    int64_t     writePos_{0};

    /// Rebuilt map from replay; mirrors coordinator's txnMap_.
    std::unordered_map<TransactionalId, TransactionMetadata> txnMap_;
};

} // namespace rune::transaction
