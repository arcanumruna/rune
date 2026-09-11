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
//   [0..3]    uint32  magic         = kMagic (0x544B4E4C = "TKNL" LE)
//   [4..7]    uint32  totalLength   = kFixedHeaderSize + varFields + 4(CRC)
//   [8..15]   int64   txnStartNs    (0 when state is not Ongoing)
//   [16..23]  int64   producerId
//   [24..25]  int16   producerEpoch
//   [26..26]  uint8   state         (TxnState enum cast to uint8_t)
//   [27..27]  uint8   _pad = 0
//   [28..31]  int32   coordinatorEpoch (full 32-bit)
//   [32..35]  int32   txnTimeoutMs
//   [36..37]  uint16  txnIdLen
//   [38..39]  uint16  partitionCount  (number of {topicLen:uint16, topic:bytes, partition:int32} tuples)
//   [40..47]  int64   pendingNewProducerId    (A2: deferred overflow PID swap; -1 = none)
//   [48..49]  int16   pendingNewProducerEpoch (A2: deferred overflow epoch;   -1 = none)
//   [50..51]  uint16  _pad = 0        (align to kFixedHeaderSize = 52)
//   [52..]    bytes   txnId (txnIdLen bytes)
//   [..]      tuples  { topicLen:uint16, topic:topicLen bytes, partition:int32 } × partitionCount
//   [..]      uint32  CRC32C of all bytes before this field
//
// kFixedHeaderSize = 52
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
    /// A2: deferred overflow PID swap (NEW-1). Persisted so a crash mid-abort does
    /// not lose the fresh identity the client was already handed. -1 == none.
    ProducerId             pendingNewProducerId{-1};
    ProducerEpoch          pendingNewProducerEpoch{-1};
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
    static constexpr uint32_t kFixedHeaderSize  = 52u;
    static constexpr uint32_t kChecksumSize     = 4u;

    /// A5: hard cap on length-prefixed fields (uint16 wire width). Inputs that
    /// exceed this are REJECTED (never silently truncated).
    static constexpr std::size_t kMaxUint16 = 0xFFFFu;

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
