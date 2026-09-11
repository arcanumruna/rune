#pragma once
// ---------------------------------------------------------------------------
// OffsetStore.h — Durable consumer committed-offset store.
//
// Semantics: Kafka OffsetCommit — each commit OVERWRITES the stored value
// for (group, topic, partition). A later commit may carry any offset value
// (the caller enforces monotonicity policy if desired; the store does not).
//
// Durability: every commit is appended to a flat log file. On construction
// the log is replayed to rebuild the in-memory map — identical state after
// a crash/reopen. A torn/partial tail record is detected (magic+length
// mismatch) and truncated before the store is opened for writing, recovering
// to the last complete record.
//
// Single-writer: no locks; no hot-path allocation beyond the file append.
// Namespace: rune::coordination
// C++23
// ---------------------------------------------------------------------------

#include <rune/coordination/Types.h>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace rune::coordination {

// ---------------------------------------------------------------------------
// CommittedOffset — what the store remembers for a (group, tp) pair.
// ---------------------------------------------------------------------------
struct CommittedOffset {
    Offset      offset{0};
    std::string metadata;
    int64_t     commitTimestampNs{0};
};

// ---------------------------------------------------------------------------
// OffsetStore
// ---------------------------------------------------------------------------
class OffsetStore {
public:
    /// Open or create the offset log at @p path.
    ///
    /// If the file exists its contents are replayed to rebuild the in-memory
    /// map. A torn tail is detected (magic mismatch or truncated header/body)
    /// and truncated so the store is always writable after construction.
    explicit OffsetStore(std::string path);
    ~OffsetStore();

    OffsetStore(const OffsetStore&)            = delete;
    OffsetStore& operator=(const OffsetStore&) = delete;

    /// Record/overwrite the committed offset for (group, tp).
    /// The record is appended to the log and fsync'd before returning.
    /// Kafka semantics: a later commit with a LOWER offset is allowed and
    /// replaces the previous value.
    void commit(const GroupId&     group,
                const TopicPartition& tp,
                Offset             offset,
                std::string_view   metadata,
                int64_t            commitTimestampNs);

    /// Fetch the committed offset for (group, tp).
    /// Returns nullopt if no commit has been recorded for this pair.
    [[nodiscard]]
    std::optional<CommittedOffset> fetch(const GroupId&        group,
                                         const TopicPartition& tp) const;

    /// Return all committed offsets for @p group.
    /// Ordering is unspecified.
    [[nodiscard]]
    std::vector<std::pair<TopicPartition, CommittedOffset>>
    fetchAll(const GroupId& group) const;

private:
    // -----------------------------------------------------------------------
    // On-disk record layout (little-endian, no padding between fields)
    // -----------------------------------------------------------------------
    //
    // Per-record:
    //   magic          : uint32   = kMagic  (integrity guard)
    //   totalLength    : uint32   = sizeof(FixedHeader) + varLen
    //   commitTs       : int64
    //   offset         : int64
    //   partition      : int32
    //   groupLen       : uint16
    //   topicLen       : uint16
    //   metadataLen    : uint16
    //   [padding]      : uint8    = 0  (aligns body to kHeaderSize)
    //   body           : groupLen bytes (group id, no nul)
    //                  + topicLen bytes (topic, no nul)
    //                  + metadataLen bytes (metadata, no nul)
    //   checksum       : uint32   = CRC32C of everything before this field
    //
    // kHeaderSize is the fixed prefix; body + checksum follow.
    // -----------------------------------------------------------------------

    static constexpr uint32_t kMagic      = 0x4F464653u; // "OFFS"
    static constexpr uint32_t kVersion    = 1u;

    // Fixed header size (magic[4] + totalLength[4] + commitTs[8] + offset[8]
    //                    + partition[4] + groupLen[2] + topicLen[2]
    //                    + metadataLen[2] + pad[2] = 36 bytes)
    static constexpr uint32_t kFixedHeaderSize = 36u;

    // Append a fully encoded record to the log file and fsync.
    void appendRecord(const GroupId&        group,
                      const TopicPartition& tp,
                      Offset                offset,
                      std::string_view      metadata,
                      int64_t               commitTimestampNs);

    // Replay the log file (called during construction).
    // Returns the file offset of the last complete record's end (= new write cursor).
    // Truncates the file to that position if a torn tail is found.
    void replayAndTruncate();

    // Apply one decoded record to the in-memory map (called during replay).
    void applyRecord(std::string group,
                     TopicPartition tp,
                     Offset offset,
                     std::string metadata,
                     int64_t commitTimestampNs);

    // -----------------------------------------------------------------------
    // In-memory index: group → { tp → CommittedOffset }
    // -----------------------------------------------------------------------
    using TpMap = std::unordered_map<TopicPartition, CommittedOffset>;
    std::unordered_map<std::string, TpMap> index_;

    std::string path_;
    int         fd_{-1};       // open file descriptor (append mode)
    int64_t     writePos_{0};  // current append position
};

} // namespace rune::coordination
