#pragma once
// TermLog.h — faithful C++ port of io.aeron.cluster.RecordingLog (TERM entries only)
// Phase-2 non-Archive form: durable term-metadata storage without Aeron Archive dependency.
//
// See: aeron-cluster/src/main/java/io/aeron/cluster/RecordingLog.java:58–92 (entry layout)
//      RecordingLog.java:950–988   (appendTerm)
//      RecordingLog.java:1085–1100 (commitLogPosition)
//      RecordingLog.java:738–747   (getTermEntry)
//      RecordingLog.java:718–730   (findLastTerm)
//      RecordingLog.java:1102–1119 (invalidateEntry)

#include <rune/consensus/Types.h>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace rune::consensus {

// Mirrors ConsensusModule.Configuration.SERVICE_ID = Aeron.NULL_VALUE = -1
// Used as serviceId sentinel for term entries (not snapshots).
// RecordingLog.java: import static io.aeron.cluster.ConsensusModule.Configuration.SERVICE_ID;
static constexpr int32_t kTermServiceId = -1;

// Entry type for a leadership term log record.
// RecordingLog.java:437: public static final int ENTRY_TYPE_TERM = 0;
static constexpr int32_t kEntryTypeTerm = 0;

// High bit of entryType = invalid flag.
// RecordingLog.java:453: public static final int ENTRY_TYPE_INVALID_FLAG = 1 << 31;
static constexpr int32_t kEntryTypeInvalidFlag = static_cast<int32_t>(1u << 31u);

// NULL_POSITION sentinel: log position for an OPEN (uncommitted) term entry.
// Matches AeronArchive.NULL_POSITION = Long.MIN_VALUE in Java.
// RecordingLog.java:955: long logPosition = NULL_POSITION;
// Note: kNullPosition is also defined in ClusterMember.h; guard against redefinition
// when both headers appear in the same translation unit (e.g. via Election.h).
#ifndef RUNE_CONSENSUS_K_NULL_POSITION_DEFINED
#define RUNE_CONSENSUS_K_NULL_POSITION_DEFINED
static constexpr int64_t kNullPosition = std::numeric_limits<int64_t>::min();
#endif

/**
 * In-memory representation of a single term entry in the TermLog.
 *
 * Mirrors RecordingLog.Entry for ENTRY_TYPE_TERM records.
 * RecordingLog.java:100–202.
 *
 * - logPosition == kNullPosition while the term is OPEN (not yet committed).
 * - isValid==false when the entry has been invalidated (high bit set on entryType).
 * - entryType == kEntryTypeTerm (0) for valid entries.
 * - serviceId == kTermServiceId (-1) for term entries.
 */
struct TermEntry {
    int64_t  recordingId;          ///< Archive recording identifier (or 0 for Phase-2 stub)
    Term     leadershipTermId;     ///< Leadership term this entry covers
    int64_t  termBaseLogPosition;  ///< Accumulated byte offset at start of this term
    int64_t  logPosition;          ///< Committed position (kNullPosition = OPEN)
    int64_t  timestamp;            ///< Cluster clock timestamp at term start
    int32_t  serviceId;            ///< kTermServiceId (-1) sentinel for term entries
    int32_t  entryType;            ///< kEntryTypeTerm (0), or with high bit = invalid
    bool     isValid;              ///< false when marked invalid via invalidateEntry
    int64_t  fileOffset = 0;       ///< Byte offset of this record in the backing file
                                   ///<   (mirrors RecordingLog.Entry.position)
};

/**
 * Durable append-only file of fixed-size TermEntry records.
 *
 * Faithful port of RecordingLog restricted to TERM entries (SNAPSHOT / STANDBY_SNAPSHOT
 * are Phase-3 concerns). Each mutation writes / rewrites the affected record(s) directly
 * to the backing file so the log survives process restart.
 *
 * On-disk record layout (little-endian, fixed 64 bytes per record):
 *   [0 .. 7]   int64_t  recordingId
 *   [8 ..15]   int64_t  leadershipTermId
 *   [16..23]   int64_t  termBaseLogPosition
 *   [24..31]   int64_t  logPosition
 *   [32..39]   int64_t  timestamp
 *   [40..43]   int32_t  serviceId
 *   [44..47]   int32_t  entryType   (high-bit = invalid flag)
 *   [48..63]   reserved / padding to 64-byte record alignment
 *
 * RecordingLog uses 64-byte RECORD_ALIGNMENT (line 502) and the fixed
 * ENDPOINT_OFFSET = 48 bytes for non-standby entries.
 */
class TermLog {
public:
    /**
     * Open or create the term log at @p path.
     *
     * If the file already exists its existing records are loaded into memory
     * (mirrors RecordingLog constructor calling reload() on an existing file).
     * If the file does not exist it is created empty.
     *
     * @param path absolute path to the backing file.
     */
    explicit TermLog(std::string path);
    ~TermLog();

    TermLog(const TermLog&)            = delete;
    TermLog& operator=(const TermLog&) = delete;

    /**
     * Append an OPEN term entry (logPosition = kNullPosition).
     *
     * Mirrors RecordingLog.java:950–988 appendTerm().
     * Persists the new record to the backing file.
     *
     * @param recordingId         Archive recording id (Phase-2: caller-supplied, e.g. 0 or actual id)
     * @param leadershipTermId    Term id — must not already exist in the log.
     * @param termBaseLogPosition Accumulated log position at the start of this term.
     * @param timestamp           Cluster clock timestamp.
     */
    void appendTerm(int64_t recordingId, Term leadershipTermId,
                    int64_t termBaseLogPosition, int64_t timestamp);

    /**
     * Commit (close) the log position for an existing open term entry.
     *
     * Mirrors RecordingLog.java:1085–1100 commitLogPosition().
     * Overwrites the logPosition field of the matching record on disk.
     *
     * @param leadershipTermId  Term to commit.
     * @param logPosition       Committed byte position (must not be kNullPosition).
     */
    void commitLogPosition(Term leadershipTermId, int64_t logPosition);

    /**
     * Return the valid term entry for the given leadershipTermId.
     *
     * Mirrors RecordingLog.java:755–764 findTermEntry() (returns nullopt instead of null).
     *
     * @return nullopt if the term does not exist or has been invalidated.
     */
    [[nodiscard]] std::optional<TermEntry> getTermEntry(Term leadershipTermId) const;

    /**
     * Return the highest-leadershipTermId valid term entry.
     *
     * Mirrors RecordingLog.java:718–730 findLastTerm().
     *
     * @return nullopt if the log contains no valid term entries.
     */
    [[nodiscard]] std::optional<TermEntry> lastTermEntry() const;

    /**
     * Mark the term entry for @p leadershipTermId as invalid.
     *
     * Sets the high bit on entryType (ENTRY_TYPE_INVALID_FLAG) and persists
     * the change to the backing file. The entry is removed from the valid-term
     * index so subsequent calls to getTermEntry / lastTermEntry skip it.
     *
     * Mirrors RecordingLog.java:1102–1119 invalidateEntry().
     *
     * @param leadershipTermId  Term to invalidate (no-op if not found).
     */
    void invalidateEntry(Term leadershipTermId);

    /**
     * Return all valid term entries in ascending leadershipTermId order.
     */
    [[nodiscard]] std::vector<TermEntry> validTermEntries() const;

    /**
     * Derive the next leadership term id.
     *
     * If there is a last valid term entry, returns lastTerm.leadershipTermId + 1.
     * Otherwise returns 0 (start of time, no prior terms).
     *
     * Mirrors how Election.java initialises leadershipTermId from the last
     * RecordingLog term entry + 1.
     */
    [[nodiscard]] Term nextLeadershipTermId() const;

private:
    // On-disk record size: 64 bytes (RECORD_ALIGNMENT in RecordingLog.java:502).
    static constexpr std::size_t kRecordSize = 64;

    // Byte offsets within each record (little-endian).
    // RecordingLog.java:458–493.
    static constexpr std::size_t kRecordingIdOffset        =  0;
    static constexpr std::size_t kLeadershipTermIdOffset   =  8;
    static constexpr std::size_t kTermBaseLogPositionOffset = 16;
    static constexpr std::size_t kLogPositionOffset        = 24;
    static constexpr std::size_t kTimestampOffset          = 32;
    static constexpr std::size_t kServiceIdOffset          = 40;
    static constexpr std::size_t kEntryTypeOffset          = 44;

    std::string            path_;
    int                    fd_ = -1;
    std::vector<TermEntry> entries_;  ///< in-memory cache, in APPEND/insertion order (index i == file record i)
    std::unordered_map<Term, std::size_t> indexByTerm_;  ///< leadershipTermId → entries_ index (O(1) lookup)

    // Load all records from the backing file into entries_.
    void loadEntries();

    // Append a new record to the end of the backing file.
    // Returns the file offset (byte position) of the written record.
    int64_t writeRecord(const TermEntry& entry);

    // Overwrite a single int64_t field within the record at file offset @p recordOffset.
    void patchInt64(int64_t recordOffset, std::size_t fieldOffset, int64_t value);

    // Overwrite a single int32_t field within the record at file offset @p recordOffset.
    void patchInt32(int64_t recordOffset, std::size_t fieldOffset, int32_t value);

    // Serialise one record into a 64-byte little-endian buffer.
    static void serialiseRecord(uint8_t buf[kRecordSize], const TermEntry& entry);

    // Find the entries_ index for the given leadershipTermId via indexByTerm_.
    // Returns -1 if not found. The returned index IS the file-record index.
    [[nodiscard]] int findIndex(Term leadershipTermId) const noexcept;
};

} // namespace rune::consensus
