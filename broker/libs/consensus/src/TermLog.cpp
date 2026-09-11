// TermLog.cpp — faithful C++ port of io.aeron.cluster.RecordingLog (TERM entries only)
//
// Key Java references:
//   RecordingLog.java:950–988   appendTerm()
//   RecordingLog.java:1085–1100 commitLogPosition()
//   RecordingLog.java:718–730   findLastTerm()
//   RecordingLog.java:738–747   getTermEntry() / findTermEntry()
//   RecordingLog.java:1102–1119 invalidateEntry()
//   RecordingLog.java:1339–1352 writeEntryToBuffer()
//   RecordingLog.java:1590–1630 persistToStorage()
//   RecordingLog.java:1632–1680 captureEntriesFromBuffer()

#include <rune/consensus/TermLog.h>

#include <bit>
#include <cerrno>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <system_error>

// POSIX file I/O
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace rune::consensus {

// ---------------------------------------------------------------------------
// Little-endian helpers
// ---------------------------------------------------------------------------

namespace {

// Write a 64-bit value to a byte buffer at @p offset in little-endian order.
void putInt64LE(uint8_t* buf, std::size_t offset, int64_t val) noexcept {
    const uint64_t u = static_cast<uint64_t>(val);
    buf[offset + 0] = static_cast<uint8_t>(u);
    buf[offset + 1] = static_cast<uint8_t>(u >>  8u);
    buf[offset + 2] = static_cast<uint8_t>(u >> 16u);
    buf[offset + 3] = static_cast<uint8_t>(u >> 24u);
    buf[offset + 4] = static_cast<uint8_t>(u >> 32u);
    buf[offset + 5] = static_cast<uint8_t>(u >> 40u);
    buf[offset + 6] = static_cast<uint8_t>(u >> 48u);
    buf[offset + 7] = static_cast<uint8_t>(u >> 56u);
}

// Write a 32-bit value to a byte buffer at @p offset in little-endian order.
void putInt32LE(uint8_t* buf, std::size_t offset, int32_t val) noexcept {
    const uint32_t u = static_cast<uint32_t>(val);
    buf[offset + 0] = static_cast<uint8_t>(u);
    buf[offset + 1] = static_cast<uint8_t>(u >>  8u);
    buf[offset + 2] = static_cast<uint8_t>(u >> 16u);
    buf[offset + 3] = static_cast<uint8_t>(u >> 24u);
}

// Read a 64-bit little-endian value from a byte buffer at @p offset.
int64_t getInt64LE(const uint8_t* buf, std::size_t offset) noexcept {
    uint64_t u = 0;
    u |= static_cast<uint64_t>(buf[offset + 0]);
    u |= static_cast<uint64_t>(buf[offset + 1]) <<  8u;
    u |= static_cast<uint64_t>(buf[offset + 2]) << 16u;
    u |= static_cast<uint64_t>(buf[offset + 3]) << 24u;
    u |= static_cast<uint64_t>(buf[offset + 4]) << 32u;
    u |= static_cast<uint64_t>(buf[offset + 5]) << 40u;
    u |= static_cast<uint64_t>(buf[offset + 6]) << 48u;
    u |= static_cast<uint64_t>(buf[offset + 7]) << 56u;
    return static_cast<int64_t>(u);
}

// Read a 32-bit little-endian value from a byte buffer at @p offset.
int32_t getInt32LE(const uint8_t* buf, std::size_t offset) noexcept {
    uint32_t u = 0;
    u |= static_cast<uint32_t>(buf[offset + 0]);
    u |= static_cast<uint32_t>(buf[offset + 1]) <<  8u;
    u |= static_cast<uint32_t>(buf[offset + 2]) << 16u;
    u |= static_cast<uint32_t>(buf[offset + 3]) << 24u;
    return static_cast<int32_t>(u);
}

// Full pwrite that retries on EINTR and asserts full @p len bytes written.
// Mirrors RecordingLog.java:1590–1606 persistToStorage().
void fullPwrite(int fd, const void* buf, std::size_t len, off_t offset) {
    const auto* ptr = static_cast<const uint8_t*>(buf);
    std::size_t remaining = len;
    off_t pos = offset;

    while (remaining > 0) {
        const ssize_t written = ::pwrite(fd, ptr, remaining, pos);
        if (written < 0) {
            if (errno == EINTR) { continue; }
            throw std::system_error(errno, std::system_category(),
                                    "TermLog: pwrite failed");
        }
        if (written == 0) {
            throw std::runtime_error("TermLog: pwrite wrote 0 bytes");
        }
        ptr       += static_cast<std::size_t>(written);
        pos       += static_cast<off_t>(written);
        remaining -= static_cast<std::size_t>(written);
    }
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// TermLog::serialiseRecord — mirrors RecordingLog.java:1339–1352
// ---------------------------------------------------------------------------

void TermLog::serialiseRecord(uint8_t buf[kRecordSize], const TermEntry& e) {
    // Zero the whole 64-byte record first (covers reserved padding bytes).
    std::memset(buf, 0, kRecordSize);

    putInt64LE(buf, kRecordingIdOffset,         e.recordingId);
    putInt64LE(buf, kLeadershipTermIdOffset,    static_cast<int64_t>(e.leadershipTermId));
    putInt64LE(buf, kTermBaseLogPositionOffset, e.termBaseLogPosition);
    putInt64LE(buf, kLogPositionOffset,         e.logPosition);
    putInt64LE(buf, kTimestampOffset,           e.timestamp);
    putInt32LE(buf, kServiceIdOffset,           e.serviceId);
    putInt32LE(buf, kEntryTypeOffset,           e.entryType);
}

// ---------------------------------------------------------------------------
// Construction / destruction
// ---------------------------------------------------------------------------

TermLog::TermLog(std::string path)
    : path_(std::move(path))
{
    // Check if file already exists.
    struct stat st{};
    const bool exists = (::stat(path_.c_str(), &st) == 0);

    // Open (or create) the backing file.
    // RecordingLog.java:566–593: opens with READ|WRITE, adds CREATE for new files.
    const int flags = O_RDWR | O_CREAT;
    fd_ = ::open(path_.c_str(), flags, 0644);
    if (fd_ < 0) {
        throw std::system_error(errno, std::system_category(),
                                "TermLog: open " + path_);
    }

    if (exists && st.st_size > 0) {
        // Reload existing records.
        // Mirrors RecordingLog.java:585–588 (reload() on existing file).
        loadEntries();
    }
    // New file: entries_ remains empty.
}

TermLog::~TermLog() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

// ---------------------------------------------------------------------------
// loadEntries — mirrors RecordingLog.java:648–699 reload()
// ---------------------------------------------------------------------------

void TermLog::loadEntries() {
    entries_.clear();
    indexByTerm_.clear();

    // Determine file size.
    struct stat st{};
    if (::fstat(fd_, &st) < 0) {
        throw std::system_error(errno, std::system_category(),
                                "TermLog: fstat " + path_);
    }

    const auto fileSize = static_cast<int64_t>(st.st_size);
    const auto recordSize = static_cast<int64_t>(kRecordSize);

    // Read full records sequentially.
    // RecordingLog.java:1632–1673 captureEntriesFromBuffer():
    //   entryType = buffer.getInt(ENTRY_TYPE_OFFSET)
    //   type      = entryType & ~ENTRY_TYPE_INVALID_FLAG
    //   isValid   = (entryType & ENTRY_TYPE_INVALID_FLAG) == 0
    for (int64_t offset = 0; offset + recordSize <= fileSize; offset += recordSize) {
        uint8_t buf[kRecordSize];
        const ssize_t n = ::pread(fd_, buf, kRecordSize, static_cast<off_t>(offset));
        if (n != static_cast<ssize_t>(kRecordSize)) {
            // Partial read — truncated file; stop.
            break;
        }

        const int32_t rawEntryType = getInt32LE(buf, kEntryTypeOffset);
        const int32_t type         = rawEntryType & ~kEntryTypeInvalidFlag;
        const bool    isValid      = (rawEntryType & kEntryTypeInvalidFlag) == 0;

        // Skip removed entries (NULL_VALUE type = -1).
        // RecordingLog.java:1665: if (NULL_VALUE != entryType) { entries.add(entry); }
        if (rawEntryType == -1) {
            continue;
        }

        // Phase 2: only load TERM entries.
        if (type != kEntryTypeTerm) {
            continue;
        }

        TermEntry e{};
        e.recordingId         = getInt64LE(buf, kRecordingIdOffset);
        e.leadershipTermId    = static_cast<Term>(getInt64LE(buf, kLeadershipTermIdOffset));
        e.termBaseLogPosition = getInt64LE(buf, kTermBaseLogPositionOffset);
        e.logPosition         = getInt64LE(buf, kLogPositionOffset);
        e.timestamp           = getInt64LE(buf, kTimestampOffset);
        e.serviceId           = getInt32LE(buf, kServiceIdOffset);
        e.entryType           = type;
        e.isValid             = isValid;
        e.fileOffset          = offset;  // mirrors RecordingLog.Entry.position

        indexByTerm_[e.leadershipTermId] = entries_.size();
        entries_.push_back(e);
    }
    // NOTE: no sort — entries_ is in append/file-record order, matching Aeron's ArrayList.
    // indexByTerm_ provides O(1) lookup by leadershipTermId.
}

// ---------------------------------------------------------------------------
// writeRecord — mirrors RecordingLog.java:1608–1630 persistToStorage(entry, buffer)
//   Finds the next 64-byte-aligned position at end of file and writes the record.
//   Returns the file offset of the written record.
// ---------------------------------------------------------------------------

int64_t TermLog::writeRecord(const TermEntry& entry) {
    // Determine current file size.
    struct stat st{};
    if (::fstat(fd_, &st) < 0) {
        throw std::system_error(errno, std::system_category(),
                                "TermLog: fstat before write");
    }

    // Align to kRecordSize (64 bytes).
    // RecordingLog.java:1617: long nextPosition = align(fileChannel.size(), RECORD_ALIGNMENT)
    const auto rawSize   = static_cast<int64_t>(st.st_size);
    const auto alignment = static_cast<int64_t>(kRecordSize);
    const int64_t recordOffset = ((rawSize + alignment - 1) / alignment) * alignment;

    uint8_t buf[kRecordSize];
    serialiseRecord(buf, entry);

    fullPwrite(fd_, buf, kRecordSize, static_cast<off_t>(recordOffset));

    // A2 fix (audit 2026-09-11): Aeron RecordingLog.persistToStorage() calls
    // fileChannel.force(false) after every write.  Without fsync/fsync the record
    // sits only in the page cache; a power loss before OS writeback loses the committed
    // leadershipTermId and its logPosition, causing nextLeadershipTermId() to regress
    // on recovery.  Mirrors RecordingLog.java:1590-1630.
    if (::fsync(fd_) < 0) {
        throw std::system_error(errno, std::system_category(),
                                "TermLog: fsync after writeRecord");
    }

    return recordOffset;
}

// ---------------------------------------------------------------------------
// patchInt64 / patchInt32
// ---------------------------------------------------------------------------

void TermLog::patchInt64(int64_t recordOffset, std::size_t fieldOffset, int64_t value) {
    uint8_t buf[8];
    putInt64LE(buf, 0, value);
    fullPwrite(fd_, buf, 8, static_cast<off_t>(recordOffset + static_cast<int64_t>(fieldOffset)));
    // A2 fix: fsync after patch — mirrors RecordingLog.java persistToStorage() force(false).
    // Committed logPosition must survive a crash; leaving it only in the page cache
    // allows nextLeadershipTermId() to regress on recovery.
    if (::fsync(fd_) < 0) {
        throw std::system_error(errno, std::system_category(),
                                "TermLog: fsync after patchInt64");
    }
}

void TermLog::patchInt32(int64_t recordOffset, std::size_t fieldOffset, int32_t value) {
    uint8_t buf[4];
    putInt32LE(buf, 0, value);
    fullPwrite(fd_, buf, 4, static_cast<off_t>(recordOffset + static_cast<int64_t>(fieldOffset)));
    // A2 fix: fsync after patch — same rationale as patchInt64.
    if (::fsync(fd_) < 0) {
        throw std::system_error(errno, std::system_category(),
                                "TermLog: fsync after patchInt32");
    }
}

// ---------------------------------------------------------------------------
// findIndex
// ---------------------------------------------------------------------------

int TermLog::findIndex(Term leadershipTermId) const noexcept {
    const auto it = indexByTerm_.find(leadershipTermId);
    if (it == indexByTerm_.end()) {
        return -1;
    }
    return static_cast<int>(it->second);
}

// ---------------------------------------------------------------------------
// appendTerm — mirrors RecordingLog.java:950–988
// ---------------------------------------------------------------------------

void TermLog::appendTerm(int64_t recordingId, Term leadershipTermId,
                         int64_t termBaseLogPosition, int64_t timestamp)
{
    // RecordingLog.java:959: if (cacheIndexByLeadershipTermIdMap.containsKey(leadershipTermId))
    //   throw new ClusterException("duplicate TERM entry");
    if (indexByTerm_.count(leadershipTermId) > 0) {
        const TermEntry& existing = entries_[indexByTerm_.at(leadershipTermId)];
        if (existing.isValid) {
            throw std::runtime_error("TermLog: duplicate TERM entry for leadershipTermId=" +
                                     std::to_string(leadershipTermId));
        }
    }

    TermEntry e{};
    e.recordingId         = recordingId;
    e.leadershipTermId    = leadershipTermId;
    e.termBaseLogPosition = termBaseLogPosition;
    e.logPosition         = kNullPosition;   // OPEN — RecordingLog.java:955
    e.timestamp           = timestamp;
    e.serviceId           = kTermServiceId;  // SERVICE_ID sentinel
    e.entryType           = kEntryTypeTerm;  // 0
    e.isValid             = true;

    // Persist to file first; writeRecord returns the file offset of this record.
    e.fileOffset = writeRecord(e);

    // Append in insertion order, matching Aeron's ArrayList (never sorted on append).
    indexByTerm_[leadershipTermId] = entries_.size();
    entries_.push_back(e);
}

// ---------------------------------------------------------------------------
// commitLogPosition — mirrors RecordingLog.java:1085–1100
// ---------------------------------------------------------------------------

void TermLog::commitLogPosition(Term leadershipTermId, int64_t logPosition) {
    // RecordingLog.java:1087–1090: look up by term id in cache map.
    const int idx = findIndex(leadershipTermId);
    if (idx < 0) {
        throw std::runtime_error("TermLog: unknown leadershipTermId=" +
                                 std::to_string(leadershipTermId));
    }

    TermEntry& e = entries_[static_cast<std::size_t>(idx)];

    // RecordingLog.java:1094: if (entry.logPosition != logPosition) { ... persist }
    if (e.logPosition == logPosition) {
        return;
    }

    // RecordingLog.java:1096–1097:
    //   buffer.putLong(0, logPosition, LITTLE_ENDIAN);
    //   persistToStorage(entry.position, LOG_POSITION_OFFSET, SIZE_OF_LONG);
    // Use the stored file offset (entry.position in Java) — correct regardless of append order.
    patchInt64(e.fileOffset, kLogPositionOffset, logPosition);

    e.logPosition = logPosition;
}

// ---------------------------------------------------------------------------
// getTermEntry — mirrors RecordingLog.java:755–764 findTermEntry()
// ---------------------------------------------------------------------------

std::optional<TermEntry> TermLog::getTermEntry(Term leadershipTermId) const {
    const int idx = findIndex(leadershipTermId);
    if (idx < 0) {
        return std::nullopt;
    }
    const TermEntry& e = entries_[static_cast<std::size_t>(idx)];
    if (!e.isValid) {
        return std::nullopt;
    }
    return e;
}

// ---------------------------------------------------------------------------
// lastTermEntry — mirrors RecordingLog.java:718–730 findLastTerm()
//   Iterates entries_ in reverse to find the last valid term entry.
// ---------------------------------------------------------------------------

std::optional<TermEntry> TermLog::lastTermEntry() const {
    // Aeron's RecordingLog.java:718–730 findLastTerm() iterates in reverse and
    // returns the first valid TERM entry. In Aeron, entries are in insertion order
    // and terms are always appended in increasing order — so reverse == highest term first.
    // Here we preserve that semantic while also handling out-of-order appends:
    // scan all entries and return the valid TERM entry with the highest leadershipTermId.
    const TermEntry* best = nullptr;
    for (const auto& e : entries_) {
        if (e.isValid && e.entryType == kEntryTypeTerm) {
            if (best == nullptr || e.leadershipTermId > best->leadershipTermId) {
                best = &e;
            }
        }
    }
    if (best == nullptr) {
        return std::nullopt;
    }
    return *best;
}

// ---------------------------------------------------------------------------
// invalidateEntry — mirrors RecordingLog.java:1102–1119
// ---------------------------------------------------------------------------

void TermLog::invalidateEntry(Term leadershipTermId) {
    const int idx = findIndex(leadershipTermId);
    if (idx < 0) {
        return; // no-op
    }

    TermEntry& e = entries_[static_cast<std::size_t>(idx)];
    if (!e.isValid) {
        return; // already invalid
    }

    // RecordingLog.java:1116–1118:
    //   int invalidEntryType = ENTRY_TYPE_INVALID_FLAG | invalidEntry.type;
    //   buffer.putInt(0, invalidEntryType, LITTLE_ENDIAN);
    //   persistToStorage(invalidEntry.position, ENTRY_TYPE_OFFSET, SIZE_OF_INT);
    const int32_t invalidEntryType = kEntryTypeInvalidFlag | e.entryType;
    patchInt32(e.fileOffset, kEntryTypeOffset, invalidEntryType);

    e.entryType = invalidEntryType;
    e.isValid   = false;
}

// ---------------------------------------------------------------------------
// validTermEntries
// ---------------------------------------------------------------------------

std::vector<TermEntry> TermLog::validTermEntries() const {
    std::vector<TermEntry> result;
    result.reserve(entries_.size());
    for (const auto& e : entries_) {
        if (e.isValid && e.entryType == kEntryTypeTerm) {
            result.push_back(e);
        }
    }
    return result;
}

// ---------------------------------------------------------------------------
// nextLeadershipTermId
// ---------------------------------------------------------------------------

Term TermLog::nextLeadershipTermId() const {
    // Election.java initialises leadershipTermId from the last RecordingLog term entry.
    // If no entries, start at 0.
    const auto last = lastTermEntry();
    if (!last.has_value()) {
        return 0;
    }
    return last->leadershipTermId + 1;
}

} // namespace rune::consensus
