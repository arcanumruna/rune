#pragma once
#include <rune/consensus/Types.h>
#include <cassert>
#include <stdexcept>

namespace rune::consensus {

/**
 * @brief In-memory Raft log with 1-based indexing.
 *
 * Log entries are stored in a vector where vector element k holds log index k+1.
 * Index 0 is a virtual sentinel entry with term 0 (represents no entry).
 * Index 1 is the first real log entry.
 *
 * All methods are 1-indexed (except internal vector indexing).
 */
class RaftLog {
public:
    RaftLog() = default;

    /**
     * Append a new entry to the log.
     * @param term The term of the entry.
     * @param command The command string.
     * @return The new lastIndex after append.
     */
    Index append(Term term, std::string command);

    /**
     * @return The index of the last entry in the log, or 0 if empty.
     */
    Index lastIndex() const;

    /**
     * @return The term of the last entry, or 0 if empty.
     */
    Term lastTerm() const;

    /**
     * Get the term at a specific index.
     * @param i The 1-indexed position.
     * @return The term at index i, or 0 if i<=0 or i>lastIndex(). Never throws.
     */
    Term termAt(Index i) const;

    /**
     * Get a reference to the log entry at a specific index.
     * @param i The 1-indexed position.
     * @return Reference to the LogEntry.
     * @throw std::out_of_range if i<1 or i>lastIndex().
     */
    const LogEntry& at(Index i) const;

    /**
     * Get all entries from start to lastIndex (inclusive).
     * @param start The 1-indexed starting position; clamped to >=1.
     * @return A vector of entries. Empty if start>lastIndex().
     */
    std::vector<LogEntry> entriesFrom(Index start) const;

    /**
     * Remove all entries from index `from` onwards (conflict resolution).
     * @param from The 1-indexed starting position to remove.
     *             If from>lastIndex(), this is a no-op.
     *             If from<=1, the log is emptied.
     */
    void truncateFrom(Index from);

    /**
     * @return The current commit index.
     */
    Index commitIndex() const;

    /**
     * Set the commit index (monotonic non-decreasing, clamped to lastIndex).
     * @param c The desired commit index.
     *          Will be clamped to [commitIndex_, lastIndex()].
     */
    void setCommitIndex(Index c);

private:
    std::vector<LogEntry> entries_;  // entries_[k] = log index k+1
    Index commitIndex_ = 0;
};

}  // namespace rune::consensus
