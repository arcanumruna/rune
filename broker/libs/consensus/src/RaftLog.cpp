#include <rune/consensus/RaftLog.h>

namespace rune::consensus {

Index RaftLog::append(Term term, std::string command) {
    entries_.push_back({term, std::move(command)});
    return lastIndex();
}

Index RaftLog::lastIndex() const {
    return static_cast<Index>(entries_.size());
}

Term RaftLog::lastTerm() const {
    if (entries_.empty()) {
        return 0;
    }
    return entries_.back().term;
}

Term RaftLog::termAt(Index i) const {
    if (i <= 0 || i > lastIndex()) {
        return 0;
    }
    return entries_[i - 1].term;
}

const LogEntry& RaftLog::at(Index i) const {
    if (i < 1 || i > lastIndex()) {
        throw std::out_of_range("RaftLog::at: index out of range");
    }
    return entries_[i - 1];
}

std::vector<LogEntry> RaftLog::entriesFrom(Index start) const {
    if (start < 1) {
        start = 1;
    }
    if (start > lastIndex()) {
        return {};
    }
    // start is 1-indexed; convert to 0-indexed for vector
    auto it_start = entries_.begin() + (start - 1);
    return std::vector<LogEntry>(it_start, entries_.end());
}

void RaftLog::truncateFrom(Index from) {
    if (from > lastIndex()) {
        return;  // no-op
    }
    if (from <= 1) {
        entries_.clear();
    } else {
        // from is 1-indexed; keep entries [1 .. from-1]
        entries_.erase(entries_.begin() + (from - 1), entries_.end());
    }
}

Index RaftLog::commitIndex() const {
    return commitIndex_;
}

void RaftLog::setCommitIndex(Index c) {
    // Monotonic non-decreasing, clamped to [commitIndex_, lastIndex()]
    commitIndex_ = std::max(commitIndex_, std::min(c, lastIndex()));
}

}  // namespace rune::consensus
