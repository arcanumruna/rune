#pragma once
#include <rune/consensus/Types.h>
#include <string>

namespace rune::consensus {

/**
 * PersistentState — the Raft-required durable state that must survive crashes.
 *
 * Fields persisted (atomically, via write-to-temp + rename + fsync-dir):
 *   currentTerm (int64_t)  — latest term server has seen; monotonically increasing.
 *   votedFor    (int32_t)  — candidateId this node voted for in currentTerm,
 *                            or kNoVote (-1) if not voted.
 *
 * Invariant: setCurrentTerm(newTerm) where newTerm > currentTerm resets
 *   votedFor to kNoVote.  If you need to set both atomically (e.g. when
 *   granting a vote in the same RPC that advances the term), use update().
 */
class PersistentState {
public:
    /// Open or create the state file at `path`.
    /// If the file exists and is valid, load it; otherwise initialise to
    /// currentTerm=0, votedFor=kNoVote and persist immediately.
    explicit PersistentState(std::string path);

    Term   currentTerm() const;
    NodeId votedFor()    const;

    /// Advance the current term.  If newTerm > currentTerm, votedFor is reset
    /// to kNoVote (standard Raft).  No-op if newTerm == currentTerm.
    /// Throws std::invalid_argument if newTerm < currentTerm.
    void setCurrentTerm(Term newTerm);

    /// Record a vote for the current term.  Does NOT change currentTerm.
    void setVotedFor(NodeId candidate);

    /// Atomically set both term and vote (used when granting a vote that also
    /// bumps the term in the same step).  Equivalent to a single flushed write
    /// of both fields together.
    void update(Term newTerm, NodeId candidate);

private:
    std::string path_;
    Term        currentTerm_{0};
    NodeId      votedFor_{kNoVote};

    void persist() const;   ///< Write both fields durably; throws on I/O error.
    void load();            ///< Load from file; called from constructor.
};

} // namespace rune::consensus
