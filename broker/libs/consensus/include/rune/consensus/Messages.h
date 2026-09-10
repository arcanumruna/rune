#pragma once
#include <rune/consensus/Types.h>
#include <variant>
#include <vector>

namespace rune::consensus {

// ---------------------------------------------------------------------------
// Flat fixed-layout Raft message structs (SBE-style, no heap on hot path)
// ---------------------------------------------------------------------------

struct RequestVote {
    Term   candidateTerm;
    NodeId candidateId;
    Index  lastLogIndex;
    Term   lastLogTerm;
};

struct RequestVoteResult {
    Term term;
    bool voteGranted;
};

struct AppendEntries {
    Term                term;
    NodeId              leaderId;
    Index               prevLogIndex;
    Term                prevLogTerm;
    std::vector<LogEntry> entries;   // empty for heartbeats
    Index               leaderCommit;
};

struct AppendEntriesResult {
    Term  term;
    bool  success;
    Index matchIndex;
};

using RaftMessage = std::variant<RequestVote, RequestVoteResult,
                                 AppendEntries, AppendEntriesResult>;

struct OutboundMessage {
    NodeId      to;
    RaftMessage msg;
};

// Caller owns and REUSES this across ticks (append-only; caller clears between ticks).
using Outbox = std::vector<OutboundMessage>;

} // namespace rune::consensus
