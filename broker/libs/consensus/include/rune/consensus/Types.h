#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <algorithm>

namespace rune::consensus {

using NodeId = int32_t;
using Term   = int64_t;
using Index  = int64_t;

static constexpr NodeId kNoVote = -1;

struct LogEntry {
    Term        term;
    std::string command;
};

enum class RaftRole { Follower, Candidate, Leader };

struct ClusterConfig {
    NodeId              self;
    std::vector<NodeId> members;

    int  size()     const { return static_cast<int>(members.size()); }
    int  majority() const { return static_cast<int>(members.size()) / 2 + 1; }
    bool isMember(NodeId n) const {
        return std::find(members.begin(), members.end(), n) != members.end();
    }
};

} // namespace rune::consensus
