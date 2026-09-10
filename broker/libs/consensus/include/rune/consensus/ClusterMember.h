#pragma once
// Faithful C++ port of Aeron's ClusterMember.
// Source: aeron-cluster/src/main/java/io/aeron/cluster/ClusterMember.java
//
// Fields: ClusterMember.java:44-66
// Algorithms: compareLog 1173-1197, quorumThreshold 853-856,
//             quorumPosition 867-895, hasActiveQuorum 828-845,
//             hasQuorumAtPosition 907-925, isUnanimousLeader 981-1002,
//             isQuorumLeader 1015-1036, willVoteFor 1292-1298.

#include <cstdint>
#include <span>
#include <rune/consensus/Types.h>

namespace rune::consensus {

// Tri-state vote, mirrors Java's Boolean (null / TRUE / FALSE).
// ClusterMember.java:66  private Boolean vote = null;
enum class Vote : int8_t {
    None    = 0,  // null   — no vote recorded
    For     = 1,  // TRUE  — voted for candidate
    Against = 2,  // FALSE — voted against candidate
};

// Sentinel values matching Aeron's NULL_VALUE / NULL_POSITION.
// io.aeron.Aeron.NULL_VALUE == Long.MIN_VALUE (-2^63)
// io.aeron.archive.client.AeronArchive.NULL_POSITION == Long.MIN_VALUE
static constexpr int64_t kNullValue    = INT64_MIN; // Aeron.NULL_VALUE
static constexpr int64_t kNullPosition = INT64_MIN; // AeronArchive.NULL_POSITION

// ClusterMember — represents one member of the cluster from this node's perspective.
// Fields mirror ClusterMember.java:44-66.
struct ClusterMember {
    // ClusterMember.java:48
    NodeId  id                        = -1;
    // ClusterMember.java:49  private long leadershipTermId = NULL_VALUE;
    Term    leadershipTermId          = kNullValue;
    // ClusterMember.java:50  private long candidateTermId = NULL_VALUE;
    Term    candidateTermId           = kNullValue;
    // ClusterMember.java:54  private long logPosition = NULL_POSITION;
    int64_t logPosition               = kNullPosition;
    // logLeadershipTermId holds the leadership term of this member's most-recently-appended
    // log entry; consumed by the Election layer's 4-arg compareLog(logLeadershipTermId, logPosition, ...)
    // for canvass/up-to-date checks (NOT by the 2-arg member overload).
    Term    logLeadershipTermId       = kNullValue;
    // ClusterMember.java:55  private long timeOfLastAppendPositionNs = NULL_VALUE;
    int64_t timeOfLastAppendPositionNs = kNullValue;
    // ClusterMember.java:66  private Boolean vote = null;
    Vote    vote                      = Vote::None;

    // -----------------------------------------------------------------------
    // Instance helpers
    // -----------------------------------------------------------------------

    // ClusterMember.java:1287-1290
    // member.isActive(nowNs, timeoutNs):
    //   return timeOfLastAppendPositionNs + timeoutNs > nowNs;
    bool isActive(int64_t nowNs, int64_t timeoutNs) const noexcept {
        // overflow-safe form of (timeOfLastAppendPositionNs + timeoutNs > nowNs)
        return nowNs - timeoutNs < timeOfLastAppendPositionNs;
    }

    // ClusterMember.java:1300-1303
    // hasReachedPosition used inside hasQuorumAtPosition
    bool hasReachedPosition(Term ltId, int64_t position, int64_t nowNs, int64_t timeoutNs) const noexcept {
        return isActive(nowNs, timeoutNs) && ltId == leadershipTermId && logPosition >= position;
    }

    // ClusterMember.java:140-149  reset()
    // Clears transient election state; mirrors Java reset().
    void reset() noexcept {
        vote              = Vote::None;
        candidateTermId   = kNullValue;
        leadershipTermId  = kNullValue;
        logPosition       = kNullPosition;
    }

    // -----------------------------------------------------------------------
    // Static algorithms (exact ports of Java statics)
    // -----------------------------------------------------------------------

    // ClusterMember.java:1173-1197  compareLog(long,long,long,long)
    // Primary key: logLeadershipTermId (higher = more recent).
    // Secondary key: logPosition (higher = more recent).
    // Returns: +1 if lhs more recent, 0 if equal, -1 if rhs more recent.
    static int compareLog(
        Term    lhsLogLeadershipTermId,
        int64_t lhsLogPosition,
        Term    rhsLogLeadershipTermId,
        int64_t rhsLogPosition) noexcept
    {
        if (lhsLogLeadershipTermId > rhsLogLeadershipTermId) return  1;
        if (lhsLogLeadershipTermId < rhsLogLeadershipTermId) return -1;
        if (lhsLogPosition         > rhsLogPosition)         return  1;
        if (lhsLogPosition         < rhsLogPosition)         return -1;
        return 0;
    }

    // ClusterMember.java:1208-1211  compareLog(ClusterMember, ClusterMember)
    // Delegates to the 4-arg overload using each member's leadershipTermId + logPosition.
    static int compareLog(const ClusterMember& lhs, const ClusterMember& rhs) noexcept {
        return compareLog(lhs.leadershipTermId, lhs.logPosition,
                          rhs.leadershipTermId, rhs.logPosition);
    }

    // ClusterMember.java:853-856  quorumThreshold(int)
    // floor(N/2) + 1  — the strict majority needed for quorum.
    static int quorumThreshold(int memberCount) noexcept {
        return (memberCount >> 1) + 1;
    }

    // ClusterMember.java:867-895  quorumPosition(ClusterMember[], long[], long, long)
    // Insertion-sort the log positions of ACTIVE members (top-K, descending).
    // rankedPositions must be pre-sized to quorumThreshold(members.size()).
    // Returns rankedPositions[K-1] = K-th highest = the quorum position.
    // NO allocation inside — caller owns rankedPositions.
    static int64_t quorumPosition(
        std::span<const ClusterMember> members,
        std::span<int64_t>             rankedPositions,
        int64_t                        nowNs,
        int64_t                        timeoutNs) noexcept
    {
        const int length = static_cast<int>(rankedPositions.size());

        // Zero the array first — ClusterMember.java:871-874
        for (int i = 0; i < length; i++) {
            rankedPositions[i] = 0;
        }

        // For each active member, insertion-sort its position into top-K.
        // ClusterMember.java:876-892
        for (const auto& member : members) {
            if (member.isActive(nowNs, timeoutNs)) {
                int64_t newPosition = member.logPosition;
                for (int i = 0; i < length; i++) {
                    const int64_t rankedPosition = rankedPositions[i];
                    if (newPosition > rankedPosition) {
                        rankedPositions[i] = newPosition;
                        newPosition = rankedPosition;
                    }
                }
            }
        }

        // ClusterMember.java:894  return rankedPositions[length - 1];
        return rankedPositions[length - 1];
    }

    // ClusterMember.java:828-845  hasActiveQuorum(ClusterMember[], long, long)
    // Returns true when active-member count >= quorumThreshold(N).
    static bool hasActiveQuorum(
        std::span<const ClusterMember> clusterMembers,
        int64_t                        nowNs,
        int64_t                        timeoutNs) noexcept
    {
        int threshold = quorumThreshold(static_cast<int>(clusterMembers.size()));
        for (const auto& member : clusterMembers) {
            if (member.isActive(nowNs, timeoutNs)) {
                if (--threshold <= 0) return true;
            }
        }
        return false;
    }

    // ClusterMember.java:907-925  hasQuorumAtPosition(ClusterMember[], long, long, long, long)
    // Counts members with isActive && leadershipTermId==given && logPosition>=position.
    // Returns true when count >= quorumThreshold.
    // Note: the Java signature also takes nowNs/timeoutNs for the isActive check inside
    // hasReachedPosition.  The spec deliverable omits those; we mirror the Java exactly.
    static bool hasQuorumAtPosition(
        std::span<const ClusterMember> clusterMembers,
        Term                           leadershipTermId,
        int64_t                        position,
        int64_t                        nowNs,
        int64_t                        timeoutNs) noexcept
    {
        int votes = 0;
        for (const auto& member : clusterMembers) {
            if (member.hasReachedPosition(leadershipTermId, position, nowNs, timeoutNs)) {
                ++votes;
            }
        }
        return votes >= quorumThreshold(static_cast<int>(clusterMembers.size()));
    }

    // ClusterMember.java:981-1002  isUnanimousLeader(ClusterMember[], long, int)
    // All non-excluded members must have candidateTermId==given AND vote==For.
    // Also requires that the accumulated vote count >= quorumThreshold (safety net).
    // gracefulClosedLeaderId: skip member with this id (pass kNullValue/-1 if none).
    static bool isUnanimousLeader(
        std::span<const ClusterMember> clusterMembers,
        Term                           candidateTermId,
        NodeId                         gracefulClosedLeaderId) noexcept
    {
        int votes = 0;
        for (const auto& member : clusterMembers) {
            if (member.id == gracefulClosedLeaderId) continue;     // ClusterMember.java:988-990
            if (candidateTermId != member.candidateTermId ||
                member.vote != Vote::For) {                         // ClusterMember.java:993
                return false;
            }
            votes++;
        }
        return votes >= quorumThreshold(static_cast<int>(clusterMembers.size())); // ClusterMember.java:1001
    }

    // ClusterMember.java:1015-1036  isQuorumLeader(ClusterMember[], long)
    // Majority of For votes, and no Against votes (for members with matching candidateTermId).
    static bool isQuorumLeader(
        std::span<const ClusterMember> clusterMembers,
        Term                           candidateTermId) noexcept
    {
        int votes = 0;
        for (const auto& member : clusterMembers) {
            if (candidateTermId == member.candidateTermId) {        // ClusterMember.java:1021
                if (member.vote == Vote::Against) return false;     // ClusterMember.java:1023-1025
                if (member.vote == Vote::For)     ++votes;          // ClusterMember.java:1027-1031
            }
        }
        return votes >= quorumThreshold(static_cast<int>(clusterMembers.size())); // ClusterMember.java:1035
    }

    // ClusterMember.java:1292-1298  willVoteFor(ClusterMember)
    // Returns true if this member's log is no more up-to-date than the candidate's
    // (i.e., compareLog(this, candidate) <= 0).
    // Also requires logPosition != NULL_POSITION (member has a valid log position).
    // ClusterMember.java:1297: return logPosition != NULL_POSITION && compareLog(this, candidate) <= 0;
    static bool willVoteFor(const ClusterMember& voter, const ClusterMember& candidate) noexcept {
        return voter.logPosition != kNullPosition &&
               compareLog(voter, candidate) <= 0;
    }
};

} // namespace rune::consensus
