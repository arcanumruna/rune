#pragma once
// Election.h — Faithful C++ port of io.aeron.cluster.Election.
// P2P-5a: INIT/CANVASS/NOMINATE/CANDIDATE_BALLOT/FOLLOWER_BALLOT states.
// P2P-5b: LeaderLogReplication..Closed + onNewLeadershipTerm/onAppendPosition/onCommitPosition.
//
// Source references (cited above each handler in Election.cpp):
//   Election.java:71-108   (fields)
//   Election.java:181-260  (doWork dispatch)
//   Election.java:653-698  (init)
//   Election.java:700-729  (canvass)
//   Election.java:732-751  (nominate)
//   Election.java:754-793  (candidateBallot)
//   Election.java:796-807  (followerBallot)
//   Election.java:809-825  (leaderLogReplication)
//   Election.java:828-865  (leaderReplay)
//   Election.java:868-874  (leaderInit)
//   Election.java:877-894  (leaderReady)
//   Election.java:960-1017 (followerReplay)
//   Election.java:1115-1131 (followerLogInit)
//   Election.java:1134-1163 (followerLogAwait)
//   Election.java:1166-1179 (followerReady)
//   Election.java:284-332  (onCanvassPosition)
//   Election.java:334-380  (onRequestVote)
//   Election.java:383-409  (onVote)
//   Election.java:411-533  (onNewLeadershipTerm)
//   Election.java:535-554  (onAppendPosition)
//   Election.java:557-587  (onCommitPosition)
//   Election.java:1385-1438 (state transition)
//   Election.java:1182-1196 (placeVote)

#include <rune/consensus/ClusterMember.h>
#include <rune/consensus/ConsensusPublisher.h>
#include <rune/consensus/ElectionState.h>
#include <rune/consensus/NodeStateFile.h>
#include <rune/consensus/TermLog.h>
#include <rune/consensus/Types.h>

#include <atomic>
#include <cstdint>
#include <random>
#include <vector>

namespace rune::consensus {

// Election — runs the Aeron Cluster election state machine.
//
// Single-threaded: doWork() and all onXxx() handlers must be called from the
// same thread (ConsensusAgent duty-cycle loop). The stateCode_ atomic is written
// with release and may be read by observer threads.
class Election {
public:
    // Election.java:110-149  constructor
    // Parameters mirror the Java constructor arguments.
    //
    // @param isNodeStartup     true on first boot (uses startupCanvassTimeoutNs instead of
    //                          leaderHeartbeatTimeoutNs for the CANVASS deadline).
    // @param gracefulClosedLeaderId  NodeId of a leader that gracefully closed (-1 = none).
    // @param leadershipTermId  leadershipTermId initialised from last RecordingLog term entry.
    // @param logLeadershipTermId  term of the most-recently-appended log entry.
    // @param logPosition       committed/replayed byte position.
    // @param appendPosition    physically-appended byte position (may exceed logPosition).
    // @param clusterMembers    all cluster members including self.
    // @param thisMemberId      id of this node.
    // @param publisher         outbound message sink.
    // @param nodeStateFile     durable candidateTermId storage.
    // @param termLog           durable term-metadata storage.
    // @param electionTimeoutNs  max wait before declaring winner or reverting to CANVASS.
    // @param electionStatusIntervalNs  cadence for CanvassPosition/RequestVote broadcasts.
    // @param startupCanvassTimeoutNs   extended canvass window on startup.
    // @param rngSeed           seed for the nomination-backoff RNG (deterministic in tests).
    Election(bool             isNodeStartup,
             NodeId           gracefulClosedLeaderId,
             Term             leadershipTermId,
             Term             logLeadershipTermId,
             int64_t          logPosition,
             int64_t          appendPosition,
             std::vector<ClusterMember> clusterMembers,
             NodeId           thisMemberId,
             ConsensusPublisher& publisher,
             NodeStateFile&   nodeStateFile,
             TermLog&         termLog,
             int64_t          electionTimeoutNs,
             int64_t          electionStatusIntervalNs,
             int64_t          startupCanvassTimeoutNs,
             uint64_t         rngSeed = 42);

    // Not copyable or movable (holds references).
    Election(const Election&)            = delete;
    Election& operator=(const Election&) = delete;

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    // Election.java:181-260  doWork dispatch
    // Returns work count (>0 if state advanced or messages sent).
    int doWork(int64_t nowNs);

    // Accessors (for tests + ConsensusAgent)

    // Election.java:1385 state()
    ElectionState state() const noexcept;

    // Read the atomic state code (release-written on each transition).
    // Mirrors ctx.electionStateCounter().setRelease(newState.code()).
    int32_t stateCode() const noexcept;

    Term    leadershipTermId()         const noexcept;
    Term    candidateTermId()          const noexcept;
    Term    logLeadershipTermId()      const noexcept;
    int64_t logPosition()              const noexcept;
    int64_t appendPosition()           const noexcept;
    int64_t notifiedCommitPosition()   const noexcept;
    NodeId  thisMemberId()             const noexcept;
    NodeId  leaderMemberId()           const noexcept;

    // eligibleNodeIds — the set of cluster members currently considered LIVE,
    // for partition-failover reassignment (Task-14 / GR-7).
    //
    // A member is live if member.isActive(nowNs, timeoutNs) — i.e. its last
    // AppendPosition (Aeron's liveness heartbeat) was within `timeoutNs`.  The
    // leader itself never records an AppendPosition for its own entry, so when
    // this node is the leader (leaderMemberId() == thisMemberId()) its own id is
    // ALWAYS included (a leader is trivially live to itself).
    //
    // Result is sorted ascending, deduplicated.  Read-only; safe to call on the
    // duty-cycle thread (same thread that drives doWork()).
    //
    // timeoutNs <= 0 means "use the cluster's leaderHeartbeatTimeoutNs" — the
    // same window consensus itself uses to declare a peer dead.  Callers that
    // don't want to hard-code a liveness window should pass 0.
    std::vector<NodeId> eligibleNodeIds(int64_t nowNs, int64_t timeoutNs = 0) const;

    // Returns true when the election has reached the CLOSED terminal state.
    // Election.java:255
    bool isClosed() const noexcept;

    // Returns the next deadline Ns that doWork is waiting on in the current state.
    // Tests use this to advance the injected clock efficiently (no busy-spin).
    // States with no pending deadline return INT64_MIN (i.e. "already past").
    int64_t nextDeadlineNs() const noexcept;

    // -----------------------------------------------------------------------
    // Message handlers (called by ConsensusAdapter on the duty-cycle thread)
    // -----------------------------------------------------------------------

    // Election.java:284-332
    void onCanvassPosition(Term    logLeadershipTermId,
                           int64_t logPosition,
                           Term    leadershipTermId,
                           NodeId  followerMemberId);

    // Election.java:334-380
    // nowNs is passed so the state transition to FollowerBallot uses the real clock.
    void onRequestVote(Term    logLeadershipTermId,
                       int64_t logPosition,
                       Term    candidateTermId,
                       NodeId  candidateId,
                       int64_t nowNs);

    // Election.java:383-409
    void onVote(Term    logLeadershipTermId,
                int64_t logPosition,
                Term    candidateTermId,
                NodeId  candidateId,
                NodeId  voterId,
                bool    vote);

    // Election.java:411-533
    // Called when a NewLeadershipTerm message is received from a peer leader.
    // Drives the follower path: sets leaderMember/leadershipTermId_/notifiedCommitPosition
    // and transitions through FollowerReplay → FollowerLogInit → FollowerLogAwait → FollowerReady.
    void onNewLeadershipTerm(int64_t nowNs,
                             Term    logLeadershipTermId,
                             Term    nextLeadershipTermId,
                             int64_t nextTermBaseLogPosition,
                             int64_t nextLogPosition,
                             Term    leadershipTermId,
                             int64_t termBaseLogPosition,
                             int64_t logPosition,
                             int64_t leaderRecordingId,
                             int64_t timestamp,
                             NodeId  leaderMemberId,
                             int32_t logSessionId,
                             bool    isStartup,
                             int64_t commitPosition);

    // Election.java:535-554
    // Called when an AppendPosition message is received from a follower.
    // Updates that member's logPosition (and timeOfLastAppendPositionNs) so the
    // leader's quorumPosition check in LEADER_LOG_REPLICATION can advance.
    void onAppendPosition(int64_t nowNs,
                          Term    leadershipTermId,
                          int64_t logPosition,
                          NodeId  followerMemberId,
                          uint8_t flags);

    // Election.java:557-587
    // Called when a CommitPosition message is received from the leader.
    // Updates notifiedCommitPosition_ and extends any pending replication deadline.
    void onCommitPosition(int64_t nowNs,
                          Term    leadershipTermId,
                          int64_t logPosition,
                          NodeId  leaderMemberId);

private:
    // -----------------------------------------------------------------------
    // State handlers — one per ElectionState
    // -----------------------------------------------------------------------
    int init(int64_t nowNs);                  // Election.java:653-698
    int canvass(int64_t nowNs);               // Election.java:700-729
    int nominate(int64_t nowNs);              // Election.java:732-751
    int candidateBallot(int64_t nowNs);       // Election.java:754-793
    int followerBallot(int64_t nowNs);        // Election.java:796-807
    int leaderLogReplication(int64_t nowNs);  // Election.java:809-825
    int leaderReplay(int64_t nowNs);          // Election.java:828-865
    int leaderInit(int64_t nowNs);            // Election.java:868-874
    int leaderReady(int64_t nowNs);           // Election.java:877-894
    int followerReplay(int64_t nowNs);        // Election.java:960-1017
    int followerCatchupInit(int64_t nowNs);   // pass-through (Archive-dependent, Phase 3)
    int followerCatchupAwait(int64_t nowNs);  // pass-through
    int followerCatchup(int64_t nowNs);       // pass-through
    int followerLogInit(int64_t nowNs);       // Election.java:1115-1131
    int followerLogAwait(int64_t nowNs);      // Election.java:1134-1163 (pass-through, no Aeron image)
    int followerReady(int64_t nowNs);         // Election.java:1166-1179

    // Closed-state leader heartbeat: if this node is the leader, continue
    // broadcasting NLT on the heartbeat interval so late-joining nodes can
    // discover the existing leader and converge.  No-op for followers.
    int closedLeaderHeartbeat(int64_t nowNs);

    // -----------------------------------------------------------------------
    // Transition helper — Election.java:1385-1438
    // -----------------------------------------------------------------------
    void state(ElectionState next, int64_t nowNs, const char* reason);

    // Election.java:1200+  electionComplete(nowNs) — transitions to CLOSED
    void electionComplete(int64_t nowNs);

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    // Election.java:1446-1451  resetMembers()
    void resetMembers();

    // Election.java:1232-1251  publishCanvassPosition()
    void publishCanvassPosition();

    // Election.java:1182-1196  placeVote(long, int, boolean)
    void placeVote(Term candidateTermId, NodeId candidateId, bool vote);

    // Publish NewLeadershipTerm + CommitPosition to all cluster members (leader heartbeat).
    // Called by leaderLogReplication() and leaderReady() on interval.
    void publishNewLeadershipTermToAll(int64_t nowNs);

    // Election.java: inline utility
    // Returns true if nowNs >= timeOfLastUpdateNs + intervalNs.
    bool hasUpdateIntervalExpired(int64_t nowNs, int64_t intervalNs) const noexcept;

    // Find a ClusterMember* by id in clusterMembers_. Returns nullptr if not found.
    ClusterMember* memberById(NodeId id) noexcept;

    // -----------------------------------------------------------------------
    // Fields — mirrors Election.java:71-108
    // -----------------------------------------------------------------------

    // Election.java:71    private final ClusterMember[] clusterMembers;
    std::vector<ClusterMember>               clusterMembers_;
    // Election.java:72    private final ClusterMember thisMember;
    NodeId                                   thisMemberId_;

    // Election.java:74    private final ConsensusPublisher consensusPublisher;
    ConsensusPublisher&                      publisher_;

    // Persistence (replacing ctx.nodeStateFile() and ctx.recordingLog())
    NodeStateFile&                           nodeStateFile_;
    TermLog&                                 termLog_;

    // Timing fields — Election.java:81-84
    int64_t timeOfLastStateChangeNs_ = 0;
    int64_t timeOfLastUpdateNs_      = 0;   // reset to initialTimeOfLastUpdateNs_ on state change
    int64_t initialTimeOfLastUpdateNs_ = 0; // nowNs - 24h at construction (ensures immediate first publish)
    int64_t nominationDeadlineNs_    = 0;

    // Leader heartbeat interval (mirrors leaderHeartbeatIntervalNs in Java).
    // We reuse electionStatusIntervalNs_ for this (same parameter in tests).
    // Election.java:81  private final long leaderHeartbeatIntervalNs;

    // Follower leader-liveness tracking (mirrors ConsensusModuleAgent's
    // timeOfLastLeaderUpdateNs + leaderHeartbeatTimeoutNs in Aeron).
    //
    // In Aeron, the ConsensusModuleAgent — not the Election — detects a dead
    // leader: it records the time of the last heartbeat from the leader and, if
    // no heartbeat arrives within leaderHeartbeatTimeoutNs, it calls
    // election.startElection() to re-enter CANVASS.  We fold that liveness
    // detection into the Election here (the ConsensusModuleAgent is not ported).
    //
    // timeOfLastLeaderUpdateNs_ is refreshed whenever a follower accepts a
    // leader (electionComplete) or receives a heartbeat from the CURRENT leader
    // (onNewLeadershipTerm / onCommitPosition of the matching term).  A follower
    // in CLOSED whose last update is older than leaderHeartbeatTimeoutNs_
    // re-enters CANVASS so the surviving nodes elect a new leader.
    //
    // INT64_MIN sentinel = "no leader heartbeat observed yet" — the timeout is
    // only armed once a follower has actually accepted a leader, so a node that
    // has never had a leader (still canvassing) is not force-re-canvassed.
    int64_t timeOfLastLeaderUpdateNs_ = INT64_MIN;
    // Derived in the constructor from the election timing parameters.
    int64_t leaderHeartbeatTimeoutNs_ = 0;

    // Log position fields — Election.java:85-87
    int64_t logPosition_             = 0;
    int64_t appendPosition_          = 0;
    int64_t notifiedCommitPosition_  = 0;  // Election.java:88

    // Term fields — Election.java:94-96
    Term leadershipTermId_    = 0;
    Term logLeadershipTermId_ = 0;
    Term candidateTermId_     = 0;

    // Election config — Election.java:80
    bool isNodeStartup_     = false;
    bool isExtendedCanvass_ = false; // true during first canvass on startup
    bool isFirstInit_       = true;  // Election.java:101

    // Election.java:100
    NodeId gracefulClosedLeaderId_ = -1;

    // Election.java:104  private ClusterMember leaderMember = null;
    ClusterMember* leaderMember_ = nullptr;

    // Cached logSessionId from addLogPublication (Phase 2: always 0 — no Aeron log pub).
    int32_t logSessionId_ = 0;

    // Whether election entry was a startup (for NLT isStartup field).
    bool isLeaderStartup_ = false;

    // Phase-2 archive pass-through flag: no Aeron Archive means no catchup path.
    // FollowerLogInit immediately goes to FollowerReady (no image wait needed).

    // Election.java:105  private ElectionState state = INIT;
    ElectionState state_ = ElectionState::Init;

    // §6.9 AtomicCounter equivalent — release-written on each state transition.
    std::atomic<int32_t> stateCode_{0}; // Init.code() == 0

    // Timeout configuration (injected, not read from ctx)
    int64_t electionTimeoutNs_         = 0;
    int64_t electionStatusIntervalNs_  = 0;
    int64_t startupCanvassTimeoutNs_   = 0;

    // Pre-allocated ranked positions array for quorumPosition().
    // Size = quorumThreshold(N); allocated in constructor.
    std::vector<int64_t> rankedPositions_;

    // Seeded RNG for nominate backoff — Election.java:723
    std::mt19937_64 rng_;
};

} // namespace rune::consensus
