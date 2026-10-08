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
#include <rune/consensus/SbeConsensusPublisher.h>

#include <type_traits>
#include <utility>
#include <rune/consensus/ElectionState.h>
#include <rune/consensus/FollowerLiveness.h>
#include <rune/consensus/NodeStateFile.h>
#include <rune/consensus/TermLog.h>
#include <rune/consensus/Types.h>
#include <rune/consensus/AckTerm.h>
#include <rune/consensus/CandidacyState.h>

#include <algorithm>
#include <cstdint>
#include <random>
#include <vector>

namespace rune::consensus {

// Election — runs the Aeron Cluster election state machine.
//
// Single-threaded: doWork() and all onXxx() handlers must be called from the
// same thread (ConsensusAgent duty-cycle loop).  F9a: stateCode_ is a plain
// duty-cycle-owned field; observer threads learn the leader view from the
// LeaderObserver ring (rune::node::LeaderObserver), never by reading Election.
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

    // State code (written on each transition).  Duty-cycle thread only (F9a).
    // Mirrors ctx.electionStateCounter() — observers get it via a ring snapshot.
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
    // Live cluster members for partition-failover reassignment (a member is live if
    // it appended a position within timeoutNs; the leader always includes itself).
    // ALLOCATION-FREE: fills the caller-provided buffer out[0..cap) and returns the
    // count — no std::vector return, so the per-duty-cycle liveness poll never heap-
    // allocates.  Result is sorted ascending and de-duplicated.  timeoutNs <= 0 uses
    // the cluster's own leaderHeartbeatTimeoutNs.
    [[nodiscard]] std::size_t eligibleNodeIds(int64_t nowNs, int64_t timeoutNs,
                                              NodeId* out, std::size_t cap) const noexcept;

    // The consensus leader-liveness window (max(electionTimeout, 3*statusInterval)).
    // Consensus uses this short window to detect a dead leader quickly.  Partition
    // FAILOVER reassignment is a much heavier decision and must use a LONGER window
    // (a multiple of this) so transient post-election heartbeat jitter is not
    // mistaken for node death.  Exposed so NodeRuntimeAgent can derive that window.
    [[nodiscard]] int64_t leaderHeartbeatTimeoutNs() const noexcept {
        return leaderHeartbeatTimeoutNs_;
    }

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

    /// Controlled shutdown (Raft leadership transfer; Aeron's graceful leader
    /// close).  A CLOSED multi-node LEADER resigns: it sends StandDown
    /// (template 59) to every follower, stops heartbeating and re-enters
    /// CANVASS; it votes but never stands for election again in this process
    /// (resigned() latches).  Returns false (nothing done) unless this node is
    /// the CLOSED leader of a multi-node cluster.  Duty-cycle thread; cold.
    bool resign(int64_t nowNs);
    /// A follower learns its leader resigned: it starts an election AT ONCE
    /// (no leader-heartbeat timeout), skipping the resigned leader in the
    /// canvass (Aeron gracefulClosedLeaderId).  Ignored unless it comes from
    /// the leader this CLOSED follower follows, for its current term.
    void onStandDown(int64_t nowNs, Term leadershipTermId, NodeId leaderMemberId);
    [[nodiscard]] bool resigned() const noexcept { return resigned_; }
    /// Controlled shutdown, any role: never stand for election again in this
    /// process (it still votes).  A current leader keeps leading until
    /// resign().  Duty-cycle thread.
    void withdrawCandidacy() noexcept { resigned_ = true; }

    /// Review 2026-10-04 MAJOR-2/3 — candidacy rule.  `fn(ctx, candidate)`
    /// decides whether this node may VOTE for `candidate`.  The broker wires it
    /// to InternalIsrKnowledge::eligible: a node outside the newest durable ISR
    /// of __cluster_metadata (which the consensus leader leads) never gathers a
    /// majority — it would lead it without its committed records.  A denied
    /// RequestVote is answered like a "my log is newer" deny.  Applied by
    /// voters only: a node always may stand (re-review MAJOR-A — its own
    /// knowledge may be stale; the voters' majority decides).  nullptr
    /// (default) = everyone eligible (data-blind, as Aeron).  Duty-cycle thread.
    /// The consensus term that may refuse an internal-ISR ack (rune/consensus/
    /// AckTerm.h): the term this node GRANTED a vote in, or followed / led a
    /// leader in.  Duty-cycle thread.
    [[nodiscard]] Term ackTermId() const noexcept {
        return ackTerm(candidateTermId_, votedTermId_, leadershipTermId_);
    }
    [[nodiscard]] Term votedTermId() const noexcept { return votedTermId_; }
    /// D24: the durable follow floor (see followFloorTermId_).
    [[nodiscard]] Term followFloorTermId() const noexcept { return followFloorTermId_; }


    /// Re-review round 3 CRITICAL-R3 — knowledge exchange.  `src(ctx, out)`
    /// fills this node's newest durable metadata-ISR state (false = none /
    /// unprovable: nothing sent); `learn(ctx, s)` adopts a peer's state when
    /// newer (durably; never acked — LEARNING bypasses the ack term rule).  The
    /// state is sent to each peer with every CanvassPosition, RequestVote and
    /// Vote.  nullptr = no exchange.  Duty-cycle thread.
    using CandidacySource = bool (*)(void* ctx, CandidacyState& out) noexcept;
    using CandidacyLearn  = void (*)(void* ctx, const CandidacyState& s) noexcept;
    void setCandidacyExchange(CandidacySource src, CandidacyLearn learn, void* ctx) noexcept {
        candidacySrc_ = src; candidacyLearn_ = learn; candidacyCtx_ = ctx;
    }
    /// A peer's CandidacyState (ConsensusAgent dispatch).
    void onCandidacyState(NodeId fromMember, const CandidacyState& s) noexcept {
        (void)fromMember;
        if (candidacyLearn_ != nullptr) candidacyLearn_(candidacyCtx_, s);
    }

    using VoteGate = bool (*)(void* ctx, NodeId candidate) noexcept;
    void setVoteGate(VoteGate fn, void* ctx) noexcept { voteGate_ = fn; voteGateCtx_ = ctx; }
    [[nodiscard]] bool mayVoteFor(NodeId candidate) const noexcept {
        return voteGate_ == nullptr || voteGate_(voteGateCtx_, candidate);
    }

    /// Task 11 fix 2 (B2): true when every outbound consensus message is a direct
    /// call (production SbeConsensusPublisher over a final sink); false only for
    /// test doubles.  BrokerNode debug-asserts it.
    [[nodiscard]] bool publishPathIsConcrete() const noexcept {
        return publisher_.sbe != nullptr && publisher_.sbe->sinkIsConcrete();
    }

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
    // ClosedLeaderStepDownRegression (DR-10 flap): CLOSED → election exits.
    [[nodiscard]] bool hasActiveFollowerQuorum_(int64_t nowNs) const noexcept;
    void leaveClosedForElection_(Term term, int64_t nowNs, const char* reason);

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
    // Concrete publisher + construction-time tag (Task 11 fix 2, B2): the
    // production SbeConsensusPublisher (final) is called DIRECTLY; Generic
    // (virtual) = test doubles only.
    struct PublisherRef {
        SbeConsensusPublisher* sbe     = nullptr;
        ConsensusPublisher*    generic = nullptr;
        static PublisherRef of(ConsensusPublisher& p) noexcept {
            PublisherRef r;
            if (auto* s = dynamic_cast<SbeConsensusPublisher*>(&p)) r.sbe = s; else r.generic = &p;
            return r;
        }
#define RUNE_ELECTION_PUB_FWD(m)                                                      \
        template <class... A> void m(A&&... a) const {                                \
            if (sbe) [[likely]] sbe->m(std::forward<A>(a)...);                         \
            else generic->m(std::forward<A>(a)...);                                    \
        }
        RUNE_ELECTION_PUB_FWD(candidacyState)
        RUNE_ELECTION_PUB_FWD(canvassPosition)
        RUNE_ELECTION_PUB_FWD(requestVote)
        RUNE_ELECTION_PUB_FWD(placeVote)
        RUNE_ELECTION_PUB_FWD(newLeadershipTerm)
        RUNE_ELECTION_PUB_FWD(appendPosition)
        RUNE_ELECTION_PUB_FWD(commitPosition)
        RUNE_ELECTION_PUB_FWD(standDown)
#undef RUNE_ELECTION_PUB_FWD
    };
    static_assert(std::is_final_v<SbeConsensusPublisher>);
    PublisherRef                             publisher_;

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
    // Derived in the constructor from the election timing parameters.  Kept as a
    // field because the PUBLIC leaderHeartbeatTimeoutNs() accessor exposes it (the
    // failover eligible-set window is 3× this) and the ctor feeds it to
    // followerLiveness_.configure().
    int64_t leaderHeartbeatTimeoutNs_ = 0;

    // Steady-state (CLOSED) follower duties as ONE state machine that OWNS its
    // clocks — the periodic AppendPosition-to-leader send AND dead-leader
    // detection.  Replaces the former scattered timeOfLastLeaderUpdateNs_ /
    // timeOfLastAppendPositionSentNs_ fields.  See FollowerLiveness.h.
    FollowerLiveness followerLiveness_;

    // Log position fields — Election.java:85-87
    int64_t logPosition_             = 0;
    int64_t appendPosition_          = 0;
    int64_t notifiedCommitPosition_  = 0;  // Election.java:88

    // Term fields — Election.java:94-96
    /// In memory only, also when adopted from a higher-term NLT / CommitPosition
    /// on leaving CLOSED (DR-10 review LOW-6) — exactly as Aeron, whose
    /// ConsensusModuleAgent.leadershipTermId is recovered from the RecordingLog
    /// (here: the TermLog, appended at electionComplete) and whose only durable
    /// election term is the NodeStateFile candidate term.  Safe: the adoption
    /// first persists candidateTermId (proposeMaxCandidateTermId, fsync), and a
    /// restarted node's init() starts from max(persisted candidate term, this),
    /// so it never votes at or below the adopted term nor re-follows the deposed
    /// leader's older term (ClosedLeaderStepDownRegression_RestartAfter...).  The
    /// ack term may fall back below it after a crash — same reasoning as
    /// votedTermId_ below: knowledge persisted, never acked.
    Term leadershipTermId_    = 0;
    Term logLeadershipTermId_ = 0;
    Term candidateTermId_     = 0;
    /// Highest term this node GRANTED a vote in (MAJOR-R3) — a denial or an
    /// own (abandoned) candidacy never raises it.  After a restart it starts at
    /// the durable follow floor (D24; round-4 LOW-R4-4 closed): a grant before
    /// the crash still refuses an older term's internal ISR states.
    Term votedTermId_         = 0;
    /// D24: the durable FOLLOW FLOOR (NodeStateFile) — the highest term this
    /// node granted a vote in, led, followed, or adopted from a leader's
    /// NLT / CommitPosition.  It never follows a leader below it; it MAY follow
    /// one below candidateTermId_ when only its own failed candidacies (or
    /// denials) raised that — they helped no leader (DR-4 review MAJOR-1).
    Term followFloorTermId_   = -1;
    /// Raises the follow floor and the candidate term to `term`, durably (one sync).
    void raiseFollowFloor_(Term term);

    // Election config — Election.java:80
    bool isNodeStartup_     = false;
    bool isExtendedCanvass_ = false; // true during first canvass on startup
    bool isFirstInit_       = true;  // Election.java:101

    // Election.java:100 — scoped to ONE election (Aeron: a constructor argument
    // of that election); cleared at electionComplete (DR-10 review LOW-2).
    NodeId gracefulClosedLeaderId_ = -1;
    /// Controlled shutdown: this node resigned its leadership (resign()) — it
    /// never nominates itself again; while canvassing it re-sends StandDown
    /// for the term it resigned (loss recovery on the status interval).
    bool resigned_       = false;
    Term resignedTermId_ = -1;
    /// Entry time of this node's current leadership (LeaderLogReplication);
    /// INT64_MIN = never led.  The failover-liveness grace (eligibleNodeIds).
    int64_t leaderSinceNs_ = INT64_MIN;
    /// gracefulClosedLeaderId_ was set by a StandDown: the resigned leader's
    /// own canvass does not clear it until this election completes.
    bool skipFromStandDown_ = false;

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

    // §6.9 electionStateCounter equivalent — written on each state transition.
    // Duty-cycle-owned plain field (F9a: no atomics).
    int32_t stateCode_{0}; // Init.code() == 0

    // Timeout configuration (injected, not read from ctx)
    int64_t electionTimeoutNs_         = 0;
    int64_t electionStatusIntervalNs_  = 0;
    int64_t startupCanvassTimeoutNs_   = 0;

    // Pre-allocated ranked positions array for quorumPosition().
    // Size = quorumThreshold(N); allocated in constructor.
    std::vector<int64_t> rankedPositions_;

    // Seeded RNG for nominate backoff — Election.java:723
    std::mt19937_64 rng_;

    // Candidacy rule (setVoteGate).
    VoteGate voteGate_    = nullptr;
    void*    voteGateCtx_ = nullptr;
    // Knowledge exchange (setCandidacyExchange).
    CandidacySource candidacySrc_   = nullptr;
    CandidacyLearn  candidacyLearn_ = nullptr;
    void*           candidacyCtx_   = nullptr;
    void publishCandidacyState_(NodeId toMember);
    /// CRITICAL-R3: the candidate's own vote counts only if its own durable
    /// knowledge admits it (re-evaluated every ballot step).
    void gateSelfVote_() noexcept;
};

} // namespace rune::consensus
