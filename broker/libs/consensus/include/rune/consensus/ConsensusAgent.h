#pragma once
// ConsensusAgent.h — Single-writer consensus duty-cycle agent.
//
// Faithful C++ port of the ConsensusModuleAgent.doWork() core (§4.2, §4.4).
// Owns the duty-cycle loop logic: drain ingress → dispatch to Election →
// return workCount.  AgentRunner calls doWork(nowNs) on a single thread.
//
// ConsensusAgent implements:
//   - ConsensusMessageHandler (onXxx handlers → forwarded to election_)
//   - Agent (doWork interface consumed by AgentRunner)
//
// Source references:
//   ConsensusModuleAgent.java:403-476  doWork
//   ConsensusModuleAgent.java:524-561  idleStrategy / idle
//   §4.2, §4.4 of the spec doc

#include <rune/consensus/ConsensusMessageHandler.h>
#include <rune/consensus/ConsensusAdapter.h>
#include <rune/consensus/Election.h>
#include <rune/consensus/IngressRing.h>
#include <rune/consensus/ElectionState.h>
#include <rune/consensus/Types.h>

#include <cstdint>

namespace rune::consensus {

// ---------------------------------------------------------------------------
// Agent — minimal interface consumed by AgentRunner.
// ---------------------------------------------------------------------------
struct Agent {
    virtual ~Agent() = default;
    /// Perform one duty-cycle.  Returns work count (>0 if any work was done).
    virtual int doWork(int64_t nowNs) = 0;
    /// Thread name for this agent's AgentRunner thread (for `ps -M`/`top -H`/
    /// profilers).  Keep short: macOS truncates the visible name and Linux
    /// caps pthread names at 15 chars + NUL.  Default identifies the base.
    virtual const char* name() const noexcept { return "rune-agent"; }
};

// ---------------------------------------------------------------------------
// ConsensusAgent
// ---------------------------------------------------------------------------
class ConsensusAgent final : public Agent, public ConsensusMessageHandler {
public:
    // ConsensusModuleAgent.java:403-476  (constructor parameters match the Java
    // constructor's relevant fields).
    //
    // @param election      The Election state machine (not owned; must outlive this).
    // @param ingressRing   MPSC ring from which ingress SBE frames are drained.
    //
    // The ConsensusAdapter is constructed internally, bound to *this as handler.
    explicit ConsensusAgent(Election&     election,
                            IngressRing&  ingressRing) noexcept;

    ConsensusAgent(const ConsensusAgent&)            = delete;
    ConsensusAgent& operator=(const ConsensusAgent&) = delete;

    // -----------------------------------------------------------------------
    // Agent::doWork — §4.2
    //
    // Single-writer, called from one thread only (AgentRunner).
    //
    // Duty cycle:
    //   a) drain up to FRAGMENT_LIMIT ingress frames from ingressRing_;
    //      each frame → adapter_.onFragment(nowNs, buf, len) →
    //        ConsensusAdapter dispatches to this->onXxx(nowNs, ...) →
    //        forwarded to election_.onXxx(...)
    //   b) workCount += election_.doWork(nowNs)
    //   c) return total workCount
    // -----------------------------------------------------------------------
    int doWork(int64_t nowNs) override;

    // -----------------------------------------------------------------------
    // ConsensusMessageHandler overrides — forward to election_
    // -----------------------------------------------------------------------

    void onCanvassPosition(
        int64_t nowNs,
        Term    logLeadershipTermId,
        int64_t logPosition,
        Term    leadershipTermId,
        NodeId  followerMemberId,
        int32_t protocolVersion) override;

    void onRequestVote(
        int64_t nowNs,
        Term    logLeadershipTermId,
        int64_t logPosition,
        Term    candidateTermId,
        NodeId  candidateMemberId,
        int32_t protocolVersion) override;

    void onVote(
        int64_t nowNs,
        Term    candidateTermId,
        Term    logLeadershipTermId,
        int64_t logPosition,
        NodeId  candidateMemberId,
        NodeId  followerMemberId,
        bool    vote) override;

    void onNewLeadershipTerm(
        int64_t nowNs,
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
        int32_t appVersion,
        bool    isStartup,
        int64_t commitPosition) override;

    void onAppendPosition(
        int64_t nowNs,
        Term    leadershipTermId,
        int64_t logPosition,
        NodeId  followerMemberId,
        uint8_t flags) override;

    void onCommitPosition(
        int64_t nowNs,
        Term    leadershipTermId,
        int64_t logPosition,
        NodeId  leaderMemberId) override;

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    Election& election() noexcept { return election_; }
    const Election& election() const noexcept { return election_; }

    ElectionState state() const noexcept { return election_.state(); }
    bool isClosed()       const noexcept { return election_.isClosed(); }

    // Total ingress messages processed since construction (for tests).
    // F9a: owner-thread state — read it on the duty-cycle thread, or after the
    // AgentRunner was joined (join = happens-before).  Never cross-thread live.
    int64_t messagesProcessed() const noexcept { return messagesProcessed_; }

private:
    // The stored nowNs during a doWork() — used by onXxx handlers
    // (called synchronously from drain()) to pass time to election_.
    int64_t nowNsForHandlers_{0};

    Election&         election_;
    IngressRing&      ingressRing_;
    ConsensusAdapter  adapter_;   // dispatches decoded frames back to *this

    // Count of ingress messages dispatched (for integration-test assertions).
    // Duty-cycle-thread-owned plain counter (F9a: no atomics).
    int64_t messagesProcessed_{0};
};

} // namespace rune::consensus
