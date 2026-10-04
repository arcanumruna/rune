#pragma once
// ---------------------------------------------------------------------------
// InternalIsrKnowledge.h — a node's DURABLE knowledge of the ISRs of the
// INTERNAL partition __cluster_metadata, and the consensus candidacy rule
// built on it.  (Re-review MEDIUM-B: __consumer_offsets / __transaction_state
// moved to the controller-ISR path — their ISRs no longer gate the consensus.)
//
// Review 2026-10-04 MAJOR-2 / MAJOR-3.  Internal partitions are led by the
// consensus leader, which the (Aeron) election picks without looking at the
// internal logs.  Kafka-consistent closure (the KIP-497/841 maximal-ISR rule +
// a Raft vote rule over the ISR — KIP-966's ELR idea):
//
//   * An internal ISR change is a VERSIONED state (leaderEpoch, version,
//     members) the internal leader publishes to every replica.  Each replica
//     PERSISTS the newest state it sees (this class — tmp + fsync + rename)
//     and then acks it.  The change COMMITS once a majority of the cluster
//     persisted it; until then the leader's HW counts committed ∪ proposed.
//   * A node votes for a consensus candidate only if the candidate is a member
//     of the newest __cluster_metadata ISR it persisted (eligible()).  Any node
//     may stand (re-review MAJOR-A); a state older than the node's consensus
//     term is never persisted or acked (MEDIUM-A).
//
// Safety: a committed change is on a majority; a winning candidate needs the
// votes of a majority; the two majorities intersect, so some voter knows the
// newest committed ISR (or a newer one) and refuses a non-member.  Every member
// of that ISR holds every committed record (HW = min over the maximal ISR), so
// the consensus winner — the new internal leader — never lacks committed
// internal records, and a stuck node can be shrunk out (its removal commits on
// the majority) without ever becoming electable again until re-admitted.
// Durable, so a whole-cluster restart keeps the rule.
//
// Single-writer: the CONTROL thread (internal replicas, the transport handlers
// and the Election all run there).  Cold: written only on ISR changes.
//
// fsync placement (re-review LOW-A): each persisted change costs two fsyncs
// (the file, then its directory) on the control/consensus duty-cycle thread.
// They stay there deliberately: the ack must follow the durable write
// (persist-then-ack), the store has one writer, and handing it to another
// thread would need a ring round-trip per change for no gain.  It is cold —
// only ISR changes of __cluster_metadata (deaths, rejoins, lag shrinks; the
// coordinator partitions moved to the controller path, MEDIUM-B), a few
// milliseconds each, orders of magnitude below the consensus heartbeat /
// election timeouts (hundreds of ms+) — and never on a produce/fetch path.
// ---------------------------------------------------------------------------

#include <rune/replication/Types.h>

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace rune::replication {

struct InternalIsrState {
    static constexpr int kMaxMembers = 16;
    int32_t topicId     = 0;
    int32_t partition   = 0;
    int64_t leaderEpoch = -1;
    int64_t version     = 0;
    int32_t count       = 0;
    std::array<int32_t, kMaxMembers> members{};

    [[nodiscard]] bool contains(int32_t node) const noexcept {
        for (int i = 0; i < count; ++i) if (members[static_cast<std::size_t>(i)] == node) return true;
        return false;
    }
    /// (leaderEpoch, version) strictly newer than (e, v).
    [[nodiscard]] bool newerThan(int64_t e, int64_t v) const noexcept {
        return leaderEpoch > e || (leaderEpoch == e && version > v);
    }
};

class InternalIsrKnowledge {
public:
    /// The ONE internal partition whose ISR decides consensus candidacy
    /// (re-review MEDIUM-B): __cluster_metadata, partition 0.
    static constexpr int32_t kCandidacyTopicId = -3;

    /// Memory only (unit rigs).
    InternalIsrKnowledge() = default;
    /// Durable: loads `path` if present.  An ABSENT file = no knowledge yet
    /// (every node eligible — the fresh-cluster state).  A file that exists
    /// but does not validate (corrupt, short, unknown version, unreadable)
    /// FAILS CLOSED (re-review LOW-A): votable() is false and eligible()
    /// denies every candidate — this node can no longer prove what it acked,
    /// so it refuses to vote — until it durably persists a fresh state (any
    /// successful offer(), i.e. a state from the current leadership: MEDIUM-A
    /// refuses older ones).  Chosen over refusing to start: equally safe (a
    /// non-voter can never help elect a non-member), while the node keeps
    /// serving its user partitions and self-heals from the leader with no
    /// operator action.
    explicit InternalIsrKnowledge(std::string path);

    InternalIsrKnowledge(const InternalIsrKnowledge&)            = delete;
    InternalIsrKnowledge& operator=(const InternalIsrKnowledge&) = delete;

    /// Adopt `s` if it is newer than what is known for its partition, durably
    /// (persisted before returning).  Returns true when this node now holds
    /// `s` or a newer state for that partition (the caller may ack it), false
    /// when the write failed (never ack an unpersisted state) or when
    /// s.leaderEpoch is older than this node's consensus term (MEDIUM-A: never
    /// persisted, never acked).
    bool offer(const InternalIsrState& s);

    /// The newest state known for (topicId, partition), nullptr = none.
    [[nodiscard]] const InternalIsrState* latest(int32_t topicId, int32_t partition) const noexcept;

    /// The consensus candidacy rule: `candidate` is a member of the newest
    /// known ISR of __cluster_metadata (true when nothing is known; false for
    /// everyone while the knowledge is unprovable — LOW-A).
    [[nodiscard]] bool eligible(int32_t candidate) const noexcept;

    /// Election vote-gate thunk (rune::consensus::Election::setVoteGate).
    static bool voteGate(void* self, int32_t candidate) noexcept {
        auto* k = static_cast<InternalIsrKnowledge*>(self);
        k->retryLoadIfPendingNoexcept_();
        return k->eligible(candidate);
    }

    /// The node's CONSENSUS term source (BrokerNode: the Election's
    /// max(candidateTermId, leadershipTermId) — the term it voted in or
    /// observed).  nullptr = no term rule (unit rigs).
    using TermSource = int64_t (*)(const void* ctx) noexcept;
    void setConsensusTermSource(TermSource fn, const void* ctx) noexcept { termFn_ = fn; termCtx_ = ctx; }
    [[nodiscard]] int64_t consensusTerm() const noexcept { return termFn_ ? termFn_(termCtx_) : -1; }

    /// Round-3 LOW-R3-1: a load that failed with a TRANSIENT error (EMFILE,
    /// ENFILE, EIO, EINTR, EAGAIN, ENOMEM, ENOBUFS — not corruption) leaves the
    /// store "load pending": it refuses to vote and to persist (persisting
    /// would overwrite the unread file) and retries the load on its next use
    /// (offer / adopt / the vote gate / the candidacy export) — no timer.
    void retryLoadIfPending();
    [[nodiscard]] bool loadPending() const noexcept { return loadPending_; }

    /// Round-3 CRITICAL-R3: adopt a PEER's persisted __cluster_metadata state
    /// learned in the election knowledge exchange — newest (leaderEpoch,
    /// version) wins, persisted durably, but NEVER acked and NOT term-gated
    /// (learning is not helping a leader commit: the dead leader's states are
    /// all older than the election term).  Never restores votable(): only a
    /// state received from the current leadership (offer) proves this node's
    /// own knowledge again.  Returns true when this node now holds it or newer.
    bool adopt(const InternalIsrState& s);
    /// The state this node shares in the exchange: its newest
    /// __cluster_metadata state; nullptr when none or unprovable (LOW-A).
    [[nodiscard]] const InternalIsrState* candidacyState() const noexcept {
        return (votable_ && !loadPending_) ? latest(kCandidacyTopicId, 0) : nullptr;
    }

    /// False after a failed validation of an existing file, until a fresh
    /// state was persisted (LOW-A).
    [[nodiscard]] bool votable() const noexcept { return votable_; }

    [[nodiscard]] std::size_t size() const noexcept { return entries_.size(); }
    [[nodiscard]] uint64_t persistsForTest() const noexcept { return persists_; }
    [[nodiscard]] uint64_t staleTermRefusals() const noexcept { return staleTermRefusals_; }

private:
    bool persist_();
    void load_();
    void failClosed_(const char* why);
    void loadTransient_(const char* what, int err);
    bool adoptNewest_(const InternalIsrState& s, bool provesOwnKnowledge);
    void retryLoadIfPendingNoexcept_() noexcept {
        try { retryLoadIfPending(); } catch (...) {}   // a failed retry stays pending
    }

    TermSource                    termFn_  = nullptr;
    const void*                   termCtx_ = nullptr;
    std::string                   path_;
    std::vector<InternalIsrState> entries_;
    uint64_t                      persists_ = 0;
    uint64_t                      staleTermRefusals_ = 0;
    bool                          votable_ = true;
    bool                          loadPending_ = false;   ///< LOW-R3-1: transient load failure
};

} // namespace rune::replication
