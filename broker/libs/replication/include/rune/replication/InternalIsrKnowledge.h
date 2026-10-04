#pragma once
// ---------------------------------------------------------------------------
// InternalIsrKnowledge.h — a node's DURABLE knowledge of the ISRs of the
// INTERNAL partitions (__cluster_metadata, __consumer_offsets,
// __transaction_state, ...), and the consensus candidacy rule built on it.
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
//   * A node votes for a consensus candidate — and stands itself — only if the
//     candidate is a member of the newest ISR it persisted for EVERY internal
//     partition (eligible()).
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
    /// Memory only (unit rigs).
    InternalIsrKnowledge() = default;
    /// Durable: loads `path` if present (a missing / corrupt file = no
    /// knowledge yet, i.e. every node eligible — the fresh-cluster state).
    explicit InternalIsrKnowledge(std::string path);

    InternalIsrKnowledge(const InternalIsrKnowledge&)            = delete;
    InternalIsrKnowledge& operator=(const InternalIsrKnowledge&) = delete;

    /// Adopt `s` if it is newer than what is known for its partition, durably
    /// (persisted before returning).  Returns true when this node now holds
    /// `s` or a newer state for that partition (the caller may ack it), false
    /// when the write failed (never ack an unpersisted state).
    bool offer(const InternalIsrState& s);

    /// The newest state known for (topicId, partition), nullptr = none.
    [[nodiscard]] const InternalIsrState* latest(int32_t topicId, int32_t partition) const noexcept;

    /// The consensus candidacy rule: `candidate` is a member of the newest
    /// known ISR of every internal partition (true when nothing is known).
    [[nodiscard]] bool eligible(int32_t candidate) const noexcept;

    /// Election vote-gate thunk (rune::consensus::Election::setVoteGate).
    static bool voteGate(void* self, int32_t candidate) noexcept {
        return static_cast<const InternalIsrKnowledge*>(self)->eligible(candidate);
    }

    [[nodiscard]] std::size_t size() const noexcept { return entries_.size(); }
    [[nodiscard]] uint64_t persistsForTest() const noexcept { return persists_; }

private:
    bool persist_();
    void load_();

    std::string                   path_;
    std::vector<InternalIsrState> entries_;
    uint64_t                      persists_ = 0;
};

} // namespace rune::replication
