// StaticBalancer.cpp — Pure, deterministic, co-location-aware static balancer.
//
// Algorithm overview
// ──────────────────
// 1. Validate all topics (RF ≤ nodes, co-partition group consistency).
// 2. Group topics by coPartitionGroup; topics without a group are singletons.
// 3. For each group (or singleton), compute the leader assignment for every
//    partition index using striped round-robin over the sorted node list.
//    The stripe offset for each group is drawn from a global counter so that
//    different groups start at different nodes — spreading leadership globally.
// 4. For each (topicId, partition), pick RF replicas starting at the same
//    leader position and stepping through the node ring, so the leader is
//    always the first replica (and thus always ∈ ISR).
// 5. Build BalancerRecord list: set rec.isr / rec.replicas views from the
//    owned vectors, set both epochs to 0.
//
// Determinism: input topics are processed in their given order (callers are
// expected to pass them in a stable order; the tests do so).  The node ring is
// built by sorting liveNodes ascending before any computation.

#include "rune/metadata/StaticBalancer.h"

#include <algorithm>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

namespace rune::metadata {

namespace {

// ---------------------------------------------------------------------------
// Validate one co-partition group (all members share the same partitionCount
// and the same partitionKey).
// ---------------------------------------------------------------------------
void validateGroup(const std::vector<const TopicSpec*>& members)
{
    if (members.size() <= 1) return;

    const int32_t     expectedCount = members[0]->partitionCount;
    const std::string expectedKey   = members[0]->partitionKey;

    for (std::size_t i = 1; i < members.size(); ++i) {
        if (members[i]->partitionCount != expectedCount) {
            throw BalancerError(
                "co-partition group '" + members[0]->coPartitionGroup +
                "': partition count mismatch (" +
                std::to_string(expectedCount) + " vs " +
                std::to_string(members[i]->partitionCount) + ")");
        }
        if (members[i]->partitionKey != expectedKey) {
            throw BalancerError(
                "co-partition group '" + members[0]->coPartitionGroup +
                "': partition key mismatch ('" + expectedKey + "' vs '" +
                members[i]->partitionKey + "')");
        }
    }
}

// ---------------------------------------------------------------------------
// Build an Int32ArrayView that points into a std::vector<int32_t>.
// The vector must be kept alive for the lifetime of the view.
// ---------------------------------------------------------------------------
rune::protocol::metadata::Int32ArrayView makeView(const std::vector<int32_t>& v)
{
    return rune::protocol::metadata::Int32ArrayView(
        v.data(),
        static_cast<uint16_t>(v.size()));
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// StaticBalancer::assign — main entry point
// ---------------------------------------------------------------------------
std::vector<BalancerRecord> StaticBalancer::assign(
    const std::vector<TopicSpec>& topics,
    const std::vector<NodeId>&    liveNodes)
{
    // ── 0. Trivial case ────────────────────────────────────────────────────
    if (topics.empty()) return {};

    const auto nodeCount = static_cast<int32_t>(liveNodes.size());

    // ── 1. Validate RF for every topic ────────────────────────────────────
    for (const auto& t : topics) {
        if (static_cast<int32_t>(t.rf) > nodeCount) {
            throw BalancerError(
                "topic " + std::to_string(t.topicId) +
                ": RF (" + std::to_string(t.rf) +
                ") > liveNodes.size() (" + std::to_string(nodeCount) + ")");
        }
    }

    // ── 2. Build sorted node ring (determinism: sort once, use everywhere) ─
    std::vector<NodeId> ring = liveNodes;
    std::sort(ring.begin(), ring.end());

    // ── 3. Group topics by coPartitionGroup ────────────────────────────────
    //       Preserve input topic order within each group.
    //       Use a std::map so group processing order is deterministic (sorted
    //       by group name).  Singletons (empty group) each get a unique
    //       synthetic key so they don't collide.
    //
    //       We process named groups first (in sorted order), then singletons
    //       in their original topic-input order.  This is purely for clarity;
    //       all group offsets are derived from an incrementing global counter
    //       so the actual iteration order affects only partition→node mapping,
    //       not correctness of co-location or balance invariants.

    // Collect named groups in sorted order.
    std::map<std::string, std::vector<const TopicSpec*>> namedGroups;
    std::vector<const TopicSpec*> singletons;

    for (const auto& t : topics) {
        if (!t.coPartitionGroup.empty())
            namedGroups[t.coPartitionGroup].push_back(&t);
        else
            singletons.push_back(&t);
    }

    // Validate every named group.
    for (const auto& [name, members] : namedGroups)
        validateGroup(members);

    // ── 4. Assign partitions ───────────────────────────────────────────────
    //
    // Global leader-counter: tracks the total number of leadership slots
    // already assigned.  Striped round-robin: partition p of the k-th group
    // that starts at offset `offset` gets ring[(offset + p) % nodeCount].
    //
    // For co-located groups all member topics get the SAME leader per index p,
    // seeded by the same offset.  This is the deliberate Kafka-extension:
    // within a group, index-aligned partitions are placed together.
    //
    // Balance: each topic independently contributes `partitionCount` leadership
    // assignments.  Across all topics the striped pattern distributes load.

    // We pre-compute how many leadership assignments happen before each
    // processing step so the stripe offset respects the running counter.
    // The running stripe position is simply: next_stripe = total_assigned_so_far.

    std::vector<BalancerRecord> result;
    result.reserve(
        [&]() {
            std::size_t total = 0;
            for (const auto& t : topics) total += static_cast<std::size_t>(t.partitionCount);
            return total;
        }());

    int32_t globalLeaderOffset = 0; // striping seed for the next group

    // Helper: emit records for a collection of topics with the same partition
    // count and a shared stripe offset (co-location: all use the same offset).
    auto emitGroup = [&](const std::vector<const TopicSpec*>& members,
                         int32_t                              stripeOffset)
    {
        const int32_t partCount = members[0]->partitionCount;

        for (int32_t p = 0; p < partCount; ++p) {
            // Leader for this partition index: same for all members in group.
            const int32_t leaderIdx = (stripeOffset + p) % nodeCount;
            const NodeId  leader    = ring[static_cast<std::size_t>(leaderIdx)];

            for (const TopicSpec* spec : members) {
                const int32_t rf = static_cast<int32_t>(spec->rf);

                // Build replica list: leader first, then rf-1 successors
                // around the ring.  No duplicates (rf ≤ nodeCount guaranteed).
                std::vector<int32_t> replicas;
                replicas.reserve(static_cast<std::size_t>(rf));
                for (int32_t r = 0; r < rf; ++r) {
                    const int32_t idx = (leaderIdx + r) % nodeCount;
                    replicas.push_back(ring[static_cast<std::size_t>(idx)]);
                }

                // ISR = same as replicas on initial assignment.
                std::vector<int32_t> isr = replicas;

                BalancerRecord br;
                br.isr      = std::move(isr);
                br.replicas = std::move(replicas);

                auto& rec        = br.rec;
                rec.topicId       = spec->topicId;
                rec.partition     = p;
                rec.leaderNode    = leader;
                rec.leaderEpoch   = 0;
                rec.partitionEpoch = 0;
                rec.isr      = makeView(br.isr);
                rec.replicas = makeView(br.replicas);

                result.push_back(std::move(br));
            }
        }
    };

    // Process named groups in sorted-name order (deterministic).
    for (const auto& [name, members] : namedGroups) {
        // The stripe offset for this group is the current global counter.
        const int32_t stripeOffset = globalLeaderOffset % nodeCount;
        emitGroup(members, stripeOffset);
        // Advance global counter by the number of partition leadership slots
        // emitted for ALL members of the group.  Since all members share the
        // same leader per index, only the per-index leadership counts once for
        // the global balance counter.
        globalLeaderOffset += members[0]->partitionCount;
    }

    // Process singletons in input order.
    for (const TopicSpec* spec : singletons) {
        const int32_t stripeOffset = globalLeaderOffset % nodeCount;
        emitGroup({spec}, stripeOffset);
        globalLeaderOffset += spec->partitionCount;
    }

    return result;
}

} // namespace rune::metadata
