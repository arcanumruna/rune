#pragma once
// ReplicaRegistry.h — Per-partition PartitionReplica registry (GR-3).
//
// Maps (topicId, partition) → PartitionReplica, paralleling TopicRegistry.
// Owns the PartitionReplica instances; the TopicRegistry owns the Log instances.
// Both share the same non-owning Log* pointer.
//
// Threading: SINGLE-WRITER — all access must be on the AgentRunner duty-cycle thread.
// Constructed in BrokerNode::start() after TopicRegistry (so Log* are valid).

#include <rune/replication/PartitionReplica.h>
#include <rune/replication/InternalIsrKnowledge.h>
#include <rune/replication/Types.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>
#include <unordered_map>
#include <vector>

namespace rune::node {

class ReplicaRegistry {
public:
    ReplicaRegistry() = default;

    // Non-copyable, non-movable (owns PartitionReplica instances).
    ReplicaRegistry(const ReplicaRegistry&)            = delete;
    ReplicaRegistry& operator=(const ReplicaRegistry&) = delete;

    // -----------------------------------------------------------------------
    // add — register a pre-constructed PartitionReplica.
    // -----------------------------------------------------------------------
    /// KIP-903 (review 2026-10-04 MEDIUM-4): this node's PROCESS incarnation
    /// (monotonic per node, bumped on every start — BrokerNode), stamped on
    /// every replica added from now on.  Set before the first add().
    void setProcessIncarnation(int64_t inc) noexcept { processIncarnation_ = inc; }
    /// MAJOR-2/3: the node's durable internal-ISR knowledge, wired into the
    /// __cluster_metadata replica (MEDIUM-B: only it) added from now on.
    /// Non-owning.
    void setInternalIsrKnowledge(rune::replication::InternalIsrKnowledge* k) noexcept { internalIsrKnowledge_ = k; }
    [[nodiscard]] int64_t processIncarnation() const noexcept { return processIncarnation_; }

    void add(int32_t topicId, int32_t partition,
             std::unique_ptr<rune::replication::PartitionReplica> replica)
    {
        const Key key{topicId, partition};
        if (map_.count(key)) remove(topicId, partition);   // never destroy a live, owned replica in place
        // Task 10 fix 3: a dense id (reused only after the previous holder is
        // freed) + an incarnation.  Workers guard every op by (id, incarnation):
        // pointer reuse can never pass as ownership.
        uint32_t id;
        if (!freeIds_.empty()) { id = freeIds_.back(); freeIds_.pop_back(); }
        else id = nextId_++;
        const uint32_t inc = allocIncarnation_();
        replica->setRegistryHandle(id, inc);
        // KIP-903: every replica of this process stamps the node's process
        // incarnation on its FetchRequests (0 = untracked: unit rigs).
        if (processIncarnation_ != 0) replica->setProcessIncarnation(processIncarnation_);
        // MAJOR-2/3: internal replicas commit ISR changes on a majority's
        // durable knowledge (this node's store).
        // MEDIUM-B: only __cluster_metadata (the coordinator partitions are on
        // the controller-ISR path; __topic_schemas is not in this registry).
        if (topicId == rune::replication::InternalIsrKnowledge::kCandidacyTopicId &&
            internalIsrKnowledge_ != nullptr)
            replica->setInternalIsrKnowledge(internalIsrKnowledge_);
        auto* raw = replica.get();
        map_[key] = Entry{std::move(replica), id, inc};
        ordered_.push_back(raw);
        ++generation_;
    }

    // -----------------------------------------------------------------------
    // Task 10 round 4 — identity handles.  Control-thread holders (transport,
    // gateway pending acks, bridge pending role ops, op routing) refer to a
    // replica by (registryId, incarnation) and resolve it HERE at use time.
    // The identity is stored in the map entry itself: resolving it never reads
    // through a replica pointer.  A miss or an incarnation mismatch = stale.
    // -----------------------------------------------------------------------
    // F15 §3.1: the role the control plane last ROUTED for this replica
    // incarnation (the bridge records it when it routes a role op).  A HINT for
    // the gateway's fast NotLeader only: it may lead or lag the owning worker,
    // which stays the sole authority (role gate on W).  Unknown = route.
    enum class IntendedRole : uint8_t { Unknown = 0, Leader = 1, Follower = 2 };

    struct Handle {
        rune::replication::PartitionReplica* replica     = nullptr;
        uint32_t                             id          = 0;
        uint32_t                             incarnation = 0;
        IntendedRole                         intendedRole = IntendedRole::Unknown;
        explicit operator bool() const noexcept { return replica != nullptr; }
    };
    /// Never assigned to a live replica: marks an op whose target is gone.
    static constexpr uint32_t kStaleIncarnation = 0xFFFFFFFFu;

    [[nodiscard]] Handle lookupHandle(int32_t topicId, int32_t partition) const noexcept {
        auto it = map_.find(Key{topicId, partition});
        if (it == map_.end()) return {};
        return Handle{it->second.replica.get(), it->second.id, it->second.inc, it->second.intended};
    }
    /// F15 §3.1: record the role just routed for (topicId, partition) — only if
    /// the entry is still `incarnation` (a stale routing never tags a new one).
    /// Control thread only (the registry is single-writer).
    void setIntendedRole(int32_t topicId, int32_t partition, uint32_t incarnation,
                         IntendedRole role) noexcept {
        auto it = map_.find(Key{topicId, partition});
        if (it != map_.end() && it->second.inc == incarnation && it->second.intended != role) {
            it->second.intended = role;
            ++roleChanges_;
        }
    }
    /// D9 re-review 2 MEDIUM-2: bumped on every routed role change and every
    /// removal — the gateway prunes per-partition state on a change (no timer).
    [[nodiscard]] uint64_t roleChanges() const noexcept { return roleChanges_; }
    /// The live replica for (topicId, partition) iff it is still `incarnation`.
    [[nodiscard]] rune::replication::PartitionReplica*
    resolve(int32_t topicId, int32_t partition, uint32_t incarnation) const noexcept {
        auto it = map_.find(Key{topicId, partition});
        if (it == map_.end() || it->second.inc != incarnation) return nullptr;
        return it->second.replica.get();
    }

    /// Round 5: an external holder of incarnations that outlive the registry
    /// entry (WorkerPool's F7 owner map keeps removed incarnations until every
    /// worker acked) — consulted after a wrap so a reused value never collides.
    /// Plain function pointer + context (cold path, control thread only).
    using IncarnationInUseFn = bool (*)(const void* ctx, uint32_t incarnation);
    void setIncarnationInUseHook(IncarnationInUseFn fn, const void* ctx) noexcept {
        inUseFn_ = fn; inUseCtx_ = ctx;
    }

    /// Test seam (M4): start incarnation allocation at `next` (exercise the wrap).
    void setNextIncarnationForTest(uint32_t next) noexcept { nextIncarnation_ = next; }

    /// One past the largest dense id handed out (workers size their tables by it).
    [[nodiscard]] uint32_t idCapacity() const noexcept { return nextId_; }

    // -----------------------------------------------------------------------
    // lookup — non-owning pointer; nullptr if not found.
    // -----------------------------------------------------------------------
    [[nodiscard]] rune::replication::PartitionReplica*
    lookup(int32_t topicId, int32_t partition) const noexcept
    {
        auto it = map_.find(Key{topicId, partition});
        return (it != map_.end()) ? it->second.replica.get() : nullptr;
    }

    // -----------------------------------------------------------------------
    // lookupByPartitionId — for AeronPartitionTransport routing.
    // For the DR scope (single-topic cluster), partitionId alone is sufficient.
    // Scans in insertion order; O(n) but n is small (partitions per node).
    // -----------------------------------------------------------------------
    [[nodiscard]] rune::replication::PartitionReplica*
    lookupByPartitionId(rune::replication::PartitionId pid) const noexcept
    {
        for (auto* r : ordered_) {
            if (r && r->partitionId() == pid) return r;
        }
        return nullptr;
    }

    // -----------------------------------------------------------------------
    // allReplicas — ordered view for duty-cycle doWork iteration.
    // -----------------------------------------------------------------------
    [[nodiscard]] const std::vector<rune::replication::PartitionReplica*>&
    allReplicas() const noexcept { return ordered_; }

    // -----------------------------------------------------------------------
    // remove — quarantine (erase) the replica keyed by (topicId, partition).
    //
    // Called by ReplicationMetadataBridge::applyDeleteForKey when the committed
    // assignment map no longer includes this node for the partition (Note B /
    // spec §16 stray-replica quarantine).  After removal, lookup() returns
    // nullptr for that key so the deleted partition can never be routed to again.
    //
    // The replica is RETIRED here, not destroyed: it is freed by
    // collectRetired() once every worker acked the SetOwnership that drops it.
    // The underlying Log and disk data are NOT touched (owned by the
    // TopicRegistry / the caller's layer).  Its dense id is reused only after it
    // is freed; its incarnation never names another live replica, so any holder
    // that refers to it by (topic, partition, incarnation) resolves to nothing.
    //
    // Complexity: O(n) for the ordered_ erase (n = total registered replicas;
    // typically small).  Hot path is add/lookup; remove is uncommon (assignment
    // change).
    // -----------------------------------------------------------------------
    void remove(int32_t topicId, int32_t partition)
    {
        const Key key{topicId, partition};
        auto it = map_.find(key);
        if (it == map_.end()) return;  // already absent — idempotent

        // Remove the raw pointer from ordered_ before destroying the unique_ptr
        // so ordered_ never holds a dangling pointer even transiently.
        auto* raw = it->second.replica.get();
        auto  oit = std::find(ordered_.begin(), ordered_.end(), raw);
        if (oit != ordered_.end()) ordered_.erase(oit);

        // Task 10 fix 3: do NOT destroy yet — a worker may still own it until it
        // applies the SetOwnership for this generation.  Retired here, freed by
        // collectRetired() once every worker acked an ownership gen >= this one.
        ++generation_;
        ++roleChanges_;
        retired_.push_back(Retired{generation_, std::move(it->second.replica), it->second.id,
                                   it->second.inc});
        map_.erase(it);
    }

    /// Free retired replicas no worker can still reference: those removed at a
    /// generation <= safeGen (the minimum OWNERSHIP_ACK across workers; the
    /// current generation when no worker pool exists).  Control thread only.
    std::size_t collectRetired(std::uint64_t safeGen) {
        std::size_t freed = 0;
        for (std::size_t i = 0; i < retired_.size(); ) {
            if (retired_[i].gen <= safeGen) {
                freeIds_.push_back(retired_[i].id);
                retired_[i] = std::move(retired_.back());
                retired_.pop_back();
                ++freed;
            } else {
                ++i;
            }
        }
        return freed;
    }
    [[nodiscard]] std::size_t retiredCountForTest() const noexcept { return retired_.size(); }

    // -----------------------------------------------------------------------
    // replicaCount
    // -----------------------------------------------------------------------
    [[nodiscard]] std::size_t replicaCount() const noexcept { return ordered_.size(); }

    // -----------------------------------------------------------------------
    // generation — monotonic counter incremented on every real mutation
    // (add or successful remove).  WorkerPool::reconcile() compares this
    // instead of replicaCount() so that a same-count add()+remove() pair
    // (net-zero count change) is still detected as a change — preventing a
    // latent use-after-free once E2 introduces runtime add().
    // -----------------------------------------------------------------------
    [[nodiscard]] std::uint64_t generation() const noexcept { return generation_; }

private:
    struct Key {
        int32_t topicId;
        int32_t partition;
        bool operator==(const Key& o) const noexcept {
            return topicId == o.topicId && partition == o.partition;
        }
    };
    struct KeyHash {
        std::size_t operator()(const Key& k) const noexcept {
            std::size_t h = static_cast<std::size_t>(k.topicId);
            h ^= (static_cast<std::size_t>(k.partition) + 0x9e3779b9u + (h << 6) + (h >> 2));
            return h;
        }
    };

    struct Entry {
        std::unique_ptr<rune::replication::PartitionReplica> replica;
        uint32_t id  = 0;
        uint32_t inc = 0;
        IntendedRole intended = IntendedRole::Unknown;   ///< F15 hint (per incarnation)
    };
    // M4: incarnations are 32-bit on the ring.  On wrap, 0 (unregistered /
    // not gated) and kStaleIncarnation are skipped, and — once wrapped — any
    // value still held by a live or retired replica is skipped too (cold: adds
    // are control-plane events), so an incarnation is never live twice.
    uint32_t allocIncarnation_() noexcept {
        for (;;) {
            const uint32_t v = nextIncarnation_++;
            if (nextIncarnation_ == 0) wrapped_ = true;
            if (v == 0 || v == kStaleIncarnation) { wrapped_ = true; continue; }
            if (wrapped_ && incarnationInUse_(v)) continue;
            return v;
        }
    }
    [[nodiscard]] bool incarnationInUse_(uint32_t v) const noexcept {
        for (const auto& [k, e] : map_) { (void)k; if (e.inc == v) return true; }
        for (const auto& r : retired_) if (r.inc == v) return true;
        if (inUseFn_ && inUseFn_(inUseCtx_, v)) return true;   // e.g. WorkerPool pending erase
        return false;
    }

    std::unordered_map<Key, Entry, KeyHash> map_;
    std::vector<rune::replication::PartitionReplica*> ordered_;  // stable insertion order
    std::uint64_t generation_ = 0;
    struct Retired {
        std::uint64_t gen;
        std::unique_ptr<rune::replication::PartitionReplica> replica;
        uint32_t id  = 0;
        uint32_t inc = 0;
    };
    std::vector<Retired>  retired_;
    std::vector<uint32_t> freeIds_;
    int64_t processIncarnation_ = 0;   ///< KIP-903 (see setProcessIncarnation)
    rune::replication::InternalIsrKnowledge* internalIsrKnowledge_ = nullptr;   ///< MAJOR-2/3
    uint32_t              nextId_          = 1;   // 0 = unregistered (not gated)
    uint32_t              nextIncarnation_ = 1;
    bool                  wrapped_         = false;
    IncarnationInUseFn    inUseFn_         = nullptr;
    const void*           inUseCtx_        = nullptr;
    uint64_t roleChanges_ = 0;   ///< D9: role-change / removal counter (control thread)
};

} // namespace rune::node
