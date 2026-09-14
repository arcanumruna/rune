#pragma once
// ---------------------------------------------------------------------------
// PartitionReplica.h — Single-writer partition replica: leader and follower
//                      roles for the Rune data-plane replication layer.
//
// Namespace: rune::replication
// C++23. NO locks. NO busy-spin. Single thread owns all state.
//
// ## Overview (spec §4, plan P3-3)
//
// PartitionReplica ties together:
//   - Phase-1 rune::storage::Log  (persistent in-order byte store)
//   - Phase-1 rune::storage::LeaderEpochCache  (epoch→startOffset truncation)
//   - P3-1  ReplicaLeaderState  (HW + per-follower LEO)
//   - P3-2  IsrManager          (ISR shrink/expand, produce gate)
//   - P3-2  EpochFencing        (stale-epoch rejection, truncation offset)
//   - P3-1  ReplicationMessages (FetchRequest/FetchResponse SBE codecs)
//
// ## Role model
//
//   Role::Leader  — appends client batches, serves follower fetches, advances HW.
//   Role::Follower — fetches from leader, truncates on epoch change, applies batches.
//
// Both roles are driven by a single `doWork(int64_t nowNs)` call that MUST be
// called by one owning thread.  doWork() samples nowNs once and runs all
// periodic work (ISR checks, fetch emission) without spinning or sleeping.
//
// ## Transport injection
//
// PartitionTransport is a pure abstract interface so tests can plug in a
// deterministic fake.  Production code will implement it over Aeron; tests use
// InMemoryTransport below.
//
// ## No hot-path allocation
//
// All scratch buffers are pre-allocated in the constructor.  Reusable SBE
// codec instances are fields.  Record views (std::span) point into caller or
// log buffers.
// ---------------------------------------------------------------------------

#include <cstdint>
#include <functional>
#include <mutex>
#include <span>
#include <vector>
#include <string>

#include <rune/replication/Types.h>
#include <rune/replication/HighWatermark.h>
#include <rune/replication/IsrManager.h>
#include <rune/replication/EpochFencing.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/storage/Log.h>
#include <rune/storage/LeaderEpochCache.h>

namespace rune::replication {

// ---------------------------------------------------------------------------
// Role
// ---------------------------------------------------------------------------
enum class Role { Leader, Follower };

// ---------------------------------------------------------------------------
// Error codes (returned in FetchResponseMsg::errorCode)
// ---------------------------------------------------------------------------
inline constexpr int16_t kErrNone         =  0;
inline constexpr int16_t kErrStaleEpoch   = -1;   // request epoch < local epoch
inline constexpr int16_t kErrNotLeader    = -2;   // this node is not the leader
inline constexpr int16_t kErrMinIsr       = -3;   // ISR too small to produce
inline constexpr int16_t kErrOutOfRange   = -4;   // fetchOffset out of range

// ---------------------------------------------------------------------------
// PartitionConfig — constructor argument bundle
// ---------------------------------------------------------------------------
struct PartitionConfig {
    PartitionId partitionId       = 0;
    NodeId      selfId            = 0;
    int64_t     replicaLagMaxMs   = 10'000;         ///< ms before follower shrunk from ISR
    Offset      replicaLagMaxOffset = 10'000;       ///< offset lag before ISR shrink
    int         minIsr            = 1;              ///< min ISR for acksAll produce
    int64_t     fetchIntervalNs   = 50'000'000LL;   ///< follower fetch cadence (50 ms backstop; push is steady-state)
    int64_t     isrCheckIntervalNs = 10'000'000LL;  ///< leader ISR check cadence (10 ms)
    int32_t     maxFetchBytes     = 1 << 20;        ///< max bytes per fetch (1 MiB)
    bool        pushOnAppend      = true;            ///< GR-10: leader pushes to ISR followers immediately on append
    bool        fastAckOnApply    = true;            ///< GR-10: follower fast-acks (emit FetchRequest immediately after applyReplicated)
};

// ---------------------------------------------------------------------------
// PartitionTransport — injected interface (no Aeron in this file)
//
// Leader uses sendFetchResponse(); follower uses sendFetchRequest().
// The fake pumps responses back into the leader via its onFetchRequest() and
// pumps requests back into the follower via its applyReplicated().
// ---------------------------------------------------------------------------
class PartitionTransport {
public:
    virtual ~PartitionTransport() = default;

    /// (Follower → Leader) send a FetchRequest.
    virtual void sendFetchRequest(const FetchRequestMsg& req) = 0;

    /// (Leader → Follower) send a FetchResponse.
    virtual void sendFetchResponse(NodeId toFollower,
                                   const FetchResponseMsg& resp) = 0;
};

// ---------------------------------------------------------------------------
// PartitionReplica
// ---------------------------------------------------------------------------
class PartitionReplica {
public:
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// \param cfg        Partition configuration.
    /// \param role       Initial role (Leader or Follower).
    /// \param epoch      Current leader epoch (stamped on every batch).
    /// \param replicas   Full replica set (NodeIds, including self).
    /// \param isr        Initial ISR (subset of replicas; often == replicas at start).
    /// \param log        Owning pointer to the partition Log (pre-constructed).
    /// \param transport  Injected transport (fake for tests, Aeron for prod).
    PartitionReplica(PartitionConfig                       cfg,
                     Role                                   role,
                     Epoch                                  epoch,
                     std::vector<NodeId>                    replicas,
                     std::vector<NodeId>                    isr,
                     rune::storage::Log*                    log,
                     PartitionTransport*                    transport);

    // -----------------------------------------------------------------------
    // Role transitions (called by control plane on leader election / demotion)
    // -----------------------------------------------------------------------

    /// Transition to Leader with the given epoch.
    /// Resets ISR from replicas, seeds ReplicaLeaderState.
    void becomeLeader(Epoch newEpoch, std::vector<NodeId> isr);

    /// Transition to Follower with the given leader epoch.
    void becomeFollower(Epoch leaderEpoch, NodeId leaderId);

    // -----------------------------------------------------------------------
    // Leader API
    // -----------------------------------------------------------------------

    /// Append a record batch to the leader's log and advance LEO.
    ///
    /// \param records   Raw record bytes (the batch payload — caller-managed).
    /// \param acksAll   true → require |ISR| >= minIsr (majority-durable produce).
    /// \returns base offset of the appended batch, or -1 if gated (minIsr).
    ///
    /// Does NOT block. Does NOT wait for followers. The caller drives doWork()
    /// to pump follower fetches which advance the HW.
    Offset append(std::span<const uint8_t> records, bool acksAll = true);

    /// Reconcile the leader's tracked LEO with the underlying Log and recompute HW.
    ///
    /// Called after a batch is appended DIRECTLY to the leader's partition Log by
    /// a co-located writer that bypasses append() — e.g. the transaction layer
    /// stamping already-framed transactional data batches or control (COMMIT/ABORT)
    /// markers into the same partition log the replica owns.  Without this call the
    /// leader's ReplicaLeaderState would still hold the pre-append LEO and HW would
    /// never advance to cover the externally-written bytes.
    ///
    /// GR-10: when pushOnAppend is enabled, also pushes the newly-appended bytes
    /// to each ISR follower via an unsolicited FetchResponse (non-blocking).
    ///
    /// No-op on a follower (follower LEO/HW are driven by applyReplicated()).
    void onLeaderLogAppended() noexcept;

    /// Handle an inbound FetchRequest from a follower.
    ///
    /// Epoch-fences (rejects stale leaderEpoch with kErrStaleEpoch).
    /// Updates follower LEO in ReplicaLeaderState + IsrManager.
    /// Recomputes HW.
    /// Sends a FetchResponse via transport_.
    ///
    /// \param req    The fetch request from the follower.
    /// \param nowNs  Current wall-clock time (nanoseconds) used to record the
    ///               follower's real fetch time in IsrManager.  Callers MUST pass
    ///               the actual current time so time-lag eviction is correct.
    ///
    /// \returns kErrNone on success, error code otherwise.
    int16_t onFetchRequest(const FetchRequestMsg& req, int64_t nowNs);

    /// Read committed records (≤ highWatermark) starting at fetchOffset.
    ///
    /// Returns empty span if fetchOffset >= highWatermark (nothing committed
    /// beyond the caller's position yet).
    ///
    /// The returned vector's lifetime is the caller's responsibility.
    std::vector<uint8_t> read(Offset fetchOffset, int32_t maxBytes) const;

    // -----------------------------------------------------------------------
    // Follower API
    // -----------------------------------------------------------------------

    /// Apply a FetchResponse received from the leader.
    ///
    /// On epoch change (response.leaderEpoch > local):
    ///   1. Compute truncation offset via EpochFencing.
    ///   2. Truncate local Log to that offset.
    ///   3. Adopt the new epoch.
    ///
    /// Then append the records to the local Log (if any).
    /// Update local HW = min(local LEO, response.highWatermark).
    void applyReplicated(const FetchResponseMsg& resp);

    /// Build a FetchRequest for the leader: fetchOffset = local LEO.
    FetchRequestMsg nextFetch() const noexcept;

    // -----------------------------------------------------------------------
    // doWork — single-writer periodic step (no spin, no sleep)
    // -----------------------------------------------------------------------

    /// Drive periodic work for this replica.
    ///
    /// Leader:   runs IsrManager shrink/expand on isrCheckIntervalNs cadence;
    ///           propagates updated ISR into ReplicaLeaderState.
    /// Follower: emits a FetchRequest when fetchIntervalNs has elapsed.
    ///
    /// \param nowNs   Injected clock (nanoseconds). Sampled ONCE per call.
    /// \returns Number of work items performed (0 when idle — no spin).
    int doWork(int64_t nowNs);

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    [[nodiscard]] Role        role()           const noexcept { return role_; }
    [[nodiscard]] Epoch       epoch()          const noexcept { return epoch_; }
    [[nodiscard]] Offset      logEndOffset()   const noexcept { return log_->logEndOffset(); }
    [[nodiscard]] Offset      highWatermark()  const noexcept;
    [[nodiscard]] PartitionId partitionId()    const noexcept { return cfg_.partitionId; }
    [[nodiscard]] NodeId      selfId()         const noexcept { return cfg_.selfId; }

    /// Non-owning pointer to the underlying Log (for test inspection and replay seeding).
    [[nodiscard]] rune::storage::Log* log() const noexcept { return log_; }

    /// ISR as seen by the leader (empty for follower).
    [[nodiscard]] const std::vector<NodeId>& isr() const noexcept;

private:
    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// (Leader) Recompute HW from ReplicaLeaderState and propagate to leader ISR.
    void recomputeHw();

    /// (Leader) Propagate current ISR into ReplicaLeaderState.
    void syncIsrToLeaderState();

    /// (Follower) Truncate the log to `truncateOffset` (inclusive last valid
    /// offset = truncateOffset - 1).  Updates local LEO.
    void truncateLog(Offset truncateOffset);

    /// (Leader) GR-10: push records starting at fromOffset to all ISR followers
    /// via unsolicited FetchResponse.  Non-blocking (tryClaim/offer); a dropped
    /// push is fine — the fetch backstop recovers it.  Uses encodeScratch_ (no
    /// per-call alloc).  Only called when cfg_.pushOnAppend is true.
    void pushToFollowers(Offset fromOffset) noexcept;

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------
    PartitionConfig      cfg_;
    Role                 role_;
    Epoch                epoch_;
    NodeId               leaderId_;           ///< (follower) leader's NodeId; -1 if unknown

    rune::storage::Log*         log_;         ///< non-owning (owner is the test/container)
    PartitionTransport*         transport_;   ///< non-owning

    // Leader-side components
    ReplicaLeaderState   leaderState_;        ///< HW + per-follower LEO
    IsrManager*          isrManager_ = nullptr; // heap — allocated on becomeLeader
    std::unique_ptr<IsrManager> isrManagerOwned_; ///< ownership wrapper

    // Epoch caches (both roles need one for truncation)
    rune::storage::LeaderEpochCache epochCache_;

    // Timing state
    int64_t lastIsrCheckNs_  = 0;   ///< last ISR shrink/expand timestamp
    int64_t lastFetchEmitNs_ = 0;   ///< last follower fetch emission timestamp

    // Pre-allocated scratch buffers (no hot-path alloc)
    static constexpr std::size_t kScratchSize = 4 * 1024 * 1024; // 4 MiB
    std::vector<uint8_t> encodeScratch_;   ///< for FetchResponse encoding

    // Follower HW (leader uses leaderState_.highWatermark())
    Offset followerHw_ = 0;

    // GR-10: push tracking — last offset that was pushed to followers.
    // Avoids re-pushing bytes already pushed in the same append cycle.
    Offset lastPushedOffset_ = 0;
};

// ---------------------------------------------------------------------------
// InMemoryTransport — deterministic fake for tests
//
// Implements a simple in-memory message queue between a leader PartitionReplica
// and one or more follower PartitionReplicas. Call pump() to deliver all
// pending messages (requests to leader, responses to followers).
//
// Usage in a bounded test driver:
//   InMemoryTransport transport(leader, followers);
//   for (int i = 0; i < CAP && !done; ++i) {
//       clock += step;
//       leader.doWork(clock);
//       for (auto& f : followers) f.doWork(clock);
//       transport.pump();
//       done = check();
//   }
//   ASSERT_TRUE(done) << "stalled after " << CAP << " iterations";
// ---------------------------------------------------------------------------
class InMemoryTransport : public PartitionTransport {
public:
    struct PendingRequest {
        FetchRequestMsg req;
    };
    struct PendingResponse {
        NodeId          toFollower;
        FetchResponseMsg resp;
        std::vector<uint8_t> recordsBuf; // owns the bytes resp.records points into
    };

    // Register leader and followers after construction.
    void setLeader(PartitionReplica* leader) { leader_ = leader; }
    void addFollower(NodeId id, PartitionReplica* follower) {
        followers_[id] = follower;
    }
    /// Remove a follower from the transport (simulates network partition/crash).
    void removeFollower(NodeId id) {
        followers_.erase(id);
    }

    // PartitionTransport implementation
    //
    // GR-10: sendFetchRequest and sendFetchResponse are guarded by a mutex so
    // that the leader push path (called from the produce thread) and the
    // pump() path (called from the replication duty-cycle thread) are safe to
    // call concurrently.  In single-threaded tests the lock is uncontended.
    void sendFetchRequest(const FetchRequestMsg& req) override {
        std::lock_guard lk(mu_);
        pendingRequests_.push_back({req});
    }

    void sendFetchResponse(NodeId toFollower,
                           const FetchResponseMsg& resp) override {
        PendingResponse pr;
        pr.toFollower = toFollower;
        // Deep-copy records bytes so the response outlives the leader's scratch buf.
        pr.recordsBuf.assign(resp.records.begin(), resp.records.end());
        pr.resp = resp;
        pr.resp.records = std::span<const uint8_t>(pr.recordsBuf.data(),
                                                    pr.recordsBuf.size());
        std::lock_guard lk(mu_);
        pendingResponses_.push_back(std::move(pr));
    }

    /// Deliver all pending messages: FetchRequests → leader, FetchResponses → followers.
    ///
    /// \param nowNs  Current wall-clock time passed to onFetchRequest() so the
    ///               leader records the follower's real fetch timestamp.  Defaults
    ///               to 0 for backward compatibility with tests that do not care
    ///               about time-lag eviction; real tests should supply the clock.
    ///
    /// Returns total messages delivered.
    int pump(int64_t nowNs = 0) {
        int count = 0;

        // Deliver fetch requests to the leader.
        std::vector<PendingRequest> reqs;
        {
            std::lock_guard lk(mu_);
            reqs.swap(pendingRequests_);
        }
        for (auto& pr : reqs) {
            if (leader_) {
                leader_->onFetchRequest(pr.req, nowNs);
                ++count;
            }
        }

        // Deliver fetch responses to followers.
        std::vector<PendingResponse> resps;
        {
            std::lock_guard lk(mu_);
            resps.swap(pendingResponses_);
        }
        for (auto& pr : resps) {
            auto it = followers_.find(pr.toFollower);
            if (it != followers_.end()) {
                it->second->applyReplicated(pr.resp);
                ++count;
            }
        }

        return count;
    }

    /// Number of in-flight messages (for test assertions).
    [[nodiscard]] std::size_t pendingCount() const noexcept {
        std::lock_guard lk(mu_);
        return pendingRequests_.size() + pendingResponses_.size();
    }

private:
    PartitionReplica* leader_ = nullptr;
    std::unordered_map<NodeId, PartitionReplica*> followers_;

    mutable std::mutex           mu_;              ///< guards pendingRequests_/pendingResponses_
    std::vector<PendingRequest>  pendingRequests_;
    std::vector<PendingResponse> pendingResponses_;
};

} // namespace rune::replication
