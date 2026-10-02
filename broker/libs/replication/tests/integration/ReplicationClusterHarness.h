#pragma once
// ---------------------------------------------------------------------------
// ReplicationClusterHarness.h — Deterministic multi-node replication cluster
//                               harness for integration tests (P3-6).
//
// Namespace: rune::replication::test
// C++23. NO busy-spin. NO while(!done). All loops bounded + ASSERT(done).
// Injected monotonic clock. FilteringTransport. REAL P3 components.
//
// ## Design (mirrored from ClusterHarness.h for consensus P2P-9)
//
//   ReplicationClusterHarness owns:
//     - N Log objects (one per node) in N RAII TempDir directories.
//     - N PartitionReplica objects (one per node) sharing one FilteringTransport.
//     - One PartitionFailoverCoordinator wired to all replicas.
//     - One injected int64_t clock_ (nanoseconds).
//
//   The FilteringTransport wraps an InMemoryTransport and gates message
//   delivery based on the harness's alive_/partitioned_ arrays.  Since all
//   replicas share the SAME transport instance (by pointer), re-wiring the
//   leader or isolating a node never requires constructing a new transport —
//   we just update the filtering policy.
//
//   driveUntil(pred, maxSteps, stepNs):
//     Bounded driver: iterate at most maxSteps times.  Each step advances the
//     clock by stepNs, calls doWork() on all ALIVE replicas, and pumps the
//     transport.  Returns true iff pred() is true after the loop.
//     NEVER hangs — caller ASSERT_TRUE(driveUntil(...)).
//
//   Faults:
//     killLeader()     — marks leader dead, removes from drive loop.
//     reviveNode(idx)  — marks node alive, transitions to follower.
//     partitionOff(idx)— stops delivering messages to/from that node.
//     healPartition()  — restores full connectivity.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/replication/PartitionReplica.h>
#include <rune/replication/PartitionFailoverCoordinator.h>
#include <rune/replication/FileRecordingStore.h>
#include <rune/replication/PartitionRecorder.h>
#include <rune/replication/PartitionReplay.h>
#include <rune/storage/Log.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace rune::replication::test {

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
static inline std::string makeUniqSuffix() {
    static std::atomic<uint64_t> gSeq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_" +
           std::to_string(gSeq.fetch_add(1, std::memory_order_relaxed)) + "_" +
           std::to_string(ts);
}

struct HarnessDir {
    std::string path;
    explicit HarnessDir(const std::string& tag) {
        path = std::filesystem::temp_directory_path().string()
               + "/rune_integ_" + tag + "_" + makeUniqSuffix();
        std::filesystem::create_directories(path);
    }
    ~HarnessDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
    HarnessDir(const HarnessDir&)            = delete;
    HarnessDir& operator=(const HarnessDir&) = delete;
    HarnessDir(HarnessDir&&) noexcept        = default;
    HarnessDir& operator=(HarnessDir&&)      = delete;
};

static inline std::vector<uint8_t> makePayload(uint8_t fill, std::size_t sz = 16) {
    return std::vector<uint8_t>(sz, fill);
}

// ---------------------------------------------------------------------------
// FilteringTransport — PartitionTransport that gates delivery based on
// which nodes are alive/partitioned.
//
// All N replicas share a single FilteringTransport instance (by pointer).
// The harness updates currentLeaderId_ and the isolated set to control
// which messages are delivered, without needing to create a new transport.
// ---------------------------------------------------------------------------
class FilteringTransport : public PartitionTransport {
public:
    // Register a replica (all roles — leader detection is dynamic).
    void registerReplica(NodeId id, PartitionReplica* replica) {
        replicas_[id] = replica;
    }

    // Update which node is the current leader.
    void setCurrentLeader(NodeId id) { currentLeaderId_ = id; }

    // Mark a node as isolated (won't send OR receive).
    void isolate(NodeId id)  { isolated_.insert(id); }
    void deisolate(NodeId id){ isolated_.erase(id); }
    void clearIsolated()     { isolated_.clear(); }

    // --- PartitionTransport interface ---

    // Called by a follower's doWork() when it wants to send a FetchRequest.
    bool sendFetchRequest(const FetchRequestMsg& req) override {
        // Drop if sender (follower) is isolated.
        if (isolated_.count(req.followerId)) return false;
        // Drop if the current leader is isolated.
        if (isolated_.count(currentLeaderId_)) return false;
        pendingRequests_.push_back({req});
        return true;
    }

    // Called by the leader's onFetchRequest() to send a FetchResponse.
    bool sendFetchResponse(NodeId toFollower,
                           const FetchResponseMsg& resp) override {
        // Drop if recipient follower is isolated.
        if (isolated_.count(toFollower)) return false;
        // Drop if leader is isolated (this call is from the leader).
        if (isolated_.count(currentLeaderId_)) return false;

        PendingResponse pr;
        pr.toFollower = toFollower;
        pr.recordsBuf.assign(resp.records.begin(), resp.records.end());
        pr.resp = resp;
        pr.resp.records = std::span<const uint8_t>(pr.recordsBuf.data(),
                                                    pr.recordsBuf.size());
        pendingResponses_.push_back(std::move(pr));
        return true;
    }

    // DR-BUG-6: OffsetsForLeaderEpoch + reconcile peer-routed messages.
    bool sendOffsetsForLeaderEpochRequest(
            NodeId toLeader, const OffsetsForLeaderEpochReqMsg& req) override {
        if (isolated_.count(req.followerId) || isolated_.count(toLeader)) return false;
        pendingEpochReqs_.push_back({toLeader, req});
        return true;
    }
    bool sendOffsetsForLeaderEpochResponse(
            NodeId toFollower, const OffsetsForLeaderEpochRespMsg& resp) override {
        if (isolated_.count(toFollower)) return false;
        pendingEpochResps_.push_back({toFollower, resp});
        return true;
    }
    bool sendReconcileRequest(
            NodeId toPeer, const FetchRequestMsg& req) override {
        if (isolated_.count(toPeer) || isolated_.count(req.followerId)) return false;
        pendingReconcileReqs_.push_back({toPeer, req});
        return true;
    }
    bool sendReconcileResponse(
            NodeId toPeer, const FetchResponseMsg& resp) override {
        if (isolated_.count(toPeer)) return false;
        PendingResponse pr;
        pr.toFollower = toPeer;
        pr.recordsBuf.assign(resp.records.begin(), resp.records.end());
        pr.resp = resp;
        pr.resp.records = std::span<const uint8_t>(pr.recordsBuf.data(),
                                                    pr.recordsBuf.size());
        pendingReconcileResps_.push_back(std::move(pr));
        return true;
    }

    // Deliver all pending messages.
    int pump(int64_t nowNs) {
        int count = 0;

        // Deliver fetch requests to the current leader.
        std::vector<PendingRequest> reqs;
        reqs.swap(pendingRequests_);
        for (auto& pr : reqs) {
            auto it = replicas_.find(currentLeaderId_);
            if (it != replicas_.end() && it->second != nullptr) {
                if (it->second->role() == Role::Leader) {
                    it->second->onFetchRequest(pr.req, nowNs);
                    ++count;
                }
                // If node has become a follower (e.g. fenced), drop the request.
            }
        }

        // Deliver fetch responses to followers.
        std::vector<PendingResponse> resps;
        resps.swap(pendingResponses_);
        for (auto& pr : resps) {
            auto it = replicas_.find(pr.toFollower);
            if (it != replicas_.end() && it->second != nullptr) {
                it->second->applyReplicated(pr.resp);
                ++count;
            }
        }

        // DR-BUG-6: OffsetsForLeaderEpoch requests → target replica (by NodeId).
        std::vector<PendingEpochReq> ereqs;
        ereqs.swap(pendingEpochReqs_);
        for (auto& e : ereqs) {
            auto it = replicas_.find(e.to);
            if (it != replicas_.end() && it->second != nullptr) {
                it->second->onOffsetsForLeaderEpochRequest(e.req);
                ++count;
            }
        }

        // DR-BUG-6: OffsetsForLeaderEpoch responses → target follower.
        std::vector<PendingEpochResp> eresps;
        eresps.swap(pendingEpochResps_);
        for (auto& e : eresps) {
            auto it = replicas_.find(e.to);
            if (it != replicas_.end() && it->second != nullptr) {
                it->second->applyOffsetsForLeaderEpochResponse(e.resp);
                ++count;
            }
        }

        // DR-BUG-6: reconcile requests → target (ahead survivor).
        std::vector<PendingReconcileReq> rreqs;
        rreqs.swap(pendingReconcileReqs_);
        for (auto& r : rreqs) {
            auto it = replicas_.find(r.to);
            if (it != replicas_.end() && it->second != nullptr) {
                it->second->onReconcileRequest(r.req);
                ++count;
            }
        }

        // DR-BUG-6: reconcile responses → target (deficient leader).
        std::vector<PendingResponse> rresps;
        rresps.swap(pendingReconcileResps_);
        for (auto& pr : rresps) {
            auto it = replicas_.find(pr.toFollower);
            if (it != replicas_.end() && it->second != nullptr) {
                it->second->applyReconcileResponse(pr.resp);
                ++count;
            }
        }

        return count;
    }

    [[nodiscard]] std::size_t pendingCount() const noexcept {
        return pendingRequests_.size() + pendingResponses_.size()
             + pendingEpochReqs_.size() + pendingEpochResps_.size()
             + pendingReconcileReqs_.size() + pendingReconcileResps_.size();
    }

    [[nodiscard]] NodeId currentLeaderId() const noexcept {
        return currentLeaderId_;
    }

private:
    struct PendingRequest {
        FetchRequestMsg req;
    };
    struct PendingResponse {
        NodeId            toFollower;
        FetchResponseMsg  resp;
        std::vector<uint8_t> recordsBuf;
    };
    struct PendingEpochReq  { NodeId to; OffsetsForLeaderEpochReqMsg  req; };
    struct PendingEpochResp { NodeId to; OffsetsForLeaderEpochRespMsg resp; };
    struct PendingReconcileReq { NodeId to; FetchRequestMsg req; };

    NodeId currentLeaderId_ = -1;
    std::unordered_map<NodeId, PartitionReplica*> replicas_;
    std::set<NodeId> isolated_;

    std::vector<PendingRequest>  pendingRequests_;
    std::vector<PendingResponse> pendingResponses_;
    std::vector<PendingEpochReq>  pendingEpochReqs_;    ///< DR-BUG-6
    std::vector<PendingEpochResp> pendingEpochResps_;   ///< DR-BUG-6
    std::vector<PendingReconcileReq> pendingReconcileReqs_;  ///< DR-BUG-6
    std::vector<PendingResponse>  pendingReconcileResps_;    ///< DR-BUG-6
};

// ---------------------------------------------------------------------------
// ReplicationClusterHarness
// ---------------------------------------------------------------------------
class ReplicationClusterHarness {
public:
    static constexpr int     kDefaultCap = 500;
    static constexpr int64_t kStep1ms    = 1'000'000LL;

    explicit ReplicationClusterHarness(int n,
                                        int64_t lagMaxMs = 1000,
                                        int     minIsr   = 1)
        : n_(n)
    {
        for (int i = 1; i <= n; ++i) nodeIds_.push_back(static_cast<NodeId>(i));

        std::vector<NodeId> isr = nodeIds_;
        currentIsrBeforeFailover_ = isr;

        // Build per-node Log + PartitionReplica.
        for (int i = 0; i < n; ++i) {
            NodeId id = nodeIds_[i];
            dirs_.emplace_back("h" + std::to_string(id) + "n" + std::to_string(n));
            logs_.emplace_back(
                std::make_unique<rune::storage::Log>(dirs_.back().path));

            Role role = (i == 0) ? Role::Leader : Role::Follower;
            PartitionConfig cfg = makeConfig(id, lagMaxMs, 10000, minIsr,
                                             kStep1ms, kStep1ms);
            replicas_.emplace_back(
                std::make_unique<PartitionReplica>(
                    cfg, role, /*epoch=*/1,
                    nodeIds_, isr,
                    logs_.back().get(), &transport_));

            alive_.push_back(true);
            partitioned_.push_back(false);
        }

        // Wire filtering transport.
        transport_.setCurrentLeader(nodeIds_[0]);
        for (int i = 0; i < n; ++i) {
            transport_.registerReplica(nodeIds_[i], replicas_[i].get());
        }

        // Build coordinator.
        PartitionMetadata meta;
        meta.partitionId = 0;
        meta.leader      = nodeIds_[0];
        meta.replicas    = nodeIds_;
        meta.isr         = isr;
        meta.leaderEpoch = 1;

        coord_ = std::make_unique<PartitionFailoverCoordinator>(meta, &liveness_,
                                                                 kStep1ms);
        for (int i = 0; i < n; ++i) {
            coord_->registerReplica(nodeIds_[i], replicas_[i].get());
        }

        currentLeaderIdx_ = 0;
    }

    // -----------------------------------------------------------------------
    // Drive loop — bounded deterministic step.
    // -----------------------------------------------------------------------
    bool driveUntil(std::function<bool()> done,
                    int maxSteps = kDefaultCap,
                    int64_t stepNs = kStep1ms)
    {
        for (int i = 0; i < maxSteps && !done(); ++i) {
            clock_ += stepNs;
            for (int j = 0; j < n_; ++j) {
                if (alive_[j] && !partitioned_[j]) {
                    replicas_[j]->doWork(clock_);
                }
            }
            transport_.pump(clock_);
            coord_->doWork(clock_);
        }
        return done();
    }

    // -----------------------------------------------------------------------
    // Produce helpers
    // -----------------------------------------------------------------------

    Offset appendAndCommit(int count, uint8_t fill = 0xAB) {
        PartitionReplica* ldr = leaderReplica();
        EXPECT_NE(ldr, nullptr) << "No leader to append to";
        if (!ldr) return -1;

        for (int i = 0; i < count; ++i) {
            ldr->append(makePayload(static_cast<uint8_t>(fill + i)));
        }
        Offset leo = ldr->logEndOffset();
        bool ok = driveUntil([&]{ return ldr->highWatermark() >= leo; });
        EXPECT_TRUE(ok) << "HW did not advance to " << leo;
        return ldr->highWatermark();
    }

    // -----------------------------------------------------------------------
    // Fault injection
    // -----------------------------------------------------------------------

    int killLeader() {
        int idx = currentLeaderIdx_;
        NodeId deadId = nodeIds_[idx];
        alive_[idx] = false;
        liveness_.markDead(deadId);
        // Signal through the REAL production path: mark the node dead in the
        // liveness oracle (above) AND call signalLeaderDead(deadId) so that
        // the subsequent driveUntil inside electNewLeader() will trigger the
        // election via coord_->doWork().  FIX 1 guarantees exactly one
        // election per signal: the liveness guard ensures a stale/leftover
        // signal can never demote a healthy new leader.
        coord_->signalLeaderDead(deadId);
        // Isolate in transport so it doesn't receive messages.
        transport_.isolate(deadId);
        return idx;
    }

    void reviveNode(int idx) {
        alive_[idx] = true;
        liveness_.markAlive(nodeIds_[idx]);
        partitioned_[idx] = false;
        transport_.deisolate(nodeIds_[idx]);
        // Transition to follower under current epoch.
        replicas_[idx]->becomeFollower(coord_->currentEpoch(), coord_->currentLeader());
    }

    void partitionOff(int idx) {
        partitioned_[idx] = true;
        transport_.isolate(nodeIds_[idx]);
    }

    void healPartition() {
        for (int i = 0; i < n_; ++i) {
            if (!alive_[i]) continue; // killed nodes stay killed
            partitioned_[i] = false;
            transport_.deisolate(nodeIds_[i]);
        }
    }

    // Drive election through the REAL signal path: coord_->doWork() consumes
    // the leaderDeadSignaled_ flag set by killLeader() and elects via
    // electNewLeader().  FIX 2: kill-leader scenarios go through this path
    // instead of calling coord_->electNewLeader() directly.
    bool electNewLeader() {
        // If the signal was already consumed by a previous doWork() tick (e.g.
        // an earlier driveUntil already triggered the election), check whether
        // a live leader is already in place — if so, we're done.
        auto alreadyElected = [&]() -> bool {
            NodeId nl = coord_->currentLeader();
            for (int i = 0; i < n_; ++i)
                if (nodeIds_[i] == nl && alive_[i]) return true;
            return false;
        };

        if (!alreadyElected()) {
            // Drive until doWork() processes the death signal and elects.
            driveUntil(alreadyElected);
        }

        bool ok = alreadyElected();
        if (ok) {
            NodeId nl = coord_->currentLeader();
            transport_.setCurrentLeader(nl);
            for (int i = 0; i < n_; ++i) {
                if (nodeIds_[i] == nl) {
                    currentLeaderIdx_ = i;
                    break;
                }
            }
        }
        return ok;
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    [[nodiscard]] bool hasLeader() const noexcept {
        NodeId ldr = coord_->currentLeader();
        if (ldr == -1) return false;
        for (int i = 0; i < n_; ++i)
            if (nodeIds_[i] == ldr && alive_[i]) return true;
        return false;
    }

    [[nodiscard]] NodeId   currentLeader()  const noexcept { return coord_->currentLeader(); }
    [[nodiscard]] Epoch    currentEpoch()   const noexcept { return coord_->currentEpoch(); }
    [[nodiscard]] const std::vector<NodeId>& currentIsr() const noexcept { return coord_->currentIsr(); }

    [[nodiscard]] Offset highWatermark() const noexcept {
        PartitionReplica* ldr = leaderReplica();
        return ldr ? ldr->highWatermark() : 0;
    }

    [[nodiscard]] PartitionReplica* replicaAt(int idx) const noexcept {
        return replicas_[idx].get();
    }

    [[nodiscard]] PartitionReplica* replicaById(NodeId id) const noexcept {
        for (int i = 0; i < n_; ++i)
            if (nodeIds_[i] == id) return replicas_[i].get();
        return nullptr;
    }

    [[nodiscard]] rune::storage::Log* logAt(int idx)  const noexcept { return logs_[idx].get(); }
    [[nodiscard]] NodeId nodeId(int idx)               const noexcept { return nodeIds_[idx]; }
    [[nodiscard]] bool   isAlive(int idx)              const noexcept { return alive_[idx]; }
    [[nodiscard]] int    nodeCount()                   const noexcept { return n_; }
    [[nodiscard]] int64_t clock()                      const noexcept { return clock_; }

    [[nodiscard]] PartitionReplica* leaderReplica() const noexcept {
        NodeId ldr = coord_->currentLeader();
        for (int i = 0; i < n_; ++i)
            if (nodeIds_[i] == ldr && alive_[i]) return replicas_[i].get();
        return nullptr;
    }

    [[nodiscard]] PartitionFailoverCoordinator& coordinator() noexcept { return *coord_; }
    [[nodiscard]] FilteringTransport& transport() noexcept { return transport_; }

    // -----------------------------------------------------------------------
    // Invariant assertions
    // -----------------------------------------------------------------------

    void assertNoCommittedLoss(Offset hwBefore) {
        ASSERT_GT(hwBefore, 0);
        for (int i = 0; i < n_; ++i) {
            if (!alive_[i]) continue;
            EXPECT_GE(replicas_[i]->logEndOffset(), hwBefore)
                << "Node " << nodeIds_[i] << " LEO=" << replicas_[i]->logEndOffset()
                << " < hwBefore=" << hwBefore << " — committed loss!";
        }
    }

    void assertHwMonotonic(Offset hwBefore) {
        PartitionReplica* ldr = leaderReplica();
        ASSERT_NE(ldr, nullptr);
        EXPECT_GE(ldr->logEndOffset(), hwBefore)
            << "New leader LEO " << ldr->logEndOffset()
            << " < old HW " << hwBefore << " — HW regressed!";
    }

    void assertEpochBumped(Epoch expectedOldEpoch) {
        EXPECT_EQ(coord_->currentEpoch(), expectedOldEpoch + 1);
    }

    void assertOnlyIsrCanBeLeader(const std::vector<NodeId>& isrBeforeFailover) {
        NodeId nl = coord_->currentLeader();
        bool found = std::find(isrBeforeFailover.begin(), isrBeforeFailover.end(), nl)
                     != isrBeforeFailover.end();
        EXPECT_TRUE(found) << "New leader " << nl << " was NOT in ISR before failover!";
    }

    void snapshotIsr()  { currentIsrBeforeFailover_ = coord_->currentIsr(); }
    [[nodiscard]] const std::vector<NodeId>& isrSnapshot() const noexcept {
        return currentIsrBeforeFailover_;
    }

    void advanceClock(int64_t ns) { clock_ += ns; }

private:
    static PartitionConfig makeConfig(NodeId id,
                                      int64_t lagMaxMs  = 1000,
                                      Offset  lagMaxOff = 10000,
                                      int     minIsr    = 1,
                                      int64_t fetchInt  = kStep1ms,
                                      int64_t isrCheck  = kStep1ms)
    {
        PartitionConfig cfg;
        cfg.partitionId         = 0;
        cfg.selfId              = id;
        cfg.replicaLagMaxMs     = lagMaxMs;
        cfg.replicaLagMaxOffset = lagMaxOff;
        cfg.minIsr              = minIsr;
        cfg.fetchIntervalNs     = fetchInt;
        cfg.isrCheckIntervalNs  = isrCheck;
        cfg.maxFetchBytes       = 1 << 20;
        return cfg;
    }

    int                                              n_;
    std::vector<NodeId>                              nodeIds_;
    std::vector<HarnessDir>                          dirs_;
    std::vector<std::unique_ptr<rune::storage::Log>> logs_;
    std::vector<std::unique_ptr<PartitionReplica>>   replicas_;
    std::vector<bool>                                alive_;
    std::vector<bool>                                partitioned_;

    FilteringTransport                               transport_;
    SimpleNodeLiveness                               liveness_;
    std::unique_ptr<PartitionFailoverCoordinator>    coord_;

    int64_t  clock_            = 0;
    int      currentLeaderIdx_ = 0;

    std::vector<NodeId> currentIsrBeforeFailover_;
};

} // namespace rune::replication::test
