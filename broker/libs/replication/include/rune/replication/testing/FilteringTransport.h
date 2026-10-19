#pragma once
// ---------------------------------------------------------------------------
// FilteringTransport.h — moved VERBATIM from
// tests/integration/ReplicationClusterHarness.h (catalog history Task 20) so the
// catalog interaction-matrix harness (libs/node/tests/support) shares it.
// Test support only: nothing in the broker includes it.
// ---------------------------------------------------------------------------

#include <rune/replication/PartitionReplica.h>

#include <cstdint>
#include <set>
#include <unordered_map>
#include <vector>

namespace rune::replication::test {

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

} // namespace rune::replication::test
