#pragma once
// TestClusterCore.h — catalog history Task 17: the gtest-free engine behind TestCluster (TestCluster.h), also used
// by the benchmarks.  See TestCluster.h for the contract.
//
// Topics exist only through DDL (spec §8: RUNE_TOPICS / NodeConfig::topics are gone).  A test starts
// its BrokerNode(s) and then defines each topic here:
//
//     auto node = std::make_unique<BrokerNode>(cfg);
//     node->start();
//     ASSERT_EQ(TestCluster({node.get()}).defineTopic("events", 3), 0);
//
// The DDL goes over the wire (ShardTestClient -> gateway -> admin ring -> __catalog event -> applier ->
// effects), exactly as a real client's does — nothing is injected.  Any node may be asked: a follower
// answers Redirect(leader) and the request follows it; a leader still catching up answers the
// retriable NotLeader and the request is retried after 50 ms (both bounded by the timeout).  The same
// requestId is re-sent on every retry, so a lost answer is deduped by the broker, never a TopicExists.
// After the OK the call waits until the topic serves on every node that holds a replica of it (C1
// generalized: applied at its catalog id AND placed with its roles routed).
//
// Topic ids follow creation order from 0, so a test that defines its topics in its old RUNE_TOPICS
// order keeps its ids.  A failure is `ADD_FAILURE()` + -1 — a deadline is a failure, never a hang.
#include "ShardTestClient.h"

#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/CatalogTypes.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

namespace rune::node::test {

class TestClusterCore {
public:
    using Clock = std::chrono::steady_clock;

    /// The outcome of one DefineTopic, without gtest side effects (error-path tests assert on it).
    struct DefineResult {
        int32_t     topicId = -1;   ///< the catalog id on success, else -1
        int         code    = 0;    ///< 0 = OK; the wire ErrorCode of the failure; -1 = the deadline passed
        std::string detail;         ///< the ErrorResponse message / what was being waited for
        [[nodiscard]] bool ok() const noexcept { return code == 0 && topicId >= 0; }
    };

    explicit TestClusterCore(std::vector<BrokerNode*> nodes) : nodes_(std::move(nodes)) {}
    TestClusterCore(const TestClusterCore&)            = delete;
    TestClusterCore& operator=(const TestClusterCore&) = delete;
    ~TestClusterCore() {
        client_.reset();   // the client's driver before its directory
        if (!dir_.empty()) { std::error_code ec; std::filesystem::remove_all(dir_, ec); }
    }

    /// Define `spec` and wait until it serves: the explicit answer (an error code + detail, or the deadline),
    /// no gtest side effects — benchmarks and the error-path tests use this.
    DefineResult tryDefineTopic(const TopicSpec& spec, std::chrono::milliseconds timeout) {
        namespace pc = rune::protocol::client;
        const auto deadline = Clock::now() + timeout;
        if (nodes_.empty()) return fail_(-2, "TestCluster has no nodes");
        ensureClient_();
        const rune::protocol::RequestId rid{0x7E57'C100'0000'0000ull | nonce_, ++counter_};
        const rune::protocol::TopicConfigView cfg = wireConfig_(spec);

        std::size_t target = 0;   // any node; a Redirect names the leader
        std::string last = "no attempt completed";
        while (Clock::now() < deadline) {
            const NodeConfig& nc = nodes_[target]->effectiveConfigForTest();
            const std::string& channel = nc.clientEndpoint.channel;
            const int32_t      stream  = nc.clientEndpoint.streamId;
            const int64_t sid = sessionOn_(channel, stream);
            if (sid <= 0) { last = "cannot connect to " + channel; sleep_(); continue; }

            const int64_t corr = client_->sendDefineTopic(channel, stream, sid, spec.name,
                                                          static_cast<uint32_t>(spec.partitionCount), {},
                                                          spec.persistence == PersistenceMode::Transient ? 1 : 0,
                                                          rid, spec.replicationFactor, cfg);
            if (corr == 0) { last = "the request could not be sent"; sleep_(); continue; }
            const auto until = std::min(deadline, Clock::now() + std::chrono::seconds(10));
            const auto ans = client_->awaitAnyDdlAnswer(corr, until);
            if (!ans) { last = "no answer from " + channel; continue; }

            if (ans->responseTemplate == static_cast<uint16_t>(pc::TemplateId::ErrorResponse)) {
                if (ans->code == static_cast<int16_t>(pc::ErrorCode::NotLeader)) {
                    ++notLeaderRetries_;
                    last = "NotLeader from " + channel;
                    sleep_();
                    continue;
                }
                if (ans->code == static_cast<int16_t>(pc::ErrorCode::Redirect)) {
                    bool known = false;
                    for (std::size_t i = 0; i < nodes_.size(); ++i)
                        if (nodes_[i]->effectiveConfigForTest().clientEndpoint.channel == ans->message) {
                            target = i;
                            known  = true;
                        }
                    if (!known) return fail_(ans->code, "Redirect to an endpoint that is not one of the cluster's: " + ans->message);
                    ++redirectsFollowed_;
                    last = "Redirect to " + ans->message;
                    continue;
                }
                return fail_(ans->code, ans->message);
            }
            if (ans->code != 0) return fail_(ans->code, "the DefineTopicResponse carries no detail");

            const auto resp = client_->awaitDefine(corr, Clock::now() + std::chrono::seconds(2));
            if (!resp) return fail_(static_cast<int>(pc::ErrorCode::Internal), "an OK answer without a DefineTopicResponse");
            const int32_t id = resp->topicId;
            if (!awaitServes_(id, spec.partitionCount, deadline))
                return fail_(-1, "topic id " + std::to_string(id) + " was acknowledged but does not serve on every replica holder");
            return DefineResult{id, 0, {}};
        }
        return fail_(-1, "timed out; " + last);
    }

    /// How many Redirect answers this cluster followed / NotLeader answers it retried (test observability).
    [[nodiscard]] std::size_t redirectsFollowed() const noexcept { return redirectsFollowed_; }
    [[nodiscard]] std::size_t notLeaderRetries() const noexcept { return notLeaderRetries_; }

private:
    static DefineResult fail_(int code, std::string detail) {
        DefineResult r;
        r.code   = code;
        r.detail = std::move(detail);
        return r;
    }
    static void sleep_() { std::this_thread::sleep_for(std::chrono::milliseconds(50)); }

    static rune::protocol::TopicConfigView wireConfig_(const TopicSpec& s) {
        rune::protocol::TopicConfigView c;
        c.cleanupPolicy             = static_cast<uint8_t>(s.cleanupPolicy);
        c.retentionMs               = s.retentionMs;
        c.retentionBytes            = s.retentionBytes;
        c.deleteRetentionMs         = s.deleteRetentionMs;
        c.minCleanableDirtyRatioPpm = static_cast<int32_t>(std::llround(s.minCleanableDirtyRatio * 1'000'000.0));
        c.segmentRollBytes          = s.segmentRollBytes;
        c.minCompactionLagMs        = s.minCompactionLagMs;
        c.maxCompactionLagMs        = s.maxCompactionLagMs;
        c.retentionCheckMs          = s.retentionCheckMs;
        c.fileDeleteDelayMs         = s.fileDeleteDelayMs;
        c.durability                = s.durability;
        c.durabilityTier            = s.durabilityTier;
        c.coPartitionGroup          = s.coPartitionGroup;
        c.partitionKey              = s.partitionKey;
        return c;
    }

    void ensureClient_() {
        if (client_) return;
        static std::atomic<int> seq{0};
        dir_ = std::filesystem::temp_directory_path() /
               ("rune_tc_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        std::filesystem::create_directories(dir_);
        client_ = std::make_unique<ShardTestClient>(dir_ / "client-aeron");
        client_->keepSessionsAlive(true);
        client_->setClientVersion(rune::protocol::client::kClientVersion);   // a pre-v8 session is refused by a follower
        nonce_ = (static_cast<uint64_t>(std::random_device{}()) << 16 ^ std::random_device{}()) & 0x0000'FFFF'FFFF'FFFFull;
    }

    /// One session per gateway channel, reused by every DDL of this cluster.
    int64_t sessionOn_(const std::string& channel, int32_t stream) {
        for (const auto& s : sessions_)
            if (s.first == channel) return s.second;
        const int64_t sid = client_->connect(channel, stream);
        if (sid > 0) sessions_.emplace_back(channel, sid);
        return sid;
    }

    /// Every node that holds a replica of the topic serves it; a node that holds none (rf < nodes) is done
    /// once it knows the placement and is not in it.
    bool awaitServes_(int32_t topicId, int32_t partitions, Clock::time_point deadline) {
        for (BrokerNode* n : nodes_) {
            for (;;) {
                if (n->catalogTopicServesForTest(topicId)) break;
                if (holdsNoReplica_(*n, topicId, partitions)) break;
                if (Clock::now() >= deadline) return false;
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
        }
        return true;
    }
    static bool holdsNoReplica_(BrokerNode& n, int32_t topicId, int32_t partitions) {
        return n.onControlThreadForTest([&n, topicId, partitions] {
            const int32_t self = n.nodeIdForTest();
            for (int32_t p = 0; p < partitions; ++p) {
                const auto* a = n.metadataServiceForTest()->assignment(topicId, p);
                if (a == nullptr) return false;   // the placement has not reached this node yet
                for (int32_t r : a->replicas)
                    if (r == self) return false;
            }
            return true;
        });
    }

    std::vector<BrokerNode*>                         nodes_;
    std::filesystem::path                            dir_;
    std::unique_ptr<ShardTestClient>                 client_;
    std::vector<std::pair<std::string, int64_t>>     sessions_;
    uint64_t                                         nonce_   = 0;
    uint64_t                                         counter_ = 0;
    std::size_t                                      redirectsFollowed_ = 0;
    std::size_t                                      notLeaderRetries_  = 0;
};

} // namespace rune::node::test
