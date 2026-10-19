#pragma once
// TestCluster.h — catalog history Task 17: the in-process way to create a topic.
//
// Topics exist only through DDL (spec §8: the static topic list and NodeConfig::topics are gone).  A test starts
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
// Topic ids follow creation order from 0, so a test that defines its topics in the order its old static
// list had keeps its ids.  A failure is `ADD_FAILURE()` + -1 — a deadline is a failure, never a hang.
#include "TestClusterCore.h"

#include <gtest/gtest.h>

namespace rune::node::test {

class TestCluster : public TestClusterCore {
public:
    explicit TestCluster(std::vector<BrokerNode*> nodes) : TestClusterCore(std::move(nodes)) {}

    /// Define `spec` (every field a static topic entry carried) and wait until it serves.
    /// Returns the topic id; ADD_FAILURE + -1 on an error answer or the deadline.
    int32_t defineTopic(const TopicSpec& spec, std::chrono::milliseconds timeout = std::chrono::seconds(30)) {
        const DefineResult r = tryDefineTopic(spec, timeout);
        if (!r.ok())
            ADD_FAILURE() << "TestCluster::defineTopic('" << spec.name << "') failed: code " << r.code
                          << (r.code == -1 ? " (deadline)" : "") << " — " << r.detail;
        return r.ok() ? r.topicId : -1;
    }
    int32_t defineTopic(std::string name, int32_t partitions,
                        std::chrono::milliseconds timeout = std::chrono::seconds(30)) {
        TopicSpec s;
        s.name           = std::move(name);
        s.partitionCount = partitions;
        return defineTopic(s, timeout);
    }
};

/// Topics exist only through DDL.  A test that must put data into a topic's partition dir BEFORE its node boots
/// (the cleaner suites write through a standalone Log) boots `cfg` once, defines `specs` through the catalog and
/// stops: the test's own boot recovers the topics from __catalog, together with whatever it wrote into their
/// dirs meanwhile.  Fails the test (ADD_FAILURE) if the defining boot does not elect or a define fails.
inline void defineTopicsThenStop(const NodeConfig& cfg, const std::vector<TopicSpec>& specs) {
    BrokerNode node{cfg};
    node.start();
    if (!node.awaitLeader(std::chrono::seconds(10))) {
        ADD_FAILURE() << "defineTopicsThenStop: the defining boot did not self-elect";
        node.stop();
        return;
    }
    {
        TestCluster cluster({&node});
        for (const TopicSpec& spec : specs) {
            if (cluster.defineTopic(spec) < 0) break;   // the failure is already recorded
        }
    }
    node.stop();
}

} // namespace rune::node::test
