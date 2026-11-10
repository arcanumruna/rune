// LeaseLineageBrokerTest.cpp - lease topics Task 27 (spec A.6): the control thread posts the committed data lineage (token +
// generation) of a transient lease partition to its owning worker, over a REAL single-node broker (default config).  A process
// restart is a new lineage: generation 0 -> 1, a different token.
#include "support/LeaseBrokerRig.h"

#include <rune/node/Worker.h>

#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <thread>

using namespace rune::node::test;
using namespace std::chrono_literals;
namespace rc = rune::client;

namespace {

/// The lineage note of (topic, 0) on whichever worker owns it, polled (real time, hard cap) until a note exists.
std::optional<rune::node::LeaseLineageNote> noteOf(LeaseBrokerRig& rig, int32_t topic) {
    auto& node = rig.broker().node();
    const auto end = std::chrono::steady_clock::now() + 20s;
    do {
        for (int32_t w = 0; w < node.workerCountForTest(); ++w) {
            auto n = node.onWorkerThreadForTest(w, [topic](rune::node::Worker& wk) { return wk.leaseLineageForTest(topic, 0); });
            if (n.has_value()) return n;
        }
        std::this_thread::sleep_for(5ms);
    } while (std::chrono::steady_clock::now() < end);
    return std::nullopt;
}

}  // namespace

// Mutation: skip the LeaseLineageFanout step in NodeRuntimeAgent::doWork -> no note ever arrives, red.
TEST(LeaseLineageRegression, SingleNodeGenerationAcrossRestartRegression) {
    LeaseBrokerRig rig("lease-lineage-gen");
    rc::TopicOptions to;
    to.lease = lbr::spec(60'000, 1000, 60'000);   // an explicit max_ttl: a transient lease topic needs one
    to.config.cleanupPolicy = 1;
    const auto d = rig.ua().defineTopicAndWait("lin", 1, "claim", lbr::claimBlob(), rc::Persistence::Transient, to, {}, 60s);
    ASSERT_EQ(d.errorCode, 0) << d.detail;

    const auto first = noteOf(rig, d.topicId);
    ASSERT_TRUE(first.has_value()) << "the owning worker never learned the lineage";
    EXPECT_EQ(first->generation, 0u);
    EXPECT_NE(first->lineage, 0u);
    EXPECT_GE(first->leaderEpoch, 0);

    rig.restart();
    // The restarted worker is a new object: its note is empty until the fresh lineage is posted.
    const auto second = noteOf(rig, d.topicId);
    ASSERT_TRUE(second.has_value()) << "the owning worker never learned the lineage after the restart";
    EXPECT_EQ(second->generation, 1u) << "a process restart mints over the existing record: generation + 1";
    EXPECT_NE(second->lineage, first->lineage);
}
