// CheckedWritesBrokerRegressionTest.cpp — checked writes Part 02: typed validation against REAL brokers and the REAL
// client on the default (shard) runtime.  Worker state is read only inside onOwnerThreadForTest (a direct read from
// the test thread would race the worker).
#include "client/OneBroker.h"
#include "client/CheckedWritesBrokerSupport.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

using namespace std::chrono_literals;
using namespace rune::node;
using namespace rune::node::test;
namespace cwb = rune::node::test::cw_broker;

TEST(CheckedWritesBrokerRegression, TopicCreateWaitsForItsSpecOnEveryWorkerRegression) {
    OneBroker b("cw-order", OneBroker::Runtime::Shard, [](NodeConfig& cfg) { cfg.testHoldWriteSpecPasses = 300; });
    rune::client::Client& c = *b.connectClient();
    const auto r = cwb::defineTyped(c, "chars", 2);
    ASSERT_EQ(r.errorCode, 0) << r.detail;
    for (int32_t p = 0; p < 2; ++p)
        EXPECT_TRUE(cwb::hasWriteState(b.node(), r.topicId, p)) << "partition " << p << ": the topic served before its spec";
    EXPECT_EQ(cwb::produceRaw(b, "chars", r.topicId, 2, std::string("\x01\x02\x03", 3), rune::client::Key(uint64_t{1})),
              static_cast<uint16_t>(rune::protocol::client::ErrorCode::SchemaValidationFailed));
}

TEST(CheckedWritesBrokerRegression, DroppedWhileAwaitingSpecNeverBuildsRegression) {
    OneBroker b("cw-drop", OneBroker::Runtime::Shard, [](NodeConfig& cfg) { cfg.testHoldWriteSpecPasses = 400000; });
    rune::client::Client& c = *b.connectClient();
    // The create is parked behind the held spec; the define answers only once it served, so drive the drop from a second
    // client while the first is still waiting.
    std::atomic<int32_t> topicId{-1};
    std::thread definer([&] {
        const auto r = cwb::defineTyped(c, "chars", 2);
        topicId = r.errorCode == 0 ? r.topicId : -2;
    });
    // The id is known to the catalog before the topic serves.
    int32_t id = -1;
    for (int i = 0; i < 2000 && id < 0; ++i) {
        id = cwb::catalogTopicId(b.node(), "chars");
        if (id < 0) std::this_thread::sleep_for(5ms);
    }
    ASSERT_GE(id, 0);
    rune::client::Client& c2 = *b.connectClient();
    const auto dropped = c2.dropTopicAndWait("chars", {}, 20s);
    EXPECT_EQ(dropped.errorCode, 0) << dropped.detail;
    definer.join();
    EXPECT_FALSE(b.node().catalogTopicServesForTest(id));
    for (int32_t p = 0; p < 2; ++p) EXPECT_FALSE(cwb::hasWriteState(b.node(), id, p));
    EXPECT_FALSE(cwb::anyWorkerHasValidator(b.node(), id));
}
