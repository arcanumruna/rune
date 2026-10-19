// SecurityReadServerTest.cpp — authentication Task 10: `__security` reads are the catalog read contract (backlog up to
// the HW, ONE caught-up marker, live events, history epoch) served by the admin thread's SECOND CatalogReadServer;
// the server stamps the topic it was built for (fix-on-sight: it hard-coded kCatalogTopicId twice).
#include "support/CatalogReadRig.h"

#include <rune/node/SecurityPartition.h>

#include <gtest/gtest.h>

#include <filesystem>

using rune::node::test::ReadRig;
namespace pc = rune::protocol::client;

TEST(SecurityReadServerRegression, ServesSecurityUpToTheHighWatermarkThenTheMarker) {
    ReadRig r("sec_rd_basic", false, 64, rune::node::kSecurityTopicId);
    r.commitEvents(4);
    r.server.subscribe(/*session=*/7, /*corr=*/1, /*from=*/-2);
    r.server.pump();
    ASSERT_TRUE(r.subscribeAnswered(7, 1));
    EXPECT_EQ(r.deliveredOffsets(7), (std::vector<int64_t>{0, 1, 2, 3}));
    EXPECT_EQ(r.markers(7), (std::vector<int64_t>{4})) << "the caught-up marker at the HW";
    EXPECT_EQ(r.notes(7).size(), 1u) << "the history epoch note opens the stream";
    r.commitEvents(1);
    r.server.pump();
    EXPECT_EQ(r.deliveredOffsets(7).back(), 4) << "live events follow";
    EXPECT_EQ(r.markers(7).size(), 1u) << "one marker per subscription";
    // The subscription id lives in the security id space (bit 62 + bit 61): one id names one server.
    const int64_t sid = r.subscriptionIdOf(7, 1);
    EXPECT_TRUE(rune::gateway::isCatalogSubscriptionId(sid));
    EXPECT_TRUE(rune::gateway::isSecuritySubscriptionId(sid));
    // The history epoch is persisted in the security log's own directory.
    EXPECT_TRUE(std::filesystem::exists(std::filesystem::path(r.replica().log()->dir()) / "history_epoch"));
    EXPECT_NE(r.replica().log()->dir().find("__security"), std::string::npos);
}

TEST(CatalogReadServerRegression, ServesTheTopicItWasBuiltForRegression) {
    // A server built for -7 stamps -7 on EVERY frame (note, data, marker); the -6 server's frames are unchanged.
    for (const int32_t topic : {rune::node::kSecurityTopicId, rune::node::kCatalogTopicId}) {
        ReadRig r(topic == rune::node::kSecurityTopicId ? "topic_sec" : "topic_cat", false, 64, topic);
        r.commitEvents(2);
        r.server.subscribe(9, 1, -2);
        r.server.pump();
        const auto recs = r.records(9);
        ASSERT_GE(recs.size(), 3u) << "note + data + marker";
        for (const auto& rec : recs) {
            EXPECT_EQ(rec.topicId, topic) << "topic " << topic;
            EXPECT_EQ(rec.partition, 0);
        }
        EXPECT_FALSE(rune::gateway::isSecuritySubscriptionId(r.subscriptionIdOf(9, 1)) == (topic == rune::node::kCatalogTopicId));
    }
}
