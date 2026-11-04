// CheckedWritesTransientViewFailoverBrokerRegressionTest.cpp - checked writes Part 10 (spec 9a): a persisted view over a
// TRANSIENT topic (rf 3) when the broker leading the source partition is crashed: the view moves to the new leader and serves
// the rows of the source ring it replicated, then folds what is produced next.  Crash tests are serial (RUN_SERIAL).
#include "client/ClusterClient.h"
#include "client/TransientViewBrokerSupport.h"

#include <gtest/gtest.h>

using namespace std::chrono_literals;
using namespace rune::node::test::catalog_cluster;
namespace tv = rune::node::test::transient_view_broker;
namespace rc = rune::client;

TEST(CheckedWritesTransientViewFailoverBrokerRegression, LeaderCrashServesTheRowsFromTheNewLeadersRingRegression) {
    ThreeNodes c = ThreeNodes::start("tv_fo");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t seed = c.runningNodes().front();
    auto admin = connectClient(c, seed);
    rc::TopicOptions to;
    to.replicationFactor = 3;
    const auto src = admin->defineTopicAndWait("presence", 1, "id", rune::node::test::OneBroker::idDescriptor("presence"), rc::Persistence::Transient, to, {}, 60s);
    ASSERT_EQ(src.errorCode, 0) << src.detail;
    const auto rows = admin->createViewAndWait("p_rows", "id", 0, tv::rowsBlob("p_rows", src.topicId), {}, 60s);
    ASSERT_EQ(rows.errorCode, 0) << rows.detail;
    const auto totals = admin->createViewAndWait("p_totals", "hp", 0, tv::totalsBlob("p_totals", src.topicId), {}, 60s);
    ASSERT_EQ(totals.errorCode, 0) << totals.detail;

    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, src.topicId, 1, leaders, 30s));
    const int32_t victim = leaders[0];
    int32_t survivor = -1;
    for (int id : c.runningNodes()) if (id != victim) { survivor = id; break; }
    ASSERT_GT(survivor, 0);

    {
        tv::Prod prod(clientConfigFor(c, victim), "presence", src.topicId, rc::Acks::All);
        ASSERT_TRUE(prod.put(1, 5));
        ASSERT_TRUE(prod.put(2, 5));
        ASSERT_TRUE(prod.put(3, 7));
        ASSERT_TRUE(tv::awaitRow(*admin, rows.viewTopicId, 3, {3, 7}));
    }
    admin->close();

    c.crash(victim);
    ASSERT_TRUE(awaitLeaders(c, src.topicId, 1, leaders, 60s)) << "a survivor leads the source partition";
    ASSERT_NE(leaders[0], victim);

    auto reader = connectClient(c, survivor);
    EXPECT_TRUE(tv::awaitRow(*reader, rows.viewTopicId, 1, {1, 5}));
    EXPECT_TRUE(tv::awaitRow(*reader, rows.viewTopicId, 3, {3, 7}));
    EXPECT_TRUE(tv::awaitRow(*reader, totals.viewTopicId, 5, {5, 2, 1}));

    tv::Prod prod(clientConfigFor(c, survivor), "presence", src.topicId, rc::Acks::All);
    ASSERT_TRUE(prod.put(4, 5));
    ASSERT_TRUE(prod.put(1, 7));
    EXPECT_TRUE(tv::awaitRow(*reader, rows.viewTopicId, 4, {4, 5}));
    EXPECT_TRUE(tv::awaitRow(*reader, rows.viewTopicId, 1, {1, 7}));
    EXPECT_TRUE(tv::awaitRow(*reader, totals.viewTopicId, 5, {5, 2, 2}));
    EXPECT_TRUE(tv::awaitRow(*reader, totals.viewTopicId, 7, {7, 2, 1}));
}
