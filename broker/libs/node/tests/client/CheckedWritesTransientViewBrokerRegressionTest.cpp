// CheckedWritesTransientViewBrokerRegressionTest.cpp - checked writes Part 10 (spec 9a): persisted views over a TRANSIENT
// topic against a real broker (shard runtime) and the real client.  A persisted view keeps the rows of its last checkpoint
// across a restart and folds what the (empty) ring holds again.  Every wait is bounded; nothing sleeps for a fixed time.
#include "client/OneBroker.h"
#include "client/TransientViewBrokerSupport.h"

#include <gtest/gtest.h>

using namespace std::chrono_literals;
using namespace rune::node;
using namespace rune::node::test;
namespace tv = rune::node::test::transient_view_broker;
namespace rc = rune::client;

namespace {

constexpr uint16_t kViewKeyInvalid = 35, kUnknownTopic = 7, kTopicExists = 25;

/// "presence": transient, key id, {id, hp}.
rc::DefineTopicResult definePresence(rc::Client& c) {
    return c.defineTopicAndWait("presence", 1, "id", OneBroker::idDescriptor("presence"), rc::Persistence::Transient, {}, {}, 60s);
}

/// The persisted views of the tests: p_rows (id, hp) and p_totals (hp, n, minId).
struct Views {
    int32_t rows   = -1;
    int32_t totals = -1;
};
Views createViews(rc::Client& c, int32_t src) {
    Views v;
    const auto rows = c.createViewAndWait("p_rows", "id", 0, tv::rowsBlob("p_rows", src), {}, 60s);
    EXPECT_EQ(rows.errorCode, 0) << rows.detail << " (37 is never sent)";
    v.rows = rows.viewTopicId;
    const auto totals = c.createViewAndWait("p_totals", "hp", 0, tv::totalsBlob("p_totals", src), {}, 60s);
    EXPECT_EQ(totals.errorCode, 0) << totals.detail;
    v.totals = totals.viewTopicId;
    return v;
}

void awaitLevel(BrokerNode& n) {
    bool raised = false;
    for (int i = 0; i < 800 && !raised; ++i) {
        raised = n.onWorkerThreadForTest(0, [](Worker& w) { return w.clusterFeatureLevelForTest(); }) >= rune::replication::MetadataFeature::kCheckedWrites;
        if (!raised) std::this_thread::sleep_for(25ms);
    }
    ASSERT_TRUE(raised) << "the cluster feature level reaches kCheckedWrites";
}

}  // namespace

TEST(CheckedWritesTransientViewBrokerRegression, PersistedViewsOverATransientTopicFoldAndServeRegression) {
    OneBroker b("tv_serve", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    const auto src = definePresence(c);
    ASSERT_EQ(src.errorCode, 0) << src.detail;
    const Views v = createViews(c, src.topicId);
    ASSERT_GE(v.rows, 0);
    ASSERT_GE(v.totals, 0);

    tv::Prod prod(b.clientConfig(), "presence", src.topicId, rc::Acks::All);
    ASSERT_TRUE(prod.put(1, 5));
    ASSERT_TRUE(prod.put(2, 5));
    ASSERT_TRUE(prod.put(3, 7));

    EXPECT_TRUE(tv::awaitRow(c, v.rows, 3, {3, 7}));
    EXPECT_TRUE(tv::awaitRow(c, v.totals, 5, {5, 2, 1})) << "hp 5: keys 1 and 2";
    EXPECT_TRUE(tv::awaitRow(c, v.totals, 7, {7, 1, 3})) << "hp 7: key 3";
}

TEST(CheckedWritesTransientViewBrokerRegression, FullRestartKeepsTheCheckpointRowsAndFoldsTheNewRingRegression) {
    OneBroker b("tv_restart", OneBroker::Runtime::Shard);
    int32_t srcId = -1;
    Views v;
    {
        rc::Client& c = *b.connectClient();
        const auto src = definePresence(c);
        ASSERT_EQ(src.errorCode, 0) << src.detail;
        srcId = src.topicId;
        v = createViews(c, srcId);
        tv::Prod prod(b.clientConfig(), "presence", srcId, rc::Acks::All);
        ASSERT_TRUE(prod.put(1, 5));
        ASSERT_TRUE(prod.put(2, 5));
        ASSERT_TRUE(prod.put(3, 7));
        ASSERT_TRUE(tv::awaitRow(c, v.rows, 3, {3, 7}));
        ASSERT_TRUE(tv::awaitRow(c, v.totals, 5, {5, 2, 1}));
    }
    const std::string dataDir = b.node().effectiveConfigForTest().dataDir;
    EXPECT_TRUE(tv::awaitCheckpointO(dataDir, v.rows, 3, 10s)) << "the Replace view checkpointed all three records";
    EXPECT_TRUE(tv::awaitCheckpointO(dataDir, v.totals, 3, 10s)) << "and so did the aggregate";

    b.restart();
    rc::Client& c = *b.connectClient();
    const auto again = definePresence(c);
    EXPECT_EQ(again.errorCode, kTopicExists) << "the topic is still in the catalog";
    EXPECT_EQ(again.topicId, srcId);

    // Before anything is produced into the new (empty) ring: the checkpoint rows are served.
    EXPECT_TRUE(tv::awaitRow(c, v.rows, 1, {1, 5}));
    EXPECT_TRUE(tv::awaitRow(c, v.rows, 2, {2, 5}));
    EXPECT_TRUE(tv::awaitRow(c, v.rows, 3, {3, 7}));
    EXPECT_TRUE(tv::awaitRow(c, v.totals, 5, {5, 2, 1}));

    // The first produces land at offsets 0 and 1 of the new ring, below the old cursor 3: they must fold.
    tv::Prod prod(b.clientConfig(), "presence", srcId, rc::Acks::All);
    ASSERT_TRUE(prod.put(4, 5));
    ASSERT_TRUE(prod.put(1, 7));
    EXPECT_TRUE(tv::awaitRow(c, v.rows, 4, {4, 5}));
    EXPECT_TRUE(tv::awaitRow(c, v.rows, 1, {1, 7}));
    EXPECT_TRUE(tv::awaitRow(c, v.totals, 5, {5, 2, 2})) << "keys 2 and 4: key 1 moved out, nothing counted twice";
    EXPECT_TRUE(tv::awaitRow(c, v.totals, 7, {7, 2, 1})) << "keys 1 and 3";
}

TEST(CheckedWritesTransientViewBrokerRegression, ViewsOverAKeylessTransientTopicAreRefusedRegression) {
    OneBroker b("tv_keyless", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    const auto src = c.defineTopicAndWait("events_t", 1, "", OneBroker::idDescriptor("events_t"), rc::Persistence::Transient, {}, {}, 60s);
    ASSERT_EQ(src.errorCode, 0) << src.detail;

    const auto persisted = c.createViewAndWait("ev_p", "id", 0, tv::rowsBlob("ev_p", src.topicId), {}, 30s);
    EXPECT_EQ(persisted.errorCode, kViewKeyInvalid) << "Part 02's keyless rule, never 37";
    EXPECT_EQ(persisted.detail, "view source needs a key");
    const auto transient = c.createViewAndWait("ev_t", "id", 1, tv::rowsBlob("ev_t", src.topicId, /*persistence=*/1), {}, 30s);
    EXPECT_EQ(transient.errorCode, kViewKeyInvalid);
    EXPECT_EQ(transient.detail, "view source needs a key");
    for (const char* name : {"ev_p", "ev_t"})
        EXPECT_EQ(c.getTopicSchemaAndWait(name, 30s).errorCode, kUnknownTopic) << name << ": no sink was created";
}

TEST(CheckedWritesTransientViewBrokerRegression, ViewAckOnAPersistedViewOverATransientTopicMeansFoldedRegression) {
    OneBroker b("tv_viewack", OneBroker::Runtime::Shard);
    awaitLevel(b.node());
    rc::Client& c = *b.connectClient();
    const auto src = definePresence(c);
    ASSERT_EQ(src.errorCode, 0) << src.detail;
    const Views v = createViews(c, src.topicId);
    ASSERT_GE(v.rows, 0);

    uint64_t id = 0;
    for (const rc::Acks acks : {rc::Acks::All | rc::Acks::View, rc::Acks::Leader | rc::Acks::View}) {
        tv::Prod prod(b.clientConfig(), "presence", src.topicId, acks);
        ++id;
        ASSERT_TRUE(prod.put(id, id * 10)) << "the View ack arrives";
        // One read, no retry loop: the ack means the record is folded.  Only a read that never reached the leader is repeated.
        rc::ViewRowResult row;
        for (int i = 0; i < 20; ++i) {
            row = c.getViewRowAndWait(v.rows, static_cast<int64_t>(id), 0, 10s);
            if (row.status != rc::ViewRowResult::kNotLeader) break;
            std::this_thread::sleep_for(50ms);
        }
        EXPECT_EQ(row.status, rc::ViewRowResult::kOk) << "acks " << static_cast<int>(acks) << ": the acked record is already in the view";
    }
}
