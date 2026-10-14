// ClientDdlBrokerRegressionTest.cpp — catalog history Task 11 (F7): the REAL
// client (rune::client::Client, Thread runner) against ONE REAL in-process
// BrokerNode over Aeron UDP.  The client's blocking *AndWait DDL pumps its own
// thread — a fake broker cannot serve these calls.  Every result carries the
// DDL's requestId (Task 5), which is the one the broker wrote into the event.
// (The multi-node blocking cases — a follower's Redirect followed by *AndWait,
// NotLeader until the budget below minIsr — land in Task 22; the below-minIsr
// explicit failure + exactly-once retry is pinned here already, on three brokers.)
#include "client/OneBroker.h"
#include "support/CatalogBrokerCluster.h"

#include <rune/protocol/client/SchemaMessages.h>

#include <gtest/gtest.h>

#include <chrono>
#include <span>
#include <string>
#include <thread>

using namespace std::chrono_literals;
using rune::node::test::OneBroker;

TEST(ClientDdlBrokerRegression, EveryDdlAndWaitRoundTripsThroughARealBroker) {
    OneBroker b("cb_all");
    auto client = b.connectClient();
    rune::client::DdlOptions o; o.statement = "topic u partitions 1;";
    const auto created = client->defineTopicAndWait("u", 1, "", {}, false, rune::client::Persistence::Disk, {}, o, 10s);
    ASSERT_EQ(created.errorCode, 0) << created.detail;
    EXPECT_FALSE(created.requestId.none()) << "the result names its requestId";
    const rune::protocol::client::ConfigChangeWire ch{1, 60'000};
    const auto cfg = client->alterTopicConfigAndWait("u", 0, std::span(&ch, 1), {}, 10s);
    EXPECT_EQ(cfg.errorCode, 0) << cfg.detail;
    const auto schema = client->defineTopicAndWait("u", 1, "id", b.idDescriptor(), false,
                                                   rune::client::Persistence::Disk, {}, {}, 10s);
    EXPECT_EQ(schema.errorCode, 0) << "CreateSchema on the untyped topic: " << schema.detail;
    const auto altered = client->alterTopicAndWait("u", 0, b.addColumnOps("mana"), {}, 10s);
    EXPECT_EQ(altered.errorCode, 0) << altered.detail;
    const auto view = client->createViewAndWait("v", "id", 0, b.viewPlanOver("u"), {}, 10s);
    EXPECT_EQ(view.errorCode, 0) << view.detail;
    const auto alteredView = client->alterViewAndWait("v", "id", 0, /*expectedVersion=*/1,
                                                      b.viewPlanOver("u", /*extra=*/true), {}, 10s);
    EXPECT_EQ(alteredView.errorCode, 0) << alteredView.detail;
    const auto droppedView = client->dropViewAndWait("v", {}, 10s);
    EXPECT_EQ(droppedView.errorCode, 0) << droppedView.detail;
    const auto dropped = client->dropTopicAndWait("u", {}, 10s);
    EXPECT_EQ(dropped.errorCode, 0) << dropped.detail;

    const auto log = b.catalogEvents();
    ASSERT_EQ(log.size(), 8u);
    EXPECT_EQ(log[0].event.statement, "topic u partitions 1;");
    EXPECT_FALSE(log[0].event.principal.requestId.none());
    // Every result's requestId is the one its event carries (F14: a retry with it dedupes).
    const rune::protocol::RequestId ids[] = {created.requestId,  cfg.requestId,         schema.requestId,
                                             altered.requestId,  view.requestId,        alteredView.requestId,
                                             droppedView.requestId, dropped.requestId};
    for (std::size_t i = 0; i < 8; ++i) {
        EXPECT_FALSE(ids[i].none()) << i;
        EXPECT_EQ(log[i].event.principal.requestId, ids[i]) << "event " << i;
    }
}

TEST(ClientDdlBrokerRegression, InvalidRequestDetailFromTheBrokerReachesTheCaller) {
    OneBroker b("cb_rf");
    auto client = b.connectClient();
    rune::client::TopicOptions topic; topic.replicationFactor = 5;
    const auto r = client->defineTopicAndWait("t", 1, "", {}, false, rune::client::Persistence::Disk, topic, {}, 10s);
    EXPECT_EQ(r.errorCode, 39);
    EXPECT_EQ(r.detail, "replicationFactor 5 > broker count 1");
    EXPECT_FALSE(r.requestId.none());
    EXPECT_TRUE(b.catalogEvents().empty()) << "a refused DDL appends nothing";
}

TEST(ClientDdlBrokerRegression, ARetriedRequestIdGetsTheOriginalResultFromTheBroker) {
    OneBroker b("cb_dedupe");
    auto client = b.connectClient();
    rune::client::DdlOptions o; o.requestId = {.hi = 0xC11E, .lo = 42};
    const auto first  = client->defineTopicAndWait("t", 1, "", {}, false, rune::client::Persistence::Disk, {}, o, 10s);
    const auto second = client->defineTopicAndWait("t", 1, "", {}, false, rune::client::Persistence::Disk, {}, o, 10s);
    EXPECT_EQ(first.errorCode, 0);
    EXPECT_EQ(second.errorCode, 0) << "not TopicExists";
    EXPECT_EQ(second.topicId, first.topicId);
    EXPECT_EQ(first.requestId, o.requestId);
    EXPECT_EQ(second.requestId, o.requestId);
    EXPECT_EQ(b.catalogEvents().size(), 1u);
    // A different requestId for the same name is a new DDL: TopicExists.
    const auto third = client->defineTopicAndWait("t", 1, "", {}, false, rune::client::Persistence::Disk, {}, {}, 10s);
    EXPECT_EQ(third.errorCode, 25);
    EXPECT_NE(third.requestId, o.requestId);
}

// ===========================================================================
// Binding (Task 11): below the catalog's minIsr the real client fails EXPLICITLY
// once its budget runs out — never a hang — and the same requestId, retried
// once the brokers are back, runs the DDL exactly once.  Three real brokers.
// ===========================================================================
namespace {

using namespace rune::node::test::catalog_cluster;

int32_t consensusLeaderOf(ThreeNodes& c) {
    for (rune::node::BrokerNode* n : c.live())
        if (n->isLeader()) return n->nodeIdForTest();
    return -1;
}

/// Events carrying `rid` in broker `n`'s __catalog log.
std::size_t eventsWithRequestId(rune::node::BrokerNode& n, const rune::protocol::RequestId& rid) {
    return n.onAdminThreadForTest([&n, &rid]() -> std::size_t {
        rune::replication::PartitionReplica* r = n.catalogReplicaForTest();
        if (r == nullptr) return 0;
        const std::vector<uint8_t> data = r->log()->read(0, 1 << 22);
        std::size_t count = 0, pos = 0;
        while (pos < data.size()) {
            rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
            if (!reader.ensureValid()) break;
            reader.forEachRecord([&](std::string_view, std::string_view val, int64_t, int64_t) {
                const auto ev = rune::catalog::decode(
                    std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()), val.size()));
                if (ev && ev->principal.requestId == rid) ++count;
            });
            if (reader.totalLength() == 0) break;
            pos += static_cast<std::size_t>(reader.totalLength());
        }
        return count;
    });
}

} // namespace

TEST(ClientDdlBrokerRegression, BelowMinIsrTheClientFailsExplicitlyAtItsBudgetAndTheRetryRunsOnce) {
    ThreeNodes c = ThreeNodes::start("cb_minisr");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    int32_t leader = -1;
    for (const auto deadline = std::chrono::steady_clock::now() + 30s;
         std::chrono::steady_clock::now() < deadline && leader < 0;) {
        const int32_t ctl = consensusLeaderOf(c);
        if (ctl > 0 && c.settledLeader() == ctl) leader = ctl;
        else std::this_thread::sleep_for(20ms);
    }
    ASSERT_GT(leader, 0) << "the controller leads the catalog";

    rune::client::ClientConfig cfg;
    cfg.gatewayChannel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(c.ports.client[static_cast<std::size_t>(leader - 1)]);
    cfg.gatewayStreamId = kClientStream;
    cfg.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    cfg.egressStreamId  = 2;
    cfg.dataDir         = (c.dirs[static_cast<std::size_t>(leader - 1)]->path / "client").string();
    cfg.ddlTimeoutMs    = 8'000;
    cfg.ddlRetryBudget  = std::chrono::milliseconds(4'000);
    rune::client::Client client(cfg);
    client.connectAndWait(20s);
    ASSERT_EQ(client.defineTopicAndWait("data", 1, "", {}, false, rune::client::Persistence::Disk, {}, {}, 20s).errorCode, 0);

    for (int id = 1; id <= 3; ++id) if (id != leader) c.stop(id);
    rune::client::DdlOptions o; o.requestId = {.hi = 0xC11E, .lo = 77};
    const auto t0 = std::chrono::steady_clock::now();
    bool explicitFailure = false;
    try {
        const auto r = client.defineTopicAndWait("more", 1, "", {}, false, rune::client::Persistence::Disk, {}, o, 30s);
        // Review M5: exactly the retriable NotLeader past the budget — never another code.
        EXPECT_EQ(r.errorCode, 2) << "below minIsr: NotLeader until the budget ends (" << r.detail << ")";
        explicitFailure = r.errorCode == 2;
    } catch (const std::exception& e) {
        // …or the client's DDL deadline: Timeout, explicit.
        const std::string what = e.what();
        EXPECT_NE(what.find("timed out"), std::string::npos) << what;
        explicitFailure = what.find("timed out") != std::string::npos;
    }
    EXPECT_TRUE(explicitFailure);
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 20s) << "ends at the client's budget / deadline, never a hang";

    for (int id = 1; id <= 3; ++id) if (id != leader) c.startNode(id);
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    rune::client::DefineTopicResult retry;
    for (const auto deadline = std::chrono::steady_clock::now() + 90s; std::chrono::steady_clock::now() < deadline;) {
        try {
            retry = client.defineTopicAndWait("more", 1, "", {}, false, rune::client::Persistence::Disk, {}, o, 30s);
            if (retry.errorCode == 0) break;
        } catch (const std::exception&) {
            retry.errorCode = 0xFFFF;
        }
        std::this_thread::sleep_for(200ms);
    }
    ASSERT_EQ(retry.errorCode, 0) << "the same requestId, once the catalog is writable again";
    EXPECT_EQ(retry.requestId, o.requestId);
    const int32_t now = c.settledLeader();
    ASSERT_GT(now, 0);
    EXPECT_EQ(eventsWithRequestId(c.node(now), o.requestId), 1u) << "no DDL lost or duplicated";
}
