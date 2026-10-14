// CatalogReadBrokerTest.cpp — catalog history Task 12 on THREE in-process
// BrokerNodes over REAL Aeron UDP, driven by the wire client ShardTestClient:
// a Subscribe on `__catalog` (topic id −6) is served by EVERY broker — the
// catalog leader, a follower, a broker whose apply halted — from its own replica
// up to its own HW: the committed backlog, then ONE caught-up marker (an empty
// Records at the HW), then the live events; a session's close ends its catalog
// subscriptions on the admin thread.
#include "support/CatalogBrokerCluster.h"
#include "support/DdlHarness.h"          // charBlob
#include "support/ShardTestClient.h"

#include <rune/node/BrokerNode.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <string>
#include <thread>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::catalog_cluster;
using namespace std::chrono_literals;
namespace pc = rune::protocol::client;
using Clock = std::chrono::steady_clock;

namespace {

std::string channelOf(const ThreeNodes& c, int id) {
    return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(c.ports.client[static_cast<std::size_t>(id - 1)]);
}

int64_t connectSettled(ShardTestClient& cl, const ThreeNodes& c, int id, std::chrono::seconds within) {
    for (const auto deadline = Clock::now() + within; Clock::now() < deadline;) {
        const int64_t sid = cl.connect(channelOf(c, id), kClientStream);
        if (sid > 0) return sid;
        std::this_thread::sleep_for(50ms);
    }
    return -1;
}

/// A define on the catalog leader, retried (bounded) while it answers the retriable NotLeader.
int16_t defineSettled(ShardTestClient& cl, const std::string& ch, int64_t sid, const std::string& name) {
    std::optional<ShardTestClient::AnyAnswer> a;
    for (const auto deadline = Clock::now() + 30s; Clock::now() < deadline;) {
        a = cl.awaitAnyDdlAnswer(cl.sendDefineTopic(ch, kClientStream, sid, name, 1, charBlob()), Clock::now() + 10s);
        if (a && a->code != static_cast<int16_t>(pc::ErrorCode::NotLeader)) return a->code;
        std::this_thread::sleep_for(50ms);
    }
    return a ? a->code : int16_t{-1};
}

/// What one catalog subscription received: every record offset, every marker.
struct Seen {
    std::vector<int64_t> offsets;
    std::vector<int64_t> markers;
};
Seen seenBy(const ShardTestClient& cl, int64_t subId) {
    namespace b = rune::protocol::batch;
    Seen s;
    for (const auto& r : cl.records()) {
        if (r.subscriptionId != subId) continue;
        if (r.bytes.empty()) { s.markers.push_back(r.baseOffset); continue; }
        std::size_t pos = 0;
        while (pos + static_cast<std::size_t>(b::kRecordBatchOverhead) <= r.bytes.size()) {
            rune::protocol::RecordBatchReader rd(r.bytes.data() + pos, r.bytes.size() - pos);
            for (int32_t k = 0; k <= rd.lastOffsetDelta(); ++k) s.offsets.push_back(rd.baseOffset() + k);
            pos += static_cast<std::size_t>(rd.totalLength());
        }
    }
    return s;
}
std::vector<int64_t> upTo(int64_t n) {
    std::vector<int64_t> v;
    for (int64_t i = 0; i < n; ++i) v.push_back(i);
    return v;
}
int64_t catalogHw(BrokerNode& n) {
    return n.onAdminThreadForTest([&n] { return n.adminWorkerAgentForTest()->worker().catalogReplica()->highWatermark(); });
}
std::size_t catalogSubs(BrokerNode& n) {
    return n.onAdminThreadForTest([&n] { return n.adminWorkerAgentForTest()->worker().readServer()->subscriptions(); });
}

/// Subscribe on `id` from the start; the subscription id once answered OK (0 = not OK in time).
int64_t subscribeFromStart(ShardTestClient& cl, const ThreeNodes& c, int id, int64_t sid) {
    const int64_t corr = cl.sendCatalogSubscribe(channelOf(c, id), kClientStream, sid, /*earliest=*/-2);
    const auto code = cl.awaitSubscribeCode(corr, Clock::now() + 15s);
    if (!code || *code != 0) return 0;
    return cl.subscriptionIdOf(corr).value_or(0);
}

} // namespace

// RED (before Task 12): a −6 Subscribe was routed like a data partition — no
// broker served the catalog (UnknownTopic / Redirect), so no client could read
// it from a follower.  Now every broker serves its committed prefix.
TEST(CatalogReadBrokerRegression, EveryBrokerServesTheCommittedCatalogThenOneMarkerThenLiveEventsRegression) {
    ThreeNodes c = ThreeNodes::start("rd_every");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t leader = c.catalogLeaderId();
    ASSERT_GT(leader, 0);
    ShardTestClient cl(c.dirs[0]->path / "client-aeron");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersion);
    std::vector<int64_t> sid(4, 0);
    sid[static_cast<std::size_t>(leader)] = connectSettled(cl, c, leader, 20s);
    ASSERT_GT(sid[static_cast<std::size_t>(leader)], 0);
    for (const char* name : {"a", "b"})
        ASSERT_EQ(defineSettled(cl, channelOf(c, leader), sid[static_cast<std::size_t>(leader)], name), 0) << name;
    for (int id = 1; id <= 3; ++id) {
        if (id != leader) sid[static_cast<std::size_t>(id)] = connectSettled(cl, c, id, 20s);
        ASSERT_GT(sid[static_cast<std::size_t>(id)], 0) << id;
        ASSERT_TRUE(c.awaitCatalogHighWatermark(c.node(id), 2, 15s)) << id;
    }

    std::vector<int64_t> sub(4, 0);
    for (int id = 1; id <= 3; ++id) {
        SCOPED_TRACE(id);
        sub[static_cast<std::size_t>(id)] = subscribeFromStart(cl, c, id, sid[static_cast<std::size_t>(id)]);
        ASSERT_NE(sub[static_cast<std::size_t>(id)], 0) << "answered OK on broker " << id << (id == leader ? " (leader)" : " (follower)");
        const int64_t subId = sub[static_cast<std::size_t>(id)];
        ASSERT_TRUE(cl.pollUntil([&] { return !seenBy(cl, subId).markers.empty(); })) << "caught up on broker " << id;
        const Seen s = seenBy(cl, subId);
        ASSERT_EQ(s.markers.size(), 1u);
        EXPECT_GE(s.markers[0], 2);
        EXPECT_LE(s.markers[0], catalogHw(c.node(id))) << "never past the broker's committed HW";
        EXPECT_EQ(s.offsets, upTo(s.markers[0])) << "the whole committed prefix, in order, before the marker";
        EXPECT_EQ(catalogSubs(c.node(id)), 1u);
    }

    // A live event reaches every subscriber — no second marker.
    ASSERT_EQ(defineSettled(cl, channelOf(c, leader), sid[static_cast<std::size_t>(leader)], "live"), 0);
    for (int id = 1; id <= 3; ++id) {
        SCOPED_TRACE(id);
        const int64_t subId = sub[static_cast<std::size_t>(id)];
        const int64_t marker = seenBy(cl, subId).markers.at(0);
        ASSERT_TRUE(cl.pollUntil([&] { return seenBy(cl, subId).offsets.size() > static_cast<std::size_t>(marker); }))
            << "the live event on broker " << id;
        const Seen s = seenBy(cl, subId);
        EXPECT_EQ(s.offsets[static_cast<std::size_t>(marker)], marker);
        EXPECT_EQ(s.markers.size(), 1u) << "one marker per subscription";
    }

    // A session's close ends its catalog subscriptions on the admin thread.
    for (int id = 1; id <= 3; ++id)
        ASSERT_TRUE(cl.disconnect(channelOf(c, id), kClientStream, sid[static_cast<std::size_t>(id)]));
    for (int id = 1; id <= 3; ++id) {
        bool freed = false;
        for (const auto deadline = Clock::now() + 15s; Clock::now() < deadline && !freed;) {
            freed = catalogSubs(c.node(id)) == 0;
            if (!freed) std::this_thread::sleep_for(20ms);
        }
        EXPECT_TRUE(freed) << "broker " << id << " freed the closed session's catalog subscription";
    }
}

// A broker whose catalog apply HALTED keeps serving what it holds committed —
// a client reading the catalog gets the committed prefix from it too (it never
// answers a read Internal), then the marker at its own HW.
TEST(CatalogReadBrokerRegression, HaltedBrokerStillServesItsCommittedCatalogRegression) {
    ThreeNodes c = ThreeNodes::start("rd_halted");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t leader = c.catalogLeaderId();
    ASSERT_GT(leader, 0);
    const int32_t follower = leader == 1 ? 2 : 1;
    ShardTestClient cl(c.dirs[0]->path / "client-aeron");
    cl.keepSessionsAlive(true);
    cl.setClientVersion(pc::kClientVersion);
    const int64_t onLeader = connectSettled(cl, c, leader, 20s);
    ASSERT_GT(onLeader, 0);
    ASSERT_EQ(defineSettled(cl, channelOf(c, leader), onLeader, "before"), 0);

    BrokerNode& halted = c.node(follower);
    halted.onAdminThreadForTest([&halted] {
        halted.adminWorkerAgentForTest()->worker().applierForTest().tamperNextFingerprintForTest();
        return 0;
    });
    ASSERT_EQ(defineSettled(cl, channelOf(c, leader), onLeader, "trigger"), 0);
    bool isHalted = false;
    for (const auto deadline = Clock::now() + 15s; Clock::now() < deadline && !isHalted;) {
        isHalted = halted.onAdminThreadForTest(
            [&halted] { return halted.adminWorkerAgentForTest()->worker().roleName() == "Halted"; });
        if (!isHalted) std::this_thread::sleep_for(20ms);
    }
    ASSERT_TRUE(isHalted);
    // A follower learns the HW on its next fetch: both events are known committed there.
    ASSERT_TRUE(c.awaitCatalogHighWatermark(halted, 2, 15s));

    const int64_t onHalted = connectSettled(cl, c, follower, 20s);
    ASSERT_GT(onHalted, 0);
    const int64_t subId = subscribeFromStart(cl, c, follower, onHalted);
    ASSERT_NE(subId, 0) << "a halted broker answers a catalog read OK";
    ASSERT_TRUE(cl.pollUntil([&] { return !seenBy(cl, subId).markers.empty(); }));
    const Seen s = seenBy(cl, subId);
    ASSERT_EQ(s.markers.size(), 1u);
    EXPECT_GE(s.markers[0], 2) << "both committed events are served, the tampered one included (it is committed)";
    EXPECT_LE(s.markers[0], catalogHw(halted));
    EXPECT_EQ(s.offsets, upTo(s.markers[0]));
}
