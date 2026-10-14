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
#include <tuple>
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

// Fix round 0 (Task 12 ruling 1): a catalog reader SLOWER than its egress
// window is PAUSED (its subscription keeps its own cursor; nothing queues for
// it) and resumed when space returns — never expired for being slow (that would
// kill the client's other subscriptions and produce).  It reads a catalog many
// times larger than the window + the session spill quota to the end, its
// session's spill never grows, and a FAST reader on another session is not held
// back.  RED: the slow session's spill filled and the gateway expired it
// (SessionExpired) — its catalog read never reached the end.
TEST(CatalogReadBrokerRegression, SlowCatalogReaderIsPausedNeverExpiredAndAFastReaderIsUnaffectedRegression) {
    ThreeNodes c = ThreeNodes::start("rd_slow");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t leader = c.catalogLeaderId();
    ASSERT_GT(leader, 0);
    BrokerNode& node = c.node(leader);
    ShardTestClient fast(c.dirs[0]->path / "fast-aeron");
    fast.keepSessionsAlive(true);
    fast.setClientVersion(pc::kClientVersion);
    const int64_t sidF = connectSettled(fast, c, leader, 20s);
    ASSERT_GT(sidF, 0);
    ASSERT_EQ(defineSettled(fast, channelOf(c, leader), sidF, "cfg"), 0);
    // ~4 MB of catalog: 40 events of ~100 KiB (the statement text).
    const std::string statement(100 * 1024, 's');
    for (int i = 0; i < 40; ++i) {
        const int64_t corr = fast.sendAlterTopicConfig(channelOf(c, leader), kClientStream, sidF, "cfg", 0,
                                                       {{1, 60'000 + i}}, {}, statement);
        const auto a = fast.awaitDdl(corr, Clock::now() + 15s);
        ASSERT_TRUE(a.has_value()) << i;
        ASSERT_EQ(a->errorCode, 0) << i << " " << a->detail;
    }
    const int64_t hw = catalogHw(node);
    ASSERT_GE(hw, 41);
    const auto quota = rune::gateway::ClientGateway::spillSizing().sessionQuota;
    ASSERT_GT(static_cast<std::size_t>(hw) * statement.size(), 6 * quota) << "a catalog several times the buffer";

    ShardTestClient slow(c.dirs[0]->path / "slow-aeron");
    slow.keepSessionsAlive(true);
    slow.setClientVersion(pc::kClientVersion);
    const int64_t sidS = connectSettled(slow, c, leader, 20s);
    ASSERT_GT(sidS, 0);
    const int64_t corrS = slow.sendCatalogSubscribe(channelOf(c, leader), kClientStream, sidS, -2);
    const int64_t corrF = fast.sendCatalogSubscribe(channelOf(c, leader), kClientStream, sidF, -2);

    std::size_t maxSpill = 0;
    bool slowAlive = true;
    std::optional<Clock::time_point> fastDone, slowDone;
    const auto start = Clock::now();
    auto caughtUp = [](const ShardTestClient& cl, std::optional<int64_t> id) {
        return id && !seenBy(cl, *id).markers.empty();
    };
    auto lastSample = Clock::now();
    uint64_t tick = 0;
    for (const auto deadline = start + 90s; Clock::now() < deadline && !(fastDone && slowDone);) {
        if (++tick % 4 == 0) slow.pollThrottled(1);   // 1 fragment / ~4 ms: far slower than the broker pushes
        fast.pollThrottled(4096);
        if (!fastDone && caughtUp(fast, fast.subscriptionIdOf(corrF))) fastDone = Clock::now();
        if (!slowDone && caughtUp(slow, slow.subscriptionIdOf(corrS))) slowDone = Clock::now();
        if (Clock::now() - lastSample > 20ms) {
            lastSample = Clock::now();
            const auto [spill, alive] = node.onControlThreadForTest([&node, sidS] {
                auto* g = node.gatewayForTest();
                return std::pair{g->spilledBytesForTest(sidS), g->sessionExistsForTest(sidS)};
            });
            maxSpill = std::max(maxSpill, spill);
            slowAlive = slowAlive && alive;
        }
        std::this_thread::sleep_for(1ms);
    }
    for (const auto& e : slow.errors())
        EXPECT_NE(e.code, static_cast<int16_t>(pc::ErrorCode::SessionExpired)) << "a slow reader is never expired";
    EXPECT_TRUE(slowAlive) << "the slow session stayed";
    ASSERT_TRUE(fastDone.has_value()) << "the fast reader caught up";
    ASSERT_TRUE(slowDone.has_value()) << "the slow reader read the whole catalog to the end";
    const Seen s = seenBy(slow, *slow.subscriptionIdOf(corrS));
    EXPECT_EQ(s.markers.size(), 1u);
    EXPECT_EQ(s.offsets, upTo(s.markers.at(0))) << "every record once, in order, to the end";
    EXPECT_GE(s.markers.at(0), hw);
    const Seen f = seenBy(fast, *fast.subscriptionIdOf(corrF));
    EXPECT_EQ(f.offsets, upTo(f.markers.at(0)));
    EXPECT_LE(maxSpill, 4u * 1024u) << "the slow session's spill never grew (catalog frames never spill)";
    EXPECT_LT(*fastDone, *slowDone) << "the fast reader is not held back by the slow one";
    EXPECT_LT(*fastDone - start, 10s) << "the fast reader is not held back by the slow one";
    const auto [pauses, resumes, paused] = node.onControlThreadForTest([&node] {
        const auto& f = node.catalogReadFlowForTest();
        return std::tuple{f.pauses(), f.resumes(), f.paused()};
    });
    EXPECT_GT(pauses, 0u) << "non-vacuous: the slow reader was back-pressured (paused)";
    EXPECT_EQ(resumes, pauses) << "every pause resumed";
    EXPECT_EQ(paused, 0u) << "nothing left paused";
}
