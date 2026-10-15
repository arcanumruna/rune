// ClientDdlClusterRegressionTest.cpp — catalog history Task 22 (F7, binding B2): the REAL client's blocking
// *AndWait DDL and catalog reads across THREE real in-process BrokerNodes over Aeron UDP — what neither
// the fake broker nor one node can exercise: a follower's Redirect followed, a leader crashed mid-DDL, a
// catalog below its minIsr, and a read of __catalog whose broker lost its history.
//
// Test-only seams (listed in the Task 22 report): BrokerNode::crashForTest (a REAL crash: no controlled
// shutdown), BrokerNode::holdCatalogAnswersForTest (the leader's DDL is committed but never answered),
// ThreeNodes::killCatalogLeaderAfterNextAppend (a watcher crashes the leader right after its committed append).
#include "client/ClusterClient.h"
#include "support/DdlHarness.h"          // charBlob / viewPlanBlob
#include "support/ShardTestClient.h"

#include <rune/node/AdminWorker.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/catalog/CatalogQueries.h>
#include <rune/catalog/CatalogReplayer.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node::test::catalog_cluster;
namespace rcat = rune::catalog;

namespace {
constexpr auto kDisk = rune::client::Persistence::Disk;
}

// F7: the blocking client API through a FOLLOWER: the DDL is Redirected to the catalog leader and the
// *AndWait returns the leader's answer; every broker ends with the same catalog.
TEST(ClientDdlClusterRegression, AndWaitThroughAFollowerFollowsTheRedirect) {
    ThreeNodes c = ThreeNodes::start("cc_redirect");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t follower = aFollowerOfTheCatalog(c);
    ASSERT_GT(follower, 0);
    auto client = connectClient(c, follower);
    const auto r = client->defineTopicAndWait("t", 1, "", {}, false, kDisk, {}, {}, 20s);
    EXPECT_EQ(r.errorCode, 0) << r.detail;
    EXPECT_FALSE(r.requestId.none());
    std::string why;
    EXPECT_TRUE(c.awaitCatalogDigestsEqual(30s, &why)) << why;
    EXPECT_EQ(c.countEventsWithRequestId(r.requestId), 1u);
}

// F7 + spec §5.2: the catalog leader is CRASHED right after it committed the DDL's event and before it
// answered (the client is in doubt).  The client's session to it is lost, the DDL is re-sent toward its
// seed (a surviving broker), which redirects to the new leader; the SAME requestId dedupes there to the
// original result — success, not TopicExists, and exactly one event on every broker (the crashed leader
// included, once it rejoins).
// (The client is seeded on a FOLLOWER: a client seeded on the broker that dies has no live seed to re-send
// toward - Client::driveDdl_ re-sends a lost session's DDL "toward the seed".)
TEST(ClientDdlClusterRegression, LeaderKilledMidDdlTheRetryGetsTheOriginalResultOnce) {
    ThreeNodes c = ThreeNodes::start("cc_kill");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t follower = aFollowerOfTheCatalog(c);
    ASSERT_GT(follower, 0);
    auto cfg = clientConfigFor(c, follower);
    cfg.ddlTimeoutMs   = 90'000;
    cfg.ddlRetryBudget = 80s;
    auto client = connectClient(c, cfg);
    ASSERT_EQ(client->defineTopicAndWait("warm", 1, "", {}, false, kDisk, {}, {}, 60s).errorCode, 0);

    rune::client::DdlOptions o; o.requestId = {.hi = 0xC11E, .lo = 77};
    c.killCatalogLeaderAfterNextAppend();                         // test seam: crash the leader after its committed append
    const auto r = client->defineTopicAndWait("t", 1, "", {}, false, kDisk, {}, o, 90s);
    const int32_t killed = c.reapKilledLeader();
    ASSERT_GT(killed, 0) << "the leader was crashed after the DDL's append";
    EXPECT_EQ(r.errorCode, 0) << "NotLeader/in-doubt retried within the budget, deduplicated on the new leader: " << r.detail;
    EXPECT_EQ(r.requestId, o.requestId);
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    std::string why;
    ASSERT_TRUE(c.awaitCatalogDigestsEqual(30s, &why)) << why;
    EXPECT_EQ(c.countEventsWithRequestId(o.requestId), 1u);
    c.startNode(killed);                                           // the crashed leader rejoins as a follower
    ASSERT_TRUE(c.awaitCatalogDigestsEqual(60s, &why)) << why;
    EXPECT_EQ(c.countEventsWithRequestId(o.requestId), 1u) << "once on every broker, the rejoined one included";
    EXPECT_EQ(c.runningNodes().size(), 3u);
}

// F7 + spec §5.5: below the catalog's minIsr after a REAL loss of both followers, the leader cannot shrink its ISR (the
// shrink needs the metadata quorum): the DDL's event never commits, the leader answers the retriable NotLeader when its commit
// timeout expires (F13, AdminWorker::ddlTimeoutNs, 10 s), and the blocking DDL fails EXPLICITLY with NotLeader once the client's
// retry budget is spent - before the client's own DDL deadline, never a hang, never the Timeout of a silent broker.
// (The immediate-refusal branch - the ISR already below minIsr at the append, AdminWorker.cpp ~745 - is reachable only by a
// graceful exit; it is covered by CatalogLeadershipTest / AdminWorkerTest "below minIsr".)
// RED (the F13 commit-timeout answer disabled): the client sees no answer and throws Timeout at its 20 s DDL deadline.
TEST(ClientDdlClusterRegression, BelowMinIsrAndWaitFailsExplicitlyWithNotLeader) {
    ThreeNodes c = ThreeNodes::start("cc_minisr");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    // The controller (consensus leader) leads the catalog (ruling 7: it prefers itself).  Waited for - never assumed
    // (a slower build elects in another order first).
    int32_t leader = -1;
    for (const auto deadline = std::chrono::steady_clock::now() + 30s; std::chrono::steady_clock::now() < deadline && leader < 0;) {
        int32_t ctl = -1;
        for (rune::node::BrokerNode* n : c.live()) if (n->isLeader()) ctl = n->nodeIdForTest();
        if (ctl > 0 && c.settledLeader() == ctl) leader = ctl;
        else std::this_thread::sleep_for(20ms);
    }
    ASSERT_GT(leader, 0) << "the controller leads the catalog";
    auto warm = connectClient(c, leader);
    ASSERT_EQ(warm->defineTopicAndWait("warm", 1, "", {}, false, kDisk, {}, {}, 60s).errorCode, 0);
    auto cfg = clientConfigFor(c, leader);
    cfg.ddlTimeoutMs   = 20'000;
    cfg.ddlRetryBudget = 1500ms;
    auto client = connectClient(c, cfg);
    for (int n : c.runningNodes()) if (n != leader) c.crash(n);   // a REAL loss: the ISR can never shrink without a quorum

    const auto t0 = std::chrono::steady_clock::now();
    rune::client::DefineTopicResult r;
    EXPECT_NO_THROW(r = client->defineTopicAndWait("t", 1, "", {}, false, kDisk, {}, {}, 30s));
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_EQ(r.errorCode, 2) << "an explicit NotLeader failure once the budget is spent (spec §5.5): " << r.detail;
    EXPECT_NE(r.detail.find("budget exhausted"), std::string::npos) << r.detail;
    const auto commitTimeout = std::chrono::nanoseconds(rune::node::AdminWorker::ddlTimeoutNs());
    EXPECT_GE(took, commitTimeout - 1s) << "answered by the broker's F13 commit timeout, not earlier";
    EXPECT_LT(took, commitTimeout + 1500ms + 3s) << "ends before the client's 20 s DDL deadline (the broker answered)";
}

// Task 22 fix round 1 (M5): the client whose SEED dies with its DDL in flight.  The catalog leader commits the DDL and never
// answers it, then is crashed; the client is seeded on that very broker, so its re-send toward the seed has nowhere to go: it
// ends EXPLICITLY (SessionLost, or NotLeader) inside its retry budget - never a hang to the operation timeout - and the same
// requestId sent through a surviving broker gets the original result, exactly one event on every broker.
TEST(ClientDdlClusterRegression, SeedLeaderCrashedMidDdlFailsExplicitlyAndTheRetryElsewhereGetsTheResultRegression) {
    ThreeNodes c = ThreeNodes::start("cc_seed");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t leader = c.settledLeader();
    ASSERT_GT(leader, 0);
    auto cfg = clientConfigFor(c, leader);
    cfg.ddlRetryBudget = 3s;
    cfg.ddlTimeoutMs   = 40'000;
    auto a = connectClient(c, cfg);
    ASSERT_EQ(a->defineTopicAndWait("warm", 1, "", {}, false, kDisk, {}, {}, 60s).errorCode, 0);

    rune::client::DdlOptions o; o.requestId = {.hi = 0xC11E, .lo = 91};
    c.node(leader).holdCatalogAnswersForTest(true);          // the leader commits the DDL and never answers it
    c.killCatalogLeaderAfterNextAppend();                    // ... then dies
    const auto t0 = std::chrono::steady_clock::now();
    bool threw = false;
    int code = -1;
    std::string detail;
    try {
        const auto r = a->defineTopicAndWait("t", 1, "", {}, false, kDisk, {}, o, 30s);
        code = r.errorCode; detail = r.detail;
    } catch (const std::exception& e) {
        threw = true; detail = e.what();
    }
    const auto took = std::chrono::steady_clock::now() - t0;
    const int32_t killed = c.reapKilledLeader();
    ASSERT_EQ(killed, leader) << "the leader was crashed after the DDL's append";
    EXPECT_TRUE((threw && detail.find("session") != std::string::npos) || code == 2)
        << "an explicit failure (SessionLost, or NotLeader), threw=" << threw << " code=" << code << ": " << detail;
    // The client notices a dead seed by its session going stale (SessionMachine responseTimeoutNs, 10 s of silence), THEN its
    // retry budget runs: the explicit failure comes at ~13 s, never at the 30 s operation timeout.
    EXPECT_LT(took, 10s + cfg.ddlRetryBudget + 2s) << "ends inside the stale-session threshold + the retry budget, never at the 30 s operation timeout";

    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    int32_t survivor = -1;
    for (int n : c.runningNodes()) survivor = n;
    ASSERT_GT(survivor, 0);
    auto bcfg = clientConfigFor(c, survivor);
    bcfg.ddlTimeoutMs = 90'000; bcfg.ddlRetryBudget = 80s;
    auto b = connectClient(c, bcfg);
    const auto rb = b->defineTopicAndWait("t", 1, "", {}, false, kDisk, {}, o, 90s);
    EXPECT_EQ(rb.errorCode, 0) << "the same requestId through a survivor gets the original result, not TopicExists: " << rb.detail;
    EXPECT_EQ(rb.requestId, o.requestId);
    std::string why;
    ASSERT_TRUE(c.awaitCatalogDigestsEqual(60s, &why)) << why;
    EXPECT_EQ(c.countEventsWithRequestId(o.requestId), 1u);
    c.startNode(killed);
    ASSERT_TRUE(c.awaitCatalogDigestsEqual(90s, &why)) << why;
    EXPECT_EQ(c.countEventsWithRequestId(o.requestId), 1u) << "once on every broker, the rejoined one included";
}

// Task 22 fix round 1 (C1): the REAL client reads a view row through EVERY broker.  Only the broker that leads the view's source
// partition serves the row; every other broker answers a Redirect to it, which the client follows within its budget
// (Client::isRoutedCtlOp).  Without the routing the non-source brokers' reads end in a Timeout (the ErrorResponse was dropped as a
// data-op Redirect) or a wrong AbsentKey.
// RED (isRoutedCtlOp reverted in the client's ErrorResponse branch): getViewRowAndWait fails on the non-source brokers.
namespace {
[[nodiscard]] std::vector<uint8_t> characterBatch(int64_t id, int64_t hp) {
    std::string key(8, '\0');
    for (int i = 0; i < 8; ++i) key[static_cast<std::size_t>(i)] = static_cast<char>(uint64_t(id) >> (8 * i));
    std::string value;
    auto le = [&](uint64_t v, int n) { for (int i = 0; i < n; ++i) value.push_back(static_cast<char>(v >> (8 * i))); };
    le(16, 2); le(1, 2); le(static_cast<uint64_t>(id), 8); le(static_cast<uint64_t>(hp), 8);   // a v1 character: {id, hp}
    rune::protocol::RecordBatchBuilder bb(0, 0);
    bb.append(key, value, 0);
    return bb.build();
}
/// Produce acks=all through the broker that leads (topic, 0) until it is acked (bounded).
bool produceCharacter(ThreeNodes& c, int32_t topicId, int64_t id, int64_t hp) {
    rune::node::test::ShardTestClient cl(c.clientsDir->path / "produce");
    for (const auto deadline = std::chrono::steady_clock::now() + 60s; std::chrono::steady_clock::now() < deadline;) {
        const int via = c.runningNodes().front();
        const int64_t s0 = cl.connect(c.clientChannel(via), rune::node::test::catalog_cluster::kClientStream);
        int leaderId = -1;
        if (s0 > 0)
            for (const auto& pi : cl.metadata(c.clientChannel(via), rune::node::test::catalog_cluster::kClientStream, s0))
                if (pi.topicId == topicId && pi.partition == 0) leaderId = pi.leaderId;
        if (leaderId > 0) {
            const std::string ch = c.clientChannel(leaderId);
            const int64_t sid = cl.connect(ch, rune::node::test::catalog_cluster::kClientStream);
            const int64_t corr = cl.nextCorrelationId();
            if (sid > 0 && cl.produce(ch, rune::node::test::catalog_cluster::kClientStream, sid, corr, topicId, 0, /*acks=All*/ 1,
                                      characterBatch(id, hp))) {
                cl.pollUntil([&] { return cl.ackFor(corr).has_value(); });
                if (const auto ack = cl.ackFor(corr); ack && ack->errorCode == 0) return true;
            }
        }
        std::this_thread::sleep_for(100ms);
    }
    return false;
}
[[nodiscard]] int64_t hpOfRow(const std::vector<uint8_t>& row) {   // [bitmap 1 B][id i64][hp i64]
    if (row.size() != 17) return -1;
    int64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<int64_t>(row[9 + static_cast<std::size_t>(i)]) << (8 * i);
    return v;
}
}  // namespace

TEST(ClientDdlClusterRegression, GetViewRowThroughEveryBrokerFollowsTheRedirectRegression) {
    ThreeNodes c = ThreeNodes::start("cc_gvr");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t leader = c.settledLeader();
    auto admin = connectClient(c, leader);
    const auto src = admin->defineTopicAndWait("src", 1, "id", rune::node::test::charBlob(), false, kDisk, {}, {}, 60s);
    ASSERT_EQ(src.errorCode, 0) << src.detail;
    const auto view = admin->createViewAndWait("v", "id", 0, rune::node::test::viewPlanBlob("v", src.topicId, 0), {}, 60s);
    ASSERT_EQ(view.errorCode, 0) << view.detail;
    ASSERT_TRUE(produceCharacter(c, src.topicId, 101, 7));

    for (int n : c.runningNodes()) {
        auto client = connectClient(c, n);
        rune::client::ViewRowResult r;
        for (const auto deadline = std::chrono::steady_clock::now() + 60s; std::chrono::steady_clock::now() < deadline;) {
            try {
                r = client->getViewRowAndWait(view.viewTopicId, 101, 0, 10s);
            } catch (const std::exception& e) {
                ADD_FAILURE() << "getViewRowAndWait through node " << n << " threw: " << e.what();
                break;
            }
            const bool retriable = r.status == rune::client::ViewRowResult::kNotReady ||
                                   r.status == rune::client::ViewRowResult::kUnknownView ||
                                   r.status == rune::client::ViewRowResult::kNotLeader;
            if (!retriable) break;
            std::this_thread::sleep_for(50ms);
        }
        EXPECT_EQ(r.status, rune::client::ViewRowResult::kOk) << "GetViewRow through node " << n;
        EXPECT_EQ(hpOfRow(r.rowBytes), 7) << "node " << n;
        const auto absent = client->getViewRowAndWait(view.viewTopicId, 999, 0, 10s);
        EXPECT_EQ(absent.status, rune::client::ViewRowResult::kAbsentKey) << "an absent key through node " << n;
    }
}

// Binding B2 (Task 13): the client's catalog cache across a leader crash and a broker that lost its history.
// The cache is (prefix, the serving broker's history epoch) (CatalogReader): a stream whose epoch differs
// (a wiped / truncated-below-served broker draws a new one) drops the cache and re-reads from offset 0.
//   1. the reader (seeded on follower F) caches the history, epoch E;
//   2. the leader is crashed mid-DDL: the reader's next read CONTINUES the cache (F's history only grew:
//      same epoch, no re-read) and equals the library's replay of F's own log;
//   3. F's disk is lost (stopped, wiped, restarted): it refetches the committed history from the new leader
//      under a NEW epoch; the reader's cache is dropped and fully re-read from 0 - never served stale -
//      and equals the replay of the cluster's committed log.
// (M4) The content comparison below cannot tell a stale history from a fresh one - the cache prefix equals the truth - so the
// DISCRIMINATOR is the epoch stats (epochChanges >= 1, the last accepted-from offset 0).  A truly stale history cannot be served
// on a clean cluster: the read server never serves above its high watermark (CatalogReadServer.cpp:179-183) and the epoch
// changes only when the committed history is cut below the served floor (CatalogReadServer.cpp:356-373), i.e. with lost or
// replaced history - which is exactly what step 3 provides.  (The read is idle, not literally mid-read, while the leader crashes.)
TEST(ClientDdlClusterRegression, CatalogHistoryTruncatedMidReadIsReReadNotServedStaleRegression) {
    ThreeNodes c = ThreeNodes::start("cc_hist");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t F = aFollowerOfTheCatalog(c);
    ASSERT_GT(F, 0);
    auto writerCfg = clientConfigFor(c, F);
    writerCfg.ddlTimeoutMs = 90'000; writerCfg.ddlRetryBudget = 80s;
    auto writer = connectClient(c, writerCfg);
    ASSERT_EQ(writer->defineTopicAndWait("t1", 1, "", {}, false, kDisk, {}, {}, 60s).errorCode, 0);
    ASSERT_EQ(writer->defineTopicAndWait("t2", 1, "", {}, false, kDisk, {}, {}, 60s).errorCode, 0);

    auto readerCfg = clientConfigFor(c, F);
    readerCfg.reconnectForeverAfterLive = true;
    auto reader = connectClient(c, readerCfg);
    ASSERT_TRUE(c.awaitCatalogDigestsEqual(30s));
    auto h1 = reader->catalogHistory({}, 30s);
    ASSERT_TRUE(h1.has_value()) << h1.error().detail;
    EXPECT_EQ(h1->size(), 2u);
    const auto epoch0 = reader->catalogReader().historyEpoch();
    ASSERT_TRUE(epoch0.has_value());
    EXPECT_EQ(reader->catalogReader().stats().epochChanges, 0u);

    // 2. the leader crashed mid-DDL; the read continues the cache under the same epoch.
    rune::client::DdlOptions o; o.requestId = {.hi = 0xB2, .lo = 1};
    c.killCatalogLeaderAfterNextAppend();
    const auto t3 = writer->defineTopicAndWait("t3", 1, "", {}, false, kDisk, {}, o, 90s);
    const int32_t killed = c.reapKilledLeader();
    ASSERT_GT(killed, 0);
    ASSERT_EQ(t3.errorCode, 0) << t3.detail;
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    std::string why;
    ASSERT_TRUE(c.awaitCatalogDigestsEqual(30s, &why)) << why;
    auto h2 = reader->catalogHistory({}, 30s);
    ASSERT_TRUE(h2.has_value()) << h2.error().detail;
    ASSERT_EQ(h2->size(), 3u);
    EXPECT_EQ(h2->back().event.principal.requestId, o.requestId);
    EXPECT_EQ(reader->catalogReader().historyEpoch(), epoch0) << "F's history only grew: the same epoch";
    EXPECT_EQ(reader->catalogReader().stats().epochChanges, 0u);

    // 3. F loses its disk: a new history epoch, the same committed history.
    c.startNode(killed);
    ASSERT_TRUE(c.awaitCatalogDigestsEqual(60s, &why)) << why;
    c.stop(F);
    c.wipe(F);
    c.startNode(F);
    ASSERT_TRUE(c.awaitCatalogDigestsEqual(60s, &why)) << why;
    std::optional<std::vector<rcat::RecordedEvent>> h3;
    std::string lastErr;
    for (const auto deadline = std::chrono::steady_clock::now() + 90s;
         std::chrono::steady_clock::now() < deadline && !h3;) {
        try {
            auto r = reader->catalogHistory({}, 20s);
            if (r.has_value()) h3 = std::move(*r); else lastErr = r.error().detail;
        } catch (const std::exception& e) { lastErr = e.what(); }
        if (!h3) std::this_thread::sleep_for(200ms);
    }
    ASSERT_TRUE(h3.has_value()) << "the reader reconnects to the restarted F: " << lastErr;
    const auto truth = c.catalogEventsOn(F);
    ASSERT_EQ(h3->size(), truth.size());
    for (std::size_t i = 0; i < truth.size(); ++i) {
        EXPECT_EQ((*h3)[i].offset, truth[i].offset);
        EXPECT_EQ((*h3)[i].event, truth[i].event) << "event " << i << " exactly as the cluster committed it";
    }
    EXPECT_GE(reader->catalogReader().stats().epochChanges, 1u) << "the cache was dropped, not continued";
    EXPECT_NE(reader->catalogReader().historyEpoch(), epoch0);
    const auto offsets = c.node(F).onAdminThreadForTest([&] {
        const auto* rs = c.node(F).adminWorkerAgentForTest()->worker().readServer();
        return rs != nullptr ? rs->acceptedFromOffsetsForTest() : std::vector<int64_t>{};
    });
    // The client resumes from its cache end (3); the restarted broker's stream names a NEW epoch, so the
    // client drops the cache and subscribes again from offset 0: a full re-read (never the stale prefix).
    ASSERT_GE(offsets.size(), 2u);
    EXPECT_EQ(offsets.front(), 3) << "resumed from the cache end first";
    EXPECT_EQ(offsets.back(), 0) << "the restarted broker was then read from offset 0: a full re-read";
}
