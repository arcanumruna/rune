// ClientDdlClusterRegressionTest.cpp — catalog history Task 22 (F7, binding B2): the REAL client's blocking
// *AndWait DDL and catalog reads across THREE real in-process BrokerNodes over Aeron UDP — what neither
// the fake broker nor one node can exercise: a follower's Redirect followed, a leader crashed mid-DDL, a
// catalog below its minIsr, and a read of __catalog whose broker lost its history.
//
// Test-only seams (listed in the Task 22 report): BrokerNode::crashForTest (a REAL crash: no controlled
// shutdown), BrokerNode::holdCatalogAnswersForTest (the leader's DDL is committed but never answered),
// ThreeNodes::killCatalogLeaderAfterNextAppend (a watcher crashes the leader right after its committed append).
#include "client/ClusterClient.h"

#include <rune/catalog/CatalogQueries.h>
#include <rune/catalog/CatalogReplayer.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

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

// F7 + spec §5.5: below the catalog's minIsr the blocking DDL fails EXPLICITLY (NotLeader) when the client's
// retry budget is spent - never a hang, never the 30 s operation timeout.
TEST(ClientDdlClusterRegression, BelowMinIsrAndWaitFailsExplicitlyWithNotLeader) {
    ThreeNodes c = ThreeNodes::start("cc_minisr");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    // The controller (consensus leader) leads the catalog (ruling 7: it prefers itself): the lone survivor below keeps
    // deciding the ISR shrink.  Waited for - never assumed (a slower build elects in another order first).
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
    cfg.ddlRetryBudget = std::chrono::milliseconds(1500);
    auto client = connectClient(c, cfg);
    for (int n : c.runningNodes()) if (n != leader) c.stop(n);
    // The leader learns its followers are gone (their publications close: its ISR shrinks below minIsr at once on
    // a fast build; a sanitizer build may take its replicaLagMaxMs, or - with no consensus quorum left to decide the
    // shrink - never).  A DDL sent while the ISR still names them waits for a commit that cannot come.
    const bool shrank = c.awaitCatalogIsrSizeOn(leader, 1, 20s);
    const auto t0 = std::chrono::steady_clock::now();
    bool explicitFailure = false;
    int code = -1;
    std::string detail;
    try {
        const auto r = client->defineTopicAndWait("t", 1, "", {}, false, kDisk, {}, {}, 30s);
        code = r.errorCode; detail = r.detail;
        explicitFailure = r.errorCode == 2;
    } catch (const std::exception& e) {
        detail = e.what();
        explicitFailure = detail.find("timed out") != std::string::npos;   // the client's DDL deadline: Timeout, explicit
    }
    const auto took = std::chrono::steady_clock::now() - t0;
    if (shrank) {
        // Below minIsr the append is refused at once: the budget (1.5 s), not the 30 s operation timeout, ends it.
        EXPECT_EQ(code, 2) << "an explicit NotLeader failure once the budget is spent (spec §5.5): " << detail;
        EXPECT_LT(took, 10s) << "the budget, not the 30 s op timeout, ends it";
    } else {
        // The ISR never shrank (no quorum to decide it): the leader answers NotLeader when the event does not commit
        // (10 s), the client fails at its budget / DDL deadline (15 s) - explicit either way, never a hang.
        std::fprintf(stderr, "[ minIsr ] the lone leader's ISR never shrank: explicit failure path (code %d: %s) after %ld ms\n",
                     code, detail.c_str(), (long)std::chrono::duration_cast<std::chrono::milliseconds>(took).count());
        EXPECT_TRUE(explicitFailure) << "code " << code << ": " << detail;
        EXPECT_LT(took, 25s) << "ends at the client's DDL deadline, never the 30 s op timeout";
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
