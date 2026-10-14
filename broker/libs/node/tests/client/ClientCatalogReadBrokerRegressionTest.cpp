// ClientCatalogReadBrokerRegressionTest.cpp — catalog history Task 13 (F7): the
// REAL client's blocking catalogHistory / catalogAsOf / catalogDiff against ONE
// REAL in-process BrokerNode over Aeron UDP (Thread runner).  The client reads
// __catalog with Subscribe(-6) from its cache end until the broker's caught-up
// marker and replays it with the shared verifying library; every expected value
// comes from replaying the broker's OWN log with that library — never hand-typed.
#include "client/OneBroker.h"

#include <rune/catalog/CatalogQueries.h>

#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <vector>

using namespace std::chrono_literals;
using rune::node::test::OneBroker;
namespace rcat = rune::catalog;

TEST(ClientCatalogReadBrokerRegression, HistoryAsOfAndDiffMatchTheLibraryOverTheBrokersLog) {
    OneBroker b("cr_all");
    auto client = b.connectClient();
    b.runSchemaLifecycle(*client, "players");   // define typed, alter add, alter rename, set compat (via *AndWait)
    ASSERT_EQ(client->defineTopicAndWait("events", 2, "", {}, false, rune::client::Persistence::Disk, {}, {}, 10s)
                  .errorCode,
              0);
    const auto log = b.catalogEvents();   // RecordedEvents decoded from the node's __catalog
    ASSERT_EQ(log.size(), 5u);

    const rcat::HistoryFilter players{.type = rcat::ObjectType::Schema, .name = "players"};
    auto h = client->catalogHistory(players, 10s);
    ASSERT_TRUE(h.has_value()) << h.error().detail;
    const auto want = rcat::history(log, players);
    ASSERT_EQ(h->size(), want.size());
    EXPECT_EQ(h->size(), 4u);
    for (std::size_t i = 0; i < want.size(); ++i) {
        EXPECT_EQ((*h)[i].offset, want[i]->offset);
        EXPECT_EQ((*h)[i].event, want[i]->event) << "event " << i << " exactly as the broker wrote it";
    }

    auto s = client->catalogAsOf({.by = rcat::AsOf::By::Offset, .value = 2}, 10s);
    ASSERT_TRUE(s.has_value()) << s.error().detail;
    EXPECT_EQ(s->digest(), rcat::stateAsOf(log, {.by = rcat::AsOf::By::Offset, .value = 2})->digest());

    auto d = client->catalogDiff({.by = rcat::AsOf::By::Offset, .value = 0},
                                 {.by = rcat::AsOf::By::Offset, .value = static_cast<int64_t>(log.size()) - 1}, 10s);
    ASSERT_TRUE(d.has_value()) << d.error().detail;
    EXPECT_FALSE(d->empty());
    const auto wantDiff = rcat::diff(*rcat::stateAsOf(log, {.by = rcat::AsOf::By::Offset, .value = 0}),
                                     *rcat::stateAsOf(log, {.by = rcat::AsOf::By::Offset,
                                                            .value = static_cast<int64_t>(log.size()) - 1}));
    ASSERT_EQ(d->size(), wantDiff.size());
    for (std::size_t i = 0; i < wantDiff.size(); ++i) {
        EXPECT_EQ((*d)[i].name, wantDiff[i].name);
        EXPECT_EQ((*d)[i].change, wantDiff[i].change);
        EXPECT_EQ((*d)[i].fields, wantDiff[i].fields);
    }
    // The whole cache is the broker's log, replayed by the same library.
    rcat::CatalogReplayer lib;
    for (const auto& e : log) ASSERT_TRUE(lib.feedDecoded(e.offset, e.event));
    EXPECT_EQ(client->catalogReader().state().digest(), lib.state().digest());
}

TEST(ClientCatalogReadBrokerRegression, ASecondReadSeesNewDdlFromTheCachedPrefix) {
    OneBroker b("cr_tail");
    auto client = b.connectClient();
    ASSERT_EQ(client->defineTopicAndWait("a", 1, "", {}, false, rune::client::Persistence::Disk, {}, {}, 10s).errorCode,
              0);
    ASSERT_EQ(client->catalogHistory({}, 10s)->size(), 1u);
    ASSERT_EQ(client->defineTopicAndWait("b", 1, "", {}, false, rune::client::Persistence::Disk, {}, {}, 10s).errorCode,
              0);
    ASSERT_EQ(client->catalogHistory({}, 10s)->size(), 2u);
    EXPECT_EQ(b.catalogSubscribeFromOffsets(), (std::vector<int64_t>{0, 1})) << "the client resumed from its cache end";
}

// An as-of point the log does not reach is the library's explicit refusal
// (Kind::Replay with its detail), never an empty or partial state.  (The
// withheld marker — a leaderless catalog, Task 12 F1 — is pinned on the fake:
// CatalogReaderRegression.NoCaughtUpMarkerIsATimeoutNotAHang and
// RevalidationCutShortByTheDeadlineIsRevalidatedNextTimeRegression.)
TEST(ClientCatalogReadBrokerRegression, AnAsOfPastTheLogIsAnExplicitErrorNotAHang) {
    OneBroker b("cr_err");
    auto client = b.connectClient();
    ASSERT_EQ(client->defineTopicAndWait("a", 1, "", {}, false, rune::client::Persistence::Disk, {}, {}, 10s).errorCode,
              0);
    auto s = client->catalogAsOf({.by = rcat::AsOf::By::Version, .value = 9, .type = rcat::ObjectType::Topic,
                                  .name = "a"},
                                 10s);
    ASSERT_FALSE(s.has_value());
    EXPECT_EQ(s.error().kind, rune::client::CatalogReadError::Kind::Replay);
    EXPECT_NE(s.error().detail.find("no version 9"), std::string::npos) << s.error().detail;
}

// An empty catalog: the marker at HW 0 ends the read at once (no event, no hang).
TEST(ClientCatalogReadBrokerRegression, AnEmptyCatalogReadsAsAnEmptyHistory) {
    OneBroker b("cr_empty");
    auto client = b.connectClient();
    auto h = client->catalogHistory({}, 10s);
    ASSERT_TRUE(h.has_value()) << h.error().detail;
    EXPECT_TRUE(h->empty());
}
