// NsSlotRegressionTest.cpp - namespaces Phase 2 Task 8 (D22): per-node namespace slots and the broadcast.
//
// Interaction matrix (event x state x timing):
//   | event                       | state                         | timing                       | expected                               | test
//   |-----------------------------|-------------------------------|------------------------------|----------------------------------------|-----
//   | assign / release / reuse    | 0, 1, max-1, max, max+1 ns    | sequential                   | lowest free; nullopt at max+1          | AssignReleaseReuseRegression
//   | Create past the capacity    | broker at maxNamespaces       | sequential                   | NamespaceLimitExceeded, no slot        | CreateBeyondCapacityRefusedRegression
//   | stale (slot, nsId) tag      | slot reassigned to another ns | after the reuse              | counted (worker + metric), refused     | StaleSlotMismatchCountedRegression
//   | create, first produce       | 3 workers                     | back-to-back (no wait)       | every worker holds the pair, 0 mismatch| EveryWorkerSeesAssignBeforeFirstRequestRegression
//   | restart                     | 3 namespaces                  | restart in between           | the same (slot, nsId) pairs            | RestartRebuildsSameSlotsRegression
//   | delete then create          | an erased namespace's slot    | after the erase              | the lowest free slot is reused         | ErasedSlotReusedRegression
//   | metric registration         | every namespace               | create, erase, restart       | owner tag = nsId, slot value           | MetricsRegIdIsNsIdRegression
#include "MetricsTestReader.h"
#include "support/AuthRig.h"

#include <rune/client/Client.h>
#include <rune/client/producer/Producer.h>
#include <rune/node/NsSlotTable.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
namespace rc = rune::client;
using namespace std::chrono_literals;

namespace {

constexpr const char* kAdminPw = "namespace-admin-password-1";
constexpr const char* kUserPw  = "namespace-user-password-12";

std::unique_ptr<rc::Client> connected(AuthBroker& b, const std::string& user, std::vector<rc::TopicSpec> topics = {}) {
    rc::ClientConfig c;
    c.gatewayChannel  = b.channel();
    c.gatewayStreamId = 1;
    c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    c.egressStreamId  = 2;
    c.dataDir         = b.clientDir().string();
    c.username        = user;
    c.password        = user == "admin" ? kAdminPw : kUserPw;
    c.offerCrypto     = true;
    c.tlsTrustPem     = AuthBroker::tlsCaPem();
    c.topics          = std::move(topics);
    auto cl = std::make_unique<rc::Client>(c);
    cl->connectAndWait(20s);
    return cl;
}

std::vector<AuthGrant> userGrants() {
    return {prefix(SO::Create, ST::Topic, ""), prefix(SO::Describe, ST::Topic, ""), prefix(SO::Write, ST::Topic, ""),
            prefix(SO::Read, ST::Topic, "")};
}

bool waitFor(const std::function<bool()>& pred, std::chrono::seconds limit = 60s) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(20ms);
    }
    return pred();
}

std::span<const uint8_t> bytesOf(const std::string& s) { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }

using Pairs = std::vector<std::pair<uint16_t, uint32_t>>;

Pairs workerPairs(BrokerNode& n, int32_t w) {
    return n.onWorkerThreadForTest(w, [](Worker& wk) {
        Pairs v;
        const NsSlotTable& t = wk.nsSlotsForTest();
        for (uint32_t s = 0; s < t.capacity(); ++s)
            if (t.nsIdAt(static_cast<uint16_t>(s)) != NsSlotTable::kFreeSlot)
                v.emplace_back(static_cast<uint16_t>(s), t.nsIdAt(static_cast<uint16_t>(s)));
        return v;
    });
}

uint32_t idOf(rc::Client& admin, const std::string& name) {
    auto r = admin.listNamespaces();
    if (r) for (const auto& n : *r) if (n.name == name && n.state != 4) return n.nsId;
    return 0;
}
bool hasNs(rc::Client& admin, uint32_t nsId) {
    auto r = admin.listNamespaces();
    if (r) for (const auto& n : *r) if (n.nsId == nsId) return true;
    return false;
}

}  // namespace

// ---- the table: 0, 1, max-1, max, max+1 namespaces; lowest free; the pair is checked ----------------------------
TEST(NsSlotRegression, AssignReleaseReuseRegression) {
    for (const uint32_t cap : {1u, 2u, 3u, 8u}) {
        NsSlotTable t(cap);
        EXPECT_EQ(t.capacity(), cap);
        EXPECT_EQ(t.slotOf(0), 0u) << "default owns slot 0 from construction";
        EXPECT_EQ(t.used(), 1u);
        // Fill to the capacity: nsIds 1.. take slots 1.. (lowest free first).
        for (uint32_t i = 1; i < cap; ++i) {
            const auto s = t.assign(100 + i);
            ASSERT_TRUE(s.has_value()) << "cap " << cap << " ns " << i << " (max-1 / max)";
            EXPECT_EQ(*s, i);
            EXPECT_EQ(t.assign(100 + i), s) << "assign is idempotent";
        }
        EXPECT_EQ(t.used(), cap);
        EXPECT_FALSE(t.assign(9999).has_value()) << "max+1 gets no slot";
        EXPECT_EQ(t.slotOf(9999), kNoNsSlot);
        if (cap > 2) {
            // release slot 1 (wrong pair first), then the next assign reuses the lowest free slot.
            EXPECT_FALSE(t.release(1, 555)) << "the pair is checked";
            EXPECT_TRUE(t.matches(1, 101));
            EXPECT_TRUE(t.release(1, 101));
            EXPECT_FALSE(t.matches(1, 101));
            EXPECT_FALSE(t.release(1, 101)) << "double release";
            EXPECT_TRUE(t.release(2, 102));
            EXPECT_EQ(t.assign(7000), std::optional<uint16_t>(1)) << "lowest free first";
            EXPECT_EQ(t.assign(7001), std::optional<uint16_t>(2));
        }
        EXPECT_FALSE(t.release(0, 0)) << "default is never freed";
        EXPECT_TRUE(t.matches(0, 0));
        EXPECT_FALSE(t.matches(static_cast<uint16_t>(cap), 0)) << "out of range";
    }
    // A reused slot never inherits the old owner's limits.
    NsSlotTable t(4);
    const auto s = t.assign(5);
    rune::protocol::NamespaceLimits l;
    l.maxTopics = 3;
    t.setLimits(*s, l);
    EXPECT_EQ(t.limits(*s).maxTopics, 3u);
    ASSERT_TRUE(t.release(*s, 5));
    EXPECT_EQ(t.assign(6), s);
    EXPECT_NE(t.limits(*s).maxTopics, 3u);
}

// ---- a broker at its capacity: the Create is refused before any event (and no slot is taken) ----------------------
TEST(NsSlotRegression, CreateBeyondCapacityRefusedRegression) {
    AuthBroker::Options o;
    o.configure = [](NodeConfig& cfg) { cfg.maxNamespaces = 3; };
    AuthBroker b("nsslot_cap", o);
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    ASSERT_TRUE(admin->createNamespace("a").has_value());
    ASSERT_TRUE(admin->createNamespace("b").has_value());
    const auto c = admin->createNamespace("c");
    ASSERT_FALSE(c.has_value());
    EXPECT_EQ(c.error().errorCode, static_cast<int>(pc::ErrorCode::NamespaceLimitExceeded));
    const Pairs want{{0, 0}, {1, 1}, {2, 2}};
    ASSERT_TRUE(waitFor([&] { return b.node().nsSlotsForTest() == want; }, 10s));
    EXPECT_EQ(b.node().nsSlotAssignFailuresForTest(), 0u);
    EXPECT_EQ(idOf(*admin, "c"), 0u) << "no event was written";
}

// ---- a stale (slot, nsId) tag is counted and refused, never applied to the slot's new owner ------------------------
TEST(NsSlotRegression, StaleSlotMismatchCountedRegression) {
    AuthBroker b("nsslot_stale");
    b.bootstrapAdmin(kAdminPw);
    const auto base = b.counter(static_cast<int32_t>(RuneMetric::NsSlotMismatch));
    ASSERT_TRUE(base.has_value());
    // Slot 900 is far above anything the node assigned: drive the worker's own table on its own thread.
    const auto res = b.node().onWorkerThreadForTest(0, [](Worker& w) {
        struct R { bool staleBefore, freshBefore, staleAfter, freshAfter; uint64_t mismatches; };
        rune::protocol::NamespaceLimits l;
        w.applyNsSlotAssignDirect(900, 7, l);
        const bool okFirst = w.checkNsTagForTest(900, 7);
        const bool rel = w.applyNsSlotReleaseDirect(900, 7);
        w.applyNsSlotAssignDirect(900, 9, l);
        R r{okFirst, rel, false, false, 0};
        r.staleAfter = w.checkNsTagForTest(900, 7);   // the stale tag
        r.freshAfter = w.checkNsTagForTest(900, 9);
        (void)w.applyNsSlotReleaseDirect(900, 9);
        r.mismatches = w.nsSlotMismatchesForTest();
        return r;
    });
    EXPECT_TRUE(res.staleBefore) << "while 7 owns the slot its tag matches";
    EXPECT_TRUE(res.freshBefore);
    EXPECT_FALSE(res.staleAfter) << "the tag (900, 7) must not match the slot's new owner (nsId check)";
    EXPECT_TRUE(res.freshAfter);
    EXPECT_EQ(res.mismatches, 1u) << "exactly the one stale use is counted";
    // rune.ns.slot_mismatch reaches the metrics file (worker -> gateway signal -> control publish).
    EXPECT_TRUE(b.awaitCounter(static_cast<int32_t>(RuneMetric::NsSlotMismatch), *base + 1)) 
        << "rune.ns.slot_mismatch did not rise by one";
}

// ---- 3 workers: the assign is on every worker before the first request of the namespace --------------------------
TEST(NsSlotRegression, EveryWorkerSeesAssignBeforeFirstRequestRegression) {
    AuthBroker::Options o;
    o.configure = [](NodeConfig& cfg) { cfg.workerCount = 3; };
    AuthBroker b("nsslot_workers", o);
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    const auto a = admin->createNamespace("a");
    ASSERT_TRUE(a.has_value()) << a.error().detail;
    b.createUser("a/ua", kUserPw, userGrants());
    // No wait between the create and the first request: define a topic and produce at once.
    auto ua = connected(b, "a/ua");
    const auto def = ua->defineTopicAndWait("t", 1, "", false, {}, 20s);
    ASSERT_EQ(def.errorCode, 0) << def.detail;
    ua->close();
    auto up = connected(b, "a/ua", {rc::TopicSpec{"t", def.topicId, 1}});
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::All;
    rc::Producer p = up->producer(pcfg);
    for (int i = 0; i < 5; ++i) p.send(up->topic("t"), rc::Key(int64_t{i}), bytesOf("v" + std::to_string(i)));
    ASSERT_EQ(p.closeAndWait(30s), rc::OpStatus::Ok) << "the first produce of a fresh namespace was refused";
    up->close();
    const uint16_t slot = b.node().nsSlotOfForTest(*a);
    ASSERT_NE(slot, kNoNsSlot);
    for (int32_t w = 0; w < 3; ++w) {
        const Pairs ps = workerPairs(b.node(), w);
        EXPECT_NE(std::find(ps.begin(), ps.end(), std::make_pair(slot, *a)), ps.end()) << "worker " << w << " missed the assign";
        EXPECT_EQ(b.node().onWorkerThreadForTest(w, [](Worker& wk) { return wk.nsSlotMismatchesForTest(); }), 0u)
            << "worker " << w << " saw a request before its assign";
    }
}

// ---- restart: the same (slot, nsId) pairs ------------------------------------------------------------------------------
TEST(NsSlotRegression, RestartRebuildsSameSlotsRegression) {
    AuthBroker b("nsslot_restart");
    b.bootstrapAdmin(kAdminPw);
    {
        auto admin = connected(b, "admin");
        for (const char* n : {"a", "b", "c"}) ASSERT_TRUE(admin->createNamespace(n).has_value()) << n;
    }
    const Pairs want{{0, 0}, {1, 1}, {2, 2}, {3, 3}};
    ASSERT_TRUE(waitFor([&] { return b.node().nsSlotsForTest() == want; }, 10s));
    b.restart();
    EXPECT_EQ(b.node().nsSlotsForTest(), want) << "the authoritative table after the restart";
    EXPECT_EQ(workerPairs(b.node(), 0), want) << "the worker's table after the restart";
    EXPECT_EQ(b.node().nsSlotAssignFailuresForTest(), 0u);
}

// ---- an erased namespace's slot is freed, and the next namespace takes the lowest free slot ---------------------------
TEST(NsSlotRegression, ErasedSlotReusedRegression) {
    AuthBroker b("nsslot_reuse");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    const auto a = admin->createNamespace("a");
    const auto bb = admin->createNamespace("b");
    ASSERT_TRUE(a.has_value() && bb.has_value());
    ASSERT_EQ(b.node().nsSlotOfForTest(*a), 1u);
    ASSERT_EQ(b.node().nsSlotOfForTest(*bb), 2u);
    ASSERT_TRUE(admin->deleteNamespace("a").has_value());
    // DELETING still owns the slot; the erase frees it (never NamespaceDeleted).
    ASSERT_TRUE(waitFor([&] { return !hasNs(*admin, *a); }, 90s)) << "a was never erased";
    ASSERT_TRUE(waitFor([&] { return b.node().nsSlotOfForTest(*a) == kNoNsSlot; }, 10s)) << "the erased slot was not freed";
    const auto d = admin->createNamespace("d");
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(b.node().nsSlotOfForTest(*d), 1u) << "the freed slot is the lowest free one";
    EXPECT_EQ(b.node().nsSlotOfForTest(*bb), 2u) << "another namespace's slot never moves";
    const Pairs want{{0, 0}, {1, *d}, {2, *bb}};
    ASSERT_TRUE(waitFor([&] { return workerPairs(b.node(), 0) == want; }, 10s)) << "the worker's table follows the release + assign";
    EXPECT_EQ(b.node().nsSlotAssignFailuresForTest(), 0u);
}

// ---- the per-namespace metric: owner tag = nsId, value = the slot; freed on the erase; a scrape never merges ----------
TEST(NsSlotRegression, MetricsRegIdIsNsIdRegression) {
    AuthBroker b("nsslot_metric");
    b.bootstrapAdmin(kAdminPw);
    auto admin = connected(b, "admin");
    const auto a = admin->createNamespace("a");
    ASSERT_TRUE(a.has_value());
    const std::string path = b.node().metricsFilePath();
    const auto typeId = static_cast<int32_t>(RuneNsMetric::Slot);
    // Keyed by the nsId (the 4-byte LE role key), so two namespaces' series never merge.
    ASSERT_TRUE(waitFor([&] { return readRuneMetricLiveAt(path, typeId, static_cast<int32_t>(*a)).has_value(); }, 10s));
    const auto live = readRuneMetricLiveAt(path, typeId, static_cast<int32_t>(*a));
    EXPECT_EQ(live->ownerTag, static_cast<int64_t>(*a)) << "the owner tag is the nsId";
    EXPECT_EQ(live->value, 1);
    const auto def = readRuneMetricLiveAt(path, typeId, 0);
    ASSERT_TRUE(def.has_value()) << "default has its series";
    EXPECT_EQ(def->ownerTag, 0);
    EXPECT_EQ(def->value, 0);
    ASSERT_TRUE(admin->deleteNamespace("a").has_value());
    ASSERT_TRUE(waitFor([&] { return !readRuneMetricLiveAt(path, typeId, static_cast<int32_t>(*a)).has_value(); }, 90s))
        << "the erased namespace's series is still scraped";
    const auto d = admin->createNamespace("d");
    ASSERT_TRUE(d.has_value());
    ASSERT_TRUE(waitFor([&] { return readRuneMetricLiveAt(path, typeId, static_cast<int32_t>(*d)).has_value(); }, 10s));
    const auto ld = readRuneMetricLiveAt(path, typeId, static_cast<int32_t>(*d));
    EXPECT_EQ(ld->ownerTag, static_cast<int64_t>(*d));
    EXPECT_FALSE(readRuneMetricLiveAt(path, typeId, static_cast<int32_t>(*a)).has_value()) << "the old series did not come back";
}
