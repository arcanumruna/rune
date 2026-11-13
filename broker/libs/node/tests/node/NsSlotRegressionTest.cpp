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
#include "support/AllocWindow.h"
#include "support/AuthRig.h"

#include <rune/client/Client.h>
#include <rune/client/producer/Producer.h>
#include <rune/node/NsSlotFanout.h>
#include <rune/node/NsSlotTable.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <functional>
#include <memory>
#include <random>
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
    const auto def = ua->defineTopicAndWait("t", 1, "", {}, 20s);
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
    b.restart(/*reloginAdmin=*/false);   // the rig relogs in with its own default admin password
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

// ---- #710 / #729: the fan-out of tick-rate throttles is bounded, allocation-free and keeps assign-before-throttle ----
namespace {

/// A worker whose ring can be made to refuse; applies what it accepts to a slot table like the real worker does.
struct StubWorker {
    bool full = false;
    bool refuseAssign = false;   // the ring has room for a throttle but not for the (much larger) assign
    NsSlotTable slots{8};
    uint64_t accepted = 0, throttles = 0, mismatches = 0;
    std::array<int64_t, 8> lastUntil{};
    std::array<uint32_t, 8> throttlesOfSlot{};
    std::vector<WorkerOp::Type> order;   // only filled while `record` (outside any AllocWindow)
    bool record = false;
    EnqueueResult enqueueOp(WorkerOp op) {
        if (full || (refuseAssign && op.type == WorkerOp::Type::NsSlotAssign)) return EnqueueResult::Full;
        ++accepted;
        if (record) order.push_back(op.type);
        const auto slot = static_cast<uint16_t>(op.topicId);
        const auto nsId = static_cast<uint32_t>(op.nextOffset);
        switch (op.type) {
        case WorkerOp::Type::NsSlotAssign:  (void)slots.place(slot, nsId); break;
        case WorkerOp::Type::NsSlotRelease: (void)slots.release(slot, nsId); break;
        case WorkerOp::Type::NsThrottle:
            if (!slots.matches(slot, nsId)) { ++mismatches; break; }
            ++throttles; ++throttlesOfSlot[slot]; lastUntil[slot] = op.hwBound;
            break;
        default: break;
        }
        return EnqueueResult::Enqueued;
    }
};
struct StubPool {
    std::vector<StubWorker> w;
    explicit StubPool(int n) : w(static_cast<std::size_t>(n)) {}
    int32_t workerCount() const noexcept { return static_cast<int32_t>(w.size()); }
    StubWorker& worker(int32_t i) { return w[static_cast<std::size_t>(i)]; }
};
constexpr int kPumpCap = 8;   // every drive loop is capped

}  // namespace

TEST(NsSlotRegression, ThrottleFanoutSteadyStateZeroAllocRegression) {
    NsSlotFanout f;
    f.configure(64, 2);
    StubPool pool(2);
    for (uint16_t s = 1; s <= 4; ++s) f.post(NsSlotAssign{s, 100u + s, rune::protocol::NamespaceLimits{}});
    for (int i = 0; i < kPumpCap && f.pendingForTest() != 0; ++i) (void)f.pump(pool);
    ASSERT_EQ(f.pendingForTest(), 0u);
    ASSERT_EQ(pool.w[0].slots.nsIdAt(4), 104u);   // slot 0 is `default`
    ASSERT_EQ(pool.w[1].slots.nsIdAt(4), 104u);
    pool.w[1].full = true;   // worker 1's ring refuses from now on
    {
        rune::testing::AllocWindow w;
        for (int i = 1; i <= 10'000; ++i) {
            for (uint16_t s = 1; s <= 4; ++s)
                f.post(Throttle{s, 100u + s, ThrottleKind::ProduceBytes, int64_t{1'000} + i});
            (void)f.pump(pool);
        }
        EXPECT_EQ(w.count(), 0u) << "sustained throttling against a full ring allocates";
    }
    EXPECT_EQ(f.pendingForTest(), 0u) << "tick-rate throttles never queue in the ordered FIFO";
    EXPECT_EQ(f.dirtyForTest(1), 4u) << "one dirty entry per slot, not one per post";
    EXPECT_EQ(f.dirtyForTest(0), 0u);
    EXPECT_EQ(pool.w[1].throttles, 0u);
    pool.w[1].full = false;
    for (int i = 0; i < kPumpCap; ++i) (void)f.pump(pool);
    EXPECT_EQ(pool.w[1].throttles, 4u) << "exactly one throttle per slot is delivered";
    for (uint16_t s = 1; s <= 4; ++s) {
        EXPECT_EQ(pool.w[1].throttlesOfSlot[s], 1u) << "slot " << s;
        EXPECT_EQ(pool.w[1].lastUntil[s], 1'000 + 10'000) << "slot " << s << ": the LAST value wins";
    }
    EXPECT_EQ(f.dirtyForTest(1), 0u);
}

TEST(NsSlotRegression, AssignAlwaysPrecedesThrottleRegression) {
    NsSlotFanout f;
    f.configure(8, 2);
    StubPool pool(2);
    for (auto& wk : pool.w) { wk.full = true; wk.record = true; }
    f.post(NsSlotAssign{3, 300u, rune::protocol::NamespaceLimits{}});
    f.post(Throttle{3, 300u, ThrottleKind::ProduceBytes, 777});
    f.post(ViewMemOver{3, 300u, true});
    for (int i = 0; i < kPumpCap; ++i) (void)f.pump(pool);
    for (auto& wk : pool.w) wk.full = false;
    for (int i = 0; i < kPumpCap; ++i) (void)f.pump(pool);
    for (int w = 0; w < 2; ++w) {
        ASSERT_EQ(pool.w[static_cast<std::size_t>(w)].order.size(), 3u) << "worker " << w;
        EXPECT_EQ(pool.w[static_cast<std::size_t>(w)].order[0], WorkerOp::Type::NsSlotAssign) << "worker " << w << ": assign first";
        EXPECT_EQ(pool.w[static_cast<std::size_t>(w)].mismatches, 0u);
        EXPECT_EQ(pool.w[static_cast<std::size_t>(w)].lastUntil[3], 777);
    }
    // The slot is released and re-assigned to another namespace while the old namespace's throttle is still owed.
    for (auto& wk : pool.w) wk.full = true;
    f.post(NsSlotRelease{3, 300u});
    f.post(NsSlotAssign{3, 301u, rune::protocol::NamespaceLimits{}});
    f.post(Throttle{3, 300u, ThrottleKind::FetchBytes, 888});   // the OLD owner's
    for (int i = 0; i < kPumpCap; ++i) (void)f.pump(pool);
    for (auto& wk : pool.w) wk.full = false;
    for (int i = 0; i < kPumpCap; ++i) (void)f.pump(pool);
    for (int w = 0; w < 2; ++w) {
        StubWorker& wk = pool.w[static_cast<std::size_t>(w)];
        EXPECT_EQ(wk.slots.nsIdAt(3), 301u) << "worker " << w;
        EXPECT_EQ(wk.mismatches, 1u) << "worker " << w << ": the stale throttle is dropped by the tag check";
        EXPECT_NE(wk.lastUntil[3], 888);
    }
    EXPECT_EQ(f.pendingForTest(), 0u);
    EXPECT_EQ(f.orderedOverflowsForTest(), 0u);

    // Worker 1's ring has room for a throttle but not for the assign: its throttle must wait behind the assign.
    pool.w[1].refuseAssign = true;
    pool.w[1].order.clear();
    f.post(NsSlotAssign{5, 500u, rune::protocol::NamespaceLimits{}});
    f.post(Throttle{5, 500u, ThrottleKind::ProduceBytes, 999});
    for (int i = 0; i < kPumpCap; ++i) (void)f.pump(pool);
    EXPECT_EQ(pool.w[0].slots.nsIdAt(5), 500u);
    EXPECT_EQ(pool.w[0].lastUntil[5], 999) << "worker 0 is not held up by worker 1";
    EXPECT_TRUE(pool.w[1].order.empty()) << "a throttle overtook the assign on worker 1";
    EXPECT_EQ(pool.w[1].mismatches, 1u) << "only the earlier stale throttle (unchanged)";
    pool.w[1].refuseAssign = false;
    for (int i = 0; i < kPumpCap; ++i) (void)f.pump(pool);
    ASSERT_EQ(pool.w[1].order.size(), 2u);
    EXPECT_EQ(pool.w[1].order[0], WorkerOp::Type::NsSlotAssign);
    EXPECT_EQ(pool.w[1].order[1], WorkerOp::Type::NsThrottle);
    EXPECT_EQ(pool.w[1].lastUntil[5], 999);
}

// #866: a throttle still owed to a worker when its slot is released is voided, never delivered as a tag mismatch.
TEST(NsSlotRegression, AReleaseVoidsTheSlotsOwedThrottlesRegression) {
    NsSlotFanout f;
    f.configure(8, 2);
    StubPool pool(2);
    f.post(NsSlotAssign{3, 300u, rune::protocol::NamespaceLimits{}});
    for (int i = 0; i < kPumpCap; ++i) (void)f.pump(pool);
    for (auto& wk : pool.w) wk.full = true;
    f.post(Throttle{3, 300u, ThrottleKind::FetchBytes, 888});    // owed to both workers
    f.post(ViewMemOver{3, 300u, true});
    for (int i = 0; i < kPumpCap; ++i) (void)f.pump(pool);
    f.post(NsSlotRelease{3, 300u});
    f.post(NsSlotAssign{3, 301u, rune::protocol::NamespaceLimits{}});
    for (auto& wk : pool.w) wk.full = false;
    for (int i = 0; i < kPumpCap; ++i) (void)f.pump(pool);
    EXPECT_EQ(f.voidDropsForTest(), 4u) << "two cells, two workers";
    for (int w = 0; w < 2; ++w) {
        StubWorker& wk = pool.w[static_cast<std::size_t>(w)];
        EXPECT_EQ(wk.slots.nsIdAt(3), 301u) << "worker " << w;
        EXPECT_EQ(wk.mismatches, 0u) << "worker " << w << ": nothing of the old owner arrives";
        EXPECT_EQ(f.dirtyForTest(w), 0u) << "worker " << w;
    }
    // The new owner's throttle is delivered as usual.
    f.post(Throttle{3, 301u, ThrottleKind::FetchBytes, 999});
    for (int i = 0; i < kPumpCap; ++i) (void)f.pump(pool);
    for (int w = 0; w < 2; ++w) EXPECT_EQ(pool.w[static_cast<std::size_t>(w)].lastUntil[3], 999) << "worker " << w;
}

// #864: a Deleting namespace is upserted once per delete step; only the first NsDeleting of a slot owner is queued.
TEST(NsSlotRegression, RepeatedDeletingUpsertsNeverOverflowTheOrderedFifoAdversarialRegression) {
    constexpr uint16_t kSlots = 4;                 // FIFO capacity 16: 18 posts without the dedup
    NsSlotFanout f;
    f.configure(kSlots, 1);
    StubPool pool(1);
    pool.w[0].full = true;
    for (uint16_t s = 1; s <= 3; ++s) {
        const uint32_t ns = 100u + s;
        f.post(NsSlotAssign{s, ns, rune::protocol::NamespaceLimits{}});
        for (int up = 0; up < 4; ++up) f.post(Throttle{s, ns, ThrottleKind::NsDeleting, 0});
        f.post(NsSlotRelease{s, ns});
        for (int i = 0; i < 4; ++i) (void)f.pump(pool);
    }
    EXPECT_EQ(f.orderedOverflowsForTest(), 0u) << "an ordered slot message was lost";
    EXPECT_EQ(f.deletingDedupsForTest(), 9u) << "three repeats per slot are dropped";
    pool.w[0].full = false;
    for (int i = 0; i < kPumpCap; ++i) (void)f.pump(pool);
    for (uint16_t s = 1; s <= 3; ++s) EXPECT_EQ(pool.w[0].slots.nsIdAt(s), NsSlotTable::kFreeSlot) << "slot " << s;
}

// #864: the dedup is per slot OWNER: a slot released and given to another namespace gets its own NsDeleting.
TEST(NsSlotRegression, ANewOwnerOfASlotGetsItsOwnDeletingRegression) {
    NsSlotFanout f;
    f.configure(4, 1);
    StubPool pool(1);
    f.post(NsSlotAssign{1, 101, rune::protocol::NamespaceLimits{}});
    f.post(Throttle{1, 101, ThrottleKind::NsDeleting, 0});
    (void)f.pump(pool);   // #1020: the first owner is delivered, else its Release would cancel its unseen entries
    ASSERT_EQ(f.pendingForTest(), 0u);
    f.post(NsSlotRelease{1, 101});
    f.post(NsSlotAssign{1, 202, rune::protocol::NamespaceLimits{}});
    f.post(Throttle{1, 202, ThrottleKind::NsDeleting, 0});
    f.post(Throttle{1, 202, ThrottleKind::NsDeleting, 0});
    EXPECT_EQ(f.pendingForTest(), 3u) << "release, assign, deleting";
    EXPECT_EQ(f.deletingDedupsForTest(), 1u);
    for (int i = 0; i < kPumpCap && f.pendingForTest() != 0; ++i) (void)f.pump(pool);
    EXPECT_EQ(f.pendingForTest(), 0u);
    EXPECT_EQ(pool.w[0].slots.nsIdAt(1), 202u);
    EXPECT_EQ(pool.w[0].throttles, 2u) << "one NsDeleting per slot owner reached the worker";
}

// ---- #1020: the ordered FIFO edits unseen entries in place, so it cannot fill during one ring stall ---------------
namespace {

/// A worker that records every ordered op it accepts and applies it like Worker::applyNsSlotAssign_ / Release_ /
/// applyNsThrottle_ do (place on a new owner, a tag mismatch is counted and not applied; NsDeleting is sticky).
struct ModelWorker {
    struct Seen { WorkerOp::Type type; uint32_t nsId; uint64_t maxTopics; uint16_t slot; };
    bool full = false;
    NsSlotTable slots{8};
    std::array<uint64_t, 8> maxTopics{};
    std::array<bool, 8> deleting{};
    uint64_t mismatches = 0;
    std::vector<Seen> log;
    EnqueueResult enqueueOp(WorkerOp op) {
        if (full) return EnqueueResult::Full;
        const auto slot = static_cast<uint16_t>(op.topicId);
        const auto nsId = static_cast<uint32_t>(op.nextOffset);
        uint64_t mt = 0;
        switch (op.type) {
        case WorkerOp::Type::NsSlotAssign: {
            rune::protocol::NamespaceLimits l;
            (void)rune::protocol::decodeLimits(op.schemaUpdateBlob.data(), op.schemaUpdateBlob.size(), l);
            mt = l.maxTopics;
            if (slots.nsIdAt(slot) != nsId) {
                if (!slots.place(slot, nsId)) { ++mismatches; break; }
                deleting[slot] = false;
            }
            slots.setLimits(slot, l);
            maxTopics[slot] = l.maxTopics;
            break;
        }
        case WorkerOp::Type::NsSlotRelease:
            if (!slots.release(slot, nsId)) { ++mismatches; break; }
            deleting[slot] = false;
            break;
        case WorkerOp::Type::NsThrottle:
            if (!slots.matches(slot, nsId)) { ++mismatches; break; }
            if (static_cast<ThrottleKind>(op.partition) == ThrottleKind::NsDeleting) deleting[slot] = true;
            break;
        default: break;
        }
        log.push_back(Seen{op.type, nsId, mt, slot});
        return EnqueueResult::Enqueued;
    }
};
struct ModelPool {
    std::vector<ModelWorker> w;
    explicit ModelPool(int n) : w(static_cast<std::size_t>(n)) {}
    int32_t workerCount() const noexcept { return static_cast<int32_t>(w.size()); }
    ModelWorker& worker(int32_t i) { return w[static_cast<std::size_t>(i)]; }
};
rune::protocol::NamespaceLimits limitsOf(uint64_t maxTopics) {
    rune::protocol::NamespaceLimits l;
    l.maxTopics = maxTopics;
    return l;
}
void drain(NsSlotFanout& f, ModelPool& pool) {
    for (auto& wk : pool.w) wk.full = false;
    for (int i = 0; i < 64 && (f.pendingForTest() != 0 || f.dirtyForTest(0) != 0); ++i) (void)f.pump(pool);
}

}  // namespace

TEST(NsSlotFanoutRegression, RepeatedLimitChangesDuringARingStallNeverFillTheFifoRegression) {
    NsSlotFanout f;
    f.configure(8, 3);
    ModelPool pool(3);
    pool.w[1].full = true;
    for (uint16_t s = 1; s <= 3; ++s) f.post(NsSlotAssign{s, 100u + s, limitsOf(1)});
    (void)f.pump(pool);   // worker 0 gets the first assign; worker 1's ring refuses and the FIFO stalls there
    for (uint64_t i = 2; i <= 10'000; ++i) {
        for (uint16_t s = 1; s <= 3; ++s) f.post(NsSlotAssign{s, 100u + s, limitsOf(i)});
        ASSERT_LE(f.pendingForTest(), 4u) << "step " << i;
    }
    EXPECT_EQ(f.orderedOverflowsForTest(), 0u);
    drain(f, pool);
    for (auto& wk : pool.w)
        for (uint16_t s = 1; s <= 3; ++s) {
            EXPECT_EQ(wk.slots.nsIdAt(s), 100u + s);
            EXPECT_EQ(wk.maxTopics[s], 10'000u) << "slot " << s << ": the LAST limits";
            uint64_t last = 0;
            for (const auto& e : wk.log) if (e.type == WorkerOp::Type::NsSlotAssign && e.slot == s) last = e.maxTopics;
            EXPECT_EQ(last, 10'000u);
        }
}

TEST(NsSlotFanoutRegression, SlotReuseDuringARingStallNeverFillsTheFifoRegression) {
    NsSlotFanout f;
    f.configure(8, 2);
    ModelPool pool(2);
    pool.w[1].full = true;
    constexpr uint32_t kCycles = 10'000;
    for (uint32_t i = 1; i <= kCycles; ++i) {
        f.post(NsSlotAssign{1, 1000u + i, limitsOf(i)});
        f.post(Throttle{1, 1000u + i, ThrottleKind::NsDeleting, 0});
        f.post(NsSlotRelease{1, 1000u + i});
        ASSERT_LE(f.pendingForTest(), 4u) << "cycle " << i;
    }
    f.post(NsSlotAssign{1, 99'999u, limitsOf(7)});
    EXPECT_EQ(f.orderedOverflowsForTest(), 0u);
    drain(f, pool);
    for (auto& wk : pool.w) {
        ASSERT_FALSE(wk.log.empty());
        EXPECT_EQ(wk.log.back().type, WorkerOp::Type::NsSlotAssign);
        EXPECT_EQ(wk.log.back().nsId, 99'999u);
        for (const auto& e : wk.log) EXPECT_EQ(e.nsId, 99'999u) << "an op of a cancelled owner reached a worker";
        EXPECT_EQ(wk.mismatches, 0u);
        EXPECT_FALSE(wk.deleting[1]);
        EXPECT_EQ(wk.slots.nsIdAt(1), 99'999u);
    }
}

TEST(NsSlotFanoutRegression, APartlyDeliveredAssignIsNeverEditedRegression) {
    NsSlotFanout f;
    f.configure(8, 3);
    ModelPool pool(3);
    pool.w[1].full = true;
    f.post(NsSlotAssign{1, 101u, limitsOf(1)});
    (void)f.pump(pool);                       // worker 0 has L1; worker 1 refuses
    ASSERT_EQ(pool.w[0].log.size(), 1u);
    f.post(NsSlotAssign{1, 101u, limitsOf(2)});
    EXPECT_EQ(f.pendingForTest(), 2u) << "the partly delivered entry is not edited: a second entry is queued";
    drain(f, pool);
    ASSERT_EQ(pool.w[0].log.size(), 2u);
    EXPECT_EQ(pool.w[0].log[0].maxTopics, 1u);
    EXPECT_EQ(pool.w[0].log[1].maxTopics, 2u);
    for (auto& wk : pool.w) {
        EXPECT_EQ(wk.maxTopics[1], 2u);
        EXPECT_EQ(wk.slots.nsIdAt(1), 101u);
    }
    // A fresh owner released after it was announced (by an earlier Assign) still gets its Release.
    pool.w[1].full = true;
    f.post(NsSlotAssign{1, 101u, limitsOf(3)});   // unseen refresh of an owner the workers already know
    f.post(NsSlotRelease{1, 101u});               // must NOT cancel: the workers hold the owner
    drain(f, pool);
    for (auto& wk : pool.w) EXPECT_EQ(wk.slots.nsIdAt(1), NsSlotTable::kFreeSlot) << "the Release was lost";
}

TEST(NsSlotFanoutRegression, ModelCheckedRandomPostsAndRingRefusalsConvergeRegression) {
    constexpr uint32_t kSlotCap = 6;   // slots 1..5 are usable
    constexpr int kWorkers = 3;
    for (uint64_t seed = 1; seed <= 20; ++seed) {
        std::mt19937_64 rng(seed * 0x9E3779B97F4A7C15ULL);
        NsSlotFanout f;
        f.configure(kSlotCap, kWorkers);
        ModelPool pool(kWorkers);
        struct Truth { uint32_t ns = NsSlotTable::kFreeSlot; uint64_t maxTopics = 0; bool deleting = false; };
        std::array<Truth, kSlotCap> truth{};
        uint32_t nextNs = 100;
        uint64_t nextLimit = 1;
        std::size_t maxPending = 0;
        for (int step = 0; step < 20'000; ++step) {
            const auto slot = static_cast<uint16_t>(1 + rng() % (kSlotCap - 1));
            Truth& t = truth[slot];
            switch (rng() % 4) {
            case 0:   // a new owner on a free slot, or a limits change of the owner
                if (t.ns == NsSlotTable::kFreeSlot) { t = Truth{nextNs++, nextLimit, false}; }
                else t.maxTopics = nextLimit;
                f.post(NsSlotAssign{slot, t.ns, limitsOf(nextLimit++)});
                break;
            case 1:
                if (t.ns != NsSlotTable::kFreeSlot) {
                    t.deleting = true;
                    f.post(Throttle{slot, t.ns, ThrottleKind::NsDeleting, 0});
                }
                break;
            case 2:
                if (t.ns != NsSlotTable::kFreeSlot) { f.post(NsSlotRelease{slot, t.ns}); t = Truth{}; }
                break;
            default: break;
            }
            if (rng() % 5 == 0) pool.w[rng() % kWorkers].full = (rng() % 3) != 0;   // mostly refusing
            if (rng() % 3 == 0) (void)f.pump(pool);
            maxPending = std::max(maxPending, f.pendingForTest());
            ASSERT_EQ(f.orderedOverflowsForTest(), 0u) << "seed " << seed << " step " << step;
        }
        drain(f, pool);
        ASSERT_EQ(f.pendingForTest(), 0u) << "seed " << seed;
        EXPECT_LE(maxPending, std::size_t{6} * kSlotCap + 16);
        for (int w = 0; w < kWorkers; ++w) {
            const ModelWorker& wk = pool.w[static_cast<std::size_t>(w)];
            EXPECT_EQ(wk.mismatches, 0u) << "seed " << seed << " worker " << w;
            for (uint16_t s = 1; s < kSlotCap; ++s) {
                EXPECT_EQ(wk.slots.nsIdAt(s), truth[s].ns) << "seed " << seed << " worker " << w << " slot " << s;
                if (truth[s].ns == NsSlotTable::kFreeSlot) continue;
                EXPECT_EQ(wk.maxTopics[s], truth[s].maxTopics) << "seed " << seed << " worker " << w << " slot " << s;
                EXPECT_EQ(wk.deleting[s], truth[s].deleting) << "seed " << seed << " worker " << w << " slot " << s;
            }
        }
    }
}
