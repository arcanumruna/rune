// NamespaceReaperRegressionTest.cpp - namespaces Phase 2 Task 7 (D17, D18; rule 16): the NamespaceReaper drives every
// DELETING namespace to removal, in order, and converges from the registry alone across a crash and a leader change.
//
// Two layers.  (1) The state machine over a REAL CatalogState (events applied through the catalog's own rules) with a
// scripted port: deterministic, an injected clock (one tick = one doWork pass), a hard iteration cap that fails.
// (2) Three real AdminWorkers (CatalogTrio): the reaper stepped by the catalog leader's AdminWorker, a leader change
// after each durable step.
//
// Interaction matrix (event x namespace set x timing):
//   | event                    | namespaces                   | timing                         | expected                          | test
//   |--------------------------|------------------------------|--------------------------------|-----------------------------------|-----
//   | every step, in order     | a: 2 views 3 topics, groups, | sequential                     | the exact op sequence             | DrivesEveryStepInOrder
//   |                          | txn state, security records  |                                |                                   |
//   | crash (fresh reaper)     | a                            | after each of the 7 events and | end state = the no-crash run's,   | CrashAtEveryStep...
//   |                          |                              | mid-tombstone batches          | no event applied twice            |
//   | leader change            | a (real AdminWorkers, trio)  | after each of the 6 events     | the new leader finishes           | LeaderChangeAtEveryStep
//   | stale result             | a                            | leadership lost with a drop    | ignored; the new one is matched   | InFlightResultAfterLeadershipLoss
//   | refused submit / code    | a                            | ring full, NotLeader answers   | retried, same object, none skipped| DropRefusedIsRetriedNotSkipped
//   | two DELETING namespaces  | a, b                         | sequential                     | all of a, then all of b           | TwoDeletingNamespacesSerial
//   | nothing to clean         | empty                        | sequential                     | steps only, erased                | EmptyNamespaceErases
#include "support/CatalogTrio.h"

#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogFingerprint.h>
#include <rune/catalog/CatalogState.h>
#include <rune/catalog/NamespaceRegistry.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/node/NamespaceReaper.h>

#include <gtest/gtest.h>

#include <functional>
#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace rc = rune::catalog;
namespace cd = rune::codec;
namespace rp = rune::protocol;
using namespace rune::node;
using namespace rune::node::test;

namespace {

constexpr int kCap = 10'000;   // hard cap on every drive loop: fail, never hang

// ── the catalog, built through its own apply rules ────────────────────────────────────────────────────────────
struct Cat {
    rc::CatalogState s;
    int64_t          off = 0;
    std::vector<std::string> applied;   // "drop view v1", "step 1", ... : every reaper-visible event, in apply order

    bool go(rc::CatalogEvent e, const std::string& tag = {}) {
        auto fp = rc::previewFingerprint(s, e);
        if (!fp) return false;
        e.fingerprint = *fp;
        if (!rc::apply(s, e, off)) return false;
        ++off;
        if (!tag.empty()) applied.push_back(tag);
        return true;
    }
    static rc::CatalogEvent base(rc::Ops ops, std::string name, uint32_t nsId, uint64_t objectId, uint32_t prev) {
        rc::CatalogEvent e;
        e.formatVer = rc::kFormatV2;
        e.nsId = nsId;
        e.kind = rc::kindOf(ops);
        e.ops = std::move(ops);
        e.objectName = std::move(name);
        e.objectId = objectId;
        e.prevVersion = prev;
        e.newVersion = prev + 1;
        return e;
    }
    const rc::NamespaceRecord* rec(uint32_t id) const { return s.namespaces().byId(id); }
    uint32_t verOf(uint32_t id) const { const auto* r = rec(id); return r ? r->version : 0; }
    std::string nameOf(uint32_t id) const { const auto* r = rec(id); return r ? r->name : "?"; }
    uint32_t create(const std::string& name) {
        const uint32_t id = s.namespaces().nextNsId();
        EXPECT_TRUE(go(base(rc::NamespaceCreatedOps{name, {}}, name, id, 0, 0)));
        return id;
    }
    void topic(uint32_t ns, const std::string& name) {
        rc::CreateTopicOps o;
        o.topicId = s.nextTopicId();
        EXPECT_TRUE(go(base(std::move(o), name, ns, s.nextObjectId(), 0)));
    }
    void view(uint32_t ns, const std::string& name, std::vector<std::string> sources) {
        cd::DescriptorBuilder out(name);
        out.add("id", cd::PrimType::Int64, 1, true);
        rc::ViewDef v;
        v.viewTopicId = s.nextTopicId();
        v.sinkPartitions = 1;
        v.keyField = "id";
        v.sourceNames = std::move(sources);
        v.normalizedPlanBlob = {1, 2, 3};
        v.compiledPlanBlob = {9};
        v.outputDescriptorBlob = cd::serialize(out.build());
        EXPECT_TRUE(go(base(rc::CreateViewOps{std::move(v)}, name, ns, s.nextObjectId(), 0)));
    }
    void deleteNs(uint32_t ns) { EXPECT_TRUE(go(base(rc::NamespaceDeletedOps{}, nameOf(ns), ns, 0, verOf(ns)))); }
    bool dropTopic(uint32_t ns, const std::string& name) {
        const rc::TopicObject* t = s.topicByName(ns, name);
        return t != nullptr && go(base(rc::DropTopicOps{t->topicId}, name, ns, t->objectId, t->version), "drop topic " + name);
    }
    bool dropView(uint32_t ns, const std::string& name) {
        const rc::ViewObject* v = s.viewByName(ns, name);
        return v != nullptr && go(base(rc::DropViewOps{v->def.viewTopicId}, name, ns, v->objectId, v->version), "drop view " + name);
    }
    bool step(uint32_t ns, uint8_t bit) {
        return rec(ns) != nullptr &&
               go(base(rc::NamespaceDeleteStepOps{bit}, nameOf(ns), ns, 0, verOf(ns)), "step " + std::to_string(bit));
    }
    bool erased(uint32_t ns) const { return rec(ns) == nullptr; }
};

// ── the scripted port: the control thread's stores and the admin worker's DDL path, simulated ─────────────────
struct Stores {   // survive a reaper "crash" (they are the broker's, not the reaper's)
    std::map<uint32_t, int> groupKeys, txnKeys;
    std::map<uint32_t, bool> securityRecords;   // true until the sim clears it
};

struct Sim final : ReaperPort {
    Cat&    cat;
    Stores& stores;
    std::unique_ptr<NamespaceReaper> reaper;
    std::vector<std::string> ops;                  // every request the reaper made, in order (retries included)
    struct Pending { bool control; uint64_t corr; ReapStatus st; uint16_t code; };
    std::vector<Pending> inbox;                    // results delivered at the start of the next tick
    int refuseSubmits = 0;                         // the next N submits return false (a full ring)
    int notLeaderAnswers = 0;                      // the next N drops are NOT applied and answer NotLeader
    bool holdResults = false;                      // keep results in the inbox (a slow admin thread)
    uint32_t batch = 256;
    int64_t now = 0;

    Sim(Cat& c, Stores& st, uint32_t b = 256) : cat(c), stores(st), reaper(std::make_unique<NamespaceReaper>(b)), batch(b) {}

    void crash() { reaper = std::make_unique<NamespaceReaper>(batch); inbox.clear(); }   // a restart: Idle, in-flight lost

    bool askControl(uint64_t corr, ReapOp op, uint32_t ns, uint32_t b) override {
        if (refuseSubmits > 0) { --refuseSubmits; return false; }
        auto drain = [&](std::map<uint32_t, int>& keys, const char* what) {
            int& left = keys[ns];
            ops.push_back(std::string(what) + " ns" + std::to_string(ns));
            if (left == 0) { inbox.push_back({true, corr, ReapStatus::Done, 0}); return; }
            left -= std::min<int>(left, static_cast<int>(b));
            inbox.push_back({true, corr, ReapStatus::More, 0});
        };
        switch (op) {
        case ReapOp::AbortTxns:
            ops.push_back("abort ns" + std::to_string(ns));
            inbox.push_back({true, corr, ReapStatus::Done, 0});
            break;
        case ReapOp::TombstoneGroups:   drain(stores.groupKeys, "tombstone-groups"); break;
        case ReapOp::TombstoneTxnState: drain(stores.txnKeys, "tombstone-txn"); break;
        }
        return true;
    }
    bool submitDrop(uint64_t corr, uint32_t ns, bool isView, const std::string& name) override {
        if (refuseSubmits > 0) { --refuseSubmits; return false; }
        ops.push_back(std::string(isView ? "drop-view " : "drop-topic ") + name);
        if (notLeaderAnswers > 0) { --notLeaderAnswers; inbox.push_back({false, corr, ReapStatus::Done, 2}); return true; }
        const bool ok = isView ? cat.dropView(ns, name) : cat.dropTopic(ns, name);
        inbox.push_back({false, corr, ReapStatus::Done, static_cast<uint16_t>(ok ? 0 : 7)});
        return true;
    }
    bool submitDeleteStep(uint64_t corr, uint32_t ns, uint8_t step) override {
        if (refuseSubmits > 0) { --refuseSubmits; return false; }
        ops.push_back("step " + std::to_string(step));
        const bool ok = cat.step(ns, step);
        inbox.push_back({false, corr, ReapStatus::Done, static_cast<uint16_t>(ok ? 0 : 39)});
        return true;
    }
    bool securityClean(uint32_t ns, std::string_view) override { return !stores.securityRecords[ns]; }

    /// One pass: deliver the results, then doWork.
    void tick() {
        now += 1'000'000;
        if (!holdResults) {
            auto in = std::move(inbox);
            inbox.clear();
            for (const Pending& p : in) {
                if (p.control) reaper->onControlResult(p.corr, p.st);
                else reaper->onDdlResult(p.corr, p.code);
            }
        }
        (void)reaper->doWork(now, cat.s, *this);
    }
    /// Ticks until `done()`; fails (returns false) at the cap.
    bool run(const std::function<bool()>& done) {
        for (int i = 0; i < kCap; ++i) {
            if (done()) return true;
            tick();
        }
        return done();
    }
    /// The security purge (the __security leader's) lands after `afterOps` ops have been made.
    std::function<void()> hook;
};

/// The standard namespace: views v1 v2 over topics t1 t2, plus a plain topic t3; 600 group keys, 3 txn keys, security
/// records present.  Returns its id (DELETING after deleteNs).
uint32_t standardNs(Cat& cat, Stores& st, const std::string& name = "a") {
    const uint32_t ns = cat.create(name);
    cat.topic(ns, "t1");
    cat.topic(ns, "t2");
    cat.topic(ns, "t3");
    cat.view(ns, "v1", {"t1"});
    cat.view(ns, "v2", {"t2"});
    st.groupKeys[ns] = 600;
    st.txnKeys[ns] = 3;
    st.securityRecords[ns] = true;
    return ns;
}

/// The expected op sequence for the standard namespace with batch 256.
std::vector<std::string> expectedOps(uint32_t ns) {
    const std::string n = std::to_string(ns);
    return {"abort ns" + n,  "step 1",
            "drop-view v1",  "drop-view v2",
            "drop-topic t1", "drop-topic t2", "drop-topic t3",
            "tombstone-groups ns" + n, "tombstone-groups ns" + n, "tombstone-groups ns" + n, "tombstone-groups ns" + n, "step 2",
            "tombstone-txn ns" + n,    "tombstone-txn ns" + n,    "step 4",
            "step 8"};
}

std::string join(const std::vector<std::string>& v) {
    std::ostringstream os;
    for (const auto& s : v) os << "\n  " << s;
    return os.str();
}

/// Runs the standard namespace to the end on a fresh Sim; the security purge lands once step 4 was requested.
void runToEnd(Sim& sim, uint32_t ns) {
    ASSERT_TRUE(sim.run([&] {
        if (sim.stores.securityRecords[ns] && std::find(sim.ops.begin(), sim.ops.end(), "step 4") != sim.ops.end())
            sim.stores.securityRecords[ns] = false;   // the __security leader's NamespacePurged applied on this node
        return sim.cat.erased(ns);
    })) << "not erased within the cap; state " << sim.reaper->stateName() << "; ops:" << join(sim.ops);
}

}  // namespace

// ── the state machine ─────────────────────────────────────────────────────────────────────────────────────────
TEST(NamespaceReaperRegression, DrivesEveryStepInOrderRegression) {
    Cat cat; Stores st;
    const uint32_t ns = standardNs(cat, st);
    cat.deleteNs(ns);
    Sim sim(cat, st);
    runToEnd(sim, ns);
    EXPECT_EQ(sim.ops, expectedOps(ns)) << join(sim.ops);
    EXPECT_TRUE(cat.erased(ns));
    EXPECT_EQ(cat.applied, (std::vector<std::string>{"step 1", "drop view v1", "drop view v2", "drop topic t1",
                                                    "drop topic t2", "drop topic t3", "step 2", "step 4", "step 8"}));
    for (int i = 0; i < 5; ++i) sim.tick();
    EXPECT_EQ(sim.reaper->stateName(), "Idle");
    EXPECT_EQ(sim.reaper->eventsIssued(), 9u);
}

TEST(NamespaceReaperRegression, EmptyNamespaceErasesRegression) {
    Cat cat; Stores st;
    const uint32_t ns = cat.create("e");
    st.securityRecords[ns] = false;
    cat.deleteNs(ns);
    Sim sim(cat, st);
    ASSERT_TRUE(sim.run([&] { return cat.erased(ns); })) << join(sim.ops);
    const std::string n = std::to_string(ns);
    EXPECT_EQ(sim.ops, (std::vector<std::string>{"abort ns" + n, "step 1", "tombstone-groups ns" + n, "step 2",
                                                 "tombstone-txn ns" + n, "step 4", "step 8"}));
}

TEST(NamespaceReaperRegression, TwoDeletingNamespacesSerialRegression) {
    Cat cat; Stores st;
    const uint32_t a = standardNs(cat, st, "a");
    const uint32_t b = standardNs(cat, st, "b");
    cat.deleteNs(b);   // b is deleted FIRST, but a has the lower nsId: a is reaped first
    cat.deleteNs(a);
    Sim sim(cat, st);
    ASSERT_TRUE(sim.run([&] {
        for (const uint32_t ns : {a, b})
            if (st.securityRecords[ns] && std::find(sim.ops.begin(), sim.ops.end(), "step 4") != sim.ops.end() &&
                cat.rec(ns) != nullptr && std::get<rc::NsDeleting>(cat.rec(ns)->state).stepMask & rc::TxnStateTombstoned)
                st.securityRecords[ns] = false;
        return cat.erased(a) && cat.erased(b);
    })) << join(sim.ops);
    // All of a's events precede every event of b.
    const std::string an = "ns" + std::to_string(a), bn = "ns" + std::to_string(b);
    std::size_t lastA = 0, firstB = sim.ops.size();
    for (std::size_t i = 0; i < sim.ops.size(); ++i) {
        const bool isB = sim.ops[i].find(bn) != std::string::npos;
        const bool isA = sim.ops[i].find(an) != std::string::npos;
        if (isA) lastA = i;
        if (isB && firstB == sim.ops.size()) firstB = i;
    }
    EXPECT_LT(lastA, firstB) << "b's first control op came before a's last: " << join(sim.ops);
    EXPECT_EQ(std::count(cat.applied.begin(), cat.applied.end(), "step 8"), 2);
    EXPECT_EQ(std::count(cat.applied.begin(), cat.applied.end(), "drop topic t1"), 2);
}

TEST(NamespaceReaperRegression, DropRefusedIsRetriedNotSkippedRegression) {
    Cat cat; Stores st;
    const uint32_t ns = standardNs(cat, st);
    cat.deleteNs(ns);
    Sim sim(cat, st);
    sim.refuseSubmits = 3;       // the first three submits find the ring full
    sim.notLeaderAnswers = 2;    // and the first two drops that do go out are answered NotLeader (not applied)
    runToEnd(sim, ns);
    EXPECT_TRUE(cat.erased(ns));
    // Every object was dropped exactly once, in order, whatever was refused on the way.
    EXPECT_EQ(cat.applied, (std::vector<std::string>{"step 1", "drop view v1", "drop view v2", "drop topic t1",
                                                    "drop topic t2", "drop topic t3", "step 2", "step 4", "step 8"}));
    // The refused v1 drop was asked for again BEFORE v2 was ever tried.
    const auto firstV2 = std::find(sim.ops.begin(), sim.ops.end(), "drop-view v2");
    const auto v1Count = std::count(sim.ops.begin(), firstV2, "drop-view v1");
    EXPECT_EQ(v1Count, 3) << "v1: two NotLeader answers, then the one that applied; v2 never ahead of it: " << join(sim.ops);
}

TEST(NamespaceReaperRegression, InFlightResultAfterLeadershipLossIgnoredRegression) {
    Cat cat; Stores st;
    const uint32_t ns = standardNs(cat, st);
    cat.deleteNs(ns);
    Sim sim(cat, st);
    // Up to the first drop in flight, results held back (a slow admin thread).
    ASSERT_TRUE(sim.run([&] { return !sim.ops.empty() && sim.ops.back() == "step 1"; }));
    sim.tick();                                   // delivers the step's result
    sim.tick();                                   // the reaper issues the first drop (v1)
    ASSERT_EQ(sim.ops.back(), "drop-view v1");
    ASSERT_EQ(sim.inbox.size(), 1u);
    const auto stale = sim.inbox.front();         // the result of the drop issued under the old leadership
    sim.inbox.clear();
    sim.reaper->onLeadershipLost();
    EXPECT_EQ(sim.reaper->stateName(), "Idle");
    sim.reaper->onDdlResult(stale.corr, stale.code);    // arrives late: Idle ignores it
    EXPECT_EQ(sim.reaper->stateName(), "Idle");
    // Leadership regained: the reaper re-derives; v1 was applied by the old leader's drop, so the next op is v2.
    sim.tick();
    EXPECT_EQ(sim.ops.back(), "drop-view v2");
    const auto* dv = std::get_if<ReapDroppingViews>(&sim.reaper->state());
    ASSERT_NE(dv, nullptr);
    ASSERT_NE(dv->inFlightCorrelation, 0u);
    EXPECT_NE(dv->inFlightCorrelation, stale.corr) << "a new correlation";
    sim.reaper->onDdlResult(stale.corr, stale.code);    // the stale one again, now in a DroppingViews state
    const auto* dv2 = std::get_if<ReapDroppingViews>(&sim.reaper->state());
    ASSERT_NE(dv2, nullptr);
    EXPECT_NE(dv2->inFlightCorrelation, 0u) << "a stale result must not clear the new in-flight slot";
    runToEnd(sim, ns);
    EXPECT_TRUE(cat.erased(ns));
}

// A crash (a fresh reaper: Idle, in-flight lost) after each of the 7 durable events of the standard run (and before the
// first), and in the middle of the tombstone batches: the run completes, the end state and the sequence of applied
// catalog events equal the run that never crashed, and no event was applied twice.
// Mutation: a reaper that starts Idle forever after a restart (never re-derives) -> every crash point fails.
TEST(NamespaceReaperRegression, CrashAtEveryStepEndStateEqualsNoCrashRegression) {
    std::vector<std::string> reference;
    {
        Cat cat; Stores st;
        const uint32_t ns = standardNs(cat, st);
        cat.deleteNs(ns);
        Sim sim(cat, st);
        runToEnd(sim, ns);
        reference = cat.applied;
        ASSERT_EQ(reference.size(), 9u);
    }
    int points = 0;
    for (int events = 0; events <= 9; ++events) {       // 9 reaper events: step 1, 2 views, 3 topics, steps 2, 4, 8
        SCOPED_TRACE("crash after " + std::to_string(events) + " events");
        Cat cat; Stores st;
        const uint32_t ns = standardNs(cat, st);
        cat.deleteNs(ns);
        Sim sim(cat, st);
        st.securityRecords[ns] = false;                 // the __security purge already applied: step 8 only waits for its turn
        sim.reaper->setIssueBudgetForTest(events);
        for (int i = 0; i < 400; ++i) sim.tick();       // runs until the budget stops it
        EXPECT_EQ(sim.reaper->eventsIssued(), static_cast<uint64_t>(events));
        sim.crash();                                    // the reaper object is gone; the broker's stores and catalog stay
        ASSERT_TRUE(std::holds_alternative<ReapIdle>(sim.reaper->state()));
        runToEnd(sim, ns);
        EXPECT_EQ(cat.applied, reference) << "end sequence differs from the no-crash run";
        EXPECT_TRUE(cat.erased(ns));
        ++points;
    }
    EXPECT_EQ(points, 10);
}

// The same crash, inside the tombstone batches themselves: after j of the 4 group calls the reaper dies; the stores keep
// what was written, the fresh reaper continues (a tombstone of an absent key is harmless).
TEST(NamespaceReaperRegression, CrashInsideTombstoneBatchesRegression) {
    for (int calls = 1; calls <= 4; ++calls) {
        SCOPED_TRACE("crash after " + std::to_string(calls) + " group calls");
        Cat cat; Stores st;
        const uint32_t ns = standardNs(cat, st);
        cat.deleteNs(ns);
        Sim sim(cat, st);
        ASSERT_TRUE(sim.run([&] {
            return std::count(sim.ops.begin(), sim.ops.end(), "tombstone-groups ns" + std::to_string(ns)) >= calls;
        }));
        sim.crash();
        runToEnd(sim, ns);
        EXPECT_TRUE(cat.erased(ns));
        EXPECT_EQ(st.groupKeys[ns], 0) << "every group key was tombstoned";
        EXPECT_EQ(std::count(cat.applied.begin(), cat.applied.end(), "step 2"), 1);
    }
}

// ── real AdminWorkers (three brokers) ─────────────────────────────────────────────────────────────────────────
namespace {

struct TrioFix {
    CatalogTrio trio;
    uint32_t ns = 0;
    explicit TrioFix(const char* tag) : trio(tag) {
        const int64_t c = trio.submitCreateNamespaceNoWait(1, "a", {});
        EXPECT_EQ(trio.awaitAnswer(1, c).code, 0);
        ns = trio.admin(1).applier().state().namespaces().byName("a")->nsId;
        for (const char* t : {"t1", "t2"}) EXPECT_EQ(trio.define(1, {.name = t, .partitions = 1, .nsId = ns}).code, 0);
        EXPECT_TRUE(trio.awaitConverged());
    }
    void deleteNs() {
        NamespaceDdlMsg m;
        m.session = CatalogTrio::kSession; m.corr = 90'000; m.kind = 5; m.name = "a"; m.clientVersion = 14;
        m.meta.sessionId = CatalogTrio::kSession;
        m.meta.requestId = rp::RequestId{0x7710, 90'000};
        trio.admin(1).submit(DdlRequest{std::move(m)});
        for (int i = 0; i < 4; ++i) trio.pump();
    }
    [[nodiscard]] bool erasedOn(int32_t b) { return trio.admin(b).applier().state().namespaces().byId(ns) == nullptr; }
    bool awaitErased(int32_t b) {
        for (int i = 0; i < CatalogTrio::kCap && !erasedOn(b); ++i) trio.pump();
        return erasedOn(b);
    }
    /// How many events of `kind` the leader's log holds for this namespace.
    std::size_t count(int32_t b, rc::EventKind kind) {
        std::size_t n = 0;
        for (const auto& r : trio.recordedLog(b)) n += r.event.kind == kind && r.event.nsId == ns ? 1 : 0;
        return n;
    }
};

}  // namespace

// The leadership moves after each durable step the reaper took (0..6 of its 6 events: step 1, two topic drops, steps 2, 4,
// 8): the new leader's reaper re-derives from the applied registry and finishes; no step or drop is applied twice.
TEST(NamespaceReaperRegression, LeaderChangeAtEveryStepRegression) {
    for (int events = 0; events <= 6; ++events) {
        SCOPED_TRACE("leader change after " + std::to_string(events) + " reaper events");
        TrioFix f(("reaper_lc" + std::to_string(events)).c_str());
        f.trio.admin(1).reaperForTest().setIssueBudgetForTest(events);
        f.deleteNs();
        // The old leader takes its `events` steps and stops (the budget); everything it appended commits and applies.
        for (int i = 0; i < 2000; ++i) f.trio.pump();
        ASSERT_TRUE(f.trio.awaitConverged());
        if (events < 6) ASSERT_FALSE(f.erasedOn(1)) << "the budget should have stopped the reaper before the end";
        EXPECT_EQ(f.trio.admin(1).reaper().eventsIssued(), static_cast<uint64_t>(events));
        f.trio.moveCatalogLeadership(2);
        ASSERT_TRUE(f.trio.awaitRole(2, "LeaderServing", std::chrono::seconds(10)));
        ASSERT_TRUE(f.awaitErased(2)) << "the new leader never finished; its reaper is " << f.trio.admin(2).reaper().stateName();
        ASSERT_TRUE(f.trio.awaitConverged());
        for (const int32_t b : {1, 2, 3}) EXPECT_TRUE(f.erasedOn(b)) << "broker " << b;
        EXPECT_TRUE(f.trio.allDigestsEqual());
        EXPECT_EQ(f.count(2, rc::EventKind::NamespaceDeleted), 1u);
        EXPECT_EQ(f.count(2, rc::EventKind::DropTopic), 2u) << "each topic dropped exactly once";
        EXPECT_EQ(f.count(2, rc::EventKind::NamespaceDeleteStep), 4u) << "each of the four step bits recorded exactly once";
    }
}

// No leader change: the catalog leader's own reaper runs the standard delete to the end (the AdminWorker wiring).
TEST(NamespaceReaperRegression, AdminWorkerDrivesDeleteToEndRegression) {
    TrioFix f("reaper_e2e");
    f.deleteNs();
    ASSERT_TRUE(f.awaitErased(1));
    ASSERT_TRUE(f.trio.awaitConverged());
    for (const int32_t b : {1, 2, 3}) EXPECT_TRUE(f.erasedOn(b));
    // The name is free at once and the id is never reused.
    const int64_t c = f.trio.submitCreateNamespaceNoWait(1, "a", {});
    EXPECT_EQ(f.trio.awaitAnswer(1, c).code, 0);
    EXPECT_GT(f.trio.admin(1).applier().state().namespaces().byName("a")->nsId, f.ns);
}
