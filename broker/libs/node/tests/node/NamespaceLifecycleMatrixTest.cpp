// NamespaceLifecycleMatrixTest.cpp - namespaces Phase 2 Task 17 (spec §8): the namespace LIFECYCLE interaction matrix
// (support/NamespaceLifecycleMatrixExpectations.h): 9 ops x 6 callers x 6 targets = 324 cells (24 n/a, 300 applicable)
// plus 36 timing cells = 336 executed.  Every cell's expectation comes from expect(), never from this file.
//
//   * Cell/*   - the 300 applicable sequential cells on ONE real authenticating broker (default config; the Anonymous
//                cells on a second broker with anonymous read on), via support/NamespaceLifecycleRig.h.
//   * Timing/* - the 36 timing cells: back-to-back (9), crash mid-delete (7, one real broker restarted after each delete
//                step), leader change mid-delete (7, three real AdminWorkers on CatalogTrio), follower catch-up (3,
//                CatalogTrio), limit change mid-tick (5, the real NsLimitAggregator on its injected clock), recreate
//                under Throttle (2: a worker's ring and the gateway's connection count, real broker), txn open (3,
//                real client against a real broker).
//
// Interaction matrix (op x caller x target x timing): see the expectations header; the counts are pinned by
// NamespaceLifecycleMatrixCountRegression.PinnedRegression.
#include "support/CatalogTrio.h"
#include "support/NamespaceLifecycleMatrixExpectations.h"
#include "support/NamespaceLifecycleRig.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/node/NamespacePaths.h>
#include <rune/node/NamespaceReaper.h>
#include <rune/node/NsLimitAggregator.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::nslc;
namespace pc = rune::protocol::client;
namespace rc = rune::client;
using namespace std::chrono_literals;

TEST(NamespaceLifecycleMatrixCountRegression, PinnedRegression) {
    const LcCounts k = countCells();
    EXPECT_EQ(k.cells, 324u);
    EXPECT_EQ(k.na, 24u);
    EXPECT_EQ(k.applicable, 300u);
    EXPECT_EQ(k.timing, 36u);
    EXPECT_EQ(k.executed, 336u);
    EXPECT_EQ(timingVariants(LcTiming::BackToBack), kLcOps);
    EXPECT_EQ(static_cast<std::size_t>(kDeletePoints), timingVariants(LcTiming::CrashMidDelete));
}

// ── the 300 sequential cells ──────────────────────────────────────────────────────────────────────────────────────
namespace {

struct Cell { LcOp op; LcCaller caller; LcTarget target; int ordinal; };
std::vector<Cell> allCells() {
    std::vector<Cell> v;
    int ord = 0;
    for (std::size_t o = 0; o < kLcOps; ++o)
        for (std::size_t c = 0; c < kLcCallers; ++c)
            for (std::size_t t = 0; t < kLcTargets; ++t)
                if (!naRule(LcOp(o), LcCaller(c), LcTarget(t))) v.push_back({LcOp(o), LcCaller(c), LcTarget(t), ord++});
    return v;
}
std::string cellName(const Cell& c) {
    return std::string(name(c.op)) + "_" + name(c.caller) + "_" + name(c.target);
}

}  // namespace

class NamespaceLifecycleMatrixRegression : public ::testing::TestWithParam<Cell> {
protected:
    static void SetUpTestSuite() { main_ = std::make_unique<LcRig>("nslcmx"); }
    static void TearDownTestSuite() { main_.reset(); anon_.reset(); }
    static LcRig& rigFor(LcCaller c) {
        if (c != LcCaller::Anonymous) return *main_;
        if (!anon_) anon_ = std::make_unique<LcRig>("nslcmxanon", /*anonymous=*/true);
        return *anon_;
    }
    static inline std::unique_ptr<LcRig> main_, anon_;
};

TEST_P(NamespaceLifecycleMatrixRegression, Cell) {
    const Cell& p = GetParam();
    LcRig& rig = rigFor(p.caller);
    const LcOutcome want = expect(p.op, p.caller, p.target);
    const std::string nm = LcRig::nameOf(p.target, p.ordinal, false);
    const LcRig::Cell got = rig.run(p.op, p.caller, p.target, p.ordinal);
    const std::string label = cellName(p) + ": want " + describe(want) + ", got code " + std::to_string(got.a.code) +
                              " [" + got.a.detail + "] after=" + name(got.after) + (got.deletingEntry ? "+deleting" : "");
    {   // every observed code is one the matrix can emit
        const auto codes = expectedCodes();
        EXPECT_TRUE(got.a.code == 0 || std::find(codes.begin(), codes.end(), static_cast<ErrorCode>(got.a.code)) != codes.end())
            << label << ": code " << got.a.code << " is not in expectedCodes()";
    }
    const bool loginTime = (p.caller == LcCaller::NsAdminSame || p.caller == LcCaller::UserInNs) &&
                           (p.target == LcTarget::SuspendedRA || p.target == LcTarget::Deleting);
    EXPECT_EQ(got.loginRefused, loginTime) << label << ": the op is sent iff the caller could log in";
    switch (want.kind) {
        case LcOutcome::Ok:
        case LcOutcome::OkWithWarning:
            EXPECT_EQ(got.a.code, 0) << label;
            if (want.kind == LcOutcome::OkWithWarning)
                EXPECT_EQ(got.warning.rfind("retention clamped on ", 0), 0u) << label << ": the warning";
            else
                EXPECT_TRUE(got.warning.empty()) << label << ": no warning on a plain Ok";
            break;
        case LcOutcome::Refused:
            EXPECT_EQ(got.a.code, static_cast<int>(want.code)) << label;
            break;
        case LcOutcome::Missing: {
            EXPECT_EQ(got.a.code, static_cast<int>(want.code)) << label;
            // Exactly the answer for an unknown name: the same op on a never-created twin gives the same bytes.
            LcRig::Cell twin = rig.run(p.op, p.caller, p.target, p.ordinal, /*twin=*/true);
            EXPECT_EQ(twin.a.code, got.a.code) << label << " (twin)";
            LcRig::Cell g = got;
            g.a.askedNs = nm;
            twin.a.askedNs = LcRig::nameOf(p.target, p.ordinal, true);
            EXPECT_EQ(g.a.normalized(twin.a), twin.a.normalized(g.a)) << label << ": not byte-equal to an unknown name's answer";
            break;
        }
    }
    // The target's state by name, from a follow-up listing by the cluster admin.
    if (want.after == LcTarget::Deleting) {
        EXPECT_EQ(got.after, LcTarget::Missing) << label << ": a DELETING name resolves to nothing";
        EXPECT_TRUE(got.deletingEntry) << label << ": the DELETING entry is listed";
    } else {
        EXPECT_EQ(got.after, want.after) << label;
    }
    const bool ran = p.caller == LcCaller::ClusterAdmin;
    if (ran && want.kind == LcOutcome::Ok && p.op == LcOp::Delete) EXPECT_TRUE(got.deletingEntry) << label << ": a DELETING entry (reaper held)";
    if (ran && want.kind == LcOutcome::Ok && p.op == LcOp::Recreate) {
        EXPECT_NE(got.nsIdAfter, 0u) << label;
        EXPECT_NE(got.nsIdAfter, got.nsIdBefore) << label << ": a recreated name gets a new nsId";
    }
    if (ran && want.kind == LcOutcome::Ok && p.op == LcOp::SetLimitsLowerBelowUsage) {
        EXPECT_TRUE(got.topicsRemain) << label << ": lowering a limit deletes nothing";
        EXPECT_EQ(got.followUpCode, static_cast<int>(ErrorCode::NamespaceLimitExceeded)) << label << ": a new topic is refused";
    }
    // #708: the stored limit is read back for every accepted cell, the Default target included, and LowerBelowUsage too.
    if (ran && want.kind != LcOutcome::Missing &&
        (p.op == LcOp::SetLimitsRaise || p.op == LcOp::SetLimitsLowerRetention || p.op == LcOp::SetLimitsLowerBelowUsage)) {
        const auto& o = got;
        if (p.op == LcOp::SetLimitsRaise) EXPECT_GT(o.limitsAfter.maxTopics, 5000u) << label << ": the raised limit is stored";
        else if (p.op == LcOp::SetLimitsLowerBelowUsage) EXPECT_EQ(o.limitsAfter.maxTopics, 1u) << label << ": the lowered topic limit is stored";
        else EXPECT_EQ(o.limitsAfter.maxRetentionMs, 1u) << label << ": the lowered retention is stored";
    }
    // A refused SetLimits changed no stored limit: read back the way the accepted cells do.
    if (!ran && !got.loginRefused && (p.op == LcOp::SetLimitsRaise || p.op == LcOp::SetLimitsLowerBelowUsage ||
                                      p.op == LcOp::SetLimitsLowerRetention)) {
        for (std::size_t i = 0; i < rune::protocol::kNamespaceLimitFieldCount; ++i)
            EXPECT_EQ(LcRig::getField(got.limitsAfter, static_cast<int>(i)), LcRig::getField(got.limitsBefore, static_cast<int>(i)))
                << label << ": limit field " << i << " changed by a refused SetLimits";
    }
    // A caller that is not the cluster admin changed nothing anywhere else: the target is as prepared.
    if (!ran) {
        const LcTarget was = p.target == LcTarget::Deleting ? LcTarget::Missing : p.target;
        EXPECT_EQ(got.after, was) << label << ": a refused caller changed the target";
    }
}

INSTANTIATE_TEST_SUITE_P(All, NamespaceLifecycleMatrixRegression, ::testing::ValuesIn(allCells()),
                         [](const auto& info) { return cellName(info.param); });

// ── the 36 timing cells ───────────────────────────────────────────────────────────────────────────────────────────
namespace {

struct TCell { LcTiming timing; int variant; };
std::vector<TCell> timingCells() {
    std::vector<TCell> v;
    for (const LcTiming tm : {LcTiming::BackToBack, LcTiming::CrashMidDelete, LcTiming::LeaderChangeMidDelete,
                              LcTiming::FollowerCatchUp, LcTiming::LimitChangeMidTick, LcTiming::RecreateUnderThrottle,
                              LcTiming::TxnOpen})
        for (std::size_t i = 0; i < timingVariants(tm); ++i) v.push_back({tm, static_cast<int>(i)});
    return v;
}
std::string tName(const TCell& c) {
    std::string v = std::to_string(c.variant);
    switch (c.timing) {
        case LcTiming::BackToBack: v = name(LcOp(c.variant)); break;
        case LcTiming::CrashMidDelete:
        case LcTiming::LeaderChangeMidDelete: v = deletePointName(c.variant); break;
        case LcTiming::FollowerCatchUp: v = c.variant == 0 ? "Suspend" : c.variant == 1 ? "Delete" : "Recreate"; break;
        case LcTiming::LimitChangeMidTick: v = rateField(c.variant); std::replace(v.begin(), v.end(), '-', '_'); break;
        case LcTiming::RecreateUnderThrottle: v = c.variant == 0 ? "WorkerRing" : "Gateway"; break;
        case LcTiming::TxnOpen: v = c.variant == 0 ? "SuspendRO" : c.variant == 1 ? "SuspendRA" : "Delete"; break;
        default: break;
    }
    return std::string(name(c.timing)) + "_" + v;
}

constexpr int kPollCap = 3000;   // 10 ms polls: a hard cap that FAILS (30 s), never a hang

// ── back-to-back ──
void backToBack(const TCell& p, LcRig& rig) {
    const LcOp op = LcOp(p.variant);
    const LcTarget tgt = op == LcOp::Recreate ? LcTarget::Missing : LcTarget::Active;
    const int ord = 5000 + p.variant;
    const std::string nm = LcRig::nameOf(tgt, ord, false);
    rig.holdReaper();   // a DELETING namespace stays listed while the cell observes it
    rig.prepare(nm, tgt, ord, LcCaller::ClusterAdmin);
    const auto cur = rig.observe(nm).limits;
    const int64_t c1 = rig.sendOp(op, rig.rootSid(), tgt, nm, cur);
    const int64_t c2 = rig.sendOp(op, rig.rootSid(), tgt, nm, cur);   // before the first reply
    const auto a1 = rig.await(c1);
    const auto a2 = rig.await(c2);
    const LcOutcome first = expect(op, LcCaller::ClusterAdmin, tgt);
    const LcOutcome second = backToBackSecond(op);
    const std::string label = tName(p) + ": ";
    EXPECT_EQ(a1.code, static_cast<int>(first.code)) << label << "first [" << a1.detail << "]";
    EXPECT_EQ(a2.code, static_cast<int>(second.code)) << label << "second [" << a2.detail << "]";
    const auto o = rig.observe(nm);
    if (second.after == LcTarget::Missing) EXPECT_EQ(o.byName, LcTarget::Missing) << label << "final state";
    else EXPECT_EQ(o.byName, second.after) << label << "final state";
    if (op == LcOp::Delete) EXPECT_TRUE(o.deleting) << label << "exactly one DELETING entry";
    if (op == LcOp::Delete || op == LcOp::Recreate) {
        std::size_t n = 0;
        for (const auto& s : rig.list()) n += s.name == nm ? 1 : 0;
        EXPECT_EQ(n, 1u) << label << "the op took effect exactly once";
    }
    if (rig.observe(nm).byName != LcTarget::Missing) (void)rig.ns(rig.rootSid(), pc::NamespaceDdlKind::Delete, nm);
    rig.releaseReaper();
}

// ── crash mid-delete ──
void crashMidDelete(const TCell& p) {
    const int point = p.variant;
    LcRig rig("nslccrash" + std::to_string(point));
    const std::string nm = "d";
    const int64_t userSid = rig.seedDeleteWorkload(nm);
    const uint32_t id = rig.observe(nm).nsId;
    ASSERT_NE(id, 0u);
    rig.setBudget(reaperEventsAtPoint(point));
    ASSERT_EQ(rig.ns(rig.rootSid(), pc::NamespaceDdlKind::Delete, nm).code, 0);
    EXPECT_TRUE(rig.waitSessionGone(userSid)) << "the delete fences the namespace's sessions";
    ASSERT_TRUE(rig.until([&] { return rig.reaperEventsIssued() == static_cast<uint64_t>(reaperEventsAtPoint(point)); }))
        << "the reaper never issued its " << point << " events";
    if (point < 6) EXPECT_FALSE(rig.erased(id)) << "stopped at point " << deletePointName(point);
    // The crash: the broker stops and starts again over the same data; its reaper starts Idle.
    rig.restart(/*holdReaper=*/false);
    ASSERT_TRUE(rig.until([&] { return rig.erased(id); })) << "removal did not complete after the restart";
    EXPECT_FALSE(std::filesystem::exists(nsRoot(rig.dataDir(), id))) << "ns-<id> directory";
    EXPECT_FALSE(std::filesystem::exists(std::filesystem::path(nsRoot(rig.dataDir(), id).string() + ".deleting")));
    EXPECT_EQ(rig.tryLogin("d/u").code, static_cast<int>(ErrorCode::AuthenticationFailed)) << "the namespace's user is gone";
    EXPECT_TRUE(rig.auth().keyedStoresCleanBounded(id)) << "a live consumer-offset or transaction-state key of the deleted namespace";
    EXPECT_TRUE(rig.nsSlotFreed(id)) << "the deleted namespace's slot is still held";
    EXPECT_EQ(rig.catalogEventCount(rune::catalog::EventKind::DropTopic, id), 2u) << "each topic dropped exactly once";
    EXPECT_EQ(rig.catalogEventCount(rune::catalog::EventKind::NamespaceDeleteStep, id), 4u) << "each step bit recorded exactly once";
    EXPECT_EQ(rig.catalogEventCount(rune::catalog::EventKind::NamespaceDeleted, id), 1u);
    const auto oldTopics = rig.topicIds(nm);
    ASSERT_EQ(oldTopics.size(), 2u);
    for (const int32_t t : oldTopics)
        EXPECT_EQ(rig.auth().produce(rig.rootSid(), t), static_cast<int>(ErrorCode::UnknownTopic)) << "old topic id " << t;
    // The name is free and the new namespace sees nothing of the old one.
    ASSERT_EQ(rig.ns(rig.rootSid(), pc::NamespaceDdlKind::Create, nm).code, 0);
    const auto again = rig.observe(nm);
    EXPECT_NE(again.nsId, id);
    EXPECT_EQ(again.byName, LcTarget::Active);
    {   // the re-created namespace lists none of the old namespace's topics
        const auto md = rig.rootMetadata();
        for (const int32_t t : oldTopics) EXPECT_FALSE(std::binary_search(md.begin(), md.end(), t)) << "old topic " << t << " listed";
    }
    EXPECT_EQ(rig.tryLogin("d/u").code, static_cast<int>(ErrorCode::AuthenticationFailed)) << "no old user";
}

// ── three real AdminWorkers ──
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
    int64_t submit(int32_t b, uint8_t kind, const std::string& name, uint8_t mode = 1) {
        NamespaceDdlMsg m;
        m.session = CatalogTrio::kSession; m.corr = 90'000 + ++seq_; m.kind = kind; m.name = name; m.mode = mode;
        m.clientVersion = 14;
        m.meta.sessionId = CatalogTrio::kSession;
        m.meta.requestId = rune::protocol::RequestId{0x7710, static_cast<uint64_t>(m.corr)};
        const int64_t corr = m.corr;
        trio.admin(b).submit(DdlRequest{std::move(m)});
        return corr;
    }
    void deleteNs() { (void)submit(1, 5, "a"); for (int i = 0; i < 4; ++i) trio.pump(); }
    [[nodiscard]] bool erasedOn(int32_t b) { return trio.admin(b).applier().state().namespaces().byId(ns) == nullptr; }
    bool awaitErased(int32_t b) {
        for (int i = 0; i < CatalogTrio::kCap && !erasedOn(b); ++i) trio.pump();
        return erasedOn(b);
    }
    [[nodiscard]] std::size_t count(int32_t b, rune::catalog::EventKind kind) {
        std::size_t n = 0;
        for (const auto& r : trio.recordedLog(b)) n += r.event.kind == kind && r.event.nsId == ns ? 1 : 0;
        return n;
    }
    int seq_ = 0;
};

void leaderChangeMidDelete(const TCell& p) {
    const int point = p.variant;
    TrioFix f(("lcdel" + std::to_string(point)).c_str());
    f.trio.admin(1).reaperForTest().setIssueBudgetForTest(reaperEventsAtPoint(point));
    f.deleteNs();
    for (int i = 0; i < 2000; ++i) f.trio.pump();
    ASSERT_TRUE(f.trio.awaitConverged());
    if (point < 6) ASSERT_FALSE(f.erasedOn(1)) << "the budget should have stopped the reaper before the end";
    EXPECT_EQ(f.trio.admin(1).reaper().eventsIssued(), static_cast<uint64_t>(point));
    f.trio.moveCatalogLeadership(2);
    ASSERT_TRUE(f.trio.awaitRole(2, "LeaderServing", std::chrono::seconds(10)));
    ASSERT_TRUE(f.awaitErased(2)) << "the new leader never finished; its reaper is " << f.trio.admin(2).reaper().stateName();
    ASSERT_TRUE(f.trio.awaitConverged());
    for (const int32_t b : {1, 2, 3}) EXPECT_TRUE(f.erasedOn(b)) << "broker " << b;
    EXPECT_TRUE(f.trio.allDigestsEqual());
    EXPECT_EQ(f.count(2, rune::catalog::EventKind::NamespaceDeleted), 1u);
    EXPECT_EQ(f.count(2, rune::catalog::EventKind::DropTopic), 2u) << "each topic dropped exactly once";
    EXPECT_EQ(f.count(2, rune::catalog::EventKind::NamespaceDeleteStep), 4u) << "each step bit recorded exactly once";
}

// ── a follower offline across the op, then caught up ──
void followerCatchUp(const TCell& p) {
    TrioFix f(("lccatch" + std::to_string(p.variant)).c_str());
    f.trio.isolate(3);
    switch (p.variant) {
        case 0: EXPECT_EQ(f.trio.awaitAnswer(1, f.submit(1, 3, "a", 2)).code, 0); break;   // Suspend RejectAll
        case 1: f.deleteNs(); ASSERT_TRUE(f.awaitErased(1)); break;
        default: {
            f.deleteNs();
            ASSERT_TRUE(f.awaitErased(1));
            const int64_t c = f.trio.submitCreateNamespaceNoWait(1, "a", {});
            EXPECT_EQ(f.trio.awaitAnswer(1, c).code, 0);
            break;
        }
    }
    f.trio.deisolate(3);
    ASSERT_TRUE(f.trio.awaitConverged());
    ASSERT_TRUE(p.variant != 1 || f.awaitErased(3)) << "the follower never saw the erase";
    for (int i = 0; i < 200; ++i) f.trio.pump();
    ASSERT_TRUE(f.trio.awaitConverged());
    EXPECT_TRUE(f.trio.allDigestsEqual()) << "the follower's catalog differs from the leader's";
    const auto* lead = f.trio.admin(1).applier().state().namespaces().byName("a");
    const auto* foll = f.trio.admin(3).applier().state().namespaces().byName("a");
    ASSERT_EQ(lead == nullptr, foll == nullptr) << "the name resolves on one of them only";
    if (lead != nullptr) {
        EXPECT_EQ(lead->nsId, foll->nsId);
        EXPECT_EQ(rune::catalog::tagOf(lead->state), rune::catalog::tagOf(foll->state));
        if (p.variant == 0) EXPECT_EQ(rune::catalog::tagOf(foll->state), rune::catalog::NsStateTag::SuspendedRejectAll);
        if (p.variant == 2) EXPECT_NE(lead->nsId, f.ns) << "a recreated name has a new id on every broker";
    }
    EXPECT_EQ(f.erasedOn(3), f.erasedOn(1));
}

// ── a SetLimits between the aggregator's read and its broadcast ──
void limitChangeMidTick(const TCell& p) {
    struct RateCase { std::size_t index; ThrottleKind kind; NsMetric metric; };
    static constexpr RateCase kCases[] = {
        {13, ThrottleKind::ProduceBytes, NsMetric::ProduceBytes},
        {14, ThrottleKind::ProduceRecords, NsMetric::ProduceRecords},
        {15, ThrottleKind::FetchBytes, NsMetric::FetchBytes},
        {16, ThrottleKind::RequestOps, NsMetric::RequestOps},
        {17, ThrottleKind::QueryCycles, NsMetric::QueryCycles},
    };
    const RateCase& rc = kCases[p.variant];
    constexpr int64_t kTick = 10'000'000;
    constexpr uint16_t kSlot = 1;
    constexpr uint32_t kNs = 7;
    auto limitsWith = [&](uint64_t perSec) {
        rune::protocol::NamespaceLimits l;
        LcRig::setField(l, static_cast<int>(rc.index), perSec);
        return l;
    };
    NsLimitAggregator agg;
    agg.configure(8, 1, kTick);
    int64_t now = 1'000'000'000;
    (void)agg.doWork(now);
    agg.onSlotAssign(kSlot, kNs, limitsWith(1000));
    std::vector<Throttle> got;
    auto drain = [&] { agg.drain([&](const Throttle& t) { got.push_back(t); }, [](const ViewMemOver&) {}); };
    ASSERT_TRUE(agg.report(0, kSlot, kNs, rc.metric, 100));   // limit x tick = 10: debt 90 -> 90 ms
    now += kTick;
    (void)agg.doWork(now);                                      // the read
    agg.onSlotAssign(kSlot, kNs, limitsWith(1'000'000));        // the SetLimits lands before the broadcast
    drain();
    ASSERT_EQ(got.size(), 1u) << tName(p);
    EXPECT_EQ(got[0].kind, rc.kind);
    EXPECT_EQ(got[0].untilNs, now + 90'000'000) << "this tick's decision is the old limit's";
    now += kTick;
    (void)agg.doWork(now);                                      // the next tick applies the new limit
    drain();
    ASSERT_EQ(got.size(), 2u) << tName(p) << ": the next tick must act on the new limit";
    EXPECT_EQ(got[1].kind, rc.kind);
    EXPECT_EQ(got[1].untilNs, 0) << "the raised limit clears the throttle";
    // And the other direction: lowered between a read and a broadcast, the next tick prolongs it.
    NsLimitAggregator a2;
    a2.configure(8, 1, kTick);
    int64_t n2 = 1'000'000'000;
    (void)a2.doWork(n2);
    a2.onSlotAssign(kSlot, kNs, limitsWith(1000));
    ASSERT_TRUE(a2.report(0, kSlot, kNs, rc.metric, 100));
    n2 += kTick;
    (void)a2.doWork(n2);
    a2.onSlotAssign(kSlot, kNs, limitsWith(100));
    std::vector<Throttle> g2;
    a2.drain([&](const Throttle& t) { g2.push_back(t); }, [](const ViewMemOver&) {});
    ASSERT_EQ(g2.size(), 1u);
    n2 += kTick;
    (void)a2.doWork(n2);
    a2.drain([&](const Throttle& t) { g2.push_back(t); }, [](const ViewMemOver&) {});
    ASSERT_EQ(g2.size(), 2u);
    EXPECT_GT(g2[1].untilNs, g2[0].untilNs) << "a lowered limit prolongs the deadline";
}

// ── delete and recreate while a Throttle for the old nsId is in flight ──
void recreateUnderThrottleWorker() {
    LcRig rig("nslcthrw", false, /*holdReaper=*/false);
    BrokerNode& node = rig.broker().node();
    rig.createNs("a");
    const uint32_t oldId = rig.observe("a").nsId;
    ASSERT_NE(oldId, 0u);
    auto slotOf = [&](uint32_t id) {
        return node.onWorkerThreadForTest(0, [id](Worker& w) {
            const NsSlotTable& t = w.nsSlotsForTest();
            for (uint32_t s = 0; s < t.capacity(); ++s)
                if (t.nsIdAt(static_cast<uint16_t>(s)) == id) return static_cast<int>(s);
            return -1;
        });
    };
    ASSERT_TRUE(rig.until([&] { return slotOf(oldId) >= 0; })) << "the worker never got the slot";
    const int slot = slotOf(oldId);
    ASSERT_EQ(rig.ns(rig.rootSid(), pc::NamespaceDdlKind::Delete, "a").code, 0);
    ASSERT_TRUE(rig.until([&] { return rig.erased(oldId); }));
    rig.createNs("a");
    const uint32_t newId = rig.observe("a").nsId;
    ASSERT_NE(newId, oldId);
    ASSERT_TRUE(rig.until([&] { return slotOf(newId) == slot; })) << "the lowest free slot is reused by the new namespace";
    const int32_t tid = rig.auth().defineTopic(rig.rootSid(), "a/t");
    ASSERT_GE(tid, 0);
    // The aggregator's in-flight Throttle for the OLD owner arrives after the recreate.
    const auto res = node.onWorkerThreadForTest(0, [&](Worker& w) {
        struct R { uint64_t mismatches; int64_t until; };
        const uint64_t before = w.nsSlotMismatchesForTest();
        w.applyNsThrottleDirect(static_cast<uint16_t>(slot), oldId, ThrottleKind::ProduceBytes, INT64_MAX);
        w.applyNsThrottleDirect(static_cast<uint16_t>(slot), oldId, ThrottleKind::Disk, INT64_MAX);
        return R{w.nsSlotMismatchesForTest() - before, w.nsThrottleUntilForTest(static_cast<uint16_t>(slot), ThrottleKind::ProduceBytes)};
    });
    EXPECT_EQ(res.mismatches, 2u) << "the stale messages are dropped and counted";
    EXPECT_EQ(res.until, 0) << "the new owner is never throttled by the old one's message";
    EXPECT_EQ(rig.auth().produce(rig.rootSid(), tid, /*untilOk=*/true), 0) << "the new namespace produces";
}

void recreateUnderThrottleGateway() {
    LcRig rig("nslcthrg", false, /*holdReaper=*/false);
    constexpr int kMaxConnections = 18;
    using K = pc::NamespaceDdlKind;
    const int64_t root = rig.rootSid();
    auto make = [&] {
        const int64_t c = rig.sendNs(root, K::Create, "a", 1, {}, kMaxConnections, 1);
        ASSERT_EQ(rig.await(c).code, 0);
        for (const char* u : {"a/u1", "a/u2"})
            rig.broker().createUser(u, AuthRig::kPw, {grants::prefix(grants::SO::Read, grants::ST::Topic, "")});
    };
    make();
    const uint32_t oldId = rig.observe("a").nsId;
    const auto first = rig.tryLogin("a/u1");
    ASSERT_EQ(first.code, 0);
    EXPECT_EQ(rig.tryLogin("a/u2").code, static_cast<int>(ErrorCode::NamespaceLimitExceeded)) << "premise: the one connection is taken";
    ASSERT_EQ(rig.ns(root, K::Delete, "a").code, 0);
    ASSERT_TRUE(rig.until([&] { return rig.erased(oldId); }));
    EXPECT_TRUE(rig.waitSessionGone(first.sid));
    make();
    ASSERT_NE(rig.observe("a").nsId, oldId);
    // The reused slot starts from zero usage: the old namespace's session is not counted against the new one.
    const auto fresh = rig.tryLogin("a/u1");
    EXPECT_EQ(fresh.code, 0) << "a recreated namespace inherits nothing of the old connection count";
    EXPECT_EQ(rig.tryLogin("a/u2").code, static_cast<int>(ErrorCode::NamespaceLimitExceeded)) << "and its own limit applies";
}

// ── an open transaction when the op lands ──
constexpr const char* kAdminPw = "namespace-admin-password-1";
constexpr const char* kUserPw = "namespace-user-password-12";

std::unique_ptr<rc::Client> newClient(AuthBroker& b, const std::string& user, std::vector<rc::TopicSpec> topics = {}) {
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
    auto client = std::make_unique<rc::Client>(c);
    client->connectAndWait(20s);
    return client;
}
std::span<const uint8_t> bytesOf(const std::string& s) { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }
bool waitFor(const std::function<bool()>& pred, std::chrono::seconds limit = 30s) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::yield();   // #711: a poll on the awaited state under a hard deadline; no wall-clock sleep
    }
    return pred();
}

void txnOpen(const TCell& p) {
    using namespace grants;
    AuthBroker b("nslctxn" + std::to_string(p.variant));
    b.bootstrapAdmin(kAdminPw);
    auto admin = newClient(b, "admin");
    ASSERT_TRUE(admin->createNamespace("a").has_value());
    b.createUser("a/ua", kUserPw,
                 {prefix(SO::Create, ST::Topic, ""), prefix(SO::Describe, ST::Topic, ""), prefix(SO::Write, ST::Topic, ""),
                  prefix(SO::Read, ST::Topic, ""), prefix(SO::Read, ST::Group, ""), prefix(SO::Describe, ST::Group, ""),
                  prefix(SO::Write, ST::TransactionalId, ""), prefix(SO::Describe, ST::TransactionalId, "")});
    rc::TopicSpec spec;
    {
        auto c = newClient(b, "a/ua");
        const auto r = c->defineTopicAndWait("t", 1, "", {}, 20s);
        ASSERT_EQ(r.errorCode, 0) << r.detail;
        spec = rc::TopicSpec{"t", r.topicId, 1};
        c->close();
    }
    const rc::TopicHandle th{spec.topicId, 1};
    auto ua = newClient(b, "a/ua", {spec});
    rc::ProducerConfig tcfg;
    tcfg.acks = rc::Acks::All;
    tcfg.transactionalId = "tx";
    rc::Producer txn = ua->producer(tcfg);
    txn.initTransactionsAndWait(30s);
    txn.beginTransaction();
    for (int i = 0; i < 10; ++i) {
        const std::string v = "x" + std::to_string(i);
        txn.send(ua->topic("t"), rc::Key(static_cast<int64_t>(i)), bytesOf(v));
    }
    txn.flushAndWait(30s);
    {   // a plain SENTINEL after the transaction's records: read_committed cannot deliver it while the txn is open
        rc::ProducerConfig pcfg;
        pcfg.acks = rc::Acks::All;
        rc::Producer plain = admin->producer(pcfg);
        plain.send(th, rc::Key(int64_t{1}), bytesOf("S"));
        ASSERT_EQ(plain.closeAndWait(30s), rc::OpStatus::Ok);
    }
    std::mutex mu;
    std::vector<std::string> values;
    rc::SubscribeOptions o;
    o.from = rc::From::earliest();
    o.isolation = rc::Isolation::ReadCommitted;
    rc::Subscription sub = admin->subscribe(th, 0, o, [&](const rc::Record& r) {
        std::lock_guard<std::mutex> lk(mu);
        values.emplace_back(reinterpret_cast<const char*>(r.value.data()), r.value.size());
    });
    auto snapshot = [&] { std::lock_guard<std::mutex> lk(mu); return values; };
    // Premise (no wall-time wait): a read_uncommitted reader of the same partition has seen the sentinel, so the log
    // holds it - yet read_committed has delivered nothing: the open transaction pins the last stable offset.
    std::atomic<bool> uncommittedSawS{false};
    rc::SubscribeOptions ou;
    ou.from = rc::From::earliest();
    ou.isolation = rc::Isolation::ReadUncommitted;
    rc::Subscription subU = admin->subscribe(th, 0, ou, [&](const rc::Record& r) {
        if (r.value.size() == 1 && r.value[0] == static_cast<uint8_t>('S')) uncommittedSawS = true;
    });
    ASSERT_TRUE(waitFor([&] { return uncommittedSawS.load(); }, 20s)) << "premise: the sentinel is in the log";
    subU.unsubscribe();
    EXPECT_TRUE(snapshot().empty()) << "premise: an open transaction pins the last stable offset";
    // #721: a DELETING namespace is not readable by id, the cluster admin's subscription included (it ends at the Deleting
    // apply), so for Delete the witness of the abort is the partition's last stable offset (pinned below the sentinel by the
    // open transaction, past it once the ABORT marker is in the log) instead of the sentinel's delivery.
    auto replicaInt = [&](auto read) {
        return b.node().onOwnerThreadForTest(spec.topicId, 0, [&](rune::replication::PartitionReplica* r) {
            return r != nullptr ? static_cast<int64_t>(read(*r)) : int64_t{-1};
        });
    };
    const auto lsoOf = [&] { return replicaInt([](rune::replication::PartitionReplica& r) { return r.cleanerLastStableOffset(); }); };
    const auto endOf = [&] { return replicaInt([](rune::replication::PartitionReplica& r) { return r.logEndOffset(); }); };
    const int64_t endBefore = endOf();
    if (p.variant >= 2) ASSERT_LT(lsoOf(), endBefore) << "premise: the open transaction pins the LSO below the sentinel";
    switch (p.variant) {
        case 0: ASSERT_TRUE(admin->suspendNamespace("a", pc::NsSuspendMode::ReadOnly).has_value()); break;
        case 1: ASSERT_TRUE(admin->suspendNamespace("a", pc::NsSuspendMode::RejectAll).has_value()); break;
        default:
            // Hold the reaper before its first event: `a` is Deleting (its transactions aborted at the gate) but a/t is
            // not dropped yet, so the read_committed reader can still see the sentinel (the TxnOpen_Delete flake).
            b.node().onAdminThreadForTest([&] {
                b.node().adminWorkerAgentForTest()->worker().reaperForTest().setIssueBudgetForTest(0);
                return 0;
            });
            ASSERT_TRUE(admin->deleteNamespace("a").has_value());
            break;
    }
    if (p.variant >= 2) {
        ASSERT_TRUE(waitFor([&] { return lsoOf() >= endBefore; }, 40s))
            << "the LSO never passed the sentinel: the open transaction was not aborted; lso " << lsoOf() << " end " << endOf();
    } else {
        ASSERT_TRUE(waitFor([&] { const auto s = snapshot(); return std::find(s.begin(), s.end(), "S") != s.end(); }, 40s))
            << "the sentinel never became visible: the open transaction was not aborted";
        for (const std::string& v : snapshot()) EXPECT_EQ(v, "S") << "an aborted record is visible to read_committed";
    }
    // The commit fails at once with a definite outcome - never by waiting out its timeout (review B1: a timeout used to
    // pass here and hid two client hangs: EndTxn 51 retried until the deadline, a RejectAll's corr-0 51 ignored).
    std::string err;
    const auto t0 = std::chrono::steady_clock::now();
    try { txn.commitTransactionAndWait(15s); } catch (const std::exception& e) { err = e.what(); }
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_LT(took, 3s) << "the commit waited instead of failing: " << err;
    EXPECT_EQ(err.find("timed out"), std::string::npos) << err;
    if (p.variant == 0) {
        // ReadOnly: the session lives; the gateway refuses EndTxn NamespaceSuspended and the client goes FatalError.
        EXPECT_EQ(err, "commitTransaction(): broker error code 51 (EndTxn failed (fatal))");
        // #715: a platform-aborted end is offered exactly once - no client resend loop on the 51
        EXPECT_EQ(txn.endTxnAttemptsForTest(), 1u) << "EndTxn was not submitted exactly once";
        EXPECT_EQ(txn.endTxnWireOffersForTest(), 1u) << "EndTxn was not offered on the wire exactly once";
    } else {
        // RejectAll (corr-0 51) / Delete (corr-0 46) ended the session: the client failed for good before the commit.
        EXPECT_EQ(err, "commitTransaction(): client closed");
        EXPECT_EQ(txn.endTxnAttemptsForTest(), 0u) << "the session ended first: the commit must not send EndTxn at all";
    }
    sub.unsubscribe();
}

}  // namespace

class NamespaceLifecycleMatrixTimingRegression : public ::testing::TestWithParam<TCell> {
protected:
    static void SetUpTestSuite() { shared_ = std::make_unique<LcRig>("nslcmxt"); }
    static void TearDownTestSuite() { shared_.reset(); }
    static inline std::unique_ptr<LcRig> shared_;
};

TEST_P(NamespaceLifecycleMatrixTimingRegression, Timing) {
    const TCell& p = GetParam();
    switch (p.timing) {
        case LcTiming::BackToBack: backToBack(p, *shared_); break;
        case LcTiming::CrashMidDelete: crashMidDelete(p); break;
        case LcTiming::LeaderChangeMidDelete: leaderChangeMidDelete(p); break;
        case LcTiming::FollowerCatchUp: followerCatchUp(p); break;
        case LcTiming::LimitChangeMidTick: limitChangeMidTick(p); break;
        case LcTiming::RecreateUnderThrottle: p.variant == 0 ? recreateUnderThrottleWorker() : recreateUnderThrottleGateway(); break;
        case LcTiming::TxnOpen: txnOpen(p); break;
        case LcTiming::Sequential: FAIL() << "not a timing cell";
    }
}

INSTANTIATE_TEST_SUITE_P(All, NamespaceLifecycleMatrixTimingRegression, ::testing::ValuesIn(timingCells()),
                         [](const auto& info) { return tName(info.param); });

// Spec §4: DELETING refuses every request from the NamespaceDeleted commit on, the cluster admin's too (its exemption is
// from suspend only).  The topics are dropped later, one reaper step at a time; a produce acked in between would be erased
// with the namespace.  Held reaper = the window stays open: the cluster admin's produce to the topic id answers UnknownTopic
// (like a missing namespace), also after a restart (the DELETING gate is rebuilt from the catalog), and after the erase.
// Found by NamespaceStressRegression seed 18 (invariant 6, a delete right after a restart).
TEST(NamespaceDeletingProduceRegression, ClusterAdminProduceToDeletingNamespaceIsUnknownTopicRegression) {
    constexpr int kUnknownTopic = static_cast<int>(pc::ErrorCode::UnknownTopic);
    LcRig rig("nsdelprod");   // the reaper is held: a deleted namespace stays DELETING
    rig.prepare("dp", LcTarget::Active, 0, LcCaller::ClusterAdmin);
    const std::vector<int32_t> ids = rig.topicIds("dp");
    ASSERT_EQ(ids.size(), 2u);
    for (const int32_t id : ids) ASSERT_EQ(rig.auth().produce(rig.rootSid(), id, /*untilOk=*/true), 0) << "baseline: topic " << id;
    const uint32_t nsId = rig.observe("dp").nsId;
    ASSERT_NE(nsId, 0u);
    ASSERT_EQ(rig.ns(rig.rootSid(), pc::NamespaceDdlKind::Delete, "dp").code, 0);
    // The gate rides the workers' ingress ring (one hop after the commit): bounded.
    for (const int32_t id : ids)
        EXPECT_TRUE(rig.until([&] { return rig.auth().produce(rig.rootSid(), id) == kUnknownTopic; }, 500))
            << "topic " << id << " of the DELETING namespace still takes a produce";
    ASSERT_FALSE(rig.erased(nsId)) << "the reaper is held: the namespace is still DELETING";

    rig.restart(/*holdReaper=*/true);
    // Review U1: the gate is rebuilt from the booted catalog before the workers run, not from the next live catalog
    // event - so NO produce after the restart is acked (the log does not grow), not only an eventual UnknownTopic.
    for (const int32_t id : ids) {
        const int64_t endBefore = rig.auth().logEnd(id);
        int acked = 0;
        EXPECT_TRUE(rig.until([&] {
            const int code = rig.auth().produce(rig.rootSid(), id);
            if (code == 0) ++acked;
            return code == kUnknownTopic;
        }, 500)) << "after a restart, topic " << id << " of the DELETING namespace still takes a produce";
        EXPECT_EQ(acked, 0) << "after a restart, topic " << id << " of the DELETING namespace acked a produce";
        EXPECT_EQ(rig.auth().logEnd(id), endBefore) << "after a restart, topic " << id << " grew";
    }

    rig.releaseReaper();
    ASSERT_TRUE(rig.until([&] { return rig.erased(nsId); }));
    for (const int32_t id : ids) EXPECT_EQ(rig.auth().produce(rig.rootSid(), id), kUnknownTopic) << "erased: topic " << id;
}

// #709 (in-process twin of namespace-e2e.sh step 15): a namespace holding a VIEW, a consumer group's committed offset and
// a transactional id is deleted: the reaper drops the view (its own DropView event), the keyed stores hold nothing of the
// old id, no ns-<id> directory is left, and the old view id answers UnknownTopic; the re-created namespace is empty.
// Mutation: skip the reaper's DroppingViews step (NamespaceReaper.cpp) -> the view is never dropped (DropView count 0).
TEST(NamespaceLifecycleMatrixDeleteRegression, DeleteWithViewsGroupsTxnsLeavesNothingRegression) {
    LcRig rig("nsdelvgt");
    const std::string nm = "d";
    const int64_t userSid = rig.seedDeleteWorkload(nm);
    const uint32_t id = rig.observe(nm).nsId;
    ASSERT_NE(id, 0u);
    const int32_t src = rig.topicIds(nm).front();
    const int32_t view = rig.auth().createView(rig.rootSid(), nm + "/v", src);
    ASSERT_GE(view, 0) << "the view is created in the namespace";
    const auto oldTopics = rig.topicIds(nm);
    ASSERT_EQ(oldTopics.size(), 2u);
    ASSERT_EQ(rig.ns(rig.rootSid(), pc::NamespaceDdlKind::Delete, nm).code, 0);
    EXPECT_TRUE(rig.waitSessionGone(userSid));
    rig.releaseReaper();   // the rig holds the reaper from the start
    ASSERT_TRUE(rig.until([&] { return rig.erased(id); })) << "the delete never completed";
    EXPECT_EQ(rig.catalogEventCount(rune::catalog::EventKind::DropView, id), 1u) << "the view dropped exactly once";
    EXPECT_TRUE(rig.until([&] {
        return !std::filesystem::exists(nsRoot(rig.dataDir(), id)) &&
               !std::filesystem::exists(std::filesystem::path(nsRoot(rig.dataDir(), id).string() + ".deleting"));
    })) << "ns-<id> directory";
    EXPECT_TRUE(rig.auth().keyedStoresCleanBounded(id)) << "a live consumer-offset or transaction-state key of the deleted namespace";
    ASSERT_EQ(rig.ns(rig.rootSid(), pc::NamespaceDdlKind::Create, nm).code, 0);
    EXPECT_NE(rig.observe(nm).nsId, id);
    const auto md = rig.rootMetadata();
    for (const int32_t t : oldTopics) EXPECT_FALSE(std::binary_search(md.begin(), md.end(), t)) << "old topic " << t << " listed";
    EXPECT_FALSE(std::binary_search(md.begin(), md.end(), view)) << "the old view " << view << " is listed";
}

// #721: DELETING gated produce at the worker, but a read of the namespace by id was still served to a root session
// (exempt from the Deleting revoke): its live subscriptions kept receiving, a new Subscribe was accepted and GetViewRow
// answered, until the reaper dropped the topic.  Held reaper = the window stays open.
TEST(NamespaceDeletingProduceRegression, RootReadsOfDeletingNamespaceAreUnknownTopicRegression) {
    constexpr int kUnknownTopic = static_cast<int>(pc::ErrorCode::UnknownTopic);
    LcRig rig("nsdelread");   // the reaper is held: a deleted namespace stays DELETING
    rig.prepare("dr", LcTarget::Active, 0, LcCaller::ClusterAdmin);
    const std::vector<int32_t> ids = rig.topicIds("dr");
    ASSERT_EQ(ids.size(), 2u);
    AuthRig& a = rig.auth();
    auto& c = a.c();
    const int64_t sid = rig.rootSid();
    const int32_t vid = a.createView(sid, "dr/v", ids[0]);
    ASSERT_GE(vid, 0);
    for (int i = 0; i < 5; ++i) ASSERT_EQ(a.produce(sid, ids[0], /*untilOk=*/true), 0) << "record " << i;

    const int64_t s1 = c.sendSubscribe(a.ch(), a.stream(), sid, ids[0], 0);
    const auto s1Code = c.awaitSubscribeCode(s1, AuthRig::dl());
    ASSERT_TRUE(s1Code.has_value());
    ASSERT_EQ(*s1Code, 0);
    const auto s1Id = c.subscriptionIdOf(s1);
    ASSERT_TRUE(s1Id.has_value());
    auto framesOf = [&](int64_t subId) {
        std::size_t n = 0;
        for (const auto& r : c.records()) if (r.subscriptionId == subId) ++n;
        return n;
    };
    ASSERT_TRUE(c.pollUntil([&] { return framesOf(*s1Id) > 0; })) << "baseline: S1 receives the 5 records";
    const int64_t rowCorr = c.sendGetViewRow(a.ch(), a.stream(), sid, vid, 1);
    const auto row = c.awaitViewRowOrError(rowCorr, AuthRig::dl());
    ASSERT_TRUE(row.has_value());
    ASSERT_FALSE(row->isError) << "baseline: GetViewRow code " << row->errorCode;
    ASSERT_LE(row->status, 2u) << "baseline: GetViewRow status";

    const uint32_t nsId = rig.observe("dr").nsId;
    ASSERT_NE(nsId, 0u);
    ASSERT_EQ(rig.ns(sid, pc::NamespaceDdlKind::Delete, "dr").code, 0);

    // (1) S1 ends with UnknownTopic at the Deleting apply (same path as a topic drop), not at the reaper's drop.
    ASSERT_TRUE(c.pollUntil([&] {
        for (const auto& e : c.errors()) if (e.correlationId == s1) return e.code == kUnknownTopic;
        return false;
    })) << "S1 of the DELETING namespace was not ended";
    const std::size_t framesAtEnd = framesOf(*s1Id);
    ASSERT_FALSE(rig.erased(nsId)) << "the reaper is held: the namespace is still DELETING";

    // (2) a new root Subscribe by the topic's id answers like a topic that never existed.
    constexpr int32_t kNeverExisted = 987654;
    const int64_t nCorr = c.sendSubscribe(a.ch(), a.stream(), sid, kNeverExisted, 0);
    const auto nCode = c.awaitSubscribeCode(nCorr, AuthRig::dl());
    ASSERT_TRUE(nCode.has_value());
    ASSERT_EQ(*nCode, kUnknownTopic) << "baseline: a never-existing id";
    for (const int32_t id : ids) {
        const int64_t corr = c.sendSubscribe(a.ch(), a.stream(), sid, id, 0);
        const auto code = c.awaitSubscribeCode(corr, AuthRig::dl());
        ASSERT_TRUE(code.has_value()) << "topic " << id;
        EXPECT_EQ(*code, *nCode) << "a root Subscribe to topic " << id << " of the DELETING namespace";
        EXPECT_EQ(c.subscriptionIdOf(corr).value_or(0), 0) << "topic " << id << " got a subscription";   // a refusal carries id 0
    }

    // (3) GetViewRow of the namespace's view: the missing-view answer.
    const int64_t gCorr = c.sendGetViewRow(a.ch(), a.stream(), sid, vid, 1);
    const auto g = c.awaitViewRowOrError(gCorr, AuthRig::dl());
    ASSERT_TRUE(g.has_value());
    EXPECT_FALSE(g->isError);
    EXPECT_EQ(g->status, pc::ViewRowResponse::kUnknownView) << "GetViewRow on a view of the DELETING namespace";

    EXPECT_EQ(framesOf(*s1Id), framesAtEnd) << "S1 received a frame after it ended";
}

// ── #708: lowering a namespace's max-retention-ms reaches the cleaner and deletes the oldest segment ─────────────────
namespace {
std::vector<std::string> segmentFileNames(const std::filesystem::path& topicDir) {
    std::vector<std::string> v;
    std::error_code ec;
    if (!std::filesystem::exists(topicDir, ec)) return v;
    for (const auto& e : std::filesystem::recursive_directory_iterator(topicDir, ec))
        if (e.is_regular_file() && e.path().extension() == ".log") v.push_back(e.path().filename().string());
    std::sort(v.begin(), v.end());
    return v;
}
}  // namespace

TEST(NamespaceLifecycleMatrixLimitsRegression, SetLimitsLowerRetentionDeletesOldestSegmentRegression) {
    using namespace grants;
    AuthBroker b("nslclr");
    b.bootstrapAdmin(kAdminPw);
    auto admin = newClient(b, "admin");
    const auto nsId = admin->createNamespace("a");
    ASSERT_TRUE(nsId.has_value());
    b.createUser("a/ua", kUserPw,
                 {prefix(SO::Create, ST::Topic, ""), prefix(SO::Describe, ST::Topic, ""), prefix(SO::Write, ST::Topic, ""),
                  prefix(SO::Read, ST::Topic, "")});
    // topic r: 4 KiB segments, retention 1 h, the topic's own retention pass every 50 ms (a per-topic setting)
    rc::TopicOptions topic;
    topic.config.segmentRollBytes = 4096;
    topic.config.retentionMs = 3'600'000;
    topic.config.retentionCheckMs = 50;
    rc::TopicSpec spec;
    {
        auto c = newClient(b, "a/ua");
        const auto r = c->defineTopicAndWait("r", 1, "", {}, rc::Persistence::Disk, topic, {}, 20s);
        ASSERT_EQ(r.errorCode, 0) << r.detail;
        spec = rc::TopicSpec{"r", r.topicId, 1};
        c->close();
    }
    auto ua = newClient(b, "a/ua", {spec});
    {   // ~600 KiB: a 4 KiB roll setting still maps 16 KiB per segment (about 14 records), so this is at least 16 segments
        rc::ProducerConfig pcfg;
        pcfg.acks = rc::Acks::All;
        pcfg.batchRecords = 1;   // one batch per record: a segment rolls between batches, never inside one
        rc::Producer p = ua->producer(pcfg);
        const std::string v(1000, 'x');
        for (int i = 0; i < 600; ++i) p.send(ua->topic("r"), rc::Key(static_cast<int64_t>(i)), bytesOf(v));
        ASSERT_EQ(p.closeAndWait(30s), rc::OpStatus::Ok);
    }
    const auto logStart = [&] {
        return b.node().onOwnerThreadForTest(spec.topicId, 0, [](rune::replication::PartitionReplica* r) {
            return r != nullptr && r->log() != nullptr ? r->log()->logStartOffset() : int64_t{-1};
        });
    };
    const auto dir = topicsRoot(b.dir() / "broker", *nsId) / "r";
    const std::vector<std::string> before = segmentFileNames(dir);
    ASSERT_GE(before.size(), 16u) << "premise: the data spread over at least 16 segments";
    // premise: the cleaner has run passes (its own 50 ms cadence) and, with a 1 h retention, deleted nothing
    const uint64_t cycles0 = b.node().cleanerCyclesForTest();
    ASSERT_TRUE(waitFor([&] { return b.node().cleanerCyclesForTest() >= cycles0 + 3; }, 20s)) << "premise: the cleaner is running";
    ASSERT_EQ(logStart(), 0) << "premise: nothing is deleted under a 1 h retention";
    ASSERT_EQ(segmentFileNames(dir).front(), before.front()) << "premise: the oldest segment is still there";

    std::size_t retentionField = rune::protocol::kNamespaceLimitFieldCount;
    for (std::size_t i = 0; i < rune::client::kNamespaceLimitFields.size(); ++i)
        if (rune::client::kNamespaceLimitFields[i].name == "max-retention-ms") retentionField = i;
    ASSERT_LT(retentionField, rune::protocol::kNamespaceLimitFieldCount);
    rune::client::NamespaceLimitsPatch patch;
    ASSERT_TRUE(patch.set(retentionField, 1));
    ASSERT_TRUE(admin->setNamespaceLimits("a", patch).has_value());

    EXPECT_TRUE(waitFor([&] { return logStart() > 0; }, 40s))
        << "the namespace's lowered retention never reached the cleaner: the log start offset stays " << logStart();
    EXPECT_TRUE(waitFor([&] { const auto now = segmentFileNames(dir); return !now.empty() && now.front() != before.front(); }, 20s))
        << "the oldest segment file survived";
}
