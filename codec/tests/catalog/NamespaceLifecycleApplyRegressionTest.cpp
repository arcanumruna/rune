// Namespaces Phase 2 Task 2 (plan 2026-10-26-rune-namespace-lifecycle-limits.md, D17, D18): the suspend / resume /
// delete apply rules and the NamespaceDeleteStep (kind 36) event.
//   ACTIVE -> SUSPENDED(m); SUSPENDED(m) -> ACTIVE | SUSPENDED(other); ACTIVE | SUSPENDED --Deleted--> DELETING
//   --(every object dropped + the four steps)--> erased.
#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogFingerprintV2.h>
#include <rune/catalog/CatalogState.h>
#include <rune/catalog/NamespaceRegistry.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <expected>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace rc = rune::catalog;
namespace cd = rune::codec;
namespace rp = rune::protocol;

namespace {

using Res = std::expected<rc::Applied, rc::ApplyError>;
using T = rc::NsStateTag;
constexpr uint8_t kSteps[4] = {rc::TxnsAborted, rc::GroupsTombstoned, rc::TxnStateTombstoned, rc::SecurityPurged};

/// The leader's way of building an event (stamp the previewed fingerprint, then apply), over formatVer 2.
struct Rig {
    rc::CatalogState s;
    int64_t          off = 0;

    Res go(rc::CatalogEvent e) {
        auto fp = rc::previewFingerprint(s, e);
        if (!fp) return std::unexpected(fp.error());
        e.fingerprint = *fp;
        auto a = rc::apply(s, e, off);
        if (a) ++off;
        return a;
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
    const rc::NamespaceRecord* rec(uint32_t nsId) const { return s.namespaces().byId(nsId); }
    std::string nameOf(uint32_t nsId) const { const auto* r = rec(nsId); return r ? r->name : "?"; }
    uint32_t verOf(uint32_t nsId) const { const auto* r = rec(nsId); return r ? r->version : 0; }
    const rc::NsDeleting* deleting(uint32_t nsId) const {
        const auto* r = rec(nsId);
        return r ? std::get_if<rc::NsDeleting>(&r->state) : nullptr;
    }

    Res nsCreate(const std::string& name, rp::NamespaceLimits l = {}) {
        return go(base(rc::NamespaceCreatedOps{name, l}, name, s.namespaces().nextNsId(), 0, 0));
    }
    Res nsLimits(uint32_t nsId, const rp::NamespaceLimits& l) {
        return go(base(rc::NamespaceLimitsSetOps{l}, nameOf(nsId), nsId, 0, verOf(nsId)));
    }
    Res nsState(uint32_t nsId, T to) {
        return go(base(rc::NamespaceStateChangedOps{to}, nameOf(nsId), nsId, 0, verOf(nsId)));
    }
    Res nsDelete(uint32_t nsId) { return go(base(rc::NamespaceDeletedOps{}, nameOf(nsId), nsId, 0, verOf(nsId))); }
    Res nsStep(uint32_t nsId, uint8_t step) {
        return go(base(rc::NamespaceDeleteStepOps{step}, nameOf(nsId), nsId, 0, verOf(nsId)));
    }
    Res topic(uint32_t nsId, const std::string& name) {
        rc::CreateTopicOps o;
        o.topicId = s.nextTopicId();
        return go(base(std::move(o), name, nsId, s.nextObjectId(), 0));
    }
    Res dropTopic(uint32_t nsId, const std::string& name) {
        const rc::TopicObject* t = s.topicByName(nsId, name);
        return go(base(rc::DropTopicOps{t ? t->topicId : -1}, name, nsId, t ? t->objectId : 0, t ? t->version : 0));
    }
    Res alterTopic(uint32_t nsId, const std::string& name) {
        const rc::TopicObject* t = s.topicByName(nsId, name);
        return go(base(rc::AlterTopicConfigOps{{rc::ConfigChange{rp::ConfigKey::RetentionMs, 1000}}}, name, nsId,
                       t ? t->objectId : 0, t ? t->version : 0));
    }
    Res view(uint32_t nsId, const std::string& name, std::vector<std::string> sources) {
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
        return go(base(rc::CreateViewOps{std::move(v)}, name, nsId, s.nextObjectId(), 0));
    }
    Res dropView(uint32_t nsId, const std::string& name) {
        const rc::ViewObject* v = s.viewByName(nsId, name);
        return go(base(rc::DropViewOps{v ? v->def.viewTopicId : -1}, name, nsId, v ? v->objectId : 0, v ? v->version : 0));
    }
    uint64_t topicId(uint32_t nsId, const std::string& n) const { return s.topicByName(nsId, n)->objectId; }
    uint64_t viewId(uint32_t nsId, const std::string& n) const { return s.viewByName(nsId, n)->objectId; }
};

void expectRefused(const Res& r, const std::string& what) {
    ASSERT_FALSE(r.has_value()) << what << ": expected a refusal";
    EXPECT_EQ(r.error().code, rc::ApplyError::Code::Precondition) << what << ": " << r.error().detail;
}

/// Reaches `from` for namespace 1 ("n"): ACTIVE, SUSPENDED(m) or DELETING.
void reach(Rig& r, T from) {
    ASSERT_TRUE(r.nsCreate("n").has_value());
    if (from == T::Deleting) ASSERT_TRUE(r.nsDelete(1).has_value());
    else if (from != T::Active) ASSERT_TRUE(r.nsState(1, from).has_value());
}

} // namespace

TEST(NamespaceLifecycleApplyRegression, TransitionTableRegression) {
    enum Ev { ToActive, ToRO, ToRA, ToDeleting, Deleted, Step1 };
    struct Row { T from; Ev ev; bool ok; T next; };
    const Row rows[] = {
        {T::Active, ToActive, false, T::Active},   {T::Active, ToRO, true, T::SuspendedReadOnly},
        {T::Active, ToRA, true, T::SuspendedRejectAll},   {T::Active, ToDeleting, false, T::Active},
        {T::Active, Deleted, true, T::Deleting},   {T::Active, Step1, false, T::Active},

        {T::SuspendedReadOnly, ToActive, true, T::Active},   {T::SuspendedReadOnly, ToRO, false, T::SuspendedReadOnly},
        {T::SuspendedReadOnly, ToRA, true, T::SuspendedRejectAll},   {T::SuspendedReadOnly, ToDeleting, false, T::SuspendedReadOnly},
        {T::SuspendedReadOnly, Deleted, true, T::Deleting},   {T::SuspendedReadOnly, Step1, false, T::SuspendedReadOnly},

        {T::SuspendedRejectAll, ToActive, true, T::Active},   {T::SuspendedRejectAll, ToRO, true, T::SuspendedReadOnly},
        {T::SuspendedRejectAll, ToRA, false, T::SuspendedRejectAll},   {T::SuspendedRejectAll, ToDeleting, false, T::SuspendedRejectAll},
        {T::SuspendedRejectAll, Deleted, true, T::Deleting},   {T::SuspendedRejectAll, Step1, false, T::SuspendedRejectAll},

        {T::Deleting, ToActive, false, T::Deleting},   {T::Deleting, ToRO, false, T::Deleting},
        {T::Deleting, ToRA, false, T::Deleting},   {T::Deleting, ToDeleting, false, T::Deleting},
        {T::Deleting, Deleted, false, T::Deleting},   {T::Deleting, Step1, true, T::Deleting},
    };
    static_assert(std::size(rows) == 24);
    EXPECT_EQ(std::size(rows), 24u);
    for (const Row& row : rows) {
        Rig r;
        reach(r, row.from);
        const uint64_t before = r.s.digest();
        Res res;
        switch (row.ev) {
        case ToActive:   res = r.nsState(1, T::Active); break;
        case ToRO:       res = r.nsState(1, T::SuspendedReadOnly); break;
        case ToRA:       res = r.nsState(1, T::SuspendedRejectAll); break;
        case ToDeleting: res = r.nsState(1, T::Deleting); break;
        case Deleted:    res = r.nsDelete(1); break;
        case Step1:      res = r.nsStep(1, rc::TxnsAborted); break;
        }
        const std::string what = "from " + std::to_string(int(row.from)) + " event " + std::to_string(int(row.ev));
        if (row.ok) {
            EXPECT_TRUE(res.has_value()) << what;
        } else {
            expectRefused(res, what);
            EXPECT_EQ(r.s.digest(), before) << what << ": a refusal changes nothing";
        }
        ASSERT_NE(r.rec(1), nullptr) << what;
        EXPECT_EQ(rc::tagOf(r.rec(1)->state), row.next) << what;
    }
}

TEST(NamespaceLifecycleApplyRegression, StateChangedToDeletingRefusedRegression) {
    for (const uint32_t ns : {0u, 1u}) {
        for (const T from : {T::Active, T::SuspendedReadOnly, T::SuspendedRejectAll}) {
            Rig r;
            ASSERT_TRUE(r.nsCreate("n").has_value());
            if (from != T::Active) ASSERT_TRUE(r.nsState(ns, from).has_value());
            const auto res = r.nsState(ns, T::Deleting);
            expectRefused(res, "ns " + std::to_string(ns) + " from " + std::to_string(int(from)));
            EXPECT_NE(res.error().detail.find("NamespaceDeleted"), std::string::npos) << res.error().detail;
            EXPECT_FALSE(std::holds_alternative<rc::NsDeleting>(r.rec(ns)->state));
        }
    }
}

TEST(NamespaceLifecycleApplyRegression, DeletedFillsPendingSetsRegression) {
    Rig r;
    ASSERT_TRUE(r.nsCreate("a").has_value());   // 1
    ASSERT_TRUE(r.nsCreate("b").has_value());   // 2
    for (const char* t : {"t1", "t2", "t3"}) ASSERT_TRUE(r.topic(1, t).has_value());
    ASSERT_TRUE(r.topic(2, "t1").has_value());
    ASSERT_TRUE(r.topic(0, "t1").has_value());
    ASSERT_TRUE(r.view(1, "v1", {"t1"}).has_value());
    ASSERT_TRUE(r.view(1, "v2", {"t2"}).has_value());
    ASSERT_TRUE(r.view(2, "v1", {"t1"}).has_value());
    ASSERT_TRUE(r.nsDelete(1).has_value());
    const rc::NsDeleting* d = r.deleting(1);
    ASSERT_NE(d, nullptr);
    EXPECT_EQ(d->pendingTopics, (std::set<uint64_t>{r.topicId(1, "t1"), r.topicId(1, "t2"), r.topicId(1, "t3")}));
    EXPECT_EQ(d->pendingViews, (std::set<uint64_t>{r.viewId(1, "v1"), r.viewId(1, "v2")}));
    EXPECT_EQ(d->stepMask, 0);
    EXPECT_EQ(r.rec(1)->version, 2u) << "the version chains";
    EXPECT_EQ(r.deleting(2), nullptr) << "the other namespace is untouched";
}

TEST(NamespaceLifecycleApplyRegression, DropInDeletingClearsPendingRegression) {
    Rig r;
    ASSERT_TRUE(r.nsCreate("a").has_value());
    for (const char* t : {"t1", "t2", "t3"}) ASSERT_TRUE(r.topic(1, t).has_value());
    ASSERT_TRUE(r.view(1, "v1", {"t1"}).has_value());
    ASSERT_TRUE(r.view(1, "v2", {"t2"}).has_value());
    const uint64_t v1 = r.viewId(1, "v1"), v2 = r.viewId(1, "v2");
    const uint64_t t1 = r.topicId(1, "t1"), t2 = r.topicId(1, "t2"), t3 = r.topicId(1, "t3");
    ASSERT_TRUE(r.nsDelete(1).has_value());
    // every other object kind is refused with Precondition
    expectRefused(r.topic(1, "w"), "CreateTopic in DELETING");
    expectRefused(r.alterTopic(1, "t1"), "AlterTopicConfig in DELETING");
    expectRefused(r.view(1, "v3", {"t3"}), "CreateView in DELETING");
    ASSERT_NE(r.deleting(1), nullptr);
    EXPECT_EQ(r.deleting(1)->pendingTopics.size(), 3u);
    // drops remove exactly their id
    ASSERT_TRUE(r.dropView(1, "v1").has_value());
    EXPECT_EQ(r.deleting(1)->pendingViews, (std::set<uint64_t>{v2}));
    ASSERT_TRUE(r.dropView(1, "v2").has_value());
    EXPECT_TRUE(r.deleting(1)->pendingViews.empty());
    ASSERT_TRUE(r.dropTopic(1, "t2").has_value());
    EXPECT_EQ(r.deleting(1)->pendingTopics, (std::set<uint64_t>{t1, t3}));
    ASSERT_TRUE(r.dropTopic(1, "t1").has_value());
    ASSERT_TRUE(r.dropTopic(1, "t3").has_value());
    EXPECT_TRUE(r.deleting(1)->pendingTopics.empty());
    (void)v1; (void)t2;
    ASSERT_NE(r.rec(1), nullptr) << "no steps recorded yet: not erased";
}

TEST(NamespaceLifecycleApplyRegression, StepIdempotentRegression) {
    Rig r;
    ASSERT_TRUE(r.nsCreate("a").has_value());
    ASSERT_TRUE(r.nsDelete(1).has_value());
    const rc::NamespaceRecord before = *r.rec(1);
    ASSERT_TRUE(r.nsStep(1, rc::GroupsTombstoned).has_value());
    EXPECT_EQ(r.deleting(1)->stepMask, 2);
    ASSERT_TRUE(r.nsStep(1, rc::GroupsTombstoned).has_value()) << "an already-set bit still applies";
    EXPECT_EQ(r.deleting(1)->stepMask, 2);
    const rc::NamespaceRecord& after = *r.rec(1);
    EXPECT_EQ(after.version, before.version + 2) << "versions chained once per event";
    EXPECT_EQ(after.nsId, before.nsId);
    EXPECT_EQ(after.name, before.name);
    EXPECT_EQ(after.limits, before.limits);
    EXPECT_EQ(after.fingerprint, before.fingerprint);
    EXPECT_EQ(std::get<rc::NsDeleting>(after.state).pendingTopics, std::get<rc::NsDeleting>(before.state).pendingTopics);
}

TEST(NamespaceLifecycleApplyRegression, EraseOnlyWhenCompleteRegression) {
    // mask 0..15 x {nothing pending, a topic and a view pending} x {steps first, drops first}
    int cases = 0;
    for (int mask = 0; mask < 16; ++mask) {
        for (int withObjects = 0; withObjects < 2; ++withObjects) {
            for (int dropsFirst = 0; dropsFirst < 2; ++dropsFirst) {
                if (!withObjects && dropsFirst) continue;
                Rig r;
                ASSERT_TRUE(r.nsCreate("a").has_value());
                if (withObjects) {
                    ASSERT_TRUE(r.topic(1, "t").has_value());
                    ASSERT_TRUE(r.view(1, "v", {"t"}).has_value());
                }
                ASSERT_TRUE(r.nsDelete(1).has_value());
                std::vector<Res> events;
                auto drops = [&] {
                    if (!withObjects) return;
                    events.push_back(r.dropView(1, "v"));
                    events.push_back(r.dropTopic(1, "t"));
                };
                auto steps = [&] {
                    for (const uint8_t b : kSteps) if (mask & b) events.push_back(r.nsStep(1, b));
                };
                if (dropsFirst) { drops(); steps(); } else { steps(); drops(); }
                const std::string what = "mask " + std::to_string(mask) + " objects " + std::to_string(withObjects) +
                                         " dropsFirst " + std::to_string(dropsFirst);
                for (const Res& e : events) ASSERT_TRUE(e.has_value()) << what << (e ? "" : e.error().detail);
                const bool erased = mask == 15;   // every pending object was dropped above
                EXPECT_EQ(r.rec(1) == nullptr, erased) << what;
                int erasing = 0;
                for (const Res& e : events) erasing += e->nsErased ? 1 : 0;
                EXPECT_EQ(erasing, erased ? 1 : 0) << what;
                if (erased) {
                    ASSERT_FALSE(events.empty());
                    EXPECT_TRUE(events.back()->nsErased) << what << ": the erasing event is the last one";
                    EXPECT_EQ(r.s.namespaces().nextNsId(), 2u) << what << ": the id is never reused";
                }
                ++cases;
            }
        }
    }
    EXPECT_EQ(cases, 16 * 3);
    // partial: all four steps but an object still live -> still DELETING
    Rig r;
    ASSERT_TRUE(r.nsCreate("a").has_value());
    ASSERT_TRUE(r.topic(1, "t").has_value());
    ASSERT_TRUE(r.nsDelete(1).has_value());
    for (const uint8_t b : kSteps) { const auto e = r.nsStep(1, b); ASSERT_TRUE(e.has_value()); EXPECT_FALSE(e->nsErased); }
    ASSERT_NE(r.rec(1), nullptr);
    EXPECT_TRUE(r.deleting(1)->complete() == false);
    const auto last = r.dropTopic(1, "t");
    ASSERT_TRUE(last.has_value());
    EXPECT_TRUE(last->nsErased);
    EXPECT_EQ(r.rec(1), nullptr);
}

TEST(NamespaceLifecycleApplyRegression, NameReusableWhileDeletingRegression) {
    Rig r;
    ASSERT_TRUE(r.nsCreate("a").has_value());   // 1
    ASSERT_TRUE(r.topic(1, "t").has_value());
    ASSERT_TRUE(r.nsDelete(1).has_value());
    EXPECT_EQ(r.s.namespaces().byName("a"), nullptr) << "a DELETING namespace is not reachable by name";
    ASSERT_TRUE(r.nsCreate("a").has_value()) << "the name is free while the old one is DELETING";
    ASSERT_NE(r.s.namespaces().byName("a"), nullptr);
    EXPECT_EQ(r.s.namespaces().byName("a")->nsId, 2u);
    ASSERT_NE(r.s.namespaces().byId(1), nullptr);
    EXPECT_TRUE(std::holds_alternative<rc::NsDeleting>(r.s.namespaces().byId(1)->state));
    // finishing the old one does not disturb the new one's name
    ASSERT_TRUE(r.dropTopic(1, "t").has_value());
    for (const uint8_t b : kSteps) ASSERT_TRUE(r.nsStep(1, b).has_value());
    EXPECT_EQ(r.s.namespaces().byId(1), nullptr);
    ASSERT_NE(r.s.namespaces().byName("a"), nullptr);
    EXPECT_EQ(r.s.namespaces().byName("a")->nsId, 2u);
}

TEST(NamespaceLifecycleApplyRegression, DeletingCountsTowardMaxNamespacesRegression) {
    Rig r;
    EXPECT_EQ(r.s.namespaces().liveCount(), 1u);
    ASSERT_TRUE(r.nsCreate("a").has_value());
    ASSERT_TRUE(r.nsCreate("b").has_value());
    EXPECT_EQ(r.s.namespaces().liveCount(), 3u);
    ASSERT_TRUE(r.nsDelete(1).has_value());
    EXPECT_EQ(r.s.namespaces().liveCount(), 3u) << "a DELETING namespace still counts";
    for (const uint8_t b : kSteps) ASSERT_TRUE(r.nsStep(1, b).has_value());
    EXPECT_EQ(r.s.namespaces().liveCount(), 2u) << "erased: one less";
}

TEST(NamespaceLifecycleApplyRegression, OtherDdlInDeletingIsMissingRegression) {
    Rig r;
    ASSERT_TRUE(r.nsCreate("a").has_value());
    ASSERT_TRUE(r.nsDelete(1).has_value());
    expectRefused(r.nsLimits(1, rp::NamespaceLimits{}), "LimitsSet on DELETING");
    expectRefused(r.nsState(1, T::Active), "StateChanged(Active) on DELETING");
    expectRefused(r.nsState(1, T::SuspendedReadOnly), "StateChanged(RO) on DELETING");
    expectRefused(r.nsDelete(1), "Deleted on DELETING");
    // a step on an ACTIVE or SUSPENDED namespace is refused too
    ASSERT_TRUE(r.nsCreate("b").has_value());
    expectRefused(r.nsStep(2, rc::TxnsAborted), "Step on ACTIVE");
    ASSERT_TRUE(r.nsState(2, T::SuspendedRejectAll).has_value());
    expectRefused(r.nsStep(2, rc::TxnsAborted), "Step on SUSPENDED");
}

TEST(NamespaceLifecycleApplyRegression, DefaultNeverDeletedRegression) {
    Rig r;
    expectRefused(r.nsDelete(0), "Deleted(0)");
    expectRefused(r.nsStep(0, rc::TxnsAborted), "Step(0)");
    ASSERT_TRUE(r.nsState(0, T::SuspendedRejectAll).has_value()) << "default may be suspended";
    expectRefused(r.nsDelete(0), "Deleted(0) while suspended");
    ASSERT_TRUE(r.nsState(0, T::Active).has_value());
    EXPECT_TRUE(std::holds_alternative<rc::NsActive>(r.rec(0)->state));
}

TEST(NamespaceLifecycleApplyRegression, MirrorSkipsDeletingRegression) {
    rc::NamespaceMirror m;
    auto record = [](uint32_t id, const std::string& name, uint32_t version, rc::NsState st) {
        rc::NamespaceRecord rec;
        rec.nsId = id; rec.name = name; rec.version = version; rec.state = std::move(st);
        return rec;
    };
    ASSERT_TRUE(m.upsert(record(1, "a", 1, rc::NsActive{})));
    ASSERT_NE(m.registry().byName("a"), nullptr);
    // entering DELETING removes the name but keeps the record by id
    ASSERT_TRUE(m.upsert(record(1, "a", 2, rc::NsDeleting{})));
    EXPECT_EQ(m.registry().byName("a"), nullptr);
    ASSERT_NE(m.registry().byId(1), nullptr);
    // a new namespace owns the name now
    ASSERT_TRUE(m.upsert(record(2, "a", 1, rc::NsActive{})));
    ASSERT_NE(m.registry().byName("a"), nullptr);
    EXPECT_EQ(m.registry().byName("a")->nsId, 2u);
    // a later (higher-version) upsert of the old DELETING record does not steal the name
    ASSERT_TRUE(m.upsert(record(1, "a", 3, rc::NsDeleting{.stepMask = 1})));
    EXPECT_EQ(m.registry().byName("a")->nsId, 2u);
    // erase(old) keeps the new one's name
    EXPECT_TRUE(m.erase(1));
    EXPECT_EQ(m.registry().byId(1), nullptr);
    ASSERT_NE(m.registry().byName("a"), nullptr);
    EXPECT_EQ(m.registry().byName("a")->nsId, 2u);
    EXPECT_FALSE(m.erase(1)) << "already gone";
    // erase of the current owner removes its name
    EXPECT_TRUE(m.erase(2));
    EXPECT_EQ(m.registry().byName("a"), nullptr);
}
