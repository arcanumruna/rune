// NamespaceMatrixTest.cpp - namespaces Task 12a: the Op x Caller x Target matrix (support/NamespaceMatrixExpectations.h)
// on ONE real authenticating broker (default config; the Anonymous rows on a second broker with anonymous read on).
// The sequential cells only; each cell's expectation comes from expect(), never from this file.
#include "support/NamespaceMatrixExpectations.h"
#include "support/NamespaceRig.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

using namespace rune::node::test::nsmatrix;
using rune::node::test::NamespaceRig;

TEST(NamespaceMatrixRegression, MatrixCountRegression) {
    const Counts k = countCells();
    EXPECT_EQ(k.sequential, 560u);
    EXPECT_EQ(k.na, 21u);
    EXPECT_EQ(k.applicable, 539u);
    EXPECT_EQ(k.timing, 56u);
    EXPECT_EQ(k.total, 595u);
}

// #647: the rig cached each user session for the whole suite, past the broker's 10 s idle reap.  A cached session idle
// for more than 5 s is logged in again (the same rule as AuthBroker::freshAdminSid_).
TEST(NamespaceRigRegression, StaleUserSessionReloggedRegression) {
    NamespaceRig rig;
    const int64_t first = rig.userSessionForTest("a/ungranted");
    EXPECT_EQ(rig.userSessionForTest("a/ungranted"), first) << "a fresh session is reused";
    rig.ageUserSessionForTest("a/ungranted", std::chrono::seconds(6));
    const int64_t second = rig.userSessionForTest("a/ungranted");
    EXPECT_NE(second, first) << "a session idle past 5 s must be replaced before the 10 s reap can take it";
    // the replacement serves a real cell: the same answer the matrix expects of this caller
    for (std::size_t o = 0; o < kOps; ++o) {
        if (naRule(Op(o), Target::OwnNs)) continue;
        const NamespaceRig::Answer got = rig.run(Op(o), Caller::UserUngranted, Target::OwnNs, 0);
        EXPECT_EQ(got.code, expect(Op(o), Caller::UserUngranted, Target::OwnNs).code) << name(Op(o));
        break;
    }

    // #647 follow-up: root's session and the timing cells' second sessions are replaced after 5 s idle too.
    const int64_t rootFirst = rig.rootSessionForTest();
    EXPECT_EQ(rig.rootSessionForTest(), rootFirst) << "a fresh root session is reused";
    rig.ageRootSessionForTest(std::chrono::seconds(6));
    const int64_t rootSecond = rig.rootSessionForTest();
    EXPECT_NE(rootSecond, rootFirst) << "root's session idle past 5 s must be replaced";
    for (std::size_t o = 0; o < kOps; ++o) {
        if (naRule(Op(o), Target::OwnNs)) continue;
        const NamespaceRig::Answer got = rig.run(Op(o), Caller::ClusterAdmin, Target::OwnNs, 0);
        EXPECT_EQ(got.code, expect(Op(o), Caller::ClusterAdmin, Target::OwnNs).code) << name(Op(o));
        break;
    }
    for (const Caller who : {Caller::ClusterAdmin, Caller::NsAdminSame}) {
        const int64_t s1 = rig.secondSessionForTest(who);
        EXPECT_EQ(rig.secondSessionForTest(who), s1) << "a fresh second session is reused";
        rig.ageSecondSessionForTest(who, std::chrono::seconds(6));
        EXPECT_NE(rig.secondSessionForTest(who), s1) << "a second session idle past 5 s must be replaced: " << name(who);
    }
}

namespace {
struct Cell { Op op; Caller caller; Target target; int ordinal; };
std::vector<Cell> allCells() {
    std::vector<Cell> v;
    int ord = 0;
    for (std::size_t o = 0; o < kOps; ++o)
        for (std::size_t c = 0; c < kCallers; ++c)
            for (std::size_t t = 0; t < kTargets; ++t)
                if (!naRule(Op(o), Target(t))) v.push_back({Op(o), Caller(c), Target(t), ord++});
    return v;
}
std::string cellName(const Cell& c) {
    return std::string(name(c.op)) + "_" + name(c.caller) + "_" + name(c.target);
}
}  // namespace

class NamespaceMatrixRegression : public ::testing::TestWithParam<Cell> {
protected:
    static void SetUpTestSuite() { rig_ = std::make_unique<NamespaceRig>(); }
    static void TearDownTestSuite() { rig_.reset(); }
    static inline std::unique_ptr<NamespaceRig> rig_;
};

TEST_P(NamespaceMatrixRegression, SequentialCell) {
    const Cell& c = GetParam();
    const Outcome want = expect(c.op, c.caller, c.target);
    const NamespaceRig::Answer got = rig_->run(c.op, c.caller, c.target, c.ordinal);
    const std::string label = cellName(c) + ": want " + describe(want) + ", got code " + std::to_string(got.code) + " [" + got.detail + "]";
    switch (want.kind) {
        case Kind::Ok: case Kind::Exists: case Kind::NotAuthorized: case Kind::Refused: case Kind::LimitExceeded:
            EXPECT_EQ(got.code, want.code) << label;
            break;
        case Kind::Missing:
            EXPECT_TRUE(isExpectedMissingCode(c.op, c.caller, got.code)) << label;
            break;
        case Kind::NotApplicable: FAIL() << label; break;
    }
    // Hidden objects (a namespace the caller cannot see): whatever the answer, it is byte-equal to the same op on
    // a namespace that does not exist.
    const bool foreignCaller = c.caller == Caller::NsAdminForeign || c.caller == Caller::UserForeign;
    const bool hidden = !isNamespaceOp(c.op) &&
                        ((c.target == Target::ForeignNs && c.caller != Caller::ClusterAdmin && c.caller != Caller::NsAdminSame) ||
                         (c.target == Target::OwnNs && foreignCaller));
    // Phase 2 Task 5 (D19): a transactional id is scoped to the caller's own namespace, so InitProducerId answers Ok
    // for a foreign caller too, and its body carries a freshly allocated producer id (unique per call): there is no
    // hidden object to compare, and the two Ok bodies differ in the id alone.
    const bool freshIdAnswer = c.op == Op::TxnInit && got.code == 0;
    if (hidden && !freshIdAnswer) {
        const NamespaceRig::Answer twin = rig_->runTwin(c.op, c.caller, c.target, c.ordinal);
        EXPECT_EQ(twin.code, got.code) << "ForeignEqualsMissingBytesRegression: " << label;
        EXPECT_EQ(got.normalized(twin), twin.normalized(got))
            << "ForeignEqualsMissingBytesRegression: " << label << " (twin code " << twin.code << ")";
    }
    if (c.op == Op::ListNamespaces && want.kind == Kind::Ok) {
        std::vector<std::string> names = got.nsNames;
        if (c.caller == Caller::ClusterAdmin) {
            for (const char* n : {"default", "a", "b"})
                EXPECT_NE(std::find(names.begin(), names.end(), n), names.end()) << label << ": root lists " << n;
        } else {
            const bool foreignNs = c.target == Target::ForeignNs;   // the rig flips the b-side / a-side session there
            const bool bSide = c.caller == Caller::NsAdminSame ? foreignNs
                             : c.caller == Caller::NsAdminForeign ? !foreignNs
                             : c.caller == Caller::UserForeign && !foreignNs;
            const std::string own = bSide ? "b" : "a";
            EXPECT_EQ(names, std::vector<std::string>{own}) << label << ": only the caller's own namespace";
        }
    }
}

INSTANTIATE_TEST_SUITE_P(All, NamespaceMatrixRegression, ::testing::ValuesIn(allCells()),
                         [](const auto& info) { return cellName(info.param); });

// ── Task 12b: the one-node timing cells (BackToBack, ConcurrentSameName, Restart) ────────────────────────────────
namespace {
struct TCell { Op op; Caller caller; Timing timing; int ordinal; };
std::vector<TCell> timingCells() {
    std::vector<TCell> v;
    int ord = 10'000;
    for (std::size_t o = 0; o < kOps; ++o)
        for (std::size_t c = 0; c < kCallers; ++c)
            for (const Timing tm : {Timing::BackToBack, Timing::ConcurrentSameName, Timing::Restart})
                if (isTimingCell(Op(o), Caller(c), Target::OwnNs, tm)) v.push_back({Op(o), Caller(c), tm, ord++});
    return v;
}
std::string tCellName(const TCell& c) {
    return std::string(name(c.op)) + "_" + name(c.caller) + "_" + name(c.timing);
}
/// What a repeat of an applied first op answers.
int dupCode(Op o) {
    switch (o) {
        case Op::CreateNamespace: case Op::CreateTopic: return 25;
        case Op::CreateView: return 32;
        case Op::CreateUser: case Op::Grant: return 39;
        case Op::DropTopic: return 7;
        default: return 0;   // SetNamespaceLimits
    }
}
}  // namespace

// #650: the plan's "Justified deviations" section (2026-10-26-rune-namespaces.md) records these rules; pin each one so
// the doc and the expectations cannot drift apart.  No broker: expect() and the code tables only.
TEST(NamespaceMatrixCountRegression, JustifiedDeviationsPinnedRegression) {
    // anonymous Fetch and GetViewRow are NotAuthorized (spec §11), not the plan's rule 2 "Ok"
    for (const Op o : {Op::Fetch, Op::GetViewRow})
        for (const Target t : {Target::OwnNs, Target::SameNameBoth, Target::ForeignNs, Target::Missing})
            EXPECT_EQ(expect(o, Caller::Anonymous, t), Outcome::notAuthorized()) << name(o) << "/" << name(t);
    // an ungranted data op on a ForeignNs target is NotAuthorized, not Missing
    for (const Op o : {Op::CreateTopic, Op::AlterTopic, Op::DropTopic, Op::Produce, Op::Fetch, Op::GetTopicSchema})
        EXPECT_EQ(expect(o, Caller::UserUngranted, Target::ForeignNs), Outcome::notAuthorized()) << name(o);
    // root ListNamespaces on a Missing target is Ok
    EXPECT_EQ(expect(Op::ListNamespaces, Caller::ClusterAdmin, Target::Missing), Outcome::ok());
    // a duplicate user or role answers 39, the code of the security "missing" answer
    for (const Op o : {Op::CreateUser, Op::CreateRole}) {
        EXPECT_EQ(expect(o, Caller::ClusterAdmin, Target::OwnNs), Outcome::exists(39)) << name(o);
        EXPECT_EQ(expectedCodes(o)[0], 39) << name(o);
    }
    // DropTopic duplicate -> 7 and Grant duplicate -> 39: the same codes as their "missing" answers
    EXPECT_EQ(dupCode(Op::DropTopic), 7);
    EXPECT_EQ(expectedCodes(Op::DropTopic)[0], 7);
    EXPECT_EQ(dupCode(Op::Grant), 39);
    EXPECT_EQ(expectedCodes(Op::Grant)[0], 39);
}

TEST(NamespaceMatrixTimingCountRegression, TimingCellCountRegression) {
    EXPECT_EQ(timingCells().size(), 42u);
}

class NamespaceMatrixTimingRegression : public ::testing::TestWithParam<TCell> {
protected:
    static void SetUpTestSuite() { rig_ = std::make_unique<NamespaceRig>(); }
    static void TearDownTestSuite() { rig_.reset(); }
    static inline std::unique_ptr<NamespaceRig> rig_;
};

TEST_P(NamespaceMatrixTimingRegression, TimingCell) {
    const TCell& p = GetParam();
    const Outcome first = expect(p.op, p.caller, isCreateOp(p.op) ? Target::Missing : Target::OwnNs);
    const int dup = dupCode(p.op);
    const bool ok = first.kind == Kind::Ok;
    const std::string label = tCellName(p) + ": want " + describe(first);
    NamespaceRig& r = *rig_;
    r.prepareTimed(p.op, p.ordinal);
    const uint64_t lim = 100u + static_cast<uint64_t>(p.ordinal);
    switch (p.timing) {
        case Timing::BackToBack: {
            const rune::protocol::RequestId rid{0xB2B, static_cast<uint64_t>(p.ordinal)};
            const int64_t c1 = r.sendTimed(p.op, p.caller, p.ordinal, rid, false, lim);
            const int64_t c2 = r.sendTimed(p.op, p.caller, p.ordinal, rid, false, lim);
            const auto a1 = r.awaitTimed(c1);
            const auto a2 = r.awaitTimed(c2);
            EXPECT_EQ(a1.code, first.code) << label << " [" << a1.detail << "]";
            EXPECT_EQ(a2.code, first.code) << label << " (the retry) [" << a2.detail << "]";
            if (ok) {
                EXPECT_EQ(a1.normalized(a2), a2.normalized(a1)) << label << ": the retry's answer is not byte-equal";
                const auto third = r.timed(p.op, p.caller, p.ordinal, {}, lim);
                EXPECT_EQ(third.code, dup) << label << ": the op did not take effect exactly once [" << third.detail << "]";
            }
            break;
        }
        case Timing::ConcurrentSameName: {
            r.secondSession(p.caller);
            const int64_t c1 = r.sendTimed(p.op, p.caller, p.ordinal, {}, false, lim);
            const int64_t c2 = r.sendTimed(p.op, p.caller, p.ordinal, {}, true, 200u + static_cast<uint64_t>(p.ordinal));
            const auto a1 = r.awaitTimed(c1);
            const auto a2 = r.awaitTimed(c2);
            if (first.kind == Kind::NotAuthorized) {
                EXPECT_EQ(a1.code, 45) << label;
                EXPECT_EQ(a2.code, 45) << label;
            } else if (p.op == Op::SetNamespaceLimits) {
                EXPECT_EQ(a1.code, 0) << label;
                EXPECT_EQ(a2.code, 0) << label;
                const int64_t got = r.maxTopicsOfA();
                EXPECT_TRUE(got == static_cast<int64_t>(lim) || got == static_cast<int64_t>(200u + p.ordinal))
                    << label << ": a's maxTopics is " << got;
            } else {
                const int okCount = (a1.code == 0) + (a2.code == 0);
                const int dupCount = (a1.code == dup) + (a2.code == dup);
                EXPECT_EQ(okCount, 1) << label << ": codes " << a1.code << ", " << a2.code;
                EXPECT_EQ(dupCount, 1) << label << ": codes " << a1.code << ", " << a2.code;
            }
            break;
        }
        default: {   // Restart
            struct Snap { NamespaceRig::Answer ns; std::vector<int32_t> ids; uint64_t digest = 0; };
            auto snap = [&] { return Snap{r.rootListNamespaces(), r.rootMetadata(), r.securityDigest()}; };
            const auto a = r.timed(p.op, p.caller, p.ordinal, {}, lim);
            EXPECT_EQ(a.code, first.code) << label << " [" << a.detail << "]";
            // #652: the catalog event of a tenant's DDL carries the qualified login name, before and after a restart.
            const bool principalCell = ok && p.op == Op::CreateTopic;
            const std::string wantPrincipal = p.caller == Caller::ClusterAdmin ? "admin" : "a/admin";
            const std::string made = "k" + std::to_string(p.ordinal);
            if (principalCell) {
                const auto got = r.createTopicPrincipalForTest(made);
                ASSERT_TRUE(got.has_value()) << label << ": no CreateTopic event for " << made;
                EXPECT_EQ(*got, wantPrincipal) << label << ": the audit principal before the restart";
            }
            const Snap s1 = snap();
            r.restartForTest();
            EXPECT_TRUE(r.awaitRootListed(r.topicIdA(), s1.ids.size())) << label << ": the topics did not come back";
            const Snap s2 = snap();
            EXPECT_EQ(s1.ns.normalized(s2.ns), s2.ns.normalized(s1.ns)) << label << ": ListNamespaces changed";
            EXPECT_EQ(s1.ids, s2.ids) << label << ": the topic listing changed";
            EXPECT_EQ(s1.digest, s2.digest) << label << ": the security view changed";
            if (principalCell) {
                const auto got = r.createTopicPrincipalForTest(made);
                ASSERT_TRUE(got.has_value()) << label << ": the CreateTopic event is gone after the restart";
                EXPECT_EQ(*got, wantPrincipal) << label << ": the audit principal after the restart";
            }
            if (ok) {
                const auto again = r.timed(p.op, p.caller, p.ordinal, {}, lim);
                EXPECT_EQ(again.code, dup) << label << ": the effect did not survive the restart [" << again.detail << "]";
            }
            break;
        }
    }
}

INSTANTIATE_TEST_SUITE_P(All, NamespaceMatrixTimingRegression, ::testing::ValuesIn(timingCells()),
                         [](const auto& info) { return tCellName(info.param); });
