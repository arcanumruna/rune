// NamespaceMatrixTest.cpp - namespaces Task 12a: the Op x Caller x Target matrix (support/NamespaceMatrixExpectations.h)
// on ONE real authenticating broker (default config; the Anonymous rows on a second broker with anonymous read on).
// The sequential cells only; each cell's expectation comes from expect(), never from this file.
#include "support/NamespaceMatrixExpectations.h"
#include "support/NamespaceRig.h"

#include <gtest/gtest.h>

#include <algorithm>
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
    if (hidden) {
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
            const Snap s1 = snap();
            r.restartForTest();
            EXPECT_TRUE(r.awaitRootListed(r.topicIdA(), s1.ids.size())) << label << ": the topics did not come back";
            const Snap s2 = snap();
            EXPECT_EQ(s1.ns.normalized(s2.ns), s2.ns.normalized(s1.ns)) << label << ": ListNamespaces changed";
            EXPECT_EQ(s1.ids, s2.ids) << label << ": the topic listing changed";
            EXPECT_EQ(s1.digest, s2.digest) << label << ": the security view changed";
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
