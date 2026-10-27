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
            EXPECT_TRUE(isExpectedMissingCode(c.op, got.code)) << label;
            if (c.target == Target::ForeignNs || c.target == Target::OwnNs) {
                const NamespaceRig::Answer twin = rig_->runTwin(c.op, c.caller, c.target, c.ordinal);
                EXPECT_EQ(got.normalized(), twin.normalized())
                    << "ForeignEqualsMissingBytesRegression: " << label << " (twin code " << twin.code << ")";
            }
            break;
        case Kind::NotApplicable: FAIL() << label; break;
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
