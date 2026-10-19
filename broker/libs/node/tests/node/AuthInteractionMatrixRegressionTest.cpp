// Authentication spec section 11 - the request x grant x timing matrix (720 cells, NotApplicable skipped naming its rule).
#include "support/AuthMatrixExpectations.h"
#include "support/AuthMatrixRig.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

using namespace rune::node::test::amatrix;
using rune::node::test::AuthMatrixRig;

namespace {
struct Cell { Op op; Grant grant; Timing timing; };
std::vector<Cell> allCells() {
    std::vector<Cell> v;
    for (int o = 0; o < int(Op::kCount); ++o)
        for (int g = 0; g < int(Grant::kCount); ++g)
            for (int t = 0; t < int(Timing::kCount); ++t) v.push_back({Op(o), Grant(g), Timing(t)});
    return v;
}
}  // namespace

class AuthMatrixRegression : public ::testing::TestWithParam<Cell> {
protected:
    static void SetUpTestSuite() { rig_ = std::make_unique<AuthMatrixRig>(); }
    static void TearDownTestSuite() { rig_.reset(); }
    static inline std::unique_ptr<AuthMatrixRig> rig_;
};

namespace {
bool isStream(Op op) { return op == Op::Subscribe || op == Op::CatalogRead || op == Op::SecurityRead; }
}  // namespace

TEST_P(AuthMatrixRegression, Cell) {
    const Cell c = GetParam();
    if (const NaRule na = naRule(c.op, c.grant, c.timing); na != NaRule::None) GTEST_SKIP() << name(na);
    const ExpectedCodes want = expectedCodes(c.op, c.grant, c.timing);
    const std::string label = std::string(name(c.op)) + "/" + std::string(name(c.grant)) + "/" + std::string(name(c.timing));
    const bool missing = c.timing == Timing::ObjectMissing;
    const Outcome outcome = expect(c.op, c.grant, c.timing);
    auto cell = rig_->newCell();
    struct Closer { decltype(cell)& c; ~Closer() { c.close(); } } closer{cell};

    if (c.timing != Timing::CreatedAfterLogin) { cell.createObjects(c.op, /*supportingOnly=*/missing); ASSERT_FALSE(::testing::Test::HasFatalFailure()); }
    if (c.timing != Timing::GrantedAfterLogin) { cell.grantFor(c.op, c.grant); ASSERT_FALSE(::testing::Test::HasFatalFailure()); }
    else { cell.grantFor(c.op, Grant::None); }   // creates the user only
    cell.login();
    if (c.timing == Timing::CreatedAfterLogin) { cell.createObjects(c.op); ASSERT_FALSE(::testing::Test::HasFatalFailure()); }

    const auto first = cell.issue(c.op, missing);
    EXPECT_EQ(first.code, want.first) << label;
    EXPECT_EQ(first.templ, first.wantTempl) << label << ": the answer is not in the op's own response type";
    if (c.op == Op::Metadata) EXPECT_EQ(first.present, want.presentFirst) << label;
    if (want.viewStatus != kNone) EXPECT_EQ(first.viewStatus, want.viewStatus) << label;

    // Streams under RevokedMid / UserDisabledMid: the subscription is live and receiving before the change.
    std::string why;
    const bool liveStream = isStream(c.op) && first.subId > 0 &&
                            (outcome == Outcome::EndedNotAuthorized || outcome == Outcome::SessionRevoked);
    if (liveStream) EXPECT_TRUE(cell.makeLive(c.op, first, why)) << label << ": " << why;

    bool disabled = false;
    switch (outcome) {
        case Outcome::EndedNotAuthorized: {
            cell.revokeFor(c.op, c.grant);
            ASSERT_FALSE(::testing::Test::HasFatalFailure());
            if (streaming(c.op)) {
                EXPECT_EQ(cell.awaitEnd(c.op, first), want.second) << label;
                if (liveStream) EXPECT_TRUE(cell.noDeliveryAfterEnd(c.op, first, why)) << label << ": every-cell assertion 4: " << why;
            } else {
                const auto second = cell.issue(c.op, false);
                EXPECT_EQ(second.code, want.second) << label;
                EXPECT_EQ(second.templ, second.wantTempl) << label << ": the second answer is not in the op's own response type";
                if (c.op == Op::Metadata) EXPECT_EQ(second.present, want.presentSecond) << label;
            }
            break;
        }
        case Outcome::SessionRevoked: {
            cell.disableUser();
            ASSERT_FALSE(::testing::Test::HasFatalFailure());
            disabled = true;
            EXPECT_EQ(cell.awaitSessionLevel(), want.second) << label;
            if (isStream(c.op)) {
                EXPECT_EQ(cell.errorOn(first.corr), want.second) << label;
                if (liveStream) EXPECT_TRUE(cell.noDeliveryAfterEnd(c.op, first, why)) << label << ": every-cell assertion 4: " << why;
            }
            // The reconnect attempt of a disabled user is a failed login: the broker's per-host brute-force curve (spec
            // section 5, 10 free failures per source) would delay every later legitimate login of this broker process, so
            // a fixed, order-independent 7 of the 79 R5 cells attempt it (the curve itself is LoginThrottleTest's).
            if ((int(c.op) * 5 + int(c.grant)) % 11 == 0) EXPECT_EQ(cell.relogin(), want.reconnect) << label;
            break;
        }
        case Outcome::DeniedThenOk: {
            cell.grantFor(c.op, c.grant);
            ASSERT_FALSE(::testing::Test::HasFatalFailure());
            const auto second = cell.issue(c.op, false);
            EXPECT_EQ(second.code, want.second) << label;
            EXPECT_EQ(second.templ, second.wantTempl) << label << ": the second answer is not in the op's own response type";
            if (c.op == Op::Metadata) EXPECT_EQ(second.present, want.presentSecond) << label;
            break;
        }
        default: break;
    }
    if (want.first == 45 && c.timing != Timing::GrantedAfterLogin)
        EXPECT_EQ(cell.hiddenTwinProblem(c.op, missing, first), "") << label << ": every-cell assertion 3: no existence leak";
    EXPECT_EQ(cell.answersProblem(), "") << label << ": every-cell assertion 2";
    EXPECT_EQ(cell.countersProblem(disabled), "") << label << ": every-cell assertion 5";
    EXPECT_EQ(cell.principalProblem(), "") << label << ": every-cell assertion 6";
}

INSTANTIATE_TEST_SUITE_P(All, AuthMatrixRegression, ::testing::ValuesIn(allCells()), [](const auto& info) {
    return std::string(name(info.param.op)) + "_" + std::string(name(info.param.grant)) + "_" + std::string(name(info.param.timing));
});

TEST(AuthMatrixCountRegression, CellCountsArePinned) {
    int total = 0, na[7] = {}, outcome[8] = {};
    for (const Cell& c : allCells()) {
        ++total;
        ++na[int(naRule(c.op, c.grant, c.timing))];
        ++outcome[int(expect(c.op, c.grant, c.timing))];
    }
    EXPECT_EQ(total, 720);
    EXPECT_EQ(na[int(NaRule::NA1)], 66); EXPECT_EQ(na[int(NaRule::NA2)], 24); EXPECT_EQ(na[int(NaRule::NA3)], 72);
    EXPECT_EQ(na[int(NaRule::NA4)], 22); EXPECT_EQ(na[int(NaRule::NA5)], 10); EXPECT_EQ(na[int(NaRule::NA6)], 9);
    EXPECT_EQ(outcome[int(Outcome::NotApplicable)], 203);
    const int applicable = total - outcome[int(Outcome::NotApplicable)];
    EXPECT_EQ(applicable, 517);
    EXPECT_EQ(outcome[int(Outcome::Ok)], 144 + 12 + 8);            // R7 + R3 implicit + R3 created
    EXPECT_EQ(outcome[int(Outcome::EndedNotAuthorized)], 79);
    EXPECT_EQ(outcome[int(Outcome::SessionRevoked)], 79);
    EXPECT_EQ(outcome[int(Outcome::DeniedThenOk)], 79);
    EXPECT_EQ(outcome[int(Outcome::Missing)], 24);                 // UnknownTopic
    EXPECT_EQ(outcome[int(Outcome::FilteredOut)], 3 + 4);          // Metadata R1 (3 timings) + R3 (4 grants)
    EXPECT_EQ(outcome[int(Outcome::Denied)], 59 + 8 + 18);         // R1 non-Metadata + R2 + R3 id-addressed (incl. ViewOpen)
}
