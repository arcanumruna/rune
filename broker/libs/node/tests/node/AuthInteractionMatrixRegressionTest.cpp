// Authentication spec section 11 - the request x grant x timing matrix (720 cells, NotApplicable skipped naming its rule).
#include "support/AuthMatrixExpectations.h"
#include "support/AuthMatrixRig.h"

#include <gtest/gtest.h>
#include <gtest/gtest-spi.h>

#include <sys/stat.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <filesystem>
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

static void runCell(AuthMatrixRig& rig, const Cell& c) {
    if (const NaRule na = naRule(c.op, c.grant, c.timing); na != NaRule::None) GTEST_SKIP() << name(na);
    const ExpectedCodes want = expectedCodes(c.op, c.grant, c.timing);
    const std::string label = std::string(name(c.op)) + "/" + std::string(name(c.grant)) + "/" + std::string(name(c.timing));
    const bool missing = c.timing == Timing::ObjectMissing;
    const Outcome outcome = expect(c.op, c.grant, c.timing);
    auto cell = rig.newCell();
    struct Closer { decltype(cell)& c; ~Closer() { c.close(); } } closer{cell};

    if (c.timing != Timing::CreatedAfterLogin) { cell.createObjects(c.op, /*supportingOnly=*/missing); ASSERT_FALSE(::testing::Test::HasFatalFailure()); }
    if (c.timing != Timing::GrantedAfterLogin) { cell.grantFor(c.op, c.grant); ASSERT_FALSE(::testing::Test::HasFatalFailure()); }
    else { cell.grantFor(c.op, Grant::None); }   // creates the user only
    cell.login();
    if (c.timing == Timing::CreatedAfterLogin) { cell.createObjects(c.op); ASSERT_FALSE(::testing::Test::HasFatalFailure()); }

    const auto first = cell.issue(c.op, missing);
    EXPECT_EQ(first.code, want.first) << label;
    EXPECT_EQ(first.templ, first.wantTempl) << label << ": the answer is not in the op's own response type";
    if (want.first == 45) EXPECT_EQ(first.appended, 0) << label << ": #278: a refused produce appended";
    if (c.op == Op::Metadata) EXPECT_EQ(first.present, want.presentFirst) << label;
    if (want.viewStatus != kNone) EXPECT_EQ(first.viewStatus, want.viewStatus) << label;

    // Streams under RevokedMid / UserDisabledMid: the subscription is live and receiving before the change.
    std::string why;
    const bool liveStream = isStream(c.op) && (outcome == Outcome::EndedNotAuthorized || outcome == Outcome::SessionRevoked);
    if (liveStream) { EXPECT_GT(first.subId, 0) << label << ": a live stream has a subscription id"; if (first.subId <= 0) return; }
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
                if (want.second == 45) EXPECT_EQ(second.appended, 0) << label << ": #278: a refused produce appended";
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

TEST_P(AuthMatrixRegression, Cell) { runCell(*rig_, GetParam()); }

INSTANTIATE_TEST_SUITE_P(All, AuthMatrixRegression, ::testing::ValuesIn(allCells()), [](const auto& info) {
    return std::string(name(info.param.op)) + "_" + std::string(name(info.param.grant)) + "_" + std::string(name(info.param.timing));
});

// N1: the matrix once failed a cell at random because the client's Keepalive ticks drew their correlation ids from the same
// counter as requests, so lastCorrelationId() (read after a helper's wait) named a Keepalive, not the request.
TEST(AuthMatrixRigRegression, KeepaliveDoesNotTakeCorrelationIdsFromRequestsRegression) {
    rune::node::test::AuthRig r("kacorr");
    const int64_t sid = r.newUser({});
    (void)sid;
    const int64_t before = r.c().lastCorrelationId();
    r.c().pollFor(std::chrono::milliseconds(2500), [] { return false; });   // two Keepalive ticks of the live sessions
    EXPECT_EQ(r.c().lastCorrelationId(), before) << "a Keepalive consumed a request correlation id";
}

// #4: a Cell's baseline of the logins counters was a racy read of the CnC counter (published asynchronously by the control
// thread), taken while the rig's own setup logins were still unpublished: 1-2 low under load, then "logins ok counter is 5,
// expected 4".  The baseline is now the rig's own tally (admin, builder, the writer's user, the writer = 4 setup logins),
// after waiting for the counters to catch up.  The end-to-end check that this holds under load is the
// `ctest -R AuthMatrix -j6` run (the old racy read failed 8-9 of 722 cells there); a delay injection is not deterministic.
TEST(AuthMatrixRigRegression, CellBaselineIsTheRigTallyNotARacyReadRegression) {
    AuthMatrixRig rig;
    EXPECT_EQ(rig.issuedOk(), 4);
    EXPECT_EQ(rig.issuedFailed(), 0);
    EXPECT_EQ(rig.issuedRevoked(), 0);
    auto cell = rig.newCell();
    EXPECT_EQ(cell.okAtStart(), 4);
    EXPECT_EQ(cell.failedAtStart(), 0);
    EXPECT_EQ(cell.revokedAtStart(), 0);
    cell.grantFor(AuthMatrixRig::Op::Produce, AuthMatrixRig::Grant::None);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    cell.login();
    EXPECT_EQ(cell.countersProblem(false), "");
    EXPECT_EQ(rig.issuedOk(), 5);
    cell.close();
    auto next = rig.newCell();   // the next cell's baseline includes that login, whatever the publish timing was
    EXPECT_EQ(next.okAtStart(), 5);
}

namespace {
// Allocated bytes (st_blocks * 512) under `dir`; symlinks are not followed.
uint64_t allocatedBytes(const std::filesystem::path& dir) {
    uint64_t sum = 0;
    struct stat st{};
    if (::lstat(dir.c_str(), &st) != 0) return 0;
    sum += static_cast<uint64_t>(st.st_blocks) * 512;
    if (!S_ISDIR(st.st_mode)) return sum;
    std::error_code ec;
    for (std::filesystem::directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec)) {
        struct stat cs{};
        if (::lstat(it->path().c_str(), &cs) != 0 || S_ISLNK(cs.st_mode)) continue;
        sum += S_ISDIR(cs.st_mode) ? allocatedBytes(it->path()) : static_cast<uint64_t>(cs.st_blocks) * 512;
    }
    return sum;
}
}  // namespace

// #571: every topic's active log segment is fallocate'd to its segmentRollBytes (64 MiB default), ~67.7 MB per topic and
// ~35 GB for the 720 cells in one process.  The rig now defines its topics with kRigSegmentRollBytes (1 MiB).  Measured with
// one shard (65 executed cells): before 3.69 GB allocated at peak (~57 MB per cell); after 449 MB for 64 cells (6 fixed internal topics x 67 MB
// = ~400 MB of that is the product default, plus ~71 MB of rig topics, ~1.1 MB each).  The budget is
// min(512 MiB, 4 x 449 MB) = 512 MiB over the first 64 applicable cells; 64 MiB rig topics would exceed 4 GB.
// Mutation: kRigSegmentRollBytes = 64 MiB turns this red.
TEST(AuthMatrixDiskRegression, TheMatrixRigStaysUnderADiskBudgetRegression) {
    AuthMatrixRig rig;
    int ran = 0;
    for (const Cell& c : allCells()) {
        if (naRule(c.op, c.grant, c.timing) != NaRule::None) continue;
        if (ran++ == 64) break;
        runCell(rig, c);
        ASSERT_FALSE(::testing::Test::HasFatalFailure());
    }
    const uint64_t bytes = allocatedBytes(rig.rig().broker().dir());
    GTEST_LOG_(INFO) << "allocated bytes after " << (ran > 64 ? 64 : ran) << " cells: " << bytes;
    constexpr uint64_t kBudget = 512ull << 20;
    EXPECT_LT(bytes, kBudget) << "the rig's data dir holds " << bytes << " allocated bytes after 64 cells";
}

// ── PR #574 adversarial review: attacks on the #4 baseline and the #571 rig topic size ─────────────────────────────
namespace {
using AMOp = AuthMatrixRig::Op;
using AMGrant = AuthMatrixRig::Grant;
// Waits (bounded) until the CnC counter `id` reads exactly `want`; false on the deadline.
bool counterReaches(AuthMatrixRig& rig, std::int32_t id, int64_t want) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (std::chrono::steady_clock::now() < deadline) {
        if (rig.rig().broker().counter(id).value_or(-1) == want) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return false;
}
// One real login the rig never tallies (a stray), already published to the CnC counter before the next cell starts.
void strayLogin(AuthMatrixRig& rig, bool ok) {
    const int64_t sid = rig.rig().wire().connect(rig.rig().ch());
    ASSERT_GT(sid, 0);
    const std::string pw = ok ? rune::node::test::AuthRig::kPw : "not-the-password-000";
    const auto l = rig.rig().wire().login(rig.rig().ch(), sid, "builder", {pw, pw});
    ASSERT_EQ(l.ok(), ok);
    const std::int32_t id = ok ? rune::node::kLoginsOkCounterTypeId : rune::node::kLoginsFailedCounterTypeId;
    ASSERT_TRUE(counterReaches(rig, id, (ok ? rig.issuedOk() : rig.issuedFailed()) + 1))
        << "the stray login never reached the CnC counter";
}
}  // namespace

// #4 must not mask a real stray login: a login nobody in the cell made, landed between two cells and fully published,
// still fails the next cell's assertion 5 (a baseline re-read from the counter would silently adopt it).
TEST(AuthMatrixRigRegression, AStrayLoginBetweenCellsFailsTheNextCellAdversarialRegression) {
    AuthMatrixRig rig;
    strayLogin(rig, /*ok=*/true);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    auto cell = rig.newCell();
    cell.grantFor(AMOp::Produce, AMGrant::None);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    cell.login();
    const std::string problem = cell.countersProblem(false);
    EXPECT_NE(problem.find("logins ok counter is"), std::string::npos) << "a stray successful login was masked: '" << problem << "'";
    cell.close();
}

TEST(AuthMatrixRigRegression, AStrayFailedLoginBetweenCellsFailsTheNextCellAdversarialRegression) {
    AuthMatrixRig rig;
    strayLogin(rig, /*ok=*/false);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    auto cell = rig.newCell();
    cell.grantFor(AMOp::Produce, AMGrant::None);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    cell.login();
    const std::string problem = cell.countersProblem(false);
    EXPECT_NE(problem.find("logins failed counter is"), std::string::npos) << "a stray failed login was masked: '" << problem << "'";
    cell.close();
}

// The #4 wait is bounded and loud: a tally the broker never reaches (a login the rig believes happened) fails the cell's
// setup within the wait's bound, exactly once, instead of hanging or passing.
TEST(AuthMatrixRigRegression, AnOverTalliedLoginFailsTheBaselineWaitWithinItsBoundAdversarialRegression) {
    AuthMatrixRig rig;
    rig.noteLogin(true);   // the rig now believes in one more successful login than the broker accepted
    ::testing::TestPartResultArray results;
    const auto t0 = std::chrono::steady_clock::now();
    {
        ::testing::ScopedFakeTestPartResultReporter reporter(
            ::testing::ScopedFakeTestPartResultReporter::INTERCEPT_ONLY_CURRENT_THREAD, &results);
        auto cell = rig.newCell();
        (void)cell;
    }
    const auto took = std::chrono::steady_clock::now() - t0;
    ASSERT_EQ(results.size(), 1) << "the baseline wait did not report the unreachable tally exactly once";
    EXPECT_TRUE(results.GetTestPartResult(0).nonfatally_failed());
    EXPECT_NE(std::string(results.GetTestPartResult(0).message()).find("logins ok counter is"), std::string::npos)
        << results.GetTestPartResult(0).message();
    EXPECT_LT(took, std::chrono::seconds(30)) << "the baseline wait is not bounded";
}

namespace {
// The data dir's growth since `before`, once a new topic's segment has been preallocated (growth >= 1 MiB, bounded
// wait: a partition opens asynchronously after its DDL answer); 0 if it never materialised.
uint64_t grownBy(const std::filesystem::path& dir, uint64_t before) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (std::chrono::steady_clock::now() < deadline) {
        const uint64_t now = allocatedBytes(dir);
        if (now >= before + (1ull << 20)) return now - before;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return 0;
}
}  // namespace

// #571 on every rig path that creates a topic, not only the first 64 cells (Produce / ProduceRouted / TxnProduce):
// the builder's defineTopic / defineTopicCode, the untyped topic, and the CreateTopic / CreateSchema / CreateView ops
// as the cell's own user.  Each new topic adds ~1 MiB, never a 64 MiB preallocated segment.
TEST(AuthMatrixDiskRegression, EveryRigTopicCreatePathRollsAtTheRigSegmentSizeAdversarialRegression) {
    AuthMatrixRig rig;
    const auto dir = rig.rig().broker().dir();
    constexpr uint64_t kPerTopicCeiling = 16ull << 20;   // 1 MiB segment + index + slack; the 64 MiB default is 4x over
    {
        const uint64_t before = allocatedBytes(dir);
        ASSERT_GE(rig.rig().defineTopic(rig.rig().builder, "adv.def"), 0);
        const uint64_t grew = grownBy(dir, before);
        EXPECT_GT(grew, 0u) << "AuthRig::defineTopic: no segment appeared";
        EXPECT_LT(grew, kPerTopicCeiling) << "AuthRig::defineTopic";
    }
    {
        const uint64_t before = allocatedBytes(dir);
        ASSERT_EQ(rig.rig().defineTopicCode(rig.rig().builder, "adv.code"), 0);
        const uint64_t grew = grownBy(dir, before);
        EXPECT_GT(grew, 0u) << "AuthRig::defineTopicCode: no segment appeared";
        EXPECT_LT(grew, kPerTopicCeiling) << "AuthRig::defineTopicCode";
    }
    for (const AMOp op : {AMOp::CreateTopic, AMOp::CreateSchema, AMOp::CreateView}) {
        const Cell c{op, Grant::Exact, Timing::Steady};
        ASSERT_EQ(naRule(c.op, c.grant, c.timing), NaRule::None);
        ASSERT_EQ(expect(c.op, c.grant, c.timing), Outcome::Ok);
        const uint64_t before = allocatedBytes(dir);
        runCell(rig, c);
        ASSERT_FALSE(::testing::Test::HasFatalFailure());
        const uint64_t grew = grownBy(dir, before);
        EXPECT_GT(grew, 0u) << name(op) << ": no segment appeared";
        // CreateTopic: the new topic; CreateSchema: the untyped topic + its schema; CreateView: the source topic.
        EXPECT_LT(grew, 2 * kPerTopicCeiling) << name(op) << " grew the data dir by " << grew << " bytes";
    }
}

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
