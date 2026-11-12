// Authentication spec section 11 - the request x grant x timing matrix (720 cells, NotApplicable skipped naming its rule).
#include "support/AuthMatrixExpectations.h"
#include "support/AuthMatrixRig.h"

#include <gtest/gtest.h>
#include <gtest/gtest-spi.h>

#include <sys/stat.h>

#include <algorithm>
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
    if (isViewRead(c.op))
        EXPECT_EQ(first.viewStatus, viewStatusOf(want.first)) << label << ": the exact ViewRowResponse status";

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
                if (isViewRead(c.op))
                    EXPECT_EQ(second.viewStatus, viewStatusOf(want.second)) << label << ": exact status";
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
            // relogin() resets the login throttle after each probe (issue #189's seam) and EVERY R5 cell probes it.
            EXPECT_EQ(cell.relogin(), want.reconnect) << label;
            break;
        }
        case Outcome::DeniedThenOk: {
            cell.grantFor(c.op, c.grant);
            ASSERT_FALSE(::testing::Test::HasFatalFailure());
            const auto second = cell.issue(c.op, false);
            EXPECT_EQ(second.code, want.second) << label;
            EXPECT_EQ(second.templ, second.wantTempl) << label << ": the second answer is not in the op's own response type";
            if (isViewRead(c.op)) EXPECT_EQ(second.viewStatus, viewStatusOf(want.second)) << label << ": exact status";
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
// Allocated bytes (st_blocks * 512) under `dir`; symlinks are not followed.  With `skipAeron`, subdirectories named
// "aeron" (embedded media-driver dirs) are not counted.
uint64_t allocatedBytes(const std::filesystem::path& dir, bool skipAeron = false) {
    uint64_t sum = 0;
    struct stat st{};
    if (::lstat(dir.c_str(), &st) != 0) return 0;
    sum += static_cast<uint64_t>(st.st_blocks) * 512;
    if (!S_ISDIR(st.st_mode)) return sum;
    std::error_code ec;
    for (std::filesystem::directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec)) {
        struct stat cs{};
        if (::lstat(it->path().c_str(), &cs) != 0 || S_ISLNK(cs.st_mode)) continue;
        if (S_ISDIR(cs.st_mode)) {
            if (skipAeron && it->path().filename() == "aeron") continue;
            sum += allocatedBytes(it->path(), skipAeron);
        } else {
            sum += static_cast<uint64_t>(cs.st_blocks) * 512;
        }
    }
    return sum;
}

// Log data under the rig root: everything except Aeron media-driver dirs (`aeron` anywhere, and the root's `client*`
// children, which are the test clients' embedded drivers).
uint64_t logDataBytes(const std::filesystem::path& root) {
    uint64_t sum = 0;
    std::error_code ec;
    for (std::filesystem::directory_iterator it(root, ec), end; !ec && it != end; it.increment(ec)) {
        if (it->path().filename().string().rfind("client", 0) == 0) continue;
        sum += allocatedBytes(it->path(), true);
    }
    return sum;
}
}  // namespace

// #571: every topic's active log segment is fallocate'd to its segmentRollBytes (64 MiB default), ~67.7 MB per topic and
// ~35 GB for the 720 cells in one process.  The rig now defines its topics with kRigSegmentRollBytes (1 MiB).  Measured with
// one shard (65 executed cells): before 3.69 GB allocated at peak (~57 MB per cell); after 449 MB for 64 cells (6 fixed internal topics x 67 MB
// = ~400 MB of that is the product default, plus ~71 MB of rig topics, ~1.1 MB each).  The budget is
// min(512 MiB, 4 x 449 MB) = 512 MiB over the first 64 applicable cells; 64 MiB rig topics would exceed 4 GB.
// That 449 MB counted the Aeron media-driver dirs too, so the internal-topic share above is not log data.
// The measure covers log data only: it excludes the Aeron media-driver dirs (broker/aeron and the client* dirs), because a
// closed session's receive images linger ~10 s by Aeron's liveness rule (~500 MB of 3 MB logbuffers in client1/images
// over this test's ~10 s) and are not log data.  Measured log data for 64 cells: 76 MB; the budget is 256 MiB (3x).
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
    const uint64_t bytes = logDataBytes(rig.rig().broker().dir());
    GTEST_LOG_(INFO) << "log data bytes after " << (ran > 64 ? 64 : ran) << " cells: " << bytes;
    constexpr uint64_t kBudget = 256ull << 20;
    EXPECT_LT(bytes, kBudget) << "the rig's log data holds " << bytes << " allocated bytes after 64 cells";
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
    const std::int32_t id = ok ? rune::node::typeIdOf(rune::node::RuneMetric::LoginsOk) : rune::node::typeIdOf(rune::node::RuneMetric::LoginsFailed);
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

// #579: one stray login fails exactly ONE cell: the cell that sees the counter above the tally fails (and names it), the
// rig then adopts the surplus into its tally, so the cells after it report only their own deltas.
TEST(AuthMatrixRigRegression, AStrayLoginFailsExactlyOneCellRegression) {
    AuthMatrixRig rig;
    strayLogin(rig, /*ok=*/true);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    std::string problems[3];
    for (std::string& problem : problems) {
        auto cell = rig.newCell();
        cell.grantFor(AMOp::Produce, AMGrant::None);
        ASSERT_FALSE(::testing::Test::HasFatalFailure());
        cell.login();
        problem = cell.countersProblem(false);
        cell.close();
    }
    EXPECT_NE(problems[0].find("logins ok counter is"), std::string::npos) << "the cell after the stray must fail: '" << problems[0] << "'";
    EXPECT_EQ(problems[1], "") << "the second cell reports only its own deltas";
    EXPECT_EQ(problems[2], "") << "the third cell reports only its own deltas";
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
    {
        // The raw test client with NO config argument (AuthStress's user CREATE TOPIC, CatalogBoot and ~35 other
        // call sites): ShardTestClient::sendDefineTopic defaults to a 1 MiB segment, never the 64 MiB broker default.
        auto& r = rig.rig();
        const uint64_t before = allocatedBytes(dir);
        ASSERT_EQ(r.ddlCode(r.c().sendDefineTopic(r.ch(), r.stream(), r.builder, "adv.rawdefault", 1,
                                                  rune::node::test::authRigCharBlob())), 0);
        const uint64_t grew = grownBy(dir, before);
        EXPECT_GT(grew, 0u) << "ShardTestClient::sendDefineTopic default: no segment appeared";
        EXPECT_LT(grew, kPerTopicCeiling) << "ShardTestClient::sendDefineTopic default config";
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

// ── #217: collision and refusal answers reveal nothing about an object the caller cannot DESCRIBE ────────────────────
// D15: any operation on a name implies DESCRIBE on it (per kind: TOPIC grants describe topics, VIEW grants views),
// so the principals differ by KIND.  `topicOnly` holds CREATE/ALTER/DROP on TOPIC 'secret.' / 'Secret.' (never a view),
// `viewOnly` the same on VIEW, `seer` both, `reader` both plus DESCRIBE ON CLUSTER (reads the whole catalog, #220),
// `catOnly` topicOnly plus DESCRIBE ON CLUSTER (no VIEW grant: only the catalog bit shows it a view).
namespace {
namespace pcl = rune::protocol::client;

struct DdlAns {
    int code = -1;
    int64_t id = -2, version = -2;
    std::string detail;
    std::vector<uint8_t> body;   // the frame without its header (session / correlation id)
};

DdlAns ddlAnswerOf(rune::node::test::AuthRig& r, int64_t corr) {
    DdlAns a;
    const auto any = r.c().awaitAnyDdlAnswer(corr, rune::node::test::AuthRig::dl(15));
    const auto raw = r.c().awaitRaw(corr, rune::node::test::AuthRig::dl(2));
    if (!any || !raw) return a;
    const auto h = pcl::MessageHeader::decode(raw->data(), raw->size());
    if (!h) return a;
    a.body.assign(raw->begin() + pcl::MessageHeader::ENCODED_SIZE, raw->end());
    const uint8_t* p = raw->data();
    const std::size_t n = raw->size();
    using T = pcl::TemplateId;
    switch (static_cast<T>(h->templateId)) {
        case T::DefineTopicResponse:
            if (const auto m = pcl::DefineTopicResponse::decode(p, n)) {
                a.code = m->errorCode; a.id = m->topicId; a.version = m->version;
            }
            break;
        case T::AlterTopicResponse:
            if (const auto m = pcl::AlterTopicResponse::decode(p, n)) {
                a.code = m->errorCode; a.id = m->topicId; a.version = m->newVersion;
            }
            break;
        case T::CreateViewResponse:
            if (const auto m = pcl::CreateViewResponse::decode(p, n)) {
                a.code = m->errorCode; a.id = m->viewTopicId; a.version = 0;
            }
            break;
        case T::DdlResponse:
            if (const auto m = pcl::DdlResponse::decode(p, n)) {
                a.code = m->errorCode; a.id = m->gateTopicId; a.version = m->newVersion;
                a.detail = std::string(m->detail);
            }
            break;
        default:
            if (const auto m = pcl::ErrorResponse::decode(p, n)) {
                a.code = m->code; a.id = -1; a.version = 0; a.detail = std::string(m->message);
            }
            break;
    }
    return a;
}

bool hasLe32(const std::vector<uint8_t>& b, int32_t v) {
    const uint8_t le[4] = {uint8_t(v), uint8_t(v >> 8), uint8_t(v >> 16), uint8_t(v >> 24)};
    for (std::size_t i = 0; i + 4 <= b.size(); ++i)
        if (std::equal(le, le + 4, b.begin() + static_cast<std::ptrdiff_t>(i))) return true;
    return false;
}
bool hasName(const std::vector<uint8_t>& b, std::string_view s) {
    return std::search(b.begin(), b.end(), s.begin(), s.end()) != b.end();
}
}  // namespace

TEST(AuthInteractionMatrixRegression, CollisionAnswersRevealNothingToAPrincipalWithoutDescribeRegression) {
    using namespace rune::node::test;
    using namespace rune::node::test::grants;
    AuthRig r("coll217");
    const int32_t src = r.defineTopic(r.builder, "secret.src");
    const int32_t payroll = r.defineTopic(r.builder, "secret.payroll");
    const int32_t openSrc = r.defineTopic(r.builder, "open.src");
    const int32_t board = r.createView(r.builder, "secret.board", src);
    ASSERT_GE(src, 0); ASSERT_GE(payroll, 0); ASSERT_GE(openSrc, 0); ASSERT_GE(board, 0);

    std::vector<AuthGrant> topicG, viewG;
    for (const char* p : {"secret.", "Secret."})
        for (const auto op : {SO::Create, SO::Alter, SO::Drop}) {
            topicG.push_back(prefix(op, ST::Topic, p));
            viewG.push_back(prefix(op, ST::View, p));
        }
    topicG.push_back(prefix(SO::Read, ST::Topic, "open."));   // a view's source needs READ
    // (topicG already holds the READ of the sources)
    std::vector<AuthGrant> seerG = topicG, readerG, catOnlyG = topicG;
    seerG.insert(seerG.end(), viewG.begin(), viewG.end());
    readerG = seerG;
    viewG.push_back(prefix(SO::Read, ST::Topic, "open."));
    readerG.push_back(cluster(SO::Describe));
    catOnlyG.push_back(cluster(SO::Describe));
    const int64_t topicOnly = r.newUser(topicG), viewOnly = r.newUser(viewG), seer = r.newUser(seerG),
                  reader = r.newUser(readerG), catOnly = r.newUser(catOnlyG);

    auto define = [&](int64_t sid, const std::string& n) {
        return ddlAnswerOf(r, r.c().sendDefineTopic(r.ch(), r.stream(), sid, n, 1, authRigCharBlob(), 0, {}, 0,
                                                    authRigTopicConfig()));
    };
    auto createView = [&](int64_t sid, const std::string& n) {
        return ddlAnswerOf(r, r.c().sendCreateView(r.ch(), r.stream(), sid, n, "id", 0, authRigViewPlan(n, {openSrc})));
    };
    auto alter = [&](int64_t sid, const std::string& n) {
        rune::codec::AlterOp o{};
        o.type = rune::codec::AlterOpType::Add; o.name = "extra";
        o.primType = rune::codec::PrimType::Int64; o.fixedLen = 0;
        return ddlAnswerOf(r, r.c().sendAlterTopic(r.ch(), r.stream(), sid, n, 0, rune::codec::encodeAlterOps({o})));
    };
    auto alterCfg = [&](int64_t sid, const std::string& n) {
        const pcl::ConfigChangeWire cc{static_cast<uint8_t>(rune::protocol::ConfigKey::RetentionMs), 600'000};
        return ddlAnswerOf(r, r.c().sendAlterTopicConfig(r.ch(), r.stream(), sid, n, 0, {cc}));
    };
    auto dropTopic = [&](int64_t sid, const std::string& n) {
        return ddlAnswerOf(r, r.c().sendDropTopic(r.ch(), r.stream(), sid, n));
    };
    constexpr uint16_t kTopicExists = 25, kViewNotAlterable = 38, kUnknownTopic = 7;
    auto blindShape = [&](const DdlAns& a, const char* what) {
        EXPECT_EQ(a.code, kTopicExists) << what;
        EXPECT_EQ(a.id, -1) << what;
        EXPECT_EQ(a.version, 0) << what;
        EXPECT_FALSE(hasLe32(a.body, payroll) || hasLe32(a.body, board) || hasLe32(a.body, src))
            << what << ": a hidden id";
    };

    // CREATE TOPIC secret.payroll: the topic kind is describable to topicOnly, seer, reader (real id and version).
    for (const int64_t who : {topicOnly, seer, reader}) {
        const DdlAns a = define(who, "secret.payroll");
        EXPECT_EQ(a.code, kTopicExists); EXPECT_EQ(a.id, payroll); EXPECT_EQ(a.version, 1);
    }
    // CREATE VIEW secret.payroll (a TOPIC name): viewOnly cannot describe topics.
    blindShape(createView(viewOnly, "secret.payroll"), "CREATE VIEW over a topic name, viewOnly");
    EXPECT_EQ(createView(seer, "secret.payroll").id, payroll);
    EXPECT_EQ(createView(reader, "secret.payroll").id, payroll);
    // CREATE TOPIC secret.board (a VIEW name): topicOnly cannot describe views.
    blindShape(define(topicOnly, "secret.board"), "CREATE TOPIC over a view name, topicOnly");
    EXPECT_EQ(define(seer, "secret.board").id, board);
    EXPECT_EQ(define(reader, "secret.board").id, board);
    EXPECT_EQ(define(catOnly, "secret.board").id, board);   // no VIEW grant at all: the catalog bit alone shows it
    // CREATE TOPIC Secret.Payroll (letter case): only a catalog reader sees the other object.
    for (const int64_t who : {topicOnly, seer}) {
        const DdlAns a = define(who, "Secret.Payroll");
        EXPECT_EQ(a.code, kTopicExists); EXPECT_EQ(a.id, -1); EXPECT_EQ(a.version, 0);
        EXPECT_FALSE(hasName(a.body, "secret.payroll"));
        EXPECT_FALSE(hasLe32(a.body, payroll));
    }
    for (const int64_t who : {reader, catOnly}) {
        const DdlAns a = define(who, "Secret.Payroll");
        EXPECT_EQ(a.code, kTopicExists); EXPECT_EQ(a.id, payroll);
    }
    // ALTER TOPIC on a view name: topicOnly gets exactly the answer of a name that is no object.
    {
        const DdlAns hidden = alter(topicOnly, "secret.board"), missing = alter(topicOnly, "secret.missing");
        EXPECT_EQ(hidden.code, kUnknownTopic);
        EXPECT_EQ(hidden.code, missing.code); EXPECT_EQ(hidden.id, missing.id);
        EXPECT_EQ(hidden.version, missing.version);
        EXPECT_EQ(hidden.body, missing.body);
        EXPECT_EQ(alter(seer, "secret.board").code, kViewNotAlterable);
        EXPECT_EQ(alter(reader, "secret.board").code, kViewNotAlterable);
        EXPECT_EQ(alter(catOnly, "secret.board").code, kViewNotAlterable);
    }
    {
        const DdlAns hidden = alterCfg(topicOnly, "secret.board"), missing = alterCfg(topicOnly, "secret.missing");
        EXPECT_EQ(hidden.code, kUnknownTopic);
        EXPECT_EQ(hidden.code, missing.code); EXPECT_EQ(hidden.id, missing.id);
        EXPECT_EQ(hidden.version, missing.version);
        EXPECT_FALSE(hasName(hidden.body, "secret.missing"));
        std::string swapped = missing.detail;
        swapped.replace(swapped.find("secret.missing"), 14, "secret.board");
        EXPECT_EQ(hidden.detail, swapped);
        EXPECT_EQ(alterCfg(seer, "secret.board").code, kViewNotAlterable);
        EXPECT_EQ(alterCfg(reader, "secret.board").code, kViewNotAlterable);
    }
    // DROP TOPIC secret.src (read by the view secret.board): the view's name goes to catalog readers only.
    for (const int64_t who : {topicOnly, seer}) {
        const DdlAns a = dropTopic(who, "secret.src");
        EXPECT_NE(a.code, 0);
        EXPECT_NE(a.detail.find("is read by a view"), std::string::npos) << a.detail;
        EXPECT_FALSE(hasName(a.body, "secret.board"));
        EXPECT_FALSE(hasLe32(a.body, board));
    }
    {
        const DdlAns a = dropTopic(catOnly, "secret.src");
        EXPECT_NE(a.code, 0);
        EXPECT_NE(a.detail.find("'secret.board'"), std::string::npos) << a.detail;
    }
}
