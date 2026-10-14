// DdlFinalFixRegressionTest.cpp — DDL conformance final fix wave, catalog history
// Task 9 port.
//
//   I2 / I5b (KL2, in doubt) — catalog form: a DDL whose event was appended and
//       then timed out / was demoted is answered the RETRIABLE NotLeader (F13,
//       spec §5.2 — no longer Internal); the event commits afterwards; a same-name
//       op is then answered from the catalog (TopicExists / the original result
//       for its retry) and a restart replays the log cleanly.  The harness-driven
//       orphan / adoption cases (I1, KL1b) are Task 10's port (DdlHarness on
//       CatalogDdlRig) — a create whose event never committed leaves nothing.
//   M2  (a failed __topic_schemas / __views rebuild refuses DDL) is gone with the
//       rebuild: the catalog is applied by the verifying CatalogApplier, whose
//       halt answers Internal (CatalogWritePathTest / AdminWorkerTest pin it).
//   M3  the in-flight state holds exactly what the commit answer needs.
//   C1  (a view plan never overtakes its sink's commit) is structural now: the
//       applier posts sinkApplied before viewInstalled on one FIFO sink.
//   Concern 4: a held DDL OK is answered Internal when its topic never serves.
#include <gtest/gtest.h>
#include "support/CatalogDdlRig.h"
#include <rune/node/AdminWorker.h>
#include <rune/node/DdlAnswerGate.h>

#include <string>
#include <type_traits>
#include <vector>

using namespace rune::node;
using rune::node::test::CatalogDdlRig;

// ── M3: the in-flight state (rule 3) ───────────────────────────────────────────
namespace {
template <class T> concept HasTicket = requires(T t) { t.ticket; };
template <class T> concept HoldsCommitAnswer = requires(T t) { t.req; t.event; t.offset; t.startedNs; };
} // namespace
static_assert(std::variant_size_v<AdminWorker::InFlight> == 2, "Idle | AwaitingCommit — no per-kind id states");
static_assert(!HasTicket<AdminWorker::AwaitingCommit>, "no create-topic round-trip: no ticket");
static_assert(HoldsCommitAnswer<AdminWorker::AwaitingCommit>, "the request, its event and its offset");

TEST(DdlFixRegression, M3_TheInFlightStateIsIdleOrAwaitingCommitRegression) {
    CatalogDdlRig rig("ff_m3", 3);
    rig.becomeServingLeader({1, 2});   // follower 2 silent: the event stays uncommitted
    EXPECT_EQ(rig.admin().inFlightStateName(), "Idle");
    (void)rig.submitDefineNoWait({.name = "t", .partitions = 1});
    EXPECT_EQ(rig.admin().inFlightStateName(), "AwaitingCommit");
    EXPECT_EQ(rig.admin().startedCount() - rig.admin().answeredCount(), 1u) << "exactly one started, unanswered";
}

// ── I2 / I5b: in doubt, catalog form ───────────────────────────────────────────
namespace {
enum class InDoubtFault : uint8_t { Timeout, Demotion };
enum class FirstOp : uint8_t { Define, View };
struct InDoubtCase { FirstOp first; InDoubtFault fault; };
std::string caseName(const testing::TestParamInfo<InDoubtCase>& i) {
    return std::string(i.param.first == FirstOp::Define ? "Define" : "View") + "_" +
           (i.param.fault == InDoubtFault::Timeout ? "Timeout" : "Demotion");
}
} // namespace

class DdlInDoubtRegression : public ::testing::TestWithParam<InDoubtCase> {};

TEST_P(DdlInDoubtRegression, CatalogSameNameOpThenRestart) {
    const InDoubtCase c = GetParam();
    CatalogDdlRig rig("ff_indoubt", 3);
    rig.becomeServingLeader();
    ASSERT_EQ(rig.define({.name = "src", .partitions = 1, .typed = true}).code, 0);
    rig.becomeServingLeader({1, 2});   // follower 2 silent from here on
    const rune::protocol::RequestId rid = rig.freshRequestId();
    int64_t corr = 0;
    if (c.first == FirstOp::Define) {
        corr = rig.submitDefineNoWait({.name = "x", .partitions = 1, .requestId = rid});
    } else {
        CreateViewMsg m;
        m.session = rig.sessionId(); m.corr = rig.nextCorr(); m.viewName = "x"; m.keyField = "id";
        m.planBlob = rig.plan("x", "src", false);
        m.meta.requestId = rid;
        corr = rig.submitRawNoWait(DdlRequest{std::move(m)});
    }
    ASSERT_EQ(rig.admin().inFlightStateName(), "AwaitingCommit");
    if (c.fault == InDoubtFault::Timeout) {
        rig.advanceClockPast(AdminWorker::ddlTimeoutNs());
    } else {
        rig.becomeFollower(/*leaderId=*/2);
    }
    ASSERT_TRUE(rig.answerFor(corr).has_value());
    EXPECT_EQ(rig.answerFor(corr)->code, 2) << "in doubt: the retriable NotLeader (F13 / spec 5.2), never Internal";
    // The appended event commits after its DDL was answered.
    if (c.fault == InDoubtFault::Demotion) rig.becomeServingLeader();   // every follower fetches: the tail commits
    else                                   rig.resumeFollower(2);
    for (int i = 0; i < 16; ++i) rig.pump();
    ASSERT_FALSE(rig.admin().applier().halted());
    ASSERT_EQ(rig.admin().roleName(), "LeaderServing");
    // Every same-name op is answered from the catalog — never a second object.
    EXPECT_EQ(rig.define({.name = "x", .partitions = 1}).code, 25);
    EXPECT_EQ(rig.createView("x", "src").code, c.first == FirstOp::View ? 32 : 25);
    // The retry of the in-doubt DDL is its original success (dedupe).
    if (c.first == FirstOp::Define) {
        const auto retry = rig.define({.name = "x", .partitions = 1, .requestId = rid});
        EXPECT_EQ(retry.code, 0);
    }
    const uint64_t digest = rig.admin().applier().state().digest();
    rig.restart();
    EXPECT_FALSE(rig.admin().applier().halted()) << "the restart replays the whole log";
    EXPECT_EQ(rig.admin().applier().state().digest(), digest);
}

INSTANTIATE_TEST_SUITE_P(InDoubt, DdlInDoubtRegression,
                         ::testing::Values(InDoubtCase{FirstOp::Define, InDoubtFault::Timeout},
                                           InDoubtCase{FirstOp::Define, InDoubtFault::Demotion},
                                           InDoubtCase{FirstOp::View, InDoubtFault::Timeout},
                                           InDoubtCase{FirstOp::View, InDoubtFault::Demotion}),
                         caseName);

// ── Concern 4 (controller ruling): a held DDL OK is never sent for a topic that
// does not serve.  DdlAnswerGate holds a DefineTopic / CreateView OK until its
// topic serves; past the hold limit with the topic STILL not serving, the request
// is answered Internal (6, in doubt) — never the OK (a false OK is a silent
// fallback).  The answer keeps the session + correlation id, so it is never lost.
namespace {
namespace pcl = rune::protocol::client;

std::vector<uint8_t> defineOk(int64_t corr, int32_t topicId) {
    pcl::DefineTopicResponse r;
    r.sessionId = 7; r.correlationId = corr; r.topicId = topicId; r.version = 1; r.errorCode = 0;
    std::vector<uint8_t> b(r.encodedSize());
    b.resize(r.encode(b.data()));
    return b;
}
std::vector<uint8_t> viewOk(int64_t corr, int32_t topicId) {
    pcl::CreateViewResponse r;
    r.sessionId = 7; r.correlationId = corr; r.viewTopicId = topicId; r.outputFingerprint = 0xabcdu;
    r.errorCode = 0;
    std::vector<uint8_t> b(r.encodedSize());
    b.resize(r.encode(b.data()));
    return b;
}
struct Sent { int64_t session; std::vector<uint8_t> frame; };
} // namespace

TEST(DdlAnswerGateRegression, HeldOkForATopicThatNeverServesIsAnsweredInternalNotOkRegression) {
    constexpr int64_t kHold = 1'000;
    DdlAnswerGate gate(4096, kHold);
    auto never = [](int32_t) { return false; };
    std::vector<Sent> sent;
    auto send = [&](int64_t s, const uint8_t* f, std::size_t n) { sent.push_back({s, {f, f + n}}); };
    std::vector<int32_t> overdue;
    auto onOverdue = [&](int64_t, int32_t t) { overdue.push_back(t); };

    const auto d = defineOk(11, 40);
    const auto v = viewOk(12, 41);
    ASSERT_EQ(gate.offer(100, d.data(), d.size(), 0, never), DdlAnswerGate::Offer::Held);
    ASSERT_EQ(gate.offer(101, v.data(), v.size(), 0, never), DdlAnswerGate::Offer::Held);

    EXPECT_EQ(gate.release(kHold - 1, never, send, onOverdue), 0) << "held within the limit";
    EXPECT_TRUE(sent.empty());

    // Past the limit: the define head goes out — as Internal.  The view becomes the
    // head at that moment, so it gets its own full hold, then goes out Internal too.
    EXPECT_EQ(gate.release(kHold, never, send, onOverdue), 1);
    EXPECT_EQ(gate.release(2 * kHold, never, send, onOverdue), 1);
    ASSERT_EQ(sent.size(), 2u);
    EXPECT_TRUE(gate.empty()) << "never swallowed";
    EXPECT_EQ(overdue, (std::vector<int32_t>{40, 41})) << "each in-doubt answer is reported (log + count)";

    EXPECT_EQ(sent[0].session, 100);
    const auto dr = pcl::DefineTopicResponse::decode(sent[0].frame.data(), sent[0].frame.size());
    ASSERT_TRUE(dr.has_value());
    EXPECT_EQ(dr->errorCode, static_cast<uint16_t>(pcl::ErrorCode::Internal)) << "in doubt, never OK";
    EXPECT_EQ(dr->correlationId, 11);
    EXPECT_EQ(dr->sessionId, 7);
    EXPECT_EQ(dr->topicId, -1);

    EXPECT_EQ(sent[1].session, 101);
    const auto vr = pcl::CreateViewResponse::decode(sent[1].frame.data(), sent[1].frame.size());
    ASSERT_TRUE(vr.has_value());
    EXPECT_EQ(vr->errorCode, static_cast<uint16_t>(pcl::ErrorCode::Internal)) << "in doubt, never OK";
    EXPECT_EQ(vr->correlationId, 12);
    EXPECT_EQ(vr->viewTopicId, -1);
}

TEST(DdlAnswerGateRegression, HeldOkWhoseTopicServesInTimeIsSentAsOkRegression) {
    constexpr int64_t kHold = 1'000;
    DdlAnswerGate gate(4096, kHold);
    bool serves = false;
    auto ready = [&](int32_t) { return serves; };
    std::vector<Sent> sent;
    auto send = [&](int64_t s, const uint8_t* f, std::size_t n) { sent.push_back({s, {f, f + n}}); };
    int overdue = 0;
    auto onOverdue = [&](int64_t, int32_t) { ++overdue; };

    const auto d = defineOk(21, 50);
    ASSERT_EQ(gate.offer(100, d.data(), d.size(), 0, ready), DdlAnswerGate::Offer::Held);
    EXPECT_EQ(gate.release(10, ready, send, onOverdue), 0);
    serves = true;
    EXPECT_EQ(gate.release(kHold + 5, ready, send, onOverdue), 1) << "serving at release: OK, even late";
    ASSERT_EQ(sent.size(), 1u);
    EXPECT_EQ(sent[0].frame, d) << "the original OK frame, byte for byte";
    EXPECT_EQ(overdue, 0);
}
