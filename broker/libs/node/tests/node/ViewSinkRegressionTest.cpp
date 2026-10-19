// ViewSinkRegressionTest.cpp -- VLS Part B (M14): a view sink is a replica-less, catalog-only namespace.
// One real ClientGateway + WorkerPool (GatewayViewReadRig) drives every case.
#include "support/ViewReadRig.h"

#include <rune/gateway/ObjectIndex.h>
#include <rune/gateway/PermissionTable.h>
#include <rune/gateway/SecurityView.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

namespace {
using rune::node::test::GatewayViewReadRig;
namespace pc = rune::protocol::client;
using Mode = GatewayViewReadRig::Mode;

constexpr int32_t kWorkers = 4;

std::vector<uint8_t> produceFrame(int64_t sid, int32_t topic, int64_t corr) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.append("k", "v", 1000);
    const auto batch = b.build();
    pc::Produce m;
    m.sessionId = sid; m.correlationId = corr; m.topicId = topic; m.partition = 0; m.partitionKnown = 1;
    m.acks = 0; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
    m.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> f(m.encodedSize());
    m.encode(f.data());
    return f;
}
std::vector<uint8_t> subscribeFrame(int64_t sid, int32_t topic, int64_t corr) {
    pc::Subscribe m;
    m.sessionId = sid; m.correlationId = corr; m.topicId = topic; m.partition = 0; m.partitionKnown = 1;
    std::vector<uint8_t> f(m.encodedSize());
    m.encode(f.data());
    return f;
}
void requireAuth(GatewayViewReadRig& rig, rune::gateway::SecurityView& view) {
    rig.gw.setAuthMode(rune::gateway::GatewayAuthMode::Required, &view);
    rune::gateway::ObjectIndex idx;
    idx.onTopic(GatewayViewReadRig::kSrcTopic, "character");
    idx.onView(GatewayViewReadRig::kViewTopic, "char_hp");
    rig.gw.setSessionAuthForTest(rig.sid, rune::gateway::AuthAuthenticated{
        1, "nobody", rune::gateway::PermissionTable::compile({}, idx)});   // no grants: default deny
}
}  // namespace

// X188: the sink used to be remapped onto the source's router group AFTER its replicas were assigned ("would MOVE" ->
// logic_error -> broker halt).  A sink now owns no replica and no router entry; GetViewRow routes by the SOURCE.
TEST(ViewSinkNoMoveRegression, GetViewRowOnAMultiWorkerPoolRoutesBySourceAndTheSinkOwnsNothing) {
    GatewayViewReadRig rig(Mode::ServedHere, /*fillers=*/0, kWorkers);   // the ctor warms up until key 7 is served
    ASSERT_NE(rig.pool.ownerWorkerId(GatewayViewReadRig::kViewTopic, 0), rig.srcOwner)
        << "the test is only meaningful when routing by the sink id would land on another worker";
    EXPECT_TRUE(rig.readAndAwait(rig.getViewRowFrame(900, 7))) << "OK through the gateway, on the source owner";
    EXPECT_EQ(rig.lastHp, 70);
    EXPECT_TRUE(rig.registry->isViewSink(GatewayViewReadRig::kViewTopic));
    EXPECT_EQ(rig.registry->topic(GatewayViewReadRig::kViewTopic), nullptr) << "no serving spec, no log";
    EXPECT_EQ(rig.registry->log(GatewayViewReadRig::kViewTopic, 0), nullptr);
}

TEST(ViewSinkPartitionMismatchRegression, ASinkWithAnotherPartitionCountThanItsSourceAnswersUnknownView) {
    GatewayViewReadRig rig(Mode::ServedHere, 0);
    ASSERT_TRUE(rig.readAndAwait(rig.getViewRowFrame(901, 7))) << "matching partitioning: served";
    rune::node::TopicSpec src;   // the source is registered with 2 partitions; the sink has 1
    src.name = "character"; src.partitionCount = 2;
    src.persistence = rune::node::PersistenceMode::Transient;
    ASSERT_EQ(rig.registry->createTopicAt(GatewayViewReadRig::kSrcTopic, src), GatewayViewReadRig::kSrcTopic);
    rig.gw.applyGetViewRowForTest(rig.getViewRowFrame(902, 7).data(), rig.getViewRowFrame(902, 7).size());
    const auto r = pc::ViewRowResponse::decode(rig.lastFrame.data(), rig.lastFrame.size());
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->status, pc::ViewRowResponse::kUnknownView) << "fail closed: never a wrong-table read";
}

TEST(ViewSinkRefusalRegression, GatewayProduceAndSubscribeOnASinkAnswerInvalidRequest) {
    GatewayViewReadRig rig(Mode::ServedHere, 0);
    const auto p = produceFrame(rig.sid, GatewayViewReadRig::kViewTopic, 910);
    rig.gw.applyProduceForTest(p.data(), p.size());
    EXPECT_EQ(rig.lastCode(), static_cast<int32_t>(pc::ErrorCode::InvalidRequest)) << "Produce";
    const auto s = subscribeFrame(rig.sid, GatewayViewReadRig::kViewTopic, 911);
    rig.gw.applySubscribeForTest(s.data(), s.size());
    EXPECT_EQ(rig.lastCode(), static_cast<int32_t>(pc::ErrorCode::InvalidRequest)) << "Subscribe";
}

// M14: under auth the authorization answer (45) comes BEFORE the InvalidRequest refusal -- a hidden sink and a missing
// id answer alike.
TEST(ViewSinkRefusalRegression, UnderAuthAnUngrantedSessionGets45BeforeInvalidRequest) {
    GatewayViewReadRig rig(Mode::ServedHere, 0);
    rune::gateway::SecurityView view{rune::security::SecurityState{}};
    requireAuth(rig, view);
    const auto p = produceFrame(rig.sid, GatewayViewReadRig::kViewTopic, 920);
    rig.gw.applyProduceForTest(p.data(), p.size());
    EXPECT_EQ(rig.lastCode(), static_cast<int32_t>(pc::ErrorCode::NotAuthorized)) << "Produce";
    const auto s = subscribeFrame(rig.sid, GatewayViewReadRig::kViewTopic, 921);
    rig.gw.applySubscribeForTest(s.data(), s.size());
    EXPECT_EQ(rig.lastCode(), static_cast<int32_t>(pc::ErrorCode::NotAuthorized)) << "Subscribe";
}

// Worker ingress (shard mode bypasses the gateway): a produce to a sink id is InvalidRequest once the plan is installed
// on the worker; before that (no plan yet) it is the retriable NotLeader.
TEST(ViewSinkRefusalRegression, WorkerProduceOnASinkIsInvalidRequestOnceThePlanIsInstalled) {
    using namespace rune::node;
    auto produceOn = [](WorkerPool& pool, int32_t w, int32_t topic) {
        WorkerOp op;
        op.type = WorkerOp::Type::Produce;
        op.topicId = topic; op.partition = 0; op.sessionId = 1; op.correlationId = 77;
        op.recordCount = 1;
        op.recordBatchBytes = std::vector<uint8_t>(64, 0);
        EXPECT_TRUE(pool.worker(w).tryEnqueueOp(std::move(op)));
        for (int i = 0; i < 4; ++i) (void)pool.worker(w).doWork(1'000'000LL * (i + 1));
        int16_t code = -1;
        pool.drainAllReturns([&](WorkerReturn r) { code = r.errorCode; });
        return code;
    };
    GatewayViewReadRig rig(Mode::ServedHere, 0);
    EXPECT_EQ(produceOn(rig.pool, rig.srcOwner, GatewayViewReadRig::kViewTopic),
              static_cast<int16_t>(pc::ErrorCode::InvalidRequest)) << "plan installed";
    WorkerPool bare(1, rune::metadata::ShardRouter(1), 1 << 22, 1 << 25, 64, 64);
    EXPECT_EQ(produceOn(bare, 0, GatewayViewReadRig::kViewTopic),
              static_cast<int16_t>(pc::ErrorCode::NotLeader)) << "no plan yet";
}
