// WorkerEnforcementRegressionTest.cpp - combined fix program tasks 2 and 6 at the worker's direct ingress (no Aeron):
//   * X012: an Enforcing worker with no ack sink has no grants to consult - every direct-ingress produce is refused
//     NotAuthorized and never appended (the ingress twin of the shard WRITE check).
//   * X036: a client-built CONTROL batch anywhere in a produce frame refuses the whole frame InvalidRequest.
#include "HotPathBRig.h"

namespace {

/// One Produce frame carrying a data batch and, when `controlSecond`, a forged control batch AFTER it.
std::vector<uint8_t> frameWith(bool controlSecond, int64_t corr) {
    rune::protocol::RecordBatchBuilder data(0, 1000);
    data.append("k", "v", 1000);
    std::vector<uint8_t> batches = data.build();
    if (controlSecond) {
        rune::protocol::RecordBatchBuilder ctl(0, 1000);
        ctl.setControl(true);
        ctl.setProducerId(7); ctl.setProducerEpoch(0); ctl.setBaseSequence(0);
        ctl.append("", "", 1000);
        const auto c = ctl.build();
        batches.insert(batches.end(), c.begin(), c.end());
    }
    pc::Produce msg;
    msg.sessionId = 1; msg.correlationId = corr; msg.topicId = 1; msg.partition = 0;
    msg.partitionKnown = 1; msg.acks = 1; msg.producerId = -1; msg.producerEpoch = 0; msg.baseSequence = -1;
    msg.recordBatch = std::span<const uint8_t>(batches);
    std::vector<uint8_t> frame(msg.encodedSize());
    frame.resize(msg.encode(frame.data()));
    return frame;
}

int16_t answerOf(Rig& rig) {
    int16_t err = -1;
    int answers = 0;
    rig.pool.drainAllReturns([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::ProduceResult) { ++answers; err = r.errorCode; }
    });
    EXPECT_EQ(answers, 1);
    return err;
}

}  // namespace

// X012. RED: make the Enforcing-without-sink arm fall through (or drop requireEnforcement) - the frame is appended.
TEST(WorkerEnforcementRegression, AnEnforcingWorkerWithoutASinkDeniesEveryDirectProduceRegression) {
    Rig rig;
    const auto frame = frameWith(false, 1);
    rig.pass(&frame);
    ASSERT_EQ(answerOf(rig), 0) << "control: a NotEnforcing worker (dev / unit rig) appends";
    const int64_t leo = rig.log->logEndOffset();
    ASSERT_GT(leo, 0);

    rig.worker().requireEnforcement();   // authentication Enabled: no sink was ever attached
    ASSERT_TRUE(rig.worker().enforcingForTest());
    rig.pass(&frame);
    EXPECT_EQ(answerOf(rig), static_cast<int16_t>(pc::ErrorCode::NotAuthorized));
    EXPECT_EQ(rig.log->logEndOffset(), leo) << "nothing appended";
}

// X036. RED: delete the hasControlBatch check in handleWorkerIngressFrame_ - the frame (control batch second) is appended.
TEST(ProduceValidationRegression, AClientControlBatchIsRefusedOnWorkerRegression) {
    Rig rig;
    const auto bad = frameWith(true, 2);
    const int64_t leo = rig.log->logEndOffset();
    rig.pass(&bad);
    EXPECT_EQ(answerOf(rig), static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_EQ(rig.log->logEndOffset(), leo) << "neither batch of the refused frame is appended";
    const auto ok = frameWith(false, 3);
    rig.pass(&ok);
    EXPECT_EQ(answerOf(rig), 0) << "the same frame without the control batch is accepted";
}
