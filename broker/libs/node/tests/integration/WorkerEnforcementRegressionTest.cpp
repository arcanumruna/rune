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

// X036. RED: delete the refusesClientBatches check in handleWorkerIngressFrame_ - the frame (control batch second) is appended.
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

// X036 (review I1): same chain rules at the worker's direct ingress.  RED: fail-open walk - the frames are appended.
TEST(ProduceValidationRegression, AMalformedBatchChainIsRefusedOnWorkerRegression) {
    Rig rig;
    rune::protocol::RecordBatchBuilder first(0, 1000);
    first.append("k", "v", 1000);
    const std::vector<uint8_t> a = first.build();
    rune::protocol::RecordBatchBuilder second(0, 1000);
    second.append("k2", "v2", 1000);
    const std::vector<uint8_t> b = second.build();
    auto patch = [&](std::vector<uint8_t>& chain, uint32_t total) {
        const uint32_t v = total - 12;
        for (int i = 0; i < 4; ++i) chain[a.size() + static_cast<std::size_t>(rune::protocol::batch::kLengthOffset) + i] = static_cast<uint8_t>(v >> (8 * i));
    };
    for (int variant = 1; variant <= 3; ++variant) {
        std::vector<uint8_t> chain = a;
        if (variant == 3) chain.insert(chain.end(), b.begin(), b.begin() + 10);
        else {
            chain.insert(chain.end(), b.begin(), b.end());
            patch(chain, variant == 1 ? static_cast<uint32_t>(b.size()) + 4096 : 30u);
        }
        pc::Produce msg;
        msg.sessionId = 1; msg.correlationId = 10 + variant; msg.topicId = 1; msg.partition = 0;
        msg.partitionKnown = 1; msg.acks = 1; msg.producerId = -1; msg.producerEpoch = 0; msg.baseSequence = -1;
        msg.recordBatch = std::span<const uint8_t>(chain);
        std::vector<uint8_t> frame(msg.encodedSize());
        frame.resize(msg.encode(frame.data()));
        const int64_t leo = rig.log->logEndOffset();
        rig.pass(&frame);
        EXPECT_EQ(answerOf(rig), static_cast<int16_t>(pc::ErrorCode::InvalidRequest)) << "variant " << variant;
        EXPECT_EQ(rig.log->logEndOffset(), leo) << "variant " << variant;
    }
}
