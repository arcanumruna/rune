// ---------------------------------------------------------------------------
// ControlMarkerTest.cpp — Unit tests for ControlMarker (P5-3).
//
// TDD: build COMMIT/ABORT marker batch; identify control batches; extract
// payload; marker-per-partition on EndTxn; coordinator integration.
// ---------------------------------------------------------------------------

#include <rune/transaction/ControlMarker.h>
#include <rune/transaction/TransactionCoordinator.h>
#include <rune/transaction/TransactionMessages.h>

#include <gtest/gtest.h>

#include <map>
#include <vector>

using namespace rune::transaction;

namespace {

static TopicPartition tp(std::string t, int32_t p) {
    return TopicPartition{std::move(t), p};
}

static std::vector<TopicPartition> tps(std::initializer_list<TopicPartition> l) {
    return std::vector<TopicPartition>(l);
}

static constexpr int64_t kNow = 1'000'000'000LL;

} // anonymous namespace

// ===========================================================================
// Basic build tests
// ===========================================================================

TEST(ControlMarkerTest, BuildReturnsCorrectSize) {
    auto batch = ControlMarker::build(1, 0, ControlType::Commit, 0);
    EXPECT_EQ(batch.size(), ControlMarker::ENCODED_SIZE);
}

TEST(ControlMarkerTest, BuildCommitIsControlBatch) {
    auto batch = ControlMarker::build(1, 0, ControlType::Commit, 0);
    EXPECT_TRUE(ControlMarker::isControlBatch(batch.data()));
}

TEST(ControlMarkerTest, BuildAbortIsControlBatch) {
    auto batch = ControlMarker::build(1, 0, ControlType::Abort, 0);
    EXPECT_TRUE(ControlMarker::isControlBatch(batch.data()));
}

TEST(ControlMarkerTest, BuildMagicByte) {
    auto batch = ControlMarker::build(5, 2, ControlType::Commit, 0);
    // magic byte is at offset 16 in the batch
    EXPECT_EQ(batch[16], static_cast<uint8_t>(ControlMarker::kMagicByte));
}

TEST(ControlMarkerTest, BuildControlBitSet) {
    auto batch = ControlMarker::build(1, 0, ControlType::Commit, 0);
    // attributes at [21..22]: bit 5 (0x20) must be set
    int16_t attrs;
    std::memcpy(&attrs, batch.data() + 21, 2);
    EXPECT_NE(attrs & ControlMarker::kAttrControl, 0);
}

TEST(ControlMarkerTest, BuildTransactionalBitSet) {
    auto batch = ControlMarker::build(1, 0, ControlType::Commit, 0);
    int16_t attrs;
    std::memcpy(&attrs, batch.data() + 21, 2);
    EXPECT_NE(attrs & ControlMarker::kAttrTransactional, 0);
}

// ===========================================================================
// isControlBatch — distinguish from normal data batch
// ===========================================================================

TEST(ControlMarkerTest, NormalBatchNotControl) {
    // A "normal" batch has magic=2 but no CONTROL bit.
    uint8_t buf[ControlMarker::ENCODED_SIZE] = {};
    buf[16] = 2; // magic
    int16_t attrs = 0x0010; // transactional but NOT control
    std::memcpy(buf + 21, &attrs, 2);
    EXPECT_FALSE(ControlMarker::isControlBatch(buf));
}

TEST(ControlMarkerTest, WrongMagicNotControl) {
    auto batch = ControlMarker::build(1, 0, ControlType::Commit, 0);
    batch[16] = 1; // corrupt magic
    EXPECT_FALSE(ControlMarker::isControlBatch(batch.data()));
}

TEST(ControlMarkerTest, ZeroBytesNotControl) {
    uint8_t buf[ControlMarker::ENCODED_SIZE] = {};
    EXPECT_FALSE(ControlMarker::isControlBatch(buf));
}

// ===========================================================================
// extract — decode the marker payload
// ===========================================================================

TEST(ControlMarkerTest, ExtractCommitMarker) {
    auto batch = ControlMarker::build(/*pid=*/42, /*epoch=*/3,
                                       ControlType::Commit, /*coordEpoch=*/7);
    ControlMarkerPayload payload{};
    ASSERT_TRUE(ControlMarker::extract(batch.data(), 0, payload));
    EXPECT_EQ(payload.controlType,       ControlType::Commit);
    EXPECT_EQ(payload.coordinatorEpoch,  7);
    EXPECT_EQ(payload.producerId,        42);
    EXPECT_EQ(payload.producerEpoch,     3);
}

TEST(ControlMarkerTest, ExtractAbortMarker) {
    auto batch = ControlMarker::build(99, 1, ControlType::Abort, 12);
    ControlMarkerPayload payload{};
    ASSERT_TRUE(ControlMarker::extract(batch.data(), 0, payload));
    EXPECT_EQ(payload.controlType,      ControlType::Abort);
    EXPECT_EQ(payload.coordinatorEpoch, 12);
    EXPECT_EQ(payload.producerId,       99);
    EXPECT_EQ(payload.producerEpoch,    1);
}

TEST(ControlMarkerTest, ExtractFailsOnNonControlBatch) {
    uint8_t buf[ControlMarker::ENCODED_SIZE] = {};
    ControlMarkerPayload payload{};
    EXPECT_FALSE(ControlMarker::extract(buf, 0, payload));
}

TEST(ControlMarkerTest, CommitAndAbortMarkersDistinguishable) {
    auto commit = ControlMarker::build(1, 0, ControlType::Commit, 0);
    auto abort  = ControlMarker::build(1, 0, ControlType::Abort,  0);

    ControlMarkerPayload cp{}, ap{};
    ASSERT_TRUE(ControlMarker::extract(commit.data(), 0, cp));
    ASSERT_TRUE(ControlMarker::extract(abort.data(),  0, ap));

    EXPECT_EQ(cp.controlType, ControlType::Commit);
    EXPECT_EQ(ap.controlType, ControlType::Abort);
    EXPECT_NE(cp.controlType, ap.controlType);
}

TEST(ControlMarkerTest, ExtractWithNonZeroCoordinatorEpoch) {
    auto batch = ControlMarker::build(1, 0, ControlType::Commit, 999);
    ControlMarkerPayload p{};
    ASSERT_TRUE(ControlMarker::extract(batch.data(), 0, p));
    EXPECT_EQ(p.coordinatorEpoch, 999);
}

TEST(ControlMarkerTest, ExtractWithLargeProducerId) {
    auto batch = ControlMarker::build(INT64_MAX / 2, INT16_MAX, ControlType::Abort, 42);
    ControlMarkerPayload p{};
    ASSERT_TRUE(ControlMarker::extract(batch.data(), 0, p));
    EXPECT_EQ(p.producerId,       INT64_MAX / 2);
    EXPECT_EQ(p.producerEpoch,    INT16_MAX);
    EXPECT_EQ(p.controlType,      ControlType::Abort);
    EXPECT_EQ(p.coordinatorEpoch, 42);
}

// ===========================================================================
// writeMarkers — one marker per partition on EndTxn
// ===========================================================================

TEST(ControlMarkerTest, WriteMarkersCommitThreePartitions) {
    ProducerEpoch epoch{0};
    std::string txnId = "txn-3p";
    TransactionCoordinator coord;
    auto ir = coord.initProducerId(txnId, 60'000, kNow);
    ASSERT_TRUE(ir.ok());
    epoch = ir.producerEpoch;
    coord.addPartitions(txnId, ir.producerId, ir.producerEpoch,
                         tps({tp("a", 0), tp("b", 1), tp("c", 2)}), kNow);

    ProducerId pid = coord.pidOf(txnId);
    ASSERT_NE(pid, -1);

    // Capture markers written.
    std::map<TopicPartition, std::vector<std::vector<uint8_t>>> captured;
    auto appendFn = [&](const TopicPartition& p, const std::vector<uint8_t>& batch) {
        captured[p].push_back(batch);
    };

    int count = ControlMarker::writeMarkers(coord, txnId, pid, epoch,
                                             /*commit=*/true, /*coordEpoch=*/0,
                                             kNow, appendFn);

    EXPECT_EQ(count, 3);
    EXPECT_EQ(captured.size(), 3u);

    // Each marker must be COMMIT.
    for (auto& [partition, batches] : captured) {
        ASSERT_EQ(batches.size(), 1u);
        ControlMarkerPayload p{};
        ASSERT_TRUE(ControlMarker::extract(batches[0].data(), 0, p));
        EXPECT_EQ(p.controlType, ControlType::Commit);
    }

    // Coordinator must be in Empty state after completeTransaction.
    EXPECT_EQ(coord.state(txnId), TxnState::Empty);
}

TEST(ControlMarkerTest, WriteMarkersAbortThreePartitions) {
    ProducerEpoch epoch{0};
    std::string txnId = "txn-abort3";
    TransactionCoordinator coord;
    auto ir = coord.initProducerId(txnId, 60'000, kNow);
    ASSERT_TRUE(ir.ok());
    epoch = ir.producerEpoch;
    coord.addPartitions(txnId, ir.producerId, ir.producerEpoch,
                         tps({tp("x", 0), tp("y", 1), tp("z", 2)}), kNow);
    ProducerId pid = coord.pidOf(txnId);

    std::vector<std::pair<TopicPartition, std::vector<uint8_t>>> captured;
    auto appendFn = [&](const TopicPartition& p, const std::vector<uint8_t>& batch) {
        captured.emplace_back(p, batch);
    };

    int count = ControlMarker::writeMarkers(coord, txnId, pid, epoch,
                                             /*commit=*/false, 0, kNow, appendFn);

    EXPECT_EQ(count, 3);
    for (auto& [partition, batch] : captured) {
        ControlMarkerPayload pl{};
        ASSERT_TRUE(ControlMarker::extract(batch.data(), 0, pl));
        EXPECT_EQ(pl.controlType, ControlType::Abort);
    }

    EXPECT_EQ(coord.state(txnId), TxnState::Empty);
}

TEST(ControlMarkerTest, WriteMarkersOnePartition) {
    ProducerEpoch epoch{0};
    std::string txnId = "txn-1p";
    TransactionCoordinator coord;
    auto ir = coord.initProducerId(txnId, 60'000, kNow);
    ASSERT_TRUE(ir.ok());
    epoch = ir.producerEpoch;
    coord.addPartitions(txnId, ir.producerId, ir.producerEpoch,
                         tps({tp("sole", 0)}), kNow);
    ProducerId pid = coord.pidOf(txnId);

    int captureCount = 0;
    auto appendFn = [&](const TopicPartition&, const std::vector<uint8_t>&) {
        ++captureCount;
    };

    int count = ControlMarker::writeMarkers(coord, txnId, pid, epoch,
                                             true, 0, kNow, appendFn);
    EXPECT_EQ(count, 1);
    EXPECT_EQ(captureCount, 1);
    EXPECT_EQ(coord.state(txnId), TxnState::Empty);
}

TEST(ControlMarkerTest, WriteMarkersZeroPartitions) {
    // A txn with no partitions (Empty state) cannot be ended.
    TransactionCoordinator coord;
    auto ir = coord.initProducerId(std::string("txn-empty"), 60'000, kNow);
    // State is Empty, no partitions.

    int count = 0;
    auto appendFn = [&](const TopicPartition&, const std::vector<uint8_t>&) { ++count; };

    // endTransaction on Empty state → InvalidTransition → writeMarkers returns 0.
    int written = ControlMarker::writeMarkers(coord, "txn-empty", ir.producerId,
                                               ir.producerEpoch, true, 0, kNow, appendFn);
    EXPECT_EQ(written, 0);
    EXPECT_EQ(count, 0);
}

TEST(ControlMarkerTest, WriteMarkersCorrectPartitionsReceiveMarkers) {
    ProducerEpoch epoch{0};
    std::string txnId = "txn-specific";
    TransactionCoordinator coord;
    auto ir = coord.initProducerId(txnId, 60'000, kNow);
    ASSERT_TRUE(ir.ok());
    epoch = ir.producerEpoch;
    coord.addPartitions(txnId, ir.producerId, ir.producerEpoch,
                         tps({tp("foo", 3), tp("bar", 7), tp("baz", 0)}), kNow);
    ProducerId pid = coord.pidOf(txnId);

    std::set<TopicPartition> seen;
    auto appendFn = [&](const TopicPartition& p, const std::vector<uint8_t>&) {
        seen.insert(p);
    };

    int count = ControlMarker::writeMarkers(coord, txnId, pid, epoch,
                                             true, 0, kNow, appendFn);
    EXPECT_EQ(count, 3);
    EXPECT_TRUE(seen.contains(tp("foo", 3)));
    EXPECT_TRUE(seen.contains(tp("bar", 7)));
    EXPECT_TRUE(seen.contains(tp("baz", 0)));
}

TEST(ControlMarkerTest, WriteMarkersCoordEpochPropagated) {
    ProducerEpoch epoch{0};
    std::string txnId = "txn-coord-epoch";
    TransactionCoordinator coord;
    auto ir = coord.initProducerId(txnId, 60'000, kNow);
    ASSERT_TRUE(ir.ok());
    epoch = ir.producerEpoch;
    coord.addPartitions(txnId, ir.producerId, ir.producerEpoch, tps({tp("p", 0)}), kNow);
    ProducerId pid = coord.pidOf(txnId);

    ControlMarkerPayload captured{};
    auto appendFn = [&](const TopicPartition&, const std::vector<uint8_t>& b) {
        ControlMarker::extract(b.data(), 0, captured);
    };

    ControlMarker::writeMarkers(coord, txnId, pid, epoch, true, /*coordEpoch=*/42, kNow, appendFn);
    EXPECT_EQ(captured.coordinatorEpoch, 42);
}

// ===========================================================================
// Integration with TransactionCoordinator
// ===========================================================================

TEST(ControlMarkerTest, IntegrationCommitPath) {
    TransactionCoordinator coord;
    auto ir = coord.initProducerId(std::string("integ-commit"), 60'000, kNow);
    ASSERT_TRUE(ir.ok());

    auto ar = coord.addPartitions("integ-commit", ir.producerId, ir.producerEpoch,
                                   tps({tp("p0", 0), tp("p1", 1), tp("p2", 2)}), kNow);
    ASSERT_TRUE(ar.ok());
    EXPECT_EQ(coord.state("integ-commit"), TxnState::Ongoing);

    std::vector<std::pair<TopicPartition, ControlMarkerPayload>> captured;
    auto appendFn = [&](const TopicPartition& p, const std::vector<uint8_t>& b) {
        ControlMarkerPayload pl{};
        EXPECT_TRUE(ControlMarker::extract(b.data(), 0, pl));
        captured.emplace_back(p, pl);
    };

    int count = ControlMarker::writeMarkers(coord, "integ-commit",
                                             ir.producerId, ir.producerEpoch,
                                             /*commit=*/true, 0, kNow, appendFn);
    EXPECT_EQ(count, 3);
    EXPECT_EQ(captured.size(), 3u);
    for (auto& [p, pl] : captured) {
        EXPECT_EQ(pl.controlType, ControlType::Commit);
    }
    EXPECT_EQ(coord.state("integ-commit"), TxnState::Empty);
}

TEST(ControlMarkerTest, IntegrationAbortPath) {
    TransactionCoordinator coord;
    auto ir = coord.initProducerId(std::string("integ-abort"), 60'000, kNow);
    ASSERT_TRUE(ir.ok());

    coord.addPartitions("integ-abort", ir.producerId, ir.producerEpoch,
                         tps({tp("p0", 0), tp("p1", 1), tp("p2", 2)}), kNow);

    std::vector<ControlMarkerPayload> captured;
    auto appendFn = [&](const TopicPartition&, const std::vector<uint8_t>& b) {
        ControlMarkerPayload pl{};
        ControlMarker::extract(b.data(), 0, pl);
        captured.push_back(pl);
    };

    int count = ControlMarker::writeMarkers(coord, "integ-abort",
                                             ir.producerId, ir.producerEpoch,
                                             /*commit=*/false, 0, kNow, appendFn);
    EXPECT_EQ(count, 3);
    for (auto& pl : captured) {
        EXPECT_EQ(pl.controlType, ControlType::Abort);
    }
    EXPECT_EQ(coord.state("integ-abort"), TxnState::Empty);
}

TEST(ControlMarkerTest, IntegrationStateAfterCommit) {
    TransactionCoordinator coord;
    auto ir = coord.initProducerId(std::string("state-after"), 60'000, kNow);
    coord.addPartitions("state-after", ir.producerId, ir.producerEpoch,
                         tps({tp("q", 0)}), kNow);

    auto appendFn = [](const TopicPartition&, const std::vector<uint8_t>&) {};
    ControlMarker::writeMarkers(coord, "state-after", ir.producerId, ir.producerEpoch,
                                 true, 0, kNow, appendFn);

    // After completeTransaction, state is Empty and another txn can start.
    EXPECT_EQ(coord.state("state-after"), TxnState::Empty);
    auto ar = coord.addPartitions("state-after", ir.producerId, ir.producerEpoch,
                                   tps({tp("q", 0)}), kNow + 1);
    EXPECT_TRUE(ar.ok());
}

TEST(ControlMarkerTest, IntegrationStateAfterAbort) {
    TransactionCoordinator coord;
    auto ir = coord.initProducerId(std::string("state-abort"), 60'000, kNow);
    coord.addPartitions("state-abort", ir.producerId, ir.producerEpoch,
                         tps({tp("r", 0)}), kNow);

    auto appendFn = [](const TopicPartition&, const std::vector<uint8_t>&) {};
    ControlMarker::writeMarkers(coord, "state-abort", ir.producerId, ir.producerEpoch,
                                 false, 0, kNow, appendFn);

    EXPECT_EQ(coord.state("state-abort"), TxnState::Empty);
}

// ===========================================================================
// Verify batch is byte-exact (build → extract round-trip)
// ===========================================================================

TEST(ControlMarkerTest, RoundTripByteExact) {
    for (auto ct : {ControlType::Commit, ControlType::Abort}) {
        auto batch = ControlMarker::build(/*pid=*/12345678LL, /*epoch=*/9,
                                           ct, /*coordEpoch=*/3);
        ControlMarkerPayload p{};
        ASSERT_TRUE(ControlMarker::extract(batch.data(), 0, p));
        EXPECT_EQ(p.producerId,        12345678LL);
        EXPECT_EQ(p.producerEpoch,     9);
        EXPECT_EQ(p.controlType,       ct);
        EXPECT_EQ(p.coordinatorEpoch,  3);
    }
}

TEST(ControlMarkerTest, ControlBatchNotMistakennForDataBatch) {
    // A control batch must be distinguishable from a normal data batch.
    uint8_t normalBuf[ControlMarker::ENCODED_SIZE] = {};
    normalBuf[16] = 2;  // magic=2
    int16_t normalAttrs = 0x0010; // transactional but not control
    std::memcpy(normalBuf + 21, &normalAttrs, 2);

    auto control = ControlMarker::build(1, 0, ControlType::Commit, 0);

    EXPECT_FALSE(ControlMarker::isControlBatch(normalBuf));
    EXPECT_TRUE(ControlMarker::isControlBatch(control.data()));
}

// ===========================================================================
// Multiple partitions: each partition gets exactly one marker
// ===========================================================================

TEST(ControlMarkerTest, ExactlyOneMarkerPerPartition) {
    std::string txnId = "txn-one-each";
    constexpr int N = 8;
    std::vector<TopicPartition> parts;
    for (int i = 0; i < N; ++i) parts.push_back(tp("t", i));

    TransactionCoordinator coord;
    auto ir = coord.initProducerId(txnId, 60'000, kNow);
    ASSERT_TRUE(ir.ok());
    coord.addPartitions(txnId, ir.producerId, ir.producerEpoch, parts, kNow);

    std::map<TopicPartition, int> markerCount;
    auto appendFn = [&](const TopicPartition& p, const std::vector<uint8_t>&) {
        markerCount[p]++;
    };

    ControlMarker::writeMarkers(coord, txnId, ir.producerId, ir.producerEpoch,
                                 true, 0, kNow, appendFn);

    EXPECT_EQ(markerCount.size(), static_cast<std::size_t>(N));
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(markerCount[tp("t", i)], 1) << "partition " << i;
    }
}
