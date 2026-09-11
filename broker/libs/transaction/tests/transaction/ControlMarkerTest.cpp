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

#include <cstring>
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
    EXPECT_TRUE(ControlMarker::isControlBatch(batch.data(), 0, ControlMarker::ENCODED_SIZE));
}

TEST(ControlMarkerTest, BuildAbortIsControlBatch) {
    auto batch = ControlMarker::build(1, 0, ControlType::Abort, 0);
    EXPECT_TRUE(ControlMarker::isControlBatch(batch.data(), 0, ControlMarker::ENCODED_SIZE));
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
    EXPECT_FALSE(ControlMarker::isControlBatch(buf, 0, ControlMarker::ENCODED_SIZE));
}

TEST(ControlMarkerTest, WrongMagicNotControl) {
    auto batch = ControlMarker::build(1, 0, ControlType::Commit, 0);
    batch[16] = 1; // corrupt magic
    EXPECT_FALSE(ControlMarker::isControlBatch(batch.data(), 0, ControlMarker::ENCODED_SIZE));
}

TEST(ControlMarkerTest, ZeroBytesNotControl) {
    uint8_t buf[ControlMarker::ENCODED_SIZE] = {};
    EXPECT_FALSE(ControlMarker::isControlBatch(buf, 0, ControlMarker::ENCODED_SIZE));
}

// ===========================================================================
// extract — decode the marker payload
// ===========================================================================

TEST(ControlMarkerTest, ExtractCommitMarker) {
    auto batch = ControlMarker::build(/*pid=*/42, /*epoch=*/3,
                                       ControlType::Commit, /*coordEpoch=*/7);
    ControlMarkerPayload payload{};
    ASSERT_TRUE(ControlMarker::extract(batch.data(), 0, payload, ControlMarker::ENCODED_SIZE));
    EXPECT_EQ(payload.controlType,       ControlType::Commit);
    EXPECT_EQ(payload.coordinatorEpoch,  7);
    EXPECT_EQ(payload.producerId,        42);
    EXPECT_EQ(payload.producerEpoch,     3);
}

TEST(ControlMarkerTest, ExtractAbortMarker) {
    auto batch = ControlMarker::build(99, 1, ControlType::Abort, 12);
    ControlMarkerPayload payload{};
    ASSERT_TRUE(ControlMarker::extract(batch.data(), 0, payload, ControlMarker::ENCODED_SIZE));
    EXPECT_EQ(payload.controlType,      ControlType::Abort);
    EXPECT_EQ(payload.coordinatorEpoch, 12);
    EXPECT_EQ(payload.producerId,       99);
    EXPECT_EQ(payload.producerEpoch,    1);
}

TEST(ControlMarkerTest, ExtractFailsOnNonControlBatch) {
    uint8_t buf[ControlMarker::ENCODED_SIZE] = {};
    ControlMarkerPayload payload{};
    EXPECT_FALSE(ControlMarker::extract(buf, 0, payload, ControlMarker::ENCODED_SIZE));
}

TEST(ControlMarkerTest, CommitAndAbortMarkersDistinguishable) {
    auto commit = ControlMarker::build(1, 0, ControlType::Commit, 0);
    auto abort  = ControlMarker::build(1, 0, ControlType::Abort,  0);

    ControlMarkerPayload cp{}, ap{};
    ASSERT_TRUE(ControlMarker::extract(commit.data(), 0, cp, ControlMarker::ENCODED_SIZE));
    ASSERT_TRUE(ControlMarker::extract(abort.data(),  0, ap, ControlMarker::ENCODED_SIZE));

    EXPECT_EQ(cp.controlType, ControlType::Commit);
    EXPECT_EQ(ap.controlType, ControlType::Abort);
    EXPECT_NE(cp.controlType, ap.controlType);
}

TEST(ControlMarkerTest, ExtractWithNonZeroCoordinatorEpoch) {
    auto batch = ControlMarker::build(1, 0, ControlType::Commit, 999);
    ControlMarkerPayload p{};
    ASSERT_TRUE(ControlMarker::extract(batch.data(), 0, p, ControlMarker::ENCODED_SIZE));
    EXPECT_EQ(p.coordinatorEpoch, 999);
}

TEST(ControlMarkerTest, ExtractWithLargeProducerId) {
    auto batch = ControlMarker::build(INT64_MAX / 2, INT16_MAX, ControlType::Abort, 42);
    ControlMarkerPayload p{};
    ASSERT_TRUE(ControlMarker::extract(batch.data(), 0, p, ControlMarker::ENCODED_SIZE));
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
        ASSERT_TRUE(ControlMarker::extract(batches[0].data(), 0, p, ControlMarker::ENCODED_SIZE));
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
        ASSERT_TRUE(ControlMarker::extract(batch.data(), 0, pl, ControlMarker::ENCODED_SIZE));
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
        ControlMarker::extract(b.data(), 0, captured, ControlMarker::ENCODED_SIZE);
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
        EXPECT_TRUE(ControlMarker::extract(b.data(), 0, pl, ControlMarker::ENCODED_SIZE));
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
        ControlMarker::extract(b.data(), 0, pl, ControlMarker::ENCODED_SIZE);
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
        ASSERT_TRUE(ControlMarker::extract(batch.data(), 0, p, ControlMarker::ENCODED_SIZE));
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

    EXPECT_FALSE(ControlMarker::isControlBatch(normalBuf, 0, ControlMarker::ENCODED_SIZE));
    EXPECT_TRUE(ControlMarker::isControlBatch(control.data(), 0, ControlMarker::ENCODED_SIZE));
}

// ===========================================================================
// Multiple partitions: each partition gets exactly one marker
// ===========================================================================

// ===========================================================================
// S1 — CRC + bounds checks in isControlBatch / extract (adversarial buffers)
// ===========================================================================

TEST(ControlMarkerTest, TruncatedBufferRejectedByIsControlBatch) {
    auto batch = ControlMarker::build(1, 0, ControlType::Commit, 0);
    ASSERT_EQ(batch.size(), ControlMarker::ENCODED_SIZE);
    // Pass avail < ENCODED_SIZE — must return false (no over-read).
    EXPECT_FALSE(ControlMarker::isControlBatch(batch.data(), 0,
                                                ControlMarker::ENCODED_SIZE - 1));
}

TEST(ControlMarkerTest, ShortBufferWithRealLengthNotControlNoOverRead) {
    // LOW fix (avail now REQUIRED): a genuinely short allocation (smaller than
    // ENCODED_SIZE) whose real length is passed must be rejected WITHOUT reading
    // past its end.  Under ASAN this catches any over-read.  Sizing the buffer to
    // exactly the available bytes means an over-read would touch unowned heap.
    constexpr std::size_t kShort = ControlMarker::ENCODED_SIZE - 5;
    std::vector<uint8_t> buf(kShort, 0u);
    buf[16] = 2;  // magic — looks batch-like
    int16_t attrs = static_cast<int16_t>(ControlMarker::kAttrControl |
                                          ControlMarker::kAttrTransactional);
    std::memcpy(buf.data() + 21, &attrs, 2);
    // Real available length is kShort (< ENCODED_SIZE) → must be rejected.
    EXPECT_FALSE(ControlMarker::isControlBatch(buf.data(), 0, buf.size()))
        << "a buffer shorter than ENCODED_SIZE must not be treated as a control batch";
    ControlMarkerPayload pl{};
    EXPECT_FALSE(ControlMarker::extract(buf.data(), 0, pl, buf.size()))
        << "extract must reject a short buffer without over-reading";
}

TEST(ControlMarkerTest, ControlBitSetButBadCrcRejected) {
    // Craft a batch with magic=2 and CONTROL bit, but wrong CRC.
    uint8_t buf[ControlMarker::ENCODED_SIZE] = {};
    buf[16] = 2;
    int16_t attrs = static_cast<int16_t>(ControlMarker::kAttrControl |
                                          ControlMarker::kAttrTransactional);
    std::memcpy(buf + 21, &attrs, 2);
    // CRC field (bytes 17-20) is zero — incorrect.
    EXPECT_FALSE(ControlMarker::isControlBatch(buf, 0, ControlMarker::ENCODED_SIZE));
}

TEST(ControlMarkerTest, ExtractTruncatedBufferReturnsFalse) {
    auto batch = ControlMarker::build(1, 0, ControlType::Commit, 0);
    ControlMarkerPayload pl{};
    // Pass 10 bytes less than needed.
    EXPECT_FALSE(ControlMarker::extract(batch.data(), 0, pl,
                                         ControlMarker::ENCODED_SIZE - 10));
}

TEST(ControlMarkerTest, ExtractCrcFlippedReturnsFalse) {
    auto batch = ControlMarker::build(42, 2, ControlType::Abort, 5);
    batch[17] ^= 0xFFu;  // corrupt 1 byte of CRC
    ControlMarkerPayload pl{};
    EXPECT_FALSE(ControlMarker::extract(batch.data(), 0, pl, ControlMarker::ENCODED_SIZE));
}

TEST(ControlMarkerTest, DataBatchWithControlBitAndBadCrcNotExtracted) {
    // Adversarial: magic=2, CONTROL bit set, but fabricated CRC → must fail.
    uint8_t buf[ControlMarker::ENCODED_SIZE] = {};
    buf[16] = 2;
    int16_t attrs = static_cast<int16_t>(ControlMarker::kAttrControl);
    std::memcpy(buf + 21, &attrs, 2);
    // CRC is zero: wrong.
    ControlMarkerPayload pl{};
    EXPECT_FALSE(ControlMarker::extract(buf, 0, pl, ControlMarker::ENCODED_SIZE));
}

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

// ===========================================================================
// Part B (control batch / marker format) — additional edge-case coverage
// ===========================================================================
namespace {

// Test-local CRC32C (Castagnoli) so we can re-stamp a batch after mutating a
// field, exercising extract() branches PAST the CRC gate.
static uint32_t testCrc32c(const uint8_t* data, std::size_t len) noexcept {
    static uint32_t tbl[256];
    static bool init = false;
    if (!init) {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int j = 0; j < 8; ++j) c = (c >> 1) ^ (0x82F63B78u & -(c & 1u));
            tbl[i] = c;
        }
        init = true;
    }
    uint32_t crc = 0xFFFFFFFFu;
    for (std::size_t i = 0; i < len; ++i)
        crc = (crc >> 8) ^ tbl[(crc ^ data[i]) & 0xFFu];
    return crc ^ 0xFFFFFFFFu;
}

// Re-stamp the batch CRC at [17..20] over bytes [21..ENCODED_SIZE).
static void restampCrc(std::vector<uint8_t>& b) {
    uint32_t c = testCrc32c(b.data() + ControlMarker::CRC_DATA_START,
                            ControlMarker::ENCODED_SIZE - ControlMarker::CRC_DATA_START);
    std::memcpy(b.data() + ControlMarker::CRC_OFFSET, &c, 4);
}

// Offsets inside the record (record starts at BATCH_HEADER_SIZE = 61):
//   [61] attrs, [62] tsDelta, [63] offDelta, [64] keyLen varint,
//   [65..66] key(int16), [67] valLen varint, [68..69] version,
//   [70..73] coordinatorEpoch, [74] hdrCount varint.
constexpr std::size_t kKeyLenOff = 64;
constexpr std::size_t kValLenOff = 67;

} // anonymous namespace

// #17 — extract with keyLength varint != 2 → clean false (the keyLen==2 else
// branch). We set keyLen's zig-zag varint to encode 0 (0x00) and re-stamp CRC so
// the CRC gate passes and we actually reach the keyLen check.
TEST(ControlMarkerTest, ExtractKeyLenNotTwoFails) {
    auto b = ControlMarker::build(1, 0, ControlType::Commit, 0);
    b[kKeyLenOff] = 0x00;   // zig-zag(0) = 0 → keyLen 0, != 2
    restampCrc(b);
    ASSERT_TRUE(ControlMarker::isControlBatch(b.data(), 0, ControlMarker::ENCODED_SIZE));
    ControlMarkerPayload p;
    EXPECT_FALSE(ControlMarker::extract(b.data(), 0, p, ControlMarker::ENCODED_SIZE));
}

// keyLen encoded as a MAX-size (10-byte) varint continuation → exercises the
// read_varint_zigzag_safe 10-byte cap. All-0x80 bytes never terminate → false.
TEST(ControlMarkerTest, ExtractKeyLenMaxSizeVarintFails) {
    auto b = ControlMarker::build(1, 0, ControlType::Commit, 0);
    // Fill from keyLen offset onward with continuation bytes (0x80) so the varint
    // never terminates within its 10-byte budget.
    for (std::size_t i = kKeyLenOff; i < ControlMarker::ENCODED_SIZE; ++i) b[i] = 0x80;
    restampCrc(b);
    ControlMarkerPayload p;
    EXPECT_FALSE(ControlMarker::extract(b.data(), 0, p, ControlMarker::ENCODED_SIZE));
}

// #18 — extract with valueLength varint != 6 → clean false.
TEST(ControlMarkerTest, ExtractValLenNotSixFails) {
    auto b = ControlMarker::build(1, 0, ControlType::Commit, 0);
    b[kValLenOff] = 0x08;   // zig-zag(4) = 8 → valLen 4, != 6
    restampCrc(b);
    ASSERT_TRUE(ControlMarker::isControlBatch(b.data(), 0, ControlMarker::ENCODED_SIZE));
    ControlMarkerPayload p;
    EXPECT_FALSE(ControlMarker::extract(b.data(), 0, p, ControlMarker::ENCODED_SIZE));
}

// #19 — extract truncated at EACH field boundary in the record. Every avail in
// [BATCH_HEADER_SIZE .. ENCODED_SIZE) must fail cleanly (no over-read).
TEST(ControlMarkerTest, ExtractTruncatedPerFieldFails) {
    auto b = ControlMarker::build(1, 0, ControlType::Commit, 5);
    for (std::size_t avail = 0; avail < ControlMarker::ENCODED_SIZE; ++avail) {
        ControlMarkerPayload p;
        EXPECT_FALSE(ControlMarker::extract(b.data(), 0, p, avail)) << "avail=" << avail;
    }
    // Full length succeeds.
    ControlMarkerPayload ok;
    EXPECT_TRUE(ControlMarker::extract(b.data(), 0, ok, ControlMarker::ENCODED_SIZE));
}

// #20 — isControlBatch / extract at a NON-ZERO offset into a larger buffer.
TEST(ControlMarkerTest, ExtractAtNonZeroOffset) {
    constexpr std::size_t kPad = 37;
    auto batch = ControlMarker::build(9, 3, ControlType::Abort, 11);
    std::vector<uint8_t> buf(kPad + ControlMarker::ENCODED_SIZE + kPad, 0xAB);
    std::memcpy(buf.data() + kPad, batch.data(), batch.size());

    EXPECT_TRUE(ControlMarker::isControlBatch(buf.data(), kPad, ControlMarker::ENCODED_SIZE));
    ControlMarkerPayload p;
    ASSERT_TRUE(ControlMarker::extract(buf.data(), kPad, p, ControlMarker::ENCODED_SIZE));
    EXPECT_EQ(p.producerId, 9);
    EXPECT_EQ(p.producerEpoch, 3);
    EXPECT_EQ(p.controlType, ControlType::Abort);
    EXPECT_EQ(p.coordinatorEpoch, 11);
}

// ===========================================================================
// Round-2 Residual-6: ControlMarker CRC region off-by-one boundary.
//
// (a) avail = ENCODED_SIZE-1 (1-byte short of full) → isControlBatch rejects,
//     extract rejects — no over-read.
// (b) A valid batch with its last CRC-covered byte (index ENCODED_SIZE-CRC_DATA_START-1
//     within the covered region, i.e. byte ENCODED_SIZE-1 overall) flipped → CRC
//     mismatch → both functions cleanly reject.
// (c) avail = ENCODED_SIZE+1 (1-byte longer buffer) with valid content at
//     [0..ENCODED_SIZE-1] and garbage at ENCODED_SIZE → succeeds (extra byte
//     beyond the fixed ENCODED_SIZE region is not read).
// ===========================================================================
TEST(ControlMarkerTest, CrcBoundaryOffByOneHandledCorrectly) {
    auto good = ControlMarker::build(3, 1, ControlType::Commit, 7);
    ASSERT_EQ(good.size(), ControlMarker::ENCODED_SIZE);

    // (a) One byte short: isControlBatch must reject (< ENCODED_SIZE), extract too.
    {
        ControlMarkerPayload p;
        EXPECT_FALSE(ControlMarker::isControlBatch(
            good.data(), 0, ControlMarker::ENCODED_SIZE - 1))
            << "(a) avail=ENCODED_SIZE-1 must be rejected";
        EXPECT_FALSE(ControlMarker::extract(
            good.data(), 0, p, ControlMarker::ENCODED_SIZE - 1))
            << "(a) extract avail=ENCODED_SIZE-1 must be rejected";
    }

    // (b) Flip the very last byte of the CRC-covered region (byte ENCODED_SIZE-1)
    //     and re-verify: CRC check must catch the single-bit flip and reject.
    {
        auto bad = good;
        bad[ControlMarker::ENCODED_SIZE - 1] ^= 0x01; // flip last covered byte
        // Do NOT re-stamp the CRC — the CRC is now stale (wrong).
        EXPECT_FALSE(ControlMarker::isControlBatch(
            bad.data(), 0, ControlMarker::ENCODED_SIZE))
            << "(b) last-byte flip must be caught by CRC check";
        ControlMarkerPayload p;
        EXPECT_FALSE(ControlMarker::extract(
            bad.data(), 0, p, ControlMarker::ENCODED_SIZE))
            << "(b) extract with last-byte CRC mismatch must return false";
    }

    // (c) avail = ENCODED_SIZE+1: the extra byte is beyond the fixed record;
    //     the valid content in [0..ENCODED_SIZE-1] must decode successfully.
    {
        std::vector<uint8_t> wider(ControlMarker::ENCODED_SIZE + 1);
        std::memcpy(wider.data(), good.data(), good.size());
        wider[ControlMarker::ENCODED_SIZE] = 0xFF; // garbage beyond record
        EXPECT_TRUE(ControlMarker::isControlBatch(
            wider.data(), 0, ControlMarker::ENCODED_SIZE + 1))
            << "(c) extra byte at the end must not affect a valid decode";
        ControlMarkerPayload p;
        ASSERT_TRUE(ControlMarker::extract(
            wider.data(), 0, p, ControlMarker::ENCODED_SIZE + 1));
        EXPECT_EQ(p.producerId,       3);
        EXPECT_EQ(p.producerEpoch,    1);
        EXPECT_EQ(p.controlType,      ControlType::Commit);
        EXPECT_EQ(p.coordinatorEpoch, 7);
    }
}

// #21 — build at extreme producerId / epoch values round-trips.
TEST(ControlMarkerTest, BuildExtremeProducerIdEpochRoundTrips) {
    for (ProducerId pid : {INT64_MIN, ProducerId{-1}, INT64_MAX}) {
        for (ProducerEpoch ep : {ProducerEpoch{INT16_MIN}, ProducerEpoch{-1},
                                 ProducerEpoch{INT16_MAX}}) {
            auto b = ControlMarker::build(pid, ep, ControlType::Commit, -7);
            ControlMarkerPayload p;
            ASSERT_TRUE(ControlMarker::extract(b.data(), 0, p, ControlMarker::ENCODED_SIZE))
                << "pid=" << pid << " ep=" << ep;
            EXPECT_EQ(p.producerId, pid);
            EXPECT_EQ(p.producerEpoch, ep);
            EXPECT_EQ(p.coordinatorEpoch, -7);
        }
    }
}
