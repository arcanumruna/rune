// NsProduceSizeLimitRegressionTest.cpp - max-message-bytes and max-batch-bytes at the owning WORKER's produce intake,
// driven deterministically (the test thread is the worker; HotPathBRig, no Aeron, no sleeps).  Semantics: 0 = unlimited;
// a batch longer than max-batch-bytes, or a record body longer than max-message-bytes, is refused NamespaceLimitExceeded (52)
// and nothing is appended.
//
// Interaction matrix (limit x {below, at, above} x shape):
//   | limit              | operation                | shape                                 | expected                       | test
//   |--------------------|--------------------------|---------------------------------------|--------------------------------|-----
//   | max-message-bytes  | leader produce, 1 record | body L-1 / L / L+1                    | ok / ok / 52, no append        | MessageBytesRegression
//   | max-message-bytes  | leader produce, N records| total > L, every record <= L          | ok (the walk refuses a record) | MessageBytesManySmallRecordsRegression
//   | max-message-bytes  | leader produce, N records| one record L+1 among small ones       | 52                             | MessageBytesOneLongRecordRegression
//   | max-batch-bytes    | leader produce           | batch L-1 / L / L+1                   | ok / ok / 52, no append        | BatchBytesRegression
//   | both 0             | leader produce           | large batch                           | ok (unlimited)                 | UnlimitedRegression
//   | both              | slot release + reassign  | no limits                             | ok                             | ReleaseThenReassignRegression
//   | every gate (#712) | leader produce, TRANSIENT| size x2 / throttle / DELETING / OverDisk | 52 / 53 / UnknownTopic / served | TransientPartitionEnforcedRegression
//   | request-ops (#712)| leader produce, TRANSIENT| one produce                          | counted once                   | TransientPartitionRequestOpsCountedRegression
#include "HotPathBRig.h"
#include "TransientRig.h"
#include "support/LimitEnforcementTable.h"

#include <rune/protocol/NamespaceLimits.h>
#include <rune/protocol/Varint.h>

namespace {

constexpr uint32_t kNs   = 1;
constexpr uint16_t kSlot = 1;

void assign(Rig& rig, uint64_t maxMessage, uint64_t maxBatch) {
    rig.log->setNsTag(kNs, kSlot);
    rune::protocol::NamespaceLimits l;
    l.maxMessageBytes = maxMessage;
    l.maxBatchBytes   = maxBatch;
    rig.worker().applyNsSlotAssignDirect(kSlot, kNs, l);
}

/// Record body length (the length varint of the first record).
uint64_t bodyLenOf(const std::vector<uint8_t>& batch) {
    size_t pos = rune::protocol::batch::kRecordBatchOverhead;
    return static_cast<uint64_t>(rune::protocol::readVarint(batch.data(), pos));
}

std::vector<uint8_t> batchOf(const std::vector<std::size_t>& valueSizes) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    int i = 0;
    for (std::size_t n : valueSizes) b.append("k", std::string(n, 'v'), 1000 + i++);
    return b.build();
}

/// A one-record batch whose record body is exactly `body` bytes.
std::vector<uint8_t> batchWithBody(uint64_t body) {
    for (std::size_t v = 0; v < body + 8; ++v) {
        auto b = batchOf({v});
        if (bodyLenOf(b) == body) return b;
    }
    ADD_FAILURE() << "no record of body " << body;
    return {};
}

/// A one-record batch whose total length is exactly `total` bytes.
std::vector<uint8_t> batchWithTotal(uint64_t total) {
    for (std::size_t v = 0; v < total; ++v) {
        auto b = batchOf({v});
        if (b.size() == total) return b;
        if (b.size() > total) break;
    }
    ADD_FAILURE() << "no batch of total " << total;
    return {};
}

std::vector<uint8_t> frameOf(const std::vector<uint8_t>& batch, int64_t corr) {
    pc::Produce msg;
    msg.sessionId = 1; msg.correlationId = corr; msg.topicId = 1; msg.partition = 0;
    msg.partitionKnown = 1; msg.acks = 1; msg.producerId = -1; msg.producerEpoch = 0; msg.baseSequence = -1;
    msg.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> frame(msg.encodedSize());
    msg.encode(frame.data());
    return frame;
}

/// Produce `batch`; the single answer's ErrorCode (-1: none).
int produce(Rig& rig, const std::vector<uint8_t>& batch) {
    static int64_t corr = 100;
    const auto frame = frameOf(batch, ++corr);
    rig.pass(&frame);
    int code = -1;
    rig.pool.drainAllReturns([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::ProduceResult) code = r.errorCode;
    });
    return code;
}

constexpr int kRefused = static_cast<int>(pc::ErrorCode::NamespaceLimitExceeded);

/// Below / At / Above of one limit through `make(size)`.
template <class Make>
void cells(Rig& rig, uint64_t limit, Make make, bool messageRow) {
    auto run = [&](uint64_t size, bool expectOk, const char* cell) {
        const int64_t endBefore = rig.log->logEndOffset();
        const uint64_t refusals = rig.worker().nsSizeRefusalsForTest();
        const auto batch = make(size);
        ASSERT_FALSE(batch.empty()) << cell;
        const int code = produce(rig, batch);
        if (expectOk) {
            EXPECT_EQ(code, 0) << cell;
            EXPECT_GT(rig.log->logEndOffset(), endBefore) << cell << ": appended";
            EXPECT_EQ(rig.worker().nsSizeRefusalsForTest(), refusals) << cell;
        } else {
            EXPECT_EQ(code, kRefused) << cell;
            EXPECT_EQ(rig.log->logEndOffset(), endBefore) << cell << ": nothing appended";
            EXPECT_EQ(rig.worker().nsSizeRefusalsForTest(), refusals + 1) << cell;
        }
    };
    (void)messageRow;
    run(limit - 1, true, "Below");
    run(limit, true, "At");
    run(limit + 1, false, "Above");
}

}  // namespace

TEST(NsProduceSizeLimitRegression, MessageBytesRegression) {
    Rig rig;
    assign(rig, 1024, 0);
    cells(rig, 1024, batchWithBody, true);
}

TEST(NsProduceSizeLimitRegression, BatchBytesRegression) {
    Rig rig;
    assign(rig, 0, 4096);
    cells(rig, 4096, batchWithTotal, false);
}

TEST(NsProduceSizeLimitRegression, MessageBytesManySmallRecordsRegression) {
    Rig rig;
    assign(rig, 1024, 0);
    const auto batch = batchOf({600, 600, 600, 600});   // every record < 1024, the batch is > 2400
    ASSERT_GT(batch.size(), 1024u);
    const int64_t end = rig.log->logEndOffset();
    EXPECT_EQ(produce(rig, batch), 0);
    EXPECT_GT(rig.log->logEndOffset(), end);
    EXPECT_EQ(rig.worker().nsSizeRefusalsForTest(), 0u) << "the walk refuses only on a long record";
}

TEST(NsProduceSizeLimitRegression, MessageBytesOneLongRecordRegression) {
    Rig rig;
    assign(rig, 1024, 0);
    const auto batch = batchOf({10, 10, 1100, 10});
    const int64_t end = rig.log->logEndOffset();
    EXPECT_EQ(produce(rig, batch), kRefused);
    EXPECT_EQ(rig.log->logEndOffset(), end);
    EXPECT_EQ(rig.worker().nsSizeRefusalsForTest(), 1u);
}

TEST(NsProduceSizeLimitRegression, UnlimitedRegression) {
    Rig rig;
    assign(rig, 0, 0);
    const auto batch = batchOf({60000, 60000});
    const int64_t end = rig.log->logEndOffset();
    EXPECT_EQ(produce(rig, batch), 0);
    EXPECT_GT(rig.log->logEndOffset(), end);
    EXPECT_EQ(rig.worker().nsSizeRefusalsForTest(), 0u);
}

TEST(NsProduceSizeLimitRegression, ReleaseThenReassignRegression) {
    Rig rig;
    assign(rig, 1024, 4096);
    const auto big = batchOf({5000});
    ASSERT_EQ(produce(rig, big), kRefused);
    ASSERT_TRUE(rig.worker().applyNsSlotReleaseDirect(kSlot, kNs));
    assign(rig, 0, 0);   // the slot is reused by the namespace without limits
    const int64_t end = rig.log->logEndOffset();
    EXPECT_EQ(produce(rig, big), 0) << "a released slot keeps no limit";
    EXPECT_GT(rig.log->logEndOffset(), end);
    EXPECT_EQ(rig.worker().nsSizeRefusalsForTest(), 1u);
}

namespace {
constexpr int kInvalid = static_cast<int>(pc::ErrorCode::InvalidRequest);

/// Recompute the length field (bytes after it) and the CRC-32C of a hand-edited batch.
void refix(std::vector<uint8_t>& b) {
    namespace pb = rune::protocol::batch;
    const uint32_t len = static_cast<uint32_t>(b.size() - 12);
    for (int i = 0; i < 4; ++i) b[pb::kLengthOffset + i] = static_cast<uint8_t>(len >> (8 * i));
    const uint32_t crc = rune::protocol::crc32c(b.data() + pb::kAttributesOffset, b.size() - pb::kAttributesOffset);
    for (int i = 0; i < 4; ++i) b[pb::kCrcOffset + i] = static_cast<uint8_t>(crc >> (8 * i));
}
}  // namespace

// The record length varint is a claim; consumers ignore it.  A record that says 10 but carries 2000 value bytes
// must not pass max-message-bytes=1024: the walk parses the fields exactly as forEachRecord does, and a record whose
// fields do not end at start+length is malformed (InvalidRequest), nothing appended.
TEST(NsProduceSizeLimitRegression, LyingRecordLengthAdversarialRegression) {
    Rig rig;
    assign(rig, 1024, 0);
    auto b = batchOf({2000});
    size_t pos = rune::protocol::batch::kRecordBatchOverhead;
    (void)rune::protocol::readVarint(b.data(), pos);
    ASSERT_EQ(pos, static_cast<size_t>(rune::protocol::batch::kRecordBatchOverhead) + 2) << "a 2-byte length varint";
    b[rune::protocol::batch::kRecordBatchOverhead]     = 0x94;   // zigzag(10) = 20, non-canonical 2-byte form
    b[rune::protocol::batch::kRecordBatchOverhead + 1] = 0x00;
    refix(b);
    const int64_t end = rig.log->logEndOffset();
    EXPECT_EQ(produce(rig, b), kInvalid);
    EXPECT_EQ(rig.log->logEndOffset(), end) << "nothing appended";
}

TEST(NsProduceSizeLimitRegression, RecordCountMismatchAdversarialRegression) {
    Rig rig;
    assign(rig, 1024, 0);
    auto b = batchOf({2000});
    b[rune::protocol::batch::kRecordCountOffset] = 2;   // header says 2, the body holds 1
    refix(b);
    const int64_t end = rig.log->logEndOffset();
    EXPECT_EQ(produce(rig, b), kInvalid);
    EXPECT_EQ(rig.log->logEndOffset(), end);
}

TEST(NsProduceSizeLimitRegression, TrailingBytesAdversarialRegression) {
    Rig rig;
    assign(rig, 1024, 0);
    auto b = batchOf({2000});
    for (int i = 0; i < 7; ++i) b.push_back(0x55);   // garbage after the last record
    refix(b);
    const int64_t end = rig.log->logEndOffset();
    EXPECT_EQ(produce(rig, b), kInvalid);
    EXPECT_EQ(rig.log->logEndOffset(), end);
}

// ---- #712: a transient partition (InMemoryLog, no disk Log) goes through the same namespace gates -------------------------

namespace {

using rune::node::test::TransientExpect;
using rune::node::test::TransientGate;

constexpr uint32_t kTNs   = 1;
constexpr uint16_t kTSlot = 1;

void tAssign(transient_rig::Rig& rig, uint64_t maxMessage, uint64_t maxBatch) {
    rig.mem.setNsTag(kTNs, kTSlot);
    rune::protocol::NamespaceLimits l;
    l.maxMessageBytes = maxMessage;
    l.maxBatchBytes   = maxBatch;
    rig.worker().applyNsSlotAssignDirect(kTSlot, kTNs, l);
}

}  // namespace

TEST(NsProduceSizeLimitRegression, TransientPartitionEnforcedRegression) {
    for (const auto& row : rune::node::test::kTransientRows) {
        SCOPED_TRACE(std::string(row.operation));
        transient_rig::Rig rig;
        const auto big = transient_rig::batchOf(1, 4096);   // one 4 KiB record
        switch (row.gate) {
            case TransientGate::MessageBytes:   tAssign(rig, 1024, 0); break;
            case TransientGate::BatchBytes:     tAssign(rig, 0, 2048); break;
            case TransientGate::Throttle:
                tAssign(rig, 0, 0);
                rig.worker().applyNsThrottleDirect(kTSlot, kTNs, rune::node::ThrottleKind::ProduceBytes, rig.now + 1'000'000'000);
                break;
            case TransientGate::Deleting:
                tAssign(rig, 0, 0);
                rig.worker().applyNsThrottleDirect(kTSlot, kTNs, rune::node::ThrottleKind::NsDeleting, 0);
                break;
            case TransientGate::OverDiskExempt:
                tAssign(rig, 0, 0);
                rig.worker().applyNsThrottleDirect(kTSlot, kTNs, rune::node::ThrottleKind::Disk, INT64_MAX);
                ASSERT_TRUE(rig.worker().nsOverDiskForTest(kTSlot)) << "precondition: the slot's OverDisk byte is set";
                break;
        }
        const int64_t endBefore = rig.mem.logEndOffset();
        const int16_t code = rig.produce(big, 1);
        switch (row.expect) {
            case TransientExpect::LimitExceeded52:
                EXPECT_EQ(code, static_cast<int16_t>(pc::ErrorCode::NamespaceLimitExceeded));
                EXPECT_EQ(rig.mem.logEndOffset(), endBefore) << "nothing appended";
                break;
            case TransientExpect::Throttled53:
                EXPECT_EQ(code, static_cast<int16_t>(pc::ErrorCode::Throttled));
                EXPECT_EQ(rig.mem.logEndOffset(), endBefore) << "nothing appended";
                break;
            case TransientExpect::UnknownTopic:
                EXPECT_EQ(code, static_cast<int16_t>(pc::ErrorCode::UnknownTopic));
                EXPECT_EQ(rig.mem.logEndOffset(), endBefore) << "nothing appended";
                break;
            case TransientExpect::Served:
                EXPECT_EQ(code, 0);
                EXPECT_GT(rig.mem.logEndOffset(), endBefore) << "appended";
                EXPECT_EQ(rig.worker().overDiskRefusalsForTest(), 0u) << "the OverDisk gate never counts a transient refusal";
                break;
        }
    }
}

TEST(NsProduceSizeLimitRegression, TransientPartitionRequestOpsCountedRegression) {
    transient_rig::Rig rig;
    tAssign(rig, 0, 0);
    ASSERT_EQ(rig.worker().nsCounterForTest(kTSlot, rune::node::NsMetric::RequestOps), 0u);
    EXPECT_EQ(rig.produce(transient_rig::batchOf(2, 100), 1), 0);
    EXPECT_EQ(rig.worker().nsCounterForTest(kTSlot, rune::node::NsMetric::RequestOps), 1u) << "one produce, one request op";
    EXPECT_GT(rig.worker().nsCounterForTest(kTSlot, rune::node::NsMetric::ProduceBytes), 0u) << "produce accounting reads the same tag";
    EXPECT_EQ(rig.worker().nsCounterForTest(kTSlot, rune::node::NsMetric::ProduceRecords), 2u);
    EXPECT_EQ(rig.worker().nsDiskBytesForTest(kTSlot), 0) << "a transient partition is never charged disk bytes";
}

// #712 siblings: fetch pacing and fetch-bytes accounting read the same tag, so a transient source is paced and counted.
TEST(NsProduceSizeLimitRegression, TransientPartitionFetchPacedAndCountedRegression) {
    transient_rig::Rig rig;
    tAssign(rig, 0, 0);
    ASSERT_EQ(rig.produce(transient_rig::batchOf(2, 200), 1), 0);
    const uint64_t opsBefore = rig.worker().nsCounterForTest(kTSlot, rune::node::NsMetric::RequestOps);
    const auto served = rig.readOnce(/*subId=*/5, /*from=*/0);
    ASSERT_TRUE(served.has_value());
    EXPECT_GT(rig.worker().nsCounterForTest(kTSlot, rune::node::NsMetric::FetchBytes), 0u) << "the delivered bytes are charged to the slot";
    EXPECT_GT(rig.worker().nsCounterForTest(kTSlot, rune::node::NsMetric::RequestOps), opsBefore) << "a read is a request op";
    // Over its fetch-bytes rate: the read is UNANSWERED (push bound -1) and counted as throttled; nothing is delivered.
    const uint64_t fetchBytes = rig.worker().nsCounterForTest(kTSlot, rune::node::NsMetric::FetchBytes);
    rig.worker().applyNsThrottleDirect(kTSlot, kTNs, rune::node::ThrottleKind::FetchBytes, rig.now + 1'000'000'000);
    const auto paced = rig.readOnce(/*subId=*/6, /*from=*/0);
    ASSERT_TRUE(paced.has_value());
    EXPECT_EQ(paced->pushEffectiveBound(), -1) << "an unanswered read";
    EXPECT_EQ(rig.worker().throttledRefusalsForTest(), 1u);
    EXPECT_EQ(rig.worker().nsCounterForTest(kTSlot, rune::node::NsMetric::FetchBytes), fetchBytes) << "nothing was read for a paced namespace";
}

// #712 slot-state cells: the (slot, nsId) tag check covers a transient partition too.
TEST(NsProduceSizeLimitRegression, TransientPartitionSlotStateRegression) {
    transient_rig::Rig rig;
    rig.mem.setNsTag(kTNs, kTSlot);   // the slot was never assigned on this worker: retriable NotLeader
    const auto batch = transient_rig::batchOf(1, 100);
    EXPECT_EQ(rig.produce(batch, 1), static_cast<int16_t>(pc::ErrorCode::NotLeader));
    tAssign(rig, 0, 0);               // assigned: served
    EXPECT_EQ(rig.produce(batch, 1), 0);
    ASSERT_TRUE(rig.worker().applyNsSlotReleaseDirect(kTSlot, kTNs));
    rig.worker().applyNsSlotAssignDirect(kTSlot, kTNs + 1, rune::protocol::NamespaceLimits{});   // reused by another namespace
    EXPECT_EQ(rig.produce(batch, 1), static_cast<int16_t>(pc::ErrorCode::InvalidRequest)) << "a stale tag is never charged to the new owner";
    EXPECT_EQ(rig.worker().nsCounterForTest(kTSlot, rune::node::NsMetric::ProduceBytes), 0u);
}
