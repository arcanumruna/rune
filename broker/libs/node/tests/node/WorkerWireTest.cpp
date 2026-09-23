// ---------------------------------------------------------------------------
// WorkerWireTest.cpp — unit tests for WorkerWire flat codec (R1).
//
// Test matrix:
//   WorkerOp round-trips (encode→decode, scalar + tail):
//     1. Produce         — recordBatchBytes tail
//     2. PushRead        — no tail
//     3. ApplyReplicated — replicatedPayload.records tail
//     4. BecomeLeader    — isr (NodeId[]) tail
//     5. BecomeFollower  — no tail
//     6. SetOwnership    — ownedSet (uintptr_t[]) tail
//
//   WorkerReturn round-trips:
//     7. ProduceResult   — no tail
//     8. PushReadResult  — chunk tail
//
//   Ring integration:
//     9. ProduceRoundTripThroughRing — encode→write→drain→decode for a 4KB
//        batch, asserting all scalars + tail bytes are byte-identical.
// ---------------------------------------------------------------------------

#include <rune/node/WorkerWire.h>
#include <rune/node/OneToOneByteRing.h>

#include <gtest/gtest.h>
#include <vector>
#include <cstdint>
#include <cstring>
#include <memory>

using rune::node::WorkerOp;
using rune::node::WorkerReturn;
using rune::node::WorkerOpWire;
using rune::node::WorkerReturnWire;
using rune::node::TailKind;
using rune::node::encodeWorkerOp;
using rune::node::encodeWorkerReturn;
using rune::node::decodeWorkerOpHeader;
using rune::node::decodeWorkerReturnHeader;
using rune::node::workerOpTail;
using rune::node::workerReturnTail;
using rune::node::workerOpMsgTypeId;
using rune::node::workerReturnMsgTypeId;
using rune::node::OneToOneByteRing;

namespace {

// Scratch buffer for encode (256 KiB — sufficient for 4KB batch tests)
constexpr std::size_t kScratchCap = 256 * 1024;

std::vector<uint8_t> makeScratch() {
    return std::vector<uint8_t>(kScratchCap, 0);
}

// Build a byte vector with a recognisable repeating pattern.
std::vector<uint8_t> makeBytes(std::size_t n, uint8_t seed = 0x5A) {
    std::vector<uint8_t> v(n);
    for (std::size_t i = 0; i < n; ++i) {
        v[i] = static_cast<uint8_t>((seed ^ uint8_t(i & 0xFF)) + uint8_t(i >> 8));
    }
    return v;
}

// Sentinel pointer values (non-null, in-process safe for cast checks only).
static int       sDummyReplica = 42;
static int       sDummyLog     = 99;
inline rune::replication::PartitionReplica* dummyReplica() {
    return reinterpret_cast<rune::replication::PartitionReplica*>(&sDummyReplica);
}
inline rune::storage::Log* dummyLog() {
    return reinterpret_cast<rune::storage::Log*>(&sDummyLog);
}

// Fill a WorkerOp with representative common scalar values.
WorkerOp makeBaseOp(WorkerOp::Type type) {
    WorkerOp op{};
    op.type              = type;
    op.topicId           = 7;
    op.partition         = 3;
    op.replica           = dummyReplica();
    op.logPtr            = dummyLog();
    op.sessionId         = 0x1122334455667788LL;
    op.correlationId     = 0xAABBCCDDEEFF0011LL;
    op.acks              = 1;
    op.producerId        = 987654321LL;
    op.producerEpoch     = 7;
    op.baseSequence      = 42;
    op.lastOffsetDelta   = 9;
    op.recordCount       = 10;
    op.batchAttrs        = 0x0010;
    op.deferAck          = true;
    op.durabilityTier    = 2;
    op.enqueuedAtNs      = 1234567890123456789LL;
    op.subscriptionId    = 555;
    op.nextOffset        = 100;
    op.hwBound           = 200;
    op.isolation         = 1;
    op.isGroup           = true;
    op.groupCarrierReceived = false;
    op.roleEpoch         = 99;
    op.roleLeaderId      = 2;
    op.replicatedPayload.leaderEpoch    = 11;
    op.replicatedPayload.partition      = 3;
    op.replicatedPayload.highWatermark  = 500;
    op.replicatedPayload.logStartOffset = 50;
    op.replicatedPayload.errorCode      = 0;
    op.replicatedPayload.topicId        = 7;
    return op;
}

// Assert every scalar field in WorkerOpWire matches the original WorkerOp.
void assertOpScalars(const WorkerOpWire& w, const WorkerOp& op) {
    EXPECT_EQ(w.type,             static_cast<uint8_t>(op.type));
    EXPECT_EQ(w.topicId,          op.topicId);
    EXPECT_EQ(w.partition,        op.partition);
    EXPECT_EQ(w.replica,          reinterpret_cast<uintptr_t>(op.replica));
    EXPECT_EQ(w.logPtr,           reinterpret_cast<uintptr_t>(op.logPtr));
    EXPECT_EQ(w.sessionId,        op.sessionId);
    EXPECT_EQ(w.correlationId,    op.correlationId);
    EXPECT_EQ(w.acks,             op.acks);
    EXPECT_EQ(w.producerId,       op.producerId);
    EXPECT_EQ(w.producerEpoch,    op.producerEpoch);
    EXPECT_EQ(w.baseSequence,     op.baseSequence);
    EXPECT_EQ(w.lastOffsetDelta,  op.lastOffsetDelta);
    EXPECT_EQ(w.recordCount,      op.recordCount);
    EXPECT_EQ(w.batchAttrs,       op.batchAttrs);
    EXPECT_EQ(w.deferAck,         op.deferAck ? uint8_t{1} : uint8_t{0});
    EXPECT_EQ(w.durabilityTier,   op.durabilityTier);
    EXPECT_EQ(w.enqueuedAtNs,     op.enqueuedAtNs);
    EXPECT_EQ(w.subscriptionId,   op.subscriptionId);
    EXPECT_EQ(w.nextOffset,       op.nextOffset);
    EXPECT_EQ(w.hwBound,          op.hwBound);
    EXPECT_EQ(w.isolation,        op.isolation);
    EXPECT_EQ(w.isGroup,          op.isGroup ? uint8_t{1} : uint8_t{0});
    EXPECT_EQ(w.groupCarrierReceived, op.groupCarrierReceived ? uint8_t{1} : uint8_t{0});
    EXPECT_EQ(w.roleEpoch,        static_cast<int64_t>(op.roleEpoch));
    EXPECT_EQ(w.roleLeaderId,     static_cast<int32_t>(op.roleLeaderId));
    EXPECT_EQ(w.replLeaderEpoch,  op.replicatedPayload.leaderEpoch);
    EXPECT_EQ(w.replPartition,    op.replicatedPayload.partition);
    EXPECT_EQ(w.replHighWatermark,  op.replicatedPayload.highWatermark);
    EXPECT_EQ(w.replLogStartOffset, op.replicatedPayload.logStartOffset);
    EXPECT_EQ(w.replErrorCode,    op.replicatedPayload.errorCode);
    EXPECT_EQ(w.replTopicId,      op.replicatedPayload.topicId);
}

} // namespace

// ---------------------------------------------------------------------------
// 1. Produce — recordBatchBytes tail
// ---------------------------------------------------------------------------
TEST(WorkerWire, ProduceRoundTrip) {
    WorkerOp op = makeBaseOp(WorkerOp::Type::Produce);
    op.recordBatchBytes = makeBytes(512, 0xAB);

    auto scratch = makeScratch();
    std::size_t total = encodeWorkerOp(op, scratch.data(), scratch.size());

    ASSERT_GE(total, sizeof(WorkerOpWire) + op.recordBatchBytes.size());

    const WorkerOpWire* hdr = decodeWorkerOpHeader(scratch.data());
    ASSERT_NE(hdr, nullptr);

    assertOpScalars(*hdr, op);
    EXPECT_EQ(hdr->tailKind, static_cast<uint8_t>(TailKind::RecordBatch));
    EXPECT_EQ(hdr->tailLen,  static_cast<uint32_t>(op.recordBatchBytes.size()));

    const uint8_t* tail = workerOpTail(scratch.data());
    ASSERT_NE(tail, nullptr);
    EXPECT_EQ(std::memcmp(tail, op.recordBatchBytes.data(), op.recordBatchBytes.size()), 0);

    EXPECT_EQ(workerOpMsgTypeId(op), static_cast<int32_t>(WorkerOp::Type::Produce) + 1);
}

// ---------------------------------------------------------------------------
// 2. PushRead — no tail
// ---------------------------------------------------------------------------
TEST(WorkerWire, PushReadRoundTrip) {
    WorkerOp op = makeBaseOp(WorkerOp::Type::PushRead);

    auto scratch = makeScratch();
    std::size_t total = encodeWorkerOp(op, scratch.data(), scratch.size());
    EXPECT_EQ(total, sizeof(WorkerOpWire));

    const WorkerOpWire* hdr = decodeWorkerOpHeader(scratch.data());
    assertOpScalars(*hdr, op);
    EXPECT_EQ(hdr->tailKind, static_cast<uint8_t>(TailKind::None));
    EXPECT_EQ(hdr->tailLen,  0u);
    EXPECT_EQ(workerOpTail(scratch.data()), nullptr);
}

// ---------------------------------------------------------------------------
// 3. ApplyReplicated — replicatedPayload.records tail
// ---------------------------------------------------------------------------
TEST(WorkerWire, ApplyReplicatedRoundTrip) {
    WorkerOp op = makeBaseOp(WorkerOp::Type::ApplyReplicated);
    op.replicatedPayload.records = makeBytes(256, 0xCD);

    auto scratch = makeScratch();
    std::size_t total = encodeWorkerOp(op, scratch.data(), scratch.size());
    ASSERT_GE(total, sizeof(WorkerOpWire) + op.replicatedPayload.records.size());

    const WorkerOpWire* hdr = decodeWorkerOpHeader(scratch.data());
    assertOpScalars(*hdr, op);
    EXPECT_EQ(hdr->tailKind, static_cast<uint8_t>(TailKind::ReplicatedRecords));
    EXPECT_EQ(hdr->tailLen,  static_cast<uint32_t>(op.replicatedPayload.records.size()));

    const uint8_t* tail = workerOpTail(scratch.data());
    ASSERT_NE(tail, nullptr);
    EXPECT_EQ(std::memcmp(tail, op.replicatedPayload.records.data(),
                          op.replicatedPayload.records.size()), 0);
}

// ---------------------------------------------------------------------------
// 4. BecomeLeader — isr NodeId[] tail
// ---------------------------------------------------------------------------
TEST(WorkerWire, BecomeLeaderRoundTrip) {
    WorkerOp op = makeBaseOp(WorkerOp::Type::BecomeLeader);
    op.isr = {1, 2, 3, 5};  // 4 NodeId values

    auto scratch = makeScratch();
    std::size_t expectedTailBytes = op.isr.size() * sizeof(rune::replication::NodeId);
    std::size_t total = encodeWorkerOp(op, scratch.data(), scratch.size());
    ASSERT_EQ(total, sizeof(WorkerOpWire) + expectedTailBytes);

    const WorkerOpWire* hdr = decodeWorkerOpHeader(scratch.data());
    assertOpScalars(*hdr, op);
    EXPECT_EQ(hdr->tailKind, static_cast<uint8_t>(TailKind::IsrList));
    EXPECT_EQ(hdr->tailLen,  static_cast<uint32_t>(expectedTailBytes));

    const uint8_t* tail = workerOpTail(scratch.data());
    ASSERT_NE(tail, nullptr);
    // Decode NodeIds from tail
    ASSERT_EQ(hdr->tailLen % sizeof(rune::replication::NodeId), 0u);
    std::size_t count = hdr->tailLen / sizeof(rune::replication::NodeId);
    ASSERT_EQ(count, op.isr.size());
    for (std::size_t i = 0; i < count; ++i) {
        rune::replication::NodeId nodeId{};
        std::memcpy(&nodeId, tail + i * sizeof(rune::replication::NodeId), sizeof(nodeId));
        EXPECT_EQ(nodeId, op.isr[i]) << "isr[" << i << "] mismatch";
    }
}

// ---------------------------------------------------------------------------
// 5. BecomeFollower — no tail
// ---------------------------------------------------------------------------
TEST(WorkerWire, BecomeFollowerRoundTrip) {
    WorkerOp op = makeBaseOp(WorkerOp::Type::BecomeFollower);

    auto scratch = makeScratch();
    std::size_t total = encodeWorkerOp(op, scratch.data(), scratch.size());
    EXPECT_EQ(total, sizeof(WorkerOpWire));

    const WorkerOpWire* hdr = decodeWorkerOpHeader(scratch.data());
    assertOpScalars(*hdr, op);
    EXPECT_EQ(hdr->tailKind, static_cast<uint8_t>(TailKind::None));
    EXPECT_EQ(hdr->tailLen,  0u);
    EXPECT_EQ(workerOpTail(scratch.data()), nullptr);
}

// ---------------------------------------------------------------------------
// 6. SetOwnership — ownedSet as uintptr_t[] tail
// ---------------------------------------------------------------------------
TEST(WorkerWire, SetOwnershipRoundTrip) {
    WorkerOp op = makeBaseOp(WorkerOp::Type::SetOwnership);
    // Use sentinel replica pointers
    static int dummy0 = 0, dummy1 = 1, dummy2 = 2;
    op.ownedSet = {
        reinterpret_cast<rune::replication::PartitionReplica*>(&dummy0),
        reinterpret_cast<rune::replication::PartitionReplica*>(&dummy1),
        reinterpret_cast<rune::replication::PartitionReplica*>(&dummy2),
    };

    auto scratch = makeScratch();
    std::size_t expectedTailBytes = op.ownedSet.size() * sizeof(uintptr_t);
    std::size_t total = encodeWorkerOp(op, scratch.data(), scratch.size());
    ASSERT_EQ(total, sizeof(WorkerOpWire) + expectedTailBytes);

    const WorkerOpWire* hdr = decodeWorkerOpHeader(scratch.data());
    assertOpScalars(*hdr, op);
    EXPECT_EQ(hdr->tailKind, static_cast<uint8_t>(TailKind::OwnedSet));
    EXPECT_EQ(hdr->tailLen,  static_cast<uint32_t>(expectedTailBytes));

    const uint8_t* tail = workerOpTail(scratch.data());
    ASSERT_NE(tail, nullptr);
    ASSERT_EQ(hdr->tailLen % sizeof(uintptr_t), 0u);
    std::size_t count = hdr->tailLen / sizeof(uintptr_t);
    ASSERT_EQ(count, op.ownedSet.size());
    for (std::size_t i = 0; i < count; ++i) {
        uintptr_t pval{};
        std::memcpy(&pval, tail + i * sizeof(uintptr_t), sizeof(uintptr_t));
        EXPECT_EQ(pval, reinterpret_cast<uintptr_t>(op.ownedSet[i]))
            << "ownedSet[" << i << "] pointer mismatch";
    }
}

// ---------------------------------------------------------------------------
// 7. ProduceResult — no tail
// ---------------------------------------------------------------------------
TEST(WorkerWire, ProduceResultRoundTrip) {
    WorkerReturn ret{};
    ret.type             = WorkerReturn::Type::ProduceResult;
    ret.sessionId        = 0x1122334455667788LL;
    ret.correlationId    = 0xAABBCCDD11223344LL;
    ret.partition        = 3;
    ret.baseOffset       = 1000;
    ret.lastOffset       = 1009;
    ret.errorCode        = 0;
    ret.deferAck         = true;
    ret.replica          = dummyReplica();
    ret.producerId       = 987654321LL;
    ret.producerEpoch    = 7;
    ret.baseSequence     = 42;
    ret.recordCount      = 10;
    ret.topicId          = 7;
    ret.batchAttrs       = 0x0010;
    ret.enqueuedAtNs     = 9876543210LL;
    ret.subscriptionId   = 555;
    ret.readTopicId      = 7;
    ret.readPartition    = 3;
    ret.newNextOffset    = 1010;
    ret.hwBound          = 1050;
    ret.isolation        = 0;
    ret.isGroup          = false;
    ret.groupCarrierReceived = false;

    auto scratch = makeScratch();
    std::size_t total = encodeWorkerReturn(ret, scratch.data(), scratch.size());
    EXPECT_EQ(total, sizeof(WorkerReturnWire));

    const WorkerReturnWire* hdr = decodeWorkerReturnHeader(scratch.data());
    ASSERT_NE(hdr, nullptr);
    EXPECT_EQ(hdr->type,             static_cast<uint8_t>(ret.type));
    EXPECT_EQ(hdr->sessionId,        ret.sessionId);
    EXPECT_EQ(hdr->correlationId,    ret.correlationId);
    EXPECT_EQ(hdr->partition,        ret.partition);
    EXPECT_EQ(hdr->baseOffset,       ret.baseOffset);
    EXPECT_EQ(hdr->lastOffset,       ret.lastOffset);
    EXPECT_EQ(hdr->errorCode,        ret.errorCode);
    EXPECT_EQ(hdr->deferAck,         ret.deferAck ? uint8_t{1} : uint8_t{0});
    EXPECT_EQ(hdr->replica,          reinterpret_cast<uintptr_t>(ret.replica));
    EXPECT_EQ(hdr->producerId,       ret.producerId);
    EXPECT_EQ(hdr->producerEpoch,    ret.producerEpoch);
    EXPECT_EQ(hdr->baseSequence,     ret.baseSequence);
    EXPECT_EQ(hdr->recordCount,      ret.recordCount);
    EXPECT_EQ(hdr->topicId,          ret.topicId);
    EXPECT_EQ(hdr->batchAttrs,       ret.batchAttrs);
    EXPECT_EQ(hdr->enqueuedAtNs,     ret.enqueuedAtNs);
    EXPECT_EQ(hdr->subscriptionId,   ret.subscriptionId);
    EXPECT_EQ(hdr->readTopicId,      ret.readTopicId);
    EXPECT_EQ(hdr->readPartition,    ret.readPartition);
    EXPECT_EQ(hdr->newNextOffset,    ret.newNextOffset);
    EXPECT_EQ(hdr->hwBound,          ret.hwBound);
    EXPECT_EQ(hdr->isolation,        ret.isolation);
    EXPECT_EQ(hdr->isGroup,          ret.isGroup ? uint8_t{1} : uint8_t{0});
    EXPECT_EQ(hdr->groupCarrierReceived, ret.groupCarrierReceived ? uint8_t{1} : uint8_t{0});
    EXPECT_EQ(hdr->tailKind,         static_cast<uint8_t>(TailKind::None));
    EXPECT_EQ(hdr->tailLen,          0u);
    EXPECT_EQ(workerReturnTail(scratch.data()), nullptr);

    EXPECT_EQ(workerReturnMsgTypeId(ret),
              static_cast<int32_t>(WorkerReturn::Type::ProduceResult) + 1);
}

// ---------------------------------------------------------------------------
// 8. PushReadResult — chunk tail
// ---------------------------------------------------------------------------
TEST(WorkerWire, PushReadResultRoundTrip) {
    WorkerReturn ret{};
    ret.type             = WorkerReturn::Type::PushReadResult;
    ret.sessionId        = 111;
    ret.subscriptionId   = 222;
    ret.readTopicId      = 5;
    ret.readPartition    = 2;
    ret.newNextOffset    = 300;
    ret.hwBound          = 500;
    ret.isolation        = 1;
    ret.isGroup          = true;
    ret.groupCarrierReceived = true;
    ret.chunk            = makeBytes(800, 0xDE);

    auto scratch = makeScratch();
    std::size_t total = encodeWorkerReturn(ret, scratch.data(), scratch.size());
    ASSERT_EQ(total, sizeof(WorkerReturnWire) + ret.chunk.size());

    const WorkerReturnWire* hdr = decodeWorkerReturnHeader(scratch.data());
    ASSERT_NE(hdr, nullptr);
    EXPECT_EQ(hdr->type,             static_cast<uint8_t>(ret.type));
    EXPECT_EQ(hdr->subscriptionId,   ret.subscriptionId);
    EXPECT_EQ(hdr->readTopicId,      ret.readTopicId);
    EXPECT_EQ(hdr->readPartition,    ret.readPartition);
    EXPECT_EQ(hdr->newNextOffset,    ret.newNextOffset);
    EXPECT_EQ(hdr->hwBound,          ret.hwBound);
    EXPECT_EQ(hdr->isolation,        ret.isolation);
    EXPECT_EQ(hdr->isGroup,          ret.isGroup ? uint8_t{1} : uint8_t{0});
    EXPECT_EQ(hdr->groupCarrierReceived, ret.groupCarrierReceived ? uint8_t{1} : uint8_t{0});
    EXPECT_EQ(hdr->tailKind,         static_cast<uint8_t>(TailKind::Chunk));
    EXPECT_EQ(hdr->tailLen,          static_cast<uint32_t>(ret.chunk.size()));

    const uint8_t* tail = workerReturnTail(scratch.data());
    ASSERT_NE(tail, nullptr);
    EXPECT_EQ(std::memcmp(tail, ret.chunk.data(), ret.chunk.size()), 0);
}

// ---------------------------------------------------------------------------
// 9. ProduceRoundTripThroughRing — encode→write→drain→decode with 4KB batch
// ---------------------------------------------------------------------------
TEST(WorkerWire, ProduceRoundTripThroughRing) {
    // Ring must be large enough: sizeof(WorkerOpWire) + 4KB batch + agrona overhead.
    // sizeof(WorkerOpWire) < 256 bytes; batch = 4096; TRAILER ~ 128 bytes.
    // Use 64KB ring to be safe.
    OneToOneByteRing ring(/*payloadCapacity=*/64 * 1024);

    WorkerOp op = makeBaseOp(WorkerOp::Type::Produce);
    op.recordBatchBytes = makeBytes(4096, 0x77);

    auto scratch = makeScratch();
    std::size_t encodedLen = encodeWorkerOp(op, scratch.data(), scratch.size());
    ASSERT_GT(encodedLen, sizeof(WorkerOpWire));

    int32_t typeId = workerOpMsgTypeId(op);
    EXPECT_GE(typeId, 1);  // agrona requirement

    bool ok = ring.write(scratch.data(), encodedLen, typeId);
    ASSERT_TRUE(ok) << "write() should succeed for 4KB batch in 64KB ring";

    bool handlerCalled = false;
    int count = ring.drain(
        [&](int32_t msgTypeId,
            aeron::concurrent::AtomicBuffer& buf,
            aeron::util::index_t             offset,
            aeron::util::index_t             length)
        {
            handlerCalled = true;
            EXPECT_EQ(msgTypeId, typeId);
            EXPECT_EQ(static_cast<std::size_t>(length), encodedLen);

            // Decode header + tail from ring view (zero-copy)
            const uint8_t* frame = buf.buffer() + offset;

            const WorkerOpWire* hdr = decodeWorkerOpHeader(frame);
            ASSERT_NE(hdr, nullptr);

            // Assert all scalars
            EXPECT_EQ(hdr->type,          static_cast<uint8_t>(op.type));
            EXPECT_EQ(hdr->topicId,       op.topicId);
            EXPECT_EQ(hdr->partition,     op.partition);
            EXPECT_EQ(hdr->replica,       reinterpret_cast<uintptr_t>(op.replica));
            EXPECT_EQ(hdr->logPtr,        reinterpret_cast<uintptr_t>(op.logPtr));
            EXPECT_EQ(hdr->sessionId,     op.sessionId);
            EXPECT_EQ(hdr->correlationId, op.correlationId);
            EXPECT_EQ(hdr->acks,          op.acks);
            EXPECT_EQ(hdr->producerId,    op.producerId);
            EXPECT_EQ(hdr->producerEpoch, op.producerEpoch);
            EXPECT_EQ(hdr->baseSequence,  op.baseSequence);
            EXPECT_EQ(hdr->lastOffsetDelta, op.lastOffsetDelta);
            EXPECT_EQ(hdr->recordCount,   op.recordCount);
            EXPECT_EQ(hdr->batchAttrs,    op.batchAttrs);
            EXPECT_EQ(hdr->deferAck,      op.deferAck ? uint8_t{1} : uint8_t{0});
            EXPECT_EQ(hdr->durabilityTier, op.durabilityTier);
            EXPECT_EQ(hdr->enqueuedAtNs,  op.enqueuedAtNs);
            EXPECT_EQ(hdr->tailKind,      static_cast<uint8_t>(TailKind::RecordBatch));
            EXPECT_EQ(hdr->tailLen,       static_cast<uint32_t>(op.recordBatchBytes.size()));

            // Assert tail bytes are byte-identical
            const uint8_t* tail = workerOpTail(frame);
            ASSERT_NE(tail, nullptr);
            EXPECT_EQ(std::memcmp(tail, op.recordBatchBytes.data(),
                                  op.recordBatchBytes.size()), 0);
        },
        /*limit=*/1);

    EXPECT_EQ(count, 1);
    EXPECT_TRUE(handlerCalled);
}
