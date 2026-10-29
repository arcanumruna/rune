// CheckedWritesWorkerTest.cpp — checked writes Part 02 Task 4: the worker side of typed validation.  A Worker is
// driven directly (no Aeron): WriteSpecUpdate / SchemaUpdate / ClusterFeatureLevel ops install the spec and the level,
// a Produce op answers the way produceCore_ decides.
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WriteState.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include "support/CheckedWritesBatches.h"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace rune::node;
using namespace rune::node::cw;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::storage::Log;

namespace {

constexpr int32_t kTopic = 1;
constexpr int16_t kOk = 0, kNotLeader = 2, kSchemaValidationFailed = 24;

class NullTransport : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};

std::atomic<uint32_t> gSeq{0};

struct Rig {
    fs::path                          dir;
    std::unique_ptr<Log>              log;
    NullTransport                     transport;
    std::unique_ptr<PartitionReplica> replica;
    std::unique_ptr<Worker>           worker;
    std::vector<WorkerReturn>         returns;
    int64_t                           nowNs = 1;
    int64_t                           corr  = 1;

    explicit Rig(bool owned = true) {
        dir = fs::temp_directory_path() / ("rune_cwworker_" + std::to_string(::getpid()) + "_" + std::to_string(gSeq++));
        fs::create_directories(dir);
        log = std::make_unique<Log>(dir.string(), 64LL * 1024 * 1024);
        log->recover();
        replica = makeReplica();
        worker = std::make_unique<Worker>(0);
        if (owned) own();
    }
    ~Rig() { worker.reset(); replica.reset(); log.reset(); std::error_code ec; fs::remove_all(dir, ec); }

    std::unique_ptr<PartitionReplica> makeReplica() {
        PartitionConfig cfg{};
        cfg.topicId = kTopic; cfg.partitionId = 0; cfg.selfId = 1; cfg.minIsr = 1;
        cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL; cfg.maxFetchBytes = 1 << 20; cfg.pushOnAppend = false;
        return std::make_unique<PartitionReplica>(cfg, rune::replication::Role::Leader, 1, std::vector<rune::replication::NodeId>{1},
                                                  std::vector<rune::replication::NodeId>{1}, log.get(), &transport);
    }
    void own() { worker->setOwnedDirect(Worker::OwnedSet{replica.get()}); }
    void disown() { worker->setOwnedDirect(Worker::OwnedSet{}); }

    void pass(int n = 3) {
        for (int i = 0; i < n; ++i) (void)worker->doWork(nowNs += 1'000'000);
        worker->drainReturn([&](WorkerReturn r) { returns.push_back(std::move(r)); });
    }
    void send(WorkerOp op) { ASSERT_TRUE(worker->tryEnqueueOp(std::move(op))); pass(); }

    void sendVersion(const rune::codec::Descriptor& d, uint16_t version) {
        WorkerOp op;
        op.type = WorkerOp::Type::SchemaUpdate; op.topicId = kTopic; op.clientVersion = version;
        op.schemaUpdateBlob = rune::codec::serialize(d);
        send(std::move(op));
    }
    void sendSpec(const TopicWriteSpecWire& w) {
        WorkerOp op;
        op.type = WorkerOp::Type::WriteSpecUpdate; op.topicId = w.topicId;
        op.schemaUpdateBlob.resize(kTopicWriteSpecWireLen);
        ASSERT_EQ(encodeTopicWriteSpec(w, op.schemaUpdateBlob), kTopicWriteSpecWireLen);
        send(std::move(op));
    }
    void sendLevel(int16_t level) {
        WorkerOp op;
        op.type = WorkerOp::Type::ClusterFeatureLevel; op.nextOffset = level;
        send(std::move(op));
    }
    void typed(bool legacyValidate = true) {
        sendVersion(matrixDescriptor(), 1);
        TopicWriteSpecWire w;
        w.topicId = kTopic; w.keyFieldId = kIdTag; w.keyType = rune::codec::PrimType::UInt64; w.keyWidth = 8; w.legacyValidate = legacyValidate;
        sendSpec(w);
    }

    WorkerOp produceOp(std::vector<uint8_t> batch) {
        WorkerOp op;
        op.type = WorkerOp::Type::Produce; op.topicId = kTopic; op.partition = 0; op.replica = replica.get();
        op.sessionId = 1; op.correlationId = corr++;
        op.lastOffsetDelta = 0; op.recordCount = 1; op.durabilityTier = 0;
        op.recordBatchBytes = std::move(batch);
        return op;
    }
    int16_t produce(std::vector<uint8_t> batch) {
        returns.clear();
        WorkerOp op = produceOp(std::move(batch));
        const int64_t id = op.correlationId;
        send(std::move(op));
        for (const auto& r : returns)
            if (r.type == WorkerReturn::Type::ProduceResult && r.correlationId == id) return r.errorCode;
        return int16_t{-1};
    }
    int16_t produce(const TypedRecord& r) { return produce(batchOf({r})); }
};

TypedRecord badUtf8() { Parts p; p.bio = std::string("\xC0\xAF", 2); return encode(p); }
TypedRecord wrongKey() { Parts p; p.key = leBytes(p.id, 4); return encode(p); }

}  // namespace

TEST(CheckedWritesWorker, UntypedTopicHasNoWriteStateAndAcceptsAnyBytes) {
    Rig r;
    EXPECT_EQ(r.worker->writeStateForTest(kTopic, 0), nullptr);
    Parts p;
    p.version = 77; p.blockLength = 3;
    EXPECT_EQ(r.produce(encode(p)), kOk);
}

TEST(CheckedWritesWorker, SpecBeforeOwnershipCreatesTheStateOnOwnership) {
    Rig r(/*owned=*/false);
    r.typed();
    EXPECT_EQ(r.worker->writeStateForTest(kTopic, 0), nullptr) << "not owned yet";
    r.own();
    const PartitionWriteState* ws = r.worker->writeStateForTest(kTopic, 0);
    ASSERT_NE(ws, nullptr);
    ASSERT_NE(ws->spec, nullptr);
    EXPECT_EQ(ws->spec->topicId, kTopic);
    r.sendLevel(5);
    EXPECT_EQ(r.produce(badUtf8()), kSchemaValidationFailed);
}

TEST(CheckedWritesWorker, SpecAfterOwnershipAttachesToOwnedPartitions) {
    Rig r;
    Parts garbage; garbage.blockLength = 3;
    EXPECT_EQ(r.produce(encode(garbage)), kOk);
    r.typed();
    EXPECT_NE(r.worker->writeStateForTest(kTopic, 0), nullptr);
    EXPECT_EQ(r.produce(encode(garbage)), kSchemaValidationFailed);
}

TEST(CheckedWritesWorkerRegression, VersionAheadIsRetriableUntilInstalledRegression) {
    Rig r;
    r.typed();
    Parts ahead; ahead.version = 2;
    EXPECT_EQ(r.produce(encode(ahead)), kNotLeader);
    EXPECT_EQ(r.worker->typedVersionAheadForTest(), 1u);
    r.sendVersion(matrixDescriptor(2), 2);
    EXPECT_EQ(r.produce(encode(ahead)), kOk);
    Parts zero; zero.version = 0;
    EXPECT_EQ(r.produce(encode(zero)), kSchemaValidationFailed);
    EXPECT_EQ(r.worker->typedVersionAheadForTest(), 1u);
}

TEST(CheckedWritesWorkerRegression, LevelRaiseStartsFullValidationRegression) {
    Rig r;
    r.typed(/*legacyValidate=*/false);
    r.sendLevel(4);
    ASSERT_EQ(r.produce(badUtf8()), kOk) << "below the level a topic created without validate accepts it";
    ASSERT_EQ(r.produce(wrongKey()), kOk);
    const int64_t leo = r.log->logEndOffset();
    r.sendLevel(5);
    EXPECT_EQ(r.produce(badUtf8()), kSchemaValidationFailed);
    EXPECT_EQ(r.produce(wrongKey()), kSchemaValidationFailed);
    EXPECT_EQ(r.log->logEndOffset(), leo) << "refused batches are not appended";
    EXPECT_EQ(r.produce(validRecord()), kOk);
    EXPECT_FALSE(r.log->read(0, 1 << 20).empty()) << "the records accepted earlier still read back";
}

TEST(CheckedWritesWorker, FeatureLevelNeverLowers) {
    Rig r;
    r.typed(false);
    r.sendLevel(5);
    r.sendLevel(4);
    EXPECT_EQ(r.worker->clusterFeatureLevelForTest(), 5);
    EXPECT_EQ(r.produce(badUtf8()), kSchemaValidationFailed);
}

TEST(CheckedWritesWorker, RoleChangesKeepTheWriteState) {
    Rig r;
    r.typed();
    const PartitionWriteState* before = r.worker->writeStateForTest(kTopic, 0);
    ASSERT_NE(before, nullptr);
    WorkerOp follower;
    follower.type = WorkerOp::Type::BecomeFollower; follower.topicId = kTopic; follower.partition = 0; follower.replica = r.replica.get();
    follower.logPtr = r.replica->log(); follower.roleEpoch = 2; follower.roleLeaderId = 2;
    r.send(std::move(follower));
    WorkerOp leader;
    leader.type = WorkerOp::Type::BecomeLeader; leader.topicId = kTopic; leader.partition = 0; leader.replica = r.replica.get();
    leader.logPtr = r.replica->log(); leader.roleEpoch = 3; leader.isr = {1};
    r.send(std::move(leader));
    EXPECT_EQ(r.worker->writeStateForTest(kTopic, 0), before);
}

TEST(CheckedWritesWorker, NewIncarnationGetsAFreshState) {
    Rig r;
    r.typed();
    const TopicWriteSpec* spec = r.worker->writeStateForTest(kTopic, 0)->spec;
    r.disown();
    EXPECT_EQ(r.worker->writeStateForTest(kTopic, 0), nullptr);
    auto second = r.makeReplica();
    r.worker->setOwnedDirect(Worker::OwnedSet{second.get()});
    const PartitionWriteState* ws = r.worker->writeStateForTest(kTopic, 0);
    ASSERT_NE(ws, nullptr);
    EXPECT_EQ(ws->spec, spec);
    r.worker->setOwnedDirect(Worker::OwnedSet{});
}

TEST(CheckedWritesWorker, TopicDropReleasesSpecAndStates) {
    Rig r;
    r.typed();
    ASSERT_TRUE(r.worker->hasValidatorForTest(kTopic));
    WorkerOp drop;
    drop.type = WorkerOp::Type::TopicDropped; drop.topicId = kTopic;
    r.send(std::move(drop));
    EXPECT_FALSE(r.worker->hasValidatorForTest(kTopic));
    EXPECT_EQ(r.worker->writeStateForTest(kTopic, 0), nullptr);
}

TEST(CheckedWritesWorker, MixedVersionBatchValidatesEachRecordByItsVersion) {
    Rig r;
    r.typed();
    r.sendVersion(matrixDescriptor(2), 2);
    r.sendLevel(5);
    EXPECT_EQ(r.produce(batchOf({validRecord(1, 1), validRecord(2, 2)})), kOk);
    Parts v2bad; v2bad.version = 2; v2bad.blockLength = 29;
    EXPECT_EQ(r.produce(batchOf({validRecord(1, 1), encode(v2bad)})), kSchemaValidationFailed);
}

TEST(CheckedWritesWorker, LateVersionGetsItsKeyOffset) {
    using rune::codec::DescriptorBuilder;
    using rune::codec::PrimType;
    Rig r;
    const auto v1 = DescriptorBuilder("t", 1).add("a", PrimType::Int32).add("id", PrimType::UInt64).build();
    const auto v2 = DescriptorBuilder("t", 2).add("a", PrimType::Int32).add("id", PrimType::UInt64).add("b", PrimType::Int64, 2).build();
    r.sendVersion(v1, 1);
    TopicWriteSpecWire w;
    w.topicId = kTopic; w.keyFieldId = 1; w.keyType = PrimType::UInt64; w.keyWidth = 8; w.legacyValidate = true;
    r.sendSpec(w);
    r.sendVersion(v2, 2);   // arrives after the facts
    r.sendLevel(5);
    const auto value = [](uint64_t id) {
        return leBytes(20, 2) + leBytes(2, 2) + leBytes(5, 4) + leBytes(id, 8) + leBytes(9, 8);
    };
    EXPECT_EQ(r.produce(TypedRecord{leBytes(77, 8), value(77)}), kOk);
    EXPECT_EQ(r.produce(TypedRecord{leBytes(78, 8), value(77)}), kSchemaValidationFailed);
}

TEST(CheckedWritesWorkerRegression, TombstoneOnValidatedTopicIsAcceptedRegression) {
    for (int16_t level : {int16_t{4}, int16_t{5}}) {
        Rig r;
        r.typed();
        r.sendLevel(level);
        Parts p; p.tombstone = true;
        EXPECT_EQ(r.produce(encode(p)), kOk) << "level " << level;
    }
}

TEST(CheckedWritesWorker, ControlRecordsAreNeverValidated) {
    Rig r;
    r.typed();
    r.sendLevel(5);
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.setControl(true);
    b.setProducerId(5); b.setProducerEpoch(0); b.setBaseSequence(0);
    b.append("", std::string("\x00\x00\x00\x01\x00\x00\x00\x00", 8), 1000);
    WorkerOp op;
    op.type = WorkerOp::Type::AppendControlRecord; op.topicId = kTopic; op.partition = 0; op.replica = r.replica.get();
    op.logPtr = r.replica->log(); op.correlationId = 99; op.recordBatchBytes = b.build();
    r.returns.clear();
    r.send(std::move(op));
    ASSERT_FALSE(r.returns.empty());
    EXPECT_EQ(r.returns.back().type, WorkerReturn::Type::ControlRecordAppended);
}
