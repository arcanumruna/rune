// ---------------------------------------------------------------------------
// ProduceValidationTest.cpp — Task 7: per-worker schema validation hook.
//
// Tests the SchemaUpdate → executeProduceWire_ validation hook using a Worker
// driven directly (no Aeron media driver required).
//
// Test matrix:
//
//   1. SchemaUpdate_Regression_ValidatorInstalledOnWorkerThread
//        Enqueue a SchemaUpdate WorkerOp to the worker.  doWork drains it.
//        Assert: hasValidatorForTest(topicId) is true after draining.
//        Regression: validator is installed on the worker thread (shared-nothing).
//
//   2. SchemaUpdate_Regression_WellFormedRecord_AppendProceeds
//        Install a validator for topicId via SchemaUpdate.
//        Produce a well-formed record (blockLength + version + fixed fields match
//        the descriptor + valid enum value).
//        Assert: produce returns errorCode=0 (OK) AND log->logEndOffset() advances.
//        Regression: a valid record must NOT be rejected.
//
//   3. SchemaUpdate_Regression_MalformedRecord_ValidationFailed
//        Install a validator for topicId via SchemaUpdate.
//        Produce a record whose value is TOO SHORT (empty span) — cannot satisfy
//        the fixed block.
//        Assert: produce returns errorCode=24 (SchemaValidationFailed) AND
//                log->logEndOffset() is UNCHANGED (no append).
//        Regression: a malformed record must be rejected with no append.
//
//   4. SchemaUpdate_Regression_BadEnumRecord_ValidationFailed
//        Install a validator for topicId with a descriptor that has an Enum field
//        with a whitelist {0,1,2}.  Produce a record with enum value 99 (not in
//        whitelist).
//        Assert: errorCode=24 AND log end offset unchanged.
//        Regression: a bad enum must be caught before append.
//
//   5. SchemaUpdate_Regression_NoValidator_Falls_Through
//        Produce to a topicId that has NO SchemaUpdate.
//        Assert: produce returns errorCode=0 AND appends.
//        Regression: the absence of a validator must NOT block produce.
//
// Harness: a single Worker owning one InMemoryLog leader PartitionReplica
// (minIsr=1).  The Worker is driven by calling doWork() directly.
// No Aeron media driver needed (uses the SPSC ingress ring directly).
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/Worker.h>
#include <rune/node/BrokerNode.h>      // AdminWorkerAgent (admin->control ring)
#include <rune/node/SchemaFanout.h>
#include <rune/node/SchemaRegistry.h>
#include <thread>
#include <rune/node/WorkerOp.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Validator.h>
#include "support/AllocWindow.h"

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <unistd.h>

namespace fs = std::filesystem;

using rune::node::Worker;
using rune::node::WorkerOp;
using rune::node::WorkerReturn;
using rune::replication::PartitionReplica;
using rune::replication::PartitionConfig;
using rune::storage::Log;
using rune::codec::Descriptor;
using rune::codec::DescriptorBuilder;
using rune::codec::PrimType;
using rune::codec::Validator;
using rune::protocol::RecordBatchBuilder;

namespace rune::node::test {
namespace {

// ---------------------------------------------------------------------------
// NullTransport — no-op replication transport for single-node tests.
// ---------------------------------------------------------------------------
class NullTransport : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId,
                           const rune::replication::FetchResponseMsg&) override { return true; }
};

// ---------------------------------------------------------------------------
// Helpers — build descriptors, records, and batches.
// ---------------------------------------------------------------------------

/// A simple 3-field descriptor matching ValidatorTest.cpp ("c" schema):
///   id    : Int64
///   type  : Enum  (whitelist: {0,1,2})
///   name  : String
static Descriptor makeCharactersDescriptor() {
    return DescriptorBuilder("characters")
        .add("id",   PrimType::Int64)
        .add("type", PrimType::Enum, /*sinceVersion=*/1, /*isKey=*/false, {0, 1, 2})
        .add("name", PrimType::String)
        .build();
}

/// Build a VALID record frame for the "characters" descriptor.
/// blockLength=12 (int64 + int32), version=1.
///   block: id=1 (LE64), type=1 (LE32 enum)
///   vardata: name length (LE32) + "Alice"
static std::vector<uint8_t> makeValidRecordValue(int64_t id = 1,
                                                  int32_t enumType = 1,
                                                  const char* name = "Alice") {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) {
        b.push_back(static_cast<uint8_t>(v));
        b.push_back(static_cast<uint8_t>(v >> 8));
    };
    auto le32 = [&](uint32_t v) {
        b.push_back(static_cast<uint8_t>(v));
        b.push_back(static_cast<uint8_t>(v >> 8));
        b.push_back(static_cast<uint8_t>(v >> 16));
        b.push_back(static_cast<uint8_t>(v >> 24));
    };
    auto le64 = [&](uint64_t v) {
        for (int i = 0; i < 8; ++i) b.push_back(static_cast<uint8_t>(v >> (i * 8)));
    };
    le16(12);                          // blockLength = 12 (int64 + int32)
    le16(1);                           // version = 1
    le64(static_cast<uint64_t>(id));   // id (int64)
    le32(static_cast<uint32_t>(enumType)); // type (enum = int32)
    std::string_view sv(name);
    le32(static_cast<uint32_t>(sv.size())); // name length
    b.insert(b.end(), sv.begin(), sv.end());
    return b;
}

/// Build a record batch whose single record's VALUE is `recValue`.
static std::vector<uint8_t> makeBatchWithValue(const std::vector<uint8_t>& recValue) {
    RecordBatchBuilder b(0, /*baseTimestamp=*/1000);
    std::string_view key{};
    std::string_view val(reinterpret_cast<const char*>(recValue.data()), recValue.size());
    b.append(key, val, 1000);
    return b.build();
}

/// Build a record batch containing MULTIPLE records, one per entry of
/// `values` -- used to simulate a producer that evolved mid-batch (GC3 /
/// C51-55): a single RecordBatch whose records carry different schema
/// versions.
static std::vector<uint8_t> makeBatchWithValues(const std::vector<std::vector<uint8_t>>& values) {
    RecordBatchBuilder b(0, /*baseTimestamp=*/1000);
    std::string_view key{};
    for (std::size_t i = 0; i < values.size(); ++i) {
        std::string_view val(reinterpret_cast<const char*>(values[i].data()), values[i].size());
        b.append(key, val, 1000 + static_cast<int64_t>(i));
    }
    return b.build();
}

/// An EMPTY record value (too short to satisfy any descriptor).
static std::vector<uint8_t> makeEmptyValue() { return {}; }

/// A truncated value — shorter than the fixed block.
static std::vector<uint8_t> makeTruncatedValue() {
    // blockLength=12, version=1, then only 3 bytes (too short for the 12-byte block).
    return {12, 0, 1, 0, 0xFF, 0xFF, 0xFF};
}

// ---------------------------------------------------------------------------
// Task 12: an EVOLVING "people" descriptor —
//   id    : Int64  (since v1, never retired)
//   age   : Int32  (since v1, RETIRED at v2 — untilVersion=2)
//   score : Int64  (since v2)
// materializeAsOf(evolving, 1) == {id, age}  (blockLength 12).
// materializeAsOf(evolving, 2) == {id, score} (blockLength 16).
// ---------------------------------------------------------------------------
static Descriptor makePeopleEvolvingDescriptor() {
    return DescriptorBuilder("people", /*version=*/2)
        .addTagged("id",    0, PrimType::Int64, /*since=*/1, /*until=*/0xFFFF, false, {})
        .addTagged("age",   1, PrimType::Int32, /*since=*/1, /*until=*/2,      false, {})
        .addTagged("score", 2, PrimType::Int64, /*since=*/2, /*until=*/0xFFFF, false, {})
        .build();
}

/// A record whose fixed block is EXACTLY (blockLength, version, id, extra:i64-or-i32).
/// `extraIsI64` selects whether the second fixed field is written as 8 bytes
/// (the v2 "score" layout) or 4 bytes (the v1 "age" layout) — the caller picks
/// `version` independently so a MISMATCHED (version, layout) pair can be built
/// (the "dropped-age layout stamped as v2" regression case).
static std::vector<uint8_t> makePeopleRecordValue(uint16_t version, int64_t id,
                                                  int64_t extra, bool extraIsI64) {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    auto le32 = [&](uint32_t v) { for (int i = 0; i < 4; ++i) b.push_back(uint8_t(v >> (8*i))); };
    auto le64 = [&](uint64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(v >> (8*i))); };
    const uint16_t blockLength = extraIsI64 ? 16 : 12;   // id(8) + extra(8 or 4)
    le16(blockLength);
    le16(version);
    le64(static_cast<uint64_t>(id));
    if (extraIsI64) le64(static_cast<uint64_t>(extra));
    else            le32(static_cast<uint32_t>(extra));
    return b;
}

// ---------------------------------------------------------------------------
// TempDir RAII — manages a temp directory for the disk-backed Log.
// ---------------------------------------------------------------------------
static std::atomic<uint32_t> g_dirSeq{0};
struct TempDir {
    fs::path path;
    explicit TempDir(const char* tag = "prodval") {
        auto seq = g_dirSeq.fetch_add(1, std::memory_order_relaxed);
        path = fs::temp_directory_path() /
               (std::string("rune_") + tag + "_" +
                std::to_string(static_cast<long>(::getpid())) +
                "_" + std::to_string(seq));
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

// ---------------------------------------------------------------------------
// WorkerHarness — disk-backed Log + Worker (no Aeron).
//
// Disk-backed Log is required because executeProduceWire_ calls
// log->appendNoSyncRestamp, which is only available on Log (not InMemoryLog).
// The disk is a tmpdir that is cleaned up on destruction.
// ---------------------------------------------------------------------------
struct WorkerHarness {
    TempDir                           tempDir;
    std::unique_ptr<Log>              log;
    NullTransport                     transport;
    std::unique_ptr<PartitionReplica> replica;
    std::unique_ptr<Worker>           worker;

    WorkerReturn lastReturn{};
    int16_t      lastErrorCode = -1;
    int          totalReturns  = 0;

    explicit WorkerHarness(int32_t topicId = 1, int32_t partition = 0)
    {
        log = std::make_unique<Log>(tempDir.path.string(),
                                    /*rollBytes=*/64LL * 1024 * 1024);
        log->recover();

        PartitionConfig cfg{};
        cfg.topicId             = topicId;
        cfg.partitionId         = static_cast<rune::replication::PartitionId>(partition);
        cfg.selfId              = 1;
        cfg.minIsr              = 1;
        cfg.replicaLagMaxMs     = 10'000;
        cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs     = 1'000'000LL;
        cfg.isrCheckIntervalNs  = 1'000'000LL;
        cfg.maxFetchBytes       = 1 << 20;
        cfg.pushOnAppend        = false;
        replica = std::make_unique<PartitionReplica>(
            cfg,
            rune::replication::Role::Leader, /*epoch=*/1,
            std::vector<rune::replication::NodeId>{1},
            std::vector<rune::replication::NodeId>{1},
            log.get(), &transport);
        worker = std::make_unique<Worker>(/*workerId=*/0);
        worker->setOwnedDirect(Worker::OwnedSet{replica.get()});
    }

    /// Enqueue a SchemaUpdate op for the given topicId + descriptor blob,
    /// installing a validator for `version` (Task 12; default 1 — every
    /// pre-Task-12 test descriptor is version 1).
    void enqueueSchemaUpdate(int32_t topicId,
                             const std::vector<uint8_t>& blob,
                             uint16_t version = 1) {
        WorkerOp op;
        op.type             = WorkerOp::Type::SchemaUpdate;
        op.topicId          = topicId;
        op.clientVersion    = static_cast<int32_t>(version);   // Task 12 (see WorkerOp.h)
        op.schemaUpdateBlob = blob;
        ASSERT_TRUE(worker->tryEnqueueOp(std::move(op)))
            << "SchemaUpdate enqueue failed (ring full)";
    }

    /// Enqueue the topic's WriteSpecUpdate (the key facts) after its schema versions: `legacyValidate` = the old
    /// `validate = true`.  Keyless unless `keyFieldId` names a key field.
    void enqueueWriteSpec(int32_t topicId, bool legacyValidate = true, uint16_t keyFieldId = kNoKeyField,
                          PrimType keyType = PrimType::Int32, uint8_t keyWidth = 0) {
        TopicWriteSpecWire w;
        w.topicId = topicId; w.keyFieldId = keyFieldId; w.keyType = keyType; w.keyWidth = keyWidth; w.legacyValidate = legacyValidate;
        WorkerOp op;
        op.type             = WorkerOp::Type::WriteSpecUpdate;
        op.topicId          = topicId;
        op.schemaUpdateBlob.resize(kTopicWriteSpecWireLen);
        ASSERT_EQ(encodeTopicWriteSpec(w, op.schemaUpdateBlob), kTopicWriteSpecWireLen);
        ASSERT_TRUE(worker->tryEnqueueOp(std::move(op))) << "WriteSpecUpdate enqueue failed (ring full)";
    }

    /// Enqueue a Produce op carrying `batch` for (topicId, partition).
    void enqueueProduceBatch(int32_t topicId, int32_t partition,
                             std::vector<uint8_t> batch,
                             int64_t correlationId = 1,
                             int64_t sessionId = 1,
                             int64_t producerId = -1,
                             int32_t baseSequence = -1) {
        // Compute lastOffsetDelta from the batch header.
        int32_t lastOffsetDelta = 0;
        if (batch.size() >= static_cast<std::size_t>(rune::protocol::batch::kRecordCountOffset + 4)) {
            const int32_t rc = static_cast<int32_t>(
                rune::protocol::detail::getLE32(
                    batch.data() + rune::protocol::batch::kRecordCountOffset));
            lastOffsetDelta = (rc > 0) ? (rc - 1) : 0;
        }
        WorkerOp op;
        op.type             = WorkerOp::Type::Produce;
        op.topicId          = topicId;
        op.partition        = partition;
        op.replica          = replica.get();
        op.logPtr           = nullptr; // disk-backed replica: getLogFromWire_ uses replica->log()
        op.sessionId        = sessionId;
        op.correlationId    = correlationId;
        op.producerId       = producerId;
        op.baseSequence     = baseSequence;
        op.lastOffsetDelta  = lastOffsetDelta;
        op.recordCount      = lastOffsetDelta + 1;
        op.durabilityTier   = 0; // Leader tier (ack immediately)
        op.recordBatchBytes = std::move(batch);
        ASSERT_TRUE(worker->tryEnqueueOp(std::move(op)))
            << "Produce enqueue failed (ring full)";
    }

    /// Drive the worker's duty cycle and collect returns.
    void tick(int n = 1) {
        int64_t nowNs = 1;
        for (int i = 0; i < n; ++i) {
            worker->doWork(nowNs += 1'000'000LL);
        }
        worker->drainReturn([this](WorkerReturn ret) {
            ++totalReturns;
            lastReturn    = ret;
            lastErrorCode = ret.errorCode;
        });
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Regression: SchemaUpdate installs a Validator on the worker thread (not inline).
TEST(ProduceValidation, SchemaUpdate_Regression_ValidatorInstalledOnWorkerThread) {
    WorkerHarness h;
    const int32_t topicId = 1;

    const Descriptor desc = makeCharactersDescriptor();
    const std::vector<uint8_t> blob = rune::codec::serialize(desc);

    // Precondition: no validator yet.
    ASSERT_FALSE(h.worker->hasValidatorForTest(topicId));

    h.enqueueSchemaUpdate(topicId, blob);
    h.enqueueWriteSpec(topicId);
    h.tick();  // worker drains ingress → executeSchemaUpdateWire_

    // Postcondition: validator installed.
    EXPECT_TRUE(h.worker->hasValidatorForTest(topicId))
        << "Validator must be installed after doWork drains the SchemaUpdate op";
}

/// Regression: a well-formed record passes validation and gets appended.
TEST(ProduceValidation, SchemaUpdate_Regression_WellFormedRecord_AppendProceeds) {
    WorkerHarness h;
    const int32_t topicId = 1;

    // Install validator.
    const std::vector<uint8_t> blob =
        rune::codec::serialize(makeCharactersDescriptor());
    h.enqueueSchemaUpdate(topicId, blob);
    h.enqueueWriteSpec(topicId);
    h.tick();
    ASSERT_TRUE(h.worker->hasValidatorForTest(topicId));

    const int64_t leoBefore = h.log->logEndOffset();

    // Produce a well-formed record.
    const auto recVal = makeValidRecordValue(/*id=*/42, /*type=*/1, "Alice");
    auto batch = makeBatchWithValue(recVal);
    h.enqueueProduceBatch(topicId, /*partition=*/0, std::move(batch));
    h.tick();

    EXPECT_EQ(h.lastErrorCode, static_cast<int16_t>(0))
        << "Well-formed record must not be rejected (errorCode must be 0)";
    EXPECT_GT(h.log->logEndOffset(), leoBefore)
        << "Log LEO must advance after a successful append";
}

/// Regression: a malformed record (too short for the fixed block) is rejected
/// with SchemaValidationFailed=24 and NO append.
TEST(ProduceValidation, SchemaUpdate_Regression_MalformedRecord_ValidationFailed) {
    WorkerHarness h;
    const int32_t topicId = 1;

    const std::vector<uint8_t> blob =
        rune::codec::serialize(makeCharactersDescriptor());
    h.enqueueSchemaUpdate(topicId, blob);
    h.enqueueWriteSpec(topicId);
    h.tick();
    ASSERT_TRUE(h.worker->hasValidatorForTest(topicId));

    const int64_t leoBefore = h.log->logEndOffset();

    // Produce a record whose VALUE is truncated (4 bytes — cannot satisfy the
    // 12-byte fixed block of the "characters" descriptor).
    const auto recVal = makeTruncatedValue();
    auto batch = makeBatchWithValue(recVal);
    h.enqueueProduceBatch(topicId, /*partition=*/0, std::move(batch));
    h.tick();

    static constexpr int16_t kSchemaValidationFailed = 24;
    EXPECT_EQ(h.lastErrorCode, kSchemaValidationFailed)
        << "Malformed record must return SchemaValidationFailed=24";
    EXPECT_EQ(h.log->logEndOffset(), leoBefore)
        << "No append must occur when validation fails";
}

/// Regression: a record with an enum value outside the whitelist is rejected.
TEST(ProduceValidation, SchemaUpdate_Regression_BadEnumRecord_ValidationFailed) {
    WorkerHarness h;
    const int32_t topicId = 1;

    const std::vector<uint8_t> blob =
        rune::codec::serialize(makeCharactersDescriptor());
    h.enqueueSchemaUpdate(topicId, blob);
    h.enqueueWriteSpec(topicId);
    h.tick();
    ASSERT_TRUE(h.worker->hasValidatorForTest(topicId));

    const int64_t leoBefore = h.log->logEndOffset();

    // enum value 99 is NOT in {0,1,2} — must be rejected.
    const auto recVal = makeValidRecordValue(/*id=*/1, /*type=*/99, "Bad");
    auto batch = makeBatchWithValue(recVal);
    h.enqueueProduceBatch(topicId, /*partition=*/0, std::move(batch));
    h.tick();

    static constexpr int16_t kSchemaValidationFailed = 24;
    EXPECT_EQ(h.lastErrorCode, kSchemaValidationFailed)
        << "Bad enum value must return SchemaValidationFailed=24";
    EXPECT_EQ(h.log->logEndOffset(), leoBefore)
        << "No append must occur when enum validation fails";
}

/// Regression: a topic with no SchemaUpdate installed must not have its
/// produce blocked — the validation hook must be a single NO-OP branch.
TEST(ProduceValidation, SchemaUpdate_Regression_NoValidator_FallsThrough) {
    WorkerHarness h;
    const int32_t topicId = 1;

    // Do NOT install a validator — no SchemaUpdate.
    ASSERT_FALSE(h.worker->hasValidatorForTest(topicId));

    const int64_t leoBefore = h.log->logEndOffset();

    // Produce anything — should go through.
    const auto recVal = makeValidRecordValue(/*id=*/7, /*type=*/2, "Bob");
    auto batch = makeBatchWithValue(recVal);
    h.enqueueProduceBatch(topicId, /*partition=*/0, std::move(batch));
    h.tick();

    EXPECT_EQ(h.lastErrorCode, static_cast<int16_t>(0))
        << "Produce without a validator must succeed (no blocking)";
    EXPECT_GT(h.log->logEndOffset(), leoBefore)
        << "Log must advance when no validator is installed";
}

// ---------------------------------------------------------------------------
// Task 12: version-keyed validators_ — a record validates against the
// Validator matching ITS OWN writer version (bytes [2..3] of the record
// value), never the topic's other installed version.
// ---------------------------------------------------------------------------

/// Install v1 (id+age) and v2 (id+score, age dropped) validators for the SAME
/// topic.  A v1-stamped record (age layout) validates against v1; a
/// v2-stamped record (score layout) validates against v2; a record stamped
/// v2 but still carrying the RETIRED v1 (age) layout is rejected — the
/// selection is by the record's OWN version, never a same-topic fallback.
TEST(WorkerValidator, ValidatesRecordAgainstItsOwnSchemaVersion) {
    WorkerHarness h;
    const int32_t topicId = 1;

    const Descriptor evolving = makePeopleEvolvingDescriptor();
    const Descriptor v1 = rune::codec::materializeAsOf(evolving, 1);   // {id, age}
    const Descriptor v2 = rune::codec::materializeAsOf(evolving, 2);   // {id, score}

    h.enqueueSchemaUpdate(topicId, rune::codec::serialize(v1), /*version=*/1);
    h.enqueueSchemaUpdate(topicId, rune::codec::serialize(v2), /*version=*/2);
    h.enqueueWriteSpec(topicId);
    h.tick();
    ASSERT_TRUE(h.worker->hasValidatorForTest(topicId, /*version=*/1));
    ASSERT_TRUE(h.worker->hasValidatorForTest(topicId, /*version=*/2));

    // A v1 record (id + age, 12-byte block) stamped version=1 validates OK.
    {
        const int64_t leoBefore = h.log->logEndOffset();
        auto batch = makeBatchWithValue(
            makePeopleRecordValue(/*version=*/1, /*id=*/1, /*extra=*/30, /*extraIsI64=*/false));
        h.enqueueProduceBatch(topicId, /*partition=*/0, std::move(batch));
        h.tick();
        EXPECT_EQ(h.lastErrorCode, static_cast<int16_t>(0))
            << "a v1 record must validate against the v1 (id+age) validator";
        EXPECT_GT(h.log->logEndOffset(), leoBefore);
    }

    // A v2 record (id + score, 16-byte block) stamped version=2 validates OK.
    {
        const int64_t leoBefore = h.log->logEndOffset();
        auto batch = makeBatchWithValue(
            makePeopleRecordValue(/*version=*/2, /*id=*/2, /*extra=*/99, /*extraIsI64=*/true));
        h.enqueueProduceBatch(topicId, /*partition=*/0, std::move(batch));
        h.tick();
        EXPECT_EQ(h.lastErrorCode, static_cast<int16_t>(0))
            << "a v2 record must validate against the v2 (id+score) validator";
        EXPECT_GT(h.log->logEndOffset(), leoBefore);
    }

    // A record stamped version=2 but still carrying the RETIRED v1 (age,
    // 12-byte block) layout is rejected — the v2 validator (blockLength=16)
    // must NOT accept it, and the worker must NOT fall back to the v1
    // validator just because it happens to be installed for the same topic.
    {
        const int64_t leoBefore = h.log->logEndOffset();
        auto batch = makeBatchWithValue(
            makePeopleRecordValue(/*version=*/2, /*id=*/3, /*extra=*/30, /*extraIsI64=*/false));
        h.enqueueProduceBatch(topicId, /*partition=*/0, std::move(batch));
        h.tick();
        static constexpr int16_t kSchemaValidationFailed = 24;
        EXPECT_EQ(h.lastErrorCode, kSchemaValidationFailed)
            << "a v2-stamped record with the dropped-age v1 layout must be rejected";
        EXPECT_EQ(h.log->logEndOffset(), leoBefore)
            << "no append when the stamped version's own validator rejects the record";
    }
}

// Regression: a record stamped with a version that has NO installed validator
// (even though OTHER versions of the same topic DO have one) is rejected as
// an unknown schema version — never validated against whichever version
// happens to be installed.
TEST(ProduceValidationRegression, UnknownSchemaVersionRejectedRegression) {
    WorkerHarness h;
    const int32_t topicId = 1;

    // Only v1 is installed.
    h.enqueueSchemaUpdate(topicId, rune::codec::serialize(makeCharactersDescriptor()),
                         /*version=*/1);
    h.enqueueWriteSpec(topicId);
    h.tick();
    ASSERT_TRUE(h.worker->hasValidatorForTest(topicId, /*version=*/1));
    ASSERT_FALSE(h.worker->hasValidatorForTest(topicId, /*version=*/2));

    const int64_t leoBefore = h.log->logEndOffset();

    // A record stamped version=2 — no v2 validator was ever pushed.
    std::vector<uint8_t> recVal = makeValidRecordValue(/*id=*/1, /*type=*/1, "Eve");
    recVal[2] = 2; recVal[3] = 0;   // overwrite the version header field: 1 -> 2
    auto batch = makeBatchWithValue(recVal);
    h.enqueueProduceBatch(topicId, /*partition=*/0, std::move(batch));
    h.tick();

    // Checked writes (P2): a version ABOVE the highest installed one is retriable (its SchemaUpdate may still be on
    // the way); it is never validated against another version.
    static constexpr int16_t kNotLeader = 2;
    EXPECT_EQ(h.lastErrorCode, kNotLeader)
        << "a version above the installed ones is retriable, never validated against "
           "whichever other version happens to be installed";
    EXPECT_EQ(h.log->logEndOffset(), leoBefore)
        << "no append for an unknown schema version";
}

// A version BELOW the highest installed one that has no entry stays a definite rejection.
TEST(ProduceValidationRegression, UnknownLowerSchemaVersionIsDefiniteRegression) {
    WorkerHarness h;
    const int32_t topicId = 1;
    h.enqueueSchemaUpdate(topicId, rune::codec::serialize(makeCharactersDescriptor()), /*version=*/2);
    h.enqueueWriteSpec(topicId);
    h.tick();
    ASSERT_FALSE(h.worker->hasValidatorForTest(topicId, /*version=*/1));

    auto batch = makeBatchWithValue(makeValidRecordValue(/*id=*/1, /*type=*/1, "Eve"));   // stamped version 1
    h.enqueueProduceBatch(topicId, /*partition=*/0, std::move(batch));
    h.tick();

    static constexpr int16_t kSchemaValidationFailed = 24;
    EXPECT_EQ(h.lastErrorCode, kSchemaValidationFailed);
}

// ---------------------------------------------------------------------------
// GC3 (C51-55, N14-18): hot-path mixed-version batch -- a SINGLE RecordBatch
// containing records of DIFFERENT writer versions (a producer that evolved
// mid-batch, or a replay that stitches versions together). Each record must
// be validated against its OWN stamped version's validator -- the per-record
// (topicId, version) lookup in schemaRejects_ (Worker.h), never a single
// topic-wide/latest validator.
// ---------------------------------------------------------------------------
TEST(ProduceValidation, MixedVersionBatch_Regression_EachRecordValidatesAgainstItsOwnVersion) {
    WorkerHarness h;
    const int32_t topicId = 1;

    const Descriptor evolving = makePeopleEvolvingDescriptor();
    const Descriptor v1 = rune::codec::materializeAsOf(evolving, 1);   // {id, age}
    const Descriptor v2 = rune::codec::materializeAsOf(evolving, 2);   // {id, score}
    h.enqueueSchemaUpdate(topicId, rune::codec::serialize(v1), /*version=*/1);
    h.enqueueSchemaUpdate(topicId, rune::codec::serialize(v2), /*version=*/2);
    h.enqueueWriteSpec(topicId);
    h.tick();
    ASSERT_TRUE(h.worker->hasValidatorForTest(topicId, 1));
    ASSERT_TRUE(h.worker->hasValidatorForTest(topicId, 2));

    // A single batch mixing a valid v1 record and a valid v2 record. No
    // cross-version false reject: both are well-formed under their OWN
    // stamped version, so the whole batch must be accepted and appended.
    {
        const int64_t leoBefore = h.log->logEndOffset();
        auto v1rec = makePeopleRecordValue(/*version=*/1, /*id=*/1, /*extra=*/30, /*extraIsI64=*/false);
        auto v2rec = makePeopleRecordValue(/*version=*/2, /*id=*/2, /*extra=*/99, /*extraIsI64=*/true);
        auto batch = makeBatchWithValues({v1rec, v2rec});
        h.enqueueProduceBatch(topicId, /*partition=*/0, std::move(batch));
        h.tick();
        EXPECT_EQ(h.lastErrorCode, static_cast<int16_t>(0))
            << "a batch mixing valid v1 and v2 records must not be rejected";
        EXPECT_EQ(h.log->logEndOffset(), leoBefore + 2)
            << "both records of the mixed-version batch must be appended";
    }

    // Same batch shape, but the v2-stamped record carries the RETIRED v1
    // (age) layout. The v-keyed lookup must catch THIS specific record even
    // though the other (v1) record in the SAME batch is perfectly valid --
    // proof the per-record version key is actually used, not a single
    // version check hoisted for the whole batch. The batch is atomic: one
    // bad record rejects the whole batch, nothing partially appends.
    {
        const int64_t leoBefore = h.log->logEndOffset();
        auto v1rec   = makePeopleRecordValue(/*version=*/1, /*id=*/3, /*extra=*/31, /*extraIsI64=*/false);
        auto badV2rec = makePeopleRecordValue(/*version=*/2, /*id=*/4, /*extra=*/32, /*extraIsI64=*/false);
        auto batch = makeBatchWithValues({v1rec, badV2rec});
        h.enqueueProduceBatch(topicId, /*partition=*/0, std::move(batch));
        h.tick();
        static constexpr int16_t kSchemaValidationFailed = 24;
        EXPECT_EQ(h.lastErrorCode, kSchemaValidationFailed)
            << "a mixed-version batch with one mis-versioned record must be rejected as a whole";
        EXPECT_EQ(h.log->logEndOffset(), leoBefore)
            << "no partial append -- batch validation is atomic";
    }
}

// ---------------------------------------------------------------------------
// GC3 (N9-13): forward-read at the worker. A worker has installed the
// validator for vK ONLY (v(K+1)'s ALTER has not yet been fanned out to this
// worker). It is handed a genuinely well-formed v(K+1) record -- a field
// ("score") that was added AFTER vK. Per schemaRejects_ (Worker.h, the
// per-record lookup keyed by the record's own stamped (topicId, version)):
// an unknown version is a HARD REJECT ("unknown schema version") -- it is
// NEVER validated against whichever OTHER version happens to be installed
// for the same topic, even a strict-prefix older one. This matches (and
// pins) the exact production behavior read from Worker.h's schemaRejects_.
// ---------------------------------------------------------------------------
TEST(ProduceValidationRegression, ForwardReadRecordNewerThanInstalledValidatorRejectedRegression) {
    WorkerHarness h;
    const int32_t topicId = 1;

    const Descriptor evolving = makePeopleEvolvingDescriptor();
    const Descriptor v1 = rune::codec::materializeAsOf(evolving, 1);   // {id, age}

    // Only v1 is installed -- the worker has not received v2's (id+score)
    // validator fan-out yet.
    h.enqueueSchemaUpdate(topicId, rune::codec::serialize(v1), /*version=*/1);
    h.enqueueWriteSpec(topicId);
    h.tick();
    ASSERT_TRUE(h.worker->hasValidatorForTest(topicId, 1));
    ASSERT_FALSE(h.worker->hasValidatorForTest(topicId, 2));

    const int64_t leoBefore = h.log->logEndOffset();

    // A record stamped v2, using v2's OWN (id+score, 16-byte) layout -- a
    // genuinely well-formed v2 record; it simply arrives at a worker that
    // doesn't know v2 yet.
    auto batch = makeBatchWithValue(
        makePeopleRecordValue(/*version=*/2, /*id=*/9, /*extra=*/42, /*extraIsI64=*/true));
    h.enqueueProduceBatch(topicId, /*partition=*/0, std::move(batch));
    h.tick();

    static constexpr int16_t kNotLeader = 2;   // checked writes (P2): retriable until the v2 SchemaUpdate lands
    EXPECT_EQ(h.lastErrorCode, kNotLeader)
        << "forward-read: a well-formed v2 record at a worker with only the v1 validator "
           "installed is retriable, never accepted or mis-validated against the v1 validator";
    EXPECT_EQ(h.log->logEndOffset(), leoBefore)
        << "no append for a record whose stamped version has no installed validator";
}

// Checked writes: a typed partition's validation allocates nothing, accepted or refused (valid, invalid UTF-8,
// version-ahead), at the checked level.
TEST(CheckedWritesValidationRegression, TypedProduceAllocatesNothingRegression) {
    WorkerHarness h;
    const int32_t topicId = 1;
    h.enqueueSchemaUpdate(topicId, rune::codec::serialize(makeCharactersDescriptor()));
    h.enqueueWriteSpec(topicId);
    WorkerOp lvl;
    lvl.type = WorkerOp::Type::ClusterFeatureLevel;
    lvl.nextOffset = rune::replication::MetadataFeature::kCheckedWrites;
    ASSERT_TRUE(h.worker->tryEnqueueOp(std::move(lvl)));
    h.tick();
    ASSERT_NE(h.worker->writeStateForTest(topicId, 0), nullptr);

    const auto valid = makeValidRecordValue(1, 1, "Alice");
    auto badUtf8 = makeValidRecordValue(1, 1, "Alice");
    badUtf8[badUtf8.size() - 1] = 0xFF;                        // an invalid UTF-8 byte in the string
    auto ahead = makeValidRecordValue(1, 1, "Alice");
    ahead[2] = 9; ahead[3] = 0;                                // a version no SchemaUpdate has installed
    const std::vector<uint8_t>* kinds[] = {&valid, &badUtf8, &ahead};

    constexpr int kWarm = 200, kMeasured = 1000;
    std::vector<WorkerOp> ops;
    ops.reserve(kWarm + kMeasured);
    const auto makeOp = [&](int i) {
        WorkerOp op;
        op.type = WorkerOp::Type::Produce; op.topicId = topicId; op.partition = 0; op.replica = h.replica.get();
        op.sessionId = 1; op.correlationId = i + 1; op.durabilityTier = 0;
        op.recordBatchBytes = makeBatchWithValue(*kinds[i % 3]);
        return op;
    };
    for (int i = 0; i < kWarm + kMeasured; ++i) ops.push_back(makeOp(i));
    int64_t nowNs = 1;
    const auto run = [&](int from, int to) {
        for (int i = from; i < to; ++i) {
            ASSERT_TRUE(h.worker->tryEnqueueOp(std::move(ops[static_cast<std::size_t>(i)])));
            h.worker->doWork(nowNs += 1'000'000LL);
            h.worker->drainReturn([&](WorkerReturn) {});
        }
    };
    run(0, kWarm);
    rune::testing::AllocWindow window;
    run(kWarm, kWarm + kMeasured);
    EXPECT_EQ(window.count(), 0u);
    EXPECT_GT(h.worker->typedRejectsForTest(), 0u);
    EXPECT_GT(h.worker->typedVersionAheadForTest(), 0u);
}

// Validation runs after dedupe (SD2): a sequence problem is answered before the batch is judged, a refused batch does not
// consume its sequence, and a duplicate retry never reaches the hook.
namespace {
std::vector<uint8_t> idempotentBatch(const std::vector<uint8_t>& recValue, int64_t pid, int32_t seq) {
    RecordBatchBuilder b(0, 1000);
    b.setProducerId(pid); b.setProducerEpoch(0); b.setBaseSequence(seq);
    b.append(std::string_view{}, std::string_view(reinterpret_cast<const char*>(recValue.data()), recValue.size()), 1000);
    return b.build();
}
struct TypedHarness : WorkerHarness {
    TypedHarness() {
        enqueueSchemaUpdate(1, rune::codec::serialize(makeCharactersDescriptor()));
        enqueueWriteSpec(1);
        WorkerOp lvl;
        lvl.type = WorkerOp::Type::ClusterFeatureLevel;
        lvl.nextOffset = rune::replication::MetadataFeature::kCheckedWrites;
        EXPECT_TRUE(worker->tryEnqueueOp(std::move(lvl)));
        tick();
    }
    int16_t send(std::vector<uint8_t> batch, int64_t corr, int64_t pid, int32_t seq) {
        enqueueProduceBatch(1, 0, std::move(batch), corr, 1, pid, seq);
        tick();
        return lastErrorCode;
    }
};
}  // namespace

TEST(CheckedWritesValidationRegression, ValidationRunsAfterDedupeRegression) {
    TypedHarness h;
    auto invalid = makeValidRecordValue(1, 1, "Alice");
    invalid[invalid.size() - 1] = 0xFF;
    const auto valid = makeValidRecordValue(1, 1, "Alice");
    EXPECT_EQ(h.send(idempotentBatch(invalid, 11, 5), 1, 11, 5), static_cast<int16_t>(rune::protocol::client::ErrorCode::UnknownProducerId))
        << "a new producer that does not start at 0 is answered before the batch is judged";
    EXPECT_EQ(h.send(idempotentBatch(invalid, 11, 0), 2, 11, 0), static_cast<int16_t>(rune::protocol::client::ErrorCode::SchemaValidationFailed));
    EXPECT_EQ(h.log->logEndOffset(), 0);
    EXPECT_EQ(h.send(idempotentBatch(valid, 11, 0), 3, 11, 0), 0) << "the refusal did not consume the sequence";
    EXPECT_EQ(h.log->logEndOffset(), 1);
    EXPECT_EQ(h.send(idempotentBatch(invalid, 11, 5), 4, 11, 5), static_cast<int16_t>(rune::protocol::client::ErrorCode::OutOfOrderSequence))
        << "a gap after an accepted batch is out of order, whatever the bytes say";
}

TEST(CheckedWritesValidationRegression, DuplicateRetryNeverRunsTheHookRegression) {
    TypedHarness h;
    const auto valid = makeValidRecordValue(1, 1, "Alice");
    EXPECT_EQ(h.send(idempotentBatch(valid, 11, 0), 1, 11, 0), 0);
    EXPECT_EQ(h.send(idempotentBatch(valid, 11, 0), 2, 11, 0), 0);
    EXPECT_EQ(h.worker->hookChecksForTest(), 1u);
    EXPECT_EQ(h.log->logEndOffset(), 1);
}

TEST(CheckedWritesValidation, UntypedTopicNeverBuildsAHook) {
    WorkerHarness h;
    for (int i = 0; i < 100; ++i) {
        h.enqueueProduceBatch(1, 0, makeBatchWithValue(makeValidRecordValue(1, 1, "Alice")), i + 1);
        h.tick();
    }
    EXPECT_EQ(h.log->logEndOffset(), 100);
    EXPECT_EQ(h.worker->hookChecksForTest(), 0u);
}

} // namespace
// ===========================================================================
// F3 round 6, item 3: CONFIRMED data race fix.  The AdminWorker (ADMIN thread)
// used to call worker.tryEnqueueOp() directly from deps.pushValidators — a
// SECOND producer on each worker's SPSC ingress ring (the control thread is
// the first): ring corruption + a race on the worker's producer-side state.
// Now the admin thread only posts on its admin->control ring (T10's ring,
// extended with a SchemaUpdate message) and the CONTROL thread fans out to
// the workers (the single producer is preserved).  Deterministic proof via a
// thread-id test hook: every worker-ring enqueue happens on the control
// thread, never the admin thread — while the control thread is concurrently
// producing ordinary ops to the same worker ring.  (The TSan proof is
// deferred to the Task-16 TSan run: build-fast is not TSan-instrumented.)
// ===========================================================================
TEST(ProduceValidationRegression, SchemaUpdateEnqueuedOnlyByControlThreadRegression) {
    const Descriptor desc = makeCharactersDescriptor();
    const std::vector<uint8_t> blob = rune::codec::serialize(desc);

    SchemaRegistry sreg;
    rune::node::AdminWorkerAgent agent(sreg, /*selfId=*/1, rune::node::AdminDeps{});
    rune::metadata::ShardRouter router(2);
    rune::node::WorkerPool pool(2, router, /*ingressRingBytes=*/1 << 20);
    rune::node::SchemaFanout fanout;
    // Test-only thread-identity capture (null in production — item 5 of round 7).
    struct EnqueueThreads {
        std::thread::id first{};
        bool            foreign = false;
        static void hook(void* ctx) noexcept {
            auto* t = static_cast<EnqueueThreads*>(ctx);
            const auto id = std::this_thread::get_id();
            if (t->first == std::thread::id{}) t->first = id;
            else if (t->first != id) t->foreign = true;
        }
    } threads;
    fanout.setEnqueueHookForTest(&EnqueueThreads::hook, &threads);

    // No std::atomic / mutex (Rune rule): completion is the message count the
    // control thread drains off the ring (ring-only synchronisation).
    constexpr int kTopics = 50;
    std::thread::id adminId, controlId;
    std::thread admin([&] {
        adminId = std::this_thread::get_id();
        // Catalog history Task 9: validators reach the control thread as the
        // applier's catalog effects (the production sink's postCatalog_ path).
        for (int t = 0; t < kTopics; ++t) {
            std::vector<uint8_t> frame(rune::node::schemaUpdateWireLen(blob.size()));
            rune::node::writeSchemaUpdate(frame.data(), 100 + t, /*version=*/1, blob);
            agent.postCatalogFrameForTest(rune::node::kA2cSchemaUpdate, frame);
        }
        while (agent.catalogBacklogForTest() != 0) agent.flushCatalogBacklogForTest();
    });
    std::thread control([&] {
        controlId = std::this_thread::get_id();
        int produced = 0;
        int received = 0;
        for (;;) {
            received += agent.failureRing().drain([&](int32_t tid, aeron::concurrent::AtomicBuffer& b,
                                          aeron::util::index_t off, aeron::util::index_t len) {
                ASSERT_EQ(tid, rune::node::kA2cSchemaUpdate);
                const auto m = rune::node::decodeSchemaUpdate(
                    reinterpret_cast<const uint8_t*>(b.buffer() + off), static_cast<std::size_t>(len));
                fanout.post(m.topicId, m.version, m.blob);
            }, 16);
            (void)fanout.pump(pool);
            if (received == kTopics && fanout.pendingForTest() == 0) break;
            // Concurrent ordinary produce-path traffic from the SAME (control) thread.
            // (A Produce with no log → the worker answers an error return.)
            WorkerOp op;
            op.type = WorkerOp::Type::Produce;
            op.recordBatchBytes.assign(16, 0x5);
            if (static_cast<bool>(pool.worker(produced % 2).tryEnqueueOp(std::move(op)))) ++produced;
            for (int w = 0; w < 2; ++w) {                 // consumer side (this test's worker owner)
                (void)pool.worker(w).doWork(1);
                pool.worker(w).drainReturn([](const WorkerReturn&) {});
            }
        }
    });
    admin.join();
    control.join();
    for (int w = 0; w < 2; ++w) {
        (void)pool.worker(w).doWork(1);
        pool.worker(w).drainReturn([](const WorkerReturn&) {});
    }

    EXPECT_EQ(fanout.enqueuedForTest(), static_cast<uint64_t>(kTopics * 2));
    EXPECT_EQ(threads.first, controlId)
        << "SchemaUpdate must be enqueued into worker rings by the CONTROL thread";
    EXPECT_NE(threads.first, adminId)
        << "the admin thread must never produce into a worker SPSC ring";
    EXPECT_FALSE(threads.foreign) << "every enqueue on one thread";
    for (int w = 0; w < 2; ++w) {
        for (int t = 0; t < kTopics; ++t)
            EXPECT_TRUE(pool.worker(w).hasValidatorForTest(100 + t)) << "worker " << w << " topic " << t;
    }
}

// Retry-on-full is preserved (the control thread keeps the update pending and
// retries next cycle), and an update that can NEVER fit is dropped with an
// error naming the size (TooLarge), not retried forever as "ring full".
TEST(ProduceValidationRegression, SchemaFanoutRetriesOnFullDropsTooLargeRegression) {
    const std::vector<uint8_t> blob = rune::codec::serialize(makeCharactersDescriptor());
    rune::metadata::ShardRouter router(1);
    rune::node::WorkerPool pool(1, router, /*ingressRingBytes=*/64 * 1024);
    rune::node::SchemaFanout fanout;

    // Fill worker 0's ring until Full.
    for (int i = 0; i < 10000; ++i) {
        WorkerOp op;
        op.type = WorkerOp::Type::Produce;
        op.recordBatchBytes.assign(pool.maxIngressTail(), 0x1);
        if (!pool.worker(0).tryEnqueueOp(std::move(op))) break;
    }
    for (int i = 0; i < 10000; ++i) {   // top off: no gap left for a blob-sized op
        WorkerOp op;
        op.type = WorkerOp::Type::Produce;
        op.recordBatchBytes.assign(blob.size(), 0x1);
        if (!pool.worker(0).tryEnqueueOp(std::move(op))) break;
    }
    fanout.post(7, /*version=*/1, blob);
    (void)fanout.pump(pool);
    EXPECT_EQ(fanout.pendingForTest(), 1u) << "ring full → kept pending (retry), not dropped";
    auto drainWorker = [&] {   // bounded: the ring holds a finite number of ops
        for (int i = 0; i < 1000; ++i) {
            (void)pool.worker(0).doWork(1);
            pool.worker(0).drainReturn([](const WorkerReturn&) {});
        }
    };
    drainWorker();
    (void)fanout.pump(pool);
    EXPECT_EQ(fanout.pendingForTest(), 0u) << "retried and delivered next cycle";
    drainWorker();
    EXPECT_TRUE(pool.worker(0).hasValidatorForTest(7));

    fanout.post(8, /*version=*/1, std::vector<uint8_t>(pool.maxIngressTail() + 1, 0x2));
    (void)fanout.pump(pool);
    EXPECT_EQ(fanout.pendingForTest(), 0u) << "TooLarge is dropped (error names the size)";
    EXPECT_EQ(fanout.tooLargeDropsForTest(), 1u);
}

// F3 round 7, item 3: the admin→control ring is dispatched on the message
// type EXPLICITLY.  Before: any non-SchemaUpdate frame was decoded as a
// storage failure — an unknown type (or a <4-byte frame) would have been
// misread as an fsync failure and HALTED the broker.
TEST(ProduceValidationRegression, AdminToControlDispatchIsExplicitRegression) {
    int failures = 0, schemas = 0;
    int32_t gotErr = 0; std::string gotDir; int32_t gotTopic = -1;
    auto onFailure = [&](int32_t e, std::string_view d) { ++failures; gotErr = e; gotDir = d; };
    auto onSchema  = [&](rune::node::SchemaUpdateMsg m) { ++schemas; gotTopic = m.topicId; };

    std::vector<uint8_t> fail(4 + 3);
    const int32_t eio = 5; std::memcpy(fail.data(), &eio, 4); std::memcpy(fail.data() + 4, "dir", 3);
    EXPECT_EQ(rune::node::dispatchAdminToControl(rune::node::kA2cStorageFailure, fail.data(), fail.size(),
                                                 onFailure, onSchema),
              rune::node::A2cDispatch::StorageFailure);
    EXPECT_EQ(failures, 1); EXPECT_EQ(gotErr, 5); EXPECT_EQ(gotDir, "dir");

    std::vector<uint8_t> su(4 + 2, 0x7); const int32_t tid = 42; std::memcpy(su.data(), &tid, 4);
    EXPECT_EQ(rune::node::dispatchAdminToControl(rune::node::kA2cSchemaUpdate, su.data(), su.size(),
                                                 onFailure, onSchema),
              rune::node::A2cDispatch::SchemaUpdate);
    EXPECT_EQ(schemas, 1); EXPECT_EQ(gotTopic, 42);

    // Unknown type: skipped — NEVER treated as a storage failure.
    EXPECT_EQ(rune::node::dispatchAdminToControl(99, fail.data(), fail.size(), onFailure, onSchema),
              rune::node::A2cDispatch::Unknown);
    // Short frames: rejected, never decoded.
    EXPECT_EQ(rune::node::dispatchAdminToControl(rune::node::kA2cStorageFailure, fail.data(), 3,
                                                 onFailure, onSchema),
              rune::node::A2cDispatch::Malformed);
    EXPECT_EQ(rune::node::dispatchAdminToControl(rune::node::kA2cSchemaUpdate, su.data(), 3,
                                                 onFailure, onSchema),
              rune::node::A2cDispatch::Malformed);
    EXPECT_EQ(failures, 1) << "no halt from an unknown/short frame";
    EXPECT_EQ(schemas, 1);
}

// F3 round 7, item 4: the largest descriptor the request ring can deliver
// (DefineTopic with empty name/key) actually round-trips through BOTH rings.
TEST(ProduceValidationRegression, MaxDescriptorFitsBothAdminRingsRegression) {
    rune::node::OneToOneByteRing request(rune::node::kAdminRequestRingBytes);
    rune::node::OneToOneByteRing a2c(rune::node::kAdminToControlRingBytes);
    rune::node::DefineTopicMsg m;
    m.blob.assign(rune::node::kMaxDescriptorViaRequestRing, 0x5A);
    EXPECT_TRUE(rune::node::encodeDefineTopic(request, m));
    EXPECT_TRUE(rune::node::encodeSchemaUpdate(a2c, 1, /*version=*/1, m.blob))
        << "a descriptor accepted by the request ring must fit the admin->control ring";
}

} // namespace rune::node::test
