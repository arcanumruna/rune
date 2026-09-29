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
#include <rune/node/WorkerOp.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Validator.h>

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
    void sendFetchRequest(const rune::replication::FetchRequestMsg&) override {}
    void sendFetchResponse(rune::replication::NodeId,
                           const rune::replication::FetchResponseMsg&) override {}
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

/// An EMPTY record value (too short to satisfy any descriptor).
static std::vector<uint8_t> makeEmptyValue() { return {}; }

/// A truncated value — shorter than the fixed block.
static std::vector<uint8_t> makeTruncatedValue() {
    // blockLength=12, version=1, then only 3 bytes (too short for the 12-byte block).
    return {12, 0, 1, 0, 0xFF, 0xFF, 0xFF};
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

    /// Enqueue a SchemaUpdate op for the given topicId + descriptor blob.
    void enqueueSchemaUpdate(int32_t topicId,
                             const std::vector<uint8_t>& blob) {
        WorkerOp op;
        op.type             = WorkerOp::Type::SchemaUpdate;
        op.topicId          = topicId;
        op.schemaUpdateBlob = blob;
        ASSERT_TRUE(worker->tryEnqueueOp(std::move(op)))
            << "SchemaUpdate enqueue failed (ring full)";
    }

    /// Enqueue a Produce op carrying `batch` for (topicId, partition).
    void enqueueProduceBatch(int32_t topicId, int32_t partition,
                             std::vector<uint8_t> batch,
                             int64_t correlationId = 1,
                             int64_t sessionId = 1) {
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

} // namespace
} // namespace rune::node::test
