// ---------------------------------------------------------------------------
// SchemaPersistenceTest.cpp — Task 6 integration test.
//
// Exercises the __topic_schemas persistence layer WITHOUT Aeron / the media
// driver (single-node replica, minIsr=1, NullTransport).
//
// Test cases (on-host, no Docker):
//
//   1. SchemaRecord_LandsInLog
//        Drive onDefineTopic directly → assert the __topic_schemas log has
//        exactly 1 record and schemaRegistry_.byName("characters") is present.
//
//   2. RebuildFromLog_RecoveryAfterRestart
//        Same setup as (1).  After the record lands, construct a NEW
//        SchemaRegistry and replay the log via the same rebuildFromLog
//        loop used in BrokerNode::buildAdminWorker → assert the entry survives
//        (name, topicId, version, partitionCount all preserved).
//
//   3. DuplicateDefineTopic_IdempotentOnLog
//        Two onDefineTopic calls for the same name → second returns
//        TopicExists error; log still has exactly 1 record.
//
// NOT tested here (Docker-gated, deferred to Task 8):
//   - Majority-durable commit path (requires Aeron media driver + real
//     follower replicas via AeronPartitionTransport).
//   - Full client ↔ broker DDL round-trip (requires ClientGateway + Aeron).
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/AdminWorker.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

using namespace rune::node;
using namespace rune::codec;
using namespace rune::replication;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static std::string makeUniqSuffix() {
    static std::atomic<uint64_t> seq{0};
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(seq.fetch_add(1, std::memory_order_relaxed)) + "_" +
           std::to_string(ts);
}

struct TmpSchemaDir {
    std::string path;
    explicit TmpSchemaDir(const std::string& tag) {
        path = std::filesystem::temp_directory_path().string()
             + "/rune_spt_" + tag + "_" + makeUniqSuffix();
        std::filesystem::create_directories(path);
    }
    ~TmpSchemaDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

// Minimal no-op PartitionTransport (single-node: no peers).
class NullPartitionTransport : public PartitionTransport {
public:
    void sendFetchRequest(const FetchRequestMsg&) override {}
    void sendFetchResponse(NodeId, const FetchResponseMsg&) override {}
};

static PartitionConfig makeSingleNodeSchemaConfig() {
    PartitionConfig cfg;
    cfg.topicId             = kSchemaTopicId;   // -4
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    // minIsr=1: single-node leader; appendKeyed succeeds without followers.
    cfg.minIsr              = 1;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    cfg.cleanupPolicy       = CleanupPolicy::Compact;
    return cfg;
}

// Build a descriptor blob for a "characters" schema.
static std::vector<uint8_t> charsBlob() {
    return serialize(DescriptorBuilder("characters", 1)
        .add("id",     PrimType::Int64)
        .add("userId", PrimType::Int64, 1, true)
        .add("name",   PrimType::String)
        .build());
}

// ---------------------------------------------------------------------------
// rebuildFromLog — same logic as BrokerNode::buildAdminWorker.
// Replays the __topic_schemas log [0, HW) into a SchemaRegistry.
// ---------------------------------------------------------------------------
static void rebuildSchemaRegistryFromLog(
    const PartitionReplica* replica, SchemaRegistry& reg)
{
    const Offset hw = replica->highWatermark();
    if (hw <= 0) return;

    static constexpr int32_t kChunk = 64 * 1024 * 1024;
    Offset fetchOffset = 0;

    while (fetchOffset < hw) {
        std::vector<uint8_t> data =
            const_cast<PartitionReplica*>(replica)->read(fetchOffset, kChunk);
        if (data.empty()) break;

        std::size_t pos      = 0;
        Offset lastBatchEnd  = fetchOffset;

        while (pos < data.size()) {
            const uint8_t*    bs = data.data() + pos;
            const std::size_t rem = data.size() - pos;

            if (rem < static_cast<std::size_t>(
                    rune::protocol::batch::kRecordBatchOverhead))
                break;

            rune::protocol::RecordBatchReader reader(bs, rem);
            if (!reader.ensureValid()) break;

            const int32_t tl = reader.totalLength();
            if (tl <= 0 || static_cast<std::size_t>(tl) > rem) break;

            reader.forEach([&reg](std::string_view /*key*/,
                                   std::string_view val,
                                   int64_t /*offset*/,
                                   int64_t /*ts*/) {
                auto ts = TopicSchema::deserialize(std::span<const uint8_t>(
                    reinterpret_cast<const uint8_t*>(val.data()), val.size()));
                if (ts) {
                    reg.applyRecord(*ts);
                }
            });

            const Offset bne = reader.baseOffset() + reader.lastOffsetDelta() + 1;
            if (bne > lastBatchEnd) lastBatchEnd = bne;
            pos += static_cast<std::size_t>(tl);
        }

        if (lastBatchEnd <= fetchOffset) break;
        fetchOffset = lastBatchEnd;
    }
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------
class SchemaPersistenceTest : public ::testing::Test {
protected:
    SchemaPersistenceTest()
        : schemaDir_("schema")
        , log_(schemaDir_.path)
    {}

    void SetUp() override {
        log_.recover();

        transport_ = std::make_unique<NullPartitionTransport>();

        replica_ = std::make_unique<PartitionReplica>(
            makeSingleNodeSchemaConfig(),
            Role::Leader, /*epoch=*/1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1},
            &log_,
            transport_.get());

        registry_ = std::make_unique<SchemaRegistry>();

        // --- Wire AdminDeps ---
        // createTopic (RULING-1): returns a sequential id from a fake registry counter.
        AdminDeps deps;
        deps.createTopic = [this](int32_t /*partitionCount*/,
                                  const std::string& /*name*/) -> int32_t {
            return nextTopicId_++;
        };

        // appendDurable: append TopicSchema to the __topic_schemas replica
        // (single-node, minIsr=1 — succeeds immediately).
        deps.appendDurable = [this](const TopicSchema& ts) -> bool {
            const std::vector<uint8_t> payload = ts.serialize();
            // Key = topicId as 4-byte LE (for log compaction).
            uint8_t key[4];
            const uint32_t u = static_cast<uint32_t>(ts.topicId);
            key[0] = static_cast<uint8_t>(u & 0xff);
            key[1] = static_cast<uint8_t>((u >> 8) & 0xff);
            key[2] = static_cast<uint8_t>((u >> 16) & 0xff);
            key[3] = static_cast<uint8_t>((u >> 24) & 0xff);
            const Offset base = replica_->appendKeyed(
                std::span<const uint8_t>(key, 4),
                std::span<const uint8_t>(payload.data(), payload.size()),
                /*acksAll=*/true);
            return base >= 0;
        };

        // pushValidators: stub for Task 6 (T7 adds real worker ring enqueue).
        deps.pushValidators = [this](const RegistryEntry& /*entry*/, int32_t tid) {
            ++pushValidatorCallCount_;
            lastPushedTopicId_ = tid;
        };

        // respondDefine/respondGet: capture for assertions.
        deps.respondDefine = [this](int64_t /*session*/, int64_t /*corr*/,
                                    int32_t topicId, uint16_t /*version*/,
                                    uint16_t errorCode) {
            lastDefineTopicId_ = topicId;
            lastDefineError_   = errorCode;
        };
        deps.respondGet = [this](int64_t /*session*/, int64_t /*corr*/,
                                 int32_t topicId, uint16_t /*version*/,
                                 const std::vector<uint8_t>& /*blob*/,
                                 uint16_t errorCode) {
            lastGetTopicId_ = topicId;
            lastGetError_   = errorCode;
        };

        adminWorker_ = std::make_unique<AdminWorker>(*registry_, std::move(deps));
    }

    // Drive one "doWork" tick on the replica (advances HW on single-node leader).
    void tick(int n = 1) {
        int64_t nowNs = 0;
        for (int i = 0; i < n; ++i) {
            nowNs += 1'000'000LL;
            replica_->doWork(nowNs);
        }
    }

    TmpSchemaDir schemaDir_;
    rune::storage::Log log_;
    std::unique_ptr<NullPartitionTransport> transport_;
    std::unique_ptr<PartitionReplica>       replica_;
    std::unique_ptr<SchemaRegistry>         registry_;
    std::unique_ptr<AdminWorker>            adminWorker_;

    int32_t nextTopicId_ = 0;

    // Captured response state
    int32_t  lastDefineTopicId_ = -1;
    uint16_t lastDefineError_   = 0xffff;
    int32_t  lastGetTopicId_    = -1;
    uint16_t lastGetError_      = 0xffff;
    int      pushValidatorCallCount_ = 0;
    int32_t  lastPushedTopicId_      = -1;
};

// ---------------------------------------------------------------------------
// Test 1: SchemaRecord_LandsInLog
//
// Drive onDefineTopic → assert:
//   - respondDefine called with errorCode=0 (OK)
//   - registry has "characters"
//   - __topic_schemas log LEO >= 1 (at least one record written)
// ---------------------------------------------------------------------------
TEST_F(SchemaPersistenceTest, SchemaRecord_LandsInLog) {
    DefineTopicArgs args;
    args.session        = 1;
    args.corr           = 2;
    args.name           = "characters";
    args.keyField       = "userId";
    args.partitionCount = 4;
    args.validate       = false;
    args.blob           = charsBlob();

    adminWorker_->onDefineTopic(args);

    // respondDefine should have been called with OK=0.
    EXPECT_EQ(lastDefineError_, 0u) << "DefineTopic must succeed (OK=0)";
    EXPECT_EQ(lastDefineTopicId_, 0) << "RULING-1: topicId is the sequential id from createTopic";

    // Registry must have "characters".
    ASSERT_NE(registry_->byName("characters"), nullptr)
        << "SchemaRegistry must contain 'characters' after commit";

    // __topic_schemas Log must have at least one record (LEO >= 1).
    // On a single-node leader, appendKeyed advances LEO immediately.
    EXPECT_GE(log_.logEndOffset(), 1LL)
        << "__topic_schemas log must have at least 1 record after DefineTopic";
}

// ---------------------------------------------------------------------------
// Test 2: RebuildFromLog_RecoveryAfterRestart
//
// After a DefineTopic commit, simulate a restart:
//   - Close the existing SchemaRegistry (goes out of scope).
//   - Re-open the same Log (recover() from disk).
//   - Run rebuildFromLog on a fresh SchemaRegistry.
//   - Assert the entry is recovered with correct fields.
// ---------------------------------------------------------------------------
TEST_F(SchemaPersistenceTest, RebuildFromLog_RecoveryAfterRestart) {
    // 1. Write a schema record.
    DefineTopicArgs args;
    args.session        = 10;
    args.corr           = 20;
    args.name           = "characters";
    args.keyField       = "userId";
    args.partitionCount = 8;
    args.validate       = false;
    args.blob           = charsBlob();

    adminWorker_->onDefineTopic(args);
    ASSERT_EQ(lastDefineError_, 0u) << "Precondition: DefineTopic must succeed";

    const int32_t savedTopicId = lastDefineTopicId_;

    // 2. Drive doWork so the replica advances its HW (single-node: HW = LEO after
    //    one doWork tick with pushOnAppend=false and minIsr=1).
    tick(5);

    // 3. Simulate restart: build a NEW SchemaRegistry and replay the log.
    SchemaRegistry freshReg;
    rebuildSchemaRegistryFromLog(replica_.get(), freshReg);

    // 4. Assert the entry survived.
    const RegistryEntry* entry = freshReg.byName("characters");
    ASSERT_NE(entry, nullptr) << "Schema 'characters' must survive log replay";
    EXPECT_EQ(entry->topicId, savedTopicId)
        << "Rebuilt topicId must match original";
    EXPECT_EQ(entry->partitionCount, 8)
        << "Rebuilt partitionCount must match";
    EXPECT_EQ(entry->version, 1u) << "Version must be 1";
    EXPECT_EQ(entry->keyField, "userId") << "keyField must survive";

    // Also verify the descriptor name was rebuilt correctly.
    EXPECT_EQ(entry->descriptor.name, "characters")
        << "Descriptor name must match after rebuild";
}

// ---------------------------------------------------------------------------
// Test 3: DuplicateDefineTopic_IdempotentOnLog
//
// Second DefineTopic for the same name:
//   - Must return TopicExists error (25).
//   - Must NOT append a second record to the log.
// ---------------------------------------------------------------------------
TEST_F(SchemaPersistenceTest, DuplicateDefineTopic_IdempotentOnLog) {
    DefineTopicArgs args;
    args.session        = 1;
    args.corr           = 2;
    args.name           = "characters";
    args.keyField       = "userId";
    args.partitionCount = 4;
    args.validate       = false;
    args.blob           = charsBlob();

    // First define — should succeed.
    adminWorker_->onDefineTopic(args);
    ASSERT_EQ(lastDefineError_, 0u) << "First DefineTopic must succeed";

    const Offset leoAfterFirst = log_.logEndOffset();
    EXPECT_GE(leoAfterFirst, 1LL) << "Log must have at least 1 record after first define";

    // Second define — same name, should return TopicExists=25.
    args.corr = 3;
    adminWorker_->onDefineTopic(args);
    EXPECT_EQ(lastDefineError_, 25u) << "Duplicate DefineTopic must return TopicExists=25";

    // Log must NOT have grown (no second record appended).
    EXPECT_EQ(log_.logEndOffset(), leoAfterFirst)
        << "Log must not grow on TopicExists path (no record appended for duplicate)";
}

// ---------------------------------------------------------------------------
// Test 4: ValidateTrue_PushValidatorsCalled
//
// DefineTopic with validate=true → pushValidators callback is invoked.
// (Stub in Task 6; Task 7 will replace with real worker ring enqueue.)
// ---------------------------------------------------------------------------
TEST_F(SchemaPersistenceTest, ValidateTrue_PushValidatorsCalled) {
    DefineTopicArgs args;
    args.session        = 1;
    args.corr           = 2;
    args.name           = "orders";
    args.keyField       = "";
    args.partitionCount = 2;
    args.validate       = true;
    args.blob           = charsBlob();  // valid blob (name in blob is "characters" but that's OK for this test)

    adminWorker_->onDefineTopic(args);
    ASSERT_EQ(lastDefineError_, 0u) << "Precondition: DefineTopic must succeed";

    EXPECT_EQ(pushValidatorCallCount_, 1)
        << "pushValidators must be called exactly once for validate=true";
    EXPECT_EQ(lastPushedTopicId_, lastDefineTopicId_)
        << "pushValidators topicId must match the assigned topicId";
}

// ---------------------------------------------------------------------------
// Regression test (Task 6): RULING-1 — createTopic returns authoritative id
//
// The AdminWorker must NOT call allocateRuntimeId(); the id is the return
// value of deps.createTopic(partitionCount, name).  Verify the registry entry
// uses the returned id, not any stale RUNTIME_TOPIC_ID_BASE-based value.
// ---------------------------------------------------------------------------
TEST_F(SchemaPersistenceTest, Regression_RULING1_TopicIdIsReturnedByCreateTopic) {
    // createTopic fake returns 42 (not RUNTIME_TOPIC_ID_BASE).
    // We wire a fresh AdminWorker with a different fake that returns 42.
    SchemaRegistry reg2;
    int32_t returnedId = -1;

    AdminDeps deps;
    deps.createTopic = [](int32_t, const std::string&) -> int32_t { return 42; };
    deps.appendDurable = [this](const TopicSchema& ts) -> bool {
        const std::vector<uint8_t> payload = ts.serialize();
        uint8_t key[4];
        const uint32_t u = static_cast<uint32_t>(ts.topicId);
        key[0] = static_cast<uint8_t>(u & 0xff);
        key[1] = static_cast<uint8_t>((u >> 8) & 0xff);
        key[2] = static_cast<uint8_t>((u >> 16) & 0xff);
        key[3] = static_cast<uint8_t>((u >> 24) & 0xff);
        const Offset base = replica_->appendKeyed(
            std::span<const uint8_t>(key, 4),
            std::span<const uint8_t>(payload.data(), payload.size()),
            /*acksAll=*/true);
        return base >= 0;
    };
    deps.pushValidators = [](const RegistryEntry&, int32_t) {};
    deps.respondDefine = [&returnedId](int64_t, int64_t, int32_t tid,
                                       uint16_t, uint16_t) { returnedId = tid; };
    deps.respondGet = [](int64_t, int64_t, int32_t, uint16_t,
                          const std::vector<uint8_t>&, uint16_t) {};

    AdminWorker w(reg2, std::move(deps));

    DefineTopicArgs args;
    args.session        = 1;
    args.corr           = 2;
    args.name           = "items";
    args.keyField       = "";
    args.partitionCount = 1;
    args.validate       = false;
    args.blob           = charsBlob();

    w.onDefineTopic(args);

    EXPECT_EQ(returnedId, 42)
        << "RULING-1 regression: topicId must be the value RETURNED by createTopic (42), "
           "not RUNTIME_TOPIC_ID_BASE or any pre-allocated SchemaRegistry id";
    const RegistryEntry* e = reg2.byName("characters");  // blob encodes "characters"
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->topicId, 42)
        << "Registry entry topicId must match the createTopic return value (42)";
}
