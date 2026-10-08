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
#include <rune/node/NodeRuntimeAgent.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/AlterOps.h>

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
// NullPartitionTransport — now the production rune::replication one (Task 11 fix 2).

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

// ALTER ops helpers (mirrors AdminWorkerTest.cpp's helpers) — used by the
// multi-version replay regression test below.
static std::vector<uint8_t> altersOpsBlob(const std::vector<AlterOp>& ops) {
    return encodeAlterOps(ops);
}
static AlterOp addOp(const std::string& name, PrimType t) {
    AlterOp op{}; op.type = AlterOpType::Add; op.name = name; op.primType = t; op.fixedLen = 0;
    return op;
}
static AlterOp setCompatOp(CompatMode m) {
    AlterOp op{}; op.type = AlterOpType::SetCompatibility; op.mode = m;
    return op;
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

        // --- Wire AdminDeps (Task 8a async model) ---
        // requestCreateTopic (RULING-2 B): the admin worker posts a create-topic
        // request; the SIMULATED control thread (below) replies with a sequential
        // id via onCreateTopicReply.  The AdminWorker itself owns the replica and
        // performs appendDurable + HW-gated commit internally (RULING-2 D).
        AdminDeps deps;
        deps.requestCreateTopic = [this](int64_t ticket, int32_t /*partitionCount*/,
                                         const std::string& /*name*/) -> bool {
            pendingTicket_ = ticket;
            return true;
        };
        deps.pushValidators = [this](const RegistryEntry& /*entry*/, int32_t tid) {
            ++pushValidatorCallCount_;
            lastPushedTopicId_ = tid;
        };
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
        deps.respondAlter = [this](int64_t /*session*/, int64_t /*corr*/,
                                   int32_t topicId, uint16_t newVersion,
                                   uint16_t errorCode) {
            lastAlterTopicId_ = topicId;
            lastAlterVersion_ = newVersion;
            lastAlterError_   = errorCode;
        };

        adminWorker_ = std::make_unique<AdminWorker>(
            *registry_, replica_.get(), /*selfId=*/1,
            std::vector<int32_t>{1}, std::move(deps));
        // Promote to leader (single-node) so DDL is served.
        adminWorker_->onRole(AdminRoleMsg{/*leader=*/true, /*epoch=*/1, /*leaderId=*/1});
    }

    // Drive one DefineTopic through the async state machine to completion:
    // submit → doWork loop, answering the createTopic request via the reply ring,
    // ticking until the HW-gated commit resolves.
    void driveDefine(const DefineTopicMsg& msg) {
        adminWorker_->submitDefineTopic(msg);
        pendingTicket_ = -1;
        int64_t nowNs = 0;
        for (int i = 0; i < 50; ++i) {
            nowNs += 1'000'000LL;
            adminWorker_->doWork(nowNs);
            if (pendingTicket_ >= 0) {
                CreateTopicReplyMsg reply;
                reply.ticket  = pendingTicket_;
                reply.topicId = nextTopicId_++;
                pendingTicket_ = -1;
                adminWorker_->onCreateTopicReply(reply);
            }
        }
    }

    // Drive one ALTER through the (synchronous, no createTopic round-trip)
    // state machine to completion: submit → doWork loop until the single-node
    // HW gate resolves.
    void driveAlter(const AlterTopicMsg& msg) {
        adminWorker_->submitAlterTopic(msg);
        int64_t nowNs = 0;
        for (int i = 0; i < 50; ++i) {
            nowNs += 1'000'000LL;
            adminWorker_->doWork(nowNs);
        }
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
    int64_t pendingTicket_ = -1;   // set by requestCreateTopic; consumed by driveDefine

    // Captured response state
    int32_t  lastDefineTopicId_ = -1;
    uint16_t lastDefineError_   = 0xffff;
    int32_t  lastGetTopicId_    = -1;
    uint16_t lastGetError_      = 0xffff;
    int32_t  lastAlterTopicId_  = -1;
    uint16_t lastAlterVersion_  = 0;
    uint16_t lastAlterError_    = 0xffff;
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
    DefineTopicMsg args;
    args.session        = 1;
    args.corr           = 2;
    args.name           = "characters";
    args.keyField       = "userId";
    args.partitionCount = 4;
    args.validate       = false;
    args.blob           = charsBlob();

    driveDefine(args);

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
    DefineTopicMsg args;
    args.session        = 10;
    args.corr           = 20;
    args.name           = "characters";
    args.keyField       = "userId";
    args.partitionCount = 8;
    args.validate       = false;
    args.blob           = charsBlob();

    driveDefine(args);
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
    DefineTopicMsg args;
    args.session        = 1;
    args.corr           = 2;
    args.name           = "characters";
    args.keyField       = "userId";
    args.partitionCount = 4;
    args.validate       = false;
    args.blob           = charsBlob();

    // First define — should succeed.
    driveDefine(args);
    ASSERT_EQ(lastDefineError_, 0u) << "First DefineTopic must succeed";

    const Offset leoAfterFirst = log_.logEndOffset();
    EXPECT_GE(leoAfterFirst, 1LL) << "Log must have at least 1 record after first define";

    // Second define — same name, should return TopicExists=25.
    args.corr = 3;
    driveDefine(args);
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
    DefineTopicMsg args;
    args.session        = 1;
    args.corr           = 2;
    args.name           = "orders";
    args.keyField       = "";
    args.partitionCount = 2;
    args.validate       = true;
    args.blob           = charsBlob();  // valid blob (name in blob is "characters" but that's OK for this test)

    driveDefine(args);
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
    // The authoritative id is the one the control thread REPLIES (RULING-1/B).
    // Here the simulated control thread replies topicId=42 for the pending ticket;
    // the registry entry + response must carry 42.
    SchemaRegistry reg2;
    int32_t returnedId = -1;
    int64_t ticket = -1;

    AdminDeps deps;
    deps.requestCreateTopic = [&ticket](int64_t t, int32_t, const std::string&) -> bool {
        ticket = t; return true;
    };
    deps.pushValidators = [](const RegistryEntry&, int32_t) {};
    deps.respondDefine = [&returnedId](int64_t, int64_t, int32_t tid,
                                       uint16_t, uint16_t) { returnedId = tid; };
    deps.respondGet = [](int64_t, int64_t, int32_t, uint16_t,
                          const std::vector<uint8_t>&, uint16_t) {};

    AdminWorker w(reg2, replica_.get(), /*selfId=*/1,
                  std::vector<int32_t>{1}, std::move(deps));
    w.onRole(AdminRoleMsg{/*leader=*/true, /*epoch=*/1, /*leaderId=*/1});

    DefineTopicMsg args;
    args.session        = 1;
    args.corr           = 2;
    args.name           = "items";
    args.keyField       = "";
    args.partitionCount = 1;
    args.validate       = false;
    args.blob           = charsBlob();
    w.submitDefineTopic(args);

    int64_t nowNs = 0;
    for (int i = 0; i < 50; ++i) {
        nowNs += 1'000'000LL;
        w.doWork(nowNs);
        if (ticket >= 0) {
            w.onCreateTopicReply(CreateTopicReplyMsg{ticket, 42});
            ticket = -1;
        }
    }

    EXPECT_EQ(returnedId, 42)
        << "RULING-1 regression: topicId must be the value the control thread REPLIED (42)";
    const RegistryEntry* e = reg2.byName("characters");  // blob encodes "characters"
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->topicId, 42)
        << "Registry entry topicId must match the replied create-topic id (42)";
}

// ---------------------------------------------------------------------------
// Regression (Task 15): ReplayReconstructsAllVersionsAndNextTag
//
// Task 9 made SchemaRegistry::applyRecord APPEND a version per topicId (never
// overwrite/collapse). This test proves BOTH replay paths that depend on that
// append semantics actually reconstruct every version of a topic, not just the
// latest:
//
//   Path A — boot replay: BrokerNode.cpp's [0,HW) forEachRecord → applyRecord
//            loop, mirrored verbatim by rebuildSchemaRegistryFromLog() above.
//   Path B — follower rebuild: AdminWorker::rebuildFromLog_(), exercised here
//            via a FRESH AdminWorker + FRESH SchemaRegistry wired to the SAME
//            replica/log and promoted leader->leader at the SAME epoch (the
//            same reuse pattern as Regression_RULING1 above) — a same-epoch
//            becomeLeader() is an idempotent ISR re-assert (see
//            PartitionReplica::becomeLeader), so this only exercises the
//            Follower->Leader onRole edge's rebuildFromLog_() call without
//            disturbing the fixture's own replica_/adminWorker_ state.
//
// Scenario: DEFINE v1 (3 fields) -> ALTER #1 ADDs "score" (-> v2, nextTag
// 3->4) -> ALTER #2 SETs compatibility to FORWARD (-> v3, distinct from the
// default BACKWARD). Both replay paths must reconstruct versions {1,2,3},
// latest()==v3, and the v3-current nextTag/compatMode.
// ---------------------------------------------------------------------------
TEST_F(SchemaPersistenceTest, ReplayReconstructsAllVersionsAndNextTag) {
    // 1. DEFINE v1: "characters" (id, userId, name) == 3 fields.
    DefineTopicMsg def;
    def.session        = 1;
    def.corr           = 2;
    def.name           = "characters";
    def.keyField       = "userId";
    def.partitionCount = 4;
    def.validate       = false;
    def.blob           = charsBlob();
    driveDefine(def);
    ASSERT_EQ(lastDefineError_, 0u) << "Precondition: DefineTopic must succeed";
    const int32_t topicId = lastDefineTopicId_;

    // 2. ALTER -> v2: ADD "score" (fresh durable tag == old fieldCount, 3).
    AlterTopicMsg alter1;
    alter1.session         = 10;
    alter1.corr            = 11;
    alter1.topicName       = "characters";
    alter1.expectedVersion = 0;  // unconditional
    alter1.opsBlob         = altersOpsBlob({addOp("score", PrimType::Int64)});
    driveAlter(alter1);
    ASSERT_EQ(lastAlterError_, 0u) << "Precondition: ALTER #1 (ADD score) must succeed";
    ASSERT_EQ(lastAlterVersion_, 2u);

    // 3. ALTER -> v3: SET COMPATIBILITY FORWARD (distinct from the default
    //    BACKWARD, so the test can assert the persisted compatMode on replay).
    AlterTopicMsg alter2;
    alter2.session         = 12;
    alter2.corr            = 13;
    alter2.topicName       = "characters";
    alter2.expectedVersion = 0;
    alter2.opsBlob         = altersOpsBlob({setCompatOp(CompatMode::Forward)});
    driveAlter(alter2);
    ASSERT_EQ(lastAlterError_, 0u) << "Precondition: ALTER #2 (SET COMPAT) must succeed";
    ASSERT_EQ(lastAlterVersion_, 3u);

    tick(5);  // belt-and-suspenders HW advance (matches Test 2's pattern)

    // ── Path A: boot replay into a FRESH registry. ──────────────────────────
    SchemaRegistry bootReg;
    rebuildSchemaRegistryFromLog(replica_.get(), bootReg);

    const RegistryEntry* v1 = bootReg.byIdVersion(topicId, 1);
    const RegistryEntry* v2 = bootReg.byIdVersion(topicId, 2);
    const RegistryEntry* v3 = bootReg.byIdVersion(topicId, 3);
    ASSERT_NE(v1, nullptr) << "boot replay: v1 must survive (not collapsed by v2/v3)";
    ASSERT_NE(v2, nullptr) << "boot replay: v2 must survive";
    ASSERT_NE(v3, nullptr) << "boot replay: v3 must survive";
    EXPECT_EQ(v1->version, 1u);
    EXPECT_EQ(v2->version, 2u);
    EXPECT_EQ(v3->version, 3u);

    const RegistryEntry* bootLatest = bootReg.latest(topicId);
    ASSERT_NE(bootLatest, nullptr);
    EXPECT_EQ(bootLatest->version, 3u) << "latest() must be v3 after boot replay";
    EXPECT_EQ(bootLatest->nextTag, 4u)
        << "nextTag (bumped 3->4 by the v2 ADD) must survive replay";
    EXPECT_EQ(bootLatest->compatMode, CompatMode::Forward)
        << "compatMode set by ALTER #2 must survive replay";

    // byFingerprint must still resolve the OLDER v1 fingerprint (old producers
    // stamping records against the pre-ALTER schema).
    EXPECT_EQ(bootReg.byFingerprint(topicId, v1->fingerprint), v1);

    // ── Path B: AdminWorker::rebuildFromLog_ (follower -> leader promotion). ─
    SchemaRegistry promotedReg;
    AdminDeps noopDeps;
    noopDeps.requestCreateTopic = [](int64_t, int32_t, const std::string&) { return true; };
    noopDeps.pushValidators     = [](const RegistryEntry&, int32_t) {};
    noopDeps.respondDefine      = [](int64_t, int64_t, int32_t, uint16_t, uint16_t) {};
    noopDeps.respondGet         = [](int64_t, int64_t, int32_t, uint16_t,
                                     const std::vector<uint8_t>&, uint16_t) {};
    noopDeps.respondAlter       = [](int64_t, int64_t, int32_t, uint16_t, uint16_t) {};

    AdminWorker promotedWorker(promotedReg, replica_.get(), /*selfId=*/1,
                               std::vector<int32_t>{1}, std::move(noopDeps));
    promotedWorker.onRole(AdminRoleMsg{/*leader=*/true, /*epoch=*/1, /*leaderId=*/1});

    const RegistryEntry* pv1 = promotedReg.byIdVersion(topicId, 1);
    const RegistryEntry* pv2 = promotedReg.byIdVersion(topicId, 2);
    const RegistryEntry* pv3 = promotedReg.byIdVersion(topicId, 3);
    ASSERT_NE(pv1, nullptr) << "rebuildFromLog_: v1 must survive (not collapsed by v2/v3)";
    ASSERT_NE(pv2, nullptr) << "rebuildFromLog_: v2 must survive";
    ASSERT_NE(pv3, nullptr) << "rebuildFromLog_: v3 must survive";
    EXPECT_EQ(pv1->version, 1u);
    EXPECT_EQ(pv2->version, 2u);
    EXPECT_EQ(pv3->version, 3u);

    const RegistryEntry* promotedLatest = promotedReg.latest(topicId);
    ASSERT_NE(promotedLatest, nullptr);
    EXPECT_EQ(promotedLatest->version, 3u) << "latest() must be v3 after rebuildFromLog_";
    EXPECT_EQ(promotedLatest->nextTag, 4u)
        << "nextTag must survive rebuildFromLog_ (follower rebuild)";
    EXPECT_EQ(promotedLatest->compatMode, CompatMode::Forward)
        << "compatMode must survive rebuildFromLog_ (follower rebuild)";
}

// ---------------------------------------------------------------------------
// Final-review Regression (Finding 1): CorruptRecordAbortsRebuildRegression
//
// Pre-fix, AdminWorker::rebuildFromLog_ SILENTLY SKIPPED a record whose
// TopicSchema::deserialize failed (corrupt/truncated payload). Because
// SchemaRegistry stores versions in a per-topic deque indexed by
// (version - 1), a silently-skipped record between two valid commits left an
// UNDETECTED version gap: the replay would "succeed" quietly while applying
// records AFTER the corrupt one as if nothing had been dropped.
//
// This test appends a DELIBERATELY TRUNCATED record (2 bytes — short of the
// 4 bytes TopicSchema::deserialize needs for just its first field) directly
// onto the __topic_schemas replica, sandwiched between a valid v1 DEFINE and
// a valid v2 ALTER, then promotes a FRESH AdminWorker/SchemaRegistry (the
// Follower->Leader rebuildFromLog_ edge — same pattern as Path B above) to
// replay the whole log from scratch (simulating a node recovering purely
// from disk).
//
// Post-fix: rebuildFailed() must be true (loud + detectable), v1 (before the
// corruption) must still be reconstructed, and v2 (after the corruption)
// must NOT be present — proving the rebuild ABORTED at the corrupt record
// instead of silently skipping it and papering over the gap.
// ---------------------------------------------------------------------------
TEST_F(SchemaPersistenceTest, CorruptRecordAbortsRebuildRegression) {
    // 1. DEFINE v1 "characters" via the normal production commit path.
    DefineTopicMsg def;
    def.session        = 1;
    def.corr           = 2;
    def.name           = "characters";
    def.keyField       = "userId";
    def.partitionCount = 4;
    def.validate       = false;
    def.blob           = charsBlob();
    driveDefine(def);
    ASSERT_EQ(lastDefineError_, 0u) << "Precondition: DefineTopic (v1) must succeed";
    const int32_t topicId = lastDefineTopicId_;

    tick(5);  // advance HW to cover v1

    // 2. Append a DELIBERATELY CORRUPT/TRUNCATED record DIRECTLY onto the
    //    same replica (bypassing AdminWorker's own commit path) — simulates a
    //    torn/garbled __topic_schemas payload landing between two valid
    //    commits. The outer RecordBatch framing (length/CRC) is valid — only
    //    the TopicSchema payload itself is malformed, exactly the condition
    //    rebuildFromLog_'s `if (ts)` guard is meant to catch.
    {
        const uint8_t keyBuf[6] = {0, 0, 0, 0, 0, 0};  // arbitrary; irrelevant here
        const uint8_t corruptPayload[2] = {0xDE, 0xAD};  // < 4B: fails readI32LE(topicId)
        const rune::replication::Offset base = replica_->appendKeyed(
            std::span<const uint8_t>(keyBuf, 6),
            std::span<const uint8_t>(corruptPayload, 2),
            /*acksAll=*/true);
        ASSERT_GE(base, 0) << "Precondition: the corrupt record must land in the log";
        replica_->onLeaderLogAppended();
    }
    tick(5);  // advance HW to cover the corrupt record

    // 3. A further valid ALTER AFTER the corrupt record, on the LIVE
    //    adminWorker_/registry_ (unaffected by the raw append above — the
    //    in-memory registry only ever changes via applyRecord on commit, not
    //    by reading the log back). This is the record a pre-fix silent-skip
    //    would have applied as if the corrupt record never existed.
    AlterTopicMsg alter1;
    alter1.session         = 10;
    alter1.corr            = 11;
    alter1.topicName       = "characters";
    alter1.expectedVersion = 0;
    alter1.opsBlob         = altersOpsBlob({addOp("score", PrimType::Int64)});
    driveAlter(alter1);
    ASSERT_EQ(lastAlterError_, 0u) << "Precondition: ALTER after the corrupt record must "
                                      "still succeed on the LIVE (uncorrupted) registry";
    ASSERT_EQ(lastAlterVersion_, 2u);

    tick(5);  // advance HW to cover v2

    // 4. Promote a FRESH AdminWorker/SchemaRegistry — the Follower->Leader
    //    rebuildFromLog_ edge — to replay the WHOLE log from scratch, as a
    //    recovering node would.
    SchemaRegistry promotedReg;
    AdminDeps noopDeps;
    noopDeps.requestCreateTopic = [](int64_t, int32_t, const std::string&) { return true; };
    noopDeps.pushValidators     = [](const RegistryEntry&, int32_t) {};
    noopDeps.respondDefine      = [](int64_t, int64_t, int32_t, uint16_t, uint16_t) {};
    noopDeps.respondGet         = [](int64_t, int64_t, int32_t, uint16_t,
                                     const std::vector<uint8_t>&, uint16_t) {};
    noopDeps.respondAlter       = [](int64_t, int64_t, int32_t, uint16_t, uint16_t) {};

    AdminWorker promotedWorker(promotedReg, replica_.get(), /*selfId=*/1,
                               std::vector<int32_t>{1}, std::move(noopDeps));
    promotedWorker.onRole(AdminRoleMsg{/*leader=*/true, /*epoch=*/1, /*leaderId=*/1});

    EXPECT_TRUE(promotedWorker.rebuildFailed())
        << "Regression: a corrupt/truncated __topic_schemas record must be LOUDLY "
           "detected (rebuildFailed()==true), never silently skipped";

    const RegistryEntry* pv1 = promotedReg.byIdVersion(topicId, 1);
    ASSERT_NE(pv1, nullptr) << "v1 (committed BEFORE the corrupt record) must still "
                                "be reconstructed";
    EXPECT_EQ(pv1->version, 1u);

    const RegistryEntry* pv2 = promotedReg.byIdVersion(topicId, 2);
    EXPECT_EQ(pv2, nullptr)
        << "Regression: the v2 ALTER record AFTER the corrupt one must NOT be "
           "silently applied — rebuildFromLog_ must ABORT at the corrupt record, "
           "never continue past it and paper over the version gap";
}

// ---------------------------------------------------------------------------
// Final-review Regression (Finding 2): CompatMode2RejectedRegression
//
// TopicSchema::deserialize's trailing (nextTag, compatMode) tail previously
// static_cast the raw compatMode byte straight into rune::codec::CompatMode
// with no range check — a byte outside {0=Backward, 1=Forward} was silently
// accepted as a well-formed (if semantically bogus) CompatMode. This test
// proves a byte of 2 (neither enumerator) is now rejected, same as every
// other bounds/shape check in deserialize().
// ---------------------------------------------------------------------------
TEST(TopicSchemaDeserializeRegression, CompatMode2RejectedRegression) {
    TopicSchema ts;
    ts.topicId        = 1;
    ts.version        = 1;
    ts.partitionCount = 1;
    ts.validate       = false;
    ts.keyField       = "id";
    ts.descriptorBlob = serialize(DescriptorBuilder("t", 1).add("id", PrimType::Int64).build());
    ts.nextTag        = 1;
    // Deliberately malformed: neither CompatMode::Backward(0) nor
    // CompatMode::Forward(1) — bypass the enum's type safety the same way a
    // corrupt/garbled on-disk byte would.
    ts.compatMode     = static_cast<CompatMode>(2);

    const std::vector<uint8_t> blob = ts.serialize();
    const auto decoded = TopicSchema::deserialize(
        std::span<const uint8_t>(blob.data(), blob.size()));

    EXPECT_FALSE(decoded.has_value())
        << "Regression: compatMode byte=2 (neither Backward(0) nor Forward(1)) must be "
           "REJECTED by deserialize(), not silently accepted as a valid CompatMode";
}

// ---------------------------------------------------------------------------
// Task 15b regression: OldVersionsSurviveCompactionRegression
//
// __topic_schemas is a Compact-policy log (BrokerNode.cpp:2825-2828).  Before
// Task 15b, BOTH appendKeyed call sites in AdminWorker.cpp (the DefineTopic
// create commit, onCreateTopicReply, and the ALTER commit) keyed every
// TopicSchema record by topicId ALONE (4-byte i32 LE).  The Kafka-style log
// cleaner (rune::storage::LogCleaner) compacts on EXACTLY the key stored with
// the record via appendKeyed (RecordBatchReader::forEachRecord's `key` param,
// fed straight into the dedupe OffsetMap) — there is no separate
// key-extractor for this log.  So pre-fix, v1/v2/v3 of the same topic were
// the SAME compaction key, and the cleaner physically deleted all but the
// latest version — destroying the version history that old-fingerprint
// client binding (Tasks 8/13/19) and multi-version replay (Task 15) depend
// on.
//
// This test appends v1/v2/v3 via the PRODUCTION AdminWorker append path
// (DefineTopic + 2x ALTER) into a Log configured with segmentRollBytes=1 (so
// EVERY append rolls a fresh segment — v1 and v2 end up SEALED/compactable,
// not stuck in the never-compacted active segment), runs REAL compaction via
// the production duty-cycle mechanism (rune::node::NodeRuntimeAgent::
// runBoundedCompaction over a rune::node::ReplicaRegistry — the exact harness
// CompactionCorrectnessTest.cpp's OffsetStoreRebuildFromCompactedLog test
// uses; NOT hand-rolled), then rebuilds a FRESH SchemaRegistry from the
// now-compacted log and asserts versions {1,2,3} ALL survive.
//
// Pre-fix (topicId-only key): FAILS — only v3 survives compaction.
// Post-fix (topicId,version composite key): PASSES.
// ---------------------------------------------------------------------------
TEST(SchemaPersistenceCompaction, OldVersionsSurviveCompactionRegression) {
    TmpSchemaDir dir("compact");

    // segmentRollBytes=1: maybeRoll() seals the active segment once it holds
    // >= 1 byte, so the batch AFTER the first one always starts a fresh
    // segment.  Three appends (v1, v2, v3) therefore land in THREE distinct
    // segments: v1 and v2 sealed (compactable), v3 in the active segment.
    rune::storage::Log log(dir.path, /*segmentRollBytes=*/1);
    log.recover();

    auto transport = std::make_unique<NullPartitionTransport>();

    PartitionConfig cfg = makeSingleNodeSchemaConfig();
    cfg.minCleanableDirtyRatio = 0.0;  // compact eagerly regardless of dirty fraction

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, transport.get());

    SchemaRegistry registry;

    int64_t  pendingTicket     = -1;
    int32_t  nextTopicId       = 0;
    int32_t  lastDefineTopicId = -1;
    uint16_t lastDefineError   = 0xffff;
    uint16_t lastAlterVersion  = 0;
    uint16_t lastAlterError    = 0xffff;

    AdminDeps deps;
    deps.requestCreateTopic = [&](int64_t ticket, int32_t, const std::string&) -> bool {
        pendingTicket = ticket;
        return true;
    };
    deps.pushValidators = [](const RegistryEntry&, int32_t) {};
    deps.respondDefine  = [&](int64_t, int64_t, int32_t topicId, uint16_t,
                             uint16_t errorCode) {
        lastDefineTopicId = topicId;
        lastDefineError   = errorCode;
    };
    deps.respondGet = [](int64_t, int64_t, int32_t, uint16_t,
                         const std::vector<uint8_t>&, uint16_t) {};
    deps.respondAlter = [&](int64_t, int64_t, int32_t, uint16_t newVersion,
                           uint16_t errorCode) {
        lastAlterVersion = newVersion;
        lastAlterError   = errorCode;
    };

    AdminWorker worker(registry, replica.get(), /*selfId=*/1,
                       std::vector<int32_t>{1}, std::move(deps));
    worker.onRole(AdminRoleMsg{/*leader=*/true, /*epoch=*/1, /*leaderId=*/1});

    auto driveDefineLocal = [&](const DefineTopicMsg& msg) {
        worker.submitDefineTopic(msg);
        pendingTicket = -1;
        int64_t nowNs = 0;
        for (int i = 0; i < 50; ++i) {
            nowNs += 1'000'000LL;
            worker.doWork(nowNs);
            if (pendingTicket >= 0) {
                CreateTopicReplyMsg reply;
                reply.ticket   = pendingTicket;
                reply.topicId  = nextTopicId++;
                pendingTicket  = -1;
                worker.onCreateTopicReply(reply);
            }
        }
    };
    auto driveAlterLocal = [&](const AlterTopicMsg& msg) {
        worker.submitAlterTopic(msg);
        int64_t nowNs = 0;
        for (int i = 0; i < 50; ++i) {
            nowNs += 1'000'000LL;
            worker.doWork(nowNs);
        }
    };

    // v1: DEFINE "characters" (3 fields).
    DefineTopicMsg def;
    def.session        = 1;
    def.corr           = 2;
    def.name           = "characters";
    def.keyField       = "userId";
    def.partitionCount = 4;
    def.validate       = false;
    def.blob           = charsBlob();
    driveDefineLocal(def);
    ASSERT_EQ(lastDefineError, 0u) << "Precondition: DefineTopic (v1) must succeed";
    const int32_t topicId = lastDefineTopicId;

    // v2: ALTER — ADD "score".
    AlterTopicMsg alter1;
    alter1.session         = 10;
    alter1.corr            = 11;
    alter1.topicName       = "characters";
    alter1.expectedVersion = 0;
    alter1.opsBlob         = altersOpsBlob({addOp("score", PrimType::Int64)});
    driveAlterLocal(alter1);
    ASSERT_EQ(lastAlterError, 0u) << "Precondition: ALTER #1 (v2) must succeed";
    ASSERT_EQ(lastAlterVersion, 2u);

    // v3: ALTER — SET COMPATIBILITY FORWARD.
    AlterTopicMsg alter2;
    alter2.session         = 12;
    alter2.corr            = 13;
    alter2.topicName       = "characters";
    alter2.expectedVersion = 0;
    alter2.opsBlob         = altersOpsBlob({setCompatOp(CompatMode::Forward)});
    driveAlterLocal(alter2);
    ASSERT_EQ(lastAlterError, 0u) << "Precondition: ALTER #2 (v3) must succeed";
    ASSERT_EQ(lastAlterVersion, 3u);

    // Belt-and-suspenders HW advance (same pattern as the other tests above).
    {
        int64_t nowNs = 0;
        for (int i = 0; i < 5; ++i) {
            nowNs += 1'000'000LL;
            replica->doWork(nowNs);
        }
    }

    // Capture v1's fingerprint BEFORE compaction, from an independent replay —
    // this is the value an old client's typed gate would have compiled
    // against, and must still resolve via byFingerprint() AFTER compaction.
    SchemaRegistry preCompactReg;
    rebuildSchemaRegistryFromLog(replica.get(), preCompactReg);
    const RegistryEntry* preV1 = preCompactReg.byIdVersion(topicId, 1);
    ASSERT_NE(preV1, nullptr) << "Precondition: v1 must be present before compaction";
    const uint64_t v1Fingerprint = preV1->fingerprint;

    // Sanity: rollBytes=1 must have produced >= 2 segments (>=1 sealed +
    // active), or this test would not exercise compaction at all.
    const auto basesBefore = log.segmentBaseOffsets();
    ASSERT_GE(basesBefore.size(), 2u)
        << "Need >= 2 segments (sealed + active) to exercise compaction of "
           "superseded schema versions — vacuousness guard";

    // --- Run REAL compaction via the production duty-cycle mechanism. ---
    rune::node::ReplicaRegistry replicaRegistry;
    PartitionReplica* rawReplica = replica.get();
    replicaRegistry.add(kSchemaTopicId, /*partition=*/0, std::move(replica));

    class NoopConsensusAgent : public rune::consensus::Agent {
    public:
        int doWork(int64_t) override { return 0; }
    };
    NoopConsensusAgent consensusAgent;
    rune::node::NodeRuntimeAgent runtimeAgent(
        consensusAgent, /*election=*/nullptr, /*bridge=*/nullptr,
        /*transport=*/nullptr, &replicaRegistry, /*gateway=*/nullptr);

    const std::size_t sealedCount = basesBefore.size() - 1;
    for (std::size_t i = 0; i < sealedCount + 20; ++i) {
        runtimeAgent.runBoundedCompaction(static_cast<int64_t>(i) * 1'000'000LL);
    }
    log.reloadSegments();

    // --- Rebuild a FRESH SchemaRegistry from the (now compacted) log. ---
    SchemaRegistry postCompactReg;
    rebuildSchemaRegistryFromLog(rawReplica, postCompactReg);

    const RegistryEntry* v1 = postCompactReg.byIdVersion(topicId, 1);
    const RegistryEntry* v2 = postCompactReg.byIdVersion(topicId, 2);
    const RegistryEntry* v3 = postCompactReg.byIdVersion(topicId, 3);
    ASSERT_NE(v1, nullptr)
        << "v1 must survive compaction — pre-Task-15b topicId-only keying "
           "collapses all versions into one compaction key and this FAILS";
    ASSERT_NE(v2, nullptr) << "v2 must survive compaction";
    ASSERT_NE(v3, nullptr) << "v3 must survive compaction";
    EXPECT_EQ(v1->version, 1u);
    EXPECT_EQ(v2->version, 2u);
    EXPECT_EQ(v3->version, 3u);

    const RegistryEntry* lat = postCompactReg.latest(topicId);
    ASSERT_NE(lat, nullptr);
    EXPECT_EQ(lat->version, 3u) << "latest() must be v3 after compaction + rebuild";

    EXPECT_EQ(postCompactReg.byFingerprint(topicId, v1Fingerprint), v1)
        << "byFingerprint for the OLD v1 fingerprint must still resolve after "
           "compaction (old-fingerprint client binding depends on this)";
}
