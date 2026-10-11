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
#include <rune/node/RuntimeTopicRecovery.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Record.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
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
static AlterOp dropOp(const std::string& name) {
    AlterOp op{}; op.type = AlterOpType::Drop; op.name = name;
    return op;
}
// Used by the GC5 long-lineage compaction test (renames the live display
// name while preserving the field's durable tag -- decode-by-tag must be
// unaffected by a rename chain).
static AlterOp renameOp(const std::string& oldName, const std::string& newName) {
    AlterOp op{}; op.type = AlterOpType::Rename; op.name = oldName; op.newName = newName;
    return op;
}

// ---------------------------------------------------------------------------
// rebuildFromLog — same logic as BrokerNode::buildAdminWorker (including its
// fail-loud-on-corrupt-record fix: see CorruptRecordAbortsBootReplayRegression
// below). Replays the __topic_schemas log [0, HW) into a SchemaRegistry.
//
// Returns false iff a record's TopicSchema::deserialize failed (corrupt/
// truncated payload) — the replay ABORTS at that point (no later batch is
// applied), matching BrokerNode::buildAdminWorker's throw-and-degrade path.
// Returns true on a clean (possibly partial/empty) replay.
// ---------------------------------------------------------------------------
static bool rebuildSchemaRegistryFromLog(
    const PartitionReplica* replica, SchemaRegistry& reg)
{
    const Offset hw = replica->highWatermark();
    if (hw <= 0) return true;

    static constexpr int32_t kChunk = 64 * 1024 * 1024;
    Offset fetchOffset = 0;
    bool replayFailed = false;

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

            reader.forEach([&reg, &replayFailed](std::string_view /*key*/,
                                   std::string_view val,
                                   int64_t /*offset*/,
                                   int64_t /*ts*/) {
                auto ts = TopicSchema::deserialize(std::span<const uint8_t>(
                    reinterpret_cast<const uint8_t*>(val.data()), val.size()));
                if (ts) {
                    reg.applyRecord(*ts);
                } else {
                    replayFailed = true;  // corrupt/truncated — abort, never skip
                }
            });
            if (replayFailed) break;

            const Offset bne = reader.baseOffset() + reader.lastOffsetDelta() + 1;
            if (bne > lastBatchEnd) lastBatchEnd = bne;
            pos += static_cast<std::size_t>(tl);
        }
        if (replayFailed) break;

        if (lastBatchEnd <= fetchOffset) break;
        fetchOffset = lastBatchEnd;
    }
    return !replayFailed;
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
                                         const std::string& /*name*/,
                                         const std::string& /*keyField*/,
                                         uint8_t /*cleanupPolicy*/) -> bool {
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
    deps.requestCreateTopic = [&ticket](int64_t t, int32_t, const std::string&,
                                        const std::string&, uint8_t) -> bool {
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
    noopDeps.requestCreateTopic = [](int64_t, int32_t, const std::string&, const std::string&, uint8_t) { return true; };
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
    noopDeps.requestCreateTopic = [](int64_t, int32_t, const std::string&, const std::string&, uint8_t) { return true; };
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
// Follow-up final-review Regression: CorruptRecordAbortsBootReplayRegression
//
// Same-class bug as Finding 1 above, but on the BOOT-time replay path
// (BrokerNode::buildAdminWorker's inline [0,HW) rebuild — the PRIMARY schema
// recovery path, hit on EVERY boot, not just a Follower->Leader promotion).
// Pre-fix, that inline loop ALSO silently skipped a record whose
// TopicSchema::deserialize failed, leaving the same kind of undetected
// version gap. BrokerNode::buildAdminWorker now throws std::runtime_error on
// such a record (caught by its own enclosing try/catch, which logs
// spdlog::error and degrades — schema DDL unavailable — same convention as
// every other AdminWorker setup failure in that function).
//
// rebuildSchemaRegistryFromLog() above is this test file's established
// stand-in for that inline boot logic (BrokerNode itself needs a full Aeron
// driver to construct, too heavy for this test binary) and has been updated
// to mirror the SAME fail-loud/abort behavior, returning false on a corrupt
// record. This test exercises that mirror with the identical sandwiched-
// corruption setup as CorruptRecordAbortsRebuildRegression above.
// ---------------------------------------------------------------------------
TEST_F(SchemaPersistenceTest, CorruptRecordAbortsBootReplayRegression) {
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

    // 2. Append a DELIBERATELY CORRUPT/TRUNCATED record directly onto the
    //    replica, sandwiched between v1 and the v2 ALTER below — same
    //    construction as CorruptRecordAbortsRebuildRegression.
    {
        const uint8_t keyBuf[6] = {0, 0, 0, 0, 0, 0};
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
    //    adminWorker_/registry_ (unaffected by the raw append above).
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

    // 4. Boot-time replay of the WHOLE log from scratch (same logic as
    //    BrokerNode::buildAdminWorker, post-fix) — must fail loud and abort.
    SchemaRegistry bootReg;
    const bool ok = rebuildSchemaRegistryFromLog(replica_.get(), bootReg);

    EXPECT_FALSE(ok)
        << "Regression: a corrupt/truncated __topic_schemas record on the BOOT "
           "replay path must be LOUDLY detected (replay returns false), never "
           "silently skipped";

    const RegistryEntry* bv1 = bootReg.byIdVersion(topicId, 1);
    ASSERT_NE(bv1, nullptr) << "v1 (committed BEFORE the corrupt record) must still "
                                "be reconstructed";
    EXPECT_EQ(bv1->version, 1u);

    const RegistryEntry* bv2 = bootReg.byIdVersion(topicId, 2);
    EXPECT_EQ(bv2, nullptr)
        << "Regression: the v2 ALTER record AFTER the corrupt one must NOT be "
           "silently applied — boot replay must ABORT at the corrupt record, "
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
// GC4 (crash-recovery matrix): truncation points -- unit level.
//
// CompatMode2RejectedRegression above proves a MALFORMED trailing byte is
// rejected; these three prove deserialize() rejects a record TRUNCATED at
// each of the three distinct variable-length regions of the wire layout
// (see the header comment on TopicSchema in SchemaRegistry.h) -- inside the
// keyField bytes, inside the descriptorBlob bytes, and inside the trailing
// (nextTag, compatMode) tail -- not just the gross few-byte garbage payload
// the integration-level CorruptRecordAbortsRebuildRegression/
// CorruptRecordAbortsBootReplayRegression tests use (that garbage is
// truncated inside the very FIRST field, topicId). Each case must recover
// NOTHING (nullopt), never a partial/garbled TopicSchema.
// ---------------------------------------------------------------------------
TEST(TopicSchemaDeserializeRegression, TruncatedInsideKeyFieldRejectedRegression) {
    TopicSchema ts;
    ts.topicId        = 7;
    ts.version        = 1;
    ts.partitionCount = 4;
    ts.validate       = false;
    ts.keyField       = "userId";  // 6 bytes
    ts.descriptorBlob = serialize(DescriptorBuilder("t", 1).add("id", PrimType::Int64).build());
    ts.nextTag        = 1;
    ts.compatMode     = CompatMode::Backward;

    const std::vector<uint8_t> full = ts.serialize();

    // Fixed header (topicId+version+partitionCount+validate) = 11 bytes,
    // + u16 keyLen prefix = 13 -- keyField bytes start there and run for
    // keyField.size() (6) bytes. Truncate to 3 of the 6 keyField bytes: the
    // keyLen prefix promises 6 bytes but only 3 are actually present.
    constexpr size_t kKeyFieldStart = 13;
    const size_t truncateLen = kKeyFieldStart + 3;
    ASSERT_LT(truncateLen, full.size()) << "vacuousness guard: must actually truncate";

    const std::vector<uint8_t> truncated(full.begin(), full.begin() + truncateLen);
    const auto decoded = TopicSchema::deserialize(
        std::span<const uint8_t>(truncated.data(), truncated.size()));
    EXPECT_FALSE(decoded.has_value())
        << "Regression: a record truncated MID-keyField (keyLen prefix promises more "
           "bytes than are present) must be rejected, never partially decoded";
}

TEST(TopicSchemaDeserializeRegression, TruncatedInsideDescriptorBlobRejectedRegression) {
    TopicSchema ts;
    ts.topicId        = 7;
    ts.version        = 1;
    ts.partitionCount = 4;
    ts.validate       = false;
    ts.keyField       = "userId";
    ts.descriptorBlob = serialize(DescriptorBuilder("characters", 1)
        .add("id",     PrimType::Int64)
        .add("userId", PrimType::Int64, 1, true)
        .add("name",   PrimType::String)
        .build());
    ts.nextTag        = 3;
    ts.compatMode     = CompatMode::Backward;

    const std::vector<uint8_t> full = ts.serialize();

    constexpr size_t kKeyFieldStart = 13;
    const size_t descLenPos    = kKeyFieldStart + ts.keyField.size();      // 19
    const size_t descBlobStart = descLenPos + 4;                          // 23
    ASSERT_GT(ts.descriptorBlob.size(), 4u)
        << "vacuousness guard: need a descriptorBlob big enough to truncate mid-blob";
    const size_t truncateLen = descBlobStart + (ts.descriptorBlob.size() / 2);
    ASSERT_LT(truncateLen, full.size()) << "vacuousness guard: must actually truncate";

    const std::vector<uint8_t> truncated(full.begin(), full.begin() + truncateLen);
    const auto decoded = TopicSchema::deserialize(
        std::span<const uint8_t>(truncated.data(), truncated.size()));
    EXPECT_FALSE(decoded.has_value())
        << "Regression: a record truncated MID-descriptorBlob (descLen prefix promises "
           "more bytes than are present) must be rejected, never partially decoded";
}

TEST(TopicSchemaDeserializeRegression, TruncatedInsideTrailingTailRejectedRegression) {
    TopicSchema ts;
    ts.topicId        = 7;
    ts.version        = 1;
    ts.partitionCount = 4;
    ts.validate       = false;
    ts.keyField       = "userId";
    ts.descriptorBlob = serialize(DescriptorBuilder("t", 1).add("id", PrimType::Int64).build());
    ts.nextTag        = 1;
    ts.compatMode     = CompatMode::Forward;  // non-default, so a wrong decode would be visible

    const std::vector<uint8_t> full = ts.serialize();
    ASSERT_GE(full.size(), 3u);
    const size_t tailStart = full.size() - 3;  // nextTag(2) + compatMode(1)

    // remaining == 1: only nextTag's first byte present.
    {
        const std::vector<uint8_t> truncated(full.begin(), full.begin() + tailStart + 1);
        const auto decoded = TopicSchema::deserialize(
            std::span<const uint8_t>(truncated.data(), truncated.size()));
        EXPECT_FALSE(decoded.has_value())
            << "Regression: a tail truncated to 1 leftover byte (mid-nextTag) must be rejected";
    }
    // remaining == 2: nextTag fully present, compatMode byte missing.
    {
        const std::vector<uint8_t> truncated(full.begin(), full.begin() + tailStart + 2);
        const auto decoded = TopicSchema::deserialize(
            std::span<const uint8_t>(truncated.data(), truncated.size()));
        EXPECT_FALSE(decoded.has_value())
            << "Regression: a tail truncated to 2 leftover bytes (nextTag present, "
               "compatMode byte missing) must be rejected";
    }
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
    deps.requestCreateTopic = [&](int64_t ticket, int32_t, const std::string&, const std::string&, uint8_t) -> bool {
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

// ===========================================================================
// GC4: crash-recovery matrix (truncation points, HW-gating, mixed data+
// schema replay, idempotence). Extends the corrupt-record/compaction
// regressions above with the remaining `[GAP]` scenarios from the
// G3-G32 range of docs/superpowers/specs/2026-10-08-rune-schema-evolution-
// test-matrix.md:
//   - G39-44 (truncation at each distinct field boundary, not just the
//     gross garbage CorruptRecordAbortsRebuildRegression/
//     CorruptRecordAbortsBootReplayRegression already cover)
//   - G9-14 (HW-gated application: a record past HW is never applied)
//   - G27-32 (mixed data-log + schema-log replay: every data record still
//     decodes per its own writer version)
//   - G51-56 (replay idempotence / time-independence)
// ===========================================================================

// A byte-for-byte valid v2 TopicSchema serialization for `topicId` (ADD
// "score" over the v1 "characters" schema from charsBlob()) -- appended
// WHOLE by the HW-gating test below, and TRUNCATED at varying byte offsets
// by the three truncation-point tests that follow it.
static std::vector<uint8_t> wouldBeV2SchemaBuf(int32_t topicId) {
    TopicSchema ts;
    ts.topicId        = topicId;
    ts.version        = 2;
    ts.partitionCount = 4;
    ts.validate       = false;
    ts.keyField       = "userId";
    ts.descriptorBlob = serialize(DescriptorBuilder("characters", 1)
        .add("id",     PrimType::Int64)
        .add("userId", PrimType::Int64, 1, true)
        .add("name",   PrimType::String)
        .add("score",  PrimType::Int64)
        .build());
    ts.nextTag    = 4;
    ts.compatMode = CompatMode::Backward;
    return ts.serialize();
}

// Append `payload` as a RAW record directly onto `replica` (bypassing
// AdminWorker's commit path) -- same raw-append primitive
// CorruptRecordAbortsRebuildRegression/CorruptRecordAbortsBootReplayRegression
// use for their sandwiched-corruption setup, generalized to take an
// arbitrary (possibly truncated, possibly whole) byte span so callers
// control exactly what lands in the log.
static void appendRawSchemaRecord(PartitionReplica* replica, std::span<const uint8_t> payload) {
    const uint8_t keyBuf[6] = {0, 0, 0, 0, 0, 0};
    const Offset base = replica->appendKeyed(
        std::span<const uint8_t>(keyBuf, 6), payload, /*acksAll=*/true);
    ASSERT_GE(base, 0) << "Precondition: the raw record must land in the log";
    replica->onLeaderLogAppended();
}

// [blockLength:u16][version:u16][fixed block] -- the record wire shape
// readField() expects, and the same shape WorkerReplicationValidatorTest.cpp's
// encodeFixedRecord uses for the validator-side mixed-version tests
// (duplicated locally to keep this file self-contained). `fields` is
// (byteOffsetFromBlockStart, byteWidth, value).
static std::vector<uint8_t> encodeFixedRecord(
    uint16_t version, uint32_t blockLength,
    const std::vector<std::tuple<uint32_t, uint32_t, uint64_t>>& fields)
{
    std::vector<uint8_t> b(4 + blockLength, 0);
    b[0] = static_cast<uint8_t>(blockLength); b[1] = static_cast<uint8_t>(blockLength >> 8);
    b[2] = static_cast<uint8_t>(version);     b[3] = static_cast<uint8_t>(version >> 8);
    for (const auto& [offset, width, value] : fields) {
        for (uint32_t i = 0; i < width; ++i)
            b[4 + offset + i] = static_cast<uint8_t>(value >> (8 * i));
    }
    return b;
}

// Generic reader for ANY replica's [0,HW) committed records -- returns each
// record's raw VALUE bytes in log order. Same batch-walking shape as
// rebuildSchemaRegistryFromLog above, generalized to a plain data topic
// (used by the mixed data+schema replay test to pull the data log's
// records back after a simulated restart).
static std::vector<std::vector<uint8_t>> readAllRecordValues(const PartitionReplica* replica) {
    std::vector<std::vector<uint8_t>> out;
    const Offset hw = replica->highWatermark();
    if (hw <= 0) return out;

    static constexpr int32_t kChunk = 64 * 1024 * 1024;
    Offset fetchOffset = 0;

    while (fetchOffset < hw) {
        std::vector<uint8_t> data =
            const_cast<PartitionReplica*>(replica)->read(fetchOffset, kChunk);
        if (data.empty()) break;

        std::size_t pos     = 0;
        Offset lastBatchEnd = fetchOffset;

        while (pos < data.size()) {
            const uint8_t*    bs  = data.data() + pos;
            const std::size_t rem = data.size() - pos;

            if (rem < static_cast<std::size_t>(
                    rune::protocol::batch::kRecordBatchOverhead))
                break;

            rune::protocol::RecordBatchReader reader(bs, rem);
            if (!reader.ensureValid()) break;

            const int32_t tl = reader.totalLength();
            if (tl <= 0 || static_cast<std::size_t>(tl) > rem) break;

            reader.forEach([&out](std::string_view /*key*/, std::string_view val,
                                   int64_t /*offset*/, int64_t /*ts*/) {
                out.emplace_back(val.begin(), val.end());
            });

            const Offset bne = reader.baseOffset() + reader.lastOffsetDelta() + 1;
            if (bne > lastBatchEnd) lastBatchEnd = bne;
            pos += static_cast<std::size_t>(tl);
        }

        if (lastBatchEnd <= fetchOffset) break;
        fetchOffset = lastBatchEnd;
    }
    return out;
}

// Compare two independently-rebuilt SchemaRegistry instances for identical
// state of EVERY version 1..maxVersion of `topicId` -- same version number,
// nextTag, compatMode, fingerprint, keyField, partitionCount,
// descriptorBlob bytes, and the exact materialized field layout
// (name/tag/offset/type per field, in order). Used by the idempotence/
// time-independence regression below: replaying the identical
// __topic_schemas log twice must produce IDENTICAL registry state, not
// merely "equivalent enough".
static void expectRegistriesByteIdenticalForAllVersions(
    const SchemaRegistry& a, const SchemaRegistry& b,
    int32_t topicId, uint16_t maxVersion)
{
    for (uint16_t v = 1; v <= maxVersion; ++v) {
        const RegistryEntry* ea = a.byIdVersion(topicId, v);
        const RegistryEntry* eb = b.byIdVersion(topicId, v);
        ASSERT_NE(ea, nullptr) << "registry A missing version " << v;
        ASSERT_NE(eb, nullptr) << "registry B missing version " << v;

        EXPECT_EQ(ea->version, eb->version) << "version mismatch at v" << v;
        EXPECT_EQ(ea->nextTag, eb->nextTag) << "nextTag mismatch at v" << v;
        EXPECT_EQ(static_cast<int>(ea->compatMode), static_cast<int>(eb->compatMode))
            << "compatMode mismatch at v" << v;
        EXPECT_EQ(ea->fingerprint, eb->fingerprint) << "fingerprint mismatch at v" << v;
        EXPECT_EQ(ea->keyField, eb->keyField) << "keyField mismatch at v" << v;
        EXPECT_EQ(ea->partitionCount, eb->partitionCount) << "partitionCount mismatch at v" << v;
        EXPECT_EQ(ea->descriptorBlob, eb->descriptorBlob) << "descriptorBlob mismatch at v" << v;

        ASSERT_EQ(ea->descriptor.fields.size(), eb->descriptor.fields.size())
            << "materialized field count mismatch at v" << v;
        for (std::size_t i = 0; i < ea->descriptor.fields.size(); ++i) {
            const auto& fa = ea->descriptor.fields[i];
            const auto& fb = eb->descriptor.fields[i];
            EXPECT_EQ(fa.name, fb.name) << "field[" << i << "].name mismatch at v" << v;
            EXPECT_EQ(fa.id,   fb.id)   << "field[" << i << "].id mismatch at v" << v;
            EXPECT_EQ(fa.offset, fb.offset) << "field[" << i << "].offset mismatch at v" << v;
            EXPECT_EQ(static_cast<int>(fa.type), static_cast<int>(fb.type))
                << "field[" << i << "].type mismatch at v" << v;
        }
    }

    const RegistryEntry* la = a.latest(topicId);
    const RegistryEntry* lb = b.latest(topicId);
    ASSERT_NE(la, nullptr);
    ASSERT_NE(lb, nullptr);
    EXPECT_EQ(la->version, lb->version) << "latest() version mismatch between two replays";
}

// ---------------------------------------------------------------------------
// GC4 (G39-44): truncation-point matrix -- boot replay.
//
// CorruptRecordAbortsBootReplayRegression above proves a GROSS 2-byte
// garbage payload (truncated inside the very first field, topicId) is
// rejected. These three vary WHERE a record is torn instead: a record that
// is otherwise a byte-for-byte valid v2 TopicSchema serialization, cut off
// mid-keyField, mid-descriptorBlob, and mid-trailing-tail respectively.
// Each must abort the boot replay exactly like the gross-garbage case (fail
// loud, v1 survives, nothing past the torn record is applied) -- recovery
// must recover EXACTLY the complete-version prefix, never a partial/
// garbled version, regardless of where inside the record the tear lands.
// ---------------------------------------------------------------------------
TEST_F(SchemaPersistenceTest, TruncationInsideKeyFieldAbortsBootReplayRegression) {
    DefineTopicMsg def;
    def.session = 1; def.corr = 2; def.name = "characters";
    def.keyField = "userId"; def.partitionCount = 4; def.validate = false;
    def.blob = charsBlob();
    driveDefine(def);
    ASSERT_EQ(lastDefineError_, 0u) << "Precondition: DefineTopic (v1) must succeed";
    const int32_t topicId = lastDefineTopicId_;
    tick(5);

    const std::vector<uint8_t> v2Buf = wouldBeV2SchemaBuf(topicId);
    // Fixed header (11) + keyLen prefix (2) = 13; keyField "userId" is 6
    // bytes -- truncate to 3 of them (keyLen prefix promises 6, only 3 land).
    const size_t truncateLen = 13 + 3;
    ASSERT_LT(truncateLen, v2Buf.size());
    appendRawSchemaRecord(replica_.get(),
                           std::span<const uint8_t>(v2Buf.data(), truncateLen));
    tick(5);

    SchemaRegistry bootReg;
    const bool ok = rebuildSchemaRegistryFromLog(replica_.get(), bootReg);
    EXPECT_FALSE(ok) << "Regression: a record torn mid-keyField must abort boot replay, "
                         "not silently recover a partial version";

    const RegistryEntry* v1 = bootReg.byIdVersion(topicId, 1);
    ASSERT_NE(v1, nullptr) << "v1 (committed before the tear) must still be reconstructed";
    EXPECT_EQ(bootReg.byIdVersion(topicId, 2), nullptr)
        << "the torn v2 record must NOT be partially/garbled-applied";
}

TEST_F(SchemaPersistenceTest, TruncationInsideDescriptorBlobAbortsBootReplayRegression) {
    DefineTopicMsg def;
    def.session = 1; def.corr = 2; def.name = "characters";
    def.keyField = "userId"; def.partitionCount = 4; def.validate = false;
    def.blob = charsBlob();
    driveDefine(def);
    ASSERT_EQ(lastDefineError_, 0u) << "Precondition: DefineTopic (v1) must succeed";
    const int32_t topicId = lastDefineTopicId_;
    tick(5);

    const std::vector<uint8_t> v2Buf = wouldBeV2SchemaBuf(topicId);
    const size_t keyFieldStart = 13;
    const size_t descLenPos    = keyFieldStart + std::string("userId").size();  // 19
    const size_t descBlobStart = descLenPos + 4;                                // 23
    const size_t descBlobLen   = v2Buf.size() - 3 /*tail*/ - descBlobStart;
    ASSERT_GT(descBlobLen, 2u)
        << "vacuousness guard: need a descriptorBlob big enough to truncate mid-blob";
    const size_t truncateLen = descBlobStart + (descBlobLen / 2);
    ASSERT_LT(truncateLen, v2Buf.size());
    appendRawSchemaRecord(replica_.get(),
                           std::span<const uint8_t>(v2Buf.data(), truncateLen));
    tick(5);

    SchemaRegistry bootReg;
    const bool ok = rebuildSchemaRegistryFromLog(replica_.get(), bootReg);
    EXPECT_FALSE(ok) << "Regression: a record torn mid-descriptorBlob must abort boot replay";

    const RegistryEntry* v1 = bootReg.byIdVersion(topicId, 1);
    ASSERT_NE(v1, nullptr) << "v1 (committed before the tear) must still be reconstructed";
    EXPECT_EQ(bootReg.byIdVersion(topicId, 2), nullptr)
        << "the torn v2 record must NOT be partially/garbled-applied";
}

TEST_F(SchemaPersistenceTest, TruncationInsideTrailingTailAbortsBootReplayRegression) {
    DefineTopicMsg def;
    def.session = 1; def.corr = 2; def.name = "characters";
    def.keyField = "userId"; def.partitionCount = 4; def.validate = false;
    def.blob = charsBlob();
    driveDefine(def);
    ASSERT_EQ(lastDefineError_, 0u) << "Precondition: DefineTopic (v1) must succeed";
    const int32_t topicId = lastDefineTopicId_;
    tick(5);

    const std::vector<uint8_t> v2Buf = wouldBeV2SchemaBuf(topicId);
    ASSERT_GE(v2Buf.size(), 3u);
    const size_t tailStart   = v2Buf.size() - 3;
    const size_t truncateLen = tailStart + 1;  // nextTag's first byte only present
    appendRawSchemaRecord(replica_.get(),
                           std::span<const uint8_t>(v2Buf.data(), truncateLen));
    tick(5);

    SchemaRegistry bootReg;
    const bool ok = rebuildSchemaRegistryFromLog(replica_.get(), bootReg);
    EXPECT_FALSE(ok) << "Regression: a record torn mid-trailing-tail must abort boot replay";

    const RegistryEntry* v1 = bootReg.byIdVersion(topicId, 1);
    ASSERT_NE(v1, nullptr) << "v1 (committed before the tear) must still be reconstructed";
    EXPECT_EQ(bootReg.byIdVersion(topicId, 2), nullptr)
        << "the torn v2 record must NOT be partially/garbled-applied";
}

// ---------------------------------------------------------------------------
// GC4 (G9-14): HW-gating.
//
// A record present in the log but beyond the applied/high-watermark
// boundary must NOT be applied on replay -- only [0, HW) is ever read by
// rebuildSchemaRegistryFromLog (mirroring BrokerNode::buildAdminWorker's
// boot-time [0,HW) forEachRecord loop, and PartitionReplica::read()'s own
// "committed read (<= highWatermark)" contract).
//
// IMPORTANT MIRROR-HARNESS LIMITATION (documented per the task's own
// allowance): on a SINGLE-NODE leader with minIsr=1, this harness's
// PartitionReplica advances HW EAGERLY on every append once the (trivial,
// self-only) ISR is synced -- there is no reachable "appended but HW
// hasn't caught up yet" timing window to race against, because a
// single-node ISR of {self} satisfies minIsr=1 immediately. That window
// only exists for a REAL multi-node ISR waiting on follower acks, which
// needs the Aeron media driver + real follower replicas -- explicitly
// out of scope for this on-host mirror (see this file's own top-of-file
// comment re: "Majority-durable commit path").
//
// This test instead exercises the SAME underlying HW-gating mechanism
// (PartitionReplica::computeServedHw()/highWatermark(): HW stays clamped
// at the frozen lastHealthyHw_ floor while |ISR| < minIsr -- see
// PartitionReplica.cpp) via a STRUCTURALLY unreachable quorum: minIsr=2
// configured over a SINGLE replica node. The ISR can therefore never
// reach minIsr, so HW is pinned at 0 forever -- deterministic, not a
// timing race -- while appendKeyed(acksAll=false) still lands records in
// the log (acksAll=false bypasses only the APPEND-time ISR gate; HW
// computation is unaffected and still clamps). This proves exactly the
// scenario G9-14 calls for: a record present in the log past the
// committed/HW boundary is excluded from [0,HW) replay.
// ---------------------------------------------------------------------------
TEST(SchemaPersistenceHwGating, RecordPastHighWatermarkExcludedFromReplayRegression) {
    TmpSchemaDir dir("hwgate");
    rune::storage::Log log(dir.path);
    log.recover();
    auto transport = std::make_unique<NullPartitionTransport>();

    PartitionConfig cfg = makeSingleNodeSchemaConfig();
    cfg.minIsr = 2;   // UNREACHABLE: only 1 replica node is configured below.

    auto replica = std::make_unique<PartitionReplica>(
        cfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &log, transport.get());

    // Drive doWork a bunch BEFORE the append -- this is exactly what would
    // advance HW if the ISR could ever satisfy minIsr; it must not.
    {
        int64_t nowNs = 0;
        for (int i = 0; i < 50; ++i) { nowNs += 1'000'000LL; replica->doWork(nowNs); }
    }
    ASSERT_EQ(replica->highWatermark(), 0)
        << "Precondition: HW starts at 0 (nothing appended yet)";

    // A byte-for-byte VALID v1 TopicSchema record, appended directly with
    // acksAll=false (bypasses the APPEND-time ISR gate -- with acksAll=true
    // and an unmet minIsr, committedIsrMeetsMinIsr_ would refuse the append
    // outright, so nothing would land in the log at all; acksAll=false is
    // what gets a record INTO the log without the ISR's blessing, exactly
    // the "appended, not yet committed" state G9-14 describes).
    TopicSchema ts;
    ts.topicId        = 99;
    ts.version        = 1;
    ts.partitionCount = 1;
    ts.validate       = false;
    ts.keyField       = "id";
    ts.descriptorBlob = serialize(DescriptorBuilder("hwgate", 1)
        .add("id", PrimType::Int64, 1, true).build());
    ts.nextTag        = 1;
    ts.compatMode     = CompatMode::Backward;
    const std::vector<uint8_t> buf = ts.serialize();

    const uint8_t keyBuf[6] = {0, 0, 0, 0, 0, 0};
    const Offset base = replica->appendKeyed(
        std::span<const uint8_t>(keyBuf, 6),
        std::span<const uint8_t>(buf.data(), buf.size()),
        /*acksAll=*/false);
    ASSERT_GE(base, 0) << "Precondition: acksAll=false must land the record in the log "
                           "even though minIsr is unmet";
    replica->onLeaderLogAppended();

    ASSERT_GT(replica->logEndOffset(), 0)
        << "Precondition: the record is actually present in the log";

    // Vacuousness guard: HW must stay pinned at 0 (unmet minIsr), or this
    // test exercises nothing.
    ASSERT_EQ(replica->highWatermark(), 0)
        << "Vacuousness guard: HW must stay pinned at 0 (unmet minIsr) for this test to "
           "actually exercise the gate; if this fails, PartitionReplica's HW-gating "
           "semantics changed and this test needs rethinking, not deleting";

    // More ticks -- still must not advance; the ISR requirement is
    // structurally unmet, not merely "hasn't caught up yet".
    {
        int64_t nowNs = 0;
        for (int i = 0; i < 50; ++i) { nowNs += 1'000'000LL; replica->doWork(nowNs); }
    }
    EXPECT_EQ(replica->highWatermark(), 0)
        << "HW must remain pinned regardless of how many doWork ticks run";

    // --- The actual G9-14 assertion: replay of [0,HW) must see NOTHING --
    //     the record is physically present in the log but excluded from
    //     replay because it is beyond (in fact, at-or-past) HW. ---
    SchemaRegistry reg;
    const bool ok = rebuildSchemaRegistryFromLog(replica.get(), reg);
    EXPECT_TRUE(ok) << "an empty (hw<=0) replay must report success, not failure";
    EXPECT_EQ(reg.byIdVersion(99, 1), nullptr)
        << "Regression: a record PAST the high-watermark must NOT be applied on replay "
           "-- only [0,HW) is ever committed/replayed, even though the record is "
           "physically present in the log";
}

// ---------------------------------------------------------------------------
// GC4 (G27-32): mixed data+schema replay.
//
// Builds a __topic_schemas lineage of 3 versions (v1 {id,amount}; v2 ADD
// bonus; v3 DROP amount) via the PRODUCTION AdminWorker commit path, AND a
// separate plain data-topic log containing one record PER version, each
// encoded against THAT version's own materialized layout (offsets shift:
// v1 blockLength=16 {id@0,amount@8}; v2 blockLength=24 {id@0,amount@8,
// bonus@16}; v3 blockLength=16 {id@0,bonus@8} -- amount's retirement
// reclaims its offset, same as the J1 DROP offset-reclaim rule). Simulates
// a restart (rebuild BOTH logs from scratch) and asserts every data record
// still decodes correctly using ITS OWN writer version's materialized
// descriptor (RegistryEntry::descriptor via byIdVersion) -- never a
// different version's descriptor, which would misread the wrong bytes as
// the wrong field (demonstrated by the final cross-version assertion).
// ---------------------------------------------------------------------------
TEST_F(SchemaPersistenceTest, MixedDataAndSchemaReplay_RecordsDecodePerWriterVersionRegression) {
    // --- Schema side: v1 {id(tag0,key), amount(tag1)} -> v2 ADD bonus(tag2)
    //     -> v3 DROP amount. ---
    DefineTopicMsg def;
    def.session        = 1;
    def.corr           = 2;
    def.name           = "events";
    def.keyField       = "id";
    def.partitionCount = 1;
    def.validate       = false;
    def.blob           = serialize(DescriptorBuilder("events", 1)
        .add("id",     PrimType::Int64, 1, true)
        .add("amount", PrimType::Int64)
        .build());
    driveDefine(def);
    ASSERT_EQ(lastDefineError_, 0u) << "Precondition: DefineTopic (v1) must succeed";
    const int32_t topicId = lastDefineTopicId_;

    AlterTopicMsg alterAdd;
    alterAdd.session         = 10;
    alterAdd.corr            = 11;
    alterAdd.topicName       = "events";
    alterAdd.expectedVersion = 0;
    alterAdd.opsBlob         = altersOpsBlob({addOp("bonus", PrimType::Int64)});
    driveAlter(alterAdd);
    ASSERT_EQ(lastAlterError_, 0u) << "Precondition: ALTER (ADD bonus, v2) must succeed";
    ASSERT_EQ(lastAlterVersion_, 2u);

    AlterTopicMsg alterDrop;
    alterDrop.session         = 12;
    alterDrop.corr            = 13;
    alterDrop.topicName       = "events";
    alterDrop.expectedVersion = 0;
    alterDrop.opsBlob         = altersOpsBlob({dropOp("amount")});
    driveAlter(alterDrop);
    ASSERT_EQ(lastAlterError_, 0u) << "Precondition: ALTER (DROP amount, v3) must succeed";
    ASSERT_EQ(lastAlterVersion_, 3u);

    tick(5);

    // --- Data side: a SEPARATE plain-topic log, one record per version,
    //     each encoded against that version's OWN layout. ---
    TmpSchemaDir dataDir("events_data");
    rune::storage::Log dataLog(dataDir.path);
    dataLog.recover();
    auto dataTransport = std::make_unique<NullPartitionTransport>();
    PartitionConfig dataCfg = makeSingleNodeSchemaConfig();
    dataCfg.topicId         = topicId;
    dataCfg.cleanupPolicy   = CleanupPolicy::Delete;
    auto dataReplica = std::make_unique<PartitionReplica>(
        dataCfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &dataLog, dataTransport.get());

    constexpr uint16_t kIdTag = 0, kAmountTag = 1, kBonusTag = 2;

    // v1 record: id=111, amount=222. blockLength=16 {id@0(8), amount@8(8)}.
    const std::vector<uint8_t> r1 =
        encodeFixedRecord(1, 16, {{0, 8, 111}, {8, 8, 222}});
    // v2 record: id=333, amount=444, bonus=555. blockLength=24.
    const std::vector<uint8_t> r2 =
        encodeFixedRecord(2, 24, {{0, 8, 333}, {8, 8, 444}, {16, 8, 555}});
    // v3 record: id=777, bonus=888 (amount dropped -> bonus's offset
    // reclaims amount's old slot). blockLength=16 {id@0(8), bonus@8(8)}.
    const std::vector<uint8_t> r3 =
        encodeFixedRecord(3, 16, {{0, 8, 777}, {8, 8, 888}});

    for (const auto& rec : {r1, r2, r3}) {
        appendRawSchemaRecord(dataReplica.get(),
                               std::span<const uint8_t>(rec.data(), rec.size()));
    }
    {
        int64_t nowNs = 0;
        for (int i = 0; i < 5; ++i) { nowNs += 1'000'000LL; dataReplica->doWork(nowNs); }
    }

    // --- Simulate a restart: rebuild BOTH logs from scratch. ---
    SchemaRegistry freshSchemaReg;
    ASSERT_TRUE(rebuildSchemaRegistryFromLog(replica_.get(), freshSchemaReg));

    const RegistryEntry* e1 = freshSchemaReg.byIdVersion(topicId, 1);
    const RegistryEntry* e2 = freshSchemaReg.byIdVersion(topicId, 2);
    const RegistryEntry* e3 = freshSchemaReg.byIdVersion(topicId, 3);
    ASSERT_NE(e1, nullptr); ASSERT_NE(e2, nullptr); ASSERT_NE(e3, nullptr);

    const std::vector<std::vector<uint8_t>> dataRecords = readAllRecordValues(dataReplica.get());
    ASSERT_EQ(dataRecords.size(), 3u) << "all 3 data records must survive replay";

    // --- Each record decodes correctly using ITS OWN writer version's
    //     materialized descriptor (read from the embedded header version,
    //     dispatched via byIdVersion -- exactly how a real consumer would). ---
    auto headerVersion = [](const std::vector<uint8_t>& rec) -> uint16_t {
        return static_cast<uint16_t>(rec[2] | (rec[3] << 8));
    };

    const RegistryEntry* entries[3] = {e1, e2, e3};
    for (const auto& rec : dataRecords) {
        const uint16_t v = headerVersion(rec);
        ASSERT_GE(v, 1); ASSERT_LE(v, 3);
        const RegistryEntry* e = entries[v - 1];
        const std::span<const uint8_t> span(rec.data(), rec.size());

        const auto id = readField(e->descriptor, span, kIdTag);
        ASSERT_TRUE(id.present) << "id must decode at v" << v;

        if (v == 1) {
            EXPECT_EQ(id.i, 111);
            const auto amount = readField(e->descriptor, span, kAmountTag);
            ASSERT_TRUE(amount.present) << "amount must be present at v1";
            EXPECT_EQ(amount.i, 222);
            EXPECT_FALSE(readField(e->descriptor, span, kBonusTag).present)
                << "bonus does not exist yet at v1";
        } else if (v == 2) {
            EXPECT_EQ(id.i, 333);
            const auto amount = readField(e->descriptor, span, kAmountTag);
            ASSERT_TRUE(amount.present) << "amount must still be present at v2";
            EXPECT_EQ(amount.i, 444);
            const auto bonus = readField(e->descriptor, span, kBonusTag);
            ASSERT_TRUE(bonus.present) << "bonus must be present at v2";
            EXPECT_EQ(bonus.i, 555);
        } else {
            EXPECT_EQ(id.i, 777);
            EXPECT_FALSE(readField(e->descriptor, span, kAmountTag).present)
                << "amount was dropped at v3 -- must decode as absent, never garbage";
            const auto bonus = readField(e->descriptor, span, kBonusTag);
            ASSERT_TRUE(bonus.present) << "bonus must be present at v3";
            EXPECT_EQ(bonus.i, 888);
        }
    }

    // --- Contrast: decoding the v3 record with the WRONG (v2) descriptor
    //     does NOT fail loud -- it silently returns bonus's byte range
    //     mislabeled as "amount" (888, bonus's real value) instead of the
    //     correct v3 answer (amount is retired -> absent, asserted above
    //     via e3->descriptor). This is exactly why decode must always use
    //     the WRITER's own version's materialized descriptor. ---
    const auto wrongDecode = readField(e2->descriptor,
                                        std::span<const uint8_t>(r3.data(), r3.size()),
                                        kAmountTag);
    ASSERT_TRUE(wrongDecode.present);
    EXPECT_EQ(wrongDecode.i, 888)
        << "cross-version misread: the v2 descriptor's 'amount' offset lands on what is "
           "actually r3's 'bonus' bytes -- proving correctness depends on decoding each "
           "record against ITS OWN writer version, never a different version's descriptor";
}

// ---------------------------------------------------------------------------
// GC4 (G51-56): replay idempotence / time-independence.
//
// Replaying the identical __topic_schemas log TWICE (two independent FRESH
// SchemaRegistry instances, same boot-replay mirror) must yield
// byte-identical registry state: same versions, nextTag, compatMode,
// fingerprints, and materialized field layout for every version -- not
// merely "equivalent", but IDENTICAL, regardless of how many times or in
// what order replay runs against the same [0,HW) byte range.
// ---------------------------------------------------------------------------
TEST_F(SchemaPersistenceTest, ReplayTwiceYieldsByteIdenticalRegistryStateRegression) {
    DefineTopicMsg def;
    def.session = 1; def.corr = 2; def.name = "characters";
    def.keyField = "userId"; def.partitionCount = 4; def.validate = false;
    def.blob = charsBlob();
    driveDefine(def);
    ASSERT_EQ(lastDefineError_, 0u) << "Precondition: DefineTopic (v1) must succeed";
    const int32_t topicId = lastDefineTopicId_;

    AlterTopicMsg alter1;
    alter1.session = 10; alter1.corr = 11; alter1.topicName = "characters";
    alter1.expectedVersion = 0;
    alter1.opsBlob = altersOpsBlob({addOp("score", PrimType::Int64)});
    driveAlter(alter1);
    ASSERT_EQ(lastAlterError_, 0u);
    ASSERT_EQ(lastAlterVersion_, 2u);

    AlterTopicMsg alter2;
    alter2.session = 12; alter2.corr = 13; alter2.topicName = "characters";
    alter2.expectedVersion = 0;
    alter2.opsBlob = altersOpsBlob({setCompatOp(CompatMode::Forward)});
    driveAlter(alter2);
    ASSERT_EQ(lastAlterError_, 0u);
    ASSERT_EQ(lastAlterVersion_, 3u);

    tick(5);

    SchemaRegistry replayA;
    SchemaRegistry replayB;
    ASSERT_TRUE(rebuildSchemaRegistryFromLog(replica_.get(), replayA));
    ASSERT_TRUE(rebuildSchemaRegistryFromLog(replica_.get(), replayB));

    expectRegistriesByteIdenticalForAllVersions(replayA, replayB, topicId, /*maxVersion=*/3);

    // A third replay, after an intervening read() on the same replica (a
    // read must never mutate state) -- must agree too: replay order/count
    // never changes the outcome.
    (void)replica_->read(0, 1 << 20);
    SchemaRegistry replayC;
    ASSERT_TRUE(rebuildSchemaRegistryFromLog(replica_.get(), replayC));
    expectRegistriesByteIdenticalForAllVersions(replayA, replayC, topicId, /*maxVersion=*/3);
}

// ===========================================================================
// GC5: compaction x long-lineage mixed-read (H3-H14, C71-78 of
// docs/superpowers/specs/2026-10-08-rune-schema-evolution-test-matrix.md).
//
// Extends OldVersionsSurviveCompactionRegression (3-version lineage, v1
// fingerprint-only check) to a LONG 12-version lineage with a genuine MIX of
// ADD/DROP/RENAME, run through the exact same production machinery:
//   - real AdminWorker DefineTopic + 11x ALTER commits (never a hand-rolled
//     TopicSchema/registry poke),
//   - a Log configured with segmentRollBytes=1 (every append rolls a fresh
//     segment, so all 12 versions are individually compactable -- not stuck
//     together in one never-compacted active segment),
//   - the REAL duty-cycle compaction path (rune::node::NodeRuntimeAgent::
//     runBoundedCompaction over a rune::node::ReplicaRegistry) -- the exact
//     harness OldVersionsSurviveCompactionRegression and
//     CompactionCorrectnessTest.cpp use, NOT a fake.
//
// Lineage (tags assigned in ADD order; a dash under "op" means "unchanged
// from the previous version"):
//   v1  DEFINE   id(tag0,key) amount(tag1) label(tag2)
//   v2  ADD      score(tag3)
//   v3  ADD      bonus(tag4)
//   v4  DROP     amount (tag1 retired)
//   v5  RENAME   label -> title            (tag2 unchanged)
//   v6  ADD      flag(tag5)
//   v7  DROP     score (tag3 retired)
//   v8  RENAME   title -> caption          (tag2 unchanged)
//   v9  ADD      extra(tag6)
//   v10 DROP     bonus (tag4 retired)
//   v11 RENAME   caption -> headline       (tag2 unchanged)
//   v12 ADD      final(tag7)
//
// Assertions (H3-H8): EVERY version 1..12 survives compaction --
// byIdVersion(topicId,K) present for all K, AND byFingerprint(topicId,fp(vK))
// resolves to that same entry for all K (fp captured from an independent
// PRE-compaction replay, exactly like the old-fingerprint-client-binding
// rationale the base regression documents, but for the FULL lineage, not
// just v1).
//
// Edge (H1/H9-H14 tie-in): the compaction key is (topicId,version), so
// distinct versions are distinct keys and none are evicted -- confirmed by
// re-reading the raw post-compaction log and asserting exactly 12 distinct
// (topicId,version) records survive (none lost, none duplicated).
//
// Mixed read (C71-78): after compaction, synthetic data records written
// under v1, v5, v9, and v12 (spanning the full ADD/DROP/RENAME history) are
// decoded EACH against its own writer version's surviving materialized
// descriptor (postCompactReg.byIdVersion(topicId, writerVersion)->descriptor)
// -- every live field at that version decodes to its expected value, and
// every tag NOT live at that version (retired before it, or not yet added)
// reads definitively absent, never garbage from a neighboring version's
// offset table.
// ===========================================================================

// Build a raw fixed-block test record for one post-compaction RegistryEntry:
// every field LIVE at `entry.version` gets a distinct deterministic value
// (version*1000 + durable tag), laid out at that field's OWN materialized
// offset (rune::codec::materializeAsOf's real output, read back from the
// registry -- never hand-computed), so the record is byte-correct for
// exactly the schema version it claims in its header. Reuses
// encodeFixedRecord (defined above for MixedDataAndSchemaReplay_*).
static std::vector<uint8_t> encodeRecordForEntry(const RegistryEntry& entry) {
    std::vector<std::tuple<uint32_t, uint32_t, uint64_t>> fields;
    fields.reserve(entry.descriptor.fields.size());
    for (const auto& f : entry.descriptor.fields) {
        const uint64_t value = static_cast<uint64_t>(entry.version) * 1000ULL
                              + static_cast<uint64_t>(f.id);
        fields.emplace_back(f.offset, 8u, value);
    }
    return encodeFixedRecord(entry.version, entry.descriptor.blockLength, fields);
}

TEST(SchemaPersistenceCompaction, LongLineageSurvivesCompactionAndMixedReadRegression) {
    TmpSchemaDir dir("compact_long_lineage");

    // segmentRollBytes=1: every append rolls a fresh segment (same trick
    // OldVersionsSurviveCompactionRegression uses), so all 12 appends land
    // in up to 12 distinct segments -- every version except the very last
    // ends up sealed/compactable.
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
    deps.requestCreateTopic = [&](int64_t ticket, int32_t, const std::string&, const std::string&, uint8_t) -> bool {
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
    auto driveAlterLocal = [&](const std::string& topicName,
                                const std::vector<AlterOp>& ops,
                                uint16_t expectedNewVersion) {
        AlterTopicMsg alter;
        alter.session         = 100 + expectedNewVersion;
        alter.corr            = 200 + expectedNewVersion;
        alter.topicName       = topicName;
        alter.expectedVersion = 0;
        alter.opsBlob         = altersOpsBlob(ops);
        worker.submitAlterTopic(alter);
        int64_t nowNs = 0;
        for (int i = 0; i < 50; ++i) {
            nowNs += 1'000'000LL;
            worker.doWork(nowNs);
        }
        ASSERT_EQ(lastAlterError, 0u)
            << "Precondition: ALTER -> v" << expectedNewVersion << " must succeed";
        ASSERT_EQ(lastAlterVersion, expectedNewVersion);
    };

    // v1: DEFINE "ledger" {id(key), amount, label} -- all Int64 for a
    // uniform fixed-block encode/decode in the mixed-read phase below (the
    // per-type matrix (C31-40) is covered elsewhere; this test's job is the
    // long-lineage x compaction interaction, not type coverage).
    DefineTopicMsg def;
    def.session        = 1;
    def.corr            = 2;
    def.name           = "ledger";
    def.keyField       = "id";
    def.partitionCount = 4;
    def.validate       = false;
    def.blob           = serialize(DescriptorBuilder("ledger", 1)
        .add("id",     PrimType::Int64, 1, true)
        .add("amount", PrimType::Int64)
        .add("label",  PrimType::Int64)
        .build());
    driveDefineLocal(def);
    ASSERT_EQ(lastDefineError, 0u) << "Precondition: DefineTopic (v1) must succeed";
    const int32_t topicId = lastDefineTopicId;

    // v2..v12: the ADD/DROP/RENAME mix documented in the header comment above.
    driveAlterLocal("ledger", {addOp("score", PrimType::Int64)}, 2);
    driveAlterLocal("ledger", {addOp("bonus", PrimType::Int64)}, 3);
    driveAlterLocal("ledger", {dropOp("amount")}, 4);
    driveAlterLocal("ledger", {renameOp("label", "title")}, 5);
    driveAlterLocal("ledger", {addOp("flag", PrimType::Int64)}, 6);
    driveAlterLocal("ledger", {dropOp("score")}, 7);
    driveAlterLocal("ledger", {renameOp("title", "caption")}, 8);
    driveAlterLocal("ledger", {addOp("extra", PrimType::Int64)}, 9);
    driveAlterLocal("ledger", {dropOp("bonus")}, 10);
    driveAlterLocal("ledger", {renameOp("caption", "headline")}, 11);
    driveAlterLocal("ledger", {addOp("final", PrimType::Int64)}, 12);

    constexpr uint16_t kLineageLength = 12;

    // Belt-and-suspenders HW advance (same pattern as the base regression).
    {
        int64_t nowNs = 0;
        for (int i = 0; i < 5; ++i) { nowNs += 1'000'000LL; replica->doWork(nowNs); }
    }

    // Capture every version's fingerprint BEFORE compaction, from an
    // independent replay -- these are the values an old client's typed gate
    // would have compiled against at each point in the lineage, and every
    // one of them must still resolve via byFingerprint() AFTER compaction.
    SchemaRegistry preCompactReg;
    ASSERT_TRUE(rebuildSchemaRegistryFromLog(replica.get(), preCompactReg));
    std::vector<uint64_t> fingerprintByVersion(kLineageLength + 1, 0);
    for (uint16_t v = 1; v <= kLineageLength; ++v) {
        const RegistryEntry* e = preCompactReg.byIdVersion(topicId, v);
        ASSERT_NE(e, nullptr) << "Precondition: v" << v << " must be present before compaction";
        fingerprintByVersion[v] = e->fingerprint;
    }

    // Sanity: rollBytes=1 must have produced one segment per append (11
    // sealed + 1 active) -- or this test would not exercise compaction of
    // the FULL lineage (vacuousness guard, same intent as the base
    // regression's >=2u check, scaled to 12 versions).
    const auto basesBefore = log.segmentBaseOffsets();
    ASSERT_GE(basesBefore.size(), static_cast<std::size_t>(kLineageLength))
        << "Need >= 12 segments (11 sealed + active) to exercise compaction "
           "across the ENTIRE long lineage -- vacuousness guard";

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
    ASSERT_TRUE(rebuildSchemaRegistryFromLog(rawReplica, postCompactReg));

    // H3-H8: EVERY version 1..12 survives, latest()==12, and EVERY version's
    // PRE-compaction fingerprint still resolves post-compaction.
    const RegistryEntry* entryByVersion[kLineageLength + 1] = {};
    for (uint16_t v = 1; v <= kLineageLength; ++v) {
        const RegistryEntry* e = postCompactReg.byIdVersion(topicId, v);
        ASSERT_NE(e, nullptr)
            << "v" << v << " must survive compaction of the full 12-version lineage -- "
               "the (topicId,version) compaction key must keep every version distinct";
        EXPECT_EQ(e->version, v);
        entryByVersion[v] = e;

        EXPECT_EQ(postCompactReg.byFingerprint(topicId, fingerprintByVersion[v]), e)
            << "byFingerprint for v" << v << "'s PRE-compaction fingerprint must still "
               "resolve to the SAME entry after compaction";
    }

    const RegistryEntry* lat = postCompactReg.latest(topicId);
    ASSERT_NE(lat, nullptr);
    EXPECT_EQ(lat->version, kLineageLength) << "latest() must be v12 after compaction + rebuild";

    // Edge (H1/H9-H14 tie-in): the compacted __topic_schemas log itself still
    // contains exactly 12 distinct (topicId,version) records for this topic
    // -- none evicted (would show < 12), none duplicated (would show >12 or
    // a repeated version), confirming the compaction key really is
    // (topicId,version), not topicId alone.
    {
        const std::vector<std::vector<uint8_t>> rawSchemaRecords = readAllRecordValues(rawReplica);
        std::vector<uint16_t> versionsSeen;
        for (const auto& raw : rawSchemaRecords) {
            const auto ts = TopicSchema::deserialize(
                std::span<const uint8_t>(raw.data(), raw.size()));
            ASSERT_TRUE(ts.has_value()) << "every surviving raw schema record must deserialize";
            if (ts->topicId == topicId) versionsSeen.push_back(ts->version);
        }
        ASSERT_EQ(versionsSeen.size(), static_cast<std::size_t>(kLineageLength))
            << "the compacted log must retain exactly 12 distinct records for this topic "
               "-- (topicId,version) keying means none of the 12 versions collide";
        std::sort(versionsSeen.begin(), versionsSeen.end());
        for (uint16_t v = 1; v <= kLineageLength; ++v) {
            EXPECT_EQ(versionsSeen[v - 1], v)
                << "surviving record set must be EXACTLY {1..12}, no gaps/dupes";
        }
    }

    // --- C71-78: mixed read AFTER compaction. Synthetic data records
    //     written under v1, v5, v9, and v12 (spanning the full ADD/DROP/
    //     RENAME history) must each decode correctly against ITS OWN
    //     writer version's surviving post-compaction descriptor. ---
    TmpSchemaDir dataDir("ledger_data");
    rune::storage::Log dataLog(dataDir.path);
    dataLog.recover();
    auto dataTransport = std::make_unique<NullPartitionTransport>();
    PartitionConfig dataCfg = makeSingleNodeSchemaConfig();
    dataCfg.topicId         = topicId;
    dataCfg.cleanupPolicy   = CleanupPolicy::Delete;
    auto dataReplica = std::make_unique<PartitionReplica>(
        dataCfg, Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &dataLog, dataTransport.get());

    const uint16_t mixedVersions[] = {1, 5, 9, 12};
    for (const uint16_t v : mixedVersions) {
        const std::vector<uint8_t> rec = encodeRecordForEntry(*entryByVersion[v]);
        appendRawSchemaRecord(dataReplica.get(),
                               std::span<const uint8_t>(rec.data(), rec.size()));
    }
    {
        int64_t nowNs = 0;
        for (int i = 0; i < 5; ++i) { nowNs += 1'000'000LL; dataReplica->doWork(nowNs); }
    }

    const std::vector<std::vector<uint8_t>> dataRecords = readAllRecordValues(dataReplica.get());
    ASSERT_EQ(dataRecords.size(), 4u) << "all 4 mixed-version data records must survive replay";

    auto headerVersion = [](const std::vector<uint8_t>& rec) -> uint16_t {
        return static_cast<uint16_t>(rec[2] | (rec[3] << 8));
    };

    // Every durable tag ever allocated across the lineage (tags 0..7) --
    // used below to assert BOTH presence (live fields decode to their
    // expected value) and absence (retired-or-not-yet-added fields decode
    // as absent, never garbage) for each record's writer version.
    constexpr uint16_t kMaxTagExclusive = 8;

    for (const auto& rec : dataRecords) {
        const uint16_t v = headerVersion(rec);
        const RegistryEntry* e = postCompactReg.byIdVersion(topicId, v);
        ASSERT_NE(e, nullptr) << "writer version v" << v << " must resolve post-compaction";
        EXPECT_EQ(e, entryByVersion[v])
            << "mixed-read decode must dispatch to the SAME surviving entry captured above";

        std::vector<bool> liveTag(kMaxTagExclusive, false);
        for (const auto& f : e->descriptor.fields) liveTag[f.id] = true;

        const std::span<const uint8_t> span(rec.data(), rec.size());
        for (uint16_t tag = 0; tag < kMaxTagExclusive; ++tag) {
            const auto fv = readField(e->descriptor, span, tag);
            if (liveTag[tag]) {
                ASSERT_TRUE(fv.present)
                    << "tag " << tag << " is live at v" << v << " and must decode present";
                EXPECT_EQ(fv.i, static_cast<int64_t>(v) * 1000 + tag)
                    << "tag " << tag << " at v" << v << " decoded the wrong value -- "
                       "likely decoded against the wrong version's descriptor";
            } else {
                EXPECT_FALSE(fv.present)
                    << "tag " << tag << " is NOT live at v" << v << " (retired before it or "
                       "not yet added) and must decode absent, never garbage from a "
                       "neighboring version's offset table";
            }
        }
    }
}

// ---------------------------------------------------------------------------
// REGRESSION (user-topic restart recovery) — writer/reader contract on the REAL
// DDL path.  A restarted broker recreates runtime topics from __topic_schemas
// (recoverRuntimeTopicSpecs); the topic must come back under the name the client
// gave DefineTopic — which is independent of the descriptor's name — or boot
// opens the wrong on-disk log.  Drive the real AdminWorker, then run boot
// recovery over a data dir holding exactly the catalog it wrote.
// ---------------------------------------------------------------------------
TEST_F(SchemaPersistenceTest, DefineTopicRecordRecoversTopicUnderItsOwnNameRegression) {
    DefineTopicMsg args;
    args.session        = 30;
    args.corr           = 31;
    args.name           = "orders_topic";   // descriptor (charsBlob) is named "characters"
    args.keyField       = "userId";
    args.partitionCount = 6;
    args.validate       = false;
    args.blob           = charsBlob();

    driveDefine(args);
    ASSERT_EQ(lastDefineError_, 0u) << "Precondition: DefineTopic must succeed";
    tick(5);

    TmpSchemaDir dataDir("rtr_data");
    std::filesystem::copy(schemaDir_.path,
                          std::filesystem::path(dataDir.path) / "__topic_schemas",
                          std::filesystem::copy_options::recursive);

    const auto specs = rune::node::recoverRuntimeTopicSpecs(dataDir.path, /*firstRuntimeId*/ 0);
    ASSERT_EQ(specs.size(), 1u);
    EXPECT_EQ(specs[0].topicId, lastDefineTopicId_);
    EXPECT_EQ(specs[0].name, "orders_topic") << "must be the TOPIC name, not the descriptor name";
    EXPECT_EQ(specs[0].partitionCount, 6);
    EXPECT_EQ(specs[0].cleanupPolicy, rune::node::TopicSpec::CleanupPolicy::Delete);
}
