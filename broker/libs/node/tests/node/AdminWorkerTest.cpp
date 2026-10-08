// AdminWorkerTest.cpp — Task 8a unit tests for the async DDL state machine.
//
// The AdminWorker now SOLELY owns the __topic_schemas PartitionReplica (RULING-2)
// and the DDL path is ASYNC: createTopic is a ring round-trip to the (simulated)
// control thread.  The test harness plays the control thread by capturing
// createTopic requests via deps.requestCreateTopic and feeding the reply back via
// onCreateTopicReply, then driving doWork() to advance the replica HW (single-node
// leader → HW advances immediately) so AWAITING_COMMIT resolves.

#include <gtest/gtest.h>
#include "rune/node/AdminWorker.h"
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/Log.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/AlterOps.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>

using namespace rune::node;
using namespace rune::codec;
using namespace rune::replication;

namespace {

std::vector<uint8_t> charsBlob() {
    return serialize(DescriptorBuilder("characters",1)
        .add("id",PrimType::Int64).add("userId",PrimType::Int64,1,true)
        .add("name",PrimType::String).build());
}

std::string uniqDir(const char* tag) {
    static std::atomic<uint64_t> seq{0};
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = std::filesystem::temp_directory_path().string() + "/rune_awt_" + tag + "_"
           + std::to_string(seq.fetch_add(1)) + "_" + std::to_string(ts);
    std::filesystem::create_directories(p);
    return p;
}

class NullPartitionTransport : public PartitionTransport {
public:
    bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const FetchResponseMsg&) override { return true; }
};

// Harness: real single-node leader schema replica + AdminWorker + a simulated
// control thread that answers createTopic requests.
struct Harness {
    std::string dir;
    rune::storage::Log log;
    std::unique_ptr<NullPartitionTransport> transport;
    std::unique_ptr<PartitionReplica> replica;
    SchemaRegistry reg;
    std::unique_ptr<AdminWorker> w;

    // Captured control-thread state.
    int  creates = 0, pushes = 0;
    int32_t nextTopicId = 0;
    int32_t lastTopicId = -1;
    uint16_t lastErr = 0xffff;
    int getCount = 0;
    // ALTER response capture (Task 11).
    int      alterResponses  = 0;
    uint16_t lastAlterErr     = 0xffff;
    uint16_t lastAlterVersion = 0;
    int32_t  lastAlterTopicId = -1;
    // Pending createTopic request (ticket, partitionCount) awaiting a reply.
    int64_t pendingTicket = -1;
    bool    failCreate = false;

    explicit Harness()
        : dir(uniqDir("h")), log(dir) {
        log.recover();
        transport = std::make_unique<NullPartitionTransport>();
        PartitionConfig cfg;
        cfg.topicId = kSchemaTopicId; cfg.partitionId = 0; cfg.selfId = 1;
        cfg.minIsr = 1; cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL;
        cfg.maxFetchBytes = 1 << 20; cfg.cleanupPolicy = CleanupPolicy::Compact;
        replica = std::make_unique<PartitionReplica>(
            cfg, Role::Follower, /*epoch=*/0,
            std::vector<NodeId>{1}, std::vector<NodeId>{1}, &log, transport.get());

        AdminDeps d;
        d.requestCreateTopic = [this](int64_t ticket, int32_t, const std::string&) -> bool {
            ++creates; pendingTicket = ticket; return true;
        };
        d.pushValidators = [this](const RegistryEntry&, int32_t) { ++pushes; };
        d.respondDefine = [this](int64_t,int64_t,int32_t tid,uint16_t,uint16_t e) {
            lastTopicId = tid; lastErr = e;
        };
        d.respondGet = [this](int64_t,int64_t,int32_t,uint16_t,const std::vector<uint8_t>&,uint16_t e) {
            ++getCount; lastErr = e;
        };
        d.respondAlter = [this](int64_t,int64_t,int32_t tid,uint16_t v,uint16_t e) {
            ++alterResponses; lastAlterTopicId = tid; lastAlterVersion = v; lastAlterErr = e;
        };
        w = std::make_unique<AdminWorker>(reg, replica.get(), 1,
                                          std::vector<int32_t>{1}, std::move(d));
        // Promote to leader (control thread posts a role event; here direct).
        w->onRole(AdminRoleMsg{/*leader=*/true, /*epoch=*/1, /*leaderId=*/1});
    }

    // Drive one full DDL to completion: submit → doWork (starts + requests) →
    // control reply → doWork loop until the state machine settles.
    void runDefine(int64_t session, int64_t corr, const std::string& name,
                   const std::string& keyField, int32_t parts, bool validate,
                   std::vector<uint8_t> blob) {
        DefineTopicMsg m;
        m.session = session; m.corr = corr; m.name = name; m.keyField = keyField;
        m.partitionCount = parts; m.validate = validate; m.blob = std::move(blob);
        w->submitDefineTopic(std::move(m));
        pendingTicket = -1;
        int64_t nowNs = 0;
        for (int i = 0; i < 50; ++i) {
            nowNs += 1'000'000LL;
            w->doWork(nowNs);
            // Simulate the control thread answering a pending createTopic request.
            if (pendingTicket >= 0) {
                CreateTopicReplyMsg reply;
                reply.ticket  = pendingTicket;
                reply.topicId = failCreate ? -1 : nextTopicId++;
                pendingTicket = -1;
                w->onCreateTopicReply(reply);
            }
            if (w->pendingCount() == 0) {
                // keep ticking a few cycles so AWAITING_COMMIT HW gate resolves
            }
        }
    }

    // Drive one ALTER to completion.  ALTER skips the createTopic round-trip, so
    // we just submit and pump doWork() until the single-node HW gate resolves.
    void runAlter(int64_t session, int64_t corr, const std::string& name,
                  uint16_t expectedVersion, std::vector<uint8_t> opsBlob) {
        AlterTopicMsg m;
        m.session = session; m.corr = corr; m.topicName = name;
        m.expectedVersion = expectedVersion; m.opsBlob = std::move(opsBlob);
        w->submitAlterTopic(std::move(m));
        int64_t nowNs = 0;
        for (int i = 0; i < 50; ++i) { nowNs += 1'000'000LL; w->doWork(nowNs); }
    }
};

// Build an ALTER ops blob (shared rune-ql/broker wire format) from `ops`.
std::vector<uint8_t> opsBlob(const std::vector<AlterOp>& ops) {
    return encodeAlterOps(ops);
}

AlterOp addOp(const std::string& name, PrimType t) {
    AlterOp op{}; op.type = AlterOpType::Add; op.name = name; op.primType = t; op.fixedLen = 0;
    return op;
}
AlterOp dropOp(const std::string& name) {
    AlterOp op{}; op.type = AlterOpType::Drop; op.name = name; return op;
}
AlterOp setCompatOp(CompatMode m) {
    AlterOp op{}; op.type = AlterOpType::SetCompatibility; op.mode = m; return op;
}

} // namespace

TEST(AdminWorker, DefineTopicHappyPath) {
    Harness h;
    h.runDefine(1,2,"characters","userId",16,true,charsBlob());
    EXPECT_EQ(h.creates,1);
    EXPECT_EQ(h.pushes,1);                       // validate=true → validators pushed
    EXPECT_EQ(h.lastErr,0);                       // OK
    EXPECT_EQ(h.lastTopicId, 0);                  // first sequential id
    ASSERT_NE(h.reg.byName("characters"), nullptr);
}

TEST(AdminWorker, RejectsMalformedDescriptor) {
    Harness h;
    h.runDefine(1,2,"bad","",1,false,{0,1,2});    // not a valid blob
    EXPECT_EQ(h.creates,0);                        // never reached createTopic
    EXPECT_EQ(h.lastErr,23);                       // InvalidSchema
}

TEST(AdminWorker, DuplicateNameReturnsTopicExists) {
    Harness h;
    h.runDefine(1,2,"characters","userId",16,false,charsBlob());
    ASSERT_EQ(h.lastErr,0);
    h.lastErr = 0xffff;
    h.runDefine(1,3,"characters","userId",16,false,charsBlob());
    EXPECT_EQ(h.lastErr,25);                       // TopicExists
    EXPECT_EQ(h.creates,1);                         // no second create
}

TEST(AdminWorker, NoValidatorPushWhenValidateOff) {
    Harness h;
    h.runDefine(1,2,"characters","userId",16,false,charsBlob());
    EXPECT_EQ(h.pushes,0);
    EXPECT_EQ(h.lastErr,0);
}

TEST(AdminWorker, GetSchemaUnknownTopic) {
    Harness h;
    h.w->submitGetSchema(GetSchemaMsg{1,2,"nope"});
    EXPECT_EQ(h.lastErr,7);                         // UnknownTopic
}

TEST(AdminWorker, GetSchemaKnownTopic) {
    Harness h;
    h.runDefine(1,2,"characters","userId",16,false,charsBlob());
    ASSERT_EQ(h.lastErr,0);
    h.lastErr = 0xffff;
    h.w->submitGetSchema(GetSchemaMsg{1,2,"characters"});
    EXPECT_EQ(h.lastErr,0);                          // OK
}

TEST(AdminWorker, CreateTopicFailureRespondsInternal) {
    Harness h;
    h.failCreate = true;
    h.runDefine(1,2,"characters","userId",16,false,charsBlob());
    EXPECT_EQ(h.lastErr,6);                          // Internal
    EXPECT_EQ(h.reg.byName("characters"), nullptr);  // no registry update
}

// Regression (RULING-2 D): appendDurable is committed only when the replica HW
// covers the appended offset — never on base>=0 alone.  On a single-node leader
// (minIsr=1) HW advances on doWork; the happy path proves the HW gate resolves.
TEST(AdminWorkerRegression, CommitGatedOnHighWatermark) {
    Harness h;
    h.runDefine(1,2,"characters","userId",16,false,charsBlob());
    EXPECT_EQ(h.lastErr,0);
    EXPECT_GE(h.log.logEndOffset(), 1LL);
    EXPECT_GE(h.replica->highWatermark(), 1LL);
}

// Regression (RULING-2 D): DDL is leader-only.  A follower AdminWorker must
// reject DefineTopic with Internal rather than asserting.
TEST(AdminWorkerRegression, FollowerRejectsDefine) {
    Harness h;
    h.w->onRole(AdminRoleMsg{/*leader=*/false, /*epoch=*/2, /*leaderId=*/2});
    h.runDefine(1,2,"characters","userId",16,false,charsBlob());
    EXPECT_EQ(h.lastErr,6);                          // Internal (not leader)
    EXPECT_EQ(h.creates,0);
}

// Regression (Task 10): the create path must route version/nextTag through a
// single contiguous-version helper rather than a hardcoded literal. A freshly
// defined topic is still version 1 (no behavior change), and nextTag starts
// past every field the schema already declares (fieldCount) so Task 11's
// ALTER allocates a fresh tag instead of colliding with an existing one.
TEST(AdminWorkerRegression, DefineTopicVersionOneAndNextTagEqualsFieldCount) {
    Harness h;
    h.runDefine(1,2,"characters","userId",16,false,charsBlob());
    ASSERT_EQ(h.lastErr,0);
    EXPECT_EQ(h.lastTopicId, 0);
    const RegistryEntry* e = h.reg.byName("characters");
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->version, 1);
    // charsBlob(): id, userId, name == 3 fields.
    EXPECT_EQ(h.reg.nextTagFor(e->topicId), 3);
}

// ── Task 11: ALTER TOPIC ─────────────────────────────────────────────────────

// Happy path: ADD a column bumps the version contiguously (v1 → v2) and the new
// field is allocated a fresh durable tag == the pre-ALTER field count (3).
TEST(AdminWorker, AlterAddColumnBumpsVersionAndAllocatesTag) {
    Harness h;
    h.runDefine(1,2,"characters","userId",16,false,charsBlob());
    ASSERT_EQ(h.lastErr,0);
    const int32_t tid = h.reg.byName("characters")->topicId;

    h.runAlter(10,11,"characters",/*expected=*/0, opsBlob({addOp("score",PrimType::Int64)}));
    EXPECT_EQ(h.lastAlterErr, 0);
    EXPECT_EQ(h.lastAlterVersion, 2);

    const RegistryEntry* e = h.reg.byName("characters");
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->version, 2);
    // nextTag advanced past the newly allocated tag (3 → 4).
    EXPECT_EQ(h.reg.nextTagFor(tid), 4);
    // The new field is live at v2 and carries durable tag 3 (== old fieldCount).
    const auto& fields = e->descriptor.fields;
    auto it = std::find_if(fields.begin(), fields.end(),
                           [](const auto& f){ return f.name == "score"; });
    ASSERT_NE(it, fields.end());
    EXPECT_EQ(it->id, 3);
}

// Regression (compat gate): DROP under FORWARD compatibility is rejected with
// SchemaIncompatible and the version is NOT bumped.  Also pins that the compat
// mode set by a prior ALTER persists into the next ALTER's gate.
TEST(AdminWorkerRegression, AlterDropUnderForwardRejected) {
    Harness h;
    h.runDefine(1,2,"characters","userId",16,false,charsBlob());
    ASSERT_EQ(h.lastErr,0);

    // ALTER #1: switch to FORWARD compatibility → v2.
    h.runAlter(10,11,"characters",0, opsBlob({setCompatOp(CompatMode::Forward)}));
    ASSERT_EQ(h.lastAlterErr, 0);
    ASSERT_EQ(h.reg.byName("characters")->version, 2);

    // ALTER #2: DROP a live field under the now-persisted FORWARD mode → reject.
    h.runAlter(12,13,"characters",0, opsBlob({dropOp("name")}));
    EXPECT_EQ(h.lastAlterErr, 29);                       // SchemaIncompatible
    EXPECT_EQ(h.reg.byName("characters")->version, 2);   // version unchanged
}

// Regression (optimistic CAS): a stale expectedVersion must be rejected with
// SchemaConflict and must NOT clobber the newer committed version (no lost update).
TEST(AdminWorkerRegression, AlterWithStaleExpectedVersionConflicts) {
    Harness h;
    h.runDefine(1,2,"characters","userId",16,false,charsBlob());
    ASSERT_EQ(h.lastErr,0);

    // Unconditional ALTER → v2.
    h.runAlter(10,11,"characters",0, opsBlob({addOp("score",PrimType::Int64)}));
    ASSERT_EQ(h.lastAlterErr, 0);
    ASSERT_EQ(h.reg.byName("characters")->version, 2);

    // Now a client that still thinks the topic is v1 tries to ALTER with
    // expectedVersion=1 → stale → SchemaConflict, registry stays v2.
    h.runAlter(12,13,"characters",/*expected=*/1, opsBlob({addOp("extra",PrimType::Int64)}));
    EXPECT_EQ(h.lastAlterErr, 30);                       // SchemaConflict
    EXPECT_EQ(h.lastAlterVersion, 2);                    // response carries current version
    EXPECT_EQ(h.reg.byName("characters")->version, 2);   // no lost update
}

// Regression (atomicity): a multi-op ALTER where a later op fails rejects the
// WHOLE statement — nothing is committed and the version is not bumped.
TEST(AdminWorkerRegression, MultiOpAlterIsAtomicOnFailure) {
    Harness h;
    h.runDefine(1,2,"characters","userId",16,false,charsBlob());
    ASSERT_EQ(h.lastErr,0);
    const int32_t tid = h.reg.byName("characters")->topicId;

    // ADD (valid) then DROP a field that does not exist → applyOps fails on the
    // DROP, so the ADD is rolled back too (atomic).
    h.runAlter(10,11,"characters",0,
               opsBlob({addOp("score",PrimType::Int64), dropOp("ghost")}));
    EXPECT_EQ(h.lastAlterErr, 23);                       // InvalidSchema
    EXPECT_EQ(h.reg.byName("characters")->version, 1);   // version unchanged
    EXPECT_EQ(h.reg.nextTagFor(tid), 3);                 // no tag burned
}
