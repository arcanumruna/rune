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
};

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
