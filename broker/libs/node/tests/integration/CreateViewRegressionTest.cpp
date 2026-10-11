// ---------------------------------------------------------------------------
// CreateViewRegressionTest.cpp — Task 19 (view engine phase 1): the CreateView
// control plane (create-and-run path).
//
// Exercises the AdminWorker DDL state machine for CREATE VIEW end-to-end WITHOUT
// Aeron / the media driver (single-node replicas, minIsr=1, NullTransport) —
// the same deterministic in-process harness style as SchemaPersistenceTest:
//
//   DefineTopic a keyed "character" topic (id key, hp) → CreateView "char_hp"
//   over it (output id,hp; Replace) → drive the async state machine to commit.
//
// Asserts:
//   * respondCreateView OK + a real viewTopicId + non-zero output fingerprint;
//   * the durable ViewRegistry has char_hp byName (persisted to __views);
//   * the compiled plan the admin thread fanned out installs on a real
//     WorkerViewSet through the REAL control->worker install ring +
//     WorkerViewSet::drainInstalls (the view is interested in the source topic);
//   * a second CreateView for the same name returns ViewExists.
//
// NOT tested here (Docker-gated / later tasks): majority-durable commit over
// real Aeron followers, the full gateway client round-trip, restart recovery
// (installViewForRecovery / driveRebuild — Task 19b).
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/AdminWorker.h>
#include <rune/node/AdminChannels.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/ViewRegistry.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/Log.h>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <rune/view/ViewPlan.h>
#include <rune/view/ViewTypes.h>

#include <rune/protocol/client/Protocol.h>

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
namespace rv = rune::view;
namespace pc = rune::protocol::client;

namespace {

std::string uniqSuffix() {
    static std::atomic<uint64_t> seq{0};
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(seq.fetch_add(1, std::memory_order_relaxed)) + "_" + std::to_string(ts);
}

struct TmpDir {
    std::string path;
    explicit TmpDir(const std::string& tag) {
        path = std::filesystem::temp_directory_path().string() + "/rune_cvrt_" + tag + "_" + uniqSuffix();
        std::filesystem::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; std::filesystem::remove_all(path, ec); }
};

PartitionConfig singleNodeCfg(int32_t topicId) {
    PartitionConfig cfg;
    cfg.topicId             = topicId;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = 1;   // single-node: appendKeyed commits without followers
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    cfg.cleanupPolicy       = CleanupPolicy::Compact;
    return cfg;
}

// "character" source schema: id (Int64 key) + hp (Int64).
std::vector<uint8_t> characterBlob() {
    return serialize(DescriptorBuilder("character", 1)
        .add("id", PrimType::Int64, 1, /*isKey=*/true)
        .add("hp", PrimType::Int64)
        .build());
}

// Authored char_hp view over `srcTopicId`: key id; output id + hp (Replace).
std::vector<uint8_t> charHpPlanBlob(int32_t srcTopicId) {
    rv::ViewPlan p;
    p.viewName = "char_hp";
    p.key      = "id";
    p.persistence = 0;  // persisted
    p.sources.push_back(rv::SourceSpec{srcTopicId, "id"});
    p.output.push_back(rv::AuthoredField{"id", PrimType::Int64, 0, "id", rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"hp", PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
    return rv::serializeAuthored(p);
}

} // namespace

// ---------------------------------------------------------------------------
// Fixture — admin worker driving BOTH __topic_schemas and __views, plus a real
// WorkerViewSet fed by the real control->worker install ring.
// ---------------------------------------------------------------------------
class CreateViewRegression : public ::testing::Test {
protected:
    CreateViewRegression()
        : schemaDir_("schema"), viewsDir_("views")
        , schemaLog_(schemaDir_.path), viewsLog_(viewsDir_.path)
        , viewSet_(installChannel_, viewsDir_.path) {}

    void SetUp() override {
        schemaLog_.recover();
        viewsLog_.recover();
        schemaTransport_ = std::make_unique<NullPartitionTransport>();
        viewsTransport_  = std::make_unique<NullPartitionTransport>();

        schemaReplica_ = std::make_unique<PartitionReplica>(
            singleNodeCfg(kSchemaTopicId), Role::Leader, /*epoch=*/1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1}, &schemaLog_, schemaTransport_.get());
        viewsReplica_ = std::make_unique<PartitionReplica>(
            singleNodeCfg(kViewsTopicId), Role::Leader, /*epoch=*/1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1}, &viewsLog_, viewsTransport_.get());

        schemaRegistry_ = std::make_unique<SchemaRegistry>();
        viewRegistry_   = std::make_unique<ViewRegistry>();

        AdminDeps deps;
        deps.requestCreateTopic = [this](int64_t ticket, int32_t, const std::string&,
                                         const std::string& keyField, uint8_t cleanupPolicy) -> bool {
            pendingTicket_ = ticket;
            lastSinkKeyField_      = keyField;       // capture: a view sink is keyed...
            lastSinkCleanupPolicy_ = cleanupPolicy;  // ...and Compact.
            return true;
        };
        deps.pushValidators = [](const RegistryEntry&, int32_t) {};
        deps.respondDefine  = [this](int64_t, int64_t, int32_t topicId, uint16_t, uint16_t err) {
            lastDefineTopicId_ = topicId; lastDefineError_ = err;
        };
        deps.respondGet   = [](int64_t, int64_t, int32_t, uint16_t, const std::vector<uint8_t>&, uint16_t) {};
        deps.respondAlter = [](int64_t, int64_t, int32_t, uint16_t, uint16_t) {};
        deps.respondCreateView = [this](int64_t, int64_t, int32_t viewTopicId,
                                        uint64_t fp, uint16_t err) {
            lastViewTopicId_  = viewTopicId;
            lastViewFinger_   = fp;
            lastViewError_    = err;
        };
        // Fan the compiled plan out exactly as the control thread does: encode it
        // onto the worker's REAL install ring (the worker drains it on its own
        // thread via WorkerViewSet::drainInstalls).
        deps.pushViewPlan = [this](const ViewPlanUpdateMsg& vp) {
            ASSERT_TRUE(encodeViewPlanUpdate(installChannel_.install,
                                             ViewSnapshotChannel::kInstallMsgType, vp));
        };

        adminWorker_ = std::make_unique<AdminWorker>(
            *schemaRegistry_, schemaReplica_.get(), /*selfId=*/1, std::vector<int32_t>{1},
            std::move(deps), viewsReplica_.get(), viewRegistry_.get());
        adminWorker_->onRole(AdminRoleMsg{/*leader=*/true, /*epoch=*/1, /*leaderId=*/1});
    }

    void driveDefine(const DefineTopicMsg& msg) {
        adminWorker_->submitDefineTopic(msg);
        pump_();
    }
    void driveCreateView(const CreateViewMsg& msg) {
        adminWorker_->submitCreateView(msg);
        pump_();
    }
    // Drive the async state machine: tick doWork, answering the simulated
    // control-thread createTopic reply with a fresh sequential id.
    void pump_() {
        pendingTicket_ = -1;
        int64_t nowNs = 0;
        for (int i = 0; i < 100; ++i) {
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

    DefineTopicMsg defineCharacter() {
        DefineTopicMsg d;
        d.session = 1; d.corr = 10; d.partitionCount = 1; d.name = "character";
        d.keyField = "id"; d.validate = false; d.blob = characterBlob();
        return d;
    }

    TmpDir schemaDir_, viewsDir_;
    rune::storage::Log schemaLog_, viewsLog_;
    ViewSnapshotChannel installChannel_;        // the worker's own channel (install ring used here)
    WorkerViewSet       viewSet_;               // a real per-worker view set
    std::unique_ptr<NullPartitionTransport> schemaTransport_, viewsTransport_;
    std::unique_ptr<PartitionReplica>       schemaReplica_, viewsReplica_;
    std::unique_ptr<SchemaRegistry>         schemaRegistry_;
    std::unique_ptr<ViewRegistry>           viewRegistry_;
    std::unique_ptr<AdminWorker>            adminWorker_;

    int32_t nextTopicId_   = 0;
    int64_t pendingTicket_ = -1;

    int32_t  lastDefineTopicId_ = -1;
    uint16_t lastDefineError_   = 0xffff;
    int32_t  lastViewTopicId_   = -2;
    uint64_t lastViewFinger_    = 0;
    uint16_t lastViewError_     = 0xffff;
    std::string lastSinkKeyField_;
    uint8_t     lastSinkCleanupPolicy_ = 0xff;
};

// ---------------------------------------------------------------------------
// The create-and-run path: compile + persist + respond + fan-out install.
// ---------------------------------------------------------------------------
TEST_F(CreateViewRegression, CreateViewPersistsCompilesAndInstalls) {
    // 1. Source topic.
    driveDefine(defineCharacter());
    ASSERT_EQ(lastDefineError_, 0u) << "precondition: DefineTopic(character) must succeed";
    const int32_t srcTopicId = lastDefineTopicId_;
    ASSERT_GE(srcTopicId, 0);

    // 2. CreateView char_hp over it.
    CreateViewMsg cv;
    cv.session = 1; cv.corr = 20; cv.persistence = 0;
    cv.viewName = "char_hp"; cv.keyField = "id"; cv.planBlob = charHpPlanBlob(srcTopicId);
    driveCreateView(cv);

    // 3. OK + a real view topic id + non-zero output fingerprint.
    EXPECT_EQ(lastViewError_, static_cast<uint16_t>(pc::ErrorCode::OK));
    EXPECT_GE(lastViewTopicId_, 0) << "CreateView must return the view's sink topic id";
    EXPECT_NE(lastViewFinger_, 0u) << "output fingerprint must be published";

    // 4. The view sink was requested keyed + Compact (table semantics).
    EXPECT_EQ(lastSinkKeyField_, "id");
    EXPECT_EQ(lastSinkCleanupPolicy_, 1u) << "view sink must be CleanupPolicy::Compact";

    // 5. Durable catalog: the view is in the ViewRegistry by name.
    const ViewRecord* rec = viewRegistry_->byName("char_hp");
    ASSERT_NE(rec, nullptr) << "char_hp must be persisted to __views";
    EXPECT_EQ(rec->viewTopicId, lastViewTopicId_);
    EXPECT_FALSE(rec->compiledPlanBlob.empty());

    // 6. The owning worker's WorkerViewSet installed the compiled plan (via the
    //    REAL control->worker install ring + drainInstalls).
    const int installed = viewSet_.drainInstalls();
    EXPECT_EQ(installed, 1) << "exactly one view plan should install";
    EXPECT_EQ(viewSet_.viewCount(), 1u);
    EXPECT_TRUE(viewSet_.interestedIn(srcTopicId))
        << "the installed view must subscribe to its source topic";

    // 7. A second CreateView for the same name → ViewExists.
    lastViewError_ = 0xffff;
    CreateViewMsg cv2 = cv;
    cv2.corr = 21;
    driveCreateView(cv2);
    EXPECT_EQ(lastViewError_, static_cast<uint16_t>(pc::ErrorCode::ViewExists));
}

namespace {
std::vector<uint8_t> orderBlob() {
    return serialize(DescriptorBuilder("order", 1)
        .add("orderId", PrimType::Int64, 1, /*isKey=*/true)
        .add("userId", PrimType::Int64)
        .add("amount", PrimType::Int64)
        .build());
}
std::vector<uint8_t> totalsPlanBlob(int32_t srcTopicId, const std::string& groupField) {
    rv::ViewPlan p;
    p.viewName = "totals_" + groupField; p.key = groupField; p.persistence = 0;
    p.sources.push_back(rv::SourceSpec{srcTopicId, groupField});
    p.output.push_back(rv::AuthoredField{groupField, PrimType::Int64, 0, groupField, rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"total", PrimType::Int64, 0, "amount", rv::MergeRule::Sum});
    return rv::serializeAuthored(p);
}
} // namespace

TEST_F(CreateViewRegression, AggregateOverMultiPartitionSourceIsNotCoPartitioned) {
    DefineTopicMsg d;
    d.session = 1; d.corr = 30; d.partitionCount = 2; d.name = "order";
    d.keyField = "orderId"; d.validate = false; d.blob = orderBlob();
    driveDefine(d);
    ASSERT_EQ(lastDefineError_, 0u);
    const int32_t src = lastDefineTopicId_;

    CreateViewMsg cv;
    cv.session = 1; cv.corr = 31; cv.persistence = 0;
    cv.viewName = "totals_userId"; cv.keyField = "userId"; cv.planBlob = totalsPlanBlob(src, "userId");
    driveCreateView(cv);
    EXPECT_EQ(lastViewError_, 36u) << "group-by userId over a 2-partition source keyed by orderId";

    // Grouping by the source's declared key field is ALSO rejected: a contributor
    // is its 8-byte wire key, the client partitions by wire key, and the broker
    // never enforces wire key == declared keyField — so "group-by == keyField"
    // does not guarantee every contributor of a group lands on one partition.
    lastViewError_ = 0xffff;
    cv.corr = 32; cv.viewName = "totals_orderId"; cv.keyField = "orderId";
    cv.planBlob = totalsPlanBlob(src, "orderId");
    driveCreateView(cv);
    EXPECT_EQ(lastViewError_, 36u)
        << "group-by == source keyField over a 2-partition source must be rejected too";
}

TEST_F(CreateViewRegression, ReplaceOnlyViewOverMultiPartitionSourceIsAllowed) {
    DefineTopicMsg d;
    d.session = 1; d.corr = 50; d.partitionCount = 2; d.name = "order2";
    d.keyField = "orderId"; d.validate = false; d.blob = orderBlob();
    driveDefine(d);
    ASSERT_EQ(lastDefineError_, 0u);

    rv::ViewPlan p;
    p.viewName = "order_amount"; p.key = "orderId"; p.persistence = 0;
    p.sources.push_back(rv::SourceSpec{lastDefineTopicId_, "orderId"});
    p.output.push_back(rv::AuthoredField{"orderId", PrimType::Int64, 0, "orderId", rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"amount", PrimType::Int64, 0, "amount", rv::MergeRule::Replace});

    CreateViewMsg cv;
    cv.session = 1; cv.corr = 51; cv.persistence = 0;
    cv.viewName = "order_amount"; cv.keyField = "orderId"; cv.planBlob = rv::serializeAuthored(p);
    driveCreateView(cv);
    EXPECT_EQ(lastViewError_, static_cast<uint16_t>(pc::ErrorCode::OK))
        << "the co-partition gate applies to aggregate views only";
}

TEST_F(CreateViewRegression, AggregateOverSinglePartitionSourceIsAllowed) {
    DefineTopicMsg d;
    d.session = 1; d.corr = 40; d.partitionCount = 1; d.name = "order1";
    d.keyField = "orderId"; d.validate = false; d.blob = orderBlob();
    driveDefine(d);
    ASSERT_EQ(lastDefineError_, 0u);
    CreateViewMsg cv;
    cv.session = 1; cv.corr = 41; cv.persistence = 0;
    cv.viewName = "totals_userId"; cv.keyField = "userId";
    cv.planBlob = totalsPlanBlob(lastDefineTopicId_, "userId");
    driveCreateView(cv);
    EXPECT_EQ(lastViewError_, static_cast<uint16_t>(pc::ErrorCode::OK));
}
