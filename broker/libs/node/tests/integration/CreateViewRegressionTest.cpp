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

#include <unistd.h>
#include <gtest/gtest.h>

#include <rune/node/AdminWorker.h>
#include <rune/node/AdminChannels.h>
#include <rune/node/AdminCreateTopic.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/ViewRegistry.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/Log.h>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/AlterOps.h>

#include <rune/view/ViewPlan.h>
#include <rune/view/ViewTypes.h>

#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/view/ViewRow.h>
#include <rune/storage/LogReadView.h>

#include <algorithm>
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
    // The pid keeps ctest -j parallel PROCESSES apart (each starts seq at 0 and the
    // clock can tick identically in two of them; a shared dir was removed under a peer).
    return std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1, std::memory_order_relaxed)) + "_" + std::to_string(ts);
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

        buildAdmin_();
    }

    // Build the admin side (registries + AdminWorker) over the CURRENT replicas
    // and promote it — on a restart (restartAdmin_) the promotion edge replays
    // __topic_schemas + __views exactly as a broker boot does.
    void buildAdmin_() {
        schemaRegistry_ = std::make_unique<SchemaRegistry>();
        viewRegistry_   = std::make_unique<ViewRegistry>();

        AdminDeps deps;
        deps.requestCreateTopic = [this](int64_t ticket, int32_t parts, const std::string& name,
                                         const std::string& keyField, uint8_t cleanupPolicy,
                                         uint8_t /*persistence*/) -> bool {
            pendingTicket_ = ticket;
            ++createRequests_;
            maxOutstanding_ = std::max(maxOutstanding_, ++outstandingCreates_);
            pendingReq_ = CreateTopicReqMsg{ticket, parts, name, keyField, cleanupPolicy};
            lastSinkKeyField_      = keyField;       // capture: a view sink is keyed...
            lastSinkCleanupPolicy_ = cleanupPolicy;  // ...and Compact.
            return true;
        };
        deps.pushValidators = [](const RegistryEntry&, int32_t) {};
        deps.topicCommitted = [this](int32_t topicId) { committedIds_.push_back(topicId); };
        deps.respondDefine  = [this](int64_t, int64_t, int32_t topicId, uint16_t, uint16_t err) {
            lastDefineTopicId_ = topicId; lastDefineError_ = err;
            defineResults_.push_back(err);
        };
        deps.respondGet   = [](int64_t, int64_t, int32_t, uint16_t, int32_t, const std::vector<uint8_t>&, uint16_t) {};
        deps.respondAlter = [this](int64_t, int64_t, int32_t topicId, uint16_t version, uint16_t err) {
            alterResults_.push_back(AlterResult{topicId, version, err});
        };
        deps.respondCreateView = [this](int64_t, int64_t, int32_t viewTopicId,
                                        uint64_t fp, uint16_t err) {
            lastViewTopicId_  = viewTopicId;
            lastViewFinger_   = fp;
            lastViewError_    = err;
            viewResults_.push_back(err);
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

    // Simulated broker restart: fresh replicas (Follower, as at boot) over the
    // SAME durable logs, fresh registries, fresh AdminWorker, promoted to leader
    // (rebuildFromLog_ + recoverViewsOnPromotion_ run on that edge).
    void restartAdmin_() {
        adminWorker_.reset();
        schemaReplica_ = std::make_unique<PartitionReplica>(
            singleNodeCfg(kSchemaTopicId), Role::Follower, /*epoch=*/0,
            std::vector<NodeId>{1}, std::vector<NodeId>{1}, &schemaLog_, schemaTransport_.get());
        viewsReplica_ = std::make_unique<PartitionReplica>(
            singleNodeCfg(kViewsTopicId), Role::Follower, /*epoch=*/0,
            std::vector<NodeId>{1}, std::vector<NodeId>{1}, &viewsLog_, viewsTransport_.get());
        buildAdmin_();
    }

    // Final review I1: answer create-topic requests like BrokerNode's control
    // thread (createTopicForAdmin over a REAL TopicRegistry — idempotent by
    // name, authoritative count, `existed`) instead of a fresh fake id.
    void useRealTopicRegistry_(const std::vector<rune::node::TopicSpec>& bootTopics) {
        topicReg_ = std::make_unique<rune::gateway::TopicRegistry>(bootTopics, topicsDir_.path);
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
                if (topicReg_) {
                    reply = createTopicForAdmin(*topicReg_, pendingReq_);
                } else {
                    reply.ticket         = pendingTicket_;
                    reply.topicId        = nextTopicId_++;
                    reply.partitionCount = pendingReq_.partitionCount;
                }
                pendingTicket_ = -1;
                --outstandingCreates_;
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
    TmpDir topicsDir_{"topics"};
    std::unique_ptr<rune::gateway::TopicRegistry> topicReg_;   // null = fake control ids
    CreateTopicReqMsg pendingReq_;
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

    // U2: per-op response capture + create-request accounting.
    struct AlterResult { int32_t topicId; uint16_t version; uint16_t err; };
    std::vector<uint16_t>    defineResults_, viewResults_;
    std::vector<AlterResult> alterResults_;
    int createRequests_    = 0;   // requestCreateTopic calls
    int outstandingCreates_ = 0;  // requested, reply not yet delivered
    int maxOutstanding_     = 0;  // must never exceed 1 (single in-flight DDL op)

    int32_t  lastDefineTopicId_ = -1;
    uint16_t lastDefineError_   = 0xffff;
    int32_t  lastViewTopicId_   = -2;
    std::vector<int32_t> committedIds_;   // B10 fix round 1: deps.topicCommitted
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

    // 8. B10 fix round 1 (I1): the source's DefineTopic and the view sink's record
    //    each signalled "committed" exactly once; the refused duplicate did not.
    EXPECT_EQ(committedIds_, (std::vector<int32_t>{srcTopicId, lastViewTopicId_}));
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

// ---------------------------------------------------------------------------
// Phase 2b C final review — a view named like an existing topic.
//
// startCreateView_ only checked the VIEW registry, and the control thread's
// createTopic is idempotent by name: `CREATE VIEW character ... FROM character`
// got the SOURCE topic's id back as its "sink" — the view folded its own output
// into its source (feedback loop) and re-registered the source's schema.  Both
// a typed existing topic (schema registry hit, checked up front) and an untyped
// one (only the control thread knows it — the reply's `existed` flag) must be
// rejected with TopicExists, leaving the topic and its schema untouched.
// ---------------------------------------------------------------------------
TEST_F(CreateViewRegression, ViewNamedLikeExistingTypedTopicIsRejectedRegression) {
    useRealTopicRegistry_({});
    driveDefine(defineCharacter());
    ASSERT_EQ(lastDefineError_, 0u);
    const int32_t srcTopicId = lastDefineTopicId_;
    const RegistryEntry* before = schemaRegistry_->byName("character");
    ASSERT_NE(before, nullptr);
    const uint64_t srcFp = before->fingerprint;

    CreateViewMsg cv;
    cv.session = 1; cv.corr = 60; cv.persistence = 0;
    cv.viewName = "character"; cv.keyField = "id"; cv.planBlob = charHpPlanBlob(srcTopicId);
    driveCreateView(cv);

    EXPECT_EQ(lastViewError_, static_cast<uint16_t>(pc::ErrorCode::TopicExists));
    EXPECT_EQ(viewRegistry_->byName("character"), nullptr) << "no view may be persisted";
    EXPECT_EQ(viewSet_.drainInstalls(), 0) << "no view plan may be installed";
    const RegistryEntry* after = schemaRegistry_->byName("character");
    ASSERT_NE(after, nullptr);
    EXPECT_EQ(after->topicId, srcTopicId);
    EXPECT_EQ(after->fingerprint, srcFp) << "the source topic's schema is untouched";
}

TEST_F(CreateViewRegression, ViewNamedLikeExistingUntypedTopicIsRejectedRegression) {
    rune::node::TopicSpec boot; boot.name = "raw_events"; boot.partitionCount = 1; boot.topicId = 0;
    useRealTopicRegistry_({boot});   // a RUNE_TOPICS boot topic: no schema
    driveDefine(defineCharacter());
    ASSERT_EQ(lastDefineError_, 0u);
    const int32_t srcTopicId = lastDefineTopicId_;

    CreateViewMsg cv;
    cv.session = 1; cv.corr = 61; cv.persistence = 0;
    cv.viewName = "raw_events"; cv.keyField = "id"; cv.planBlob = charHpPlanBlob(srcTopicId);
    driveCreateView(cv);

    EXPECT_EQ(lastViewError_, static_cast<uint16_t>(pc::ErrorCode::TopicExists));
    EXPECT_EQ(viewRegistry_->byName("raw_events"), nullptr) << "no view may be persisted";
    EXPECT_EQ(viewSet_.drainInstalls(), 0) << "no view plan may be installed";
    EXPECT_EQ(schemaRegistry_->byName("raw_events"), nullptr)
        << "the existing topic must not acquire the view's output schema";
    EXPECT_EQ(topicReg_->topic(0)->cleanupPolicy, rune::node::TopicSpec::CleanupPolicy::Delete)
        << "the existing topic is not turned into a view sink";
}

// ---------------------------------------------------------------------------
// The view's typed output topic resolves by the VIEW name — before and after a
// restart — not by its output descriptor's name (the authored plan name).  The
// view TopicSchema used to carry an empty topicName, so it was indexed under
// the descriptor name (here "char_hp", which names nothing the client created)
// and GetTopicSchema("hp_by_char") answered UnknownTopic.  The restart leg also
// covers final review I2 through the AdminWorker rebuildFromLog_ path: the
// source topic "chars_src" (descriptor "character") resolves by its own name.
// ---------------------------------------------------------------------------
TEST_F(CreateViewRegression, ViewSinkResolvesByViewNameAcrossRestartRegression) {
    DefineTopicMsg d = defineCharacter();
    d.name = "chars_src";                     // descriptor is named "character"
    driveDefine(d);
    ASSERT_EQ(lastDefineError_, 0u);
    const int32_t srcTopicId = lastDefineTopicId_;

    CreateViewMsg cv;
    cv.session = 1; cv.corr = 62; cv.persistence = 0;
    cv.viewName = "hp_by_char";               // plan (output descriptor) is named "char_hp"
    cv.keyField = "id"; cv.planBlob = charHpPlanBlob(srcTopicId);
    driveCreateView(cv);
    ASSERT_EQ(lastViewError_, static_cast<uint16_t>(pc::ErrorCode::OK));
    const int32_t viewTopicId = lastViewTopicId_;

    auto check = [&](const char* when) {
        const RegistryEntry* v = schemaRegistry_->byName("hp_by_char");
        ASSERT_NE(v, nullptr) << when << ": the view resolves by its view name";
        EXPECT_EQ(v->topicId, viewTopicId) << when;
        EXPECT_EQ(v->topicName, "hp_by_char") << when;
        EXPECT_EQ(schemaRegistry_->byName("char_hp"), nullptr)
            << when << ": the output descriptor name is not a topic name";
        const RegistryEntry* s = schemaRegistry_->byName("chars_src");
        ASSERT_NE(s, nullptr) << when << ": the source resolves by its defined name";
        EXPECT_EQ(s->topicId, srcTopicId) << when;
        EXPECT_EQ(schemaRegistry_->byName("character"), nullptr)
            << when << ": the source descriptor name is not a topic name";
    };
    check("before restart");

    restartAdmin_();
    ASSERT_FALSE(adminWorker_->rebuildFailed());
    ASSERT_NE(viewRegistry_->byName("hp_by_char"), nullptr) << "the view catalog recovered";
    check("after restart");
}

// ---------------------------------------------------------------------------
// Phase 2b E — a view over a TRANSIENT source must itself be transient.
// A persisted view would checkpoint rows whose source data is gone after a
// restart.  Rejected at CREATE VIEW time with ViewSourceTransient (37), before
// any sink topic or __views record exists.
// ---------------------------------------------------------------------------
namespace {
std::vector<uint8_t> charHpPlanBlobP(int32_t srcTopicId, uint8_t persistence) {
    rv::ViewPlan p;
    p.viewName = "char_hp"; p.key = "id"; p.persistence = persistence;
    p.sources.push_back(rv::SourceSpec{srcTopicId, "id"});
    p.output.push_back(rv::AuthoredField{"id", PrimType::Int64, 0, "id", rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"hp", PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
    return rv::serializeAuthored(p);
}
} // namespace

TEST_F(CreateViewRegression, PersistedViewOverTransientSourceIsRejectedRegression) {
    DefineTopicMsg d = defineCharacter();
    d.persistence = 1;   // transient
    driveDefine(d);
    ASSERT_EQ(lastDefineError_, 0u);
    const int32_t src = lastDefineTopicId_;
    ASSERT_NE(schemaRegistry_->byId(src), nullptr);
    ASSERT_EQ(schemaRegistry_->byId(src)->persistence, PersistenceMode::Transient);

    CreateViewMsg cv;
    cv.session = 1; cv.corr = 70; cv.persistence = 0;   // persisted
    cv.viewName = "char_hp"; cv.keyField = "id"; cv.planBlob = charHpPlanBlobP(src, 0);
    pendingTicket_ = -1;
    driveCreateView(cv);

    EXPECT_EQ(lastViewError_, 37u);
    EXPECT_EQ(lastViewTopicId_, -1);
    EXPECT_EQ(viewRegistry_->byName("char_hp"), nullptr) << "no __views record";
    EXPECT_EQ(schemaRegistry_->byName("char_hp"), nullptr) << "no sink topic schema";
    EXPECT_TRUE(lastSinkKeyField_.empty()) << "no sink topic was requested";
    EXPECT_EQ(viewSet_.drainInstalls(), 0);
}

// Final review M5: an unknown persistence byte on the CreateView ring frame is
// never cast; the decode marks the message malformed and the worker answers
// InvalidSchema (23) with the frame's session/corr.
TEST_F(CreateViewRegression, RingDecodeRejectsUnknownPersistenceByteRegression) {
    std::vector<uint8_t> raw(8 + 8 + 1 + 2 + 2 + 4, 0);
    raw[0] = 1;            // session = 1
    raw[8] = 71;           // corr = 71
    raw[16] = 2;           // persistence: one past Transient
    const CreateViewMsg cv = decodeCreateView(raw.data(), raw.size());
    EXPECT_TRUE(cv.malformed);
    EXPECT_EQ(cv.session, 1);
    EXPECT_EQ(cv.corr, 71);

    pendingTicket_ = -1;
    driveCreateView(cv);
    EXPECT_EQ(lastViewError_, 23u);
    EXPECT_EQ(lastViewTopicId_, -1);

    raw[16] = 1;           // a known mode is not flagged
    EXPECT_FALSE(decodeCreateView(raw.data(), raw.size()).malformed);
}

// No-hang wave (R1): a DIRECTLY submitted CreateViewMsg (not via the ring
// decode, so `malformed` is false) with an unknown persistence byte is
// rejected InvalidSchema — it used to pass as "not 0" (transient-like) and be
// recorded as 2.
TEST_F(CreateViewRegression, DirectCreateViewWithUnknownPersistenceIsInvalidSchemaRegression) {
    CreateViewMsg cv;
    cv.session = 1; cv.corr = 72; cv.persistence = 2; cv.viewName = "v2"; cv.keyField = "id";
    ASSERT_FALSE(cv.malformed);
    pendingTicket_ = -1;
    driveCreateView(cv);
    EXPECT_EQ(lastViewError_, 23u);
    EXPECT_EQ(lastViewTopicId_, -1);
    EXPECT_EQ(pendingTicket_, -1) << "no sink create requested";
}

// ===========================================================================
// Final review U1/U2 — DDL serialization + name collisions + view not alterable.
// Every test enqueues the ops back to back BEFORE any reply is delivered.
// ===========================================================================
namespace {
DefineTopicMsg defineNamed(int64_t corr, const std::string& name, uint8_t persistence = 0) {
    DefineTopicMsg d;
    d.session = 1; d.corr = corr; d.partitionCount = 1; d.name = name;
    d.keyField = "id"; d.validate = false; d.blob = characterBlob(); d.persistence = persistence;
    return d;
}
CreateViewMsg viewNamed(int64_t corr, const std::string& name, int32_t srcTopicId) {
    CreateViewMsg cv;
    cv.session = 1; cv.corr = corr; cv.persistence = 0;
    cv.viewName = name; cv.keyField = "id"; cv.planBlob = charHpPlanBlob(srcTopicId);
    return cv;
}
std::vector<uint8_t> addColumnOps() {
    rune::codec::AlterOp op{}; op.type = rune::codec::AlterOpType::Add;
    op.name = "extra"; op.primType = PrimType::Int64; op.fixedLen = 0;
    return rune::codec::encodeAlterOps({op});
}
} // namespace

TEST_F(CreateViewRegression, U1_AlterTopicOnAViewIsViewNotAlterableRegression) {
    driveDefine(defineNamed(1, "character"));
    ASSERT_EQ(lastDefineError_, 0u);
    driveCreateView(viewNamed(2, "char_hp", lastDefineTopicId_));
    ASSERT_EQ(lastViewError_, 0u);
    const int64_t schemaEnd = schemaLog_.logEndOffset();
    const uint16_t versionBefore = schemaRegistry_->byName("char_hp")->version;

    AlterTopicMsg a; a.session = 1; a.corr = 3; a.topicName = "char_hp";
    a.expectedVersion = 0; a.opsBlob = addColumnOps();
    adminWorker_->submitAlterTopic(a);
    pump_();

    ASSERT_EQ(alterResults_.size(), 1u);
    EXPECT_EQ(alterResults_[0].err, 38u) << "ViewNotAlterable";
    EXPECT_EQ(schemaLog_.logEndOffset(), schemaEnd) << "nothing appended";
    EXPECT_EQ(schemaRegistry_->byName("char_hp")->version, versionBefore);
}

TEST_F(CreateViewRegression, U2_SameNameTopicsBackToBackExactlyOneWinsRegression) {
    for (const uint8_t secondPersistence : {uint8_t{0}, uint8_t{1}}) {
        SCOPED_TRACE(secondPersistence);
        const std::string name = secondPersistence ? "dup_t" : "dup_d";
        defineResults_.clear(); createRequests_ = 0;
        const int64_t before = schemaLog_.logEndOffset();
        adminWorker_->submitDefineTopic(defineNamed(1, name, 0));
        adminWorker_->submitDefineTopic(defineNamed(2, name, secondPersistence));
        pump_();
        ASSERT_EQ(defineResults_.size(), 2u);
        EXPECT_EQ(defineResults_[0], 0u) << "FIFO: the first define wins";
        EXPECT_EQ(defineResults_[1], 25u) << "TopicExists";
        EXPECT_EQ(createRequests_, 1) << "the loser never reaches the control thread";
        EXPECT_EQ(schemaRegistry_->byName(name)->version, 1u);
        EXPECT_EQ(schemaLog_.logEndOffset() - before, 1) << "exactly one TopicSchema record";
        EXPECT_EQ(adminWorker_->pendingCount(), 0u);
    }
    EXPECT_EQ(maxOutstanding_, 1);
}

TEST_F(CreateViewRegression, U2_ViewThenTopicAndTopicThenViewSameNameRegression) {
    driveDefine(defineNamed(1, "character"));
    ASSERT_EQ(lastDefineError_, 0u);
    const int32_t src = lastDefineTopicId_;

    // view X then topic X
    defineResults_.clear(); viewResults_.clear(); createRequests_ = 0;
    adminWorker_->submitCreateView(viewNamed(10, "X", src));
    adminWorker_->submitDefineTopic(defineNamed(11, "X"));
    pump_();
    ASSERT_EQ(viewResults_.size(), 1u);
    ASSERT_EQ(defineResults_.size(), 1u);
    EXPECT_EQ(viewResults_[0], 0u);
    EXPECT_EQ(defineResults_[0], 25u) << "topic over a view's name: TopicExists";
    EXPECT_EQ(createRequests_, 1) << "nothing created for the loser";

    // topic Y then view Y
    defineResults_.clear(); viewResults_.clear(); createRequests_ = 0;
    adminWorker_->submitDefineTopic(defineNamed(20, "Y"));
    adminWorker_->submitCreateView(viewNamed(21, "Y", src));
    pump_();
    ASSERT_EQ(defineResults_.size(), 1u);
    ASSERT_EQ(viewResults_.size(), 1u);
    EXPECT_EQ(defineResults_[0], 0u);
    EXPECT_EQ(viewResults_[0], 25u) << "view over a topic's name: TopicExists";
    EXPECT_EQ(createRequests_, 1);
    EXPECT_EQ(viewRegistry_->byName("Y"), nullptr);
    EXPECT_EQ(maxOutstanding_, 1);
}

TEST_F(CreateViewRegression, U2_SameNameViewsBackToBackOneViewExistsRegression) {
    driveDefine(defineNamed(1, "character"));
    ASSERT_EQ(lastDefineError_, 0u);
    viewResults_.clear(); createRequests_ = 0;
    adminWorker_->submitCreateView(viewNamed(10, "X", lastDefineTopicId_));
    adminWorker_->submitCreateView(viewNamed(11, "X", lastDefineTopicId_));
    pump_();
    ASSERT_EQ(viewResults_.size(), 2u);
    EXPECT_EQ(viewResults_[0], 0u);
    EXPECT_EQ(viewResults_[1], 32u) << "ViewExists";
    EXPECT_EQ(createRequests_, 1);
    EXPECT_EQ(viewRegistry_->all().size(), 1u);
    EXPECT_EQ(maxOutstanding_, 1);
}

TEST_F(CreateViewRegression, U2_AlterQueuedBehindCreateRunsAfterCommitRegression) {
    adminWorker_->submitDefineTopic(defineNamed(1, "t"));
    AlterTopicMsg a; a.session = 1; a.corr = 2; a.topicName = "t";
    a.expectedVersion = 0; a.opsBlob = addColumnOps();
    adminWorker_->submitAlterTopic(a);

    // Tick until the create's id request is out; the control reply is withheld.
    int64_t now = 0;
    while (pendingTicket_ < 0 && now < 50'000'000LL) { now += 1'000'000LL; adminWorker_->doWork(now); }
    ASSERT_GE(pendingTicket_, 0);
    for (int i = 0; i < 5; ++i) { now += 1'000'000LL; adminWorker_->doWork(now); }
    EXPECT_TRUE(alterResults_.empty()) << "ALTER must not start while the CREATE is in flight";
    EXPECT_TRUE(defineResults_.empty());
    EXPECT_EQ(adminWorker_->pendingCount(), 1u) << "the ALTER waits in the FIFO";
    EXPECT_EQ(schemaRegistry_->byName("t"), nullptr);

    // Deliver the reply; CREATE commits, THEN the ALTER runs and succeeds at v2.
    CreateTopicReplyMsg reply; reply.ticket = pendingTicket_; reply.topicId = nextTopicId_++;
    pendingTicket_ = -1; --outstandingCreates_;
    adminWorker_->onCreateTopicReply(reply);
    for (int i = 0; i < 100; ++i) { now += 1'000'000LL; adminWorker_->doWork(now); }

    ASSERT_EQ(defineResults_.size(), 1u);
    EXPECT_EQ(defineResults_[0], 0u);
    ASSERT_EQ(alterResults_.size(), 1u);
    EXPECT_EQ(alterResults_[0].err, 0u);
    EXPECT_EQ(alterResults_[0].version, 2u);
    EXPECT_EQ(schemaRegistry_->byName("t")->version, 2u);
    EXPECT_EQ(adminWorker_->pendingCount(), 0u);
    EXPECT_EQ(maxOutstanding_, 1);
}

TEST_F(CreateViewRegression, TransientViewOverTransientSourceIsAcceptedAndFoldsRegression) {
    DefineTopicMsg d = defineCharacter();
    d.persistence = 1;
    driveDefine(d);
    ASSERT_EQ(lastDefineError_, 0u);
    const int32_t src = lastDefineTopicId_;

    CreateViewMsg cv;
    cv.session = 1; cv.corr = 71; cv.persistence = 1;   // transient
    cv.viewName = "char_hp"; cv.keyField = "id"; cv.planBlob = charHpPlanBlobP(src, 1);
    driveCreateView(cv);
    ASSERT_EQ(lastViewError_, 0u);
    ASSERT_GE(lastViewTopicId_, 0);
    ASSERT_NE(viewRegistry_->byName("char_hp"), nullptr);
    ASSERT_EQ(viewSet_.drainInstalls(), 1);

    // Fold one record (id 7, hp 55) from the transient source.
    std::vector<uint8_t> key(8, 0); key[0] = 7;
    std::vector<uint8_t> value;
    auto le = [&](uint64_t x, int n) { for (int i = 0; i < n; ++i) value.push_back(uint8_t(x >> (8 * i))); };
    le(16, 2); le(1, 2); le(7, 8); le(55, 8);
    rune::protocol::RecordBatchBuilder bb(0, 0);
    bb.append(std::string_view(reinterpret_cast<const char*>(key.data()), key.size()),
              std::string_view(reinterpret_cast<const char*>(value.data()), value.size()), 0);
    const auto batch = bb.build();
    auto rr = [&](int64_t, int64_t) {
        return rune::storage::LogReadView{batch.data(), batch.size(), 0, 1};
    };
    viewSet_.driveFolds(src, 0, rr, 1, 0);
    const rv::RowLayout* L = nullptr;
    const uint8_t* row = viewSet_.getRow(lastViewTopicId_, 0, 7, L);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(rv::readSlot(row, *L, 1).i, 55);
}
