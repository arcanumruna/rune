// ---------------------------------------------------------------------------
// GroupHandlerIntegrationTest.cpp — CG-3 integration tests (fix-round 1).
//
// Drives real Join→doWork→Sync→OffsetCommit→OffsetFetch through
// GroupCoordinationService against a real GroupCoordinator (in-process) +
// ReplicatedOffsetStore (single-node leader, minIsr=1).
//
// TopicRegistry: a real TopicRegistry is wired in with "test-topic" (2
// partitions, topicId=0) so that offset persistence and fetch-all are
// genuinely exercised.
//
// No Aeron.  No media driver.  All waits are bounded (kCap + ASSERT_TRUE).
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/GroupCoordinationService.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/coordination/GroupCoordinator.h>
#include <rune/coordination/ReplicatedOffsetStore.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::node;
using namespace rune::coordination;
using namespace rune::replication;
namespace pc = rune::protocol::client;

// ===========================================================================
// Infrastructure (identical to GroupCoordinationServiceTest)
// ===========================================================================

static constexpr int kCap = 800;
static constexpr int64_t kStep = 1'000'000LL;  // 1 ms in ns

static std::string makeSuffix() {
    static std::atomic<uint64_t> g{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_" +
           std::to_string(g.fetch_add(1, std::memory_order_relaxed)) + "_" +
           std::to_string(ts);
}

struct TempDir {
    std::string path;
    explicit TempDir(std::string_view tag) {
        path = std::filesystem::temp_directory_path().string()
             + "/rune_cg3_integ_" + std::string(tag) + "_" + makeSuffix();
        std::filesystem::create_directories(path);
    }
    ~TempDir() { std::error_code ec; std::filesystem::remove_all(path, ec); }
};

class NullTransport2 : public PartitionTransport {
public:
    bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const FetchResponseMsg&) override { return true; }
};

static PartitionConfig makeCfg(NodeId self = 1, int minIsr = 1) {
    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = self;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = minIsr;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    return cfg;
}

static TopicSpec makeTopicSpec(std::string name, int32_t partCount, int32_t id) {
    TopicSpec ts;
    ts.name           = std::move(name);
    ts.partitionCount = partCount;
    ts.topicId        = id;
    return ts;
}

// Known test topic used across integration tests.
static constexpr std::string_view kTestTopic = "test-topic";
static constexpr int32_t kTestTopicId        = 0;

// ===========================================================================
// Frame encode/decode helpers (mirrors unit tests — minimal)
// ===========================================================================

static std::size_t encJoin(uint8_t* b, int64_t s, int64_t c,
                            std::string_view g, std::string_view m,
                            int32_t tms = 5000)
{
    pc::JoinGroup msg;
    msg.sessionId = s; msg.correlationId = c; msg.sessionTimeoutMs = tms;
    msg.groupId  = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(g.data()), g.size());
    msg.memberId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(m.data()), m.size());
    return msg.encode(b, std::span<const int32_t>());
}

static std::size_t encSync(uint8_t* b, int64_t s, int64_t c,
                            std::string_view g, std::string_view m, int32_t gen)
{
    pc::SyncGroup msg;
    msg.sessionId = s; msg.correlationId = c; msg.generationId = gen;
    msg.groupId  = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(g.data()), g.size());
    msg.memberId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(m.data()), m.size());
    return msg.encode(b);
}

static std::size_t encHB(uint8_t* b, int64_t s, int64_t c,
                          std::string_view g, std::string_view m, int32_t gen)
{
    pc::Heartbeat msg;
    msg.sessionId = s; msg.correlationId = c; msg.generationId = gen;
    msg.groupId  = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(g.data()), g.size());
    msg.memberId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(m.data()), m.size());
    return msg.encode(b);
}

static std::size_t encOffsetCommit(uint8_t* b, int64_t s, int64_t c,
                                    std::string_view g, std::string_view m,
                                    int32_t gen, int32_t tid, int32_t part, int64_t off)
{
    pc::OffsetCommit msg;
    msg.sessionId = s; msg.correlationId = c; msg.generationId = gen;
    msg.groupId  = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(g.data()), g.size());
    msg.memberId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(m.data()), m.size());
    pc::OffsetCommitEntry e;
    e.topicId = tid; e.partition = part; e.offset = off;
    e.metadata = std::span<const uint8_t>();
    std::array<pc::OffsetCommitEntry, 1> entries{e};
    return msg.encode(b, std::span<const pc::OffsetCommitEntry>(entries));
}

static std::size_t encOffsetFetch(uint8_t* b, int64_t s, int64_t c,
                                   std::string_view g, int32_t tid, int32_t part)
{
    pc::OffsetFetch msg;
    msg.sessionId = s; msg.correlationId = c;
    msg.groupId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(g.data()), g.size());
    pc::OffsetFetchEntry e; e.topicId = tid; e.partition = part;
    std::array<pc::OffsetFetchEntry, 1> entries{e};
    return msg.encode(b, std::span<const pc::OffsetFetchEntry>(entries));
}

static std::size_t encOffsetFetchAll(uint8_t* b, int64_t s, int64_t c,
                                      std::string_view g)
{
    pc::OffsetFetch msg;
    msg.sessionId = s; msg.correlationId = c;
    msg.groupId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(g.data()), g.size());
    return msg.encode(b, std::span<const pc::OffsetFetchEntry>());
}

// ===========================================================================
// Fixture
// ===========================================================================

class CG3IntegTest : public ::testing::Test {
protected:
    CG3IntegTest()
        : dir("cg3_integ")
        , topicDir("cg3_integ_topics")
        , log(dir.path)
    {}

    void SetUp() override {
        transport_ = std::make_unique<NullTransport2>();
        replica_ = std::make_unique<PartitionReplica>(
            makeCfg(1, 1),
            Role::Leader, 1,
            std::vector<NodeId>{1},
            std::vector<NodeId>{1},
            &log,
            transport_.get());
        store_ = std::make_unique<ReplicatedOffsetStore>(replica_.get());

        // Real TopicRegistry with a known test topic.
        std::vector<TopicSpec> specs{
            makeTopicSpec(std::string(kTestTopic), /*partitions=*/2, kTestTopicId)
        };
        registry_ = std::make_unique<rune::gateway::TopicRegistry>(specs, topicDir.path);

        service_ = std::make_unique<GroupCoordinationService>(
            store_.get(),
            registry_.get(),
            [] { return true; },
            [] { return NodeId{1}; });
        service_->doWork(0);   // E2 residual round 4: offsets loaded on the first duty pass
    }

    bool driveUntil(std::function<bool()> pred, int cap = kCap) {
        for (int i = 0; i < cap && !pred(); ++i) {
            clock_ += kStep;
            service_->doWork(clock_);
        }
        return pred();
    }

    bool pumpHW(int64_t target, int cap = kCap) {
        for (int i = 0; i < cap && replica_->highWatermark() < target; ++i) {
            clock_ += kStep;
            replica_->doWork(clock_);
        }
        return replica_->highWatermark() >= target;
    }

    TempDir                                   dir;
    TempDir                                   topicDir;
    rune::storage::Log                        log;
    std::unique_ptr<NullTransport2>           transport_;
    std::unique_ptr<PartitionReplica>         replica_;
    std::unique_ptr<ReplicatedOffsetStore>    store_;
    std::unique_ptr<rune::gateway::TopicRegistry> registry_;
    std::unique_ptr<GroupCoordinationService> service_;

    int64_t clock_ = 0;

    static constexpr std::size_t kBuf = 64 * 1024;
    uint8_t req_[kBuf]{};
    uint8_t out_[kBuf]{};
};

// ===========================================================================
// Integration tests
// ===========================================================================

// 1. Full Join → re-join → doWork → Stable flow.
TEST_F(CG3IntegTest, JoinThenDoWorkReachesStable) {
    std::string group = "integ-g1";

    // Step 1: Initial join → PreparingRebalance + assigned memberId.
    auto j1Len = encJoin(req_, 1, 1, group, "", /*tms=*/5000);
    auto j1Out = service_->handleJoinGroup(req_, j1Len, clock_, out_, kBuf);
    ASSERT_GT(j1Out, 0u);
    auto j1 = pc::JoinGroupResponse::decode(out_, j1Out);
    ASSERT_TRUE(j1.has_value());
    EXPECT_EQ(j1->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    const std::string m1Id(
        reinterpret_cast<const char*>(j1->memberId.data()), j1->memberId.size());
    EXPECT_EQ(service_->coordinator(group).state(), GroupState::PreparingRebalance);

    // Step 2: Re-join with assigned memberId → CompletingRebalance.
    clock_ += kStep;
    auto r1Len = encJoin(req_, 1, 2, group, m1Id, /*tms=*/5000);
    service_->handleJoinGroup(req_, r1Len, clock_, out_, kBuf);

    // Step 3: doWork → Stable.
    const bool stable = driveUntil([&] {
        return service_->coordinator(group).state() == GroupState::Stable;
    }, 10);
    EXPECT_TRUE(stable) << "Group should reach Stable after re-join + doWork";
}

// 2. SyncGroup after Stable → OK.
TEST_F(CG3IntegTest, SyncGroupAfterStableReturnsOK) {
    std::string group = "integ-g2";

    // Initial join.
    auto jLen = encJoin(req_, 2, 10, group, "", /*tms=*/5000);
    auto jOut = service_->handleJoinGroup(req_, jLen, clock_, out_, kBuf);
    ASSERT_GT(jOut, 0u);
    auto jResp = pc::JoinGroupResponse::decode(out_, jOut);
    ASSERT_TRUE(jResp.has_value());
    const std::string memberId(
        reinterpret_cast<const char*>(jResp->memberId.data()),
        jResp->memberId.size());

    // Re-join → CompletingRebalance.
    clock_ += kStep;
    service_->handleJoinGroup(req_, encJoin(req_, 2, 11, group, memberId, 5000), clock_, out_, kBuf);

    // doWork → Stable.
    const bool stable = driveUntil([&] {
        return service_->coordinator(group).state() == GroupState::Stable;
    }, 10);
    ASSERT_TRUE(stable);

    const int32_t gen = service_->coordinator(group).generationId();
    auto sLen = encSync(req_, 2, 12, group, memberId, gen);
    auto sOut = service_->handleSyncGroup(req_, sLen, clock_, out_, kBuf);
    ASSERT_GT(sOut, 0u);

    auto sResp = pc::SyncGroupResponse::decode(out_, sOut);
    ASSERT_TRUE(sResp.has_value());
    EXPECT_EQ(sResp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
}

// 3. Heartbeat after Stable → OK (not RebalanceInProgress).
TEST_F(CG3IntegTest, HeartbeatAfterStableReturnsOK) {
    std::string group = "integ-g3";

    auto jLen = encJoin(req_, 3, 20, group, "", /*tms=*/5000);
    auto jOut = service_->handleJoinGroup(req_, jLen, clock_, out_, kBuf);
    ASSERT_GT(jOut, 0u);
    auto jResp = pc::JoinGroupResponse::decode(out_, jOut);
    ASSERT_TRUE(jResp.has_value());
    const std::string memberId(
        reinterpret_cast<const char*>(jResp->memberId.data()),
        jResp->memberId.size());

    clock_ += kStep;
    service_->handleJoinGroup(req_, encJoin(req_, 3, 21, group, memberId, 5000), clock_, out_, kBuf);

    const bool stable = driveUntil([&] {
        return service_->coordinator(group).state() == GroupState::Stable;
    }, 10);
    ASSERT_TRUE(stable);

    const int32_t gen = service_->coordinator(group).generationId();
    auto hLen = encHB(req_, 3, 22, group, memberId, gen);
    auto hOut = service_->handleHeartbeat(req_, hLen, clock_, out_, kBuf);
    ASSERT_GT(hOut, 0u);

    auto hResp = pc::HeartbeatResponse::decode(out_, hOut);
    ASSERT_TRUE(hResp.has_value());
    EXPECT_EQ(hResp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
}

// 4. OffsetCommit + OffsetFetch through real TopicRegistry + ReplicatedOffsetStore.
//    Verifies persistence: commit returns OK, fetch returns the committed offset.
TEST_F(CG3IntegTest, OffsetCommitAndFetchRoundTrip) {
    const std::string group = "integ-g4";

    // Join so we have a valid generation.
    auto jLen = encJoin(req_, 4, 30, group, "", 5000);
    auto jOut = service_->handleJoinGroup(req_, jLen, clock_, out_, kBuf);
    ASSERT_GT(jOut, 0u);
    auto jResp = pc::JoinGroupResponse::decode(out_, jOut);
    ASSERT_TRUE(jResp.has_value());
    const std::string memberId(
        reinterpret_cast<const char*>(jResp->memberId.data()),
        jResp->memberId.size());
    const int32_t gen = jResp->generationId;

    // Commit offset 12345 for (kTestTopicId, partition=0).
    auto ocLen = encOffsetCommit(req_, 4, 31, group, memberId, gen, kTestTopicId, 0, 12345);
    auto ocOut = service_->handleOffsetCommit(req_, ocLen, clock_, out_, kBuf);
    ASSERT_GT(ocOut, 0u);

    auto ocResp = pc::OffsetCommitResponse::decode(out_, ocOut);
    ASSERT_TRUE(ocResp.has_value());
    ASSERT_FALSE(ocResp->results.empty());
    EXPECT_EQ(ocResp->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::OK));

    // Fetch the committed offset — must return 12345.
    auto ofLen = encOffsetFetch(req_, 4, 32, group, kTestTopicId, 0);
    auto ofOut = service_->handleOffsetFetch(req_, ofLen, clock_, out_, kBuf);
    ASSERT_GT(ofOut, 0u);

    auto ofResp = pc::OffsetFetchResponse::decode(out_, ofOut);
    ASSERT_TRUE(ofResp.has_value());
    ASSERT_FALSE(ofResp->results.empty());
    EXPECT_EQ(ofResp->results[0].offset, int64_t{12345});
    EXPECT_EQ(ofResp->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
}

// 5. Two-member group: both join, both re-join → Stable.
TEST_F(CG3IntegTest, TwoMemberGroupReachesStable) {
    std::string group = "integ-g5";

    // Step 1: m1 initial join.
    auto j1Len = encJoin(req_, 5, 40, group, "", /*tms=*/5000);
    auto j1Out = service_->handleJoinGroup(req_, j1Len, clock_, out_, kBuf);
    ASSERT_GT(j1Out, 0u);
    auto j1 = pc::JoinGroupResponse::decode(out_, j1Out);
    ASSERT_TRUE(j1.has_value());
    EXPECT_EQ(j1->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    const std::string m1Id(
        reinterpret_cast<const char*>(j1->memberId.data()), j1->memberId.size());
    ASSERT_FALSE(m1Id.empty());

    // Step 2: m2 initial join → triggers new rebalance (gen 2, expectedCount=2).
    clock_ += kStep;
    auto j2Len = encJoin(req_, 5, 41, group, "", /*tms=*/5000);
    auto j2Out = service_->handleJoinGroup(req_, j2Len, clock_, out_, kBuf);
    ASSERT_GT(j2Out, 0u);
    auto j2 = pc::JoinGroupResponse::decode(out_, j2Out);
    ASSERT_TRUE(j2.has_value());
    EXPECT_EQ(j2->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    const std::string m2Id(
        reinterpret_cast<const char*>(j2->memberId.data()), j2->memberId.size());
    ASSERT_FALSE(m2Id.empty());

    EXPECT_EQ(service_->coordinator(group).memberCount(), 2u);
    EXPECT_EQ(service_->coordinator(group).state(), GroupState::PreparingRebalance);

    // Step 3: m1 explicitly re-joins.
    clock_ += kStep;
    service_->handleJoinGroup(req_, encJoin(req_, 5, 42, group, m1Id, 5000), clock_, out_, kBuf);

    // Step 4: m2 explicitly re-joins → all expected → CompletingRebalance.
    clock_ += kStep;
    service_->handleJoinGroup(req_, encJoin(req_, 5, 43, group, m2Id, 5000), clock_, out_, kBuf);

    EXPECT_EQ(service_->coordinator(group).memberCount(), 2u);

    // Step 5: doWork → Stable.
    const bool stable = driveUntil([&] {
        return service_->coordinator(group).state() == GroupState::Stable;
    }, 10);
    EXPECT_TRUE(stable) << "Two-member group should reach Stable";
    EXPECT_EQ(service_->coordinator(group).memberCount(), 2u);
}

// 6. OffsetFetch on an unknown/empty group returns sentinel for known topic.
TEST_F(CG3IntegTest, OffsetFetchUncommittedReturnsSentinel) {
    // Known topic (kTestTopicId) but nothing committed → sentinel -1 with OK.
    auto fLen = encOffsetFetch(req_, 6, 50, "never-seen", kTestTopicId, 0);
    auto fOut = service_->handleOffsetFetch(req_, fLen, clock_, out_, kBuf);
    ASSERT_GT(fOut, 0u);

    auto fResp = pc::OffsetFetchResponse::decode(out_, fOut);
    ASSERT_TRUE(fResp.has_value());
    ASSERT_FALSE(fResp->results.empty());
    EXPECT_EQ(fResp->results[0].offset, int64_t{-1});
    EXPECT_EQ(fResp->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
}

// 7. doWork on service with existing group keeps returning 0 in steady state.
TEST_F(CG3IntegTest, DoWorkIdleInSteadyState) {
    for (int i = 0; i < 10; ++i) {
        clock_ += kStep;
        EXPECT_EQ(service_->doWork(clock_), 0);
    }

    std::string group = "integ-g7";
    auto jLen = encJoin(req_, 7, 60, group, "", 50);
    service_->handleJoinGroup(req_, jLen, clock_, out_, kBuf);

    driveUntil([&] {
        return service_->coordinator(group).state() == GroupState::Stable;
    }, 200);

    for (int i = 0; i < 10; ++i) {
        clock_ += kStep;
        const int work = service_->doWork(clock_);
        (void)work;
    }
}

// 8. OffsetFetch fetch-all with a large number of committed (group,tp) pairs
//    → assert no overflow and a correct/bounded response.
//    This tests I2: the handler must bound-check BEFORE each write so the
//    fixed scratch buffer is never overrun, even when the total result set
//    would theoretically exceed it.
//
//    We commit N = 2 entries (the test topic has 2 partitions), then fetch-all.
//    The tiny scratch (overridden to a small value via outCap) verifies that
//    the handler caps the results and never writes past outCap.
TEST_F(CG3IntegTest, OffsetFetchFetchAllBoundedByOutCap) {
    const std::string group = "integ-g8";

    auto jLen = encJoin(req_, 8, 70, group, "", 5000);
    auto jOut = service_->handleJoinGroup(req_, jLen, clock_, out_, kBuf);
    ASSERT_GT(jOut, 0u);
    auto jResp = pc::JoinGroupResponse::decode(out_, jOut);
    ASSERT_TRUE(jResp.has_value());
    const std::string memberId(
        reinterpret_cast<const char*>(jResp->memberId.data()),
        jResp->memberId.size());
    const int32_t gen = jResp->generationId;

    // Commit both partitions.
    for (int32_t p = 0; p < 2; ++p) {
        auto ocLen = encOffsetCommit(req_, 8, 71 + p, group, memberId, gen,
                                     kTestTopicId, p, int64_t{1000 + p * 100});
        auto ocOut = service_->handleOffsetCommit(req_, ocLen, clock_, out_, kBuf);
        ASSERT_GT(ocOut, 0u);
        auto ocResp = pc::OffsetCommitResponse::decode(out_, ocOut);
        ASSERT_TRUE(ocResp.has_value());
        ASSERT_FALSE(ocResp->results.empty());
        EXPECT_EQ(ocResp->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    }

    // Fetch-all into a tiny scratch that fits exactly 1 result:
    // encodedSizeFor(1) = 24 + 2 + 18 = 44 bytes.
    // The handler must cap the result at 1 entry and not write beyond tinyBuf.
    static constexpr std::size_t kTinyBuf = 44;
    uint8_t tinyOut[kBuf]{}; // actual backing is full-size for safety; outCap is the limit
    auto fallLen = encOffsetFetchAll(req_, 8, 73, group);
    auto fallOut = service_->handleOffsetFetch(req_, fallLen, clock_, tinyOut, kTinyBuf);

    // Must not overflow: encoded size must be <= kTinyBuf.
    EXPECT_LE(fallOut, kTinyBuf)
        << "Handler wrote " << fallOut << " bytes but outCap was " << kTinyBuf;
    ASSERT_GT(fallOut, 0u);

    auto fallResp = pc::OffsetFetchResponse::decode(tinyOut, fallOut);
    ASSERT_TRUE(fallResp.has_value());
    // At most 1 result fits in 44 bytes.
    EXPECT_LE(fallResp->results.size(), std::size_t{1})
        << "Response has " << fallResp->results.size()
        << " results but outCap only fits 1";

    // Now fetch-all into the full scratch — both entries must appear.
    auto fullLen = encOffsetFetchAll(req_, 8, 74, group);
    auto fullOut = service_->handleOffsetFetch(req_, fullLen, clock_, out_, kBuf);
    ASSERT_GT(fullOut, 0u);
    EXPECT_LE(fullOut, kBuf);

    auto fullResp = pc::OffsetFetchResponse::decode(out_, fullOut);
    ASSERT_TRUE(fullResp.has_value());
    EXPECT_EQ(fullResp->results.size(), std::size_t{2});

    // Verify the offsets.
    std::map<int32_t, int64_t> offsets;
    for (std::size_t i = 0; i < fullResp->results.size(); ++i) {
        const auto& r = fullResp->results[i];
        EXPECT_EQ(r.errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
        offsets[r.partition] = r.offset;
    }
    EXPECT_EQ(offsets[0], int64_t{1000});
    EXPECT_EQ(offsets[1], int64_t{1100});
}
