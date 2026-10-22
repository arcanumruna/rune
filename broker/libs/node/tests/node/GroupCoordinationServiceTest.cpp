// ---------------------------------------------------------------------------
// GroupCoordinationServiceTest.cpp — CG-3 unit tests (fix-round 1).
//
// Injected clock + fake leader toggle.  No Aeron.  No Aeron media driver.
// Uses a single-node PartitionReplica (leader, minIsr=1) as the backing
// replica for the ReplicatedOffsetStore (same pattern as
// ReplicatedOffsetStoreTest.cpp in CG-2).
//
// TopicRegistry: a real TopicRegistry is wired into the fixture with a known
// test topic ("test-topic", 2 partitions, topicId=0) so that OffsetCommit
// persistence, MinIsrViolation, and OffsetFetch fetch-all are genuinely
// exercised (not silently bypassed by null-registry UnknownTopic returns).
//
// Test matrix:
//   1. doWork on empty service → 0; coordinator("g") lazily creates.
//   2. coordinatorFor returns the leaderId even when not local coordinator.
//   3. Join → memberId+generationId, state PreparingRebalance.
//   4. After re-join + doWork → Stable; SyncGroup returns OK.
//   5. Heartbeat during rebalance → RebalanceInProgress.
//   6. Heartbeat stale generation → StaleGeneration.
//   7. Heartbeat unknown member → UnknownMember.
//   8. Leave shrinks membership + triggers rebalance.
//   9. Any handler when isLocalCoordinator()==false → NotCoordinator.
//  10. OffsetCommit correct-gen persists; OffsetFetch returns the REAL offset.
//  11. OffsetCommit wrong gen → IllegalGeneration.
//  12. OffsetFetch uncommitted → sentinel (-1) with OK.
//  13. OffsetCommit minIsr-gated commit → MinIsrViolation (genuine path via real registry).
//  14. OffsetFetch fetch-all returns all committed (group,tp) pairs (genuine fetch-all path).
//  15. Defensive OffsetCommit iteration: overstated count / truncated frame → no over-read.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/GroupCoordinationService.h>
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/Gpid.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/coordination/GroupCoordinator.h>
#include <rune/coordination/ReplicatedOffsetStore.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/node/GroupErrorMapping.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::node;
using namespace rune::coordination;
using namespace rune::replication;
namespace pc = rune::protocol::client;

// ===========================================================================
// Test infrastructure
// ===========================================================================

static constexpr int kDriverCap = 600;

static std::string makeTempSuffix() {
    static std::atomic<uint64_t> g_seq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_"
         + std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed)) + "_"
         + std::to_string(ts);
}

struct TempDir {
    std::string path;
    explicit TempDir(std::string_view tag) {
        path = std::filesystem::temp_directory_path().string()
             + "/rune_cg3_test_" + std::string(tag) + "_" + makeTempSuffix();
        std::filesystem::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

// Null transport — single-node leader (minIsr=1) never sends to followers.
class NullTransport : public PartitionTransport {
public:
    bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const FetchResponseMsg&) override { return true; }
};

static PartitionConfig makeSingleNodeConfig(NodeId selfId = 1, int minIsr = 1) {
    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = selfId;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = minIsr;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    return cfg;
}

// Build a TopicSpec for use in TopicRegistry construction.
static TopicSpec makeTopicSpec(std::string name, int32_t partCount, int32_t id) {
    TopicSpec ts;
    ts.name           = std::move(name);
    ts.partitionCount = partCount;
    ts.topicId        = id;
    return ts;
}

static TopicPartition tp(std::string topic, int32_t part) {
    return {std::move(topic), part};
}

// ===========================================================================
// Encode / decode helpers for test frames
// ===========================================================================

// Encode a JoinGroup request into buf; return bytes written.
static std::size_t encodeJoinGroup(uint8_t* buf,
                                   int64_t sessionId,
                                   int64_t corrId,
                                   std::string_view groupId,
                                   std::string_view memberId,
                                   int32_t sessionTimeoutMs = 5000,
                                   std::span<const uint8_t> filter = {})
{
    pc::JoinGroup msg;
    msg.filter           = filter;
    msg.sessionId        = sessionId;
    msg.correlationId    = corrId;
    msg.sessionTimeoutMs = sessionTimeoutMs;
    msg.groupId  = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(groupId.data()), groupId.size());
    msg.memberId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(memberId.data()), memberId.size());
    return msg.encode(buf, std::span<const int32_t>());
}

static std::size_t encodeSyncGroup(uint8_t* buf,
                                   int64_t sessionId,
                                   int64_t corrId,
                                   std::string_view groupId,
                                   std::string_view memberId,
                                   int32_t generationId)
{
    pc::SyncGroup msg;
    msg.sessionId     = sessionId;
    msg.correlationId = corrId;
    msg.generationId  = generationId;
    msg.groupId  = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(groupId.data()), groupId.size());
    msg.memberId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(memberId.data()), memberId.size());
    return msg.encode(buf);
}

static std::size_t encodeHeartbeat(uint8_t* buf,
                                   int64_t sessionId,
                                   int64_t corrId,
                                   std::string_view groupId,
                                   std::string_view memberId,
                                   int32_t generationId)
{
    pc::Heartbeat msg;
    msg.sessionId     = sessionId;
    msg.correlationId = corrId;
    msg.generationId  = generationId;
    msg.groupId  = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(groupId.data()), groupId.size());
    msg.memberId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(memberId.data()), memberId.size());
    return msg.encode(buf);
}

static std::size_t encodeLeaveGroup(uint8_t* buf,
                                    int64_t sessionId,
                                    int64_t corrId,
                                    std::string_view groupId,
                                    std::string_view memberId)
{
    pc::LeaveGroup msg;
    msg.sessionId     = sessionId;
    msg.correlationId = corrId;
    msg.groupId  = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(groupId.data()), groupId.size());
    msg.memberId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(memberId.data()), memberId.size());
    return msg.encode(buf);
}

// Single-entry OffsetCommit request.
static std::size_t encodeOffsetCommit(uint8_t* buf,
                                      int64_t sessionId,
                                      int64_t corrId,
                                      std::string_view groupId,
                                      std::string_view memberId,
                                      int32_t generationId,
                                      int32_t topicId,
                                      int32_t partition,
                                      int64_t offset,
                                      std::string_view metadata = "")
{
    pc::OffsetCommit msg;
    msg.sessionId     = sessionId;
    msg.correlationId = corrId;
    msg.generationId  = generationId;
    msg.groupId  = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(groupId.data()), groupId.size());
    msg.memberId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(memberId.data()), memberId.size());

    pc::OffsetCommitEntry entry;
    entry.topicId   = topicId;
    entry.partition = partition;
    entry.offset    = offset;
    entry.metadata  = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(metadata.data()), metadata.size());

    std::array<pc::OffsetCommitEntry, 1> entries{entry};
    return msg.encode(buf, std::span<const pc::OffsetCommitEntry>(entries));
}

// Single-entry OffsetFetch request.
static std::size_t encodeOffsetFetch(uint8_t* buf,
                                     int64_t sessionId,
                                     int64_t corrId,
                                     std::string_view groupId,
                                     int32_t topicId,
                                     int32_t partition)
{
    pc::OffsetFetch msg;
    msg.sessionId     = sessionId;
    msg.correlationId = corrId;
    msg.groupId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(groupId.data()), groupId.size());

    pc::OffsetFetchEntry entry;
    entry.topicId   = topicId;
    entry.partition = partition;

    std::array<pc::OffsetFetchEntry, 1> entries{entry};
    return msg.encode(buf, std::span<const pc::OffsetFetchEntry>(entries));
}

// Fetch-all OffsetFetch request (empty entries list).
static std::size_t encodeOffsetFetchAll(uint8_t* buf,
                                        int64_t sessionId,
                                        int64_t corrId,
                                        std::string_view groupId)
{
    pc::OffsetFetch msg;
    msg.sessionId     = sessionId;
    msg.correlationId = corrId;
    msg.groupId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(groupId.data()), groupId.size());
    return msg.encode(buf, std::span<const pc::OffsetFetchEntry>());
}

// Decode HeartbeatResponse errorCode from response buffer.
static int16_t decodeHBErrorCode(const uint8_t* out, std::size_t len) {
    auto opt = pc::HeartbeatResponse::decode(out, len);
    EXPECT_TRUE(opt.has_value());
    return opt ? opt->errorCode : int16_t{-1};
}

static int16_t decodeJoinErrorCode(const uint8_t* out, std::size_t len) {
    auto opt = pc::JoinGroupResponse::decode(out, len);
    EXPECT_TRUE(opt.has_value());
    return opt ? opt->errorCode : int16_t{-1};
}

static int16_t decodeSyncErrorCode(const uint8_t* out, std::size_t len) {
    auto opt = pc::SyncGroupResponse::decode(out, len);
    EXPECT_TRUE(opt.has_value());
    return opt ? opt->errorCode : int16_t{-1};
}

static int16_t decodeLeaveErrorCode(const uint8_t* out, std::size_t len) {
    auto opt = pc::LeaveGroupResponse::decode(out, len);
    EXPECT_TRUE(opt.has_value());
    return opt ? opt->errorCode : int16_t{-1};
}

// ===========================================================================
// Fixture
// ===========================================================================

// Known test topic: "test-topic", 2 partitions, topicId=0.
static constexpr std::string_view kTestTopic = "test-topic";
static constexpr int32_t kTestTopicId        = 0;

class CG3ServiceTest : public ::testing::Test {
protected:
    static constexpr NodeId kLeader = 1;
    static constexpr int64_t kStep  = 1'000'000LL; // 1 ms in ns

    CG3ServiceTest()
        : dir("cg3_svc")
        , topicDir("cg3_svc_topics")
        , log(dir.path)
    {}

    void SetUp() override {
        transport_ = std::make_unique<NullTransport>();
        replica_ = std::make_unique<PartitionReplica>(
            makeSingleNodeConfig(kLeader, /*minIsr=*/1),
            Role::Leader, /*epoch=*/1,
            std::vector<NodeId>{kLeader},
            std::vector<NodeId>{kLeader},
            &log,
            transport_.get());
        store_ = std::make_unique<ReplicatedOffsetStore>(replica_.get());

        // Build a real TopicRegistry with one known topic so OffsetCommit
        // persistence and fetch-all paths are genuinely exercised.
        std::vector<TopicSpec> specs{
            makeTopicSpec(std::string(kTestTopic), /*partitions=*/2, kTestTopicId)
        };
        registry_ = std::make_unique<rune::gateway::TopicRegistry>(specs, topicDir.path);

        // Create service: initially the leader; real TopicRegistry wired in.
        service_ = std::make_unique<GroupCoordinationService>(
            store_.get(),
            registry_.get(),
            [this] { return isLeader_; },
            [this] { return leaderNodeId_; });
        // E2 residual round 4: the coordinator serves offsets only once it has
        // loaded them (its first duty pass as coordinator, as in BrokerNode).
        service_->doWork(clock_);
    }

    // Pump replica HW (for offset commit visibility).
    bool pumpHW(int64_t target) {
        for (int i = 0; i < kDriverCap && replica_->highWatermark() < target; ++i) {
            clock_ += kStep;
            replica_->doWork(clock_);
        }
        return replica_->highWatermark() >= target;
    }

    // Drive doWork until pred or cap exceeded.
    bool driveUntil(std::function<bool()> pred, int cap = kDriverCap) {
        for (int i = 0; i < cap && !pred(); ++i) {
            clock_ += kStep;
            service_->doWork(clock_);
        }
        return pred();
    }

    TempDir                                  dir;
    TempDir                                  topicDir;
    rune::storage::Log                       log;
    std::unique_ptr<NullTransport>           transport_;
    std::unique_ptr<PartitionReplica>        replica_;
    std::unique_ptr<ReplicatedOffsetStore>   store_;
    std::unique_ptr<rune::gateway::TopicRegistry> registry_;
    std::unique_ptr<GroupCoordinationService> service_;

    bool   isLeader_     = true;
    NodeId leaderNodeId_ = kLeader;
    int64_t clock_       = 0;

    static constexpr std::size_t kBufSize = 64 * 1024;
    uint8_t req_[kBufSize]{};
    uint8_t out_[kBufSize]{};
};

// ===========================================================================
// Tests
// ===========================================================================

// 1. doWork on empty service → 0; coordinator("g") lazily creates.
TEST_F(CG3ServiceTest, DoWorkEmptyReturnsZero) {
    EXPECT_EQ(service_->doWork(clock_), 0);

    // Coordinator access creates it lazily.
    auto& gc = service_->coordinator("test-group");
    EXPECT_EQ(gc.state(), GroupState::Empty);
    EXPECT_EQ(gc.memberCount(), 0u);
}

// 2. coordinatorFor returns leaderId even when not local coordinator.
TEST_F(CG3ServiceTest, CoordinatorForReturnsLeaderId) {
    leaderNodeId_ = 42;
    isLeader_     = false;
    EXPECT_EQ(service_->coordinatorFor("any-group"), NodeId{42});

    isLeader_     = true;
    leaderNodeId_ = 1;
    EXPECT_EQ(service_->coordinatorFor("any-group"), NodeId{1});
}

// 3. Join → memberId+generationId, state PreparingRebalance.
TEST_F(CG3ServiceTest, JoinGroupReturnsValidMemberAndGeneration) {
    auto reqLen = encodeJoinGroup(req_, 1, 100, "grp1", "");
    auto outLen = service_->handleJoinGroup(req_, reqLen, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    EXPECT_GT(outLen, 0u);

    auto resp = pc::JoinGroupResponse::decode(out_, outLen);
    ASSERT_TRUE(resp.has_value());
    EXPECT_EQ(resp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_GT(resp->generationId, int32_t{0});
    EXPECT_FALSE(resp->memberId.empty());  // assigned a memberId

    // State should be PreparingRebalance (first join triggers rebalance).
    EXPECT_EQ(service_->coordinator("grp1").state(), GroupState::PreparingRebalance);
}

// 4. After doWork drives the rebalance → Stable, SyncGroup returns OK.
//
// Kafka-faithful: when a member's initial join triggers a rebalance, the
// member must explicitly re-join (send JoinGroup again with its assigned
// memberId).  Once all expected members have re-joined, the coordinator
// transitions to CompletingRebalance.  GroupCoordinationService::doWork
// detects this and calls autoAssignAndSync, driving the group to Stable.
//
// Flow:
//   1. Join (empty memberId) → assigned memberId + state=PreparingRebalance.
//   2. Re-join with assigned memberId → all expected members rejoined →
//      coordinator transitions to CompletingRebalance internally.
//   3. doWork detects CompletingRebalance + !leaderSynced → autoAssignAndSync
//      → state=Stable.
//   4. SyncGroup with the current generationId → OK.
TEST_F(CG3ServiceTest, DoWorkAutoAssignsAfterJoinTimeout) {
    std::string groupId = "g-timeout";

    // Step 1: Initial join with empty memberId → PreparingRebalance.
    auto reqLen = encodeJoinGroup(req_, 1, 101, groupId, "", /*sessionTimeoutMs=*/5000);
    auto outLen = service_->handleJoinGroup(req_, reqLen, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    ASSERT_GT(outLen, 0u);
    auto joinResp = pc::JoinGroupResponse::decode(out_, outLen);
    ASSERT_TRUE(joinResp.has_value());
    EXPECT_EQ(joinResp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));

    const std::string memberId(
        reinterpret_cast<const char*>(joinResp->memberId.data()),
        joinResp->memberId.size());
    EXPECT_FALSE(memberId.empty());
    EXPECT_EQ(service_->coordinator(groupId).state(), GroupState::PreparingRebalance);

    // Step 2: Re-join with the assigned memberId.
    clock_ += kStep;
    auto rejoinLen = encodeJoinGroup(req_, 1, 102, groupId, memberId, /*sessionTimeoutMs=*/5000);
    auto rejoinOut = service_->handleJoinGroup(req_, rejoinLen, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    ASSERT_GT(rejoinOut, 0u);
    auto rejoinResp = pc::JoinGroupResponse::decode(out_, rejoinOut);
    ASSERT_TRUE(rejoinResp.has_value());
    EXPECT_EQ(rejoinResp->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));

    // Step 3: doWork detects CompletingRebalance + !leaderSynced →
    // autoAssignAndSync → Stable.
    const bool stable = driveUntil([&] {
        return service_->coordinator(groupId).state() == GroupState::Stable;
    }, 10);
    EXPECT_TRUE(stable) << "Group should reach Stable after all-members-rejoined + doWork";

    // Step 4: SyncGroup with the final generationId → OK.
    const int32_t gen = service_->coordinator(groupId).generationId();
    auto syncLen = encodeSyncGroup(req_, 1, 103, groupId, memberId, gen);
    auto syncOut = service_->handleSyncGroup(req_, syncLen, clock_, out_, kBufSize);
    ASSERT_GT(syncOut, 0u);
    EXPECT_EQ(decodeSyncErrorCode(out_, syncOut), static_cast<int16_t>(pc::ErrorCode::OK));
}

// 5. Heartbeat during rebalance → RebalanceInProgress.
TEST_F(CG3ServiceTest, HeartbeatDuringRebalanceReturnsRebalanceInProgress) {
    std::string groupId = "g-rebalance";

    // Join → PreparingRebalance.
    auto reqLen = encodeJoinGroup(req_, 1, 200, groupId, "", 5000);
    auto outLen = service_->handleJoinGroup(req_, reqLen, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    ASSERT_GT(outLen, 0u);

    auto joinResp = pc::JoinGroupResponse::decode(out_, outLen);
    ASSERT_TRUE(joinResp.has_value());

    const std::string memberId(
        reinterpret_cast<const char*>(joinResp->memberId.data()),
        joinResp->memberId.size());
    const int32_t gen = joinResp->generationId;

    EXPECT_EQ(service_->coordinator(groupId).state(), GroupState::PreparingRebalance);

    // Heartbeat while in PreparingRebalance → RebalanceInProgress.
    auto hbLen = encodeHeartbeat(req_, 1, 201, groupId, memberId, gen);
    auto hbOut = service_->handleHeartbeat(req_, hbLen, clock_, out_, kBufSize);
    ASSERT_GT(hbOut, 0u);

    EXPECT_EQ(decodeHBErrorCode(out_, hbOut),
              static_cast<int16_t>(pc::ErrorCode::RebalanceInProgress));
}

// 6. Heartbeat stale generation → StaleGeneration.
TEST_F(CG3ServiceTest, HeartbeatStaleGenerationReturnsStaleGeneration) {
    std::string groupId = "g-stale";

    auto reqLen = encodeJoinGroup(req_, 1, 300, groupId, "", 5000);
    auto outLen = service_->handleJoinGroup(req_, reqLen, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    ASSERT_GT(outLen, 0u);

    auto joinResp = pc::JoinGroupResponse::decode(out_, outLen);
    ASSERT_TRUE(joinResp.has_value());
    const std::string memberId(
        reinterpret_cast<const char*>(joinResp->memberId.data()),
        joinResp->memberId.size());

    // Stale generation = current - 1.
    const int32_t staleGen = joinResp->generationId - 1;
    auto hbLen = encodeHeartbeat(req_, 1, 301, groupId, memberId, staleGen);
    auto hbOut = service_->handleHeartbeat(req_, hbLen, clock_, out_, kBufSize);
    ASSERT_GT(hbOut, 0u);

    EXPECT_EQ(decodeHBErrorCode(out_, hbOut),
              static_cast<int16_t>(pc::ErrorCode::StaleGeneration));
}

// 7. Heartbeat unknown member → UnknownMember.
TEST_F(CG3ServiceTest, HeartbeatUnknownMemberReturnsUnknownMember) {
    std::string groupId = "g-unknown";

    // Join so the group exists.
    auto reqLen = encodeJoinGroup(req_, 1, 400, groupId, "", 5000);
    service_->handleJoinGroup(req_, reqLen, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);

    const int32_t gen = service_->coordinator(groupId).generationId();

    auto hbLen = encodeHeartbeat(req_, 1, 401, groupId, "ghost-member-999", gen);
    auto hbOut = service_->handleHeartbeat(req_, hbLen, clock_, out_, kBufSize);
    ASSERT_GT(hbOut, 0u);

    EXPECT_EQ(decodeHBErrorCode(out_, hbOut),
              static_cast<int16_t>(pc::ErrorCode::UnknownMember));
}

// 8. Leave shrinks membership + triggers rebalance.
TEST_F(CG3ServiceTest, LeaveGroupTriggersMembershipChange) {
    std::string groupId = "g-leave";

    // Step 1: Initial join.
    auto j1Len = encodeJoinGroup(req_, 1, 500, groupId, "", 5000);
    auto j1Out = service_->handleJoinGroup(req_, j1Len, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    ASSERT_GT(j1Out, 0u);
    auto j1Resp = pc::JoinGroupResponse::decode(out_, j1Out);
    ASSERT_TRUE(j1Resp.has_value());
    const std::string m1(
        reinterpret_cast<const char*>(j1Resp->memberId.data()),
        j1Resp->memberId.size());

    // Step 2: Re-join → CompletingRebalance internally.
    clock_ += kStep;
    auto r1Len = encodeJoinGroup(req_, 1, 501, groupId, m1, 5000);
    service_->handleJoinGroup(req_, r1Len, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);

    // Step 3: doWork → Stable.
    const bool stable = driveUntil([&] {
        return service_->coordinator(groupId).state() == GroupState::Stable;
    }, 10);
    EXPECT_TRUE(stable);

    const std::size_t countBefore = service_->coordinator(groupId).memberCount();
    EXPECT_EQ(countBefore, 1u);

    // Leave.
    auto leaveLen = encodeLeaveGroup(req_, 1, 502, groupId, m1);
    auto leaveOut = service_->handleLeaveGroup(req_, leaveLen, clock_, out_, kBufSize);
    ASSERT_GT(leaveOut, 0u);
    EXPECT_EQ(decodeLeaveErrorCode(out_, leaveOut),
              static_cast<int16_t>(pc::ErrorCode::OK));

    // After leave, group should be Empty (single member left).
    EXPECT_EQ(service_->coordinator(groupId).memberCount(), 0u);
    EXPECT_EQ(service_->coordinator(groupId).state(), GroupState::Empty);
}

// 9. Any handler when isLocalCoordinator()==false → NotCoordinator.
//    Also verifies NotCoordinator is returned for empty-entries OffsetCommit
//    and OffsetFetch (Minor-5 fix: empty-entries no longer returns silent OK).
TEST_F(CG3ServiceTest, AllHandlersReturnNotCoordinatorWhenNotLeader) {
    isLeader_ = false;

    // JoinGroup.
    auto jLen = encodeJoinGroup(req_, 1, 600, "g", "");
    auto jOut = service_->handleJoinGroup(req_, jLen, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    EXPECT_EQ(decodeJoinErrorCode(out_, jOut),
              static_cast<int16_t>(pc::ErrorCode::NotCoordinator));

    // SyncGroup.
    auto sLen = encodeSyncGroup(req_, 1, 601, "g", "m", 1);
    auto sOut = service_->handleSyncGroup(req_, sLen, clock_, out_, kBufSize);
    EXPECT_EQ(decodeSyncErrorCode(out_, sOut),
              static_cast<int16_t>(pc::ErrorCode::NotCoordinator));

    // Heartbeat.
    auto hLen = encodeHeartbeat(req_, 1, 602, "g", "m", 1);
    auto hOut = service_->handleHeartbeat(req_, hLen, clock_, out_, kBufSize);
    EXPECT_EQ(decodeHBErrorCode(out_, hOut),
              static_cast<int16_t>(pc::ErrorCode::NotCoordinator));

    // LeaveGroup.
    auto lgLen = encodeLeaveGroup(req_, 1, 603, "g", "m");
    auto lgOut = service_->handleLeaveGroup(req_, lgLen, clock_, out_, kBufSize);
    EXPECT_EQ(decodeLeaveErrorCode(out_, lgOut),
              static_cast<int16_t>(pc::ErrorCode::NotCoordinator));

    // OffsetCommit — non-empty entries → first result is NotCoordinator.
    auto ocLen = encodeOffsetCommit(req_, 1, 604, "g", "m", 1, kTestTopicId, 0, 10);
    auto ocOut = service_->handleOffsetCommit(req_, ocLen, clock_, out_, kBufSize);
    ASSERT_GT(ocOut, 0u);
    auto ocResp = pc::OffsetCommitResponse::decode(out_, ocOut);
    ASSERT_TRUE(ocResp.has_value());
    ASSERT_FALSE(ocResp->results.empty());
    EXPECT_EQ(ocResp->results[0].errorCode,
              static_cast<int16_t>(pc::ErrorCode::NotCoordinator));

    // OffsetFetch — non-empty entries → first result is NotCoordinator.
    auto ofLen = encodeOffsetFetch(req_, 1, 605, "g", kTestTopicId, 0);
    auto ofOut = service_->handleOffsetFetch(req_, ofLen, clock_, out_, kBufSize);
    ASSERT_GT(ofOut, 0u);
    auto ofResp = pc::OffsetFetchResponse::decode(out_, ofOut);
    ASSERT_TRUE(ofResp.has_value());
    ASSERT_FALSE(ofResp->results.empty());
    EXPECT_EQ(ofResp->results[0].errorCode,
              static_cast<int16_t>(pc::ErrorCode::NotCoordinator));

    // OffsetFetch fetch-all (empty entries) — should also return NotCoordinator
    // via the sentinel result (Minor-5 fix).
    auto fallLen = encodeOffsetFetchAll(req_, 1, 606, "g");
    auto fallOut = service_->handleOffsetFetch(req_, fallLen, clock_, out_, kBufSize);
    ASSERT_GT(fallOut, 0u);
    auto fallResp = pc::OffsetFetchResponse::decode(out_, fallOut);
    ASSERT_TRUE(fallResp.has_value());
    ASSERT_FALSE(fallResp->results.empty()) << "Empty-entries NotCoordinator must return sentinel result";
    EXPECT_EQ(fallResp->results[0].errorCode,
              static_cast<int16_t>(pc::ErrorCode::NotCoordinator));

    // OffsetCommit empty entries — should also return NotCoordinator sentinel.
    {
        pc::OffsetCommit emptyCommit;
        emptyCommit.sessionId     = 1;
        emptyCommit.correlationId = 607;
        emptyCommit.generationId  = 1;
        const std::string g = "g";
        emptyCommit.groupId = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(g.data()), g.size());
        const std::string m = "m";
        emptyCommit.memberId = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(m.data()), m.size());
        auto ecLen = emptyCommit.encode(req_, std::span<const pc::OffsetCommitEntry>());
        auto ecOut = service_->handleOffsetCommit(req_, ecLen, clock_, out_, kBufSize);
        ASSERT_GT(ecOut, 0u);
        auto ecResp = pc::OffsetCommitResponse::decode(out_, ecOut);
        ASSERT_TRUE(ecResp.has_value());
        ASSERT_FALSE(ecResp->results.empty()) << "Empty-entries NotCoordinator must return sentinel result";
        EXPECT_EQ(ecResp->results[0].errorCode,
                  static_cast<int16_t>(pc::ErrorCode::NotCoordinator));
    }
}

// 10. OffsetCommit correct-gen persists; OffsetFetch returns the REAL offset.
//     Uses the real TopicRegistry so the commit path goes through
//     ReplicatedOffsetStore (not rejected as UnknownTopic).
TEST_F(CG3ServiceTest, OffsetCommitAndFetchRoundTrip) {
    std::string groupId = "g-offset";

    // Join + re-join → Stable so the generation is correct.
    auto jLen = encodeJoinGroup(req_, 1, 700, groupId, "", 5000);
    auto jOut = service_->handleJoinGroup(req_, jLen, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    ASSERT_GT(jOut, 0u);
    auto jResp = pc::JoinGroupResponse::decode(out_, jOut);
    ASSERT_TRUE(jResp.has_value());
    const std::string memberId(
        reinterpret_cast<const char*>(jResp->memberId.data()),
        jResp->memberId.size());
    const int32_t gen = jResp->generationId;

    // OffsetCommit: topicId=kTestTopicId (registered), partition=0, offset=42.
    // With a real registry, this resolves "test-topic" and calls
    // ReplicatedOffsetStore::commit → persists.
    auto ocLen = encodeOffsetCommit(req_, 1, 701, groupId, memberId, gen,
                                    kTestTopicId, 0, 42, "meta");
    auto ocOut = service_->handleOffsetCommit(req_, ocLen, clock_, out_, kBufSize);
    ASSERT_GT(ocOut, 0u);

    auto ocResp = pc::OffsetCommitResponse::decode(out_, ocOut);
    ASSERT_TRUE(ocResp.has_value());
    // minIsr=1 single-node → commit succeeds → OK (not UnknownTopic or MinIsrViolation).
    ASSERT_FALSE(ocResp->results.empty());
    EXPECT_EQ(ocResp->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::OK));

    // OffsetFetch should return the committed offset = 42 (not sentinel -1).
    auto ofLen = encodeOffsetFetch(req_, 1, 702, groupId, kTestTopicId, 0);
    auto ofOut = service_->handleOffsetFetch(req_, ofLen, clock_, out_, kBufSize);
    ASSERT_GT(ofOut, 0u);
    auto ofResp = pc::OffsetFetchResponse::decode(out_, ofOut);
    ASSERT_TRUE(ofResp.has_value());
    ASSERT_FALSE(ofResp->results.empty());
    EXPECT_EQ(ofResp->results[0].offset, int64_t{42});
    EXPECT_EQ(ofResp->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
}

// E2 final fix wave (C1): a TRANSIENT partition's committed offset is a data
// position of one ring lineage.  The cluster restarts (here: the coordinator is
// rebuilt from the replicated log with a NEW current lineage) and the ring is
// empty, refilling from offset 0: OffsetFetch must answer "no committed offset"
// (-1) — the member then applies its reset policy — instead of the old 5000
// (the member would drop the first 5000 post-restart records as duplicates).
// A partition with no lineage (a disk partition: provider 0) keeps its offset.
// RED: the restarted coordinator answered 5000.
namespace {
uint64_t g_lineage = 0;
uint64_t testLineage(void*, int32_t /*topicId*/, int32_t partition) noexcept {
    return partition == 0 ? g_lineage : 0;   // partition 0 transient, partition 1 "disk"
}
}  // namespace

TEST_F(CG3ServiceTest, TransientCommittedOffsetOfAnEndedLineageIsAnsweredAsNoneRegression) {
    const std::string groupId = "g-transient";
    g_lineage = 0x1111;
    service_->setTransientLineage(&testLineage, nullptr);
    auto jLen = encodeJoinGroup(req_, 1, 950, groupId, "", 5000);
    auto jOut = service_->handleJoinGroup(req_, jLen, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    auto jResp = pc::JoinGroupResponse::decode(out_, jOut);
    ASSERT_TRUE(jResp.has_value());
    const std::string memberId(reinterpret_cast<const char*>(jResp->memberId.data()), jResp->memberId.size());
    for (int32_t p : {0, 1}) {
        auto ocLen = encodeOffsetCommit(req_, 1, 951 + p, groupId, memberId, jResp->generationId, kTestTopicId,
                                        p, p == 0 ? 5000 : 70, "m");
        auto ocOut = service_->handleOffsetCommit(req_, ocLen, clock_, out_, kBufSize);
        auto ocResp = pc::OffsetCommitResponse::decode(out_, ocOut);
        ASSERT_TRUE(ocResp.has_value());
        ASSERT_EQ(ocResp->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    }
    ASSERT_TRUE(pumpHW(2));
    auto fetch = [&](GroupCoordinationService& svc, int32_t p) {
        auto ofLen = encodeOffsetFetch(req_, 1, 960, groupId, kTestTopicId, p);
        auto ofOut = svc.handleOffsetFetch(req_, ofLen, clock_, out_, kBufSize);
        auto r = pc::OffsetFetchResponse::decode(out_, ofOut);
        EXPECT_TRUE(r.has_value());
        return r.has_value() && !r->results.empty() ? r->results[0].offset : int64_t{-99};
    };
    EXPECT_EQ(fetch(*service_, 0), 5000) << "the live lineage's offset is answered";

    // A full restart: the coordinator replays the log; the ring lineage is new.
    ReplicatedOffsetStore replayed(replica_.get());
    ASSERT_TRUE(replayed.rebuildFromLog());
    GroupCoordinationService restarted(&replayed, registry_.get(), [] { return true; }, [] { return NodeId{1}; });
    g_lineage = 0x2222;
    restarted.setTransientLineage(&testLineage, nullptr);
    restarted.doWork(clock_);   // round 4: loads the offsets (first duty pass)
    EXPECT_EQ(fetch(restarted, 0), -1)
        << "SILENT LOSS: an offset of the ring's previous life was answered after the restart";
    EXPECT_EQ(fetch(restarted, 1), 70) << "a partition without a lineage (disk) lost its committed offset";
    // The fetch-all form omits it (as if never committed).
    auto allLen = encodeOffsetFetchAll(req_, 1, 961, groupId);
    auto allOut = restarted.handleOffsetFetch(req_, allLen, clock_, out_, kBufSize);
    auto all = pc::OffsetFetchResponse::decode(out_, allOut);
    ASSERT_TRUE(all.has_value());
    for (const auto& r : all->results)
        EXPECT_FALSE(r.topicId == kTestTopicId && r.partition == 0) << "fetch-all answered the ended lineage's offset";
    EXPECT_EQ(restarted.staleLineageOffsets(), 2u);
}

// E2 final fix wave (C1 multi-node, option b): while the partition's CURRENT
// leadership has not confirmed its data lineage (right after a full-cluster
// restart, or a leader move, until the controller commits the lineage), the
// coordinator can answer neither the committed offset (it may be of the old
// lineage) nor "none" (it may be valid): OffsetFetch and OffsetCommit answer
// that entry NotLeader — an existing retriable code the client retries with
// its bounded budget.  Never a guess, never silence.  Once confirmed: the
// stamp rules as before.  RED: the pending window answered the old offset.
namespace {
uint64_t g_pendingLineage = GroupCoordinationService::kLineagePending;
uint64_t pendingThenKnown(void*, int32_t, int32_t partition) noexcept {
    return partition == 0 ? g_pendingLineage : 0;
}
}  // namespace

TEST_F(CG3ServiceTest, TransientOffsetsAreAnsweredRetriablyWhileTheLineageIsPendingRegression) {
    const std::string groupId = "g-pending";
    g_pendingLineage = 0x1111;
    service_->setTransientLineage(&pendingThenKnown, nullptr);
    auto jLen = encodeJoinGroup(req_, 1, 970, groupId, "", 5000);
    auto jOut = service_->handleJoinGroup(req_, jLen, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    auto jResp = pc::JoinGroupResponse::decode(out_, jOut);
    ASSERT_TRUE(jResp.has_value());
    const std::string memberId(reinterpret_cast<const char*>(jResp->memberId.data()), jResp->memberId.size());
    auto commit = [&](int64_t off) {
        auto ocLen = encodeOffsetCommit(req_, 1, 971, groupId, memberId, jResp->generationId, kTestTopicId, 0, off, "m");
        auto ocOut = service_->handleOffsetCommit(req_, ocLen, clock_, out_, kBufSize);
        auto ocResp = pc::OffsetCommitResponse::decode(out_, ocOut);
        EXPECT_TRUE(ocResp.has_value());
        return ocResp.has_value() ? ocResp->results[0].errorCode : int16_t{-99};
    };
    auto fetch = [&](int16_t& code) {
        auto ofLen = encodeOffsetFetch(req_, 1, 972, groupId, kTestTopicId, 0);
        auto ofOut = service_->handleOffsetFetch(req_, ofLen, clock_, out_, kBufSize);
        auto r = pc::OffsetFetchResponse::decode(out_, ofOut);
        EXPECT_TRUE(r.has_value());
        code = r.has_value() ? r->results[0].errorCode : int16_t{-99};
        return r.has_value() ? r->results[0].offset : int64_t{-99};
    };
    ASSERT_EQ(commit(5000), static_cast<int16_t>(pc::ErrorCode::OK));
    ASSERT_TRUE(pumpHW(1));
    int16_t code = 0;
    EXPECT_EQ(fetch(code), 5000);

    g_pendingLineage = GroupCoordinationService::kLineagePending;   // a full restart: not confirmed yet
    const int64_t during = fetch(code);
    EXPECT_EQ(code, static_cast<int16_t>(pc::ErrorCode::NotLeader))
        << "the pending window was answered " << during << " (a guess: the offset may belong to data that did not survive)";
    EXPECT_EQ(during, -1);
    EXPECT_EQ(commit(9), static_cast<int16_t>(pc::ErrorCode::NotLeader)) << "a commit was stamped while the lineage was unknown";
    EXPECT_GE(service_->pendingLineageAnswers(), 2u);

    g_pendingLineage = 0x2222;   // the new leadership MINTED a new lineage
    EXPECT_EQ(fetch(code), -1) << "the old lineage's offset was answered after a re-mint";
    EXPECT_EQ(code, static_cast<int16_t>(pc::ErrorCode::OK));
    // Residual round 5 (R1): this member's generation predates the mint — its
    // position may come from the old data: the commit is fenced and the group
    // rebalanced; after re-joining, the commit is accepted.
    EXPECT_EQ(commit(3), static_cast<int16_t>(pc::ErrorCode::IllegalGeneration));
    auto rj = encodeJoinGroup(req_, 1, 973, groupId, memberId, 5000);
    auto rjOut = service_->handleJoinGroup(req_, rj, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    auto rjResp = pc::JoinGroupResponse::decode(out_, rjOut);
    ASSERT_TRUE(rjResp.has_value());
    ASSERT_GT(rjResp->generationId, jResp->generationId);
    auto oc = encodeOffsetCommit(req_, 1, 974, groupId, memberId, rjResp->generationId, kTestTopicId, 0, 3, "m");
    auto ocOut = service_->handleOffsetCommit(req_, oc, clock_, out_, kBufSize);
    auto ocResp = pc::OffsetCommitResponse::decode(out_, ocOut);
    ASSERT_TRUE(ocResp.has_value());
    ASSERT_EQ(ocResp->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    ASSERT_TRUE(pumpHW(2));
    EXPECT_EQ(fetch(code), 3);
}

// 11. OffsetCommit wrong gen → IllegalGeneration.
TEST_F(CG3ServiceTest, OffsetCommitWrongGenerationReturnsIllegalGeneration) {
    std::string groupId = "g-illegalgen";

    auto jLen = encodeJoinGroup(req_, 1, 800, groupId, "", 5000);
    auto jOut = service_->handleJoinGroup(req_, jLen, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    ASSERT_GT(jOut, 0u);
    auto jResp = pc::JoinGroupResponse::decode(out_, jOut);
    ASSERT_TRUE(jResp.has_value());
    const std::string memberId(
        reinterpret_cast<const char*>(jResp->memberId.data()),
        jResp->memberId.size());

    // Wrong generation = current + 999.
    const int32_t wrongGen = jResp->generationId + 999;
    auto ocLen = encodeOffsetCommit(req_, 1, 801, groupId, memberId, wrongGen,
                                    kTestTopicId, 0, 99);
    auto ocOut = service_->handleOffsetCommit(req_, ocLen, clock_, out_, kBufSize);
    ASSERT_GT(ocOut, 0u);

    auto ocResp = pc::OffsetCommitResponse::decode(out_, ocOut);
    ASSERT_TRUE(ocResp.has_value());
    ASSERT_FALSE(ocResp->results.empty());
    EXPECT_EQ(ocResp->results[0].errorCode,
              static_cast<int16_t>(pc::ErrorCode::IllegalGeneration));
}

// 12. OffsetFetch uncommitted → sentinel (-1) with OK.
TEST_F(CG3ServiceTest, OffsetFetchUncommittedReturnsSentinel) {
    // Registered topic, but nothing committed.
    auto ofLen = encodeOffsetFetch(req_, 1, 900, "never-committed-group", kTestTopicId, 0);
    auto ofOut = service_->handleOffsetFetch(req_, ofLen, clock_, out_, kBufSize);
    ASSERT_GT(ofOut, 0u);

    auto ofResp = pc::OffsetFetchResponse::decode(out_, ofOut);
    ASSERT_TRUE(ofResp.has_value());
    ASSERT_FALSE(ofResp->results.empty());
    // Known topic, not committed → offset = -1 (no-offset sentinel), errorCode = OK.
    EXPECT_EQ(ofResp->results[0].offset, int64_t{-1});
    EXPECT_EQ(ofResp->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
}

// 13. OffsetCommit minIsr-gated commit → MinIsrViolation (genuine path).
//     Uses a real TopicRegistry so the topic resolves; the minIsr=2 replica
//     with only 1 node in ISR gates the commit.
TEST_F(CG3ServiceTest, OffsetCommitMinIsrViolationWhenReplicaGated) {
    // Create a service backed by a minIsr=2 replica (but only 1 node → gated).
    TempDir dir2("cg3_minIsr");
    TempDir topicDir2("cg3_minIsr_topics");
    rune::storage::Log log2(dir2.path);
    NullTransport nt2;
    PartitionReplica replica2(
        makeSingleNodeConfig(1, /*minIsr=*/2),  // minIsr=2, only 1 node → gated
        Role::Leader, /*epoch=*/1,
        std::vector<NodeId>{1},
        std::vector<NodeId>{1},   // ISR = {1} — below minIsr=2
        &log2,
        &nt2);

    ReplicatedOffsetStore store2(&replica2);

    // Real TopicRegistry — same topic as the main fixture.
    std::vector<TopicSpec> specs{
        makeTopicSpec(std::string(kTestTopic), 2, kTestTopicId)
    };
    rune::gateway::TopicRegistry registry2(specs, topicDir2.path);

    bool leader = true;
    GroupCoordinationService svc2(
        &store2, &registry2,
        [&leader] { return leader; },
        [] { return NodeId{1}; });
    svc2.doWork(0);   // round 4: loads the (empty) offsets log

    std::string groupId = "g-minisr";
    uint8_t req2[64 * 1024]{};
    uint8_t out2[64 * 1024]{};

    auto jLen = encodeJoinGroup(req2, 1, 1000, groupId, "", 5000);
    auto jOut = svc2.handleJoinGroup(req2, jLen, rune::protocol::client::kClientVersion, 0, out2, sizeof(out2));
    ASSERT_GT(jOut, 0u);
    auto jResp = pc::JoinGroupResponse::decode(out2, jOut);
    ASSERT_TRUE(jResp.has_value());

    const std::string memberId(
        reinterpret_cast<const char*>(jResp->memberId.data()),
        jResp->memberId.size());
    const int32_t gen = jResp->generationId;

    // OffsetCommit with the real topicId → topic resolves → commit() called →
    // minIsr gate fires (ISR size 1 < minIsr 2) → MinIsrViolation.
    auto ocLen = encodeOffsetCommit(req2, 1, 1001, groupId, memberId, gen,
                                    kTestTopicId, 0, 77);
    auto ocOut = svc2.handleOffsetCommit(req2, ocLen, 0, out2, sizeof(out2));
    ASSERT_GT(ocOut, 0u);

    auto ocResp = pc::OffsetCommitResponse::decode(out2, ocOut);
    ASSERT_TRUE(ocResp.has_value());
    ASSERT_FALSE(ocResp->results.empty());
    // Now the topic resolves → commit() is reached → returns false (minIsr gated)
    // → MinIsrViolation (not UnknownTopic).
    EXPECT_EQ(ocResp->results[0].errorCode,
              static_cast<int16_t>(pc::ErrorCode::MinIsrViolation));
}

// 14. OffsetFetch fetch-all returns all committed (group,tp) pairs.
//     Commits N distinct (topicId,partition) pairs, then fetch-all and verify
//     each committed offset appears in the result.
TEST_F(CG3ServiceTest, OffsetFetchFetchAllReturnAllCommitted) {
    std::string groupId = "g-fetchall";

    auto jLen = encodeJoinGroup(req_, 1, 1100, groupId, "", 5000);
    auto jOut = service_->handleJoinGroup(req_, jLen, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    ASSERT_TRUE(pc::JoinGroupResponse::decode(out_, jOut).has_value());
    auto jResp = pc::JoinGroupResponse::decode(out_, jOut);
    ASSERT_TRUE(jResp.has_value());
    const std::string memberId(
        reinterpret_cast<const char*>(jResp->memberId.data()),
        jResp->memberId.size());
    const int32_t gen = jResp->generationId;

    // Commit partition 0 and partition 1 of the test topic.
    auto oc0Len = encodeOffsetCommit(req_, 1, 1101, groupId, memberId, gen,
                                     kTestTopicId, 0, 100);
    auto oc0Out = service_->handleOffsetCommit(req_, oc0Len, clock_, out_, kBufSize);
    ASSERT_GT(oc0Out, 0u);
    {
        auto r = pc::OffsetCommitResponse::decode(out_, oc0Out);
        ASSERT_TRUE(r.has_value());
        ASSERT_FALSE(r->results.empty());
        EXPECT_EQ(r->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    }

    auto oc1Len = encodeOffsetCommit(req_, 1, 1102, groupId, memberId, gen,
                                     kTestTopicId, 1, 200);
    auto oc1Out = service_->handleOffsetCommit(req_, oc1Len, clock_, out_, kBufSize);
    ASSERT_GT(oc1Out, 0u);
    {
        auto r = pc::OffsetCommitResponse::decode(out_, oc1Out);
        ASSERT_TRUE(r.has_value());
        ASSERT_FALSE(r->results.empty());
        EXPECT_EQ(r->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    }

    // Fetch-all: empty entries → returns all committed pairs.
    auto fallLen = encodeOffsetFetchAll(req_, 1, 1103, groupId);
    auto fallOut = service_->handleOffsetFetch(req_, fallLen, clock_, out_, kBufSize);
    ASSERT_GT(fallOut, 0u);

    auto fallResp = pc::OffsetFetchResponse::decode(out_, fallOut);
    ASSERT_TRUE(fallResp.has_value());
    // Should have 2 results (partition 0 and partition 1).
    ASSERT_EQ(fallResp->results.size(), std::size_t{2});

    // Collect returned (topicId, partition, offset) — order not guaranteed.
    std::map<std::pair<int32_t,int32_t>, int64_t> returned;
    for (std::size_t i = 0; i < fallResp->results.size(); ++i) {
        const auto& r = fallResp->results[i];
        returned[{r.topicId, r.partition}] = r.offset;
        EXPECT_EQ(r.errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    }

    EXPECT_EQ((returned[{kTestTopicId, 0}]), int64_t{100});
    EXPECT_EQ((returned[{kTestTopicId, 1}]), int64_t{200});
}

// 15. Defensive OffsetCommit iteration: overstated count / truncated frame.
//     Builds a raw OffsetCommit frame claiming count=5 but only has bytes for
//     1 entry.  The handler must iterate only the truly-present entries (the
//     view's iterator stops when bytes run out), no over-read, no crash.
TEST_F(CG3ServiceTest, OffsetCommitDefensiveIterationTruncatedFrame) {
    std::string groupId = "g-truncated";

    // Join first so the group exists and gen is valid.
    auto jLen = encodeJoinGroup(req_, 1, 1200, groupId, "", 5000);
    auto jOut = service_->handleJoinGroup(req_, jLen, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    ASSERT_GT(jOut, 0u);
    auto jResp = pc::JoinGroupResponse::decode(out_, jOut);
    ASSERT_TRUE(jResp.has_value());
    const std::string memberId(
        reinterpret_cast<const char*>(jResp->memberId.data()),
        jResp->memberId.size());
    const int32_t gen = jResp->generationId;

    // Build a well-formed single-entry OffsetCommit frame.
    auto wellFormedLen = encodeOffsetCommit(req_, 1, 1201, groupId, memberId, gen,
                                            kTestTopicId, 0, 55);
    ASSERT_GT(wellFormedLen, 0u);

    // Find the count field in the frame and overstate it to 5.
    // OffsetCommit frame layout: MessageHeader(24) + groupId_len(2) + groupId_bytes
    // + memberId_len(2) + memberId_bytes + generationId(4) + count(2) + entries...
    // Locate the count by searching for the encoded count=1 (little-endian 0x01 0x00).
    // We know the total frame size, so the count field is 2 bytes before the first entry.
    // Each entry is: topicId(4) + partition(4) + offset(8) + metadata_len(2) + metadata = 18+
    // Rather than parsing, truncate the frame by 10 bytes (removing most of the entry
    // bytes) and then pass the truncated frame.  The decoder will see a short frame
    // for most entries and the iterator will stop early.
    // We pass wellFormedLen - 10 bytes → truncated mid-entry.
    // The handler must not crash.
    const std::size_t truncatedLen = (wellFormedLen > 10u) ? wellFormedLen - 10u : wellFormedLen;
    auto truncOut = service_->handleOffsetCommit(req_, truncatedLen, clock_, out_, kBufSize);
    // The handler either: (a) decode fails → returns 0, or (b) iterates
    // only the truly-present entries.  Either is correct; no crash is the key.
    // If decode fails: truncOut == 0 (that's valid defensive behaviour).
    // If decode succeeds with 0 or 1 entry: truncOut > 0.
    // We just assert it didn't crash (any return value is acceptable).
    (void)truncOut;

    // Now corrupt the count field explicitly: build a well-formed 1-entry frame,
    // then patch the count to 99 (far more than the bytes present).
    auto patchedLen = encodeOffsetCommit(req_, 1, 1202, groupId, memberId, gen,
                                         kTestTopicId, 0, 55);
    ASSERT_GT(patchedLen, 0u);

    // Find and overwrite the entry count field.  The count field is the first
    // uint16 of the entries array.  Search from the end of the fixed header
    // to locate it: after MessageHeader(24)+groupId_len(2)+groupId+memberId_len(2)
    // +memberId+generationId(4) the count(2) follows.  For our known-short strings
    // we can brute-force search for the pattern [0x01, 0x00] (count=1 LE) that
    // precedes the entry bytes.  Since topicId starts at that position this is
    // unambiguous: after the scan start the count=1 is the ONLY [0x01,0x00] pair
    // immediately before an entry block starting with topicId=0 [0x00,0x00,0x00,0x00].
    // To keep it simple: the exact byte offset = 24 + 2 + groupId.size() +
    //                                            2 + memberId.size() + 4
    const std::size_t countOffset =
        24u                      // MessageHeader
        + 2u + groupId.size()    // groupId length + bytes
        + 2u + memberId.size()   // memberId length + bytes
        + 4u;                    // generationId
    ASSERT_LT(countOffset + 2u, patchedLen);

    // Overstate count to 99.
    req_[countOffset]     = 99;
    req_[countOffset + 1] = 0;

    auto patchedOut = service_->handleOffsetCommit(req_, patchedLen, clock_, out_, kBufSize);
    // The decoder must stop when bytes run out.  No crash.  Either returns 0
    // (decode failed) or returns a valid response with <= actual entries.
    if (patchedOut > 0u) {
        auto pResp = pc::OffsetCommitResponse::decode(out_, patchedOut);
        ASSERT_TRUE(pResp.has_value());
        // The handler can only have processed the 1 entry worth of bytes present.
        // It must not have produced 99 results (no over-read).
        EXPECT_LE(pResp->results.size(), std::size_t{2})
            << "Defensive iteration must stop at frame boundary; got "
            << pResp->results.size() << " results from a 1-entry frame with count=99";
    }
    // Either decode-fail (0) or bounded iteration: both are correct.
}

// Task 11 fix 2 (B1): once the runtime pushes leadership as a value, the
// per-cycle doWork (worker 0's pass under fused placement) never calls the
// isLeader std::function.
TEST(GroupCoordinationServiceRegression, PushedLeadershipNeverCallsTheLeaderFunction) {
    int calls = 0;
    GroupCoordinationService svc(nullptr, nullptr, [&calls] { ++calls; return false; });
    svc.setLocalLeadership(false);
    for (int i = 0; i < 100; ++i) (void)svc.doWork(i);
    EXPECT_FALSE(svc.isLocalCoordinator());
    EXPECT_EQ(calls, 0) << "isLeaderFn_ called on the duty cycle after a leadership push";
}

// ---------------------------------------------------------------------------
// Review: TCS/GCS kept stale in-memory state when the coordinator partition's
// log content changed under them (the old reconcile appended foreign batches;
// any truncation does the same).  The offset index is valid only for the
// leadership AND the log content it was rebuilt from: a log-generation /
// leadership change re-arms the rebuild, so a truncated-away commit is never
// served.
// ---------------------------------------------------------------------------
TEST_F(CG3ServiceTest, OffsetIndexRebuiltWhenTheLogContentChangesRegression) {
    ASSERT_TRUE(driveUntil([&] { return true; }, 1));
    service_->doWork(clock_ += kStep);
    ASSERT_TRUE(store_->commit("g-stale", tp("t", 0), 7, "", 1));
    ASSERT_TRUE(pumpHW(replica_->logEndOffset()));
    service_->doWork(clock_ += kStep);
    ASSERT_TRUE(store_->fetch("g-stale", tp("t", 0)).has_value());

    // Between two duty passes: demotion, a KIP-279 truncation that removes the
    // commit record, re-promotion (a lone leader completes at once).
    replica_->becomeFollower(2, 9);
    FetchResponseMsg div{};
    div.leaderEpoch = 2; div.partition = 0; div.errorCode = rune::replication::kErrDivergingEpoch;
    { static rune::replication::ErrorPayloadBuf epb_; div.records = epb_.set(replica_->lastFetchedEpoch(), 0); }
    replica_->applyReplicated(div);
    ASSERT_EQ(replica_->logEndOffset(), 0) << "precondition: the commit record is gone";
    replica_->becomeLeader(3, {kLeader});
    ASSERT_TRUE(replica_->acceptsProduce());
    ASSERT_TRUE(store_->commit("g-new", tp("t", 1), 3, "", 2));
    ASSERT_TRUE(pumpHW(replica_->logEndOffset()));

    service_->doWork(clock_ += kStep);
    EXPECT_FALSE(store_->fetch("g-stale", tp("t", 0)).has_value())
        << "a commit truncated out of the log is still served from the stale index";
    EXPECT_TRUE(store_->fetch("g-new", tp("t", 1)).has_value());
}

// ===========================================================================
// No-hang wave (R2): every group handler answers a request whose body does not
// decode with its own response type + InvalidRequest on the header's
// correlationId (it used to return 0 — dropped, the member waited out its
// timeout).
// ===========================================================================
namespace {
std::size_t groupHeaderOnly(uint8_t* buf, pc::TemplateId tid, int64_t corr) {
    pc::MessageHeader h;
    h.blockLength = 32; h.templateId = static_cast<uint16_t>(tid);
    h.sessionId = 5; h.correlationId = corr;
    h.encode(buf);
    return pc::MessageHeader::ENCODED_SIZE;
}
} // namespace

TEST_F(CG3ServiceTest, MalformedGroupRequestsAreAnsweredInvalidRequestRegression) {
    constexpr auto kInv = static_cast<int16_t>(pc::ErrorCode::InvalidRequest);
    std::size_t n = groupHeaderOnly(req_, pc::TemplateId::JoinGroup, 1);
    std::size_t o = service_->handleJoinGroup(req_, n, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    auto j = pc::JoinGroupResponse::decode(out_, o);
    ASSERT_TRUE(j.has_value()); EXPECT_EQ(j->correlationId, 1); EXPECT_EQ(j->errorCode, kInv);

    n = groupHeaderOnly(req_, pc::TemplateId::SyncGroup, 2);
    o = service_->handleSyncGroup(req_, n, clock_, out_, kBufSize);
    auto s = pc::SyncGroupResponse::decode(out_, o);
    ASSERT_TRUE(s.has_value()); EXPECT_EQ(s->correlationId, 2); EXPECT_EQ(s->errorCode, kInv);

    n = groupHeaderOnly(req_, pc::TemplateId::Heartbeat, 3);
    o = service_->handleHeartbeat(req_, n, clock_, out_, kBufSize);
    auto hb = pc::HeartbeatResponse::decode(out_, o);
    ASSERT_TRUE(hb.has_value()); EXPECT_EQ(hb->correlationId, 3); EXPECT_EQ(hb->errorCode, kInv);

    n = groupHeaderOnly(req_, pc::TemplateId::LeaveGroup, 4);
    o = service_->handleLeaveGroup(req_, n, clock_, out_, kBufSize);
    auto l = pc::LeaveGroupResponse::decode(out_, o);
    ASSERT_TRUE(l.has_value()); EXPECT_EQ(l->correlationId, 4); EXPECT_EQ(l->errorCode, kInv);

    n = groupHeaderOnly(req_, pc::TemplateId::OffsetCommit, 5);
    o = service_->handleOffsetCommit(req_, n, clock_, out_, kBufSize);
    auto oc = pc::OffsetCommitResponse::decode(out_, o);
    ASSERT_TRUE(oc.has_value()); EXPECT_EQ(oc->correlationId, 5);
    ASSERT_EQ(oc->results.size(), 1u); EXPECT_EQ(oc->results[0].errorCode, kInv);

    n = groupHeaderOnly(req_, pc::TemplateId::OffsetFetch, 6);
    o = service_->handleOffsetFetch(req_, n, clock_, out_, kBufSize);
    auto of = pc::OffsetFetchResponse::decode(out_, o);
    ASSERT_TRUE(of.has_value()); EXPECT_EQ(of->correlationId, 6);
    ASSERT_EQ(of->results.size(), 1u); EXPECT_EQ(of->results[0].errorCode, kInv);
}

// No-hang fix round 3 (fix-on-sight): an unknown coordinator result maps to
// Internal — it used to map to OK (an error silently answered as success).
TEST(GroupErrorMappingRegression, UnknownCoordinatorResultIsInternalNotOkRegression) {
    using rune::node::joinErrorToProto;
    using rune::node::heartbeatErrorToProto;
    using rune::node::syncErrorToProto;
    constexpr auto kOk  = static_cast<int16_t>(pc::ErrorCode::OK);
    constexpr auto kInt = static_cast<int16_t>(pc::ErrorCode::Internal);
    EXPECT_EQ(joinErrorToProto(0), kOk);
    EXPECT_EQ(joinErrorToProto(rune::coordination::JoinResult::kErrDead), static_cast<int16_t>(pc::ErrorCode::GroupDead));
    EXPECT_EQ(heartbeatErrorToProto(rune::coordination::HeartbeatResult::kOk), kOk);
    EXPECT_EQ(syncErrorToProto(rune::coordination::SyncResult::kOk), kOk);
    for (const int unknown : {77, -1, 1000}) {
        EXPECT_EQ(joinErrorToProto(unknown), kInt) << unknown;
        EXPECT_EQ(heartbeatErrorToProto(unknown), kInt) << unknown;
        EXPECT_EQ(syncErrorToProto(unknown), kInt) << unknown;
    }
}

// No-hang fix round 4 (item 8, ruling): the gateway answers a pre-v5 client
// UnknownMember instead of an Internal / InvalidRequest it would ignore — and
// evicts the members on that session so the answer is TRUE: a later Heartbeat
// of that member is UnknownMember (the v4 client re-joins on it).  Members on
// other sessions are untouched.
TEST_F(CG3ServiceTest, EvictSessionMembersMakesUnknownMemberTrueRegression) {
    const std::string groupId = "g-evict";
    auto joinAs = [&](int64_t sid, int64_t corr) {
        const auto n = encodeJoinGroup(req_, sid, corr, groupId, "", 5000);
        const auto o = service_->handleJoinGroup(req_, n, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
        const auto r = pc::JoinGroupResponse::decode(out_, o);
        EXPECT_TRUE(r.has_value());
        return std::string(reinterpret_cast<const char*>(r->memberId.data()), r->memberId.size());
    };
    const std::string oldMember = joinAs(/*sid=*/7, 900);
    const std::string newMember = joinAs(/*sid=*/8, 901);
    ASSERT_EQ(service_->coordinator(groupId).memberCount(), 2u);

    service_->evictSessionMembers(/*sessionId=*/7, clock_);
    EXPECT_EQ(service_->coordinator(groupId).memberCount(), 1u) << "the old client's member was not evicted";

    const int32_t gen = service_->coordinator(groupId).generationId();
    auto hb = encodeHeartbeat(req_, 7, 902, groupId, oldMember, gen);
    auto o = service_->handleHeartbeat(req_, hb, clock_, out_, kBufSize);
    ASSERT_GT(o, 0u);
    EXPECT_EQ(decodeHBErrorCode(out_, o), static_cast<int16_t>(pc::ErrorCode::UnknownMember));
    hb = encodeHeartbeat(req_, 8, 903, groupId, newMember, gen);
    o = service_->handleHeartbeat(req_, hb, clock_, out_, kBufSize);
    ASSERT_GT(o, 0u);
    EXPECT_NE(decodeHBErrorCode(out_, o), static_cast<int16_t>(pc::ErrorCode::UnknownMember))
        << "a member on another session was evicted";
    service_->evictSessionMembers(/*sessionId=*/7, clock_);   // idempotent: nothing left to evict
    EXPECT_EQ(service_->coordinator(groupId).memberCount(), 1u);
}

// No-hang fix round 5 (N23): the pre-v5 eviction is SCOPED.  One (v4) session
// hosts members of group A and group B; an error in A evicts only A's member —
// B's member on the same session keeps heartbeating OK (no rebalance thrash of
// a healthy group).  The OffsetFetch variant (group only, no member) evicts
// this session's members of A only.  A member mapped to ANOTHER session is
// never evicted (no spoofing).  RED (round 4): evictSessionMembers took both.
TEST_F(CG3ServiceTest, OldClientEvictionIsScopedToTheRequestsGroupRegression) {
    auto joinAs = [&](const std::string& groupId, int64_t sid, int64_t corr) {
        const auto n = encodeJoinGroup(req_, sid, corr, groupId, "", 5000);
        const auto o = service_->handleJoinGroup(req_, n, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
        const auto r = pc::JoinGroupResponse::decode(out_, o);
        EXPECT_TRUE(r.has_value());
        return std::string(reinterpret_cast<const char*>(r->memberId.data()), r->memberId.size());
    };
    auto heartbeatCode = [&](const std::string& groupId, int64_t sid, const std::string& member, int64_t corr) {
        const int32_t gen = service_->coordinator(groupId).generationId();
        const auto n = encodeHeartbeat(req_, sid, corr, groupId, member, gen);
        const auto o = service_->handleHeartbeat(req_, n, clock_, out_, kBufSize);
        EXPECT_GT(o, 0u);
        return decodeHBErrorCode(out_, o);
    };
    const int64_t v4 = 7;
    const std::string a = joinAs("grp-A", v4, 910);
    const std::string b = joinAs("grp-B", v4, 911);
    const int16_t bBefore = heartbeatCode("grp-B", v4, b, 912);
    ASSERT_NE(bBefore, static_cast<int16_t>(pc::ErrorCode::UnknownMember));

    // Spoof guard: (grp-A, a) is not mapped to session 99.
    service_->evictMember("grp-A", a, /*sessionId=*/99, clock_);
    EXPECT_EQ(service_->coordinator("grp-A").memberCount(), 1u) << "another session's member was evicted";

    service_->evictMember("grp-A", a, v4, clock_);   // Heartbeat / SyncGroup Internal in A
    EXPECT_EQ(service_->coordinator("grp-A").memberCount(), 0u);
    EXPECT_EQ(heartbeatCode("grp-A", v4, a, 913), static_cast<int16_t>(pc::ErrorCode::UnknownMember));
    EXPECT_EQ(service_->coordinator("grp-B").memberCount(), 1u) << "group B's member was evicted for A's error";
    EXPECT_EQ(heartbeatCode("grp-B", v4, b, 914), bBefore) << "group B was disturbed by A's error";

    // OffsetFetch variant: the group only.
    const std::string a2 = joinAs("grp-A", v4, 915);
    ASSERT_EQ(service_->coordinator("grp-A").memberCount(), 1u);
    service_->evictMember("grp-A", std::string_view{}, v4, clock_);
    EXPECT_EQ(service_->coordinator("grp-A").memberCount(), 0u);
    EXPECT_EQ(service_->coordinator("grp-B").memberCount(), 1u);
    EXPECT_EQ(heartbeatCode("grp-B", v4, b, 916), bBefore);
    (void)a2;
}

// ===========================================================================
// E2 final fix wave, residual round 4: until this coordinator has LOADED the
// offsets of its __consumer_offsets partition (the whole log, for the current
// leadership), OffsetFetch answers the retriable NotLeader — never -1 / an
// older offset from an empty or partial index (a guess) — and OffsetCommit is
// refused NotLeader too (the load clears + replays the index: a commit taken
// before it would be wiped).  Disk and transient topics alike.
// ===========================================================================
namespace {
struct FetchAnswer { int16_t code; int64_t offset; };
FetchAnswer fetchOne(GroupCoordinationService& svc, uint8_t* req, uint8_t* out, std::size_t cap,
                     const std::string& group, int32_t topicId, int32_t p) {
    auto len = encodeOffsetFetch(req, 1, 990, group, topicId, p);
    auto n   = svc.handleOffsetFetch(req, len, 0, out, cap);
    auto r   = pc::OffsetFetchResponse::decode(out, n);
    if (!r.has_value() || r->results.empty()) return {int16_t{-99}, int64_t{-99}};
    return {r->results[0].errorCode, r->results[0].offset};
}
}  // namespace

// (a) The coordinator restarts (a new replica / store / service over the same
// on-disk __consumer_offsets log) and an OffsetFetch arrives BEFORE its first
// duty pass loaded the offsets: NotLeader, then — once loaded — the real
// offset; never -1.  A commit in that window is refused, and one after the
// load survives.  RED (no load gate): the first answer was -1 (OK).
TEST_F(CG3ServiceTest, OffsetFetchBeforeTheOffsetsAreLoadedIsRetriableNeverNoneRegression) {
    const std::string groupId = "g-reload";
    ASSERT_TRUE(store_->commit(groupId, tp(std::string(kTestTopic), 0), 42, "", 1));
    ASSERT_TRUE(pumpHW(replica_->logEndOffset()));
    ASSERT_EQ(fetchOne(*service_, req_, out_, kBufSize, groupId, kTestTopicId, 0).offset, 42);

    // Restart: everything above the log is rebuilt from scratch.
    service_.reset();
    store_.reset();
    replica_.reset();
    rune::storage::Log log2(dir.path);
    log2.recover();
    PartitionReplica rep2(makeSingleNodeConfig(kLeader, 1), Role::Leader, /*epoch=*/2,
                          std::vector<NodeId>{kLeader}, std::vector<NodeId>{kLeader}, &log2, transport_.get());
    ReplicatedOffsetStore store2(&rep2);
    GroupCoordinationService svc2(&store2, registry_.get(), [] { return true; }, [] { return NodeId{kLeader}; });

    // Before any duty pass: the request races the reload.
    const FetchAnswer first = fetchOne(svc2, req_, out_, kBufSize, groupId, kTestTopicId, 0);
    EXPECT_EQ(first.code, static_cast<int16_t>(pc::ErrorCode::NotLeader))
        << "answered " << first.offset << " before the offsets were loaded (a guess: the committed offset is 42)";
    EXPECT_FALSE(first.offset == -1 && first.code == static_cast<int16_t>(pc::ErrorCode::OK))
        << "SILENT REWIND: 'no committed offset' answered before the reload";

    // The member's commit in that window: refused retriably (the load would wipe it).
    auto jLen = encodeJoinGroup(req_, 1, 991, groupId, "", 5000);
    auto jOut = svc2.handleJoinGroup(req_, jLen, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    auto jResp = pc::JoinGroupResponse::decode(out_, jOut);
    ASSERT_TRUE(jResp.has_value());
    const std::string memberId(reinterpret_cast<const char*>(jResp->memberId.data()), jResp->memberId.size());
    auto commit = [&](int64_t off) {
        auto l = encodeOffsetCommit(req_, 1, 992, groupId, memberId, jResp->generationId, kTestTopicId, 1, off, "");
        auto n = svc2.handleOffsetCommit(req_, l, clock_, out_, kBufSize);
        auto r = pc::OffsetCommitResponse::decode(out_, n);
        return r.has_value() && !r->results.empty() ? r->results[0].errorCode : int16_t{-99};
    };
    EXPECT_EQ(commit(7), static_cast<int16_t>(pc::ErrorCode::NotLeader)) << "a commit was taken before the load";

    // Duty passes load the offsets; every answer meanwhile is NotLeader or 42.
    FetchAnswer a{};
    for (int i = 0; i < 200; ++i) {
        clock_ += kStep;
        rep2.doWork(clock_);
        svc2.doWork(clock_);
        a = fetchOne(svc2, req_, out_, kBufSize, groupId, kTestTopicId, 0);
        if (a.code != static_cast<int16_t>(pc::ErrorCode::NotLeader)) break;
    }
    EXPECT_EQ(a.code, static_cast<int16_t>(pc::ErrorCode::OK)) << "never loaded";
    EXPECT_EQ(a.offset, 42) << "after the load the real committed offset must be answered";
    EXPECT_GE(svc2.offsetsNotLoadedAnswersForTest(), 2u);

    ASSERT_EQ(commit(9), static_cast<int16_t>(pc::ErrorCode::OK));
    clock_ += kStep;
    rep2.doWork(clock_);
    svc2.doWork(clock_);
    EXPECT_EQ(fetchOne(svc2, req_, out_, kBufSize, groupId, kTestTopicId, 1).offset, 9)
        << "a commit after the load was lost";
}

// (b) A PARTIAL log: the leader's high-watermark is below its log end (a
// follower in the ISR has not caught up after a restart).  Loading the prefix
// would answer an OLDER offset (10) for a group whose newest commit (30) sits
// above the HW.  The load waits for the whole log: NotLeader until then, then
// 30.  RED (prefix load): 10 was answered.
TEST_F(CG3ServiceTest, OffsetsAreNotLoadedFromALogPrefixBelowTheLogEndRegression) {
    service_.reset();
    store_.reset();
    replica_.reset();
    TempDir d3("cg3_prefix");
    rune::storage::Log log3(d3.path);
    log3.recover();
    PartitionReplica rep3(makeSingleNodeConfig(kLeader, /*minIsr=*/1), Role::Leader, /*epoch=*/1,
                          std::vector<NodeId>{kLeader, 2}, std::vector<NodeId>{kLeader, 2}, &log3, transport_.get());
    ReplicatedOffsetStore store3(&rep3);
    const std::string groupId = "g-prefix";
    for (const int64_t off : {10, 20, 30}) ASSERT_TRUE(store3.commit(groupId, tp(std::string(kTestTopic), 0), off, "", 1));
    ASSERT_EQ(rep3.logEndOffset(), 3);
    auto followerAt = [&](int64_t fetchOffset) {
        rune::replication::FetchRequestMsg f{};
        f.leaderEpoch = 1; f.partition = rep3.partitionId(); f.followerId = 2;
        f.fetchOffset = fetchOffset; f.maxBytes = 1 << 20; f.topicId = rep3.topicId();
        clock_ += kStep;
        (void)rep3.onFetchRequest(f, clock_);
        rep3.doWork(clock_);
    };
    followerAt(1);   // the follower holds record 0 only: HW 1 < log end 3
    ASSERT_EQ(rep3.highWatermark(), 1) << "precondition: a partial log (HW below the log end)";

    GroupCoordinationService svc3(&store3, registry_.get(), [] { return true; }, [] { return NodeId{kLeader}; });
    for (int i = 0; i < 5; ++i) {
        clock_ += kStep;
        svc3.doWork(clock_);
        const FetchAnswer a = fetchOne(svc3, req_, out_, kBufSize, groupId, kTestTopicId, 0);
        EXPECT_EQ(a.code, static_cast<int16_t>(pc::ErrorCode::NotLeader))
            << "answered " << a.offset << " from a log prefix (the newest commit is 30)";
    }
    followerAt(3);   // caught up: HW = log end
    ASSERT_EQ(rep3.highWatermark(), 3);
    clock_ += kStep;
    svc3.doWork(clock_);
    const FetchAnswer a = fetchOne(svc3, req_, out_, kBufSize, groupId, kTestTopicId, 0);
    EXPECT_EQ(a.code, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_EQ(a.offset, 30) << "not the newest committed offset after the whole log loaded";
}

// E2 final fix wave, residual round 5 — R1 (coordinator half): a NEW lineage
// minted for a transient partition supersedes every committed offset of the
// old one.  (a) The lineage-change sweep forces every group holding such an
// offset to rebalance (generation bump) — once per new lineage; a commit at
// the old generation is fenced (IllegalGeneration).  (b) A commit that reaches
// the coordinator before the sweep saw the change forces the rebalance itself
// and is refused.  A commit in the new generation is accepted.  RED (no
// superseded-lineage handling): the old-generation commit was accepted.
namespace {
uint64_t g_r1Lineage = 0xA;
uint64_t g_r1Version = 1;
uint64_t r1Lineage(void*, int32_t, int32_t partition) noexcept { return partition == 0 ? g_r1Lineage : 0; }
uint64_t r1Version(void*) noexcept { return g_r1Version; }
}  // namespace

TEST_F(CG3ServiceTest, SupersededLineageForcesARebalanceAndFencesOldGenerationCommitsRegression) {
    const std::string groupId = "g-r1";
    g_r1Lineage = 0xA; g_r1Version = 1;
    service_->setTransientLineage(&r1Lineage, nullptr);
    service_->setTransientLineageVersion(&r1Version, nullptr);
    auto join = [&](const std::string& member) {
        auto l = encodeJoinGroup(req_, 1, 1100, groupId, member, 5000);
        auto n = service_->handleJoinGroup(req_, l, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
        auto r = pc::JoinGroupResponse::decode(out_, n);
        EXPECT_TRUE(r.has_value());
        return std::make_pair(std::string(reinterpret_cast<const char*>(r->memberId.data()), r->memberId.size()),
                              r->generationId);
    };
    auto commit = [&](const std::string& member, int32_t gen, int64_t off) {
        auto l = encodeOffsetCommit(req_, 1, 1101, groupId, member, gen, kTestTopicId, 0, off, "");
        auto n = service_->handleOffsetCommit(req_, l, clock_, out_, kBufSize);
        auto r = pc::OffsetCommitResponse::decode(out_, n);
        return r.has_value() && !r->results.empty() ? r->results[0].errorCode : int16_t{-99};
    };
    auto tick = [&] { clock_ += kStep; replica_->doWork(clock_); service_->doWork(clock_); };
    auto [member, gen0] = join("");
    ASSERT_TRUE(driveUntil([&] { return true; }, 1));
    ASSERT_EQ(commit(member, gen0, 5000), static_cast<int16_t>(pc::ErrorCode::OK));
    tick();
    EXPECT_EQ(service_->supersededLineageRebalances(), 0u);

    // (a) A new lineage is minted (the version moves): the sweep rebalances.
    g_r1Lineage = 0xB; ++g_r1Version;
    tick();
    EXPECT_EQ(service_->supersededLineageRebalances(), 1u) << "the group was not rebalanced for the new lineage";
    EXPECT_EQ(commit(member, gen0, 5100), static_cast<int16_t>(pc::ErrorCode::IllegalGeneration))
        << "SILENT LOSS: a commit at the pre-mint generation was accepted";
    for (int i = 0; i < 5; ++i) tick();
    EXPECT_EQ(service_->supersededLineageRebalances(), 1u) << "rebalanced more than once for one new lineage";
    auto [m2, gen1] = join(member);
    EXPECT_GT(gen1, gen0);
    for (int i = 0; i < 5; ++i) tick();
    EXPECT_EQ(commit(m2, gen1, 3), static_cast<int16_t>(pc::ErrorCode::OK)) << "the new generation's commit was refused";

    // (b) Another mint the sweep has not seen yet (no version move): the first
    // commit forces the rebalance itself and is refused.
    g_r1Lineage = 0xC;
    tick();   // the version did not move: no sweep
    EXPECT_EQ(commit(m2, gen1, 4), static_cast<int16_t>(pc::ErrorCode::IllegalGeneration))
        << "SILENT LOSS: a commit raced ahead of the sweep and was stamped with the new lineage";
    EXPECT_GE(service_->supersededLineageCommitsFenced(), 1u);
    EXPECT_EQ(service_->supersededLineageRebalances(), 2u);
}

// E2 final fix wave, residual round 6 — F4: steady state allocates nothing.
// While a new lineage stays PENDING the coordinator re-checks only the pending
// (group, partition) set every 100 ms — no fetchAll over every group — and
// decides it once it resolves; an Empty group's lineage state is pruned.  RED
// (no pending re-check / no prune): never decided / marks kept forever.
namespace {
uint64_t g_f4Lineage = 0xA;
uint64_t g_f4Version = 1;
uint64_t f4Lineage(void*, int32_t, int32_t partition) noexcept { return partition == 0 ? g_f4Lineage : 0; }
uint64_t f4Version(void*) noexcept { return g_f4Version; }
}  // namespace

TEST_F(CG3ServiceTest, PendingLineageIsReCheckedAloneAndEmptyGroupsArePrunedRegression) {
    g_f4Lineage = 0xA; g_f4Version = 1;
    service_->setTransientLineage(&f4Lineage, nullptr);
    service_->setTransientLineageVersion(&f4Version, nullptr);
    auto tick = [&] { clock_ += kStep; replica_->doWork(clock_); service_->doWork(clock_); };
    auto l = encodeJoinGroup(req_, 1, 1300, "g-f4", "", 60'000);
    auto r = pc::JoinGroupResponse::decode(out_, service_->handleJoinGroup(req_, l, rune::protocol::client::kClientVersion, clock_, out_, kBufSize));
    ASSERT_TRUE(r.has_value());
    const std::string m(reinterpret_cast<const char*>(r->memberId.data()), r->memberId.size());
    l = encodeOffsetCommit(req_, 1, 1301, "g-f4", m, r->generationId, kTestTopicId, 0, 50, "");
    ASSERT_EQ(pc::OffsetCommitResponse::decode(out_, service_->handleOffsetCommit(req_, l, clock_, out_, kBufSize))
                  ->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    tick();
    g_f4Lineage = GroupCoordinationService::kLineagePending; ++g_f4Version;
    tick();
    EXPECT_EQ(service_->pendingLineageChecksForTest(), 1u) << "the undecided partition was not kept for re-check";
    const int32_t gen = service_->coordinator("g-f4").generationId();
    g_f4Lineage = 0xD;   // resolved (a mint), WITHOUT a version move: only the re-check can see it
    for (int i = 0; i < 200; ++i) tick();
    EXPECT_EQ(service_->pendingLineageChecksForTest(), 0u);
    EXPECT_GT(service_->coordinator("g-f4").generationId(), gen) << "the pending re-check never decided it";
    ASSERT_GE(service_->supersededMarkGroupsForTest(), 1u);
    l = encodeLeaveGroup(req_, 1, 1302, "g-f4", m);
    (void)service_->handleLeaveGroup(req_, l, clock_, out_, kBufSize);
    ASSERT_TRUE(driveUntil([&] { return service_->coordinator("g-f4").isEmpty(); }, 20));
    g_f4Lineage = 0xE; ++g_f4Version;
    tick();
    EXPECT_EQ(service_->supersededMarkGroupsForTest(), 0u) << "an Empty group's superseded marks were never pruned";
}

// E2 final fix wave, residual round 6 — F1 (coordinator half) + F4.
// A group whose CURRENT generation consumes a transient partition (it is in its
// assignment) but holds NO committed offset on it: a new lineage still forces
// it to rebalance (sweep) — and a commit that races ahead of the sweep, with no
// stored offset to compare with, is fenced because the generation predates the
// mint.  F4: while the new lineage is pending the coordinator re-checks only
// that pending (group, partition) — no fetchAll over every group — and an
// Empty group's lineage state is pruned.  RED (only committed offsets): the
// no-commit group was never rebalanced and the racing commit was accepted.
namespace {
uint64_t g_f1Lineage = 0xA;
uint64_t g_f1Version = 1;
uint64_t f1Lineage(void*, int32_t, int32_t partition) noexcept { return partition == 0 ? g_f1Lineage : 0; }
uint64_t f1Version(void*) noexcept { return g_f1Version; }
std::size_t encodeJoinWithTopic(uint8_t* buf, int64_t corr, std::string_view group, std::string_view member,
                                int32_t topicId) {
    pc::JoinGroup msg;
    msg.sessionId = 1; msg.correlationId = corr; msg.sessionTimeoutMs = 60'000;
    msg.groupId  = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(group.data()), group.size());
    msg.memberId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(member.data()), member.size());
    const std::array<int32_t, 1> topics{topicId};
    return msg.encode(buf, std::span<const int32_t>(topics));
}
}  // namespace

TEST_F(CG3ServiceTest, GroupConsumingAPartitionWithoutACommitIsFencedOnANewLineageRegression) {
    g_f1Lineage = 0xA; g_f1Version = 1;
    service_->setTransientLineage(&f1Lineage, nullptr);
    service_->setTransientLineageVersion(&f1Version, nullptr);
    auto stableMember = [&](const std::string& group) {
        auto l = encodeJoinWithTopic(req_, 1200, group, "", kTestTopicId);
        auto r = pc::JoinGroupResponse::decode(out_, service_->handleJoinGroup(req_, l, rune::protocol::client::kClientVersion, clock_, out_, kBufSize));
        const std::string m(reinterpret_cast<const char*>(r->memberId.data()), r->memberId.size());
        l = encodeJoinWithTopic(req_, 1201, group, m, kTestTopicId);
        (void)service_->handleJoinGroup(req_, l, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
        EXPECT_TRUE(driveUntil([&] { return service_->coordinator(group).state() == GroupState::Stable; }, 20));
        return m;
    };
    auto commit = [&](const std::string& group, const std::string& m, int32_t gen, int64_t off) {
        auto l = encodeOffsetCommit(req_, 1, 1202, group, m, gen, kTestTopicId, 0, off, "");
        auto r = pc::OffsetCommitResponse::decode(out_, service_->handleOffsetCommit(req_, l, clock_, out_, kBufSize));
        return r.has_value() && !r->results.empty() ? r->results[0].errorCode : int16_t{-99};
    };
    auto tick = [&] { clock_ += kStep; replica_->doWork(clock_); service_->doWork(clock_); };

    // (a) The sweep: "g-swept" consumes partition 0 (assigned), never committed.
    const std::string m1 = stableMember("g-swept");
    const int32_t gen1 = service_->coordinator("g-swept").generationId();
    g_f1Lineage = 0xB; ++g_f1Version;   // a new lineage
    tick();
    EXPECT_GT(service_->coordinator("g-swept").generationId(), gen1)
        << "SILENT LOSS: a group consuming the partition without a commit was not rebalanced";
    EXPECT_EQ(commit("g-swept", m1, gen1, 20), static_cast<int16_t>(pc::ErrorCode::IllegalGeneration));

    // (b) The commit path, ahead of the sweep (the version has not moved yet).
    const std::string m2 = stableMember("g-raced");
    const int32_t gen2 = service_->coordinator("g-raced").generationId();
    g_f1Lineage = 0xC;   // minted; the coordinator has not swept it
    EXPECT_EQ(commit("g-raced", m2, gen2, 20), static_cast<int16_t>(pc::ErrorCode::IllegalGeneration))
        << "SILENT LOSS: a commit with no stored offset, from a generation older than the mint, was accepted";
    EXPECT_GT(service_->coordinator("g-raced").generationId(), gen2);

    // F4: a PENDING lineage is re-checked alone (no fetchAll each 100 ms) and
    // decided once it resolves.
    const std::string m3 = stableMember("g-pending");
    g_f1Lineage = GroupCoordinationService::kLineagePending; ++g_f1Version;
    tick();
    EXPECT_GE(service_->pendingLineageChecksForTest(), 1u) << "the undecided partition was not kept for re-check";
    const int32_t gen3 = service_->coordinator("g-pending").generationId();
    g_f1Lineage = 0xD;   // resolved (a mint) — WITHOUT a version move: only the pending re-check can see it
    for (int i = 0; i < 200; ++i) tick();   // > 100 ms of duty time
    EXPECT_EQ(service_->pendingLineageChecksForTest(), 0u);
    EXPECT_GT(service_->coordinator("g-pending").generationId(), gen3) << "the pending re-check never decided it";

    // F4: once a group is Empty its lineage state is pruned (no unbounded marks).
    ASSERT_GE(service_->supersededMarkGroupsForTest(), 3u);
    for (const auto& [g, m] : {std::pair<std::string, std::string>{"g-swept", m1}, {"g-raced", m2}, {"g-pending", m3}}) {
        auto l = encodeLeaveGroup(req_, 1, 1203, g, m);
        (void)service_->handleLeaveGroup(req_, l, clock_, out_, kBufSize);
        ASSERT_TRUE(driveUntil([&] { return service_->coordinator(g).isEmpty(); }, 20)) << g << " never emptied";
    }
    g_f1Lineage = 0xE; ++g_f1Version;   // the next lineage change sweeps
    tick();
    EXPECT_EQ(service_->supersededMarkGroupsForTest(), 0u) << "an Empty group's superseded marks were never pruned";
}

// Catalog history Task 8 fix round 1 (I3): the catalog DROPS a topic a group
// consumes — the group rebalances it away EXPLICITLY (revoke + a new assignment
// without the dropped partitions), never silently keeping dead partitions.
// RED: onTopicDropped a no-op — the group stays Stable, assigned to a topic
// that no longer exists (its member's push just stops).
TEST_F(CG3ServiceTest, DroppedTopicIsRebalancedAwayFromItsGroupsRegression) {
    const std::string groupId = "g-drop";
    auto join = [&](const std::string& member, int64_t corr) {
        pc::JoinGroup msg;   // subscribed to the test topic (by id)
        msg.sessionId = 1; msg.correlationId = corr; msg.sessionTimeoutMs = 5000;
        msg.groupId  = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(groupId.data()), groupId.size());
        msg.memberId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(member.data()), member.size());
        const std::array<int32_t, 1> topics{kTestTopicId};
        auto n = msg.encode(req_, std::span<const int32_t>(topics));
        auto o = service_->handleJoinGroup(req_, n, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
        auto r = pc::JoinGroupResponse::decode(out_, o);
        EXPECT_TRUE(r.has_value());
        return r ? std::string(reinterpret_cast<const char*>(r->memberId.data()), r->memberId.size()) : std::string{};
    };
    const std::string memberId = join("", 201);
    clock_ += kStep;
    (void)join(memberId, 202);
    ASSERT_TRUE(driveUntil([&] { return service_->coordinator(groupId).state() == GroupState::Stable; }, 10));
    auto holdsTopic = [&] {
        for (const auto& m : service_->coordinator(groupId).members())
            for (const auto& tp : m.assignment) if (tp.topic == kTestTopic) return true;
        return false;
    };
    ASSERT_TRUE(holdsTopic()) << "non-vacuous: the group consumes the topic";
    const int32_t genBefore = service_->coordinator(groupId).generationId();

    ASSERT_TRUE(registry_->dropTopic(kTestTopicId));
    EXPECT_EQ(service_->onTopicDropped(kTestTopic, clock_), 1u);
    EXPECT_EQ(service_->coordinator(groupId).state(), GroupState::PreparingRebalance)
        << "the group learns the drop explicitly (a rebalance)";
    EXPECT_EQ(service_->onTopicDropped(kTestTopic, clock_), 0u) << "idempotent: already rebalancing it away";
    clock_ += kStep;
    (void)join(memberId, 203);
    ASSERT_TRUE(driveUntil([&] { return service_->coordinator(groupId).state() == GroupState::Stable; }, 10));
    EXPECT_GT(service_->coordinator(groupId).generationId(), genBefore);
    EXPECT_FALSE(holdsTopic()) << "the dropped topic's partitions are no longer assigned";
}

// Task 9 fix round 1 (Minor 1, C1 for groups): a catalog topic APPLIED here but not
// PLACED yet is not assignable — a rebalance in that window would hand out
// partitions the gateway refuses NotPlaced.  It joins the next rebalance once its
// placement commits; a static topic is assignable from boot.
TEST_F(CG3ServiceTest, AppliedButNotPlacedTopicIsNotAssignedUntilPlacedRegression) {
    rune::node::TopicSpec rt = makeTopicSpec("runtime", /*partitions=*/3, /*id=*/kTestTopicId + 1);
    ASSERT_EQ(registry_->createTopicAt(kTestTopicId + 1, rt), kTestTopicId + 1);
    rune::gateway::ClientGateway gw = rune::gateway::ClientGateway::makeForTest(
        [](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1);
    gw.setTopicRegistry(registry_.get());
    gw.setNodeIdForTest(kLeader);
    auto route = [&](bool placed) {
        std::vector<rune::metadata::RoutingImage::BuildEntry> img;
        for (int32_t p = 0; p < 2; ++p) img.push_back({rune::metadata::gpidOf(kTestTopicId, p), kLeader, 1});
        if (placed)
            for (int32_t p = 0; p < 3; ++p) img.push_back({rune::metadata::gpidOf(kTestTopicId + 1, p), kLeader, 1});
        gw.setRoutingImageSnapshot(
            rune::metadata::RoutingImage::build(img, rune::metadata::RoutingImage::Source::Committed));
    };
    route(/*placed=*/false);
    service_->setGateway(&gw);

    auto counts = service_->topicPartitionCountsForTest();
    EXPECT_EQ(counts.count(std::string(kTestTopic)), 1u) << "a static topic is assignable";
    EXPECT_EQ(counts.count("runtime"), 0u) << "applied, not placed: not assignable";

    route(/*placed=*/true);
    counts = service_->topicPartitionCountsForTest();
    ASSERT_EQ(counts.count("runtime"), 1u) << "placed: assignable";
    EXPECT_EQ(counts.at("runtime"), 3);
    service_->setGateway(nullptr);
}

// ---------------------------------------------------------------------------
// Phase 2c (spec 13.3): one filter per consumer group, fixed by the first member.
// ---------------------------------------------------------------------------
namespace {
const std::vector<uint8_t> kGroupFilterA{1, 1, 0xAB};
const std::vector<uint8_t> kGroupFilterB{1, 1, 0xCD};
}

TEST_F(CG3ServiceTest, MatchingFilterJoinsAndRebalancesNormally) {
    constexpr int32_t v10 = rune::protocol::client::kClientVersion;
    auto n = encodeJoinGroup(req_, 1, 1, "fg", "", 5000, kGroupFilterA);
    auto r1 = pc::JoinGroupResponse::decode(out_, service_->handleJoinGroup(req_, n, v10, clock_, out_, kBufSize));
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    const std::string m1(reinterpret_cast<const char*>(r1->memberId.data()), r1->memberId.size());

    n = encodeJoinGroup(req_, 2, 2, "fg", "", 5000, kGroupFilterA);   // a second member, the same bytes
    auto r2 = pc::JoinGroupResponse::decode(out_, service_->handleJoinGroup(req_, n, v10, clock_, out_, kBufSize));
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_EQ(service_->coordinator("fg").memberCount(), 2u);
    EXPECT_EQ(service_->coordinator("fg").state(), GroupState::PreparingRebalance);
    EXPECT_GT(r2->generationId, r1->generationId);
}

TEST_F(CG3ServiceTest, FilterMismatchAnswersInvalidRequestWithDetailRegression) {
    constexpr int32_t v10 = rune::protocol::client::kClientVersion;
    auto n = encodeJoinGroup(req_, 1, 1, "fg", "", 5000, kGroupFilterA);
    (void)service_->handleJoinGroup(req_, n, v10, clock_, out_, kBufSize);
    const auto gen0   = service_->coordinator("fg").generationId();
    const auto state0 = service_->coordinator("fg").state();

    for (const auto* other : {&kGroupFilterB, static_cast<const std::vector<uint8_t>*>(nullptr)}) {
        n = encodeJoinGroup(req_, 2, 2, "fg", "", 5000, other ? std::span<const uint8_t>(*other) : std::span<const uint8_t>{});
        const std::size_t o = service_->handleJoinGroup(req_, n, v10, clock_, out_, kBufSize);
        auto r = pc::JoinGroupResponse::decode(out_, o);
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(r->errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
        EXPECT_TRUE(r->memberId.empty());
        EXPECT_EQ(r->detail.substr(0, pc::kGroupFilterMismatchPrefix.size()), pc::kGroupFilterMismatchPrefix);
        EXPECT_NE(r->detail.find("0101ab"), std::string_view::npos) << r->detail;
    }
    EXPECT_EQ(service_->coordinator("fg").memberCount(), 1u) << "the refused joiner is not a member";
    EXPECT_EQ(service_->coordinator("fg").generationId(), gen0) << "a refusal does not rebalance";
    EXPECT_EQ(service_->coordinator("fg").state(), state0);
}

TEST_F(CG3ServiceTest, OldMemberWithoutFilterIsRefusedFromAFilteredGroupRegression) {
    auto n = encodeJoinGroup(req_, 1, 1, "fg", "", 5000, kGroupFilterA);
    (void)service_->handleJoinGroup(req_, n, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);

    n = encodeJoinGroup(req_, 2, 2, "fg", "");
    const std::size_t o = service_->handleJoinGroup(req_, n, /*clientVersion=*/9, clock_, out_, kBufSize);
    auto r = pc::JoinGroupResponse::decode(out_, o);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_TRUE(r->detail.empty());
    EXPECT_EQ(o, pc::JoinGroupResponse{}.encodedSize()) << "the byte-identical legacy frame";
    EXPECT_EQ(service_->coordinator("fg").memberCount(), 1u);
}

TEST_F(CG3ServiceTest, SoleMemberRejoiningWithADifferentFilterIsRefusedAndKeptRegression) {
    constexpr int32_t v10 = rune::protocol::client::kClientVersion;
    auto n = encodeJoinGroup(req_, 1, 1, "fg", "", 5000, kGroupFilterA);
    auto r1 = pc::JoinGroupResponse::decode(out_, service_->handleJoinGroup(req_, n, v10, clock_, out_, kBufSize));
    ASSERT_TRUE(r1.has_value());
    const std::string m1(reinterpret_cast<const char*>(r1->memberId.data()), r1->memberId.size());
    const auto gen0 = service_->coordinator("fg").generationId();

    n = encodeJoinGroup(req_, 1, 2, "fg", m1, 5000, kGroupFilterB);
    auto r2 = pc::JoinGroupResponse::decode(out_, service_->handleJoinGroup(req_, n, v10, clock_, out_, kBufSize));
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_EQ(service_->coordinator("fg").memberCount(), 1u) << "the refusal does not remove the member";
    EXPECT_EQ(service_->coordinator("fg").generationId(), gen0);
    EXPECT_EQ(std::vector<uint8_t>(service_->coordinator("fg").filterBytes().begin(),
                                   service_->coordinator("fg").filterBytes().end()), kGroupFilterA);
}

TEST_F(CG3ServiceTest, FilterJoiningAGroupFixedWithNoFilterNamesNoneRegression) {
    constexpr int32_t v10 = rune::protocol::client::kClientVersion;
    auto n = encodeJoinGroup(req_, 1, 1, "fg", "");
    (void)service_->handleJoinGroup(req_, n, v10, clock_, out_, kBufSize);
    n = encodeJoinGroup(req_, 2, 2, "fg", "", 5000, kGroupFilterA);
    auto r = pc::JoinGroupResponse::decode(out_, service_->handleJoinGroup(req_, n, v10, clock_, out_, kBufSize));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_TRUE(r->detail.ends_with("filter none")) << r->detail;
}

TEST_F(CG3ServiceTest, PreV5MemberIsRefusedWithProtocolVersionFromAFilteredGroupRegression) {
    auto n = encodeJoinGroup(req_, 1, 1, "fg", "", 5000, kGroupFilterA);
    (void)service_->handleJoinGroup(req_, n, rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    for (const int32_t old : {3, 4}) {   // below 5 re-joins forever on InvalidRequest
        n = encodeJoinGroup(req_, 2, 2, "fg", "");
        const std::size_t o = service_->handleJoinGroup(req_, n, old, clock_, out_, kBufSize);
        auto r = pc::JoinGroupResponse::decode(out_, o);
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(r->errorCode, static_cast<int16_t>(pc::ErrorCode::ProtocolVersion)) << old;
        EXPECT_TRUE(r->detail.empty());
        EXPECT_EQ(o, pc::JoinGroupResponse{}.encodedSize());
    }
    EXPECT_EQ(service_->coordinator("fg").memberCount(), 1u);
}

TEST_F(CG3ServiceTest, OverCapJoinGroupFilterIsRefusedAndNeverFixesTheGroupRegression) {
    constexpr int32_t v10 = rune::protocol::client::kClientVersion;
    const std::vector<uint8_t> big(rune::codec::kMaxFilterBytes + 1, 0x42);
    const std::vector<uint8_t> atCap(rune::codec::kMaxFilterBytes, 0x42);
    auto n = encodeJoinGroup(req_, 1, 1, "fg", "", 5000, big);
    std::size_t o = service_->handleJoinGroup(req_, n, v10, clock_, out_, kBufSize);
    auto r = pc::JoinGroupResponse::decode(out_, o);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_FALSE(r->detail.empty());
    EXPECT_EQ(service_->coordinator("fg").memberCount(), 0u);
    EXPECT_TRUE(service_->coordinator("fg").filterBytes().empty());

    // the cap itself is admitted (validation of the bytes is the Subscribe path's job)
    n = encodeJoinGroup(req_, 1, 2, "fg", "", 5000, atCap);
    r = pc::JoinGroupResponse::decode(out_, service_->handleJoinGroup(req_, n, v10, clock_, out_, kBufSize));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->errorCode, static_cast<int16_t>(pc::ErrorCode::OK));
}

// ===========================================================================
// Issues #123 / #124 / #169: a member id is bound to the session that obtained it.  Sync, Heartbeat, Leave and
// OffsetCommit naming another session's member are answered exactly as for a member that does not exist
// (UnknownMember), and change nothing.
// ===========================================================================
namespace {
struct JoinedMember { std::string id; int32_t generation = -1; };
}  // namespace

class MemberBindingTest : public CG3ServiceTest {
protected:
    std::string memberOf(std::size_t outLen) {
        const auto r = pc::JoinGroupResponse::decode(out_, outLen);
        EXPECT_TRUE(r.has_value());
        return r ? std::string(reinterpret_cast<const char*>(r->memberId.data()), r->memberId.size()) : std::string{};
    }
    /// Session `sid` joins `group` as a new member and the group reaches Stable with it alone.
    JoinedMember joinStable(int64_t sid, const std::string& group) {
        std::size_t n = service_->handleJoinGroup(req_, encodeJoinGroup(req_, sid, ++corr_, group, "", 5000), rune::protocol::client::kClientVersion, clock_,
                                                  out_, kBufSize);
        const std::string id = memberOf(n);
        clock_ += kStep;
        n = service_->handleJoinGroup(req_, encodeJoinGroup(req_, sid, ++corr_, group, id, 5000), rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
        EXPECT_EQ(memberOf(n), id);
        EXPECT_TRUE(driveUntil([&] { return service_->coordinator(group).state() == GroupState::Stable; }, 10));
        return {id, service_->coordinator(group).generationId()};
    }
    int16_t heartbeat(int64_t sid, const std::string& group, const std::string& member, int32_t gen) {
        return decodeHBErrorCode(out_, service_->handleHeartbeat(req_, encodeHeartbeat(req_, sid, ++corr_, group, member, gen),
                                                               clock_, out_, kBufSize));
    }
    int16_t sync(int64_t sid, const std::string& group, const std::string& member, int32_t gen) {
        return decodeSyncErrorCode(out_, service_->handleSyncGroup(req_, encodeSyncGroup(req_, sid, ++corr_, group, member, gen),
                                                                 clock_, out_, kBufSize));
    }
    int16_t leave(int64_t sid, const std::string& group, const std::string& member) {
        return decodeLeaveErrorCode(out_, service_->handleLeaveGroup(req_, encodeLeaveGroup(req_, sid, ++corr_, group, member),
                                                                   clock_, out_, kBufSize));
    }
    int16_t commit(int64_t sid, const std::string& group, const std::string& member, int32_t gen, int64_t offset) {
        const std::size_t n = service_->handleOffsetCommit(
            req_, encodeOffsetCommit(req_, sid, ++corr_, group, member, gen, kTestTopicId, 0, offset), clock_, out_, kBufSize);
        const auto r = pc::OffsetCommitResponse::decode(out_, n);
        return r && !r->results.empty() ? r->results[0].errorCode : int16_t{-1};
    }
    int64_t committed(const std::string& group) {
        const std::size_t n = service_->handleOffsetFetch(req_, encodeOffsetFetch(req_, 1, ++corr_, group, kTestTopicId, 0),
                                                          clock_, out_, kBufSize);
        const auto r = pc::OffsetFetchResponse::decode(out_, n);
        return r && !r->results.empty() ? r->results[0].offset : int64_t{-999};
    }
    static constexpr int16_t kUnknownMember = static_cast<int16_t>(pc::ErrorCode::UnknownMember);
    static constexpr int16_t kOk            = static_cast<int16_t>(pc::ErrorCode::OK);
    int64_t corr_ = 10'000;
};

TEST_F(MemberBindingTest, LeaveGroupOfAnotherSessionsMemberIsRefusedRegression) {
    const JoinedMember m = joinStable(/*sid=*/1, "world-7");
    EXPECT_EQ(leave(/*sid=*/2, "world-7", m.id), kUnknownMember) << "another session's member is not this session's to evict";
    EXPECT_EQ(leave(2, "world-7", "world-7-no-such-member"), kUnknownMember) << "the same answer as a member that does not exist";
    auto& gc = service_->coordinator("world-7");
    EXPECT_EQ(gc.memberCount(), 1u);
    EXPECT_EQ(gc.state(), GroupState::Stable) << "no rebalance";
    EXPECT_EQ(gc.generationId(), m.generation);
    EXPECT_EQ(heartbeat(1, "world-7", m.id, m.generation), kOk) << "the owner is still a member";
    EXPECT_EQ(leave(1, "world-7", m.id), kOk) << "the owner itself may leave";
    EXPECT_EQ(gc.memberCount(), 0u);
}

TEST_F(MemberBindingTest, SyncGroupOfAnotherSessionsMemberIsRefusedRegression) {
    const JoinedMember m = joinStable(1, "g-sync");
    EXPECT_EQ(sync(2, "g-sync", m.id, m.generation), kUnknownMember) << "another session's assignment is not handed out";
    EXPECT_EQ(sync(2, "g-sync", "g-sync-no-such-member", m.generation), kUnknownMember);
    EXPECT_EQ(sync(1, "g-sync", m.id, m.generation), kOk);
}

TEST_F(MemberBindingTest, HeartbeatOfAnotherSessionsMemberIsRefusedRegression) {
    const JoinedMember m = joinStable(1, "g-hb");
    EXPECT_EQ(heartbeat(2, "g-hb", m.id, m.generation), kUnknownMember);
    EXPECT_EQ(heartbeat(2, "g-hb", "g-hb-no-such-member", m.generation), kUnknownMember);
    // Only the foreign session heartbeats: the member is not kept alive by it and expires on its own timeout.
    const int64_t until = clock_ + 6'000 * kStep;
    while (clock_ < until) {
        clock_ += 100 * kStep;
        (void)heartbeat(2, "g-hb", m.id, m.generation);
        service_->doWork(clock_);
    }
    EXPECT_EQ(service_->coordinator("g-hb").memberCount(), 0u) << "a foreign heartbeat refreshed the member";
}

TEST_F(MemberBindingTest, OffsetCommitOfAnotherSessionsMemberIsRefusedRegression) {
    const JoinedMember m = joinStable(1, "g-commit");
    ASSERT_EQ(commit(1, "g-commit", m.id, m.generation, 100), kOk);
    EXPECT_EQ(commit(2, "g-commit", m.id, m.generation, 500), kUnknownMember);
    EXPECT_EQ(commit(2, "g-commit", "g-commit-no-such-member", m.generation, 500), kUnknownMember);
    EXPECT_EQ(committed("g-commit"), 100) << "the foreign commit moved the group's position";
}

TEST_F(MemberBindingTest, JoinGroupWithAnotherSessionsMemberIdIsRefusedRegression) {
    const JoinedMember m = joinStable(1, "g-rejoin");
    // Without a gateway every session is the same (anonymous) principal; a different principal is covered in
    // AuthEnforcementTest.  Here: an unknown member id never names a member - it gets a fresh one.
    const std::size_t n = service_->handleJoinGroup(
        req_, encodeJoinGroup(req_, 2, ++corr_, "g-rejoin", "g-rejoin-chosen-by-the-client", 5000), rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    const std::string got = memberOf(n);
    EXPECT_NE(got, "g-rejoin-chosen-by-the-client") << "a client-chosen id is never adopted";
    EXPECT_NE(got, m.id);
    EXPECT_FALSE(got.empty());
}

// Issue #169: generations restart with the coordinator; a commit from a member of the previous coordinator that
// carries a generation number the new one reuses is refused (the member id is checked, not only the generation).
TEST_F(MemberBindingTest, OffsetCommitFromAPreRestartMemberIsRefusedRegression) {
    const JoinedMember old = joinStable(1, "g-restart");
    ASSERT_EQ(commit(1, "g-restart", old.id, old.generation, 100), kOk);
    ASSERT_TRUE(pumpHW(1));

    // Restart the coordinator over the same replicated offsets.
    service_ = std::make_unique<GroupCoordinationService>(store_.get(), registry_.get(), [this] { return isLeader_; },
                                                          [this] { return leaderNodeId_; });
    service_->doWork(clock_);
    ASSERT_EQ(committed("g-restart"), 100);
    const JoinedMember fresh = joinStable(3, "g-restart");
    ASSERT_EQ(fresh.generation, old.generation) << "precondition: the new coordinator reuses the generation number";

    EXPECT_EQ(commit(1, "g-restart", old.id, old.generation, 500), kUnknownMember);
    EXPECT_EQ(commit(3, "g-restart", old.id, old.generation, 500), kUnknownMember) << "the old id, even on a live session";
    EXPECT_EQ(committed("g-restart"), 100) << "a pre-restart member's late commit moved the position";
    EXPECT_EQ(commit(3, "g-restart", fresh.id, fresh.generation, 200), kOk);
}

// Issue #124 (principal binding): sessions 1 and 3 are user 7, session 2 is user 8.  User 8 naming user 7's member
// in a JoinGroup gets a new member of its own; user 7 re-joining from a new session (a reconnect) takes its member
// over, and the old session no longer owns it.
namespace {
uint64_t principalBySession(void*, int64_t sid) noexcept { return sid == 2 ? 8 : 7; }
}  // namespace

TEST_F(MemberBindingTest, JoinGroupWithAnotherPrincipalsMemberIdJoinsANewMemberRegression) {
    service_->setPrincipalResolverForTest(&principalBySession, nullptr);
    const JoinedMember m = joinStable(/*sid=*/1, "g-principal");
    const std::string foreign = memberOf(service_->handleJoinGroup(
        req_, encodeJoinGroup(req_, /*sid=*/2, ++corr_, "g-principal", m.id, 5000), rune::protocol::client::kClientVersion, clock_, out_, kBufSize));
    EXPECT_NE(foreign, m.id) << "another principal took over the member (and its assignment)";
    EXPECT_FALSE(foreign.empty());
    EXPECT_EQ(service_->coordinator("g-principal").memberCount(), 2u);

    // Same principal, new session: a re-join of its own member.
    const std::string again = memberOf(service_->handleJoinGroup(
        req_, encodeJoinGroup(req_, /*sid=*/3, ++corr_, "g-principal", m.id, 5000), rune::protocol::client::kClientVersion, clock_, out_, kBufSize));
    EXPECT_EQ(again, m.id) << "a reconnecting owner keeps its member";
    const int32_t gen = service_->coordinator("g-principal").generationId();
    EXPECT_EQ(heartbeat(1, "g-principal", m.id, gen), kUnknownMember) << "the old session no longer owns it";
    EXPECT_NE(heartbeat(3, "g-principal", m.id, gen), kUnknownMember);
}

// The member-id table drops a member the coordinator removed on its own (session expiry): its id is unbound, so a
// re-join naming it - even by the old owner's session - is a fresh member, and nothing accumulates.
TEST_F(MemberBindingTest, ExpiredMemberLosesItsBindingRegression) {
    const JoinedMember m = joinStable(1, "g-expire");
    ASSERT_TRUE(driveUntil([&] { return service_->coordinator("g-expire").memberCount() == 0; }, 10'000));
    EXPECT_EQ(heartbeat(1, "g-expire", m.id, m.generation), kUnknownMember);
    EXPECT_EQ(commit(1, "g-expire", m.id, m.generation, 7), kUnknownMember);
    EXPECT_EQ(leave(1, "g-expire", m.id), kUnknownMember);
    const std::string next = memberOf(service_->handleJoinGroup(
        req_, encodeJoinGroup(req_, 1, ++corr_, "g-expire", m.id, 5000), rune::protocol::client::kClientVersion, clock_, out_, kBufSize));
    EXPECT_NE(next, m.id);
}

// ===========================================================================
// Adversarial review of PR #423 (issues #123 / #124 / #169): attack the member-binding area through the service's
// public handlers (the wire frames the gateway forwards).
// ===========================================================================

// Protocol state (replay after Leave): a member id that left is never re-adopted - not by its own session, not by
// another - and every member-scoped request naming it is UnknownMember.
TEST_F(MemberBindingTest, LeftMemberIdIsNeverReadoptedAdversarialRegression) {
    const JoinedMember m = joinStable(1, "g-left");
    ASSERT_EQ(leave(1, "g-left", m.id), kOk);
    for (const int64_t sid : {int64_t{1}, int64_t{2}}) {
        EXPECT_EQ(heartbeat(sid, "g-left", m.id, m.generation), kUnknownMember) << "session " << sid;
        EXPECT_EQ(sync(sid, "g-left", m.id, m.generation),
                  sync(sid, "g-left", "g-left-9-00000000000000000000000000000000", m.generation)) << "session " << sid;
        EXPECT_EQ(commit(sid, "g-left", m.id, m.generation, 9), kUnknownMember) << "session " << sid;
        EXPECT_EQ(leave(sid, "g-left", m.id), kUnknownMember) << "session " << sid;
        const std::string got = memberOf(service_->handleJoinGroup(
            req_, encodeJoinGroup(req_, sid, ++corr_, "g-left", m.id, 5000), rune::protocol::client::kClientVersion, clock_, out_, kBufSize));
        EXPECT_NE(got, m.id) << "session " << sid << ": a departed member id was re-adopted";
        EXPECT_FALSE(got.empty());
    }
}

// Protocol state x group state: requests naming another session's member while the group is PreparingRebalance and
// then Stable change nothing (no generation bump, no member removed, no position moved) and are answered exactly as
// for a member that does not exist.
TEST_F(MemberBindingTest, ForeignRequestsInEveryRebalanceStateChangeNothingAdversarialRegression) {
    const JoinedMember a = joinStable(1, "g-states");
    auto& gc = service_->coordinator("g-states");
    ASSERT_EQ(commit(1, "g-states", a.id, a.generation, 100), kOk);
    // A second member (session 4) joins: PreparingRebalance.
    const std::string b = memberOf(service_->handleJoinGroup(
        req_, encodeJoinGroup(req_, 4, ++corr_, "g-states", "", 5000), rune::protocol::client::kClientVersion, clock_, out_, kBufSize));
    ASSERT_EQ(gc.state(), GroupState::PreparingRebalance);
    auto attack = [&](const char* where) {
        const int32_t gen = gc.generationId();
        const GroupState st = gc.state();
        const std::size_t members = gc.memberCount();
        const std::string nobody = "g-states-9-00000000000000000000000000000000";
        for (const std::string& victim : {a.id, b}) {
            EXPECT_EQ(heartbeat(2, "g-states", victim, gen), heartbeat(2, "g-states", nobody, gen)) << where;
            EXPECT_EQ(sync(2, "g-states", victim, gen), sync(2, "g-states", nobody, gen)) << where;
            EXPECT_EQ(commit(2, "g-states", victim, gen, 777), kUnknownMember) << where;
            EXPECT_EQ(leave(2, "g-states", victim), kUnknownMember) << where;
        }
        EXPECT_EQ(gc.generationId(), gen) << where;
        EXPECT_EQ(gc.state(), st) << where;
        EXPECT_EQ(gc.memberCount(), members) << where;
        EXPECT_EQ(committed("g-states"), 100) << where << ": a foreign commit moved the position";
    };
    attack("PreparingRebalance");
    (void)service_->handleJoinGroup(req_, encodeJoinGroup(req_, 1, ++corr_, "g-states", a.id, 5000), rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    (void)service_->handleJoinGroup(req_, encodeJoinGroup(req_, 4, ++corr_, "g-states", b, 5000), rune::protocol::client::kClientVersion, clock_, out_, kBufSize);
    ASSERT_TRUE(driveUntil([&] { return gc.state() == GroupState::Stable; }, 10));
    attack("Stable");
    EXPECT_EQ(gc.memberCount(), 2u);
}

// Concurrency / reconnect: the owner re-joins from a new session (same principal) and so moves its member there.  The
// old session closing afterwards (session reap) must not evict the moved member, and the old session no longer owns it.
TEST_F(MemberBindingTest, ClosingTheOldSessionDoesNotEvictAMemberItsOwnerMovedAdversarialRegression) {
    const JoinedMember m = joinStable(1, "g-moved");
    const std::string again = memberOf(service_->handleJoinGroup(
        req_, encodeJoinGroup(req_, 3, ++corr_, "g-moved", m.id, 5000), rune::protocol::client::kClientVersion, clock_, out_, kBufSize));
    ASSERT_EQ(again, m.id) << "a reconnect of the same principal keeps its member";
    service_->evictSessionMembers(/*sessionId=*/1, clock_);
    EXPECT_EQ(service_->coordinator("g-moved").memberCount(), 1u) << "the old session's reap evicted the moved member";
    const int32_t gen = service_->coordinator("g-moved").generationId();
    EXPECT_EQ(heartbeat(1, "g-moved", m.id, gen), kUnknownMember);
    EXPECT_EQ(leave(1, "g-moved", m.id), kUnknownMember);
    EXPECT_NE(heartbeat(3, "g-moved", m.id, gen), kUnknownMember);
    EXPECT_EQ(leave(3, "g-moved", m.id), kOk);
}

// Input bounds: the binding key is "<group>:<member>".  A group name and a member id that concatenate to another
// group's key must never reach that member, and a JoinGroup naming such an id must not adopt it (an id is never taken
// from the client) nor move the other group's binding.  All sessions are the same (anonymous) principal here, so the
// principal check does not mask the alias.
TEST_F(MemberBindingTest, MemberKeySeparatorDoesNotAliasAcrossGroupsAdversarialRegression) {
    const JoinedMember victim = joinStable(1, "x:y");
    const int32_t gen = victim.generation;
    // Group "x" + member "y:<victim id>" builds the victim's key "x:y:<victim id>".
    const std::string alias = "y:" + victim.id;
    EXPECT_EQ(heartbeat(2, "x", alias, gen), kUnknownMember);
    EXPECT_EQ(leave(2, "x", alias), kUnknownMember);
    EXPECT_EQ(commit(2, "x", alias, gen, 5), kUnknownMember);
    const std::string got = memberOf(service_->handleJoinGroup(
        req_, encodeJoinGroup(req_, 2, ++corr_, "x", alias, 5000), rune::protocol::client::kClientVersion, clock_, out_, kBufSize));
    EXPECT_NE(got, alias);
    EXPECT_EQ(service_->coordinator("x:y").memberCount(), 1u);
    EXPECT_EQ(heartbeat(1, "x:y", victim.id, gen), kOk) << "the victim is untouched";
}

// Input bounds: an EMPTY member id (a standalone consumer's commit, or an id cleared by a refusal) is never a member:
// OffsetCommit, Heartbeat, Sync and Leave naming it are UnknownMember and the position does not move.
TEST_F(MemberBindingTest, EmptyMemberIdIsNeverAMemberAdversarialRegression) {
    const JoinedMember m = joinStable(1, "g-empty");
    ASSERT_EQ(commit(1, "g-empty", m.id, m.generation, 100), kOk);
    for (const int64_t sid : {int64_t{1}, int64_t{2}}) {
        EXPECT_EQ(commit(sid, "g-empty", "", m.generation, 500), kUnknownMember) << "session " << sid;
        EXPECT_EQ(heartbeat(sid, "g-empty", "", m.generation), kUnknownMember) << "session " << sid;
        EXPECT_EQ(sync(sid, "g-empty", "", m.generation), kUnknownMember) << "session " << sid;
        EXPECT_EQ(leave(sid, "g-empty", ""), kUnknownMember) << "session " << sid;
    }
    EXPECT_EQ(committed("g-empty"), 100);
    EXPECT_EQ(service_->coordinator("g-empty").memberCount(), 1u);
}

// Input bounds: a LeaveGroup truncated at every byte offset is answered (InvalidRequest, or nothing for a frame too
// short to carry a header) and never evicts the member.
TEST_F(MemberBindingTest, LeaveGroupTruncatedAtEveryOffsetAdversarialRegression) {
    const JoinedMember m = joinStable(1, "g-trunc");
    const std::size_t full = encodeLeaveGroup(req_, 1, ++corr_, "g-trunc", m.id);
    std::vector<uint8_t> frame(req_, req_ + full);
    for (std::size_t cut = 0; cut < full; ++cut) {
        std::vector<uint8_t> t(frame.begin(), frame.begin() + static_cast<std::ptrdiff_t>(cut));
        const std::size_t n = service_->handleLeaveGroup(t.data(), t.size(), clock_, out_, kBufSize);
        if (n > 0) {
            const auto r = pc::LeaveGroupResponse::decode(out_, n);
            const auto e = pc::ErrorResponse::decode(out_, n);
            const int code = r ? r->errorCode : (e ? static_cast<int>(e->code) : -1);
            EXPECT_NE(code, static_cast<int>(kOk)) << "cut " << cut << ": a truncated Leave succeeded";
        }
        ASSERT_EQ(service_->coordinator("g-trunc").memberCount(), 1u) << "cut " << cut << ": the member was evicted";
    }
    EXPECT_EQ(heartbeat(1, "g-trunc", m.id, m.generation), kOk);
}

// ===========================================================================
// #358: a COMMITTED transaction's offset is Applied only once the __consumer_offsets record is committed (the
// offsets replica's high watermark passed it).  Until then Replicating - the transaction coordinator keeps the
// transaction PrepareCommit and asks again - and asking again waits on the same record (never re-appends) unless
// the offsets replica's leadership changed under it (the record may be gone): then it is appended again.
// ===========================================================================
TEST(TxnOffsetApplyRegression, ATransactionalOffsetIsAppliedOnlyOnceTheOffsetsRecordIsCommittedRegression) {
    using rune::coordination::TxnOffsetApply;
    TempDir dir("txnoff_hw"), topicDir("txnoff_hw_topics");
    rune::storage::Log log(dir.path);
    NullTransport transport;
    // Leader 1 with follower 2 in the ISR: an acks-all append commits only once 2 has fetched it.
    PartitionReplica rep(makeSingleNodeConfig(1, /*minIsr=*/1), Role::Leader, /*epoch=*/1,
                         std::vector<NodeId>{1, 2}, std::vector<NodeId>{1, 2}, &log, &transport);
    ReplicatedOffsetStore store(&rep);
    rune::gateway::TopicRegistry registry(std::vector<TopicSpec>{makeTopicSpec(std::string(kTestTopic), 2, kTestTopicId)},
                                          topicDir.path);
    GroupCoordinationService svc(&store, &registry, [] { return true; }, [] { return NodeId{1}; });
    int64_t clock = 0;
    svc.doWork(clock);   // loads the (empty) offsets

    const auto followerFetches = [&](int64_t upTo) {
        FetchRequestMsg req{};
        req.leaderEpoch = rep.epoch(); req.partition = 0; req.followerId = 2; req.fetchOffset = upTo;
        req.maxBytes = 1 << 20; req.topicId = 0;
        clock += 1'000'000;
        (void)rep.onFetchRequest(req, clock);
        rep.doWork(clock);
    };

    EXPECT_EQ(svc.applyTransactionalOffset("g", tp(std::string(kTestTopic), 0), 42, clock), TxnOffsetApply::Replicating)
        << "appended, not committed: never Applied on the local append alone";
    const auto leo = rep.logEndOffset();
    ASSERT_EQ(rep.highWatermark(), 0);
    EXPECT_EQ(svc.applyTransactionalOffset("g", tp(std::string(kTestTopic), 0), 42, clock), TxnOffsetApply::Replicating);
    EXPECT_EQ(rep.logEndOffset(), leo) << "asking again waits on the record, it does not append another";
    EXPECT_EQ(svc.txnOffsetsReplicating(), 1u);

    followerFetches(leo);
    ASSERT_GT(rep.highWatermark(), leo - 1) << "the follower has the record: committed";
    EXPECT_EQ(svc.applyTransactionalOffset("g", tp(std::string(kTestTopic), 0), 42, clock), TxnOffsetApply::Applied);
    EXPECT_EQ(svc.txnOffsetsReplicating(), 0u);
    EXPECT_EQ(rep.logEndOffset(), leo);
    ASSERT_TRUE(store.fetch("g", tp(std::string(kTestTopic), 0)).has_value());
    EXPECT_EQ(store.fetch("g", tp(std::string(kTestTopic), 0))->offset, 42);

    // A pending record whose leadership changes (it may be truncated away) is appended again by the new leadership.
    EXPECT_EQ(svc.applyTransactionalOffset("g", tp(std::string(kTestTopic), 1), 7, clock), TxnOffsetApply::Replicating);
    const auto leo2 = rep.logEndOffset();
    rep.becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/2);
    rep.becomeLeader(/*newEpoch=*/3, std::vector<NodeId>{1, 2});
    EXPECT_EQ(svc.applyTransactionalOffset("g", tp(std::string(kTestTopic), 1), 7, clock), TxnOffsetApply::Refused)
        << "the new leadership has not reloaded the offsets yet";
    followerFetches(rep.logEndOffset());   // the new leadership commits its log end ...
    clock += 1'000'000;
    svc.doWork(clock);                     // ... and the group coordinator reloads the offsets
    const auto r = svc.applyTransactionalOffset("g", tp(std::string(kTestTopic), 1), 7, clock);
    EXPECT_EQ(r, TxnOffsetApply::Replicating);
    EXPECT_GT(rep.logEndOffset(), leo2) << "a new leadership appends the offset again";
    EXPECT_EQ(svc.txnOffsetsReplicating(), 1u) << "the earlier leadership's entry is gone";
    followerFetches(rep.logEndOffset());
    EXPECT_EQ(svc.applyTransactionalOffset("g", tp(std::string(kTestTopic), 1), 7, clock), TxnOffsetApply::Applied);

    // Not the coordinator: Refused, nothing appended.
    GroupCoordinationService notCoord(&store, &registry, [] { return false; }, [] { return NodeId{2}; });
    const auto leo3 = rep.logEndOffset();
    EXPECT_EQ(notCoord.applyTransactionalOffset("g", tp(std::string(kTestTopic), 0), 99, clock), TxnOffsetApply::Refused);
    EXPECT_EQ(rep.logEndOffset(), leo3);
}

// #358: a single-replica offsets partition commits on the append itself - Applied at once, nothing waits.
TEST(TxnOffsetApplyRegression, ASingleReplicaOffsetsPartitionAppliesAtOnceRegression) {
    using rune::coordination::TxnOffsetApply;
    TempDir dir("txnoff_one"), topicDir("txnoff_one_topics");
    rune::storage::Log log(dir.path);
    NullTransport transport;
    PartitionReplica rep(makeSingleNodeConfig(1, 1), Role::Leader, 1, std::vector<NodeId>{1}, std::vector<NodeId>{1},
                         &log, &transport);
    ReplicatedOffsetStore store(&rep);
    rune::gateway::TopicRegistry registry(std::vector<TopicSpec>{makeTopicSpec(std::string(kTestTopic), 2, kTestTopicId)},
                                          topicDir.path);
    GroupCoordinationService svc(&store, &registry, [] { return true; }, [] { return NodeId{1}; });
    svc.doWork(0);
    auto r = svc.applyTransactionalOffset("g", tp(std::string(kTestTopic), 0), 5, 0);
    if (r == TxnOffsetApply::Replicating) {   // the HW may move on the replica's next duty pass
        rep.doWork(1'000'000);
        r = svc.applyTransactionalOffset("g", tp(std::string(kTestTopic), 0), 5, 1'000'000);
    }
    EXPECT_EQ(r, TxnOffsetApply::Applied);
    EXPECT_EQ(svc.txnOffsetsReplicating(), 0u);
}

// Adversarial review of PR #434 (#358): transactional offsets of several transactions for the same (group, tp),
// interleaved while they replicate.  A record waited on by a transaction that never asks again (its coordinator
// moved away) must not stand in for a later transaction that commits the same offset value after another value was
// committed in between: the later commit must append, and the committed offset must end at its value.
TEST(TxnOffsetApplyRegression, InterleavedTransactionsOnOneGroupPartitionEndAtTheLastCommitAdversarialRegression) {
    using rune::coordination::TxnOffsetApply;
    TempDir dir("txnoff_adv"), topicDir("txnoff_adv_topics");
    rune::storage::Log log(dir.path);
    NullTransport transport;
    PartitionReplica rep(makeSingleNodeConfig(1, /*minIsr=*/1), Role::Leader, /*epoch=*/1,
                         std::vector<NodeId>{1, 2}, std::vector<NodeId>{1, 2}, &log, &transport);
    ReplicatedOffsetStore store(&rep);
    rune::gateway::TopicRegistry registry(std::vector<TopicSpec>{makeTopicSpec(std::string(kTestTopic), 2, kTestTopicId)},
                                          topicDir.path);
    GroupCoordinationService svc(&store, &registry, [] { return true; }, [] { return NodeId{1}; });
    int64_t clock = 0;
    svc.doWork(clock);
    const auto followerFetches = [&] {
        FetchRequestMsg req{};
        req.leaderEpoch = rep.epoch(); req.partition = 0; req.followerId = 2; req.fetchOffset = rep.logEndOffset();
        req.maxBytes = 1 << 20; req.topicId = 0;
        clock += 1'000'000;
        (void)rep.onFetchRequest(req, clock);
        rep.doWork(clock);
    };
    const auto t0 = tp(std::string(kTestTopic), 0);

    // T1 offers 42 and never asks again (its coordinator moved away); T2 commits 50.
    EXPECT_EQ(svc.applyTransactionalOffset("g", t0, 42, clock), TxnOffsetApply::Replicating);
    EXPECT_EQ(svc.applyTransactionalOffset("g", t0, 50, clock), TxnOffsetApply::Replicating);
    followerFetches();
    EXPECT_EQ(svc.applyTransactionalOffset("g", t0, 50, clock), TxnOffsetApply::Applied);
    ASSERT_EQ(store.fetch("g", t0)->offset, 50);

    // T3 commits 42 (the group was rewound).  It must be appended and end as the committed offset.
    const auto leo = rep.logEndOffset();
    TxnOffsetApply r = svc.applyTransactionalOffset("g", t0, 42, clock);
    if (r == TxnOffsetApply::Replicating) {
        followerFetches();
        r = svc.applyTransactionalOffset("g", t0, 42, clock);
    }
    EXPECT_EQ(r, TxnOffsetApply::Applied);
    EXPECT_GT(rep.logEndOffset(), leo) << "T3's commit is appended, not answered by T1's older record";
    EXPECT_EQ(store.fetch("g", t0)->offset, 42) << "the committed offset is the last transaction's";
}

// PR #434 review F2: two transactions waiting on one (group, tp) never re-append over each other.  The older waiter is
// superseded by the newer append: it is answered from its own (older) record, never appends again, so the value
// ends as the last append's - no rewind, no ping-pong.  A later transaction (another token) always appends.
TEST(TxnOffsetApplyRegression, AnOlderWaiterIsSupersededAndNeverAppendsOverTheNewerValueRegression) {
    using rune::coordination::TxnOffsetApply;
    TempDir dir("txnoff_sup"), topicDir("txnoff_sup_topics");
    rune::storage::Log log(dir.path);
    NullTransport transport;
    PartitionReplica rep(makeSingleNodeConfig(1, 1), Role::Leader, 1, std::vector<NodeId>{1, 2},
                         std::vector<NodeId>{1, 2}, &log, &transport);
    ReplicatedOffsetStore store(&rep);
    rune::gateway::TopicRegistry registry(std::vector<TopicSpec>{makeTopicSpec(std::string(kTestTopic), 2, kTestTopicId)},
                                          topicDir.path);
    GroupCoordinationService svc(&store, &registry, [] { return true; }, [] { return NodeId{1}; });
    int64_t clock = 0;
    svc.doWork(clock);
    const auto fetch = [&] {
        FetchRequestMsg req{};
        req.leaderEpoch = rep.epoch(); req.partition = 0; req.followerId = 2; req.fetchOffset = rep.logEndOffset();
        req.maxBytes = 1 << 20; req.topicId = 0;
        clock += 1'000'000;
        (void)rep.onFetchRequest(req, clock);
        rep.doWork(clock);
    };
    const auto t0 = tp(std::string(kTestTopic), 0);
    constexpr uint64_t kTx1 = 11, kTx2 = 13, kTx3 = 15;
    EXPECT_EQ(svc.applyTransactionalOffset("g", t0, 42, clock, kTx1), TxnOffsetApply::Replicating);
    EXPECT_EQ(svc.applyTransactionalOffset("g", t0, 50, clock, kTx2), TxnOffsetApply::Replicating);
    const auto leo = rep.logEndOffset();
    EXPECT_EQ(svc.applyTransactionalOffset("g", t0, 42, clock, kTx1), TxnOffsetApply::Replicating);
    EXPECT_EQ(rep.logEndOffset(), leo) << "the superseded waiter waits on its own record";
    fetch();
    EXPECT_EQ(svc.applyTransactionalOffset("g", t0, 42, clock, kTx1), TxnOffsetApply::Applied);
    EXPECT_EQ(svc.applyTransactionalOffset("g", t0, 50, clock, kTx2), TxnOffsetApply::Applied);
    EXPECT_EQ(rep.logEndOffset(), leo) << "nothing re-appended";
    EXPECT_EQ(store.fetch("g", t0)->offset, 50) << "the last append wins";
    EXPECT_EQ(svc.txnOffsetsReplicating(), 0u);
    // The same value again by a later transaction: appended, not answered by any earlier record.
    EXPECT_EQ(svc.applyTransactionalOffset("g", t0, 50, clock, kTx3), TxnOffsetApply::Replicating);
    EXPECT_GT(rep.logEndOffset(), leo);
    // Bounded without eviction: a full key refuses a NEW transaction (its coordinator retries); no waiter is dropped.
    std::size_t accepted = svc.txnOffsetsReplicating();
    for (uint64_t t = 100; t < 200; ++t)
        if (svc.applyTransactionalOffset("g", t0, static_cast<int64_t>(t), clock, t) == TxnOffsetApply::Replicating) ++accepted;
    EXPECT_EQ(svc.txnOffsetsReplicating(), accepted);
    EXPECT_EQ(svc.txnOffsetsReplicating(), 64u);
    EXPECT_EQ(svc.applyTransactionalOffset("g", t0, 999, clock, 999), TxnOffsetApply::Refused);
    EXPECT_EQ(svc.applyTransactionalOffset("g", t0, 50, clock, kTx3), TxnOffsetApply::Replicating) << "kept, not evicted";
    // The coordinator releases a transaction it no longer completes: its waiter leaves.
    svc.releaseTransactionalOffsets(kTx3);
    EXPECT_EQ(svc.txnOffsetsReplicating(), 63u);
}

// PR #434 re-check: after the offsets replica's leadership changed under several waiters, only the NEWEST appends
// again; the older ones wait for it and are answered by it - the committed offset ends at the newest commit, never
// at an older value re-appended later.
TEST(TxnOffsetApplyRegression, AfterALeadershipChangeOnlyTheNewestWaiterAppendsAgainRegression) {
    using rune::coordination::TxnOffsetApply;
    TempDir dir("txnoff_lc"), topicDir("txnoff_lc_topics");
    rune::storage::Log log(dir.path);
    NullTransport transport;
    PartitionReplica rep(makeSingleNodeConfig(1, 1), Role::Leader, 1, std::vector<NodeId>{1, 2},
                         std::vector<NodeId>{1, 2}, &log, &transport);
    ReplicatedOffsetStore store(&rep);
    rune::gateway::TopicRegistry registry(std::vector<TopicSpec>{makeTopicSpec(std::string(kTestTopic), 2, kTestTopicId)},
                                          topicDir.path);
    GroupCoordinationService svc(&store, &registry, [] { return true; }, [] { return NodeId{1}; });
    int64_t clock = 0;
    svc.doWork(clock);
    const auto fetch = [&] {
        FetchRequestMsg req{};
        req.leaderEpoch = rep.epoch(); req.partition = 0; req.followerId = 2; req.fetchOffset = rep.logEndOffset();
        req.maxBytes = 1 << 20; req.topicId = 0;
        clock += 1'000'000;
        (void)rep.onFetchRequest(req, clock);
        rep.doWork(clock);
    };
    const auto t0 = tp(std::string(kTestTopic), 0);
    EXPECT_EQ(svc.applyTransactionalOffset("g", t0, 42, clock, 1), TxnOffsetApply::Replicating);
    EXPECT_EQ(svc.applyTransactionalOffset("g", t0, 50, clock, 2), TxnOffsetApply::Replicating);
    rep.becomeFollower(2, 2);
    rep.becomeLeader(3, std::vector<NodeId>{1, 2});
    fetch();
    clock += 1'000'000;
    svc.doWork(clock);                          // the offsets are reloaded under the new leadership
    const auto leo = rep.logEndOffset();
    EXPECT_EQ(svc.applyTransactionalOffset("g", t0, 42, clock, 1), TxnOffsetApply::Replicating)
        << "the older waiter waits for the newest";
    EXPECT_EQ(rep.logEndOffset(), leo) << "an older value is never appended again";
    EXPECT_EQ(svc.applyTransactionalOffset("g", t0, 50, clock, 2), TxnOffsetApply::Replicating);
    EXPECT_GT(rep.logEndOffset(), leo) << "the newest appends again";
    fetch();
    EXPECT_EQ(svc.applyTransactionalOffset("g", t0, 42, clock, 1), TxnOffsetApply::Applied);
    EXPECT_EQ(svc.applyTransactionalOffset("g", t0, 50, clock, 2), TxnOffsetApply::Applied);
    EXPECT_EQ(store.fetch("g", t0)->offset, 50);
}

// Re-check of PR #434 round 1 (F2 redesign): two or more concurrent transactional committers on one (group, tp).
namespace {
struct OffRig {
    TempDir dir{"txnoff_rr"}, topicDir{"txnoff_rr_topics"};
    rune::storage::Log log{dir.path};
    NullTransport transport;
    PartitionReplica rep{makeSingleNodeConfig(1, 1), Role::Leader, 1, std::vector<NodeId>{1, 2},
                         std::vector<NodeId>{1, 2}, &log, &transport};
    ReplicatedOffsetStore store{&rep};
    rune::gateway::TopicRegistry registry{std::vector<TopicSpec>{makeTopicSpec(std::string(kTestTopic), 2, kTestTopicId)},
                                          topicDir.path};
    GroupCoordinationService svc{&store, &registry, [] { return true; }, [] { return NodeId{1}; }};
    int64_t clock = 0;
    OffRig() { svc.doWork(clock); }
    void fetch() {
        FetchRequestMsg req{};
        req.leaderEpoch = rep.epoch(); req.partition = 0; req.followerId = 2; req.fetchOffset = rep.logEndOffset();
        req.maxBytes = 1 << 20; req.topicId = 0;
        clock += 1'000'000;
        (void)rep.onFetchRequest(req, clock);
        rep.doWork(clock);
    }
};
}  // namespace

TEST(TxnOffsetApplyRegression, ANewerWaiterAppliedFirstNeverLetsTheOlderOneRewindAdversarialRegression) {
    using rune::coordination::TxnOffsetApply;
    OffRig g;
    const auto t0 = tp(std::string(kTestTopic), 0);
    EXPECT_EQ(g.svc.applyTransactionalOffset("g", t0, 42, g.clock, 11), TxnOffsetApply::Replicating);
    EXPECT_EQ(g.svc.applyTransactionalOffset("g", t0, 50, g.clock, 13), TxnOffsetApply::Replicating);
    g.fetch();
    EXPECT_EQ(g.svc.applyTransactionalOffset("g", t0, 50, g.clock, 13), TxnOffsetApply::Applied);   // newer first
    const auto leo = g.rep.logEndOffset();
    const auto r = g.svc.applyTransactionalOffset("g", t0, 42, g.clock, 11);                        // then older
    EXPECT_EQ(r, TxnOffsetApply::Applied);
    EXPECT_EQ(g.rep.logEndOffset(), leo) << "the older waiter re-appended";
    EXPECT_EQ(g.store.fetch("g", t0)->offset, 50) << "the committed offset was rewound";
}

TEST(TxnOffsetApplyRegression, ANinthWaiterNeverMakesTheEvictedOldestRewindAdversarialRegression) {
    using rune::coordination::TxnOffsetApply;
    OffRig g;
    const auto t0 = tp(std::string(kTestTopic), 0);
    for (uint64_t t = 1; t <= 9; ++t)
        EXPECT_EQ(g.svc.applyTransactionalOffset("g", t0, static_cast<int64_t>(100 + t), g.clock, t), TxnOffsetApply::Replicating);
    const auto leo = g.rep.logEndOffset();
    (void)g.svc.applyTransactionalOffset("g", t0, 101, g.clock, 1);   // the evicted oldest asks again
    EXPECT_EQ(g.rep.logEndOffset(), leo) << "the evicted waiter re-appended its older value";
    g.fetch();
    for (uint64_t t = 1; t <= 9; ++t) (void)g.svc.applyTransactionalOffset("g", t0, static_cast<int64_t>(100 + t), g.clock, t);
    EXPECT_EQ(g.store.fetch("g", t0)->offset, 109) << "the committed offset is the newest commit's";
}

// ===========================================================================
// Issue #433: a plain OffsetCommit is answered only once its __consumer_offsets record is COMMITTED (the high
// watermark passed it), as the transactional path does (#358).  Before the fix the answer left on the local append,
// and a failover of the offsets partition in that window lost an offset the consumer had been told was committed.
// Issue #438: a (group, tp) held by an unfinished transaction is neither fetched nor plainly committed (retriable).
// ===========================================================================
namespace {
struct GroupAnswers {
    std::vector<std::vector<uint8_t>> frames;
    static void sink(void* ctx, int64_t, const uint8_t* f, std::size_t n) {
        static_cast<GroupAnswers*>(ctx)->frames.emplace_back(f, f + n);
    }
};

/// OffRig (two-replica offsets partition) plus one Stable member of `group` bound to session 1.
struct CommitRig : OffRig {
    GroupAnswers answers;
    uint8_t      req[4096]{};
    uint8_t      out[4096]{};
    int64_t      corr = 1;
    std::string  member;
    int32_t      gen = -1;
    explicit CommitRig(const std::string& group) {
        svc.setAnswerSinkForTest(&GroupAnswers::sink, &answers);
        std::size_t n = svc.handleJoinGroup(req, encodeJoinGroup(req, 1, corr++, group, ""), rune::protocol::client::kClientVersion, clock, out, sizeof out);
        const auto j = pc::JoinGroupResponse::decode(out, n);
        if (j) member.assign(reinterpret_cast<const char*>(j->memberId.data()), j->memberId.size());
        (void)svc.handleJoinGroup(req, encodeJoinGroup(req, 1, corr++, group, member), rune::protocol::client::kClientVersion, clock, out, sizeof out);
        for (int i = 0; i < 200 && svc.coordinator(group).state() != GroupState::Stable; ++i) {
            clock += 10'000'000;
            svc.doWork(clock);
        }
        gen = svc.coordinator(group).generationId();
    }
    std::size_t commit(const std::string& group, int64_t offset) {
        return svc.handleOffsetCommit(req, encodeOffsetCommit(req, 1, corr++, group, member, gen, kTestTopicId, 0, offset),
                                      clock, out, sizeof out);
    }
    static int16_t codeOf(const uint8_t* f, std::size_t n) {
        const auto r = pc::OffsetCommitResponse::decode(f, n);
        return r && !r->results.empty() ? r->results[0].errorCode : int16_t{-1};
    }
};
}  // namespace

TEST(OffsetCommitDurabilityRegression, APlainCommitIsAnsweredOnlyOnceItsRecordIsCommittedRegression) {
    CommitRig g("g433");
    ASSERT_FALSE(g.member.empty());
    ASSERT_EQ(g.svc.coordinator("g433").state(), GroupState::Stable);
    EXPECT_EQ(g.commit("g433", 77), 0u) << "answered on the local append, before the record committed";
    EXPECT_TRUE(g.answers.frames.empty());
    EXPECT_EQ(g.svc.parkedOffsetCommits(), 1u);
    g.svc.doWork(g.clock);
    EXPECT_TRUE(g.answers.frames.empty()) << "released before the follower had the record";
    g.fetch();                                   // the follower fetched: the high watermark passes the record
    g.svc.doWork(g.clock);
    ASSERT_EQ(g.answers.frames.size(), 1u);
    EXPECT_EQ(CommitRig::codeOf(g.answers.frames[0].data(), g.answers.frames[0].size()), 0);
    EXPECT_EQ(g.svc.parkedOffsetCommits(), 0u);
    EXPECT_EQ(g.store.fetch("g433", tp(std::string(kTestTopic), 0))->offset, 77);
}

// #471: OffsetFetch must not serve an offset whose __consumer_offsets record is not committed yet.
TEST(OffsetCommitDurabilityRegression, AFetchOfAParkedCommitIsRetriableUntilItsRecordCommitsRegression) {
    struct Version {
        int32_t v = 10;
        static int32_t of(void* ctx, int64_t) noexcept { return static_cast<Version*>(ctx)->v; }
    };
    const auto unstable = static_cast<int16_t>(pc::ErrorCode::UnstableOffsetCommit);
    const auto notCoord = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
    for (const int32_t version : {10, 9}) {
        SCOPED_TRACE(::testing::Message() << "version=" << version);
        const std::string group = "g471v" + std::to_string(version);
        CommitRig g(group);
        Version ver{version};
        g.svc.setClientVersionResolverForTest(&Version::of, &ver);
        const int16_t refused = version == 9 ? notCoord : unstable;
        ASSERT_EQ(g.commit(group, 88), 0u);
        ASSERT_EQ(g.svc.parkedOffsetCommits(), 1u);
        std::size_t n = g.svc.handleOffsetFetch(g.req, encodeOffsetFetch(g.req, 1, g.corr++, group, kTestTopicId, 0),
                                                g.clock, g.out, sizeof g.out);
        auto f = pc::OffsetFetchResponse::decode(g.out, n);
        ASSERT_TRUE(f && f->results.size() == 1u);
        EXPECT_EQ(f->results[0].errorCode, refused) << "an uncommitted offset was handed out";
        EXPECT_EQ(f->results[0].offset, -1);
        n = g.svc.handleOffsetFetch(g.req, encodeOffsetFetchAll(g.req, 1, g.corr++, group), g.clock, g.out, sizeof g.out);
        f = pc::OffsetFetchResponse::decode(g.out, n);
        ASSERT_TRUE(f && f->results.size() == 1u);
        EXPECT_EQ(f->results[0].errorCode, refused);
        EXPECT_EQ(f->results[0].topicId, -1);
        EXPECT_EQ(f->results[0].offset, -1);
        g.fetch();   // the follower has the record: the high watermark passes it
        g.svc.doWork(g.clock);
        n = g.svc.handleOffsetFetch(g.req, encodeOffsetFetch(g.req, 1, g.corr++, group, kTestTopicId, 0), g.clock,
                                    g.out, sizeof g.out);
        f = pc::OffsetFetchResponse::decode(g.out, n);
        ASSERT_TRUE(f && f->results.size() == 1u);
        EXPECT_EQ(f->results[0].errorCode, int16_t{0});
        EXPECT_EQ(f->results[0].offset, 88);
        n = g.svc.handleOffsetFetch(g.req, encodeOffsetFetchAll(g.req, 1, g.corr++, group), g.clock, g.out, sizeof g.out);
        f = pc::OffsetFetchResponse::decode(g.out, n);
        ASSERT_TRUE(f && f->results.size() == 1u);
        EXPECT_EQ(f->results[0].errorCode, int16_t{0});
        EXPECT_EQ(f->results[0].offset, 88);
    }
}

TEST(OffsetCommitDurabilityRegression, AnOffsetsLeaderChangeUnderAParkedCommitNeverAnswersOkRegression) {
    CommitRig g("g433lc");
    ASSERT_EQ(g.commit("g433lc", 88), 0u);
    g.rep.becomeFollower(2, 2);                  // the record may be gone with the old leadership
    g.rep.becomeLeader(3, std::vector<NodeId>{1, 2});
    g.fetch();
    g.clock += 1'000'000;
    g.svc.doWork(g.clock);
    ASSERT_EQ(g.answers.frames.size(), 1u) << "a parked commit is always answered (no hang)";
    EXPECT_EQ(CommitRig::codeOf(g.answers.frames[0].data(), g.answers.frames[0].size()),
              static_cast<int16_t>(pc::ErrorCode::NotCoordinator))
        << "OK was answered for a record that may not have survived the leader change";
}

TEST(OffsetCommitDurabilityRegression, AHeldKeyIsNeitherFetchedNorPlainlyCommittedRegression) {
    CommitRig g("g438");
    struct Probe {
        rune::coordination::TxnOffsetHold hold = rune::coordination::TxnOffsetHold::Held;
        static rune::coordination::TxnOffsetHold of(void* ctx, const std::string&, const TopicPartition&) {
            return static_cast<Probe*>(ctx)->hold;
        }
    } probe;
    g.svc.setTxnOffsetHoldProbe(&Probe::of, &probe);
    const auto unstable = static_cast<int16_t>(pc::ErrorCode::UnstableOffsetCommit);
    std::size_t n = g.svc.handleOffsetFetch(g.req, encodeOffsetFetch(g.req, 1, g.corr++, "g438", kTestTopicId, 0), g.clock,
                                            g.out, sizeof g.out);
    auto f = pc::OffsetFetchResponse::decode(g.out, n);
    ASSERT_TRUE(f && !f->results.empty());
    EXPECT_EQ(f->results[0].errorCode, unstable) << "a held key's committed offset was handed out";
    const auto leo = g.rep.logEndOffset();
    n = g.commit("g438", 5);
    ASSERT_GT(n, 0u) << "a refusal is answered at once";
    EXPECT_EQ(CommitRig::codeOf(g.out, n), unstable);
    EXPECT_EQ(g.rep.logEndOffset(), leo) << "a plain record was appended over a held key";
    probe.hold = rune::coordination::TxnOffsetHold::Unknown;   // the txn coordinator is not loaded here
    n = g.svc.handleOffsetFetch(g.req, encodeOffsetFetch(g.req, 1, g.corr++, "g438", kTestTopicId, 0), g.clock, g.out,
                                sizeof g.out);
    f = pc::OffsetFetchResponse::decode(g.out, n);
    ASSERT_TRUE(f && !f->results.empty());
    EXPECT_EQ(f->results[0].errorCode, unstable) << "unknown hold state must be treated as held";
    probe.hold = rune::coordination::TxnOffsetHold::Free;
    n = g.commit("g438", 6);
    if (n == 0) { g.fetch(); g.svc.doWork(g.clock); }
    EXPECT_EQ(g.store.fetch("g438", tp(std::string(kTestTopic), 0))->offset, 6);
}

// ===========================================================================
// M1 interaction matrix (#433 / #438 / #125): parked OffsetCommit answers, holds × request × client version, and
// the TxnOffsetCommit member fence.
// ===========================================================================
namespace {
std::size_t encodeTwoEntryCommit(uint8_t* buf, int64_t sessionId, int64_t corr, std::string_view group,
                                 std::string_view member, int32_t gen, int32_t topicB, int64_t offset) {
    pc::OffsetCommit msg;
    msg.sessionId = sessionId; msg.correlationId = corr; msg.generationId = gen;
    msg.groupId  = {reinterpret_cast<const uint8_t*>(group.data()), group.size()};
    msg.memberId = {reinterpret_cast<const uint8_t*>(member.data()), member.size()};
    const std::array<pc::OffsetCommitEntry, 2> entries{
        pc::OffsetCommitEntry{kTestTopicId, 0, offset, {}}, pc::OffsetCommitEntry{topicB, 0, offset, {}}};
    return msg.encode(buf, std::span<const pc::OffsetCommitEntry>(entries));
}
int64_t corrOf(const std::vector<uint8_t>& f) {
    const auto r = pc::OffsetCommitResponse::decode(f.data(), f.size());
    return r ? r->correlationId : -1;
}
}  // namespace

TEST(OffsetCommitDurabilityRegression, ParkedAnswersLeaveInRequestOrderEvenWhenALaterOneIsCommittedRegression) {
    CommitRig g("gorder");
    const int64_t c1 = g.corr;
    ASSERT_EQ(g.commit("gorder", 10), 0u);
    g.fetch();   // the first record commits ...
    const int64_t c2 = g.corr;
    // ... but its answer is still parked (doWork has not run): the next commit queues behind it, even though the
    // follower will have its record at once.
    ASSERT_EQ(g.commit("gorder", 11), 0u) << "a commit overtook a parked one on the same coordinator";
    EXPECT_EQ(g.svc.parkedOffsetCommits(), 2u);
    g.fetch();
    g.svc.doWork(g.clock);
    ASSERT_EQ(g.answers.frames.size(), 2u);
    EXPECT_EQ(corrOf(g.answers.frames[0]), c1);
    EXPECT_EQ(corrOf(g.answers.frames[1]), c2);
    EXPECT_EQ(CommitRig::codeOf(g.answers.frames[1].data(), g.answers.frames[1].size()), 0);
}

TEST(OffsetCommitDurabilityRegression, ACoordinatorThatStopsCoordinatingAnswersParkedCommitsNotCoordinatorRegression) {
    CommitRig g("glost");
    ASSERT_EQ(g.commit("glost", 5), 0u);
    g.svc.setLocalLeadership(false);
    g.fetch();   // the record even commits: this node no longer coordinates the group, so it does not say OK
    g.svc.doWork(g.clock);
    ASSERT_EQ(g.answers.frames.size(), 1u) << "a parked commit is always answered";
    EXPECT_EQ(CommitRig::codeOf(g.answers.frames[0].data(), g.answers.frames[0].size()),
              static_cast<int16_t>(pc::ErrorCode::NotCoordinator));
    EXPECT_EQ(g.svc.parkedOffsetCommits(), 0u);
}

TEST(OffsetCommitDurabilityRegression, ALostParkedCommitKeepsItsNonOkEntriesAndRewritesOnlyOkOnesRegression) {
    CommitRig g("gmixed");
    const std::size_t n = g.svc.handleOffsetCommit(
        g.req, encodeTwoEntryCommit(g.req, 1, g.corr++, "gmixed", g.member, g.gen, /*unknown topic*/ 77, 9), g.clock,
        g.out, sizeof g.out);
    ASSERT_EQ(n, 0u);
    g.rep.becomeFollower(2, 2);
    g.rep.becomeLeader(3, std::vector<NodeId>{1, 2});
    g.svc.doWork(g.clock);
    ASSERT_EQ(g.answers.frames.size(), 1u);
    const auto r = pc::OffsetCommitResponse::decode(g.answers.frames[0].data(), g.answers.frames[0].size());
    ASSERT_TRUE(r && r->results.size() == 2u);
    EXPECT_EQ(r->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::NotCoordinator));
    EXPECT_EQ(r->results[1].errorCode, static_cast<int16_t>(pc::ErrorCode::UnknownTopic));
}

TEST(OffsetCommitDurabilityRegression, AFullParkRefusesNotCoordinatorWithoutAppendingRegression) {
    CommitRig g("gfull");
    std::size_t parked = 0;
    for (int i = 0; i < 1000 && g.commit("gfull", 100 + i) == 0; ++i) ++parked;
    ASSERT_EQ(parked, g.svc.parkedOffsetCommits());
    ASSERT_GT(parked, 0u);
    const auto leo = g.rep.logEndOffset();
    const std::size_t n = g.commit("gfull", 5000);
    ASSERT_GT(n, 0u) << "a full park answers at once (never a hang)";
    EXPECT_EQ(CommitRig::codeOf(g.out, n), static_cast<int16_t>(pc::ErrorCode::NotCoordinator));
    EXPECT_EQ(g.rep.logEndOffset(), leo) << "a refused commit was appended";
    EXPECT_GE(g.svc.parkRefusalsForTest(), 1u);
    g.fetch();
    g.svc.doWork(g.clock);
    EXPECT_EQ(g.answers.frames.size(), parked) << "every parked answer is released once its record commits";
    EXPECT_EQ(g.svc.parkedOffsetCommits(), 0u);
    EXPECT_EQ(g.commit("gfull", 6000), 0u) << "room again once the park drained";
}

// #438 × request kind × client version: Free serves; Held / Unknown answer the retriable code (50 to a client >= 10
// or of unknown version, NotCoordinator below) and append nothing.
TEST(OffsetCommitDurabilityRegression, HoldStateByRequestByClientVersionMatrixRegression) {
    using H = rune::coordination::TxnOffsetHold;
    struct Probe {
        H hold = H::Free;
        static H of(void* ctx, const std::string&, const TopicPartition&) { return static_cast<Probe*>(ctx)->hold; }
    };
    struct Version {
        int32_t v = -1;
        static int32_t of(void* ctx, int64_t) noexcept { return static_cast<Version*>(ctx)->v; }
    };
    const auto unstable = static_cast<int16_t>(pc::ErrorCode::UnstableOffsetCommit);
    const auto notCoord = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
    int row = 0;
    for (const H hold : {H::Free, H::Held, H::Unknown}) {
        for (const int32_t version : {-1, 9, 10}) {
            const std::string group = "gm" + std::to_string(row++);
            CommitRig g(group);
            Probe probe{hold};
            Version ver{version};
            g.svc.setClientVersionResolverForTest(&Version::of, &ver);
            ASSERT_EQ(g.commit(group, 40), 0u);   // a committed offset to (not) hand out: committed while Free
            g.fetch();
            g.svc.doWork(g.clock);
            g.svc.setTxnOffsetHoldProbe(&Probe::of, &probe);
            const int16_t refused = version == 9 ? notCoord : unstable;
            SCOPED_TRACE(::testing::Message() << "hold=" << static_cast<int>(hold) << " version=" << version);
            // OffsetFetch of one entry
            std::size_t n = g.svc.handleOffsetFetch(g.req, encodeOffsetFetch(g.req, 1, g.corr++, group, kTestTopicId, 0),
                                                    g.clock, g.out, sizeof g.out);
            auto f = pc::OffsetFetchResponse::decode(g.out, n);
            ASSERT_TRUE(f && f->results.size() == 1u);
            EXPECT_EQ(f->results[0].errorCode, hold == H::Free ? int16_t{0} : refused);
            EXPECT_EQ(f->results[0].offset, hold == H::Free ? int64_t{40} : int64_t{-1});
            // OffsetFetch of every committed offset (fetch-all)
            n = g.svc.handleOffsetFetch(g.req, encodeOffsetFetchAll(g.req, 1, g.corr++, group), g.clock, g.out,
                                        sizeof g.out);
            f = pc::OffsetFetchResponse::decode(g.out, n);
            ASSERT_TRUE(f && f->results.size() == 1u);
            EXPECT_EQ(f->results[0].errorCode, hold == H::Free ? int16_t{0} : refused);
            // plain OffsetCommit
            const auto leo = g.rep.logEndOffset();
            n = g.commit(group, 41);
            if (hold == H::Free) {
                EXPECT_EQ(n, 0u) << "parked until committed";
                EXPECT_EQ(g.rep.logEndOffset(), leo + 1);
            } else {
                ASSERT_GT(n, 0u);
                EXPECT_EQ(CommitRig::codeOf(g.out, n), refused);
                EXPECT_EQ(g.rep.logEndOffset(), leo) << "a plain record was appended over a held key";
            }
        }
    }
}

// #125: GroupCoordinationService::checkTxnOffsetMember applies exactly the OffsetCommit member rules.
TEST(OffsetCommitDurabilityRegression, TxnOffsetMemberFenceMatrixRegression) {
    using EC = pc::ErrorCode;
    CommitRig g("gfence");
    struct Row { const char* what; std::string group; std::string member; int32_t gen; int64_t session; EC want; };
    const std::vector<Row> rows = {
        {"the member at its generation", "gfence", g.member, g.gen, 1, EC::OK},
        {"a stale generation", "gfence", g.member, g.gen - 1, 1, EC::IllegalGeneration},
        {"a future generation", "gfence", g.member, g.gen + 1, 1, EC::IllegalGeneration},
        {"another session's member", "gfence", g.member, g.gen, 2, EC::UnknownMember},
        {"an unknown member", "gfence", "nobody", g.gen, 1, EC::UnknownMember},
        {"an empty member", "gfence", "", g.gen, 1, EC::UnknownMember},
        {"an unknown group", "other", g.member, g.gen, 1, EC::UnknownMember},
    };
    for (const Row& r : rows)
        EXPECT_EQ(g.svc.checkTxnOffsetMember(r.group, r.member, r.gen, r.session), r.want) << r.what;
    g.svc.setLocalLeadership(false);
    EXPECT_EQ(g.svc.checkTxnOffsetMember("gfence", g.member, g.gen, 1), EC::NotCoordinator);
}

// ===========================================================================
// Adversarial review of PR #466 (#433): the parked-answer pool at its bounds, and a lost parked commit re-sent.
// ===========================================================================
namespace {
/// A plain OffsetCommit of `n` entries: entry i is (kTestTopicId, i % 2, base + i), except every 7th names an unknown
/// topic (its answer is UnknownTopic - a marker of the entry's position in the parked results ring).
std::vector<uint8_t> encodeWideCommit(int64_t sessionId, int64_t corr, std::string_view group, std::string_view member,
                                      int32_t gen, std::size_t n, int64_t base) {
    std::vector<pc::OffsetCommitEntry> entries(n);
    for (std::size_t i = 0; i < n; ++i)
        entries[i] = pc::OffsetCommitEntry{i % 7 == 3 ? 4242 : kTestTopicId, static_cast<int32_t>(i % 2),
                                           base + static_cast<int64_t>(i), {}};
    pc::OffsetCommit msg;
    msg.sessionId = sessionId; msg.correlationId = corr; msg.generationId = gen;
    msg.groupId  = {reinterpret_cast<const uint8_t*>(group.data()), group.size()};
    msg.memberId = {reinterpret_cast<const uint8_t*>(member.data()), member.size()};
    std::vector<uint8_t> buf(64 + group.size() + member.size() + n * 32);
    buf.resize(msg.encode(buf.data(), std::span<const pc::OffsetCommitEntry>(entries)));
    return buf;
}
void expectWideAnswer(const std::vector<uint8_t>& f, std::size_t n, int16_t okCode, const char* what) {
    const auto r = pc::OffsetCommitResponse::decode(f.data(), f.size());
    ASSERT_TRUE(r) << what;
    ASSERT_EQ(r->results.size(), n) << what;
    for (std::size_t i = 0; i < n; ++i) {
        const bool unknown = i % 7 == 3;
        ASSERT_EQ(r->results[i].topicId, unknown ? 4242 : kTestTopicId) << what << " entry " << i;
        ASSERT_EQ(r->results[i].partition, static_cast<int32_t>(i % 2)) << what << " entry " << i;
        ASSERT_EQ(r->results[i].errorCode,
                  unknown ? static_cast<int16_t>(pc::ErrorCode::UnknownTopic) : okCode) << what << " entry " << i;
    }
}
}  // namespace

// Never a hang, bounded memory: the results ring (8192) refuses a commit that does not fit WITHOUT appending, every
// parked answer drains, and a commit whose results WRAP the ring is answered entry-for-entry in request order.
TEST(OffsetCommitDurabilityRegression, TheParkedResultsRingRefusesWhenFullAndWrapsInOrderAdversarialRegression) {
    CommitRig g("gring");
    constexpr std::size_t kN = 3000;   // 3 x 3000 > 8192: the third does not fit
    std::vector<uint8_t> out(kN * 16 + 64);
    auto commitWide = [&](int64_t base) {
        const auto req = encodeWideCommit(1, g.corr++, "gring", g.member, g.gen, kN, base);
        return g.svc.handleOffsetCommit(req.data(), req.size(), g.clock, out.data(), out.size());
    };
    ASSERT_EQ(commitWide(1'000), 0u);
    ASSERT_EQ(commitWide(10'000), 0u);
    EXPECT_EQ(g.svc.parkedOffsetCommits(), 2u);
    const auto leo = g.rep.logEndOffset();
    const std::size_t n = commitWide(20'000);
    ASSERT_GT(n, 0u) << "a commit that does not fit the results ring was parked (or dropped)";
    EXPECT_EQ(g.rep.logEndOffset(), leo) << "a refused commit appended records";
    {
        const auto r = pc::OffsetCommitResponse::decode(out.data(), n);
        ASSERT_TRUE(r && r->results.size() == kN);
        for (const auto& e : r->results)
            ASSERT_EQ(e.errorCode, static_cast<int16_t>(pc::ErrorCode::NotCoordinator)) << "a refusal must be retriable";
    }
    g.fetch();
    g.svc.doWork(g.clock);
    ASSERT_EQ(g.answers.frames.size(), 2u);
    expectWideAnswer(g.answers.frames[0], kN, 0, "first");
    expectWideAnswer(g.answers.frames[1], kN, 0, "second");
    g.answers.frames.clear();
    // The ring's head is now at 6000: this commit's 3000 results wrap past 8192.
    ASSERT_EQ(commitWide(30'000), 0u);
    g.fetch();
    g.svc.doWork(g.clock);
    ASSERT_EQ(g.answers.frames.size(), 1u);
    expectWideAnswer(g.answers.frames[0], kN, 0, "wrapped");
    // The last known-topic entry of each partition is the committed offset (never an older one of the request).
    // Entry 2999 (partition 1) names the unknown topic (2999 % 7 == 3): partition 1's last is 2997, partition 0's 2998.
    EXPECT_EQ(g.store.fetch("gring", tp(std::string(kTestTopic), 1))->offset, 30'000 + 2'997);
    EXPECT_EQ(g.store.fetch("gring", tp(std::string(kTestTopic), 0))->offset, 30'000 + 2'998);
    EXPECT_EQ(g.svc.parkedOffsetCommits(), 0u);
}

// The same commit sent twice around a lost park: the first answer is NotCoordinator (the offsets leader changed), the
// re-sent one is OK only once committed, and the committed offset never moves back to the earlier commit's.
TEST(OffsetCommitDurabilityRegression, ACommitResentAfterALostParkIsOkOnceCommittedAndNeverMovesBackAdversarialRegression) {
    CommitRig g("gresend");
    ASSERT_EQ(g.commit("gresend", 50), 0u);
    g.fetch();
    g.svc.doWork(g.clock);
    ASSERT_EQ(g.answers.frames.size(), 1u);
    ASSERT_EQ(CommitRig::codeOf(g.answers.frames[0].data(), g.answers.frames[0].size()), 0);
    g.answers.frames.clear();

    ASSERT_EQ(g.commit("gresend", 88), 0u);
    ASSERT_EQ(g.commit("gresend", 88), 0u) << "the duplicate queues behind the first";
    g.rep.becomeFollower(2, 2);
    g.rep.becomeLeader(3, std::vector<NodeId>{1, 2});
    g.svc.doWork(g.clock);
    ASSERT_EQ(g.answers.frames.size(), 2u) << "both parked answers are released on the leader change";
    for (const auto& f : g.answers.frames)
        EXPECT_EQ(CommitRig::codeOf(f.data(), f.size()), static_cast<int16_t>(pc::ErrorCode::NotCoordinator));
    g.answers.frames.clear();
    g.fetch();             // the new leadership commits its tail: the coordinator reloads the offsets index
    g.svc.doWork(g.clock);
    EXPECT_EQ(g.store.fetch("gresend", tp(std::string(kTestTopic), 0))->offset, 88) << "the reloaded index";

    {
        const std::size_t n = g.commit("gresend", 88);   // the client's retry
        ASSERT_EQ(n, 0u) << "answered inline: " << CommitRig::codeOf(g.out, n);
    }
    g.svc.doWork(g.clock);
    EXPECT_TRUE(g.answers.frames.empty()) << "OK before the retried record committed under the new leadership";
    g.fetch();
    g.svc.doWork(g.clock);
    ASSERT_EQ(g.answers.frames.size(), 1u);
    EXPECT_EQ(CommitRig::codeOf(g.answers.frames[0].data(), g.answers.frames[0].size()), 0);
    EXPECT_EQ(g.store.fetch("gresend", tp(std::string(kTestTopic), 0))->offset, 88);
}

// #213: a JoinGroup's sessionTimeoutMs is clamped to the coordinator's maximum; it also sets the rebalance timeout.
TEST(GroupCoordinationServiceRegression, SessionTimeoutAboveTheMaximumIsClampedRegression) {
    for (const int32_t maxMs : {GroupCoordinationService::kDefaultMaxSessionTimeoutMs, 10'000}) {
        SCOPED_TRACE(::testing::Message() << "max=" << maxMs);
        OffRig g;
        g.svc.setMaxSessionTimeoutMs(maxMs);
        uint8_t req[4096]{}, out[4096]{};
        const std::string group = "g213";
        std::size_t n = g.svc.handleJoinGroup(req, encodeJoinGroup(req, 1, 1, group, "", INT32_MAX), pc::kClientVersion, g.clock, out, sizeof out);
        const auto j = pc::JoinGroupResponse::decode(out, n);
        ASSERT_TRUE(j.has_value());
        const std::string member(reinterpret_cast<const char*>(j->memberId.data()), j->memberId.size());
        (void)g.svc.handleJoinGroup(req, encodeJoinGroup(req, 1, 2, group, member, INT32_MAX), pc::kClientVersion, g.clock, out, sizeof out);
        EXPECT_EQ(g.svc.coordinator(group).rebalanceTimeoutMs(), maxMs) << "an unbounded session timeout set the rebalance timeout";
        for (int i = 0; i < 200 && g.svc.coordinator(group).state() != GroupState::Stable; ++i) {
            g.clock += 10'000'000;
            g.svc.doWork(g.clock);
        }
        ASSERT_EQ(g.svc.coordinator(group).state(), GroupState::Stable);
        ASSERT_EQ(g.svc.coordinator(group).memberCount(), 1u);
        const int64_t t0 = g.clock;
        g.clock = t0 + (static_cast<int64_t>(maxMs) - 1'000) * 1'000'000;   // just before the deadline, no heartbeat
        g.svc.doWork(g.clock);
        EXPECT_EQ(g.svc.coordinator(group).memberCount(), 1u);
        g.clock = t0 + (static_cast<int64_t>(maxMs) + 1'000) * 1'000'000;   // just past it
        g.svc.doWork(g.clock);
        EXPECT_EQ(g.svc.coordinator(group).memberCount(), 0u) << "the silent member outlived the maximum session timeout";
    }
}

// ===========================================================================
// Adversarial review of PR #497 (#471 / #213)
// ===========================================================================
namespace {
std::size_t encodeOffsetFetchTwo(uint8_t* buf, int64_t corr, std::string_view group, int32_t p0, int32_t p1) {
    pc::OffsetFetch msg;
    msg.sessionId     = 1;
    msg.correlationId = corr;
    msg.groupId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(group.data()), group.size());
    const std::array<pc::OffsetFetchEntry, 2> entries{pc::OffsetFetchEntry{kTestTopicId, p0},
                                                      pc::OffsetFetchEntry{kTestTopicId, p1}};
    return msg.encode(buf, std::span<const pc::OffsetFetchEntry>(entries));
}
}  // namespace

// #471: a committed value overwritten by a parked commit is never served as the new value (nor the old one as OK),
// a sibling key that IS committed is still served, and a fetch-all with one unstable key is the retriable sentinel.
TEST(OffsetCommitDurabilityRegression, AParkedOverwriteIsNeverServedButItsCommittedSiblingIsAdversarialRegression) {
    const std::string group = "g471adv";
    CommitRig g(group);
    const auto unstable = static_cast<int16_t>(pc::ErrorCode::UnstableOffsetCommit);
    ASSERT_EQ(g.commit(group, 50), 0u);
    ASSERT_EQ(g.svc.handleOffsetCommit(g.req, encodeOffsetCommit(g.req, 1, g.corr++, group, g.member, g.gen,
                                                                 kTestTopicId, 1, 60),
                                       g.clock, g.out, sizeof g.out), 0u);
    g.fetch();
    g.svc.doWork(g.clock);
    ASSERT_EQ(g.svc.parkedOffsetCommits(), 0u);
    ASSERT_EQ(g.commit(group, 88), 0u);   // overwrite of p0: parked, its record above the high watermark
    ASSERT_EQ(g.svc.parkedOffsetCommits(), 1u);

    std::size_t n = g.svc.handleOffsetFetch(g.req, encodeOffsetFetchTwo(g.req, g.corr++, group, 0, 1), g.clock, g.out,
                                            sizeof g.out);
    auto f = pc::OffsetFetchResponse::decode(g.out, n);
    ASSERT_TRUE(f && f->results.size() == 2u);
    EXPECT_EQ(f->results[0].partition, 0);
    EXPECT_EQ(f->results[0].errorCode, unstable) << "the parked overwrite (or the superseded value) was served";
    EXPECT_EQ(f->results[0].offset, -1);
    EXPECT_EQ(f->results[1].partition, 1);
    EXPECT_EQ(f->results[1].errorCode, int16_t{0}) << "a committed sibling key was refused";
    EXPECT_EQ(f->results[1].offset, 60);

    n = g.svc.handleOffsetFetch(g.req, encodeOffsetFetchAll(g.req, 1, g.corr++, group), g.clock, g.out, sizeof g.out);
    f = pc::OffsetFetchResponse::decode(g.out, n);
    ASSERT_TRUE(f && f->results.size() == 1u) << "a fetch-all listed keys while one of them was unstable";
    EXPECT_EQ(f->results[0].topicId, -1);
    EXPECT_EQ(f->results[0].errorCode, unstable);

    g.fetch();
    g.svc.doWork(g.clock);
    n = g.svc.handleOffsetFetch(g.req, encodeOffsetFetchAll(g.req, 1, g.corr++, group), g.clock, g.out, sizeof g.out);
    f = pc::OffsetFetchResponse::decode(g.out, n);
    ASSERT_TRUE(f && f->results.size() == 2u);
    for (const auto r : f->results) {
        EXPECT_EQ(r.errorCode, int16_t{0});
        EXPECT_EQ(r.offset, r.partition == 0 ? 88 : 60);
    }
}

// #471: the offsets partition changes leader between the commit's append and its high-watermark advance.  Until the
// new leadership has rebuilt its index from a committed log, the key is never served; once it has, the surviving
// record's value is served as committed.
TEST(OffsetCommitDurabilityRegression, ALeaderChangeBeforeTheHighWatermarkNeverServesTheParkedValueAdversarialRegression) {
    const std::string group = "g471lc";
    CommitRig g(group);
    ASSERT_EQ(g.commit(group, 50), 0u);
    g.fetch();
    g.svc.doWork(g.clock);
    ASSERT_EQ(g.commit(group, 88), 0u);
    g.rep.becomeFollower(2, 2);
    g.rep.becomeLeader(3, std::vector<NodeId>{1, 2});
    g.clock += 1'000'000;
    g.svc.doWork(g.clock);
    for (const bool all : {false, true}) {
        const std::size_t len = all ? encodeOffsetFetchAll(g.req, 1, g.corr++, group)
                                    : encodeOffsetFetch(g.req, 1, g.corr++, group, kTestTopicId, 0);
        const std::size_t n = g.svc.handleOffsetFetch(g.req, len, g.clock, g.out, sizeof g.out);
        const auto f = pc::OffsetFetchResponse::decode(g.out, n);
        ASSERT_TRUE(f && f->results.size() == 1u);
        EXPECT_NE(f->results[0].errorCode, int16_t{0}) << (all ? "fetch-all" : "fetch") << " served before the rebuild";
        EXPECT_EQ(f->results[0].offset, -1);
    }
    for (int i = 0; i < 20; ++i) { g.fetch(); g.svc.doWork(g.clock); }
    const std::size_t n = g.svc.handleOffsetFetch(g.req, encodeOffsetFetch(g.req, 1, g.corr++, group, kTestTopicId, 0),
                                                  g.clock, g.out, sizeof g.out);
    const auto f = pc::OffsetFetchResponse::decode(g.out, n);
    ASSERT_TRUE(f && f->results.size() == 1u);
    EXPECT_EQ(f->results[0].errorCode, int16_t{0});
    EXPECT_EQ(f->results[0].offset, 88) << "the record survived the leader change and is committed now";
}

// #213: every bound of a JoinGroup's sessionTimeoutMs, for a new member and a re-joining one, against the default
// maximum and one below the 30 s default: the member's session timeout and the group's rebalance timeout never exceed
// the maximum, and a missing (<= 0) value takes the default, itself clamped.
TEST(GroupCoordinationServiceRegression, JoinGroupSessionTimeoutBoundsTableAdversarialRegression) {
    for (const int32_t maxMs : {GroupCoordinationService::kDefaultMaxSessionTimeoutMs, 10'000}) {
        for (const int32_t req : {INT32_MIN, -1, 0, 1, maxMs - 1, maxMs, maxMs + 1, INT32_MAX}) {
            SCOPED_TRACE(::testing::Message() << "max=" << maxMs << " requested=" << req);
            const int32_t want = req <= 0 ? std::min(30'000, maxMs) : std::min(req, maxMs);
            OffRig g;
            g.svc.setMaxSessionTimeoutMs(maxMs);
            uint8_t rq[4096]{}, out[4096]{};
            const std::string group = "g213adv";
            std::size_t n = g.svc.handleJoinGroup(rq, encodeJoinGroup(rq, 1, 1, group, "", 5'000), pc::kClientVersion, g.clock, out, sizeof out);
            const auto j = pc::JoinGroupResponse::decode(out, n);
            ASSERT_TRUE(j.has_value());
            const std::string member(reinterpret_cast<const char*>(j->memberId.data()), j->memberId.size());
            // A brand-new member asking for `req` (another session, same group).
            n = g.svc.handleJoinGroup(rq, encodeJoinGroup(rq, 2, 2, group, "", req), pc::kClientVersion, g.clock, out, sizeof out);
            ASSERT_TRUE(pc::JoinGroupResponse::decode(out, n).has_value());
            EXPECT_EQ(g.svc.coordinator(group).rebalanceTimeoutMs(), want) << "new member";
            // The first member re-joins asking for `req`.
            n = g.svc.handleJoinGroup(rq, encodeJoinGroup(rq, 1, 3, group, member, req), pc::kClientVersion, g.clock, out, sizeof out);
            ASSERT_TRUE(pc::JoinGroupResponse::decode(out, n).has_value());
            EXPECT_EQ(g.svc.coordinator(group).rebalanceTimeoutMs(), want) << "re-join";
            for (const auto& m : g.svc.coordinator(group).members()) {
                EXPECT_LE(m.sessionTimeoutMs, maxMs) << m.memberId;
                EXPECT_GT(m.sessionTimeoutMs, 0) << m.memberId;
                if (m.memberId == member) EXPECT_EQ(m.sessionTimeoutMs, want) << "re-joined member";
            }
        }
    }
}

// #213: a coordinator maximum of 0 or below is no maximum at all - it never turns every join into a 0 ms session.
TEST(GroupCoordinationServiceRegression, ANonPositiveMaxSessionTimeoutKeepsTheDefaultAdversarialRegression) {
    for (const int32_t bad : {0, -1, INT32_MIN}) {
        SCOPED_TRACE(::testing::Message() << "max=" << bad);
        OffRig g;
        g.svc.setMaxSessionTimeoutMs(bad);
        EXPECT_EQ(g.svc.maxSessionTimeoutMs(), GroupCoordinationService::kDefaultMaxSessionTimeoutMs);
        uint8_t rq[4096]{}, out[4096]{};
        (void)g.svc.handleJoinGroup(rq, encodeJoinGroup(rq, 1, 1, "g213neg", "", 5'000), pc::kClientVersion, g.clock, out, sizeof out);
        EXPECT_EQ(g.svc.coordinator("g213neg").rebalanceTimeoutMs(), 5'000);
    }
}
