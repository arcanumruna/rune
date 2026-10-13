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
#include <rune/gateway/TopicRegistry.h>
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
                                   int32_t sessionTimeoutMs = 5000)
{
    pc::JoinGroup msg;
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
    auto outLen = service_->handleJoinGroup(req_, reqLen, clock_, out_, kBufSize);
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
    auto outLen = service_->handleJoinGroup(req_, reqLen, clock_, out_, kBufSize);
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
    auto rejoinOut = service_->handleJoinGroup(req_, rejoinLen, clock_, out_, kBufSize);
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
    auto outLen = service_->handleJoinGroup(req_, reqLen, clock_, out_, kBufSize);
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
    auto outLen = service_->handleJoinGroup(req_, reqLen, clock_, out_, kBufSize);
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
    service_->handleJoinGroup(req_, reqLen, clock_, out_, kBufSize);

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
    auto j1Out = service_->handleJoinGroup(req_, j1Len, clock_, out_, kBufSize);
    ASSERT_GT(j1Out, 0u);
    auto j1Resp = pc::JoinGroupResponse::decode(out_, j1Out);
    ASSERT_TRUE(j1Resp.has_value());
    const std::string m1(
        reinterpret_cast<const char*>(j1Resp->memberId.data()),
        j1Resp->memberId.size());

    // Step 2: Re-join → CompletingRebalance internally.
    clock_ += kStep;
    auto r1Len = encodeJoinGroup(req_, 1, 501, groupId, m1, 5000);
    service_->handleJoinGroup(req_, r1Len, clock_, out_, kBufSize);

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
    auto jOut = service_->handleJoinGroup(req_, jLen, clock_, out_, kBufSize);
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
    auto jOut = service_->handleJoinGroup(req_, jLen, clock_, out_, kBufSize);
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
    auto jOut = service_->handleJoinGroup(req_, jLen, clock_, out_, kBufSize);
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
    auto jOut = service_->handleJoinGroup(req_, jLen, clock_, out_, kBufSize);
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
    auto rjOut = service_->handleJoinGroup(req_, rj, clock_, out_, kBufSize);
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
    auto jOut = service_->handleJoinGroup(req_, jLen, clock_, out_, kBufSize);
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
    auto jOut = svc2.handleJoinGroup(req2, jLen, 0, out2, sizeof(out2));
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
    auto jOut = service_->handleJoinGroup(req_, jLen, clock_, out_, kBufSize);
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
    auto jOut = service_->handleJoinGroup(req_, jLen, clock_, out_, kBufSize);
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
    std::size_t o = service_->handleJoinGroup(req_, n, clock_, out_, kBufSize);
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
        const auto o = service_->handleJoinGroup(req_, n, clock_, out_, kBufSize);
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
        const auto o = service_->handleJoinGroup(req_, n, clock_, out_, kBufSize);
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
    auto jOut = svc2.handleJoinGroup(req_, jLen, clock_, out_, kBufSize);
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
        auto n = service_->handleJoinGroup(req_, l, clock_, out_, kBufSize);
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
    auto r = pc::JoinGroupResponse::decode(out_, service_->handleJoinGroup(req_, l, clock_, out_, kBufSize));
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
