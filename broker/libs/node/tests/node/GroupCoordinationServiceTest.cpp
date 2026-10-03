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
    div.divergingEpoch = replica_->lastFetchedEpoch(); div.divergingEndOffset = 0;
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
