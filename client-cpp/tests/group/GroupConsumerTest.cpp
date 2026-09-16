// tests/group/GroupConsumerTest.cpp
//
// CG-5 — GroupConsumer unit tests.
//
// Test strategy:
//   We test GroupConsumerHub directly (state machine, records routing,
//   callback dispatch) without the full Aeron transport stack.
//   The hub receives frames via onJoinGroupResponse / onSyncGroupResponse /
//   onHeartbeatResponse / onRecords / onAssign — all runner-thread calls.
//   We simulate the runner thread inline (single-threaded tests).
//
// Test matrix:
//   1.  ConnectTriggersJoinGroup
//         onConnected() on Unjoined consumer → state becomes Joining.
//
//   2.  JoinGroupResponseOK_SendsSyncGroup
//         JoinGroupResponse{OK} → state becomes Syncing; memberId stored.
//
//   3.  SyncGroupResponseOK_FiresOnAssigned
//         SyncGroupResponse{OK, partitions} → state Stable; onAssigned fired
//         with the correct partitions.
//
//   4.  RecordsForAssignedPartition_DeliveredAndPositionAdvances
//         Records(topicId, partition) in assignment → onRecords fired;
//         position advances to baseOffset+1.
//
//   5.  RecordsForUnassignedPartition_NotDelivered
//         Records for a partition NOT in assignment → onRecords NOT fired.
//
//   6.  CommitEnqueuedAndSent
//         commit() sets commitRequested; doWork sends OffsetCommit (captured).
//
//   7.  CommitSyncResolvesOnOffsetCommitResponse
//         commitSync() returns future; onOffsetCommitResponse resolves it.
//
//   8.  HeartbeatResponseRebalanceInProgress_TransitionsToRejoining
//         HeartbeatResponse{RebalanceInProgress} → state = Rejoining.
//
//   9.  RejoiningState_DoWorkSendsJoinGroup
//         Rejoining consumer + doWork → JoinGroup sent (captured).
//
//  10.  AssignFrame_FiresRevokedAndAssigned
//         Assign{newSet} with different partitions → onRevoked(removed),
//         onAssigned(added) fired with correct partition lists.
//
//  11.  AssignFrameEmpty_RevokesAll
//         Assign{} (empty) → onRevoked(all) fired; assignment becomes empty.
//
//  12.  CloseDeactivates_NoDispatchAfterClose
//         close() → active() false; subsequent onRecords not delivered.
//
//  13.  CommitSyncFailsOnClose
//         commitSync() pending when close() → future throws.
//
//  14.  NotCoordinatorResponse_TransitionsToRejoining
//         HeartbeatResponse{NotCoordinator} → Rejoining.
//
//  15.  AutocommitTimer_SendsOffsetCommit
//         After auto-commit interval elapses, doWork sends OffsetCommit.

#include <rune/client/group/GroupConsumer.h>
#include <rune/client/group/GroupConsumerHub.h>

#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;
using namespace rune::client::group;

namespace rpc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

static constexpr int32_t kTopicId    = 42;
static constexpr int32_t kPartition  = 0;
static constexpr int64_t kSessionId  = 99;

/// Build an encoded JoinGroupResponse{OK, generationId, memberId}.
static std::vector<uint8_t> makeJoinGroupResponseOK(int32_t genId,
                                                      const std::string& memberId)
{
    rpc::JoinGroupResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = 0;
    msg.generationId  = genId;
    msg.isLeader      = 0;
    msg.errorCode     = 0;  // OK
    const auto mbrBytes = reinterpret_cast<const uint8_t*>(memberId.data());
    msg.memberId = std::span<const uint8_t>(mbrBytes, memberId.size());

    const std::size_t sz = msg.encodedSize();
    std::vector<uint8_t> buf(sz);
    msg.encode(buf.data());
    return buf;
}

/// Build an encoded JoinGroupResponse{errorCode}.
static std::vector<uint8_t> makeJoinGroupResponseError(int16_t errorCode)
{
    rpc::JoinGroupResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = 0;
    msg.generationId  = 0;
    msg.isLeader      = 0;
    msg.errorCode     = errorCode;
    msg.memberId      = {};

    const std::size_t sz = msg.encodedSize();
    std::vector<uint8_t> buf(sz);
    msg.encode(buf.data());
    return buf;
}

/// Build an encoded SyncGroupResponse{OK, assignment}.
static std::vector<uint8_t> makeSyncGroupResponseOK(
    const std::vector<rpc::TopicPartition32>& asgn)
{
    rpc::SyncGroupResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = 0;
    msg.errorCode     = 0;  // OK

    const std::size_t sz = rpc::SyncGroupResponse::encodedSizeFor(asgn.size());
    std::vector<uint8_t> buf(sz);
    msg.encode(buf.data(), asgn);
    return buf;
}

/// Build an encoded SyncGroupResponse{errorCode}.
static std::vector<uint8_t> makeSyncGroupResponseError(int16_t errorCode)
{
    rpc::SyncGroupResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = 0;
    msg.errorCode     = errorCode;

    const std::size_t sz = rpc::SyncGroupResponse::encodedSizeFor(0);
    std::vector<uint8_t> buf(sz);
    msg.encode(buf.data(), {});
    return buf;
}

/// Build an encoded HeartbeatResponse{errorCode}.
static std::vector<uint8_t> makeHeartbeatResponse(int16_t errorCode)
{
    rpc::HeartbeatResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = 0;
    msg.errorCode     = errorCode;

    std::vector<uint8_t> buf(rpc::HeartbeatResponse::ENCODED_SIZE);
    msg.encode(buf.data());
    return buf;
}

/// Build an encoded OffsetCommitResponse (empty results).
static std::vector<uint8_t> makeOffsetCommitResponse()
{
    rpc::OffsetCommitResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = 0;

    const std::size_t sz = rpc::OffsetCommitResponse::encodedSizeFor(0);
    std::vector<uint8_t> buf(sz);
    msg.encode(buf.data(), {});
    return buf;
}

/// Build an encoded Assign frame.
static std::vector<uint8_t> makeAssign(int32_t genId,
                                        const std::vector<rpc::TopicPartition32>& asgn)
{
    rpc::Assign msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = 0;
    msg.generationId  = genId;

    const std::size_t sz = rpc::Assign::encodedSizeFor(asgn.size());
    std::vector<uint8_t> buf(sz);
    msg.encode(buf.data(), asgn);
    return buf;
}

/// Build an encoded Records frame (minimal: empty record batch).
static std::vector<uint8_t> makeRecords(int32_t topicId, int32_t partition,
                                         int64_t baseOffset, int64_t subId = 0)
{
    namespace rpcD = rune::protocol::client;
    // Records frame layout (DataMessages templateId=20):
    // MessageHeader(24) + subscriptionId(8) + topicId(4) + partition(4) +
    // baseOffset(8) + varLen(2) + recordBatch
    // We encode directly using the codec adapter.
    // Use an empty record batch (just the var-length prefix = 0).
    constexpr std::size_t kSz = 24 + 8 + 4 + 4 + 8 + 2;
    std::vector<uint8_t> buf(kSz, 0);

    // Use DataMessages Records encode (imported via codec).
    // We use the DataCodec encodeRecords helper.
    rune::protocol::client::Records msg;
    msg.sessionId      = kSessionId;
    msg.correlationId  = 0;
    msg.subscriptionId = subId;
    msg.topicId        = topicId;
    msg.partition      = partition;
    msg.baseOffset     = baseOffset;
    msg.recordBatch    = {};  // empty

    const std::size_t sz = msg.encodedSize();
    buf.resize(sz);
    msg.encode(buf.data());
    return buf;
}

// A captured send call.
struct CapturedFrame {
    std::vector<uint8_t> data;
};

/// Capture sent frames via SendFn.
struct CaptureSink {
    std::vector<CapturedFrame> frames;

    SendFn fn() {
        return [this](const uint8_t* buf, std::size_t len) -> bool {
            CapturedFrame f;
            f.data.assign(buf, buf + len);
            frames.push_back(std::move(f));
            return true;
        };
    }

    void clear() { frames.clear(); }

    /// Return the templateId of the i-th captured frame (from MessageHeader).
    uint16_t templateId(std::size_t i) const {
        if (i >= frames.size()) return 0;
        const auto& d = frames[i].data;
        if (d.size() < 4) return 0;
        // MessageHeader: blockLength(2) + templateId(2) at bytes 0..3
        return static_cast<uint16_t>(d[2]) |
               (static_cast<uint16_t>(d[3]) << 8);
    }
};

// ---------------------------------------------------------------------------
// Helper: run a consumer through Joining → Syncing → Stable with one partition.
// ---------------------------------------------------------------------------
static void bringToStable(GroupConsumerHub& hub, CaptureSink& sink,
                           int32_t topicId, int32_t partition,
                           int32_t genId = 1,
                           const std::string& memberId = "m1")
{
    // Trigger join.
    hub.onConnected(kSessionId, sink.fn());
    sink.clear();

    // Feed JoinGroupResponse{OK}.
    auto jgr = makeJoinGroupResponseOK(genId, memberId);
    hub.onJoinGroupResponse(jgr.data(), jgr.size(), kSessionId, sink.fn());
    sink.clear();

    // Feed SyncGroupResponse{OK, {topicId, partition}}.
    std::vector<rpc::TopicPartition32> asgn = {{topicId, partition}};
    auto sgr = makeSyncGroupResponseOK(asgn);
    hub.onSyncGroupResponse(sgr.data(), sgr.size(), kSessionId, sink.fn());
    sink.clear();
}

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

class GroupConsumerHubTest : public ::testing::Test {
protected:
    void SetUp() override {
        hub_ = std::make_unique<GroupConsumerHub>();
        impl_ = std::make_shared<GroupConsumerImpl>();
        impl_->groupId  = "test-group";
        impl_->topicIds = {kTopicId};
        impl_->cfg.sessionTimeoutMs     = 30000;
        impl_->cfg.autoCommitIntervalMs = 5000;
        impl_->cfg.autoCommit           = true;
        impl_->active.store(true, std::memory_order_relaxed);
        impl_->state = GroupConsumerState::Unjoined;
        hub_->add(impl_);
    }

    void TearDown() override {
        hub_.reset();
        impl_.reset();
    }

    std::unique_ptr<GroupConsumerHub> hub_;
    std::shared_ptr<GroupConsumerImpl> impl_;
    CaptureSink sink_;
};

// ---------------------------------------------------------------------------
// 1. ConnectTriggersJoinGroup
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, ConnectTriggersJoinGroup) {
    hub_->onConnected(kSessionId, sink_.fn());
    EXPECT_EQ(impl_->state, GroupConsumerState::Joining);
    // Should have sent a JoinGroup frame (templateId 32).
    ASSERT_FALSE(sink_.frames.empty());
    EXPECT_EQ(sink_.templateId(0),
              static_cast<uint16_t>(rpc::TemplateId::JoinGroup));
}

// ---------------------------------------------------------------------------
// 2. JoinGroupResponseOK_SendsSyncGroup
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, JoinGroupResponseOK_SendsSyncGroup) {
    hub_->onConnected(kSessionId, sink_.fn());
    sink_.clear();

    auto jgr = makeJoinGroupResponseOK(1, "member-1");
    hub_->onJoinGroupResponse(jgr.data(), jgr.size(), kSessionId, sink_.fn());

    EXPECT_EQ(impl_->state, GroupConsumerState::Syncing);
    EXPECT_EQ(impl_->memberId, "member-1");
    EXPECT_EQ(impl_->generationId, 1);

    // Should have sent SyncGroup (templateId 34).
    ASSERT_FALSE(sink_.frames.empty());
    EXPECT_EQ(sink_.templateId(0),
              static_cast<uint16_t>(rpc::TemplateId::SyncGroup));
}

// ---------------------------------------------------------------------------
// 3. SyncGroupResponseOK_FiresOnAssigned
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, SyncGroupResponseOK_FiresOnAssigned) {
    std::vector<TopicPartition> assigned;
    impl_->cbAssigned = [&](std::vector<TopicPartition> v) {
        assigned = std::move(v);
    };

    bringToStable(*hub_, sink_, kTopicId, kPartition);

    EXPECT_EQ(impl_->state, GroupConsumerState::Stable);
    ASSERT_EQ(assigned.size(), 1u);
    EXPECT_EQ(assigned[0].topicId,   kTopicId);
    EXPECT_EQ(assigned[0].partition, kPartition);
    // Assignment map populated.
    EXPECT_EQ(impl_->assignment.size(), 1u);
}

// ---------------------------------------------------------------------------
// 4. RecordsForAssignedPartition_DeliveredAndPositionAdvances
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, RecordsForAssignedPartition_DeliveredAndPositionAdvances) {
    int recordsCalled = 0;
    int64_t observedBaseOffset = -1;
    impl_->cbRecords = [&](const RecordSet& rs) {
        ++recordsCalled;
        observedBaseOffset = rs.baseOffset;
    };

    bringToStable(*hub_, sink_, kTopicId, kPartition);

    auto recBuf = makeRecords(kTopicId, kPartition, /*baseOffset=*/10);
    codec::RecordsView rv;
    rv.topicId    = kTopicId;
    rv.partition  = kPartition;
    rv.baseOffset = 10;
    rv.sessionId  = kSessionId;

    hub_->onRecords(rv);

    EXPECT_EQ(recordsCalled, 1);
    EXPECT_EQ(observedBaseOffset, 10);

    // Position should advance to baseOffset + 1 = 11.
    const TopicPartition tp{kTopicId, kPartition};
    auto it = impl_->assignment.find(tp);
    ASSERT_NE(it, impl_->assignment.end());
    EXPECT_EQ(it->second, 11);
}

// ---------------------------------------------------------------------------
// 5. RecordsForUnassignedPartition_NotDelivered
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, RecordsForUnassignedPartition_NotDelivered) {
    int recordsCalled = 0;
    impl_->cbRecords = [&](const RecordSet&) { ++recordsCalled; };

    bringToStable(*hub_, sink_, kTopicId, kPartition);

    codec::RecordsView rv;
    rv.topicId    = kTopicId;
    rv.partition  = kPartition + 1;  // DIFFERENT partition — not assigned
    rv.baseOffset = 5;
    rv.sessionId  = kSessionId;

    hub_->onRecords(rv);

    EXPECT_EQ(recordsCalled, 0);  // must NOT be delivered
}

// ---------------------------------------------------------------------------
// 6. CommitEnqueuedAndSent
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, CommitEnqueuedAndSent) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);

    // Set a position so commit has something to send.
    const TopicPartition tp{kTopicId, kPartition};
    impl_->assignment[tp] = 7;

    // Enqueue commit.
    impl_->commitRequested.store(true, std::memory_order_release);

    // doWork should drain the request and send OffsetCommit.
    const int64_t nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    hub_->doWork(nowNs, kSessionId, sink_.fn());

    // Check that an OffsetCommit frame was sent (templateId 40).
    bool found = false;
    for (std::size_t i = 0; i < sink_.frames.size(); ++i) {
        if (sink_.templateId(i) ==
            static_cast<uint16_t>(rpc::TemplateId::OffsetCommit)) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "Expected OffsetCommit frame to be sent";

    // commitRequested must be cleared.
    EXPECT_FALSE(impl_->commitRequested.load(std::memory_order_acquire));
}

// ---------------------------------------------------------------------------
// 7. CommitSyncResolvesOnOffsetCommitResponse
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, CommitSyncResolvesOnOffsetCommitResponse) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);

    // Arm commitSync.
    std::promise<void> prom;
    auto fut = prom.get_future();
    {
        std::lock_guard<std::mutex> lk(impl_->commitSyncMtx);
        impl_->commitSyncPromise = std::move(prom);
        impl_->commitSyncPending = true;
    }

    // Inject OffsetCommitResponse.
    auto ocr = makeOffsetCommitResponse();
    hub_->onOffsetCommitResponse(ocr.data(), ocr.size());

    // Future should be resolved.
    ASSERT_EQ(fut.wait_for(100ms), std::future_status::ready);
    EXPECT_NO_THROW(fut.get());

    // Pending flag cleared.
    std::lock_guard<std::mutex> lk(impl_->commitSyncMtx);
    EXPECT_FALSE(impl_->commitSyncPending);
}

// ---------------------------------------------------------------------------
// 8. HeartbeatResponseRebalanceInProgress_TransitionsToRejoining
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, HeartbeatResponseRebalanceInProgress_TransitionsToRejoining) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);

    auto hbr = makeHeartbeatResponse(
        static_cast<int16_t>(rpc::ErrorCode::RebalanceInProgress));
    hub_->onHeartbeatResponse(hbr.data(), hbr.size(), kSessionId, sink_.fn());

    EXPECT_EQ(impl_->state, GroupConsumerState::Rejoining);
}

// ---------------------------------------------------------------------------
// 9. RejoiningState_DoWorkSendsJoinGroup
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, RejoiningState_DoWorkSendsJoinGroup) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);

    // Force Rejoining state.
    impl_->state = GroupConsumerState::Rejoining;
    sink_.clear();

    const int64_t nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    hub_->doWork(nowNs, kSessionId, sink_.fn());

    // Must have sent JoinGroup (templateId 32).
    bool found = false;
    for (std::size_t i = 0; i < sink_.frames.size(); ++i) {
        if (sink_.templateId(i) ==
            static_cast<uint16_t>(rpc::TemplateId::JoinGroup)) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "Expected JoinGroup frame on Rejoining doWork";
    EXPECT_EQ(impl_->state, GroupConsumerState::Joining);
}

// ---------------------------------------------------------------------------
// 10. AssignFrame_FiresRevokedAndAssigned
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, AssignFrame_FiresRevokedAndAssigned) {
    std::vector<TopicPartition> revoked;
    std::vector<TopicPartition> assigned;
    impl_->cbRevoked  = [&](std::vector<TopicPartition> v) { revoked  = v; };
    impl_->cbAssigned = [&](std::vector<TopicPartition> v) { assigned = v; };

    // Bring to Stable with partition 0.
    bringToStable(*hub_, sink_, kTopicId, 0);

    revoked.clear();
    assigned.clear();
    sink_.clear();

    // Assign frame: remove partition 0, add partition 1.
    auto assignBuf = makeAssign(2, {{kTopicId, 1}});
    hub_->onAssign(assignBuf.data(), assignBuf.size(), kSessionId, sink_.fn());

    // Revoked: partition 0.
    ASSERT_EQ(revoked.size(), 1u);
    EXPECT_EQ(revoked[0].topicId,   kTopicId);
    EXPECT_EQ(revoked[0].partition, 0);

    // Assigned: partition 1.
    ASSERT_EQ(assigned.size(), 1u);
    EXPECT_EQ(assigned[0].topicId,   kTopicId);
    EXPECT_EQ(assigned[0].partition, 1);

    // Assignment map: only partition 1.
    EXPECT_EQ(impl_->assignment.size(), 1u);
    EXPECT_EQ(impl_->assignment.count(TopicPartition{kTopicId, 1}), 1u);
    EXPECT_EQ(impl_->assignment.count(TopicPartition{kTopicId, 0}), 0u);

    // generationId updated.
    EXPECT_EQ(impl_->generationId, 2);
}

// ---------------------------------------------------------------------------
// 11. AssignFrameEmpty_RevokesAll
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, AssignFrameEmpty_RevokesAll) {
    std::vector<TopicPartition> revoked;
    impl_->cbRevoked = [&](std::vector<TopicPartition> v) { revoked = v; };

    bringToStable(*hub_, sink_, kTopicId, kPartition);
    revoked.clear();

    // Assign{} — empty assignment = revoke all.
    auto assignBuf = makeAssign(3, {});
    hub_->onAssign(assignBuf.data(), assignBuf.size(), kSessionId, sink_.fn());

    ASSERT_EQ(revoked.size(), 1u);
    EXPECT_EQ(revoked[0].topicId,   kTopicId);
    EXPECT_EQ(revoked[0].partition, kPartition);

    EXPECT_TRUE(impl_->assignment.empty());
}

// ---------------------------------------------------------------------------
// 12. CloseDeactivates_NoDispatchAfterClose
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, CloseDeactivates_NoDispatchAfterClose) {
    int recordsCalled = 0;
    impl_->cbRecords = [&](const RecordSet&) { ++recordsCalled; };

    bringToStable(*hub_, sink_, kTopicId, kPartition);

    // Close the consumer.
    impl_->active.store(false, std::memory_order_release);

    // Records should not be delivered to an inactive consumer.
    codec::RecordsView rv;
    rv.topicId    = kTopicId;
    rv.partition  = kPartition;
    rv.baseOffset = 0;
    hub_->onRecords(rv);

    EXPECT_EQ(recordsCalled, 0);
}

// ---------------------------------------------------------------------------
// 13. CommitSyncFailsOnClose
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, CommitSyncFailsOnClose) {
    // Create a GroupConsumer handle.
    GroupConsumer gc(impl_);

    bringToStable(*hub_, sink_, kTopicId, kPartition);

    auto fut = gc.commitSync();
    gc.close();

    // Future should be failed (not block forever).
    ASSERT_EQ(fut.wait_for(100ms), std::future_status::ready);
    EXPECT_THROW(fut.get(), std::exception);
}

// ---------------------------------------------------------------------------
// 14. NotCoordinatorResponse_TransitionsToRejoining
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, NotCoordinatorResponse_TransitionsToRejoining) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);

    auto hbr = makeHeartbeatResponse(
        static_cast<int16_t>(rpc::ErrorCode::NotCoordinator));
    hub_->onHeartbeatResponse(hbr.data(), hbr.size(), kSessionId, sink_.fn());

    EXPECT_EQ(impl_->state, GroupConsumerState::Rejoining);
}

// ---------------------------------------------------------------------------
// 15. AutocommitTimer_SendsOffsetCommit
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, AutocommitTimer_SendsOffsetCommit) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);

    // Set a position.
    const TopicPartition tp{kTopicId, kPartition};
    impl_->assignment[tp] = 5;

    // Set auto-commit timer to be in the past.
    impl_->nextAutoCommit = std::chrono::steady_clock::now() - 1ms;

    sink_.clear();

    const int64_t nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    hub_->doWork(nowNs, kSessionId, sink_.fn());

    // Check OffsetCommit was sent.
    bool found = false;
    for (std::size_t i = 0; i < sink_.frames.size(); ++i) {
        if (sink_.templateId(i) ==
            static_cast<uint16_t>(rpc::TemplateId::OffsetCommit)) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "Expected auto-commit OffsetCommit frame";
}

// ---------------------------------------------------------------------------
// 16. JoinGroupResponseError_TransitionsToRejoining
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, JoinGroupResponseError_TransitionsToRejoining) {
    hub_->onConnected(kSessionId, sink_.fn());
    EXPECT_EQ(impl_->state, GroupConsumerState::Joining);
    sink_.clear();

    auto jgr = makeJoinGroupResponseError(
        static_cast<int16_t>(rpc::ErrorCode::RebalanceInProgress));
    hub_->onJoinGroupResponse(jgr.data(), jgr.size(), kSessionId, sink_.fn());

    EXPECT_EQ(impl_->state, GroupConsumerState::Rejoining);
}

// ---------------------------------------------------------------------------
// 17. SyncGroupResponseError_TransitionsToRejoining
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, SyncGroupResponseError_TransitionsToRejoining) {
    hub_->onConnected(kSessionId, sink_.fn());
    sink_.clear();

    // Feed JoinGroup OK → Syncing.
    auto jgr = makeJoinGroupResponseOK(1, "m1");
    hub_->onJoinGroupResponse(jgr.data(), jgr.size(), kSessionId, sink_.fn());
    EXPECT_EQ(impl_->state, GroupConsumerState::Syncing);
    sink_.clear();

    // Feed SyncGroup error.
    auto sgr = makeSyncGroupResponseError(
        static_cast<int16_t>(rpc::ErrorCode::NotCoordinator));
    hub_->onSyncGroupResponse(sgr.data(), sgr.size(), kSessionId, sink_.fn());

    EXPECT_EQ(impl_->state, GroupConsumerState::Rejoining);
}

// ---------------------------------------------------------------------------
// 18. PositionNotRegressOnNewRecords
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, PositionNotRegressOnNewRecords) {
    impl_->cbRecords = [](const RecordSet&) {};
    bringToStable(*hub_, sink_, kTopicId, kPartition);

    const TopicPartition tp{kTopicId, kPartition};

    // Deliver records at offset 10.
    {
        codec::RecordsView rv;
        rv.topicId = kTopicId; rv.partition = kPartition;
        rv.baseOffset = 10;
        hub_->onRecords(rv);
    }
    EXPECT_EQ(impl_->assignment.at(tp), 11);

    // Deliver records at a LOWER baseOffset (stale / redelivery).
    {
        codec::RecordsView rv;
        rv.topicId = kTopicId; rv.partition = kPartition;
        rv.baseOffset = 5;
        hub_->onRecords(rv);
    }
    // Position must NOT regress below 11.
    EXPECT_EQ(impl_->assignment.at(tp), 11);
}

// ---------------------------------------------------------------------------
// 19. GroupConsumer_CloseIsIdempotent
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, GroupConsumer_CloseIsIdempotent) {
    GroupConsumer gc(impl_);
    EXPECT_TRUE(gc.active());

    gc.close();
    EXPECT_FALSE(gc.active());

    // Second close must not crash.
    EXPECT_NO_THROW(gc.close());
    EXPECT_FALSE(gc.active());
}

// ---------------------------------------------------------------------------
// 20. GroupConsumer_MoveSemantics
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, GroupConsumer_MoveSemantics) {
    GroupConsumer gc1(impl_);
    EXPECT_TRUE(gc1.active());

    GroupConsumer gc2(std::move(gc1));
    EXPECT_TRUE(gc2.active());
    // gc1 is in moved-from state; gc2 owns the impl.

    gc2.close();
    EXPECT_FALSE(gc2.active());
    EXPECT_FALSE(impl_->active.load(std::memory_order_acquire));
}

// ---------------------------------------------------------------------------
// Include DataMessages for makeRecords() helper — pull in Records::encode.
// This avoids rewriting the wire encoding in the test file.
// ---------------------------------------------------------------------------
// (already included transitively via DataCodec.h above)
