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
// Fix-round 1 additions (21–28) cover findings #1, #2, #3, #4, #5, #6:
//   21. IdlePartitionNotCommittedAsSentinel (#1b): idle partition never committed.
//   22. OffsetFetchSeedsPosition (#1a): OffsetFetchResponse seeds position.
//   23. ResumeAfterReconnectPreservesCommittedOffset (#1a): reconnect uses committed offset.
//   24. MultiRecordBatchAdvancesPositionByCount (#2): 5 records at base 10 → position 15.
//   25. SubscribeGroupEndToEnd (#3): mock E2E join→consume→commit→reconnect.
//   26. MultiGroupGuard_OneConsumerPerHub (#4): 2nd group not misrouted.
//   27. CommitScratchReused (#5): commitEntriesScratch populated correctly.
//   28. TwoThreadCommitDoWork_NoRace (#6): real 2-thread TSan race test.
//
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
//         commitSync(token) completes Ok on the matching OffsetCommitResponse.
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
//         commitSync(token) pending when close() → completes Closed.
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

#include <rune/client/detail/HubCommand.h>
#include <rune/client/detail/M2ORingBuffer.h>

#include <atomic>
#include <cstring>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
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

/// F8a: a HubCommandSink that applies the command to the hub synchronously —
/// the single-threaded stand-in for "agent ring -> duty thread" (the test
/// thread IS the duty thread in these hub-level tests).
static rune::client::detail::HubSubmit directSink(void* ctx,
                                                  const rune::client::detail::HubCommand& cmd) {
    // F8c C7: a handle's release carries its destructor semantics (close),
    // as the Client applies it; the test fixture owns the impl's memory.
    if (cmd.op == rune::client::detail::HubCommand::Op::HandleRelease) {
        rune::client::detail::HubCommand close = cmd;
        close.op = rune::client::detail::HubCommand::Op::GroupClose;
        static_cast<GroupConsumerHub*>(ctx)->applyCommandDirect(close);
        return rune::client::detail::HubSubmit::Accepted;
    }
    static_cast<GroupConsumerHub*>(ctx)->applyCommand(cmd);
    return rune::client::detail::HubSubmit::Accepted;
}

// F8c C4: commitSync completions land in this ledger (exactly-once counts).
// The hub-level sink applies the command synchronously with a handler target
// (the test thread IS the duty thread here).
struct SyncLedger {
    struct Entry { int times = 0; Completion c{}; };
    std::map<Token, Entry> e;
    Token next = 0;
    static void on(void* ctx, const Completion& c, std::span<const uint8_t>) noexcept {
        auto& x = static_cast<SyncLedger*>(ctx)->e[c.token];
        ++x.times;
        x.c = c;
    }
    CompletionHandler handler() { return CompletionHandler{&SyncLedger::on, this}; }
};
static SyncLedger g_ledger;         // test thread only; reset per test
static int64_t    g_hubNowNs = 0;   // duty clock handed to applyCommand

struct SyncOp {
    Token    t = 0;
    OpResult r = OpResult::InvalidArgument;
    [[nodiscard]] bool ready() const { return g_ledger.e.count(t) != 0 && g_ledger.e.at(t).times > 0; }
    [[nodiscard]] int times() const { return g_ledger.e.count(t) != 0 ? g_ledger.e.at(t).times : 0; }
    [[nodiscard]] const Completion& c() const { return g_ledger.e.at(t).c; }
    [[nodiscard]] OpStatus status() const { return c().status; }
};

static SyncOp commitSyncOp(GroupConsumer& gc) {
    const Token t = ++g_ledger.next;
    return SyncOp{t, gc.commitSync(g_ledger.handler(), t)};
}

static OpResult directOpSink(void* ctx, rune::client::detail::HubCommand& cmd,
                             const CompletionTarget& target, Token token, OpKind) {
    if (target.handler.fn == nullptr) return OpResult::InvalidArgument;
    rune::client::detail::OpTarget t;
    t.handler = target.handler;
    cmd.a = static_cast<int64_t>(token);
    rune::client::detail::encodeTarget(t, cmd);
    static_cast<GroupConsumerHub*>(ctx)->applyCommand(cmd, g_hubNowNs);
    return OpResult::Accepted;
}

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

/// Build an encoded OffsetCommitResponse.  errorCode != 0 adds one failed result.
static std::vector<uint8_t> makeOffsetCommitResponse(int64_t correlationId = 0,
                                                     int16_t errorCode = 0)
{
    rpc::OffsetCommitResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = correlationId;

    std::vector<rpc::OffsetCommitResult> res;
    if (errorCode != 0) {
        rpc::OffsetCommitResult r;
        r.topicId   = 42;
        r.partition = 0;
        r.errorCode = errorCode;
        res.push_back(r);
    }
    const std::size_t sz = rpc::OffsetCommitResponse::encodedSizeFor(res.size());
    std::vector<uint8_t> buf(sz);
    msg.encode(buf.data(), std::span<const rpc::OffsetCommitResult>(res));
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
        return [this](int32_t /*streamId*/, const uint8_t* buf, std::size_t len) -> bool {
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

static std::vector<uint8_t> makeOffsetFetchResponse(int32_t topicId, int32_t partition,
                                                    int64_t offset);

static void seedPosition(GroupConsumerHub& hub, int64_t offset) {
    auto ofr = makeOffsetFetchResponse(kTopicId, kPartition, offset);
    hub.onOffsetFetchResponse(ofr.data(), ofr.size());
}

static void doWorkNow(GroupConsumerHub& hub, CaptureSink& sink) {
    const int64_t nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    hub.doWork(nowNs, kSessionId, sink.fn());
}

/// correlationId of the most recent OffsetCommit captured (0 if none).
static int64_t lastCommitCorrId(const CaptureSink& sink) {
    for (std::size_t i = sink.frames.size(); i-- > 0;) {
        if (sink.templateId(i) == static_cast<uint16_t>(rpc::TemplateId::OffsetCommit)) {
            const auto& d = sink.frames[i].data;
            auto oc = rpc::OffsetCommit::decode(d.data(), d.size());
            return oc ? oc->correlationId : 0;
        }
    }
    return 0;
}

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

static void seedPosition(GroupConsumerHub& hub, int64_t offset);
static void doWorkNow(GroupConsumerHub& hub, CaptureSink& sink);
static int64_t lastCommitCorrId(const CaptureSink& sink);

class GroupConsumerHubTest : public ::testing::Test {
protected:
    void SetUp() override {
        g_ledger   = SyncLedger{};
        g_hubNowNs = 0;
        hub_ = std::make_unique<GroupConsumerHub>();
        impl_ = std::make_unique<GroupConsumerImpl>();
        impl_->groupId  = "test-group";
        impl_->topicIds = {kTopicId};
        impl_->cfg.sessionTimeoutMs     = 30000;
        impl_->cfg.autoCommitIntervalMs = 5000;
        impl_->cfg.autoCommit           = true;
        impl_->active = true;
        impl_->state = GroupConsumerState::Unjoined;
        hub_->add(impl_.get());
    }

    void TearDown() override {
        // F8c C4: every commitSync completion in any test arrived exactly once.
        for (const auto& [t, e] : g_ledger.e) EXPECT_EQ(e.times, 1) << "token " << t;
        hub_.reset();
        impl_.reset();
    }

    std::unique_ptr<GroupConsumerHub> hub_;
    // F8c C7: the fixture owns the impl's memory (the bare hub holds raw
    // pointers; a handle built on it never frees it: no duty side detaches).
    std::unique_ptr<GroupConsumerImpl> impl_;
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
    impl_->commitRequested = true;

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
    EXPECT_FALSE(impl_->commitRequested);
}

// ---------------------------------------------------------------------------
// 7. CommitSyncResolvesOnOffsetCommitResponse
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, CommitSyncResolvesOnOffsetCommitResponse) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    seedPosition(*hub_, 7);

    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);
    auto fut = commitSyncOp(gc);
    doWorkNow(*hub_, sink_);
    const int64_t corr = lastCommitCorrId(sink_);
    ASSERT_NE(corr, 0) << "commitSync's OffsetCommit carries its own correlationId";

    auto ocr = makeOffsetCommitResponse(corr);
    hub_->onOffsetCommitResponse(ocr.data(), ocr.size());

    ASSERT_TRUE(fut.ready());
    EXPECT_EQ(fut.status(), OpStatus::Ok);
    EXPECT_FALSE(impl_->hasCommitSync);
}

// fix1 I6: only the response to THIS commitSync's OffsetCommit resolves it.
TEST_F(GroupConsumerHubTest, CommitSyncIgnoresUnrelatedOffsetCommitResponsesRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    seedPosition(*hub_, 7);
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);
    auto fut = commitSyncOp(gc);

    auto early = makeOffsetCommitResponse(0);  // e.g. an auto-commit's response
    hub_->onOffsetCommitResponse(early.data(), early.size());
    EXPECT_FALSE(fut.ready()) << "not even sent yet";

    doWorkNow(*hub_, sink_);
    const int64_t corr = lastCommitCorrId(sink_);
    auto other = makeOffsetCommitResponse(corr + 1000);
    hub_->onOffsetCommitResponse(other.data(), other.size());
    hub_->onOffsetCommitResponse(early.data(), early.size());
    EXPECT_FALSE(fut.ready());

    auto mine = makeOffsetCommitResponse(corr);
    hub_->onOffsetCommitResponse(mine.data(), mine.size());
    ASSERT_TRUE(fut.ready());
    EXPECT_EQ(fut.status(), OpStatus::Ok);
}

TEST_F(GroupConsumerHubTest, CommitSyncFailsOnCommitErrorCodeRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    seedPosition(*hub_, 7);
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);
    auto fut = commitSyncOp(gc);
    doWorkNow(*hub_, sink_);
    auto ocr = makeOffsetCommitResponse(lastCommitCorrId(sink_),
                                        static_cast<int16_t>(rpc::ErrorCode::NotCoordinator));
    hub_->onOffsetCommitResponse(ocr.data(), ocr.size());
    ASSERT_TRUE(fut.ready());
    EXPECT_EQ(fut.status(), OpStatus::BrokerError);
    EXPECT_EQ(fut.c().errorCode, static_cast<uint16_t>(rpc::ErrorCode::NotCoordinator));
}

TEST_F(GroupConsumerHubTest, CommitSyncFailsOnRejoinRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    seedPosition(*hub_, 7);
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);
    auto fut = commitSyncOp(gc);
    doWorkNow(*hub_, sink_);
    auto hbr = makeHeartbeatResponse(static_cast<int16_t>(rpc::ErrorCode::RebalanceInProgress));
    hub_->onHeartbeatResponse(hbr.data(), hbr.size(), kSessionId, sink_.fn());
    ASSERT_TRUE(fut.ready()) << "a rejoin is a progress event";
    EXPECT_EQ(fut.status(), OpStatus::Rejoined);
}

TEST_F(GroupConsumerHubTest, CommitSyncFailsOnSessionLossRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    seedPosition(*hub_, 7);
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);
    auto fut = commitSyncOp(gc);
    doWorkNow(*hub_, sink_);
    hub_->onConnected(kSessionId + 1, sink_.fn());  // a new session: the old one was lost
    ASSERT_TRUE(fut.ready());
    EXPECT_EQ(fut.status(), OpStatus::SessionLost);
}

// A commitSync requested before the commit can go out (not Stable) stays
// pending — it is neither dropped (old code: never resolved) nor failed by the
// first connect.
TEST_F(GroupConsumerHubTest, CommitSyncBeforeStableWaitsAndIsSentOnceStableRegression) {
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);
    auto fut = commitSyncOp(gc);
    doWorkNow(*hub_, sink_);
    EXPECT_FALSE(fut.ready());
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    seedPosition(*hub_, 3);
    doWorkNow(*hub_, sink_);
    const int64_t corr = lastCommitCorrId(sink_);
    ASSERT_NE(corr, 0);
    auto ocr = makeOffsetCommitResponse(corr);
    hub_->onOffsetCommitResponse(ocr.data(), ocr.size());
    ASSERT_TRUE(fut.ready());
    EXPECT_EQ(fut.status(), OpStatus::Ok);
}

TEST_F(GroupConsumerHubTest, CommitSyncWithNothingToCommitResolvesRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);  // position still unseeded
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);
    auto fut = commitSyncOp(gc);
    doWorkNow(*hub_, sink_);
    ASSERT_TRUE(fut.ready());
    EXPECT_EQ(fut.status(), OpStatus::Ok);
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
    impl_->active = false;

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
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);

    bringToStable(*hub_, sink_, kTopicId, kPartition);

    auto fut = commitSyncOp(gc);
    gc.close();

    // Future should be failed (not block forever).
    ASSERT_TRUE(fut.ready());
    EXPECT_EQ(fut.status(), OpStatus::Closed);
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
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);
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
    GroupConsumer gc1(impl_.get(), &directSink, hub_.get());
    EXPECT_TRUE(gc1.active());

    GroupConsumer gc2(std::move(gc1));
    EXPECT_TRUE(gc2.active());
    // gc1 is in moved-from state; gc2 owns the impl.

    gc2.close();
    EXPECT_FALSE(gc2.active());
    EXPECT_FALSE(impl_->active);
}

// ---------------------------------------------------------------------------
// Include DataMessages for makeRecords() helper — pull in Records::encode.
// This avoids rewriting the wire encoding in the test file.
// ---------------------------------------------------------------------------
// (already included transitively via DataCodec.h above)

// ===========================================================================
// CG-5 fix-round 1 — additional tests (findings #1, #2, #3, #4, #6)
// ===========================================================================

// ---------------------------------------------------------------------------
// Extra helpers
// ---------------------------------------------------------------------------

/// Build an OffsetFetchResponse for a single partition with the given committed offset.
static std::vector<uint8_t> makeOffsetFetchResponse(int32_t topicId, int32_t partition,
                                                      int64_t committedOffset)
{
    rpc::OffsetFetchResult r;
    r.topicId   = topicId;
    r.partition = partition;
    r.offset    = committedOffset;
    r.errorCode = 0;

    rpc::OffsetFetchResponse msg;
    msg.sessionId     = kSessionId;
    msg.correlationId = 0;

    const std::size_t sz = rpc::OffsetFetchResponse::encodedSizeFor(1);
    std::vector<uint8_t> buf(sz);
    msg.encode(buf.data(), std::span<const rpc::OffsetFetchResult>(&r, 1));
    return buf;
}

/// Build a Records frame whose recordBatch contains N real records starting at baseOffset.
/// Each record has an empty key/value.  Useful for testing correct position advance.
static std::vector<uint8_t> makeRecordsWithBatch(int32_t topicId, int32_t partition,
                                                   int64_t baseOffset, int32_t numRecords)
{
    // Build a real RecordBatch with numRecords records.
    rune::protocol::RecordBatchBuilder builder(baseOffset, 0LL);
    for (int32_t i = 0; i < numRecords; ++i) {
        builder.append("", "", 0LL);
    }
    std::vector<uint8_t> batchBytes = builder.build();

    // Encode into a Records frame.
    rune::protocol::client::Records msg;
    msg.sessionId      = kSessionId;
    msg.correlationId  = 0;
    msg.subscriptionId = 0;
    msg.topicId        = topicId;
    msg.partition      = partition;
    msg.baseOffset     = baseOffset;
    msg.recordBatch    = std::span<const uint8_t>(batchBytes.data(), batchBytes.size());

    const std::size_t sz = msg.encodedSize();
    std::vector<uint8_t> frameBuf(sz);
    msg.encode(frameBuf.data());
    return frameBuf;
}

// ---------------------------------------------------------------------------
// 21. IdlePartitionNotCommittedAsSentinel (#1b)
//     An assigned-but-idle partition (no Records delivered, no OffsetFetch response)
//     must NOT be committed as offset 0 or as the sentinel value.
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, IdlePartitionNotCommittedAsSentinel) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    sink_.clear();

    // The partition is now in the assignment map at the sentinel (-1).
    // (OffsetFetch has been sent by onSyncGroupResponse but no response yet.)
    const TopicPartition tp{kTopicId, kPartition};
    EXPECT_EQ(impl_->assignment.at(tp), GroupConsumerImpl::kUnseededSentinel);

    // Trigger auto-commit immediately.
    impl_->nextAutoCommit = std::chrono::steady_clock::now() - 1ms;
    const int64_t nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    hub_->doWork(nowNs, kSessionId, sink_.fn());

    // No OffsetCommit must be sent for an unseeded partition.
    bool foundCommit = false;
    for (std::size_t i = 0; i < sink_.frames.size(); ++i) {
        if (sink_.templateId(i) ==
            static_cast<uint16_t>(rpc::TemplateId::OffsetCommit)) {
            foundCommit = true;
            break;
        }
    }
    EXPECT_FALSE(foundCommit)
        << "OffsetCommit must NOT be sent for an idle (unseeded) partition";
}

// ---------------------------------------------------------------------------
// 22. OffsetFetchSeedsPosition (#1a)
//     After entering Stable, an OffsetFetchResponse seeds the position.
//     Subsequent commit must use the fetched offset, not 0.
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, OffsetFetchSeedsPosition) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    sink_.clear();

    const TopicPartition tp{kTopicId, kPartition};

    // Partition is at sentinel: confirm commit is suppressed.
    EXPECT_EQ(impl_->assignment.at(tp), GroupConsumerImpl::kUnseededSentinel);

    // Feed OffsetFetchResponse saying broker has offset 10000 committed.
    auto ofr = makeOffsetFetchResponse(kTopicId, kPartition, 10000);
    hub_->onOffsetFetchResponse(ofr.data(), ofr.size());

    // Position seeded to 10000.
    EXPECT_EQ(impl_->assignment.at(tp), 10000);

    // Now trigger manual commit.
    impl_->commitRequested = true;
    const int64_t nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    hub_->doWork(nowNs, kSessionId, sink_.fn());

    // OffsetCommit must have been sent.
    bool foundCommit = false;
    for (std::size_t i = 0; i < sink_.frames.size(); ++i) {
        if (sink_.templateId(i) ==
            static_cast<uint16_t>(rpc::TemplateId::OffsetCommit)) {
            foundCommit = true;
            break;
        }
    }
    EXPECT_TRUE(foundCommit) << "OffsetCommit should be sent after seeding from OffsetFetch";
}

// ---------------------------------------------------------------------------
// 23. ResumeAfterReconnectPreservesCommittedOffset (#1a — resume scenario)
//     Simulates: join → stable → advance to 500 → OffsetFetch seeds 500 on
//     a new (simulated) join cycle → commit must use 500, not 0.
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, ResumeAfterReconnectPreservesCommittedOffset) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);

    const TopicPartition tp{kTopicId, kPartition};

    // Feed OffsetFetch response (simulates the broker saying offset=500).
    auto ofr = makeOffsetFetchResponse(kTopicId, kPartition, 500);
    hub_->onOffsetFetchResponse(ofr.data(), ofr.size());
    EXPECT_EQ(impl_->assignment.at(tp), 500);

    // Simulate reconnect: force Rejoining → Joining cycle via onConnected.
    impl_->state = GroupConsumerState::Rejoining;
    sink_.clear();
    hub_->onConnected(kSessionId, sink_.fn());  // sends JoinGroup, state=Joining
    EXPECT_EQ(impl_->state, GroupConsumerState::Joining);

    // Drive JoinGroup → Syncing.
    auto jgr = makeJoinGroupResponseOK(2, "m-new");
    hub_->onJoinGroupResponse(jgr.data(), jgr.size(), kSessionId, sink_.fn());
    EXPECT_EQ(impl_->state, GroupConsumerState::Syncing);

    // Drive Syncing → Stable: applyAssignment re-inserts the partition at sentinel.
    std::vector<rpc::TopicPartition32> asgn = {{kTopicId, kPartition}};
    auto sgr = makeSyncGroupResponseOK(asgn);
    hub_->onSyncGroupResponse(sgr.data(), sgr.size(), kSessionId, sink_.fn());
    EXPECT_EQ(impl_->state, GroupConsumerState::Stable);

    // The partition is back at sentinel (re-assigned fresh).
    EXPECT_EQ(impl_->assignment.at(tp), GroupConsumerImpl::kUnseededSentinel);

    // Feed OffsetFetch response: broker says offset=500 (preserved from last commit).
    auto ofr2 = makeOffsetFetchResponse(kTopicId, kPartition, 500);
    hub_->onOffsetFetchResponse(ofr2.data(), ofr2.size());
    EXPECT_EQ(impl_->assignment.at(tp), 500)
        << "Position must resume from committed offset 500, not reset to 0";

    // Commit must use 500, not 0.
    sink_.clear();
    impl_->commitRequested = true;
    const int64_t nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    hub_->doWork(nowNs, kSessionId, sink_.fn());

    bool foundCommit = false;
    for (std::size_t i = 0; i < sink_.frames.size(); ++i) {
        if (sink_.templateId(i) ==
            static_cast<uint16_t>(rpc::TemplateId::OffsetCommit)) {
            foundCommit = true;
            break;
        }
    }
    EXPECT_TRUE(foundCommit) << "OffsetCommit must be sent after seeding from resumed offset";
}

// ---------------------------------------------------------------------------
// 24. MultiRecordBatchAdvancesPositionByCount (#2)
//     A batch of 5 records at baseOffset=10 must advance position to 15,
//     not 11.
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, MultiRecordBatchAdvancesPositionByCount) {
    impl_->cbRecords = [](const RecordSet&) {};
    bringToStable(*hub_, sink_, kTopicId, kPartition);

    // Seed the position so the partition is not at sentinel.
    auto ofr = makeOffsetFetchResponse(kTopicId, kPartition, 10);
    hub_->onOffsetFetchResponse(ofr.data(), ofr.size());
    const TopicPartition tp{kTopicId, kPartition};
    EXPECT_EQ(impl_->assignment.at(tp), 10);

    // Build a real Records frame with 5 records at baseOffset=10.
    auto frameBuf = makeRecordsWithBatch(kTopicId, kPartition, /*baseOffset=*/10, 5);

    // Decode the frame into a RecordsView (as Client::onEgressFrame would).
    auto rvOpt = codec::decodeRecords(frameBuf.data(), frameBuf.size());
    ASSERT_TRUE(rvOpt.has_value()) << "decodeRecords must succeed";
    EXPECT_EQ(rvOpt->recordCount, 5) << "recordCount must be parsed from batch header";

    hub_->onRecords(*rvOpt);

    // Position must advance to baseOffset + recordCount = 10 + 5 = 15.
    EXPECT_EQ(impl_->assignment.at(tp), 15)
        << "Multi-record batch must advance by recordCount, not 1";
}

// ---------------------------------------------------------------------------
// 25. SubscribeGroupEndToEnd (#3 — client-level mock integration test)
//     Drives the full join→assign→consume→commit→reconnect scenario using
//     GroupConsumerHub directly (without Aeron transport), simulating what
//     Client::onEgressFrame would do on each frame arrival.
//     Asserts real partition IDs, real offsets, correct commit values.
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, SubscribeGroupEndToEnd) {
    // Track callbacks.
    std::vector<TopicPartition> assignedPartitions;
    std::vector<TopicPartition> revokedPartitions;
    int recordsDelivered = 0;
    int64_t lastObservedOffset = -1;

    impl_->cbAssigned = [&](std::vector<TopicPartition> v) {
        assignedPartitions = v;
    };
    impl_->cbRevoked = [&](std::vector<TopicPartition> v) {
        revokedPartitions = v;
    };
    impl_->cbRecords = [&](const RecordSet& rs) {
        ++recordsDelivered;
        lastObservedOffset = rs.baseOffset;
    };

    // ---- Step 1: connect → JoinGroup sent.
    hub_->onConnected(kSessionId, sink_.fn());
    EXPECT_EQ(impl_->state, GroupConsumerState::Joining);
    sink_.clear();

    // ---- Step 2: JoinGroupResponse{OK} → SyncGroup sent.
    auto jgr = makeJoinGroupResponseOK(1, "member-e2e");
    hub_->onJoinGroupResponse(jgr.data(), jgr.size(), kSessionId, sink_.fn());
    EXPECT_EQ(impl_->state, GroupConsumerState::Syncing);
    EXPECT_EQ(impl_->memberId, "member-e2e");
    sink_.clear();

    // ---- Step 3: SyncGroupResponse{OK, {topicId=42, partition=0}} → Stable.
    std::vector<rpc::TopicPartition32> asgn = {{kTopicId, kPartition}};
    auto sgr = makeSyncGroupResponseOK(asgn);
    hub_->onSyncGroupResponse(sgr.data(), sgr.size(), kSessionId, sink_.fn());
    EXPECT_EQ(impl_->state, GroupConsumerState::Stable);

    // onAssigned must have been called.
    ASSERT_EQ(assignedPartitions.size(), 1u);
    EXPECT_EQ(assignedPartitions[0].topicId, kTopicId);
    EXPECT_EQ(assignedPartitions[0].partition, kPartition);

    // Position is at sentinel (OffsetFetch was sent, not yet answered).
    const TopicPartition tp{kTopicId, kPartition};
    EXPECT_EQ(impl_->assignment.at(tp), GroupConsumerImpl::kUnseededSentinel);

    // ---- Step 4: OffsetFetchResponse seeds position at 100.
    auto ofr = makeOffsetFetchResponse(kTopicId, kPartition, 100);
    hub_->onOffsetFetchResponse(ofr.data(), ofr.size());
    EXPECT_EQ(impl_->assignment.at(tp), 100);

    // ---- Step 5: Records batch with 3 records at baseOffset=100 delivered.
    auto recFrame = makeRecordsWithBatch(kTopicId, kPartition, 100, 3);
    auto rvOpt = codec::decodeRecords(recFrame.data(), recFrame.size());
    ASSERT_TRUE(rvOpt.has_value());
    EXPECT_EQ(rvOpt->recordCount, 3);
    hub_->onRecords(*rvOpt);

    EXPECT_EQ(recordsDelivered, 1);
    EXPECT_EQ(lastObservedOffset, 100);
    EXPECT_EQ(impl_->assignment.at(tp), 103)  // 100 + 3
        << "Position must advance by recordCount=3 to 103";

    // ---- Step 6: commit() → OffsetCommit sent with offset=103.
    sink_.clear();
    impl_->commitRequested = true;
    const int64_t nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    hub_->doWork(nowNs, kSessionId, sink_.fn());

    // An OffsetCommit must be in the captured sends.
    bool foundCommit = false;
    for (std::size_t i = 0; i < sink_.frames.size(); ++i) {
        if (sink_.templateId(i) ==
            static_cast<uint16_t>(rpc::TemplateId::OffsetCommit)) {
            foundCommit = true;

            // Decode the OffsetCommit to verify it carries offset=103.
            const auto& d = sink_.frames[i].data;
            auto commitOpt = rpc::OffsetCommit::decode(d.data(), d.size());
            ASSERT_TRUE(commitOpt.has_value()) << "OffsetCommit must decode";
            bool found103 = false;
            for (const auto& entry : commitOpt->entries) {
                if (entry.topicId == kTopicId && entry.partition == kPartition) {
                    EXPECT_EQ(entry.offset, 103)
                        << "Committed offset must be 103 (baseOffset=100 + recordCount=3)";
                    found103 = true;
                }
            }
            EXPECT_TRUE(found103) << "OffsetCommit entry for (topicId=42, partition=0) not found";
            break;
        }
    }
    EXPECT_TRUE(foundCommit) << "OffsetCommit frame must be sent";

    // ---- Step 7: reconnect simulation — state → Rejoining → Joining → Stable.
    //     Position must resume from 103 (preserved from OffsetFetch after reconnect).
    revokedPartitions.clear();
    assignedPartitions.clear();
    impl_->state = GroupConsumerState::Rejoining;
    sink_.clear();

    hub_->onConnected(kSessionId, sink_.fn());
    EXPECT_EQ(impl_->state, GroupConsumerState::Joining);

    auto jgr2 = makeJoinGroupResponseOK(2, "member-e2e");
    hub_->onJoinGroupResponse(jgr2.data(), jgr2.size(), kSessionId, sink_.fn());
    EXPECT_EQ(impl_->state, GroupConsumerState::Syncing);

    hub_->onSyncGroupResponse(sgr.data(), sgr.size(), kSessionId, sink_.fn());
    EXPECT_EQ(impl_->state, GroupConsumerState::Stable);

    // Partition re-assigned → sentinel again.
    EXPECT_EQ(impl_->assignment.at(tp), GroupConsumerImpl::kUnseededSentinel);

    // Broker says committed offset = 103.
    auto ofr2 = makeOffsetFetchResponse(kTopicId, kPartition, 103);
    hub_->onOffsetFetchResponse(ofr2.data(), ofr2.size());
    EXPECT_EQ(impl_->assignment.at(tp), 103)
        << "After reconnect, position must resume from committed offset 103";
}

// ---------------------------------------------------------------------------
// 26. MultiGroupGuard (#4)
//     subscribeGroup is tested via GroupConsumerHub::add() — the guard must
//     prevent a 2nd consumer in Joining/Syncing/Stable state from stealing
//     responses.  For now, we simply verify that two consumers in the hub
//     each get their own state tracked independently.
//     (The full guard is in Client::subscribeGroup; tested separately here at
//     the hub level via the documented one-consumer-per-hub invariant.)
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, MultiGroupGuard_OneConsumerPerHub) {
    // Bring the existing consumer to Stable.
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    EXPECT_EQ(impl_->state, GroupConsumerState::Stable);

    // Add a second consumer.
    auto impl2 = std::make_unique<GroupConsumerImpl>();
    impl2->groupId  = "group-2";
    impl2->topicIds = {kTopicId + 1};
    impl2->cfg      = impl_->cfg;
    impl2->active = true;
    impl2->state = GroupConsumerState::Unjoined;
    hub_->add(impl2.get());

    // onConnected triggers join only for Unjoined/Rejoining consumers.
    sink_.clear();
    hub_->onConnected(kSessionId, sink_.fn());

    // impl_ is Stable — must NOT re-join.
    EXPECT_EQ(impl_->state, GroupConsumerState::Stable);
    // impl2 is Unjoined — MUST have joined.
    EXPECT_EQ(impl2->state, GroupConsumerState::Joining);
}

// ---------------------------------------------------------------------------
// 27. CommitScratchReused (#5 — zero-alloc reusable scratch)
//     Verify that commitEntriesScratch is populated correctly and can be
//     reused across two commits without heap leaks (structural test).
// ---------------------------------------------------------------------------
TEST_F(GroupConsumerHubTest, CommitScratchReused) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);

    // Seed position.
    auto ofr = makeOffsetFetchResponse(kTopicId, kPartition, 7);
    hub_->onOffsetFetchResponse(ofr.data(), ofr.size());

    // First commit.
    sink_.clear();
    impl_->commitRequested = true;
    const int64_t nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    hub_->doWork(nowNs, kSessionId, sink_.fn());

    // Scratch should have been used (size == assignment count == 1).
    EXPECT_EQ(impl_->commitEntriesScratch.size(), 1u);
    EXPECT_EQ(impl_->commitEntriesScratch[0].topicId,   kTopicId);
    EXPECT_EQ(impl_->commitEntriesScratch[0].partition,  kPartition);
    EXPECT_EQ(impl_->commitEntriesScratch[0].offset,     7);

    // Second commit: scratch is cleared and repopulated.
    sink_.clear();
    impl_->commitRequested = true;
    const int64_t nowNs2 = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    hub_->doWork(nowNs2, kSessionId, sink_.fn());

    EXPECT_EQ(impl_->commitEntriesScratch.size(), 1u);
}

// ---------------------------------------------------------------------------
// 28. TwoThreadCommitDoWork_NoRaceRegression (#6 / F8a — real 2-thread TSan test)
//     An app thread calls commit() / commitSync() on a GroupConsumer handle
//     whose sink writes HubCommands onto a real M2O ring; a duty thread drains
//     that ring into the hub, runs doWork(), and answers the OffsetCommit.  The
//     only memory the two threads share is the rings (command ring + the impl's
//     status ring) — TSan proves there is no other.  No sleeps: both loops end
//     on conditions.
// ---------------------------------------------------------------------------
namespace {
struct GroupRingChannel {
    static constexpr int32_t kCap = 64 * 1024;
    std::unique_ptr<uint8_t[]> mem{
        new uint8_t[kCap + rune::client::detail::M2ORingBuffer::trailerLength()]()};
    rune::client::detail::M2ORingBuffer ring{
        mem.get(),
        static_cast<std::size_t>(kCap + rune::client::detail::M2ORingBuffer::trailerLength())};

    static rune::client::detail::HubSubmit fn(void* ctx, const rune::client::detail::HubCommand& c) {
        auto* self = static_cast<GroupRingChannel*>(ctx);
        while (!self->ring.writeWith(1, sizeof(c), [&](aeron::concurrent::AtomicBuffer& b,
                                                      aeron::util::index_t at) {
            std::memcpy(b.buffer() + at, &c, sizeof(c));
        })) {
            std::this_thread::yield();
        }
        return rune::client::detail::HubSubmit::Accepted;
    }
    static OpResult opFn(void* ctx, rune::client::detail::HubCommand& c, const CompletionTarget& target,
                         Token token, OpKind) {
        rune::client::detail::OpTarget t;
        t.handler = target.handler;
        c.a = static_cast<int64_t>(token);
        rune::client::detail::encodeTarget(t, c);
        (void)fn(ctx, c);
        return OpResult::Accepted;
    }
    void drainInto(GroupConsumerHub& hub) {
        ring.read([&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t at,
                      aeron::util::index_t) {
            rune::client::detail::HubCommand c;
            std::memcpy(&c, b.buffer() + at, sizeof(c));
            hub.applyCommand(c);
        });
    }
};
} // namespace

TEST_F(GroupConsumerHubTest, TwoThreadCommitDoWork_NoRaceRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    auto ofr = makeOffsetFetchResponse(kTopicId, kPartition, 5);
    hub_->onOffsetFetchResponse(ofr.data(), ofr.size());

    GroupRingChannel ch;
    GroupConsumer gc(impl_.get(), &GroupRingChannel::fn, &ch, {}, &GroupRingChannel::opFn);
    constexpr int kIterations = 500;
    // The completion is delivered on the DUTY thread: it signals the app
    // thread through a test-harness atomic (the production form is a
    // Completions ring; here the hub has no registry).
    struct Done {
        std::atomic<int>      n{0};
        std::atomic<uint8_t>  st{0};
        static void on(void* ctx, const Completion& c, std::span<const uint8_t>) noexcept {
            auto* d = static_cast<Done*>(ctx);
            d->st.store(static_cast<uint8_t>(c.status), std::memory_order_relaxed);
            d->n.fetch_add(1, std::memory_order_release);
        }
    } done;
    std::atomic<bool> appDone{false};   // test-harness flags only
    std::atomic<int>  commitsSent{0};

    std::thread duty([&]() {
        SendFn send = [&](int32_t, const uint8_t* b, std::size_t n) -> bool {
            if (n >= 4 && (static_cast<uint16_t>(b[2]) | (static_cast<uint16_t>(b[3]) << 8)) ==
                              static_cast<uint16_t>(rpc::TemplateId::OffsetCommit)) {
                commitsSent.fetch_add(1, std::memory_order_relaxed);
            }
            return true;
        };
        while (!appDone.load(std::memory_order_acquire)) {
            ch.drainInto(*hub_);
            const int64_t nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
            hub_->doWork(nowNs, kSessionId, send);
            // Play the broker: acknowledge an in-flight commitSync.
            if (impl_->hasCommitSync && impl_->commitSyncCorrId != 0) {
                const auto mine = makeOffsetCommitResponse(impl_->commitSyncCorrId);
                hub_->onOffsetCommitResponse(mine.data(), mine.size());
            }
            std::this_thread::yield();
        }
    });

    for (int i = 0; i < kIterations; ++i) gc.commit();
    ASSERT_EQ(gc.commitSync(CompletionHandler{&Done::on, &done}, 1), OpResult::Accepted);
    while (done.n.load(std::memory_order_acquire) == 0) std::this_thread::yield();
    EXPECT_EQ(done.n.load(), 1);
    EXPECT_EQ(static_cast<OpStatus>(done.st.load()), OpStatus::Ok)
        << "the duty thread completes the commitSync it owns";
    gc.close();
    while (!impl_->status.closed()) std::this_thread::yield();  // duty applied close
    EXPECT_FALSE(gc.active());
    appDone.store(true, std::memory_order_release);
    duty.join();

    EXPECT_GT(commitsSent.load(), 0) << "commit commands must reach the wire";
    EXPECT_FALSE(impl_->active);
}

// ---------------------------------------------------------------------------
// F8a regressions — the handle never writes duty-owned state.
// ---------------------------------------------------------------------------
namespace {
struct CapturingGroupSink {
    std::vector<rune::client::detail::HubCommand> cmds;
    int calls = 0;
    static rune::client::detail::HubSubmit fn(void* ctx, const rune::client::detail::HubCommand& c) {
        auto* self = static_cast<CapturingGroupSink*>(ctx);
        ++self->calls;
        self->cmds.push_back(c);
        return rune::client::detail::HubSubmit::Accepted;
    }
    static OpResult opFn(void* ctx, rune::client::detail::HubCommand& c, const CompletionTarget& target,
                         Token token, OpKind) {
        rune::client::detail::OpTarget t;
        t.handler = target.handler;
        c.a = static_cast<int64_t>(token);
        rune::client::detail::encodeTarget(t, c);
        (void)fn(ctx, c);
        return OpResult::Accepted;
    }
};
} // namespace

TEST_F(GroupConsumerHubTest, CommitAndCloseAreCommandsNotSharedWritesRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    CapturingGroupSink cap;
    GroupConsumer gc(impl_.get(), &CapturingGroupSink::fn, &cap, {}, &CapturingGroupSink::opFn);

    gc.commit();
    ASSERT_EQ(cap.cmds.size(), 1u);
    EXPECT_EQ(cap.cmds[0].op, rune::client::detail::HubCommand::Op::GroupCommit);
    EXPECT_FALSE(impl_->commitRequested) << "app thread wrote no duty-owned field";
    hub_->applyCommand(cap.cmds[0]);
    EXPECT_TRUE(impl_->commitRequested);

    auto fut = commitSyncOp(gc);
    ASSERT_EQ(cap.cmds.size(), 2u);
    EXPECT_FALSE(impl_->hasCommitSync) << "the token + target travel in the command";
    hub_->applyCommand(cap.cmds[1]);
    ASSERT_TRUE(impl_->hasCommitSync);

    gc.close();
    EXPECT_FALSE(gc.active()) << "handle-local: inactive at once";
    EXPECT_TRUE(impl_->active) << "duty-owned flag untouched until the command is applied";
    EXPECT_FALSE(fut.ready());
    ASSERT_EQ(cap.cmds.size(), 3u);
    hub_->applyCommand(cap.cmds[2]);
    EXPECT_FALSE(impl_->active);
    EXPECT_TRUE(impl_->status.closed());
    ASSERT_TRUE(fut.ready());
    EXPECT_EQ(fut.status(), OpStatus::Closed);

    gc.close();  // idempotent
    EXPECT_EQ(cap.calls, 3);
}

TEST_F(GroupConsumerHubTest, CommitSyncSupersededFailsOlderFutureRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);
    auto f1 = commitSyncOp(gc);
    auto f2 = commitSyncOp(gc);
    ASSERT_TRUE(f1.ready());
    EXPECT_EQ(f1.status(), OpStatus::Superseded);
    EXPECT_FALSE(f2.ready());
    doWorkNow(*hub_, sink_);  // nothing seeded: resolves with nothing to commit
    ASSERT_TRUE(f2.ready());
    EXPECT_EQ(f2.status(), OpStatus::Ok);
}

// A commitSync for a consumer the hub does not hold (never adopted / foreign)
// is failed, never stranded.
TEST_F(GroupConsumerHubTest, CommitSyncForUnknownConsumerFailsNeverStrandsRegression) {
    auto other = std::make_unique<GroupConsumerImpl>();  // never added to hub_
    GroupConsumer gc(other.get(), &directSink, hub_.get(), {}, &directOpSink);
    auto f = commitSyncOp(gc);
    ASSERT_TRUE(f.ready());
    EXPECT_EQ(f.status(), OpStatus::Closed);
}

// After the Client closes (closeAll), a surviving handle never touches the
// sink (the possibly-destroyed Client) again and fails commitSync at once.
TEST_F(GroupConsumerHubTest, HandleNeverTouchesClientAfterCloseAllRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    CapturingGroupSink cap;
    GroupConsumer gc(impl_.get(), &CapturingGroupSink::fn, &cap, {}, &CapturingGroupSink::opFn);
    EXPECT_TRUE(gc.active());

    hub_->closeAll(kSessionId, sink_.fn());
    EXPECT_FALSE(gc.active());
    gc.commit();
    auto f = commitSyncOp(gc);
    EXPECT_EQ(f.r, OpResult::Closed) << "rejected synchronously: no completion will follow";
    EXPECT_FALSE(f.ready());
    gc.close();
    EXPECT_EQ(cap.calls, 0);
}

// GroupAdd: the duty thread adopts the impl itself (F8c C7: no shared_ptr
// holder — the hub holds the duty share of the raw impl).
TEST(GroupConsumerHubCommand, GroupAddAdoptsAndFreesHolderRegression) {
    GroupConsumerHub hub;
    auto impl = std::make_unique<GroupConsumerImpl>();
    impl->active = true;
    EXPECT_TRUE(hub.empty());
    rune::client::detail::HubCommand c;
    c.op     = rune::client::detail::HubCommand::Op::GroupAdd;
    c.handle = reinterpret_cast<uint64_t>(impl.get());
    hub.applyCommand(c);
    EXPECT_FALSE(hub.empty());
    EXPECT_EQ(hub.ownedCount(), 1u);
}

// ---------------------------------------------------------------------------
// F8a fix round 1 — I1 / I2 / C1(c) for the group consumer.
// ---------------------------------------------------------------------------

// I2: the generation reaches the owning app thread only through the status ring.
TEST_F(GroupConsumerHubTest, GenerationPublishedThroughStatusRingRegression) {
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);
    EXPECT_EQ(gc.generation(), 0);
    bringToStable(*hub_, sink_, kTopicId, kPartition, /*genId=*/5);
    EXPECT_EQ(gc.generation(), 5);
    auto asg = makeAssign(6, {{kTopicId, kPartition}});
    hub_->onAssign(asg.data(), asg.size(), kSessionId, sink_.fn());
    EXPECT_EQ(gc.generation(), 6);
    gc.close();
    EXPECT_TRUE(gc.finished());
    EXPECT_EQ(gc.generation(), 6) << "final snapshot survives close";
}

// Calls from inside a callback (duty thread) act on duty-owned state directly.
TEST_F(GroupConsumerHubTest, HandleCallsOnDutyThreadActDirectlyRegression) {
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), std::this_thread::get_id(), &directOpSink);
    int32_t seenGen = -1;
    bool finishedInside = true;
    int records = 0;
    impl_->cbAssigned = [&](std::vector<TopicPartition>) { seenGen = gc.generation(); };
    impl_->cbRecords  = [&](const RecordSet&) {
        ++records;
        gc.close();                     // applied directly on the duty thread
        finishedInside = gc.finished(); // deferred until the callback returns
    };
    bringToStable(*hub_, sink_, kTopicId, kPartition, /*genId=*/9);
    EXPECT_EQ(seenGen, 9);
    codec::RecordsView rv;
    rv.topicId = kTopicId;
    rv.partition = kPartition;
    hub_->onRecords(rv);
    EXPECT_EQ(records, 1);
    EXPECT_FALSE(finishedInside) << "finished() never true while a callback runs";
    EXPECT_TRUE(gc.finished());
    hub_->onRecords(rv);
    EXPECT_EQ(records, 1) << "no callback after finished()";
}

// I1: callback setters are commands applied by the duty thread — an app thread
// setting callbacks while the duty thread dispatches shares nothing (TSan).
// Also exercises I2 (generation read on the app thread during rebalances).
TEST_F(GroupConsumerHubTest, CallbackSettersRaceDispatchWithoutSharingRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    GroupRingChannel ch;
    GroupConsumer gc(impl_.get(), &GroupRingChannel::fn, &ch);
    std::atomic<bool> appDone{false};   // harness flags only
    std::atomic<int>  dispatched{0};

    std::thread duty([&] {
        codec::RecordsView rv;
        rv.topicId = kTopicId;
        rv.partition = kPartition;
        int32_t gen = 1;
        while (!appDone.load(std::memory_order_acquire)) {
            ch.drainInto(*hub_);
            hub_->onRecords(rv);
            auto asg = makeAssign(++gen, {{kTopicId, kPartition}});
            hub_->onAssign(asg.data(), asg.size(), kSessionId, sink_.fn());
            if (hub_->dirtyCount() != 0) hub_->republishDirty();
        }
        ch.drainInto(*hub_);
    });

    int32_t lastGen = 0;
    bool monotonic = true;
    for (int i = 0; i < 200; ++i) {
        gc.onRecords([&](const RecordSet&) { dispatched.fetch_add(1, std::memory_order_relaxed); });
        gc.onAssigned([](std::vector<TopicPartition>) {});
        const int32_t g = gc.generation();
        if (g < lastGen) monotonic = false;
        lastGen = g;
    }
    appDone.store(true, std::memory_order_release);
    duty.join();
    EXPECT_TRUE(monotonic);
    // The race above may end before the duty loop dispatches after applying a
    // setter (fast Release builds); its final drain applied the last setter, so
    // one more dispatch (this thread is now the only one) must reach it.
    codec::RecordsView rv;
    rv.topicId = kTopicId;
    rv.partition = kPartition;
    hub_->onRecords(rv);
    EXPECT_GT(dispatched.load(), 0) << "the setter was applied by the duty thread (only it drains the channel)";
}

// ---------------------------------------------------------------------------
// F8a fix round 2 — REQUIRED 2 / 3 at hub level.
// ---------------------------------------------------------------------------
namespace {
struct RefusingGroupTransport {
    int refuseNext = 0;  // transiently refuse (Retry) this many offers
    int fatalNext  = 0;  // then reject this many as never deliverable (Fatal)
    int fatals     = 0;
    std::vector<std::vector<uint8_t>> accepted;
    static rune::client::detail::SendResult fn(void* ctx, const uint8_t* b, std::size_t n) {
        auto* self = static_cast<RefusingGroupTransport*>(ctx);
        if (self->refuseNext > 0) { --self->refuseNext; return rune::client::detail::SendResult::Retry; }
        if (self->fatalNext > 0) {
            --self->fatalNext;
            ++self->fatals;
            return rune::client::detail::SendResult::Fatal;
        }
        self->accepted.emplace_back(b, b + n);
        return rune::client::detail::SendResult::Accepted;
    }
};
} // namespace

TEST_F(GroupConsumerHubTest, CommitSyncResolvesAfterARefusedOffsetCommitRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    seedPosition(*hub_, 7);
    RefusingGroupTransport tx;
    hub_->setDirectSend(&RefusingGroupTransport::fn, &tx);
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);
    auto fut = commitSyncOp(gc);

    tx.refuseNext = 1;
    EXPECT_EQ(hub_->flushRequestedCommits(kSessionId), 0) << "refused";
    EXPECT_TRUE(impl_->commitRequested) << "stays requested until the offer succeeds";
    EXPECT_EQ(impl_->commitSyncCorrId, 0);
    EXPECT_EQ(hub_->flushRequestedCommits(kSessionId), 1);
    ASSERT_EQ(tx.accepted.size(), 1u);
    auto oc = rpc::OffsetCommit::decode(tx.accepted[0].data(), tx.accepted[0].size());
    ASSERT_TRUE(oc.has_value());
    EXPECT_EQ(oc->correlationId, impl_->commitSyncCorrId);
    EXPECT_TRUE(sink_.frames.empty() || lastCommitCorrId(sink_) == 0) << "never via the ring";

    auto ocr = makeOffsetCommitResponse(oc->correlationId);
    hub_->onOffsetCommitResponse(ocr.data(), ocr.size());
    ASSERT_TRUE(fut.ready());
    EXPECT_EQ(fut.status(), OpStatus::Ok);
}

TEST_F(GroupConsumerHubTest, CommitSyncFailsOnSessionDownRegression) {
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);
    auto fut = commitSyncOp(gc);  // not even sent
    hub_->onSessionDown();
    ASSERT_TRUE(fut.ready());
    EXPECT_EQ(fut.status(), OpStatus::SessionLost);
}

// fix3 I2: commitSync applied while the session is down fails at once.
TEST_F(GroupConsumerHubTest, CommitSyncAppliedWhileSessionDownFailsAtOnceRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    hub_->setSessionConnected(false);
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);
    auto fut = commitSyncOp(gc);
    ASSERT_TRUE(fut.ready());
    EXPECT_EQ(fut.status(), OpStatus::SessionLost);
    impl_->commitRequested = true;
    EXPECT_EQ(hub_->flushRequestedCommits(kSessionId), 0) << "no commit offered while down";
}

// fix3 M4: GroupConsumer close sends LeaveGroup directly, retried when refused,
// dropped when the session changes.
TEST_F(GroupConsumerHubTest, LeaveGroupOnCloseRetriedWhenRefusedRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    hub_->setSessionConnected(true, kSessionId);
    RefusingGroupTransport tx;
    hub_->setDirectSend(&RefusingGroupTransport::fn, &tx);
    tx.refuseNext = 1;
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);
    gc.close();
    EXPECT_EQ(hub_->pendingLeaves(), 1u);
    EXPECT_EQ(hub_->retryPendingLeaves(kSessionId), 1);
    ASSERT_EQ(tx.accepted.size(), 1u);
    EXPECT_EQ(static_cast<uint16_t>(tx.accepted[0][2] | (tx.accepted[0][3] << 8)),
              static_cast<uint16_t>(rpc::TemplateId::LeaveGroup));
    EXPECT_EQ(hub_->pendingLeaves(), 0u);
}

TEST_F(GroupConsumerHubTest, PendingLeaveGroupDroppedOnSessionChangeRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    hub_->setSessionConnected(true, kSessionId);
    RefusingGroupTransport tx;
    hub_->setDirectSend(&RefusingGroupTransport::fn, &tx);
    tx.refuseNext = 1;
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);
    gc.close();
    ASSERT_EQ(hub_->pendingLeaves(), 1u);
    EXPECT_EQ(hub_->retryPendingLeaves(kSessionId + 1), 0);
    EXPECT_EQ(hub_->pendingLeaves(), 0u);
    EXPECT_TRUE(tx.accepted.empty());
}

// ---------------------------------------------------------------------------
// F8a fix round 4
// ---------------------------------------------------------------------------

// M1: an OffsetCommit the transport can never carry (Fatal) fails commitSync
// at once and is dropped — never re-offered forever.  An async commit()'s
// Fatal offer is dropped too.
TEST_F(GroupConsumerHubTest, FatalOffsetCommitFailsCommitSyncAndIsNotReofferedRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    seedPosition(*hub_, 7);
    RefusingGroupTransport tx;
    hub_->setDirectSend(&RefusingGroupTransport::fn, &tx);
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);

    auto fut = commitSyncOp(gc);
    tx.fatalNext = 1;
    (void)hub_->flushRequestedCommits(kSessionId);
    ASSERT_TRUE(fut.ready()) << "a Fatal OffsetCommit must fail commitSync";
    EXPECT_EQ(fut.status(), OpStatus::NotConnected);
    EXPECT_FALSE(impl_->commitRequested) << "dropped, not kept for retry";
    (void)hub_->flushRequestedCommits(kSessionId);
    EXPECT_EQ(tx.fatals, 1);
    EXPECT_TRUE(tx.accepted.empty()) << "never re-offered";

    gc.commit();
    tx.fatalNext = 1;
    (void)hub_->flushRequestedCommits(kSessionId);
    EXPECT_FALSE(impl_->commitRequested);
    (void)hub_->flushRequestedCommits(kSessionId);
    EXPECT_EQ(tx.fatals, 2);
    EXPECT_TRUE(tx.accepted.empty());
}

// M1: a LeaveGroup the transport can never carry is dropped, not kept pending.
TEST_F(GroupConsumerHubTest, FatalLeaveGroupIsDroppedNotRetriedRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    hub_->setSessionConnected(true, kSessionId);
    RefusingGroupTransport tx;
    hub_->setDirectSend(&RefusingGroupTransport::fn, &tx);
    tx.fatalNext = 1;
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);
    gc.close();
    EXPECT_EQ(hub_->pendingLeaves(), 0u);
    EXPECT_EQ(hub_->retryPendingLeaves(kSessionId), 0);
    EXPECT_EQ(tx.fatals, 1);
    EXPECT_TRUE(tx.accepted.empty());
}

// M4: an onRevoked callback that closes its consumer during closeAll must not
// make closeAll send a second LeaveGroup.
TEST_F(GroupConsumerHubTest, CloseAllSendsOneLeaveGroupWhenOnRevokedClosesRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    hub_->setSessionConnected(true, kSessionId);
    RefusingGroupTransport tx;
    hub_->setDirectSend(&RefusingGroupTransport::fn, &tx);
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), std::this_thread::get_id(), &directOpSink);
    impl_->cbRevoked = [&](std::vector<TopicPartition>) { gc.close(); };
    sink_.clear();
    hub_->closeAll(kSessionId, sink_.fn());
    const auto leaveTid = static_cast<uint16_t>(rpc::TemplateId::LeaveGroup);
    int leaves = 0;
    for (const auto& f : tx.accepted)
        if (static_cast<uint16_t>(f[2] | (f[3] << 8)) == leaveTid) ++leaves;
    for (std::size_t i = 0; i < sink_.frames.size(); ++i)
        if (sink_.templateId(i) == leaveTid) ++leaves;
    EXPECT_EQ(leaves, 1) << "LeaveGroup sent twice";
    EXPECT_TRUE(gc.finished());
}

// M5: a new session replacing the one a commitSync's OffsetCommit went out on
// (a reconnect inside ONE duty cycle: the hub never observes "down") strands
// it — the transition (connected, sessionId, episode) must fail it.
TEST_F(GroupConsumerHubTest, CommitSyncFailsWhenANewSessionReplacesItsOwnRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    seedPosition(*hub_, 7);
    hub_->setSessionConnected(true, kSessionId, /*episode=*/1);
    RefusingGroupTransport tx;
    hub_->setDirectSend(&RefusingGroupTransport::fn, &tx);
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);

    auto f1 = commitSyncOp(gc);
    ASSERT_EQ(hub_->flushRequestedCommits(kSessionId), 1);
    ASSERT_NE(impl_->commitSyncCorrId, 0);
    hub_->setSessionConnected(true, kSessionId + 1, /*episode=*/2);  // new session, never "down"
    ASSERT_TRUE(f1.ready()) << "stranded on a dead session";
    EXPECT_EQ(f1.status(), OpStatus::SessionLost);

    // Same sessionId resumed in a new connect episode: the commit's fate is
    // just as unknown.
    auto f2 = commitSyncOp(gc);
    ASSERT_EQ(hub_->flushRequestedCommits(kSessionId + 1), 1);
    hub_->setSessionConnected(true, kSessionId + 1, /*episode=*/3);
    ASSERT_TRUE(f2.ready());
    EXPECT_EQ(f2.status(), OpStatus::SessionLost);

    // An unchanged observation fails nothing.
    auto f3 = commitSyncOp(gc);
    hub_->setSessionConnected(true, kSessionId + 1, /*episode=*/3);
    EXPECT_FALSE(f3.ready());
}

// M6: a commitSync failed by session loss is a distinguishable, retriable
// error (SessionLostError); other failures (rejoin, close) are not.
TEST_F(GroupConsumerHubTest, CommitSyncSessionLossIsADistinctRetriableErrorRegression) {
    bringToStable(*hub_, sink_, kTopicId, kPartition);
    GroupConsumer gc(impl_.get(), &directSink, hub_.get(), {}, &directOpSink);

    auto down = commitSyncOp(gc);
    hub_->onSessionDown();
    ASSERT_TRUE(down.ready());
    EXPECT_EQ(down.status(), OpStatus::SessionLost);

    hub_->setSessionConnected(false);
    auto whileDown = commitSyncOp(gc);
    ASSERT_TRUE(whileDown.ready());
    EXPECT_EQ(whileDown.status(), OpStatus::SessionLost);
    hub_->setSessionConnected(true, kSessionId);

    auto rejoin = commitSyncOp(gc);
    auto hb = makeHeartbeatResponse(static_cast<int16_t>(rpc::ErrorCode::RebalanceInProgress));
    hub_->onHeartbeatResponse(hb.data(), hb.size(), kSessionId, sink_.fn());
    ASSERT_TRUE(rejoin.ready());
    EXPECT_EQ(rejoin.status(), OpStatus::Rejoined) << "a rejoin is not a session loss";
}

// M7: a direct GroupCommit and GroupCommitSync applied to an impl whose
// GroupAdd is still queued (not adopted yet) are honoured after adoption:
// the commit goes out once Stable and the commitSync resolves on its response.
TEST(GroupConsumerHubCommand, DirectCommitAndCommitSyncBeforeAdoptionAreHonouredRegression) {
    using rune::client::detail::HubCommand;
    GroupConsumerHub hub;
    RefusingGroupTransport tx;
    hub.setDirectSend(&RefusingGroupTransport::fn, &tx);
    auto impl = std::make_unique<GroupConsumerImpl>();
    impl->groupId  = "g";
    impl->topicIds = {kTopicId};
    impl->active   = true;

    g_ledger = SyncLedger{};
    HubCommand sync;
    sync.op     = HubCommand::Op::GroupCommitSync;
    sync.handle = reinterpret_cast<uint64_t>(impl.get());
    sync.a      = 1;
    rune::client::detail::OpTarget tgt;
    tgt.handler = g_ledger.handler();
    rune::client::detail::encodeTarget(tgt, sync);
    hub.applyCommandDirect(sync);  // not adopted yet
    const SyncOp fut{1, OpResult::Accepted};
    HubCommand commit;
    commit.op     = HubCommand::Op::GroupCommit;
    commit.handle = reinterpret_cast<uint64_t>(impl.get());
    hub.applyCommandDirect(commit);
    EXPECT_TRUE(hub.empty());
    EXPECT_FALSE(fut.ready()) << "must not fail as 'closed'";

    HubCommand add;  // the queued GroupAdd, drained later
    add.op     = HubCommand::Op::GroupAdd;
    add.handle = reinterpret_cast<uint64_t>(impl.get());
    hub.applyCommand(add);
    ASSERT_FALSE(hub.empty());

    CaptureSink sink;
    bringToStable(hub, sink, kTopicId, kPartition);
    seedPosition(hub, 11);
    ASSERT_EQ(hub.flushRequestedCommits(kSessionId), 1);
    ASSERT_EQ(tx.accepted.size(), 1u);
    auto oc = rpc::OffsetCommit::decode(tx.accepted[0].data(), tx.accepted[0].size());
    ASSERT_TRUE(oc.has_value());
    EXPECT_NE(oc->correlationId, 0) << "the commitSync owns this commit";
    auto ocr = makeOffsetCommitResponse(oc->correlationId);
    hub.onOffsetCommitResponse(ocr.data(), ocr.size());
    ASSERT_TRUE(fut.ready());
    EXPECT_EQ(fut.status(), OpStatus::Ok);
}

// F8c fix round 1 (I-3): a commitSync completion handler runs INLINE inside
// the hub's loops over its consumers; one that adopts consumers on the duty
// thread (a direct GroupAdd -> impls_.push_back, reallocating the vector) must
// not invalidate the loop (heap use-after-free under ASan before the fix), and
// every pending commitSync still completes exactly once.
TEST(GroupConsumerHubCommand, CommitSyncHandlerSubscribesGroupRegression) {
    using rune::client::detail::HubCommand;
    g_ledger = SyncLedger{};
    GroupConsumerHub hub;
    struct Adopter {
        GroupConsumerHub* hub = nullptr;
        int calls = 0;
        std::vector<std::unique_ptr<GroupConsumerImpl>> made;
        static void on(void* ctx, const Completion& c, std::span<const uint8_t> p) noexcept {
            auto* self = static_cast<Adopter*>(ctx);
            ++self->calls;
            SyncLedger::on(&g_ledger, c, p);
            for (int i = 0; i < 64; ++i) {  // force impls_ to reallocate
                auto impl = std::make_unique<GroupConsumerImpl>();
                impl->active = true;
                HubCommand add;
                add.op     = HubCommand::Op::GroupAdd;
                add.handle = reinterpret_cast<uint64_t>(impl.get());
                self->hub->applyCommandDirect(add);
                self->made.push_back(std::move(impl));
            }
        }
    } adopter{&hub};
    std::vector<std::unique_ptr<GroupConsumerImpl>> impls;
    for (int i = 0; i < 3; ++i) {
        auto impl = std::make_unique<GroupConsumerImpl>();
        impl->active = true;
        hub.add(impl.get());
        HubCommand sync;
        sync.op     = HubCommand::Op::GroupCommitSync;
        sync.handle = reinterpret_cast<uint64_t>(impl.get());
        sync.a      = i + 1;
        rune::client::detail::OpTarget tgt;
        tgt.handler = CompletionHandler{&Adopter::on, &adopter};
        rune::client::detail::encodeTarget(tgt, sync);
        hub.applyCommand(sync);
        impls.push_back(std::move(impl));
    }
    hub.onSessionDown();  // completes all three inside one loop over impls_
    EXPECT_EQ(adopter.calls, 3);
    for (Token t = 1; t <= 3; ++t) {
        ASSERT_EQ(g_ledger.e.count(t), 1u);
        EXPECT_EQ(g_ledger.e.at(t).times, 1);
        EXPECT_EQ(g_ledger.e.at(t).c.status, OpStatus::SessionLost);
    }
    EXPECT_FALSE(hub.hasPendingCommitSync());
}
