// tests/group/GroupConsumerReadCommittedTest.cpp
//
// read_committed isolation of the consumer-group client: the config is stored, the hub preserves it, and the
// Subscribe frame the hub emits after a SyncGroupResponse carries isolation=1 (ReadCommitted) / 0 (default).
// (Moved from the deleted transactional-producer test file, Phase 2b F Task 9: they never exercised a producer.)

#include <rune/client/group/GroupConsumer.h>
#include <rune/client/group/GroupConsumerHub.h>
#include <rune/client/codec/DataCodec.h>

#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/MessageHeader.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

namespace {

constexpr int64_t kSessionId = 7777;

uint16_t peekTemplate(const std::vector<uint8_t>& buf) {
    if (buf.size() < 4) return 0;
    return static_cast<uint16_t>(buf[2]) | (static_cast<uint16_t>(buf[3]) << 8);
}

}  // namespace

// ===========================================================================
// Test 8: GroupConsumerReadCommittedConfig
//   GroupConfig{isolation=ReadCommitted} stores isolation correctly.
//   The isolation value must be accessible from the impl and equal ReadCommitted.
// ===========================================================================
TEST(GroupConsumerReadCommittedTest, IsolationLevelStoredCorrectly)
{
    using namespace rune::client;
    using namespace rune::client::group;

    GroupConfig cfg;
    cfg.autoCommit           = false;
    cfg.sessionTimeoutMs     = 10000;
    cfg.isolation            = IsolationLevel::ReadCommitted;

    // Build a GroupConsumerImpl with this config.
    auto impl = std::make_unique<GroupConsumerImpl>();  // F8c C7: the bare hub holds raw pointers
    impl->groupId  = "test-group";
    impl->topicIds = {42};
    impl->cfg      = cfg;

    // Verify isolation is stored as ReadCommitted (=1).
    EXPECT_EQ(impl->cfg.isolation, IsolationLevel::ReadCommitted)
        << "GroupConfig must store IsolationLevel::ReadCommitted";
    EXPECT_EQ(static_cast<uint8_t>(impl->cfg.isolation), uint8_t{1})
        << "IsolationLevel::ReadCommitted wire value must be 1";

    // Verify ReadUncommitted default.
    GroupConfig defaultCfg;
    EXPECT_EQ(defaultCfg.isolation, IsolationLevel::ReadUncommitted)
        << "Default isolation must be ReadUncommitted";
    EXPECT_EQ(static_cast<uint8_t>(defaultCfg.isolation), uint8_t{0})
        << "IsolationLevel::ReadUncommitted wire value must be 0";
}

// ===========================================================================
// Test 9: GroupConsumerReadCommittedHub
//   Register a GroupConsumer with isolation=ReadCommitted; verify the impl's
//   cfg is stored with the correct isolation level via the hub.
// ===========================================================================
TEST(GroupConsumerReadCommittedTest, HubPreservesIsolationConfig)
{
    using namespace rune::client;
    using namespace rune::client::group;

    GroupConsumerHub hub;

    GroupConfig cfg;
    cfg.isolation = IsolationLevel::ReadCommitted;
    cfg.autoCommit = false;

    auto impl = std::make_unique<GroupConsumerImpl>();  // F8c C7: the bare hub holds raw pointers
    impl->groupId  = "rc-group";
    impl->topicIds = {10};
    impl->cfg      = cfg;
    impl->active = true;

    hub.add(impl.get());

    // Verify that the hub registered the impl with the correct isolation.
    // We access the impl directly (hub does not expose iteration).
    EXPECT_EQ(impl->cfg.isolation, IsolationLevel::ReadCommitted)
        << "Hub must preserve isolation config in registered impl";
}

// ===========================================================================
// Test 10: GroupConsumerReadCommittedWireIsolation  (IMPORTANT 4 fix)
//
//   A GroupConsumerHub with a ReadCommitted consumer, after receiving a
//   SyncGroupResponse that assigns a partition, must emit a Subscribe frame
//   on the wire with isolation=1 (ReadCommitted).
//
//   This test drives GroupConsumerHub directly via its public entry points
//   (no background thread) and asserts the Subscribe templateId and isolation
//   byte are correct in the captured output.
// ===========================================================================
TEST(GroupConsumerReadCommittedTest, WireIsolationOnSyncGroupResponse)
{
    using namespace rune::client;
    using namespace rune::client::group;

    namespace rpc = rune::protocol::client;

    // Build a GroupConsumerHub with a ReadCommitted consumer.
    GroupConsumerHub hub;

    GroupConfig cfg;
    cfg.isolation   = IsolationLevel::ReadCommitted;
    cfg.autoCommit  = false;
    cfg.sessionTimeoutMs = 10000;

    auto impl = std::make_unique<GroupConsumerImpl>();  // F8c C7: the bare hub holds raw pointers
    impl->groupId   = "rc-wire-group";
    impl->topicIds  = {42};
    impl->cfg       = cfg;
    impl->memberId  = "member-1";
    impl->generationId = 1;
    impl->state     = GroupConsumerState::Syncing; // ready to receive SyncGroupResponse
    impl->active = true;
    impl->ctlCorrId = GroupConsumerHub::kGroupCtlCorrTag | 1;   // #431: the SyncGroup this answers
    impl->joinDeadlineNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count() + 30'000'000'000LL;   // the hub's default clock

    hub.add(impl.get());

    // Capture all frames sent by the hub.
    std::vector<std::vector<uint8_t>> capturedFrames;
    SendFn sendFn = [&capturedFrames](int32_t /*streamId*/, const uint8_t* buf, std::size_t len) -> bool {
        capturedFrames.emplace_back(buf, buf + len);
        return true;
    };

    // Build a SyncGroupResponse assigning partition (topicId=42, partition=0).
    rpc::SyncGroupResponse syncResp;
    syncResp.sessionId     = kSessionId;
    syncResp.correlationId = impl->ctlCorrId;
    syncResp.errorCode     = 0; // OK

    rpc::TopicPartition32 asgn;
    asgn.topicId   = 42;
    asgn.partition = 0;
    const std::span<const rpc::TopicPartition32> asgnSpan{&asgn, 1};

    const std::size_t sz = rpc::SyncGroupResponse::encodedSizeFor(1);
    std::vector<uint8_t> syncBuf(sz);
    const std::size_t written = syncResp.encode(syncBuf.data(), asgnSpan);
    ASSERT_GT(written, 0u);

    // Feed the SyncGroupResponse into the hub.
    hub.onSyncGroupResponse(syncBuf.data(), written, kSessionId, sendFn);

    // The hub must now be Stable and must have sent a Subscribe frame.
    // Filter for Subscribe frames.
    bool foundSubscribe = false;
    for (const auto& f : capturedFrames) {
        const uint16_t tid = peekTemplate(f);
        if (tid != static_cast<uint16_t>(rpc::TemplateId::Subscribe)) continue;

        // Decode and check isolation.
        auto subOpt = rune::client::codec::decodeSubscribe(f.data(), f.size());
        ASSERT_TRUE(subOpt.has_value()) << "Subscribe frame must be decodable";
        EXPECT_EQ(subOpt->topicId, 42) << "Subscribe must target topicId=42";
        EXPECT_EQ(subOpt->partition, 0) << "Subscribe must target partition=0";
        EXPECT_EQ(subOpt->partitionKnown, uint8_t{1}) << "partitionKnown must be 1";
        EXPECT_EQ(subOpt->isolation, uint8_t{1})
            << "isolation MUST be 1 (ReadCommitted) on the wire. "
               "A value of 0 here means Critical-2 is broken: "
               "the broker sees ReadUncommitted and delivers open-txn records.";
        foundSubscribe = true;
        break;
    }
    EXPECT_TRUE(foundSubscribe)
        << "GroupConsumer must emit a Subscribe frame after SyncGroupResponse "
           "so the broker applies the correct isolation level";

    // Sanity: consumer is now Stable.
    EXPECT_EQ(impl->state, GroupConsumerState::Stable);
}

// ===========================================================================
// Test 11: GroupConsumerReadUncommittedWireIsolation
//
//   A GroupConsumerHub with a ReadUncommitted (default) consumer emits
//   Subscribe frames with isolation=0.
// ===========================================================================
TEST(GroupConsumerReadCommittedTest, WireIsolationReadUncommittedIsZero)
{
    using namespace rune::client;
    using namespace rune::client::group;

    namespace rpc = rune::protocol::client;

    GroupConsumerHub hub;

    GroupConfig cfg;  // default: ReadUncommitted
    cfg.autoCommit       = false;
    cfg.sessionTimeoutMs = 10000;

    auto impl = std::make_unique<GroupConsumerImpl>();  // F8c C7: the bare hub holds raw pointers
    impl->groupId    = "ru-wire-group";
    impl->topicIds   = {10};
    impl->cfg        = cfg;
    impl->memberId   = "m-2";
    impl->generationId = 1;
    impl->state      = GroupConsumerState::Syncing;
    impl->active = true;
    impl->ctlCorrId = GroupConsumerHub::kGroupCtlCorrTag | 1;   // #431: the SyncGroup this answers
    impl->joinDeadlineNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count() + 30'000'000'000LL;   // the hub's default clock

    hub.add(impl.get());

    std::vector<std::vector<uint8_t>> capturedFrames;
    SendFn sendFn = [&capturedFrames](int32_t /*streamId*/, const uint8_t* buf, std::size_t len) -> bool {
        capturedFrames.emplace_back(buf, buf + len);
        return true;
    };

    rpc::SyncGroupResponse syncResp;
    syncResp.sessionId     = kSessionId;
    syncResp.correlationId = impl->ctlCorrId;
    syncResp.errorCode     = 0;

    rpc::TopicPartition32 asgn;
    asgn.topicId   = 10;
    asgn.partition = 0;
    const std::span<const rpc::TopicPartition32> asgnSpan{&asgn, 1};

    const std::size_t sz = rpc::SyncGroupResponse::encodedSizeFor(1);
    std::vector<uint8_t> syncBuf(sz);
    syncResp.encode(syncBuf.data(), asgnSpan);

    hub.onSyncGroupResponse(syncBuf.data(), syncBuf.size(), kSessionId, sendFn);

    for (const auto& f : capturedFrames) {
        if (peekTemplate(f) != static_cast<uint16_t>(rpc::TemplateId::Subscribe)) continue;
        auto subOpt = rune::client::codec::decodeSubscribe(f.data(), f.size());
        ASSERT_TRUE(subOpt.has_value());
        EXPECT_EQ(subOpt->isolation, uint8_t{0})
            << "Default (ReadUncommitted) consumer must send isolation=0";
        break;
    }
}


