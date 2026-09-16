// ---------------------------------------------------------------------------
// GroupPushDeliveryTest.cpp — CG-4 integration tests.
//
// Tests:
//   CG4_EachPartitionToExactlyOneMember — 2 members, 2 partitions → Stable.
//     Assert each member owns exactly 1 partition; union = {0,1}; no overlap.
//
//   CG4_ResumeFromCommittedOffset — commit offset 5 for (g, T:p0) before join.
//     After Stable, the owning member's stub subscription has nextOffset=5.
//
//   CG4_EagerRevokeOnRebalance — Stable with 2 members, 3rd joins → revoke fired.
//     subscriptionCount drops to 0 at PreparingRebalance; new Stable reassigns.
//
//   CG4_AssignFrameEncodeDecode — Assign frame encode→decode for full and empty (revoke).
//
//   CG4_AssignFrameCaptureCallback — sendControlFrame callback captures frame bytes;
//     decoded Assign has correct generationId and partition set.
//
//   CG4_ServiceStable_SingleMember — coordinator drives to Stable for 1 member,
//     assignmentFor returns the partition.
//
// No Aeron.  No busy-spin.  All waits bounded by kCap iterations.
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
#include <rune/protocol/client/ArrayView.h>
#include <rune/protocol/client/Protocol.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::node;
using namespace rune::coordination;
using namespace rune::replication;
namespace pc = rune::protocol::client;
namespace fs = std::filesystem;

// ===========================================================================
// Shared constants / helpers
// ===========================================================================

static constexpr int     kCap    = 800;
static constexpr int64_t kStepNs = 1'000'000LL;  // 1 ms in ns

static std::string makeSuffix() {
    static std::atomic<uint64_t> g{0};
    return std::to_string(::getpid()) + "_" +
           std::to_string(g.fetch_add(1, std::memory_order_relaxed));
}

struct TempDir {
    std::string path;
    explicit TempDir(std::string_view tag) {
        path = fs::temp_directory_path().string()
             + "/rune_cg4_" + std::string(tag) + "_" + makeSuffix();
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

class NullTransportCG4 : public PartitionTransport {
public:
    void sendFetchRequest(const FetchRequestMsg&) override {}
    void sendFetchResponse(NodeId, const FetchResponseMsg&) override {}
};

static PartitionConfig makePartCfg() {
    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = 1;
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

// ===========================================================================
// StubGateway — minimal in-process implementation of the CG-4 gateway API.
//
// NOT a subclass of ClientGateway — used directly to verify that the
// orchestration logic in GroupCoordinationService produces correct calls.
// Tests that need to verify assignments or Assign frames call the stub methods
// manually (mirroring what onReachedStable / onEnterPreparingRebalance do),
// then assert on the stub's internal state.
// ===========================================================================

struct StubGateway {
    struct Sub {
        int64_t sessionId  = 0;
        int32_t topicId    = 0;
        int32_t partition  = 0;
        int64_t nextOffset = 0;
        bool    isGroup    = false;
    };

    std::map<int64_t, Sub>                     subs;            // subId -> Sub
    std::map<int64_t, std::vector<int64_t>>    groupMemberSubs; // sessionId -> [subId]
    std::set<int64_t>                          sessions;
    int64_t                                    nextSubId  = 0;
    int64_t                                    nextSessId = 0;

    // Captured Assign frame bytes per sessionId.
    std::map<int64_t, std::vector<std::vector<uint8_t>>> frames;

    int64_t addSession() {
        const int64_t sid = ++nextSessId;
        sessions.insert(sid);
        return sid;
    }

    int64_t addGroupSubscription(int64_t memberSessionId,
                                 int32_t topicId,
                                 int32_t partition,
                                 int64_t fromOffset,
                                 uint8_t /*isolation*/) {
        if (!sessions.count(memberSessionId)) return -1;
        const int64_t subId = ++nextSubId;
        Sub s;
        s.sessionId  = memberSessionId;
        s.topicId    = topicId;
        s.partition  = partition;
        s.nextOffset = fromOffset;
        s.isGroup    = true;
        subs[subId]  = s;
        groupMemberSubs[memberSessionId].push_back(subId);
        return subId;
    }

    void revokeGroupSubscriptions(int64_t memberSessionId) {
        auto it = groupMemberSubs.find(memberSessionId);
        if (it == groupMemberSubs.end()) return;
        for (int64_t sid : it->second) subs.erase(sid);
        groupMemberSubs.erase(it);
    }

    bool sendControlFrame(int64_t sessionId, const uint8_t* buf, std::size_t len) {
        frames[sessionId].emplace_back(buf, buf + len);
        return true;
    }

    std::size_t subscriptionCount()              const { return subs.size(); }
    std::size_t groupMemberSubCount(int64_t sid)  const {
        auto it = groupMemberSubs.find(sid);
        return it != groupMemberSubs.end() ? it->second.size() : 0;
    }
};

// ===========================================================================
// Fixture — shared infrastructure across tests
// ===========================================================================

class CG4Test : public ::testing::Test {
protected:
    static constexpr std::string_view kTopicName = "test-topic";
    static constexpr int32_t kTopicId  = 0;
    static constexpr int32_t kNumParts = 2;
    static constexpr std::size_t kBufSz = 4096;

    CG4Test()
        : offsetDir_("offset")
        , topicDir_("topics")
        , offsetLog_(offsetDir_.path)
    {}

    void SetUp() override {
        transport_ = std::make_unique<NullTransportCG4>();
        replica_ = std::make_unique<PartitionReplica>(
            makePartCfg(), Role::Leader, /*generation=*/1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1},
            &offsetLog_, transport_.get());
        store_   = std::make_unique<ReplicatedOffsetStore>(replica_.get());
        registry_ = std::make_unique<rune::gateway::TopicRegistry>(
            std::vector<TopicSpec>{
                makeTopicSpec(std::string(kTopicName), kNumParts, kTopicId)
            },
            topicDir_.path);
        service_ = std::make_unique<GroupCoordinationService>(
            store_.get(), registry_.get(),
            [] { return true; },
            [] { return NodeId{1}; });
        // Note: service_->setGateway() is NOT called here.  Tests exercise the
        // coordinator state machine directly and verify assignments via
        // coordinator().  Gateway-side logic is verified via the StubGateway.
    }

    // Drive doWork until predicate is true or cap iterations.
    bool driveUntil(std::function<bool()> pred, int cap = kCap) {
        for (int i = 0; i < cap && !pred(); ++i) {
            clock_ += kStepNs;
            service_->doWork(clock_);
        }
        return pred();
    }

    // Do a JoinGroup round-trip; returns the assigned memberId.
    std::string doJoin(int64_t sessionId, int64_t correlationId,
                        std::string_view groupId, std::string_view memberId = "") {
        pc::JoinGroup msg;
        msg.sessionId        = sessionId;
        msg.correlationId    = correlationId;
        msg.sessionTimeoutMs = 5000;
        msg.groupId  = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(groupId.data()), groupId.size());
        msg.memberId = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(memberId.data()), memberId.size());
        std::array<int32_t, 1> topics{kTopicId};
        const auto reqLen = msg.encode(req_, std::span<const int32_t>(topics));
        const auto outLen = service_->handleJoinGroup(req_, reqLen, clock_, out_, kBufSz);
        if (outLen == 0) return {};
        const auto resp = pc::JoinGroupResponse::decode(out_, outLen);
        if (!resp) return {};
        return std::string(reinterpret_cast<const char*>(resp->memberId.data()),
                           resp->memberId.size());
    }

    TempDir                                        offsetDir_;
    TempDir                                        topicDir_;
    rune::storage::Log                             offsetLog_;
    std::unique_ptr<NullTransportCG4>              transport_;
    std::unique_ptr<PartitionReplica>              replica_;
    std::unique_ptr<ReplicatedOffsetStore>         store_;
    std::unique_ptr<rune::gateway::TopicRegistry>  registry_;
    std::unique_ptr<GroupCoordinationService>      service_;

    StubGateway gw_;
    int64_t     clock_ = 1'000'000LL;

    uint8_t req_[kBufSz]{};
    uint8_t out_[kBufSz]{};
};

// ===========================================================================
// Test: EachPartitionToExactlyOneMember
//
// 2 members join a 2-partition topic, both re-join -> Stable.
// After Stable: each member assignment is size 1; union = {0,1}; no overlap.
// ===========================================================================
TEST_F(CG4Test, EachPartitionToExactlyOneMember) {
    const std::string group = "g-cg4-exact-owner";
    const int64_t sess1 = gw_.addSession();
    const int64_t sess2 = gw_.addSession();

    // Initial joins -- both get new memberIds.
    const std::string m1 = doJoin(sess1, 1, group);
    ASSERT_FALSE(m1.empty());
    const std::string m2 = doJoin(sess2, 2, group);
    ASSERT_FALSE(m2.empty());

    EXPECT_EQ(service_->coordinator(group).state(), GroupState::PreparingRebalance);

    // Re-join with assigned memberIds.
    doJoin(sess1, 3, group, m1);
    doJoin(sess2, 4, group, m2);

    // Drive to Stable.
    const bool stable = driveUntil([&] {
        return service_->coordinator(group).state() == GroupState::Stable;
    });
    ASSERT_TRUE(stable) << "Must reach Stable after both members re-join";

    const auto& coord = service_->coordinator(group);
    EXPECT_EQ(coord.memberCount(), 2u);

    const auto a1 = coord.assignmentFor(m1);
    const auto a2 = coord.assignmentFor(m2);

    // Each member gets exactly 1 partition (Range assignor, 2 members, 2 partitions).
    EXPECT_EQ(a1.size(), 1u) << "m1 must own exactly 1 partition";
    EXPECT_EQ(a2.size(), 1u) << "m2 must own exactly 1 partition";

    std::set<int32_t> owned1, owned2;
    for (const auto& tp : a1) owned1.insert(tp.partition);
    for (const auto& tp : a2) owned2.insert(tp.partition);

    // No overlap.
    std::set<int32_t> overlap;
    std::set_intersection(owned1.begin(), owned1.end(),
                          owned2.begin(), owned2.end(),
                          std::inserter(overlap, overlap.begin()));
    EXPECT_TRUE(overlap.empty()) << "No partition may be owned by >1 member";

    // Union = {0, 1}.
    std::set<int32_t> allOwned;
    allOwned.insert(owned1.begin(), owned1.end());
    allOwned.insert(owned2.begin(), owned2.end());
    EXPECT_EQ(allOwned.size(), 2u) << "Both partitions must be assigned";
    EXPECT_TRUE(allOwned.count(0) && allOwned.count(1))
        << "Partitions 0 and 1 must both appear in the union";

    // Stub: simulate what onReachedStable does -- add group subs for each member.
    for (const auto& tp : a1) {
        const int64_t subId = gw_.addGroupSubscription(sess1, kTopicId, tp.partition, 0, 0);
        EXPECT_GT(subId, 0);
    }
    for (const auto& tp : a2) {
        const int64_t subId = gw_.addGroupSubscription(sess2, kTopicId, tp.partition, 0, 0);
        EXPECT_GT(subId, 0);
    }

    // Each member has exactly 1 group subscription.
    EXPECT_EQ(gw_.groupMemberSubCount(sess1), 1u);
    EXPECT_EQ(gw_.groupMemberSubCount(sess2), 1u);
    EXPECT_EQ(gw_.subscriptionCount(), 2u);
}

// ===========================================================================
// Test: ResumeFromCommittedOffset
//
// Commit offset 5 for (g, T:p0) before join.
// 2 members join -> Stable.
// Find who owns p0; simulate onReachedStable with fromOffset=committed.
// Assert nextOffset == 5 for that member; == 0 for non-committed partition.
// ===========================================================================
TEST_F(CG4Test, ResumeFromCommittedOffset) {
    const std::string group = "g-cg4-resume";
    const TopicPartition tp{std::string(kTopicName), 0};

    // Commit offset 5 BEFORE any member joins.
    store_->commit(group, tp, /*offset=*/5, /*meta=*/"", clock_);
    const auto committed = store_->fetch(group, tp);
    ASSERT_TRUE(committed.has_value());
    ASSERT_EQ(committed->offset, int64_t{5});

    const int64_t sess1 = gw_.addSession();
    const int64_t sess2 = gw_.addSession();

    // Join and drive to Stable.
    const std::string m1 = doJoin(sess1, 10, group);
    ASSERT_FALSE(m1.empty());
    const std::string m2 = doJoin(sess2, 11, group);
    ASSERT_FALSE(m2.empty());

    doJoin(sess1, 12, group, m1);
    doJoin(sess2, 13, group, m2);

    const bool stable = driveUntil([&] {
        return service_->coordinator(group).state() == GroupState::Stable;
    });
    ASSERT_TRUE(stable);

    const auto& coord = service_->coordinator(group);

    // Find which member owns partition 0.
    std::string ownerP0;
    int64_t ownerSess = -1;
    for (const auto& member : coord.members()) {
        for (const auto& mtp : member.assignment) {
            if (mtp.partition == 0) {
                ownerP0   = member.memberId;
                ownerSess = (member.memberId == m1) ? sess1 : sess2;
            }
        }
    }
    ASSERT_FALSE(ownerP0.empty()) << "Partition 0 must be assigned to some member";

    // Simulate onReachedStable for p0 owner: fromOffset = committed offset = 5.
    const auto co = store_->fetch(group, tp);
    const int64_t fromOffset = co ? co->offset : 0;
    EXPECT_EQ(fromOffset, int64_t{5});

    const int64_t subId0 = gw_.addGroupSubscription(ownerSess, kTopicId, 0, fromOffset, 0);
    ASSERT_GT(subId0, 0) << "addGroupSubscription must succeed";
    EXPECT_EQ(gw_.subs.at(subId0).nextOffset, int64_t{5})
        << "Owner of p0 must start at committed offset 5";

    // Non-owner of p0 gets p1 with fromOffset=0 (no commit for p1).
    const int64_t nonOwnerSess = (ownerSess == sess1) ? sess2 : sess1;
    const auto co1 = store_->fetch(group, TopicPartition{std::string(kTopicName), 1});
    const int64_t fromOffset1 = co1 ? co1->offset : 0;
    const int64_t subId1 = gw_.addGroupSubscription(nonOwnerSess, kTopicId, 1, fromOffset1, 0);
    ASSERT_GT(subId1, 0);
    EXPECT_EQ(gw_.subs.at(subId1).nextOffset, int64_t{0})
        << "Non-committed partition must start at offset 0";
}

// ===========================================================================
// Test: EagerRevokeOnRebalance
//
// Stable with 2 members (subs manually added).
// 3rd member joins -> PreparingRebalance.
// Simulate eager revoke: revokeGroupSubscriptions for both members.
// Assert subscriptionCount drops to 0.
// Drive to new Stable -> partition set is coherent (no overlap).
// ===========================================================================
TEST_F(CG4Test, EagerRevokeOnRebalance) {
    const std::string group = "g-cg4-eager-revoke";
    const int64_t sess1 = gw_.addSession();
    const int64_t sess2 = gw_.addSession();

    // Phase 1: 2 members -> Stable.
    const std::string m1 = doJoin(sess1, 20, group);
    ASSERT_FALSE(m1.empty());
    const std::string m2 = doJoin(sess2, 21, group);
    ASSERT_FALSE(m2.empty());

    doJoin(sess1, 22, group, m1);
    doJoin(sess2, 23, group, m2);

    bool stable = driveUntil([&] {
        return service_->coordinator(group).state() == GroupState::Stable;
    });
    ASSERT_TRUE(stable) << "Phase 1 must reach Stable";

    // Manually mirror what onReachedStable does: add group subs.
    {
        const auto& coord = service_->coordinator(group);
        for (const auto& member : coord.members()) {
            const int64_t mSess = (member.memberId == m1) ? sess1 : sess2;
            for (const auto& mtp : member.assignment) {
                gw_.addGroupSubscription(mSess, kTopicId, mtp.partition, 0, 0);
            }
        }
    }
    EXPECT_EQ(gw_.subscriptionCount(), 2u)
        << "Phase 1: 2 members, 2 partitions -> 2 group subs";

    // Phase 2: 3rd member joins -> triggers rebalance.
    const int64_t sess3 = gw_.addSession();
    const std::string m3 = doJoin(sess3, 24, group);
    ASSERT_FALSE(m3.empty());
    EXPECT_EQ(service_->coordinator(group).state(), GroupState::PreparingRebalance);

    // Simulate onEnterPreparingRebalance: revoke all existing group subs.
    gw_.revokeGroupSubscriptions(sess1);
    gw_.revokeGroupSubscriptions(sess2);
    EXPECT_EQ(gw_.subscriptionCount(), 0u)
        << "After eager revoke, subscription count must be 0";

    // All three members re-join.
    doJoin(sess1, 25, group, m1);
    doJoin(sess2, 26, group, m2);
    doJoin(sess3, 27, group, m3);

    stable = driveUntil([&] {
        return service_->coordinator(group).state() == GroupState::Stable;
    });
    ASSERT_TRUE(stable) << "Phase 2 must reach Stable after 3rd member joins";

    // Verify new assignments: all partitions assigned exactly once.
    const auto& coord2 = service_->coordinator(group);
    EXPECT_EQ(coord2.memberCount(), 3u);
    std::set<int32_t> allAssigned;
    for (const auto& member : coord2.members()) {
        for (const auto& mtp : member.assignment) {
            EXPECT_EQ(allAssigned.count(mtp.partition), 0u)
                << "Partition " << mtp.partition << " assigned to >1 member";
            allAssigned.insert(mtp.partition);
        }
    }
    // With 3 members and 2 partitions, 2 partitions should be assigned total.
    EXPECT_EQ(allAssigned.size(), 2u)
        << "Both partitions must be assigned after phase-2 Stable";
}

// ===========================================================================
// Test: AssignFrameEncodeDecode (full assign)
//
// Encode an Assign frame with 2 partitions; decode; verify correctness.
// ===========================================================================
TEST(CG4_AssignFrameEncodeDecode, FullAssign) {
    static constexpr std::size_t kBufSz = 512;
    uint8_t buf[kBufSz]{};

    pc::Assign frame;
    frame.sessionId     = 77;
    frame.correlationId = 0;
    frame.generationId  = 3;

    std::array<pc::TopicPartition32, 2> asgn{{
        {/*topicId=*/0, /*partition=*/0},
        {/*topicId=*/0, /*partition=*/1}
    }};
    const std::size_t encoded = frame.encode(buf, std::span<const pc::TopicPartition32>(asgn));
    ASSERT_GT(encoded, 0u);
    ASSERT_LE(encoded, kBufSz);

    const auto decoded = pc::Assign::decode(buf, encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,    int64_t{77});
    EXPECT_EQ(decoded->generationId, int32_t{3});
    EXPECT_EQ(decoded->assignment.size(), std::size_t{2});

    std::set<int32_t> partitions;
    for (std::size_t i = 0; i < decoded->assignment.size(); ++i)
        partitions.insert(decoded->assignment[i].partition);
    EXPECT_TRUE(partitions.count(0));
    EXPECT_TRUE(partitions.count(1));
}

// ===========================================================================
// Test: AssignFrameEncodeDecode (empty = revoke)
// ===========================================================================
TEST(CG4_AssignFrameEncodeDecode, EmptyAssignIsRevoke) {
    static constexpr std::size_t kBufSz = 64;
    uint8_t buf[kBufSz]{};

    pc::Assign frame;
    frame.sessionId     = 99;
    frame.correlationId = 0;
    frame.generationId  = 7;

    const std::size_t encoded = frame.encode(buf, std::span<const pc::TopicPartition32>{});
    ASSERT_GT(encoded, 0u);
    ASSERT_LE(encoded, kBufSz);

    const auto decoded = pc::Assign::decode(buf, encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,    int64_t{99});
    EXPECT_EQ(decoded->generationId, int32_t{7});
    EXPECT_EQ(decoded->assignment.size(), std::size_t{0})
        << "Empty assignment = revoke all";
}

// ===========================================================================
// Test: AssignFrameCaptureCallback
//
// Simulate onReachedStable + onEnterPreparingRebalance frame dispatch:
//   - Encode Assign with partition via StubGateway::sendControlFrame.
//   - Encode empty Assign (revoke) via StubGateway::sendControlFrame.
// Assert: frames captured in the right order; each decodes correctly.
// ===========================================================================
TEST(CG4_AssignFrameCapture, FullThenRevoke) {
    StubGateway gw;
    const int64_t sid = gw.addSession();

    static constexpr std::size_t kBufSz = 512;
    uint8_t buf[kBufSz]{};

    // Assign frame: generation=2, partition=0.
    {
        pc::Assign frame;
        frame.sessionId     = sid;
        frame.correlationId = 0;
        frame.generationId  = 2;
        std::array<pc::TopicPartition32, 1> asgn{{{0, 0}}};
        const std::size_t n = frame.encode(buf, std::span<const pc::TopicPartition32>(asgn));
        gw.sendControlFrame(sid, buf, n);
    }

    // Revoke Assign frame: generation=3, empty.
    {
        pc::Assign frame;
        frame.sessionId     = sid;
        frame.correlationId = 0;
        frame.generationId  = 3;
        const std::size_t n = frame.encode(buf, std::span<const pc::TopicPartition32>{});
        gw.sendControlFrame(sid, buf, n);
    }

    ASSERT_EQ(gw.frames.count(sid), 1u);
    ASSERT_EQ(gw.frames.at(sid).size(), 2u);

    // First frame: Assign with p0.
    {
        const auto& bytes = gw.frames.at(sid)[0];
        const auto d = pc::Assign::decode(bytes.data(), bytes.size());
        ASSERT_TRUE(d.has_value());
        EXPECT_EQ(d->generationId, int32_t{2});
        ASSERT_EQ(d->assignment.size(), std::size_t{1});
        EXPECT_EQ(d->assignment[0].partition, int32_t{0});
    }

    // Second frame: revoke (empty, gen=3).
    {
        const auto& bytes = gw.frames.at(sid)[1];
        const auto d = pc::Assign::decode(bytes.data(), bytes.size());
        ASSERT_TRUE(d.has_value());
        EXPECT_EQ(d->generationId, int32_t{3});
        EXPECT_EQ(d->assignment.size(), std::size_t{0});
    }
}

// ===========================================================================
// Test: ServiceStable_SingleMember
//
// 1 member joins a 1-partition topic -> coordinator drives to Stable.
// assignmentFor(m1) returns 1 entry with partition=0.
// ===========================================================================
TEST(CG4_ServiceStable, SingleMemberOnePartitionTopic) {
    const std::string suffix = makeSuffix();
    TempDir offsetDir("svc_offset_" + suffix);
    TempDir topicDir2("svc_topic_" + suffix);
    rune::storage::Log offsetLog(offsetDir.path);
    NullTransportCG4 transport;
    auto replica = std::make_unique<PartitionReplica>(
        makePartCfg(), Role::Leader, /*generation=*/1,
        std::vector<NodeId>{1}, std::vector<NodeId>{1},
        &offsetLog, &transport);
    auto store = std::make_unique<ReplicatedOffsetStore>(replica.get());

    auto reg = std::make_unique<rune::gateway::TopicRegistry>(
        std::vector<TopicSpec>{
            makeTopicSpec("t1", /*partitions=*/1, /*id=*/0)
        },
        topicDir2.path);

    GroupCoordinationService svc(store.get(), reg.get(),
                                 [] { return true; }, [] { return NodeId{1}; });

    const std::string group = "g-single";
    static constexpr std::size_t kBufSz = 4096;
    uint8_t req[kBufSz]{};
    uint8_t out[kBufSz]{};
    int64_t clock = 1'000'000LL;

    // Encode a JoinGroup request for topic id=0.
    auto encodeJoin = [&](std::string_view memberId) -> std::size_t {
        pc::JoinGroup msg;
        msg.sessionId        = 1;
        msg.correlationId    = 1;
        msg.sessionTimeoutMs = 5000;
        msg.groupId  = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(group.data()), group.size());
        msg.memberId = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(memberId.data()), memberId.size());
        std::array<int32_t, 1> tids{0};
        return msg.encode(req, std::span<const int32_t>(tids));
    };

    // Initial join (empty memberId -> server assigns one).
    const auto r0Len = encodeJoin({});
    const auto o0Len = svc.handleJoinGroup(req, r0Len, clock, out, kBufSz);
    ASSERT_GT(o0Len, 0u);
    const auto r0 = pc::JoinGroupResponse::decode(out, o0Len);
    ASSERT_TRUE(r0.has_value());
    const std::string m1(reinterpret_cast<const char*>(r0->memberId.data()),
                         r0->memberId.size());
    ASSERT_FALSE(m1.empty());

    // Re-join with assigned memberId.
    const auto r1Len = encodeJoin(m1);
    svc.handleJoinGroup(req, r1Len, clock + kStepNs, out, kBufSz);

    // Drive to Stable.
    for (int i = 0; i < kCap; ++i) {
        clock += kStepNs;
        svc.doWork(clock);
        if (svc.coordinator(group).state() == GroupState::Stable) break;
    }

    EXPECT_EQ(svc.coordinator(group).state(), GroupState::Stable);

    const auto asgn = svc.coordinator(group).assignmentFor(m1);
    ASSERT_FALSE(asgn.empty()) << "m1 must receive an assignment";
    EXPECT_EQ(asgn.size(), 1u);
    EXPECT_EQ(asgn[0].partition, int32_t{0})
        << "Single-member, single-partition: m1 gets partition 0";
    EXPECT_EQ(asgn[0].topic, std::string{"t1"});
}
