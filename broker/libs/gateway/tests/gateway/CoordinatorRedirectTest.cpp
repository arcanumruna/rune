// D4 (backlog "group subscribe redirect"): a follower answers group / txn
// coordinator requests with ErrorResponse{NotCoordinator, leader endpoint} so
// the client's seed session moves to the coordinator (the consensus leader).  A
// node that IS the leader, or that does not know the leader's endpoint yet,
// hands the request to the coordination service (which fences NotCoordinator).
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/IGroupRequestHandler.h>
#include <rune/gateway/ITxnRequestHandler.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/TxnMessages.h>

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <vector>

namespace rune::gateway {
namespace {
namespace pc = rune::protocol::client;

struct CountingGroup final : IGroupRequestHandler {
    int calls = 0;
    size_t handleJoinGroup(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { ++calls; return 0; }
    size_t handleSyncGroup(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { ++calls; return 0; }
    size_t handleHeartbeat(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { ++calls; return 0; }
    size_t handleLeaveGroup(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { ++calls; return 0; }
    size_t handleOffsetCommit(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { ++calls; return 0; }
    size_t handleOffsetFetch(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { ++calls; return 0; }
};
struct CountingTxn final : ITxnRequestHandler {
    int calls = 0;
    size_t handleInitProducerId(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { ++calls; return 0; }
    size_t handleAddPartitions(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { ++calls; return 0; }
    size_t handleAddOffsets(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { ++calls; return 0; }
    size_t handleTxnOffsetCommit(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { ++calls; return 0; }
    size_t handleEndTxn(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { ++calls; return 0; }
};

struct Rig {
    LeaderView view;
    std::vector<std::vector<uint8_t>> out;
    CountingGroup group;
    CountingTxn txn;
    ClientGateway gw;
    int64_t sid = 0;
    explicit Rig(LeaderView v)
        : view(std::move(v)),
          gw(ClientGateway::makeForTestWithLeaderView(
              [this](const uint8_t* b, std::size_t n) -> int64_t { out.emplace_back(b, b + n); return 1; },
              [this] { return view; })) {
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { out.emplace_back(b, b + n); });
        gw.setGroupService(&group);
        gw.setTxnService(&txn);
        sid = gw.injectTestSession();
    }
    std::optional<pc::ErrorResponse> lastError() const {
        for (auto it = out.rbegin(); it != out.rend(); ++it)
            if (auto e = pc::ErrorResponse::decode(it->data(), it->size())) return e;
        return std::nullopt;
    }
    void joinGroup(int64_t corr) {
        const std::string g = "g1";
        pc::JoinGroup q; q.sessionId = sid; q.correlationId = corr; q.sessionTimeoutMs = 10'000;
        q.groupId = {reinterpret_cast<const uint8_t*>(g.data()), g.size()};
        const int32_t topics[1] = {2};
        std::vector<uint8_t> f(pc::JoinGroup::encodedSizeFor(q.groupId, q.memberId, 1));
        q.encode(f.data(), std::span<const int32_t>(topics, 1));
        gw.applyGroupForTest(f.data(), f.size(), pc::JoinGroup::TEMPLATE_ID);
    }
    void endTxn(int64_t corr) {
        const std::string t = "tx";
        pc::EndTxn q; q.sessionId = sid; q.correlationId = corr; q.producerId = 1; q.producerEpoch = 0; q.commit = 1;
        q.transactionalId = {reinterpret_cast<const uint8_t*>(t.data()), t.size()};
        std::vector<uint8_t> f(q.encodedSize()); q.encode(f.data());
        gw.applyTxnForTest(f.data(), f.size(), static_cast<uint16_t>(pc::TemplateId::EndTxn));
    }
};

TEST(CoordinatorRedirectRegression, FollowerRedirectsAGroupJoinToTheLeaderRegression) {
    Rig r(LeaderView{/*isLeader=*/false, "rune-1:40200"});
    r.joinGroup(77);
    EXPECT_EQ(r.group.calls, 0) << "a follower handed the join to its (non-coordinator) service";
    const auto e = r.lastError();
    ASSERT_TRUE(e.has_value()) << "no ErrorResponse on the session";
    EXPECT_EQ(e->code, static_cast<int16_t>(pc::ErrorCode::NotCoordinator));
    EXPECT_EQ(e->message, "rune-1:40200");
    EXPECT_EQ(e->correlationId, 77);
    EXPECT_EQ(r.gw.coordinatorRedirectsForTest(), 1u);
}

TEST(CoordinatorRedirectRegression, FollowerRedirectsATxnRequestToTheLeaderRegression) {
    Rig r(LeaderView{/*isLeader=*/false, "rune-1:40200"});
    r.endTxn(91);
    EXPECT_EQ(r.txn.calls, 0);
    const auto e = r.lastError();
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->code, static_cast<int16_t>(pc::ErrorCode::NotCoordinator));
    EXPECT_EQ(e->correlationId, 91);
}

TEST(CoordinatorRedirectRegression, LeaderAndUnknownEndpointReachTheServiceRegression) {
    Rig leader(LeaderView{/*isLeader=*/true, ""});
    leader.joinGroup(1); leader.endTxn(2);
    EXPECT_EQ(leader.group.calls, 1);
    EXPECT_EQ(leader.txn.calls, 1);
    EXPECT_EQ(leader.gw.coordinatorRedirectsForTest(), 0u);
    Rig electing(LeaderView{/*isLeader=*/false, ""});   // leader unknown: the service fences NotCoordinator
    electing.joinGroup(3); electing.endTxn(4);
    EXPECT_EQ(electing.group.calls, 1);
    EXPECT_EQ(electing.txn.calls, 1);
    EXPECT_EQ(electing.gw.coordinatorRedirectsForTest(), 0u);
}

} // namespace
} // namespace rune::gateway
