// GatewayRedirectResponsePortRegressionTest.cpp - NAT-safe egress (spec D3, fix round 1a, I6): EVERY Redirect /
// NotCoordinator the gateway answers carries the target node's gateway response-control port ("endpoint#port") to a
// client >= 16 and the bare endpoint to an older one - not only the connect-time Redirect.  The sites:
//   produce (cluster-level fallback), view row / subscribe (sendRouteRefusal_), DDL, NotCoordinator.
// (The connect-time Redirect is GatewayRedirectTest; the moved-subscription Redirect is TransientGatewayTest.)
// Deterministic, no Aeron.
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/IGroupRequestHandler.h>
#include <rune/metadata/Gpid.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <gtest/gtest.h>

#include <functional>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace rune::gateway {
namespace {
namespace pc = rune::protocol::client;

constexpr int32_t kSelf = 1, kLeader = 2;
constexpr int32_t kLeaderPort = 8455;
const std::string kEndpoint = "aeron:udp?endpoint=10.0.0.2:9000";

struct NullGroup final : IGroupRequestHandler {
    size_t handleJoinGroup(const uint8_t*, size_t, int32_t, int64_t, uint8_t*, size_t, const rune::coordination::NsRequestContext&) override { return 0; }
    size_t handleSyncGroup(const uint8_t*, size_t, int64_t, uint8_t*, size_t, const rune::coordination::NsRequestContext&) override { return 0; }
    size_t handleHeartbeat(const uint8_t*, size_t, int64_t, uint8_t*, size_t, const rune::coordination::NsRequestContext&) override { return 0; }
    size_t handleLeaveGroup(const uint8_t*, size_t, int64_t, uint8_t*, size_t, const rune::coordination::NsRequestContext&) override { return 0; }
    size_t handleOffsetCommit(const uint8_t*, size_t, int64_t, uint8_t*, size_t, const rune::coordination::NsRequestContext&) override { return 0; }
    size_t handleOffsetFetch(const uint8_t*, size_t, int64_t, uint8_t*, size_t, const rune::coordination::NsRequestContext&) override { return 0; }
};

struct Rig {
    std::vector<std::vector<uint8_t>> out;
    NullGroup group;
    ClientGateway gw;
    explicit Rig(bool follower)
        : gw(ClientGateway::makeForTestWithLeaderView(
              [this](const uint8_t* b, std::size_t n) -> int64_t { out.emplace_back(b, b + n); return 1; },
              [follower] { return LeaderView{!follower, follower ? kEndpoint : std::string{}, follower ? kLeader : -1}; })) {
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { out.emplace_back(b, b + n); });
        gw.setGroupService(&group);
        gw.setNodeIdForTest(kSelf);
        gw.setNodeEndpointMap({{kSelf, "aeron:udp?endpoint=10.0.0.1:9000"}, {kLeader, kEndpoint}});
        gw.setResponseControl("", {{kLeader, kLeaderPort}, {kSelf, 8400}});
    }
    std::optional<pc::ErrorResponse> lastError() const {
        for (auto it = out.rbegin(); it != out.rend(); ++it)
            if (auto e = pc::ErrorResponse::decode(it->data(), it->size())) return e;
        return std::nullopt;
    }
};

struct Site {
    const char* name;
    pc::ErrorCode code;
    std::function<void(Rig&, int64_t sid)> drive;
    bool follower = true;   // the cluster-level LeaderView says "not the leader"
};

std::vector<Site> sites() {
    std::vector<Site> v;
    v.push_back({"produce", pc::ErrorCode::Redirect, [](Rig& r, int64_t sid) {
        rune::protocol::RecordBatchBuilder b(0, 0);
        b.append(std::string_view{}, std::string_view{"v"}, 0);
        const std::vector<uint8_t> batch = b.build();
        pc::Produce m;
        m.sessionId = sid; m.correlationId = 11; m.topicId = 0; m.partition = 0;
        m.partitionKnown = 1; m.acks = 0; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
        m.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
        std::vector<uint8_t> buf(m.encodedSize());
        m.encode(buf.data());
        r.gw.applyProduceForTest(buf.data(), buf.size());
    }});
    v.push_back({"viewRowOrSubscribe", pc::ErrorCode::Redirect, [](Rig& r, int64_t sid) {
        // sendRouteRefusal_ (the shared Redirect of Subscribe and GetViewRow), reached through the worker's
        // not-leader sentinel for a view whose source partition another node leads.
        constexpr int32_t kView = 0x40000011, kSrc = 9;
        r.gw.noteViewSource(kView, kSrc);
        r.gw.setRoutingImageSnapshot(rune::metadata::RoutingImage::build(
            std::vector<rune::metadata::RoutingImage::BuildEntry>{{rune::metadata::gpidOf(kSrc, 0), kLeader, 1}}));
        rune::node::WorkerReturn ret;
        ret.type          = rune::node::WorkerReturn::Type::ViewRowResult;
        ret.sessionId     = sid;
        ret.correlationId = 11;
        ret.readTopicId   = kView;
        ret.readPartition = 0;
        ret.errorCode     = rune::node::kViewRowNotLeaderInternal;
        r.gw.handleWorkerReturnForTest(ret);
    }});
    v.push_back({"ddl", pc::ErrorCode::Redirect, [](Rig& r, int64_t sid) {
        r.gw.setCatalogRoute(rune::gateway::RouteRemote{kEndpoint});
        pc::DropTopic m; m.sessionId = sid; m.correlationId = 11; m.topicName = "t";
        std::vector<uint8_t> buf(m.encodedSize());
        buf.resize(m.encode(buf.data()));
        r.gw.applyDdlFrameForTest(buf.data(), buf.size());
    }});
    v.push_back({"notCoordinator", pc::ErrorCode::NotCoordinator, [](Rig& r, int64_t sid) {
        const std::string g = "g1";
        pc::JoinGroup q; q.sessionId = sid; q.correlationId = 11; q.sessionTimeoutMs = 10'000;
        q.groupId = {reinterpret_cast<const uint8_t*>(g.data()), g.size()};
        const int32_t topics[1] = {2};
        std::vector<uint8_t> f(pc::JoinGroup::encodedSizeFor(q.groupId, q.memberId, 1, 0));
        q.encode(f.data(), std::span<const int32_t>(topics, 1));
        r.gw.applyGroupForTest(f.data(), f.size(), pc::JoinGroup::TEMPLATE_ID);
    }});
    return v;
}
} // namespace

// Every site x {client 15, client 16}: v16 -> endpoint + "#" + port; v15 -> the bare endpoint.
TEST(GatewayRedirectResponsePortRegression, EveryRedirectSiteCarriesThePortRegression) {
    for (const Site& site : sites()) {
        for (const int32_t ver : {pc::kClientVersionResponseEgress - 1, pc::kClientVersionResponseEgress}) {
            SCOPED_TRACE(std::string(site.name) + " x client " + std::to_string(ver));
            Rig r(site.follower);
            const int64_t sid = site.name == std::string("viewRowOrSubscribe")
                                    ? r.gw.injectReliableTestSessionForTest(0, ver)
                                    : r.gw.injectTestSession(0, ver);
            ASSERT_EQ(sid, 1) << "the produce handler answers the makeForTest session id";
            site.drive(r, sid);
            const auto e = r.lastError();
            ASSERT_TRUE(e.has_value()) << "no ErrorResponse";
            EXPECT_EQ(e->code, static_cast<int16_t>(site.code));
            EXPECT_EQ(e->correlationId, 11);
            const std::string want = ver >= pc::kClientVersionResponseEgress
                                         ? kEndpoint + "#" + std::to_string(kLeaderPort) : kEndpoint;
            EXPECT_EQ(std::string(e->message), want);
        }
    }
}

// A node without a gateway response port (or one we do not know) gives every client the bare endpoint.
TEST(GatewayRedirectResponsePortRegression, NodeWithoutAPortGivesTheBareEndpointRegression) {
    for (const Site& site : sites()) {
        SCOPED_TRACE(site.name);
        Rig r(site.follower);
        r.gw.setResponseControl("", {{kLeader, 0}, {kSelf, 8400}});
        const int64_t sid = site.name == std::string("viewRowOrSubscribe")
                                ? r.gw.injectReliableTestSessionForTest(0, pc::kClientVersionResponseEgress)
                                : r.gw.injectTestSession(0, pc::kClientVersionResponseEgress);
        site.drive(r, sid);
        const auto e = r.lastError();
        ASSERT_TRUE(e.has_value());
        EXPECT_EQ(std::string(e->message), kEndpoint);
    }
}

} // namespace rune::gateway
