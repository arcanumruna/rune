// CheckedWritesStatsGatewayTest.cpp - checked writes Part 08 Task 7: GetStats through a real ClientGateway + WorkerPool.  The request
// is routed to the owner of the object's partition (a view by its source) after the refusals in their fixed order; the worker's
// StatsResult is encoded as a StatsResponse, and its not-leader sentinel is a redirect.
#include <gtest/gtest.h>

#include <rune/gateway/ObjectIndex.h>
#include <rune/gateway/PermissionTable.h>
#include <rune/gateway/SecurityView.h>
#include <rune/protocol/client/StatsMessages.h>

#include "support/CheckedWritesViewReadRig.h"

namespace {

using rune::node::test::CwViewReadGatewayRig;
namespace pc = rune::protocol::client;
using SR = pc::StatsResponse;

constexpr int32_t kSrc = CwViewReadGatewayRig::kSrcTopic, kView = CwViewReadGatewayRig::kViewTopic;
constexpr int32_t kOther = 2;
constexpr const char* kOtherEndpoint = "other-node:9000";

struct StatsGatewayRig {
    explicit StatsGatewayRig(int32_t partitions = 1, int32_t workers = 1) : rig(partitions, workers) {
        rune::node::TopicSpec src;
        src.name = "character";
        src.partitionCount = partitions;
        EXPECT_EQ(rig.registry->createTopicAt(kSrc, src), kSrc);
    }
    std::vector<uint8_t> frame(int64_t corr, pc::StatsScope scope, int32_t objectId, int32_t partition, int64_t session = 0) const {
        pc::GetStats m;
        m.sessionId = session != 0 ? session : rig.sid; m.correlationId = corr; m.scope = scope; m.objectId = objectId; m.partition = partition;
        std::vector<uint8_t> f(m.encodedSize());
        m.encode(f.data());
        return f;
    }
    void send(const std::vector<uint8_t>& f) { rig.gw.applyGetStatsForTest(f.data(), f.size()); }
    /// The status of the last frame when it is a StatsResponse, else -1 (an ErrorResponse is 1000 + its code).
    [[nodiscard]] int32_t lastStatus() const {
        if (const auto r = lastStats()) return r->status;
        return rig.lastStatus();
    }
    [[nodiscard]] std::optional<SR> lastStats() const {
        const auto h = pc::MessageHeader::decode(rig.lastFrame.data(), rig.lastFrame.size());
        if (!h || h->templateId != SR::TEMPLATE_ID) return std::nullopt;
        return SR::decode(rig.lastFrame.data(), rig.lastFrame.size());
    }
    [[nodiscard]] uint64_t served(int32_t worker) { return rig.pool.worker(worker).getStatsForTest(); }
    [[nodiscard]] uint64_t servedTotal() {
        uint64_t t = 0;
        for (int32_t w = 0; w < rig.pool.workerCount(); ++w) t += served(w);
        return t;
    }
    /// The image names `node` the leader of (kSrc, partition) and this node the leader of the rest.
    void leaderElsewhere(int32_t partition, int32_t node, int32_t partitions) {
        std::vector<rune::metadata::RoutingImage::BuildEntry> img;
        for (int32_t p = 0; p < partitions; ++p) img.push_back({rune::metadata::gpidOf(kSrc, p), p == partition ? node : CwViewReadGatewayRig::kSelf, 1});
        rig.gw.setRoutingImageSnapshot(rune::metadata::RoutingImage::build(img, rune::metadata::RoutingImage::Source::Committed));
        rig.gw.setNodeEndpointMap({{kOther, kOtherEndpoint}});
    }

    CwViewReadGatewayRig rig;
};

}  // namespace

TEST(CheckedWritesStatsGateway, RoutesToTheOwner) {
    StatsGatewayRig g(1, 4);
    const int32_t owner = g.rig.pool.ownerWorkerId(kSrc, 0);

    g.send(g.frame(1, pc::StatsScope::TopicPartition, kSrc, 0));
    ASSERT_TRUE(g.rig.awaitFrame());
    ASSERT_EQ(g.lastStatus(), SR::kOk);
    EXPECT_EQ(g.servedTotal(), 1u);
    EXPECT_EQ(g.served(owner), 1u) << "scope 1 lands on (topic, p)'s worker";

    g.send(g.frame(2, pc::StatsScope::ViewPartition, kView, 0));
    ASSERT_TRUE(g.rig.awaitFrame());
    const auto resp = g.lastStats();
    ASSERT_TRUE(resp.has_value());
    EXPECT_EQ(resp->status, SR::kOk);
    EXPECT_EQ(g.servedTotal(), 2u);
    EXPECT_EQ(g.served(owner), 2u) << "scope 2 lands on the source's worker";
}

TEST(CheckedWritesStatsGateway, RefusalsInOrder) {
    StatsGatewayRig g(4, 2);
    const auto expectStatus = [&](const std::vector<uint8_t>& f, int32_t status, const char* what) {
        const uint64_t before = g.rig.frames;
        g.send(f);
        EXPECT_EQ(g.rig.frames, before + 1) << what << " is answered at once";
        EXPECT_EQ(g.lastStatus(), status) << what;
    };

    const int64_t old = g.rig.gw.injectTestSession(0, pc::kClientVersionCheckedWrites - 1);
    expectStatus(g.frame(1, pc::StatsScope::TopicPartition, kSrc, 0, old), SR::kInvalidRequest, "a v11 session");
    expectStatus(g.frame(2, pc::StatsScope::TopicPartition, 99, 0), SR::kUnknownObject, "an unknown topic");
    expectStatus(g.frame(3, pc::StatsScope::TopicPartition, kSrc, 4), SR::kInvalidRequest, "partition 4 of 4");
    expectStatus(g.frame(4, pc::StatsScope::TopicPartition, kSrc, -1), SR::kInvalidRequest, "partition -1");
    EXPECT_EQ(g.servedTotal(), 0u) << "a refused request never reaches a worker";

    g.leaderElsewhere(1, kOther, 4);
    g.send(g.frame(6, pc::StatsScope::TopicPartition, kSrc, 1));
    const auto redirect = pc::ErrorResponse::decode(g.rig.lastFrame.data(), g.rig.lastFrame.size());
    ASSERT_TRUE(redirect.has_value());
    EXPECT_EQ(redirect->code, static_cast<int16_t>(pc::ErrorCode::Redirect));
    EXPECT_EQ(redirect->message, kOtherEndpoint);
    g.send(g.frame(7, pc::StatsScope::ViewPartition, kView, 1));
    const auto viewRedirect = pc::ErrorResponse::decode(g.rig.lastFrame.data(), g.rig.lastFrame.size());
    ASSERT_TRUE(viewRedirect.has_value());
    EXPECT_EQ(viewRedirect->message, kOtherEndpoint) << "a view redirects to its source's leader";
    EXPECT_EQ(g.servedTotal(), 0u);

    // A full ring: nothing runs the workers, so the op ring fills.
    const auto frame = g.frame(8, pc::StatsScope::TopicPartition, kSrc, 0);
    int32_t status = -1;
    for (int i = 0; i < 1'000'000 && status != SR::kBusy; ++i) {
        g.send(frame);
        status = g.lastStatus();
    }
    EXPECT_EQ(status, SR::kBusy);
}

TEST(CheckedWritesStatsGateway, DescribeIsRequired) {
    StatsGatewayRig g;
    rune::gateway::SecurityView view{rune::security::SecurityState{}};
    g.rig.gw.setAuthMode(rune::gateway::GatewayAuthMode::Required, &view);
    rune::gateway::ObjectIndex idx;
    idx.onTopic(kSrc, "character");
    idx.onView(kView, "account_characters");
    g.rig.gw.setBootObjects(idx);
    g.rig.gw.setSessionAuthForTest(g.rig.sid, rune::gateway::AuthAuthenticated{1, "nobody", rune::gateway::PermissionTable::compile({}, idx)});

    g.send(g.frame(1, pc::StatsScope::TopicPartition, kSrc, 0));
    EXPECT_EQ(g.lastStatus(), SR::kNotAuthorized);
    g.send(g.frame(2, pc::StatsScope::ViewPartition, kView, 0));
    EXPECT_EQ(g.lastStatus(), SR::kNotAuthorized);
    EXPECT_EQ(g.servedTotal(), 0u);
}

TEST(CheckedWritesStatsGateway, EncodesTheResponse) {
    StatsGatewayRig g;
    rune::node::WorkerReturn ret;
    ret.type = rune::node::WorkerReturn::Type::StatsResult;
    ret.sessionId = g.rig.sid; ret.correlationId = 7; ret.readTopicId = kSrc; ret.readPartition = 0;
    ret.isolation = static_cast<uint8_t>(pc::StatsScope::TopicPartition);
    ret.recordCount = 2;
    ret.chunk.resize(2 * SR::kStatBytes + 2 + 2 * SR::kConstraintBytes);
    pc::detail::putLE16(ret.chunk.data(), 0, 1);
    pc::detail::putLE64(ret.chunk.data(), 2, 11);
    pc::detail::putLE16(ret.chunk.data(), 10, 7);
    pc::detail::putLE64(ret.chunk.data(), 12, -3);
    pc::detail::putLE16(ret.chunk.data(), 20, 2);
    pc::detail::putLE32(ret.chunk.data(), 22, 0x40000601);
    pc::detail::putLE64(ret.chunk.data(), 26, 5);
    pc::detail::putLE32(ret.chunk.data(), 34, 9);
    pc::detail::putLE64(ret.chunk.data(), 38, 6);
    g.rig.gw.handleWorkerReturnForTest(ret);

    const auto resp = g.lastStats();
    ASSERT_TRUE(resp.has_value());
    EXPECT_EQ(resp->correlationId, 7);
    EXPECT_EQ(resp->status, SR::kOk);
    std::vector<std::pair<uint16_t, int64_t>> stats;
    resp->forEachStat([&](uint16_t id, int64_t v) { stats.emplace_back(id, v); });
    EXPECT_EQ(stats, (std::vector<std::pair<uint16_t, int64_t>>{{1, 11}, {7, -3}}));
    std::vector<std::pair<uint32_t, uint64_t>> constraints;
    resp->forEachConstraint([&](uint32_t id, uint64_t n) { constraints.emplace_back(id, n); });
    EXPECT_EQ(constraints, (std::vector<std::pair<uint32_t, uint64_t>>{{0x40000601u, 5u}, {9u, 6u}}));
}

TEST(CheckedWritesStatsGateway, NotLeaderSentinelRedirectsByScope) {
    StatsGatewayRig g;
    g.leaderElsewhere(0, kOther, 1);
    for (const auto scope : {pc::StatsScope::TopicPartition, pc::StatsScope::ViewPartition}) {
        rune::node::WorkerReturn ret;
        ret.type = rune::node::WorkerReturn::Type::StatsResult;
        ret.sessionId = g.rig.sid; ret.correlationId = 3; ret.readPartition = 0;
        ret.readTopicId = scope == pc::StatsScope::TopicPartition ? kSrc : kView;
        ret.isolation = static_cast<uint8_t>(scope);
        ret.errorCode = rune::node::kViewRowNotLeaderInternal;
        ret.chunk.assign(2, 0);
        g.rig.gw.handleWorkerReturnForTest(ret);
        const auto err = pc::ErrorResponse::decode(g.rig.lastFrame.data(), g.rig.lastFrame.size());
        ASSERT_TRUE(err.has_value()) << static_cast<int>(scope);
        EXPECT_EQ(err->code, static_cast<int16_t>(pc::ErrorCode::Redirect)) << static_cast<int>(scope);
        EXPECT_EQ(err->message, kOtherEndpoint) << static_cast<int>(scope);
    }
}
