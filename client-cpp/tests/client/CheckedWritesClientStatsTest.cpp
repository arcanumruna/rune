// tests/client/CheckedWritesClientStatsTest.cpp - checked writes Part 08 Task 8: Client::getStats over GetStats (91):
// the frame it writes, the payload decode, the BrokerTooOld gate and the completion a StatsResponse (92) ends as.
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/protocol/client/StatsMessages.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc  = rune::client;
namespace rpc = rune::protocol::client;
using rc::OpStatus;

namespace {

struct DataDirs {
    std::vector<std::string> dirs;
    ~DataDirs() {
        std::error_code ec;
        for (const std::string& d : dirs) std::filesystem::remove_all(d, ec);
    }
    std::string next() {
        dirs.push_back((std::filesystem::temp_directory_path() /
                        ("cw_stats_" + std::to_string(::getpid()) + "_" + std::to_string(dirs.size()))).string());
        return dirs.back();
    }
} g_dataDirs;

rc::ClientConfig invokerCfg(int32_t in, int32_t eg) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = eg;
    c.dataDir         = g_dataDirs.next();
    c.runnerMode      = rc::RunnerMode::Invoker;
    return c;
}

struct Rig {
    std::unique_ptr<rc::Client>  client;   // constructed first: owns the driver
    test_support::InvokerGateway gw;       // destroyed first
    explicit Rig(const rc::ClientConfig& c)
        : client(std::make_unique<rc::Client>(c)),
          gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {}
    template <class P>
    bool drive(P&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            client->doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        test_support::TestCq cq(*client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client->connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
        ASSERT_TRUE(drive([&] { return gw.metadataRequests > 0; }));
    }
};

// A StatsResponse frame carrying {verdictLag 7, verdictWatermark 90, autoIncrementCounter 5} and constraint 4 -> 2.
std::vector<uint8_t> statsFrame(uint16_t status, int64_t sessionId = 0, int64_t correlationId = 0) {
    std::vector<uint8_t> stats(3 * rpc::StatsResponse::kStatBytes);
    const struct { rpc::StatId id; int64_t v; } sorted[] = {   // ascending ids on the wire
        {rpc::StatId::verdictLag, 7}, {rpc::StatId::verdictWatermark, 90}, {rpc::StatId::autoIncrementCounter, 5}};
    for (std::size_t i = 0; i < 3; ++i) {
        rpc::detail::putLE16(stats.data(), static_cast<int>(i * 10), static_cast<uint16_t>(sorted[i].id));
        rpc::detail::putLE64(stats.data(), static_cast<int>(i * 10 + 2), sorted[i].v);
    }
    std::vector<uint8_t> cons(rpc::StatsResponse::kConstraintBytes);
    rpc::detail::putLE32(cons.data(), 0, 4);
    rpc::detail::putLE64(cons.data(), 4, 2);
    rpc::StatsResponse m;
    m.sessionId       = sessionId;
    m.correlationId   = correlationId;
    m.status          = status;
    m.count           = 3;
    m.statBytes       = stats;
    m.constraintCount = 1;
    m.constraintBytes = cons;
    std::vector<uint8_t> out(m.encodedSize());
    m.encode(out.data());
    return out;
}

// The payload a completion carries: the response bytes after `count`.
std::vector<uint8_t> payloadOf(const std::vector<uint8_t>& frame) {
    const std::size_t at = rpc::MessageHeader::ENCODED_SIZE + rpc::StatsResponse::BODY_FIXED;
    return {frame.begin() + static_cast<std::ptrdiff_t>(at), frame.end()};
}

}  // namespace

TEST(CheckedWritesClientStats, DecodePayload) {
    const auto payload = payloadOf(statsFrame(0));
    rc::StatsResult r;
    ASSERT_TRUE(rc::decodeStatsPayload(payload, 3, r));
    EXPECT_EQ(r.get(rpc::StatId::verdictLag), 7);
    EXPECT_EQ(r.get(rpc::StatId::verdictWatermark), 90);
    EXPECT_EQ(r.get(rpc::StatId::scanRows), std::nullopt);
    ASSERT_EQ(r.constraints.size(), 1u);
    EXPECT_EQ(r.constraints[0].constraintId, 4u);
    EXPECT_EQ(r.constraints[0].rejections, 2u);

    for (std::size_t cut = 0; cut < payload.size(); ++cut) {
        rc::StatsResult t;
        EXPECT_FALSE(rc::decodeStatsPayload(std::span<const uint8_t>(payload.data(), cut), 3, t)) << cut;
    }
    auto trailing = payload;
    trailing.push_back(0);
    rc::StatsResult t;
    EXPECT_FALSE(rc::decodeStatsPayload(trailing, 3, t));
}

TEST(CheckedWritesClientStats, BrokerTooOldIsLocal) {
    Rig r(invokerCfg(4701, 4702));
    r.gw.advertisedBrokerVersion = rpc::kBrokerVersionCheckedWrites - 1;
    int sent = 0;
    r.gw.templateHandlers[rpc::GetStats::TEMPLATE_ID] = [&](int, const uint8_t*, std::size_t) { ++sent; };
    r.connect();
    // GetStats is gated on kBrokerVersionCheckedWrites: the version just below it is too old.
    test_support::TestCq cq(*r.client);
    auto get = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client->getStats(rpc::StatsScope::TopicPartition, 3, 0, q, t);
    });
    ASSERT_TRUE(r.drive([&] { return get.ready(); }));
    EXPECT_EQ(get.status(), OpStatus::BrokerTooOld);
    EXPECT_EQ(sent, 0);
}

TEST(CheckedWritesClientStats, FrameIsTheWireMessage) {
    Rig r(invokerCfg(4703, 4704));
    std::vector<rpc::GetStats> seen;
    r.gw.templateHandlers[rpc::GetStats::TEMPLATE_ID] = [&](int, const uint8_t* f, std::size_t n) {
        auto m = rpc::GetStats::decode(f, n);
        ASSERT_TRUE(m.has_value());
        seen.push_back(*m);
    };
    r.connect();
    test_support::TestCq cq(*r.client);
    auto get = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client->getStats(rpc::StatsScope::ViewPartition, 12, 0, q, t);
    });
    ASSERT_TRUE(get.accepted());
    ASSERT_TRUE(r.drive([&] { return !seen.empty(); }));
    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0].scope, rpc::StatsScope::ViewPartition);
    EXPECT_EQ(seen[0].objectId, 12);
    EXPECT_EQ(seen[0].partition, 0);
}

TEST(CheckedWritesClientStats, ResponseCompletesWithTheCountAndPayload) {
    Rig r(invokerCfg(4705, 4706));
    r.gw.templateHandlers[rpc::GetStats::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t n) {
        auto m = rpc::GetStats::decode(f, n);
        ASSERT_TRUE(m.has_value());
        const auto resp = statsFrame(0, m->sessionId, m->correlationId);
        r.gw.send(ep, resp.data(), resp.size());
    };
    r.connect();
    test_support::TestCq cq(*r.client);
    auto get = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client->getStats(rpc::StatsScope::TopicPartition, 3, 0, q, t);
    });
    ASSERT_TRUE(r.drive([&] { return get.ready(); }));
    ASSERT_EQ(get.status(), OpStatus::Ok) << get.describe();
    EXPECT_EQ(get.outcome().c.value, 3);
    rc::StatsResult res;
    ASSERT_TRUE(rc::decodeStatsPayload(get.outcome().payload, 3, res));
    EXPECT_EQ(res.get(rpc::StatId::autoIncrementCounter), 5);
}

TEST(CheckedWritesClientStats, NotAuthorizedRidesAsBrokerError) {
    Rig r(invokerCfg(4707, 4708));
    r.gw.templateHandlers[rpc::GetStats::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t n) {
        auto m = rpc::GetStats::decode(f, n);
        ASSERT_TRUE(m.has_value());
        rpc::StatsResponse resp;
        resp.sessionId     = m->sessionId;
        resp.correlationId = m->correlationId;
        resp.status        = rpc::StatsResponse::kNotAuthorized;
        std::vector<uint8_t> out(resp.encodedSize());
        r.gw.send(ep, out.data(), resp.encode(out.data()));
    };
    r.connect();
    test_support::TestCq cq(*r.client);
    auto get = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client->getStats(rpc::StatsScope::ViewPartition, 12, 0, q, t);
    });
    ASSERT_TRUE(r.drive([&] { return get.ready(); }));
    EXPECT_EQ(get.status(), OpStatus::BrokerError);
    EXPECT_EQ(get.outcome().c.errorCode, rpc::StatsResponse::kNotAuthorized);
}

TEST(CheckedWritesClientStats, NotLeaderUntilTheBudgetEndsIsKNotLeader) {
    rc::ClientConfig cfg = invokerCfg(4709, 4710);
    cfg.viewReadRetryBudget = std::chrono::milliseconds(300);
    Rig r(cfg);
    int sent = 0;
    r.gw.templateHandlers[rpc::GetStats::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t n) {
        auto m = rpc::GetStats::decode(f, n);
        ASSERT_TRUE(m.has_value());
        ++sent;
        r.gw.sendError(ep, static_cast<int16_t>(rpc::ErrorCode::NotLeader), "", m->sessionId, m->correlationId);
    };
    r.connect();
    test_support::TestCq cq(*r.client);
    auto get = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client->getStats(rpc::StatsScope::TopicPartition, 3, 0, q, t);
    });
    ASSERT_TRUE(r.drive([&] { return get.ready(); }));
    EXPECT_EQ(get.status(), OpStatus::BrokerError);
    EXPECT_EQ(get.outcome().c.errorCode, rc::StatsResult::kNotLeader);
    EXPECT_GT(sent, 1) << "the request was re-sent inside the budget";
}

TEST(CheckedWritesClientStatsRegression, StatusTwoIsRoutedLikeNotLeaderRegression) {
    rc::ClientConfig cfg = invokerCfg(4711, 4712);
    cfg.viewReadRetryBudget = std::chrono::milliseconds(300);
    Rig r(cfg);
    int sent = 0;
    r.gw.templateHandlers[rpc::GetStats::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t n) {
        auto m = rpc::GetStats::decode(f, n);
        ASSERT_TRUE(m.has_value());
        ++sent;
        rpc::StatsResponse resp;
        resp.sessionId     = m->sessionId;
        resp.correlationId = m->correlationId;
        resp.status        = 2;
        std::vector<uint8_t> out(resp.encodedSize());
        r.gw.send(ep, out.data(), resp.encode(out.data()));
    };
    r.connect();
    test_support::TestCq cq(*r.client);
    auto get = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client->getStats(rpc::StatsScope::TopicPartition, 3, 0, q, t);
    });
    ASSERT_TRUE(r.drive([&] { return get.ready(); }));
    EXPECT_EQ(get.outcome().c.errorCode, rc::StatsResult::kNotLeader);
    EXPECT_GT(sent, 1) << "status 2 is retried, not delivered as a status";
}

TEST(CheckedWritesClientStats, BlockingFormReportsBrokerTooOldAsAStatus) {
    Rig r(invokerCfg(4713, 4714));
    r.gw.advertisedBrokerVersion = rpc::kBrokerVersionCheckedWrites - 1;
    r.connect();
    rc::StatsResult res;
    std::atomic<bool> done{false};
    // The blocking form pumps the client itself; the fake gateway must keep running beside it.
    std::thread pump([&] { while (!done) r.gw.pump(); });
    res = r.client->getStatsAndWait(rpc::StatsScope::TopicPartition, 3, 0, std::chrono::seconds(5));
    done = true;
    pump.join();
    EXPECT_EQ(res.status, rc::StatsResult::kBrokerTooOld);
}
