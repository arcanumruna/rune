// Catalog history §5.1/§5.2/§9: every DDL follows Redirect, retries NotLeader inside
// its budget, then fails explicitly; the requestId is stable across every resend.
// Non-blocking ops on the single-threaded fake (F7): the test thread drives
// client.doWork() + gw.pump(); the blocking *AndWait forms are proven against
// real BrokerNodes (Task 11: one node; Task 22: three nodes).
#include <filesystem>
#include "../support/FreePort.h"
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"
#include <rune/client/Client.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <optional>
#include <span>
#include <stdexcept>
#include <thread>
#include <string>
#include <sys/socket.h>
#include <tuple>
#include <unistd.h>
#include <vector>

namespace pc = rune::protocol::client;
namespace rc = rune::client;

namespace {
int64_t g_now = 1'000'000'000;                      // test thread only: invoker mode has one thread
int64_t fakeNow() noexcept { return g_now; }
int g_ctr = 0;                                      // test thread only

// Never a fixed UDP port (GroupRoutingRegressionTest MEDIUM-6): ctest -j runs
// test processes in parallel — the kernel picks a free one.
std::string udp(int port) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port); }

rc::ClientConfig invokerCfg(int32_t in, int32_t eg) {   // as InvokerClientTest.cpp's invokerCfg, plus the clock
    rc::ClientConfig c;
    c.gatewayChannel = "aeron:ipc"; c.gatewayStreamId = in; c.egressChannel = "aeron:ipc"; c.egressStreamId = eg;
    c.dataDir = std::filesystem::temp_directory_path().string() + "/ddl_redirect_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    c.runnerMode = rc::RunnerMode::Invoker;
    c.nanoClock = &fakeNow;
    return c;
}
struct Rig {   // InvokerClientTest.cpp's Rig: client + fake on the client's embedded driver
    rc::Client                   client;
    test_support::InvokerGateway gw;
    explicit Rig(const rc::ClientConfig& c)
        : client(c), gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {}
    template <class P> bool drive(P&& pred, int64_t stepNs = 0) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            g_now += stepNs; client.doWork(); gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    void passes(int n, int64_t stepNs = 0) { for (int i = 0; i < n; ++i) { g_now += stepNs; client.doWork(); gw.pump(); } }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        test_support::TestCq cq(client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client.connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }
    void answerError(int ep, pc::ErrorCode code, const std::string& msg, int64_t session, int64_t corr) {
        gw.sendError(ep, static_cast<int16_t>(code), msg, session, corr);
    }
};
std::string payloadText(const test_support::PendingOp& f) {
    const auto b = f.bytes(); return {b.begin(), b.end()};
}
} // namespace

TEST(ClientDdlRedirectRegression, NotLeaderIsRetriedWithTheSameRequestIdThenSucceeds) {
    Rig r(invokerCfg(3960, 3961));
    r.connect();
    std::vector<rune::protocol::RequestId> seen;
    int n = 0;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        seen.push_back(m->ext.requestId);
        if (n++ < 2) { r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId); return; }
        pc::DdlResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId;
        resp.objectId = 4; resp.newVersion = 2;
        r.gw.sendDdlResponse(ep, resp);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/50'000'000));   // walks the 100 ms → 200 ms backoff
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(f.outcome().c.value, 4);
    EXPECT_EQ(f.outcome().c.aux, 2);
    ASSERT_EQ(seen.size(), 3u);
    EXPECT_FALSE(seen[0].none());
    EXPECT_NE(seen[0].hi, 0u) << "the random client instance id (F14)";
    EXPECT_EQ(seen[0], seen[1]);
    EXPECT_EQ(seen[1], seen[2]);
}

TEST(ClientDdlRedirectRegression, NotLeaderPastTheBudgetIsAnExplicitFailureNotAHang) {
    rc::ClientConfig cfg = invokerCfg(3962, 3963);
    cfg.ddlRetryBudget = std::chrono::milliseconds(400);
    Rig r(cfg);
    r.connect();
    r.gw.templateHandlers[pc::DefineTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DefineTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    const int64_t t0 = g_now;
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.defineTopic("t", 1, "", {}, false, rc::Persistence::Disk, {}, {}, q, t);
    });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/20'000'000));
    ASSERT_TRUE(f.failed());
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(f.outcome().c.errorCode, static_cast<uint16_t>(pc::ErrorCode::NotLeader));
    EXPECT_LT(g_now - t0, int64_t{2'000'000'000}) << "the 400 ms budget, not the op timeout, ends it";
}

TEST(ClientDdlRedirectRegression, RedirectIsFollowedToTheLeaderEndpoint) {
    Rig r(invokerCfg(3964, 3965));
    const std::string leaderCh = udp(rune::client::testing::allocFreeUdpPort());
    const int leader = r.gw.addNode(leaderCh, 3964);          // a second fake node (IngressEndpointRoutingTest)
    r.connect();
    int leaderSaw = 0;
    r.gw.templateHandlers[pc::AlterTopicConfig::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::AlterTopicConfig::decode(f, len);
        ASSERT_TRUE(m.has_value());
        if (ep != leader) { r.answerError(ep, pc::ErrorCode::Redirect, leaderCh, m->sessionId, m->correlationId); return; }
        ++leaderSaw;
        pc::DdlResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId; resp.newVersion = 3;
        r.gw.sendDdlResponse(ep, resp);
    };
    const pc::ConfigChangeWire ch{1, 60'000};
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.alterTopicConfig("t", 0, std::span(&ch, 1), {}, q, t);
    });
    // 1 ms per pass: the leader's UDP session retries its ConnectRequest on the duty clock.
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/1'000'000));
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(leaderSaw, 1);
    EXPECT_EQ(f.outcome().c.aux, 3);
}

TEST(ClientDdlRedirectRegression, RedirectLoopEndsExplicitly) {
    Rig r(invokerCfg(3966, 3967));
    const std::string loopCh = udp(rune::client::testing::allocFreeUdpPort());
    (void)r.gw.addNode(loopCh, 3966);
    r.connect();
    r.gw.templateHandlers[pc::DropView::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropView::decode(f, len);
        ASSERT_TRUE(m.has_value());
        r.answerError(ep, pc::ErrorCode::Redirect, loopCh, m->sessionId, m->correlationId);   // every node: "ask loopCh"
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropView("v", {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/1'000'000));   // UDP connect retry: duty clock
    ASSERT_TRUE(f.failed());
    EXPECT_EQ(f.outcome().c.errorCode, static_cast<uint16_t>(pc::ErrorCode::Redirect));
    EXPECT_NE(payloadText(f).find("redirected more than"), std::string::npos);
}

TEST(ClientDdlRedirectRegression, InvalidRequestDetailReachesTheCaller) {
    Rig r(invokerCfg(3968, 3969));
    r.connect();
    r.gw.templateHandlers[pc::DefineTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DefineTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        r.answerError(ep, pc::ErrorCode::InvalidRequest, "replicationFactor 5 > broker count 3", m->sessionId,
                      m->correlationId);
    };
    rc::TopicOptions topic; topic.replicationFactor = 5;
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.defineTopic("t", 1, "", {}, false, rc::Persistence::Disk, topic, {}, q, t);
    });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_EQ(f.outcome().c.errorCode, 39);
    EXPECT_EQ(payloadText(f), "replicationFactor 5 > broker count 3");
}

// ---------------------------------------------------------------------------
// Whole-class matrix: EVERY DDL op the client sends × EVERY answer class —
// OK, Redirect (followed to the named node), NotLeader (resent with the same
// requestId), InvalidRequest (detail surfaced), no answer (explicit Timeout).
// No cell may hang or swallow the answer.
// ---------------------------------------------------------------------------
namespace {
enum class DdlOp { DefineTopic, AlterTopic, CreateView, DropTopic, DropView, AlterView, AlterTopicConfig };
enum class Answer { Ok, RedirectThenOk, NotLeaderThenOk, InvalidRequest, NoAnswer };

const char* name(DdlOp o) {
    switch (o) {
    case DdlOp::DefineTopic: return "DefineTopic";
    case DdlOp::AlterTopic: return "AlterTopic";
    case DdlOp::CreateView: return "CreateView";
    case DdlOp::DropTopic: return "DropTopic";
    case DdlOp::DropView: return "DropView";
    case DdlOp::AlterView: return "AlterView";
    case DdlOp::AlterTopicConfig: return "AlterTopicConfig";
    }
    return "?";
}
const char* name(Answer a) {
    switch (a) {
    case Answer::Ok: return "Ok";
    case Answer::RedirectThenOk: return "RedirectThenOk";
    case Answer::NotLeaderThenOk: return "NotLeaderThenOk";
    case Answer::InvalidRequest: return "InvalidRequest";
    case Answer::NoAnswer: return "NoAnswer";
    }
    return "?";
}
uint16_t templateOf(DdlOp o) {
    switch (o) {
    case DdlOp::DefineTopic: return pc::DefineTopic::TEMPLATE_ID;
    case DdlOp::AlterTopic: return pc::AlterTopic::TEMPLATE_ID;
    case DdlOp::CreateView: return pc::CreateView::TEMPLATE_ID;
    case DdlOp::DropTopic: return pc::DropTopic::TEMPLATE_ID;
    case DdlOp::DropView: return pc::DropView::TEMPLATE_ID;
    case DdlOp::AlterView: return pc::AlterView::TEMPLATE_ID;
    case DdlOp::AlterTopicConfig: return pc::AlterTopicConfig::TEMPLATE_ID;
    }
    return 0;
}
/// The DDL extension of a frame of op `o` (nullopt: it did not decode or carried none).
std::optional<pc::DdlExtension> extOf(DdlOp o, const uint8_t* f, std::size_t len) {
    switch (o) {
    case DdlOp::DefineTopic: { auto m = pc::DefineTopic::decode(f, len); if (m && m->hasExt) return m->ext; break; }
    case DdlOp::AlterTopic: { auto m = pc::AlterTopic::decode(f, len); if (m && m->hasExt) return m->ext; break; }
    case DdlOp::CreateView: { auto m = pc::CreateView::decode(f, len); if (m && m->hasExt) return m->ext; break; }
    case DdlOp::DropTopic: { auto m = pc::DropTopic::decode(f, len); if (m) return m->ext; break; }
    case DdlOp::DropView: { auto m = pc::DropView::decode(f, len); if (m) return m->ext; break; }
    case DdlOp::AlterView: { auto m = pc::AlterView::decode(f, len); if (m) return m->ext; break; }
    case DdlOp::AlterTopicConfig: { auto m = pc::AlterTopicConfig::decode(f, len); if (m) return m->ext; break; }
    }
    return std::nullopt;
}
/// The op's own success answer: value 9 / aux 5 where the response carries them.
void answerOk(test_support::InvokerGateway& gw, DdlOp o, int ep, int64_t sid, int64_t corr) {
    std::vector<uint8_t> out(256);
    std::size_t n = 0;
    switch (o) {
    case DdlOp::DefineTopic: {
        pc::DefineTopicResponse r; r.sessionId = sid; r.correlationId = corr; r.topicId = 9; r.version = 5;
        n = r.encode(out.data()); break;
    }
    case DdlOp::AlterTopic: {
        pc::AlterTopicResponse r; r.sessionId = sid; r.correlationId = corr; r.topicId = 9; r.newVersion = 5;
        n = r.encode(out.data()); break;
    }
    case DdlOp::CreateView: {
        pc::CreateViewResponse r; r.sessionId = sid; r.correlationId = corr; r.viewTopicId = 9; r.outputFingerprint = 5;
        n = r.encode(out.data()); break;
    }
    default: {
        pc::DdlResponse r; r.sessionId = sid; r.correlationId = corr; r.objectId = 9; r.newVersion = 5;
        n = r.encode(out.data()); break;
    }
    }
    gw.send(ep, out.data(), n);
}
rc::OpResult issue(rc::Client& c, DdlOp o, rc::Completions& q, rc::Token t) {
    static const uint8_t kBlob[] = {1, 2, 3};
    const std::span<const uint8_t> blob(kBlob, sizeof kBlob);
    static const pc::ConfigChangeWire kChange{1, 60'000};
    switch (o) {
    case DdlOp::DefineTopic: return c.defineTopic("t", 1, "", {}, false, rc::Persistence::Disk, {}, {}, q, t);
    case DdlOp::AlterTopic: return c.alterTopic("t", 1, blob, {}, q, t);
    case DdlOp::CreateView: return c.createView("v", "k", 0, blob, {}, q, t);
    case DdlOp::DropTopic: return c.dropTopic("t", {}, q, t);
    case DdlOp::DropView: return c.dropView("v", {}, q, t);
    case DdlOp::AlterView: return c.alterView("v", "k", 0, 1, blob, {}, q, t);
    case DdlOp::AlterTopicConfig: return c.alterTopicConfig("t", 1, std::span(&kChange, 1), {}, q, t);
    }
    return rc::OpResult::InvalidArgument;
}
} // namespace

class ClientDdlAnswerMatrixRegression : public ::testing::TestWithParam<std::tuple<DdlOp, Answer>> {};

TEST_P(ClientDdlAnswerMatrixRegression, EveryDdlOpHandlesEveryAnswerClass) {
    const auto [op, answer] = GetParam();
    rc::ClientConfig cfg = invokerCfg(3970, 3971);
    // NoAnswer ends at the DDL deadline: 2 s keeps it inside the idle fake
    // session's liveness window (past it the session, not the op, would end).
    cfg.ddlTimeoutMs = 2'000;
    Rig r(cfg);
    const std::string leaderCh = udp(rune::client::testing::allocFreeUdpPort());
    const int leader = r.gw.addNode(leaderCh, 3970);
    r.connect();
    std::vector<rune::protocol::RequestId> ids;
    std::vector<int> arrivedAt;
    r.gw.templateHandlers[templateOf(op)] = [&](int ep, const uint8_t* f, std::size_t len) {
        const auto ext = extOf(op, f, len);
        ASSERT_TRUE(ext.has_value()) << "every DDL carries the v8 extension";
        ids.push_back(ext->requestId);
        arrivedAt.push_back(ep);
        const auto h = pc::MessageHeader::decode(f, len);
        ASSERT_TRUE(h.has_value());
        const bool first = arrivedAt.size() == 1;
        switch (answer) {
        case Answer::Ok: answerOk(r.gw, op, ep, h->sessionId, h->correlationId); break;
        case Answer::RedirectThenOk:
            if (ep != leader) r.answerError(ep, pc::ErrorCode::Redirect, leaderCh, h->sessionId, h->correlationId);
            else answerOk(r.gw, op, ep, h->sessionId, h->correlationId);
            break;
        case Answer::NotLeaderThenOk:
            if (first) r.answerError(ep, pc::ErrorCode::NotLeader, "", h->sessionId, h->correlationId);
            else answerOk(r.gw, op, ep, h->sessionId, h->correlationId);
            break;
        case Answer::InvalidRequest:
            r.answerError(ep, pc::ErrorCode::InvalidRequest, "bad ddl", h->sessionId, h->correlationId);
            break;
        case Answer::NoAnswer: break;
        }
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return issue(r.client, op, q, t); });
    ASSERT_TRUE(f.accepted());
    // A real-ish clock: 10 ms per pass walks the NotLeader backoff and the op deadline.
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/10'000'000)) << "the op must end (no hang)";
    ASSERT_FALSE(ids.empty());
    for (const auto& id : ids) {
        EXPECT_FALSE(id.none());
        EXPECT_EQ(id, ids.front()) << "the requestId is stable across every resend";
    }
    switch (answer) {
    case Answer::Ok:
    case Answer::RedirectThenOk:
    case Answer::NotLeaderThenOk:
        ASSERT_TRUE(f.ok()) << f.describe();
        EXPECT_EQ(f.outcome().c.value, 9);
        EXPECT_EQ(f.outcome().c.aux, 5);
        if (answer == Answer::RedirectThenOk) {
            ASSERT_EQ(arrivedAt.size(), 2u);
            EXPECT_EQ(arrivedAt[0], 0);
            EXPECT_EQ(arrivedAt[1], leader) << "the Redirect was followed to the named node";
        } else if (answer == Answer::NotLeaderThenOk) {
            ASSERT_EQ(arrivedAt.size(), 2u) << "resent once after NotLeader";
        } else {
            EXPECT_EQ(arrivedAt.size(), 1u);
        }
        break;
    case Answer::InvalidRequest:
        EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError);
        EXPECT_EQ(f.outcome().c.errorCode, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest));
        EXPECT_EQ(payloadText(f), "bad ddl");
        EXPECT_EQ(arrivedAt.size(), 1u) << "never retried";
        break;
    case Answer::NoAnswer:
        EXPECT_EQ(f.outcome().c.status, rc::OpStatus::Timeout);
        EXPECT_EQ(arrivedAt.size(), 1u);
        break;
    }
}

INSTANTIATE_TEST_SUITE_P(
    AllOpsAllAnswers, ClientDdlAnswerMatrixRegression,
    ::testing::Combine(::testing::Values(DdlOp::DefineTopic, DdlOp::AlterTopic, DdlOp::CreateView, DdlOp::DropTopic,
                                         DdlOp::DropView, DdlOp::AlterView, DdlOp::AlterTopicConfig),
                       ::testing::Values(Answer::Ok, Answer::RedirectThenOk, Answer::NotLeaderThenOk,
                                         Answer::InvalidRequest, Answer::NoAnswer)),
    [](const ::testing::TestParamInfo<std::tuple<DdlOp, Answer>>& i) {
        return std::string(name(std::get<0>(i.param))) + "_" + name(std::get<1>(i.param));
    });

// A typed (pre-v8 shaped) response carrying NotLeader (2) follows the NotLeader
// rule too — it is not a terminal broker error.
TEST(ClientDdlRedirectRegression, TypedResponseWithNotLeaderCodeIsRetried) {
    Rig r(invokerCfg(3972, 3973));
    r.connect();
    int n = 0;
    r.gw.templateHandlers[pc::AlterTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::AlterTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        pc::AlterTopicResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId;
        resp.topicId = 3; resp.newVersion = 2;
        resp.errorCode = n++ == 0 ? static_cast<uint16_t>(pc::ErrorCode::NotLeader) : 0;
        std::vector<uint8_t> out(resp.encodedSize());
        r.gw.send(ep, out.data(), resp.encode(out.data()));
    };
    static const uint8_t kOps[] = {0};
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.alterTopic("t", 1, std::span<const uint8_t>(kOps, 1), {}, q, t);
    });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/50'000'000));
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(n, 2);
    EXPECT_EQ(f.outcome().c.aux, 2);
}

// A DdlResponse carrying NotLeader (2) is retried; past the budget it fails
// explicitly with that code and the broker's detail.
TEST(ClientDdlRedirectRegression, DdlResponseNotLeaderPastTheBudgetFailsWithItsDetail) {
    rc::ClientConfig cfg = invokerCfg(3974, 3975);
    cfg.ddlRetryBudget = std::chrono::milliseconds(300);
    Rig r(cfg);
    r.connect();
    int n = 0;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        ++n;
        pc::DdlResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId;
        resp.errorCode = static_cast<uint16_t>(pc::ErrorCode::NotLeader); resp.detail = "catalog leader catching up";
        r.gw.sendDdlResponse(ep, resp);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/20'000'000));
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(f.outcome().c.errorCode, static_cast<uint16_t>(pc::ErrorCode::NotLeader));
    EXPECT_GE(n, 2) << "retried inside the budget";
    EXPECT_NE(payloadText(f).find("catalog leader catching up"), std::string::npos);
}

// A specific DDL code in a DdlResponse (e.g. TopicExists 25) is terminal and
// carries objectId / newVersion / detail exactly as answered.
TEST(ClientDdlRedirectRegression, DdlResponseSpecificCodeIsSurfacedNotRetried) {
    Rig r(invokerCfg(3976, 3977));
    r.connect();
    int n = 0;
    r.gw.templateHandlers[pc::AlterView::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::AlterView::decode(f, len);
        ASSERT_TRUE(m.has_value());
        ++n;
        pc::DdlResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId;
        resp.errorCode = 36; resp.objectId = 11; resp.newVersion = 4; resp.detail = "stale expectedVersion";
        r.gw.sendDdlResponse(ep, resp);
    };
    static const uint8_t kPlan[] = {1};
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.alterView("v", "k", 0, 3, std::span<const uint8_t>(kPlan, 1), {}, q, t);
    });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_EQ(n, 1);
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(f.outcome().c.errorCode, 36);
    EXPECT_EQ(f.outcome().c.value, 11);
    EXPECT_EQ(f.outcome().c.aux, 4);
    EXPECT_EQ(payloadText(f), "stale expectedVersion");
}

// A Redirect to a node whose session never connects holds the frame for that
// node — and the op still ends, explicitly, at its deadline (never a hang).
TEST(ClientDdlRedirectRegression, RedirectToAnUnreachableNodeEndsInTimeout) {
    rc::ClientConfig cfg = invokerCfg(3978, 3979);
    cfg.ddlTimeoutMs = 500;   // the DDL deadline
    Rig r(cfg);
    r.connect();
    const std::string nowhere = udp(rune::client::testing::allocFreeUdpPort());   // no fake node listens there
    int seen = 0;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        ++seen;
        r.answerError(ep, pc::ErrorCode::Redirect, nowhere, m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/10'000'000));
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::Timeout);
    EXPECT_EQ(seen, 1);
}

// Redirect, then NotLeader from the leader: the resend goes to the LEADER (not
// back to the seed) with the same requestId, and the answer completes the op.
TEST(ClientDdlRedirectRegression, NotLeaderAfterARedirectResendsToTheLeaderWithTheSameRequestId) {
    Rig r(invokerCfg(3998, 3999));
    const std::string leaderCh = udp(rune::client::testing::allocFreeUdpPort());
    const int leader = r.gw.addNode(leaderCh, 3998);
    r.connect();
    std::vector<int> at;
    std::vector<rune::protocol::RequestId> ids;
    r.gw.templateHandlers[pc::CreateView::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::CreateView::decode(f, len);
        ASSERT_TRUE(m.has_value() && m->hasExt);
        at.push_back(ep);
        ids.push_back(m->ext.requestId);
        if (ep != leader) { r.answerError(ep, pc::ErrorCode::Redirect, leaderCh, m->sessionId, m->correlationId); return; }
        if (at.size() == 2) { r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId); return; }
        pc::CreateViewResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId;
        resp.viewTopicId = 12;
        std::vector<uint8_t> out(resp.encodedSize());
        r.gw.send(ep, out.data(), resp.encode(out.data()));
    };
    static const uint8_t kPlan[] = {1};
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.createView("v", "k", 0, std::span<const uint8_t>(kPlan, 1), {}, q, t);
    });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/10'000'000));
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(f.outcome().c.value, 12);
    ASSERT_EQ(at.size(), 3u);
    EXPECT_EQ(at[0], 0);
    EXPECT_EQ(at[1], leader);
    EXPECT_EQ(at[2], leader);
    EXPECT_EQ(ids[0], ids[1]);
    EXPECT_EQ(ids[1], ids[2]);
}

// Fix round 0 (A): the broker holds a CreateTopic OK until the topic serves (up
// to 5 s).  A DDL's deadline is its own (ClientConfig::ddlTimeoutMs, 15 s by
// default) — not the 5 s control-op deadline, which would expire exactly as
// the broker answers.
TEST(ClientDdlRedirectRegression, DdlAnsweredAfterSixSecondsSucceedsRegression) {
    Rig r(invokerCfg(4430, 4431));   // default config
    r.connect();
    int ep = -1;
    int64_t sid = 0, corr = 0;
    r.gw.templateHandlers[pc::DefineTopic::TEMPLATE_ID] = [&](int e, const uint8_t* f, std::size_t len) {
        auto m = pc::DefineTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        ep = e; sid = m->sessionId; corr = m->correlationId;   // held: answered later
    };
    test_support::TestCq cq(r.client);
    const int64_t t0 = g_now;
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) {
        return r.client.defineTopic("t", 1, "", {}, false, rc::Persistence::Disk, {}, {}, q, t);
    });
    ASSERT_TRUE(f.accepted());
    ASSERT_FALSE(r.drive([&] { return f.ready() || g_now - t0 >= 6'000'000'000; }, /*stepNs=*/10'000'000) &&
                 f.ready())
        << "the DDL must still be pending at 6 s: " << f.describe();
    ASSERT_GE(ep, 0);
    pc::DefineTopicResponse resp; resp.sessionId = sid; resp.correlationId = corr; resp.topicId = 21; resp.version = 1;
    std::vector<uint8_t> out(resp.encodedSize());
    r.gw.send(ep, out.data(), resp.encode(out.data()));
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(f.outcome().c.value, 21);
}

// Fix round 0 (A): ddlTimeoutMs bounds the WHOLE DDL — an unanswered one ends
// Timeout at that deadline, and the NotLeader retry budget never outlives it.
TEST(ClientDdlRedirectRegression, DdlTimeoutBoundsTheWholeDdlIncludingItsRetryBudgetRegression) {
    rc::ClientConfig cfg = invokerCfg(4432, 4433);
    cfg.ddlTimeoutMs   = 700;
    cfg.ddlRetryBudget = std::chrono::milliseconds(60'000);
    Rig r(cfg);
    r.connect();
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    const int64_t t0 = g_now;
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/10'000'000));
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(f.outcome().c.errorCode, static_cast<uint16_t>(pc::ErrorCode::NotLeader));
    EXPECT_LT(g_now - t0, int64_t{1'000'000'000}) << "the 700 ms DDL deadline, not the 60 s budget";
}

// Fix round 0 (C): the seed session's ConnectRequest correlationIds and the
// Client's control-op correlationIds come from two counters that both counted
// from 1 — the n-th connect and the n-th control op carried the SAME id, so an
// answer to one was taken for the other.  Here the seed's connect and a pending
// control op are counter value n of their own spaces; a (late) answer to the
// connect must not complete the control op, and the control op's own answer
// still completes it.  The id spaces are disjoint for every control op.
TEST(ClientCorrIdRegression, ConnectAnswerAndControlAnswerFromTheSameCounterValueAreEachRoutedRegression) {
    rc::ClientConfig cfg = invokerCfg(4434, 4435);
    cfg.connectTimeoutMs = 1'000;   // = the session's per-attempt connect timeout
    Rig r(cfg);
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    // The node drops the first ConnectRequests: each timed-out attempt takes the
    // seed's NEXT connect id (the first connect op times out meanwhile; the
    // session keeps trying), while the Client's counter does not move.
    r.gw.silentEndpoints.insert(0);
    test_support::TestCq cq(r.client);
    auto first = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.connect(q, t); });
    ASSERT_TRUE(r.drive([&] { return r.gw.connectCorrIds.size() >= 3; }, /*stepNs=*/10'000'000));
    r.gw.silentEndpoints.clear();
    ASSERT_TRUE(r.drive([&] { return r.client.state() == rune::client::session::SessionState::Connected; },
                        /*stepNs=*/10'000'000));
    auto conn = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.connect(q, t); });
    ASSERT_TRUE(r.drive([&] { return conn.ready(); }));
    ASSERT_TRUE(conn.ok()) << conn.describe();
    const int64_t connectCorr = r.gw.connectCorrIds.back();   // the connect that was answered

    int     ep = -1;
    int64_t sid = 0, ddlCorr = 0;
    bool    held = false;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int e, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        ep = e; sid = m->sessionId; ddlCorr = m->correlationId;
        if (m->correlationId < connectCorr) {   // below the connect's id: answer, the test issues another
            pc::DdlResponse resp; resp.sessionId = sid; resp.correlationId = ddlCorr;
            r.gw.sendDdlResponse(e, resp);
        } else {
            held = true;                          // at / past it: held until the end
        }
    };
    test_support::PendingOp f;
    for (int n = 0; n < 64 && !held; ++n) {   // a control op whose counter value reaches the connect's
        f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
        ASSERT_TRUE(r.drive([&] { return held || f.ready(); }));
        if (!held) ASSERT_TRUE(f.ok()) << f.describe();
    }
    ASSERT_TRUE(held);
    // A (late) answer to the CONNECT, carrying the connect's own correlationId.
    r.answerError(0, pc::ErrorCode::InvalidRequest, "late answer to the connect", sid, connectCorr);
    for (int i = 0; i < 200; ++i) { r.client.doWork(); r.gw.pump(); }
    ASSERT_FALSE(f.ready()) << "a connect answer completed a control op (ids " << connectCorr << " / " << ddlCorr
                            << "): " << f.describe();
    EXPECT_NE(ddlCorr, connectCorr) << "control-op ids and connect ids are disjoint";
    EXPECT_EQ(r.client.state(), rune::client::session::SessionState::Connected);
    // The control op's own answer completes it.
    pc::DdlResponse resp; resp.sessionId = sid; resp.correlationId = ddlCorr; resp.objectId = 5;
    r.gw.sendDdlResponse(ep, resp);
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(f.outcome().c.value, 5);
}

// Task 5 re-review F3: repeated leader moves — every hop answers Redirect to the
// next node — reach the Redirect cap and end the DDL explicitly with BrokerError
// Redirect (3), never a hang and never an endless hop; every hop carries the
// SAME requestId.
TEST(ClientDdlRedirectRegression, RepeatedRedirectsReachTheCapAndEndBrokerErrorRedirectRegression) {
    Rig r(invokerCfg(3976, 3977));
    r.gw.uniqueSessionIds = true;
    const std::string aCh = udp(rune::client::testing::allocFreeUdpPort());
    const std::string bCh = udp(rune::client::testing::allocFreeUdpPort());
    const int a = r.gw.addNode(aCh, 3976);
    const int b = r.gw.addNode(bCh, 3976);
    r.connect();
    std::vector<std::pair<int, rune::protocol::RequestId>> hops;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        hops.emplace_back(ep, m->ext.requestId);
        // seed -> A -> B -> A -> B ...: the leader keeps moving.
        r.answerError(ep, pc::ErrorCode::Redirect, ep == a ? bCh : aCh, m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/10'000'000));
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(f.outcome().c.errorCode, static_cast<uint16_t>(pc::ErrorCode::Redirect));
    EXPECT_NE(payloadText(f).find("redirected more than"), std::string::npos) << payloadText(f);
    ASSERT_EQ(hops.size(), static_cast<std::size_t>(rc::Client::kMaxDdlRedirects) + 1)
        << "the seed + one hop per followed Redirect, then the cap";
    EXPECT_EQ(hops[1].first, a);
    EXPECT_EQ(hops[2].first, b);
    for (const auto& h : hops) EXPECT_EQ(h.second, hops[0].second) << "the SAME requestId on every hop";
    r.passes(100, 10'000'000);
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);
}

// Task 5 re-review F4: a requestId stamp that would not fit the frame (the
// issue-time decode check bypassed through the test seam) fails the DDL
// InvalidArgument on the duty thread — it is never sent carrying {0,0}.
TEST(ClientDdlRedirectRegression, ASkippedRequestIdStampFailsTheOpAndIsNeverSentRegression) {
    Rig r(invokerCfg(3974, 3975));
    r.connect();
    int arrivals = 0;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int, const uint8_t*, std::size_t) { ++arrivals; };
    r.client.testSkewNextDdlRequestIdAt(1);
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::InvalidArgument);
    EXPECT_NE(payloadText(f).find("requestId"), std::string::npos) << payloadText(f);
    r.passes(50);
    EXPECT_EQ(arrivals, 0) << "never sent unstamped";
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);
    // The seam is one-shot: the next DDL is stamped and sent.
    auto g = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(g.accepted());
    ASSERT_TRUE(r.drive([&] { return arrivals == 1; }));
}

// Task 15 (`rune ql --verbose`): ClientConfig::traceDdlRetries records every FOLLOWED Redirect
// and every scheduled NotLeader retry; off (the default) records nothing.
TEST(ClientDdlRedirectRegression, TraceDdlRetriesRecordsEveryFollowedRedirectAndNotLeaderRetryRegression) {
    for (const bool trace : {true, false}) {
        SCOPED_TRACE(trace ? "trace on" : "trace off (default)");
        rc::ClientConfig cfg = invokerCfg(trace ? 3980 : 3982, trace ? 3981 : 3983);
        cfg.traceDdlRetries = trace;
        Rig r(cfg);
        const std::string leaderCh = udp(rune::client::testing::allocFreeUdpPort());
        const int leader = r.gw.addNode(leaderCh, cfg.gatewayStreamId);
        r.connect();
        int leaderCalls = 0;
        r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
            auto m = pc::DropTopic::decode(f, len);
            ASSERT_TRUE(m.has_value());
            if (ep != leader) { r.answerError(ep, pc::ErrorCode::Redirect, leaderCh, m->sessionId, m->correlationId); return; }
            if (leaderCalls++ == 0) { r.answerError(ep, pc::ErrorCode::NotLeader, "catching up", m->sessionId, m->correlationId); return; }
            pc::DdlResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId; resp.newVersion = 2;
            r.gw.sendDdlResponse(ep, resp);
        };
        test_support::TestCq cq(r.client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
        ASSERT_TRUE(f.accepted());
        ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/50'000'000));
        ASSERT_TRUE(f.ok()) << f.describe();
        std::vector<rc::Client::DdlRetryNote> notes;
        const std::size_t dropped = r.client.takeDdlRetryNotes(notes);
        EXPECT_EQ(dropped, 0u);
        if (!trace) {
            EXPECT_TRUE(notes.empty()) << "nothing is recorded unless asked for";
            continue;
        }
        ASSERT_EQ(notes.size(), 2u);
        EXPECT_EQ(notes[0].kind, rc::Client::DdlRetryNote::Kind::Redirect);
        EXPECT_EQ(notes[0].redirects, 1);
        EXPECT_EQ(notes[0].text, leaderCh);
        EXPECT_EQ(notes[1].kind, rc::Client::DdlRetryNote::Kind::NotLeader);
        EXPECT_EQ(notes[1].text, "catching up");
        EXPECT_GT(notes[1].backoffMs, 0);
        EXPECT_TRUE(r.client.takeDdlRetryNotes(notes) == 0 && notes.size() == 2u) << "taking drains: a second take adds nothing";
    }
}

// ── fix round 1, R2: tracing is Invoker-only and enforced ───────────────────

TEST(ClientDdlRedirectRegression, TraceDdlRetriesIsRefusedOutsideInvokerModeRegression) {
    auto cfg = invokerCfg(4436, 4437);
    cfg.runnerMode = rc::RunnerMode::Thread;
    cfg.traceDdlRetries = true;
    EXPECT_THROW({ rc::Client c(cfg); }, std::invalid_argument);
}

TEST(ClientDdlRedirectRegression, TakeDdlRetryNotesOffTheOwnerThreadThrowsRegression) {
    auto cfg = invokerCfg(4438, 4439);
    cfg.traceDdlRetries = true;
    Rig r(cfg);
    bool threw = false;
    std::thread t([&] {
        std::vector<rc::Client::DdlRetryNote> v;
        try { (void)r.client.takeDdlRetryNotes(v); } catch (const std::logic_error&) { threw = true; }
    });
    t.join();
    EXPECT_TRUE(threw);
    std::vector<rc::Client::DdlRetryNote> v;
    EXPECT_EQ(r.client.takeDdlRetryNotes(v), 0u) << "the owner thread may take";
}

// ── fix round 1, R6: a Redirect whose offer is Fatal was never followed ─────

TEST(ClientDdlRedirectRegression, TraceRecordsNoRedirectWhoseOfferIsFatalRegression) {
    using Offer = rune::client::transport::AeronTransport::OfferResult;
    auto cfg = invokerCfg(4446, 4447);
    cfg.traceDdlRetries = true;
    Rig r(cfg);
    r.connect();
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        r.client.testForceIngressOffers(Offer::Fatal, 1);
        r.answerError(ep, pc::ErrorCode::Redirect, "aeron:ipc", m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_FALSE(f.ok());
    std::vector<rc::Client::DdlRetryNote> notes;
    (void)r.client.takeDdlRetryNotes(notes);
    for (const auto& n : notes) EXPECT_NE(n.kind, rc::Client::DdlRetryNote::Kind::Redirect) << "an unfollowed redirect is not reported";
}

// ── Task 22 fix round 1 (C1): GetViewRow follows the source leader's Redirect / NotLeader ──
//
// A view row is served only by the broker that leads the view's source partition; every
// other broker answers ErrorResponse{Redirect, leader endpoint} (bare NotLeader when
// leaderless).  The client re-sends the read on the named node's session inside its
// budget, exactly as a DDL (isRoutedCtlOp), and never lets the broker's routing code
// (2 = NotLeader, 3 = Redirect) pass for the ViewRowResponse statuses 2 / 3.

namespace {
void answerViewRow(Rig& r, int ep, const pc::GetViewRow& m, uint16_t status, const std::vector<uint8_t>& row) {
    pc::ViewRowResponse resp;
    resp.sessionId = m.sessionId; resp.correlationId = m.correlationId;
    resp.status = status; resp.rowLen = static_cast<uint32_t>(row.size());
    resp.rowBytes = row.empty() ? nullptr : row.data(); resp.rowBytesLen = static_cast<uint32_t>(row.size());
    std::vector<uint8_t> out(resp.encodedSize());
    const std::size_t n = resp.encode(out.data());
    r.gw.send(ep, out.data(), n);
}
}  // namespace

TEST(ClientDdlRedirectRegression, GetViewRowRedirectIsFollowedToTheSourceLeaderAndAnswersTheRowRegression) {
    Rig r(invokerCfg(4450, 4451));
    const std::string leaderCh = udp(rune::client::testing::allocFreeUdpPort());
    const int leader = r.gw.addNode(leaderCh, 4450);
    r.connect();
    std::vector<int> arrivedOn;
    const std::vector<uint8_t> row{1, 2, 3, 4, 5, 6, 7, 8};
    r.gw.templateHandlers[pc::GetViewRow::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::GetViewRow::decode(f, len);
        ASSERT_TRUE(m.has_value());
        arrivedOn.push_back(ep);
        if (ep != leader) { r.answerError(ep, pc::ErrorCode::Redirect, leaderCh, m->sessionId, m->correlationId); return; }
        answerViewRow(r, ep, *m, pc::ViewRowResponse::kOk, row);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getViewRow(0x40000007, 101, 0, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/1'000'000));
    ASSERT_TRUE(f.ok()) << f.describe();
    const auto res = f.getViewRowResult();
    EXPECT_EQ(res.status, rc::ViewRowResult::kOk);
    EXPECT_EQ(res.rowBytes, row);
    ASSERT_EQ(arrivedOn.size(), 2u);
    EXPECT_NE(arrivedOn[0], leader) << "the read starts on the seed";
    EXPECT_EQ(arrivedOn[1], leader) << "and is re-sent on the named leader's session";

    // The Redirect with no data op in flight must not retarget the seed (redirectBySeed_): the
    // next read still starts on the seed.
    arrivedOn.clear();
    auto g = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getViewRow(0x40000007, 102, 0, q, t); });
    ASSERT_TRUE(r.drive([&] { return g.ready(); }, /*stepNs=*/1'000'000));
    ASSERT_TRUE(g.ok()) << g.describe();
    ASSERT_FALSE(arrivedOn.empty());
    EXPECT_NE(arrivedOn.front(), leader) << "the seed was not retargeted by a GetViewRow Redirect";
}

TEST(ClientDdlRedirectRegression, GetViewRowNotLeaderIsRetriedInsideTheBudgetThenAnswersRegression) {
    Rig r(invokerCfg(4452, 4453));
    r.connect();
    int n = 0;
    r.gw.templateHandlers[pc::GetViewRow::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::GetViewRow::decode(f, len);
        ASSERT_TRUE(m.has_value());
        if (n++ < 3) { r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId); return; }
        answerViewRow(r, ep, *m, pc::ViewRowResponse::kAbsentKey, {});
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getViewRow(0x40000007, 999, 0, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/50'000'000));
    ASSERT_TRUE(f.failed() || f.ok());
    EXPECT_EQ(f.getViewRowResult().status, rc::ViewRowResult::kAbsentKey) << "the real status after three NotLeader answers";
    EXPECT_EQ(n, 4);
}

TEST(ClientDdlRedirectRegression, GetViewRowNotLeaderPastTheBudgetIsKNotLeaderNeverNotReadyRegression) {
    rc::ClientConfig cfg = invokerCfg(4454, 4455);
    cfg.viewReadRetryBudget = std::chrono::milliseconds(400);
    Rig r(cfg);
    r.connect();
    r.gw.templateHandlers[pc::GetViewRow::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::GetViewRow::decode(f, len);
        ASSERT_TRUE(m.has_value());
        r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    const int64_t t0 = g_now;
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getViewRow(0x40000007, 101, 0, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/20'000'000));
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(f.getViewRowResult().status, rc::ViewRowResult::kNotLeader)
        << "the routing failure is the client-side kNotLeader (8), not NotReady (2) / SchemaMismatch (3)";
    EXPECT_LE(g_now - t0, int64_t{400'000'000} + int64_t{200'000'000}) << "within the budget + 200 ms";
}

TEST(ClientViewRowRegression, GetViewRowRetryBudgetIsNotTheDdlBudgetRegression) {
    rc::ClientConfig cfg = invokerCfg(4812, 4813);
    cfg.ddlRetryBudget = std::chrono::milliseconds(50);
    cfg.viewReadRetryBudget = std::chrono::milliseconds(1000);
    Rig r(cfg);
    r.connect();
    int n = 0;
    r.gw.templateHandlers[pc::GetViewRow::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::GetViewRow::decode(f, len);
        ASSERT_TRUE(m.has_value());
        if (n++ < 2) { r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId); return; }
        answerViewRow(r, ep, *m, pc::ViewRowResponse::kAbsentKey, {});
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getViewRow(0x40000007, 101, 0, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/20'000'000));
    EXPECT_EQ(f.getViewRowResult().status, rc::ViewRowResult::kAbsentKey)
        << "a read is retried for viewReadRetryBudget, not the (shorter) DDL budget";
    EXPECT_EQ(n, 3);
}

TEST(ClientViewRowRegression, GetViewRowDeadlineIsNotTheDdlDeadlineRegression) {
    rc::ClientConfig cfg = invokerCfg(4814, 4815);
    cfg.ddlTimeoutMs = 100;
    cfg.pingTimeoutMs = 2000;
    Rig r(cfg);
    r.connect();
    int seenEp = -1;
    std::optional<pc::GetViewRow> seen;
    r.gw.templateHandlers[pc::GetViewRow::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::GetViewRow::decode(f, len);
        ASSERT_TRUE(m.has_value());
        seenEp = ep;
        seen = *m;
    };
    test_support::TestCq cq(r.client);
    const int64_t t0 = g_now;
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getViewRow(0x40000007, 101, 0, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return g_now - t0 >= 500'000'000; }, /*stepNs=*/20'000'000));
    ASSERT_TRUE(seen.has_value());
    EXPECT_FALSE(f.ready()) << "a read is not bounded by the DDL deadline";
    answerViewRow(r, seenEp, *seen, pc::ViewRowResponse::kAbsentKey, {});
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/20'000'000));
    EXPECT_EQ(f.getViewRowResult().status, rc::ViewRowResult::kAbsentKey);
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError) << "the OpStatus a non-OK view status carries (as in the kNotLeader test)";
}

// ---- Adversarial review of batch C9 (#76): a view read and a DDL each run on their own budget / deadline. ----

// Both directions at once: a DDL and a GetViewRow refused NotLeader forever, in flight together.  The read ends on
// the (short) viewReadRetryBudget while the DDL is still retrying; the DDL ends on its own (long) ddlRetryBudget.
TEST(ClientViewRowRegression, GetViewRowAndADdlInFlightTogetherEachEndOnItsOwnBudgetAdversarialRegression) {
    rc::ClientConfig cfg = invokerCfg(4870, 4871);
    cfg.ddlRetryBudget      = std::chrono::milliseconds(1500);
    cfg.viewReadRetryBudget = std::chrono::milliseconds(200);
    Rig r(cfg);
    r.connect();
    int views = 0, ddls = 0;
    r.gw.templateHandlers[pc::GetViewRow::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::GetViewRow::decode(f, len);
        ASSERT_TRUE(m.has_value());
        ++views;
        r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId);
    };
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        ++ddls;
        r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    const int64_t t0 = g_now;
    auto d = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    auto v = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getViewRow(0x40000007, 101, 0, q, t); });
    ASSERT_TRUE(d.accepted());
    ASSERT_TRUE(v.accepted());
    ASSERT_TRUE(r.drive([&] { return v.ready(); }, /*stepNs=*/10'000'000));
    const int64_t viewEnd = g_now - t0;
    EXPECT_EQ(v.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(v.getViewRowResult().status, rc::ViewRowResult::kNotLeader);
    EXPECT_GE(viewEnd, int64_t{200'000'000}) << "not before the view-read budget";
    EXPECT_LE(viewEnd, int64_t{200'000'000} + int64_t{150'000'000}) << "on the view-read budget, not the DDL one";
    EXPECT_FALSE(d.ready()) << "the DDL is still inside its own 1500 ms budget: " << d.describe();
    ASSERT_TRUE(r.drive([&] { return d.ready(); }, /*stepNs=*/10'000'000));
    const int64_t ddlEnd = g_now - t0;
    EXPECT_EQ(d.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(d.outcome().c.errorCode, static_cast<uint16_t>(pc::ErrorCode::NotLeader));
    EXPECT_GE(ddlEnd, int64_t{1'500'000'000}) << "the DDL is not cut short by the view-read budget";
    EXPECT_LE(ddlEnd, int64_t{1'500'000'000} + int64_t{1'100'000'000});
    EXPECT_GE(views, 2);
    EXPECT_GT(ddls, views) << "the DDL kept retrying after the read ended";
}

// Bound 0: a zero view-read budget ends the read on its first NotLeader (one send), whatever the DDL budget is.
TEST(ClientViewRowRegression, GetViewRowZeroRetryBudgetEndsOnTheFirstNotLeaderAdversarialRegression) {
    rc::ClientConfig cfg = invokerCfg(4872, 4873);
    cfg.ddlRetryBudget      = std::chrono::milliseconds(10'000);
    cfg.viewReadRetryBudget = std::chrono::milliseconds(0);
    Rig r(cfg);
    r.connect();
    int n = 0;
    r.gw.templateHandlers[pc::GetViewRow::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::GetViewRow::decode(f, len);
        ASSERT_TRUE(m.has_value());
        ++n;
        r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getViewRow(0x40000007, 101, 0, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/10'000'000));
    EXPECT_EQ(f.getViewRowResult().status, rc::ViewRowResult::kNotLeader);
    EXPECT_EQ(n, 1) << "no resend past a zero budget";
}

// Deadline, the reverse of GetViewRowDeadlineIsNotTheDdlDeadlineRegression: a read never answered ends at
// pingTimeoutMs even when the DDL deadline is far longer.
TEST(ClientViewRowRegression, GetViewRowNeverAnsweredEndsAtItsOwnDeadlineNotTheLongerDdlOneAdversarialRegression) {
    rc::ClientConfig cfg = invokerCfg(4874, 4875);
    cfg.ddlTimeoutMs  = 10'000;
    cfg.pingTimeoutMs = 300;
    Rig r(cfg);
    r.connect();
    int n = 0;
    r.gw.templateHandlers[pc::GetViewRow::TEMPLATE_ID] = [&](int, const uint8_t*, std::size_t) { ++n; };
    test_support::TestCq cq(r.client);
    const int64_t t0 = g_now;
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getViewRow(0x40000007, 101, 0, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/10'000'000));
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::Timeout);
    EXPECT_LE(g_now - t0, int64_t{300'000'000} + int64_t{100'000'000});
    EXPECT_EQ(n, 1);
}

// Session loss (the resend pump's DdlSent branch, the second site of the fix): a read redirected to node A whose
// session then dies unanswered is re-sent toward the seed; the seed now answers NotLeader forever.  The budget that
// starts at the loss is viewReadRetryBudget (300 ms), not ddlRetryBudget (4 s).
TEST(ClientViewRowRegression, GetViewRowSessionLostThenNotLeaderEndsOnTheViewReadBudgetAdversarialRegression) {
    rc::ClientConfig cfg = invokerCfg(4876, 4877);
    cfg.ddlRetryBudget      = std::chrono::milliseconds(4000);
    cfg.viewReadRetryBudget = std::chrono::milliseconds(300);
    cfg.pingTimeoutMs       = 8000;
    Rig r(cfg);
    r.gw.uniqueSessionIds = true;
    const std::string aCh = udp(rune::client::testing::allocFreeUdpPort());
    const int a = r.gw.addNode(aCh, 4876);
    r.connect();
    struct Seen { int ep; int64_t sid; };
    std::vector<Seen> at;
    bool redirect = true;
    r.gw.templateHandlers[pc::GetViewRow::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::GetViewRow::decode(f, len);
        ASSERT_TRUE(m.has_value());
        at.push_back(Seen{ep, m->sessionId});
        if (ep == 0 && redirect) { r.answerError(ep, pc::ErrorCode::Redirect, aCh, m->sessionId, m->correlationId); return; }
        if (ep == 0) r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId);
        // A never answers.
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getViewRow(0x40000007, 101, 0, q, t); });
    ASSERT_TRUE(r.drive([&] { return at.size() == 2; }, /*stepNs=*/10'000'000));
    ASSERT_EQ(at[1].ep, a);
    redirect = false;
    const int64_t lostAt = g_now;
    r.gw.expireSession(at[1].sid);   // A's session dies with the read unanswered
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/10'000'000));
    const int64_t took = g_now - lostAt;
    EXPECT_EQ(f.getViewRowResult().status, rc::ViewRowResult::kNotLeader) << f.describe();
    EXPECT_LE(took, int64_t{300'000'000} + int64_t{1'200'000'000})
        << "ended on the 300 ms view-read budget started at the loss, not the 4 s DDL budget";
    EXPECT_GE(at.size(), 3u) << "re-sent toward the seed after the loss";
}

// Input bound: viewReadRetryBudget = max ("unbounded; let the deadline end it") must leave the read bounded by its
// own deadline (Timeout at pingTimeoutMs), never overflow the ms -> ns conversion into an immediate failure.
TEST(ClientViewRowRegression, GetViewRowMaxRetryBudgetIsBoundedByTheReadDeadlineAdversarialRegression) {
    rc::ClientConfig cfg = invokerCfg(4878, 4879);
    cfg.viewReadRetryBudget = std::chrono::milliseconds::max();
    cfg.pingTimeoutMs       = 500;
    Rig r(cfg);
    r.connect();
    int n = 0;
    r.gw.templateHandlers[pc::GetViewRow::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::GetViewRow::decode(f, len);
        ASSERT_TRUE(m.has_value());
        ++n;
        r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    const int64_t t0 = g_now;
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getViewRow(0x40000007, 101, 0, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/10'000'000));
    EXPECT_GE(n, 2) << "retried: a max budget is not an exhausted one";
    EXPECT_GE(g_now - t0, int64_t{400'000'000}) << "ran to the read's own deadline: " << f.describe();
}

// The DDL sibling of the bound above (same conversion, same overflow).
TEST(ClientViewRowRegression, DdlMaxRetryBudgetIsBoundedByTheDdlDeadlineAdversarialRegression) {
    rc::ClientConfig cfg = invokerCfg(4880, 4881);
    cfg.ddlRetryBudget = std::chrono::milliseconds::max();
    cfg.ddlTimeoutMs   = 500;
    Rig r(cfg);
    r.connect();
    int n = 0;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        ++n;
        r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    const int64_t t0 = g_now;
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/10'000'000));
    EXPECT_GE(n, 2) << "retried: a max budget is not an exhausted one";
    EXPECT_GE(g_now - t0, int64_t{400'000'000}) << "ran to the DDL's own deadline: " << f.describe();
}

// ── #687: the routed control ops' retry machine (DDLs and GetViewRow) ──

// A Redirect that is the FIRST answer (no NotLeader seen) toward a node whose session never comes up held the op
// with no budget: it ended only at the op deadline as Timeout, while the same situation after a NotLeader ends
// SessionLost "node unreachable" once the retry budget is spent.  Both now end the same way (DDL).
// RED before the fix: Timeout.
TEST(RoutedCtlRegression, RedirectWithoutNotLeaderToAnUnreachableNodeEndsNodeUnreachableRegression) {
    rc::ClientConfig cfg = invokerCfg(4460, 4461);
    cfg.ddlRetryBudget = std::chrono::milliseconds(300);
    cfg.ddlTimeoutMs   = 3'000;
    Rig r(cfg);
    r.connect();
    const std::string nowhere = udp(rune::client::testing::allocFreeUdpPort());   // no node listens there
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        r.answerError(ep, pc::ErrorCode::Redirect, nowhere, m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/20'000'000)) << "the DDL must end, not hang";
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::SessionLost) << payloadText(f);
    EXPECT_EQ(payloadText(f).rfind("node unreachable", 0), 0u) << payloadText(f);
    r.passes(100, 20'000'000);
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);
}

// The same for GetViewRow (its own budget, viewReadRetryBudget).
TEST(RoutedCtlRegression, GetViewRowRedirectWithoutNotLeaderToAnUnreachableNodeEndsNodeUnreachableRegression) {
    rc::ClientConfig cfg = invokerCfg(4462, 4463);
    cfg.viewReadRetryBudget = std::chrono::milliseconds(300);
    cfg.pingTimeoutMs       = 3'000;
    Rig r(cfg);
    r.connect();
    const std::string nowhere = udp(rune::client::testing::allocFreeUdpPort());
    r.gw.templateHandlers[pc::GetViewRow::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::GetViewRow::decode(f, len);
        ASSERT_TRUE(m.has_value());
        r.answerError(ep, pc::ErrorCode::Redirect, nowhere, m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getViewRow(0x40000007, 101, 0, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/20'000'000)) << "the read must end, not hang";
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::SessionLost) << payloadText(f);
    EXPECT_EQ(payloadText(f).rfind("node unreachable", 0), 0u) << payloadText(f);
    r.passes(100, 20'000'000);
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);
}

// A GetViewRow routing failure carries the route it died on and the broker's own code in its text (the code in
// errorCode is the client-side kNotLeader).  RED before the fix: only the generic budget text.
TEST(RoutedCtlRegression, GetViewRowRoutingFailureNamesRouteAndCodeRegression) {
    rc::ClientConfig cfg = invokerCfg(4464, 4465);
    cfg.viewReadRetryBudget = std::chrono::milliseconds(200);
    Rig r(cfg);
    r.connect();
    r.gw.templateHandlers[pc::GetViewRow::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::GetViewRow::decode(f, len);
        ASSERT_TRUE(m.has_value());
        r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getViewRow(0x40000007, 101, 0, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/20'000'000));
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(f.getViewRowResult().status, rc::ViewRowResult::kNotLeader);
    const std::string text = payloadText(f);
    EXPECT_NE(text.find("route:"), std::string::npos) << text;
    EXPECT_NE(text.find("broker code 2"), std::string::npos) << text;
}

// Offer::Retry also means Aeron back-pressure on a node session that IS up.  A Redirect-held op whose node is
// connected but back-pressured must keep retrying (it is answered once the node drains), never end SessionLost
// "node unreachable" at the budget's end.  Setup: the node session is brought up by a first DDL; then the gateway
// stops draining that node's ingress and two large DDLs are redirected to the node, so the second is refused by the full
// publication while the node's session stays Connected.
TEST(RoutedCtlRegression, BackPressureOnAConnectedNodeIsNotNodeUnreachableRegression) {
    rc::ClientConfig cfg = invokerCfg(4466, 4467);
    cfg.ddlRetryBudget = std::chrono::milliseconds(300);
    cfg.ddlTimeoutMs   = 5'000;
    Rig r(cfg);
    const std::string nodeCh = udp(rune::client::testing::allocFreeUdpPort());
    const int node = r.gw.addNode(nodeCh, 4466);
    r.connect();
    int seedBig = 0;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        if (ep != node) { r.answerError(ep, pc::ErrorCode::Redirect, nodeCh, m->sessionId, m->correlationId); return; }
        pc::DdlResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId;
        resp.objectId = 4; resp.newVersion = 2;
        r.gw.sendDdlResponse(ep, resp);
    };
    r.gw.templateHandlers[pc::AlterTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::AlterTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        if (ep != node) {
            r.answerError(ep, pc::ErrorCode::Redirect, nodeCh, m->sessionId, m->correlationId);
            ++seedBig;
            return;
        }
        pc::AlterTopicResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId;
        resp.topicId = 9; resp.newVersion = 5;
        std::vector<uint8_t> out(256);
        r.gw.send(ep, out.data(), resp.encode(out.data()));
    };
    test_support::TestCq cq(r.client);
    // 1. bring the node's session up.
    auto warm = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(warm.accepted());
    ASSERT_TRUE(r.drive([&] { return warm.ready(); }, /*stepNs=*/1'000'000));
    ASSERT_TRUE(warm.ok()) << warm.describe();
    r.gw.pausedEndpoints.insert(node);   // the node's ingress is no longer drained
    // 2. several large DDLs, redirected to the node, which is no longer drained.
    const std::vector<uint8_t> big(480 * 1024, 0x5a);
    std::vector<test_support::PendingOp> ops;
    for (int k = 0; k < 16; ++k) {
        ops.push_back(cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.alterTopic("t", 1, big, {}, q, t); }));
        ASSERT_TRUE(ops.back().accepted());
    }
    ASSERT_TRUE(r.drive([&] { return seedBig >= 16; }, /*stepNs=*/1'000'000)) << "all were redirected";
    // 3. well past the 300 ms budget (and inside the 5 s deadline): nothing ends, nothing is SessionLost.
    r.passes(40, 20'000'000);   // 800 ms
    ASSERT_GT(r.client.testIngressOfferRefused(), 0u) << "precondition: the node's full publication refused an offer";
    for (auto& o : ops)
        EXPECT_FALSE(o.ready() && o.outcome().c.status == rc::OpStatus::SessionLost) << payloadText(o);
    EXPECT_FALSE(ops.back().ready()) << "the back-pressured op keeps retrying until its deadline";
    // 4. the node drains: both are answered.
    r.gw.pausedEndpoints.clear();
    ASSERT_TRUE(r.drive([&] { for (auto& o : ops) if (!o.ready()) return false; return true; }, /*stepNs=*/1'000'000));
    for (auto& o : ops) EXPECT_TRUE(o.ok()) << o.describe();
}

// ── #687 review attacks (adversarial) ──

// Back-pressure on the SEED's resend (AwaitingResend path): a NotLeader without an endpoint makes the op re-send
// toward the seed; every re-offer is then refused (Retry) while the seed session stays Connected.  That is
// back-pressure, not an unreachable node: past the retry budget the op ends BrokerError NotLeader "retry budget
// exhausted", never SessionLost "node unreachable".  Mutation (the cause set to NodeUnreachable on every Retry):
// SessionLost - red.
TEST(RoutedCtlRegression, BackPressuredSeedResendPastBudgetIsNotLeaderNotUnreachableAdversarialRegression) {
    using Offer = rune::client::transport::AeronTransport::OfferResult;
    rc::ClientConfig cfg = invokerCfg(4480, 4481);
    cfg.ddlRetryBudget = std::chrono::milliseconds(300);
    cfg.ddlTimeoutMs   = 5'000;
    Rig r(cfg);
    r.connect();
    int answered = 0;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        ++answered;
        r.answerError(ep, pc::ErrorCode::NotLeader, "", m->sessionId, m->correlationId);
        r.client.testForceIngressOffers(Offer::Retry, 1'000'000);   // from now on the seed is back-pressured
    };
    test_support::TestCq cq(r.client);
    const int64_t t0 = g_now;
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/20'000'000)) << "the DDL must end, not hang";
    r.client.testForceIngressOffers(Offer::Retry, 0);
    EXPECT_EQ(answered, 1) << "no resend got through the back-pressured seed";
    EXPECT_GT(r.client.testIngressOfferRefused(), 0u) << "precondition: the resends were refused";
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError) << payloadText(f);
    EXPECT_EQ(f.outcome().c.errorCode, static_cast<uint16_t>(pc::ErrorCode::NotLeader));
    EXPECT_NE(payloadText(f).find("retry budget exhausted"), std::string::npos) << payloadText(f);
    EXPECT_LT(g_now - t0, int64_t{2'000'000'000}) << "the 300 ms budget ends it, not the 5 s deadline";
    r.passes(50, 20'000'000);
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);
}

// The held path toward the seed's OWN channel: a first-answer Redirect naming the seed's physical ingress maps to
// session 0, and every offer there is refused while the seed stays Connected (back-pressure).  The op is held past its
// retry budget (no "node unreachable": the node IS up) and is answered once the seed drains.  Mutation (the held
// path ending SessionLost without checking the node's session): SessionLost at the budget's end - red.
TEST(RoutedCtlRegression, RedirectToTheSeedsOwnChannelUnderBackPressureIsHeldUntilAnsweredAdversarialRegression) {
    using Offer = rune::client::transport::AeronTransport::OfferResult;
    rc::ClientConfig cfg = invokerCfg(4482, 4483);
    cfg.ddlRetryBudget = std::chrono::milliseconds(300);
    cfg.ddlTimeoutMs   = 5'000;
    Rig r(cfg);
    r.connect();
    int seen = 0;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        if (++seen == 1) {
            r.answerError(ep, pc::ErrorCode::Redirect, cfg.gatewayChannel, m->sessionId, m->correlationId);
            r.client.testForceIngressOffers(Offer::Retry, 1'000'000);
            return;
        }
        pc::DdlResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId;
        resp.objectId = 4; resp.newVersion = 7;
        r.gw.sendDdlResponse(ep, resp);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return seen >= 1; }, /*stepNs=*/1'000'000));
    r.passes(40, 20'000'000);   // 800 ms: well past the 300 ms budget, inside the 5 s deadline
    EXPECT_GT(r.client.testIngressOfferRefused(), 0u) << "precondition: the held offers were refused";
    EXPECT_FALSE(f.ready()) << "a back-pressured but connected seed ended the held op: " << payloadText(f);
    r.client.testForceIngressOffers(Offer::Retry, 0);   // the seed drains
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/1'000'000));
    EXPECT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(seen, 2) << "the same op reached the seed exactly once more";
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
}

// GetViewRow that dies on the redirect cap at a NODE carries that node's route and the broker's Redirect code (3), with
// the client-side kNotLeader in the result.  Mutation (no route text): red.
TEST(RoutedCtlRegression, GetViewRowRedirectCapNamesTheNodeRouteAndBrokerCode3AdversarialRegression) {
    Rig r(invokerCfg(4484, 4485));
    const std::string nodeCh = udp(rune::client::testing::allocFreeUdpPort());
    const int node = r.gw.addNode(nodeCh, 4484);
    r.connect();
    int atNode = 0;
    r.gw.templateHandlers[pc::GetViewRow::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::GetViewRow::decode(f, len);
        ASSERT_TRUE(m.has_value());
        if (ep == node) ++atNode;
        r.answerError(ep, pc::ErrorCode::Redirect, nodeCh, m->sessionId, m->correlationId);   // always "ask nodeCh"
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getViewRow(0x40000007, 101, 0, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/1'000'000));
    EXPECT_EQ(atNode, rc::Client::kMaxDdlRedirects) << "it died at the node, after following every allowed redirect";
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(f.getViewRowResult().status, rc::ViewRowResult::kNotLeader);
    const std::string text = payloadText(f);
    EXPECT_NE(text.find("redirected more than"), std::string::npos) << text;
    EXPECT_NE(text.find("(route: " + nodeCh + ", broker code 3)"), std::string::npos) << text;
    EXPECT_EQ(text.find("route: seed"), std::string::npos) << text;
}

// ── Whole-branch review (C21–C23) attacks ──

// #687 x the budget selection: a Redirect-held op toward an unreachable node is bounded by ITS OWN budget - a DDL by
// ddlRetryBudget, a GetViewRow by viewReadRetryBudget - never by the other one.  Both ops are held at once on one
// client whose two budgets differ (300 ms vs 1.5 s) and whose op timeouts are far away (20 s): the read ends
// "node unreachable" after ~300 ms of duty clock, the DDL after ~1.5 s.
// RED (mutation): the isDdlOp(...) selection in onDdlRedirect_'s DdlHeldForNode budget swapped or fixed to one budget.
TEST(RoutedCtlRegression, HeldDdlAndHeldViewReadEachEndAtTheirOwnBudgetAdversarialRegression) {
    rc::ClientConfig cfg = invokerCfg(4490, 4491);
    cfg.ddlRetryBudget      = std::chrono::milliseconds(1'500);
    cfg.viewReadRetryBudget = std::chrono::milliseconds(300);
    cfg.ddlTimeoutMs        = 20'000;
    cfg.pingTimeoutMs       = 20'000;
    Rig r(cfg);
    r.connect();
    const std::string nowhere = udp(rune::client::testing::allocFreeUdpPort());   // no node listens there
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::DropTopic::decode(f, len);
        ASSERT_TRUE(m.has_value());
        r.answerError(ep, pc::ErrorCode::Redirect, nowhere, m->sessionId, m->correlationId);
    };
    r.gw.templateHandlers[pc::GetViewRow::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        auto m = pc::GetViewRow::decode(f, len);
        ASSERT_TRUE(m.has_value());
        r.answerError(ep, pc::ErrorCode::Redirect, nowhere, m->sessionId, m->correlationId);
    };
    test_support::TestCq cq(r.client);
    const int64_t t0 = g_now;
    auto ddl  = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    auto read = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getViewRow(0x40000007, 101, 0, q, t); });
    ASSERT_TRUE(ddl.accepted());
    ASSERT_TRUE(read.accepted());
    ASSERT_TRUE(r.drive([&] { return read.ready(); }, /*stepNs=*/10'000'000)) << "the read must end, not hang";
    const int64_t readEndedAfter = g_now - t0;
    EXPECT_GE(readEndedAfter, int64_t{300'000'000}) << "the read ended before its own 300 ms budget";
    EXPECT_LT(readEndedAfter, int64_t{1'000'000'000}) << "the read was held on the DDL's 1.5 s budget";
    EXPECT_EQ(read.outcome().c.status, rc::OpStatus::SessionLost) << payloadText(read);
    EXPECT_EQ(payloadText(read).rfind("node unreachable", 0), 0u) << payloadText(read);
    EXPECT_FALSE(ddl.ready()) << "the DDL ended on the read's 300 ms budget";
    ASSERT_TRUE(r.drive([&] { return ddl.ready(); }, /*stepNs=*/10'000'000)) << "the DDL must end, not hang";
    const int64_t ddlEndedAfter = g_now - t0;
    EXPECT_GE(ddlEndedAfter, int64_t{1'500'000'000});
    EXPECT_LT(ddlEndedAfter, int64_t{3'000'000'000}) << "the DDL outlived its own budget";
    EXPECT_EQ(ddl.outcome().c.status, rc::OpStatus::SessionLost) << payloadText(ddl);
    r.passes(100, 20'000'000);
    EXPECT_EQ(cq.timesCompleted(ddl.token()), 1);
    EXPECT_EQ(cq.timesCompleted(read.token()), 1);
    EXPECT_EQ(r.client.testPendingControlOps(), 0u);
}
