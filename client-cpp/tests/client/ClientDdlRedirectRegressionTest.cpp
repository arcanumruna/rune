// Catalog history §5.1/§5.2/§9: every DDL follows Redirect, retries NotLeader inside
// its budget, then fails explicitly; the requestId is stable across every resend.
// Non-blocking ops on the single-threaded fake (F7): the test thread drives
// client.doWork() + gw.pump(); the blocking *AndWait forms are proven against
// real BrokerNodes (Task 11: one node; Task 22: three nodes).
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"
#include <rune/client/Client.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <optional>
#include <span>
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
int freePort() {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in a{};
    a.sin_family      = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port        = 0;
    (void)::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof a);
    socklen_t len = sizeof a;
    (void)::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
    ::close(fd);
    return ntohs(a.sin_port);
}
std::string udp(int port) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port); }

rc::ClientConfig invokerCfg(int32_t in, int32_t eg) {   // as InvokerClientTest.cpp's invokerCfg, plus the clock
    rc::ClientConfig c;
    c.gatewayChannel = "aeron:ipc"; c.gatewayStreamId = in; c.egressChannel = "aeron:ipc"; c.egressStreamId = eg;
    c.dataDir = "/tmp/ddl_redirect_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
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
    const std::string leaderCh = udp(freePort());
    const int leader = r.gw.addEndpoint(leaderCh, 3964);          // a second fake node (IngressEndpointRoutingTest)
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
    const std::string loopCh = udp(freePort());
    (void)r.gw.addEndpoint(loopCh, 3966);
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
    const std::string leaderCh = udp(freePort());
    const int leader = r.gw.addEndpoint(leaderCh, 3970);
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
    const std::string nowhere = udp(freePort());   // no fake node listens there
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
    const std::string leaderCh = udp(freePort());
    const int leader = r.gw.addEndpoint(leaderCh, 3998);
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
