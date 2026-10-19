// Authentication spec §5/§10 (Task 7): the client against the InvokerGateway's SCRAM-speaking broker model —
// login success/failure, codes 44..47 on every response type, SessionRevoked as a terminal, the salted-password
// cache.  The fake is as strict as the real broker will be (nothing but the login before it finishes; always two
// salts; one proof per salt), so a vacuous pass is not possible: each test also asserts what the broker SAW.
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/agent/ClientAgent.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <gtest/gtest.h>

#include <chrono>
#include <fstream>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace pc  = rune::protocol::client;
namespace rc  = rune::client;
namespace rcc = rune::client::codec;

namespace {
int64_t g_now = 1'000'000'000;
int64_t fakeNow() noexcept { return g_now; }
int g_ctr = 0;
const char* kPw = "correct horse battery";

int freePort() {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in a{};
    a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = 0;
    (void)::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof a);
    socklen_t len = sizeof a;
    (void)::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
    ::close(fd);
    return ntohs(a.sin_port);
}
std::string udp(int port) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port); }

rc::ClientConfig authCfg(int32_t in, const std::string& user, const std::string& pw) {
    rc::ClientConfig c;
    c.gatewayChannel = "aeron:ipc"; c.gatewayStreamId = in; c.egressChannel = "aeron:ipc"; c.egressStreamId = in + 1;
    c.dataDir = "/tmp/client_auth_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    c.runnerMode = rc::RunnerMode::Invoker;
    c.nanoClock = &fakeNow;
    c.ddlTimeoutMs = 3'000;
    c.username = user; c.password = pw;
    rc::TopicSpec ts; ts.name = "t"; ts.topicId = 1; ts.partitionCount = 1;
    c.topics.push_back(ts);
    return c;
}

struct Rig {
    rc::Client                   client;
    test_support::InvokerGateway gw;
    explicit Rig(const rc::ClientConfig& c, pc::AuthMode mode = pc::AuthMode::Required)
        : client(c), gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {
        gw.authMode = mode;
        gw.addUser("ws1", kPw);
    }
    template <class P> bool drive(P&& pred, int64_t stepNs = 0) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            g_now += stepNs; client.doWork(); gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    void passes(int n, int64_t stepNs = 0) { for (int i = 0; i < n; ++i) { g_now += stepNs; client.doWork(); gw.pump(); } }
    /// connect(): returns the pending op (the caller decides what success means).
    test_support::PendingOp connectOp(test_support::TestCq& cq) {
        EXPECT_TRUE(drive([&] { return gw.ready(); }));
        return cq.issue([&](rc::Completions& q, rc::Token t) { return client.connect(q, t); });
    }
    void connectOk() {
        test_support::TestCq cq(client);
        auto f = connectOp(cq);
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }
};

void answerDdl(Rig& r, const uint8_t* f, std::size_t len, uint16_t code, int* seen) {
    auto m = pc::DropTopic::decode(f, len);
    ASSERT_TRUE(m.has_value());
    if (seen) ++*seen;
    pc::DdlResponse resp; resp.sessionId = m->sessionId; resp.correlationId = m->correlationId; resp.errorCode = code;
    resp.detail = "not authorized: DROP on TOPIC t";
    r.gw.sendDdlResponse(0, resp);
}
void noopRecords(const rc::Record&) {}
} // namespace

// ---- the login --------------------------------------------------------------------------------------------

TEST(ClientAuthErrorsRegression, ACorrectPasswordConnectsAndReportsAuthenticated) {
    Rig r(authCfg(3700, "ws1", kPw));
    test_support::TestCq cq(r.client);
    EXPECT_EQ(r.client.authStatus().kind, rc::AuthStatus::Kind::Anonymous) << "before any login";
    auto f = r.connectOp(cq);
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(r.gw.authOk, 1);
    EXPECT_EQ(r.gw.unauthenticatedRequests, 0) << "no request leaves before Connected";
    const auto a = r.client.authStatus();
    EXPECT_EQ(a.kind, rc::AuthStatus::Kind::Authenticated);
    EXPECT_EQ(a.user, "ws1");
    EXPECT_EQ(a.brokerMode, pc::AuthMode::Required);
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
}

TEST(ClientAuthErrorsRegression, WrongPasswordFailsConnectWithAuthenticationFailed) {
    Rig r(authCfg(3702, "ws1", "not the password!"));
    test_support::TestCq cq(r.client);
    auto f = r.connectOp(cq);
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    ASSERT_TRUE(f.failed());
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(f.outcome().c.errorCode, 44);
    EXPECT_STREQ(rc::authErrorName(f.outcome().c.errorCode), "AuthenticationFailed");
    EXPECT_EQ(r.gw.authFailed, 1);
    EXPECT_EQ(r.gw.authOk, 0);
    ASSERT_TRUE(r.drive([&] { return r.client.state() == rc::session::SessionState::Rejected; }));
    EXPECT_EQ(r.client.authStatus().kind, rc::AuthStatus::Kind::Anonymous);
}

TEST(ClientAuthErrorsRegression, UnknownUserIsTheSameFailureAsAWrongPassword) {
    Rig r(authCfg(3704, "nobody", kPw));
    test_support::TestCq cq(r.client);
    auto f = r.connectOp(cq);
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_EQ(f.outcome().c.errorCode, 44);
    EXPECT_EQ(r.gw.authFirsts, 1) << "the exchange ran to the end: nothing distinguished the user";
    EXPECT_EQ(r.gw.authFinals, 1);
}

TEST(ClientAuthErrorsRegression, NoCredentialsFailsConnectWithAuthenticationFailed) {
    Rig r(authCfg(3706, "", ""));
    test_support::TestCq cq(r.client);
    auto f = r.connectOp(cq);
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    ASSERT_TRUE(f.failed());
    EXPECT_EQ(f.outcome().c.errorCode, 44);
    // The broker enforces: the AuthClientFirst DID reach it (the client never short-circuits); it refused it.
    EXPECT_EQ(r.gw.authFailed, 1);
    EXPECT_EQ(r.gw.authOk, 0);
}

TEST(ClientAuthErrorsRegression, ARateLimitedLoginReportsRetryAfterOnTheConnectFailure) {
    Rig r(authCfg(3708, "ws1", "not the password!"));
    r.gw.retryAfterMsOnFailure = 800;
    test_support::TestCq cq(r.client);
    auto f = r.connectOp(cq);
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_EQ(f.outcome().c.errorCode, 44);
    EXPECT_EQ(f.outcome().c.aux, 800) << "Completion::aux = the broker's retryAfterMs";
}

TEST(ClientAuthErrorsRegression, ABrokerThatCannotProveThePasswordIsRefused) {
    Rig r(authCfg(3710, "ws1", kPw));
    r.gw.breakServerSignature = true;
    test_support::TestCq cq(r.client);
    auto f = r.connectOp(cq);
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    ASSERT_TRUE(f.failed()) << "never Connected on an unproven broker";
    EXPECT_EQ(f.outcome().c.errorCode, 44);
    EXPECT_NE(r.client.state(), rc::session::SessionState::Connected);
}

TEST(ClientAuthErrorsRegression, ADevModeBrokerConnectsAnonymously) {
    Rig r(authCfg(3712, "", ""), pc::AuthMode::DevDisabled);
    r.connectOk();
    EXPECT_EQ(r.gw.authFirsts, 0);
    const auto a = r.client.authStatus();
    EXPECT_EQ(a.kind, rc::AuthStatus::Kind::Anonymous);
    EXPECT_EQ(a.brokerMode, pc::AuthMode::DevDisabled);
}

TEST(ClientAuthErrorsRegression, AFakeBrokerRefusesAnyRequestBeforeTheLoginFinishes) {
    // Vacuity guard: proves the fake's strictness is live, so "unauthenticatedRequests == 0" above means something.
    Rig r(authCfg(3714, "ws1", kPw));
    test_support::TestCq cq(r.client);
    auto f = r.connectOp(cq);
    ASSERT_TRUE(r.drive([&] { return !r.gw.authStates().empty(); }));
    const int64_t sid = r.gw.authStates().begin()->first;
    uint8_t buf[128];
    const std::size_t n = rcc::encodePing(buf, sizeof buf, "x", sid, 99);
    r.gw.injectFrame(0, buf, n);
    EXPECT_EQ(r.gw.unauthenticatedRequests, 1);
    EXPECT_TRUE(r.gw.authStates().empty()) << "the session ended";
}

TEST(ClientAuthErrorsRegression, PasswordFileIsReadOnceAndBothSetIsRefused) {
    const std::string path = "/tmp/client_auth_pw_" + std::to_string(::getpid());
    { std::ofstream(path, std::ios::binary) << kPw << "\n"; }
    {
        rc::ClientConfig c = authCfg(3716, "ws1", "");
        c.passwordFile = path;
        Rig r(c);
        r.connectOk();   // the trailing newline was stripped: the SCRAM proof matched
        EXPECT_EQ(r.gw.authOk, 1);
    }
    {
        rc::ClientConfig c = authCfg(3718, "ws1", kPw);
        c.passwordFile = path;
        EXPECT_THROW(rc::Client{c}, std::invalid_argument);
    }
    {
        rc::ClientConfig c = authCfg(3720, "ws1", "");
        c.passwordFile = path + ".missing";
        EXPECT_THROW(rc::Client{c}, std::runtime_error);
    }
    ::unlink(path.c_str());
}

// ---- 45 / 47 on an op --------------------------------------------------------------------------------------

TEST(ClientAuthErrorsRegression, NotAuthorizedFailsTheOpWithoutRetry) {
    Rig r(authCfg(3722, "ws1", kPw));
    r.connectOk();
    int seen = 0;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int, const uint8_t* f, std::size_t len) {
        answerDdl(r, f, len, 45, &seen);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    ASSERT_TRUE(f.failed());
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(f.outcome().c.errorCode, 45);
    r.passes(300, 20'000'000);   // 6 s of duty cycles: a retry would show
    EXPECT_EQ(seen, 1) << "exactly one DropTopic was ever sent: NotAuthorized is never re-sent";
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected) << "only the op failed";
}

TEST(ClientAuthErrorsRegression, PasswordChangeRequiredIsReportedAndOnlyTheOpFails) {
    Rig r(authCfg(3724, "ws1", kPw));
    r.gw.mustChangePassword = true;
    r.connectOk();
    EXPECT_EQ(r.client.authStatus().kind, rc::AuthStatus::Kind::PasswordChangeRequired);
    EXPECT_EQ(r.client.authStatus().user, "ws1");
    int seen = 0;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int, const uint8_t* f, std::size_t len) {
        answerDdl(r, f, len, 47, &seen);
    };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_EQ(f.outcome().c.errorCode, 47);
    EXPECT_STREQ(rc::authErrorName(47), "PasswordChangeRequired");
    r.passes(100, 20'000'000);
    EXPECT_EQ(seen, 1);
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
}

TEST(ClientAuthErrorsRegression, NotAuthorizedOnAProduceEndsTheBatchNotRetried) {
    Rig r(authCfg(3726, "ws1", kPw));
    r.connectOk();
    rc::ProducerConfig pcfg; pcfg.acks = rc::Acks::Leader; pcfg.lingerUs = 60'000'000; pcfg.produceTimeoutMs = 1'000;
    pcfg.deliveryTimeoutMs = 10'000;
    rc::Producer p = r.client.producer(pcfg);
    const auto topic = r.client.topic("t");
    int produces = 0;
    r.gw.templateHandlers[pc::Produce::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        const auto pv = rcc::decodeProduce(f, len);
        ASSERT_TRUE(pv.has_value());
        ++produces;
        uint8_t b[64];
        const std::size_t n = rcc::encodeProduceResponse(b, sizeof b, 45, pv->partition, -1, pv->sessionId, pv->correlationId);
        r.gw.send(ep, b, n);
    };
    test_support::TestCq cq(r.client);
    const std::vector<uint8_t> val(16, 0x5C);
    auto s = cq.send(p, topic, rc::Key(uint64_t{1}), val);
    ASSERT_TRUE(s.accepted());
    p.flush();
    ASSERT_TRUE(r.drive([&] { return s.ready(); }));
    EXPECT_EQ(s.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(s.outcome().c.errorCode, 45);
    r.passes(200, 20'000'000);
    EXPECT_EQ(produces, 1) << "never re-sent";
}

// ---- subscriptions -----------------------------------------------------------------------------------------

TEST(ClientAuthErrorsRegression, NotAuthorizedOnASubscriptionEndsIt) {
    Rig r(authCfg(3728, "ws1", kPw));
    r.connectOk();
    int seen = 0;
    r.gw.templateHandlers[pc::Subscribe::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        const auto sv = rcc::decodeSubscribe(f, len);
        ASSERT_TRUE(sv.has_value());
        ++seen;
        r.gw.sendError(ep, 45, "not authorized: READ on TOPIC t", sv->sessionId, sv->correlationId);
    };
    const auto topic = r.client.topic("t");
    rc::Subscription sub = r.client.subscribe(topic, 0, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return sub.finished(); }));
    EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::NotAuthorized);
    EXPECT_EQ(sub.brokerErrorCode(), 45);
    r.passes(100, 20'000'000);
    EXPECT_EQ(seen, 1) << "not re-armed";
}

TEST(ClientAuthErrorsRegression, ALiveSubscriptionWhoseReadIsRevokedEndsNotAuthorized) {
    Rig r(authCfg(3730, "ws1", kPw));
    r.connectOk();
    const auto topic = r.client.topic("t");
    rc::Subscription sub = r.client.subscribe(topic, 0, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return !r.gw.subscribed.empty() && sub.subscriptionId() != 0; }));
    const auto live = r.gw.subscribed.back();
    r.gw.sendError(0, 45, "not authorized: READ on TOPIC t", live.sessionId, live.corrId);   // the revoke
    ASSERT_TRUE(r.drive([&] { return sub.finished(); }));
    EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::NotAuthorized);
}

// ---- SessionRevoked (D26) ----------------------------------------------------------------------------------

TEST(ClientAuthErrorsRegression, SessionRevokedFailsEverythingInFlightAndDoesNotReconnectRegression) {
    Rig r(authCfg(3732, "ws1", kPw));
    r.connectOk();
    r.gw.replyAcks = false;   // produces stay in flight
    rc::ProducerConfig pcfg; pcfg.acks = rc::Acks::Leader; pcfg.lingerUs = 60'000'000; pcfg.maxInFlight = 8;
    pcfg.produceTimeoutMs = 20'000; pcfg.deliveryTimeoutMs = 60'000;
    rc::Producer p = r.client.producer(pcfg);
    const auto topic = r.client.topic("t");
    test_support::TestCq cq(r.client);
    const std::vector<uint8_t> val(16, 0x5C);
    std::vector<test_support::PendingOp> sends;
    for (uint64_t i = 1; i <= 3; ++i) {
        sends.push_back(cq.send(p, topic, rc::Key(i), val));
        ASSERT_TRUE(sends.back().accepted());
        p.flush();
        const std::size_t want = static_cast<std::size_t>(i);
        ASSERT_TRUE(r.drive([&] { return r.gw.produced.size() + r.gw.held.size() >= want; }));
    }
    rc::Subscription sub = r.client.subscribe(topic, 0, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return !r.gw.subscribed.empty() && sub.subscriptionId() != 0; }));
    const int connectsBefore = r.gw.connectsArrived;

    r.gw.revokeSession(r.gw.assignedSessionId);
    for (auto& s : sends) {
        ASSERT_TRUE(r.drive([&] { return s.ready(); }));
        EXPECT_EQ(s.outcome().c.status, rc::OpStatus::BrokerError);
        EXPECT_EQ(s.outcome().c.errorCode, 46) << "SessionRevoked";
    }
    ASSERT_TRUE(r.drive([&] { return sub.finished(); }));
    EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::SessionRevoked);
    EXPECT_EQ(sub.brokerErrorCode(), 46);
    r.passes(200, 10'000'000);   // 2 s of duty cycles
    EXPECT_EQ(r.gw.connectsArrived, connectsBefore) << "a revoked session is never reconnected (D26)";
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Rejected);
}

// Task 8 gap (b): a transactional producer's pending op is part of the session-wide revoke pass.
TEST(ClientAuthErrorsRegression, SessionRevokedFailsATransactionalProducersPendingOpRegression) {
    Rig r(authCfg(3742, "ws1", kPw));
    r.connectOk();
    rc::TransactionalProducer tp = r.client.transactionalProducer("tx-revoked");
    test_support::TestCq cq(r.client);
    auto init = cq.issue([&](rc::Completions& q, rc::Token t) { return tp.initTransactions(q, t); });
    ASSERT_TRUE(init.accepted());
    ASSERT_TRUE(r.drive([&] { return r.gw.initPids > 0; })) << "the InitProducerId is in flight, unanswered";
    EXPECT_FALSE(init.ready());
    r.gw.revokeSession(r.gw.assignedSessionId);
    ASSERT_TRUE(r.drive([&] { return init.ready(); }));
    EXPECT_EQ(init.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(init.outcome().c.errorCode, 46) << "SessionRevoked, not Closed / Fenced / a timeout";
}

// Task 8 gap (c): records accepted but not yet sent complete SessionRevoked too (D26), not Closed.
TEST(ClientAuthErrorsRegression, UnsentRecordsCompleteSessionRevokedNotClosedRegression) {
    Rig r(authCfg(3744, "ws1", kPw));
    r.connectOk();
    rc::ProducerConfig pcfg; pcfg.acks = rc::Acks::Leader; pcfg.lingerUs = 60'000'000; pcfg.maxInFlight = 8;
    pcfg.produceTimeoutMs = 20'000; pcfg.deliveryTimeoutMs = 60'000;
    rc::Producer p = r.client.producer(pcfg);
    const auto topic = r.client.topic("t");
    test_support::TestCq cq(r.client);
    const std::vector<uint8_t> val(16, 0x5C);
    auto s = cq.send(p, topic, rc::Key(uint64_t{1}), val);   // accepted, never flushed: lingers in the builder
    ASSERT_TRUE(s.accepted());
    r.passes(20);
    ASSERT_TRUE(r.gw.produced.empty() && r.gw.held.empty()) << "the record was never sent";
    r.gw.revokeSession(r.gw.assignedSessionId);
    ASSERT_TRUE(r.drive([&] { return s.ready(); }));
    EXPECT_EQ(s.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(s.outcome().c.errorCode, 46);
}

TEST(ClientAuthErrorsRegression, SessionRevokedFailsAPendingControlOpToo) {
    Rig r(authCfg(3734, "ws1", kPw));
    r.connectOk();
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int, const uint8_t*, std::size_t) { /* never answers */ };
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return r.gw.framesReceived > 0 && r.gw.authOk == 1; }));
    r.passes(5);
    r.gw.revokeSession(r.gw.assignedSessionId);
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(f.outcome().c.errorCode, 46);
}

// ---- the salted-password cache -----------------------------------------------------------------------------

TEST(ClientAuthErrorsRegression, ScramCachePaysPbkdf2OncePerVerifierRegression) {
    // Three sessions (the seed + two more nodes) log in with the same verifier: PBKDF2 runs once per distinct
    // (salt, iterations) — the real one and the fake pad every broker lists (review I3: always two salts) —
    // not once per session.
    rune::client::agent::AgentConfig cfg;
    cfg.transport.ingressChannel  = "aeron:ipc";
    cfg.transport.ingressStreamId = 3736;
    cfg.transport.egressChannel   = "aeron:ipc";
    cfg.transport.egressStreamId  = 3737;
    cfg.transport.dataDir = "/tmp/client_auth_agent_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    cfg.transport.driverMode = rune::client::transport::DriverMode::Invoker;
    cfg.session.egressChannel  = "aeron:ipc";
    cfg.session.egressStreamId = 3737;
    cfg.session.username = "ws1";
    cfg.session.password = kPw;
    rune::client::agent::ClientAgent agent(cfg);
    agent.start();
    test_support::InvokerGateway gw(cfg.transport.dataDir + "/aeron", "aeron:ipc", 3736, "aeron:ipc", 3737);
    gw.authMode = pc::AuthMode::Required;
    gw.addUser("ws1", kPw);
    gw.uniqueSessionIds = true;   // each node session gets its own id (the login state is keyed by it)
    const std::string n1 = udp(freePort()), n2 = udp(freePort());
    (void)gw.addNode(n1, 3736);
    (void)gw.addNode(n2, 3736);
    auto now = [] {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch()).count();
    };
    auto drive = [&](auto&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
        while (std::chrono::steady_clock::now() < deadline) {
            agent.doWork(now()); gw.pump();
            if (pred()) return true;
        }
        return false;
    };
    ASSERT_TRUE(drive([&] { return gw.ready(); }));
    ASSERT_TRUE(agent.submitConnect());
    (void)agent.sessionForEndpoint(n1);
    (void)agent.sessionForEndpoint(n2);
    ASSERT_TRUE(drive([&] { return gw.authOk == 3; }))
        << "authOk " << gw.authOk << " failed " << gw.authFailed << " connects " << gw.connects << " firsts "
        << gw.authFirsts << " finals " << gw.authFinals << " sessions " << agent.sessionCount()
        << " s0 " << static_cast<int>(agent.sessionStateAt(0)) << " s1 " << static_cast<int>(agent.sessionStateAt(1))
        << " s2 " << static_cast<int>(agent.sessionStateAt(2));
    EXPECT_EQ(gw.authFailed, 0);
    EXPECT_EQ(agent.scramCache().pbkdf2Computations(), 2u) << "one PBKDF2 per distinct salt, not per session";
    EXPECT_EQ(agent.scramCache().hits(), 4u) << "the two later sessions hit the cache for both salts";
}
