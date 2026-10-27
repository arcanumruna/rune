// Authentication spec §5/§10 (Task 7): the client against the InvokerGateway's SCRAM-speaking broker model —
// login success/failure, codes 44..47 on every response type, SessionRevoked as a terminal, the salted-password
// cache.  The fake is as strict as the real broker will be (nothing but the login before it finishes; always two
// salts; one proof per salt), so a vacuous pass is not possible: each test also asserts what the broker SAW.
#include <filesystem>
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/agent/ClientAgent.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/ql/Parser.h>
#include <rune/ql/Backends.h>

#include <gtest/gtest.h>

#include <chrono>
#include <optional>
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

std::string testPem(const char* name) {
    std::ifstream f(std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/" + name, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
}

rc::ClientConfig authCfg(int32_t in, const std::string& user, const std::string& pw) {
    rc::ClientConfig c;
    c.gatewayChannel = "aeron:ipc"; c.gatewayStreamId = in; c.egressChannel = "aeron:ipc"; c.egressStreamId = in + 1;
    c.dataDir = std::filesystem::temp_directory_path().string() + "/client_auth_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    c.runnerMode = rc::RunnerMode::Invoker;
    c.nanoClock = &fakeNow;
    c.ddlTimeoutMs = 3'000;
    c.username = user; c.password = pw;
    c.offerCrypto = true;               // issue #14: credentials only over TLS - the fake serves the test certificate
    c.tlsTrustPem = testPem("ca.pem");
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
        gw.tlsCertPem = testPem("server.pem");
        gw.tlsKeyPem  = testPem("server.key");
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
    const std::string path = std::filesystem::temp_directory_path().string() + "/client_auth_pw_" + std::to_string(::getpid());
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

// Review B I1 (D26 is per session): a LEADER NODE's session revoked (a D28-style halt of that node) fails its
// requests promptly with 46 and ends its subscription — the Client survives and the seed keeps working.  RED
// (before): the node-1 batches were held by a Rejected session and completed DeliveryFailed after 30 s.
namespace {
struct TwoNodeRig {
    rc::ClientConfig c;
    rc::Client client;
    test_support::InvokerGateway gw;
    int n1 = 0;
    std::string node1;
    static rc::ClientConfig cfg(int32_t in) {
        rc::ClientConfig c = authCfg(in, "ws1", kPw);
        c.nanoClock = nullptr;                      // UDP node sessions retry their connect on the real duty clock
        c.topics[0].partitionCount = 2;
        return c;
    }
    explicit TwoNodeRig(int32_t in) : c(cfg(in)), client(c), gw(c.dataDir + "/aeron", "aeron:ipc", in, "aeron:ipc", in + 1) {
        gw.authMode = pc::AuthMode::Required;
        gw.tlsCertPem = testPem("server.pem");   // issue #14: every node session is TLS (names 127.0.0.1)
        gw.tlsKeyPem  = testPem("server.key");
        gw.addUser("ws1", kPw);
        gw.uniqueSessionIds = true;
        gw.replyFromArrival = true;
        const int p0 = freePort(), p1 = freePort();
        (void)gw.addEndpoint(udp(p0), in);
        node1 = udp(p1);
        n1 = gw.addEndpoint(node1, in);
        gw.nodeOfEndpoint[n1] = n1;
        gw.brokerHost  = "127.0.0.1";
        gw.brokerPort  = p0;
        gw.moreBrokers = {{1, "127.0.0.1", p1}};
        gw.partitions  = {{1, 0, 1, 1, in, ""}, {1, 1, 0, 1, in, ""}};   // p0 led by node 1, p1 by the seed
    }
    template <class P> bool drive(P&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            client.doWork(); gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    void passes(int n) { for (int i = 0; i < n; ++i) { client.doWork(); gw.pump(); } }
    static rc::Key keyFor(int32_t partition) {
        for (uint64_t i = 1;; ++i) if (rc::Key(i).partition(2) == partition) return rc::Key(i);
    }
};
} // namespace

TEST(ClientAuthErrorsRegression, ARevokedLeaderNodeSessionFailsItsRequestsSessionRevokedButNotTheClientRegression) {
    TwoNodeRig r(3760);
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    test_support::TestCq cq(r.client);
    auto conn = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.connect(q, t); });
    ASSERT_TRUE(r.drive([&] { return conn.ready(); }));
    ASSERT_TRUE(conn.ok()) << conn.describe();
    ASSERT_TRUE(r.drive([&] { return r.client.testLeaderFor(1, 0) == 1; })) << "metadata names node 1 as p0's leader";
    r.gw.replyAcks = false;                          // node-1 batches stay in flight
    rc::ProducerConfig pcfg; pcfg.acks = rc::Acks::Leader; pcfg.lingerUs = 60'000'000; pcfg.batchBytes = 64 * 1024;
    pcfg.maxInFlight = 8; pcfg.produceTimeoutMs = 60'000; pcfg.deliveryTimeoutMs = 120'000;
    rc::Producer p = r.client.producer(pcfg);
    const auto topic = r.client.topic("t");
    const std::vector<uint8_t> val(16, 0x5C);
    std::vector<test_support::PendingOp> sends;
    for (int i = 0; i < 3; ++i) {
        sends.push_back(cq.send(p, topic, TwoNodeRig::keyFor(0), val));
        ASSERT_TRUE(sends.back().accepted()) << "send " << i << " result " << static_cast<int>(sends.back().sendResult());
        p.flush();
        r.passes(30);
    }
    ASSERT_TRUE(r.drive([&] { return r.gw.produced.size() >= 1; })) << "a batch in flight on node 1 (authOk " << r.gw.authOk << " firsts " << r.gw.authFirsts << " connects " << r.gw.connects << " s1 " << static_cast<int>(r.client.testSessionStateAt(1)) << " frames " << r.gw.framesReceived << " produced " << r.gw.produced.size() << " held " << r.gw.held.size() << ")";
    for (const auto& pr : r.gw.produced) EXPECT_EQ(pr.endpoint, r.n1);
    r.gw.subscribeLeader[0] = r.n1;                  // the seed redirects the Subscribe to node 1
    rc::Subscription sub = r.client.subscribe(topic, 0, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return !r.gw.subscribed.empty() && sub.subscriptionId() != 0; }));
    ASSERT_EQ(r.gw.subscribed.back().endpoint, r.n1);
    const int64_t node1Sid = r.gw.subscribed.back().sessionId;
    ASSERT_NE(node1Sid, r.gw.authSessionIds.front()) << "node 1 has its own session";

    r.gw.revokeSession(node1Sid, r.n1);              // 46 per subscription, then the session-level 46
    r.passes(50);                                    // PROMPTLY — not after deliveryTimeoutMs
    for (auto& s : sends) {
        ASSERT_TRUE(s.ready()) << "a batch routed to the revoked node's session was held";
        EXPECT_EQ(s.outcome().c.status, rc::OpStatus::BrokerError);
        EXPECT_EQ(s.outcome().c.errorCode, 46);
    }
    ASSERT_TRUE(r.drive([&] { return sub.finished(); }));
    EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::SessionRevoked);
    // A new send to the revoked route fails at once too; the Client and the seed are fine.
    auto later = cq.send(p, topic, TwoNodeRig::keyFor(0), val);
    ASSERT_TRUE(later.accepted());
    p.flush();
    r.passes(50);
    ASSERT_TRUE(later.ready());
    EXPECT_EQ(later.outcome().c.errorCode, 46);
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected) << "D26 is per session: the Client survives";
    r.gw.replyAcks = true;
    auto viaSeed = cq.send(p, topic, TwoNodeRig::keyFor(1), val);
    ASSERT_TRUE(viaSeed.accepted());
    p.flush();
    ASSERT_TRUE(r.drive([&] { return viaSeed.ready(); }));
    EXPECT_TRUE(viaSeed.ok()) << viaSeed.describe();
}

// #291: a TRANSACTIONAL producer whose batches wait on a leader node session that goes Rejected fails them at once
// (the Review-B path reaches transactional producers too) and its transaction is abortable.
TEST(ClientAuthErrorsRegression, ATransactionalProducerWithBatchesOnARejectedLeaderNodeSessionFailsAtOnceRegression) {
    TwoNodeRig r(3858);
    r.gw.initPidReply = true;
    r.gw.replyTxn     = true;
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    test_support::TestCq cq(r.client);
    auto conn = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.connect(q, t); });
    ASSERT_TRUE(r.drive([&] { return conn.ready(); }));
    ASSERT_TRUE(conn.ok()) << conn.describe();
    ASSERT_TRUE(r.drive([&] { return r.client.testLeaderFor(1, 0) == 1; })) << "metadata names node 1 as p0's leader";
    rc::ProducerConfig pcfg; pcfg.acks = rc::Acks::All; pcfg.lingerUs = 60'000'000; pcfg.batchBytes = 64 * 1024;
    pcfg.maxInFlight = 8; pcfg.produceTimeoutMs = 60'000; pcfg.deliveryTimeoutMs = 120'000;
    pcfg.transactionalId = "tx-leader-rejected"; pcfg.transactionOpTimeoutMs = 30'000;
    rc::Producer p = r.client.producer(pcfg);
    auto init = cq.issue([&](rc::Completions& q, rc::Token t) { return p.initTransactions(q, t); });
    ASSERT_TRUE(r.drive([&] { return init.ready(); }));
    ASSERT_TRUE(init.ok()) << init.describe();
    r.gw.replyAcks = false;                          // node-1 batches stay in flight
    p.beginTransaction();
    const auto topic = r.client.topic("t");
    const std::vector<uint8_t> val(16, 0x5C);
    std::vector<test_support::PendingOp> sends;
    for (int i = 0; i < 2; ++i) {
        sends.push_back(cq.send(p, topic, TwoNodeRig::keyFor(0), val));
        ASSERT_TRUE(sends.back().accepted()) << "send " << i << " result " << static_cast<int>(sends.back().sendResult());
        p.flush();
        r.passes(30);
    }
    ASSERT_TRUE(r.drive([&] { return r.gw.produced.size() >= 1; })) << "a txn batch in flight on node 1";
    for (const auto& pr : r.gw.produced) EXPECT_EQ(pr.endpoint, r.n1);
    r.gw.subscribeLeader[0] = r.n1;                  // the seed redirects the Subscribe to node 1
    rc::Subscription sub = r.client.subscribe(topic, 0, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return !r.gw.subscribed.empty() && sub.subscriptionId() != 0; }));
    ASSERT_EQ(r.gw.subscribed.back().endpoint, r.n1);
    const int64_t node1Sid = r.gw.subscribed.back().sessionId;
    ASSERT_NE(node1Sid, r.gw.authSessionIds.front()) << "node 1 has its own session";

    r.gw.revokeSession(node1Sid, r.n1);
    r.passes(50);
    for (auto& s : sends) {
        ASSERT_TRUE(s.ready()) << "a txn batch was held behind a Rejected session";
        EXPECT_EQ(s.outcome().c.status, rc::OpStatus::BrokerError);
        EXPECT_EQ(s.outcome().c.errorCode, 46);
    }
    EXPECT_EQ(p.state(), rc::TxnState::AbortableError);
    auto commit = cq.issue([&](rc::Completions& q, rc::Token t) { return p.commitTransaction(q, t); });
    ASSERT_TRUE(r.drive([&] { return commit.ready(); }));
    EXPECT_FALSE(commit.ok());
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
}

TEST(ClientAuthErrorsRegression, ALeaderNodeLoginRefusedFailsItsRequestsAuthenticationFailedButNotTheClientRegression) {
    TwoNodeRig r(3762);
    r.gw.rejectLoginsAt.insert(r.n1);                // node 1 refuses every login (a halted broker, D28)
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    test_support::TestCq cq(r.client);
    auto conn = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.connect(q, t); });
    ASSERT_TRUE(r.drive([&] { return conn.ready(); }));
    ASSERT_TRUE(conn.ok()) << conn.describe();
    ASSERT_TRUE(r.drive([&] { return r.client.testLeaderFor(1, 0) == 1; }));
    rc::ProducerConfig pcfg; pcfg.acks = rc::Acks::Leader; pcfg.lingerUs = 60'000'000; pcfg.batchBytes = 64 * 1024;
    pcfg.maxInFlight = 8; pcfg.produceTimeoutMs = 60'000; pcfg.deliveryTimeoutMs = 120'000;
    rc::Producer p = r.client.producer(pcfg);
    const std::vector<uint8_t> val(16, 0x5C);
    auto s = cq.send(p, r.client.topic("t"), TwoNodeRig::keyFor(0), val);
    ASSERT_TRUE(s.accepted());
    p.flush();
    ASSERT_TRUE(r.drive([&] { return s.ready(); })) << "the batch was held behind a Rejected session";
    EXPECT_EQ(s.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(s.outcome().c.errorCode, 44);
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
}

// #290: control ops and group / plain fetches HOMED on a Rejected node session (its user revoked there, or its
// login refused — a halted broker, D28) end at once with that session's code, as produce batches do (Review B
// I1).  Each one is told apart from its own deadline by its outcome: a deadline ends a DDL Timeout, a Subscribe
// TimedOut, and leaves a group member Stable.  RED (before): each waited for that deadline.
namespace {
void connectTwoNode(TwoNodeRig& r, test_support::TestCq& cq) {
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    auto conn = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.connect(q, t); });
    ASSERT_TRUE(r.drive([&] { return conn.ready(); }));
    ASSERT_TRUE(conn.ok()) << conn.describe();
    ASSERT_TRUE(r.drive([&] { return r.client.testLeaderFor(1, 0) == 1; })) << "metadata names node 1";
}
} // namespace

TEST(ClientAuthErrorsRegression, ADdlRedirectedToALoginRefusedNodeFailsAtOnceWithItsCodeRegression) {
    TwoNodeRig r(3840);
    r.gw.rejectLoginsAt.insert(r.n1);
    test_support::TestCq cq(r.client);
    connectTwoNode(r, cq);
    int atNode1 = 0;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        const auto h = pc::MessageHeader::decode(f, len);
        ASSERT_TRUE(h.has_value());
        if (ep == 0) r.gw.sendError(0, static_cast<int16_t>(pc::ErrorCode::Redirect), r.node1, h->sessionId,
                                    h->correlationId);
        else ++atNode1;
    };
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError) << f.describe();
    EXPECT_EQ(f.outcome().c.errorCode, 44);
    EXPECT_EQ(atNode1, 0);
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
}

TEST(ClientAuthErrorsRegression, ADdlInFlightOnARevokedNodeSessionFailsAtOnceRegression) {
    TwoNodeRig r(3842);
    test_support::TestCq cq(r.client);
    connectTwoNode(r, cq);
    int64_t node1Sid = 0;
    r.gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        const auto h = pc::MessageHeader::decode(f, len);
        ASSERT_TRUE(h.has_value());
        if (ep == 0) r.gw.sendError(0, static_cast<int16_t>(pc::ErrorCode::Redirect), r.node1, h->sessionId,
                                    h->correlationId);
        else node1Sid = h->sessionId;   // node 1 holds it unanswered
    };
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.dropTopic("t", {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return node1Sid != 0; })) << "the DDL reached node 1";
    r.gw.revokeSession(node1Sid, r.n1);
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError) << f.describe();
    EXPECT_EQ(f.outcome().c.errorCode, 46);
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
}

TEST(ClientAuthErrorsRegression, AGroupFetchHomedOnALoginRefusedNodeRefusesTheConsumerAtOnceRegression) {
    TwoNodeRig r(3844);
    r.gw.rejectLoginsAt.insert(r.n1);
    r.gw.groupProtocol   = true;
    r.gw.groupAssignment = {{1, 0}};                 // p0, led by node 1
    auto gc = r.client.subscribeGroup("g", {"t"});
    test_support::TestCq cq(r.client);
    connectTwoNode(r, cq);
    ASSERT_TRUE(r.drive([&] { return gc.finished(); })) << "the member waited on a Rejected node session"
        << " gen " << gc.generation() << " joins " << r.gw.joinGroups << " s1 "
        << static_cast<int>(r.client.testSessionStateAt(1)) << " sessions " << r.client.testSessionCount()
       ;
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::RefusedByBroker);
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
}

TEST(ClientAuthErrorsRegression, APlainSubscriptionHomedOnALoginRefusedNodeEndsAtOnceRegression) {
    TwoNodeRig r(3846);
    r.gw.rejectLoginsAt.insert(r.n1);
    test_support::TestCq cq(r.client);
    connectTwoNode(r, cq);
    r.gw.subscribeLeader[0] = r.n1;                  // the seed redirects the Subscribe to node 1
    rc::Subscription sub = r.client.subscribe(r.client.topic("t"), 0, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return sub.finished(); }));
    EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::RejectedByBroker);
    EXPECT_EQ(sub.brokerErrorCode(), 44);
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
}

// ---- PR #513 adversarial review (#290 area): sibling entry points to a Rejected node session -------------------
namespace {
const test_support::InvokerGateway::Subscribed* subscribedAt(const test_support::InvokerGateway& gw, int ep) {
    for (auto it = gw.subscribed.rbegin(); it != gw.subscribed.rend(); ++it) if (it->endpoint == ep) return &*it;
    return nullptr;
}
} // namespace

// Network x auth: a BOUND plain subscription whose node session is revoked while the per-subscription
// SessionRevoked answers are lost (only the session-level notice arrives).  Its own refusal never comes: the
// Rejected(46) session alone must end it at once — never re-armed toward a session that can never carry it.
TEST(ClientAuthErrorsRegression, ABoundSubscriptionOnARevokedNodeWithItsAnswerLostEndsAtOnceAdversarialRegression) {
    TwoNodeRig r(3850);
    test_support::TestCq cq(r.client);
    connectTwoNode(r, cq);
    r.gw.subscribeLeader[0] = r.n1;
    rc::Subscription sub = r.client.subscribe(r.client.topic("t"), 0, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return subscribedAt(r.gw, r.n1) != nullptr && sub.active(); }));
    const int64_t sid = subscribedAt(r.gw, r.n1)->sessionId;
    r.gw.subscribed.clear();                         // the per-subscription answers are lost
    r.gw.revokeSession(sid, r.n1);
    ASSERT_TRUE(r.drive([&] { return sub.finished(); })) << "the bound subscription waited on a Rejected session";
    EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::SessionRevoked);
    EXPECT_EQ(sub.brokerErrorCode(), 46);
    EXPECT_TRUE(r.gw.subscribed.empty()) << "re-subscribed after the revoke";
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
}

// Sibling routed control op: GetViewRow shares the DDL driver (isRoutedCtlOp).  Redirected to a node whose login
// is refused, it ends at once with 44 and is never sent there.
TEST(ClientAuthErrorsRegression, AGetViewRowRedirectedToALoginRefusedNodeFailsAtOnceAdversarialRegression) {
    TwoNodeRig r(3852);
    r.gw.rejectLoginsAt.insert(r.n1);
    test_support::TestCq cq(r.client);
    connectTwoNode(r, cq);
    int atNode1 = 0;
    r.gw.templateHandlers[pc::GetViewRow::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
        const auto m = pc::GetViewRow::decode(f, len);
        ASSERT_TRUE(m.has_value());
        if (ep == 0) r.gw.sendError(0, static_cast<int16_t>(pc::ErrorCode::Redirect), r.node1, m->sessionId,
                                    m->correlationId);
        else ++atNode1;
    };
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.getViewRow(0x40000007, 101, 0, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_EQ(f.outcome().c.status, rc::OpStatus::BrokerError) << f.describe();
    EXPECT_EQ(f.outcome().c.errorCode, 44);
    EXPECT_EQ(atNode1, 0);
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
}

// Network x auth x group: a STABLE member whose fetch node session is revoked, its fetch's own SessionRevoked
// answer lost.  The member is refused SessionRevoked at once (the session went Connected -> Rejected).
TEST(ClientAuthErrorsRegression, AStableMembersFetchNodeRevokedWithItsAnswerLostRefusesAtOnceAdversarialRegression) {
    TwoNodeRig r(3854);
    r.gw.groupProtocol   = true;
    r.gw.groupAssignment = {{1, 0}};                 // p0, led by node 1
    auto gc = r.client.subscribeGroup("g", {"t"});
    test_support::TestCq cq(r.client);
    connectTwoNode(r, cq);
    ASSERT_TRUE(r.drive([&] { return subscribedAt(r.gw, r.n1) != nullptr; })) << "the fetch reached node 1";
    r.passes(64);
    const int64_t sid = subscribedAt(r.gw, r.n1)->sessionId;
    r.gw.subscribed.clear();                         // the fetch's own answer is lost
    r.gw.revokeSession(sid, r.n1);
    ASSERT_TRUE(r.drive([&] { return gc.finished(); })) << "the member waited on a Rejected fetch session";
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::SessionRevoked) << static_cast<int>(gc.failReason());
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
}

// Sibling named in #290 itself: the group COORDINATOR is on a node whose login is refused.  The member must end
// at once with the refusal, not cycle seed -> NotCoordinator -> Rejected node until a join budget runs out.
TEST(ClientAuthErrorsRegression, AMemberWhoseCoordinatorNodeRefusesItsLoginEndsAtOnceAdversarialRegression) {
    TwoNodeRig r(3856);
    r.gw.rejectLoginsAt.insert(r.n1);
    r.gw.groupProtocol         = true;
    r.gw.groupAssignment       = {{1, 1}};           // p1, led by the seed
    r.gw.coordinatorEndpoint   = r.n1;
    r.gw.coordinatorRedirectTo = r.node1.substr(r.node1.find("endpoint=") + 9);
    auto gc = r.client.subscribeGroup("g", {"t"});
    test_support::TestCq cq(r.client);
    connectTwoNode(r, cq);
    const auto t0 = std::chrono::steady_clock::now();
    ASSERT_TRUE(r.drive([&] { return gc.finished(); })) << "the member waited on a Rejected coordinator session";
    EXPECT_EQ(gc.failReason(), rc::GroupFailReason::RefusedByBroker) << static_cast<int>(gc.failReason());
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(2));
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
}

// #291: the transaction COORDINATOR is on a node whose login is refused.  A transactional producer waiting on it
// ends FatalError at once with the refusal, not at its op deadline.
TEST(ClientAuthErrorsRegression, ATransactionalProducerWhoseCoordinatorNodeRefusesItsLoginEndsAtOnceAdversarialRegression) {
    TwoNodeRig r(3860);
    r.gw.rejectLoginsAt.insert(r.n1);
    r.gw.coordinatorEndpoint   = r.n1;
    r.gw.coordinatorRedirectTo = r.node1.substr(r.node1.find("endpoint=") + 9);
    test_support::TestCq cq(r.client);
    connectTwoNode(r, cq);
    rc::ProducerConfig pcfg; pcfg.acks = rc::Acks::All;
    pcfg.transactionalId = "tx-coord-rejected"; pcfg.transactionOpTimeoutMs = 30'000;
    rc::Producer p = r.client.producer(pcfg);
    const auto t0 = std::chrono::steady_clock::now();
    auto init = cq.issue([&](rc::Completions& q, rc::Token t) { return p.initTransactions(q, t); });
    ASSERT_TRUE(r.drive([&] { return init.ready(); })) << "InitProducerId waited on a Rejected coordinator session";
    EXPECT_EQ(init.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(init.outcome().c.errorCode, 44);
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(2));
    EXPECT_EQ(p.state(), rc::TxnState::FatalError);
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
    // A plain producer on the same client: its coordinator is the same refused node (the real gateway answers every
    // InitProducerId, plain ones too, with NotCoordinator), so its id request can never be answered here.  Its record
    // ends at once with the refusal - it is never held behind an InitProducerId <-> NotCoordinator loop.
    rc::ProducerConfig plain; plain.acks = rc::Acks::Leader; plain.lingerUs = 0;
    plain.deliveryTimeoutMs = 120'000; plain.produceTimeoutMs = 60'000;
    rc::Producer pp = r.client.producer(plain);
    const auto topic = r.client.topic("t");
    const std::vector<uint8_t> val(16, 0x5C);
    auto held = cq.send(pp, topic, TwoNodeRig::keyFor(1), val);
    ASSERT_TRUE(held.accepted());
    pp.flush();
    ASSERT_TRUE(r.drive([&] { return held.ready(); }))
        << "a plain producer's record held behind a Rejected coordinator; plainInit " << r.gw.plainInitPids;
    EXPECT_EQ(held.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(held.outcome().c.errorCode, 44);
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
}

// Review B I5: against a Required broker with no security admin yet, a credential-less client's session is Anonymous
// and may carry ONLY the bootstrap: no keepalive, no connect-time metadata request (the strict fake ends a session
// that sends anything else).  The bootstrap itself — MAC checked, accepted once — creates the admin.
TEST(ClientAuthErrorsRegression, ABootstrapOnlySessionSendsNothingButTheBootstrapRegression) {
    Rig r(authCfg(3756, "", ""));
    r.gw.users.clear();
    r.gw.bootstrapped = false;
    const std::string keyFile(64, 'k');
    r.gw.clusterKey.assign(keyFile.begin(), keyFile.end());
    r.connectOk();
    EXPECT_EQ(r.client.authStatus().kind, rc::AuthStatus::Kind::Anonymous);
    EXPECT_EQ(r.client.authStatus().brokerMode, pc::AuthMode::Required);
    r.passes(300, 20'000'000);   // 6 s of duty cycles: a keepalive (1 s) or metadata request would show
    EXPECT_EQ(r.gw.unauthenticatedRequests, 0) << "the Anonymous session sent something other than the bootstrap";
    EXPECT_EQ(r.gw.pings, 0);
    EXPECT_EQ(r.gw.metadataRequests, 0);
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
    const std::vector<uint8_t> key(keyFile.begin(), keyFile.end());
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.bootstrapSecurity("admin", "a one-time password 24", key, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(r.gw.bootstraps, 1);
    // A second bootstrap is refused (spec §7): the cluster has an admin now.
    auto again = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.bootstrapSecurity("admin2", "another one-time pw 24", key, q, t); });
    ASSERT_TRUE(again.accepted());
    ASSERT_TRUE(r.drive([&] { return again.ready(); }));
    EXPECT_EQ(again.outcome().c.status, rc::OpStatus::BrokerError);
    EXPECT_EQ(again.outcome().c.errorCode, 39);
    EXPECT_EQ(r.gw.bootstraps, 1);
}

// A credentialed client is refused by that same fresh cluster, naming the fix (review B I3 + I5).
TEST(ClientAuthErrorsRegression, ACredentialedClientAgainstAnUnbootstrappedClusterIsToldToBootstrapRegression) {
    Rig r(authCfg(3758, "ws1", kPw));
    r.gw.users.clear();
    r.gw.bootstrapped = false;
    test_support::TestCq cq(r.client);
    auto f = r.connectOp(cq);
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    ASSERT_TRUE(f.failed());
    EXPECT_EQ(f.outcome().c.errorCode, 44);
    const auto b = f.bytes();
    EXPECT_NE(std::string(b.begin(), b.end()).find("rune security bootstrap"), std::string::npos)
        << std::string(b.begin(), b.end());
    EXPECT_EQ(r.gw.authFirsts, 0) << "no SCRAM was attempted on a session the broker called Anonymous";
}

// Review B I4: after the user changed THEIR OWN password the Client's credential is the new one — a later login
// (a failover, a redirect, a resumed session that expired) must use it, and a one-time session is a normal one.
TEST(ClientAuthErrorsRegression, AfterASelfSetPasswordTheNextLoginUsesTheNewPasswordRegression) {
    Rig r(authCfg(3750, "ws1", kPw));
    r.gw.mustChange.insert("ws1");   // the bootstrap-style one-time password
    r.connectOk();
    EXPECT_EQ(r.client.authStatus().kind, rc::AuthStatus::Kind::PasswordChangeRequired);
    auto st = rune::ql::parse("alter user ws1 set password 'new password 456';");
    ASSERT_TRUE(st.has_value());
    auto plan = rune::ql::applySecurityPlan(*st);
    ASSERT_TRUE(plan.has_value());
    // The fake applies the change (a built-in must-change handler) and answers OK; the Client then installs it.
    test_support::TestCq cq(r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.securityDdl(*plan, {}, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    ASSERT_TRUE(f.ok()) << f.describe();
    EXPECT_EQ(r.gw.unprovenCredentialChanges, 0) << "issue #114: the client proved its current password";
    EXPECT_EQ(r.client.authStatus().kind, rc::AuthStatus::Kind::Authenticated) << "the one-time flag is cleared";
    ASSERT_EQ(r.gw.authOk, 1);
    // The session expires: the re-login must present the NEW password.
    r.gw.expireSession(r.gw.assignedSessionId);
    ASSERT_TRUE(r.drive([&] { return r.gw.authOk == 2 || r.gw.authFailed > 0; }, 10'000'000));
    EXPECT_EQ(r.gw.authOk, 2);
    EXPECT_EQ(r.gw.authFailed, 0) << "the stale one-time password was presented";
    ASSERT_TRUE(r.drive([&] { return r.client.state() == rc::session::SessionState::Connected; }, 10'000'000));
    EXPECT_EQ(r.client.authStatus().kind, rc::AuthStatus::Kind::Authenticated);
}

// Review B M6 (the machine-level canSendData() test asserted a getter nothing in production calls): the REAL
// path.  Records, a Subscribe and a control op are issued while the login is held mid-flight; the strict fake counts
// every request that reaches it before the login finished — it must stay 0, and everything flows once Connected.
TEST(ClientAuthErrorsRegression, NoDataFrameLeavesBeforeConnectedRegression) {
    Rig r(authCfg(3754, "ws1", kPw));
    r.gw.holdAuthFirst = true;                   // a slow login
    test_support::TestCq cq(r.client);
    auto conn = r.connectOp(cq);
    ASSERT_TRUE(r.drive([&] { return r.gw.heldAuthFirsts == 1; }));
    rc::ProducerConfig pcfg; pcfg.acks = rc::Acks::Leader; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    rc::Producer p = r.client.producer(pcfg);
    const auto topic = r.client.topic("t");
    const std::vector<uint8_t> val(16, 0x5C);
    auto s = cq.send(p, topic, rc::Key(uint64_t{1}), val);
    ASSERT_TRUE(s.accepted());
    p.flush();
    rc::Subscription sub = r.client.subscribe(topic, 0, {}, &noopRecords);
    auto ping = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client.ping({}, q, t); });
    r.passes(300, 1'000'000);
    EXPECT_FALSE(conn.ready()) << "still logging in";
    EXPECT_TRUE(r.gw.produced.empty() && r.gw.held.empty()) << "a record left the session before Connected";
    EXPECT_TRUE(r.gw.subscribed.empty()) << "a Subscribe left the session before Connected";
    EXPECT_EQ(r.gw.pings, 0) << "a control op left the session before Connected";
    EXPECT_EQ(r.gw.unauthenticatedRequests, 0);
    r.gw.releaseHeldAuth();                      // the login completes
    ASSERT_TRUE(r.drive([&] { return conn.ready(); }));
    ASSERT_TRUE(conn.ok()) << conn.describe();
    ASSERT_TRUE(r.drive([&] { return !r.gw.produced.empty() && !r.gw.subscribed.empty(); }));
    EXPECT_EQ(r.gw.unauthenticatedRequests, 0);
}

// Review B I5 (d): a resume of a session that IS authenticated keeps its login — no second SCRAM exchange.
TEST(ClientAuthErrorsRegression, AResumedSessionKeepsItsLoginRegression) {
    Rig r(authCfg(3752, "ws1", kPw));
    r.gw.uniqueSessionIds = true;
    r.connectOk();
    ASSERT_EQ(r.gw.authFirsts, 1);
    const int64_t sid = r.client.state() == rc::session::SessionState::Connected ? r.gw.authSessionIds.front() : 0;
    r.client.testForceResume();   // the link drops: Connected -> Resuming -> a resume ConnectRequest with the hint
    ASSERT_TRUE(r.drive([&] { return r.gw.resumedAnswers == 1; }, 10'000'000));
    ASSERT_TRUE(r.drive([&] { return r.client.state() == rc::session::SessionState::Connected; }, 10'000'000));
    EXPECT_EQ(r.gw.authFirsts, 1) << "no second login across the resume";
    EXPECT_EQ(r.gw.authOk, 1);
    EXPECT_EQ(r.client.authStatus().kind, rc::AuthStatus::Kind::Authenticated) << "an earned resume";
    EXPECT_EQ(r.client.authStatus().user, "ws1");
    EXPECT_NE(sid, 0);
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
    rc::ProducerConfig tcfg;
    tcfg.acks            = rc::Acks::All;
    tcfg.transactionalId = "tx-revoked";
    rc::Producer tp = r.client.producer(tcfg);
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
    cfg.transport.dataDir = std::filesystem::temp_directory_path().string() + "/client_auth_agent_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
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

// ---- issues #14 / #115: credentials only over TLS; no name-less system trust -------------------------------------

// A MITM that strips the crypto offer (here: a broker without a certificate answers the offer in plaintext) never gets
// a credentialed client's login: requireCrypto is forced on with credentials, so no AuthClientFirst is ever sent and
// the connect fails.  RED (issue #14): drop `c.requireCrypto = true` from validatedConfig - the client logs in plaintext.
TEST(ClientAuthErrorsRegression, ACredentialedClientNeverLogsInOverAStrippedConnectRegression) {
    rc::ClientConfig c = authCfg(3790, "ws1", kPw);
    ASSERT_FALSE(c.requireCrypto) << "precondition: the caller did not ask for it";
    Rig r(c);
    r.gw.tlsCertPem.clear();   // the offer is answered plaintext (chosenCipher 0)
    r.gw.tlsKeyPem.clear();
    test_support::TestCq cq(r.client);
    auto f = r.connectOp(cq);
    ASSERT_TRUE(r.drive([&] { return f.ready(); }, /*stepNs=*/1'000'000'000LL));
    EXPECT_FALSE(f.ok()) << f.describe();
    EXPECT_EQ(r.gw.authFirsts, 0) << "no SCRAM leg ever left the client in clear";
    EXPECT_EQ(r.gw.unauthenticatedRequests, 0);
    EXPECT_GE(r.gw.connects, 1);
    EXPECT_NE(r.client.state(), rc::session::SessionState::Rejected) << "the stripped answer is unauthenticated";
}

// Credentials (username / password / passwordFile) or the bootstrap-only mode without offerCrypto are refused at
// construction - never sent in clear; insecurePlaintextAuth (tests only) allows it.  RED: drop the offerCrypto check.
TEST(ClientAuthErrorsRegression, CredentialsWithoutTlsAreRefusedAtConstructionRegression) {
    auto plain = [](rc::ClientConfig c) { c.offerCrypto = false; c.tlsTrustPem.reset(); return c; };
    EXPECT_THROW(rc::Client{plain(authCfg(3792, "ws1", kPw))}, std::invalid_argument);
    {
        rc::ClientConfig c = plain(authCfg(3794, "", ""));
        c.securityBootstrapOnly = true;
        EXPECT_THROW(rc::Client{c}, std::invalid_argument) << "the bootstrap material is credentials too";
    }
    {
        rc::ClientConfig c = plain(authCfg(3796, "ws1", kPw));
        c.insecurePlaintextAuth = true;
        EXPECT_NO_THROW(rc::Client{c});
    }
    EXPECT_NO_THROW(rc::Client{plain(authCfg(3798, "", ""))}) << "no credentials: TLS stays optional";
}

// Issue #115: offerCrypto with the system store, no tlsExpectHost and a gateway channel that names no host would check
// no name at all - any public certificate for any name would pass.  Refused at construction; a host in the channel, an
// explicit tlsExpectHost or a pinned CA each make it valid.  RED: drop the tlsHostOfChannel check in validatedConfig.
TEST(ClientCryptoRegression, SystemTrustWithoutAHostIsRefusedRegression) {
    auto sys = [](int32_t in) {
        rc::ClientConfig c = authCfg(in, "", "");
        c.tlsTrustPem.reset();   // the system store
        return c;
    };
    EXPECT_THROW(rc::Client{sys(3800)}, std::invalid_argument) << "aeron:ipc names no host";
    {
        rc::ClientConfig c = sys(3802);
        c.tlsExpectHost = "broker.example";
        EXPECT_NO_THROW(rc::Client{c});
    }
    {
        rc::ClientConfig c = sys(3804);
        c.gatewayChannel = udp(freePort());   // names a host (127.0.0.1)
        EXPECT_NO_THROW(rc::Client{c});
    }
    EXPECT_NO_THROW(rc::Client{authCfg(3806, "", "")}) << "a pinned CA";
}

// ---- adversarial review of PR #476 (issues #14 / #115): a TLS session never acts on a frame in clear ----------------

namespace {
/// Connected over TLS + one produce in flight (its ack held).  Returns the produce's broker-side record.
test_support::InvokerGateway::Produced produceInFlight(Rig& r, rc::Producer& p, test_support::TestCq& cq,
                                                       std::optional<test_support::PendingOp>& s) {
    r.gw.replyAcks = false;
    const auto topic = r.client.topic("t");
    s.emplace(cq.send(p, topic, rc::Key(uint64_t{1}), std::vector<uint8_t>(16, 0x5C)));
    EXPECT_TRUE(s->accepted());
    p.flush();
    EXPECT_TRUE(r.drive([&] { return r.gw.produced.size() + r.gw.held.size() >= 1; }));
    return !r.gw.held.empty() ? r.gw.held.back() : r.gw.produced.back();
}
}  // namespace

// Any UDP sender can put a frame on the client's egress (a new Aeron image).  On a TLS session a ProduceResponse in
// clear must never complete a produce - whatever session id its header names (the session's own, 0, or one nobody
// holds) - or a forged OK silently loses the record.  Only the sealed ack completes it.
TEST(ClientAuthErrorsRegression, AForgedPlaintextAckNeverCompletesATlsProduceAdversarialRegression) {
    Rig r(authCfg(3810, "ws1", kPw));
    r.connectOk();
    ASSERT_GE(r.gw.tlsSessions, 1) << "precondition: the session is TLS";
    rc::ProducerConfig pcfg; pcfg.acks = rc::Acks::Leader; pcfg.lingerUs = 60'000'000;
    pcfg.produceTimeoutMs = 20'000; pcfg.deliveryTimeoutMs = 60'000;
    rc::Producer p = r.client.producer(pcfg);
    test_support::TestCq cq(r.client);
    std::optional<test_support::PendingOp> s;
    const auto pr = produceInFlight(r, p, cq, s);
    for (const int64_t sid : {pr.sessionId, int64_t{0}, int64_t{0x7EAD'BEEF}}) {
        uint8_t b[64];
        const std::size_t n = rcc::encodeProduceResponse(b, sizeof b, 0, pr.partition, 777, sid, pr.corrId);
        ASSERT_TRUE(r.gw.sendInClear(0, std::vector<uint8_t>(b, b + n)));   // raw: never sealed
        r.passes(50);
        EXPECT_FALSE(s->ready()) << "a plaintext ack naming session " << sid << " completed a TLS produce";
    }
    r.gw.ackAt(pr, 0, 100);   // the real, sealed ack
    ASSERT_TRUE(r.drive([&] { return s->ready(); }));
    EXPECT_EQ(s->outcome().c.status, rc::OpStatus::Ok);
}

// A forged ErrorResponse{Redirect} in clear (session id 0) for an in-flight produce must not move a TLS session to the
// attacker's endpoint (where the certificate name would follow the endpoint's host).
TEST(ClientAuthErrorsRegression, AForgedPlaintextRedirectNeverMovesATlsSessionAdversarialRegression) {
    Rig r(authCfg(3812, "ws1", kPw));
    r.connectOk();
    ASSERT_GE(r.gw.tlsSessions, 1);
    rc::ProducerConfig pcfg; pcfg.acks = rc::Acks::Leader; pcfg.lingerUs = 60'000'000;
    pcfg.produceTimeoutMs = 20'000; pcfg.deliveryTimeoutMs = 60'000;
    rc::Producer p = r.client.producer(pcfg);
    test_support::TestCq cq(r.client);
    std::optional<test_support::PendingOp> s;
    const auto pr = produceInFlight(r, p, cq, s);
    const int connectsBefore = r.gw.connectsArrived;
    for (const int64_t sid : {int64_t{0}, int64_t{0x7EAD'BEEF}}) {
        r.gw.sendError(0, static_cast<int16_t>(pc::ErrorCode::Redirect), "127.0.0.1:" + std::to_string(freePort()), sid,
                       pr.corrId);   // sid not held by the gateway's TLS keys: sent in clear
        r.passes(100, 1'000'000);
        EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected) << "sid " << sid;
    }
    EXPECT_EQ(r.gw.connectsArrived, connectsBefore) << "no reconnect was started";
    r.gw.ackAt(pr, 0, 100);
    ASSERT_TRUE(r.drive([&] { return s->ready(); }));
    EXPECT_EQ(s->outcome().c.status, rc::OpStatus::Ok) << "the session never left the real broker";
    // Control (non-vacuity): the same Redirect SEALED under the session's keys is followed.
    std::optional<test_support::PendingOp> s2;
    const auto pr2 = produceInFlight(r, p, cq, s2);
    r.gw.sendError(0, static_cast<int16_t>(pc::ErrorCode::Redirect), "127.0.0.1:" + std::to_string(freePort()),
                   pr2.sessionId, pr2.corrId);
    EXPECT_TRUE(r.drive([&] { return r.client.state() != rc::session::SessionState::Connected; }, 1'000'000))
        << "control: a sealed Redirect moves the session";
}

// The final-leg HandshakeFailed verdict (ruling N4-I1) is matched by (session id, connect correlation id) - both stay
// the same for the session's whole life.  Long after the broker has PROVEN its keys (sealed answers opened), a forged
// HandshakeContinueResponse{29} in clear must not end the live TLS session for good (Rejected: it never reconnects).
TEST(ClientAuthErrorsRegression, AForgedFinalLegFailureNeverEndsALiveTlsSessionAdversarialRegression) {
    Rig r(authCfg(3814, "ws1", kPw));
    r.connectOk();
    ASSERT_GE(r.gw.tlsSessions, 1);
    ASSERT_FALSE(r.gw.connectCorrIds.empty());
    // The broker has proven its keys: a sealed answer completed an op.
    rc::ProducerConfig pcfg; pcfg.acks = rc::Acks::Leader; pcfg.lingerUs = 60'000'000;
    pcfg.produceTimeoutMs = 20'000; pcfg.deliveryTimeoutMs = 60'000;
    rc::Producer p = r.client.producer(pcfg);
    test_support::TestCq cq(r.client);
    auto s = cq.send(p, r.client.topic("t"), rc::Key(uint64_t{1}), std::vector<uint8_t>(16, 0x5C));
    ASSERT_TRUE(s.accepted());
    p.flush();
    ASSERT_TRUE(r.drive([&] { return s.ready(); }));
    ASSERT_EQ(s.outcome().c.status, rc::OpStatus::Ok);
    // The forgery: the session id and the connect correlation id (both small, sequential) and code 29, in clear.
    std::vector<uint8_t> f;
    const std::size_t n = rcc::encodeHandshakeContinueResponse(
        f, static_cast<int16_t>(pc::ErrorCode::HandshakeFailed), {}, r.gw.assignedSessionId, r.gw.connectCorrIds.back());
    f.resize(n);
    ASSERT_TRUE(r.gw.sendInClear(0, f));
    r.passes(200, 1'000'000);
    EXPECT_NE(r.client.state(), rc::session::SessionState::Rejected)
        << "a forged plaintext frame ended a live, key-proven TLS session for good";
}

// Re-check of PR #476 fix round 1: the one clear frame a crypto-required client still reads is an ErrorResponse on
// an in-flight ConnectRequest's correlation id.  Forged on a reconnecting seed, with every refusal code, it must
// neither fail the produce in flight nor reject the client: it only concerns the connect,
// which is retried.  Once the broker answers again, the session reconnects over TLS and the produce completes Ok.
TEST(ClientAuthErrorsRegression, AForgedConnectLegErrorNeverFailsWorkInFlightAdversarialRegression) {
    for (const auto code : {pc::ErrorCode::NotAuthorized, pc::ErrorCode::SessionRevoked, pc::ErrorCode::AuthenticationFailed,
                            pc::ErrorCode::PasswordChangeRequired, pc::ErrorCode::ProtocolVersion,
                            pc::ErrorCode::SessionExpired, pc::ErrorCode::NotLeader}) {
        Rig r(authCfg(3820 + 2 * static_cast<int32_t>(code) % 60, "ws1", kPw));
        r.connectOk();
        rc::ProducerConfig pcfg; pcfg.acks = rc::Acks::Leader; pcfg.lingerUs = 60'000'000;
        pcfg.produceTimeoutMs = 60'000; pcfg.deliveryTimeoutMs = 120'000;
        rc::Producer p = r.client.producer(pcfg);
        test_support::TestCq cq(r.client);
        std::optional<test_support::PendingOp> s;
        (void)produceInFlight(r, p, cq, s);

        // The broker expires the session; the reconnect's ConnectRequest is captured, not answered.
        std::vector<int64_t> heldConnects;
        r.gw.templateHandlers[pc::ConnectRequest::TEMPLATE_ID] = [&](int, const uint8_t* f, std::size_t len) {
            if (const auto c = rcc::decodeConnectRequest(f, len)) heldConnects.push_back(c->correlationId);
        };
        r.gw.expireSession(r.gw.assignedSessionId);
        ASSERT_TRUE(r.drive([&] { return !heldConnects.empty(); }, 1'000'000)) << int(code);
        for (const int64_t sid : {int64_t{0}, r.gw.assignedSessionId}) {
            std::vector<uint8_t> e(256);
            e.resize(rcc::encodeErrorResponse(e.data(), e.size(), static_cast<int16_t>(code), "forged", sid,
                                              heldConnects.back()));
            const std::size_t before = heldConnects.size();
            ASSERT_TRUE(r.gw.sendInClear(0, e));
            // Non-vacuity: the forged answer reached the connect - it was retried with a new ConnectRequest.
            EXPECT_TRUE(r.drive([&] { return heldConnects.size() > before; }, 1'000'000))
                << "code " << int(code) << ": the forged connect-leg answer was not read";
            r.passes(50);
            EXPECT_FALSE(s->ready()) << "code " << int(code) << ": a forged connect-leg error failed a produce";
            EXPECT_NE(r.client.state(), rc::session::SessionState::Rejected) << "code " << int(code);
        }
        // The broker answers again: the session reconnects over TLS and the produce is delivered.
        r.gw.templateHandlers.erase(pc::ConnectRequest::TEMPLATE_ID);
        r.gw.replyAcks = true;
        ASSERT_TRUE(r.drive([&] { return r.client.state() == rc::session::SessionState::Connected; }, 50'000'000))
            << "code " << int(code);
        ASSERT_TRUE(r.drive([&] {
            if (!r.gw.held.empty()) { for (const auto& h : r.gw.held) r.gw.ack(h); r.gw.held.clear(); }
            return s->ready();
        }, 10'000'000)) << int(code);
        EXPECT_EQ(s->outcome().c.status, rc::OpStatus::Ok) << "code " << int(code) << " " << s->describe();
    }
}

// ---- #515: a subscription is ended with a node session's reject code only while that session is STILL its route --
namespace {
/// TwoNodeRig plus a third node (broker node 2) that can take p0 over.
struct ThreeNodeRig : TwoNodeRig {
    int n2 = 0;
    explicit ThreeNodeRig(int32_t in) : TwoNodeRig(in) {
        const int p2 = freePort();
        n2 = gw.addEndpoint(udp(p2), in);
        gw.nodeOfEndpoint[n2] = n2;
        gw.moreBrokers.push_back({2, "127.0.0.1", p2});
    }
    /// Node 1's session is revoked (its per-subscription answers lost) in the SAME client pass in which the
    /// metadata answer moves p0 to node 2: both frames are queued before the client polls again.
    [[nodiscard]] bool revokeNode1AndMoveP0ToNode2(int64_t node1Sid) {
        gw.holdMetadata = true;
        const std::size_t before = gw.metadataCorrIds.size();
        client.testRequestMetadataNow();
        if (!drive([&] { return gw.metadataCorrIds.size() > before; })) return false;   // no metadata request
        gw.subscribeLeader[0] = n2;
        gw.partitions = {{1, 0, 2, 2, c.gatewayStreamId, ""}, {1, 1, 0, 1, c.gatewayStreamId, ""}};
        gw.subscribed.clear();                          // node 1's per-subscription answers are lost
        gw.revokeSession(node1Sid, n1);                 // node 1's session -> Rejected(46)
        gw.sendMetadata(gw.metadataSessionIds.back(), gw.metadataCorrIds.back(), gw.metadataEndpoints.back());   // p0 is node 2's now
        gw.holdMetadata = false;
        return true;
    }
};
} // namespace

// Bound on node 1; node 1's session is Rejected in the very pass fresh metadata moves p0 to node 2.  RED (before):
// driveHomed checked the reject code before the route and ended the subscription with 46 though a healthy node
// leads p0 — the group path refuses only when the CURRENT leader is Rejected.
TEST(ClientAuthErrorsRegression, ASubscriptionWhoseRejectedHomeMovedFollowsTheRouteRegression) {
    ThreeNodeRig r(3870);
    test_support::TestCq cq(r.client);
    connectTwoNode(r, cq);
    r.gw.subscribeLeader[0] = r.n1;
    rc::Subscription sub = r.client.subscribe(r.client.topic("t"), 0, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return subscribedAt(r.gw, r.n1) != nullptr && sub.active(); }));
    const int64_t node1Sid = subscribedAt(r.gw, r.n1)->sessionId;
    ASSERT_TRUE(r.revokeNode1AndMoveP0ToNode2(node1Sid)) << "no metadata request";
    ASSERT_TRUE(r.drive([&] { return sub.finished() || subscribedAt(r.gw, r.n2) != nullptr; }));
    EXPECT_FALSE(sub.finished()) << "ended with node 1's code " << sub.brokerErrorCode()
                                 << " though p0 moved to a healthy node";
    ASSERT_NE(subscribedAt(r.gw, r.n2), nullptr) << "no Subscribe for p0 reached node 2";
    ASSERT_TRUE(r.drive([&] { return sub.finished() || sub.active(); }));
    EXPECT_TRUE(sub.active());
    EXPECT_FALSE(sub.finished());
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
}

// The new home is Rejected too: the NEXT driveHomed pass ends the subscription, with node 2's code (44 = a refused
// login), not node 1's (46).
TEST(ClientAuthErrorsRegression, ASubscriptionWhoseRejectedHomeMovedToAnotherRejectedNodeEndsWithItsCodeRegression) {
    ThreeNodeRig r(3872);
    r.gw.rejectLoginsAt.insert(r.n2);
    test_support::TestCq cq(r.client);
    connectTwoNode(r, cq);
    r.gw.subscribeLeader[0] = r.n1;
    rc::Subscription sub = r.client.subscribe(r.client.topic("t"), 0, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return subscribedAt(r.gw, r.n1) != nullptr && sub.active(); }));
    const int64_t node1Sid = subscribedAt(r.gw, r.n1)->sessionId;
    ASSERT_TRUE(r.revokeNode1AndMoveP0ToNode2(node1Sid)) << "no metadata request";
    ASSERT_TRUE(r.drive([&] { return sub.finished(); })) << "the subscription waited on a Rejected session";
    EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::RejectedByBroker);
    EXPECT_EQ(sub.brokerErrorCode(), 44) << "node 2's code, not the old home's 46";
    EXPECT_EQ(subscribedAt(r.gw, r.n2), nullptr) << "nothing was ever sent to the refused node";
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
}

// The pending (not yet answered) twin of the first: the Subscribe reached node 1 but its answer is lost, then node
// 1's session is Rejected in the pass the metadata moves p0 to node 2.
TEST(ClientAuthErrorsRegression, APendingSubscribeWhoseRejectedHomeMovedFollowsTheRouteRegression) {
    ThreeNodeRig r(3874);
    test_support::TestCq cq(r.client);
    connectTwoNode(r, cq);
    r.gw.subscribeLeader[0] = r.n1;
    r.gw.dropSubscribeResponsesNext = 1;             // the Subscribe is registered at node 1, its answer is lost
    rc::Subscription sub = r.client.subscribe(r.client.topic("t"), 0, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return subscribedAt(r.gw, r.n1) != nullptr; }));
    ASSERT_EQ(sub.subscriptionId(), 0) << "the answer was meant to be lost";
    const int64_t node1Sid = subscribedAt(r.gw, r.n1)->sessionId;
    ASSERT_TRUE(r.revokeNode1AndMoveP0ToNode2(node1Sid)) << "no metadata request";
    ASSERT_TRUE(r.drive([&] { return sub.finished() || subscribedAt(r.gw, r.n2) != nullptr; }));
    EXPECT_FALSE(sub.finished()) << "ended with node 1's code " << sub.brokerErrorCode()
                                 << " though p0 moved to a healthy node";
    ASSERT_NE(subscribedAt(r.gw, r.n2), nullptr) << "no Subscribe for p0 reached node 2";
    ASSERT_TRUE(r.drive([&] { return sub.finished() || sub.active(); }));
    EXPECT_TRUE(sub.active());
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
}

// #515 adversarial (M31+ review): the reject-vs-route ordering from every side.  Node 1's session is Rejected (46)
// in the very pass fresh metadata names p0's leader as (a) node 1 itself, (b) the seed, then (c) the route follows
// to node 2 and a later answer flips p0 BACK onto Rejected node 1.  (a) and (c) must end with 46 and send nothing
// to the Rejected node; (b) must follow to the seed and stay live.  Never a loop of re-sends.
TEST(ClientAuthErrorsRegression, ARejectedHomeEndsOnlyWhileItIsTheRouteFromEverySideAdversarialRegression) {
    enum class Cell { StillNode1, ToSeed, ToNode2ThenBack };
    int32_t port = 3890;
    for (const Cell cell : {Cell::StillNode1, Cell::ToSeed, Cell::ToNode2ThenBack}) {
        SCOPED_TRACE(static_cast<int>(cell));
        ThreeNodeRig r(port);
        port += 2;
        test_support::TestCq cq(r.client);
        connectTwoNode(r, cq);
        r.gw.subscribeLeader[0] = r.n1;
        rc::Subscription sub = r.client.subscribe(r.client.topic("t"), 0, {}, &noopRecords);
        ASSERT_TRUE(r.drive([&] { return subscribedAt(r.gw, r.n1) != nullptr && sub.active(); }));
        const int64_t node1Sid = subscribedAt(r.gw, r.n1)->sessionId;
        // One answer, queued in the same pass as the revocation, names p0's leader.
        const auto answerWith = [&](int leaderNode, int leaderEp, int32_t epoch, bool revoke) {
            r.gw.holdMetadata = true;
            const std::size_t before = r.gw.metadataCorrIds.size();
            r.client.testRequestMetadataNow();
            if (!r.drive([&] { return r.gw.metadataCorrIds.size() > before; })) return false;
            r.gw.subscribeLeader[0] = leaderEp;
            r.gw.partitions = {{1, 0, leaderNode, epoch, r.c.gatewayStreamId, ""},
                               {1, 1, 0, 1, r.c.gatewayStreamId, ""}};
            r.gw.subscribed.clear();
            if (revoke) r.gw.revokeSession(node1Sid, r.n1);
            r.gw.sendMetadata(r.gw.metadataSessionIds.back(), r.gw.metadataCorrIds.back(),
                              r.gw.metadataEndpoints.back());
            r.gw.holdMetadata = false;
            return true;
        };
        if (cell == Cell::StillNode1) {
            ASSERT_TRUE(answerWith(1, r.n1, 1, true)) << "no metadata request";
            ASSERT_TRUE(r.drive([&] { return sub.finished(); })) << "a Rejected home that is still the route hung";
            EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::SessionRevoked);   // 46
            EXPECT_EQ(sub.brokerErrorCode(), 46);
            EXPECT_EQ(subscribedAt(r.gw, r.n2), nullptr);
            EXPECT_EQ(subscribedAt(r.gw, 0), nullptr) << "a Rejected home that is still the route fell back to the seed";
        } else if (cell == Cell::ToSeed) {
            ASSERT_TRUE(answerWith(0, 0, 2, true)) << "no metadata request";
            ASSERT_TRUE(r.drive([&] { return sub.finished() || subscribedAt(r.gw, 0) != nullptr; }));
            EXPECT_FALSE(sub.finished()) << "ended with " << sub.brokerErrorCode() << " though p0 moved to the seed";
            ASSERT_NE(subscribedAt(r.gw, 0), nullptr) << "no Subscribe for p0 reached the seed";
            ASSERT_TRUE(r.drive([&] { return sub.finished() || sub.active(); }));
            EXPECT_TRUE(sub.active());
        } else {
            ASSERT_TRUE(answerWith(2, r.n2, 2, true)) << "no metadata request";
            ASSERT_TRUE(r.drive([&] { return sub.finished() || (subscribedAt(r.gw, r.n2) != nullptr && sub.active()); }));
            ASSERT_FALSE(sub.finished()) << "ended with " << sub.brokerErrorCode() << " though p0 moved to node 2";
            ASSERT_TRUE(answerWith(1, r.n1, 3, false)) << "no second metadata request";   // flips back onto node 1
            ASSERT_TRUE(r.drive([&] { return sub.finished(); })) << "a route flipped back onto a Rejected node hung";
            EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::SessionRevoked);   // 46
            EXPECT_EQ(sub.brokerErrorCode(), 46) << "the CURRENT route's code";
            EXPECT_EQ(subscribedAt(r.gw, r.n1), nullptr) << "a Subscribe was sent to the Rejected node";
        }
        const std::size_t sent = r.gw.subscribed.size();
        r.passes(200);
        EXPECT_EQ(r.gw.subscribed.size(), sent) << "the subscription kept re-sending after it settled";
        EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
    }
}

// ---- #446: a Subscribe OK that arrives after the client's subscribe timeout ---------------------------------------
// The broker registered the Subscribe, the client timed it out (TimedOut) and only then does the OK arrive.  RED
// (before): the OK was dropped as an unknown correlationId and the broker kept the subscription, pushing records
// until the session closed; now the client releases it with an Unsubscribe naming the OK's subscription id.
TEST(ClientAuthErrorsRegression, LateSubscribeOkIsReleasedEndToEndRegression) {
    rc::ClientConfig c = authCfg(3880, "ws1", kPw);
    c.subscribeTimeoutMs = 1;       // the Subscribe's deadline is on the real clock; the sweep runs on the duty clock
    Rig r(c);
    r.connectOk();
    r.gw.dropSubscribeResponsesNext = 1;             // registered at the broker, its answer is not sent in time
    rc::Subscription sub = r.client.subscribe(r.client.topic("t"), 0, {}, &noopRecords);
    ASSERT_TRUE(r.drive([&] { return !r.gw.subscribed.empty(); }));      // frozen duty clock: no sweep yet
    const auto registered = r.gw.subscribed.back();
    ASSERT_TRUE(r.drive([&] { return sub.finished(); }, 10'000'000)) << "the Subscribe never timed out";
    EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::TimedOut);
    ASSERT_TRUE(r.gw.unsubscribed.empty());

    uint8_t out[64];                                  // the broker's OK, late
    const std::size_t n = rcc::encodeSubscribeResponse(out, sizeof out, 0, registered.partition,
                                                       registered.subscriptionId, registered.sessionId,
                                                       registered.corrId);
    ASSERT_GT(n, 0u);
    r.gw.send(0, out, n);
    ASSERT_TRUE(r.drive([&] { return !r.gw.unsubscribed.empty(); }))
        << "the broker still holds subscription " << registered.subscriptionId;
    EXPECT_EQ(r.gw.unsubscribed.back(), registered.subscriptionId);
    EXPECT_EQ(r.client.state(), rc::session::SessionState::Connected);
}
