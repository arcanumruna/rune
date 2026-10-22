// AuthTlsBrokerRegressionTest.cpp - issues #10 / #13 / #113 / #137: authentication over TLS, the DEFAULT transport of
// an authenticating broker (combined fix program M1).  One real authenticating BrokerNode with tlsOnly (every client
// session TLS: what parseNodeConfig yields), the REAL `rune` CLI and the REAL client over TLS, and a raw plaintext
// sender playing the attacker who can put any UDP frame on the wire.
//
//   * a plaintext Connect is refused HandshakeFailed (29) and creates no session (#10);
//   * a plaintext frame naming an encrypted session's id never opens, never ends it and never resumes it (#10);
//   * `rune security bootstrap` and security DDL run over TLS (#13) - so verifiers, the bootstrap material and the
//     __security reads never travel in clear (#113);
//   * a login runs over TLS, and in shard mode the worker decrypts an encrypted produce and then checks WRITE (#137).
#include "support/AuthBroker.h"

#include <rune/cli/Cli.h>
#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/node/Worker.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
namespace pc = rune::protocol::client;
namespace rc = rune::client;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

constexpr const char* kOtp      = "one-time-password-tls-1";
constexpr const char* kAdminPw  = "admin-password-over-tls-1";
constexpr const char* kWs1Pw    = "ws1-password-over-tls-1";

fs::path scratchDir() {
    static const fs::path d = [] {
        fs::path p = fs::temp_directory_path() / ("rune_auth_tls_test_" + std::to_string(::getpid()));
        fs::create_directories(p);
        return p;
    }();
    return d;
}

std::string writeTemp(const std::string& name, const std::string& content) {
    const fs::path p = scratchDir() / name;
    std::ofstream o(p, std::ios::binary);
    o << content;
    fs::permissions(p, fs::perms::owner_read | fs::perms::owner_write);
    return p.string();
}

int runCli(std::vector<std::string> args, std::string* out = nullptr, std::string* err = nullptr) {
    std::stringstream o, e;
    const int rc = rune::cli::run(std::move(args), o, e);
    if (out) *out = o.str();
    if (err) *err = e.str();
    return rc;
}

AuthBroker::Options tlsOnly(bool shard = false) {
    AuthBroker::Options o;
    o.tlsOnly = true;
    o.shard = shard;
    return o;
}

/// `rune security bootstrap` + the forced first password change + `script`, all with the CLI over TLS (--tls-ca).
void bootstrapOverTls(AuthBroker& b, const std::string& script = "") {
    const std::string otp = writeTemp("otp", std::string(kOtp) + "\n");
    const std::string ca = AuthBroker::tlsCaFile().string();
    std::string out, err;
    ASSERT_EQ(runCli({"security", "bootstrap", "--admin", "admin", "--cluster-key-file", b.keyFile().string(),
                      "--password-file", otp, "--broker", b.endpoint(), "--tls-ca", ca}, &out, &err), 0) << err;
    const std::string change = writeTemp("change.rql", std::string("alter user admin set password '") + kAdminPw + "';\n");
    ASSERT_EQ(runCli({"ql", "--broker", b.endpoint(), "--user", "admin", "--password-file", otp, "-f", change, "--tls-ca", ca},
                     &out, &err), 0) << err;
    if (script.empty()) return;
    const std::string pw = writeTemp("admin.pw", std::string(kAdminPw) + "\n");
    const std::string file = writeTemp("script.rql", script);
    ASSERT_EQ(runCli({"ql", "--broker", b.endpoint(), "--user", "admin", "--password-file", pw, "-f", file, "--tls-ca", ca},
                     &out, &err), 0) << err;
}

/// The real client over TLS (the broker's test CA; the certificate name is the endpoint's host, 127.0.0.1).
std::unique_ptr<rc::Client> tlsClient(AuthBroker& b, const std::string& user, const std::string& pw,
                                      rc::RunnerMode mode = rc::RunnerMode::Thread) {
    rc::ClientConfig c;
    c.gatewayChannel  = b.channel();
    c.gatewayStreamId = 1;
    c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    c.egressStreamId  = 2;
    c.dataDir         = b.clientDir().string();
    c.runnerMode      = mode;
    c.username        = user;
    c.password        = pw;
    c.offerCrypto     = true;
    c.tlsTrustPem     = AuthBroker::tlsCaPem();
    return std::make_unique<rc::Client>(c);
}

std::string connectError(rc::Client& c) {
    try {
        c.connectAndWait(20s);
    } catch (const std::exception& e) {
        return e.what();
    }
    return {};
}

}  // namespace

// Issue #10 (M1): an authenticating broker answers a plaintext Connect HandshakeFailed (29) and creates no session -
// a plaintext session (sequential ids, frames bound to nothing) could be taken over by any UDP sender.  RED: pass
// `false` for requireTls to the gateway (BrokerNode: config_.requiresClientTls()).
TEST(AuthSessionBindingRegression, APlaintextConnectToAnAuthenticatingBrokerIsRefusedRegression) {
    AuthBroker b("tlsplainconnect", tlsOnly());
    AuthWireClient& w = b.wire();
    EXPECT_EQ(w.connect(b.channel()), -1) << "no session";
    ASSERT_TRUE(w.lastConnect().has_value()) << "an explicit answer, not a silent drop";
    EXPECT_EQ(w.lastConnect()->code, static_cast<int16_t>(pc::ErrorCode::HandshakeFailed));
    // The same broker serves a TLS client (the refusal is about the transport, not the broker).
    bootstrapOverTls(b);
    auto admin = tlsClient(b, "admin", kAdminPw);
    EXPECT_EQ(connectError(*admin), "");
    EXPECT_EQ(admin->authStatus().kind, rc::AuthStatus::Kind::Authenticated);
    admin->closeImmediately();
}

// Issue #10: a plaintext frame naming an ENCRYPTED session's id is dropped before it is read - it neither runs a request
// as that session nor ends it - and a plaintext Connect naming the id never resumes it (the plaintext resume-by-id
// is gone).  The session keeps working.  RED: restore ClientGateway::resumeAuthenticated_ for encrypted sessions, or
// skip the AEAD open in onFragment.
TEST(AuthSessionBindingRegression, AFrameNamingAnotherSessionsIdNeverOpensRegression) {
    AuthBroker b("tlsbinding", tlsOnly());
    bootstrapOverTls(b, "grant create on topic prefix '' to user admin;\n");   // ADMIN ON CLUSTER is not CREATE
    auto admin = tlsClient(b, "admin", kAdminPw, rc::RunnerMode::Invoker);
    ASSERT_EQ(connectError(*admin), "");
    const int64_t sid = admin->testSeedSessionId();
    ASSERT_GT(sid, 0);
    ASSERT_TRUE(b.node().gatewayHasSessionForTest(sid));

    AuthWireClient& attacker = b.wire();
    // 1. A request in clear, as the admin's session: never executed.
    (void)attacker.raw().defineTopicUntil(b.channel(), AuthWireClient::kClientStream, sid, "hijacked", 1, false, {},
                                          std::chrono::steady_clock::now() + 2s);
    // 2. A Disconnect in clear: the session is not ended.
    ASSERT_TRUE(attacker.raw().disconnect(b.channel(), AuthWireClient::kClientStream, sid));
    // 3. A Connect naming the session id (a resume hint): refused, no session, nothing resumed.
    attacker.raw().setConnectHint(sid);
    EXPECT_EQ(attacker.connect(b.channel(), pc::kClientVersionAuth, 5s), -1);
    ASSERT_TRUE(attacker.lastConnect().has_value());
    EXPECT_EQ(attacker.lastConnect()->code, static_cast<int16_t>(pc::ErrorCode::HandshakeFailed));
    EXPECT_NE(attacker.lastConnect()->assignedSessionId, sid);

    std::this_thread::sleep_for(200ms);   // let the gateway poll everything the attacker sent
    EXPECT_TRUE(b.node().gatewayHasSessionForTest(sid)) << "a plaintext Disconnect never ends an encrypted session";
    // The admin's own session still works, and the topic the attacker named was never created by its request.
    const auto made = admin->defineTopicAndWait("hijacked", 1, "", false, {}, 20s);
    EXPECT_EQ(made.errorCode, 0) << made.detail << " - the attacker's DefineTopic must not have created it";
    EXPECT_EQ(admin->testSeedSessionId(), sid) << "the same session, never replaced";
    admin->closeImmediately();
}

// Issue #137: a SCRAM login over an encrypted session (the AuthServerFirst / AuthServerFinal answers are sealed), its
// counter, and a wrong password still refused over TLS.
TEST(AuthTlsRegression, LoginOverTlsRegression) {
    AuthBroker b("tlslogin", tlsOnly());
    bootstrapOverTls(b, std::string("create user ws1 with password '") + kWs1Pw + "';\n");
    auto ws1 = tlsClient(b, "ws1", kWs1Pw);
    ASSERT_EQ(connectError(*ws1), "");
    const rc::AuthStatus a = ws1->authStatus();
    EXPECT_EQ(a.kind, rc::AuthStatus::Kind::Authenticated);
    EXPECT_EQ(a.user, "ws1");
    EXPECT_EQ(a.brokerMode, pc::AuthMode::Required);
    ws1->closeImmediately();
    auto bad = tlsClient(b, "ws1", "not the right password");
    const std::string err = connectError(*bad);
    EXPECT_NE(err.find("AuthenticationFailed"), std::string::npos) << err;
    bad->closeImmediately();
}

// Issue #137: in shard mode the worker decrypts an encrypted produce and only then applies the WRITE check - a granted
// topic is appended, an ungranted one is refused NotAuthorized (45); the decrypt never bypasses the check.
TEST(AuthTlsRegression, ShardProduceDecryptsThenChecksWriteRegression) {
    AuthBroker b("tlsshard", tlsOnly(/*shard=*/true));
    bootstrapOverTls(b, std::string("create user ws1 with password '") + kWs1Pw + "';\n"
                        "grant create on topic prefix '' to user admin;\n"
                        "grant write on topic 'orders' to user ws1;\n"
                        "grant describe on cluster to user ws1;\n");
    auto admin = tlsClient(b, "admin", kAdminPw);
    ASSERT_EQ(connectError(*admin), "");
    const auto orders = admin->defineTopicAndWait("orders", 1, "", false, {}, 20s);
    const auto secret = admin->defineTopicAndWait("secret", 1, "", false, {}, 20s);
    ASSERT_EQ(orders.errorCode, 0) << orders.detail;
    ASSERT_EQ(secret.errorCode, 0) << secret.detail;
    admin->closeImmediately();

    auto ws1 = tlsClient(b, "ws1", kWs1Pw);
    ASSERT_EQ(connectError(*ws1), "");
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::All;
    pcfg.lingerUs = 1;
    pcfg.batchBytes = 1;
    pcfg.produceTimeoutMs = 20'000;
    pcfg.deliveryTimeoutMs = 30'000;
    auto producer = ws1->producer(pcfg);
    rc::Completions cq(*ws1);
    std::map<rc::Token, rc::Completion> done;
    auto poll = [&] { (void)cq.poll([&](const rc::Completion& d, std::span<const uint8_t>) { done[d.token] = d; }); };
    const std::vector<uint8_t> value{1, 2, 3, 4};
    auto sendTo = [&](int32_t topicId, rc::Token tok) {
        for (const auto dl = std::chrono::steady_clock::now() + 20s; std::chrono::steady_clock::now() < dl;) {
            if (producer.trySend(rc::TopicHandle{topicId, 1}, rc::Key(uint64_t{7}), value, cq, tok) == rc::SendResult::Ok)
                return true;
            poll();
            std::this_thread::sleep_for(1ms);
        }
        return false;
    };
    ASSERT_TRUE(sendTo(orders.topicId, 1));
    ASSERT_TRUE(sendTo(secret.topicId, 2));
    for (const auto dl = std::chrono::steady_clock::now() + 40s; std::chrono::steady_clock::now() < dl && done.size() < 2;) {
        poll();
        std::this_thread::sleep_for(2ms);
    }
    ASSERT_EQ(done.size(), 2u);
    EXPECT_EQ(done[1].status, rc::OpStatus::Ok) << "errorCode " << done[1].errorCode << ": the granted topic is appended";
    EXPECT_EQ(done[2].status, rc::OpStatus::BrokerError);
    EXPECT_EQ(done[2].errorCode, 45) << "NotAuthorized: the decrypted produce is still checked for WRITE";
    const uint64_t workerRefusals =
        b.node().onWorkerThreadForTest(0, [](Worker& wk) { return wk.workerNotAuthorizedForTest(); });
    EXPECT_EQ(workerRefusals, 1u) << "the shard worker (its own ingress, the session's key) made the refusal";
    EXPECT_EQ(b.node().gatewayNotAuthorizedForTest(), 0u) << "not the gateway";
    producer.close();
    ws1->closeImmediately();
}

// Issue #13: the bootstrap, a password change, security DDL and a __security read, all with the shipped CLI over TLS
// to a broker that serves only TLS.  Without --tls-ca the system store does not trust the test CA: the CLI fails the
// connect (it never falls back to plaintext).  RED: drop applyTls from LiveBroker::open (the CLI connects in plaintext
// and the broker refuses it HandshakeFailed).
TEST(RuneCliSecurityBrokerRegression, BootstrapAndDdlOverTlsRegression) {
    AuthBroker b("clitls", tlsOnly());
    const std::string otp = writeTemp("otp-sys", std::string(kOtp) + "\n");
    std::string out, err;
    EXPECT_NE(runCli({"security", "bootstrap", "--admin", "admin", "--cluster-key-file", b.keyFile().string(),
                      "--password-file", otp, "--broker", b.endpoint()}, &out, &err), 0)
        << "the system store does not trust the test CA: refused, never plaintext";
    EXPECT_FALSE(b.node().securityViewForTest().hasSecurityAdmin);
    bootstrapOverTls(b, "create role r;\n"
                        "create user u with password 'user-password-over-tls';\n"
                        "grant write on topic prefix 'world.' to role r;\n"
                        "grant role r to user u;\n");
    const std::string pw = writeTemp("admin.pw", std::string(kAdminPw) + "\n");
    ASSERT_EQ(runCli({"ql", "--broker", b.endpoint(), "--user", "admin", "--password-file", pw, "-e", "show grants for user u;",
                      "--tls-ca", AuthBroker::tlsCaFile().string()}, &out, &err), 0) << err;
    EXPECT_NE(out.find("WRITE"), std::string::npos) << out;
    // RUNE_TLS_CA_FILE / RUNE_TLS_HOST stand in for the flags; a wrong --tls-host fails the certificate check.
    ::setenv("RUNE_TLS_CA_FILE", AuthBroker::tlsCaFile().string().c_str(), 1);
    ::setenv("RUNE_TLS_HOST", "localhost", 1);
    EXPECT_EQ(runCli({"ql", "--broker", b.endpoint(), "--user", "admin", "--password-file", pw, "-e", "show users;"}, &out, &err), 0)
        << err;
    ::unsetenv("RUNE_TLS_HOST");
    EXPECT_NE(runCli({"ql", "--broker", b.endpoint(), "--user", "admin", "--password-file", pw, "-e", "show users;",
                      "--tls-host", "not-the-broker.example"}, &out, &err), 0) << "the certificate does not name it";
    ::unsetenv("RUNE_TLS_CA_FILE");
}

// Issue #13: --insecure-plaintext never sends credentials or the bootstrap - refused before any connect, with the reason
// - and conflicts with --tls-ca.  RED: drop the credential refusal in support::applyTls.
TEST(RuneCliSecurityBrokerRegression, InsecurePlaintextNeverSendsCredentialsRegression) {
    AuthBroker b("cliplain", tlsOnly());
    const std::string otp = writeTemp("otp-plain", std::string(kOtp) + "\n");
    std::string out, err;
    EXPECT_NE(runCli({"security", "bootstrap", "--admin", "admin", "--cluster-key-file", b.keyFile().string(),
                      "--password-file", otp, "--broker", b.endpoint(), "--insecure-plaintext"}, &out, &err), 0);
    EXPECT_NE(err.find("never sends credentials"), std::string::npos) << err;
    EXPECT_FALSE(b.node().securityViewForTest().hasSecurityAdmin);
    EXPECT_NE(runCli({"ql", "--broker", b.endpoint(), "--user", "admin", "--password-file", otp, "-e", "show users;",
                      "--insecure-plaintext"}, &out, &err), 0);
    EXPECT_NE(err.find("never sends credentials"), std::string::npos) << err;
    EXPECT_NE(runCli({"ql", "--broker", b.endpoint(), "-e", "show history;", "--insecure-plaintext", "--tls-ca",
                      AuthBroker::tlsCaFile().string()}, &out, &err), 0);
    EXPECT_NE(err.find("cannot be combined"), std::string::npos) << err;
    EXPECT_TRUE(b.awaitCounter(kLoginsOkCounterTypeId, 0)) << "nothing logged in";
    // The cluster is still bootstrappable over TLS: nothing was consumed.
    bootstrapOverTls(b);
    EXPECT_TRUE(b.node().securityViewForTest().hasSecurityAdmin);
}

// Adversarial review of PR #476 (issue #10, shard worker ingress): a Produce in clear naming an ENCRYPTED session's id,
// sent straight to the worker's own ingress and to the gateway, is never appended and never acked - the worker's
// ingress is a second entry point to the same session state.  The real client's next produce lands at offset 0.
TEST(AuthSessionBindingRegression, APlaintextProduceNamingAnEncryptedSessionIsNeverAppendedAdversarialRegression) {
    AuthBroker b("tlsplainproduce", tlsOnly(/*shard=*/true));
    bootstrapOverTls(b, std::string("create user ws1 with password '") + kWs1Pw + "';\n"
                        "grant create on topic prefix '' to user admin;\n"
                        "grant write on topic 'orders' to user ws1;\n"
                        "grant describe on cluster to user ws1;\n");
    auto admin = tlsClient(b, "admin", kAdminPw);
    ASSERT_EQ(connectError(*admin), "");
    const auto orders = admin->defineTopicAndWait("orders", 1, "", false, {}, 20s);
    ASSERT_EQ(orders.errorCode, 0) << orders.detail;
    admin->closeImmediately();

    auto ws1 = tlsClient(b, "ws1", kWs1Pw, rc::RunnerMode::Invoker);
    ASSERT_EQ(connectError(*ws1), "");
    const int64_t sid = ws1->testSeedSessionId();
    ASSERT_GT(sid, 0);

    AuthWireClient& attacker = b.wire();
    const auto batch = makeBatch(1);
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(attacker.raw().produce(b.workerIngressChannel(), AuthWireClient::kClientStream, sid, 9000 + i,
                                           orders.topicId, 0, 1, batch));
        ASSERT_TRUE(attacker.raw().produce(b.channel(), AuthWireClient::kClientStream, sid, 9100 + i, orders.topicId, 0,
                                           1, batch));
    }
    // Event-driven: the worker has seen (and refused to open) every clear frame sent to its ingress.
    const auto dl = std::chrono::steady_clock::now() + 20s;
    uint64_t refused = 0;
    while (std::chrono::steady_clock::now() < dl &&
           (refused = b.node().onWorkerThreadForTest(0, [](Worker& wk) { return wk.decryptFailuresForTest(); })) < 4) {
        ws1->doWork();
    }
    EXPECT_GE(refused, 4u) << "the worker never saw the clear produces (or opened them)";

    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::All; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    pcfg.produceTimeoutMs = 20'000; pcfg.deliveryTimeoutMs = 30'000;
    auto producer = ws1->producer(pcfg);
    rc::Completions cq(*ws1);
    std::optional<rc::Completion> done;
    const std::vector<uint8_t> value{1, 2, 3, 4};
    bool sent = false;
    for (const auto d2 = std::chrono::steady_clock::now() + 30s; std::chrono::steady_clock::now() < d2 && !done;) {
        if (!sent) sent = producer.trySend(rc::TopicHandle{orders.topicId, 1}, rc::Key(uint64_t{7}), value, cq, 1) ==
                          rc::SendResult::Ok;
        ws1->doWork();
        (void)cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) { done = c; });
    }
    ASSERT_TRUE(done.has_value());
    EXPECT_EQ(done->status, rc::OpStatus::Ok) << done->errorCode;
    EXPECT_EQ(done->value, 0) << "a clear produce naming the encrypted session was appended before it";
    producer.close();
    ws1->closeImmediately();
}
