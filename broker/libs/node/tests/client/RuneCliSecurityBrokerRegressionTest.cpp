// RuneCliSecurityBrokerRegressionTest.cpp - authentication Task 11: the REAL `rune` CLI (rune::cli::run, which builds
// its own Client) against a REAL authenticating BrokerNode: `rune security bootstrap`, the forced first password
// change, then security DDL and `show grants` as the admin.  Credentials only ever travel as --user /
// --password-file; every wait is bounded by the CLI's own client timeouts.
#include "support/AuthBroker.h"

#include <rune/cli/Cli.h>
#include <rune/client/Client.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace rune::node::test;
namespace pc = rune::protocol::client;
namespace fs = std::filesystem;

namespace {

fs::path scratchDir() {
    static const fs::path d = [] {
        fs::path p = fs::temp_directory_path() / ("rune_cli_sec_test_" + std::to_string(::getpid()));
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

// Issue #13: the CLI reaches a UDP broker over TLS and sends credentials only over it; every call trusts the broker's
// test CA (the broker accepts plaintext too: its raw-frame rig helpers are plaintext).
int runCli(std::vector<std::string> args, std::string* out = nullptr, std::string* err = nullptr) {
    if (!args.empty() && std::find(args.begin(), args.end(), "--insecure-plaintext") == args.end() &&
        std::find(args.begin(), args.end(), "--tls-ca") == args.end()) {
        args.push_back("--tls-ca");
        args.push_back(AuthBroker::tlsCaFile().string());
    }
    std::stringstream o, e;
    const int rc = rune::cli::run(std::move(args), o, e);
    if (out) *out = o.str();
    if (err) *err = e.str();
    return rc;
}

std::string squeeze(const std::string& s) {   // table padding -> single spaces
    std::string out;
    bool space = false;
    for (const char c : s) {
        if (c == ' ') { if (!space) out += c; space = true; }
        else { out += c; space = false; }
    }
    return out;
}

constexpr const char* kOtp = "one-time-password-1";
constexpr const char* kWs1Password = "ws1-password-123456";
constexpr const char* kNewPassword = "a-brand-new-password-1";

}  // namespace

TEST(RuneCliSecurityBrokerRegression, BootstrapThenChangePassword) {
    AuthBroker b("clibootstrap");
    const std::string keyFile = b.keyFile().string();
    const std::string otpFile = writeTemp("otp1", std::string(kOtp) + "\n");
    std::string out, err;
    ASSERT_EQ(runCli({"security", "bootstrap", "--admin", "admin", "--cluster-key-file", keyFile, "--password-file", otpFile,
                      "--broker", b.endpoint()}, &out, &err), 0) << err;
    EXPECT_NE(out.find("bootstrapped admin"), std::string::npos) << out;
    // A second run is refused (exit 1) and says why: the cluster already has a security admin.
    EXPECT_EQ(runCli({"security", "bootstrap", "--admin", "admin2", "--cluster-key-file", keyFile, "--password-file", otpFile,
                      "--broker", b.endpoint()}, &out, &err), 1);
    EXPECT_TRUE(err.find("already bootstrapped") != std::string::npos || err.find("already has a security admin") != std::string::npos)
        << err;
    // The one-time password must be changed first: a statement before that is answered PasswordChangeRequired (47)...
    EXPECT_NE(runCli({"ql", "--broker", b.endpoint(), "--user", "admin", "--password-file", otpFile, "-e", "create role r0;"}, &out, &err), 0);
    // ... and ALTER USER admin SET PASSWORD is the one statement it may run.
    // (a password literal never goes on the command line: the statement is a file)
    const std::string change = writeTemp("change.rql", std::string("alter user admin set password '") + kNewPassword + "';\n");
    ASSERT_EQ(runCli({"ql", "--broker", b.endpoint(), "--user", "admin", "--password-file", otpFile, "-f", change}, &out, &err), 0) << err;
    const std::string newFile = writeTemp("pw2", std::string(kNewPassword) + "\n");
    EXPECT_EQ(runCli({"ql", "--broker", b.endpoint(), "--user", "admin", "--password-file", newFile, "-e", "create role r;"}, &out, &err), 0)
        << err;
    // The old one-time password no longer logs in.
    EXPECT_NE(runCli({"ql", "--broker", b.endpoint(), "--user", "admin", "--password-file", otpFile, "-e", "create role r2;"}, &out, &err), 0);
}

TEST(RuneCliSecurityBrokerRegression, CreateUserGrantAndShowGrants) {
    AuthBroker b("cligrants");
    b.bootstrapAdmin(kNewPassword);
    const std::string newFile = writeTemp("pw3", std::string(kNewPassword) + "\n");
    const std::string script = writeTemp("grants.rql",
        "create role r;\n"
        "create user u with password 'user-password-1234';\n"
        "grant write on topic prefix 'world.' to role r;\n"
        "grant role r to user u;\n");
    std::string out, err;
    ASSERT_EQ(runCli({"ql", "--broker", b.endpoint(), "--user", "admin", "--password-file", newFile, "-f", script}, &out, &err), 0) << err;
    ASSERT_EQ(runCli({"ql", "--broker", b.endpoint(), "--user", "admin", "--password-file", newFile, "-e", "show grants for user u;"},
                     &out, &err), 0) << err;
    EXPECT_NE(squeeze(out).find("role:r WRITE TOPIC PREFIX 'world.'"), std::string::npos) << out;
    ASSERT_EQ(runCli({"ql", "--broker", b.endpoint(), "--user", "admin", "--password-file", newFile, "-e", "show users;"}, &out, &err), 0) << err;
    EXPECT_NE(out.find("u"), std::string::npos);
}

namespace {
/// `rune ql` as the admin (password kNewPassword) running `statement`.
int adminQl(AuthBroker& b, const std::string& statement, std::string* err = nullptr) {
    static const std::string pw = writeTemp("admin-pw", std::string(kNewPassword) + "\n");
    static int n = 0;
    const std::string file = writeTemp("stmt" + std::to_string(++n) + ".rql", statement + "\n");   // never a literal in argv
    std::string out, e;
    const int rc = runCli({"ql", "--broker", b.endpoint(), "--user", "admin", "--password-file", pw, "-f", file}, &out, &e);
    if (err) *err = e;
    return rc;
}
bool awaitNoSecurityAdmin(AuthBroker& b) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (b.node().securityViewForTest().hasSecurityAdmin) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return true;
}
std::unique_ptr<rune::client::Client> credentialedClient(AuthBroker& b, const std::string& user, const std::string& pw) {
    rune::client::ClientConfig c;
    c.gatewayChannel  = b.channel();
    c.gatewayStreamId = 1;
    c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    c.egressStreamId  = 2;
    c.dataDir         = b.clientDir().string();
    c.username        = user;
    c.password        = pw;
    c.offerCrypto     = true;                    // issue #14: credentials only over TLS
    c.tlsTrustPem     = AuthBroker::tlsCaPem();
    return std::make_unique<rune::client::Client>(c);
}
}  // namespace

// Issue #121: removing the last enabled security admin is refused unless the statement says FORCE - whichever
// statement would do it (DISABLE, DROP USER, REVOKE ADMIN ON CLUSTER); with another enabled admin it is allowed.
TEST(AuthBootstrapWindowRegression, LastAdminCannotBeDisabledWithoutForceRegression) {
    AuthBroker b("clilastadmin");
    b.bootstrapAdmin(kNewPassword);
    std::string err;
    for (const char* st : {"alter user admin disable;", "drop user admin;", "revoke admin on cluster from user admin;"}) {
        EXPECT_NE(adminQl(b, st, &err), 0) << st;
        EXPECT_NE(err.find("last enabled security admin"), std::string::npos) << st << ": " << err;
    }
    EXPECT_TRUE(b.node().securityViewForTest().hasSecurityAdmin) << "nothing was applied";
    // A second enabled admin: then the first may go without FORCE.
    ASSERT_EQ(adminQl(b, "create user admin2 with password 'admin2-password-1234';", &err), 0) << err;
    ASSERT_EQ(adminQl(b, "grant admin on cluster to user admin2;", &err), 0) << err;
    EXPECT_EQ(adminQl(b, "revoke admin on cluster from user admin2;", &err), 0) << "admin still holds it: " << err;
    EXPECT_NE(adminQl(b, "revoke admin on cluster from user admin;", &err), 0) << "admin2 lost it again";
    ASSERT_EQ(adminQl(b, "grant admin on cluster to user admin2;", &err), 0) << err;
    // Disabling yourself ends your own session (SessionRevoked), so the outcome is read from the state.
    (void)adminQl(b, "alter user admin disable;", &err);
    EXPECT_NE(adminQl(b, "create role after_disable;", &err), 0) << "admin is disabled: admin2 remained, no FORCE needed";
    EXPECT_TRUE(b.node().securityViewForTest().hasSecurityAdmin) << "admin2";
}

// Issue #121: a REOPENED bootstrap window (the last admin disabled with FORCE) answers MustAuthenticate, so the other
// users still log in with the real client; `rune security bootstrap` still recovers the cluster (cluster-key MAC).
TEST(AuthBootstrapWindowRegression, ACredentialedClientLogsInDuringAReopenedWindowRegression) {
    AuthBroker b("clireopen");
    b.bootstrapAdmin(kNewPassword);
    std::string err;
    ASSERT_EQ(adminQl(b, std::string("create user ws1 with password '") + kWs1Password + "';", &err), 0) << err;
    (void)adminQl(b, "alter user admin disable force;", &err);   // the admin's own session ends (SessionRevoked)
    ASSERT_TRUE(awaitNoSecurityAdmin(b)) << "the window is open again";

    auto ws1 = credentialedClient(b, "ws1", kWs1Password);
    std::string connectErr;
    try { ws1->connectAndWait(std::chrono::seconds(20)); } catch (const std::exception& e) { connectErr = e.what(); }
    EXPECT_EQ(connectErr, "") << "a credentialed client logs in while the window is open";
    EXPECT_EQ(ws1->authStatus().kind, rune::client::AuthStatus::Kind::Authenticated);
    ws1->closeImmediately();

    // The cluster-key holder re-bootstraps through the same MustAuthenticate answer (no login on that connection).
    const std::string otpFile = writeTemp("otp-reopen", std::string(kOtp) + "\n");
    std::string out;
    EXPECT_EQ(runCli({"security", "bootstrap", "--admin", "admin9", "--cluster-key-file", b.keyFile().string(),
                      "--password-file", otpFile, "--broker", b.endpoint()}, &out, &err), 0) << err;
    EXPECT_TRUE(b.node().securityViewForTest().hasSecurityAdmin);
    // ... and once an admin exists again, a second bootstrap is refused with the explicit text.
    EXPECT_EQ(runCli({"security", "bootstrap", "--admin", "admin10", "--cluster-key-file", b.keyFile().string(),
                      "--password-file", otpFile, "--broker", b.endpoint()}, &out, &err), 1);
    EXPECT_NE(err.find("already bootstrapped"), std::string::npos) << err;
}

namespace {
int userQl(AuthBroker& b, const std::string& user, const std::string& pwFile, const std::string& statement, std::string* err) {
    static int n = 0;
    const std::string file = writeTemp("ustmt" + std::to_string(++n) + ".rql", statement + "\n");
    std::string out;
    return runCli({"ql", "--broker", b.endpoint(), "--user", user, "--password-file", pwFile, "-f", file}, &out, err);
}
}  // namespace

// Issue #121: the guard judges the EFFECTIVE admin, also when ADMIN comes through a role: dropping the role, revoking
// it from the user, or revoking ADMIN from the role are each refused while that is the last enabled admin.
TEST(AuthBootstrapWindowRegression, TheLastAdminThroughARoleIsGuardedTooAdversarialRegression) {
    AuthBroker b("clilastrole");
    b.bootstrapAdmin(kNewPassword);
    std::string err;
    ASSERT_EQ(adminQl(b, "create role ops;", &err), 0) << err;
    ASSERT_EQ(adminQl(b, "grant admin on cluster to role ops;", &err), 0) << err;
    ASSERT_EQ(adminQl(b, "create user admin2 with password 'admin2-password-1234';", &err), 0) << err;
    ASSERT_EQ(adminQl(b, "grant role ops to user admin2;", &err), 0) << err;
    (void)adminQl(b, "alter user admin disable;", &err);   // admin2 (through ops) remains: allowed, ends admin's session
    ASSERT_NE(adminQl(b, "create role probe;", &err), 0) << "admin is disabled";
    const std::string pw2 = writeTemp("admin2-pw", "admin2-password-1234\n");
    for (const char* st : {"drop role ops;", "revoke role ops from user admin2;", "revoke admin on cluster from role ops;"}) {
        EXPECT_NE(userQl(b, "admin2", pw2, st, &err), 0) << st;
        EXPECT_NE(err.find("last enabled security admin"), std::string::npos) << st << ": " << err;
    }
    EXPECT_TRUE(b.node().securityViewForTest().hasSecurityAdmin);
    EXPECT_EQ(userQl(b, "admin2", pw2, "create role still_admin;", &err), 0) << "admin2 still holds ADMIN: " << err;
}

// Issue #114 through the REAL CLI and client on a REAL broker: a user without ADMIN changes its own password (the
// client proves its current one on the wire); the new password then logs in.
TEST(AuthEnforcementRegression, ARealClientChangesItsOwnPasswordWithAProofRegression) {
    AuthBroker b("cliselfpw");
    b.bootstrapAdmin(kNewPassword);
    std::string err;
    ASSERT_EQ(adminQl(b, std::string("create user ws1 with password '") + kWs1Password + "';", &err), 0) << err;
    const std::string ws1Pw = writeTemp("ws1-pw", std::string(kWs1Password) + "\n");
    EXPECT_EQ(userQl(b, "ws1", ws1Pw, "alter user ws1 add password 'ws1-second-password-1';", &err), 0) << err;
    auto second = credentialedClient(b, "ws1", "ws1-second-password-1");
    std::string connectErr;
    try { second->connectAndWait(std::chrono::seconds(20)); } catch (const std::exception& e) { connectErr = e.what(); }
    EXPECT_EQ(connectErr, "") << "the added password logs in";
    second->closeImmediately();
    // Changing ANOTHER user's password still needs ADMIN, proof or not.
    EXPECT_NE(userQl(b, "ws1", ws1Pw, "alter user admin set password 'hijacked-password-1';", &err), 0);
    EXPECT_NE(err.find("ADMIN"), std::string::npos) << err;
}

// ── Adversarial: the bootstrap window (the area of #121) ────────────────────────────────────────────────────────────
// Replays and races at the window: a captured bootstrap frame replayed after the window REOPENS is MAC-valid but
// dedupes on its nonce - it creates no admin; two new bootstraps racing in the reopened window create exactly one.
TEST(AuthBootstrapWindowRegression, AReplayedBootstrapCreatesNoAdminAndRacingBootstrapsCreateOneAdversarialRegression) {
    AuthBroker b("clireplay");
    AuthWireClient& w = b.wire();
    const int64_t s0 = w.connect(b.channel());
    ASSERT_GT(s0, 0);
    const std::vector<uint8_t> captured = w.bootstrapBytes(s0, b.keyContents(), "admin", AuthBroker::kBootstrapPassword);
    ASSERT_EQ(w.sendBootstrapBytes(b.channel(), s0, captured).errorCode, 0);
    // The admin changes its one-time password and leaves for good (FORCE): the window reopens.
    const int64_t s1 = w.connect(b.channel());
    ASSERT_TRUE(w.login(b.channel(), s1, "admin", {AuthBroker::kBootstrapPassword, AuthBroker::kBootstrapPassword}).ok());
    ASSERT_EQ(w.alterPassword(b.channel(), s1, "admin", pc::UserChangeKind::SetPassword, kNewPassword).errorCode, 0);
    ASSERT_EQ(w.createUser(b.channel(), s1, "ws1", kWs1Password).errorCode, 0);
    pc::SecurityDdl disable; disable.sessionId = s1; disable.kind = pc::SecurityOpKind::AlterUser; disable.name = "admin";
    disable.change = pc::UserChangeKind::Disable; disable.flags = pc::kSecurityDdlFlagForce;
    // The admin's own session is revoked by its DISABLE (SessionRevoked, no DdlResponse): do not wait out the deadline.
    (void)w.securityDdl(b.channel(), disable, "ALTER USER admin DISABLE FORCE;", std::chrono::seconds{2});
    ASSERT_TRUE(awaitNoSecurityAdmin(b));
    const int64_t applied = b.node().securityViewForTest().appliedOffset;

    // The replay: answered with its ORIGINAL result, nothing appended, no admin.
    const int64_t s2 = w.connect(b.channel());
    ASSERT_GT(s2, 0);
    EXPECT_EQ(w.lastConnect()->authState, pc::AuthState::MustAuthenticate);
    const auto replay = w.sendBootstrapBytes(b.channel(), s2, captured);
    EXPECT_EQ(replay.errorCode, 0) << replay.detail;
    EXPECT_EQ(b.node().securityViewForTest().appliedOffset, applied) << "the replay appended nothing";
    EXPECT_FALSE(b.node().securityViewForTest().hasSecurityAdmin) << "and created no admin";

    // Two fresh bootstraps back to back on two sessions: exactly one admin.
    AuthWireClient& w2 = b.wire();
    const int64_t sa = w.connect(b.channel()), sb = w2.connect(b.channel());
    ASSERT_GT(sa, 0);
    ASSERT_GT(sb, 0);
    const auto fa = w.bootstrapBytes(sa, b.keyContents(), "rescueA", "rescue-otp-123456", 0x50);
    const auto fb = w2.bootstrapBytes(sb, b.keyContents(), "rescueB", "rescue-otp-123456", 0x60);
    std::thread other([&] { (void)w2.sendBootstrapBytes(b.channel(), sb, fb); });
    (void)w.sendBootstrapBytes(b.channel(), sa, fa);
    other.join();
    EXPECT_TRUE(b.node().securityViewForTest().hasSecurityAdmin);
    EXPECT_EQ(b.node().securityViewForTest().appliedOffset, applied + 1) << "one bootstrap event, not two";
}

// A reopened window survives a restart: the security log replays, a connect is still told MustAuthenticate and the
// real client still logs in (a restart must not turn the cluster back into a "never bootstrapped" one).
TEST(AuthBootstrapWindowRegression, AReopenedWindowSurvivesARestartAdversarialRegression) {
    AuthBroker b("clireopenrestart");
    b.bootstrapAdmin(kNewPassword);
    std::string err;
    ASSERT_EQ(adminQl(b, std::string("create user ws1 with password '") + kWs1Password + "';", &err), 0) << err;
    (void)adminQl(b, "alter user admin disable force;", &err);
    ASSERT_TRUE(awaitNoSecurityAdmin(b));
    b.restart(/*reloginAdmin=*/false);
    ASSERT_FALSE(b.node().securityViewForTest().hasSecurityAdmin);
    AuthWireClient& w = b.wire();
    ASSERT_GT(w.connect(b.channel()), 0);
    EXPECT_EQ(w.lastConnect()->authState, pc::AuthState::MustAuthenticate);
    auto ws1 = credentialedClient(b, "ws1", kWs1Password);
    std::string connectErr;
    try { ws1->connectAndWait(std::chrono::seconds(20)); } catch (const std::exception& e) { connectErr = e.what(); }
    EXPECT_EQ(connectErr, "");
    EXPECT_EQ(ws1->authStatus().kind, rune::client::AuthStatus::Kind::Authenticated);
    ws1->closeImmediately();
    // Wrong credentials in the reopened window are a plain failed login, not "no security admin yet".
    auto wrong = credentialedClient(b, "ws1", "not-the-password-1");
    try { wrong->connectAndWait(std::chrono::seconds(20)); connectErr.clear(); } catch (const std::exception& e) { connectErr = e.what(); }
    EXPECT_NE(connectErr.find("AuthenticationFailed"), std::string::npos) << connectErr;
    EXPECT_EQ(connectErr.find("no security admin"), std::string::npos) << connectErr;
    wrong->closeImmediately();
}

// Two enabled admins disabling EACH OTHER at the same moment: the leader serializes security DDL (the next request is
// judged only after the previous one applied), so exactly one succeeds and one admin always remains.
TEST(AuthBootstrapWindowRegression, TwoAdminsDisablingEachOtherConcurrentlyLeaveOneAdversarialRegression) {
    AuthBroker b("climutual");
    b.bootstrapAdmin(kNewPassword);
    std::string err;
    ASSERT_EQ(adminQl(b, "create user admin2 with password 'admin2-password-1234';", &err), 0) << err;
    ASSERT_EQ(adminQl(b, "grant admin on cluster to user admin2;", &err), 0) << err;
    {
        AuthWireClient& w1 = b.wire();
        AuthWireClient& w2 = b.wire();
        const int64_t s1 = w1.connect(b.channel()), s2 = w2.connect(b.channel());
        ASSERT_TRUE(w1.login(b.channel(), s1, "admin", {kNewPassword, kNewPassword}).ok());
        ASSERT_TRUE(w2.login(b.channel(), s2, "admin2", {"admin2-password-1234", "admin2-password-1234"}).ok());
        // The loser's session is revoked by the winner's DISABLE (SessionRevoked, no DdlResponse): a short wait each.
        auto disable = [&](AuthWireClient& w, int64_t sid, const char* who) {
            pc::SecurityDdl m; m.sessionId = sid; m.kind = pc::SecurityOpKind::AlterUser; m.name = who;
            m.change = pc::UserChangeKind::Disable;
            return w.securityDdl(b.channel(), m, std::string("ALTER USER ") + who + " DISABLE;", std::chrono::seconds{3});
        };
        AuthWireClient::DdlAnswer a1, a2;
        std::thread t2([&] { a2 = disable(w2, s2, "admin"); });
        a1 = disable(w1, s1, "admin2");
        t2.join();
        const int oks = (a1.errorCode == 0 ? 1 : 0) + (a2.errorCode == 0 ? 1 : 0);
        EXPECT_LE(oks, 1) << a1.errorCode << " " << a1.detail << " / " << a2.errorCode << " " << a2.detail;
        EXPECT_TRUE(b.node().securityViewForTest().hasSecurityAdmin) << "one admin always remains";
    }
}
