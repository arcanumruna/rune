// RuneCliSecurityBrokerRegressionTest.cpp - authentication Task 11: the REAL `rune` CLI (rune::cli::run, which builds
// its own Client) against a REAL authenticating BrokerNode: `rune security bootstrap`, the forced first password
// change, then security DDL and `show grants` as the admin.  Credentials only ever travel as --user /
// --password-file; every wait is bounded by the CLI's own client timeouts.
#include "support/AuthBroker.h"

#include <rune/cli/Cli.h>
#include <rune/client/Client.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace rune::node::test;
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

int runCli(std::vector<std::string> args, std::string* out = nullptr, std::string* err = nullptr) {
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
TEST(AuthBootstrapWindowRegression, TheLastAdminThroughARoleIsGuardedTooRegression) {
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
