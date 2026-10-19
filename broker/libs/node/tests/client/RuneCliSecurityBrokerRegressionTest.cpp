// RuneCliSecurityBrokerRegressionTest.cpp - authentication Task 11: the REAL `rune` CLI (rune::cli::run, which builds
// its own Client) against a REAL authenticating BrokerNode: `rune security bootstrap`, the forced first password
// change, then security DDL and `show grants` as the admin.  Credentials only ever travel as --user /
// --password-file; every wait is bounded by the CLI's own client timeouts.
#include "support/AuthBroker.h"

#include <rune/cli/Cli.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
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
