// Authentication Tasks 7-8 + review B — the rune CLI against the InvokerGateway's strict SCRAM-speaking broker model:
// the global credential flags (--user / --password-file, on `rune ql`, `rune security` AND `rune schema`), the
// dev-mode banner, `rune ql` security statements, and `rune security bootstrap` end to end against a REQUIRED broker
// (Anonymous only before the bootstrap, only template 80, a MAC check, accepted once).  The CLI runs on the test
// thread (its own Invoker client); the fake broker is pumped on a helper thread that attaches to the CLI's
// embedded driver once its directory appears.  A CLI run closes its driver on exit, so every invocation gets its
// own fake; cluster state that must survive between runs is snapshotted and re-installed.
#include "support/InvokerGateway.h"

#include "rune/cli/Cli.h"

#include <rune/crypto/Kdf.h>
#include <rune/crypto/Scram.h>
#include <rune/protocol/client/SecurityMessages.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <thread>
#include <unistd.h>

namespace pc = rune::protocol::client;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {
std::atomic<int> g_ctr{0};

/// The cluster state that outlives one CLI run.
struct ClusterState {
    std::map<std::string, std::vector<rune::crypto::scram::Verifier>> users;
    std::set<std::string> mustChange;
    bool bootstrapped = false;
};

/// A fake broker on a helper thread.  `setup` runs on that thread right after the gateway exists.
struct LiveFake {
    std::string dir;
    int32_t in, out;
    std::atomic<bool> stop{false};
    std::thread t;
    std::mutex m;
    std::vector<std::vector<uint8_t>> securityDdls;
    // Read after finish():
    ClusterState state;
    int authOk = 0, authFailed = 0, authFirsts = 0, bootstraps = 0;
    std::vector<uint8_t> lastDdlFrame;

    LiveFake(int32_t i, pc::AuthMode md, const std::function<void(test_support::InvokerGateway&, LiveFake&)>& setup)
        : in(i), out(i + 1) {
        dir = "/tmp/cli_sec_live_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr.fetch_add(1));
        fs::create_directories(dir);
        t = std::thread([this, md, setup] {
            const std::string cnc = dir + "/aeron/cnc.dat";
            for (int k = 0; k < 6000 && !stop.load() && !fs::exists(cnc); ++k) std::this_thread::sleep_for(2ms);
            if (stop.load()) return;
            std::this_thread::sleep_for(50ms);   // let the driver finish writing its cnc file
            test_support::InvokerGateway gw(dir + "/aeron", "aeron:ipc", in, "aeron:ipc", out);
            gw.authMode = md;
            setup(gw, *this);
            while (!stop.load()) { if (gw.pump() == 0) std::this_thread::yield(); }
            std::lock_guard<std::mutex> l(m);
            state.users = gw.users; state.mustChange = gw.mustChange; state.bootstrapped = gw.bootstrapped;
            authOk = gw.authOk; authFailed = gw.authFailed; authFirsts = gw.authFirsts; bootstraps = gw.bootstraps;
        });
    }
    void finish() { stop.store(true); if (t.joinable()) t.join(); }
    ~LiveFake() { finish(); std::error_code ec; fs::remove_all(dir, ec); }
    [[nodiscard]] std::vector<std::string> baseArgs() const {
        return {"--broker", "aeron:ipc", "--cli-stream-in", std::to_string(in), "--cli-stream-out", std::to_string(out),
                "--cli-data-dir", dir};
    }
};

struct CliRun { int code; std::string out, err; };
CliRun runCli(std::vector<std::string> args) {
    std::ostringstream o, e;
    const int code = rune::cli::run(std::move(args), o, e);
    return {code, o.str(), e.str()};
}
std::string writeFile(const std::string& path, const std::string& content) {
    std::ofstream(path, std::ios::binary) << content;
    return path;
}
std::vector<std::string> cat(std::vector<std::string> a, const std::vector<std::string>& b) {
    a.insert(a.end(), b.begin(), b.end());
    return a;
}
fs::path scratch() {
    static const fs::path d = [] {
        fs::path p = fs::temp_directory_path() / ("cli_sec_live_files_" + std::to_string(::getpid()));
        fs::create_directories(p);
        return p;
    }();
    return d;
}
using FakeSetup = std::function<void(test_support::InvokerGateway&, LiveFake&)>;
int32_t g_stream = 3900;
/// One CLI invocation against its OWN fake broker; the fake (finished) is returned for inspection.
CliRun live(pc::AuthMode mode, const FakeSetup& setup, const std::vector<std::string>& args,
            std::unique_ptr<LiveFake>* hold = nullptr, const std::string& stdinText = {}, bool useStdin = false) {
    auto fake = std::make_unique<LiveFake>(g_stream, mode, setup);
    g_stream += 2;
    std::istringstream in(stdinText);
    std::streambuf* old = nullptr;
    if (useStdin) old = std::cin.rdbuf(in.rdbuf());
    const CliRun r = runCli(cat(args, fake->baseArgs()));
    if (useStdin) std::cin.rdbuf(old);
    fake->finish();
    if (hold) *hold = std::move(fake);
    return r;
}
const FakeSetup kNothing = [](test_support::InvokerGateway&, LiveFake&) {};
/// FakeSetup: a security-DDL handler that answers OK (and records the frame).
FakeSetup ddlOk(std::atomic<int>* count = nullptr) {
    return [count](test_support::InvokerGateway& gw, LiveFake& f) {
        gw.addUser("ws1", "correct horse battery");
        gw.templateHandlers[pc::SecurityDdl::TEMPLATE_ID] = [&gw, &f, count](int, const uint8_t* b, std::size_t n) {
            auto m = pc::SecurityDdl::decode(b, n);
            if (!m) return;
            { std::lock_guard<std::mutex> l(f.m); f.lastDdlFrame.assign(b, b + n); }
            if (count) ++*count;
            pc::DdlResponse d; d.sessionId = m->sessionId; d.correlationId = m->correlationId; d.objectId = 5; d.newVersion = 1;
            gw.sendDdlResponse(0, d);
        };
    };
}
const std::string kClusterKey(64, 'a');
} // namespace

TEST(RuneCliSecurityLiveRegression, LoginFlagsAuthenticateAndSecurityDdlRunsRedacted) {
    std::atomic<int> ddls{0};
    const std::string pwFile = writeFile((scratch() / "pw1").string(), "correct horse battery\n");   // trailing newline: stripped
    const auto r = live(pc::AuthMode::Required, ddlOk(&ddls),
                        {"ql", "--user", "ws1", "--password-file", pwFile, "-e", "create role world_server;"});
    ASSERT_EQ(r.code, 0) << r.err;
    EXPECT_NE(r.out.find("ok: CREATE ROLE world_server;"), std::string::npos) << r.out;
    EXPECT_EQ(ddls.load(), 1);
    EXPECT_EQ(r.err.find("WARNING"), std::string::npos) << "an enforcing broker prints no dev banner";
    EXPECT_EQ(r.out.find("correct horse"), std::string::npos);
    EXPECT_EQ(r.err.find("correct horse"), std::string::npos);
}

TEST(RuneCliSecurityLiveRegression, ABadPasswordFileIsAnExplicitAuthenticationFailure) {
    const std::string pwFile = writeFile((scratch() / "pw2").string(), "not the right password\n");
    const FakeSetup withUser = [](test_support::InvokerGateway& gw, LiveFake&) { gw.addUser("ws1", "correct horse battery"); };
    const auto r = live(pc::AuthMode::Required, withUser,
                        {"ql", "--user", "ws1", "--password-file", pwFile, "-e", "create role r;"});
    EXPECT_EQ(r.code, 1);
    EXPECT_NE(r.err.find("AuthenticationFailed"), std::string::npos) << r.err;
    EXPECT_EQ(r.err.find("not the right"), std::string::npos) << "the password is never echoed";
    // No credentials at all: the same failure (the broker enforces).
    const auto none = live(pc::AuthMode::Required, withUser, {"ql", "-e", "create role r;"});
    EXPECT_EQ(none.code, 1);
    EXPECT_NE(none.err.find("AuthenticationFailed"), std::string::npos) << none.err;
}

TEST(RuneCliSecurityLiveRegression, DevModePrintsTheBannerOnStderr) {
    const auto r = live(pc::AuthMode::DevDisabled, ddlOk(), {"ql", "-e", "create role r;"});
    ASSERT_EQ(r.code, 0) << r.err;
    const std::string banner = "WARNING: this broker runs with authentication DISABLED (RUNE_AUTH=disabled, dev mode)";
    const auto at = r.err.find(banner);
    ASSERT_NE(at, std::string::npos) << r.err;
    EXPECT_EQ(r.err.find(banner, at + 1), std::string::npos) << "exactly once";
}

// Review B I6: the credential flags and the banner are global — `rune schema …` logs in like `rune ql` does.
TEST(RuneCliSecurityLiveRegression, SchemaCommandsLogInWithTheGlobalFlags) {
    const std::string pwFile = writeFile((scratch() / "pw3").string(), "correct horse battery\n");
    const FakeSetup withUser = [](test_support::InvokerGateway& gw, LiveFake&) { gw.addUser("ws1", "correct horse battery"); };
    {
        std::unique_ptr<LiveFake> fake;
        const auto r = live(pc::AuthMode::Required, withUser, {"schema", "list", "--user", "ws1", "--password-file", pwFile},
                            &fake);
        EXPECT_EQ(r.code, 0) << r.err;
        EXPECT_EQ(fake->authOk, 1) << "the schema command authenticated";
    }
    {   // without the flags: AuthenticationFailed, exit 1
        const auto r = live(pc::AuthMode::Required, withUser, {"schema", "list"});
        EXPECT_EQ(r.code, 1);
        EXPECT_NE(r.err.find("AuthenticationFailed"), std::string::npos) << r.err;
    }
    {   // an unreadable password file is a clean CLI error, not an exception
        const auto r = live(pc::AuthMode::Required, withUser,
                            {"schema", "list", "--user", "ws1", "--password-file", "/nonexistent/pw"});
        EXPECT_EQ(r.code, 1);
        EXPECT_NE(r.err.find("rune error"), std::string::npos) << r.err;
    }
    {   // dev mode: the banner, once, on a schema command too
        const auto r = live(pc::AuthMode::DevDisabled, kNothing, {"schema", "list"});
        EXPECT_EQ(r.code, 0) << r.err;
        const std::string banner = "WARNING: this broker runs with authentication DISABLED";
        const auto at = r.err.find(banner);
        ASSERT_NE(at, std::string::npos) << r.err;
        EXPECT_EQ(r.err.find(banner, at + 1), std::string::npos);
    }
    {   // --password is refused on schema commands too
        const auto r = runCli({"schema", "list", "--password", "secret"});
        EXPECT_EQ(r.code, 2);
        EXPECT_EQ(r.err.find("secret"), std::string::npos);
    }
}

// Review B I5 + spec §7: the bootstrap against a REQUIRED, not-yet-bootstrapped broker — Anonymous session, only
// template 80, the cluster-key MAC verified, the admin created must-change; then the one-time password works for
// exactly one thing (changing it), and a second bootstrap is explicitly refused.
TEST(RuneCliSecurityLiveRegression, BootstrapAgainstARequiredBrokerThenLoginMustChangePassword) {
    const std::string keyFile = writeFile((scratch() / "key").string(), kClusterKey);
    const FakeSetup fresh = [](test_support::InvokerGateway& gw, LiveFake&) {
        gw.clusterKey.assign(kClusterKey.begin(), kClusterKey.end());
    };
    std::unique_ptr<LiveFake> boot;
    const auto r = live(pc::AuthMode::Required, fresh,
                        {"security", "bootstrap", "--admin", "admin", "--cluster-key-file", keyFile}, &boot);
    ASSERT_EQ(r.code, 0) << r.err;
    // Exactly one line: the one-time password, 24 alphanumerics, on stdout only.
    const std::string prefix = "one-time password for admin: ";
    ASSERT_EQ(r.out.rfind(prefix, 0), 0u) << r.out;
    const std::string pw = r.out.substr(prefix.size(), r.out.size() - prefix.size() - 1);
    EXPECT_EQ(pw.size(), 24u);
    EXPECT_EQ(std::count(r.out.begin(), r.out.end(), '\n'), 1);
    EXPECT_EQ(r.err.find(pw), std::string::npos) << "the password is printed once, on stdout only";
    EXPECT_EQ(boot->bootstraps, 1);
    EXPECT_TRUE(boot->state.bootstrapped);
    EXPECT_EQ(boot->state.mustChange.count("admin"), 1u) << "the admin must change the one-time password first";
    ASSERT_EQ(boot->state.users.count("admin"), 1u);
    {   // the broker stored a verifier of THAT password — and never the password
        const auto& v = boot->state.users["admin"][0];
        auto sp = rune::crypto::scram::saltPassword(rune::crypto::Digest::Sha512, pw, v.salt, v.iterations);
        ASSERT_TRUE(sp.has_value());
        const std::string am = "n=admin,r=a,r=ab,s=x,i=4096,c=biws,r=ab";
        auto proof = rune::crypto::scram::clientProof(rune::crypto::Digest::Sha512, *sp, am);
        EXPECT_TRUE(rune::crypto::scram::verifyClientProof(rune::crypto::Digest::Sha512, v.storedKey, am, proof->view()));
    }

    // (2) log in with the one-time password: every request but the password change answers 47 ...
    const std::string otp = writeFile((scratch() / "otp").string(), pw + "\n");
    const FakeSetup after = [&boot](test_support::InvokerGateway& gw, LiveFake&) {
        gw.users = boot->state.users; gw.mustChange = boot->state.mustChange; gw.bootstrapped = true;
    };
    const auto blocked = live(pc::AuthMode::Required, after,
                              {"ql", "--user", "admin", "--password-file", otp, "-e", "create role r;"});
    EXPECT_EQ(blocked.code, 1);
    EXPECT_NE(blocked.err.find("PasswordChangeRequired"), std::string::npos) << blocked.err;
    // ... and the change itself works (the statement comes from a FILE: a password literal is never on argv).
    const std::string script = writeFile((scratch() / "change.rql").string(),
                                         "alter user admin set password 'brand new password 99';\n");
    const auto changed = live(pc::AuthMode::Required, after,
                              {"ql", "--user", "admin", "--password-file", otp, "-f", script});
    EXPECT_EQ(changed.code, 0) << changed.err;
    EXPECT_NE(changed.out.find("ok: ALTER USER admin SET PASSWORD '<redacted>';"), std::string::npos) << changed.out;
    EXPECT_EQ(changed.out.find("brand new"), std::string::npos);
}

TEST(RuneCliSecurityLiveRegression, BootstrapTwiceIsAnExplicitRefusal) {
    const std::string keyFile = writeFile((scratch() / "key2").string(), kClusterKey);
    const FakeSetup bootstrapped = [](test_support::InvokerGateway& gw, LiveFake&) {
        gw.clusterKey.assign(kClusterKey.begin(), kClusterKey.end());
        gw.addUser("admin", "an existing admin password");   // the cluster HAS a security admin
    };
    const auto r = live(pc::AuthMode::Required, bootstrapped,
                        {"security", "bootstrap", "--admin", "admin2", "--cluster-key-file", keyFile});
    EXPECT_EQ(r.code, 1);
    EXPECT_NE(r.err.find("already has a security admin"), std::string::npos) << r.err;
    EXPECT_NE(r.err.find("already bootstrapped"), std::string::npos) << r.err;   // the docker e2e (Task 20) greps for it
    EXPECT_EQ(r.err.find("code 44"), std::string::npos) << "never the bare error code";
    EXPECT_TRUE(r.out.empty()) << "no one-time password is printed for a refused bootstrap: " << r.out;
}

TEST(RuneCliSecurityLiveRegression, BootstrapWithAWrongKeyFileIsRefused) {
    const std::string wrongKey = writeFile((scratch() / "wrongkey").string(), std::string(64, 'b'));
    const FakeSetup fresh = [](test_support::InvokerGateway& gw, LiveFake&) {
        gw.clusterKey.assign(kClusterKey.begin(), kClusterKey.end());
    };
    std::unique_ptr<LiveFake> fake;
    const auto r = live(pc::AuthMode::Required, fresh,
                        {"security", "bootstrap", "--admin", "admin", "--cluster-key-file", wrongKey}, &fake);
    EXPECT_EQ(r.code, 1);
    EXPECT_NE(r.err.find("bootstrap refused"), std::string::npos) << r.err;
    EXPECT_NE(r.err.find("cluster key"), std::string::npos) << r.err;
    EXPECT_EQ(fake->bootstraps, 0);
    EXPECT_FALSE(fake->state.bootstrapped);
    EXPECT_TRUE(r.out.empty()) << r.out;
}

TEST(RuneCliSecurityLiveRegression, BootstrapFromAPasswordFileAndTheLengthRule) {
    const std::string keyFile = writeFile((scratch() / "key3").string(), kClusterKey);
    const FakeSetup fresh = [](test_support::InvokerGateway& gw, LiveFake&) {
        gw.clusterKey.assign(kClusterKey.begin(), kClusterKey.end());
    };
    const std::string pwFile = writeFile((scratch() / "otp3").string(), "a-one-time-password-24c\n");
    const auto r2 = live(pc::AuthMode::Required, fresh,
                         {"security", "bootstrap", "--admin", "admin", "--cluster-key-file", keyFile, "--password-file", pwFile});
    ASSERT_EQ(r2.code, 0) << r2.err;
    EXPECT_EQ(r2.out, "bootstrapped admin (password from " + pwFile + ")\n");
    // A one-time password under the minimum length is refused locally.
    const std::string shortFile = writeFile((scratch() / "short").string(), "tooshort\n");
    const auto r3 = live(pc::AuthMode::Required, fresh,
                         {"security", "bootstrap", "--admin", "admin", "--cluster-key-file", keyFile, "--password-file", shortFile});
    EXPECT_EQ(r3.code, 1);
    EXPECT_NE(r3.err.find("password must be at least 12 characters"), std::string::npos) << r3.err;
}

// Review B I7 (the safe form): the statement with a password literal arrives on stdin (-f -); the frame the broker
// sees carries the REDACTED statement and a verifier of the password, never the password.
TEST(RuneCliSecurityLiveRegression, APasswordStatementFromStdinSendsARedactedStatementAndAVerifier) {
    const std::string pwFile = writeFile((scratch() / "pw4").string(), "correct horse battery\n");
    std::unique_ptr<LiveFake> fake;
    const std::string password = "a stdin password 42";
    const auto r = live(pc::AuthMode::Required, ddlOk(),
                        {"ql", "--user", "ws1", "--password-file", pwFile, "-f", "-"}, &fake,
                        "create user svc with password '" + password + "';\n", /*useStdin=*/true);
    ASSERT_EQ(r.code, 0) << r.err;
    std::vector<uint8_t> frame;
    { std::lock_guard<std::mutex> l(fake->m); frame = fake->lastDdlFrame; }
    auto m = pc::SecurityDdl::decode(frame.data(), frame.size());
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->ext.statement, "CREATE USER svc WITH PASSWORD '<redacted>';");
    EXPECT_EQ(std::search(frame.begin(), frame.end(), password.begin(), password.begin() + 8), frame.end());
    const std::vector<uint8_t> salt(m->verifier.salt.begin(), m->verifier.salt.end());
    auto sp = rune::crypto::scram::saltPassword(rune::crypto::Digest::Sha512, password, salt, m->verifier.iterations);
    ASSERT_TRUE(sp.has_value());
    const std::string am = "n=svc,r=a,r=ab,s=x,i=4096,c=biws,r=ab";
    auto proof = rune::crypto::scram::clientProof(rune::crypto::Digest::Sha512, *sp, am);
    rune::crypto::DigestBytes stored; stored.size = 64; std::memcpy(stored.bytes.data(), m->verifier.storedKey.data(), 64);
    EXPECT_TRUE(rune::crypto::scram::verifyClientProof(rune::crypto::Digest::Sha512, stored, am, proof->view()));
}
