// Authentication Task 8 — the rune CLI against the InvokerGateway's SCRAM-speaking broker model: the global
// credential flags (--user / --password-file), the dev-mode banner, `rune ql` security statements and
// `rune security bootstrap` end to end.  The CLI runs on the test thread (its own Invoker client); the fake broker is
// pumped on a helper thread that attaches to the CLI's embedded driver once its directory appears.
#include "support/InvokerGateway.h"

#include "rune/cli/Cli.h"

#include <rune/crypto/Kdf.h>
#include <rune/protocol/client/SecurityMessages.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
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

/// A fake broker on a helper thread.  `setup` runs on that thread right after the gateway exists (install handlers).
struct LiveFake {
    std::string dir;
    int32_t in, out;
    std::atomic<bool> stop{false};
    std::atomic<bool> ready{false};
    std::thread t;
    std::mutex m;
    std::vector<std::vector<uint8_t>> securityDdls, bootstraps;
    std::atomic<int> authOk{0}, authFailed{0};
    pc::AuthMode mode = pc::AuthMode::InProcess;

    LiveFake(int32_t i, pc::AuthMode md, const std::function<void(test_support::InvokerGateway&, LiveFake&)>& setup)
        : in(i), out(i + 1), mode(md) {
        dir = "/tmp/cli_sec_live_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr.fetch_add(1));
        fs::create_directories(dir);
        t = std::thread([this, setup] {
            const std::string cnc = dir + "/aeron/cnc.dat";
            for (int i = 0; i < 6000 && !stop.load() && !fs::exists(cnc); ++i) std::this_thread::sleep_for(2ms);
            if (stop.load()) return;
            std::this_thread::sleep_for(50ms);   // let the driver finish writing its cnc file
            test_support::InvokerGateway gw(dir + "/aeron", "aeron:ipc", in, "aeron:ipc", out);
            gw.authMode = mode;
            gw.addUser("ws1", "correct horse battery");
            setup(gw, *this);
            ready.store(true);
            while (!stop.load()) { if (gw.pump() == 0) std::this_thread::yield(); }
            authOk = gw.authOk; authFailed = gw.authFailed;
        });
    }
    ~LiveFake() { stop.store(true); if (t.joinable()) t.join(); std::error_code ec; fs::remove_all(dir, ec); }
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
using Setup = std::function<void(test_support::InvokerGateway&, LiveFake&)>;
int32_t g_stream = 3900;
/// One CLI invocation against its OWN fake broker (a CLI run closes its embedded driver on exit).
CliRun live(pc::AuthMode mode, const Setup& setup, const std::vector<std::string>& args, LiveFake** keep = nullptr,
            std::unique_ptr<LiveFake>* hold = nullptr) {
    auto fake = std::make_unique<LiveFake>(g_stream, mode, setup);
    g_stream += 2;
    const CliRun r = runCli(cat(args, fake->baseArgs()));
    if (keep) *keep = fake.get();
    if (hold) *hold = std::move(fake);
    return r;
}
} // namespace

TEST(RuneCliSecurityLiveRegression, LoginFlagsAuthenticateAndSecurityDdlRunsRedacted) {
    std::atomic<int> ddls{0};
    const std::string pwFile = writeFile((scratch() / "pw1").string(), "correct horse battery\n");   // trailing newline: stripped
    const auto r = live(pc::AuthMode::Required,
        [&](test_support::InvokerGateway& gw, LiveFake&) {
            gw.templateHandlers[pc::SecurityDdl::TEMPLATE_ID] = [&gw, &ddls](int, const uint8_t* b, std::size_t n) {
                auto m = pc::SecurityDdl::decode(b, n);
                if (!m) return;
                ++ddls;
                pc::DdlResponse d; d.sessionId = m->sessionId; d.correlationId = m->correlationId; d.objectId = 5; d.newVersion = 1;
                gw.sendDdlResponse(0, d);
            };
        },
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
    const auto r = live(pc::AuthMode::Required, [](test_support::InvokerGateway&, LiveFake&) {},
                        {"ql", "--user", "ws1", "--password-file", pwFile, "-e", "create role r;"});
    EXPECT_EQ(r.code, 1);
    EXPECT_NE(r.err.find("AuthenticationFailed"), std::string::npos) << r.err;
    EXPECT_EQ(r.err.find("not the right"), std::string::npos) << "the password is never echoed";
    // No credentials at all: the same failure (the broker enforces).
    const auto none = live(pc::AuthMode::Required, [](test_support::InvokerGateway&, LiveFake&) {},
                           {"ql", "-e", "create role r;"});
    EXPECT_EQ(none.code, 1);
    EXPECT_NE(none.err.find("AuthenticationFailed"), std::string::npos) << none.err;
}

TEST(RuneCliSecurityLiveRegression, DevModePrintsTheBannerOnStderr) {
    const auto r = live(pc::AuthMode::DevDisabled,
        [](test_support::InvokerGateway& gw, LiveFake&) {
            gw.templateHandlers[pc::SecurityDdl::TEMPLATE_ID] = [&gw](int, const uint8_t* b, std::size_t n) {
                auto m = pc::SecurityDdl::decode(b, n);
                if (!m) return;
                pc::DdlResponse d; d.sessionId = m->sessionId; d.correlationId = m->correlationId; d.objectId = 1; d.newVersion = 1;
                gw.sendDdlResponse(0, d);
            };
        },
        {"ql", "-e", "create role r;"});
    ASSERT_EQ(r.code, 0) << r.err;
    const std::string banner = "WARNING: this broker runs with authentication DISABLED (RUNE_AUTH=disabled, dev mode)";
    const auto at = r.err.find(banner);
    ASSERT_NE(at, std::string::npos) << r.err;
    EXPECT_EQ(r.err.find(banner, at + 1), std::string::npos) << "exactly once";
}

TEST(RuneCliSecurityLiveRegression, BootstrapPrintsTheOneTimePasswordOnceAndProvesTheClusterKey) {
    const std::vector<uint8_t> keyBytes(64, 'a');
    const std::string keyFile = writeFile((scratch() / "key").string(), std::string(64, 'a'));
    auto handler = [](test_support::InvokerGateway& gw, LiveFake& f) {
        gw.templateHandlers[pc::SecurityBootstrap::TEMPLATE_ID] = [&gw, &f](int, const uint8_t* b, std::size_t n) {
            auto m = pc::SecurityBootstrap::decode(b, n);
            if (!m) return;
            { std::lock_guard<std::mutex> l(f.m); f.bootstraps.emplace_back(b, b + n); }
            pc::DdlResponse d; d.sessionId = m->sessionId; d.correlationId = m->correlationId; d.objectId = 1; d.newVersion = 1;
            gw.sendDdlResponse(0, d);
        };
    };
    std::unique_ptr<LiveFake> hold;
    const auto r = live(pc::AuthMode::InProcess, handler, {"security", "bootstrap", "--admin", "admin", "--cluster-key-file", keyFile},
                        nullptr, &hold);
    ASSERT_EQ(r.code, 0) << r.err;
    // Exactly one line: the one-time password, 24 alphanumerics.
    const std::string prefix = "one-time password for admin: ";
    ASSERT_EQ(r.out.rfind(prefix, 0), 0u) << r.out;
    const std::string pw = r.out.substr(prefix.size(), r.out.size() - prefix.size() - 1);
    EXPECT_EQ(pw.size(), 24u);
    EXPECT_EQ(r.out.back(), '\n');
    EXPECT_EQ(std::count(r.out.begin(), r.out.end(), '\n'), 1);
    EXPECT_EQ(r.err.find(pw), std::string::npos) << "the password is printed once, on stdout only";
    // The broker saw a MAC that verifies under the key file, and no password on the wire.
    std::vector<uint8_t> frame;
    { std::lock_guard<std::mutex> l(hold->m); ASSERT_EQ(hold->bootstraps.size(), 1u); frame = hold->bootstraps[0]; }
    auto m = pc::SecurityBootstrap::decode(frame.data(), frame.size());
    ASSERT_TRUE(m.has_value());
    EXPECT_EQ(m->adminName, "admin");
    std::vector<uint8_t> input;
    pc::bootstrapMacInput(*m, input);
    const auto key = rune::crypto::deriveClusterKey(keyBytes);
    rune::crypto::DigestBytes mac;
    ASSERT_TRUE(rune::crypto::hmac(rune::crypto::Digest::Sha512, key->master, input, mac));
    EXPECT_EQ(std::memcmp(mac.bytes.data(), m->mac.data(), 64), 0);
    EXPECT_EQ(std::search(frame.begin(), frame.end(), pw.begin(), pw.end()), frame.end()) << "no password on the wire";
    hold.reset();

    // The non-interactive form: the password comes from a file; nothing secret is printed.
    const std::string pwFile = writeFile((scratch() / "otp").string(), "a-one-time-password-24c\n");
    const auto r2 = live(pc::AuthMode::InProcess, handler,
                         {"security", "bootstrap", "--admin", "admin", "--cluster-key-file", keyFile, "--password-file", pwFile});
    ASSERT_EQ(r2.code, 0) << r2.err;
    EXPECT_EQ(r2.out, "bootstrapped admin (password from " + pwFile + ")\n");
    // A one-time password under the minimum length is refused locally.
    const std::string shortFile = writeFile((scratch() / "short").string(), "tooshort\n");
    const auto r3 = live(pc::AuthMode::InProcess, handler,
                         {"security", "bootstrap", "--admin", "admin", "--cluster-key-file", keyFile, "--password-file", shortFile});
    EXPECT_EQ(r3.code, 1);
    EXPECT_NE(r3.err.find("password must be at least 12 characters"), std::string::npos) << r3.err;
}
