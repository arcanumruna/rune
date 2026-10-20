// Phase 2b Part D: the real `rune console` against a real in-process broker (catalog ruling F7).
#include "client/OneBroker.h"
#include "support/AuthRig.h"

#include <rune/cli/Cli.h>
#include <rune/cli/Console.h>   // runLiveOnFdForTest
#include <rune/client/session/SessionMachine.h>

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

using namespace std::chrono_literals;
using rune::node::test::AuthBroker;
using rune::node::test::OneBroker;
namespace fs = std::filesystem;

namespace {
struct Run { int rc; std::string out, err; };

template <class Broker>
Run consoleLive(Broker& b, std::vector<std::string> extra, const std::string& input) {
    std::vector<std::string> args{"console", "--broker", b.endpoint()};
    args.insert(args.end(), extra.begin(), extra.end());
    std::istringstream in(input);
    std::ostringstream out, err;
    const int rc = rune::cli::run(args, in, out, err);
    return {rc, out.str(), err.str()};
}

std::string writeFile(const std::string& name, const std::string& content) {
    static const fs::path dir = [] {
        fs::path p = fs::temp_directory_path() / ("rune_console_test_" + std::to_string(::getpid()));
        fs::create_directories(p);
        return p;
    }();
    const fs::path p = dir / name;
    std::ofstream o(p, std::ios::binary);
    o << content;
    fs::permissions(p, fs::perms::owner_read | fs::perms::owner_write);
    return p.string();
}

constexpr const char* kUserPw = "console-user-pw-1234";

/// A console on a pipe in its own thread (the pipe plays the terminal).
struct PipeConsole {
    int fds[2]{-1, -1};
    std::ostringstream out, err;
    uint64_t pumps = 0;
    int rc = -1;
    std::thread t;
    explicit PipeConsole(std::vector<std::string> args) {
        if (::pipe(fds) != 0) throw std::runtime_error("pipe");
        t = std::thread([this, a = std::move(args)] { rc = rune::cli::console::runLiveOnFdForTest(a, fds[0], out, err, &pumps); });
    }
    void send(const std::string& s) { ASSERT_EQ(::write(fds[1], s.data(), s.size()), static_cast<ssize_t>(s.size())); }
    void finish() {
        if (fds[1] >= 0) { ::close(fds[1]); fds[1] = -1; }
        if (t.joinable()) t.join();
        if (fds[0] >= 0) { ::close(fds[0]); fds[0] = -1; }
    }
    ~PipeConsole() { finish(); }
};
} // namespace

TEST(RuneConsoleBrokerRegression, InteractiveDefinesAltersAndReadsInOneSessionRegression) {
    OneBroker b("console_live");
    const auto r = consoleLive(b, {},
        "topic player { id int64, name string } partitions 8 key id;\n"
        "alter topic player add column level int32;\n"
        "transient topic ticks { id int64 } partitions 1 key id;\n"
        "show history topic player;\n");
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("ok topic player v1 (8 partitions)"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("ok topic player v2"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("ok transient topic ticks v1 (1 partition)"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("CreateTopic"), std::string::npos) << "show history ran in the same session: " << r.out;
    const auto ev = b.catalogEvents();
    ASSERT_GE(ev.size(), 3u);
    EXPECT_EQ(ev[0].event.statement, "topic player { id int64, name string } partitions 8 key id;")
        << "each DDL records its own statement text";
}

TEST(RuneConsoleBrokerRegression, OneSessionForEveryStatementRegression) {
    OneBroker b("console_one");
    const uint64_t before = b.node().testConnectsAccepted();
    const auto r = consoleLive(b, {}, "topic a partitions 1;\ntopic b partitions 1;\ntopic c partitions 1;\n");
    ASSERT_EQ(r.rc, 0) << r.err;
    EXPECT_EQ(b.node().testConnectsAccepted() - before, 1u) << "one connected client for the whole session";
}

TEST(RuneConsoleBrokerRegression, BrokerErrorKeepsTheSessionRegression) {
    OneBroker b("console_err");
    const auto r = consoleLive(b, {}, "topic t partitions 1;\ntopic t partitions 1;\ntopic u partitions 1;\n");
    EXPECT_EQ(r.rc, 0);
    EXPECT_NE(r.err.find("TopicExists (25)"), std::string::npos) << r.err;
    EXPECT_NE(r.out.find("ok topic u (1 partition)"), std::string::npos) << "the session continues: " << r.out;
}

TEST(RuneConsoleBrokerRegression, ScriptStopsAtTheFirstBrokerErrorWithItsCodeRegression) {
    OneBroker b("console_script");
    const std::string f = writeFile("script.rql",
        "topic ev { id int64, a int32 } partitions 1 key id;\n"
        "alter topic ev set compatibility forward;\n"
        "alter topic ev drop column a;\n"          // a DROP under FORWARD: SchemaIncompatible
        "topic never partitions 1;\n");
    const auto r = consoleLive(b, {"-f", f}, "");
    EXPECT_EQ(r.rc, static_cast<int>(rune::protocol::client::ErrorCode::SchemaIncompatible)) << r.err;
    EXPECT_NE(r.err.find(f + ":statement 3: "), std::string::npos) << r.err;
    for (const auto& e : b.catalogEvents()) EXPECT_NE(e.event.objectName, "never") << "nothing after the failure is issued";
}

TEST(RuneConsoleBrokerRegression, ValidateAndExpectedVersionReachTheBrokerRegression) {
    OneBroker b("console_flags");
    const std::string f1 = writeFile("flags1.rql", "topic fv { id int64 } partitions 1 key id;\nalter topic fv add column a int32;\n");
    ASSERT_EQ(consoleLive(b, {"-f", f1, "--validate", "--expected-version", "1"}, "").rc, 0);
    {   // --validate reached the broker: the CreateTopic carries validate = true (and a plain run does not)
        const std::string f3 = writeFile("flags3.rql", "topic plain { id int64 } partitions 1 key id;\n");
        ASSERT_EQ(consoleLive(b, {"-f", f3}, "").rc, 0);
        bool sawFv = false, sawPlain = false;
        for (const auto& e : b.catalogEvents()) {
            const auto* c = std::get_if<rune::catalog::CreateTopicOps>(&e.event.ops);
            if (c == nullptr || !c->schema) continue;
            if (e.event.objectName == "fv") { sawFv = true; EXPECT_TRUE(c->schema->validate); }
            if (e.event.objectName == "plain") { sawPlain = true; EXPECT_FALSE(c->schema->validate); }
        }
        EXPECT_TRUE(sawFv && sawPlain);
    }
    const std::string f2 = writeFile("flags2.rql", "alter topic fv add column b int32;\n");
    const auto r = consoleLive(b, {"-f", f2, "--expected-version", "1"}, "");   // the topic is at v2 now
    EXPECT_EQ(r.rc, static_cast<int>(rune::protocol::client::ErrorCode::SchemaConflict)) << r.err;
}

TEST(RuneConsoleBrokerRegression, IdlePromptKeepsTheSessionAliveRegression) {
    // Review Focus #6 / DD9: an interactive console idle past the session's stale-session window stays connected.
    OneBroker b("console_idle");
    PipeConsole c({"console", "--broker", b.endpoint()});
    c.send("topic idle1 partitions 1;\n");
    std::this_thread::sleep_for(std::chrono::nanoseconds(rune::client::session::SessionConfig{}.responseTimeoutNs) + 2s);
    c.send("topic idle2 partitions 1;\n");
    c.finish();   // EOF: the console leaves
    EXPECT_EQ(c.rc, 0) << c.err.str();
    EXPECT_NE(c.out.str().find("ok topic idle2"), std::string::npos) << c.err.str();
    EXPECT_GT(c.pumps, 10u) << "the client was pumped while the prompt waited";
}

TEST(RuneConsoleBrokerRegression, LoginWithAPasswordFileAndNotAuthorizedKeepsTheSessionRegression) {
    using namespace rune::node::test::grants;
    AuthBroker b("console_login");
    b.bootstrapAdmin();
    b.createUser("u", kUserPw, {prefix(SO::Create, ST::Topic, "ok_")});
    const std::string pw = writeFile("u.pw", std::string(kUserPw) + "\n");
    const auto r = consoleLive(b, {"--user", "u", "--password-file", pw},
                               "topic ok_a partitions 1;\ntopic other partitions 1;\ntopic ok_b partitions 1;\n");
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("ok topic ok_a"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("ok topic ok_b"), std::string::npos) << "the session continues after NotAuthorized: " << r.out;
    EXPECT_NE(r.err.find("NotAuthorized (45)"), std::string::npos) << r.err;
    EXPECT_EQ(r.out.find("ok topic other"), std::string::npos) << r.out;

    // A user without a password file and without a terminal is a usage error, and nothing connects.
    const uint64_t before = b.node().testConnectsAccepted();
    const auto u = consoleLive(b, {"--user", "u"}, "topic ok_c partitions 1;\n");
    EXPECT_EQ(u.rc, 2);
    EXPECT_NE(u.err.find("--password-file"), std::string::npos) << u.err;
    EXPECT_EQ(b.node().testConnectsAccepted(), before) << "no connect";
}

TEST(RuneConsoleBrokerRegression, CatalogReadRefusalSetsTheScriptExitCodeRegression) {
    // Carried finding (Task 2 review): a catalog READ refused by the broker carries its code to QlRun::lastErrorCode, so
    // `-f` exits with it (here NotAuthorized, 45) instead of the generic 1.
    AuthBroker b("console_readcode");
    b.bootstrapAdmin();
    b.createUser("nobody", kUserPw, {});
    const std::string pw = writeFile("nobody.pw", std::string(kUserPw) + "\n");
    const std::string f = writeFile("history.rql", "show history;\n");
    const auto r = consoleLive(b, {"-f", f, "--user", "nobody", "--password-file", pw}, "");
    EXPECT_EQ(r.rc, static_cast<int>(rune::protocol::client::ErrorCode::NotAuthorized)) << r.err;
}

TEST(RuneConsoleBrokerRegression, SessionRevokedEndsTheConsoleRegression) {
    // Auth D26 / DD11: the admin disables the user between two input lines; the next statement fails SessionRevoked.
    using namespace rune::node::test::grants;
    AuthBroker b("console_revoked");
    b.bootstrapAdmin();
    b.createUser("u", kUserPw, {prefix(SO::Create, ST::Topic, "ok_")});
    const std::string pw = writeFile("revoked.pw", std::string(kUserPw) + "\n");
    PipeConsole c({"console", "--broker", b.endpoint(), "--user", "u", "--password-file", pw});
    c.send("topic ok_a partitions 1;\n");
    std::this_thread::sleep_for(500ms);
    b.disableUser("u");
    std::this_thread::sleep_for(1500ms);   // the console pumps while idle and learns the session ended
    c.send("topic ok_b partitions 1;\ntopic ok_c partitions 1;\n");
    c.finish();
    EXPECT_EQ(c.rc, 46) << c.err.str();
    EXPECT_NE(c.err.str().find("the session ended"), std::string::npos) << c.err.str();
    EXPECT_NE(c.out.str().find("ok topic ok_a"), std::string::npos) << c.out.str();
    EXPECT_EQ(c.out.str().find("ok topic ok_c"), std::string::npos) << "no statement after the revoke ran";
}
