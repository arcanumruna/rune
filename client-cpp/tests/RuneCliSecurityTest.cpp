// Authentication Task 8 — `rune security gen-cluster-key | bootstrap`, `rune ql` security statements, the global
// credential flags: OFFLINE (--dry-run, the pure helpers, usage errors).  Nothing here talks to a broker (the CLI
// against real brokers lives in the broker repo's RuneCliSecurityBrokerRegressionTest).
#include "rune/cli/Cli.h"
#include "rune/cli/SecurityCommands.h"

#include <rune/security/testing/SecurityGoldenScript.h>

#include <gtest/gtest.h>

#include <cctype>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace {
struct CliRun { int code; std::string out, err; };
CliRun runCli(std::vector<std::string> args) {
    std::ostringstream out, err;
    const int code = rune::cli::run(std::move(args), out, err);
    return {code, out.str(), err.str()};
}
fs::path tmpPath(const std::string& name) {
    static const fs::path d = [] {
        fs::path p = fs::temp_directory_path() / ("rune_cli_security_test_" + std::to_string(::getpid()));
        fs::create_directories(p);
        return p;
    }();
    return d / name;
}
struct EnvGuard {
    std::string name; std::optional<std::string> old;
    EnvGuard(const char* n, const char* v) : name(n) {
        if (const char* o = std::getenv(n)) old = o;
        if (v) ::setenv(n, v, 1); else ::unsetenv(n);
    }
    ~EnvGuard() { if (old) ::setenv(name.c_str(), old->c_str(), 1); else ::unsetenv(name.c_str()); }
};
} // namespace

TEST(RuneCliSecurityRegression, GenClusterKeyWritesSixtyFourHexCharsAtMode0600) {
    const std::string path = tmpPath("cluster.key").string();
    fs::remove(path);
    const CliRun r = runCli({"security", "gen-cluster-key", "--out", path});
    ASSERT_EQ(r.code, 0) << r.err;
    std::ifstream f(path, std::ios::binary);
    std::stringstream ss; ss << f.rdbuf();
    const std::string key = ss.str();
    ASSERT_EQ(key.size(), 64u);
    for (const char c : key) EXPECT_TRUE(std::isxdigit(static_cast<unsigned char>(c)) && !std::isupper(static_cast<unsigned char>(c))) << c;
    struct stat st{};
    ASSERT_EQ(::stat(path.c_str(), &st), 0);
    EXPECT_EQ(st.st_mode & 0777, 0600u);
    EXPECT_EQ(r.out.find(key), std::string::npos) << "the key itself is never printed";
    // An existing key file is never overwritten.
    const CliRun again = runCli({"security", "gen-cluster-key", "--out", path});
    EXPECT_EQ(again.code, 1);
    std::ifstream f2(path, std::ios::binary);
    std::stringstream ss2; ss2 << f2.rdbuf();
    EXPECT_EQ(ss2.str(), key);
    // Two generated keys differ.
    const std::string path2 = tmpPath("cluster2.key").string();
    fs::remove(path2);
    ASSERT_EQ(runCli({"security", "gen-cluster-key", "--out", path2}).code, 0);
    std::ifstream f3(path2, std::ios::binary);
    std::stringstream ss3; ss3 << f3.rdbuf();
    EXPECT_NE(ss3.str(), key);
    // --out is required.
    EXPECT_EQ(runCli({"security", "gen-cluster-key"}).code, 2);
}

TEST(RuneCliSecurityRegression, PasswordIsNeverAcceptedFromArgvOrEnv) {
    {   // --password secret: a usage error naming the safe alternatives
        const CliRun r = runCli({"ql", "--password", "secret", "--dry-run", "-e", "show users;"});
        EXPECT_EQ(r.code, 2);
        EXPECT_NE(r.err.find("--password is not accepted"), std::string::npos) << r.err;
        EXPECT_NE(r.err.find("--password-file"), std::string::npos);
        EXPECT_EQ(r.err.find("secret"), std::string::npos) << "the refused value is not echoed";
    }
    {
        const CliRun r = runCli({"ql", "--password=secret", "--dry-run", "-e", "show users;"});
        EXPECT_EQ(r.code, 2);
        EXPECT_EQ(r.err.find("secret"), std::string::npos);
    }
    {   // the same for security commands
        const CliRun r = runCli({"security", "bootstrap", "--admin", "a", "--password", "secret"});
        EXPECT_EQ(r.code, 2);
        EXPECT_EQ(r.err.find("secret"), std::string::npos);
    }
    {   // RUNE_PASSWORD is ignored, with a warning
        EnvGuard g("RUNE_PASSWORD", "hunter2-hunter2");
        const CliRun r = runCli({"ql", "--dry-run", "-e", "show users;"});
        EXPECT_EQ(r.code, 0) << r.err;
        EXPECT_NE(r.err.find("RUNE_PASSWORD is ignored"), std::string::npos) << r.err;
        EXPECT_EQ(r.err.find("hunter2"), std::string::npos) << "the value is never printed";
    }
    {   // the global credential flags are accepted
        const CliRun r = runCli({"ql", "--user", "ws1", "--password-file", "/nonexistent", "--dry-run", "-e", "show users;"});
        EXPECT_EQ(r.code, 0) << r.err;
    }
}

namespace {
/// Runs the CLI with `text` on stdin (the `-f -` form).
CliRun runCliWithStdin(std::vector<std::string> args, const std::string& text) {
    std::istringstream in(text);
    auto* old = std::cin.rdbuf(in.rdbuf());
    CliRun r = runCli(std::move(args));
    std::cin.rdbuf(old);
    return r;
}
} // namespace

// Review B I7: a password literal on the command line (-e) is visible to ps and the shell history: refused
// (exit 2, nothing planned or sent, the literal never echoed) for a statement that CARRIES one — even with --dry-run.
TEST(RuneCliSecurityRegression, APasswordLiteralInArgvIsRefusedRegression) {
    for (const bool dry : {true, false}) {
        std::vector<std::string> args = {"ql"};
        if (dry) args.push_back("--dry-run");
        args.insert(args.end(), {"-e", "create user u with password 'long enough pw';"});
        const CliRun r = runCli(args);
        EXPECT_EQ(r.code, 2) << "dry=" << dry;
        EXPECT_NE(r.err.find("visible to ps and shell history"), std::string::npos) << r.err;
        EXPECT_NE(r.err.find("-f -"), std::string::npos) << "the message names the safe forms";
        EXPECT_EQ(r.err.find("long enough"), std::string::npos) << "the literal is never echoed";
        EXPECT_TRUE(r.out.empty()) << "nothing was planned: " << r.out;
    }
    // A statement WITHOUT a password literal is fine on -e.
    EXPECT_EQ(runCli({"ql", "--dry-run", "-e", "alter user u disable;"}).code, 0);
    // The safe form: stdin.
    const CliRun ok = runCliWithStdin({"ql", "--dry-run", "-f", "-"}, "create user u with password 'long enough pw';");
    EXPECT_EQ(ok.code, 0) << ok.err;
    EXPECT_EQ(ok.out, "CREATE USER u WITH PASSWORD '<redacted>';\n");
}

TEST(RuneCliSecurityRegression, QlDryRunPrintsTheRedactedStatement) {
    const CliRun r = runCliWithStdin({"ql", "--dry-run", "-f", "-"}, "create user u with password 'long enough pw';");
    ASSERT_EQ(r.code, 0) << r.err;
    EXPECT_EQ(r.out, "CREATE USER u WITH PASSWORD '<redacted>';\n");
    EXPECT_EQ(r.out.find("long enough"), std::string::npos);
    EXPECT_EQ(r.err.find("long enough"), std::string::npos);
    // A script: every statement planned, none echoing a password.
    const std::string script = tmpPath("sec.rql").string();
    { std::ofstream(script) << "CREATE ROLE world_server;\nGRANT WRITE ON TOPIC PREFIX 'world.' TO ROLE world_server;\n"
                               "ALTER USER ws1 SET PASSWORD 'p4ssword-long-enough' TERMINATE SESSIONS;\nSHOW GRANTS FOR USER ws1;\n"; }
    const CliRun s = runCli({"ql", "--dry-run", "-f", script});
    ASSERT_EQ(s.code, 0) << s.err;
    EXPECT_NE(s.out.find("CREATE ROLE world_server;"), std::string::npos);
    EXPECT_NE(s.out.find("ALTER USER ws1 SET PASSWORD '<redacted>' TERMINATE SESSIONS;"), std::string::npos);
    EXPECT_EQ(s.out.find("p4ssword"), std::string::npos);
    EXPECT_NE(s.out.find("SHOW GRANTS FOR USER ws1"), std::string::npos);
}

TEST(RuneCliSecurityRegression, BootstrapRequiresAClusterKeyFile) {
    EnvGuard g("RUNE_CLUSTER_KEY_FILE", nullptr);
    const CliRun r = runCli({"security", "bootstrap", "--admin", "admin", "--broker", "127.0.0.1:1"});
    EXPECT_EQ(r.code, 2);
    EXPECT_NE(r.err.find("--cluster-key-file"), std::string::npos) << r.err;
    EXPECT_NE(r.err.find("RUNE_CLUSTER_KEY_FILE"), std::string::npos);
    // --admin is required too; an unknown subcommand is a usage error.
    EXPECT_EQ(runCli({"security", "bootstrap", "--cluster-key-file", "/k"}).code, 2);
    EXPECT_EQ(runCli({"security", "nope"}).code, 2);
    EXPECT_EQ(runCli({"security"}).code, 2);
    // An unreadable key file is an explicit error, not a usage error.
    const CliRun bad = runCli({"security", "bootstrap", "--admin", "admin", "--cluster-key-file", "/nonexistent/key", "--broker", "127.0.0.1:1"});
    EXPECT_EQ(bad.code, 1);
    EXPECT_NE(bad.err.find("cannot read"), std::string::npos);
}

TEST(RuneCliSecurityRegression, BootstrapRefusesAGroupReadablePasswordFileRegression) {
    EnvGuard g("RUNE_CLUSTER_KEY_FILE", nullptr);
    const std::string key = tmpPath("pwmode.key").string();
    const std::string pw  = tmpPath("pwmode.pw").string();
    { std::ofstream(key, std::ios::binary) << std::string(64, 'a'); }
    ::chmod(key.c_str(), 0600);
    { std::ofstream(pw, std::ios::binary) << "a-one-time-password-24c\n"; }
    ::chmod(pw.c_str(), 0644);
    const CliRun r = runCli({"security", "bootstrap", "--admin", "a", "--cluster-key-file", key, "--password-file", pw,
                             "--broker", "x:1"});
    EXPECT_EQ(r.code, 1);
    EXPECT_NE(r.err.find("group or others"), std::string::npos) << r.err;
    EXPECT_EQ(r.err.find("a-one-time-password"), std::string::npos) << "the password is never echoed";
}

TEST(RuneCliSecurityRegression, TheOneTimePasswordIs24UnbiasedAlphanumerics) {
    std::set<std::string> seen;
    std::array<int, 128> histogram{};
    for (int i = 0; i < 400; ++i) {
        const std::string pw = rune::cli::generateOneTimePassword();
        ASSERT_EQ(pw.size(), 24u);
        for (const char c : pw) {
            ASSERT_TRUE(std::isalnum(static_cast<unsigned char>(c))) << c;
            ++histogram[static_cast<unsigned char>(c)];
        }
        seen.insert(pw);
    }
    EXPECT_EQ(seen.size(), 400u);
    // Every one of the 62 characters shows up in 9600 draws (expected ~155 each): a stuck / modulo-clipped alphabet would not.
    int distinct = 0;
    for (int c = 0; c < 128; ++c) if (histogram[static_cast<std::size_t>(c)] > 0) ++distinct;
    EXPECT_EQ(distinct, 62);
}

TEST(RuneCliSecurityRegression, ShowTablesHaveTheDocumentedColumns) {
    namespace sec = rune::security;
    const auto script = sec::testing::goldenScriptV1();
    std::vector<sec::RecordedEvent> log;
    for (std::size_t i = 0; i < script.size(); ++i) log.push_back({static_cast<int64_t>(i), script[i]});
    const std::string hist = rune::cli::formatSecurityHistoryTable(log);
    EXPECT_EQ(hist.substr(0, hist.find('\n')).find("offset"), 0u);
    EXPECT_NE(hist.find("time(UTC)"), std::string::npos);
    EXPECT_NE(hist.find("principal"), std::string::npos);
    EXPECT_NE(hist.find("statement"), std::string::npos);
    auto state = sec::stateAsOf(log, sec::AsOf{sec::AsOf::By::Offset, 8});
    ASSERT_TRUE(state.has_value());
    const std::string users = rune::cli::formatUsersTable(sec::users(*state));
    EXPECT_NE(users.find("name"), std::string::npos);
    EXPECT_NE(users.find("must_change"), std::string::npos);
    EXPECT_NE(users.find("ws1"), std::string::npos);
    const std::string roles = rune::cli::formatRolesTable(sec::roles(*state));
    EXPECT_NE(roles.find("world_server"), std::string::npos);
    const auto g = sec::grantsFor(*state, sec::GranteeKind::User, "ws1");
    ASSERT_TRUE(g.has_value());
    const std::string grants = rune::cli::formatGrantsTable(*g);
    EXPECT_EQ(grants.substr(0, grants.find('\n')).find("source"), 0u);
    EXPECT_NE(grants.find("role:world_server"), std::string::npos);
    EXPECT_NE(grants.find("PREFIX 'world.'"), std::string::npos);
    EXPECT_NE(grants.find("WRITE"), std::string::npos);
    EXPECT_EQ(grants.find("direct"), std::string::npos) << "ws1 has no direct grants at offset 8";
    // Verifier bytes are never part of any row.
    EXPECT_EQ(users.find("storedKey"), std::string::npos);
}
