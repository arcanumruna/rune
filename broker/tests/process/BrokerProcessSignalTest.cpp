// BrokerProcessSignalTest.cpp — F9a fix round 1: REAL process-directed signal
// delivery to the rune_broker binary.  main() blocks SIGINT/SIGTERM/SIGUSR1/
// SIGUSR2 in every thread and a sigwait thread turns them into control-ring
// events; this spawns the actual binary, sends kill(pid, sig) and expects a
// clean stop with exit status 0 (never a default-action death, never a hang).
#include <rune/aeronutil/testing/FreePort.h>

#include <gtest/gtest.h>

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

struct Broker {
    pid_t       pid = -1;
    fs::path    dir;
    std::string logPath;

    std::string log() const {
        std::ifstream f(logPath);
        std::stringstream ss;
        ss << f.rdbuf();
        return ss.str();
    }
};

// Spawn rune_broker (single node).  ignoreSigint: start it the way a shell
// starts a background job — SIGINT inherited as SIG_IGN across exec.
Broker spawnBroker(int slot, bool ignoreSigint) {
    Broker b;
    b.dir = fs::temp_directory_path() /
            ("rune_sigproc_" + std::to_string(::getpid()) + "_" + std::to_string(slot));
    fs::remove_all(b.dir);
    fs::create_directories(b.dir);
    b.logPath = (b.dir / "broker.log").string();
    // Every port the broker binds is explicit and owned by this process (FreePort.h: flock-owned port blocks),
    // so concurrently running test processes never collide: consensus, client, replication and the 1-worker
    // shard bases (ingress / repl / egress control), advertised in RUNE_PEERS (id@host:cons:client:repl:
    // replBase:ingressBase:egressControlBase) so nothing is derived from the node id (fixed 41000+ ports).
    auto port = [] { return std::to_string(rune::aeronutil::testing::allocFreeUdpPort()); };
    const std::string cons = port(), client = port(), repl = port(), rb = port(), ib = port(), eg = port();

    std::vector<std::string> env;
    for (char** e = environ; *e; ++e) {
        if (std::string(*e).rfind("RUNE_", 0) == 0) continue;   // isolate from the caller
        env.emplace_back(*e);
    }
    env.push_back("RUNE_NODE_ID=0");
    env.push_back("RUNE_PEERS=0@127.0.0.1:" + cons + ":" + client + ":" + repl + ":" + rb + ":" + ib + ":" + eg);
    env.push_back("RUNE_CLIENT_ENDPOINT=aeron:udp?endpoint=127.0.0.1:" + client);
    env.push_back("RUNE_WORKER_INGRESS_BASE_PORT=" + ib);
    env.push_back("RUNE_WORKER_REPL_BASE_PORT=" + rb);
    env.push_back("RUNE_WORKER_EGRESS_CONTROL_BASE_PORT=" + eg);
    env.push_back("RUNE_DATA_DIR=" + (b.dir / "data").string());
    env.push_back("RUNE_AERON_DIR=" + (b.dir / "aeron").string());
    std::vector<char*> envp;
    for (auto& s : env) envp.push_back(s.data());
    envp.push_back(nullptr);

    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_addopen(&fa, STDOUT_FILENO, b.logPath.c_str(),
                                     O_WRONLY | O_CREAT | O_TRUNC, 0644);
    posix_spawn_file_actions_adddup2(&fa, STDOUT_FILENO, STDERR_FILENO);
    posix_spawnattr_t at;
    posix_spawnattr_init(&at);
    sigset_t empty;
    sigemptyset(&empty);
    posix_spawnattr_setsigmask(&at, &empty);          // child starts unblocked
    posix_spawnattr_setflags(&at, POSIX_SPAWN_SETSIGMASK);

    struct sigaction oldInt{};
    if (ignoreSigint) {
        struct sigaction ign{};
        ign.sa_handler = SIG_IGN;
        sigemptyset(&ign.sa_mask);
        ::sigaction(SIGINT, &ign, &oldInt);           // inherited across exec
    }
    char* argv[] = {const_cast<char*>(RUNE_BROKER_BIN), nullptr};
    const int rc = posix_spawn(&b.pid, RUNE_BROKER_BIN, &fa, &at, argv, envp.data());
    if (ignoreSigint) ::sigaction(SIGINT, &oldInt, nullptr);
    posix_spawn_file_actions_destroy(&fa);
    posix_spawnattr_destroy(&at);
    if (rc != 0) b.pid = -1;
    return b;
}

bool awaitRunning(const Broker& b, std::chrono::seconds budget) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline) {
        if (b.log().find("rune-broker: running") != std::string::npos) return true;
        int st = 0;
        if (::waitpid(b.pid, &st, WNOHANG) == b.pid) return false;   // died early
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

// Wait for exit; on timeout SIGKILL (so a hang fails instead of wedging ctest).
// Returns the raw wait status, or -1 on a hang.
int awaitExit(Broker& b, std::chrono::seconds budget) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline) {
        int st = 0;
        if (::waitpid(b.pid, &st, WNOHANG) == b.pid) { b.pid = -1; return st; }
        std::this_thread::sleep_for(20ms);
    }
    ::kill(b.pid, SIGKILL);
    int st = 0;
    ::waitpid(b.pid, &st, 0);
    b.pid = -1;
    return -1;
}

void expectCleanExitOn(int sig, int slot, bool ignoreSigint = false, bool frDumpFirst = false) {
    Broker b = spawnBroker(slot, ignoreSigint);
    ASSERT_GT(b.pid, 0) << "posix_spawn failed for " << RUNE_BROKER_BIN;
    ASSERT_TRUE(awaitRunning(b, 30s)) << "broker never reached the run loop:\n" << b.log();
    if (frDumpFirst) {
        ASSERT_EQ(::kill(b.pid, SIGUSR1), 0);
        std::this_thread::sleep_for(300ms);
        int st = 0;
        ASSERT_EQ(::waitpid(b.pid, &st, WNOHANG), 0) << "SIGUSR1 must not stop/kill the broker";
    }
    ASSERT_EQ(::kill(b.pid, sig), 0);
    const int st = awaitExit(b, 30s);
    ASSERT_NE(st, -1) << "broker did not exit within 30 s of signal " << sig << ":\n" << b.log();
    ASSERT_TRUE(WIFEXITED(st)) << "broker died by signal " << (WIFSIGNALED(st) ? WTERMSIG(st) : 0)
                               << " instead of a clean stop:\n" << b.log();
    EXPECT_EQ(WEXITSTATUS(st), 0) << b.log();
    EXPECT_NE(b.log().find("rune-broker: stopped"), std::string::npos) << b.log();
    std::error_code ec;
    fs::remove_all(b.dir, ec);
}

} // namespace

TEST(BrokerProcessSignalRegression, ProcessDirectedSigintStopsCleanly) {
    expectCleanExitOn(SIGINT, 0);
}

TEST(BrokerProcessSignalRegression, ProcessDirectedSigtermStopsCleanly) {
    expectCleanExitOn(SIGTERM, 1);
}

// A shell background job inherits SIGINT as SIG_IGN; an ignored signal is
// discarded even while blocked, so main() must reset it before sigwait().
TEST(BrokerProcessSignalRegression, InheritedIgnoredSigintStillStops) {
    expectCleanExitOn(SIGINT, 2, /*ignoreSigint=*/true);
}

// SIGUSR1 (flight-recorder dump) is an event, not a stop.
TEST(BrokerProcessSignalRegression, Sigusr1DoesNotStopThenSigtermDoes) {
    expectCleanExitOn(SIGTERM, 3, /*ignoreSigint=*/false, /*frDumpFirst=*/true);
}
