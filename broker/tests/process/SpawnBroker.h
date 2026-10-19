#pragma once
// SpawnBroker.h — spawn the REAL rune_broker binary for the process-level tests (moved out of
// BrokerProcessSignalTest.cpp for StorageFormatProcessTest, ISR design v3 Step 5).
#include <rune/aeronutil/testing/FreePort.h>

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

namespace rune::processtest {

namespace fs = std::filesystem;
using namespace std::chrono_literals;

struct Broker {
    pid_t       pid = -1;
    fs::path    dir;       // the test's scratch dir (holds data/, aeron/ and broker.log)
    std::string logPath;   // stdout + stderr

    [[nodiscard]] fs::path dataDir() const { return dir / "data"; }

    std::string log() const {
        std::ifstream f(logPath);
        std::stringstream ss;
        ss << f.rdbuf();
        return ss.str();
    }
};

/// The scratch dir of (prefix, slot), wiped.
inline fs::path scratchDir(const std::string& prefix, int slot) {
    const fs::path d = fs::temp_directory_path() /
                       (prefix + "_" + std::to_string(::getpid()) + "_" + std::to_string(slot));
    fs::remove_all(d);
    fs::create_directories(d);
    return d;
}

struct SpawnOptions {
    bool                     ignoreSigint = false;   // start it like a shell background job (SIGINT = SIG_IGN)
    std::vector<std::string> extraEnv;               // "KEY=value", appended after the base env
    std::vector<std::string> args;                   // argv[1..]
    bool                     brokerEnv = true;       // false: no RUNE_* at all (the CLI path)
    bool                     wipeDir = true;         // false: reuse an existing scratch dir (a restart)
};

// Spawn rune_broker (single node) in `dir`.
inline Broker spawnBrokerIn(const fs::path& dir, int logSeq, const SpawnOptions& o) {
    Broker b;
    b.dir = dir;
    b.logPath = (b.dir / ("broker" + std::to_string(logSeq) + ".log")).string();
    std::vector<std::string> env;
    for (char** e = environ; *e; ++e) {
        if (std::string(*e).rfind("RUNE_", 0) == 0) continue;   // isolate from the caller
        env.emplace_back(*e);
    }
    if (o.brokerEnv) {
        // Every port the broker binds is explicit and owned by this process (FreePort.h: flock-owned port
        // blocks), so concurrently running test processes never collide: consensus, client, replication and
        // the 1-worker shard bases (ingress / repl / egress control), advertised in RUNE_PEERS
        // (id@host:cons:client:repl:replBase:ingressBase:egressControlBase) so nothing is derived from the
        // node id (fixed 41000+ ports).
        auto port = [] { return std::to_string(rune::aeronutil::testing::allocFreeUdpPort()); };
        const std::string cons = port(), client = port(), repl = port(), rb = port(), ib = port(), eg = port();
        env.push_back("RUNE_NODE_ID=0");
        env.push_back("RUNE_PEERS=0@127.0.0.1:" + cons + ":" + client + ":" + repl + ":" + rb + ":" + ib + ":" + eg);
        env.push_back("RUNE_CLIENT_ENDPOINT=aeron:udp?endpoint=127.0.0.1:" + client);
        env.push_back("RUNE_WORKER_INGRESS_BASE_PORT=" + ib);
        env.push_back("RUNE_WORKER_REPL_BASE_PORT=" + rb);
        env.push_back("RUNE_WORKER_EGRESS_CONTROL_BASE_PORT=" + eg);
        env.push_back("RUNE_DATA_DIR=" + b.dataDir().string());
        env.push_back("RUNE_AERON_DIR=" + (b.dir / "aeron").string());
    }
    for (const auto& e : o.extraEnv) env.push_back(e);
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
    if (o.ignoreSigint) {
        struct sigaction ign{};
        ign.sa_handler = SIG_IGN;
        sigemptyset(&ign.sa_mask);
        ::sigaction(SIGINT, &ign, &oldInt);           // inherited across exec
    }
    std::vector<std::string> argStore;
    argStore.emplace_back(RUNE_BROKER_BIN);
    for (const auto& a : o.args) argStore.push_back(a);
    std::vector<char*> argv;
    for (auto& a : argStore) argv.push_back(a.data());
    argv.push_back(nullptr);
    const int rc = posix_spawn(&b.pid, RUNE_BROKER_BIN, &fa, &at, argv.data(), envp.data());
    if (o.ignoreSigint) ::sigaction(SIGINT, &oldInt, nullptr);
    posix_spawn_file_actions_destroy(&fa);
    posix_spawnattr_destroy(&at);
    if (rc != 0) b.pid = -1;
    return b;
}

inline Broker spawnBroker(const std::string& prefix, int slot, const SpawnOptions& o) {
    return spawnBrokerIn(scratchDir(prefix, slot), 0, o);
}

inline bool awaitLog(const Broker& b, const std::string& needle, std::chrono::seconds budget) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline) {
        if (b.log().find(needle) != std::string::npos) return true;
        int st = 0;
        if (::waitpid(b.pid, &st, WNOHANG) == b.pid) return b.log().find(needle) != std::string::npos;   // died early
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

inline bool awaitRunning(const Broker& b, std::chrono::seconds budget) {
    return awaitLog(b, "rune-broker: running", budget);
}

// Wait for exit; on timeout SIGKILL (so a hang fails instead of wedging ctest).
// Returns the raw wait status, or -1 on a hang.
inline int awaitExit(Broker& b, std::chrono::seconds budget) {
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

} // namespace rune::processtest
