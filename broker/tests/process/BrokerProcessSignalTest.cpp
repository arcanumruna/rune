// BrokerProcessSignalTest.cpp — F9a fix round 1: REAL process-directed signal
// delivery to the rune_broker binary.  main() blocks SIGINT/SIGTERM/SIGUSR1/
// SIGUSR2 in every thread and a sigwait thread turns them into control-ring
// events; this spawns the actual binary, sends kill(pid, sig) and expects a
// clean stop with exit status 0 (never a default-action death, never a hang).
#include "SpawnBroker.h"

#include <gtest/gtest.h>

#include <csignal>
#include <filesystem>
#include <string>
#include <thread>

#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using namespace rune::processtest;

namespace {

// Spawn rune_broker (single node).  ignoreSigint: start it the way a shell
// starts a background job — SIGINT inherited as SIG_IGN across exec.
Broker spawnBroker(int slot, bool ignoreSigint) {
    SpawnOptions o;
    o.ignoreSigint = ignoreSigint;
    // ISR design v3 Step 5: a fresh data dir is formatted explicitly (unset refuses to start, exit 78).
    o.extraEnv.push_back("RUNE_STORAGE_FORMAT=new");
    return rune::processtest::spawnBroker("rune_sigproc", slot, o);
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
