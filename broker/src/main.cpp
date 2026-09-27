// rune-broker main entrypoint — BR-2
//
// Reads NodeConfig from environment variables; constructs a BrokerNode;
// starts it; waits for self-election (1-node) or runs until signaled;
// handles SIGINT/SIGTERM for clean shutdown.

#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>

#include <rune_fr/FlightRecorder.h>

#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// Signal handling (global flag; safe from signal context per POSIX)
// ---------------------------------------------------------------------------
static std::atomic<bool> g_stop{false};
static std::atomic<bool> g_frDump{false};   // SIGUSR1 → dump flight-recorder report mid-run

static void sighandler(int /*sig*/) noexcept {
    g_stop.store(true, std::memory_order_release);
}

static void frdumphandler(int /*sig*/) noexcept {
    g_frDump.store(true, std::memory_order_release);
}

// ---------------------------------------------------------------------------
// rune-fr — flight recorder init + report helpers.
// ---------------------------------------------------------------------------
// Per-thread ring size: env RUNE_FR_RING_BYTES, default 128 MiB/thread (~5.6M
// 24-byte records) so a full bench run is captured without drop-on-full. MUST be
// set BEFORE any broker thread registers a ring (i.e. before node.start()).
static void frConfigure() {
    std::uint64_t ringBytes = 128ull * 1024 * 1024;
    if (const char* rb = std::getenv("RUNE_FR_RING_BYTES")) {
        const std::uint64_t v = std::strtoull(rb, nullptr, 10);
        if (v > 0) ringBytes = v;
    }
    rune_fr::Recorder::instance().configure(static_cast<size_t>(ringBytes));

    if (std::getenv("RUNE_FR_ENABLE")) {
        rune_fr::enable();
        spdlog::info("rune-fr: flight recording ENABLED (ring {} MiB/thread)",
                     ringBytes / (1024 * 1024));
    }
}

// Drain all thread rings and write histogram + Chrome-trace + raw tape to
// RUNE_FR_OUT (default cwd). Off the hot path (shutdown / SIGUSR1).
static void frReport(const std::string& tag) {
    if (!rune_fr::isEnabled()) return;
    rune_fr::Recorder::instance().drain();
    const char* out = std::getenv("RUNE_FR_OUT");
    const std::string dir = (out && *out) ? out : ".";
    {
        std::ofstream f(dir + "/fr-hist-" + tag + ".txt");
        if (f) rune_fr::dumpHistograms(f);
    }
    rune_fr::exportChromeTrace(dir + "/fr-trace-" + tag + ".json");
    rune_fr::writeTape(dir + "/fr-tape-" + tag + ".bin");
    spdlog::info("rune-fr: report written to {} (tag={})", dir, tag);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main() {
    // Install signal handlers before anything else.
    std::signal(SIGINT,  sighandler);
    std::signal(SIGTERM, sighandler);
    std::signal(SIGUSR1, frdumphandler);   // dump flight-recorder report without stopping

    // TEMP DEBUG (CB-5 diagnosis): honor RUNE_LOG_LEVEL=debug/trace so the
    // gateway's redirect/accept decisions are visible during E2E debugging.
    if (const char* lvl = std::getenv("RUNE_LOG_LEVEL")) {
        spdlog::set_level(spdlog::level::from_str(lvl));
    }

    // Parse configuration from environment.
    auto [cfg, err] = rune::node::parseNodeConfigFromEnv();
    if (!cfg.has_value()) {
        std::cerr << "rune-broker: configuration error: " << err << "\n";
        return 2;
    }

    spdlog::info("rune-broker: node {} starting, dataDir={}",
                 cfg->nodeId, cfg->dataDir);
    if (!cfg->peers.empty()) {
        const auto& self = cfg->self();
        spdlog::info("rune-broker: consensus endpoint = {}:{}",
                     self.host, self.consensusPort);
    }

    // Flight recorder: size per-thread rings + arm recording BEFORE start()
    // spawns the duty-cycle/worker threads that register their rings.
    frConfigure();

    // Construct and start the broker node.
    rune::node::BrokerNode node{std::move(*cfg)};
    try {
        node.start();
    } catch (const std::exception& ex) {
        spdlog::error("rune-broker: failed to start: {}", ex.what());
        return 1;
    }

    // For a 1-node cluster, await self-election.
    if (node.awaitLeader(5s)) {
        spdlog::info("rune-broker: node {} is LEADER term {}",
                     node.leaderId(), node.leadershipTermId());
    } else {
        spdlog::warn("rune-broker: leadership not reached within 5 s — continuing");
    }

    // Run until SIGINT / SIGTERM.
    //
    // Observability (CB-5): main.cpp previously logged "is LEADER" exactly once
    // at startup, which masked re-election after a leader-kill.  Poll the
    // observed leaderId and log every leadership change so failover is visible in
    // the container logs (e.g. a survivor electing a new leader after the old
    // leader dies).  The E2E scans these lines to confirm post-kill recovery.
    //
    // DR-5 fix: include leadershipTermId() in each announcement so the oracle
    // can filter out stale pre-election announcements from a healed ex-leader.
    spdlog::info("rune-broker: running — send SIGINT or SIGTERM to stop");
    rune::consensus::NodeId lastObservedLeader = node.leaderId();
    unsigned frDumpSeq = 0;
    while (!g_stop.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(100ms);
        if (g_frDump.exchange(false, std::memory_order_acq_rel)) {
            frReport("run" + std::to_string(frDumpSeq++));
        }
        // Publish the machine-readable leadership signal (Aeron node-role +
        // leadership-term counters).  The dr-e2e harness reads these via
        // rune_role instead of scraping the "is LEADER" log lines below — the
        // counters are authoritative and log-level independent.
        node.publishRole();
        const rune::consensus::NodeId cur = node.leaderId();
        if (cur != lastObservedLeader) {
            lastObservedLeader = cur;
            if (cur >= 0) {
                spdlog::info("rune-broker: leadership change — node {} is LEADER term {}",
                             cur, node.leadershipTermId());
            } else {
                spdlog::info("rune-broker: leadership change — no leader (election in progress)");
            }
        }
    }

    spdlog::info("rune-broker: shutdown signal received — stopping");
    node.stop();
    // Threads are joined + quiesced now → drain rings and write the final report.
    frReport("shutdown");
    spdlog::info("rune-broker: stopped");
    return 0;
}
