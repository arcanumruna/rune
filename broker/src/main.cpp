// rune-broker main entrypoint — BR-2
//
// Reads NodeConfig from environment variables; constructs a BrokerNode;
// starts it; waits for self-election (1-node) or runs until signaled;
// handles SIGINT/SIGTERM for clean shutdown.

#include <rune/node/BrokerNode.h>
#include <rune/node/FdBudget.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/SignalControl.h>
#include <rune/node/ThreadAffinity.h>

#include <rune_fr/FlightRecorder.h>

#include <spdlog/spdlog.h>

#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// Control events (F9a: no globals, no atomics, no async signal handlers).
//
// SIGINT/SIGTERM/SIGUSR1 are BLOCKED in every thread (masked first thing in
// main, inherited by every thread created afterwards); a dedicated SignalThread
// sigwait()s them and posts Stop / FrDump events on `events` (a ControlRing).
// The broker's fail-stop halt (runtime thread) posts Halt{exitCode} on the same
// ring.  main() is the ring's one consumer.
// ---------------------------------------------------------------------------

// fsyncgate fail-stop: BrokerNode calls this ONCE (runtime thread, or main
// during start()) when a log's fsync failed.  The main loop stops the node
// through the normal stop path, then exits with the code (74, EX_IOERR).
static void haltHandler(void* ctx, int exitCode) noexcept {
    auto* events = static_cast<rune::node::ControlEventRing*>(ctx);
    if (!rune::node::postHalt(*events, exitCode)) {
        spdlog::critical("rune-broker: control ring full — halt {} could not be posted; aborting",
                         exitCode);
        std::_Exit(exitCode);   // fail-stop must never be lost
    }
}

// main-thread state fed by drainControlEvents (owned by main).
struct ControlState {
    bool stop     = false;
    int  exitCode = 0;   // non-zero: halt requested by the broker (fail-stop)
    int  frDumps  = 0;   // pending SIGUSR1 report requests
};

static void drainControlEvents(rune::node::ControlEventRing& events, ControlState& st) {
    events.poll([&st](std::int32_t type, std::int64_t a, std::int64_t) {
        switch (type) {
            case rune::node::kStopEvent:   st.stop = true; break;
            case rune::node::kFrDumpEvent: ++st.frDumps; break;
            case rune::node::kHaltEvent:
                if (st.exitCode == 0) st.exitCode = static_cast<int>(a);
                st.stop = true;
                break;
            default: break;
        }
    });
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
    // Disambiguate per broker so co-located cluster nodes sharing one RUNE_FR_OUT
    // dir do not overwrite each other's reports: suffix with node id + pid.
    const char* nid = std::getenv("RUNE_NODE_ID");
    const std::string sfx = "-n" + std::string(nid ? nid : "x") +
                            "-p" + std::to_string(static_cast<long>(::getpid())) +
                            "-" + tag;
    {
        std::ofstream f(dir + "/fr-hist" + sfx + ".txt");
        if (f) rune_fr::dumpHistograms(f);
    }
    rune_fr::exportChromeTrace(dir + "/fr-trace" + sfx + ".json");
    rune_fr::writeTape(dir + "/fr-tape" + sfx + ".bin");
    spdlog::info("rune-fr: report written to {} (suffix={})", dir, sfx);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main() {
    // F9a: block the control signals BEFORE any thread exists (every thread
    // inherits the mask), then hand them to the one sigwait thread.
    //
    // WARNING: blockControlSignals() MUST stay the first thing main() does.  Any
    // thread created BEFORE it (or any thread that later unblocks SIGINT/SIGTERM/
    // SIGUSR1/SIGUSR2) can receive a process-directed signal with the default
    // action — killing the broker without a clean stop — or swallow it, so the
    // signal thread never sees it and shutdown breaks.
    if (const int rc = rune::node::blockControlSignals(); rc != 0) {
        std::cerr << "rune-broker: pthread_sigmask failed: " << rc << "\n";
        return 2;
    }
    rune::node::ControlEventRing events;          // → main (sole consumer)
    rune::node::SignalThread     signalThread{events};
    ControlState                 ctl;

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

    // FD-1: raise the soft open-file limit to the hard limit (logged), and
    // refuse to start when the hard limit cannot hold the configured
    // partitions (a mid-run EMFILE would fail-stop logs and halt the broker).
    {
        std::string fdErr;
        if (!rune::node::applyFdLimitAtStartup(*cfg, fdErr)) {
            spdlog::error("rune-broker: {}", fdErr);
            std::cerr << "rune-broker: " << fdErr << "\n";
            return 2;
        }
    }

    // Flight recorder: size per-thread rings + arm recording BEFORE start()
    // spawns the duty-cycle/worker threads that register their rings.
    frConfigure();

    // Construct and start the broker node.
    rune::node::BrokerNode node{std::move(*cfg)};
    node.setHaltHandler(&haltHandler, &events);
    try {
        node.start();
    } catch (const std::exception& ex) {
        spdlog::error("rune-broker: failed to start: {}", ex.what());
        // A storage failure found during startup (failed log recovery) exits
        // through the halt path's code (74, EX_IOERR).
        drainControlEvents(events, ctl);
        return ctl.exitCode != 0 ? ctl.exitCode : 1;
    }

    // D2: the main + signal threads predate start()'s non-worker pin — move them off the worker cores too.
    if (const auto& cpus = node.nonWorkerCpus(); !cpus.empty()) {
        (void)rune::node::pinCurrentThreadToCpus(cpus);
        (void)rune::node::pinThreadToCpus(signalThread.nativeHandle(), cpus);
    }

    // For a 1-node cluster, await self-election.
    if (node.awaitLeader(5s)) {
        const auto v = node.leaderSnapshot();   // ONE snapshot: id + term never torn
        spdlog::info("rune-broker: node {} is LEADER term {}", v.leaderId, v.term);
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
    for (;;) {
        drainControlEvents(events, ctl);
        if (ctl.stop) break;
        std::this_thread::sleep_for(100ms);
        drainControlEvents(events, ctl);
        if (ctl.stop) break;
        if (ctl.frDumps > 0) {
            ctl.frDumps = 0;
            frReport("run" + std::to_string(frDumpSeq++));
        }
        // Publish the machine-readable leadership signal (Aeron node-role +
        // leadership-term counters).  The dr-e2e harness reads these via
        // rune_role instead of scraping the "is LEADER" log lines below — the
        // counters are authoritative and log-level independent.
        node.publishRole();
        const auto v = node.leaderSnapshot();   // ONE snapshot: id + term never torn
        const rune::consensus::NodeId cur = v.leaderId;
        if (cur != lastObservedLeader) {
            lastObservedLeader = cur;
            if (cur >= 0) {
                spdlog::info("rune-broker: leadership change — node {} is LEADER term {}",
                             cur, v.term);
            } else {
                spdlog::info("rune-broker: leadership change — no leader (election in progress)");
            }
        }
    }

    const int exitCode = ctl.exitCode;
    if (exitCode != 0) {
        // Fail-stop: never a hand-off (the storage is suspect) — stop at once.
        spdlog::critical("rune-broker: HALTING on storage failure — stopping (exit {})", exitCode);
    } else {
        spdlog::info("rune-broker: shutdown signal received — stopping");
        // Controlled shutdown (Kafka parity): hand partition leadership (and,
        // on the controller, the consensus) to in-sync peers BEFORE stopping,
        // so no broker keeps sending clients to this node.  Bounded by
        // RUNE_CONTROLLED_SHUTDOWN_MS (0 = off); on expiry it stops as before.
        if (const auto budget = node.controlledShutdownBudget(); budget.count() > 0)
            (void)node.controlledShutdown(budget);
    }
    node.stop();
    // Threads are joined + quiesced now → drain rings and write the final report.
    frReport("shutdown");
    spdlog::info("rune-broker: stopped");
    return exitCode;
}
