// rune-broker main entrypoint — BR-2
//
// Reads NodeConfig from environment variables; constructs a BrokerNode;
// starts it; waits for self-election (1-node) or runs until signaled;
// handles SIGINT/SIGTERM for clean shutdown.

#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>

#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>

using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// Signal handling (global flag; safe from signal context per POSIX)
// ---------------------------------------------------------------------------
static std::atomic<bool> g_stop{false};

static void sighandler(int /*sig*/) noexcept {
    g_stop.store(true, std::memory_order_release);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main() {
    // Install signal handlers before anything else.
    std::signal(SIGINT,  sighandler);
    std::signal(SIGTERM, sighandler);

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
        spdlog::info("rune-broker: node {} is LEADER", node.leaderId());
    } else {
        spdlog::warn("rune-broker: leadership not reached within 5 s — continuing");
    }

    // Run until SIGINT / SIGTERM.
    spdlog::info("rune-broker: running — send SIGINT or SIGTERM to stop");
    while (!g_stop.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(100ms);
    }

    spdlog::info("rune-broker: shutdown signal received — stopping");
    node.stop();
    spdlog::info("rune-broker: stopped");
    return 0;
}
