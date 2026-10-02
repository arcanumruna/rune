// examples/rune_client_ping.cpp — CC-7
//
// E2E ping example: uses rune::client::Client to connect to a live broker node
// and send RUNE_PINGS pings, asserting each echo matches.
//
// Environment:
//   RUNE_TARGET          — node client ingress channel (required)
//                          e.g. "aeron:udp?endpoint=rune-1:40200"
//   RUNE_TARGET_STREAM   — int, default 1
//   RUNE_EGRESS          — this client's own egress channel (required)
//                          e.g. "aeron:udp?endpoint=rune-client:40400"
//   RUNE_EGRESS_STREAM   — int, default 2
//   RUNE_DATA_DIR        — Aeron driver dir (required; no /tmp default: must
//                          be unique per container)
//   RUNE_PINGS           — number of pings to send, default 20
//   RUNE_PING_INTERVAL_MS — sleep between pings (ms), default 500
//   RUNE_TIMEOUT_MS      — per-operation timeout ms, default 10000
//
// Exit 0 iff all pings returned echo-matched Pongs (allowing transient
// failures around a cluster node-kill window — see RUNE_PINGS tolerance below).
// Prints "CLIENT: OK ping=<n>" per success, "CLIENT: WARN ping=<n> TIMEOUT"
// per transient miss, and "CLIENT: DONE pings=<n> ok=<n>" at the end.
// Exits non-zero with reason if the run is unrecoverable or ok < min_required.
//
// M-A limitations documented:
//   - Client targets a fixed node for the session lifetime.
//   - If the client's OWN target dies, the session would need to retarget to
//     a surviving node's endpoint — that is Milestone B (requires per-peer
//     client endpoint map from the broker + REDIRECT follow in SessionMachine).
//   - The E2E kills a node that is NOT the client's target, so the client's
//     target keeps serving through re-election, proving cluster-churn survival.

#include <rune/client/Client.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using rune::client::Bytes;
using rune::client::Client;
using rune::client::ClientConfig;

// ---------------------------------------------------------------------------
// Env helpers
// ---------------------------------------------------------------------------

static std::string getEnvStr(const char* name, const std::string& def = {}) {
    const char* v = std::getenv(name);
    return (v && *v) ? std::string(v) : def;
}

static int getEnvInt(const char* name, int def) {
    const char* v = std::getenv(name);
    return (v && *v) ? std::atoi(v) : def;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main() {
    const std::string target       = getEnvStr("RUNE_TARGET");
    const int         targetStream = getEnvInt("RUNE_TARGET_STREAM", 1);
    const std::string egress       = getEnvStr("RUNE_EGRESS");
    const int         egressStream = getEnvInt("RUNE_EGRESS_STREAM", 2);
    const std::string dataDir      = getEnvStr("RUNE_DATA_DIR");
    const int         numPings     = getEnvInt("RUNE_PINGS", 20);
    const int         intervalMs   = getEnvInt("RUNE_PING_INTERVAL_MS", 500);
    const int         timeoutMs    = getEnvInt("RUNE_TIMEOUT_MS", 10000);

    if (target.empty()) {
        std::cerr << "CLIENT: RUNE_TARGET is required\n";
        return 1;
    }
    if (egress.empty()) {
        std::cerr << "CLIENT: RUNE_EGRESS is required\n";
        return 1;
    }
    if (dataDir.empty()) {
        std::cerr << "CLIENT: RUNE_DATA_DIR is required\n";
        return 1;
    }
    if (numPings <= 0) {
        std::cerr << "CLIENT: RUNE_PINGS must be > 0\n";
        return 1;
    }

    std::cerr << "CLIENT: target=" << target << " stream=" << targetStream
              << " egress=" << egress << " egressStream=" << egressStream
              << " dataDir=" << dataDir
              << " pings=" << numPings
              << " intervalMs=" << intervalMs
              << " timeoutMs=" << timeoutMs << "\n";

    // -------------------------------------------------------------------------
    // Build ClientConfig
    // -------------------------------------------------------------------------
    ClientConfig cfg;
    cfg.gatewayChannel  = target;
    cfg.gatewayStreamId = targetStream;
    cfg.egressChannel   = egress;
    cfg.egressStreamId  = egressStream;
    cfg.dataDir         = dataDir;
    cfg.connectTimeoutMs = static_cast<int64_t>(timeoutMs);
    cfg.pingTimeoutMs    = static_cast<int64_t>(timeoutMs);

    // -------------------------------------------------------------------------
    // Construct + connect
    // -------------------------------------------------------------------------
    int rc = 0;

    try {
        Client c(cfg);

        // Optional observability callbacks.
        c.onConnected([](int64_t sid) {
            std::cerr << "CLIENT: onConnected sessionId=" << sid << "\n";
        });
        c.onDisconnected([]() {
            std::cerr << "CLIENT: onDisconnected\n";
        });
        c.onError([](const std::string& msg) {
            std::cerr << "CLIENT: onError: " << msg << "\n";
        });

        // connectAndWait() — the op itself is bounded by connectTimeoutMs.
        std::cerr << "CLIENT: connecting...\n";
        c.connectAndWait();  // throws on error / timeout
        std::cerr << "CLIENT: connected\n";

        // -------------------------------------------------------------------------
        // Ping loop
        // -------------------------------------------------------------------------
        // Tolerance: the brief allows a bounded number of transient failures
        // around the kill window provided the run recovers and finishes with pongs.
        // We permit up to 20% misses (or 2, whichever is larger) as the kill window.
        // The E2E kills a NON-target node, so actual misses should be zero.
        const int maxMisses = std::max(2, numPings / 5);

        int ok   = 0;
        int miss = 0;

        for (int n = 1; n <= numPings; ++n) {
            // Build a simple echo payload: "rune-ping-<n>"
            const std::string echoStr = "rune-ping-" + std::to_string(n);
            Bytes payload(echoStr.begin(), echoStr.end());

            try {
                // Throws if not connected; the op is bounded by pingTimeoutMs.
                Bytes echo = c.pingAndWait(payload);

                // Verify echo matches.
                const std::string echoGot(echo.begin(), echo.end());
                if (echoGot != echoStr) {
                    std::cerr << "CLIENT: FAIL ping=" << n
                              << " echo mismatch: got='" << echoGot
                              << "' want='" << echoStr << "'\n";
                    // Count as a miss — could be a protocol bug; hard-fail.
                    rc = 1;
                    std::cerr << "CLIENT: FATAL echo mismatch — aborting\n";
                    break;
                }

                std::cout << "CLIENT: OK ping=" << n << "\n";
                std::cout.flush();
                ++ok;

            } catch (const std::exception& ex) {
                std::cerr << "CLIENT: WARN ping=" << n
                          << " TIMEOUT/ERROR: " << ex.what() << "\n";
                ++miss;
                if (miss > maxMisses) {
                    std::cerr << "CLIENT: FATAL too many misses (" << miss
                              << " > " << maxMisses
                              << ") — aborting\n";
                    rc = 1;
                    break;
                }
                // Small recovery sleep before retrying.
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                continue;
            }

            // Interval between pings (skip on last).
            if (n < numPings) {
                std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
            }
        }

        // Final result line (always printed regardless of rc).
        std::cout << "CLIENT: DONE pings=" << numPings
                  << " ok=" << ok << "\n";
        std::cout.flush();

        // Require all pings to have returned pongs (misses cost rc).
        if (rc == 0 && ok < numPings - maxMisses) {
            std::cerr << "CLIENT: FAIL insufficient pongs: ok=" << ok
                      << " need>=" << (numPings - maxMisses) << "\n";
            rc = 1;
        }

        c.close();

    } catch (const std::exception& ex) {
        std::cerr << "CLIENT: FATAL exception: " << ex.what() << "\n";
        rc = 2;
    }

    return rc;
}
