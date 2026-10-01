// examples/rune_produce_subscribe.cpp — CB-5 C
//
// E2E produce/subscribe example: connects to a live Rune cluster, produces N
// records, subscribes from Earliest, and asserts all N records are delivered
// in offset order.  Then kills the leader (optional) and produces M more,
// asserting the cluster recovers and the additional records are delivered.
//
// Environment:
//   RUNE_TARGET          — node client ingress channel (required)
//                          e.g. "aeron:udp?endpoint=rune-1:40200"
//   RUNE_TARGET_STREAM   — int, default 1
//   RUNE_EGRESS          — this client's own egress channel (required)
//                          e.g. "aeron:udp?endpoint=rune-client:40400"
//   RUNE_EGRESS_STREAM   — int, default 2
//   RUNE_DATA_DIR        — Aeron driver dir (required)
//   RUNE_TOPICS          — comma-separated "name:id:partitions" entries
//                          e.g. "events:1:1,orders:2:4"
//                          default: "events:1:1"
//   RUNE_PRODUCE_N       — records to produce in phase 1, default 20
//   RUNE_PRODUCE_M       — records to produce in phase 2 (post-kill), default 10
//                          set 0 to skip phase 2
//   RUNE_TIMEOUT_MS      — per-operation timeout ms, default 10000
//   RUNE_LEADER_KILL_CMD — shell command to kill the leader before phase 2
//                          (optional; skipped if empty)
//                          e.g. "docker kill rune-1"
//   RUNE_RECOVERY_WAIT_MS— ms to wait after leader kill for re-election, default 5000
//
// Exit 0 iff:
//   Phase 1: all N produced records arrive in offset order via subscriber.
//   Phase 2 (if RUNE_PRODUCE_M > 0): all M records arrive post-recovery.
//
// Prints "CLIENT: OK produce phase1=<n>" per produced record and
//        "CLIENT: OK record offset=<o>" per delivered record.

#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;

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
// Topic config parser: "name:id:partitions,..."
// ---------------------------------------------------------------------------

static std::vector<TopicSpec> parseTopics(const std::string& s) {
    std::vector<TopicSpec> out;
    if (s.empty()) return out;

    std::size_t pos = 0;
    while (pos <= s.size()) {
        const std::size_t comma = s.find(',', pos);
        const std::string tok = s.substr(pos, (comma == std::string::npos ? s.size() : comma) - pos);
        pos = (comma == std::string::npos ? s.size() + 1 : comma + 1);
        if (tok.empty()) continue;

        // tok = "name:id:partitions"
        const auto c1 = tok.find(':');
        if (c1 == std::string::npos) continue;
        const auto c2 = tok.find(':', c1 + 1);
        if (c2 == std::string::npos) continue;

        TopicSpec ts;
        ts.name           = tok.substr(0, c1);
        ts.topicId        = std::atoi(tok.c_str() + c1 + 1);
        ts.partitionCount = std::atoi(tok.c_str() + c2 + 1);
        out.push_back(std::move(ts));
    }
    return out;
}

// ---------------------------------------------------------------------------
// Produce N records on partition 0 of topic, waiting for each ack.
// Returns false if any send fails.
// ---------------------------------------------------------------------------

static bool phaseProduceN(Client& c, const TopicHandle& th, int N,
                           int timeoutMs, int phase) {
    ProducerConfig pcfg;
    pcfg.acks             = Acks::Leader;
    pcfg.lingerUs         = 1;
    pcfg.batchBytes       = 1;
    pcfg.maxInFlight      = 16;
    pcfg.produceTimeoutMs = static_cast<int64_t>(timeoutMs);

    auto p = c.producer(pcfg);

    for (int n = 1; n <= N; ++n) {
        const std::string valStr = "phase" + std::to_string(phase) +
                                   "-record-" + std::to_string(n);
        const std::vector<uint8_t> val(valStr.begin(), valStr.end());

        // Use record number as key (uint64 key).
        Key key(static_cast<uint64_t>(n));

        auto fut = p.send(th, key, std::span<const uint8_t>(val), want_future);

        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeoutMs);
        if (fut.wait_until(deadline) != std::future_status::ready) {
            std::cerr << "CLIENT: FAIL produce phase" << phase
                      << " n=" << n << " TIMEOUT\n";
            return false;
        }
        try {
            const int64_t offset = fut.get();
            std::cout << "CLIENT: OK produce phase" << phase
                      << "=" << n << " offset=" << offset << "\n";
            std::cout.flush();
        } catch (const std::exception& ex) {
            std::cerr << "CLIENT: FAIL produce phase" << phase
                      << " n=" << n << ": " << ex.what() << "\n";
            return false;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// Subscribe and collect records until we have `need` records or timeout.
// Returns collected offsets in delivery order.
// ---------------------------------------------------------------------------

static std::vector<int64_t> collectRecords(Client& c, const TopicHandle& th,
                                            int need, int timeoutMs) {
    std::mutex              mtx;
    std::vector<int64_t>    offsets;
    offsets.reserve(static_cast<std::size_t>(need));

    SubscribeOptions opts;
    opts.from      = From::earliest();
    opts.isolation = Isolation::ReadUncommitted;

    auto sub = c.subscribe(th, /*partition=*/0, opts, [&](const Record& r) {
        std::lock_guard<std::mutex> lk(mtx);
        offsets.push_back(r.offset);
        std::cout << "CLIENT: OK record offset=" << r.offset << "\n";
        std::cout.flush();
    });

    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeoutMs);

    while (std::chrono::steady_clock::now() < deadline) {
        {
            std::lock_guard<std::mutex> lk(mtx);
            if (static_cast<int>(offsets.size()) >= need) break;
        }
        std::this_thread::sleep_for(10ms);
    }

    // Unsubscribe before returning.
    sub.unsubscribe();

    std::lock_guard<std::mutex> lk(mtx);
    return offsets;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main() {
    const std::string target        = getEnvStr("RUNE_TARGET");
    const int         targetStream  = getEnvInt("RUNE_TARGET_STREAM",   1);
    const std::string egress        = getEnvStr("RUNE_EGRESS");
    const int         egressStream  = getEnvInt("RUNE_EGRESS_STREAM",   2);
    const std::string dataDir       = getEnvStr("RUNE_DATA_DIR");
    const std::string topicsStr     = getEnvStr("RUNE_TOPICS",          "events:1:1");
    const int         produceN      = getEnvInt("RUNE_PRODUCE_N",       20);
    const int         produceM      = getEnvInt("RUNE_PRODUCE_M",       10);
    const int         timeoutMs     = getEnvInt("RUNE_TIMEOUT_MS",      10000);
    const std::string leaderKillCmd = getEnvStr("RUNE_LEADER_KILL_CMD");
    const int         recoveryWait  = getEnvInt("RUNE_RECOVERY_WAIT_MS", 5000);

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

    std::cerr << "CLIENT: target=" << target << " stream=" << targetStream
              << " egress=" << egress << " egressStream=" << egressStream
              << " dataDir=" << dataDir
              << " topics=" << topicsStr
              << " produceN=" << produceN << " produceM=" << produceM
              << " timeoutMs=" << timeoutMs << "\n";

    // Build ClientConfig.
    ClientConfig cfg;
    cfg.gatewayChannel   = target;
    cfg.gatewayStreamId  = targetStream;
    cfg.egressChannel    = egress;
    cfg.egressStreamId   = egressStream;
    cfg.dataDir          = dataDir;
    cfg.connectTimeoutMs = static_cast<int64_t>(timeoutMs);
    cfg.pingTimeoutMs    = static_cast<int64_t>(timeoutMs);
    cfg.idleMode         = IdleMode::LowLatency;

    cfg.topics = parseTopics(topicsStr);
    if (cfg.topics.empty()) {
        std::cerr << "CLIENT: no valid topics parsed from RUNE_TOPICS='"
                  << topicsStr << "'\n";
        return 1;
    }

    int rc = 0;

    try {
        Client c(cfg);

        c.onConnected([](int64_t sid) {
            std::cerr << "CLIENT: onConnected sessionId=" << sid << "\n";
        });
        c.onDisconnected([]() {
            std::cerr << "CLIENT: onDisconnected\n";
        });
        c.onError([](const std::string& msg) {
            std::cerr << "CLIENT: onError: " << msg << "\n";
        });

        std::cerr << "CLIENT: connecting...\n";
        c.connectAndWait();
        std::cerr << "CLIENT: connected\n";

        // Use the first topic for produce+subscribe.
        const TopicHandle th = c.topic(cfg.topics[0].name);
        if (!th.valid()) {
            std::cerr << "CLIENT: topic '" << cfg.topics[0].name
                      << "' not valid\n";
            return 1;
        }

        // ---- Phase 1: produce N records, then subscribe and assert delivery ----

        std::cerr << "CLIENT: phase1 produce N=" << produceN << "\n";
        if (!phaseProduceN(c, th, produceN, timeoutMs, 1)) {
            std::cerr << "CLIENT: FAIL phase1 produce\n";
            rc = 1;
        } else {
            std::cerr << "CLIENT: phase1 produce done\n";

            // Subscribe from Earliest and wait for N records.
            std::cerr << "CLIENT: phase1 subscribe from earliest...\n";
            const int collectTimeoutMs = timeoutMs * 3;  // extra slack
            auto offsets = collectRecords(c, th, produceN, collectTimeoutMs);

            if (static_cast<int>(offsets.size()) < produceN) {
                std::cerr << "CLIENT: FAIL phase1 subscribe: got "
                          << offsets.size() << " / " << produceN << "\n";
                rc = 1;
            } else {
                // Assert offsets are monotonically non-decreasing.
                bool ordered = true;
                for (std::size_t i = 1; i < offsets.size(); ++i) {
                    if (offsets[i] <= offsets[i - 1]) {
                        ordered = false;
                        std::cerr << "CLIENT: FAIL out-of-order at idx=" << i
                                  << " offset[i-1]=" << offsets[i - 1]
                                  << " offset[i]=" << offsets[i] << "\n";
                        rc = 1;
                    }
                }
                if (ordered) {
                    std::cerr << "CLIENT: phase1 OK: " << offsets.size()
                              << " records in offset order\n";
                }
            }
        }

        // ---- Phase 2: optional leader kill + produce M more ----

        if (rc == 0 && produceM > 0) {
            if (!leaderKillCmd.empty()) {
                std::cerr << "CLIENT: killing leader: " << leaderKillCmd << "\n";
                if (std::system(leaderKillCmd.c_str()) != 0) {
                    std::cerr << "CLIENT: WARN leader-kill command returned non-zero\n";
                }

                std::cerr << "CLIENT: waiting " << recoveryWait
                          << "ms for re-election...\n";
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(recoveryWait));
                std::cerr << "CLIENT: recovery wait done\n";
            }

            std::cerr << "CLIENT: phase2 produce M=" << produceM << "\n";
            if (!phaseProduceN(c, th, produceM, timeoutMs * 3, 2)) {
                std::cerr << "CLIENT: FAIL phase2 produce\n";
                rc = 1;
            } else {
                // Subscribe and collect M more records (from latest approach:
                // we actually collect produceN + produceM total from Earliest,
                // then check the last M are new).  Simpler: just assert M more
                // arrive by checking total count.
                std::cerr << "CLIENT: phase2 subscribe...\n";
                const int total = produceN + produceM;
                const int collectTimeoutMs = timeoutMs * 4;
                auto offsets = collectRecords(c, th, total, collectTimeoutMs);

                if (static_cast<int>(offsets.size()) < total) {
                    std::cerr << "CLIENT: FAIL phase2 subscribe: got "
                              << offsets.size() << " / " << total << "\n";
                    rc = 1;
                } else {
                    // Check monotonic order across all records.
                    bool ordered = true;
                    for (std::size_t i = 1; i < offsets.size(); ++i) {
                        if (offsets[i] <= offsets[i - 1]) {
                            ordered = false;
                            std::cerr << "CLIENT: FAIL phase2 out-of-order at idx="
                                      << i << " offsets[i-1]=" << offsets[i - 1]
                                      << " offsets[i]=" << offsets[i] << "\n";
                            rc = 1;
                        }
                    }
                    if (ordered) {
                        std::cerr << "CLIENT: phase2 OK: " << offsets.size()
                                  << " total records in offset order post-recovery\n";
                    }
                }
            }
        }

        std::cout << "CLIENT: DONE rc=" << rc << "\n";
        std::cout.flush();

        c.close();

    } catch (const std::exception& ex) {
        std::cerr << "CLIENT: FATAL exception: " << ex.what() << "\n";
        rc = 2;
    }

    return rc;
}
