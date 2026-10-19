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
//   RUNE_CLIENT_TOPIC_TABLE          — comma-separated "name:id:partitions" entries
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
//
// Authentication (spec §5):
//   RUNE_USER / RUNE_PASSWORD_FILE — the login (see ExampleAuth.h); every mode uses them.
//
// World-server mode (RUNE_MODE=ws; authentication docker e2e, docker/auth-e2e.sh): a long-running
// producer and/or group consumer that reports every authentication outcome explicitly.
//   RUNE_WS_ROLE         both (default) | produce | consume
//   RUNE_PRODUCE_TOPIC   topic written (default: the first of RUNE_CLIENT_TOPIC_TABLE)
//   RUNE_CONSUME_TOPIC   topic read    (default: the first of RUNE_CLIENT_TOPIC_TABLE)
//   RUNE_GROUP_ID        consume in this consumer group (unset: a plain subscription from earliest)
//   RUNE_PRODUCE_N       records to produce, acked one by one (0 = until RUNE_STOP_FILE exists), default 20
//   RUNE_PRODUCE_INTERVAL_US  pause after every acked record (default 0)
//   RUNE_CONSUME_EXPECT  consumer exit 0 once this many DISTINCT records arrived (0 = until RUNE_STOP_FILE)
//   RUNE_EXPECT_FILE     consumer exit 0 once every sequence number listed in this file (the producer's
//                        RUNE_ACKED_FILE, written when it ends) has arrived.  Role both: every acked record.
//   RUNE_STOP_FILE       when this file exists the producer stops producing and the consumer stops consuming
//   RUNE_ACKED_FILE / RUNE_RECEIVED_FILE  where the acked / received sequence numbers are written (one per line)
//   RUNE_ACKS            all (default) | leader: the producer's acks (an acked record is durable with all)
//   RUNE_QUIET           1 = no per-record output
// Output (stdout): "WS: READY" once connected (and the group assigned), "WS-PRODUCER: acked=<a> failed=<f>",
//   "WS-CONSUMER: distinct=<d> total=<t>".  A produce refused with an authentication code exits 1 printing
//   "WS: produce refused: <Name> (<code>)"; a subscription or group the broker ended prints
//   "subscription ended: <Name>" and exits 1; a login failure prints the client's message (e.g.
//   "AuthenticationFailed") and exits 1.

#include "ExampleAuth.h"
#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <mutex>
#include <optional>
#include <set>
#include <string_view>
#include <sys/stat.h>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;

// F8c C6: one acked send waited on a Completions (examples only — the library
// deliberately offers no per-record *AndWait sugar).  false = rejected or the
// wait timed out (`out.status` = Timeout then; the op still completes later).
static bool sendAndWaitAck(Completions& cq, Producer& p, const TopicHandle& th, const Key& key,
                           std::span<const uint8_t> v, Token t, int timeoutMs, Completion& out) {
    for (;;) {
        const SendResult r = p.trySend(th, key, v, cq, t);
        if (r == SendResult::Ok) break;
        if (r == SendResult::QueueFull) { (void)cq.poll([](const Completion&, std::span<const uint8_t>) {}); continue; }
        if (r != SendResult::BackPressured) { out.status = OpStatus::Closed; return false; }
        std::this_thread::yield();
    }
    const auto w = cq.waitFor(t, std::chrono::milliseconds(timeoutMs),
                              [&](const Completion& c, std::span<const uint8_t>) { out = c; });
    if (w != Completions::Wait::Done) { out.status = OpStatus::Timeout; return false; }
    return true;
}

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
    Completions cq(c);

    for (int n = 1; n <= N; ++n) {
        const std::string valStr = "phase" + std::to_string(phase) +
                                   "-record-" + std::to_string(n);
        const std::vector<uint8_t> val(valStr.begin(), valStr.end());

        // Use record number as key (uint64 key).
        Key key(static_cast<uint64_t>(n));

        Completion done;
        const bool waited = sendAndWaitAck(cq, p, th, key, std::span<const uint8_t>(val),
                                           static_cast<Token>(n), timeoutMs, done);
        if (!waited && done.status == OpStatus::Timeout) {
            std::cerr << "CLIENT: FAIL produce phase" << phase
                      << " n=" << n << " TIMEOUT\n";
            return false;
        }
        if (waited && done.status == OpStatus::Ok) {
            const int64_t offset = done.value;
            std::cout << "CLIENT: OK produce phase" << phase
                      << "=" << n << " offset=" << offset << "\n";
            std::cout.flush();
        } else {
            std::cerr << "CLIENT: FAIL produce phase" << phase
                      << " n=" << n << ": status=" << static_cast<int>(done.status)
                      << " errorCode=" << done.errorCode << "\n";
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
// World-server mode (RUNE_MODE=ws)
// ---------------------------------------------------------------------------

namespace {

bool fileExists(const std::string& p) {
    struct stat st {};
    return !p.empty() && ::stat(p.c_str(), &st) == 0;
}

const char* subFailName(SubscriptionFailReason r, uint16_t code) {
    switch (r) {
    case SubscriptionFailReason::NotAuthorized:   return "NotAuthorized";
    case SubscriptionFailReason::SessionRevoked:  return "SessionRevoked";
    case SubscriptionFailReason::RejectedByBroker:
        if (const char* n = authErrorName(code)) return n;
        return "RejectedByBroker";
    case SubscriptionFailReason::SessionLost:     return "SessionLost";
    case SubscriptionFailReason::TimedOut:        return "TimedOut";
    default:                                      return "Ended";
    }
}

const char* groupFailName(GroupFailReason r) {
    switch (r) {
    case GroupFailReason::NotAuthorized:        return "NotAuthorized";
    case GroupFailReason::SessionRevoked:       return "SessionRevoked";
    case GroupFailReason::BrokerTooOld:         return "BrokerTooOld";
    case GroupFailReason::RefusedByBroker:      return "RefusedByBroker";
    case GroupFailReason::RetryBudgetExhausted: return "RetryBudgetExhausted";
    default:                                    return "Ended";
    }
}

void writeSeqFile(const std::string& path, const std::set<long>& seqs) {
    if (path.empty()) return;
    std::ofstream f(path, std::ios::trunc);
    for (const long s : seqs) f << s << "\n";
}

// "ws:<seq>" -> seq (-1 if foreign).
long parseSeq(std::string_view v) {
    if (v.size() < 4 || v.substr(0, 3) != "ws:") return -1;
    long n = 0;
    for (const char ch : v.substr(3)) {
        if (ch < '0' || ch > '9') return -1;
        n = n * 10 + (ch - '0');
    }
    return n;
}

struct WsSeen {
    std::mutex     mtx;
    std::set<long> distinct;
    long           total = 0;
    std::size_t    count() { std::lock_guard<std::mutex> lk(mtx); return distinct.size(); }
    void add(long seq) { std::lock_guard<std::mutex> lk(mtx); ++total; if (seq >= 0) distinct.insert(seq); }
};

int runWorldServer(ClientConfig& cfg, const std::string& role, int timeoutMs) {
    const bool doProduce = role == "both" || role == "produce";
    const bool doConsume = role == "both" || role == "consume";
    if (!doProduce && !doConsume) { std::cerr << "CLIENT: RUNE_WS_ROLE must be both|produce|consume\n"; return 2; }

    const std::string produceTopic = getEnvStr("RUNE_PRODUCE_TOPIC", cfg.topics[0].name);
    const std::string consumeTopic = getEnvStr("RUNE_CONSUME_TOPIC", cfg.topics[0].name);
    const std::string group        = getEnvStr("RUNE_GROUP_ID");
    const int         produceN     = getEnvInt("RUNE_PRODUCE_N", 20);
    const int         intervalUs   = getEnvInt("RUNE_PRODUCE_INTERVAL_US", 0);
    const int         expect       = getEnvInt("RUNE_CONSUME_EXPECT", 0);
    const std::string stopFile     = getEnvStr("RUNE_STOP_FILE");
    const std::string ackedFile    = getEnvStr("RUNE_ACKED_FILE");
    const std::string receivedFile = getEnvStr("RUNE_RECEIVED_FILE");
    const bool        quiet        = getEnvInt("RUNE_QUIET", 0) != 0;

    WsSeen seen;
    std::atomic<bool> assigned{!doConsume || group.empty()};
    int rc = 0;

    try {
        Client c(cfg);
        c.onError([](const std::string& msg) { std::cerr << "CLIENT: onError: " << msg << "\n"; });

        // The group consumer is created BEFORE connect (setup-phase contract).
        std::optional<GroupConsumer> gc;
        if (doConsume && !group.empty()) {
            GroupConfig gcfg;
            gcfg.autoCommit           = true;
            gcfg.autoCommitIntervalMs = 1000;
            gcfg.autoOffsetReset      = OffsetReset::Earliest;
            gc.emplace(c.subscribeGroup(group, {consumeTopic}, gcfg));
            gc->onAssigned([&](std::vector<TopicPartition>) { assigned.store(true); });
            gc->onRecords([&](const RecordSet& rs) {
                rs.forEachRecord([&](std::string_view, std::string_view val, int64_t, int64_t) {
                    seen.add(parseSeq(val));
                });
            });
        }

        c.connectAndWait();

        std::optional<Subscription> sub;
        if (doConsume && group.empty()) {
            const TopicHandle th = c.topic(consumeTopic);
            SubscribeOptions opts;
            opts.from      = From::earliest();
            opts.isolation = Isolation::ReadUncommitted;
            sub.emplace(c.subscribe(th, 0, opts, [&](const Record& r) {
                seen.add(parseSeq(std::string_view(reinterpret_cast<const char*>(r.value.data()), r.value.size())));
            }));
        }

        auto consumerEnded = [&]() -> const char* {
            if (gc && gc->finished()) return groupFailName(gc->failReason());
            if (sub && !sub->active()) return subFailName(sub->failReason(), sub->brokerErrorCode());
            return nullptr;
        };

        const auto readyDeadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs * 6);
        while (!assigned.load() && std::chrono::steady_clock::now() < readyDeadline) {
            if (const char* e = consumerEnded()) { std::cout << "subscription ended: " << e << "\n"; std::cout.flush(); return 1; }
            std::this_thread::sleep_for(10ms);
        }
        if (!assigned.load()) { std::cerr << "CLIENT: FAIL the group was never assigned\n"; return 1; }
        std::cout << "WS: READY\n";
        std::cout.flush();

        std::set<long> acked;
        long failed = 0;

        if (doProduce) {
            const TopicHandle th = c.topic(produceTopic);
            if (!th.valid()) { std::cerr << "CLIENT: topic '" << produceTopic << "' not valid\n"; return 1; }
            ProducerConfig pcfg;
            // "Acked" must mean durable: with acks=leader a record acked by a leader that is then restarted may never
            // have been replicated (the cluster-key rotation restarts every broker under this producer).
            pcfg.acks             = getEnvStr("RUNE_ACKS", "all") == "leader" ? Acks::Leader : Acks::All;
            pcfg.lingerUs         = 1;
            pcfg.batchBytes       = 1;
            pcfg.maxInFlight      = 16;
            pcfg.produceTimeoutMs = static_cast<int64_t>(timeoutMs);
            auto p = c.producer(pcfg);
            Completions cq(c);
            int consecutiveFailures = 0;
            for (long n = 1; produceN == 0 || n <= produceN; ++n) {
                if (fileExists(stopFile)) break;
                if (const char* e = consumerEnded()) { std::cout << "subscription ended: " << e << "\n"; std::cout.flush(); rc = 1; break; }
                const std::string valStr = "ws:" + std::to_string(n);
                const std::vector<uint8_t> val(valStr.begin(), valStr.end());
                Completion done;
                const bool waited = sendAndWaitAck(cq, p, th, Key(static_cast<uint64_t>(n)), std::span<const uint8_t>(val),
                                                   static_cast<Token>(n), timeoutMs, done);
                if (waited && done.status == OpStatus::Ok) {
                    acked.insert(n);
                    consecutiveFailures = 0;
                    if (!quiet) std::cout << "CLIENT: OK produce ws=" << n << " offset=" << done.value << "\n";
                } else if (waited && done.status == OpStatus::BrokerError && authErrorName(done.errorCode) != nullptr) {
                    std::cout << "WS: produce refused: " << authErrorName(done.errorCode) << " (" << done.errorCode << ")\n";
                    std::cout.flush();
                    rc = 1;
                    break;
                } else {
                    ++failed;
                    std::cerr << "CLIENT: produce ws=" << n << " not acked: status=" << static_cast<int>(done.status)
                              << " errorCode=" << done.errorCode << "\n";
                    if (++consecutiveFailures >= 50) { std::cerr << "CLIENT: FAIL 50 consecutive produce failures\n"; rc = 1; break; }
                }
                if (intervalUs > 0) std::this_thread::sleep_for(std::chrono::microseconds(intervalUs));
            }
            std::cout << "WS-PRODUCER: acked=" << acked.size() << " failed=" << failed << "\n";
            std::cout.flush();
            writeSeqFile(ackedFile, acked);
        }

        if (doConsume && rc == 0) {
            // Done when (role both) every acked record, (RUNE_EXPECT_FILE written by the producer at its end) every
            // record listed in that file, (RUNE_CONSUME_EXPECT) that many distinct records, or (none of these) when
            // the stop file appears has arrived.  A bounded wait; never a silent stop.
            const std::string expectFile = getEnvStr("RUNE_EXPECT_FILE");
            std::set<long> expected;
            bool haveExpected = false;
            if (doProduce) { expected = acked; haveExpected = true; }
            auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs * 6);
            auto coversExpected = [&]() {
                std::lock_guard<std::mutex> lk(seen.mtx);
                return std::includes(seen.distinct.begin(), seen.distinct.end(), expected.begin(), expected.end());
            };
            for (;;) {
                if (const char* e = consumerEnded()) {
                    std::cout << "subscription ended: " << e << "\n"; std::cout.flush();
                    rc = 1;
                    break;
                }
                if (!haveExpected && !expectFile.empty() && fileExists(expectFile)) {
                    std::ifstream f(expectFile);
                    long v = 0;
                    while (f >> v) expected.insert(v);
                    haveExpected = true;
                    deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs * 6);
                }
                if (haveExpected) {
                    if (coversExpected()) break;
                    if (std::chrono::steady_clock::now() >= deadline) {
                        std::cerr << "CLIENT: FAIL consume: " << seen.count() << " distinct, " << expected.size()
                                  << " expected records not all received\n";
                        rc = 1;
                        break;
                    }
                } else if (expect > 0) {
                    if (seen.count() >= static_cast<std::size_t>(expect)) break;
                    if (std::chrono::steady_clock::now() >= deadline) {
                        std::cerr << "CLIENT: FAIL consume: " << seen.count() << " / " << expect << " distinct records\n";
                        rc = 1;
                        break;
                    }
                } else if (expectFile.empty() && fileExists(stopFile)) {
                    break;
                }
                std::this_thread::sleep_for(10ms);
            }
            std::cout << "WS-CONSUMER: distinct=" << seen.count() << " total=" << seen.total << "\n";
            std::cout.flush();
            {
                std::lock_guard<std::mutex> lk(seen.mtx);
                writeSeqFile(receivedFile, seen.distinct);
            }
        }

        if (sub) sub->unsubscribe();
        std::cout << "CLIENT: DONE rc=" << rc << "\n";
        std::cout.flush();
        c.close();
    } catch (const std::exception& ex) {
        // A login failure surfaces here with its code name ("connect(): broker error code 44 (AuthenticationFailed)").
        std::cout << "WS: FAIL " << ex.what() << "\n";
        std::cout.flush();
        std::cerr << "CLIENT: FATAL exception: " << ex.what() << "\n";
        return 1;
    }
    return rc;
}

} // namespace

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main() {
    if (std::getenv("RUNE_TOPICS") != nullptr) {
        std::cerr << "CLIENT: RUNE_TOPICS was renamed to RUNE_CLIENT_TOPIC_TABLE (the client's name:topicId:partitions table)\n";
        return 2;
    }
    const std::string target        = getEnvStr("RUNE_TARGET");
    const int         targetStream  = getEnvInt("RUNE_TARGET_STREAM",   1);
    const std::string egress        = getEnvStr("RUNE_EGRESS");
    const int         egressStream  = getEnvInt("RUNE_EGRESS_STREAM",   2);
    const std::string dataDir       = getEnvStr("RUNE_DATA_DIR");
    const std::string topicsStr     = getEnvStr("RUNE_CLIENT_TOPIC_TABLE",          "events:1:1");
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
    rune::examples::applyEnvCredentials(cfg);   // RUNE_USER / RUNE_PASSWORD_FILE
    cfg.connectTimeoutMs = static_cast<int64_t>(timeoutMs);
    cfg.pingTimeoutMs    = static_cast<int64_t>(timeoutMs);
    cfg.idleMode         = IdleMode::LowLatency;

    cfg.topics = parseTopics(topicsStr);
    if (cfg.topics.empty()) {
        std::cerr << "CLIENT: no valid topics parsed from RUNE_CLIENT_TOPIC_TABLE='"
                  << topicsStr << "'\n";
        return 1;
    }

    if (getEnvStr("RUNE_MODE") == "ws") return runWorldServer(cfg, getEnvStr("RUNE_WS_ROLE", "both"), timeoutMs);

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
