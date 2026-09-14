// examples/dr_client.cpp — DR-1 configurable disaster-recovery driver
//
// A richer, fully env-driven produce/subscribe driver used by the Rune
// disaster-recovery E2E harness (rune-broker/dr-e2e).  It reuses the same
// rune::client API as rune_produce_subscribe but adds the knobs the DR matrix
// sweeps over, and emits MACHINE-PARSEABLE result lines on stdout so the
// pytest harness can assert invariants (no-loss / ordering / exactly-once /
// resume-from-offset / durable-after-restart).
//
// ─────────────────────────────────────────────────────────────────────────
// Environment (all optional except RUNE_TARGET / RUNE_EGRESS / RUNE_DATA_DIR):
//
//   Connection:
//     RUNE_TARGET          node client ingress channel (required)
//                            e.g. "aeron:udp?endpoint=rune-1:40200"
//     RUNE_TARGET_STREAM   int, default 1
//     RUNE_EGRESS          this client's own egress channel (required)
//                            e.g. "aeron:udp?endpoint=dr-client-0:40400"
//     RUNE_EGRESS_STREAM   int, default 2
//     RUNE_DATA_DIR        Aeron driver dir (required)
//     RUNE_TOPICS          "name:id:partitions,..." default "events:0:1"
//     RUNE_TIMEOUT_MS      per-op timeout ms, default 10000
//
//   Workload:
//     RUNE_MODE            "produce" | "subscribe" | "produce_subscribe"
//                            default "produce_subscribe"
//     RUNE_RECORDS         records to produce (produce modes), default 20
//     RUNE_KEYS            number of distinct keys to spread records across,
//                            default 1.  Record i uses key (i % RUNE_KEYS).
//     RUNE_CLIENT_ID       integer id of this client instance, default 0.
//                            Used to disjoint the key space + seq space across
//                            concurrent clients: effective key = clientId*KEYS+k
//                            and the record payload carries clientId so the
//                            harness can attribute delivered records per client.
//     RUNE_RATE_HZ         produce rate cap (records/sec); 0 = unthrottled
//                            (default 0).
//
//   Subscribe:
//     RUNE_FROM            "earliest" | "latest" | "offset:<N>"  default
//                            "earliest".
//     RUNE_EXPECT          number of records the subscriber should collect
//                            before declaring success; default = RECORDS (for
//                            produce_subscribe) or 0 (subscribe-only waits the
//                            full RUNE_SUBSCRIBE_MS window).
//     RUNE_SUBSCRIBE_MS    max ms to wait while collecting, default
//                            RUNE_TIMEOUT_MS*3.
//     RUNE_SLOW_SUBSCRIBER_MS  per-record artificial handler delay (ms) to
//                            model a slow/backpressured consumer, default 0.
//                            NB: applied OFF the agent thread (buffered) so we
//                            never violate the "handler must not block" rule.
//     RUNE_SUB_PARTITIONS  number of partitions to subscribe to (0..N-1),
//                            default 1 (= partition 0 only, backward-compatible).
//                            Set to the topic's partitionCount to cover ALL
//                            partitions in multi-partition end-to-end tests.
//
// ─────────────────────────────────────────────────────────────────────────
// Output (stdout, one field-per-token, stable across releases):
//
//   PRODUCED cid=<c> key=<k> seq=<s> part=<p> offset=<o>
//   DELIVERED cid=<c> key=<k> seq=<s> part=<p> offset=<o>
//   SUMMARY produced=<np> delivered=<nd> mode=<m> rc=<rc>
//
//   For DELIVERED, cid/key/seq are decoded from the record VALUE payload,
//   which the producer encodes as "cid=<c>;key=<k>;seq=<s>".  A record whose
//   value does not match that schema (e.g. produced by another driver run) is
//   still emitted with cid=-1 key=-1 seq=-1 but counted.
//
// Exit code: 0 on success, 1 on workload assertion failure, 2 on exception.

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
#include <cstring>
#include <deque>
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
// From parser: "earliest" | "latest" | "offset:<N>"
// ---------------------------------------------------------------------------
static From parseFrom(const std::string& s) {
    if (s.rfind("offset:", 0) == 0) {
        return From::offset(std::atoll(s.c_str() + 7));
    }
    if (s == "latest") return From::latest();
    return From::earliest();
}

// Payload schema: "cid=<c>;key=<k>;seq=<s>".  Decoded on the subscribe side.
static std::string encodePayload(int cid, int key, int seq) {
    return "cid=" + std::to_string(cid) + ";key=" + std::to_string(key) +
           ";seq=" + std::to_string(seq);
}
static void decodePayload(const std::string& v, int& cid, int& key, int& seq) {
    cid = key = seq = -1;
    const char* p = v.c_str();
    const char* c = std::strstr(p, "cid=");
    const char* k = std::strstr(p, "key=");
    const char* s = std::strstr(p, "seq=");
    if (c) cid = std::atoi(c + 4);
    if (k) key = std::atoi(k + 4);
    if (s) seq = std::atoi(s + 4);
}

// ---------------------------------------------------------------------------
// Produce `records` records across `keys` distinct keys at up to `rateHz`.
// Emits "PRODUCED ..." per acked record.  Returns #acked.
// ---------------------------------------------------------------------------
static int doProduce(Client& c, const TopicHandle& th, int cid, int records,
                     int keys, int rateHz, int timeoutMs, int32_t partitionCount) {
    ProducerConfig pcfg;
    pcfg.acks             = Acks::Leader;
    pcfg.lingerUs         = 1;
    pcfg.batchBytes       = 1;
    pcfg.maxInFlight      = 16;
    pcfg.produceTimeoutMs = static_cast<int64_t>(timeoutMs);
    auto p = c.producer(pcfg);

    const int64_t interNs =
        (rateHz > 0) ? static_cast<int64_t>(1'000'000'000LL / rateHz) : 0;
    auto nextSlot = std::chrono::steady_clock::now();

    int acked = 0;
    for (int seq = 0; seq < records; ++seq) {
        if (interNs > 0) {
            std::this_thread::sleep_until(nextSlot);
            nextSlot += std::chrono::nanoseconds(interNs);
        }
        const int keyIdx = keys > 0 ? (seq % keys) : 0;
        // Disjoint the key space across clients so concurrent producers never
        // collide on a key (keeps per-key ordering attributable per client).
        const uint64_t effKey =
            static_cast<uint64_t>(cid) * 1'000'000ULL + static_cast<uint64_t>(keyIdx);
        const std::string valStr = encodePayload(cid, keyIdx, seq);
        const std::vector<uint8_t> val(valStr.begin(), valStr.end());

        Key key(effKey);
        const int32_t part = key.partition(partitionCount);
        auto fut = p.send(th, key, std::span<const uint8_t>(val), want_future);
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        if (fut.wait_until(deadline) != std::future_status::ready) {
            std::cerr << "CLIENT: FAIL produce cid=" << cid << " seq=" << seq
                      << " TIMEOUT\n";
            return acked;
        }
        try {
            const int64_t offset = fut.get();
            std::cout << "PRODUCED cid=" << cid << " key=" << keyIdx
                      << " seq=" << seq << " part=" << part << " offset=" << offset << "\n";
            std::cout.flush();
            ++acked;
        } catch (const std::exception& ex) {
            std::cerr << "CLIENT: FAIL produce cid=" << cid << " seq=" << seq
                      << ": " << ex.what() << "\n";
            return acked;
        }
    }
    return acked;
}

// ---------------------------------------------------------------------------
// Subscribe from `from` and collect until `expect` records (or timeout).
// Emits "DELIVERED ..." per record.  Returns #delivered.
//
// `subPartitions`: number of partitions to subscribe to (0..subPartitions-1).
//   1 (default) = partition 0 only (backward-compatible single-partition mode).
//   >1           = subscribe to ALL partitions 0..subPartitions-1 so that the
//                  harness can assert assert_no_committed_loss across all of them.
//
// The agent-thread handler only COPIES the record into a queue (never blocks).
// A separate drain thread emits the DELIVERED lines and applies the optional
// slow-subscriber delay, so we model a slow consumer without violating the
// "handler must not block the agent thread" contract.
// ---------------------------------------------------------------------------
static int doSubscribe(Client& c, const TopicHandle& th, From from, int expect,
                       int windowMs, int slowMs, int expectCidMin,
                       int expectCidExact, int subPartitions) {
    struct Rec { int64_t offset; int32_t partition; std::string value; };
    std::mutex mtx;
    std::deque<Rec> q;
    std::atomic<int> delivered{0};

    SubscribeOptions opts;
    opts.from      = from;
    opts.isolation = Isolation::ReadUncommitted;

    // Subscribe to each partition and keep handles alive until we're done.
    // Thread the partition index into each handler so DELIVERED lines carry part=.
    const int nParts = (subPartitions > 0) ? subPartitions : 1;
    std::vector<Subscription> subs;
    subs.reserve(nParts);
    for (int part = 0; part < nParts; ++part) {
        subs.push_back(c.subscribe(th, part, opts, [&mtx, &q, part](const Record& r) {
            std::string v(reinterpret_cast<const char*>(r.value.data()), r.value.size());
            std::lock_guard<std::mutex> lk(mtx);
            q.push_back(Rec{r.offset, static_cast<int32_t>(part), std::move(v)});
        }));
    }

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(windowMs);
    while (std::chrono::steady_clock::now() < deadline) {
        Rec rec;
        bool have = false;
        {
            std::lock_guard<std::mutex> lk(mtx);
            if (!q.empty()) { rec = std::move(q.front()); q.pop_front(); have = true; }
        }
        if (have) {
            if (slowMs > 0) std::this_thread::sleep_for(std::chrono::milliseconds(slowMs));
            int cid, key, seq;
            decodePayload(rec.value, cid, key, seq);
            std::cout << "DELIVERED cid=" << cid << " key=" << key
                      << " seq=" << seq << " part=" << rec.partition
                      << " offset=" << rec.offset << "\n";
            std::cout.flush();
            // Only records from this run's cid range (>= expectCidMin), and if
            // expectCidExact >= 0 only that exact cid, count toward `expect`.
            // Lets a subscriber on a persistent/shared partition drain past
            // older records and (for multi-client runs) target just its OWN
            // records without racing other producers.
            const bool counts =
                cid >= expectCidMin &&
                (expectCidExact < 0 || cid == expectCidExact);
            if (counts) {
                const int d = delivered.fetch_add(1) + 1;
                if (expect > 0 && d >= expect) break;
            }
        } else {
            std::this_thread::sleep_for(2ms);
        }
    }

    // Drain anything still queued after the loop broke on `expect` — harmless.
    for (auto& sub : subs) sub.unsubscribe();
    return delivered.load();
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
    const std::string topicsStr    = getEnvStr("RUNE_TOPICS", "events:0:1");
    const int         timeoutMs    = getEnvInt("RUNE_TIMEOUT_MS", 10000);

    const std::string mode         = getEnvStr("RUNE_MODE", "produce_subscribe");
    const int         records      = getEnvInt("RUNE_RECORDS", 20);
    const int         keys         = std::max(1, getEnvInt("RUNE_KEYS", 1));
    const int         cid          = getEnvInt("RUNE_CLIENT_ID", 0);
    const int         rateHz       = getEnvInt("RUNE_RATE_HZ", 0);

    const From        from         = parseFrom(getEnvStr("RUNE_FROM", "earliest"));
    const int         subWindowMs  = getEnvInt("RUNE_SUBSCRIBE_MS", timeoutMs * 3);
    const int         slowMs       = getEnvInt("RUNE_SLOW_SUBSCRIBER_MS", 0);
    const int         expectDefault =
        (mode == "subscribe") ? 0 : records;
    const int         expect       = getEnvInt("RUNE_EXPECT", expectDefault);
    // Records with cid >= expectCidMin count toward `expect` (default: all).
    const int         expectCidMin = getEnvInt("RUNE_EXPECT_CID_MIN", -2147483647);
    // If >=0, ONLY records with this exact cid count toward `expect` (own-cid).
    const int         expectCidExact = getEnvInt("RUNE_EXPECT_CID_EXACT", -1);
    // RUNE_SUB_PARTITIONS: number of partitions to subscribe (0..N-1).
    // Default 1 = partition 0 only (backward-compatible).  Set to the topic's
    // partitionCount (from RUNE_TOPICS) to cover ALL partitions end-to-end.
    const int         subPartitions  = getEnvInt("RUNE_SUB_PARTITIONS", 1);

    if (target.empty() || egress.empty() || dataDir.empty()) {
        std::cerr << "CLIENT: RUNE_TARGET, RUNE_EGRESS and RUNE_DATA_DIR are required\n";
        return 1;
    }

    std::cerr << "CLIENT: cid=" << cid << " mode=" << mode
              << " target=" << target << " records=" << records
              << " keys=" << keys << " rateHz=" << rateHz
              << " from(wire)=" << from.toWire() << " expect=" << expect
              << " slowMs=" << slowMs << " subPartitions=" << subPartitions << "\n";

    ClientConfig cfg;
    cfg.gatewayChannel   = target;
    cfg.gatewayStreamId  = targetStream;
    cfg.egressChannel    = egress;
    cfg.egressStreamId   = egressStream;
    cfg.dataDir          = dataDir;
    cfg.connectTimeoutMs = static_cast<int64_t>(timeoutMs);
    cfg.pingTimeoutMs    = static_cast<int64_t>(timeoutMs);
    cfg.idleMode         = IdleMode::LowLatency;
    cfg.topics           = parseTopics(topicsStr);
    if (cfg.topics.empty()) {
        std::cerr << "CLIENT: no valid topics parsed from RUNE_TOPICS='" << topicsStr << "'\n";
        return 1;
    }

    int rc = 0;
    int produced = 0, delivered = 0;

    try {
        Client c(cfg);
        c.onError([](const std::string& msg) { std::cerr << "CLIENT: onError: " << msg << "\n"; });

        std::cerr << "CLIENT: connecting...\n";
        c.connect().get();
        std::cerr << "CLIENT: connected\n";

        // No settle window needed: DR-BUG-1 is fixed. Producer registration
        // is now safe to call at any time after Client construction via the
        // lock-free copy-on-write snapshot in ProducerHub.

        const TopicHandle th = c.topic(cfg.topics[0].name);
        if (!th.valid()) {
            std::cerr << "CLIENT: topic '" << cfg.topics[0].name << "' not valid\n";
            return 1;
        }

        const bool doP = (mode == "produce" || mode == "produce_subscribe");
        const bool doS = (mode == "subscribe" || mode == "produce_subscribe");

        if (doP) {
            produced = doProduce(c, th, cid, records, keys, rateHz, timeoutMs, th.partitionCount);
            if (produced < records) {
                std::cerr << "CLIENT: FAIL produced " << produced << "/" << records << "\n";
                rc = 1;
            }
        }

        if (doS && rc == 0) {
            delivered = doSubscribe(c, th, from, expect, subWindowMs, slowMs,
                                    expectCidMin, expectCidExact, subPartitions);
            if (expect > 0 && delivered < expect) {
                std::cerr << "CLIENT: FAIL delivered " << delivered << "/" << expect << "\n";
                rc = 1;
            }
        }

        std::cout << "SUMMARY produced=" << produced << " delivered=" << delivered
                  << " mode=" << mode << " rc=" << rc << "\n";
        std::cout.flush();
        c.close();
    } catch (const std::exception& ex) {
        std::cerr << "CLIENT: FATAL exception: " << ex.what() << "\n";
        std::cout << "SUMMARY produced=" << produced << " delivered=" << delivered
                  << " mode=" << mode << " rc=2\n";
        std::cout.flush();
        rc = 2;
    }
    return rc;
}
