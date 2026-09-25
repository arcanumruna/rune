// examples/rune_consume_throughput.cpp — subscribe/read THROUGHPUT load client.
//
// The read-side counterpart to rune_produce_throughput.  Subscribes to ALL
// partitions of a topic and counts delivered records, reporting records/s +
// MB/s.  Two modes (RUNE_BENCH_MODE):
//
//   catchup (default) — From::earliest: drain a pre-produced backlog of
//                       RUNE_BENCH_RECORDS records as fast as the broker can
//                       push them.  Measures pure catch-up read throughput
//                       (the offset-index binary-search + segment scan path).
//                       Timer runs from first record to the last of N.
//   tail             — From::latest: subscribe at LEO and read while a SEPARATE
//                       producer writes concurrently.  Measures sustained
//                       tailing throughput + end-to-end lag (produce ts → deliver).
//                       Runs for RUNE_BENCH_DURATION_S seconds.
//
// Config (env), shared with the produce bench where sensible:
//   RUNE_TARGET / RUNE_TARGET_STREAM   gateway ingress channel (required)
//   RUNE_EGRESS / RUNE_EGRESS_STREAM   this client's egress channel (required)
//   RUNE_DATA_DIR                      Aeron client dir (required)
//   RUNE_TOPICS                        "name:id:partitions" (first topic used)
//   RUNE_BENCH_MODE                    catchup | tail   (default catchup)
//   RUNE_BENCH_RECORDS                 catchup: records to drain (default 1000000)
//   RUNE_BENCH_DURATION_S              tail: seconds to run (default 10)
//   RUNE_BENCH_VALUE_BYTES             payload size for MB/s calc (default 256)
//   RUNE_BENCH_FROM                    earliest | latest | offset:N (overrides mode default)
//   RUNE_BENCH_IDLE_MS                 catchup: stop after this long with no new records (default 3000)

#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/subscriber/Subscription.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace rune::client;

static std::string envStr(const char* n, const std::string& d = {}) {
    const char* v = std::getenv(n); return (v && *v) ? std::string(v) : d;
}
static int64_t envI64(const char* n, int64_t d) {
    const char* v = std::getenv(n); return (v && *v) ? std::atoll(v) : d;
}

static std::vector<TopicSpec> parseTopics(const std::string& s) {
    std::vector<TopicSpec> out;
    std::size_t pos = 0;
    while (pos <= s.size()) {
        const std::size_t comma = s.find(',', pos);
        const std::string tok = s.substr(pos, (comma == std::string::npos ? s.size() : comma) - pos);
        pos = (comma == std::string::npos ? s.size() + 1 : comma + 1);
        if (tok.empty()) continue;
        const auto c1 = tok.find(':'); if (c1 == std::string::npos) continue;
        const auto c2 = tok.find(':', c1 + 1); if (c2 == std::string::npos) continue;
        TopicSpec ts;
        ts.name = tok.substr(0, c1);
        ts.topicId = std::atoi(tok.c_str() + c1 + 1);
        ts.partitionCount = std::atoi(tok.c_str() + c2 + 1);
        out.push_back(std::move(ts));
    }
    return out;
}

int main() {
    const std::string target  = envStr("RUNE_TARGET");
    const std::string egress  = envStr("RUNE_EGRESS");
    const std::string dataDir = envStr("RUNE_DATA_DIR");
    const std::string topics  = envStr("RUNE_TOPICS", "bench:0:8");
    if (target.empty() || egress.empty() || dataDir.empty()) {
        std::cerr << "RUNE_TARGET, RUNE_EGRESS, RUNE_DATA_DIR are required\n";
        return 2;
    }
    const std::string mode      = envStr("RUNE_BENCH_MODE", "catchup");
    const int64_t nRecords      = envI64("RUNE_BENCH_RECORDS", 1'000'000);
    const int64_t durationS     = envI64("RUNE_BENCH_DURATION_S", 10);
    const int32_t valueBytes    = static_cast<int32_t>(envI64("RUNE_BENCH_VALUE_BYTES", 256));
    const int64_t idleMs        = envI64("RUNE_BENCH_IDLE_MS", 3000);

    ClientConfig cfg;
    cfg.gatewayChannel  = target;
    cfg.gatewayStreamId = static_cast<int32_t>(envI64("RUNE_TARGET_STREAM", 1));
    cfg.egressChannel   = egress;
    cfg.egressStreamId  = static_cast<int32_t>(envI64("RUNE_EGRESS_STREAM", 2));
    cfg.dataDir         = dataDir;
    cfg.idleMode        = IdleMode::LowLatency;
    cfg.topics          = parseTopics(topics);
    if (cfg.topics.empty()) { std::cerr << "no topics parsed\n"; return 2; }

    const int32_t partitions = cfg.topics[0].partitionCount;

    // Resolve the starting point: RUNE_BENCH_FROM overrides the mode default.
    From from = (mode == "tail") ? From::latest() : From::earliest();
    if (const std::string f = envStr("RUNE_BENCH_FROM"); !f.empty()) {
        if (f == "earliest")            from = From::earliest();
        else if (f == "latest")         from = From::latest();
        else if (f.rfind("offset:", 0) == 0) from = From::offset(std::atoll(f.c_str() + 7));
    }

    std::cerr << "CONSUME bench: mode=" << mode
              << " topic=" << cfg.topics[0].name << " partitions=" << partitions
              << " from=" << (int)from.kind
              << (mode == "tail" ? (" durationS=" + std::to_string(durationS))
                                 : (" records=" + std::to_string(nRecords)))
              << " value=" << valueBytes << "\n";

    // Shared counters (written on the agent thread from RecordHandler).
    std::atomic<uint64_t> recs{0};
    std::atomic<uint64_t> bytes{0};
    std::atomic<int64_t>  lagSumNs{0};   // tail mode: sum of (nowNs - record.timestamp)
    std::atomic<uint64_t> lagSamples{0};

    using clk = std::chrono::steady_clock;

    try {
        Client c(cfg);
        c.onError([](const std::string& m){ std::cerr << "onError: " << m << "\n"; });
        std::cerr << "connecting...\n";
        c.connect().get();
        std::cerr << "connected\n";
        const TopicHandle th = c.topic(cfg.topics[0].name);
        if (!th.valid()) { std::cerr << "topic invalid\n"; return 2; }

        auto wallNowNs = []() -> int64_t {
            return std::chrono::duration_cast<std::chrono::nanoseconds>(
                       std::chrono::system_clock::now().time_since_epoch()).count();
        };

        RecordHandler handler = [&](const Record& r) {
            recs.fetch_add(1, std::memory_order_relaxed);
            bytes.fetch_add(r.value.size(), std::memory_order_relaxed);
            if (mode == "tail" && r.timestamp > 0) {
                const int64_t lag = wallNowNs() - r.timestamp;
                if (lag > 0 && lag < 60'000'000'000LL) { // ignore absurd/negative
                    lagSumNs.fetch_add(lag, std::memory_order_relaxed);
                    lagSamples.fetch_add(1, std::memory_order_relaxed);
                }
            }
        };

        SubscribeOptions opts;
        opts.from      = from;
        opts.isolation = Isolation::ReadUncommitted;

        std::vector<Subscription> subs;
        subs.reserve(static_cast<std::size_t>(partitions));
        for (int32_t p = 0; p < partitions; ++p) {
            subs.emplace_back(c.subscribe(th, p, opts, handler));
        }
        std::cerr << "subscribed " << partitions << " partitions\n";

        if (mode == "tail") {
            // Warm a moment so subscriptions are established, then measure a window.
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            const uint64_t r0 = recs.load(), b0 = bytes.load();
            const auto t0 = clk::now();
            std::this_thread::sleep_for(std::chrono::seconds(durationS));
            const auto t1 = clk::now();
            const uint64_t r = recs.load() - r0, b = bytes.load() - b0;
            const double secs = std::chrono::duration<double>(t1 - t0).count();
            const double rps = r / secs;
            const uint64_t ls = lagSamples.load();
            const double avgLagUs = ls ? (double)lagSumNs.load() / ls / 1000.0 : 0.0;
            std::cout << "RESULT mode=tail secs=" << secs
                      << " records=" << r
                      << " records_per_sec=" << (uint64_t)rps
                      << " MB_per_sec=" << (b / (1024.0*1024.0) / secs)
                      << " avg_lag_us=" << avgLagUs << "\n";
        } else {
            // catchup: wait for first record (bounded), then time to drain.
            const auto waitStart = clk::now();
            while (recs.load(std::memory_order_acquire) == 0) {
                if (clk::now() - waitStart > std::chrono::seconds(10)) {
                    std::cerr << "no records delivered within 10s — aborting\n";
                    std::cout << "RESULT mode=catchup secs=0 records=0 records_per_sec=0"
                                 " MB_per_sec=0 NO_RECORDS\n";
                    for (auto& s : subs) s.unsubscribe();
                    return 3;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            const auto t0 = clk::now();
            uint64_t last = 0; auto lastProgress = clk::now();
            for (;;) {
                const uint64_t r = recs.load(std::memory_order_acquire);
                if (r >= static_cast<uint64_t>(nRecords)) break;
                if (r != last) { last = r; lastProgress = clk::now(); }
                else if (clk::now() - lastProgress > std::chrono::milliseconds(idleMs)) {
                    std::cerr << "idle stop: no new records for " << idleMs << "ms\n";
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            const auto t1 = clk::now();
            const uint64_t r = recs.load(), b = bytes.load();
            const double secs = std::chrono::duration<double>(t1 - t0).count();
            const double rps = secs > 0 ? r / secs : 0;
            std::cout << "RESULT mode=catchup secs=" << secs
                      << " records=" << r
                      << " records_per_sec=" << (uint64_t)rps
                      << " MB_per_sec=" << (b / (1024.0*1024.0) / (secs > 0 ? secs : 1)) << "\n";
        }

        for (auto& s : subs) s.unsubscribe();
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "FATAL: " << ex.what() << "\n";
        return 1;
    }
}
