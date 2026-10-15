// examples/rune_produce_throughput.cpp — network produce THROUGHPUT load client.
//
//   With arguments: invoker-shard open-loop bench — see examples/bench/InvokerBench.h (--rate, --mode, ...).
//   Without arguments: the legacy env-driven agent-mode bench below (unchanged).
//
// Drives the REAL client → broker path over Aeron (UDP or IPC) at maximum rate,
// using the batched/pipelined Producer, and reports records/s + MB/s + (acked
// mode) latency.  This is the fair, network-footing counterpart to Kafka's
// kafka-producer-perf-test.
//
// Two modes (RUNE_BENCH_MODE):
//   void  (default) — fire-and-forget Producer::send(); measures the raw batched
//                     throughput the client can push (agent recordsSent/framesSent
//                     counters).  No per-send heap.  Light app-side pacing keeps
//                     the app→agent ring from overflowing (drops are reported).
//   acked           — bounded-outstanding callbacks; measures ACKED throughput
//                     with no drops (Kafka-faithful) + p50/p99 ack latency.
//
// Config (env):
//   RUNE_TARGET          gateway ingress channel, e.g. aeron:udp?endpoint=127.0.0.1:40200 (required)
//   RUNE_TARGET_STREAM   default 1
//   RUNE_EGRESS          this client's egress channel, e.g. aeron:udp?endpoint=127.0.0.1:40400 (required)
//   RUNE_EGRESS_STREAM   default 2
//   RUNE_DATA_DIR        Aeron client dir (required)
//   RUNE_CLIENT_TOPIC_TABLE          "name:id:partitions" (first topic is used), e.g. bench:0:8
//   RUNE_BENCH_RECORDS   total records to produce (default 2000000)
//   RUNE_BENCH_VALUE_BYTES  payload size (default 256)
//   RUNE_BENCH_KEYS      distinct keys (spreads across partitions; default = partitions)
//   RUNE_BENCH_WARMUP    warmup records discarded before timing (default 100000)
//   RUNE_BENCH_MODE      void | acked (default void)
//   RUNE_BENCH_WINDOW    acked mode: max outstanding records (default 200000)
//   RUNE_BENCH_MTU       client Aeron MTU (default 1408 = the library default and the
//                        broker's interface-derived MTU — backlog B5; 8192 =
//                        transport::kJumboMtuLength is OPT-IN for loopback / jumbo
//                        paths).  The library's RUNE_CLIENT_AERON_MTU env override
//                        still wins over this.
//   Producer knobs via ProducerConfig::fromEnv():
//     RUNE_ACKS RUNE_LINGER_US RUNE_BATCH_BYTES RUNE_BATCH_RECORDS RUNE_MAX_IN_FLIGHT

#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>
#include <rune_fr/FlightRecorder.h>

#include "BenchEnv.h"
#include "FrRun.h"
#include "InvokerBench.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace rune::client;

using rune::bench::envI64;
using rune::bench::envStr;
using rune::bench::parseTopics;

int main(int argc, char** argv) {
    if (std::getenv("RUNE_TOPICS") != nullptr) {
        std::cerr << "CLIENT: RUNE_TOPICS was renamed to RUNE_CLIENT_TOPIC_TABLE (the client's name:topicId:partitions table)\n";
        return 2;
    }
    // rune-fr: armed by RUNE_FR_ENABLE before any client thread starts; the
    // report is written to RUNE_FR_OUT on every exit path, in EVERY mode
    // (legacy void/acked AND the invoker open/saturate bench).
    rune::bench::FrRun fr(argc > 1 ? "invoker" : "agent");
    // Any argument selects the invoker-shard bench (spec §5); env-only runs keep the
    // legacy agent-mode path unchanged (VM scripts and the default-config A/B use it).
    if (argc > 1) return rune::bench::invokerBenchMain(argc, argv);
    const std::string target  = envStr("RUNE_TARGET");
    const std::string egress  = envStr("RUNE_EGRESS");
    const std::string dataDir = envStr("RUNE_DATA_DIR");
    const std::string topics  = envStr("RUNE_CLIENT_TOPIC_TABLE", "bench:0:8");
    if (target.empty() || egress.empty() || dataDir.empty()) {
        std::cerr << "RUNE_TARGET, RUNE_EGRESS, RUNE_DATA_DIR are required\n";
        return 2;
    }
    const int64_t nRecords = envI64("RUNE_BENCH_RECORDS", 2'000'000);
    const int32_t valueBytes = static_cast<int32_t>(envI64("RUNE_BENCH_VALUE_BYTES", 256));
    const int64_t warmup   = envI64("RUNE_BENCH_WARMUP", 100'000);
    const std::string mode = envStr("RUNE_BENCH_MODE", "void");
    const int64_t window   = envI64("RUNE_BENCH_WINDOW", 200'000);

    ClientConfig cfg;
    cfg.gatewayChannel  = target;
    cfg.gatewayStreamId = static_cast<int32_t>(envI64("RUNE_TARGET_STREAM", 1));
    cfg.egressChannel   = egress;
    cfg.egressStreamId  = static_cast<int32_t>(envI64("RUNE_EGRESS_STREAM", 2));
    cfg.dataDir         = dataDir;
    // Backlog B5: perf numbers count at the real-network MTU (1408, the library
    // default); jumbo (8192) is opt-in via RUNE_BENCH_MTU.
    cfg.mtuBytes        = static_cast<std::size_t>(envI64("RUNE_BENCH_MTU", 1408));
    {   // RUNE_IDLE_MODE: busyspin | lowlatency (default) | balanced
        const std::string im = envStr("RUNE_IDLE_MODE", "lowlatency");
        cfg.idleMode = (im == "busyspin") ? IdleMode::BusySpin
                     : (im == "balanced") ? IdleMode::Balanced
                                          : IdleMode::LowLatency;
    }
    cfg.topics          = parseTopics(topics);
    if (cfg.topics.empty()) { std::cerr << "no topics parsed\n"; return 2; }

    const int32_t partitions = cfg.topics[0].partitionCount;
    const int64_t keys = envI64("RUNE_BENCH_KEYS", partitions > 0 ? partitions : 1);

    ProducerConfig pcfg = ProducerConfig::fromEnv();
    // Acks + pipeline depth overrides (fromEnv only covers linger/batch).
    // For WAN throughput, acks=none (fire-and-forget streaming) + a deep
    // maxInFlight are what let batching hide the round-trip latency.
    if (const char* a = std::getenv("RUNE_BENCH_ACKS")) {
        const std::string am = a;
        pcfg.acks = (am == "leader") ? Acks::Leader
                  : (am == "all")    ? Acks::All
                                     : Acks::None;
    }
    pcfg.maxInFlight = static_cast<int32_t>(envI64("RUNE_BENCH_MAX_IN_FLIGHT", pcfg.maxInFlight));
    std::cerr << "THROUGHPUT bench: mode=" << mode
              << " records=" << nRecords << " value=" << valueBytes
              << " topic=" << cfg.topics[0].name << " partitions=" << partitions
              << " keys=" << keys
              << " acks=" << (int)pcfg.acks << " lingerUs=" << pcfg.lingerUs
              << " batchBytes=" << pcfg.batchBytes << " batchRecords=" << pcfg.batchRecords
              << " maxInFlight=" << pcfg.maxInFlight << "\n";

    const std::vector<uint8_t> val(static_cast<std::size_t>(valueBytes), uint8_t{0xAB});

    try {
        Client c(cfg);
        c.onError([](const std::string& m){ std::cerr << "onError: " << m << "\n"; });
        std::cerr << "connecting...\n";
        c.connectAndWait();
        std::cerr << "connected\n";
        const TopicHandle th = c.topic(cfg.topics[0].name);
        if (!th.valid()) { std::cerr << "topic invalid\n"; return 2; }

        auto p = c.producer(pcfg);

        // ---- Warmup (discarded) --------------------------------------------
        for (int64_t i = 0; i < warmup; ++i)
            p.send(th, Key(static_cast<uint64_t>(i % keys)), std::span<const uint8_t>(val));
        p.flush();
        std::this_thread::sleep_for(std::chrono::milliseconds(300));

        using clk = std::chrono::steady_clock;

        if (mode == "acked") {
            std::atomic<uint64_t> acked{0};      // resolved (success OR failure) — drives window
            std::atomic<uint64_t> succeeded{0};  // offset >= 0 (actually persisted)
            std::atomic<uint64_t> failed{0};     // offset < 0 (ring-full / broker error)
            std::atomic<uint64_t> sent{0};
            std::vector<int64_t> lat;                 // ack latency samples (ns), sparse
            lat.reserve(1 << 16);
            std::atomic<uint64_t> sampleGate{0};
            // F8c C6: acks arrive at the producer's onAck handler (a plain
            // function pointer, runner thread) — token = record index; the
            // send timestamp is published to the runner by the ring itself
            // (written before the send, read after its ack).
            std::vector<clk::time_point> sendTs(static_cast<std::size_t>(nRecords));
            struct AckCtx {
                std::atomic<uint64_t>*         acked;
                std::atomic<uint64_t>*         succeeded;
                std::atomic<uint64_t>*         failed;
                std::atomic<uint64_t>*         sampleGate;
                std::vector<int64_t>*          lat;
                std::vector<clk::time_point>*  sendTs;
            } actx{&acked, &succeeded, &failed, &sampleGate, &lat, &sendTs};
            p.onAck([](void* raw, Token tok, int64_t /*offset*/, OpStatus st, uint16_t) noexcept {
                auto* a = static_cast<AckCtx*>(raw);
                RUNE_FR_MARK_PRODUCE(rune_fr::Site::ClientDrvAckCb, static_cast<uint32_t>(tok));
                // `acked` is published LAST (release): main stops waiting once it
                // reaches nRecords and then reads succeeded/failed/lat and reuses
                // this frame's stack — every other write must happen-before it.
                if (st == OpStatus::Ok) {
                    a->succeeded->fetch_add(1, std::memory_order_relaxed);
                    // sample ~1/256 SUCCESSFUL acks for latency
                    if ((a->sampleGate->fetch_add(1, std::memory_order_relaxed) & 0xFF) == 0 &&
                        a->lat->size() < a->lat->capacity()) {
                        a->lat->push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(
                            clk::now() - (*a->sendTs)[static_cast<std::size_t>(tok)]).count());
                    }
                } else {
                    a->failed->fetch_add(1, std::memory_order_relaxed);
                }
                a->acked->fetch_add(1, std::memory_order_release);
            }, &actx);
            const auto t0 = clk::now();
            // DIAG: sample sent/acked every 250ms to see whether acks arrive as a
            // steady rate or in bursts (RUNE_BENCH_PROGRESS=1 to enable).
            std::atomic<bool> progDone{false};
            std::thread prog;
            if (std::getenv("RUNE_BENCH_PROGRESS")) {
                prog = std::thread([&]{
                    uint64_t lastA = 0, lastS = 0;
                    while (!progDone.load(std::memory_order_acquire)) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(250));
                        const uint64_t a = acked.load(), s = sent.load();
                        std::cerr << "  t+prog sent=" << s << " (+" << (s-lastS)*4
                                  << "/s) acked=" << a << " (+" << (a-lastA)*4
                                  << "/s) inflight=" << (s-a) << "\n";
                        lastA = a; lastS = s;
                    }
                });
            }
            // Window wait with a BACKOFF (spin → yield → short sleep), not a bare
            // yield loop: on a shared box a yield-spinning load generator holds a
            // scheduler slot the whole time the window is full, stealing CPU from the
            // brokers it measures (strace: ~14k sched_yield/s).  A 20µs sleep is
            // negligible against ms-scale ack round-trips.
            auto waitUntil = [](auto&& ready) {
                for (int n = 0; !ready(); ++n) {
                    if (n < 64)      { /* spin */ }
                    else if (n < 72) std::this_thread::yield();
                    else             std::this_thread::sleep_for(std::chrono::microseconds(20));
                }
            };
            for (int64_t i = 0; i < nRecords; ++i) {
                // bounded outstanding — backpressure like Kafka buffer.memory
                const auto submitTs = clk::now();
                waitUntil([&]{ return static_cast<int64_t>(i - acked.load(std::memory_order_acquire)) < window; });
                const auto sendTs1 = clk::now();
                // Driver: window cleared → send.  value = µs SPENT waiting on the
                // MIF window (ack starvation) — the driver's view of pipeline stall.
                const double windowWaitUs =
                    std::chrono::duration<double, std::micro>(sendTs1 - submitTs).count();
                RUNE_FR_MARK_VAL_PRODUCE(rune_fr::Site::ClientDrvSend,
                    static_cast<uint32_t>(i), windowWaitUs);
                sendTs[static_cast<std::size_t>(i)] = sendTs1;
                p.send(th, Key(static_cast<uint64_t>(i % keys)), std::span<const uint8_t>(val),
                       AckToken{static_cast<Token>(i)});
                sent.fetch_add(1, std::memory_order_release);
            }
            p.flush();
            waitUntil([&]{ return acked.load(std::memory_order_acquire) >= static_cast<uint64_t>(nRecords); });
            progDone.store(true, std::memory_order_release);
            if (prog.joinable()) prog.join();
            const auto t1 = clk::now();
            const double secs = std::chrono::duration<double>(t1 - t0).count();
            const uint64_t ok = succeeded.load(), bad = failed.load();
            // REAL durable throughput = SUCCEEDED records / wall time (offset>=0).
            const double rps = ok / secs;
            std::sort(lat.begin(), lat.end());
            auto pct = [&](double q){ return lat.empty() ? 0.0 :
                lat[std::min(lat.size()-1, (size_t)(lat.size()*q))] / 1000.0; };
            std::cout << "RESULT mode=acked attempted=" << nRecords
                      << " succeeded=" << ok << " failed=" << bad
                      << " secs=" << secs
                      << " records_per_sec=" << (uint64_t)rps
                      << " MB_per_sec=" << (rps * valueBytes / (1024.0*1024.0))
                      << " recordsSent=" << p.recordsSent()
                      << " framesSent=" << p.framesSent()
                      << " p50_us=" << pct(0.50) << " p99_us=" << pct(0.99)
                      << " p999_us=" << pct(0.999) << "\n";
        } else {
            // void fire-and-forget — measure agent-accepted throughput.
            uint64_t attempted = 0, dropped = 0;
            const uint64_t r0 = p.recordsSent();
            const uint64_t f0 = p.framesSent();
            const auto t0 = clk::now();
            for (int64_t i = 0; i < nRecords; ++i) {
                p.send(th, Key(static_cast<uint64_t>(i % keys)), std::span<const uint8_t>(val));
                ++attempted;
                // Bounded backpressure: let the agent catch up but NEVER hang.
                // Cap the spin so a stalled/gated agent can't wedge the fire loop.
                if ((attempted & 0xFFF) == 0) {
                    for (int spin = 0; spin < 200000; ++spin) {
                        if (attempted - (p.recordsSent() - r0) <= static_cast<uint64_t>(window)) break;
                        std::this_thread::yield();
                    }
                }
            }
            p.flush();
            // drain: wait until the agent stops making progress, or a hard cap.
            const auto drainDeadline = clk::now() + std::chrono::seconds(30);
            uint64_t last = 0; int stable = 0;
            while (stable < 40 && clk::now() < drainDeadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                const uint64_t sent = p.recordsSent() - r0;
                if (sent == last) ++stable; else { stable = 0; last = sent; }
                if (sent >= static_cast<uint64_t>(nRecords)) break;
            }
            const auto t1 = clk::now();
            const uint64_t recSent = p.recordsSent() - r0;
            const uint64_t frmSent = p.framesSent() - f0;
            dropped = attempted - recSent;
            const double secs = std::chrono::duration<double>(t1 - t0).count();
            const double rps = recSent / secs;
            std::cout << "RESULT mode=void attempted=" << attempted
                      << " recordsSent=" << recSent << " framesSent=" << frmSent
                      << " dropped=" << dropped
                      << " records_per_batch=" << (frmSent ? (double)recSent/frmSent : 0.0)
                      << " secs=" << secs
                      << " records_per_sec=" << (uint64_t)rps
                      << " MB_per_sec=" << (rps * valueBytes / (1024.0*1024.0)) << "\n";
        }
        fr.dump();   // the client-side flight-recorder report (also on every other exit)
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "FATAL: " << ex.what() << "\n";
        return 1;
    }
}
