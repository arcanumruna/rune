// bench/ProducerAccumBench.cpp — isolate the client accumulator engine.
//
// Measures the throughput of the Producer's agent-side path
// (drainRing → flushBatches → encode → offer) with a COUNTING SINK — no Aeron,
// no gateway, no network. The client is the only thing under test, so this is
// the fastest loop for optimizing the batching engine (e.g. the drain-one +
// scan-all-partitions-per-record cost).
//
// Two threads (realistic): an app thread fires submitSendRecord() with NO drops
// (retries on ring-full); an agent thread runs the exact ProducerHub drain+flush
// loop against a sink that just counts frames/bytes and returns accept.
//
// Env:
//   ACCUM_RECORDS      total records (default 5000000)
//   ACCUM_VALUE_BYTES  payload size (default 256)
//   ACCUM_PARTITIONS   distinct partitions (default 8)
//   ACCUM_RING_CAP     producer ring usable bytes, pow2 (default 4194304 = 4MiB)
//   ACCUM_TIER2        1 = send Tier-2 batches (BatchBuilder: add x ACCUM_T2_RECORDS + send) instead of
//                      single records; ACCUM_RECORDS still counts records (default 0 = off)
//   ACCUM_T2_RECORDS   records per Tier-2 batch (default 8)
//   Producer knobs: RUNE_LINGER_US, RUNE_BATCH_BYTES, RUNE_BATCH_RECORDS,
//                   RUNE_MAX_IN_FLIGHT, RUNE_ACKS (via ProducerConfig::fromEnv)

#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/CountingSink.h>
#include <rune/client/Key.h>
#include <rune/client/producer/BatchBuilder.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <string>
#include <type_traits>
#include <thread>
#include <vector>

using namespace rune::client;
using rune::client::producer::ProducerImpl;

static int64_t envI64(const char* n, int64_t d) {
    const char* v = std::getenv(n); return (v && *v) ? std::atoll(v) : d;
}

// true if add() refused the record (pool exhausted); the older void add() never refuses.
template <class B>
static bool addRefused(B& bb, std::span<const uint8_t> v) {
    if constexpr (std::is_void_v<decltype(bb.add(v))>) { bb.add(v); return false; }
    else return bb.add(v) != OpResult::Accepted;
}

int main() {
    const int64_t N          = envI64("ACCUM_RECORDS", 5'000'000);
    const int32_t valueBytes = (int32_t)envI64("ACCUM_VALUE_BYTES", 256);
    const int32_t partitions = (int32_t)envI64("ACCUM_PARTITIONS", 8);
    const int32_t ringCap    = (int32_t)envI64("ACCUM_RING_CAP", 4 * 1024 * 1024);
    const int32_t topicId    = 0;

    ProducerConfig cfg = ProducerConfig::fromEnv();
    // Engine micro-bench: default acks=None so batching triggers purely on
    // size/linger (no inFlight ack-gating, which would need a simulated broker).
    // ACCUM_ACKS=leader|all|none to override; ACCUM_MAX_IN_FLIGHT for gated modes.
    {
        const char* a = std::getenv("ACCUM_ACKS");
        std::string am = a ? std::string(a) : "none";
        cfg.acks = (am == "leader") ? Acks::Leader
                 : (am == "all")    ? Acks::All
                                    : Acks::None;
        cfg.maxInFlight = (int32_t)envI64("ACCUM_MAX_IN_FLIGHT", cfg.maxInFlight);
    }
    std::cerr << "ACCUM bench: records=" << N << " value=" << valueBytes
              << " partitions=" << partitions << " ringCap=" << ringCap
              << " acks=" << (int)cfg.acks << " lingerUs=" << cfg.lingerUs
              << " batchBytes=" << cfg.batchBytes << " batchRecords=" << cfg.batchRecords
              << " maxInFlight=" << cfg.maxInFlight << "\n";

    ProducerImpl impl(cfg, ringCap);

    const bool concrete = std::getenv("ACCUM_SINK") == nullptr ||
                          std::string(std::getenv("ACCUM_SINK")) != "function";
    uint64_t frames = 0, bytes = 0;   // agent-thread only; read after join()
    const rune::client::producer::CountingSink csink{&frames, &bytes};
    const ProducerImpl::SendFn fsink = csink;  // ACCUM_SINK=function: same work via std::function
    auto flush = [&](int64_t nowNs) {
        return concrete ? impl.flushBatches(nowNs, /*sessionId=*/1, csink)
                        : impl.flushBatches(nowNs, /*sessionId=*/1, fsink);
    };

    const std::vector<uint8_t> val((std::size_t)valueBytes, uint8_t{0xAB});
    std::atomic<bool> appDone{false};

    using clk = std::chrono::steady_clock;

    // Agent thread — replicate ProducerHub::drainAndFlush, driven by REAL
    // wall-clock time so linger behaves exactly as in production.
    auto wallNs = []() -> int64_t {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            clk::now().time_since_epoch()).count();
    };
    std::thread agent([&]{
        for (;;) {
            int work = 0;
            // F12: same loop as ProducerHub::drainAndFlush — drain a whole pass,
            // flush once (dirty partitions only), re-drain only at a boundary.
            int budget = 256;
            for (int it = 0; it < 256; ++it) {
                const int64_t nowNs = wallNs();
                const int d = impl.drainRing(nowNs, budget);
                const int f = flush(nowNs);
                work += d + f;
                budget -= d;
                const auto stop = impl.lastDrainStop();
                if (budget <= 0 || stop == ProducerImpl::DrainStop::Drained) break;
                if (stop == ProducerImpl::DrainStop::Blocked && f == 0) break;
            }
            // idle flush so linger-based flushes still fire when the ring is empty
            flush(wallNs());
            if (appDone.load(std::memory_order_acquire) &&
                impl.recordsSent() >= (uint64_t)N && work == 0)
                break;
        }
    });

    const auto t0 = clk::now();
    const bool    tier2  = envI64("ACCUM_TIER2", 0) != 0;
    const int32_t t2Recs = (int32_t)envI64("ACCUM_T2_RECORDS", 8);
    if (tier2) {
        // Tier-2: one explicit batch per iteration.  A full slot pool (HEAD) back-pressures
        // add(): rebuild the batch until it is accepted.  (Compiles against both the
        // void-add() and the OpResult-add() BatchBuilder, to compare commits.)
        for (int64_t i = 0; i < N; i += t2Recs) {
            const int32_t part = (int32_t)((i / t2Recs) % partitions);
            for (;;) {
                BatchBuilder bb(&impl, topicId, part);
                bool full = false;
                for (int32_t r = 0; r < t2Recs && !full; ++r) {
                    full = addRefused(bb, std::span<const uint8_t>(val));
                }
                if (!full && bb.send() == OpResult::Accepted) break;
                std::this_thread::yield();
            }
        }
    } else
    for (int64_t i = 0; i < N; ++i) {
        const int32_t part = (int32_t)(i % partitions);
        Key key((uint64_t)i);
        while (!impl.submitSendRecord(topicId, part, key,
                                      std::span<const uint8_t>(val),
                                      /*wantAck=*/false, /*corrId=*/0)) {
            std::this_thread::yield();      // ring full: retry, never drop
        }
    }
    // Wait until every record is drained + batched NATURALLY (recordsFull/size).
    // Do NOT submitFlush() here — flush() means "send everything now" and would
    // force the still-buffered ring tail into size-1 batches, corrupting the
    // batched-throughput measurement.  Timing ends when all N are drained.
    while (impl.recordsSent() < (uint64_t)N) std::this_thread::yield();
    const auto t1 = clk::now();
    impl.submitFlush();                     // flush residual AFTER timing
    appDone.store(true, std::memory_order_release);
    agent.join();

    const double secs = std::chrono::duration<double>(t1 - t0).count();
    const uint64_t rec = impl.recordsSent();
    const uint64_t frm = impl.framesSent();
    const double rps = rec / secs;
    std::cout << "RESULT records=" << rec << " frames=" << frm
              << " records_per_batch=" << (frm ? (double)rec / frm : 0.0)
              << " secs=" << secs
              << " records_per_sec=" << (uint64_t)rps
              << " MB_per_sec=" << (rps * valueBytes / (1024.0 * 1024.0)) << "\n";
    return 0;
}
