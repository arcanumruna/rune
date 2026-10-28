// bench/ProducerHandleBench.cpp — F8c C7: the producer hot path THROUGH THE
// HANDLE (Producer::trySend), in isolation (no Aeron).  C7 changed what the
// handle holds (a raw single-owner ProducerImpl* instead of a shared_ptr): the
// send must still be one load of impl_ and the same instructions.
//
// App thread: N sends through the Producer handle — void (acks=None) or acked
// (AckToken: the tagged 64-bit token, onAck function pointer).  Duty thread:
// drain, flush to a sink (acked: capture the batch ids and ack each batch at
// once, so the per-record ack fan-out runs for every record).
// Env: HANDLE_MODE=void|acked, HANDLE_RECORDS, HANDLE_PARTITIONS,
//      HANDLE_BATCH_RECORDS, HANDLE_LAT_EVERY.
// #376: the result line also carries p50_ns / p99_ns, the latency of the app thread's Producer::trySend over every
// HANDLE_LAT_EVERY-th send (default 16; 1 = every send; back-pressure spins included), recorded into a vector sized
// before the timed loop (no allocation inside it).  Sampling keeps the clock reads off most sends, so ns_per_rec stays
// comparable with the pre-#376 baselines.
#include <rune/client/Key.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace rune::client;
using rune::client::producer::ProducerImpl;

static int64_t envI64(const char* n, int64_t d) {
    const char* v = std::getenv(n);
    return (v && *v) ? std::atoll(v) : d;
}

int main() {
    const char* modeEnv = std::getenv("HANDLE_MODE");
    const bool  acked   = modeEnv != nullptr && std::string(modeEnv) == "acked";
    const int64_t N          = envI64("HANDLE_RECORDS", 2'000'000);
    const int32_t partitions = static_cast<int32_t>(envI64("HANDLE_PARTITIONS", 8));
    ProducerConfig cfg;
    cfg.acks              = acked ? Acks::Leader : Acks::None;
    cfg.lingerUs          = 0;
    cfg.batchRecords      = static_cast<int32_t>(envI64("HANDLE_BATCH_RECORDS", 64));
    cfg.maxInFlight       = 32;
    cfg.deliveryTimeoutMs = 600'000;
    cfg.produceTimeoutMs  = 600'000;

#ifdef RUNE_BENCH_PARENT_SHARED_PTR
    auto          sp = std::make_shared<ProducerImpl>(cfg, 4 * 1024 * 1024);  // pre-C7 handle
    ProducerImpl& impl = *sp;
    Producer      p(sp);
#else
    auto*         raw = new ProducerImpl(cfg, 4 * 1024 * 1024);  // C7: the handle owns it (bare)
    ProducerImpl& impl = *raw;
    Producer      p(raw);
#endif

    std::vector<int64_t> corr;
    corr.reserve(1 << 16);
    const ProducerImpl::SendFn sink = [&corr, acked](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
        if (acked && n >= 24) {
            int64_t c = 0;
            std::memcpy(&c, b + 16, 8);
            corr.push_back(c);
        }
        return true;
    };
    uint64_t completed = 0;  // duty thread only
    impl.setOnAck([](void* c, Token, int64_t, OpStatus, uint16_t) noexcept { ++*static_cast<uint64_t*>(c); },
                  &completed);
    std::atomic<bool> done{false};
    using clk = std::chrono::steady_clock;
    clk::time_point tEnd;
    // C10 review: the duty loop gives up after kIdleCap consecutive passes without progress (nothing sent, nothing
    // acked) - a lost ack or a wedged window fails the bench (and its smoke ctest) fast instead of hanging it.
    constexpr int64_t kIdleCap = 20'000'000;
    std::thread duty([&] {
        int64_t  off = 0;
        uint64_t lastProgress = 0;
        int64_t  idle = 0;
        for (;;) {
            const int64_t now = clk::now().time_since_epoch().count();
            impl.drainRing(now, 256);
            impl.flushBatches(now, 1, sink);
            for (int64_t c : corr) {
                impl.onProduceResponse(c, off, 0);
                off += 64;
            }
            corr.clear();
            if (acked ? completed >= static_cast<uint64_t>(N) : impl.recordsSent() >= static_cast<uint64_t>(N)) break;
            const uint64_t progress = completed + impl.recordsSent();
            if (progress != lastProgress) {
                lastProgress = progress;
                idle         = 0;
            } else if (++idle > kIdleCap) {
                break;   // wedged: report it below, never hang
            }
        }
        tEnd = clk::now();
        done.store(true, std::memory_order_release);
    });
    const std::vector<uint8_t> val(64, 0xAB);
    const int64_t every = std::max<int64_t>(1, envI64("HANDLE_LAT_EVERY", 16));
    std::vector<int64_t> lat(static_cast<std::size_t>((N + every - 1) / every));  // sized before the timed loop
    std::size_t nLat = 0;
    const auto t0 = clk::now();
    for (int64_t i = 0; i < N; ++i) {
        const Key         key(static_cast<uint64_t>(i));
        const TopicHandle topic{0, partitions};
        const bool        sample = i % every == 0;
        const auto        s0     = sample ? clk::now() : clk::time_point{};
        bool wedged = false;
        while ((acked ? p.trySend(topic, key, val, AckToken{static_cast<Token>(i)}) : p.trySend(topic, key, val)) !=
               SendResult::Ok) {
            if (done.load(std::memory_order_acquire)) {   // the duty loop gave up
                wedged = true;
                break;
            }
            std::this_thread::yield();
        }
        if (wedged) break;
        if (sample) lat[nLat++] = std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - s0).count();
    }
    while (!done.load(std::memory_order_acquire)) std::this_thread::yield();
    duty.join();
    const double secs = std::chrono::duration<double>(tEnd - t0).count();
    std::sort(lat.begin(), lat.begin() + static_cast<std::ptrdiff_t>(nLat));
    const int64_t p50 = nLat > 0 ? lat[(nLat - 1) / 2] : 0;
    const int64_t p99 = nLat > 0 ? lat[static_cast<std::size_t>(0.99 * static_cast<double>(nLat - 1))] : 0;
    std::printf("RESULT mode=%s records=%lld secs=%.4f rec_per_s=%.0f ns_per_rec=%.2f p50_ns=%lld p99_ns=%lld lat_every=%lld\n",
                acked ? "acked" : "void", static_cast<long long>(N), secs, N / secs, secs * 1e9 / N,
                static_cast<long long>(p50), static_cast<long long>(p99), static_cast<long long>(every));
    if (nLat == 0 || impl.recordsSent() < static_cast<uint64_t>(N) || (acked && completed < static_cast<uint64_t>(N))) {
        std::fprintf(stderr, "FAIL: sent=%llu completed=%llu of %lld, latency samples=%zu\n",
                     static_cast<unsigned long long>(impl.recordsSent()), static_cast<unsigned long long>(completed),
                     static_cast<long long>(N), nLat);
        return 1;
    }
    return 0;
}
