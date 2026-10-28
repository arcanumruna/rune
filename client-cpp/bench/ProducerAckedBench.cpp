// bench/ProducerAckedBench.cpp — F8c C6: the per-record ACKED producer path in
// isolation (no Aeron).  App thread: N acked sends (AckToken: the tagged 64-bit
// token, onAck function pointer).  Duty thread: drain, flush to a sink that
// captures batch ids, ack each batch at once — the per-record ack fan-out runs
// for every record.  Env: ACKED_RECORDS, ACKED_PARTITIONS, ACKED_BATCH_RECORDS, ACKED_LAT_EVERY.
// #376: the result line also carries p50_ns / p99_ns, the latency of the app thread's trySubmit call (back-pressure
// spins included) over every ACKED_LAT_EVERY-th send (default 16; 1 = every send).  The samples go into a vector
// sized before the timed loop (no allocation inside it); sampling keeps the two clock reads off most sends, so
// ns_per_rec stays comparable with the pre-#376 baselines.
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/Key.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>
using namespace rune::client;
using rune::client::producer::ProducerImpl;
static int64_t envI64(const char* n, int64_t d) { const char* v = std::getenv(n); return (v && *v) ? std::atoll(v) : d; }
int main() {
    const int64_t N = envI64("ACKED_RECORDS", 2'000'000);
    const int32_t partitions = (int32_t)envI64("ACKED_PARTITIONS", 8);
    ProducerConfig cfg;
    cfg.acks = Acks::Leader; cfg.lingerUs = 0; cfg.batchRecords = (int32_t)envI64("ACKED_BATCH_RECORDS", 64);
    cfg.maxInFlight = 32; cfg.deliveryTimeoutMs = 600'000; cfg.produceTimeoutMs = 600'000;
    ProducerImpl impl(cfg, 4 * 1024 * 1024);
    std::vector<int64_t> corr; corr.reserve(1 << 16);
    const ProducerImpl::SendFn sink = [&corr](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
        if (n >= 24) { int64_t c = 0; std::memcpy(&c, b + 16, 8); corr.push_back(c); }
        return true;
    };
    uint64_t completed = 0;  // duty thread only
    std::atomic<bool> done{false};
    using clk = std::chrono::steady_clock;
    clk::time_point tEnd;
    impl.setOnAck([](void* c, Token, int64_t, OpStatus, uint16_t) noexcept { ++*static_cast<uint64_t*>(c); }, &completed);
    // C10 review: the duty loop gives up after kIdleCap consecutive passes without progress (nothing sent, nothing
    // acked) - a lost ack or a wedged window fails the bench (and its smoke ctest) fast instead of hanging it.
    constexpr int64_t kIdleCap = 20'000'000;
    std::thread duty([&] {
        int64_t  off = 0;
        uint64_t lastProgress = 0;
        int64_t  idle = 0;
        while (completed < (uint64_t)N) {
            const int64_t now = clk::now().time_since_epoch().count();
            impl.drainRing(now, 256);
            impl.flushBatches(now, 1, sink);
            for (int64_t c : corr) { impl.onProduceResponse(c, off, 0); off += 64; }
            corr.clear();
            const uint64_t progress = completed + impl.recordsSent();
            if (progress != lastProgress) { lastProgress = progress; idle = 0; }
            else if (++idle > kIdleCap) break;   // wedged: report it below, never hang
        }
        tEnd = clk::now();
        done.store(true, std::memory_order_release);
    });
    const std::vector<uint8_t> val(64, 0xAB);
    const int64_t every = std::max<int64_t>(1, envI64("ACKED_LAT_EVERY", 16));
    std::vector<int64_t> lat(static_cast<std::size_t>((N + every - 1) / every));  // sized before the timed loop
    std::size_t nLat = 0;
    const auto t0 = clk::now();
    for (int64_t i = 0; i < N; ++i) {
        Key key((uint64_t)i);
        const int64_t h = static_cast<int64_t>(rune::client::producer::ackhandle::forHandler(static_cast<uint64_t>(i)));
        const bool sample = i % every == 0;
        const auto s0 = sample ? clk::now() : clk::time_point{};
        bool wedged = false;
        while (impl.trySubmit(0, (int32_t)(i % partitions), key, val, h) != SendResult::Ok) {
            if (done.load(std::memory_order_acquire)) { wedged = true; break; }   // the duty loop gave up
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
    std::printf("RESULT acked_records=%lld secs=%.4f rec_per_s=%.0f ns_per_rec=%.2f p50_ns=%lld p99_ns=%lld lat_every=%lld\n",
                (long long)N, secs, N / secs, secs * 1e9 / N, (long long)p50, (long long)p99, (long long)every);
    if (completed != (uint64_t)N || nLat == 0) {  // every record acked exactly once, and a latency was measured
        std::fprintf(stderr, "FAIL: completed=%llu of %lld, latency samples=%zu\n", (unsigned long long)completed,
                     (long long)N, nLat);
        return 1;
    }
    return 0;
}
