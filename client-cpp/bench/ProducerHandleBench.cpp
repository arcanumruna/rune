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
//      HANDLE_BATCH_RECORDS.
#include <rune/client/Key.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>

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
    std::thread duty([&] {
        int64_t off = 0;
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
        }
        tEnd = clk::now();
        done.store(true, std::memory_order_release);
    });
    const std::vector<uint8_t> val(64, 0xAB);
    const auto t0 = clk::now();
    for (int64_t i = 0; i < N; ++i) {
        const Key         key(static_cast<uint64_t>(i));
        const TopicHandle topic{0, partitions};
        if (acked) {
            while (p.trySend(topic, key, val, AckToken{static_cast<Token>(i)}) != SendResult::Ok) {
                std::this_thread::yield();
            }
        } else {
            while (p.trySend(topic, key, val) != SendResult::Ok) std::this_thread::yield();
        }
    }
    while (!done.load(std::memory_order_acquire)) std::this_thread::yield();
    duty.join();
    const double secs = std::chrono::duration<double>(tEnd - t0).count();
    std::printf("RESULT mode=%s records=%lld secs=%.4f rec_per_s=%.0f ns_per_rec=%.2f\n", acked ? "acked" : "void",
                static_cast<long long>(N), secs, N / secs, secs * 1e9 / N);
}
