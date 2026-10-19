// bench/M2OClaimBench.cpp — M2ORingBuffer tryClaim/commit micro-benchmark.
//
// F8c C1(a): the producer hot path (Producer::trySend in Thread mode) is
// tryClaim() + an in-place write + commit() on the producer's M2ORingBuffer.
// This isolates exactly that, so the linearizable tail-bit close can be compared
// before/after without the batching engine, Aeron or a network in the way.
//
// Modes (CLAIM_MODE):
//   single  (default) — one thread: claim + write + commit, draining the ring
//                       with read() whenever a claim reports full.  Reports
//                       ns per claim+commit (the uncontended CAS path).
//   mpsc              — CLAIM_PRODUCERS app threads claim/commit concurrently,
//                       one consumer thread drains with read().  Reports total
//                       Mrec/s and ns/record (contended CAS on the tail word).
// Env:
//   CLAIM_RECORDS    records per run (default 20000000 single / 4000000 mpsc per producer)
//   CLAIM_VALUE      payload bytes (default 64 — the record size the ring sees)
//   CLAIM_PRODUCERS  producer threads in mpsc mode (default 2)
//   CLAIM_RING_CAP   ring usable bytes, pow2 (default 4 MiB)
//   CLAIM_REPS       repetitions; the best and median are printed (default 5)
//
// Bench-local code: std::atomic is used only for the start barrier / done count
// of the harness (not part of the measured ring).

#include <rune/client/detail/M2ORingBuffer.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using rune::client::detail::M2ORingBuffer;

namespace {

int64_t envI64(const char* n, int64_t d) {
    const char* v = std::getenv(n);
    return (v != nullptr && *v != 0) ? std::atoll(v) : d;
}

struct Ring {
    explicit Ring(std::size_t cap)
        : bytes(cap + static_cast<std::size_t>(M2ORingBuffer::trailerLength()))
        , mem(static_cast<uint8_t*>(std::aligned_alloc(64, bytes)), &std::free)
        , ring((std::memset(mem.get(), 0, bytes), mem.get()), bytes) {}
    std::size_t bytes;
    std::unique_ptr<uint8_t, decltype(&std::free)> mem;
    M2ORingBuffer ring;
};

double nowSec() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

// One run of single-threaded claim+commit.  Returns ns per record.
double runSingle(int64_t n, std::size_t value, std::size_t cap) {
    Ring r(cap);
    uint64_t sink = 0;
    auto drain = [&] {
        r.ring.read([&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                        aeron::util::index_t len) { sink += b.buffer()[off] + static_cast<uint64_t>(len); });
    };
    const double t0 = nowSec();
    for (int64_t i = 0; i < n; ++i) {
        aeron::util::index_t idx;
        while ((idx = r.ring.tryClaim(1, value)) < 0) drain();
        std::memset(r.ring.base() + idx, static_cast<int>(i), value);
        r.ring.commit(idx);
    }
    const double t1 = nowSec();
    drain();
    if (sink == 42) std::puts("");  // keep the reads alive
    return (t1 - t0) * 1e9 / static_cast<double>(n);
}

// One run of P producers + 1 consumer.  Returns ns per record (wall / total).
double runMpsc(int producers, int64_t perProducer, std::size_t value, std::size_t cap) {
    Ring r(cap);
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::atomic<int> done{0};
    const int64_t total = perProducer * producers;
    int64_t consumed = 0;
    std::thread consumer([&] {
        ready.fetch_add(1);
        while (!go.load(std::memory_order_acquire)) {}
        while (consumed < total) {
            consumed += r.ring.read([](int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                                       aeron::util::index_t) {});
        }
    });
    std::vector<std::thread> ps;
    for (int p = 0; p < producers; ++p) {
        ps.emplace_back([&, p] {
            ready.fetch_add(1);
            while (!go.load(std::memory_order_acquire)) {}
            for (int64_t i = 0; i < perProducer; ++i) {
                aeron::util::index_t idx;
                while ((idx = r.ring.tryClaim(1, value)) < 0) {}
                std::memset(r.ring.base() + idx, p, value);
                r.ring.commit(idx);
            }
            done.fetch_add(1);
        });
    }
    while (ready.load() != producers + 1) {}
    const double t0 = nowSec();
    go.store(true, std::memory_order_release);
    for (auto& t : ps) t.join();
    consumer.join();
    const double t1 = nowSec();
    return (t1 - t0) * 1e9 / static_cast<double>(total);
}

} // namespace

int main() {
    const std::string mode = std::getenv("CLAIM_MODE") ? std::getenv("CLAIM_MODE") : "single";
    const auto value     = static_cast<std::size_t>(envI64("CLAIM_VALUE", 64));
    const auto cap       = static_cast<std::size_t>(envI64("CLAIM_RING_CAP", 4 * 1024 * 1024));
    const int  reps      = static_cast<int>(envI64("CLAIM_REPS", 5));
    const int  producers = static_cast<int>(envI64("CLAIM_PRODUCERS", 2));
    std::vector<double> ns;
    for (int i = 0; i < reps; ++i) {
        ns.push_back(mode == "mpsc"
                         ? runMpsc(producers, envI64("CLAIM_RECORDS", 4'000'000), value, cap)
                         : runSingle(envI64("CLAIM_RECORDS", 20'000'000), value, cap));
    }
    std::sort(ns.begin(), ns.end());
    std::printf("M2OClaimBench mode=%s value=%zu producers=%d best=%.2f ns/rec median=%.2f ns/rec "
                "(%.1f Mrec/s best)\n",
                mode.c_str(), value, mode == "mpsc" ? producers : 1, ns.front(), ns[ns.size() / 2],
                1e3 / ns.front());
    return 0;
}
