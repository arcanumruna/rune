// bench/ProducerClaimBench.cpp — Phase 2b Part B gate: per-record cost and
// allocations of the four produce forms, in-process (no Aeron):
//   send        Producer::trySend(topic, key, span)    — encoded in a caller buffer
//   claim       Producer::tryClaim + PositionEncoder    — encoded in place
//   typed_send  TypedTopic<Position>::send(rec)         — struct -> reused buffer -> send
//   typed_claim TypedTopic<Position>::claim(key)        — encoded in place
// CLAIM_INGRESS=thread  (default): this thread produces, a duty thread drains
//                        the producer ring and flushes (Thread mode);
//               invoker : one thread, Direct ingress, a flush pass every 64
//                        records (Invoker mode; the pass is not timed).
// Per-record latency = the app-side call(s) for one record including any
// back-pressure retry (steady_clock around each record).  Under Rosetta the
// clock ticks every ~41.7 ns, so p50/p99 are quantised; a second, amortised
// phase therefore produces another N records with NO per-record clock reads and
// times blocks of CLAIM_BLOCK (default 1024) records: blk_mean_ns /
// blk_median_ns = ns per record over the blocks (Invoker blocks include the
// owner's every-64 flush pass — the owner pays it).  Allocations = global
// operator new on THIS thread after a 10'000-record warm-up, over both phases.
// Exit 1 if a claim form allocated (spec B: claim paths 0 allocations in
// steady state).
#include "../tests/support/AllocCounter.h"
#include "gen/position.gen.h"

#include <rune/client/Key.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/producer/CountingSink.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

using namespace rune::client;
using rune::client::producer::CountingSink;
using rune::client::producer::ProducerImpl;
using rune::gen::Position;
using rune::gen::PositionEncoder;
using clk = std::chrono::steady_clock;

namespace {
int64_t envI64(const char* n, int64_t d) {
    const char* v = std::getenv(n);
    return (v != nullptr && *v != '\0') ? std::atoll(v) : d;
}
std::string envStr(const char* n, const char* d) {
    const char* v = std::getenv(n);
    return (v != nullptr && *v != '\0') ? std::string(v) : std::string(d);
}
int64_t nowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now().time_since_epoch()).count();
}
enum class Mode { Send, Claim, TypedSend, TypedClaim };
template <class Enc>
void encodeInto(Enc& e, int64_t i) {
    e.zone(static_cast<int32_t>(i & 7)).x(1.5).y(2.5).z(-3.5).seq(static_cast<uint32_t>(i))
        .tick(static_cast<uint64_t>(i)).alive(true).kind(1).tag("bench");
}
}  // namespace

int main() {
    const std::string modeName = envStr("CLAIM_MODE", "claim");
    const Mode mode = modeName == "send"       ? Mode::Send
                    : modeName == "typed_send" ? Mode::TypedSend
                    : modeName == "typed_claim" ? Mode::TypedClaim
                                               : Mode::Claim;
    const bool    invoker    = envStr("CLAIM_INGRESS", "thread") == "invoker";
    const int64_t N          = envI64("CLAIM_RECORDS", 1'000'000);
    const int32_t partitions = static_cast<int32_t>(envI64("CLAIM_PARTITIONS", 8));
    constexpr int64_t kWarm  = 10'000;
    if (N <= kWarm) {
        std::fprintf(stderr, "CLAIM_RECORDS must exceed the %lld-record warm-up\n", static_cast<long long>(kWarm));
        return 2;
    }

    ProducerConfig cfg;
    cfg.acks         = Acks::None;
    cfg.lingerUs     = 0;
    cfg.batchRecords = 64;
    cfg.maxInFlight  = 32;
    auto* raw = new ProducerImpl(cfg, 32 * 1024 * 1024);  // the default producer ring
    ProducerImpl& impl = *raw;
    if (invoker) impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
    const TopicHandle topic{0, partitions};
    std::optional<Producer>             plain;
    std::optional<TypedTopic<Position>> typed;
    if (mode == Mode::TypedSend || mode == Mode::TypedClaim) {
        typed.emplace(Producer{raw}, topic, uint16_t{1});
    } else {
        plain.emplace(raw);
    }

    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    std::atomic<bool> stop{false};
    std::thread duty;
    if (!invoker) {
        duty = std::thread([&] {
            while (!stop.load(std::memory_order_acquire)) {
                const int64_t t = nowNs();
                impl.publishHeartbeat(t);  // a blocking typed send() waits on this, never times out
                impl.drainRing(t, 256);
                impl.flushBatches(t, 1, sink);
            }
            for (int i = 0; i < 4096; ++i) {  // drain what is left
                const int64_t t = nowNs();
                impl.drainRing(t, 256);
                impl.flushBatches(t, 1, sink);
            }
        });
    }
    auto relieve = [&] {  // back-pressured: Invoker flushes itself, Thread mode yields to the duty thread
        if (invoker) impl.flushBatches(nowNs(), 1, sink);
        else std::this_thread::yield();
    };

    const int64_t block = std::max<int64_t>(1, envI64("CLAIM_BLOCK", 1024));
    std::vector<int64_t> lat(static_cast<std::size_t>(N));
    std::vector<int64_t> blk(static_cast<std::size_t>(N / block));  // amortised phase: ns/record per block
    std::vector<uint8_t> scratch(PositionEncoder::kEncodedLength);  // "send": the caller's own buffer
    long allocs = 0;
    auto produceOne = [&](int64_t i) {
        const Key key(i);
        switch (mode) {
        case Mode::Send: {
            PositionEncoder e(scratch);
            e.userId(i);
            encodeInto(e, i);
            while (plain->trySend(topic, key, scratch) != SendResult::Ok) relieve();
            break;
        }
        case Mode::Claim:
            for (;;) {
                Claim c = plain->tryClaim(topic, key, PositionEncoder::kEncodedLength);
                if (c) {
                    PositionEncoder e(c.buffer());
                    e.userId(i);
                    encodeInto(e, i);
                    (void)c.commit();
                    break;
                }
                relieve();
            }
            break;
        case Mode::TypedSend: {
            Position rec;
            rec.userId = i;
            rec.zone   = static_cast<int32_t>(i & 7);
            rec.x = 1.5; rec.y = 2.5; rec.z = -3.5;
            rec.seq   = static_cast<uint32_t>(i);
            rec.tick  = static_cast<uint64_t>(i);
            rec.alive = true;
            rec.kind  = 1;
            std::memcpy(rec.tag.data(), "bench", 5);
            typed->send(rec);  // void send: Thread mode waits on the duty thread
            break;
        }
        case Mode::TypedClaim:
            for (;;) {
                auto w = typed->claim(i);
                if (w) {
                    encodeInto(w, i);
                    (void)w.commit();
                    break;
                }
                relieve();
            }
            break;
        }
    };
    // Phase 1: per-record latency.
    for (int64_t i = 0; i < N; ++i) {
        if (i == kWarm) alloc_counter::startThisThread();
        const int64_t t0 = nowNs();
        produceOne(i);
        lat[static_cast<std::size_t>(i)] = nowNs() - t0;
        if (invoker && (i & 63) == 63) impl.flushBatches(nowNs(), 1, sink);  // end of an owner pass (untimed)
    }
    // Phase 2: amortised — blocks of `block` records, one clock pair per block.
    for (std::size_t b = 0; b < blk.size(); ++b) {
        const int64_t first = N + static_cast<int64_t>(b) * block;
        const int64_t t0    = nowNs();
        for (int64_t i = first; i < first + block; ++i) {
            produceOne(i);
            if (invoker && (i & 63) == 63) impl.flushBatches(nowNs(), 1, sink);  // end of an owner pass
        }
        blk[b] = nowNs() - t0;
    }
    allocs = alloc_counter::stopThisThread();
    if (invoker) {
        impl.flushBatches(nowNs(), 1, sink);
    } else {
        stop.store(true, std::memory_order_release);
        duty.join();
    }

    std::vector<int64_t> steady(lat.begin() + kWarm, lat.end());
    auto pct = [&](double q) {
        const auto k = static_cast<std::size_t>(q * static_cast<double>(steady.size() - 1));
        std::nth_element(steady.begin(), steady.begin() + static_cast<std::ptrdiff_t>(k), steady.end());
        return steady[k];
    };
    const int64_t p50 = pct(0.50);
    const int64_t p99 = pct(0.99);
    double blkMean = 0.0, blkMedian = 0.0;
    if (!blk.empty()) {
        long double sum = 0;
        for (const int64_t v : blk) sum += v;
        blkMean = static_cast<double>(sum / static_cast<long double>(blk.size()) / static_cast<long double>(block));
        const std::size_t mid = blk.size() / 2;
        std::nth_element(blk.begin(), blk.begin() + static_cast<std::ptrdiff_t>(mid), blk.end());
        blkMedian = static_cast<double>(blk[mid]) / static_cast<double>(block);
    }
    // Fields after frames= are appended (Task 10) so existing parsers still work.
    std::printf("RESULT mode=%s ingress=%s records=%lld p50_ns=%lld p99_ns=%lld allocs=%ld frames=%llu"
                " blk_ops=%lld blocks=%zu blk_mean_ns=%.2f blk_median_ns=%.2f\n",
                modeName.c_str(), invoker ? "invoker" : "thread", static_cast<long long>(N),
                static_cast<long long>(p50), static_cast<long long>(p99), allocs,
                static_cast<unsigned long long>(frames), static_cast<long long>(block), blk.size(), blkMean,
                blkMedian);
    const bool claimForm = mode == Mode::Claim || mode == Mode::TypedClaim;
    if (claimForm && allocs != 0) {
        std::fprintf(stderr, "GATE FAIL: %s allocated %ld times in steady state (spec B: 0)\n",
                     modeName.c_str(), allocs);
        return 1;
    }
    return 0;
}
