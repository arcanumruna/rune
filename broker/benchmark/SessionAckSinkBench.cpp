// SessionAckSinkBench.cpp — Task 13 (spec §3.4): the worker-side ack path.
//
//   BM_SessionAckSinkFlush/acks/sessions — enqueue `acks` ProduceResponses
//     round-robin over `sessions` Ready sessions, then one flush (find + encode
//     in the claim) — the per-pass work the worker now does instead of relaying
//     each answer through ret_ to the runtime.  Reported per ack (items/s).
//     The claim is a bump pointer into a flat buffer (no driver) so the number
//     isolates the sink: table lookup, ordering, encode.
//   BM_SessionAckSinkLookupAtScale/sessions — one enqueue+flush of a single
//     ack with `sessions` sessions in the table (open-addressing probe cost).

#include <rune/node/SessionAckSink.h>

#include <benchmark/benchmark.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <memory>

using namespace rune::node;

namespace {
struct BumpPolicy {
    struct Pub { int id = 0; };
    using PubHandle = Pub;
    std::array<uint8_t, 1 << 16>* buf = nullptr;
    std::size_t* off = nullptr;
    int64_t next = 1;
    int64_t add(std::string_view, int32_t) noexcept { return next++; }
    PubFind find(int64_t reg, PubHandle& out) noexcept { out.id = static_cast<int>(reg); return PubFind::Ready; }
    template <class E> int64_t claim(PubHandle&, std::size_t len, E&& enc) noexcept {
        if (*off + len > buf->size()) *off = 0;
        enc(buf->data() + *off);
        *off += len;
        return static_cast<int64_t>(*off);
    }
};
using Sink = SessionAckSinkT<BumpPolicy>;

std::unique_ptr<Sink> readySink(std::array<uint8_t, 1 << 16>& buf, std::size_t& off, int64_t sessions) {
    BumpPolicy p; p.buf = &buf; p.off = &off;
    auto s = std::make_unique<Sink>(p);
    for (int64_t sid = 1; sid <= sessions; ++sid) s->announce(sid, "aeron:udp?endpoint=10.0.0.1:40123", 1001);
    int64_t now = 1;
    for (int i = 0; i < 4; ++i) s->flush(now += 1000);
    return s;
}
} // namespace

static void BM_SessionAckSinkFlush(benchmark::State& state) {
    const int64_t acks = state.range(0), sessions = state.range(1);
    auto buf = std::make_unique<std::array<uint8_t, 1 << 16>>();
    std::size_t off = 0;
    auto s = readySink(*buf, off, sessions);
    int64_t corr = 0, now = 1'000'000;
    for (auto _ : state) {
        for (int64_t i = 0; i < acks; ++i)
            s->enqueue(AckEntry{1 + (i % sessions), ++corr, corr, 0, 0});
        benchmark::DoNotOptimize(s->flush(now += 1000));
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * acks);
}
BENCHMARK(BM_SessionAckSinkFlush)->Args({1, 1})->Args({16, 1})->Args({64, 4})->Args({256, 16})
    ->Unit(benchmark::kNanosecond);

static void BM_SessionAckSinkLookupAtScale(benchmark::State& state) {
    const int64_t sessions = state.range(0);
    auto buf = std::make_unique<std::array<uint8_t, 1 << 16>>();
    std::size_t off = 0;
    auto s = readySink(*buf, off, sessions);
    int64_t corr = 0, now = 1'000'000, sid = 0;
    for (auto _ : state) {
        sid = sid % sessions + 1;
        s->enqueue(AckEntry{sid, ++corr, corr, 0, 0});
        benchmark::DoNotOptimize(s->flush(now += 1000));
    }
}
BENCHMARK(BM_SessionAckSinkLookupAtScale)->Arg(1)->Arg(64)->Arg(1024)->Arg(4000)->Unit(benchmark::kNanosecond);
