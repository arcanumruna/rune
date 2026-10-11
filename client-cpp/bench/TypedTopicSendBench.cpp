// bench/TypedTopicSendBench.cpp — Phase 2b Part C: TypedTopic::send cost with
// key routing (in-process, no Aeron).  The producer stub does the same
// routing work Producer::send does (key.partition(handle.partitionCount)) and
// nothing else, so the delta between the two benches is the cost of building
// Key(rec.key()) + hashing it.
//
//   BM_TypedTopicSendKeyed    real generated rune::gen::Characters (userId key), 8 partitions
//   BM_TypedTopicSendUnkeyed  the same record behind a wrapper with NO key() (pre-2b path)
//
// Gate (CLAUDE.md #4): 0 allocations per send — pinned by
// TypedTopicRoutingRegression.KeyedSendAllocatesNothingInSteadyState; this bench
// records the latency delta.

#include "characters.gen.h"

#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/TypedTopic.h>

#include <benchmark/benchmark.h>

#include <cstdint>
#include <span>
#include <string_view>

namespace rc = rune::client;

namespace {

struct RoutingNullProducer {
    int32_t lastPartition = 0;
    void send(rc::TopicHandle t, const rc::Key& k, std::span<const uint8_t> v) noexcept {
        lastPartition = k.partition(t.partitionCount);   // Producer::send's routing step
        benchmark::DoNotOptimize(lastPartition);
        benchmark::DoNotOptimize(v.data());
    }
};

// The pre-2b shape: same bytes, no key() -> Key{} -> partition 0.
struct UnkeyedCharacters {
    static constexpr uint64_t         kFingerprint = rune::gen::Characters::kFingerprint;
    static constexpr std::string_view kSchemaName  = rune::gen::Characters::kSchemaName;
    rune::gen::Characters c;
    [[nodiscard]] std::size_t encodedSize() const noexcept { return c.encodedSize(); }
    std::size_t encode(uint8_t* b) const noexcept { return c.encode(b); }
};

void BM_TypedTopicSendKeyed(benchmark::State& state) {
    rc::TypedTopic<rune::gen::Characters, RoutingNullProducer> topic(
        RoutingNullProducer{}, rc::TopicHandle{1, 8}, /*version=*/1);
    rune::gen::Characters rec;
    rec.id = 1; rec.userId = 0; rec.strength = 3; rec.name = "bench";
    for (auto _ : state) {
        ++rec.userId;
        topic.send(rec);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_TypedTopicSendKeyed);

void BM_TypedTopicSendUnkeyed(benchmark::State& state) {
    rc::TypedTopic<UnkeyedCharacters, RoutingNullProducer> topic(
        RoutingNullProducer{}, rc::TopicHandle{1, 8}, /*version=*/1);
    UnkeyedCharacters rec;
    rec.c.id = 1; rec.c.userId = 0; rec.c.strength = 3; rec.c.name = "bench";
    for (auto _ : state) {
        ++rec.c.userId;
        topic.send(rec);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_TypedTopicSendUnkeyed);

} // namespace

BENCHMARK_MAIN();
