// AuthCheckBench.cpp - authentication Task 13 (spec §12): the per-produce authorization check, gate <= 2 ns, 0 allocations.
//
//   BM_GatewayPermissionTableAllows        PermissionTable::allows over a 10 000-topic table, random ids (pre-generated)
//   BM_WorkerAllowsWrite                   allowsWrite on a resolved WriteGrantSlot (10 000-topic bitmap), random ids
//   BM_WorkerSlotLookupPlusAllowsWrite     the worker's full per-frame cost: SessionAckSink slot probe + allowsWrite
//                                          (reported; the probe is the one hash the frame's admission already pays)
//   BM_WorkerFrameCheckPidOwner/0|1        issue #122: the worker's per-frame check (slot probe + WRITE bit) without /
//                                          with the producer-id owner check, over the same frame records (A/B)
//   BM_GatewayFrameCheckPidOwner/0|1       the same at the gateway (PermissionTable::allows without / with the check)
#include "BenchGate.h"

#include <rune/gateway/ObjectIndex.h>
#include <rune/gateway/PermissionTable.h>
#include <rune/node/SessionAckSink.h>
#include <rune/node/WorkerWriteGrants.h>
#include <rune/protocol/ProducerIdBinding.h>
#include <rune/security/Permissions.h>

#include <benchmark/benchmark.h>

#include <atomic>
#include <memory>
#include <random>
#include <string>
#include <vector>

extern std::atomic<int64_t> g_allocCount;
extern std::atomic<bool>    g_allocEnabled;

namespace {

constexpr int32_t kTopics = 10'000;
constexpr std::size_t kIds = 1 << 16;   // power of two: the cycle index is a mask

void allocsOn() { g_allocCount.store(0); g_allocEnabled.store(true); }
void allocsOff(benchmark::State& state) {
    g_allocEnabled.store(false);
    const auto it = static_cast<double>(state.iterations());
    const double perOp = it > 0 ? static_cast<double>(g_allocCount.load()) / it : 0.0;
    state.counters["allocs_per_op"] = perOp;
    rune::bench::gateCheck(state, g_allocCount.load() == 0, "the authorization check allocated");
}

std::vector<int32_t> randomTopicIds() {
    std::mt19937 rng(12345);
    std::uniform_int_distribution<int32_t> d(0, kTopics - 1);
    std::vector<int32_t> ids(kIds);
    for (auto& i : ids) i = d(rng);
    return ids;
}

void BM_GatewayPermissionTableAllows(benchmark::State& state) {
    rune::gateway::ObjectIndex idx;
    for (int32_t i = 0; i < kTopics; ++i) idx.onTopic(i, "t" + std::to_string(i));
    const std::vector<rune::security::GrantSpec> grants = {
        {rune::security::Operation::Write, {rune::security::ResourceType::Topic, rune::security::PatternKind::Prefix, "t1"}}};
    const auto table = rune::gateway::PermissionTable::compile(grants, idx);
    const uint8_t write = rune::security::opBit(rune::security::Operation::Write);
    const auto ids = randomTopicIds();
    std::size_t k = 0;
    uint64_t allowed = 0;
    allocsOn();
    for (auto _ : state) {
        allowed += table.allows(ids[k++ & (kIds - 1)], write) ? 1u : 0u;
    }
    allocsOff(state);
    benchmark::DoNotOptimize(allowed);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_GatewayPermissionTableAllows)->Unit(benchmark::kNanosecond)->MinTime(0.5)->Repetitions(10)->ReportAggregatesOnly(true);

rune::node::WriteGrantSlot makeSlot() {
    rune::node::WriteGrantSlot s;
    s.bits.assign((kTopics + 63) / 64, 0);
    for (int32_t t = 0; t < kTopics; ++t)
        if (t % 3 != 0) s.bits[static_cast<std::size_t>(t) >> 6] |= uint64_t{1} << (t & 63);
    return s;
}

void BM_WorkerAllowsWrite(benchmark::State& state) {
    const rune::node::WriteGrantSlot slot = makeSlot();
    const auto ids = randomTopicIds();
    std::size_t k = 0;
    uint64_t allowed = 0;
    allocsOn();
    for (auto _ : state) {
        allowed += rune::node::allowsWrite(slot, ids[k++ & (kIds - 1)]) ? 1u : 0u;
    }
    allocsOff(state);
    benchmark::DoNotOptimize(allowed);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_WorkerAllowsWrite)->Unit(benchmark::kNanosecond)->MinTime(0.5)->Repetitions(10)->ReportAggregatesOnly(true);

void BM_WorkerSlotLookupPlusAllowsWrite(benchmark::State& state) {
    auto sink = std::make_unique<rune::node::SessionAckSink>(rune::node::AeronSessionPubPolicy{nullptr, 0});
    constexpr int kSessions = 200;   // within the sink's channel pool: no publication is ever created (null client)
    for (int64_t sid = 1; sid <= kSessions; ++sid) {
        (void)sink->announce(sid, "aeron:ipc", 7, 9, false);
        std::vector<uint64_t> words = makeSlot().bits;
        (void)sink->installWriteGrants(sid, words, false, 1, /*owner=*/7);
    }
    const auto ids = randomTopicIds();
    std::vector<int64_t> sids(kIds);
    std::mt19937 rng(777);
    for (auto& s : sids) s = 1 + static_cast<int64_t>(rng() % kSessions);
    std::size_t k = 0;
    uint64_t allowed = 0;
    allocsOn();
    for (auto _ : state) {
        const std::size_t i = k++ & (kIds - 1);
        const rune::node::WriteGrantSlot* g = sink->writeGrants(sids[i]);
        allowed += (g != nullptr && rune::node::allowsWrite(*g, ids[i])) ? 1u : 0u;
    }
    allocsOff(state);
    benchmark::DoNotOptimize(allowed);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_WorkerSlotLookupPlusAllowsWrite)->Unit(benchmark::kNanosecond)->MinTime(0.5)->Repetitions(10)->ReportAggregatesOnly(true);

// Issue #122: the per-frame cost of the producer-id owner check, as a fair A/B in ONE binary: the same frame records
// (session, topic and producer id side by side, as a frame carries them; every id the session's own - a refusal is
// cold), Arg 0 = the check before #122, Arg 1 = plus the owner check.
struct Frame { int64_t sid; int64_t pid; int32_t topic; };
std::vector<Frame> frames(int sessions) {
    std::mt19937 rng(4242);
    std::uniform_int_distribution<int32_t> d(0, kTopics - 1);
    std::vector<Frame> f(kIds);
    for (auto& x : f) {
        x.sid = 1 + static_cast<int64_t>(rng() % static_cast<uint32_t>(sessions));
        x.pid = rune::protocol::bindProducerId(static_cast<int64_t>(rng() & 0xFFFF), /*owner=*/7);
        x.topic = d(rng);
    }
    return f;
}

void BM_WorkerFrameCheckPidOwner(benchmark::State& state) {
    const bool pidCheck = state.range(0) != 0;
    auto sink = std::make_unique<rune::node::SessionAckSink>(rune::node::AeronSessionPubPolicy{nullptr, 0});
    constexpr int kSessions = 200;
    for (int64_t sid = 1; sid <= kSessions; ++sid) {
        (void)sink->announce(sid, "aeron:ipc", 7, 9, false);
        std::vector<uint64_t> words = makeSlot().bits;
        (void)sink->installWriteGrants(sid, words, false, 1, /*owner=*/7);
    }
    const auto fs = frames(kSessions);
    std::size_t k = 0;
    uint64_t allowed = 0;
    allocsOn();
    for (auto _ : state) {
        const Frame& f = fs[k++ & (kIds - 1)];
        const rune::node::WriteGrantSlot* g = sink->writeGrants(f.sid);
        bool ok = g != nullptr && rune::node::allowsWrite(*g, f.topic);
        if (pidCheck) ok = ok & (g == nullptr || rune::protocol::producerIdUsableByTag(f.pid, g->ownerTag));
        allowed += ok ? 1u : 0u;
    }
    allocsOff(state);
    benchmark::DoNotOptimize(allowed);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_WorkerFrameCheckPidOwner)->Arg(0)->Arg(1)->Unit(benchmark::kNanosecond)->MinTime(0.5)->Repetitions(10)->ReportAggregatesOnly(true);

void BM_GatewayFrameCheckPidOwner(benchmark::State& state) {
    const bool pidCheck = state.range(0) != 0;
    rune::gateway::ObjectIndex idx;
    for (int32_t i = 0; i < kTopics; ++i) idx.onTopic(i, "t" + std::to_string(i));
    const std::vector<rune::security::GrantSpec> grants = {
        {rune::security::Operation::Write, {rune::security::ResourceType::Topic, rune::security::PatternKind::Prefix, "t1"}}};
    const auto table = rune::gateway::PermissionTable::compile(grants, idx);
    const uint8_t write = rune::security::opBit(rune::security::Operation::Write);
    const auto fs = frames(1);
    const uint64_t userId = 7;
    std::size_t k = 0;
    uint64_t allowed = 0;
    allocsOn();
    for (auto _ : state) {
        const Frame& f = fs[k++ & (kIds - 1)];
        bool ok = table.allows(f.topic, write);
        if (pidCheck) ok = ok & rune::protocol::producerIdUsableBy(f.pid, userId);
        allowed += ok ? 1u : 0u;
    }
    allocsOff(state);
    benchmark::DoNotOptimize(allowed);
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_GatewayFrameCheckPidOwner)->Arg(0)->Arg(1)->Unit(benchmark::kNanosecond)->MinTime(0.5)->Repetitions(10)->ReportAggregatesOnly(true);

}  // namespace
