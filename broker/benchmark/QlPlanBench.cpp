// benchmark/QlPlanBench.cpp — Checked Writes Part 08 (spec §14): the QL <-> plan gates.
//
//   BM_QlTextToPlan/{projection,composite,lower,aggregate_check} — ql::parse + applyViewPlan (a pre-built ViewSource) +
//     serializeAuthored of one `create view`: "QL text -> wire".  Gate: p50 <= 5 us (the repo's QL gate).
//   BM_QlWireToPlan/{...} — deserializeAuthored + compileView of the same blobs: "wire -> plan".  Gate: p50 <= 5 us.
//
// Both report p50_ns / p99_ns and ns_per_stmt.  The statements are the Part 08 golden's: character_by_id,
// account_characters, character_name, account_stats.
#include "BenchGate.h"

#include <benchmark/benchmark.h>
#include <rune/codec/ViewPlanAuthored.h>
#include <rune/ql/Backends.h>
#include <rune/ql/Parser.h>
#include <rune/view/PlanCompiler.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace {

namespace ql = rune::ql;

constexpr int     kCalls  = 1000;
constexpr double  kGateNs = 5000.0;
constexpr int32_t kSrcId  = 7;
constexpr int32_t kViewId = 0x40000090;

struct Stmt {
    const char* name;
    const char* text;
};

constexpr Stmt kStmts[] = {
    {"projection", "create view character_by_id key characterId as select userId, name, gender from character;"},
    {"composite", "create view account_characters key (userId, characterId) as select name, gender from character;"},
    {"lower", "create unique view character_name key lower(name) as nameKey as select characterId from character;"},
    {"aggregate_check", "create view account_stats key userId as select count(*) as characters from character "
                        "constraint max_characters check (characters <= 12);"},
};

constexpr const char* kCharacter =
    "topic character { characterId uint64 autoincrement, userId uint64, name string(16), gender uint16, hairSkin uint16, "
    "hairSheet uint16, headSkin uint16, createdAt int64, lastPlayed int64 } partitions 1 key characterId;";

ql::ViewSource characterSource() {
    auto st = ql::parse(kCharacter);
    ql::ViewSource src = ql::viewSourceOf(std::get<ql::TopicDef>(*st));
    src.topicId = kSrcId;
    return src;
}

void report(benchmark::State& state, std::vector<double>& ns, const char* msg) {
    std::sort(ns.begin(), ns.end());
    const double p50 = ns[ns.size() / 2], p99 = ns[ns.size() * 99 / 100];
    state.counters["p50_ns"]      = p50;
    state.counters["p99_ns"]      = p99;
    state.counters["ns_per_stmt"] = p50;
    state.SetItemsProcessed(state.iterations() * kCalls);
    rune::bench::gateCheck(state, p50 <= kGateNs, msg);
}

void textToPlan(benchmark::State& state, const Stmt* s) {
    const ql::ViewSource src = characterSource();
    std::vector<double> ns(kCalls);
    for (auto _ : state) {
        for (int i = 0; i < kCalls; ++i) {
            const auto t0 = std::chrono::steady_clock::now();
            auto st = ql::parse(s->text);
            if (!st) { state.SkipWithError(st.error().message.c_str()); return; }
            auto plan = ql::applyViewPlan(std::get<ql::CreateViewDef>(*st), src);
            if (!plan) { state.SkipWithError(plan.error().message.c_str()); return; }
            auto wire = rune::view::serializeAuthored(*plan);
            const auto t1 = std::chrono::steady_clock::now();
            benchmark::DoNotOptimize(wire);
            ns[static_cast<std::size_t>(i)] = std::chrono::duration<double, std::nano>(t1 - t0).count();
        }
    }
    report(state, ns, "p50_ns > 5000 (QL text -> wire gate)");
}

void wireToPlan(benchmark::State& state, const Stmt* s) {
    const ql::ViewSource src = characterSource();
    auto st = ql::parse(s->text);
    const std::vector<uint8_t> wire = rune::view::serializeAuthored(*ql::applyViewPlan(std::get<ql::CreateViewDef>(*st), src));
    const rune::codec::Descriptor* srcs[] = {&src.descriptor};
    std::vector<double> ns(kCalls);
    for (auto _ : state) {
        for (int i = 0; i < kCalls; ++i) {
            const auto t0 = std::chrono::steady_clock::now();
            auto plan = rune::view::deserializeAuthored(wire);
            auto comp = rune::view::compileView(*plan, srcs, kViewId);
            const auto t1 = std::chrono::steady_clock::now();
            benchmark::DoNotOptimize(comp);
            ns[static_cast<std::size_t>(i)] = std::chrono::duration<double, std::nano>(t1 - t0).count();
            if (!comp) { state.SkipWithError(comp.error().message.c_str()); return; }
        }
    }
    report(state, ns, "p50_ns > 5000 (wire -> plan gate)");
}

}  // namespace

static const bool kQlBenches = [] {
    for (const Stmt& s : kStmts) {
        benchmark::RegisterBenchmark((std::string("BM_QlTextToPlan/") + s.name).c_str(), textToPlan, &s)->Unit(benchmark::kMicrosecond);
        benchmark::RegisterBenchmark((std::string("BM_QlWireToPlan/") + s.name).c_str(), wireToPlan, &s)->Unit(benchmark::kMicrosecond);
    }
    return true;
}();
