// benchmark/CatalogReplayBench.cpp — the library replay of a 10 000-event __catalog log
// (spec §6 / global constraint 5).  Gate: one iteration (a full verifying replay of all
// 10 000 events through a fresh CatalogReplayer: decode, gap check, apply, fingerprint
// verify) < 200 ms in Release; the broker-level boot gate (< 1 s) is Task 23's.
#include <benchmark/benchmark.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

namespace {
namespace rc = rune::catalog;
namespace cd = rune::codec;

constexpr int kEvents = 10'000;
constexpr int kNames  = 500;

/// A valid log: per name, the cycle CreateTopic (typed) → AlterSchema (ADD) →
/// AlterTopicConfig → CreateView over it → DropView → DropTopic, names cycled so every
/// name is dropped and reused.  Each event stamped with previewFingerprint, as the leader does.
std::vector<std::vector<uint8_t>> buildLog() {
    rc::CatalogState s;
    std::vector<std::vector<uint8_t>> out;
    out.reserve(kEvents);
    const std::vector<uint8_t> topicDesc = [] {
        cd::DescriptorBuilder d("t");
        d.add("id", cd::PrimType::Int64, 1, true).add("hp", cd::PrimType::Int32).add("name", cd::PrimType::String);
        return cd::serialize(d.build());
    }();
    const std::vector<uint8_t> viewOut = [] {
        cd::DescriptorBuilder d("v");
        d.add("id", cd::PrimType::Int64, 1, true).add("hp", cd::PrimType::Int32);
        return cd::serialize(d.build());
    }();
    auto emit = [&](rc::Ops ops, const std::string& name, uint64_t objectId, uint32_t prev) {
        const auto i = static_cast<int64_t>(out.size());
        rc::CatalogEvent e;
        e.kind = rc::kindOf(ops); e.ops = std::move(ops); e.objectName = name; e.objectId = objectId;
        e.prevVersion = prev; e.newVersion = prev + 1;
        e.statement = "-- bench " + std::to_string(i);
        e.principal = rc::Principal{.sessionId = 1, .clientName = "bench", .sourceAddress = "local", .authenticated = {},
                                    .requestId = rc::RequestId{0xBE7C, static_cast<uint64_t>(i) + 1},
                                    .requestFingerprint = static_cast<uint64_t>(i)};
        e.timestampMs = 1'760'000'000'000 + i;
        auto fp = rc::previewFingerprint(s, e);
        if (!fp) { std::fprintf(stderr, "bench log %lld: %s\n", static_cast<long long>(i), fp.error().detail.c_str()); std::abort(); }
        e.fingerprint = *fp;
        if (auto a = rc::apply(s, e, i); !a) { std::fprintf(stderr, "bench apply %lld: %s\n", static_cast<long long>(i), a.error().detail.c_str()); std::abort(); }
        out.push_back(rc::encode(e).value());
    };
    for (int step = 0; static_cast<int>(out.size()) < kEvents; ++step) {
        const int stage = (step / kNames) % 6;
        const std::string name = "t" + std::to_string(step % kNames);
        const std::string view = "v" + std::to_string(step % kNames);
        switch (stage) {
        case 0: {
            rc::CreateTopicOps o; o.topicId = s.nextTopicId(); o.partitions = 4; o.replicationFactor = 3;
            o.schema = rc::SchemaDef{.keyField = "id", .validate = true, .descriptorBlob = topicDesc, .nextTag = 3};
            emit(std::move(o), name, s.nextObjectId(), 0);
            break;
        }
        case 1: {
            const rc::TopicObject& t = *s.topicByName(rune::catalog::kDefaultNsId, name);
            cd::AlterOp add{}; add.type = cd::AlterOpType::Add; add.name = "mana"; add.primType = cd::PrimType::Int32;
            add.fixedLen = 0; add.mode = cd::CompatMode::Backward;
            auto r = rc::resolveAlterOps(*t.schema, std::vector<cd::AlterOp>{add});
            emit(rc::AlterSchemaOps{r.value()}, name, t.objectId, t.version);
            break;
        }
        case 2: {
            const rc::TopicObject& t = *s.topicByName(rune::catalog::kDefaultNsId, name);
            emit(rc::AlterTopicConfigOps{{{rc::ConfigKey::RetentionMs, 60'000 + step}}}, name, t.objectId, t.version);
            break;
        }
        case 3: {
            rc::ViewDef v; v.viewTopicId = s.nextTopicId(); v.sinkPartitions = 4; v.keyField = "id"; v.sourceNames = {name};   // mirrors the source (#143)
            v.normalizedPlanBlob = {1, 0, 0, 0, 0}; v.compiledPlanBlob = {2}; v.outputDescriptorBlob = viewOut;
            emit(rc::CreateViewOps{std::move(v)}, view, s.nextObjectId(), 0);
            break;
        }
        case 4: {
            const rc::ViewObject& v = *s.viewByName(rune::catalog::kDefaultNsId, view);
            emit(rc::DropViewOps{v.def.viewTopicId}, view, v.objectId, v.version);
            break;
        }
        default: {
            const rc::TopicObject& t = *s.topicByName(rune::catalog::kDefaultNsId, name);
            emit(rc::DropTopicOps{t.topicId}, name, t.objectId, t.version);
            break;
        }
        }
    }
    return out;
}

void BM_CatalogReplay10k(benchmark::State& st) {
    static const std::vector<std::vector<uint8_t>> log = buildLog();
    for (auto _ : st) {
        rc::CatalogReplayer r;
        for (std::size_t i = 0; i < log.size(); ++i) {
            auto a = r.feed(static_cast<int64_t>(i), log[i]);
            if (!a) { st.SkipWithError(rc::describe(a.error()).c_str()); return; }
            benchmark::DoNotOptimize(a->fingerprint);
        }
        benchmark::DoNotOptimize(r.state().digest());
    }
    st.SetItemsProcessed(st.iterations() * static_cast<int64_t>(log.size()));
}
} // namespace

BENCHMARK(BM_CatalogReplay10k)->Unit(benchmark::kMillisecond);
