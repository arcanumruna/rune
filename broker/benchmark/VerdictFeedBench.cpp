// #804: a checked-writes control record is validated (framing + CRC32C) once per parse.
//
// BM_ControlParseMarker/0  the old shape: recordType + parseMarker on the raw bytes, three validations per marker.
// BM_ControlParseMarker/1  parseControl once, then the view overload (no CRC).
// BM_VerdictFeedBatchMarker  the leadership-scan feed (verdictFeedBatch) for one 64-entry marker, the real consumer.
#include <benchmark/benchmark.h>

#include <rune/node/VerdictSource.h>
#include <rune/node/WriteState.h>
#include <rune/storage/ControlRecordLayout.h>

#include <cstdint>
#include <vector>

namespace {
namespace cr = rune::storage::control_record;

std::vector<uint8_t> marker64() {
    std::vector<cr::MarkerEntry> e(64);
    for (uint32_t i = 0; i < e.size(); ++i) e[i] = {i, 55, 0x00070001};
    std::vector<uint8_t> b(cr::markerBatchBytes(64));
    const cr::MarkerHeader h{1000, 1064, 3, 64};
    if (cr::buildVerdictMarker(b, h, e) != b.size()) b.clear();
    return b;
}

void BM_ControlParseMarker(benchmark::State& state) {
    const auto b = marker64();
    const bool once = state.range(0) == 1;
    int64_t sum = 0;
    for (auto _ : state) {
        if (once) {
            const auto c = cr::parseControl(b.data(), b.size());
            const auto m = c ? cr::parseMarker(*c) : std::nullopt;
            for (uint32_t i = 0; m && i < m->header.count; ++i) sum += m->offsetAt(i) + m->codeAt(i);
        } else {
            if (cr::recordType(b.data(), b.size()) == cr::kVerdictMarker) {
                const auto m = cr::parseMarker(b.data(), b.size());   // header + value: two more validations
                const auto h = cr::parseMarkerHeader(b.data(), b.size());
                for (uint32_t i = 0; m && h && i < m->header.count; ++i) sum += m->offsetAt(i) + m->codeAt(i);
            }
        }
        benchmark::DoNotOptimize(sum);
    }
}
BENCHMARK(BM_ControlParseMarker)->Arg(0)->Arg(1);

void BM_VerdictFeedBatchMarker(benchmark::State& state) {
    const auto b = marker64();
    rune::node::PartitionWriteState w;
    for (auto _ : state) {
        rune::node::verdictFeedBegin(&w, 0, 0);
        rune::node::verdictFeedBatch(&w, b.data(), b.size(), 1064);
        benchmark::ClobberMemory();
    }
}
BENCHMARK(BM_VerdictFeedBatchMarker);
}  // namespace
