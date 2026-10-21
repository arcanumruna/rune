// benchmark/TypedDecodeBench.cpp — #418: the consume-side cost of the generated typed readers, which place every
// field by the record's own version.  A record of the type's own version (the steady state) reads at constant
// offsets behind one predicted branch; the other rows are measured too.
#include <benchmark/benchmark.h>

#include "generated/allfixed_sample.gen.h"
#include "generated/characters.gen.h"

#include <cstring>
#include <vector>

namespace {

std::vector<uint8_t> allfixedRecord() {
    rune::gen::AllfixedSample s;
    s.id = 42; s.a = -7; s.b = 9; s.c = 11; s.d = 13; s.e = 1.5; s.f = true; s.g = 2;
    std::memcpy(s.code.data(), "ABC", 3);
    std::vector<uint8_t> v(s.encodedSize());
    v.resize(s.encode(v.data()));
    return v;
}

std::vector<uint8_t> charactersRecord() {
    rune::gen::Characters s;
    s.id = 1; s.userId = 2; s.strength = 3; s.name = "a-character-name";
    std::vector<uint8_t> v(s.encodedSize());
    v.resize(s.encode(v.data()));
    return v;
}

// The zero-copy Decoder: construct over the record, read every field (what a consume handler does per record).
void BM_AllfixedDecoderReadAll(benchmark::State& st) {
    const std::vector<uint8_t> rec = allfixedRecord();
    const std::span<const uint8_t> sp(rec);
    for (auto _ : st) {
        benchmark::DoNotOptimize(sp.data());
        const rune::gen::AllfixedSample::Decoder d(sp);
        int64_t acc = d.id() + d.a() + d.b() + d.c() + static_cast<int64_t>(d.d()) + d.f() + d.g();
        double e = d.e();
        benchmark::DoNotOptimize(acc);
        benchmark::DoNotOptimize(e);
        benchmark::DoNotOptimize(d.code().data());
    }
}
BENCHMARK(BM_AllfixedDecoderReadAll);

// The generated struct's decode() of an all-fixed record.
void BM_AllfixedStructDecode(benchmark::State& st) {
    const std::vector<uint8_t> rec = allfixedRecord();
    const std::span<const uint8_t> sp(rec);
    rune::gen::AllfixedSample s;
    for (auto _ : st) {
        benchmark::DoNotOptimize(sp.data());
        static_cast<void>(s.decode(sp));
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(BM_AllfixedStructDecode);

// The generated struct's decode() of a record with a var field.
void BM_CharactersStructDecode(benchmark::State& st) {
    const std::vector<uint8_t> rec = charactersRecord();
    const std::span<const uint8_t> sp(rec);
    rune::gen::Characters s;
    for (auto _ : st) {
        benchmark::DoNotOptimize(sp.data());
        static_cast<void>(s.decode(sp));
        benchmark::DoNotOptimize(s);
    }
}
BENCHMARK(BM_CharactersStructDecode);

}  // namespace
