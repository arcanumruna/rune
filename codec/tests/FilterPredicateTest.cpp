// FilterPredicateTest.cpp — Phase 2c Task 3: CompiledFilter verdicts on records of several schema versions.
#include "FilterTestSupport.h"

#include <rune/codec/Filter.h>
#include <rune/codec/FilterPredicate.h>

#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <new>
#include <vector>

// A counting global operator new for the allocation test (this test binary only).
namespace { std::size_t gNewCount = 0; }
void* operator new(std::size_t n) { ++gNewCount; if (void* p = std::malloc(n ? n : 1)) return p; throw std::bad_alloc(); }
void* operator new[](std::size_t n) { ++gNewCount; if (void* p = std::malloc(n ? n : 1)) return p; throw std::bad_alloc(); }
void* operator new(std::size_t n, std::align_val_t a) { ++gNewCount; if (void* p = std::aligned_alloc(std::size_t(a), (n + std::size_t(a) - 1) / std::size_t(a) * std::size_t(a))) return p; throw std::bad_alloc(); }
void* operator new[](std::size_t n, std::align_val_t a) { return ::operator new(n, a); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
// The nothrow forms route through the replaced throwing forms. Otherwise a nothrow allocation (libstdc++'s
// get_temporary_buffer under std::stable_sort, e.g. in gtest) comes from the default/ASan allocator but is released
// by the replaced delete: ASan alloc-dealloc-mismatch, and the allocation goes uncounted.
void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
    try { return ::operator new(n); } catch (...) { return nullptr; }
}
void* operator new[](std::size_t n, const std::nothrow_t&) noexcept {
    try { return ::operator new[](n); } catch (...) { return nullptr; }
}
void operator delete(void* p, const std::nothrow_t&) noexcept { ::operator delete(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { ::operator delete[](p); }

using namespace rune::codec;
using namespace filtertest;
using rune::Match;
using rune::anyOf;

namespace {
struct FB { FilterBytes b; FilterWriter w{std::span<uint8_t>(b.bytes.data(), b.bytes.size())};
            std::span<const uint8_t> done() { auto n = w.finish(); EXPECT_TRUE(n.has_value()); b.len = uint16_t(n.value_or(0)); return b.span(); } };

const Descriptor& pe() { static const Descriptor d = playerEvent(); return d; }
const FilterSchema& schema() { static const FilterSchema s = FilterSchema::build(pe(), 3); return s; }

CompiledFilter compileOk(std::span<const uint8_t> b) {
    auto c = CompiledFilter::compile(b, schema());
    EXPECT_TRUE(c.has_value());
    return std::move(*c);
}
FilterVerdict ev(const CompiledFilter& f, const std::vector<uint8_t>& rec) { return f.evaluate(rec, false, schema()); }
}  // namespace

TEST(FilterPredicate, EqAndInMatchAndMiss) {
    FB a; a.w.term(1, PrimType::UInt32, 4, Match<uint32_t>(7u));
    const auto eq = compileOk(a.done());
    FB b; b.w.term(1, PrimType::UInt32, 4, Match<uint32_t>(anyOf(3u, 7u, 9u)));
    const auto in = compileOk(b.done());
    FB c; c.w.term(1, PrimType::UInt32, 4, Match<uint32_t>(7u)); c.w.term(6, PrimType::Int64, 8, Match<int64_t>(anyOf<int64_t>(-5, 100)));
    const auto both = compileOk(c.done());
    for (uint16_t v = 1; v <= 3; ++v) {
        EXPECT_EQ(ev(eq, encodeRecord(pe(), v, {.worldId = 7})), FilterVerdict::Match) << v;
        EXPECT_EQ(ev(eq, encodeRecord(pe(), v, {.worldId = 8})), FilterVerdict::NoMatch) << v;
        EXPECT_EQ(ev(in, encodeRecord(pe(), v, {.worldId = 9})), FilterVerdict::Match) << v;
        EXPECT_EQ(ev(in, encodeRecord(pe(), v, {.worldId = 4})), FilterVerdict::NoMatch) << v;
        EXPECT_EQ(ev(both, encodeRecord(pe(), v, {.worldId = 7, .hp = -5})), FilterVerdict::Match) << v;
        EXPECT_EQ(ev(both, encodeRecord(pe(), v, {.worldId = 7, .hp = 6})), FilterVerdict::NoMatch) << v;
        EXPECT_EQ(ev(both, encodeRecord(pe(), v, {.worldId = 1, .hp = 100})), FilterVerdict::NoMatch) << v;
    }
}

TEST(FilterPredicate, LargeInUsesBinarySearch) {
    std::vector<uint32_t> vals; for (uint32_t i = 0; i < 64; ++i) vals.push_back(i * 3);
    FB a; a.w.term(1, PrimType::UInt32, 4, Match<uint32_t>(anyOf(std::span<const uint32_t>(vals))));
    const auto in = compileOk(a.done());
    ASSERT_EQ(in.term(0).count, 64);
    for (uint32_t w = 0; w < 200; ++w)
        EXPECT_EQ(ev(in, encodeRecord(pe(), 1, {.worldId = w})), (w % 3 == 0 && w < 192) ? FilterVerdict::Match : FilterVerdict::NoMatch) << w;
}

TEST(FilterPredicate, IndexTermIsFirstEqElseFirstIn) {
    FB a; a.w.term(1, PrimType::UInt32, 4, Match<uint32_t>(anyOf(1u, 2u))); a.w.term(2, PrimType::Int32, 4, Match<int32_t>(5));
    const auto eqAndIn = compileOk(a.done());
    EXPECT_EQ(eqAndIn.indexTerm(), 1);
    EXPECT_EQ(eqAndIn.term(1).op, FilterOp::Eq);
    FB b; b.w.term(1, PrimType::UInt32, 4, Match<uint32_t>(anyOf(1u, 2u))); b.w.term(2, PrimType::Int32, 4, Match<int32_t>(anyOf(5, 6)));
    const auto inOnly = compileOk(b.done());
    EXPECT_EQ(inOnly.indexTerm(), 0);
    FB c; c.w.term(1, PrimType::UInt32, 4, Match<uint32_t>(1u)); c.w.term(2, PrimType::Int32, 4, Match<int32_t>(5));
    EXPECT_EQ(compileOk(c.done()).indexTerm(), 0);
    EXPECT_EQ(eqAndIn.termCount(), 2);
    EXPECT_EQ(inOnly.values(1).size(), 2u);
    EXPECT_LT(inOnly.values(1)[0], inOnly.values(1)[1]);
}

TEST(FilterPredicate, ResidualSkipsTheIndexTerm) {
    FB a; a.w.term(1, PrimType::UInt32, 4, Match<uint32_t>(7u)); a.w.term(6, PrimType::Int64, 8, Match<int64_t>(100));
    const auto f = compileOk(a.done());
    ASSERT_EQ(f.indexTerm(), 0);
    auto wrongIndex = encodeRecord(pe(), 1, {.worldId = 99, .hp = 100});
    EXPECT_EQ(f.evaluateResidual(wrongIndex, 1, schema()), FilterVerdict::Match);   // index term not looked at
    EXPECT_EQ(ev(f, wrongIndex), FilterVerdict::NoMatch);
    auto wrongResidual = encodeRecord(pe(), 1, {.worldId = 7, .hp = 1});
    EXPECT_EQ(f.evaluateResidual(wrongResidual, 1, schema()), FilterVerdict::NoMatch);
    // a single-term filter has no residual
    FB b; b.w.term(1, PrimType::UInt32, 4, Match<uint32_t>(7u));
    EXPECT_EQ(compileOk(b.done()).evaluateResidual(wrongIndex, 1, schema()), FilterVerdict::Match);
}

TEST(FilterPredicate, NegativeInt32EqualsItsStoredBytes) {
    FB a; a.w.term(2, PrimType::Int32, 4, Match<int32_t>(-1));
    const auto f = compileOk(a.done());
    EXPECT_EQ(ev(f, encodeRecord(pe(), 1, {.channel = -1})), FilterVerdict::Match);
    EXPECT_EQ(ev(f, encodeRecord(pe(), 1, {.channel = 1})), FilterVerdict::NoMatch);
    FB b; b.w.term(6, PrimType::Int64, 8, Match<int64_t>(std::numeric_limits<int64_t>::min()));
    EXPECT_EQ(ev(compileOk(b.done()), encodeRecord(pe(), 3, {.hp = std::numeric_limits<int64_t>::min()})), FilterVerdict::Match);
}

TEST(FilterPredicateRegression, ShortFixedStrLiteralMatchesTheZeroPaddedFieldRegression) {
    FB a; a.w.term(3, PrimType::FixedStr, 8, Match<std::array<char, 8>>("eu"));
    const auto f = compileOk(a.done());
    EXPECT_EQ(ev(f, encodeRecord(pe(), 1, {.zone = "eu"})), FilterVerdict::Match);
    EXPECT_EQ(ev(f, encodeRecord(pe(), 3, {.zone = "eu"})), FilterVerdict::Match);
    EXPECT_EQ(ev(f, encodeRecord(pe(), 1, {.zone = "eu-west"})), FilterVerdict::NoMatch);
    EXPECT_EQ(ev(f, encodeRecord(pe(), 1, {.zone = "us"})), FilterVerdict::NoMatch);
}

TEST(FilterPredicate, EnumAndBoolTerms) {
    FB a; a.w.term(4, PrimType::Enum, 4, Match<int32_t>(anyOf(2, 3))); a.w.term(5, PrimType::Bool, 1, Match<bool>(true));
    const auto f = compileOk(a.done());
    EXPECT_EQ(ev(f, encodeRecord(pe(), 2, {.kind = 3, .alive = true})), FilterVerdict::Match);
    EXPECT_EQ(ev(f, encodeRecord(pe(), 2, {.kind = 1, .alive = true})), FilterVerdict::NoMatch);
    EXPECT_EQ(ev(f, encodeRecord(pe(), 2, {.kind = 3, .alive = false})), FilterVerdict::NoMatch);
}

TEST(FilterPredicate, DroppedFieldIsAbsentNotNoMatch) {
    FB a; a.w.term(2, PrimType::Int32, 4, Match<int32_t>(0));   // channel: dropped at v3; value 0 would match a zeroed slot
    const auto f = compileOk(a.done());
    EXPECT_EQ(ev(f, encodeRecord(pe(), 3, {})), FilterVerdict::Absent);
    EXPECT_EQ(ev(f, encodeRecord(pe(), 2, {})), FilterVerdict::Match);
    FB g; g.w.term(9, PrimType::UInt32, 4, Match<uint32_t>(0u));   // guild: added at v2
    EXPECT_EQ(ev(compileOk(g.done()), encodeRecord(pe(), 1, {})), FilterVerdict::Absent);
    // Absent outranks NoMatch regardless of term order
    FB h; h.w.term(1, PrimType::UInt32, 4, Match<uint32_t>(7u)); h.w.term(2, PrimType::Int32, 4, Match<int32_t>(0));
    EXPECT_EQ(ev(compileOk(h.done()), encodeRecord(pe(), 3, {.worldId = 1})), FilterVerdict::Absent);
}

TEST(FilterPredicate, UnknownVersionIsReportedNotGuessed) {
    FB a; a.w.term(1, PrimType::UInt32, 4, Match<uint32_t>(7u));
    const auto f = compileOk(a.done());
    auto rec = encodeRecord(pe(), 3, {.worldId = 7});
    rec[2] = 9; rec[3] = 0;
    EXPECT_EQ(ev(f, rec), FilterVerdict::UnknownVersion);
    EXPECT_EQ(f.evaluateResidual(rec, 9, schema()), FilterVerdict::UnknownVersion);
    rec[2] = 0;   // version 0
    EXPECT_EQ(ev(f, rec), FilterVerdict::UnknownVersion);
}

TEST(FilterPredicateRegression, NullValueNeverMatchesAndIsCountedRegression) {
    FB a; a.w.term(1, PrimType::UInt32, 4, Match<uint32_t>(7u));
    FB b; b.w.term(1, PrimType::UInt32, 4, Match<uint32_t>(anyOf(7u, 8u))); b.w.term(6, PrimType::Int64, 8, Match<int64_t>(0));
    FB c; c.w.term(2, PrimType::Int32, 4, Match<int32_t>(0));
    const auto rec = encodeRecord(pe(), 3, {.worldId = 7});
    for (auto sp : {a.done(), b.done(), c.done()}) {
        const auto f = compileOk(sp);
        EXPECT_EQ(f.evaluate(rec, true, schema()), FilterVerdict::NullValue);
        EXPECT_EQ(f.evaluate({}, true, schema()), FilterVerdict::NullValue);
    }
}

TEST(FilterPredicateRegression, ShortValueNeverReadsPastItsEndRegression) {
    FB a; a.w.term(1, PrimType::UInt32, 4, Match<uint32_t>(0u)); a.w.term(6, PrimType::Int64, 8, Match<int64_t>(0));
    const auto f = compileOk(a.done());
    const FieldSlot hp = schema().slot(schema().indexOfTag(6), 1);
    const std::size_t end = hp.offset + hp.width;
    const auto full = encodeRecord(pe(), 1, {});
    ASSERT_GE(full.size(), end);
    for (std::size_t n = 0; n < end; ++n) {
        std::vector<uint8_t> v(n);   // heap block of exactly n bytes: ASan flags any over-read
        std::copy(full.begin(), full.begin() + std::min(n, full.size()), v.begin());
        const auto ve = f.evaluate(v, false, schema());
        EXPECT_EQ(ve, FilterVerdict::Malformed) << n;
        if (n >= 4) EXPECT_EQ(f.evaluateResidual(v, 1, schema()), FilterVerdict::Malformed) << n;
    }
    std::vector<uint8_t> ok(full.begin(), full.begin() + end);
    EXPECT_EQ(f.evaluate(ok, false, schema()), FilterVerdict::Match);
}

TEST(FilterPredicate, CompileAllocatesOnceEvaluateNever) {
    FB a; a.w.term(1, PrimType::UInt32, 4, Match<uint32_t>(anyOf(1u, 2u, 7u))); a.w.term(6, PrimType::Int64, 8, Match<int64_t>(5));
    const auto sp = a.done();
    (void)schema();   // build the fixture schema outside the counted window
    const std::size_t c0 = gNewCount;
    auto f = CompiledFilter::compile(sp, schema());
    ASSERT_TRUE(f.has_value());
    EXPECT_EQ(gNewCount - c0, 1u);   // the single values_ reserve
    const auto rec = encodeRecord(pe(), 2, {.worldId = 7, .hp = 5});
    std::size_t matches = 0;
    const std::size_t n0 = gNewCount;
    for (int i = 0; i < 10000; ++i) {
        matches += f->evaluate(rec, false, schema()) == FilterVerdict::Match;
        matches += f->evaluateResidual(rec, 2, schema()) == FilterVerdict::Match;
    }
    EXPECT_EQ(gNewCount - n0, 0u);
    EXPECT_EQ(matches, 20000u);
    const std::size_t p0 = gNewCount;
    (void)parseFilter(sp, schema());
    EXPECT_EQ(gNewCount - p0, 0u);   // parseFilter never allocates
}
