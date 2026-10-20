// benchmark/ViewFoldBench.cpp
//
// Task 11 (§14.0) — the fold->merge pipeline HOT-PATH GATE.
//
// Measures rune::view::foldBatch() (Task 10) under a churn workload: a
// stream of committed, CRC-stamped, wire-format record batches (built via
// rune::protocol::batch::RecordBatchBuilder, mirroring
// libs/view/tests/view/ViewFoldTest.cpp's own encodeCharacter()/makeBatch()
// helpers) for a single-source Replace view keyed on characterId, 60% new
// key / 25% existing-key update / 15% tombstone, folded into a real
// ViewTable through the real PlanCompiler-compiled plan.
//
// Two gate statistics:
//
//   record_p50_us / record_p99_us
//       = the per-PROCESSED-RECORD pipeline cost (version-aware decode ->
//         project -> Replace merge -> ViewTable upsert/erase). GATE:
//         record_p99_us < 5us.
//   batch_p50_us / batch_p99_us
//       = the per-foldBatch()-CALL cost, i.e. what a caller actually pays
//         each time it folds one newly-committed batch (RecordBatchReader
//         construction + ensureValid() + N records' pipeline cost). GATE:
//         batch_p99_us < 10us.
//
// Batches in this workload are SMALL but not all size 1 (cycling through
// {1,2,1,3,2,1,4,2}, avg 2 records/batch -- the brief's own "single-record
// (or small) batches" wording). record_p50/p99 is a TRUE per-record order
// statistic, not a batch-amortized estimate: dividing a multi-record
// batch's wall-clock latency by its record count and counting that
// quotient once per record would smooth genuine per-record tail variance
// evenly across the batch, understating the real single-record tail by up
// to ~the max batch size -- a counter gated and named "per record" has to
// actually BE one. So:
//   - trueRecordLatNs holds ONE sample per foldBatch() call whose batch
//     size is EXACTLY 1 (the size-1 slots in the cycle above: 3 of 8, ~37%
//     of batches) -- for those calls the whole call IS one record's
//     pipeline cost, with zero batch-overhead amortization to divide out.
//     This is record_p50/p99's population (~375K samples over the 2M-record
//     measured pass -- see counts below).
//   - batchLatNs holds ONE sample per foldBatch() call of ANY size (1-4) --
//     batch_p50/p99's population, representing the real per-call cost a
//     caller actually pays regardless of how many records land in that
//     batch.
//
// Unlike the ViewTable storage gate (rehash events are ~0.5-0.8% of ops,
// so p999 is needed to land inside that tail -- see ViewTableBench.cpp),
// EVERY record here traverses the exact same fold pipeline: there is no
// rare/common split, so p99 (not p999) is the correct, representative gate
// cut (per the task brief).
//
// THREE-PHASE DESIGN (mirrors ViewTableBench.cpp's own churn/steady-find
// split, for the same reason): ViewTable's own contract (ViewTable.h) is
// "zero heap allocation in put()/get()/erase() steady state, EXCEPT (a) the
// deliberate chunk allocation ... and (b) the deliberate, bounded index
// growth machinery" -- i.e. zero-alloc holds only while the table is NOT
// actively growing its index/chunks. This workload's 60/25/15 mix is net
// +45%-of-ops population growth with NO plateau, so the index keeps
// doubling throughout the run; computing the alloc delta over the SAME
// window used for the latency percentiles would attribute ViewTable's
// documented, rare growth-path allocations to the fold pipeline, which
// would not be a fold-hot-path finding. Instead:
//   1. WARMUP (20,000 records; not timed, not alloc-tracked): folds enough
//      records to materialize VersionDescriptorCache's one-time
//      first-sight-of-writer-version-1 allocation (every record here is
//      version 1 -- the common "long run against the same version" case
//      the cache exists for) and let the table grow past its initial small
//      index capacity.
//   2. MEASURED LATENCY pass (2,000,000 records; timed, NOT alloc-tracked):
//      produces record_p50/p99 and batch_p50/p99. The table keeps growing
//      through this pass -- that's fine for a p99 gate (growth events are
//      a small fraction of 2M ops, same reasoning as the brief's own
//      p99-vs-p999 note).
//   3. STEADY ALLOC-CHECK pass (100,000 more records; alloc-tracked via
//      g_allocCount/g_allocEnabled): by the time phase 2 ends the index
//      capacity (cap_, doubling from 4096) has already grown far past the
//      final population reached (verified empirically: index crosses its
//      LAST doubling threshold for ~900K rows well before phase 2 ends,
//      leaving ~800K+ rows of headroom -- an order of magnitude more than
//      this 100K-record probe's own ~45K-row net growth -- before the NEXT
//      doubling could fire), so no index/chunk growth allocation is
//      possible inside this window. table.drainDirty() (itself alloc-free
//      by inspection: two std::vector::clear() calls, capacity retained)
//      is called once, OUTSIDE the tracked window, immediately beforehand
//      to flush the per-rid dirty-tracking side vectors (dirtyList_/
//      deleted_) back to size 0 at their already-large retained capacity
//      -- removing the one other (amortized, standard-vector) growth
//      vector that is not de-amortized/pre-provisioned the way the index
//      is, and that this benchmark would otherwise never drain on its own
//      (a real system drains it every checkpoint interval; Task 11 does
//      the same here, once, for the same reason).
//
// BM_ViewFoldAggregate (Phase 2a) reuses this exact harness (op stream, batch
// building, the three phases, percentile/counter reporting) for the aggregate
// fold; see the comment above it for what is warm (contributors primed).
//
// All batch bytes are pre-built (RecordBatchBuilder::build(), CRC-stamped,
// ensureValid()-passing -- a real, not synthetic, wire format) BEFORE any
// timed/tracked phase; building/grouping the op stream is harness cost,
// never measured.
#include <benchmark/benchmark.h>
#include <rune/codec/Descriptor.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/AggFold.h>
#include <rune/view/AggState.h>
#include <rune/view/ViewFold.h>
#include <rune/view/ViewRow.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// Defined once in benchmark/BenchmarkMain.cpp; shared by every benchmark TU
// that needs the zero-alloc assertion (see that file's header comment).
extern std::atomic<int64_t> g_allocCount;
extern std::atomic<bool>    g_allocEnabled;

namespace {

using namespace rune::view;
using rune::codec::PrimType;

// --- Fixtures, mirroring libs/view/tests/view/ViewFoldTest.cpp exactly ---

// v1 "character" source: {characterId: Int64 key (tag 0), hp: Int32 (tag 1)}.
// Every record in this workload is written against this SAME version (1) --
// the common case VersionDescriptorCache is designed for (one allocation,
// ever, during warmup; every later lookup is a hash hit, no allocation).
rune::codec::Descriptor charSourceV1() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", PrimType::Int64, 1, /*isKey*/ true)
        .add("hp", PrimType::Int32)
        .build();
}

rune::view::ViewPlan hpView() {
    rune::view::ViewPlan v;
    v.viewName = "char_hp";
    v.key      = "characterId";
    v.sources  = {{5, "characterId"}};
    v.output   = {{"characterId", PrimType::Int64, 0, "characterId", rune::view::MergeRule::Replace},
                  {"hp", PrimType::Int32, 0, "hp", rune::view::MergeRule::Replace}};
    return v;
}

// 8-byte little-endian encoding of an Int64 key -- the wire ROUTING
// (compaction/merge) key, per ViewFold.cpp's documented model.
std::vector<uint8_t> keyBytes(int64_t id) {
    std::vector<uint8_t> b(8);
    for (int i = 0; i < 8; ++i) b[static_cast<size_t>(i)] = static_cast<uint8_t>(uint64_t(id) >> (8 * i));
    return b;
}

std::string_view asSv(const std::vector<uint8_t>& b) {
    return std::string_view(reinterpret_cast<const char*>(b.data()), b.size());
}

// Hand-frames a codec record VALUE: [blockLength u16][version u16][id i64][hp i32].
std::vector<uint8_t> encodeCharacter(int64_t id, int32_t hp, uint16_t version) {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    auto le32 = [&](int32_t v) { for (int i = 0; i < 4; ++i) b.push_back(uint8_t(uint32_t(v) >> (8 * i))); };
    auto le64 = [&](int64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(uint64_t(v) >> (8 * i))); };
    const uint16_t block = 8 + 4; // id(8) + hp(4)
    le16(block);
    le16(version);
    le64(id);
    le32(hp);
    return b;
}

// One churn op, pre-generated (harness bookkeeping, not part of what's
// measured): a brand-new key, an update of an existing key, or a tombstone.
struct Op {
    uint64_t key;
    int32_t  hp;
    bool     tombstone;
};

// Returns the `frac`-th percentile (e.g. 0.99 for p99) of `v`, sorting it in
// place. Shared pattern with benchmark/ViewTableBench.cpp.
double percentileUs(std::vector<uint32_t>& v, double frac) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    size_t i = static_cast<size_t>(v.size() * frac);
    if (i >= v.size()) i = v.size() - 1;
    return v[i] / 1000.0; // ns -> us
}

// --- Aggregate fixtures (Phase 2a) ---
// {orderId Int64 (key), userId Int64, amount Int64}
rune::codec::Descriptor orderSourceV1() {
    return rune::codec::DescriptorBuilder("order", 1)
        .add("orderId", PrimType::Int64, 1, /*isKey*/ true)
        .add("userId", PrimType::Int64)
        .add("amount", PrimType::Int64)
        .build();
}

rune::view::ViewPlan userTotalsView() {
    rune::view::ViewPlan v;
    v.viewName = "user_totals";
    v.key      = "userId";
    v.sources  = {{5, "userId"}};
    v.output   = {{"userId", PrimType::Int64, 0, "userId", rune::view::MergeRule::Replace},
                  {"n", PrimType::Int64, 0, "", rune::view::MergeRule::CountAll},
                  {"total", PrimType::Int64, 0, "amount", rune::view::MergeRule::Sum},
                  {"avg", PrimType::Float64, 0, "amount", rune::view::MergeRule::Avg}};
    return v;
}

// --- Retractable aggregate fixtures (Phase 2b) ---
// user_totals' Count(*)/Sum plus the four list rules over amount: a LINKED view.
rune::view::ViewPlan userStatsView() {
    rune::view::ViewPlan v;
    v.viewName = "user_stats";
    v.key      = "userId";
    v.sources  = {{5, "userId"}};
    v.output   = {{"userId", PrimType::Int64, 0, "userId", rune::view::MergeRule::Replace},
                  {"n", PrimType::Int64, 0, "", rune::view::MergeRule::CountAll},
                  {"total", PrimType::Int64, 0, "amount", rune::view::MergeRule::Sum},
                  {"max", PrimType::Int64, 0, "amount", rune::view::MergeRule::Max},
                  {"min", PrimType::Int64, 0, "amount", rune::view::MergeRule::Min},
                  {"last", PrimType::Int64, 0, "amount", rune::view::MergeRule::Last},
                  {"lastNn", PrimType::Int64, 0, "amount", rune::view::MergeRule::LastNonNull}};
    return v;
}

// One group's Count(*) + Max(amount): isolates the O(group) rescan.
rune::view::ViewPlan groupMaxView() {
    rune::view::ViewPlan v;
    v.viewName = "group_max";
    v.key      = "userId";
    v.sources  = {{5, "userId"}};
    v.output   = {{"userId", PrimType::Int64, 0, "userId", rune::view::MergeRule::Replace},
                  {"n", PrimType::Int64, 0, "", rune::view::MergeRule::CountAll},
                  {"max", PrimType::Int64, 0, "amount", rune::view::MergeRule::Max}};
    return v;
}
constexpr int64_t kExtremeGroup = 7;

// [blockLength u16 = 24][version u16][orderId i64][userId i64][amount i64]
std::vector<uint8_t> encodeOrder(int64_t id, int64_t user, int64_t amount, uint16_t version) {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    auto le64 = [&](int64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(uint64_t(v) >> (8 * i))); };
    le16(24); le16(version); le64(id); le64(user); le64(amount);
    return b;
}

// --- Shared harness (used by BM_ViewFoldReplace and BM_ViewFoldAggregate) ---

constexpr uint64_t kWarmupOps     = 20'000;
constexpr uint64_t kMeasuredOps   = 2'000'000;
constexpr uint64_t kAllocProbeOps = 100'000;
constexpr uint64_t kTotalOps      = kWarmupOps + kMeasuredOps + kAllocProbeOps;
// Small-batch cycle (avg 2 records/batch) -- "single-record (or small)
// batches" per the brief -- so record_p* and batch_p* are genuinely
// distinct populations (see file header).
constexpr int kBatchSizes[] = {1, 2, 1, 3, 2, 1, 4, 2};

// Pre-generates the 60% new key / 25% update / 15% tombstone churn stream
// (NOT timed).
std::vector<Op> generateChurnOps(uint64_t total) {
    std::vector<uint64_t> alive;
    alive.reserve(static_cast<size_t>(total * 0.65) + 16);
    std::unordered_map<uint64_t, uint32_t> idx;
    idx.reserve(static_cast<size_t>(total * 1.3));

    std::mt19937_64 rng(0xF01DF01Dull);
    std::uniform_int_distribution<int> opDist(0, 99);
    std::uniform_int_distribution<size_t> pick;

    uint64_t nextKey = 1;
    std::vector<Op> ops;
    ops.reserve(total);

    for (uint64_t i = 0; i < total; ++i) {
        const int r = opDist(rng);
        const bool doCreate = (r < 60) || alive.empty();
        const bool doUpdate = !doCreate && (r < 85);

        if (doCreate) {
            const uint64_t key = nextKey++;
            idx[key] = static_cast<uint32_t>(alive.size());
            alive.push_back(key);
            ops.push_back(Op{key, static_cast<int32_t>(key), false});
        } else if (doUpdate) {
            pick.param(std::uniform_int_distribution<size_t>::param_type(0, alive.size() - 1));
            const uint64_t key = alive[pick(rng)];
            ops.push_back(Op{key, static_cast<int32_t>(key ^ i), false});
        } else {
            pick.param(std::uniform_int_distribution<size_t>::param_type(0, alive.size() - 1));
            const size_t pos = pick(rng);
            const uint64_t key = alive[pos];
            const uint32_t lastIdx = static_cast<uint32_t>(alive.size() - 1);
            const uint64_t lastKey = alive[lastIdx];
            alive[pos] = lastKey;
            idx[lastKey] = static_cast<uint32_t>(pos);
            alive.pop_back();
            idx.erase(key);
            ops.push_back(Op{key, 0, true});
        }
    }
    return ops;
}

// Real, CRC-stamped wire bytes grouped into small committed batches.
struct BatchStream {
    std::vector<uint8_t>  bytes;
    std::vector<size_t>   byteOffset;
    std::vector<uint32_t> byteLen;
    std::vector<uint32_t> recCount;
    std::vector<int64_t>  baseOffset;
    size_t size() const { return baseOffset.size(); }

    rune::storage::LogReadView view(size_t i) const {
        return rune::storage::LogReadView{bytes.data() + byteOffset[i], byteLen[i],
                                          baseOffset[i], baseOffset[i] + recCount[i]};
    }
    int64_t bound(size_t i) const { return baseOffset[i] + recCount[i]; }
};

// `append(RecordBatchBuilder&, const Op&)` writes one record (NOT timed).
template <class AppendFn>
BatchStream buildStream(const std::vector<Op>& ops, const int* sizes, size_t sizesLen,
                        AppendFn&& append) {
    BatchStream s;
    s.bytes.reserve(ops.size() * 48);
    s.byteOffset.reserve(ops.size());
    s.byteLen.reserve(ops.size());
    s.recCount.reserve(ops.size());
    s.baseOffset.reserve(ops.size());

    size_t opIdx = 0, sizeIdx = 0;
    int64_t nextOffset = 0;
    while (opIdx < ops.size()) {
        const uint32_t want = static_cast<uint32_t>(sizes[sizeIdx % sizesLen]);
        ++sizeIdx;
        const uint32_t n = static_cast<uint32_t>(std::min<size_t>(want, ops.size() - opIdx));

        rune::protocol::RecordBatchBuilder bb(nextOffset, 0);
        for (uint32_t j = 0; j < n; ++j) append(bb, ops[opIdx + j]);

        const size_t byteOff = s.bytes.size();
        auto bytes = bb.build();
        s.bytes.insert(s.bytes.end(), bytes.begin(), bytes.end());

        s.baseOffset.push_back(nextOffset);
        s.byteOffset.push_back(byteOff);
        s.byteLen.push_back(static_cast<uint32_t>(bytes.size()));
        s.recCount.push_back(n);

        nextOffset += n;
        opIdx += n;
    }
    return s;
}

struct PhaseResult {
    std::vector<uint32_t> batchLatNs;      // one sample per timed fold call (any batch size 1-4)
    std::vector<uint32_t> trueRecordLatNs; // one sample per timed fold call of EXACTLY 1 record
    uint64_t applied = 0, deleted = 0, skipped = 0;
    uint64_t warmupRecs = 0, measuredRecs = 0, probeRecs = 0;
    int64_t  steadyAlloc = 0;
};

// Runs the three phases (see file header): WARMUP, MEASURED latency pass,
// STEADY alloc-check pass. `foldOne(i)` folds batch i and returns FoldStats;
// `drainAll()` flushes every table's dirty-tracking side vectors (stands in
// for checkpointing). If `drainEvery` > 0, drainAll() additionally runs
// after every `drainEvery` batches, OUTSIDE the timed region, in all three
// phases (a real system drains each checkpoint interval); drainAll() also
// always runs once immediately before the alloc probe.
template <class FoldFn, class DrainFn>
PhaseResult runPhases(const BatchStream& s, FoldFn&& foldOne, DrainFn&& drainAll,
                      uint64_t drainEvery) {
    PhaseResult r;
    size_t bi = 0;
    uint64_t sinceDrain = 0;
    auto maybeDrain = [&] {
        if (drainEvery != 0 && ++sinceDrain >= drainEvery) {
            drainAll();
            sinceDrain = 0;
        }
    };
    auto tally = [&](const FoldStats& fs) {
        r.applied += fs.applied;
        r.deleted += fs.deleted;
        r.skipped += fs.skippedMalformed + fs.skippedControl;
    };

    // Phase 1: WARMUP (NOT timed, NOT alloc-tracked).
    while (bi < s.size() && r.warmupRecs < kWarmupOps) {
        foldOne(bi);
        r.warmupRecs += s.recCount[bi];
        ++bi;
        maybeDrain();
    }

    // Phase 2: MEASURED latency pass (timed, NOT alloc-tracked).
    r.batchLatNs.reserve(kMeasuredOps / 2 + 16);
    // Only size-1-batch calls land here: a size-1 call's whole latency IS one
    // record's pipeline cost, with no batch-overhead amortization to divide
    // out (~37% of batches; ~375K samples over the 2M-record pass).
    r.trueRecordLatNs.reserve(kMeasuredOps / 2 + 16);
    while (bi < s.size() && r.measuredRecs < kMeasuredOps) {
        const uint32_t n = s.recCount[bi];

        const auto t0 = std::chrono::steady_clock::now();
        auto stats = foldOne(bi);
        const auto t1 = std::chrono::steady_clock::now();

        const uint64_t ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
        r.batchLatNs.push_back(static_cast<uint32_t>(ns));
        if (n == 1) r.trueRecordLatNs.push_back(static_cast<uint32_t>(ns));

        tally(stats);
        r.measuredRecs += n;
        ++bi;
        maybeDrain();
    }

    // Flush the dirty-tracking side vectors back to size 0 at their retained
    // capacity so the probe window starts with maximal headroom (see header).
    drainAll();
    sinceDrain = 0;

    // Phase 3: STEADY ALLOC-CHECK pass.
    g_allocCount.store(0, std::memory_order_relaxed);
    g_allocEnabled.store(true, std::memory_order_relaxed);
    while (bi < s.size() && r.probeRecs < kAllocProbeOps) {
        tally(foldOne(bi));
        r.probeRecs += s.recCount[bi];
        ++bi;
        maybeDrain(); // drainDirty() itself must not allocate either
    }
    g_allocEnabled.store(false, std::memory_order_relaxed);
    r.steadyAlloc = g_allocCount.load(std::memory_order_relaxed);
    return r;
}

// Reports the shared counters under `prefix` ("" for Replace, "agg_" for
// the aggregate bench).
void reportCommon(benchmark::State& st, const char* prefix, PhaseResult& r, size_t finalSize) {
    auto set = [&](const char* name, double v) { st.counters[std::string(prefix) + name] = v; };
    set("record_p50_us", percentileUs(r.trueRecordLatNs, 0.50));
    set("record_p99_us", percentileUs(r.trueRecordLatNs, 0.99)); // GATE: < 5us
    set("batch_p50_us",  percentileUs(r.batchLatNs, 0.50));
    set("batch_p99_us",  percentileUs(r.batchLatNs, 0.99));      // GATE: < 10us
    set("steady_alloc",  static_cast<double>(r.steadyAlloc));    // GATE: == 0
    set("true_record_samples", static_cast<double>(r.trueRecordLatNs.size()));
    set("measured_batches",    static_cast<double>(r.batchLatNs.size()));
    set("alloc_probe_records", static_cast<double>(r.probeRecs));
    set("applied_total",  static_cast<double>(r.applied));
    set("deleted_total",  static_cast<double>(r.deleted));
    set("skipped_total",  static_cast<double>(r.skipped));
    set("final_size",     static_cast<double>(finalSize));
}

// --- Shared aggregate rig (BM_ViewFoldAggregate, BM_ViewFoldRetractable) ---
// 1,000,000 contributors / 10,000 groups, amounts from the churn's `hp`.
constexpr uint64_t kAggContributors = 1'000'000;
constexpr uint64_t kAggGroups       = 10'000;

struct AggRig {
    rune::codec::Descriptor   src  = orderSourceV1();
    rune::view::CompileResult comp;
    rune::view::AggLayout     agg;
    ViewTable                 view, contributors, groups;
    VersionDescriptorCache    vdc;
    uint64_t                  rescans = 0;   // summed FoldStats::aggRescans (0 for a 2a-only view)

    AggRig(const rune::view::ViewPlan& plan, int32_t viewTopicId)
        : comp(compile(plan, src, viewTopicId)),
          agg(rune::view::buildAggLayout(comp.plan, src).value()),
          view(comp.rowLayout), contributors(agg.contributorRow), groups(agg.groupRow), vdc(src) {}

    static rune::view::CompileResult compile(const rune::view::ViewPlan& plan,
                                             const rune::codec::Descriptor& src, int32_t id) {
        const rune::codec::Descriptor* srcs[] = {&src};
        return compileView(plan, srcs, id).value();
    }

    // Contributor id c in [0, 1M); group fixed per c.
    static void append(rune::protocol::RecordBatchBuilder& bb, const Op& op) {
        const int64_t c = static_cast<int64_t>(op.key % kAggContributors);
        const auto kb = keyBytes(c);
        if (op.tombstone) {
            bb.append(asSv(kb), std::string_view{}, 0);
        } else {
            const auto val = encodeOrder(c, c % kAggGroups, static_cast<int64_t>(op.hp), 1);
            bb.append(asSv(kb), asSv(val), 0);
        }
    }
    void drainAll() {
        auto nop = [](uint32_t, const uint8_t*, bool, uint64_t) {};
        view.drainDirty(nop);
        contributors.drainDirty(nop);
        groups.drainDirty(nop);
    }
    rune::view::FoldStats foldStream(const BatchStream& s, size_t i) {
        auto lv = s.view(i);
        int64_t cur = 0;
        const auto fs = foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, view, contributors,
                                           groups, lv, s.bound(i), cur);
        rescans += fs.aggRescans;
        return fs;
    }
    // Creates-only fold over the whole contributor space (NOT timed), then drains;
    // the tables reach their final population and capacity. Resets `rescans`.
    void prime() {
        constexpr int kPrimeBatch[] = {64};
        std::vector<Op> ops;
        ops.reserve(kAggContributors);
        for (uint64_t c = 0; c < kAggContributors; ++c)
            ops.push_back(Op{c, static_cast<int32_t>(c ^ 0x5a5a), false});
        const auto ps = buildStream(ops, kPrimeBatch, 1, append);
        for (size_t i = 0; i < ps.size(); ++i) foldStream(ps, i);
        drainAll();
        rescans = 0;
    }
};

} // namespace

static void BM_ViewFoldReplace(benchmark::State& st) {
    for (auto _ : st) {
        auto src = charSourceV1();
        const rune::codec::Descriptor* srcs[] = {&src};
        auto comp = compileView(hpView(), srcs, 0x51000001).value();

        ViewTable table(comp.rowLayout);
        VersionDescriptorCache vdc(src);

        const auto ops = generateChurnOps(kTotalOps);
        const auto stream = buildStream(
            ops, kBatchSizes, std::size(kBatchSizes),
            [](rune::protocol::RecordBatchBuilder& bb, const Op& op) {
                const auto kb = keyBytes(static_cast<int64_t>(op.key));
                if (op.tombstone) {
                    bb.append(asSv(kb), std::string_view{}, 0);
                } else {
                    const auto val = encodeCharacter(static_cast<int64_t>(op.key), op.hp, 1);
                    bb.append(asSv(kb), asSv(val), 0);
                }
            });

        auto foldOne = [&](size_t i) {
            auto lv = stream.view(i);
            int64_t cur = 0;
            return foldBatch(comp.plan, comp.rowLayout, vdc, table, lv, stream.bound(i), cur);
        };
        // Replace: one drain right before the alloc probe (unchanged from Task 11).
        auto drainAll = [&] { table.drainDirty([](uint32_t, const uint8_t*, bool, uint64_t) {}); };

        auto r = runPhases(stream, foldOne, drainAll, /*drainEvery*/ 0);

        benchmark::DoNotOptimize(table.size());
        reportCommon(st, "", r, table.size());
    }
}
BENCHMARK(BM_ViewFoldReplace)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(1)
    ->UseRealTime();

// Aggregate fold gate (Phase 2a): user_totals grouped by userId over
// 1,000,000 contributors / 10,000 groups. What is warm before timing:
//   - contributors is PRIMED (creates-only fold over the whole contributor
//     space [0, 1M)), so that table is at its final population and capacity
//     (index, chunks, dirty vectors) and every timed update/delete hits a
//     LIVE contributor -> each upsert is a retract + re-add of the same
//     group (the realistic hot path). agg_retract_readd_ops proves how many
//     timed ops took that path.
//   - groups and the output view are populated by the priming too (10,000
//     rows each); VersionDescriptorCache is warmed by the priming.
//   - all three tables are drained every kDrainEveryBatches batches (outside
//     the timed region; mimics checkpointing) and right before the probe.
//     The contributors table cannot grow in the probe window (population is
//     bounded by the 1M key space and its capacity was reached while priming).
static void BM_ViewFoldAggregate(benchmark::State& st) {
    constexpr uint64_t kContributors      = kAggContributors;   // the liveness proof below uses it
    constexpr uint64_t kDrainEveryBatches = 1000;

    for (auto _ : st) {
        AggRig rig(userTotalsView(), 0x51000002);
        auto& view = rig.view;
        auto& contributors = rig.contributors;
        auto& groups = rig.groups;
        auto drainAll = [&] { rig.drainAll(); };
        auto foldStream = [&](const BatchStream& s, size_t i) { return rig.foldStream(s, i); };
        auto append = &AggRig::append;
        rig.prime();

        const auto ops = generateChurnOps(kTotalOps);
        const auto stream = buildStream(ops, kBatchSizes, std::size(kBatchSizes), append);

        auto r = runPhases(
            stream, [&](size_t i) { return foldStream(stream, i); }, drainAll, kDrainEveryBatches);

        // --- Retract+re-add proof (post-run bookkeeping, outside every timed
        //     region): replay liveness over the op stream; a timed upsert whose
        //     contributor was already live took the retract + re-add path.
        uint64_t retractReadd = 0, timedUpserts = 0, deleteOfLive = 0;
        {
            std::vector<bool> live(kContributors, true); // primed
            const uint64_t timedEnd = r.warmupRecs + r.measuredRecs;
            for (uint64_t i = 0; i < timedEnd; ++i) {
                const size_t c = static_cast<size_t>(ops[i].key % kContributors);
                const bool timed = i >= r.warmupRecs;
                if (ops[i].tombstone) {
                    if (timed && live[c]) ++deleteOfLive;
                    live[c] = false;
                } else {
                    if (timed) {
                        ++timedUpserts;
                        if (live[c]) ++retractReadd;
                    }
                    live[c] = true;
                }
            }
        }

        benchmark::DoNotOptimize(view.size());
        benchmark::DoNotOptimize(groups.size());
        benchmark::DoNotOptimize(contributors.size());
        reportCommon(st, "agg_", r, view.size());
        st.counters["agg_retract_readd_ops"]  = static_cast<double>(retractReadd);
        st.counters["agg_timed_upsert_ops"]   = static_cast<double>(timedUpserts);
        st.counters["agg_delete_of_live_ops"] = static_cast<double>(deleteOfLive);
        st.counters["agg_contributors_final"] = static_cast<double>(contributors.size());

        const double recP99 = st.counters["agg_record_p99_us"];
        const double batP99 = st.counters["agg_batch_p99_us"];
        const bool pass = recP99 < 5.0 && batP99 < 10.0 && r.steadyAlloc == 0;
        st.counters["agg_gate_pass"] = pass ? 1.0 : 0.0;
        std::fprintf(stderr,
                     "[ViewFoldAggregate gate] agg_record_p99_us=%.3f (<5) agg_batch_p99_us=%.3f (<10) "
                     "steady_alloc=%lld (==0) => %s\n",
                     recP99, batP99, static_cast<long long>(r.steadyAlloc), pass ? "PASS" : "FAIL");
    }
}
BENCHMARK(BM_ViewFoldAggregate)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(1)
    ->UseRealTime();

// Retractable aggregate gate (Phase 2b, §14.0): user_stats = Count(*), Sum,
// Max, Min, Last, LastNonNull over amount, grouped by userId — the SAME rig,
// workload, priming, drain cadence and three phases as BM_ViewFoldAggregate
// (shared AggRig: 1,000,000 contributors / 10,000 groups, ~100 per group). The
// churn's pseudo-random amounts retract a group's cached Min or Max on roughly
// 2% of retracting ops, so this IS the typical case including its occasional
// O(group) rescans (ret_rescans counts them). BM_ViewFoldExtremeRetract
// isolates that path. GATE: ret_record_p99_us < 5, ret_batch_p99_us < 10,
// steady_alloc == 0.
static void BM_ViewFoldRetractable(benchmark::State& st) {
    constexpr uint64_t kDrainEveryBatches = 1000;

    for (auto _ : st) {
        AggRig rig(userStatsView(), 0x51000003);
        rig.prime();   // also zeroes rescans: count the churn's only

        const auto ops = generateChurnOps(kTotalOps);
        const auto stream = buildStream(ops, kBatchSizes, std::size(kBatchSizes), AggRig::append);
        auto r = runPhases(
            stream, [&](size_t i) { return rig.foldStream(stream, i); }, [&] { rig.drainAll(); },
            kDrainEveryBatches);

        benchmark::DoNotOptimize(rig.view.size());
        benchmark::DoNotOptimize(rig.groups.size());
        benchmark::DoNotOptimize(rig.contributors.size());
        reportCommon(st, "ret_", r, rig.view.size());
        st.counters["ret_rescans"]            = static_cast<double>(rig.rescans);
        st.counters["ret_contributors_final"] = static_cast<double>(rig.contributors.size());

        const double recP99 = st.counters["ret_record_p99_us"];
        const double batP99 = st.counters["ret_batch_p99_us"];
        const bool pass = recP99 < 5.0 && batP99 < 10.0 && r.steadyAlloc == 0;
        st.counters["ret_gate_pass"] = pass ? 1.0 : 0.0;
        std::fprintf(stderr,
                     "[ViewFoldRetractable gate] ret_record_p99_us=%.3f (<5) ret_batch_p99_us=%.3f (<10) "
                     "steady_alloc=%lld (==0) rescans=%llu => %s\n",
                     recP99, batP99, static_cast<long long>(r.steadyAlloc),
                     static_cast<unsigned long long>(rig.rescans), pass ? "PASS" : "FAIL");
    }
}
BENCHMARK(BM_ViewFoldRetractable)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(1)
    ->UseRealTime();
