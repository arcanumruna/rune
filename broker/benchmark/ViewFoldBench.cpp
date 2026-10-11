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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <random>
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

// [blockLength u16 = 24][version u16][orderId i64][userId i64][amount i64]
std::vector<uint8_t> encodeOrder(int64_t id, int64_t user, int64_t amount, uint16_t version) {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    auto le64 = [&](int64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(uint64_t(v) >> (8 * i))); };
    le16(24); le16(version); le64(id); le64(user); le64(amount);
    return b;
}

} // namespace

static void BM_ViewFoldReplace(benchmark::State& st) {
    constexpr uint64_t kWarmupOps     = 20'000;
    constexpr uint64_t kMeasuredOps   = 2'000'000;
    constexpr uint64_t kAllocProbeOps = 100'000;
    constexpr uint64_t kTotalOps      = kWarmupOps + kMeasuredOps + kAllocProbeOps;
    // Small-batch cycle (avg 2 records/batch) -- "single-record (or small)
    // batches" per the brief -- so record_p* and batch_p* are genuinely
    // distinct populations (see file header).
    constexpr int kBatchSizes[] = {1, 2, 1, 3, 2, 1, 4, 2};
    constexpr size_t kBatchSizesLen = sizeof(kBatchSizes) / sizeof(kBatchSizes[0]);

    for (auto _ : st) {
        auto src = charSourceV1();
        const rune::codec::Descriptor* srcs[] = {&src};
        auto comp = compileView(hpView(), srcs, 0x51000001).value();

        ViewTable table(comp.rowLayout);
        VersionDescriptorCache vdc(src);

        // --- Pre-generate the churn op stream (NOT timed) -----------------
        std::vector<uint64_t> alive;
        alive.reserve(static_cast<size_t>(kTotalOps * 0.65) + 16);
        std::unordered_map<uint64_t, uint32_t> idx;
        idx.reserve(static_cast<size_t>(kTotalOps * 1.3));

        std::mt19937_64 rng(0xF01DF01Dull);
        std::uniform_int_distribution<int> opDist(0, 99);
        std::uniform_int_distribution<size_t> pick;

        uint64_t nextKey = 1;
        std::vector<Op> ops;
        ops.reserve(kTotalOps);

        for (uint64_t i = 0; i < kTotalOps; ++i) {
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

        // --- Group ops into small committed batches; build real,
        //     CRC-stamped wire bytes (NOT timed). -----------------------
        std::vector<uint8_t> allBytes;
        allBytes.reserve(ops.size() * 48);
        std::vector<size_t>  batchByteOffset;
        std::vector<uint32_t> batchByteLen;
        std::vector<uint32_t> batchRecCount;
        std::vector<int64_t>  batchBaseOffset;
        batchByteOffset.reserve(ops.size());
        batchByteLen.reserve(ops.size());
        batchRecCount.reserve(ops.size());
        batchBaseOffset.reserve(ops.size());

        {
            size_t opIdx = 0;
            size_t sizeIdx = 0;
            int64_t nextOffset = 0;
            while (opIdx < ops.size()) {
                const uint32_t want = static_cast<uint32_t>(kBatchSizes[sizeIdx % kBatchSizesLen]);
                ++sizeIdx;
                const uint32_t n = static_cast<uint32_t>(std::min<size_t>(want, ops.size() - opIdx));

                rune::protocol::RecordBatchBuilder bb(nextOffset, 0);
                for (uint32_t j = 0; j < n; ++j) {
                    const Op& op = ops[opIdx + j];
                    const auto kb = keyBytes(static_cast<int64_t>(op.key));
                    if (op.tombstone) {
                        bb.append(asSv(kb), std::string_view{}, 0);
                    } else {
                        const auto val = encodeCharacter(static_cast<int64_t>(op.key), op.hp, 1);
                        bb.append(asSv(kb), asSv(val), 0);
                    }
                }

                const size_t byteOff = allBytes.size();
                auto bytes = bb.build();
                allBytes.insert(allBytes.end(), bytes.begin(), bytes.end());

                batchBaseOffset.push_back(nextOffset);
                batchByteOffset.push_back(byteOff);
                batchByteLen.push_back(static_cast<uint32_t>(bytes.size()));
                batchRecCount.push_back(n);

                nextOffset += n;
                opIdx += n;
            }
        }

        const size_t numBatches = batchBaseOffset.size();

        auto foldOneBatch = [&](size_t i) {
            const uint32_t n = batchRecCount[i];
            rune::storage::LogReadView view{
                allBytes.data() + batchByteOffset[i], batchByteLen[i],
                batchBaseOffset[i], batchBaseOffset[i] + n};
            int64_t cur = 0;
            return foldBatch(comp.plan, comp.rowLayout, vdc, table, view, batchBaseOffset[i] + n, cur);
        };

        // --- Phase 1: WARMUP (NOT timed, NOT alloc-tracked). See file
        //     header for why. -------------------------------------------
        size_t bi = 0;
        uint64_t recFolded = 0;
        while (bi < numBatches && recFolded < kWarmupOps) {
            foldOneBatch(bi);
            recFolded += batchRecCount[bi];
            ++bi;
        }

        // --- Phase 2: MEASURED LATENCY pass (timed, NOT alloc-tracked):
        //     record_p50/p99 (TRUE per-record, size-1-batch calls only),
        //     batch_p50/p99 (every foldBatch() call, any size). ------------
        std::vector<uint32_t> batchLatNs;
        batchLatNs.reserve(kMeasuredOps / 2 + 16);
        // Only size-1-batch calls are pushed here -- see file header: a
        // size-1 foldBatch() call's whole latency IS one record's pipeline
        // cost, with no batch-overhead amortization to divide out. ~37% of
        // batches are size 1 (the kBatchSizes cycle below), so this is a
        // large (~375K over a 2M-record pass), genuinely single-record
        // subpopulation -- not a quotient smoothed across a multi-record
        // batch.
        std::vector<uint32_t> trueRecordLatNs;
        trueRecordLatNs.reserve(kMeasuredOps / 2 + 16);

        uint64_t appliedTotal = 0, deletedTotal = 0, skippedTotal = 0;
        uint64_t measuredRecFolded = 0;
        while (bi < numBatches && measuredRecFolded < kMeasuredOps) {
            const uint32_t n = batchRecCount[bi];

            const auto t0 = std::chrono::steady_clock::now();
            auto stats = foldOneBatch(bi);
            const auto t1 = std::chrono::steady_clock::now();

            const uint64_t ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
            batchLatNs.push_back(static_cast<uint32_t>(ns));
            if (n == 1) {
                // Genuine single-record fold: no division, no amortization.
                trueRecordLatNs.push_back(static_cast<uint32_t>(ns));
            }

            appliedTotal += stats.applied;
            deletedTotal += stats.deleted;
            skippedTotal += stats.skippedMalformed + stats.skippedControl;

            measuredRecFolded += n;
            ++bi;
        }

        // Flush the per-rid dirty-tracking side vectors (dirtyList_/
        // deleted_) back to size 0 at their already-large retained
        // capacity -- itself alloc-free (two clear() calls) -- so the
        // alloc-check window below cannot see one of their own (ordinary,
        // non-pre-provisioned) amortized growth reallocations. See file
        // header.
        table.drainDirty([](uint32_t, const uint8_t*, bool, uint64_t) {});

        // --- Phase 3: STEADY ALLOC-CHECK pass. By this point the index's
        //     capacity has already grown (via its de-amortized,
        //     pre-provisioned doubling machinery) far past this probe's
        //     own modest net growth -- see file header for the headroom
        //     arithmetic -- so this window is genuinely allocation-free
        //     BY DESIGN, not by luck. -------------------------------------
        g_allocCount.store(0, std::memory_order_relaxed);
        g_allocEnabled.store(true, std::memory_order_relaxed);

        uint64_t probeRecFolded = 0;
        while (bi < numBatches && probeRecFolded < kAllocProbeOps) {
            auto stats = foldOneBatch(bi);
            appliedTotal += stats.applied;
            deletedTotal += stats.deleted;
            skippedTotal += stats.skippedMalformed + stats.skippedControl;
            probeRecFolded += batchRecCount[bi];
            ++bi;
        }

        g_allocEnabled.store(false, std::memory_order_relaxed);
        const int64_t steadyAlloc = g_allocCount.load(std::memory_order_relaxed);

        benchmark::DoNotOptimize(table.size());

        // record_p50/p99: TRUE per-record order statistic (size-1-batch
        // calls only -- see file header and phase-2 loop comment above; NOT
        // a batch-latency/n quotient).
        st.counters["record_p50_us"] = percentileUs(trueRecordLatNs, 0.50);
        st.counters["record_p99_us"] = percentileUs(trueRecordLatNs, 0.99); // GATE: < 5us
        st.counters["batch_p50_us"]  = percentileUs(batchLatNs, 0.50);
        st.counters["batch_p99_us"]  = percentileUs(batchLatNs, 0.99);  // GATE: < 10us
        st.counters["steady_alloc"]  = static_cast<double>(steadyAlloc); // GATE: == 0
        st.counters["true_record_samples"] = static_cast<double>(trueRecordLatNs.size());
        st.counters["measured_batches"] = static_cast<double>(batchLatNs.size());
        st.counters["alloc_probe_records"] = static_cast<double>(probeRecFolded);
        st.counters["applied_total"]    = static_cast<double>(appliedTotal);
        st.counters["deleted_total"]    = static_cast<double>(deletedTotal);
        st.counters["skipped_total"]    = static_cast<double>(skippedTotal);
        st.counters["final_size"]       = static_cast<double>(table.size());
    }
}
BENCHMARK(BM_ViewFoldReplace)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(1)
    ->UseRealTime();

static void BM_ViewFoldAggregate(benchmark::State& st) {
    constexpr uint64_t kWarmupOps     = 20'000;
    constexpr uint64_t kMeasuredOps   = 2'000'000;
    constexpr uint64_t kAllocProbeOps = 100'000;
    constexpr uint64_t kTotalOps      = kWarmupOps + kMeasuredOps + kAllocProbeOps;
    // Small-batch cycle (avg 2 records/batch) -- "single-record (or small)
    // batches" per the brief -- so record_p* and batch_p* are genuinely
    // distinct populations (see file header).
    constexpr int kBatchSizes[] = {1, 2, 1, 3, 2, 1, 4, 2};
    constexpr size_t kBatchSizesLen = sizeof(kBatchSizes) / sizeof(kBatchSizes[0]);

    for (auto _ : st) {
        auto src = orderSourceV1();
        const rune::codec::Descriptor* srcs[] = {&src};
        auto comp = compileView(userTotalsView(), srcs, 0x51000002).value();
        auto agg = rune::view::buildAggLayout(comp.plan, src).value();

        ViewTable view(comp.rowLayout);
        ViewTable contributors(agg.contributorRow), groups(agg.groupRow);
        VersionDescriptorCache vdc(src);

        // --- Pre-generate the churn op stream (NOT timed) -----------------
        std::vector<uint64_t> alive;
        alive.reserve(static_cast<size_t>(kTotalOps * 0.65) + 16);
        std::unordered_map<uint64_t, uint32_t> idx;
        idx.reserve(static_cast<size_t>(kTotalOps * 1.3));

        std::mt19937_64 rng(0xF01DF01Dull);
        std::uniform_int_distribution<int> opDist(0, 99);
        std::uniform_int_distribution<size_t> pick;

        uint64_t nextKey = 1;
        std::vector<Op> ops;
        ops.reserve(kTotalOps);

        for (uint64_t i = 0; i < kTotalOps; ++i) {
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

        // --- Group ops into small committed batches; build real,
        //     CRC-stamped wire bytes (NOT timed). -----------------------
        std::vector<uint8_t> allBytes;
        allBytes.reserve(ops.size() * 48);
        std::vector<size_t>  batchByteOffset;
        std::vector<uint32_t> batchByteLen;
        std::vector<uint32_t> batchRecCount;
        std::vector<int64_t>  batchBaseOffset;
        batchByteOffset.reserve(ops.size());
        batchByteLen.reserve(ops.size());
        batchRecCount.reserve(ops.size());
        batchBaseOffset.reserve(ops.size());

        {
            size_t opIdx = 0;
            size_t sizeIdx = 0;
            int64_t nextOffset = 0;
            while (opIdx < ops.size()) {
                const uint32_t want = static_cast<uint32_t>(kBatchSizes[sizeIdx % kBatchSizesLen]);
                ++sizeIdx;
                const uint32_t n = static_cast<uint32_t>(std::min<size_t>(want, ops.size() - opIdx));

                rune::protocol::RecordBatchBuilder bb(nextOffset, 0);
                for (uint32_t j = 0; j < n; ++j) {
                    const Op& op = ops[opIdx + j];
                    // Contributor id c in [0, 1M); group fixed per c.
                    const int64_t c = static_cast<int64_t>(op.key % 1'000'000ull);
                    const auto kb = keyBytes(c);
                    if (op.tombstone) {
                        bb.append(asSv(kb), std::string_view{}, 0);
                    } else {
                        const auto val = encodeOrder(c, c % 10'000, static_cast<int64_t>(op.hp), 1);
                        bb.append(asSv(kb), asSv(val), 0);
                    }
                }

                const size_t byteOff = allBytes.size();
                auto bytes = bb.build();
                allBytes.insert(allBytes.end(), bytes.begin(), bytes.end());

                batchBaseOffset.push_back(nextOffset);
                batchByteOffset.push_back(byteOff);
                batchByteLen.push_back(static_cast<uint32_t>(bytes.size()));
                batchRecCount.push_back(n);

                nextOffset += n;
                opIdx += n;
            }
        }

        const size_t numBatches = batchBaseOffset.size();

        auto foldOneBatch = [&](size_t i) {
            const uint32_t n = batchRecCount[i];
            rune::storage::LogReadView lv{
                allBytes.data() + batchByteOffset[i], batchByteLen[i],
                batchBaseOffset[i], batchBaseOffset[i] + n};
            int64_t cur = 0;
            return foldAggregateBatch(comp.plan, comp.rowLayout, agg, vdc, view, contributors, groups,
                                      lv, batchBaseOffset[i] + n, cur);
        };

        // --- Phase 1: WARMUP (NOT timed, NOT alloc-tracked). See file
        //     header for why. -------------------------------------------
        size_t bi = 0;
        uint64_t recFolded = 0;
        while (bi < numBatches && recFolded < kWarmupOps) {
            foldOneBatch(bi);
            recFolded += batchRecCount[bi];
            ++bi;
        }

        // --- Phase 2: MEASURED LATENCY pass (timed, NOT alloc-tracked):
        //     record_p50/p99 (TRUE per-record, size-1-batch calls only),
        //     batch_p50/p99 (every foldBatch() call, any size). ------------
        std::vector<uint32_t> batchLatNs;
        batchLatNs.reserve(kMeasuredOps / 2 + 16);
        // Only size-1-batch calls are pushed here -- see file header: a
        // size-1 foldBatch() call's whole latency IS one record's pipeline
        // cost, with no batch-overhead amortization to divide out. ~37% of
        // batches are size 1 (the kBatchSizes cycle below), so this is a
        // large (~375K over a 2M-record pass), genuinely single-record
        // subpopulation -- not a quotient smoothed across a multi-record
        // batch.
        std::vector<uint32_t> trueRecordLatNs;
        trueRecordLatNs.reserve(kMeasuredOps / 2 + 16);

        uint64_t appliedTotal = 0, deletedTotal = 0, skippedTotal = 0;
        uint64_t measuredRecFolded = 0;
        while (bi < numBatches && measuredRecFolded < kMeasuredOps) {
            const uint32_t n = batchRecCount[bi];

            const auto t0 = std::chrono::steady_clock::now();
            auto stats = foldOneBatch(bi);
            const auto t1 = std::chrono::steady_clock::now();

            const uint64_t ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
            batchLatNs.push_back(static_cast<uint32_t>(ns));
            if (n == 1) {
                // Genuine single-record fold: no division, no amortization.
                trueRecordLatNs.push_back(static_cast<uint32_t>(ns));
            }

            appliedTotal += stats.applied;
            deletedTotal += stats.deleted;
            skippedTotal += stats.skippedMalformed + stats.skippedControl;

            measuredRecFolded += n;
            ++bi;
        }

        // Flush the per-rid dirty-tracking side vectors (dirtyList_/
        // deleted_) back to size 0 at their already-large retained
        // capacity -- itself alloc-free (two clear() calls) -- so the
        // alloc-check window below cannot see one of their own (ordinary,
        // non-pre-provisioned) amortized growth reallocations. See file
        // header.
        view.drainDirty([](uint32_t, const uint8_t*, bool, uint64_t) {});

        // --- Phase 3: STEADY ALLOC-CHECK pass. By this point the index's
        //     capacity has already grown (via its de-amortized,
        //     pre-provisioned doubling machinery) far past this probe's
        //     own modest net growth -- see file header for the headroom
        //     arithmetic -- so this window is genuinely allocation-free
        //     BY DESIGN, not by luck. -------------------------------------
        g_allocCount.store(0, std::memory_order_relaxed);
        g_allocEnabled.store(true, std::memory_order_relaxed);

        uint64_t probeRecFolded = 0;
        while (bi < numBatches && probeRecFolded < kAllocProbeOps) {
            auto stats = foldOneBatch(bi);
            appliedTotal += stats.applied;
            deletedTotal += stats.deleted;
            skippedTotal += stats.skippedMalformed + stats.skippedControl;
            probeRecFolded += batchRecCount[bi];
            ++bi;
        }

        g_allocEnabled.store(false, std::memory_order_relaxed);
        const int64_t steadyAlloc = g_allocCount.load(std::memory_order_relaxed);

        benchmark::DoNotOptimize(view.size());
        benchmark::DoNotOptimize(groups.size());

        // record_p50/p99: TRUE per-record order statistic (size-1-batch
        // calls only -- see file header and phase-2 loop comment above; NOT
        // a batch-latency/n quotient).
        st.counters["agg_record_p50_us"] = percentileUs(trueRecordLatNs, 0.50);
        st.counters["agg_record_p99_us"] = percentileUs(trueRecordLatNs, 0.99); // GATE: < 5us
        st.counters["agg_batch_p50_us"]  = percentileUs(batchLatNs, 0.50);
        st.counters["agg_batch_p99_us"]  = percentileUs(batchLatNs, 0.99);  // GATE: < 10us
        st.counters["agg_steady_alloc"]  = static_cast<double>(steadyAlloc); // GATE: == 0
        st.counters["agg_true_record_samples"] = static_cast<double>(trueRecordLatNs.size());
        st.counters["agg_measured_batches"] = static_cast<double>(batchLatNs.size());
        st.counters["agg_alloc_probe_records"] = static_cast<double>(probeRecFolded);
        st.counters["agg_applied_total"]    = static_cast<double>(appliedTotal);
        st.counters["agg_deleted_total"]    = static_cast<double>(deletedTotal);
        st.counters["agg_skipped_total"]    = static_cast<double>(skippedTotal);
        st.counters["agg_final_size"]       = static_cast<double>(view.size());

        const double recP99 = st.counters["agg_record_p99_us"];
        const double batP99 = st.counters["agg_batch_p99_us"];
        const bool pass = recP99 < 5.0 && batP99 < 10.0 && steadyAlloc == 0;
        st.counters["agg_gate_pass"] = pass ? 1.0 : 0.0;
        std::fprintf(stderr,
                     "[ViewFoldAggregate gate] agg_record_p99_us=%.3f (<5) agg_batch_p99_us=%.3f (<10) "
                     "steady_alloc=%lld (==0) => %s\n",
                     recP99, batP99, static_cast<long long>(steadyAlloc), pass ? "PASS" : "FAIL");
    }
}
BENCHMARK(BM_ViewFoldAggregate)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(1)
    ->UseRealTime();
