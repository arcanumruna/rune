// libs/view/tests/view/ViewTableTest.cpp
#include <gtest/gtest.h>
#include <rune/view/ViewTable.h>
#include <rune/view/ViewRow.h>
#include <rune/codec/Descriptor.h>
#include <unordered_map>
#include <random>
#include <vector>
#include <algorithm>
#include <utility>
using namespace rune::view;
using rune::codec::DescriptorBuilder; using rune::codec::PrimType; using rune::codec::FieldValue;

static RowLayout hpLayout() {
    return buildRowLayout(DescriptorBuilder("v",1)
        .add("id",PrimType::Int64,1,true).add("hp",PrimType::Int32).build());
}
static void setHp(uint8_t* row, const RowLayout& L, int32_t hp) {
    FieldValue v{}; v.type=PrimType::Int32; v.present=true; v.i=hp;
    writeSlot(row, L, 1, v);
}

TEST(ViewTable, InsertFindUpdateDelete) {
    RowLayout L = hpLayout();
    ViewTable t(L);
    setHp(t.put(1001), L, 50);
    ASSERT_NE(t.get(1001), nullptr);
    EXPECT_EQ(readSlot(t.get(1001), L, 1).i, 50);
    setHp(t.put(1001), L, 75);                 // upsert same key -> no dup
    EXPECT_EQ(t.size(), 1u);
    EXPECT_EQ(readSlot(t.get(1001), L, 1).i, 75);
    EXPECT_TRUE(t.erase(1001));
    EXPECT_EQ(t.get(1001), nullptr);
    EXPECT_EQ(t.size(), 0u);
}

TEST(ViewTable, BackshiftKeepsCollidingKeysFindable) {
    RowLayout L = hpLayout();
    ViewTable t(L, /*slots*/16);
    for (uint64_t k = 1; k <= 10; ++k) setHp(t.put(k), L, (int)k);
    EXPECT_TRUE(t.erase(5));
    for (uint64_t k = 1; k <= 10; ++k) {
        if (k == 5) { EXPECT_EQ(t.get(k), nullptr); continue; }
        ASSERT_NE(t.get(k), nullptr) << "key " << k << " lost after backshift";
        EXPECT_EQ(readSlot(t.get(k), L, 1).i, (int)k);
    }
}

TEST(ViewTable, FreeListRecyclesRowIds) {
    RowLayout L = hpLayout();
    ViewTable t(L);
    setHp(t.put(1), L, 1); t.erase(1);
    setHp(t.put(2), L, 2);               // should reuse rid 0
    EXPECT_EQ(t.size(), 1u);
    EXPECT_EQ(readSlot(t.get(2), L, 1).i, 2);
}

TEST(ViewTable, GrowsByAppendingChunksRowsNeverMove) {
    RowLayout L = hpLayout();
    // Index slots sized generously (128, power of two) so this test exercises
    // ONLY row-slab chunk growth (Task 4), not index growth (deferred to
    // Task 5 — index rehash is out of scope here). Tiny chunkBytes (4 rows
    // per chunk) forces many chunk boundaries over 100 inserts.
    ViewTable t(L, /*slots*/128, /*chunkBytes*/ L.rowLen * 4);
    for (uint64_t k = 1; k <= 100; ++k) { uint8_t* r = t.put(k); setHp(r, L, (int)k); }
    EXPECT_GT(t.chunkCount(), 1u);
    for (uint64_t k = 1; k <= 100; ++k) {
        ASSERT_NE(t.get(k), nullptr);
        EXPECT_EQ(readSlot(t.get(k), L, 1).i, (int)k);   // all survive growth
    }
}

TEST(ViewTable, RowPointerStableAcrossChunkGrowth) {
    // Regression: a row pointer captured BEFORE a chunk-growth event must
    // still be valid (and point at the same row) AFTER growth, because rows
    // never move once allocated — only new chunks are appended.
    RowLayout L = hpLayout();
    ViewTable t(L, /*slots*/128, /*chunkBytes*/ L.rowLen * 4); // 4 rows/chunk
    uint8_t* early = t.put(1);
    setHp(early, L, 111);
    EXPECT_EQ(t.chunkCount(), 1u);
    for (uint64_t k = 2; k <= 50; ++k) setHp(t.put(k), L, (int)k);
    EXPECT_GT(t.chunkCount(), 1u) << "expected growth past the first chunk";
    EXPECT_EQ(readSlot(early, L, 1).i, 111) << "row moved after chunk growth";
    EXPECT_EQ(early, const_cast<uint8_t*>(t.get(1))) << "rid->address mapping changed";
}

// ---- Task 5: incremental (de-amortized) index rehash --------------------

TEST(ViewTable, IncrementalRehashKeepsAllKeysFindable) {
    RowLayout L = hpLayout();
    ViewTable t(L, /*slots*/16);           // forces many grows
    for (uint64_t k = 1; k <= 200000; ++k) setHp(t.put(k), L, (int)(k & 0x7fffffff));
    for (uint64_t k = 1; k <= 200000; ++k) {
        ASSERT_NE(t.get(k), nullptr) << "lost key " << k << " across rehash";
    }
    EXPECT_EQ(t.size(), 200000u);
}

// Review-focus #3: a write burst that outruns pre-provision must still be correct.
TEST(ViewTableFallbackRegression, RehashFallbackWhenPreprovisionNotReady) {
    RowLayout L = hpLayout();
    ViewTable t(L, /*slots*/16);
    // insert with no interleaved ops so pre-provision has no spare cycles -> forces fallbacks
    for (uint64_t k = 1; k <= 300000; ++k) setHp(t.put(k), L, 1);
    for (uint64_t k = 1; k <= 300000; ++k) ASSERT_NE(t.get(k), nullptr);
    EXPECT_EQ(t.size(), 300000u);           // no lost/duplicated rows even under fallback
}

// Randomized churn (insert/update/delete) across MANY grow thresholds,
// cross-checked against std::unordered_map as the reference model — same
// style as Task 3's BackshiftKeepsCollidingKeysFindable-class regression,
// extended to exercise the migration window (new-then-old lookups, erase
// during migration, repeated grows) rather than a single fixed-capacity
// table.
TEST(ViewTable, RandomizedChurnAcrossManyGrowsMatchesUnorderedMap) {
    RowLayout L = hpLayout();
    ViewTable t(L, /*slots*/16);
    std::unordered_map<uint64_t, int32_t> model;
    std::mt19937_64 rng(20261009);
    for (int iter = 0; iter < 100000; ++iter) {
        uint64_t key = (rng() % 20000) + 1; // churny: keeps some keys alive across many grows
        if ((rng() % 4) == 0) {
            bool want = model.count(key) != 0;
            bool got = t.erase(key);
            ASSERT_EQ(want, got) << "erase mismatch key=" << key << " iter=" << iter;
            model.erase(key);
        } else {
            int32_t v = (int32_t)(rng() % 1000000);
            setHp(t.put(key), L, v);
            model[key] = v;
        }
        if (iter % 2000 == 0) {
            ASSERT_EQ(t.size(), model.size()) << "size mismatch at iter " << iter;
        }
    }
    ASSERT_EQ(t.size(), model.size());
    for (auto& [k, v] : model) {
        const uint8_t* row = t.get(k);
        ASSERT_NE(row, nullptr) << "missing key " << k;
        EXPECT_EQ(readSlot(row, L, 1).i, v) << "value mismatch key " << k;
    }
}

TEST(ViewTable, WorstRehashTaxIsTracked) {
    RowLayout L = hpLayout();
    ViewTable t(L, /*slots*/16);
    for (uint64_t k = 1; k <= 50000; ++k) setHp(t.put(k), L, 1);
    EXPECT_GE(t.worstRehashTaxUs(), 0.0);
}

// Regression for a self-review fix found while implementing Task 5:
// migrateStep(), as ported from the design spike, bounded its loop on
// entries ACTUALLY MOVED, not entries SCANNED. Read literally, that is a
// latent risk: if the OLD index's unscanned tail were ever mostly dead
// (kEmpty/kTomb), a single call could scan far past kMigK hunting for live
// entries to move, undermining the whole point of incremental
// (de-amortized) rehash (a bounded worst-case cost per op). (Investigation
// during self-review found this specific risk is not reachable through
// put()/erase() as invoked today — every op, including erase(), also
// drives migrateStep() forward by up to kMigK, so the scan cursor cannot
// fall meaningfully behind a burst of tombstoning OLD-only erases. The
// fix — bounding the loop on entries SCANNED, i.e. ocur_ advanced, not on
// entries moved — is kept anyway as strictly more robust/defensive: it is
// a direct, data-independent guarantee instead of an emergent one, costs
// nothing in the common (mostly-live) case, and remains correct if a
// future caller ever changes that invocation pattern.)
//
// This test exercises the OLD-index tombstone path itself (erase() of a
// key that is still only reachable via findO() during an active
// migration): it must mark that OLD slot kTomb (not touch OLD's probe
// structure) and migrateStep() must later skip it rather than migrating
// or losing it — covered here under real grow-triggered migration, with
// heavy interleaved erase churn, checked against correctness only (no
// surviving/erased key lost or resurrected).
TEST(ViewTableMigrateScanBoundRegression, EraseDuringMigrationTombstonesOldCorrectly) {
    RowLayout L = hpLayout();
    ViewTable t(L, /*slots*/1u << 16); // 65536-slot OLD index once it grows
    uint64_t k = 1;
    for (; k <= 55706; ++k) setHp(t.put(k), L, 1); // the 55706th put triggers startGrow()

    // Heavy erase churn immediately after the grow, before migration has
    // drained much of anything: tombstone every third just-grown-from key
    // (scattered essentially uniformly across the OLD index by mix()).
    for (uint64_t e = 1; e <= 55706; e += 3) t.erase(e);

    // Drive the rest of the table's ops to completion (also drains migration).
    for (; k <= 200000; ++k) setHp(t.put(k), L, 1);

    for (uint64_t e = 1; e <= 55706; ++e) {
        if (e % 3 == 1) EXPECT_EQ(t.get(e), nullptr) << "erased key " << e << " resurrected";
        else ASSERT_NE(t.get(e), nullptr) << "lost key " << e << " (not erased)";
    }
    for (uint64_t v = 55707; v < 200000; ++v) ASSERT_NE(t.get(v), nullptr) << "lost key " << v;
}

// ---- Task 6: row-sized dirty tracking for differential checkpoint -------

TEST(ViewTable, DirtyTrackingCoalescesAndClears) {
    RowLayout L = hpLayout();
    ViewTable t(L);
    setHp(t.put(1), L, 1);
    setHp(t.put(1), L, 2);   // same row twice in interval
    setHp(t.put(2), L, 9);
    std::vector<uint64_t> seenKeys;
    t.drainDirty([&](uint32_t, const uint8_t*, bool del, uint64_t key){ if(!del) seenKeys.push_back(key); });
    std::sort(seenKeys.begin(), seenKeys.end());
    EXPECT_EQ(seenKeys.size(), 2u);          // key 1 coalesced to one entry
    EXPECT_EQ(seenKeys[0], 1u); EXPECT_EQ(seenKeys[1], 2u);
    std::vector<uint32_t> again;
    t.drainDirty([&](uint32_t rid, const uint8_t*, bool, uint64_t){ again.push_back(rid); });
    EXPECT_TRUE(again.empty());              // cleared after drain
}

TEST(ViewTable, EraseSurfacesDeletedEntryWithKey) {
    RowLayout L = hpLayout();
    ViewTable t(L);
    setHp(t.put(42), L, 7);
    t.drainDirty([](uint32_t, const uint8_t*, bool, uint64_t){}); // clear the put's dirty entry first
    EXPECT_TRUE(t.erase(42));

    std::vector<std::pair<uint64_t,bool>> seen;
    t.drainDirty([&](uint32_t, const uint8_t* row, bool del, uint64_t key){
        seen.emplace_back(key, del);
        if (del) EXPECT_EQ(row, nullptr);
    });
    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0].first, 42u);
    EXPECT_TRUE(seen[0].second);

    std::vector<uint32_t> again;
    t.drainDirty([&](uint32_t rid, const uint8_t*, bool, uint64_t){ again.push_back(rid); });
    EXPECT_TRUE(again.empty());
}

// Regression (found by code review): drainDirty() must emit deletes BEFORE
// upserts. A delete-then-reinsert of the SAME key within one drain interval
// (put(A); erase(A); put(A) again with a different row value) frees A's rid,
// clears its dirty bit and records a {rid,key} delete entry, then re-marks
// the SAME rid dirty on reinsert. If drainDirty emitted the upsert first and
// the delete second (the original, buggy order), a Task-12-style replay that
// applies entries in emit order would do put(A)+copy-new-row then erase(A),
// leaving A permanently ABSENT -- silent data loss of a key that is actually
// still live. Delete-first is safe in every case: a delete for a key NOT
// reinserted this interval is a correct no-op replay against existing
// checkpoint state, and a delete immediately followed by its own reinsert's
// upsert correctly leaves the key present with the latest row.
//
// This test does not just check emit order -- it performs an actual
// mini-replay onto a second, independent ViewTable (as Task 12's checkpoint
// consumer will) and asserts the REPLAYED table's final state is correct.
TEST(ViewTableDirtyTrackingRegression, DeleteThenReinsertSameKeyWithinIntervalReplaysCorrectly) {
    RowLayout L = hpLayout();
    ViewTable t(L);
    constexpr uint64_t A = 777;
    setHp(t.put(A), L, 111);   // put(A) -> rid R, dirty
    ASSERT_TRUE(t.erase(A));   // erase(A) -> frees R, dirty cleared, deleted_={R,A}
    setHp(t.put(A), L, 222);   // put(A) again, same key -> reuses rid R, dirty again

    ViewTable replay(L); // independent second table; drainDirty's entries are replayed onto it
    t.drainDirty([&](uint32_t, const uint8_t* row, bool deleted, uint64_t key){
        if (deleted) {
            replay.erase(key);
        } else {
            uint8_t* r = replay.put(key);
            std::copy(row, row + L.rowLen, r);
        }
    });

    const uint8_t* got = replay.get(A);
    ASSERT_NE(got, nullptr) << "key " << A << " must be PRESENT after replay (delete-then-reinsert "
                                "in one interval must not be lost)";
    EXPECT_EQ(readSlot(got, L, 1).i, 222) << "replayed row must carry the LATEST (second put's) value";
}

// CRITICAL regression (found by external code review, NOT caught by this
// agent's own self-review): migrateStep()'s live-entry copy branch must
// tombstone the OLD slot the instant it copies that entry into NEW.
// Without that, findO() has no notion of "already migrated" — it only
// stops its linear probe at a true kEmpty — so the OLD "ghost" of an
// already-migrated key stays matchable for the rest of the migration
// window. Failure sequence: (1) key K is migrated OLD->NEW by
// migrateStep(); (2) erase(K) succeeds via findP (NEW), backshifts K out
// of NEW, and returns K's rid to the free-list — but never touches OLD;
// (3) put(K), still mid-migration: findP(K) fails (removed from NEW),
// findO(K) matches the stale OLD ghost, and put() returns
// rowAddr(orid_[oi]) treating it as an idempotent update of an "existing"
// key — no write to tags_/keys_/rid_, no ++sz_, and K's rid is now
// dangling in BOTH the free-list and the stale ghost (a future put() for
// some unrelated new key can pop that same rid from the free-list and
// silently alias/overwrite K's row). Once migration later fully drains
// (ot_/ok_/orid_ freed), K is permanently un-gettable and size() stays
// off by one per key that hit this path.
//
// This test deterministically reproduces that exact precondition — rather
// than hoping a loop statistically triggers it — by duplicating
// ViewTable's internal mix() (a fixed, documented SplitMix64-style
// finalizer) to PREDICT each key's OLD-index slot and select a batch of
// keys guaranteed to already be migrated to NEW (slot < the migration
// cursor) before running erase()+put() on them mid-migration.
TEST(ViewTableMigrationRegression, EraseThenReinsertAlreadyMigratedKeyDuringMigrationIsNotLost) {
    auto mixLikeViewTable = [](uint64_t x) -> uint64_t {
        x ^= x >> 30; x *= 0xbf58476d1ce4e5b9ULL;
        x ^= x >> 27; x *= 0x94d049bb133111ebULL;
        x ^= x >> 31;
        return x;
    };
    constexpr size_t kMigKMirror = 256; // ViewTable::kMigK is private; mirrored here for prediction only

    RowLayout L = hpLayout();
    ViewTable t(L, /*slots*/1u << 16); // 65536: this becomes OLD once it grows
    const size_t omask = (1u << 16) - 1;

    uint64_t k = 1;
    for (; k <= 55706; ++k) setHp(t.put(k), L, 1); // the 55706th put triggers startGrow()

    // Advance migration partway (50 more ops => the drain cursor has swept
    // past kMigKMirror*50 == 12800 of the 65536 OLD slots) without letting
    // it fully drain (65536/256 == 256 ops would be needed for that).
    constexpr int kAdvanceOps = 50;
    for (int i = 0; i < kAdvanceOps; ++i) { setHp(t.put(k), L, 1); ++k; }
    const size_t scannedSoFar = kAdvanceOps * kMigKMirror;

    // Any key whose predicted OLD slot already lies behind the drain
    // cursor is guaranteed already migrated: migrateStep() sweeps the
    // cursor strictly forward over the OLD slot array and never revisits
    // a slot once passed.
    std::vector<uint64_t> migratedKeys;
    for (uint64_t cand = 1; cand <= 55706 && migratedKeys.size() < 200; ++cand) {
        if ((mixLikeViewTable(cand) & omask) < scannedSoFar) migratedKeys.push_back(cand);
    }
    ASSERT_GE(migratedKeys.size(), 50u) << "test setup: not enough predicted-migrated keys found";

    const size_t sizeBefore = t.size();
    for (uint64_t mk : migratedKeys) {
        ASSERT_TRUE(t.erase(mk)) << "erase of already-migrated key " << mk << " should succeed";
        setHp(t.put(mk), L, 777); // reinsert while (for the earlier keys in this batch) still mid-migration
    }

    // The direct, immediate symptom: each lost erase+put pair silently
    // skips ++sz_, so size() drifts low by however many keys hit the bug.
    EXPECT_EQ(t.size(), sizeBefore)
        << "erase()+put() pairs on already-migrated keys must be size-neutral";

    // Drive the rest of the table's ops to completion (also fully drains
    // migration, freeing ot_/ok_/orid_) with a disjoint key range, so a
    // reused/aliased rid would show up as wrong content on an unrelated key.
    for (; k <= 120000; ++k) setHp(t.put(k), L, 2);

    for (uint64_t mk : migratedKeys) {
        ASSERT_NE(t.get(mk), nullptr) << "key " << mk << " permanently LOST after migration fully drained";
        EXPECT_EQ(readSlot(t.get(mk), L, 1).i, 777) << "key " << mk << " corrupted (possible rid aliasing)";
    }
    for (uint64_t v = 55757; v < 120000; ++v) {
        ASSERT_NE(t.get(v), nullptr) << "lost key " << v;
        EXPECT_EQ(readSlot(t.get(v), L, 1).i, 2) << "key " << v << " corrupted (possible rid aliasing)";
    }
}
