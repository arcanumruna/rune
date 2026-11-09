// Lease topics Task 12: promotion arming (spec 9.6, 9.7; M1, M2, M9) on a real worker.  A promotion is a grant as leader, then a demotion and a
// promotion: the lease runtime is created afresh over a table that already holds rows, and its promotion pass arms them.
//
// T_ready is the runtime's creation time (the leaseNow of the duty pass that first sees the lease index ready).  The lease clock is driven by
// hand; every drive loop is capped.
#include "LeaseRig.h"

#include <random>
#include <set>

using namespace ltr;

namespace {

bool setup(Lk& k, std::initializer_list<uint64_t> keys, uint64_t ttl = 5000) {
    std::vector<RawRec> recs;
    for (uint64_t id : keys) recs.push_back(grantRec(id, ttl));
    if (!k.produce(recs)) return false;
    return k.r.driveUntilJudged();
}
int64_t msToNs(uint64_t ms) { return int64_t(ms) * kMs; }

}  // namespace

// 1. (M1) The leader heartbeat timeout is added to a promoted key's remaining time: deadline = T_ready + extend + (remaining, else ttl).
// Mutation: drop `extendNs_` from LeaseRuntime::promotionDeadline: both deadlines are red.
TEST(LeasePromotionArmingRegression, ExtendAddedRegression) {
    Lk k(specWith(1000, 0, 1000, 1000));
    ASSERT_TRUE(setup(k, {1}, 10000));
    ASSERT_TRUE(setup(k, {2}, 5000));
    ASSERT_TRUE(k.waitRuntime());
    const int64_t v2 = k.versionOf(2);
    k.appendCheckpoint(2, v2, 4000, 5000);
    for (int i = 0; i < kCap && k.rowRemaining(2) != 4000; ++i) k.r.pass();
    ASSERT_EQ(k.rowRemaining(2), 4000u);
    k.r.worker->setLeaseExtendNs(2000 * kMs);
    k.advanceMs(700);
    k.r.demote();
    k.r.promote();
    ASSERT_TRUE(k.waitRuntime());
    const int64_t ready = k.rt()->readyNs();
    EXPECT_EQ(ready, k.leaseNow) << "T_ready is the runtime's creation time";
    ASSERT_EQ(k.rt()->stats().promotionLinked, 2u);
    EXPECT_EQ(k.entry(1).state, LeaseEntryState::Armed);
    EXPECT_EQ(k.entry(1).deadlineNs, ready + 2000 * kMs + 10000 * kMs) << "remaining 0: the ttl";
    EXPECT_EQ(k.entry(2).state, LeaseEntryState::Armed);
    EXPECT_EQ(k.entry(2).deadlineNs, ready + 2000 * kMs + 4000 * kMs) << "a folded Checkpoint(4000): its remaining";
    EXPECT_TRUE(k.rt()->promotionDone());
}

// 2. (M9) A seeded log of grants, checkpoints (left to fire), keepalive clears, revokes and late grants; after the promotion every live key's
// deadline is exactly T_ready + extend + (remaining > 0 ? remaining : ttl) read from its row; revoked keys have no entry.
// Mutations: `remaining > 0 ? remaining : ttl` -> `ttl` (ignores the checkpoint): red on the checkpointed keys; -> `remaining` (a cleared key
// has remaining 0): red on the cleared and the fresh keys.
TEST(LeasePromotionArmingRegression, SeededLogRegression) {
    Lk k(specWith(1000, 1, 1000, 100'000));
    std::mt19937 rng(7);
    constexpr uint64_t kN = 200, kLate = 20;
    std::vector<uint64_t> ttl(kN + kLate + 1, 0);
    for (uint64_t id = 1; id <= kN; ++id) ttl[id] = 3000 + rng() % 17000;
    for (uint64_t id = kN + 1; id <= kN + kLate; ++id) ttl[id] = 3000 + rng() % 17000;
    for (int half = 0; half < 2; ++half) {
        std::vector<RawRec> recs;
        for (uint64_t id = 1 + 100 * half; id <= 100 * (half + 1); ++id) recs.push_back(grantRec(id, ttl[id]));
        ASSERT_TRUE(k.produce(recs));
    }
    ASSERT_TRUE(k.r.driveUntilJudged());
    ASSERT_TRUE(k.waitRuntime());
    // let every checkpoint fire (all ttl > 3 s > interval): one round of Checkpoint(R)
    k.advanceMs(1000);
    for (int i = 0; i < kCap && countOp(k.r, rec::LeaseOp::Checkpoint) < kN; ++i) k.r.pass();
    ASSERT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), kN);
    k.r.passes(10);
    // late grants: fresh keys with remaining 0
    ASSERT_TRUE(k.produce([&] { std::vector<RawRec> v; for (uint64_t id = kN + 1; id <= kN + kLate; ++id) v.push_back(grantRec(id, ttl[id])); return v; }()));
    ASSERT_TRUE(k.r.driveUntilJudged());
    // a seeded shuffle picks the 40 revoked and the 40 cleared keys
    std::vector<uint64_t> ids(kN);
    for (uint64_t i = 0; i < kN; ++i) ids[i] = i + 1;
    std::shuffle(ids.begin(), ids.end(), rng);
    const std::vector<uint64_t> revoked(ids.begin(), ids.begin() + 40), cleared(ids.begin() + 40, ids.begin() + 80);
    std::vector<std::pair<std::string, int64_t>> ka;
    for (uint64_t id : cleared) ka.push_back(E(id, k.versionOf(id)));
    k.enqueue(ka);
    for (int i = 0; i < kCap; ++i) {
        bool all = true;
        for (uint64_t id : cleared) all = all && k.rowRemaining(id) == 0;
        if (all) break;
        k.step();
    }
    for (uint64_t id : cleared) ASSERT_EQ(k.rowRemaining(id), 0u) << "cleared key " << id;
    std::vector<RawRec> rv;
    for (uint64_t id : revoked) rv.push_back(revokeRec(id, k.versionOf(id)));
    ASSERT_TRUE(k.produce(rv));
    ASSERT_TRUE(k.r.driveUntilJudged());
    k.r.passes(5);
    for (uint64_t id : revoked) ASSERT_EQ(k.ridOf(id), UINT32_MAX) << "revoked key " << id;
    k.r.worker->setLeaseExtendNs(1500 * kMs);
    k.advanceMs(700);
    k.r.demote();
    k.r.promote();
    ASSERT_TRUE(k.waitRuntime());
    const int64_t ready = k.rt()->readyNs();
    EXPECT_EQ(ready, k.leaseNow);
    std::size_t checkpointed = 0, zero = 0;
    for (uint64_t id = 1; id <= kN + kLate; ++id) {
        if (std::find(revoked.begin(), revoked.end(), id) != revoked.end()) {
            EXPECT_EQ(k.ridOf(id), UINT32_MAX) << "key " << id;
            continue;
        }
        const uint64_t rem = k.rowRemaining(id), t = k.rowTtl(id);
        ASSERT_NE(rem, UINT64_MAX) << "key " << id;
        ASSERT_EQ(t, ttl[id]);
        const uint32_t rid = k.ridOf(id);
        ASSERT_NE(rid, UINT32_MAX);
        const auto& e = k.rt()->timers().entry(rid);
        ASSERT_EQ(e.state, LeaseEntryState::Armed) << "key " << id;
        EXPECT_EQ(e.deadlineNs, ready + 1500 * kMs + msToNs(rem > 0 ? rem : t)) << "key " << id << " remaining " << rem << " ttl " << t;
        (rem > 0 ? checkpointed : zero)++;
    }
    EXPECT_GE(checkpointed, 100u) << "the mix has checkpointed keys";
    EXPECT_GE(zero, 40u) << "and cleared / fresh keys";
    EXPECT_EQ(k.rt()->stats().promotionLinked, kN + kLate - 40);
}

// 3. (M2 unit twin) The promotion arms at the moment the index is READY, not at the earlier moment the table was refolded.  The judge is held
// across the role change so the index is not ready from t0 to t1 = t0 + 5 s; the runtime is created at t1 and every deadline uses t1.
// Mutation: construct the runtime with the leaseNow of the first duty pass after the role change (a temporary member of Worker): red.
TEST(LeasePromotionArmingRegression, ArmsAtReadyNotRefoldRegression) {
    Lk k(specWith(1000, 0, 1000, 1000));
    ASSERT_TRUE(setup(k, {1, 2, 3}, 5000));
    ASSERT_TRUE(k.waitRuntime());
    k.r.holdJudge(true);
    (void)k.send({updateRec(99, 0)});   // no row for key 99: the hook cannot track it, so the index is not ready until the judge passes it
    k.r.passes(30);
    ASSERT_FALSE(k.r.leaseReady());
    const int64_t t0 = k.leaseNow;
    k.r.demote();
    k.r.promote();
    k.r.passes(20);
    ASSERT_FALSE(k.r.leaseReady());
    ASSERT_EQ(k.rt(), nullptr) << "no runtime while the index is not ready";
    k.advanceMs(5000);
    const int64_t t1 = k.leaseNow;
    ASSERT_EQ(t1, t0 + 5000 * kMs);
    k.r.passes(5);
    ASSERT_EQ(k.rt(), nullptr);
    k.r.holdJudge(false);
    ASSERT_TRUE(k.r.driveUntilJudged());
    ASSERT_TRUE(k.waitRuntime());
    EXPECT_EQ(k.rt()->readyNs(), t1) << "created at the moment the index became ready";
    for (uint64_t id = 1; id <= 3; ++id) {
        ASSERT_EQ(k.entry(id).state, LeaseEntryState::Armed) << "key " << id;
        EXPECT_EQ(k.entry(id).deadlineNs, t1 + 5000 * kMs) << "key " << id;
    }
}

// 4. The pass links at most kLeasePromotionChunk row ids per duty cycle.  3 x 4096 + 7 rows: pass 1 links exactly 4096, pass 4 has linked
// them all.  A keepalive for a not-yet-linked key during the walk is served Ok and links it (the pass then skips it).
// Mutation: chunk = UINT32_MAX (LeaseRuntime::promoteChunk_): pass 1 links every row: red.
TEST(LeasePromotionArmingRegression, ChunkedLinkingRegression) {
    constexpr uint64_t kRows = 3 * 4096 + 7;
    Lk k(specWith(13000, 0, 1000, 1000));
    ASSERT_TRUE(k.grantMany(kRows, 5000));
    const int passes = k.promoteStepwise();
    ASSERT_GT(passes, 0);
    EXPECT_EQ(k.rt()->stats().promotionLinked, 4096u) << "after the creation pass";
    EXPECT_FALSE(k.rt()->promotionDone());
    // a key whose row id is in the last chunk
    uint64_t late = 0;
    for (uint64_t id = kRows; id > kRows - 20; --id) if (k.ridOf(id) >= 3 * 4096 + 1) { late = id; break; }
    ASSERT_NE(late, 0u);
    ASSERT_FALSE(k.rt()->timers().covers(k.ridOf(late)) && k.entry(late).state != LeaseEntryState::Free) << "not linked yet";
    const int64_t v = k.versionOf(late);
    k.advanceMs(100);
    const auto fr = k.keepAlive({E(late, v)});   // served at Step 0 of pass 2, before that pass's promotion chunk
    ASSERT_EQ(fr.size(), 1u);
    EXPECT_EQ(fr[0].es[0].code, 0);
    EXPECT_EQ(k.rt()->stats().promotionLinked, 8192u) << "after pass 2";
    EXPECT_EQ(k.entry(late).state, LeaseEntryState::Armed);
    EXPECT_EQ(k.entry(late).deadlineNs, k.leaseNow + 5000 * kMs) << "armed by the keepalive; the pass did not relink it";
    k.r.pass();
    EXPECT_EQ(k.rt()->stats().promotionLinked, 3u * 4096u) << "after pass 3";
    EXPECT_FALSE(k.rt()->promotionDone());
    k.r.pass();
    EXPECT_EQ(k.rt()->stats().promotionLinked, kRows - 1) << "after pass 4: every row but the one the keepalive linked";
    EXPECT_TRUE(k.rt()->promotionDone());
    for (uint64_t id = 1; id <= kRows; ++id) {
        if (id % 997 != 0 && id != kRows && id != late) continue;   // a sample, plus the last row
        EXPECT_EQ(k.entry(id).state, LeaseEntryState::Armed) << "key " << id;
    }
}

// 5. A key the pass has not reached never expires early: it has no entry, so nothing can fire for it before the pass that links it.  Chunk 1,
// four keys (ttl 3 s): the creation pass links one; the clock then jumps to ready + 10 s (every linked deadline, ready + 3 s, is long due).
// Each following pass links ONE more key and expires exactly the keys linked so far - never one the walk has not reached - and each key
// expires exactly once.  Mutation: ignore the chunk (`uint32_t budget = promoteChunk_` -> UINT32_MAX in LeaseRuntime::promote): the creation
// pass links all four, the first pass after the jump expires all four: red.
TEST(LeasePromotionArmingRegression, NotLinkedNeverExpiresEarlyRegression) {
    Lk k(specWith(1000, 0, 1000, 1000));
    ASSERT_TRUE(setup(k, {1, 2, 3, 4}, 3000));
    k.r.worker->setLeasePromotionChunkForTest(1);
    ASSERT_GT(k.promoteStepwise(), 0);
    const int64_t ready = k.rt()->readyNs();
    ASSERT_EQ(k.rt()->stats().promotionLinked, 1u) << "the creation pass links one row";
    uint32_t ridOfKey[5] = {};
    for (uint64_t id = 1; id <= 4; ++id) ridOfKey[id] = k.ridOf(id);
    k.leaseNow = ready + 10'000 * kMs;
    for (uint64_t pass = 1; pass <= 3; ++pass) {
        k.r.pass();
        EXPECT_EQ(k.rt()->stats().promotionLinked, pass + 1) << "pass " << pass;
        std::set<uint32_t> expired;
        for (const auto& s : withOp(k.r, rec::LeaseOp::Expire)) {
            for (uint64_t id = 1; id <= 4; ++id) if (s.key == keyOf(id)) expired.insert(ridOfKey[id]);
        }
        EXPECT_EQ(countOp(k.r, rec::LeaseOp::Expire), pass + 1) << "pass " << pass << ": one record per linked key, none for an unreached one";
        // the rows walked so far are rids [0, pass]: every expired key must be one of them (rids are dense from 0 in grant order)
        uint32_t minRid = UINT32_MAX;
        for (uint64_t id = 1; id <= 4; ++id) minRid = std::min(minRid, ridOfKey[id]);
        for (const uint32_t rid : expired) EXPECT_LE(rid - minRid, pass) << "pass " << pass << ": an Expire for a key the walk had not reached";
    }
    k.r.passes(10);
    const auto ex = withOp(k.r, rec::LeaseOp::Expire);
    ASSERT_EQ(ex.size(), 4u) << "all four expire";
    std::set<std::string> keys;
    for (const auto& s : ex) keys.insert(s.key);
    EXPECT_EQ(keys.size(), 4u) << "each exactly once";
}

// 6. Erased rows are not linked: the pass walks row ids and skips the ones an Expire / Revoke erased.  A row id reused by a new grant is armed
// by its Granted fold, once, whether the pass has passed it or not.
// Mutation: drop the kLeaseRowErased write in LeaseIndex.cpp: the pass links the 10 erased rows (promotionLinked 30): red.
TEST(LeasePromotionArmingRegression, ErasedRowsNotLinkedRegression) {
    for (const int leg : {0, 1}) {
        Lk k(specWith(1000, 0, 1000, 1000));
        std::vector<RawRec> recs;
        for (uint64_t id = 1; id <= 30; ++id) recs.push_back(grantRec(id, 5000));
        ASSERT_TRUE(k.produce(recs));
        ASSERT_TRUE(k.r.driveUntilJudged());
        std::vector<uint32_t> erasedRids;
        std::vector<RawRec> rv;
        for (uint64_t id = 1; id <= 10; ++id) {
            erasedRids.push_back(k.ridOf(id));
            rv.push_back(revokeRec(id, k.versionOf(id)));
        }
        ASSERT_TRUE(k.produce(rv));
        ASSERT_TRUE(k.r.driveUntilJudged());
        k.r.passes(3);
        for (uint64_t id = 1; id <= 10; ++id) ASSERT_EQ(k.ridOf(id), UINT32_MAX) << "key " << id;
        if (leg == 1) k.r.worker->setLeasePromotionChunkForTest(0);   // the pass has not started when the rid is reused
        k.r.demote();
        k.r.promote();
        ASSERT_TRUE(k.waitRuntime());
        for (uint32_t rid : erasedRids) {
            EXPECT_EQ(k.rt()->timers().entry(rid).state, LeaseEntryState::Free) << "leg " << leg << " rid " << rid;
            EXPECT_FALSE(k.rt()->timers().deadline().linked(rid)) << "leg " << leg << " rid " << rid;
        }
        if (leg == 0) EXPECT_EQ(k.rt()->stats().promotionLinked, 20u) << "only the live rows";
        else           EXPECT_EQ(k.rt()->stats().promotionLinked, 0u);
        k.advanceMs(250);
        ASSERT_TRUE(k.produce({grantRec(100, 5000)}));
        ASSERT_TRUE(k.r.driveUntilJudged());
        k.r.passes(3);
        const uint32_t rid = k.ridOf(100);
        EXPECT_NE(std::find(erasedRids.begin(), erasedRids.end(), rid), erasedRids.end()) << "leg " << leg << ": the new grant reused an erased rid";
        EXPECT_EQ(k.rt()->timers().entry(rid).state, LeaseEntryState::Armed);
        EXPECT_EQ(k.rt()->timers().entry(rid).deadlineNs, k.leaseNow + 5000 * kMs) << "leg " << leg << ": armed by its Granted fold";
        if (leg == 1) {
            k.rt()->setPromotionChunkForTest(lease::kLeasePromotionChunk);
            k.r.passes(3);
            EXPECT_TRUE(k.rt()->promotionDone());
            EXPECT_EQ(k.rt()->stats().promotionLinked, 20u) << "the pass links the 20 old live rows and skips the reused one";
            EXPECT_EQ(k.rt()->timers().entry(rid).deadlineNs, k.leaseNow + 5000 * kMs) << "and does not relink it";
        }
        EXPECT_EQ(k.rt()->stats().grantNotFree, 0u);
    }
}

// 7. A fold of a present row whose entry is still Free (the pass has not reached it, or the fold created the row) links it first, at the
// promotion deadline the pass would have used.  Injected with onEventsForTest while the pass is held back.
// Mutation: drop the `present && e.state == Free` link in LeaseRuntime::onEvents: both entries stay Free: red.
TEST(LeasePromotionArmingRegression, FoldOfFreeRowLinksRegression) {
    Lk k(specWith(1000, 0, 1000, 1000));
    ASSERT_TRUE(setup(k, {1, 2}, 5000));
    k.r.worker->setLeaseExtendNs(1000 * kMs);
    k.r.worker->setLeasePromotionChunkForTest(0);
    k.r.demote();
    k.r.promote();
    ASSERT_TRUE(k.waitRuntime());
    const int64_t ready = k.rt()->readyNs();
    ASSERT_EQ(k.entry(1).state, LeaseEntryState::Free);
    ASSERT_EQ(k.entry(2).state, LeaseEntryState::Free);
    k.advanceMs(300);
    LeaseEvent up;
    up.kind = LeaseEventKind::Updated; up.rid = k.ridOf(1); up.offset = 500; up.ttlMs = 5000; up.remainingMs = 0;
    k.r.worker->onEventsForTest(VerdictRig::kSrc, 0, std::span<const LeaseEvent>(&up, 1), k.leaseNow);
    EXPECT_EQ(k.entry(1).state, LeaseEntryState::Armed);
    EXPECT_EQ(k.entry(1).deadlineNs, ready + 1000 * kMs + 5000 * kMs) << "an Updated fold of a Free entry: remaining 0, the ttl";
    LeaseEvent ck;
    ck.kind = LeaseEventKind::Checkpointed; ck.rid = k.ridOf(2); ck.offset = 501; ck.ttlMs = 5000; ck.remainingMs = 4000;
    k.r.worker->onEventsForTest(VerdictRig::kSrc, 0, std::span<const LeaseEvent>(&ck, 1), k.leaseNow);
    EXPECT_EQ(k.entry(2).state, LeaseEntryState::Armed);
    EXPECT_EQ(k.entry(2).deadlineNs, ready + 1000 * kMs + 4000 * kMs) << "a Checkpointed fold of a Free entry: its remaining";
}

// 8. The idle skip waits for the promotion pass: with no events and no wheel due, every duty cycle still advances the walk until it is done
// (hasPendingWork is true while promotionDone() is false); once done, the idle passes cost no lookup again.
// Mutation: drop the `|| !promotionDone()` term from LeaseRuntime::hasPendingWork: the passes are skipped, the walk stalls at 1 row: red.
TEST(LeasePromotionArmingRegression, IdleSkipWaitsForPromotionRegression) {
    Lk k(specWith(1000, 0, 1000, 1000));
    ASSERT_TRUE(setup(k, {1, 2, 3, 4, 5}, 5000));
    k.r.worker->setLeasePromotionChunkForTest(1);
    ASSERT_GT(k.promoteStepwise(), 0);
    EXPECT_EQ(k.rt()->stats().promotionLinked, 1u);
    for (uint64_t want = 2; want <= 5; ++want) {
        k.r.pass();
        EXPECT_EQ(k.rt()->stats().promotionLinked, want) << "one row per cycle, nothing else to do";
    }
    EXPECT_TRUE(k.rt()->promotionDone());
    k.r.passes(3);
    const uint64_t lookups = k.r.worker->leaseDutyLookupsForTest();
    k.r.passes(50);
    EXPECT_EQ(k.r.worker->leaseDutyLookupsForTest(), lookups) << "done: idle again";
}

// 9. (F1) An acked lease never expires early after a failed cold growth.  A fold of a LIVE row (a Granted event, or an Updated fold of a row
// past the store) whose entry cannot be grown (bad_alloc) leaves a Free entry on a live row; linking it later would use the PROMOTION deadline
// (ready + extend + ttl, the OLD runtime's ready time), which can be before g + ttl.  The runtime is therefore rebuilt: it is dropped right
// after the fold and the next pass recreates it at a fresh ready time >= g, so every promotion deadline is >= g + extend + ttl.
// Setup: extend 2 s, ttl 10 s, 4100 rows, a runtime created at T_ready whose store holds the first chunk only (promotion chunk 0); 30 s later
// the growth seam is on and key K (leg 0: a new grant past the headroom; leg 1: an update of the present row 4100) folds.  The runtime is
// rebuilt on the following pass (readyNs == that pass's leaseNow); with the seam off a keepalive at g + 1 s is Ok; no Expire for K at any step
// up to g + 10 s - 1 ms; exactly one by g + 11.5 s (the keepalive moved the deadline to g + 11 s).
// Mutation: do not set `rebuildOwed_` (both growth-failure paths in LeaseRuntime::onEvents): the old runtime stays, the keepalive's Free-arm links K at
// the OLD promotion deadline, long past: an Expire at ~g + 1 s: red (both legs).
TEST(LeasePromotionArmingRegression, GrowFailedGrantNeverEarlyRegression) {
    constexpr uint64_t kRows = 4100;
    constexpr uint64_t kTtl  = 10'000;
    for (const int leg : {0, 1}) {
        SCOPED_TRACE(leg == 0 ? "leg 0: a grant past the store" : "leg 1: an Updated fold of a present row past the store");
        Lk k(specWith(13000, 0, 1000, 1000));
        k.r.worker->setLeaseExtendNs(2000 * kMs);
        ASSERT_TRUE(k.grantMany(kRows, kTtl));
        k.r.worker->setLeasePromotionChunkForTest(0);   // the first runtime does not walk: its store keeps the first chunk
        ASSERT_GT(k.promoteStepwise(), 0);
        ASSERT_EQ(k.rt()->timers().capacity(), lease::kLeaseEntryChunk);
        const int64_t ready0 = k.rt()->readyNs();
        k.r.worker->setLeasePromotionChunkForTest(lease::kLeasePromotionChunk);   // the rebuilt runtime walks
        k.advanceMs(30'000);
        const int64_t g = k.leaseNow;
        k.rt()->setGrowThrowForTest(true);
        const uint64_t key = leg == 0 ? kRows + 1 : kRows;
        if (leg == 0) ASSERT_TRUE(k.produce({grantRec(key, kTtl)}));
        else          ASSERT_TRUE(k.produce({updateRec(key, k.versionOf(key))}));
        ASSERT_TRUE(k.r.driveUntilJudged());
        k.r.passes(3);
        ASSERT_NE(k.rt(), nullptr);
        EXPECT_GT(k.rt()->readyNs(), ready0) << "the runtime was rebuilt";
        EXPECT_EQ(k.rt()->readyNs(), g) << "at the pass's leaseNow";
        EXPECT_EQ(k.rt()->stats().growFailures, 0u) << "the new runtime";
        k.rt()->setGrowThrowForTest(false);
        k.r.passes(3);
        EXPECT_TRUE(k.rt()->promotionDone());   // (EXPECT: a mutant that keeps the chunk-0 runtime must still reach the Expire assertions below)
        k.advanceMs(1000);
        const int64_t ver = k.versionOf(key);
        const auto fr = k.keepAlive({E(key, ver)});
        EXPECT_EQ(fr.size(), 1u) << "answered at once (a due key would park)";   // (EXPECT: a mutant must still reach the Expire assertions)
        if (!fr.empty()) EXPECT_EQ(fr[0].es[0].code, 0);
        auto expiresOf = [&] {
            std::size_t n = 0;
            for (const auto& s : withOp(k.r, rec::LeaseOp::Expire)) n += s.key == keyOf(key) ? 1 : 0;
            return n;
        };
        EXPECT_EQ(expiresOf(), 0u);
        const int64_t last = g + msToNs(kTtl) - 1 * kMs;
        while (k.leaseNow < last) {
            k.leaseNow = std::min(k.leaseNow + 500 * kMs, last);
            k.r.passes(2);
            ASSERT_EQ(expiresOf(), 0u) << "an acked lease expired at g + " << (k.leaseNow - g) / kMs << " ms";
        }
        k.leaseNow = g + 11'500 * kMs;
        k.r.passes(5);
        EXPECT_EQ(expiresOf(), 1u);
        k.r.passes(30);
        EXPECT_EQ(expiresOf(), 1u) << "exactly once";
    }
}

// 10. (F10) The promotion pass reports the rids it WALKED as work, so a chunk made of erased rids is not idle: chunk 1, rows 0..2 erased and
// row 3 live: every pass that walks returns work >= 1 until the walk is done.
// Mutation: return the entries LINKED instead of the rids walked (LeaseRuntime::promote): the passes over the erased rows return 0: red.
TEST(LeasePromotionArmingRegression, ErasedChunkIsWorkRegression) {
    Lk k(specWith(1000, 0, 1000, 1000));
    ASSERT_TRUE(setup(k, {1, 2, 3, 4}, 5000));
    std::vector<RawRec> rv;
    for (uint64_t id = 1; id <= 3; ++id) rv.push_back(revokeRec(id, k.versionOf(id)));
    ASSERT_TRUE(k.produce(rv));
    ASSERT_TRUE(k.r.driveUntilJudged());
    k.r.passes(5);
    for (uint64_t id = 1; id <= 3; ++id) ASSERT_EQ(k.ridOf(id), UINT32_MAX);
    ASSERT_EQ(k.ridOf(4), 3u);
    k.r.worker->setLeasePromotionChunkForTest(1);
    k.r.demote();
    k.r.promoteWithoutPass();
    int walkingPasses = 0;
    for (int i = 0; i < 400; ++i) {
        const bool walking = k.rt() != nullptr && !k.rt()->promotionDone();
        const int work = k.r.pass();
        if (walking) {
            ++walkingPasses;
            EXPECT_GE(work, 1) << "a walking pass over " << (walkingPasses) << " is work";
        }
        if (k.rt() != nullptr && k.rt()->promotionDone()) break;
    }
    EXPECT_GE(walkingPasses, 2) << "the passes over the erased rows were observed";
    ASSERT_NE(k.rt(), nullptr);
    EXPECT_TRUE(k.rt()->promotionDone());
    EXPECT_EQ(k.rt()->stats().promotionLinked, 1u);
}

// 11. (Task 12 re-review) A growth failure inside the promotion walk reports NO work (the worker backs off) and does not advance the cursor
// past the rid that failed; the retry once memory is back walks on and finishes.  4100 rows against a one-chunk (4096) entry store.
// Mutation: count the failed rid as walked / advance the cursor (LeaseRuntime::promote) -> the cursor moves or the retry skips the rid: red.
TEST(LeasePromotionArmingRegression, GrowStallReportsNoWorkRegression) {
    Lk k(specWith(13000, 0, 1000, 1000));
    ASSERT_TRUE(k.grantMany(4100, 10'000));
    k.r.worker->setLeasePromotionChunkForTest(0);
    ASSERT_GT(k.promoteStepwise(), 0);
    ASSERT_EQ(k.rt()->timers().capacity(), lease::kLeaseEntryChunk);
    ASSERT_EQ(k.rt()->promoteCursorForTest(), 0u);
    const auto lk = k.r.vs->leaseIndexFor(VerdictRig::kSrc, 0, k.r.replica->leadershipGeneration(), true, true);
    ASSERT_NE(lk.ref.table, nullptr);
    const auto sp = specWith(13000, 0, 1000, 1000);
    k.rt()->setPromotionChunkForTest(lease::kLeasePromotionChunk * 4);
    k.rt()->setGrowThrowForTest(true);
    EXPECT_EQ(k.rt()->promote(lk.ref, sp, k.leaseNow), 0u) << "stalled: no work reported";
    const uint32_t stuck = k.rt()->promoteCursorForTest();
    EXPECT_EQ(stuck, lease::kLeaseEntryChunk) << "the rids that fit were handled; the failed one is not";
    EXPECT_FALSE(k.rt()->promotionDone());
    EXPECT_EQ(k.rt()->promote(lk.ref, sp, k.leaseNow), 0u);
    EXPECT_EQ(k.rt()->promoteCursorForTest(), stuck) << "unchanged on the retry";
    k.rt()->setGrowThrowForTest(false);
    EXPECT_GT(k.rt()->promote(lk.ref, sp, k.leaseNow), 0u);
    EXPECT_TRUE(k.rt()->promotionDone());
}
