// Lease topics Task 12: checkpoints (spec 9.2, 9.5; R3, R4) on a real worker: LeaseRuntime::emitCheckpoints, the checkpoint scheduling rule, the
// rate window and the keepalive interplay.
//
// Every test drives the lease clock by hand (setLeaseClockForTest); every drive loop is capped.  The grants of a test are folded at one fixed
// clock value L, so a checkpoint is due at exactly L + interval.
#include "LeaseRig.h"
#include "AllocWindow.h"

#include <rune/storage/testing/DemandChannelForTest.h>

#include <array>
#include <map>
#include <set>

using namespace ltr;

namespace {

/// `keys` granted with `ttl`, judged, the runtime up.
bool setup(Lk& k, std::initializer_list<uint64_t> keys, uint64_t ttl = 5000) {
    std::vector<RawRec> recs;
    for (uint64_t id : keys) recs.push_back(grantRec(id, ttl));
    if (!k.produce(recs)) return false;
    if (!k.r.driveUntilJudged()) return false;
    return k.waitRuntime();
}
/// The remaining column of every Checkpoint record of the log, in log order.
std::vector<uint64_t> checkpointRemainings(VerdictRig& r) {
    std::vector<uint64_t> out;
    for (const auto& s : withOp(r, rec::LeaseOp::Checkpoint)) out.push_back(le(s.value, 28));
    return out;
}
/// Passes until `n` Checkpoint records are in the log (hard cap).
bool waitCheckpoints(Lk& k, std::size_t n) {
    for (int i = 0; i < kCap && countOp(k.r, rec::LeaseOp::Checkpoint) < n; ++i) k.r.pass();
    return countOp(k.r, rec::LeaseOp::Checkpoint) >= n;
}

}  // namespace

// 1. A checkpoint is scheduled only for a lease whose ttl exceeds the interval, and only when lease.checkpoint is on.
// Mutation: `rMs <= spec.checkpointIntervalMs` -> `<` in LeaseRuntime::checkpointTime: the ttl == interval lease is scheduled: red.
TEST(LeaseCheckpointRegression, OnlyWhenTtlAboveIntervalRegression) {
    {   // ttl 5 s > interval 1 s: a Checkpoint at grant + 1 s with remaining 4000
        Lk k(specWith(1000, 1, 1000, 1000));
        ASSERT_TRUE(setup(k, {1}, 5000));
        const uint32_t rid = k.ridOf(1);
        EXPECT_EQ(k.entry(1).checkpointNs, k.leaseNow + 1000 * kMs);
        EXPECT_TRUE(k.rt()->timers().checkpoint().linked(rid));
        k.advanceMs(999);
        k.r.passes(5);
        EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 0u) << "not before grant + 1 s";
        k.advanceMs(1);
        ASSERT_TRUE(waitCheckpoints(k, 1));
        EXPECT_EQ(checkpointRemainings(k.r), std::vector<uint64_t>{4000});
    }
    {   // ttl 1 s == interval 1 s: never scheduled, never a Checkpoint; the Expire comes
        Lk k(specWith(1000, 1, 1000, 1000));
        ASSERT_TRUE(setup(k, {1}, 1000));
        EXPECT_EQ(k.entry(1).checkpointNs, lease::kLeaseNoCheckpoint);
        EXPECT_FALSE(k.rt()->timers().checkpoint().linked(k.ridOf(1)));
        k.advanceMs(3000);
        k.r.passes(5);
        EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 0u);
        EXPECT_EQ(countOp(k.r, rec::LeaseOp::Expire), 1u);
    }
    {   // lease.checkpoint off: never
        Lk k(specWith(1000, 0, 1000, 1000));
        ASSERT_TRUE(setup(k, {1}, 5000));
        EXPECT_EQ(k.entry(1).checkpointNs, lease::kLeaseNoCheckpoint);
        k.advanceMs(3000);
        k.r.passes(5);
        EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 0u);
        EXPECT_EQ(countOp(k.r, rec::LeaseOp::Expire), 0u);
    }
}

// 2. A checkpoint of a lease with remaining <= interval is skipped (etcd: no checkpoint of a lease about to end).  ttl 2.5 s, interval 1 s:
// one at +1 s (remaining 1500), none at +2 s (remaining 500), the Expire at 2.5 s.
// Mutation: drop the `remainingMs <= spec.checkpointIntervalMs` skip in emitCheckpoints' sink: a Checkpoint(500) at +2 s: red.
TEST(LeaseCheckpointRegression, SkippedWhenRemainingBelowIntervalRegression) {
    Lk k(specWith(1000, 1, 1000, 1000));
    ASSERT_TRUE(setup(k, {1}, 2500));
    k.advanceMs(1000);
    ASSERT_TRUE(waitCheckpoints(k, 1));
    k.r.passes(5);
    EXPECT_EQ(checkpointRemainings(k.r), std::vector<uint64_t>{1500});
    EXPECT_EQ(k.entry(1).state, LeaseEntryState::Armed);
    EXPECT_EQ(k.entry(1).checkpointNs, k.leaseNow + 1000 * kMs) << "the fold of the entry's own record scheduled the next one";
    k.advanceMs(1000);
    k.r.passes(5);
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 1u) << "no checkpoint of a lease with 500 ms left";
    EXPECT_EQ(k.entry(1).state, LeaseEntryState::Armed);
    EXPECT_EQ(k.entry(1).checkpointNs, lease::kLeaseNoCheckpoint) << "unscheduled";
    EXPECT_FALSE(k.rt()->timers().checkpoint().linked(k.ridOf(1)));
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Expire), 0u);
    k.advanceMs(500);
    k.r.passes(5);
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Expire), 1u);
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 1u);
}

// 3. remaining = ceil_ms(deadline - leaseNow): deadline - now = 1 500 000 001 ns is 1501 ms.
// Mutation: floor instead of ceil in emitCheckpoints' sink: 1500: red.
TEST(LeaseCheckpointRegression, CeilRoundingRegression) {
    Lk k(specWith(1000, 1, 1000, 1000));
    ASSERT_TRUE(setup(k, {1}, 3000));
    k.leaseNow += 1'499'999'999;   // deadline (L + 3 s) - now = 1 500 000 001 ns
    ASSERT_TRUE(waitCheckpoints(k, 1));
    EXPECT_EQ(checkpointRemainings(k.r), std::vector<uint64_t>{1501});
    k.r.passes(5);
    EXPECT_EQ(k.rowRemaining(1), 1501u) << "the folded row";
}

// 4. The rate window and the batch cap.  20 000 one-shot leases (the plan's 1 M, cut for test time), ttl 60 s, interval 1 s, rate 3000: in every
// aligned second (the lease clock, from the runtime's ready time) at most 3000 checkpoints are appended, no batch holds more than 1000, and the
// rate is used (no stall).  Second leg: rate 100 000 (above the batch cap): every batch is still <= 1000.
// Mutations: drop the window reset (ckptUsed_ = 0): the rate never refills, the total is red; kLeaseCheckpointBatchMax 1000 -> 1001: the batch is red.
TEST(LeaseCheckpointRegression, RateAndBatchCapsRegression) {
    {
        Lk k(specWith(25000, 1, 1000, 3000));
        ASSERT_TRUE(k.grantMany(20000, 60000));
        ASSERT_TRUE(k.waitRuntime());
        const int64_t ready = k.rt()->readyNs();
        const int64_t leo0  = k.r.leo();
        std::array<uint64_t, 12> perWindow{};
        uint64_t prev = 0;
        for (int i = 0; i < 10000; ++i) {
            k.advanceMs(1);
            k.r.pass();
            const uint64_t now = k.rt()->stats().checkpointsAppended;
            const std::size_t w = std::size_t((k.leaseNow - ready) / 1'000'000'000);
            ASSERT_LT(w, perWindow.size());
            perWindow[w] += now - prev;
            prev = now;
        }
        uint64_t total = 0;
        for (std::size_t w = 0; w < perWindow.size(); ++w) {
            EXPECT_LE(perWindow[w], 3000u) << "aligned second " << w;
            total += perWindow[w];
        }
        EXPECT_GE(total, 9u * 3000u) << "the rate is used: no stall";
        EXPECT_EQ(perWindow[0], 0u) << "nothing is due in the first second";
        std::size_t biggest = 0, ckptRecords = 0;
        for (const auto& b : storedBatches(k.r, leo0)) {
            if (b.control || b.recs.empty()) continue;
            std::size_t ck = 0;
            for (const auto& s : b.recs) ck += rec::leaseOpOf(s.attrs) == rec::LeaseOp::Checkpoint ? 1 : 0;
            if (ck == 0) continue;
            EXPECT_EQ(ck, b.recs.size()) << "a checkpoint batch holds only checkpoints";
            EXPECT_LE(b.recs.size(), 1000u) << "batch at offset " << b.base;
            biggest = std::max(biggest, b.recs.size());
            ckptRecords += ck;
        }
        EXPECT_EQ(biggest, 1000u) << "the batch cap is reached";
        EXPECT_EQ(ckptRecords, total);
        EXPECT_EQ(k.rt()->stats().checkpointsAppended, total);
    }
    {
        Lk k(specWith(8000, 1, 1000, 100'000));
        ASSERT_TRUE(k.grantMany(5000, 60000));
        ASSERT_TRUE(k.waitRuntime());
        const int64_t leo0 = k.r.leo();
        for (int i = 0; i < 1500; ++i) { k.advanceMs(1); k.r.pass(); }
        std::size_t biggest = 0, ckptRecords = 0;
        for (const auto& b : storedBatches(k.r, leo0)) {
            if (b.control || b.recs.empty()) continue;
            std::size_t ck = 0;
            for (const auto& s : b.recs) ck += rec::leaseOpOf(s.attrs) == rec::LeaseOp::Checkpoint ? 1 : 0;
            if (ck == 0) continue;
            EXPECT_LE(b.recs.size(), 1000u) << "batch at offset " << b.base;
            biggest = std::max(biggest, b.recs.size());
            ckptRecords += ck;
        }
        EXPECT_EQ(biggest, 1000u);
        EXPECT_GE(ckptRecords, 5000u) << "every lease was checkpointed once the second passed";
    }
}

// 5. A keepalive clears the checkpoint's remaining time ONCE: the first keepalive after a checkpoint appends one Checkpoint(0); the second finds
// remaining 0 and is answered at once with no record.
TEST(LeaseCheckpointRegression, ClearedByFirstKeepAliveOnlyRegression) {
    Lk k(specWith(1000, 1, 1000, 1000));
    ASSERT_TRUE(setup(k, {1}, 5000));
    const int64_t v = k.versionOf(1);
    k.advanceMs(1000);
    ASSERT_TRUE(waitCheckpoints(k, 1));
    k.r.passes(5);
    ASSERT_EQ(k.rowRemaining(1), 4000u);
    (void)k.keepAlive({E(1, v)});   // the clear is appended, folds and answers (within a pass when the judge is not held)
    ASSERT_TRUE(k.waitFrames(1));
    ASSERT_EQ(k.frames.size(), 1u);
    EXPECT_EQ(k.frames[0].es[0].code, 0);
    EXPECT_EQ(k.frames[0].es[0].ttl, 5000u);
    EXPECT_EQ(k.rowRemaining(1), 0u);
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 2u) << "Checkpoint(4000) and one Checkpoint(0)";
    EXPECT_EQ(checkpointRemainings(k.r), (std::vector<uint64_t>{4000, 0}));
    const auto fr = k.keepAlive({E(1, v)});
    ASSERT_EQ(fr.size(), 1u) << "answered at once";
    EXPECT_EQ(fr[0].es[0].code, 0);
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 2u) << "no second clear";
    EXPECT_EQ(k.rt()->stats().clearsAppended, 1u);
}

// 6. A key kept alive every ttl/3 for 10 intervals costs at most two lease records per interval: one Checkpoint(R) and one Checkpoint(0).
TEST(LeaseCheckpointRegression, AtMostTwoRecordsPerIntervalRegression) {
    Lk k(specWith(1000, 1, 1000, 1000));
    ASSERT_TRUE(setup(k, {1}, 6000));
    const int64_t v = k.versionOf(1);
    const int64_t start = k.leaseNow;
    std::array<uint64_t, 12> perInterval{};
    uint64_t prev = 0;
    for (int i = 1; i <= 10000; ++i) {
        k.advanceMs(1);
        if (i % 2000 == 0) k.enqueue({E(1, v)});   // every ttl / 3
        k.step();
        const uint64_t now = k.rt()->stats().checkpointsAppended + k.rt()->stats().clearsAppended;
        const std::size_t w = std::size_t((k.leaseNow - start - 1) / (1000 * kMs));
        ASSERT_LT(w, perInterval.size());
        perInterval[w] += now - prev;
        prev = now;
    }
    for (std::size_t w = 0; w < 10; ++w) EXPECT_LE(perInterval[w], 2u) << "interval " << w;
    EXPECT_GE(k.rt()->stats().clearsAppended, 3u) << "the keepalives did clear";
    EXPECT_GE(k.rt()->stats().checkpointsAppended, 8u) << "the checkpoints did run";
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Expire), 0u) << "the key stayed alive";
}

// 7. An ALTER of the checkpoint keys applies to checkpoints scheduled after the catalog event (the alter seam: the WriteSpecUpdate op the
// catalog applier hands the worker, VerdictRig::alterLeaseSpec).  The already-linked time stays; the next one uses the new interval;
// checkpoint = 0 unschedules the next due checkpoint without a record.
TEST(LeaseCheckpointRegression, CheckpointKeyAlterAppliesLaterRegression) {
    Lk k(specWith(1000, 1, 1000, 1000));
    ASSERT_TRUE(setup(k, {1}, 20000));
    auto spec = specWith(1000, 1, 3000, 1000);
    k.r.alterLeaseSpec(spec);
    EXPECT_EQ(k.entry(1).checkpointNs, k.leaseNow + 1000 * kMs) << "the linked checkpoint time stays";
    k.advanceMs(1000);
    ASSERT_TRUE(waitCheckpoints(k, 1));
    k.r.passes(5);
    EXPECT_EQ(checkpointRemainings(k.r), std::vector<uint64_t>{19000}) << "fired at its old time";
    EXPECT_EQ(k.entry(1).checkpointNs, k.leaseNow + 3000 * kMs) << "the one after uses the new interval";
    k.advanceMs(2999);
    k.r.passes(5);
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 1u);
    k.advanceMs(1);
    ASSERT_TRUE(waitCheckpoints(k, 2));
    k.r.passes(5);
    EXPECT_EQ(checkpointRemainings(k.r), (std::vector<uint64_t>{19000, 16000}));
    spec.checkpoint = 0;
    k.r.alterLeaseSpec(spec);
    k.advanceMs(3000);
    k.r.passes(5);
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 2u) << "checkpoint = 0: the due checkpoint emits nothing";
    EXPECT_EQ(k.entry(1).checkpointNs, lease::kLeaseNoCheckpoint) << "and is unscheduled";
    EXPECT_FALSE(k.rt()->timers().checkpoint().linked(k.ridOf(1)));
    EXPECT_EQ(k.entry(1).state, LeaseEntryState::Armed);
}

// 8. The fold of the entry's OWN Checkpoint(R) leaves it Armed with the NEXT checkpoint one interval from the fold, not the past time it was
// polled at; no second Checkpoint follows in the next passes.
// Mutation: drop `timers_.scheduleCheckpoint(...)` before foldStep_ in onEvents' Checkpointed case: setState(Armed) relinks the past time and a
// second Checkpoint is appended at once: red.
TEST(LeaseCheckpointRegression, OwnCheckpointFoldReschedulesRegression) {
    Lk k(specWith(1000, 1, 1000, 1000));
    ASSERT_TRUE(setup(k, {1}, 5000));
    k.advanceMs(1000);
    ASSERT_TRUE(waitCheckpoints(k, 1));
    for (int i = 0; i < kCap && k.entry(1).state != LeaseEntryState::Armed; ++i) k.r.pass();
    ASSERT_EQ(k.entry(1).state, LeaseEntryState::Armed);
    EXPECT_EQ(k.entry(1).checkpointNs, k.leaseNow + 1000 * kMs) << "fold time + interval";
    EXPECT_TRUE(k.rt()->timers().checkpoint().linked(k.ridOf(1)));
    k.r.passes(10);
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 1u) << "no second Checkpoint in the next 10 passes";
}

// 9. (R4) A keepalive during Checkpointing parks (no frame); the fold of the checkpoint replays it into the clear path, and it is answered Ok
// only after the Checkpoint(0) folds.
TEST(LeaseCheckpointRegression, KeepAliveDuringCheckpointParksRegression) {
    Lk k(specWith(1000, 1, 1000, 1000));
    ASSERT_TRUE(setup(k, {1}, 5000));
    const int64_t v = k.versionOf(1);
    k.r.holdJudge(true);
    k.advanceMs(1000);
    k.r.pass();
    ASSERT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 1u);
    ASSERT_EQ(k.entry(1).state, LeaseEntryState::Checkpointing);
    auto fr = k.keepAlive({E(1, v)});
    EXPECT_TRUE(fr.empty()) << "parked: no frame";
    k.r.passes(3);
    k.collect();
    EXPECT_TRUE(k.frames.empty());
    EXPECT_EQ(k.rt()->stats().clearsAppended, 0u);
    k.r.holdJudge(false);
    for (int i = 0; i < kCap && k.rt()->stats().clearsAppended == 0; ++i) k.step();   // the checkpoint folds, the replayed keepalive takes the clear path
    ASSERT_EQ(k.rt()->stats().clearsAppended, 1u);
    k.r.holdJudge(true);   // the Checkpoint(0) is appended and not yet folded
    k.r.passes(3);
    k.collect();
    EXPECT_TRUE(k.frames.empty()) << "not answered before the Checkpoint(0) folds";
    EXPECT_EQ(k.entry(1).state, LeaseEntryState::Clearing);
    k.r.holdJudge(false);
    ASSERT_TRUE(k.waitFrames(1));
    EXPECT_EQ(k.frames[0].es[0].code, 0);
    EXPECT_EQ(k.frames[0].es[0].ttl, 5000u);
    EXPECT_EQ(k.rowRemaining(1), 0u);
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 2u);
}

// 10. (hard rule 4) A checkpoint batch and a promotion chunk allocate nothing: the second batch of due checkpoints, the next promotion chunk and
// the judge folds of the checkpoints all run inside the window, after one warm-up pass of each.
TEST(LeaseCheckpointAllocRegression, CheckpointAndPromotionNoAllocRegression) {
    Lk k(specWith(1000, 1, 1000, 1000));
    ASSERT_TRUE(k.grantMany(400, 60000));
    k.r.worker->setLeasePromotionChunkForTest(100);
    ASSERT_GT(k.promoteStepwise(), 0);   // pass 1 of the promotion: 100 rows linked (cold)
    ASSERT_EQ(k.rt()->stats().promotionLinked, 100u);
    k.advanceMs(1000);
    k.r.pass();                      // warm-up: the second chunk and the first checkpoint batch (the first 100 rows)
    ASSERT_EQ(k.rt()->stats().promotionLinked, 200u);
    ASSERT_GE(k.rt()->stats().checkpointsAppended, 100u);
    k.advanceMs(1000);
    const uint64_t ckptBefore = k.rt()->stats().checkpointsAppended;
    uint64_t allocs = 0;
    {
        rune::testing::AllocWindow w;
        k.r.passes(4);   // the third and fourth chunks, the next checkpoint batches, the judge folds of all of them
        allocs = w.count();
    }
    EXPECT_EQ(k.rt()->stats().promotionLinked, 400u);
    EXPECT_GT(k.rt()->stats().checkpointsAppended, ckptBefore);
    EXPECT_EQ(allocs, 0u);
}

// 11. (F3) A key whose deadline and checkpoint time are BOTH due in one pass is never checkpointed: it expires first.  expiryBatch 1: pass 1
// expires one key and the checkpoint sink meets the other, due and Armed, with its checkpoint time long due and the gate open.  No Checkpoint
// record is ever appended; the sink unschedules the due key; both keys expire (once each) over the passes.
// Mutation: the sink ignores `due` (LeaseRuntime::emitCheckpoints: `const bool due = false`, and `!due` out of `wanted`): a Checkpoint is appended: red.
TEST(LeaseCheckpointRegression, DueKeyNeverCheckpointedRegression) {
    auto spec = specWith(1000, 1, 1000, 1000);
    spec.expiryBatch = 1;
    Lk k(spec);
    ASSERT_TRUE(setup(k, {1, 2}, 5000));
    const uint32_t rids[2] = {k.ridOf(1), k.ridOf(2)};   // taken now: an expired key's row is erased
    k.advanceMs(6000);   // the checkpoint (+1 s) and the deadline (+5 s) are due in the SAME pass
    k.r.pass();
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Expire), 1u) << "expiryBatch 1: one Expire in pass 1";
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 0u);
    std::size_t armed = 0;
    for (const uint32_t rid : rids) {
        if (k.rt()->timers().entry(rid).state != LeaseEntryState::Armed) continue;
        ++armed;
        EXPECT_EQ(k.rt()->timers().entry(rid).checkpointNs, lease::kLeaseNoCheckpoint) << "the due key was unscheduled, not checkpointed";
        EXPECT_FALSE(k.rt()->timers().checkpoint().linked(rid));
    }
    EXPECT_EQ(armed, 1u) << "the other key is still Armed, waiting for its Expire";
    k.r.passes(10);
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 0u) << "never";
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Expire), 2u);
    for (const uint32_t rid : rids) EXPECT_EQ(k.rt()->timers().entry(rid).checkpointNs, lease::kLeaseNoCheckpoint);
}

// 12. (F4a) The per-key gate defers a due checkpoint: an update of the key appended but not judged means the re-encode would miss it.  Nothing
// is appended while the judge is held (checkpointDeferred counts), and the checkpoint goes out once the update is judged.
// Mutation: the sink takes the gate as open (`const bool gate = true`): a Checkpoint is appended while held: red.
TEST(LeaseCheckpointRegression, GateClosedDefersThenEmitsRegression) {
    Lk k(specWith(1000, 1, 1000, 1000));
    ASSERT_TRUE(setup(k, {1}, 5000));
    const int64_t v = k.versionOf(1);
    k.r.holdJudge(true);
    (void)k.send({updateRec(1, v)});
    k.r.passes(30);
    k.advanceMs(1000);
    k.r.passes(5);
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 0u) << "the gate is closed";
    EXPECT_GE(k.rt()->stats().checkpointDeferred, 1u);
    EXPECT_EQ(k.entry(1).state, LeaseEntryState::Armed);
    k.r.holdJudge(false);
    ASSERT_TRUE(waitCheckpoints(k, 1));
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 1u);
}

// 13. (F4b) A refused append (ISR below min-ISR, or a throwing storage) appends nothing, leaves the tokens unspent and the entries due on the
// next cycle: with rate 1 per second the next pass after the refusal clears still emits the checkpoint in the SAME one-second window.
// Mutation: spend the tokens on a refusal (`ckptUsed_ += n` in the refused path of emitCheckpoints): the checkpoint waits a second: red.
TEST(LeaseCheckpointRegression, RefusedAppendRetriesNextCycleRegression) {
    {   // a throwing append
        Lk k(specWith(1000, 1, 1000, 1));
        ASSERT_TRUE(setup(k, {1}, 5000));
        k.advanceMs(1000);
        k.r.replica->setLeaseAppendThrowForTest(true);
        k.r.passes(3);
        EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 0u);
        EXPECT_GE(k.rt()->stats().appendErrors, 1u);
        EXPECT_EQ(k.rt()->stats().checkpointsAppended, 0u);
        EXPECT_EQ(k.entry(1).state, LeaseEntryState::Armed);
        k.r.replica->setLeaseAppendThrowForTest(false);
        k.r.pass();
        EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 1u) << "the very next pass: the tokens were not spent";
    }
    {   // ISR below min-ISR
        Lk k(specWith(1000, 1, 1000, 1), /*follower=*/true, RigTuning{.minIsr = 2});
        const int64_t c = k.send({grantRec(1, 5000)});
        const auto ack = k.r.ackFetching(c);
        ASSERT_TRUE(ack);
        ASSERT_EQ(ack->code, 0);
        ASSERT_TRUE(k.r.driveUntilJudged());
        ASSERT_TRUE(k.waitRuntime());
        const int32_t live[1] = {1};
        k.r.replica->shrinkIsrToLive(std::span<const int32_t>(live, 1), k.r.now);   // |ISR| 1 < minIsr 2
        k.advanceMs(1000);
        const int64_t leo = k.r.leo();
        k.r.passes(3);
        EXPECT_EQ(k.r.leo(), leo) << "nothing appended";
        EXPECT_EQ(k.rt()->stats().checkpointBacklog, 3u) << "1 record x 3 attempts, counted as the CHECKPOINT backlog";
        EXPECT_EQ(k.rt()->stats().expiryBacklog, 0u);
        EXPECT_EQ(k.rt()->stats().checkpointsAppended, 0u);
        k.r.replica->becomeLeader(k.r.replica->epoch(), std::vector<rune::replication::NodeId>{1, 2});
        for (int i = 0; i < 5 && countOp(k.r, rec::LeaseOp::Checkpoint) == 0; ++i) k.r.pass();
        EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 1u) << "emitted on the next cycle after the ISR is back";
    }
}

// 14. (F4c) A full batch leaves the rest: five due checkpoints, a byte cap that fits two records: 2, then 2, then 1 over the passes - none lost,
// none doubled (five distinct keys).
// Mutation: the sink drops a key it cannot fit (return true instead of false at Fit::Full): the rest never go out: red.
TEST(LeaseCheckpointRegression, FullBatchLeavesRestRegression) {
    Lk k(specWith(1000, 1, 1000, 1000));
    ASSERT_TRUE(setup(k, {1, 2, 3, 4, 5}, 5000));
    k.r.worker->setLeaseBatchCapForTest(250);   // 61 header + 2 x 75 = 211 fit, 3 x 75 = 286 do not
    k.advanceMs(1000);
    std::vector<std::size_t> after;
    for (int i = 0; i < 3; ++i) {
        k.r.pass();
        after.push_back(countOp(k.r, rec::LeaseOp::Checkpoint));
    }
    EXPECT_EQ(after, (std::vector<std::size_t>{2, 4, 5}));
    k.r.passes(5);
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 5u) << "no repeat";
    std::set<std::string> keys;
    for (const auto& s : withOp(k.r, rec::LeaseOp::Checkpoint)) keys.insert(s.key);
    EXPECT_EQ(keys.size(), 5u) << "five distinct keys";
}

// 15. (F4d) Roll back-pressure: while the log's roll is deferred no checkpoint is appended (counted as a roll-deferred skip); they go out once a
// segment is ready.  Mutation: drop the rollDeferred check in leaseDuty_: a Checkpoint lands while held: red.
TEST(LeaseCheckpointRegression, RollDeferredSkipsCheckpointsRegression) {
    // rollBytes below the smallest batch (61 bytes of batch overhead plus a record): every segment holds one batch and is full, so the
    // active one is full whatever the last control record's size (a v2 constraint-set change is 102 bytes, a marker 95).
    Lk k(specWith(1000, 1, 1000, 1000), false, RigTuning{}, /*rollBytes=*/64);
    ASSERT_TRUE(setup(k, {1, 2}, 5000));
    struct Hook { static bool fn(void*, rune::storage::Log*, rune::storage::Log::ProvisionDemand) noexcept { return true; } };
    rune::storage::testing::DemandChannelForTest demandCh{&Hook::fn, nullptr};
    k.r.log->setProvisioning(&demandCh);
    ASSERT_TRUE(k.r.log->rollDeferred()) << "the active segment is full and no segment is ready";
    k.advanceMs(1000);
    const int64_t leo = k.r.leo();
    k.r.passes(5);
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 0u) << "held: no Checkpoint";
    EXPECT_EQ(k.r.leo(), leo);
    EXPECT_GE(k.rt()->stats().rollDeferredSkips, 5u);
    ASSERT_EQ(k.r.log->provisionReady(1), 1);
    EXPECT_TRUE(waitCheckpoints(k, 2)) << "released: both are checkpointed";
    (void)k.r.log->finalizeStagingRenames();
    (void)k.r.log->drainStagingRenamesDone();
}

// 16. (F8, F4e) A rejected own Checkpoint(R) returns the entry to Armed with the NEXT checkpoint one interval from the rejection - not the past time
// it was polled at, which would append the same checkpoint again at once.
// Mutation: drop the reschedule before the relink in onEvents' BrokerRejected path: a second Checkpoint on the next pass: red.
TEST(LeaseCheckpointRegression, CheckpointRejectedRearmsLaterRegression) {
    Lk k(specWith(1000, 1, 1000, 1000));
    ASSERT_TRUE(setup(k, {1}, 5000));
    const int64_t v = k.versionOf(1);
    k.r.holdJudge(true);   // the Checkpoint stays in flight
    k.advanceMs(1000);
    k.r.pass();
    ASSERT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 1u);
    ASSERT_EQ(k.entry(1).state, LeaseEntryState::Checkpointing);
    const int64_t at = k.leaseNow;
    LeaseEvent ev;
    ev.kind = LeaseEventKind::BrokerRejected; ev.rid = k.ridOf(1); ev.offset = withOp(k.r, rec::LeaseOp::Checkpoint)[0].offset; ev.version = v;
    k.r.worker->onEventsForTest(VerdictRig::kSrc, 0, std::span<const LeaseEvent>(&ev, 1), at);
    ASSERT_EQ(k.entry(1).state, LeaseEntryState::Armed);
    EXPECT_EQ(k.rt()->stats().ownRecordRejected, 1u);
    EXPECT_EQ(k.entry(1).checkpointNs, at + 1000 * kMs) << "one interval from the rejection";
    k.r.passes(5);
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 1u) << "not again at once";
    k.advanceMs(999);
    k.r.passes(3);
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 1u) << "not before leaseNow + interval";
    k.advanceMs(1);
    ASSERT_TRUE(waitCheckpoints(k, 2));
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 2u);
}

// 17. (F6) A checkpoint that cannot be appended because its lone record exceeds the hard cap is skipped (spec 9.5: a checkpoint may be skipped):
// the key is unscheduled, not left forever-deferred in the wheel (the pass has no pending work afterwards).  A later keepalive re-arm reschedules.
// (The encode-failure branch shares this code; it has no test seam.)
// Mutation: return false (defer) at Fit::Oversized in the checkpoint sink: the row stays scheduled and due, hasPendingWork stays true: red.
TEST(LeaseCheckpointRegression, OversizedCheckpointUnscheduledRegression) {
    Lk k(specWith(1000, 1, 1000, 1000));
    ASSERT_TRUE(setup(k, {1}, 5000));
    k.r.worker->setMaxBatchBytes(100);   // below one checkpoint batch (61 + ~75)
    k.advanceMs(1000);
    k.r.passes(3);
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 0u);
    EXPECT_GE(k.rt()->stats().oversizedDeferred, 1u);
    EXPECT_EQ(k.entry(1).state, LeaseEntryState::Armed);
    EXPECT_EQ(k.entry(1).checkpointNs, lease::kLeaseNoCheckpoint) << "unscheduled";
    EXPECT_FALSE(k.rt()->timers().checkpoint().linked(k.ridOf(1)));
    EXPECT_FALSE(k.rt()->hasPendingWork(k.leaseNow)) << "no forever-deferred row";
    k.r.worker->setMaxBatchBytes(64u << 10);
    k.r.passes(3);
    EXPECT_EQ(countOp(k.r, rec::LeaseOp::Checkpoint), 0u) << "skipped, not retried";
}

// 18. (F7) A parked keepalive answered Ok by the remaining-0 branch of flushClears renews the lease AND schedules its next checkpoint (as every
// keepalive re-arm does).  Setup: a Checkpoint(R) in flight, a keepalive parked behind it, 500 ms left when the checkpoint is refused (so the
// refusal unschedules: remaining <= interval); the replay takes the clear path, the row's remaining is 0: Ok at once, deadline now + ttl.
// Mutation: drop `scheduleIfNone_` in flushClears' remaining-0 branch: checkpointNs stays kLeaseNoCheckpoint: red.
TEST(LeaseCheckpointRegression, ClearRenewalSchedulesNextCheckpointRegression) {
    Lk k(specWith(1000, 1, 1000, 1000));
    ASSERT_TRUE(setup(k, {1}, 5000));
    const int64_t v = k.versionOf(1);
    k.r.holdJudge(true);
    k.advanceMs(1000);
    k.r.pass();
    ASSERT_EQ(k.entry(1).state, LeaseEntryState::Checkpointing);
    EXPECT_TRUE(k.keepAlive({E(1, v)}).empty()) << "parked behind the checkpoint";
    k.advanceMs(3500);   // 500 ms left
    LeaseEvent ev;
    ev.kind = LeaseEventKind::BrokerRejected; ev.rid = k.ridOf(1); ev.offset = withOp(k.r, rec::LeaseOp::Checkpoint)[0].offset; ev.version = v;
    k.r.worker->onEventsForTest(VerdictRig::kSrc, 0, std::span<const LeaseEvent>(&ev, 1), k.leaseNow);
    ASSERT_EQ(k.entry(1).state, LeaseEntryState::Armed);
    EXPECT_EQ(k.entry(1).checkpointNs, lease::kLeaseNoCheckpoint) << "500 ms left: nothing to checkpoint";
    k.step();
    ASSERT_EQ(k.frames.size(), 1u);
    EXPECT_EQ(k.frames[0].es[0].code, 0);
    EXPECT_EQ(k.frames[0].es[0].ttl, 5000u);
    EXPECT_EQ(k.entry(1).deadlineNs, k.leaseNow + 5000 * kMs);
    EXPECT_EQ(k.entry(1).checkpointNs, k.leaseNow + 1000 * kMs) << "the renewal scheduled the next checkpoint";
    EXPECT_TRUE(k.rt()->timers().checkpoint().linked(k.ridOf(1)));
}
