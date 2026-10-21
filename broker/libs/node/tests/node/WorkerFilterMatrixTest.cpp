// WorkerFilterMatrixTest.cpp - Phase 2c Task 14: the worker-level filter interaction matrix.
//
// 5 ops x 4 entities x 14 timings = 280 cells: 232 applicable, 48 not applicable (NA1 40, NA2 8), every one named by
// FilterMatrixExpectations.h.  Each applicable cell builds a Rig (a real Worker over a real disk Log or a transient
// ring, an embedded Aeron driver, a model client), runs the timing's script and asserts the six points of "Every cell
// asserts": the outcome from expect(), the exact delivered set, a monotonic position that reaches the end, unfiltered
// members unaffected, zero allocation in the measured passes, and the counters.
#include "support/FilterMatrixExpectations.h"
#include "support/FilterMatrixRig.h"

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <tuple>

using namespace rune::node::filtermatrix;
using namespace std::chrono_literals;

namespace {

using Ms = std::vector<Member>;

constexpr std::array<Op, 5> kOps{Op::Unfiltered, Op::Eq, Op::In, Op::EqAndIn, Op::MixedIndexFields};
constexpr std::array<Entity, 4> kEntities{Entity::DiskRU, Entity::DiskRC, Entity::TransientRU, Entity::TransientRC};
constexpr std::array<Timing, 14> kTimings{
    Timing::SteadyAllMatch, Timing::SteadyPartial, Timing::SteadyNoMatch, Timing::StraddlingResume, Timing::QuietRun,
    Timing::OpenTxn, Timing::AbortedTxn, Timing::CommittedTxn, Timing::OwnershipMove, Timing::TransientEviction,
    Timing::AlterAddField, Timing::AlterDropFilteredField, Timing::VersionArrivesLate, Timing::BackPressure};

const char* name(Op o) {
    switch (o) {
    case Op::Unfiltered: return "Unfiltered"; case Op::Eq: return "Eq"; case Op::In: return "In";
    case Op::EqAndIn: return "EqAndIn"; case Op::MixedIndexFields: return "MixedIndexFields";
    }
    return "?";
}
const char* name(Entity e) {
    switch (e) {
    case Entity::DiskRU: return "DiskRU"; case Entity::DiskRC: return "DiskRC";
    case Entity::TransientRU: return "TransientRU"; case Entity::TransientRC: return "TransientRC";
    }
    return "?";
}
const char* name(Timing t) {
    switch (t) {
    case Timing::SteadyAllMatch: return "SteadyAllMatch"; case Timing::SteadyPartial: return "SteadyPartial";
    case Timing::SteadyNoMatch: return "SteadyNoMatch"; case Timing::StraddlingResume: return "StraddlingResume";
    case Timing::QuietRun: return "QuietRun"; case Timing::OpenTxn: return "OpenTxn"; case Timing::AbortedTxn: return "AbortedTxn";
    case Timing::CommittedTxn: return "CommittedTxn"; case Timing::OwnershipMove: return "OwnershipMove";
    case Timing::TransientEviction: return "TransientEviction"; case Timing::AlterAddField: return "AlterAddField";
    case Timing::AlterDropFilteredField: return "AlterDropFilteredField"; case Timing::VersionArrivesLate: return "VersionArrivesLate";
    case Timing::BackPressure: return "BackPressure";
    }
    return "?";
}
const char* name(NaRule r) { return r == NaRule::NA1 ? "NA1" : "NA2"; }

// ---------------------------------------------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------------------------------------------
[[nodiscard]] Ms filtered(const Ms& ms) {
    Ms v;
    for (const auto& m : ms) if (m.kind != MK::Unfiltered) v.push_back(m);
    return v;
}
/// One worker with the schema installed and the cell's warm-up done; returns the offset the members subscribe from.
int64_t start(Rig& r, uint16_t schemaVersion = 2, int primeRows = 120) {
    r.addWorker();
    r.installSchema(0, schemaVersion);
    return r.prime(0, schemaVersion, primeRows);
}
bool awaitInSync(Rig& r, const Ms& ms) {
    return r.pumpUntil([&] {
        for (const auto& m : ms) if (m.kind != MK::Unfiltered && !r.inSync(m)) return false;
        return true;
    });
}
/// The records of batch `bi` that member kind `k` should receive.
std::vector<int64_t> matchesInBatch(const Rig& r, MK k, const BatchRec& b) {
    std::vector<int64_t> v;
    for (const auto& rec : r.recs())
        if (rec.off >= b.base && rec.off <= b.last && r.recVisible(rec) && matches(k, rec)) v.push_back(rec.off);
    return v;
}

/// The per-outcome assertions that hold at the end of the steady cells (R1-R6).
void checkSteadyOutcome(Rig& r, Entity e, Timing t, const Ms& ms, std::size_t wi = 0) {
    const Outcome out = expect(r.op(), e, t);
    SCOPED_TRACE(std::string("outcome check for ") + name(t));
    const auto c = r.w(wi).filterCountersForTest();
    for (const auto& m : ms) {
        if (m.worker != wi) continue;
        MemberView& v = r.view(m.sub);
        if (m.kind == MK::Unfiltered) {
            EXPECT_TRUE(v.notes.empty()) << "unfiltered subscriptions never get notes";
            continue;
        }
        switch (out) {
        case Outcome::ByteIdentical: ADD_FAILURE() << "a filtered member in an Unfiltered cell"; break;
        case Outcome::ZeroCopyOriginal: {
            EXPECT_FALSE(v.dataFrames.empty());
            for (std::size_t i = 0; i < v.dataFrames.size(); ++i) {
                const BatchRec* src = nullptr;
                for (const auto& b : r.batches()) if (b.base == v.dataFrameBases[i]) src = &b;
                ASSERT_NE(src, nullptr);
                EXPECT_EQ(v.dataFrames[i], src->bytes) << "frame " << i << " is not the log batch byte for byte";
            }
            EXPECT_EQ(c.rebuilt, 0u);
            EXPECT_GT(c.zeroCopy, 0u);
            break;
        }
        case Outcome::Rebuilt: {
            std::size_t withMatch = 0;
            for (const auto& b : r.batches()) if (!b.control && b.last >= v.start && !matchesInBatch(r, m.kind, b).empty()) ++withMatch;
            ASSERT_EQ(v.dataFrames.size(), withMatch) << "one frame per source batch holding a match";
            for (std::size_t i = 0; i < v.dataFrames.size(); ++i) {
                const BatchRec* src = nullptr;
                for (const auto& b : r.batches()) if (b.base == v.dataFrameBases[i]) src = &b;
                ASSERT_NE(src, nullptr);
                rune::protocol::RecordBatchReader rd(v.dataFrames[i].data(), v.dataFrames[i].size());
                EXPECT_EQ(rd.baseOffset(), src->base);
                EXPECT_EQ(rd.baseOffset() + rd.lastOffsetDelta(), src->last) << "same lastOffsetDelta";
                std::vector<int64_t> got;
                rd.forEachRecord([&](std::string_view, std::string_view, int64_t o, int64_t) { got.push_back(o); });
                EXPECT_EQ(got, matchesInBatch(r, m.kind, *src)) << "exactly the matching records, original offsets";
                EXPECT_NE(v.dataFrames[i], src->bytes) << "a partial batch must be rebuilt";
            }
            EXPECT_EQ(c.zeroCopy, 0u);
            break;
        }
        case Outcome::NoteOnly:
            EXPECT_TRUE(v.dataFrames.empty()) << "no data frame";
            ASSERT_FALSE(v.notes.empty());
            EXPECT_EQ(v.notes.back(), r.logEnd()) << "the last note is the log end";
            break;
        case Outcome::WholeBatchFromPosition: {
            ASSERT_FALSE(v.dataFrames.empty());
            const BatchRec* holder = nullptr;
            for (const auto& b : r.batches()) if (b.base <= v.start && v.start <= b.last) holder = &b;
            ASSERT_NE(holder, nullptr);
            EXPECT_EQ(v.dataFrameBases.front(), holder->base) << "the first frame is the whole straddling batch";
            std::size_t below = 0;
            for (const auto& rec : r.recs())
                if (rec.off >= holder->base && rec.off < v.start && r.recVisible(rec) && matches(m.kind, rec)) ++below;
            EXPECT_EQ(v.straddleDrops, below) << "the model client dropped exactly the records below the start";
            break;
        }
        default: break;
        }
    }
}

/// Assertions 5 and 6 for the cells whose only exclusions are the ones named.
void finish(Rig& r, const Ms& ms, const std::string& where, uint64_t absent = 0, uint64_t unknown = 0, std::size_t wi = 0) {
    r.verifyCounters(wi, ms, absent, unknown, where);
    r.verifyNoAllocation(where);
    EXPECT_GT(r.hotPasses(), 0u) << "no measured pass: the allocation window never opened";
}

// ---------------------------------------------------------------------------------------------------------------
// Scripts
// ---------------------------------------------------------------------------------------------------------------
void runSteady(Rig& r, Entity e, Timing t, const std::vector<std::vector<PlayerRow>>& pattern) {
    const int64_t b0 = start(r);
    Ms ms = r.subscribeMembers(0, b0);
    for (int wave = 1; wave <= 2; ++wave) {   // wave 1 warms up, wave 2 is the measured one
        for (const auto& rows : pattern) r.append(rows);
        ASSERT_TRUE(r.converge(ms)) << "wave " << wave;
        r.verifyAll(ms, "wave " + std::to_string(wave));
    }
    checkSteadyOutcome(r, e, t, ms);
    finish(r, ms, "steady");
}

void runStraddling(Rig& r, Entity e) {
    const int64_t b0 = start(r);
    // batch 0 @b0..b0+3, batch 1 @b0+4..b0+7 (the straddling one), batch 2 @b0+8..b0+11
    r.append({rowM(), rowN(), rowW(), rowC()});
    r.append({rowM(), rowN(), rowM(), rowN()});
    r.append({rowN(), rowM(), rowC(), rowW()});
    Ms ms = r.subscribeMembers(0, /*from=*/b0 + 6);
    ASSERT_TRUE(r.converge(ms));
    r.verifyAll(ms, "straddle");
    r.append({rowM(), rowM(), rowN(), rowN()});   // live
    r.append({rowW(), rowM(), rowC(), rowN()});
    ASSERT_TRUE(r.converge(ms));
    r.verifyAll(ms, "live");
    checkSteadyOutcome(r, e, Timing::StraddlingResume, ms);
    finish(r, ms, "straddle");
}

void runQuietRun(Rig& r, Entity e) {
    constexpr int kBatches = 2500, kRows = 80;   // 200,000 non-matching records
    // ---- leg A: by count (time notes effectively off) ----
    r.addWorker(true, /*noteIntervalNs=*/100'000'000'000LL);
    r.installSchema(0, 2);
    const int64_t b0 = r.prime(0, 2);
    r.append({rowM()});   // the one match, @b0
    Ms ms = r.subscribeMembers(0, b0);
    ASSERT_TRUE(awaitInSync(r, ms));
    ASSERT_TRUE(r.pumpUntil([&] { for (const auto& m : ms) if (r.view(m.sub).pos < r.targetPos(m) && m.kind == MK::Unfiltered) return false; return true; }));
    r.settle(20ms);
    const std::vector<PlayerRow> quiet = rep(rowN(), kRows);
    for (int i = 0; i < kBatches; ++i) r.append(quiet);
    const int64_t end = r.logEnd();
    ASSERT_EQ(end, b0 + 1 + static_cast<int64_t>(kBatches) * kRows);
    ASSERT_TRUE(r.pumpUntil([&] {
        for (const auto& m : ms) {
            if (m.kind == MK::Unfiltered) { if (r.view(m.sub).pos < end) return false; }
            else if (r.w(0).subscriptionPositionForTest(m.sub) < end) return false;
        }
        return true;
    }, 60s)) << "the scan never reached the log end";
    r.settle(30ms);
    const std::size_t bytesPerRec = r.batches().back().bytes.size() / kRows;
    const int64_t slack = static_cast<int64_t>(128 * 1024 / bytesPerRec) + 2 * kRows;   // one pass worth of records
    for (const auto& m : ms) {
        const MemberView& v = r.view(m.sub);
        for (const auto& s : v.violations) ADD_FAILURE() << s;
        if (m.kind == MK::Unfiltered) { r.verifyMember(m, "leg A"); continue; }
        EXPECT_EQ(v.delivered, (std::vector<int64_t>{b0})) << "only the one match";
        std::string ns;
        for (const int64_t n : v.notes) ns += std::to_string(n) + " ";
        ASSERT_GE(v.notes.size(), 3u) << "notes: " << ns << " end " << end << " 200,000 skipped records need at least three count notes";
        int64_t prev = b0 + 1;   // the position after the match
        for (const int64_t n : v.notes) {
            EXPECT_GT(n, prev - 1) << "note offsets increase";
            EXPECT_LE(n - prev, 65'536 + slack) << "a note within each 65,536 skipped records";
            prev = n;
        }
        EXPECT_LT(end - prev, 65'536 + slack) << "the tail after the last note stays below the next count";
    }
    r.verifyCounters(0, ms, 0, 0, "leg A");
    // ---- leg B: by time (a short run, then a 60 ms pause) ----
    const int64_t startB = r.batches()[r.batches().size() - 12].base;
    const std::size_t wb = r.addWorker(true, 2'000'000);
    r.installSchema(wb, 2);
    Ms mb = r.subscribeMembers(wb, startB, 10);
    ASSERT_TRUE(r.converge(mb));
    r.settle(60ms);
    for (const auto& m : mb) {
        r.verifyMember(m, "leg B");
        if (m.kind == MK::Unfiltered) continue;
        const MemberView& v = r.view(m.sub);
        EXPECT_TRUE(v.dataFrames.empty());
        ASSERT_FALSE(v.notes.empty()) << "the pause needs a note by time";
        EXPECT_EQ(v.notes.back(), r.logEnd()) << "the final note is the log end";
    }
    r.verifyCounters(wb, mb, 0, 0, "leg B");
    r.verifyNoAllocation("quiet run");
    EXPECT_GT(r.hotPasses(), 0u);
    (void)e;
}

void runTxn(Rig& r, Entity e, Timing t) {
    // The read_committed state of a partition is built from its log when the worker takes it over, so the whole
    // transactional history is in the log before the worker exists.  The first batch (the largest) is also the
    // warm-up: the members rebuild it in the first, unmeasured phase.
    std::vector<PlayerRow> big;
    for (int i = 0; i < 120; ++i) big.push_back(i % 2 == 0 ? rowM() : rowN());
    const int64_t b0 = r.append(big);
    r.append({rowM(), rowN()});                              // plain committed data
    const int64_t lso = r.appendTxn(7, {rowM(), rowN(), rowM()});   // lso .. lso+2 transactional
    const int64_t lastTxn = lso + 2;
    r.append({rowM(), rowN()});                              // plain, behind the transaction
    r.addWorker();
    r.installSchema(0, 2);
    Ms ms = r.subscribeMembers(0, b0);
    ASSERT_TRUE(r.converge(ms));
    r.verifyAll(ms, "txn open");
    const Outcome out = expect(r.op(), e, t);
    for (const auto& m : filtered(ms)) {
        const MemberView& v = r.view(m.sub);
        if (t == Timing::OpenTxn) {
            if (out == Outcome::WithheldUntilCommit) {
                for (const int64_t o : v.delivered) EXPECT_LT(o, lso) << "nothing past the LSO under read_committed";
                EXPECT_LE(v.pos, lso);
            } else {
                EXPECT_EQ(out, Outcome::Delivered);
                EXPECT_NE(std::find(v.delivered.begin(), v.delivered.end(), lastTxn), v.delivered.end())
                    << "read_uncommitted sees the open transaction's match";
            }
        }
    }
    const bool commit = t != Timing::AbortedTxn;
    r.marker(0, 7, commit);
    ASSERT_TRUE(r.converge(ms));
    r.verifyAll(ms, commit ? "txn committed" : "txn aborted");
    for (const auto& m : filtered(ms)) {
        const MemberView& v = r.view(m.sub);
        const bool has4 = std::find(v.delivered.begin(), v.delivered.end(), lastTxn) != v.delivered.end();
        if (t == Timing::AbortedTxn) {
            if (out == Outcome::NeverDelivered) {
                for (const int64_t o : v.delivered) EXPECT_FALSE(o >= lso && o <= lastTxn) << "an aborted record was delivered: " << o;
            } else {
                EXPECT_EQ(out, Outcome::Delivered);
                EXPECT_TRUE(has4) << "read_uncommitted sees aborted records";
            }
        } else {
            if (t == Timing::CommittedTxn) EXPECT_EQ(out, Outcome::Delivered);
            EXPECT_TRUE(has4) << "committed records are delivered";
        }
    }
    r.append({rowM(), rowM(), rowN()});   // plain data after the marker: the measured wave
    r.append({rowN(), rowM()});
    ASSERT_TRUE(r.converge(ms));
    r.verifyAll(ms, "after the marker");
    finish(r, ms, "txn");
}

void runOwnershipMove(Rig& r, Entity e) {
    (void)e;
    const int64_t b0 = start(r);
    Ms ms = r.subscribeMembers(0, b0);
    for (int i = 0; i < 3; ++i) r.append({rowM(), rowN(), rowW(), rowC()});
    ASSERT_TRUE(r.converge(ms));
    r.verifyAll(ms, "before the move");
    std::map<int64_t, std::size_t> seen;
    for (const auto& m : ms) seen[m.sub] = r.view(m.sub).dataFrames.size();
    r.disown(0);   // the partition leaves worker 0 (a disk partition's subscription stays hosted there, so it is retired)
    for (const auto& m : ms) r.unsubscribe(m);
    r.settle(30ms);
    for (const auto& m : ms) seen[m.sub] = r.view(m.sub).dataFrames.size();
    for (int i = 0; i < 3; ++i) r.append({rowM(), rowP(), rowN(), rowM()});
    r.settle(40ms);
    for (const auto& m : ms) EXPECT_EQ(r.view(m.sub).dataFrames.size(), seen[m.sub]) << "the old owner pushed after the move";
    // the rig re-subscribes on a second worker from the model client's position
    const std::size_t w1 = r.addWorker();
    r.installSchema(w1, 2);
    (void)r.prime(w1, 2);
    Ms ms2;
    std::map<int64_t, std::pair<std::size_t, std::size_t>> mark;   // frames / notes the old owner delivered
    for (const auto& m : ms) {
        mark[m.sub] = {r.view(m.sub).dataFrames.size(), r.view(m.sub).notes.size()};
        ms2.push_back(r.resubscribe(m, w1, r.view(m.sub).pos));
    }
    ASSERT_TRUE(r.converge(ms2));
    r.verifyAll(ms2, "after the move");   // the whole log, exactly once: no duplicate, no gap
    r.append({rowM(), rowC(), rowN()});
    r.append({rowW(), rowM()});
    ASSERT_TRUE(r.converge(ms2));
    r.verifyAll(ms2, "live on the new owner");
    r.verifyCounters(w1, ms2, 0, 0, "new owner", &mark);
    r.verifyNoAllocation("ownership move");
}

void runEviction(Rig& r) {
    const int64_t b0 = start(r, 2, /*primeRows=*/40);
    const std::vector<PlayerRow> rows{rowM(), rowN(), rowW(), rowC(), rowP(), rowN(), rowM(), rowN(), rowC(), rowM(),
                                      rowN(), rowW(), rowM(), rowN(), rowN(), rowC(), rowM(), rowN(), rowW(), rowN()};
    for (int i = 0; i < 3; ++i) r.append(rows);
    Ms ms = r.subscribeMembers(0, b0);
    ASSERT_TRUE(r.converge(ms));
    r.verifyAll(ms, "before the eviction");
    const int64_t cursor = r.logEnd();
    ASSERT_EQ(r.w(0).transientCursorSkipsForTest(), 0u);
    for (int i = 0; i < 20; ++i) r.append(rows);   // the producer outruns the reader (no pass meanwhile)
    const int64_t ringStart = r.mem()->logStartOffset();
    ASSERT_GT(ringStart, cursor) << "precondition: the ring evicted records the reader had not read";
    r.setEvicted(cursor, ringStart);
    ASSERT_TRUE(r.converge(ms));
    r.verifyAll(ms, "after the eviction");
    const uint64_t skips = r.w(0).transientCursorSkipsForTest();
    EXPECT_GE(skips, 1u) << "the E2 eviction counter";
    EXPECT_EQ(r.w(0).transientRecordsSkippedForTest(), skips * static_cast<uint64_t>(ringStart - cursor))
        << "every skip counted the evicted records of its cursor";
    for (const auto& m : filtered(ms)) {
        for (const int64_t o : r.view(m.sub).delivered) EXPECT_FALSE(o >= cursor && o < ringStart) << "evicted record delivered: " << o;
    }
    for (int i = 0; i < 2; ++i) r.append(rows);   // later matches are delivered
    ASSERT_TRUE(r.converge(ms));
    r.verifyAll(ms, "later matches");
    for (int i = 0; i < 2; ++i) r.append(rows);
    ASSERT_TRUE(r.converge(ms));
    r.verifyAll(ms, "measured wave");
    finish(r, ms, "eviction");
}

void runAlterAdd(Rig& r) {
    const int64_t b0 = start(r, 1);
    Ms ms = r.subscribeMembers(0, b0);
    r.append({rowM(), rowN(), rowW(), rowC()}, 1);
    r.append({rowC(), rowM(), rowN(), rowN()}, 1);
    ASSERT_TRUE(r.converge(ms));
    r.verifyAll(ms, "v1");
    r.installSchema(0, 2);   // ALTER ADD guild
    for (int wave = 0; wave < 2; ++wave) {
        r.append({rowM(), rowP(), rowN(), rowW()}, 2);
        r.append({rowN(), rowM(), rowC(), rowN()}, 1);   // a lagging v1 producer interleaves
        r.append({rowW(), rowM(), rowN(), rowM()}, 2);
        ASSERT_TRUE(r.converge(ms));
        r.verifyAll(ms, "v1 and v2 interleaved");
    }
    finish(r, ms, "alter add");
}

void runAlterDrop(Rig& r, Entity e) {
    const int64_t b0 = start(r, 2);
    Ms ms = r.subscribeMembers(0, b0);
    r.append({rowM(), rowN(), rowW(), rowC()}, 2);
    r.append({rowM(), rowM(), rowN(), rowP()}, 2);
    ASSERT_TRUE(r.converge(ms));
    r.verifyAll(ms, "v2");
    r.installSchema(0, 3);   // ALTER DROP channel
    for (int wave = 0; wave < 2; ++wave) {
        r.append({rowM(), rowN(), rowW(), rowC()}, 3);
        r.append({rowP(), rowM(), rowN(), rowW()}, 3);
        ASSERT_TRUE(r.converge(ms));
        r.verifyAll(ms, "v3");
    }
    uint64_t v3 = 0, v3World = 0;
    for (const auto& rec : r.recs()) if (rec.ver == 3) { ++v3; v3World += rec.row.worldId == kWorld; }
    const Outcome out = expect(r.op(), e, Timing::AlterDropFilteredField);
    uint64_t absent = 0;
    for (const auto& m : filtered(ms)) {
        const MemberView& v = r.view(m.sub);
        std::size_t v3Delivered = 0;
        for (const int64_t o : v.delivered) v3Delivered += r.recs()[static_cast<std::size_t>(o)].ver == 3;
        if (out == Outcome::AbsentExcluded) {
            EXPECT_EQ(v3Delivered, 0u) << "no v3 record may be delivered by a channel filter";
            absent = (m.kind == MK::ChannelIn) ? v3 : v3World;
        } else {
            EXPECT_EQ(out, Outcome::Delivered);
            EXPECT_GT(v3Delivered, 0u) << "the filtered field survives the drop: v3 matches are delivered";
        }
    }
    finish(r, ms, "alter drop", absent, 0);
}

void runVersionLate(Rig& r) {
    // ---- leg "never": the schema never arrives; the 50 ms wait expires ----
    const int64_t b0 = start(r, 1);
    r.append({rowM(), rowN()}, 1);   // @b0,b0+1
    Ms never = r.subscribeMembers(0, b0);
    ASSERT_TRUE(awaitInSync(r, never));
    ASSERT_TRUE(r.converge(never));
    r.verifyAll(never, "never: v1");
    r.skipVersion(2);   // the worker never learns v2: its records are excluded
    r.append({rowM()}, 2);   // @b0+2, a v2 record
    r.settle(20ms);
    for (const auto& m : filtered(never)) EXPECT_LE(r.view(m.sub).pos, b0 + 2) << "stalled behind the unknown version";
    r.settle(150ms);   // past the 50 ms wait
    r.append({rowM()}, 1);   // @b0+3
    ASSERT_TRUE(r.converge(never));
    r.verifyAll(never, "never: expired");
    r.verifyCounters(0, never, 0, /*unknown=*/1, "never leg");
    for (const auto& m : never) r.unsubscribe(m);
    r.settle(20ms);
    // ---- leg "late": the schema arrives 20 ms after the record ----
    r.clearSkipVersions();
    const std::size_t wl = r.addWorker();
    r.installSchema(wl, 1);
    Ms late = r.subscribeMembers(wl, r.logEnd(), 10);
    ASSERT_TRUE(awaitInSync(r, late));
    ASSERT_TRUE(r.converge(late));
    r.append({rowM()}, 2);   // the v2 record, before its schema
    r.settle(15ms);
    for (const auto& m : filtered(late)) {
        EXPECT_TRUE(r.view(m.sub).delivered.empty()) << "a record of an unknown version must wait";
        EXPECT_LE(r.view(m.sub).pos, r.logEnd() - 1);
    }
    r.installSchema(wl, 2);   // the FilterSchema op, 20 ms late
    ASSERT_TRUE(r.converge(late));
    r.verifyAll(late, "late: delivered");
    r.append({rowM()}, 1);
    r.append({rowM()}, 1);
    ASSERT_TRUE(r.converge(late));
    r.verifyAll(late, "late: after");
    for (const auto& m : filtered(late)) EXPECT_EQ(r.view(m.sub).delivered.size(), 3u) << "once";
    r.verifyCounters(wl, late, 0, 0, "late leg");
    r.verifyNoAllocation("version late");
}

void runBackPressure(Rig& r) {
    const int64_t b0 = start(r);
    Ms ms = r.subscribeMembers(0, b0);
    r.append(rep(rowM(), 60));
    ASSERT_TRUE(r.converge(ms));
    ASSERT_TRUE(awaitInSync(r, ms));
    r.verifyAll(ms, "before");
    for (int i = 0; i < 80; ++i) r.append(rep(rowM(), 60));
    for (int i = 0; i < 12; ++i) { r.stepUnpolled(); std::this_thread::sleep_for(200us); }   // the egress refuses
    const bool isFiltered = r.op() != Op::Unfiltered;
    if (isFiltered) EXPECT_GE(r.w(0).filterCountersForTest().fellBehind, 1u) << "the egress never refused (precondition)";
    ASSERT_TRUE(r.converge(ms, 60s));
    r.verifyAll(ms, "after the refusals");   // no loss, no duplicate; unfiltered neighbours complete and byte-identical
    if (isFiltered) {
        const auto c = r.w(0).filterCountersForTest();
        EXPECT_GE(c.rejoined, 1u);
        EXPECT_EQ(c.rejoined, c.fellBehind);
        ASSERT_TRUE(awaitInSync(r, ms));
        for (const auto& m : filtered(ms)) EXPECT_TRUE(r.inSync(m)) << "InSync again";
    }
    for (int i = 0; i < 4; ++i) r.append(rep(rowM(), 60));
    ASSERT_TRUE(r.converge(ms));
    r.verifyAll(ms, "measured");
    r.verifyCounters(0, ms, 0, 0, "back pressure");
    r.verifyNoAllocation("back pressure");
}

// ---------------------------------------------------------------------------------------------------------------
using CellParam = std::tuple<Op, Entity, Timing>;
class WorkerFilterMatrixTest : public ::testing::TestWithParam<CellParam> {};

TEST_P(WorkerFilterMatrixTest, Cell) {
    const auto [op, ent, t] = GetParam();
    if (const auto na = naRule(op, ent, t)) GTEST_SKIP() << "NA: " << name(*na);
    RigConfig cfg;
    if (t == Timing::TransientEviction) cfg.ringBytes = 12 * 1024;
    Rig rig(op, ent, cfg);
    switch (t) {
    case Timing::SteadyAllMatch: runSteady(rig, ent, t, {rep(rowM(), 4), rep(rowM(), 3), rep(rowM(), 5)}); break;
    case Timing::SteadyPartial:
        runSteady(rig, ent, t, {{rowM(), rowN(), rowM()}, {rowN(), rowM(), rowN(), rowN()}, {rowW(), rowC(), rowM(), rowN()},
                                {rowM(), rowM(), rowN()}});
        break;
    case Timing::SteadyNoMatch:
        runSteady(rig, ent, t, {rep(rowN(), 3), rep(rowN(), 5), rep(rowN(), 2)});
        break;
    case Timing::StraddlingResume: runStraddling(rig, ent); break;
    case Timing::QuietRun: runQuietRun(rig, ent); break;
    case Timing::OpenTxn:
    case Timing::AbortedTxn:
    case Timing::CommittedTxn: runTxn(rig, ent, t); break;
    case Timing::OwnershipMove: runOwnershipMove(rig, ent); break;
    case Timing::TransientEviction: runEviction(rig); break;
    case Timing::AlterAddField: runAlterAdd(rig); break;
    case Timing::AlterDropFilteredField: runAlterDrop(rig, ent); break;
    case Timing::VersionArrivesLate: runVersionLate(rig); break;
    case Timing::BackPressure: runBackPressure(rig); break;
    }
}

std::vector<CellParam> allCells() {
    std::vector<CellParam> v;
    for (const Op o : kOps) for (const Entity e : kEntities) for (const Timing t : kTimings) v.emplace_back(o, e, t);
    return v;
}

INSTANTIATE_TEST_SUITE_P(FilterMatrixRegression, WorkerFilterMatrixTest, ::testing::ValuesIn(allCells()),
                         [](const ::testing::TestParamInfo<CellParam>& i) {
                             return std::string(name(std::get<0>(i.param))) + "_" + name(std::get<1>(i.param)) + "_" +
                                    name(std::get<2>(i.param));
                         });

// ---------------------------------------------------------------------------------------------------------------
TEST(FilterMatrixCountRegression, CellCountsArePinned) {
    int total = 0, applicable = 0, na1 = 0, na2 = 0, r1 = 0, r11 = 0;
    std::map<int, int> byRule;   // timing index -> applicable filtered cells
    for (const Op o : kOps)
        for (const Entity e : kEntities)
            for (const Timing t : kTimings) {
                ++total;
                const auto na = naRule(o, e, t);
                if (na) { (*na == NaRule::NA1 ? na1 : na2)++; continue; }
                ++applicable;
                if (o == Op::Unfiltered) { ++r1; continue; }
                if (t == Timing::TransientEviction) ++r11;
                ++byRule[static_cast<int>(t)];
            }
    EXPECT_EQ(total, 280);
    EXPECT_EQ(applicable, 232);
    EXPECT_EQ(na1, 40);
    EXPECT_EQ(na2, 8);
    EXPECT_EQ(r1, 16) << "R1: 4 entities x 4 timings";
    EXPECT_EQ(r11, 8) << "R11: 4 filtered ops x 2 transient entities";
    for (const Timing t : kTimings) {   // every other rule: 4 filtered ops x 4 entities
        if (t == Timing::TransientEviction) continue;
        EXPECT_EQ(byRule[static_cast<int>(t)], 16) << name(t);
    }
    // the outcome table is total over the applicable cells
    for (const Op o : kOps) for (const Entity e : kEntities) for (const Timing t : kTimings)
        if (!naRule(o, e, t) && o != Op::Unfiltered) EXPECT_NE(expect(o, e, t), Outcome::ByteIdentical);
}

}  // namespace
