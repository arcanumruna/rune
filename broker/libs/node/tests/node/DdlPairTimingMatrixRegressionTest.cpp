// DdlPairTimingMatrixRegressionTest.cpp — DDL conformance plan Task 9, ported to the
// catalog write path by catalog history Task 10.
//
// A then B, for every create kind of A (and AlterAdd), 11 kinds of B, Same/Different
// name, and the 9 timing classes T1..T7.  B's expectation = the Occupant table row
// for the state of its name WHEN B EXECUTES (B is serialized behind A), except that
// a read (GetSchema), a CAS version and ALTER VIEW's persistence are taken at SUBMIT
// time.  Concurrent timings (T2-T4) are also checked for timing-invariance against a
// sequential T1 twin.
//
// Timing classes on the catalog path (the control-thread AwaitingId state and its
// create-reply faults are gone — Task 9):
//   T1 sequential; T2 both queued before either runs;
//   T3 B submitted behind A's append, before the pass that applies it;
//   T4 B submitted while A waits on a HELD HW (AwaitingCommit);
//   T5a A's append refused (the ISR below minIsr): NotLeader, nothing appended;
//   T5b A's commit times out (NotLeader, F13) and B runs while the leader catches up
//       (B answered the retriable NotLeader); A's event then commits and applies;
//   T5c A's commit times out, then commits and applies; then B;
//   T6  A demoted in AwaitingCommit (NotLeader), re-promoted, its event commits; then B;
//   T7  a restart between A and B.
#include <gtest/gtest.h>
#include "support/DdlHarness.h"

#include <ostream>
#include <string>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;

namespace {

enum class Timing : uint8_t {
    T1_Sequential, T2_BothQueued, T3_BehindAnUnappliedAppend, T4_DuringAwaitingCommit,
    T5a_RefusedBelowMinIsr, T5b_TimeoutThenBWhileCatchingUp, T5c_TimeoutThenCommits,
    T6_DemotedInAwaitingCommit, T7_RestartBetween,
};
const char* timingName(Timing t) {
    switch (t) {
        case Timing::T1_Sequential:                  return "T1_Sequential";
        case Timing::T2_BothQueued:                  return "T2_BothQueued";
        case Timing::T3_BehindAnUnappliedAppend:     return "T3_BehindAnUnappliedAppend";
        case Timing::T4_DuringAwaitingCommit:        return "T4_DuringAwaitingCommit";
        case Timing::T5a_RefusedBelowMinIsr:         return "T5a_RefusedBelowMinIsr";
        case Timing::T5b_TimeoutThenBWhileCatchingUp: return "T5b_TimeoutThenBWhileCatchingUp";
        case Timing::T5c_TimeoutThenCommits:         return "T5c_TimeoutThenCommits";
        case Timing::T6_DemotedInAwaitingCommit:     return "T6_DemotedInAwaitingCommit";
        case Timing::T7_RestartBetween:              return "T7_RestartBetween";
    }
    return "?";
}
enum class Rel : uint8_t { Same, Different };

struct PairCase { Op a; Op b; Rel rel; Timing t; };
void PrintTo(const PairCase& c, std::ostream* os) {
    *os << opName(c.a) << " then " << opName(c.b) << (c.rel == Rel::Same ? " same " : " different ")
        << timingName(c.t);
}

/// A is answered NotLeader (retriable) instead of OK.
bool aAnsweredNotLeader(Timing t) { return t >= Timing::T5a_RefusedBelowMinIsr && t <= Timing::T6_DemotedInAwaitingCommit; }
/// A's event is appended (and, by the end of the schedule, committed and applied).
bool aApplied(Timing t) { return t != Timing::T5a_RefusedBelowMinIsr; }
bool concurrent(Timing t) {
    return t == Timing::T2_BothQueued || t == Timing::T3_BehindAnUnappliedAppend ||
           t == Timing::T4_DuringAwaitingCommit;
}
bool heldHw(Timing t) {
    return t == Timing::T4_DuringAwaitingCommit || t == Timing::T5b_TimeoutThenBWhileCatchingUp ||
           t == Timing::T5c_TimeoutThenCommits || t == Timing::T6_DemotedInAwaitingCommit;
}

std::string aName(Op a) { return a == Op::AlterAdd ? "src" : "n"; }
std::string bName(const PairCase& c) {
    if (c.rel == Rel::Same) return aName(c.a);
    return isCreate(c.b) ? "m" : "src2";
}

Occupant committedKind(Op a) {
    switch (a) {
        case Op::DefineDisk:      return Occupant::TypedDisk;
        case Op::DefineTransient: return Occupant::TypedTransient;
        case Op::ViewPersisted:   return Occupant::ViewPersisted;
        case Op::ViewTransient:   return Occupant::ViewTransient;
        default:                  return Occupant::TypedDisk;   // AlterAdd on src
    }
}
Occupant occupantAfterA(const PairCase& c) { return aApplied(c.t) ? committedKind(c.a) : Occupant::Free; }
/// B's name as B is SUBMITTED (before A applied, in a concurrent timing).
Occupant occupantAtBSubmit(const PairCase& c) {
    if (c.rel == Rel::Different) return isCreate(c.b) ? Occupant::Free : Occupant::TypedDisk;
    if (concurrent(c.t) || c.t == Timing::T5b_TimeoutThenBWhileCatchingUp)
        return c.a == Op::AlterAdd ? Occupant::TypedDisk : Occupant::Free;
    return occupantAfterA(c);
}

Cell expectedB(const PairCase& c) {
    const Occupant o = c.rel == Rel::Same ? occupantAfterA(c)
                     : (isCreate(c.b) ? Occupant::Free : Occupant::TypedDisk);
    // KL3: a read is answered at SUBMIT time from the applied state.
    if (c.b == Op::GetSchema) return expectedCell(occupantAtBSubmit(c), Op::GetSchema);
    // T5b: B starts while the leader catches up on A's timed-out event.
    if (c.t == Timing::T5b_TimeoutThenBWhileCatchingUp) return Cell{kNotLeader, 0};
    Cell cell = expectedCell(o, c.b);
    // CAS read v1 at submit time; A's ALTER commits v2 first: lost-update protection.
    if (c.rel == Rel::Same && concurrent(c.t) && c.b == Op::AlterCasCurrent && c.a == Op::AlterAdd)
        cell = Cell{kSchemaConflict, 0};
    return cell;
}

/// ALTER VIEW sends the view's persistence as read at SUBMIT time: in a concurrent
/// timing the view does not exist yet, so the request asks for a persisted view.
Occupant occupantAfterB(const PairCase& c, Occupant before) {
    if (c.b == Op::AlterView && concurrent(c.t) && c.rel == Rel::Same) return Occupant::ViewPersisted;
    return occupantAfterSuccess(before, c.b);
}

uint32_t expectedBVersion(const PairCase& c) {
    const bool aAlter = c.a == Op::AlterAdd;
    switch (c.b) {
        case Op::DefineDisk: case Op::DefineTransient:
        case Op::ViewPersisted: case Op::ViewTransient:
            return 1;
        case Op::AlterAdd: case Op::AlterCasCurrent:   // the schema version
        case Op::DropTopic: case Op::DropView: case Op::AlterView: case Op::AlterTopicConfig:   // the object version
            return c.rel == Rel::Different ? 2 : (aAlter ? 3 : 2);
        case Op::GetSchema:
            if (c.rel == Rel::Different) return 1;
            return aAlter ? (concurrent(c.t) ? 1 : 2) : 1;
        default:
            return 0;
    }
}

void expectOccupant(const Catalog& cat, const std::string& name, Occupant o) {
    const CatalogEntry* e = findEntry(cat, name);
    switch (o) {
        case Occupant::Free:
            EXPECT_EQ(e, nullptr) << "'" << name << "' must not exist";
            return;
        case Occupant::TypedDisk:
        case Occupant::TypedTransient:
            ASSERT_NE(e, nullptr) << name;
            EXPECT_EQ(e->kind, EntryKind::Typed) << name;
            EXPECT_EQ(e->persistence, o == Occupant::TypedTransient ? PersistenceMode::Transient : PersistenceMode::Disk);
            return;
        case Occupant::ViewPersisted:
        case Occupant::ViewTransient:
            ASSERT_NE(e, nullptr) << name;
            EXPECT_EQ(e->kind, EntryKind::View) << name;
            EXPECT_EQ(e->persistence, o == Occupant::ViewTransient ? PersistenceMode::Transient : PersistenceMode::Disk);
            return;
        default:
            ADD_FAILURE() << "unexpected occupant " << occupantName(o);
    }
}

struct Ran { int64_t a = 0; int64_t b = 0; };

Ran runPair(DdlHarness& h, const PairCase& c) {
    Ran r;
    const std::string an = aName(c.a);
    const std::string bn = bName(c);
    switch (c.t) {
        case Timing::T1_Sequential:
            r.a = h.submit(c.a, an); h.settle();
            r.b = h.submit(c.b, bn); h.settle();
            break;
        case Timing::T2_BothQueued:
            r.a = h.submit(c.a, an);
            r.b = h.submit(c.b, bn);
            h.settle();
            break;
        case Timing::T3_BehindAnUnappliedAppend: {
            r.a = h.submit(c.a, an);
            EXPECT_TRUE(h.tickUntilAppended());
            EXPECT_FALSE(h.response(r.a).has_value()) << "A is answered only once its event is applied";
            const uint64_t events = h.eventsAppended();
            r.b = h.submit(c.b, bn);
            EXPECT_EQ(h.eventsAppended(), events) << "B appended while A was in flight";
            h.settle();
            break;
        }
        case Timing::T4_DuringAwaitingCommit: {
            r.a = h.submit(c.a, an);
            EXPECT_TRUE(h.tickUntilAppended());
            r.b = h.submit(c.b, bn);
            const uint64_t events = h.eventsAppended();
            for (int i = 0; i < 5; ++i) h.tick();
            EXPECT_FALSE(h.response(r.a).has_value()) << "A answered before its event committed";
            if (c.b != Op::GetSchema) EXPECT_FALSE(h.response(r.b).has_value()) << "B ran while A was AwaitingCommit";
            EXPECT_EQ(h.eventsAppended(), events) << "B appended while A was in flight";
            h.settle();   // releases the held commit
            break;
        }
        case Timing::T5a_RefusedBelowMinIsr:
            h.shrinkIsr();
            r.a = h.submit(c.a, an);
            h.settle();
            EXPECT_TRUE(h.response(r.a).has_value()) << "A refused at its append";
            h.restoreIsr();
            h.settle();
            r.b = h.submit(c.b, bn);
            h.settle();
            break;
        case Timing::T5b_TimeoutThenBWhileCatchingUp:
            r.a = h.submit(c.a, an);
            EXPECT_TRUE(h.tickUntilAppended());
            h.advance(AdminWorker::ddlTimeoutNs() + 1'000'000);
            h.tick();
            EXPECT_TRUE(h.response(r.a).has_value()) << "A timed out";
            EXPECT_EQ(h.worker().roleName(), "LeaderCatchingUp") << "its event is not applied yet";
            r.b = h.submit(c.b, bn);
            h.tick();
            EXPECT_TRUE(h.response(r.b).has_value()) << "B answered while the leader catches up";
            h.settle();   // A's event commits and applies; the leader serves again
            break;
        case Timing::T5c_TimeoutThenCommits:
            r.a = h.submit(c.a, an);
            EXPECT_TRUE(h.tickUntilAppended());
            h.advance(AdminWorker::ddlTimeoutNs() + 1'000'000);
            h.tick();
            EXPECT_TRUE(h.response(r.a).has_value()) << "A timed out";
            h.settle();
            r.b = h.submit(c.b, bn);
            h.settle();
            break;
        case Timing::T6_DemotedInAwaitingCommit:
            r.a = h.submit(c.a, an);
            EXPECT_TRUE(h.tickUntilAppended());
            h.demote();
            EXPECT_TRUE(h.response(r.a).has_value()) << "answered at the demotion (spec §5.2)";
            for (int i = 0; i < 5; ++i) h.tick();
            h.promote();
            h.settle();   // re-promoted: the event commits under the new epoch and applies
            r.b = h.submit(c.b, bn);
            h.settle();
            break;
        case Timing::T7_RestartBetween:
            r.a = h.submit(c.a, an); h.settle();
            h.restart();
            r.b = h.submit(c.b, bn); h.settle();
            break;
    }
    return r;
}

std::vector<PairCase> pairCases() {
    const Op as[] = {Op::DefineDisk, Op::DefineTransient, Op::ViewPersisted, Op::ViewTransient};
    const Op bs[] = {Op::DefineDisk, Op::DefineTransient, Op::ViewPersisted, Op::ViewTransient,
                     Op::AlterAdd, Op::AlterCasCurrent, Op::GetSchema,
                     Op::DropTopic, Op::DropView, Op::AlterView, Op::AlterTopicConfig};
    std::vector<PairCase> v;
    for (Op a : as)
        for (Op b : bs)
            for (Rel rel : {Rel::Same, Rel::Different})
                for (int t = 0; t <= static_cast<int>(Timing::T7_RestartBetween); ++t)
                    v.push_back({a, b, rel, static_cast<Timing>(t)});
    for (Op b : bs)
        for (Rel rel : {Rel::Same, Rel::Different})
            for (Timing t : {Timing::T1_Sequential, Timing::T2_BothQueued, Timing::T4_DuringAwaitingCommit,
                             Timing::T7_RestartBetween})
                v.push_back({Op::AlterAdd, b, rel, t});
    return v;
}

class DdlPairTimingMatrixRegression : public ::testing::TestWithParam<PairCase> {};

TEST_P(DdlPairTimingMatrixRegression, AThenB) {
    const PairCase& c = GetParam();
    DdlHarnessOptions o;
    o.holdCommits = heldHw(c.t);
    DdlHarness h(o);
    ASSERT_TRUE(h.violations().empty()) << h.violationText();
    const uint64_t events0 = h.eventsAppended();
    const Catalog cat0 = h.catalog();
    const int64_t logEnd0 = h.catalogLogEnd();
    const int plans0 = h.viewPlanPushes();
    const int recovery0 = h.recoveryViewInstalls();

    const Ran r = runPair(h, c);
    if (HasFatalFailure()) return;

    const std::optional<Response> ra = h.response(r.a);
    const std::optional<Response> rb = h.response(r.b);
    ASSERT_TRUE(ra.has_value()) << "A answered";
    ASSERT_TRUE(rb.has_value()) << "B answered";
    EXPECT_EQ(ra->err, aAnsweredNotLeader(c.t) ? kNotLeader : kOk);
    if (!aAnsweredNotLeader(c.t)) EXPECT_EQ(ra->version, c.a == Op::AlterAdd ? 2u : 1u);
    const Cell wb = expectedB(c);
    EXPECT_EQ(rb->err, wb.code);
    if (wb.code == kOk) EXPECT_EQ(rb->version, expectedBVersion(c));
    const uint8_t aEvents = aApplied(c.t) ? 1 : 0;
    EXPECT_EQ(h.eventsAppended() - events0, aEvents + wb.events) << "__catalog events appended (A's + B's)";
    EXPECT_EQ(h.catalogLogEnd() - logEnd0, aEvents + wb.events) << "one event per accepted op: no half-state";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();

    // Live view installs: A's view once applied, B's CREATE / ALTER VIEW.  A restart
    // re-installs every live view through the boot replay (T7: A's view); a
    // leadership flap re-installs nothing — every broker applies each event once.
    const auto installs = [](Op op) { return isView(op) || op == Op::AlterView ? 1 : 0; };
    const bool bWrote = wb.code == kOk && c.b != Op::GetSchema;
    EXPECT_EQ(h.viewPlanPushes() - plans0, (aApplied(c.t) && isView(c.a) ? 1 : 0) + (bWrote ? installs(c.b) : 0))
        << "worker view installs (A's + B's)";
    EXPECT_EQ(h.recoveryViewInstalls() - recovery0,
              c.t == Timing::T7_RestartBetween && isView(c.a) ? 1 : 0)
        << "recovery view installs";

    // Final state of both names.
    const Catalog fin = h.catalog();
    const Occupant afterA = occupantAfterA(c);
    if (c.rel == Rel::Same) {
        expectOccupant(fin, aName(c.a), wb.code == kOk ? occupantAfterB(c, afterA) : afterA);
    } else {
        expectOccupant(fin, aName(c.a), afterA);
        const Occupant before = isCreate(c.b) ? Occupant::Free : Occupant::TypedDisk;
        expectOccupant(fin, bName(c), wb.code == kOk ? occupantAfterB(c, before) : before);
    }
    // Versions: A's committed version, then B's ALTER (if any) on top of it.
    if (c.rel == Rel::Same && wb.code == kOk && isAlter(c.b))
        if (const CatalogEntry* e = findEntry(fin, bName(c))) EXPECT_EQ(e->version, expectedBVersion(c));
    // Every name the pair did not touch is exactly as it was (no stray entry).
    for (const CatalogEntry& e : cat0) {
        if (e.name == aName(c.a) || e.name == bName(c)) continue;
        const CatalogEntry* x = findEntry(fin, e.name);
        ASSERT_NE(x, nullptr) << e.name;
        EXPECT_EQ(*x, e) << "every other entry is untouched";
    }
    std::ptrdiff_t delta = 0;   // the pair's names that are new (+1) or dropped (-1)
    if (!findEntry(cat0, aName(c.a)) && findEntry(fin, aName(c.a))) ++delta;
    if (findEntry(cat0, aName(c.a)) && !findEntry(fin, aName(c.a))) --delta;
    if (c.rel == Rel::Different) {
        if (!findEntry(cat0, bName(c)) && findEntry(fin, bName(c))) ++delta;
        if (findEntry(cat0, bName(c)) && !findEntry(fin, bName(c))) --delta;
    }
    EXPECT_EQ(static_cast<std::ptrdiff_t>(fin.size()), static_cast<std::ptrdiff_t>(cat0.size()) + delta)
        << "only the pair's own names were added or dropped";

    // Timing invariance: a concurrent schedule ends exactly like the sequential one
    // (the submit-time reads — GetSchema, a CAS, ALTER VIEW's persistence — excepted).
    if (concurrent(c.t) && c.b != Op::GetSchema && c.b != Op::AlterCasCurrent && c.b != Op::AlterView) {
        DdlHarness twin;
        PairCase s = c;
        s.t = Timing::T1_Sequential;
        const Ran tr = runPair(twin, s);
        EXPECT_EQ(fin, twin.catalog()) << "timing changed the outcome";
        ASSERT_TRUE(twin.response(tr.b).has_value());
        EXPECT_EQ(rb->err, twin.response(tr.b)->err);
    }

    h.restart();
    EXPECT_EQ(h.catalog(), fin) << "state after a restart";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

INSTANTIATE_TEST_SUITE_P(Ddl, DdlPairTimingMatrixRegression, ::testing::ValuesIn(pairCases()),
    [](const ::testing::TestParamInfo<PairCase>& i) {
        return std::string(opName(i.param.a)) + "_then_" + opName(i.param.b) + "_" +
               (i.param.rel == Rel::Same ? "Same" : "Different") + "_" + timingName(i.param.t);
    });

} // namespace
