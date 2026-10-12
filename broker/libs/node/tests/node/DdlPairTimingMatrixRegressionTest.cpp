// DdlPairTimingMatrixRegressionTest.cpp — DDL conformance plan Task 9.
//
// A then B, for every create kind of A (and AlterAdd), 7 kinds of B, Same/Different
// name, and the 9 timing classes T1..T7.  B's expectation = the Occupant table row
// for the state of its name WHEN B EXECUTES (B is serialized behind A), except that
// a read (GetSchema) and a CAS version are taken at SUBMIT time.  Concurrent timings
// (T2-T4) are also checked for timing-invariance against a sequential T1 twin.
#include <gtest/gtest.h>
#include "support/DdlHarness.h"

#include <ostream>
#include <string>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;

namespace {

enum class Timing : uint8_t {
    T1_Sequential, T2_BothQueued, T3_DuringAwaitingId, T4_DuringAwaitingCommit,
    T5a_ReplyFailsNoCreate, T5b_ReplyFailsAfterCreate, T5c_TimeoutInAwaitingId,
    T6_DemotedInAwaitingId, T7_RestartBetween,
};
const char* timingName(Timing t) {
    switch (t) {
        case Timing::T1_Sequential:             return "T1_Sequential";
        case Timing::T2_BothQueued:             return "T2_BothQueued";
        case Timing::T3_DuringAwaitingId:       return "T3_DuringAwaitingId";
        case Timing::T4_DuringAwaitingCommit:   return "T4_DuringAwaitingCommit";
        case Timing::T5a_ReplyFailsNoCreate:    return "T5a_ReplyFailsNoCreate";
        case Timing::T5b_ReplyFailsAfterCreate: return "T5b_ReplyFailsAfterCreate";
        case Timing::T5c_TimeoutInAwaitingId:   return "T5c_TimeoutInAwaitingId";
        case Timing::T6_DemotedInAwaitingId:    return "T6_DemotedInAwaitingId";
        case Timing::T7_RestartBetween:         return "T7_RestartBetween";
    }
    return "?";
}
enum class Rel : uint8_t { Same, Different };

struct PairCase { Op a; Op b; Rel rel; Timing t; };
void PrintTo(const PairCase& c, std::ostream* os) {
    *os << opName(c.a) << " then " << opName(c.b) << (c.rel == Rel::Same ? " same " : " different ")
        << timingName(c.t);
}

bool aFails(Timing t) { return t >= Timing::T5a_ReplyFailsNoCreate && t <= Timing::T6_DemotedInAwaitingId; }
bool aLeavesOrphan(Timing t) {
    return t == Timing::T5b_ReplyFailsAfterCreate || t == Timing::T5c_TimeoutInAwaitingId ||
           t == Timing::T6_DemotedInAwaitingId;
}
bool concurrent(Timing t) {
    return t == Timing::T2_BothQueued || t == Timing::T3_DuringAwaitingId || t == Timing::T4_DuringAwaitingCommit;
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
Occupant occupantAfterA(const PairCase& c) {
    if (!aFails(c.t)) return committedKind(c.a);
    return aLeavesOrphan(c.t) ? orphanOf(c.a) : Occupant::Free;
}

Cell expectedB(const PairCase& c) {
    const Occupant o = c.rel == Rel::Same ? occupantAfterA(c)
                     : (isCreate(c.b) ? Occupant::Free : Occupant::TypedDisk);
    Cell cell = expectedCell(o, c.b);
    if (c.rel == Rel::Same && concurrent(c.t)) {
        // KL3: a read is answered at SUBMIT time from the committed state (before A).
        if (c.b == Op::GetSchema)
            cell = expectedCell(c.a == Op::AlterAdd ? Occupant::TypedDisk : Occupant::Free, Op::GetSchema);
        // CAS read v1 at submit time; A's ALTER commits v2 first: lost-update protection.
        if (c.b == Op::AlterCasCurrent && c.a == Op::AlterAdd) cell = Cell{kSchemaConflict, 0};
    }
    return cell;
}

uint16_t expectedBVersion(const PairCase& c) {
    const bool aAlter = c.a == Op::AlterAdd;
    switch (c.b) {
        case Op::DefineDisk: case Op::DefineTransient:
        case Op::ViewPersisted: case Op::ViewTransient:
            return 1;
        case Op::AlterAdd: case Op::AlterCasCurrent:
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
        case Occupant::SchemaLessDisk:
        case Occupant::OrphanSink:
        case Occupant::SchemaLessTransient:
            ASSERT_NE(e, nullptr) << name;
            EXPECT_EQ(e->kind, EntryKind::SchemaLess) << name;
            EXPECT_EQ(e->persistence, o == Occupant::SchemaLessTransient ? PersistenceMode::Transient : PersistenceMode::Disk);
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
        case Timing::T3_DuringAwaitingId: {
            h.setControl(ControlMode::Hold);
            r.a = h.submit(c.a, an);
            EXPECT_TRUE(h.tickUntilCreatePending());
            r.b = h.submit(c.b, bn);
            const int reqs = h.createRequests();
            for (int i = 0; i < 5; ++i) h.tick();
            if (c.b != Op::GetSchema) EXPECT_FALSE(h.response(r.b).has_value()) << "B ran while A was AwaitingId";
            EXPECT_EQ(h.createRequests(), reqs) << "B requested a topic while A was in flight";
            h.setControl(ControlMode::Auto);
            h.deliverCreate();
            h.settle();
            break;
        }
        case Timing::T4_DuringAwaitingCommit: {
            r.a = h.submit(c.a, an);
            EXPECT_TRUE(h.tickUntilAppended());
            r.b = h.submit(c.b, bn);
            const int reqs = h.createRequests();
            for (int i = 0; i < 5; ++i) h.tick();
            EXPECT_FALSE(h.response(r.a).has_value()) << "A answered before its record committed";
            if (c.b != Op::GetSchema) EXPECT_FALSE(h.response(r.b).has_value()) << "B ran while A was AwaitingCommit";
            EXPECT_EQ(h.createRequests(), reqs);
            h.settle();   // releases the held commits
            break;
        }
        case Timing::T5a_ReplyFailsNoCreate:
        case Timing::T5b_ReplyFailsAfterCreate:
            h.setControl(ControlMode::Hold);
            r.a = h.submit(c.a, an);
            EXPECT_TRUE(h.tickUntilCreatePending());
            h.failCreate(/*afterCreate=*/c.t == Timing::T5b_ReplyFailsAfterCreate);
            h.setControl(ControlMode::Auto);
            h.settle();
            r.b = h.submit(c.b, bn);
            h.settle();
            break;
        case Timing::T5c_TimeoutInAwaitingId:
            h.setControl(ControlMode::Hold);
            r.a = h.submit(c.a, an);
            EXPECT_TRUE(h.tickUntilCreatePending());
            h.advance(AdminWorker::ddlTimeoutNs() + 1'000'000);
            h.tick();
            EXPECT_TRUE(h.response(r.a).has_value()) << "A timed out";
            h.deliverCreate();   // the late reply: stale ticket, topic created (orphan)
            h.setControl(ControlMode::Auto);
            h.settle();
            r.b = h.submit(c.b, bn);
            h.settle();
            break;
        case Timing::T6_DemotedInAwaitingId:
            h.setControl(ControlMode::Hold);
            r.a = h.submit(c.a, an);
            EXPECT_TRUE(h.tickUntilCreatePending());
            h.demote();
            EXPECT_TRUE(h.response(r.a).has_value()) << "B4: answered at demotion";
            h.deliverCreate();   // the late reply: stale ticket, topic created (orphan)
            h.promote();
            h.setControl(ControlMode::Auto);
            h.settle();
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
                     Op::AlterAdd, Op::AlterCasCurrent, Op::GetSchema};
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
    o.holdCommits = c.t == Timing::T4_DuringAwaitingCommit;
    DdlHarness h(o);
    ASSERT_TRUE(h.violations().empty()) << h.violationText();
    const int reqs0 = h.createRequests();
    const Catalog cat0 = h.catalog();
    const int64_t schemaEnd0 = h.schemaLogEnd();
    const int64_t viewsEnd0 = h.viewsLogEnd();
    const int plans0 = h.viewPlanPushes();

    const Ran r = runPair(h, c);
    if (HasFatalFailure()) return;

    const std::optional<Response> ra = h.response(r.a);
    const std::optional<Response> rb = h.response(r.b);
    ASSERT_TRUE(ra.has_value()) << "A answered";
    ASSERT_TRUE(rb.has_value()) << "B answered";
    EXPECT_EQ(ra->err, aFails(c.t) ? kInternal : kOk);
    if (!aFails(c.t)) EXPECT_EQ(ra->version, c.a == Op::AlterAdd ? 2u : 1u);
    const Cell wb = expectedB(c);
    EXPECT_EQ(rb->err, wb.code);
    if (wb.code == kOk) EXPECT_EQ(rb->version, expectedBVersion(c));
    EXPECT_EQ(h.createRequests() - reqs0, (isCreate(c.a) ? 1 : 0) + wb.createRequests)
        << "requestCreateTopic calls (A's + B's)";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();

    // Catalog log ends: exactly one record per COMMITTED DDL op, in the right log —
    // a failed A (T5-T6) and a rejected B append nothing (no half-state).
    const bool aOk = !aFails(c.t);
    const bool bWrote = wb.code == kOk && c.b != Op::GetSchema;
    const auto schemaRecs = [](Op op) { return isDefine(op) || isAlter(op) ? 1 : 0; };
    const auto viewRecs = [](Op op) { return isView(op) ? 1 : 0; };
    EXPECT_EQ(h.schemaLogEnd() - schemaEnd0, (aOk ? schemaRecs(c.a) : 0) + (bWrote ? schemaRecs(c.b) : 0))
        << "TopicSchema records appended (A's + B's)";
    EXPECT_EQ(h.viewsLogEnd() - viewsEnd0, (aOk ? viewRecs(c.a) : 0) + (bWrote ? viewRecs(c.b) : 0))
        << "__views records appended (A's + B's)";
    if (c.t != Timing::T7_RestartBetween)   // a restart re-installs every view (the counter keeps counting)
        EXPECT_EQ(h.viewPlanPushes() - plans0, (aOk ? viewRecs(c.a) : 0) + (bWrote ? viewRecs(c.b) : 0))
            << "worker view installs (A's + B's)";

    // Final registry state of both names.
    const Catalog fin = h.catalog();
    const Occupant afterA = occupantAfterA(c);
    if (c.rel == Rel::Same) {
        expectOccupant(fin, aName(c.a), wb.code == kOk ? occupantAfterSuccess(afterA, c.b) : afterA);
    } else {
        expectOccupant(fin, aName(c.a), afterA);
        const Occupant before = isCreate(c.b) ? Occupant::Free : Occupant::TypedDisk;
        expectOccupant(fin, bName(c), wb.code == kOk ? occupantAfterSuccess(before, c.b) : before);
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
    size_t present = 0;   // the pair's names that are new in the catalog
    if (!findEntry(cat0, aName(c.a)) && findEntry(fin, aName(c.a))) ++present;
    if (c.rel == Rel::Different && !findEntry(cat0, bName(c)) && findEntry(fin, bName(c))) ++present;
    EXPECT_EQ(fin.size(), cat0.size() + present) << "only the pair's own names were added";

    // Timing invariance: a concurrent schedule ends exactly like the sequential one.
    if (concurrent(c.t) && c.b != Op::GetSchema && c.b != Op::AlterCasCurrent) {
        DdlHarness twin;
        PairCase s = c;
        s.t = Timing::T1_Sequential;
        const Ran tr = runPair(twin, s);
        EXPECT_EQ(fin, twin.catalog()) << "timing changed the outcome";
        ASSERT_TRUE(twin.response(tr.b).has_value());
        EXPECT_EQ(rb->err, twin.response(tr.b)->err);
    }

    h.restart();
    EXPECT_EQ(h.catalog(), h.expectedAfterRestart(fin)) << "state after a restart";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

INSTANTIATE_TEST_SUITE_P(Ddl, DdlPairTimingMatrixRegression, ::testing::ValuesIn(pairCases()),
    [](const ::testing::TestParamInfo<PairCase>& i) {
        return std::string(opName(i.param.a)) + "_then_" + opName(i.param.b) + "_" +
               (i.param.rel == Rel::Same ? "Same" : "Different") + "_" + timingName(i.param.t);
    });

} // namespace
