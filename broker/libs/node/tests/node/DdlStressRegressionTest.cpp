// DdlStressRegressionTest.cpp — DDL conformance plan Task 10.
//
// Deterministic, seeded, model-checked: 3 simulated sessions submit random DDL over
// a small colliding name pool (incl. a case variant, a reserved and an empty name)
// while the scheduler delivers control replies (ok / fail / fail-after-create /
// timeout / demotion) and releases commits in random interleavings, with frequent
// restarts (>= 5% of steps; each drains to quiescence first).  The broker is
// multi-node throughout (replica set {1,2}, also after a restart): commits can be
// held and leadership can flap in every boot.  CREATE VIEW reads 1 or 2 sources
// drawn from typed disk / typed transient topics, view outputs, schema-less and
// missing names.  Every answer is checked against a reference model — name ->
// Occupant (kind + persistence) + version — that applies ops in FIFO submission
// order using ONLY the expectation table and the plan's view-source rules.  Worker
// view installs (commit and recovery) are checked against the model.  The final
// catalog is checked against the model before and after a restart.
//
// Zero-alloc gate: the admin thread's steady-state loop — AdminWorker::doWork while
// idle, while parked in AwaitingId, and while an op waits on a held HW gate — must
// not allocate.  (DDL processing itself is the cold path and may.)  Own executable:
// replaces global operator new.
//
// On the first failure of a seed the run stops and prints the seed, the replay
// command, the event trace up to the failure (the reproducing prefix) and its
// minimal slice (the events on the failing name + leadership flaps + restarts).
#include <gtest/gtest.h>
#include "support/DdlHarness.h"
#include <rune/node/TopicName.h>

#include <cstdio>
#include <cstdlib>
#include <map>
#include <new>
#include <random>
#include <string>
#include <vector>

namespace {
bool     gArmed       = false;   // single-threaded test binary: plain globals, no atomics
uint64_t gArmedAllocs = 0;
}
void* operator new(std::size_t n) {
    if (gArmed) ++gArmedAllocs;
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t n) {
    if (gArmed) ++gArmedAllocs;
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

using namespace rune::node;
using namespace rune::node::test;

namespace {

enum class Fault : uint8_t { None, FailNoCreate, FailAfterCreate, TimeoutInAwaitingId, DemoteInAwaitingId };

const char* faultName(Fault f) {
    switch (f) {
        case Fault::None:                return "none";
        case Fault::FailNoCreate:        return "failNoCreate";
        case Fault::FailAfterCreate:     return "failAfterCreate";
        case Fault::TimeoutInAwaitingId: return "timeout";
        case Fault::DemoteInAwaitingId:  return "demote";
    }
    return "?";
}

/// Task 9's timing classes, as the stress schedule produces them.  T1..T4: the
/// worker's state when a DDL op is SUBMITTED (idle / behind queued ops / another op
/// in AwaitingId / another op in AwaitingCommit).  T5a..T6: a create reply acted on
/// with that fault.  T7: an op on a name a DDL op touched in an earlier boot and
/// none since this boot (A, restart, B).
enum Timing : uint8_t { T1, T2, T3, T4, T5a, T5b, T5c, T6, T7, kTimingCount };
const char* const kTimingNames[kTimingCount] = {"T1", "T2", "T3", "T4", "T5a", "T5b", "T5c", "T6", "T7"};
Timing timingOfFault(Fault f) {
    switch (f) {
        case Fault::FailNoCreate:        return T5a;
        case Fault::FailAfterCreate:     return T5b;
        case Fault::TimeoutInAwaitingId: return T5c;
        case Fault::DemoteInAwaitingId:  return T6;
        case Fault::None:                break;
    }
    return kTimingCount;
}

struct Planned   { Op op; std::string name; Fault fault; std::vector<std::string> sources; };
struct Submitted {
    int64_t corr; Op op; std::string name; Fault fault;
    std::vector<std::string> sources;
    uint16_t sourceErr;   // the view-source rules' verdict at SUBMIT time (0 = sources ok)
};

const std::vector<std::string> kPool = {"a", "b", "A", "s", "src", "__views", ""};
// View sources: typed disk (src, src2), typed transient (tsrc), a view's output
// (vsrc), schema-less static (s), a never-defined name (nosuch), and pool names
// whose kind changes as the schedule runs (typed / view / orphan / case variant).
const std::vector<std::string> kSources = {"src", "src2", "tsrc", "vsrc", "s", "nosuch", "a", "b", "A"};
constexpr int kSessions = 3;
constexpr int kOpsPerSession = 40;
constexpr int kMaxSteps = 50'000;
constexpr int kMaxDrainIterations = 10'000;
constexpr uint64_t kSetupDdlOps = 4;   // src, src2 (harness) + tsrc, vsrc (runSeed) before the schedule

struct Model {
    std::map<std::string, Occupant> occ;       // exact name -> occupant (absent = Free)
    std::map<std::string, uint16_t> version;
    std::vector<std::string>        statics;   // schema-less boot topics survive restarts

    Occupant at(const std::string& n) const {
        if (!isValidTopicName(n)) return Occupant::BadName;
        if (const auto it = occ.find(n); it != occ.end() && it->second != Occupant::Free) return it->second;
        for (const auto& [m, o] : occ)
            if (m != n && o != Occupant::Free && topicNamesFoldEqual(m, n)) return Occupant::CaseVariant;
        return Occupant::Free;
    }
    Occupant exact(const std::string& n) const {
        const auto it = occ.find(n);
        return it == occ.end() ? Occupant::Free : it->second;
    }
    /// The plan's view-source rules, on the sources as they stand at SUBMIT time
    /// (a source is bound by exact name to a topic id then).  Per source, in order:
    /// a view's output -> 41 (D3b/B9; checked first); not a typed topic (missing,
    /// schema-less, orphan, case variant) -> 33; a typed transient topic under a
    /// persisted view -> 37 (Phase 2b E).  Then 2+ sources -> 34: Phase 1 views are
    /// single-source and the compiler rejects more fail-closed.
    uint16_t sourceRule(Op op, const std::vector<std::string>& sources) const {
        for (const std::string& s : sources) {
            const Occupant o = exact(s);
            if (o == Occupant::ViewPersisted || o == Occupant::ViewTransient) return kViewSourceIsView;
            if (o != Occupant::TypedDisk && o != Occupant::TypedTransient) return kViewSourceNotFound;
            if (o == Occupant::TypedTransient && op == Op::ViewPersisted) return kViewSourceTransient;
        }
        return sources.size() > 1 ? kViewFieldUnresolved : kOk;
    }
    /// Apply `s` (FIFO order) and return its expected answer.  The table's
    /// admin-detected rejections (createRequests == 0: 40, 32, 25 on a typed name)
    /// precede the source rules; the source rules precede the sink request, so they
    /// precede every control-detected cell (createRequests == 1) and every fault.
    uint16_t apply(const Submitted& s) {
        const Occupant o = at(s.name);
        const Cell c = expectedCell(o, s.op);
        if (isView(s.op) && c.createRequests == 1 && s.sourceErr != kOk) return s.sourceErr;
        if (c.createRequests == 1 && s.fault != Fault::None) {
            if (s.fault != Fault::FailNoCreate && o == Occupant::Free) occ[s.name] = orphanOf(s.op);
            return kInternal;
        }
        if (c.code == kOk) {
            if (isCreate(s.op)) { occ[s.name] = occupantAfterSuccess(o, s.op); version[s.name] = 1; }
            else if (isAlter(s.op)) version[s.name] = static_cast<uint16_t>(version[s.name] + 1);
        }
        return c.code;
    }
    void restart() {   // an orphan's name is free again (it became __orphan.<id> or vanished)
        for (auto& [n, o] : occ) {
            const bool schemaLess = o == Occupant::SchemaLessDisk || o == Occupant::SchemaLessTransient ||
                                    o == Occupant::OrphanSink;
            bool isStatic = false;
            for (const std::string& s : statics) isStatic = isStatic || s == n;
            if (schemaLess && !isStatic) o = Occupant::Free;
        }
    }
    int views() const {
        int n = 0;
        for (const auto& [name, o] : occ) n += (o == Occupant::ViewPersisted || o == Occupant::ViewTransient) ? 1 : 0;
        return n;
    }
};

Op drawOp(std::mt19937_64& rng) {
    static const Op ops[] = {Op::DefineDisk, Op::DefineTransient, Op::ViewPersisted, Op::ViewTransient,
                             Op::AlterAdd, Op::AlterSetCompat, Op::GetSchema};
    return ops[rng() % (sizeof(ops) / sizeof(ops[0]))];
}
Fault drawFault(std::mt19937_64& rng) {
    const uint64_t r = rng() % 100;
    if (r < 70) return Fault::None;
    return static_cast<Fault>(1 + (r % 4));
}
std::vector<std::string> drawSources(std::mt19937_64& rng) {
    std::vector<std::string> v{kSources[rng() % kSources.size()]};
    if (rng() % 5 == 0) v.push_back(kSources[rng() % kSources.size()]);   // 20%: two sources
    return v;
}
std::string joined(const std::vector<std::string>& v) {
    std::string s;
    for (const std::string& x : v) s += (s.empty() ? "" : ",") + x;
    return s;
}

void expectCatalogMatchesModel(DdlHarness& h, const Model& m, const char* when) {
    const Catalog cat = h.catalog();
    std::vector<std::string> names = kPool;
    for (const char* n : {"src2", "tsrc", "vsrc", "nosuch"}) names.push_back(n);
    for (const std::string& n : names) {
        SCOPED_TRACE(std::string(when) + " name='" + n + "'");
        const CatalogEntry* e = findEntry(cat, n);
        const auto it = m.occ.find(n);
        const Occupant o = (it == m.occ.end() || !isValidTopicName(n)) ? Occupant::Free : it->second;
        switch (o) {
            case Occupant::Free:
                EXPECT_EQ(e, nullptr);
                break;
            case Occupant::TypedDisk:
            case Occupant::TypedTransient:
                ASSERT_NE(e, nullptr);
                EXPECT_EQ(e->kind, EntryKind::Typed);
                EXPECT_EQ(e->persistence, o == Occupant::TypedTransient ? PersistenceMode::Transient : PersistenceMode::Disk);
                EXPECT_EQ(e->version, m.version.at(n));
                break;
            case Occupant::ViewPersisted:
            case Occupant::ViewTransient:
                ASSERT_NE(e, nullptr);
                EXPECT_EQ(e->kind, EntryKind::View);
                EXPECT_EQ(e->persistence, o == Occupant::ViewTransient ? PersistenceMode::Transient : PersistenceMode::Disk);
                break;
            default:   // SchemaLessDisk / SchemaLessTransient / OrphanSink
                ASSERT_NE(e, nullptr);
                EXPECT_EQ(e->kind, EntryKind::SchemaLess);
                EXPECT_EQ(e->persistence, o == Occupant::SchemaLessTransient ? PersistenceMode::Transient : PersistenceMode::Disk);
                break;
        }
    }
}

/// Arm the allocation counter around ticks that must be steady-state.
void expectNoSteadyStateAlloc(DdlHarness& h, const char* what) {
    for (int i = 0; i < 3; ++i) h.tick();   // warm-up: first-use paths may allocate once
    gArmedAllocs = 0;
    gArmed = true;
    for (int i = 0; i < 16; ++i) h.tick();
    gArmed = false;
    EXPECT_EQ(gArmedAllocs, 0u) << "AdminWorker::doWork allocated in steady state (" << what << ")";
}

struct StressStats {
    uint64_t steps = 0, ddl = 0, reads = 0, views = 0, twoSourceViews = 0, restarts = 0, flaps = 0, flapsAfterRestart = 0;
    uint64_t allocGates = 0, heldAfterRestart = 0;   // HW-gate waits in a rebooted (multi-node) broker
    uint64_t faults[5] = {};                // create replies acted on, by Fault
    uint64_t timing[kTimingCount] = {};     // Task 9 timing classes produced
    std::map<uint16_t, uint64_t> codes;     // DDL + read answers, by error code
};
StressStats gTotals;   // summed over the seeds this process ran (reported at the end)

void runSeed(uint64_t seed) {
    SCOPED_TRACE("seed=" + std::to_string(seed) + "  replay: RUNE_DDL_STRESS_SEED=" + std::to_string(seed) +
                 " rune_ddl_stress_tests --gtest_filter=DdlStressEnvRegression.EnvSeed");
    std::mt19937_64 rng(seed);

    DdlHarnessOptions o;
    o.holdCommits = true;   // replica set {1,2} for the whole run, restarts included
    TopicSpec s; s.name = "s"; s.partitionCount = 1;
    o.statics.push_back(s);
    DdlHarness h(o);   // pre-defines src, src2 (typed disk v1)
    for (const auto& [op, name] : {std::pair{Op::DefineTransient, "tsrc"}, std::pair{Op::ViewPersisted, "vsrc"}}) {
        const int64_t c = h.submit(op, name);   // vsrc reads src
        h.settle();
        ASSERT_TRUE(h.response(c).has_value() && h.response(c)->err == kOk) << "setup " << name;
    }
    ASSERT_TRUE(h.violations().empty()) << h.violationText();
    ASSERT_EQ(h.startedTotal(), kSetupDdlOps);
    h.setControl(ControlMode::Hold);
    const int commitInstalls0 = h.viewPlanPushes();

    Model m;
    m.statics = {"s"};
    m.occ["s"] = Occupant::SchemaLessDisk;
    m.occ["src"]  = Occupant::TypedDisk;      m.version["src"]  = 1;
    m.occ["src2"] = Occupant::TypedDisk;      m.version["src2"] = 1;
    m.occ["tsrc"] = Occupant::TypedTransient; m.version["tsrc"] = 1;
    m.occ["vsrc"] = Occupant::ViewPersisted;  m.version["vsrc"] = 1;

    std::vector<std::vector<Planned>> scripts(kSessions);
    for (auto& sc : scripts)
        for (int i = 0; i < kOpsPerSession; ++i) {
            Planned p{drawOp(rng), kPool[rng() % kPool.size()], drawFault(rng), {}};
            if (isView(p.op)) p.sources = drawSources(rng);
            sc.push_back(std::move(p));
        }
    std::vector<int> next(kSessions, 0);

    StressStats st;
    // T7 bookkeeping: names a DDL op touched in an earlier boot / in this boot.
    std::vector<std::string> touchedEarlierBoot, touchedThisBoot;
    auto contains = [](const std::vector<std::string>& v, const std::string& n) {
        for (const std::string& x : v) if (x == n) return true;
        return false;
    };

    // The event trace: every scheduler action, in order (runs of ticks coalesced).
    // On a failure it is printed twice: the whole reproducing prefix, and the
    // minimal slice — the events on the failing name (case-folded: a case variant
    // collides) plus every leadership flap and restart.
    struct Event { std::string text; std::string name; bool global; int ticks; };
    std::vector<Event> trace;
    std::string failName;
    bool failed = false;
    auto note = [&](std::string e, const std::string& name = {}, bool global = false) {
        trace.push_back(Event{std::move(e), name, global, 0});
    };
    auto noteTick = [&] {
        if (trace.empty() || trace.back().ticks == 0) trace.push_back(Event{{}, {}, false, 0});
        ++trace.back().ticks;
    };
    auto render = [](const Event& e) { return e.ticks ? "tick x" + std::to_string(e.ticks) : e.text; };
    auto dumpTrace = [&] {
        std::string t = "full trace (" + std::to_string(trace.size()) + " events):\n";
        for (std::size_t i = 0; i < trace.size(); ++i) t += "  " + std::to_string(i) + ": " + render(trace[i]) + "\n";
        if (failed) {
            t += "minimal trace (events on '" + failName + "' + flaps/restarts):\n";
            for (std::size_t i = 0; i < trace.size(); ++i)
                if (trace[i].global || (!trace[i].ticks && topicNamesFoldEqual(trace[i].name, failName)))
                    t += "  " + std::to_string(i) + ": " + render(trace[i]) + "\n";
        }
        return t;
    };
    auto fail = [&](const std::string& name) { if (!failed) { failed = true; failName = name; } };

    std::vector<Submitted> ddl;   // DDL submissions, FIFO
    std::size_t applied = 0;      // DDL answers folded into the model
    int expectedCommitInstalls = 0;
    auto processAnswers = [&] {
        while (applied < ddl.size()) {
            const Submitted& sub = ddl[applied];
            const std::optional<Response> r = h.response(sub.corr);
            if (!r) break;
            const uint16_t want = m.apply(sub);
            ++st.codes[r->err];
            if (isView(sub.op) && want == kOk) ++expectedCommitInstalls;
            if (r->err != want) fail(sub.name);
            EXPECT_EQ(r->err, want) << "op#" << applied << " " << opName(sub.op) << " '" << sub.name
                                    << "' fault=" << faultName(sub.fault)
                                    << (isView(sub.op) ? " sources=" + joined(sub.sources) : std::string());
            ++applied;
        }
    };
    auto expectRecoveryInstalls = [&](int before, const char* when) {
        if (h.recoveryViewInstalls() - before != m.views()) fail("");
        EXPECT_EQ(h.recoveryViewInstalls() - before, m.views()) << "recovery view installs at " << when;
    };
    auto actOnCreate = [&] {
        if (!h.worker().hasInFlight()) { note("deliver stale create", {}, true); h.deliverCreate(); return; }
        const Submitted& in = ddl[h.startedTotal() - kSetupDdlOps - 1];
        note(std::string("act on create of ") + opName(in.op) + " '" + in.name + "': " + faultName(in.fault),
             in.name, in.fault == Fault::DemoteInAwaitingId);
        ++st.faults[static_cast<int>(in.fault)];
        if (const Timing t = timingOfFault(in.fault); t != kTimingCount) ++st.timing[t];
        switch (in.fault) {
            case Fault::None:            h.deliverCreate(); break;
            case Fault::FailNoCreate:    h.failCreate(false); break;
            case Fault::FailAfterCreate: h.failCreate(true); break;
            case Fault::TimeoutInAwaitingId:
                h.advance(AdminWorker::ddlTimeoutNs() + 1'000'000);
                h.tick();            // answers the op Internal (may start the next op)
                h.deliverCreate();   // the stale reply (FIFO: it is the oldest request)
                break;
            case Fault::DemoteInAwaitingId: {
                // B4: the demotion answers the in-flight op Internal; queued ops stay
                // queued and run on the re-promoted leader (no tick in between).
                const int rec0 = h.recoveryViewInstalls();
                ++st.flaps;
                if (st.restarts > 0) ++st.flapsAfterRestart;
                h.demote();
                h.deliverCreate();   // stale reply
                h.promote();
                processAnswers();    // the demoted op's Internal (it committed no view)
                expectRecoveryInstalls(rec0, "a leadership flap");
                break;
            }
        }
    };
    /// A restart: drain to quiescence (the harness restarts only then), restart,
    /// check the recovered views were re-installed.
    auto restartNow = [&]() -> bool {
        note("restart: drain", {}, true);
        for (int i = 0; !h.quiescent(); ++i) {
            if (i == kMaxDrainIterations) { ADD_FAILURE() << "restart drain did not quiesce\n" << dumpTrace(); return false; }
            if (h.hasPendingCreate()) actOnCreate();
            else if (h.commitHeld()) h.releaseCommits();
            else h.tick();
            processAnswers();
        }
        processAnswers();
        note("restart", {}, true);
        ++st.restarts;
        for (const std::string& n : touchedThisBoot)
            if (!contains(touchedEarlierBoot, n)) touchedEarlierBoot.push_back(n);
        touchedThisBoot.clear();
        const int rec0 = h.recoveryViewInstalls();
        m.restart();
        h.restart();
        expectRecoveryInstalls(rec0, "a restart");
        return true;
    };
    auto scriptsDone = [&] {
        for (int i = 0; i < kSessions; ++i) if (next[i] < kOpsPerSession) return false;
        return true;
    };

    int step = 0;
    for (; step < kMaxSteps; ++step) {
        if (scriptsDone() && h.quiescent()) break;
        const uint64_t a = rng() % 100;
        if (a < 28) {
            const int si = static_cast<int>(rng() % kSessions);
            if (next[si] == kOpsPerSession) continue;
            const Planned& p = scripts[si][next[si]++];
            if (p.op == Op::GetSchema) {
                processAnswers();
                note("session " + std::to_string(si) + " GetSchema '" + p.name + "'", p.name);
                ++st.reads;
                const int64_t c = h.submit(Op::GetSchema, p.name);
                const std::optional<Response> r = h.response(c);
                ASSERT_TRUE(r.has_value()) << "a read is answered synchronously";
                const uint16_t want = expectedCell(m.at(p.name), Op::GetSchema).code;
                ++st.codes[r->err];
                if (r->err != want) fail(p.name);
                EXPECT_EQ(r->err, want) << "GetSchema '" << p.name << "'";
                if (r->err == kOk) EXPECT_EQ(r->version, m.version[p.name]) << "GetSchema '" << p.name << "'";
            } else {
                processAnswers();   // the model must stand where the registry stands (sources bind now)
                // Timing class of this submission.
                Timing t = T1;
                if (h.worker().pendingCount() > 0) t = T2;
                else if (h.worker().hasInFlight()) t = h.hasPendingCreate() ? T3 : T4;
                ++st.timing[t];
                if (contains(touchedEarlierBoot, p.name) && !contains(touchedThisBoot, p.name)) ++st.timing[T7];
                if (!contains(touchedThisBoot, p.name)) touchedThisBoot.push_back(p.name);

                const uint16_t srcErr = isView(p.op) ? m.sourceRule(p.op, p.sources) : kOk;
                note("session " + std::to_string(si) + " submit " + opName(p.op) + " '" + p.name +
                     "' fault=" + faultName(p.fault) +
                     (isView(p.op) ? " sources=" + joined(p.sources) : std::string()), p.name);
                ++st.ddl;
                if (isView(p.op)) { ++st.views; if (p.sources.size() > 1) ++st.twoSourceViews; }
                const int64_t c = isView(p.op) ? h.submitView(p.op, p.name, p.sources) : h.submit(p.op, p.name);
                ddl.push_back(Submitted{c, p.op, p.name, p.fault, p.sources, srcErr});
            }
        } else if (a < 64) {
            noteTick();
            h.tick();
        } else if (a < 79) {
            if (h.hasPendingCreate()) {
                if (h.worker().hasInFlight() && rng() % 4 == 0) {   // parked in AwaitingId
                    note("alloc gate: create-reply wait");
                    ++st.allocGates;
                    expectNoSteadyStateAlloc(h, "create-reply wait");
                }
                actOnCreate();
            }
        } else if (a < 89) {
            // Only a genuinely held commit is a steady-state HW-gate wait: once the HW
            // covers the record the next tick commits + answers + starts the next op
            // (the cold path, which may allocate).
            if (h.worker().hasInFlight() && !h.hasPendingCreate() && h.commitHeld()) {
                note("alloc gate: HW-gate wait");
                ++st.allocGates;
                if (st.restarts > 0) ++st.heldAfterRestart;
                expectNoSteadyStateAlloc(h, "HW-gate wait");
            }
            note("release commits");
            h.releaseCommits();
        } else if (a < 93) {
            if (h.quiescent()) {
                note("alloc gate: idle");
                ++st.allocGates;
                expectNoSteadyStateAlloc(h, "idle");
            }
        } else {   // 7%: restart
            if (!restartNow()) return;
        }
        processAnswers();
        if (::testing::Test::HasFailure()) {
            ADD_FAILURE() << "first failure at step " << step << "; trace up to it:\n" << dumpTrace();
            return;
        }
    }
    ASSERT_LT(step, kMaxSteps) << "the schedule did not quiesce\n" << dumpTrace();
    processAnswers();
    EXPECT_EQ(applied, ddl.size()) << "every DDL op answered";
    EXPECT_EQ(h.viewPlanPushes() - commitInstalls0, expectedCommitInstalls)
        << "one worker install per committed CREATE VIEW (recovery installs counted apart)";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
    expectCatalogMatchesModel(h, m, "end");
    expectNoSteadyStateAlloc(h, "idle at end");

    const int rec0 = h.recoveryViewInstalls();
    m.restart();
    h.restart();
    expectRecoveryInstalls(rec0, "the final restart");
    expectCatalogMatchesModel(h, m, "after restart");
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
    if (::testing::Test::HasFailure()) ADD_FAILURE() << "trace:\n" << dumpTrace();

    st.steps = static_cast<uint64_t>(step);
    gTotals.steps += st.steps; gTotals.ddl += st.ddl; gTotals.reads += st.reads;
    gTotals.views += st.views; gTotals.twoSourceViews += st.twoSourceViews;
    gTotals.restarts += st.restarts; gTotals.heldAfterRestart += st.heldAfterRestart; gTotals.flaps += st.flaps; gTotals.flapsAfterRestart += st.flapsAfterRestart;
    gTotals.allocGates += st.allocGates;
    for (int i = 0; i < 5; ++i) gTotals.faults[i] += st.faults[i];
    for (int i = 0; i < kTimingCount; ++i) gTotals.timing[i] += st.timing[i];
    for (const auto& [c, n] : st.codes) gTotals.codes[c] += n;
    ::testing::Test::RecordProperty("steps", static_cast<int>(st.steps));
    ::testing::Test::RecordProperty("ddlOps", static_cast<int>(st.ddl));
    ::testing::Test::RecordProperty("restarts", static_cast<int>(st.restarts));
    ::testing::Test::RecordProperty("allocGates", static_cast<int>(st.allocGates));
}

class DdlStressRegression : public ::testing::TestWithParam<uint64_t> {};
TEST_P(DdlStressRegression, SeededScheduleMatchesTheModel) { runSeed(GetParam()); }
INSTANTIATE_TEST_SUITE_P(Seeds, DdlStressRegression, ::testing::Range<uint64_t>(1, 65));

TEST(DdlStressEnvRegression, EnvSeed) {
    const char* s = std::getenv("RUNE_DDL_STRESS_SEED");
    if (s == nullptr) GTEST_SKIP() << "set RUNE_DDL_STRESS_SEED=<seed> to replay one seed";
    runSeed(std::strtoull(s, nullptr, 10));
}

/// Prints what this process exercised (all seeds when run as one binary) as ONE
/// summary line: totals, the per-code answer histogram and the timing classes.
class StressTotalsPrinter : public ::testing::EmptyTestEventListener {
    void OnTestProgramEnd(const ::testing::UnitTest&) override {
        const StressStats& t = gTotals;
        if (t.steps == 0) return;
        std::string line = "[ddl-stress] steps=" + std::to_string(t.steps) + " ddlOps=" + std::to_string(t.ddl) +
                           " reads=" + std::to_string(t.reads) + " views=" + std::to_string(t.views) +
                           " twoSourceViews=" + std::to_string(t.twoSourceViews) +
                           " restarts=" + std::to_string(t.restarts);
        char pct[32];
        std::snprintf(pct, sizeof(pct), " (%.1f%% of steps", 100.0 * static_cast<double>(t.restarts) /
                                                             static_cast<double>(t.steps));
        line += pct;
        line += std::string(")") + " heldCommitsAfterRestart=" + std::to_string(t.heldAfterRestart) +
                " flaps=" + std::to_string(t.flaps) + " (after a restart " + std::to_string(t.flapsAfterRestart) + ")" +
                " allocGates=" + std::to_string(t.allocGates);
        line += " | createReplies ok=" + std::to_string(t.faults[0]) + " failNoCreate=" + std::to_string(t.faults[1]) +
                " failAfterCreate=" + std::to_string(t.faults[2]) + " timeout=" + std::to_string(t.faults[3]) +
                " demote=" + std::to_string(t.faults[4]);
        line += " | codes";
        for (const auto& [c, n] : t.codes) line += " " + std::to_string(c) + "=" + std::to_string(n);
        line += " | timing";
        for (int i = 0; i < kTimingCount; ++i) line += std::string(" ") + kTimingNames[i] + "=" + std::to_string(t.timing[i]);
        std::printf("%s\n", line.c_str());
    }
};
const bool kTotalsRegistered = [] {
    ::testing::UnitTest::GetInstance()->listeners().Append(new StressTotalsPrinter);
    return true;
}();

} // namespace
