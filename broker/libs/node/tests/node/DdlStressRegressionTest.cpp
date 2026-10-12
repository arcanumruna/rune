// DdlStressRegressionTest.cpp — DDL conformance plan Task 10.
//
// Deterministic, seeded, model-checked: 3 simulated sessions submit random DDL over
// a small colliding name pool (incl. a case variant, a reserved and an empty name)
// while the scheduler delivers control replies (ok / fail / fail-after-create /
// timeout / demotion) and releases commits in random interleavings, with occasional
// restarts.  Every answer is checked against a reference model — name ->
// Occupant (kind + persistence) + version — that applies ops in FIFO submission
// order using ONLY the expectation table.  The final catalog is checked against the
// model before and after a restart.
//
// Zero-alloc gate: the admin thread's steady-state loop — AdminWorker::doWork while
// idle, and while an op waits on the HW gate — must not allocate.  (DDL processing
// itself is the cold path and may.)  Own executable: replaces global operator new.
//
// On the first failure of a seed the run stops and prints the seed, the replay
// command, the event trace up to the failure (the reproducing prefix) and its
// minimal slice (the events on the failing name + leadership flaps + restarts).
#include <gtest/gtest.h>
#include "support/DdlHarness.h"
#include <rune/node/TopicName.h>

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

struct Planned   { Op op; std::string name; Fault fault; };
struct Submitted { int64_t corr; Op op; std::string name; Fault fault; };

const std::vector<std::string> kPool = {"a", "b", "A", "s", "src", "__views", ""};
constexpr int kSessions = 3;
constexpr int kOpsPerSession = 40;
constexpr int kMaxSteps = 50'000;
constexpr uint64_t kSetupDdlOps = 2;   // the harness defines src, src2 before the schedule

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
    /// Apply `s` (FIFO order) and return its expected answer.
    uint16_t apply(const Submitted& s) {
        const Occupant o = at(s.name);
        const Cell c = expectedCell(o, s.op);
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

void expectCatalogMatchesModel(DdlHarness& h, const Model& m, const char* when) {
    const Catalog cat = h.catalog();
    std::vector<std::string> names = kPool;
    names.push_back("src2");
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
    uint64_t steps = 0, ddl = 0, reads = 0, restarts = 0, allocGates = 0;
    uint64_t faults[5] = {};            // create replies acted on, by Fault
    std::map<uint16_t, uint64_t> codes; // DDL + read answers, by error code
};
StressStats gTotals;   // summed over the seeds this process ran (reported at the end)

void runSeed(uint64_t seed) {
    SCOPED_TRACE("seed=" + std::to_string(seed) + "  replay: RUNE_DDL_STRESS_SEED=" + std::to_string(seed) +
                 " rune_ddl_stress_tests --gtest_filter=DdlStressEnvRegression.EnvSeed");
    std::mt19937_64 rng(seed);

    DdlHarnessOptions o;
    o.holdCommits = true;
    TopicSpec s; s.name = "s"; s.partitionCount = 1;
    o.statics.push_back(s);
    DdlHarness h(o);   // pre-defines src, src2 (typed disk v1)
    ASSERT_TRUE(h.violations().empty()) << h.violationText();
    h.setControl(ControlMode::Hold);

    Model m;
    m.statics = {"s"};
    m.occ["s"] = Occupant::SchemaLessDisk;
    m.occ["src"] = Occupant::TypedDisk;  m.version["src"] = 1;
    m.occ["src2"] = Occupant::TypedDisk; m.version["src2"] = 1;

    std::vector<std::vector<Planned>> scripts(kSessions);
    for (auto& sc : scripts)
        for (int i = 0; i < kOpsPerSession; ++i)
            sc.push_back(Planned{drawOp(rng), kPool[rng() % kPool.size()], drawFault(rng)});
    std::vector<int> next(kSessions, 0);

    StressStats st;
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

    std::vector<Submitted> ddl;   // DDL submissions, FIFO
    std::size_t applied = 0;      // DDL answers folded into the model
    auto processAnswers = [&] {
        while (applied < ddl.size()) {
            const std::optional<Response> r = h.response(ddl[applied].corr);
            if (!r) break;
            const uint16_t want = m.apply(ddl[applied]);
            ++st.codes[r->err];
            if (r->err != want && !failed) { failed = true; failName = ddl[applied].name; }
            EXPECT_EQ(r->err, want) << "op#" << applied << " " << opName(ddl[applied].op) << " '"
                                    << ddl[applied].name << "' fault=" << faultName(ddl[applied].fault);
            ++applied;
        }
    };
    auto actOnCreate = [&] {
        if (!h.worker().hasInFlight()) { note("deliver stale create", {}, true); h.deliverCreate(); return; }
        const Submitted& in = ddl[h.startedTotal() - kSetupDdlOps - 1];
        note(std::string("act on create of ") + opName(in.op) + " '" + in.name + "': " + faultName(in.fault),
             in.name, in.fault == Fault::DemoteInAwaitingId);
        ++st.faults[static_cast<int>(in.fault)];
        switch (in.fault) {
            case Fault::None:            h.deliverCreate(); break;
            case Fault::FailNoCreate:    h.failCreate(false); break;
            case Fault::FailAfterCreate: h.failCreate(true); break;
            case Fault::TimeoutInAwaitingId:
                h.advance(AdminWorker::ddlTimeoutNs() + 1'000'000);
                h.tick();            // answers the op Internal (may start the next op)
                h.deliverCreate();   // the stale reply (FIFO: it is the oldest request)
                break;
            case Fault::DemoteInAwaitingId:
                // B4: the demotion answers the in-flight op Internal; queued ops stay
                // queued and run on the re-promoted leader (no tick in between).
                h.demote();
                h.deliverCreate();   // stale reply
                h.promote();
                break;
        }
    };
    auto scriptsDone = [&] {
        for (int i = 0; i < kSessions; ++i) if (next[i] < kOpsPerSession) return false;
        return true;
    };

    int step = 0;
    for (; step < kMaxSteps; ++step) {
        if (scriptsDone() && h.quiescent()) break;
        const uint64_t a = rng() % 100;
        if (a < 30) {
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
                const Occupant occ = m.at(p.name);
                ++st.codes[r->err];
                if (r->err != expectedCell(occ, Op::GetSchema).code && !failed) { failed = true; failName = p.name; }
                EXPECT_EQ(r->err, expectedCell(occ, Op::GetSchema).code) << "GetSchema '" << p.name << "'";
                if (r->err == kOk) EXPECT_EQ(r->version, m.version[p.name]) << "GetSchema '" << p.name << "'";
            } else {
                note("session " + std::to_string(si) + " submit " + opName(p.op) + " '" + p.name +
                     "' fault=" + faultName(p.fault), p.name);
                ++st.ddl;
                const int64_t c = h.submit(p.op, p.name);
                ddl.push_back(Submitted{c, p.op, p.name, p.fault});
            }
        } else if (a < 70) {
            noteTick();
            h.tick();
        } else if (a < 85) {
            if (h.hasPendingCreate()) {
                if (h.worker().hasInFlight() && rng() % 4 == 0) {   // parked in AwaitingId
                    note("alloc gate: create-reply wait");
                    ++st.allocGates;
                    expectNoSteadyStateAlloc(h, "create-reply wait");
                }
                actOnCreate();
            }
        } else if (a < 95) {
            // Only a genuinely held commit is a steady-state HW-gate wait: once the HW
            // covers the record the next tick commits + answers + starts the next op
            // (the cold path, which may allocate).
            if (h.worker().hasInFlight() && !h.hasPendingCreate() && h.commitHeld()) {
                note("alloc gate: HW-gate wait");
                ++st.allocGates;
                expectNoSteadyStateAlloc(h, "HW-gate wait");
            }
            note("release commits");
            h.releaseCommits();
        } else if (a < 98) {
            if (h.quiescent()) {
                note("alloc gate: idle");
                ++st.allocGates;
                expectNoSteadyStateAlloc(h, "idle");
            }
        } else if (h.quiescent()) {
            processAnswers();
            note("restart", {}, true);
            ++st.restarts;
            m.restart();
            h.restart();
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
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
    expectCatalogMatchesModel(h, m, "end");
    expectNoSteadyStateAlloc(h, "idle at end");

    m.restart();
    h.restart();
    expectCatalogMatchesModel(h, m, "after restart");
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
    if (::testing::Test::HasFailure()) ADD_FAILURE() << "trace:\n" << dumpTrace();

    st.steps = static_cast<uint64_t>(step);
    gTotals.steps += st.steps; gTotals.ddl += st.ddl; gTotals.reads += st.reads;
    gTotals.restarts += st.restarts; gTotals.allocGates += st.allocGates;
    for (int i = 0; i < 5; ++i) gTotals.faults[i] += st.faults[i];
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

/// Prints what this process exercised (all seeds when run as one binary).
class StressTotalsPrinter : public ::testing::EmptyTestEventListener {
    void OnTestProgramEnd(const ::testing::UnitTest&) override {
        if (gTotals.steps == 0) return;
        std::printf("[ddl-stress] steps=%llu ddlOps=%llu reads=%llu restarts=%llu allocGates=%llu\n",
                    static_cast<unsigned long long>(gTotals.steps), static_cast<unsigned long long>(gTotals.ddl),
                    static_cast<unsigned long long>(gTotals.reads), static_cast<unsigned long long>(gTotals.restarts),
                    static_cast<unsigned long long>(gTotals.allocGates));
        std::printf("[ddl-stress] create replies: ok=%llu failNoCreate=%llu failAfterCreate=%llu timeout=%llu demote=%llu\n",
                    static_cast<unsigned long long>(gTotals.faults[0]), static_cast<unsigned long long>(gTotals.faults[1]),
                    static_cast<unsigned long long>(gTotals.faults[2]), static_cast<unsigned long long>(gTotals.faults[3]),
                    static_cast<unsigned long long>(gTotals.faults[4]));
        std::printf("[ddl-stress] answers by code:");
        for (const auto& [c, n] : gTotals.codes)
            std::printf(" %u=%llu", static_cast<unsigned>(c), static_cast<unsigned long long>(n));
        std::printf("\n");
    }
};
const bool kTotalsRegistered = [] {
    ::testing::UnitTest::GetInstance()->listeners().Append(new StressTotalsPrinter);
    return true;
}();

} // namespace
