// DdlStressRegressionTest.cpp — DDL conformance plan Task 10, ported to the
// __catalog write path by catalog history Task 10.
//
// Deterministic, seeded, model-checked: 3 simulated sessions submit random DDL —
// creates, alters, reads, and the catalog ops DROP TOPIC / DROP VIEW / ALTER VIEW /
// ALTER TOPIC CONFIG — over a small colliding name pool (incl. a case variant, an
// untyped topic, a reserved and an empty name) while the scheduler holds and
// releases the catalog's HW in random interleavings and strikes the in-flight op
// with its planned fault: none (the commit is released), a commit TIMEOUT (F13:
// answered NotLeader; the leader catches up — NotLeader to every DDL — until the
// event commits, released at once or left held across later steps and restarts) or
// a DEMOTION (spec §5.2: NotLeader, every queued op NotLeader, the event commits
// under the re-promoted leader).  It also opens ISR-below-minIsr windows (every
// append refused, NotLeader, nothing appended) and restarts frequently (>= 5% of
// steps; each drains to quiescence first).  The broker is multi-node throughout
// (replica set {1,2}, also after a restart).  CREATE / ALTER VIEW read 1 or 2
// sources drawn from typed disk / typed transient topics, view outputs, an untyped
// topic and missing names, each bound by id at SUBMIT time.  Every answer is
// checked against a reference model — name -> Occupant (kind + persistence) +
// version + incarnation (its topic id) — that applies ops in FIFO order using ONLY
// the expectation table and the plan's view-source and drop rules.  Worker view
// installs (live and boot replay) are checked against the model.  The final
// catalog is checked against the model before and after a restart.
//
// Zero-alloc gate: the admin thread's steady-state loop — AdminWorker::doWork while
// idle, while an op waits on a held HW gate, and while the leader waits to catch
// up on a held event — must not allocate.  (DDL processing itself is the cold path
// and may.)  Own executable: replaces global operator new.
//
// On the first failure of a seed the run stops and prints the seed, the replay
// command, the event trace up to the failure (the reproducing prefix) and its
// minimal slice (the events on the failing name + leadership flaps + restarts).
#include <gtest/gtest.h>
#include "support/DdlHarness.h"
#include <rune/catalog/TopicName.h>

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

enum class Fault : uint8_t { None, TimeoutInAwaitingCommit, DemoteInAwaitingCommit };
constexpr int kFaultCount = 3;

const char* faultName(Fault f) {
    switch (f) {
        case Fault::None:                    return "none";
        case Fault::TimeoutInAwaitingCommit: return "timeout";
        case Fault::DemoteInAwaitingCommit:  return "demote";
    }
    return "?";
}

/// The timing classes the stress schedule produces.  T1..T4: the worker's state
/// when a DDL op is SUBMITTED (idle / behind queued ops / the leader catching up on
/// a held event / another op in AwaitingCommit).  T5a: an op started while the ISR
/// is below minIsr (its append refused).  T5c / T5d: a commit timeout released at
/// once / left held.  T6: a demotion in AwaitingCommit.  T7: an op on a name a DDL
/// op touched in an earlier boot and none since this boot (A, restart, B).
enum Timing : uint8_t { T1, T2, T3, T4, T5a, T5c, T5d, T6, T7, kTimingCount };
const char* const kTimingNames[kTimingCount] = {"T1", "T2", "T3", "T4", "T5a", "T5c", "T5d", "T6", "T7"};

struct Bound { std::string name; int32_t topicId; };   // a source bound by id at submit (-1 = unbound)
struct Planned   { Op op; std::string name; Fault fault; std::vector<std::string> sources; };
struct Submitted {
    int64_t corr; Op op; std::string name; Fault fault;
    std::vector<Bound> sources;
    bool persistedView = true;   // CREATE VIEW persisted / ALTER VIEW's submit-time persistence
    bool inDoubt = false;        // struck in AwaitingCommit (timeout / demotion): NotLeader, applied later
};

/// What the model expects an answer to be.  checkIdVersion: an OK whose id and
/// version the model predicts (the topic / sink id from its own id counter; the
/// schema version, or the OBJECT version for a DdlResponse).
struct Expect { uint16_t code; int32_t id = -1; uint32_t version = 0; bool checkIdVersion = false; };
/// Review I1: an op the implementation APPENDED (struck in AwaitingCommit) that the
/// model would have refused — never a code the broker sends, so the seed fails.
constexpr uint16_t kWronglyAccepted = 0xFFFF;

const std::vector<std::string> kPool = {"a", "b", "A", "s", "src", "__views", ""};
// View sources: typed disk (src, src2), typed transient (tsrc), a view's output
// (vsrc), an untyped topic (s), a never-defined name (nosuch), and pool names whose
// kind changes as the schedule runs (typed / view / dropped / case variant).
const std::vector<std::string> kSources = {"src", "src2", "tsrc", "vsrc", "s", "nosuch", "a", "b", "A"};
constexpr int kSessions = 3;
constexpr int kOpsPerSession = 40;
constexpr int kMaxSteps = 50'000;
constexpr int kMaxDrainIterations = 10'000;
constexpr uint64_t kSetupDdlOps = 5;   // src, src2 (harness) + s, tsrc, vsrc (runSeed) before the schedule

struct Model {
    std::map<std::string, Occupant> occ;       // exact name -> occupant (absent = Free)
    std::map<std::string, uint16_t> version;   // the schema version a GetSchema answers
    std::map<std::string, uint32_t> objVersion;   // the catalog object version
    std::map<std::string, int32_t>  topicId;   // the topic / view-sink id the name holds
    std::map<std::string, std::vector<std::string>> viewSources;   // live view -> its source names
    int32_t nextTopicId = 0;   // the catalog's dense id counter (CreateTopic / CreateView / AlterView)
    /// The leader refuses DDL (a demotion or a catch-up on a held event): every op
    /// that starts now is answered NotLeader.
    bool refusing = false;
    bool isrShrunk = false;    // appends refused: an op whose cell succeeds is NotLeader
    std::optional<Submitted> pending;   // the in-doubt op whose event is not applied yet
    std::string wronglyAccepted;        // why the last kWronglyAccepted was expected

    Occupant at(const std::string& n) const {
        if (!rune::catalog::isValidTopicName(n)) return Occupant::BadName;
        if (const auto it = occ.find(n); it != occ.end()) return it->second;
        for (const auto& [m, o] : occ)
            if (m != n && rune::catalog::topicNamesFoldEqual(m, n)) return Occupant::CaseVariant;
        return Occupant::Free;
    }
    Occupant exact(const std::string& n) const {
        const auto it = occ.find(n);
        return it == occ.end() ? Occupant::Free : it->second;
    }
    static bool isTyped(Occupant o) { return o == Occupant::TypedDisk || o == Occupant::TypedTransient; }
    static bool isViewOcc(Occupant o) { return o == Occupant::ViewPersisted || o == Occupant::ViewTransient; }
    /// A source as the client binds it at SUBMIT time (GetTopicSchema): a typed topic
    /// or a view output binds to its id; anything else is unbound.
    Bound bind(const std::string& n) const {
        const Occupant o = exact(n);
        if (!isTyped(o) && !isViewOcc(o)) return Bound{n, -1};
        return Bound{n, topicId.at(n)};
    }
    /// The plan's view-source rules, evaluated when the op STARTS on the ids bound
    /// at submit.  Per source, in order: unbound or no longer that id -> 33; a
    /// view's output -> 41 (D3b/B9); not a typed topic -> 33; a typed transient
    /// topic under a persisted view -> 37 (Phase 2b E).  Then 2+ sources -> 34.
    uint16_t sourceRule(const Submitted& s) const {
        for (const Bound& b : s.sources) {
            const auto it = topicId.find(b.name);
            if (b.topicId < 0 || it == topicId.end() || it->second != b.topicId) return kViewSourceNotFound;
            const Occupant o = exact(b.name);
            if (isViewOcc(o)) return kViewSourceIsView;
            if (!isTyped(o)) return kViewSourceNotFound;
            if (o == Occupant::TypedTransient && s.persistedView) return kViewSourceTransient;
        }
        return s.sources.size() > 1 ? kViewFieldUnresolved : kOk;
    }
    bool readByAView(const std::string& topic) const {
        for (const auto& [v, srcs] : viewSources)
            for (const std::string& x : srcs) if (x == topic) return true;
        return false;
    }
    /// The answer `s` gets on the state as it stands (no effect applied).
    uint16_t verdict(const Submitted& s) const {
        const Occupant o = at(s.name);
        const Cell c = expectedCell(o, s.op);
        if (c.code != kOk) return c.code;
        if (readsSources(s.op)) {
            if (const uint16_t e = sourceRule(s); e != kOk) return e;
        }
        if (s.op == Op::DropTopic && readByAView(s.name)) return kInvalidRequest;
        return kOk;
    }
    /// Apply an accepted op; returns the id / version its OK answer carries.
    Expect applyEffect(const Submitted& s) {
        const Occupant o = at(s.name);
        switch (s.op) {
            case Op::DefineDisk: case Op::DefineTransient:
                if (o == Occupant::Free) { topicId[s.name] = nextTopicId++; objVersion[s.name] = 1; }
                else ++objVersion[s.name];   // an untyped topic adopts the schema: its next event
                occ[s.name] = occupantAfterSuccess(o, s.op);
                version[s.name] = 1;
                return Expect{kOk, topicId[s.name], 1, true};
            case Op::ViewPersisted: case Op::ViewTransient:
            case Op::AlterView: {
                occ[s.name] = s.persistedView ? Occupant::ViewPersisted : Occupant::ViewTransient;
                topicId[s.name] = nextTopicId++;   // a (fresh) sink id
                version[s.name] = 1;               // its output's schema
                objVersion[s.name] = s.op == Op::AlterView ? objVersion[s.name] + 1 : 1;
                std::vector<std::string> names;
                for (const Bound& b : s.sources) names.push_back(b.name);
                viewSources[s.name] = std::move(names);
                // CreateView: its sink id, v1; AlterView (a DdlResponse): gated on the fresh sink, the object version.
                return Expect{kOk, topicId[s.name], s.op == Op::AlterView ? objVersion[s.name] : 1u, true};
            }
            case Op::DropTopic: case Op::DropView: {
                const uint32_t v = objVersion[s.name] + 1;
                occ.erase(s.name); version.erase(s.name); objVersion.erase(s.name); topicId.erase(s.name);
                viewSources.erase(s.name);
                return Expect{kOk, -1, v, true};
            }
            case Op::AlterTopicConfig:
                return Expect{kOk, -1, ++objVersion[s.name], true};
            case Op::GetSchema:
                return Expect{kOk};
            default:   // every ALTER bumps the schema version and the object version
                ++objVersion[s.name];
                version[s.name] = static_cast<uint16_t>(version[s.name] + 1);
                return Expect{kOk, topicId[s.name], version[s.name], true};
        }
    }
    /// Apply `s` (FIFO order) and return its expected answer.
    Expect apply(const Submitted& s) {
        if (s.inDoubt) {
            // Review I1: the implementation appended it (only an accepted op awaits a
            // commit).  The model must AGREE it was acceptable before its effect may
            // land — a wrongly accepted op (a second create on a taken name, ...) fails.
            const uint16_t v = verdict(s);
            if (v != kOk || isrShrunk) {
                wronglyAccepted = std::string(opName(s.op)) + " '" + s.name + "' was appended (struck in "
                                  "AwaitingCommit) but the model refuses it with " + std::to_string(v);
                return Expect{kWronglyAccepted};
            }
            if (refusing) pending = s; else (void)applyEffect(s);
            return Expect{kNotLeader};
        }
        if (refusing) return Expect{kNotLeader};
        const uint16_t v = verdict(s);
        if (v != kOk) return Expect{v};
        if (isrShrunk) return Expect{kNotLeader};   // the append is refused: nothing appended
        return applyEffect(s);
    }
    /// The held in-doubt event commits and is applied; the leader serves again.
    void resolve() {
        if (pending) (void)applyEffect(*pending);
        pending.reset();
        refusing = false;
    }
    int views() const {
        int n = 0;
        for (const auto& [name, o] : occ) n += isViewOcc(o) ? 1 : 0;
        return n;
    }
};

Op drawOp(std::mt19937_64& rng) {
    static const Op ops[] = {Op::DefineDisk, Op::DefineTransient, Op::ViewPersisted, Op::ViewTransient,
                             Op::AlterAdd, Op::AlterSetCompat, Op::GetSchema,
                             Op::DropTopic, Op::DropView, Op::AlterView, Op::AlterTopicConfig};
    return ops[rng() % (sizeof(ops) / sizeof(ops[0]))];
}
Fault drawFault(std::mt19937_64& rng) {
    const uint64_t r = rng() % 100;
    if (r < 76) return Fault::None;
    return r < 88 ? Fault::TimeoutInAwaitingCommit : Fault::DemoteInAwaitingCommit;
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
        const Occupant o = rune::catalog::isValidTopicName(n) ? m.exact(n) : Occupant::Free;
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
            default:   // SchemaLessDisk / SchemaLessTransient
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
    uint64_t faults[kFaultCount] = {};      // in-flight ops struck, by Fault
    uint64_t inDoubtOps = 0;                // ops answered NotLeader in AwaitingCommit
    uint64_t heldInDoubt = 0;               // ... whose event stayed held across later steps
    uint64_t refusedQueued = 0;             // ops answered NotLeader while the leader refused DDL
    uint64_t isrWindows = 0;                // ISR-below-minIsr windows opened
    uint64_t timing[kTimingCount] = {};     // timing classes produced
    std::map<uint16_t, uint64_t> codes;     // DDL + read answers, by error code
};
StressStats gTotals;   // summed over the seeds this process ran (reported at the end)

void runSeed(uint64_t seed) {
    SCOPED_TRACE("seed=" + std::to_string(seed) + "  replay: RUNE_DDL_STRESS_SEED=" + std::to_string(seed) +
                 " rune_ddl_stress_tests --gtest_filter=DdlStressEnvRegression.EnvSeed");
    std::mt19937_64 rng(seed);

    DdlHarnessOptions o;
    o.holdCommits = true;   // replica set {1,2} for the whole run, restarts included
    DdlHarness h(o);        // pre-defines src, src2 (typed disk v1)
    {
        DefineTopicMsg s; s.name = "s"; s.partitionCount = 1;   // untyped (was a static topic)
        const int64_t c = h.submitRawDefine(std::move(s));
        h.settle();
        ASSERT_TRUE(h.response(c).has_value() && h.response(c)->err == kOk) << "setup s";
    }
    for (const auto& [op, name] : {std::pair{Op::DefineTransient, "tsrc"}, std::pair{Op::ViewPersisted, "vsrc"}}) {
        const int64_t c = h.submit(op, name);   // vsrc reads src
        h.settle();
        ASSERT_TRUE(h.response(c).has_value() && h.response(c)->err == kOk) << "setup " << name;
    }
    ASSERT_TRUE(h.violations().empty()) << h.violationText();
    ASSERT_EQ(h.startedTotal(), kSetupDdlOps);
    const int commitInstalls0 = h.viewPlanPushes();

    Model m;
    for (const char* n : {"src", "src2", "s", "tsrc", "vsrc"}) {
        m.topicId[n] = m.nextTopicId++;   // the setup's creates, in order
        m.objVersion[n] = 1;
        ASSERT_EQ(h.idOf(n), m.topicId[n]) << "setup id of " << n;
    }
    m.occ["s"]    = Occupant::SchemaLessDisk;
    m.occ["src"]  = Occupant::TypedDisk;      m.version["src"]  = 1;
    m.occ["src2"] = Occupant::TypedDisk;      m.version["src2"] = 1;
    m.occ["tsrc"] = Occupant::TypedTransient; m.version["tsrc"] = 1;
    m.occ["vsrc"] = Occupant::ViewPersisted;  m.version["vsrc"] = 1;
    m.viewSources["vsrc"] = {"src"};

    std::vector<std::vector<Planned>> scripts(kSessions);
    for (auto& sc : scripts)
        for (int i = 0; i < kOpsPerSession; ++i) {
            Planned p{drawOp(rng), kPool[rng() % kPool.size()], drawFault(rng), {}};
            if (readsSources(p.op)) p.sources = drawSources(rng);
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
                if (trace[i].global || (!trace[i].ticks && rune::catalog::topicNamesFoldEqual(trace[i].name, failName)))
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
            const bool refusing = m.refusing;
            const bool hadPending = m.pending.has_value();
            const Expect want = m.apply(sub);
            ++st.codes[r->err];
            if (refusing && !sub.inDoubt) ++st.refusedQueued;
            const bool installs = readsSources(sub.op) && (want.code == kOk || (sub.inDoubt && want.code == kNotLeader));
            // A view installed now (an OK, or an in-doubt one applied at once); a held one at resolve().
            if (installs && !(sub.inDoubt && m.pending.has_value() && !hadPending)) ++expectedCommitInstalls;
            const std::string what = std::string("op#") + std::to_string(applied) + " " + opName(sub.op) + " '" +
                                     sub.name + "' fault=" + faultName(sub.fault) +
                                     (readsSources(sub.op) ? " sources=" + [&] {
                                         std::string x;
                                         for (const Bound& b : sub.sources) x += (x.empty() ? "" : ",") + b.name;
                                         return x;
                                     }() : std::string());
            if (want.code == kWronglyAccepted) {
                fail(sub.name);
                ADD_FAILURE() << what << ": " << m.wronglyAccepted;
            } else {
                if (r->err != want.code) fail(sub.name);
                EXPECT_EQ(r->err, want.code) << what;
                if (want.checkIdVersion && r->err == kOk) {
                    // Review M3: the answer's id and version, from the model's own counters.
                    if (r->id != want.id || r->version != want.version) fail(sub.name);
                    EXPECT_EQ(r->id, want.id) << what << ": the answer's topic / sink id";
                    EXPECT_EQ(r->version, want.version) << what << ": the answer's version";
                }
            }
            ++applied;
        }
    };
    /// The model's in-doubt event is applied (its commit released, or a restart).
    auto resolveModel = [&] {
        if (m.pending && readsSources(m.pending->op)) ++expectedCommitInstalls;
        m.resolve();
    };
    /// Release a held in-doubt event: first every queued op is answered NotLeader
    /// (they bound their sources on the state without it), then the HW covers the
    /// event, the leader applies it and serves again.
    auto releaseHeldEvent = [&] {
        for (int i = 0; i < kMaxDrainIterations && h.worker().pendingCount() > 0; ++i) h.tick();
        processAnswers();
        note("release the held event", {}, true);
        h.releaseCommits();
        for (int i = 0; i < kMaxDrainIterations && !h.worker().isLeader(); ++i) h.tick();
        if (!h.worker().isLeader()) ADD_FAILURE() << "the leader never caught up on the released event";
        resolveModel();
    };
    /// The in-flight op (the oldest unanswered, FIFO) takes its planned fault.
    auto actOnInFlight = [&] {
        processAnswers();
        Submitted& in = ddl[applied];
        note(std::string("act on ") + opName(in.op) + " '" + in.name + "': " + faultName(in.fault), in.name,
             in.fault == Fault::DemoteInAwaitingCommit);
        ++st.faults[static_cast<int>(in.fault)];
        switch (in.fault) {
            case Fault::None:
                h.releaseCommits();
                h.tick();   // applied + answered (may start the next op)
                break;
            case Fault::TimeoutInAwaitingCommit: {
                in.inDoubt = true;
                ++st.inDoubtOps;
                m.refusing = true;
                h.advance(AdminWorker::ddlTimeoutNs() + 1'000'000);
                h.tick();   // NotLeader (F13); the leader catches up: queued ops NotLeader
                processAnswers();
                if (rng() % 2 == 0) { ++st.timing[T5c]; releaseHeldEvent(); }
                else { ++st.timing[T5d]; ++st.heldInDoubt; note("the event stays held", {}, true); }
                break;
            }
            case Fault::DemoteInAwaitingCommit: {
                in.inDoubt = true;
                ++st.inDoubtOps;
                ++st.timing[T6];
                ++st.flaps;
                if (st.restarts > 0) ++st.flapsAfterRestart;
                const int rec0 = h.recoveryViewInstalls();
                m.refusing = true;
                h.demote();   // the in-flight op: NotLeader at once (spec §5.2)
                for (int i = 0; i < kMaxDrainIterations && h.worker().pendingCount() > 0; ++i) h.tick();
                if (h.worker().pendingCount() > 0) ADD_FAILURE() << "the demoted admin thread did not drain its queue";
                processAnswers();
                h.promote();
                releaseHeldEvent();
                if (h.recoveryViewInstalls() != rec0) fail("");
                EXPECT_EQ(h.recoveryViewInstalls(), rec0) << "a leadership flap re-installs no view";
                break;
            }
        }
    };
    /// A restart: drain to quiescence (the harness restarts only then), restart,
    /// check the boot replay re-installed every live view; a held event is applied.
    auto restartNow = [&]() -> bool {
        note("restart: drain", {}, true);
        for (int i = 0; !h.quiescent(); ++i) {
            if (i == kMaxDrainIterations) { ADD_FAILURE() << "restart drain did not quiesce\n" << dumpTrace(); return false; }
            if (h.worker().hasInFlight() && h.commitHeld()) actOnInFlight();
            else h.tick();
            processAnswers();
        }
        processAnswers();
        if (m.isrShrunk) { h.restoreIsr(); m.isrShrunk = false; }
        note("restart", {}, true);
        ++st.restarts;
        for (const std::string& n : touchedThisBoot)
            if (!contains(touchedEarlierBoot, n)) touchedEarlierBoot.push_back(n);
        touchedThisBoot.clear();
        const int rec0 = h.recoveryViewInstalls();
        const int viewsAtCheckpoint = m.views();   // a held event is not in the boot replay
        h.restart();
        if (h.recoveryViewInstalls() - rec0 != viewsAtCheckpoint) fail("");
        EXPECT_EQ(h.recoveryViewInstalls() - rec0, viewsAtCheckpoint) << "recovery view installs at a restart";
        resolveModel();   // the rebooted leader learned the HW: the held event is applied live
        return true;
    };
    auto scriptsDone = [&] {
        for (int i = 0; i < kSessions; ++i) if (next[i] < kOpsPerSession) return false;
        return true;
    };

    int step = 0;
    for (; step < kMaxSteps; ++step) {
        if (scriptsDone() && h.quiescent() && !m.refusing && !m.isrShrunk) break;
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
                Timing t = T1;
                if (h.worker().pendingCount() > 0) t = T2;
                else if (m.refusing) t = T3;
                else if (h.worker().hasInFlight()) t = T4;
                ++st.timing[t];
                if (m.isrShrunk) ++st.timing[T5a];
                if (contains(touchedEarlierBoot, p.name) && !contains(touchedThisBoot, p.name)) ++st.timing[T7];
                if (!contains(touchedThisBoot, p.name)) touchedThisBoot.push_back(p.name);

                Submitted sub{0, p.op, p.name, p.fault, {}, p.op != Op::ViewTransient, false};
                if (p.op == Op::AlterView) {
                    // Read at SUBMIT time from the model; review M4: the projected
                    // ViewRegistry must agree with it (a disagreement fails, never copied).
                    const Occupant o = m.exact(p.name);
                    sub.persistedView = o != Occupant::ViewTransient;
                    const ViewRecord* v = h.views().byName(p.name);
                    const bool agree = Model::isViewOcc(o) ? v != nullptr && (v->persistence == 1) == (o == Occupant::ViewTransient)
                                                           : v == nullptr;
                    if (!agree) fail(p.name);
                    EXPECT_TRUE(agree) << "ALTER VIEW '" << p.name << "': the ViewRegistry disagrees with the model ("
                                       << occupantName(o) << ")";
                }
                for (const std::string& src : p.sources) sub.sources.push_back(m.bind(src));
                note("session " + std::to_string(si) + " submit " + opName(p.op) + " '" + p.name +
                     "' fault=" + faultName(p.fault) +
                     (readsSources(p.op) ? " sources=" + joined(p.sources) : std::string()), p.name);
                ++st.ddl;
                if (readsSources(p.op)) { ++st.views; if (p.sources.size() > 1) ++st.twoSourceViews; }
                sub.corr = readsSources(p.op) ? h.submitView(p.op, p.name, p.sources) : h.submit(p.op, p.name);
                ddl.push_back(std::move(sub));
            }
        } else if (a < 64) {
            noteTick();
            h.tick();
        } else if (a < 82) {
            // A held HW: the in-flight op takes its fault, or a held in-doubt event
            // is released.  Only a genuinely held commit is a steady-state wait.
            if (h.worker().hasInFlight() && h.commitHeld()) {
                if (rng() % 4 == 0) {
                    note("alloc gate: HW-gate wait");
                    ++st.allocGates;
                    if (st.restarts > 0) ++st.heldAfterRestart;
                    expectNoSteadyStateAlloc(h, "HW-gate wait");
                }
                actOnInFlight();
            } else if (m.refusing && !h.worker().hasInFlight()) {
                if (h.worker().pendingCount() == 0 && rng() % 3 == 0) {
                    note("alloc gate: catching-up wait");
                    ++st.allocGates;
                    expectNoSteadyStateAlloc(h, "catching-up wait");
                }
                releaseHeldEvent();
            }
        } else if (a < 87) {
            // An ISR-below-minIsr window opens or closes (never over an in-flight op
            // or a held event: the leadership change would release it).
            processAnswers();
            if (m.isrShrunk) {
                note("isr restored", {}, true);
                h.restoreIsr();
                m.isrShrunk = false;
            } else if (!h.worker().hasInFlight() && !m.refusing) {
                note("isr below minIsr", {}, true);
                ++st.isrWindows;
                h.shrinkIsr();
                m.isrShrunk = true;
            }
        } else if (a < 93) {
            if (h.quiescent() && !m.refusing) {
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
        << "one live install per applied CREATE / ALTER VIEW (boot replay installs counted apart)";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
    expectCatalogMatchesModel(h, m, "end");
    expectNoSteadyStateAlloc(h, "idle at end");

    const int rec0 = h.recoveryViewInstalls();
    h.restart();
    EXPECT_EQ(h.recoveryViewInstalls() - rec0, m.views()) << "recovery view installs at the final restart";
    expectCatalogMatchesModel(h, m, "after restart");
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
    if (::testing::Test::HasFailure()) ADD_FAILURE() << "trace:\n" << dumpTrace();

    st.steps = static_cast<uint64_t>(step);
    gTotals.steps += st.steps; gTotals.ddl += st.ddl; gTotals.reads += st.reads;
    gTotals.views += st.views; gTotals.twoSourceViews += st.twoSourceViews;
    gTotals.restarts += st.restarts; gTotals.heldAfterRestart += st.heldAfterRestart; gTotals.flaps += st.flaps;
    gTotals.flapsAfterRestart += st.flapsAfterRestart;
    gTotals.allocGates += st.allocGates;
    for (int i = 0; i < kFaultCount; ++i) gTotals.faults[i] += st.faults[i];
    gTotals.inDoubtOps += st.inDoubtOps;
    gTotals.heldInDoubt += st.heldInDoubt;
    gTotals.refusedQueued += st.refusedQueued;
    gTotals.isrWindows += st.isrWindows;
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
        line += " | inFlight ok=" + std::to_string(t.faults[0]) + " timeout=" + std::to_string(t.faults[1]) +
                " demote=" + std::to_string(t.faults[2]) +
                " (in doubt " + std::to_string(t.inDoubtOps) + ", held " + std::to_string(t.heldInDoubt) + ")" +
                " refusedQueued=" + std::to_string(t.refusedQueued) +
                " isrWindows=" + std::to_string(t.isrWindows);
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
