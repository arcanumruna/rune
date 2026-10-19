#pragma once
// DdlHarness.h — DDL conformance plan: the conformance harness, on the catalog
// write path (catalog history Task 10).
//
// Built on CatalogDdlRig (Task 9): ONE serving catalog leader — a real disk
// __catalog Log, its PartitionReplica (replica set {1,2}: node 1 leads, follower 2
// fetches) and the AdminWorker that owns the CatalogApplier and the registries it
// projects.  Every DDL is one __catalog event; nothing is created behind the
// catalog's back (no control-thread round-trip, no orphan, no placeholder).
//
//   * ControlMode::Hold = "hold the HW": follower 2 stops fetching, so an appended
//     event waits in AwaitingCommit until releaseCommits() (one fetch at the LEO);
//     Auto: follower 2 fetches on every tick (an append commits on the next one).
//   * demote / promote: the catalog role at a new epoch (promotion catches up —
//     NotLeader — until the whole log is applied, then serves).
//   * shrinkIsr / restoreIsr: the ISR below the catalog's minIsr (majority) — every
//     append is refused (NotLeader, nothing appended).
//   * restart (only when quiescent): the log recovered, a fresh AdminWorker whose
//     one boot replay rebuilds the catalog, then the leadership again; the leader
//     learns the committed HW from one follower fetch (it holds every appended
//     event) and later commits are held again while in Hold.
//
// After EVERY admin step it checks the serialization invariant: started-but-
// unanswered DDL ops == (hasInFlight() ? 1 : 0); no event appended while another
// op is in flight (and at most one per pass); DDL answers in FIFO submission
// order; the projected registries agree with the applied catalog.  Violations are
// recorded, never thrown: every test asserts violations().empty().
//
// Single-threaded, test-only.  Must NOT be included from production code.
#include "CatalogDdlRig.h"
#include "DdlExpectations.h"

#include <rune/node/AdminChannels.h>
#include <rune/node/AdminWorker.h>
#include <rune/node/PersistenceMode.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/ViewRegistry.h>

#include <rune/catalog/CatalogFingerprint.h>
#include <rune/catalog/CatalogState.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/view/ViewPlan.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

namespace rune::node::test {

// ── payloads ────────────────────────────────────────────────────────────────
/// Every typed DefineTopic's descriptor: {id Int64 (key), hp Int64}.
inline std::vector<uint8_t> charBlob() {
    return rune::codec::serialize(rune::codec::DescriptorBuilder("character", 1)
        .add("id", rune::codec::PrimType::Int64, 1, /*isKey=*/true)
        .add("hp", rune::codec::PrimType::Int64)
        .build());
}

/// Authored {id, hp} Replace view keyed by "id" over EVERY id in `srcTopicIds`
/// (output fields read source 0; `extraColumn` adds hp2 — an ALTER VIEW's new
/// output).  Phase 1 views are single-source: the compiler rejects 2+ sources
/// fail-closed (ViewFieldUnresolved) AFTER the per-source checks.
inline std::vector<uint8_t> viewPlanBlob(const std::string& viewName, const std::vector<int32_t>& srcTopicIds,
                                         uint8_t persistence, bool extraColumn = false) {
    namespace rv = rune::view;
    rv::ViewPlan p;
    p.viewName = viewName;
    p.key = "id";
    p.persistence = persistence;
    for (const int32_t id : srcTopicIds) p.sources.push_back(rv::SourceSpec{id, "id"});
    p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::Int64, 0, "id", rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"hp", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
    if (extraColumn)
        p.output.push_back(rv::AuthoredField{"hp2", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
    return rv::serializeAuthored(p);
}
inline std::vector<uint8_t> viewPlanBlob(const std::string& viewName, int32_t srcTopicId, uint8_t persistence) {
    return viewPlanBlob(viewName, std::vector<int32_t>{srcTopicId}, persistence);
}

/// The ALTER ops blob for `op` (uniq makes every ADD's field name fresh).
inline std::vector<uint8_t> alterOpsFor(Op op, uint64_t uniq) {
    rune::codec::AlterOp o{};
    switch (op) {
        case Op::AlterDrop:
            o.type = rune::codec::AlterOpType::Drop; o.name = "hp"; break;
        case Op::AlterRename:
            o.type = rune::codec::AlterOpType::Rename; o.name = "hp"; o.newName = "health"; break;
        case Op::AlterRenameKey:   // the key field: the schema's keyField must follow (B8)
            o.type = rune::codec::AlterOpType::Rename; o.name = "id"; o.newName = "pk"; break;
        case Op::AlterSetCompat:
            o.type = rune::codec::AlterOpType::SetCompatibility; o.mode = rune::codec::CompatMode::Forward; break;
        default:
            o.type = rune::codec::AlterOpType::Add; o.name = "extra" + std::to_string(uniq);
            o.primType = rune::codec::PrimType::Int64; o.fixedLen = 0; break;
    }
    return rune::codec::encodeAlterOps({o});
}

/// The ALTER TOPIC CONFIG every Op::AlterTopicConfig sends (an alterable key, a valid value).
inline std::vector<rune::catalog::ConfigChange> configChangesFor(uint64_t uniq) {
    return {rune::catalog::ConfigChange{rune::catalog::ConfigKey::RetentionMs,
                                        static_cast<int64_t>(60'000 + uniq)}};
}

// ── catalog snapshot ────────────────────────────────────────────────────────
enum class EntryKind : uint8_t { SchemaLess, Typed, View };

/// One live catalog object (topic or view) as the applied __catalog states it.
struct CatalogEntry {
    int32_t         id = -1;            // topicId / the view's sink topic id
    std::string     name;
    EntryKind       kind = EntryKind::SchemaLess;
    PersistenceMode persistence = PersistenceMode::Disk;
    uint16_t        version = 0;        // typed: the schema version; view: its output's (1); untyped: 0
    uint32_t        objectVersion = 0;  // the catalog object version (every event on it bumps it)
    int32_t         partitions = 0;
    uint64_t        fingerprint = 0;    // rune::catalog::fingerprintOf (the definition, config included)
    std::string     keyField;           // the schema's / view's key field ("" when untyped)
    bool operator==(const CatalogEntry&) const = default;
};
using Catalog = std::vector<CatalogEntry>;   // in id order

inline void PrintTo(const CatalogEntry& e, std::ostream* os) {
    *os << "{id=" << e.id << " name='" << e.name << "' kind="
        << (e.kind == EntryKind::Typed ? "Typed" : e.kind == EntryKind::View ? "View" : "SchemaLess")
        << " persistence=" << (e.persistence == PersistenceMode::Transient ? "Transient" : "Disk")
        << " v" << e.version << " obj v" << e.objectVersion << " parts=" << e.partitions
        << " fp=" << e.fingerprint << " key='" << e.keyField << "'}";
}

[[nodiscard]] inline const CatalogEntry* findEntry(const Catalog& c, const std::string& name) {
    for (const CatalogEntry& e : c) if (e.name == name) return &e;
    return nullptr;
}
/// A pointer into a temporary snapshot would dangle: name the snapshot first.
const CatalogEntry* findEntry(Catalog&&, const std::string&) = delete;

/// One answer.  id: the topic / view sink id the answer names (a DdlResponse: its
/// gateTopicId); version: the schema version (Define / Alter / Get), 1 for a
/// CreateView OK, the OBJECT version for a DdlResponse.
struct Response {
    Op          op;
    int64_t     corr;
    int32_t     id;
    uint32_t    version;
    uint16_t    err;
    std::string detail;
    uint64_t    objectId = 0;
};

enum class ControlMode : uint8_t { Auto, Hold };

struct DdlHarnessOptions {
    bool holdCommits = false;   // start in ControlMode::Hold: commits wait for releaseCommits()
    bool withSources = true;    // pre-define "src" and "src2" (typed, disk, 1 partition)
};

class DdlHarness {
public:
    static constexpr int32_t kFollower = 2;

    explicit DdlHarness(DdlHarnessOptions o = {}) : opt_(o) {
        rig_ = std::make_unique<CatalogDdlRig>("ddlh", /*brokers=*/2);
        control_ = opt_.holdCommits ? ControlMode::Hold : ControlMode::Auto;
        hookAnswers_();
        bootLeader_();
        if (opt_.withSources) {
            for (const char* n : {"src", "src2"}) {
                const int64_t c = submit(Op::DefineDisk, n);
                settle();
                const std::optional<Response> r = response(c);
                if (!r || r->err != kOk) violations_.push_back(std::string("setup: define ") + n + " failed");
            }
        }
    }
    DdlHarness(const DdlHarness&) = delete;
    DdlHarness& operator=(const DdlHarness&) = delete;

    // ── submit (returns the op's correlation id) ────────────────────────────
    /// Queued (never driven).  A view op's source and a CAS version are resolved at
    /// SUBMIT time (an unknown source name -> id -1); GetSchema is answered at once.
    int64_t submit(Op op, const std::string& name, const std::string& viewSource = "src") {
        if (readsSources(op)) return submitView(op, name, {viewSource});
        const int64_t corr = nextCorr_(op);
        switch (op) {
            case Op::DefineDisk:
            case Op::DefineTransient: {
                DefineTopicMsg m;
                m.session = rig_->sessionId(); m.corr = corr; m.partitionCount = 1; m.validate = false;
                m.name = name; m.keyField = "id"; m.blob = charBlob();
                m.persistence = op == Op::DefineTransient ? 1 : 0;
                m.meta = rig_->meta();
                enqueue_(DdlRequest{std::move(m)});
                break;
            }
            case Op::GetSchema: {
                rig_->admin().submitGetSchema(GetSchemaMsg{rig_->sessionId(), corr, name});   // a pure read
                if (const auto a = rig_->schemaAnswerFor(corr))
                    responses_.push_back(Response{Op::GetSchema, corr, a->topicId, a->version, a->code, {}, 0});
                else
                    violations_.push_back("GetSchema '" + name + "' was not answered at once");
                break;
            }
            case Op::DropTopic: {
                DropTopicMsg m;
                m.session = rig_->sessionId(); m.corr = corr; m.name = name; m.meta = rig_->meta();
                enqueue_(DdlRequest{std::move(m)});
                break;
            }
            case Op::DropView: {
                DropViewMsg m;
                m.session = rig_->sessionId(); m.corr = corr; m.name = name; m.meta = rig_->meta();
                enqueue_(DdlRequest{std::move(m)});
                break;
            }
            case Op::AlterTopicConfig: {
                AlterTopicConfigMsg m;
                m.session = rig_->sessionId(); m.corr = corr; m.name = name;
                m.changes = configChangesFor(static_cast<uint64_t>(corr));
                m.meta = rig_->meta();
                enqueue_(DdlRequest{std::move(m)});
                break;
            }
            default: {
                AlterTopicMsg m;
                m.session = rig_->sessionId(); m.corr = corr; m.topicName = name;
                if (op == Op::AlterCasCurrent) {
                    const RegistryEntry* e = schemas().byName(name);
                    m.expectedVersion = e ? e->version : 1;   // read at SUBMIT time
                } else if (op == Op::AlterCasStale) {
                    m.expectedVersion = 99;
                }
                m.opsBlob = alterOpsFor(op, static_cast<uint64_t>(corr));
                m.meta = rig_->meta();
                enqueue_(DdlRequest{std::move(m)});
                break;
            }
        }
        return corr;
    }
    /// CREATE VIEW / ALTER VIEW over several sources, each resolved name -> topic id
    /// at SUBMIT time from the projected SchemaRegistry exactly like a client's
    /// GetTopicSchema (an untyped or unknown name -> id -1).  ALTER VIEW keeps the
    /// view's current persistence and adds an output column.
    int64_t submitView(Op op, const std::string& name, const std::vector<std::string>& sources) {
        const int64_t corr = nextCorr_(op);
        std::vector<int32_t> ids;
        for (const std::string& src : sources) {
            const RegistryEntry* e = schemas().byName(src);
            ids.push_back(e ? e->topicId : -1);
        }
        if (op == Op::AlterView) {
            AlterViewMsg m;
            m.session = rig_->sessionId(); m.corr = corr; m.name = name; m.keyField = "id";
            const ViewRecord* v = views().byName(name);
            m.persistence = v ? v->persistence : 0;
            m.planBlob = viewPlanBlob(name, ids, m.persistence, /*extraColumn=*/true);
            m.meta = rig_->meta();
            enqueue_(DdlRequest{std::move(m)});
            return corr;
        }
        CreateViewMsg m;
        m.session = rig_->sessionId(); m.corr = corr; m.persistence = op == Op::ViewTransient ? 1 : 0;
        m.viewName = name; m.keyField = "id";
        m.planBlob = viewPlanBlob(name, ids, m.persistence);
        m.meta = rig_->meta();
        enqueue_(DdlRequest{std::move(m)});
        return corr;
    }
    /// Hand-built requests: the harness stamps session + corr, and a fresh meta
    /// unless the message carries a requestId of its own (a deliberate retry).
    int64_t submitRawDefine(DefineTopicMsg m) { return submitRaw_(std::move(m), Op::DefineDisk); }
    int64_t submitRawView(CreateViewMsg m) { return submitRaw_(std::move(m), Op::ViewPersisted); }
    int64_t submitRawAlter(AlterTopicMsg m) { return submitRaw_(std::move(m), Op::AlterAdd); }
    int64_t submitRawDropTopic(DropTopicMsg m) { return submitRaw_(std::move(m), Op::DropTopic); }
    int64_t submitRawAlterView(AlterViewMsg m) { return submitRaw_(std::move(m), Op::AlterView); }
    /// A ring frame that failed to decode: its own variant alternative.
    int64_t submitRawMalformed(uint16_t templateId, std::string reason) {
        const int64_t corr = nextCorr_(templateId >= kTidDropTopic ? Op::DropTopic : Op::ViewPersisted);
        enqueue_(DdlRequest{MalformedDdl{templateId, rig_->sessionId(), corr, std::move(reason)}});
        return corr;
    }

    // ── driving ─────────────────────────────────────────────────────────────
    /// One admin pass (+ follower 2's fetch unless the HW is held).
    void tick() {
        const uint64_t appended0 = rig_->admin().eventsAppended();
        const bool     wasInFlight = rig_->admin().hasInFlight();
        const uint64_t answered0 = rig_->admin().answeredCount();
        rig_->pump();
        const uint64_t appended = rig_->admin().eventsAppended() - appended0;
        if (appended > 1) violations_.push_back("tick: " + std::to_string(appended) + " events appended in one pass");
        if (appended == 1 && wasInFlight && rig_->admin().answeredCount() == answered0)
            violations_.push_back("tick: an event was appended while another op was in flight");
        if (appended == 1 && !rig_->admin().hasInFlight())
            violations_.push_back("tick: an event was appended but no op is awaiting its commit");
        checkInvariant_("doWork");
    }
    [[nodiscard]] bool quiescent() const {
        return rig_->admin().pendingCount() == 0 && !rig_->admin().hasInFlight();
    }
    /// Tick (releasing held commits) until quiescent — and, while leading, every
    /// appended event committed and applied (the leader serves again).
    void settle(int maxTicks = 400) {
        for (int i = 0; i < maxTicks; ++i) {
            releaseCommits();
            tick();
            if (quiescent() && (!leading_ || (!commitHeld() && worker().isLeader()))) { tick(); return; }
        }
        violations_.push_back("settle: not quiescent after " + std::to_string(maxTicks) + " ticks");
    }
    /// Tick until the in-flight op's event is appended (it then awaits its commit).
    bool tickUntilAppended(int maxTicks = 50) {
        const int64_t start = catalogLogEnd();
        for (int i = 0; i < maxTicks && catalogLogEnd() == start; ++i) tick();
        return catalogLogEnd() > start;
    }

    // ── the HW (ControlMode::Hold = follower 2 stops fetching) ──────────────
    void setControl(ControlMode m) {
        control_ = m;
        if (m == ControlMode::Hold) rig_->silence(kFollower);
        else rig_->unsilence(kFollower);
    }
    /// Follower 2 fetches once at the leader's LEO: the HW covers every appended event.
    void releaseCommits() {
        if (!leading_ || !commitHeld()) return;
        rig_->fetchOnce(kFollower);
    }
    [[nodiscard]] bool holdingCommits() const noexcept { return control_ == ControlMode::Hold; }
    /// A held commit is outstanding: the catalog's HW is behind its LEO.
    [[nodiscard]] bool commitHeld() const {
        return rig_->replica().highWatermark() < rig_->replica().logEndOffset();
    }

    // ── time / leadership / restart ─────────────────────────────────────────
    void advance(int64_t ns) noexcept { rig_->advanceClock(ns); }
    /// The catalog role moves to broker 2: the in-flight DDL is answered NotLeader
    /// at once (spec §5.2), the queued ones on the following ticks.
    void demote() {
        leading_ = false;
        rig_->becomeFollower(kFollower, /*drive=*/false);
        checkInvariant_("demote");
    }
    /// Broker 1 leads again at a new epoch (it catches up, then serves).
    void promote() {
        leading_ = true;
        rig_->assumeLeadership({1, kFollower});
        checkInvariant_("promote");
    }
    /// The ISR shrinks below the catalog's minIsr (a majority): appends are refused.
    void shrinkIsr() {
        rig_->assumeLeadership({1});
        checkInvariant_("shrinkIsr");
    }
    void restoreIsr() { promote(); }
    /// The ISR changes at the SAME epoch (a follower leaves / rejoins it — review
    /// M7): not a leadership change; an op in AwaitingCommit stays in flight.
    void setIsrInPlace(std::vector<int32_t> isr) {
        rig_->updateIsr(std::move(isr));
        checkInvariant_("setIsrInPlace");
    }
    /// A broker restart (only when quiescent): same data dir, same replica set.
    void restart() {
        if (!quiescent()) violations_.push_back("restart requested while not quiescent");
        startedBeforeBoot_  += rig_->admin().startedCount();
        appendedBeforeBoot_ += rig_->admin().eventsAppended();
        countEffects_();
        rig_->restart();
        hookAnswers_();
        bootLeader_();
    }

    // ── checker self-test ───────────────────────────────────────────────────
    /// Feed the invariant checker an answer the AdminWorker did NOT produce (a
    /// duplicate, spurious or misordered DDL answer), then run the post-step check
    /// — proves the checker CAN fail.  Only DdlHarnessRegression's negative tests
    /// call this; a matrix test that calls it is wrong by construction.
    void injectAnswerForCheckerSelfTest(int64_t corr, uint16_t err) {
        CatalogDdlRig::Answer a;
        a.code = err;
        onDdlAnswer_(corr, a);
        checkInvariant_("injected answer");
    }

    // ── observation ─────────────────────────────────────────────────────────
    /// Every live topic and view of the APPLIED catalog, in id order.  Also checks
    /// that the projected registries agree with it (a violation otherwise).
    [[nodiscard]] Catalog catalog() {
        Catalog c;
        const rune::catalog::CatalogState& s = state();
        for (const rune::catalog::TopicObject* t : s.topics()) {
            CatalogEntry e;
            e.id = t->topicId; e.name = t->name; e.partitions = t->partitions;
            e.persistence = t->persistence == rune::catalog::Persistence::Transient ? PersistenceMode::Transient
                                                                                     : PersistenceMode::Disk;
            e.objectVersion = t->version;
            e.fingerprint = rune::catalog::fingerprintOf(*t);
            if (t->schema) {
                e.kind = EntryKind::Typed;
                e.version = t->schema->schemaVersion;
                e.keyField = t->schema->keyField;
                const RegistryEntry* r = schemas().byName(t->name);
                if (r == nullptr || r->topicId != t->topicId || r->version != e.version || r->keyField != e.keyField)
                    violations_.push_back("catalog: the SchemaRegistry disagrees with typed topic '" + t->name + "'");
            } else if (schemas().byName(t->name) != nullptr) {
                violations_.push_back("catalog: untyped topic '" + t->name + "' has a registry schema");
            }
            c.push_back(std::move(e));
        }
        for (const rune::catalog::ViewObject* v : s.views()) {
            CatalogEntry e;
            e.id = v->def.viewTopicId; e.name = v->name; e.kind = EntryKind::View;
            e.persistence = v->def.persistence == rune::catalog::Persistence::Transient ? PersistenceMode::Transient
                                                                                         : PersistenceMode::Disk;
            e.objectVersion = v->version;
            e.partitions = v->def.sinkPartitions;
            e.fingerprint = rune::catalog::fingerprintOf(*v);
            e.keyField = v->def.keyField;
            const RegistryEntry* r = schemas().byName(v->name);
            e.version = r ? r->version : 0;
            const ViewRecord* vr = views().byName(v->name);
            if (vr == nullptr || vr->viewTopicId != e.id || r == nullptr || r->topicId != e.id)
                violations_.push_back("catalog: the registries disagree with view '" + v->name + "'");
            c.push_back(std::move(e));
        }
        if (views().all().size() != s.views().size())
            violations_.push_back("catalog: the ViewRegistry holds a view the catalog does not");
        std::sort(c.begin(), c.end(), [](const CatalogEntry& a, const CatalogEntry& b) { return a.id < b.id; });
        return c;
    }
    [[nodiscard]] std::optional<Response> response(int64_t corr) const {
        for (const Response& r : responses_) if (r.corr == corr) return r;
        return std::nullopt;
    }
    [[nodiscard]] const std::vector<Response>& responses() const noexcept { return responses_; }
    [[nodiscard]] const std::vector<std::string>& violations() const noexcept { return violations_; }
    [[nodiscard]] std::string violationText() const {
        std::string s;
        for (const std::string& v : violations_) s += v + "\n";
        return s;
    }
    /// __catalog events appended by the leader (all boots) — the former
    /// requestCreateTopic count: 0 for a rejection, 1 for an accepted op.
    [[nodiscard]] uint64_t eventsAppended() const { return appendedBeforeBoot_ + rig_->admin().eventsAppended(); }
    /// Worker view installs made LIVE by a committed CREATE VIEW / ALTER VIEW
    /// (the applier's live sink); the boot replay's installs are counted apart.
    [[nodiscard]] int viewPlanPushes() const { return livePushesBefore_ + countLive_("viewInstalled:"); }
    /// Recovery installs: one per live view on every boot replay (a restart).  A
    /// leadership flap re-installs nothing (every broker applies once — Task 7).
    [[nodiscard]] int recoveryViewInstalls() const { return bootPushesBefore_ + countBoot_("viewInstalled:"); }
    /// Validator fan-outs (schemaChanged) posted live.
    [[nodiscard]] int validatorPushes() const { return validatorsBefore_ + countLive_("schemaChanged:"); }
    [[nodiscard]] int64_t catalogLogEnd() { return rig_->replica().logEndOffset(); }
    /// Live topics + views.
    [[nodiscard]] int32_t topicCount() const {
        return static_cast<int32_t>(state().topics().size() + state().views().size());
    }
    /// The topic / view-sink id `name` names (-1 = none).
    [[nodiscard]] int32_t idOf(const std::string& name) const {
        if (const rune::catalog::TopicObject* t = state().topicByName(name)) return t->topicId;
        if (const rune::catalog::ViewObject* v = state().viewByName(name)) return v->def.viewTopicId;
        return -1;
    }
    [[nodiscard]] uint64_t startedTotal() const { return startedBeforeBoot_ + rig_->admin().startedCount(); }
    [[nodiscard]] int64_t now() const noexcept { return rig_->now(); }
    [[nodiscard]] const std::filesystem::path& root() const noexcept { return rig_->root(); }
    [[nodiscard]] std::string catalogDir() const { return rig_->catalogDir(); }
    [[nodiscard]] AdminWorker& worker() { return rig_->admin(); }
    [[nodiscard]] const rune::catalog::CatalogState& state() const { return rig_->admin().applier().state(); }
    [[nodiscard]] const SchemaRegistry& schemas() const { return rig_->schemas(); }
    [[nodiscard]] const ViewRegistry& views() const { return rig_->views(); }
    [[nodiscard]] CatalogDdlRig& rig() noexcept { return *rig_; }

private:
    template <class M>
    int64_t submitRaw_(M m, Op op) {
        const int64_t corr = nextCorr_(op);
        m.session = rig_->sessionId(); m.corr = corr;
        if (m.meta.requestId.none()) m.meta = rig_->meta(m.meta.statement);
        enqueue_(DdlRequest{std::move(m)});
        return corr;
    }
    void enqueue_(DdlRequest req) { (void)rig_->enqueue(std::move(req)); }
    int64_t nextCorr_(Op op) {
        const int64_t corr = rig_->nextCorr();
        opByCorr_[corr] = op;
        return corr;
    }

    void hookAnswers_() {
        answeredSinceBoot_ = 0;
        refusedSeen_ = 0;
        rig_->onAnswerRecorded([this](int64_t corr, const CatalogDdlRig::Answer& a) { onDdlAnswer_(corr, a); });
    }
    /// The leadership, then (Hold) one follower fetch so the rebooted leader learns
    /// the committed HW (the follower holds every appended event), then driven
    /// until it serves.
    void bootLeader_() {
        leading_ = true;
        if (control_ == ControlMode::Hold) rig_->silence(kFollower);
        else rig_->unsilence(kFollower);
        rig_->assumeLeadership({1, kFollower});
        for (int i = 0; i < CatalogDdlRig::kCap && !rig_->admin().isLeader(); ++i) {
            rig_->fetchOnce(kFollower);
            rig_->pump();
        }
        if (!rig_->admin().isLeader())
            violations_.push_back("boot: the catalog leader never served DDL (" +
                                  std::string(rig_->admin().roleName()) + ")");
        checkInvariant_("boot");
    }

    void onDdlAnswer_(int64_t corr, const CatalogDdlRig::Answer& a) {
        const auto it = opByCorr_.find(corr);
        const Op op = it == opByCorr_.end() ? Op::DefineDisk : it->second;
        Response r{op, corr, a.topicId, a.version, a.code, a.detail, a.objectId};
        if (a.kind == CatalogDdlRig::Answer::Kind::Ddl) r.id = a.gateTopicId;
        if (a.kind == CatalogDdlRig::Answer::Kind::CreateView) r.version = a.code == kOk ? 1u : 0u;   // v1 output
        if (rig_->admin().pendingDdlRefused() > refusedSeen_) {
            // No-hang: a submit beyond kMaxPendingDdl is answered Internal at once,
            // never started — outside the started/answered accounting and ahead of
            // the ops still queued (not a FIFO violation).
            ++refusedSeen_;
            responses_.push_back(std::move(r));
            return;
        }
        const int64_t unanswered = unanswered_();
        if (unanswered != 1)
            violations_.push_back("answer for corr " + std::to_string(corr) + " with " +
                                  std::to_string(unanswered) + " ops started-but-unanswered (want 1)");
        if (corr <= lastDdlCorr_)
            violations_.push_back("answer for corr " + std::to_string(corr) + " out of FIFO order (previous " +
                                  std::to_string(lastDdlCorr_) + ")");
        lastDdlCorr_ = corr;
        ++answeredSinceBoot_;
        responses_.push_back(std::move(r));
    }

    [[nodiscard]] int64_t unanswered_() const {
        return static_cast<int64_t>(rig_->admin().startedCount()) - static_cast<int64_t>(answeredSinceBoot_);
    }
    void checkInvariant_(const char* where) {
        const int64_t unanswered = unanswered_();
        const int64_t inFlight   = rig_->admin().hasInFlight() ? 1 : 0;
        if (unanswered != inFlight)
            violations_.push_back(std::string(where) + ": " + std::to_string(unanswered) +
                                  " DDL ops started-but-unanswered, in flight " + std::to_string(inFlight));
    }

    [[nodiscard]] int countLive_(const char* prefix) const {
        int n = 0;
        for (const std::string& e : rig_->effects()) n += e.starts_with(prefix) ? 1 : 0;
        return n;
    }
    [[nodiscard]] int countBoot_(const char* prefix) const {
        int n = 0;
        for (const std::string& e : rig_->bootEffects()) n += e.starts_with(prefix) ? 1 : 0;
        return n;
    }
    void countEffects_() {
        livePushesBefore_ += countLive_("viewInstalled:");
        bootPushesBefore_ += countBoot_("viewInstalled:");
        validatorsBefore_ += countLive_("schemaChanged:");
    }

    DdlHarnessOptions              opt_;
    std::unique_ptr<CatalogDdlRig> rig_;
    ControlMode                    control_ = ControlMode::Auto;
    bool                           leading_ = false;
    std::vector<Response>          responses_;
    std::map<int64_t, Op>          opByCorr_;
    std::vector<std::string>       violations_;
    int64_t  lastDdlCorr_ = 0;
    uint64_t answeredSinceBoot_ = 0;
    uint64_t refusedSeen_ = 0;          // queue-full refusals already accounted (kMaxPendingDdl)
    uint64_t startedBeforeBoot_ = 0;
    uint64_t appendedBeforeBoot_ = 0;
    int      livePushesBefore_ = 0, bootPushesBefore_ = 0, validatorsBefore_ = 0;
};

} // namespace rune::node::test
