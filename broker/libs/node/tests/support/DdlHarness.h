#pragma once
// DdlHarness.h — DDL conformance plan: the conformance harness.
//
// Extends AdminWorkerTest's fake-control harness pattern (a real __topic_schemas
// replica + an AdminWorker whose control thread is played by the test) with:
//   * the __views replica + ViewRegistry (CREATE VIEW);
//   * a REAL TopicRegistry answered by createTopicForAdmin — BrokerNode's own
//     control-thread function (idempotent by name, authoritative count/mode);
//   * control replies the test can hold, fail (before/after creating) or deliver
//     late (a stale ticket after a timeout / demotion);
//   * commits the test can hold: replica set {1,2}, the HW waits for follower 2's
//     fetch (releaseCommits);
//   * demote / promote, and a restart that boots like BrokerNode
//     (recoverRuntimeTopicSpecs -> fresh TopicRegistry -> fresh registries ->
//     promotion replays __topic_schemas and __views).
//
// After EVERY admin step it checks the serialization invariant: started-but-
// unanswered DDL ops == (hasInFlight() ? 1 : 0); no requestCreateTopic while an op
// is in flight; DDL answers in FIFO submission order.  Violations are recorded,
// never thrown: every test asserts violations().empty().
//
// restart() runs recoverRuntimeTopics + reconcileTopicDirs exactly as
// BrokerNode::start does, then reboots with the SAME replica set.  Single-node
// ({1}): promotion replays the catalogs up to the HW, which a single-node leader
// advances to its LEO.  Multi-node ({1,2}, holdCommits): the rebooted node first
// follows node 2 (which holds every appended record) and learns the committed HW
// from one empty fetch answer, then is promoted — promotion replays up to that HW,
// and later commits are held again until releaseCommits().
//
// Single-threaded, test-only.  Must NOT be included from production code.
#include "DdlExpectations.h"

#include <rune/gateway/TopicRegistry.h>
#include <rune/node/AdminChannels.h>
#include <rune/node/AdminCreateTopic.h>
#include <rune/node/AdminWorker.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/PersistenceMode.h>
#include <rune/node/RuntimeTopicRecovery.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/ViewRegistry.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/Log.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/view/ViewPlan.h>

#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

namespace rune::node::test {

// ── payloads ────────────────────────────────────────────────────────────────
/// Every DefineTopic's descriptor: {id Int64 (key), hp Int64}.
inline std::vector<uint8_t> charBlob() {
    return rune::codec::serialize(rune::codec::DescriptorBuilder("character", 1)
        .add("id", rune::codec::PrimType::Int64, 1, /*isKey=*/true)
        .add("hp", rune::codec::PrimType::Int64)
        .build());
}

/// Authored {id, hp} Replace view keyed by "id" over `srcTopicId`.
inline std::vector<uint8_t> viewPlanBlob(const std::string& viewName, int32_t srcTopicId,
                                         uint8_t persistence) {
    namespace rv = rune::view;
    rv::ViewPlan p;
    p.viewName = viewName;
    p.key = "id";
    p.persistence = persistence;
    p.sources.push_back(rv::SourceSpec{srcTopicId, "id"});
    p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::Int64, 0, "id", rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"hp", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
    return rv::serializeAuthored(p);
}

/// The ALTER ops blob for `op` (uniq makes every ADD's field name fresh).
inline std::vector<uint8_t> alterOpsFor(Op op, uint64_t uniq) {
    rune::codec::AlterOp o{};
    switch (op) {
        case Op::AlterDrop:
            o.type = rune::codec::AlterOpType::Drop; o.name = "hp"; break;
        case Op::AlterRename:
            o.type = rune::codec::AlterOpType::Rename; o.name = "hp"; o.newName = "health"; break;
        case Op::AlterRenameKey:   // the key field: TopicSchema.keyField must follow (B8)
            o.type = rune::codec::AlterOpType::Rename; o.name = "id"; o.newName = "pk"; break;
        case Op::AlterSetCompat:
            o.type = rune::codec::AlterOpType::SetCompatibility; o.mode = rune::codec::CompatMode::Forward; break;
        default:
            o.type = rune::codec::AlterOpType::Add; o.name = "extra" + std::to_string(uniq);
            o.primType = rune::codec::PrimType::Int64; o.fixedLen = 0; break;
    }
    return rune::codec::encodeAlterOps({o});
}

/// Authored {id, hp} Replace view keyed by "id" over EVERY id in `srcTopicIds`
/// (output fields read source 0).  Phase 1 views are single-source: the compiler
/// rejects 2+ sources fail-closed (ViewFieldUnresolved) AFTER the per-source checks.
inline std::vector<uint8_t> viewPlanBlob(const std::string& viewName, const std::vector<int32_t>& srcTopicIds,
                                         uint8_t persistence) {
    namespace rv = rune::view;
    rv::ViewPlan p;
    p.viewName = viewName;
    p.key = "id";
    p.persistence = persistence;
    for (const int32_t id : srcTopicIds) p.sources.push_back(rv::SourceSpec{id, "id"});
    p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::Int64, 0, "id", rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"hp", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
    return rv::serializeAuthored(p);
}

// ── catalog snapshot ────────────────────────────────────────────────────────
enum class EntryKind : uint8_t { SchemaLess, Typed, View };

/// One TopicRegistry id with what the admin catalogs say about it.
struct CatalogEntry {
    int32_t         id = -1;
    std::string     name;
    EntryKind       kind = EntryKind::SchemaLess;
    PersistenceMode persistence = PersistenceMode::Disk;
    uint16_t        version = 0;
    int32_t         partitions = 0;
    uint64_t        fingerprint = 0;
    std::string     keyField;   // the schema's key field ("" when schema-less)
    bool operator==(const CatalogEntry&) const = default;
};
using Catalog = std::vector<CatalogEntry>;   // in topic-id order

inline void PrintTo(const CatalogEntry& e, std::ostream* os) {
    *os << "{id=" << e.id << " name='" << e.name << "' kind="
        << (e.kind == EntryKind::Typed ? "Typed" : e.kind == EntryKind::View ? "View" : "SchemaLess")
        << " persistence=" << (e.persistence == PersistenceMode::Transient ? "Transient" : "Disk")
        << " v" << e.version << " parts=" << e.partitions << " fp=" << e.fingerprint
        << " key='" << e.keyField << "'}";
}

[[nodiscard]] inline const CatalogEntry* findEntry(const Catalog& c, const std::string& name) {
    for (const CatalogEntry& e : c) if (e.name == name) return &e;
    return nullptr;
}

struct Response { Op op; int64_t corr; int32_t id; uint16_t version; uint16_t err; };

enum class ControlMode : uint8_t { Auto, Hold };

struct DdlHarnessOptions {
    std::vector<TopicSpec> statics;   // RUNE_TOPICS-style boot topics (schema-less), ids 0..
    bool holdCommits = false;         // replica set {1,2}: commits wait for releaseCommits()
    bool withSources = true;          // pre-define "src" and "src2" (typed, disk, 1 partition)
};

class DdlHarness {
public:
    explicit DdlHarness(DdlHarnessOptions o = {}) : opt_(std::move(o)), root_(makeRoot_()) {
        boot_();
        if (opt_.withSources) {
            for (const char* n : {"src", "src2"}) {
                const int64_t c = submit(Op::DefineDisk, n);
                settle();
                const std::optional<Response> r = response(c);
                if (!r || r->err != kOk) violations_.push_back(std::string("setup: define ") + n + " failed");
            }
        }
    }
    ~DdlHarness() {
        teardown_();
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }
    DdlHarness(const DdlHarness&) = delete;
    DdlHarness& operator=(const DdlHarness&) = delete;

    // ── submit (returns the op's correlation id) ────────────────────────────
    int64_t submit(Op op, const std::string& name, const std::string& viewSource = "src") {
        if (isView(op)) return submitView(op, name, {viewSource});
        const int64_t corr = nextCorr_(op);
        switch (op) {
            case Op::DefineDisk:
            case Op::DefineTransient: {
                DefineTopicMsg m;
                m.session = 1; m.corr = corr; m.partitionCount = 1; m.validate = false;
                m.name = name; m.keyField = "id"; m.blob = charBlob();
                m.persistence = op == Op::DefineTransient ? 1 : 0;
                w_->submitDefineTopic(std::move(m));
                break;
            }
            case Op::GetSchema: {
                GetSchemaMsg m;
                m.session = 1; m.corr = corr; m.name = name;
                w_->submitGetSchema(m);   // answered synchronously (a pure read)
                break;
            }
            default: {
                AlterTopicMsg m;
                m.session = 1; m.corr = corr; m.topicName = name;
                if (op == Op::AlterCasCurrent) {
                    const RegistryEntry* e = schemas_->byName(name);
                    m.expectedVersion = e ? e->version : 1;   // read at SUBMIT time
                } else if (op == Op::AlterCasStale) {
                    m.expectedVersion = 99;
                }
                m.opsBlob = alterOpsFor(op, static_cast<uint64_t>(corr));
                w_->submitAlterTopic(std::move(m));
                break;
            }
        }
        return corr;
    }
    /// CREATE VIEW over several sources, each resolved name -> topic id at SUBMIT
    /// time exactly like submit() (an unknown name -> id -1).
    int64_t submitView(Op op, const std::string& name, const std::vector<std::string>& sources) {
        const int64_t corr = nextCorr_(op);
        CreateViewMsg m;
        m.session = 1; m.corr = corr; m.persistence = op == Op::ViewTransient ? 1 : 0;
        m.viewName = name; m.keyField = "id";
        std::vector<int32_t> ids;
        for (const std::string& src : sources) {
            const RegistryEntry* e = schemas_->byName(src);
            ids.push_back(e ? e->topicId : -1);
        }
        m.planBlob = viewPlanBlob(name, ids, m.persistence);
        w_->submitCreateView(std::move(m));
        return corr;
    }
    /// Malformed / hand-built requests: the harness only stamps session + corr.
    int64_t submitRawDefine(DefineTopicMsg m) {
        const int64_t corr = nextCorr_(Op::DefineDisk);
        m.session = 1; m.corr = corr;
        w_->submitDefineTopic(std::move(m));
        return corr;
    }
    int64_t submitRawView(CreateViewMsg m) {
        const int64_t corr = nextCorr_(Op::ViewPersisted);
        m.session = 1; m.corr = corr;
        w_->submitCreateView(std::move(m));
        return corr;
    }
    int64_t submitRawAlter(AlterTopicMsg m) {
        const int64_t corr = nextCorr_(Op::AlterAdd);
        m.session = 1; m.corr = corr;
        w_->submitAlterTopic(std::move(m));
        return corr;
    }

    // ── driving ─────────────────────────────────────────────────────────────
    void tick() {
        now_ += 1'000'000;
        w_->doWork(now_);
        checkInvariant_("doWork");
        while (!committedTopics_.empty()) {          // the control thread drains the ring
            (void)topics_->markTopicCommitted(committedTopics_.front());
            committedTopics_.pop_front();
        }
        if (control_ == ControlMode::Auto)
            while (!creates_.empty()) deliverCreate();
    }
    [[nodiscard]] bool quiescent() const {
        return w_->pendingCount() == 0 && !w_->hasInFlight() && creates_.empty();
    }
    /// Tick (releasing held commits) until quiescent.  Never call with a held
    /// create request outstanding — that is not quiescent by construction.
    void settle(int maxTicks = 400) {
        for (int i = 0; i < maxTicks; ++i) {
            releaseCommits();
            tick();
            if (quiescent()) { tick(); return; }
        }
        violations_.push_back("settle: not quiescent after " + std::to_string(maxTicks) + " ticks");
    }
    bool tickUntilCreatePending(int maxTicks = 50) {
        for (int i = 0; i < maxTicks && creates_.empty(); ++i) tick();
        return !creates_.empty();
    }
    bool tickUntilAppended(int maxTicks = 50) {
        const int64_t start = schemaLogEnd() + viewsLogEnd();
        for (int i = 0; i < maxTicks && schemaLogEnd() + viewsLogEnd() == start; ++i) tick();
        return schemaLogEnd() + viewsLogEnd() > start;
    }

    // ── the simulated control thread ────────────────────────────────────────
    void setControl(ControlMode m) noexcept { control_ = m; }
    [[nodiscard]] bool hasPendingCreate() const noexcept { return !creates_.empty(); }
    /// Answer the oldest create request exactly as BrokerNode's control thread does.
    /// A request whose op already ended (timeout / demotion) is a stale ticket: the
    /// worker ignores the reply, but the topic IS created (an orphan).
    void deliverCreate() {
        const CreateTopicReqMsg req = popCreate_();
        w_->onCreateTopicReply(createTopicForAdmin(*topics_, req));
        checkInvariant_("create reply");
    }
    /// Answer the oldest create request with a failure.  afterCreate=true runs the
    /// control thread's createTopic first (the reply is lost after the work: an orphan).
    void failCreate(bool afterCreate) {
        const CreateTopicReqMsg req = popCreate_();
        CreateTopicReplyMsg reply;
        if (afterCreate) reply = createTopicForAdmin(*topics_, req);
        reply.ticket  = req.ticket;
        reply.topicId = -1;
        reply.existed = false;
        reply.caseConflict = false;   // B3: a failed create is Internal, never TopicExists
        w_->onCreateTopicReply(reply);
        checkInvariant_("failed create reply");
    }
    /// Answer the oldest create request like the control thread, but with the reply's
    /// persistence byte replaced by `persistence` (malformed-reply cells).
    void deliverCreateWithPersistence(uint8_t persistence) {
        const CreateTopicReqMsg req = popCreate_();
        CreateTopicReplyMsg reply = createTopicForAdmin(*topics_, req);
        reply.persistence = persistence;
        w_->onCreateTopicReply(reply);
        checkInvariant_("malformed create reply");
    }

    // ── commits / time / leadership / restart ───────────────────────────────
    /// holdCommits: follower 2 fetches at the leader's LEO -> the HW covers every
    /// appended record of both catalogs.
    void releaseCommits() {
        if (!opt_.holdCommits || !leader_) return;
        release_(*schemaReplica_, kSchemaTopicId);
        release_(*viewsReplica_, kViewsTopicId);
    }
    [[nodiscard]] bool holdingCommits() const noexcept { return opt_.holdCommits; }
    /// A held commit is outstanding: some catalog's HW is behind its LEO (the
    /// in-flight op, if any, is parked on the HW gate until releaseCommits()).
    [[nodiscard]] bool commitHeld() const noexcept {
        return schemaReplica_->highWatermark() < schemaReplica_->logEndOffset() ||
               viewsReplica_->highWatermark() < viewsReplica_->logEndOffset();
    }
    void advance(int64_t ns) noexcept { now_ += ns; }
    void demote() {
        ++epoch_;
        leader_ = false;
        w_->onRole(AdminRoleMsg{/*leader=*/false, epoch_, /*leaderId=*/2});
        checkInvariant_("demote");
    }
    void promote() {
        ++epoch_;
        leader_ = true;
        w_->onRole(AdminRoleMsg{/*leader=*/true, epoch_, /*leaderId=*/1});
        checkInvariant_("promote");
    }
    /// A broker restart (only when quiescent): reboot from the same dataDir with the
    /// same replica set ({1,2} stays {1,2}: commits can be held after a restart).
    void restart() {
        if (!quiescent()) violations_.push_back("restart requested while not quiescent");
        startedBeforeBoot_ += w_->startedCount();
        teardown_();
        boot_(/*reboot=*/true);
    }

    // ── checker self-test ───────────────────────────────────────────────────
    /// Feed the invariant checker an answer the AdminWorker did NOT produce (a
    /// duplicate, spurious or misordered DDL answer), then run the post-step check
    /// — proves the checker CAN fail.  Only DdlHarnessRegression's negative tests
    /// call this; a matrix test that calls it is wrong by construction.
    void injectAnswerForCheckerSelfTest(int64_t corr, uint16_t err) {
        onDdlAnswer_(corr, -1, 0, err);
        checkInvariant_("injected answer");
    }

    // ── observation ─────────────────────────────────────────────────────────
    [[nodiscard]] Catalog catalog() const {
        Catalog c;
        for (int32_t id = 0; id < topics_->topicCount(); ++id) {
            const TopicSpec* s = topics_->topic(id);
            CatalogEntry e;
            e.id = id; e.name = s->name; e.partitions = s->partitionCount; e.persistence = s->persistence;
            if (const ViewRecord* v = views_->byViewTopicId(id)) {
                e.kind = EntryKind::View;
                e.persistence = v->persistence == 1 ? PersistenceMode::Transient : PersistenceMode::Disk;
                if (const RegistryEntry* r = schemas_->byId(id)) {
                    e.version = r->version; e.fingerprint = r->fingerprint; e.keyField = r->keyField;
                }
            } else if (const RegistryEntry* r = schemas_->byId(id)) {
                e.kind = EntryKind::Typed;
                e.persistence = r->persistence;
                e.version = r->version;
                e.partitions = r->partitionCount;
                e.fingerprint = r->fingerprint;
                e.keyField = r->keyField;
            }
            c.push_back(std::move(e));
        }
        return c;
    }
    /// What a restart must produce from `before`: committed entries and static
    /// topics unchanged; a schema-less RUNTIME topic (an orphan) becomes the
    /// zero-partition placeholder __orphan.<id> when a later recorded topic exists
    /// (B5), and is gone when it is the tail.  On disk (B10): a placeholder's dir
    /// is deleted, a tail orphan's dir is quarantined (never deleted).
    [[nodiscard]] Catalog expectedAfterRestart(const Catalog& before) const {
        const int32_t firstRuntime = static_cast<int32_t>(opt_.statics.size());
        int32_t lastRecorded = -1;
        for (const CatalogEntry& e : before)
            if (e.id >= firstRuntime && e.kind != EntryKind::SchemaLess) lastRecorded = e.id;
        Catalog out;
        for (const CatalogEntry& e : before) {
            if (e.id < firstRuntime || e.kind != EntryKind::SchemaLess) { out.push_back(e); continue; }
            if (e.id > lastRecorded) continue;
            CatalogEntry p;
            p.id = e.id; p.name = orphanPlaceholderName(e.id); p.kind = EntryKind::SchemaLess;
            p.persistence = PersistenceMode::Transient; p.partitions = 0;   // hidden, inert (D1)
            out.push_back(std::move(p));
        }
        return out;
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
    [[nodiscard]] int createRequests() const noexcept { return createRequests_; }
    /// Worker view installs made by a committed CREATE VIEW (restart-aware: the
    /// recovery installs a promotion fans out are counted apart, below).
    [[nodiscard]] int viewPlanPushes() const noexcept { return viewPlanPushes_; }
    /// Recovery installs (ViewPlanUpdateMsg::recovery): one per recovered view on
    /// every promotion edge — a boot / restart or a demote + promote flap.
    [[nodiscard]] int recoveryViewInstalls() const noexcept { return recoveryViewInstalls_; }
    [[nodiscard]] int validatorPushes() const noexcept { return validatorPushes_; }
    [[nodiscard]] int64_t schemaLogEnd() const { return schemaLog_->logEndOffset(); }
    [[nodiscard]] int64_t viewsLogEnd() const { return viewsLog_->logEndOffset(); }
    [[nodiscard]] int32_t topicCount() const { return topics_->topicCount(); }
    [[nodiscard]] int32_t idOf(const std::string& name) const { return topics_->topicIdForName(name).value_or(-1); }
    [[nodiscard]] uint64_t startedTotal() const noexcept { return startedBeforeBoot_ + w_->startedCount(); }
    [[nodiscard]] int64_t now() const noexcept { return now_; }
    [[nodiscard]] const std::filesystem::path& root() const noexcept { return root_; }
    AdminWorker& worker() { return *w_; }
    SchemaRegistry& schemas() { return *schemas_; }
    ViewRegistry& views() { return *views_; }
    rune::gateway::TopicRegistry& topics() { return *topics_; }

private:
    using PartitionReplica = rune::replication::PartitionReplica;

    static std::filesystem::path makeRoot_() {
        static uint64_t seq = 0;   // single-threaded test binary
        const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        auto p = std::filesystem::temp_directory_path() /
                 ("rune_ddl_" + std::to_string(::getpid()) + "_" + std::to_string(seq++) + "_" + std::to_string(ts));
        std::filesystem::create_directories(p);
        return p;
    }

    rune::replication::PartitionConfig cfg_(int32_t topicId) const {
        rune::replication::PartitionConfig c;
        c.topicId = topicId; c.partitionId = 0; c.selfId = 1; c.minIsr = 1;
        c.replicaLagMaxMs = 1'000'000'000LL;     // follower 2 never leaves the ISR on simulated time
        c.replicaLagMaxOffset = 1'000'000;
        c.fetchIntervalNs = 1'000'000LL; c.isrCheckIntervalNs = 1'000'000LL;
        c.maxFetchBytes = 1 << 20; c.pushOnAppend = false;
        c.cleanupPolicy = rune::replication::CleanupPolicy::Compact;
        return c;
    }

    void boot_(bool reboot = false) {
        std::vector<TopicSpec> specs = opt_.statics;
        RecoveredRuntimeTopics recovered =
            recoverRuntimeTopics(root_.string(), static_cast<int32_t>(opt_.statics.size()));
        for (TopicSpec& s : recovered.specs) specs.push_back(std::move(s));
        // B10: exactly as BrokerNode::start boots — placeholder ids are structural,
        // a damaged catalog disables every delete.
        (void)reconcileTopicDirs(root_.string(), specs, recovered.placeholderIds,
                                 std::chrono::duration_cast<std::chrono::seconds>(
                                     std::chrono::system_clock::now().time_since_epoch()).count(),
                                 recovered.catalogDamage);
        topics_ = std::make_unique<rune::gateway::TopicRegistry>(specs, root_.string());

        std::filesystem::create_directories(root_ / "__topic_schemas");
        std::filesystem::create_directories(root_ / "__views");
        schemaLog_ = std::make_unique<rune::storage::Log>((root_ / "__topic_schemas").string());
        schemaLog_->recover();
        viewsLog_ = std::make_unique<rune::storage::Log>((root_ / "__views").string());
        viewsLog_->recover();

        using rune::replication::NodeId;
        const std::vector<NodeId> set = opt_.holdCommits ? std::vector<NodeId>{1, 2} : std::vector<NodeId>{1};
        schemaReplica_ = std::make_unique<PartitionReplica>(cfg_(kSchemaTopicId), rune::replication::Role::Follower,
                                                            0, set, set, schemaLog_.get(), &schemaTransport_);
        viewsReplica_ = std::make_unique<PartitionReplica>(cfg_(kViewsTopicId), rune::replication::Role::Follower,
                                                           0, set, set, viewsLog_.get(), &viewsTransport_);
        schemas_ = std::make_unique<SchemaRegistry>();
        views_   = std::make_unique<ViewRegistry>();

        AdminDeps d;
        d.requestCreateTopic = [this](int64_t ticket, int32_t parts, const std::string& name,
                                      const std::string& keyField, uint8_t cleanupPolicy,
                                      uint8_t persistence) -> bool {
            if (w_->hasInFlight())
                violations_.push_back("requestCreateTopic('" + name + "') while another op is in flight");
            CreateTopicReqMsg m;
            m.ticket = ticket; m.partitionCount = parts; m.name = name; m.keyField = keyField;
            m.cleanupPolicy = cleanupPolicy; m.persistence = persistence;
            creates_.push_back(std::move(m));
            ++createRequests_;
            return true;
        };
        d.pushValidators = [this](const RegistryEntry&, int32_t) { ++validatorPushes_; };
        d.pushViewPlan   = [this](const ViewPlanUpdateMsg& vp) {
            if (vp.recovery) ++recoveryViewInstalls_; else ++viewPlanPushes_;
        };
        // B10 / D4: the admin->control kA2cTopicCommitted hand-off; the simulated
        // control thread rewrites the dir marker pending -> committed in tick().
        d.topicCommitted = [this](int32_t topicId) { committedTopics_.push_back(topicId); };
        d.respondDefine = [this](int64_t, int64_t corr, int32_t id, uint16_t v, uint16_t e) {
            onDdlAnswer_(corr, id, v, e);
        };
        d.respondCreateView = [this](int64_t, int64_t corr, int32_t id, uint64_t, uint16_t e) {
            onDdlAnswer_(corr, id, e == kOk ? 1 : 0, e);   // a view's output is v1
        };
        d.respondAlter = [this](int64_t, int64_t corr, int32_t id, uint16_t v, uint16_t e) {
            onDdlAnswer_(corr, id, v, e);
        };
        d.respondGet = [this](int64_t, int64_t corr, int32_t id, uint16_t v, int32_t,
                              const std::vector<uint8_t>&, uint16_t e) {
            responses_.push_back(Response{Op::GetSchema, corr, id, v, e});
        };
        std::vector<int32_t> rs(set.begin(), set.end());
        w_ = std::make_unique<AdminWorker>(*schemas_, schemaReplica_.get(), /*selfId=*/1, std::move(rs),
                                           std::move(d), viewsReplica_.get(), views_.get());
        answeredSinceBoot_ = 0;
        refusedSeen_ = 0;
        creates_.clear();
        committedTopics_.clear();
        if (reboot && opt_.holdCommits) learnCommittedHwFromPeer_();
        promote();
    }

    /// Multi-node reboot: follow node 2 (it holds every appended record — a restart
    /// happens only when quiescent) and learn the committed HW from one empty fetch
    /// answer, so the promotion that follows replays the catalogs up to it.
    void learnCommittedHwFromPeer_() {
        ++epoch_;
        for (auto [r, topicId] : {std::pair{schemaReplica_.get(), kSchemaTopicId},
                                  std::pair{viewsReplica_.get(), kViewsTopicId}}) {
            r->becomeFollower(static_cast<rune::replication::Epoch>(epoch_), /*leaderId=*/2);
            rune::replication::FetchResponseMsg resp{};
            resp.leaderEpoch = static_cast<rune::replication::Epoch>(epoch_);
            resp.partition = 0;
            resp.highWatermark = r->logEndOffset();
            resp.logStartOffset = 0;
            resp.errorCode = rune::replication::kErrNone;
            resp.topicId = topicId;
            r->applyReplicated(resp);
            if (r->highWatermark() != r->logEndOffset())
                violations_.push_back("reboot: follower did not learn the committed HW (topic " +
                                      std::to_string(topicId) + ")");
        }
    }

    void teardown_() {
        w_.reset();
        schemaReplica_.reset();
        viewsReplica_.reset();
        schemaLog_.reset();
        viewsLog_.reset();
        topics_.reset();
        schemas_.reset();
        views_.reset();
    }

    void release_(PartitionReplica& r, int32_t topicId) {
        const rune::replication::Offset leo = r.logEndOffset();
        if (r.highWatermark() >= leo) return;
        (void)r.onFetchRequest(rune::replication::FetchRequestMsg{
                                   static_cast<rune::replication::Epoch>(epoch_), 0, /*followerId=*/2, leo,
                                   1 << 20, topicId},
                               now_);
    }

    int64_t nextCorr_(Op op) {
        const int64_t corr = ++corr_;
        opByCorr_[corr] = op;
        return corr;
    }

    CreateTopicReqMsg popCreate_() {
        CreateTopicReqMsg req = creates_.front();
        creates_.pop_front();
        return req;
    }

    void onDdlAnswer_(int64_t corr, int32_t id, uint16_t version, uint16_t err) {
        const auto it = opByCorr_.find(corr);
        const Op op = it == opByCorr_.end() ? Op::DefineDisk : it->second;
        if (w_->pendingDdlRefused() > refusedSeen_) {
            // No-hang fix round 3: a submit beyond kMaxPendingDdl is answered Internal
            // at once, never started — outside the started/answered accounting and
            // ahead of the ops still queued (not a FIFO violation).
            ++refusedSeen_;
            responses_.push_back(Response{op, corr, id, version, err});
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
        responses_.push_back(Response{op, corr, id, version, err});
    }

    [[nodiscard]] int64_t unanswered_() const noexcept {
        return static_cast<int64_t>(w_->startedCount()) - static_cast<int64_t>(answeredSinceBoot_);
    }

    void checkInvariant_(const char* where) {
        const int64_t unanswered = unanswered_();
        const int64_t inFlight   = w_->hasInFlight() ? 1 : 0;
        if (unanswered != inFlight)
            violations_.push_back(std::string(where) + ": " + std::to_string(unanswered) +
                                  " DDL ops started-but-unanswered, in flight " + std::to_string(inFlight));
    }

    DdlHarnessOptions        opt_;
    std::filesystem::path    root_;
    rune::replication::NullPartitionTransport schemaTransport_, viewsTransport_;
    std::unique_ptr<rune::gateway::TopicRegistry> topics_;
    std::unique_ptr<rune::storage::Log>           schemaLog_, viewsLog_;
    std::unique_ptr<PartitionReplica>             schemaReplica_, viewsReplica_;
    std::unique_ptr<SchemaRegistry>               schemas_;
    std::unique_ptr<ViewRegistry>                 views_;
    std::unique_ptr<AdminWorker>                  w_;

    ControlMode                   control_ = ControlMode::Auto;
    std::deque<CreateTopicReqMsg> creates_;
    std::deque<int32_t>           committedTopics_;   // admin -> control topicCommitted hand-off
    std::vector<Response>         responses_;
    std::map<int64_t, Op>         opByCorr_;
    std::vector<std::string>      violations_;
    int64_t  now_ = 0;
    int32_t  epoch_ = 0;
    bool     leader_ = false;
    int64_t  corr_ = 0;
    int64_t  lastDdlCorr_ = 0;
    uint64_t answeredSinceBoot_ = 0;
    uint64_t refusedSeen_ = 0;          // queue-full refusals already accounted (kMaxPendingDdl)
    uint64_t startedBeforeBoot_ = 0;
    int      createRequests_ = 0, viewPlanPushes_ = 0, recoveryViewInstalls_ = 0, validatorPushes_ = 0;
};

} // namespace rune::node::test
