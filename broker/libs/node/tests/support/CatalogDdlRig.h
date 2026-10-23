#pragma once
// CatalogDdlRig.h — catalog history Task 9: one AdminWorker driving a real
// __catalog leader replica, its CatalogApplier and recording deps.  Reused by
// Tasks 10 (the DDL conformance port), 11 (leadership) and 20 (the matrix).
//
//   * a real disk Log under a temp dir (`<tmp>/__catalog`, as dataDir/__catalog)
//     and a PartitionReplica over it with replicas {1..brokers}; self = 1;
//   * the AdminWorker owns the applier (its boot replay runs in the ctor, live);
//   * becomeServingLeader(isr): the catalog role (leader, ISR) → pumped until the
//     worker serves DDL;
//   * followers: every ISR follower that is not SILENT fetches at the leader's LEO
//     on each pump (so an append commits on the next pass); silence(id) /
//     resumeFollower(id) control that — an event held at the HW;
//   * a deterministic clock (advanceClockPast(ns)); every drive loop is capped
//     and fails loudly rather than hanging;
//   * the answers (by correlation id) and recordedLog() — the events the log holds.
//
// Single-threaded test fixture: plain members, no atomics.

#include "support/TestNodeConfig.h"
#include <rune/node/AdminChannels.h>
#include <rune/node/AdminWorker.h>
#include <rune/node/CatalogApplier.h>
#include <rune/node/CatalogPartition.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/ViewRegistry.h>

#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/view/ViewPlan.h>

#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string_view>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <unistd.h>

namespace rune::node::test {

namespace ddl_rig_detail {
inline int gSeq = 0;   // single-threaded test binaries
}

class CatalogDdlRig {
public:
    static constexpr int32_t kSelf    = 1;
    static constexpr int64_t kSession = 4242;

    /// One answer as the client sees it: the response's code, the topic id / (schema
    /// or object) version it names, the detail (ErrorResponse / DdlResponse), and for a
    /// DdlResponse its objectId and gateTopicId; for a CreateView its output fingerprint.
    struct Answer {
        uint16_t    code        = 0;
        int32_t     topicId     = -1;
        uint32_t    version     = 0;
        std::string detail;
        uint64_t    objectId    = 0;
        int32_t     gateTopicId = -1;
        uint64_t    fingerprint = 0;
        enum class Kind : uint8_t { Define, Alter, CreateView, Ddl, Error } kind = Kind::Define;
    };

    struct DefineArgs {
        std::string                 name;
        int32_t                     partitions = 1;
        bool                        typed      = false;   // false = an untyped topic (empty descriptor)
        int16_t                     rf         = 0;
        std::string                 statement;
        std::string                 clientName = "rig";
        rune::protocol::RequestId   requestId{};          // {0,0} = the rig draws a fresh one
        uint8_t                     persistence = 0;
        std::string                 keyField    = "id";
        bool                        validate    = false;
        rune::protocol::TopicConfig config{};
        bool                        preV8       = false;   // no DDL extension (requestId {0,0})
    };

    explicit CatalogDdlRig(const char* tag, int32_t brokers = 3, int32_t topicMinIsr = 1,
                           std::size_t effectMaxFrame = 0)
        : effectMaxFrame_(effectMaxFrame), root_(std::filesystem::temp_directory_path() /
                (std::string("rune_ddlrig_") + tag + "_" + std::to_string(::getpid()) + "_" +
                 std::to_string(ddl_rig_detail::gSeq++))),
          brokers_(brokers), topicMinIsr_(topicMinIsr) {
        std::filesystem::remove_all(root_);
        std::filesystem::create_directories(root_ / "__catalog");
        open_();
    }
    ~CatalogDdlRig() {
        admin_.reset();
        replica_.reset();
        log_.reset();
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }
    CatalogDdlRig(const CatalogDdlRig&)            = delete;
    CatalogDdlRig& operator=(const CatalogDdlRig&) = delete;

    // ── roles ────────────────────────────────────────────────────────────────
    /// The catalog leadership with this ISR, then pumped until the worker serves
    /// DDL (fails loudly otherwise).  Default: every broker, each fetching (an
    /// append commits on the next pass).  An EXPLICIT ISR models slow followers:
    /// each follower in it starts SILENT (resumeFollower lets it fetch).
    void becomeServingLeader(std::vector<int32_t> isr = {}) {
        if (isr.empty()) {
            silent_.clear();
        } else {
            for (int32_t f : isr) if (f != kSelf) silent_.insert(f);
        }
        assumeLeadership(std::move(isr));
        for (int i = 0; i < kCap && admin_->roleName() != "LeaderServing"; ++i) pump();
        if (admin_->roleName() != "LeaderServing")
            throw std::logic_error("CatalogDdlRig: the leader never served DDL (" + std::string(admin_->roleName()) + ")");
    }
    /// Task 10: the catalog leadership at a NEW epoch with this ISR (empty = every
    /// broker) WITHOUT driving — the caller's pumps catch it up (CatchingUp →
    /// Serving).  The followers' silence is left as it is.
    void assumeLeadership(std::vector<int32_t> isr = {}) {
        if (isr.empty())
            for (int32_t b = 1; b <= brokers_; ++b) isr.push_back(b);
        isr_ = isr;
        AdminRoleMsg r;
        r.leader = true;
        r.epoch = ++epoch_;
        r.leaderId = kSelf;
        r.partitionEpoch = partitionEpoch_ = 0;
        r.isrCount = static_cast<int32_t>(isr.size());
        for (std::size_t i = 0; i < isr.size(); ++i) r.isr[i] = isr[i];
        admin_->onCatalogRole(r);
    }
    /// Task 10 fix round 1 (review M7): the ISR changes at the SAME leader epoch (a
    /// committed AlterIsr: a new partitionEpoch) — not a leadership change.  No pump.
    void updateIsr(std::vector<int32_t> isr) {
        isr_ = isr;
        AdminRoleMsg r;
        r.leader = true;
        r.epoch = epoch_;
        r.leaderId = kSelf;
        r.partitionEpoch = ++partitionEpoch_;
        r.isrCount = static_cast<int32_t>(isr.size());
        for (std::size_t i = 0; i < isr.size(); ++i) r.isr[i] = isr[i];
        admin_->onCatalogRole(r);
    }
    /// `drive` false (Task 10): the role only — the in-flight DDL is answered inside
    /// onCatalogRole; the queued ones on the caller's next pumps.
    void becomeFollower(int32_t leaderId, bool drive = true) {
        AdminRoleMsg r;
        r.leader = false;
        r.epoch = ++epoch_;
        r.leaderId = leaderId;
        admin_->onCatalogRole(r);
        if (drive) pump();
    }
    void silence(int32_t follower) { silent_.insert(follower); }
    /// Task 10: the follower fetches again from the next pump on (no pumping here).
    void unsilence(int32_t follower) { silent_.erase(follower); }
    /// Task 10: ONE fetch by `follower` at the leader's LEO (the HW covers every
    /// appended event once the replica's pass runs) — no admin pass.
    void fetchOnce(int32_t follower) {
        if (replica_->role() != rune::replication::Role::Leader) return;
        fetch_(follower);
        (void)replica_->doWork(now_);
    }
    /// The silent follower fetches again; pumped until its fetch landed.
    void resumeFollower(int32_t follower) {
        silent_.erase(follower);
        for (int i = 0; i < 8; ++i) pump();
    }

    // ── DDL ──────────────────────────────────────────────────────────────────
    Answer define(DefineArgs a) { return await_(submitDefineNoWait(std::move(a))); }
    int64_t submitDefineNoWait(DefineArgs a) {
        DefineTopicMsg m;
        m.session = kSession; m.corr = ++corr_;
        m.name = a.name; m.partitionCount = a.partitions; m.validate = a.validate;
        m.keyField = a.typed ? a.keyField : std::string{};
        if (a.typed) m.blob = descriptor(a.name);
        m.persistence = a.persistence; m.replicationFactor = a.rf; m.config = a.config;
        m.meta = meta_(a.statement, a.clientName, a.requestId, a.preV8);
        return submit_(DdlRequest{std::move(m)});
    }
    Answer alterAdd(const std::string& topic, const std::string& field,
                    rune::protocol::RequestId rid = {}, uint16_t expectedVersion = 0) {
        rune::codec::AlterOp op{};
        op.type = rune::codec::AlterOpType::Add;
        op.name = field;
        op.primType = rune::codec::PrimType::Int64;
        return alter(topic, {op}, rid, expectedVersion);
    }
    Answer alterSetCompat(const std::string& topic, rune::codec::CompatMode mode) {
        rune::codec::AlterOp op{};
        op.type = rune::codec::AlterOpType::SetCompatibility;
        op.mode = mode;
        return alter(topic, {op});
    }
    Answer alter(const std::string& topic, const std::vector<rune::codec::AlterOp>& ops,
                 rune::protocol::RequestId rid = {}, uint16_t expectedVersion = 0) {
        AlterTopicMsg m;
        m.session = kSession; m.corr = ++corr_; m.topicName = topic; m.expectedVersion = expectedVersion;
        m.opsBlob = rune::codec::encodeAlterOps(ops);
        m.meta = meta_("", "rig", rid, false);
        return await_(submit_(DdlRequest{std::move(m)}));
    }
    Answer alterConfig(const std::string& topic, std::vector<rune::catalog::ConfigChange> changes,
                       uint32_t expectedVersion = 0) {
        AlterTopicConfigMsg m;
        m.session = kSession; m.corr = ++corr_; m.name = topic; m.expectedVersion = expectedVersion;
        m.changes = std::move(changes);
        m.meta = meta_("", "rig", {}, false);
        return await_(submit_(DdlRequest{std::move(m)}));
    }
    Answer createView(const std::string& view, const std::string& source, bool extraColumn = false,
                      uint8_t persistence = 0) {
        CreateViewMsg m;
        m.session = kSession; m.corr = ++corr_; m.viewName = view; m.keyField = "id"; m.persistence = persistence;
        m.planBlob = plan(view, source, extraColumn);
        m.meta = meta_("", "rig", {}, false);
        return await_(submit_(DdlRequest{std::move(m)}));
    }
    Answer alterView(const std::string& view, const std::string& source, bool extraColumn,
                     uint32_t expectedVersion = 0) {
        AlterViewMsg m;
        m.session = kSession; m.corr = ++corr_; m.name = view; m.keyField = "id"; m.expectedVersion = expectedVersion;
        m.planBlob = plan(view, source, extraColumn);
        m.meta = meta_("", "rig", {}, false);
        return await_(submit_(DdlRequest{std::move(m)}));
    }
    Answer dropView(const std::string& view) {
        DropViewMsg m;
        m.session = kSession; m.corr = ++corr_; m.name = view;
        m.meta = meta_("", "rig", {}, false);
        return await_(submit_(DdlRequest{std::move(m)}));
    }
    Answer dropTopic(const std::string& topic) {
        DropTopicMsg m;
        m.session = kSession; m.corr = ++corr_; m.name = topic;
        m.meta = meta_("", "rig", {}, false);
        return await_(submit_(DdlRequest{std::move(m)}));
    }
    /// A GetTopicSchema answer (a pure read: answered at once).
    struct SchemaAnswer {
        uint16_t code = 0; int32_t topicId = -1; uint16_t version = 0; int32_t partitionCount = 0;
        std::vector<uint8_t> blob;
    };
    SchemaAnswer getSchema(const std::string& name, uint64_t fingerprint = 0) {
        const int64_t corr = ++corr_;
        admin_->submitGetSchema(GetSchemaMsg{kSession, corr, name, fingerprint});
        const auto it = gets_.find(corr);
        if (it == gets_.end()) throw std::logic_error("CatalogDdlRig: GetSchema was not answered at once");
        return it->second;
    }
    /// The next correlation id the rig would use (for hand-built requests).
    [[nodiscard]] int64_t nextCorr() noexcept { return ++corr_; }
    /// Any request (its corr is replaced); pumped until answered.
    Answer submitRaw(DdlRequest req) { return await_(submit_(std::move(req))); }
    int64_t submitRawNoWait(DdlRequest req) { return submit_(std::move(req)); }
    /// Task 10: queued on the worker WITHOUT any pump (the caller drives) — its corr.
    int64_t enqueue(DdlRequest req) {
        const int64_t corr = std::visit([](auto& m) { return m.corr; }, req);
        admin_->submit(std::move(req));
        return corr;
    }
    /// The DdlMeta every rig request carries (a fresh requestId unless `rid` is set).
    [[nodiscard]] DdlMeta meta(const std::string& statement = {}, rune::protocol::RequestId rid = {},
                               bool preV8 = false) {
        return meta_(statement, "rig", rid, preV8);
    }

    // ── observation ──────────────────────────────────────────────────────────
    /// Task 23: a catalog Subscribe on `session` (fromOffset -2 = earliest) — no pump.
    void subscribeCatalog(int64_t session, int64_t fromOffset = -2) {
        admin_->catalogSubscribe(CatalogSubscribeMsg{session, ++corr_, fromOffset});
    }
    [[nodiscard]] bool catalogReaderLive() const {
        const CatalogReadServer* s = admin_->readServer();
        return s != nullptr && catalogSubId_ != 0 && std::string_view(s->phaseName(catalogSubId_)) == "Live";
    }
    [[nodiscard]] uint64_t catalogProbes() const noexcept { return catalogProbes_; }
    [[nodiscard]] uint64_t catalogFrames() const noexcept { return catalogFrames_; }
    [[nodiscard]] std::optional<Answer> answerFor(int64_t corr) const {
        const auto it = answers_.find(corr);
        if (it == answers_.end()) return std::nullopt;
        return it->second;
    }
    [[nodiscard]] std::size_t answerCount() const noexcept { return answers_.size(); }
    /// The GetTopicSchema answer recorded for `corr` (a direct submitGetSchema).
    [[nodiscard]] std::optional<SchemaAnswer> schemaAnswerFor(int64_t corr) const {
        const auto it = gets_.find(corr);
        if (it == gets_.end()) return std::nullopt;
        return it->second;
    }
    /// Every event the log holds (committed or not), in offset order.
    [[nodiscard]] std::vector<rune::catalog::RecordedEvent> recordedLog() const {
        std::vector<rune::catalog::RecordedEvent> out;
        int64_t next = 0;
        const int64_t leo = log_->logEndOffset();
        while (next < leo) {
            const std::vector<uint8_t> data = log_->read(next, 1 << 22);
            if (data.empty()) break;
            std::size_t pos = 0;
            const int64_t before = next;
            while (pos < data.size()) {
                rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
                if (!reader.ensureValid()) break;
                const auto tl = static_cast<std::size_t>(reader.totalLength());
                reader.forEachRecord([&](std::string_view, std::string_view val, int64_t off, int64_t) {
                    if (off < next) return;
                    auto ev = rune::catalog::decode(
                        std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()), val.size()));
                    if (!ev) throw std::logic_error("CatalogDdlRig: a logged event does not decode");
                    out.push_back(rune::catalog::RecordedEvent{off, std::move(*ev)});
                    next = off + 1;
                });
                if (tl == 0) break;
                pos += tl;
            }
            if (next == before) break;
        }
        return out;
    }
    [[nodiscard]] std::size_t countEventsWithRequestId(const rune::protocol::RequestId& rid) const {
        std::size_t n = 0;
        for (const auto& r : recordedLog()) n += r.event.principal.requestId == rid ? 1 : 0;
        return n;
    }
    [[nodiscard]] AdminWorker& admin() noexcept { return *admin_; }
    [[nodiscard]] rune::replication::PartitionReplica& replica() noexcept { return *replica_; }
    [[nodiscard]] const SchemaRegistry& schemas() const noexcept { return schemas_; }
    [[nodiscard]] const ViewRegistry& views() const noexcept { return views_; }
    [[nodiscard]] int64_t sessionId() const noexcept { return kSession; }
    [[nodiscard]] std::string catalogDir() const { return (root_ / "__catalog").string(); }
    void onAnswer(std::function<void()> fn) { onAnswer_ = std::move(fn); }
    /// Task 10: called with every answer right AFTER it is recorded.
    void onAnswerRecorded(std::function<void(int64_t corr, const Answer&)> fn) { onRecorded_ = std::move(fn); }
    /// The effects the boot replay posted (the coalesced BOOT sink, Task 7 I1), same
    /// format as effects(); cleared by restart().
    [[nodiscard]] const std::vector<std::string>& bootEffects() const noexcept { return bootEffects_; }
    [[nodiscard]] const std::filesystem::path& root() const noexcept { return root_; }
    [[nodiscard]] int64_t now() const noexcept { return now_; }
    /// The clock moves WITHOUT a pump (Task 10: the caller's next pump sees it).
    void advanceClock(int64_t ns) noexcept { now_ += ns; }
    /// The live effects the applier posted: "<effect>:<topicId>".
    [[nodiscard]] const std::vector<std::string>& effects() const noexcept { return effects_; }

    /// Same dir: the log recovered, fresh registries, a fresh AdminWorker whose
    /// bootCatalog replays the local prefix (the one boot replay); a Follower again.
    void restart() {
        admin_.reset();
        replica_.reset();
        log_.reset();
        schemas_ = SchemaRegistry{};
        views_   = ViewRegistry{};
        effects_.clear();
        catalogSubId_ = 0;
        bootEffects_.clear();
        isr_.clear();
        silent_.clear();
        open_();
    }

    // ── time / driving ───────────────────────────────────────────────────────
    void advanceClockPast(int64_t ns) {
        now_ += ns + 1;
        pump();
    }
    /// One pass: the admin worker, then every non-silent ISR follower fetches at
    /// the leader's LEO (the next pass commits and applies what it acked).
    void pump() {
        now_ += 1'000'000;
        (void)admin_->doWork(now_);
        if (replica_->role() != rune::replication::Role::Leader) return;
        for (int32_t f : isr_) {
            if (f == kSelf || silent_.contains(f)) continue;
            fetch_(f);
        }
        (void)replica_->doWork(now_);
    }

    // ── building blocks (public: tests compose their own requests) ───────────
    /// A typed descriptor: id (Int64 key) + hp (Int64).
    [[nodiscard]] static std::vector<uint8_t> descriptor(const std::string& name) {
        return rune::codec::serialize(rune::codec::DescriptorBuilder(name, 1)
                                          .add("id", rune::codec::PrimType::Int64, 1, /*isKey=*/true)
                                          .add("hp", rune::codec::PrimType::Int64)
                                          .build());
    }
    /// An authored plan over `source` (by its CURRENT catalog topic id): id + hp
    /// (+ id again as "id2" when extraColumn).
    [[nodiscard]] std::vector<uint8_t> plan(const std::string& view, const std::string& source, bool extraColumn) const {
        namespace rv = rune::view;
        rv::ViewPlan p;
        p.viewName = view;
        p.key      = "id";
        const rune::catalog::TopicObject* t = admin_->applier().state().topicByName(source);
        p.sources.push_back(rv::SourceSpec{t ? t->topicId : 999'999, "id"});
        p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::Int64, 0, "id", rv::MergeRule::Replace});
        p.output.push_back(rv::AuthoredField{"hp", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
        if (extraColumn)
            p.output.push_back(rv::AuthoredField{"hp2", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
        return rv::serializeAuthored(p);
    }
    [[nodiscard]] rune::protocol::RequestId freshRequestId() noexcept { return {0xD1A6, ++ridCounter_}; }

    static constexpr int kCap = 4096;   // every drive loop's hard iteration cap

private:
    DdlMeta meta_(const std::string& statement, const std::string& client, rune::protocol::RequestId rid, bool preV8) {
        DdlMeta m;
        m.statement     = statement;
        m.sessionId     = kSession;
        m.sourceAddress = "aeron:udp?endpoint=rig-client:40123";
        if (preV8) return m;   // no extension: no client name, no request id
        m.clientName = client;
        m.requestId  = rid.none() ? freshRequestId() : rid;
        return m;
    }
    int64_t submit_(DdlRequest req) {
        const int64_t corr = std::visit([](auto& m) { return m.corr; }, req);
        admin_->submit(std::move(req));
        for (int i = 0; i < 4; ++i) pump();   // started: appended (or answered)
        return corr;
    }
    Answer await_(int64_t corr) {
        for (int i = 0; i < kCap && !answers_.contains(corr); ++i) pump();
        const auto it = answers_.find(corr);
        if (it == answers_.end())
            throw std::logic_error("CatalogDdlRig: corr " + std::to_string(corr) + " was never answered");
        return it->second;
    }
    void record_(int64_t corr, Answer a) {
        if (onAnswer_) onAnswer_();
        answers_[corr] = std::move(a);
        if (onRecorded_) onRecorded_(corr, answers_[corr]);
    }
    void fetch_(int32_t f) {
        rune::replication::FetchRequestMsg ack{};
        ack.leaderEpoch      = static_cast<int32_t>(replica_->epoch());
        ack.partition        = kCatalogPartition;
        ack.followerId       = f;
        ack.fetchOffset      = replica_->logEndOffset();
        ack.maxBytes         = 1 << 20;
        ack.topicId          = kCatalogTopicId;
        ack.lastFetchedEpoch = -1;
        replica_->onFetchRequest(ack, now_);
    }

    void open_() {
        log_ = std::make_unique<rune::storage::Log>(catalogDir(), kTestSegmentRollBytes);
        log_->recover();
        NodeConfig nc;
        boundTestSegments(nc);
        nc.nodeId = kSelf;
        // A silent follower stays in the ISR across the rig's clock jumps (the
        // 30 s DDL timeout): ISR changes are the controller's (Task 6), not a lag.
        nc.replicaLagMaxMs = 3'600'000;
        std::vector<rune::replication::NodeId> all;
        for (int32_t b = 1; b <= brokers_; ++b) all.push_back(b);
        replica_ = std::make_unique<rune::replication::PartitionReplica>(
            makeCatalogPartitionConfig(nc, static_cast<std::size_t>(brokers_)), rune::replication::Role::Follower, 0,
            all, all, log_.get(), &transport_);

        AdminDeps deps;
        deps.brokerCount = brokers_;
        deps.topicMinIsr = topicMinIsr_;
        deps.effectMaxFrameBytes = effectMaxFrame_;
        deps.respondDefine = [this](int64_t, int64_t corr, int32_t topicId, uint16_t version, uint16_t code) {
            record_(corr, Answer{code, topicId, version, {}, 0, -1, 0, Answer::Kind::Define});
        };
        deps.respondAlter = [this](int64_t, int64_t corr, int32_t topicId, uint16_t version, uint16_t code) {
            record_(corr, Answer{code, topicId, version, {}, 0, -1, 0, Answer::Kind::Alter});
        };
        deps.respondCreateView = [this](int64_t, int64_t corr, int32_t viewTopicId, uint64_t fp, uint16_t code) {
            record_(corr, Answer{code, viewTopicId, 0, {}, 0, -1, fp, Answer::Kind::CreateView});
        };
        deps.respondDdl = [this](int64_t, int64_t corr, uint16_t code, uint64_t objectId, uint32_t newVersion,
                                 int32_t gate, std::string_view detail) {
            record_(corr, Answer{code, -1, newVersion, std::string(detail), objectId, gate, 0, Answer::Kind::Ddl});
        };
        deps.respondError = [this](int64_t, int64_t corr, uint16_t code, std::string_view message) {
            record_(corr, Answer{code, -1, 0, std::string(message), 0, -1, 0, Answer::Kind::Error});
        };
        deps.respondGet = [this](int64_t, int64_t corr, int32_t topicId, uint16_t version, int32_t partitions,
                                 const std::vector<uint8_t>& blob, uint16_t code) {
            gets_[corr] = SchemaAnswer{code, topicId, version, partitions, blob};
        };
        // Task 23: the catalog read egress (counts only — zero-alloc, so the idle-reader gate
        // measures the admin pass, never the rig).
        deps.catalogRead = [this](const CatalogReadFrameHead& h, std::span<const uint8_t> f) {
            ++catalogFrames_;
            if (f.empty() && h.baseOffset == kProbeOffset) { ++catalogProbes_; return true; }
            if (catalogSubId_ == 0)
                if (const auto s = rune::protocol::client::SubscribeResponse::decode(f.data(), f.size()))
                    catalogSubId_ = s->subscriptionId;
            return true;
        };
        admin_ = std::make_unique<AdminWorker>(schemas_, kSelf, std::move(deps), &views_, replica_.get(), nullptr);
        CatalogEffects fx;
        fx.topicApplied  = [this](const rune::catalog::TopicObject& t) { effects_.push_back("topicApplied:" + std::to_string(t.topicId)); };
        fx.sinkApplied   = [this](const SinkSpec& s) { effects_.push_back("sinkApplied:" + std::to_string(s.topicId)); };
        fx.topicDropped  = [this](int32_t id, const std::string&) { effects_.push_back("topicDropped:" + std::to_string(id)); };
        fx.viewInstalled = [this](const rune::catalog::ViewObject& v) { effects_.push_back("viewInstalled:" + std::to_string(v.def.viewTopicId)); };
        fx.schemaChanged = [this](const rune::catalog::TopicObject& t, uint16_t) {
            effects_.push_back("schemaChanged:" + std::to_string(t.topicId) + "@v" +
                               std::to_string(t.schema ? t.schema->schemaVersion : 0));
        };
        fx.halted = [this](const rune::catalog::ApplyError&) { effects_.push_back("halted"); };
        fx.maxFrameBytes = effectMaxFrame_;   // the applier sizes against the same frame as the leader gate
        CatalogEffects boot;                  // Task 10: the boot replay's (coalesced) sink, recorded apart
        boot.topicApplied  = [this](const rune::catalog::TopicObject& t) { bootEffects_.push_back("topicApplied:" + std::to_string(t.topicId)); };
        boot.sinkApplied   = [this](const SinkSpec& s) { bootEffects_.push_back("sinkApplied:" + std::to_string(s.topicId)); };
        boot.viewInstalled = [this](const rune::catalog::ViewObject& v) { bootEffects_.push_back("viewInstalled:" + std::to_string(v.def.viewTopicId)); };
        boot.halted        = [this](const rune::catalog::ApplyError&) { bootEffects_.push_back("halted"); };
        boot.maxFrameBytes = effectMaxFrame_;
        (void)admin_->bootCatalog(BootEffects{std::move(boot)}, LiveEffects{std::move(fx)});
    }

    std::size_t                                          effectMaxFrame_ = 0;
    std::filesystem::path                                root_;
    int32_t                                              brokers_;
    int32_t                                              topicMinIsr_;
    std::unique_ptr<rune::storage::Log>                  log_;
    rune::replication::NullPartitionTransport            transport_;
    std::unique_ptr<rune::replication::PartitionReplica> replica_;
    SchemaRegistry                                       schemas_;
    ViewRegistry                                         views_;
    std::unique_ptr<AdminWorker>                         admin_;
    std::vector<int32_t>                                 isr_;
    std::set<int32_t>                                    silent_;
    std::map<int64_t, Answer>                            answers_;
    std::map<int64_t, SchemaAnswer>                      gets_;
    std::vector<std::string>                             effects_;
    std::vector<std::string>                             bootEffects_;
    std::function<void()>                                onAnswer_;
    std::function<void(int64_t, const Answer&)>          onRecorded_;
    int32_t                                              epoch_ = 0;
    int32_t                                              partitionEpoch_ = 0;
    int64_t                                              now_   = 1'000'000'000;
    int64_t                                              corr_  = 0;
    uint64_t                                             ridCounter_ = 0;
    uint64_t catalogFrames_ = 0;  uint64_t catalogProbes_ = 0;  int64_t catalogSubId_ = 0;
};

} // namespace rune::node::test
