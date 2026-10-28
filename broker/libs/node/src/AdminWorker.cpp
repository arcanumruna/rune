// AdminWorker.cpp — the catalog write path (catalog history spec §5.1, Task 9).
// See AdminWorker.h.
#include "rune/node/AdminWorker.h"

#include <rune/node/CatalogEffectFrames.h>
#include <rune/node/FdBudget.h>
#include <rune/node/PersistenceMode.h>

#include <rune/catalog/CatalogFingerprint.h>
#include <rune/catalog/TopicName.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>   // kSchemaLineageFingerprint (#418)
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/WorkerReplicationTransport.h>   // the __catalog plane (Task 6)
#include <rune/view/AggState.h>      // rune::view::isAggregateView (Phase 2a)
#include <rune/view/PlanCompiler.h>  // rune::view::compileView
#include <rune/view/ViewPlan.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <exception>
#include <stdexcept>
#include <type_traits>

namespace rune::node {

namespace cat = rune::catalog;

namespace {

// Error codes (rune::protocol::client::ErrorCode values).
constexpr uint16_t kOK                   =  0;
constexpr uint16_t kNotLeader            =  2;
constexpr uint16_t kInternal             =  6;
constexpr uint16_t kUnknownTopic         =  7;
// Namespaces Task 12: a create in a namespace that does not exist (the gateway sends a hidden one as kHiddenNsId,
// which never exists) answers UnknownTopic before any other check, so a hidden and a missing namespace answer alike.
// The detail never names the namespace.
constexpr const char* kNoNamespaceDetail = "no such namespace";
constexpr uint16_t kInvalidSchema        = 23;
constexpr uint16_t kTopicExists          = 25;
constexpr uint16_t kSchemaIncompatible   = 30;
constexpr uint16_t kSchemaConflict       = 31;
constexpr uint16_t kViewExists           = 32;
constexpr uint16_t kViewSourceNotFound   = 33;
constexpr uint16_t kViewFieldUnresolved  = 34;
constexpr uint16_t kViewKeyInvalid       = 35;
constexpr uint16_t kViewNotCoPartitioned = 36;
constexpr uint16_t kViewSourceTransient  = 37;
constexpr uint16_t kViewNotAlterable     = 38;
constexpr uint16_t kInvalidRequest       = 39;
constexpr uint16_t kInvalidTopicName     = 40;
constexpr uint16_t kViewSourceIsView     = 41;
constexpr uint16_t kNotAuthorized        = 45;
constexpr uint16_t kNamespaceLimitExceeded = 52;   // only to clients >= kClientVersionNamespaces; older get 39

/// A malformed request carries no meta: it is treated as id-less (pre-v8) — it is
/// answered before the dedupe and never fingerprinted.
const DdlMeta kNoMeta{};
const DdlMeta& metaOf(const DdlRequest& req) noexcept {
    return std::visit([](const auto& m) -> const DdlMeta& {
        using M = std::decay_t<decltype(m)>;
        if constexpr (std::is_same_v<M, MalformedDdl> || std::is_same_v<M, ListNamespacesMsg>) return kNoMeta;
        else return m.meta;
    }, req);
}

/// The largest encoded event the leader writes.  A catalog record must reach
/// every follower in one fetch (kDefaultMaxFetchBytes) and the applier's first
/// read; half of it leaves the batch framing ample room.  Every request the admin
/// ring can carry (<= 128 KiB) yields far less, so this is a backstop.
/// Task 12: and every event must fit one Records frame of every catalog
/// subscriber (kCatalogMaxEventBytes) — an event no client could read is never written.
constexpr std::size_t kMaxCatalogEventBytes =
    std::min(static_cast<std::size_t>(rune::replication::kDefaultMaxFetchBytes) / 2, kCatalogMaxEventBytes);

std::string quotedName(std::string_view s) { return "'" + cat::escapeNameForLog(s) + "'"; }

DdlRejection reject(uint16_t code, std::string detail = {}, int32_t topicId = -1, uint16_t version = 0) {
    return DdlRejection{code, std::move(detail), topicId, version};
}

/// The wire limits of the frozen v1 layout (kMaxStr16Bytes / kMaxStr32Bytes /
/// kMaxListCount): an over-long client field is refused, naming it, BEFORE the
/// event is encoded (binding 4).  nullopt = within every limit.
std::optional<std::string> wireLimitError(const cat::CatalogEvent& e) {
    auto s16 = [](std::string_view what, std::size_t n) -> std::optional<std::string> {
        if (n <= cat::kMaxStr16Bytes) return std::nullopt;
        return std::string{what} + " is " + std::to_string(n) + " bytes, longer than the " +
               std::to_string(cat::kMaxStr16Bytes) + "-byte catalog limit";
    };
    auto s32 = [](std::string_view what, std::size_t n) -> std::optional<std::string> {
        if (n <= cat::kMaxStr32Bytes) return std::nullopt;
        return std::string{what} + " is " + std::to_string(n) + " bytes, longer than the catalog limit";
    };
    auto list = [](std::string_view what, std::size_t n) -> std::optional<std::string> {
        if (n <= cat::kMaxListCount) return std::nullopt;
        return std::string{what} + " has " + std::to_string(n) + " entries, more than the " +
               std::to_string(cat::kMaxListCount) + " a catalog event holds";
    };
    if (auto r = s16("the object name", e.objectName.size())) return r;
    if (auto r = s32("the statement", e.statement.size())) return r;
    if (auto r = s16("the client name", e.principal.clientName.size())) return r;
    if (auto r = s16("the source address", e.principal.sourceAddress.size())) return r;
    if (auto r = s16("the authenticated principal", e.principal.authenticated.size())) return r;
    if (e.replayedFrom) {
        if (auto r = s16("replayedFrom's cluster id", e.replayedFrom->clusterId.size())) return r;
        if (auto r = s16("replayedFrom's principal", e.replayedFrom->principal.size())) return r;
    }
    auto config = [&](const cat::TopicConfig& c) -> std::optional<std::string> {
        if (auto r = s16("the co-partition group", c.coPartitionGroup.size())) return r;
        return s16("the partition key", c.partitionKey.size());
    };
    auto schema = [&](const cat::SchemaDef& d) -> std::optional<std::string> {
        if (auto r = s16("the key field", d.keyField.size())) return r;
        return s32("the descriptor", d.descriptorBlob.size());
    };
    auto view = [&](const cat::ViewDef& v) -> std::optional<std::string> {
        if (auto r = s16("the view key field", v.keyField.size())) return r;
        if (auto r = list("the view's source list", v.sourceNames.size())) return r;
        for (const auto& n : v.sourceNames)
            if (auto r = s16("a source name", n.size())) return r;
        if (auto r = s32("the normalized plan", v.normalizedPlanBlob.size())) return r;
        if (auto r = s32("the compiled plan", v.compiledPlanBlob.size())) return r;
        return s32("the output descriptor", v.outputDescriptorBlob.size());
    };
    return std::visit([&](const auto& o) -> std::optional<std::string> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, cat::CreateTopicOps>) {
            if (auto r = config(o.config)) return r;
            if (o.schema) return schema(*o.schema);
            return std::nullopt;
        } else if constexpr (std::is_same_v<T, cat::CreateSchemaOps>) {
            return schema(o.schema);
        } else if constexpr (std::is_same_v<T, cat::AlterSchemaOps>) {
            if (auto r = list("the ALTER operation list", o.ops.size())) return r;
            for (const auto& op : o.ops)
                if (auto r = s16("an ALTER field name", op.name.size())) return r;
            return std::nullopt;
        } else if constexpr (std::is_same_v<T, cat::AlterTopicConfigOps>) {
            return list("the config change list", o.changes.size());
        } else if constexpr (std::is_same_v<T, cat::CreateViewOps> || std::is_same_v<T, cat::AlterViewOps>) {
            return view(o.view);
        } else {
            return std::nullopt;
        }
    }, e.ops);
}

int64_t wallClockMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

/// The fold-equal live name `n` lands on, and its answer (TopicExists with the
/// object's topic id).
std::optional<DdlRejection> foldCollision(const cat::CatalogState& s, uint32_t nsId, std::string_view n) {
    const std::string* other = s.liveNameFoldingOnto(nsId, n);
    if (other == nullptr) return std::nullopt;
    int32_t id = -1;
    if (const cat::TopicObject* t = s.topicByName(nsId, *other)) id = t->topicId;
    else if (const cat::ViewObject* v = s.viewByName(nsId, *other)) id = v->def.viewTopicId;
    return reject(kTopicExists, "name " + quotedName(n) + " differs only in letter case from " + quotedName(*other), id);
}

// FNV-1a-64 helpers for requestFingerprint (rune::catalog's building blocks).
struct Fnv {
    uint64_t h = 0xcbf29ce484222325ull;
    void u(uint64_t v) noexcept { h = cat::detail::fnvU64(h, v); }
    void s(std::string_view v) noexcept { h = cat::detail::fnvBytes(h, v.data(), v.size()); }
    void b(const std::vector<uint8_t>& v) noexcept { h = cat::detail::fnvBytes(h, v.data(), v.size()); }
};
void h_bytes(Fnv& f, const uint8_t* p, std::size_t n) noexcept { f.h = cat::detail::fnvBytes(f.h, p, n); }

} // namespace

// ─── requestFingerprint (F14) ───────────────────────────────────────────────
uint16_t requestTemplateId(const DdlRequest& req) noexcept {
    return std::visit([](const auto& m) -> uint16_t {
        using M = std::decay_t<decltype(m)>;
        if constexpr (std::is_same_v<M, DefineTopicMsg>)      return kTidDefineTopic;
        else if constexpr (std::is_same_v<M, AlterTopicMsg>)  return kTidAlterTopic;
        else if constexpr (std::is_same_v<M, CreateViewMsg>)  return kTidCreateView;
        else if constexpr (std::is_same_v<M, DropTopicMsg>)   return kTidDropTopic;
        else if constexpr (std::is_same_v<M, DropViewMsg>)    return kTidDropView;
        else if constexpr (std::is_same_v<M, AlterViewMsg>)   return kTidAlterView;
        else if constexpr (std::is_same_v<M, NamespaceDdlMsg>)   return kTidNamespaceDdl;
        else if constexpr (std::is_same_v<M, ListNamespacesMsg>) return kTidListNamespaces;
        else if constexpr (std::is_same_v<M, MalformedDdl>)   return m.templateId;
        else                                                  return kTidAlterTopicConfig;
    }, req);
}

uint64_t requestFingerprint(const DdlRequest& req) noexcept {
    Fnv f;
    f.u(requestTemplateId(req));
    std::visit([&f](const auto& m) {
        using M = std::decay_t<decltype(m)>;
        if constexpr (std::is_same_v<M, MalformedDdl> || std::is_same_v<M, ListNamespacesMsg>) {
            return;   // never deduped: answered before the dedupe (startNext_); a list is a pure read
        } else if constexpr (std::is_same_v<M, NamespaceDdlMsg>) {
            // The statement as resolved by the client: kind, name, the present mask and the given limits.
            // Never the denial or the client version (a retry from an upgraded client is the same statement).
            f.s(m.meta.statement);
            f.u(m.kind); f.s(m.name);
            for (uint8_t b : m.mask) f.u(b);
            uint8_t raw[rune::protocol::kNamespaceLimitsBytes];
            rune::protocol::encodeLimits(m.limits, raw);
            h_bytes(f, raw, sizeof raw);
        } else {
        f.s(m.meta.statement);
        f.u(m.nsId);
        if constexpr (std::is_same_v<M, DefineTopicMsg>) {
            f.s(m.name);
            f.u(static_cast<uint32_t>(m.partitionCount)); f.u(m.validate ? 1 : 0); f.s(m.keyField); f.b(m.blob);
            f.u(m.persistence); f.u(static_cast<uint16_t>(m.replicationFactor));
            const auto& c = m.config;
            f.u(c.cleanupPolicy); f.u(static_cast<uint64_t>(c.retentionMs)); f.u(static_cast<uint64_t>(c.retentionBytes));
            f.u(static_cast<uint64_t>(c.deleteRetentionMs)); f.u(static_cast<uint32_t>(c.minCleanableDirtyRatioPpm));
            f.u(static_cast<uint64_t>(c.segmentRollBytes)); f.u(static_cast<uint64_t>(c.minCompactionLagMs));
            f.u(static_cast<uint64_t>(c.maxCompactionLagMs)); f.u(static_cast<uint64_t>(c.retentionCheckMs));
            f.u(static_cast<uint64_t>(c.fileDeleteDelayMs)); f.u(c.durability); f.u(c.durabilityTier);
            f.s(c.coPartitionGroup); f.s(c.partitionKey);
        } else if constexpr (std::is_same_v<M, AlterTopicMsg>) {
            f.s(m.topicName); f.u(m.expectedVersion); f.b(m.opsBlob);
        } else if constexpr (std::is_same_v<M, CreateViewMsg>) {
            f.s(m.viewName); f.u(m.persistence); f.s(m.keyField); f.b(m.planBlob);
        } else if constexpr (std::is_same_v<M, DropTopicMsg> || std::is_same_v<M, DropViewMsg>) {
            f.s(m.name);
        } else if constexpr (std::is_same_v<M, AlterViewMsg>) {
            f.s(m.name); f.s(m.keyField); f.u(m.persistence); f.u(m.expectedVersion); f.b(m.planBlob);
        } else {
            f.s(m.name); f.u(m.expectedVersion); f.u(m.changes.size());
            for (const auto& c : m.changes) { f.u(static_cast<uint8_t>(c.key)); f.u(static_cast<uint64_t>(c.value)); }
        }
        }
    }, req);
    return f.h;
}

DdlRejection encodeErrorAnswer(cat::EncodeError e) {
    switch (e) {
    case cat::EncodeError::KindOpsMismatch:
    case cat::EncodeError::UnknownKind:
    case cat::EncodeError::UnknownFormatVer:
        return reject(kInternal);
    case cat::EncodeError::FieldTooLong:
    case cat::EncodeError::TooManyElements:
    case cat::EncodeError::ReservedKind:
    case cat::EncodeError::BadEnum:
        break;
    }
    return reject(kInvalidRequest, "the catalog event cannot be encoded: " + std::string{cat::encodeErrorName(e)});
}

// ─── construction ───────────────────────────────────────────────────────────
namespace {
std::string catalogDirOf(rune::replication::PartitionReplica* r) {
    return (r != nullptr && r->log() != nullptr) ? r->log()->dir() : std::string{};
}
} // namespace

AdminWorker::AdminWorker(SchemaRegistry& reg,
                         int32_t selfId,
                         AdminDeps deps,
                         ViewRegistry* viewRegistry,
                         rune::replication::PartitionReplica* catalogReplica,
                         rune::replication::WorkerReplicationTransportImpl<rune::replication::AeronPubRegistry>*
                             catalogPlane)
    : reg_(reg)
    , selfId_(selfId)
    , deps_(std::move(deps))
    , ownViews_(viewRegistry == nullptr ? std::make_unique<ViewRegistry>() : nullptr)
    , viewRegistry_(viewRegistry != nullptr ? viewRegistry : ownViews_.get())
    , catalogReplica_(catalogReplica)
    , catalogPlane_(catalogPlane)
    , applier_(catalogDirOf(catalogReplica), reg, *viewRegistry_) {
    securityCatchUp_ = SecurityCatchUp{deps_.securityMaxStalenessNs};   // issues #15 / #111: F from the config
    if (catalogReplica_ != nullptr) {
        // One max fetch + the batch framing slack the worker's scratch carries.
        catalogReadScratch_.reset(static_cast<std::size_t>(rune::replication::kDefaultMaxFetchBytes) + 64u * 1024u);
        catalogReplica_->setReadScratch(catalogReadScratch_.data(), catalogReadScratch_.size());
        // Task 12: catalog reads.  Ids carry the node prefix the gateway's do
        // ((nodeId + 1) << 40), so they stay cluster-unique.
        // No egress wired (DDL-only rigs): nothing ever leaves — a subscription
        // stays Answering (bounded by the cap), never answered as if served.
        if (!deps_.catalogRead) spdlog::debug("AdminWorker: no catalog read egress wired — catalog reads not served");
        readServer_.emplace(kCatalogTopicId, *catalogReplica_,
                            deps_.catalogRead ? deps_.catalogRead
                                              : CatalogReadEgress([](const CatalogReadFrameHead&, std::span<const uint8_t>) { return false; }),
                            kMaxCatalogSubscriptions, static_cast<int64_t>(selfId_ + 1) << 40);
    }
    eventBuf_.reserve(64 * 1024);
}

void AdminWorker::catalogSubscribe(const CatalogSubscribeMsg& m) {
    if (readServer_) {
        readServer_->subscribe(m.session, m.corr, m.fromOffset);
        return;
    }
    spdlog::error("AdminWorker: catalog Subscribe (session {}) on a broker without a __catalog — answered Internal",
                  m.session);
    if (!deps_.catalogRead) return;
    namespace pc = rune::protocol::client;
    pc::SubscribeResponse r;
    r.sessionId     = m.session;
    r.correlationId = m.corr;
    r.errorCode     = static_cast<int16_t>(pc::ErrorCode::Internal);
    uint8_t frame[pc::SubscribeResponse::ENCODED_SIZE];
    const std::size_t n = r.encode(frame);
    (void)deps_.catalogRead(CatalogReadFrameHead{m.session, 0, -1}, std::span<const uint8_t>(frame, n));   // cold: test configurations only
}

void AdminWorker::catalogUnsubscribe(const CatalogUnsubscribeMsg& m) {
    // A single unsubscribe names ONE server by its id space; a session close (-1) ends the session's reads on both.
    if (m.subscriptionId == rune::gateway::kEndCatalogReadsOnly) {   // Task 14: DESCRIBE ON CLUSTER revoked - the security reads stay
        if (readServer_) readServer_->unsubscribe(m.session, -1);
        return;
    }
    if (m.subscriptionId != -1 && rune::gateway::isSecuritySubscriptionId(m.subscriptionId)) {
        if (securityReadServer_) securityReadServer_->unsubscribe(m.session, m.subscriptionId);
        return;
    }
    if (m.subscriptionId == -1 && securityReadServer_) securityReadServer_->unsubscribe(m.session, -1);
    if (readServer_) readServer_->unsubscribe(m.session, m.subscriptionId);
}

void AdminWorker::securitySubscribe(const CatalogSubscribeMsg& m) {
    if (securityReadServer_) {
        securityReadServer_->subscribe(m.session, m.corr, m.fromOffset);
        return;
    }
    spdlog::error("AdminWorker: __security Subscribe (session {}) on a broker without a __security - answered Internal",
                  m.session);
    if (!deps_.catalogRead) return;
    namespace pc = rune::protocol::client;
    pc::SubscribeResponse r;
    r.sessionId     = m.session;
    r.correlationId = m.corr;
    r.errorCode     = static_cast<int16_t>(pc::ErrorCode::Internal);
    uint8_t frame[pc::SubscribeResponse::ENCODED_SIZE];
    const std::size_t n = r.encode(frame);
    (void)deps_.catalogRead(CatalogReadFrameHead{m.session, 0, -1}, std::span<const uint8_t>(frame, n));
}

void AdminWorker::securityUnsubscribe(const CatalogUnsubscribeMsg& m) {
    if (securityReadServer_) securityReadServer_->unsubscribe(m.session, m.subscriptionId);
}

void AdminWorker::rejectSecurityNoLog_(const SecurityRequest& req) {
    // A configuration without a __security (rigs): every security request is answered Internal, never dropped.
    const auto [session, corr] = std::visit([](const auto& r) { return std::pair<int64_t, int64_t>{r.session, r.corr}; }, req);
    if (deps_.respondSecurity) deps_.respondSecurity(session, corr, 6, 0, 0, -1, "this broker has no __security");
}

int AdminWorker::bootCatalog(BootEffects boot, LiveEffects live) {
    int replayed = 0;
    if (catalogReplica_ != nullptr) replayed = applier_.replayLocal(*catalogReplica_, std::move(boot));   // throws: refuse boot
    if (!applier_.goLive(std::move(live)))
        throw std::runtime_error("AdminWorker: the CatalogApplier refused to go live (phase " +
                                 std::string{applier_.phaseName()} + ") — refusing to start");
    return replayed;
}

void AdminWorker::adoptBootedApplier(CatalogApplier booted, LiveEffects live) {
    if (booted.phaseName() != "Booted")
        throw std::runtime_error("AdminWorker: the adopted CatalogApplier is in phase " +
                                 std::string{booted.phaseName()} + ", not Booted — the broker's startup phase runs "
                                 "the one boot replay; refusing to start");
    if (pendingLive_)
        throw std::logic_error("AdminWorker: an applier was already adopted");
    applier_     = std::move(booted);
    pendingLive_ = std::move(live);
}

void AdminWorker::attachSecurity(SecurityApplier booted, rune::replication::PartitionReplica* replica,
                                 SecurityLiveEffects live) {
    if (booted.phaseName() != "Booted")
        throw std::runtime_error("AdminWorker::attachSecurity: the SecurityApplier is in phase " +
                                 std::string{booted.phaseName()} + ", not Booted - the startup thread must replay "
                                 "the local __security prefix first (refusing to start)");
    securityApplier_.emplace(std::move(booted));
    securityReplica_ = replica;
    securityAdmin_.emplace(*securityApplier_, replica, deps_.respondSecurity, selfId_);
    // Namespaces Task 8: `applier_` is a member (move-assigned in place by adoptBootedApplier), so its state's address
    // is stable for this worker's life.
    securityAdmin_->bindCatalog(&applier_.state());
    if (replica != nullptr)
        // Authentication Task 10: `__security` reads ride a second CatalogReadServer on the same egress; its ids carry
        // the security id bit so one id names one server.
        securityReadServer_.emplace(kSecurityTopicId, *replica,
                                    deps_.catalogRead ? deps_.catalogRead
                                                      : CatalogReadEgress([](const CatalogReadFrameHead&, std::span<const uint8_t>) { return false; }),
                                    kMaxCatalogSubscriptions,
                                    (static_cast<int64_t>(selfId_ + 1) << 40) | rune::gateway::kSecuritySubscriptionIdBit);
    pendingSecurityLive_ = std::move(live);
}

void AdminWorker::deferTopicDirReconcile(std::string dataDir) {
    bootReconcile_ = AwaitingCatchUp{std::move(dataDir), false};
}

std::string_view AdminWorker::bootReconcileStateName() const noexcept {
    switch (bootReconcile_.index()) {
    case 0:  return "None";
    case 1:  return "AwaitingCatchUp";
    default: return "Done";
    }
}

bool AdminWorker::bootReconcileReady_() const noexcept {
    return catalogReplica_ != nullptr && !applier_.halted() &&
           catalogReplica_->role() != rune::replication::Role::Leader && catalogReplica_->caughtUpWithLeader() &&
           applier_.nextOffset() > 0 && applier_.nextOffset() >= catalogReplica_->logEndOffset();
}

int AdminWorker::stepBootReconcile_() {
    auto* d = std::get_if<AwaitingCatchUp>(&bootReconcile_);
    if (d == nullptr) return 0;
    if (applier_.halted()) {
        spdlog::critical("AdminWorker: the deferred topic-dir reconcile is CANCELLED — the catalog apply halted "
                         "(catalog damage: no destructive cleanup); the catch-up sentinel stays pending");
        bootReconcile_ = BootReconcileDone{};
        return 1;
    }
    if (catalogReplica_ != nullptr && catalogReplica_->role() == rune::replication::Role::Leader) {
        if (!d->leaderWaitLogged) {
            spdlog::critical("AdminWorker: this broker leads __catalog without a caught-up history — topic dirs stay "
                             "untouched until __catalog is restored or this broker catches up as a follower");
            d->leaderWaitLogged = true;
        }
        return 0;
    }
    if (!bootReconcileReady_()) return 0;
    if (!writeCatchUpSentinel(d->dataDir, applier_.nextOffset() - 1))
        spdlog::critical("AdminWorker: cannot persist the catch-up sentinel in {} — it stays pending: the next boot "
                         "defers again", d->dataDir);
    spdlog::info("AdminWorker: caught up to the catalog leader at offset {} — the deferred topic-dir reconcile runs "
                 "at the next boot", applier_.nextOffset());
    bootReconcile_ = BootReconcileDone{};
    return 1;
}

std::string_view AdminWorker::roleName() const noexcept {
    switch (role_.index()) {
    case 0:  return "Follower";
    case 1:  return "LeaderCatchingUp";
    case 2:  return "LeaderServing";
    default: return "Halted";
    }
}

namespace {
int32_t epochOf(const CatalogRole& r) noexcept {
    return std::visit([](const auto& st) { return st.epoch; }, r);
}
} // namespace

void AdminWorker::setRole_(CatalogRole next) {
    const bool changed = next.index() != role_.index() || epochOf(next) != epochOf(role_);
    if (epochOf(next) != epochOf(role_)) stampEvidence_();   // R3-M1: a catalog leadership epoch change, stamped
    role_ = std::move(next);
    if (changed) spdlog::info("AdminWorker: __catalog role -> {} (epoch {})", roleName(), epochOf(role_));
}

bool AdminWorker::caughtUp_() const noexcept {
    // Task 11: every record the leader holds — a previous epoch's tail included —
    // is committed (HW = min LEO over the ISR) AND applied here.
    const int64_t hw = catalogReplica_->highWatermark();
    return !applier_.halted() && hw == catalogReplica_->logEndOffset() && applier_.nextOffset() == hw;
}

std::string_view AdminWorker::inFlightStateName() const noexcept {
    return std::holds_alternative<Idle>(inFlight_) ? "Idle" : "AwaitingCommit";
}

// ─── roles ──────────────────────────────────────────────────────────────────
// The role is absolute and epoch-stamped (the committed assignment); the
// replica's own role machine handles re-asserts and refuses a leadership at an
// epoch it already led at (restart fence → asks the controller for a bump).
void AdminWorker::onCatalogRole(const AdminRoleMsg& role) {
    if (catalogReplica_ == nullptr) return;
    namespace rr = rune::replication;
    if (role.leader) {
        std::vector<rr::NodeId> isr;
        isr.reserve(static_cast<std::size_t>(role.isrCount));
        for (int i = 0; i < role.isrCount; ++i)
            isr.push_back(static_cast<rr::NodeId>(role.isr[static_cast<std::size_t>(i)]));
        catalogReplica_->becomeLeader(static_cast<rr::Epoch>(role.epoch), std::move(isr), role.partitionEpoch);
        if (catalogReplica_->role() == rr::Role::Leader) {
            // A halted broker stays Halted (its replica still leads until the
            // controller moves the leadership; it accepts no DDL).
            if (std::holds_alternative<CatalogHalted>(role_)) return;
            // A promotion (or a leadership at a new epoch) starts catching up — the
            // gate opens in updateRole_ once the whole log is committed and
            // applied; a same-epoch re-assert (an ISR update) keeps serving.
            const auto* s = std::get_if<CatalogLeaderServing>(&role_);
            if (s == nullptr || s->epoch != role.epoch) setRole_(CatalogLeaderCatchingUp{role.epoch});
            return;
        }
        demote_(-1, role.epoch);   // the replica refused the leadership (restart fence)
        return;
    }
    catalogReplica_->becomeFollower(static_cast<rr::Epoch>(role.epoch), static_cast<rr::NodeId>(role.leaderId));
    demote_(role.leaderId, role.epoch);
}

void AdminWorker::onCatalogHoldLeadership(const AdminRoleMsg& role) {
    if (catalogReplica_ != nullptr) catalogReplica_->holdLeadershipForStorage(role.epoch, role.partitionEpoch);
    demote_(-1, role.epoch);
}

void AdminWorker::demote_(int32_t leaderId, int32_t epoch) {
    if (!std::holds_alternative<CatalogHalted>(role_)) setRole_(CatalogFollower{leaderId, epoch});
    // Spec §5.2: an in-flight DDL is answered at once — the retriable NotLeader
    // (its event may still commit under the new leader; the retry dedupes).
    if (auto* a = std::get_if<AwaitingCommit>(&inFlight_)) {
        const DdlRequest req = std::move(a->req);
        inFlight_ = Idle{};
        ++notLeaderDemoted_;
        stampEvidence_();
        answer_(req, kNotLeader, "the catalog leader changed while the DDL awaited its commit");
    }
}

void AdminWorker::onCatalogAlterIsrResponse(const rune::replication::AlterIsrResponseMsg& r) {
    if (catalogReplica_ == nullptr) return;
    catalogReplica_->onAlterIsrResponse(r);
}

// Follower ⇄ CatchingUp ⇄ Serving, from the replica's role and what the applier
// has applied: a leader serves DDL only once every event in its log is applied
// (so the state it validates against is the state its append will extend).
bool AdminWorker::updateRole_() {
    if (catalogReplica_ == nullptr) return false;
    namespace rr = rune::replication;
    const auto epoch = static_cast<int32_t>(catalogReplica_->epoch());
    if (applier_.halted()) {   // terminal: every DDL answers Internal from now on
        if (std::holds_alternative<CatalogHalted>(role_)) return false;
        const cat::ApplyError* e = applier_.haltError();
        setRole_(CatalogHalted{e != nullptr ? e->offset : -1, epoch});
        return true;
    }
    const bool leads = catalogReplica_->role() == rr::Role::Leader;
    if (!leads) {
        if (!std::holds_alternative<CatalogFollower>(role_)) {
            demote_(-1, epoch);
            return true;
        }
        return false;
    }
    const bool caughtUp = caughtUp_();
    if (const auto* c = std::get_if<CatalogLeaderCatchingUp>(&role_); c && caughtUp && !hasInFlight()) {
        spdlog::info("AdminWorker: catalog leader (epoch {}) caught up at offset {} — serving DDL", c->epoch,
                     applier_.nextOffset());
        setRole_(CatalogLeaderServing{c->epoch});
        return true;
    }
    if (const auto* s = std::get_if<CatalogLeaderServing>(&role_); s && !caughtUp && !hasInFlight()) {
        // An appended event this broker has not applied (a commit that timed out):
        // no new event may be built on a state that does not include it.
        setRole_(CatalogLeaderCatchingUp{s->epoch});
        return true;
    }
    if (std::holds_alternative<CatalogFollower>(role_)) {
        setRole_(CatalogLeaderCatchingUp{epoch});
        return true;
    }
    return false;
}

// ─── ingress ────────────────────────────────────────────────────────────────
void AdminWorker::onPeerLinkKeys(const PeerLinkKeysMsg& m) noexcept {
    rune::peerlink::LinkKeys k;
    k.epoch = m.epoch; k.sendMaster = m.sendMaster; k.recvMaster = m.recvMaster; k.install = m.install;
    ++peerLinkKeysSeen_;
    if (catalogPlane_ != nullptr && catalogPlane_->peerSealed() && !catalogPlane_->installPeerLink(m.peer, k)) [[unlikely]]
        spdlog::error("AdminWorker: the link keys of node {} (epoch {}) could not be installed - admin frames "
                      "to/from it are dropped", m.peer, k.epoch);
}

uint32_t AdminWorker::peerLinkEpochForTest(int32_t peer) const noexcept {
    return catalogPlane_ != nullptr ? catalogPlane_->peerLinkEpoch(peer) : 0u;
}

uint64_t AdminWorker::peerUnknownEpochDropsForTest() const noexcept {
    return catalogPlane_ != nullptr ? catalogPlane_->unknownEpochFrames() : 0u;
}

uint64_t AdminWorker::peerFrameDrops() const noexcept {
    return catalogPlane_ != nullptr ? catalogPlane_->peerFrameDrops() : 0u;
}

void AdminWorker::submit(DdlRequest req) {
    // Namespaces Task 7: ListNamespaces is a read of the applied registry — answered here on any broker, never
    // queued, deduped or appended (outside the started/answered pair, like submitGetSchema).
    if (const auto* list = std::get_if<ListNamespacesMsg>(&req)) {
        respondList_(*list);
        return;
    }
    if (pending_.size() >= kMaxPendingDdl) [[unlikely]] {
        ++pendingDdlRefused_;
        // Not started: answered directly (respond_), outside the started/answered pair.
        respond_(req, Response{kInternal, {}, -1, 0, 0, 0, -1, 0});
        return;
    }
    pending_.push_back(std::move(req));
}

void AdminWorker::submitGetSchema(const GetSchemaMsg& msg) {
    // A pure read of the projected registry.  `requestedFingerprint` 0 = latest;
    // non-zero = the exact historical version with that per-version fingerprint.
    const auto* named = reg_.byName(msg.nsId, msg.name);   // namespaces Task 7: scoped to the request's namespace
    if (!named) {
        deps_.respondGet(msg.session, msg.corr, -1, 0, /*partitionCount=*/0, {}, kUnknownTopic);
        return;
    }
    if (msg.requestedFingerprint == rune::protocol::client::kSchemaLineageFingerprint) {
        // #418: the topic's lineage - the latest entry's EVOLVING blob (every field ever declared, with its
        // lifetime), so a typed reader can place a record of any version.
        deps_.respondGet(msg.session, msg.corr, named->topicId, named->version, named->partitionCount,
                         named->descriptorBlob, kOK);
        return;
    }
    const RegistryEntry* e = (msg.requestedFingerprint == 0)
                                 ? named
                                 : reg_.byFingerprint(named->topicId, msg.requestedFingerprint);
    if (!e) {
        deps_.respondGet(msg.session, msg.corr, -1, 0, /*partitionCount=*/0, {}, kUnknownTopic);
        return;
    }
    // The wire blob is the MATERIALIZED-as-of-version descriptor (its fingerprint
    // is e->fingerprint), never the evolving blob; the partition count is the
    // topic's (the latest entry).
    deps_.respondGet(msg.session, msg.corr, e->topicId, e->version, named->partitionCount,
                     rune::codec::serialize(e->descriptor), kOK);
}

void AdminWorker::respondList_(const ListNamespacesMsg& m) {
    namespace pc = rune::protocol::client;
    if (!deps_.respondListNamespaces) return;
    if (catalogReplica_ == nullptr) {   // no catalog here: nothing to list from
        if (deps_.respondError) deps_.respondError(m.session, m.corr, kInternal, "no catalog replica on this broker");
        return;
    }
    // D15: the cluster admin sees every namespace; anyone else only its own.  A page is at most 100 entries
    // (~33 KB); nextAfter = the last id returned when more remain, else 0 (done).  The smallest page is 2, so a
    // first page never ends on `default` (nextAfter 0 would read as done).
    constexpr std::size_t kPage = 100;
    const std::size_t max = m.max == 0 || m.max > kPage ? kPage : std::max<std::size_t>(m.max, 2);
    pc::ListNamespacesResponse resp;
    resp.sessionId     = m.session;
    resp.correlationId = m.corr;
    bool more = false;
    applier_.state().namespaces().forEach([&](const cat::NamespaceRecord& rec) {
        if (more) return;
        if (m.clusterAdmin == 0 && rec.nsId != m.callerNsId) return;
        // afterNsId is exclusive; 0 also starts at the beginning, so the predefined `default` (nsId 0) heads the
        // first page.
        if (m.afterNsId != 0 && rec.nsId <= m.afterNsId) return;
        if (resp.namespaces.size() == max) { more = true; return; }
        pc::NamespaceInfo info;
        info.nsId   = rec.nsId;
        info.name   = rec.name;
        info.state  = static_cast<uint8_t>(cat::tagOf(rec.state));
        // The untouched predefined default carries this broker's boot defaults (it is never an event).
        info.limits = (rec.nsId == rune::catalog::kDefaultNsId && rec.version == 0) ? deps_.nsDefaults.limits
                                                                                      : rec.limits;
        resp.namespaces.push_back(std::move(info));
    });
    resp.nextAfter = more ? resp.namespaces.back().nsId : 0;
    deps_.respondListNamespaces(m.session, m.corr, resp);
}

// ─── doWork ─────────────────────────────────────────────────────────────────
int AdminWorker::doWork(int64_t nowNs) {
    // Task 23 fix round 3 (R2-I1): the max inter-pass gap — a cold-path stall signal independent of every protocol
    // decision (no allocation, no atomics; admin-thread-owned).
    if (lastPassNs_ != 0 && nowNs - lastPassNs_ > maxPassGapNs_) maxPassGapNs_ = nowNs - lastPassNs_;
    lastPassNs_ = nowNs;
    int work = 0;
    if (pendingLive_) [[unlikely]] {   // Task 16: the booted applier goes live, once, on this thread
        if (!applier_.goLive(std::move(*pendingLive_)))
            spdlog::critical("AdminWorker: the booted CatalogApplier refused to go live (phase {}) — the catalog "
                             "will not apply", applier_.phaseName());
        pendingLive_.reset();
        ++work;
    }
    if (pendingSecurityLive_) [[unlikely]] {   // Authentication Task 9: the booted security applier goes live, once
        if (!securityApplier_->goLive(std::move(*pendingSecurityLive_)))
            spdlog::critical("AdminWorker: the booted SecurityApplier refused to go live (phase {}) — __security "
                             "will not apply", securityApplier_->phaseName());
        pendingSecurityLive_.reset();
        ++work;
    }
    if (catalogPlane_ && catalogReplica_) {
        work += catalogPlane_->drivePeerResolve();
        // ONE admin plane: frames are dispatched to the replica their topic id names (-6 catalog, -7 security).
        const AdminLogReplicas replicas{catalogReplica_, securityReplica_};
        work += catalogPlane_->poll(kCatalogFragmentLimit,
            [this, nowNs, &replicas](int32_t topicId, int32_t partition, uint16_t templateId,
                                     const uint8_t* frame, std::size_t len) {
                dispatchAdminLogFrame(replicas, topicId, partition, templateId, frame, len, nowNs, catalogFrameStats_);
            });
    }
    if (securityReplica_) {
        work += securityReplica_->doWork(nowNs);
        if (!securityApplyHeldForTest_) [[likely]] work += securityApplier_->applyCommitted(*securityReplica_);
        work += securityAdmin_->doWork(nowNs);
        // Issue #603: SecurityAdmin may have appended (a single-node / sole-ISR append commits at once): apply that
        // too before the verdict, so "behind" never flips for a record committed and applicable in this same pass.
        // Idle: one compare.
        if (!securityApplyHeldForTest_) [[likely]] work += securityApplier_->applyCommitted(*securityReplica_);
        work += stepSecurityServing_(nowNs);   // after the applier: the verdict follows every event it posted
    }
    if (catalogReplica_) {
        work += catalogReplica_->doWork(nowNs);
        if (!applyHeldForTest_) [[likely]] work += applier_.applyCommitted(*catalogReplica_);
        if (std::holds_alternative<AwaitingCatchUp>(bootReconcile_)) [[unlikely]] work += stepBootReconcile_();
        // Task 12: catalog reads, after the applier (what it serves is the HW the
        // replica pass just computed; a halted applier does not stop reads).
        work += readServer_->pump(nowNs);
    }
    // Authentication Task 10: `__security` reads, after ITS applier (what it serves is the HW its replica pass just
    // computed; a halted applier does not stop reads).
    if (securityReadServer_) work += securityReadServer_->pump(nowNs);
    if (advance_(nowNs)) ++work;
    if (updateRole_()) ++work;
    // Rejections answer at once (nothing in flight): take up to a bounded batch
    // per pass; an accepted DDL goes in flight and ends the batch.
    for (int i = 0; i < 16 && !hasInFlight() && !pending_.empty(); ++i) {
        startNext_(nowNs);
        ++work;
    }
    return work;
}

// Issues #15 / #111 (decision M7): is this broker's `__security` state caught up and fresh?  Posted on change.
int AdminWorker::stepSecurityServing_(int64_t nowNs) {
    if (!deps_.securityServing) return 0;
    namespace rr = rune::replication;
    SecurityCatchUp::Inputs in;
    in.nowNs              = nowNs;
    in.present            = securityReplica_ != nullptr && securityApplier_.has_value();
    in.halted             = in.present && securityApplier_->halted();
    in.leader             = in.present && securityReplica_->role() == rr::Role::Leader;
    in.epoch              = in.present ? static_cast<int32_t>(securityReplica_->epoch()) : 0;
    in.leaderServing      = in.leader && securityAdmin_ && securityAdmin_->isLeader() &&
                            std::get<SecurityLeaderServing>(securityAdmin_->role()).epoch == in.epoch;
    in.leaderAnswers      = in.present ? securityReplica_->leaderAnswers() : 0;
    in.caughtUpWithLeader = in.present && securityReplica_->caughtUpWithLeader();
    in.appliedNext        = in.present && !in.halted ? securityApplier_->nextOffset() : -1;
    in.highWatermark      = in.present ? securityReplica_->leaderCommittedHw() : 0;   // PR #459 F1: never capped at LEO
    const bool caughtUp = securityCatchUp_.step(in);
    const int32_t vEpoch = caughtUp ? securityCatchUp_.verdictEpoch() : -1;
    const bool vLeader = caughtUp && securityCatchUp_.verdictAsLeader();
    const bool vHeld = caughtUp && securityCatchUp_.held();   // issue #460
    // #205: the events this broker knows exist - its own log end (a follower appended them before the leader could
    // commit them, acks=all) or the leader's HW, whichever is further.  Posted in the pass that appended them - AFTER a
    // follower's fast-ack of them has left (it is sent inside the frame dispatch); the gateway also treats a view that
    // moved between the two login legs as superseded, which covers that window unless a thread stalls in it (#518).
    const int64_t knownEnd = in.present ? std::max<int64_t>(securityReplica_->logEndOffset(), in.highWatermark) : -1;
    // Issue #603: is the view behind committed records this broker knows of?  Stepped after the applier ran in this
    // pass (see doWork), so only a held, slow or halted apply leaves it set; posted on its edges only.
    const bool behindEdge = securityBehind_.step(in.present, in.halted, in.appliedNext, in.highWatermark);
    const bool behind = securityBehind_.behind();
    if (behindEdge) [[unlikely]] ++securityBehindEdges_;
    if (behindEdge) [[unlikely]]
        spdlog::warn("AdminWorker: __security view {} (applied {} / committed hw {}){}", behind ? "BEHIND" : "caught up",
                     in.appliedNext, securityBehind_.knownCommittedHw(),
                     behind ? " - grant-checked operations answer AuthUnavailable until it is applied" : "");
    const bool verdictSame = securityServingPosted_ == (caughtUp ? 1 : 0) && securityServingPostedEpoch_ == vEpoch &&
                             securityServingPostedLeader_ == vLeader && securityServingPostedHeld_ == vHeld;
    if (verdictSame && !behindEdge && securityServingPostedKnownEnd_ == knownEnd) [[likely]] return 0;
    securityServingPosted_ = caughtUp ? 1 : 0;
    securityServingPostedEpoch_ = vEpoch;
    securityServingPostedLeader_ = vLeader;
    securityServingPostedHeld_ = vHeld;
    securityServingPostedKnownEnd_ = knownEnd;
    deps_.securityServing(caughtUp, in.highWatermark, vEpoch, vLeader, knownEnd, behind, vHeld);
    if (verdictSame) return 1;   // only the known end / the behind edge moved: not worth this log line
    spdlog::info("AdminWorker: __security {} (role {}, epoch {}, applied {} / hw {})",
                 caughtUp ? "caught up - logins served" : "not caught up - logins answer AuthUnavailable",
                 in.leader ? "leader" : "follower", in.epoch, in.appliedNext, in.highWatermark);
    return 1;
}

// ─── startNext_ — gate, dedupe, build, dry-run, append ──────────────────────
void AdminWorker::startNext_(int64_t nowNs) {
    ++started_;
    DdlRequest req = std::move(pending_.front());
    pending_.pop_front();
    const DdlMeta& meta = metaOf(req);

    // 1. The role gate.  A halted applier accepts no catalog change (spec §5.3).
    if (catalogReplica_ == nullptr) {
        answer_(req, kInternal, "this broker has no __catalog");
        return;
    }
    if (applier_.halted() && !std::holds_alternative<CatalogHalted>(role_)) (void)updateRole_();
    if (const auto* h = std::get_if<CatalogHalted>(&role_)) {
        // Spec §9: a halted broker answers Internal — never a redirect or a retry.
        answer_(req, kInternal, "catalog apply halted at offset " + std::to_string(h->offset));
        return;
    }
    // Fix round 2 (F2): a broker that LEADS __catalog while its boot reconcile awaits catch-up has an untrusted
    // (wiped) history — it appends nothing to it.  Explicit and non-retriable, never NotLeader.
    if (std::holds_alternative<AwaitingCatchUp>(bootReconcile_) &&
        catalogReplica_->role() == rune::replication::Role::Leader) {
        ++ddlRefusedUntrustedLeader_;
        if (ddlRefusedUntrustedLeader_ % 1024 == 1)
            spdlog::critical("AdminWorker: DDL refused — this broker leads __catalog without its history (the local "
                             "catalog was wiped while catalog-made topic dirs exist); restore dataDir/__catalog, or "
                             "let this broker catch up as a follower (refused so far: {})", ddlRefusedUntrustedLeader_);
        answer_(req, kInternal, "this broker leads __catalog without its history (local catalog wiped) — no catalog "
                                "change is accepted until __catalog is restored");
        return;
    }
    if (const auto* f = std::get_if<CatalogFollower>(&role_)) {
        if (meta.requestId.none() && f->leaderId >= 0 && f->leaderId != selfId_) {
            // F37: a pre-v8 client cannot follow a Redirect to the known leader — an
            // explicit InvalidRequest in its own DDL reply, never a wait-out.  (No
            // known leader — an election, a broker starting — is the retriable
            // NotLeader for every client: there is nowhere to route it yet.)
            ++preV8Refused_;
            spdlog::warn("AdminWorker: a pre-v8 client's DDL (session {}) reached a broker that is not the catalog "
                         "leader — answered InvalidRequest: client too old for cluster DDL routing; upgrade "
                         "(refused so far: {})", meta.sessionId, preV8Refused_);
            answer_(req, kInvalidRequest);
            return;
        }
        answer_(req, kNotLeader);
        return;
    }
    if (std::holds_alternative<CatalogLeaderCatchingUp>(role_)) {
        answer_(req, kNotLeader);
        return;
    }

    // 2. A frame the ring decode could not read: answered with its reason
    //    (InvalidSchema for the pre-v8 kinds, InvalidRequest + detail for 74–77).
    if (const auto* bad = std::get_if<MalformedDdl>(&req)) {
        spdlog::warn("AdminWorker: malformed DDL frame (template {}, session {}, corr {}): {} — answered",
                     bad->templateId, bad->session, bad->corr, bad->reason);
        const bool v8Only = bad->templateId >= kTidDropTopic;
        answer_(req, v8Only ? kInvalidRequest : kInvalidSchema,
                v8Only ? "malformed request: " + bad->reason : std::string{});
        return;
    }

    const cat::CatalogState& state = applier_.state();
    // 3. Dedupe (F14) on (requestId, statement fingerprint).
    const uint64_t fp = requestFingerprint(req);
    if (!meta.requestId.none()) {
        if (const auto rec = state.request(meta.requestId)) {
            if (rec->requestFingerprint == fp) {
                answerOk_(req, *rec);   // a retry of a committed DDL: its ORIGINAL result
            } else {
                answer_(req, kInvalidRequest, "requestId reused for a different statement");
            }
            return;
        }
    }

    // 3b. Namespaces Task 7 (D15): the gateway's authorization verdict rides the message — answered after the dedupe,
    //     so a retry of a committed namespace DDL still gets its original result.
    if (const auto* ns = std::get_if<NamespaceDdlMsg>(&req); ns != nullptr && !ns->denial.empty()) {
        answer_(req, kNotAuthorized, ns->denial);
        return;
    }

    // 4. Build the event against the applied catalog (the dry run is its gate).
    Built built = buildEvent_(req, state);
    if (!built) {
        answerRejection_(req, built.error());
        return;
    }
    built->principal.requestFingerprint = fp;
    // Issue #296: a topic the brokers could not open (EMFILE on every replica build) is refused before it commits.
    if (auto refusal = fdBudgetRefusal_(*built, state)) {
        ++ddlRefusedFdBudget_;
        spdlog::warn("AdminWorker: {} (refused so far: {})", *refusal, ddlRefusedFdBudget_);
        answer_(req, kInvalidRequest, *refusal);
        return;
    }
    (void)appendEvent_(req, std::move(*built), nowNs);
}

namespace {
/// Issue #296: the fields of a catalog topic the open-file formula reads (FdBudget.h; a TopicObject is never a sink).
TopicSpec fdSpecOf(int32_t partitions, const rune::protocol::TopicConfig& c) {
    TopicSpec t;
    t.partitionCount   = partitions;
    t.retentionBytes   = c.retentionBytes;
    t.segmentRollBytes = c.segmentRollBytes;
    return t;
}
}  // namespace

std::optional<std::string> AdminWorker::fdBudgetRefusal_(const cat::CatalogEvent& e, const cat::CatalogState& s) const {
    if (deps_.fdLimit == 0) return std::nullopt;   // unknown limit: never refuse
    const bool create = std::holds_alternative<cat::CreateTopicOps>(e.ops);
    const auto* alter = std::get_if<cat::AlterTopicConfigOps>(&e.ops);
    if (!create && alter == nullptr) return std::nullopt;   // views own no log; drops only lower the need
    const uint64_t ready = deps_.readySegmentsPerPartition;
    // PR #611 review: the terms saturate (FdBudget.h), so "does it grow" compares the DDL's own term, never a
    // difference of (possibly saturated) sums.
    const cat::TopicObject* altered = nullptr;
    uint64_t oldTerm = 0, newTerm = 0;
    if (create) {
        const auto& o = std::get<cat::CreateTopicOps>(e.ops);
        newTerm = fdNeedOfTopic(fdSpecOf(o.partitions, o.config), ready);
    } else {
        altered = s.topicByName(e.nsId, e.objectName);
        if (altered == nullptr) return std::nullopt;   // the dry run already refused it
        rune::protocol::TopicConfig next = altered->config;
        cat::applyConfigChanges(next, *alter);   // the resulting config (the apply's own rule)
        oldTerm = fdNeedOfTopic(fdSpecOf(altered->partitions, altered->config), ready);
        newTerm = fdNeedOfTopic(fdSpecOf(altered->partitions, next), ready);
    }
    if (newTerm <= oldTerm) return std::nullopt;   // the need does not grow: admitted even while over budget
    uint64_t before = fdNeedBase(), after = fdNeedBase();
    for (const cat::TopicObject* t : s.topics()) {
        const uint64_t term = fdNeedOfTopic(fdSpecOf(t->partitions, t->config), ready);
        before = fdNeedAdd(before, term);
        after = fdNeedAdd(after, t == altered ? newTerm : term);
    }
    if (create) after = fdNeedAdd(after, newTerm);
    if (after <= deps_.fdLimit) return std::nullopt;
    return std::string(create ? "CREATE TOPIC" : "ALTER TOPIC") + " would raise this broker's estimated open-file need to " +
           std::to_string(after) + ", above its open-file limit " + std::to_string(deps_.fdLimit) + " (now " +
           std::to_string(before) + "); raise LimitNOFILE / --ulimit nofile, or use fewer partitions, a larger "
           "segment.bytes or a smaller retention.bytes";
}

bool AdminWorker::appendEvent_(const DdlRequest& req, cat::CatalogEvent e, int64_t nowNs) {
    const cat::CatalogState& state = applier_.state();
    if (auto err = wireLimitError(e)) {
        answer_(req, kInvalidRequest, *err);
        return false;
    }
    // Task 7 C1: every effect frame of the event — and of every view whose source
    // it changes — must fit the admin→control ring, or it is never written.
    const std::size_t maxFrame = deps_.effectMaxFrameBytes != 0 ? deps_.effectMaxFrameBytes : catalogEffectMaxFrame();
    if (auto refusal = catalogEventEffectsRefusal(state, e, maxFrame)) {
        answer_(req, kInvalidRequest, *refusal);
        return false;
    }
    if (auto enc = cat::encodeInto(e, eventBuf_); !enc) {
        const DdlRejection r = encodeErrorAnswer(enc.error());
        if (r.code == kInternal)
            spdlog::error("AdminWorker: {} '{}' does not encode ({}) — a leader bug; answered Internal, nothing "
                          "appended", cat::kindName(e.kind), cat::escapeNameForLog(e.objectName),
                          cat::encodeErrorName(enc.error()));
        answerRejection_(req, r);
        return false;
    }
    if (eventBuf_.size() > kMaxCatalogEventBytes) {
        answer_(req, kInvalidRequest, "the catalog event is " + std::to_string(eventBuf_.size()) +
                                          " bytes, larger than the " + std::to_string(kMaxCatalogEventBytes) +
                                          "-byte catalog record limit");
        return false;
    }

    uint8_t key[8];
    for (int i = 0; i < 8; ++i) key[i] = static_cast<uint8_t>((e.objectId >> (8 * i)) & 0xff);
    const rune::replication::Offset base = catalogReplica_->appendKeyed(
        std::span<const uint8_t>(key, 8), std::span<const uint8_t>(eventBuf_.data(), eventBuf_.size()),
        /*acksAll=*/true);
    if (base < 0) {
        // Spec §5.5: below minIsr the catalog accepts no change — retriable.
        answer_(req, kNotLeader, "the catalog is below its minIsr");
        return false;
    }
    ++eventsAppended_;
    catalogReplica_->onLeaderLogAppended();   // reconcile LEO / HW, push to followers
    inFlight_ = AwaitingCommit{req, std::move(e), base, nowNs};
    return true;
}

// ─── advance_ — commit + local apply, timeout, halt ─────────────────────────
bool AdminWorker::advance_(int64_t nowNs) {
    auto* a = std::get_if<AwaitingCommit>(&inFlight_);
    if (a == nullptr) return false;
    if (answersHeldForTest_) [[unlikely]] return false;   // Task 22 seam: the DDL is never answered
    // Committed AND applied locally — its effects posted (lastApplied is the
    // EFFECTED prefix: an event staged by a halt never counts).  Checked before
    // the halt: an event applied before a later halt still gets its OK.
    if (applier_.lastApplied().offset >= a->offset) {
        // The answer is what the applied event recorded.
        const DdlMeta& meta = metaOf(a->req);
        std::optional<cat::RequestRecord> rec =
            meta.requestId.none() ? recordOfLastApplied_(a->offset) : applier_.state().request(meta.requestId);
        DdlRequest req = std::move(a->req);
        const int64_t offset = a->offset;
        inFlight_ = Idle{};
        if (!rec || rec->offset != offset) {
            spdlog::error("AdminWorker: the event at offset {} applied but its result is not the in-flight DDL's "
                          "(a leader bug) — answered Internal", offset);
            answer_(req, kInternal);
            return true;
        }
        answerOk_(req, *rec);
        return true;
    }
    if (applier_.halted()) {   // halted at or before the event: it will never be applied here
        DdlRequest req = std::move(a->req);
        const int64_t offset = a->offset;
        inFlight_ = Idle{};
        // Review M4: the event may well have committed (and applied on every
        // other broker) — say so, and how to learn its result.
        answer_(req, kInternal, "catalog apply halted on this broker at offset " +
                                    std::to_string(applier_.haltError() ? applier_.haltError()->offset : -1) +
                                    "; the DDL's event (offset " + std::to_string(offset) +
                                    ") may have committed — retry with the same requestId on another broker for "
                                    "its result");
        return true;
    }
    // Spec §5.5 / §9: the catalog fell below its minIsr while this DDL awaited its commit.  The event
    // cannot commit until a broker returns - answer the retriable NotLeader now (never wait out the
    // client's 15 s deadline).  updateRole_ then holds the leader in CatchingUp (LEO > HW) until the
    // event commits and applies, so a retry with the same requestId gets NotLeader until then, and the
    // original result (dedupe) after - never a second event.
    if (static_cast<int32_t>(catalogReplica_->isr().size()) <
        catalogMinIsr(static_cast<std::size_t>(deps_.brokerCount))) {
        DdlRequest req = std::move(a->req);
        const int64_t offset = a->offset;
        inFlight_ = Idle{};
        spdlog::warn("AdminWorker: the catalog fell below its minIsr while the event at offset {} awaited its "
                     "commit - answered NotLeader (retriable; a retry with the same requestId gets its result "
                     "once it commits)", offset);
        ++notLeaderBelowMinIsr_;
        stampEvidence_();
        answer_(req, kNotLeader, "the catalog fell below its minIsr while the DDL awaited its commit");
        return true;
    }
    if (nowNs - a->startedNs > kDdlTimeoutNs) {
        // F13: the event may still commit — the retriable NotLeader, never
        // Internal.  updateRole_ holds the leader in CatchingUp until it applied it.
        DdlRequest req = std::move(a->req);
        const int64_t offset = a->offset;
        inFlight_ = Idle{};
        spdlog::warn("AdminWorker: the event at offset {} did not commit within {} s — answered NotLeader "
                     "(retriable; a retry with the same requestId gets its result if it commits)",
                     offset, kDdlTimeoutNs / 1'000'000'000LL);
        ++notLeaderCommitTimeout_;
        stampEvidence_();
        answer_(req, kNotLeader, "the catalog event did not commit in time");
        return true;
    }
    return false;
}

std::optional<cat::RequestRecord> AdminWorker::recordOfLastApplied_(int64_t offset) const {
    const CatalogApplier::AppliedAt& la = applier_.lastApplied();
    if (la.offset != offset) return std::nullopt;
    const cat::CatalogState& s = applier_.state();
    cat::RequestRecord r{.offset = la.offset, .kind = la.applied.kind, .objectId = la.applied.objectId,
                         .newVersion = la.applied.newVersion, .topicId = -1, .schemaVersion = 0,
                         .requestFingerprint = 0};
    switch (la.applied.kind) {
    case cat::EventKind::NamespaceCreated:
    case cat::EventKind::NamespaceLimitsSet:
    case cat::EventKind::NamespaceStateChanged:
    case cat::EventKind::NamespaceDeleted:
        return r;   // namespaces Task 7: objectId is the nsId, never a topic's objectId
    case cat::EventKind::CreateTopic:
    case cat::EventKind::CreateView:
    case cat::EventKind::AlterView:
        r.topicId = la.applied.createdTopicId;
        break;
    case cat::EventKind::DropTopic:
    case cat::EventKind::DropView:
        r.topicId = la.applied.droppedTopicId;
        break;
    default:
        if (const cat::TopicObject* t = s.topicByObjectId(la.applied.objectId)) r.topicId = t->topicId;
        break;
    }
    if (const cat::TopicObject* t = s.topicByObjectId(la.applied.objectId); t && t->schema)
        r.schemaVersion = t->schema->schemaVersion;
    return r;
}

std::optional<uint64_t> AdminWorker::viewOutputFingerprint_(const cat::RequestRecord& r) const {
    auto fpOf = [](const std::vector<uint8_t>& blob) -> std::optional<uint64_t> {
        try {
            return rune::codec::fingerprint(rune::codec::deserialize(std::span<const uint8_t>(blob.data(), blob.size())));
        } catch (const std::exception&) {
            return std::nullopt;
        }
    };
    if (const cat::ViewObject* v = applier_.state().viewByObjectId(r.objectId); v && v->version == r.newVersion)
        return fpOf(v->def.outputDescriptorBlob);
    // The view changed since (a retry answered after a later DDL): the event at
    // the record's offset holds the original definition.
    if (catalogReplica_ == nullptr) return std::nullopt;
    const std::vector<uint8_t> data = catalogReplica_->read(r.offset, rune::replication::kDefaultMaxFetchBytes);
    std::optional<uint64_t> out;
    std::size_t pos = 0;
    while (pos < data.size() && !out) {
        rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
        if (!reader.ensureValid()) break;
        const auto tl = static_cast<std::size_t>(reader.totalLength());
        reader.forEachRecord([&](std::string_view, std::string_view val, int64_t off, int64_t) {
            if (off != r.offset) return;
            auto ev = cat::decode(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()), val.size()));
            if (!ev) return;
            if (const auto* c = std::get_if<cat::CreateViewOps>(&ev->ops)) out = fpOf(c->view.outputDescriptorBlob);
            if (const auto* c = std::get_if<cat::AlterViewOps>(&ev->ops)) out = fpOf(c->view.outputDescriptorBlob);
        });
        if (tl == 0) break;
        pos += tl;
    }
    return out;
}

// ─── answers ────────────────────────────────────────────────────────────────
// Every exit of a STARTED DDL ends here — the one place answered_ moves.
void AdminWorker::answer_(const DdlRequest& req, const Response& r) {
    ++answered_;
    respond_(req, r);
}

void AdminWorker::answer_(const DdlRequest& req, uint16_t code, std::string_view detail, int32_t topicId,
                          uint16_t version) {
    answer_(req, Response{code, detail, topicId, version, 0, version, -1, 0});
}

void AdminWorker::answerRejection_(const DdlRequest& req, const DdlRejection& r) {
    answer_(req, r.code, r.detail, r.topicId, r.version);
}

void AdminWorker::answerOk_(const DdlRequest& req, const cat::RequestRecord& r) {
    uint64_t viewFp = 0;
    if (std::holds_alternative<CreateViewMsg>(req)) {
        const std::optional<uint64_t> fp = viewOutputFingerprint_(r);
        if (!fp) {
            // Never an OK carrying a made-up fingerprint (a client pins it).
            spdlog::error("AdminWorker: the CreateView at offset {} cannot be re-read for its output fingerprint — "
                          "answered Internal", r.offset);
            answer_(req, kInternal);
            return;
        }
        viewFp = *fp;
    }
    const int32_t gate = r.kind == cat::EventKind::AlterView ? r.topicId : -1;
    answer_(req, Response{kOK, {}, r.topicId, r.schemaVersion, r.objectId, r.newVersion, gate, viewFp});
}

void AdminWorker::respond_(const DdlRequest& req, const Response& r) {
    // A detail rides an ErrorResponse for the pre-v8 response types (their own
    // frames have no detail field) — InvalidRequest only (spec §9).
    const bool viaError = r.code == kInvalidRequest && !r.detail.empty() && deps_.respondError;
    auto byTemplate = [&](uint16_t tid, int64_t session, int64_t corr) {
        switch (tid) {
        case kTidDefineTopic:
            if (viaError) deps_.respondError(session, corr, r.code, r.detail);
            else if (deps_.respondDefine) deps_.respondDefine(session, corr, r.topicId, r.version, r.code);
            return;
        case kTidAlterTopic:
            if (viaError) deps_.respondError(session, corr, r.code, r.detail);
            else if (deps_.respondAlter) deps_.respondAlter(session, corr, r.topicId, r.version, r.code);
            return;
        case kTidCreateView:
            if (viaError) deps_.respondError(session, corr, r.code, r.detail);
            else if (deps_.respondCreateView)
                deps_.respondCreateView(session, corr, r.topicId, r.viewFingerprint, r.code);
            return;
        default:
            if (deps_.respondDdl)
                deps_.respondDdl(session, corr, r.code, r.objectId, r.newVersion, r.gateTopicId, r.detail);
            return;
        }
    };
    std::visit([&](const auto& m) { byTemplate(requestTemplateId(req), m.session, m.corr); }, req);
}

// ─── event building ─────────────────────────────────────────────────────────
AdminWorker::Built AdminWorker::buildEvent_(const DdlRequest& req, const cat::CatalogState& s) const {
    Built b = std::visit([&](const auto& m) -> Built {
        using M = std::decay_t<decltype(m)>;
        if constexpr (std::is_same_v<M, DefineTopicMsg>)      return buildDefine_(m, s);
        else if constexpr (std::is_same_v<M, AlterTopicMsg>)  return buildAlter_(m, s);
        else if constexpr (std::is_same_v<M, CreateViewMsg>)  return buildCreateView_(m, s);
        else if constexpr (std::is_same_v<M, AlterViewMsg>)   return buildAlterView_(m, s);
        else if constexpr (std::is_same_v<M, DropViewMsg>)    return buildDropView_(m, s);
        else if constexpr (std::is_same_v<M, DropTopicMsg>)   return buildDropTopic_(m, s);
        else if constexpr (std::is_same_v<M, NamespaceDdlMsg>) return buildNamespace_(m, s);
        else if constexpr (std::is_same_v<M, ListNamespacesMsg>) return Rejected(reject(kInternal));   // answered in submit()
        else if constexpr (std::is_same_v<M, MalformedDdl>)   return Rejected(reject(kInternal));   // answered earlier
        else                                                  return buildAlterConfig_(m, s);
    }, req);
    if (!b) return b;

    // The principal, statement and time (spec §4; F22: timestampMs is the
    // leader's wall clock when the event is built for its append).
    const DdlMeta& meta = metaOf(req);
    const int64_t session = std::visit([](const auto& m) { return m.session; }, req);
    b->formatVer                 = cat::kCurrentFormatVer;
    b->statement                 = meta.statement;
    b->principal.sessionId       = meta.sessionId != 0 ? meta.sessionId : session;
    b->principal.clientName      = meta.clientName;
    b->principal.sourceAddress   = meta.sourceAddress;
    b->principal.authenticated   = meta.authenticated;   // authentication Task 12: the session's user ("" = anonymous)
    b->principal.requestId       = meta.requestId;
    b->replayedFrom = meta.replayedFrom;
    b->timestampMs = wallClockMs();
    b->kind        = cat::kindOf(b->ops);

    // The gate (binding 1): the SAME frozen rules every broker applies, dry-run on
    // this state.  The checks above chose the answer's code; anything they did not
    // anticipate is refused here with the library's own reason.
    auto preview = cat::previewFingerprint(s, *b);
    if (!preview) {
        const std::string& why = preview.error().detail;
        switch (b->kind) {
        case cat::EventKind::CreateSchema:
        case cat::EventKind::AlterSchema:
        case cat::EventKind::SetCompatibility:
            return Rejected(reject(kInvalidSchema, why));
        case cat::EventKind::CreateTopic:
            if (std::get<cat::CreateTopicOps>(b->ops).schema) return Rejected(reject(kInvalidSchema, why));
            return Rejected(reject(kInvalidRequest, why));
        default:
            return Rejected(reject(kInvalidRequest, why));
        }
    }
    b->fingerprint = *preview;
    return b;
}

AdminWorker::Built AdminWorker::buildDefine_(const DefineTopicMsg& m, const cat::CatalogState& s) const {
    if (s.namespaces().byId(m.nsId) == nullptr) return Rejected(reject(kUnknownTopic, kNoNamespaceDetail));
    if (!persistenceFromWire(m.persistence)) return Rejected(reject(kInvalidSchema));   // a direct submit
    if (!cat::isValidTopicName(m.name)) return Rejected(reject(kInvalidTopicName));
    if (m.partitionCount < 1) return Rejected(reject(kInvalidSchema));

    // A non-empty descriptor must be well-formed; nextTag starts past every tag.
    uint16_t nextTag = 0;
    if (!m.blob.empty()) {
        try {
            const rune::codec::Descriptor d =
                rune::codec::deserialize(std::span<const uint8_t>(m.blob.data(), m.blob.size()));
            // #667: a NEW definition may not carry a width-0 FixedStr (replay of historical events still accepts it).
            if (const auto* zf = rune::codec::zeroWidthFixedStr(d))
                return Rejected(reject(kInvalidSchema, "FixedStr column '" + zf->name + "' needs a width of at least 1"));
            uint32_t past = static_cast<uint32_t>(d.fields.size());
            for (const auto& f : d.fields) past = std::max<uint32_t>(past, uint32_t{f.id} + 1);
            if (past > 0xFFFF) return Rejected(reject(kInvalidSchema));
            nextTag = static_cast<uint16_t>(past);
        } catch (...) {
            return Rejected(reject(kInvalidSchema));
        }
    }

    cat::CatalogEvent e;
    e.nsId       = m.nsId;
    e.objectName = m.name;
    if (const cat::TopicObject* t = s.topicByName(m.nsId, m.name)) {
        if (t->schema) return Rejected(reject(kTopicExists, {}, t->topicId, t->schema->schemaVersion));
        if (m.blob.empty()) return Rejected(reject(kTopicExists, {}, t->topicId, 0));   // untyped, nothing to adopt
        // Issue #129: adopting a schema is a new version of a LIVE topic (every other producer's records are then
        // validated against it), so it needs ALTER on the topic - the gateway's verdict - not CREATE alone.
        if (!m.adoptDenial.empty()) return Rejected(reject(kNotAuthorized, m.adoptDenial));
        // Final review I1: an EXPLICIT transient define over an untyped DISK topic
        // cannot be honoured (the topic keeps its log backend).
        if (m.persistence == static_cast<uint8_t>(PersistenceMode::Transient) &&
            t->persistence == cat::Persistence::Disk)
            return Rejected(reject(kTopicExists, {}, t->topicId, 0));
        // Schema-less adoption (ruling 3): the untyped topic gains its schema.
        e.objectId    = t->objectId;
        e.prevVersion = t->version;
        e.newVersion  = t->version + 1;
        e.ops = cat::CreateSchemaOps{cat::SchemaDef{m.keyField, m.validate, m.blob, nextTag}};
        return e;
    }
    if (const cat::ViewObject* v = s.viewByName(m.nsId, m.name)) {
        const RegistryEntry* out = reg_.byId(v->def.viewTopicId);
        return Rejected(reject(kTopicExists, {}, v->def.viewTopicId, out ? out->version : uint16_t{0}));
    }
    if (auto fold = foldCollision(s, m.nsId, m.name)) return Rejected(std::move(*fold));

    // Spec §8: the replication factor is bounded by the brokers and the minIsr.
    const int32_t rf = m.replicationFactor;
    if (rf < 0) return Rejected(reject(kInvalidRequest, "replicationFactor " + std::to_string(rf) + " < 0"));
    if (rf > deps_.brokerCount)
        return Rejected(reject(kInvalidRequest, "replicationFactor " + std::to_string(rf) + " > broker count " +
                                                    std::to_string(deps_.brokerCount)));
    // Task 11 fix round 1 (I2): leader-only validation (not a replayed library
    // rule): never commit a topic its controller cannot place — a catalog-halted
    // broker is never a placement target.
    if (const int32_t placeable = placeableBrokers(); rf > placeable) {
        std::string halted;
        for (uint16_t i = 0; i < placement_.haltedCount; ++i)
            halted += (halted.empty() ? "" : ", ") + std::string("broker ") + std::to_string(placement_.halted[i]);
        return Rejected(reject(kInvalidRequest, "replicationFactor " + std::to_string(rf) + " > placeable brokers " +
                                                    std::to_string(placeable) +
                                                    (halted.empty() ? std::string{}
                                                                    : " (" + halted + ": catalog apply halted)")));
    }
    if (rf > 0 && rf < deps_.topicMinIsr)
        return Rejected(reject(kInvalidRequest, "replicationFactor " + std::to_string(rf) + " < minIsr " +
                                                    std::to_string(deps_.topicMinIsr)));
    // THE config rule (one validator, the library's — Task 2).
    if (auto err = cat::topicConfigError(m.config)) return Rejected(reject(kInvalidRequest, *err));

    e.objectId    = s.nextObjectId();
    e.prevVersion = 0;
    e.newVersion  = 1;
    cat::CreateTopicOps op;
    op.topicId           = s.nextTopicId();
    op.partitions        = m.partitionCount;
    op.replicationFactor = m.replicationFactor;
    op.persistence       = m.persistence == 1 ? cat::Persistence::Transient : cat::Persistence::Disk;
    op.config            = m.config;
    if (!m.blob.empty()) op.schema = cat::SchemaDef{m.keyField, m.validate, m.blob, nextTag};
    e.ops = std::move(op);
    return e;
}

AdminWorker::Built AdminWorker::buildAlter_(const AlterTopicMsg& m, const cat::CatalogState& s) const {
    // U1: a view's output is derived from its plan — never altered directly.
    if (s.viewByName(m.nsId, m.topicName)) return Rejected(reject(kViewNotAlterable));
    const cat::TopicObject* t = s.topicByName(m.nsId, m.topicName);
    // F3: an untyped topic has no schema to alter — UnknownTopic, as today.
    if (t == nullptr || !t->schema) return Rejected(reject(kUnknownTopic));
    const int32_t  topicId = t->topicId;
    const uint16_t cur     = t->schema->schemaVersion;
    // Optimistic CAS on the schema version: a lost update is never possible.
    if (m.expectedVersion != 0 && m.expectedVersion != cur) return Rejected(reject(kSchemaConflict, {}, topicId, cur));
    auto ops = rune::codec::decodeAlterOps(std::span<const uint8_t>(m.opsBlob.data(), m.opsBlob.size()));
    if (!ops || ops->empty()) return Rejected(reject(kInvalidSchema, {}, topicId, cur));

    cat::CatalogEvent e;
    e.nsId        = m.nsId;
    e.objectName  = t->name;
    e.objectId    = t->objectId;
    e.prevVersion = t->version;
    e.newVersion  = t->version + 1;
    bool onlySetCompat = true;
    rune::codec::CompatMode mode = t->schema->compat;
    for (const auto& op : *ops) {
        onlySetCompat = onlySetCompat && op.type == rune::codec::AlterOpType::SetCompatibility;
        if (op.type == rune::codec::AlterOpType::SetCompatibility) mode = op.mode;
    }
    if (onlySetCompat) {   // ruling 3: an ALTER of only SET COMPATIBILITY is SetCompatibility
        e.ops = cat::SetCompatibilityOps{mode};
        return e;
    }
    // The compatibility gate (the engine apply runs): SchemaIncompatible.
    if (!rune::codec::checkCompatibility(*ops, mode)) return Rejected(reject(kSchemaIncompatible, {}, topicId, cur));
    // F20: stored resolved to durable tags.
    auto resolved = cat::resolveAlterOps(*t->schema, std::span<const rune::codec::AlterOp>(ops->data(), ops->size()));
    if (!resolved) return Rejected(reject(kInvalidSchema, resolved.error(), topicId, cur));
    e.ops = cat::AlterSchemaOps{std::move(*resolved)};
    return e;
}

std::expected<cat::ViewDef, DdlRejection>
AdminWorker::viewDef_(const std::string& name, const std::string& keyField, uint8_t persistence,
                      const std::vector<uint8_t>& planBlob, int32_t viewTopicId, uint32_t nsId,
                      const cat::CatalogState& s) const {
    namespace rv = rune::view;
    std::optional<rv::ViewPlan> authored =
        rv::deserializeAuthored(std::span<const uint8_t>(planBlob.data(), planBlob.size()));
    if (!authored || authored->sources.empty()) return Rejected(reject(kViewFieldUnresolved));

    // Resolve each source against the applied catalog (descriptors from the
    // registry the applier projects).
    std::vector<const rune::codec::Descriptor*> srcDescs;
    std::vector<std::string> sourceNames;
    srcDescs.reserve(authored->sources.size());
    for (const auto& src : authored->sources) {
        // B9 / D3b: a view's output carries no change events — a view over it
        // would stay silently empty.
        if (s.viewByViewTopicId(src.topicId) != nullptr) return Rejected(reject(kViewSourceIsView));
        const cat::TopicObject* t = s.topicByTopicId(src.topicId);
        // Namespaces Task 7 (D11): a view reads only its own namespace's topics — for every caller, the cluster
        // admin included.  A foreign source answers EXACTLY like a missing one (spec §1, hidden): sources are topic
        // ids a client picks freely, so naming the topic would enumerate another namespace's topics.  The ApplyV2
        // preview (buildEvent_) is the backstop for anything this misses.
        if (t != nullptr && t->nsId != nsId) return Rejected(reject(kViewSourceNotFound));
        const RegistryEntry* e = t != nullptr && t->schema ? reg_.byId(src.topicId) : nullptr;
        if (e == nullptr) return Rejected(reject(kViewSourceNotFound));
        // Phase 2b E: a view that reads a transient topic must itself be transient.
        if (t->persistence == cat::Persistence::Transient && persistence == 0)
            return Rejected(reject(kViewSourceTransient));
        srcDescs.push_back(&e->descriptor);
        sourceNames.push_back(t->name);
    }
    auto compiled = rv::compileView(*authored,
                                    std::span<const rune::codec::Descriptor* const>(srcDescs.data(), srcDescs.size()),
                                    viewTopicId);
    if (!compiled) {
        // CompileError is a message, not an enum: key-related → KeyInvalid, else FieldUnresolved.
        std::string lc = compiled.error().message;
        std::transform(lc.begin(), lc.end(), lc.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return Rejected(reject(lc.find("key") != std::string::npos ? kViewKeyInvalid : kViewFieldUnresolved));
    }
    // Phase 2a co-partitioning: an aggregate view over a multi-partition source is refused.
    const cat::TopicObject* first = s.topicByTopicId(authored->sources[0].topicId);
    if (rv::isAggregateView(compiled->plan) && first->partitions > 1) return Rejected(reject(kViewNotCoPartitioned));

    cat::ViewDef def;
    def.viewTopicId    = viewTopicId;
    def.sinkPartitions = first->partitions;   // co-partitioned with its (first) source
    def.persistence    = persistence == 1 ? cat::Persistence::Transient : cat::Persistence::Disk;
    def.keyField       = keyField;
    def.sourceNames    = std::move(sourceNames);
    // Ruling 11: the fingerprinted plan names its sources by ordinal, never by id.
    rv::ViewPlan normalized = *authored;
    for (std::size_t i = 0; i < normalized.sources.size(); ++i) normalized.sources[i].topicId = static_cast<int32_t>(i);
    def.normalizedPlanBlob   = rv::serializeAuthored(normalized);
    compiled->plan.viewTopicId = viewTopicId;
    def.compiledPlanBlob     = rv::serialize(compiled->plan);
    def.outputDescriptorBlob = rune::codec::serialize(compiled->outputDescriptor);
    (void)name;
    return def;
}

AdminWorker::Built AdminWorker::buildCreateView_(const CreateViewMsg& m, const cat::CatalogState& s) const {
    if (s.namespaces().byId(m.nsId) == nullptr) return Rejected(reject(kUnknownTopic, kNoNamespaceDetail));
    if (m.persistence > 1) return Rejected(reject(kInvalidSchema));   // a direct submit
    if (!cat::isValidTopicName(m.viewName)) return Rejected(reject(kInvalidTopicName));
    if (const cat::ViewObject* v = s.viewByName(m.nsId, m.viewName))
        return Rejected(reject(kViewExists, {}, v->def.viewTopicId));
    if (const cat::TopicObject* t = s.topicByName(m.nsId, m.viewName)) return Rejected(reject(kTopicExists, {}, t->topicId));
    if (auto fold = foldCollision(s, m.nsId, m.viewName)) return Rejected(std::move(*fold));
    auto def = viewDef_(m.viewName, m.keyField, m.persistence, m.planBlob, s.nextTopicId(), m.nsId, s);
    if (!def) return Rejected(std::move(def.error()));
    cat::CatalogEvent e;
    e.nsId        = m.nsId;
    e.objectName  = m.viewName;
    e.objectId    = s.nextObjectId();
    e.prevVersion = 0;
    e.newVersion  = 1;
    e.ops         = cat::CreateViewOps{std::move(*def)};
    return e;
}

AdminWorker::Built AdminWorker::buildAlterView_(const AlterViewMsg& m, const cat::CatalogState& s) const {
    if (m.persistence > 1) return Rejected(reject(kInvalidSchema));   // a direct submit
    const cat::ViewObject* v = s.viewByName(m.nsId, m.name);
    if (v == nullptr) return Rejected(reject(kUnknownTopic, "no view named " + quotedName(m.name)));
    if (m.expectedVersion != 0 && m.expectedVersion != v->version)
        return Rejected(reject(kSchemaConflict, "view " + quotedName(m.name) + " is at version " + std::to_string(v->version),
                               v->def.viewTopicId, static_cast<uint16_t>(std::min<uint32_t>(v->version, 0xFFFF))));
    // Ruling 4: a FRESH sink under the same objectId.
    auto def = viewDef_(m.name, m.keyField, m.persistence, m.planBlob, s.nextTopicId(), m.nsId, s);
    if (!def) return Rejected(std::move(def.error()));
    cat::CatalogEvent e;
    e.nsId        = m.nsId;
    e.objectName  = v->name;
    e.objectId    = v->objectId;
    e.prevVersion = v->version;
    e.newVersion  = v->version + 1;
    e.ops         = cat::AlterViewOps{std::move(*def)};
    return e;
}

AdminWorker::Built AdminWorker::buildDropView_(const DropViewMsg& m, const cat::CatalogState& s) const {
    const cat::ViewObject* v = s.viewByName(m.nsId, m.name);
    if (v == nullptr) return Rejected(reject(kUnknownTopic, "no view named " + quotedName(m.name)));
    cat::CatalogEvent e;
    e.nsId        = m.nsId;
    e.objectName  = v->name;
    e.objectId    = v->objectId;
    e.prevVersion = v->version;
    e.newVersion  = v->version + 1;
    e.ops         = cat::DropViewOps{v->def.viewTopicId};
    return e;
}

AdminWorker::Built AdminWorker::buildDropTopic_(const DropTopicMsg& m, const cat::CatalogState& s) const {
    if (s.viewByName(m.nsId, m.name)) return Rejected(reject(kInvalidRequest, quotedName(m.name) + " is a view; use DROP VIEW"));
    const cat::TopicObject* t = s.topicByName(m.nsId, m.name);
    if (t == nullptr) return Rejected(reject(kUnknownTopic, "no topic named " + quotedName(m.name)));
    if (const cat::ViewObject* v = s.viewReading(m.nsId, m.name))
        return Rejected(reject(kInvalidRequest, "topic " + quotedName(m.name) + " is the source of view " + quotedName(v->name)));
    cat::CatalogEvent e;
    e.nsId        = m.nsId;
    e.objectName  = t->name;
    e.objectId    = t->objectId;
    e.prevVersion = t->version;
    e.newVersion  = t->version + 1;
    e.ops         = cat::DropTopicOps{t->topicId};
    return e;
}

AdminWorker::Built AdminWorker::buildAlterConfig_(const AlterTopicConfigMsg& m, const cat::CatalogState& s) const {
    if (s.viewByName(m.nsId, m.name)) return Rejected(reject(kViewNotAlterable));
    const cat::TopicObject* t = s.topicByName(m.nsId, m.name);
    if (t == nullptr) return Rejected(reject(kUnknownTopic, "no topic named " + quotedName(m.name)));
    if (m.expectedVersion != 0 && m.expectedVersion != t->version)
        return Rejected(reject(kSchemaConflict, "topic " + quotedName(m.name) + " is at version " + std::to_string(t->version),
                               t->topicId));
    // Empty / duplicate keys / out-of-range values: the dry run answers with the
    // library's reason (one validator — the resulting config's topicConfigError).
    cat::CatalogEvent e;
    e.nsId        = m.nsId;
    e.objectName  = t->name;
    e.objectId    = t->objectId;
    e.prevVersion = t->version;
    e.newVersion  = t->version + 1;
    e.ops         = cat::AlterTopicConfigOps{m.changes};
    return e;
}

AdminWorker::Built AdminWorker::buildNamespace_(const NamespaceDdlMsg& m, const cat::CatalogState& s) const {
    namespace pc = rune::protocol::client;
    const cat::NamespaceRegistry& nss = s.namespaces();
    cat::CatalogEvent e;
    switch (static_cast<pc::NamespaceDdlKind>(m.kind)) {
    case pc::NamespaceDdlKind::Create: {
        if (!rune::catalog::validNamespaceName(m.name))
            return Rejected(reject(kInvalidRequest, "invalid namespace name " + quotedName(m.name)));
        if (nss.byName(m.name) != nullptr)
            return Rejected(reject(kTopicExists, "namespace " + quotedName(m.name) + " already exists"));
        // RUNE_MAX_NAMESPACES counts every namespace that exists (`default` and DELETING ones included).
        if (nss.liveCount() >= deps_.maxNamespaces) {
            const uint16_t code = m.clientVersion >= pc::kClientVersionNamespaces ? kNamespaceLimitExceeded
                                                                                  : kInvalidRequest;
            return Rejected(reject(code, "namespace limit reached (" + std::to_string(nss.liveCount()) + " of " +
                                             std::to_string(deps_.maxNamespaces) + ")"));
        }
        const uint32_t nsId = nss.nextNsId();
        e.nsId        = nsId;
        e.objectId    = nsId;
        e.objectName  = m.name;
        e.prevVersion = 0;
        e.newVersion  = 1;
        // The FULL record at request time: absent fields from this broker's current defaults.
        e.ops = cat::NamespaceCreatedOps{m.name, resolveLimits(m.mask, m.limits, deps_.nsDefaults.limits)};
        return e;
    }
    case pc::NamespaceDdlKind::SetLimits: {
        const cat::NamespaceRecord* rec = nss.byName(m.name);
        if (rec == nullptr) return Rejected(reject(kUnknownTopic, "no namespace named " + quotedName(m.name)));
        // The untouched predefined default holds no event-carried limits: its base is the boot defaults.
        const rune::protocol::NamespaceLimits& base =
            rec->nsId == rune::catalog::kDefaultNsId && rec->version == 0 ? deps_.nsDefaults.limits : rec->limits;
        e.nsId        = rec->nsId;
        e.objectId    = rec->nsId;
        e.objectName  = rec->name;
        e.prevVersion = rec->version;
        e.newVersion  = rec->version + 1;
        e.ops = cat::NamespaceLimitsSetOps{resolveLimits(m.mask, m.limits, base)};
        return e;
    }
    }
    return Rejected(reject(kInvalidRequest, "unknown namespace DDL kind " + std::to_string(m.kind)));
}

} // namespace rune::node
