#include "rune/node/AdminWorker.h"
#include <rune/node/TopicName.h>
#include <rune/codec/Serialize.h>  // rune::codec::deserialize/serialize (throws on bad input)
#include <rune/codec/Evolve.h>     // checkCompatibility / applyOps (Task 11)
#include <rune/codec/AlterOps.h>   // decodeAlterOps
#include <rune/view/AggState.h>     // rune::view::isAggregateView (Phase 2a)
#include <rune/view/PlanCompiler.h> // rune::view::compileView (Task 19)
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/WorkerReplicationTransport.h>   // the __catalog plane (Task 6)
#include <rune/protocol/RecordBatch.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <type_traits>
#include <variant>

namespace rune::node {

// Error code constants (protocol-agnostic integers fixed by Task 1 ErrorCode enum):
static constexpr uint16_t kOK             =  0;
static constexpr uint16_t kInternal       =  6;
static constexpr uint16_t kUnknownTopic   =  7;
static constexpr uint16_t kInvalidSchema  = 23;
static constexpr uint16_t kTopicExists    = 25;
static constexpr uint16_t kSchemaIncompatible = 30;  // ALTER compat gate reject
static constexpr uint16_t kSchemaConflict     = 31;  // ALTER stale expectedVersion (CAS)
// Task 19 view-engine error codes (rune::protocol::client::ErrorCode).
static constexpr uint16_t kViewExists         = 32;  // CreateView on an existing name
static constexpr uint16_t kViewSourceNotFound = 33;  // plan references a missing source topic
static constexpr uint16_t kViewFieldUnresolved= 34;  // plan references an unresolved field
static constexpr uint16_t kViewKeyInvalid     = 35;  // key field invalid under the source schema
static constexpr uint16_t kViewNotCoPartitioned = 36;  // aggregate group-by not co-partitioned
static constexpr uint16_t kViewNotAlterable     = 38;  // AlterTopic on a view's output topic
static constexpr uint16_t kInvalidTopicName     = 40;  // DefineTopic/CreateView: name fails TopicName.h (B1/B2)
static constexpr uint16_t kViewSourceIsView     = 41;  // CreateView: a source is another view's output (B9)
static constexpr uint16_t kViewSourceTransient  = 37;  // persisted view over a transient source (Phase 2b E)

AdminWorker::AdminWorker(SchemaRegistry& reg,
                         rune::replication::PartitionReplica* replica,
                         int32_t selfId,
                         std::vector<int32_t> replicaSet,
                         AdminDeps deps,
                         rune::replication::PartitionReplica* viewsReplica,
                         ViewRegistry* viewRegistry,
                         rune::replication::PartitionReplica* catalogReplica,
                         rune::replication::WorkerReplicationTransportImpl<rune::replication::AeronPubRegistry>*
                             catalogPlane)
    : reg_(reg)
    , replica_(replica)
    , selfId_(selfId)
    , replicaSet_(std::move(replicaSet))
    , deps_(std::move(deps))
    , viewsReplica_(viewsReplica)
    , viewRegistry_(viewRegistry)
    , catalogReplica_(catalogReplica)
    , catalogPlane_(catalogPlane) {
    if (catalogReplica_ != nullptr) {
        // One max fetch + the batch framing slack the worker's scratch carries.
        catalogReadScratch_.resize(static_cast<std::size_t>(rune::replication::kDefaultMaxFetchBytes) + 64u * 1024u);
        catalogReplica_->setReadScratch(catalogReadScratch_.data(), catalogReadScratch_.size());
    }
}

// ─── __catalog role (control → admin; catalog history Task 6) ───────────────
// The role is absolute and epoch-stamped (the committed assignment); the
// replica's own role machine handles re-asserts (same-epoch leader = ISR-only
// update; same-epoch same-leader follower = no-op) and refuses a leadership at
// an epoch it already led at (restart fence → asks the controller for a bump).
void AdminWorker::onCatalogRole(const AdminRoleMsg& role) {
    if (catalogReplica_ == nullptr) return;
    namespace rr = rune::replication;
    if (role.leader) {
        std::vector<rr::NodeId> isr;
        isr.reserve(static_cast<std::size_t>(role.isrCount));
        for (int i = 0; i < role.isrCount; ++i)
            isr.push_back(static_cast<rr::NodeId>(role.isr[static_cast<std::size_t>(i)]));
        catalogReplica_->becomeLeader(static_cast<rr::Epoch>(role.epoch), std::move(isr), role.partitionEpoch);
    } else {
        catalogReplica_->becomeFollower(static_cast<rr::Epoch>(role.epoch),
                                        static_cast<rr::NodeId>(role.leaderId));
    }
}

void AdminWorker::onCatalogAlterIsrResponse(const rune::replication::AlterIsrResponseMsg& r) {
    if (catalogReplica_ == nullptr) return;
    catalogReplica_->onAlterIsrResponse(r);
}

// ─── nextSchemaVersion_ — single source of truth for CONTIGUOUS versions ────
// (Task 10). DefineTopic (create, this task) and ALTER (Task 11) both route
// their commit's version through this one helper.
uint16_t AdminWorker::nextSchemaVersion_(int32_t topicId) const {
    const RegistryEntry* latest = reg_.latest(topicId);
    return latest ? static_cast<uint16_t>(latest->version + 1) : kInitialSchemaVersion;
}

// ─── Role events (control → admin) ──────────────────────────────────────────
//
// RULING-2 A: the schema replica's role is driven ONLY here, on the admin thread.
// C (rebuild-on-promotion): on a Follower→Leader edge, replay the log so a
// promoted node re-materialises the registry from __topic_schemas before serving
// DDL as leader.  Time-independent: fires exactly once on the edge, off the log.
void AdminWorker::onRole(const AdminRoleMsg& role) {
    epoch_ = role.epoch;
    if (role.leader) {
        if (replica_) {
            std::vector<rune::replication::NodeId> isr;
            isr.reserve(replicaSet_.size());
            for (int32_t id : replicaSet_)
                isr.push_back(static_cast<rune::replication::NodeId>(id));
            replica_->becomeLeader(static_cast<rune::replication::Epoch>(role.epoch),
                                   std::move(isr));
        }
        // Task 19: the __views replica is admin-owned too — drive its role on the
        // SAME edge (so CreateView's appendKeyed sees a leader replica).
        if (viewsReplica_) {
            std::vector<rune::replication::NodeId> visr;
            visr.reserve(replicaSet_.size());
            for (int32_t id : replicaSet_)
                visr.push_back(static_cast<rune::replication::NodeId>(id));
            viewsReplica_->becomeLeader(static_cast<rune::replication::Epoch>(role.epoch),
                                        std::move(visr));
        }
        leader_ = true;
        if (!wasLeader_) {
            // Follower→Leader edge: rebuild registry from the replicated log.
            rebuildFromLog_();
            if (rebuildFailed_) [[unlikely]] {
                // Surfaced loudly (see rebuildFromLog_ + rebuildFailed()):
                // this node is now serving DDL as leader on top of a
                // registry rebuild that detected and aborted on a corrupt
                // __topic_schemas record — never silently treated as healthy.
                spdlog::critical(
                    "AdminWorker: promoted to LEADER (epoch={}) with a FAILED "
                    "__topic_schemas rebuild — the SchemaRegistry may be "
                    "missing versions past the corrupt record. See the prior "
                    "CORRUPT __topic_schemas record log line.", role.epoch);
            }
            // Task 19b: now that reg_ is rebuilt (sources resolve), recover the
            // durable view catalog from __views and fan out recovery installs.
            recoverViewsOnPromotion_();
            wasLeader_ = true;
        }
    } else {
        if (replica_) {
            replica_->becomeFollower(
                static_cast<rune::replication::Epoch>(role.epoch),
                static_cast<rune::replication::NodeId>(role.leaderId));
        }
        if (viewsReplica_) {
            viewsReplica_->becomeFollower(
                static_cast<rune::replication::Epoch>(role.epoch),
                static_cast<rune::replication::NodeId>(role.leaderId));
        }
        leader_    = false;
        wasLeader_ = false;  // next promotion re-triggers rebuild
        // DDL conformance B4: a DDL op cannot commit on a follower — the HW it would
        // poll is the new leader's, and its appended record may be truncated.
        // Answer it now (Internal: outcome unknown, the client retries on the new
        // leader) instead of riding the 30 s timeout or committing off a follower HW.
        // An op demoted in AwaitingCommit is in doubt (KL2): never applied live, its
        // record replays on a restart if it survives.
        if (hasInFlight())
            answerInFlight_(kInternal, inFlightAlterTopicId_());
    }
}

// ─── Ingress ────────────────────────────────────────────────────────────────
// No-hang fix round 3 (fix-on-sight): the serialized DDL queue is BOUNDED
// (kMaxPendingDdl).  A request beyond it is answered Internal at once — never
// queued without bound (which could also overflow the bounded DDL-answer
// backlog: AdminWorkerAgent::haltEgressBacklogFull_).
void AdminWorker::submitDefineTopic(DefineTopicMsg msg) {
    if (pending_.size() >= kMaxPendingDdl) [[unlikely]] {
        ++pendingDdlRefused_;
        deps_.respondDefine(msg.session, msg.corr, -1, 0, kInternal);
        return;
    }
    pending_.emplace_back(std::in_place_type<DefineTopicMsg>, std::move(msg));
}

void AdminWorker::submitCreateView(CreateViewMsg msg) {
    if (pending_.size() >= kMaxPendingDdl) [[unlikely]] {
        ++pendingDdlRefused_;
        deps_.respondCreateView(msg.session, msg.corr, -1, 0, kInternal);
        return;
    }
    pending_.emplace_back(std::in_place_type<CreateViewMsg>, std::move(msg));
}

void AdminWorker::submitAlterTopic(AlterTopicMsg msg) {
    if (pending_.size() >= kMaxPendingDdl) [[unlikely]] {
        ++pendingDdlRefused_;
        deps_.respondAlter(msg.session, msg.corr, -1, 0, kInternal);
        return;
    }
    pending_.emplace_back(std::in_place_type<AlterTopicMsg>, std::move(msg));
}

void AdminWorker::submitGetSchema(const GetSchemaMsg& msg) {
    // GetSchema is a pure read of the admin-owned registry — no round-trip.
    //
    // Task 13: `msg.requestedFingerprint` selects WHICH version is returned —
    // 0 means latest; non-zero resolves the exact historical version whose
    // per-version (materialized-as-of) fingerprint matches, so a typed client
    // compiled against an older schema can fetch the descriptor it needs.
    const auto* named = reg_.byName(msg.name);
    if (!named) {
        deps_.respondGet(msg.session, msg.corr, -1, 0, /*partitionCount=*/0, {}, kUnknownTopic);
        return;
    }
    const RegistryEntry* e = (msg.requestedFingerprint == 0)
                                 ? named
                                 : reg_.byFingerprint(named->topicId, msg.requestedFingerprint);
    if (!e) {
        // No version of this (known) topic has the requested fingerprint —
        // same wire error as an unknown topic (no dedicated "stale fingerprint"
        // code on the wire yet).
        deps_.respondGet(msg.session, msg.corr, -1, 0, /*partitionCount=*/0, {}, kUnknownTopic);
        return;
    }
    // IMPORTANT: `e->descriptorBlob` is the raw EVOLVING blob (every field ever
    // declared, live or retired) — NOT what a client should rehash. The client
    // typed-gate recomputes fingerprint(deserialize(responseBlob)) and compares
    // it to the fingerprint it compiled against, so the wire blob MUST
    // deserialize to the MATERIALIZED-as-of-`e->version` descriptor (whose
    // fingerprint is exactly `e->fingerprint`, by construction in
    // SchemaRegistry::applyRecord). Serialize `e->descriptor` (materialized),
    // never `e->descriptorBlob` (evolving).
    // Phase 2b Part C: report the TOPIC's partition count (`named`, the latest
    // entry — partition count is a topic property, carried unchanged by every
    // ALTER), so a typed client bound to ANY version routes records by key
    // across every partition instead of all to partition 0.
    deps_.respondGet(msg.session, msg.corr,
                     e->topicId, e->version, named->partitionCount,
                     rune::codec::serialize(e->descriptor), kOK);
}

// ─── control → admin reply ──────────────────────────────────────────────────
void AdminWorker::onCreateTopicReply(const CreateTopicReplyMsg& reply) {
    // Only an op in an AwaitingId state (DefineTopic / CreateView) takes a reply,
    // and only the one whose ticket matches.
    int64_t ticket = 0;
    if (const auto* d = std::get_if<DefineAwaitingId>(&inFlight_))     ticket = d->ticket;
    else if (const auto* v = std::get_if<ViewAwaitingId>(&inFlight_)) ticket = v->ticket;
    else return;   // Stale/mismatched reply — ignore (the op may have timed out).
    if (ticket != reply.ticket) return;

    // DDL conformance B3: the name differs only in ASCII case from an existing topic
    // (they would share one directory on a case-insensitive filesystem).  Nothing
    // was created; answer TopicExists with the existing topic's id — for DefineTopic
    // AND CreateView, before either branch could adopt that topic.
    if (reply.caseConflict && reply.topicId >= 0) {
        answerInFlight_(kTopicExists, reply.topicId);
        return;
    }
    // ── CREATE VIEW branch (Task 19): the reply carries the view's SINK topicId,
    //    which is ALSO the view's own topic id.  Append the durable ViewRecord to
    //    __views and gate on viewsReplica_'s HW (not the schema replica's).
    if (auto* v = std::get_if<ViewAwaitingId>(&inFlight_)) {
        onCreateViewReply_(*v, reply);
        return;
    }

    DefineAwaitingId& op = std::get<DefineAwaitingId>(inFlight_);
    if (reply.topicId < 0) {
        // Control thread failed to create the topic → Internal error.
        deps_.respondDefine(op.msg.session, op.msg.corr, -1, 0, kInternal);
        inFlight_ = Idle{};
        return;
    }
    // Final review I1: the name already named a topic (createTopic is idempotent
    // by name — a RUNE_TOPICS boot topic, a recovered runtime topic).  One that
    // already has a schema (registered under an id this name lookup missed, e.g.
    // a pre-name-trailer record indexed by its descriptor name) is TopicExists,
    // exactly like the up-front byName check.  A schema-less one accepts this
    // schema — with ITS real partition count (below), never the requested one.
    if (reply.existed) {
        if (const RegistryEntry* ex = reg_.byId(reply.topicId)) {
            deps_.respondDefine(op.msg.session, op.msg.corr, ex->topicId, ex->version,
                                kTopicExists);
            inFlight_ = Idle{};
            return;
        }
        // Final review I2 (KL1b closed): a runtime Compact topic is a CREATE VIEW
        // sink — committed, in doubt (KL2) or an orphan.  Never adopted as a typed
        // topic: its view record (if any) would then share an id with this schema
        // record and the next boot would refuse the dual claim.  TopicExists.
        if (reply.existingIsRuntimeCompact) {
            deps_.respondDefine(op.msg.session, op.msg.corr, reply.topicId, 0, kTopicExists);
            inFlight_ = Idle{};
            return;
        }
    }
    const int32_t partitionCount = reply.partitionCount > 0 ? reply.partitionCount
                                                            : op.msg.partitionCount;
    // Phase 2b E: AUTHORITATIVE mode from the control thread (same bug class as the
    // partition count): a schema-less existing topic keeps its own log backend.
    // No-hang wave (R1): an unknown mode byte is never defaulted to Disk — the
    // define fails explicitly (Internal), nothing is recorded, the worker goes Idle.
    const auto mode = reply.persistence != CreateTopicReplyMsg::kPersistenceNotReported
                          ? persistenceFromWire(reply.persistence)
                          : persistenceFromWire(op.msg.persistence);
    if (!mode) {
        deps_.respondDefine(op.msg.session, op.msg.corr, -1, 0, kInternal);
        inFlight_ = Idle{};
        return;
    }
    const PersistenceMode persistence = *mode;

    // Final review I1: an EXPLICIT `transient topic` define over an existing
    // schema-less DISK topic cannot be honoured (the topic keeps its log backend);
    // answering OK would hand the client a topic that silently is not transient.
    // TopicExists, nothing recorded.  The reverse (no byte / disk request over a
    // transient topic) stays accepted and records Transient: old clients send no
    // byte and must keep adopting static persistence=transient topics.
    if (reply.existed &&
        op.msg.persistence == static_cast<uint8_t>(PersistenceMode::Transient) &&
        persistence == PersistenceMode::Disk) {
        deps_.respondDefine(op.msg.session, op.msg.corr, reply.topicId, 0, kTopicExists);
        inFlight_ = Idle{};
        return;
    }

    // ── AWAITING_COMMIT ── append the durable TopicSchema record.
    TopicSchema ts;
    ts.topicId        = reply.topicId;
    ts.version        = nextSchemaVersion_(reply.topicId);
    ts.partitionCount = partitionCount;   // authoritative (I1)
    ts.keyField       = op.msg.keyField;
    ts.validate       = op.msg.validate;
    ts.descriptorBlob = op.msg.blob;
    // DefineTopic (create): nextTag starts past every field the fresh schema
    // already declares, so the first ALTER ADD (Task 11) allocates a tag that
    // doesn't collide with an existing one.
    ts.nextTag        = op.fieldCount;
    // Durable topic name (independent of the descriptor's name): boot recovery
    // recreates the topic under it (RuntimeTopicRecovery).
    ts.topicName      = op.msg.name;
    ts.persistence    = persistence;   // authoritative (reply)

    // D: leader guard.  If we are not the leader, DDL cannot be committed here.
    if (!leader_ || !replica_) {
        deps_.respondDefine(op.msg.session, op.msg.corr, -1, 0, kInternal);
        inFlight_ = Idle{};
        return;
    }

    const std::vector<uint8_t> payload = ts.serialize();
    // Task 15b: the __topic_schemas log is CleanupPolicy::Compact, and the
    // Kafka-style LogCleaner compacts on EXACTLY the key stored here via
    // appendKeyed (it has no separate key-extractor for this log) — so a
    // topicId-only key would make every version of a topic collide on the
    // SAME compaction key and the cleaner would physically delete all but the
    // latest version.  Key by the COMPOSITE (topicId:i32 LE, version:u16 LE)
    // = 6 bytes instead, so every version is its own distinct compaction key
    // and survives compaction (old-fingerprint client binding + multi-version
    // replay depend on every version staying durable).
    uint8_t keyBuf[6];
    const uint32_t u = static_cast<uint32_t>(reply.topicId);
    keyBuf[0] = static_cast<uint8_t>(u & 0xff);
    keyBuf[1] = static_cast<uint8_t>((u >> 8) & 0xff);
    keyBuf[2] = static_cast<uint8_t>((u >> 16) & 0xff);
    keyBuf[3] = static_cast<uint8_t>((u >> 24) & 0xff);
    const uint16_t v = ts.version;
    keyBuf[4] = static_cast<uint8_t>(v & 0xff);
    keyBuf[5] = static_cast<uint8_t>((v >> 8) & 0xff);

    const rune::replication::Offset base =
        replica_->appendKeyed(std::span<const uint8_t>(keyBuf, 6),
                              std::span<const uint8_t>(payload.data(), payload.size()),
                              /*acksAll=*/true);
    if (base < 0) {
        // Gated (minIsr not met) → Internal error to the client.
        deps_.respondDefine(op.msg.session, op.msg.corr, -1, 0, kInternal);
        inFlight_ = Idle{};
        return;
    }
    // D: acksAll → wait until HW covers the appended offset (majority-durable).
    // Single-record keyed batch → last == base.
    DefineAwaitingCommit next{
        .msg            = std::move(op.msg),
        .startedNs      = op.startedNs,
        .fieldCount     = op.fieldCount,
        .topicId        = reply.topicId,
        .partitionCount = partitionCount,
        .persistence    = persistence,
        .appendedLast   = base,
    };
    replica_->onLeaderLogAppended();  // reconcile LEO/HW + push to followers
    inFlight_ = std::move(next);
}

// ─── onCreateViewReply_ — CREATE VIEW sink id came back; append the ViewRecord ─
void AdminWorker::onCreateViewReply_(ViewAwaitingId& op, const CreateTopicReplyMsg& reply) {
    if (reply.topicId < 0) {
        deps_.respondCreateView(op.view.session, op.view.corr, -1, 0, kInternal);
        inFlight_ = Idle{};
        return;
    }
    // The view name already named a topic the schema registry does not know (an
    // untyped boot topic): createTopic handed back THAT topic, not a fresh sink.
    // Adopting it would fold the view's output into an unrelated (or its own
    // source) topic — reject; nothing was created, so nothing to undo.
    // Task 14 (KL1 follow-up): EXCEPT a retry's own orphan sink — a runtime topic
    // with exactly this sink's spec (sinkSpecMatches, decided by the control thread)
    // that no TopicSchema / ViewRecord claims.  It is adopted: the ViewRecord below
    // claims it, and its commit promotes the dir marker (topicCommitted).
    const bool adoptOrphanSink = reply.existed && reply.sinkSpecMatches &&
                                 reg_.byId(reply.topicId) == nullptr &&
                                 viewRegistry_ != nullptr &&
                                 viewRegistry_->byViewTopicId(reply.topicId) == nullptr;
    if (reply.existed && !adoptOrphanSink) {
        deps_.respondCreateView(op.view.session, op.view.corr, reply.topicId, 0, kTopicExists);
        inFlight_ = Idle{};
        return;
    }
    // D: leader guard + views catalog wired.
    if (!leader_ || !viewsReplica_ || !viewRegistry_) {
        deps_.respondCreateView(op.view.session, op.view.corr, -1, 0, kInternal);
        inFlight_ = Idle{};
        return;
    }

    // Stamp the authoritative sink id onto the compiled plan (it is the view's
    // own topic id) BEFORE serializing it into the durable ViewRecord.
    op.compiledPlan.viewTopicId = reply.topicId;

    ViewRecord rec;
    rec.viewTopicId     = reply.topicId;
    rec.viewName        = op.view.viewName;
    rec.compiledPlanBlob = rune::view::serialize(op.compiledPlan);
    rec.persistence     = op.view.persistence;
    // Task 19b: persist the OUTPUT descriptor so a cold restart can re-register
    // the view's typed output topic (GetTopicSchema / openView) EXACTLY as
    // created — its field names + FixedStr widths (hence its fingerprint) are
    // NOT reconstructable from the compiled plan alone.
    rec.outputDescriptorBlob = rune::codec::serialize(op.viewOutDesc);

    const std::vector<uint8_t> payload = rec.serialize();
    // __views is CleanupPolicy::Compact and keyed by the viewTopicId (4-byte LE
    // int32) — the compaction key the cleaner uses — so the latest record per
    // view survives compaction (views are not versioned: one record per view id).
    uint8_t keyBuf[4];
    const uint32_t u = static_cast<uint32_t>(reply.topicId);
    keyBuf[0] = static_cast<uint8_t>(u & 0xff);
    keyBuf[1] = static_cast<uint8_t>((u >> 8) & 0xff);
    keyBuf[2] = static_cast<uint8_t>((u >> 16) & 0xff);
    keyBuf[3] = static_cast<uint8_t>((u >> 24) & 0xff);

    const rune::replication::Offset base =
        viewsReplica_->appendKeyed(std::span<const uint8_t>(keyBuf, 4),
                                   std::span<const uint8_t>(payload.data(), payload.size()),
                                   /*acksAll=*/true);
    if (base < 0) {
        deps_.respondCreateView(op.view.session, op.view.corr, -1, 0, kInternal);
        inFlight_ = Idle{};
        return;
    }
    // The sink's partition count: the AUTHORITATIVE one the control thread
    // reported (final review I1); a reply that did not report one falls back to
    // the source's (the sink is requested co-partitioned with it).
    int32_t sinkPartitions = reply.partitionCount;
    if (sinkPartitions <= 0) {
        sinkPartitions = 1;
        if (!op.compiledPlan.sourceTopicId.empty()) {
            if (const RegistryEntry* se = reg_.byId(op.compiledPlan.sourceTopicId[0]))
                sinkPartitions = se->partitionCount;
        }
    }
    ViewAwaitingCommit next{
        .view              = std::move(op.view),
        .startedNs         = op.startedNs,
        .outputFingerprint = op.compiledPlan.outputFingerprint,
        .viewOutDesc       = std::move(op.viewOutDesc),
        .viewSrcDesc       = std::move(op.viewSrcDesc),
        .partitionCount    = sinkPartitions,
        .viewRecord        = std::move(rec),         // applied on the HW gate (advance_)
        .appendedLast      = base,
    };
    viewsReplica_->onLeaderLogAppended();
    inFlight_ = std::move(next);
}

// ─── doWork ─────────────────────────────────────────────────────────────────
int AdminWorker::doWork(int64_t nowNs) {
    int work = 0;

    // 1. Drive the replica(s) (RULING-2 A: admin thread SOLELY drives doWork).
    if (replica_)      work += replica_->doWork(nowNs);
    if (viewsReplica_) work += viewsReplica_->doWork(nowNs);   // Task 19: __views

    // 1b. Catalog history Task 6: the __catalog plane (inbound frames applied
    //     inline, zero-copy) and then the replica (push / fetch / ISR / HW).
    //     DR-15: a peer publication unresolved at startup is re-added here.
    if (catalogPlane_ && catalogReplica_) {
        work += catalogPlane_->drivePeerResolve();
        work += catalogPlane_->poll(kCatalogFragmentLimit,
            [this, nowNs](int32_t topicId, int32_t partition, uint16_t templateId,
                          const uint8_t* frame, std::size_t len) {
                dispatchCatalogFrame(*catalogReplica_, topicId, partition, templateId, frame, len,
                                     nowNs, catalogFrameStats_);
            });
    }
    if (catalogReplica_) work += catalogReplica_->doWork(nowNs);

    // 2. Advance the in-flight DDL (HW check / commit).
    if (advance_(nowNs)) ++work;

    // 3. Start the next queued DefineTopic if idle.
    if (!hasInFlight() && !pending_.empty()) {
        startNext_(nowNs);
        ++work;
    }
    return work;
}

// ─── startNext_ — PARSING → name-lookup → CREATING (request id) ──────────────
void AdminWorker::startNext_(int64_t nowNs) {
    ++started_;
    PendingDdl pd = std::move(pending_.front());
    pending_.pop_front();

    // Final review M2: the last promotion's catalog rebuild failed — the registries
    // may miss records past the corrupt one (a create could adopt a committed
    // topic's sink, a define re-use a recorded name).  Serve no DDL: Internal.
    if ((rebuildFailed_ || viewsRebuildFailed_) && leader_) [[unlikely]] {
        ++refusedForFailedRebuild_;
        std::visit([&](const auto& m) {
            using M = std::decay_t<decltype(m)>;
            if constexpr (std::is_same_v<M, DefineTopicMsg>)
                deps_.respondDefine(m.session, m.corr, -1, 0, kInternal);
            else if constexpr (std::is_same_v<M, CreateViewMsg>)
                deps_.respondCreateView(m.session, m.corr, -1, 0, kInternal);
            else
                deps_.respondAlter(m.session, m.corr, -1, 0, kInternal);
        }, pd);
        return;
    }

    if (auto* alter = std::get_if<AlterTopicMsg>(&pd)) {
        startAlter_(std::move(*alter), nowNs);
        return;
    }
    if (auto* view = std::get_if<CreateViewMsg>(&pd)) {
        startCreateView_(std::move(*view), nowNs);
        return;
    }

    DefineTopicMsg msg = std::move(std::get<DefineTopicMsg>(pd));

    // ── malformed ── No-hang wave (R1): an unknown persistence byte on a
    // directly submitted request is rejected InvalidSchema, nothing started —
    // never cast, never defaulted to Disk.  (The ring decode drops the blob for
    // the same byte, which the PARSING step below also answers InvalidSchema.)
    if (!persistenceFromWire(msg.persistence)) {
        deps_.respondDefine(msg.session, msg.corr, -1, 0, kInvalidSchema);
        return;
    }

    // ── DDL conformance B1/B2 ── the name becomes dataDir/topics/<name>/: one safe
    //    path segment, never an internal "__" name.  Nothing is created.
    if (!isValidTopicName(msg.name)) {
        deps_.respondDefine(msg.session, msg.corr, -1, 0, kInvalidTopicName);
        return;
    }
    // ── DDL conformance B6 ── a topic has at least one partition.
    if (msg.partitionCount < 1) {
        deps_.respondDefine(msg.session, msg.corr, -1, 0, kInvalidSchema);
        return;
    }

    // ── PARSING ── validate the descriptor blob is well-formed, and capture
    // its field count in the same pass (seeds nextTag for the create path —
    // see DefineAwaitingId::fieldCount — avoiding a second deserialize of the same blob
    // later).
    std::size_t fieldCount = 0;
    try {
        const rune::codec::Descriptor parsed = rune::codec::deserialize(
            std::span<const uint8_t>(msg.blob.data(), msg.blob.size()));
        fieldCount = parsed.fields.size();
    } catch (...) {
        deps_.respondDefine(msg.session, msg.corr, -1, 0, kInvalidSchema);
        return;
    }

    // ── name-lookup ── already exists → TopicExists.
    if (const auto* ex = reg_.byName(msg.name)) {
        deps_.respondDefine(msg.session, msg.corr, ex->topicId, ex->version,
                            kTopicExists);
        return;
    }

    // ── leader guard (D) ── DDL is leader-only.
    if (!leader_) {
        deps_.respondDefine(msg.session, msg.corr, -1, 0, kInternal);
        return;
    }

    // ── CREATING ── post a create-topic request to the control thread (ring).
    DefineAwaitingId op{
        .msg        = std::move(msg),
        .ticket     = nextTicket_++,
        .startedNs  = nowNs,
        .fieldCount = static_cast<uint16_t>(fieldCount),
    };

    if (!deps_.requestCreateTopic(op.ticket, op.msg.partitionCount, op.msg.name,
                                  /*keyField=*/"", /*cleanupPolicy=*/0,
                                  op.msg.persistence)) {
        // Ring full (should not happen for rare DDL) → Internal; requeue would risk
        // reordering, so fail fast and let the client retry.
        deps_.respondDefine(op.msg.session, op.msg.corr, -1, 0, kInternal);
        return;
    }
    inFlight_ = std::move(op);
}

// ─── startCreateView_ — serialized CREATE VIEW (compile + validate + sink) ────
//
// Runs on the admin thread, reusing the DDL state machine so a CREATE VIEW is
// serialized with every other DDL op (single inFlight_).  Like DefineTopic it
// goes through the createTopic id round-trip (the view's keyed+Compact SINK
// topic) → AwaitingId; the durable ViewRecord is appended when the sink id comes
// back (onCreateTopicReply), and commit (advance_) applies it to the registry,
// registers the view's output descriptor and fans the compiled plan to workers.
void AdminWorker::startCreateView_(CreateViewMsg msg, int64_t nowNs) {
    namespace rv = rune::view;

    // ── malformed ── the ring decode rejected an unknown persistence byte, or
    // (no-hang wave R1) a directly submitted request carries one: rejected
    // InvalidSchema, never treated as "not disk" / recorded as-is.
    if (msg.malformed || msg.persistence > 1) {
        deps_.respondCreateView(msg.session, msg.corr, -1, 0, kInvalidSchema);
        return;
    }

    // ── DDL conformance B1/B2 ── the view name is its sink topic's directory name.
    if (!isValidTopicName(msg.viewName)) {
        deps_.respondCreateView(msg.session, msg.corr, -1, 0, kInvalidTopicName);
        return;
    }

    // ── leader guard (D) ── DDL is leader-only; views catalog must be wired.
    if (!leader_ || !viewsReplica_ || !viewRegistry_) {
        deps_.respondCreateView(msg.session, msg.corr, -1, 0, kInternal);
        return;
    }

    // ── ViewExists ── a view with this name already committed.
    if (const ViewRecord* ex = viewRegistry_->byName(msg.viewName)) {
        deps_.respondCreateView(msg.session, msg.corr, ex->viewTopicId, 0, kViewExists);
        return;
    }

    // ── TopicExists ── the view name already names a TYPED topic (a source, or
    //    any DefineTopic'd topic).  The view's sink IS a topic of that name, so
    //    the control thread's idempotent createTopic would hand back the
    //    existing topic as the sink (`CREATE VIEW characters ... FROM
    //    characters` → feedback loop + the source's schema overwritten).  An
    //    UNTYPED existing topic is caught on the reply (`existed`).
    if (const RegistryEntry* ex = reg_.byName(msg.viewName)) {
        deps_.respondCreateView(msg.session, msg.corr, ex->topicId, 0, kTopicExists);
        return;
    }

    // ── deserialize the authored plan ──
    std::optional<rv::ViewPlan> authored =
        rv::deserializeAuthored(std::span<const uint8_t>(msg.planBlob.data(), msg.planBlob.size()));
    if (!authored || authored->sources.empty()) {
        deps_.respondCreateView(msg.session, msg.corr, -1, 0, kViewFieldUnresolved);
        return;
    }

    // ── resolve each source topic's current descriptor from the schema registry ──
    // A missing source => ViewSourceNotFound (resolved HERE, not via CompileError,
    // so the wire code is exact — compileView only sees descriptors that exist).
    std::vector<const rune::codec::Descriptor*> srcDescs;
    srcDescs.reserve(authored->sources.size());
    for (const auto& s : authored->sources) {
        // DDL conformance B9 / D3b: a view's output carries no change events (its
        // sink is a materialised table, not a changelog) until Phase 3 — a view over
        // it would stay silently empty.  Reject; nothing is created.
        if (viewRegistry_->byViewTopicId(s.topicId) != nullptr) {
            deps_.respondCreateView(msg.session, msg.corr, -1, 0, kViewSourceIsView);
            return;
        }
        const RegistryEntry* e = reg_.byId(s.topicId);
        if (!e) {
            deps_.respondCreateView(msg.session, msg.corr, -1, 0, kViewSourceNotFound);
            return;
        }
        // Phase 2b E: a view that reads a transient topic must itself be transient
        // (msg.persistence == 1) — a persisted view would checkpoint rows whose
        // source data is gone after a restart.  The error path carries a code
        // only: "<source> is transient; a view over it must be transient too
        // (use: create transient view ...)".
        if (e->persistence == PersistenceMode::Transient && msg.persistence == 0) {
            deps_.respondCreateView(msg.session, msg.corr, -1, 0, kViewSourceTransient);
            return;
        }
        srcDescs.push_back(&e->descriptor);
    }

    // ── compile + validate ── (viewTopicId stamped later, on the sink reply) ──
    std::expected<rv::CompileResult, rv::CompileError> compiled =
        rv::compileView(*authored,
                        std::span<const rune::codec::Descriptor* const>(srcDescs.data(), srcDescs.size()),
                        /*viewTopicId=*/-1);
    if (!compiled) {
        // CompileError is a human-readable message, not an enum — classify the
        // common categories to the right wire error code (key-related → KeyInvalid,
        // otherwise field/column resolution → FieldUnresolved).
        std::string lc = compiled.error().message;
        std::transform(lc.begin(), lc.end(), lc.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        const uint16_t code = (lc.find("key") != std::string::npos)
                                  ? kViewKeyInvalid : kViewFieldUnresolved;
        deps_.respondCreateView(msg.session, msg.corr, -1, 0, code);
        return;
    }

    // Phase 2a co-partitioning: an aggregate view groups by a VALUE field, but
    // records are partitioned by their 8-byte wire key, and under table semantics
    // a contributor IS its wire key. The broker never enforces wire key ==
    // declared keyField, so even "group-by == source keyField" does not keep a
    // group's contributors on one partition (it either makes every group
    // single-contributor or silently splits groups). Every aggregate view over
    // a source with more than one partition is therefore rejected.
    if (rv::isAggregateView(compiled->plan)) {
        const RegistryEntry* coSrc = reg_.byId(authored->sources[0].topicId);
        if (coSrc != nullptr && coSrc->partitionCount > 1) {
            deps_.respondCreateView(msg.session, msg.corr, -1, 0, kViewNotCoPartitioned);
            return;
        }
    }

    // ── CREATING ── post a keyed+Compact SINK create request to the control
    // thread; the view sink is co-partitioned with its (single) source (spec §11.2).
    ViewAwaitingId op{
        .view         = std::move(msg),
        .ticket       = nextTicket_++,
        .startedNs    = nowNs,
        .compiledPlan = std::move(compiled->plan),
        .viewOutDesc  = std::move(compiled->outputDescriptor),
        .viewSrcDesc  = *srcDescs[0],   // single-source (co-partition-trivial)
    };

    const RegistryEntry* srcEntry = reg_.byId(authored->sources[0].topicId);
    const int32_t sinkPartitions  = srcEntry ? srcEntry->partitionCount : 1;

    if (!deps_.requestCreateTopic(op.ticket, sinkPartitions, op.view.viewName,
                                  op.view.keyField, /*cleanupPolicy=*/1 /*Compact*/,
                                  /*persistence=*/0 /*Disk in Part E; follows the view's persistence once sinks carry change events (parked)*/)) {
        deps_.respondCreateView(op.view.session, op.view.corr, -1, 0, kInternal);
        return;
    }
    inFlight_ = std::move(op);
}

// ─── startAlter_ — serialized ALTER TOPIC (CAS + compat gate + atomic apply) ──
//
// Runs on the admin thread, reusing the DDL state machine so an ALTER is
// serialized with CREATE and other ALTERs (single inFlight_).  Unlike CREATE it
// SKIPS AwaitingId — the topic already exists, so there is no id round-trip to
// the control thread; it commits synchronously to the append/AwaitingCommit path.
void AdminWorker::startAlter_(AlterTopicMsg msg, int64_t nowNs) {
    // ── leader guard (D) ── DDL is leader-only.
    if (!leader_ || !replica_) {
        deps_.respondAlter(msg.session, msg.corr, -1, 0, kInternal);
        return;
    }

    // U1: a view's output schema is registered in reg_ under the view's name but is
    //     derived from its plan — never altered directly.  Nothing appended.
    if (viewRegistry_ && viewRegistry_->byName(msg.topicName)) {
        deps_.respondAlter(msg.session, msg.corr, -1, 0, kViewNotAlterable);
        return;
    }

    // a. resolve the topic's LATEST applied schema.
    const RegistryEntry* latest = reg_.byName(msg.topicName);
    if (!latest) {
        deps_.respondAlter(msg.session, msg.corr, -1, 0, kUnknownTopic);
        return;
    }
    const int32_t  topicId    = latest->topicId;
    const uint16_t curVersion = latest->version;

    // b. optimistic CAS — a non-zero expectedVersion that no longer matches the
    //    latest version means another ALTER landed first; reject (NO lost update).
    if (msg.expectedVersion != 0 && msg.expectedVersion != curVersion) {
        deps_.respondAlter(msg.session, msg.corr, topicId, curVersion, kSchemaConflict);
        return;
    }

    // c. decode the ops blob (bounds-checked; nullopt on any truncation).
    auto ops = rune::codec::decodeAlterOps(
        std::span<const uint8_t>(msg.opsBlob.data(), msg.opsBlob.size()));
    if (!ops) {
        deps_.respondAlter(msg.session, msg.corr, topicId, curVersion, kInvalidSchema);
        return;
    }

    // d. effective compatibility mode: a SET_COMPATIBILITY op (if present) sets
    //    both the mode the gate checks AND the mode persisted on the new record;
    //    otherwise the current mode carries forward.
    rune::codec::CompatMode mode = latest->compatMode;
    for (const auto& op : *ops)
        if (op.type == rune::codec::AlterOpType::SetCompatibility) mode = op.mode;

    // e. compatibility gate (e.g. DROP is rejected under Forward).
    if (!rune::codec::checkCompatibility(*ops, mode)) {
        deps_.respondAlter(msg.session, msg.corr, topicId, curVersion, kSchemaIncompatible);
        return;
    }

    // f. apply the ops to the CURRENT EVOLVING descriptor (all fields incl.
    //    retired), bumping a fresh contiguous version.  applyOps is atomic: on
    //    any failed op it returns an error and leaves `nextTag` untouched, so a
    //    rejected ALTER appends nothing and does not bump the version.
    rune::codec::Descriptor evolving;
    try {
        evolving = rune::codec::deserialize(std::span<const uint8_t>(
            latest->descriptorBlob.data(), latest->descriptorBlob.size()));
    } catch (...) {
        // The stored evolving blob must be well-formed (it was validated on the
        // way in); a throw here is an internal invariant breach, not client error.
        deps_.respondAlter(msg.session, msg.corr, topicId, curVersion, kInternal);
        return;
    }
    uint16_t       nextTag    = latest->nextTag;
    const uint16_t newVersion = static_cast<uint16_t>(curVersion + 1);  // contiguous, computed ONCE
    auto res = rune::codec::applyOps(evolving, *ops, nextTag, newVersion);
    if (!res) {
        // Surface the EvolveError as InvalidSchema (message available in res.error()).
        deps_.respondAlter(msg.session, msg.corr, topicId, curVersion, kInvalidSchema);
        return;
    }

    // DDL conformance B8 / D3a: a RENAME of the key field moves keyField with it.
    // Ops apply in order, so a chain (id -> a, a -> b) ends at b.  Only reached when
    // applyOps succeeded (a rejected ALTER changes nothing).
    std::string keyField = latest->keyField;
    for (const auto& op : *ops) {
        if (op.type == rune::codec::AlterOpType::Rename && !keyField.empty() && op.name == keyField)
            keyField = op.newName;
    }

    // g. build the new durable TopicSchema (same id/partitions/validate; keyField
    //    follows a key-field RENAME (B8);
    //    NEW evolving descriptor + post-applyOps nextTag + effective compatMode).
    TopicSchema ts;
    ts.topicId        = topicId;
    ts.version        = newVersion;
    ts.partitionCount = latest->partitionCount;
    ts.keyField       = keyField;                          // follows a key-field RENAME (B8)
    ts.validate       = latest->validator.has_value();   // same as current
    ts.descriptorBlob = rune::codec::serialize(*res);     // the NEW evolving descriptor
    ts.nextTag        = nextTag;                           // advanced by applyOps
    ts.compatMode     = mode;
    ts.topicName      = latest->topicName;                 // carried forward unchanged
    ts.persistence    = latest->persistence;               // carried forward unchanged (Phase 2b E)

    const std::vector<uint8_t> payload = ts.serialize();
    // Task 15b: composite (topicId, version) compaction key — see the matching
    // comment at the DefineTopic append site in onCreateTopicReply above.
    uint8_t keyBuf[6];
    const uint32_t u = static_cast<uint32_t>(topicId);
    keyBuf[0] = static_cast<uint8_t>(u & 0xff);
    keyBuf[1] = static_cast<uint8_t>((u >> 8) & 0xff);
    keyBuf[2] = static_cast<uint8_t>((u >> 16) & 0xff);
    keyBuf[3] = static_cast<uint8_t>((u >> 24) & 0xff);
    const uint16_t v = ts.version;
    keyBuf[4] = static_cast<uint8_t>(v & 0xff);
    keyBuf[5] = static_cast<uint8_t>((v >> 8) & 0xff);

    const rune::replication::Offset base =
        replica_->appendKeyed(std::span<const uint8_t>(keyBuf, 6),
                              std::span<const uint8_t>(payload.data(), payload.size()),
                              /*acksAll=*/true);
    if (base < 0) {
        // Gated (minIsr not met) → Internal; nothing committed, version unchanged.
        deps_.respondAlter(msg.session, msg.corr, topicId, curVersion, kInternal);
        return;
    }

    // ts.topicId == topicId and ts.version == newVersion (computed ONCE above).
    AlterAwaitingCommit op{
        .alter        = std::move(msg),
        .startedNs    = nowNs,
        .alterSchema  = std::move(ts),
        .appendedLast = base,   // single-record keyed batch → last == base
    };
    replica_->onLeaderLogAppended();  // reconcile LEO/HW + push to followers
    inFlight_ = std::move(op);
}

// ─── answerInFlight_ — end the in-flight op without committing ──────────────
void AdminWorker::answerInFlight_(uint16_t code, int32_t topicId) {
    std::visit([&](const auto& st) {
        using S = std::decay_t<decltype(st)>;
        if constexpr (std::is_same_v<S, AlterAwaitingCommit>) {
            deps_.respondAlter(st.alter.session, st.alter.corr, topicId, 0, code);
        } else if constexpr (std::is_same_v<S, ViewAwaitingId> ||
                             std::is_same_v<S, ViewAwaitingCommit>) {
            deps_.respondCreateView(st.view.session, st.view.corr, topicId, 0, code);
        } else if constexpr (std::is_same_v<S, DefineAwaitingId> ||
                             std::is_same_v<S, DefineAwaitingCommit>) {
            deps_.respondDefine(st.msg.session, st.msg.corr, topicId, 0, code);
        }
        // Idle: nothing in flight, nothing to answer.
    }, inFlight_);
    inFlight_ = Idle{};
}

int32_t AdminWorker::inFlightAlterTopicId_() const noexcept {
    const auto* alter = std::get_if<AlterAwaitingCommit>(&inFlight_);
    return alter ? alter->alterSchema.topicId : -1;
}

std::string_view AdminWorker::inFlightStateName() const noexcept {
    return std::visit([](const auto& st) -> std::string_view {
        using S = std::decay_t<decltype(st)>;
        if constexpr (std::is_same_v<S, Idle>)                      return "Idle";
        else if constexpr (std::is_same_v<S, DefineAwaitingId>)     return "DefineAwaitingId";
        else if constexpr (std::is_same_v<S, DefineAwaitingCommit>) return "DefineAwaitingCommit";
        else if constexpr (std::is_same_v<S, AlterAwaitingCommit>)  return "AlterAwaitingCommit";
        else if constexpr (std::is_same_v<S, ViewAwaitingId>)       return "ViewAwaitingId";
        else                                                         return "ViewAwaitingCommit";
    }, inFlight_);
}

// ─── answerMissingCommitReplica_ — AwaitingCommit with no replica to gate on ──
// B4 fix round 1: never a silent drop (the client would hang).  Unreachable in
// production — an op reaches AwaitingCommit only after appending to that very
// replica — so it is loud: logged, counted, answered Internal (outcome unknown).
void AdminWorker::answerMissingCommitReplica_(const char* catalog) {
    ++commitReplicaMissing_;
    // The client's correlation id identifies the op (M3: no ticket kept past the reply).
    const int64_t corr = std::visit([](const auto& st) -> int64_t {
        using S = std::decay_t<decltype(st)>;
        if constexpr (std::is_same_v<S, DefineAwaitingCommit>) return st.msg.corr;
        else if constexpr (std::is_same_v<S, ViewAwaitingCommit>) return st.view.corr;
        else if constexpr (std::is_same_v<S, AlterAwaitingCommit>) return st.alter.corr;
        else return 0;
    }, inFlight_);
    spdlog::error("AdminWorker: in-flight DDL op (corr={}) in AwaitingCommit has no {} "
                  "replica / catalog to commit on; answering Internal (count={})",
                  corr, catalog, commitReplicaMissing_);
    answerInFlight_(kInternal, inFlightAlterTopicId_());
}

// ─── advance_ — AWAITING_COMMIT (HW gate) + timeout ──────────────────────────
bool AdminWorker::advance_(int64_t nowNs) {
    if (!hasInFlight()) return false;

    // Timeout guard (error path only — not a convergence gate).
    const int64_t startedNs = std::visit([](const auto& st) -> int64_t {
        if constexpr (requires { st.startedNs; }) return st.startedNs;
        else return 0;
    }, inFlight_);
    if (nowNs - startedNs > kDdlTimeoutNs) {
        answerInFlight_(kInternal, inFlightAlterTopicId_());
        return true;
    }

    // Task 19: CREATE VIEW's durable record lives on the __views replica, so its
    // HW gate (and commit) use viewsReplica_, not the schema replica_.
    if (const auto* view = std::get_if<ViewAwaitingCommit>(&inFlight_)) {
        if (!viewsReplica_ || !viewRegistry_) [[unlikely]] {
            answerMissingCommitReplica_("__views");
            return true;
        }
        if (viewsReplica_->highWatermark() <= view->appendedLast) return false;
        commitCreateView_(*view);
        inFlight_ = Idle{};
        return true;
    }

    // ── ALTER on-commit ── apply the (pre-built, version computed ONCE in
    //    startAlter_) record to the registry and respond OK.
    if (const auto* alter = std::get_if<AlterAwaitingCommit>(&inFlight_)) {
        if (!replica_) [[unlikely]] {
            answerMissingCommitReplica_("__topic_schemas");
            return true;
        }
        // D: true majority-durable — HW must cover the appended offset.
        if (replica_->highWatermark() <= alter->appendedLast) return false;

        const int32_t  topicId    = alter->alterSchema.topicId;
        const uint16_t newVersion = alter->alterSchema.version;
        reg_.applyRecord(alter->alterSchema);
        // Task 12: fan out the evolved schema to the produce/consume workers
        // (validator refresh for the new version) now that the worker side
        // keys validators_ by (topicId, version) — each writer version gets
        // its own Validator, so this never clobbers the still-live older
        // version's validator.  byIdVersion (not byId/latest) pins exactly
        // the version this ALTER just committed, independent of any later
        // concurrent commit (single-threaded admin worker, but explicit).
        if (alter->alterSchema.validate) {
            if (const RegistryEntry* entry = reg_.byIdVersion(topicId, newVersion))
                deps_.pushValidators(*entry, topicId);
        }
        deps_.respondAlter(alter->alter.session, alter->alter.corr, topicId, newVersion, kOK);
        inFlight_ = Idle{};
        return true;
    }

    // ── CREATE on-commit ── apply to registry, push validators, respond OK.
    if (const auto* def = std::get_if<DefineAwaitingCommit>(&inFlight_)) {
        if (!replica_) [[unlikely]] {
            answerMissingCommitReplica_("__topic_schemas");
            return true;
        }
        // D: true majority-durable — HW must cover the appended offset.
        if (replica_->highWatermark() <= def->appendedLast) return false;

        TopicSchema ts;
        ts.topicId        = def->topicId;
        ts.version        = nextSchemaVersion_(def->topicId);
        ts.partitionCount = def->partitionCount;   // authoritative (I1)
        ts.keyField       = def->msg.keyField;
        ts.validate       = def->msg.validate;
        ts.descriptorBlob = def->msg.blob;
        ts.nextTag        = def->fieldCount;
        ts.topicName      = def->msg.name;      // same as the durable record
        ts.persistence    = def->persistence;   // same as the durable record
        reg_.applyRecord(ts);
        // B10 / D4 fix round 1: the record is committed -> mark the topic dir.
        if (deps_.topicCommitted) deps_.topicCommitted(def->topicId);
        if (def->msg.validate) {
            if (const RegistryEntry* entry = reg_.byId(def->topicId))
                deps_.pushValidators(*entry, def->topicId);
        }
        deps_.respondDefine(def->msg.session, def->msg.corr, def->topicId, ts.version, kOK);
        inFlight_ = Idle{};
        return true;
    }
    return false;   // an AwaitingId state: waiting for the create-topic reply
}

// ─── commitCreateView_ — CREATE VIEW on-commit (HW gate passed) ──────────────
void AdminWorker::commitCreateView_(const ViewAwaitingCommit& op) {
    const int32_t viewTopicId = op.viewRecord.viewTopicId;

    // 1. Apply the durable ViewRecord to the in-memory catalog.
    viewRegistry_->applyRecord(op.viewRecord);
    // B10 / D4 fix round 1: the view record is committed -> mark the sink's dir.
    if (deps_.topicCommitted) deps_.topicCommitted(viewTopicId);

    // 2. Register the view's OUTPUT descriptor in the SchemaRegistry so the view
    //    is a typed topic (GetTopicSchema / openView handshake).  In-memory only
    //    here (create-and-run); restart recovery (Task 19b) re-registers it from
    //    the ViewRecord's compiled plan.  op.partitionCount was resolved on the
    //    reply (authoritative, or the source's).
    TopicSchema ts;
    ts.topicId        = viewTopicId;
    ts.version        = kInitialSchemaVersion;
    ts.partitionCount = op.partitionCount;
    ts.keyField       = op.view.keyField;
    ts.validate       = false;
    ts.descriptorBlob = rune::codec::serialize(op.viewOutDesc);
    ts.nextTag        = static_cast<uint16_t>(op.viewOutDesc.fields.size());
    // Index the view's typed output topic by the VIEW name (GetTopicSchema /
    // openView pass it), never by the output descriptor's name.
    ts.topicName      = op.view.viewName;
    reg_.applyRecord(ts);

    // 3. Respond OK with the view's topic id + output fingerprint.
    deps_.respondCreateView(op.view.session, op.view.corr,
                            viewTopicId, op.outputFingerprint, kOK);

    // 4. Fan the compiled plan out to the workers (installView on each worker's
    //    OWN thread, via the control thread — never cross-thread here).  The
    //    durable record's blob IS the serialized plan (M3: one copy).
    if (deps_.pushViewPlan) {
        ViewPlanUpdateMsg vp;
        vp.viewTopicId = viewTopicId;
        vp.persistence = op.view.persistence;
        vp.planBlob    = op.viewRecord.compiledPlanBlob;
        vp.outDescBlob = rune::codec::serialize(op.viewOutDesc);
        vp.srcDescBlob = rune::codec::serialize(op.viewSrcDesc);
        deps_.pushViewPlan(vp);
    }
}

// ─── rebuildFromLog_ — C: replay [0, HW) into the registry on promotion ──────
void AdminWorker::rebuildFromLog_() {
    rebuildFailed_ = false;  // fresh attempt on every Follower→Leader edge
    if (!replica_) return;
    const rune::replication::Offset hw = replica_->highWatermark();
    if (hw <= 0) return;

    static constexpr int32_t kChunk = 64 * 1024 * 1024;
    // C1 x KL2: every topic this replay applies is a committed catalog entry and
    // must serve — an in-doubt record (its op answered Internal, never applied
    // live) becomes servable here, not only at the next restart.  Cold path.
    std::vector<int32_t> replayed;
    rune::replication::Offset fetchOffset = 0;
    while (fetchOffset < hw) {
        std::vector<uint8_t> data = replica_->read(fetchOffset, kChunk);
        if (data.empty()) break;

        std::size_t pos = 0;
        rune::replication::Offset lastBatchEnd = fetchOffset;
        while (pos < data.size()) {
            const uint8_t*    bs  = data.data() + pos;
            const std::size_t rem = data.size() - pos;
            if (rem < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead))
                break;
            rune::protocol::RecordBatchReader reader(bs, rem);
            if (!reader.ensureValid()) break;
            const int32_t tl = reader.totalLength();
            if (tl <= 0 || static_cast<std::size_t>(tl) > rem) break;
            const rune::replication::Offset batchBase = reader.baseOffset();
            reader.forEachRecord([this, batchBase, &replayed](std::string_view /*key*/, std::string_view val,
                                  int64_t /*offset*/, int64_t /*ts*/) {
                auto ts = TopicSchema::deserialize(std::span<const uint8_t>(
                    reinterpret_cast<const uint8_t*>(val.data()), val.size()));
                if (ts) {
                    reg_.applyRecord(*ts);
                    if (ts->topicId >= 0 &&
                        std::find(replayed.begin(), replayed.end(), ts->topicId) == replayed.end())
                        replayed.push_back(ts->topicId);
                    return;
                }
                // FAIL LOUD (never a silent skip): SchemaRegistry stores
                // versions in a per-topic deque indexed by (version-1); a
                // silently-dropped record here would leave an UNDETECTED gap
                // and serve wrong/missing schema versions after recovery.
                rebuildFailed_ = true;
                spdlog::critical(
                    "AdminWorker: CORRUPT __topic_schemas record at batch "
                    "baseOffset={} — TopicSchema::deserialize failed "
                    "(truncated/corrupt payload). ABORTING rebuildFromLog_ "
                    "rather than silently skipping it (would leave an "
                    "undetected schema-version gap).",
                    batchBase);
            });
            if (rebuildFailed_) return;  // abort: no further batches applied
            const rune::replication::Offset bne =
                reader.baseOffset() + reader.lastOffsetDelta() + 1;
            if (bne > lastBatchEnd) lastBatchEnd = bne;
            pos += static_cast<std::size_t>(tl);
        }
        if (lastBatchEnd <= fetchOffset) break;
        fetchOffset = lastBatchEnd;
    }
    // kA2cTopicCommitted for each replayed topic (a no-op on the control thread for
    // one that already serves — boot-built topics, earlier commits).
    if (deps_.topicCommitted)
        for (const int32_t id : replayed) deps_.topicCommitted(id);
    spdlog::info("AdminWorker: rebuilt SchemaRegistry from __topic_schemas log (hw={})", hw);
}

// ─── recoverViewsOnPromotion_ — rebuild ViewRegistry + fan out recovery ───────
// The view counterpart of rebuildFromLog_, on the SAME Follower->Leader edge.
void AdminWorker::recoverViewsOnPromotion_() {
    viewsRebuildFailed_ = false;   // fresh attempt on every Follower->Leader edge
    if (!viewsReplica_ || !viewRegistry_) return;

    // 1. Rebuild the ViewRegistry from the committed __views log [0, HW).
    //    HW is now > 0 (the views replica was promoted on this same edge, just
    //    above in onRole), unlike at construction where a fresh Follower is 0.
    const rune::replication::Offset hw = viewsReplica_->highWatermark();
    if (hw > 0) {
        static constexpr int32_t kChunk = 64 * 1024 * 1024;
        rune::replication::Offset fetchOffset = 0;
        bool failed = false;
        while (fetchOffset < hw && !failed) {
            std::vector<uint8_t> data = viewsReplica_->read(fetchOffset, kChunk);
            if (data.empty()) break;
            std::size_t pos = 0;
            rune::replication::Offset lastBatchEnd = fetchOffset;
            while (pos < data.size()) {
                const uint8_t*    bs  = data.data() + pos;
                const std::size_t rem = data.size() - pos;
                if (rem < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead))
                    break;
                rune::protocol::RecordBatchReader reader(bs, rem);
                if (!reader.ensureValid()) break;
                const int32_t tl = reader.totalLength();
                if (tl <= 0 || static_cast<std::size_t>(tl) > rem) break;
                const rune::replication::Offset batchBase = reader.baseOffset();
                reader.forEachRecord([this, batchBase, &failed](std::string_view /*key*/,
                                      std::string_view val, int64_t /*off*/, int64_t /*ts*/) {
                    auto vr = ViewRecord::deserialize(std::span<const uint8_t>(
                        reinterpret_cast<const uint8_t*>(val.data()), val.size()));
                    if (vr) { viewRegistry_->applyRecord(*vr); return; }
                    // FAIL LOUD (mirrors rebuildFromLog_): a corrupt __views record
                    // would otherwise leave an undetected catalog gap.
                    failed = true;
                    spdlog::critical(
                        "AdminWorker: CORRUPT __views record at batch baseOffset={} — "
                        "ViewRecord::deserialize failed; ABORTING __views rebuild "
                        "rather than silently skipping (would leave a view-catalog gap).",
                        batchBase);
                });
                if (failed) break;
                const rune::replication::Offset bne =
                    reader.baseOffset() + reader.lastOffsetDelta() + 1;
                if (bne > lastBatchEnd) lastBatchEnd = bne;
                pos += static_cast<std::size_t>(tl);
            }
            if (lastBatchEnd <= fetchOffset) break;
            fetchOffset = lastBatchEnd;
        }
        if (failed) {   // surfaced loudly above; do not fan out a partial catalog
            viewsRebuildFailed_ = true;   // M2: no DDL is served on a partial view catalog
            return;
        }
    }

    // 2. For each recovered durable view: re-register its OUTPUT schema in reg_
    //    (so GetTopicSchema / openView keep working after restart) and fan out a
    //    RECOVERY install (recovery=true) via the SAME path CreateView uses.
    int recovered = 0;
    for (const ViewRecord* rec : viewRegistry_->all()) {
        if (rec == nullptr) continue;

        auto plan = rune::view::deserializeCompiledViewPlan(
            std::span<const uint8_t>(rec->compiledPlanBlob.data(), rec->compiledPlanBlob.size()));
        if (!plan || plan->sourceTopicId.empty()) {
            spdlog::error("AdminWorker: view '{}' (id={}) skipped on recovery — compiled "
                          "plan missing/un-deserializable", rec->viewName, rec->viewTopicId);
            continue;
        }
        const int32_t sourceTopicId = plan->sourceTopicId[0];
        const RegistryEntry* srcEntry = reg_.byId(sourceTopicId);
        if (srcEntry == nullptr) {
            spdlog::error("AdminWorker: view '{}' (id={}) skipped on recovery — source topic "
                          "{} not found in SchemaRegistry", rec->viewName, rec->viewTopicId,
                          sourceTopicId);
            continue;
        }

        // Re-register the view's typed OUTPUT topic from the durable blob (exact
        // bytes -> exact fingerprint), mirroring commitCreateView_.
        if (!rec->outputDescriptorBlob.empty() && reg_.byId(rec->viewTopicId) == nullptr) {
            try {
                const rune::codec::Descriptor outDesc = rune::codec::deserialize(
                    std::span<const uint8_t>(rec->outputDescriptorBlob.data(),
                                             rec->outputDescriptorBlob.size()));
                TopicSchema ts;
                ts.topicId        = rec->viewTopicId;
                ts.version        = kInitialSchemaVersion;
                ts.partitionCount = srcEntry->partitionCount;
                ts.validate       = false;
                ts.descriptorBlob = rec->outputDescriptorBlob;
                ts.nextTag        = static_cast<uint16_t>(outDesc.fields.size());
                ts.topicName      = rec->viewName;   // resolve by the view name (as at create)
                for (const auto& f : outDesc.fields) {
                    if (f.isKey) { ts.keyField = f.name; break; }
                }
                reg_.applyRecord(ts);
            } catch (const std::exception& ex) {
                spdlog::error("AdminWorker: view '{}' (id={}) output descriptor failed to "
                              "re-register on recovery: {}", rec->viewName, rec->viewTopicId,
                              ex.what());
                // Continue: the worker still serves rows (fingerprint + layout come
                // from the plan); only GetTopicSchema is affected.
            }
        }

        // C1 x KL2: the view's record is committed — its sink serves (a no-op on the
        // control thread unless it was in doubt).  Posted before the plan: the same
        // FIFO ring.
        if (deps_.topicCommitted) deps_.topicCommitted(rec->viewTopicId);

        // Fan out a RECOVERY install — installViewForRecovery on each worker's own
        // thread, which loads the checkpoint + rebuilds to the committed tail.
        if (deps_.pushViewPlan) {
            ViewPlanUpdateMsg vp;
            vp.viewTopicId = rec->viewTopicId;
            vp.persistence = rec->persistence;
            vp.recovery    = true;
            vp.planBlob    = rec->compiledPlanBlob;
            vp.outDescBlob = rec->outputDescriptorBlob;
            vp.srcDescBlob = rune::codec::serialize(srcEntry->descriptor);
            deps_.pushViewPlan(vp);
        }
        ++recovered;
    }
    if (recovered > 0)
        spdlog::info("AdminWorker: recovered {} durable view(s) on promotion "
                     "(ViewRegistry rebuild + recovery fanout)", recovered);
}

} // namespace rune::node
