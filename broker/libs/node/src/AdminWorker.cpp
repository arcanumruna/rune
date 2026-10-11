#include "rune/node/AdminWorker.h"
#include <rune/codec/Serialize.h>  // rune::codec::deserialize/serialize (throws on bad input)
#include <rune/codec/Evolve.h>     // checkCompatibility / applyOps (Task 11)
#include <rune/codec/AlterOps.h>   // decodeAlterOps
#include <rune/view/AggState.h>     // rune::view::isAggregateView (Phase 2a)
#include <rune/view/PlanCompiler.h> // rune::view::compileView (Task 19)
#include <rune/replication/PartitionReplica.h>
#include <rune/protocol/RecordBatch.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>

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

AdminWorker::AdminWorker(SchemaRegistry& reg,
                         rune::replication::PartitionReplica* replica,
                         int32_t selfId,
                         std::vector<int32_t> replicaSet,
                         AdminDeps deps,
                         rune::replication::PartitionReplica* viewsReplica,
                         ViewRegistry* viewRegistry)
    : reg_(reg)
    , replica_(replica)
    , selfId_(selfId)
    , replicaSet_(std::move(replicaSet))
    , deps_(std::move(deps))
    , viewsReplica_(viewsReplica)
    , viewRegistry_(viewRegistry) {}

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
    }
}

// ─── Ingress ────────────────────────────────────────────────────────────────
void AdminWorker::submitDefineTopic(DefineTopicMsg msg) {
    PendingDdl pd;
    pd.kind   = DdlKind::Create;
    pd.define = std::move(msg);
    pending_.push_back(std::move(pd));
}

void AdminWorker::submitCreateView(CreateViewMsg msg) {
    PendingDdl pd;
    pd.kind = DdlKind::CreateView;
    pd.view = std::move(msg);
    pending_.push_back(std::move(pd));
}

void AdminWorker::submitAlterTopic(AlterTopicMsg msg) {
    PendingDdl pd;
    pd.kind  = DdlKind::Alter;
    pd.alter = std::move(msg);
    pending_.push_back(std::move(pd));
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
    if (state_ != DdlState::AwaitingId || !inFlight_ ||
        inFlight_->ticket != reply.ticket) {
        // Stale/mismatched reply — ignore (the op may have timed out).
        return;
    }
    // ── CREATE VIEW branch (Task 19): the reply carries the view's SINK topicId,
    //    which is ALSO the view's own topic id.  Append the durable ViewRecord to
    //    __views and gate on viewsReplica_'s HW (not the schema replica's).
    if (inFlight_->kind == DdlKind::CreateView) {
        onCreateViewReply_(reply);
        return;
    }

    if (reply.topicId < 0) {
        // Control thread failed to create the topic → Internal error.
        deps_.respondDefine(inFlight_->msg.session, inFlight_->msg.corr,
                            -1, 0, kInternal);
        inFlight_.reset();
        state_ = DdlState::Idle;
        return;
    }
    inFlight_->topicId = reply.topicId;

    // ── AWAITING_COMMIT ── append the durable TopicSchema record.
    TopicSchema ts;
    ts.topicId        = reply.topicId;
    ts.version        = nextSchemaVersion_(reply.topicId);
    ts.partitionCount = inFlight_->msg.partitionCount;
    ts.keyField       = inFlight_->msg.keyField;
    ts.validate       = inFlight_->msg.validate;
    ts.descriptorBlob = inFlight_->msg.blob;
    // DefineTopic (create): nextTag starts past every field the fresh schema
    // already declares, so the first ALTER ADD (Task 11) allocates a tag that
    // doesn't collide with an existing one.
    ts.nextTag        = inFlight_->fieldCount;
    // Durable topic name (independent of the descriptor's name): boot recovery
    // recreates the topic under it (RuntimeTopicRecovery).
    ts.topicName      = inFlight_->msg.name;

    // D: leader guard.  If we are not the leader, DDL cannot be committed here.
    if (!leader_ || !replica_) {
        deps_.respondDefine(inFlight_->msg.session, inFlight_->msg.corr,
                            -1, 0, kInternal);
        inFlight_.reset();
        state_ = DdlState::Idle;
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
        deps_.respondDefine(inFlight_->msg.session, inFlight_->msg.corr,
                            -1, 0, kInternal);
        inFlight_.reset();
        state_ = DdlState::Idle;
        return;
    }
    // D: acksAll → wait until HW covers the appended offset (majority-durable).
    inFlight_->appendedBase = base;
    inFlight_->appendedLast = base;  // single-record keyed batch → last == base
    replica_->onLeaderLogAppended();  // reconcile LEO/HW + push to followers
    state_ = DdlState::AwaitingCommit;
}

// ─── onCreateViewReply_ — CREATE VIEW sink id came back; append the ViewRecord ─
void AdminWorker::onCreateViewReply_(const CreateTopicReplyMsg& reply) {
    if (reply.topicId < 0) {
        deps_.respondCreateView(inFlight_->view.session, inFlight_->view.corr,
                                -1, 0, kInternal);
        inFlight_.reset();
        state_ = DdlState::Idle;
        return;
    }
    // D: leader guard + views catalog wired.
    if (!leader_ || !viewsReplica_ || !viewRegistry_) {
        deps_.respondCreateView(inFlight_->view.session, inFlight_->view.corr,
                                -1, 0, kInternal);
        inFlight_.reset();
        state_ = DdlState::Idle;
        return;
    }

    inFlight_->topicId = reply.topicId;
    // Stamp the authoritative sink id onto the compiled plan (it is the view's
    // own topic id) BEFORE serializing it into the durable ViewRecord.
    inFlight_->compiledPlan.viewTopicId = reply.topicId;

    ViewRecord rec;
    rec.viewTopicId     = reply.topicId;
    rec.viewName        = inFlight_->view.viewName;
    rec.compiledPlanBlob = rune::view::serialize(inFlight_->compiledPlan);
    rec.persistence     = inFlight_->view.persistence;
    // Task 19b: persist the OUTPUT descriptor so a cold restart can re-register
    // the view's typed output topic (GetTopicSchema / openView) EXACTLY as
    // created — its field names + FixedStr widths (hence its fingerprint) are
    // NOT reconstructable from the compiled plan alone.
    rec.outputDescriptorBlob = rune::codec::serialize(inFlight_->viewOutDesc);
    inFlight_->viewRecord = rec;  // applied on the HW gate (advance_)

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
        deps_.respondCreateView(inFlight_->view.session, inFlight_->view.corr,
                                -1, 0, kInternal);
        inFlight_.reset();
        state_ = DdlState::Idle;
        return;
    }
    inFlight_->appendedBase = base;
    inFlight_->appendedLast = base;
    viewsReplica_->onLeaderLogAppended();
    state_ = DdlState::AwaitingCommit;
}

// ─── doWork ─────────────────────────────────────────────────────────────────
int AdminWorker::doWork(int64_t nowNs) {
    int work = 0;

    // 1. Drive the replica(s) (RULING-2 A: admin thread SOLELY drives doWork).
    if (replica_)      work += replica_->doWork(nowNs);
    if (viewsReplica_) work += viewsReplica_->doWork(nowNs);   // Task 19: __views

    // 2. Advance the in-flight DDL (HW check / commit).
    if (advance_(nowNs)) ++work;

    // 3. Start the next queued DefineTopic if idle.
    if (state_ == DdlState::Idle && !pending_.empty()) {
        startNext_(nowNs);
        ++work;
    }
    return work;
}

// ─── startNext_ — PARSING → name-lookup → CREATING (request id) ──────────────
void AdminWorker::startNext_(int64_t nowNs) {
    PendingDdl pd = std::move(pending_.front());
    pending_.pop_front();

    if (pd.kind == DdlKind::Alter) {
        startAlter_(std::move(pd.alter), nowNs);
        return;
    }
    if (pd.kind == DdlKind::CreateView) {
        startCreateView_(std::move(pd.view), nowNs);
        return;
    }

    DefineTopicMsg msg = std::move(pd.define);

    // ── PARSING ── validate the descriptor blob is well-formed, and capture
    // its field count in the same pass (seeds nextTag for the create path —
    // see DdlOp::fieldCount — avoiding a second deserialize of the same blob
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
    DdlOp op;
    op.ticket     = nextTicket_++;
    op.startedNs  = nowNs;
    op.fieldCount = static_cast<uint16_t>(fieldCount);
    op.msg        = std::move(msg);

    if (!deps_.requestCreateTopic(op.ticket, op.msg.partitionCount, op.msg.name,
                                  /*keyField=*/"", /*cleanupPolicy=*/0)) {
        // Ring full (should not happen for rare DDL) → Internal; requeue would risk
        // reordering, so fail fast and let the client retry.
        deps_.respondDefine(op.msg.session, op.msg.corr, -1, 0, kInternal);
        return;
    }
    inFlight_ = std::move(op);
    state_    = DdlState::AwaitingId;
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
        const RegistryEntry* e = reg_.byId(s.topicId);
        if (!e) {
            deps_.respondCreateView(msg.session, msg.corr, -1, 0, kViewSourceNotFound);
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
    DdlOp op;
    op.kind              = DdlKind::CreateView;
    op.ticket            = nextTicket_++;
    op.startedNs         = nowNs;
    op.view              = std::move(msg);
    op.compiledPlan      = std::move(compiled->plan);
    op.viewOutDesc       = std::move(compiled->outputDescriptor);
    op.viewSrcDesc       = *srcDescs[0];   // single-source (co-partition-trivial)
    op.outputFingerprint = op.compiledPlan.outputFingerprint;

    const RegistryEntry* srcEntry = reg_.byId(authored->sources[0].topicId);
    const int32_t sinkPartitions  = srcEntry ? srcEntry->partitionCount : 1;

    if (!deps_.requestCreateTopic(op.ticket, sinkPartitions, op.view.viewName,
                                  op.view.keyField, /*cleanupPolicy=*/1 /*Compact*/)) {
        deps_.respondCreateView(op.view.session, op.view.corr, -1, 0, kInternal);
        return;
    }
    inFlight_ = std::move(op);
    state_    = DdlState::AwaitingId;
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

    // g. build the new durable TopicSchema (same id/partitions/validate/keyField;
    //    NEW evolving descriptor + post-applyOps nextTag + effective compatMode).
    TopicSchema ts;
    ts.topicId        = topicId;
    ts.version        = newVersion;
    ts.partitionCount = latest->partitionCount;
    ts.keyField       = latest->keyField;
    ts.validate       = latest->validator.has_value();   // same as current
    ts.descriptorBlob = rune::codec::serialize(*res);     // the NEW evolving descriptor
    ts.nextTag        = nextTag;                           // advanced by applyOps
    ts.compatMode     = mode;
    ts.topicName      = latest->topicName;                 // carried forward unchanged

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

    DdlOp op;
    op.kind         = DdlKind::Alter;
    op.alter        = std::move(msg);
    op.topicId      = topicId;
    op.newVersion   = newVersion;
    op.alterSchema  = std::move(ts);
    op.appendedBase = base;
    op.appendedLast = base;   // single-record keyed batch → last == base
    op.startedNs    = nowNs;
    replica_->onLeaderLogAppended();  // reconcile LEO/HW + push to followers
    inFlight_ = std::move(op);
    state_    = DdlState::AwaitingCommit;
}

// ─── advance_ — AWAITING_COMMIT (HW gate) + timeout ──────────────────────────
bool AdminWorker::advance_(int64_t nowNs) {
    if (state_ == DdlState::Idle || !inFlight_) return false;

    // Timeout guard (error path only — not a convergence gate).
    if (nowNs - inFlight_->startedNs > kDdlTimeoutNs) {
        if (inFlight_->kind == DdlKind::Alter)
            deps_.respondAlter(inFlight_->alter.session, inFlight_->alter.corr,
                               inFlight_->topicId, 0, kInternal);
        else if (inFlight_->kind == DdlKind::CreateView)
            deps_.respondCreateView(inFlight_->view.session, inFlight_->view.corr,
                                    -1, 0, kInternal);
        else
            deps_.respondDefine(inFlight_->msg.session, inFlight_->msg.corr,
                                -1, 0, kInternal);
        inFlight_.reset();
        state_ = DdlState::Idle;
        return true;
    }

    if (state_ == DdlState::AwaitingCommit) {
        // Task 19: CREATE VIEW's durable record lives on the __views replica, so
        // its HW gate (and commit) use viewsReplica_, not the schema replica_.
        if (inFlight_->kind == DdlKind::CreateView) {
            if (!viewsReplica_ || !viewRegistry_) {
                inFlight_.reset(); state_ = DdlState::Idle; return true;
            }
            if (viewsReplica_->highWatermark() <= inFlight_->appendedLast) return false;
            commitCreateView_();
            inFlight_.reset();
            state_ = DdlState::Idle;
            return true;
        }

        if (!replica_) { inFlight_.reset(); state_ = DdlState::Idle; return true; }
        // D: true majority-durable — HW must cover the appended offset.
        if (replica_->highWatermark() <= inFlight_->appendedLast) return false;

        // ── ALTER on-commit ── apply the (pre-built, version computed ONCE in
        //    startAlter_) record to the registry and respond OK.
        if (inFlight_->kind == DdlKind::Alter) {
            reg_.applyRecord(inFlight_->alterSchema);
            // Task 12: fan out the evolved schema to the produce/consume workers
            // (validator refresh for the new version) now that the worker side
            // keys validators_ by (topicId, version) — each writer version gets
            // its own Validator, so this never clobbers the still-live older
            // version's validator.  byIdVersion (not byId/latest) pins exactly
            // the version this ALTER just committed, independent of any later
            // concurrent commit (single-threaded admin worker, but explicit).
            if (inFlight_->alterSchema.validate) {
                if (const RegistryEntry* entry =
                        reg_.byIdVersion(inFlight_->topicId, inFlight_->newVersion))
                    deps_.pushValidators(*entry, inFlight_->topicId);
            }
            deps_.respondAlter(inFlight_->alter.session, inFlight_->alter.corr,
                               inFlight_->topicId, inFlight_->newVersion, kOK);
            inFlight_.reset();
            state_ = DdlState::Idle;
            return true;
        }

        {
            // ── CREATE on-commit ── apply to registry, push validators, respond OK.
            TopicSchema ts;
            ts.topicId        = inFlight_->topicId;
            ts.version        = nextSchemaVersion_(inFlight_->topicId);
            ts.partitionCount = inFlight_->msg.partitionCount;
            ts.keyField       = inFlight_->msg.keyField;
            ts.validate       = inFlight_->msg.validate;
            ts.descriptorBlob = inFlight_->msg.blob;
            ts.nextTag        = inFlight_->fieldCount;
            ts.topicName      = inFlight_->msg.name;   // same as the durable record
            reg_.applyRecord(ts);
            if (inFlight_->msg.validate) {
                if (const RegistryEntry* entry = reg_.byId(inFlight_->topicId))
                    deps_.pushValidators(*entry, inFlight_->topicId);
            }
            deps_.respondDefine(inFlight_->msg.session, inFlight_->msg.corr,
                                inFlight_->topicId, ts.version, kOK);
            inFlight_.reset();
            state_ = DdlState::Idle;
            return true;
        }
    }
    return false;
}

// ─── commitCreateView_ — CREATE VIEW on-commit (HW gate passed) ──────────────
void AdminWorker::commitCreateView_() {
    const int32_t viewTopicId = inFlight_->topicId;

    // 1. Apply the durable ViewRecord to the in-memory catalog.
    viewRegistry_->applyRecord(inFlight_->viewRecord);

    // 2. Register the view's OUTPUT descriptor in the SchemaRegistry so the view
    //    is a typed topic (GetTopicSchema / openView handshake).  In-memory only
    //    here (create-and-run); restart recovery (Task 19b) re-registers it from
    //    the ViewRecord's compiled plan.
    int32_t sinkPartitions = 1;
    if (!inFlight_->compiledPlan.sourceTopicId.empty()) {
        if (const RegistryEntry* se = reg_.byId(inFlight_->compiledPlan.sourceTopicId[0]))
            sinkPartitions = se->partitionCount;
    }
    TopicSchema ts;
    ts.topicId        = viewTopicId;
    ts.version        = kInitialSchemaVersion;
    ts.partitionCount = sinkPartitions;
    ts.keyField       = inFlight_->view.keyField;
    ts.validate       = false;
    ts.descriptorBlob = rune::codec::serialize(inFlight_->viewOutDesc);
    ts.nextTag        = static_cast<uint16_t>(inFlight_->viewOutDesc.fields.size());
    reg_.applyRecord(ts);

    // 3. Respond OK with the view's topic id + output fingerprint.
    deps_.respondCreateView(inFlight_->view.session, inFlight_->view.corr,
                            viewTopicId, inFlight_->outputFingerprint, kOK);

    // 4. Fan the compiled plan out to the workers (installView on each worker's
    //    OWN thread, via the control thread — never cross-thread here).
    if (deps_.pushViewPlan) {
        ViewPlanUpdateMsg vp;
        vp.viewTopicId = viewTopicId;
        vp.persistence = inFlight_->view.persistence;
        vp.planBlob    = rune::view::serialize(inFlight_->compiledPlan);
        vp.outDescBlob = rune::codec::serialize(inFlight_->viewOutDesc);
        vp.srcDescBlob = rune::codec::serialize(inFlight_->viewSrcDesc);
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
            reader.forEachRecord([this, batchBase](std::string_view /*key*/, std::string_view val,
                                  int64_t /*offset*/, int64_t /*ts*/) {
                auto ts = TopicSchema::deserialize(std::span<const uint8_t>(
                    reinterpret_cast<const uint8_t*>(val.data()), val.size()));
                if (ts) {
                    reg_.applyRecord(*ts);
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
    spdlog::info("AdminWorker: rebuilt SchemaRegistry from __topic_schemas log (hw={})", hw);
}

// ─── recoverViewsOnPromotion_ — rebuild ViewRegistry + fan out recovery ───────
// The view counterpart of rebuildFromLog_, on the SAME Follower->Leader edge.
void AdminWorker::recoverViewsOnPromotion_() {
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
        if (failed) return;  // surfaced loudly above; do not fan out a partial catalog
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
