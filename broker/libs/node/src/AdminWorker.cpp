#include "rune/node/AdminWorker.h"
#include <rune/codec/Serialize.h>  // rune::codec::deserialize/serialize (throws on bad input)
#include <rune/codec/Evolve.h>     // checkCompatibility / applyOps (Task 11)
#include <rune/codec/AlterOps.h>   // decodeAlterOps
#include <rune/replication/PartitionReplica.h>
#include <rune/protocol/RecordBatch.h>
#include <spdlog/spdlog.h>

namespace rune::node {

// Error code constants (protocol-agnostic integers fixed by Task 1 ErrorCode enum):
static constexpr uint16_t kOK             =  0;
static constexpr uint16_t kInternal       =  6;
static constexpr uint16_t kUnknownTopic   =  7;
static constexpr uint16_t kInvalidSchema  = 23;
static constexpr uint16_t kTopicExists    = 25;
static constexpr uint16_t kSchemaIncompatible = 29;  // ALTER compat gate reject
static constexpr uint16_t kSchemaConflict     = 30;  // ALTER stale expectedVersion (CAS)

AdminWorker::AdminWorker(SchemaRegistry& reg,
                         rune::replication::PartitionReplica* replica,
                         int32_t selfId,
                         std::vector<int32_t> replicaSet,
                         AdminDeps deps)
    : reg_(reg)
    , replica_(replica)
    , selfId_(selfId)
    , replicaSet_(std::move(replicaSet))
    , deps_(std::move(deps)) {}

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
        leader_ = true;
        if (!wasLeader_) {
            // Follower→Leader edge: rebuild registry from the replicated log.
            rebuildFromLog_();
            wasLeader_ = true;
        }
    } else {
        if (replica_) {
            replica_->becomeFollower(
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
        deps_.respondGet(msg.session, msg.corr, -1, 0, {}, kUnknownTopic);
        return;
    }
    const RegistryEntry* e = (msg.requestedFingerprint == 0)
                                 ? named
                                 : reg_.byFingerprint(named->topicId, msg.requestedFingerprint);
    if (!e) {
        // No version of this (known) topic has the requested fingerprint —
        // same wire error as an unknown topic (no dedicated "stale fingerprint"
        // code on the wire yet).
        deps_.respondGet(msg.session, msg.corr, -1, 0, {}, kUnknownTopic);
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
    deps_.respondGet(msg.session, msg.corr,
                     e->topicId, e->version,
                     rune::codec::serialize(e->descriptor), kOK);
}

// ─── control → admin reply ──────────────────────────────────────────────────
void AdminWorker::onCreateTopicReply(const CreateTopicReplyMsg& reply) {
    if (state_ != DdlState::AwaitingId || !inFlight_ ||
        inFlight_->ticket != reply.ticket) {
        // Stale/mismatched reply — ignore (the op may have timed out).
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

// ─── doWork ─────────────────────────────────────────────────────────────────
int AdminWorker::doWork(int64_t nowNs) {
    int work = 0;

    // 1. Drive the replica (RULING-2 A: admin thread SOLELY drives doWork).
    if (replica_) work += replica_->doWork(nowNs);

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

    if (!deps_.requestCreateTopic(op.ticket, op.msg.partitionCount, op.msg.name)) {
        // Ring full (should not happen for rare DDL) → Internal; requeue would risk
        // reordering, so fail fast and let the client retry.
        deps_.respondDefine(op.msg.session, op.msg.corr, -1, 0, kInternal);
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
        else
            deps_.respondDefine(inFlight_->msg.session, inFlight_->msg.corr,
                                -1, 0, kInternal);
        inFlight_.reset();
        state_ = DdlState::Idle;
        return true;
    }

    if (state_ == DdlState::AwaitingCommit) {
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

// ─── rebuildFromLog_ — C: replay [0, HW) into the registry on promotion ──────
void AdminWorker::rebuildFromLog_() {
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
            reader.forEachRecord([this](std::string_view /*key*/, std::string_view val,
                                  int64_t /*offset*/, int64_t /*ts*/) {
                auto ts = TopicSchema::deserialize(std::span<const uint8_t>(
                    reinterpret_cast<const uint8_t*>(val.data()), val.size()));
                if (ts) reg_.applyRecord(*ts);
            });
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

} // namespace rune::node
