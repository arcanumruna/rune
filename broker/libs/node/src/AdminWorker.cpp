#include "rune/node/AdminWorker.h"
#include <rune/codec/Serialize.h>  // rune::codec::deserialize (throws on bad input)
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
    pending_.push_back(std::move(msg));
}

void AdminWorker::submitGetSchema(const GetSchemaMsg& msg) {
    // GetSchema is a pure read of the admin-owned registry — no round-trip.
    const auto* e = reg_.byName(msg.name);
    if (!e) {
        deps_.respondGet(msg.session, msg.corr, -1, 0, {}, kUnknownTopic);
        return;
    }
    deps_.respondGet(msg.session, msg.corr,
                     e->topicId, e->version, e->descriptorBlob, kOK);
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
    ts.version        = 1;
    ts.partitionCount = inFlight_->msg.partitionCount;
    ts.keyField       = inFlight_->msg.keyField;
    ts.validate       = inFlight_->msg.validate;
    ts.descriptorBlob = inFlight_->msg.blob;

    // D: leader guard.  If we are not the leader, DDL cannot be committed here.
    if (!leader_ || !replica_) {
        deps_.respondDefine(inFlight_->msg.session, inFlight_->msg.corr,
                            -1, 0, kInternal);
        inFlight_.reset();
        state_ = DdlState::Idle;
        return;
    }

    const std::vector<uint8_t> payload = ts.serialize();
    uint8_t keyBuf[4];
    const uint32_t u = static_cast<uint32_t>(reply.topicId);
    keyBuf[0] = static_cast<uint8_t>(u & 0xff);
    keyBuf[1] = static_cast<uint8_t>((u >> 8) & 0xff);
    keyBuf[2] = static_cast<uint8_t>((u >> 16) & 0xff);
    keyBuf[3] = static_cast<uint8_t>((u >> 24) & 0xff);

    const rune::replication::Offset base =
        replica_->appendKeyed(std::span<const uint8_t>(keyBuf, 4),
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
    DefineTopicMsg msg = std::move(pending_.front());
    pending_.pop_front();

    // ── PARSING ── validate the descriptor blob is well-formed.
    try {
        [[maybe_unused]] auto _ = rune::codec::deserialize(
            std::span<const uint8_t>(msg.blob.data(), msg.blob.size()));
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
    op.ticket    = nextTicket_++;
    op.startedNs = nowNs;
    op.msg       = std::move(msg);

    if (!deps_.requestCreateTopic(op.ticket, op.msg.partitionCount, op.msg.name)) {
        // Ring full (should not happen for rare DDL) → Internal; requeue would risk
        // reordering, so fail fast and let the client retry.
        deps_.respondDefine(op.msg.session, op.msg.corr, -1, 0, kInternal);
        return;
    }
    inFlight_ = std::move(op);
    state_    = DdlState::AwaitingId;
}

// ─── advance_ — AWAITING_COMMIT (HW gate) + timeout ──────────────────────────
bool AdminWorker::advance_(int64_t nowNs) {
    if (state_ == DdlState::Idle || !inFlight_) return false;

    // Timeout guard (error path only — not a convergence gate).
    if (nowNs - inFlight_->startedNs > kDdlTimeoutNs) {
        deps_.respondDefine(inFlight_->msg.session, inFlight_->msg.corr,
                            -1, 0, kInternal);
        inFlight_.reset();
        state_ = DdlState::Idle;
        return true;
    }

    if (state_ == DdlState::AwaitingCommit) {
        if (!replica_) { inFlight_.reset(); state_ = DdlState::Idle; return true; }
        // D: true majority-durable — HW must cover the appended offset.
        if (replica_->highWatermark() > inFlight_->appendedLast) {
            // ── on-commit ── apply to registry, push validators, respond OK.
            TopicSchema ts;
            ts.topicId        = inFlight_->topicId;
            ts.version        = 1;
            ts.partitionCount = inFlight_->msg.partitionCount;
            ts.keyField       = inFlight_->msg.keyField;
            ts.validate       = inFlight_->msg.validate;
            ts.descriptorBlob = inFlight_->msg.blob;
            reg_.applyRecord(ts);
            if (inFlight_->msg.validate) {
                if (const RegistryEntry* entry = reg_.byId(inFlight_->topicId))
                    deps_.pushValidators(*entry, inFlight_->topicId);
            }
            deps_.respondDefine(inFlight_->msg.session, inFlight_->msg.corr,
                                inFlight_->topicId, 1, kOK);
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
            reader.forEach([this](std::string_view /*key*/, std::string_view val,
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
