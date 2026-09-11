// ---------------------------------------------------------------------------
// PartitionReplica.cpp — Single-writer partition replica implementation.
//
// Design: see PartitionReplica.h
// C++23. No locks. No busy-spin. No per-call heap allocation on the hot path
// (record bytes are managed by the Log; the encode scratch buffer is reused).
//
// Record format: every batch going into the Log is a well-formed rune RecordBatch
// (61-byte header + variable records) so that Log::read()/truncateTo() work with
// the existing segment scanner.
// ---------------------------------------------------------------------------

#include <rune/replication/PartitionReplica.h>
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <stdexcept>

namespace rune::replication {

// ---------------------------------------------------------------------------
// Helper: build a minimal RecordBatch wrapping raw bytes as a single record.
// Uses the protocol builder so the batch header is valid (CRC stamped etc).
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makeBatch(int64_t baseOffset,
                                       std::span<const uint8_t> payload)
{
    rune::protocol::RecordBatchBuilder builder(baseOffset, /*baseTimestamp=*/0);
    // Wrap the raw payload as a single record (key=empty, value=payload).
    std::string_view keyView{};
    std::string_view valView(reinterpret_cast<const char*>(payload.data()),
                              payload.size());
    builder.append(keyView, valView, /*timestamp=*/0);
    return builder.build();
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

PartitionReplica::PartitionReplica(PartitionConfig          cfg,
                                   Role                     role,
                                   Epoch                    epoch,
                                   std::vector<NodeId>      replicas,
                                   std::vector<NodeId>      isr,
                                   rune::storage::Log*      log,
                                   PartitionTransport*      transport)
    : cfg_(cfg)
    , role_(role)
    , epoch_(epoch)
    , leaderId_(-1)
    , log_(log)
    , transport_(transport)
    , encodeScratch_(kScratchSize, uint8_t{0})
{
    assert(log_       != nullptr && "log must not be null");
    assert(transport_ != nullptr && "transport must not be null");

    if (role_ == Role::Leader) {
        isrManagerOwned_ = std::make_unique<IsrManager>(replicas, cfg_.selfId);
        isrManagerOwned_->setIsr(isr);
        isrManager_ = isrManagerOwned_.get();

        leaderState_.setLeaderId(cfg_.selfId);
        leaderState_.setIsr(isr);
        leaderState_.setLeaderLogEndOffset(log_->logEndOffset());

        if (epoch_ >= 0) {
            epochCache_.assign(static_cast<int32_t>(epoch_),
                               log_->logEndOffset());
        }
    }
}

// ---------------------------------------------------------------------------
// Role transitions
// ---------------------------------------------------------------------------

void PartitionReplica::becomeLeader(Epoch newEpoch, std::vector<NodeId> isr)
{
    epoch_ = newEpoch;
    role_  = Role::Leader;

    std::vector<NodeId> replicas = isr; // simplification: replicas == isr at election
    if (isrManagerOwned_) {
        // Preserve replica knowledge; just update ISR.
        isrManagerOwned_->setIsr(isr);
    } else {
        isrManagerOwned_ = std::make_unique<IsrManager>(replicas, cfg_.selfId);
        isrManagerOwned_->setIsr(isr);
    }
    isrManager_ = isrManagerOwned_.get();

    leaderState_.setLeaderId(cfg_.selfId);
    leaderState_.setIsr(isr);
    leaderState_.setLeaderLogEndOffset(log_->logEndOffset());

    epochCache_.assign(static_cast<int32_t>(epoch_), log_->logEndOffset());

    lastIsrCheckNs_ = 0;
}

void PartitionReplica::becomeFollower(Epoch leaderEpoch, NodeId leaderId)
{
    epoch_    = leaderEpoch;
    role_     = Role::Follower;
    leaderId_ = leaderId;
    followerHw_ = 0;
    lastFetchEmitNs_ = 0;
}

// ---------------------------------------------------------------------------
// Leader: append
// ---------------------------------------------------------------------------

Offset PartitionReplica::append(std::span<const uint8_t> records, bool acksAll)
{
    assert(role_ == Role::Leader && "append() called on follower");

    if (!isrManager_->canProduce(cfg_.minIsr, acksAll)) {
        return -1; // gated by min-ISR
    }

    Offset baseOffset = log_->logEndOffset();

    // Build a proper RecordBatch so the Log segment scanner works correctly.
    auto batch = makeBatch(baseOffset, records);
    // lastOffset = baseOffset + 0 (one record per batch = lastOffsetDelta=0).
    log_->append(batch, baseOffset);

    // Leader LEO is now baseOffset + 1.
    leaderState_.setLeaderLogEndOffset(log_->logEndOffset());
    // Leader self-advances HW (leader always in ISR, so majority includes leader).
    recomputeHw();

    return baseOffset;
}

// ---------------------------------------------------------------------------
// Leader: onFetchRequest
// ---------------------------------------------------------------------------

int16_t PartitionReplica::onFetchRequest(const FetchRequestMsg& req)
{
    assert(role_ == Role::Leader && "onFetchRequest() called on follower");

    // Epoch fence: reject stale leader epochs.
    if (isStaleEpoch(req.leaderEpoch, epoch_)) {
        FetchResponseMsg::encode(
            encodeScratch_.data(), 0,
            epoch_, cfg_.partitionId,
            leaderState_.highWatermark(),
            /*logStartOffset=*/0,
            kErrStaleEpoch,
            {});
        auto resp = FetchResponseMsg::decode(encodeScratch_.data(), 0);
        transport_->sendFetchResponse(req.followerId, resp);
        return kErrStaleEpoch;
    }

    // If the follower carries a higher epoch, fence ourselves (shouldn't happen
    // in normal operation, but be safe).
    if (mustStepDown(req.leaderEpoch, epoch_)) {
        FetchResponseMsg::encode(
            encodeScratch_.data(), 0,
            epoch_, cfg_.partitionId,
            leaderState_.highWatermark(),
            /*logStartOffset=*/0,
            kErrStaleEpoch,
            {});
        auto resp = FetchResponseMsg::decode(encodeScratch_.data(), 0);
        transport_->sendFetchResponse(req.followerId, resp);
        return kErrStaleEpoch;
    }

    // Record follower fetch: update ISR manager + leader state.
    // We pass lastIsrCheckNs_ as the "now" for the ISR manager time tracking.
    isrManager_->onFollowerFetch(req.followerId, req.fetchOffset, lastIsrCheckNs_);
    leaderState_.updateFollowerFetchOffset(req.followerId, req.fetchOffset);
    // HW is already recomputed inside updateFollowerFetchOffset().

    // Read records from the log.
    std::vector<uint8_t> logBytes;
    if (req.maxBytes > 0 && req.fetchOffset < log_->logEndOffset()) {
        logBytes = log_->read(req.fetchOffset, req.maxBytes);
    }

    FetchResponseMsg::encode(
        encodeScratch_.data(), 0,
        epoch_,
        cfg_.partitionId,
        leaderState_.highWatermark(),
        /*logStartOffset=*/0,
        kErrNone,
        std::span<const uint8_t>(logBytes.data(), logBytes.size()));

    auto resp = FetchResponseMsg::decode(encodeScratch_.data(), 0);
    transport_->sendFetchResponse(req.followerId, resp);

    return kErrNone;
}

// ---------------------------------------------------------------------------
// Leader: committed read (≤ highWatermark)
// ---------------------------------------------------------------------------

std::vector<uint8_t> PartitionReplica::read(Offset fetchOffset,
                                              int32_t maxBytes) const
{
    Offset hw = highWatermark();
    if (fetchOffset >= hw) {
        return {}; // nothing committed at this position
    }
    return log_->read(fetchOffset, maxBytes);
}

// ---------------------------------------------------------------------------
// Follower: applyReplicated
// ---------------------------------------------------------------------------

void PartitionReplica::applyReplicated(const FetchResponseMsg& resp)
{
    if (resp.errorCode != kErrNone) {
        // Error response — ignore (control plane handles epoch changes externally).
        return;
    }

    // Epoch change: truncate divergent tail before applying.
    if (resp.leaderEpoch > epoch_) {
        Offset truncOff = truncationOffsetForEpochChange(epochCache_,
                                                          resp.leaderEpoch);
        if (truncOff >= 0) {
            truncateLog(truncOff);
        }
        epoch_ = resp.leaderEpoch;
        epochCache_.assign(static_cast<int32_t>(epoch_), log_->logEndOffset());
    }

    // Append replicated records if any.
    if (!resp.records.empty()) {
        Offset baseOffset = log_->logEndOffset();
        auto batch = makeBatch(baseOffset, resp.records);
        log_->append(batch, baseOffset);
    }

    // Advance local HW = min(localLeo, leader's HW).
    Offset localLeo = log_->logEndOffset();
    Offset newHw    = std::min(localLeo, resp.highWatermark);
    if (newHw > followerHw_) {
        followerHw_ = newHw;
    }
}

// ---------------------------------------------------------------------------
// Follower: nextFetch
// ---------------------------------------------------------------------------

FetchRequestMsg PartitionReplica::nextFetch() const noexcept
{
    FetchRequestMsg req{};
    req.leaderEpoch = epoch_;
    req.partition   = cfg_.partitionId;
    req.followerId  = cfg_.selfId;
    req.fetchOffset = log_->logEndOffset();
    req.maxBytes    = cfg_.maxFetchBytes;
    return req;
}

// ---------------------------------------------------------------------------
// doWork
// ---------------------------------------------------------------------------

int PartitionReplica::doWork(int64_t nowNs)
{
    int work = 0;

    if (role_ == Role::Leader) {
        if (nowNs - lastIsrCheckNs_ >= cfg_.isrCheckIntervalNs) {
            lastIsrCheckNs_ = nowNs;
            ++work; // ISR check cycle itself counts as work

            isrManager_->maybeShrinkIsr(nowNs,
                                         log_->logEndOffset(),
                                         cfg_.replicaLagMaxMs,
                                         cfg_.replicaLagMaxOffset);
            if (isrManager_->isrChanged()) {
                syncIsrToLeaderState();
                recomputeHw();
            }

            isrManager_->maybeExpandIsr(nowNs, leaderState_.highWatermark());
            if (isrManager_->isrChanged()) {
                syncIsrToLeaderState();
            }
        }
    } else {
        // Follower: emit a fetch on cadence.
        if (nowNs - lastFetchEmitNs_ >= cfg_.fetchIntervalNs) {
            lastFetchEmitNs_ = nowNs;
            FetchRequestMsg req = nextFetch();
            transport_->sendFetchRequest(req);
            ++work;
        }
    }

    return work;
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

Offset PartitionReplica::highWatermark() const noexcept
{
    if (role_ == Role::Leader) {
        return leaderState_.highWatermark();
    }
    return followerHw_;
}

const std::vector<NodeId>& PartitionReplica::isr() const noexcept
{
    static const std::vector<NodeId> kEmpty{};
    if (role_ == Role::Leader && isrManager_) {
        return isrManager_->isr();
    }
    return kEmpty;
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

void PartitionReplica::recomputeHw()
{
    // Ensure leader LEO is current before computing.
    leaderState_.setLeaderLogEndOffset(log_->logEndOffset());
    // ReplicaLeaderState recomputes HW internally on the next follower update
    // or when we force it via setLeaderLogEndOffset.  Because HW is only
    // recomputed inside updateFollowerFetchOffset() and setLeaderLogEndOffset
    // doesn't trigger it, we force a recompute by re-issuing the leader's own
    // LEO update which calls recomputeHw() inside the state object.
    //
    // Actually ReplicaLeaderState::setLeaderLogEndOffset does NOT call
    // recomputeHw() — only updateFollowerFetchOffset does. For a 1-member ISR
    // (leader only), we can force recompute by pretending the leader fetched:
    // But that's wrong for a multi-member ISR.
    //
    // Solution: we expose a public recompute trigger by calling
    // updateFollowerFetchOffset for the leader itself with the current LEO.
    // This is semantically correct: the leader "fetched" its own LEO.
    leaderState_.updateFollowerFetchOffset(cfg_.selfId, log_->logEndOffset());
}

void PartitionReplica::syncIsrToLeaderState()
{
    leaderState_.setIsr(isrManager_->isr());
}

void PartitionReplica::truncateLog(Offset truncateOffset)
{
    Offset currentLeo = log_->logEndOffset();
    if (truncateOffset >= currentLeo) return;

    log_->truncateTo(truncateOffset);

    // Conservatively cap followerHw_.
    if (followerHw_ > truncateOffset) {
        followerHw_ = truncateOffset;
    }
}

} // namespace rune::replication
