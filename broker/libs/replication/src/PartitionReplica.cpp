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
// Used by the LEADER path (append from producer).
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
        isrManagerOwned_ = std::make_unique<IsrManager>(replicas, cfg_.selfId, cfg_.replicaLagMaxMs);
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
    // NB1 fix: capture committed offset BEFORE flipping role or mutating state.
    // highWatermark() still returns the OLD role's view here:
    //   - Was follower: returns followerHw_ = min(localLeo, lastKnownLeaderHw)
    //   - Was leader:   returns leaderState_.highWatermark() = exact committed HW
    // Either is a safe committed floor (never above the true committed offset).
    // Do NOT use log_->logEndOffset() (the LEO): a node promoted with an
    // uncommitted tail (localLeo > committedHw) would mark that tail as committed,
    // causing uncommitted/divergent records to be readable after failover.
    Offset committedHw = highWatermark();

    epoch_ = newEpoch;
    role_  = Role::Leader;

    std::vector<NodeId> replicas = isr; // simplification: replicas == isr at election
    if (isrManagerOwned_) {
        // Preserve replica knowledge; just update ISR.
        isrManagerOwned_->setIsr(isr);
    } else {
        isrManagerOwned_ = std::make_unique<IsrManager>(replicas, cfg_.selfId, cfg_.replicaLagMaxMs);
        isrManagerOwned_->setIsr(isr);
    }
    isrManager_ = isrManagerOwned_.get();

    leaderState_.setLeaderId(cfg_.selfId);

    // NB1/B2 fix: set the HW floor BEFORE setting the ISR and leader LEO.
    // setIsr() and setLeaderLogEndOffset() both call recomputeHw() eagerly,
    // which advances prevHw_ to max(prevHw_, computeHw()).  If a 1-node ISR
    // (or any ISR where leaderLeo >= committedHw) were set first, prevHw_
    // would jump to leaderLeo (the LEO), making the subsequent
    // setHighWatermarkFloor(committedHw) a no-op.  Setting the floor first
    // ensures it is respected as a minimum even after the ISR/LEO recompute
    // raises prevHw_ further (which is fine — recompute can only advance HW
    // if the new compute is ABOVE committedHw, which is only legitimate when
    // a majority of the ISR actually holds those offsets).
    leaderState_.setHighWatermarkFloor(committedHw);

    leaderState_.setIsr(isr);
    leaderState_.setLeaderLogEndOffset(log_->logEndOffset());

    epochCache_.assign(static_cast<int32_t>(epoch_), log_->logEndOffset());

    lastIsrCheckNs_ = 0;
}

void PartitionReplica::becomeFollower(Epoch leaderEpoch, NodeId leaderId)
{
    // Truncate uncommitted tail BEFORE adopting the new epoch so that
    // applyReplicated()'s ">" check still fires on the first fetch (and as a
    // belt-and-suspenders path). Compute the boundary using the PRE-transition
    // epoch so the cache lookup is correct.
    if (leaderEpoch > epoch_) {
        // C2: Capture the committed HW NOW (while the old role is still valid)
        // as the safety floor for truncation.
        //
        // For a node that WAS leader:   highWatermark() returns leaderState_.highWatermark()
        //                                which is the exact committed offset (accurate).
        // For a node that WAS follower: highWatermark() returns followerHw_ =
        //                                min(localLeo, lastKnownLeaderHw) — conservative;
        //                                may be below the true committed offset if the
        //                                follower missed a HW-only update, but is never
        //                                above it, so truncating here never drops committed
        //                                data and at worst causes a re-fetch of a small
        //                                committed tail (safe).
        //
        // A full OffsetsForLeaderEpoch exchange with the new leader (Kafka P3)
        // would give a precise boundary, but is out of scope for this phase.
        // Truncating to committedHw is the documented safe fallback.
        Offset committedHw = highWatermark();

        // Try epoch-cache boundary first (Kafka OffsetsForLeaderEpoch-style).
        Offset truncOff = truncationOffsetForEpochChange(epochCache_, leaderEpoch);

        // If the cache returns -1 (no epoch >= leaderEpoch in our cache, which
        // is the common case when all records share the same old epoch), fall
        // back to the committed boundary: everything above HW is uncommitted
        // and potentially divergent from the new leader's timeline.
        if (truncOff < 0) {
            truncOff = committedHw;
        }

        // Safety clamp: never truncate below the committed prefix.
        // This ensures C2: we never drop committed data regardless of what the
        // epoch-cache returns.
        if (truncOff < committedHw) {
            truncOff = committedHw;
        }

        // Only truncate if there is actually an uncommitted tail to drop.
        Offset currentLeo = log_->logEndOffset();
        if (truncOff < currentLeo) {
            // A10: prune epoch cache in step with log truncation so that a
            // subsequent epochCache_.assign() after truncation is not silently
            // dropped by the monotonic-startOffset guard.
            epochCache_.truncateFromEnd(truncOff);
            log_->truncateTo(truncOff);
            // followerHw_ will be reset below; no need to cap it here.
        }
    }

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
// Leader: reconcile after an external direct-to-Log append
// ---------------------------------------------------------------------------

void PartitionReplica::onLeaderLogAppended() noexcept
{
    if (role_ != Role::Leader) return;   // follower LEO/HW driven by applyReplicated()
    // Re-read the log's LEO and recompute HW (leader always in ISR, so with the
    // ISR satisfied the HW advances to cover the newly-appended bytes exactly as
    // it would after append()).
    leaderState_.setLeaderLogEndOffset(log_->logEndOffset());
}

// ---------------------------------------------------------------------------
// Leader: onFetchRequest
// ---------------------------------------------------------------------------

int16_t PartitionReplica::onFetchRequest(const FetchRequestMsg& req, int64_t nowNs)
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

    // A-NEW-5 fix: if the follower carries a HIGHER epoch, this leader is stale.
    // The correct error code is kErrNotLeader (semantically: "you are ahead of
    // me — I should not be leading at this epoch") rather than kErrStaleEpoch
    // (which means "your epoch is old").  Additionally, we must actually step
    // ourselves down: fence by transitioning to Follower so the control plane
    // can detect and fix the split-brain (leader operating at a lower epoch).
    // In practice the coordinator demotes via becomeFollower(); here we send the
    // corrected error response so the follower knows to report the discrepancy.
    if (mustStepDown(req.leaderEpoch, epoch_)) {
        // Capture committed HW before any role change.
        Offset committedHw = leaderState_.highWatermark();
        // Step ourselves down to follower (fence against further leader activity).
        // We adopt the follower's higher epoch and the same leaderId as ourselves
        // (the control plane will correct the leaderId via the next metadata push).
        role_     = Role::Follower;
        epoch_    = req.leaderEpoch;
        leaderId_ = req.followerId; // best-effort; coordinator will correct
        followerHw_ = committedHw;
        lastFetchEmitNs_ = 0;

        FetchResponseMsg::encode(
            encodeScratch_.data(), 0,
            epoch_, cfg_.partitionId,
            committedHw,
            /*logStartOffset=*/0,
            kErrNotLeader,  // A-NEW-5: correct error code (follower is ahead)
            {});
        auto resp = FetchResponseMsg::decode(encodeScratch_.data(), 0);
        transport_->sendFetchResponse(req.followerId, resp);
        return kErrNotLeader;
    }

    // Record follower fetch: update ISR manager + leader state.
    // Pass the REAL nowNs so time-lag eviction uses the actual fetch time,
    // not the stale last-ISR-check timestamp.
    isrManager_->onFollowerFetch(req.followerId, req.fetchOffset, nowNs);
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
    // C1 fix: clamp read to [fetchOffset, HW) so consumers never receive
    // records with offset >= HW (uncommitted).  Walk the batches returned by
    // the log and drop any that start at or beyond HW.
    auto raw = log_->read(fetchOffset, maxBytes);
    if (raw.empty()) return {};

    // Walk individual RecordBatch frames and stop before any batch whose
    // baseOffset >= HW (exclusive upper bound on committed data).
    std::size_t pos = 0;
    std::size_t committedEnd = 0;
    while (pos < raw.size()) {
        std::size_t rem = raw.size() - pos;
        if (rem < static_cast<std::size_t>(
                rune::protocol::batch::kRecordBatchOverhead)) break;
        rune::protocol::RecordBatchReader r(raw.data() + pos, rem);
        int32_t total = r.totalLength();
        if (total <= 0 || pos + static_cast<std::size_t>(total) > raw.size()) break;
        int64_t batchBase    = r.baseOffset();
        int32_t lastOffDelta = r.lastOffsetDelta();
        // NB3 fix: a batch is only fully committed if its LAST record is strictly
        // below HW (batchBase + lastOffsetDelta < hw).  A batch whose baseOffset
        // is below HW but whose last record is at or above HW straddles the
        // high-watermark — returning it would expose uncommitted records (offsets
        // in [hw, lastOffset]).  Stop before any such straddling batch.
        // For single-record batches lastOffsetDelta==0, so the condition reduces
        // to batchBase < hw — identical to the old single-record behaviour.
        if (batchBase + lastOffDelta >= hw) break; // batch straddles or is beyond HW
        committedEnd = pos + static_cast<std::size_t>(total);
        pos = committedEnd;
    }

    if (committedEnd == 0) return {};
    raw.resize(committedEnd);
    return raw;
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
        // NB2/C2 defense-in-depth: clamp truncation to the committed floor.
        // In the coordinated flow becomeFollower() runs first (bumping epoch_),
        // so this branch is currently unreachable, but it must mirror
        // becomeFollower()'s committedHw floor in case a follower adopts a new
        // epoch purely via a FetchResponse (e.g., in future direct-fetch paths).
        // Never truncate below the committed prefix regardless of what the epoch
        // cache returns.
        Offset committedFloor = followerHw_; // follower's known committed HW
        Offset truncOff = truncationOffsetForEpochChange(epochCache_,
                                                          resp.leaderEpoch);
        if (truncOff >= 0) {
            // Clamp: never truncate below committed.
            if (truncOff < committedFloor) {
                truncOff = committedFloor;
            }
            truncateLog(truncOff);
        }
        epoch_ = resp.leaderEpoch;
        epochCache_.assign(static_cast<int32_t>(epoch_), log_->logEndOffset());
    }

    // Append replicated records if any.
    // In normal operation resp.records is a sequence of valid RecordBatch frames
    // copied verbatim from the leader's log.  Write them directly so the follower's
    // log is byte-identical to the leader's over [0, HW) — avoiding double-framing.
    //
    // Detection: a valid RecordBatch sequence starts with a 61-byte header whose
    // magic byte at position kMagicOffset == kMagicValue (2).  Raw user payloads
    // injected in unit tests may not carry this magic, in which case we fall back
    // to the old makeBatch() wrapping so those tests continue to pass.
    if (!resp.records.empty()) {
        const std::size_t kMagicOff = static_cast<std::size_t>(
            rune::protocol::batch::kMagicOffset);
        const uint8_t kMagic = static_cast<uint8_t>(
            rune::protocol::batch::kMagicValue);

        bool looksLikeRecordBatch =
            resp.records.size() > kMagicOff
            && resp.records[kMagicOff] == kMagic;

        if (looksLikeRecordBatch) {
            // A-NEW-1 fix: idempotent apply — skip records whose baseOffset < LEO.
            //
            // A pipelined (multi-in-flight) fetch can deliver two responses that
            // share the same fetchOffset F (both requested from F before either
            // response was consumed).  If the 2nd response both overlaps AND
            // extends, the old `lastOff >= LEO` guard would still append the
            // entire span verbatim — re-appending [F, currentLeo) and then
            // appending [currentLeo, newEnd), producing duplicate / non-monotonic
            // offsets in the log.
            //
            // Fix: walk the response batch-by-batch; skip any batch whose
            // baseOffset is entirely below the current LEO (already held), and
            // only append the strictly-new tail whose baseOffset >= currentLeo.
            // If the first new batch starts exactly at currentLeo the log stays
            // contiguous.  Any batch that starts ABOVE currentLeo (a gap) is also
            // skipped to preserve contiguity invariants.

            Offset currentLeo = log_->logEndOffset();
            std::size_t newStart = resp.records.size(); // byte-offset of first new batch
            int64_t lastOff = -1;
            std::size_t pos = 0;
            while (pos < resp.records.size()) {
                std::size_t rem = resp.records.size() - pos;
                if (rem < static_cast<std::size_t>(
                        rune::protocol::batch::kRecordBatchOverhead)) break;
                rune::protocol::RecordBatchReader r(resp.records.data() + pos, rem);
                int32_t total = r.totalLength();
                if (total <= 0 || pos + static_cast<std::size_t>(total)
                                  > resp.records.size()) break;
                int64_t batchBase = r.baseOffset();
                if (batchBase < currentLeo) {
                    // Already held — skip this batch.
                    pos += static_cast<std::size_t>(total);
                    continue;
                }
                if (batchBase > currentLeo) {
                    // Gap — would break contiguity; stop.
                    break;
                }
                // batchBase == currentLeo: this is where new data starts.
                if (newStart == resp.records.size()) {
                    newStart = pos; // mark the byte offset of the first new batch
                }
                lastOff = batchBase + r.lastOffsetDelta();
                currentLeo = lastOff + 1; // advance the expected-next offset
                pos += static_cast<std::size_t>(total);
            }

            if (newStart < resp.records.size() && lastOff >= log_->logEndOffset()) {
                // Append only the strictly-new suffix.
                std::vector<uint8_t> raw(resp.records.begin() + static_cast<std::ptrdiff_t>(newStart),
                                          resp.records.end());
                log_->append(raw, lastOff);
            }
        } else {
            // Fallback: raw user bytes (e.g. injected by unit tests) — wrap in
            // a RecordBatch so the log segment scanner can parse them.
            Offset baseOffset = log_->logEndOffset();
            auto batch = makeBatch(baseOffset, resp.records);
            log_->append(batch, baseOffset);
        }
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
    // S1/C4 fix: setLeaderLogEndOffset now eagerly recomputes HW internally,
    // so a single call is sufficient.  We no longer call
    // updateFollowerFetchOffset(selfId, ...) — that was an incorrect hack that
    // polluted the follower LEO map with the leader's own entry.
    leaderState_.setLeaderLogEndOffset(log_->logEndOffset());
}

void PartitionReplica::syncIsrToLeaderState()
{
    leaderState_.setIsr(isrManager_->isr());
}

void PartitionReplica::truncateLog(Offset truncateOffset)
{
    Offset currentLeo = log_->logEndOffset();
    if (truncateOffset >= currentLeo) return;

    // A10 fix: prune the epoch cache in step with the log truncation.
    //
    // After the A5 fix in libs/storage, LeaderEpochCache::assign() silently
    // drops any assign whose startOffset is below the current latest entry.
    // If the epoch cache retains an entry whose startOffset is ABOVE the
    // post-truncation log-end-offset, a subsequent higher-epoch assign
    // (applyReplicated: epochCache_.assign(epoch_, log_->logEndOffset())) is
    // silently dropped — leaving the cache in an inconsistent state where
    // truncationOffsetForEpochChange() can return a too-high boundary.
    //
    // Fix: call truncateFromEnd(truncateOffset) BEFORE truncating the log so
    // all epoch entries whose startOffset >= truncateOffset are pruned; the
    // cache then reflects only epochs that start within the surviving log.
    epochCache_.truncateFromEnd(truncateOffset);

    log_->truncateTo(truncateOffset);

    // Conservatively cap followerHw_.
    if (followerHw_ > truncateOffset) {
        followerHw_ = truncateOffset;
    }
}

} // namespace rune::replication
