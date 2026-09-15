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

        // DR-BUG-3: seed the min-ISR-gated committed floor from the initial HW
        // (see becomeLeader for the rationale — prevents a gated HW of 0 from
        // hiding already-committed data while the ISR is below minIsr).
        lastHealthyHw_ = leaderState_.highWatermark();

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

    // DR-BUG-3: seed the min-ISR-gated committed floor from the (majority-)
    // committed HW captured above.  Without this, a newly-elected leader whose
    // ISR has not yet re-expanded to minIsr would report a GATED highWatermark()
    // of min(rawHw, lastHealthyHw_=0) == 0, hiding ALL already-committed data
    // from HW-bounded subscribers (GR-6) during the ISR-rebuild window — even
    // though committedHw is genuinely majority-durable.  committedHw is the last
    // majority-committed offset (exactly the floor we are allowed to expose while
    // the ISR is below minIsr), so it is the correct seed.
    if (committedHw > lastHealthyHw_) {
        lastHealthyHw_ = committedHw;
    }

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

    // GR-10: push newly-appended bytes to ISR followers immediately (best-effort).
    // Collapsed to ~RTT from the follower-poll floor.
    if (cfg_.pushOnAppend) {
        pushToFollowers(baseOffset);
    }

    return baseOffset;
}

// ---------------------------------------------------------------------------
// Leader: reconcile after an external direct-to-Log append
// ---------------------------------------------------------------------------

void PartitionReplica::onLeaderLogAppended() noexcept
{
    if (role_ != Role::Leader) return;   // follower LEO/HW driven by applyReplicated()
    // Capture the pre-update LEO so pushToFollowers() knows which bytes are new.
    Offset prevLeo = lastPushedOffset_;
    // Re-read the log's LEO and recompute HW (leader always in ISR, so with the
    // ISR satisfied the HW advances to cover the newly-appended bytes exactly as
    // it would after append()).
    leaderState_.setLeaderLogEndOffset(log_->logEndOffset());

    // GR-10: push newly-appended bytes to ISR followers immediately (best-effort).
    if (cfg_.pushOnAppend) {
        pushToFollowers(prevLeo);
    }
}

// ---------------------------------------------------------------------------
// Leader: onFetchRequest
// ---------------------------------------------------------------------------

int16_t PartitionReplica::onFetchRequest(const FetchRequestMsg& req, int64_t nowNs)
{
    // Guard: drop silently if this replica is not yet the leader.
    // This can happen during the window between Aeron transport receiving a
    // FetchRequest and the metadata bridge promoting us via becomeLeader().
    // Not an error — the follower will retry on the next fetch interval.
    if (role_ != Role::Leader) {
        return kErrNotLeader;
    }

    // Epoch fence: reject stale leader epochs.
    if (isStaleEpoch(req.leaderEpoch, epoch_)) {
        FetchResponseMsg::encode(
            encodeScratch_.data(), 0,
            epoch_, cfg_.partitionId,
            leaderState_.highWatermark(),
            /*logStartOffset=*/0,
            kErrStaleEpoch,
            {},
            cfg_.topicId);   // DR-BUG-5: stamp topicId for transport routing
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
            {},
            cfg_.topicId);   // DR-BUG-5: stamp topicId for transport routing
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

        // GR-1 fix: whole-batch alignment.
        //
        // Walk the returned bytes with RecordBatchReader and truncate to the
        // largest whole-batch prefix that does NOT exceed req.maxBytes.
        // This ensures the encoded FetchResponse fits within the Aeron
        // publication's maxMessageLength (callers cap req.maxBytes to
        // pub.maxMessageLength() - kFetchResponseOverhead before calling us).
        // Without this, a partial-batch tail could push the encoded response
        // over the transport ceiling, causing offer() to throw.
        if (!logBytes.empty()) {
            const std::size_t cap = static_cast<std::size_t>(req.maxBytes);
            std::size_t pos = 0;
            std::size_t committedEnd = 0;
            while (pos < logBytes.size()) {
                std::size_t rem = logBytes.size() - pos;
                if (rem < static_cast<std::size_t>(
                        rune::protocol::batch::kRecordBatchOverhead)) break;
                rune::protocol::RecordBatchReader r(logBytes.data() + pos, rem);
                int32_t total = r.totalLength();
                if (total <= 0) break;
                std::size_t nextEnd = pos + static_cast<std::size_t>(total);
                if (nextEnd > logBytes.size()) break;
                // Stop before a batch that would push the byte count over cap.
                if (nextEnd > cap) break;
                committedEnd = nextEnd;
                pos = committedEnd;
            }
            logBytes.resize(committedEnd);
        }
    }

    // DR-BUG-3: advertise the min-ISR-GATED committed HW to followers via the
    // gated accessor.  followerHw_ = min(localLeo, resp.highWatermark), so a
    // rejoining follower never treats a minority leader's uncommitted tail as
    // committed.  Note this does NOT bound catch-up: log_->read() above is
    // intentionally NOT HW-bounded, so the follower still fetches (and applies)
    // the uncommitted tail and advances its own LEO — which is exactly what
    // lets the leader's real (majority) HW advance and the ISR re-expand.  Only
    // the follower's *committed view* (followerHw_) is held at the majority floor
    // until the ISR heals.
    const Offset advertiseHw = highWatermark();
    FetchResponseMsg::encode(
        encodeScratch_.data(), 0,
        epoch_,
        cfg_.partitionId,
        advertiseHw,
        /*logStartOffset=*/0,
        kErrNone,
        std::span<const uint8_t>(logBytes.data(), logBytes.size()),
        cfg_.topicId);   // DR-BUG-5: stamp topicId for transport routing

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

    // GR-10 stale-epoch fence: drop any push/response from a deposed leader.
    //
    // With push-on-append enabled, a leader that has just been deposed may
    // send unsolicited FetchResponse frames before it learns it is stale.
    // The follower has already incremented its epoch_ via becomeFollower();
    // allowing the stale push to proceed would corrupt the follower's log
    // with records from the wrong epoch.
    //
    // Fix: if resp.leaderEpoch < epoch_, the frame is from a deposed leader —
    // drop it unconditionally.  The epoch-upgrade path (> epoch_) is kept for
    // the rare case where a follower learns of a new epoch via a FetchResponse
    // directly (future direct-fetch path).
    if (resp.leaderEpoch < epoch_) {
        return; // stale-epoch push — deposed leader must not mutate a newer follower's log
    }

    // Capture pre-apply state to detect if anything changed (for fast-ack guard).
    const Offset preLeo = log_->logEndOffset();
    const Offset preHw  = followerHw_;

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

    // GR-10: fast-ack — immediately emit a FetchRequest to the leader carrying
    // our new LEO.  This collapses acks=All from fetchInterval+RTT to ~RTT
    // because the leader receives the follower's updated LEO and advances HW
    // without waiting for the next fetch interval to fire.
    //
    // A duplicate periodic fetch from doWork() is harmless — the leader simply
    // re-reads from the follower's fetchOffset (idempotent, no divergence).
    //
    // Guard: only fast-ack if new records arrived OR HW advanced.  An empty
    // HW-only push with no actual new data does not warrant an extra round-trip.
    const Offset postLeo = log_->logEndOffset();
    const Offset postHw  = followerHw_;
    if (cfg_.fastAckOnApply && role_ == Role::Follower
        && (postLeo > preLeo || postHw > preHw)) {
        FetchRequestMsg ack = nextFetch();
        transport_->sendFetchRequest(ack);
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
    req.topicId     = cfg_.topicId;   // DR-BUG-5: stamp topicId for transport routing
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
    if (role_ != Role::Leader) {
        return followerHw_;
    }

    // Raw majority-of-ISR HW from the leader state (spec §4).
    const Offset rawHw = leaderState_.highWatermark();

    // DR-BUG-3: min-ISR COMMIT gate on the exposed HW.
    //
    // computeHw() takes the majority-th highest LEO across the CURRENT ISR.
    // When the ISR shrinks to a lone minority survivor (|ISR|==1, majority-of-1
    // == leaderLeo), rawHw jumps to the leader's own LEO — offsets that no real
    // majority holds.  Exposing that as "committed" would let the acks=All drain
    // ack it and HW-bounded subscribers (GR-6) read it, only for it to be
    // truncated on failover.  So: while the ISR is BELOW minIsr, the effective
    // committed HW must not advance past the last offset committed while the ISR
    // was healthy.
    //
    // lastHealthyHw_ tracks that frozen floor.  It advances (monotonically) only
    // while |ISR| >= minIsr; below minIsr it is held constant and clamps rawHw.
    const int minIsr  = (cfg_.minIsr >= 1) ? cfg_.minIsr : 1;
    const int isrSize = isrManager_ ? static_cast<int>(isrManager_->isr().size()) : 0;

    if (isrSize >= minIsr) {
        // Healthy ISR: rawHw is a legitimate majority-committed offset.  Advance
        // the frozen floor and expose the raw HW.
        if (rawHw > lastHealthyHw_) {
            lastHealthyHw_ = rawHw;
        }
        return rawHw;
    }

    // ISR below minIsr: do not advance past the last majority-committed offset.
    // (rawHw may be at or above lastHealthyHw_ due to the monotonic prevHw_
    // floor; clamp down to the frozen healthy value.)
    return (rawHw < lastHealthyHw_) ? rawHw : lastHealthyHw_;
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

// ---------------------------------------------------------------------------
// GR-10: pushToFollowers — send newly-appended bytes to ISR followers immediately.
//
// Called from append() and onLeaderLogAppended() on the leader duty-cycle thread.
// Non-blocking: sendFetchResponse() is a best-effort tryClaim/offer.  A dropped
// push is recovered by the follower's periodic fetch backstop.
//
// Design:
//   1. Read log bytes from [fromOffset, LEO) using the existing log_.read() path.
//   2. Apply the same whole-batch alignment as onFetchRequest() to ensure the
//      encoded FetchResponse fits within transport limits.
//   3. Encode a FetchResponse into encodeScratch_ (pre-allocated, zero per-call alloc).
//   4. Call sendFetchResponse(followerId, resp) for each ISR follower != selfId.
//   5. Update lastPushedOffset_ so onLeaderLogAppended() does not re-push.
//
// Epoch fencing: the FetchResponse carries the current leader epoch; followers
// compare it against their local epoch and drop mismatches (existing applyReplicated
// guard: resp.leaderEpoch > epoch_ → truncate; resp.leaderEpoch < epoch_ → silent
// drop via errorCode path handled above).  Stale-epoch pushes are harmless but
// wasteful; the follower's fast-ack fetch-response will carry the new epoch.
// ---------------------------------------------------------------------------
void PartitionReplica::pushToFollowers(Offset fromOffset) noexcept
{
    if (role_ != Role::Leader) return;
    if (!isrManager_) return;

    const std::vector<NodeId>& isrList = isrManager_->isr();
    if (isrList.empty()) return;

    // Nothing new to push if fromOffset is at or above current LEO.
    Offset currentLeo = log_->logEndOffset();
    if (fromOffset >= currentLeo) {
        lastPushedOffset_ = currentLeo;
        return;
    }

    // Read the bytes from the log — same path as onFetchRequest().
    std::vector<uint8_t> logBytes = log_->read(fromOffset, cfg_.maxFetchBytes);

    // Apply whole-batch alignment to avoid partial-batch tails.
    if (!logBytes.empty()) {
        const std::size_t cap = static_cast<std::size_t>(cfg_.maxFetchBytes);
        std::size_t pos = 0;
        std::size_t committedEnd = 0;
        while (pos < logBytes.size()) {
            std::size_t rem = logBytes.size() - pos;
            if (rem < static_cast<std::size_t>(
                    rune::protocol::batch::kRecordBatchOverhead)) break;
            rune::protocol::RecordBatchReader r(logBytes.data() + pos, rem);
            int32_t total = r.totalLength();
            if (total <= 0) break;
            std::size_t nextEnd = pos + static_cast<std::size_t>(total);
            if (nextEnd > logBytes.size()) break;
            if (nextEnd > cap) break;
            committedEnd = nextEnd;
            pos = committedEnd;
        }
        logBytes.resize(committedEnd);
    }

    // Encode once into encodeScratch_ — reuse for all followers.
    // DR-BUG-3: advertise the min-ISR-gated HW (see onFetchRequest).
    FetchResponseMsg::encode(
        encodeScratch_.data(), 0,
        epoch_,
        cfg_.partitionId,
        highWatermark(),
        /*logStartOffset=*/0,
        kErrNone,
        std::span<const uint8_t>(logBytes.data(), logBytes.size()),
        cfg_.topicId);   // DR-BUG-5: stamp topicId for transport routing

    auto resp = FetchResponseMsg::decode(encodeScratch_.data(), 0);

    // Push to each ISR follower (skip self).
    for (NodeId followerId : isrList) {
        if (followerId == cfg_.selfId) continue;
        transport_->sendFetchResponse(followerId, resp);
    }

    lastPushedOffset_ = currentLeo;
}

} // namespace rune::replication
