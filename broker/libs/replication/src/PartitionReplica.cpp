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
    } else {
        // DR-BUG-6: seed the follower's epoch cache so it knows its latest
        // leader-epoch (needed to build the OffsetsForLeaderEpoch request).
        // The initial epoch's records (if any pre-seeded) begin at offset 0.
        if (epoch_ >= 0) {
            epochCache_.assign(static_cast<int32_t>(epoch_), /*startOffset=*/0);
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

    // DR-BUG-6: arm the follower-lag TIME-clock reset for the first leader
    // doWork(nowNs).  Until it fires, no ISR check runs (lastIsrCheckNs_==0 makes
    // the first doWork the ISR-check tick, and we reset the clock there BEFORE the
    // shrink), so followers get a full lag window to probe/fetch instead of being
    // evicted at t=election+10ms with lastFetchTimeNs=0.  This is what lets an
    // up-to-date survivor stay in the ISR while it runs its OffsetsForLeaderEpoch
    // probe, so the reverse-reconcile engages instead of an authoritative truncate.
    leaderLagClockNeedsReset_ = true;
}

void PartitionReplica::becomeFollower(Epoch leaderEpoch, NodeId leaderId)
{
    // DR-BUG-6 — DEFERRED, epoch-correct truncation (KIP-101).
    //
    // We do NOT truncate eagerly to the committed HW here.  Eagerly truncating a
    // follower's tail to its OWN (possibly stale) committed-HW view is the DR-8
    // data-loss bug: an up-to-date survivor that never received the dead leader's
    // final HW-advance would destroy committed records it alone still holds
    // before the (possibly deficient) new leader can pull them.
    //
    // Instead, on epoch change we DEFER: we arm an OffsetsForLeaderEpoch probe.
    // The follower asks the NEW leader for the end offset of the last epoch they
    // share, and only then truncates to the TRUE divergence point
    // (applyOffsetsForLeaderEpochResponse).  While the probe is pending the
    // follower does not fetch and does not truncate — it keeps its full log so
    // the new leader can reconcile any committed tail it is missing.
    //
    // C2 floor: capture the committed HW NOW (while the old role's view is still
    // valid) as a hard lower bound the deferred truncation may never cross.
    if (leaderEpoch > epoch_) {
        Offset committedHw = highWatermark();

        epochProbePending_  = true;
        epochProbeEpoch_    = leaderEpoch;   // epoch we are transitioning INTO
        epochProbeFloor_    = committedHw;    // never truncate below this
        epochProbeAttempts_ = 0;
    } else if (leaderEpoch == epoch_ && epochProbePending_
               && epochProbeEpoch_ == leaderEpoch) {
        // Re-assertion of the SAME epoch we are already probing (e.g. a node
        // demoted at the new epoch while dead, then revived under that same
        // epoch): PRESERVE the pending probe so the deferred truncation still
        // fires once the exchange completes.  Reset the attempt counter and
        // re-capture the committed floor so the probe restarts cleanly.
        epochProbeFloor_    = highWatermark();
        epochProbeAttempts_ = 0;
    } else {
        // No epoch advance and no in-flight probe for this epoch: no probe, no
        // truncation (normal same-epoch re-assert / initial seed).
        epochProbePending_ = false;
    }

    epoch_    = leaderEpoch;
    role_     = Role::Follower;
    leaderId_ = leaderId;
    followerHw_ = 0;
    lastFetchEmitNs_ = 0;
    reconcileTarget_ = -1;
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

    // DR-BUG-6 — deficient-leader reverse-catch-up backstop.
    //
    // Primary trigger is the OffsetsForLeaderEpoch probe path, which carries the
    // follower's TAIL epoch and is the ONLY place we can validate lineage.  A
    // plain FetchRequest does NOT carry the follower's tail epoch (req.leaderEpoch
    // is the epoch the follower fetches UNDER — our current epoch — not the epoch
    // of its ahead tail), so we MUST NOT decide keep-vs-truncate from a plain
    // fetch: doing so was the divergence bug (any ahead tail looked "keepable").
    //
    // Backstop policy: if an ahead follower reaches a plain fetch, we only CONTINUE
    // a reconcile the probe path already established with THIS peer (idempotent
    // retry).  We never START a lineage-blind pull here.  If no such reconcile is
    // in flight, we serve nothing this round and let the follower's own epoch
    // probe (armed on becomeFollower) drive the lineage-checked decision.
    if (req.fetchOffset > log_->logEndOffset()) {
        if (reconcileTarget_ == req.followerId) {
            FetchRequestMsg pull{};
            pull.leaderEpoch = epoch_;
            pull.partition   = cfg_.partitionId;
            pull.followerId  = cfg_.selfId;               // WE are the puller
            pull.fetchOffset = log_->logEndOffset();      // from our LEO
            pull.maxBytes    = cfg_.maxFetchBytes;
            pull.topicId     = cfg_.topicId;
            transport_->sendReconcileRequest(req.followerId, pull);
        }
        // Serve nothing forward to an ahead follower: forward-serving under the
        // new epoch could stamp divergent bytes.  The follower keeps its tail
        // until its epoch probe resolves the correct truncation point.
        return kErrNone;
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

    // DR-BUG-6: maintain the leader-epoch cache on the follower.  If this
    // response's epoch is newer than anything we have recorded, record its
    // start offset as the offset where this epoch's first applied records began
    // (preLeo).  assign() is monotonic + idempotent, so a repeated same-epoch
    // call is a harmless no-op.  This keeps latestEpoch() accurate so the
    // OffsetsForLeaderEpoch request carries the correct requestedEpoch.
    if (resp.leaderEpoch >= 0
        && static_cast<int32_t>(resp.leaderEpoch) > epochCache_.latestEpoch()) {
        epochCache_.assign(static_cast<int32_t>(resp.leaderEpoch), preLeo);
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
// DR-BUG-6 — OffsetsForLeaderEpoch exchange (KIP-101)
// ---------------------------------------------------------------------------

// (Leader) DR-BUG-6 divergence guard — epoch-lineage gate (see header).
bool PartitionReplica::followerTailIsSharedContinuation(
        Epoch requestedEpoch, Offset followerLeo) const
{
    const Offset leaderLeo = log_->logEndOffset();

    // The follower must genuinely be AHEAD for a reverse-reconcile to make sense.
    if (followerLeo <= leaderLeo) return false;

    // A follower with no epoch history (requestedEpoch < 0) cannot be validated
    // against our lineage → not a shared continuation (truncate it).
    if (requestedEpoch < 0) return false;

    // KIP-101 lineage resolution against OUR OWN epoch cache.
    auto r = epochCache_.endOffsetForEpoch(
        static_cast<int32_t>(requestedEpoch), leaderLeo);
    const int32_t responseEpoch = r.first;
    const Offset  endOffset     = r.second;

    // Shared continuation ⇔ we share the EXACT epoch the follower's tail is in
    // (responseEpoch == requestedEpoch) AND that epoch is our own latest OPEN
    // epoch, so its boundary is our LEO (endOffset == leaderLeo, i.e. >= leaderLeo).
    // A follower tail in an epoch we only share at an OLDER boundary — or in an
    // epoch we never held (responseEpoch < requestedEpoch, or -1) — is divergent.
    return responseEpoch == static_cast<int32_t>(requestedEpoch)
        && endOffset == leaderLeo;
}

// (Leader) Answer a follower's epoch-boundary query from the LeaderEpochCache.
//
// DR-BUG-6 divergence guard — the deficient-leader reconcile now requires BOTH
// conditions, and the EPOCH-LINEAGE condition is the one that closes the
// divergence hole:
//
//   (1) followerTailIsSharedContinuation(requestedEpoch, followerLeo) — the
//       follower's ahead tail is on THIS leader's OWN epoch lineage (a genuine
//       continuation of our still-open shared epoch, KIP-101).  A DIVERGENT tail
//       is ALWAYS in a non-shared epoch (that is what makes it divergent), so
//       this check alone rejects the reviewer's contradictory-tail scenario: the
//       divergent survivor's tail epoch is one WE never held → we answer
//       authoritatively → it TRUNCATES to the shared-epoch boundary.  This is the
//       fix for the DIVERGENCE bug the old ISR-only gate had.
//
//   (2) followerInIsr — WITHIN a shared epoch, offset+epoch cannot distinguish a
//       COMMITTED continuation (DR-8: keep+reconcile) from an UNCOMMITTED tail
//       (old-leader tail: truncate) — both are pure same-epoch records above the
//       survivor's HW.  ISR membership is the only remaining discriminator: the
//       dead old leader is excluded from the new ISR by the failover coordinator,
//       so its uncommitted same-epoch tail fails this gate and truncates, while a
//       genuine ISR survivor's committed tail is kept and reverse-reconciled.
//
// Both must hold to keep+reconcile.  The epoch-lineage check (1) is the necessary
// addition that the old ISR-only gate lacked; (2) remains for the shared-epoch
// committed-vs-uncommitted case that epoch metadata cannot resolve.
void PartitionReplica::onOffsetsForLeaderEpochRequest(
        const OffsetsForLeaderEpochReqMsg& req)
{
    // Only a current leader can answer.
    if (role_ != Role::Leader) {
        OffsetsForLeaderEpochRespMsg resp{};
        resp.leaderEpoch   = epoch_;
        resp.partition     = cfg_.partitionId;
        resp.responseEpoch = -1;
        resp.endOffset     = 0;
        resp.errorCode     = kErrNotLeader;
        resp.topicId       = cfg_.topicId;
        transport_->sendOffsetsForLeaderEpochResponse(req.followerId, resp);
        return;
    }

    // DR-BUG-6 HW-RECOVERY: an OffsetsForLeaderEpoch probe is proof of liveness and
    // carries the follower's LEO.  A survivor DEFERS normal fetching while it
    // probes, so without this the leader never learns the survivor's LEO and its
    // majority-of-ISR high-watermark stays stuck at 0 after failover — the whole
    // committed set becomes UNDELIVERABLE to HW-bounded subscribers (GR-6) even
    // though every record is durably on disk (observed: full committed set
    // undelivered on a post-failover subscribe).  Treat the probe like a fetch for
    // HW/ISR bookkeeping: record the follower's LEO (clamped to our own LEO — a
    // follower can never advance our committed HW beyond what WE hold) so the HW
    // recomputes and the survivor stays a live ISR member.  This is pure HW/liveness
    // bookkeeping; it does NOT change any truncation decision below.
    {
        const Offset trackedLeo = std::min<Offset>(req.followerLeo, log_->logEndOffset());
        if (trackedLeo >= 0) {
            leaderState_.updateFollowerFetchOffset(req.followerId, trackedLeo);
        }
    }

    // Deficient-leader detection — EPOCH-LINEAGE gated AND ISR gated (see the
    // block comment above).  (1) The ahead tail must be a genuine continuation of
    // OUR OWN still-open shared epoch (rejects divergent non-shared-epoch tails),
    // AND (2) the follower must be in the ISR (rejects a dead old leader's
    // uncommitted same-epoch tail).  Only then reverse-reconcile + DEFER.
    const bool followerInIsr =
        isrManager_ && !isrManager_->isr().empty()
        && std::find(isrManager_->isr().begin(), isrManager_->isr().end(),
                     req.followerId) != isrManager_->isr().end();

    const bool sharedCont =
        followerTailIsSharedContinuation(req.requestedEpoch, req.followerLeo);

    if (followerInIsr && sharedCont) {
        // (Re)issue the reverse pull.  We (re)send when idle (reconcileTarget_<0)
        // OR when the pending target is this same follower — the previous pull or
        // its response may have been dropped by the transport, and the follower
        // re-probes on cadence, so re-issuing here is the natural retry (it is
        // idempotent: applyReconcileResponse only appends the strictly-new tail).
        if (reconcileTarget_ < 0 || reconcileTarget_ == req.followerId) {
            reconcileTarget_ = req.followerId;
            FetchRequestMsg pull{};
            pull.leaderEpoch = epoch_;
            pull.partition   = cfg_.partitionId;
            pull.followerId  = cfg_.selfId;               // WE are the puller
            pull.fetchOffset = log_->logEndOffset();      // from our LEO
            pull.maxBytes    = cfg_.maxFetchBytes;
            pull.topicId     = cfg_.topicId;
            transport_->sendReconcileRequest(req.followerId, pull);
        }
        // Fallback-race hardening (the Major): send an EXPLICIT defer keep-alive
        // instead of silently dropping the answer.  Silence lets the follower's
        // fallback probe counter climb to kEpochProbeMaxAttempts under packet loss
        // and fire the destructive fallback-to-committedHw truncation while our
        // reconcile pull is still in flight — reintroducing the DR-8 loss.  The
        // keep-alive tells the follower: KEEP your tail, RESET your counter.
        OffsetsForLeaderEpochRespMsg defer{};
        defer.leaderEpoch   = epoch_;
        defer.partition     = cfg_.partitionId;
        defer.responseEpoch = static_cast<Epoch>(req.requestedEpoch);
        defer.endOffset     = req.followerLeo;   // "keep everything up to your LEO"
        defer.errorCode     = kErrReconcileDeferred;
        defer.topicId       = cfg_.topicId;
        transport_->sendOffsetsForLeaderEpochResponse(req.followerId, defer);
        return;
    }

    // Authoritative answer from our cache.
    auto r = epochCache_.endOffsetForEpoch(
        static_cast<int32_t>(req.requestedEpoch), log_->logEndOffset());

    OffsetsForLeaderEpochRespMsg resp{};
    resp.leaderEpoch   = epoch_;
    resp.partition     = cfg_.partitionId;
    resp.responseEpoch = r.first;
    resp.endOffset     = r.second;
    resp.errorCode     = kErrNone;
    resp.topicId       = cfg_.topicId;
    transport_->sendOffsetsForLeaderEpochResponse(req.followerId, resp);
}

// (Follower) Apply the leader's answer: the DEFERRED, epoch-correct truncation.
void PartitionReplica::applyOffsetsForLeaderEpochResponse(
        const OffsetsForLeaderEpochRespMsg& resp)
{
    // Only meaningful while a probe is pending for this epoch.
    if (!epochProbePending_) return;

    // Fence: the answer must come from our current-epoch leader.  A stale answer
    // (older leaderEpoch) is dropped — we re-probe on the next doWork().
    if (resp.leaderEpoch < epoch_) return;

    if (resp.errorCode == kErrReconcileDeferred) {
        // Fallback-race hardening (the Major): the leader is actively reverse-
        // reconciling a genuine committed continuation from US.  This is an
        // explicit KEEP-ALIVE, not a truncation directive.  KEEP our tail and
        // RESET the fallback probe counter so a slow reconcile under sustained
        // packet loss never trips the destructive fallback-to-committedHw
        // truncation (which reintroduced the DR-8 committed-loss).  Stay pending;
        // doWork() re-probes on cadence and the leader answers kErrNone once its
        // LEO covers our tail (endOffset >= our LEO → no-op truncation).
        epochProbeAttempts_ = 0;
        return;
    }

    if (resp.errorCode != kErrNone) {
        // Leader could not answer (not leader yet / transient).  Keep the probe
        // pending; doWork() re-issues it.  Do NOT truncate — deferring is safe.
        return;
    }

    const Offset currentLeo     = log_->logEndOffset();
    const Offset committedFloor  = epochProbeFloor_;

    // KIP-101 truncation: drop everything at/beyond the leader's end offset for
    // the last epoch we share — the TRUE divergence point.  endOffset is:
    //   • the start of the leader's next epoch (if the shared epoch has CLOSED
    //     on the leader) → our records beyond it diverge from the leader's newer
    //     epoch (uncommitted/stale tail) → truncate;
    //   • the leader's LEO (if the shared epoch is still OPEN on the leader) →
    //     we truncate to the leader's LEO, dropping only what the leader does
    //     not have.
    //
    // The DR-8 "ISR-member ahead within a shared open epoch" case never reaches
    // here with a data-losing endOffset: the leader detects that case on the
    // probe, reverse-reconciles the missing committed records from us FIRST, and
    // only answers once its own LEO covers our tail — so endOffset >= our LEO
    // and this truncation is a no-op.  For a NON-ISR ahead tail (uncommitted),
    // the leader answers immediately with endOffset at its boundary and we
    // correctly truncate the uncommitted tail.
    Offset truncOff = std::min<Offset>(currentLeo, resp.endOffset);

    // C2 defence-in-depth: never truncate below the committed floor captured at
    // transition — committed data is never dropped regardless of the answer.
    if (truncOff < committedFloor) {
        truncOff = committedFloor;
    }

    if (truncOff < currentLeo) {
        truncateLog(truncOff);   // prunes epoch cache in step (A10)
    }

    // Probe resolved — resume normal fetching under the new epoch.
    epochProbePending_ = false;
    lastFetchEmitNs_   = 0;   // fetch immediately on next doWork()

    // Kick a fetch right now so convergence does not wait a full interval.
    FetchRequestMsg req = nextFetch();
    transport_->sendFetchRequest(req);
}

// (Any role) Serve a reconcile pull from an ahead survivor's log.
void PartitionReplica::onReconcileRequest(const FetchRequestMsg& req)
{
    // Read raw bytes [req.fetchOffset, LEO).  A survivor serves everything it
    // holds up to its own LEO — these are committed-or-better records the
    // deficient new leader needs.  (Not HW-bounded: the survivor may itself have
    // a stale HW view, which is exactly the DR-8 case — but every byte it holds
    // up to LEO was part of the committed prefix on the old leader.)
    std::vector<uint8_t> logBytes;
    Offset leo = log_->logEndOffset();
    if (req.maxBytes > 0 && req.fetchOffset < leo) {
        logBytes = log_->read(req.fetchOffset, req.maxBytes);

        // Whole-batch alignment (same as onFetchRequest) so the encoded frame
        // fits transport limits.
        if (!logBytes.empty()) {
            const std::size_t cap = static_cast<std::size_t>(req.maxBytes);
            std::size_t pos = 0, committedEnd = 0;
            while (pos < logBytes.size()) {
                std::size_t rem = logBytes.size() - pos;
                if (rem < static_cast<std::size_t>(
                        rune::protocol::batch::kRecordBatchOverhead)) break;
                rune::protocol::RecordBatchReader r(logBytes.data() + pos, rem);
                int32_t total = r.totalLength();
                if (total <= 0) break;
                std::size_t nextEnd = pos + static_cast<std::size_t>(total);
                if (nextEnd > logBytes.size() || nextEnd > cap) break;
                committedEnd = nextEnd;
                pos = committedEnd;
            }
            logBytes.resize(committedEnd);
        }

        // DR-BUG-6 divergence guard — stamp each served batch's partitionLeaderEpoch
        // from OUR epoch lineage (the epoch that owns each offset).  The field is
        // excluded from the CRC, so stamping in place needs no re-checksum.  The
        // deficient leader validates these epochs against its OWN accepted lineage
        // in applyReconcileResponse() and rejects any batch from an unshared epoch —
        // so a same-offset batch from a divergent epoch can never be adopted.
        {
            std::size_t pos = 0;
            while (pos < logBytes.size()) {
                std::size_t rem = logBytes.size() - pos;
                if (rem < static_cast<std::size_t>(
                        rune::protocol::batch::kRecordBatchOverhead)) break;
                rune::protocol::RecordBatchReader r(logBytes.data() + pos, rem);
                int32_t total = r.totalLength();
                if (total <= 0 || pos + static_cast<std::size_t>(total)
                                  > logBytes.size()) break;
                int32_t ownEp = epochCache_.epochForOffset(r.baseOffset());
                if (ownEp >= 0) {
                    rune::protocol::RecordBatchReader::stampLeaderEpoch(
                        logBytes.data() + pos, ownEp);
                }
                pos += static_cast<std::size_t>(total);
            }
        }
    }

    // Reply carrying the records and our LEO as the highWatermark hint (the
    // deficient leader uses byte content, not this HW, to advance its own).
    FetchResponseMsg::encode(
        encodeScratch_.data(), 0,
        epoch_,                          // our epoch (>= leader's; fences stale)
        cfg_.partitionId,
        leo,                             // survivor LEO
        /*logStartOffset=*/0,
        kErrNone,
        std::span<const uint8_t>(logBytes.data(), logBytes.size()),
        cfg_.topicId);
    auto resp = FetchResponseMsg::decode(encodeScratch_.data(), 0);
    transport_->sendReconcileResponse(req.followerId, resp);
}

// (Leader) Apply reconcile records pulled from an ahead survivor.
void PartitionReplica::applyReconcileResponse(const FetchResponseMsg& resp)
{
    if (resp.errorCode != kErrNone) { reconcileTarget_ = -1; return; }
    if (role_ != Role::Leader)      { reconcileTarget_ = -1; return; }

    // Stale-epoch fence: ignore a reconcile answer from an older epoch.
    if (resp.leaderEpoch < epoch_) { reconcileTarget_ = -1; return; }

    if (!resp.records.empty()) {
        const std::size_t kMagicOff = static_cast<std::size_t>(
            rune::protocol::batch::kMagicOffset);
        const uint8_t kMagic = static_cast<uint8_t>(
            rune::protocol::batch::kMagicValue);
        bool looksLikeRecordBatch =
            resp.records.size() > kMagicOff && resp.records[kMagicOff] == kMagic;

        if (looksLikeRecordBatch) {
            // Append only the strictly-new tail (baseOffset >= current LEO),
            // preserving contiguity — identical idempotent walk to applyReplicated.
            Offset currentLeo = log_->logEndOffset();
            std::size_t newStart = resp.records.size();
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
                if (batchBase < currentLeo) { pos += static_cast<std::size_t>(total); continue; }
                if (batchBase > currentLeo) break; // gap — stop

                // DR-BUG-6 divergence guard — per-batch epoch-lineage validation.
                //
                // A survivor stamps each served batch's partitionLeaderEpoch from
                // its own lineage (onReconcileRequest).  Before adopting a batch we
                // require its stamped epoch to be on OUR accepted lineage — an epoch
                // we ourselves hold in our epochCache.  This rejects a same-offset
                // batch pulled from an UNSHARED / divergent epoch (the two-survivor
                // contradiction): we never append it, and we stop the walk so no
                // later (also-suspect) batch is adopted either.  A batch with an
                // UNSET epoch (-1, e.g. legacy / raw-injected data with no lineage
                // metadata) is accepted for backward compatibility — the primary
                // epoch-lineage gate on the probe path already prevents pulling from
                // a divergent source in the first place; this is defence in depth.
                int32_t batchEp = r.partitionLeaderEpoch();
                if (batchEp >= 0 && !epochCache_.hasEpoch(batchEp)) {
                    break; // off-lineage batch — reject and stop.
                }

                if (newStart == resp.records.size()) newStart = pos;
                lastOff = batchBase + r.lastOffsetDelta();
                currentLeo = lastOff + 1;
                pos += static_cast<std::size_t>(total);
            }
            if (newStart < resp.records.size() && lastOff >= log_->logEndOffset()) {
                std::vector<uint8_t> raw(
                    resp.records.begin() + static_cast<std::ptrdiff_t>(newStart),
                    resp.records.end());
                // Byte-identity: the survivor stamped partitionLeaderEpoch on the
                // served copy for our validation above.  Restore it to the UNSET
                // sentinel (-1) before appending so a reconciled batch is
                // BYTE-IDENTICAL to the same batch obtained via normal replication
                // (which carries -1).  Otherwise a replica that reconciled a batch
                // and one that replicated it would hold cosmetically-different bytes
                // at the same offset — a (benign but real) log divergence.  The
                // field is CRC-excluded, so restoring it needs no re-checksum.
                std::size_t rp = 0;
                while (rp < raw.size()) {
                    std::size_t rem = raw.size() - rp;
                    if (rem < static_cast<std::size_t>(
                            rune::protocol::batch::kRecordBatchOverhead)) break;
                    rune::protocol::RecordBatchReader rr(raw.data() + rp, rem);
                    int32_t tl = rr.totalLength();
                    if (tl <= 0 || rp + static_cast<std::size_t>(tl) > raw.size()) break;
                    rune::protocol::RecordBatchReader::stampLeaderEpoch(
                        raw.data() + rp, -1);
                    rp += static_cast<std::size_t>(tl);
                }
                log_->append(raw, lastOff);
            }
        }
    }

    // DR-BUG-6: re-stamp our leader-epoch boundary at the reconciled frontier.
    //
    // becomeLeader() optimistically stamped epoch_ → oldLEO (our stale LEO at
    // election).  The records we just pulled are OLD-epoch committed records that
    // belong BELOW our new-epoch boundary, so that boundary must move up to our
    // new LEO — otherwise our LeaderEpochCache would claim the new epoch began
    // in the MIDDLE of recovered old-epoch data, and a subsequent
    // OffsetsForLeaderEpoch answer would tell an ISR survivor to truncate
    // committed records (re-introducing the DR-8 loss).
    //
    // We only wrote the new epoch's boundary; we have not appended any records
    // under it yet (LEO advanced solely from the pulled old-epoch records), so
    // moving the boundary up is safe and correct.
    {
        const Offset newLeo = log_->logEndOffset();
        const int32_t curEp = static_cast<int32_t>(epoch_);
        const int64_t curEpStart = epochCache_.startOffsetFor(curEp);
        if (curEpStart >= 0 && curEpStart < newLeo) {
            // Drop the premature boundary and re-assign at the true frontier.
            epochCache_.truncateFromEnd(curEpStart);
            epochCache_.assign(curEp, newLeo);
        }
    }

    // Reflect the pulled records into the leader's own LEO/HW machinery.
    leaderState_.setLeaderLogEndOffset(log_->logEndOffset());
    recomputeHw();

    // Reconcile round complete; clear the in-flight marker so a further gap (if
    // any) can trigger another pull.
    reconcileTarget_ = -1;

    // GR-10: push the freshly-reconciled records to ISR followers so the whole
    // cluster converges on the recovered committed prefix.
    if (cfg_.pushOnAppend) {
        pushToFollowers(0);
    }
}

// ---------------------------------------------------------------------------
// doWork
// ---------------------------------------------------------------------------

int PartitionReplica::doWork(int64_t nowNs)
{
    int work = 0;

    if (role_ == Role::Leader) {
        // DR-BUG-6: reset the follower-lag TIME clock on the first tick after
        // becoming leader (becomeLeader has no clock).  This MUST happen before
        // the first maybeShrinkIsr so followers are not evicted with a stale
        // lastFetchTimeNs of 0 (see becomeLeader / IsrManager::resetLagClock).
        if (leaderLagClockNeedsReset_) {
            isrManager_->resetLagClock(nowNs);
            leaderLagClockNeedsReset_ = false;
        }

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
        // DR-BUG-6: while an OffsetsForLeaderEpoch probe is pending we DEFER
        // normal fetching and (re)issue the epoch probe on cadence instead.  We
        // must not fetch under the new epoch until we have decided the correct
        // truncation point — otherwise the leader could push us data before we
        // reconcile our own tail.  The probe is idempotent; re-issuing it until
        // the answer arrives is safe (the leader may not have been promoted yet).
        if (epochProbePending_) {
            if (nowNs - lastFetchEmitNs_ >= cfg_.fetchIntervalNs) {
                lastFetchEmitNs_ = nowNs;

                // DR-BUG-6 liveness fallback (NON-DESTRUCTIVE).
                //
                // The pre-fix fallback truncated to the committed-HW floor after
                // kEpochProbeMaxAttempts unanswered probes.  That was the residual
                // DR-8 committed-loss: under packet loss / while the new leader is
                // still being promoted, the probe goes unanswered, the counter
                // climbs to the cap, and the survivor truncates to its STALE HW
                // view — dropping committed records the (deficient) new leader has
                // not yet reconciled (observed live: LEO 68 -> 67, floor 67).
                //
                // Correct behaviour: an unanswered probe is NOT evidence of
                // divergence, so it must NEVER trigger a truncation.  On reaching
                // the cap we (a) KEEP the full tail (no truncation — committed data
                // is never dropped on a timeout), (b) keep the probe ARMED so the
                // leader's eventual AUTHORITATIVE answer (kErrNone) still performs
                // the correct, divergence-safe truncation via
                // applyOffsetsForLeaderEpochResponse (this is the no-divergence
                // backstop — a genuinely divergent tail is truncated then), and
                // (c) ALSO emit a plain fetch for liveness so the follower makes
                // progress even if the leader is slow to answer the probe.  A plain
                // fetch from an ahead follower hits the leader's reconcile backstop
                // (fetchOffset > leaderLEO → serves nothing, keeps our tail), so it
                // is safe and cannot stamp divergent bytes.  We reset the attempt
                // counter so probing continues indefinitely rather than giving up.
                if (epochProbeAttempts_ >= kEpochProbeMaxAttempts) {
                    epochProbeAttempts_ = 0;   // keep probing; never give up destructively
                    FetchRequestMsg fr = nextFetch();
                    transport_->sendFetchRequest(fr);
                    ++work;
                    // fall through to re-issue the probe this cycle too.
                }

                ++epochProbeAttempts_;
                OffsetsForLeaderEpochReqMsg req{};
                req.currentEpoch   = epoch_;
                req.partition      = cfg_.partitionId;
                req.followerId     = cfg_.selfId;
                // Our latest leader-epoch (largest epoch we have records for).
                req.requestedEpoch = epochCache_.latestEpoch();
                req.followerLeo    = log_->logEndOffset();
                req.topicId        = cfg_.topicId;
                transport_->sendOffsetsForLeaderEpochRequest(leaderId_, req);
                ++work;
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
