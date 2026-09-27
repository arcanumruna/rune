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

#include <spdlog/spdlog.h>

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
    , diskLog_(log)
    , memLog_(nullptr)
    , memoryBacked_(false)
    , transport_(transport)
    , encodeScratch_(kScratchSize, uint8_t{0})
{
    assert(diskLog_   != nullptr && "log must not be null");
    assert(transport_ != nullptr && "transport must not be null");
    initReplica(std::move(replicas), std::move(isr));
}

PartitionReplica::PartitionReplica(PartitionConfig            cfg,
                                   Role                       role,
                                   Epoch                      epoch,
                                   std::vector<NodeId>        replicas,
                                   std::vector<NodeId>        isr,
                                   rune::storage::InMemoryLog* log,
                                   PartitionTransport*        transport)
    : cfg_(cfg)
    , role_(role)
    , epoch_(epoch)
    , leaderId_(-1)
    , diskLog_(nullptr)
    , memLog_(log)
    , memoryBacked_(true)
    , transport_(transport)
    , encodeScratch_(kScratchSize, uint8_t{0})
{
    assert(memLog_    != nullptr && "log must not be null");
    assert(transport_ != nullptr && "transport must not be null");
    initReplica(std::move(replicas), std::move(isr));
}

// Shared construction logic for both backend overloads (de-virtualized seam).
void PartitionReplica::initReplica(std::vector<NodeId> replicas,
                                   std::vector<NodeId> isr)
{
    // Retain the full replica set (RF) for the lifetime of this replica so a
    // follower-first replica can rebuild a correct IsrManager on a later
    // becomeLeader() instead of collapsing replicas == isr (see header note).
    configuredReplicas_ = replicas;

    // Task 9: restore the LeaderEpochCache from the durable checkpoint BEFORE any
    // assign() calls below.  Disk-backed only — memory-backed partitions have no
    // checkpoint file (and don't survive a full cluster restart by design).
    // A missing/corrupt checkpoint is silently tolerated: assign() below seeds the
    // epoch cache from scratch, which is the correct fallback for a fresh log or a
    // corrupt checkpoint.
    if (!memoryBacked_ && diskLog_ != nullptr) {
        (void)epochCache_.restoreFromFile(diskLog_->dir() + "/leader-epoch-checkpoint");
    }

    if (role_ == Role::Leader) {
        isrManagerOwned_ = std::make_unique<IsrManager>(replicas, cfg_.selfId, cfg_.replicaLagMaxMs);
        isrManagerOwned_->setIsr(isr);
        isrManager_ = isrManagerOwned_.get();

        // Durability: set the quorum rule and effective minIsr before seeding ISR/LEO.
        leaderState_.setDurabilityMode(cfg_.durability);
        effectiveMinIsr_ = (cfg_.durability == DurabilityMode::AllInSync)
            ? static_cast<int>(replicas.size())
            : cfg_.minIsr;

        leaderState_.setLeaderId(cfg_.selfId);
        leaderState_.setIsr(isr);
        leaderState_.setLeaderLogEndOffset(logEndOffset_());

        // DR-BUG-3: seed the min-ISR-gated committed floor from the initial HW
        // (see becomeLeader for the rationale — prevents a gated HW of 0 from
        // hiding already-committed data while the ISR is below minIsr).
        lastHealthyHw_ = leaderState_.highWatermark();

        if (epoch_ >= 0) {
            epochCache_.assign(static_cast<int32_t>(epoch_),
                               logEndOffset_());
        }
    } else {
        // DR-BUG-6: seed the follower's epoch cache so it knows its latest
        // leader-epoch (needed to build the OffsetsForLeaderEpoch request).
        // The initial epoch's records (if any pre-seeded) begin at offset 0.
        if (epoch_ >= 0) {
            epochCache_.assign(static_cast<int32_t>(epoch_), /*startOffset=*/0);
        }
    }

    // Seed the cleaner-visible snapshots from the initial state.
    publishCleanerState();
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
    // Do NOT use logEndOffset_() (the LEO): a node promoted with an
    // uncommitted tail (localLeo > committedHw) would mark that tail as committed,
    // causing uncommitted/divergent records to be readable after failover.
    Offset committedHw = highWatermark();

    epoch_ = newEpoch;
    role_  = Role::Leader;

    // Use the full assigned replica set (RF), NOT the (possibly leader-only) ISR,
    // as the IsrManager's replica set.  Collapsing replicas == isr here would make
    // maybeExpandIsr() unable to ever re-admit the other replicas, pinning the ISR
    // at its election size forever (→ MinIsrViolation on acks=all when a partition
    // is elected with a leader-only ISR).  configuredReplicas_ is seeded at
    // construction and survives the follower→leader transition.
    std::vector<NodeId> replicas =
        !configuredReplicas_.empty() ? configuredReplicas_ : isr;
    if (isrManagerOwned_) {
        // Preserve replica knowledge; just update ISR.
        isrManagerOwned_->setIsr(isr);
    } else {
        isrManagerOwned_ = std::make_unique<IsrManager>(replicas, cfg_.selfId, cfg_.replicaLagMaxMs);
        isrManagerOwned_->setIsr(isr);
    }
    isrManager_ = isrManagerOwned_.get();

    spdlog::info(
        "PartitionReplica[node={}] becomeLeader topic={} part={} epoch={} "
        "isr.size={} replicaSet.size={} minIsr={}",
        cfg_.selfId, cfg_.topicId, static_cast<int>(cfg_.partitionId),
        static_cast<long long>(newEpoch), isr.size(), replicas.size(),
        cfg_.minIsr);

    leaderState_.setLeaderId(cfg_.selfId);

    // Durability quorum: set the per-topic rule FIRST, before seeding the floor
    // and ISR/LEO.  setDurabilityMode resets prevHw_ to 0 and recomputes (which
    // yields 0 since ISR/LEO are not yet set), so it must precede
    // setHighWatermarkFloor to avoid clobbering the committed-HW floor.
    leaderState_.setDurabilityMode(cfg_.durability);

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
    leaderState_.setLeaderLogEndOffset(logEndOffset_());

    // Effective minIsr for the acksAll produce gate:
    //   Majority  → cfg_.minIsr (user-configured floor)
    //   AllInSync → RF (= replicas.size()): every replica must be in the ISR.
    effectiveMinIsr_ = (cfg_.durability == DurabilityMode::AllInSync)
        ? static_cast<int>(replicas.size())
        : cfg_.minIsr;

    epochCache_.assign(static_cast<int32_t>(epoch_), logEndOffset_());

    // Task 9: persist the updated epoch history so a full restart can rebuild it.
    // Disk-backed only — memory-backed partitions have no durable log (and their
    // epoch cache is rebuilt from scratch on restart by design).
    if (!memoryBacked_ && diskLog_ != nullptr) {
        epochCache_.checkpointToFile(diskLog_->dir() + "/leader-epoch-checkpoint");
    }

    lastIsrCheckNs_ = 0;

    // DR-BUG-6 LEADER-COMPLETENESS GATE — arm on EVERY becomeLeader (churn-robust).
    //
    // Hold the served/advertised committed HW at the pre-failover committed floor
    // (committedHw, captured above under the OLD role's view) until we have proven
    // we hold the majority committed frontier.  survivorFrontier_ is reset so a
    // prior term's stale reports never satisfy this term's completeness quorum;
    // we seed our OWN frontier (our LEO) — self is always a live ISR member and
    // always "reports in".  leaderComplete_ starts false; maybeCompleteLeadership()
    // (called from the first doWork / first survivor report) flips it to true the
    // moment a majority of the ISR has reported in AND our LEO covers the on-lineage
    // majority frontier with no reconcile in flight.  For a clean failover with no
    // ahead survivor (or a single-member ISR) that condition holds immediately, so
    // the gate never stalls normal operation.
    completenessFloor_ = committedHw;
    leaderComplete_    = false;
    survivorFrontier_.clear();
    survivorFrontier_[cfg_.selfId] = logEndOffset_();
    reconcileTarget_   = -1;   // drop any pull latched under a prior term
    reconcileIssuedNs_ = -1;

    // DR-BUG-6: arm the follower-lag TIME-clock reset for the first leader
    // doWork(nowNs).  Until it fires, no ISR check runs (lastIsrCheckNs_==0 makes
    // the first doWork the ISR-check tick, and we reset the clock there BEFORE the
    // shrink), so followers get a full lag window to probe/fetch instead of being
    // evicted at t=election+10ms with lastFetchTimeNs=0.  This is what lets an
    // up-to-date survivor stay in the ISR while it runs its OffsetsForLeaderEpoch
    // probe, so the reverse-reconcile engages instead of an authoritative truncate.
    leaderLagClockNeedsReset_ = true;

    // DR-BUG-6 completeness gate: evaluate immediately.  For a lone-member ISR
    // (majority == 1, self already reported) or any ISR whose only member ahead is
    // ourselves, all completion conditions hold NOW, so leadership completes here
    // and normal HW serving is never gated — the gate only ever HOLDS when there is
    // a genuine live survivor we still need to hear from / reconcile.
    maybeCompleteLeadership();

    // Publish the new role + served HW so the cleaner observes this node as
    // Leader (and the gated HW) without reading the live leader/ISR state.
    publishCleanerState();
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
    lastKnownLeaderHw_ = 0;   // new leader — relearn its HW before "caught up" applies
    heardFromLeader_ = false;
    lastResponseEmpty_ = false;
    lastFetchEmitNs_ = 0;
    reconcileTarget_ = -1;

    // Publish the demotion so the cleaner stops treating this node as Leader
    // (cleanerRole() flips to Follower) — no live-state read on the cleaner.
    publishCleanerState();
}

// ---------------------------------------------------------------------------
// Leader: append
// ---------------------------------------------------------------------------

Offset PartitionReplica::append(std::span<const uint8_t> records, bool acksAll)
{
    assert(role_ == Role::Leader && "append() called on follower");

    if (!isrManager_->canProduce(effectiveMinIsr_, acksAll)) {
        return -1; // gated by min-ISR
    }

    Offset baseOffset = logEndOffset_();

    // Build a proper RecordBatch so the Log segment scanner works correctly.
    auto batch = makeBatch(baseOffset, records);
    // lastOffset = baseOffset + 0 (one record per batch = lastOffsetDelta=0).
    onLog([&](auto& L){ L.append(batch, baseOffset); });

    // Leader LEO is now baseOffset + 1.
    leaderState_.setLeaderLogEndOffset(logEndOffset_());
    // Leader self-advances HW (leader always in ISR, so majority includes leader).
    recomputeHw();

    // Mirror the advanced HW into the cleaner-visible atomic (release-store).
    publishCleanerState();

    // GR-10: push newly-appended bytes to ISR followers immediately (best-effort).
    // Collapsed to ~RTT from the follower-poll floor.
    if (cfg_.pushOnAppend) {
        pushToFollowers(baseOffset);
    }

    return baseOffset;
}

// ---------------------------------------------------------------------------
// Leader: append with an explicit record key (CP-1 compaction support)
// ---------------------------------------------------------------------------

Offset PartitionReplica::appendKeyed(std::span<const uint8_t> key,
                                      std::span<const uint8_t> value,
                                      bool acksAll)
{
    assert(role_ == Role::Leader && "appendKeyed() called on follower");

    if (!isrManager_->canProduce(effectiveMinIsr_, acksAll)) {
        return -1; // gated by min-ISR
    }

    Offset baseOffset = logEndOffset_();

    // Build a one-record batch with the real key and value.
    rune::protocol::RecordBatchBuilder builder(baseOffset, /*baseTimestamp=*/0);
    builder.append(
        std::string_view(reinterpret_cast<const char*>(key.data()), key.size()),
        std::string_view(reinterpret_cast<const char*>(value.data()), value.size()),
        /*timestamp=*/0);
    auto batch = builder.build();

    onLog([&](auto& L){ L.append(batch, baseOffset); });

    leaderState_.setLeaderLogEndOffset(logEndOffset_());
    recomputeHw();

    // Mirror the advanced HW into the cleaner-visible atomic (release-store).
    publishCleanerState();

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
    leaderState_.setLeaderLogEndOffset(logEndOffset_());

    // setLeaderLogEndOffset() recomputes HW eagerly — mirror it to the cleaner.
    publishCleanerState();

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
        lastKnownLeaderHw_ = 0;   // relearn the new leader's HW before "caught up" applies
        heardFromLeader_ = false;
        lastResponseEmpty_ = false;
        lastFetchEmitNs_ = 0;

        // DR-9 m1: arm the KIP-101 epoch probe exactly as becomeFollower() does
        // (lines 213-216).  Self-demotion bumps epoch_ directly, so applyReplicated's
        // epoch-upgrade truncation never fires; without arming the probe here, any
        // uncommitted tail this stale leader appended above committedHw lingers
        // unpruned.  The floor (committedHw) guarantees the probe never truncates
        // committed data — it only reconciles/prunes the divergent uncommitted tail.
        epochProbePending_  = true;
        epochProbeEpoch_    = req.leaderEpoch; // epoch we are transitioning INTO
        epochProbeFloor_    = committedHw;      // never truncate below this
        epochProbeAttempts_ = 0;

        // Self-demotion: publish the role change so the cleaner stops treating
        // this node as Leader (no live-state read on the cleaner thread).
        publishCleanerState();

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
    if (req.fetchOffset > logEndOffset_()) {
        // DR-BUG-6 completeness gate: an ahead follower reached a plain fetch whose
        // tail lineage we cannot validate here — clamp its frontier to our LEO so
        // it never raises the target (the epoch-probe path does the lineage-checked
        // recording that legitimately raises it).  We still (re-)evaluate so a
        // divergent ahead follower does not block completion forever.
        if (!leaderComplete_) {
            survivorFrontier_[req.followerId] = logEndOffset_();
            maybeCompleteLeadership();
        }
        if (reconcileTarget_ == req.followerId) {
            FetchRequestMsg pull{};
            pull.leaderEpoch = epoch_;
            pull.partition   = cfg_.partitionId;
            pull.followerId  = cfg_.selfId;               // WE are the puller
            pull.fetchOffset = logEndOffset_();      // from our LEO
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

    // DR-BUG-6 completeness gate: record this survivor's frontier.  A plain fetch
    // cannot validate lineage (req carries the fetch epoch, not the tail epoch), so
    // it can NEVER raise the frontier above our own LEO — clamp it.  Once reconcile
    // has raised our LEO, subsequent fetches naturally record the higher value.
    if (!leaderComplete_) {
        survivorFrontier_[req.followerId] =
            std::min<Offset>(req.fetchOffset, logEndOffset_());
        maybeCompleteLeadership();
    }

    // Read records from the log.
    std::vector<uint8_t> logBytes;
    if (req.maxBytes > 0 && req.fetchOffset < logEndOffset_()) {
        logBytes = onLog([&](auto& L){ return L.read(req.fetchOffset, req.maxBytes); });

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
    auto raw = onLog([&](auto& L){ return L.read(fetchOffset, maxBytes); });
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

    // Adaptive fetch cadence bookkeeping: we have now heard from the current (or a
    // newer) leader.  Record its advertised HW BEFORE any gap/truncate handling, so a
    // follower that has a gap (or is otherwise behind) still learns the leader is
    // ahead and keeps fetching FAST to backfill.  doWork uses this to pick the fetch
    // cadence: fast while behind, slow keepalive once caught up (no idle spin).
    heardFromLeader_ = true;
    if (resp.highWatermark > lastKnownLeaderHw_) lastKnownLeaderHw_ = resp.highWatermark;
    // Empty response ⟺ caught up (leader had nothing beyond our fetch offset).
    lastResponseEmpty_ = resp.records.empty();

    // Capture pre-apply state to detect if anything changed (for fast-ack guard).
    const Offset preLeo = logEndOffset_();
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
        epochCache_.assign(static_cast<int32_t>(epoch_), logEndOffset_());
        // Task 9: persist the follower's updated epoch history (disk-backed only).
        if (!memoryBacked_ && diskLog_ != nullptr) {
            epochCache_.checkpointToFile(diskLog_->dir() + "/leader-epoch-checkpoint");
        }
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

            Offset currentLeo = logEndOffset_();
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

            if (newStart < resp.records.size() && lastOff >= logEndOffset_()) {
                // Append only the strictly-new suffix.
                std::vector<uint8_t> raw(resp.records.begin() + static_cast<std::ptrdiff_t>(newStart),
                                          resp.records.end());
                onLog([&](auto& L){ L.append(raw, lastOff); });
            }
        } else {
            // Fallback: raw user bytes (e.g. injected by unit tests) — wrap in
            // a RecordBatch so the log segment scanner can parse them.
            Offset baseOffset = logEndOffset_();
            auto batch = makeBatch(baseOffset, resp.records);
            onLog([&](auto& L){ L.append(batch, baseOffset); });
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
        // Task 9: persist follower epoch history on new-epoch adoption (disk-backed only).
        if (!memoryBacked_ && diskLog_ != nullptr) {
            epochCache_.checkpointToFile(diskLog_->dir() + "/leader-epoch-checkpoint");
        }
    }

    // Advance local HW = min(localLeo, leader's HW).
    Offset localLeo = logEndOffset_();
    Offset newHw    = std::min(localLeo, resp.highWatermark);
    if (newHw > followerHw_) {
        followerHw_ = newHw;
    }
    // (lastKnownLeaderHw_ / heardFromLeader_ were recorded at the top of this
    //  function, before any gap/truncate handling.)

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
    const Offset postLeo = logEndOffset_();
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
    req.fetchOffset = logEndOffset_();
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
    const Offset leaderLeo = logEndOffset_();

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
        const Offset trackedLeo = std::min<Offset>(req.followerLeo, logEndOffset_());
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

    // DR-BUG-6 completeness gate: the probe is the ONLY lineage-validated report.
    // An ISR survivor whose ahead tail is a genuine on-lineage continuation
    // contributes its FULL LEO to the frontier (we must reconcile up to it before
    // we may advertise/serve).  Any other case (divergent tail, non-ISR, or not
    // ahead) is clamped to our own LEO so it can never raise the frontier.
    if (!leaderComplete_) {
        const Offset onLineage =
            (followerInIsr && sharedCont)
                ? req.followerLeo
                : std::min<Offset>(req.followerLeo, logEndOffset_());
        survivorFrontier_[req.followerId] = onLineage;
        maybeCompleteLeadership();
    }

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
            pull.fetchOffset = logEndOffset_();      // from our LEO
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
        static_cast<int32_t>(req.requestedEpoch), logEndOffset_());

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

    const Offset currentLeo     = logEndOffset_();
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
    Offset leo = logEndOffset_();
    if (req.maxBytes > 0 && req.fetchOffset < leo) {
        logBytes = onLog([&](auto& L){ return L.read(req.fetchOffset, req.maxBytes); });

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
            Offset currentLeo = logEndOffset_();
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
            if (newStart < resp.records.size() && lastOff >= logEndOffset_()) {
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
                onLog([&](auto& L){ L.append(raw, lastOff); });
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
        const Offset newLeo = logEndOffset_();
        const int32_t curEp = static_cast<int32_t>(epoch_);
        const int64_t curEpStart = epochCache_.startOffsetFor(curEp);
        if (curEpStart >= 0 && curEpStart < newLeo) {
            // Drop the premature boundary and re-assign at the true frontier.
            epochCache_.truncateFromEnd(curEpStart);
            epochCache_.assign(curEp, newLeo);
            // Task 9: persist the corrected epoch boundary (disk-backed only).
            if (!memoryBacked_ && diskLog_ != nullptr) {
                epochCache_.checkpointToFile(diskLog_->dir() + "/leader-epoch-checkpoint");
            }
        }
    }

    // Reflect the pulled records into the leader's own LEO/HW machinery.
    leaderState_.setLeaderLogEndOffset(logEndOffset_());
    recomputeHw();

    // Reconcile round complete; clear the in-flight marker so a further gap (if
    // any) can trigger another pull.
    reconcileTarget_ = -1;

    // DR-BUG-6 completeness gate: our LEO advanced by the pulled on-lineage tail.
    // Refresh our own frontier entry and re-evaluate — once our LEO covers the
    // majority frontier with no pull outstanding, leadership completes and the
    // held HW is released to cover the recovered records.
    if (!leaderComplete_) {
        survivorFrontier_[cfg_.selfId] = logEndOffset_();
        maybeCompleteLeadership();
    }

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

        // Task #13 (read-path): advance the served/gateway HW as the local log
        // grows.  recomputeHw() → setLeaderLogEndOffset(logEndOffset_()) recomputes
        // HW = min(ISR LEOs): for a single-node ISR this tracks LEO; for a
        // multi-node ISR it stays correctly bounded by follower fetch offsets.
        // Previously HW was recomputed ONLY on an ISR change, so a stable-ISR
        // leader froze the served HW at its becomeLeader value — subscribers
        // stalled at a stale read bound even though produce had committed far
        // beyond it, and publishCleanerState() kept re-publishing the stale
        // snapshot.  Counted as work only when the HW actually moves, so a truly
        // idle leader still parks.
        {
            const Offset servedBefore = leaderState_.highWatermark();
            recomputeHw();
            if (leaderState_.highWatermark() != servedBefore) ++work;
        }

        if (nowNs - lastIsrCheckNs_ >= cfg_.isrCheckIntervalNs) {
            lastIsrCheckNs_ = nowNs;
            // NOTE: the bare ISR-check TICK is NOT work — counting it made the
            // idle-strategy see work>0 every 10ms and never park (busy-spin on an
            // idle/no-follower leader).  Only count work when the check ACTUALLY
            // changes ISR state or emits a reconcile (below).

            isrManager_->maybeShrinkIsr(nowNs,
                                         logEndOffset_(),
                                         cfg_.replicaLagMaxMs,
                                         cfg_.replicaLagMaxOffset);
            if (isrManager_->isrChanged()) {
                syncIsrToLeaderState();
                recomputeHw();
                ++work;
            }

            isrManager_->maybeExpandIsr(nowNs, leaderState_.highWatermark());
            if (isrManager_->isrChanged()) {
                syncIsrToLeaderState();
                ++work;
            }

            // DR-BUG-6 completeness gate: re-evaluate each ISR check.  This lets a
            // lone-member ISR complete promptly and lets completion proceed once a
            // genuinely lagging/dead survivor has been shrunk out of the ISR (so it
            // no longer counts toward the quorum/frontier).  No busy-spin — bounded
            // to the ISR-check cadence.
            maybeCompleteLeadership();

            // DR-BUG-6 CHURN-ROBUSTNESS: while still INCOMPLETE, ACTIVELY (re)drive
            // the reverse-reconcile toward a known on-lineage-ahead survivor instead
            // of waiting passively for that survivor's next probe round-trip.  Under
            // 10% loss + double-election churn a single probe→reconcile→response
            // chain is easily dropped inside the window; leader-driven, cadence-based
            // re-issue makes catch-up self-healing.  We only pull from a survivor we
            // have ALREADY validated as on-lineage (survivorFrontier_ holds its FULL
            // ahead LEO — the probe path only records the full LEO for a shared
            // continuation; a divergent tail is clamped to our LEO and so never
            // appears ahead here).  Idempotent: applyReconcileResponse appends only
            // the strictly-new tail.  Bounded to the ISR-check cadence; no spin.
            if (!leaderComplete_) {
                // Self-heal a stuck pull: if the in-flight reconcile has gone
                // unanswered past the stuck window (request/response dropped), clear
                // the latch so we can re-issue below.
                if (reconcileTarget_ >= 0 && reconcileIssuedNs_ >= 0
                    && (nowNs - reconcileIssuedNs_) >= kReconcileStuckNs) {
                    reconcileTarget_   = -1;
                    reconcileIssuedNs_ = -1;
                }

                if (reconcileTarget_ < 0) {
                    const Offset myLeo = logEndOffset_();
                    NodeId aheadPeer = -1;
                    Offset aheadLeo  = myLeo;
                    for (NodeId id : isrManager_->isr()) {
                        if (id == cfg_.selfId) continue;
                        auto it = survivorFrontier_.find(id);
                        if (it != survivorFrontier_.end() && it->second > aheadLeo) {
                            aheadLeo  = it->second;
                            aheadPeer = id;
                        }
                    }
                    if (aheadPeer >= 0) {
                        reconcileTarget_   = aheadPeer;
                        reconcileIssuedNs_ = nowNs;
                        FetchRequestMsg pull{};
                        pull.leaderEpoch = epoch_;
                        pull.partition   = cfg_.partitionId;
                        pull.followerId  = cfg_.selfId;          // WE are the puller
                        pull.fetchOffset = myLeo;                // from our LEO
                        pull.maxBytes    = cfg_.maxFetchBytes;
                        pull.topicId     = cfg_.topicId;
                        transport_->sendReconcileRequest(aheadPeer, pull);
                        ++work;
                    }
                }
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
                req.followerLeo    = logEndOffset_();
                req.topicId        = cfg_.topicId;
                transport_->sendOffsetsForLeaderEpochRequest(leaderId_, req);
                ++work;
            }
        } else {
            // Follower: emit a fetch on cadence.
            //
            // CAUGHT UP (our LEO has reached the highest leader HW we have heard):
            //   the periodic fetch is only an ISR-liveness keepalive + a lost-push
            //   backstop — new data arrives via GR-10 push + fast-ack, not here.  Use
            //   the SLOW keepalive cadence and DO NOT count it as work, so the idle
            //   backoff parks instead of spinning a 5ms fetch/empty-response ping-pong
            //   (which otherwise pinned both control threads on an idle cluster).
            // BEHIND (or leader position not yet known): fetch on the FAST cadence and
            //   count it as work — this is real catch-up.
            // Caught up ⟺ we have heard the leader's position AND our LEO has reached
            // the highest leader HW we heard.  A follower that has not yet heard from
            // the leader (heardFromLeader_ == false) is NOT treated as caught up even
            // at offset 0, so it bootstraps/backfills on the fast cadence.
            const bool caughtUp = heardFromLeader_ && lastResponseEmpty_;
            // Caught-up keepalive: slow, but ALWAYS several fetches per replica-lag
            // window so the follower is never evicted from the ISR and ISR
            // re-expansion stays responsive (must stay well under replicaLagMaxMs).
            // Scales with the lag window: a 20ms window keeps the 5ms fast cadence; a
            // 1s window gives 250ms; a 10s production window gives 2.5s idle keepalive.
            // Never faster than the fast cadence.
            const int64_t keepaliveNs = std::max<int64_t>(
                cfg_.fetchIntervalNs,
                (cfg_.replicaLagMaxMs * 1'000'000LL) / 4);
            const int64_t interval = caughtUp ? keepaliveNs : cfg_.fetchIntervalNs;
            if (nowNs - lastFetchEmitNs_ >= interval) {
                lastFetchEmitNs_ = nowNs;
                FetchRequestMsg req = nextFetch();
                transport_->sendFetchRequest(req);
                if (!caughtUp) ++work;
            }
        }
    }

    // Publish the current served HW + role for the cleaner (release-store).
    // This is the steady-state publication site: it covers every HW/ISR/
    // completeness/role change made on this duty tick (ISR shrink/expand,
    // completeness gate, fetch-driven HW advances) so the cleaner's acquire-load
    // converges to the current committed HW without ever reading the live state.
    publishCleanerState();

    return work;
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

// Pure, side-effect-free computation of the served committed HW.  Does NOT
// touch lastHealthyHw_ (the mutable floor) — that write is done ONLY by
// highWatermark() below, on the duty thread.  Reads leaderState_/isrManager_
// non-atomically → duty-thread only.
Offset PartitionReplica::computeServedHw() const noexcept
{
    if (role_ != Role::Leader) {
        return followerHw_;
    }

    // Raw majority-of-ISR HW from the leader state (spec §4).
    const Offset rawHw = leaderState_.highWatermark();

    // DR-BUG-6 LEADER-COMPLETENESS GATE.  While incomplete, hold the served HW
    // at the pre-failover floor (see the field comment / becomeLeader).
    if (!leaderComplete_) {
        return completenessFloor_;
    }

    // DR-BUG-3: min-ISR COMMIT gate on the exposed HW.  While the ISR is BELOW
    // effectiveMinIsr_ the effective committed HW must not advance past the last offset
    // committed while the ISR was healthy (lastHealthyHw_).
    // effectiveMinIsr_ == cfg_.minIsr (Majority) or RF (AllInSync).
    const int minIsr  = (effectiveMinIsr_ >= 1) ? effectiveMinIsr_ : 1;
    const int isrSize = isrManager_ ? static_cast<int>(isrManager_->isr().size()) : 0;

    if (isrSize >= minIsr) {
        // Healthy ISR: rawHw is a legitimate majority-committed offset.
        // (The floor advance to lastHealthyHw_ happens in highWatermark().)
        return (rawHw > lastHealthyHw_) ? rawHw : lastHealthyHw_;
    }

    // ISR below minIsr: clamp down to the frozen healthy value.
    return (rawHw < lastHealthyHw_) ? rawHw : lastHealthyHw_;
}

Offset PartitionReplica::highWatermark() const noexcept
{
    const Offset served = computeServedHw();

    // DR-BUG-3: advance the frozen floor while the ISR is healthy.  This is the
    // ONLY write to lastHealthyHw_; it is a duty-thread-only side effect.  The
    // cleaner NEVER calls highWatermark() (it uses cleanerHighWatermark()), so
    // this write never races with the cleaner thread.
    if (role_ == Role::Leader && leaderComplete_) {
        const int minIsr  = (effectiveMinIsr_ >= 1) ? effectiveMinIsr_ : 1;
        const int isrSize = isrManager_ ? static_cast<int>(isrManager_->isr().size()) : 0;
        if (isrSize >= minIsr) {
            const Offset rawHw = leaderState_.highWatermark();
            if (rawHw > lastHealthyHw_) lastHealthyHw_ = rawHw;
        }
    }

    return served;
}

// Publish the served HW + role into the cleaner-visible atomics.  DUTY-THREAD
// ONLY.  Uses computeServedHw() (no lastHealthyHw_ write, no floor mutation) so
// publication itself is a pure read of already-duty-owned state; the release
// store makes it visible to the cleaner's acquire load.
void PartitionReplica::publishCleanerState() noexcept
{
    publishedHwForCleaner_.store(computeServedHw(), std::memory_order_release);
    publishedRoleForCleaner_.store(static_cast<int>(role_), std::memory_order_release);
    // Task #13: publish current ISR size for the gateway's drainPendingAcks()
    // ISR-health check — closes the live isr().size() read race (Task #13 acks race).
    // Leader: publish actual ISR size; Follower: publish 0 (not applicable).
    const int isrSize = (role_ == Role::Leader && isrManager_)
        ? static_cast<int>(isrManager_->isr().size())
        : 0;
    publishedIsrSizeForGateway_.store(isrSize, std::memory_order_release);
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

// ---------------------------------------------------------------------------
// DR-BUG-6 LEADER-COMPLETENESS GATE
// ---------------------------------------------------------------------------

// (Leader) The highest ON-LINEAGE offset any ISR member holds — the frontier the
// new leader must reconcile up to before it may advertise/serve.
//
// survivorFrontier_ already holds ON-LINEAGE-CLAMPED values (recorded at the probe
// / fetch / reconcile sites): an on-lineage ahead survivor contributes its FULL
// LEO (a genuine committed continuation of our still-open shared epoch we must
// recover), a divergent / lineage-unverifiable ahead tail is clamped to our own
// LEO (it can NEVER raise the frontier — no-divergence), and self contributes our
// own LEO.  The target is the MAX over the ISR: the highest committed offset that
// exists on our lineage across live survivors.  We are complete once our own LEO
// reaches it (the reverse-reconcile pulls the gap in), at which point a MAJORITY
// (self + the ahead survivor we pulled from) genuinely holds it.
Offset PartitionReplica::majorityCommittedFrontier() const
{
    Offset target = logEndOffset_();   // self is always a live ISR member
    if (!isrManager_) return target;
    for (NodeId id : isrManager_->isr()) {
        auto it = survivorFrontier_.find(id);
        if (it != survivorFrontier_.end() && it->second > target) {
            target = it->second;
        }
    }
    return target;
}

// (Leader) Re-evaluate the completeness gate.
//
// Completion requires ALL of:
//   (1) QUORUM — we have HEARD from a MAJORITY of the ISR (self always counts).
//       This prevents completing before an ahead survivor has even had a chance
//       to report its frontier (which would let us serve a regressed view).
//   (2) FRONTIER COVERED — our own LEO has reached the on-lineage majority
//       frontier: there is no on-lineage survivor still holding committed records
//       we have not pulled.  (majorityCommittedFrontier() folds the reconcile
//       target in; a divergent tail is clamped out, so it never blocks us.)
//   (3) NO RECONCILE IN FLIGHT — reconcileTarget_ < 0, so a pull we started is not
//       still outstanding (its records may not be applied yet).
//
// When all hold we release the gate: served HW resumes at the normal min-ISR-gated
// majority HW (>= completenessFloor_, monotonic).  Bounded, single-writer, no spin
// — this is called from the sites that change our knowledge (fetch, probe,
// reconcile-apply, ISR check).  For a clean failover with no ahead survivor (or a
// lone-member ISR) all three hold on the first evaluation, so the gate is a no-op
// in the steady state.
void PartitionReplica::maybeCompleteLeadership()
{
    if (leaderComplete_) return;
    if (role_ != Role::Leader || !isrManager_) return;

    const int isrSize  = static_cast<int>(isrManager_->isr().size());
    if (isrSize <= 0) return;
    const int majority = isrSize / 2 + 1;

    // (1) Quorum of the ISR has reported in.
    int reported = 0;
    for (NodeId id : isrManager_->isr()) {
        if (survivorFrontier_.find(id) != survivorFrontier_.end()) ++reported;
    }
    if (reported < majority) return;

    // (3) No reconcile pull outstanding.
    if (reconcileTarget_ >= 0) return;

    // (2) Our LEO covers the on-lineage majority frontier.
    if (logEndOffset_() < majorityCommittedFrontier()) return;

    // Complete: release the served HW.  The floor was the last majority-committed
    // offset; the raw HW is now >= it (we hold the frontier) so serving resumes
    // without any regression.
    leaderComplete_ = true;
}

void PartitionReplica::recomputeHw()
{
    // S1/C4 fix: setLeaderLogEndOffset now eagerly recomputes HW internally,
    // so a single call is sufficient.  We no longer call
    // updateFollowerFetchOffset(selfId, ...) — that was an incorrect hack that
    // polluted the follower LEO map with the leader's own entry.
    leaderState_.setLeaderLogEndOffset(logEndOffset_());
}

void PartitionReplica::syncIsrToLeaderState()
{
    leaderState_.setIsr(isrManager_->isr());
}

void PartitionReplica::truncateLog(Offset truncateOffset)
{
    Offset currentLeo = logEndOffset_();
    if (truncateOffset >= currentLeo) return;

    // A10 fix: prune the epoch cache in step with the log truncation.
    //
    // After the A5 fix in libs/storage, LeaderEpochCache::assign() silently
    // drops any assign whose startOffset is below the current latest entry.
    // If the epoch cache retains an entry whose startOffset is ABOVE the
    // post-truncation log-end-offset, a subsequent higher-epoch assign
    // (applyReplicated: epochCache_.assign(epoch_, logEndOffset_())) is
    // silently dropped — leaving the cache in an inconsistent state where
    // truncationOffsetForEpochChange() can return a too-high boundary.
    //
    // Fix: call truncateFromEnd(truncateOffset) BEFORE truncating the log so
    // all epoch entries whose startOffset >= truncateOffset are pruned; the
    // cache then reflects only epochs that start within the surviving log.
    epochCache_.truncateFromEnd(truncateOffset);

    // Task 9: persist the pruned epoch history (disk-backed only).
    if (!memoryBacked_ && diskLog_ != nullptr) {
        epochCache_.checkpointToFile(diskLog_->dir() + "/leader-epoch-checkpoint");
    }

    onLog([&](auto& L){ L.truncateTo(truncateOffset); });

    // Task 11: PSM re-rebuild on low truncation.
    //
    // The PSM rebuild (rebuildFromLog) is HW-bounded by design: the dedup
    // window only contains state for offsets in [0, rebuildHw_).  This is
    // safe UNLESS a KIP-101 divergence truncation later lands AT OR BELOW
    // rebuildHw_: after the truncation the log no longer contains those
    // offsets, so the dedup window holds stale state — it would either:
    //   • accept a genuine producer retry as a duplicate (because the
    //     window saw seq N at offset X that no longer exists), or
    //   • reject a valid in-order sequence (because lastSequence is above
    //     the surviving log end and the next batch looks like a gap).
    //
    // Fix: when truncateOffset <= psm_.rebuildHighWatermark(), re-run
    // rebuildFromLog atomically with the log truncation (the log has already
    // been truncated above, so the scan naturally stops at truncateOffset).
    // This is called on the single-writer duty-thread and is inseparable from
    // the truncation, so there is no window of observable inconsistency.
    //
    // When truncateOffset > rebuildHw_ the PSM window only covers offsets
    // that all survive the truncation; no stale state, no re-rebuild needed.
    if (psm_.rebuildHighWatermark() >= 0
        && truncateOffset <= psm_.rebuildHighWatermark()) {
        onLog([&](auto& L) {
            psm_.rebuildFromLog(cfg_.topicId, L, truncateOffset, cfg_.partitionId);
        });
    }

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

    // Single-writer guard: if the ISR contains only self (no followers), there
    // is nothing to push.  Advance lastPushedOffset_ and return WITHOUT touching
    // the log — log_->read() races with the duty-cycle thread's drainCleanerSwaps
    // (segments_ mutation) and any concurrent fetch from a test or internal caller.
    {
        bool hasFollower = false;
        for (NodeId id : isrList) {
            if (id != cfg_.selfId) { hasFollower = true; break; }
        }
        if (!hasFollower) {
            lastPushedOffset_ = logEndOffset_();
            return;
        }
    }

    // Nothing new to push if fromOffset is at or above current LEO.
    Offset currentLeo = logEndOffset_();
    if (fromOffset >= currentLeo) {
        lastPushedOffset_ = currentLeo;
        return;
    }

    // Read the bytes from the log — same path as onFetchRequest().
    std::vector<uint8_t> logBytes = onLog([&](auto& L){ return L.read(fromOffset, cfg_.maxFetchBytes); });

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
