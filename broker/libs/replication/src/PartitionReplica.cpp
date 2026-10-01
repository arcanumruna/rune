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
#include <rune/replication/BatchPrefix.h>

#include <limits>
#include <rune/protocol/RecordBatch.h>
#include <rune_fr/FlightRecorder.h>
#if RUNE_REPLICATION_HAS_AERON
#include <rune/replication/WorkerReplicationTransport.h>
#include <rune/replication/AeronPartitionTransport.h>
#endif

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>

namespace rune::replication {

// ---------------------------------------------------------------------------
// Helper: build a minimal RecordBatch wrapping raw bytes as a single record.
// Uses the protocol builder so the batch header is valid (CRC stamped etc).
// Used by the LEADER path (append from producer).
// ---------------------------------------------------------------------------
// Frame a raw payload as a single-record RecordBatch, REUSING this replica's
// builder + output buffer so steady-state framing is zero-allocation.  Both the
// builder's internal records_ vector and batchScratch_ retain capacity across
// calls (reset()/buildInto() clear without freeing), eliminating the ~12 heap
// allocations/call the old build()-returns-a-fresh-vector path incurred.
//
// The returned reference is valid until the NEXT makeBatch_ call; every caller
// consumes it synchronously (appends to the log) before any further call, and
// PartitionReplica is single-writer, so this is safe and non-reentrant.  The
// framed bytes are byte-identical to the old path.
const std::vector<uint8_t>& PartitionReplica::makeBatch_(Offset baseOffset,
                                                         std::span<const uint8_t> payload)
{
    // Lazily create the reused builder on first framing use (avoids touching the
    // two constructors / initReplica); the branch is predictable after warm-up.
    if (!batchBuilder_) {
        batchBuilder_ = std::make_unique<rune::protocol::RecordBatchBuilder>(0, 0);
    }
    batchBuilder_->reset(baseOffset, /*baseTimestamp=*/0);
    // Wrap the raw payload as a single record (key=empty, value=payload).
    std::string_view keyView{};
    std::string_view valView(reinterpret_cast<const char*>(payload.data()),
                              payload.size());
    batchBuilder_->append(keyView, valView, /*timestamp=*/0);
    batchBuilder_->buildInto(batchScratch_);
    return batchScratch_;
}

// ---------------------------------------------------------------------------
// FollowerSync tracing gate.  TEMPORARY high-verbosity diagnostics: logs every
// follower-sync state change and the factors that drive it (onResponse inputs,
// decide emits, resets).  On by default; set RUNE_FS_LOG=0 to silence for clean
// throughput measurement.  Checked once (static-init) so it costs nothing on
// the hot path when off.
// ---------------------------------------------------------------------------
static bool fsLogEnabled() noexcept {
    static const bool on = [] {
        const char* e = std::getenv("RUNE_FS_LOG");
        return e == nullptr || (e[0] != '0' || e[1] != '\0');
    }();
    return on;
}


// ---------------------------------------------------------------------------
// Transport dispatch (spec §3.5a.1): the kind is fixed at construction, so this
// is one perfectly-predicted branch; the PerWorker arm is a direct call on the
// final WorkerReplicationTransport (inlinable), everything else goes virtual.
// ---------------------------------------------------------------------------
template <class F>
[[gnu::always_inline]] inline auto PartitionReplica::onTransport(F&& f)
{
#if RUNE_REPLICATION_HAS_AERON
    if (tkind_ == TransportKind::PerWorker) { return f(*perWorker_); }
    if (tkind_ == TransportKind::Aeron)     { return f(*aeronT_); }       // final → direct
#endif
    if (tkind_ == TransportKind::Null)      { return f(*nullT_); }        // final → direct no-op
    return f(*transport_);                                                // test fakes only
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
{
    assert(memLog_    != nullptr && "log must not be null");
    assert(transport_ != nullptr && "transport must not be null");
    initReplica(std::move(replicas), std::move(isr));
}

// Defined here (not defaulted in the header) so the unique_ptr<RecordBatchBuilder>
// member destroys a COMPLETE type — RecordBatch.h is included in this TU.
PartitionReplica::~PartitionReplica() = default;

// Shared construction logic for both backend overloads (de-virtualized seam).
void PartitionReplica::initReplica(std::vector<NodeId> replicas,
                                   std::vector<NodeId> isr)
{
    // Construction-time transport classification (never re-evaluated on the hot path).
    tkind_ = transport_->kind();
#if RUNE_REPLICATION_HAS_AERON
    if (tkind_ == TransportKind::PerWorker) perWorker_ = static_cast<PerWorkerTransport*>(transport_);
    if (tkind_ == TransportKind::Aeron)     aeronT_    = static_cast<AeronPartitionTransport*>(transport_);
#else
    if (tkind_ != TransportKind::Null) tkind_ = TransportKind::Generic;
#endif
    if (tkind_ == TransportKind::Null)      nullT_     = static_cast<NullPartitionTransport*>(transport_);

    // Retain the full replica set (RF) for the lifetime of this replica so a
    // follower-first replica can rebuild a correct IsrManager on a later
    // becomeLeader() instead of collapsing replicas == isr (see header note).
    configuredReplicas_ = replicas;
    if (!followerSlots_.init(configuredReplicas_)) {
        throw std::invalid_argument(
            "PartitionReplica: replica set size " + std::to_string(configuredReplicas_.size()) +
            " exceeds FollowerSlots::kMaxReplicas=" + std::to_string(FollowerSlots::kMaxReplicas));
    }

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
    if (role_ != Role::Leader) ++leadershipGeneration_;   // F15 S4: a new leadership (not an epoch bump)
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
        checkpointEpochs_();
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
    followerSlots_.resetLeadership();   // conflation + push coverage are per-leadership
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

        // DR-BUG-6 deadlock fix: only arm the epoch probe when we actually have an
        // UNCOMMITTED TAIL (localLeo > committedHw) that could diverge from the new
        // leader and thus needs KIP-101 truncation.  A follower that is merely BEHIND
        // (localLeo <= committedHw — no records past its committed point) has NOTHING
        // to truncate; arming the probe there only DEFERS its normal fetching until an
        // answer that may never come (leader slow to promote / packet loss), leaving
        // the follower frozen at a stale LEO → it never catches up → never re-admitted
        // to the ISR → acks=all stalls at minIsr forever (dr5_leader_isolation post-heal
        // hang: survivor stuck lastFetchOffset=8 while leader HW=23).  With no tail there
        // is no divergence risk (the committed prefix is identical across the lineage),
        // so we skip the probe and fetch normally to pull forward to the leader's HW.
        if (logEndOffset_() > committedHw) {
            epochProbePending_  = true;
            epochProbeEpoch_    = leaderEpoch;   // epoch we are transitioning INTO
            epochProbeFloor_    = committedHw;    // never truncate below this
            epochProbeAttempts_ = 0;
        } else {
            // No uncommitted tail → nothing to reconcile → fetch normally.
            epochProbePending_ = false;
        }
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
    if (role_ != Role::Follower) ++leadershipGeneration_;   // F15 S4
    role_     = Role::Follower;
    leaderId_ = leaderId;
    followerHw_ = 0;
    followerSync_.reset();    // new leadership epoch → bootstrap fetch state afresh
    wedgedAtOffset_ = -1;     // F18: a new leadership may serve it (re-evaluated)
    if (fsLogEnabled()) {
        spdlog::info("FollowerSync[node={} part={}] reset (becomeFollower epoch={} leader={})",
                     cfg_.selfId, cfg_.partitionId, leaderEpoch, leaderId);
    }
    lastFetchEmitNs_ = 0;     // (DR epoch-probe cadence; separate from FollowerSync)
    reconcileTarget_ = -1;
    followerSlots_.resetLeadership();

    // Publish the demotion so the cleaner stops treating this node as Leader
    // (cleanerRole() flips to Follower) — no live-state read on the cleaner.
    publishCleanerState();
}

// ---------------------------------------------------------------------------
// Leader: append
// ---------------------------------------------------------------------------

Offset PartitionReplica::append(std::span<const uint8_t> records, bool acksAll)
{
    // F15 N2: a txn marker must land AS a control batch, never as the value of
    // a wrapper data batch (pid -1, no CONTROL bit) — rebuilds and consumers
    // would never recognise it.
    if (isControlBatch(records)) [[unlikely]] return appendControlBatch(records, acksAll);
    assert(role_ == Role::Leader && "append() called on follower");
    ++rtAppends_;

    if (!isrManager_->canProduce(effectiveMinIsr_, acksAll)) {
        return -1; // gated by min-ISR
    }

    Offset baseOffset = logEndOffset_();

    // Build a proper RecordBatch so the Log segment scanner works correctly.
    // Bind by const& — makeBatch returns a reused thread-local buffer; consumed
    // synchronously by the append below before any further makeBatch call.
    const auto& batch = makeBatch_(baseOffset, records);
    // lastOffset = baseOffset + 0 (one record per batch = lastOffsetDelta=0).
    if (externalFollowerFlush_) {
        // Shard mode (spec §3.1): no inline fsync on the loop (txn markers come
        // through here).  Page-cache append + a flush request the owning worker
        // submits to its io backend in the same pass.
        onLog([&](auto& L){ L.appendNoSync(batch.data(), batch.size(), baseOffset); });
        if (!memoryBacked_) followerFlushRequested_ = true;
    } else {
        onLog([&](auto& L){ L.append(batch, baseOffset); });
    }

    // Leader LEO is now baseOffset + 1.
    leaderState_.setLeaderLogEndOffset(logEndOffset_());
    // Leader self-advances HW (leader always in ISR, so majority includes leader).
    recomputeHw();

    // Mirror the advanced HW into the cleaner-visible atomic (release-store).
    publishCleanerState();

    // GR-10: push newly-appended bytes to ISR followers immediately.  This is the
    // DIRECT append API (used by tests and internal callers that append one batch
    // and expect it replicated without a separate doWork tick).  The PRODUCE HOT
    // PATH does NOT come through here — it uses onLeaderLogAppended(), whose push is
    // COALESCED in doWork() (that is the throughput fix).  A coalesced doWork push
    // won't double-send after this one: pushToFollowers advances lastPushedOffset_
    // to the current LEO, so doWork's `leo > lastPushedOffset_` guard is false.
    if (cfg_.pushOnAppend) {
        pushToFollowers(baseOffset);
    }
    return baseOffset;
}

// ---------------------------------------------------------------------------
// Leader: append a prebuilt control batch (F15 N2)
// ---------------------------------------------------------------------------

bool PartitionReplica::isControlBatch(std::span<const uint8_t> bytes) noexcept
{
    namespace b = rune::protocol::batch;
    if (bytes.size() < static_cast<std::size_t>(b::kRecordBatchOverhead)) return false;
    const uint8_t* p = bytes.data();
    if (p[b::kMagicOffset] != 2) return false;
    const auto attrs = static_cast<uint16_t>(p[b::kAttributesOffset] |
                                             (static_cast<uint16_t>(p[b::kAttributesOffset + 1]) << 8));
    if ((attrs & static_cast<uint16_t>(b::kAttrControl)) == 0) return false;   // the common case: one test
    rune::protocol::RecordBatchReader r(p, bytes.size());
    return static_cast<std::size_t>(r.totalLength()) == bytes.size() && r.ensureValid();
}

Offset PartitionReplica::appendControlBatch(std::span<const uint8_t> controlBatch, bool acksAll)
{
    if (role_ != Role::Leader) return kAppendNotLeader;          // retriable; never an assert
    if (!isControlBatch(controlBatch)) return kAppendInvalidBatch;
    ++rtAppends_;
    if (!isrManager_->canProduce(effectiveMinIsr_, acksAll)) return kAppendMinIsr;

    const int32_t lastOffsetDelta = rune::protocol::RecordBatchReader(
        controlBatch.data(), controlBatch.size()).lastOffsetDelta();
    Offset baseOffset = logEndOffset_();
    if (memoryBacked_) {
        // InMemoryLog stores the batch bytes verbatim: stamp the base offset
        // into the reused scratch (capacity persists — no steady-state alloc).
        batchScratch_.assign(controlBatch.begin(), controlBatch.end());
        const auto u = static_cast<uint64_t>(baseOffset);
        for (int i = 0; i < 8; ++i) batchScratch_[static_cast<std::size_t>(i)] = static_cast<uint8_t>(u >> (8 * i));
        if (memLog_->appendNoSync(batchScratch_.data(), batchScratch_.size(),
                                  baseOffset + lastOffsetDelta) < 0) return kAppendBackPressure;
    } else {
        // Zero-copy restamp (pwritev): the caller's bytes are never mutated.
        diskLog_->appendNoSyncRestamp(controlBatch.data(), controlBatch.size(), lastOffsetDelta, baseOffset);
        if (externalFollowerFlush_) {
            followerFlushRequested_ = true;   // the owning worker submits it (shard, never inline)
        } else {
            diskLog_->sync();                 // append()'s durable-by-default fsync, unchanged
        }
    }

    leaderState_.setLeaderLogEndOffset(logEndOffset_());
    recomputeHw();
    publishCleanerState();
    if (cfg_.pushOnAppend) pushToFollowers(baseOffset);
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

    // Direct append API → immediate push (see append() note); hot path coalesces.
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
    // Re-read the log's LEO and recompute HW (leader always in ISR, so with the
    // ISR satisfied the HW advances to cover the newly-appended bytes exactly as
    // it would after append()).
    leaderState_.setLeaderLogEndOffset(logEndOffset_());

    // setLeaderLogEndOffset() recomputes HW eagerly — mirror it to the cleaner.
    publishCleanerState();

    // THROUGHPUT FIX: do NOT push per produce op here.  This seam is called once
    // per produce op by the gateway/worker (append-direct-to-Log path); pushing
    // inline sent one tiny 1-record replication message per record (~383 B), so the
    // follower applied ~1 record per op and acks=all crawled at ~900 rec/s.  The
    // replication push is now COALESCED in doWork() (pushToFollowers(lastPushedOffset_)
    // drains every record appended since the last push in ONE batched message), so a
    // worker that drains many produce ops in a cycle replicates them as one batch.
    // Latency is unchanged at low load: doWork runs in the same worker cycle.
}

// ---------------------------------------------------------------------------
// Leader: onFetchRequest
// ---------------------------------------------------------------------------

int16_t PartitionReplica::onFetchRequest(const FetchRequestMsg& req, int64_t nowNs)
{
    if (req.maxBytes == 0) ++rtFastAcks_;   // pure fast-ack (LEO advance only)
    RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplFetchReqHandle,
                             static_cast<uint32_t>(req.followerId));
    // Guard: drop silently if this replica is not yet the leader.
    // This can happen during the window between Aeron transport receiving a
    // FetchRequest and the metadata bridge promoting us via becomeLeader().
    // Not an error — the follower will retry on the next fetch interval.
    if (role_ != Role::Leader) {
        return kErrNotLeader;
    }

    // Epoch fence: reject stale leader epochs.
    if (isStaleEpoch(req.leaderEpoch, epoch_)) {
        auto resp = makeFetchResponse(leaderState_.highWatermark(), kErrStaleEpoch, {});
        onTransport([&](auto& t) { t.sendFetchResponse(req.followerId, resp); });
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
        ++leadershipGeneration_;   // F15 S4: self-demotion ends this leadership
        role_     = Role::Follower;
        epoch_    = req.leaderEpoch;
        leaderId_ = req.followerId; // best-effort; coordinator will correct
        followerHw_ = committedHw;
        followerSync_.reset();    // self-demotion → bootstrap fetch state afresh
        wedgedAtOffset_ = -1;     // F18
        followerSlots_.resetLeadership();
        if (fsLogEnabled()) {
            spdlog::info("FollowerSync[node={} part={}] reset (self-demote to follower "
                         "epoch={} newLeaderGuess={})",
                         cfg_.selfId, cfg_.partitionId, req.leaderEpoch, req.followerId);
        }
        lastFetchEmitNs_ = 0;     // (DR epoch-probe cadence; separate from FollowerSync)

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

        // A-NEW-5: kErrNotLeader — the follower is ahead of this stale leader.
        auto resp = makeFetchResponse(committedHw, kErrNotLeader, {});
        onTransport([&](auto& t) { t.sendFetchResponse(req.followerId, resp); });
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
            onTransport([&](auto& t) { t.sendReconcileRequest(req.followerId, pull); });
        }
        // Serve nothing forward to an ahead follower: forward-serving under the
        // new epoch could stamp divergent bytes.  The follower keeps its tail
        // until its epoch probe resolves the correct truncation point.
        return kErrNone;
    }


    // FETCH CONFLATION + NO-DUP-FETCH (spec §3.5) — FollowerSlots.h: an IDENTICAL
    // request (same fetchOffset AND LEO) within kFetchResendNs is conflated — a
    // behind follower re-fetches the same range while its prior response is in
    // flight, and re-serving it would flood its ingress ring with duplicate apply
    // ops.  Decided BEFORE the liveness update (B-2): the decision is slot-local
    // (no ISR/HW input), and it lets the single log read below happen ONLY for a
    // Data serve — a conflated or ack-only fetch reads nothing, as before.
    FollowerSlot* const slot = followerSlots_.find(req.followerId);
    const FetchServe serve = slot
        ? decideFetchServe(*slot, req.fetchOffset, req.maxBytes, logEndOffset_(),
                           cfg_.pushOnAppend, nowNs, kFetchResendNs)
        : (req.maxBytes > 0 ? FetchServe::Data : FetchServe::AckOnly);

    // F18 / KIP-74 (I-1, B-2): ONE readView for a Data serve, reused for the serve
    // below.  readView always includes the first batch whole, so v.len > maxBytes
    // ⟺ that first batch ALONE exceeds maxBytes (the transport already capped
    // maxBytes to its frame); only then is the (cold) frame ceiling queried.  An
    // unservable batch — stored under a larger config, or by an older broker — is
    // answered with an explicit, counted kErrRecordTooLarge, and the request is NOT
    // recorded as ISR liveness: the follower stops fetching it and leaves the ISR
    // through the existing lag check (HW advances with the remaining ISR; acks=all
    // fails per min-ISR) instead of re-fetching forever behind silent drops.
    rune::storage::LogReadView view{};
    if (serve == FetchServe::Data && req.fetchOffset < logEndOffset_()) {
        view = onLog([&](auto& L) { return L.readView(req.fetchOffset, req.maxBytes); });
        if (view.len > static_cast<std::size_t>(req.maxBytes)) [[unlikely]] {
            int64_t frame = std::numeric_limits<int64_t>::max();
            onTransport([&](auto& t) { frame = t.maxFetchRecordsBytes(); });
            if (static_cast<int64_t>(view.len) > frame) {
                if (recordTooLargeServed_++ == 0 || (recordTooLargeServed_ & 1023u) == 0) {
                    spdlog::error("PartitionReplica[node={} topic={} part={}]: batch at offset {} is "
                                  "{} B > the {} B replication frame — UNREPLICABLE; answering "
                                  "follower {} kErrRecordTooLarge (not ISR liveness; it will leave "
                                  "the ISR).  Raise RUNE_REPL_OUTBOUND_RING_BYTES / "
                                  "RUNE_AERON_TERM_BUFFER_BYTES.  (count={})",
                                  cfg_.selfId, cfg_.topicId, cfg_.partitionId, view.baseOffset,
                                  view.len, frame, req.followerId, recordTooLargeServed_);
                }
                // Never conflate an unservable fetch (a duplicate must not be
                // answered silently or refresh liveness): forget this serve.
                if (slot) slot->servedTimeNs = 0;
                auto resp = makeFetchResponse(highWatermark(), kErrRecordTooLarge, {});
                onTransport([&](auto& t) { t.sendFetchResponse(req.followerId, resp); });
                return kErrRecordTooLarge;
            }
        }
    }

    // Record follower fetch: update ISR manager + leader state.
    // Pass the REAL nowNs so time-lag eviction uses the actual fetch time,
    // not the stale last-ISR-check timestamp.
    isrManager_->onFollowerFetch(req.followerId, req.fetchOffset, nowNs);
    const Offset prevHwOnFetch = leaderState_.highWatermark();
    leaderState_.updateFollowerFetchOffset(req.followerId, req.fetchOffset);
    // HW is already recomputed inside updateFollowerFetchOffset().
    // FR: acks=all COMMIT point — this follower position update just lifted the
    //     majority high-watermark. Keyed by (partition<<24)|committedOffset so it
    //     pairs 1:1 with the leader's ReplLeaderAppend (round-trip commit latency).
    {
        const Offset newHwOnFetch = leaderState_.highWatermark();
        if (newHwOnFetch > prevHwOnFetch) {
            RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplHwAdvance,
                (static_cast<uint32_t>(cfg_.partitionId) << 24) |
                    (static_cast<uint32_t>(newHwOnFetch - 1) & 0xFFFFFFu));
        }
    }
    // (FR: the onFetchRequest SPAN [ReplFetchReqHandle] measures this handler; the
    //  control→worker handoff is measured cleanly via ReplFragRecv→ReplWorkerOpRun.)

    // DR-BUG-6 completeness gate: record this survivor's frontier.  A plain fetch
    // cannot validate lineage (req carries the fetch epoch, not the tail epoch), so
    // it can NEVER raise the frontier above our own LEO — clamp it.  Once reconcile
    // has raised our LEO, subsequent fetches naturally record the higher value.
    if (!leaderComplete_) {
        survivorFrontier_[req.followerId] =
            std::min<Offset>(req.fetchOffset, logEndOffset_());
        maybeCompleteLeadership();
    }

    // Conflated (decided above): an identical response is still in flight — ISR
    // liveness + HW were updated above, so skipping the RESPONSE is safe; the
    // timeout re-serve recovers a genuinely lost push/response.
    if (serve == FetchServe::Conflate) return kErrNone;

    // Read records (Data only; AckOnly answers the HW with no records).
    // Zero-copy: the ONE view read above (B-2) — a view into the log (disk mmap /
    // memory ring).  The transport copies the bytes ONCE, straight into the
    // publication term.  (No append/truncate/readView ran in between.)
    std::span<const uint8_t> records{};
    // F3 fix-round-1, item 3 (merged onto F18's single read): fetchOffset < LEO
    // means there IS more data past this point, so an EMPTY view can only be a
    // transient local read-map failure (readView()/ensureReadMap() are noexcept
    // and return empty on failure — LogSegment::readMapFailures()), NEVER
    // "caught up".  An empty kErrNone Data response would make the follower's
    // FollowerSync treat it as caught up; skip responding this cycle instead —
    // the follower's un-acked fetch resends and the lazy map retries.  ISR
    // liveness / HW bookkeeping above is unaffected.
    if (serve == FetchServe::Data && req.fetchOffset < logEndOffset_() && view.empty())
        [[unlikely]] return kErrNone;
    if (serve == FetchServe::Data && !view.empty()) {
        // GR-1: whole batches only.  F18 / KIP-74: the first batch at fetchOffset
        // is ALWAYS served whole even above req.maxBytes (readView already always
        // includes it); later batches stay bounded by req.maxBytes.  A first batch
        // that does not fit one frame was answered kErrRecordTooLarge above.
        const BatchPrefix pre = wholeBatchPrefix(view.data, view.len,
                                                 static_cast<std::size_t>(req.maxBytes),
                                                 kNoPushCeiling);
        records = std::span<const uint8_t>(view.data, pre.bytes);
    }

    // DR-BUG-3: advertise the min-ISR-GATED committed HW to followers via the
    // gated accessor.  followerHw_ = min(localLeo, resp.highWatermark), so a
    // rejoining follower never treats a minority leader's uncommitted tail as
    // committed.  Note this does NOT bound catch-up: the log read above is
    // intentionally NOT HW-bounded, so the follower still fetches (and applies)
    // the uncommitted tail and advances its own LEO — which is exactly what
    // lets the leader's real (majority) HW advance and the ISR re-expand.  Only
    // the follower's *committed view* (followerHw_) is held at the majority floor
    // until the ISR heals.
    if (!records.empty()) ++fetchDataServes_;
    const Offset advertiseHw = highWatermark();
    auto resp = makeFetchResponse(advertiseHw, kErrNone, records);
    onTransport([&](auto& t) { t.sendFetchResponse(req.followerId, resp); });

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
    ++rtApplies_;
    rtApplyBytes_ += static_cast<int64_t>(resp.records.size());
    RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplFetchRespApply,
                             static_cast<uint32_t>(cfg_.partitionId));
    if (resp.errorCode != kErrNone) {
        // F18 (I-1) ⇄ F3 (fix-round-2 item A): the batch at our LEO cannot be
        // replicated in one frame (the leader's, or — injected by our own
        // transport — our worker ring's).  Real data, never "caught up": force
        // FollowerSync Behind (F3) and, not retriable for this offset, WEDGE here
        // (F18: no further fetches → no ISR liveness → the leader evicts us by its
        // existing lag check).  Counted + logged.  Cleared by a leadership/epoch
        // change or a truncation moving LEO.  A stale-epoch answer (deposed
        // leader) never wedges.
        // (The leader answers for the offset we fetched = our LEO: a false wedge
        //  is impossible on this path — nothing can move LEO past an unservable batch.)
        if (resp.errorCode == kErrRecordTooLarge) {
            followerSync_.forceBehind();
            if (role_ == Role::Follower && resp.leaderEpoch >= epoch_)
                wedgeAt_(logEndOffset_(), "the leader cannot fit it in one replication frame");
        }
        // Other errors — ignore (control plane handles epoch changes externally).
        return;
    }
    // fsyncgate fail-stop: a log whose fsync failed accepts no further data
    // (the broker is halting; restart recovery + replication re-heal it).
    if (!memoryBacked_ && diskLog_ != nullptr && diskLog_->storageFailed()) [[unlikely]] return;

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
    // FollowerSync: a response from the (current/newer) leader arrived.  Non-empty
    // ⟺ still behind (keep fetching); empty ⟺ caught up (keepalive only).  This also
    // clears any outstanding single-outstanding data-fetch (the leader answered).
    {
        const FollowerSync::State fsPrev = followerSync_.state();
        const bool fsWasArmed = followerSync_.fetchArmed();
        const bool hadRecords = !resp.records.empty();
        followerSync_.onResponse(hadRecords, resp.highWatermark);
        // Change-gated: only log a STATE transition (not every applied response — that
        // floods under load and starves the worker).
        if (fsLogEnabled() && fsPrev != followerSync_.state()) {
            spdlog::info(
                "FollowerSync[node={} part={}] onResponse hadRecords={} nrec={} "
                "leaderHw={} localLeo={} | {} -> {} armed {}->{}",
                cfg_.selfId, cfg_.partitionId, hadRecords, resp.records.size(),
                resp.highWatermark, logEndOffset_(),
                FollowerSync::toString(fsPrev),
                FollowerSync::toString(followerSync_.state()),
                fsWasArmed, followerSync_.fetchArmed());
        }
    }

    // Capture pre-apply state to detect if anything changed (for fast-ack guard).
    const Offset preLeo = logEndOffset_();
    const Offset preHw  = followerHw_;
    // (FR: applyReplicated entry is measured by the ReplFetchRespApply SPAN; the
    //  control→worker handoff that delivers this op is ReplFragRecv→ReplWorkerOpRun.)

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
            checkpointEpochs_();
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
                // THROUGHPUT FIX: appendNoSync — do NOT fsync per apply.  The syncing
                // append() cost ~693µs mean (disk fsync) PER batch on the follower,
                // capping acks=all at ~1.5k rec/s.  acks=all crash-durability comes
                // from ISR REPLICATION (multiple page-cache copies), NOT per-record
                // fsync on each follower (same as Kafka's default + rune's documented
                // appendNoSync contract).  A single coalesced sync() runs once per
                // duty cycle below (followerApplyDirty_), amortising the fsync.
                onLog([&](auto& L){ L.appendNoSync(raw.data(), raw.size(), lastOff); });
                followerApplyDirty_ = true;
            }
        } else {
            // Fallback: raw user bytes (e.g. injected by unit tests) — wrap in
            // a RecordBatch so the log segment scanner can parse them.
            Offset baseOffset = logEndOffset_();
            // Merge: keep main's zero-alloc makeBatch_ (reused buffer) + multinode
            // throughput fix (appendNoSync; coalesced sync via followerApplyDirty_).
            const auto& batch = makeBatch_(baseOffset, resp.records);
            onLog([&](auto& L){ L.appendNoSync(batch.data(), batch.size(), baseOffset); });
            followerApplyDirty_ = true;
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
            checkpointEpochs_();
        }
    }

    // Advance local HW = min(localLeo, leader's HW).
    Offset localLeo = logEndOffset_();
    Offset newHw    = std::min(localLeo, resp.highWatermark);
    if (newHw > followerHw_) {
        followerHw_ = newHw;
    }
    // (followerSync_.onResponse() was recorded at the top of this function,
    //  before any gap/truncate handling, so a gapped-but-behind follower still
    //  learns the leader is ahead and keeps fetching FAST.)

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
    // FR: the follower has now appended up to postLeo-1 — the "last replica received
    //     offset X" marker. Keyed by (partition<<24)|lastOffset to pair with the
    //     leader's ReplLeaderAppend / ReplHwAdvance for end-to-end (cross-node)
    //     replication latency.
    if (postLeo > preLeo) {
        RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplFollowerRecv,
            (static_cast<uint32_t>(cfg_.partitionId) << 24) |
                (static_cast<uint32_t>(postLeo - 1) & 0xFFFFFFu));
    }
    if (cfg_.fastAckOnApply && role_ == Role::Follower
        && (postLeo > preLeo || postHw > preHw)) {
        FetchRequestMsg ack = nextFetch();
        ack.maxBytes = 0;   // PURE ACK: advance leader HW only; NEVER request data
                            // (data flows via push / the single-outstanding fetch).
        sendFetchToLeader_(ack);
    }
}

// F18: enter the wedged state at `at` (idempotent per offset): no further fetch
// or epoch probe (doWork), so the leader's existing lag check evicts us.
void PartitionReplica::wedgeAt_(Offset at, const char* why)
{
    if (wedgedAtOffset_ == at) return;
    wedgedAtOffset_ = at;
    ++wedgedRefusals_;
    spdlog::error("PartitionReplica[node={} topic={} part={}]: follower WEDGED at offset {} — "
                  "the batch there is unreplicable ({}); stopped fetching it, leaving the ISR "
                  "(count={})", cfg_.selfId, cfg_.topicId, cfg_.partitionId, at, why,
                  wedgedRefusals_);
}

void PartitionReplica::onUnapplicableBatch(Offset batchBase, Epoch leaderEpoch)
{
    if (role_ != Role::Follower || leaderEpoch < epoch_) return;   // stale / not a follower
    if (batchBase != logEndOffset_()) return;   // not the batch we need: never wedge (B-1)
    wedgeAt_(batchBase, "it cannot fit one frame of this node's worker ingress ring");
}

// Spec §3.5.2: the follower knows its leader — unicast.  Before the leader is
// known (leaderId_ < 0) fall back to the transport's broadcast.
void PartitionReplica::sendFetchToLeader_(const FetchRequestMsg& req)
{
    if (leaderId_ >= 0 && leaderId_ != cfg_.selfId)
        onTransport([&](auto& t) { t.sendFetchRequestTo(leaderId_, req); });
    else
        onTransport([&](auto& t) { t.sendFetchRequest(req); });
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
        onTransport([&](auto& t) { t.sendOffsetsForLeaderEpochResponse(req.followerId, resp); });
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
            // BUG FIX (ISR-tracking divergence): the probe must ALSO advance the
            // IsrManager's per-follower fetch offset/time, exactly like a normal
            // fetch (onFetchRequest updates BOTH).  Updating only leaderState_ left
            // IsrManager's offset stale, so maybeShrinkIsr() evicted a follower that
            // is in fact caught up (it was reporting its LEO via probes, not normal
            // fetches, during epoch churn) → ISR collapsed below minIsr → produce
            // rejected with MinIsrViolation even at 1 partition.  Keeping the two
            // trackings in lock-step is what the comment above always intended
            // ("the survivor stays a live ISR member").
            isrManager_->onFollowerFetch(req.followerId, trackedLeo, lastKnownNowNs_);
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
            onTransport([&](auto& t) { t.sendReconcileRequest(req.followerId, pull); });
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
        onTransport([&](auto& t) { t.sendOffsetsForLeaderEpochResponse(req.followerId, defer); });
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
    onTransport([&](auto& t) { t.sendOffsetsForLeaderEpochResponse(req.followerId, resp); });
}

// (Follower) Apply the leader's answer: the DEFERRED, epoch-correct truncation.
void PartitionReplica::applyOffsetsForLeaderEpochResponse(
        const OffsetsForLeaderEpochRespMsg& resp)
{
    if (fsLogEnabled()) {
        spdlog::info("EpochProbe[node={} part={}] RESP epoch={} endOffset={} err={} "
                     "pending={} myLeo={} floor={}",
                     cfg_.selfId, cfg_.partitionId, resp.leaderEpoch, resp.endOffset,
                     resp.errorCode, epochProbePending_, logEndOffset_(), epochProbeFloor_);
    }
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
    sendFetchToLeader_(req);
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

        // Whole-batch alignment — the SAME walk as onFetchRequest/push (GR-1).
        // F18 / KIP-74: an oversized first batch is served whole (L.read always
        // returns it), later batches stay bounded by req.maxBytes.
        if (!logBytes.empty()) {
            logBytes.resize(wholeBatchPrefix(logBytes.data(), logBytes.size(),
                                             static_cast<std::size_t>(req.maxBytes),
                                             kNoPushCeiling).bytes);
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
    // survivor LEO as the HW hint; our epoch (>= leader's) fences stale.
    auto resp = makeFetchResponse(
        leo, kErrNone,
        std::span<const uint8_t>(logBytes.data(), logBytes.size()));
    onTransport([&](auto& t) { t.sendReconcileResponse(req.followerId, resp); });
}

// (Leader) Apply reconcile records pulled from an ahead survivor.
void PartitionReplica::applyReconcileResponse(const FetchResponseMsg& resp)
{
    if (resp.errorCode != kErrNone) { reconcileTarget_ = -1; return; }
    if (role_ != Role::Leader)      { reconcileTarget_ = -1; return; }
    if (!memoryBacked_ && diskLog_ != nullptr && diskLog_->storageFailed()) { reconcileTarget_ = -1; return; }

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
                // F15 §2.1 step 3: a reconcile append adds FOREIGN (old-term)
                // batches; nothing below the pre-append LEO changes.
                lastTruncationOffset_ = logEndOffset_();
                ++logGeneration_;
                if (externalFollowerFlush_) {
                    // Shard mode: page-cache append; durability via the owning
                    // worker's io backend (flush request), never an inline fsync.
                    onLog([&](auto& L){ L.appendNoSync(raw.data(), raw.size(), lastOff); });
                    if (!memoryBacked_) followerFlushRequested_ = true;
                } else {
                    onLog([&](auto& L){ L.append(raw, lastOff); });
                }
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
                checkpointEpochs_();
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

// The leader-epoch checkpoint is THE single accepted synchronous disk write on a
// shard loop (Task 10 fix-1 ruling): it is a correctness ordering point (the
// epoch must be durable before the replica acts in it), it happens only on role
// / epoch transitions, and the file is a few KB.  Counted so it is measurable;
// making it async with role-transition gating is tracked in the plan.
void PartitionReplica::checkpointEpochs_()
{
    ++epochCheckpointWrites_;
    epochCache_.checkpointToFile(diskLog_->dir() + "/leader-epoch-checkpoint");
}

int PartitionReplica::doWork(int64_t nowNs)
{
    int work = 0;
    lastKnownNowNs_ = nowNs;   // most-recent duty-cycle clock, reused by message
                               // handlers (e.g. onOffsetsForLeaderEpochRequest) that
                               // have no nowNs of their own but need a fetch timestamp.

    // TIME-GATED coalesced follower flush: the follower applies via appendNoSync
    // (no per-apply fsync — that cost ~693µs/batch, capping acks=all at ~1.5k/s).
    // A fsync EVERY duty cycle instead capped throughput at the cycle rate; here we
    // fsync at most once per kFollowerFlushIntervalNs, amortising it over the many
    // applies in that window (Kafka likewise flushes followers on a coarse interval,
    // not per record — acks=all crash-durability comes from ISR replication).
    // Disk-backed only (memory-backed sync() is a no-op).  Bounded flush lag =
    // kFollowerFlushIntervalNs.
    static constexpr int64_t kFollowerFlushIntervalNs = 20'000'000LL; // 20 ms
    if (followerApplyDirty_ && !memoryBacked_
        && nowNs - lastFollowerSyncNs_ >= kFollowerFlushIntervalNs) {
        followerApplyDirty_ = false;
        lastFollowerSyncNs_ = nowNs;
        if (externalFollowerFlush_) {
            // Shard mode: the owning worker submits it to its io backend (never
            // blocks the shard loop).  Raised at most once per interval.
            followerFlushRequested_ = true;
        } else {
            RUNE_FR_SPAN_STORAGE(rune_fr::Site::StorageFlush,
                                 static_cast<uint32_t>(cfg_.partitionId));
            // fsyncgate: a failed fsync never escapes doWork (callers run on
            // AgentRunner threads with no catch → std::terminate).  The log is now
            // permanently failed (LogSegment::sync marks it); the OWNER's loop
            // sees storageFailed() and routes it to the broker halt (exit 74).
            try {
                onLog([&](auto& L){ L.sync(); });
            } catch (const std::exception& ex) {
                if (!memoryBacked_ && diskLog_ != nullptr) diskLog_->markStorageFailed(5 /*EIO*/);
                spdlog::critical("PartitionReplica[topic={} part={}]: follower flush fsync failed: {}",
                                 cfg_.topicId, cfg_.partitionId, ex.what());
            }
        }
        ++work;
    }

    // Per-second rate diagnostic (RUNE_FS_LOG) — localise the acks=all bottleneck.
    if (fsLogEnabled()) {
        if (rtLastNs_ == 0) { rtLastNs_ = nowNs; rtLastHw_ = leaderState_.highWatermark(); rtLastLeo_ = logEndOffset_(); }
        else if (nowNs - rtLastNs_ >= 1'000'000'000LL) {
            const Offset hw = leaderState_.highWatermark();
            const Offset leo = logEndOffset_();
            spdlog::info("RATE[node={} part={} role={}] appends/s={} applies/s={} pushes/s={} "
                         "fastAcksRecv/s={} hwAdv/s={} leoAdv/s={} recsPerApply={} "
                         "bytesPerPush={} hw={} leo={}",
                         cfg_.selfId, cfg_.partitionId, role_ == Role::Leader ? "L" : "F",
                         rtAppends_, rtApplies_, rtPushes_, rtFastAcks_,
                         hw - rtLastHw_, leo - rtLastLeo_,
                         rtApplies_ ? (leo - rtLastLeo_) / rtApplies_ : 0,
                         rtPushes_ ? rtPushBytes_ / rtPushes_ : 0, hw, leo);
            rtLastNs_ = nowNs; rtAppends_ = rtApplies_ = rtPushes_ = rtFastAcks_ = 0;
            rtApplyBytes_ = rtPushBytes_ = 0;
            rtLastHw_ = hw; rtLastLeo_ = leo;
        }
    }

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

        // GR-10 COALESCED replication push: drain ALL records appended since the
        // last push into ONE batched push, instead of one tiny push per append.
        // append()/appendKeyed() no longer push inline (see note there); the worker
        // drains many produce ops into the log in a single ingress-drain, then this
        // runs once in the same cycle → the follower applies one large batch instead
        // of thousands of 1-record messages.  This is the acks=all throughput fix.
        if (cfg_.pushOnAppend && logEndOffset_() > lastPushedOffset_) {
            // FLOW CONTROL: never push more than kPushFlowWindow records ahead of the
            // SLOWEST ISR follower's acked LEO.  Outrunning the follower makes Aeron
            // back-pressure and DROP the best-effort push → the follower stalls behind
            // the gap and only recovers via the slow keepalive → the acks=all "push
            // outran follower" collapse (HW freezes with ISR full).  Capping in-flight
            // to a window throttles the leader to the follower's apply rate (natural
            // backpressure to the client) → STABLE.  When the follower is at the window
            // (behind) we skip the push entirely — no wasted log read/encode this cycle;
            // the follower's fetch/keepalive drains it and its ack advances the window.
            Offset minFollowerLeo = logEndOffset_();
            const auto& fleos = leaderState_.followerLeos();
            for (NodeId id : isrManager_->isr()) {
                if (id == cfg_.selfId) continue;
                const auto it = fleos.find(id);
                const Offset fl = (it != fleos.end()) ? it->second : 0;
                if (fl < minFollowerLeo) minFollowerLeo = fl;
            }
            const Offset pushCeiling = minFollowerLeo + kPushFlowWindow;
            if (pushCeiling > lastPushedOffset_ &&
                !unsendablePushSuppressed_(pushCeiling)) {
                pushToFollowers(lastPushedOffset_, pushCeiling);
                ++work;
            }
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

            // Change-gated diagnostic (cheap): only log when the ISR size shrinks
            // below the full replica set OR recovers — logging every check starves
            // the worker under load (observed: leo frozen at 0).
            if (fsLogEnabled()) {
                const int isrSz = static_cast<int>(isrManager_->isr().size());
                const int repl  = static_cast<int>(isrManager_->replicas().size());
                if (isrSz != lastLoggedIsrSize_) {
                    lastLoggedIsrSize_ = isrSz;
                    std::string dump;
                    for (NodeId n : isrManager_->replicas()) {
                        dump += " n" + std::to_string(n) + "=off"
                              + std::to_string(isrManager_->debugLastFetchOffset(n))
                              + "/age" + std::to_string(
                                    isrManager_->debugFetchAgeNs(n, nowNs) / 1'000'000)
                              + "ms" + (isrManager_->inIsr(n) ? "(isr)" : "(out)");
                    }
                    spdlog::info("ISRChange[node={} part={}] isr {}->{} of {} hw={} leo={}{}",
                                 cfg_.selfId, cfg_.partitionId, isrSz, isrSz, repl,
                                 leaderState_.highWatermark(), logEndOffset_(), dump);
                }
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
                        onTransport([&](auto& t) { t.sendReconcileRequest(aheadPeer, pull); });
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
        // F18 (I-1): a wedge is bound to the offset — a truncation that moved our
        // LEO clears it (time-independent: no timer).  While wedged the follower
        // sends NOTHING: no fetch, no keepalive, and no KIP-101 epoch probe (minor 1:
        // probes refresh the leader's ISR liveness), so the leader's lag check
        // evicts it.  The probe re-arms on the next leadership/epoch change.
        if (wedgedAtOffset_ >= 0 && wedgedAtOffset_ != logEndOffset_()) wedgedAtOffset_ = -1;
        if (epochProbePending_ && wedgedAtOffset_ < 0) {
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
                    sendFetchToLeader_(fr);
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
                if (fsLogEnabled()) {
                    spdlog::info("EpochProbe[node={} part={}] SEND to leader={} "
                                 "requestedEpoch={} followerLeo={} (epoch_={} attempt={})",
                                 cfg_.selfId, cfg_.partitionId, leaderId_,
                                 req.requestedEpoch, req.followerLeo, epoch_,
                                 epochProbeAttempts_);
                }
                onTransport([&](auto& t) { t.sendOffsetsForLeaderEpochRequest(leaderId_, req); });
                ++work;
            }
        } else {
            // Follower normal fetch — driven ENTIRELY by the FollowerSync state
            // machine (single source of truth; see FollowerSync in the header).
            //   InSync → DATA keepalive (pulls any push the follower missed; returns
            //            empty when genuinely caught up).  Behind → SINGLE-OUTSTANDING
            //            data-fetch on the fast cadence (kills the redundant-fetch flood
            //            at its source), with a timeout re-send.
            // Keepalive cadence: small enough that a LOST push is rediscovered within
            // ~keepaliveNs (else the follower stalls behind a dropped push until ISR
            // eviction — the InSync-stall collapse), yet not so fast it spins an idle
            // follower.  Capped at 20 ms and always << replicaLagMaxMs (ISR-safe).
            const int64_t keepaliveNs = std::clamp<int64_t>(
                (cfg_.replicaLagMaxMs * 1'000'000LL) / 4,
                cfg_.fetchIntervalNs, 20'000'000LL);
            const bool wedged = wedgedAtOffset_ >= 0;   // (cleared above if LEO moved)
            const FollowerSync::State fsPrev = followerSync_.state();
            const bool fsWasArmed = followerSync_.fetchArmed();
            const FollowerSync::Action act = wedged
                ? FollowerSync::Action{false, false}       // wedged: no fetch, no liveness
                : followerSync_.decide(nowNs, cfg_.fetchIntervalNs, keepaliveNs, kFetchResendNs);
            if (act.emit) {
                FetchRequestMsg req = nextFetch();
                if (!act.wantData) req.maxBytes = 0;   // pure-ack keepalive
                sendFetchToLeader_(req);
                if (act.wantData) ++work;              // real catch-up counts as work
                if (fsLogEnabled()) {
                    spdlog::info(
                        "FollowerSync[node={} part={}] EMIT state={} wantData={} "
                        "fetchOffset={} leaderId={} armedBefore={} "
                        "(fast={}us keep={}us resend={}us)",
                        cfg_.selfId, cfg_.partitionId,
                        FollowerSync::toString(fsPrev), act.wantData,
                        req.fetchOffset, leaderId_, fsWasArmed,
                        cfg_.fetchIntervalNs / 1000, keepaliveNs / 1000,
                        kFetchResendNs / 1000);
                }
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

    // Controller position snapshot: mirror this replica's view of replica-node LEOs
    // for the leadership-convergence path (see controllerReplicaLeo() in the header).
    // Stored as leo+1 so a zero-init slot reads back as "unknown".  Self is always
    // known; a leader additionally publishes each follower's last-fetched LEO.
    publishedControllerEpoch_.store(static_cast<int32_t>(epoch_), std::memory_order_release);
    if (cfg_.selfId >= 0 && cfg_.selfId < kMaxControllerNodes) {
        publishedReplicaLeo_[static_cast<std::size_t>(cfg_.selfId)].store(
            logEndOffset_() + 1, std::memory_order_release);
    }
    if (role_ == Role::Leader) {
        for (const auto& [fid, leo] : leaderState_.followerLeos()) {
            if (fid >= 0 && fid < kMaxControllerNodes) {
                publishedReplicaLeo_[static_cast<std::size_t>(fid)].store(
                    leo + 1, std::memory_order_release);
            }
        }
    }
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
    const Offset frontier = majorityCommittedFrontier();
    if (logEndOffset_() < frontier) return;

    // Complete: release the served HW.  The floor was the last majority-committed
    // offset; the raw HW is now >= it (we hold the frontier) so serving resumes
    // without any regression.
    leaderComplete_ = true;

    // DR-BUG-3 × DR-BUG-6 INTERACTION FIX: recover the healthy-committed floor.
    // A new leader seeds lastHealthyHw_ from its OWN follower-view committed HW at
    // becomeLeader — which may LAG the true committed point (it had not yet observed
    // the last commit when the old leader died).  Under AllInSync the post-failover
    // ISR (survivors) sits below minIsr=RF, so the DR-BUG-3 min-ISR gate clamps the
    // served HW to lastHealthyHw_ — and if that seed is stale, an already-committed
    // record stays hidden FOREVER (observed: all-in-sync failover stalls one record
    // short of committedHw).  Raise the healthy floor to the RAW MAJORITY-OF-ISR HW
    // (leaderState_.highWatermark()): the offset a MAJORITY of the current ISR
    // provably holds.  That is genuinely committed (pre-failover AllInSync commits
    // were held by ALL replicas → held by every survivor → included in the majority
    // HW), and it can NEVER expose an uncommitted tail held only by this leader —
    // the majority computation excludes any offset a minority holds (NB1 guard: a
    // promoted follower with an uncommitted [committed,leo) tail and one peer at 0
    // yields majority HW == committed, so the floor stays put).  Do NOT use
    // majorityCommittedFrontier() here — that returns this leader's own LEO (the
    // ahead-follower reconcile target), which WOULD leak the uncommitted tail.
    const Offset recoveredHw = leaderState_.highWatermark();
    if (recoveredHw > lastHealthyHw_) lastHealthyHw_ = recoveredHw;
}

void PartitionReplica::recomputeHw()
{
    // S1/C4 fix: setLeaderLogEndOffset now eagerly recomputes HW internally,
    // so a single call is sufficient.  We no longer call
    // updateFollowerFetchOffset(selfId, ...) — that was an incorrect hack that
    // polluted the follower LEO map with the leader's own entry.
    const Offset prevHwRecompute = leaderState_.highWatermark();
    leaderState_.setLeaderLogEndOffset(logEndOffset_());
    // FR: HW may advance on the leader here (append-time / acks=leader / single-node).
    //     The follower-driven acks=all commit advance is marked in onFetchRequest.
    //     Keyed by (partition<<24)|committedOffset to pair with ReplLeaderAppend.
    const Offset newHwRecompute = leaderState_.highWatermark();
    if (newHwRecompute > prevHwRecompute) {
        RUNE_FR_MARK_REPLICATION(rune_fr::Site::ReplHwAdvance,
            (static_cast<uint32_t>(cfg_.partitionId) << 24) |
                (static_cast<uint32_t>(newHwRecompute - 1) & 0xFFFFFFu));
    }
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
        checkpointEpochs_();
    }

    onLog([&](auto& L){ L.truncateTo(truncateOffset); });

    // F15 §2.1 step 3: the log's existing content changed at and above
    // truncateOffset — every parked waiter / RC build re-validates against it.
    lastTruncationOffset_ = truncateOffset;
    ++logGeneration_;

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
//   1. Take a ZERO-COPY view of [fromOffset, LEO) via readView() (spec §3.5a.2).
//   2. Trim it with the shared whole-batch walk (wholeBatchPrefix, same as
//      onFetchRequest()) so the encoded FetchResponse fits within transport limits.
//   3. Build a FetchResponse via makeFetchResponse() (zero-copy records view).
//   4. Call sendFetchResponse(followerId, resp) for each ISR follower != selfId.
//   5. Update lastPushedOffset_ so onLeaderLogAppended() does not re-push.
//
// Epoch fencing: the FetchResponse carries the current leader epoch; followers
// compare it against their local epoch and drop mismatches (existing applyReplicated
// guard: resp.leaderEpoch > epoch_ → truncate; resp.leaderEpoch < epoch_ → silent
// drop via errorCode path handled above).  Stale-epoch pushes are harmless but
// wasteful; the follower's fast-ack fetch-response will carry the new epoch.
// ---------------------------------------------------------------------------
void PartitionReplica::pushToFollowers(Offset fromOffset, Offset pushCeiling) noexcept
{
    RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplPushToFollowers,
                             static_cast<uint32_t>(cfg_.partitionId));
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
    // (FR: pushToFollowers duration is the ReplPushToFollowers SPAN; the actual
    //  send is measured by the transport ReplOutEnqueue→ReplOutDrain seq pair.)

    // m-3: the push prefix is bounded by the transport's deliverable FRAME, not
    // only maxFetchBytes (1 MiB) — a prefix above the frame was built, dropped by
    // the transport, yet still advanced pushedThrough (costing a resend round-trip).
    int64_t frame = std::numeric_limits<int64_t>::max();
    onTransport([&](auto& t) { frame = t.maxFetchRecordsBytes(); });
    const std::size_t pushCap = static_cast<std::size_t>(
        std::min<int64_t>(cfg_.maxFetchBytes, frame));

    ++rtPushes_;
    // Zero-copy: a view into the log (same path as onFetchRequest()); the
    // transport copies the bytes once, straight into each follower's term buffer.
    //
    // (F3 round 6 item 1 — push chunk capped at the transport's LIVE ceiling, so
    // a coalesced push is never dropped after lastPushedOffset_ advanced — is
    // exactly F18 m-3's pushCap above; one ceiling: maxFetchRecordsBytes().)
    const rune::storage::LogReadView v =
        onLog([&](auto& L) { return L.readView(fromOffset, static_cast<int32_t>(pushCap)); });
    // Advance lastPushedOffset_ by ONLY what is actually pushed (ceiling + cap bound).
    // The read is capped at min(maxFetchBytes, frame); under a large backlog one
    // push covers a prefix and the next duty cycle pushes the rest.  F18 / KIP-74:
    // a first batch larger than maxFetchBytes is still pushed whole (never stuck).
    // FLOW CONTROL: no batch that begins at or beyond pushCeiling is pushed
    // (in-flight <= kPushFlowWindow).
    const BatchPrefix pre = wholeBatchPrefix(v.data, v.len, pushCap, pushCeiling);
    if (pre.bytes == 0) return;   // nothing deliverable within ceiling/cap this pass
    // F18 (I-1): an oversized first batch (KIP-74) that exceeds the FRAME is
    // unservable — never push it (the transport would drop it and pushedThrough
    // would lie); the fetch path answers kErrRecordTooLarge and the followers
    // leave the ISR, after which this partition stops pushing (ISR = self).
    if (static_cast<int64_t>(pre.bytes) > frame) [[unlikely]] {
        // F3 round 6, item 8: frame 0 = no publication resolved yet — not a
        // property of this batch: never arm the memo / log a "ceiling of 0".
        if (frame <= 0) return;
        // F3 round 5, item I2: remember this head so doWork() does not re-read /
        // re-parse it every cycle until LEO, the ISR, the follower position
        // (pushCeiling) or the epoch changes; the critical log fires once per
        // unsendable-offset transition.
        const Offset firstBase = rune::protocol::RecordBatchReader(v.data, v.len).baseOffset();
        ++unsendableBatchLogCount_;
        unsendablePush_ = UnsendablePushMemo{fromOffset, currentLeo, pushCeiling,
                                             isrSignature_(), epoch_};
        if (firstBase != unsendablePushLoggedOffset_) {
            unsendablePushLoggedOffset_ = firstBase;
            ++unsendablePushLogEmits_;
            spdlog::critical(
                "PartitionReplica[node={} part={}] PUSH batch at offset={} is {} "
                "bytes — exceeds this transport's replication frame of {} bytes.  Push "
                "cannot deliver it (evaluations={}); the follower's own fetch gets "
                "kErrRecordTooLarge.",
                cfg_.selfId, cfg_.partitionId, firstBase, pre.bytes, frame,
                unsendableBatchLogCount_);
        }
        return;
    }
    const Offset      pushedThrough = pre.endOffset;
    const std::size_t sendBytes     = pre.bytes;
    unsendablePush_.fromOffset = -1;   // F3 r5 I2: a sendable head disarms the memo

    // Build once — reused for all ISR followers; the view stays valid for the loop
    // (no append/truncate on this thread in between).
    // DR-BUG-3: advertise the min-ISR-gated HW (see onFetchRequest).
    auto resp = makeFetchResponse(highWatermark(), kErrNone,
                                  std::span<const uint8_t>(v.data, sendBytes));
    rtPushBytes_ += static_cast<int64_t>(sendBytes);

    for (NodeId followerId : isrList) {
        if (followerId == cfg_.selfId) continue;
        onTransport([&](auto& t) { t.sendFetchResponse(followerId, resp); });
        if (FollowerSlot* s = followerSlots_.find(followerId)) {
            if (pushedThrough > s->pushedThrough) s->pushedThrough = pushedThrough;
        }
    }
    lastPushedOffset_ = pushedThrough;   // next cycle pushes the rest once the
                                         // follower's ack lifts the window.
}

// ---------------------------------------------------------------------------
// F3 round 5, item I2: unsendable-push suppression (see header).
// ---------------------------------------------------------------------------
bool PartitionReplica::unsendablePushSuppressed_(Offset pushCeiling) const noexcept
{
    if (unsendablePush_.fromOffset < 0) [[likely]] return false;   // unarmed
    return unsendablePush_.fromOffset  == lastPushedOffset_ &&
           unsendablePush_.leo         == logEndOffset_() &&
           unsendablePush_.pushCeiling == pushCeiling &&
           unsendablePush_.epoch       == epoch_ &&
           unsendablePush_.isrSig      == isrSignature_();
}

uint64_t PartitionReplica::isrSignature_() const noexcept
{
    uint64_t sig = 1469598103934665603ULL;   // FNV-1a over (size, ids in order)
    if (!isrManager_) return sig;
    const std::vector<NodeId>& isr = isrManager_->isr();
    sig = (sig ^ static_cast<uint64_t>(isr.size())) * 1099511628211ULL;
    for (NodeId id : isr) sig = (sig ^ static_cast<uint64_t>(id)) * 1099511628211ULL;
    return sig;
}

} // namespace rune::replication
