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
#include <rune/replication/InternalIsrKnowledge.h>
#include <rune/replication/DiagFlags.h>

#include <limits>
#include <rune/protocol/RecordBatch.h>
#include <rune_fr/FlightRecorder.h>
#if RUNE_REPLICATION_HAS_AERON
#include <rune/replication/WorkerReplicationTransport.h>
#include <rune/replication/AeronPartitionTransport.h>
#endif

#include <spdlog/spdlog.h>

#include <algorithm>
#include <bit>
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
// decide emits, resets).  OPT-IN: off unless RUNE_FS_LOG is set to a value
// other than "0" — a default deployment never formats a log line per follower
// fetch.  Read once at static init into a namespace-scope const (no function-
// local static: no guard-variable acquire load per call on the replication path).
// Only read after main() (replicas are never built by a static initializer).
// ---------------------------------------------------------------------------
static const bool g_fsLogEnabled = diagFlagFromEnv(std::getenv("RUNE_FS_LOG"));
static bool fsLogEnabled() noexcept { return g_fsLogEnabled; }


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
    replicaFetchPos_.assign(configuredReplicas_.size(), 0);   // D3b fix round 1
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
        const bool restored =
            epochCache_.restoreFromFile(diskLog_->dir() + "/leader-epoch-checkpoint");
        // Fix round 1 (MEDIUM-5): a missing / corrupt checkpoint over a non-empty
        // recovered log is REBUILT from the batches' stamped epochs (never left
        // silently empty — an empty cache answers every validation as divergent).
        if (!restored && diskLog_->isRecovered() && logEndOffset_() > diskLog_->logStartOffset()) {
            rebuildEpochCacheFromLog_();
            if (epochCache_.latestEpoch() >= 0) checkpointEpochs_();
        }
        // DUR-1: the checkpoint is fsynced (tmp+fsync+rename) but, with periodic
        // fsync off, the log tail it describes may not have survived an unclean
        // shutdown (power loss) — recovery cut the log back.  An entry starting
        // PAST the recovered log end describes records that no longer exist and
        // poisons the cache: every later assign() at the (lower) LEO is silently
        // dropped as non-monotonic (A5), so a becomeLeader epoch is never
        // recorded and KIP-101 answers / reconcile lineage checks go wrong.
        // Kafka truncates the cache to the recovered LEO the same way
        // (truncateFromEnd(LEO): every entry with startOffset >= LEO goes — an
        // epoch with no surviving records names nothing).  Only against a
        // RECOVERED log (the LEO is the true log end): an un-recovered Log
        // reports its last segment's base and must never cost us epoch history.
        const Offset leo = logEndOffset_();
        const int32_t latest = epochCache_.latestEpoch();
        if (diskLog_->isRecovered() && latest >= 0 && epochCache_.startOffsetFor(latest) >= leo) {
            epochCache_.truncateFromEnd(leo);
            checkpointEpochs_();
        }
    }

    if (memoryBacked_ && epochCache_.latestEpoch() < 0 && logEndOffset_() > 0)
        rebuildEpochCacheFromLog_();   // a pre-populated in-memory log (no checkpoint)

    if (role_ == Role::Leader) {
        isrManagerOwned_ = std::make_unique<IsrManager>(replicas, cfg_.selfId, cfg_.replicaLagMaxMs);
        isrManagerOwned_->setIsr(isr);
        isrManager_ = isrManagerOwned_.get();

        // Durability: set the quorum rule and effective minIsr before seeding ISR/LEO.
        leaderState_.setDurabilityMode(cfg_.durability);
        effectiveMinIsr_ = computeEffectiveMinIsr_(static_cast<int>(replicas.size()));
        committedIsr_    = isr;

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
            if (!memoryBacked_ && diskLog_ != nullptr)
                diskLog_->setLeaderEpochStamp(static_cast<int32_t>(epoch_));
        }
    }
    // (Follower: the epoch cache holds only what the log's batches carry —
    //  Kafka builds it from the stamped batches on append, never from the
    //  epoch the replica was constructed / told about.)

    // Seed the cleaner-visible snapshots from the initial state.
    publishCleanerState();
}

// ---------------------------------------------------------------------------
// Role transitions
// ---------------------------------------------------------------------------

void PartitionReplica::becomeLeader(Epoch newEpoch, std::vector<NodeId> isr, int32_t partitionEpoch)
{
    // Same-epoch re-assert (a partitionEpoch-only ISR change re-applies the
    // assignment with the SAME leaderEpoch): the controller COMMITTED a new ISR
    // (our AlterIsr proposal, or its own change).  Only the ISR changes; nothing
    // else of this leadership — follower slots / push state, HW floors, the
    // epoch cache — is reset.
    // A leader is always a member of its own ISR (defensive: a controller
    // record must never hand a leader an ISR without itself).
    if (std::find(isr.begin(), isr.end(), cfg_.selfId) == isr.end()) isr.push_back(cfg_.selfId);
    if (role_ == Role::Leader && newEpoch == epoch_ && isrManager_ != nullptr) {
        applyCommittedIsr_(std::move(isr), partitionEpoch);
        publishCleanerState();
        return;
    }

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
    // as the IsrManager's replica set (see configuredReplicas_).
    std::vector<NodeId> replicas =
        !configuredReplicas_.empty() ? configuredReplicas_ : isr;
    if (isrManagerOwned_) {
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
    // D3b fix round 1: fetch positions are relearned in every leadership.
    std::fill(replicaFetchPos_.begin(), replicaFetchPos_.end(), Offset{0});
    // Kafka resetReplicaState (review 2026-10-04 CRITICAL-1): a new leadership
    // forgets every follower position learned in an earlier one — a member that
    // has not fetched in THIS leadership must never be counted (HW) or published
    // (controller positions) at a stale position.
    leaderState_.resetFollowers();
    clearPublishedFollowerPositions_();

    // Durability quorum: set the per-topic rule FIRST, before seeding the floor
    // and ISR/LEO (setDurabilityMode resets the floors).
    leaderState_.setDurabilityMode(cfg_.durability);

    // NB1/B2 fix: set the HW floor BEFORE setting the ISR and leader LEO (both
    // recompute HW eagerly; the floor must be in place first).
    leaderState_.setHighWatermarkFloor(committedHw);

    // DR-BUG-3: seed the min-ISR-gated committed floor from the committed HW
    // captured above (else the gated HW reads 0 while the ISR re-expands).
    if (committedHw > lastHealthyHw_) {
        lastHealthyHw_ = committedHw;
    }

    leaderState_.setIsr(isr);
    leaderState_.setLeaderLogEndOffset(logEndOffset_());

    effectiveMinIsr_ = computeEffectiveMinIsr_(static_cast<int>(replicas.size()));
    committedIsr_    = isr;
    isrProposal_     = IsrProposal{};
    committedPartitionEpoch_ = partitionEpoch;
    expandBackoffUntilNs_    = 0;
    // MAJOR-2/3 (internal quorum mode): the ISR handed in (every replica) is
    // only the HW-counting SEED of this leadership — it is never published as
    // eligible; the first published ISR names only members validated at
    // >= max(HW, electionLeo_) (see updateIsr_).
    internalSeedPending_ = internalQuorum_();
    internalIsrVersion_  = 0;
    internalIsrAcks_     = 0;
    internalFencedMask_  = 0;

    // Kafka makeLeader: the new epoch starts at our LEO; every batch we append
    // from now on carries it (Log restamp for the produce path; the replica's
    // own append paths stamp it explicitly).
    epochCache_.assign(static_cast<int32_t>(epoch_), logEndOffset_());
    if (!memoryBacked_ && diskLog_ != nullptr) {
        diskLog_->setLeaderEpochStamp(static_cast<int32_t>(epoch_));
        checkpointEpochs_();
    }

    lastIsrCheckNs_ = 0;

    // Kafka model (HW = min LEO over the ISR): every ISR member holds every
    // committed record and a leader is always elected from the ISR — this
    // leader holds every committed record and is writable at once.  (No
    // completeness gate, no adoption: a leader never takes records it did not
    // hold when elected.)
    electionLeo_ = logEndOffset_();
    followerSlots_.resetLeadership();   // conflation, push coverage, validation: per leadership

    // DR-BUG-6: reset the follower-lag TIME clock on the first leader doWork
    // (becomeLeader has no clock) so followers are not evicted at once with a
    // stale lastFetchTimeNs of 0.
    leaderLagClockNeedsReset_ = true;

    publishCleanerState();
}

void PartitionReplica::becomeFollower(Epoch leaderEpoch, NodeId leaderId)
{
    // Same-epoch re-assert of the same leader: nothing changes.
    if (role_ == Role::Follower && leaderEpoch == epoch_ && leaderId == leaderId_) {
        publishCleanerState();
        return;
    }

    // Kafka makeFollower: no truncation here.  Divergence is found and resolved
    // on the fetch path: our first fetch carries lastFetchedEpoch, the leader
    // validates it against its lineage and answers DivergingEpoch when it is
    // not on it (KIP-320/595 + the KIP-279 truncation loop).
    epoch_    = leaderEpoch;
    if (role_ != Role::Follower) ++leadershipGeneration_;   // F15 S4
    role_     = Role::Follower;
    leaderId_ = leaderId;
    followerHw_ = 0;
    followerSync_.reset();    // new leadership epoch → bootstrap fetch state afresh
    wedgedAtOffset_ = -1;     // F18: a new leadership may serve it (re-evaluated)
    if (!memoryBacked_ && diskLog_ != nullptr) diskLog_->setLeaderEpochStamp(-1);
    // Drop a phantom epoch entry (assigned at becomeLeader at our LEO, never
    // appended in): our lastFetchedEpoch must name the epoch of our last record.
    {
        const int32_t latest = epochCache_.latestEpoch();
        if (latest >= 0 && epochCache_.startOffsetFor(latest) >= logEndOffset_()) {
            epochCache_.truncateFromEnd(logEndOffset_());
            if (!memoryBacked_ && diskLog_ != nullptr) checkpointEpochs_();
        }
    }
    if (fsLogEnabled()) {
        spdlog::info("FollowerSync[node={} part={}] reset (becomeFollower epoch={} leader={})",
                     cfg_.selfId, cfg_.partitionId, leaderEpoch, leaderId);
    }
    lastFetchEmitNs_ = 0;
    followerSlots_.resetLeadership();
    clearPublishedFollowerPositions_();   // a follower publishes only its own position

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
    ++rtAppends_;
    // B1: a follower or an incomplete leader refuses EVERY append (acks=all or
    // not — the MetadataService's acks=false records too; every caller queues /
    // retries on a negative result, never ignores it).
    if (!acceptsProduce()) [[unlikely]] return kAppendNotLeader;

    if (!committedIsrMeetsMinIsr_(acksAll)) {
        return -1; // gated by min-ISR
    }

    Offset baseOffset = logEndOffset_();

    // Build a proper RecordBatch so the Log segment scanner works correctly.
    // Bind by const& — makeBatch returns a reused thread-local buffer; consumed
    // synchronously by the append below before any further makeBatch call.
    makeBatch_(baseOffset, records);
    // Kafka parity: the batch carries the epoch of the leader that appended it.
    rune::protocol::RecordBatchReader::stampLeaderEpoch(batchScratch_.data(),
                                                        static_cast<int32_t>(epoch_));
    const auto& batch = batchScratch_;
    // lastOffset = baseOffset + 0 (one record per batch = lastOffsetDelta=0).
    if (externalFollowerFlush_) {
        // Worker-driven replica (spec §3.1, every placement — F30): no inline
        // fsync on the worker (txn markers come through here).  Page-cache
        // append + a flush request the owning worker submits to its io backend
        // in the same pass.
        onLog([&](auto& L){ L.appendNoSync(batch.data(), batch.size(), baseOffset); });
        if (!memoryBacked_) followerFlushRequested_ = true;
    } else {
        onLog([&](auto& L){ L.append(batch, baseOffset); });
    }

    // Leader LEO is now baseOffset + 1.
    leaderState_.setLeaderLogEndOffset(logEndOffset_());
    // Recompute the HW (min LEO over the ISR; the leader is always a member).
    recomputeHw();

    // Mirror the advanced HW into the cleaner-visible atomic (release-store).
    publishCleanerState();

    // GR-10: push newly-appended bytes to ISR followers immediately.  This is the
    // DIRECT append API (used by tests and internal callers that append one batch
    // and expect it replicated without a separate doWork tick).  The PRODUCE HOT
    // PATH does NOT come through here — it uses onLeaderLogAppended(), whose push is
    // COALESCED in doWork() (that is the throughput fix).  A coalesced doWork push
    // won't double-send after this one: pushToFollowers advances each follower's
    // accepted cursor to the current LEO, so doWork's `leo > pushFloor_()` is false.
    if (cfg_.pushOnAppend) {
        (void)pushToFollowers();
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
    if (!acceptsProduce()) return kAppendNotLeader;              // retriable (B1: also while incomplete)
    if (!isControlBatch(controlBatch)) return kAppendInvalidBatch;
    ++rtAppends_;
    if (!committedIsrMeetsMinIsr_(acksAll)) return kAppendMinIsr;

    const int32_t lastOffsetDelta = rune::protocol::RecordBatchReader(
        controlBatch.data(), controlBatch.size()).lastOffsetDelta();
    Offset baseOffset = logEndOffset_();
    if (memoryBacked_) {
        // InMemoryLog stores the batch bytes verbatim: stamp the base offset
        // into the reused scratch (capacity persists — no steady-state alloc).
        batchScratch_.assign(controlBatch.begin(), controlBatch.end());
        const auto u = static_cast<uint64_t>(baseOffset);
        for (int i = 0; i < 8; ++i) batchScratch_[static_cast<std::size_t>(i)] = static_cast<uint8_t>(u >> (8 * i));
        rune::protocol::RecordBatchReader::stampLeaderEpoch(batchScratch_.data(),
                                                            static_cast<int32_t>(epoch_));
        if (memLog_->appendNoSync(batchScratch_.data(), batchScratch_.size(),
                                  baseOffset + lastOffsetDelta) < 0) return kAppendBackPressure;
    } else {
        // Zero-copy restamp (pwritev): the caller's bytes are never mutated.
        diskLog_->appendNoSyncRestamp(controlBatch.data(), controlBatch.size(), lastOffsetDelta, baseOffset);
        if (externalFollowerFlush_) {
            followerFlushRequested_ = true;   // the owning worker submits it (never inline)
        } else {
            diskLog_->sync();                 // append()'s durable-by-default fsync, unchanged
        }
    }

    leaderState_.setLeaderLogEndOffset(logEndOffset_());
    recomputeHw();
    publishCleanerState();
    if (cfg_.pushOnAppend) (void)pushToFollowers();
    return baseOffset;
}

// ---------------------------------------------------------------------------
// Leader: append with an explicit record key (CP-1 compaction support)
// ---------------------------------------------------------------------------

Offset PartitionReplica::appendKeyed(std::span<const uint8_t> key,
                                      std::span<const uint8_t> value,
                                      bool acksAll)
{
    if (!acceptsProduce()) [[unlikely]] return kAppendNotLeader;   // B1 (see append())

    if (!committedIsrMeetsMinIsr_(acksAll)) {
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
    rune::protocol::RecordBatchReader::stampLeaderEpoch(batch.data(), static_cast<int32_t>(epoch_));

    onLog([&](auto& L){ L.append(batch, baseOffset); });

    leaderState_.setLeaderLogEndOffset(logEndOffset_());
    recomputeHw();

    // Mirror the advanced HW into the cleaner-visible atomic (release-store).
    publishCleanerState();

    // Direct append API → immediate push (see append() note); hot path coalesces.
    if (cfg_.pushOnAppend) {
        (void)pushToFollowers();
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
    // replication push is now COALESCED in doWork() (pushToFollowers from each follower's cursor
    // drains every record appended since the last push in ONE batched message), so a
    // worker that drains many produce ops in a cycle replicates them as one batch.
    // Latency is unchanged at low load: doWork runs in the same worker cycle.
}

// ---------------------------------------------------------------------------
// Leader: onFetchRequest
// ---------------------------------------------------------------------------

int16_t PartitionReplica::onFetchRequest(const FetchRequestMsg& req, int64_t nowNs,
                                         uint32_t connGen)
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
        // F27: an error answer is not owed — the metadata role/epoch change that
        // fences this follower drives its recovery; a refusal is only counted.
        if (!onTransport([&](auto& t) { return t.sendFetchResponse(req.followerId, resp); }))
            ++sendRefusals_;
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
        clearPublishedFollowerPositions_();
        if (fsLogEnabled()) {
            spdlog::info("FollowerSync[node={} part={}] reset (self-demote to follower "
                         "epoch={} newLeaderGuess={})",
                         cfg_.selfId, cfg_.partitionId, req.leaderEpoch, req.followerId);
        }
        lastFetchEmitNs_ = 0;     // (DR epoch-probe cadence; separate from FollowerSync)

        // (No truncation / probe here: our next fetch carries lastFetchedEpoch
        //  and the real leader's validation drives any KIP-279 truncation.)
        if (!memoryBacked_ && diskLog_ != nullptr) diskLog_->setLeaderEpochStamp(-1);

        // Self-demotion: publish the role change so the cleaner stops treating
        // this node as Leader (no live-state read on the cleaner thread).
        publishCleanerState();

        // A-NEW-5: kErrNotLeader — the follower is ahead of this stale leader.
        auto resp = makeFetchResponse(committedHw, kErrNotLeader, {});
        // F27: not owed (see the stale-epoch answer above); counted.
        if (!onTransport([&](auto& t) { return t.sendFetchResponse(req.followerId, resp); }))
            ++sendRefusals_;
        return kErrNotLeader;
    }

    // KIP-903: a fetch from an OLDER process incarnation of this follower than
    // one already seen is a frame of a dead process (still in flight across the
    // restart): never credited, never answered.  A NEWER one is adopted; when it
    // replaces a known one, the follower restarted (checked below the HW).
    bool incarnationChanged = false;
    if (req.incarnation != 0) {
        if (FollowerSlot* const s0 = followerSlots_.find(req.followerId)) {
            if (req.incarnation < s0->incarnation) [[unlikely]] return kErrStaleIncarnation;
            if (req.incarnation > s0->incarnation) {
                incarnationChanged = s0->incarnation != 0;
                s0->incarnation    = req.incarnation;
                // Round-3 LOW-R3-1: a restarted replica may have lost (or failed
                // closed on) its internal-ISR knowledge — forget its ack so the
                // anti-entropy re-sends the latest state (cold: restarts only).
                if (incarnationChanged && internalQuorum_()) {
                    const int idx = slotIndex_(req.followerId);
                    if (idx >= 0) internalIsrAcks_ &= ~(1u << idx);
                }
            }
        }
    }

    // Mixed v2/v3 replication is UNSUPPORTED (fail closed): a v2 follower has
    // no lineage to validate — only an empty log (nothing to diverge) is served.
    FetchRequestMsg vreq = req;
    if (req.lastFetchedEpoch == kLastFetchedEpochV2Frame) [[unlikely]] {
        if (req.fetchOffset > 0) {
            if (v2RefusalsLogged_++ == 0)
                spdlog::critical("PartitionReplica[node={} topic={} part={}]: refusing a v2 (pre-"
                                 "KIP-320) FetchRequest from follower {} at offset {} — mixed v2/v3 "
                                 "replication is unsupported (no lineage to validate)",
                                 cfg_.selfId, cfg_.topicId, cfg_.partitionId, req.followerId,
                                 req.fetchOffset);
            auto ref = makeFetchResponse(highWatermark(), kErrUnsupportedWireVersion, {});
            if (!onTransport([&](auto& t) { return t.sendFetchResponse(req.followerId, ref); }))
                ++sendRefusals_;
            return kErrUnsupportedWireVersion;
        }
        vreq.lastFetchedEpoch = -1;
    }

    // KIP-320/595 — validate EVERY fetch against our epoch lineage.  The
    // follower's log ends at (fetchOffset, lastFetchedEpoch); it is on our
    // lineage iff we hold that epoch and it does not end before fetchOffset on
    // our side.  Otherwise answer DivergingEpoch = our endOffsetForEpoch(
    // lastFetchedEpoch) and credit NOTHING (ISR liveness, HW, push position): only a fetch that passed validation counts.
    if (vreq.lastFetchedEpoch >= 0) {
        const Offset leo = logEndOffset_();
        const auto ans = epochCache_.endOffsetForEpoch(vreq.lastFetchedEpoch, leo);
        if (ans.first < vreq.lastFetchedEpoch || ans.second < req.fetchOffset) [[unlikely]] {
            // We hold every committed record (we were elected from the ISR):
            // whatever the follower holds past our lineage is uncommitted —
            // KIP-279 truncation directive.
            ErrorPayload{ans.first, ans.second}.encode(errPayload_.data());
            auto div = makeFetchResponse(highWatermark(), kErrDivergingEpoch,
                                         std::span<const uint8_t>(errPayload_.data(), ErrorPayload::SIZE));
            if (!onTransport([&](auto& t) { return t.sendFetchResponse(req.followerId, div); }))
                ++sendRefusals_;       // not owed: the follower re-fetches (and re-learns it)
            return kErrDivergingEpoch;
        }
    } else if (req.fetchOffset > logEndOffset_()) [[unlikely]] {
        // No epoch to validate (an empty-log / unstamped-log follower) yet ahead
        // of us: Kafka's OFFSET_OUT_OF_RANGE, carrying our log end offset (the
        // follower truncates to it).  Never credited, never served.
        ErrorPayload{-1, logEndOffset_()}.encode(errPayload_.data());
        auto oor = makeFetchResponse(highWatermark(), kErrOutOfRange,
                                     std::span<const uint8_t>(errPayload_.data(), ErrorPayload::SIZE));
        if (!onTransport([&](auto& t) { return t.sendFetchResponse(req.followerId, oor); }))
            ++sendRefusals_;
        return kErrOutOfRange;
    }

    // FETCH CONFLATION + NO-DUP-FETCH (spec §3.5) — FollowerSlots.h: an IDENTICAL
    // request (same fetchOffset AND LEO) within kFetchResendNs is conflated — a
    // behind follower re-fetches the same range while its prior response is in
    // flight, and re-serving it would flood its ingress ring with duplicate apply
    // ops.  Decided BEFORE the liveness update (B-2): the decision is slot-local
    // (no ISR/HW input), and it lets the single log read below happen ONLY for a
    // Data serve — a conflated or ack-only fetch reads nothing, as before.
    FollowerSlot* const slot = followerSlots_.find(req.followerId);
    // DUR-1 / KIP-966: the follower's position in this leadership BEFORE this
    // fetch (-1 = none yet) — see the lost-acked-data check below.
    const Offset prevAckedLeo = slot ? slot->ackedLeo : -1;
    if (slot) {
        // F27 R1: a new connection generation = the link went down and back up;
        // frames "sent" meanwhile may be gone → rewind so they are re-sent from
        // this fetch (served Data right now, pushes resume from here).
        if (connGen != 0 && connGen != slot->connGen) {
            if (slot->connGen != 0) slot->rewindForReconnect(req.fetchOffset);
            slot->connGen = connGen;
        }
        // F27: the follower's own position in this leadership (its push cursor
        // floor), and a real fetch supersedes any answer still owed to it.
        slot->ackedLeo  = req.fetchOffset;
        slot->validated = true;   // passed lineage validation in this leadership
        // F2: only a request that SUPERSEDES the owed answer discharges it — a
        // data fetch, or a position beyond it.  A pure ack at the same offset
        // (e.g. after an HW-only answer arrived) must not clear an owed Data
        // answer, or the follower's catch-up would wait out its resend window.
        if (req.maxBytes > 0 || req.fetchOffset > slot->owed.fetchOffset)
            slot->owed = FollowerSlot::OwedAnswer{};
    }
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
                sendFetchAnswer_(slot, req.followerId, resp, req.fetchOffset, req.maxBytes,
                                 /*data=*/false, /*dataEnd=*/-1);
                return kErrRecordTooLarge;
            }
        }
    }

    // Record follower fetch: update ISR manager + leader state.
    // Pass the REAL nowNs so time-lag eviction uses the actual fetch time,
    // not the stale last-ISR-check timestamp.
    isrManager_->onFollowerFetch(req.followerId, req.fetchOffset, nowNs, logEndOffset_());
    const Offset prevHwOnFetch = leaderState_.highWatermark();
    leaderState_.updateFollowerFetchOffset(req.followerId, req.fetchOffset);
    noteReplicaFetchPos_(req.followerId, req.fetchOffset);   // D3b fix round 1: marker-removal bound
    // HW is already recomputed inside updateFollowerFetchOffset().
    // FR: acks=all COMMIT point — this follower position update just lifted the
    //     high-watermark (min LEO over the ISR). Keyed by (partition<<24)|committedOffset so it
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

    // DUR-1 / KIP-966 / KIP-903 (data plane): a follower whose validated fetch
    // lies BELOW the HW lost committed records (an unclean shutdown: its
    // unflushed page cache is gone; periodic fsync is off by default).  Proof:
    //   - its position went backwards within this leadership, or
    //   - its process incarnation changed (it restarted), or
    //   - it is a committed-ISR member (review 2026-10-04 MEDIUM-4): every
    //     member holds everything below the HW (HW = min over the ISR; a new
    //     leader's HW floor never exceeds the committed offset) — so this holds
    //     even when this leader has no history of it (slot reset, or the leader
    //     changed while it restarted).
    // It must neither count toward the HW nor satisfy min-ISR, nor stay
    // electable: evict it now (it would otherwise stay "in sync" — it keeps
    // fetching and its offset lag is small).  It re-enters through expansion
    // once its LEO is back at max(HW, electionLeo_).  Cold path.
    if (req.fetchOffset < prevHwOnFetch) [[unlikely]] {
        const bool committedMember =
            std::find(committedIsr_.begin(), committedIsr_.end(), req.followerId) != committedIsr_.end();
        if (slot != nullptr && !slot->unclean &&
            (prevAckedLeo > req.fetchOffset || incarnationChanged || committedMember)) {
            slot->unclean = true;   // kept out of the ISR until caught up
            if (isrManager_->inIsr(req.followerId)) {   // it was counted: it lost committed records
                spdlog::warn("PartitionReplica[node={} topic={} part={}]: ISR member {} fetched at "
                             "{} below the HW {} (previous position {}, restarted={}) — it lost "
                             "committed records (unclean restart): removed from the ISR until "
                             "caught up", cfg_.selfId, cfg_.topicId, cfg_.partitionId,
                             req.followerId, req.fetchOffset, prevHwOnFetch, prevAckedLeo,
                             incarnationChanged);
                updateIsr_(nowNs);
            }
        }
    } else if (slot != nullptr && slot->unclean) {
        slot->unclean = false;   // caught up again (validated): may re-enter via expansion
    }

    // Conflated (decided above): an identical response is still in flight — ISR
    // liveness + HW were updated above, so skipping the RESPONSE is safe.  F27:
    // only an answer that was actually SENT is ever conflated against (a refused
    // one is forgotten + owed, see sendFetchAnswer_).
    if (serve == FetchServe::Conflate) return kErrNone;

    // Read records (Data only; AckOnly answers the HW with no records).
    // Zero-copy: the ONE view read above (B-2) — a view into the log (disk mmap /
    // memory ring).  The transport copies the bytes ONCE, straight into the
    // publication term.  (No append/truncate/readView ran in between.)
    std::span<const uint8_t> records{};
    Offset servedEnd = -1;   // F6: exclusive end of the Data served (if any)
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
        servedEnd = pre.endOffset;
    }

    // DR-BUG-3: advertise the min-ISR-GATED committed HW to followers via the
    // gated accessor.  followerHw_ = min(localLeo, resp.highWatermark), so a
    // rejoining follower never treats a minority leader's uncommitted tail as
    // committed.  Note this does NOT bound catch-up: the log read above is
    // intentionally NOT HW-bounded, so the follower still fetches (and applies)
    // the uncommitted tail and advances its own LEO — which is exactly what
    // lets the leader's real HW (min over the ISR) advance and the ISR re-expand.  Only
    // the follower's *committed view* (followerHw_) is held at the min-ISR-gated floor
    // until the ISR heals.
    const Offset advertiseHw = highWatermark();
    auto resp = makeFetchResponse(advertiseHw, kErrNone, records);
    if (sendFetchAnswer_(slot, req.followerId, resp, req.fetchOffset, req.maxBytes,
                         /*data=*/!records.empty(), records.empty() ? -1 : servedEnd) &&
        !records.empty())
        ++fetchDataServes_;   // counts answers that actually LEFT (F27)

    return kErrNone;
}

// ---------------------------------------------------------------------------
// F27: fetch answers are never silently dropped.  A refused answer (Aeron
// back-pressure / not connected) is forgotten as served — an identical retry is
// never conflated against it — and OWED: retryOwedFetchAnswers_() re-sends it on
// the leader's next pass, so neither side waits out a resend timer.
// ---------------------------------------------------------------------------
bool PartitionReplica::sendFetchAnswer_(FollowerSlot* slot, NodeId follower,
                                        const FetchResponseMsg& resp, Offset fetchOffset,
                                        int32_t maxBytes, bool data, Offset dataEnd)
{
    const bool ok = onTransport([&](auto& t) { return t.sendFetchResponse(follower, resp); });
    if (ok) [[likely]] {
        if (slot && dataEnd > slot->dataServedThrough) slot->dataServedThrough = dataEnd;   // F6
        return true;
    }
    ++sendRefusals_;
    if (slot != nullptr) {
        slot->forgetServe();
        slot->owed = FollowerSlot::OwedAnswer{fetchOffset, maxBytes, data, resp.errorCode};
    }
    return false;
}

int PartitionReplica::retryOwedFetchAnswers_() noexcept
{
    int sent = 0;
    for (std::size_t i = 0; i < followerSlots_.size(); ++i) {
        FollowerSlot& s = followerSlots_.at(i);
        if (s.owed.fetchOffset < 0 || s.id == cfg_.selfId) [[likely]] continue;
        const FollowerSlot::OwedAnswer owed = s.owed;
        // Re-derive the answer from the log NOW (zero-copy view, same whole-batch
        // walk as onFetchRequest).  No ISR liveness refresh: only a real fetch is
        // evidence the follower is alive.
        rune::storage::LogReadView view{};
        std::span<const uint8_t> records{};
        Offset dataEnd = -1;
        const Offset leo = logEndOffset_();
        if (owed.data && owed.fetchOffset < leo) {
            // The view itself is re-taken every attempt (cheap; never cached across
            // passes — a cleaner swap may remap the segment between them).
            view = onLog([&](auto& L) { return L.readView(owed.fetchOffset, owed.maxBytes); });
            if (view.empty()) [[unlikely]] continue;   // transient read-map failure: next pass
            // F7 / R2: the whole-batch walk is memoised only while the LEO, the
            // view (same mapping, same length) AND the log's segment install
            // generation are unchanged — a compaction swap at the same LEO
            // rewrites the bytes below it and must rebuild (never a torn batch).
            const uint64_t installGen = onLog([&](auto& L) { return L.installGeneration(); });
            if (s.owed.builtLeo != leo || s.owed.builtView != view.data ||
                s.owed.builtLen != view.len || s.owed.builtInstallGen != installGen) {
                ++owedRebuilds_;
                const BatchPrefix pre = wholeBatchPrefix(view.data, view.len,
                                                         static_cast<std::size_t>(owed.maxBytes),
                                                         kNoPushCeiling);
                s.owed.builtLeo        = leo;
                s.owed.builtBytes      = pre.bytes;
                s.owed.builtEnd        = pre.endOffset;
                s.owed.builtView       = view.data;
                s.owed.builtLen        = view.len;
                s.owed.builtInstallGen = installGen;
            }
            records = std::span<const uint8_t>(view.data, s.owed.builtBytes);
            dataEnd = s.owed.builtEnd;
        }
        auto resp = makeFetchResponse(highWatermark(), owed.errorCode, records);
        const bool ok = onTransport([&](auto& t) { return t.sendFetchResponse(s.id, resp); });
        if (ok) {
            s.owed = FollowerSlot::OwedAnswer{};
            if (dataEnd > s.dataServedThrough) s.dataServedThrough = dataEnd;   // F6
            if (!records.empty()) ++fetchDataServes_;
            ++sent;
        } else {
            ++sendRefusals_;
        }
    }
    return sent;
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
        // KIP-320/595: our (fetchOffset, lastFetchedEpoch) is off the leader's
        // lineage — KIP-279 truncation, then re-fetch.
        ErrorPayload ep{};
        if (resp.errorCode == kErrDivergingEpoch && role_ == Role::Follower &&
            resp.leaderEpoch == epoch_ && ErrorPayload::decode(resp.records, ep) &&
            ep.epoch >= -1 && ep.endOffset >= 0) {
            onDivergingEpoch_(ep.epoch, ep.endOffset);
        }
        // OffsetOutOfRange (we fetched without an epoch — empty or unstamped log —
        // and are ahead of the leader): Kafka truncates to the leader's log end.
        if (resp.errorCode == kErrOutOfRange && role_ == Role::Follower &&
            resp.leaderEpoch == epoch_ && ErrorPayload::decode(resp.records, ep) &&
            ep.endOffset >= 0 && ep.endOffset < logEndOffset_()) {
            onDivergingEpoch_(/*epoch=*/-1, ep.endOffset);
        }
        if (resp.errorCode == kErrUnsupportedWireVersion && v2RefusalsLogged_++ == 0) {
            spdlog::critical("PartitionReplica[node={} topic={} part={}]: dropped a v2 (pre-KIP-320) "
                             "replication frame from leader {} — mixed v2/v3 replication is "
                             "unsupported", cfg_.selfId, cfg_.topicId, cfg_.partitionId, leaderId_);
        }
        // Other errors: ignored (the control plane drives role changes).
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
    // A frame from a NEWER epoch than the one the control plane gave us is not
    // ours to apply yet (Kafka: FENCED/UNKNOWN_LEADER_EPOCH): no truncation or
    // epoch adoption here — once we follow that epoch, our fetch (validated by
    // its leader) brings the data.
    if (resp.leaderEpoch > epoch_) return;

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
                // Kafka LeaderEpochFileCache.assign on append: the cache is built
                // from the epochs the leader STAMPED on the batches.
                if (assignEpochsFromBatches_(raw.data(), raw.size()) &&
                    !memoryBacked_ && diskLog_ != nullptr)
                    checkpointEpochs_();
            }
        } else {
            // Fallback: raw user bytes (e.g. injected by unit tests) — wrap in
            // a RecordBatch so the log segment scanner can parse them.
            Offset baseOffset = logEndOffset_();
            // Merge: keep main's zero-alloc makeBatch_ (reused buffer) + multinode
            // throughput fix (appendNoSync; coalesced sync via followerApplyDirty_).
            makeBatch_(baseOffset, resp.records);
            rune::protocol::RecordBatchReader::stampLeaderEpoch(batchScratch_.data(),
                                                                static_cast<int32_t>(resp.leaderEpoch));
            const auto& batch = batchScratch_;
            onLog([&](auto& L){ L.appendNoSync(batch.data(), batch.size(), baseOffset); });
            followerApplyDirty_ = true;
            if (assignEpochsFromBatches_(batch.data(), batch.size()) &&
                !memoryBacked_ && diskLog_ != nullptr)
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
        // F27: a refused ack is OWED and re-sent on the next doWork pass (the
        // acks=all commit point depends on it — never wait for a fetch timer).
        if (sendFetchToLeader_(ack)) followerSync_.onFetchSent();
        else { followerSync_.onAckRefused(); ++sendRefusals_; }
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
bool PartitionReplica::sendFetchToLeader_(const FetchRequestMsg& req)
{
    if (leaderId_ >= 0 && leaderId_ != cfg_.selfId)
        return onTransport([&](auto& t) { return t.sendFetchRequestTo(leaderId_, req); });
    return onTransport([&](auto& t) { return t.sendFetchRequest(req); });
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
    req.lastFetchedEpoch = lastFetchedEpoch();   // KIP-320/595
    req.incarnation      = processIncarnation_;  // KIP-903
    return req;
}

int32_t PartitionReplica::lastFetchedEpoch() const noexcept
{
    const Offset leo = logEndOffset_();
    if (leo <= 0) return -1;
    const int32_t latest = epochCache_.latestEpoch();
    if (latest < 0) return -1;
    // Common case: the latest epoch owns records (it starts below our LEO).  A
    // leader-turned-follower may hold an epoch it never appended in (assigned
    // at becomeLeader at its LEO): fall back to the epoch owning LEO-1.
    if (epochCache_.startOffsetFor(latest) < leo) return latest;
    return epochCache_.epochForOffset(leo - 1);
}

// Cold: rebuild the epoch cache from the stamped batches of the whole log.
void PartitionReplica::rebuildEpochCacheFromLog_()
{
    Offset at = onLog([](auto& L) { return L.logStartOffset(); });
    const Offset leo = logEndOffset_();
    for (int guard = 0; at < leo && guard < (1 << 20); ++guard) {
        // Copying read (cold): never leaves sealed segments mapped (lazy maps).
        const std::vector<uint8_t> buf = onLog([&](auto& L) { return L.read(at, 1 << 20); });
        if (buf.empty()) break;
        struct { const uint8_t* data; std::size_t len; } v{buf.data(), buf.size()};
        (void)assignEpochsFromBatches_(v.data, v.len);
        // Advance past the last whole batch in the view.
        Offset next = at;
        std::size_t pos = 0;
        while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= v.len) {
            rune::protocol::RecordBatchReader r(v.data + pos, v.len - pos);
            const int32_t total = r.totalLength();
            if (total <= 0 || pos + static_cast<std::size_t>(total) > v.len) break;
            next = r.baseOffset() + r.lastOffsetDelta() + 1;
            pos += static_cast<std::size_t>(total);
        }
        if (next <= at) break;
        at = next;
    }
}

// Kafka LeaderEpochFileCache.assign on append: each batch whose stamped epoch
// is newer than the latest cached one starts that epoch at its base offset.
bool PartitionReplica::assignEpochsFromBatches_(const uint8_t* p, std::size_t len)
{
    bool changed = false;
    std::size_t pos = 0;
    while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= len) {
        rune::protocol::RecordBatchReader r(p + pos, len - pos);
        const int32_t total = r.totalLength();
        if (total <= 0 || pos + static_cast<std::size_t>(total) > len) break;
        const int32_t ep = r.partitionLeaderEpoch();
        if (ep >= 0 && ep > epochCache_.latestEpoch()) {
            epochCache_.assign(ep, r.baseOffset());
            changed = true;
        }
        pos += static_cast<std::size_t>(total);
    }
    return changed;
}

// KIP-279 truncation on a DivergingEpoch answer (Kafka
// AbstractFetcherThread.getOffsetTruncationState): the leader's lineage for
// our last epoch ends at (divergingEpoch, divergingEndOffset).
//   - we hold divergingEpoch too → truncate to min(its end on the leader, its
//     end on our side): everything past that point diverges;
//   - we never held it → truncate to our own end of the largest epoch below it
//     (we may diverge earlier still): the next fetch, carrying the older epoch,
//     runs another round.
// Then re-fetch at once (the loop converges in at most one round per epoch).
void PartitionReplica::onDivergingEpoch_(int32_t divergingEpoch, Offset divergingEndOffset)
{
    const Offset leo = logEndOffset_();
    const auto mine = epochCache_.endOffsetForEpoch(divergingEpoch, leo);
    // A log with no epoch history at all (unstamped legacy batches): Kafka's
    // "no local epoch info" case — truncate to the leader's end offset.
    Offset truncTo = (divergingEpoch < 0 || epochCache_.latestEpoch() < 0)
        ? divergingEndOffset
        : (mine.first == divergingEpoch)
            ? std::min<Offset>(divergingEndOffset, mine.second)
            : mine.second;
    if (truncTo > leo) truncTo = leo;
    if (truncTo < 0) truncTo = 0;
    if (truncTo < followerHw_) [[unlikely]] {
        // Kafka logs and truncates: a complete leader's lineage ends below what
        // we were told is committed — only an election of a replica lacking
        // committed records (controller side, E1b) leads here.
        spdlog::critical("PartitionReplica[node={} topic={} part={}]: leader {} (epoch {}) lineage "
                         "ends at {} (epoch {}) below our known committed offset {} (LEO {}) — "
                         "truncating (an unclean / deficient election happened upstream)",
                         cfg_.selfId, cfg_.topicId, cfg_.partitionId, leaderId_,
                         static_cast<long long>(epoch_), divergingEndOffset,
                         divergingEpoch, followerHw_, leo);
    }
    if (truncTo < leo) truncateLog(truncTo);   // prunes the epoch cache in step (A10)
    followerSync_.reset();
    lastFetchEmitNs_ = 0;
    if (sendFetchToLeader_(nextFetch())) followerSync_.onFetchSent();
    else ++sendRefusals_;
}

// ---------------------------------------------------------------------------
// OffsetsForLeaderEpoch (KIP-101) — authoritative answer only.
// ---------------------------------------------------------------------------
void PartitionReplica::onOffsetsForLeaderEpochRequest(const OffsetsForLeaderEpochReqMsg& req)
{
    OffsetsForLeaderEpochRespMsg resp{};
    resp.leaderEpoch = epoch_;
    resp.partition   = cfg_.partitionId;
    resp.topicId     = cfg_.topicId;
    if (role_ != Role::Leader) {
        resp.responseEpoch = -1;
        resp.endOffset     = 0;
        resp.errorCode     = kErrNotLeader;
    } else {
        const auto ans = epochCache_.endOffsetForEpoch(
            static_cast<int32_t>(req.requestedEpoch), logEndOffset_());
        resp.responseEpoch = ans.first;
        resp.endOffset     = ans.second;
        resp.errorCode     = kErrNone;
    }
    sendEpochAnswer_(req.followerId, resp);
}

void PartitionReplica::applyOffsetsForLeaderEpochResponse(const OffsetsForLeaderEpochRespMsg&)
{
    // Retired: this build never probes (divergence is resolved on the fetch path).
}

void PartitionReplica::onReconcileRequest(const FetchRequestMsg&)
{
    // Retired DR-BUG-6 reverse reconcile: a leader never takes records it did
    // not hold when elected, so no pull is ever served.
}

void PartitionReplica::applyReconcileResponse(const FetchResponseMsg&)
{
    // Retired DR-BUG-6 reverse reconcile (see onReconcileRequest).
}

// ---------------------------------------------------------------------------
// F27 F3: an epoch answer the transport refused is OWED and re-sent on the next
// doWork pass.
// ---------------------------------------------------------------------------
void PartitionReplica::sendEpochAnswer_(NodeId to, const OffsetsForLeaderEpochRespMsg& m)
{
    FollowerSlot* const slot = followerSlots_.find(to);
    if (slot) slot->owedEpoch = FollowerSlot::OwedEpochAnswer{};   // superseded by this one
    if (onTransport([&](auto& t) { return t.sendOffsetsForLeaderEpochResponse(to, m); })) [[likely]]
        return;
    ++sendRefusals_;
    if (slot) slot->owedEpoch = FollowerSlot::OwedEpochAnswer{true, m};
}

int PartitionReplica::retryOwedDrSends_() noexcept
{
    int sent = 0;
    for (std::size_t i = 0; i < followerSlots_.size(); ++i) {
        FollowerSlot& s = followerSlots_.at(i);
        if (!s.owedEpoch.valid) [[likely]] continue;
        const OffsetsForLeaderEpochRespMsg m = s.owedEpoch.msg;
        if (onTransport([&](auto& t) { return t.sendOffsetsForLeaderEpochResponse(s.id, m); })) {
            s.owedEpoch = FollowerSlot::OwedEpochAnswer{};
            ++sent;
        } else {
            ++sendRefusals_;
        }
    }
    return sent;
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
    // cfg_.followerFlushIntervalNs (default 20 ms; the broker passes its NodeConfig value, OFF by
    // default — durability via replication, Kafka model).  0 = never.
    if (followerApplyDirty_ && !memoryBacked_ && cfg_.followerFlushIntervalNs > 0
        && nowNs - lastFollowerSyncNs_ >= cfg_.followerFlushIntervalNs) {
        followerApplyDirty_ = false;
        lastFollowerSyncNs_ = nowNs;
        if (externalFollowerFlush_) {
            // Worker-driven (every placement): the owning worker submits it
            // to its io backend (never blocks the worker).  Raised at most
            // once per interval.
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

    // F27 F3: owed DR frames (epoch answers, reconcile pull / answer) — any role.
    work += retryOwedDrSends_();

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
        // F27: owed fetch answers (refused by the transport) go out first.
        work += retryOwedFetchAnswers_();
        const Offset pushFloor = cfg_.pushOnAppend ? pushFloor_() : logEndOffset_();
        if (cfg_.pushOnAppend && logEndOffset_() > pushFloor) {
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
            if (pushCeiling > pushFloor &&
                !unsendablePushSuppressed_(pushCeiling, pushFloor)) {
                // F27: work only for ACCEPTED pushes — a follower whose pushes are
                // refused (back-pressured / not connected) is retried every pass,
                // but lets the idle strategy back off instead of spinning a core.
                if (pushToFollowers(pushCeiling, pushFloor) > 0) ++work;
            }
        }

        if (nowNs - lastIsrCheckNs_ >= cfg_.isrCheckIntervalNs) {
            lastIsrCheckNs_ = nowNs;
            // NOTE: the bare ISR-check TICK is NOT work — counting it made the
            // idle-strategy see work>0 every 10ms and never park (busy-spin on an
            // idle/no-follower leader).  Only count work when the check ACTUALLY
            // changes ISR state or emits a reconcile (below).

            if (updateIsr_(nowNs)) ++work;

            // Change-gated diagnostic (cheap): only log when the ISR size changes.
            if (fsLogEnabled()) {
                const int isrSz = static_cast<int>(committedIsr_.size());
                if (isrSz != lastLoggedIsrSize_) {
                    lastLoggedIsrSize_ = isrSz;
                    spdlog::info("ISRChange[node={} part={}] committed isr {} (maximal {}) hw={} leo={}",
                                 cfg_.selfId, cfg_.partitionId, isrSz, isrManager_->isr().size(),
                                 leaderState_.highWatermark(), logEndOffset_());
                }
            }

        }
    } else {
        // F18 (I-1): a wedge is bound to the offset — a truncation that moved our
        // LEO clears it (time-independent: no timer).  While wedged the follower
        // sends NOTHING (no fetch, no keepalive), so the leader's lag check evicts it.
        if (wedgedAtOffset_ >= 0 && wedgedAtOffset_ != logEndOffset_()) wedgedAtOffset_ = -1;
        {
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
                // F27: a refused fetch never left — undo the emit so the NEXT pass
                // re-sends it (no resend-timer wait for our own refusal).
                const bool sent = sendFetchToLeader_(req);
                if (sent) followerSync_.onFetchSent();
                else { followerSync_.onFetchRefused(); ++sendRefusals_; }
                // F4: real catch-up counts as work only when it LEFT — a refused
                // fetch is retried next pass but lets the idle strategy back off.
                if (sent && act.wantData) ++work;
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
            // F27: re-send a fast-ack the transport refused (the leader's acks=all
            // HW waits on it).  A fetch sent above carries the same LEO and has
            // already discharged it.
            if (!wedged && followerSync_.ackOwed()) {
                FetchRequestMsg ack = nextFetch();
                ack.maxBytes = 0;
                if (sendFetchToLeader_(ack)) { followerSync_.onFetchSent(); ++work; }
                else ++sendRefusals_;
            }
        }
    }

    // Publish the current served HW + role for the cleaner (release-store).
    // This is the steady-state publication site: it covers every HW/ISR/
    // role change made on this duty tick (ISR shrink/expand, fetch-driven HW
    // advances) so the cleaner's acquire-load
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

    // Raw HW from the leader state: min LEO over the (maximal) ISR.
    const Offset rawHw = leaderState_.highWatermark();

    // DR-BUG-3: min-ISR COMMIT gate on the exposed HW.  While the ISR is BELOW
    // effectiveMinIsr_ the effective committed HW must not advance past the last offset
    // committed while the ISR was healthy (lastHealthyHw_).
    // effectiveMinIsr_ == cfg_.minIsr (Majority) or RF (AllInSync).
    const int minIsr  = (effectiveMinIsr_ >= 1) ? effectiveMinIsr_ : 1;
    const int isrSize = static_cast<int>(committedIsr_.size());

    if (isrSize >= minIsr) {
        // Healthy ISR: rawHw is committed on every ISR member (>= minIsr of them).
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
    if (role_ == Role::Leader) {
        const int minIsr  = (effectiveMinIsr_ >= 1) ? effectiveMinIsr_ : 1;
        const int isrSize = static_cast<int>(committedIsr_.size());
        const Offset rawHw = leaderState_.highWatermark();
        if (isrSize >= minIsr) {
            if (rawHw > lastHealthyHw_) lastHealthyHw_ = rawHw;
        } else {
            // DR-BUG-3 × failover (was the completion-time recovery): a new
            // leader seeds the healthy floor from its own possibly-stale follower
            // view.  Records it held AT ELECTION that every current ISR member
            // now holds (raw HW = min over the ISR) were written by the previous
            // leader to its whole ISR — re-expose them even while the ISR sits
            // below min-ISR (AllInSync after a node death), never anything
            // appended since (that waits for a healthy ISR).
            const Offset recovered = std::min(rawHw, electionLeo_);
            if (recovered > lastHealthyHw_) lastHealthyHw_ = recovered;
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
        ? static_cast<int>(committedIsr_.size())
        : 0;
    publishedIsrSizeForGateway_.store(isrSize, std::memory_order_release);

    // D3b fix round 1: the cleaner's marker-removal bound — the minimum fetch
    // position over EVERY assigned replica (not just the ISR; a replica that never
    // fetched in this leadership counts as 0).  A follower publishes 0 (it never cleans).
    Offset markerBound = 0;
    if (role_ == Role::Leader) {
        markerBound = kNoCleanerLso;
        for (std::size_t i = 0; i < configuredReplicas_.size(); ++i)
            if (configuredReplicas_[i] != cfg_.selfId) markerBound = std::min(markerBound, replicaFetchPos_[i]);
    }
    publishedMarkerBoundForCleaner_.store(markerBound, std::memory_order_release);

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
        // Only followers whose fetch passed validation in THIS leadership
        // (CRITICAL-1): an unvalidated follower's position is unknown here.
        for (const auto& [fid, leo] : leaderState_.followerLeos()) {
            if (fid < 0 || fid >= kMaxControllerNodes) continue;
            const FollowerSlot* sl = followerSlots_.find(fid);
            if (sl == nullptr || !sl->validated) continue;
            publishedReplicaLeo_[static_cast<std::size_t>(fid)].store(
                leo + 1, std::memory_order_release);
        }
    }
}

void PartitionReplica::clearPublishedFollowerPositions_() noexcept
{
    for (int n = 0; n < kMaxControllerNodes; ++n) {
        if (n == cfg_.selfId) continue;
        publishedReplicaLeo_[static_cast<std::size_t>(n)].store(0, std::memory_order_release);
    }
}

const std::vector<NodeId>& PartitionReplica::maximalIsrForTest() const noexcept
{
    static const std::vector<NodeId> kEmpty{};
    return (role_ == Role::Leader && isrManager_) ? isrManager_->isr() : kEmpty;
}

const std::vector<NodeId>& PartitionReplica::isr() const noexcept
{
    static const std::vector<NodeId> kEmpty{};
    if (role_ == Role::Leader && isrManager_) {
        return committedIsr_;   // the controller-committed ISR (min-ISR, elections)
    }
    return kEmpty;
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ISR management — Kafka model (HW = min LEO over the maximal ISR).
//
// The COMMITTED ISR (committedIsr_) is the controller's: the controller elects
// only from it, and min-ISR (acks=all) is checked against it.  A leader with an
// ISR channel (isrChannel_, production / the DR harness) never changes it on its
// own: it PROPOSES a change (AlterIsr, Kafka AlterPartition) and keeps computing
// the HW over the MAXIMAL ISR = committed ∪ proposed (KIP-497/841): a member
// being shrunk still holds the HW back until the controller has committed its
// removal (so it can no longer be elected while records past its LEO are
// committed), a member being added counts at once (it was validated at >= HW).
// Without a channel (single node, unit rigs) the leader is its own controller:
// a change commits immediately.
// ---------------------------------------------------------------------------
int PartitionReplica::computeEffectiveMinIsr_(int rf) const noexcept
{
    if (cfg_.durability == DurabilityMode::AllInSync) return rf;
    const int m = cfg_.minIsr;
    return m < 1 ? 1 : m;
}

bool PartitionReplica::committedIsrMeetsMinIsr_(bool acksAll) const noexcept
{
    if (!acksAll) return true;
    return static_cast<int>(committedIsr_.size()) >= effectiveMinIsr_;
}

namespace {
bool sameMembers_(const std::vector<NodeId>& a, const std::vector<NodeId>& b) noexcept {
    if (a.size() != b.size()) return false;
    for (NodeId id : a)
        if (std::find(b.begin(), b.end(), id) == b.end()) return false;
    return true;
}
} // namespace

void PartitionReplica::applyCommittedIsr_(std::vector<NodeId> isr, int32_t partitionEpoch)
{
    // MEDIUM-1: the controller commits a proposal only while its base
    // partitionEpoch is the committed one — so ANY committed assignment with a
    // different partitionEpoch resolves the pending proposal (it was committed,
    // or it can never commit any more): the maximal ISR collapses to the new
    // committed one and the next check re-derives from it.
    // Without a partitionEpoch (not controller-managed): a committed CHANGE of
    // members resolves it; a same-members re-assert keeps it pending.
    const bool changed = !sameMembers_(isr, committedIsr_);
    committedIsr_ = std::move(isr);
    if (partitionEpoch >= 0) {
        if (isrProposal_.pending && partitionEpoch != isrProposal_.basePartitionEpoch)
            isrProposal_.pending = false;
        committedPartitionEpoch_ = partitionEpoch;
    } else if (changed || (isrProposal_.pending && sameMembers_(isrProposal_.isr, committedIsr_))) {
        isrProposal_.pending = false;
    }
    applyMaximalIsr_();
}

void PartitionReplica::onAlterIsrResponse(const AlterIsrResponseMsg& r)
{
    if (role_ != Role::Leader || isrManager_ == nullptr || !isrProposal_.pending) return;
    if (r.leaderEpoch != epoch_) return;                         // an answer for another leadership
    bool clear = false;
    switch (r.errorCode) {
        case kAlterIsrIneligibleReplica:
            // Nothing committed: the controller dropped the fenced additions and
            // the rest equals the committed ISR (answered for OUR base only).
            clear = (r.partitionEpoch == isrProposal_.basePartitionEpoch);
            if (clear) expandBackoffUntilNs_ = lastKnownNowNs_ + kAlterIsrResendNs;
            break;
        case kAlterIsrInvalidUpdateVersion:
            // Our base is not the committed partitionEpoch: the proposal can never
            // commit; the newer assignment (already applied, or on its way) is
            // what the next proposal derives from.
            clear = (r.partitionEpoch != isrProposal_.basePartitionEpoch);
            break;
        case kAlterIsrFencedLeaderEpoch:
        case kAlterIsrInvalidRequest:
            clear = true;
            break;
        default:   // kAlterIsrOk: resolved by the applied assignment
            break;
    }
    if (!clear) return;
    isrProposal_.pending = false;
    applyMaximalIsr_();
    publishCleanerState();
}

void PartitionReplica::applyMaximalIsr_()
{
    isrManager_->setIsr(committedIsr_);
    if (isrProposal_.pending)
        for (NodeId id : isrProposal_.isr) (void)isrManager_->addToIsr(id);
    syncIsrToLeaderState();
    recomputeHw();
}

void PartitionReplica::shrinkIsrToLive(std::span<const int32_t> live, int64_t nowNs)
{
    if (role_ != Role::Leader || isrManager_ == nullptr || live.empty()) return;
    if (internalQuorum_()) {
        // Internal quorum mode: the dead are FENCED (decided out of the next
        // proposal, never published as members) until the cluster sees them
        // live again; they re-enter through the normal validated expansion.
        uint32_t mask = 0;
        for (std::size_t i = 0; i < followerSlots_.size(); ++i) {
            FollowerSlot& sl = followerSlots_.at(i);
            if (sl.id == cfg_.selfId || std::find(live.begin(), live.end(), sl.id) != live.end()) continue;
            mask |= (1u << i);
            sl.validated = false;   // must prove a validated position again to re-enter
        }
        if (mask != internalFencedMask_) {
            internalFencedMask_ = mask;
            (void)updateIsr_(nowNs);
        }
        return;
    }
    bool anyDead = false;
    for (NodeId id : committedIsr_)
        if (id != cfg_.selfId && std::find(live.begin(), live.end(), id) == live.end()) anyDead = true;
    if (!anyDead) return;
    for (std::size_t i = 0; i < followerSlots_.size(); ++i) {
        FollowerSlot& sl = followerSlots_.at(i);
        if (sl.id != cfg_.selfId && std::find(live.begin(), live.end(), sl.id) == live.end())
            sl.validated = false;   // must prove a validated position again to re-enter
    }
    std::vector<NodeId> desired;   // cold: only when a member died
    desired.reserve(committedIsr_.size());
    for (NodeId id : committedIsr_)
        if (id == cfg_.selfId || std::find(live.begin(), live.end(), id) != live.end()) desired.push_back(id);
    (void)changeIsr_(desired, nowNs);
}

bool PartitionReplica::updateIsr_(int64_t nowNs)
{
    if (role_ != Role::Leader || isrManager_ == nullptr) return false;
    if (internalQuorum_() && !isrProposal_.pending && internalIsrVersion_ > 0 &&
        nowNs - isrProposal_.sentNs >= kAlterIsrResendNs) {
        // Anti-entropy (re-review 2026-10-04 MAJOR-A): after a commit,
        // isrProposal_ still holds the COMMITTED (version, members); keep
        // re-sending it to every replica that has not acked it (a lost frame,
        // a node that was down) — knowledge converges without a lucky frame.
        // Cold: one small frame per un-acked replica per resend interval.
        const uint32_t all = followerSlots_.size() >= 32
            ? ~0u : ((1u << followerSlots_.size()) - 1u);
        if ((internalIsrAcks_ & all) != all) publishInternalIsr_(nowNs);
    }
    if ((isrChannel_ != nullptr || internalQuorum_()) && isrProposal_.pending) {
        // One proposal in flight: re-send it until the controller (internal:
        // a majority of the replicas) commits it or another committed change
        // resolves it (the request, the answer or the record may have been lost).
        if (nowNs - isrProposal_.sentNs >= kAlterIsrResendNs) {
            if (internalQuorum_()) publishInternalIsr_(nowNs);
            else                   sendIsrProposal_(nowNs);
        }
        return false;
    }
    // Desired ISR: the committed one, shrunk by the Kafka lag rule (no caught-
    // up fetch within replicaLagMaxMs), minus unclean (regressed) members,
    // expanded by followers validated in this leadership at >= HW.  Steady
    // state allocates nothing (IsrManager works in place on reserved capacity).
    //
    // Every partition — internal ones included (review 2026-10-04 MAJOR-3) —
    // lag-shrinks: a live but stuck member must not freeze the HW.  For an
    // INTERNAL partition (led by the consensus leader) that is safe because the
    // change commits only on a majority's durable knowledge and the consensus
    // never votes for a node outside the newest durable ISR (InternalIsrKnowledge).
    //
    // Re-review MEDIUM-C: only where the removal is decided by a quorum the
    // election respects — user partitions, partitions whose ISR the controller
    // commits (isrChannel_: the coordinator partitions, MEDIUM-B), and
    // partitions on the internal-ISR store.  Any other internal partition
    // (__topic_schemas, -4: self-managed, outside the store) never lag-shrinks:
    // a shrunk node would stay electable while missing acked records.
    isrManager_->setIsr(committedIsr_);
    if (!internalPartition_() || internalQuorum_() || isrChannel_ != nullptr)
        (void)isrManager_->maybeShrinkIsr(nowNs, logEndOffset_(), cfg_.replicaLagMaxMs,
                                          cfg_.replicaLagMaxOffset);
    for (std::size_t i = 0; i < followerSlots_.size(); ++i) {
        const FollowerSlot& sl = followerSlots_.at(i);
        if (sl.id == cfg_.selfId) continue;
        if (sl.unclean || (internalFencedMask_ & (1u << i)) != 0) (void)isrManager_->removeFromIsr(sl.id);
    }
    // Expansion considers only replicas OUTSIDE the committed ISR: a member the
    // shrink just removed sits at the HW it pins (HW = min over the ISR), so
    // "fetch offset >= HW" would re-admit it in the same pass and the HW would
    // never move (Kafka evaluates expansion against the HW of the shrunk ISR).
    // Kafka isFollowerInSync (review 2026-10-04 MAJOR-1): the follower must also
    // hold everything this leader held when elected (its leader-epoch start
    // offset = electionLeo_) — right after an election our HW is the floor
    // seeded from our follower view, which can lag the true committed offset;
    // a replica admitted below it would be electable while missing committed
    // records.
    const Offset joinFloor = std::max(leaderState_.highWatermark(), electionLeo_);
    // MEDIUM-2: no addition right after the controller answered INELIGIBLE.
    const bool mayExpand = nowNs >= expandBackoffUntilNs_;
    (void)isrManager_->maybeExpandIsrIf(nowNs, joinFloor, [this, mayExpand](NodeId n) {
        if (!mayExpand) return false;
        if (std::find(committedIsr_.begin(), committedIsr_.end(), n) != committedIsr_.end())
            return false;
        const int idx = slotIndex_(n);
        if (idx >= 0 && (internalFencedMask_ & (1u << idx)) != 0) return false;
        const FollowerSlot* sl = followerSlots_.find(n);
        return sl != nullptr && sl->validated && !sl->unclean;   // validated in THIS leadership
    });
    if (internalSeedPending_) {
        // MAJOR-2: the first ISR of a new internal leadership is published only
        // once every member it keeps has fetched (validated) at >= joinFloor —
        // a member that has not (yet) is undecided: it stays counted (seed) until
        // it confirms or the lag rule above shrinks it.  A published ISR is the
        // consensus candidacy set: it must never name a deficient member.
        for (NodeId n : isrManager_->isr()) {
            if (n == cfg_.selfId) continue;
            const FollowerSlot* sl = followerSlots_.find(n);
            const FollowerState* fs = isrManager_->followerState(n);
            if (sl == nullptr || !sl->validated || sl->unclean || fs == nullptr ||
                fs->lastFetchOffset < joinFloor) {
                applyMaximalIsr_();   // restore the maximal (seed) ISR the HW counts
                return false;
            }
        }
        return changeIsr_(isrManager_->isr(), nowNs, /*force=*/true);
    }
    // `desired` aliases isrManager_'s vector: changeIsr_ copies it out before it
    // re-seeds isrManager_ with the maximal ISR.
    const bool changed = changeIsr_(isrManager_->isr(), nowNs);
    if (!changed) applyMaximalIsr_();   // restore the maximal ISR the HW counts
    return changed;
}

bool PartitionReplica::changeIsr_(const std::vector<NodeId>& desired, int64_t nowNs, bool force)
{
    if (!force && sameMembers_(desired, committedIsr_)) return false;
    if (internalQuorum_()) {
        // MAJOR-2/3: publish a new version; it commits once a majority of the
        // replicas durably holds it.  HW over committed ∪ proposed meanwhile.
        if (isrProposal_.pending) return false;   // one change at a time
        isrProposal_.pending = true;
        isrProposal_.isr.assign(desired.begin(), desired.end());
        isrProposal_.version = ++internalIsrVersion_;
        internalIsrAcks_     = 0;
        applyMaximalIsr_();
        publishInternalIsr_(nowNs);
        maybeCommitInternalIsr_();
        publishCleanerState();
        return true;
    }
    if (isrChannel_ == nullptr) {
        // Self-managed: the leader is its own controller.
        committedIsr_.assign(desired.begin(), desired.end());
        applyMaximalIsr_();
        publishCleanerState();
        return true;
    }
    if (isrProposal_.pending) return false;   // one change at a time (re-sent by updateIsr_)
    // Controller-managed: propose; HW over committed ∪ proposed meanwhile — a
    // member being shrunk keeps holding the HW back until the controller
    // committed its removal (it is electable until then).
    isrProposal_.pending = true;
    isrProposal_.isr.assign(desired.begin(), desired.end());
    isrProposal_.basePartitionEpoch = committedPartitionEpoch_;
    applyMaximalIsr_();
    sendIsrProposal_(nowNs);
    publishCleanerState();
    return true;
}

int PartitionReplica::slotIndex_(NodeId id) const noexcept
{
    for (std::size_t i = 0; i < followerSlots_.size(); ++i)
        if (followerSlots_.at(i).id == id) return static_cast<int>(i);
    return -1;
}

void PartitionReplica::publishInternalIsr_(int64_t nowNs)
{
    isrProposal_.sentNs = nowNs;
    InternalIsrMsg m{};
    m.topicId     = cfg_.topicId;
    m.partition   = cfg_.partitionId;
    m.leaderId    = cfg_.selfId;
    m.leaderEpoch = epoch_;
    m.version     = isrProposal_.version;
    m.isrCount    = static_cast<int32_t>(std::min<std::size_t>(isrProposal_.isr.size(), InternalIsrMsg::kMaxIsr));
    for (int i = 0; i < m.isrCount; ++i) m.isr[static_cast<std::size_t>(i)] = isrProposal_.isr[static_cast<std::size_t>(i)];
    // Our own durable copy first: it is our ack (and our vote gate's view).
    const int self = slotIndex_(cfg_.selfId);
    if (self >= 0 && (internalIsrAcks_ & (1u << self)) == 0) {
        InternalIsrState st{};
        st.topicId = m.topicId; st.partition = m.partition; st.leaderEpoch = m.leaderEpoch;
        st.version = m.version; st.count = m.isrCount; st.members = m.isr;
        // LOW-R4-1: our own proposal — persisted (our ack + vote-gate view),
        // shared in the election exchange only once committed.
        if (isrKnowledge_->offerOwnProposal(st)) internalIsrAcks_ |= (1u << self);
    }
    for (std::size_t i = 0; i < followerSlots_.size(); ++i) {
        const NodeId to = followerSlots_.at(i).id;
        if (to == cfg_.selfId || (internalIsrAcks_ & (1u << i)) != 0) continue;
        if (!onTransport([&](auto& t) { return t.sendInternalIsr(to, m); })) ++sendRefusals_;
    }
}

void PartitionReplica::maybeCommitInternalIsr_()
{
    if (!isrProposal_.pending) return;
    const int majority = static_cast<int>(configuredReplicas_.size()) / 2 + 1;
    if (std::popcount(internalIsrAcks_) < majority) return;
    committedIsr_.assign(isrProposal_.isr.begin(), isrProposal_.isr.end());
    isrProposal_.pending  = false;
    internalSeedPending_  = false;
    if (isrKnowledge_ != nullptr) {
        // LOW-R4-1: committed — now the state this node shares in the exchange.
        InternalIsrState st{};
        st.topicId = cfg_.topicId; st.partition = cfg_.partitionId; st.leaderEpoch = epoch_;
        st.version = isrProposal_.version;
        st.count   = static_cast<int32_t>(std::min<std::size_t>(isrProposal_.isr.size(), InternalIsrMsg::kMaxIsr));
        for (int i = 0; i < st.count; ++i)
            st.members[static_cast<std::size_t>(i)] = isrProposal_.isr[static_cast<std::size_t>(i)];
        isrKnowledge_->noteCommitted(st);
    }
    applyMaximalIsr_();
    publishCleanerState();
}

void PartitionReplica::onInternalIsrAck(const InternalIsrAckMsg& m)
{
    // Pending: the ack counts toward the commit majority.  Committed: it stops
    // the anti-entropy re-send to that replica (MAJOR-A).
    if (!internalQuorum_() || role_ != Role::Leader || internalIsrVersion_ == 0) return;
    if (m.leaderEpoch != epoch_ || m.version < isrProposal_.version) return;   // an older state
    const int idx = slotIndex_(m.followerId);
    if (idx < 0) return;
    internalIsrAcks_ |= (1u << idx);
    maybeCommitInternalIsr_();
}

void PartitionReplica::onInternalIsr(const InternalIsrMsg& m)
{
    if (!internalQuorum_() || m.isrCount < 0 || m.isrCount > InternalIsrMsg::kMaxIsr) return;
    InternalIsrState st{};
    st.topicId = m.topicId; st.partition = m.partition; st.leaderEpoch = m.leaderEpoch;
    st.version = m.version; st.count = m.isrCount; st.members = m.isr;
    if (!isrKnowledge_->offer(st)) return;   // not persisted: never ack
    // Ack only the leader this replica follows, at its epoch: a deposed leader
    // (a newer epoch exists for us) is never helped to commit.
    if (role_ != Role::Follower || m.leaderEpoch != epoch_ || m.leaderId != leaderId_) return;
    InternalIsrAckMsg a{};
    a.topicId = m.topicId; a.partition = m.partition; a.followerId = cfg_.selfId;
    a.leaderEpoch = m.leaderEpoch; a.version = m.version;
    if (!onTransport([&](auto& t) { return t.sendInternalIsrAck(m.leaderId, a); })) ++sendRefusals_;
}

void PartitionReplica::sendIsrProposal_(int64_t nowNs)
{
    isrProposal_.sentNs = nowNs;
    AlterIsrMsg m{};
    m.topicId     = cfg_.topicId;
    m.partition   = cfg_.partitionId;
    m.leaderId    = cfg_.selfId;
    m.leaderEpoch = epoch_;
    m.isrCount    = static_cast<int32_t>(std::min<std::size_t>(isrProposal_.isr.size(), AlterIsrMsg::kMaxIsr));
    for (int i = 0; i < m.isrCount; ++i) m.isr[static_cast<std::size_t>(i)] = isrProposal_.isr[static_cast<std::size_t>(i)];
    m.partitionEpoch = isrProposal_.basePartitionEpoch;   // MEDIUM-1: compare-and-set version
    if (!isrChannel_->sendAlterIsr(m)) ++sendRefusals_;   // re-sent on the next resend tick
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
// Called from doWork() (the coalesced produce hot path), append()/appendKeyed()
// (direct API) and after a reconcile, on the replica's owning thread.
// Non-blocking: sendFetchResponse() is a tryClaim/offer that REPORTS acceptance.
//
// F27 — a push is never dropped silently:
//   * each ISR follower f is pushed from ITS OWN cursor (FollowerSlot::pushCursor
//     = max(pushedThrough, its last reported fetch offset));
//   * pushedThrough advances ONLY when the transport accepted the push to f;
//   * a refused push (BACK_PRESSURED / ADMIN_ACTION / NOT_CONNECTED) leaves f's
//     cursor where it was, so the NEXT pass re-sends [cursor_f, …) to f alone —
//     followers that got it are not re-sent, and no timer is involved.
// Followers that share a cursor (the common case) share ONE log view + response.
//
// Design:
//   1. Per follower: from = its cursor; a ZERO-COPY view of [from, LEO) via
//      readView() (spec §3.5a.2), trimmed with the shared whole-batch walk
//      (wholeBatchPrefix, same as onFetchRequest()) to the frame and the flow
//      ceiling (in-flight <= kPushFlowWindow: never more than the window).
//   2. Build a FetchResponse via makeFetchResponse() (zero-copy records view),
//      send, and advance that follower's pushedThrough on acceptance only.
//
// Epoch fencing: the FetchResponse carries the current leader epoch; followers
// compare it against their local epoch and drop mismatches (existing applyReplicated
// guard: resp.leaderEpoch > epoch_ → truncate; resp.leaderEpoch < epoch_ → silent
// drop via errorCode path handled above).  Stale-epoch pushes are harmless but
// wasteful; the follower's fast-ack fetch-response will carry the new epoch.
// ---------------------------------------------------------------------------
Offset PartitionReplica::pushFloor_() const noexcept
{
    Offset floor = logEndOffset_();
    if (!isrManager_) return floor;
    for (NodeId id : isrManager_->isr()) {
        if (id == cfg_.selfId) continue;
        if (const FollowerSlot* s = followerSlots_.find(id)) {
            const Offset c = s->pushCursor();
            if (c < floor) floor = c;
        }
    }
    return floor;
}

int PartitionReplica::pushToFollowers(Offset pushCeiling, Offset floorIn) noexcept
{
    RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplPushToFollowers,
                             static_cast<uint32_t>(cfg_.partitionId));
    if (role_ != Role::Leader) return 0;
    if (!isrManager_) return 0;

    const std::vector<NodeId>& isrList = isrManager_->isr();
    const Offset currentLeo = logEndOffset_();
    const Offset floor      = floorIn >= 0 ? floorIn : pushFloor_();   // F9: passed by doWork

    // m-3: the push prefix is bounded by the transport's deliverable FRAME, not
    // only maxFetchBytes (1 MiB).  Queried once, lazily (only if a push is due).
    int64_t     frame   = -1;
    std::size_t pushCap = 0;

    // One view + response per distinct cursor (followers in step share it).
    Offset                     builtFrom = -1;
    bool                       builtOk   = false;
    rune::storage::LogReadView v{};
    BatchPrefix                pre{};
    FetchResponseMsg           resp{};
    bool                       counted   = false;
    int                        accepted  = 0;

    for (NodeId followerId : isrList) {
        if (followerId == cfg_.selfId) continue;
        FollowerSlot* const slot = followerSlots_.find(followerId);
        if (slot == nullptr) [[unlikely]] continue;     // impossible: ISR ⊆ replica set
        // KIP-320/595: push only to a follower whose fetch passed lineage
        // validation in this leadership — a push is applied without its own
        // lineage check, so an unvalidated (possibly divergent) follower would
        // graft our records onto its divergent tail.
        if (!slot->validated) [[unlikely]] continue;
        const Offset from = slot->pushCursor();
        if (from >= currentLeo) continue;               // this follower has (been sent) it all

        if (from != builtFrom) {
            builtFrom = from;
            builtOk   = false;
            if (frame < 0) {
                frame = std::numeric_limits<int64_t>::max();
                onTransport([&](auto& t) { frame = t.maxFetchRecordsBytes(); });
                pushCap = static_cast<std::size_t>(std::min<int64_t>(cfg_.maxFetchBytes, frame));
            }
            if (!counted) { ++rtPushes_; counted = true; }
            // Zero-copy: a view into the log (same path as onFetchRequest()); the
            // transport copies the bytes once, straight into the follower's term.
            // (F3 round 6 item 1: chunk capped at the transport's LIVE ceiling —
            // F18 m-3's pushCap; one ceiling: maxFetchRecordsBytes().)
            v = onLog([&](auto& L) { return L.readView(from, static_cast<int32_t>(pushCap)); });
            // F18 / KIP-74: a first batch larger than maxFetchBytes is still pushed
            // whole (never stuck).  FLOW CONTROL: no batch that begins at or beyond
            // pushCeiling is pushed (in-flight <= kPushFlowWindow).
            pre = wholeBatchPrefix(v.data, v.len, pushCap, pushCeiling);
            if (pre.bytes == 0) continue;               // nothing deliverable within ceiling/cap
            // F18 (I-1): an oversized first batch (KIP-74) that exceeds the FRAME is
            // unservable — never push it; the fetch path answers kErrRecordTooLarge
            // and the followers leave the ISR (ISR = self → no more pushes).
            if (static_cast<int64_t>(pre.bytes) > frame) [[unlikely]] {
                // F3 round 6, item 8: frame 0 = no publication resolved yet — not a
                // property of this batch: never arm the memo / log a "ceiling of 0".
                if (frame <= 0) continue;
                // F3 round 5, item I2: remember this head (at the push floor) so
                // doWork() does not re-read / re-parse it every cycle until LEO, the
                // ISR, the floor (pushCeiling) or the epoch changes; the critical
                // log fires once per unsendable-offset transition.
                const Offset firstBase = rune::protocol::RecordBatchReader(v.data, v.len).baseOffset();
                ++unsendableBatchLogCount_;
                if (from == floor)
                    unsendablePush_ = UnsendablePushMemo{from, currentLeo, pushCeiling,
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
                continue;
            }
            if (from == floor) unsendablePush_.fromOffset = -1;   // a sendable head disarms the memo
            // DR-BUG-3: advertise the min-ISR-gated HW (see onFetchRequest).
            resp = makeFetchResponse(highWatermark(), kErrNone,
                                     std::span<const uint8_t>(v.data, pre.bytes));
            builtOk = true;
        }
        if (!builtOk) continue;

        const bool ok = onTransport([&](auto& t) { return t.sendFetchResponse(followerId, resp); });
        if (ok) [[likely]] {
            ++accepted;
            rtPushBytes_ += static_cast<int64_t>(pre.bytes);
            if (pre.endOffset > slot->pushedThrough) slot->pushedThrough = pre.endOffset;
        } else {
            ++sendRefusals_;   // cursor unchanged: re-sent from it on the next pass
        }
    }
    return accepted;
}

// ---------------------------------------------------------------------------
// F3 round 5, item I2: unsendable-push suppression (see header).
// ---------------------------------------------------------------------------
bool PartitionReplica::unsendablePushSuppressed_(Offset pushCeiling, Offset floor) const noexcept
{
    if (unsendablePush_.fromOffset < 0) [[likely]] return false;   // unarmed
    return unsendablePush_.fromOffset  == floor &&
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
