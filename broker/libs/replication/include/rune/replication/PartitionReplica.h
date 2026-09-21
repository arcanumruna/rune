#pragma once
// ---------------------------------------------------------------------------
// PartitionReplica.h — Single-writer partition replica: leader and follower
//                      roles for the Rune data-plane replication layer.
//
// Namespace: rune::replication
// C++23. NO locks. NO busy-spin. Single thread owns all state.
//
// ## Overview (spec §4, plan P3-3)
//
// PartitionReplica ties together:
//   - Phase-1 rune::storage::Log  (persistent in-order byte store)
//   - Phase-1 rune::storage::LeaderEpochCache  (epoch→startOffset truncation)
//   - P3-1  ReplicaLeaderState  (HW + per-follower LEO)
//   - P3-2  IsrManager          (ISR shrink/expand, produce gate)
//   - P3-2  EpochFencing        (stale-epoch rejection, truncation offset)
//   - P3-1  ReplicationMessages (FetchRequest/FetchResponse SBE codecs)
//
// ## Role model
//
//   Role::Leader  — appends client batches, serves follower fetches, advances HW.
//   Role::Follower — fetches from leader, truncates on epoch change, applies batches.
//
// Both roles are driven by a single `doWork(int64_t nowNs)` call that MUST be
// called by one owning thread.  doWork() samples nowNs once and runs all
// periodic work (ISR checks, fetch emission) without spinning or sleeping.
//
// ## Transport injection
//
// PartitionTransport is a pure abstract interface so tests can plug in a
// deterministic fake.  Production code will implement it over Aeron; tests use
// InMemoryTransport below.
//
// ## No hot-path allocation
//
// All scratch buffers are pre-allocated in the constructor.  Reusable SBE
// codec instances are fields.  Record views (std::span) point into caller or
// log buffers.
// ---------------------------------------------------------------------------

#include <cstdint>
#include <functional>
#include <mutex>
#include <span>
#include <unordered_map>
#include <vector>
#include <string>

#include <rune/replication/Types.h>
#include <rune/replication/HighWatermark.h>
#include <rune/replication/IsrManager.h>
#include <rune/replication/EpochFencing.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/storage/Log.h>
#include <rune/storage/LeaderEpochCache.h>

namespace rune::replication {

// ---------------------------------------------------------------------------
// Role
// ---------------------------------------------------------------------------
enum class Role { Leader, Follower };

// ---------------------------------------------------------------------------
// Error codes (returned in FetchResponseMsg::errorCode)
// ---------------------------------------------------------------------------
inline constexpr int16_t kErrNone         =  0;
inline constexpr int16_t kErrStaleEpoch   = -1;   // request epoch < local epoch
inline constexpr int16_t kErrNotLeader    = -2;   // this node is not the leader
inline constexpr int16_t kErrMinIsr       = -3;   // ISR too small to produce
inline constexpr int16_t kErrOutOfRange   = -4;   // fetchOffset out of range
// DR-BUG-6: leader is deferring the epoch answer while it reverse-reconciles a
// genuine committed continuation from this (ahead, on-lineage) follower.  This
// is a KEEP-ALIVE, NOT a truncation directive: the follower KEEPS its tail and
// RESETS its fallback probe counter so a slow reconcile under packet loss never
// trips the destructive fallback-to-committedHw truncation (the DR-8 loss).
inline constexpr int16_t kErrReconcileDeferred = -5;

// ---------------------------------------------------------------------------
// PartitionConfig — constructor argument bundle
// ---------------------------------------------------------------------------
struct PartitionConfig {
    int32_t     topicId           = 0;   ///< DR-BUG-5: topic identifier for multi-topic routing
    PartitionId partitionId       = 0;
    NodeId      selfId            = 0;
    int64_t     replicaLagMaxMs   = 10'000;         ///< ms before follower shrunk from ISR
    Offset      replicaLagMaxOffset = 10'000;       ///< offset lag before ISR shrink
    int         minIsr            = 1;              ///< min ISR for acksAll produce
    int64_t     fetchIntervalNs   = 50'000'000LL;   ///< follower fetch cadence (50 ms backstop; push is steady-state)
    int64_t     isrCheckIntervalNs = 10'000'000LL;  ///< leader ISR check cadence (10 ms)
    int32_t     maxFetchBytes     = 1 << 20;        ///< max bytes per fetch (1 MiB)
    bool        pushOnAppend      = true;            ///< GR-10: leader pushes to ISR followers immediately on append
    bool        fastAckOnApply    = true;            ///< GR-10: follower fast-acks (emit FetchRequest immediately after applyReplicated)
    CleanupPolicy cleanupPolicy   = CleanupPolicy::Delete;  ///< CP-1: log cleanup policy for this partition
    // CP-2: compaction tuning fields.
    double      minCleanableDirtyRatio = 0.5;        ///< CP-2: min dirty-offset fraction before compaction fires [0.0, 1.0]
    int64_t     deleteRetentionMs      = 86'400'000LL; ///< CP-2: how long (ms) to retain tombstones before removing them
    // CP-3: size/time retention (used by Delete + CompactDelete policies).
    int64_t     retentionBytes         = -1;          ///< CP-3: keep this many bytes; -1 = unbounded (no size deletion)
    int64_t     retentionMs            = -1;          ///< CP-3: keep segments younger than this ms; -1 = unbounded
};

// ---------------------------------------------------------------------------
// PartitionTransport — injected interface (no Aeron in this file)
//
// Leader uses sendFetchResponse(); follower uses sendFetchRequest().
// The fake pumps responses back into the leader via its onFetchRequest() and
// pumps requests back into the follower via its applyReplicated().
// ---------------------------------------------------------------------------
class PartitionTransport {
public:
    virtual ~PartitionTransport() = default;

    /// (Follower → Leader) send a FetchRequest.
    virtual void sendFetchRequest(const FetchRequestMsg& req) = 0;

    /// (Leader → Follower) send a FetchResponse.
    virtual void sendFetchResponse(NodeId toFollower,
                                   const FetchResponseMsg& resp) = 0;

    // -----------------------------------------------------------------------
    // DR-BUG-6 — OffsetsForLeaderEpoch exchange + deficient-leader reconcile.
    //
    // These are peer-routed (addressed to a specific NodeId).  They have SAFE
    // DEFAULT NO-OP bodies so a transport that has not implemented them yet
    // still compiles AND the replica degrades to the conservative
    // truncate-to-committed-HW fallback (never loses committed data, but may
    // re-fetch a small committed tail).  InMemoryTransport (tests),
    // FilteringTransport (integration harness) and AeronPartitionTransport
    // (production) implement them fully.
    // -----------------------------------------------------------------------

    /// (New follower → New leader) ask for the leader's end offset for the last
    /// epoch they share (KIP-101), BEFORE the follower truncates on epoch change.
    virtual void sendOffsetsForLeaderEpochRequest(
            NodeId /*toLeader*/, const OffsetsForLeaderEpochReqMsg& /*req*/) {}

    /// (Leader → New follower) the epoch-boundary answer.
    virtual void sendOffsetsForLeaderEpochResponse(
            NodeId /*toFollower*/, const OffsetsForLeaderEpochRespMsg& /*resp*/) {}

    /// (Deficient leader → ahead survivor) pull missing committed records
    /// [fromOffset, survivorLEO) — a REVERSE fetch (leader fetches from a
    /// follower).  Reuses the FetchRequest shape.
    virtual void sendReconcileRequest(
            NodeId /*toPeer*/, const FetchRequestMsg& /*req*/) {}

    /// (Ahead survivor → deficient leader) serve the requested records.
    /// Reuses the FetchResponse shape.
    virtual void sendReconcileResponse(
            NodeId /*toPeer*/, const FetchResponseMsg& /*resp*/) {}
};

// ---------------------------------------------------------------------------
// PartitionReplica
// ---------------------------------------------------------------------------
class PartitionReplica {
public:
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// \param cfg        Partition configuration.
    /// \param role       Initial role (Leader or Follower).
    /// \param epoch      Current leader epoch (stamped on every batch).
    /// \param replicas   Full replica set (NodeIds, including self).
    /// \param isr        Initial ISR (subset of replicas; often == replicas at start).
    /// \param log        Owning pointer to the partition Log (pre-constructed).
    /// \param transport  Injected transport (fake for tests, Aeron for prod).
    PartitionReplica(PartitionConfig                       cfg,
                     Role                                   role,
                     Epoch                                  epoch,
                     std::vector<NodeId>                    replicas,
                     std::vector<NodeId>                    isr,
                     rune::storage::Log*                    log,
                     PartitionTransport*                    transport);

    // -----------------------------------------------------------------------
    // Role transitions (called by control plane on leader election / demotion)
    // -----------------------------------------------------------------------

    /// Transition to Leader with the given epoch.
    /// Resets ISR from replicas, seeds ReplicaLeaderState.
    void becomeLeader(Epoch newEpoch, std::vector<NodeId> isr);

    /// Transition to Follower with the given leader epoch.
    void becomeFollower(Epoch leaderEpoch, NodeId leaderId);

    // -----------------------------------------------------------------------
    // Leader API
    // -----------------------------------------------------------------------

    /// Append a record batch to the leader's log and advance LEO.
    ///
    /// \param records   Raw record bytes (the batch payload — caller-managed).
    /// \param acksAll   true → require |ISR| >= minIsr (majority-durable produce).
    /// \returns base offset of the appended batch, or -1 if gated (minIsr).
    ///
    /// Does NOT block. Does NOT wait for followers. The caller drives doWork()
    /// to pump follower fetches which advance the HW.
    Offset append(std::span<const uint8_t> records, bool acksAll = true);

    /// CP-1: append a single record with an explicit (non-empty) key.
    ///
    /// Builds a one-record RecordBatch with the supplied key and value bytes,
    /// so the on-disk record carries a real compaction key.  All other semantics
    /// (ISR gate, HW advance, push-on-append) are identical to append().
    ///
    /// \param key      Record key bytes (e.g. compaction identity for __consumer_offsets).
    /// \param value    Record value bytes (the payload).
    /// \param acksAll  true → require |ISR| >= minIsr (majority-durable produce).
    /// \returns base offset of the appended batch, or -1 if gated (minIsr).
    Offset appendKeyed(std::span<const uint8_t> key,
                       std::span<const uint8_t> value,
                       bool acksAll = true);

    /// Reconcile the leader's tracked LEO with the underlying Log and recompute HW.
    ///
    /// Called after a batch is appended DIRECTLY to the leader's partition Log by
    /// a co-located writer that bypasses append() — e.g. the transaction layer
    /// stamping already-framed transactional data batches or control (COMMIT/ABORT)
    /// markers into the same partition log the replica owns.  Without this call the
    /// leader's ReplicaLeaderState would still hold the pre-append LEO and HW would
    /// never advance to cover the externally-written bytes.
    ///
    /// GR-10: when pushOnAppend is enabled, also pushes the newly-appended bytes
    /// to each ISR follower via an unsolicited FetchResponse (non-blocking).
    ///
    /// No-op on a follower (follower LEO/HW are driven by applyReplicated()).
    void onLeaderLogAppended() noexcept;

    /// Handle an inbound FetchRequest from a follower.
    ///
    /// Epoch-fences (rejects stale leaderEpoch with kErrStaleEpoch).
    /// Updates follower LEO in ReplicaLeaderState + IsrManager.
    /// Recomputes HW.
    /// Sends a FetchResponse via transport_.
    ///
    /// \param req    The fetch request from the follower.
    /// \param nowNs  Current wall-clock time (nanoseconds) used to record the
    ///               follower's real fetch time in IsrManager.  Callers MUST pass
    ///               the actual current time so time-lag eviction is correct.
    ///
    /// \returns kErrNone on success, error code otherwise.
    int16_t onFetchRequest(const FetchRequestMsg& req, int64_t nowNs);

    /// Read committed records (≤ highWatermark) starting at fetchOffset.
    ///
    /// Returns empty span if fetchOffset >= highWatermark (nothing committed
    /// beyond the caller's position yet).
    ///
    /// The returned vector's lifetime is the caller's responsibility.
    std::vector<uint8_t> read(Offset fetchOffset, int32_t maxBytes) const;

    // -----------------------------------------------------------------------
    // Follower API
    // -----------------------------------------------------------------------

    /// Apply a FetchResponse received from the leader.
    ///
    /// On epoch change (response.leaderEpoch > local):
    ///   1. Compute truncation offset via EpochFencing.
    ///   2. Truncate local Log to that offset.
    ///   3. Adopt the new epoch.
    ///
    /// Then append the records to the local Log (if any).
    /// Update local HW = min(local LEO, response.highWatermark).
    void applyReplicated(const FetchResponseMsg& resp);

    /// Build a FetchRequest for the leader: fetchOffset = local LEO.
    FetchRequestMsg nextFetch() const noexcept;

    // -----------------------------------------------------------------------
    // DR-BUG-6 — OffsetsForLeaderEpoch exchange (KIP-101) + deficient-leader
    // reverse-catch-up reconcile.
    // -----------------------------------------------------------------------

    /// (Leader) Handle an OffsetsForLeaderEpochRequest from a follower that is
    /// deciding whether/where to truncate on an epoch change.  Answers from the
    /// LeaderEpochCache: (responseEpoch, endOffset) — see LeaderEpochCache::
    /// endOffsetForEpoch.  Sends the response via
    /// transport_->sendOffsetsForLeaderEpochResponse().
    void onOffsetsForLeaderEpochRequest(const OffsetsForLeaderEpochReqMsg& req);

    /// (Follower) Apply the leader's OffsetsForLeaderEpoch answer: perform the
    /// DEFERRED, correct truncation.
    ///   • responseEpoch == our latest epoch  → SHARED (possibly open) epoch:
    ///     no divergence.  We keep our tail (the leader reconciles anything it
    ///     lacks).  Truncate only to min(ourLEO, endOffset) which, for a shared
    ///     open epoch (endOffset == leaderLEO), can be BELOW our LEO — but those
    ///     are committed records the leader is missing, so we KEEP them.
    ///   • responseEpoch  < our latest epoch  → GENUINE divergence at the epoch
    ///     boundary: truncate to min(ourLEO, endOffset).
    /// Never truncates below our committed HW floor (defence in depth).
    void applyOffsetsForLeaderEpochResponse(const OffsetsForLeaderEpochRespMsg& resp);

    /// (Any role) Serve a reconcile pull: read raw log bytes [fromOffset, LEO)
    /// and reply via transport_->sendReconcileResponse().  Used by an ahead
    /// survivor to feed committed records to a deficient new leader.
    void onReconcileRequest(const FetchRequestMsg& req);

    /// (Leader) Apply reconcile records pulled from an ahead survivor: append
    /// the strictly-new committed tail and advance LEO/HW.
    void applyReconcileResponse(const FetchResponseMsg& resp);

    // -----------------------------------------------------------------------
    // doWork — single-writer periodic step (no spin, no sleep)
    // -----------------------------------------------------------------------

    /// Drive periodic work for this replica.
    ///
    /// Leader:   runs IsrManager shrink/expand on isrCheckIntervalNs cadence;
    ///           propagates updated ISR into ReplicaLeaderState.
    /// Follower: emits a FetchRequest when fetchIntervalNs has elapsed.
    ///
    /// \param nowNs   Injected clock (nanoseconds). Sampled ONCE per call.
    /// \returns Number of work items performed (0 when idle — no spin).
    int doWork(int64_t nowNs);

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    [[nodiscard]] Role        role()           const noexcept { return role_; }
    [[nodiscard]] Epoch       epoch()          const noexcept { return epoch_; }
    [[nodiscard]] Offset      logEndOffset()   const noexcept { return log_->logEndOffset(); }
    [[nodiscard]] Offset      highWatermark()  const noexcept;
    [[nodiscard]] int32_t     topicId()        const noexcept { return cfg_.topicId; }
    [[nodiscard]] PartitionId partitionId()    const noexcept { return cfg_.partitionId; }
    [[nodiscard]] NodeId      selfId()         const noexcept { return cfg_.selfId; }
    /// CP-1: the log cleanup policy for this partition (consulted by the LogCleaner).
    [[nodiscard]] CleanupPolicy cleanupPolicy() const noexcept { return cfg_.cleanupPolicy; }

    // CP-2: compaction config accessors.
    /// Minimum dirty-offset fraction that must be exceeded before compaction fires.
    [[nodiscard]] double      minCleanableDirtyRatio() const noexcept { return cfg_.minCleanableDirtyRatio; }
    /// How long (ms) tombstones are retained before being fully removed.
    [[nodiscard]] int64_t     deleteRetentionMs()      const noexcept { return cfg_.deleteRetentionMs; }
    // CP-3: size/time retention accessors.
    /// Max bytes to retain; -1 = unbounded.
    [[nodiscard]] int64_t     retentionBytes()         const noexcept { return cfg_.retentionBytes; }
    /// Max age (ms) to retain; -1 = unbounded.
    [[nodiscard]] int64_t     retentionMs()            const noexcept { return cfg_.retentionMs; }
    /// CP-2: offset up to which the log has already been compacted (cleaned watermark).
    /// Initially 0; advanced by runBoundedCompaction after each segment is cleaned.
    [[nodiscard]] Offset lastCleanOffset() const noexcept { return lastCleanOffset_; }
    /// CP-2: advance the cleaned watermark (called by NodeRuntimeAgent after each segment).
    void setLastCleanOffset(Offset o) noexcept { lastCleanOffset_ = o; }

    /// Non-owning pointer to the underlying Log (for test inspection and replay seeding).
    [[nodiscard]] rune::storage::Log* log() const noexcept { return log_; }

    /// ISR as seen by the leader (empty for follower).
    [[nodiscard]] const std::vector<NodeId>& isr() const noexcept;

private:
    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// (Leader) DR-BUG-6 divergence guard — is an ahead follower's tail a genuine
    /// continuation of THIS leader's OWN epoch lineage (KIP-101), i.e. safe to
    /// keep+reverse-reconcile rather than direct the follower to truncate?
    ///
    /// Gates on the follower's tail EPOCH, NOT on ISR membership.  Because the
    /// GR-2 bridge seeds ISR = ALL replicas, ISR membership cannot distinguish a
    /// committed continuation from a divergent tail — both look "in ISR".  The
    /// only sound discriminator is epoch lineage:
    ///
    ///   endOffsetForEpoch(requestedEpoch, leaderLEO) → (responseEpoch, endOffset)
    ///
    /// The follower's ahead tail is a shared continuation ONLY when the leader
    /// shares the EXACT epoch the follower's tail is in AND that epoch is the
    /// leader's own latest OPEN epoch (endOffset == leaderLEO, boundary >=
    /// leaderLEO).  Then the follower's [leaderLEO, followerLEO) tail extends the
    /// leader's still-open shared epoch → keep + reverse-reconcile.  Otherwise the
    /// follower's tail is from a divergent / non-shared epoch → the leader answers
    /// authoritatively and the follower TRUNCATES to the shared-epoch end offset.
    [[nodiscard]] bool followerTailIsSharedContinuation(
            Epoch requestedEpoch, Offset followerLeo) const;

    /// (Leader) Recompute HW from ReplicaLeaderState and propagate to leader ISR.
    void recomputeHw();

    /// (Leader) Propagate current ISR into ReplicaLeaderState.
    void syncIsrToLeaderState();

    /// (Follower) Truncate the log to `truncateOffset` (inclusive last valid
    /// offset = truncateOffset - 1).  Updates local LEO.
    void truncateLog(Offset truncateOffset);

    /// (Leader) GR-10: push records starting at fromOffset to all ISR followers
    /// via unsolicited FetchResponse.  Non-blocking (tryClaim/offer); a dropped
    /// push is fine — the fetch backstop recovers it.  Uses encodeScratch_ (no
    /// per-call alloc).  Only called when cfg_.pushOnAppend is true.
    void pushToFollowers(Offset fromOffset) noexcept;

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------
    PartitionConfig      cfg_;
    Role                 role_;
    Epoch                epoch_;
    NodeId               leaderId_;           ///< (follower) leader's NodeId; -1 if unknown

    rune::storage::Log*         log_;         ///< non-owning (owner is the test/container)
    PartitionTransport*         transport_;   ///< non-owning

    // Leader-side components
    ReplicaLeaderState   leaderState_;        ///< HW + per-follower LEO
    IsrManager*          isrManager_ = nullptr; // heap — allocated on becomeLeader
    std::unique_ptr<IsrManager> isrManagerOwned_; ///< ownership wrapper

    // Epoch caches (both roles need one for truncation)
    rune::storage::LeaderEpochCache epochCache_;

    // Timing state
    int64_t lastIsrCheckNs_  = 0;   ///< last ISR shrink/expand timestamp
    int64_t lastFetchEmitNs_ = 0;   ///< last follower fetch emission timestamp

    // Pre-allocated scratch buffers (no hot-path alloc)
    static constexpr std::size_t kScratchSize = 4 * 1024 * 1024; // 4 MiB
    std::vector<uint8_t> encodeScratch_;   ///< for FetchResponse encoding

    // Follower HW (leader uses leaderState_.highWatermark())
    Offset followerHw_ = 0;

    // DR-BUG-6: OffsetsForLeaderEpoch deferred-truncation state.
    //
    // On becomeFollower(newEpoch) we do NOT truncate eagerly.  Instead we set
    // epochProbePending_ and remember the epoch we are probing under and the
    // committed floor captured at transition time.  The follower issues an
    // OffsetsForLeaderEpochRequest to the new leader and defers all fetching
    // until the answer arrives (applyOffsetsForLeaderEpochResponse), which does
    // the correct truncation.  This prevents an up-to-date survivor from
    // dropping its committed tail before the (possibly deficient) new leader can
    // pull it (the DR-8 loss).
    bool   epochProbePending_ = false;
    Epoch  epochProbeEpoch_    = -1;   ///< epoch we transitioned into
    Offset epochProbeFloor_    = 0;    ///< committed HW floor at transition
    int    epochProbeAttempts_ = 0;    ///< probes issued without an answer

    // DR-BUG-6: after this many unanswered probes, fall back to the conservative
    // committed-HW truncation and resume fetching.  Keeps the replica live even
    // when the transport does not implement OffsetsForLeaderEpoch or the new
    // leader is not promoted yet — it degrades EXACTLY to the pre-DR-BUG-6
    // behaviour (never loses committed data; may re-fetch a small committed
    // tail), never a permanent stall.
    static constexpr int kEpochProbeMaxAttempts = 8;

    // DR-BUG-6: deficient-leader reconcile state.
    //
    // A newly-elected leader may be BEHIND the committed frontier (a deficient
    // node elected by the data-blind consensus).  When it sees a follower whose
    // LEO exceeds its own, it pulls the missing committed records from that
    // ahead survivor via sendReconcileRequest() before advertising a HW that
    // covers them.  reconcileTarget_ tracks which peer we are pulling from (to
    // avoid a storm of duplicate reconcile requests); -1 means idle.
    NodeId reconcileTarget_    = -1;   ///< peer we are reverse-fetching from

    // DR-BUG-6 CHURN-ROBUSTNESS: wall-clock (ns) at which the in-flight reconcile
    // pull was issued.  If a pull goes unanswered (its request or response dropped
    // under packet loss) reconcileTarget_ would otherwise latch forever and stall
    // the leader-driven re-drive.  The incomplete-leader doWork clears a stuck
    // target after kReconcileStuckNs so the pull re-arms on the next cadence — the
    // self-healing retry that makes catch-up robust to sustained loss / churn.
    int64_t reconcileIssuedNs_ = -1;
    static constexpr int64_t kReconcileStuckNs = 250'000'000LL; // 250 ms

    // CP-2: cleaned watermark — the log offset up to which compaction has already
    // been applied.  Starts at 0 (nothing compacted).  NodeRuntimeAgent advances
    // this after each successful segment compaction so already-clean prefixes are
    // never re-scanned or re-rewritten on the next duty-cycle call.
    Offset lastCleanOffset_ = 0;

    // GR-10: push tracking — last offset that was pushed to followers.
    // Avoids re-pushing bytes already pushed in the same append cycle.
    Offset lastPushedOffset_ = 0;

    // DR-BUG-6: on becomeLeader the follower-lag TIME clock must be reset to the
    // election time so a newly-elected leader does not evict every follower from
    // the ISR on its first ISR check (their lastFetchTimeNs is 0 → instantly
    // "time-lagged").  We defer the reset to the first leader doWork(nowNs) since
    // becomeLeader has no clock.  See IsrManager::resetLagClock() for the full
    // rationale (this eviction is what disabled the reverse-reconcile in
    // production and caused the DR-8 committed-loss).
    bool leaderLagClockNeedsReset_ = false;

    // DR-BUG-6 LEADER-COMPLETENESS GATE (the final DR-8 close).
    //
    // A newly-promoted partition leader may have been elected by the data-BLIND
    // consensus while it is BEHIND the true committed frontier (the double-election
    // churn case).  Serving its own regressed committed HW immediately is the
    // residual DR-8 loss: a committed record held on an up-to-date survivor is
    // never delivered / gets dropped because the deficient leader advertises a HW
    // at its own frontier and the reverse-reconcile has not (yet, or reliably under
    // loss) caught it up.
    //
    // The gate enforces leader-completeness at the REPLICATION layer (not the Raft
    // election, which stays untouched for consensus safety): on becomeLeader we
    // enter a NOT-YET-COMPLETE state and HOLD the served/advertised high-watermark
    // at the pre-failover committed floor (completenessFloor_).  While incomplete
    // the leader does NOT advance the committed offset past that floor — so neither
    // the acks=All drain nor HW-bounded subscribers (GR-6) can observe a regressed
    // or partial view.  We LEARN each live ISR survivor's committed frontier from
    // the probe/fetch requests they send (their LEO + epoch — the lag-clock reset
    // keeps them in ISR so they report in), compute the highest offset a MAJORITY
    // of the ISR holds (the true committed frontier), reverse-reconcile UP TO it
    // from an ahead on-lineage survivor, and only THEN mark leadership COMPLETE and
    // advertise a HW >= that frontier.  The served HW never REGRESSES below a
    // previously-committed frontier across the failover (completenessFloor_ is a
    // hard floor; the post-complete HW is monotonic).
    //
    // NO-DIVERGENCE: the frontier is computed ONLY from ON-LINEAGE survivor reports.
    // A divergent ahead survivor (one whose tail fails the epoch-lineage gate
    // followerTailIsSharedContinuation) contributes only min(itsLeo, ourLeo) to the
    // majority computation — its divergent tail can NEVER raise the frontier nor be
    // adopted (the per-batch hasEpoch gate in applyReconcileResponse rejects it).
    // Genuine divergent tails still truncate exactly as before.
    //
    // BOUNDED / CHURN-ROBUST: the gate is re-armed on EVERY becomeLeader (so it
    // survives leadership flaps under loss); completion is decided per-doWork /
    // per-report with no busy-spin; and it degrades safely — if a majority of the
    // ISR never reports in (genuine quorum loss), the DR-BUG-3 min-ISR gate already
    // pins the committed offset at the floor, so the incomplete state can never
    // expose uncommitted data.  Completion requires only that we have HEARD from a
    // majority AND our own LEO covers the majority frontier with no reconcile in
    // flight — an event that the reverse-reconcile machinery drives deterministically.
    bool   leaderComplete_    = true;   ///< false while a new leader reconciles to the frontier
    Offset completenessFloor_ = 0;      ///< pre-failover committed HW; served HW held here until complete

    /// (Leader) Re-evaluate the completeness gate: if we have heard from a majority
    /// of the ISR and our LEO covers the on-lineage majority frontier with no
    /// reconcile pending, mark leadership complete and release the served HW.
    void maybeCompleteLeadership();

    /// (Leader) The highest offset a MAJORITY of the ISR holds, computed from
    /// ON-LINEAGE survivor reports only (a divergent ahead survivor is clamped to
    /// our own LEO).  == the true committed frontier we must reconcile up to.
    [[nodiscard]] Offset majorityCommittedFrontier() const;

    // DR-BUG-6: per-survivor last-reported LEO (frontier), recorded from probe and
    // fetch requests.  Used to (a) decide when a MAJORITY of the ISR has reported
    // in (completeness quorum) and (b) — clamped for divergent tails — compute the
    // majority committed frontier.  Single-writer (leader thread); bounded by |ISR|.
    std::unordered_map<NodeId, Offset> survivorFrontier_;

    // DR-BUG-3: min-ISR-gated committed offset (Kafka min.insync.replicas
    // COMMIT semantics).  ReplicaLeaderState::computeHw() uses majority-of-ISR:
    // when the ISR shrinks to a single member (the leader stranded in a
    // minority), majority-of-1 == the leader's own LEO, so the raw HW jumps to
    // uncommitted-by-a-real-majority offsets.  That polluted HW must NOT be
    // exposed as "committed" — neither to the acks=All drain nor to HW-bounded
    // subscribers (GR-6) — or a minority leader's records would be acked/
    // delivered and then truncated on failover (data-loss / phantom reads).
    //
    // lastHealthyHw_ is the last HW observed while the ISR was healthy
    // (|ISR| >= minIsr).  While |ISR| < minIsr, highWatermark() clamps to this
    // frozen floor so the effective committed offset cannot advance past the
    // last majority-committed offset.  It is monotonic (only advances) and is
    // refreshed on every leader HW read/recompute while the ISR is healthy.
    mutable Offset lastHealthyHw_ = 0;
};

// ---------------------------------------------------------------------------
// InMemoryTransport — deterministic fake for tests
//
// Implements a simple in-memory message queue between a leader PartitionReplica
// and one or more follower PartitionReplicas. Call pump() to deliver all
// pending messages (requests to leader, responses to followers).
//
// Usage in a bounded test driver:
//   InMemoryTransport transport(leader, followers);
//   for (int i = 0; i < CAP && !done; ++i) {
//       clock += step;
//       leader.doWork(clock);
//       for (auto& f : followers) f.doWork(clock);
//       transport.pump();
//       done = check();
//   }
//   ASSERT_TRUE(done) << "stalled after " << CAP << " iterations";
// ---------------------------------------------------------------------------
class InMemoryTransport : public PartitionTransport {
public:
    struct PendingRequest {
        FetchRequestMsg req;
    };
    struct PendingResponse {
        NodeId          toFollower;
        FetchResponseMsg resp;
        std::vector<uint8_t> recordsBuf; // owns the bytes resp.records points into
    };

    // Register leader and followers after construction.
    void setLeader(PartitionReplica* leader) {
        leader_ = leader;
        // DR-BUG-6: also index the leader by NodeId so peer-routed messages
        // (OffsetsForLeaderEpoch responses, reconcile requests) can reach it.
        if (leader) byId_[leader->selfId()] = leader;
    }
    void addFollower(NodeId id, PartitionReplica* follower) {
        followers_[id] = follower;
        byId_[id] = follower;   // DR-BUG-6: index for peer-routed messages
    }
    /// Remove a follower from the transport (simulates network partition/crash).
    void removeFollower(NodeId id) {
        followers_.erase(id);
        byId_.erase(id);
    }

    // PartitionTransport implementation
    //
    // GR-10: sendFetchRequest and sendFetchResponse are guarded by a mutex so
    // that the leader push path (called from the produce thread) and the
    // pump() path (called from the replication duty-cycle thread) are safe to
    // call concurrently.  In single-threaded tests the lock is uncontended.
    void sendFetchRequest(const FetchRequestMsg& req) override {
        std::lock_guard lk(mu_);
        pendingRequests_.push_back({req});
    }

    void sendFetchResponse(NodeId toFollower,
                           const FetchResponseMsg& resp) override {
        PendingResponse pr;
        pr.toFollower = toFollower;
        // Deep-copy records bytes so the response outlives the leader's scratch buf.
        pr.recordsBuf.assign(resp.records.begin(), resp.records.end());
        pr.resp = resp;
        pr.resp.records = std::span<const uint8_t>(pr.recordsBuf.data(),
                                                    pr.recordsBuf.size());
        std::lock_guard lk(mu_);
        pendingResponses_.push_back(std::move(pr));
    }

    // DR-BUG-6: OffsetsForLeaderEpoch + reconcile peer-routed messages.
    void sendOffsetsForLeaderEpochRequest(
            NodeId toLeader, const OffsetsForLeaderEpochReqMsg& req) override {
        std::lock_guard lk(mu_);
        pendingEpochReqs_.push_back({toLeader, req});
    }
    void sendOffsetsForLeaderEpochResponse(
            NodeId toFollower, const OffsetsForLeaderEpochRespMsg& resp) override {
        std::lock_guard lk(mu_);
        pendingEpochResps_.push_back({toFollower, resp});
    }
    void sendReconcileRequest(
            NodeId toPeer, const FetchRequestMsg& req) override {
        std::lock_guard lk(mu_);
        pendingReconcileReqs_.push_back({toPeer, req});
    }
    void sendReconcileResponse(
            NodeId toPeer, const FetchResponseMsg& resp) override {
        PendingResponse pr;
        pr.toFollower = toPeer;
        pr.recordsBuf.assign(resp.records.begin(), resp.records.end());
        pr.resp = resp;
        pr.resp.records = std::span<const uint8_t>(pr.recordsBuf.data(),
                                                    pr.recordsBuf.size());
        std::lock_guard lk(mu_);
        pendingReconcileResps_.push_back(std::move(pr));
    }

    /// Deliver all pending messages: FetchRequests → leader, FetchResponses → followers.
    ///
    /// \param nowNs  Current wall-clock time passed to onFetchRequest() so the
    ///               leader records the follower's real fetch timestamp.  Defaults
    ///               to 0 for backward compatibility with tests that do not care
    ///               about time-lag eviction; real tests should supply the clock.
    ///
    /// Returns total messages delivered.
    int pump(int64_t nowNs = 0) {
        int count = 0;

        // Deliver fetch requests to the leader.
        std::vector<PendingRequest> reqs;
        {
            std::lock_guard lk(mu_);
            reqs.swap(pendingRequests_);
        }
        for (auto& pr : reqs) {
            if (leader_) {
                leader_->onFetchRequest(pr.req, nowNs);
                ++count;
            }
        }

        // Deliver fetch responses to followers.
        std::vector<PendingResponse> resps;
        {
            std::lock_guard lk(mu_);
            resps.swap(pendingResponses_);
        }
        for (auto& pr : resps) {
            auto it = followers_.find(pr.toFollower);
            if (it != followers_.end()) {
                it->second->applyReplicated(pr.resp);
                ++count;
            }
        }

        // DR-BUG-6: OffsetsForLeaderEpoch requests → target (the new leader).
        std::vector<PendingEpochReq> ereqs;
        {
            std::lock_guard lk(mu_);
            ereqs.swap(pendingEpochReqs_);
        }
        for (auto& e : ereqs) {
            auto it = byId_.find(e.to);
            if (it != byId_.end()) {
                it->second->onOffsetsForLeaderEpochRequest(e.req);
                ++count;
            }
        }

        // DR-BUG-6: OffsetsForLeaderEpoch responses → target (the follower).
        std::vector<PendingEpochResp> eresps;
        {
            std::lock_guard lk(mu_);
            eresps.swap(pendingEpochResps_);
        }
        for (auto& e : eresps) {
            auto it = byId_.find(e.to);
            if (it != byId_.end()) {
                it->second->applyOffsetsForLeaderEpochResponse(e.resp);
                ++count;
            }
        }

        // DR-BUG-6: reconcile requests → target (the ahead survivor).
        std::vector<PendingReconcileReq> rreqs;
        {
            std::lock_guard lk(mu_);
            rreqs.swap(pendingReconcileReqs_);
        }
        for (auto& r : rreqs) {
            auto it = byId_.find(r.to);
            if (it != byId_.end()) {
                it->second->onReconcileRequest(r.req);
                ++count;
            }
        }

        // DR-BUG-6: reconcile responses → target (the deficient leader).
        std::vector<PendingResponse> rresps;
        {
            std::lock_guard lk(mu_);
            rresps.swap(pendingReconcileResps_);
        }
        for (auto& pr : rresps) {
            auto it = byId_.find(pr.toFollower);
            if (it != byId_.end()) {
                it->second->applyReconcileResponse(pr.resp);
                ++count;
            }
        }

        return count;
    }

    /// Number of in-flight messages (for test assertions).
    [[nodiscard]] std::size_t pendingCount() const noexcept {
        std::lock_guard lk(mu_);
        return pendingRequests_.size() + pendingResponses_.size()
             + pendingEpochReqs_.size() + pendingEpochResps_.size()
             + pendingReconcileReqs_.size() + pendingReconcileResps_.size();
    }

private:
    struct PendingEpochReq  { NodeId to; OffsetsForLeaderEpochReqMsg  req; };
    struct PendingEpochResp { NodeId to; OffsetsForLeaderEpochRespMsg resp; };
    struct PendingReconcileReq { NodeId to; FetchRequestMsg req; };

    PartitionReplica* leader_ = nullptr;
    std::unordered_map<NodeId, PartitionReplica*> followers_;
    std::unordered_map<NodeId, PartitionReplica*> byId_;   ///< DR-BUG-6: all replicas by id

    mutable std::mutex           mu_;              ///< guards all pending queues
    std::vector<PendingRequest>  pendingRequests_;
    std::vector<PendingResponse> pendingResponses_;
    std::vector<PendingEpochReq>  pendingEpochReqs_;   ///< DR-BUG-6
    std::vector<PendingEpochResp> pendingEpochResps_;  ///< DR-BUG-6
    std::vector<PendingReconcileReq> pendingReconcileReqs_;  ///< DR-BUG-6
    std::vector<PendingResponse>  pendingReconcileResps_;    ///< DR-BUG-6
};

} // namespace rune::replication
