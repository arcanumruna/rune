#pragma once
// ---------------------------------------------------------------------------
// ExactlyOnceHarness.h — Deterministic EOS integration harness (P5-5).
//
// Namespace: rune::transaction::test
// C++23. NO busy-spin. NO while(!done). All loops bounded + ASSERT(done).
// Injected monotonic clock. REAL Phase-5 components:
//   TransactionCoordinator + TransactionLog (durable)
//   ProducerStateManager (idempotent dedup)
//   rune::storage::Log (partition log)
//   ReadCommittedView (LSO + aborted-txn index + read_committed filtering)
//   rune::coordination::OffsetStore (transactional offset commits)
//   ControlMarker::writeMarkers / resolvePendingMarkers
//
// ## Design (mirrors ReplicationClusterHarness / ConsumerGroupHarness pattern)
//
//   ExactlyOnceHarness owns:
//     - ONE TransactionLog at a durable temp path (survives coordinator restart).
//     - ONE TransactionCoordinator wired to the TransactionLog.
//     - N partition Logs (one per partition-id), each in its own RAII dir.
//     - N LsoTracker + AbortedTxnIndex + ReadCommittedView (one per partition).
//     - N ProducerStateManager (one per partition).
//     - ONE OffsetStore (durable).
//     - One injected int64_t clock_ (nanoseconds).
//
//   driveUntil(pred, maxSteps, stepNs):
//     Bounded driver: iterate at most maxSteps times. Each step advances the
//     clock by stepNs and calls doWork() on the coordinator.
//     Returns true iff pred() is true after the loop.
//     NEVER hangs — caller ASSERT_TRUE(driveUntil(...)).
//
//   Fault injection:
//     recreateCoordinator() — destroy + recreate coordinator from SAME durable
//                             TransactionLog path (simulates failover).  The
//                             TransactionLog is reopened (replay) so state is
//                             fully recovered.
//
//   Helper transactions:
//     beginTxn(txnId, pid, epoch)     — addPartitions to all configured parts.
//     appendTxnData(pid, epoch, part, seq, val)  — ProducerStateManager validate+
//                                          update, Log::append, LsoTracker update.
//     commitTxn(txnId, pid, epoch)    — endTransaction(commit) + writeMarkers.
//     abortTxn(txnId, pid, epoch)     — endTransaction(abort) + writeMarkers.
//     readCommitted(partIdx, fromOff)  — ReadCommittedView::read.
//     readUncommitted(partIdx, fromOff)— ReadCommittedView::read (ReadUncommitted).
//
//   Invariant assertions (helpers):
//     assertCommittedVisible(...)    — txn committed → data in read_committed.
//     assertAbortedInvisible(...)    — txn aborted  → data NOT in read_committed.
//     assertReadUncommittedSeesData()— read_uncommitted sees data regardless.
//     assertLsoNotExposed(partIdx)   — LSO ≤ HW always.
//     assertNoAbortedVisible(partIdx)— no aborted batch in read_committed result.
//
// ## Unique temp paths
//   /tmp/rune_eos_integ_<tag>_<pid>_<counter>_<timestamp>
// ---------------------------------------------------------------------------

#include <rune/transaction/TransactionCoordinator.h>
#include <rune/transaction/TransactionLog.h>
#include <rune/transaction/ControlMarker.h>
#include <rune/transaction/ReadCommittedView.h>
#include <rune/transaction/Types.h>
#include <rune/storage/Log.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/coordination/OffsetStore.h>
#include <rune/coordination/Types.h>
#include <rune/protocol/RecordBatch.h>

#include <map>

#include <gtest/gtest.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unistd.h>
#include <vector>

namespace rune::transaction::test {

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

static inline std::string makeUniqSuffix() {
    static std::atomic<uint64_t> gSeq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_" +
           std::to_string(gSeq.fetch_add(1, std::memory_order_relaxed)) + "_" +
           std::to_string(ts);
}

/// RAII directory — auto-removed on destruction.
struct HarnessDir {
    std::string path;
    explicit HarnessDir(const std::string& tag) {
        path = std::filesystem::temp_directory_path().string()
               + "/rune_eos_integ_" + tag + "_" + makeUniqSuffix();
        std::filesystem::create_directories(path);
    }
    ~HarnessDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
    HarnessDir(const HarnessDir&)            = delete;
    HarnessDir& operator=(const HarnessDir&) = delete;
    HarnessDir(HarnessDir&&) noexcept        = default;
    HarnessDir& operator=(HarnessDir&&)      = delete;
};

// ---------------------------------------------------------------------------
// Per-partition state held by the harness.
// ---------------------------------------------------------------------------
struct PartitionState {
    std::string              topic;
    int32_t                  partIdx{0};
    HarnessDir               dir;
    rune::storage::Log       log;
    LsoTracker               lsoTracker;
    AbortedTxnIndex          abortedIndex;
    ReadCommittedView        view;
    rune::storage::ProducerStateManager psm;

    explicit PartitionState(const std::string& topicName, int32_t idx)
        : topic(topicName)
        , partIdx(idx)
        , dir("p" + std::to_string(idx))
        , log(dir.path)
        , view(&lsoTracker, &abortedIndex)
    {}

    TopicPartition tp() const { return TopicPartition{topic, partIdx}; }

    int64_t hw() const { return log.logEndOffset(); }
};

// ---------------------------------------------------------------------------
// Build a data batch with a specific producerId / epoch / sequence.
// Uses RecordBatchBuilder for clean LE encoding.
// ---------------------------------------------------------------------------
static inline std::vector<uint8_t> makeDataBatch(int64_t baseOffset,
                                                   int64_t producerId,
                                                   int16_t producerEpoch,
                                                   int32_t baseSequence,
                                                   std::string_view value = "payload")
{
    rune::protocol::RecordBatchBuilder builder(baseOffset, 0);
    builder.setProducerId(producerId);
    builder.setProducerEpoch(producerEpoch);
    builder.setBaseSequence(baseSequence);
    builder.append({}, value, 0);
    return builder.build();
}

/// Append a control marker (COMMIT or ABORT) for producerId; update LsoTracker.
static inline int64_t appendControlMarker(PartitionState& ps,
                                           int64_t producerId,
                                           int16_t producerEpoch,
                                           ControlType ct,
                                           int32_t coordEpoch = 0)
{
    int64_t off = ps.log.logEndOffset();
    auto batch = ControlMarker::build(producerId,
                                      static_cast<ProducerEpoch>(producerEpoch),
                                      ct, coordEpoch);
    // Stamp baseOffset at [0..7] LE (outside the CRC region [21..end]).
    uint64_t u = static_cast<uint64_t>(off);
    for (int i = 0; i < 8; ++i) batch[i] = static_cast<uint8_t>(u >> (i * 8));
    ps.log.append(batch, off);

    bool commit = (ct == ControlType::Commit);
    int64_t firstOff = ps.lsoTracker.onControlMarker(producerId, off, commit);
    if (!commit && firstOff >= 0) {
        ps.abortedIndex.onAbortMarker(producerId, firstOff, off);
    }
    return off;
}

/// Parse base offsets from a ReadCommittedView::read result buffer.
static inline std::vector<int64_t> parseOffsets(const std::vector<uint8_t>& raw)
{
    std::vector<int64_t> result;
    std::size_t pos = 0;
    while (pos < raw.size()) {
        std::size_t rem = raw.size() - pos;
        if (rem < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead))
            break;
        rune::protocol::RecordBatchReader reader(raw.data() + pos, rem);
        int32_t total = reader.totalLength();
        if (total <= 0) break;
        result.push_back(reader.baseOffset());
        pos += static_cast<std::size_t>(total);
    }
    return result;
}

// ---------------------------------------------------------------------------
// ExactlyOnceHarness
// ---------------------------------------------------------------------------
class ExactlyOnceHarness {
public:
    static constexpr int     kDefaultCap = 1000;
    static constexpr int64_t kStep1ms    = 1'000'000LL;  // 1 ms in ns

    /// \param numPartitions  Number of partitions (each gets its own Log).
    /// \param topicName      Topic name used for TopicPartition construction.
    /// \param txnTimeoutMs   Default transaction timeout (ms) for the coordinator.
    explicit ExactlyOnceHarness(int         numPartitions  = 3,
                                 std::string topicName      = "test-topic",
                                 int32_t     txnTimeoutMs   = 60'000)
        : txnLogDir_("txnlog")
        , offsetStoreDir_("offsets")
        , txnLogPath_(txnLogDir_.path + "/txn.log")
        , offsetStorePath_(offsetStoreDir_.path + "/offsets.log")
        , txnTimeoutMs_(txnTimeoutMs)
    {
        // Build per-partition state.
        for (int i = 0; i < numPartitions; ++i) {
            parts_.emplace_back(
                std::make_unique<PartitionState>(topicName, i));
        }
        allPartitions_.reserve(static_cast<std::size_t>(numPartitions));
        for (auto& p : parts_) allPartitions_.push_back(p->tp());

        // Build durable offset store (must be constructed before coordinator so
        // the OffsetApplyCallback lambda can capture it).
        offsetStore_ = std::make_unique<rune::coordination::OffsetStore>(offsetStorePath_);

        // Build coordinator + log, wiring the OffsetApplyCallback so that
        // completeTransaction(COMMIT) atomically applies pending offsets.
        txnLog_  = std::make_unique<TransactionLog>(txnLogPath_);
        coord_   = std::make_unique<TransactionCoordinator>(
                       ProducerIdAllocator{}, txnTimeoutMs_,
                       txnLog_.get(), /*coordinatorEpoch=*/0,
                       makeOffsetApplyCallback());
    }

    // -----------------------------------------------------------------------
    // Drive loop — bounded deterministic step.
    // -----------------------------------------------------------------------

    /// Advance clock by stepNs and call doWork on the coordinator.
    /// Repeat until pred() is true or maxSteps exceeded.
    /// Returns true iff pred() satisfied.
    /// NEVER hangs — caller ASSERT_TRUE(driveUntil(...)).
    bool driveUntil(std::function<bool()> pred,
                    int     maxSteps = kDefaultCap,
                    int64_t stepNs   = kStep1ms)
    {
        for (int i = 0; i < maxSteps && !pred(); ++i) {
            clock_ += stepNs;
            coord_->doWork(clock_);
        }
        return pred();
    }

    // -----------------------------------------------------------------------
    // Coordinator failover (recreate from same durable log path).
    // -----------------------------------------------------------------------

    /// Destroy the coordinator + log, reopen from the SAME durable path,
    /// reconstruct the coordinator with recovered state.  Simulates a crash +
    /// restart of the transaction coordinator.
    ///
    /// \param newCoordEpoch  Coordinator epoch to use after failover (bump).
    void recreateCoordinator(int32_t newCoordEpoch = 1) {
        coord_.reset();
        txnLog_.reset();

        // Reopen durable log — replays entries from disk.
        txnLog_ = std::make_unique<TransactionLog>(txnLogPath_);

        // Reseed the PID allocator past the highest recovered PID.
        ProducerIdAllocator alloc;
        for (auto& [txnId, meta] : txnLog_->all()) {
            alloc.reseedAtLeast(meta.producerId);
        }

        // Reconstruct coordinator with recovered log, wiring the same callback.
        coord_ = std::make_unique<TransactionCoordinator>(
                     std::move(alloc), txnTimeoutMs_,
                     txnLog_.get(), newCoordEpoch,
                     makeOffsetApplyCallback());
    }

    // -----------------------------------------------------------------------
    // Transaction helpers
    // -----------------------------------------------------------------------

    /// InitProducerId for a transactional producer (new txnId or re-init).
    InitProducerIdResult initProducer(const TransactionalId& txnId,
                                       int32_t txnTimeoutMs = 0)
    {
        int32_t tms = (txnTimeoutMs > 0) ? txnTimeoutMs : txnTimeoutMs_;
        return coord_->initProducerId(txnId, tms, clock_);
    }

    /// InitProducerId for a non-transactional (idempotent-only) producer.
    InitProducerIdResult initProducerNoTxn(int32_t txnTimeoutMs = 60'000)
    {
        return coord_->initProducerId(std::nullopt, txnTimeoutMs, clock_);
    }

    /// addPartitions for all configured partitions.
    AddPartitionsResult addAllPartitions(const TransactionalId& txnId,
                                         ProducerId             pid,
                                         ProducerEpoch          epoch)
    {
        return coord_->addPartitions(txnId, pid, epoch, allPartitions_, clock_);
    }

    /// addPartitions for a specific subset of partitions.
    AddPartitionsResult addPartitions(const TransactionalId&          txnId,
                                      ProducerId                       pid,
                                      ProducerEpoch                    epoch,
                                      const std::vector<TopicPartition>& parts)
    {
        return coord_->addPartitions(txnId, pid, epoch, parts, clock_);
    }

    /// Append a transactional data batch to partition[partIdx].
    /// Validates + updates ProducerStateManager.  Updates LsoTracker.
    /// Returns the log base offset or -1 on validation failure.
    int64_t appendTxnData(ProducerId    producerId,
                           ProducerEpoch producerEpoch,
                           int           partIdx,
                           int32_t       baseSequence,
                           std::string_view value = "payload")
    {
        assert(partIdx >= 0 && partIdx < static_cast<int>(parts_.size()));
        PartitionState& ps = *parts_[static_cast<std::size_t>(partIdx)];

        int32_t lastSeq = baseSequence; // single record per batch
        auto vr = ps.psm.validate(producerId, partIdx,
                                   producerEpoch, baseSequence, lastSeq);
        if (vr.isDuplicate()) return vr.retryBaseOffset; // idempotent: return orig offset
        if (!vr.isAccept())   return -1;

        int64_t off = ps.log.logEndOffset();
        auto batch = makeDataBatch(off, producerId, producerEpoch, baseSequence, value);
        ps.log.append(batch, off);

        ps.psm.update(producerId, partIdx, producerEpoch,
                      baseSequence, lastSeq, off);
        ps.lsoTracker.onProduceBatch(producerId, off);
        return off;
    }

    /// Commit a transaction: endTransaction(commit) → writeMarkers → completeTransaction.
    /// Also updates each partition's LsoTracker (COMMIT marker).
    bool commitTxn(const TransactionalId& txnId,
                   ProducerId             producerId,
                   ProducerEpoch          producerEpoch,
                   int32_t                coordEpoch = 0)
    {
        auto endRes = coord_->endTransaction(txnId, producerId, producerEpoch,
                                             /*commit=*/true, clock_);
        if (!endRes.ok()) return false;

        // Write COMMIT markers to all involved partitions.
        for (auto& p : parts_) {
            for (const auto& tp : endRes.partitions) {
                if (tp == p->tp()) {
                    appendControlMarker(*p, producerId, producerEpoch,
                                        ControlType::Commit, coordEpoch);
                }
            }
        }

        auto cr = coord_->completeTransaction(txnId);
        return cr.ok();
    }

    /// Abort a transaction: endTransaction(abort) → writeMarkers → completeTransaction.
    bool abortTxn(const TransactionalId& txnId,
                  ProducerId             producerId,
                  ProducerEpoch          producerEpoch,
                  int32_t                coordEpoch = 0)
    {
        auto endRes = coord_->endTransaction(txnId, producerId, producerEpoch,
                                             /*commit=*/false, clock_);
        if (!endRes.ok()) return false;

        // Write ABORT markers to all involved partitions.
        for (auto& p : parts_) {
            for (const auto& tp : endRes.partitions) {
                if (tp == p->tp()) {
                    appendControlMarker(*p, producerId, producerEpoch,
                                        ControlType::Abort, coordEpoch);
                }
            }
        }

        auto cr = coord_->completeTransaction(txnId);
        return cr.ok();
    }

    /// Resolve pending markers (fence-aborts, timeout-aborts, recovered Prepare*).
    /// Writes ABORT/COMMIT markers for each pending entry via resolvePendingMarkers.
    int resolvePending(int32_t coordEpoch = 0) {
        auto appendFn = [&](const TopicPartition& tp,
                             const std::vector<uint8_t>& markerBatch) {
            for (auto& ps : parts_) {
                if (ps->tp() == tp) {
                    // Extract producerId + controlType from the marker batch.
                    ControlMarkerPayload payload;
                    bool ok = ControlMarker::extract(markerBatch.data(), 0,
                                                     payload,
                                                     markerBatch.size());
                    if (!ok) return;

                    int64_t off = ps->log.logEndOffset();
                    // Stamp baseOffset LE at [0..7].
                    std::vector<uint8_t> batch = markerBatch;
                    uint64_t u = static_cast<uint64_t>(off);
                    for (int i = 0; i < 8; ++i)
                        batch[i] = static_cast<uint8_t>(u >> (i * 8));
                    ps->log.append(batch, off);

                    bool isCommit = (payload.controlType == ControlType::Commit);
                    int64_t firstOff = ps->lsoTracker.onControlMarker(
                                           payload.producerId, off, isCommit);
                    if (!isCommit && firstOff >= 0) {
                        ps->abortedIndex.onAbortMarker(payload.producerId,
                                                        firstOff, off);
                    }
                }
            }
        };

        return ControlMarker::resolvePendingMarkers(*coord_, appendFn);
    }

    // -----------------------------------------------------------------------
    // Read helpers
    // -----------------------------------------------------------------------

    /// read_committed from partition[partIdx] starting at fetchOffset.
    std::vector<uint8_t> readCommitted(int partIdx, int64_t fetchOffset = 0,
                                         int32_t maxBytes = 1 << 20) const
    {
        const PartitionState& ps = *parts_.at(static_cast<std::size_t>(partIdx));
        return ps.view.read(const_cast<rune::storage::Log*>(&ps.log),
                            ps.hw(), fetchOffset, maxBytes,
                            IsolationLevel::ReadCommitted);
    }

    /// read_uncommitted from partition[partIdx] starting at fetchOffset.
    std::vector<uint8_t> readUncommitted(int partIdx, int64_t fetchOffset = 0,
                                          int32_t maxBytes = 1 << 20) const
    {
        const PartitionState& ps = *parts_.at(static_cast<std::size_t>(partIdx));
        return ps.view.read(const_cast<rune::storage::Log*>(&ps.log),
                            ps.hw(), fetchOffset, maxBytes,
                            IsolationLevel::ReadUncommitted);
    }

    /// Rebuild view state from scratch (used after coordinator failover to
    /// pick up markers written by the new coordinator).
    void rebuildView(int partIdx) {
        PartitionState& ps = *parts_.at(static_cast<std::size_t>(partIdx));
        ps.lsoTracker.clear();
        ps.abortedIndex.clear();
        ps.view.rebuild(&ps.log, ps.hw());
    }

    void rebuildAllViews() {
        for (int i = 0; i < static_cast<int>(parts_.size()); ++i) {
            rebuildView(i);
        }
    }

    // -----------------------------------------------------------------------
    // OffsetStore helpers (transactional offset commit)
    // -----------------------------------------------------------------------

    void commitOffset(const std::string& group,
                      const rune::coordination::TopicPartition& tp,
                      rune::coordination::Offset offset)
    {
        offsetStore_->commit(group, tp, offset, "", clock_);
    }

    std::optional<rune::coordination::CommittedOffset>
    fetchOffset(const std::string& group,
                const rune::coordination::TopicPartition& tp) const
    {
        return offsetStore_->fetch(group, tp);
    }

    /// Bind consumer-group offsets to the current transaction (F3).
    /// Records them as PENDING within the txn; applied on commit, discarded on abort.
    AddPartitionsResult addOffsetsToTxn(
        const TransactionalId& txnId,
        ProducerId             pid,
        ProducerEpoch          epoch,
        const std::string&     group,
        const std::map<rune::coordination::TopicPartition,
                       rune::coordination::Offset>& offsets)
    {
        return coord_->addOffsetsToTxn(txnId, pid, epoch, group, offsets, clock_);
    }

    // -----------------------------------------------------------------------
    // Invariant assertions
    // -----------------------------------------------------------------------

    /// A committed txn's data at the given offsets is visible under read_committed.
    void assertCommittedVisible(int partIdx,
                                 const std::vector<int64_t>& expectedOffsets)
    {
        auto raw = readCommitted(partIdx);
        auto got = parseOffsets(raw);
        for (int64_t expOff : expectedOffsets) {
            bool found = false;
            for (int64_t gotOff : got) {
                if (gotOff == expOff) { found = true; break; }
            }
            EXPECT_TRUE(found)
                << "Committed data at offset " << expOff
                << " on partition " << partIdx
                << " NOT visible under read_committed";
        }
    }

    /// An aborted txn's data at the given offsets is NOT visible under read_committed.
    void assertAbortedInvisible(int partIdx,
                                  const std::vector<int64_t>& abortedOffsets)
    {
        auto raw = readCommitted(partIdx);
        auto got = parseOffsets(raw);
        for (int64_t abortOff : abortedOffsets) {
            bool found = false;
            for (int64_t gotOff : got) {
                if (gotOff == abortOff) { found = true; break; }
            }
            EXPECT_FALSE(found)
                << "Aborted data at offset " << abortOff
                << " on partition " << partIdx
                << " IS visible under read_committed (invariant violated)";
        }
    }

    /// read_uncommitted sees the data at the given offsets.
    void assertReadUncommittedSeesData(int partIdx,
                                        const std::vector<int64_t>& expectedOffsets)
    {
        auto raw = readUncommitted(partIdx);
        auto got = parseOffsets(raw);
        for (int64_t expOff : expectedOffsets) {
            bool found = false;
            for (int64_t gotOff : got) {
                if (gotOff == expOff) { found = true; break; }
            }
            EXPECT_TRUE(found)
                << "Data at offset " << expOff
                << " on partition " << partIdx
                << " NOT visible under read_uncommitted";
        }
    }

    /// LSO never exceeds HW for all partitions.
    void assertLsoLeHw() {
        for (int i = 0; i < static_cast<int>(parts_.size()); ++i) {
            const PartitionState& ps = *parts_.at(static_cast<std::size_t>(i));
            int64_t hw  = ps.hw();
            int64_t lso = ps.lsoTracker.lso(hw);
            EXPECT_LE(lso, hw)
                << "LSO=" << lso << " > HW=" << hw << " on partition " << i;
        }
    }

    /// On partition[partIdx], read_committed returns NO records belonging to
    /// any aborted transaction (checks abortedIndex).
    void assertNoAbortedDataVisible(int partIdx) {
        const PartitionState& ps = *parts_.at(static_cast<std::size_t>(partIdx));
        auto raw = readCommitted(partIdx);
        auto offsets = parseOffsets(raw);
        for (int64_t off : offsets) {
            // Get the producerId of this batch from the raw read result.
            // The batch is at the beginning of the raw block for that offset.
            // We search the raw buffer by scanning.
            // Simple approach: just check the abortedIndex doesn't flag any returned batch.
            // We need to extract producerId from each batch.
            auto rawAll = readUncommitted(partIdx);
            std::size_t pos = 0;
            while (pos < rawAll.size()) {
                std::size_t rem = rawAll.size() - pos;
                if (rem < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead))
                    break;
                rune::protocol::RecordBatchReader reader(rawAll.data() + pos, rem);
                int32_t total = reader.totalLength();
                if (total <= 0) break;
                int64_t batchOff = reader.baseOffset();
                if (batchOff == off) {
                    int64_t pid = reader.producerId();
                    EXPECT_FALSE(ps.abortedIndex.isAborted(pid, off))
                        << "read_committed returned aborted batch: pid=" << pid
                        << " off=" << off << " on partition " << partIdx;
                }
                pos += static_cast<std::size_t>(total);
            }
        }
    }

    /// All-or-nothing: for a committed txn, data must appear on ALL partitions.
    void assertAllPartitionsHaveData(const std::vector<int>& partIndices,
                                      const std::vector<std::vector<int64_t>>& offsets)
    {
        assert(partIndices.size() == offsets.size());
        for (std::size_t i = 0; i < partIndices.size(); ++i) {
            assertCommittedVisible(partIndices[i], offsets[i]);
        }
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    [[nodiscard]] TransactionCoordinator& coordinator()  noexcept { return *coord_; }
    [[nodiscard]] TransactionLog&         txnLog()       noexcept { return *txnLog_; }
    [[nodiscard]] rune::coordination::OffsetStore& offsetStore() noexcept { return *offsetStore_; }
    [[nodiscard]] int64_t                 clock()        const noexcept { return clock_; }
    [[nodiscard]] int                     numPartitions() const noexcept {
        return static_cast<int>(parts_.size());
    }
    [[nodiscard]] PartitionState&         partition(int i) {
        return *parts_.at(static_cast<std::size_t>(i));
    }
    [[nodiscard]] const PartitionState&   partition(int i) const {
        return *parts_.at(static_cast<std::size_t>(i));
    }
    [[nodiscard]] const std::vector<TopicPartition>& allPartitions() const noexcept {
        return allPartitions_;
    }
    [[nodiscard]] const std::string& txnLogPath() const noexcept { return txnLogPath_; }

    void advanceClock(int64_t ns) noexcept { clock_ += ns; }

private:
    // -----------------------------------------------------------------------
    // Build the OffsetApplyCallback that routes committed offsets from the
    // coordinator's completeTransaction(COMMIT) path into the OffsetStore.
    // -----------------------------------------------------------------------
    OffsetApplyCallback makeOffsetApplyCallback() {
        return [this](const std::string& group,
                      const rune::coordination::TopicPartition& tp,
                      rune::coordination::Offset offset,
                      int64_t /*timestampNs*/) {
            offsetStore_->commit(group, tp, offset, "", clock_);
        };
    }

    HarnessDir   txnLogDir_;
    HarnessDir   offsetStoreDir_;
    std::string  txnLogPath_;
    std::string  offsetStorePath_;
    int32_t      txnTimeoutMs_;

    std::vector<std::unique_ptr<PartitionState>>   parts_;
    std::vector<TopicPartition>                     allPartitions_;

    std::unique_ptr<TransactionLog>                 txnLog_;
    std::unique_ptr<TransactionCoordinator>         coord_;
    std::unique_ptr<rune::coordination::OffsetStore> offsetStore_;

    int64_t clock_ = 1'000'000'000LL; // start non-zero (Kafka-like)
};

} // namespace rune::transaction::test
