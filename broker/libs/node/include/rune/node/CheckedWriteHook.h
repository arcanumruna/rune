#pragma once
// CheckedWriteHook.h - checked writes Part 03: the node half of the write hook handleProduceBatch<LogT, HookT> calls
// for a typed partition.  check() is one bounded walk over the batch (attribute bits, Part 02's per-record rules,
// intents), then the section 11.2 requirement table.  Hot path: no allocation, no lock, no virtual call.
#include <rune/node/WriteState.h>
#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/node/AutoInc.h>
#include <rune/node/lease/LeaseIndex.h>
#include <rune/storage/ControlRecordLayout.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>
#include <rune/storage/ProducePath.h>
#include <rune/storage/WriteHookPolicy.h>

#include <rune/protocol/Crc32c.h>
#include <rune/protocol/LeaseRecord.h>

#include <array>
#include <cassert>
#include <cstring>
#include <cstddef>
#include <cstdint>
#include <span>

namespace rune::node {

inline constexpr int32_t kMaxJudgedBatchRecords = rune::protocol::client::kMaxJudgedBatchRecords;
inline constexpr std::size_t kReservationScratch = 80;
static_assert(rune::storage::control_record::reservationBatchBytes() <= kReservationScratch);

inline constexpr int64_t kLeaseRetryAfterNs = 1'000'000;   ///< the Throttled retry-after of a lease revoke (1 ms)

/// Lease topics: the produce path's per-worker work area (allocated once, cold; the hook is built per produce and owns nothing).
/// Zero allocation on the hot path: the same-batch update-then-revoke key set (open addressing, generation-stamped so a batch
/// clears it in O(1)), the touched row ids, the revoke replacement marks and spans, and the one re-encoded value.
struct LeaseHookWork {
    static constexpr std::size_t kSetSlots = 4096;   // twice the batch cap: load <= 0.5
    struct Slot { uint32_t gen = 0; uint32_t keyLen = 0; uint64_t hash = 0; const char* key = nullptr; };
    std::array<Slot, kSetSlots>                                       set{};
    uint32_t                                                          gen = 0;
    std::array<uint32_t, rune::protocol::record::kMaxLeaseBatchRecords> rids{};      ///< rows the batch updates or revokes (stage)
    std::array<uint8_t, rune::protocol::record::kMaxLeaseBatchRecords>  replace{};   ///< 1 = this record is a revoke to re-encode
    std::array<rune::protocol::RecordSpan, rune::protocol::record::kMaxLeaseBatchRecords> spans{};
    std::array<uint8_t, 4 + 65535 + 1>                                valueTmp{};    ///< one re-encoded revoke value (u16 block length)
};

/// The lease hook's scratch bytes for a worker whose intake limit is `maxIntake`: a rebuilt batch (grants stamped, revokes
/// re-encoded at the widest installed version) is refused by check() when it would exceed it, so this is a bound, not a worst-case
/// sum.  The ONE definition: the worker and the test rig both size the scratch with it.
[[nodiscard]] constexpr std::size_t leaseScratchBytes(std::size_t maxIntake) noexcept { return 2 * maxIntake; }

/// What the produce frame and the worker say about one write.
struct WriteFacts {
    uint8_t acks               = 0;       ///< the Produce.acks wire byte (bit 0 All, bit 1 View)
    bool    idempotent         = false;
    bool    transactional      = false;
    bool    atomicBatch        = false;
    bool    importGranted      = false;
    bool    importDenialDeferred = false;   // #506: the security view is behind: a missing IMPORT is not a verdict
    int64_t producerId         = -1;
    bool    legacyMode         = false;
    bool    levelCheckedWrites = false;
    // Lease topics only (Task 5a); the worker computes them only for a lease topic, so another topic pays nothing.
    int32_t clientVersion      = 0;       ///< the producing session's announced client version (0 = not announced)
    bool    levelLeases        = false;   ///< the cluster feature level is at least MetadataFeature::kLeases
    bool    viewMemOver        = false;   ///< the namespace's view memory is over its limit (nsViewMemOver)
    bool    leasesOver         = false;   ///< the namespace is at or over max-leases on this node (nsLeasesOver)
    LeaseIndexRef  leaseIndex;                ///< the partition's lease index (null table: none)
    bool    leaseIndexReady    = false;       ///< that index reflects every judged record of this leadership (leaseIndexReady)
    LeaseHookWork* leaseWork   = nullptr;     ///< the worker's lease work area (required for a lease topic)
    /// A transient lease topic's rebirth fence (spec A.6): > 0 = a grant is answered Throttled with this retry-after (ns); 0 = grants are
    /// judged as usual.  Updates, revokes and keepalives are never held.  A persisted lease topic never sets it.
    int64_t leaseGrantHoldNs   = 0;
    int32_t txnJudgedRecordsMax = 4096;   ///< the node's bound on a batch the judge holds whole (rule 10)
    int64_t txnRecords         = 0;       ///< a transactional batch: the records its open transaction already has here
    bool    txnJudged          = false;   ///< a transactional batch: the judge judges its transaction whole (vote or marker) without this batch's own intents
};

/// Whether a verdict view of this kind provides the key index (member 0 of a logged set): a lease index is a key index
/// over a wider key.  Task 5 decides the lease hook; `TopicHookFacts::keyIndex` is unchanged.
[[nodiscard]] constexpr bool providesKeyIndex(rune::view::VerdictViewKind k) noexcept {
    return k == rune::view::VerdictViewKind::KeyIndex || k == rune::view::VerdictViewKind::LeaseIndex;
}

/// What the topic says.  Later parts set the members; in Part 03 every member is false.
struct TopicHookFacts {
    bool keyIndex          = false;
    bool autoIncrement     = false;
    bool constrainedSource = false;
    bool verdictSource     = false;
    bool keyIndexActive    = false;   ///< the partition's logged constraint set has the key index as member 0 (P5-4)
    bool autoIncReady      = false;   ///< the partition's id counter is Ready
    bool lease             = false;   ///< a lease topic (spec->lease)
    const rune::protocol::LeaseSpec* leaseSpec = nullptr;   ///< the lease topic's spec; null otherwise
};
[[nodiscard]] inline TopicHookFacts topicHookFactsOf(const PartitionWriteState& state) noexcept {
    TopicHookFacts f;
    f.keyIndex          = state.spec->keyIndex;
    f.autoIncrement     = state.spec->autoIncrement;
    f.autoIncReady      = state.autoInc.ready();
    f.lease             = state.spec->lease.has_value();
    f.leaseSpec         = f.lease ? &*state.spec->lease : nullptr;
    f.verdictSource     = state.isVerdictSource();
    f.constrainedSource = state.isConstrainedSource();
    const ConstraintSet& logged = state.constraintSets.latest();
    f.keyIndexActive    = logged.count > 0 && providesKeyIndex(logged.members[0].kind);
    return f;
}

/// What the record walk found.
struct BatchIntents {
    bool    reservedBits      = false;   ///< a record with attributes & kAttrReservedMask, or intent 3
    bool    explicitKeyIntent = false;   ///< insert with a non-key-0 key, update, removeExisting
    bool    keyZeroCreate     = false;   ///< insert with an all-zero key at keyWidth on an autoIncrement topic
    bool    mixedKeyZero      = false;   ///< keyZeroCreate and at least one record that is not one
    bool    anyIntent         = false;   ///< any record with intent != 0
    int32_t records           = 0;
    bool    plainUpsert       = false;   ///< a live record with intent 0 on an autoIncrement topic
    bool    explicitInsert    = false;   ///< insert with a non-key-0 key on an autoIncrement topic (an import)
    bool    importBadKey      = false;   ///< an import whose id is negative, above the cap, or not this partition's
    bool    autoIncExhausted  = false;   ///< the key-0 batch would pass the counter's cap
    bool    autoIncWindowFull = false;   ///< the key-0 batch's producer cannot be recorded in the assigned-base window
    uint32_t keyZeroCount     = 0;
    uint64_t importMaxN       = 0;       ///< the highest counter value an import carries (0 = none)
    uint32_t leaseGrants      = 0;       ///< lease topic: records that are grants (Put + insert)
};

/// IntentInTxnOrAtomicBatch and TxnOnConstrainedSource are unused since Part 11 (P11-14); their values keep the counter array's indices.
enum class HookRefusal : uint8_t {
    None, ReservedBits, ValidationFailed, VersionAhead, LegacyMode, LevelTooLow, NoKeyIndex,
    ExplicitKeyNeedsIdempotentAndView, IntentInTxnOrAtomicBatch, TxnOnConstrainedSource,
    KeyZeroCreateNeedsIdempotentAll, KeyZeroCreateNeedsView, MixedKeyZeroBatch, ViewBatchTooLarge,
    ViewWithoutReplica, KeyIndexActivating, UpsertOnAutoIncrement, ImportNotGranted, ImportOutOfRange,
    AutoIncExhausted, AutoIncWindowFull, AutoIncActivating, AtomicBatchTooLarge, TxnTooLarge,
    LeaseSessionUnannounced, LeaseUpdateThenRevoke, LeaseRevokeIndexNotReady, LeaseRevokeBehindUpdate, LeaseRevokeBatchTooLarge,
    LeaseClientVersion, LeaseNeedsView, LeaseInTxn, LeaseBatchTooLarge, LeaseBadRecord, LeaseTtlTooLarge, LeaseViewMemOver,
    LeaseMaxLeases, LeaseFenced,
    VersionRetired,   ///< a record's schema version is below the topic's retire floor (#419): SchemaVersionRetired, nothing stored
    Count_
};

struct RequirementVerdict {
    rune::protocol::client::ErrorCode code{};
    HookRefusal                       why = HookRefusal::None;
};

/// The first requirement a write fails, in the order of the spec's rule list.
[[nodiscard]] constexpr RequirementVerdict evaluateRequirements(const BatchIntents& b, const WriteFacts& f, const TopicHookFacts& t) noexcept {
    using rune::protocol::client::ErrorCode;
    using rune::protocol::client::acksAll;
    using rune::protocol::client::acksView;
    const auto refuse = [](HookRefusal why) { return RequirementVerdict{ErrorCode::InvalidRequest, why}; };
    const bool view = acksView(f.acks);
    if (b.reservedBits) return refuse(HookRefusal::ReservedBits);
    if (f.legacyMode && (b.anyIntent || view)) return refuse(HookRefusal::LegacyMode);
    if (!f.levelCheckedWrites && (b.anyIntent || view)) return refuse(HookRefusal::LevelTooLow);
    if (b.anyIntent && !t.keyIndex && !t.lease) return refuse(HookRefusal::NoKeyIndex);
    if (b.explicitKeyIntent && (!f.idempotent || (!view && !f.transactional))) return refuse(HookRefusal::ExplicitKeyNeedsIdempotentAndView);
    if (b.keyZeroCreate) {
        if (b.mixedKeyZero) return refuse(HookRefusal::MixedKeyZeroBatch);
        if (!f.idempotent || !acksAll(f.acks)) return refuse(HookRefusal::KeyZeroCreateNeedsIdempotentAll);
        if (t.constrainedSource && !view && !f.transactional) return refuse(HookRefusal::KeyZeroCreateNeedsView);
    }
    if (t.autoIncrement) {
        if (b.plainUpsert) return refuse(HookRefusal::UpsertOnAutoIncrement);
        if (b.explicitInsert && !f.importGranted)
            return {f.importDenialDeferred ? ErrorCode::AuthUnavailable : ErrorCode::NotAuthorized, HookRefusal::ImportNotGranted};
        if (b.importBadKey) return refuse(HookRefusal::ImportOutOfRange);
        if (b.keyZeroCreate && b.autoIncExhausted) return {ErrorCode::AutoIncrementExhausted, HookRefusal::AutoIncExhausted};
    }
    if (view && b.records > kMaxJudgedBatchRecords && t.verdictSource) return refuse(HookRefusal::ViewBatchTooLarge);
    if (f.atomicBatch && t.verdictSource && b.records > f.txnJudgedRecordsMax) return refuse(HookRefusal::AtomicBatchTooLarge);
    // P11-7: a transaction the judge judges whole is TxnTooLarge past the knob at its vote or marker, and the marker's rejection needs one
    // index entry per record; the batch that takes it past the knob is refused here.  Records are always counted (at append, or by the log
    // scan after a failover).  What escapes: a transaction whose batches were all admitted before the source was judged whole, and the
    // records a transient ring evicted before its rebuild; the judge keeps its loud stall for more than 2^24 of them.
    if (f.transactional && t.verdictSource && (f.txnJudged || b.anyIntent) && f.txnRecords + b.records > f.txnJudgedRecordsMax)
        return {ErrorCode::TxnTooLarge, HookRefusal::TxnTooLarge};
    if (t.autoIncrement) {
        if ((b.keyZeroCreate || b.explicitInsert) && !t.autoIncReady) return {ErrorCode::NotLeader, HookRefusal::AutoIncActivating};
        if (b.keyZeroCreate && b.autoIncWindowFull) return {ErrorCode::NotLeader, HookRefusal::AutoIncWindowFull};
    }
    if (b.anyIntent && !t.keyIndexActive) return {ErrorCode::NotLeader, HookRefusal::KeyIndexActivating};   // P5-4: never applied unjudged; after every definite refusal
    return {};
}

/// The write hook of a typed partition.  `state.spec` must be set.
class CheckedWriteHook {
public:
    static constexpr bool kActive = true;

    CheckedWriteHook(PartitionWriteState& state, const WriteFacts& facts, std::span<uint8_t> scratch) noexcept
        : state_(state), facts_(facts), scratch_(scratch) {}

    [[nodiscard]] rune::protocol::client::ErrorCode check(const uint8_t* batch, std::size_t len, int32_t recordCount) noexcept;

    /// Autoincrement only: appends a due reservation record, then patches the assigned ids and the CRC into a copy of the
    /// key-0 batch in the scratch; an import-only batch just moves the counter.  Any other batch passes through.
    template <class LogT>
    [[nodiscard]] rune::storage::HookStaged stage(LogT& log, const uint8_t* batch, std::size_t len) noexcept;
    void onAppended(int64_t base, int64_t last) noexcept;
    /// #419: adds this batch's records to SpecVersion::producedRecords (onAppended only: a refused batch counts nothing).
    void countProduced_() noexcept;
    /// The produce failed after stage() took the key-0 batch's ids (its data append was refused or threw): they are burned.
    void onProduceFailed() noexcept {
        if (idsTaken_) {
            state_.autoIncStats.idsBurned += keyZeroCount_;
            state_.autoInc.burnedThrough(plan_.firstN + keyZeroCount_ - 1);
        }
        idsTaken_ = false;
    }
    [[nodiscard]] uint64_t assignedBase() const noexcept { return assignedBase_; }
    [[nodiscard]] HookRefusal refusal() const noexcept { return refusal_; }
    /// With a Throttled answer: how long the client should wait before the retry (ns); 0 otherwise.
    [[nodiscard]] int64_t retryAfterNs() const noexcept { return retryAfterNs_; }
    [[nodiscard]] bool checked() const noexcept { return checked_; }
    /// check() ran clean and a record carries an intent.
    [[nodiscard]] bool anyIntent() const noexcept { return anyIntent_; }

private:
    [[nodiscard]] rune::storage::HookStaged patchKeyZeroIds_(const uint8_t* batch, std::size_t len) noexcept;
    template <class LogT>
    [[nodiscard]] rune::storage::HookStaged stageLease_(LogT& log, const uint8_t* batch, std::size_t len) noexcept;
    /// check(), lease topic, after the per-record rules: the same-batch update-then-revoke refusal, then the Throttled retries.
    [[nodiscard]] rune::protocol::client::ErrorCode checkLeaseBatch_(const uint8_t* batch, std::size_t len, uint32_t updates, uint32_t revokes) noexcept;
    static void logLeaseEncode_() noexcept;   // cold
    static void logLeaseScratch_(std::size_t need, std::size_t cap) noexcept;   // cold

    PartitionWriteState& state_;
    WriteFacts           facts_;
    std::span<uint8_t>   scratch_;   // key-0 batches are copied here to patch their ids
    HookRefusal          refusal_ = HookRefusal::None;
    bool                 checked_ = false;
    bool                 anyIntent_ = false;
    AutoIncPlan          plan_;
    uint32_t             keyZeroCount_ = 0;
    uint32_t             leaseGrants_  = 0;   // lease topic: the grants check() found (stage() stamps them)
    uint32_t             leaseUpdates_ = 0;   // lease topic: the updates / revokes check() found (stage() records their rows)
    uint32_t             leaseRevokes_ = 0;
    bool                 leaseUntracked_ = false;   // stage() found an update the index cannot track (not ready, or no row): onAppended raises leaseUntrackedThrough
    uint32_t             leaseTouched_ = 0;   // rows stage() recorded in leaseWork->rids; onAppended() writes lastAppended
    // #419: the accepted records per schema version, for SpecVersion::producedRecords.  The walk keeps one run (the version of the
    // first versioned record and how many share it); a batch of mixed versions sets mixed_ and onAppended re-walks it (rare).
    uint16_t             runVersion_ = 0;
    uint32_t             runCount_   = 0;
    bool                 mixed_      = false;
    const uint8_t*       batch_      = nullptr;   // check()'s batch (it outlives the produce call): countProduced_ re-walks it when mixed_
    std::size_t          batchLen_   = 0;
    int64_t              retryAfterNs_ = 0;
    uint64_t             importMaxN_   = 0;
    uint64_t             assignedBase_ = 0;
    bool                 idsTaken_     = false;   // stage() committed the key-0 ids and onAppended has not run
    std::array<uint8_t, kReservationScratch> resv_;
};

template <class LogT>
rune::storage::HookStaged CheckedWriteHook::stage(LogT& log, const uint8_t* batch, std::size_t len) noexcept {
    using rune::protocol::client::ErrorCode;
    if (state_.spec->lease) [[unlikely]] return stageLease_(log, batch, len);
    if (keyZeroCount_ == 0 && importMaxN_ == 0) return {ErrorCode::OK, batch, len};
    if (plan_.reserve) {
        const std::size_t n = rune::storage::control_record::buildReservation(resv_, plan_.reserveThroughN);
        try {
            int64_t at = 0;
            if (log.appendNoSyncRestamp(resv_.data(), n, 0, at) < 0) return {ErrorCode::TransientLogFull, nullptr, 0};
        } catch (...) {
            return {ErrorCode::Internal, nullptr, 0};
        }
        state_.autoInc.commitReservation(plan_.reserveThroughN);
        ++state_.autoIncStats.reservationsAppended;
    }
    if (keyZeroCount_ == 0) {
        if (plan_.newNextN > state_.autoInc.nextN()) ++state_.autoIncStats.importsMovedCounter;
        state_.autoInc.commitNext(plan_.newNextN);
        return {ErrorCode::OK, batch, len};
    }
    const rune::storage::HookStaged patched = patchKeyZeroIds_(batch, len);
    if (patched.error == ErrorCode::OK) {
        state_.autoInc.commitNext(plan_.newNextN);
        idsTaken_ = true;
    }
    return patched;
}

/// Lease topic (spec 6.4).  One cheap pass finds the rows the batch updates or revokes (recorded for onAppended's lastAppended)
/// and the client revokes to re-encode: a revoke whose key has a row at the record's lease_version carries the FULL row at the
/// topic's latest installed version (R9), so the stored record needs no later schema evolution.  A batch with a re-encoded
/// revoke is rebuilt into the scratch (the lengths change); a batch with grants is copied there and each grant's lease_version
/// is stamped with the offset the append will assign (the log's end offset plus the record's offset delta; this is the
/// partition's single writer, and appendNoSyncRestamp takes base = logEndOffset()) and its TTL raised to minTtlMs; the CRC is
/// recomputed.  A batch with neither is appended as it is (zero-copy).
template <class LogT>
rune::storage::HookStaged CheckedWriteHook::stageLease_(LogT& log, const uint8_t* batch, std::size_t len) noexcept {
    namespace b   = rune::protocol::batch;
    namespace rec = rune::protocol::record;
    using rune::protocol::client::ErrorCode;
    assert(!state_.spec->autoIncrement && "a lease topic has no autoincrement");
    LeaseHookWork* const work = facts_.leaseWork;
    if (work == nullptr) [[unlikely]] return {ErrorCode::Internal, nullptr, 0};
    leaseTouched_ = 0;
    leaseUntracked_ = false;
    const LeaseIndexRef& ix = facts_.leaseIndex;
    if (leaseUpdates_ + leaseRevokes_ != 0 && (ix.table == nullptr || !facts_.leaseIndexReady)) leaseUntracked_ = true;
    uint32_t replacements = 0;
    if (leaseUpdates_ + leaseRevokes_ != 0 && ix.table != nullptr) {
        const bool ok = rune::protocol::forEachRecordSpan(batch, len, [&](const rune::protocol::RecordSpan& r) {
            if (r.index >= work->replace.size()) return;   // check() caps a lease batch at kMaxLeaseBatchRecords
            work->replace[r.index] = 0;
            const rec::LeaseOp op = rec::leaseOpOf(r.attributes);
            if (rec::intentOf(r.attributes) != static_cast<uint8_t>(rec::Intent::Existing)) return;   // a grant
            if (op != rec::LeaseOp::Put && op != rec::LeaseOp::Revoke) return;
            const auto key = rune::view::ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(r.key.data()), r.key.size()});
            uint32_t rid = 0;
            const uint8_t* row = ix.table->find(key, rid);   // one probe: the row and its id
            if (row == nullptr) {
                leaseUntracked_ = true;   // nothing is recorded for a key without a row
            } else {
                work->rids[leaseTouched_++] = rid;
                if (op == rec::LeaseOp::Revoke) {
                    if (leaseRowVersion(row, *ix.layout) == leaseRecordVersion(r.value)) {
                        work->replace[r.index] = 1;
                        ++replacements;
                    }
                }
            }
        });
        if (!ok) [[unlikely]] return {ErrorCode::Internal, nullptr, 0};
    }
    if (replacements == 0 && leaseGrants_ == 0) return {ErrorCode::OK, batch, len};
    if (len > scratch_.size()) [[unlikely]] {
        logLeaseScratch_(len, scratch_.size());
        return {ErrorCode::Internal, nullptr, 0};
    }
    uint8_t* const out = scratch_.data();
    const uint8_t* cur = batch;
    std::size_t curLen = len;
    if (replacements != 0) {
        const LeaseWriter latest = latestLeaseWriter(*state_.spec);
        if (latest.descriptor == nullptr) [[unlikely]] return {ErrorCode::Internal, nullptr, 0};
        std::size_t count = 0;
        const bool ok = rune::protocol::forEachRecordSpan(batch, len, [&](const rune::protocol::RecordSpan& r) {
            if (count < work->spans.size()) work->spans[count] = r;
            ++count;
        });
        if (!ok || count > work->spans.size()) [[unlikely]] return {ErrorCode::Internal, nullptr, 0};
        bool encodeFailed = false;
        // The latest version's slot map (cold fill once per install, then a lookup): re-encoding is O(fields), not O(fields^2).
        const uint16_t* const slotOf = ix.cover != nullptr
            ? leaseSlotMapFor(*ix.cover, latest.version, latest.installSeq, *ix.layout, *ix.outDesc, *latest.descriptor) : nullptr;
        const std::size_t n = rune::protocol::rebuildBatchReplacingValues(
            std::span<const uint8_t>(batch, len), std::span<const rune::protocol::RecordSpan>(work->spans.data(), count),
            [&](std::size_t i) -> std::span<const uint8_t> {
                const rune::protocol::RecordSpan& r = work->spans[i];
                if (work->replace[i] == 0) return {};
                const auto key = rune::view::ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(r.key.data()), r.key.size()});
                const uint8_t* row = ix.table->get(key);
                const std::size_t m = row == nullptr ? 0 : encodeLeaseValue(row, *ix.layout, *ix.outDesc, *latest.descriptor, latest.version,
                                                                              work->valueTmp.data(), work->valueTmp.size(), slotOf);
                if (m == 0) { encodeFailed = true; return {}; }
                return std::span<const uint8_t>(work->valueTmp.data(), m);
            },
            scratch_);
        if (n == 0 || encodeFailed) [[unlikely]] {
            assert(false && "a lease revoke rebuild must fit the scratch");
            if (encodeFailed) logLeaseEncode_(); else logLeaseScratch_(len, scratch_.size());
            return {ErrorCode::InvalidRequest, nullptr, 0};
        }
        cur = out;
        curLen = n;
    }
    if (leaseGrants_ != 0) {
        if (cur != out) std::memcpy(out, batch, len);
        const int64_t base = log.logEndOffset();
        const uint64_t minTtl = state_.spec->lease->minTtlMs;
        const bool ok = rune::protocol::forEachRecordSpan(out, curLen, [&](const rune::protocol::RecordSpan& r) {
            if (rec::leaseOpOf(r.attributes) != rec::LeaseOp::Put || rec::intentOf(r.attributes) != static_cast<uint8_t>(rec::Intent::Insert)) return;
            uint8_t* v = out + (r.value.data() - reinterpret_cast<const char*>(out));
            const uint64_t version = static_cast<uint64_t>(base) + static_cast<uint64_t>(r.offsetDelta);
            for (std::size_t i = 0; i < 8; ++i) v[rec::kLeaseVersionValueOffset + i] = static_cast<uint8_t>(version >> (8 * i));
            uint64_t ttl = 0;
            for (std::size_t i = 0; i < 8; ++i) ttl |= static_cast<uint64_t>(v[rec::kLeaseTtlValueOffset + i]) << (8 * i);
            if (ttl < minTtl)
                for (std::size_t i = 0; i < 8; ++i) v[rec::kLeaseTtlValueOffset + i] = static_cast<uint8_t>(minTtl >> (8 * i));
        });
        if (!ok) [[unlikely]] return {ErrorCode::Internal, nullptr, 0};
        const uint32_t crc = rune::protocol::crc32c(out + b::kAttributesOffset, curLen - b::kAttributesOffset);
        for (std::size_t i = 0; i < 4; ++i) out[b::kCrcOffset + i] = static_cast<uint8_t>(crc >> (8 * i));
        cur = out;
    }
    return {ErrorCode::OK, cur, curLen};
}

}  // namespace rune::node

namespace rune::storage {

extern template ProduceOutcome handleProduceBatch<Log, rune::node::CheckedWriteHook>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, Log&,
    ProducerStateManager&, bool, int, int, uint8_t, bool, rune::node::CheckedWriteHook&) noexcept;
extern template ProduceOutcome handleProduceBatch<InMemoryLog, rune::node::CheckedWriteHook>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, InMemoryLog&,
    ProducerStateManager&, bool, int, int, uint8_t, bool, rune::node::CheckedWriteHook&) noexcept;

}  // namespace rune::storage
