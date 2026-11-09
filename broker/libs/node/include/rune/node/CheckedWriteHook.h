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
#include <rune/storage/ControlRecordLayout.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>
#include <rune/storage/ProducePath.h>
#include <rune/storage/WriteHookPolicy.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace rune::node {

inline constexpr int32_t kMaxJudgedBatchRecords = rune::protocol::client::kMaxJudgedBatchRecords;
inline constexpr std::size_t kReservationScratch = 80;
static_assert(rune::storage::control_record::reservationBatchBytes() <= kReservationScratch);

/// What the produce frame and the worker say about one write.
struct WriteFacts {
    uint8_t acks               = 0;       ///< the Produce.acks wire byte (bit 0 All, bit 1 View)
    bool    idempotent         = false;
    bool    transactional      = false;
    bool    atomicBatch        = false;
    bool    importGranted      = false;
    int64_t producerId         = -1;
    bool    legacyMode         = false;
    bool    levelCheckedWrites = false;
    int32_t txnJudgedRecordsMax = 4096;   ///< the node's bound on a batch the judge holds whole (rule 10)
    int64_t txnRecords         = 0;       ///< a transactional batch: the records its open transaction already has here
    bool    txnJudged          = false;   ///< a transactional batch: the judge judges its transaction whole (vote or marker) without this batch's own intents
};

/// What the topic says.  Later parts set the members; in Part 03 every member is false.
struct TopicHookFacts {
    bool keyIndex          = false;
    bool autoIncrement     = false;
    bool constrainedSource = false;
    bool verdictSource     = false;
    bool keyIndexActive    = false;   ///< the partition's logged constraint set has the key index as member 0 (P5-4)
    bool autoIncReady      = false;   ///< the partition's id counter is Ready
};
[[nodiscard]] inline TopicHookFacts topicHookFactsOf(const PartitionWriteState& state) noexcept {
    TopicHookFacts f;
    f.keyIndex          = state.spec->keyIndex;
    f.autoIncrement     = state.spec->autoIncrement;
    f.autoIncReady      = state.autoInc.ready();
    f.verdictSource     = state.isVerdictSource();
    f.constrainedSource = state.isConstrainedSource();
    const ConstraintSet& logged = state.constraintSets.latest();
    f.keyIndexActive    = logged.count > 0 && logged.members[0].kind == rune::view::VerdictViewKind::KeyIndex;
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
};

/// IntentInTxnOrAtomicBatch and TxnOnConstrainedSource are unused since Part 11 (P11-14); their values keep the counter array's indices.
enum class HookRefusal : uint8_t {
    None, ReservedBits, ValidationFailed, VersionAhead, LegacyMode, LevelTooLow, NoKeyIndex,
    ExplicitKeyNeedsIdempotentAndView, IntentInTxnOrAtomicBatch, TxnOnConstrainedSource,
    KeyZeroCreateNeedsIdempotentAll, KeyZeroCreateNeedsView, MixedKeyZeroBatch, ViewBatchTooLarge,
    ViewWithoutReplica, KeyIndexActivating, UpsertOnAutoIncrement, ImportNotGranted, ImportOutOfRange,
    AutoIncExhausted, AutoIncWindowFull, AutoIncActivating, AtomicBatchTooLarge, Count_
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
    if (b.anyIntent && !t.keyIndex) return refuse(HookRefusal::NoKeyIndex);
    if (b.explicitKeyIntent && (!f.idempotent || (!view && !f.transactional))) return refuse(HookRefusal::ExplicitKeyNeedsIdempotentAndView);
    if (b.keyZeroCreate) {
        if (b.mixedKeyZero) return refuse(HookRefusal::MixedKeyZeroBatch);
        if (!f.idempotent || !acksAll(f.acks)) return refuse(HookRefusal::KeyZeroCreateNeedsIdempotentAll);
        if (t.constrainedSource && !view && !f.transactional) return refuse(HookRefusal::KeyZeroCreateNeedsView);
    }
    if (t.autoIncrement) {
        if (b.plainUpsert) return refuse(HookRefusal::UpsertOnAutoIncrement);
        if (b.explicitInsert && !f.importGranted) return {ErrorCode::NotAuthorized, HookRefusal::ImportNotGranted};
        if (b.importBadKey) return refuse(HookRefusal::ImportOutOfRange);
        if (b.keyZeroCreate && b.autoIncExhausted) return {ErrorCode::AutoIncrementExhausted, HookRefusal::AutoIncExhausted};
    }
    if (view && b.records > kMaxJudgedBatchRecords && t.verdictSource) return refuse(HookRefusal::ViewBatchTooLarge);
    if (f.atomicBatch && t.verdictSource && b.records > f.txnJudgedRecordsMax) return refuse(HookRefusal::AtomicBatchTooLarge);
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
    [[nodiscard]] bool checked() const noexcept { return checked_; }
    /// check() ran clean and a record carries an intent.
    [[nodiscard]] bool anyIntent() const noexcept { return anyIntent_; }

private:
    [[nodiscard]] rune::storage::HookStaged patchKeyZeroIds_(const uint8_t* batch, std::size_t len) noexcept;

    PartitionWriteState& state_;
    WriteFacts           facts_;
    std::span<uint8_t>   scratch_;   // key-0 batches are copied here to patch their ids
    HookRefusal          refusal_ = HookRefusal::None;
    bool                 checked_ = false;
    bool                 anyIntent_ = false;
    AutoIncPlan          plan_;
    uint32_t             keyZeroCount_ = 0;
    uint64_t             importMaxN_   = 0;
    uint64_t             assignedBase_ = 0;
    bool                 idsTaken_     = false;   // stage() committed the key-0 ids and onAppended has not run
    std::array<uint8_t, kReservationScratch> resv_;
};

template <class LogT>
rune::storage::HookStaged CheckedWriteHook::stage(LogT& log, const uint8_t* batch, std::size_t len) noexcept {
    using rune::protocol::client::ErrorCode;
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

}  // namespace rune::node

namespace rune::storage {

extern template ProduceOutcome handleProduceBatch<Log, rune::node::CheckedWriteHook>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, Log&,
    ProducerStateManager&, bool, int, int, uint8_t, bool, rune::node::CheckedWriteHook&) noexcept;
extern template ProduceOutcome handleProduceBatch<InMemoryLog, rune::node::CheckedWriteHook>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, InMemoryLog&,
    ProducerStateManager&, bool, int, int, uint8_t, bool, rune::node::CheckedWriteHook&) noexcept;

}  // namespace rune::storage
