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
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>
#include <rune/storage/ProducePath.h>
#include <rune/storage/WriteHookPolicy.h>

#include <cstddef>
#include <cstdint>
#include <span>

namespace rune::node {

inline constexpr int32_t kMaxJudgedBatchRecords = rune::protocol::client::kMaxJudgedBatchRecords;

/// What the produce frame and the worker say about one write.
struct WriteFacts {
    uint8_t acks               = 0;       ///< the Produce.acks wire byte (bit 0 All, bit 1 View)
    bool    idempotent         = false;
    bool    transactional      = false;
    bool    atomicBatch        = false;
    bool    importGranted      = false;
    bool    legacyMode         = false;
    bool    levelCheckedWrites = false;
};

/// What the topic says.  Later parts set the members; in Part 03 every member is false.
struct TopicHookFacts {
    bool keyIndex          = false;
    bool autoIncrement     = false;
    bool constrainedSource = false;
    bool verdictSource     = false;
};
[[nodiscard]] inline TopicHookFacts topicHookFactsOf(const PartitionWriteState& state) noexcept {
    TopicHookFacts f;
    f.verdictSource     = state.isVerdictSource();
    f.constrainedSource = state.isConstrainedSource();
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
};

enum class HookRefusal : uint8_t {
    None, ReservedBits, ValidationFailed, VersionAhead, LegacyMode, LevelTooLow, NoKeyIndex,
    ExplicitKeyNeedsIdempotentAndView, IntentInTxnOrAtomicBatch, TxnOnConstrainedSource,
    KeyZeroCreateNeedsIdempotentAll, KeyZeroCreateNeedsView, MixedKeyZeroBatch, ViewBatchTooLarge,
    ViewWithoutReplica, Count_
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
    if (b.explicitKeyIntent && (!f.idempotent || !view)) return refuse(HookRefusal::ExplicitKeyNeedsIdempotentAndView);
    if (b.explicitKeyIntent && (f.transactional || f.atomicBatch)) return refuse(HookRefusal::IntentInTxnOrAtomicBatch);
    if ((f.transactional || f.atomicBatch) && t.constrainedSource) return refuse(HookRefusal::TxnOnConstrainedSource);
    if (b.keyZeroCreate) {
        if (b.mixedKeyZero) return refuse(HookRefusal::MixedKeyZeroBatch);
        if (!f.idempotent || !acksAll(f.acks)) return refuse(HookRefusal::KeyZeroCreateNeedsIdempotentAll);
        if (t.constrainedSource && !view) return refuse(HookRefusal::KeyZeroCreateNeedsView);
    }
    if (view && b.records > kMaxJudgedBatchRecords && t.verdictSource) return refuse(HookRefusal::ViewBatchTooLarge);
    return {};
}

/// The write hook of a typed partition.  `state.spec` must be set.
class CheckedWriteHook {
public:
    static constexpr bool kActive = true;

    CheckedWriteHook(PartitionWriteState& state, const WriteFacts& facts, std::span<uint8_t> scratch) noexcept
        : state_(state), facts_(facts), scratch_(scratch) {}

    [[nodiscard]] rune::protocol::client::ErrorCode check(const uint8_t* batch, std::size_t len, int32_t recordCount) noexcept;

    template <class LogT>
    [[nodiscard]] rune::storage::HookStaged stage(LogT&, const uint8_t* batch, std::size_t len) noexcept {
        return {rune::protocol::client::ErrorCode::OK, batch, len};
    }
    void onAppended(int64_t, int64_t) noexcept {}
    [[nodiscard]] uint64_t assignedBase() const noexcept { return 0; }
    [[nodiscard]] HookRefusal refusal() const noexcept { return refusal_; }
    [[nodiscard]] bool checked() const noexcept { return checked_; }

private:
    PartitionWriteState& state_;
    WriteFacts           facts_;
    std::span<uint8_t>   scratch_;   // 06 stages rewritten batches here
    HookRefusal          refusal_ = HookRefusal::None;
    bool                 checked_ = false;
};

}  // namespace rune::node

namespace rune::storage {

extern template ProduceOutcome handleProduceBatch<Log, rune::node::CheckedWriteHook>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, Log&,
    ProducerStateManager&, bool, int, int, uint8_t, bool, rune::node::CheckedWriteHook&) noexcept;
extern template ProduceOutcome handleProduceBatch<InMemoryLog, rune::node::CheckedWriteHook>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, InMemoryLog&,
    ProducerStateManager&, bool, int, int, uint8_t, bool, rune::node::CheckedWriteHook&) noexcept;

}  // namespace rune::storage
