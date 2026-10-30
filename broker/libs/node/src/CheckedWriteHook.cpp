// CheckedWriteHook.cpp - the record walk of CheckedWriteHook::check and the handleProduceBatch instantiations for it.
#include <rune/node/CheckedWriteHook.h>
#include <rune/storage/ProducePathImpl.h>

namespace rune::node {

namespace pc  = rune::protocol::client;
namespace rec = rune::protocol::record;

pc::ErrorCode CheckedWriteHook::check(const uint8_t* batch, std::size_t len, int32_t recordCount) noexcept {
    checked_ = true;
    const TopicWriteSpec& spec = *state_.spec;
    const TopicHookFacts  topic = topicHookFactsOf(state_);
    const int16_t level = facts_.levelCheckedWrites ? rune::replication::MetadataFeature::kCheckedWrites : rune::replication::MetadataFeature::kBase;

    BatchIntents intents;
    intents.records = recordCount;
    int32_t keyZeroCreates = 0;
    pc::ErrorCode stopped = pc::ErrorCode::OK;

    const bool walked = rune::protocol::forEachRecordSpan(batch, len, [&](const rune::protocol::RecordSpan& r) {
        if (stopped != pc::ErrorCode::OK) return;
        if ((r.attributes & rec::kAttrReservedMask) != 0 || rec::intentOf(r.attributes) == 3) {
            intents.reservedBits = true;
            stopped = pc::ErrorCode::InvalidRequest;
            refusal_ = HookRefusal::ReservedBits;
            return;
        }
        switch (checkTypedRecord(spec, r, level)) {
        case TypedCheck::Accept: break;
        case TypedCheck::Reject:
            stopped = pc::ErrorCode::SchemaValidationFailed;
            refusal_ = HookRefusal::ValidationFailed;
            return;
        case TypedCheck::VersionAhead:
            stopped = pc::ErrorCode::NotLeader;
            refusal_ = HookRefusal::VersionAhead;
            return;
        }
        const uint8_t intent = rec::intentOf(r.attributes);
        if (intent == static_cast<uint8_t>(rec::Intent::Upsert)) return;
        intents.anyIntent = true;
        bool zeroKey = r.key.size() == spec.keyWidth && spec.keyWidth != 0;
        for (std::size_t i = 0; zeroKey && i < r.key.size(); ++i) zeroKey = r.key[i] == '\0';
        if (intent == static_cast<uint8_t>(rec::Intent::Insert) && zeroKey && topic.autoIncrement) ++keyZeroCreates;
        else intents.explicitKeyIntent = true;
    });
    if (stopped != pc::ErrorCode::OK) return stopped;
    if (!walked && (facts_.levelCheckedWrites || spec.legacyValidate)) {
        refusal_ = HookRefusal::ValidationFailed;
        return pc::ErrorCode::SchemaValidationFailed;
    }
    intents.keyZeroCreate = keyZeroCreates > 0;
    intents.mixedKeyZero  = keyZeroCreates > 0 && keyZeroCreates < recordCount;

    const RequirementVerdict v = evaluateRequirements(intents, facts_, topic);
    refusal_ = v.why;
    return v.code;
}

}  // namespace rune::node

namespace rune::storage {

template ProduceOutcome handleProduceBatch<Log, rune::node::CheckedWriteHook>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, Log&,
    ProducerStateManager&, bool, int, int, uint8_t, bool, rune::node::CheckedWriteHook&) noexcept;
template ProduceOutcome handleProduceBatch<InMemoryLog, rune::node::CheckedWriteHook>(
    const uint8_t*, std::size_t, int32_t, int32_t, int64_t, int16_t, int32_t, InMemoryLog&,
    ProducerStateManager&, bool, int, int, uint8_t, bool, rune::node::CheckedWriteHook&) noexcept;

}  // namespace rune::storage
