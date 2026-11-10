// CheckedWriteHook.cpp - the record walk of CheckedWriteHook::check and the handleProduceBatch instantiations for it.
#include <rune/node/CheckedWriteHook.h>
#include <rune/protocol/Crc32c.h>
#include <rune/storage/ProducePathImpl.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstring>
#include <optional>
#include <string_view>

namespace rune::node {

namespace pc  = rune::protocol::client;
namespace rec = rune::protocol::record;

namespace {

/// The counter value of an import's key, or nullopt: negative, above the cap, or not an id of this partition.
std::optional<uint64_t> importN(const TopicWriteSpec& spec, const AutoIncState& autoInc, std::string_view key) noexcept {
    if (key.empty() || key.size() > sizeof(uint64_t)) return std::nullopt;
    uint64_t id = 0;
    std::memcpy(&id, key.data(), key.size());
    const bool signedKey = spec.keyType == rune::codec::PrimType::Int32 || spec.keyType == rune::codec::PrimType::Int64;
    if (signedKey && (static_cast<uint8_t>(key.back()) & 0x80) != 0) return std::nullopt;
    const auto partitions = static_cast<uint64_t>(autoInc.partitions());
    if (id % partitions != static_cast<uint64_t>(autoInc.partition()) || id / partitions > autoInc.nMax()) return std::nullopt;
    return id / partitions;
}

void putLe(uint8_t* p, uint64_t v, std::size_t width) noexcept {
    for (std::size_t i = 0; i < width; ++i) p[i] = static_cast<uint8_t>(v >> (8 * i));
}

}  // namespace

pc::ErrorCode CheckedWriteHook::check(const uint8_t* batch, std::size_t len, int32_t recordCount) noexcept {
    checked_ = true;
    const TopicWriteSpec& spec = *state_.spec;
    const TopicHookFacts  topic = topicHookFactsOf(state_);
    const int16_t level = facts_.levelCheckedWrites ? rune::replication::MetadataFeature::kCheckedWrites : rune::replication::MetadataFeature::kBase;

    BatchIntents intents;
    intents.records = recordCount;
    int32_t keyZeroCreates = 0;
    const bool autoInc = spec.autoIncrement;
    const bool autoIncReady = autoInc && state_.autoInc.ready();
    pc::ErrorCode stopped = pc::ErrorCode::OK;

    const bool walked = rune::protocol::forEachRecordSpan(batch, len, [&](const rune::protocol::RecordSpan& r) {
        if (stopped != pc::ErrorCode::OK) return;
        const uint8_t intent = rec::intentOf(r.attributes);
        if ((r.attributes & rec::kAttrReservedMask) != 0 || intent == 3 || (intent == static_cast<uint8_t>(rec::Intent::Insert) && r.value.empty())) {   // an insert needs a value (P5-5)
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
        if (intent == static_cast<uint8_t>(rec::Intent::Upsert)) {
            if (autoInc && !r.value.empty()) [[unlikely]] intents.plainUpsert = true;
            return;
        }
        intents.anyIntent = true;
        bool zeroKey = r.key.size() == spec.keyWidth && spec.keyWidth != 0;
        for (std::size_t i = 0; zeroKey && i < r.key.size(); ++i) zeroKey = r.key[i] == '\0';
        if (intent == static_cast<uint8_t>(rec::Intent::Insert) && zeroKey && topic.autoIncrement) ++keyZeroCreates;
        else {
            intents.explicitKeyIntent = true;
            if (intent == static_cast<uint8_t>(rec::Intent::Insert) && autoInc) [[unlikely]] {
                intents.explicitInsert = true;
                if (autoIncReady) {   // before the counter is ready the batch waits, whatever its keys are
                    const auto n = importN(spec, state_.autoInc, r.key);
                    if (n) intents.importMaxN = std::max(intents.importMaxN, *n);
                    else intents.importBadKey = true;
                }
            }
        }
    });
    if (stopped != pc::ErrorCode::OK) return stopped;
    if (!walked && (facts_.levelCheckedWrites || spec.legacyValidate)) {
        refusal_ = HookRefusal::ValidationFailed;
        return pc::ErrorCode::SchemaValidationFailed;
    }
    anyIntent_ = intents.anyIntent;
    intents.keyZeroCreate = keyZeroCreates > 0;
    intents.mixedKeyZero  = keyZeroCreates > 0 && keyZeroCreates < recordCount;
    if (autoInc) [[unlikely]] {
        intents.keyZeroCount = static_cast<uint32_t>(keyZeroCreates);
        if (autoIncReady && !intents.importBadKey && (intents.keyZeroCount > 0 || intents.importMaxN > 0)) {
            plan_ = state_.autoInc.plan(intents.keyZeroCount, intents.importMaxN);
            intents.autoIncExhausted = plan_.exhausted;
        }
        intents.autoIncWindowFull = intents.keyZeroCount > 0 && !state_.assignedBases.canAccept(facts_.producerId);
        keyZeroCount_ = intents.keyZeroCount;
        importMaxN_   = intents.importMaxN;
    }

    const RequirementVerdict v = evaluateRequirements(intents, facts_, topic);
    refusal_ = v.why;
    return v.code;
}

rune::storage::HookStaged CheckedWriteHook::patchKeyZeroIds_(const uint8_t* batch, std::size_t len) noexcept {
    if (len > scratch_.size()) [[unlikely]] {
        spdlog::error("CheckedWriteHook: key-0 batch of {} bytes exceeds the {} byte scratch", len, scratch_.size());
        return {pc::ErrorCode::Internal, nullptr, 0};
    }
    namespace b = rune::protocol::batch;
    const TopicWriteSpec& spec = *state_.spec;
    uint8_t* out = scratch_.data();
    std::memcpy(out, batch, len);
    const int32_t partitions = state_.autoInc.partitions();
    const int32_t partition  = state_.autoInc.partition();
    const bool patched = rune::protocol::forEachRecordSpan(batch, len, [&](const rune::protocol::RecordSpan& r) {
        const uint64_t id = autoIncId(plan_.firstN + r.index, partitions, partition);
        putLe(out + (r.key.data() - reinterpret_cast<const char*>(batch)), id, spec.keyWidth);
        const uint16_t version = static_cast<uint16_t>(static_cast<uint8_t>(r.value[2]) | (static_cast<uint8_t>(r.value[3]) << 8));
        putLe(out + (r.value.data() - reinterpret_cast<const char*>(batch)) + spec.versionFor(version)->keyValueOffset, id, spec.keyWidth);
    });
    if (!patched) [[unlikely]] return {pc::ErrorCode::Internal, nullptr, 0};
    putLe(out + b::kCrcOffset, rune::protocol::crc32c(out + b::kAttributesOffset, len - b::kAttributesOffset), 4);
    return {pc::ErrorCode::OK, out, len};
}

void CheckedWriteHook::onAppended(int64_t base, int64_t) noexcept {
    if (keyZeroCount_ == 0) return;
    idsTaken_ = false;
    assignedBase_ = autoIncId(plan_.firstN, state_.autoInc.partitions(), state_.autoInc.partition());
    ++state_.autoIncStats.keyZeroBatches;
    if (facts_.producerId >= 0) state_.assignedBases.record(facts_.producerId, base, assignedBase_);
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
