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

uint64_t loadLe64(std::string_view v, std::size_t off) noexcept {
    uint64_t x = 0;
    std::memcpy(&x, v.data() + off, sizeof(x));   // little-endian host, like the rest of the wire
    return x;
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
    leaseGrants_ = 0;
    leaseUpdates_ = 0;
    leaseRevokes_ = 0;
    retryAfterNs_ = 0;

    // Lease topic (spec 6.4, in the spec's order; each refusal is definite and nothing is stored).  Namespace suspension
    // is refused before the produce core (refuseSuspended_), so there is no row for it here.
    const bool lease = topic.lease;
    if (lease) [[unlikely]] {
        const auto refuseLease = [&](HookRefusal why) { refusal_ = why; return pc::ErrorCode::InvalidRequest; };
        if (facts_.clientVersion < 0) {   // the session's announce has not reached this worker yet: retriable, nothing stored
            refusal_ = HookRefusal::LeaseSessionUnannounced;
            return pc::ErrorCode::NotLeader;
        }
        if (facts_.clientVersion < pc::kClientVersionLeases || !facts_.levelLeases) return refuseLease(HookRefusal::LeaseClientVersion);
        if (!pc::acksView(facts_.acks)) return refuseLease(HookRefusal::LeaseNeedsView);
        if (facts_.transactional) return refuseLease(HookRefusal::LeaseInTxn);   // explicit: the View bit is cleared for a transaction, but the rule must not depend on that
        if (recordCount < 0 || static_cast<std::size_t>(recordCount) > rec::kMaxLeaseBatchRecords) return refuseLease(HookRefusal::LeaseBatchTooLarge);
    }
    const uint8_t reservedMask = lease ? static_cast<uint8_t>(rec::kAttrReservedMask & ~rec::kAttrLeaseOpMask) : rec::kAttrReservedMask;

    const bool walked = rune::protocol::forEachRecordSpan(batch, len, [&](const rune::protocol::RecordSpan& r) {
        if (stopped != pc::ErrorCode::OK) return;
        const uint8_t intent = rec::intentOf(r.attributes);
        if ((r.attributes & reservedMask) != 0 || (!lease && intent == 3) || (intent == static_cast<uint8_t>(rec::Intent::Insert) && r.value.empty())) {   // an insert needs a value (P5-5)
            intents.reservedBits = true;
            stopped = pc::ErrorCode::InvalidRequest;
            refusal_ = HookRefusal::ReservedBits;
            return;
        }
        if (lease) [[unlikely]] {
            const auto bad = [&](pc::ErrorCode code, HookRefusal why) { stopped = code; refusal_ = why; };
            const rec::LeaseOp op = rec::leaseOpOf(r.attributes);
            constexpr auto kInsert = static_cast<uint8_t>(rec::Intent::Insert), kExisting = static_cast<uint8_t>(rec::Intent::Existing);
            const bool grant = op == rec::LeaseOp::Put && intent == kInsert;
            const bool update = op == rec::LeaseOp::Put && intent == kExisting;
            const bool revoke = op == rec::LeaseOp::Revoke && intent == kExisting;
            if (!grant && !update && !revoke) return bad(pc::ErrorCode::InvalidRequest, HookRefusal::LeaseBadRecord);   // upsert, intent 3, revoke+insert, client Expire / Checkpoint
            if (r.value.size() < 4 + rec::kLeaseSystemBytes) return bad(pc::ErrorCode::InvalidRequest, HookRefusal::LeaseBadRecord);
            if (loadLe64(r.value, rec::kLeaseRemainingValueOffset) != 0) return bad(pc::ErrorCode::InvalidRequest, HookRefusal::LeaseBadRecord);
            // A version of 0 is a live lease id (a partition's first grant has offset 0), so only a negative one is refused; the spec's "<= 0" is relaxed by the coordinator.
            if (!grant && static_cast<int64_t>(loadLe64(r.value, rec::kLeaseVersionValueOffset)) < 0) return bad(pc::ErrorCode::InvalidRequest, HookRefusal::LeaseBadRecord);
            if (grant) {
                if (loadLe64(r.value, rec::kLeaseTtlValueOffset) > topic.leaseSpec->maxTtlMs) return bad(pc::ErrorCode::LeaseTtlTooLarge, HookRefusal::LeaseTtlTooLarge);
                ++intents.leaseGrants;
            } else if (update) {
                ++leaseUpdates_;
            } else {
                ++leaseRevokes_;
            }
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
    if (lease && leaseUpdates_ != 0 && leaseRevokes_ != 0) [[unlikely]] {
        if (const pc::ErrorCode e = checkLeaseBatch_(batch, len, leaseUpdates_, 0); e != pc::ErrorCode::OK) return e;
    }
    // Every grant is refused while the namespace's view memory is over its limit (the hook has no index lookup; a grant
    // of a live key would be KeyExists anyway).  The max-leases limit is Task 21.
    if (intents.leaseGrants > 0 && facts_.viewMemOver) [[unlikely]] {
        refusal_ = HookRefusal::LeaseViewMemOver;
        return pc::ErrorCode::NamespaceLimitExceeded;
    }
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
    if (v.code == pc::ErrorCode::OK) leaseGrants_ = intents.leaseGrants;
    // The retriable lease answers come after every definite refusal: a revoke is re-encoded from the row, so the row must be
    // current (L19).  Nothing is stored; the client retries after retryAfterNs().
    if (lease && v.code == pc::ErrorCode::OK && leaseRevokes_ != 0) [[unlikely]] return checkLeaseBatch_(batch, len, 0, leaseRevokes_);
    return v.code;
}

pc::ErrorCode CheckedWriteHook::checkLeaseBatch_(const uint8_t* batch, std::size_t len, uint32_t updates, uint32_t revokes) noexcept {
    LeaseHookWork* const work = facts_.leaseWork;
    if (work == nullptr) [[unlikely]] {
        refusal_ = HookRefusal::LeaseBadRecord;
        return pc::ErrorCode::Internal;
    }
    constexpr auto kExisting = static_cast<uint8_t>(rec::Intent::Existing);
    if (updates != 0) {
        // A batch that updates a key and LATER revokes it is refused: the revoke is re-encoded from the row, which does not yet
        // hold the update, so the stored revoke would carry the pre-update columns.  Revoke-then-update is accepted (the update
        // then gets LeaseNotFound from the judge).  An open-addressing set of key hashes, cleared by a generation stamp.
        using Slot = LeaseHookWork::Slot;
        if (++work->gen == 0) { for (Slot& sl : work->set) sl.gen = 0; work->gen = 1; }
        constexpr std::size_t mask = LeaseHookWork::kSetSlots - 1;
        const auto hashOf = [](std::string_view k) noexcept {
            uint64_t h = 0xcbf29ce484222325ull;
            for (const char c : k) h = (h ^ static_cast<uint8_t>(c)) * 0x100000001b3ull;
            return h;
        };
        bool clash = false;
        (void)rune::protocol::forEachRecordSpan(batch, len, [&](const rune::protocol::RecordSpan& r) {
            if (clash || rec::intentOf(r.attributes) != kExisting) return;
            const rec::LeaseOp op = rec::leaseOpOf(r.attributes);
            const uint64_t h = hashOf(r.key);
            std::size_t i = h & mask;
            bool found = false;
            while (work->set[i].gen == work->gen) {
                const Slot& sl = work->set[i];
                if (sl.hash == h && sl.keyLen == r.key.size() && std::memcmp(sl.key, r.key.data(), r.key.size()) == 0) { found = true; break; }
                i = (i + 1) & mask;
            }
            if (op == rec::LeaseOp::Put) {
                if (!found) work->set[i] = Slot{work->gen, static_cast<uint32_t>(r.key.size()), h, r.key.data()};
            } else if (op == rec::LeaseOp::Revoke && found) {
                clash = true;
            }
        });
        if (clash) {
            refusal_ = HookRefusal::LeaseUpdateThenRevoke;
            return pc::ErrorCode::InvalidRequest;
        }
    }
    if (revokes != 0) {
        const LeaseIndexRef& ix = facts_.leaseIndex;
        if (ix.table == nullptr || !facts_.leaseIndexReady) {
            refusal_ = HookRefusal::LeaseRevokeIndexNotReady;
            retryAfterNs_ = kLeaseRetryAfterNs;
            return pc::ErrorCode::Throttled;
        }
        pc::ErrorCode result = pc::ErrorCode::OK;
        (void)rune::protocol::forEachRecordSpan(batch, len, [&](const rune::protocol::RecordSpan& r) {
            if (result != pc::ErrorCode::OK || rec::leaseOpOf(r.attributes) != rec::LeaseOp::Revoke) return;
            const auto key = rune::view::ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(r.key.data()), r.key.size()});
            uint32_t rid = 0;
            // An update or revoke of this key is appended but not yet applied to the row: the re-encode would miss it.
            if (ix.table->ridOf(key, rid) && leaseLastAppended(*ix.table, rid) >= state_.judgedThrough) {
                refusal_ = HookRefusal::LeaseRevokeBehindUpdate;
                retryAfterNs_ = kLeaseRetryAfterNs;
                result = pc::ErrorCode::Throttled;
            }
        });
        return result;
    }
    return pc::ErrorCode::OK;
}

void CheckedWriteHook::logLeaseScratch_(std::size_t need, std::size_t cap) noexcept {
    spdlog::error("CheckedWriteHook: lease batch of {} bytes does not fit the {} byte scratch", need, cap);
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

void CheckedWriteHook::onAppended(int64_t base, int64_t last) noexcept {
    if (leaseTouched_ != 0) [[unlikely]] {   // lastAppended is written only once the append succeeded (R8: grants record nothing)
        for (uint32_t i = 0; i < leaseTouched_; ++i) setLeaseLastAppended(*facts_.leaseIndex.table, facts_.leaseWork->rids[i], last);
        leaseTouched_ = 0;
    }
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
