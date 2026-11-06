#pragma once
// StatsMessages.h — GetStats (91) and StatsResponse (92): the counters of one topic or view partition, served by
// the partition leader's owning worker; sent only to a session >= kClientVersionCheckedWrites.
//
//   GetStats (91)  client->broker   BODY_FIXED = 9
//     [24]      scope     : uint8 (StatsScope)
//     [25..28]  objectId  : int32 (topic id for scope 1, view topic id for scope 2)
//     [29..32]  partition : int32
//   StatsResponse (92)  broker->client   BODY_FIXED = 4
//     [24..25]  status : uint16
//     [26..27]  count  : uint16
//     count x {statId u16, value i64}                         (10 bytes each, statId strictly ascending)
//     [nConstraints u16]  nConstraints x {constraintId u32, rejections u64}   (12 bytes each)
// Decoders are zero-copy and require the exact frame length.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include "Codec.h"
#include "MessageHeader.h"
#include "Protocol.h"

namespace rune::protocol::client {

enum class StatsScope : uint8_t { TopicPartition = 1, ViewPartition = 2 };

/// Append-only. 1-31 are topic-partition scope, 32+ view-partition scope.
enum class StatId : uint16_t {
    rejectionsKeyExists          = 1,
    rejectionsKeyNotFound        = 2,
    rejectionsConstraint         = 3,
    verdictLag                   = 4,
    verdictWatermark             = 5,
    verdictMarkersWritten        = 6,
    rejectedNotCompacted         = 7,
    autoIncrementCounter         = 8,
    autoIncrementReservedThrough = 9,
    autoIncrementIdsBurned       = 10,
    txnVotesYes                  = 11,
    txnVotesNo                   = 12,
    txnVotesNoConstraintViolation = 13,
    txnVotesNoKeyExists          = 14,
    txnVotesNoKeyNotFound        = 15,
    txnVotesNoTxnTooLarge        = 16,
    txnVoteLatencyNsMax          = 17,
    txnVoteLatencyNsTotal        = 18,
    foldPausedOnTxnLockNs        = 19,
    txnMarkerWithoutVote         = 20,
    uniqueConflicts              = 32,
    scanRows                     = 33,
    scanPages                    = 34,
    transientRecordsEvicted      = 35,
    transientSourceResets        = 36,
};

[[nodiscard]] constexpr std::string_view statName(StatId id) noexcept {
    switch (id) {
        case StatId::rejectionsKeyExists:           return "rejectionsKeyExists";
        case StatId::rejectionsKeyNotFound:         return "rejectionsKeyNotFound";
        case StatId::rejectionsConstraint:          return "rejectionsConstraint";
        case StatId::verdictLag:                    return "verdictLag";
        case StatId::verdictWatermark:              return "verdictWatermark";
        case StatId::verdictMarkersWritten:         return "verdictMarkersWritten";
        case StatId::rejectedNotCompacted:          return "rejectedNotCompacted";
        case StatId::autoIncrementCounter:          return "autoIncrementCounter";
        case StatId::autoIncrementReservedThrough:  return "autoIncrementReservedThrough";
        case StatId::autoIncrementIdsBurned:        return "autoIncrementIdsBurned";
        case StatId::txnVotesYes:                   return "txnVotesYes";
        case StatId::txnVotesNo:                    return "txnVotesNo";
        case StatId::txnVotesNoConstraintViolation: return "txnVotesNoConstraintViolation";
        case StatId::txnVotesNoKeyExists:           return "txnVotesNoKeyExists";
        case StatId::txnVotesNoKeyNotFound:         return "txnVotesNoKeyNotFound";
        case StatId::txnVotesNoTxnTooLarge:         return "txnVotesNoTxnTooLarge";
        case StatId::txnVoteLatencyNsMax:           return "txnVoteLatencyNsMax";
        case StatId::txnVoteLatencyNsTotal:         return "txnVoteLatencyNsTotal";
        case StatId::foldPausedOnTxnLockNs:         return "foldPausedOnTxnLockNs";
        case StatId::txnMarkerWithoutVote:          return "txnMarkerWithoutVote";
        case StatId::uniqueConflicts:               return "uniqueConflicts";
        case StatId::scanRows:                      return "scanRows";
        case StatId::scanPages:                     return "scanPages";
        case StatId::transientRecordsEvicted:       return "transientRecordsEvicted";
        case StatId::transientSourceResets:         return "transientSourceResets";
    }
    return "unknown";
}

struct GetStats {
    int64_t    sessionId     = 0;
    int64_t    correlationId = 0;
    StatsScope scope         = StatsScope::TopicPartition;
    int32_t    objectId      = -1;
    int32_t    partition     = -1;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::GetStats); // 91
    static constexpr uint16_t BODY_FIXED  = 9;

    std::size_t encodedSize() const noexcept { return MessageHeader::ENCODED_SIZE + BODY_FIXED; }

    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        buf[fixedBase] = static_cast<uint8_t>(scope);
        detail::putLE32(buf, fixedBase + 1, objectId);
        detail::putLE32(buf, fixedBase + 5, partition);
        return MessageHeader::ENCODED_SIZE + BODY_FIXED;
    }

    /// nullopt on a scope other than 1 or 2, a truncation, or trailing bytes.
    static std::optional<GetStats> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail != fixedBase + BODY_FIXED) return std::nullopt;
        const uint8_t scope = buf[fixedBase];
        if (scope != static_cast<uint8_t>(StatsScope::TopicPartition) &&
            scope != static_cast<uint8_t>(StatsScope::ViewPartition)) return std::nullopt;

        GetStats msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        msg.scope         = static_cast<StatsScope>(scope);
        msg.objectId      = detail::getLE32(buf, static_cast<int>(fixedBase) + 1);
        msg.partition     = detail::getLE32(buf, static_cast<int>(fixedBase) + 5);
        return msg;
    }
};

struct StatsResponse {
    int64_t  sessionId          = 0;
    int64_t  correlationId      = 0;
    uint16_t status             = 0;
    uint16_t count              = 0;
    std::span<const uint8_t> statBytes;        // count x {statId u16, value i64}
    uint16_t constraintCount    = 0;
    std::span<const uint8_t> constraintBytes;  // constraintCount x {constraintId u32, rejections u64}

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::StatsResponse); // 92
    static constexpr uint16_t BODY_FIXED  = 4;
    static constexpr uint16_t kOk = 0, kUnknownObject = 4, kInvalidRequest = 5, kBusy = 6, kNotAuthorized = 45;
    static constexpr std::size_t kStatBytes = 10, kConstraintBytes = 12;
    static constexpr uint16_t kMaxStats = 64;
    static constexpr uint16_t kMaxConstraintEntries = 256;  // VerdictSourceStats::perConstraint capacity
    static constexpr std::size_t kMaxEncodedSize = MessageHeader::ENCODED_SIZE + BODY_FIXED + kMaxStats * kStatBytes +
                                                   2 + kMaxConstraintEntries * kConstraintBytes;
    static_assert(kMaxEncodedSize <= 68 * 1024);

    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + statBytes.size() + 2u + constraintBytes.size();
    }

    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr;
        hdr.blockLength   = BODY_FIXED;
        hdr.templateId    = TEMPLATE_ID;
        hdr.schemaId      = kSchemaId;
        hdr.version       = kProtocolVersion;
        hdr.sessionId     = sessionId;
        hdr.correlationId = correlationId;
        hdr.encode(buf);

        constexpr int fixedBase = static_cast<int>(MessageHeader::ENCODED_SIZE);
        detail::putLE16(buf, fixedBase + 0, status);
        detail::putLE16(buf, fixedBase + 2, count);

        std::size_t pos = static_cast<std::size_t>(fixedBase) + BODY_FIXED;
        if (!statBytes.empty()) std::memcpy(buf + pos, statBytes.data(), statBytes.size());
        pos += statBytes.size();
        detail::putLE16(buf, static_cast<int>(pos), constraintCount);
        pos += 2;
        if (!constraintBytes.empty()) std::memcpy(buf + pos, constraintBytes.data(), constraintBytes.size());
        return pos + constraintBytes.size();
    }

    /// Zero-copy: the spans point into buf, which must outlive the result.  nullopt on a count above kMaxStats,
    /// stat ids not strictly ascending, more than kMaxConstraintEntries constraints, a truncation, or trailing bytes.
    static std::optional<StatsResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto hdrOpt = MessageHeader::decode(buf, avail);
        if (!hdrOpt) return std::nullopt;
        const MessageHeader& hdr = *hdrOpt;
        if (hdr.schemaId   != kSchemaId)   return std::nullopt;
        if (hdr.templateId != TEMPLATE_ID) return std::nullopt;

        constexpr std::size_t fixedBase = MessageHeader::ENCODED_SIZE;
        if (avail < fixedBase + BODY_FIXED) return std::nullopt;

        StatsResponse msg;
        msg.sessionId     = hdr.sessionId;
        msg.correlationId = hdr.correlationId;
        msg.status        = detail::getLE16(buf, static_cast<int>(fixedBase) + 0);
        msg.count         = detail::getLE16(buf, static_cast<int>(fixedBase) + 2);
        if (msg.count > kMaxStats) return std::nullopt;

        std::size_t pos = fixedBase + BODY_FIXED;
        const std::size_t statLen = msg.count * kStatBytes;
        if (avail - pos < statLen + 2) return std::nullopt;
        for (uint16_t i = 0; i < msg.count; ++i) {
            if (i > 0 && detail::getLE16(buf, static_cast<int>(pos + i * kStatBytes)) <=
                         detail::getLE16(buf, static_cast<int>(pos + (i - 1) * kStatBytes))) return std::nullopt;
        }
        msg.statBytes = {buf + pos, statLen};
        pos += statLen;

        msg.constraintCount = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;
        if (msg.constraintCount > kMaxConstraintEntries) return std::nullopt;
        const std::size_t constraintLen = msg.constraintCount * kConstraintBytes;
        if (avail - pos != constraintLen) return std::nullopt;
        msg.constraintBytes = {buf + pos, constraintLen};
        return msg;
    }

    /// Calls fn(uint16_t statId, int64_t value) for each of the count stats, in ascending id order.
    template <class Fn>
    void forEachStat(Fn&& fn) const noexcept {
        for (uint16_t i = 0; i < count; ++i) {
            const int at = static_cast<int>(i * kStatBytes);
            fn(detail::getLE16(statBytes.data(), at), detail::getLE64(statBytes.data(), at + 2));
        }
    }

    /// Calls fn(uint32_t constraintId, uint64_t rejections) for each of the constraintCount entries.
    template <class Fn>
    void forEachConstraint(Fn&& fn) const noexcept {
        for (uint16_t i = 0; i < constraintCount; ++i) {
            const int at = static_cast<int>(i * kConstraintBytes);
            fn(static_cast<uint32_t>(detail::getLE32(constraintBytes.data(), at)),
               static_cast<uint64_t>(detail::getLE64(constraintBytes.data(), at + 4)));
        }
    }
};

} // namespace rune::protocol::client
