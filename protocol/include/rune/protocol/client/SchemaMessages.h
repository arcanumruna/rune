#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string_view>
#include "../CatalogTypes.h"
#include "Codec.h"
#include "MessageHeader.h"
#include "Protocol.h"

namespace rune::protocol::client {

// ---------------------------------------------------------------------------
// Catalog history (client version 8) — the DDL extension trailer.
//
// Layout (LE): statement(varstr32: u32 length — a statement can exceed 64 KiB),
//   clientName(varstr), requestIdHi:u64, requestIdLo:u64, hasReplayedFrom:u8,
//   then ONLY when hasReplayedFrom == 1: clusterId(varstr), offset:i64,
//   principal(varstr).  hasReplayedFrom is 0 or 1; anything else is refused.
// The request fingerprint is computed by the catalog leader (Task 9) — it is
// never carried on (nor trusted from) the wire.
// ---------------------------------------------------------------------------
struct DdlExtension {
    std::string_view statement;              // the original DDL text ("" for API-issued)
    std::string_view clientName;
    rune::protocol::RequestId requestId{};   // 128-bit, stable across retries (ruling 9, F14)
    uint8_t          hasReplayedFrom = 0;    // 0 | 1
    std::string_view replayedClusterId;
    int64_t          replayedOffset = -1;
    std::string_view replayedPrincipal;
    bool operator==(const DdlExtension&) const = default;
};

/// False when a field does not fit its wire width or hasReplayedFrom is not 0/1.
[[nodiscard]] constexpr bool extFits(const DdlExtension& e) noexcept {
    if (!fitsVarLen32(e.statement.size()) || !fitsVarLen16(e.clientName.size())) return false;
    if (e.hasReplayedFrom > 1) return false;
    if (e.hasReplayedFrom == 1 &&
        (!fitsVarLen16(e.replayedClusterId.size()) || !fitsVarLen16(e.replayedPrincipal.size())))
        return false;
    return true;
}

[[nodiscard]] constexpr std::size_t extEncodedSize(const DdlExtension& e) noexcept {
    std::size_t n = 4u + e.statement.size() + 2u + e.clientName.size() + 16u + 1u;
    if (e.hasReplayedFrom == 1) n += 2u + e.replayedClusterId.size() + 8u + 2u + e.replayedPrincipal.size();
    return n;
}

/// Writes the extension (the caller checked extFits); returns bytes written.
inline std::size_t putExt(uint8_t* buf, const DdlExtension& e) noexcept {
    std::size_t pos = 0;
    pos += putVarStr32(buf + pos, e.statement);
    pos += putVarStr(buf + pos, e.clientName);
    detail::putLE64(buf, static_cast<int>(pos), static_cast<int64_t>(e.requestId.hi)); pos += 8;
    detail::putLE64(buf, static_cast<int>(pos), static_cast<int64_t>(e.requestId.lo)); pos += 8;
    buf[pos++] = e.hasReplayedFrom;
    if (e.hasReplayedFrom == 1) {
        pos += putVarStr(buf + pos, e.replayedClusterId);
        detail::putLE64(buf, static_cast<int>(pos), e.replayedOffset); pos += 8;
        pos += putVarStr(buf + pos, e.replayedPrincipal);
    }
    return pos;
}

/// Reads the extension at buf[pos..avail); advances pos.  False (no over-read)
/// on truncation or a hasReplayedFrom byte other than 0/1.
inline bool getExt(const uint8_t* buf, std::size_t avail, std::size_t& pos, DdlExtension& out) noexcept {
    DdlExtension e;
    if (!getVarStr32(buf, avail, pos, e.statement)) return false;
    if (!getVarStr(buf, avail, pos, e.clientName)) return false;
    if (avail < pos + 17u) return false;
    e.requestId.hi = static_cast<uint64_t>(detail::getLE64(buf, static_cast<int>(pos))); pos += 8;
    e.requestId.lo = static_cast<uint64_t>(detail::getLE64(buf, static_cast<int>(pos))); pos += 8;
    e.hasReplayedFrom = buf[pos++];
    if (e.hasReplayedFrom > 1) return false;   // append-only: never cast
    if (e.hasReplayedFrom == 1) {
        if (!getVarStr(buf, avail, pos, e.replayedClusterId)) return false;
        if (avail < pos + 8u) return false;
        e.replayedOffset = detail::getLE64(buf, static_cast<int>(pos)); pos += 8;
        if (!getVarStr(buf, avail, pos, e.replayedPrincipal)) return false;
    }
    out = e;
    return true;
}

// ---------------------------------------------------------------------------
// TopicConfigWire — the zero-copy wire view of the ONE TopicConfig definition
// (CatalogTypes.h, F32).  Layout (LE): cleanupPolicy:u8, retentionMs:i64,
//   retentionBytes:i64, deleteRetentionMs:i64, minCleanableDirtyRatioPpm:i32,
//   segmentRollBytes:i64, minCompactionLagMs:i64, maxCompactionLagMs:i64,
//   retentionCheckMs:i64, fileDeleteDelayMs:i64, durability:u8,
//   durabilityTier:u8, coPartitionGroup(varstr), partitionKey(varstr).
// Enum bytes are range-checked (cleanupPolicy 0..2, durability 0..1,
// durabilityTier 0..2); value bounds are the catalog's rules (Task 9), not the wire's.
// ---------------------------------------------------------------------------
using TopicConfigWire = rune::protocol::TopicConfigView;

inline constexpr uint8_t     kMaxCleanupPolicy     = 2;   // 0 Delete, 1 Compact, 2 CompactDelete
inline constexpr uint8_t     kMaxDurability        = 1;   // 0 Majority, 1 AllInSync
inline constexpr uint8_t     kMaxDurabilityTier    = 2;   // 0 Leader, 1 Majority, 2 Strict
inline constexpr std::size_t kTopicConfigWireFixed = 1u + 8u * 3 + 4u + 8u * 5 + 1u + 1u;   // 71

[[nodiscard]] constexpr bool topicConfigFits(const TopicConfigWire& c) noexcept {
    return c.cleanupPolicy <= kMaxCleanupPolicy && c.durability <= kMaxDurability &&
           c.durabilityTier <= kMaxDurabilityTier &&
           fitsVarLen16(c.coPartitionGroup.size()) && fitsVarLen16(c.partitionKey.size());
}

[[nodiscard]] constexpr std::size_t topicConfigEncodedSize(const TopicConfigWire& c) noexcept {
    return kTopicConfigWireFixed + 2u + c.coPartitionGroup.size() + 2u + c.partitionKey.size();
}

/// Writes the config (the caller checked topicConfigFits); returns bytes written.
inline std::size_t putTopicConfig(uint8_t* buf, const TopicConfigWire& c) noexcept {
    std::size_t pos = 0;
    buf[pos++] = c.cleanupPolicy;
    detail::putLE64(buf, static_cast<int>(pos), c.retentionMs); pos += 8;
    detail::putLE64(buf, static_cast<int>(pos), c.retentionBytes); pos += 8;
    detail::putLE64(buf, static_cast<int>(pos), c.deleteRetentionMs); pos += 8;
    detail::putLE32(buf, static_cast<int>(pos), c.minCleanableDirtyRatioPpm); pos += 4;
    detail::putLE64(buf, static_cast<int>(pos), c.segmentRollBytes); pos += 8;
    detail::putLE64(buf, static_cast<int>(pos), c.minCompactionLagMs); pos += 8;
    detail::putLE64(buf, static_cast<int>(pos), c.maxCompactionLagMs); pos += 8;
    detail::putLE64(buf, static_cast<int>(pos), c.retentionCheckMs); pos += 8;
    detail::putLE64(buf, static_cast<int>(pos), c.fileDeleteDelayMs); pos += 8;
    buf[pos++] = c.durability;
    buf[pos++] = c.durabilityTier;
    pos += putVarStr(buf + pos, c.coPartitionGroup);
    pos += putVarStr(buf + pos, c.partitionKey);
    return pos;
}

/// Reads a config at buf[pos..avail); advances pos.  False (no over-read) on
/// truncation or an out-of-range enum byte.
inline bool getTopicConfig(const uint8_t* buf, std::size_t avail, std::size_t& pos,
                           TopicConfigWire& out) noexcept {
    if (avail < pos + kTopicConfigWireFixed) return false;
    TopicConfigWire c;
    c.cleanupPolicy = buf[pos++];
    c.retentionMs = detail::getLE64(buf, static_cast<int>(pos)); pos += 8;
    c.retentionBytes = detail::getLE64(buf, static_cast<int>(pos)); pos += 8;
    c.deleteRetentionMs = detail::getLE64(buf, static_cast<int>(pos)); pos += 8;
    c.minCleanableDirtyRatioPpm = detail::getLE32(buf, static_cast<int>(pos)); pos += 4;
    c.segmentRollBytes = detail::getLE64(buf, static_cast<int>(pos)); pos += 8;
    c.minCompactionLagMs = detail::getLE64(buf, static_cast<int>(pos)); pos += 8;
    c.maxCompactionLagMs = detail::getLE64(buf, static_cast<int>(pos)); pos += 8;
    c.retentionCheckMs = detail::getLE64(buf, static_cast<int>(pos)); pos += 8;
    c.fileDeleteDelayMs = detail::getLE64(buf, static_cast<int>(pos)); pos += 8;
    c.durability = buf[pos++];
    c.durabilityTier = buf[pos++];
    if (c.cleanupPolicy > kMaxCleanupPolicy || c.durability > kMaxDurability ||
        c.durabilityTier > kMaxDurabilityTier) return false;   // append-only: never cast
    if (!getVarStr(buf, avail, pos, c.coPartitionGroup)) return false;
    if (!getVarStr(buf, avail, pos, c.partitionKey)) return false;
    out = c;
    return true;
}

// DefineTopic (64): body = partitionCount:u32, validate:u8, then var fields
//   topicName(varstr), keyField(varstr), descriptorBlob(varbytes32),
//   [persistence:u8]  - Phase 2b E, OPTIONAL trailing byte: written only when
//   non-zero (a disk frame is byte-identical to the pre-2b layout); absent on
//   decode => disk (old clients).  Values are append-only: 0 = disk,
//   1 = transient; anything else is rejected.
//   [v8 trailer]  - catalog history: when hasExt, the persistence byte is
//   ALWAYS written, then replicationFactor:i16, TopicConfigWire, DdlExtension.
//   Decode: after the persistence byte, pos == avail => no extension (hasExt
//   false, replicationFactor 0, default config); otherwise the whole trailer
//   must decode exactly to the end of the frame (anything else => nullopt).
// encode() returns 0 (explicit failure, never a truncated field) when !fits().
struct DefineTopic {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view topicName;
    uint32_t partitionCount = 1;
    std::string_view keyField;
    bool validate = false;
    const uint8_t* descriptorBlob = nullptr;
    uint32_t descriptorLen = 0;
    uint8_t persistence = 0;   // kPersistenceDisk | kPersistenceTransient
    // v8 (catalog history) — on the wire only when hasExt.
    int16_t replicationFactor = 0;   // 0 = the cluster default (F31: i16)
    TopicConfigWire config{};
    bool hasExt = false;
    DdlExtension ext{};

    static constexpr uint8_t kPersistenceDisk      = 0;
    static constexpr uint8_t kPersistenceTransient = 1;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::DefineTopic);
    static constexpr uint16_t BODY_FIXED  = 5;  // u32 partitionCount + u8 validate

    /// False when a field exceeds its wire width, an enum byte is out of range,
    /// or v8 fields are set without hasExt (they would be silently dropped).
    [[nodiscard]] bool fits() const noexcept {
        if (!fitsVarLen16(topicName.size()) || !fitsVarLen16(keyField.size())) return false;
        if (persistence > kPersistenceTransient) return false;
        if (!hasExt) return replicationFactor == 0 && config == TopicConfigWire{};
        return topicConfigFits(config) && extFits(ext);
    }
    std::size_t encodedSize() const noexcept {
        const std::size_t base = MessageHeader::ENCODED_SIZE + BODY_FIXED
             + 2u + topicName.size()
             + 2u + keyField.size()
             + 4u + descriptorLen;
        if (hasExt) return base + 1u + 2u + topicConfigEncodedSize(config) + extEncodedSize(ext);
        return base + (persistence != kPersistenceDisk ? 1u : 0u);
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        if (!fits()) return 0;
        MessageHeader hdr; hdr.blockLength = BODY_FIXED; hdr.templateId = TEMPLATE_ID;
        hdr.schemaId = kSchemaId; hdr.version = kProtocolVersion;
        hdr.sessionId = sessionId; hdr.correlationId = correlationId; hdr.encode(buf);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        detail::putLE32(buf, static_cast<int>(pos), static_cast<int32_t>(partitionCount)); pos += 4;
        buf[pos++] = validate ? 1 : 0;
        pos += putVarStr(buf + pos, topicName);
        pos += putVarStr(buf + pos, keyField);
        pos += putVarBytes32(buf + pos, descriptorBlob, descriptorLen);
        if (hasExt) {
            buf[pos++] = persistence;   // always present when the trailer follows
            detail::putLE16s(buf, static_cast<int>(pos), replicationFactor); pos += 2;
            pos += putTopicConfig(buf + pos, config);
            pos += putExt(buf + pos, ext);
        } else if (persistence != kPersistenceDisk) {
            buf[pos++] = persistence;
        }
        return pos;
    }
    static std::optional<DefineTopic> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = MessageHeader::decode(buf, avail);
        if (!h || h->schemaId != kSchemaId || h->templateId != TEMPLATE_ID) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        if (avail < pos + BODY_FIXED) return std::nullopt;
        DefineTopic m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        m.partitionCount = static_cast<uint32_t>(detail::getLE32(buf, static_cast<int>(pos))); pos += 4;
        m.validate = buf[pos++] != 0;
        if (!getVarStr(buf, avail, pos, m.topicName)) return std::nullopt;
        if (!getVarStr(buf, avail, pos, m.keyField)) return std::nullopt;
        if (!getVarBytes32(buf, avail, pos, m.descriptorBlob, m.descriptorLen)) return std::nullopt;
        if (pos < avail) {
            const uint8_t p = buf[pos++];
            if (p > kPersistenceTransient) return std::nullopt;   // append-only: never cast
            m.persistence = p;
        }
        if (pos == avail) return m;   // pre-v8 frame: no extension
        if (avail < pos + 2u) return std::nullopt;
        m.replicationFactor = detail::getLE16s(buf, static_cast<int>(pos)); pos += 2;
        if (!getTopicConfig(buf, avail, pos, m.config)) return std::nullopt;
        if (!getExt(buf, avail, pos, m.ext)) return std::nullopt;
        if (pos != avail) return std::nullopt;   // the trailer decodes exactly to the end
        m.hasExt = true;
        return m;
    }
};

// DefineTopicResponse (65): body = topicId:i32, version:u16, errorCode:u16.
struct DefineTopicResponse {
    int64_t sessionId = 0, correlationId = 0;
    int32_t topicId = -1; uint16_t version = 0; uint16_t errorCode = 0;
    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::DefineTopicResponse);
    static constexpr uint16_t BODY_FIXED  = 8;
    std::size_t encodedSize() const noexcept { return MessageHeader::ENCODED_SIZE + BODY_FIXED; }
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr; hdr.blockLength = BODY_FIXED; hdr.templateId = TEMPLATE_ID;
        hdr.schemaId = kSchemaId; hdr.version = kProtocolVersion;
        hdr.sessionId = sessionId; hdr.correlationId = correlationId; hdr.encode(buf);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        detail::putLE32(buf, static_cast<int>(pos), topicId); pos += 4;
        detail::putLE16(buf, static_cast<int>(pos), version); pos += 2;
        detail::putLE16(buf, static_cast<int>(pos), errorCode); pos += 2;
        return pos;
    }
    static std::optional<DefineTopicResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = MessageHeader::decode(buf, avail);
        if (!h || h->schemaId != kSchemaId || h->templateId != TEMPLATE_ID) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        if (avail < pos + BODY_FIXED) return std::nullopt;
        DefineTopicResponse m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        m.topicId = detail::getLE32(buf, static_cast<int>(pos)); pos += 4;
        m.version = detail::getLE16(buf, static_cast<int>(pos)); pos += 2;
        m.errorCode = detail::getLE16(buf, static_cast<int>(pos)); pos += 2;
        return m;
    }
};

// GetTopicSchema (66): body = requestedFingerprint:u64 (0 = latest), then
//   topicName(varstr).
struct GetTopicSchema {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view topicName;
    uint64_t requestedFingerprint = 0;  // 0 = latest
    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::GetTopicSchema);
    static constexpr uint16_t BODY_FIXED  = 8;  // u64 requestedFingerprint
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 2u + topicName.size();
    }
    /// encode() returns 0 (explicit failure, never a truncated name) when !fits().
    [[nodiscard]] bool fits() const noexcept { return fitsVarLen16(topicName.size()); }
    std::size_t encode(uint8_t* buf) const noexcept {
        if (!fits()) return 0;
        MessageHeader hdr; hdr.blockLength = BODY_FIXED; hdr.templateId = TEMPLATE_ID;
        hdr.schemaId = kSchemaId; hdr.version = kProtocolVersion;
        hdr.sessionId = sessionId; hdr.correlationId = correlationId; hdr.encode(buf);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        detail::putLE64(buf, static_cast<int>(pos), static_cast<int64_t>(requestedFingerprint)); pos += 8;
        pos += putVarStr(buf + pos, topicName);
        return pos;
    }
    static std::optional<GetTopicSchema> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = MessageHeader::decode(buf, avail);
        if (!h || h->schemaId != kSchemaId || h->templateId != TEMPLATE_ID) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        if (avail < pos + BODY_FIXED) return std::nullopt;
        GetTopicSchema m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        m.requestedFingerprint = static_cast<uint64_t>(detail::getLE64(buf, static_cast<int>(pos))); pos += 8;
        if (!getVarStr(buf, avail, pos, m.topicName)) return std::nullopt;
        return m;
    }
};

// TopicSchemaResponse (67): body = topicId:i32, version:u16, errorCode:u16, then
//   descriptorBlob(varbytes32), then (Phase 2b Part C, APPENDED) partitionCount:i32.
// `version` is the schema version bound to the returned descriptorBlob (the
// requested fingerprint's version, or latest when requestedFingerprint was 0).
// `partitionCount` is the TOPIC's partition count (typed clients route records
// by key over it).  It is a TRAILING field after the var data, so blockLength /
// BODY_FIXED are unchanged: a pre-2b decoder stops after the blob and ignores
// it, and a pre-2b frame (no trailing bytes) decodes here with
// partitionCount = 0 ("not reported").
struct TopicSchemaResponse {
    int64_t sessionId = 0, correlationId = 0;
    int32_t topicId = -1; uint16_t version = 0; uint16_t errorCode = 0;
    const uint8_t* descriptorBlob = nullptr; uint32_t descriptorLen = 0;
    int32_t partitionCount = 0;  // Phase 2b Part C; 0 = not reported (pre-2b broker)
    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::TopicSchemaResponse);
    static constexpr uint16_t BODY_FIXED  = 8;
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 4u + descriptorLen + 4u;
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr; hdr.blockLength = BODY_FIXED; hdr.templateId = TEMPLATE_ID;
        hdr.schemaId = kSchemaId; hdr.version = kProtocolVersion;
        hdr.sessionId = sessionId; hdr.correlationId = correlationId; hdr.encode(buf);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        detail::putLE32(buf, static_cast<int>(pos), topicId); pos += 4;
        detail::putLE16(buf, static_cast<int>(pos), version); pos += 2;
        detail::putLE16(buf, static_cast<int>(pos), errorCode); pos += 2;
        pos += putVarBytes32(buf + pos, descriptorBlob, descriptorLen);
        detail::putLE32(buf, static_cast<int>(pos), partitionCount); pos += 4;
        return pos;
    }
    static std::optional<TopicSchemaResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = MessageHeader::decode(buf, avail);
        if (!h || h->schemaId != kSchemaId || h->templateId != TEMPLATE_ID) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        if (avail < pos + BODY_FIXED) return std::nullopt;
        TopicSchemaResponse m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        m.topicId = detail::getLE32(buf, static_cast<int>(pos)); pos += 4;
        m.version = detail::getLE16(buf, static_cast<int>(pos)); pos += 2;
        m.errorCode = detail::getLE16(buf, static_cast<int>(pos)); pos += 2;
        if (!getVarBytes32(buf, avail, pos, m.descriptorBlob, m.descriptorLen)) return std::nullopt;
        // Trailing (Phase 2b): absent / cut short on a pre-2b frame -> 0.
        if (avail >= pos + 4) {
            m.partitionCount = detail::getLE32(buf, static_cast<int>(pos)); pos += 4;
        }
        return m;
    }
};

// AlterTopic (68): body = expectedVersion:u16, then var fields
//   topicName(varstr), opsBlob(varbytes32), then (v8, only when hasExt) the
//   DdlExtension, which must decode exactly to the end of the frame.
// encode() returns 0 (explicit failure, never a truncated field) when !fits().
struct AlterTopic {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view topicName;
    uint16_t expectedVersion = 0;
    const uint8_t* opsBlob = nullptr;
    uint32_t opsLen = 0;
    bool hasExt = false;   // v8 (catalog history)
    DdlExtension ext{};

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::AlterTopic);
    static constexpr uint16_t BODY_FIXED  = 2;  // u16 expectedVersion

    [[nodiscard]] bool fits() const noexcept {
        return fitsVarLen16(topicName.size()) && (!hasExt || extFits(ext));
    }
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED
             + 2u + topicName.size()
             + 4u + opsLen
             + (hasExt ? extEncodedSize(ext) : 0u);
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        if (!fits()) return 0;
        MessageHeader hdr; hdr.blockLength = BODY_FIXED; hdr.templateId = TEMPLATE_ID;
        hdr.schemaId = kSchemaId; hdr.version = kProtocolVersion;
        hdr.sessionId = sessionId; hdr.correlationId = correlationId; hdr.encode(buf);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        detail::putLE16(buf, static_cast<int>(pos), expectedVersion); pos += 2;
        pos += putVarStr(buf + pos, topicName);
        pos += putVarBytes32(buf + pos, opsBlob, opsLen);
        if (hasExt) pos += putExt(buf + pos, ext);
        return pos;
    }
    static std::optional<AlterTopic> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = MessageHeader::decode(buf, avail);
        if (!h || h->schemaId != kSchemaId || h->templateId != TEMPLATE_ID) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        if (avail < pos + BODY_FIXED) return std::nullopt;
        AlterTopic m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        m.expectedVersion = detail::getLE16(buf, static_cast<int>(pos)); pos += 2;
        if (!getVarStr(buf, avail, pos, m.topicName)) return std::nullopt;
        if (!getVarBytes32(buf, avail, pos, m.opsBlob, m.opsLen)) return std::nullopt;
        if (pos == avail) return m;   // pre-v8 frame: no extension
        if (!getExt(buf, avail, pos, m.ext) || pos != avail) return std::nullopt;
        m.hasExt = true;
        return m;
    }
};

// AlterTopicResponse (69): body = topicId:i32, newVersion:u16, errorCode:u16.
struct AlterTopicResponse {
    int64_t sessionId = 0, correlationId = 0;
    int32_t topicId = -1; uint16_t newVersion = 0; uint16_t errorCode = 0;
    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::AlterTopicResponse);
    static constexpr uint16_t BODY_FIXED  = 8;
    std::size_t encodedSize() const noexcept { return MessageHeader::ENCODED_SIZE + BODY_FIXED; }
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr; hdr.blockLength = BODY_FIXED; hdr.templateId = TEMPLATE_ID;
        hdr.schemaId = kSchemaId; hdr.version = kProtocolVersion;
        hdr.sessionId = sessionId; hdr.correlationId = correlationId; hdr.encode(buf);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        detail::putLE32(buf, static_cast<int>(pos), topicId); pos += 4;
        detail::putLE16(buf, static_cast<int>(pos), newVersion); pos += 2;
        detail::putLE16(buf, static_cast<int>(pos), errorCode); pos += 2;
        return pos;
    }
    static std::optional<AlterTopicResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = MessageHeader::decode(buf, avail);
        if (!h || h->schemaId != kSchemaId || h->templateId != TEMPLATE_ID) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        if (avail < pos + BODY_FIXED) return std::nullopt;
        AlterTopicResponse m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        m.topicId = detail::getLE32(buf, static_cast<int>(pos)); pos += 4;
        m.newVersion = detail::getLE16(buf, static_cast<int>(pos)); pos += 2;
        m.errorCode = detail::getLE16(buf, static_cast<int>(pos)); pos += 2;
        return m;
    }
};

// CreateView (70): body = persistence:u8, then var fields
//   viewName(varstr), keyField(varstr), planBlob(varbytes32).
// planBlob carries the authored ViewPlan (serializeAuthored): the single
// source's topicId+keyField, the output fields, and the ViewSizing knobs
// (chunkBytes/capacity/maxRows) all ride inside it — this message only
// carries the view's own name/keyField/persistence plus the opaque plan.
// v8 (catalog history): when hasExt, the DdlExtension follows the planBlob and
// must decode exactly to the end of the frame.
// encode() returns 0 (explicit failure, never a truncated field) when !fits().
struct CreateView {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view viewName;
    std::string_view keyField;
    uint8_t persistence = 0;
    const uint8_t* planBlob = nullptr;
    uint32_t planLen = 0;
    bool hasExt = false;   // v8 (catalog history)
    DdlExtension ext{};

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::CreateView);
    static constexpr uint16_t BODY_FIXED  = 1;  // u8 persistence

    [[nodiscard]] bool fits() const noexcept {
        return fitsVarLen16(viewName.size()) && fitsVarLen16(keyField.size()) && persistence <= 1 &&
               (!hasExt || extFits(ext));
    }
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED
             + 2u + viewName.size()
             + 2u + keyField.size()
             + 4u + planLen
             + (hasExt ? extEncodedSize(ext) : 0u);
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        if (!fits()) return 0;
        MessageHeader hdr; hdr.blockLength = BODY_FIXED; hdr.templateId = TEMPLATE_ID;
        hdr.schemaId = kSchemaId; hdr.version = kProtocolVersion;
        hdr.sessionId = sessionId; hdr.correlationId = correlationId; hdr.encode(buf);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        buf[pos++] = persistence;
        pos += putVarStr(buf + pos, viewName);
        pos += putVarStr(buf + pos, keyField);
        pos += putVarBytes32(buf + pos, planBlob, planLen);
        if (hasExt) pos += putExt(buf + pos, ext);
        return pos;
    }
    static std::optional<CreateView> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = MessageHeader::decode(buf, avail);
        if (!h || h->schemaId != kSchemaId || h->templateId != TEMPLATE_ID) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        if (avail < pos + BODY_FIXED) return std::nullopt;
        CreateView m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        m.persistence = buf[pos++];
        if (m.persistence > 1) return std::nullopt;   // append-only: 0 = persisted, 1 = transient
        if (!getVarStr(buf, avail, pos, m.viewName)) return std::nullopt;
        if (!getVarStr(buf, avail, pos, m.keyField)) return std::nullopt;
        if (!getVarBytes32(buf, avail, pos, m.planBlob, m.planLen)) return std::nullopt;
        if (pos == avail) return m;   // pre-v8 frame: no extension
        if (!getExt(buf, avail, pos, m.ext) || pos != avail) return std::nullopt;
        m.hasExt = true;
        return m;
    }
};

// CreateViewResponse (71): body = viewTopicId:i32, outputFingerprint:u64, errorCode:u16.
struct CreateViewResponse {
    int64_t sessionId = 0, correlationId = 0;
    int32_t viewTopicId = -1; uint64_t outputFingerprint = 0; uint16_t errorCode = 0;
    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::CreateViewResponse);
    static constexpr uint16_t BODY_FIXED  = 14;  // viewTopicId(4) + outputFingerprint(8) + errorCode(2)
    std::size_t encodedSize() const noexcept { return MessageHeader::ENCODED_SIZE + BODY_FIXED; }
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr; hdr.blockLength = BODY_FIXED; hdr.templateId = TEMPLATE_ID;
        hdr.schemaId = kSchemaId; hdr.version = kProtocolVersion;
        hdr.sessionId = sessionId; hdr.correlationId = correlationId; hdr.encode(buf);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        detail::putLE32(buf, static_cast<int>(pos), viewTopicId); pos += 4;
        detail::putLE64(buf, static_cast<int>(pos), static_cast<int64_t>(outputFingerprint)); pos += 8;
        detail::putLE16(buf, static_cast<int>(pos), errorCode); pos += 2;
        return pos;
    }
    static std::optional<CreateViewResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = MessageHeader::decode(buf, avail);
        if (!h || h->schemaId != kSchemaId || h->templateId != TEMPLATE_ID) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        if (avail < pos + BODY_FIXED) return std::nullopt;
        CreateViewResponse m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        m.viewTopicId = detail::getLE32(buf, static_cast<int>(pos)); pos += 4;
        m.outputFingerprint = static_cast<uint64_t>(detail::getLE64(buf, static_cast<int>(pos))); pos += 8;
        m.errorCode = detail::getLE16(buf, static_cast<int>(pos)); pos += 2;
        return m;
    }
};

// ===========================================================================
// Catalog history (client version 8): DropTopic (74), AlterTopicConfig (75),
// DropView (76), AlterView (77) — each answered by DdlResponse (78).  All
// carry a MANDATORY DdlExtension that must decode exactly to the end of the
// frame (anything else => nullopt).  No fixed block: every body starts with a
// var field (blockLength = 0).  encode() returns 0 (explicit failure, never a
// truncated field) when !fits().
// ===========================================================================

namespace ddl_detail {
inline void putDdlHeader(uint8_t* buf, uint16_t templateId, uint16_t blockLength,
                         int64_t sessionId, int64_t correlationId) noexcept {
    MessageHeader hdr; hdr.blockLength = blockLength; hdr.templateId = templateId;
    hdr.schemaId = kSchemaId; hdr.version = kProtocolVersion;
    hdr.sessionId = sessionId; hdr.correlationId = correlationId; hdr.encode(buf);
}
inline std::optional<MessageHeader> getDdlHeader(const uint8_t* buf, std::size_t avail,
                                                 uint16_t templateId) noexcept {
    auto h = MessageHeader::decode(buf, avail);
    if (!h || h->schemaId != kSchemaId || h->templateId != templateId) return std::nullopt;
    return h;
}
} // namespace ddl_detail

// DropTopic (74): body = topicName(varstr), DdlExtension.
struct DropTopic {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view topicName;
    DdlExtension ext{};

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::DropTopic);
    static constexpr uint16_t BODY_FIXED  = 0;

    [[nodiscard]] bool fits() const noexcept { return fitsVarLen16(topicName.size()) && extFits(ext); }
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + 2u + topicName.size() + extEncodedSize(ext);
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        if (!fits()) return 0;
        ddl_detail::putDdlHeader(buf, TEMPLATE_ID, BODY_FIXED, sessionId, correlationId);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        pos += putVarStr(buf + pos, topicName);
        pos += putExt(buf + pos, ext);
        return pos;
    }
    static std::optional<DropTopic> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = ddl_detail::getDdlHeader(buf, avail, TEMPLATE_ID);
        if (!h) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        DropTopic m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        if (!getVarStr(buf, avail, pos, m.topicName)) return std::nullopt;
        if (!getExt(buf, avail, pos, m.ext) || pos != avail) return std::nullopt;
        return m;
    }
};

// AlterTopicConfig (75): body = topicName(varstr), expectedVersion:u32,
//   changes = count:u16 + (key:u8, value:i64)*count, DdlExtension.
// `changes` is the raw count-prefixed block (zero-copy); build it with
// putConfigChanges and read it with configChangeCount / configChangeAt.  Keys
// are the alterable configs of ruling 5 (rune-codec catalog::ConfigKey, values
// 1..6): retention.ms 1, retention.bytes 2, delete.retention.ms 3,
// min.cleanable.dirty.ratio (ppm) 4, min.compaction.lag.ms 5,
// max.compaction.lag.ms 6.  Decode refuses a key outside 1..6; encode refuses a
// block whose size does not match its count (the key is the receiver's check).
struct ConfigChangeWire { uint8_t key; int64_t value; bool operator==(const ConfigChangeWire&) const = default; };

inline constexpr uint8_t     kConfigKeyFirst      = 1;
inline constexpr uint8_t     kConfigKeyLast       = 6;
inline constexpr std::size_t kConfigChangeWireLen = 9;   // u8 key + i64 value

/// Bytes putConfigChanges writes for `n` changes.
[[nodiscard]] constexpr std::size_t configChangesEncodedSize(std::size_t n) noexcept {
    return 2u + n * kConfigChangeWireLen;
}
/// Writes count:u16 + (key, value)*; returns bytes written, or 0 when the
/// count does not fit a u16 (explicit failure, never truncated).
inline std::size_t putConfigChanges(uint8_t* buf, std::span<const ConfigChangeWire> changes) noexcept {
    if (!fitsVarLen16(changes.size())) return 0;
    detail::putLE16(buf, 0, static_cast<uint16_t>(changes.size()));
    std::size_t pos = 2;
    for (const ConfigChangeWire& c : changes) {
        buf[pos++] = c.key;
        detail::putLE64(buf, static_cast<int>(pos), c.value); pos += 8;
    }
    return pos;
}
/// The count of a well-formed changes block (as decode returns it).
[[nodiscard]] inline uint16_t configChangeCount(std::span<const uint8_t> changes) noexcept {
    return changes.size() < 2 ? uint16_t{0} : detail::getLE16(changes.data(), 0);
}
/// Change `i` (< configChangeCount) of a well-formed changes block.
[[nodiscard]] inline ConfigChangeWire configChangeAt(std::span<const uint8_t> changes, std::size_t i) noexcept {
    const std::size_t off = 2u + i * kConfigChangeWireLen;
    return ConfigChangeWire{changes[off], detail::getLE64(changes.data(), static_cast<int>(off + 1))};
}

struct AlterTopicConfig {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view topicName;
    uint32_t expectedVersion = 0;
    std::span<const uint8_t> changes;   // count:u16 + (key:u8, value:i64)*
    DdlExtension ext{};

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::AlterTopicConfig);
    static constexpr uint16_t BODY_FIXED  = 0;

    [[nodiscard]] bool fits() const noexcept {
        if (!fitsVarLen16(topicName.size()) || !extFits(ext)) return false;
        if (changes.size() < 2) return false;
        return changes.size() == configChangesEncodedSize(configChangeCount(changes));
    }
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + 2u + topicName.size() + 4u + changes.size() + extEncodedSize(ext);
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        if (!fits()) return 0;
        ddl_detail::putDdlHeader(buf, TEMPLATE_ID, BODY_FIXED, sessionId, correlationId);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        pos += putVarStr(buf + pos, topicName);
        detail::putLE32(buf, static_cast<int>(pos), static_cast<int32_t>(expectedVersion)); pos += 4;
        std::memcpy(buf + pos, changes.data(), changes.size()); pos += changes.size();
        pos += putExt(buf + pos, ext);
        return pos;
    }
    static std::optional<AlterTopicConfig> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = ddl_detail::getDdlHeader(buf, avail, TEMPLATE_ID);
        if (!h) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        AlterTopicConfig m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        if (!getVarStr(buf, avail, pos, m.topicName)) return std::nullopt;
        if (avail < pos + 4u + 2u) return std::nullopt;
        m.expectedVersion = static_cast<uint32_t>(detail::getLE32(buf, static_cast<int>(pos))); pos += 4;
        const uint16_t count = detail::getLE16(buf, static_cast<int>(pos));
        const std::size_t blockLen = configChangesEncodedSize(count);
        if (avail < pos + blockLen) return std::nullopt;
        for (std::size_t i = 0; i < count; ++i) {
            const uint8_t key = buf[pos + 2u + i * kConfigChangeWireLen];
            if (key < kConfigKeyFirst || key > kConfigKeyLast) return std::nullopt;   // never cast
        }
        m.changes = std::span<const uint8_t>(buf + pos, blockLen); pos += blockLen;
        if (!getExt(buf, avail, pos, m.ext) || pos != avail) return std::nullopt;
        return m;
    }
};

// DropView (76): body = viewName(varstr), DdlExtension.
struct DropView {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view viewName;
    DdlExtension ext{};

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::DropView);
    static constexpr uint16_t BODY_FIXED  = 0;

    [[nodiscard]] bool fits() const noexcept { return fitsVarLen16(viewName.size()) && extFits(ext); }
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + 2u + viewName.size() + extEncodedSize(ext);
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        if (!fits()) return 0;
        ddl_detail::putDdlHeader(buf, TEMPLATE_ID, BODY_FIXED, sessionId, correlationId);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        pos += putVarStr(buf + pos, viewName);
        pos += putExt(buf + pos, ext);
        return pos;
    }
    static std::optional<DropView> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = ddl_detail::getDdlHeader(buf, avail, TEMPLATE_ID);
        if (!h) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        DropView m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        if (!getVarStr(buf, avail, pos, m.viewName)) return std::nullopt;
        if (!getExt(buf, avail, pos, m.ext) || pos != avail) return std::nullopt;
        return m;
    }
};

// AlterView (77): body = viewName(varstr), keyField(varstr), persistence:u8
//   (0 persisted, 1 transient), expectedVersion:u32, planBlob(varbytes32),
//   DdlExtension.  Ruling 4: the view's plan and output are replaced under the
//   same objectId with a fresh sink topic.
struct AlterView {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view viewName;
    std::string_view keyField;
    uint8_t persistence = 0;
    uint32_t expectedVersion = 0;
    const uint8_t* planBlob = nullptr;
    uint32_t planLen = 0;
    DdlExtension ext{};

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::AlterView);
    static constexpr uint16_t BODY_FIXED  = 0;

    [[nodiscard]] bool fits() const noexcept {
        return fitsVarLen16(viewName.size()) && fitsVarLen16(keyField.size()) && persistence <= 1 &&
               extFits(ext);
    }
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + 2u + viewName.size() + 2u + keyField.size() + 1u + 4u
             + 4u + planLen + extEncodedSize(ext);
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        if (!fits()) return 0;
        ddl_detail::putDdlHeader(buf, TEMPLATE_ID, BODY_FIXED, sessionId, correlationId);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        pos += putVarStr(buf + pos, viewName);
        pos += putVarStr(buf + pos, keyField);
        buf[pos++] = persistence;
        detail::putLE32(buf, static_cast<int>(pos), static_cast<int32_t>(expectedVersion)); pos += 4;
        pos += putVarBytes32(buf + pos, planBlob, planLen);
        pos += putExt(buf + pos, ext);
        return pos;
    }
    static std::optional<AlterView> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = ddl_detail::getDdlHeader(buf, avail, TEMPLATE_ID);
        if (!h) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        AlterView m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        if (!getVarStr(buf, avail, pos, m.viewName)) return std::nullopt;
        if (!getVarStr(buf, avail, pos, m.keyField)) return std::nullopt;
        if (avail < pos + 1u + 4u) return std::nullopt;
        m.persistence = buf[pos++];
        if (m.persistence > 1) return std::nullopt;   // append-only: never cast
        m.expectedVersion = static_cast<uint32_t>(detail::getLE32(buf, static_cast<int>(pos))); pos += 4;
        if (!getVarBytes32(buf, avail, pos, m.planBlob, m.planLen)) return std::nullopt;
        if (!getExt(buf, avail, pos, m.ext) || pos != avail) return std::nullopt;
        return m;
    }
};

// DdlResponse (78): body = errorCode:u16, objectId:u64, newVersion:u32,
//   gateTopicId:i32, then detail(varstr).  Answers 74–77 (sessions >= 8 only).
//   objectId / newVersion name the catalog object and its object version after
//   the DDL; gateTopicId is the topic id the answer was gated on (the DDL's
//   resulting topic, e.g. AlterView's fresh sink; -1 = none) — its use is the
//   broker's write path (Task 9).  detail is empty on OK and names the reason
//   otherwise.  The frame decodes exactly to its end.
struct DdlResponse {
    int64_t sessionId = 0, correlationId = 0;
    uint16_t errorCode = 0;
    uint64_t objectId = 0;
    uint32_t newVersion = 0;
    int32_t gateTopicId = -1;
    std::string_view detail;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::DdlResponse);
    static constexpr uint16_t BODY_FIXED  = 18;  // u16 + u64 + u32 + i32

    [[nodiscard]] bool fits() const noexcept { return fitsVarLen16(detail.size()); }
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 2u + detail.size();
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        if (!fits()) return 0;
        ddl_detail::putDdlHeader(buf, TEMPLATE_ID, BODY_FIXED, sessionId, correlationId);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        detail::putLE16(buf, static_cast<int>(pos), errorCode); pos += 2;
        detail::putLE64(buf, static_cast<int>(pos), static_cast<int64_t>(objectId)); pos += 8;
        detail::putLE32(buf, static_cast<int>(pos), static_cast<int32_t>(newVersion)); pos += 4;
        detail::putLE32(buf, static_cast<int>(pos), gateTopicId); pos += 4;
        pos += putVarStr(buf + pos, this->detail);
        return pos;
    }
    static std::optional<DdlResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = ddl_detail::getDdlHeader(buf, avail, TEMPLATE_ID);
        if (!h) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        if (avail < pos + BODY_FIXED) return std::nullopt;
        DdlResponse m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        m.errorCode = detail::getLE16(buf, static_cast<int>(pos)); pos += 2;
        m.objectId = static_cast<uint64_t>(detail::getLE64(buf, static_cast<int>(pos))); pos += 8;
        m.newVersion = static_cast<uint32_t>(detail::getLE32(buf, static_cast<int>(pos))); pos += 4;
        m.gateTopicId = detail::getLE32(buf, static_cast<int>(pos)); pos += 4;
        if (!getVarStr(buf, avail, pos, m.detail) || pos != avail) return std::nullopt;
        return m;
    }
};

} // namespace rune::protocol::client
