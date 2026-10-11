#pragma once
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include "Codec.h"
#include "MessageHeader.h"
#include "Protocol.h"

namespace rune::protocol::client {

// DefineTopic (64): body = partitionCount:u32, validate:u8, then var fields
//   topicName(varstr), keyField(varstr), descriptorBlob(varbytes32).
struct DefineTopic {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view topicName;
    uint32_t partitionCount = 1;
    std::string_view keyField;
    bool validate = false;
    const uint8_t* descriptorBlob = nullptr;
    uint32_t descriptorLen = 0;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::DefineTopic);
    static constexpr uint16_t BODY_FIXED  = 5;  // u32 partitionCount + u8 validate

    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED
             + 2u + topicName.size()
             + 2u + keyField.size()
             + 4u + descriptorLen;
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr; hdr.blockLength = BODY_FIXED; hdr.templateId = TEMPLATE_ID;
        hdr.schemaId = kSchemaId; hdr.version = kProtocolVersion;
        hdr.sessionId = sessionId; hdr.correlationId = correlationId; hdr.encode(buf);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        detail::putLE32(buf, static_cast<int>(pos), static_cast<int32_t>(partitionCount)); pos += 4;
        buf[pos++] = validate ? 1 : 0;
        pos += putVarStr(buf + pos, topicName);
        pos += putVarStr(buf + pos, keyField);
        pos += putVarBytes32(buf + pos, descriptorBlob, descriptorLen);
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
    std::size_t encode(uint8_t* buf) const noexcept {
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
//   topicName(varstr), opsBlob(varbytes32).
struct AlterTopic {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view topicName;
    uint16_t expectedVersion = 0;
    const uint8_t* opsBlob = nullptr;
    uint32_t opsLen = 0;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::AlterTopic);
    static constexpr uint16_t BODY_FIXED  = 2;  // u16 expectedVersion

    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED
             + 2u + topicName.size()
             + 4u + opsLen;
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr; hdr.blockLength = BODY_FIXED; hdr.templateId = TEMPLATE_ID;
        hdr.schemaId = kSchemaId; hdr.version = kProtocolVersion;
        hdr.sessionId = sessionId; hdr.correlationId = correlationId; hdr.encode(buf);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        detail::putLE16(buf, static_cast<int>(pos), expectedVersion); pos += 2;
        pos += putVarStr(buf + pos, topicName);
        pos += putVarBytes32(buf + pos, opsBlob, opsLen);
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
struct CreateView {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view viewName;
    std::string_view keyField;
    uint8_t persistence = 0;
    const uint8_t* planBlob = nullptr;
    uint32_t planLen = 0;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::CreateView);
    static constexpr uint16_t BODY_FIXED  = 1;  // u8 persistence

    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED
             + 2u + viewName.size()
             + 2u + keyField.size()
             + 4u + planLen;
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr; hdr.blockLength = BODY_FIXED; hdr.templateId = TEMPLATE_ID;
        hdr.schemaId = kSchemaId; hdr.version = kProtocolVersion;
        hdr.sessionId = sessionId; hdr.correlationId = correlationId; hdr.encode(buf);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        buf[pos++] = persistence;
        pos += putVarStr(buf + pos, viewName);
        pos += putVarStr(buf + pos, keyField);
        pos += putVarBytes32(buf + pos, planBlob, planLen);
        return pos;
    }
    static std::optional<CreateView> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = MessageHeader::decode(buf, avail);
        if (!h || h->schemaId != kSchemaId || h->templateId != TEMPLATE_ID) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        if (avail < pos + BODY_FIXED) return std::nullopt;
        CreateView m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        m.persistence = buf[pos++];
        if (!getVarStr(buf, avail, pos, m.viewName)) return std::nullopt;
        if (!getVarStr(buf, avail, pos, m.keyField)) return std::nullopt;
        if (!getVarBytes32(buf, avail, pos, m.planBlob, m.planLen)) return std::nullopt;
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

} // namespace rune::protocol::client
