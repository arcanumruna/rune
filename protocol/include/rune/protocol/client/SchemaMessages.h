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

// GetTopicSchema (66): body = topicName(varstr).
struct GetTopicSchema {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view topicName;
    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::GetTopicSchema);
    static constexpr uint16_t BODY_FIXED  = 0;
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + 2u + topicName.size();
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr; hdr.blockLength = BODY_FIXED; hdr.templateId = TEMPLATE_ID;
        hdr.schemaId = kSchemaId; hdr.version = kProtocolVersion;
        hdr.sessionId = sessionId; hdr.correlationId = correlationId; hdr.encode(buf);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        pos += putVarStr(buf + pos, topicName);
        return pos;
    }
    static std::optional<GetTopicSchema> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = MessageHeader::decode(buf, avail);
        if (!h || h->schemaId != kSchemaId || h->templateId != TEMPLATE_ID) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        GetTopicSchema m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        if (!getVarStr(buf, avail, pos, m.topicName)) return std::nullopt;
        return m;
    }
};

// TopicSchemaResponse (67): body = topicId:i32, version:u16, errorCode:u16, then descriptorBlob(varbytes32).
struct TopicSchemaResponse {
    int64_t sessionId = 0, correlationId = 0;
    int32_t topicId = -1; uint16_t version = 0; uint16_t errorCode = 0;
    const uint8_t* descriptorBlob = nullptr; uint32_t descriptorLen = 0;
    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::TopicSchemaResponse);
    static constexpr uint16_t BODY_FIXED  = 8;
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 4u + descriptorLen;
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

} // namespace rune::protocol::client
