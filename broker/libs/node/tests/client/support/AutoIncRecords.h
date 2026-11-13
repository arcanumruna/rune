#pragma once
// AutoIncRecords.h - checked writes Part 06: the record types of the autoincrement tests.  Hand-written record types that follow the
// generated-struct contract TypedTopic<T> needs (what `rune schema generate` would emit), as KiAccount in KeyIndexRecords.h does.
// Wire = the codec's fixed block: [blockLength:u16][version:u16], then the fields in order, little-endian, strings NUL padded.
//   AiUser      {userId uint64 key, provider uint16, subject string(64), createdAt int64}
//   AiCharacter {characterId uint64 key, userId uint64, name string(16), createdAt int64}
//   Ai          {id uint64 key, n int64}
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>

namespace rune::node::test {

namespace aidetail {
inline void putLe(uint8_t* b, std::size_t off, uint64_t v, int bytes) {
    for (int i = 0; i < bytes; ++i) b[off + static_cast<std::size_t>(i)] = static_cast<uint8_t>(v >> (8 * i));
}
inline uint64_t getLe(const uint8_t* b, std::size_t off, int bytes) {
    uint64_t v = 0;
    for (int i = 0; i < bytes; ++i) v |= static_cast<uint64_t>(b[off + static_cast<std::size_t>(i)]) << (8 * i);
    return v;
}
inline void putStr(uint8_t* b, std::size_t off, const std::string& s, std::size_t width) {
    std::memset(b + off, 0, width);
    std::memcpy(b + off, s.data(), s.size() < width ? s.size() : width);
}
inline void putHeader(uint8_t* b, uint16_t blockLength, uint16_t version) {
    putLe(b, 0, blockLength, 2);
    putLe(b, 2, version, 2);
}
}  // namespace aidetail

inline rune::codec::Descriptor aiUserDescriptor() {
    return rune::codec::DescriptorBuilder("ai_user", 1)
        .add("userId", rune::codec::PrimType::UInt64, 1, /*isKey=*/true)
        .add("provider", rune::codec::PrimType::UInt16)
        .addFixedStr("subject", 64)
        .add("createdAt", rune::codec::PrimType::Int64)
        .build();
}

struct AiUser {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "ai_user";
    static constexpr uint16_t kVersion = 1, kBlockLength = 82;
    static constexpr std::size_t kSubjectBytes = 64;

    uint64_t    userId = 0;
    uint16_t    provider = 0;
    std::string subject;   ///< at most 64 bytes; the rest of the field is NUL
    int64_t     createdAt = 0;

    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + kBlockLength; }
    [[nodiscard]] std::size_t encode(uint8_t* b) const noexcept {
        aidetail::putHeader(b, kBlockLength, kVersion);
        aidetail::putLe(b, 4, userId, 8);
        aidetail::putLe(b, 12, provider, 2);
        aidetail::putStr(b, 14, subject, kSubjectBytes);
        aidetail::putLe(b, 78, static_cast<uint64_t>(createdAt), 8);
        return encodedSize();
    }
    [[nodiscard]] uint64_t key() const noexcept { return userId; }
};
inline const uint64_t AiUser::kFingerprint = rune::codec::fingerprint(aiUserDescriptor());

inline rune::codec::Descriptor aiCharacterDescriptor() {
    return rune::codec::DescriptorBuilder("ai_character", 1)
        .add("characterId", rune::codec::PrimType::UInt64, 1, /*isKey=*/true)
        .add("userId", rune::codec::PrimType::UInt64)
        .addFixedStr("name", 16)
        .add("createdAt", rune::codec::PrimType::Int64)
        .build();
}

struct AiCharacter {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "ai_character";
    static constexpr uint16_t kVersion = 1, kBlockLength = 40;
    static constexpr std::size_t kNameBytes = 16;

    uint64_t    characterId = 0;
    uint64_t    userId = 0;
    std::string name;   ///< at most 16 bytes; the rest of the field is NUL
    int64_t     createdAt = 0;

    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + kBlockLength; }
    [[nodiscard]] std::size_t encode(uint8_t* b) const noexcept {
        aidetail::putHeader(b, kBlockLength, kVersion);
        aidetail::putLe(b, 4, characterId, 8);
        aidetail::putLe(b, 12, userId, 8);
        aidetail::putStr(b, 20, name, kNameBytes);
        aidetail::putLe(b, 36, static_cast<uint64_t>(createdAt), 8);
        return encodedSize();
    }
    [[nodiscard]] uint64_t key() const noexcept { return characterId; }
};
inline const uint64_t AiCharacter::kFingerprint = rune::codec::fingerprint(aiCharacterDescriptor());

inline rune::codec::Descriptor aiDescriptor() {
    return rune::codec::DescriptorBuilder("ai", 1)
        .add("id", rune::codec::PrimType::UInt64, 1, /*isKey=*/true)
        .add("n", rune::codec::PrimType::Int64)
        .build();
}

struct Ai {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "ai";
    static constexpr uint16_t kVersion = 1, kBlockLength = 16;

    uint64_t id = 0;
    int64_t  n = 0;

    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + kBlockLength; }
    [[nodiscard]] std::size_t encode(uint8_t* b) const noexcept {
        aidetail::putHeader(b, kBlockLength, kVersion);
        aidetail::putLe(b, 4, id, 8);
        aidetail::putLe(b, 12, static_cast<uint64_t>(n), 8);
        return encodedSize();
    }
    [[nodiscard]] uint64_t key() const noexcept { return id; }
    /// The record a stored value holds; nullopt when the value is not an Ai block of this version.
    [[nodiscard]] static std::optional<Ai> decode(std::string_view v) noexcept {
        if (v.size() != 4 + kBlockLength) return std::nullopt;
        const auto* b = reinterpret_cast<const uint8_t*>(v.data());
        if (aidetail::getLe(b, 0, 2) != kBlockLength || aidetail::getLe(b, 2, 2) != kVersion) return std::nullopt;
        return Ai{.id = aidetail::getLe(b, 4, 8), .n = static_cast<int64_t>(aidetail::getLe(b, 12, 8))};
    }
};
inline const uint64_t Ai::kFingerprint = rune::codec::fingerprint(aiDescriptor());

}  // namespace rune::node::test
