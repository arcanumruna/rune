#pragma once
// RegressorsPathRecords.h - checked writes Part 09: the record types of the RegressorsPath golden script's `user` and `character` topics
// (ql/tests/golden/regressorspath.rql).  Hand-written record types that follow the generated-struct contract TypedTopic<T> needs (what
// `rune schema generate` would emit), as KiAccount in KeyIndexRecords.h does.  The schema names are the topics' names, so the
// fingerprints match the topics the script defines.  Wire = the codec's fixed block: [blockLength:u16][version:u16], then the fields
// in order, little-endian, strings NUL padded.
//   RpUser      {userId uint64 key, provider uint16, subject string(64), createdAt int64}
//   RpCharacter {characterId uint64 key, userId uint64, name string(16), gender uint16, hairSkin uint16, hairSheet uint16, headSkin uint16,
//                createdAt int64, lastPlayed int64}
//   RpLedger    {entryId uint64 key, account uint64, amount int64, memo string(16)}  (Part 11's ledger topic)
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace rune::node::test {

namespace rpdetail {
inline void putLe(uint8_t* b, std::size_t off, uint64_t v, int bytes) {
    for (int i = 0; i < bytes; ++i) b[off + static_cast<std::size_t>(i)] = static_cast<uint8_t>(v >> (8 * i));
}
inline void putStr(uint8_t* b, std::size_t off, const std::string& s, std::size_t width) {
    std::memset(b + off, 0, width);
    std::memcpy(b + off, s.data(), s.size() < width ? s.size() : width);
}
inline void putHeader(uint8_t* b, uint16_t blockLength, uint16_t version) {
    putLe(b, 0, blockLength, 2);
    putLe(b, 2, version, 2);
}
}  // namespace rpdetail

inline rune::codec::Descriptor rpUserDescriptor() {
    return rune::codec::DescriptorBuilder("user", 1)
        .add("userId", rune::codec::PrimType::UInt64, 1, /*isKey=*/true)
        .add("provider", rune::codec::PrimType::UInt16)
        .addFixedStr("subject", 64)
        .add("createdAt", rune::codec::PrimType::Int64)
        .build();
}

struct RpUser {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "user";
    static constexpr uint16_t kVersion = 1, kBlockLength = 82;
    static constexpr std::size_t kSubjectBytes = 64;

    uint64_t    userId = 0;
    uint16_t    provider = 0;
    std::string subject;   ///< at most 64 bytes; the rest of the field is NUL
    int64_t     createdAt = 0;

    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + kBlockLength; }
    [[nodiscard]] std::size_t encode(uint8_t* b) const noexcept {
        rpdetail::putHeader(b, kBlockLength, kVersion);
        rpdetail::putLe(b, 4, userId, 8);
        rpdetail::putLe(b, 12, provider, 2);
        rpdetail::putStr(b, 14, subject, kSubjectBytes);
        rpdetail::putLe(b, 78, static_cast<uint64_t>(createdAt), 8);
        return encodedSize();
    }
    [[nodiscard]] uint64_t key() const noexcept { return userId; }
};
inline const uint64_t RpUser::kFingerprint = rune::codec::fingerprint(rpUserDescriptor());

inline rune::codec::Descriptor rpCharacterDescriptor() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", rune::codec::PrimType::UInt64, 1, /*isKey=*/true)
        .add("userId", rune::codec::PrimType::UInt64)
        .addFixedStr("name", 16)
        .add("gender", rune::codec::PrimType::UInt16)
        .add("hairSkin", rune::codec::PrimType::UInt16)
        .add("hairSheet", rune::codec::PrimType::UInt16)
        .add("headSkin", rune::codec::PrimType::UInt16)
        .add("createdAt", rune::codec::PrimType::Int64)
        .add("lastPlayed", rune::codec::PrimType::Int64)
        .build();
}

struct RpCharacter {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "character";
    static constexpr uint16_t kVersion = 1, kBlockLength = 56;
    static constexpr std::size_t kNameBytes = 16;

    uint64_t    characterId = 0;
    uint64_t    userId = 0;
    std::string name;   ///< at most 16 bytes of UTF-8; the rest of the field is NUL
    uint16_t    gender = 0, hairSkin = 0, hairSheet = 0, headSkin = 0;
    int64_t     createdAt = 0, lastPlayed = 0;

    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + kBlockLength; }
    [[nodiscard]] std::size_t encode(uint8_t* b) const noexcept {
        rpdetail::putHeader(b, kBlockLength, kVersion);
        rpdetail::putLe(b, 4, characterId, 8);
        rpdetail::putLe(b, 12, userId, 8);
        rpdetail::putStr(b, 20, name, kNameBytes);
        rpdetail::putLe(b, 36, gender, 2);
        rpdetail::putLe(b, 38, hairSkin, 2);
        rpdetail::putLe(b, 40, hairSheet, 2);
        rpdetail::putLe(b, 42, headSkin, 2);
        rpdetail::putLe(b, 44, static_cast<uint64_t>(createdAt), 8);
        rpdetail::putLe(b, 52, static_cast<uint64_t>(lastPlayed), 8);
        return encodedSize();
    }
    [[nodiscard]] uint64_t key() const noexcept { return characterId; }
};
inline const uint64_t RpCharacter::kFingerprint = rune::codec::fingerprint(rpCharacterDescriptor());

inline rune::codec::Descriptor rpLedgerDescriptor() {
    return rune::codec::DescriptorBuilder("ledger", 1)
        .add("entryId", rune::codec::PrimType::UInt64, 1, /*isKey=*/true)
        .add("account", rune::codec::PrimType::UInt64)
        .add("amount", rune::codec::PrimType::Int64)
        .addFixedStr("memo", 16)
        .build();
}

struct RpLedger {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "ledger";
    static constexpr uint16_t kVersion = 1, kBlockLength = 40;
    static constexpr std::size_t kMemoBytes = 16;

    uint64_t    entryId = 0;
    uint64_t    account = 0;
    int64_t     amount = 0;
    std::string memo;   ///< at most 16 bytes; the rest of the field is NUL

    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + kBlockLength; }
    [[nodiscard]] std::size_t encode(uint8_t* b) const noexcept {
        rpdetail::putHeader(b, kBlockLength, kVersion);
        rpdetail::putLe(b, 4, entryId, 8);
        rpdetail::putLe(b, 12, account, 8);
        rpdetail::putLe(b, 20, static_cast<uint64_t>(amount), 8);
        rpdetail::putStr(b, 28, memo, kMemoBytes);
        return encodedSize();
    }
    [[nodiscard]] uint64_t key() const noexcept { return entryId; }
};
inline const uint64_t RpLedger::kFingerprint = rune::codec::fingerprint(rpLedgerDescriptor());

}  // namespace rune::node::test
