#pragma once
// KeyIndexRecords.h - checked writes Part 05: the record type of the key.index tests, {id uint64 key, name string(16), n int64}.
// A hand-written record type that follows the generated-struct contract TypedTopic<T> needs (what `rune schema generate` would
// emit), as WorldEvent in ClientAuthClusterRegressionTest.cpp does.  Wire = the codec's fixed block: [blockLength:u16][version:u16],
// then id (u64), name (16 bytes, NUL padded) and n (i64), little-endian.
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace rune::node::test {

inline rune::codec::Descriptor kiAccountDescriptor() {
    return rune::codec::DescriptorBuilder("ki_account", 1)
        .add("id", rune::codec::PrimType::UInt64, 1, /*isKey=*/true)
        .addFixedStr("name", 16)
        .add("n", rune::codec::PrimType::Int64)
        .build();
}

struct KiAccount {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "ki_account";
    static constexpr uint16_t kVersion = 1, kBlockLength = 32;
    static constexpr std::size_t kNameBytes = 16;

    uint64_t    id = 0;
    std::string name;   ///< at most 16 bytes of UTF-8; the rest of the field is NUL
    int64_t     n = 0;

    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + kBlockLength; }
    [[nodiscard]] std::size_t encode(uint8_t* b) const noexcept {
        const auto put = [&](std::size_t off, uint64_t v) { for (int i = 0; i < 8; ++i) b[off + static_cast<std::size_t>(i)] = static_cast<uint8_t>(v >> (8 * i)); };
        b[0] = kBlockLength & 0xff; b[1] = kBlockLength >> 8; b[2] = kVersion & 0xff; b[3] = kVersion >> 8;
        put(4, id);
        std::memset(b + 12, 0, kNameBytes);
        std::memcpy(b + 12, name.data(), name.size() < kNameBytes ? name.size() : kNameBytes);
        put(28, static_cast<uint64_t>(n));
        return encodedSize();
    }
    [[nodiscard]] uint64_t key() const noexcept { return id; }
};
inline const uint64_t KiAccount::kFingerprint = rune::codec::fingerprint(kiAccountDescriptor());

}  // namespace rune::node::test
