#pragma once
// CheckedWritesBatches.h — checked writes Part 02: typed-record and batch builders for the validation tests.
//
// The matrix descriptor is {id uint64, name string(16), bio string, blob bytes, kind enum{1,2,3}} (tags 0..4):
// fixed block = id(8) name(16) kind(4) = 28 bytes, then the var fields bio and blob (u32 length + bytes each).
#include <rune/codec/Descriptor.h>
#include <rune/node/WriteState.h>
#include <rune/protocol/RecordBatch.h>

#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace rune::node::cw {

enum class Entity : uint8_t { TypedLegacy, TypedPreUpgrade, TypedKeyless, TypedUncheckableKey };

inline rune::codec::Descriptor matrixDescriptor(uint16_t version = 1) {
    using rune::codec::PrimType;
    return rune::codec::DescriptorBuilder("m", version)
        .add("id", PrimType::UInt64)
        .addFixedStr("name", 16)
        .add("bio", PrimType::String)
        .add("blob", PrimType::Bytes)
        .add("kind", PrimType::Enum, 1, false, {1, 2, 3})
        .build();
}
inline constexpr uint16_t kIdTag = 0, kBioTag = 2;

inline void specFor(Entity e, TopicWriteSpec& spec) {
    TopicWriteSpecWire w;
    w.topicId        = 1;
    w.legacyValidate = e != Entity::TypedPreUpgrade;
    switch (e) {
    case Entity::TypedLegacy:
    case Entity::TypedPreUpgrade:        w.keyFieldId = kIdTag; w.keyType = rune::codec::PrimType::UInt64; w.keyWidth = 8; break;
    case Entity::TypedKeyless:           break;
    case Entity::TypedUncheckableKey:    w.keyFieldId = kBioTag; w.keyType = rune::codec::PrimType::String; w.keyWidth = 0; break;
    }
    spec.installVersion(1, matrixDescriptor());
    spec.setFacts(w);
}

struct TypedRecord {
    std::string key;     // wire key bytes
    std::string value;   // the encoded record (empty for a tombstone)
};

/// Everything a matrix cell mutates about one record.
struct Parts {
    uint64_t                   id = 7;
    std::string                name = "alice";   // zero-padded to 16 bytes; a 16-byte string is used verbatim
    std::string                bio = "hello";
    std::string                blob = std::string("\x01\x02", 2);
    int32_t                    kind = 1;
    uint16_t                   version = 1;
    std::optional<uint16_t>    blockLength;      // default 28
    std::optional<std::string> key;              // wire key; default = the 8 little-endian bytes of id
    bool                       tombstone = false;
    std::size_t                truncateBy = 0;   // bytes cut off the end of the value
};

inline std::string leBytes(uint64_t v, std::size_t n) {
    std::string s(n, '\0');
    for (std::size_t i = 0; i < n && i < 8; ++i) s[i] = static_cast<char>((v >> (8 * i)) & 0xFF);
    return s;
}

inline TypedRecord encode(const Parts& p) {
    TypedRecord r;
    r.key = p.key ? *p.key : leBytes(p.id, 8);
    if (p.tombstone) return r;
    std::string& v = r.value;
    v += leBytes(p.blockLength.value_or(28), 2);
    v += leBytes(p.version, 2);
    v += leBytes(p.id, 8);
    std::string name = p.name;
    name.resize(16, '\0');
    v += name;
    v += leBytes(static_cast<uint32_t>(p.kind), 4);
    v += leBytes(p.bio.size(), 4); v += p.bio;
    v += leBytes(p.blob.size(), 4); v += p.blob;
    v.resize(v.size() - p.truncateBy);
    return r;
}

inline TypedRecord validRecord(uint64_t id = 7, uint16_t version = 1) { Parts p; p.id = id; p.version = version; return encode(p); }

inline std::vector<uint8_t> batchOf(const std::vector<TypedRecord>& records) {
    rune::protocol::RecordBatchBuilder b(0, 1'000'000);
    for (const auto& r : records) b.append(r.key, r.value, 1'000'000);
    return b.build();
}

/// Overwrites record `recordIndex`'s key-length varint with a one-byte value that runs past the record (the CRC goes
/// stale, which the bounded walk never reads).
inline std::vector<uint8_t> withLyingKeyLength(std::vector<uint8_t> batch, std::size_t recordIndex) {
    std::size_t i = 0;
    (void)rune::protocol::forEachRecordSpan(batch.data(), batch.size(), [&](const rune::protocol::RecordSpan& s) {
        if (i++ != recordIndex) return;
        // body: [len varint][attributes 1][tsDelta varint][offsetDelta varint][keyLen varint]...; all one byte here
        std::size_t pos = s.pos;
        while (batch[pos] & 0x80) ++pos;
        ++pos;      // end of the length varint
        ++pos;      // attributes
        while (batch[pos] & 0x80) ++pos;
        ++pos;      // tsDelta
        while (batch[pos] & 0x80) ++pos;
        ++pos;      // offsetDelta
        batch[pos] = 0x7E;   // zigzag 63: longer than the rest of any matrix record
    });
    return batch;
}

}  // namespace rune::node::cw
