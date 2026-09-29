// include/rune/codec/Fingerprint.h
#pragma once
#include <cstdint>
#include <algorithm>
#include <vector>
#include <rune/codec/Descriptor.h>

namespace rune::codec {

namespace detail {

// FNV-1a-64 helpers
constexpr uint64_t kFnvSeed  = 0xcbf29ce484222325ULL;
constexpr uint64_t kFnvPrime = 0x100000001b3ULL;

constexpr uint64_t fnv1a_byte(uint64_t hash, uint8_t byte) noexcept {
    return (hash ^ byte) * kFnvPrime;
}

constexpr uint64_t fnv1a_u16le(uint64_t hash, uint16_t v) noexcept {
    hash = fnv1a_byte(hash, static_cast<uint8_t>(v & 0xff));
    hash = fnv1a_byte(hash, static_cast<uint8_t>((v >> 8) & 0xff));
    return hash;
}

constexpr uint64_t fnv1a_u32le(uint64_t hash, uint32_t v) noexcept {
    hash = fnv1a_byte(hash, static_cast<uint8_t>(v & 0xff));
    hash = fnv1a_byte(hash, static_cast<uint8_t>((v >> 8) & 0xff));
    hash = fnv1a_byte(hash, static_cast<uint8_t>((v >> 16) & 0xff));
    hash = fnv1a_byte(hash, static_cast<uint8_t>((v >> 24) & 0xff));
    return hash;
}

} // namespace detail

/// Compute a deterministic FNV-1a-64 fingerprint of a Descriptor.
///
/// Fields are processed in canonical order (sorted by id), so field declaration
/// order does not affect the result. The fingerprint covers:
///   schema name, then per field: id (LE16), type (uint8_t), sinceVersion (LE16),
///   isKey (uint8_t), name bytes, each enumValue (LE32).
///
/// offset and blockLength are intentionally excluded (they are derived values).
[[nodiscard]] inline uint64_t fingerprint(const Descriptor& d) noexcept {
    using namespace detail;

    // Collect fields sorted by id (canonical order)
    std::vector<const FieldDescriptor*> sorted;
    sorted.reserve(d.fields.size());
    for (const auto& f : d.fields) {
        sorted.push_back(&f);
    }
    std::sort(sorted.begin(), sorted.end(),
              [](const FieldDescriptor* a, const FieldDescriptor* b) noexcept {
                  return a->id < b->id;
              });

    uint64_t hash = kFnvSeed;

    // Hash the schema name
    for (unsigned char c : d.name) {
        hash = fnv1a_byte(hash, c);
    }

    // Hash each field in canonical order
    for (const FieldDescriptor* fp : sorted) {
        hash = fnv1a_u16le(hash, fp->id);
        hash = fnv1a_byte(hash, static_cast<uint8_t>(fp->type));
        hash = fnv1a_u16le(hash, fp->sinceVersion);
        hash = fnv1a_byte(hash, fp->isKey ? uint8_t{1} : uint8_t{0});
        for (unsigned char c : fp->name) {
            hash = fnv1a_byte(hash, c);
        }
        for (int32_t ev : fp->enumValues) {
            hash = fnv1a_u32le(hash, static_cast<uint32_t>(ev));
        }
    }

    return hash;
}

} // namespace rune::codec
