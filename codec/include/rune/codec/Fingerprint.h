// include/rune/codec/Fingerprint.h
#pragma once
#include <cstdint>
#include <algorithm>
#include <vector>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>

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
///   isKey (uint8_t), name length (LE16) + name bytes, enum count (LE16) +
///   each enumValue (LE32).
///
/// The name length and enum count are hashed as delimiters so the name/enum
/// byte boundary cannot be slid between structurally distinct descriptors.
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
        hash = fnv1a_u16le(hash, static_cast<uint16_t>(fp->name.size()));
        for (unsigned char c : fp->name) {
            hash = fnv1a_byte(hash, c);
        }
        hash = fnv1a_u16le(hash, static_cast<uint16_t>(fp->enumValues.size()));
        for (int32_t ev : fp->enumValues) {
            hash = fnv1a_u32le(hash, static_cast<uint32_t>(ev));
        }
        // FixedStr-only: absorb fixedLen so existing fingerprints are unchanged
        if (fp->type == PrimType::FixedStr) {
            hash = fnv1a_u16le(hash, fp->fixedLen);
        }
        // Guarded, appended last: a field live-forever (untilVersion == 0xFFFF,
        // the only kind in any pre-evolution schema) contributes nothing new,
        // so additive schemas hash identically to before this change. Only a
        // field that has actually been retired at some version absorbs that
        // version into the fingerprint, so dropping a field changes the hash.
        if (fp->untilVersion != 0xFFFF) {
            hash = fnv1a_u16le(hash, fp->untilVersion);
        }
    }

    return hash;
}

/// Version-aware fingerprint: hashes the Descriptor as it existed at
/// schema `version` (per materializeAsOf), so two versions of the same
/// evolving schema that differ only in which fields are live produce
/// different fingerprints even though the underlying Descriptor is one
/// evolving object.
[[nodiscard]] inline uint64_t fingerprintAsOf(const Descriptor& evolving, uint16_t version) noexcept {
    return fingerprint(materializeAsOf(evolving, version));
}

} // namespace rune::codec
