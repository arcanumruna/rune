// include/rune/codec/Serialize.h
#pragma once
#include <cstdint>
#include <span>
#include <stdexcept>
#include <vector>
#include <rune/codec/Descriptor.h>

namespace rune::codec {

/// Serialize a Descriptor to a compact little-endian binary blob.
///
/// Blob format:
///   [magic u32 "RCD1" = 0x31444352]
///   [nameLen u16][name bytes]
///   [version u16]
///   [fieldCount u16]
///   per field:
///     [nameLen u16][name bytes]
///     [id u16]
///     [type u8]
///     [sinceVersion u16]
///     [isKey u8]
///     [enumCount u16][enum values as i32 LE ...]
[[nodiscard]] std::vector<uint8_t> serialize(const Descriptor& d);

/// Deserialize a Descriptor from a binary blob produced by serialize().
///
/// Bounds-checks every read. Reconstructs the Descriptor via DescriptorBuilder
/// so offsets and blockLength are recomputed from scratch (not trusted from
/// the blob). Throws std::runtime_error on any malformed or truncated input.
[[nodiscard]] Descriptor deserialize(std::span<const uint8_t> blob);

} // namespace rune::codec
