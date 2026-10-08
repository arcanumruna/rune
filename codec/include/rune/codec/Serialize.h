// include/rune/codec/Serialize.h
#pragma once
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>
#include <rune/codec/Descriptor.h>

namespace rune::codec {

/// Serialize a Descriptor to a compact little-endian binary blob.
///
/// serialize() always emits the current RCD2 format:
///
///   [magic u32 "RCD2" = 0x32444352]
///   [nameLen u16][name bytes]
///   [version u16]
///   [fieldCount u16]
///   per field:
///     [nameLen u16][name bytes]
///     [tag u16]              -- explicit durable tag (FieldDescriptor::id);
///                                stable across schema versions, NOT required
///                                to equal the field's positional index
///     [type u8]
///     [sinceVersion u16]
///     [isKey u8]
///     [enumCount u16][enum values as i32 LE ...]
///     [fixedLen u16]          -- FixedStr fields only
///     [untilVersion u16]      -- exclusive; field is live for
///                                [sinceVersion, untilVersion)
///
/// Legacy RCD1 blobs (magic "RCD1" = 0x31444352) remain readable by
/// deserialize() for back-compat but are never produced by serialize():
///
///   [magic u32 "RCD1" = 0x31444352]
///   [nameLen u16][name bytes]
///   [version u16]
///   [fieldCount u16]
///   per field:
///     [nameLen u16][name bytes]
///     [id u16]                -- positional; must equal the field's index
///     [type u8]
///     [sinceVersion u16]
///     [isKey u8]
///     [enumCount u16][enum values as i32 LE ...]
///     [fixedLen u16]          -- FixedStr fields only
[[nodiscard]] std::vector<uint8_t> serialize(const Descriptor& d);

/// Deserialize a Descriptor from a binary blob produced by serialize().
///
/// Reads either magic:
///   - RCD2: `tag` is read explicitly and may be non-sequential; `untilVersion`
///     is read as stored.
///   - RCD1 (legacy): `tag` is positional and MUST equal the field's index
///     (deserialize throws otherwise, as before); `untilVersion` defaults to
///     0xFFFF (live forever), since RCD1 has no such field.
///
/// Bounds-checks every read. Reconstructs the Descriptor via DescriptorBuilder
/// so offsets and blockLength are recomputed from scratch (not trusted from
/// the blob). Throws std::runtime_error on any malformed or truncated input.
[[nodiscard]] Descriptor deserialize(std::span<const uint8_t> blob);

/// Render a Descriptor as a human-readable text representation.
///
/// Format (spec §3.1):
///   schema <name> fp=0x<hex16> block=<N> version=<V> {
///     <name padded to 10> <type padded to 8> @<offset>    [key] [sinceVersion=N] [enum(v,...)]
///     <name padded to 10> <type padded to 8> vardata#<i>  [key] [sinceVersion=N]
///   }
[[nodiscard]] std::string toText(const Descriptor& d);

} // namespace rune::codec
