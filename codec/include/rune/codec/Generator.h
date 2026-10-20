// include/rune/codec/Generator.h
#pragma once
#include <string>
#include <string_view>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>

namespace rune::codec {

/// Generate a C++23 header string for the given Descriptor.
///
/// The generated header contains:
///   - A struct named after the schema (PascalCase) in the given namespace.
///   - Typed members for each field (fixed members first, then var members
///     as std::string_view).
///   - encode(uint8_t* buf) -> size_t  : writes the record framing + data.
///   - decode(std::span<const uint8_t>) : reads fields from a record buffer.
///   - encodedSize() const -> size_t   : returns the total byte count.
///   - static constexpr uint64_t kFingerprint  (= fingerprint(descriptor)).
///   - static constexpr uint16_t kBlockLength.
///   - static constexpr std::string_view kSchemaName.
///   - static const Descriptor& descriptor()   (embeds the serialized blob).
///   - key() const (Phase 2b Part C) — ONLY when a field isKey: the key
///     field's value in the form rune::client::Key consumes (scalar by value,
///     string(N) as const std::array<char,N>&, string/bytes as a
///     std::span<const uint8_t> over the content).  TypedTopic::send routes by
///     Key(rec.key()); a schema with no key field gets no key() and its typed
///     records go to partition 0 with an empty record key.
///   - Phase 2b Part B — for a schema whose fields are ALL fixed-size (no
///     string / bytes): `<Struct>Encoder` (zero-copy writer over a claimed
///     std::span<uint8_t>: chainable setters, kBlockLength, kEncodedLength,
///     key helpers) and `<Struct>Decoder` (zero-copy reader), aliased as
///     `<Struct>::Encoder` / `<Struct>::Decoder`.
///
/// @throws std::invalid_argument if the descriptor has a key field AND a field
///         named "key" (the member would collide with the key() accessor).
///
/// @param d       The descriptor to generate code for.
/// @param nspace  The C++ namespace to emit the struct in (default "rune::gen").
[[nodiscard]] std::string generateHeader(const Descriptor& d,
                                         std::string_view nspace = "rune::gen");

} // namespace rune::codec
