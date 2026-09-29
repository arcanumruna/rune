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
///
/// @param d       The descriptor to generate code for.
/// @param nspace  The C++ namespace to emit the struct in (default "rune::gen").
[[nodiscard]] std::string generateHeader(const Descriptor& d,
                                         std::string_view nspace = "rune::gen");

} // namespace rune::codec
