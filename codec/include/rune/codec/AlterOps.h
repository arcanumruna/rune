// include/rune/codec/AlterOps.h
#pragma once
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>
#include <rune/codec/PrimType.h>

namespace rune::codec {

/// Wire-visible ALTER op kind. Values are fixed and part of the shared
/// rune-ql / rune-broker contract — do not renumber.
enum class AlterOpType : uint8_t { Add = 1, Drop = 2, Rename = 3, SetCompatibility = 4 };

/// Schema compatibility mode set by SET_COMPATIBILITY. Values are fixed —
/// do not renumber.
enum class CompatMode : uint8_t { Backward = 0, Forward = 1 };

/// A single ALTER operation. Only the fields relevant to `type` are
/// meaningful; the rest are ignored by encode/decode for that op.
struct AlterOp {
    AlterOpType type;
    std::string name;      // ADD: field name; DROP: field name; RENAME: old name
    std::string newName;   // RENAME: new name; unused otherwise
    PrimType primType;     // ADD only
    uint16_t fixedLen;     // ADD only (FixedStr width; 0 otherwise)
    CompatMode mode;       // SET_COMPATIBILITY only
};

/// Encode a sequence of ALTER ops to the fixed little-endian wire format:
///
///   opsCount        u16
///   per op:
///     opType        u8     // 1=ADD, 2=DROP, 3=RENAME, 4=SET_COMPATIBILITY
///     ADD:          nameLen u16 + name bytes, primType u8, fixedLen u16
///     DROP:         nameLen u16 + name bytes
///     RENAME:       oldLen  u16 + old bytes, newLen u16 + new bytes
///     SET_COMPAT:   mode    u8    // 0=BACKWARD, 1=FORWARD
///
/// This is the shared wire contract between rune-ql (producer) and
/// rune-broker (consumer) — the byte layout above is load-bearing and must
/// not change without updating both sides.
[[nodiscard]] std::vector<uint8_t> encodeAlterOps(const std::vector<AlterOp>& ops);

/// Decode a blob produced by encodeAlterOps(). Every read is bounds-checked;
/// any truncation (or unrecognized opType/mode byte) yields std::nullopt
/// rather than throwing or reading out of bounds.
[[nodiscard]] std::optional<std::vector<AlterOp>> decodeAlterOps(std::span<const uint8_t> blob);

} // namespace rune::codec
