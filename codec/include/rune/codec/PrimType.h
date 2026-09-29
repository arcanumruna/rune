// include/rune/codec/PrimType.h
#pragma once
#include <cstdint>
namespace rune::codec {
enum class PrimType : uint8_t { Int32, Int64, UInt16, UInt32, UInt64, Float64, Bool, Enum, String, Bytes, FixedStr };
constexpr bool isVarLen(PrimType t) { return t == PrimType::String || t == PrimType::Bytes; }  // FixedStr is a BLOCK field
constexpr uint32_t fixedSize(PrimType t) {
  switch (t) {
    case PrimType::Int32: case PrimType::UInt32: case PrimType::Enum: return 4;
    case PrimType::Int64: case PrimType::UInt64: case PrimType::Float64: return 8;
    case PrimType::UInt16: return 2;
    case PrimType::Bool: return 1;
    case PrimType::String: case PrimType::Bytes: return 0;
    case PrimType::FixedStr: return 0;   // width is per-field (FieldDescriptor.fixedLen)
  }
  return 0;
}
} // namespace rune::codec
