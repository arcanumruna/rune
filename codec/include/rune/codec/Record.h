// include/rune/codec/Record.h
#pragma once
#include <cstdint>
#include <span>
#include <string_view>
#include <rune/codec/Descriptor.h>

namespace rune::codec {

struct FieldValue {
    PrimType         type    = PrimType::Int32;
    bool             present = false;
    int64_t          i       = 0;
    uint64_t         u       = 0;
    double           f       = 0;
    bool             b       = false;
    std::string_view bytes;
};

FieldValue readField(const Descriptor&, std::span<const uint8_t> record, uint16_t fieldId) noexcept;

} // namespace rune::codec
