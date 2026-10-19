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

/// Read a single field from a record by field id.
///
/// PRECONDITION: `record` is assumed to be a pre-validated, well-formed record.
/// The reader trusts the record's declared `blockLength` (header bytes [0..1])
/// to locate the var-data section and does NOT reject a blockLength that
/// disagrees with the descriptor — it is the zero-copy hot path and stays
/// branch-lean. For untrusted input, call `Validator::validate` first; the
/// Validator is the gate that rejects malformed records. All accesses here are
/// still bounds-checked against `record.size()`, so a hostile record can never
/// read out of bounds — it may only yield an absent/empty field.
FieldValue readField(const Descriptor&, std::span<const uint8_t> record, uint16_t fieldId) noexcept;

} // namespace rune::codec
