// include/rune/codec/Validator.h
#pragma once
#include <cstdint>
#include <span>
#include <vector>
#include <rune/codec/Descriptor.h>

namespace rune::codec {

struct ValResult {
    bool        ok      = true;
    uint16_t    fieldId = 0;
    const char* reason  = nullptr;
};

/// Utf8: every String / FixedStr field must hold well-formed UTF-8 and a FixedStr's bytes after its first NUL must be NUL.
enum class StringCheck : uint8_t { None, Utf8 };

class Validator {
public:
    /// `d` may be a per-version (materialized) descriptor or the cumulative "evolving" one that still
    /// carries retired fields (untilVersion != 0xFFFF, issue #149): the latter is validated as
    /// materializeAsOf(d, d.version), so retired fields never shift the fixed offsets or the var walk.
    explicit Validator(const Descriptor& d);
    ValResult validate(std::span<const uint8_t> record) const noexcept { return validate(record, StringCheck::None); }
    ValResult validate(std::span<const uint8_t> record, StringCheck check) const noexcept;

private:
    void build_(const Descriptor& d);   // fills the tables from a descriptor with no retired field

    // Precomputed per-field validation entry
    struct FieldEntry {
        uint16_t             id;
        uint16_t             sinceVersion;
        PrimType             type;
        uint32_t             offset;      // byte offset from start of fixed block (fixed fields)
        uint32_t             size;        // byte size (fixed fields)
        std::vector<int32_t> enumValues;  // whitelist for Enum fields; empty = no check
    };

    uint32_t              blockLength_ = 0; // descriptor's blockLength (for the current version)
    uint16_t              version_     = 0; // descriptor's version
    std::vector<FieldEntry> fixedFields_;
    std::vector<FieldEntry> varFields_;
};

} // namespace rune::codec
