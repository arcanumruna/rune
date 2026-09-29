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

class Validator {
public:
    explicit Validator(const Descriptor& d);
    ValResult validate(std::span<const uint8_t> record) const noexcept;

private:
    // Precomputed per-field validation entry
    struct FieldEntry {
        uint16_t             id;
        uint16_t             sinceVersion;
        PrimType             type;
        uint32_t             offset;      // byte offset from start of fixed block (fixed fields)
        uint32_t             size;        // byte size (fixed fields)
        uint32_t             varIndex;    // position among var fields (var fields)
        std::vector<int32_t> enumValues;  // whitelist for Enum fields; empty = no check
    };

    uint32_t              blockLength_; // descriptor's blockLength (for the current version)
    uint16_t              version_;     // descriptor's version
    std::vector<FieldEntry> fixedFields_;
    std::vector<FieldEntry> varFields_;
};

} // namespace rune::codec
