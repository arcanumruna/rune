// include/rune/codec/Descriptor.h
#pragma once
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>
#include <rune/codec/PrimType.h>

namespace rune::codec {

struct FieldDescriptor {
    std::string  name;
    uint16_t     id           = 0;
    PrimType     type         = PrimType::Int32;
    uint32_t     offset       = 0;    // byte offset (fixed fields) or var index (var-len fields)
    uint16_t     sinceVersion = 1;
    bool         isKey        = false;
    std::vector<int32_t> enumValues;  // non-empty => Enum whitelist
};

struct Descriptor {
    std::string  name;
    uint16_t     version     = 1;
    uint32_t     blockLength = 0;
    std::vector<FieldDescriptor> fields;  // in declaration order

    const FieldDescriptor* find(std::string_view n) const noexcept {
        for (const auto& f : fields) {
            if (f.name == n) return &f;
        }
        return nullptr;
    }
};

class DescriptorBuilder {
public:
    explicit DescriptorBuilder(std::string schemaName, uint16_t version = 1)
        : name_(std::move(schemaName)), version_(version) {}

    DescriptorBuilder& add(std::string name, PrimType type,
                           uint16_t sinceVersion = 1, bool isKey = false,
                           std::vector<int32_t> enumValues = {}) {
        FieldDescriptor f;
        f.name         = std::move(name);
        f.id           = static_cast<uint16_t>(fields_.size());
        f.type         = type;
        f.offset       = 0;
        f.sinceVersion = sinceVersion;
        f.isKey        = isKey;
        f.enumValues   = std::move(enumValues);
        fields_.push_back(std::move(f));
        return *this;
    }

    Descriptor build() {
        Descriptor d;
        d.name    = name_;
        d.version = version_;
        d.fields  = fields_;

        // First pass: assign fixed offsets and compute blockLength
        uint32_t offset = 0;
        for (auto& f : d.fields) {
            if (!isVarLen(f.type)) {
                f.offset = offset;
                offset  += fixedSize(f.type);
            }
        }
        d.blockLength = offset;

        // Second pass: assign var-index offsets to var-len fields
        uint32_t varIndex = 0;
        for (auto& f : d.fields) {
            if (isVarLen(f.type)) {
                f.offset = varIndex++;
            }
        }

        // Enforce the additive-only invariant: iterating var fields in
        // var-index order must yield non-decreasing sinceVersion. This is the
        // load-bearing assumption both readField and Validator rely on when
        // walking the var-data section; a violation would mis-parse across
        // versions. deserialize() and the generator both funnel through
        // build(), so this single check covers every consumer.
        uint16_t prevSince = 0;
        for (const auto& f : d.fields) {
            if (isVarLen(f.type)) {
                if (f.sinceVersion < prevSince) {
                    throw std::invalid_argument(
                        "var-field sinceVersion must be non-decreasing in var-index order");
                }
                prevSince = f.sinceVersion;
            }
        }

        return d;
    }

private:
    std::string               name_;
    uint16_t                  version_;
    std::vector<FieldDescriptor> fields_;
};

} // namespace rune::codec
