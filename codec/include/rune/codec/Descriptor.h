// include/rune/codec/Descriptor.h
#pragma once
#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>
#include <rune/codec/PrimType.h>

namespace rune::codec {

struct FieldDescriptor {
    std::string  name;
    uint16_t     id           = 0;    // durable tag: stable identity across schema versions
    PrimType     type         = PrimType::Int32;
    uint32_t     offset       = 0;    // byte offset (fixed fields) or var index (var-len fields)
    uint16_t     sinceVersion = 1;
    uint16_t     untilVersion = 0xFFFF; // exclusive: field is live for v in [sinceVersion, untilVersion)
    bool         isKey        = false;
    uint16_t     fixedLen     = 0;    // non-zero for FixedStr: per-field byte width in the block
    std::vector<int32_t> enumValues;  // non-empty => Enum whitelist
};

// Byte width of a field in the fixed block (0 for var-len). FixedStr uses its per-field length.
inline uint32_t fieldWidth(const FieldDescriptor& f) noexcept {
    return f.type == PrimType::FixedStr ? f.fixedLen : fixedSize(f.type);
}

// True when field f is part of the schema at version v: [sinceVersion, untilVersion).
inline bool isLiveAt(const FieldDescriptor& f, uint16_t v) noexcept {
    return v >= f.sinceVersion && v < f.untilVersion;
}

// Lay out `fields` in place: fixed-width fields get a byte offset into the
// fixed block (in field order), var-len fields get a var-data index (in
// field order); returns the resulting blockLength (sum of fixed widths).
// Shared by DescriptorBuilder::build() and materializeAsOf() so both compute
// offsets identically for whatever live set of fields they're given.
inline uint32_t computeOffsets(std::vector<FieldDescriptor>& fields) noexcept {
    uint32_t offset = 0;
    for (auto& f : fields) {
        if (!isVarLen(f.type)) {   // FixedStr is not var-len → laid in the block
            f.offset = offset;
            offset  += fieldWidth(f);
        }
    }
    uint32_t blockLength = offset;

    uint32_t varIndex = 0;
    for (auto& f : fields) {
        if (isVarLen(f.type)) {
            f.offset = varIndex++;
        }
    }
    return blockLength;
}

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
        return addTagged(std::move(name), static_cast<uint16_t>(fields_.size()), type,
                          sinceVersion, 0xFFFF, isKey, std::move(enumValues));
    }

    DescriptorBuilder& addFixedStr(std::string name, uint16_t len,
                                   uint16_t sinceVersion = 1, bool isKey = false) {
        return addFixedStrTagged(std::move(name), static_cast<uint16_t>(fields_.size()), len,
                                  sinceVersion, 0xFFFF, isKey);
    }

    // Explicit-tag variants: the caller owns the durable `tag` (FieldDescriptor::id),
    // which must remain stable across schema versions even as fields are added/retired.
    DescriptorBuilder& addTagged(std::string name, uint16_t tag, PrimType type,
                                 uint16_t sinceVersion, uint16_t untilVersion, bool isKey,
                                 std::vector<int32_t> enumValues) {
        FieldDescriptor f;
        f.name         = std::move(name);
        f.id           = tag;
        f.type         = type;
        f.offset       = 0;
        f.sinceVersion = sinceVersion;
        f.untilVersion = untilVersion;
        f.isKey        = isKey;
        f.enumValues   = std::move(enumValues);
        fields_.push_back(std::move(f));
        return *this;
    }

    DescriptorBuilder& addFixedStrTagged(std::string name, uint16_t tag, uint16_t len,
                                         uint16_t sinceVersion, uint16_t untilVersion,
                                         bool isKey) {
        // A zero-width FixedStr has no storage: the column could never hold a value and every row layout built
        // from it would be degenerate.  Refused here, at definition time (#667).
        if (len == 0) {
            throw std::invalid_argument("FixedStr column '" + name + "' needs a width of at least 1");
        }
        FieldDescriptor f;
        f.name         = std::move(name);
        f.id           = tag;
        f.type         = PrimType::FixedStr;
        f.fixedLen     = len;
        f.offset       = 0;
        f.sinceVersion = sinceVersion;
        f.untilVersion = untilVersion;
        f.isKey        = isKey;
        fields_.push_back(std::move(f));
        return *this;
    }

    Descriptor build() {
        Descriptor d;
        d.name    = name_;
        d.version = version_;
        d.fields  = fields_;

        d.blockLength = computeOffsets(d.fields);

        // Enforce durable-tag uniqueness: every field's id/tag must be
        // distinct across the whole descriptor, live or retired. A RETIRED
        // field (untilVersion set) may legitimately share its NAME with a
        // LIVE field as long as the tag differs — that's the normal
        // rename/replace evolution pattern and stays allowed. Tags
        // colliding is a different, unsafe thing: fingerprint()'s
        // sort-by-id becomes order-ambiguous and readField()'s
        // first-match-by-id lookup becomes ambiguous. This also catches
        // the add()/addTagged() mixing footgun, where add()'s auto-tag
        // (fields_.size() at call time) can collide with an explicit tag
        // passed to addTagged().
        {
            std::vector<uint16_t> tags;
            tags.reserve(d.fields.size());
            for (const auto& f : d.fields) {
                tags.push_back(f.id);
            }
            std::sort(tags.begin(), tags.end());
            for (size_t i = 1; i < tags.size(); ++i) {
                if (tags[i] == tags[i - 1]) {
                    throw std::invalid_argument(
                        "DescriptorBuilder::build: duplicate field tag");
                }
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
