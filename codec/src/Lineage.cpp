// src/Lineage.cpp — #418: per-version field placement (see Lineage.h).
#include <rune/codec/Lineage.h>
#include <rune/codec/Evolve.h>

#include <stdexcept>
#include <string>

namespace rune::codec {

void lineageRow(const Descriptor& evolving, uint16_t version, std::span<const uint16_t> tags, uint32_t* out) {
    // The record layout of `version` is exactly materializeAsOf's (the writer encodes with it).
    const Descriptor at = materializeAsOf(evolving, version);
    for (std::size_t i = 0; i < tags.size(); ++i) {
        out[i] = kAbsentField;
        for (const auto& f : at.fields) {
            if (f.id == tags[i]) { out[i] = f.offset; break; }
        }
    }
}

FieldLineage::FieldLineage(const Descriptor& evolving, std::span<const uint16_t> tags)
    : width_(tags.size()), maxVersion_(evolving.version) {
    rows_.assign(static_cast<std::size_t>(maxVersion_) * width_, kAbsentField);
    for (uint32_t v = 1; v <= maxVersion_; ++v)
        lineageRow(evolving, static_cast<uint16_t>(v), tags, rows_.data() + static_cast<std::size_t>(v - 1u) * width_);
}

FieldLineage FieldLineage::forReader(const Descriptor& evolving, std::string_view schemaName, uint16_t version,
                                     std::span<const uint16_t> tags, std::span<const uint8_t> types,
                                     const uint32_t* rowAtVersion) {
    const auto refuse = [&](const std::string& why) {
        throw std::invalid_argument("lineage of '" + evolving.name + "' is not the lineage of '" +
                                    std::string(schemaName) + "' v" + std::to_string(version) + ": " + why);
    };
    if (evolving.name != schemaName) refuse("another schema");
    if (version == 0 || evolving.version < version) refuse("it ends at v" + std::to_string(evolving.version));
    if (tags.size() != types.size()) refuse("malformed reader");
    const Descriptor at = materializeAsOf(evolving, version);
    if (at.fields.size() != tags.size()) refuse("another field set");
    for (std::size_t i = 0; i < tags.size(); ++i) {
        const FieldDescriptor* f = nullptr;
        for (const auto& g : at.fields) if (g.id == tags[i]) { f = &g; break; }
        if (f == nullptr) refuse("field tag " + std::to_string(tags[i]) + " is not part of v" + std::to_string(version));
        if (static_cast<uint8_t>(f->type) != types[i]) refuse("field '" + f->name + "' has another type");
        if (f->offset != rowAtVersion[i]) refuse("field '" + f->name + "' has another place");
    }
    return FieldLineage(evolving, tags);
}

}  // namespace rune::codec
