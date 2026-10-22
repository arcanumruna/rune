// include/rune/codec/Lineage.h — #418: where one reader's fields sit in a record of EVERY schema version.
//
// A record carries its own version ([blockLength u16][version u16]).  DROP reclaims a field's slot and re-indexes
// the var fields after it, so the same field (durable tag) can sit at a different place in records of different
// versions.  A reader therefore decodes each record with the layout of the record's OWN version: the row of this
// table for that version.  A row has one entry per reader field (the reader's declaration order): the byte offset in
// the fixed block (fixed fields), the var index (string / bytes), or kAbsentField when the field is not part of the
// record's version (added later, or dropped before it).
//
// The generated types embed the rows for versions 1..kVersion as constants (decode without any context); a record of
// a LATER version needs the topic's lineage at run time (FieldLineage, built once from the evolving descriptor the
// broker serves — Client::lineageAndWait<T>).  Building is cold (allocates); row() is a bounds check and an index.
#pragma once
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>
#include <rune/codec/Descriptor.h>

namespace rune::codec {

/// A reader field that is not part of a record's version.
inline constexpr uint32_t kAbsentField = 0xFFFFFFFFu;

/// The row of `tags` at `version` of `evolving` (one entry per tag, see above) into `out` (tags.size() entries).
void lineageRow(const Descriptor& evolving, uint16_t version, std::span<const uint16_t> tags, uint32_t* out);

/// The rows of one reader's fields for versions 1..evolving.version of a topic's evolving descriptor.
class FieldLineage {
public:
    FieldLineage() = default;
    /// COLD.  `evolving`: every field ever declared, with its lifetime (the broker's evolving descriptor).
    /// `tags`: the reader's fields in its declaration order.
    FieldLineage(const Descriptor& evolving, std::span<const uint16_t> tags);

    /// The row for `version`, or nullptr when the lineage does not know that version (0, or newer than it).
    [[nodiscard]] const uint32_t* row(uint16_t version) const noexcept {
        if (version == 0 || version > maxVersion_) [[unlikely]] return nullptr;
        return rows_.data() + static_cast<std::size_t>(version - 1u) * width_;
    }
    /// COLD.  The lineage of a generated reader (T::lineage): throws std::invalid_argument unless `evolving` is the
    /// reader's schema - the same schema name, a lineage reaching `version`, and at `version` exactly the reader's
    /// fields (tags, types and places; names may differ: a RENAME is not versioned).
    [[nodiscard]] static FieldLineage forReader(const Descriptor& evolving, std::string_view schemaName,
                                                uint16_t version, std::span<const uint16_t> tags,
                                                std::span<const uint8_t> types, const uint32_t* rowAtVersion);

    /// The newest version this lineage knows (0 = empty).
    [[nodiscard]] uint16_t maxVersion() const noexcept { return maxVersion_; }

private:
    std::vector<uint32_t> rows_;
    std::size_t           width_      = 0;
    uint16_t              maxVersion_ = 0;
};

}  // namespace rune::codec
