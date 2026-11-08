#pragma once
// The four lease system columns every lease topic's stored schema starts with (tags 0..3), and the two inverse helpers
// that add them to / remove them from a user descriptor.  One definition for the broker, the catalog replay, the CLI
// and the QL script-scope view sources.

#include <rune/codec/Descriptor.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace rune::codec {

/// A lease topic `<t>` has a lease view named `<t>.leases`.
inline constexpr std::string_view kLeaseViewSuffix = ".leases";

struct LeaseColumn {
    std::string_view name;
    PrimType         type;
};

inline constexpr std::array<LeaseColumn, 4> kLeaseColumns = {{
    {"lease_version", PrimType::Int64},
    {"lease_ttl_ms", PrimType::UInt64},
    {"lease_holder", PrimType::UInt64},
    {"lease_remaining_ms", PrimType::UInt64},
}};

/// Lease topics (L5): the four system columns first at tags 0..3, every user field after them with its tag + 4.  False
/// when a shifted tag no longer fits.  Offsets are recomputed.
[[nodiscard]] inline bool prependLeaseColumns(Descriptor& d) {
    std::vector<FieldDescriptor> out;
    out.reserve(d.fields.size() + kLeaseColumns.size());
    uint16_t id = 0;
    for (const LeaseColumn& c : kLeaseColumns) {
        FieldDescriptor f;
        f.name = std::string(c.name); f.id = id++; f.type = c.type; f.sinceVersion = 1; f.untilVersion = 0xFFFF; f.isKey = false;
        out.push_back(std::move(f));
    }
    for (auto& f : d.fields) {
        if (uint32_t{f.id} + kLeaseColumns.size() > 0xFFFF) return false;
        f.id = static_cast<uint16_t>(f.id + kLeaseColumns.size());
        out.push_back(std::move(f));
    }
    d.fields = std::move(out);
    d.blockLength = computeOffsets(d.fields);
    return true;
}

/// The inverse of prependLeaseColumns: removes the four columns and subtracts 4 from every remaining id; the error is
/// "lease topic '<name>' does not start with the lease system columns" when they are not the first four fields
/// (name, type and order are checked).  `topicName` is only for the refusal text.
[[nodiscard]] inline std::expected<Descriptor, std::string> stripLeaseColumns(Descriptor d, std::string_view topicName) {
    const std::string refusal = "lease topic '" + std::string(topicName) + "' does not start with the lease system columns";
    if (d.fields.size() < kLeaseColumns.size()) return std::unexpected(refusal);
    for (std::size_t i = 0; i < kLeaseColumns.size(); ++i)
        if (d.fields[i].id != i || d.fields[i].name != kLeaseColumns[i].name || d.fields[i].type != kLeaseColumns[i].type)
            return std::unexpected(refusal);
    d.fields.erase(d.fields.begin(), d.fields.begin() + kLeaseColumns.size());
    for (auto& f : d.fields) f.id = static_cast<uint16_t>(f.id - kLeaseColumns.size());
    d.blockLength = computeOffsets(d.fields);
    return d;
}

}  // namespace rune::codec
