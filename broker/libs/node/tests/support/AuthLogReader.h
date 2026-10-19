#pragma once
// AuthLogReader.h - read a broker's __catalog / __security replica back as decoded events (CatalogPrincipalTest's method,
// shared with the interaction matrix): `read(offset)` runs on the admin thread via onAdminThreadForTest.

#include <rune/catalog/CatalogEvent.h>
#include <rune/node/BrokerNode.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/security/SecurityEvent.h>

#include <optional>
#include <span>
#include <string>
#include <vector>

namespace rune::node::test {

/// Every event of a replica's log, in offset order (one batch per read).
template <class Event, class Read, class Decode>
std::vector<Event> readAllEvents(Read read, Decode decode, int64_t* cursor = nullptr) {
    std::vector<Event> out;
    int64_t offset = cursor ? *cursor : 0;
    for (;;) {
        const std::vector<uint8_t> data = read(offset);
        if (data.empty()) break;
        rune::protocol::RecordBatchReader reader(data.data(), data.size());
        if (!reader.ensureValid()) break;
        int64_t last = -1;
        reader.forEachRecord([&](std::string_view, std::string_view v, int64_t off, int64_t) {
            last = off;
            auto d = decode(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()));
            if (d) out.push_back(std::move(*d));
        });
        if (last < offset) break;
        offset = last + 1;
    }
    if (cursor) *cursor = offset;
    return out;
}

inline std::vector<rune::catalog::CatalogEvent> readCatalogEvents(BrokerNode& n, int64_t* cursor = nullptr) {
    return readAllEvents<rune::catalog::CatalogEvent>(
        [&n](int64_t off) { return n.onAdminThreadForTest([&n, off] { return n.catalogReplicaForTest()->read(off, 1 << 20); }); },
        [](std::span<const uint8_t> b) {
            auto d = rune::catalog::decode(b);
            return d ? std::optional(std::move(*d)) : std::nullopt;
        }, cursor);
}
inline std::vector<rune::security::SecurityEvent> readSecurityEvents(BrokerNode& n, int64_t* cursor = nullptr) {
    return readAllEvents<rune::security::SecurityEvent>(
        [&n](int64_t off) { return n.onAdminThreadForTest([&n, off] { return n.securityReplicaForTest()->read(off, 1 << 20); }); },
        [](std::span<const uint8_t> b) {
            auto d = rune::security::decode(b);
            return d ? std::optional(std::move(*d)) : std::nullopt;
        }, cursor);
}

} // namespace rune::node::test
