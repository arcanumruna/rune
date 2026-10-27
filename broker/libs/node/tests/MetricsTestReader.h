// ---------------------------------------------------------------------------
// MetricsTestReader.h - test-side reads of a node's arcanum-metrics file (arcanum-metrics plan Task 8).
//
// Opens the file the way an external tool does (a fresh read-only mapping per call), finds the slot of
// (typeId, 4-byte LE node id) and copies it out.  Cold path; tests only.
// ---------------------------------------------------------------------------
#ifndef RUNE_NODE_TESTS_METRICS_TEST_READER_H
#define RUNE_NODE_TESTS_METRICS_TEST_READER_H

#include <rune/node/BrokerNode.h>
#include <rune/node/RuneMetrics.h>

#include <metrics/MetricsFile.h>
#include <metrics/MetricsReader.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>

namespace rune::node::test {

/// The whole Live copy of (typeId, node id) in the metrics file at `path`; nullopt when the file or the slot is
/// absent.
inline std::optional<metrics::Live> readRuneMetricLiveAt(const std::string& path, std::int32_t typeId,
                                                         std::int32_t nodeId) {
    metrics::MetricsFile file;
    if (metrics::MetricsFile::open(path.c_str(), 64, file) != metrics::FileStatus::Ok) return std::nullopt;
    const metrics::MetricsReader reader{file};
    std::array<std::byte, kRoleKeyLength> key{};
    encodeRoleKey(reinterpret_cast<std::uint8_t*>(key.data()), nodeId);
    const metrics::SlotRef ref = reader.findFirst(typeId, std::span<const std::byte>(key.data(), key.size()));
    if (ref.slot < 0) return std::nullopt;
    metrics::Live live;
    metrics::ReadStatus st = reader.read(ref.slot, live);
    if (st == metrics::ReadStatus::Reused) st = reader.read(ref.slot, live);
    if (st != metrics::ReadStatus::Live) return std::nullopt;
    return live;
}

/// The value of (typeId, node id) in the metrics file at `path`; nullopt when the file or the slot is absent.
inline std::optional<std::int64_t> readRuneMetricAt(const std::string& path, std::int32_t typeId, std::int32_t nodeId) {
    const auto live = readRuneMetricLiveAt(path, typeId, nodeId);
    if (!live) return std::nullopt;
    return live->value;
}

/// The value of `id` in `node`'s own metrics file.
inline std::optional<std::int64_t> readRuneMetric(const BrokerNode& node, RuneMetric id) {
    return readRuneMetricAt(node.metricsFilePath(), static_cast<std::int32_t>(id),
                            node.nodeIdForTest());
}

}  // namespace rune::node::test

#endif  // RUNE_NODE_TESTS_METRICS_TEST_READER_H
