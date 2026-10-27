#pragma once
// NamespacePaths.h - the on-disk layout of a namespace (namespaces plan Task 6, D14).
//
//   <dataDir>/ns-<nsId>/topics/<name>/<partition>/   a topic's partition logs
//   <dataDir>/ns-<nsId>/dropped/<topicId>.<name>     a dropped topic's dir, until purged
//   <dataDir>/ns-<nsId>/quarantine/<name>-<ts>       a dir the boot reconcile could not adopt
//
// Every disk path to a topic is built here: no other broker source spells the "topics" directory (the source guard
// broker/scripts/check_no_topics_literal.py enforces it).  A dir directly under <dataDir> that is not ns-<u32> is
// never a namespace (the boot reconcile logs it and leaves it alone); ns-<id>.deleting (a namespace drop in progress)
// is not ns-<u32> either.
#include <charconv>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>

namespace rune::node {

inline constexpr std::string_view kNsDirPrefix = "ns-";
/// The topics directory's name: under nsRoot, and (pre-namespace, never migrated) directly under <dataDir>.
inline constexpr std::string_view kTopicsDirName = "topics";

/// "ns-<nsId>" (decimal, no leading zeros).
[[nodiscard]] inline std::string nsDirName(uint32_t nsId) {
    return std::string{kNsDirPrefix} + std::to_string(nsId);
}

/// dataDir/"ns-<nsId>".
[[nodiscard]] inline std::filesystem::path nsRoot(const std::filesystem::path& dataDir, uint32_t nsId) {
    return dataDir / nsDirName(nsId);
}

/// nsRoot/"topics".
[[nodiscard]] inline std::filesystem::path topicsRoot(const std::filesystem::path& dataDir, uint32_t nsId) {
    return nsRoot(dataDir, nsId) / kTopicsDirName;
}

/// nsRoot/"dropped" (dropped topic dirs awaiting their purge).
[[nodiscard]] inline std::filesystem::path droppedRoot(const std::filesystem::path& dataDir, uint32_t nsId) {
    return nsRoot(dataDir, nsId) / "dropped";
}

/// nsRoot/"quarantine" (dirs the boot reconcile moved aside).
[[nodiscard]] inline std::filesystem::path quarantineRoot(const std::filesystem::path& dataDir, uint32_t nsId) {
    return nsRoot(dataDir, nsId) / "quarantine";
}

/// The nsId of a "ns-<u32>" dir name: decimal digits only, no sign, no leading zero (except "ns-0"), within u32.
/// Anything else ("ns-", "ns-01", "ns--1", "ns-4294967296", "ns-1.deleting", "topics") is nullopt.
[[nodiscard]] inline std::optional<uint32_t> parseNsDir(std::string_view dirName) noexcept {
    if (!dirName.starts_with(kNsDirPrefix)) return std::nullopt;
    const std::string_view digits = dirName.substr(kNsDirPrefix.size());
    if (digits.empty() || digits.size() > 10) return std::nullopt;
    if (digits.size() > 1 && digits.front() == '0') return std::nullopt;
    for (const char c : digits) {
        if (c < '0' || c > '9') return std::nullopt;
    }
    uint32_t v = 0;
    const auto [ptr, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), v);
    if (ec != std::errc{} || ptr != digits.data() + digits.size()) return std::nullopt;
    return v;
}

} // namespace rune::node
