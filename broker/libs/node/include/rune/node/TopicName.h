#pragma once
// TopicName.h — the ONE rule for a DDL-created topic or view name (DefineTopic,
// CreateView), shared by the admin thread (answers InvalidTopicName = 40 before
// anything is created) and the control thread's createTopicForAdmin (refuses the
// same names defensively).  A name becomes a directory component —
// dataDir/topics/<name>/<partition>/ — so it must be one safe path segment:
//
//   * 1..249 bytes (Kafka's limit; leaves room under a 255-byte segment);
//   * ASCII letters, digits, '.', '_', '-' only — no '/', '\\', NUL, space,
//     control bytes or non-ASCII (no path traversal, no absolute paths);
//   * not "." or "..";
//   * not starting with "__": reserved for internal topics (__topic_schemas,
//     __views, __consumer_offsets, __transaction_state, __cluster_metadata,
//     __view_snapshots) and the boot-recovery placeholders __orphan.<id>.
//
// Names are CASE-SENSITIVE identifiers, but two names that differ only in ASCII
// case would share one directory on a case-insensitive filesystem (macOS APFS
// default), so a create whose name case-folds onto an existing DIFFERENT name is
// refused with TopicExists (topicNamesFoldEqual; see createTopicForAdmin).
//
// Static RUNE_TOPICS names obey the same rule, checked at config parse (user
// decision D2d, NodeConfig.cpp — Task 5a).
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace rune::node {

inline constexpr std::size_t      kMaxTopicNameBytes   = 249;
inline constexpr std::string_view kReservedTopicPrefix = "__";

enum class TopicNameError : uint8_t { None, Empty, TooLong, DotSegment, BadChar, Reserved };

[[nodiscard]] constexpr TopicNameError checkTopicName(std::string_view n) noexcept {
    if (n.empty()) return TopicNameError::Empty;
    if (n.size() > kMaxTopicNameBytes) return TopicNameError::TooLong;
    if (n == "." || n == "..") return TopicNameError::DotSegment;
    for (const char c : n) {
        const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                        (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
        if (!ok) return TopicNameError::BadChar;
    }
    if (n.starts_with(kReservedTopicPrefix)) return TopicNameError::Reserved;
    return TopicNameError::None;
}

[[nodiscard]] constexpr bool isValidTopicName(std::string_view n) noexcept {
    return checkTopicName(n) == TopicNameError::None;
}

[[nodiscard]] constexpr char asciiLower(char c) noexcept {
    return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
}

/// True iff `a` and `b` are equal ignoring ASCII letter case (other bytes exact).
[[nodiscard]] constexpr bool topicNamesFoldEqual(std::string_view a, std::string_view b) noexcept {
    if (a.size() != b.size()) return false;
    for (std::size_t i = 0; i < a.size(); ++i)
        if (asciiLower(a[i]) != asciiLower(b[i])) return false;
    return true;
}

/// A short reason for config errors ("RUNE_TOPICS topic name 'x' is invalid (<text>)").
[[nodiscard]] constexpr std::string_view topicNameErrorText(TopicNameError e) noexcept {
    switch (e) {
        case TopicNameError::None:       return "ok";
        case TopicNameError::Empty:      return "empty";
        case TopicNameError::TooLong:    return "longer than 249 bytes";
        case TopicNameError::DotSegment: return "'.' or '..'";
        case TopicNameError::BadChar:    return "a byte outside [A-Za-z0-9._-]";
        case TopicNameError::Reserved:   return "the '__' prefix is reserved for internal topics";
    }
    return "invalid";
}

} // namespace rune::node
