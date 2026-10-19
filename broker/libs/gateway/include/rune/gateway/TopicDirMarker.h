#pragma once
// TopicDirMarker.h — catalog history Task 16 fix round 1 (R1): the topic-id marker.
//
// Every disk-backed CATALOG topic dir carries `topics/<name>/topic-id` holding exactly
// "<topicId>\n", written durably BEFORE any partition dir or Log exists.  So a dir whose marker
// names id X holds only X's data, and a dir with no marker and no record bytes holds nothing.
// Every open (TopicRegistry::createTopicAt) and every boot reconcile compares the marker with
// the claiming catalog id. 
#include <cstdint>
#include <filesystem>

namespace rune::gateway {

inline constexpr const char* kTopicIdMarkerFile    = "topic-id";
inline constexpr const char* kTopicIdMarkerTmpFile = "topic-id.tmp";

struct TopicIdMarker {
    enum class Kind : uint8_t { Absent, Valid, Malformed };
    Kind    kind = Kind::Absent;
    int32_t id   = -1;
};

/// Reads topicDir/topic-id.  Absent: no such entry.  Valid: exactly <digits>"\n", no leading zero
/// unless the whole number is "0", value <= INT32_MAX.  Malformed: anything else (a symlink, a
/// non-regular file, an older build's "pending N" / "committed N", a read error).  Never follows a
/// symlink.  Never throws.
[[nodiscard]] TopicIdMarker readTopicIdMarker(const std::filesystem::path& topicDir) noexcept;

/// Atomically and durably writes "<id>\n" (tmp + fsync + rename + directory fsync).  false (logged,
/// tmp removed) when topicDir is a symlink / not a dir or any step fails.  Never throws.
bool writeTopicIdMarker(const std::filesystem::path& topicDir, int32_t id) noexcept;

/// true iff any <digits>/ child of topicDir holds a regular (non-symlink) file with extension ".log"
/// and size > 0.  Errors count as true (conservative: "may hold data").
[[nodiscard]] bool topicDirHoldsRecords(const std::filesystem::path& topicDir) noexcept;

} // namespace rune::gateway
