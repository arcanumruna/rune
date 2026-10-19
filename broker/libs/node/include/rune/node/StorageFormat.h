#pragma once
// StorageFormat.h — how an EMPTY, not yet formatted data dir is formatted at boot
// (ISR design v3 §3.2, Step 5).  RUNE_STORAGE_FORMAT is read only by parseNodeConfig
// and only matters while the data dir holds no storage.meta.

#include <cstdint>
#include <optional>
#include <string_view>

namespace rune::node {

enum class StorageFormat : uint8_t {
    Required,     ///< unset / "": never format; an empty dir refuses to start (R-1)
    New,          ///< "new": format for a new cluster
    Join,         ///< "join": format as a new or replaced disk of a running cluster
    Auto,         ///< "auto": development only — format any empty dir
    TestFounder,  ///< in-process rigs only (the NodeConfig default): like Auto, but an
                  ///< empty dir formats as founder on single AND multi-node.  The env never yields it.
};

/// "" → Required; "new" / "join" / "auto" → that value; anything else (including any
/// spelling of TestFounder) → nullopt.  Never returns TestFounder.
[[nodiscard]] inline std::optional<StorageFormat> parseStorageFormat(std::string_view v) noexcept {
    if (v.empty())   return StorageFormat::Required;
    if (v == "new")  return StorageFormat::New;
    if (v == "join") return StorageFormat::Join;
    if (v == "auto") return StorageFormat::Auto;
    return std::nullopt;
}

/// The env spelling of a format (TestFounder, which the env never yields, prints "testfounder").
[[nodiscard]] inline const char* storageFormatName(StorageFormat f) noexcept {
    switch (f) {
        case StorageFormat::Required:    return "";
        case StorageFormat::New:         return "new";
        case StorageFormat::Join:        return "join";
        case StorageFormat::Auto:        return "auto";
        case StorageFormat::TestFounder: return "testfounder";
    }
    return "";
}

} // namespace rune::node
