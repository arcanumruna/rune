#pragma once
// Atomic file publication for files that ANOTHER PROCESS polls (RUNE_EXPECT_FILE, the acked/received lists,
// counts): write `<path>.tmp`, flush + close, then rename(2) into place.  rename within one directory is atomic on
// POSIX, so a poller sees the previous file or none, and then the complete new one, never a prefix (issue 44).
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <system_error>

namespace rune::examples {

/// Replace `path` with `bytes` atomically.  Returns false (and leaves `path` untouched) when the write or the
/// rename fails.
inline bool writeFileAtomically(const std::string& path, std::string_view bytes) {
    const std::string tmp = path + ".tmp";
    {
        std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
        if (!f) return false;
        f.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        f.flush();
        f.close();
        if (f.fail()) {
            std::error_code ignore;
            std::filesystem::remove(tmp, ignore);
            return false;
        }
    }
    std::error_code ec;
    std::filesystem::rename(tmp, path, ec);
    if (ec) {
        std::error_code ignore;
        std::filesystem::remove(tmp, ignore);
        return false;
    }
    return true;
}

} // namespace rune::examples
