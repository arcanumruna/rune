#include <rune/gateway/TopicDirMarker.h>

#include <spdlog/spdlog.h>

#include <cerrno>
#include <climits>
#include <cstdio>
#include <cstring>
#include <string>
#include <fcntl.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace rune::gateway {

namespace {
bool allDigits(const std::string& s) {
    if (s.empty()) return false;
    for (const char c : s) if (c < '0' || c > '9') return false;
    return true;
}
void fsyncDir(const fs::path& dir, bool& ok) noexcept {
    const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) { ok = false; return; }
    if (::fsync(fd) != 0) ok = false;
    ::close(fd);
}
} // namespace

TopicIdMarker readTopicIdMarker(const fs::path& topicDir) noexcept {
    TopicIdMarker m;
    try {
        const fs::path p = topicDir / kTopicIdMarkerFile;
        std::error_code ec;
        const fs::file_status st = fs::symlink_status(p, ec);
        if (ec || !fs::exists(st)) { m.kind = ec ? TopicIdMarker::Kind::Malformed : TopicIdMarker::Kind::Absent; return m; }
        m.kind = TopicIdMarker::Kind::Malformed;
        if (fs::is_symlink(st) || !fs::is_regular_file(st)) return m;
        const int fd = ::open(p.c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
        if (fd < 0) return m;
        char buf[32];
        const ssize_t n = ::read(fd, buf, sizeof buf);
        ::close(fd);
        if (n < 2 || n >= static_cast<ssize_t>(sizeof buf) || buf[n - 1] != '\n') return m;
        const std::string digits(buf, static_cast<std::size_t>(n - 1));
        if (!allDigits(digits) || digits.size() > 10) return m;
        if (digits.size() > 1 && digits[0] == '0') return m;
        const long long v = std::stoll(digits);
        if (v > INT32_MAX) return m;
        m.kind = TopicIdMarker::Kind::Valid;
        m.id   = static_cast<int32_t>(v);
    } catch (...) {
        m = TopicIdMarker{TopicIdMarker::Kind::Malformed, -1};
    }
    return m;
}

bool writeTopicIdMarker(const fs::path& topicDir, int32_t id) noexcept {
    try {
        std::error_code ec;
        const fs::file_status dst = fs::symlink_status(topicDir, ec);
        if (ec || fs::is_symlink(dst) || !fs::is_directory(dst)) {
            spdlog::error("topic-id marker: '{}' is not a plain directory — not written", topicDir.string());
            return false;
        }
        const fs::path tmp    = topicDir / kTopicIdMarkerTmpFile;
        const fs::path marker = topicDir / kTopicIdMarkerFile;
        char buf[24];
        const int len = std::snprintf(buf, sizeof buf, "%d\n", id);
        const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_NOFOLLOW | O_CLOEXEC, 0644);
        if (fd < 0) {
            spdlog::error("topic-id marker: cannot create '{}': {}", tmp.string(), std::strerror(errno));
            return false;
        }
        bool ok = true;
        int err = 0;
        for (int off = 0; off < len;) {
            const ssize_t w = ::write(fd, buf + off, static_cast<std::size_t>(len - off));
            if (w < 0 && errno == EINTR) continue;
            if (w <= 0) { ok = false; err = errno; break; }
            off += static_cast<int>(w);
        }
        if (ok && ::fsync(fd) != 0) { ok = false; err = errno; }
        ::close(fd);
        if (ok) {
            fs::rename(tmp, marker, ec);
            if (ec) { ok = false; err = ec.value(); }
        }
        if (ok) fsyncDir(topicDir, ok);
        if (!ok) {
            std::error_code rec;
            fs::remove(tmp, rec);
            spdlog::error("topic-id marker: writing '{}' failed (errno {}: {})", marker.string(), err,
                          std::strerror(err));
            return false;
        }
        return true;
    } catch (...) {
        return false;
    }
}

bool topicDirHoldsRecords(const fs::path& topicDir) noexcept {
    try {
        std::error_code ec;
        for (fs::directory_iterator it(topicDir, ec), end; !ec && it != end; it.increment(ec)) {
            std::error_code sec;
            const fs::file_status st = it->symlink_status(sec);
            if (sec) return true;
            if (!fs::is_directory(st) || fs::is_symlink(st)) continue;
            const std::string name = it->path().filename().string();
            if (!allDigits(name)) continue;
            std::error_code pec;
            for (fs::directory_iterator pit(it->path(), pec), pend; !pec && pit != pend; pit.increment(pec)) {
                std::error_code fec;
                const fs::file_status pst = pit->symlink_status(fec);
                if (fec) return true;
                if (!fs::is_regular_file(pst) || pit->path().extension() != ".log") continue;
                const auto size = fs::file_size(pit->path(), fec);
                if (fec || size > 0) return true;
            }
            if (pec) return true;
        }
        return static_cast<bool>(ec);
    } catch (...) {
        return true;
    }
}

} // namespace rune::gateway
