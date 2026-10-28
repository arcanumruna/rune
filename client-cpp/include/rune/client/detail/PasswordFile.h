#pragma once
// One reader for every password file (ClientConfig::passwordFile, `rune security bootstrap --password-file`).
#include <rune/client/detail/SecureZero.h>

#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <optional>
#include <string>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace rune::client::detail {

/// Reads a password file: an error text, or nullopt with `out` holding the password.
/// - A regular file whose mode gives group or others ANY access is refused (like ssh and the broker's cluster key:
///   chmod 0600 / 0400).  Pipes and other non-regular files (`--password-file <(...)`) are not mode-checked.
/// - A regular file owned by another user than the effective user is refused (#678).
/// - At most kMaxPasswordBytes after stripping: the read goes into a fixed stack buffer of kMaxPasswordBytes + 3,
///   which is wiped; more bytes than that is refused ("larger than 1024 bytes").
/// - Only trailing '\n' and '\r' are stripped; spaces and tabs are part of the password.
/// - An empty password (after stripping) is refused ("is empty").
[[nodiscard]] inline std::optional<std::string> readPasswordFile(const std::string& path, std::string& out) {
    char buf[kMaxPasswordBytes + 3];
    struct Guard {
        char* b; int fd;
        ~Guard() { secureZero(b, sizeof(buf)); if (fd >= 0) ::close(fd); }
    } guard{buf, -1};
    guard.fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (guard.fd < 0) return "cannot read '" + path + "'";
    struct stat st {};
    if (::fstat(guard.fd, &st) != 0) return "cannot read '" + path + "'";
    if (S_ISREG(st.st_mode) && (st.st_mode & (S_IRWXG | S_IRWXO)) != 0) {
        char m[8];
        std::snprintf(m, sizeof m, "%04o", static_cast<unsigned>(st.st_mode & 0777));
        return "'" + path + "' is accessible by group or others (mode " + m + "); chmod 0600 it";
    }
    if (S_ISREG(st.st_mode) && st.st_uid != ::geteuid())   // #678: another user could rewrite it
        return "'" + path + "' is owned by another user (uid " + std::to_string(st.st_uid) + "); it must be owned by the user running the client";
    std::size_t n = 0;
    while (n < sizeof(buf)) {
        const ssize_t r = ::read(guard.fd, buf + n, sizeof(buf) - n);
        if (r < 0) {
            if (errno == EINTR) continue;
            return "cannot read '" + path + "'";
        }
        if (r == 0) break;
        n += static_cast<std::size_t>(r);
    }
    if (n == sizeof(buf)) {
        char extra;
        ssize_t r;
        do { r = ::read(guard.fd, &extra, 1); } while (r < 0 && errno == EINTR);
        secureZero(&extra, 1);
        if (r != 0) return "'" + path + "' is larger than " + std::to_string(kMaxPasswordBytes) + " bytes";
    }
    while (n > 0 && (buf[n - 1] == '\n' || buf[n - 1] == '\r')) --n;
    if (n == 0) return "'" + path + "' is empty";
    if (n > kMaxPasswordBytes) return "'" + path + "' is larger than " + std::to_string(kMaxPasswordBytes) + " bytes";
    out.assign(buf, n);
    return std::nullopt;
}

} // namespace rune::client::detail
