// CatalogCheckpoint.cpp — `dataDir/__catalog/applied` (catalog history ruling 8).
#include <rune/node/CatalogCheckpoint.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <stdexcept>

#include <fcntl.h>
#include <unistd.h>

namespace rune::node {
namespace {

constexpr std::size_t kFileBytes = 8 + 8 + 8 + 8;

uint64_t fnv1a64(const uint8_t* p, std::size_t n) noexcept {
    uint64_t h = 0xcbf29ce484222325ULL;
    for (std::size_t i = 0; i < n; ++i) { h ^= p[i]; h *= 0x100000001b3ULL; }
    return h;
}
void putU64(uint8_t* p, uint64_t v) noexcept { for (int i = 0; i < 8; ++i) p[i] = static_cast<uint8_t>(v >> (8 * i)); }
uint64_t getU64(const uint8_t* p) noexcept {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(p[i]) << (8 * i);
    return v;
}

bool writeAllAndSync(int fd, const uint8_t* p, std::size_t n) noexcept {
    std::size_t done = 0;
    while (done < n) {
        const ssize_t w = ::write(fd, p + done, n - done);
        if (w < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        done += static_cast<std::size_t>(w);
    }
    return ::fsync(fd) == 0;
}

bool fsyncDir(const std::string& dir) noexcept {
    const int fd = ::open(dir.c_str(), O_RDONLY);
    if (fd < 0) return false;
    const bool ok = ::fsync(fd) == 0;
    const int saved = errno;
    ::close(fd);
    errno = saved;
    return ok;
}

constexpr std::array<char, 8> kEpochMagic{'R', 'C', 'A', 'T', 'H', 'E', 'P', '1'};
constexpr std::size_t kEpochFileBytes = 48;

void putU32(uint8_t* p, uint32_t v) noexcept { for (int i = 0; i < 4; ++i) p[i] = static_cast<uint8_t>(v >> (8 * i)); }
uint32_t getU32(const uint8_t* p) noexcept {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(p[i]) << (8 * i);
    return v;
}

/// tmp + fsync + rename + directory fsync.  false = not durable (errno set).
bool replaceDurably(const char* path, const char* tmp, const char* dir, const uint8_t* bytes, std::size_t n) noexcept {
    const int fd = ::open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return false;
    if (!writeAllAndSync(fd, bytes, n)) {
        const int saved = errno;
        ::close(fd);
        ::unlink(tmp);
        errno = saved;
        return false;
    }
    if (::close(fd) != 0) {
        const int saved = errno;
        ::unlink(tmp);
        errno = saved;
        return false;
    }
    if (::rename(tmp, path) != 0) {
        const int saved = errno;
        ::unlink(tmp);
        errno = saved;
        return false;
    }
    const int dfd = ::open(dir, O_RDONLY);
    if (dfd < 0) return false;
    const bool ok = ::fsync(dfd) == 0;
    const int saved = errno;
    ::close(dfd);
    errno = saved;
    return ok;
}

} // namespace

std::optional<CatalogHistoryEpoch> readCatalogHistoryEpoch(const std::string& path) noexcept {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return std::nullopt;
    std::array<uint8_t, kEpochFileBytes + 1> buf{};
    std::size_t got = 0;
    while (got < buf.size()) {
        const ssize_t r = ::read(fd, buf.data() + got, buf.size() - got);
        if (r < 0) {
            if (errno == EINTR) continue;
            ::close(fd);
            return std::nullopt;
        }
        if (r == 0) break;
        got += static_cast<std::size_t>(r);
    }
    ::close(fd);
    if (got != kEpochFileBytes) return std::nullopt;
    if (std::memcmp(buf.data(), kEpochMagic.data(), kEpochMagic.size()) != 0) return std::nullopt;
    if (fnv1a64(buf.data(), 40) != getU64(buf.data() + 40)) return std::nullopt;
    CatalogHistoryEpoch e;
    e.epoch             = getU64(buf.data() + 8);
    e.servedFloor       = static_cast<int64_t>(getU64(buf.data() + 16));
    e.anchorBase        = static_cast<int64_t>(getU64(buf.data() + 24));
    e.anchorLeaderEpoch = static_cast<int32_t>(getU32(buf.data() + 32));
    e.anchorCrc         = getU32(buf.data() + 36);
    if (e.servedFloor < 0 || (e.servedFloor > 0 && (e.anchorBase < 0 || e.anchorBase >= e.servedFloor)))
        return std::nullopt;
    return e;
}

bool writeCatalogHistoryEpoch(const std::string& path, const std::string& tmpPath, const std::string& dir,
                              const CatalogHistoryEpoch& e) noexcept {
    std::array<uint8_t, kEpochFileBytes> buf{};
    std::memcpy(buf.data(), kEpochMagic.data(), kEpochMagic.size());
    putU64(buf.data() + 8, e.epoch);
    putU64(buf.data() + 16, static_cast<uint64_t>(e.servedFloor));
    putU64(buf.data() + 24, static_cast<uint64_t>(e.anchorBase));
    putU32(buf.data() + 32, static_cast<uint32_t>(e.anchorLeaderEpoch));
    putU32(buf.data() + 36, e.anchorCrc);
    putU64(buf.data() + 40, fnv1a64(buf.data(), 40));
    return replaceDurably(path.c_str(), tmpPath.c_str(), dir.c_str(), buf.data(), buf.size());
}

std::optional<LogCheckpoint> readLogCheckpoint(const std::string& catalogDir, const LogCheckpointKind& kind) {
    const std::string path = (std::filesystem::path{catalogDir} / kCatalogCheckpointFile).string();
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) return std::nullopt;
        throw std::runtime_error(std::string{kind.name} + " checkpoint " + path + ": open failed: " + std::strerror(errno));
    }
    std::array<uint8_t, kFileBytes + 1> buf{};
    std::size_t got = 0;
    while (got < buf.size()) {
        const ssize_t r = ::read(fd, buf.data() + got, buf.size() - got);
        if (r < 0) {
            if (errno == EINTR) continue;
            const int saved = errno;
            ::close(fd);
            throw std::runtime_error(std::string{kind.name} + " checkpoint " + path + ": read failed: " + std::strerror(saved));
        }
        if (r == 0) break;
        got += static_cast<std::size_t>(r);
    }
    ::close(fd);
    if (got != kFileBytes)
        throw std::runtime_error(std::string{kind.name} + " checkpoint " + path + " is " + std::to_string(got) + " bytes (expected " +
                                 std::to_string(kFileBytes) + ") — damaged; refusing to guess what was applied");
    if (std::memcmp(buf.data(), kind.magic, sizeof kind.magic) != 0)
        throw std::runtime_error(std::string{kind.name} + " checkpoint " + path + " has a foreign magic — refusing to guess what was applied");
    if (fnv1a64(buf.data(), 24) != getU64(buf.data() + 24))
        throw std::runtime_error(std::string{kind.name} + " checkpoint " + path + " fails its checksum — damaged; refusing to guess "
                                 "what was applied");
    LogCheckpoint c;
    c.appliedOffset = static_cast<int64_t>(getU64(buf.data() + 8));
    c.digest        = getU64(buf.data() + 16);
    if (c.appliedOffset < 0)
        throw std::runtime_error(std::string{kind.name} + " checkpoint " + path + " names a negative offset " +
                                 std::to_string(c.appliedOffset));
    return c;
}

bool writeLogCheckpoint(const std::string& catalogDir, const LogCheckpoint& c, const LogCheckpointKind& kind) {
    std::array<uint8_t, kFileBytes> buf{};
    std::memcpy(buf.data(), kind.magic, sizeof kind.magic);
    putU64(buf.data() + 8, static_cast<uint64_t>(c.appliedOffset));
    putU64(buf.data() + 16, c.digest);
    putU64(buf.data() + 24, fnv1a64(buf.data(), 24));

    // Fixed-size path buffers would cap the dir length; this is the cold control
    // plane (one write per applied DDL), and the strings are built per call.
    const std::string path = (std::filesystem::path{catalogDir} / kCatalogCheckpointFile).string();
    const std::string tmp  = path + ".tmp";
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, static_cast<mode_t>(kind.fileMode));
    if (fd < 0) return false;
    if (!writeAllAndSync(fd, buf.data(), buf.size())) {
        const int saved = errno;
        ::close(fd);
        ::unlink(tmp.c_str());
        errno = saved;
        return false;
    }
    if (::close(fd) != 0) {
        const int saved = errno;
        ::unlink(tmp.c_str());
        errno = saved;
        return false;
    }
    if (::rename(tmp.c_str(), path.c_str()) != 0) {
        const int saved = errno;
        ::unlink(tmp.c_str());
        errno = saved;
        return false;
    }
    return fsyncDir(catalogDir);
}

std::optional<std::string> logCheckpointBeyondLog(const std::optional<LogCheckpoint>& c, int64_t logEndOffset,
                                                  const std::string& dir, const LogCheckpointKind& kind) {
    if (!c || c->appliedOffset < logEndOffset) return std::nullopt;
    return std::string{kind.name} + " at " + dir + ": the applied checkpoint names offset " +
           std::to_string(c->appliedOffset) + " but the local log ends at " + std::to_string(logEndOffset) +
           " — events this broker already applied are missing from its log (lost writes / a replaced data "
           "dir); replaying the shorter history would silently fork " + kind.name + ".  Refusing to start: restore "
           "the data dir (including " + kind.name + ") from backup.  Only in a multi-broker cluster whose other "
           "brokers hold " + kind.name + " may dataDir/" + kind.name + " (including '" +
           std::string{kCatalogCheckpointFile} + "') be removed so this broker refetches it from its peers; "
           "never on a single broker (docs/SELF-HOSTING.md)";
}

std::optional<std::string> catalogCheckpointBeyondLog(const std::optional<CatalogCheckpoint>& c,
                                                      int64_t logEndOffset, const std::string& catalogDir) {
    return logCheckpointBeyondLog(c, logEndOffset, catalogDir, kCatalogLogKind);
}

} // namespace rune::node
