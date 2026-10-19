// StorageIdentity.cpp — storage.meta: resolve at boot, the format CLI, origin / manifest transitions
// (ISR design v3 §3.2, Step 5).  Cold path only; every function is static and stateless.
#include <rune/node/StorageIdentity.h>

#include <rune/transaction/detail/Crc32c.h>

#include <spdlog/spdlog.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string_view>
#include <system_error>
#include <variant>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

namespace rune::node {

namespace fs = std::filesystem;
using SI = StorageIdentity;

namespace {

constexpr const char* kMeta = "storage.meta";
constexpr const char* kTmp  = "storage.meta.tmp";

// ── storage.meta parse result: one state per outcome, each holding only what it needs ──────────────────────
struct Absent {};
struct Corrupt { std::string reason; };
using Parsed = std::variant<Absent, Corrupt, SI::Loaded>;

constexpr std::array<std::pair<SI::Class, const char*>, 3> kClasses{{
    {SI::Class::Consensus, "consensus"},
    {SI::Class::Knowledge, "knowledge"},
    {SI::Class::MetadataLog, "metadataLog"},
}};

[[nodiscard]] uint8_t bit(SI::Class c) noexcept { return static_cast<uint8_t>(c); }
[[nodiscard]] bool has(uint8_t m, SI::Class c) noexcept { return (m & bit(c)) != 0; }

const char* className(SI::Class c) noexcept {
    for (const auto& [k, n] : kClasses) if (k == c) return n;
    return "?";
}

std::string hexOf(uint64_t v, int width) {
    char b[17];
    std::snprintf(b, sizeof b, "%0*llx", width, static_cast<unsigned long long>(v));
    return b;
}

std::string manifestString(uint8_t m) {
    std::string s;
    for (const auto& [k, n] : kClasses) {
        if (!has(m, k)) continue;
        if (!s.empty()) s += ',';
        s += n;
    }
    return s;
}

std::string serialize(const SI::Loaded& l) {
    std::string body = "version=1\nstorageId=" + SI::idHex(l.id) + "\norigin=" + SI::originName(l.origin) + "\n";
    if (l.origin == SI::Origin::Undecided) body += std::string("mode=") + (l.mode == SI::Mode::Auto ? "auto" : "new") + "\n";
    body += "manifest=" + manifestString(l.manifest) + "\n";
    const uint32_t crc = rune::transaction::detail::crc32c(reinterpret_cast<const uint8_t*>(body.data()), body.size());
    body += "crc=" + hexOf(crc, 8) + "\n";
    return body;
}

[[nodiscard]] bool parseHex(std::string_view s, std::size_t digits, uint64_t& out) noexcept {
    if (s.size() != digits) return false;
    uint64_t v = 0;
    for (char c : s) {
        v <<= 4;
        if (c >= '0' && c <= '9') v |= static_cast<uint64_t>(c - '0');
        else if (c >= 'a' && c <= 'f') v |= static_cast<uint64_t>(c - 'a' + 10);
        else if (c >= 'A' && c <= 'F') v |= static_cast<uint64_t>(c - 'A' + 10);
        else return false;
    }
    out = v;
    return true;
}

Parsed parseText(const std::string& text) {
    // The last line is "crc=<8 hex>\n" over every byte before it.
    if (text.size() < 14 || text.back() != '\n') return Corrupt{"truncated"};
    const std::size_t lineStart = text.rfind('\n', text.size() - 2);
    const std::size_t crcAt = lineStart == std::string::npos ? 0 : lineStart + 1;
    const std::string_view crcLine(text.data() + crcAt, text.size() - 1 - crcAt);
    uint64_t want = 0;
    if (crcAt == 0 || crcLine.substr(0, 4) != "crc=" || !parseHex(crcLine.substr(4), 8, want)) return Corrupt{"truncated"};
    const uint32_t got = rune::transaction::detail::crc32c(reinterpret_cast<const uint8_t*>(text.data()), crcAt);
    if (got != static_cast<uint32_t>(want)) return Corrupt{"CRC mismatch"};

    SI::Loaded l;
    bool sawVersion = false, sawId = false, sawOrigin = false, sawManifest = false;
    std::string_view body(text.data(), crcAt);
    while (!body.empty()) {
        const std::size_t nl = body.find('\n');
        const std::string_view line = body.substr(0, nl);
        body.remove_prefix(nl + 1);
        const std::size_t eq = line.find('=');
        if (eq == std::string_view::npos) return Corrupt{"unknown key '" + std::string(line) + "'"};
        const std::string_view k = line.substr(0, eq), v = line.substr(eq + 1);
        if (k == "version") {
            if (v != "1") return Corrupt{"unsupported version '" + std::string(v) + "'"};
            sawVersion = true;
        } else if (k == "storageId") {
            uint64_t id = 0;
            if (!parseHex(v, 16, id)) return Corrupt{"truncated"};
            if (id == 0) return Corrupt{"storageId is 0"};
            l.id = static_cast<int64_t>(id);
            sawId = true;
        } else if (k == "origin") {
            if (v == "undecided") l.origin = SI::Origin::Undecided;
            else if (v == "founder") l.origin = SI::Origin::Founder;
            else if (v == "joiner") l.origin = SI::Origin::Joiner;
            else if (v == "legacy") l.origin = SI::Origin::Legacy;
            else return Corrupt{"unknown origin '" + std::string(v) + "'"};
            sawOrigin = true;
        } else if (k == "mode") {
            if (v == "new") l.mode = SI::Mode::New;
            else if (v == "auto") l.mode = SI::Mode::Auto;
            else return Corrupt{"unknown mode '" + std::string(v) + "'"};
        } else if (k == "manifest") {
            l.manifest = 0;
            std::string_view rest = v;
            while (!rest.empty()) {
                const std::size_t c = rest.find(',');
                const std::string_view name = rest.substr(0, c);
                bool known = false;
                for (const auto& [cls, n] : kClasses) if (name == n) { l.manifest |= bit(cls); known = true; }
                if (!known) return Corrupt{"unknown manifest class '" + std::string(name) + "'"};
                if (c == std::string_view::npos) break;
                rest.remove_prefix(c + 1);
            }
            sawManifest = true;
        } else {
            return Corrupt{"unknown key '" + std::string(k) + "'"};
        }
    }
    if (!sawVersion || !sawId || !sawOrigin || !sawManifest) return Corrupt{"truncated"};
    if (l.origin == SI::Origin::Undecided && l.mode == SI::Mode::None) return Corrupt{"undecided without mode"};
    if (l.origin != SI::Origin::Undecided) l.mode = SI::Mode::None;
    return l;
}

[[noreturn]] void ioFail(const char* op, const fs::path& p, int err);

Parsed parseFile(const fs::path& dir) {
    const fs::path p = dir / kMeta;
    std::error_code ec;
    if (!fs::exists(fs::symlink_status(p, ec))) return Absent{};
    errno = 0;
    std::ifstream f(p, std::ios::binary);
    if (!f) {
        // CP1 M4: an unreadable file (EACCES, EIO, ...) is an I/O fault, not a corrupt identity: advising "wipe the
        // dir" there would discard a healthy storage id.  Only a vanished target keeps the old reading.
        const int e = errno;
        if (e == 0 || e == ENOENT) return Corrupt{"truncated"};
        ioFail("open", p, e);
    }
    std::stringstream ss;
    ss << f.rdbuf();
    return parseText(ss.str());
}

[[noreturn]] void ioFail(const char* op, const fs::path& p, int err) {
    throw StorageIoError(std::string(op) + " " + p.string() + ": " + std::strerror(err));
}

void fsyncDir(const fs::path& dir) {
    const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) ioFail("open", dir, errno);
    if (::fsync(fd) != 0) { const int e = errno; ::close(fd); ioFail("fsync", dir, e); }
    ::close(fd);
}

void unlinkDurably(const fs::path& dir, const char* name) {
    const fs::path p = dir / name;
    if (::unlink(p.c_str()) != 0) {
        if (errno == ENOENT) return;
        ioFail("unlink", p, errno);
    }
    fsyncDir(dir);
}

[[nodiscard]] bool nonEmptyFile(const fs::path& p) {
    std::error_code ec;
    return fs::is_regular_file(p, ec) && fs::file_size(p, ec) > 0 && !ec;
}

[[nodiscard]] bool metadataLogPresent(const fs::path& dir) {
    // The literal BrokerNode opens: fs::path{config_.dataDir} / "__cluster_metadata".
    std::error_code ec;
    const fs::path m = dir / "__cluster_metadata";
    if (!fs::is_directory(m, ec)) return false;
    for (auto it = fs::recursive_directory_iterator(m, ec); !ec && it != fs::recursive_directory_iterator(); it.increment(ec))
        if (nonEmptyFile(it->path())) return true;
    return false;
}

[[nodiscard]] bool classPresent(const fs::path& dir, SI::Class c) {
    switch (c) {
        case SI::Class::Consensus:   return nonEmptyFile(dir / "node-state.dat") && nonEmptyFile(dir / "term.log");
        case SI::Class::Knowledge:   return nonEmptyFile(dir / "internal-isr.state");
        case SI::Class::MetadataLog: return metadataLogPresent(dir);
    }
    return false;
}

/// The R-3b <files>: the class's files that are missing or empty.
std::string missingFilesOf(const fs::path& dir, SI::Class c) {
    std::vector<std::string> gone;
    switch (c) {
        case SI::Class::Consensus:
            if (!nonEmptyFile(dir / "node-state.dat")) gone.emplace_back("node-state.dat");
            if (!nonEmptyFile(dir / "term.log")) gone.emplace_back("term.log");
            break;
        case SI::Class::Knowledge:   gone.emplace_back("internal-isr.state"); break;
        case SI::Class::MetadataLog: gone.emplace_back("__cluster_metadata"); break;
    }
    std::string s;
    for (std::size_t i = 0; i < gone.size(); ++i) s += (i ? " and " : "") + gone[i];
    return s;
}

[[nodiscard]] uint8_t presentClasses(const fs::path& dir) {
    uint8_t m = 0;
    for (const auto& [k, n] : kClasses) if (classPresent(dir, k)) m |= bit(k);
    return m;
}

// ── "Empty" (§3.2): no broker-state entry ────────────────────────────────────────────────────────────────────
[[nodiscard]] bool isBrokerStateName(const std::string& n) {
    static constexpr std::array<std::string_view, 9> kNames{
        "node-state.dat", "term.log", "internal-isr.state", "process-incarnation", ".catalog-catchup",
        "topics", "quarantine", "dropped", "memory-epoch-fence"};
    for (auto k : kNames) if (n == k) return true;
    return n.rfind("__", 0) == 0;
}

struct Scan {
    bool                     brokerState = false;
    std::vector<std::string> foreign;   // ignored entries (lost+found, aeron/, dotfiles, ...)
};

Scan scanDir(const fs::path& dir) {
    Scan s;
    std::error_code ec;
    for (auto it = fs::directory_iterator(dir, ec); !ec && it != fs::directory_iterator(); it.increment(ec)) {
        const std::string n = it->path().filename().string();
        if (n == kMeta || n == kTmp) continue;
        if (isBrokerStateName(n)) s.brokerState = true;
        else s.foreign.push_back(n);
    }
    return s;
}

/// The Legacy rule: node-state.dat AND term.log.  Returns the missing ones ("" = the rule holds).
std::string legacyMissing(const fs::path& dir) {
    std::error_code ec;
    std::string miss;
    for (const char* f : {"node-state.dat", "term.log"})
        if (!fs::exists(dir / f, ec)) miss += (miss.empty() ? "" : ", ") + std::string(f);
    return miss;
}

// ── The refusal texts (verbatim, §3.2) ───────────────────────────────────────────────────────────────────────
std::string r1(const std::string& d) {
    return "data dir " + d + " is empty and not formatted (no storage.meta); nothing was written.\n"
           "Pick one and start again:\n"
           "  - a NEW cluster (each founding node, once):           rune_broker storage format --new --data-dir " + d + "\n"
           "  - a new or replaced disk in an EXISTING cluster:       rune_broker storage format --join --data-dir " + d + "\n"
           "  - a disposable development sandbox (may create an empty cluster on every wipe): RUNE_STORAGE_FORMAT=auto\n"
           "If " + d + " should already hold this node's data, the volume is not mounted: check RUNE_DATA_DIR and the mount.";
}
std::string r2(const std::string& d, const std::string& reason) {
    return d + "/storage.meta is unreadable (" + reason + "), so this dir's storage identity is lost.\n"
           "Restore storage.meta from a backup of this same dir, or wipe the whole dir and run\n"
           "rune_broker storage format --join --data-dir " + d + " to rejoin as a replaced disk.";
}
std::string r3(const std::string& d, const std::string& missing) {
    return d + " holds broker data but not node-state.dat and term.log (missing: " + missing + "): a partly deleted data dir\n"
           "would vote and lead with lost consensus state. Wipe the whole dir and run\n"
           "rune_broker storage format --join --data-dir " + d + "; the node re-replicates as a replaced disk.";
}
std::string r3b(const std::string& d, const char* cls, const std::string& files) {
    return d + " is damaged: storage.meta says " + cls + " existed, but " + files + " are gone. Wipe the whole dir (including\n"
           "storage.meta) and run rune_broker storage format --join --data-dir " + d + ".";
}
std::string r4(const std::string& d, int32_t n) {
    return d + " is formatted to JOIN a cluster (origin=joiner), but RUNE_PEERS lists only node " + std::to_string(n) + ": a single-node\n"
           "cluster has no other copy to rejoin from. Restore " + d + " from a backup, or, to start an EMPTY new cluster,\n"
           "wipe " + d + " and run rune_broker storage format --new --data-dir " + d + ".";
}
std::string r4b(const std::string& d, int32_t n) {
    return d + " was re-identified with --reassign-join (origin=joiner), but RUNE_PEERS lists only node " + std::to_string(n) + ": a single-node\n"
           "cluster has no other copy to rejoin from. On a single-node cluster start a restored dir as it is: restore it again\n"
           "from the backup and start it without --reassign-join.";
}

// ── CLI texts (§3.2 CLI table) ───────────────────────────────────────────────────────────────────────────────
std::string alreadyFormatted(const std::string& d, const SI::Loaded& l) {
    return "refusing: " + d + " is already formatted (storageId=" + SI::idHex(l.id) + ", origin=" + SI::originName(l.origin) +
           "); nothing changed. To replace this disk's identity, wipe the whole dir first.\n";
}
std::string legacyNotFormattable(const std::string& d) {
    return "refusing: " + d + " holds data written before storage ids existed; start the broker normally to adopt it "
           "(legacy). Formatting would give it a new identity.\n";
}

int64_t freshIdOtherThan(int64_t old) {
    for (;;) {
        const int64_t id = SI::freshId();
        if (id != old) return id;
    }
}

} // namespace

// ── public ───────────────────────────────────────────────────────────────────────────────────────────────────

const char* SI::originName(Origin o) noexcept {
    switch (o) {
        case Origin::Undecided: return "undecided";
        case Origin::Founder:   return "founder";
        case Origin::Joiner:    return "joiner";
        case Origin::Legacy:    return "legacy";
    }
    return "?";
}

std::string SI::idHex(int64_t id) { return hexOf(static_cast<uint64_t>(id), 16); }

int64_t SI::freshId() {
    std::random_device rd;
    for (;;) {
        const uint64_t v = (static_cast<uint64_t>(rd()) << 32) ^ static_cast<uint64_t>(rd());
        if (v != 0) return static_cast<int64_t>(v);
    }
}

void SI::write_(const std::string& dirStr, const Loaded& l) {
    const fs::path dir{dirStr};
    const fs::path tmp = dir / kTmp, fin = dir / kMeta;
    const std::string text = serialize(l);
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) ioFail("open", tmp, errno);
    std::size_t off = 0;
    while (off < text.size()) {
        const ssize_t n = ::write(fd, text.data() + off, text.size() - off);
        if (n < 0) {
            if (errno == EINTR) continue;
            const int e = errno;
            ::close(fd);
            ::unlink(tmp.c_str());
            ioFail("write", tmp, e);
        }
        off += static_cast<std::size_t>(n);
    }
    if (::fsync(fd) != 0) { const int e = errno; ::close(fd); ::unlink(tmp.c_str()); ioFail("fsync", tmp, e); }
    if (::close(fd) != 0) { const int e = errno; ::unlink(tmp.c_str()); ioFail("close", tmp, e); }
    if (::rename(tmp.c_str(), fin.c_str()) != 0) { const int e = errno; ::unlink(tmp.c_str()); ioFail("rename", fin, e); }
    fsyncDir(dir);
}

SI::Loaded SI::resolve(const std::string& d, StorageFormat fmt, bool singleNode, int32_t nodeId) {
    const fs::path dir{d};
    // 1. A leftover tmp is a crash before the rename: discard it (the next step formats again).
    {
        std::error_code ec;
        fs::remove(dir / kTmp, ec);
    }
    // 2. Parse.
    Parsed parsed = parseFile(dir);
    if (const auto* c = std::get_if<Corrupt>(&parsed)) throw StorageRefusal(r2(d, c->reason));

    const bool autoFmt = fmt == StorageFormat::Auto;
    if (auto* lp = std::get_if<Loaded>(&parsed)) {
        Loaded l = *lp;
        // 3. Single-node fix-ups.
        if (singleNode) {
            if (l.origin == Origin::Joiner) throw StorageRefusal(has(l.manifest, Class::Consensus) ? r4b(d, nodeId) : r4(d, nodeId));
            if (l.origin == Origin::Undecided) {
                Loaded f = l;
                f.origin = Origin::Founder;
                f.mode = Mode::None;
                write_(d, f);
                l = f;
                spdlog::info("storage: {} was undecided on a single-node cluster: persisted origin=founder", d);
            }
        }
        // 6. The manifest check.
        for (const auto& [k, n] : kClasses)
            if (has(l.manifest, k) && !classPresent(dir, k)) throw StorageRefusal(r3b(d, n, missingFilesOf(dir, k)));
        // CP1 N1 (finishes M3): a Joiner whose manifest never recorded `knowledge` must not load an internal-isr.state
        // that exists anyway - it is a restored file left by a crash between --reassign-join's storage.meta write and
        // its unlink.  Delete it before BrokerNode opens it, so it can neither be loaded nor voted.
        if (l.origin == Origin::Joiner && !has(l.manifest, Class::Knowledge) && fs::exists(dir / "internal-isr.state")) {
            unlinkDurably(dir, "internal-isr.state");
            spdlog::warn("storage: {} - removed a stale internal-isr.state (a Joiner's knowledge starts empty)", d);
        }
        // The env is only read when formatting (R-W1 / R-W2).
        if (fmt == StorageFormat::New) {
            spdlog::info("RUNE_STORAGE_FORMAT ignored: already formatted");
        }
        if (fmt == StorageFormat::New || fmt == StorageFormat::Join) {
            std::string w = std::string("RUNE_STORAGE_FORMAT=") + storageFormatName(fmt) +
                            " is only read when formatting an empty data dir. Remove it from this node's environment,\n"
                            "or a later disk loss on this node is formatted " + storageFormatName(fmt) + " without asking.";
            if (fmt == StorageFormat::Join && !singleNode) w += " (keeping RUNE_STORAGE_FORMAT=join set is safe: a replaced disk then rejoins)";
            spdlog::warn("{}", w);
        }
        if (autoFmt) spdlog::warn("RUNE_STORAGE_FORMAT=auto (development): an empty data dir is formatted automatically; never use it in production.");
        return l;
    }

    // No storage.meta.  4. The broker-state scan.
    const Scan scan = scanDir(dir);
    if (!scan.foreign.empty()) {
        std::string names;
        for (const auto& n : scan.foreign) names += (names.empty() ? "" : ", ") + n;
        spdlog::info("storage: {} — ignored foreign entries (not broker state): {}", d, names);
    }
    if (scan.brokerState) {
        // 5. The Legacy rule.
        if (const std::string miss = legacyMissing(dir); !miss.empty()) throw StorageRefusal(r3(d, miss));
        Loaded l;
        l.id = freshId();
        l.origin = Origin::Legacy;
        l.manifest = presentClasses(dir);
        // 6. The manifest is the classes that exist, so the check holds by construction.
        write_(d, l);
        spdlog::info("storage: {} holds data written before storage ids: adopted as legacy (storageId={} manifest={})",
                     d, idHex(l.id), manifestString(l.manifest));
        if (autoFmt) spdlog::warn("RUNE_STORAGE_FORMAT=auto (development): an empty data dir is formatted automatically; never use it in production.");
        return l;
    }

    // 7. Format an empty dir (refusals write nothing).
    Loaded l;
    switch (fmt) {
        case StorageFormat::Required:
            throw StorageRefusal(r1(d));
        case StorageFormat::New:
            if (singleNode) l.origin = Origin::Founder;
            else { l.origin = Origin::Undecided; l.mode = Mode::New; }
            break;
        case StorageFormat::Join:
            if (singleNode) throw StorageRefusal(r4(d, nodeId));
            l.origin = Origin::Joiner;
            break;
        case StorageFormat::Auto:
            if (singleNode) l.origin = Origin::Founder;
            else { l.origin = Origin::Undecided; l.mode = Mode::Auto; }
            break;
        case StorageFormat::TestFounder:
            l.origin = Origin::Founder;
            break;
    }
    l.id = freshId();
    write_(d, l);
    spdlog::info("storage: formatted {} ({}): storageId={} origin={}{}", d,
                 fmt == StorageFormat::TestFounder ? "in-process rig" : std::string("RUNE_STORAGE_FORMAT=") + storageFormatName(fmt),
                 idHex(l.id), originName(l.origin),
                 l.mode == Mode::New ? " mode=new" : l.mode == Mode::Auto ? " mode=auto" : "");
    if (autoFmt) spdlog::warn("RUNE_STORAGE_FORMAT=auto (development): an empty data dir is formatted automatically; never use it in production.");
    return l;
}

SI::FormatResult SI::format(const std::string& d, Origin o, Mode m, bool ignoreFormatted) {
    const fs::path dir{d};
    try {
        {
            std::error_code ec;
            fs::remove(dir / kTmp, ec);
        }
        Parsed parsed = parseFile(dir);
        if (const auto* c = std::get_if<Corrupt>(&parsed)) return {1, {}, "refusing: " + r2(d, c->reason) + "\n"};
        if (const auto* lp = std::get_if<Loaded>(&parsed)) {
            if (lp->origin == Origin::Undecided && o == Origin::Joiner) return reformatJoin(d);
            const std::string text = alreadyFormatted(d, *lp);
            if (ignoreFormatted) return {0, text, {}};
            return {1, {}, text};
        }
        if (scanDir(dir).brokerState) return {1, {}, legacyNotFormattable(d)};
        Loaded l;
        l.id = freshId();
        l.origin = o;
        l.mode = o == Origin::Undecided ? (m == Mode::None ? Mode::New : m) : Mode::None;
        write_(d, l);
        std::string out = "formatted " + d + ": storageId=" + idHex(l.id) + " origin=" + originName(l.origin);
        if (l.origin == Origin::Undecided)
            out += l.mode == Mode::Auto ? " (development; founds once a majority of the configured members has started)"
                                        : " (new cluster; founds once a majority of the configured members has started)";
        else if (l.origin == Origin::Joiner)
            out += " (joins the running cluster as a new or replaced disk)";
        return {0, out + "\n", {}};
    } catch (const StorageIoError& e) {
        return {74, {}, std::string("storage format failed: ") + e.what() + "\n"};
    }
}

SI::FormatResult SI::reformatJoin(const std::string& d) {
    try {
        Parsed parsed = parseFile(fs::path{d});
        auto* lp = std::get_if<Loaded>(&parsed);
        if (lp == nullptr) return {1, {}, "refusing: " + d + " is not formatted as undecided; nothing changed.\n"};
        if (lp->origin != Origin::Undecided) return {1, {}, alreadyFormatted(d, *lp)};
        Loaded l = *lp;
        l.origin = Origin::Joiner;
        l.mode = Mode::None;
        write_(d, l);
        return {0, "re-formatted " + d + " as joiner (it never voted, stood or led)\n", {}};
    } catch (const StorageIoError& e) {
        return {74, {}, std::string("storage format failed: ") + e.what() + "\n"};
    }
}

SI::FormatResult SI::reassignJoin(const std::string& d) {
    const fs::path dir{d};
    try {
        {
            std::error_code ec;
            fs::remove(dir / kTmp, ec);
        }
        Parsed parsed = parseFile(dir);
        if (const auto* c = std::get_if<Corrupt>(&parsed)) return {1, {}, "refusing: " + r2(d, c->reason) + "\n"};
        Loaded l;
        bool preUpgrade = false;
        if (const auto* lp = std::get_if<Loaded>(&parsed)) {
            l.id = freshIdOtherThan(lp->id);
            l.manifest = lp->manifest;
        } else {
            if (!scanDir(dir).brokerState)
                return {1, {}, "refusing: " + d + " is not formatted; --reassign-join re-identifies a restored data dir. "
                                                  "For an empty dir use --join.\n"};
            if (const std::string miss = legacyMissing(dir); !miss.empty()) return {1, {}, r3(d, miss) + "\n"};
            l.id = freshId();
            l.manifest = presentClasses(dir);
            preUpgrade = true;
        }
        l.origin = Origin::Joiner;
        l.mode = Mode::None;
        l.manifest = static_cast<uint8_t>(l.manifest & ~bit(Class::Knowledge));
        // The stale knowledge goes FIRST: a crash between the two steps leaves the old storage.meta with its
        // knowledge bit and no file (R-3b at boot; re-run the command), never a new identity voting on it.
        // Pre-upgrade dir (no storage.meta yet): the NEW identity is written FIRST (CP1 M3).  Unlinking first left a
        // window with no storage.meta, in which the next boot adopts the dir as Legacy with a fresh id and the
        // restored knowledge file still present.  A formatted dir keeps its storage.meta throughout, so there the
        // knowledge goes first.
        if (preUpgrade) {
            write_(d, l);
            unlinkDurably(dir, "internal-isr.state");
        } else {
            unlinkDurably(dir, "internal-isr.state");
            write_(d, l);
        }
        if (preUpgrade)
            return {0, "re-identified " + d + " (pre-storage-id data kept): storageId=" + idHex(l.id) +
                           " origin=joiner (it rejoins as a replaced disk)\n", {}};
        return {0, "re-identified " + d + ": storageId=" + idHex(l.id) +
                       " origin=joiner (restored data kept; it rejoins as a replaced disk)\n", {}};
    } catch (const StorageIoError& e) {
        return {74, {}, std::string("storage format failed: ") + e.what() + "\n"};
    }
}

bool SI::persistOrigin(const std::string& d, Loaded& l, Origin to) {
    if (l.origin != Origin::Undecided || (to != Origin::Founder && to != Origin::Joiner))
        throw std::logic_error(std::string("StorageIdentity::persistOrigin: illegal transition ") + originName(l.origin) +
                               " -> " + originName(to));
    Loaded n = l;
    n.origin = to;
    n.mode = Mode::None;
    try {
        write_(d, n);
    } catch (const StorageIoError& e) {
        spdlog::error("storage: persisting origin={} failed: {}", originName(to), e.what());
        return false;
    }
    l = n;
    return true;
}

bool SI::markPresent(const std::string& d, Loaded& l, Class c) {
    if (has(l.manifest, c)) return true;
    Loaded n = l;
    n.manifest = static_cast<uint8_t>(n.manifest | bit(c));
    try {
        write_(d, n);
    } catch (const StorageIoError& e) {
        spdlog::error("storage: marking {} present failed: {}", className(c), e.what());
        return false;
    }
    l = n;
    return true;
}

bool SI::clearClassForTest(const std::string& d, Class c) {
    Parsed parsed = parseFile(fs::path{d});
    auto* lp = std::get_if<Loaded>(&parsed);
    if (lp == nullptr) return false;
    Loaded n = *lp;
    n.manifest = static_cast<uint8_t>(n.manifest & ~bit(c));
    try {
        write_(d, n);
    } catch (const StorageIoError&) {
        return false;
    }
    return true;
}

// ── CLI ──────────────────────────────────────────────────────────────────────────────────────────────────────

int runStorageFormatCli(int argc, char** argv) {
    static constexpr const char* kUsage =
        "usage: rune_broker storage format (--new|--join|--reassign-join) [--data-dir D] [--ignore-formatted]\n";
    enum class Cmd : uint8_t { None, New, Join, Reassign };
    Cmd cmd = Cmd::None;
    std::string dir;
    bool ignoreFormatted = false;
    for (int i = 0; i < argc; ++i) {
        const std::string_view a(argv[i]);
        Cmd next = Cmd::None;
        if (a == "--new") next = Cmd::New;
        else if (a == "--join") next = Cmd::Join;
        else if (a == "--reassign-join") next = Cmd::Reassign;
        else if (a == "--ignore-formatted") { ignoreFormatted = true; continue; }
        else if (a == "--data-dir" && i + 1 < argc && argv[i + 1][0] != '\0') { dir = argv[++i]; continue; }
        else { std::cerr << kUsage; return 64; }
        if (cmd != Cmd::None) { std::cerr << kUsage; return 64; }   // exactly one mode
        cmd = next;
    }
    if (cmd == Cmd::None) { std::cerr << kUsage; return 64; }
    if (dir.empty()) {
        if (const char* e = std::getenv("RUNE_DATA_DIR"); e && *e) dir = e;
    }
    if (dir.empty()) {
        std::cerr << "storage format: no data dir (pass --data-dir D or set RUNE_DATA_DIR)\n" << kUsage;
        return 64;
    }
    std::error_code ec;
    fs::create_directories(dir, ec);
    if (ec) {
        std::cerr << "storage format failed: mkdir " << dir << ": " << ec.message() << "\n";
        return 74;
    }
    StorageIdentity::FormatResult r;
    switch (cmd) {
        case Cmd::New:      r = StorageIdentity::format(dir, SI::Origin::Undecided, SI::Mode::New, ignoreFormatted); break;
        case Cmd::Join:     r = StorageIdentity::format(dir, SI::Origin::Joiner, SI::Mode::None, ignoreFormatted); break;
        case Cmd::Reassign: r = StorageIdentity::reassignJoin(dir); break;
        case Cmd::None:     break;
    }
    if (!r.out.empty()) std::cout << r.out << std::flush;
    if (!r.err.empty()) std::cerr << r.err << std::flush;
    return r.code;
}

} // namespace rune::node
