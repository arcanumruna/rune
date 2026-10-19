// src/cli/SecurityCommands.cpp — see rune/cli/SecurityCommands.h.
#include "rune/cli/SecurityCommands.h"

#include "CliSupport.h"
#include "rune/cli/CatalogCommands.h"

#include <rune/client/detail/SecureZero.h>
#include <rune/crypto/Kdf.h>
#include <rune/security/SecurityEvent.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

#ifdef RUNE_CLI_LIVE_ENABLED
#include <rune/client/Client.h>
#endif

namespace rune::cli {

namespace sec = rune::security;
using namespace rune::cli::support;

// ═══════════════════════════════════════════════════════════════════════════
// Pure helpers
// ═══════════════════════════════════════════════════════════════════════════
std::string generateOneTimePassword() {
    static constexpr char kAlphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    static_assert(sizeof(kAlphabet) - 1 == 62);
    constexpr std::size_t kLength = 24;
    std::string pw;
    pw.reserve(kLength);
    std::array<uint8_t, 64> buf{};
    while (pw.size() < kLength) {
        if (!rune::crypto::randomBytes(buf)) {
            rune::client::detail::secureZero(pw.data(), pw.size());
            return {};
        }
        for (const uint8_t b : buf) {
            if (b >= 248) continue;   // 248 = 4 * 62: reject the tail, so every character is equally likely
            pw.push_back(kAlphabet[b % 62]);
            if (pw.size() == kLength) break;
        }
    }
    rune::client::detail::secureZero(buf.data(), buf.size());
    return pw;
}

bool writeClusterKeyFile(const std::string& path, std::string& why) {
    std::array<uint8_t, 32> key{};
    if (!rune::crypto::randomBytes(key)) {
        why = "no randomness available";
        return false;
    }
    static constexpr char kHex[] = "0123456789abcdef";
    std::string hex;
    hex.reserve(64);
    for (const uint8_t b : key) {
        hex.push_back(kHex[b >> 4]);
        hex.push_back(kHex[b & 15]);
    }
    rune::client::detail::secureZero(key.data(), key.size());
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0) {
        why = std::string("cannot create '") + path + "': " + std::strerror(errno) +
              (errno == EEXIST ? " (an existing key file is never overwritten)" : "");
        rune::client::detail::secureZero(hex.data(), hex.size());
        return false;
    }
    (void)::fchmod(fd, 0600);   // whatever the umask said
    std::size_t off = 0;
    bool ok = true;
    while (off < hex.size()) {
        const ssize_t n = ::write(fd, hex.data() + off, hex.size() - off);
        if (n <= 0) { ok = false; break; }
        off += static_cast<std::size_t>(n);
    }
    ::close(fd);
    rune::client::detail::secureZero(hex.data(), hex.size());
    if (!ok) {
        why = std::string("cannot write '") + path + "'";
        ::unlink(path.c_str());
        return false;
    }
    return true;
}

namespace {

std::string alignTable(const std::vector<std::vector<std::string>>& rows) {
    if (rows.empty()) return {};
    std::vector<std::size_t> width(rows[0].size(), 0);
    for (const auto& r : rows)
        for (std::size_t c = 0; c < r.size(); ++c) width[c] = std::max(width[c], r[c].size());
    std::string out;
    for (const auto& r : rows) {
        for (std::size_t c = 0; c < r.size(); ++c) {
            out += r[c];
            if (c + 1 != r.size()) out.append(width[c] - r[c].size() + 2, ' ');
        }
        out += "\n";
    }
    return out;
}

std::string orDash(std::string s) { return s.empty() ? std::string("-") : s; }

std::string oneLine(std::string s) {
    for (char& c : s)
        if (c == '\n' || c == '\r' || c == '\t') c = ' ';
    return s;
}

std::string join(const std::vector<std::string>& v) {
    std::string s;
    for (std::size_t i = 0; i < v.size(); ++i) s += (i == 0 ? "" : ",") + v[i];
    return orDash(std::move(s));
}

std::string patternText(const sec::ResourcePattern& p) {
    if (p.type == sec::ResourceType::Cluster) return "-";
    return (p.kind == sec::PatternKind::Prefix ? "PREFIX '" : "'") + p.name + "'";
}

void addGrantRows(std::vector<std::vector<std::string>>& rows, const std::string& source,
                  const std::vector<sec::GrantSpec>& specs) {
    for (const auto& g : specs) {
        rows.push_back({source, std::string(sec::operationName(g.op)), std::string(sec::resourceTypeName(g.resource.type)),
                        patternText(g.resource)});
    }
}

std::string utcMs(int64_t ms) {
    const std::time_t t = static_cast<std::time_t>(ms / 1000);
    std::tm tm{};
    ::gmtime_r(&t, &tm);
    char b[40];
    std::snprintf(b, sizeof b, "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                  tm.tm_hour, tm.tm_min, tm.tm_sec, static_cast<int>(((ms % 1000) + 1000) % 1000));
    return b;
}

} // namespace

std::string formatUsersTable(const std::vector<sec::UserRow>& users) {
    std::vector<std::vector<std::string>> rows{{"name", "enabled", "must_change", "passwords", "roles", "version"}};
    for (const auto& u : users) {
        rows.push_back({u.name, u.enabled ? "true" : "false", u.mustChangePassword ? "true" : "false",
                        std::to_string(u.passwords), join(u.roles), std::to_string(u.version)});
    }
    return alignTable(rows);
}

std::string formatRolesTable(const std::vector<sec::RoleRow>& roles) {
    std::vector<std::vector<std::string>> rows{{"name", "members", "grants", "version"}};
    for (const auto& r : roles) rows.push_back({r.name, join(r.members), std::to_string(r.grants), std::to_string(r.version)});
    return alignTable(rows);
}

std::string formatGrantsTable(const sec::GrantsView& g) {
    std::vector<std::vector<std::string>> rows{{"source", "operation", "resource", "pattern"}};
    addGrantRows(rows, "direct", g.direct);
    for (const auto& [role, specs] : g.viaRoles) addGrantRows(rows, "role:" + role, specs);
    return alignTable(rows);
}

std::string formatSecurityHistoryTable(const std::vector<sec::RecordedEvent>& events) {
    std::vector<std::vector<std::string>> rows{{"offset", "time(UTC)", "kind", "object", "version", "principal", "statement"}};
    for (const auto& r : events) {
        const sec::SecurityEvent& e = r.event;
        rows.push_back({std::to_string(r.offset), utcMs(e.timestampMs), std::string(sec::kindName(e.kind)),
                        orDash(e.objectName) + "(" + std::to_string(e.objectId) + ")",
                        std::to_string(e.prevVersion) + "\xE2\x86\x92" + std::to_string(e.newVersion),
                        orDash(e.principal.authenticated), orDash(oneLine(e.statement))});
    }
    return alignTable(rows);
}

void printSecurityUsage(std::ostream& err) {
    err << "  rune security bootstrap --admin <name> --cluster-key-file <path> --broker host:port\n"
        << "          [--password-file <path>]\n"
        << "      Creates the first security admin on a fresh cluster (ADMIN ON CLUSTER).  Without --password-file a\n"
        << "      24-character one-time password is generated and printed once; the admin must change it on first login.\n"
        << "      --cluster-key-file defaults to RUNE_CLUSTER_KEY_FILE (the bootstrap proves possession of the key).\n"
        << "  rune security gen-cluster-key --out <path>\n"
        << "      Writes a new cluster key (64 hex characters, mode 0600; an existing file is never overwritten).\n"
        << "  Credentials for every command that connects: --user <u> (or RUNE_USER) and --password-file <path>\n"
        << "      (or RUNE_PASSWORD_FILE).  A password is never accepted on the command line or from the environment.\n";
}

// ═══════════════════════════════════════════════════════════════════════════
// rune security
// ═══════════════════════════════════════════════════════════════════════════
namespace {

int securityUsageError(std::string_view cmd, std::ostream& err, const std::string& msg) {
    err << "rune security " << cmd << ": " << msg << "\n";
    printSecurityUsage(err);
    return 2;
}

int cmdGenClusterKey(const std::vector<std::string>& args, std::ostream& out, std::ostream& err) {
    ParsedArgs a;
    if (!parseArgs("security gen-cluster-key", args, {"--out"}, {}, a, err)) {
        printSecurityUsage(err);
        return 2;
    }
    if (!a.has("--out")) return securityUsageError("gen-cluster-key", err, "--out <path> is required");
    std::string why;
    if (!writeClusterKeyFile(a.value("--out"), why)) {
        err << "rune: " << why << "\n";
        return 1;
    }
    out << "wrote cluster key " << a.value("--out") << " (64 hex characters, mode 0600)\n";
    return 0;
}

bool readFileBytes(const std::string& path, std::string& bytes, std::ostream& err) {
    std::ifstream f(path, std::ios::binary);
    if (!f) {
        err << "rune: cannot read '" << path << "'\n";
        return false;
    }
    std::ostringstream buf;
    buf << f.rdbuf();
    bytes = buf.str();
    return true;
}

int cmdBootstrap(const std::vector<std::string>& args, std::ostream& out, std::ostream& err) {
    ParsedArgs a;
    if (!parseArgs("security bootstrap", args, {"--admin", "--cluster-key-file", "--broker"}, {}, a, err)) {
        printSecurityUsage(err);
        return 2;
    }
    if (!a.has("--admin") || a.value("--admin").empty()) return securityUsageError("bootstrap", err, "--admin <name> is required");
    std::string keyPath = a.value("--cluster-key-file");
    if (keyPath.empty()) {
        if (const char* env = std::getenv("RUNE_CLUSTER_KEY_FILE"); env != nullptr) keyPath = env;
    }
    if (keyPath.empty())
        return securityUsageError("bootstrap", err,
                                  "--cluster-key-file <path> is required (or RUNE_CLUSTER_KEY_FILE): the bootstrap "
                                  "proves possession of the cluster key");
    const std::string admin = a.value("--admin");
    std::string endpoint = a.value("--broker");
    if (endpoint.empty()) {
        if (const char* env = std::getenv("RUNE_BROKER"); env != nullptr) endpoint = env;
    }
    if (endpoint.empty()) return securityUsageError("bootstrap", err, "--broker host:port is required");

    // The cluster key and the one-time password.  Both are wiped before returning.
    std::string key;
    if (!readFileBytes(keyPath, key, err)) return 1;
    std::string password;
    const bool fromFile = a.has("--password-file");
    if (fromFile) {
        if (!readFileBytes(a.value("--password-file"), password, err)) {
            rune::client::detail::secureZero(key.data(), key.size());
            return 1;
        }
        while (!password.empty() && (password.back() == '\n' || password.back() == '\r')) password.pop_back();
    } else {
        password = generateOneTimePassword();
        if (password.empty()) {
            err << "rune: no randomness available to generate the one-time password\n";
            rune::client::detail::secureZero(key.data(), key.size());
            return 1;
        }
    }
    auto wipe = [&] {
        rune::client::detail::secureZero(key.data(), key.size());
        rune::client::detail::secureZero(password.data(), password.size());
    };

#ifdef RUNE_CLI_LIVE_ENABLED
    // The bootstrap proves itself with the cluster-key MAC, not a login: this connection presents no credentials.
    LiveBroker broker;
    if (!broker.open(a, endpoint, false, err, /*withCredentials=*/false)) { wipe(); return 1; }
    rune::client::SecurityDdlResult r;
    try {
        r = broker.client().bootstrapSecurityAndWait(
            admin, password, std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(key.data()), key.size()),
            std::chrono::seconds(15));
    } catch (const std::exception& e) {
        err << "rune: " << e.what() << "\n";
        wipe();
        return 1;
    }
    if (r.errorCode != 0) {
        err << "rune: bootstrap refused: code " << r.errorCode << (r.detail.empty() ? "" : " " + r.detail) << "\n";
        wipe();
        return 1;
    }
    if (fromFile) out << "bootstrapped " << admin << " (password from " << a.value("--password-file") << ")\n";
    else out << "one-time password for " << admin << ": " << password << "\n";
    wipe();
    return 0;
#else
    (void)out;
    err << "rune security bootstrap: live mode requires RUNE_CLI_LIVE_ENABLED and a real Aeron build.\n";
    wipe();
    return 1;
#endif
}

} // namespace

int cmdSecurity(const std::vector<std::string>& args, std::ostream& out, std::ostream& err) {
    if (args.empty()) {
        err << "rune security: a subcommand is required\n";
        printSecurityUsage(err);
        return 2;
    }
    const std::vector<std::string> rest(args.begin() + 1, args.end());
    if (args[0] == "gen-cluster-key") return cmdGenClusterKey(rest, out, err);
    if (args[0] == "bootstrap") return cmdBootstrap(rest, out, err);
    err << "rune security: unknown subcommand '" << args[0] << "'\n";
    printSecurityUsage(err);
    return 2;
}

} // namespace rune::cli
