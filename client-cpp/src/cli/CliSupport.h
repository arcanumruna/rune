// src/cli/CliSupport.h — argument parsing and the live broker connection, shared by `rune ql` / `rune catalog`
// (CatalogCommands.cpp) and `rune security` (SecurityCommands.cpp).  Internal to the CLI.
#pragma once

#include "rune/cli/CatalogCommands.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#ifdef RUNE_CLI_LIVE_ENABLED
#include <rune/client/Client.h>
#include <rune/protocol/client/Protocol.h>

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace rune::cli::support {

// ═══════════════════════════════════════════════════════════════════════════
// Argument parsing (strict: an unknown option, a missing value or a stray word is a usage error)
// ═══════════════════════════════════════════════════════════════════════════
struct ParsedArgs {
    std::map<std::string, std::string> values;
    std::set<std::string> flags;
    [[nodiscard]] bool has(const std::string& k) const { return values.count(k) != 0 || flags.count(k) != 0; }
    [[nodiscard]] std::string value(const std::string& k) const {
        const auto it = values.find(k);
        return it == values.end() ? std::string() : it->second;
    }
};

// The test-only overrides the schema commands already accept (not advertised).
inline constexpr std::string_view kHiddenValueFlags[] = {"--cli-stream-in", "--cli-stream-out", "--cli-data-dir",
                                                         "--cli-egress", "--user", "--password-file"};   // the global flags too (Task 8)

inline bool parseArgs(std::string_view cmd, const std::vector<std::string>& args, std::initializer_list<std::string_view> valueFlags,
               std::initializer_list<std::string_view> boolFlags, ParsedArgs& out, std::ostream& err) {
    auto isIn = [](std::string_view a, auto&& list) { return std::find(std::begin(list), std::end(list), a) != std::end(list); };
    if (std::getenv("RUNE_PASSWORD") != nullptr) {
        err << "rune: warning: RUNE_PASSWORD is ignored: a password is never read from the environment "
               "(use --password-file <path> or RUNE_PASSWORD_FILE)\n";
    }
    for (std::size_t i = 0; i < args.size(); ++i) {
        const std::string& a = args[i];
        if (a == "--password" || a.rfind("--password=", 0) == 0) {
            // Authentication (D9/§7): a password is never taken from argv (it would sit in `ps` and shell history).
            err << "rune " << cmd << ": --password is not accepted: a password is never given on the command line "
                   "(use --password-file <path> or RUNE_PASSWORD_FILE)\n";
            return false;
        }
        if (isIn(a, valueFlags) || isIn(a, kHiddenValueFlags)) {
            if (i + 1 >= args.size()) {
                err << "rune " << cmd << ": " << a << " needs a value\n";
                return false;
            }
            if (out.has(a)) {
                err << "rune " << cmd << ": " << a << " given twice\n";
                return false;
            }
            out.values[a] = args[++i];
        } else if (isIn(a, boolFlags)) {
            if (out.has(a)) {
                err << "rune " << cmd << ": " << a << " given twice\n";
                return false;
            }
            out.flags.insert(a);
        } else {
            err << "rune " << cmd << ": " << (a.rfind('-', 0) == 0 ? "unknown option '" : "unexpected argument '") << a
                << "'\n";
            return false;
        }
    }
    return true;
}

/// A non-negative int64 ("0".."9223372036854775807"), digits only.
inline std::optional<int64_t> parseOffset(std::string_view s) noexcept {
    if (s.empty()) return std::nullopt;
    int64_t v = 0;
    const auto r = std::from_chars(s.data(), s.data() + s.size(), v);
    if (r.ec != std::errc{} || r.ptr != s.data() + s.size() || v < 0) return std::nullopt;
    return v;
}

inline int usageError(std::string_view cmd, std::ostream& err, const std::string& msg) {
    err << "rune " << cmd << ": " << msg << "\n";
    printCatalogUsage(err);
    return 2;
}


// ═══════════════════════════════════════════════════════════════════════════
// Live broker access
// ═══════════════════════════════════════════════════════════════════════════
#ifdef RUNE_CLI_LIVE_ENABLED

namespace rc  = rune::client;
namespace rpc = rune::protocol::client;

inline constexpr std::chrono::seconds kConnectWait{10};
/// Bound on every catalog read (the client's reads are unbounded without one).
inline constexpr std::chrono::seconds kCatalogWait{15};

inline std::string toChannel(const std::string& endpoint) {
    if (endpoint.rfind("aeron:", 0) == 0) return endpoint;
    return "aeron:udp?endpoint=" + endpoint;
}

/// "host:port" of an `aeron:udp?endpoint=host:port[&...]` channel; "" for any other channel.
inline std::string udpEndpointOf(const std::string& channel) {
    const std::string key = "aeron:udp?endpoint=";
    if (channel.rfind(key, 0) != 0) return {};
    const auto end = channel.find('&', key.size());
    return channel.substr(key.size(), end == std::string::npos ? std::string::npos : end - key.size());
}

/// The local address the OS would use to reach `hostport` (UDP connect + getsockname — no packet is sent),
/// as an Aeron egress channel; nullopt (with `why`) when it cannot be determined.
inline std::optional<std::string> localEgressFor(const std::string& hostport, std::string& why) {
    const auto colon = hostport.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 == hostport.size()) {
        why = "'" + hostport + "' is not host:port";
        return std::nullopt;
    }
    const std::string host = hostport.substr(0, colon), port = hostport.substr(colon + 1);
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    addrinfo* res = nullptr;
    if (const int rc = ::getaddrinfo(host.c_str(), port.c_str(), &hints, &res); rc != 0 || res == nullptr) {
        why = "cannot resolve '" + host + "'";
        return std::nullopt;
    }
    std::optional<std::string> result;
    const int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd >= 0) {
        if (::connect(fd, res->ai_addr, res->ai_addrlen) == 0) {
            sockaddr_in local{};
            socklen_t len = sizeof local;
            if (::getsockname(fd, reinterpret_cast<sockaddr*>(&local), &len) == 0) {
                char ip[INET_ADDRSTRLEN] = {};
                if (::inet_ntop(AF_INET, &local.sin_addr, ip, sizeof ip) != nullptr)
                    result = std::string("aeron:udp?endpoint=") + ip + ":" + std::to_string(ntohs(local.sin_port));
            }
        }
        ::close(fd);
    }
    ::freeaddrinfo(res);
    if (!result) why = "cannot determine a local address that reaches '" + hostport + "'";
    return result;
}

/// A per-client scratch directory (the embedded driver's data dir), removed on destruction.
class ScopedDir {
public:
    ScopedDir() = default;
    explicit ScopedDir(std::filesystem::path p) : path_(std::move(p)) {}
    ScopedDir(ScopedDir&& o) noexcept : path_(std::move(o.path_)) { o.path_.clear(); }
    ScopedDir& operator=(ScopedDir&& o) noexcept {
        reset();
        path_ = std::move(o.path_);
        o.path_.clear();
        return *this;
    }
    ~ScopedDir() { reset(); }
    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    void reset() noexcept {
        if (path_.empty()) return;
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
        path_.clear();
    }
    std::filesystem::path path_;
};

/// One connected Client for one broker.  Invoker mode: this thread is the duty thread, so the
/// DDL retry notes (--verbose) are read race-free.
inline std::string envOr(const char* name) {
    const char* v = std::getenv(name);
    return v != nullptr ? std::string(v) : std::string();
}

/// Review B I6: the credential flags are GLOBAL — every command that connects resolves them the same way:
/// `--user` (else RUNE_USER) and `--password-file` (else RUNE_PASSWORD_FILE).  No password ever comes from argv or
/// the environment.
inline void applyCredentials(rc::ClientConfig& cfg, const std::string& userFlag, const std::string& passwordFileFlag) {
    cfg.username     = !userFlag.empty() ? userFlag : envOr("RUNE_USER");
    cfg.passwordFile = !passwordFileFlag.empty() ? passwordFileFlag : envOr("RUNE_PASSWORD_FILE");
}

/// Review B I6: the dev-mode banner, after any CLI connect (stderr, so it never mixes with a command's output).
inline void printDevBannerIfAny(const rc::Client& client, std::ostream& err) {
    if (client.authStatus().brokerMode == rpc::AuthMode::DevDisabled) {
        err << "WARNING: this broker runs with authentication DISABLED (RUNE_AUTH=disabled, dev mode)\n";
    }
}

/// Review B I6: `--password` / `--password=...` anywhere in `args` is refused (usage error text on `err`); a set
/// RUNE_PASSWORD is ignored with a warning.  true = the arguments are acceptable.
inline bool refusePasswordArgs(std::string_view cmd, const std::vector<std::string>& args, std::ostream& err) {
    if (std::getenv("RUNE_PASSWORD") != nullptr) {
        err << "rune: warning: RUNE_PASSWORD is ignored: a password is never read from the environment "
               "(use --password-file <path> or RUNE_PASSWORD_FILE)\n";
    }
    for (const std::string& a : args) {
        if (a == "--password" || a.rfind("--password=", 0) == 0) {
            err << "rune " << cmd << ": --password is not accepted: a password is never given on the command line "
                   "(use --password-file <path> or RUNE_PASSWORD_FILE)\n";
            return false;
        }
    }
    return true;
}

class LiveBroker {
public:
    LiveBroker() = default;
    LiveBroker(const LiveBroker&) = delete;
    LiveBroker& operator=(const LiveBroker&) = delete;
    ~LiveBroker() { close(); }

    /// false (a message on err) when the endpoint is unusable or the connect fails / times out.
    bool open(const ParsedArgs& a, const std::string& endpoint, bool verbose, std::ostream& err,
              bool withCredentials = true, bool quietFailure = false) {
        rc::ClientConfig cfg;
        cfg.gatewayChannel = toChannel(endpoint);
        cfg.clientName     = "rune-cli";
        cfg.runnerMode     = rc::RunnerMode::Invoker;
        cfg.traceDdlRetries = verbose;
        std::string egress = a.value("--cli-egress");
        if (egress.empty()) {
            if (const char* env = std::getenv("RUNE_EGRESS"); env != nullptr) egress = env;
        }
        if (egress.empty()) {
            if (const std::string ep = udpEndpointOf(cfg.gatewayChannel); !ep.empty()) {
                std::string why;
                const auto local = localEgressFor(ep, why);
                if (!local) {
                    err << "rune: " << why << "; set --cli-egress (or RUNE_EGRESS)\n";
                    return false;
                }
                egress = *local;
            }
        }
        if (!egress.empty()) cfg.egressChannel = egress;
        try {
            if (const std::string v = a.value("--cli-stream-in"); !v.empty()) cfg.gatewayStreamId = std::stoi(v);
            if (const std::string v = a.value("--cli-stream-out"); !v.empty()) cfg.egressStreamId = std::stoi(v);
        } catch (const std::exception&) {
            err << "rune: --cli-stream-in/--cli-stream-out must be integers\n";
            return false;
        }
        if (a.has("--cli-data-dir")) {
            cfg.dataDir = a.value("--cli-data-dir");   // the caller's directory, used as is (and left in place)
        } else {   // a private scratch directory per client, removed on close
            dir_ = ScopedDir(std::filesystem::temp_directory_path() /
                             ("rune-cli-" + std::to_string(::getpid()) + "-" + std::to_string(reinterpret_cast<std::uintptr_t>(this)) + "-" +
                              std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));
            cfg.dataDir = dir_.path().string();
        }
        // Authentication (Task 8): who to log in as.  --user / RUNE_USER and --password-file / RUNE_PASSWORD_FILE;
        // a password is NEVER taken from argv (parseArgs refuses --password) or from the environment.
        if (withCredentials) applyCredentials(cfg, a.value("--user"), a.value("--password-file"));
        ddlTimeout_ = std::chrono::milliseconds(cfg.ddlTimeoutMs);
        verbose_ = verbose;
        try {
            client_ = std::make_unique<rc::Client>(cfg);
            client_->connectAndWait(kConnectWait);
            printDevBannerIfAny(*client_, err);   // dev mode: say so, loudly
            return true;
        } catch (const std::exception& e) {
            lastFailure_ = e.what();
            if (std::string_view(e.what()).find("timed out") != std::string_view::npos)
                err << "rune: connect to " << endpoint << " timed out\n";
            else if (!quietFailure)
                err << "rune error: " << e.what() << "\n";
            close();
            return false;
        }
    }

    void close() noexcept {
        if (client_) {
            try {
                client_->close();
            } catch (...) {
            }
            client_.reset();
        }
        dir_ = ScopedDir();
    }

    [[nodiscard]] rc::Client& client() { return *client_; }
    /// The text of the last failed open() (the exception message), for callers that explain it themselves.
    [[nodiscard]] const std::string& lastFailure() const noexcept { return lastFailure_; }
    [[nodiscard]] std::chrono::milliseconds ddlTimeout() const noexcept { return ddlTimeout_; }

    /// --verbose: print (and clear) the Redirect / NotLeader retries the client followed since the last call.
    void flushVerbose(std::ostream& err) {
        if (!verbose_ || !client_) return;
        std::vector<rc::Client::DdlRetryNote> notes;
        const std::size_t dropped = client_->takeDdlRetryNotes(notes);
        for (const auto& n : notes) {
            if (n.kind == rc::Client::DdlRetryNote::Kind::Redirect)
                err << "rune: redirect " << n.redirects << "/" << rc::Client::kMaxDdlRedirects << " -> " << n.text << "\n";
            else
                err << "rune: NotLeader" << (n.text.empty() ? "" : " (" + n.text + ")") << ", retrying in " << n.backoffMs
                    << " ms\n";
        }
        if (dropped != 0) err << "rune: " << dropped << " further retry notes dropped\n";
    }

private:
    ScopedDir                   dir_;
    std::unique_ptr<rc::Client> client_;
    std::chrono::milliseconds   ddlTimeout_{15'000};
    bool                        verbose_ = false;
    std::string                 lastFailure_;
};

#endif   // RUNE_CLI_LIVE_ENABLED

} // namespace rune::cli::support
