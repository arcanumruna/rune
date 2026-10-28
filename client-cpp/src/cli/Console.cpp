// src/cli/Console.cpp — Phase 2b Part D: `rune console`.
//
//   rune console [--broker <ep>] [--cli-egress <ch>] [-f <script.rql|->] [--dry-run]
//                [--validate] [--expected-version N] [--request-id <32 hex>] [--verbose]
//                [--user <u>] [--password-file <p>]
//
// Interactive: "rune> " / "   ...> " until the buffered text ends with a ';' outside
// comments and strings (ql::classifyInput); each completed input is parsed (parseScript),
// prepared (engine::prepare) and run in order; a failure skips the rest of THAT input and
// never ends the session (except SessionRevoked, DD11).  exit / quit / EOF leave (0).
// Script (-f): every statement parsed and prepared first (DD6); then run in order; the first
// failure stops it (exit = the broker code, or 1).  Control plane only; single-threaded.
#include "rune/cli/Console.h"
#include "rune/cli/CatalogCommands.h"   // parseRequestId
#include "QlEngine.h"

#include "rune/ql/Parser.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <istream>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <poll.h>
#include <termios.h>
#include <unistd.h>

namespace rune::cli::console {

bool PollingFdLineSource::next(std::string& line) {
    for (;;) {
        const auto end = buf_.begin() + static_cast<std::ptrdiff_t>(len_);
        if (const auto nl = std::find(buf_.begin(), end, '\n'); nl != end) {
            const std::size_t used = static_cast<std::size_t>(nl - buf_.begin()) + 1;
            if (discarding_) {
                discarding_ = false;
                std::memmove(buf_.data(), buf_.data() + used, len_ - used);
                len_ -= used;
                continue;
            }
            line.assign(buf_.begin(), nl);
            if (!line.empty() && line.back() == '\r') line.pop_back();
            std::memmove(buf_.data(), buf_.data() + used, len_ - used);
            len_ -= used;
            return true;
        }
        if (discarding_) len_ = 0;   // no newline yet: the over-long line goes on, drop what we hold
        if (len_ == buf_.size()) {
            len_        = 0;
            discarding_ = true;
            if (err != nullptr) *err << "error: input line too long (64 KiB); the line was discarded\n";
            line.clear();
            return true;
        }
        pollfd p{fd_, POLLIN, 0};
        const int n = ::poll(&p, 1, 50);
        if (n == 0) {
            ++idlePumps;
            idle_(ctx_);
            continue;
        }
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        const ssize_t got = ::read(fd_, buf_.data() + len_, buf_.size() - len_);
        if (got < 0 && errno == EINTR) continue;
        if (got <= 0) {
            if (len_ == 0 || discarding_) return false;
            line.assign(buf_.begin(), buf_.begin() + static_cast<std::ptrdiff_t>(len_));   // a last line without '\n'
            len_ = 0;
            return true;
        }
        len_ += static_cast<std::size_t>(got);
    }
}

namespace {

struct ConsoleOptions {
    std::string scriptPath;               // -f; "" = interactive
    bool dryRun = false;
    engine::PrepareOptions prep;          // --validate, --expected-version
    std::optional<rune::catalog::RequestId> requestIdBase;   // --request-id (-f only)
#ifdef RUNE_CLI_LIVE_ENABLED
    engine::LiveOptions live;             // --broker / --cli-egress / credentials / hidden flags
#endif
};

constexpr std::string_view kPrompt             = "rune> ";
constexpr std::string_view kContinuationPrompt = "   ...> ";

void printConsoleUsage(std::ostream& err) {
    err << "Usage: rune console [--broker <ep>] [--cli-egress <ch>] [-f <script.rql|->] [--dry-run]\n"
           "                    [--validate] [--expected-version N] [--request-id <32 hex>] [--verbose]\n"
           "                    [--user <u>] [--password-file <p>] [--tls-ca <pem>] [--tls-host <name>]\n"
           "                    [--insecure-plaintext]\n";
}

int usage(std::ostream& err, const std::string& msg) {
    err << "rune console: " << msg << "\n";
    printConsoleUsage(err);
    return 2;
}

/// Records a live-session flag (accepted and ignored in a build without the live client).
void setLive([[maybe_unused]] ConsoleOptions& o, [[maybe_unused]] std::string_view flag, [[maybe_unused]] const std::string& v) {
#ifdef RUNE_CLI_LIVE_ENABLED
    if (flag == "--verbose") o.live.verbose = true;
    else if (flag == "--broker") o.live.endpoint = v;
    else if (flag == "--cli-egress") o.live.egress = v;
    else if (flag == "--user") o.live.user = v;
    else if (flag == "--password-file") o.live.passwordFile = v;
    else if (flag == "--tls-ca") o.live.tlsCa = v;              // issue #13
    else if (flag == "--tls-host") o.live.tlsHost = v;
    else if (flag == "--insecure-plaintext") o.live.insecurePlaintext = true;
    else if (flag == "--cli-stream-in") o.live.streamIn = v;
    else if (flag == "--cli-stream-out") o.live.streamOut = v;
    else if (flag == "--cli-data-dir") o.live.dataDir = v;
#endif
}

/// Strict parse: a typo never falls through to an interactive session blocked on stdin.
/// Returns 0 when ok, else the usage exit code (2).
int parseConsoleOptions(const std::vector<std::string>& args, ConsoleOptions& o, std::ostream& err) {
    static constexpr std::string_view kValue[] = {"-f", "--broker", "--cli-egress", "--expected-version", "--request-id",
                                                  "--user", "--password-file", "--tls-ca", "--tls-host",
                                                  "--cli-stream-in", "--cli-stream-out", "--cli-data-dir"};   // last 3 hidden
    static constexpr std::string_view kBool[]  = {"--dry-run", "--validate", "--verbose", "--insecure-plaintext"};
    auto in = [](std::string_view a, auto& set) {
        for (const auto s : set) if (s == a) return true;
        return false;
    };
    std::string requestId, expected;
    for (std::size_t i = 1; i < args.size(); ++i) {
        const std::string& a = args[i];
        if (in(a, kBool)) {
            if (a == "--dry-run") o.dryRun = true;
            else if (a == "--validate") o.prep.validate = true;
            else setLive(o, a, "");   // --verbose / --insecure-plaintext
            continue;
        }
        if (!in(a, kValue)) {
            return usage(err, (a.rfind('-', 0) == 0 ? "unknown option '" : "unexpected argument '") + a + "'");
        }
        if (i + 1 >= args.size()) return usage(err, a + " needs a value");
        const std::string& v = args[++i];
        if (a == "-f") o.scriptPath = v;
        else if (a == "--expected-version") expected = v;
        else if (a == "--request-id") requestId = v;
        else if (a == "--broker" || a == "--cli-egress" || a == "--user" || a == "--password-file" || a == "--cli-stream-in" ||
                 a == "--cli-stream-out" || a == "--cli-data-dir" || a == "--tls-ca" || a == "--tls-host") setLive(o, a, v);
    }
    if (!expected.empty()) {
        int value = -1;
        const auto r = std::from_chars(expected.data(), expected.data() + expected.size(), value);
        if (r.ec != std::errc{} || r.ptr != expected.data() + expected.size() || value < 0 || value > 0xFFFF)
            return usage(err, "--expected-version must be a non-negative integer (0..65535)");
        o.prep.expectedVersion = static_cast<uint16_t>(value);
    }
    if (!requestId.empty()) {
        if (o.scriptPath.empty()) return usage(err, "--request-id needs -f (statement N uses lo + N - 1)");
        o.requestIdBase = parseRequestId(requestId);
        if (!o.requestIdBase || o.requestIdBase->none()) return usage(err, "--request-id must be 32 hex digits, not all zero");
    }
    return 0;
}

// ── Prepared batches ─────────────────────────────────────────────────────────

struct PreparedStatement { engine::Prepared p; std::string text; };

/// Parse + prepare every statement of `src`.  false (message on err, prefixed by `origin`) when any
/// statement fails: nothing of `src` runs (DD6).
bool prepareAll(std::string_view origin, std::string_view src, const engine::PrepareOptions& opts,
                std::vector<PreparedStatement>& out, std::ostream& err) {
    auto parsed = rune::ql::parseScript(src);
    auto prefix = [&](std::size_t n) {
        if (!origin.empty()) err << origin << ":statement " << n << ": ";
    };
    if (!parsed) {
        const auto& e = parsed.error();
        prefix(e.statementIndex);
        err << e.error.line << ":" << e.error.col << ": " << e.error.message << "\n";
        return false;
    }
    const std::vector<std::string> texts = engine::splitStatements(src);
    if (texts.size() != parsed->size()) {
        err << "rune console: internal error: statement splitter found " << texts.size() << " statements, the parser "
            << parsed->size() << "; nothing was executed\n";
        return false;
    }
    for (std::size_t i = 0; i < parsed->size(); ++i) {
        std::string why;
        auto p = engine::prepare((*parsed)[i], opts, why);
        if (!p) {
            prefix(i + 1);
            err << why << "\n";
            return false;
        }
        out.push_back({std::move(*p), texts[i]});
    }
    return true;
}

template <class Exec>
int runBatch(Exec& exec, const std::vector<PreparedStatement>& batch, std::size_t firstIndex) {
    for (std::size_t i = 0; i < batch.size(); ++i) {
        if (const int rc = exec.execute(batch[i].p, batch[i].text, firstIndex + i); rc != 0) return rc;
    }
    return 0;
}

// ── Script mode (-f) ─────────────────────────────────────────────────────────

template <class Exec>
int runScript(Exec& exec, const ConsoleOptions& o, std::string_view src, std::ostream& err) {
    std::vector<PreparedStatement> batch;
    if (!prepareAll(o.scriptPath, src, o.prep, batch, err)) return 1;
    return runBatch(exec, batch, 1);
}

// ── Interactive mode: REPL state machine (CLAUDE.md #3) ──────────────────────
//
//   AwaitingStatement ──line, Empty──────> AwaitingStatement
//   AwaitingStatement ──exit / quit──────> leave (0)
//   any ──line, Incomplete───────────────> ContinuingStatement{text so far}
//   any ──line, Complete─────────────────> prepare + run the batch ──> AwaitingStatement
//   any ──EOF──> leave (0); a ContinuingStatement is reported, never run
//   any ──executor.sessionEnded()────────> leave (46, DD11)

struct AwaitingStatement {};
struct ContinuingStatement { std::string text; };
using ReplState = std::variant<AwaitingStatement, ContinuingStatement>;

std::string_view trimmed(std::string_view s) {
    const auto b = s.find_first_not_of(" \t\r\n");
    if (b == std::string_view::npos) return {};
    return s.substr(b, s.find_last_not_of(" \t\r\n") - b + 1);
}

bool isExitCommand(std::string_view line) {
    const auto t = trimmed(line);
    return t == "exit" || t == "quit" || t == "exit;" || t == "quit;";
}

template <class Exec>
int runInteractive(Exec& exec, const ConsoleOptions& o, LineSource& in, std::ostream& out, std::ostream& err) {
    ReplState state = AwaitingStatement{};
    std::string line;
    std::size_t index = 1;
    for (;;) {
        out << (std::holds_alternative<AwaitingStatement>(state) ? kPrompt : kContinuationPrompt) << std::flush;
        if (!in.next(line)) {
            out << "\n";
            if (std::holds_alternative<ContinuingStatement>(state))
                err << "error: input ended inside a statement (missing ';'); it was not executed\n";
            return 0;
        }
        std::string text;
        if (auto* c = std::get_if<ContinuingStatement>(&state)) text = std::move(c->text);
        else if (isExitCommand(line)) return 0;
        text += line;
        text += '\n';
        switch (rune::ql::classifyInput(text)) {
            case rune::ql::InputState::Empty: state = AwaitingStatement{}; break;
            case rune::ql::InputState::Incomplete: state = ContinuingStatement{std::move(text)}; break;
            case rune::ql::InputState::Complete: {
                state = AwaitingStatement{};
                std::vector<PreparedStatement> batch;
                if (prepareAll({}, text, o.prep, batch, err)) {
                    (void)runBatch(exec, batch, index);   // DD7: a failure skips the rest of this input
                    index += batch.size();
                }
                if (exec.sessionEnded()) return 46;
                break;
            }
        }
    }
}

// ── Sources and executors ────────────────────────────────────────────────────

struct StreamLineSource final : LineSource {
    std::istream& in;
    explicit StreamLineSource(std::istream& s) : in(s) {}
    bool next(std::string& line) override {
        if (!std::getline(in, line)) return false;
        if (!line.empty() && line.back() == '\r') line.pop_back();   // CRLF terminals / files
        return true;
    }
};

struct DryRunExecutor {
    std::ostream& out;
    std::optional<rune::catalog::RequestId> base;
    int execute(const engine::Prepared& p, const std::string&, std::size_t index) {
        std::optional<rune::catalog::RequestId> rid;
        if (base) rid = rune::catalog::RequestId{base->hi, base->lo + (index - 1)};
        engine::printPlan(p, rid, out);
        return 0;
    }
    [[nodiscard]] bool sessionEnded() const noexcept { return false; }
};

#ifdef RUNE_CLI_LIVE_ENABLED

struct LiveExecutor {
    engine::LiveBroker& broker;
    engine::QlRun       run;
    std::optional<rune::catalog::RequestId> base;
    bool ended = false;
    int execute(const engine::Prepared& p, const std::string& text, std::size_t index) {
        std::optional<rune::catalog::RequestId> rid;
        if (base) rid = rune::catalog::RequestId{base->hi, base->lo + (index - 1)};
        run.index = index;
        run.lastErrorCode = 0;
        if (run.exec(p, text, rid)) return 0;
        // A revoked session fails the client for good: the next statement sees "client closed", with no broker code.
        if (run.lastErrorCode == 0 && broker.client().sessionRejectCode() ==
                                          static_cast<int16_t>(rune::protocol::client::ErrorCode::SessionRevoked))
            run.lastErrorCode = static_cast<uint16_t>(rune::protocol::client::ErrorCode::SessionRevoked);
        if (run.lastErrorCode == static_cast<uint16_t>(rune::protocol::client::ErrorCode::SessionRevoked)) {
            ended = true;
            run.err << "error: SessionRevoked (46): the session ended\n";
        }
        return run.lastErrorCode != 0 ? run.lastErrorCode : 1;
    }
    [[nodiscard]] bool sessionEnded() const noexcept { return ended; }
};

void pumpClient(void* ctx) noexcept {
    try {
        (void)static_cast<engine::LiveBroker*>(ctx)->client().doWork();
    } catch (...) {
    }
}

/// Reads a password from the terminal with echo off; the terminal is restored on every path.  false on EOF / error.
bool promptPassword(const std::string& user, std::string& password, std::ostream& err) {
    termios saved{};
    const bool haveTermios = ::tcgetattr(STDIN_FILENO, &saved) == 0;
    if (haveTermios) {
        termios quiet = saved;
        quiet.c_lflag &= ~static_cast<tcflag_t>(ECHO);
        ::tcsetattr(STDIN_FILENO, TCSANOW, &quiet);
    }
    struct Restore {
        bool on; const termios& t;
        ~Restore() { if (on) ::tcsetattr(STDIN_FILENO, TCSANOW, &t); }
    } restore{haveTermios, saved};
    err << "password for " << user << ": " << std::flush;
    password.clear();
    bool ok = false;
    for (;;) {   // byte-wise: nothing of the following input is consumed
        char c = 0;
        const ssize_t n = ::read(STDIN_FILENO, &c, 1);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) { ok = !password.empty(); break; }
        if (c == '\n') { ok = true; break; }
        if (c != '\r') password.push_back(c);
    }
    err << "\n";
    return ok;
}

struct LiveInput {
    std::istream* in = nullptr;      // the caller's stream (interactive, non-polling)
    int           fd = -1;           // >= 0: poll this fd (tests)
    bool          canPrompt = false; // a password may be prompted on the terminal
    uint64_t*     idlePumps = nullptr;
};

int runLiveSession(const ConsoleOptions& opts, std::string_view script, const LiveInput& input, std::ostream& out,
                   std::ostream& err) {
    ConsoleOptions o = opts;
    if (o.live.endpoint.empty()) {
        if (const char* env = std::getenv("RUNE_BROKER"); env != nullptr) o.live.endpoint = env;
    }
    if (o.live.endpoint.empty()) return usage(err, "--broker host:port is required (or --dry-run)");

    // Credentials (DD10): a password only from a file or the terminal prompt.
    std::string user = o.live.user;
    if (user.empty()) { if (const char* e = std::getenv("RUNE_USER"); e != nullptr) user = e; }
    bool havePasswordFile = !o.live.passwordFile.empty();
    if (!havePasswordFile) { if (const char* e = std::getenv("RUNE_PASSWORD_FILE"); e != nullptr && *e != '\0') havePasswordFile = true; }
    std::string password;
    struct Wipe {
        std::string& s;
        ~Wipe() { if (!s.empty()) rune::client::detail::secureZero(s.data(), s.size()); }
    } wipe{password};
    if (!user.empty() && !havePasswordFile) {
        if (!(input.canPrompt && o.scriptPath.empty() && ::isatty(STDIN_FILENO)))
            return usage(err, "--password-file is required with --user (passwords are never read from argv or the environment)");
        if (!promptPassword(user, password, err)) {
            err << "rune console: no password entered\n";
            return 1;
        }
        o.live.password = password;
    }

    engine::LiveBroker broker;
    const bool opened = broker.open(o.live, err);
    if (!password.empty()) {
        rune::client::detail::secureZero(password.data(), password.size());
        password.clear();
    }
    if (!o.live.password.empty()) rune::client::detail::secureZero(o.live.password.data(), o.live.password.size());
    o.live.password.clear();
    if (!opened) return 1;
    if (broker.client().authStatus().kind == rune::client::AuthStatus::Kind::PasswordChangeRequired)
        err << "WARNING: password change required — run ALTER USER " << broker.client().authStatus().user
            << " SET PASSWORD '…';\n";

    LiveExecutor exec{broker, engine::QlRun{broker, out, err, !o.scriptPath.empty(), 1, engine::OutputStyle::Console},
                      o.requestIdBase};
    exec.run.origin = o.scriptPath;
    int rc = 0;
    if (!o.scriptPath.empty()) {
        rc = runScript(exec, o, script, err);
    } else if (input.fd >= 0 || input.canPrompt) {
        // The process stdin (a TTY or a pipe, #311) is polled and the client pumped while it is idle; only an injected
        // non-stdin stream (StreamLineSource below) is read blocking.
        PollingFdLineSource src(input.fd >= 0 ? input.fd : STDIN_FILENO, &pumpClient, &broker);
        src.err = &err;
        rc = runInteractive(exec, o, src, out, err);
        if (input.idlePumps != nullptr) *input.idlePumps = src.idlePumps;
    } else {
        StreamLineSource src(*input.in);
        rc = runInteractive(exec, o, src, out, err);
    }
    broker.flushVerbose(err);
    return rc;
}

int runLive(const ConsoleOptions& o, std::istream& in, const std::string& script, std::ostream& out, std::ostream& err) {
    return runLiveSession(o, script, LiveInput{&in, -1, &in == &std::cin, nullptr}, out, err);
}

#else

int runLive(const ConsoleOptions&, std::istream&, const std::string&, std::ostream&, std::ostream& err) {
    err << "rune console: live mode requires RUNE_CLI_LIVE_ENABLED and a real Aeron build. Use --dry-run.\n";
    return 1;
}

#endif

} // namespace

int cmdConsole(const std::vector<std::string>& args, std::istream& in, std::ostream& out, std::ostream& err) {
    ConsoleOptions o;
    if (const int rc = parseConsoleOptions(args, o, err); rc != 0) return rc;
    std::string script;
    if (!o.scriptPath.empty() && !engine::readScript(o.scriptPath, in, script, err)) return 1;
    if (o.dryRun) {
        DryRunExecutor exec{out, o.requestIdBase};
        if (!o.scriptPath.empty()) return runScript(exec, o, script, err);
        StreamLineSource src(in);
        return runInteractive(exec, o, src, out, err);
    }
    return runLive(o, in, script, out, err);
}

int runLiveOnStdinForTest([[maybe_unused]] const std::vector<std::string>& args,
                          [[maybe_unused]] int64_t (*nanoClock)() noexcept, [[maybe_unused]] std::ostream& out,
                          std::ostream& err, [[maybe_unused]] uint64_t* idlePumps) {
#ifdef RUNE_CLI_LIVE_ENABLED
    ConsoleOptions o;
    if (const int rc = parseConsoleOptions(args, o, err); rc != 0) return rc;
    o.live.nanoClock = nanoClock;
    return runLiveSession(o, {}, LiveInput{&std::cin, -1, true, idlePumps}, out, err);
#else
    err << "rune console: live mode requires RUNE_CLI_LIVE_ENABLED\n";
    return 1;
#endif
}

int runLiveOnFdForTest([[maybe_unused]] const std::vector<std::string>& args, [[maybe_unused]] int fd,
                       [[maybe_unused]] std::ostream& out, std::ostream& err, [[maybe_unused]] uint64_t* idlePumps) {
#ifdef RUNE_CLI_LIVE_ENABLED
    ConsoleOptions o;
    if (const int rc = parseConsoleOptions(args, o, err); rc != 0) return rc;
    return runLiveSession(o, {}, LiveInput{nullptr, fd, false, idlePumps}, out, err);
#else
    err << "rune console: live mode requires RUNE_CLI_LIVE_ENABLED\n";
    return 1;
#endif
}

} // namespace rune::cli::console
