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

#include <rune/client/detail/SecureZero.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <istream>
#include <iterator>
#include <limits>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <csignal>
#include <poll.h>
#include <termios.h>
#include <unistd.h>

namespace rune::cli::console {

int consoleExitStatus(uint16_t brokerCode) noexcept { return (brokerCode >= 4 && brokerCode <= 124) ? brokerCode : 125; }   // Issue #301

uint16_t consoleSessionEndCode(uint16_t sessionRejectCode, uint16_t statementErrorCode) noexcept {   // Issue #308
    return sessionRejectCode != 0 ? sessionRejectCode : (statementErrorCode == 46 ? uint16_t{46} : uint16_t{0});
}

uint16_t consoleRejectCode(int16_t sessionRejectCode) noexcept {
    return sessionRejectCode != 0 ? static_cast<uint16_t>(sessionRejectCode) : uint16_t{0};
}

int consoleScriptExitStatus(int batchStatus, bool sessionEnded, uint16_t endCode) noexcept {
    return sessionEnded ? consoleExitStatus(endCode) : batchStatus;
}

int consoleOpenFailureStatus(std::string_view failureText) noexcept {
    return failureText.find("AuthenticationFailed") != std::string_view::npos ? consoleExitStatus(44) : 1;
}

PollingFdLineSource::~PollingFdLineSource() { rune::client::detail::secureZero(buf_.data(), buf_.size()); }

bool PollingFdLineSource::next(std::string& line) {
    lastLineDiscarded = false;
    // The buffer may hold a password literal (CREATE USER ... PASSWORD '...'): every byte that leaves it is zeroed.
    const auto wipeFrom = [this](std::size_t from, std::size_t n) { rune::client::detail::secureZero(buf_.data() + from, n); };
    for (;;) {
        const auto end = buf_.begin() + static_cast<std::ptrdiff_t>(len_);
        if (const auto nl = std::find(buf_.begin(), end, '\n'); nl != end) {
            const std::size_t used = static_cast<std::size_t>(nl - buf_.begin()) + 1;
            if (discarding_) {
                discarding_ = false;
                std::memmove(buf_.data(), buf_.data() + used, len_ - used);
                wipeFrom(len_ - used, used);
                len_ -= used;
                continue;
            }
            line.assign(buf_.begin(), nl);
            if (!line.empty() && line.back() == '\r') line.pop_back();
            std::memmove(buf_.data(), buf_.data() + used, len_ - used);
            wipeFrom(len_ - used, used);
            len_ -= used;
            return true;
        }
        if (discarding_) { wipeFrom(0, len_); len_ = 0; }   // no newline yet: the over-long line goes on, drop what we hold
        if (len_ == buf_.size()) {
            wipeFrom(0, len_);
            len_        = 0;
            discarding_ = true;
            if (err != nullptr) *err << kInputLineTooLongMessage;
            line.clear();
            lastLineDiscarded = true;   // Issue #307: the caller must not treat this as an ordinary empty line
            return true;
        }
        pollfd p{fd_, POLLIN, 0};
        const int n = ::poll(&p, 1, 50);
        // A timeout pumps the client; so does a poll interrupted by a signal: signals more frequent than the timeout
        // (SIGPROF, SIGWINCH, a timer) would otherwise re-poll forever and starve the pump - and the session with it.
        if (n == 0 || (n < 0 && errno == EINTR)) {
            ++idlePumps;
            idle_(ctx_);
            continue;
        }
        if (n < 0) return false;
        const ssize_t got = ::read(fd_, buf_.data() + len_, buf_.size() - len_);
        if (got < 0 && errno == EINTR) continue;
        if (got <= 0) {
            if (len_ == 0 || discarding_) return false;
            line.assign(buf_.begin(), buf_.begin() + static_cast<std::ptrdiff_t>(len_));   // a last line without '\n'
            wipeFrom(0, len_);
            len_ = 0;
            return true;
        }
        len_ += static_cast<std::size_t>(got);
    }
}

bool readPasswordLine(int fd, std::string& password) {
    namespace cd = rune::client::detail;
    char buf[cd::kMaxPasswordBytes + 1];
    struct Wipe { char* b; ~Wipe() { cd::secureZero(b, sizeof buf); } } wipe{buf};
    password.clear();
    std::size_t n = 0;
    bool ok = false;
    bool over = false;
    for (;;) {   // byte-wise: nothing of the following input is consumed
        char c = 0;
        const ssize_t got = ::read(fd, &c, 1);
        if (got < 0 && errno == EINTR) continue;
        if (got <= 0) { ok = n > 0; break; }
        if (c == '\n') { ok = true; break; }
        if (c == '\r') continue;
        if (n == cd::kMaxPasswordBytes) { over = true; continue; }   // over the cap: keep reading to the end of the line
        buf[n++] = c;
    }
    if (over) return false;
    password.assign(buf, n);   // one allocation, no growth copies
    return ok;
}

namespace {
// The one place a signal handler needs file-scope state: it must put the terminal back before the process dies or
// stops, and a handler can reach nothing but globals.  Written only by promptPasswordOnFd, before the handlers go in.
termios                g_savedTermios{};
termios                g_quietTermios{};
volatile sig_atomic_t  g_promptFd = -1;
constexpr int          kPromptSignals[] = {SIGINT, SIGTERM, SIGHUP, SIGQUIT, SIGTSTP};
struct sigaction       g_old[std::size(kPromptSignals)]{};
struct sigaction       g_ours{};

const struct sigaction* oldActionOf(int sig) noexcept {
    for (std::size_t i = 0; i < std::size(kPromptSignals); ++i)
        if (kPromptSignals[i] == sig) return &g_old[i];
    return nullptr;
}

/// Async-signal-safe only (tcsetattr, sigaction, sigprocmask, raise).  Puts the terminal back, re-delivers the
/// signal to the disposition that was there before the prompt (die, stop, or the application's own handler), and —
/// when that returns (a continue after a stop, an ignored signal, a handler that returns) — the prompt goes on, so
/// echo goes off again and this handler goes back in (C6 review, #306).
void restoreTerminalOnSignal(int sig) {
    const int savedErrno = errno;
    const int fd = g_promptFd;
    if (fd >= 0) ::tcsetattr(fd, TCSANOW, &g_savedTermios);
    if (const struct sigaction* old = oldActionOf(sig)) ::sigaction(sig, old, nullptr);
    sigset_t unblock;
    sigemptyset(&unblock);
    sigaddset(&unblock, sig);
    ::sigprocmask(SIG_UNBLOCK, &unblock, nullptr);
    ::raise(sig);   // dies, stops until SIGCONT, or runs the old handler here
    ::sigprocmask(SIG_BLOCK, &unblock, nullptr);   // the kernel unblocks it again on return
    ::sigaction(sig, &g_ours, nullptr);
    if (fd >= 0) ::tcsetattr(fd, TCSANOW, &g_quietTermios);
    errno = savedErrno;
}
} // namespace

bool promptPasswordOnFd(int fd, const std::string& user, std::string& password, std::ostream& err) {
    termios saved{};
    const bool haveTermios = ::tcgetattr(fd, &saved) == 0;
    struct Restore {
        bool on; int fd; const termios& t;
        ~Restore() {
            if (!on) return;
            // Terminal first: a signal arriving after it finds our handler, which restores it again — harmless.
            // Handlers first would leave a window where the old default kills the process with echo off.
            ::tcsetattr(fd, TCSANOW, &t);
            g_promptFd = -1;
            for (std::size_t i = 0; i < std::size(kPromptSignals); ++i) ::sigaction(kPromptSignals[i], &g_old[i], nullptr);
        }
    } restore{haveTermios, fd, saved};
    if (haveTermios) {
        g_savedTermios = saved;
        g_quietTermios = saved;
        g_quietTermios.c_lflag &= ~static_cast<tcflag_t>(ECHO);
        g_promptFd = fd;
        // Handlers first, then echo off: a signal at any point finds the terminal restorable.  No SA_RESTART: a read
        // interrupted by a stop / continue returns EINTR and readPasswordLine retries it.  Each handled signal is
        // blocked while the handler runs, so a second one never interleaves with a restore / re-quiet.
        g_ours = {};
        g_ours.sa_handler = &restoreTerminalOnSignal;
        sigemptyset(&g_ours.sa_mask);
        for (const int sig : kPromptSignals) sigaddset(&g_ours.sa_mask, sig);
        for (std::size_t i = 0; i < std::size(kPromptSignals); ++i) ::sigaction(kPromptSignals[i], &g_ours, &g_old[i]);
        ::tcsetattr(fd, TCSANOW, &g_quietTermios);
    }
    err << "password for " << user << ": " << std::flush;
    const bool ok = readPasswordLine(fd, password);
    err << "\n";
    return ok;
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
           "                    [--insecure-plaintext]\n"
           "exit status: 0 ok, 1 error, 2 usage, 3 diff, 4..124 the broker error code, 125 any other broker error\n";
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

/// Wipes the statement texts of a batch (they may hold password literals) when it goes out of scope.
struct BatchWipe {
    std::vector<PreparedStatement>& v;
    ~BatchWipe() { for (auto& st : v) rune::client::detail::wipeString(st.text); }
};

/// Parse + prepare every statement of `src`.  false (message on err, prefixed by `origin`) when any
/// statement fails: nothing of `src` runs (DD6).
bool prepareAll(std::string_view origin, std::string_view src, const engine::PrepareOptions& opts,
                std::vector<PreparedStatement>& out, std::ostream& err) {
    auto parsed = rune::ql::parseScript(src);
    struct ParsedWipe {   // the parsed statements hold their password literals (#305)
        decltype(parsed)& r;
        ~ParsedWipe() {
            if (!r) return;
            for (auto& st : *r) {
                if (auto* c = std::get_if<rune::ql::CreateUserDef>(&st)) rune::client::detail::wipeString(c->password);
                else if (auto* a = std::get_if<rune::ql::AlterUserDef>(&st)) rune::client::detail::wipeString(a->password);
            }
        }
    } parsedWipe{parsed};
    auto prefix = [&](std::size_t n) {
        if (!origin.empty()) err << origin << ":statement " << n << ": ";
    };
    if (!parsed) {
        const auto& e = parsed.error();
        prefix(e.statementIndex);
        err << e.error.line << ":" << e.error.col << ": " << e.error.message << "\n";
        return false;
    }
    std::vector<std::string> texts = engine::splitStatements(src);
    struct TextsWipe {   // statement texts may hold password literals
        std::vector<std::string>& v;
        ~TextsWipe() { for (auto& t : v) rune::client::detail::wipeString(t); }
    } textsWipe{texts};
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

/// Issue #303: the same refusal as `rune ql`: statement N uses lo + N - 1, which must not wrap.
bool requestIdFits(const std::optional<rune::catalog::RequestId>& base, std::size_t n, std::ostream& err) {
    if (!base || n <= 1 || base->lo <= std::numeric_limits<uint64_t>::max() - (n - 1)) return true;
    err << "rune: --request-id: the low 64 bits would overflow at statement "
        << (std::numeric_limits<uint64_t>::max() - base->lo) + 2 << " of " << n
        << " (statement N uses lo + N - 1); choose a smaller id\n";
    return false;
}

// ── Interactive mode: REPL state machine (CLAUDE.md #3) ──────────────────────
//
//   AwaitingStatement ──line, Empty──────> AwaitingStatement
//   AwaitingStatement ──exit / quit──────> leave (0)
//   any ──line, Incomplete───────────────> ContinuingStatement{text so far}
//   any ──line, Complete─────────────────> prepare + run the batch ──> AwaitingStatement
//   any ──EOF──> leave (0); a ContinuingStatement is reported, never run
//   any ──executor.sessionEnded()────────> leave (the session's end code, DD11 / #308)
//   any ──line was too long (#307)────────> skipped; a ContinuingStatement is abandoned

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
    struct ReplWipe {   // the typed text may hold a password literal: nothing of it outlives the session
        ReplState& st; std::string& line;
        ~ReplWipe() {
            rune::client::detail::wipeString(line);
            if (auto* c = std::get_if<ContinuingStatement>(&st)) rune::client::detail::wipeString(c->text);
        }
    } replWipe{state, line};
    for (;;) {
        out << (std::holds_alternative<AwaitingStatement>(state) ? kPrompt : kContinuationPrompt) << std::flush;
        if (!in.next(line)) {
            out << "\n";
            if (std::holds_alternative<ContinuingStatement>(state))
                err << "error: input ended inside a statement (missing ';'); it was not executed\n";
            return 0;
        }
        if (in.lastLineDiscarded) {   // Issue #307: never join the text before and after a dropped line
            if (auto* c = std::get_if<ContinuingStatement>(&state)) {
                rune::client::detail::wipeString(c->text);
                err << "error: the statement in progress was abandoned (an input line was too long)\n";
                state = AwaitingStatement{};
            }
            continue;
        }
        std::string text;
        struct TextWipe { std::string& s; ~TextWipe() { rune::client::detail::wipeString(s); } } textWipe{text};
        if (auto* c = std::get_if<ContinuingStatement>(&state)) {
            text = std::move(c->text);
            rune::client::detail::wipeString(c->text);
        } else if (isExitCommand(line)) return 0;
        rune::client::detail::appendWiped(text, line);   // growth never frees an unwiped copy (#305)
        rune::client::detail::wipeString(line);          // consumed into `text`
        rune::client::detail::appendWiped(text, "\n");
        switch (rune::ql::classifyInput(text)) {
            case rune::ql::InputState::Empty: state = AwaitingStatement{}; break;
            case rune::ql::InputState::Incomplete: state = ContinuingStatement{std::move(text)}; break;
            case rune::ql::InputState::Complete: {
                state = AwaitingStatement{};
                std::vector<PreparedStatement> batch;
                BatchWipe batchWipe{batch};
                if (prepareAll({}, text, o.prep, batch, err)) {
                    (void)runBatch(exec, batch, index);   // DD7: a failure skips the rest of this input
                    index += batch.size();
                }
                if (exec.sessionEnded()) return consoleExitStatus(exec.endCode());
                break;
            }
        }
    }
}

// ── Sources and executors ────────────────────────────────────────────────────

struct StreamLineSource final : LineSource {
    std::istream& in;
    std::ostream* err = nullptr;   // where the "input line too long" error goes (null: silent)
    explicit StreamLineSource(std::istream& s) : in(s) {}
    bool next(std::string& line) override {
        lastLineDiscarded = false;
        // std::getline would grow `line` and free unwiped copies of a typed password (#305): read char by char.
        rune::client::detail::wipeString(line);
        std::streambuf* sb = in.rdbuf();
        if (sb == nullptr) { in.setstate(std::ios::badbit); return false; }
        bool any = false;
        for (;;) {
            const auto c = sb->sbumpc();
            if (std::char_traits<char>::eq_int_type(c, std::char_traits<char>::eof())) {
                in.setstate(any ? std::ios::eofbit : std::ios::eofbit | std::ios::failbit);
                if (!any) return false;
                break;
            }
            any = true;
            const char ch = std::char_traits<char>::to_char_type(c);
            if (ch == '\n') break;
            if (line.size() + 1 >= kMaxInputLineBytes) {   // same cap and rule as PollingFdLineSource (#307)
                rune::client::detail::wipeString(line);
                line.clear();
                for (auto d = sb->sbumpc(); !std::char_traits<char>::eq_int_type(d, std::char_traits<char>::eof()) &&
                                            std::char_traits<char>::to_char_type(d) != '\n';)
                    d = sb->sbumpc();   // drop the rest of the over-long line
                if (err != nullptr) *err << kInputLineTooLongMessage;
                lastLineDiscarded = true;
                return true;
            }
            const char one[1] = {ch};
            rune::client::detail::appendWiped(line, std::string_view(one, 1));
        }
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
    [[nodiscard]] uint16_t endCode() const noexcept { return 0; }
};

#ifdef RUNE_CLI_LIVE_ENABLED

struct LiveExecutor {
    engine::LiveBroker& broker;
    engine::QlRun       run;
    std::optional<rune::catalog::RequestId> base;
    bool ended = false;
    uint16_t endCode_ = 0;
    int execute(const engine::Prepared& p, const std::string& text, std::size_t index) {
        std::optional<rune::catalog::RequestId> rid;
        if (base) rid = rune::catalog::RequestId{base->hi, base->lo + (index - 1)};
        run.index = index;
        run.lastErrorCode = 0;
        if (run.exec(p, text, rid)) return 0;
        // Issue #308: any terminal session rejection ends the console, not only SessionRevoked.  A rejected session
        // fails the client for good: the next statement would see "client closed", with no broker code.
        const int16_t reject = broker.client().sessionRejectCode();
        const uint16_t end = consoleSessionEndCode(consoleRejectCode(reject), run.lastErrorCode);
        if (end != 0) {
            ended    = true;
            endCode_ = end;
            if (run.lastErrorCode == 0) run.lastErrorCode = end;
            run.err << "error: " << engine::errorCodeName(end) << " (" << end << "): the session ended\n";
        }
        return run.lastErrorCode != 0 ? consoleExitStatus(run.lastErrorCode) : 1;   // Issue #301
    }
    [[nodiscard]] bool sessionEnded() const noexcept { return ended; }
    [[nodiscard]] uint16_t endCode() const noexcept { return endCode_; }
};

void pumpClient(void* ctx) noexcept {
    try {
        (void)static_cast<engine::LiveBroker*>(ctx)->client().doWork();
    } catch (...) {
    }
}

/// Reads a password from the terminal with echo off; the terminal is restored on every path.  false on EOF / error.
bool promptPassword(const std::string& user, std::string& password, std::ostream& err) {
    return promptPasswordOnFd(STDIN_FILENO, user, password, err);
}

struct LiveInput {
    std::istream* in = nullptr;      // the caller's stream (interactive, non-polling)
    int           fd = -1;           // >= 0: poll this fd (tests)
    bool          canPrompt = false; // a password may be prompted on the terminal
    uint64_t*     idlePumps = nullptr;
};

int runLiveSession(const ConsoleOptions& opts, const std::vector<PreparedStatement>& batch, const LiveInput& input, std::ostream& out,
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
        ~Wipe() { rune::client::detail::wipeString(s); }
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
    rune::client::detail::wipeString(password);
    rune::client::detail::wipeString(o.live.password);
    if (!opened) return consoleOpenFailureStatus(broker.lastFailure());   // a refused login ends like a refused reconnect
    if (broker.client().authStatus().kind == rune::client::AuthStatus::Kind::PasswordChangeRequired)
        err << "WARNING: password change required — run ALTER USER " << broker.client().authStatus().user
            << " SET PASSWORD '…';\n";

    LiveExecutor exec{broker, engine::QlRun{broker, out, err, !o.scriptPath.empty(), 1, engine::OutputStyle::Console},
                      o.requestIdBase};
    exec.run.origin = o.scriptPath;
    int rc = 0;
    if (!o.scriptPath.empty()) {
        rc = consoleScriptExitStatus(runBatch(exec, batch, 1), exec.sessionEnded(), exec.endCode());   // as interactive
    } else if (input.fd >= 0 || input.canPrompt) {
        // The process stdin (a TTY or a pipe, #311) is polled and the client pumped while it is idle; only an injected
        // non-stdin stream (StreamLineSource below) is read blocking.
        PollingFdLineSource src(input.fd >= 0 ? input.fd : STDIN_FILENO, &pumpClient, &broker);
        src.err = &err;
        rc = runInteractive(exec, o, src, out, err);
        if (input.idlePumps != nullptr) *input.idlePumps = src.idlePumps;
    } else {
        StreamLineSource src(*input.in);
        src.err = &err;
        rc = runInteractive(exec, o, src, out, err);
    }
    broker.flushVerbose(err);
    return rc;
}

int runLive(const ConsoleOptions& o, std::istream& in, const std::vector<PreparedStatement>& batch, std::ostream& out,
            std::ostream& err) {
    return runLiveSession(o, batch, LiveInput{&in, -1, &in == &std::cin, nullptr}, out, err);
}

#else

int runLive(const ConsoleOptions&, std::istream&, const std::vector<PreparedStatement>&, std::ostream&, std::ostream& err) {
    err << "rune console: live mode requires RUNE_CLI_LIVE_ENABLED and a real Aeron build. Use --dry-run.\n";
    return 1;
}

#endif

} // namespace

int cmdConsole(const std::vector<std::string>& args, std::istream& in, std::ostream& out, std::ostream& err) {
    ConsoleOptions o;
    if (const int rc = parseConsoleOptions(args, o, err); rc != 0) return rc;
    std::string script;
    struct ScriptWipe { std::string& s; ~ScriptWipe() { rune::client::detail::wipeString(s); } } scriptWipe{script};
    std::vector<PreparedStatement> batch;
    BatchWipe batchWipe{batch};
    if (!o.scriptPath.empty()) {
        if (!engine::readScript(o.scriptPath, in, script, err)) return 1;
        // Issue #302: the script is parsed and prepared once, before any broker is needed.
        if (!prepareAll(o.scriptPath, script, o.prep, batch, err)) return 1;
        if (batch.empty()) return 0;
        if (!requestIdFits(o.requestIdBase, batch.size(), err)) return 1;   // Issue #303
    }
    if (o.dryRun) {
        DryRunExecutor exec{out, o.requestIdBase};
        if (!o.scriptPath.empty()) return runBatch(exec, batch, 1);
        StreamLineSource src(in);
        src.err = &err;
        return runInteractive(exec, o, src, out, err);
    }
    return runLive(o, in, batch, out, err);
}

int runInteractiveDryRunForTest(LineSource& src, std::ostream& out, std::ostream& err) {
    ConsoleOptions o;
    DryRunExecutor exec{out, std::nullopt};
    return runInteractive(exec, o, src, out, err);
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
