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

#include <charconv>
#include <istream>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace rune::cli::console {
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

/// A line of input; false at EOF.  Task 4 adds a polling TTY source.
struct LineSource {
    virtual ~LineSource() = default;
    virtual bool next(std::string& line) = 0;
};

constexpr std::string_view kPrompt             = "rune> ";
constexpr std::string_view kContinuationPrompt = "   ...> ";

void printConsoleUsage(std::ostream& err) {
    err << "Usage: rune console [--broker <ep>] [--cli-egress <ch>] [-f <script.rql|->] [--dry-run]\n"
           "                    [--validate] [--expected-version N] [--request-id <32 hex>] [--verbose]\n"
           "                    [--user <u>] [--password-file <p>]\n";
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
    else if (flag == "--cli-stream-in") o.live.streamIn = v;
    else if (flag == "--cli-stream-out") o.live.streamOut = v;
    else if (flag == "--cli-data-dir") o.live.dataDir = v;
#endif
}

/// Strict parse: a typo never falls through to an interactive session blocked on stdin.
/// Returns 0 when ok, else the usage exit code (2).
int parseConsoleOptions(const std::vector<std::string>& args, ConsoleOptions& o, std::ostream& err) {
    static constexpr std::string_view kValue[] = {"-f", "--broker", "--cli-egress", "--expected-version", "--request-id",
                                                  "--user", "--password-file",
                                                  "--cli-stream-in", "--cli-stream-out", "--cli-data-dir"};   // last 3 hidden
    static constexpr std::string_view kBool[]  = {"--dry-run", "--validate", "--verbose"};
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
            else setLive(o, "--verbose", "");
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
                 a == "--cli-stream-out" || a == "--cli-data-dir") setLive(o, a, v);
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

// Task 4 replaces this with the connected session.
int runLive(const ConsoleOptions&, std::istream&, const std::string&, std::ostream&, std::ostream& err) {
    err << "rune console: live mode requires RUNE_CLI_LIVE_ENABLED and a real Aeron build. Use --dry-run.\n";
    return 1;
}

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

} // namespace rune::cli::console
