// src/cli/QlEngine.h — the `rune ql` engine: statement preparation, dry-run printing, the connected LiveBroker and the
// per-statement QlRun executor.  Shared by `rune ql` (CatalogCommands.cpp) and `rune console`.  Internal to the CLI (not installed).
#pragma once

#include "CliSupport.h"

#include "rune/ql/Backends.h"
#include "rune/ql/Parser.h"

#include <rune/catalog/CatalogFile.h>
#include <rune/client/detail/SecureZero.h>
#include "rune/cli/SecurityCommands.h"
#include <rune/codec/AlterOps.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#ifdef RUNE_CLI_LIVE_ENABLED
#include <rune/client/Client.h>
#include <rune/protocol/client/Protocol.h>
#endif

namespace rune::cli::engine {

namespace cat = rune::catalog;
namespace ql  = rune::ql;
namespace sec = rune::security;

// ═══════════════════════════════════════════════════════════════════════════
// Formatting helpers shared with CatalogCommands.cpp
// ═══════════════════════════════════════════════════════════════════════════
[[nodiscard]] std::string hex16(uint64_t v);
[[nodiscard]] std::string orDash(std::string s);
[[nodiscard]] const char* persistenceName(cat::Persistence p) noexcept;
[[nodiscard]] const char* compatName(rune::codec::CompatMode m) noexcept;
[[nodiscard]] std::string descriptorText(const std::vector<uint8_t>& blob);

inline constexpr std::size_t kHistoryColumns = 8;
[[nodiscard]] std::array<std::string, kHistoryColumns> historyColumns(const cat::RecordedEvent& r);

[[nodiscard]] std::string_view errorCodeName(uint16_t code) noexcept;
/// `<CodeName> (<code>) <detail>`
[[nodiscard]] std::string brokerFailure(uint16_t code, const std::string& detail);
[[nodiscard]] std::string requestIdHex(const cat::RequestId& id);

// ═══════════════════════════════════════════════════════════════════════════
// Preparing statements
// ═══════════════════════════════════════════════════════════════════════════
/// A statement ready to run: every refusal that needs no broker happened while preparing it.
struct PDefine      { ql::DefineTopicPlan plan; bool validate = false; };
struct PAlter       { ql::AlterTopicPlan plan; };
struct PAlterConfig { ql::AlterTopicConfigPlan plan; };
struct PDropTopic   { std::string name; };
struct PDropView    { std::string name; };
struct PHistory     { cat::HistoryFilter filter; std::string text; };
/// Authentication (Task 8): a security DDL — the plan holds the plaintext password, wiped when the statement is done.
struct PSecurity {
    mutable ql::SecurityPlan plan;
    PSecurity() = default;
    explicit PSecurity(ql::SecurityPlan p) : plan(std::move(p)) {}
    PSecurity(PSecurity&&) noexcept = default;
    PSecurity& operator=(PSecurity&&) noexcept = default;
    PSecurity(const PSecurity&) = delete;
    PSecurity& operator=(const PSecurity&) = delete;
    ~PSecurity() { wipe(); }
    void wipe() const noexcept {
        if (!plan.password.empty()) rune::client::detail::secureZero(plan.password.data(), plan.password.size());
    }
};
/// SHOW USERS / ROLES / GRANTS / SECURITY HISTORY — client-side replays of `__security`.
struct PSecShow {
    enum class Kind { Users, Roles, Grants, History } kind = Kind::Users;
    sec::GranteeKind who = sec::GranteeKind::User;       // Grants
    std::string name;                                    // Grants / History filter
    std::optional<sec::AsOf> at;                         // Grants
    sec::HistoryFilter filter;                           // History
    std::string text;
};
struct PAsOf        { cat::ObjectType kind = cat::ObjectType::Topic; std::string name; cat::AsOf at; std::string text; };
struct PDiff        { cat::AsOf a, b; std::string onlyTopic; bool restrictToTopic = false; std::string text; };
using Prepared = std::variant<PDefine, PAlter, PAlterConfig, PDropTopic, PDropView, PHistory, PAsOf, PDiff, PSecurity, PSecShow>;

/// Statement `n` of a script (1-based) sees this id: {hi, lo + n - 1}.
struct IdSeq {
    std::optional<cat::RequestId> base;
    [[nodiscard]] cat::RequestId at(std::size_t n) const { return {base->hi, base->lo + (n - 1)}; }
};

struct PrepareOptions {
    uint16_t expectedVersion = 0;       // every `alter topic … <ops>` asserts it (0 = no check)
    bool     validate        = false;   // every `topic …` is defined with validate = true
};

/// Parse-and-plan one statement.  nullopt + `why` when it is refused (versions beyond u16, a bad
/// setting, an undecodable time).
[[nodiscard]] std::optional<Prepared> prepare(const ql::Statement&, const PrepareOptions&, std::string& why);

void printPlan(const Prepared& p, const std::optional<cat::RequestId>& rid, std::ostream& out);

/// Reads the whole script text: a file, or stdin for "-".
[[nodiscard]] bool readScript(const std::string& path, std::string& text, std::ostream& err);

/// The text of each ';'-terminated statement of `src`, comments (`-- …`) and blanks between
/// statements dropped, ';' inside a '…' string kept (the rune-ql lexical rules).
[[nodiscard]] std::vector<std::string> splitStatements(std::string_view src);

// ═══════════════════════════════════════════════════════════════════════════
// Live execution
// ═══════════════════════════════════════════════════════════════════════════
#ifdef RUNE_CLI_LIVE_ENABLED

namespace rc  = rune::client;
namespace rpc = rune::protocol::client;

[[nodiscard]] std::string catalogReadFailure(const rc::CatalogReadError& e);
[[nodiscard]] std::string securityReadFailure(const rc::CatalogReadError& e);
[[nodiscard]] int64_t steadyNowMs();
/// Waits (bounded by kCatalogWait) until the client's catalog reader holds `rid`.  The seed broker
/// serves the catalog up to ITS OWN high watermark, which can trail the leader that answered the DDL.
[[nodiscard]] std::optional<cat::RequestRecord> awaitOwnWrite(rc::Client& c, const cat::RequestId& rid, std::string& why);

/// What LiveBroker::open needs.
struct LiveOptions {
    std::string endpoint;            // host:port or an aeron: channel
    std::string egress;              // "" = RUNE_EGRESS, else derived for UDP (localEgressFor)
    std::string streamIn, streamOut, dataDir;   // hidden test overrides ("" = default)
    bool        verbose = false;
    std::string user, passwordFile;  // credentials ("" = RUNE_USER / RUNE_PASSWORD_FILE)
};

/// The LiveOptions the `--broker` / `--cli-*` / `--user` / `--password-file` flags of `a` describe.
[[nodiscard]] LiveOptions liveOptionsOf(const support::ParsedArgs& a, const std::string& endpoint, bool verbose);

/// One connected Client for one broker.  Invoker mode: this thread is the duty thread, so the
/// DDL retry notes (--verbose) are read race-free.
class LiveBroker {
public:
    LiveBroker() = default;
    LiveBroker(const LiveBroker&) = delete;
    LiveBroker& operator=(const LiveBroker&) = delete;
    ~LiveBroker() { close(); }

    /// false (a message on err) when the endpoint is unusable or the connect fails / times out.
    bool open(const LiveOptions& o, std::ostream& err, bool withCredentials = true, bool quietFailure = false);
    void close() noexcept;

    [[nodiscard]] rc::Client& client() { return *client_; }
    /// The text of the last failed open() (the exception message), for callers that explain it themselves.
    [[nodiscard]] const std::string& lastFailure() const noexcept { return lastFailure_; }
    [[nodiscard]] std::chrono::milliseconds ddlTimeout() const noexcept { return ddlTimeout_; }

    /// --verbose: print (and clear) the Redirect / NotLeader retries the client followed since the last call.
    void flushVerbose(std::ostream& err);

private:
    support::ScopedDir          dir_;
    std::unique_ptr<rc::Client> client_;
    std::chrono::milliseconds   ddlTimeout_{15'000};
    bool                        verbose_ = false;
    std::string                 lastFailure_;
};

enum class OutputStyle : uint8_t { Ql, Console };

struct QlRun {
    LiveBroker& broker;
    std::ostream& out;
    std::ostream& err;
    bool script = false;   // -f: errors read "statement N: …"
    std::size_t index = 1;
    OutputStyle style = OutputStyle::Ql;
    uint16_t lastErrorCode = 0;              // the broker code of the last failure (0: local / transport)
    std::optional<cat::RequestId> lastDdl;   // the last DDL this run issued: reads after it must show it
    std::optional<cat::RequestId> lastSecurityDdl;   // a SHOW after a security DDL waits until the reader shows it

    bool exec(const Prepared& p, const std::string& text, const std::optional<cat::RequestId>& rid);

    bool fail(const std::string& msg);
    /// A broker refusal: records its code in lastErrorCode, then fail()s with `<Name> (<code>) <detail>`.
    bool failBroker(uint16_t code, const std::string& detail);

    bool run(const PDefine& s, const rc::DdlOptions& ddl);
    bool run(const PAlter& s, const rc::DdlOptions& ddl);
    bool run(const PAlterConfig& s, const rc::DdlOptions& ddl);
    bool run(const PDropTopic& s, const rc::DdlOptions& ddl);
    bool run(const PSecurity& s, const rc::DdlOptions& ddl);
    bool run(const PSecShow& s, const rc::DdlOptions&);
    bool run(const PDropView& s, const rc::DdlOptions& ddl);
    bool run(const PHistory& s, const rc::DdlOptions&);
    bool run(const PAsOf& s, const rc::DdlOptions&);
    bool run(const PDiff& s, const rc::DdlOptions&);

private:
    bool awaitLastDdl();
    bool awaitLastSecurityDdl();
    template <class F>
    bool guarded(F&& f) {
        try {
            return f();
        } catch (const std::exception& e) {
            return fail(std::string(e.what()));
        }
    }
};

#endif   // RUNE_CLI_LIVE_ENABLED

} // namespace rune::cli::engine
