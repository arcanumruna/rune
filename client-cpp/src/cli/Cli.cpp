// src/cli/Cli.cpp
//
// Task 8/9 — rune CLI implementation (unit-testable, no main).
//
// Implements rune::cli::run(args, out, err) which is called by:
//   - tools/rune_cli.cpp (main wrapper for the `rune` executable)
//   - tests/RuneCliOfflineTest.cpp (offline unit tests)
//   - tests/RuneCliLiveTest.cpp (live loopback tests)
//

#include "rune/cli/Cli.h"
#include "rune/cli/CatalogCommands.h"
#include "rune/cli/SecurityCommands.h"
#include "CliSupport.h"

#include "rune/ql/Parser.h"
#include "rune/ql/Backends.h"
#include "rune/tools/SchemaGen.h"

#include <rune/codec/Serialize.h>
#include <rune/codec/Generator.h>
#include <rune/codec/Fingerprint.h>

#include <charconv>
#include <iomanip>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

// Live broker path requires Aeron.
#ifdef RUNE_CLI_LIVE_ENABLED
#include <rune/client/Client.h>
#include <rune/protocol/client/Protocol.h>
#include <chrono>
#include <cstdlib>
#endif

namespace {

#ifdef RUNE_CLI_LIVE_ENABLED
// F8c C3: the CLI waits with the *AndWait conveniences (no futures).
constexpr std::chrono::seconds kCliWait{10};

bool isWaitTimeout(const std::exception& e) {
    return std::string_view(e.what()).find("timed out") != std::string_view::npos;
}

/// connectAndWait with the CLI's messages; closes the client on failure.
bool connectOrReport(rune::client::Client& client, std::ostream& err) {
    try {
        client.connectAndWait(kCliWait);
        rune::cli::support::printDevBannerIfAny(client, err);   // review B I6
        return true;
    } catch (const std::exception& e) {
        if (isWaitTimeout(e)) {
            err << "rune: connect timed out\n";
        } else {
            err << "rune error: " << e.what() << "\n";
        }
        client.close();
        return false;
    }
}
#endif


// ── Helpers ──────────────────────────────────────────────────────────────────

/// Return the value of --<flag> in args, or "" if not present.
std::string flagValue(const std::vector<std::string>& args, std::string_view flag) {
    for (std::size_t i = 0; i + 1 < args.size(); ++i) {
        if (args[i] == flag) return args[i + 1];
    }
    return {};
}

/// Return true if flag is present in args.
bool hasFlag(const std::vector<std::string>& args, std::string_view flag) {
    for (const auto& a : args) {
        if (a == flag) return true;
    }
    return false;
}

/// Parse --expected-version defensively: an empty string means "not given"
/// (expectedVersion stays 0 = no CAS). A non-numeric value, a value with
/// trailing garbage, or one outside uint16_t range (negative or > 65535) is
/// a clean CLI error — never an uncaught std::invalid_argument/out_of_range
/// out of std::stoi (which would std::terminate the process). Returns false
/// (and writes a message to err) on a bad value; callers must check the
/// return value BEFORE acting on `out` and before any --dry-run short-circuit,
/// so a malformed flag is reported the same way offline and live.
bool parseExpectedVersion(std::string_view flagStr, uint16_t& out, std::ostream& err) {
    if (flagStr.empty()) { out = 0; return true; }
    int value = 0;
    const auto res = std::from_chars(flagStr.data(), flagStr.data() + flagStr.size(), value);
    if (res.ec != std::errc{} || res.ptr != flagStr.data() + flagStr.size() ||
        value < 0 || value > 0xFFFF) {
        err << "error: --expected-version must be a non-negative integer (0..65535)\n";
        return false;
    }
    out = static_cast<uint16_t>(value);
    return true;
}

void printUsage(std::ostream& err) {
    err << "Usage:\n"
        << "  rune schema apply <file|-> [--dry-run] [--validate] [--broker <ep>]\n"
        << "      Parse a .rql schema file (a 'topic' or an 'alter topic' statement);\n"
        << "      --dry-run prints the descriptor / alter plan (no broker).\n"
        << "      Without --dry-run: define (or alter) the topic on broker (live).\n"
        << "  rune schema alter <file|-> [--dry-run] [--expected-version N] [--broker <ep>]\n"
        << "      Parse an 'alter topic' .rql file; --dry-run prints the parsed ops.\n"
        << "      Without --dry-run: alter topic on broker (live), gated by\n"
        << "      --expected-version (optimistic concurrency; default 0).\n"
        << "  rune schema get <topic> --broker <ep>\n"
        << "      Fetch topic schema from broker and print descriptor.\n"
        << "  rune schema list [--verbose] --broker <ep>\n"
        << "      List known topics on broker.\n"
        << "  rune schema generate --from-file <blob> --out <dir>\n"
        << "      Generate C++ header from a serialized descriptor blob.\n"
        << "  rune schema generate <topic> --out <dir> [--broker <ep>]\n"
        << "      Fetch topic schema from broker and generate C++ header (live).\n";
    rune::cli::printCatalogUsage(err);
    rune::cli::printSecurityUsage(err);
}

// ── Live ClientConfig builder (shared by apply/get/list) ─────────────────────
//
// Hidden flags (test-only, not advertised):
//   --cli-stream-in  <n>   override gatewayStreamId  (default 1)
//   --cli-stream-out <n>   override egressStreamId   (default 2)
//   --cli-data-dir   <p>   override dataDir          (default "")
//   --cli-egress     <ch>  override egressChannel    (default "aeron:ipc")
//                          Required for a real UDP broker: the broker publishes
//                          responses back to this channel, so with a UDP gateway
//                          it must be a client-reachable UDP endpoint, e.g.
//                          "aeron:udp?endpoint=rune-ql-probe:40500".  Also read
//                          from RUNE_EGRESS if the flag is absent.
//
#ifdef RUNE_CLI_LIVE_ENABLED
rune::client::ClientConfig buildClientConfig(const std::vector<std::string>& args) {
    rune::client::ClientConfig cfg;

    // Broker channel: --broker <ep> or RUNE_BROKER env.
    cfg.gatewayChannel = flagValue(args, "--broker");
    if (cfg.gatewayChannel.empty()) {
        if (const char* env = std::getenv("RUNE_BROKER"); env) cfg.gatewayChannel = env;
    }
    if (cfg.gatewayChannel.empty()) cfg.gatewayChannel = "aeron:ipc";

    // Egress channel: --cli-egress <ch> or RUNE_EGRESS env.  For a UDP gateway
    // this MUST be a client-reachable UDP endpoint so the broker can route
    // responses (define/getSchema/produce) back to this client; the default
    // aeron:ipc only works for a loopback/same-host gateway.
    std::string egress = flagValue(args, "--cli-egress");
    if (egress.empty()) {
        if (const char* env = std::getenv("RUNE_EGRESS"); env) egress = env;
    }
    if (!egress.empty()) cfg.egressChannel = egress;

    // Stream IDs: hidden test overrides.
    const std::string sIn  = flagValue(args, "--cli-stream-in");
    const std::string sOut = flagValue(args, "--cli-stream-out");
    if (!sIn.empty())  cfg.gatewayStreamId = std::stoi(sIn);
    if (!sOut.empty()) cfg.egressStreamId  = std::stoi(sOut);

    // Aeron data directory: hidden test override.
    const std::string dataDir = flagValue(args, "--cli-data-dir");
    if (!dataDir.empty()) cfg.dataDir = dataDir;

    // Review B I6: the credential flags are global (--user / --password-file, RUNE_USER / RUNE_PASSWORD_FILE).
    rune::cli::support::applyCredentials(cfg, flagValue(args, "--user"), flagValue(args, "--password-file"));

    return cfg;
}
#endif

// ── Read source file (shared by apply paths) ──────────────────────────────────

bool readSource(const std::string& path, std::string& src, std::ostream& err) {
    if (path == "-") {
        std::ostringstream buf;
        buf << std::cin.rdbuf();
        src = buf.str();
    } else {
        std::ifstream f(path);
        if (!f) {
            err << "rune: cannot open file: " << path << "\n";
            return false;
        }
        std::ostringstream buf;
        buf << f.rdbuf();
        src = buf.str();
    }
    return true;
}

// ── alter plan text (dry-run) ─────────────────────────────────────────────────
//
// There is no shared toText() for AlterTopicDef in rune-ql/rune-codec (only
// Descriptor has one) — this CLI-local formatter is deliberately minimal and
// is not a wire/API contract; it exists purely for human-readable --dry-run
// output of `apply`/`alter` on an 'alter topic' statement.

std::string_view alterOpKindName(rune::ql::AlterOpKind k) {
    switch (k) {
        case rune::ql::AlterOpKind::Add:              return "ADD";
        case rune::ql::AlterOpKind::Drop:             return "DROP";
        case rune::ql::AlterOpKind::Rename:           return "RENAME";
        case rune::ql::AlterOpKind::SetCompatibility: return "SET_COMPATIBILITY";
    }
    return "UNKNOWN";
}

std::string_view primTypeNameForCli(rune::codec::PrimType t) {
    switch (t) {
        case rune::codec::PrimType::Int32:    return "int32";
        case rune::codec::PrimType::Int64:    return "int64";
        case rune::codec::PrimType::UInt16:   return "uint16";
        case rune::codec::PrimType::UInt32:   return "uint32";
        case rune::codec::PrimType::UInt64:   return "uint64";
        case rune::codec::PrimType::Float64:  return "float64";
        case rune::codec::PrimType::Bool:     return "bool";
        case rune::codec::PrimType::Enum:     return "enum";
        case rune::codec::PrimType::String:   return "string";
        case rune::codec::PrimType::Bytes:    return "bytes";
        case rune::codec::PrimType::FixedStr: return "string";
    }
    return "unknown";
}

void printAlterPlanText(const rune::ql::AlterTopicDef& ad, std::ostream& out) {
    out << "alter topic " << ad.name << "\n";
    for (const auto& op : ad.ops) {
        out << "  " << alterOpKindName(op.kind);
        switch (op.kind) {
            case rune::ql::AlterOpKind::Add:
                out << " " << op.field.name << " " << primTypeNameForCli(op.field.type);
                if (op.field.type == rune::codec::PrimType::FixedStr) {
                    out << "(" << op.field.fixedLen << ")";
                }
                break;
            case rune::ql::AlterOpKind::Drop:
                out << " " << op.name;
                break;
            case rune::ql::AlterOpKind::Rename:
                out << " " << op.name << " -> " << op.newName;
                break;
            case rune::ql::AlterOpKind::SetCompatibility:
                out << " " << (op.mode == rune::ql::Compatibility::Backward ? "BACKWARD" : "FORWARD");
                break;
        }
        out << "\n";
    }
}

#ifdef RUNE_CLI_LIVE_ENABLED
// Shared AlterTopicResult reporting for both `apply` (on an alter file) and
// `alter`: surfaces SchemaIncompatible/SchemaConflict/UnknownTopic with clear
// messages, any other broker errorCode generically, and newVersion on success.
int reportAlterResult(const std::string& name, const rune::client::AlterTopicResult& r,
                      std::ostream& out, std::ostream& err)
{
    namespace rpc = rune::protocol::client;
    if (r.errorCode == static_cast<uint16_t>(rpc::ErrorCode::UnknownTopic)) {
        err << "rune: unknown topic '" << name << "'\n";
        return static_cast<int>(r.errorCode);
    }
    if (r.errorCode == static_cast<uint16_t>(rpc::ErrorCode::SchemaIncompatible)) {
        err << "rune: alter rejected for topic '" << name
            << "': proposed ops are incompatible with the existing schema/data\n";
        return static_cast<int>(r.errorCode);
    }
    if (r.errorCode == static_cast<uint16_t>(rpc::ErrorCode::SchemaConflict)) {
        err << "rune: alter rejected for topic '" << name
            << "': expected version is stale (schema changed since last read; "
               "re-fetch with `rune schema get` and retry)\n";
        return static_cast<int>(r.errorCode);
    }
    if (r.errorCode != 0) {
        err << "rune: broker error code " << r.errorCode
            << " for topic '" << name << "'\n";
        return static_cast<int>(r.errorCode);
    }

    out << "rune: topic '" << name << "' altered: newVersion=" << r.newVersion << "\n";
    return 0;
}
#endif

// ── schema apply ──────────────────────────────────────────────────────────────

int cmdSchemaApply(const std::vector<std::string>& args,
                   std::ostream& out,
                   std::ostream& err)
{
    // args[0] == "apply", args[1] == <file|->
    if (args.size() < 2) {
        err << "rune schema apply: <file|-> argument required\n";
        return 1;
    }

    const bool dryRun   = hasFlag(args, "--dry-run");
    const bool validate = hasFlag(args, "--validate");
    const std::string& path = args[1];

    // ── Read source ───────────────────────────────────────────────────────────
    std::string src;
    if (!readSource(path, src, err)) return 1;

    // ── Parse ─────────────────────────────────────────────────────────────────
    // Task 16 widened parse() to a variant: a 'topic' statement defines a new
    // topic (existing path below); an 'alter topic' statement routes through
    // applyAlterPlan()/alterTopicAndWait() instead.
    auto result = rune::ql::parse(src);
    if (!result) {
        const auto& pe = result.error();
        err << path << ":" << pe.line << ":" << pe.col << ": " << pe.message << "\n";
        return 2;
    }

    // Only 'topic' (typed, plain) and 'alter topic <ops>' are `schema apply` statements;
    // every other alternative (and any topic feature apply would silently drop) is refused
    // until `rune ql` (Task 15) runs them.
    const std::string_view unsupported = std::visit([](const auto& st) -> std::string_view {
        using T = std::decay_t<decltype(st)>;
        if constexpr (std::is_same_v<T, rune::ql::TopicDef>) {
            if (st.fields.empty())  return "an untyped topic";
            if (st.replicas != 0)   return "'replicas'";
            if (!st.with.empty())   return "'with (…)' settings";
            return {};
        } else if constexpr (std::is_same_v<T, rune::ql::AlterTopicDef>) {
            return {};
        } else {
            return "this statement";
        }
    }, *result);
    if (!unsupported.empty()) {
        err << path << ": " << unsupported << " is not supported by 'schema apply' — use 'rune ql'\n";
        return 2;
    }

    const bool isAlter = std::holds_alternative<rune::ql::AlterTopicDef>(*result);

    // ── Dry run: emit descriptor/plan as text, no broker contact ──────────────
    if (dryRun) {
        if (isAlter) {
            printAlterPlanText(std::get<rune::ql::AlterTopicDef>(*result), out);
        } else {
            rune::codec::Descriptor d = rune::ql::toDescriptor(std::get<rune::ql::TopicDef>(*result));
            out << rune::codec::toText(d) << "\n";
        }
        return 0;
    }

    // ── Live: define (or alter) topic on broker ───────────────────────────────
#ifdef RUNE_CLI_LIVE_ENABLED
    using namespace rune::client;
    Client client(buildClientConfig(args));
    try {
        if (!connectOrReport(client, err)) return 1;

        if (isAlter) {
            const auto& ad = std::get<rune::ql::AlterTopicDef>(*result);
            uint16_t expectedVersion = 0;
            if (!parseExpectedVersion(flagValue(args, "--expected-version"), expectedVersion, err)) {
                client.close();
                return 1;
            }

            rune::ql::AlterTopicPlan plan = rune::ql::applyAlterPlan(ad, expectedVersion);

            AlterTopicResult r;
            try {
                r = client.alterTopicAndWait(
                    plan.name, plan.expectedVersion,
                    std::span<const uint8_t>(plan.opsBlob.data(), plan.opsBlob.size()),
                    kCliWait);
            } catch (const std::exception& e) {
                if (isWaitTimeout(e)) {
                    err << "rune: alterTopic timed out for topic: " << plan.name << "\n";
                    client.close();
                    return 1;
                }
                throw;
            }
            client.close();
            return reportAlterResult(plan.name, r, out, err);
        }

        // Build the apply plan (parse → blob).
        rune::ql::DefineTopicPlan plan = rune::ql::applyPlan(std::get<rune::ql::TopicDef>(*result));

        const rune::client::Persistence persistence =
            plan.persistence == rune::ql::Persistence::Transient
                ? rune::client::Persistence::Transient
                : rune::client::Persistence::Disk;
        DefineTopicResult r;
        try {
            r = client.defineTopicAndWait(
                plan.name,
                plan.partitions,
                plan.keyField,
                validate,
                std::span<const uint8_t>(plan.descriptorBlob.data(),
                                         plan.descriptorBlob.size()),
                persistence,
                kCliWait);
        } catch (const std::exception& e) {
            if (isWaitTimeout(e)) {
                err << "rune: defineTopic timed out for topic: " << plan.name << "\n";
                client.close();
                return 1;
            }
            throw;
        }
        client.close();

        // TopicExists (25) is not an error from the CLI perspective.
        namespace rpc = rune::protocol::client;
        if (r.errorCode == static_cast<uint16_t>(rpc::ErrorCode::TopicExists)) {
            out << "rune: topic '" << plan.name << "' already exists (topicId="
                << r.topicId << ")\n";
            return 0;
        }
        if (r.errorCode != 0) {
            err << "rune: broker error code " << r.errorCode
                << " for topic '" << plan.name << "'\n";
            return static_cast<int>(r.errorCode);
        }

        out << "rune: topic '" << plan.name << "' defined: topicId="
            << r.topicId << " version=" << r.version << "\n";
        return 0;

    } catch (const std::exception& e) {
        err << "rune error: " << e.what() << "\n";
        client.close();
        return 1;
    }
#else
    (void)validate;
    err << "rune schema apply: live mode requires RUNE_CLI_LIVE_ENABLED "
           "and a real Aeron build. Use --dry-run for offline descriptor output.\n";
    return 1;
#endif
}

// ── schema alter ──────────────────────────────────────────────────────────────

int cmdSchemaAlter(const std::vector<std::string>& args,
                   std::ostream& out,
                   std::ostream& err)
{
    // args[0] == "alter", args[1] == <file|->
    if (args.size() < 2) {
        err << "rune schema alter: <file|-> argument required\n";
        return 1;
    }

    const bool dryRun = hasFlag(args, "--dry-run");
    const std::string& path = args[1];

    // ── Read source ───────────────────────────────────────────────────────────
    std::string src;
    if (!readSource(path, src, err)) return 1;

    // ── Parse ─────────────────────────────────────────────────────────────────
    auto result = rune::ql::parse(src);
    if (!result) {
        const auto& pe = result.error();
        err << path << ":" << pe.line << ":" << pe.col << ": " << pe.message << "\n";
        return 2;
    }

    if (!std::holds_alternative<rune::ql::AlterTopicDef>(*result)) {
        err << path << ": expected an 'alter topic <ops>' statement "
               "(use `rune schema apply` to define a topic, `rune ql` for other statements)\n";
        return 2;
    }
    const auto& ad = std::get<rune::ql::AlterTopicDef>(*result);

    // Validate --expected-version BEFORE the --dry-run short-circuit, so a
    // malformed value is a clean error offline too, not just live.
    uint16_t expectedVersion = 0;
    if (!parseExpectedVersion(flagValue(args, "--expected-version"), expectedVersion, err)) {
        return 1;
    }

    // ── Dry run: emit parsed ops as text, no broker contact ───────────────────
    if (dryRun) {
        printAlterPlanText(ad, out);
        return 0;
    }

    // ── Live: alter topic on broker ───────────────────────────────────────────
#ifdef RUNE_CLI_LIVE_ENABLED
    rune::ql::AlterTopicPlan plan = rune::ql::applyAlterPlan(ad, expectedVersion);

    using namespace rune::client;
    Client client(buildClientConfig(args));
    try {
        if (!connectOrReport(client, err)) return 1;

        AlterTopicResult r;
        try {
            r = client.alterTopicAndWait(
                plan.name, plan.expectedVersion,
                std::span<const uint8_t>(plan.opsBlob.data(), plan.opsBlob.size()),
                kCliWait);
        } catch (const std::exception& e) {
            if (isWaitTimeout(e)) {
                err << "rune: alterTopic timed out for topic: " << plan.name << "\n";
                client.close();
                return 1;
            }
            throw;
        }
        client.close();
        return reportAlterResult(plan.name, r, out, err);

    } catch (const std::exception& e) {
        err << "rune error: " << e.what() << "\n";
        client.close();
        return 1;
    }
#else
    err << "rune schema alter: live mode requires RUNE_CLI_LIVE_ENABLED "
           "and a real Aeron build. Use --dry-run for offline output.\n";
    return 1;
#endif
}

// ── schema get <topic> --broker <ep> ─────────────────────────────────────────

int cmdSchemaGet(const std::vector<std::string>& args,
                 std::ostream& out,
                 std::ostream& err)
{
    // args[0] == "get", args[1] == <topic>
    if (args.size() < 2) {
        err << "rune schema get: <topic> argument required\n";
        return 1;
    }
    const std::string& topic = args[1];

#ifdef RUNE_CLI_LIVE_ENABLED
    using namespace rune::client;
    Client client(buildClientConfig(args));
    try {
        if (!connectOrReport(client, err)) return 1;

        TopicSchemaResult r;
        try {
            r = client.getTopicSchemaAndWait(topic, kCliWait);
        } catch (const std::exception& e) {
            if (isWaitTimeout(e)) {
                err << "rune: getTopicSchema timed out for topic: " << topic << "\n";
                client.close();
                return 1;
            }
            throw;
        }
        client.close();

        namespace rpc = rune::protocol::client;
        if (r.errorCode == static_cast<uint16_t>(rpc::ErrorCode::UnknownTopic)) {
            err << "rune: unknown topic '" << topic << "'\n";
            return static_cast<int>(r.errorCode);
        }
        if (r.errorCode != 0) {
            err << "rune: broker error code " << r.errorCode
                << " for topic '" << topic << "'\n";
            return static_cast<int>(r.errorCode);
        }

        rune::codec::Descriptor d = rune::codec::deserialize(
            std::span<const uint8_t>(r.descriptorBlob.data(), r.descriptorBlob.size()));
        out << rune::codec::toText(d) << "\n";
        return 0;

    } catch (const std::exception& e) {
        err << "rune error: " << e.what() << "\n";
        client.close();
        return 1;
    }
#else
    (void)topic;
    err << "rune schema get: live mode requires RUNE_CLI_LIVE_ENABLED "
           "and a real Aeron build.\n";
    return 1;
#endif
}

// ── schema list [--verbose] --broker <ep> ────────────────────────────────────

int cmdSchemaList(const std::vector<std::string>& args,
                  std::ostream& out,
                  std::ostream& err)
{
    // args[0] == "list"
    const bool verbose = hasFlag(args, "--verbose");

#ifdef RUNE_CLI_LIVE_ENABLED
    using namespace rune::client;
    Client client(buildClientConfig(args));
    try {
        if (!connectOrReport(client, err)) return 1;

        // list() uses the client-side registry of topics that were defined in
        // this session (populated from successful DefineTopicResponse frames).
        // For a full broker-side enumeration a ListTopics protocol message would
        // be needed — that is deferred to Task 10 (Docker e2e).
        const auto topics = client.listTopics(kCliWait);

        if (topics.empty()) {
            out << "(no topics applied in this session)\n";
        } else {
            for (const auto& [name, topicId] : topics) {
                out << name << " id=" << topicId;
                if (verbose) {
                    // Fetch schema for version + fingerprint.
                    {
                        try {
                            TopicSchemaResult sr = client.getTopicSchemaAndWait(name, kCliWait);
                            if (sr.errorCode == 0 && !sr.descriptorBlob.empty()) {
                                rune::codec::Descriptor d = rune::codec::deserialize(
                                    std::span<const uint8_t>(sr.descriptorBlob.data(),
                                                             sr.descriptorBlob.size()));
                                const uint64_t fp = rune::codec::fingerprint(d);
                                out << " version=" << sr.version
                                    << " fp=0x"
                                    << std::hex << std::setw(16)
                                    << std::setfill('0') << fp << std::dec;
                            }
                        } catch (...) {}
                    }
                }
                out << "\n";
            }
        }

        client.close();
        return 0;

    } catch (const std::exception& e) {
        err << "rune error: " << e.what() << "\n";
        client.close();
        return 1;
    }
#else
    (void)verbose;
    err << "rune schema list: live mode requires RUNE_CLI_LIVE_ENABLED "
           "and a real Aeron build.\n";
    return 1;
#endif
}

// ── schema generate ───────────────────────────────────────────────────────────

int cmdSchemaGenerate(const std::vector<std::string>& args,
                      std::ostream& out,
                      std::ostream& err)
{
    // args[0] == "generate"
    const std::string fromFile = flagValue(args, "--from-file");
    const std::string outDir   = flagValue(args, "--out");

    if (outDir.empty()) {
        err << "rune schema generate: --out <dir> is required\n";
        return 1;
    }

    // ── Offline: --from-file ──────────────────────────────────────────────────
    if (!fromFile.empty()) {
        try {
            auto written = rune::tools::generateFromFile(fromFile, outDir);
            out << "rune: wrote " << written << "\n";
            return 0;
        } catch (const std::exception& e) {
            err << "rune schema generate error: " << e.what() << "\n";
            return 1;
        }
    }

    // ── Live: generate <topic> --broker <ep> ─────────────────────────────────
    if (args.size() < 2) {
        err << "rune schema generate: <topic> or --from-file required\n";
        return 1;
    }
    const std::string topic = args[1]; // args[0] == "generate"

#ifdef RUNE_CLI_LIVE_ENABLED
    using namespace rune::client;
    Client client(buildClientConfig(args));
    try {
        if (!connectOrReport(client, err)) return 1;

        TopicSchemaResult r;
        try {
            r = client.getTopicSchemaAndWait(topic, kCliWait);
        } catch (const std::exception& e) {
            if (isWaitTimeout(e)) {
                err << "rune: getTopicSchema timed out for topic: " << topic << "\n";
                client.close();
                return 1;
            }
            throw;
        }

        if (r.errorCode != 0) {
            err << "rune: broker error code " << r.errorCode
                << " for topic '" << topic << "'\n";
            return 1;
        }

        rune::codec::Descriptor d = rune::codec::deserialize(
            std::span<const uint8_t>(r.descriptorBlob.data(), r.descriptorBlob.size()));
        std::string hdr = rune::codec::generateHeader(d);

        std::filesystem::path dir(outDir);
        std::filesystem::create_directories(dir);
        std::filesystem::path outPath = dir / (d.name + ".gen.h");

        std::ofstream o(outPath, std::ios::out | std::ios::trunc);
        if (!o) {
            err << "rune: cannot write: " << outPath << "\n";
            client.close();
            return 1;
        }
        o << hdr;
        o.flush();

        out << "rune: wrote " << outPath.string() << "\n";
        client.close();
        return 0;

    } catch (const std::exception& e) {
        err << "rune error: " << e.what() << "\n";
        client.close();
        return 1;
    }
#else
    (void)topic;
    err << "rune schema generate: live mode requires RUNE_CLI_LIVE_ENABLED "
           "and a real Aeron build.\n"
           "Use --from-file for offline header generation.\n";
    return 1;
#endif
}

// ── schema dispatch ───────────────────────────────────────────────────────────

int cmdSchema(const std::vector<std::string>& args,
              std::ostream& out,
              std::ostream& err)
{
    // Review B I6: the same credential rules as every other command (no password in argv / env).
    if (!rune::cli::support::refusePasswordArgs("schema", args, err)) return 2;
    // args[0] == "schema", args[1] == subcommand
    if (args.size() < 2) {
        err << "rune schema: subcommand required (apply|alter|get|list|generate)\n";
        printUsage(err);
        return 1;
    }

    const std::string& sub = args[1];
    // Pass sub-args with sub itself as args[0] for consistent indexing.
    std::vector<std::string> subArgs(args.begin() + 1, args.end());

    // The Client constructor reads a --password-file: an unreadable file is a clean CLI error, not an exception.
    try {
    if (sub == "apply") {
        return cmdSchemaApply(subArgs, out, err);
    }
    if (sub == "alter") {
        return cmdSchemaAlter(subArgs, out, err);
    }
    if (sub == "get") {
        return cmdSchemaGet(subArgs, out, err);
    }
    if (sub == "list") {
        return cmdSchemaList(subArgs, out, err);
    }
    if (sub == "generate") {
        return cmdSchemaGenerate(subArgs, out, err);
    }
    } catch (const std::exception& e) {
        err << "rune error: " << e.what() << "\n";
        return 1;
    }

    err << "rune schema: unknown subcommand '" << sub << "'\n";
    printUsage(err);
    return 1;
}

} // anonymous namespace

namespace rune::cli {

int run(std::vector<std::string> args, std::ostream& out, std::ostream& err)
{
    if (args.empty()) {
        printUsage(err);
        return 1;
    }

    const std::string& cmd = args[0];

    if (cmd == "schema") {
        return cmdSchema(args, out, err);
    }
    if (cmd == "ql") {
        return cmdQl(std::vector<std::string>(args.begin() + 1, args.end()), out, err);
    }
    if (cmd == "catalog") {
        return cmdCatalog(std::vector<std::string>(args.begin() + 1, args.end()), out, err);
    }
    if (cmd == "security") {
        return cmdSecurity(std::vector<std::string>(args.begin() + 1, args.end()), out, err);
    }

    err << "rune: unknown command '" << cmd << "'\n";
    printUsage(err);
    return 1;
}

} // namespace rune::cli
