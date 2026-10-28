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
#include "rune/cli/Console.h"
#include "rune/cli/CatalogCommands.h"
#include "rune/cli/NamespaceCommands.h"
#include "rune/cli/SecurityCommands.h"
#include "CliSupport.h"

#include "rune/tools/SchemaGen.h"

#include <rune/codec/Serialize.h>
#include <rune/codec/Generator.h>
#include <rune/codec/Fingerprint.h>

#include <iomanip>
#include <iostream>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
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

void printUsage(std::ostream& err) {
    err << "Usage:\n"
        << "  rune console [--broker <ep>] [--cli-egress <ch>] [-f <script.rql|->] [--dry-run] [--validate]\n"
        << "               [--expected-version N] [--request-id <32 hex>] [--verbose] [--user <u>] [--password-file <p>]\n"
        << "               [--tls-ca <pem>] [--tls-host <name>] [--insecure-plaintext]\n"
        << "      Run Rune QL statements interactively (prompt `rune> `) or from a script (-f; stops at\n"
        << "      the first error). --dry-run prints each statement's plan (no broker).\n"
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
    rune::cli::printNamespaceUsage(err);
}

// ── Live ClientConfig builder (shared by get/list/generate) ─────────────────────
//
// Hidden flags (test-only, not advertised):
//   --cli-stream-in  <n>   override gatewayStreamId  (default 1)
//   --cli-stream-out <n>   override egressStreamId   (default 2)
//   --cli-data-dir   <p>   override dataDir          (default and "": a private scratch directory,
//                          removed when the command ends)
//   --cli-egress     <ch>  override egressChannel    (default "aeron:ipc")
//                          Required for a real UDP broker: the broker publishes
//                          responses back to this channel, so with a UDP gateway
//                          it must be a client-reachable UDP endpoint, e.g.
//                          "aeron:udp?endpoint=rune-ql-probe:40500".  Also read
//                          from RUNE_EGRESS if the flag is absent.
//
#ifdef RUNE_CLI_LIVE_ENABLED
// `dir` receives the embedded driver's scratch directory when --cli-data-dir is absent; it must outlive the Client.
rune::client::ClientConfig buildClientConfig(const std::vector<std::string>& args, rune::cli::support::ScopedDir& dir) {
    rune::client::ClientConfig cfg;

    // Broker channel: --broker <ep> or RUNE_BROKER env.
    cfg.gatewayChannel = flagValue(args, "--broker");
    if (cfg.gatewayChannel.empty()) {
        if (const char* env = std::getenv("RUNE_BROKER"); env) cfg.gatewayChannel = env;
    }
    cfg.gatewayChannel = cfg.gatewayChannel.empty() ? "aeron:ipc" : rune::cli::support::toChannel(cfg.gatewayChannel);

    // Egress channel: --cli-egress <ch> or RUNE_EGRESS env.  For a UDP gateway
    // this MUST be a client-reachable UDP endpoint so the broker can route
    // responses (define/getSchema/produce) back to this client; the default
    // aeron:ipc only works for a loopback/same-host gateway.
    std::string egress = flagValue(args, "--cli-egress");
    if (egress.empty()) {
        if (const char* env = std::getenv("RUNE_EGRESS"); env) egress = env;
    }
    if (egress.empty()) {   // a UDP gateway answers on the local address that reaches it (as the console does)
        if (const std::string ep = rune::cli::support::udpEndpointOf(cfg.gatewayChannel); !ep.empty()) {
            std::string why;
            const auto local = rune::cli::support::localEgressFor(ep, why);
            if (!local) throw std::invalid_argument(why + "; set --cli-egress (or RUNE_EGRESS)");
            egress = *local;
        }
    }
    if (!egress.empty()) cfg.egressChannel = egress;

    // Stream IDs: hidden test overrides.
    const std::string sIn  = flagValue(args, "--cli-stream-in");
    const std::string sOut = flagValue(args, "--cli-stream-out");
    if (!sIn.empty())  cfg.gatewayStreamId = std::stoi(sIn);
    if (!sOut.empty()) cfg.egressStreamId  = std::stoi(sOut);

    // Aeron data directory: hidden test override, else a private scratch directory removed with `dir`.
    cfg.dataDir = rune::cli::support::resolveDataDir(flagValue(args, "--cli-data-dir"), dir);   // empty = scratch (#315)

    // Review B I6: the credential flags are global (--user / --password-file, RUNE_USER / RUNE_PASSWORD_FILE).
    rune::cli::support::applyCredentials(cfg, flagValue(args, "--user"), flagValue(args, "--password-file"));
    // Issue #13: TLS (--tls-ca / --tls-host / --insecure-plaintext); a usage error throws (cmdSchema reports it).
    rune::cli::support::applyTls(cfg, flagValue(args, "--tls-ca"), flagValue(args, "--tls-host"),
                                 hasFlag(args, "--insecure-plaintext"));

    return cfg;
}
#endif

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
    rune::cli::support::ScopedDir scratch;
    Client client(buildClientConfig(args, scratch));
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
    rune::cli::support::ScopedDir scratch;
    Client client(buildClientConfig(args, scratch));
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
    rune::cli::support::ScopedDir scratch;
    Client client(buildClientConfig(args, scratch));
    try {
        if (!connectOrReport(client, err)) return 1;

        TopicSchemaResult r;
        const auto lineageStarted = std::chrono::steady_clock::now();
        try {
            // #418: the topic's LINEAGE (evolving descriptor), so the generated type places records of every
            // version up to its own; a materialized descriptor would hide every DROP before it.
            r = client.getTopicSchemaAndWait(topic, rune::protocol::client::kSchemaLineageFingerprint, kCliWait);
        } catch (const std::exception& e) {
            if (isWaitTimeout(e)) {
                err << "rune: getTopicSchema timed out for topic: " << topic << "\n";
                client.close();
                return 1;
            }
            throw;
        }

        if (r.errorCode != 0) {
            err << "rune: " << client.lineageFailure(
                       topic, r.errorCode, kCliWait - (std::chrono::steady_clock::now() - lineageStarted)) << "\n";   // Issue #435
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
        err << "rune schema: subcommand required (get|list|generate)\n";
        printUsage(err);
        return 1;
    }

    const std::string& sub = args[1];
    // Pass sub-args with sub itself as args[0] for consistent indexing.
    std::vector<std::string> subArgs(args.begin() + 1, args.end());

    // The Client constructor reads a --password-file: an unreadable file is a clean CLI error, not an exception.
    try {
    if (sub == "apply" || sub == "alter") {
        err << "rune schema " << sub << " was removed: use `rune console -f <file.rql>` (or `rune ql -f`)\n";
        return 2;
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
    return run(std::move(args), std::cin, out, err);
}

int run(std::vector<std::string> args, std::istream& in, std::ostream& out, std::ostream& err)
{
    if (args.empty()) {
        printUsage(err);
        return 1;
    }

    const std::string& cmd = args[0];

    try {   // #680: nothing escapes the CLI as an uncaught exception (the commands' secret guards unwind first)
    if (cmd == "console") {
        return console::cmdConsole(args, in, out, err);
    }
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
    if (cmd == "namespace") {
        return rune::cli::cmdNamespace(std::vector<std::string>(args.begin() + 1, args.end()), out, err);
    }

    err << "rune: unknown command '" << cmd << "'\n";
    printUsage(err);
    return 1;
    } catch (const std::exception& e) {
        err << "rune: " << e.what() << "\n";
        return 1;
    }
}

} // namespace rune::cli
