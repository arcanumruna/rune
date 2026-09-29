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

#include "rune/ql/Parser.h"
#include "rune/ql/Backends.h"
#include "rune/tools/SchemaGen.h"

#include <rune/codec/Serialize.h>
#include <rune/codec/Generator.h>
#include <rune/codec/Fingerprint.h>

#include <iomanip>

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
        << "  rune schema apply <file|-> [--dry-run] [--validate] [--broker <ep>]\n"
        << "      Parse a .rql schema file; --dry-run prints descriptor (no broker).\n"
        << "      Without --dry-run: define topic on broker (live).\n"
        << "  rune schema get <topic> --broker <ep>\n"
        << "      Fetch topic schema from broker and print descriptor.\n"
        << "  rune schema list [--verbose] --broker <ep>\n"
        << "      List known topics on broker.\n"
        << "  rune schema generate --from-file <blob> --out <dir>\n"
        << "      Generate C++ header from a serialized descriptor blob.\n"
        << "  rune schema generate <topic> --out <dir> [--broker <ep>]\n"
        << "      Fetch topic schema from broker and generate C++ header (live).\n";
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
    auto result = rune::ql::parse(src);
    if (!result) {
        const auto& pe = result.error();
        err << path << ":" << pe.line << ":" << pe.col << ": " << pe.message << "\n";
        return 2;
    }

    // ── Dry run: emit descriptor as text, no broker contact ───────────────────
    if (dryRun) {
        rune::codec::Descriptor d = rune::ql::toDescriptor(*result);
        out << rune::codec::toText(d) << "\n";
        return 0;
    }

    // ── Live: define topic on broker ──────────────────────────────────────────
#ifdef RUNE_CLI_LIVE_ENABLED
    // Build the apply plan (parse → blob).
    rune::ql::DefineTopicPlan plan = rune::ql::applyPlan(*result);

    using namespace rune::client;
    Client client(buildClientConfig(args));
    try {
        auto connectFut = client.connect();
        if (connectFut.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
            err << "rune: connect timed out\n";
            client.close();
            return 1;
        }
        connectFut.get();

        auto defineFut = client.defineTopic(
            plan.name,
            plan.partitions,
            plan.keyField,
            validate,
            std::span<const uint8_t>(plan.descriptorBlob.data(),
                                     plan.descriptorBlob.size()));

        if (defineFut.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
            err << "rune: defineTopic timed out for topic: " << plan.name << "\n";
            client.close();
            return 1;
        }

        DefineTopicResult r = defineFut.get();
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
        auto connectFut = client.connect();
        if (connectFut.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
            err << "rune: connect timed out\n";
            client.close();
            return 1;
        }
        connectFut.get();

        auto schemaFut = client.getTopicSchema(topic);
        if (schemaFut.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
            err << "rune: getTopicSchema timed out for topic: " << topic << "\n";
            client.close();
            return 1;
        }
        TopicSchemaResult r = schemaFut.get();
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
        auto connectFut = client.connect();
        if (connectFut.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
            err << "rune: connect timed out\n";
            client.close();
            return 1;
        }
        connectFut.get();

        // list() uses the client-side registry of topics that were defined in
        // this session (populated from successful DefineTopicResponse frames).
        // For a full broker-side enumeration a ListTopics protocol message would
        // be needed — that is deferred to Task 10 (Docker e2e).
        const auto topics = client.listTopics();

        if (topics.empty()) {
            out << "(no topics applied in this session)\n";
        } else {
            for (const auto& [name, topicId] : topics) {
                out << name << " id=" << topicId;
                if (verbose) {
                    // Fetch schema for version + fingerprint.
                    auto schemaFut = client.getTopicSchema(name);
                    if (schemaFut.wait_for(std::chrono::seconds(10)) == std::future_status::ready) {
                        try {
                            TopicSchemaResult sr = schemaFut.get();
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
        auto connectFut = client.connect();
        if (connectFut.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
            err << "rune: connect timed out\n";
            client.close();
            return 1;
        }
        connectFut.get();

        auto schemaFut = client.getTopicSchema(topic);
        if (schemaFut.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
            err << "rune: getTopicSchema timed out for topic: " << topic << "\n";
            client.close();
            return 1;
        }
        TopicSchemaResult r = schemaFut.get();

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
    // args[0] == "schema", args[1] == subcommand
    if (args.size() < 2) {
        err << "rune schema: subcommand required (apply|get|list|generate)\n";
        printUsage(err);
        return 1;
    }

    const std::string& sub = args[1];
    // Pass sub-args with sub itself as args[0] for consistent indexing.
    std::vector<std::string> subArgs(args.begin() + 1, args.end());

    if (sub == "apply") {
        return cmdSchemaApply(subArgs, out, err);
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

    err << "rune: unknown command '" << cmd << "'\n";
    printUsage(err);
    return 1;
}

} // namespace rune::cli
