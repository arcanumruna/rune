// src/cli/Cli.cpp
//
// Task 8 — rune CLI implementation (unit-testable, no main).
//
// Implements rune::cli::run(args, out, err) which is called by:
//   - tools/rune_cli.cpp (main wrapper for the `rune` executable)
//   - tests/RuneCliOfflineTest.cpp (offline unit tests)
//

#include "rune/cli/Cli.h"

#include "rune/ql/Parser.h"
#include "rune/ql/Backends.h"
#include "rune/tools/SchemaGen.h"

#include <rune/codec/Serialize.h>
#include <rune/codec/Generator.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

// Live broker path requires Aeron.
#ifdef RUNE_CLI_LIVE_ENABLED
#include <rune/client/Client.h>
#include <chrono>
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
        << "  rune schema apply <file|-> --dry-run\n"
        << "      Parse a .rql schema file and print the descriptor (no broker).\n"
        << "  rune schema generate --from-file <blob> --out <dir>\n"
        << "      Generate C++ header from a serialized descriptor blob.\n"
        << "  rune schema generate <topic> --out <dir> [--broker <ep>]\n"
        << "      Fetch topic schema from broker and generate C++ header (live).\n";
}

// ── schema apply --dry-run ────────────────────────────────────────────────────

int cmdSchemaApply(const std::vector<std::string>& args,
                   std::ostream& out,
                   std::ostream& err)
{
    // args[0] == "apply", args[1] == <file|->
    if (args.size() < 2) {
        err << "rune schema apply: <file|-> argument required\n";
        return 1;
    }

    const bool dryRun = hasFlag(args, "--dry-run");
    if (!dryRun) {
        err << "rune schema apply: only --dry-run is supported in this build\n";
        return 1;
    }

    const std::string& path = args[1];

    // ── Read source ───────────────────────────────────────────────────────────
    std::string src;
    if (path == "-") {
        std::ostringstream buf;
        buf << std::cin.rdbuf();
        src = buf.str();
    } else {
        std::ifstream f(path);
        if (!f) {
            err << "rune: cannot open file: " << path << "\n";
            return 1;
        }
        std::ostringstream buf;
        buf << f.rdbuf();
        src = buf.str();
    }

    // ── Parse ─────────────────────────────────────────────────────────────────
    auto result = rune::ql::parse(src);
    if (!result) {
        const auto& pe = result.error();
        err << path << ":" << pe.line << ":" << pe.col << ": " << pe.message << "\n";
        return 2;
    }

    // ── Dry run: emit descriptor as text ──────────────────────────────────────
    rune::codec::Descriptor d = rune::ql::toDescriptor(*result);
    out << rune::codec::toText(d) << "\n";
    return 0;
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
    std::string broker = flagValue(args, "--broker");
    if (broker.empty()) {
        const char* env = std::getenv("RUNE_BROKER");
        if (env) broker = env;
    }
    if (broker.empty()) broker = "aeron:ipc";

    using namespace rune::client;
    ClientConfig cfg;
    cfg.gatewayChannel = broker;

    Client client(cfg);
    try {
        auto connectFut = client.connect();
        if (connectFut.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
            err << "rune: connect timed out to " << broker << "\n";
            return 1;
        }
        connectFut.get();

        auto schemaFut = client.getTopicSchema(topic);
        if (schemaFut.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
            err << "rune: getTopicSchema timed out for topic: " << topic << "\n";
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
        err << "rune schema: subcommand required (apply|generate)\n";
        printUsage(err);
        return 1;
    }

    const std::string& sub = args[1];
    // Pass sub-args with sub itself as args[0] for consistent indexing.
    std::vector<std::string> subArgs(args.begin() + 1, args.end());

    if (sub == "apply") {
        return cmdSchemaApply(subArgs, out, err);
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
