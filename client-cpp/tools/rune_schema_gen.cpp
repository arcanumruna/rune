// tools/rune_schema_gen.cpp
//
// Task 12 — rune_schema_gen CLI tool.
//
// Usage:
//   rune_schema_gen generate <topic> --out <dir> [--broker <endpoint>]
//       Connect to the broker, fetch the topic descriptor, generate a C++
//       header and write it to <dir>/<topic>.gen.h.
//
//   rune_schema_gen generate --from-file <blob> --out <dir>
//       Offline mode: read the serialized descriptor blob from <blob>, generate
//       a C++ header derived from the embedded schema name, write it to
//       <dir>/<name>.gen.h.  No broker connection required.
//
// The offline path calls rune::tools::generateFromFile() which is also
// exercised directly in the unit tests (SchemaGenToolTest.cpp).
//
// Exit codes: 0 = success, 1 = usage / runtime error.
//

#include "rune/tools/SchemaGen.h"

#include <iostream>
#include <string>
#include <string_view>

// The live-broker path requires Aeron (and is exercised by Task 13 e2e).
// Guard it so the executable can still be built for header-only targets that
// don't have aeron available (the --from-file path compiles unconditionally).
#ifdef RUNE_SCHEMA_GEN_LIVE_ENABLED
#include <rune/client/Client.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Generator.h>
#include <filesystem>
#include <fstream>
#include <chrono>
#endif

namespace {

void printUsage(const char* argv0) {
    std::cerr
        << "Usage:\n"
        << "  " << argv0 << " generate <topic> --out <dir> [--broker <endpoint>]\n"
        << "  " << argv0 << " generate --from-file <blob> --out <dir>\n"
        << "\n"
        << "Options:\n"
        << "  <topic>           Topic name to fetch schema for (live mode).\n"
        << "  --from-file <f>   Path to a serialized descriptor blob (offline mode).\n"
        << "  --out <dir>       Output directory for the generated header.\n"
        << "  --broker <ep>     Broker endpoint (default: aeron:ipc, live mode only).\n";
}

// ---------------------------------------------------------------------------
// Argument parsing helpers
// ---------------------------------------------------------------------------

/// Return the value of --<flag> in argv, or "" if not present.
std::string flagValue(int argc, char** argv, std::string_view flag) {
    for (int i = 1; i < argc - 1; ++i) {
        if (argv[i] == flag) {
            return argv[i + 1];
        }
    }
    return {};
}

bool hasFlag(int argc, char** argv, std::string_view flag) {
    for (int i = 1; i < argc; ++i) {
        if (argv[i] == flag) return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// Offline (--from-file) entry point
// ---------------------------------------------------------------------------

int runFromFile(const std::string& blobPath, const std::string& outDir) {
    try {
        auto written = rune::tools::generateFromFile(blobPath, outDir);
        std::cout << "rune_schema_gen: wrote " << written << "\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "rune_schema_gen error: " << e.what() << "\n";
        return 1;
    }
}

// ---------------------------------------------------------------------------
// Live (broker) entry point
// ---------------------------------------------------------------------------

#ifdef RUNE_SCHEMA_GEN_LIVE_ENABLED
int runLive(const std::string& topic,
            const std::string& outDir,
            const std::string& broker) {
    using namespace rune::client;

    ClientConfig cfg;
    cfg.gatewayChannel = broker.empty() ? "aeron:ipc" : broker;

    Client client(cfg);
    try {
        // Connect with a 10-second timeout.
        auto connectFut = client.connect();
        if (connectFut.wait_for(std::chrono::seconds(10)) !=
                std::future_status::ready) {
            std::cerr << "rune_schema_gen: connect timed out to " << cfg.gatewayChannel << "\n";
            return 1;
        }
        connectFut.get(); // throws on error

        auto schemaFut = client.getTopicSchema(topic);
        if (schemaFut.wait_for(std::chrono::seconds(10)) !=
                std::future_status::ready) {
            std::cerr << "rune_schema_gen: getTopicSchema timed out for topic: " << topic << "\n";
            return 1;
        }
        TopicSchemaResult r = schemaFut.get();

        if (r.errorCode != 0) {
            std::cerr << "rune_schema_gen: broker returned error code "
                      << r.errorCode << " for topic '" << topic << "'\n";
            return 1;
        }

        // Deserialize + generate header.
        rune::codec::Descriptor d = rune::codec::deserialize(
            std::span<const uint8_t>(r.descriptorBlob.data(),
                                     r.descriptorBlob.size()));
        std::string hdr = rune::codec::generateHeader(d);

        // Write <outDir>/<topic>.gen.h.
        std::filesystem::path dir(outDir);
        std::filesystem::create_directories(dir);
        std::filesystem::path outPath = dir / (d.name + ".gen.h");

        std::ofstream out(outPath, std::ios::out | std::ios::trunc);
        if (!out) {
            std::cerr << "rune_schema_gen: cannot write: " << outPath << "\n";
            return 1;
        }
        out << hdr;

        std::cout << "rune_schema_gen: wrote " << outPath.string() << "\n";
        client.close();
        return 0;

    } catch (const std::exception& e) {
        std::cerr << "rune_schema_gen error: " << e.what() << "\n";
        client.close();
        return 1;
    }
}
#endif // RUNE_SCHEMA_GEN_LIVE_ENABLED

} // anonymous namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        printUsage(argv[0]);
        return 1;
    }

    std::string_view cmd(argv[1]);
    if (cmd != "generate") {
        std::cerr << "rune_schema_gen: unknown command '" << cmd << "'\n";
        printUsage(argv[0]);
        return 1;
    }

    const std::string outDir   = flagValue(argc, argv, "--out");
    const std::string fromFile = flagValue(argc, argv, "--from-file");

    if (outDir.empty()) {
        std::cerr << "rune_schema_gen: --out <dir> is required\n";
        printUsage(argv[0]);
        return 1;
    }

    // ── Offline mode: --from-file ─────────────────────────────────────────
    if (!fromFile.empty()) {
        return runFromFile(fromFile, outDir);
    }

    // ── Live mode: generate <topic> --broker <ep> ─────────────────────────
    // argv[2] is the topic name when not --from-file.
    if (argc < 3) {
        std::cerr << "rune_schema_gen: generate requires either a <topic> or --from-file\n";
        printUsage(argv[0]);
        return 1;
    }

    const std::string topic  = argv[2];
    const std::string broker = flagValue(argc, argv, "--broker");

    (void)broker; // suppress unused warning when live mode disabled

#ifdef RUNE_SCHEMA_GEN_LIVE_ENABLED
    return runLive(topic, outDir, broker);
#else
    std::cerr << "rune_schema_gen: live mode (generate <topic>) requires "
                 "RUNE_SCHEMA_GEN_LIVE_ENABLED and a real Aeron build.\n"
                 "Use --from-file for offline header generation.\n";
    return 1;
#endif
}
