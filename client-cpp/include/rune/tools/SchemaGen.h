// include/rune/tools/SchemaGen.h
//
// Task 12 — rune_schema_gen tool: offline (--from-file) code generation helper.
//
// Exposes a testable free function that does NOT require a broker connection.
// The live-mode (generate <topic> --broker) path is implemented in main() of
// tools/rune_schema_gen.cpp and delegates to the same codegen step.
//
#pragma once
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Generator.h>
#include <rune/codec/Serialize.h>

namespace rune::tools {

/// Read a serialized descriptor blob from @p blobPath, generate the
/// corresponding C++ header via rune::codec::generateHeader(), and write it to
/// @p outDir/<name>.gen.h.
///
/// @param blobPath  Path to a file containing a blob produced by
///                  rune::codec::serialize().  Throws std::runtime_error if
///                  the file cannot be opened or is malformed.
/// @param outDir    Directory to write the generated header into.  Must exist
///                  or be createable.  Throws std::runtime_error on I/O error.
/// @returns         The absolute path of the written header file.
[[nodiscard]] inline std::string generateFromFile(const std::string& blobPath,
                                                   const std::string& outDir)
{
    // ── 1. Read blob bytes ──────────────────────────────────────────────────
    std::ifstream in(blobPath, std::ios::binary);
    if (!in) {
        throw std::runtime_error("rune_schema_gen: cannot open blob file: " + blobPath);
    }
    std::vector<uint8_t> bytes(
        (std::istreambuf_iterator<char>(in)),
        std::istreambuf_iterator<char>());
    if (bytes.empty()) {
        throw std::runtime_error("rune_schema_gen: blob file is empty: " + blobPath);
    }

    // ── 2. Deserialize → Descriptor ────────────────────────────────────────
    rune::codec::Descriptor d =
        rune::codec::deserialize(std::span<const uint8_t>(bytes.data(), bytes.size()));

    // ── 3. Generate C++ header ──────────────────────────────────────────────
    std::string hdr = rune::codec::generateHeader(d);

    // ── 4. Write <outDir>/<name>.gen.h ─────────────────────────────────────
    std::filesystem::path dir(outDir);
    std::filesystem::create_directories(dir);
    std::filesystem::path outPath = dir / (d.name + ".gen.h");

    std::ofstream out(outPath, std::ios::out | std::ios::trunc);
    if (!out) {
        throw std::runtime_error(
            "rune_schema_gen: cannot write output file: " + outPath.string());
    }
    out << hdr;
    if (!out) {
        throw std::runtime_error(
            "rune_schema_gen: write failed for: " + outPath.string());
    }

    return outPath.string();
}

} // namespace rune::tools
