// tools/rune_codec_gen_main.cpp
// CLI: rune_codec_gen <descriptor.blob> <out.h>
//
// Reads a serialized Descriptor blob, generates a C++ header via
// generateHeader(), and writes it to the given output path.

#include <rune/codec/Generator.h>
#include <rune/codec/Serialize.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: rune_codec_gen <descriptor.blob> <out.h>\n");
        return 1;
    }

    const char* blobPath = argv[1];
    const char* outPath  = argv[2];

    // ── Read the blob ────────────────────────────────────────────────────────
    std::ifstream fin(blobPath, std::ios::binary);
    if (!fin) {
        std::fprintf(stderr, "rune_codec_gen: cannot open '%s'\n", blobPath);
        return 1;
    }
    std::vector<uint8_t> blob(
        (std::istreambuf_iterator<char>(fin)),
        std::istreambuf_iterator<char>());
    fin.close();

    // ── Deserialize ──────────────────────────────────────────────────────────
    rune::codec::Descriptor d;
    try {
        d = rune::codec::deserialize(blob);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "rune_codec_gen: deserialize failed: %s\n", e.what());
        return 1;
    }

    // ── Generate header ──────────────────────────────────────────────────────
    const std::string hdr = rune::codec::generateHeader(d);

    // ── Write output ─────────────────────────────────────────────────────────
    std::ofstream fout(outPath, std::ios::trunc);
    if (!fout) {
        std::fprintf(stderr, "rune_codec_gen: cannot write '%s'\n", outPath);
        return 1;
    }
    fout << hdr;
    fout.close();

    return 0;
}
