// Writes a golden log from GoldenScript and prints the per-offset digests as a C++ array for
// tests/catalog/CatalogGoldenPins.h:
//   make_catalog_golden <out.rcat>        -> golden/v1/basic.rcat        (kBasicV1Digests)
//   make_catalog_golden <out.rcat> v2     -> golden/v2/namespaces.rcat   (kNamespacesV2Digests)
// Run ONCE when a formatVer is introduced; never re-run for an existing one.
#include <rune/catalog/CatalogFile.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <cstdio>
#include <fstream>
#include <string>
int main(int argc, char** argv) {
    if (argc != 2 && argc != 3) { std::fprintf(stderr, "usage: make_catalog_golden <out.rcat> [v1|v2]\n"); return 2; }
    const bool v2 = argc == 3 && std::string(argv[2]) == "v2";
    if (argc == 3 && !v2 && std::string(argv[2]) != "v1") { std::fprintf(stderr, "unknown version %s\n", argv[2]); return 2; }
    std::ofstream out(argv[1], std::ios::binary | std::ios::trunc);
    rune::catalog::writeRcatHeader(out);
    rune::catalog::CatalogReplayer r;
    std::printf("inline constexpr uint64_t %s[] = {\n", v2 ? "kNamespacesV2Digests" : "kBasicV1Digests");
    int64_t off = 0;
    const auto script = v2 ? rune::catalog::testing::goldenScriptV2() : rune::catalog::testing::goldenScriptV1();
    for (const auto& e : script) {
        const auto bytes = rune::catalog::encode(e);
        if (!bytes) {
            std::fprintf(stderr, "offset %lld: encode: %s\n", static_cast<long long>(off),
                         std::string(rune::catalog::encodeErrorName(bytes.error())).c_str());
            return 1;
        }
        rune::catalog::writeRcatRecord(out, off, *bytes);
        auto a = r.feed(off, *bytes);
        if (!a) { std::fprintf(stderr, "%s\n", rune::catalog::describe(a.error()).c_str()); return 1; }
        std::printf("    0x%016llxull,   // offset %lld %s\n", static_cast<unsigned long long>(r.state().digest()),
                    static_cast<long long>(off), std::string(rune::catalog::kindName(e.kind)).c_str());
        ++off;
    }
    std::printf("};\n");
    out.flush();
    if (!out) { std::fprintf(stderr, "write failed: %s\n", argv[1]); return 1; }
    return 0;
}
