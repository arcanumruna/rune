// Writes a golden log from the golden script and prints the per-offset digests as a C++ array for
// tests/security/SecurityGoldenPins.h:
//   make_security_golden <out.rsec>       -> golden/v1/basic.rsec        (kBasicV1Digests)
//   make_security_golden <out.rsec> v2    -> golden/v2/namespaces.rsec   (kNamespacesV2Digests)
// Run ONCE when a formatVer is introduced; never re-run for an existing one.
#include <rune/security/SecurityFile.h>
#include <rune/security/SecurityReplayer.h>
#include <rune/security/testing/SecurityGoldenScript.h>
#include <cstdio>
#include <fstream>
#include <string>
int main(int argc, char** argv) {
    if (argc != 2 && argc != 3) { std::fprintf(stderr, "usage: make_security_golden <out.rsec> [v1|v2]\n"); return 2; }
    const bool v2 = argc == 3 && std::string(argv[2]) == "v2";
    if (argc == 3 && !v2 && std::string(argv[2]) != "v1") { std::fprintf(stderr, "unknown version %s\n", argv[2]); return 2; }
    std::ofstream out(argv[1], std::ios::binary | std::ios::trunc);
    rune::security::writeRsecHeader(out);
    rune::security::SecurityReplayer r;
    std::printf("inline constexpr uint64_t %s[] = {\n", v2 ? "kNamespacesV2Digests" : "kBasicV1Digests");
    int64_t off = 0;
    const auto script = v2 ? rune::security::testing::goldenScriptV2() : rune::security::testing::goldenScriptV1();
    for (const auto& e : script) {
        const auto bytes = rune::security::encode(e);
        if (!bytes) {
            std::fprintf(stderr, "offset %lld: encode: %s\n", static_cast<long long>(off),
                         std::string(rune::security::encodeErrorName(bytes.error())).c_str());
            return 1;
        }
        rune::security::writeRsecRecord(out, off, *bytes);
        auto a = r.feed(off, *bytes);
        if (!a) { std::fprintf(stderr, "%s\n", rune::security::describe(a.error()).c_str()); return 1; }
        std::printf("    0x%016llxull,   // offset %lld %s\n", static_cast<unsigned long long>(r.state().digest()),
                    static_cast<long long>(off), std::string(rune::security::kindName(e.kind)).c_str());
        ++off;
    }
    std::printf("};\n");
    out.flush();
    if (!out) { std::fprintf(stderr, "write failed: %s\n", argv[1]); return 1; }
    return 0;
}
