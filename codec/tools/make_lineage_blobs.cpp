// tools/make_lineage_blobs.cpp — #418 fixture: writes <outDir>/<base>_v<N>.blob, the EVOLVING descriptor of the
// evo_mix and evo_fix lineages (tests/LineageFixtures.h) at every version 1..4 — what `rune schema generate`
// fetches from the broker at that version.  rune_codec_gen turns each into the type of that version.
#include "../tests/LineageFixtures.h"
#include <rune/codec/Serialize.h>

#include <cstdio>
#include <fstream>
#include <string>

int main(int argc, char* argv[]) {
    const std::string outDir = (argc >= 2) ? argv[1] : ".";
    using rune::codec::testing::Lineage;
    for (const Lineage l : {Lineage::Mix, Lineage::Fix}) {
        for (uint16_t v = 1; v <= 4; ++v) {
            const auto d = rune::codec::testing::evolvingAt(l, v);
            const auto blob = rune::codec::serialize(d);
            const std::string path = outDir + "/" + d.name + ".blob";
            std::ofstream f(path, std::ios::binary | std::ios::trunc);
            if (!f) {
                std::fprintf(stderr, "make_lineage_blobs: cannot open '%s'\n", path.c_str());
                return 1;
            }
            f.write(reinterpret_cast<const char*>(blob.data()), static_cast<std::streamsize>(blob.size()));
        }
    }
    return 0;
}
