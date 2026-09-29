// tools/make_fixedstr_sample_blob.cpp
// One-shot helper: writes tests/generated/fixedstr_sample.blob
// Descriptor: id int64, code FixedStr(8), name string

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <cstdio>
#include <fstream>
#include <vector>

int main(int argc, char* argv[]) {
    const char* outPath = (argc >= 2) ? argv[1] : "fixedstr_sample.blob";

    auto d = rune::codec::DescriptorBuilder("fixedstr_sample")
        .add("id",   rune::codec::PrimType::Int64,  1, false)
        .addFixedStr("code", 8, 1, false)
        .add("name", rune::codec::PrimType::String, 1, false)
        .build();

    auto blob = rune::codec::serialize(d);

    std::ofstream f(outPath, std::ios::binary | std::ios::trunc);
    if (!f) {
        std::fprintf(stderr, "make_fixedstr_sample_blob: cannot open '%s'\n", outPath);
        return 1;
    }
    f.write(reinterpret_cast<const char*>(blob.data()),
            static_cast<std::streamsize>(blob.size()));
    return 0;
}
