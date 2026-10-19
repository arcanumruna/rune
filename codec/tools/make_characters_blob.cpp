// tools/make_characters_blob.cpp
// One-shot helper: writes tests/generated/characters.blob
// Descriptor: id int64, userId int64 (key), name string, strength int32

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <cstdio>
#include <fstream>
#include <vector>

int main(int argc, char* argv[]) {
    const char* outPath = (argc >= 2) ? argv[1] : "characters.blob";

    auto d = rune::codec::DescriptorBuilder("characters")
        .add("id",       rune::codec::PrimType::Int64,  1, false)
        .add("userId",   rune::codec::PrimType::Int64,  1, true)
        .add("name",     rune::codec::PrimType::String, 1, false)
        .add("strength", rune::codec::PrimType::Int32,  1, false)
        .build();

    auto blob = rune::codec::serialize(d);

    std::ofstream f(outPath, std::ios::binary | std::ios::trunc);
    if (!f) {
        std::fprintf(stderr, "make_characters_blob: cannot open '%s'\n", outPath);
        return 1;
    }
    f.write(reinterpret_cast<const char*>(blob.data()),
            static_cast<std::streamsize>(blob.size()));
    return 0;
}
