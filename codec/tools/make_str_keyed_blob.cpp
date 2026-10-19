// tools/make_str_keyed_blob.cpp
// One-shot helper: writes tests/generated/str_keyed.blob
// Descriptor: sku string (key), qty int32 — the var-len-key case of the
// Phase 2b Part C generated key() accessor (must be a BYTE span, never a
// std::string_view: rune::client::Key would hash the view object itself).

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <cstdio>
#include <fstream>
#include <vector>

int main(int argc, char* argv[]) {
    const char* outPath = (argc >= 2) ? argv[1] : "str_keyed.blob";

    auto d = rune::codec::DescriptorBuilder("str_keyed")
        .add("sku", rune::codec::PrimType::String, 1, true)
        .add("qty", rune::codec::PrimType::Int32,  1, false)
        .build();

    auto blob = rune::codec::serialize(d);

    std::ofstream f(outPath, std::ios::binary | std::ios::trunc);
    if (!f) {
        std::fprintf(stderr, "make_str_keyed_blob: cannot open '%s'\n", outPath);
        return 1;
    }
    f.write(reinterpret_cast<const char*>(blob.data()),
            static_cast<std::streamsize>(blob.size()));
    return 0;
}
