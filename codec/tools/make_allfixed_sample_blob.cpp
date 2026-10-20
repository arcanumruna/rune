// tools/make_allfixed_sample_blob.cpp — Phase 2b Part B fixture: writes the
// allfixed_sample descriptor blob (every fixed field type, odd offsets, a key
// field) the flyweight round-trip test compiles its generated header from.
//   id Int64 KEY @0 | a Int32 @8 | b UInt16 @12 | c UInt32 @14 | d UInt64 @18 |
//   e Float64 @26 | f Bool @34 | g Enum{1,2,3} @35 | code string(6) @39
//   -> blockLength 45, kEncodedLength 49
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <cstdio>
#include <fstream>

int main(int argc, char* argv[]) {
    const char* outPath = (argc >= 2) ? argv[1] : "allfixed_sample.blob";
    using rune::codec::PrimType;
    const auto d = rune::codec::DescriptorBuilder("allfixed_sample")
        .add("id", PrimType::Int64, 1, true)
        .add("a", PrimType::Int32)
        .add("b", PrimType::UInt16)
        .add("c", PrimType::UInt32)
        .add("d", PrimType::UInt64)
        .add("e", PrimType::Float64)
        .add("f", PrimType::Bool)
        .add("g", PrimType::Enum, 1, false, {1, 2, 3})
        .addFixedStr("code", 6)
        .build();
    const auto blob = rune::codec::serialize(d);
    std::ofstream f(outPath, std::ios::binary | std::ios::trunc);
    if (!f) {
        std::fprintf(stderr, "make_allfixed_sample_blob: cannot open '%s'\n", outPath);
        return 1;
    }
    f.write(reinterpret_cast<const char*>(blob.data()), static_cast<std::streamsize>(blob.size()));
    return 0;
}
