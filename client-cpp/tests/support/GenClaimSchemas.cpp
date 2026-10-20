// tests/support/GenClaimSchemas.cpp — Phase 2b Part B: writes the all-fixed
// schemas the claim tests and the claim bench compile against (build-time
// codegen through rune-codec's generateHeader, like characters.gen.h).
//   position     userId Int64 KEY, zone Int32, x/y/z Float64, flags UInt16,
//                seq UInt32, tick UInt64, alive Bool, kind Enum{0,1,2},
//                tag string(8)            -> blockLength 63, kEncodedLength 67
//   tick_sample  seq UInt64, value Float64 (no key field)
//   short_key    code string(8) KEY, v Int64 (a key send() can route)
//   long_key     code string(20) KEY, v Int64 (a key send() cannot route)
//   eos_out      id Int64 KEY, v Int64 (examples/eos_probe.cpp's local build; the e2e image uses the
//                header `rune schema generate eos_out` writes from the broker's stored schema)
#include <rune/codec/Descriptor.h>
#include <rune/codec/Generator.h>

#include <cstdio>
#include <fstream>
#include <string>

namespace {
bool write(const std::string& path, const rune::codec::Descriptor& d) {
    std::ofstream f(path, std::ios::trunc);
    if (!f) {
        std::fprintf(stderr, "rune_gen_claim_schemas: cannot write '%s'\n", path.c_str());
        return false;
    }
    f << rune::codec::generateHeader(d);
    return static_cast<bool>(f);
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::fprintf(stderr, "usage: rune_gen_claim_schemas <out-dir>\n");
        return 1;
    }
    using rune::codec::DescriptorBuilder;
    using rune::codec::PrimType;
    const auto position = DescriptorBuilder("position")
                              .add("userId", PrimType::Int64, 1, true)
                              .add("zone", PrimType::Int32)
                              .add("x", PrimType::Float64)
                              .add("y", PrimType::Float64)
                              .add("z", PrimType::Float64)
                              .add("flags", PrimType::UInt16)
                              .add("seq", PrimType::UInt32)
                              .add("tick", PrimType::UInt64)
                              .add("alive", PrimType::Bool)
                              .add("kind", PrimType::Enum, 1, false, {0, 1, 2})
                              .addFixedStr("tag", 8)
                              .build();
    const auto tick = DescriptorBuilder("tick_sample")
                          .add("seq", PrimType::UInt64)
                          .add("value", PrimType::Float64)
                          .build();
    const auto longKey = DescriptorBuilder("long_key")
                             .addFixedStr("code", 20, 1, true)
                             .add("v", PrimType::Int64)
                             .build();
    const auto shortKey = DescriptorBuilder("short_key")
                              .addFixedStr("code", 8, 1, true)
                              .add("v", PrimType::Int64)
                              .build();
    const auto eosOut = DescriptorBuilder("eos_out")
                            .add("id", PrimType::Int64, 1, true)
                            .add("v", PrimType::Int64)
                            .build();
    const std::string dir = argv[1];
    return write(dir + "/position.gen.h", position) && write(dir + "/tick_sample.gen.h", tick) &&
                   write(dir + "/long_key.gen.h", longKey) &&
                   write(dir + "/short_key.gen.h", shortKey) &&
                   write(dir + "/eos_out.gen.h", eosOut)
               ? 0
               : 1;
}
