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
//   ki_account   id UInt64 KEY, name string(16), n Int64 (examples/checked_writes_probe.cpp: the key index e2e)
//   lease_claim  claim string(32) KEY, a UInt64, b string(8), behind the four lease system columns (the stored schema of a
//                lease topic; generated with isLease: withLease, LeasesRow - LeaseTopicBrokerRegression)
//   lease_wide   claim string(128) KEY, a UInt64 (the same, a key of the widest lease key width)
//   lease_evo    version 2 of a lease schema with a field DROPPED between live ones (a UInt32, gone Int64 until 2, tag string(4),
//                flag Bool, ratio Float64, kind Enum, w UInt16): the LeasesRow decode skips the dropped slot (LeasesRowDecodeRegression)
//   ai_user      userId UInt64 KEY, provider UInt16, subject string(64), createdAt Int64 (the same probe: the autoincrement e2e)
//   rp_user      the RegressorsPath golden script's `user` topic (schema name "user"): userId UInt64 KEY, provider UInt16,
//                subject string(64), createdAt Int64 (the same probe: the REGRESSORSPATH e2e)
//   rp_character the golden script's `character` topic (schema name "character"): characterId UInt64 KEY, userId UInt64,
//                name string(16), gender/hairSkin/hairSheet/headSkin UInt16, createdAt/lastPlayed Int64
#include <rune/codec/Descriptor.h>
#include <rune/codec/Generator.h>
#include <rune/codec/LeaseColumns.h>

#include "../../examples/filter/PlayerEventSchema.h"

#include <cstdio>
#include <fstream>
#include <string>

namespace {
bool write(const std::string& path, const rune::codec::Descriptor& d, bool isLease = false) {
    std::ofstream f(path, std::ios::trunc);
    if (!f) {
        std::fprintf(stderr, "rune_gen_claim_schemas: cannot write '%s'\n", path.c_str());
        return false;
    }
    f << rune::codec::generateHeader(d, d.version, "rune::gen", isLease);
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
    const auto kiAccount = DescriptorBuilder("ki_account")
                               .add("id", PrimType::UInt64, 1, true)
                               .addFixedStr("name", 16)
                               .add("n", PrimType::Int64)
                               .build();
    const auto aiUser = DescriptorBuilder("ai_user")
                            .add("userId", PrimType::UInt64, 1, true)
                            .add("provider", PrimType::UInt16)
                            .addFixedStr("subject", 64)
                            .add("createdAt", PrimType::Int64)
                            .build();
    auto leaseClaim = DescriptorBuilder("lease_claim")
                          .addFixedStr("claim", 32, 1, true)
                          .add("a", PrimType::UInt64)
                          .addFixedStr("b", 8)
                          .build();
    auto leaseWide = DescriptorBuilder("lease_wide")
                         .addFixedStr("claim", 128, 1, true)
                         .add("a", PrimType::UInt64)
                         .build();
    auto leaseEvo = DescriptorBuilder("lease_evo", 2)
                        .addTagged("a", 0, PrimType::UInt32, 1, 0xFFFF, false, {})
                        .addTagged("gone", 1, PrimType::Int64, 1, 2, false, {})
                        .addFixedStrTagged("tag", 2, 4, 1, 0xFFFF, false)
                        .addTagged("flag", 3, PrimType::Bool, 1, 0xFFFF, false, {})
                        .addTagged("ratio", 4, PrimType::Float64, 1, 0xFFFF, false, {})
                        .addTagged("kind", 5, PrimType::Enum, 1, 0xFFFF, false, {0, 1, 2})
                        .addTagged("w", 6, PrimType::UInt16, 1, 0xFFFF, false, {})
                        .build();
    if (!rune::codec::prependLeaseColumns(leaseEvo)) return 1;
    if (!rune::codec::prependLeaseColumns(leaseClaim) || !rune::codec::prependLeaseColumns(leaseWide)) return 1;
    const auto rpUser = DescriptorBuilder("user")
                            .add("userId", PrimType::UInt64, 1, true)
                            .add("provider", PrimType::UInt16)
                            .addFixedStr("subject", 64)
                            .add("createdAt", PrimType::Int64)
                            .build();
    const auto rpCharacter = DescriptorBuilder("character")
                                 .add("characterId", PrimType::UInt64, 1, true)
                                 .add("userId", PrimType::UInt64)
                                 .addFixedStr("name", 16)
                                 .add("gender", PrimType::UInt16)
                                 .add("hairSkin", PrimType::UInt16)
                                 .add("hairSheet", PrimType::UInt16)
                                 .add("headSkin", PrimType::UInt16)
                                 .add("createdAt", PrimType::Int64)
                                 .add("lastPlayed", PrimType::Int64)
                                 .build();
    const std::string dir = argv[1];
    return write(dir + "/position.gen.h", position) && write(dir + "/tick_sample.gen.h", tick) &&
                   write(dir + "/long_key.gen.h", longKey) &&
                   write(dir + "/short_key.gen.h", shortKey) &&
                   write(dir + "/eos_out.gen.h", eosOut) &&
                   write(dir + "/ki_account.gen.h", kiAccount) &&
                   write(dir + "/ai_user.gen.h", aiUser) &&
                   write(dir + "/lease_claim.gen.h", leaseClaim, true) &&
                   write(dir + "/lease_wide.gen.h", leaseWide, true) &&
                   write(dir + "/lease_evo.gen.h", leaseEvo, true) &&
                   write(dir + "/rp_user.gen.h", rpUser) &&
                   write(dir + "/rp_character.gen.h", rpCharacter) &&
                   write(dir + "/player_event.gen.h", rune::examples::playerEventDescriptor())
               ? 0
               : 1;
}
