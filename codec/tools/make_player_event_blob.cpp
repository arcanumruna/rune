// tools/make_player_event_blob.cpp — Phase 2c fixtures: writes player_event.blob and nine_ints.blob into the
// given directory (default "."), from which CMake generates player_event.gen.h / nine_ints.gen.h for
// FilterGeneratedTest.  The player_event builder calls duplicate client-cpp/examples/filter/PlayerEventSchema.h
// (the codec cannot include client files); a pinned literal fingerprint in both test suites keeps them equal.
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <cstdio>
#include <fstream>
#include <string>

namespace {
bool writeBlob(const std::string& path, const rune::codec::Descriptor& d) {
    const auto blob = rune::codec::serialize(d);
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    if (!f) { std::fprintf(stderr, "make_player_event_blob: cannot open '%s'\n", path.c_str()); return false; }
    f.write(reinterpret_cast<const char*>(blob.data()), static_cast<std::streamsize>(blob.size()));
    return static_cast<bool>(f);
}
}  // namespace

int main(int argc, char* argv[]) {
    const std::string dir = (argc >= 2) ? argv[1] : ".";
    using rune::codec::DescriptorBuilder;
    using rune::codec::PrimType;
    const auto playerEvent = DescriptorBuilder("player_event")
        .addTagged("playerId", 0, PrimType::UInt64, 1, 0xFFFF, /*isKey=*/true, {})
        .addTagged("worldId", 1, PrimType::UInt32, 1, 0xFFFF, false, {})
        .addTagged("channel", 2, PrimType::Int32, 1, 0xFFFF, false, {})
        .addFixedStrTagged("zone", 3, 8, 1, 0xFFFF, false)
        .addTagged("kind", 4, PrimType::Enum, 1, 0xFFFF, false, {1, 2, 3})
        .addTagged("alive", 5, PrimType::Bool, 1, 0xFFFF, false, {})
        .addTagged("hp", 6, PrimType::Int64, 1, 0xFFFF, false, {})
        .addTagged("name", 7, PrimType::String, 1, 0xFFFF, false, {})
        .addTagged("pos", 8, PrimType::Float64, 1, 0xFFFF, false, {})
        .build();
    // nine UInt32 fields f0..f8: proves the 8-term limit fails at encode.
    DescriptorBuilder nb("nine_ints");
    for (int i = 0; i < 9; ++i) nb.add("f" + std::to_string(i), PrimType::UInt32);
    const auto nineInts = nb.build();
    return writeBlob(dir + "/player_event.blob", playerEvent) && writeBlob(dir + "/nine_ints.blob", nineInts) ? 0 : 1;
}
