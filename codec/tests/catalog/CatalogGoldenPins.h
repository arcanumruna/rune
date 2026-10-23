#pragma once
// tests/catalog/CatalogGoldenPins.h — FROZEN.  The CatalogState::digest() after each event of
// tests/catalog/golden/v1/basic.rcat, printed once by tools/make_catalog_golden when formatVer 1
// was introduced.  Never regenerate: a mismatch means a v1 rule changed — ship it as formatVer 2.
// One deliberate regeneration (#143): GoldenScript's view sinks now mirror their first source's
// partition count (offsets 9..22 changed); the v1 apply rules themselves gained only that refusal.
#include <cstdint>

namespace rune::catalog::golden {
inline constexpr uint64_t kBasicV1Digests[] = {
    0x98523f9c070c40c8ull,   // offset 0 CreateTopic
    0x25c64ea0e767a999ull,   // offset 1 CreateTopic
    0xe673782c9006fcc1ull,   // offset 2 CreateTopic
    0x7dc93d4e162169e4ull,   // offset 3 CreateSchema
    0x5cd4187d79d35fccull,   // offset 4 AlterSchema
    0xf12b045704cf45d7ull,   // offset 5 AlterSchema
    0x35a465ed22014aa7ull,   // offset 6 SetCompatibility
    0xeca2c48f32008649ull,   // offset 7 AlterTopicConfig
    0xf879a217a1dfe71eull,   // offset 8 AlterTopicConfig
    0x720f0cf98ba6353aull,   // offset 9 CreateView
    0xc317e3eb26f66bccull,   // offset 10 AlterView
    0x798b48b22d08447full,   // offset 11 CreateView
    0x904814e75a130fbdull,   // offset 12 DropView
    0x0d9386c34f501f82ull,   // offset 13 DropTopic
    0x7f581d27cd6a798cull,   // offset 14 CreateTopic
    0xb6d2b1cbe474eb5aull,   // offset 15 DropView
    0xf6d5cf0794d0405full,   // offset 16 DropTopic
    0x6f1e9c39b7ac5927ull,   // offset 17 CreateTopic
    0x6313b1edddcfd74dull,   // offset 18 CreateSchema
    0x8ef27efd0b423a54ull,   // offset 19 AlterSchema
    0xbfcdc90bb2ccc31full,   // offset 20 SetCompatibility
    0x0bda66e6512fbb8aull,   // offset 21 CreateView
    0xfc48be1db130d020ull,   // offset 22 AlterView
};
} // namespace rune::catalog::golden
