#pragma once
// tests/security/SecurityGoldenPins.h — FROZEN.  The SecurityState::digest() after each event of
// tests/security/golden/v1/basic.rsec, printed once by tools/make_security_golden when formatVer 1
// was introduced.  Regenerated ONCE in review A (ops tag = kind, Bootstrap fakeSaltSeed; before any consumer existed). Never regenerate: a mismatch means a v1 rule changed — ship it as formatVer 2.
#include <cstdint>

namespace rune::security::golden {
inline constexpr uint64_t kBasicV1Digests[] = {
    0x201b215395d3ef0cull,   // offset 0 Bootstrap
    0x1c06d7c4cc120af5ull,   // offset 1 CreateRole
    0xf95b595aed953e19ull,   // offset 2 Grant
    0x586a6e6b8d7f093full,   // offset 3 Grant
    0xab5e81c2405f9f00ull,   // offset 4 Grant
    0x21eb6d69db207db3ull,   // offset 5 CreateUser
    0x7c0fad31aecc07fdull,   // offset 6 GrantRole
    0x9f988618b252aa70ull,   // offset 7 AlterUser
    0xdce44ede7d134611ull,   // offset 8 AlterUser
    0x0ee3d657ba7d80b2ull,   // offset 9 Revoke
    0xc0f8d7007268e870ull,   // offset 10 AlterUser
    0x40dcf7da01d2a9b5ull,   // offset 11 AlterUser
    0x5c3bc1205630d7c8ull,   // offset 12 AlterUser
    0xdf1bddfe72c9061full,   // offset 13 RevokeRole
    0xd5d7f73dd18010cfull,   // offset 14 DropRole
    0x5d19e16768750af3ull,   // offset 15 DropUser
};
} // namespace rune::security::golden
