#pragma once
// tests/security/SecurityGoldenPins.h — FROZEN.  The SecurityState::digest() after each event of
// tests/security/golden/v1/basic.rsec, printed once by tools/make_security_golden when formatVer 1
// was introduced.  Never regenerate: a mismatch means a v1 rule changed — ship it as formatVer 2.
#include <cstdint>

namespace rune::security::golden {
inline constexpr uint64_t kBasicV1Digests[] = {
    0x71fc2c37c00f82a5ull,   // offset 0 Bootstrap
    0x08050e24cc070b46ull,   // offset 1 CreateRole
    0x3dc384f896d2a012ull,   // offset 2 Grant
    0x28f1355b93efaf54ull,   // offset 3 Grant
    0x5e595bbbd8be65e1ull,   // offset 4 Grant
    0xacaeba6e86f1ded0ull,   // offset 5 CreateUser
    0x4e35d8a7074159deull,   // offset 6 GrantRole
    0x65ed1dcbdbe229f1ull,   // offset 7 AlterUser
    0xf041ebb9b7f876faull,   // offset 8 AlterUser
    0x2738e2b573ad2d47ull,   // offset 9 Revoke
    0x2470f1854a4353f1ull,   // offset 10 AlterUser
    0xd463756850807686ull,   // offset 11 AlterUser
    0xdede2eac742ef2b9ull,   // offset 12 AlterUser
    0xd10df3d1870a0b34ull,   // offset 13 RevokeRole
    0x386271ae760a7084ull,   // offset 14 DropRole
    0x84c4c86fede55f90ull,   // offset 15 DropUser
};
} // namespace rune::security::golden
