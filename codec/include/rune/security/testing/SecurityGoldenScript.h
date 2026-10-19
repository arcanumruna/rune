#pragma once
// rune/security/testing/SecurityGoldenScript.h — the deterministic formatVer-1 event script the
// golden generator (tools/make_security_golden.cpp) and the golden / replayer / query tests share.
// Built on a growing SecurityState exactly as the leader builds events: fill the event, stamp it with
// previewFingerprint, apply it.
//
// FROZEN with formatVer 1: tests/security/golden/v1/basic.rsec was generated from this script and
// SecurityGoldenRegression.TheGoldenScriptStillProducesTheCheckedInBytes pins it.  Never edit an
// event; a new formatVer gets a new script beside this one.
//
// Offsets (timestampMs = 1'760'000'000'000 + 1000 * offset; principal.authenticated "admin",
// "bootstrap" at 0; verifier of offset i: salt 0x10+i, storedKey 0x30+i, serverKey 0x50+i, 4096 iterations):
//   0 Bootstrap admin                  1 CreateRole world_server
//   2 Grant READ TOPIC player_events -> role      3 Grant WRITE TOPIC PREFIX 'world.' -> role
//   4 Grant READ GROUP PREFIX 'world-' -> role    5 CreateUser ws1
//   6 GrantRole world_server -> ws1    7 AlterUser ws1 AddPassword      8 AlterUser ws1 DropPassword 1
//   9 Revoke WRITE PREFIX 'world.' <- role        10 AlterUser admin SetPassword (no terminate)
//  11 AlterUser ws1 Disable           12 AlterUser ws1 Enable          13 RevokeRole world_server <- ws1
//  14 DropRole world_server           15 DropUser ws1
#include <rune/security/SecurityEvent.h>

#include <vector>

namespace rune::security::testing {

/// 16 events, offsets 0..15, every kind at least once.  Throws std::logic_error if a step does not
/// preview or apply (the script is broken — never silently emitted).
[[nodiscard]] std::vector<SecurityEvent> goldenScriptV1();

} // namespace rune::security::testing
