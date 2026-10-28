// tests/support/CryptoStateTestAccess.h - #675: the test-only seam into CryptoState's private receiver-key derivation.
// Production never changes it; a test installs a failing one to prove an underivable key seals nothing.
#pragma once

#include <rune/client/session/SessionMachine.h>

namespace rune::client::session {

struct CryptoStateTestAccess {
    using DeriveFn = bool (*)(const rune::crypto::AeadKey&, uint8_t, rune::crypto::AeadKey&) noexcept;
    static void setDeriveFn(CryptoState& c, DeriveFn fn) noexcept { c.deriveFn_ = fn; }
};

}  // namespace rune::client::session
