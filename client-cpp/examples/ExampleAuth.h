// examples/ExampleAuth.h — credentials for the example binaries (authentication, spec §5 / D5).
//
// Every example reads its login from the environment, the same names the `rune` CLI uses:
//   RUNE_USER           — the principal to log in as ("" / unset = no credentials: an authenticated broker answers
//                         AuthenticationFailed, a dev-mode broker (RUNE_AUTH=disabled) is then refused only by a
//                         credentialed client, never by this one)
//   RUNE_PASSWORD_FILE  — a file holding the password (read once at Client construction; a trailing newline is
//                         stripped).  A password is never accepted from the environment or the command line.
#pragma once

#include <rune/client/Client.h>

#include <cstdlib>

namespace rune::examples {

inline void applyEnvCredentials(rune::client::ClientConfig& cfg) {
    if (const char* u = std::getenv("RUNE_USER"); u != nullptr && *u != '\0') cfg.username = u;
    if (const char* f = std::getenv("RUNE_PASSWORD_FILE"); f != nullptr && *f != '\0') cfg.passwordFile = f;
}

} // namespace rune::examples
