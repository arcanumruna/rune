// examples/ExampleAuth.h — credentials and TLS for the example binaries (authentication, spec §5 / D5).
//
// Every example reads its login from the environment, the same names the `rune` CLI uses:
//   RUNE_USER           — the principal to log in as ("" / unset = no credentials: an authenticated broker answers
//                         AuthenticationFailed, a dev-mode broker (RUNE_AUTH=disabled) is then refused only by a
//                         credentialed client, never by this one)
//   RUNE_PASSWORD_FILE  — a file holding the password (read once at Client construction; a trailing newline is
//                         stripped).  A password is never accepted from the environment or the command line.
//   RUNE_TLS_CA_FILE    — a PEM CA bundle to trust instead of the system store (issue #13: the CLI's --tls-ca)
//   RUNE_TLS_HOST       — the certificate name to check ("" / unset = each session checks its endpoint's host)
// Credentials travel only over TLS (issue #14): with RUNE_USER, or either TLS variable, the example offers TLS and,
// with credentials, requires it.  An authenticating broker serves only TLS sessions (issue #10).
#pragma once

#include <rune/client/Client.h>

#include <cstdlib>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>

namespace rune::examples {

inline void applyEnvCredentials(rune::client::ClientConfig& cfg) {
    if (const char* u = std::getenv("RUNE_USER"); u != nullptr && *u != '\0') cfg.username = u;
    if (const char* f = std::getenv("RUNE_PASSWORD_FILE"); f != nullptr && *f != '\0') cfg.passwordFile = f;
    if (const char* ca = std::getenv("RUNE_TLS_CA_FILE"); ca != nullptr && *ca != '\0') {
        std::ifstream in(ca, std::ios::binary);
        if (!in) throw std::runtime_error(std::string("RUNE_TLS_CA_FILE: cannot read '") + ca + "'");
        cfg.tlsTrustPem = std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        cfg.offerCrypto = true;
    }
    if (const char* h = std::getenv("RUNE_TLS_HOST"); h != nullptr && *h != '\0') {
        cfg.tlsExpectHost = h;
        cfg.offerCrypto = true;
    }
    if (!cfg.username.empty()) cfg.offerCrypto = true;   // credentials only over TLS (the Client also requires it)
}

} // namespace rune::examples
