#!/usr/bin/env bash
# Regenerate the throwaway test-only CA + server cert used by TlsSessionTest.
# Run from anywhere; always writes into this script's own directory.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

# The macOS system `openssl` is LibreSSL, which lacks ed25519 keygen support.
# Prefer a real OpenSSL (Homebrew) if present.
OPENSSL=openssl
for cand in /opt/homebrew/opt/openssl@3/bin/openssl /opt/homebrew/opt/openssl/bin/openssl /usr/local/opt/openssl@3/bin/openssl; do
  if [ -x "$cand" ]; then OPENSSL="$cand"; break; fi
done

rm -f ca.key ca.pem ca.srl server.key server.csr server.pem san.ext

# Self-signed CA.
"$OPENSSL" req -x509 -newkey ed25519 -keyout ca.key -out ca.pem -days 3650 -nodes -subj "/CN=Test CA"

# Server key + CSR for localhost.
"$OPENSSL" req -newkey ed25519 -keyout server.key -out server.csr -nodes -subj "/CN=localhost"

printf "subjectAltName=DNS:localhost\n" > san.ext

# Sign the server cert with the test CA.
"$OPENSSL" x509 -req -in server.csr -CA ca.pem -CAkey ca.key -out server.pem -days 3650 -extfile san.ext

# Cleanup intermediates that aren't needed by the test.
rm -f ca.key server.csr san.ext ca.srl

echo "Generated: ca.pem server.pem server.key"
