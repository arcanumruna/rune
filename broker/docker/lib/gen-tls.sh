#!/usr/bin/env bash
# docker/lib/gen-tls.sh — a throwaway CA and one broker certificate for an e2e / bench cluster (issue #10).
#
# An authenticating broker serves only TLS client sessions, so every launcher that generates a cluster key also needs a
# broker certificate.  One certificate names every broker host of the run (the client checks each broker's endpoint
# host against it); the CA's private key is discarded once it signed.
#
#   gen-tls.sh <out-dir> [host ...]
#
# Writes <out-dir>/ca.pem (the trust anchor a client passes as --tls-ca / RUNE_TLS_CA_FILE), <out-dir>/tls.pem and
# <out-dir>/tls.key (mode 0600: RUNE_TLS_CERT_FILE / RUNE_TLS_KEY_FILE).  Hosts default to RUNE_TLS_SAN_HOSTS, else
# the docker launchers' broker names; 127.0.0.1 and localhost are always included.  Existing files are replaced.
set -euo pipefail

out="${1:?usage: gen-tls.sh <out-dir> [host ...]}"
shift
hosts=("$@")
if [ "${#hosts[@]}" -eq 0 ]; then
    # shellcheck disable=SC2206
    hosts=(${RUNE_TLS_SAN_HOSTS:-rune-1 rune-2 rune-3 a-rune-1 a-rune-2 a-rune-3 b-rune-1 b-rune-2 b-rune-3})
fi
command -v openssl >/dev/null 2>&1 || { echo "gen-tls.sh: openssl not found" >&2; exit 1; }

mkdir -p "${out}"
tmp="$(mktemp -d "${out}/.tls.XXXXXX")"
trap 'rm -rf "${tmp}"' EXIT

san="DNS:localhost,IP:127.0.0.1"
for h in "${hosts[@]}"; do
    [ -n "${h}" ] && [ "${h}" != "localhost" ] && san="${san},DNS:${h}"
done
printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth\n' "${san}" > "${tmp}/san.ext"

( umask 077
  openssl ecparam -name prime256v1 -genkey -noout -out "${tmp}/ca.key" 2>/dev/null
  openssl req -x509 -new -key "${tmp}/ca.key" -sha256 -days 30 -subj "/CN=rune e2e CA" -out "${tmp}/ca.pem" 2>/dev/null
  openssl ecparam -name prime256v1 -genkey -noout -out "${tmp}/tls.key" 2>/dev/null
  openssl req -new -key "${tmp}/tls.key" -subj "/CN=${hosts[0]:-localhost}" -out "${tmp}/tls.csr" 2>/dev/null
  openssl x509 -req -in "${tmp}/tls.csr" -CA "${tmp}/ca.pem" -CAkey "${tmp}/ca.key" -set_serial "0x$(openssl rand -hex 8)" -sha256 -days 30 \
      -extfile "${tmp}/san.ext" -out "${tmp}/tls.pem" 2>/dev/null )

install -m 0644 "${tmp}/ca.pem" "${out}/ca.pem"
install -m 0644 "${tmp}/tls.pem" "${out}/tls.pem"
install -m 0600 "${tmp}/tls.key" "${out}/tls.key"
