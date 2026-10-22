# Generates the throwaway test-only CA + localhost server certificate/key used by the TLS tests
# (server.key, server.pem, ca.pem) into <outdir> at configure time, so no private key is
# committed.  Idempotent: skipped when the three files already exist.
#
#   rune_crypto_generate_test_certs(<outdir>)
#
# The macOS system openssl is LibreSSL (no ed25519 keygen); prefer a Homebrew OpenSSL 3.
function(rune_crypto_generate_test_certs outdir)
    # v2 (issue #115): the server certificate also names IP 127.0.0.1 - the client checks an IP endpoint against the
    # iPAddress SANs.  The stamp regenerates an older set.
    if(EXISTS "${outdir}/server.key" AND EXISTS "${outdir}/server.pem" AND EXISTS "${outdir}/ca.pem"
       AND EXISTS "${outdir}/san-v2.stamp")
        return()
    endif()
    find_program(RUNE_TEST_OPENSSL openssl
        HINTS /opt/homebrew/opt/openssl@3/bin /opt/homebrew/opt/openssl/bin /usr/local/opt/openssl@3/bin
        NO_DEFAULT_PATH)
    find_program(RUNE_TEST_OPENSSL openssl REQUIRED)
    file(MAKE_DIRECTORY "${outdir}")
    file(WRITE "${outdir}/san.ext" "subjectAltName=DNS:localhost,IP:127.0.0.1\n")
    set(_o "${RUNE_TEST_OPENSSL}")
    function(_rune_run)
        execute_process(COMMAND ${ARGN} WORKING_DIRECTORY "${outdir}" RESULT_VARIABLE rc
                        OUTPUT_QUIET ERROR_VARIABLE err)
        if(NOT rc EQUAL 0)
            message(FATAL_ERROR "test cert generation failed: ${ARGN}\n${err}")
        endif()
    endfunction()
    _rune_run(${_o} req -x509 -newkey ed25519 -keyout ca.key -out ca.pem -days 3650 -nodes -subj "/CN=Test CA")
    _rune_run(${_o} req -newkey ed25519 -keyout server.key -out server.csr -nodes -subj "/CN=localhost")
    _rune_run(${_o} x509 -req -in server.csr -CA ca.pem -CAkey ca.key -out server.pem -days 3650
              -extfile san.ext -CAcreateserial)
    file(REMOVE "${outdir}/ca.key" "${outdir}/server.csr" "${outdir}/san.ext" "${outdir}/ca.srl")
    file(WRITE "${outdir}/san-v2.stamp" "DNS:localhost,IP:127.0.0.1\n")
    message(STATUS "rune-crypto: generated test TLS certificates in ${outdir}")
endfunction()
