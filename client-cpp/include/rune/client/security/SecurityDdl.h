#pragma once
// include/rune/client/security/SecurityDdl.h — authentication Task 8: the client half of security DDL.
//
// The broker never sees a password (spec §3, D9): the CLIENT turns the plan's password into a SCRAM-SHA-512
// verifier (random 16-byte salt, `scramIterations`, StoredKey, ServerKey) and sends only that, together with the
// REDACTED statement (D10).  This header builds the wire ops; Client::securityDdl* / bootstrapSecurity* send them
// with the catalog DDL's Redirect / NotLeader / request-id handling.  Cold path.
#include <rune/ql/Backends.h>

#include <rune/protocol/client/SecurityMessages.h>

#include <array>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace rune::client {

/// The result of a security DDL / bootstrap (DdlResponse 78): errorCode 0 = OK; else a broker code
/// (InvalidRequest 39 with `detail` naming the cause, NotAuthorized 45, PasswordChangeRequired 47, ...).
struct SecurityDdlResult {
    uint16_t    errorCode  = 0;
    uint64_t    objectId   = 0;      ///< the user / role id
    uint32_t    newVersion = 0;
    std::string detail;
    rune::protocol::RequestId requestId{};   ///< the id the DDL carried (generated when DdlOptions had none): reads can wait for it
};

namespace security {

/// A verifier's bytes (the VerifierWire views point into it).  Zeroed by wipe() / the destructor.
struct VerifierStorage {
    std::string                  salt;
    uint32_t                     iterations = 0;
    std::array<char, 64>         storedKey{}, serverKey{};
    VerifierStorage() = default;
    VerifierStorage(const VerifierStorage&) = delete;
    VerifierStorage& operator=(const VerifierStorage&) = delete;
    ~VerifierStorage() { wipe(); }
    void wipe() noexcept;
    [[nodiscard]] rune::protocol::client::VerifierWire view() const noexcept;
};

/// Derives the verifier of `password` (D9: at least `minPasswordLength` bytes, else std::invalid_argument
/// "password must be at least N characters").  std::runtime_error when the platform cannot salt / hash.
void buildVerifier(std::string_view password, uint32_t iterations, uint32_t minPasswordLength, VerifierStorage& out);

/// Fills every wire field of `msg` from `plan` except sessionId / correlationId / ext; a password-carrying plan's
/// verifier is built into `vs` and `msg.verifier` views it (so `vs` must outlive the encode).  Throws like
/// buildVerifier.  `name` / `pattern` / `roleName` view into `plan`.
void fillSecurityDdl(const rune::ql::SecurityPlan& plan, uint32_t iterations, uint32_t minPasswordLength,
                     VerifierStorage& vs, rune::protocol::client::SecurityDdl& msg);

/// D4 / #234: a SecurityBootstrap with everything but its MAC — verifier of `oneTimePassword`, a random 32-byte
/// fakeSaltSeed (D8), a random nonce, `mac` zero.  The proof is challenge-response: the gateway issues a one-use
/// challenge per attempt (SecurityBootstrapChallengeRequest 102 -> 103), so the MAC cannot be made here.  `macKey`
/// receives the bootstrap MAC subkey (deriveBootstrapMacKey of the cluster master derived from the key FILE contents;
/// D32: at least 32 bytes, else std::invalid_argument); the master itself is wiped.  signBootstrap() signs an encoded
/// frame with it once the challenge is known.  `vs` backs msg.verifier; `adminName` must outlive the encode.
void buildBootstrap(std::string_view adminName, std::string_view oneTimePassword,
                    std::span<const uint8_t> clusterKeyFileContents, uint32_t iterations,
                    uint32_t minPasswordLength, VerifierStorage& vs, rune::protocol::client::SecurityBootstrap& msg,
                    std::array<uint8_t, 64>& macKey);

/// #234: writes the HMAC-SHA-512 over bootstrapMacInput(frame decoded as a SecurityBootstrap, challenge) under `macKey`
/// into the frame's last 64 bytes (the MAC is the frame's last field).  False when the frame is not an encodable
/// SecurityBootstrap or the HMAC fails; the temporaries are wiped.
[[nodiscard]] bool signBootstrap(std::span<const uint8_t, 64> macKey,
                                 std::span<const uint8_t, rune::protocol::client::kBootstrapChallengeBytes> challenge,
                                 std::span<uint8_t> frame);

} // namespace security
} // namespace rune::client
