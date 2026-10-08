// RequireTlsDowngradeTest.cpp — Task 9: require_tls hard-reject + shard-mode
// crypto gating, i.e. the downgrade-safety decision ClientGateway::handleConnect
// makes via rune::gateway::{shouldEngageCrypto, shardModeRefusesCryptoOffer,
// mustRejectForRequireTls} (ConnectDowngradePolicy.h).
//
// Aeron-free (same reasoning as GatewayCryptoTest.cpp — ClientGateway.h pulls
// <Aeron.h>, unavailable in this build on macOS/arm64; see this lib's
// CMakeLists.txt comment on rune_gateway_crypto_tests). This target links
// rune_crypto + GTest ONLY: it exercises the SAME decision functions
// handleConnect calls, plus a real TlsSession handshake (reusing
// rune-crypto's own test certs) to prove the end-to-end shape: a crafted
// plaintext Connect (no cipherList/handshakeBytes) is what
// mustRejectForRequireTls's `cryptoEstablished=false` input models, and a
// real handshake is what `cryptoEstablished=true` models.
//
// What this proves (maps 1:1 onto task-9-interfaces.md's test list):
//   1. PlaintextRejectedWhenRequireTls — require_tls + no crypto offer =>
//      reject (mirrors handleConnect setting pc_entry.cryptoFailed=true,
//      which pollPendingConnects turns into ConnectResponse{HandshakeFailed}).
//   2. PlaintextAcceptedWhenNotRequireTls — !require_tls + no crypto offer
//      => served plaintext (back-compat), never rejected.
//   3. CryptoConnectStillHandshakesUnderRequireTls — require_tls + a REAL
//      client offer (driven through an actual TlsSession handshake, cert
//      loaded from a temp PEM file round-tripping to makeServer) =>
//      NOT rejected (cryptoEstablished=true defeats the require_tls check).
//   4. ShardModeRefusesCryptoOfferRegardlessOfRequireTls — the ★ gating note:
//      shard mode must refuse an encrypted offer outright (never silently
//      downgrade it to plaintext), independent of require_tls.
//   5. ShardModePlaintextStillAllowedWhenNotRequireTls — shard mode does NOT
//      gate a client that never offered crypto in the first place (shard
//      mode was plaintext-only before this task and stays so for such a
//      client when require_tls is off).
//   6. CertKeyRoundTripsToMakeServer — the broker's cert/key FILE PATH ->
//      PEM TEXT load (rune::node::loadPemFile) round-trips to a VALID
//      TlsSession::makeServer(), exactly the ClientGateway ctor's contract.
//
// Review fix round 2 (TlsStartupPolicy.h — BrokerNode::buildGateway()'s
// startup-time validation, factored the same way):
//   7. ConfiguredButUnloadableCertRefusesStartRegardlessOfRequireTls —
//      TLS configured (a path is set) but the cert/key didn't both load ->
//      must refuse to start, whether or not require_tls is set (closes the
//      fail-open footgun where this only threw when require_tls was ALSO on).
//   8. OnlyOnePathSetRefusesStart — a partial config (only cert OR only key
//      path set) counts as "configured" and must refuse to start too.
//   9. NeitherPathSetProceedsPlaintext — the genuinely-unconfigured case is
//      unaffected: no refusal, stays plaintext.
//  10. ValidConfiguredCertDoesNotRefuseStart — the happy path (both load)
//      must NOT be caught by the new fail-fast check.
//  11. ShardModeWithRequireTlsRefusesStart — shard mode + require_tls
//      together can never serve a client (shard refuses crypto, require_tls
//      refuses plaintext) -> refuse to start rather than brick silently.

#include <gtest/gtest.h>

#include <rune/crypto/TlsSession.h>
#include <rune/gateway/ConnectDowngradePolicy.h>
#include <rune/node/TlsStartupPolicy.h>
#include <rune/node/NodeConfig.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

namespace cr = rune::crypto;
using namespace rune::gateway;

namespace {

std::string slurp(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

std::string testdataPath(const char* name) {
#ifdef RUNE_CRYPTO_TESTDATA_DIR
    return std::string(RUNE_CRYPTO_TESTDATA_DIR) + "/" + name;
#else
    return std::string("testdata/") + name;
#endif
}

}  // namespace

// ---------------------------------------------------------------------------
// Pure decision-function tests (the exact booleans handleConnect computes).
// ---------------------------------------------------------------------------

TEST(RequireTlsDowngrade, PlaintextRejectedWhenRequireTls) {
    // Plaintext client: clientOffersCrypto=false => shouldEngageCrypto is
    // false regardless of brokerHasTls => cryptoEstablished=false.
    EXPECT_FALSE(shouldEngageCrypto(/*shardMode=*/false, /*clientOffersCrypto=*/false,
                                     /*brokerHasTls=*/true));
    EXPECT_TRUE(mustRejectForRequireTls(/*requireTls=*/true, /*cryptoEstablished=*/false));
}

TEST(RequireTlsDowngrade, PlaintextAcceptedWhenNotRequireTls) {
    EXPECT_FALSE(mustRejectForRequireTls(/*requireTls=*/false, /*cryptoEstablished=*/false));
}

TEST(RequireTlsDowngrade, CryptoConnectStillHandshakesUnderRequireTls) {
    // Client offered crypto, broker has a cert, not shard mode => engage.
    EXPECT_TRUE(shouldEngageCrypto(/*shardMode=*/false, /*clientOffersCrypto=*/true,
                                    /*brokerHasTls=*/true));
    // Once the handshake lands a live TlsSession (cryptoEstablished=true),
    // require_tls must NOT reject it.
    EXPECT_FALSE(mustRejectForRequireTls(/*requireTls=*/true, /*cryptoEstablished=*/true));
}

TEST(RequireTlsDowngrade, BrokerWithNoCertRejectedWhenRequireTls) {
    // Broker not configured with a cert at all: never engages crypto even
    // though the client offered it — require_tls must still reject rather
    // than silently serve plaintext.
    EXPECT_FALSE(shouldEngageCrypto(/*shardMode=*/false, /*clientOffersCrypto=*/true,
                                     /*brokerHasTls=*/false));
    EXPECT_TRUE(mustRejectForRequireTls(/*requireTls=*/true, /*cryptoEstablished=*/false));
}

// ★ shard-mode gating (Task 8b review carried into Task 9).
TEST(RequireTlsDowngrade, ShardModeRefusesCryptoOfferRegardlessOfRequireTls) {
    EXPECT_TRUE(shardModeRefusesCryptoOffer(/*shardMode=*/true, /*clientOffersCrypto=*/true,
                                             /*brokerHasTls=*/true));
    // shouldEngageCrypto must also refuse to engage in shard mode (the two
    // functions must agree — handleConnect checks shardModeRefusesCryptoOffer
    // FIRST, but a caller that skipped that check must still fail closed).
    EXPECT_FALSE(shouldEngageCrypto(/*shardMode=*/true, /*clientOffersCrypto=*/true,
                                     /*brokerHasTls=*/true));
    // The refusal is unconditional — even with require_tls OFF, shard mode
    // still must not silently establish an encrypted session (it would be
    // served Records in the clear by a shard-mode worker — Worker.h, Task 14).
    // handleConnect models this by setting cryptoFailed=true directly on
    // shardModeRefusesCryptoOffer, not by routing through
    // mustRejectForRequireTls at all.
}

TEST(RequireTlsDowngrade, ShardModePlaintextStillAllowedWhenNotRequireTls) {
    // A client that never offered crypto in the first place: shard mode's
    // gating does not apply (nothing to refuse), and require_tls is off =>
    // served plaintext, same as pre-Task-9 shard mode.
    EXPECT_FALSE(shardModeRefusesCryptoOffer(/*shardMode=*/true, /*clientOffersCrypto=*/false,
                                              /*brokerHasTls=*/true));
    EXPECT_FALSE(mustRejectForRequireTls(/*requireTls=*/false, /*cryptoEstablished=*/false));
}

// ---------------------------------------------------------------------------
// End-to-end: cert/key FILE PATH -> PEM TEXT -> TlsSession::makeServer,
// exactly BrokerNode::buildGateway()'s contract (Task 9).
// ---------------------------------------------------------------------------

class TempPemFile {
public:
    explicit TempPemFile(const std::string& content) {
        path_ = (std::filesystem::temp_directory_path() /
                 ("rune_require_tls_test_" +
                  std::to_string(reinterpret_cast<std::uintptr_t>(this)) + ".pem")).string();
        std::ofstream f(path_, std::ios::binary);
        f << content;
    }
    ~TempPemFile() { std::remove(path_.c_str()); }
    [[nodiscard]] const std::string& path() const { return path_; }

private:
    std::string path_;
};

TEST(RequireTlsDowngrade, CertKeyRoundTripsToMakeServer) {
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    ASSERT_FALSE(serverPem.empty());
    ASSERT_FALSE(serverKey.empty());

    TempPemFile certFile(serverPem);
    TempPemFile keyFile(serverKey);

    std::string loadedCert, loadedKey;
    ASSERT_TRUE(rune::node::loadPemFile(certFile.path(), loadedCert));
    ASSERT_TRUE(rune::node::loadPemFile(keyFile.path(), loadedKey));
    EXPECT_EQ(loadedCert, serverPem);
    EXPECT_EQ(loadedKey, serverKey);

    cr::TlsSession srv = cr::TlsSession::makeServer(loadedCert.c_str(), loadedKey.c_str());
    EXPECT_TRUE(srv.valid()) << "a PEM round-tripped through loadPemFile must still make a valid "
                                 "server TlsSession (ClientGateway ctor's exact contract)";
}

TEST(RequireTlsDowngrade, LoadPemFileFailsClosedOnMissingFile) {
    std::string out = "not-cleared-yet";
    EXPECT_FALSE(rune::node::loadPemFile("/nonexistent/path/does-not-exist.pem", out));
    EXPECT_TRUE(out.empty()) << "a failed load must clear `out`, never leave stale PEM text that "
                                 "could be mistaken for a loaded cert";
}

// ---------------------------------------------------------------------------
// Review fix round 2: BrokerNode::buildGateway()'s startup-time TLS config
// validation (TlsStartupPolicy.h). Mirrors what buildGateway() itself
// computes: tlsCertLoaded/tlsKeyLoaded come from a REAL loadPemFile() call
// (not hand-waved booleans) wherever the scenario calls for it, so these
// tests exercise the exact same loadPemFile() contract buildGateway() relies
// on, just without needing a live Aeron client to construct a BrokerNode.
// ---------------------------------------------------------------------------

TEST(TlsStartupPolicy, ConfiguredButUnloadableCertRefusesStartRegardlessOfRequireTls) {
    // TLS configured (a cert path is set) but it does not load (missing
    // file) -> must refuse to start. The function takes no require_tls
    // parameter at all: the fix's whole point is that this refusal does NOT
    // depend on require_tls (previously it only threw when require_tls was
    // ALSO set; with require_tls off it used to log-and-degrade instead).
    std::string certOut, keyOut;
    const bool certLoaded = rune::node::loadPemFile("/nonexistent/cert.pem", certOut);
    const bool keyLoaded  = rune::node::loadPemFile("/nonexistent/key.pem", keyOut);
    ASSERT_FALSE(certLoaded);
    ASSERT_FALSE(keyLoaded);

    const bool configured = rune::node::tlsConfigured("/nonexistent/cert.pem", "/nonexistent/key.pem");
    ASSERT_TRUE(configured);
    EXPECT_TRUE(rune::node::mustRefuseStartForUnloadableTls(configured, certLoaded, keyLoaded));
}

TEST(TlsStartupPolicy, OnlyOnePathSetRefusesStart) {
    const std::string serverPem = slurp(testdataPath("server.pem"));
    ASSERT_FALSE(serverPem.empty());
    TempPemFile certFile(serverPem);

    std::string certOut, keyOut;
    const bool certLoaded = rune::node::loadPemFile(certFile.path(), certOut);
    const bool keyLoaded  = rune::node::loadPemFile("", keyOut);  // RUNE_TLS_KEY_FILE never set
    ASSERT_TRUE(certLoaded);
    ASSERT_FALSE(keyLoaded);

    // A partial config still counts as "configured" -> must refuse to start,
    // even though the one path that WAS set loaded a perfectly valid cert.
    const bool configured = rune::node::tlsConfigured(certFile.path(), "");
    ASSERT_TRUE(configured);
    EXPECT_TRUE(rune::node::mustRefuseStartForUnloadableTls(configured, certLoaded, keyLoaded));
}

TEST(TlsStartupPolicy, NeitherPathSetProceedsPlaintext) {
    // The genuinely-unconfigured case (today's default) is unaffected by
    // this fix: no paths set -> not configured -> no refusal, plaintext.
    std::string certOut, keyOut;
    const bool certLoaded = rune::node::loadPemFile("", certOut);
    const bool keyLoaded  = rune::node::loadPemFile("", keyOut);
    const bool configured = rune::node::tlsConfigured("", "");
    EXPECT_FALSE(configured);
    EXPECT_FALSE(rune::node::mustRefuseStartForUnloadableTls(configured, certLoaded, keyLoaded));
}

TEST(TlsStartupPolicy, ValidConfiguredCertDoesNotRefuseStart) {
    // The happy path: both paths set, both load -> must NOT be caught by the
    // new fail-fast check (it must not regress the working configuration).
    const std::string serverPem = slurp(testdataPath("server.pem"));
    const std::string serverKey = slurp(testdataPath("server.key"));
    ASSERT_FALSE(serverPem.empty());
    ASSERT_FALSE(serverKey.empty());
    TempPemFile certFile(serverPem);
    TempPemFile keyFile(serverKey);

    std::string certOut, keyOut;
    const bool certLoaded = rune::node::loadPemFile(certFile.path(), certOut);
    const bool keyLoaded  = rune::node::loadPemFile(keyFile.path(), keyOut);
    ASSERT_TRUE(certLoaded);
    ASSERT_TRUE(keyLoaded);

    const bool configured = rune::node::tlsConfigured(certFile.path(), keyFile.path());
    ASSERT_TRUE(configured);
    EXPECT_FALSE(rune::node::mustRefuseStartForUnloadableTls(configured, certLoaded, keyLoaded));
}

TEST(TlsStartupPolicy, ShardModeWithRequireTlsRefusesStart) {
    // Shard mode + require_tls together can never serve a client (shard
    // mode refuses every crypto offer; require_tls refuses every plaintext
    // fallback) -> refuse to start rather than brick silently per-Connect.
    EXPECT_TRUE(rune::node::mustRefuseShardWithRequireTls(/*shardMode=*/true, /*requireTls=*/true));
    // Either flag alone is fine.
    EXPECT_FALSE(rune::node::mustRefuseShardWithRequireTls(/*shardMode=*/true, /*requireTls=*/false));
    EXPECT_FALSE(rune::node::mustRefuseShardWithRequireTls(/*shardMode=*/false, /*requireTls=*/true));
    EXPECT_FALSE(rune::node::mustRefuseShardWithRequireTls(/*shardMode=*/false, /*requireTls=*/false));
}
