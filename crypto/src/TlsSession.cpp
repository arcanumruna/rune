#include "rune/crypto/TlsSession.h"

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <cstring>
#include <utility>

namespace rune::crypto {

namespace {
// Loads a server cert+key from PEM text into ctx. Returns false on any failure.
bool loadServerIdentity(SSL_CTX* ctx, const char* certPem, const char* keyPem) {
  BIO* certBio = certPem ? BIO_new_mem_buf(certPem, -1) : nullptr;
  X509* cert = certBio ? PEM_read_bio_X509(certBio, nullptr, nullptr, nullptr) : nullptr;
  if (certBio) BIO_free(certBio);

  BIO* keyBio = keyPem ? BIO_new_mem_buf(keyPem, -1) : nullptr;
  EVP_PKEY* pkey = keyBio ? PEM_read_bio_PrivateKey(keyBio, nullptr, nullptr, nullptr) : nullptr;
  if (keyBio) BIO_free(keyBio);

  bool ok = cert && pkey && SSL_CTX_use_certificate(ctx, cert) == 1 &&
            SSL_CTX_use_PrivateKey(ctx, pkey) == 1;
  if (cert) X509_free(cert);
  if (pkey) EVP_PKEY_free(pkey);
  return ok;
}

// Loads a PEM bundle of CA certs (possibly several concatenated) into the ctx's trust
// store. An empty (non-null, zero-length) bundle is a no-op that leaves the store empty
// on purpose (used by tests to exercise the untrusted-cert path).
bool loadTrustBundle(SSL_CTX* ctx, const char* trustPem) {
  if (!trustPem || trustPem[0] == '\0') return true;  // empty trust: nothing to load, not an error
  BIO* bio = BIO_new_mem_buf(trustPem, -1);
  if (!bio) return false;
  X509_STORE* store = SSL_CTX_get_cert_store(ctx);
  bool loadedAny = false;
  for (X509* ca = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr); ca != nullptr;
       ca = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr)) {
    X509_STORE_add_cert(store, ca);
    X509_free(ca);
    loadedAny = true;
  }
  BIO_free(bio);
  return loadedAny;
}

// Attaches fresh memory BIOs to `ssl`. On success ssl_ owns both (freed by SSL_free).
// On failure, whatever was allocated is freed here and both out-params are left null.
bool attachMemoryBios(SSL* ssl, BIO*& rbio, BIO*& wbio) {
  rbio = BIO_new(BIO_s_mem());
  wbio = BIO_new(BIO_s_mem());
  if (!rbio || !wbio) {
    if (rbio) BIO_free(rbio);
    if (wbio) BIO_free(wbio);
    rbio = wbio = nullptr;
    return false;
  }
  // -1 => "no data yet, would block" rather than treating empty as a hard EOF — correct
  // for a pipe BIO with no real socket behind it.
  BIO_set_mem_eof_return(rbio, -1);
  BIO_set_mem_eof_return(wbio, -1);
  SSL_set_bio(ssl, rbio, wbio);  // ssl now owns rbio/wbio
  return true;
}
}  // namespace

void TlsSession::release() noexcept {
  // SSL_free() also frees rbio_/wbio_ once SSL_set_bio() attached them to ssl_.
  if (ssl_) SSL_free(ssl_);
  if (ctx_) SSL_CTX_free(ctx_);
  ssl_ = nullptr;
  ctx_ = nullptr;
  rbio_ = nullptr;
  wbio_ = nullptr;
}

TlsSession::~TlsSession() { release(); }

TlsSession::TlsSession(TlsSession&& o) noexcept
    : ctx_(o.ctx_), ssl_(o.ssl_), rbio_(o.rbio_), wbio_(o.wbio_), failed_(o.failed_) {
  o.ctx_ = nullptr;
  o.ssl_ = nullptr;
  o.rbio_ = nullptr;
  o.wbio_ = nullptr;
}

TlsSession& TlsSession::operator=(TlsSession&& o) noexcept {
  if (this != &o) {
    release();
    ctx_ = o.ctx_;
    ssl_ = o.ssl_;
    rbio_ = o.rbio_;
    wbio_ = o.wbio_;
    failed_ = o.failed_;
    o.ctx_ = nullptr;
    o.ssl_ = nullptr;
    o.rbio_ = nullptr;
    o.wbio_ = nullptr;
  }
  return *this;
}

bool TlsSession::valid() const noexcept { return ctx_ && ssl_ && rbio_ && wbio_; }

TlsSession TlsSession::makeServer(const char* certPem, const char* keyPem) noexcept {
  TlsSession s;
  s.ctx_ = SSL_CTX_new(TLS_server_method());
  if (!s.ctx_) return s;
  SSL_CTX_set_min_proto_version(s.ctx_, TLS1_3_VERSION);
  SSL_CTX_set_max_proto_version(s.ctx_, TLS1_3_VERSION);

  if (!loadServerIdentity(s.ctx_, certPem, keyPem)) {
    s.release();
    return s;
  }

  s.ssl_ = SSL_new(s.ctx_);
  if (!s.ssl_ || !attachMemoryBios(s.ssl_, s.rbio_, s.wbio_)) {
    s.release();
    return s;
  }
  SSL_set_accept_state(s.ssl_);
  return s;
}

TlsSession TlsSession::makeClient(const char* trustPemOrNull, const char* expectHost) noexcept {
  TlsSession s;
  s.ctx_ = SSL_CTX_new(TLS_client_method());
  if (!s.ctx_) return s;
  SSL_CTX_set_min_proto_version(s.ctx_, TLS1_3_VERSION);
  SSL_CTX_set_max_proto_version(s.ctx_, TLS1_3_VERSION);
  SSL_CTX_set_verify(s.ctx_, SSL_VERIFY_PEER, nullptr);

  bool trustOk = trustPemOrNull == nullptr ? (SSL_CTX_set_default_verify_paths(s.ctx_) == 1)
                                            : loadTrustBundle(s.ctx_, trustPemOrNull);
  if (!trustOk) {
    s.release();
    return s;
  }

  s.ssl_ = SSL_new(s.ctx_);
  if (!s.ssl_) {
    s.release();
    return s;
  }
  if (expectHost && expectHost[0]) {
    SSL_set1_host(s.ssl_, expectHost);          // hostname check against the cert SAN
    SSL_set_tlsext_host_name(s.ssl_, expectHost);  // SNI
  }
  if (!attachMemoryBios(s.ssl_, s.rbio_, s.wbio_)) {
    s.release();
    return s;
  }
  SSL_set_connect_state(s.ssl_);
  return s;
}

HsState TlsSession::readInbound(const uint8_t* buf, size_t len) noexcept {
  if (!valid() || failed_) return HsState::Failed;
  if (buf && len) BIO_write(rbio_, buf, static_cast<int>(len));

  if (SSL_is_init_finished(ssl_)) return HsState::Done;

  int rc = SSL_do_handshake(ssl_);
  if (rc == 1) {
    return SSL_is_init_finished(ssl_) ? HsState::Done : HsState::NeedInput;
  }

  int err = SSL_get_error(ssl_, rc);
  if (err == SSL_ERROR_WANT_READ) {
    // There may still be outbound bytes buffered from the part of the handshake that did
    // progress before hitting the empty rbio (e.g. the server's flight after ClientHello).
    return BIO_pending(wbio_) > 0 ? HsState::HaveOutput : HsState::NeedInput;
  }
  if (err == SSL_ERROR_WANT_WRITE) {
    return HsState::HaveOutput;
  }

  // Hard failure: bad record, cert verification failure, protocol error, etc. Don't hang —
  // surface it so pump()/callers stop retrying.
  failed_ = true;
  ERR_clear_error();
  return HsState::Failed;
}

size_t TlsSession::writeOutbound(uint8_t* out, size_t cap) noexcept {
  if (!valid() || !out || !cap) return 0;
  int n = BIO_read(wbio_, out, static_cast<int>(cap));
  return n > 0 ? static_cast<size_t>(n) : 0;
}

bool TlsSession::done() const noexcept {
  return valid() && !failed_ && SSL_is_init_finished(ssl_);
}

SessionKeys TlsSession::exportKeys(Cipher negotiated, int64_t sessionId) const noexcept {
  SessionKeys keys{};
  keys.cipher = negotiated;
  if (!valid()) return keys;

  // Bind every export to sessionId so the same TLS connection can't be reused to derive
  // the same directional key material for a different logical session.
  uint8_t ctx[8];
  for (int i = 0; i < 8; ++i) ctx[i] = static_cast<uint8_t>(sessionId >> (8 * (7 - i)));

  static constexpr char kC2bKey[] = "rune c2b key";
  static constexpr char kB2cKey[] = "rune b2c key";
  static constexpr char kC2bIv[] = "rune c2b iv";
  static constexpr char kB2cIv[] = "rune b2c iv";

  SSL_export_keying_material(const_cast<SSL*>(ssl_), keys.c2b.bytes, sizeof keys.c2b.bytes,
                              kC2bKey, sizeof(kC2bKey) - 1, ctx, sizeof ctx, 1);
  SSL_export_keying_material(const_cast<SSL*>(ssl_), keys.b2c.bytes, sizeof keys.b2c.bytes,
                              kB2cKey, sizeof(kB2cKey) - 1, ctx, sizeof ctx, 1);

  uint8_t ivbuf[4];
  SSL_export_keying_material(const_cast<SSL*>(ssl_), ivbuf, sizeof ivbuf, kC2bIv,
                              sizeof(kC2bIv) - 1, ctx, sizeof ctx, 1);
  std::memcpy(keys.c2bPrefix, ivbuf, sizeof ivbuf);
  SSL_export_keying_material(const_cast<SSL*>(ssl_), ivbuf, sizeof ivbuf, kB2cIv,
                              sizeof(kB2cIv) - 1, ctx, sizeof ctx, 1);
  std::memcpy(keys.b2cPrefix, ivbuf, sizeof ivbuf);

  return keys;
}

}  // namespace rune::crypto
