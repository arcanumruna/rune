#pragma once
#include <cstddef>
#include <cstdint>
#include "rune/crypto/Aead.h"
#include "rune/crypto/Cipher.h"

typedef struct ssl_st SSL;
typedef struct ssl_ctx_st SSL_CTX;
typedef struct bio_st BIO;

namespace rune::crypto {

enum class HsState : uint8_t { NeedInput, HaveOutput, Done, Failed };

struct SessionKeys {
  Cipher cipher;
  AeadKey c2b;
  AeadKey b2c;
  uint8_t c2bPrefix[4];
  uint8_t b2cPrefix[4];
};

// TLS 1.3 handshake run over in-memory BIOs (never a real socket) — handshake bytes get
// carried inside Rune's own Connect/ConnectResponse frames by the caller. One TlsSession
// per connection attempt; not shared across threads (shared-nothing).
class TlsSession {
 public:
  // Broker side. certPem/keyPem are PEM text (not file paths).
  static TlsSession makeServer(const char* certPem, const char* keyPem) noexcept;
  // Client side. trustPemOrNull: nullptr = trust the system store; "" = trust nothing
  // (explicitly untrusted, for tests); non-empty = PEM bundle of CA certs to trust.
  // expectHost is matched against the server cert's SAN: a DNS name (SNI + hostname verification) or an IP literal
  // (iPAddress SAN, no SNI).  "" installs no name check, which only a pinned bundle allows: system trust with no
  // expectHost returns an invalid session (issue #115 - any public certificate for any name would pass).
  static TlsSession makeClient(const char* trustPemOrNull, const char* expectHost) noexcept;

  ~TlsSession();
  TlsSession(TlsSession&& o) noexcept;
  TlsSession& operator=(TlsSession&& o) noexcept;
  TlsSession(const TlsSession&) = delete;
  TlsSession& operator=(const TlsSession&) = delete;

  // False if construction itself failed (bad PEM, OpenSSL alloc failure, etc).
  bool valid() const noexcept;

  // Feed peer handshake bytes and advance the handshake. `buf` may be null/len 0 (the
  // client's first call, to kick off ClientHello generation with no peer input yet).
  HsState readInbound(const uint8_t* buf, size_t len) noexcept;
  // Pull handshake bytes to send to the peer. Returns bytes written to `out` (<= cap),
  // 0 if there is nothing pending.
  size_t writeOutbound(uint8_t* out, size_t cap) noexcept;
  bool done() const noexcept;

  // Valid only after done(). Derives the two directional AEAD keys + 4-byte nonce
  // prefixes via the TLS 1.3 exporter. Same sessionId + role-correct direction labels on
  // both sides yields identical keys (see TlsSessionTest.HandshakeSucceedsAndKeysAgree).
  SessionKeys exportKeys(Cipher negotiated, int64_t sessionId) const noexcept;

 private:
  TlsSession() noexcept = default;
  void release() noexcept;

  SSL_CTX* ctx_ = nullptr;
  SSL* ssl_ = nullptr;
  BIO* rbio_ = nullptr;  // owned by ssl_ once SSL_set_bio succeeds
  BIO* wbio_ = nullptr;  // owned by ssl_ once SSL_set_bio succeeds
  bool failed_ = false;
};

}  // namespace rune::crypto
