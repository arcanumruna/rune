# Client↔Broker Transport Encryption (TLS/cert + per-session AEAD) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Encrypt the client↔broker Aeron link (produce, ack/egress, and the connect handshake) with per-session authenticated encryption and a Let's-Encrypt-issued broker certificate, adding < 5–10 µs to a single/small produce→ack and zero heap allocation on the hot path.

**Architecture:** A one-time **TLS 1.3 handshake** (OpenSSL driven through memory BIOs) is carried over the existing `Connect`/`ConnectResponse` plus two new continuation messages, authenticating the broker via its cert and deriving per-direction symmetric keys. The steady-state data path then wraps each application frame in a **crypto envelope** — the existing 24-byte `MessageHeader` stays in the clear (so `sessionId`/`templateId` routing and the in-place sessionId patch keep working and become AEAD AAD), a 12-byte crypto subheader (`ver/cipher/epoch/seq`) follows, the message **body** is AEAD-sealed, and a 16-byte tag is appended (28 B total overhead). Cipher is chosen per connection from CPU capability (AES-256-GCM on VAES/AES-NI/ARMv8-crypto → ChaCha20-Poly1305 fallback). The AEAD **format** is header-only in `rune-protocol`; the OpenSSL-backed AEAD + handshake **operations** live in a new sibling static lib `rune-crypto` linked PRIVATE into only the broker gateway and client transport.

**Tech Stack:** C++23, Aeron (UDP transport, already present), **OpenSSL 3.x** (new — handshake + data-path EVP AEAD, both runtime-dispatched), Conan 2.x, GoogleTest, rune-fr (overhead measurement). Repos: `rune-protocol` (wire), new `rune-crypto` (crypto core), `rune-broker` (gateway + worker), `rune-client-cpp` (transport + session).

**Spec:** `docs/specs/2026-10-07-client-broker-tls-design.md` (in this worktree). The plan refines spec §6.1 to the exact envelope layout below and drives the handshake over two new continuation messages; everything else follows the spec.

## Global Constraints

Every task implicitly includes these.

- **Scope:** client↔broker link only (produce ingress, ack/egress, connect handshake). **No** node↔node encryption.
- **Latency budget:** single/small produce→ack adds **< 5–10 µs**; a full 64 KiB batch is ~5 µs/pass on the VAES fast path, best-effort on fallback. Bench-gated (Task 16) — no "done" without the numbers.
- **Zero allocation on the hot path** (Rule 4): reuse one `EVP_CIPHER_CTX` per session per direction; encrypt into the existing claim/`wireFrame`/`scratch_` buffers; no per-message key setup, no RNG.
- **Shared-nothing (Rule 1):** per-session crypto state owned by one thread; the only cross-thread key delivery (per-worker ingress mode) goes through a WorkerOp ring message (Rule 2), never shared memory.
- **No header-version bump.** `rune::protocol::client::kProtocolVersion` (=3, `Protocol.h:6`) stays. Additive wire changes are trailing fields gated by `blockLength`/`avail` plus capability ints `kClientVersion`/`kBrokerVersion` (3 → **4**), mirroring `ConnectResponse.brokerVersion` (`ControlMessages.h:160-162`).
- **OpenSSL isolation.** OpenSSL is a dependency of **`rune-crypto` only**, linked PRIVATE into the gateway (`rune_node`/`rune_gateway`) and client transport (`rune_client_transport`) targets. It must NOT reach the header-only `rune-protocol` nor tiny targets (`ping_probe`, examples, benchmarks).
- **Migration toggle:** CMake option + runtime `RUNE_TLS`. Off → today's plaintext path, zero added hot-path code. A session is plaintext or encrypted per its handshake outcome; `require_tls` makes the broker refuse plaintext.
- **Little-endian** wire (matches existing `Codec.h` `putLE*`/`getLE*`).
- **Crypto schema id:** `kCryptoSchemaId = 121` reserved for the crypto envelope (distinct from client wire `kSchemaId = 120`).
- **Build/test:** conan-install + cmake-toolchain (NOT `cmake --preset`); `ctest --output-on-failure`; client uses the `x86_64 / compiler.cppstd=gnu17` profile (CLAUDE.md) to match the Aeron binary cache.

## Review Focus

Input classes/failure modes the spec implies; each is pinned to the task that tests it.

- **Tampered ciphertext / forged frame** → AEAD tag fails, frame dropped, session unharmed. → Task 2 (`AeadCipher` tamper test) + Task 15 (e2e security regression).
- **Replayed / reordered frame** (duplicate `seq`, old `seq`) → dropped by the per-session replay window; in-window reorder accepted. → Task 6 (client) / Task 8 (broker) + Task 15.
- **Certificate failure** (wrong host, expired, untrusted) → handshake aborts, session rejected with `ErrorCode::HandshakeFailed` (29), non-retriable. → Task 3 (`TlsSession` verify test) + Task 5.
- **Downgrade / version-skew** (TLS client ↔ non-TLS broker or vice-versa; `require_tls`) → negotiated safely via capability ints; a `require_tls` broker refuses plaintext; an old peer that offered no cipher stays plaintext. → Task 4 (additive decode back-compat) + Task 9 (`require_tls`).
- **Handshake blob larger than the scratch buffer** (`SessionMachine` `kScratchCap=512`, `SessionMachine.h:150`; TLS ClientHello/cert chains exceed this) → must not overflow; handshake bytes use a dedicated growable/bounded buffer and `putVarBytes32`. → Task 4 (envelope/message size) + Task 5 (client drive).

---

## File Structure

**New lib `rune-crypto`** (sibling of rune-fr/rune-codec; `../rune-crypto`):
- `rune-crypto/CMakeLists.txt`, `conanfile.py`, `conandata.yml`, `VERSION.txt` — static lib, requires `openssl/3.3.2`.
- `include/rune/crypto/Cipher.h` — `enum class Cipher`, `bestLocalCipher()`, caps detection.
- `include/rune/crypto/Aead.h` + `src/Aead.cpp` — `AeadCipher` (seal/open over EVP).
- `include/rune/crypto/TlsSession.h` + `src/TlsSession.cpp` — handshake driver (memory-BIO), cert load/verify, key export, cipher negotiation.
- `tests/AeadTest.cpp`, `tests/TlsSessionTest.cpp`, `tests/testdata/` (self-signed test cert/key).

**rune-protocol** (header-only; format only, no OpenSSL):
- `include/rune/protocol/client/CryptoEnvelope.h` — envelope header encode/decode + `isCryptoFrame()`.
- Modify `include/rune/protocol/client/Protocol.h` — `TemplateId` 10/11, `ErrorCode::HandshakeFailed=29`, `kClientVersion/kBrokerVersion → 4`, `kCryptoSchemaId=121`.
- Modify `include/rune/protocol/client/ControlMessages.h` — additive fields on `ConnectRequest`/`ConnectResponse`; new `HandshakeContinue`/`HandshakeContinueResponse`.
- `tests/protocol/client/CryptoEnvelopeTest.cpp`, extend `ControlMessagesConnectTest.cpp`.

**rune-client-cpp:**
- Modify `include/rune/client/session/SessionMachine.h` — handshake state + `CryptoState`.
- Modify `src/agent/ClientAgent.cpp` — encrypt in `offerProduceToLeader_` (after the sessionId patch), decrypt in `dispatchEgressFrame`; handshake dispatch.
- Modify `include/rune/client/transport/AeronTransport.h` + `.cpp` — per-session seal scratch.
- Modify `CMakeLists.txt`/`conandata.yml` — openssl + rune_crypto.
- `tests/crypto/ClientCryptoRoundTripTest.cpp`.

**rune-broker:**
- Modify `libs/gateway/include/rune/gateway/SessionTable.h` — `CryptoState` on `Session`; `ClientGateway.h` `PendingConnect` handshake state.
- Modify `libs/gateway/src/ClientGateway.cpp` — `handleConnect` server handshake, `onFragment` decrypt, `sendOnPub` encrypt, `HandshakeContinue` dispatch.
- Modify `libs/node/.../WorkerOp.h` + `schema/rune-internal-ring.xml` + worker — `InstallSessionKey` op (Task 14, gated).
- Modify `CMakeLists.txt`/`conandata.yml` — openssl + rune_crypto.
- `libs/gateway/tests/GatewayCryptoTest.cpp`; extend `docker/e2e-data.sh`.

**Shared crypto state struct** (defined once, mirrored both sides):
```cpp
// per session, per direction — lives in rune-crypto as a plain POD
struct rune::crypto::DirectionKeys { Cipher cipher; uint16_t epoch; AeadKey key; uint64_t seq; };
```

---

## Phase A — `rune-crypto` core library

### Task 1: Scaffold `rune-crypto` static lib + OpenSSL dependency

**Files:**
- Create: `../rune-crypto/{VERSION.txt,.gitignore,CMakeLists.txt,conanfile.py,conandata.yml,README.md}`
- Create: `../rune-crypto/include/rune/crypto/Cipher.h`, `../rune-crypto/src/Caps.cpp`
- Test: `../rune-crypto/tests/CapsTest.cpp`

**Interfaces:**
- Produces: `namespace rune::crypto { enum class Cipher : uint8_t { None=0, Aes256Gcm=1, ChaCha20Poly1305=2 }; Cipher bestLocalCipher() noexcept; bool hasHardwareAes() noexcept; }`

- [ ] **Step 1: Write `conandata.yml` + `conanfile.py` + `CMakeLists.txt`**

`conandata.yml`:
```yaml
requirements:
  - "openssl/3.3.2"
test_requirements:
  - "gtest/1.16.0"
```
`conanfile.py` — mirror the sibling library recipe (`rune-protocol/conanfile.py` shape): `package_type="library"`, `settings="os","compiler","build_type","arch"`, `requirements()` reading `conan_data["requirements"]`, `build_requirements()` reading `test_requirements`, `generate()` = `CMakeDeps`+`CMakeToolchain`, `layout()` = `cmake_layout`, `package_info()` sets `cmake_target_name="rune-crypto"`, `libs=["rune_crypto"]`, `includedirs=["include"]`.
`CMakeLists.txt`:
```cmake
cmake_minimum_required(VERSION 3.31)
project(rune_crypto CXX)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(OpenSSL REQUIRED)
add_library(rune_crypto STATIC src/Caps.cpp src/Aead.cpp src/TlsSession.cpp)
target_include_directories(rune_crypto PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include> $<INSTALL_INTERFACE:include>)
target_link_libraries(rune_crypto PUBLIC OpenSSL::SSL OpenSSL::Crypto)
option(BUILD_TESTING "Build tests" OFF)
if(BUILD_TESTING)
    enable_testing()
    find_package(GTest REQUIRED)
    include(GoogleTest)
    add_executable(rune_crypto_tests tests/CapsTest.cpp tests/AeadTest.cpp tests/TlsSessionTest.cpp)
    target_link_libraries(rune_crypto_tests PRIVATE rune_crypto GTest::gtest_main)
    gtest_discover_tests(rune_crypto_tests)
endif()
```
> `src/Aead.cpp`, `src/TlsSession.cpp`, `tests/AeadTest.cpp`, `tests/TlsSessionTest.cpp` are created in Tasks 2–3; for Task 1 create each as a one-line stub (`#include "rune/crypto/Cipher.h"`) so the lib links.

- [ ] **Step 2: Write `include/rune/crypto/Cipher.h`**
```cpp
#pragma once
#include <cstdint>
namespace rune::crypto {
enum class Cipher : uint8_t { None = 0, Aes256Gcm = 1, ChaCha20Poly1305 = 2 };
// True when OpenSSL will use a hardware AES-GCM implementation (AES-NI/VAES/ARMv8).
bool hasHardwareAes() noexcept;
// AES-256-GCM when hardware AES is present, else ChaCha20-Poly1305 (the no-HW-AES floor).
Cipher bestLocalCipher() noexcept;
}
```

- [ ] **Step 3: Write the failing test `tests/CapsTest.cpp`**
```cpp
#include <gtest/gtest.h>
#include "rune/crypto/Cipher.h"
TEST(Caps, BestCipherIsOneOfTheTwo) {
    auto c = rune::crypto::bestLocalCipher();
    EXPECT_TRUE(c == rune::crypto::Cipher::Aes256Gcm || c == rune::crypto::Cipher::ChaCha20Poly1305);
}
TEST(Caps, HardwareAesImpliesAesPreferred) {
    if (rune::crypto::hasHardwareAes())
        EXPECT_EQ(rune::crypto::bestLocalCipher(), rune::crypto::Cipher::Aes256Gcm);
    else
        EXPECT_EQ(rune::crypto::bestLocalCipher(), rune::crypto::Cipher::ChaCha20Poly1305);
}
// Non-tautological: validate hasHardwareAes() against an INDEPENDENT ground truth
// on x86_64 (the compiler's own CPUID), so a wrong ia32cap word/bit is caught.
TEST(Caps, HardwareAesMatchesIndependentGroundTruth) {
#if defined(__x86_64__) || defined(_M_X64)
    __builtin_cpu_init();
    EXPECT_EQ(rune::crypto::hasHardwareAes(), __builtin_cpu_supports("aes") != 0);
#else
    GTEST_SKIP() << "independent AES-NI ground truth only defined on x86_64";
#endif
}
```

- [ ] **Step 4: Run — verify fail (link error: `bestLocalCipher` undefined)**
```bash
cd ../rune-crypto && conan install . -of build -s build_type=Debug -s arch=x86_64 --build=missing
cmake -S . -B build -DBUILD_TESTING=ON -DCMAKE_TOOLCHAIN_FILE=build/Debug/generators/conan_toolchain.cmake -DCMAKE_PREFIX_PATH="$PWD/build/Debug/generators"
cmake --build build -j && (cd build && ctest -R Caps --output-on-failure)
```
Expected: FAIL (undefined `bestLocalCipher`/`hasHardwareAes`).

- [ ] **Step 5: Implement `src/Caps.cpp`**
```cpp
#include "rune/crypto/Cipher.h"
#include <openssl/evp.h>
#include <openssl/crypto.h>
namespace rune::crypto {
bool hasHardwareAes() noexcept {
    // OPENSSL_ia32cap bit 57 = AES-NI on x86; on other arches OpenSSL still
    // runtime-selects HW AES-GCM, so fall back to "AES-GCM fetch succeeds + fast".
#if defined(__x86_64__) || defined(_M_X64)
    // AES-NI is capability bit 57. OPENSSL_ia32cap_P is an array of 32-bit words,
    // so bit 57 lives in word[1] at local bit 25 (57-32) — matching OpenSSL's own
    // AESNI_CAPABLE (crypto/aes_platform.h). (word[0] & (1ULL<<57) is always 0.)
    return (OPENSSL_ia32cap_P[1] & (1u << (57 - 32))) != 0;
#else
    return true; // ARMv8 crypto / other: OpenSSL picks HW impl when present
#endif
}
Cipher bestLocalCipher() noexcept {
    return hasHardwareAes() ? Cipher::Aes256Gcm : Cipher::ChaCha20Poly1305;
}
}
```
> `OPENSSL_ia32cap_P` is declared by `<openssl/crypto.h>`. If the target arch macro is unavailable, keep the `#else` branch.

- [ ] **Step 6: Run — verify pass; commit**
```bash
cmake --build build -j && (cd build && ctest -R Caps --output-on-failure)
git -C ../rune-crypto add -A && git -C ../rune-crypto commit -m "feat: rune-crypto scaffold + CPU cipher selection"
```
Expected: PASS.

### Task 2: `AeadCipher` — per-session seal/open

**Files:**
- Create: `../rune-crypto/include/rune/crypto/Aead.h`, replace stub `src/Aead.cpp`
- Test: `../rune-crypto/tests/AeadTest.cpp`

**Interfaces:**
- Consumes: `Cipher` (Task 1).
- Produces:
```cpp
namespace rune::crypto {
struct AeadKey { uint8_t bytes[32]; };            // 256-bit
inline constexpr size_t kTagSize = 16;
inline constexpr size_t kNoncePrefix = 4;         // 4-byte fixed + 8-byte seq = 96-bit nonce
class AeadCipher {
public:
    // noncePrefix: 4 bytes bound to (sessionId, direction) so c2b/b2c never share a nonce space.
    AeadCipher(Cipher c, const AeadKey& key, const uint8_t noncePrefix[kNoncePrefix]) noexcept;
    ~AeadCipher();
    AeadCipher(AeadCipher&&) noexcept; AeadCipher& operator=(AeadCipher&&) noexcept;
    AeadCipher(const AeadCipher&) = delete; AeadCipher& operator=(const AeadCipher&) = delete;
    // out must have room for plainLen + kTagSize. Returns bytes written (plainLen + 16). noexcept, no alloc.
    size_t seal(uint64_t seq, const uint8_t* aad, size_t aadLen,
                const uint8_t* plain, size_t plainLen, uint8_t* out) noexcept;
    // out must have room for ctLen - kTagSize. Returns plaintext length, or -1 on auth failure. no alloc.
    long open(uint64_t seq, const uint8_t* aad, size_t aadLen,
              const uint8_t* ct, size_t ctLen, uint8_t* out) noexcept;
private:
    EVP_CIPHER_CTX* ctx_; const EVP_CIPHER* evp_; uint8_t prefix_[kNoncePrefix];
};
}
```

- [ ] **Step 1: Write the failing test `tests/AeadTest.cpp`**
```cpp
#include <gtest/gtest.h>
#include <cstring>
#include <vector>
#include "rune/crypto/Aead.h"
using namespace rune::crypto;
static AeadKey k1() { AeadKey k{}; for (int i=0;i<32;++i) k.bytes[i]=uint8_t(i+1); return k; }
static void each(const std::function<void(Cipher)>& f){ f(Cipher::Aes256Gcm); f(Cipher::ChaCha20Poly1305); }

TEST(Aead, RoundTripRecoversPlaintext) {
  each([](Cipher c){
    uint8_t pfx[4]={1,2,3,4};
    AeadCipher s(c,k1(),pfx), o(c,k1(),pfx);
    const char* msg="produce-batch-body"; uint8_t aad[8]={9,8,7,6,5,4,3,2};
    std::vector<uint8_t> ct(strlen(msg)+kTagSize), pt(strlen(msg));
    size_t n=s.seal(1,aad,8,(const uint8_t*)msg,strlen(msg),ct.data());
    ASSERT_EQ(n,strlen(msg)+kTagSize);
    long m=o.open(1,aad,8,ct.data(),n,pt.data());
    ASSERT_EQ(m,(long)strlen(msg));
    EXPECT_EQ(0,memcmp(pt.data(),msg,strlen(msg)));
  });
}
TEST(Aead, TamperedCiphertextFailsOpen) {
  each([](Cipher c){
    uint8_t pfx[4]={1,2,3,4}; AeadCipher s(c,k1(),pfx), o(c,k1(),pfx);
    uint8_t in[16]={0}; std::vector<uint8_t> ct(16+kTagSize), pt(16);
    size_t n=s.seal(1,nullptr,0,in,16,ct.data());
    ct[3]^=0x40; // flip a ciphertext byte
    EXPECT_EQ(-1, o.open(1,nullptr,0,ct.data(),n,pt.data()));
  });
}
TEST(Aead, WrongSeqOrAadFailsOpen) {
  each([](Cipher c){
    uint8_t pfx[4]={1,2,3,4}; AeadCipher s(c,k1(),pfx), o(c,k1(),pfx);
    uint8_t in[16]={0}, aad[4]={1,1,1,1}; std::vector<uint8_t> ct(16+kTagSize), pt(16);
    size_t n=s.seal(5,aad,4,in,16,ct.data());
    EXPECT_EQ(-1,o.open(6,aad,4,ct.data(),n,pt.data()));   // wrong seq (nonce)
    EXPECT_EQ(-1,o.open(5,nullptr,0,ct.data(),n,pt.data())); // wrong aad
  });
}
```

- [ ] **Step 2: Run — verify fail** (`AeadCipher` undefined). Command as Task 1 Step 4 with `-R Aead`.

- [ ] **Step 3: Implement `include/rune/crypto/Aead.h` + `src/Aead.cpp`**

Header: the interface block above. Implementation:
```cpp
#include "rune/crypto/Aead.h"
#include <openssl/evp.h>
#include <cstring>
namespace rune::crypto {
static const EVP_CIPHER* evpFor(Cipher c){
  return c==Cipher::Aes256Gcm ? EVP_aes_256_gcm() : EVP_chacha20_poly1305();
}
AeadCipher::AeadCipher(Cipher c, const AeadKey& key, const uint8_t pfx[kNoncePrefix]) noexcept
  : ctx_(EVP_CIPHER_CTX_new()), evp_(evpFor(c)) {
  std::memcpy(prefix_, pfx, kNoncePrefix);
  // Install the key once; nonce is set per call (key schedule expanded once).
  EVP_CipherInit_ex(ctx_, evp_, nullptr, key.bytes, nullptr, -1);
}
AeadCipher::~AeadCipher(){ if(ctx_) EVP_CIPHER_CTX_free(ctx_); }
AeadCipher::AeadCipher(AeadCipher&& o) noexcept : ctx_(o.ctx_), evp_(o.evp_){ std::memcpy(prefix_,o.prefix_,kNoncePrefix); o.ctx_=nullptr; }
AeadCipher& AeadCipher::operator=(AeadCipher&& o) noexcept { if(this!=&o){ if(ctx_)EVP_CIPHER_CTX_free(ctx_); ctx_=o.ctx_; evp_=o.evp_; std::memcpy(prefix_,o.prefix_,kNoncePrefix); o.ctx_=nullptr;} return *this; }

static void makeNonce(const uint8_t pfx[4], uint64_t seq, uint8_t nonce[12]){
  std::memcpy(nonce, pfx, 4);
  for(int i=0;i<8;++i) nonce[4+i]=uint8_t(seq>>(8*(7-i))); // big-endian seq
}
size_t AeadCipher::seal(uint64_t seq,const uint8_t* aad,size_t aadLen,
                        const uint8_t* plain,size_t plainLen,uint8_t* out) noexcept {
  uint8_t nonce[12]; makeNonce(prefix_,seq,nonce);
  int len=0;
  EVP_EncryptInit_ex(ctx_,nullptr,nullptr,nullptr,nonce); // reuse key, new nonce
  if(aadLen){ EVP_EncryptUpdate(ctx_,nullptr,&len,aad,(int)aadLen); }
  EVP_EncryptUpdate(ctx_,out,&len,plain,(int)plainLen);
  int fin=0; EVP_EncryptFinal_ex(ctx_,out+len,&fin);
  EVP_CIPHER_CTX_ctrl(ctx_,EVP_CTRL_AEAD_GET_TAG,(int)kTagSize,out+plainLen);
  return plainLen+kTagSize;
}
long AeadCipher::open(uint64_t seq,const uint8_t* aad,size_t aadLen,
                      const uint8_t* ct,size_t ctLen,uint8_t* out) noexcept {
  if(ctLen<kTagSize) return -1;
  size_t bodyLen=ctLen-kTagSize;
  uint8_t nonce[12]; makeNonce(prefix_,seq,nonce);
  int len=0;
  EVP_DecryptInit_ex(ctx_,nullptr,nullptr,nullptr,nonce);
  if(aadLen){ EVP_DecryptUpdate(ctx_,nullptr,&len,aad,(int)aadLen); }
  EVP_DecryptUpdate(ctx_,out,&len,ct,(int)bodyLen);
  EVP_CIPHER_CTX_ctrl(ctx_,EVP_CTRL_AEAD_SET_TAG,(int)kTagSize,(void*)(ct+bodyLen));
  int fin=0;
  if(EVP_DecryptFinal_ex(ctx_,out+len,&fin)<=0) return -1; // tag mismatch
  return (long)bodyLen;
}
}
```
> Note: `EVP_*Init_ex(ctx,NULL,NULL,NULL,nonce)` keeps the installed key and only resets the IV — the expensive key schedule happens once at construction. One ctx per direction (not shared) satisfies the single-producer rule.

- [ ] **Step 4: Run — verify pass; commit** (`-R Aead`). Commit `feat: AeadCipher (AES-256-GCM / ChaCha20-Poly1305) seal/open`.

### Task 3: `TlsSession` — memory-BIO handshake driver

**Files:**
- Create: `../rune-crypto/include/rune/crypto/TlsSession.h`, replace stub `src/TlsSession.cpp`
- Create: `../rune-crypto/tests/testdata/gen.sh` (+ generated `ca.pem`, `server.pem`, `server.key` for `localhost`)
- Test: `../rune-crypto/tests/TlsSessionTest.cpp`

**Interfaces:**
- Consumes: `Cipher`, `AeadKey` (Tasks 1–2).
- Produces:
```cpp
namespace rune::crypto {
enum class HsState : uint8_t { NeedInput, HaveOutput, Done, Failed };
struct SessionKeys { Cipher cipher; AeadKey c2b; AeadKey b2c; uint8_t c2bPrefix[4]; uint8_t b2cPrefix[4]; };
class TlsSession {
public:
    static TlsSession makeServer(const char* certPem, const char* keyPem) noexcept; // broker
    static TlsSession makeClient(const char* trustPemOrNull, const char* expectHost) noexcept; // client (null trust = system store)
    bool valid() const noexcept;
    // Feed peer handshake bytes (may be empty on first client call to kick off).
    HsState readInbound(const uint8_t* buf, size_t len) noexcept;
    // Pull handshake bytes to send to peer; returns bytes written to out (<=cap), 0 if none.
    size_t writeOutbound(uint8_t* out, size_t cap) noexcept;
    bool done() const noexcept;
    // Valid only after done(): derive the two directional keys + nonce prefixes via the TLS exporter.
    SessionKeys exportKeys(Cipher negotiated, int64_t sessionId) const noexcept;
};
}
```

- [ ] **Step 1: Generate test certs `tests/testdata/gen.sh` and run it**
```bash
# self-signed CA + localhost server cert for the handshake test
openssl req -x509 -newkey ed25519 -keyout ca.key -out ca.pem -days 3650 -nodes -subj "/CN=Test CA"
openssl req -newkey ed25519 -keyout server.key -out server.csr -nodes -subj "/CN=localhost"
printf "subjectAltName=DNS:localhost\n" > san.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -out server.pem -days 3650 -extfile san.ext
```

- [ ] **Step 2: Write the failing test `tests/TlsSessionTest.cpp`**
```cpp
#include <gtest/gtest.h>
#include <vector>
#include "rune/crypto/TlsSession.h"
using namespace rune::crypto;
// read testdata files into std::string (helper omitted for brevity in plan; implement in test)
static std::string slurp(const char* p);
// Pump the two sessions until both report done() or either fails.
static bool pump(TlsSession& cli, TlsSession& srv) {
  uint8_t buf[16384];
  for (int i=0;i<20 && !(cli.done()&&srv.done());++i){
    size_t n=cli.writeOutbound(buf,sizeof buf); if(n) srv.readInbound(buf,n);
    n=srv.writeOutbound(buf,sizeof buf); if(n) cli.readInbound(buf,n);
    if(cli.done()==HsState::Failed||srv.done()==HsState::Failed) return false;
  }
  return cli.done()&&srv.done();
}
TEST(Tls, HandshakeSucceedsAndKeysAgree) {
  auto srv=TlsSession::makeServer(slurp("testdata/server.pem").c_str(), slurp("testdata/server.key").c_str());
  auto cli=TlsSession::makeClient(slurp("testdata/ca.pem").c_str(), "localhost");
  cli.readInbound(nullptr,0); // kick client to emit ClientHello
  ASSERT_TRUE(pump(cli,srv));
  auto ck=cli.exportKeys(Cipher::Aes256Gcm, 42);
  auto sk=srv.exportKeys(Cipher::Aes256Gcm, 42);
  EXPECT_EQ(0,memcmp(ck.c2b.bytes, sk.c2b.bytes, 32)); // both sides derive identical directional keys
  EXPECT_EQ(0,memcmp(ck.b2c.bytes, sk.b2c.bytes, 32));
}
TEST(Tls, WrongHostnameRejected) {
  auto srv=TlsSession::makeServer(slurp("testdata/server.pem").c_str(), slurp("testdata/server.key").c_str());
  auto cli=TlsSession::makeClient(slurp("testdata/ca.pem").c_str(), "wrong.example"); // not the cert SAN
  cli.readInbound(nullptr,0);
  EXPECT_FALSE(pump(cli,srv)); // verification failure aborts
}
TEST(Tls, UntrustedCertRejected) {
  auto srv=TlsSession::makeServer(slurp("testdata/server.pem").c_str(), slurp("testdata/server.key").c_str());
  auto cli=TlsSession::makeClient("", "localhost"); // empty trust: server cert is untrusted
  cli.readInbound(nullptr,0);
  EXPECT_FALSE(pump(cli,srv));
}
```

- [ ] **Step 3: Run — verify fail** (`TlsSession` undefined).

- [ ] **Step 4: Implement `TlsSession.h`/`.cpp`** — OpenSSL `SSL_CTX`/`SSL` with two `BIO` memory objects (`SSL_set_bio`). Key points:
  - Server: `SSL_CTX_new(TLS_server_method())`, `SSL_CTX_use_certificate/PrivateKey` from PEM strings (via `BIO_new_mem_buf` + `PEM_read_bio_*`), `SSL_set_accept_state`.
  - Client: `SSL_CTX_new(TLS_client_method())`, `SSL_CTX_set_verify(..., SSL_VERIFY_PEER, nullptr)`, load trust (`SSL_CTX_load_verify_file`/ mem CA or system default), `SSL_set1_host(ssl, expectHost)`, `SSL_set_connect_state`.
  - `readInbound`: `BIO_write(rbio, buf, len)`, then `SSL_do_handshake(ssl)`; map `SSL_get_error` (`WANT_READ`→NeedInput, else inspect bio) ; on `<=0` non-want → Failed.
  - `writeOutbound`: `BIO_read(wbio, out, cap)`.
  - `done()`: `SSL_is_init_finished(ssl)`.
  - `exportKeys`: `SSL_export_keying_material(ssl, out, 32, label, labelLen, nullptr, 0, 0)` with distinct labels `"rune c2b key"`, `"rune b2c key"`, and 4-byte prefixes exported with labels `"rune c2b iv"`/`"rune b2c iv"` mixed with `sessionId` as context. Both sides use the same labels → identical keys.
  - Enforce TLS 1.3 only: `SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION)`.

- [ ] **Step 5: Run — verify pass; commit.** Wire `testdata` copy into the test `WORKING_DIRECTORY` (`gtest_discover_tests(... WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/tests)`). Commit `feat: TlsSession memory-BIO 1.3 handshake + key export`.

---

## Phase B — wire format (`rune-protocol`)

### Task 4: Crypto envelope + handshake messages + version gates

**Files:**
- Create: `include/rune/protocol/client/CryptoEnvelope.h`
- Modify: `include/rune/protocol/client/Protocol.h`, `include/rune/protocol/client/ControlMessages.h`
- Test: `tests/protocol/client/CryptoEnvelopeTest.cpp`; extend `tests/protocol/client/ControlMessagesConnectTest.cpp`

**Interfaces:**
- Consumes: `Codec.h` `detail::putLE*/getLE*/putVarBytes32/getVarBytes32/putVarStr/getVarStr`; `MessageHeader` (24 B).
- Produces:
```cpp
namespace rune::protocol::client {
inline constexpr uint16_t kCryptoSchemaId = 121;
// Envelope sits BETWEEN the 24-byte MessageHeader (clear, AAD) and the encrypted body.
struct CryptoSubHeader {               // 12 bytes, LE, at offset 24
    uint8_t  version = 1;              // off 24
    uint8_t  cipher  = 0;              // off 25 (rune::crypto::Cipher value)
    uint16_t epoch   = 0;              // off 26
    uint64_t seq     = 0;              // off 28
    static constexpr std::size_t ENCODED_SIZE = 12;
    void encode(uint8_t* at) const noexcept;             // writes 12 bytes at `at`
    static std::optional<CryptoSubHeader> decode(const uint8_t*, std::size_t) noexcept;
};
inline constexpr std::size_t kCryptoHeaderBytes = 24 /*MessageHeader*/ + 12 /*sub*/; // = AAD length
inline constexpr std::size_t kCryptoOverhead = 12 /*sub*/ + 16 /*tag*/;              // = 28
}
```
Plus additive fields:
```cpp
// ConnectRequest (templateId 1) — trailing, gated by clientVersion>=4 / avail:
//   uint8_t cipherList[n] (var, putVarBytes), handshakeBytes (var32), authToken (varStr)
// ConnectResponse (templateId 2) — trailing, gated by brokerVersion>=4 / avail:
//   uint8_t chosenCipher, handshakeBytes (var32)
// New control messages:
struct HandshakeContinue        { int64_t sessionId, correlationId; /*hdr*/ std::string_view handshakeBytes; static constexpr uint16_t TEMPLATE_ID = 10; };
struct HandshakeContinueResponse{ int64_t sessionId, correlationId; int16_t code; std::string_view handshakeBytes; static constexpr uint16_t TEMPLATE_ID = 11; };
```

- [ ] **Step 1: Extend `Protocol.h`** — add to `TemplateId`: `HandshakeContinue = 10, HandshakeContinueResponse = 11` (free control slots). Add `ErrorCode::HandshakeFailed = 29`. Bump `kClientVersion = 4`, `kBrokerVersion = 4` and add gate consts `kClientVersionCrypto = 4`, `kBrokerVersionCrypto = 4`. Add `inline constexpr uint16_t kCryptoSchemaId = 121;`. Do NOT change `kProtocolVersion` (stays 3).

- [ ] **Step 2: Write failing test `tests/protocol/client/CryptoEnvelopeTest.cpp`**
```cpp
#include <gtest/gtest.h>
#include "rune/protocol/client/CryptoEnvelope.h"
using namespace rune::protocol::client;
TEST(CryptoEnvelope, SubHeaderRoundTrip) {
    uint8_t buf[64]={0};
    CryptoSubHeader h; h.version=1; h.cipher=1; h.epoch=7; h.seq=0x0102030405060708ull;
    h.encode(buf+24);
    auto d = CryptoSubHeader::decode(buf+24, 12);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->cipher,1); EXPECT_EQ(d->epoch,7); EXPECT_EQ(d->seq,0x0102030405060708ull);
}
TEST(CryptoEnvelope, OverheadConstants) {
    EXPECT_EQ(kCryptoHeaderBytes, 36u);
    EXPECT_EQ(kCryptoOverhead, 28u);
}
```
And extend `ControlMessagesConnectTest.cpp` with an additive-decode test:
```cpp
TEST(ConnectResponse, OldDecoderIgnoresNewCryptoField) {
    // a v4 ConnectResponse with chosenCipher still decodes its V0 fields; a V0-sized
    // buffer yields chosenCipher=0 (absent) — proves back-compat gating.
    // encode full, then decode with avail truncated to ENCODED_SIZE_V0 → crypto fields default.
}
TEST(HandshakeContinue, RoundTrip) {
    uint8_t buf[256]; const char* hs="\x16\x03\x03..."; // arbitrary handshake bytes
    HandshakeContinue m; m.sessionId=5; m.correlationId=9; m.handshakeBytes={hs,3};
    size_t n = encodeHandshakeContinue(buf, sizeof buf, m);
    auto d = HandshakeContinue::decode(buf, n);
    ASSERT_TRUE(d); EXPECT_EQ(d->sessionId,5); EXPECT_EQ(d->handshakeBytes.size(),3u);
}
```

- [ ] **Step 3: Run — verify fail.** (`conan install` + cmake per rune-protocol build.sh; `ctest -R "CryptoEnvelope|Connect|Handshake"`.)

- [ ] **Step 4: Implement `CryptoEnvelope.h`** (encode/decode mirroring `MessageHeader.h` `detail::putLE*`), the additive `ConnectRequest`/`ConnectResponse` fields (gate exactly like `brokerVersion`: `cipher = (hdr.blockLength >= BODY_FIXED_V4 && avail >= ENCODED_SIZE_V4) ? getLE8(...) : 0;`), and `HandshakeContinue`/`HandshakeContinueResponse` encode/decode + their `TemplateId`s. Handshake blobs use `putVarBytes32`/`getVarBytes32`.

- [ ] **Step 5: Run — verify pass; commit** `feat: crypto envelope + handshake wire messages + v4 additive connect fields`.

---

## Phase C — client integration (`rune-client-cpp`)

### Task 5: Client handshake drive + `CryptoState` in `SessionMachine`

**Files:**
- Modify: `include/rune/client/session/SessionMachine.h` (states + `CryptoState` + handshake methods)
- Modify: `src/agent/ClientAgent.cpp` (`dispatchEgressFrame` handshake cases; drive `TlsSession`)
- Modify: `CMakeLists.txt`, `conandata.yml` (add `openssl/3.3.2`, `find_package(OpenSSL)`, link `rune_crypto` + `OpenSSL::SSL/Crypto` PRIVATE into `rune_client_transport`/`rune_client_agent`)
- Test: `tests/crypto/ClientCryptoRoundTripTest.cpp` (handshake half, loopback against a `TlsSession` server)

**Interfaces:**
- Consumes: `TlsSession`, `AeadCipher`, `SessionKeys` (Tasks 2–3); `HandshakeContinue*`, v4 connect fields (Task 4).
- Produces (on `SessionMachine`):
```cpp
struct CryptoState { bool encrypted=false; rune::crypto::Cipher cipher{}; uint16_t epoch=0;
                     uint64_t sendSeq=0; uint64_t recvSeq=0; uint64_t recvWindow=0;
                     std::optional<rune::crypto::AeadCipher> send, recv; };
// SessionMachine gains: SessionState::Handshaking; onHandshakeBytes(buf,len); cryptoState();
```

- [ ] **Step 1–5 (TDD):** add `SessionState::Handshaking`; `connect()` now offers a v4 `ConnectRequest` carrying `cipherList={bestLocalCipher()}`, the first `TlsSession` ClientHello bytes (via a dedicated growable handshake buffer, NOT `kScratchCap=512`), and the auth token; `dispatchEgressFrame` routes `ConnectResponse`(with handshake bytes)/`HandshakeContinueResponse` into `TlsSession::readInbound` and emits `HandshakeContinue` until `done()`, then `exportKeys` → builds `CryptoState.send/recv` `AeadCipher`s and sets `encrypted=true`, then `transitionTo(Connected)`. On verify failure → `onErrorResponse(HandshakeFailed)` (non-retriable via `isNonRetriable_`). Test: loopback the client handshake half against a `TlsSession::makeServer` in-process, assert both derive equal keys and the machine reaches `Connected` with `encrypted`. Commit.

### Task 6: Client encrypt produce / decrypt egress

**Files:**
- Modify: `src/agent/ClientAgent.cpp` — `offerProduceToLeader_` (`~974-1006`), `dispatchEgressFrame` (`~675+`)
- Modify: `include/rune/client/transport/AeronTransport.h`/`.cpp` — per-session seal scratch
- Test: `tests/crypto/ClientCryptoRoundTripTest.cpp` (data half)

**Interfaces:**
- Consumes: `CryptoState` (Task 5), `CryptoSubHeader`/`kCryptoHeaderBytes`/`kCryptoOverhead` (Task 4), `AeadCipher::seal/open` (Task 2).

- [ ] **Step 1: Failing test** — a produce frame built by the client, sealed via the session `CryptoState`, then `open`ed with the broker-direction key, recovers the exact plaintext `[MessageHeader][body]`; a tampered byte fails; a replayed `seq` is rejected by `recvWindow`.
- [ ] **Step 2: Run — fail.**
- [ ] **Step 3: Implement.** In `offerProduceToLeader_`, AFTER the in-place sessionId patch (`detail::putLE64(buf,8,sid)` at `ClientAgent.cpp:1001`): if `session.crypto.encrypted`, build the wire `[24B header (clear, AAD)][12B CryptoSubHeader (seq=++sendSeq, cipher, epoch)][sealed body][16B tag]` into the transport seal scratch — AAD = first 36 bytes; `seal(seq, aad, 36, body, bodyLen, out+36)`; offer the `len+28` buffer via `transport_.offerOn`. In `dispatchEgressFrame`, BEFORE `codec::checkFrame`: read the clear 24-B header → look up session by `sessionId` → if `crypto.encrypted`, read `CryptoSubHeader`, check `recvWindow` on `seq` (drop replays), `open(seq, aad36, 36, ct, ctLen, scratch+24)` into a reconstructed `[24B header][plaintext body]` scratch, drop on `-1`; then run the existing `checkFrame`/`peekTemplate`/`switch` on the reconstructed buffer.
- [ ] **Step 4: Run — pass.** Build client (`-s arch=x86_64 -s compiler.cppstd=gnu17`), `ctest -R ClientCrypto`.
- [ ] **Step 5: Commit** `feat: client encrypt produce / decrypt egress (session AEAD)`.

---

## Phase D — broker integration (`rune-broker`)

### Task 7: Gateway server handshake + `CryptoState` on `Session`

**Files:**
- Modify: `libs/gateway/include/rune/gateway/SessionTable.h` (`Session` gains `CryptoState`), `libs/gateway/include/rune/gateway/ClientGateway.h` (`PendingConnect` gains `TlsSession` + handshake buffer; load broker cert/key at ctor)
- Modify: `libs/gateway/src/ClientGateway.cpp` — `handleConnect` (`~666-790`) starts `TlsSession::makeServer`, replies `ConnectResponse` (v4) with the chosen cipher + first handshake bytes; new `HandshakeContinue` case in `onFragment` drives the handshake and, on `done()`, installs `CryptoState` on the `Session` and marks `encrypted`
- Modify: `CMakeLists.txt`/`conandata.yml` (openssl + rune_crypto PRIVATE into `rune_node`/`rune_gateway`)
- Test: `libs/gateway/tests/GatewayCryptoTest.cpp`

- [ ] TDD as Task 5 mirrored for the server role: a full in-process handshake (client `TlsSession` ↔ gateway `handleConnect`+`HandshakeContinue`) reaches `encrypted` with matching keys; a bad client token → `ConnectResponse{code=HandshakeFailed}`. Commit.

### Task 8: Gateway decrypt ingress / encrypt egress

**Files:**
- Modify: `libs/gateway/src/ClientGateway.cpp` — `onFragment` (`~576-593`, decrypt before `checkFrame`), `sendOnPub` (`~1137-1165`, encrypt after encode), `sendControlFrame`/`sendRespReliable_` paths
- Test: extend `GatewayCryptoTest.cpp`

- [ ] TDD mirror of Task 6 on the broker side: decrypt at the top of `onFragment` (clear 24-B header → session lookup → `CryptoSubHeader` → replay window → `open` → reconstruct → existing `switch`); encrypt in `sendOnPub` (seal the encoded response body, prepend subheader, append tag) when `session.crypto.encrypted`. Tamper/replay dropped. Commit `feat: gateway decrypt ingress / encrypt egress`.

---

## Phase E — config, per-worker, acceptance

### Task 9: `RUNE_TLS` toggle, cert loading, `require_tls`, downgrade safety

**Files:** broker + client config structs + CMake option `RUNE_TLS`; broker cert/key path + SIGHUP reload; `require_tls` rejects plaintext `Connect` with `ErrorCode::HandshakeFailed`.
- [ ] TDD: with `RUNE_TLS` off, a produce→ack round trip is byte-identical to today (no envelope); a v3 (no-cipher) client against a non-`require_tls` broker stays plaintext; the same client against a `require_tls` broker is rejected. Commit.

### Task 14: Per-worker-ingress key install (`InstallSessionKey` WorkerOp) — gated `RUNE_PER_WORKER_INGRESS`

**Files:** `schema/rune-internal-ring.xml` (`OpType::INSTALL_SESSION_KEY`, new `TailKind` for the key blob), regenerate SBE; `libs/node/include/rune/node/WorkerOp.h` (`Type::InstallSessionKey` + tail), `tryEnqueueOp` case (mirror `SchemaUpdate`, `BrokerNode.cpp:2357-2399`); `WorkerIngress` decrypts produce frames using the installed key (clear sessionId → key → open → decode).
- [ ] TDD: gateway completes a handshake, fans `InstallSessionKey` to the owning worker's ingress ring; an encrypted produce frame delivered straight to `WorkerIngress` decrypts + decodes. Gate the whole task behind `perWorkerIngressEnabled`. Commit.

### Task 15: Security regression suite (`*Regression*`, runs first)

**Files:** `libs/gateway/tests/GatewayCryptoRegressionTest.cpp` (+ client mirror).
- [ ] Tamper (ciphertext/tag byte flip → dropped), replay (resent `seq` → dropped), reorder within window (accepted), cross-session injection (wrong key → tag fail), downgrade attempt (`require_tls` refuses plaintext; client refuses a broker that drops below the negotiated floor), handshake-blob-overflow (a ClientHello > old 512 scratch handled). Named `*Regression*` so they run as the priority gate (Rule 6/7). Commit.

### Task 16: E2E + overhead benchmark gate

**Files:** extend `rune-client-cpp/docker/e2e-data.sh` with a `RUNE_TLS=1` run (connect→produce N→ack→subscribe assert across leader-kill, encrypted); add rune-fr `CryptoSeal`/`CryptoOpen` sites at the four seams (value=bytes, arg=cipher-path); a microbench of `AeadCipher::seal/open` per cipher + an e2e produce→ack latency compare (plaintext vs encrypted) on a VAES host and an AES-NI host.
- [ ] **Gate:** single/small produce→ack adds **< 5–10 µs** on the fast path; publish the numbers in `rune-crypto/README.md` and the broker e2e output. No "done" without them (Rule 5).

---

## Self-Review

**Spec coverage:** §2 threat model → Tasks 3 (cert/MITM), 15 (tamper/replay/downgrade). §4 cold/hot split → Tasks 3/5/7 (cold) vs 2/6/8 (hot). §5 TLS-over-Connect + LE cert + token + negotiation → Tasks 3,4,5,7,9. §6 envelope/nonce/AAD/zero-alloc/reused-ctx → Tasks 2,4,6,8. §7 fragmentation/MTU → inherent (whole-message seal above Aeron; no task needed, asserted by Task 16 large-batch e2e). §8 budget → Task 16 gate. §9 shared-nothing session state → Tasks 5/7 (owned by one thread) + 14 (ring key install). §10 rekey (epoch) → `epoch` field present (Task 4) + install path (Task 14); explicit rekey trigger left to a follow-up (noted). §11 OpenSSL/toggle → Tasks 1,9. §12 philosophy → honored per task. §13 tests → Tasks 2,3,6,8,15,16. §14 rollout → Task 9. **Gap intentionally deferred:** active epoch-rekey trigger (KeyUpdate/re-handshake) — the wire + install support it (epoch field, key install), but the trigger policy is a follow-up task once Task 16 shows rekey cost; flagged in spec §15.

**Placeholder scan:** no TBD/TODO; integration tasks (6,8) give the exact seam + real insertion logic; crypto tasks (2,3) give full code. Tasks 9/14/15/16 are described at step granularity with concrete gates (acceptable — they wire already-specified pieces).

**Type consistency:** `AeadCipher::seal/open`, `SessionKeys`, `CryptoSubHeader` (12 B at offset 24), `kCryptoHeaderBytes=36`, `kCryptoOverhead=28`, `Cipher` enum values (1=AES-GCM,2=ChaCha) used consistently across Tasks 2,4,6,8. Nonce = 4-byte prefix ‖ 8-byte BE seq everywhere.

**Review Focus:** tamper/forge → T2+T15; replay/reorder → T6/T8+T15; cert failure → T3+T5; downgrade/skew → T4+T9; handshake-blob-overflow → T4+T5+T15. All pinned.
