# Client↔Broker Transport Encryption (TLS/cert + per-session AEAD) — Design Spec

**Status:** Draft for review (brainstormed 2026-10-07; implementation in a later session).
**Owner:** rune
**Scope:** `rune-protocol` (wire envelope + handshake framing) · `rune-broker` (gateway + worker-ingress seams, session/key state) · `rune-client-cpp` (transport seams, session machine).
**Out of scope:** node↔node (broker↔broker) replication/consensus links — explicitly deferred.

---

## 0. One-paragraph summary

Encrypt the **client↔broker** network link (produce frames, ack/egress frames, and the session handshake) with authenticated encryption, without breaking Rune's ultra-low-latency, zero-allocation, shared-nothing hot path. The expensive asymmetric work — mutual-ish authentication and key agreement — runs **once at connect** as a TLS 1.3 handshake carried over the existing `Connect`/`ConnectResponse` exchange (OpenSSL driven through memory BIOs over the reliable Aeron stream); the broker presents a **Let's Encrypt** certificate for its public DNS name. The steady-state data path then does **only symmetric AEAD per message**, with the cipher chosen per connection from CPU capability — **AES-256-GCM (VAES/AVX-512 → AES-NI/ARMv8-crypto) → ChaCha20-Poly1305** fallback. Encryption happens **above Aeron** (seal the whole application message, let Aeron fragment the ciphertext exactly as it fragments plaintext today), so Aeron's fragmentation, flow control, NAK/retransmit, and the already-live `FragmentAssembler` reassembly are untouched.

---

## 1. Goals & non-goals

### Goals
- **Confidentiality + integrity + server authentication** on the client↔broker link, zero-trust against an on-path attacker (eavesdrop, tamper, inject, replay).
- **Preserve ULL.** Steady-state added latency for a latency-sensitive (single/small) produce→ack **< 5–10 µs**. No heap allocation, no locks, no syscalls on the hot path (beyond what Aeron already does).
- **CPU-adaptive fast paths with graceful fallback.** Use hardware AES (VAES/AVX-512, then AES-NI / ARMv8 crypto) when present; fall back to ChaCha20-Poly1305 otherwise — the lowest-latency option achievable without hardware AES.
- **Standard certificates.** The broker authenticates with an X.509 cert that **Let's Encrypt can issue** (public DNS name, DNS-01 ACME), verifiable by clients against the public trust store.
- **Shared-nothing conformance.** Per-session crypto state owned by exactly one thread; cross-thread key delivery (only where unavoidable) via a ring-buffer op, never shared memory.
- **Measurable.** Overhead proven with rune-fr spans and a benchmark gate, not assumed.
- **Toggle.** Compile/runtime switch (`RUNE_TLS`) so the plaintext transport and the encrypted transport can both be built, for migration and A/B latency measurement.

### Non-goals
- Node↔node (replication/consensus) encryption — deferred to a later spec. This design keeps the AEAD envelope and handshake general enough to extend there later, but implements only the client link.
- Client-certificate PKI. Let's Encrypt issues no client certs; **client authorization** is an application token/JWT carried inside the encrypted handshake, validated by the broker's existing auth path. (A private client-cert CA is a possible future addition, out of scope here.)
- Replacing Aeron's transport or its reliability/flow control. We sit strictly above it.
- Encrypting `aeron:ipc` / in-process ring hops (same-host, same trust domain) — never on the wire, so never encrypted.

---

## 2. Threat model

- **In scope:** a network adversary on the client↔broker path who can observe, modify, drop, reorder, inject, or replay UDP datagrams.
- **Guarantees:** payload confidentiality (AEAD), integrity + authenticity (AEAD tag — a modified or forged frame fails and is dropped), replay rejection (per-session sequence + window), and **server authentication** (client verifies the broker's cert chain + hostname, defeating MITM/impersonation). **Client authorization** via a token in the handshake.
- **Explicitly not defended:** a compromised endpoint (client or broker host); traffic analysis (sizes/timing — Rune batches, which partially obscures, but we add no padding in v1); node↔node links (separate trust domain, deferred).
- **Integrity note:** Aeron provides no cryptographic integrity today (UDP checksum only). The AEAD tag adds it. Because Aeron's reliability (NAK/retransmit) sits **below** the crypto, a reassembled message is always complete and loss-recovered before we decrypt — so the tag only ever fails on genuine tampering, never on transient loss (no loss amplification).

---

## 3. Topology & what is protected

Client↔broker uses `aeron:udp?endpoint=<host:port>` on two logical streams (ingress: client→broker produce/control; egress: broker→client ack/records). All three message classes on this link are protected:

| Class | Direction | Size today | Fragments today? |
|---|---|---|---|
| Produce batch | client → broker | up to 64 KiB (default `batchBytes`) | **Yes** — ~48 fragments at MTU 1408; `FragmentAssembler` already reassembles |
| Ack (`ProduceResponse`) | broker → client | 46 B fixed | No — always single-fragment |
| Egress records | broker → client | small…>MTU | Sometimes (tryClaim else offer) |
| Handshake (`Connect`/`ConnectResponse`) | both | small | No |

The handshake frames ride the **same** ingress/egress streams (there is no separate control channel) and are distinguished, as today, by `templateId`.

---

## 4. Architecture — cold path vs hot path

The design splits cleanly so the expensive work never touches the steady-state loop.

```
                        COLD (once per connection, on connect)                      HOT (every message)
client ──Connect────────▶ TLS 1.3 handshake (OpenSSL memory-BIO over the         ──AEAD-sealed produce──▶ broker
       ◀─ConnectResponse─  reliable Connect/ConnectResponse exchange):            ◀──AEAD-sealed ack──────
                           - broker presents LE cert; client verifies host         (per-session symmetric
                           - client authZ token validated                           AEAD, counter nonce,
                           - negotiate AEAD cipher from both peers' CPU caps         header as AAD, zero-alloc)
                           - derive per-direction keys from the TLS exporter
```

- **Cold path** runs on the thread that already handles `Connect` (broker: `ClientGateway`/control; client: the duty-cycle thread via `ClientAgent`). One handshake, a few round trips of small frames over Aeron's reliable delivery, asymmetric crypto cost ~tens of µs–low ms — amortized over the whole connection, **not** in the steady-state budget.
- **Hot path** is symmetric AEAD only. No asymmetric crypto, no key setup, no allocation, no RNG.

---

## 5. Cold path — handshake, certificate, key agreement

### 5.1 TLS 1.3 over the Connect exchange
Rather than invent a handshake, run **TLS 1.3 via OpenSSL** driven through **memory BIOs**: feed handshake bytes out via `BIO_read` → carry them inside `Connect`/`ConnectResponse` application frames over the existing reliable Aeron stream → feed peer bytes in via `BIO_write`. Aeron already gives reliable, ordered delivery, so the TLS handshake state machine runs cleanly over it without DTLS (no need for datagram TLS; we are not using TLS's own record layer on the wire — only its handshake + key schedule).

- **Server auth:** broker loads its X.509 cert + key into the `SSL_CTX`; client verifies the chain against the system trust store and checks the SAN/hostname against the broker's DNS name (standard `SSL_set1_host` / `X509_VERIFY_PARAM`). This is what defeats MITM.
- **Key material:** after the handshake, derive per-direction AEAD keys with the **TLS exporter** (`SSL_export_keying_material`, distinct labels for c2b and b2c). We use these raw keys with our own AEAD framing on the data path (below) — *not* `SSL_write`/`SSL_read`, to avoid TLS record-layer overhead and keep a counter nonce we control.

### 5.2 Certificate / Let's Encrypt
- Broker has a **public DNS name**; obtain/renew the cert via **ACME DNS-01** (the endpoint is `aeron:udp`, not HTTP, so HTTP-01 / TLS-ALPN-01 do not apply). Any ACME client (certbot/lego/acme.sh) writes the cert+key to a path the broker loads at startup and on SIGHUP for the 90-day renewal.
- The cert is used **only** in the TLS handshake; it never touches the data path. The transport being UDP/Aeron is irrelevant to the cert.
- Private-CA alternative (internal/IP-only deployments) is a config swap (custom trust root) — supported but not the default.

### 5.3 Client authorization
Let's Encrypt authenticates the *broker to the client*, not the reverse. Client authZ (account/API key / JWT) is carried **inside** the now-encrypted `Connect` handshake payload and validated by the broker's existing auth path before the session is admitted. Channel security (TLS) and authorization (token) are cleanly separated.

### 5.4 Cipher negotiation (the CPU-gated fast path)
Each side detects CPU crypto capability at init and advertises an ordered AEAD preference in the handshake; the broker picks the best mutually-supported cipher:

1. **AES-256-GCM** when both peers have hardware AES. OpenSSL's EVP AES-GCM runtime-dispatches internally to the **VAES/AVX-512** implementation where available, else **AES-NI (+PCLMULQDQ)**, else **ARMv8 crypto (AES+PMULL)** — we do not hand-write assembly; we inherit OpenSSL's dispatch and simply verify a vectorized impl was selected at init.
2. **ChaCha20-Poly1305** when hardware AES is absent — fast in portable SIMD and constant-time without AES-NI; the "lowest-latency by normal means" floor.

Capability is detected via OpenSSL (`OPENSSL_ia32cap` / EVP availability) and recorded so rune-fr can label which path a connection took.

---

## 6. Hot path — per-message AEAD

### 6.1 Wire envelope
AEAD is applied to the **whole application message** (above Aeron). The existing **24-byte protocol header stays in the clear and is authenticated as AAD** (so the in-place `sessionId` patch at client send and all field offsets keep working); only the message body is encrypted. An outer crypto header is prepended:

```
┌───────────────── authenticated (AAD) ─────────────────┐┌─ encrypted ─┐┌─ auth ─┐
│ crypto hdr: ver(1) | cipher(1) | epoch(2) | seq(8)     ││   body...   ││ tag(16)│
│ + existing 24-byte protocol header (blockLength,       ││             ││        │
│   templateId, schemaId, version, sessionId, corrId)    ││             ││        │
└────────────────────────────────────────────────────────┘└─────────────┘└────────┘
```
- **Overhead:** 12-byte crypto header + 16-byte tag = **28 bytes per message** (flat — per message, not per fragment).
- **Nonce (96-bit, unique per key):** `fixed(4B derived from sessionId⊕direction) ‖ seq(8B per-session monotonic counter)`. No hot-path RNG. `seq` doubles as the replay sequence.
- **AAD:** the crypto header + the 24-byte protocol header — binds `sessionId`, `templateId`, `epoch`, and `seq` so frames can't be cross-stream replayed, truncated, or type-confused.
- **Replay protection:** receiver keeps a per-session sliding window on `seq`; duplicates/old frames are dropped after tag verification.

### 6.2 Mechanics (zero-alloc, reused context)
- One **reused `EVP_CIPHER_CTX` per session per direction**; re-init only the nonce per message (`EVP_CipherInit_ex` with NULL key keeps the key schedule — no per-message rekey cost).
- **Encrypt into the existing buffers** — the `tryClaim` `BufferClaim` region, or the client's reused `InFlightRing::wireFrame`, or the gateway's fixed `scratch_` — never a new allocation.
- **Decrypt** happens after `FragmentAssembler` hands over the fully reassembled message, into a reused scratch, before frame validation/decode.

### 6.3 Why fragmentation is a non-issue
- Sender seals, then Aeron fragments the **ciphertext** exactly as it fragments plaintext today; all of Aeron's flow control / NAK / retransmit / reassembly operate unchanged on ciphertext.
- Receiver reassembles (already happening — `FragmentAssembler` is live on broker ingress and client egress) and decrypts **once**. We never touch individual fragments; there is no per-fragment crypto framing.
- **No loss amplification** (Aeron reliability is below the crypto). The only micro-effect: +28 B lowers the single-fragment ceiling from 1376 → ~1348 B at MTU 1408, so a payload right at that boundary gains one extra fragment — not a correctness issue; acks (46→74 B) are unaffected.

---

## 7. Latency budget & the batch-size interaction

AEAD cost is proportional to message bytes. A produce→ack round trip adds two passes over the produce batch (client seal + broker open) plus two tiny ack passes.

| Produce size | AEAD/pass, AES-NI ~2 GB/s | AEAD/pass, VAES ~10 GB/s |
|---|---|---|
| 46 B ack | ~tens of ns | ~tens of ns |
| ~1 KB | ~0.5 µs | ~0.1 µs |
| ~8 KB | ~4 µs | ~0.6 µs |
| full 64 KiB batch | ~32 µs | ~5 µs |

- **Latency-sensitive (single/small) produce → ack: well within < 5–10 µs** on every path.
- **A full 64 KiB throughput batch** is ~5 µs/pass on the VAES fast path (≈10 µs round trip — at the edge) and larger on the AES-NI/ChaCha fallback; amortized over the hundreds of records in such a batch this is ~tens of ns/record, i.e. a throughput scenario, not a single-op latency one.
- **Decision baked in:** the budget governs the **single/small-op** latency path, which all code paths meet. The fast path keeps even full batches near budget; the fallback is accepted as the best achievable without hardware AES. A future **TLS-record-style per-chunk AEAD** (seal in ≤MTU segments so decrypt pipelines with reassembly) is noted as an optimization if full-batch tail latency ever needs it — **not** in v1.

---

## 8. Code insertion map (current seams)

Verified against current `rune-client-cpp` / `rune-broker`. Exactly one send and one receive chokepoint per side (plus the per-worker ingress variant).

| Direction | Encrypt at | Decrypt at |
|---|---|---|
| client → broker (produce) | `AeronTransport::offerToPub` (`rune-client-cpp/src/transport/AeronTransport.cpp:254-289`) — seal in place of the `memcpy`@275 (tryClaim branch) / before `offer`@285; the `sessionId` patch is upstream at `ClientAgent.cpp:999`, so treat the 24-B header as AAD | `ClientGateway::onFragment` (`rune-broker/libs/gateway/src/ClientGateway.cpp:576-585`, before `checkFrame`) **and** `Worker::handleWorkerIngressFrame_` (`rune-broker/libs/node/include/rune/node/Worker.h:3508-3514`, before `Produce::decode`) — both post-reassembly |
| broker → client (ack/records) | `ClientGateway::sendOnPub` (`rune-broker/libs/gateway/src/ClientGateway.cpp:1137-1165`; record pushes `:2888-2907`, `:3415-3444`) | `ClientAgent::dispatchEgressFrame` (`rune-client-cpp/src/agent/ClientAgent.cpp:675+`, before validation/decode) |
| handshake | `ClientAgent` connect path (`ClientAgent.cpp:594-595` broker `handleConnect`; client `Connect` send `:662-663`) | same frames, driven through OpenSSL memory BIOs |

A single shared `seal()` / `open()` helper keeps the gateway and per-worker-ingress paths in lockstep.

---

## 9. Session & key state (shared-nothing)

- **Client:** one duty-cycle thread does both send and receive, so the whole session crypto state (send key+seq, recv key+replay window, cipher, epoch) is owned by that one thread — no sharing, no locks.
- **Broker, default gateway ingress:** `ClientGateway::doWork` is single-writer; the handshake completes there and the derived keys live in the gateway's per-session entry, owned by that same thread. No cross-thread handoff.
- **Broker, per-worker ingress (`RUNE_PER_WORKER_INGRESS`):** the handshake still completes at the gateway on `Connect`, but produce frames arrive at a `WorkerIngress` on a per-worker stream owned by a different thread. The client→broker **recv key** must therefore reach that worker — delivered via a **ring-buffer op `InstallSessionKey{sessionId, epoch, key, cipher}`** (rule 2: ring buffers are the only cross-thread event passing), never shared memory. Rekey uses the same op.
- **State machine** (extends the existing `SessionMachine`): `HANDSHAKING(ssl, bios, deadline)` → `ESTABLISHED(cipher, sendKey, sendSeq, recvKey, recvWindow, epoch)` → `REKEYING(old, new)` → `CLOSED`. Each state holds exactly its own variables (rule 3).

---

## 10. Key rotation

- Epoch-based. A time/byte/`seq` threshold (well inside the AEAD safe-data-per-key limit) triggers a rekey **off the hot path** on the control/gateway thread — either a TLS 1.3 `KeyUpdate` or a lightweight re-handshake — producing a new `epoch`. The `epoch` field in the crypto header selects the active key; the receiver keeps current + previous epoch during the switch. With a monotonic counter nonce the practical rekey cadence is driven by policy, not nonce exhaustion.

---

## 11. Build, dependencies, configuration

- **Dependency: OpenSSL 3.x** (one library for both the TLS 1.3 handshake + cert validation and the data-path EVP AEAD, both runtime-dispatched). Builds on Linux and macOS/arm64. Added via Conan, consistent with the sibling repos' dependency style. (libsodium was considered and rejected — it would mean two crypto libs and its own TLS gap.)
- **Toggle:** `RUNE_TLS` CMake option + runtime config. Off → today's plaintext transport, zero added code on the hot path. On → encrypted transport. Both buildable for migration and A/B latency measurement.
- **Config:** cert path + key path (+ reload on SIGHUP), ACME hook (external), client trust-store path (default: system), cipher preference override (default: capability-driven), `require_tls` (broker rejects plaintext when set), per-session rekey thresholds.
- **Measurement:** new rune-fr sites `CryptoSeal` / `CryptoOpen` (value = bytes, arg = cipher-path id) at the four seams, so the per-hop overhead on the produce→ack waterfall is proven, not assumed.

---

## 12. Rune engineering-philosophy conformance

1. **Shared-nothing** — §9: per-session state owned by one thread; the only cross-thread case (per-worker ingress key) goes through a ring op.
2. **Ring buffers only for cross-thread** — `InstallSessionKey` ring op; no shared key memory.
3. **State machines, state-scoped vars** — §9 session machine.
4. **Zero allocations** — §6.2: reused `EVP_CIPHER_CTX`, encrypt into existing claim/`wireFrame`/`scratch_`; handshake buffers from a bounded preallocated pool.
5. **Test + benchmark everything** — §13.
6. **Regression test on every fix** — §13 (`*Regression*` suite for tamper/replay/downgrade).
7. **Full regression suite first** — enforced by the test plan ordering.
8. **Fix-on-sight** — any hot-path allocation / shared state found while wiring the seams gets fixed in place.

---

## 13. Testing & benchmark plan

- **Handshake unit tests:** successful TLS 1.3 over memory-BIO; cert chain + hostname verification; rejection of wrong-host / expired / untrusted cert; client-token accept/reject.
- **AEAD unit tests:** seal/open round trip for each cipher path (force AES-GCM and ChaCha20-Poly1305); header-as-AAD correctness; nonce uniqueness.
- **Security regression suite (`*Regression*`, runs first):** tamper (flip a ciphertext/tag byte → dropped), replay (resend a frame → dropped by window), reorder within window (accepted), cross-session frame injection (dropped by AAD), downgrade attempt (client/broker refuse below negotiated floor).
- **Fragmentation test:** encrypted 64 KiB produce batch (multi-fragment) round-trips and decodes identically to plaintext — proves the above-Aeron design.
- **E2E:** encrypted produce→ack and encrypted egress consume, 3-node broker, with `RUNE_TLS` on.
- **Benchmark gate:** rune-fr `CryptoSeal`/`CryptoOpen` + end-to-end produce→ack latency, encrypted vs plaintext, on both a VAES host and an AES-NI host. **Gate: single/small produce→ack adds < 5–10 µs on the fast path**; publish the numbers (per earlier house rule — no "done" without the bench).

---

## 14. Rollout / migration

- Ship behind `RUNE_TLS` off by default; enable per-deployment.
- The `Connect` handshake negotiates encrypted-vs-plaintext so a mixed fleet can migrate; `require_tls` on the broker ends the migration by refusing plaintext.
- Node↔node stays plaintext (separate trust domain) until its own spec.

---

## 15. Open questions (resolve during implementation)

- Exact rekey cadence (time vs bytes vs `seq`) and whether `KeyUpdate` or re-handshake — pick after the bench shows rekey cost.
- Whether to pad small frames to blunt traffic analysis (v1: no).
- Per-worker-ingress key-install op placement in the existing worker ring schema.
- Whether egress record pushes (consumer path) share the same session keys or get their own — default: same session.
