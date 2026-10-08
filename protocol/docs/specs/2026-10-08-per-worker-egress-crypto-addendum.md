# Addendum: per-worker egress encryption (shard-mode client↔broker TLS)

**Status:** DRAFT for approval. Extends `2026-10-07-client-broker-tls-design.md`.
**Date:** 2026-10-08.

## Problem

Client↔broker TLS/AEAD is complete and proven e2e in **legacy** mode. In **shard**
mode (the broker default) the broker currently *refuses* encrypted sessions, because
consumer **Records** are pushed to the client by the **shard worker directly**
(`Worker.h` `sendRecordsFrame(*egressPub, …)`, publishing to the client's own
`req.egressChannel`), bypassing the gateway's egress sealer.

So for one session there are **two independent broker→client senders**:
- the **gateway** — acks/control (routed worker→return-ring→gateway, sealed at `sendTo`);
- the **worker(s)** — consumer Records (pushed straight to the client).

AES-GCM's nonce is `noncePrefix(4) ‖ seq(8)`. Two senders incrementing their own `seq`
under the **same** `b2c` key ⇒ nonce reuse under one key ⇒ catastrophic (loss of
confidentiality + integrity). Shared-nothing means they cannot share a `seq` counter,
and routing bulk Records through the gateway's return ring is not viable (the ring is
not sized for record batches — that detour is exactly what per-worker egress avoids).

## Non-negotiable constraint (from the owner)

The fix must add **zero** latency/throughput cost **beyond the raw AES-GCM seal/open**
that encryption inherently requires (which is accepted). Specifically it must NOT add:
per-frame key lookups on the hot path, a second/trial decrypt on the client, or extra
wire bytes. Plaintext (non-encrypted) sessions must stay **byte-identical** (zero impact).

## Design — per-sender keys, selected by an authenticated clear sender id

### 1. Per-sender b2c keys (distinct keys, not shared-key/distinct-prefix)
Each broker→client sender for a session gets its **own** `b2c` AEAD key. Distinct keys
means the senders occupy **independent nonce spaces** — nonce reuse across senders is
impossible *by construction*, with no reliance on prefixes never colliding (safer than
a shared key).

Derivation (both sides, deterministic, identical result — at handshake / cold path):
- At handshake-done, in addition to today's `c2b`/`b2c`, derive a **`b2c` master secret**
  from the TLS 1.3 exporter (one extra `SSL_export_keying_material` call, cold).
- Per-sender key = `HKDF-Expand(b2c_master, info = "rune-b2c-sender" ‖ u16(senderId), L=32)`
  (OpenSSL `EVP_KDF` HKDF; `c2bPrefix`/`b2cPrefix` reused as the 4-byte nonce prefixes).
- `senderId == 0` ⇒ **exactly today's `b2c` key** (the gateway). Workers use
  `senderId = workerId + 1`. (So senderId 0 is defined to equal the existing derivation —
  no change to the already-shipped gateway/client seal/open.)

Both the client and the broker derive these from the *same* exporter secret, so the
client can reproduce any sender's key locally — no key is ever sent to the client.

### 2. Sender id on the wire — zero added bytes
Carry `senderId` in the **existing** `CryptoSubHeader.epoch` field (uint16 @ sub-offset 2,
currently unused/zero). Split it: low byte = `epoch` (future key rotation, still 0),
high byte = `senderId` (0..255). The sub-header stays **12 bytes** — no wire growth,
`kCryptoOverhead` stays 28. It is inside the AAD, so it is **authenticated**: an attacker
flipping it only makes the client pick the wrong key ⇒ `open` fails ⇒ frame dropped
(fail-closed). (senderId 0 keeps today's all-zero `epoch` bytes ⇒ gateway framing byte-identical.)

### 3. Client receive — pick key by the clear id, no trial decrypt
The client already reads the clear 24-byte header + 12-byte sub-header before decrypting.
It reads `senderId`, selects the matching recv cipher (deriving + caching it on first sight
of a new sender — cold, once per sender), and calls `open` **once**. No fallback/second
decrypt. Each sender has its **own replay window** (its own `recvSeq`/`recvWindow`), since
each is an independent key+seq stream starting at 1.

### 4. Key distribution to the worker (cold path)
When the gateway announces a session to worker *i* (`SessionAnnounce` WorkerOp, already the
mechanism that hands the worker the session's `egressPub`), it also hands the worker the
32-byte `b2c_sender[i+1]` key + 4-byte prefix + cipher id it derived for that worker. The
worker builds one `AeadCipher` (via the existing `RecordsSeal.h` `RecordsSealCtx`) and seals
its Records with `senderId = i+1` and its own monotonic `seq`. This is a one-time per
(session,worker) cost at announce time, off the data path.

### 5. Gateway / legacy unchanged
- Gateway control/ack egress: unchanged — it is `senderId = 0`, the existing `b2c` key and
  `sendTo` seal. No code change on that path.
- Legacy (non-shard) mode: the gateway is the only sender; `senderId` stays 0; no worker keys
  derived. Byte-identical to the proven legacy e2e.
- Plaintext sessions (any mode): untouched — the encrypted branch is additive.

## Why this meets "zero overhead beyond the raw seal"
- Per-sender key derivation + distribution: **handshake / session-announce only** (cold).
- Hot seal/open: one AES-GCM op, nonce `prefix‖seq` computed exactly as today; `senderId`
  is one byte already written into the sub-header. **No extra per-frame work.**
- Client select-by-id: reads a byte it already parses; **no trial decrypt**, O(1) cache hit.
- Wire: **no added bytes** (reuses `epoch`).
- Plaintext path: byte-identical, zero-copy egress preserved.
Net per-frame cost vs. legacy encryption = **identical**; vs. plaintext = the raw seal only.

## Validation (box, x86_64)
1. Shard-mode encrypted e2e: `require_tls` broker in **shard** mode + encrypted client →
   produce + consume 20/20 (consumer records now sealed by the worker, opened by the client).
2. A/B: plaintext-shard throughput before vs. after this change — must be unchanged
   (prove the additive branch doesn't touch the plaintext hot path).
3. Isolate the technique's own cost: encrypted-legacy (gateway seals records) vs.
   encrypted-shard (worker seals records) per-frame cost — must match (same single seal).
4. Negative: forged `senderId`, wrong-key, replay, cross-sender nonce — all dropped.

## Security notes
- Distinct per-sender keys ⇒ no cross-sender nonce reuse even if two senders both start at
  seq 1. This is the core safety property.
- `senderId` authenticated via AAD ⇒ not a redirection vector (fail-closed on mismatch).
- Per-sender replay windows ⇒ a replay on one sender's stream can't affect another's.
- No key material crosses the wire; the client re-derives from the shared TLS exporter secret.

## Scope / tasks (gated behind shard-mode + an encrypted session)
- rune-crypto: `b2c` master export + `deriveSenderKey(master, senderId)` (HKDF). Unit tests
  (two senderIds ⇒ different keys; both sides agree).
- rune-protocol: document `CryptoSubHeader.epoch` split {senderId:hi8, epoch:lo8} + accessors.
- rune-broker: derive per-worker key at handshake; ship via `SessionAnnounce`; worker seals
  Records via `RecordsSeal.h` with its senderId+seq; drop the shard+require_tls startup refusal
  once this lands (replace with "supported").
- rune-client-cpp: per-sender recv cipher cache + per-sender replay window; select by senderId.
- e2e + the A/B throughput gate above.
