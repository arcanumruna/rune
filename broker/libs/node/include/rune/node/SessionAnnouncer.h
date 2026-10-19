#pragma once
// SessionAnnouncer.h — runtime-side announce-before-accept (spec §3.4, Task 13).
//
// The runtime hands every accepted session's reply destination (egress channel +
// stream) to EVERY worker over that worker's runtime→worker ring BEFORE it sends
// ConnectResponse, so no worker can see a Produce for a session it does not know.
// On Disconnect / reap / connect timeout it closes the session on every worker.
//
// Resumable per worker (rich state, spec §1a.4):
//   Announcing(sessionId, remainingMask) — a ring-full worker is retried on the
//     next announce() call for that session; workers already reached are never
//     re-sent (no duplicate announcement).
//   Closing(sessionId, remainingMask) — SessionClose retried by retryCloses()
//     every runtime cycle until every worker accepted it (never dropped).
// Per worker ring FIFO + one runtime producer: a close enqueued before a later
// announcement of a reused id is executed first (§1.1 L4 of the F15 note).
//
// Control plane (connect / disconnect rate), runtime thread only.  The vectors
// grow only with concurrent connects / pending closes (cold); a growing backlog
// (a worker's lifecycle ring permanently full) is logged at each doubling past
// kBacklogWarnAt (fix round 1, m9) and visible via pendingCloses().
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/crypto/Aead.h>
#include <rune/crypto/Cipher.h>

#include <spdlog/spdlog.h>

#include <cstdint>
#include <cstring>
#include <string_view>
#include <vector>

namespace rune::node {

class SessionAnnouncer {
public:
    /// Announce `sessionId` to every worker not yet reached.  True once every
    /// worker has it queued (then — and only then — the runtime may accept).
    /// D6: `clientVersion` (ConnectRequest.clientVersion) rides along so a
    /// worker acking directly gates UnknownProducerId per session.
    ///
    /// Task 19 (per-worker egress crypto addendum): `sessionEncrypted` tells
    /// every worker whether this session WILL be encrypted at all (known as
    /// soon as a Connect is attempting a TLS handshake — see
    /// ClientGateway::pollPendingConnects' first, announce-before-accept
    /// wave, which has no real key yet: `keyReady=false` there). Once the
    /// handshake completes and the real b2c key exists
    /// (handleHandshakeContinue_'s second wave), re-announce with
    /// `keyReady=true` + the real `b2c`/`b2cPrefix`/`cipher` — THIS call
    /// derives a DISTINCT per-sender key for EACH worker w
    /// (deriveSenderKey(b2c, w+1, ...) — senderId = w+1, unique per worker,
    /// never 0/the gateway's own id).
    ///
    /// Fail-closed either way: `sessionEncrypted && !keyReady` (the first
    /// wave) or a derivation failure (`keyReady` true but deriveSenderKey
    /// fails for worker w) both leave `senderKeyValid=false` on the wire —
    /// the worker must park/drop that session's Records rather than ever
    /// serving them in the clear (see Worker.h's sessionCrypto_ /
    /// pushOwnedSubscriptions_). `b2c`/`b2cPrefix`/`cipher` are read ONLY
    /// when `sessionEncrypted && keyReady`; `sessionEncrypted=false`
    /// (plaintext, the default) touches nothing — zero bytes added,
    /// byte-identical to every pre-Task-19 call.
    ///
    /// Task 22 (shard worker decrypts produce / per-worker ingress): `c2b`/
    /// `c2bPrefix` are the session's RAW client->broker key/prefix — UNLIKE
    /// `b2c` above, this is NOT derived per worker (ingress has a SINGLE
    /// sender, the client): every worker gets the SAME bytes, straight from
    /// the gateway's own `session.crypto.c2b`/`c2bPrefix` (Task 8's ingress
    /// open key, retained raw alongside `b2c` — see CryptoState.h). Read
    /// ONLY when `sessionEncrypted && keyReady`, same gate as `b2c`;
    /// `c2bKeyValid` on the wire mirrors `keyReady` directly (no derivation
    /// step to fail: copying raw bytes cannot fail the way deriveSenderKey
    /// can) — false on the first (parked) wave, true once the real keys
    /// exist.
    bool announce(WorkerPool& pool, int64_t sessionId, std::string_view channel, int32_t streamId,
                  int32_t clientVersion = 0, bool sessionEncrypted = false, bool keyReady = false,
                  const rune::crypto::AeadKey& b2c = {}, const uint8_t* b2cPrefix = nullptr,
                  rune::crypto::Cipher cipher = rune::crypto::Cipher::None,
                  const rune::crypto::AeadKey& c2b = {}, const uint8_t* c2bPrefix = nullptr) {
        Announcing* a = findAnnouncing_(sessionId);
        if (!a) { announcing_.push_back({sessionId, allMask_(pool)}); a = &announcing_.back(); }
        for (int32_t w = 0; w < pool.workerCount(); ++w) {
            const uint64_t bit = uint64_t{1} << w;
            if (!(a->remaining & bit)) continue;
            WorkerOp op;
            op.type            = WorkerOp::Type::SessionAnnounce;
            op.sessionId       = sessionId;
            op.egressStreamId  = streamId;
            op.egressChannel   = channel;
            op.clientVersion   = clientVersion;
            op.grantAll        = grantAll_;   // authentication Task 13 (D30): authMode != Enabled
            op.sessionEncrypted = sessionEncrypted;
            if (sessionEncrypted && keyReady) {
                rune::crypto::AeadKey senderKey{};
                const bool ok = rune::crypto::deriveSenderKey(
                    b2c, static_cast<uint8_t>(w + 1), senderKey);
                op.senderKeyValid = ok;
                if (ok) {
                    op.senderCipher = static_cast<uint8_t>(cipher);
                    std::memcpy(op.senderKeyPrefix, b2cPrefix, 4);
                    std::memcpy(op.senderKey, senderKey.bytes, 32);
                } else {
                    // Fail closed (addendum §"Scope"): never ship a half-
                    // derived key. The worker sees sessionEncrypted=true,
                    // senderKeyValid=false and parks this session's Records.
                    spdlog::critical("SessionAnnouncer: deriveSenderKey failed for session={} "
                                      "worker={} (senderId={}) — fail-closed: this worker's "
                                      "Records egress for the session will be parked, never "
                                      "served in the clear", sessionId, w, w + 1);
                }
            }
            // else: sessionEncrypted && !keyReady (the first wave — no real
            // key exists yet) or plaintext — op.senderKeyValid stays false.

            // Task 22: c2b ingress key — the SAME raw bytes for every worker
            // (no per-worker derivation; see this function's doc comment).
            // Gated identically to the b2c branch above: only once the real
            // handshake keys exist (sessionEncrypted && keyReady).
            if (sessionEncrypted && keyReady) {
                op.c2bKeyValid = true;
                std::memcpy(op.c2bKeyPrefix, c2bPrefix, 4);
                std::memcpy(op.c2bKey, c2b.bytes, 32);
            }
            // else: op.c2bKeyValid stays false — fail closed (the worker
            // parks/drops this session's produce ingress, never plaintext).

            if (pool.worker(w).tryEnqueueOp(std::move(op))) a->remaining &= ~bit;
        }
        if (a->remaining != 0) return false;
        eraseAnnouncing_(sessionId);
        return true;
    }

    /// Close `sessionId` on every worker (a partially announced one included);
    /// what a full ring refuses now is retried by retryCloses().
    void close(WorkerPool& pool, int64_t sessionId) {
        eraseAnnouncing_(sessionId);
        closing_.push_back({sessionId, allMask_(pool)});
        (void)retryCloses(pool);
        if (closing_.size() >= closeWarnAt_) [[unlikely]] {
            spdlog::warn("SessionAnnouncer: {} session closes not yet accepted by every worker "
                         "(a lifecycle ring stays full)", closing_.size());
            closeWarnAt_ *= 2;
        }
    }

    /// Re-offer every pending SessionClose; returns how many completed.
    int retryCloses(WorkerPool& pool) {
        int done = 0;
        for (std::size_t i = 0; i < closing_.size(); ) {
            Closing& c = closing_[i];
            for (int32_t w = 0; w < pool.workerCount(); ++w) {
                const uint64_t bit = uint64_t{1} << w;
                if (!(c.remaining & bit)) continue;
                WorkerOp op;
                op.type      = WorkerOp::Type::SessionClose;
                op.sessionId = c.sessionId;
                if (pool.worker(w).tryEnqueueOp(std::move(op))) c.remaining &= ~bit;
            }
            if (c.remaining == 0) { closing_[i] = closing_.back(); closing_.pop_back(); ++done; }
            else ++i;
        }
        return done;
    }

    /// Authentication Task 13 (D30): grantAll = the broker does not enforce authentication (dev / in-process): every
    /// announced session may write to every topic on a worker.  false (Enabled mode) = every shard produce of an
    /// announced session is denied until its PermissionShipper update lands.  Set ONCE from the gateway's auth mode
    /// (setAuthMode); the default (true) keeps every rig that predates authentication unchanged.
    void setGrantAll(bool grantAll) noexcept { grantAll_ = grantAll; }
    [[nodiscard]] bool grantAll() const noexcept { return grantAll_; }

    static constexpr std::size_t kBacklogWarnAt = 1024;
    [[nodiscard]] std::size_t pendingCloses() const noexcept { return closing_.size(); }
    [[nodiscard]] std::size_t pendingAnnounces() const noexcept { return announcing_.size(); }
    [[nodiscard]] std::size_t pendingClosesForTest() const noexcept { return closing_.size(); }

private:
    struct Announcing { int64_t sessionId; uint64_t remaining; };
    struct Closing    { int64_t sessionId; uint64_t remaining; };

    static uint64_t allMask_(WorkerPool& pool) noexcept {
        const int32_t n = pool.workerCount();              // <= 64 (validateShardConfig)
        return n >= 64 ? ~uint64_t{0} : ((uint64_t{1} << n) - 1);
    }
    Announcing* findAnnouncing_(int64_t sid) noexcept {
        for (auto& a : announcing_) if (a.sessionId == sid) return &a;
        return nullptr;
    }
    void eraseAnnouncing_(int64_t sid) noexcept {
        for (std::size_t i = 0; i < announcing_.size(); ++i)
            if (announcing_[i].sessionId == sid) { announcing_[i] = announcing_.back(); announcing_.pop_back(); return; }
    }

    std::vector<Announcing> announcing_;
    std::vector<Closing>    closing_;
    std::size_t             closeWarnAt_ = kBacklogWarnAt;
    bool                    grantAll_ = true;
};

} // namespace rune::node
