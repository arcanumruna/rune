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
// grow only with concurrent connects / pending closes (cold).
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>

#include <cstdint>
#include <string_view>
#include <vector>

namespace rune::node {

class SessionAnnouncer {
public:
    /// Announce `sessionId` to every worker not yet reached.  True once every
    /// worker has it queued (then — and only then — the runtime may accept).
    bool announce(WorkerPool& pool, int64_t sessionId, std::string_view channel, int32_t streamId) {
        Announcing* a = findAnnouncing_(sessionId);
        if (!a) { announcing_.push_back({sessionId, allMask_(pool)}); a = &announcing_.back(); }
        for (int32_t w = 0; w < pool.workerCount(); ++w) {
            const uint64_t bit = uint64_t{1} << w;
            if (!(a->remaining & bit)) continue;
            WorkerOp op;
            op.type           = WorkerOp::Type::SessionAnnounce;
            op.sessionId      = sessionId;
            op.egressStreamId = streamId;
            op.egressChannel  = channel;
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

    [[nodiscard]] std::size_t pendingCloses() const noexcept { return closing_.size(); }
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
};

} // namespace rune::node
