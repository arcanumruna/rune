#pragma once
// PermissionShipper.h - control-thread, resumable per-worker shipping of a session's WRITE bitmap (authentication
// Task 13, D30; the SessionAnnouncer pattern).
//
// ship() queues one update (sessionId, epoch, grantAll, the bitmap words) for EVERY worker.  Each worker receives it as
// SESSION_PERMISSIONS chunks of <= kChunkWords words, in order, the last flagged; a full lifecycle ring stops that worker
// at its next chunk and retry() resumes there - a chunk is never sent twice and never skipped.  A worker finishes the
// update it started before it starts another (it has exactly one staging buffer), and a newer update of the same session
// replaces an older queued one ("latest wins"; a worker that already holds the older one's first chunks restarts: a
// first chunk always resets its staging).  fullyShippedEpoch(session) is the newest epoch every worker has accepted;
// the gateway answers a login only once it reached that login's epoch (announce-before-accept for permissions).
//
// Control plane (login / refresh rate), control thread only.  Allocates only when an update is queued (cold).
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/node/WorkerWriteGrants.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <span>
#include <vector>

namespace rune::node {

class PermissionShipper {
public:
    static constexpr std::size_t kChunkWords = 512;
    static constexpr int         kMaxWorkers = 64;   // validateShardConfig

    /// Queue `bits` for `sessionId` to every worker; returns true once every worker accepted it.
    /// `owner` (issue #122): the session's principal, installed with the bits (producer ids must carry its tag).
    bool ship(WorkerPool& pool, int64_t sessionId, std::span<const uint64_t> bits, bool grantAll, uint64_t epoch,
              uint64_t owner) {
        // Latest wins: an older queued update of this session is dropped (a worker mid-way through it restarts).
        for (std::size_t i = 0; i < pending_.size(); ++i)
            if (pending_[i].sessionId == sessionId) { pending_.erase(pending_.begin() + static_cast<std::ptrdiff_t>(i)); break; }
        Update u;
        u.sessionId = sessionId;
        u.epoch     = epoch;
        u.grantAll  = grantAll;
        u.owner     = owner;
        u.bits.assign(bits.begin(), bits.end());
        u.pendingMask = allMask_(pool);
        pending_.push_back(std::move(u));
        (void)retry(pool);
        return fullyShippedEpoch(sessionId) >= epoch;
    }

    /// Resume every unfinished update; returns how many chunks were accepted this call.
    int retry(WorkerPool& pool) {
        int sent = 0;
        for (int32_t w = 0; w < pool.workerCount(); ++w) sent += pumpWorker_(pool, w);
        // Completed updates (every worker done) advance the shipped epoch and leave the queue.
        for (std::size_t i = 0; i < pending_.size(); ) {
            if (pending_[i].pendingMask == 0) {
                noteShipped_(pending_[i].sessionId, pending_[i].epoch);
                pending_.erase(pending_.begin() + static_cast<std::ptrdiff_t>(i));
            } else ++i;
        }
        return sent;
    }

    /// The newest epoch of `sessionId` that EVERY worker accepted (0 = none).
    [[nodiscard]] uint64_t fullyShippedEpoch(int64_t sessionId) const noexcept {
        for (const auto& s : shipped_) if (s.sessionId == sessionId) return s.epoch;
        return 0;
    }
    /// The session closed: forget its queued update and its shipped epoch.
    void forget(int64_t sessionId) noexcept {
        for (std::size_t i = 0; i < pending_.size(); ++i)
            if (pending_[i].sessionId == sessionId) { pending_.erase(pending_.begin() + static_cast<std::ptrdiff_t>(i)); break; }
        for (std::size_t i = 0; i < shipped_.size(); ++i)
            if (shipped_[i].sessionId == sessionId) { shipped_[i] = shipped_.back(); shipped_.pop_back(); break; }
    }
    [[nodiscard]] std::size_t pendingUpdates() const noexcept { return pending_.size(); }
    [[nodiscard]] uint64_t chunksSentForTest() const noexcept { return chunksSent_; }

private:
    struct Update {
        int64_t               sessionId = 0;
        uint64_t              epoch = 0;
        bool                  grantAll = false;
        uint64_t              owner    = 0;
        std::vector<uint64_t> bits;
        uint64_t              pendingMask = 0;                    // workers that have not accepted the last chunk
        std::array<uint32_t, kMaxWorkers> nextWord{};             // per worker: the next word to send
    };
    struct Shipped { int64_t sessionId; uint64_t epoch; };

    static uint64_t allMask_(WorkerPool& pool) noexcept {
        const int32_t n = pool.workerCount();
        return n >= 64 ? ~uint64_t{0} : ((uint64_t{1} << n) - 1);
    }

    /// Worker `w`: send chunks of its first unfinished update (in queue order) until its ring is full or all are done.
    int pumpWorker_(WorkerPool& pool, int32_t w) {
        const uint64_t bit = uint64_t{1} << w;
        int sent = 0;
        for (Update& u : pending_) {
            if (!(u.pendingMask & bit)) continue;
            for (;;) {
                const std::size_t total = u.bits.size();
                const std::size_t first = u.nextWord[static_cast<std::size_t>(w)];
                const std::size_t count = std::min(kChunkWords, total - first);
                const bool last = first + count == total;
                WorkerOp op;
                op.type          = WorkerOp::Type::SessionPermissions;
                op.sessionId     = u.sessionId;
                op.nextOffset    = static_cast<int64_t>(first);
                op.hwBound       = static_cast<int64_t>(total);
                op.correlationId = static_cast<int64_t>(u.epoch);
                op.producerId    = static_cast<int64_t>(u.owner);   // #122: the principal (every chunk)
                op.isolation     = static_cast<uint8_t>((last ? kPermFlagLast : 0u) | (u.grantAll ? kPermFlagGrantAll : 0u));
                op.permWords     = u.bits.data() + first;
                op.permWordCount = count;
                if (!pool.worker(w).tryEnqueueOp(std::move(op))) return sent;   // ring full: resume here next retry
                ++sent;
                ++chunksSent_;
                u.nextWord[static_cast<std::size_t>(w)] = static_cast<uint32_t>(first + count);
                if (last) { u.pendingMask &= ~bit; break; }
            }
        }
        return sent;
    }

    void noteShipped_(int64_t sessionId, uint64_t epoch) {
        for (auto& s : shipped_) if (s.sessionId == sessionId) { s.epoch = std::max(s.epoch, epoch); return; }
        shipped_.push_back({sessionId, epoch});
    }

    std::vector<Update>  pending_;
    std::vector<Shipped> shipped_;
    uint64_t             chunksSent_ = 0;
};

} // namespace rune::node
