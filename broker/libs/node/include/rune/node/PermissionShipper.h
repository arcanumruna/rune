#pragma once
// PermissionShipper.h - control-thread, resumable shipping of the WRITE tables and the sessions' bindings to every
// shard worker (authentication Task 13, D30; issue #259 - the SessionAnnouncer pattern).
//
// Issue #259: a table is shipped ONCE per distinct grant set (gateway PermissionPool), not once per session; a session
// is bound to it by one small op; a created / dropped object patches the one word that changed.  A role change over
// 10k sessions therefore ships at most one table plus 10k binds (instead of 10k full bitmaps), and a CREATE TOPIC one
// word per live table (instead of one full bitmap per session that gains WRITE).
//
// One FIFO of items (Table / Patch / Bind / Drop / SecurityBehind, issue #603), each with a sequence number; every worker receives every item, in
// queue order, as SESSION_PERMISSIONS ops (a Table in chunks of <= kChunkWords words, the last flagged; issue #196: a
// Fence, which each worker acknowledges once it installed everything before it).  A full
// lifecycle ring stops that worker at its next op and retry() resumes there - an op is never sent twice and never
// skipped; no item is coalesced or reordered (a bind always follows the content of the table it names, and a table's
// drop follows every rebind away from it).  shippedSeq() is the newest item EVERY worker accepted: the gateway answers
// a login only once its bind is there (announce-before-accept for permissions), and the D24 gate waits for an empty
// queue.
//
// Control plane (login / refresh / DDL rate), control thread only.  Allocates only when an item is queued (cold).
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/node/WorkerWriteGrants.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <deque>
#include <span>
#include <vector>

namespace rune::node {

class PermissionShipper {
public:
    static constexpr std::size_t kChunkWords = 512;
    static constexpr int         kMaxWorkers = 64;   // validateShardConfig
    static constexpr int64_t     kDenyTable  = -1;   // a bind to nothing
    static constexpr int64_t     kSuspendedTable = -2;   // a bind to nothing because the namespace is suspended ReadOnly

    /// Table `tableId`'s full WRITE bitmap.  Returns the item's sequence number.
    uint64_t shipTable(WorkerPool& pool, uint32_t tableId, std::span<const uint64_t> bits, bool grantAll) {
        Item it{Kind::Table};
        it.key = tableId; it.grantAll = grantAll; it.words.assign(bits.begin(), bits.end()); it.total = bits.size();
        return push_(pool, std::move(it));
    }
    /// Words [first, first + words.size()) of table `tableId`, which now has `total` words.
    uint64_t patchTable(WorkerPool& pool, uint32_t tableId, uint64_t first, std::span<const uint64_t> words, uint64_t total) {
        Item it{Kind::Patch};
        it.key = tableId; it.first = first; it.total = total; it.words.assign(words.begin(), words.end());
        return push_(pool, std::move(it));
    }
    /// Session `sessionId` uses table `tableId` (kDenyTable: nothing); `owner` (issue #122) its principal.
    uint64_t bind(WorkerPool& pool, int64_t sessionId, int64_t tableId, uint64_t epoch, uint64_t owner) {
        Item it{Kind::Bind};
        it.key = sessionId; it.first = static_cast<uint64_t>(tableId); it.epoch = epoch; it.owner = owner;
        return push_(pool, std::move(it));
    }
    /// Table `tableId` has no holder any more (its id may be reused by a later shipTable).
    uint64_t dropTable(WorkerPool& pool, uint32_t tableId) {
        Item it{Kind::Drop};
        it.key = tableId;
        return push_(pool, std::move(it));
    }

    /// Issue #196: an enforcement fence `fenceSeq` behind every item queued so far.  Each worker acknowledges it
    /// (SessionSignal FenceAck{sessionId 1}) once it INSTALLED those items - ring acceptance (shippedSeq) is not
    /// installation.  The D24 gate waits for the acks, not for an empty queue.
    uint64_t fence(WorkerPool& pool, uint64_t fenceSeq) {
        Item it{Kind::Fence};
        it.epoch = fenceSeq;
        return push_(pool, std::move(it));
    }

    /// Issue #603: this broker's `__security` view is (behind = true) / is no longer behind committed events it holds -
    /// FIFO behind every item queued so far.
    uint64_t securityBehind(WorkerPool& pool, bool behind) {
        Item it{Kind::SecurityBehind};
        it.first = behind ? 1 : 0;
        return push_(pool, std::move(it));
    }

    /// Resume every worker; returns how many ops were accepted this call.
    int retry(WorkerPool& pool) {
        int sent = 0;
        workers_ = std::min<int32_t>(pool.workerCount(), kMaxWorkers);
        for (int32_t w = 0; w < workers_; ++w) sent += pumpWorker_(pool, w);
        // Items every worker passed leave the queue.
        uint64_t low = endSeq_;
        for (int32_t w = 0; w < workers_; ++w) low = std::min(low, next_[static_cast<std::size_t>(w)].nextSeq);
        while (frontSeq_ < low) { queue_.pop_front(); ++frontSeq_; }
        return sent;
    }

    /// The newest sequence number queued so far (0 = none) - what a caller compares to know whether it shipped anything.
    [[nodiscard]] uint64_t queuedSeq() const noexcept { return endSeq_ - 1; }
    /// The newest sequence number every worker accepted (0 = none).
    [[nodiscard]] uint64_t shippedSeq() const noexcept { return frontSeq_ - 1; }
    [[nodiscard]] std::size_t pendingUpdates() const noexcept { return queue_.size(); }
    [[nodiscard]] uint64_t chunksSentForTest() const noexcept { return chunksSent_; }
    [[nodiscard]] uint64_t wordsSentForTest() const noexcept { return wordsSent_; }
    /// Test seam (issue #197): while held, push_ / retry() queue items but send nothing, so shippedSeq() cannot
    /// advance; the next retry() after the release ships everything.
    void setHoldForTest(bool on) noexcept { hold_ = on; }

private:
    enum class Kind : uint8_t { Table, Patch, Bind, Drop, Fence, SecurityBehind };
    struct Item {
        Kind                  kind;
        int64_t               key   = 0;   ///< table id (Table / Patch / Drop) or session id (Bind)
        uint64_t              first = 0;   ///< Patch: first word; Bind: the table id (as u64; -1 = deny)
        uint64_t              total = 0;   ///< Table / Patch: the table's word count
        uint64_t              epoch = 0;
        uint64_t              owner = 0;
        bool                  grantAll = false;
        std::vector<uint64_t> words;
    };
    struct Cursor { uint64_t nextSeq = 1; std::size_t nextWord = 0; };   // per worker: the next item and, in a Table, word

    uint64_t push_(WorkerPool& pool, Item it) {
        queue_.push_back(std::move(it));
        const uint64_t seq = endSeq_++;
        (void)retry(pool);
        return seq;
    }

    WorkerOp opOf_(const Item& it, std::size_t word, bool& last) const noexcept {
        WorkerOp op;
        op.type      = WorkerOp::Type::SessionPermissions;
        op.sessionId = it.key;
        last = true;
        switch (it.kind) {
        case Kind::Table: {
            const std::size_t count = std::min<std::size_t>(kChunkWords, it.words.size() - word);
            last = word + count == it.words.size();
            op.nextOffset    = static_cast<int64_t>(word);
            op.hwBound       = static_cast<int64_t>(it.total);
            op.isolation     = static_cast<uint8_t>(kPermFlagTable | (last ? kPermFlagLast : 0u) |
                                                    (it.grantAll ? kPermFlagGrantAll : 0u));
            op.permWords     = it.words.data() + word;
            op.permWordCount = count;
            break;
        }
        case Kind::Patch:
            op.nextOffset    = static_cast<int64_t>(it.first);
            op.hwBound       = static_cast<int64_t>(it.total);
            op.isolation     = kPermFlagPatch | kPermFlagLast;
            op.permWords     = it.words.data();
            op.permWordCount = it.words.size();
            break;
        case Kind::Bind:
            op.nextOffset    = static_cast<int64_t>(it.first);
            op.correlationId = static_cast<int64_t>(it.epoch);
            op.producerId    = static_cast<int64_t>(it.owner);   // #122: the principal
            op.isolation     = kPermFlagBind | kPermFlagLast;
            break;
        case Kind::Drop:
            op.isolation     = kPermFlagDrop | kPermFlagLast;
            break;
        case Kind::Fence:
            op.isolation     = kPermFlagFence | kPermFlagLast;
            op.correlationId = static_cast<int64_t>(it.epoch);   // the fence sequence number
            break;
        case Kind::SecurityBehind:   // issue #603
            op.isolation     = kPermFlagSecurityBehind | kPermFlagLast;
            op.nextOffset    = static_cast<int64_t>(it.first);
            break;
        }
        return op;
    }

    /// Worker `w`: send its next ops in queue order until its ring is full or it has every item.
    int pumpWorker_(WorkerPool& pool, int32_t w) {
        if (hold_) [[unlikely]] return 0;   // test seam only (issue #197)
        Cursor& cur = next_[static_cast<std::size_t>(w)];
        int sent = 0;
        while (cur.nextSeq < endSeq_) {
            const Item& it = queue_[static_cast<std::size_t>(cur.nextSeq - frontSeq_)];
            bool last = true;
            WorkerOp op = opOf_(it, cur.nextWord, last);
            const std::size_t words = op.permWordCount;
            if (!pool.worker(w).tryEnqueueOp(std::move(op))) return sent;   // ring full: resume here next retry
            ++sent;
            ++chunksSent_;
            wordsSent_ += words;
            if (last) { ++cur.nextSeq; cur.nextWord = 0; } else { cur.nextWord += words; }
        }
        return sent;
    }

    std::deque<Item>                    queue_;          // items [frontSeq_, endSeq_)
    uint64_t                            frontSeq_ = 1;
    uint64_t                            endSeq_   = 1;
    std::array<Cursor, kMaxWorkers>     next_{};
    int32_t                             workers_  = 0;
    uint64_t                            chunksSent_ = 0;
    uint64_t                            wordsSent_  = 0;
    bool                                hold_       = false;   // test seam (issue #197)
};

} // namespace rune::node
