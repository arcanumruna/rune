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
// #442: the queue is bounded (kMaxQueued items).  A worker whose ring stays full while the queue grows past the cap is
// LOST: it skips every item up to the new end and is resynced later - tryReset() enqueues one RESET op on its ring
// (the worker drops all its tables / bindings and refuses produces retriably) and the gateway then ships a full
// snapshot (tables, binds, lag level, frontier, the outstanding fence).  Items are never coalesced: dropping a queued
// Bind a newer Bind supersedes could let a worker acknowledge a Fence that sat between them without having installed
// the older Bind's revoke.  While any worker is lost shippedSeq() is frozen: nothing that waits for "every worker has
// it" (held login OKs, the object DDL OK) is satisfied on behalf of a worker that skipped items.
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
#include <utility>
#include <vector>

namespace rune::node {

class PermissionShipper {
public:
    static constexpr std::size_t kChunkWords = 512;
    static constexpr int         kMaxWorkers = 64;   // validateShardConfig
    static constexpr int64_t     kDenyTable  = -1;   // a bind to nothing
    static constexpr int64_t     kSuspendedTable = -2;   // a bind to nothing because the namespace is suspended ReadOnly
    static constexpr int64_t     kPasswordChangeTable = -3;   // a bind to nothing: must change the password (#216)

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
    /// Table `tableId`'s IMPORT bitmap, right behind its shipTable (only for a table that has an IMPORT bit).
    uint64_t shipImportTable(WorkerPool& pool, uint32_t tableId, std::span<const uint64_t> bits) {
        Item it{Kind::Table};
        it.key = tableId; it.import = true; it.words.assign(bits.begin(), bits.end()); it.total = bits.size();
        return push_(pool, std::move(it));
    }
    uint64_t patchImportTable(WorkerPool& pool, uint32_t tableId, uint64_t first, std::span<const uint64_t> words, uint64_t total) {
        Item it{Kind::Patch};
        it.key = tableId; it.import = true; it.first = first; it.total = total; it.words.assign(words.begin(), words.end());
        return push_(pool, std::move(it));
    }
    /// Session `sessionId` uses table `tableId` (kDenyTable: nothing); `owner` (issue #122) its principal.
    /// Issue #198: only called for an accepted session, whose first announce wave already reached every worker (the
    /// accept waits for it), so the bind never precedes the announce on a worker's ring; a later announce wave of the
    /// same session keeps the bound table (SessionAckSink::announce seeds grants only for a new session).
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

    /// Issues #603 / #506: this broker's `__security` view lag LEVEL (kSecurityLag*; 0 = not behind) - FIFO behind
    /// every item queued so far, so a worker installs every grant shipped before a level change before it sees it.
    uint64_t securityLag(WorkerPool& pool, uint8_t level) {
        Item it{Kind::SecurityBehind};
        it.first = level;
        return push_(pool, std::move(it));
    }

    /// #500: this broker's applied topic frontier (the catalog's nextTopicId) - FIFO behind every item queued so far,
    /// so a worker installs the permission patches of the event that moved it before it sees the new frontier.
    uint64_t topicFrontier(WorkerPool& pool, int32_t frontier) {
        Item it{Kind::TopicFrontier};
        it.first = static_cast<uint64_t>(static_cast<uint32_t>(frontier));
        return push_(pool, std::move(it));
    }

    /// Resume every worker; returns how many ops were accepted this call.
    int retry(WorkerPool& pool) {
        int sent = 0;
        workers_ = std::min<int32_t>(pool.workerCount(), kMaxWorkers);
        for (int32_t w = 0; w < workers_; ++w)
            if (!lost_[static_cast<std::size_t>(w)]) sent += pumpWorker_(pool, w);
        // A whole queue behind: the workers still at the front hold it back, so they are lost (#442).
        for (int guard = 0; queue_.size() > maxQueued_ && guard < 64; ++guard) {
            bool any = false;
            for (int32_t w = 0; w < workers_; ++w) {
                const auto wi = static_cast<std::size_t>(w);
                if (lost_[wi] || next_[wi].nextSeq != frontSeq_) continue;
                if (lostCount_ == 0) frozenShipped_ = frontSeq_ - 1;   // shippedSeq() before the first loss
                lost_[wi] = true;
                ++lostCount_;
                ++workersLost_;
                any = true;
            }
            trim_();
            if (!any) break;
        }
        trim_();
        return sent;
    }

    /// #442: the queue bound.  `pendingUpdates() <= kMaxQueued` after every push or retry (proof: a push adds one item;
    /// if the queue then exceeds the cap, every worker still at the front is lost - it no longer holds the front
    /// back - and the trim point becomes the minimum cursor over the workers that are not lost, which is > the old
    /// front, so at least one item leaves; with every worker lost the queue empties).
    static constexpr std::size_t kMaxQueued = 65536;
    /// Test seam: a small cap.
    void setMaxQueuedForTest(std::size_t n) noexcept { maxQueued_ = n; }
    /// Workers lost right now / ever (a counter for the gateway to log and export) / resynced so far.
    [[nodiscard]] std::size_t lostNow() const noexcept { return lostCount_; }
    [[nodiscard]] uint64_t workersLost() const noexcept { return workersLost_; }
    [[nodiscard]] uint64_t resyncs() const noexcept { return resyncs_; }

    /// For every lost worker enqueue ONE reset op on its ring; a worker whose ring refuses stays lost.  A worker that
    /// accepted is no longer lost and its cursor moves to the end of the queue (everything older is replaced by the
    /// snapshot the caller must now queue, in this same call, before any other item).  Returns true when at least one
    /// worker was reset.
    bool tryReset(WorkerPool& pool) {
        if (lostCount_ == 0) return false;
        workers_ = std::min<int32_t>(pool.workerCount(), kMaxWorkers);
        bool any = false;
        for (int32_t w = 0; w < workers_; ++w) {
            const auto wi = static_cast<std::size_t>(w);
            if (!lost_[wi]) continue;
            WorkerOp op;
            op.type       = WorkerOp::Type::SessionPermissions;
            op.sessionId  = 0;
            op.topicId    = kPermLagReset;
            op.nextOffset = 0;
            op.isolation  = static_cast<uint8_t>(kPermFlagSecurityBehind | kPermFlagLast);
            if (!pool.worker(w).tryEnqueueOp(std::move(op))) continue;   // ring still full: stays lost
            lost_[wi] = false;
            --lostCount_;
            ++resyncs_;
            ++chunksSent_;
            next_[wi] = Cursor{endSeq_, 0};
            any = true;
        }
        return any;
    }

    /// The newest sequence number queued so far (0 = none) - what a caller compares to know whether it shipped anything.
    [[nodiscard]] uint64_t queuedSeq() const noexcept { return endSeq_ - 1; }
    /// The newest sequence number every worker accepted (0 = none).
    /// While a worker is lost: the value at the first loss (frozen).
    [[nodiscard]] uint64_t shippedSeq() const noexcept { return lostCount_ != 0 ? frozenShipped_ : frontSeq_ - 1; }
    [[nodiscard]] std::size_t pendingUpdates() const noexcept { return queue_.size(); }
    [[nodiscard]] uint64_t chunksSentForTest() const noexcept { return chunksSent_; }
    [[nodiscard]] uint64_t wordsSentForTest() const noexcept { return wordsSent_; }
    /// Test seam (issue #197): while held, push_ / retry() queue items but send nothing, so shippedSeq() cannot
    /// advance; the next retry() after the release ships everything.
    void setHoldForTest(bool on) noexcept { hold_ = on; }
    /// Test seam (#506): the lag levels still queued, in queue order, each with the number of non-lag items queued
    /// before it (so a test can tell whether a permission item precedes a level change).  Meaningful while held.
    [[nodiscard]] std::vector<std::pair<uint8_t, std::size_t>> queuedLagsForTest() const {
        std::vector<std::pair<uint8_t, std::size_t>> out;
        std::size_t others = 0;
        for (const Item& it : queue_) {
            if (it.kind == Kind::SecurityBehind) out.emplace_back(static_cast<uint8_t>(it.first), others);
            else ++others;
        }
        return out;
    }
    /// Test seam (#500): the topic frontiers still queued, in queue order, each with the number of non-frontier,
    /// non-lag items queued before it (a permission patch must precede the frontier of its own event).  Meaningful
    /// while held.
    [[nodiscard]] std::vector<std::pair<int32_t, std::size_t>> queuedFrontiersForTest() const {
        std::vector<std::pair<int32_t, std::size_t>> out;
        std::size_t others = 0;
        for (const Item& it : queue_) {
            if (it.kind == Kind::TopicFrontier) out.emplace_back(static_cast<int32_t>(it.first), others);
            else if (it.kind != Kind::SecurityBehind) ++others;
        }
        return out;
    }

private:
    enum class Kind : uint8_t { Table, Patch, Bind, Drop, Fence, SecurityBehind, TopicFrontier };
    struct Item {
        Kind                  kind;
        int64_t               key   = 0;   ///< table id (Table / Patch / Drop) or session id (Bind)
        uint64_t              first = 0;   ///< Patch: first word; Bind: the table id (as u64; -1 = deny)
        uint64_t              total = 0;   ///< Table / Patch: the table's word count
        uint64_t              epoch = 0;
        uint64_t              owner = 0;
        bool                  grantAll = false;
        bool                  import   = false;   ///< Table / Patch: the IMPORT bitmap, not the WRITE one
        std::vector<uint64_t> words;
    };
    struct Cursor { uint64_t nextSeq = 1; std::size_t nextWord = 0; };   // per worker: the next item and, in a Table, word

    /// Items every worker that is not lost passed leave the queue.
    void trim_() {
        uint64_t low = endSeq_;
        for (int32_t w = 0; w < workers_; ++w)
            if (!lost_[static_cast<std::size_t>(w)]) low = std::min(low, next_[static_cast<std::size_t>(w)].nextSeq);
        while (frontSeq_ < low) { queue_.pop_front(); ++frontSeq_; }
    }

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
        op.topicId = it.import ? kPermBitmapImport : kPermBitmapWrite;
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
        case Kind::TopicFrontier:    // #500: the same wire flag, told apart by `topicId`
            op.topicId       = kPermLagTopicFrontier;
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
    std::size_t                         maxQueued_  = kMaxQueued;
    std::array<bool, kMaxWorkers>       lost_{};          // #442: skipped items, awaiting tryReset()
    std::size_t                         lostCount_  = 0;
    uint64_t                            frozenShipped_ = 0;
    uint64_t                            workersLost_ = 0;
    uint64_t                            resyncs_     = 0;
};

} // namespace rune::node
