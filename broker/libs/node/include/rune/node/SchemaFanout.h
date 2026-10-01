#pragma once
// SchemaFanout.h — F3 round 6, item 3: CONTROL-thread fan-out of SchemaUpdate
// ops to every produce worker.
//
// Shared-nothing: each worker's ingress ring is SPSC with the CONTROL thread
// as its single producer.  The AdminWorker (admin thread) therefore never
// enqueues into a worker ring; it posts a SchemaUpdate on its admin→control
// ring and the control thread hands it here.  SchemaFanout is owned and
// touched ONLY by the control thread (no atomics, no locks).
//
// Delivery: per pending update, a per-worker cursor.  Ok → next worker;
// Full → stop and retry on the next duty cycle (retry-on-full, never a drop);
// TooLarge → the descriptor can NEVER fit that worker's ring: log an error
// naming the size and skip that worker (never retried forever).  Cold path
// (DDL is rare): the pending vector allocates only when a DDL commits.

#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>

#include <spdlog/spdlog.h>

#include <cstdint>
#include <utility>
#include <vector>

namespace rune::node {

class SchemaFanout {
public:
    /// Control thread: queue an update for every worker.
    void post(int32_t topicId, std::vector<uint8_t> blob) {
        pending_.push_back(Pending{topicId, std::move(blob), 0});
    }

    /// Control thread, every duty cycle: deliver as much as the rings accept.
    /// Returns the number of ops enqueued (work count).
    int pump(WorkerPool& pool) {
        int work = 0;
        std::size_t done = 0;
        for (auto& p : pending_) {
            const int32_t n = pool.workerCount();
            while (p.nextWorker < n) {
                WorkerOp op;
                op.type             = WorkerOp::Type::SchemaUpdate;
                op.topicId          = p.topicId;
                op.schemaUpdateBlob = p.blob;   // copy: one per worker (cold path)
                if (enqueueHookForTest_) [[unlikely]] enqueueHookForTest_(enqueueHookCtx_);
                const EnqueueResult r = pool.worker(p.nextWorker).enqueueOp(std::move(op));
                if (r == EnqueueResult::Full) break;   // back-pressure: retry next cycle
                if (r == EnqueueResult::TooLarge) [[unlikely]] {
                    ++tooLargeDrops_;
                    spdlog::error("SchemaFanout: SchemaUpdate for topicId={} ({} B descriptor) "
                                  "can never fit worker {}'s ingress ring (max tail {} B); "
                                  "validator NOT installed on that worker — raise "
                                  "RUNE_WORKER_INGRESS_RING_BYTES",
                                  p.topicId, p.blob.size(), p.nextWorker, pool.maxIngressTail());
                } else {
                    ++enqueued_;
                    ++work;
                }
                ++p.nextWorker;
            }
            if (p.nextWorker < n) break;        // FIFO: later updates wait behind this one
            ++done;
        }
        if (done) pending_.erase(pending_.begin(), pending_.begin() + static_cast<std::ptrdiff_t>(done));
        return work;
    }

    // ---- test hooks (read on the control thread or after it joined) ----
    /// F3 round 7, item 5: called right before every worker-ring enqueue.
    /// nullptr in production (one predicted branch, no thread-id lookup).
    using EnqueueHook = void (*)(void* ctx) noexcept;
    void setEnqueueHookForTest(EnqueueHook h, void* ctx) noexcept { enqueueHookForTest_ = h; enqueueHookCtx_ = ctx; }
    [[nodiscard]] std::size_t pendingForTest() const noexcept { return pending_.size(); }
    [[nodiscard]] uint64_t    enqueuedForTest() const noexcept { return enqueued_; }
    [[nodiscard]] uint64_t    tooLargeDropsForTest() const noexcept { return tooLargeDrops_; }

private:
    struct Pending {
        int32_t              topicId;
        std::vector<uint8_t> blob;
        int32_t              nextWorker;
    };

    std::vector<Pending> pending_;
    uint64_t             enqueued_      = 0;
    uint64_t             tooLargeDrops_ = 0;
    EnqueueHook          enqueueHookForTest_ = nullptr;
    void*                enqueueHookCtx_     = nullptr;
};

} // namespace rune::node
