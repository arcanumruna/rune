#pragma once
// TopicDroppedFanout.h — Task 23 fix round 1 (R-M1): CONTROL-thread fan-out of "the catalog dropped
// topicId" to EVERY worker's ingress ring (WorkerOp::Type::TopicDropped).  Cold (once per DDL drop).
// The control thread owns the pending list; each worker owns its dropped-topic bit set.  A full ring
// is retried next duty cycle (FIFO per topic id).
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>

#include <spdlog/spdlog.h>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace rune::node {

class TopicDroppedFanout {
public:
    /// Control thread: queue `topicId` for every worker (idempotent on the worker side).
    void post(int32_t topicId) { pending_.push_back(Pending{topicId, 0}); }

    /// Control thread, every duty cycle.  Returns the number of ops enqueued.
    int pump(WorkerPool& pool) {
        int work = 0;
        std::size_t done = 0;
        for (auto& p : pending_) {
            const int32_t n = pool.workerCount();
            while (p.nextWorker < n) {
                WorkerOp op;
                op.type    = WorkerOp::Type::TopicDropped;
                op.topicId = p.topicId;
                const EnqueueResult r = pool.worker(p.nextWorker).enqueueOp(std::move(op));
                if (r == EnqueueResult::Full) break;   // back-pressure: retry next cycle
                if (r == EnqueueResult::Enqueued) { ++enqueued_; ++work; }
                else if (r == EnqueueResult::TooLarge) [[unlikely]] {   // impossible for a scalar op: loud, never silent
                    ++tooLargeDrops_;
                    spdlog::error("TopicDroppedFanout: TopicDropped for topicId={} cannot be enqueued on worker {} "
                                  "(TooLarge) - that worker keeps answering NotLeader instead of UnknownTopic",
                                  p.topicId, p.nextWorker);
                }
                ++p.nextWorker;
            }
            if (p.nextWorker < n) break;               // FIFO
            ++done;
        }
        if (done) pending_.erase(pending_.begin(), pending_.begin() + static_cast<std::ptrdiff_t>(done));
        return work;
    }

    [[nodiscard]] std::size_t pendingForTest() const noexcept { return pending_.size(); }
    [[nodiscard]] uint64_t    enqueuedForTest() const noexcept { return enqueued_; }
    [[nodiscard]] uint64_t    tooLargeDropsForTest() const noexcept { return tooLargeDrops_; }

private:
    struct Pending { int32_t topicId; int32_t nextWorker; };
    std::vector<Pending> pending_;
    uint64_t             enqueued_ = 0;
    uint64_t             tooLargeDrops_ = 0;
};

}  // namespace rune::node
