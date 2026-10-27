#pragma once
// NsSlotFanout.h - CONTROL-thread fan-out of namespace slot messages to EVERY worker's ingress ring (Task 8, D22).
// Cold (namespace DDL rate).  The control thread owns the pending list; each worker owns its slot table.  A full ring
// is retried next duty cycle; per worker the messages stay in FIFO order (the same ring as TopicDropped and every
// produce op, so an assign always precedes the first op of a topic that carries its slot).

#include <rune/node/NsRingMessages.h>
#include <rune/node/WorkerPool.h>

#include <spdlog/spdlog.h>

#include <cstddef>
#include <cstdint>
#include <variant>
#include <vector>

namespace rune::node {

class NsSlotFanout {
public:
    using Msg = std::variant<NsSlotAssign, NsSlotRelease>;

    void post(const NsSlotAssign& m) { pending_.push_back(Pending{Msg{m}, 0}); }
    void post(const NsSlotRelease& m) { pending_.push_back(Pending{Msg{m}, 0}); }

    /// Control thread, every duty cycle.  Returns the number of ops enqueued.
    int pump(WorkerPool& pool) {
        int work = 0;
        std::size_t done = 0;
        for (auto& p : pending_) {
            const int32_t n = pool.workerCount();
            while (p.nextWorker < n) {
                WorkerOp op = std::visit(
                    [](const auto& m) {
                        if constexpr (std::is_same_v<std::decay_t<decltype(m)>, NsSlotAssign>) return makeNsSlotAssignOp(m);
                        else return makeNsSlotReleaseOp(m);
                    }, p.msg);
                const EnqueueResult r = pool.worker(p.nextWorker).enqueueOp(std::move(op));
                if (r == EnqueueResult::Full) break;   // back-pressure: retry next cycle (FIFO)
                if (r == EnqueueResult::Enqueued) { ++enqueued_; ++work; }
                else [[unlikely]] {
                    ++tooLargeDrops_;
                    spdlog::error("NsSlotFanout: a namespace slot message cannot be enqueued on worker {} (TooLarge)",
                                  p.nextWorker);
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

private:
    struct Pending { Msg msg; int32_t nextWorker; };
    std::vector<Pending> pending_;
    uint64_t             enqueued_ = 0;
    uint64_t             tooLargeDrops_ = 0;
};

}  // namespace rune::node
