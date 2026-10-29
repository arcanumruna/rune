#pragma once
// ClusterFeatureLevelFanout.h — checked writes Part 02: CONTROL-thread distribution of the committed metadata feature
// level to every worker (a WorkerOp::Type::ClusterFeatureLevel on its op ring) and the admin thread
// (kMsgClusterFeatureLevel on the control -> admin ring).  Cold (one per level change).  Resumable per destination, the
// PeerLinkFanout way: a full ring is retried on the next duty cycle, a destination already reached is never re-sent,
// and a newer level SUPERSEDES an undelivered older one.
#include <rune/node/AdminChannels.h>
#include <rune/node/OneToOneByteRing.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>

#include <spdlog/spdlog.h>

#include <cstdint>

namespace rune::node {

class ClusterFeatureLevelFanout {
public:
    /// Control thread: queue `level` for every worker and the admin thread; replaces an undelivered level.
    void post(int16_t level) noexcept {
        level_       = level;
        workersDone_ = 0;
        adminDone_   = false;
        pending_     = true;
    }

    /// Control thread, every duty cycle.  `admin` may be null (no admin thread).  Returns the number delivered.
    int pump(WorkerPool* pool, OneToOneByteRing* admin) {
        if (!pending_) return 0;
        int work = 0;
        if (pool != nullptr) {
            for (int32_t w = workersDone_; w < pool->workerCount(); ++w) {
                WorkerOp op;
                op.type       = WorkerOp::Type::ClusterFeatureLevel;
                op.nextOffset = level_;
                const EnqueueResult r = pool->worker(w).enqueueOp(std::move(op));
                if (r == EnqueueResult::Full) break;   // back-pressure: this worker again next cycle
                if (r == EnqueueResult::TooLarge) [[unlikely]]
                    spdlog::error("ClusterFeatureLevelFanout: level {} cannot be enqueued on worker {} (TooLarge)", level_, w);
                else ++work;
                ++workersDone_;
            }
        }
        if (!adminDone_) {
            if (admin == nullptr) adminDone_ = true;
            else {
                uint8_t buf[kClusterFeatureLevelWireLen];
                writeClusterFeatureLevel(buf, level_);
                if (admin->write(buf, sizeof buf, kMsgClusterFeatureLevel)) { adminDone_ = true; ++work; }
            }
        }
        if (adminDone_ && (pool == nullptr || workersDone_ >= pool->workerCount())) pending_ = false;
        return work;
    }

    [[nodiscard]] bool pendingForTest() const noexcept { return pending_; }

private:
    int16_t level_       = 0;
    int32_t workersDone_ = 0;
    bool    adminDone_   = false;
    bool    pending_     = false;
};

} // namespace rune::node
