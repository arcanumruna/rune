#pragma once
// PeerLinkFanout.h — authentication plan Task 15 (D33): CONTROL-thread distribution of an established peer link's
// keys to every thread that owns an inter-broker transport and is not the control thread: every worker (a
// WorkerOp::Type::PeerLinkKeys on its lifecycle ring) and the admin thread (kMsgPeerLinkKeys on the control -> admin
// ring).  Cold (one per link establishment).  Resumable per destination, the SessionAnnouncer way: a full ring is
// retried on the next duty cycle, a destination already reached is never re-sent; a newer epoch for the same peer
// SUPERSEDES an undelivered older one (the older epoch is moot — its grace period would be over before it landed).
// The key bytes are scrubbed from the pending entry the moment every destination has them.
#include <rune/node/AdminChannels.h>
#include <rune/node/OneToOneByteRing.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/peerlink/PeerLinkKeys.h>

#include <spdlog/spdlog.h>

#include <cstdint>
#include <cstring>
#include <vector>

namespace rune::node {

class PeerLinkFanout {
public:
    /// Control thread: queue `keys` of `peer` for every worker and the admin thread.
    void post(int32_t peer, const rune::peerlink::LinkKeys& keys) {
        for (auto& p : pending_)
            if (p.peer == peer) { p.keys = keys; p.workersDone = 0; p.adminDone = false; return; }
        pending_.push_back(Pending{peer, keys, 0, false});
    }

    /// Control thread, every duty cycle.  `admin` may be null (no admin thread).  Returns the number delivered.
    int pump(WorkerPool* pool, OneToOneByteRing* admin) {
        int work = 0;
        for (std::size_t i = 0; i < pending_.size(); ) {
            Pending& p = pending_[i];
            if (pool != nullptr) {
                for (int32_t w = p.workersDone; w < pool->workerCount(); ++w) {
                    WorkerOp op;
                    op.type      = WorkerOp::Type::PeerLinkKeys;
                    op.topicId   = p.peer;
                    op.partition = static_cast<int32_t>(p.keys.epoch);
                    std::memcpy(op.peerKeys.data(), p.keys.sendMaster.data(), 32);
                    std::memcpy(op.peerKeys.data() + 32, p.keys.recvMaster.data(), 32);
                    const EnqueueResult r = pool->worker(w).enqueueOp(std::move(op));
                    scrub_(op.peerKeys.data(), op.peerKeys.size());
                    if (r == EnqueueResult::Full) break;   // back-pressure: this worker again next cycle
                    if (r == EnqueueResult::TooLarge) [[unlikely]]
                        spdlog::error("PeerLinkFanout: keys of node {} cannot be enqueued on worker {} (TooLarge)", p.peer, w);
                    else { ++delivered_; ++work; }
                    ++p.workersDone;
                }
            }
            if (!p.adminDone) {
                if (admin == nullptr) p.adminDone = true;
                else {
                    uint8_t buf[kPeerLinkKeysWireLen];
                    PeerLinkKeysMsg m;
                    m.peer = p.peer; m.epoch = p.keys.epoch; m.sendMaster = p.keys.sendMaster; m.recvMaster = p.keys.recvMaster;
                    writePeerLinkKeys(buf, m);
                    const bool ok = admin->write(buf, sizeof buf, kMsgPeerLinkKeys);
                    scrub_(buf, sizeof buf);
                    scrub_(m.sendMaster.data(), 32); scrub_(m.recvMaster.data(), 32);
                    if (ok) { p.adminDone = true; ++delivered_; ++work; }
                }
            }
            const bool workersDone = pool == nullptr || p.workersDone >= pool->workerCount();
            if (workersDone && p.adminDone) {
                scrub_(p.keys.sendMaster.data(), 32); scrub_(p.keys.recvMaster.data(), 32);
                pending_[i] = pending_.back();
                pending_.pop_back();
            } else {
                ++i;
            }
        }
        return work;
    }

    [[nodiscard]] std::size_t pendingForTest() const noexcept { return pending_.size(); }
    [[nodiscard]] uint64_t    deliveredForTest() const noexcept { return delivered_; }

private:
    struct Pending { int32_t peer; rune::peerlink::LinkKeys keys; int32_t workersDone; bool adminDone; };
    static void scrub_(uint8_t* p, std::size_t n) noexcept {
        volatile uint8_t* v = p;
        for (std::size_t i = 0; i < n; ++i) v[i] = 0;
    }
    std::vector<Pending> pending_;
    uint64_t             delivered_ = 0;
};

} // namespace rune::node
