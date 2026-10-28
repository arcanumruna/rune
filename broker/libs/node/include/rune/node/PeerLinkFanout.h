#pragma once
// PeerLinkFanout.h — authentication plan Task 15 (D33): CONTROL-thread distribution of an established peer link's
// keys to every thread that owns an inter-broker transport and is not the control thread: every worker (a
// WorkerOp::Type::PeerLinkKeys on its lifecycle ring) and the admin thread (kMsgPeerLinkKeys on the control -> admin
// ring).  Cold (one per link establishment).  Resumable per destination, the SessionAnnouncer way: a full ring is
// retried on the next duty cycle, a destination already reached is never re-sent.
// Issue #540: EVERY epoch reaches every destination, in install order per peer - the peer's matching plane may already
// seal under an epoch a destination skipped.  `post` supersedes only the peer's newest pending entry when it is the
// SAME epoch (the receive-only -> send-and-receive promotion: the newer install carries the full keys) and otherwise
// appends; `pump` never lets a later entry of a peer reach a destination before an earlier entry of that peer did.  At
// most PeerFrameCrypto::kEpochSlots entries are pending per peer: one more drops (scrubs) the oldest, which every
// destination would evict under slot pressure anyway.  There is no grace period (issue #241: each transport holds its
// epochs in slots).
// The key bytes are scrubbed from the pending entry the moment every destination has them.
#include <rune/node/AdminChannels.h>
#include <rune/node/OneToOneByteRing.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/peerlink/PeerFrameCrypto.h>
#include <rune/peerlink/PeerLinkKeys.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <climits>
#include <cstdint>
#include <cstring>
#include <vector>

namespace rune::node {

class PeerLinkFanout {
public:
    /// Control thread: queue `keys` of `peer` (with its install mode) for every worker and the admin thread.
    void post(int32_t peer, const rune::peerlink::LinkKeys& keys) {
        Pending* newest = nullptr;
        std::size_t count = 0;
        for (auto& p : pending_)
            if (p.peer == peer) { newest = &p; ++count; }
        if (newest != nullptr && newest->keys.epoch == keys.epoch) {   // the same epoch again (a promotion): supersede
            newest->keys = keys; newest->workersDone = 0; newest->adminDone = false;
            return;
        }
        if (count >= kMaxPendingPerPeer) {                              // drop the peer's OLDEST pending entry
            for (std::size_t i = 0; i < pending_.size(); ++i) {
                if (pending_[i].peer != peer) continue;
                scrub_(pending_[i].keys.sendMaster.data(), 32); scrub_(pending_[i].keys.recvMaster.data(), 32);
                pending_.erase(pending_.begin() + static_cast<std::ptrdiff_t>(i));
                break;
            }
        }
        pending_.push_back(Pending{peer, keys, 0, false});
    }

    /// Control thread, every duty cycle.  `admin` may be null (no admin thread).  Returns the number delivered.
    int pump(WorkerPool* pool, OneToOneByteRing* admin) {
        int work = 0;
        for (std::size_t i = 0; i < pending_.size(); ) {
            Pending& p = pending_[i];
            // Issue #540: an entry reaches worker w / the admin thread only after every EARLIER entry of the same peer
            // did (entries are kept in post order).
            int32_t workerLimit = INT32_MAX;
            bool    adminOpen   = true;
            for (std::size_t j = 0; j < i; ++j) {
                if (pending_[j].peer != p.peer) continue;
                workerLimit = std::min(workerLimit, pending_[j].workersDone);
                adminOpen   = adminOpen && pending_[j].adminDone;
            }
            if (pool != nullptr) {
                const int32_t upTo = std::min(workerLimit, pool->workerCount());
                for (int32_t w = p.workersDone; w < upTo; ++w) {
                    WorkerOp op;
                    op.type      = WorkerOp::Type::PeerLinkKeys;
                    op.topicId   = p.peer;
                    op.partition = static_cast<int32_t>(p.keys.epoch);
                    op.isolation = static_cast<uint8_t>(p.keys.install);   // issue #241: the install mode
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
            if (!p.adminDone && adminOpen) {
                if (admin == nullptr) p.adminDone = true;
                else {
                    uint8_t buf[kPeerLinkKeysWireLen];
                    PeerLinkKeysMsg m;
                    m.peer = p.peer; m.epoch = p.keys.epoch; m.sendMaster = p.keys.sendMaster; m.recvMaster = p.keys.recvMaster;
                    m.install = p.keys.install;
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
                pending_.erase(pending_.begin() + static_cast<std::ptrdiff_t>(i));   // order-preserving (#540)
            } else {
                ++i;
            }
        }
        return work;
    }

    [[nodiscard]] std::size_t pendingForTest() const noexcept { return pending_.size(); }
    [[nodiscard]] uint64_t    deliveredForTest() const noexcept { return delivered_; }

private:
    static constexpr std::size_t kMaxPendingPerPeer = rune::peerlink::PeerFrameCrypto::kEpochSlots;
    struct Pending { int32_t peer; rune::peerlink::LinkKeys keys; int32_t workersDone; bool adminDone; };
    static void scrub_(uint8_t* p, std::size_t n) noexcept {
        volatile uint8_t* v = p;
        for (std::size_t i = 0; i < n; ++i) v[i] = 0;
    }
    std::vector<Pending> pending_;
    uint64_t             delivered_ = 0;
};

} // namespace rune::node
