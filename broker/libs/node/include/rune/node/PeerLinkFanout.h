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
// Every post gets an install sequence number (strictly increasing, never reused - a link epoch is not: a restarted
// responder counts from 1 again) that rides each delivery; each destination sends it back once it RAN the keys
// (worker: SessionSignal PeerLinkKeysApplied; admin: kA2cPeerLinkKeysApplied).  `applied` answers whether every
// destination has run an install of `peer` at or past a sequence number - the control thread holds the handshake frame
// that lets the peer seal under the new epoch until it has (a plane meeting an epoch it does not hold drops the frame).
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
    /// Control thread: queue `keys` of `peer` (with its install mode) for every worker and the admin thread.  Returns
    /// the install's sequence number (`applied` reports when every destination ran it).
    uint64_t post(int32_t peer, const rune::peerlink::LinkKeys& keys) {
        const uint64_t seq = ++nextSeq_;
        Pending* newest = nullptr;
        std::size_t count = 0;
        for (auto& p : pending_)
            if (p.peer == peer) { newest = &p; ++count; }
        if (newest != nullptr && newest->keys.epoch == keys.epoch) {   // the same epoch again (a promotion): supersede
            newest->keys = keys; newest->seq = seq; newest->workersDone = 0; newest->adminDone = false;
            return seq;
        }
        if (count >= kMaxPendingPerPeer) {                              // drop the peer's OLDEST pending entry
            for (std::size_t i = 0; i < pending_.size(); ++i) {
                if (pending_[i].peer != peer) continue;
                scrub_(pending_[i].keys.sendMaster.data(), 32); scrub_(pending_[i].keys.recvMaster.data(), 32);
                pending_.erase(pending_.begin() + static_cast<std::ptrdiff_t>(i));
                break;
            }
        }
        pending_.push_back(Pending{peer, keys, seq, 0, false});
        return seq;
    }

    /// Control thread: worker `worker` / the admin thread ran the install `seq` of `peer`.
    void onWorkerApplied(int32_t worker, int32_t peer, uint64_t seq) {
        if (worker < 0) [[unlikely]] return;
        Applied& a = appliedOf_(peer);
        if (a.workers.size() <= static_cast<std::size_t>(worker)) a.workers.resize(static_cast<std::size_t>(worker) + 1, 0);
        a.workers[static_cast<std::size_t>(worker)] = std::max(a.workers[static_cast<std::size_t>(worker)], seq);
    }
    void onAdminApplied(int32_t peer, uint64_t seq) { Applied& a = appliedOf_(peer); a.admin = std::max(a.admin, seq); }
    /// Every one of `workers` workers and (`admin`) the admin thread ran an install of `peer` at or past `seq`: in post
    /// order per destination, so it holds that install's keys or a newer one's (each keeps its older epochs in slots).
    [[nodiscard]] bool applied(int32_t peer, uint64_t seq, int32_t workers, bool admin) const noexcept {
        const Applied* a = nullptr;
        for (const auto& x : appliedByPeer_) if (x.peer == peer) { a = &x; break; }
        if (workers <= 0 && !admin) return true;
        if (a == nullptr) return false;
        if (admin && a->admin < seq) return false;
        for (int32_t w = 0; w < workers; ++w)
            if (static_cast<std::size_t>(w) >= a->workers.size() || a->workers[static_cast<std::size_t>(w)] < seq) return false;
        return true;
    }

    /// Control thread, every duty cycle.  `admin` may be null (no admin thread).  Returns the number delivered.
    int pump(WorkerPool* pool, OneToOneByteRing* admin) { return pumpWith<WorkerPool>(pool, admin); }

    /// The pump body, templated on the pool type only as a test seam (a fake pool lets a test drain a ring between two
    /// attempts of one pump); header-only, no virtuals.
    template <class Pool>
    int pumpWith(Pool* pool, OneToOneByteRing* admin) {
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
                    op.correlationId = static_cast<int64_t>(p.seq);       // sent back once the worker ran it
                    std::memcpy(op.peerKeys.data(), p.keys.sendMaster.data(), 32);
                    std::memcpy(op.peerKeys.data() + 32, p.keys.recvMaster.data(), 32);
                    const EnqueueResult r = pool->worker(w).enqueueOp(op);
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
                    m.seq = p.seq;
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
    struct Pending { int32_t peer; rune::peerlink::LinkKeys keys; uint64_t seq; int32_t workersDone; bool adminDone; };
    /// The newest install sequence number each destination ran, per peer.
    struct Applied { int32_t peer; std::vector<uint64_t> workers; uint64_t admin = 0; };
    Applied& appliedOf_(int32_t peer) {
        for (auto& a : appliedByPeer_) if (a.peer == peer) return a;
        appliedByPeer_.push_back(Applied{peer, {}, 0});
        return appliedByPeer_.back();
    }
    static void scrub_(uint8_t* p, std::size_t n) noexcept {
        volatile uint8_t* v = p;
        for (std::size_t i = 0; i < n; ++i) v[i] = 0;
    }
    std::vector<Pending> pending_;
    std::vector<Applied> appliedByPeer_;
    uint64_t             delivered_ = 0;
    uint64_t             nextSeq_   = 0;
};

} // namespace rune::node
