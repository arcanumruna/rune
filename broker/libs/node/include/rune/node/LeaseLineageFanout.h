#pragma once
// LeaseLineageFanout.h - lease topics Task 27 (spec A.6, "How the leader learns it"): the control thread tells the
// worker that owns a transient LEASE partition which data lineage (token + generation) its leadership runs on.
//
// The lineage is committed metadata (TransientLineageRecord).  A leader that reborn a partition (spec A.6: a transient
// lease partition whose lineage changed lost its ring) must fence the old lineage's holders out; the worker therefore
// needs the committed (lineage, generation) of the leadership it runs.  This class decides WHEN to post it: once per
// (leader epoch, lineage) of every partition this node leads, as soon as the lineage is trusted for the leadership.
//
// FIFO argument: the op is posted only when none of the bridge's role ops for that worker still waits for its op ring
// (ReplicationMetadataBridge::roleOpsPendingFor), exactly like ReadLeaseFanout - every role op derived from the metadata
// this pass saw is already on the ring ahead of it, so the lineage note never overtakes the role op of its leadership
// (the worker executes its op ring in order).  A full ring, an untrusted lineage and held role ops are retried on the
// next pass; nothing is lost.
//
// Control thread only.  The posted table holds the led transient lease partitions only (control plane, cold: one entry per
// partition, one op per leadership; a partition no longer led is swept out on the next pass).
#include <cstdint>
#include <unordered_map>

#include <rune/metadata/PartitionAssignment.h>

namespace rune::node {

class LeaseLineageFanout {
public:
    /// One control pass, after MetadataService::doWork.  `changed`: the committed map or the lineages changed since the
    /// last pass (the caller compares the MSM committedVersion / lineageVersion).  `forEachLed(fn)`: calls
    /// fn(topicId, partition, leaderEpoch) for every transient LEASE partition this node leads.  `trusted(t, p)`: the
    /// trusted lineage record or nullptr.  `workerOf(t, p)` -> int32_t.  `pendingFor(w)`: role ops still wait for worker
    /// w's ring.  `enqueue(w, t, p, epoch, lineage, generation)` -> bool (false: the ring is full).  Returns the ops posted.
    template <class ForEachLed, class Trusted, class WorkerOf, class Pending, class Enqueue>
    int step(bool changed, ForEachLed&& forEachLed, Trusted&& trusted, WorkerOf&& workerOf, Pending&& pendingFor,
             Enqueue&& enqueue) {
        if (!changed && !retry_) return 0;
        retry_ = false;
        ++pass_;
        int posted = 0;
        forEachLed([&](int32_t topicId, int32_t partition, int32_t epoch) {
            const int64_t key = (static_cast<int64_t>(topicId) << 32) | static_cast<uint32_t>(partition);
            const auto it = posted_.find(key);
            if (it != posted_.end()) it->second.seenPass = pass_;   // still led: kept by the sweep below
            const rune::metadata::TransientLineage* rec = trusted(topicId, partition);
            if (rec == nullptr) { retry_ = true; return; }
            if (it != posted_.end() && it->second.epoch == epoch && it->second.lineage == rec->lineage) return;
            const int32_t w = workerOf(topicId, partition);
            if (pendingFor(w)) { retry_ = true; ++held_; return; }
            if (!enqueue(w, topicId, partition, epoch, rec->lineage, rec->generation)) { retry_ = true; return; }
            posted_[key] = Posted{epoch, rec->lineage, pass_};
            ++posted;
            ++postedTotal_;
        });
        // A partition no longer led here (leadership moved, topic dropped) leaves the table: it never grows past the led partitions.
        for (auto it = posted_.begin(); it != posted_.end();) {
            if (it->second.seenPass != pass_) it = posted_.erase(it);
            else ++it;
        }
        return posted;
    }

    [[nodiscard]] std::size_t trackedForTest() const noexcept { return posted_.size(); }

    [[nodiscard]] uint64_t postedForTest() const noexcept { return postedTotal_; }
    /// Times a post waited because role ops for the worker were still pending.
    [[nodiscard]] uint64_t heldForTest() const noexcept { return held_; }
    /// The next pass scans even if nothing changed (something was held, untrusted or refused).
    [[nodiscard]] bool retryingForTest() const noexcept { return retry_; }

private:
    struct Posted { int32_t epoch; uint64_t lineage; uint64_t seenPass; };
    std::unordered_map<int64_t, Posted> posted_;   // key = (topicId << 32) | partition
    bool     retry_       = false;
    uint64_t pass_        = 0;
    uint64_t postedTotal_ = 0;
    uint64_t held_        = 0;
};

} // namespace rune::node
