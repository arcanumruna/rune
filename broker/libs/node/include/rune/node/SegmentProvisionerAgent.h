#pragma once
// SegmentProvisionerAgent.h — hotpath-A F1 + F2: the segment pre-provisioning
// thread.
//
// Why a thread of its own: a roll must never create (open + ftruncate + mmap) or
// fallocate a segment on the worker.  The LogCleanerAgent used to pre-build ONE
// staging segment per log per cycle — and nothing at all while it sat inside a
// compaction pass (seconds for a 64 MiB group, or throttled), so 60-80% of the
// rolls in multi-roll scenarios fell back to an inline create (0.2-17 ms).  This
// agent does nothing else: it keeps `depth` READY segments per disk-backed log
// (Log::provisionReady — files, index map, read map, fallocated blocks) and
// performs the deferred staging → <base> renames (Log::finalizeStagingRenames).
//
// Event-driven (rings only, no lock):
//   worker → provisioner  `demand_`: ONE ControlRing (M2O — every worker thread
//                         offers; this thread polls).  A roll that adopts a ready
//                         segment posts its Log* (Log::ProvisionHook).  The log
//                         is refilled to `depth` and its renames finalized.
//   control → provisioner the replica view (CleanerViewChannel: the same
//                         generation-acked protocol as the cleaner's — a removed
//                         replica's Log is freed only after this thread acked a
//                         view without it).  A demand naming a log not in the
//                         current view is ignored (never dereferenced).
//   A new view (and, as a safety net, every `rescanIntervalNs`) tops up EVERY
//   log in it: new partitions get their first ready segments, and a demand lost
//   to a full ring is recovered.
//
// The ready ring / rename ring of each Log have exactly one producer / consumer
// on this side: BrokerNode turns the cleaner's own provisioning off
// (LogCleanerAgent::setExternalProvisioner).  Idle: a BackoffIdleStrategy (never
// a busy spin).
#include <rune/consensus/ConsensusAgent.h>
#include <rune/consensus/ControlRing.h>
#include <rune/node/CleanerViewChannel.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <vector>

namespace rune::node {

class SegmentProvisionerAgent final : public rune::consensus::Agent {
public:
    static constexpr int32_t kDemandMsgType = 1;
    static constexpr int64_t kDefaultRescanIntervalNs = 100'000'000;   // 100 ms safety net

    explicit SegmentProvisionerAgent(std::size_t depth,
                                     int64_t rescanIntervalNs = kDefaultRescanIntervalNs) noexcept
        : depth_(std::clamp<std::size_t>(depth, 1, rune::storage::Log::kMaxProvisionDepth)),
          rescanIntervalNs_(rescanIntervalNs > 0 ? rescanIntervalNs : kDefaultRescanIntervalNs) {}

    SegmentProvisionerAgent(const SegmentProvisionerAgent&)            = delete;
    SegmentProvisionerAgent& operator=(const SegmentProvisionerAgent&) = delete;

    /// The replica view (NodeRuntimeAgent::enableProvisionerView).  Before the
    /// runner starts.
    void attachViewChannel(CleanerViewChannel* ch) noexcept { channel_ = ch; }

    /// The hook every provisioned log posts its demand through (any worker
    /// thread): Log::setProvisioning(provisioner.hook()).
    [[nodiscard]] rune::storage::Log::ProvisionHook hook() noexcept { return {&onConsumed_, this}; }

    [[nodiscard]] std::size_t depth() const noexcept { return depth_; }
    // Test / observability (this agent's thread, or after it stopped).
    [[nodiscard]] uint64_t builtForTest()          const noexcept { return built_; }
    [[nodiscard]] uint64_t demandsServedForTest()  const noexcept { return demandsServed_; }
    [[nodiscard]] uint64_t demandsIgnoredForTest() const noexcept { return demandsIgnored_; }
    [[nodiscard]] uint64_t viewGenerationForTest() const noexcept { return viewGen_; }

    const char* name() const noexcept override { return "rune-provisioner"; }

    int doWork(int64_t nowNs) override {
        int work = 0;
        if (channel_) work += applyViewUpdates_();
        // 1. Event-driven refills: one per adopted segment (posted by the owner).
        work += demand_.poll([this](int32_t type, int64_t a, int64_t) {
            if (type != kDemandMsgType) return;
            auto* log = reinterpret_cast<rune::storage::Log*>(static_cast<std::uintptr_t>(a));
            if (!inView_(log)) { ++demandsIgnored_; return; }   // removed / not yet in the view
            ++demandsServed_;
            serve_(log);
        }, 1024);
        // 2. A new view, or the safety-net rescan: top up every log.
        if (rescanDue_ || nowNs - lastScanNs_ >= rescanIntervalNs_) {
            rescanDue_  = false;
            lastScanNs_ = nowNs;
            for (rune::storage::Log* log : logs_) work += serve_(log);
        }
        return work;
    }

private:
    static void onConsumed_(void* ctx, rune::storage::Log* log) noexcept {
        auto* self = static_cast<SegmentProvisionerAgent*>(ctx);
        // A full ring loses nothing: the periodic rescan tops the log up.
        (void)self->demand_.offer(kDemandMsgType,
                                  static_cast<int64_t>(reinterpret_cast<std::uintptr_t>(log)), 0);
    }

    int serve_(rune::storage::Log* log) {
        // Renames FIRST: the owner's rename ring holds at most one request per
        // segment adopted since the last visit (<= depth < its capacity).
        int n = log->finalizeStagingRenames();
        const int b = log->provisionReady(depth_);
        built_ += static_cast<uint64_t>(b);
        return n + b;
    }

    [[nodiscard]] bool inView_(const rune::storage::Log* log) const noexcept {
        return std::binary_search(logs_.begin(), logs_.end(), log);
    }

    // The CleanerViewChannel protocol (see LogCleanerAgent::applyViewUpdates_).
    int applyViewUpdates_() {
        int work = channel_->view.drain([this](int32_t type, aeron::concurrent::AtomicBuffer& buf,
                                               aeron::util::index_t off, aeron::util::index_t len) {
            if (type != CleanerViewChannel::kViewMsgType ||
                static_cast<std::size_t>(len) < CleanerViewChannel::kHeaderBytes) return;
            const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + off);
            uint64_t gen; uint32_t total, index, count;
            std::memcpy(&gen, p, 8); std::memcpy(&total, p + 8, 4);
            std::memcpy(&index, p + 12, 4); std::memcpy(&count, p + 16, 4);
            if (index == 0) { building_.clear(); buildingGen_ = gen; }
            else if (gen != buildingGen_ || building_.size() != index) return;   // stale partial
            for (uint32_t i = 0; i < count; ++i) {
                uint64_t v; std::memcpy(&v, p + CleanerViewChannel::kHeaderBytes + 8u * i, 8);
                building_.push_back(reinterpret_cast<rune::replication::PartitionReplica*>(
                    static_cast<std::uintptr_t>(v)));
            }
            if (building_.size() == total) {
                // Cold (registry change only): the disk-backed logs of the view, sorted.
                logs_.clear();
                for (auto* r : building_)
                    if (r != nullptr && r->isDiskBacked() && r->log() != nullptr) logs_.push_back(r->log());
                std::sort(logs_.begin(), logs_.end());
                building_.clear();
                viewGen_    = gen;
                ackPending_ = true;
                rescanDue_  = true;
            }
        }, 64);
        if (ackPending_ && channel_->ack.write(reinterpret_cast<const uint8_t*>(&viewGen_), 8,
                                               CleanerViewChannel::kAckMsgType))
            ackPending_ = false;   // a full ring is retried next pass (never lost)
        return work;
    }

    std::size_t depth_;
    int64_t     rescanIntervalNs_;
    int64_t     lastScanNs_ = 0;
    bool        rescanDue_  = false;
    rune::consensus::ControlRing<64 * 1024> demand_;   // M2O: every worker → this thread (~2.7k demands)
    CleanerViewChannel*                               channel_ = nullptr;
    std::vector<rune::replication::PartitionReplica*> building_;
    std::vector<rune::storage::Log*>                  logs_;      // sorted: the current view's disk logs
    uint64_t viewGen_     = 0;
    uint64_t buildingGen_ = 0;
    bool     ackPending_  = false;
    uint64_t built_ = 0, demandsServed_ = 0, demandsIgnored_ = 0;
};

} // namespace rune::node
