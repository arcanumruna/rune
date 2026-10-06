#pragma once
// SegmentProvisionerAgent.h — hotpath-A F1 + F2: the segment pre-provisioning
// thread.
//
// Why a thread of its own: a roll must never create (open + ftruncate + mmap) or
// fallocate a segment on the worker.  The LogCleanerAgent used to pre-build ONE
// staging segment per log per cycle — and nothing at all while it sat inside a
// compaction pass (seconds for a 64 MiB group, or throttled), so 60-80% of the
// rolls in multi-roll scenarios fell back to an inline create (0.2-17 ms).  This
// agent does nothing else: it builds READY segments (Log::provisionReady —
// files, index map, read map, fallocated blocks) and performs the deferred
// staging → <base> renames (Log::finalizeStagingRenames).
//
// DEMAND-DRIVEN (review M3 — FREE-tier boxes with thousands of mostly idle
// partitions): nothing is built for a log until its owner posts a Threshold
// demand (its active segment crossed NodeConfig::segmentProvisionThresholdPct);
// the log is then topped up to `depth` ready segments.  An Adopted demand
// (posted after each adoption) only finalizes the rename.  A node-wide cap
// bounds the bytes reserved by ready segments.  Every Threshold demand ends in
// a ready segment or a FAILURE record on the log's ready ring (review M1: a
// failed build or the cap) — the owner then rolls inline, so a held produce is
// never unbounded or silent.
//
// Event-driven (rings only, no lock, never a busy spin):
//   worker → provisioner  `demand_`: ONE ControlRing (M2O — every worker thread
//                         offers; this thread polls) + its DOORBELL (review m5):
//                         an offer wakes this thread when it parks, so it parks
//                         long when idle (ParkUntilWorkIdle with doorbell()).
//   control → provisioner the replica view (CleanerViewChannel: the same
//                         generation-acked protocol as the cleaner's — a removed
//                         replica's Log is freed only after this thread acked a
//                         view without it).  A demand naming a log not in the
//                         current view is never dereferenced: it is remembered
//                         (by address only) and served once a view contains it
//                         (a new partition can demand before its view arrives).
//
// The ready ring / rename ring of each Log have exactly one producer / consumer
// on this side: BrokerNode turns the cleaner's own provisioning off
// (LogCleanerAgent::setExternalProvisioner).
#include <rune/consensus/ConsensusAgent.h>
#include <rune/consensus/ControlRing.h>
#include <rune/node/CleanerViewChannel.h>
#include <rune/node/detail/RingDoorbell.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

namespace rune::node {

/// BrokerNode::start guard (review M4 / L8): every worker-driven disk log is
/// wired to the provisioner's demand hook, so a provisioner without a runner
/// would wedge every roll — refuse to start.
inline void requireProvisionerRunner(bool agentBuilt, bool runnerBuilt) {
    if (agentBuilt && !runnerBuilt)
        throw std::logic_error("BrokerNode: SegmentProvisionerAgent built but not wired to a runner");
}

class SegmentProvisionerAgent final : public rune::consensus::Agent {
public:
    static constexpr int32_t kDemandMsgType = 1;
    using Demand = rune::storage::Log::ProvisionDemand;

    /// `capBytes`: node-wide bytes reserved by ready segments (0 = no cap).
    explicit SegmentProvisionerAgent(std::size_t depth, int64_t capBytes = 0)
        : depth_(std::clamp<std::size_t>(depth, 1, rune::storage::Log::kMaxProvisionDepth)),
          capBytes_(capBytes > 0 ? capBytes : 0) {
        const auto w = demand_.watchInfo();
        (void)bell_.addWatch(detail::RingWatch{w.buffer, w.length, w.capacity, w.headPositionIndex});
    }

    SegmentProvisionerAgent(const SegmentProvisionerAgent&)            = delete;
    SegmentProvisionerAgent& operator=(const SegmentProvisionerAgent&) = delete;

    /// The replica view (NodeRuntimeAgent::enableProvisionerView).  Before the
    /// runner starts.
    void attachViewChannel(CleanerViewChannel* ch) noexcept { channel_ = ch; }

    /// The hook every provisioned log posts its demands through (any worker
    /// thread): Log::setProvisioning(provisioner.hook(), thresholdPct).
    [[nodiscard]] rune::storage::Log::ProvisionHook hook() noexcept { return {&onDemand_, this}; }

    /// The doorbell demands ring: give it to this agent's idle strategy
    /// (ParkUntilWorkIdle(shape, nullptr, 0, doorbell())).
    [[nodiscard]] detail::RingDoorbell* doorbell() noexcept { return &bell_; }

    [[nodiscard]] std::size_t depth() const noexcept { return depth_; }
    [[nodiscard]] int64_t     capBytes() const noexcept { return capBytes_; }
    // Test / observability (this agent's thread, or after it stopped).
    [[nodiscard]] uint64_t builtForTest()          const noexcept { return built_; }
    [[nodiscard]] uint64_t demandsServedForTest()  const noexcept { return demandsServed_; }
    [[nodiscard]] uint64_t demandsIgnoredForTest() const noexcept { return demandsIgnored_; }
    [[nodiscard]] uint64_t failuresForTest()       const noexcept { return failures_; }
    [[nodiscard]] uint64_t cappedForTest()         const noexcept { return capped_; }
    [[nodiscard]] uint64_t viewGenerationForTest() const noexcept { return viewGen_; }
    [[nodiscard]] int64_t  reservedBytesForTest()  const noexcept { return reserved_; }
    [[nodiscard]] std::size_t parkedDemandsForTest() const noexcept { return parked_.size(); }

    const char* name() const noexcept override { return "rune-provisioner"; }

    int doWork(int64_t /*nowNs*/) override {
        int work = 0;
        if (channel_) work += applyViewUpdates_();
        work += demand_.poll([this](int32_t type, int64_t a, int64_t b) {
            if (type != kDemandMsgType) return;
            auto* log = reinterpret_cast<rune::storage::Log*>(static_cast<std::uintptr_t>(a));
            const auto kind = static_cast<Demand>(b);
            if (!inView_(log)) {   // removed / not yet in the view: never dereferenced
                ++demandsIgnored_;
                park_(log, kind);
                return;
            }
            ++demandsServed_;
            serve_(log, kind);
        }, 1024);
        return work;
    }

private:
    static bool onDemand_(void* ctx, rune::storage::Log* log, Demand kind) noexcept {
        auto* self = static_cast<SegmentProvisionerAgent*>(ctx);
        // A full ring returns false: the owner re-posts (never lost).
        if (!self->demand_.offer(kDemandMsgType, static_cast<int64_t>(reinterpret_cast<std::uintptr_t>(log)),
                                 static_cast<int64_t>(kind)))
            return false;
        self->bell_.ringIfParked();
        return true;
    }

    int serve_(rune::storage::Log* log, Demand kind) {
        // Renames FIRST: an adoption is only allowed with room on the log's
        // rename ring, so this keeps the owner's next adoption unblocked.
        int n = log->finalizeStagingRenames();
        account_(log);
        if (kind != Demand::Threshold) return n;
        const int64_t segBytes = log->rollBytes();
        const std::size_t queued = log->readyQueuedApprox();
        if (queued >= depth_) return n;   // the owner adopts one of those (each ready or failure)
        if (capBytes_ > 0 && reserved_ + segBytes > capBytes_) {
            // Node-wide reservation cap: the owner rolls this one inline.
            ++capped_;
            log->reportProvisionFailure(ENOSPC);
            return n + 1;
        }
        const int b = log->provisionReady(depth_);
        if (b < 0) ++failures_;   // reported to the owner by provisionReady (review M1)
        else built_ += static_cast<uint64_t>(b);
        account_(log);
        return n + (b != 0 ? 1 : 0);
    }

    /// Reserved-bytes accounting: re-derived per log from its ring occupancy
    /// at every visit (self-correcting — a consumed segment is noticed at the
    /// log's next demand; between visits the estimate only errs high).
    void account_(rune::storage::Log* log) {
        const int64_t now = static_cast<int64_t>(log->readyQueuedApprox()) * log->rollBytes();
        int64_t& was = perLog_[log];
        reserved_ += now - was;
        was = now;
    }

    void park_(rune::storage::Log* log, Demand kind) {
        for (auto& p : parked_)
            if (p.first == log) {
                if (kind == Demand::Threshold) p.second = kind;
                return;
            }
        if (parked_.size() < kMaxParked) parked_.emplace_back(log, kind);
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
            if (building_.size() == total) installView_(gen);
        }, 64);
        if (ackPending_ && channel_->ack.write(reinterpret_cast<const uint8_t*>(&viewGen_), 8,
                                               CleanerViewChannel::kAckMsgType))
            ackPending_ = false;   // a full ring is retried next pass (never lost)
        return work;
    }

    void installView_(uint64_t gen) {
        // Cold (registry change only): the disk-backed logs of the view, sorted.
        logs_.clear();
        for (auto* r : building_)
            if (r != nullptr && r->isDiskBacked() && r->log() != nullptr) logs_.push_back(r->log());
        std::sort(logs_.begin(), logs_.end());
        building_.clear();
        // Logs gone from the view: forget their accounting (their ready
        // segments die with the Log, freed only after this ack).
        for (auto it = perLog_.begin(); it != perLog_.end();) {
            if (!inView_(it->first)) { reserved_ -= it->second; it = perLog_.erase(it); }
            else ++it;
        }
        // Demands that arrived before their log's view: serve them now.
        std::vector<std::pair<rune::storage::Log*, Demand>> parked;
        parked.swap(parked_);
        for (const auto& [log, kind] : parked) {
            if (inView_(log)) { ++demandsServed_; serve_(log, kind); }
            else parked_.emplace_back(log, kind);   // still unknown (bounded by kMaxParked)
        }
        viewGen_    = gen;
        ackPending_ = true;
    }

    static constexpr std::size_t kMaxParked = 4096;

    std::size_t depth_;
    int64_t     capBytes_;
    int64_t     reserved_ = 0;
    rune::consensus::ControlRing<64 * 1024> demand_;   // M2O: every worker → this thread (~2.7k demands)
    detail::RingDoorbell                    bell_;
    CleanerViewChannel*                               channel_ = nullptr;
    std::vector<rune::replication::PartitionReplica*> building_;
    std::vector<rune::storage::Log*>                  logs_;      // sorted: the current view's disk logs
    std::unordered_map<rune::storage::Log*, int64_t>  perLog_;    // reserved bytes per log (estimate)
    std::vector<std::pair<rune::storage::Log*, Demand>> parked_;  // demands for logs not (yet) in the view
    uint64_t viewGen_     = 0;
    uint64_t buildingGen_ = 0;
    bool     ackPending_  = false;
    uint64_t built_ = 0, demandsServed_ = 0, demandsIgnored_ = 0, failures_ = 0, capped_ = 0;
};

} // namespace rune::node
