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
#include <array>
#include <string>
#include <sys/statvfs.h>
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

    /// Metrics published by this thread (re-review N4).  Monotonic counters.
    enum Metric : int {
        kMetricBuildFailures = 0,     ///< failed builds reported to an owner (each = one inline roll)
        kMetricRollHolds,             ///< Hold demands: rolls held for want of a ready segment
        kMetricUnpreallocatedBuilds,  ///< ready segments built WITHOUT fallocate (over the cap)
        kMetricParkedDropped,         ///< parked demands discarded (log retired / table full)
        kMetricReadMapFailures,       ///< review Md5: off-owner read-map growths that failed
        kMetricCount
    };
    /// Sink called on THIS thread whenever a metric changes (BrokerNode: Aeron
    /// counters).  Plain function pointer — set before the runner starts.
    using MetricSink = void (*)(void* ctx, int metric, int64_t value) noexcept;

    /// `capBytes` (node-wide bytes reserved by ready segments' fallocate):
    /// > 0 fixed, 0 no cap, < 0 AUTO = capFreePct % of the free space of
    /// `capDir` (statvfs, re-evaluated off-thread every kCapRefreshServes
    /// builds).  Over the cap a ready segment is still built, without its
    /// block reservation (re-review N3) — never an inline roll.
    explicit SegmentProvisionerAgent(std::size_t depth, int64_t capBytes = 0, std::string capDir = {},
                                     int capFreePct = 10)
        : depth_(std::clamp<std::size_t>(depth, 1, rune::storage::Log::kMaxProvisionDepth)),
          capMode_(capBytes), capDir_(std::move(capDir)), capFreePct_(std::clamp(capFreePct, 1, 100)),
          capBytes_(capBytes > 0 ? capBytes : (capBytes < 0 ? kUnknownCap : 0)) {
        const auto w = demand_.watchInfo();
        (void)bell_.addWatch(detail::RingWatch{w.buffer, w.length, w.capacity, w.headPositionIndex});
        if (capMode_ < 0) refreshAutoCap_();
    }

    SegmentProvisionerAgent(const SegmentProvisionerAgent&)            = delete;
    SegmentProvisionerAgent& operator=(const SegmentProvisionerAgent&) = delete;

    /// The replica view (NodeRuntimeAgent::enableProvisionerView).  Before the
    /// runner starts.
    void attachViewChannel(CleanerViewChannel* ch) noexcept { channel_ = ch; }
    /// Before the runner starts.
    void setMetricSink(MetricSink fn, void* ctx) noexcept { sink_ = fn; sinkCtx_ = ctx; }

    /// The hook every provisioned log posts its demands through (any worker
    /// thread): Log::setProvisioning(provisioner.hook(), thresholdPct).
    [[nodiscard]] rune::storage::Log::ProvisionHook hook() noexcept {
        return {&onDemand_, this, &onGrow_, &onDispose_};
    }
    [[nodiscard]] uint64_t readMapsGrownForTest() const noexcept { return mapsGrown_; }
    /// Tests without a CleanerViewChannel: put `log` into the current view.
    void addToViewForTest(rune::storage::Log* log) {
        logs_.push_back(log);
        std::sort(logs_.begin(), logs_.end());
    }

    /// The doorbell demands ring: give it to this agent's idle strategy
    /// (ParkUntilWorkIdle(shape, nullptr, 0, doorbell())).
    [[nodiscard]] detail::RingDoorbell* doorbell() noexcept { return &bell_; }

    [[nodiscard]] std::size_t depth() const noexcept { return depth_; }
    [[nodiscard]] int64_t     capBytes() const noexcept { return capBytes_; }
    // Test / observability (this agent's thread, or after it stopped).
    [[nodiscard]] uint64_t builtForTest()          const noexcept { return built_; }
    [[nodiscard]] uint64_t demandsServedForTest()  const noexcept { return demandsServed_; }
    [[nodiscard]] uint64_t demandsIgnoredForTest() const noexcept { return demandsIgnored_; }
    [[nodiscard]] uint64_t failuresForTest()       const noexcept { return metrics_[kMetricBuildFailures]; }
    [[nodiscard]] uint64_t unpreallocatedForTest() const noexcept { return metrics_[kMetricUnpreallocatedBuilds]; }
    [[nodiscard]] uint64_t parkedDroppedForTest()  const noexcept { return metrics_[kMetricParkedDropped]; }
    [[nodiscard]] uint64_t rollHoldsForTest()      const noexcept { return metrics_[kMetricRollHolds]; }
    [[nodiscard]] uint64_t viewGenerationForTest() const noexcept { return viewGen_; }
    [[nodiscard]] int64_t  reservedBytesForTest()  const noexcept { return reserved_; }
    [[nodiscard]] std::size_t parkedDemandsForTest() const noexcept { return parked_.size(); }
    /// Tests: force the next AUTO cap evaluation to this free-space figure.
    void setFreeBytesForTest(int64_t v) noexcept { freeBytesForTest_ = v; if (capMode_ < 0) refreshAutoCap_(); }

    const char* name() const noexcept override { return "rune-provisioner"; }

    int doWork(int64_t /*nowNs*/) override {
        int work = 0;
        if (channel_) work += applyViewUpdates_();
        work += demand_.poll([this](int32_t type, int64_t a, int64_t b) {
            if (type == kDisposeMsgType) {   // review Md5: a retired read mapping (munmap off the owner)
                delete reinterpret_cast<rune::storage::MappedFile*>(static_cast<std::uintptr_t>(a));
                return;
            }
            if (type == kGrowMsgType) { serveGrow_(a, b); return; }
            if (type != kDemandMsgType) return;
            auto* log = reinterpret_cast<rune::storage::Log*>(static_cast<std::uintptr_t>(a));
            const auto kind = static_cast<Demand>(b);
            if (kind == Demand::Hold) bump_(kMetricRollHolds);
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
    static constexpr std::size_t kMaxParked        = 4096;
    static constexpr int64_t     kUnknownCap       = INT64_MAX / 4;   ///< AUTO before statvfs succeeded: no cap
    static constexpr uint32_t    kCapRefreshServes = 64;

    static constexpr int32_t kGrowMsgType = 2, kDisposeMsgType = 3;
    static bool onGrow_(void* ctx, rune::storage::Log* log, int64_t packed) noexcept {
        auto* self = static_cast<SegmentProvisionerAgent*>(ctx);
        if (!self->demand_.offer(kGrowMsgType, static_cast<int64_t>(reinterpret_cast<std::uintptr_t>(log)), packed))
            return false;
        self->bell_.ringIfParked();
        return true;
    }
    static bool onDispose_(void* ctx, void* map) noexcept {
        auto* self = static_cast<SegmentProvisionerAgent*>(ctx);
        if (!self->demand_.offer(kDisposeMsgType, static_cast<int64_t>(reinterpret_cast<std::uintptr_t>(map)), 0))
            return false;
        self->bell_.ringIfParked();
        return true;
    }
    /// Review Md5: map `mib` MiB of the dup'd segment fd and hand it to the log
    /// (never dereferences a log that is not in the current view).
    void serveGrow_(int64_t a, int64_t b) {
        auto* log = reinterpret_cast<rune::storage::Log*>(static_cast<std::uintptr_t>(a));
        const int fd = rune::storage::Log::requestFd(b);
        const int64_t mib = rune::storage::Log::requestMiB(b);
        if (!inView_(log)) { ::close(fd); return; }   // the owner re-requests (count-based) if it is live
        if (holdGrowsForTest_) { heldGrows_.push_back({a, b}); return; }
        try {
            auto* m = new rune::storage::MappedFile(rune::storage::MappedFile::BorrowedFd{fd}, std::string(),
                                                    static_cast<std::size_t>(mib) << 20);
            ::close(fd);
            if (!log->offerReadMap(m, b)) delete m;
            else ++mapsGrown_;
        } catch (...) {
            ::close(fd);
            bump_(kMetricReadMapFailures);
            (void)log->offerReadMapFailure(errno != 0 ? errno : ENOMEM, b);
        }
    }
public:
    /// Tests (re-review N2): hold growth requests; release them (in order) later.
    void holdGrowsForTest(bool on) {
        holdGrowsForTest_ = on;
        if (!on) { auto h = std::move(heldGrows_); heldGrows_.clear(); for (auto& [x, y] : h) serveGrow_(x, y); }
    }
private:
    bool holdGrowsForTest_ = false;
    std::vector<std::pair<int64_t, int64_t>> heldGrows_;
    uint64_t mapsGrown_ = 0;

    static bool onDemand_(void* ctx, rune::storage::Log* log, Demand kind) noexcept {
        auto* self = static_cast<SegmentProvisionerAgent*>(ctx);
        // A full ring returns false: the owner re-posts (never lost).
        if (!self->demand_.offer(kDemandMsgType, static_cast<int64_t>(reinterpret_cast<std::uintptr_t>(log)),
                                 static_cast<int64_t>(kind)))
            return false;
        self->bell_.ringIfParked();
        return true;
    }

    void bump_(int m) noexcept {
        ++metrics_[static_cast<std::size_t>(m)];
        if (sink_ != nullptr) sink_(sinkCtx_, m, static_cast<int64_t>(metrics_[static_cast<std::size_t>(m)]));
    }

    void refreshAutoCap_() noexcept {
        int64_t freeBytes = freeBytesForTest_;
        if (freeBytes < 0) {
            struct ::statvfs sv{};
            if (capDir_.empty() || ::statvfs(capDir_.c_str(), &sv) != 0) return;   // keep the last value
            freeBytes = static_cast<int64_t>(sv.f_bavail) * static_cast<int64_t>(sv.f_frsize);
        }
        // Reservations already held count as "free" for the budget (they are
        // the blocks the cap governs).
        capBytes_ = std::max<int64_t>(0, (freeBytes + reserved_) * capFreePct_ / 100);
    }

    int serve_(rune::storage::Log* log, Demand kind) {
        // Renames FIRST: an adoption is only allowed with room on the log's
        // rename ring, so this keeps the owner's next adoption unblocked.
        int n = log->finalizeStagingRenames();
        Acc& acc = account_(log);
        if (kind == Demand::Adopted) return n;
        if (log->readyQueuedApprox() >= depth_) return n;   // the owner adopts one of those
        if (capMode_ < 0 && ++capServes_ % kCapRefreshServes == 0) refreshAutoCap_();
        const int64_t segBytes = log->rollBytes();
        // Re-review N3: over the cap, still build (off the owner) — without
        // the block reservation.  Never a failure / inline roll for the cap.
        const bool falloc = capMode_ == 0 || reserved_ + segBytes <= capBytes_;
        const int b = log->provisionReady(depth_, falloc);
        if (b < 0) {
            bump_(kMetricBuildFailures);   // reported to the owner by provisionReady (review M1)
        } else if (b > 0) {
            built_ += static_cast<uint64_t>(b);
            if (falloc) { acc.falloc += b; reserved_ += static_cast<int64_t>(b) * segBytes; }
            else for (int i = 0; i < b; ++i) bump_(kMetricUnpreallocatedBuilds);
        }
        return n + (b != 0 ? 1 : 0);
    }

    struct Acc { int64_t falloc = 0; int64_t segBytes = 0; };
    /// Reserved-bytes accounting (estimate, self-correcting): a log's
    /// fallocated ready segments can only shrink to what is still queued.
    Acc& account_(rune::storage::Log* log) {
        Acc& a = perLog_[log];
        a.segBytes = log->rollBytes();
        const auto queued = static_cast<int64_t>(log->readyQueuedApprox());
        if (a.falloc > queued) { reserved_ -= (a.falloc - queued) * a.segBytes; a.falloc = queued; }
        return a;
    }

    void park_(rune::storage::Log* log, Demand kind) {
        for (auto& p : parked_)
            if (p.log == log) {
                if (kind != Demand::Adopted) p.kind = Demand::Threshold;
                return;
            }
        if (parked_.size() >= kMaxParked) {   // owner's Hold re-post recovers it (re-review N2)
            bump_(kMetricParkedDropped);
            return;
        }
        parked_.push_back({log, kind == Demand::Adopted ? Demand::Adopted : Demand::Threshold});
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
            if (!inView_(it->first)) { reserved_ -= it->second.falloc * it->second.segBytes; it = perLog_.erase(it); }
            else ++it;
        }
        // Re-review N2: a parked demand is decided by the FIRST view installed
        // after it was parked — served if its log is in it, else DISCARDED (a
        // retired / never-registered log; never dereferenced).  A live log
        // whose demand raced ahead of its registration and lost is recovered
        // by its owner's Hold re-post.  The table can never accumulate.
        std::vector<Parked> parked;
        parked.swap(parked_);
        for (const auto& pk : parked) {
            if (inView_(pk.log)) { ++demandsServed_; serve_(pk.log, pk.kind); }
            else bump_(kMetricParkedDropped);
        }
        viewGen_    = gen;
        ackPending_ = true;
    }

    struct Parked { rune::storage::Log* log; Demand kind; };

    std::size_t depth_;
    int64_t     capMode_;
    std::string capDir_;
    int         capFreePct_;
    int64_t     capBytes_;
    int64_t     freeBytesForTest_ = -1;
    uint32_t    capServes_ = 0;
    int64_t     reserved_ = 0;
    rune::consensus::ControlRing<64 * 1024> demand_;   // M2O: every worker → this thread (~2.7k demands)
    detail::RingDoorbell                    bell_;
    CleanerViewChannel*                               channel_ = nullptr;
    std::vector<rune::replication::PartitionReplica*> building_;
    std::vector<rune::storage::Log*>                  logs_;      // sorted: the current view's disk logs
    std::unordered_map<rune::storage::Log*, Acc>      perLog_;    // fallocated ready segments per log
    std::vector<Parked>                               parked_;    // demands for logs not (yet) in the view
    uint64_t viewGen_     = 0;
    uint64_t buildingGen_ = 0;
    bool     ackPending_  = false;
    uint64_t built_ = 0, demandsServed_ = 0, demandsIgnored_ = 0;
    std::array<uint64_t, kMetricCount> metrics_{};
    MetricSink sink_ = nullptr;
    void*      sinkCtx_ = nullptr;
};

} // namespace rune::node
