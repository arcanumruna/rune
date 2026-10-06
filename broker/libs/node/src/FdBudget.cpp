#include <rune/node/FdBudget.h>

#include <spdlog/spdlog.h>

#include <cerrno>
#include <cstring>
#include <string>
#include <sys/resource.h>

namespace rune::node {

namespace {
uint64_t fdsPerPartition(uint64_t openSegments) noexcept {
    return openSegments * kFdsPerSegment + kE1dFdsPerLog;
}
uint64_t openSegmentsFor(const TopicSpec& t) noexcept {
    if (t.retentionBytes <= 0 || t.segmentRollBytes <= 0) return kDefaultOpenSegments;
    const auto roll = static_cast<uint64_t>(t.segmentRollBytes);
    const auto ret  = static_cast<uint64_t>(t.retentionBytes);
    return (ret + roll - 1) / roll + 1;   // retained segments + the active one
}
} // namespace

uint64_t estimateFdNeed(const std::vector<TopicSpec>& topics, uint64_t readySegmentsPerPartition) noexcept {
    uint64_t need = kBaseFds + kInternalPartitionsReserve * fdsPerPartition(kDefaultOpenSegments);
    for (const auto& t : topics) {
        const uint64_t parts = t.partitionCount > 0 ? static_cast<uint64_t>(t.partitionCount) : 0;
        need += parts * (fdsPerPartition(openSegmentsFor(t)) + readySegmentsPerPartition * kFdsPerSegment);
    }
    return need;
}

FdLimitDecision decideFdLimit(uint64_t soft, uint64_t hard, bool unlimited, uint64_t need) {
    FdLimitDecision d;
    if (unlimited) {
        // NOFILE cannot be set to infinity (kernel nr_open / maxfilesperproc):
        // raise soft to comfortably cover the need instead.
        d.targetSoft = soft > 2 * need ? soft : 2 * need;
        d.verdict = FdLimitVerdict::Ok;
        d.message = "RLIMIT_NOFILE soft " + std::to_string(soft) + " -> " + std::to_string(d.targetSoft) +
                    ", hard unlimited; estimated need " + std::to_string(need);
        return d;
    }
    d.targetSoft = hard > soft ? hard : soft;
    const std::string values = "RLIMIT_NOFILE soft " + std::to_string(soft) + " -> " +
                               std::to_string(d.targetSoft) + ", hard " + std::to_string(hard) +
                               "; estimated need " + std::to_string(need) +
                               " (partitions x (open segments x " + std::to_string(kFdsPerSegment) +
                               " + " + std::to_string(kE1dFdsPerLog) + ") + base)";
    if (hard < need) {
        d.verdict = FdLimitVerdict::Refuse;
        d.message = values + ": the hard open-file limit is below what the configured partitions need — "
                    "refusing to start (a roll / recovery would fail with EMFILE and halt the broker). "
                    "Raise it: systemd LimitNOFILE=1048576, docker --ulimit nofile=1048576:1048576, "
                    "or ulimit -Hn.";
    } else if (hard < need + need / 4) {
        d.verdict = FdLimitVerdict::Warn;
        d.message = values + ": the hard open-file limit is within 25% of the estimated need — "
                    "raise LimitNOFILE / --ulimit nofile before adding partitions.";
    } else {
        d.verdict = FdLimitVerdict::Ok;
        d.message = values;
    }
    return d;
}

bool applyFdLimitAtStartup(const NodeConfig& cfg, std::string& error) {
    struct ::rlimit rl{};
    if (::getrlimit(RLIMIT_NOFILE, &rl) != 0) {
        spdlog::warn("rune-broker: getrlimit(RLIMIT_NOFILE) failed: {} — open-file budget not checked",
                     std::strerror(errno));
        return true;
    }
    const bool unlimited = rl.rlim_max == RLIM_INFINITY;
    const uint64_t need = estimateFdNeed(cfg.topics, static_cast<uint64_t>(cfg.segmentProvisionDepth));
    const FdLimitDecision d = decideFdLimit(static_cast<uint64_t>(rl.rlim_cur),
                                            static_cast<uint64_t>(rl.rlim_max), unlimited, need);
    if (d.verdict == FdLimitVerdict::Refuse) {
        error = d.message;
        return false;
    }
    if (d.targetSoft > static_cast<uint64_t>(rl.rlim_cur)) {
        struct ::rlimit want = rl;
        want.rlim_cur = static_cast<rlim_t>(d.targetSoft);
        if (::setrlimit(RLIMIT_NOFILE, &want) != 0) {
            // macOS caps the soft limit at kern.maxfilesperproc: keep the old
            // soft value and say so (the check above already passed on hard).
            spdlog::warn("rune-broker: setrlimit(RLIMIT_NOFILE, soft={}) failed: {} — soft stays {}",
                         d.targetSoft, std::strerror(errno), static_cast<uint64_t>(rl.rlim_cur));
        }
    }
    struct ::rlimit now{};
    (void)::getrlimit(RLIMIT_NOFILE, &now);
    if (d.verdict == FdLimitVerdict::Warn) spdlog::warn("rune-broker: {}", d.message);
    else spdlog::info("rune-broker: {}", d.message);
    if (static_cast<uint64_t>(now.rlim_cur) < need)
        spdlog::warn("rune-broker: effective soft RLIMIT_NOFILE {} is below the estimated need {}",
                     static_cast<uint64_t>(now.rlim_cur), need);
    return true;
}

} // namespace rune::node
