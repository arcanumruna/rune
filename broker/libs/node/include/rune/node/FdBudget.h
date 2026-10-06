#pragma once
// FdBudget.h — FD-1 (E1d fix round 3): the broker's open-file budget.
//
// Every recovered partition log holds, for its whole lifetime:
//   * 2 fds for E1d (the recovery-point checkpoint file + its directory), and
//   * up to kFdsPerSegment fds per OPEN segment (the .log fstream, the .log
//     POSIX fd used for pwrite/fdatasync, the .index mapping, the read-only
//     .log mapping).
// Linux defaults to a soft RLIMIT_NOFILE of 1024, which ~300 partitions
// exhaust — the next roll / recovery open() then fails with EMFILE, the log is
// fail-stopped and the broker halts (and on restart recovery fails again).
//
// At startup the broker therefore:
//   1. raises the soft limit to the hard limit and logs both;
//   2. REFUSES to start (clear error) when the hard limit is below the
//      estimated need, and WARNs when the hard limit is within 25% of it.
//
// Estimate (documented, deliberately conservative):
//   need = kBaseFds
//        + sum over configured topics of
//            partitionCount * (openSegments(topic) * kFdsPerSegment + kE1dFdsPerLog)
//        + kInternalPartitionsReserve * (kDefaultOpenSegments * kFdsPerSegment + kE1dFdsPerLog)
//   openSegments(topic) = retentionBytes > 0
//                           ? ceil(retentionBytes / segmentRollBytes) + 1   (retained + active)
//                           : kDefaultOpenSegments                          (unbounded: a floor)
// Every partition is assumed hosted on this node (replication factor up to
// the cluster size).  kBaseFds covers Aeron (driver + client), client and
// replication sockets, worker/io_uring/flusher fds, logs and internal files.
//
// Deployment: set the hard limit high — systemd `LimitNOFILE=1048576`,
// Docker `--ulimit nofile=1048576:1048576` (most engines default to ~1M),
// Kubernetes inherits the container runtime's limit.  See docs/operations.
#include <rune/node/NodeConfig.h>
#include <cstdint>
#include <string>
#include <vector>

namespace rune::node {

inline constexpr uint64_t kFdsPerSegment            = 4;
inline constexpr uint64_t kE1dFdsPerLog             = 2;
inline constexpr uint64_t kDefaultOpenSegments      = 4;
inline constexpr uint64_t kBaseFds                  = 1024;
inline constexpr uint64_t kInternalPartitionsReserve = 64;

/// Estimated open files the broker needs for `topics` (formula above).
/// `readySegmentsPerPartition` (hotpath-A review M3): the segment provisioner
/// may hold up to NodeConfig::segmentProvisionDepth READY segments per user
/// partition (kFdsPerSegment fds each) — counted so the startup check never
/// passes while the provisioner later hits EMFILE.
[[nodiscard]] uint64_t estimateFdNeed(const std::vector<TopicSpec>& topics,
                                      uint64_t readySegmentsPerPartition = 0) noexcept;

enum class FdLimitVerdict : uint8_t { Ok, Warn, Refuse };
struct FdLimitDecision {
    FdLimitVerdict verdict = FdLimitVerdict::Ok;
    uint64_t       targetSoft = 0;   ///< the soft limit to set (= hard)
    std::string    message;          ///< what to log (and the refusal error)
};
/// Pure decision: raise soft to hard; Refuse when hard < need; Warn when
/// hard < need * 1.25.  `unlimited` = the hard limit is RLIM_INFINITY.
[[nodiscard]] FdLimitDecision decideFdLimit(uint64_t soft, uint64_t hard, bool unlimited, uint64_t need);

/// Startup: getrlimit, decide, setrlimit(soft = hard), log both values.
/// Returns false (and sets `error`) when the broker must refuse to start.
bool applyFdLimitAtStartup(const NodeConfig& cfg, std::string& error);

} // namespace rune::node
