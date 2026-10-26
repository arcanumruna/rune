// ---------------------------------------------------------------------------
// RuneMetrics.h - every Rune node metric, published in the node's arcanum-metrics file.
//
// Each node creates ONE metrics file (RUNE_METRICS_FILE, default ${dataDir}/metrics/rune-metrics.dat) at start and
// allocates every row of kRuneMetrics in it before any agent thread starts (D59).  cnc.dat holds only the Aeron
// driver's own counters.
//
// D61: the enumerators equal the old Aeron counter type ids (Rune's 1'000'000+ application range; the trailing
// digits echo Aeron Cluster's 201 node role / 203 term), every row's key is the 4-byte LE node id, and the label is
// "<labelPrefix> node=<id>" - so a tool that matched (typeId, key) in cnc.dat matches the same pair here.
// 1'000'204 (the never-published MSM commit position) has no row; 236-239 stay reserved.
//
// Each row names its single writer (MetricOwner): the control thread, the observer (role / term / out-of-sync) or
// the segment provisioner.  Nothing else writes that slot (shared-nothing; SlotWriter is a release store).
//
// This header is pure (no Aeron dependency).  rune_role and the tests decode the key with decodeRoleKey().
// ---------------------------------------------------------------------------
#ifndef RUNE_NODE_RUNE_METRICS_H
#define RUNE_NODE_RUNE_METRICS_H

#include <metrics/MetricsLayout.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace rune::node {

enum class RuneMetric : std::int32_t {
    Role                       = 1'000'201,   // RoleCode
    Term                       = 1'000'203,   // the leadership term this node observes (-1 none)
    StuckMarkers               = 1'000'210,   // txn markers refused > kStuckMarkerNs and still retrying
    OutOfSyncPartitions        = 1'000'211,   // partitions replicated but not in the committed ISR (-1 unknown)
    ProvisionBuildFailures     = 1'000'212,   // segment provisioner metrics (monotonic)
    ProvisionRollHolds         = 1'000'213,
    ProvisionUnpreallocBuilds  = 1'000'214,
    ProvisionParkedDropped     = 1'000'215,
    ProvisionReadMapFailures   = 1'000'216,
    CatalogApplyHalted         = 1'000'217,   // 0 applying, else halted offset + 1
    CatalogAppliedOffset       = 1'000'218,   // last applied __catalog offset (-1 none)
    CatalogUnplaceableTopics   = 1'000'219,
    CatalogBlockedPartitions   = 1'000'220,
    LinkMtuDeficit             = 1'000'221,
    StorageGateState           = 1'000'222,   // 0 CONFIRMED, 1 UNCONFIRMED, 2 UNDECIDED
    LeaderRolesHeld            = 1'000'223,   // monotonic
    StoragePendingNodes        = 1'000'224,
    LostCommittedReports       = 1'000'225,   // monotonic
    SecurityApplyHalted        = 1'000'226,   // 0 applying, else halted offset + 1
    SecurityAppliedOffset      = 1'000'227,   // -1 none
    AuthMode                   = 1'000'228,   // 0 enabled, 1 dev-disabled
    LoginsOk                   = 1'000'229,   // monotonic
    LoginsFailed               = 1'000'230,
    LoginsRateLimited          = 1'000'231,
    SessionsRevoked            = 1'000'232,
    PeerLinksEstablished       = 1'000'233,
    PeerLinkRefusals           = 1'000'234,   // monotonic
    PeerFrameDrops             = 1'000'235,   // monotonic, summed over every transport owner
    // 1'000'236 - 1'000'239 reserved
    StuckTxnOffsetCommits      = 1'000'240,
    StuckTxnOffsetAgeMs        = 1'000'241,
};

enum class MetricOwner : std::uint8_t { Control, Observer, Provisioner };

struct RuneMetricDef {
    RuneMetric id;
    metrics::MetricKind kind;
    const char* labelPrefix;
    MetricOwner owner;
};

namespace detail {
using K = metrics::MetricKind;
using O = MetricOwner;
using M = RuneMetric;
}  // namespace detail

/// One row per live metric, in type-id order.  The row index is the slot index in BrokerNode::slots_.
inline constexpr std::array<RuneMetricDef, 30> kRuneMetrics = {{
    {detail::M::Role,                      detail::K::Gauge,   "rune-cluster-node-role",                       detail::O::Observer},
    {detail::M::Term,                      detail::K::Gauge,   "rune-cluster-leadership-term",                 detail::O::Observer},
    {detail::M::StuckMarkers,              detail::K::Gauge,   "rune-txn-stuck-markers",                       detail::O::Control},
    {detail::M::OutOfSyncPartitions,       detail::K::Gauge,   "rune-node-out-of-sync-partitions",             detail::O::Observer},
    {detail::M::ProvisionBuildFailures,    detail::K::Counter, "rune-segment-provision-build-failures",        detail::O::Provisioner},
    {detail::M::ProvisionRollHolds,        detail::K::Counter, "rune-segment-provision-roll-holds",            detail::O::Provisioner},
    {detail::M::ProvisionUnpreallocBuilds, detail::K::Counter, "rune-segment-provision-unpreallocated-builds", detail::O::Provisioner},
    {detail::M::ProvisionParkedDropped,    detail::K::Counter, "rune-segment-provision-parked-demands-dropped", detail::O::Provisioner},
    {detail::M::ProvisionReadMapFailures,  detail::K::Counter, "rune-segment-provision-read-map-failures",     detail::O::Provisioner},
    {detail::M::CatalogApplyHalted,        detail::K::Gauge,   "rune-catalog-apply-halted",                    detail::O::Control},
    {detail::M::CatalogAppliedOffset,      detail::K::Gauge,   "rune-catalog-applied-offset",                  detail::O::Control},
    {detail::M::CatalogUnplaceableTopics,  detail::K::Gauge,   "rune-catalog-unplaceable-topics",              detail::O::Control},
    {detail::M::CatalogBlockedPartitions,  detail::K::Gauge,   "rune-catalog-blocked-partitions",              detail::O::Control},
    {detail::M::LinkMtuDeficit,            detail::K::Gauge,   "rune_link_mtu_deficit",                        detail::O::Control},
    {detail::M::StorageGateState,          detail::K::Gauge,   "rune-storage-gate-state",                      detail::O::Control},
    {detail::M::LeaderRolesHeld,           detail::K::Counter, "rune-leader-roles-held",                       detail::O::Control},
    {detail::M::StoragePendingNodes,       detail::K::Gauge,   "rune-storage-pending-nodes",                   detail::O::Control},
    {detail::M::LostCommittedReports,      detail::K::Counter, "rune-lost-committed-reports",                  detail::O::Control},
    {detail::M::SecurityApplyHalted,       detail::K::Gauge,   "rune-security-apply-halted",                   detail::O::Control},
    {detail::M::SecurityAppliedOffset,     detail::K::Gauge,   "rune-security-applied-offset",                 detail::O::Control},
    {detail::M::AuthMode,                  detail::K::Gauge,   "rune-auth-mode",                               detail::O::Control},
    {detail::M::LoginsOk,                  detail::K::Counter, "rune-logins-ok",                               detail::O::Control},
    {detail::M::LoginsFailed,              detail::K::Counter, "rune-logins-failed",                           detail::O::Control},
    {detail::M::LoginsRateLimited,         detail::K::Counter, "rune-logins-rate-limited",                     detail::O::Control},
    {detail::M::SessionsRevoked,           detail::K::Counter, "rune-sessions-revoked",                        detail::O::Control},
    {detail::M::PeerLinksEstablished,      detail::K::Gauge,   "rune-peer-links-established",                  detail::O::Control},
    {detail::M::PeerLinkRefusals,          detail::K::Counter, "rune-peer-link-refusals",                      detail::O::Control},
    {detail::M::PeerFrameDrops,            detail::K::Counter, "rune-peer-frame-drops",                        detail::O::Control},
    {detail::M::StuckTxnOffsetCommits,     detail::K::Gauge,   "rune-txn-offset-commits-stuck",                detail::O::Control},
    {detail::M::StuckTxnOffsetAgeMs,       detail::K::Gauge,   "rune-txn-offset-stuck-age-ms",                 detail::O::Control},
}};

inline constexpr std::int32_t kNodeMetricCount = static_cast<std::int32_t>(kRuneMetrics.size());

/// The kRuneMetrics row of `id`; -1 when `id` has no row.  Constexpr, so a call with a constant id folds to the index.
[[nodiscard]] constexpr std::int32_t runeMetricRow(RuneMetric id) noexcept {
    for (std::int32_t i = 0; i < kNodeMetricCount; ++i)
        if (kRuneMetrics[static_cast<std::size_t>(i)].id == id) return i;
    return -1;
}
/// The row of a raw type id (a reader holding an int32 from a file); -1 when it is not a Rune metric.
[[nodiscard]] constexpr std::int32_t runeMetricRow(std::int32_t typeId) noexcept {
    return runeMetricRow(static_cast<RuneMetric>(typeId));
}

namespace detail {
constexpr bool rowsSortedAndUnique() noexcept {
    for (std::size_t i = 1; i < kRuneMetrics.size(); ++i)
        if (static_cast<std::int32_t>(kRuneMetrics[i - 1].id) >= static_cast<std::int32_t>(kRuneMetrics[i].id)) return false;
    return true;
}
}  // namespace detail
static_assert(detail::rowsSortedAndUnique(), "kRuneMetrics rows are in strictly increasing type-id order");
static_assert(runeMetricRow(RuneMetric::Role) == 0 && runeMetricRow(1'000'204) == -1);

// Role codes - mirror Aeron Cluster's Cluster.Role exactly, with an extra NONE sentinel for "this node observes no
// leader yet" (pre-election / gap).
enum class RoleCode : std::int64_t {
    None      = -1,  // no leader observed (election in progress / not ready)
    Follower  = 0,   // == Aeron Cluster.Role.FOLLOWER
    Candidate = 1,   // == Aeron Cluster.Role.CANDIDATE
    Leader    = 2,   // == Aeron Cluster.Role.LEADER
};

// Fixed key size (bytes) of every Rune metric: the 4-byte LE nodeId.
inline constexpr std::size_t kRoleKeyLength = 4;

inline const char* roleCodeName(RoleCode r) noexcept {
    switch (r) {
        case RoleCode::Leader:    return "LEADER";
        case RoleCode::Follower:  return "FOLLOWER";
        case RoleCode::Candidate: return "CANDIDATE";
        case RoleCode::None:      return "NONE";
    }
    return "NONE";
}

// Map a RoleCode int64 (as read from the metric value) to its enum, defaulting unknown values to None.
inline RoleCode roleCodeFromValue(std::int64_t v) noexcept {
    switch (v) {
        case 0:  return RoleCode::Follower;
        case 1:  return RoleCode::Candidate;
        case 2:  return RoleCode::Leader;
        default: return RoleCode::None;
    }
}

// Encode nodeId into the fixed LE key buffer.  `out` must have room for kRoleKeyLength bytes.
inline void encodeRoleKey(std::uint8_t* out, std::int32_t nodeId) noexcept {
    std::memcpy(out, &nodeId, sizeof(std::int32_t));
}

// Decode a key buffer back into nodeId.  Returns false if `len` is too small.
inline bool decodeRoleKey(const std::uint8_t* buf, std::size_t len, std::int32_t& nodeId) noexcept {
    if (buf == nullptr || len < kRoleKeyLength) return false;
    std::memcpy(&nodeId, buf, sizeof(std::int32_t));
    return true;
}

}  // namespace rune::node

#endif  // RUNE_NODE_RUNE_METRICS_H
