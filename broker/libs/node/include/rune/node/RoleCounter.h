// ---------------------------------------------------------------------------
// RoleCounter.h — Aeron-counter-based cluster node-role publication.
//
// Aeron Cluster publishes leadership through Aeron COUNTERS in the driver's CnC
// (command-and-control) file — a "Cluster Node Role" counter
// (AERON_COUNTER_CLUSTER_NODE_ROLE_TYPE_ID = 201; value codes FOLLOWER=0,
// CANDIDATE=1, LEADER=2) and a separate leadership-term / commit-position
// counter.  Observers read them out-of-band via CountersReader over the mmapped
// cnc.dat — never by scraping logs.
//
// Rune mirrors that discipline with TWO counters per node, both keyed by the
// static nodeId (the key never has to change after allocation — Aeron counter
// keys are immutable once registered):
//
//   * ROLE counter  (type kRoleCounterTypeId):  VALUE = RoleCode
//       mirroring Aeron's role codes, plus a NONE=-1 sentinel for
//       "no leader observed / pre-election".
//   * TERM counter  (type kTermCounterTypeId):  VALUE = leadershipTerm
//       the term this node currently observes (-1 if none).  Lets the oracle
//       discard stale pre-election announcements exactly like the old
//       term-aware log parser (DR-5): pick the LEADER at the highest term.
//
// Both counters carry the same key: the 4-byte LE nodeId, so a reader can
// attribute each counter to its node.
//
// This header is PURE (no Aeron dependency): the type ids, the role codes, and
// the key encode/decode.  That keeps the wire layout unit-testable without a
// media driver.  BrokerNode owns the live Aeron Counters; rune_role (the
// reader) decodes the key with decodeRoleKey().
// ---------------------------------------------------------------------------
#ifndef RUNE_NODE_ROLE_COUNTER_H
#define RUNE_NODE_ROLE_COUNTER_H

#include <cstdint>
#include <cstring>
#include <string>

namespace rune::node {

// Rune's counter type ids.  Deliberately in Rune's own application range
// (1'000'000+) rather than reusing Aeron's 200-221, so they never collide with
// counters Aeron itself registers in the same CnC file.  The trailing digits
// echo Aeron's ids for readability (201 = node role, 203 = commit/term).
inline constexpr std::int32_t kRoleCounterTypeId = 1'000'201;
inline constexpr std::int32_t kTermCounterTypeId = 1'000'203;

// Role codes — mirror Aeron Cluster's Cluster.Role exactly, with an extra
// NONE sentinel for "this node observes no leader yet" (pre-election / gap).
enum class RoleCode : std::int64_t {
    None      = -1,  // no leader observed (election in progress / not ready)
    Follower  = 0,   // == Aeron Cluster.Role.FOLLOWER
    Candidate = 1,   // == Aeron Cluster.Role.CANDIDATE
    Leader    = 2,   // == Aeron Cluster.Role.LEADER
};

// Fixed key-buffer size (bytes): just the 4-byte LE nodeId.
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

// Map a RoleCode int64 (as read from the counter value) to its enum, defaulting
// unknown values to None.
inline RoleCode roleCodeFromValue(std::int64_t v) noexcept {
    switch (v) {
        case 0:  return RoleCode::Follower;
        case 1:  return RoleCode::Candidate;
        case 2:  return RoleCode::Leader;
        default: return RoleCode::None;
    }
}

// Encode nodeId into the fixed LE key buffer.
// `out` must have room for kRoleKeyLength bytes.
inline void encodeRoleKey(std::uint8_t* out, std::int32_t nodeId) noexcept {
    std::memcpy(out, &nodeId, sizeof(std::int32_t));
}

// Decode a key buffer back into nodeId.  Returns false if `len` is too small.
inline bool decodeRoleKey(const std::uint8_t* buf, std::size_t len,
                          std::int32_t& nodeId) noexcept {
    if (buf == nullptr || len < kRoleKeyLength) return false;
    std::memcpy(&nodeId, buf, sizeof(std::int32_t));
    return true;
}

// MSM commit-position counter type id (mirrors Aeron Cluster's commit-position
// counter, id 204).  Published by the metadata state machine on the leader node
// so observers know how far the MSM log has been committed.
inline constexpr std::int32_t kMsmCommitPosTypeId = 1'000'204;

// D9 review MEDIUM-3: "stuck transaction markers" gauge (TCS::stuckMarkers()) —
// markers of Prepare* transactions refused for more than kStuckMarkerNs and
// still retrying.  Key = the nodeId (same layout as the role counter).
inline constexpr std::int32_t kStuckMarkersCounterTypeId = 1'000'210;

// Controlled shutdown review L5 — the rolling-restart readiness gate: the
// number of partitions this node replicates but is NOT in the committed ISR of
// (-1 = unknown: no controller known, or the node is stopping).  An operator
// restarts the next node only once every node reads 0.  Key = the nodeId.
inline constexpr std::int32_t kOutOfSyncCounterTypeId = 1'000'211;

// hotpath-A re-review N4 — segment provisioning metrics (monotonic), published
// by the SegmentProvisionerAgent thread.  Key = the nodeId; one type id per
// metric, in SegmentProvisionerAgent::Metric order.
inline constexpr std::int32_t kProvisionBuildFailuresCounterTypeId = 1'000'212;   // = owner inline rolls
inline constexpr std::int32_t kProvisionRollHoldsCounterTypeId     = 1'000'213;
inline constexpr std::int32_t kProvisionUnpreallocCounterTypeId    = 1'000'214;
inline constexpr std::int32_t kProvisionParkedDroppedCounterTypeId = 1'000'215;
inline constexpr std::int32_t kProvisionReadMapFailuresCounterTypeId = 1'000'216;   // hot-path B review Md5
// Catalog history Task 7 — the CatalogApplier's metrics, set on the control
// thread from the admin→control kA2cCatalogHalted / kA2cCatalogApplied messages.
//   * HALTED counter: 0 = applying; otherwise (halted offset + 1) — an operator
//     alert (docs/CATALOG.md#halted): the broker serves what it applied and
//     accepts no catalog change.
//   * APPLIED-OFFSET counter: the last applied __catalog offset (-1 = none yet).
// Key = the nodeId.
inline constexpr std::int32_t kCatalogApplyHaltedCounterTypeId  = 1'000'217;
inline constexpr std::int32_t kCatalogAppliedOffsetCounterTypeId = 1'000'218;
inline std::string catalogApplyHaltedLabel(std::int32_t nodeId) {
    return "rune-catalog-apply-halted node=" + std::to_string(nodeId);
}
/// Task 11 fix round 1 (I1/I2): topics this broker, as controller, holds Unplaceable.
inline constexpr std::int32_t kCatalogUnplaceableTopicsCounterTypeId = 1'000'219;
inline std::string catalogUnplaceableTopicsLabel(std::int32_t nodeId) {
    return "rune-catalog-unplaceable-topics node=" + std::to_string(nodeId);
}
/// Task 11 fix round 5 (M5): partitions derived BLOCKED — by the ready controller
/// only (other members publish 0; fix round 6), so the cluster sum is the count.
inline constexpr std::int32_t kCatalogBlockedPartitionsCounterTypeId = 1'000'220;
inline std::string catalogBlockedPartitionsLabel(std::int32_t nodeId) {
    return "rune-catalog-blocked-partitions node=" + std::to_string(nodeId);
}
/// DR-bugs fix round 1 (review B-I2): link-MTU watchdog — (Aeron MTU + IP/UDP header) - link MTU while the
/// link MTU on this node's path is below what one Aeron datagram needs, else 0.  Control thread only.
inline constexpr std::int32_t kLinkMtuDeficitCounterTypeId = 1'000'221;
inline std::string linkMtuDeficitLabel(std::int32_t nodeId) {
    return "rune_link_mtu_deficit node=" + std::to_string(nodeId);
}
/// ISR v3 §3.10 (B-I4): the storage gate and the controller's storage tracking.  Control thread only.
///   1'000'222  storage gate state: 0 CONFIRMED, 1 UNCONFIRMED, 2 UNDECIDED (0 on a single node)
///   1'000'223  leader roles held (routed as held, not led) by the closed gate: the bridge's monotonic count
///   1'000'224  nodes whose storage is pending on the READY controller (storageRegs_.size()), else 0
///   1'000'225  below-committed (lost committed) reports the controller received
inline constexpr std::int32_t kStorageGateStateCounterTypeId      = 1'000'222;
inline constexpr std::int32_t kLeaderRolesHeldCounterTypeId       = 1'000'223;
inline constexpr std::int32_t kStoragePendingNodesCounterTypeId   = 1'000'224;
inline constexpr std::int32_t kLostCommittedReportsCounterTypeId  = 1'000'225;
// Authentication Task 9 (counter ids assigned once - Global Constraints): 226 = the `__security` apply halted (the
// halted offset + 1; 0 = applying), 227 = the `__security` applied offset (-1 = none), 228 = the auth mode
// (0 enabled, 1 dev-disabled; in-process configs publish 0).  Same key (node id) as the catalog counters.
inline constexpr std::int32_t kSecurityApplyHaltedCounterTypeId   = 1'000'226;
inline constexpr std::int32_t kSecurityAppliedOffsetCounterTypeId = 1'000'227;
inline constexpr std::int32_t kAuthModeCounterTypeId              = 1'000'228;
// Authentication Task 11: 229 = logins ok, 230 = logins failed, 231 = logins rate-limited (monotonic, per broker; the
// gateway's control-thread values).  Same key (node id) as the other security counters.
inline constexpr std::int32_t kLoginsOkCounterTypeId          = 1'000'229;
inline constexpr std::int32_t kLoginsFailedCounterTypeId      = 1'000'230;
inline constexpr std::int32_t kLoginsRateLimitedCounterTypeId = 1'000'231;
inline std::string loginsOkLabel(std::int32_t nodeId) { return "rune-logins-ok node=" + std::to_string(nodeId); }
inline std::string loginsFailedLabel(std::int32_t nodeId) { return "rune-logins-failed node=" + std::to_string(nodeId); }
inline std::string loginsRateLimitedLabel(std::int32_t nodeId) { return "rune-logins-rate-limited node=" + std::to_string(nodeId); }
// Authentication Task 15: 233 = peers whose inter-broker link is established (0..N-1), 234 = peer-link refusals logged
// by this broker (both directions; monotonic).  Same key (node id) as the other security counters.
inline constexpr std::int32_t kPeerLinksEstablishedCounterTypeId = 1'000'233;
inline constexpr std::int32_t kPeerLinkRefusalsCounterTypeId     = 1'000'234;
inline std::string peerLinksEstablishedLabel(std::int32_t nodeId) { return "rune-peer-links-established node=" + std::to_string(nodeId); }
inline std::string peerLinkRefusalsLabel(std::int32_t nodeId) { return "rune-peer-link-refusals node=" + std::to_string(nodeId); }
inline std::string securityApplyHaltedLabel(std::int32_t nodeId) { return "rune-security-apply-halted node=" + std::to_string(nodeId); }
inline std::string securityAppliedOffsetLabel(std::int32_t nodeId) { return "rune-security-applied-offset node=" + std::to_string(nodeId); }
inline std::string authModeLabel(std::int32_t nodeId) { return "rune-auth-mode node=" + std::to_string(nodeId); }
inline std::string storageGateStateLabel(std::int32_t nodeId) { return "rune-storage-gate-state node=" + std::to_string(nodeId); }
inline std::string leaderRolesHeldLabel(std::int32_t nodeId) { return "rune-leader-roles-held node=" + std::to_string(nodeId); }
inline std::string storagePendingNodesLabel(std::int32_t nodeId) { return "rune-storage-pending-nodes node=" + std::to_string(nodeId); }
inline std::string lostCommittedReportsLabel(std::int32_t nodeId) { return "rune-lost-committed-reports node=" + std::to_string(nodeId); }
inline std::string catalogAppliedOffsetLabel(std::int32_t nodeId) {
    return "rune-catalog-applied-offset node=" + std::to_string(nodeId);
}

inline std::string provisionMetricLabel(const char* what, std::int32_t nodeId) {
    return std::string("rune-segment-provision-") + what + " node=" + std::to_string(nodeId);
}

// Human-readable counter labels (also useful when browsing counters).
inline std::string roleCounterLabel(std::int32_t nodeId) {
    return "rune-cluster-node-role node=" + std::to_string(nodeId);
}
inline std::string termCounterLabel(std::int32_t nodeId) {
    return "rune-cluster-leadership-term node=" + std::to_string(nodeId);
}
inline std::string stuckMarkersLabel(std::int32_t nodeId) {
    return "rune-txn-stuck-markers node=" + std::to_string(nodeId);
}
inline std::string outOfSyncLabel(std::int32_t nodeId) {
    return "rune-node-out-of-sync-partitions node=" + std::to_string(nodeId);
}
inline std::string msmCommitPosLabel(std::int32_t nodeId) {
    return "rune-msm-commit-position node=" + std::to_string(nodeId);
}

} // namespace rune::node

#endif // RUNE_NODE_ROLE_COUNTER_H
