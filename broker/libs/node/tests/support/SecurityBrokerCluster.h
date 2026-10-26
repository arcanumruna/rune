#pragma once
// SecurityBrokerCluster.h — authentication Task 9: the 3-broker harness (CatalogBrokerCluster.h's ThreeNodes) seen
// through `__security`: the committed (-7, 0) assignment, each broker's replica, the leader, valid events appended
// on the leader's admin thread, and what each broker's CONTROL-thread SecurityView holds.  Polls are bounded (the
// deadline is the failure, never a hang).
#include "support/CatalogBrokerCluster.h"

#include <rune/node/SecurityPartition.h>
#include <rune/security/SecurityEvent.h>
#include <rune/security/SecurityReplayer.h>
#include <rune/security/testing/SecurityGoldenScript.h>

namespace rune::node::test::security_cluster {

using namespace rune::node::test::catalog_cluster;

inline std::optional<rune::metadata::PartitionAssignment> securityAssignmentOf(BrokerNode& n) {
    return n.onControlThreadForTest([&n]() -> std::optional<rune::metadata::PartitionAssignment> {
        const MetadataService* ms = n.metadataServiceForTest();
        if (ms == nullptr) return std::nullopt;
        const auto* pa = ms->assignment(kSecurityTopicId, kSecurityPartition);
        return pa ? std::optional(*pa) : std::nullopt;
    });
}

inline CatalogView securityOf(BrokerNode& n) {
    return n.onAdminThreadForTest([&n]() -> CatalogView {
        CatalogView v;
        rr::PartitionReplica* r = n.securityReplicaForTest();
        if (r == nullptr) return v;
        v.present  = true;
        v.role     = r->role();
        v.leo      = r->logEndOffset();
        v.hw       = r->highWatermark();
        v.logStart = r->log()->logStartOffset();
        return v;
    });
}

/// The same committed security leader on every running broker, iff it is in the committed ISR and its admin-thread
/// replica is Leader; else -1.  `excluded` (a node id, 0 = none): a running but cut-off broker whose map cannot learn
/// the new assignment until it heals - it is left out of the agreement and never the answer.
inline int32_t settledSecurityLeader(ThreeNodes& c, int32_t excluded = 0) {
    int32_t leader = rune::metadata::NO_LEADER;
    for (BrokerNode* n : c.live()) {
        if (excluded != 0 && n == &c.node(excluded)) continue;
        const auto pa = securityAssignmentOf(*n);
        if (!pa || pa->leaderNode == rune::metadata::NO_LEADER) return -1;
        if (leader == rune::metadata::NO_LEADER) leader = pa->leaderNode;
        if (pa->leaderNode != leader || !contains(pa->isr, leader)) return -1;
    }
    if (leader < 1 || leader > 3 || leader == excluded || !c.running(leader)) return -1;
    return securityOf(c.node(leader)).role == rr::Role::Leader ? leader : -1;
}

inline bool awaitSecurityLeader(ThreeNodes& c, std::chrono::milliseconds within) {
    const auto deadline = std::chrono::steady_clock::now() + within;
    while (std::chrono::steady_clock::now() < deadline) {
        if (settledSecurityLeader(c) > 0) return true;
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

inline bool awaitInSecurityIsr(ThreeNodes& c, int32_t id, std::chrono::milliseconds within) {
    const auto deadline = std::chrono::steady_clock::now() + within;
    while (std::chrono::steady_clock::now() < deadline) {
        bool all = true;
        for (BrokerNode* n : c.live()) {
            const auto pa = securityAssignmentOf(*n);
            all = all && pa && contains(pa->isr, id);
        }
        if (all) return true;
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

/// Append `bytes` to `__security` on the current leader's admin thread (acks=all) and wait for the leader's HW to
/// cover it.  Returns that HW (> the record's offset), or -1 at the deadline.
inline int64_t appendSecurityBytes(ThreeNodes& c, std::vector<uint8_t> bytes, std::chrono::milliseconds within = 30s) {
    const auto deadline = std::chrono::steady_clock::now() + within;
    while (std::chrono::steady_clock::now() < deadline) {
        const int32_t leader = settledSecurityLeader(c);
        if (leader > 0) {
            BrokerNode& n = c.node(leader);
            const int64_t base = n.appendSecurityRecordOnLeaderForTest(std::span<const uint8_t>(bytes));
            if (base >= 0) {
                while (std::chrono::steady_clock::now() < deadline) {
                    const auto v = securityOf(n);
                    if (v.hw > base) return v.hw;
                    std::this_thread::sleep_for(10ms);
                }
                return -1;
            }
        }
        std::this_thread::sleep_for(20ms);
    }
    return -1;
}

/// A cluster-wide cursor over the golden security script: appendNext() appends the next valid event.
class GoldenAppender {
public:
    GoldenAppender() : events_(rune::security::testing::goldenScriptV1()) {}
    int64_t appendNext(ThreeNodes& c, std::chrono::milliseconds within = 30s) {
        auto bytes = rune::security::encode(events_.at(next_));
        if (!bytes) throw std::logic_error("GoldenAppender: a golden event does not encode");
        const int64_t hw = appendSecurityBytes(c, std::move(*bytes), within);
        if (hw >= 0) ++next_;
        return hw;
    }
    [[nodiscard]] std::size_t appended() const noexcept { return next_; }
    /// The library's digest of the first `n` golden events (what every broker's view must reach).
    [[nodiscard]] uint64_t digestOfFirst(std::size_t n) const {
        rune::security::SecurityReplayer r;
        for (std::size_t i = 0; i < n; ++i) {
            auto b = rune::security::encode(events_.at(i));
            if (!b || !r.feed(static_cast<int64_t>(i), *b)) throw std::logic_error("golden prefix does not replay");
        }
        return r.state().digest();
    }
private:
    std::vector<rune::security::SecurityEvent> events_;
    std::size_t next_ = 0;
};

/// Every running broker's SecurityView reaches `offset` within `within`.
inline bool awaitViewsAt(ThreeNodes& c, int64_t offset, std::chrono::milliseconds within) {
    const auto deadline = std::chrono::steady_clock::now() + within;
    while (std::chrono::steady_clock::now() < deadline) {
        bool all = true;
        for (BrokerNode* n : c.live()) all = all && n->securityViewForTest().appliedOffset >= offset;
        if (all) return true;
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

inline bool awaitSecurityEndOffset(BrokerNode& n, int64_t want, std::chrono::milliseconds within) {
    const auto deadline = std::chrono::steady_clock::now() + within;
    while (std::chrono::steady_clock::now() < deadline) {
        if (securityOf(n).leo >= want) return true;
        std::this_thread::sleep_for(10ms);
    }
    return false;
}

} // namespace rune::node::test::security_cluster
