#pragma once
// PermissionTable.h - what an authenticated session may do (authentication plan D15-D23, spec §2 / §6).
//
// One per session, owned by the CONTROL thread with its Session (hard rule 1).  Compiled at login (cold) from the
// user's effective grants and the live ObjectIndex: one op bitmask per topic / view id, the cluster bits, and the
// retained grants for the name-addressed requests (groups, transactional ids, DDL names - cold).  A topic / view
// created after login extends the table (onObjectCreated), a drop clears the bits.
//
// Hot path (produce / subscribe / fetch): allows() = ONE bounds check + ONE load, no allocation.  The WRITE bit of
// every topic id is mirrored in writeBits_ (one bit per id) for Task 13, which ships it to the shard workers.
#include <rune/gateway/ObjectIndex.h>
#include <rune/security/Permissions.h>

#include <array>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace rune::gateway {

class PermissionTable {
public:
    /// Every operation on every object (dev / in-process anonymous sessions).
    [[nodiscard]] static PermissionTable allowAll() {
        PermissionTable t;
        t.allowAll_ = true;
        return t;
    }
    /// Nothing at all (a session that must not touch the data plane, e.g. a one-time-password session).
    [[nodiscard]] static PermissionTable denyAll() { return PermissionTable{}; }
    /// Cold.  `effective` = rune::security::effectiveGrants(state, userId); `nsId` = the session's namespace (plan D9:
    /// an object of another namespace gets no bits - unless the grants are root, ADMIN ON CLUSTER, issue #626).
    [[nodiscard]] static PermissionTable compile(std::span<const rune::security::GrantSpec> effective, uint32_t nsId,
                                                 const ObjectIndex&);
    /// The `default` namespace (nsId 0).
    [[nodiscard]] static PermissionTable compile(std::span<const rune::security::GrantSpec> effective, const ObjectIndex& index) {
        return compile(effective, 0, index);
    }

    [[nodiscard]] bool allows(int32_t objectId, uint8_t opBits) const noexcept {   // ONE bounds check + ONE load
        return allowAll_ || (static_cast<uint32_t>(objectId) < ops_.size() && (ops_[static_cast<std::size_t>(objectId)] & opBits) == opBits);
    }
    [[nodiscard]] bool allowsCluster(uint8_t opBits) const noexcept { return allowAll_ || (clusterOps_ & opBits) == opBits; }
    /// Name-addressed (groups, transactional ids, DDL names): the retained patterns matched against `name`.  Cold.
    [[nodiscard]] bool allowsName(rune::security::ResourceType, std::string_view name, uint8_t opBits) const noexcept;
    [[nodiscard]] bool isAllowAll() const noexcept { return allowAll_; }
    /// The namespace this table was compiled for (0 for allow-all / deny-all).
    [[nodiscard]] uint32_t nsId() const noexcept { return nsId_; }
    /// Root (ADMIN ON CLUSTER) or allow-all: reaches every namespace (spec §2, issue #626).
    [[nodiscard]] bool isRoot() const noexcept { return allowAll_ || root_; }
    /// #211: true when the table holds `op` on at least one resource of `type` - any literal, prefixed or wildcard
    /// grant ON that type (allow-all: always).  O(1): kept when the table is compiled (a grant change recompiles it).
    /// Only the type's own grants count, plus root: ADMIN ON CLUSTER holds every op on every type but NAMESPACE.  The
    /// other CLUSTER grants give no implication here, which is exact for READ / WRITE (only root holds them).
    [[nodiscard]] bool hasAnyGrant(rune::security::ResourceType type, rune::security::Operation op) const noexcept {
        const auto t = static_cast<std::size_t>(type);
        return allowAll_ || (t < anyOps_.size() && (anyOps_[t] & rune::security::opBit(op)) != 0);
    }
    /// The retained effective grants (sorted, unique) - what Task 14 diffs across a refresh.  Empty for allow-all / deny-all.
    [[nodiscard]] std::span<const rune::security::GrantSpec> grants() const noexcept { return grants_; }

    /// A topic / view created after login (spec §6): set its bits from the retained grants.  Cold; may grow ops_.
    void onObjectCreated(int32_t id, const ObjectEntry&);
    void onObjectDropped(int32_t id) noexcept;   // its bits cleared

    /// The WRITE bit per topic / view id (word i bit b = id 64*i+b), kept in step with ops_ (Task 13 ships it).
    [[nodiscard]] std::span<const uint64_t> writeBits() const noexcept { return writeBits_; }

private:
    void setOps_(int32_t id, uint8_t ops);
    [[nodiscard]] uint8_t opsOf_(const ObjectEntry& e) const noexcept;   // 0 on a foreign object unless root
    bool allowAll_ = false;
    bool root_ = false;
    uint32_t nsId_ = 0;
    uint8_t clusterOps_ = 0;
    std::array<uint8_t, 5> anyOps_{};                   // #211: op bits granted on ANY resource, per ResourceType
    std::vector<uint8_t> ops_;                          // by object id; capacity persists
    std::vector<rune::security::GrantSpec> grants_;     // retained for onObjectCreated / allowsName
    std::vector<uint64_t> writeBits_;                   // WRITE bit per id, kept in step with ops_
};

} // namespace rune::gateway
