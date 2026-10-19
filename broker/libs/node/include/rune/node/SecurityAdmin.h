#pragma once
// SecurityAdmin.h — the `__security` leadership and write path on the admin thread (authentication spec §3, plan
// D36/D39).  The AdminWorker's counterpart for `__security`: it owns neither the replica nor the applier (the
// AdminWorker does; it drives both logs' replicas and appliers each pass) but reads them: the leadership is one
// std::variant (hard rule 3), each state its own variables —
//   SecurityFollower        — not the `__security` leader (leaderId -1 = leaderless);
//   SecurityLeaderCatchingUp— the replica leads but not every record of its log is both COMMITTED and APPLIED here
//                             (a fresh promotion, a previous epoch's tail, a commit that timed out);
//   SecurityLeaderServing   — leads, HW == LEO and the applier applied up to the HW: security DDL is served;
//   SecurityHalted          — this broker's SecurityApplier halted at `offset` (D28): terminal for the process.
// Every transition logs "AdminWorker: __security role -> <roleName()> (epoch N)" at info.
//
// Task 9 ships the role machine; Task 10 adds the write path (submit / in-flight / dedupe / answers).

#include <rune/node/AdminChannels.h>
#include <rune/node/SecurityApplier.h>

#include <cstdint>
#include <string_view>
#include <variant>

namespace rune::replication { class PartitionReplica; struct AlterIsrResponseMsg; }

namespace rune::node {

struct SecurityFollower        { int32_t leaderId = -1; int32_t epoch = 0; };
struct SecurityLeaderCatchingUp { int32_t epoch = 0; };
struct SecurityLeaderServing   { int32_t epoch = 0; };
struct SecurityHalted          { int64_t offset = -1; int32_t epoch = 0; };
using SecurityRole = std::variant<SecurityFollower, SecurityLeaderCatchingUp, SecurityLeaderServing, SecurityHalted>;

class SecurityAdmin {
public:
    SecurityAdmin(SecurityApplier& applier, rune::replication::PartitionReplica* replica, int32_t selfId);

    /// The replica's role (multi-node: the committed (-7, 0) assignment; single node: the consensus leader).
    void onRole(const AdminRoleMsg& role);
    /// ISR v3 Step 10: the committed `__security` leadership is HELD (storage unconfirmed): never lead; ask the
    /// controller for a failover (the restart-fence bump) and serve as a follower of nobody.
    void onHoldLeadership(const AdminRoleMsg& role);
    /// control → admin: the controller's AlterIsrResponse for (-7, 0), verbatim.
    void onAlterIsrResponse(const rune::replication::AlterIsrResponseMsg& r);

    /// One pass: derive Follower ⇄ CatchingUp ⇄ Serving ⇄ Halted from the replica and the applier.
    int doWork(int64_t nowNs);

    [[nodiscard]] const SecurityRole& role() const noexcept { return role_; }
    /// "Follower" | "LeaderCatchingUp" | "LeaderServing" | "Halted".
    [[nodiscard]] std::string_view roleName() const noexcept;
    [[nodiscard]] bool isLeader() const noexcept { return std::holds_alternative<SecurityLeaderServing>(role_); }
    [[nodiscard]] rune::replication::PartitionReplica* replica() const noexcept { return replica_; }

private:
    bool updateRole_();
    void demote_(int32_t leaderId, int32_t epoch);
    void setRole_(SecurityRole next);
    [[nodiscard]] bool caughtUp_() const noexcept;

    SecurityApplier&                     applier_;
    rune::replication::PartitionReplica* replica_;
    int32_t                              selfId_;
    SecurityRole                         role_{SecurityFollower{}};
};

} // namespace rune::node
