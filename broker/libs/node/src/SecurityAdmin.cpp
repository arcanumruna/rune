// SecurityAdmin.cpp — the `__security` leadership on the admin thread (authentication plan D39).  See SecurityAdmin.h.
#include <rune/node/SecurityAdmin.h>

#include <rune/replication/PartitionReplica.h>

#include <spdlog/spdlog.h>

#include <utility>
#include <vector>

namespace rune::node {

namespace {
int32_t epochOf(const SecurityRole& r) noexcept {
    return std::visit([](const auto& st) { return st.epoch; }, r);
}
} // namespace

SecurityAdmin::SecurityAdmin(SecurityApplier& applier, rune::replication::PartitionReplica* replica, int32_t selfId)
    : applier_(applier), replica_(replica), selfId_(selfId) {}

std::string_view SecurityAdmin::roleName() const noexcept {
    switch (role_.index()) {
    case 0:  return "Follower";
    case 1:  return "LeaderCatchingUp";
    case 2:  return "LeaderServing";
    default: return "Halted";
    }
}

void SecurityAdmin::setRole_(SecurityRole next) {
    const bool changed = next.index() != role_.index() || epochOf(next) != epochOf(role_);
    role_ = std::move(next);
    if (changed) spdlog::info("AdminWorker: __security role -> {} (epoch {})", roleName(), epochOf(role_));
}

bool SecurityAdmin::caughtUp_() const noexcept {
    // Every record the leader holds — a previous epoch's tail included — is committed (HW = min LEO over the ISR)
    // AND applied here.
    const int64_t hw = replica_->highWatermark();
    return !applier_.halted() && hw == replica_->logEndOffset() && applier_.nextOffset() == hw;
}

// The role is absolute and epoch-stamped (the committed assignment); the replica's own role machine handles
// re-asserts and refuses a leadership at an epoch it already led at (restart fence → asks the controller for a bump).
void SecurityAdmin::onRole(const AdminRoleMsg& role) {
    if (replica_ == nullptr) return;
    namespace rr = rune::replication;
    if (role.leader) {
        std::vector<rr::NodeId> isr;
        isr.reserve(static_cast<std::size_t>(role.isrCount));
        for (int i = 0; i < role.isrCount; ++i)
            isr.push_back(static_cast<rr::NodeId>(role.isr[static_cast<std::size_t>(i)]));
        replica_->becomeLeader(static_cast<rr::Epoch>(role.epoch), std::move(isr), role.partitionEpoch);
        if (replica_->role() == rr::Role::Leader) {
            // A halted broker stays Halted (its replica still leads until the controller moves the leadership;
            // it accepts no security change).
            if (std::holds_alternative<SecurityHalted>(role_)) return;
            // A promotion (or a leadership at a new epoch) starts catching up — the gate opens in updateRole_ once
            // the whole log is committed and applied; a same-epoch re-assert (an ISR update) keeps serving.
            const auto* s = std::get_if<SecurityLeaderServing>(&role_);
            if (s == nullptr || s->epoch != role.epoch) setRole_(SecurityLeaderCatchingUp{role.epoch});
            return;
        }
        demote_(-1, role.epoch);   // the replica refused the leadership (restart fence)
        return;
    }
    replica_->becomeFollower(static_cast<rr::Epoch>(role.epoch), static_cast<rr::NodeId>(role.leaderId));
    demote_(role.leaderId, role.epoch);
}

void SecurityAdmin::onHoldLeadership(const AdminRoleMsg& role) {
    if (replica_ != nullptr) replica_->holdLeadershipForStorage(role.epoch, role.partitionEpoch);
    demote_(-1, role.epoch);
}

void SecurityAdmin::demote_(int32_t leaderId, int32_t epoch) {
    if (!std::holds_alternative<SecurityHalted>(role_)) setRole_(SecurityFollower{leaderId, epoch});
}

void SecurityAdmin::onAlterIsrResponse(const rune::replication::AlterIsrResponseMsg& r) {
    if (replica_ == nullptr) return;
    replica_->onAlterIsrResponse(r);
}

// Follower ⇄ CatchingUp ⇄ Serving, from the replica's role and what the applier has applied: a leader serves
// security DDL only once every event in its log is applied (so the state it validates against is the state its
// append will extend).
bool SecurityAdmin::updateRole_() {
    if (replica_ == nullptr) return false;
    namespace rr = rune::replication;
    const auto epoch = static_cast<int32_t>(replica_->epoch());
    if (applier_.halted()) {   // terminal: every security change answers Internal from now on
        if (std::holds_alternative<SecurityHalted>(role_)) return false;
        const rune::security::ApplyError* e = applier_.haltError();
        setRole_(SecurityHalted{e != nullptr ? e->offset : -1, epoch});
        return true;
    }
    const bool leads = replica_->role() == rr::Role::Leader;
    if (!leads) {
        if (!std::holds_alternative<SecurityFollower>(role_)) {
            demote_(-1, epoch);
            return true;
        }
        return false;
    }
    const bool caughtUp = caughtUp_();
    if (const auto* c = std::get_if<SecurityLeaderCatchingUp>(&role_); c && caughtUp) {
        spdlog::info("AdminWorker: security leader (epoch {}) caught up at offset {} — serving security DDL",
                     c->epoch, applier_.nextOffset());
        setRole_(SecurityLeaderServing{c->epoch});
        return true;
    }
    if (const auto* s = std::get_if<SecurityLeaderServing>(&role_); s && !caughtUp) {
        // An appended event this broker has not applied: no new event may be built on a state that does not
        // include it.
        setRole_(SecurityLeaderCatchingUp{s->epoch});
        return true;
    }
    if (std::holds_alternative<SecurityFollower>(role_)) {
        setRole_(SecurityLeaderCatchingUp{epoch});
        return true;
    }
    return false;
}

int SecurityAdmin::doWork(int64_t /*nowNs*/) {
    return updateRole_() ? 1 : 0;
}

} // namespace rune::node
