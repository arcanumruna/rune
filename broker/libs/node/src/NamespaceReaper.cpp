// NamespaceReaper.cpp - see the header.
#include <rune/node/NamespaceReaper.h>

#include <spdlog/spdlog.h>

#include <optional>

namespace rune::node {

namespace cat = rune::catalog;

namespace {

/// The namespace a state works on (nullopt: Idle).
std::optional<uint32_t> targetOf(const ReapState& s) {
    return std::visit([](const auto& x) -> std::optional<uint32_t> {
        if constexpr (std::is_same_v<std::decay_t<decltype(x)>, ReapIdle>) return std::nullopt;
        else return x.nsId;
    }, s);
}

}  // namespace

std::string_view NamespaceReaper::stateName() const noexcept {
    switch (state_.index()) {
    case 0: return "Idle";
    case 1: return "AbortingTxns";
    case 2: return "DroppingViews";
    case 3: return "DroppingTopics";
    case 4: return "Tombstoning";
    case 5: return "RecordingStep";
    default: return "AwaitRemoval";
    }
}

ReapState NamespaceReaper::derive_(const cat::NamespaceRecord& rec) const {
    const auto& d = std::get<cat::NsDeleting>(rec.state);
    const uint32_t id = rec.nsId;
    if ((d.stepMask & cat::TxnsAborted) == 0) return ReapAbortingTxns{id, 0};
    if (!d.pendingViews.empty()) return ReapDroppingViews{id, 0};
    if (!d.pendingTopics.empty()) return ReapDroppingTopics{id, 0};
    if ((d.stepMask & cat::GroupsTombstoned) == 0) return ReapTombstoning{id, cat::GroupsTombstoned, 0};
    if ((d.stepMask & cat::TxnStateTombstoned) == 0) return ReapTombstoning{id, cat::TxnStateTombstoned, 0};
    return ReapAwaitRemoval{id};   // only SecurityPurged is left (an entry with every bit and no object is already erased)
}

int NamespaceReaper::doWork(int64_t /*nowNs*/, const cat::CatalogState& cs, ReaperPort& port) {
    const cat::NamespaceRegistry& reg = cs.namespaces();
    int work = 0;
    // The target left DELETING (erased by the apply of its last event): whatever was in flight is moot.
    if (const auto id = targetOf(state_)) {
        const cat::NamespaceRecord* rec = reg.byId(*id);
        if (rec == nullptr || !std::holds_alternative<cat::NsDeleting>(rec->state)) {
            state_ = ReapIdle{};
            ++work;
        }
    }
    if (std::holds_alternative<ReapIdle>(state_)) {
        const cat::NamespaceRecord* target = nullptr;
        reg.forEach([&](const cat::NamespaceRecord& rec) {   // nsId order: the lowest DELETING one
            if (target == nullptr && rec.nsId != 0 && std::holds_alternative<cat::NsDeleting>(rec.state)) target = &rec;
        });
        if (target == nullptr) return work;
        state_ = derive_(*target);
        ++work;
    }
    return work + step_(cs, port);
}

int NamespaceReaper::step_(const cat::CatalogState& cs, ReaperPort& port) {
    const cat::NamespaceRegistry& reg = cs.namespaces();
    return std::visit([&](auto& st) -> int {
        using T = std::decay_t<decltype(st)>;
        if constexpr (std::is_same_v<T, ReapIdle>) {
            return 0;
        } else {
            const cat::NamespaceRecord* rec = reg.byId(st.nsId);
            const auto* d = rec != nullptr ? std::get_if<cat::NsDeleting>(&rec->state) : nullptr;
            if (d == nullptr) { state_ = ReapIdle{}; return 1; }
            if constexpr (std::is_same_v<T, ReapAbortingTxns>) {
                if ((d->stepMask & cat::TxnsAborted) != 0) { state_ = ReapIdle{}; return 1; }   // recorded meanwhile
                if (st.inFlightCorrelation != 0) return 0;
                const uint64_t c = nextCorrelation_++;
                if (!port.askControl(c, ReapOp::AbortTxns, st.nsId, 0)) return 0;   // retried next pass
                st.inFlightCorrelation = c;
                return 1;
            } else if constexpr (std::is_same_v<T, ReapDroppingViews> || std::is_same_v<T, ReapDroppingTopics>) {
                constexpr bool views = std::is_same_v<T, ReapDroppingViews>;
                const auto& pending = views ? d->pendingViews : d->pendingTopics;
                if (pending.empty()) { state_ = ReapIdle{}; return 1; }   // the last drop applied: re-derive
                if (st.inFlightCorrelation != 0) return 0;
                const uint64_t objectId = *pending.begin();               // lowest objectId first: deterministic
                const std::string* name = nullptr;
                if constexpr (views) { if (const auto* v = cs.viewByObjectId(objectId)) name = &v->name; }
                else                 { if (const auto* t = cs.topicByObjectId(objectId)) name = &t->name; }
                if (name == nullptr) {   // a pending object the catalog does not hold: an invariant breach, never a spin
                    spdlog::error("NamespaceReaper: namespace {} lists {} object {} as pending but the catalog holds none",
                                  st.nsId, views ? "view" : "topic", objectId);
                    return 0;
                }
                if (budget_ == 0) return 0;
                const uint64_t c = nextCorrelation_++;
                if (!port.submitDrop(c, st.nsId, views, *name)) return 0;
                (void)takeBudget_();
                st.inFlightCorrelation = c;
                return 1;
            } else if constexpr (std::is_same_v<T, ReapTombstoning>) {
                if ((d->stepMask & st.step) != 0) { state_ = ReapIdle{}; return 1; }
                if (st.inFlightCorrelation != 0) return 0;
                const uint64_t c = nextCorrelation_++;
                const ReapOp op = st.step == cat::GroupsTombstoned ? ReapOp::TombstoneGroups : ReapOp::TombstoneTxnState;
                if (!port.askControl(c, op, st.nsId, batch_)) return 0;
                st.inFlightCorrelation = c;
                return 1;
            } else if constexpr (std::is_same_v<T, ReapRecordingStep>) {
                if ((d->stepMask & st.step) != 0) { state_ = ReapIdle{}; return 1; }   // applied
                if (st.inFlightCorrelation != 0) return 0;
                if (budget_ == 0) return 0;
                const uint64_t c = nextCorrelation_++;
                if (!port.submitDeleteStep(c, st.nsId, st.step)) return 0;
                (void)takeBudget_();
                st.inFlightCorrelation = c;
                return 1;
            } else {   // ReapAwaitRemoval
                if ((d->stepMask & cat::SecurityPurged) != 0) return 0;   // every bit is in: the apply of the last event erases it
                if (!port.securityClean(st.nsId, rec->name)) return 0;     // the __security leader's purge has not applied here yet
                state_ = ReapRecordingStep{st.nsId, cat::SecurityPurged, 0};
                return 1;
            }
        }
    }, state_);
}

void NamespaceReaper::onDdlResult(uint64_t correlation, uint16_t /*code*/) noexcept {
    if (correlation == 0) return;
    std::visit([&](auto& st) {
        using T = std::decay_t<decltype(st)>;
        if constexpr (std::is_same_v<T, ReapDroppingViews> || std::is_same_v<T, ReapDroppingTopics> ||
                      std::is_same_v<T, ReapRecordingStep>) {
            if (st.inFlightCorrelation == correlation) st.inFlightCorrelation = 0;   // the registry says what is left
        }
    }, state_);
}

void NamespaceReaper::onControlResult(uint64_t correlation, ReapStatus status) noexcept {
    if (correlation == 0) return;
    if (auto* a = std::get_if<ReapAbortingTxns>(&state_); a != nullptr && a->inFlightCorrelation == correlation) {
        if (status == ReapStatus::Done) state_ = ReapRecordingStep{a->nsId, cat::TxnsAborted, 0};
        else a->inFlightCorrelation = 0;   // More / Refused: ask again on the next pass
        return;
    }
    if (auto* t = std::get_if<ReapTombstoning>(&state_); t != nullptr && t->inFlightCorrelation == correlation) {
        if (status == ReapStatus::Done) state_ = ReapRecordingStep{t->nsId, t->step, 0};
        else t->inFlightCorrelation = 0;   // More: the next batch; Refused: the same call again
    }
}

}  // namespace rune::node
