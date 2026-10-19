#pragma once
// StorageGate.h — the node-local storage gate (ISR design v3, section 3.6 / invariant I-S1).
//
// From BrokerNode construction until its storage is CONFIRMED, a broker routes no leader role, neither stands in
// the consensus nor grants a vote.  Confirmed means: the controller's committed storage for this node equals the
// node's own id, or nothing is committed (0) and the origin is Founder or Legacy (trust on first use).
//
// One state variant, each state holding only what it needs (hard rule 3):
//   Undecided   { id, mode, lastLogNs }     - a multi-node storage formatted `--new`/`auto`; decides at runtime
//   Unconfirmed { id, origin, lastLogNs }   - a Joiner (or a Founder/Legacy whose storage the controller disowned)
//   Confirmed   { id, origin }
//
//   From        Condition                                         To          Change
//   Undecided   history (anyHistory || maxPeerTerm > 0)           Unconfirmed joined      (persist `joiner`, R-W3)
//   Undecided   !history && fresh quorum held for the dwell    Confirmed   founded     (persist `founder`)
//               (heardFresh + 1 >= members/2 + 1; the timer resets when the quorum is lost)
//   Unconfirmed c == id, or (c == 0 && origin Founder/Legacy)     Confirmed   confirmed
//   Confirmed   c != 0 && c != id                                 Unconfirmed unconfirmed
//   where c = msm.nodeStorage(self).
//
// Control thread only (owned by BrokerNode, evaluated by NodeRuntimeAgent right after the consensus step; the
// bridge reads open()).  The origin transition is applied only AFTER the storage.meta fsync returned
// (StorageIdentity::persistOrigin, inline: cold, a once-per-lifetime event).  No allocation after construction
// except in the cold log paths.

#include <rune/metadata/MetadataStateMachine.h>
#include <rune/node/StorageIdentity.h>

#include <spdlog/spdlog.h>

#include <cstdint>
#include <string>
#include <variant>

namespace rune::node {

/// What the election and the controller log say about the cluster this node is joining (ISR v3 R12).
/// Aggregate-initialised in this order by the tests.
struct Evidence {
    int     heardFresh      = 0;    ///< peers whose canvass said "valid flags, no history"
    int     members         = 0;    ///< the cluster member count
    bool    anyHistory      = false;
    int64_t maxPeerTerm     = 0;    ///< the highest leadership term a peer reported
    int32_t peerWithHistory = -1;
    // Diagnostics for the UNCONFIRMED log line only (never a decision input).
    int     peersHeard      = 0;
    int32_t controllerId    = -1;
    int64_t controllerTerm  = 0;
    uint64_t heldLeaderRoles = 0;
    uint32_t memberMask = 0;       ///< the configured member ids below 32
    uint32_t heardFreshMask = 0;   ///< members whose canvass said fresh (founding diagnostics: the unheard ones are logged)
};

class StorageGate {
public:
    using Origin = StorageIdentity::Origin;
    using Mode   = StorageIdentity::Mode;
    enum class Change : uint8_t { none, founded, joined, confirmed, unconfirmed };

    /// `storage` is BrokerNode's one Loaded object: persistOrigin()/markPresent() update the instance the gate and
    /// the mayBootstrap thunk read.
    /// `shortDwellNs`: founding when EVERY member said "no history" (nobody can hold history).  `longDwellNs`: founding on a
    /// plain majority - it must outlast the transport's own reconnect time to a restarted peer (CP2 C1-R).  No default:
    /// every construction names both.
    StorageGate(StorageIdentity::Loaded& storage, const std::string& dataDir, int32_t selfId, int members,
                int64_t shortDwellNs, int64_t longDwellNs)
        : storage_(storage), dataDir_(dataDir), self_(selfId), members_(members), shortDwellNs_(shortDwellNs),
          longDwellNs_(longDwellNs) {
        switch (storage.origin) {
            case Origin::Undecided: state_ = Undecided{storage.id, storage.mode, 0, kNoQuorumYet}; break;
            case Origin::Joiner:    state_ = Unconfirmed{storage.id, Origin::Joiner, 0}; break;
            case Origin::Founder:
            case Origin::Legacy:    state_ = Confirmed{storage.id, storage.origin}; break;
        }
    }
    StorageGate(const StorageGate&)            = delete;
    StorageGate& operator=(const StorageGate&) = delete;

    /// One evaluation (every control cycle).  Returns the transition taken this call.
    Change evaluate(const rune::metadata::MetadataStateMachine& msm, const Evidence& ev, int64_t nowNs) {
        const int64_t c = msm.nodeStorage(self_);
        if (auto* u = std::get_if<Undecided>(&state_)) {
            const bool history = ev.anyHistory || ev.maxPeerTerm > 0;
            if (history) {
                if (!StorageIdentity::persistOrigin(dataDir_, storage_, Origin::Joiner)) return Change::none;   // retried
                spdlog::warn("storage of node {} was {}, but peer {} reports an established cluster (term {}):\n"
                             "joining it as a new disk (origin=joiner; it never voted or led). If RUNE_PEERS should "
                             "name a different cluster,\nstop this node now.",
                             self_, u->mode == Mode::Auto ? "auto-formatted" : "formatted for a NEW cluster",
                             ev.peerWithHistory, ev.maxPeerTerm);
                state_ = Unconfirmed{storage_.id, Origin::Joiner, 0};
                return Change::joined;
            }
            if (ev.heardFresh + 1 >= members_majority_(ev)) {
                // C1: a fresh quorum must PERSIST for a full dwell, so every live member that holds history has
                // messaged us (each sends at least once per status/heartbeat interval) before we may found.
                if (u->freshQuorumSinceNs == kNoQuorumYet) u->freshQuorumSinceNs = nowNs;
                const bool unanimous = ev.heardFresh + 1 >= members_for_(ev);
                if (nowNs - u->freshQuorumSinceNs < (unanimous ? shortDwellNs_ : longDwellNs_)) return Change::none;
                if (!StorageIdentity::persistOrigin(dataDir_, storage_, Origin::Founder)) return Change::none;
                if (!unanimous) {
                    std::string unheard;
                    for (int m = 0; m < 32; ++m)
                        if (m != self_ && (ev.memberMask & (1u << m)) != 0 && (ev.heardFreshMask & (1u << m)) == 0) unheard += (unheard.empty() ? "" : ",") + std::to_string(m);
                    spdlog::warn("storage of node {} founded without hearing members {{{}}}: if one of them holds this "
                                 "cluster's history, stop this node now", self_, unheard);
                }
                spdlog::info("storage CONFIRMED (id={}): founded a new cluster (heard {} of {} members fresh)",
                             StorageIdentity::idHex(storage_.id), ev.heardFresh, members_for_(ev));
                state_ = Confirmed{storage_.id, Origin::Founder};
                return Change::founded;
            }
            else u->freshQuorumSinceNs = kNoQuorumYet;
            if (nowNs - u->lastLogNs >= kLogEveryNs || u->lastLogNs == 0) {
                u->lastLogNs = nowNs == 0 ? 1 : nowNs;
                spdlog::warn("storage UNDECIDED (id={}, mode={}): heard {} of {} members at term 0; founding needs {}. "
                             "No vote, no candidacy, no leader role.",
                             StorageIdentity::idHex(u->id), u->mode == Mode::Auto ? "auto" : "new", ev.heardFresh,
                             members_for_(ev), members_majority_(ev) - 1);
            }
            return Change::none;
        }
        if (auto* u = std::get_if<Unconfirmed>(&state_)) {
            if (c == u->id || (c == 0 && (u->origin == Origin::Founder || u->origin == Origin::Legacy))) {
                spdlog::info("storage CONFIRMED (id={})", StorageIdentity::idHex(u->id));
                state_ = Confirmed{u->id, u->origin};
                return Change::confirmed;
            }
            logUnconfirmed_(*u, c, msm.featureLevel(), ev, nowNs);
            return Change::none;
        }
        const auto& k = std::get<Confirmed>(state_);
        if (c != 0 && c != k.id) {
            Unconfirmed u{k.id, k.origin, 0};
            state_ = u;
            logUnconfirmed_(std::get<Unconfirmed>(state_), c, msm.featureLevel(), ev, nowNs);
            return Change::unconfirmed;
        }
        return Change::none;
    }

    [[nodiscard]] int64_t longDwellNs() const noexcept { return longDwellNs_; }
    [[nodiscard]] bool open()    const noexcept { return std::holds_alternative<Confirmed>(state_); }
    [[nodiscard]] bool mayVote() const noexcept { return open(); }
    [[nodiscard]] bool mayStand() const noexcept { return open(); }
    [[nodiscard]] int64_t id() const noexcept {
        return std::visit([](const auto& s) { return s.id; }, state_);
    }
    /// AlterIsrMsg::kStorageOrigin*: Founder 1, Legacy 2, Joiner 3, Undecided 4.
    [[nodiscard]] uint8_t originWireCode() const noexcept {
        if (std::holds_alternative<Undecided>(state_)) return 4;
        const Origin o = std::holds_alternative<Confirmed>(state_) ? std::get<Confirmed>(state_).origin
                                                                   : std::get<Unconfirmed>(state_).origin;
        switch (o) { case Origin::Founder: return 1; case Origin::Legacy: return 2; case Origin::Joiner: return 3; default: return 4; }
    }
    /// 0 CONFIRMED, 1 UNCONFIRMED, 2 UNDECIDED (kStorageGateStateCounterTypeId).
    [[nodiscard]] int stateCode() const noexcept {
        return std::holds_alternative<Confirmed>(state_) ? 0 : std::holds_alternative<Unconfirmed>(state_) ? 1 : 2;
    }
    [[nodiscard]] const char* stateName() const noexcept {
        return stateCode() == 0 ? "CONFIRMED" : stateCode() == 1 ? "UNCONFIRMED" : "UNDECIDED";
    }

private:
    struct Undecided   { int64_t id; Mode mode; int64_t lastLogNs; int64_t freshQuorumSinceNs; };
    static constexpr int64_t kNoQuorumYet = INT64_MIN;
    struct Unconfirmed { int64_t id; Origin origin; int64_t lastLogNs; };
    struct Confirmed   { int64_t id; Origin origin; };
    static constexpr int64_t kLogEveryNs = 30'000'000'000LL;

    [[nodiscard]] int members_for_(const Evidence& ev) const noexcept { return ev.members > 0 ? ev.members : members_; }
    [[nodiscard]] int members_majority_(const Evidence& ev) const noexcept { return members_for_(ev) / 2 + 1; }

    void logUnconfirmed_(Unconfirmed& u, int64_t committed, int16_t level, const Evidence& ev, int64_t nowNs) {
        if (u.lastLogNs != 0 && nowNs - u.lastLogNs < kLogEveryNs) return;   // on entry, then every 30 s
        u.lastLogNs = nowNs == 0 ? 1 : nowNs;
        std::string s = "storage UNCONFIRMED (id=" + StorageIdentity::idHex(u.id) + ", origin=" +
                        StorageIdentity::originName(u.origin) + ", committed=" +
                        (committed == 0 ? std::string("none") : StorageIdentity::idHex(committed)) + "): holding " +
                        std::to_string(ev.heldLeaderRoles) + " leader roles, not voting, not standing until the "
                        "controller confirms this storage. controller=" +
                        (ev.controllerId < 0 ? std::string("none") : std::to_string(ev.controllerId)) + " term=" +
                        std::to_string(ev.controllerTerm) + " featureLevel=" + std::to_string(level);
        if (level < 2) s += "; waiting for every member to upgrade (feature level < 2)";
        if (ev.maxPeerTerm == 0)
            s += "; no leader heard (peers at term 0): if this is a NEW cluster, every node was formatted --join - "
                 "wipe and format --new";
        const int q = members_majority_(ev) - ev.peersHeard;   // A-M3: voters still missing for a quorum
        if (q > 0) s += "; a quorum needs " + std::to_string(q) + " more voters";
        spdlog::critical("{}", s);
    }

    StorageIdentity::Loaded&                          storage_;
    std::string                                       dataDir_;
    int32_t                                           self_;
    int                                               members_;
    int64_t                                           shortDwellNs_;
    int64_t                                           longDwellNs_;
    std::variant<Undecided, Unconfirmed, Confirmed>   state_{Confirmed{0, Origin::Founder}};
};

} // namespace rune::node
