#pragma once
// ControlledShutdown.h — Kafka-parity controlled shutdown, node side.
//
// Problem (dockerized DR-15 SIGTERM, 2026-10-05): on SIGTERM a node exited
// still advertised as the leader of its partitions; failover took ~2 s
// (consensus re-election + partition failover) and meanwhile every broker
// redirected clients to the stopped node.
//
// Kafka (KRaft BrokerHeartbeat wantShutDown): a broker that is asked to stop
// first tells the controller, which moves every partition it leads to another
// in-sync replica and removes it from every ISR; the broker waits until that
// is done, then stops.  Rune does the same:
//
//   Running  ──begin()──▶ Draining ──(controller node, its user partitions
//      ▲                     │        moved)──▶ Resigning ──▶ Done
//   Registering              └──(nothing left here)──────────▶ Done
//
//   Registering — once per process: tell the controller "incarnation X, NOT
//     stopping" (AlterIsr channel, kNodeRegister) until acked: it clears a
//     shutdown mark an earlier incarnation of this node left (a fast restart
//     after a graceful stop must rejoin the ISRs).
//   Draining — the controller is asked (kNodeShutdown, re-sent until the
//     CURRENT controller acked it; again after every controller change) to
//     move this node's partition leadership and drop it from the ISRs.  The
//     consensus vote is kept, but this node never stands for election again.
//     Done when the node's own COMMITTED map shows it in no ISR except as the
//     leader of a partition it is the SOLE in-sync replica of (such a
//     partition cannot move without an unclean election: it stays here and
//     goes offline when the node stops — exactly as before, no data loss).
//   Resigning — this node is the controller (consensus leader): once its USER
//     partitions moved it resigns the consensus (Election::resign: StandDown
//     to every follower, which elect a successor at once).  Done when a
//     successor leads and the successor moved the coordinator partitions.
//
// Every transition is an EVENT (a committed metadata change, a consensus
// state change, a controller ack): no timer decides anything.  The re-send
// cadence (kResendNs, the AlterIsr cadence) only recovers a lost message.  The
// caller's overall budget (BrokerNode::controlledShutdown) is a last-resort
// cap that falls back to today's stop (logged).
//
// Threads: doWork()/onResponse() — CONTROL thread only (the runtime duty
// cycle; cold: the runtime's slow tick).  begin() — any thread (ring offer).
// observe() — the ONE observer thread (BrokerNode's start() thread).  Every
// cross-thread hop is a ControlRing (no shared state, no atomics).
#include <rune/consensus/ControlRing.h>
#include <rune/consensus/Election.h>
#include <rune/consensus/ElectionState.h>
#include <rune/metadata/AssignmentMap.h>
#include <rune/node/MetadataService.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/ReplicationMessages.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstdint>
#include <variant>
#include <vector>

namespace rune::node {

class ControlledShutdown {
public:
    enum class Phase : int32_t { Registering = 1, Running = 2, Draining = 3, Resigning = 4, Done = 5 };

    /// Observer view: the last published phase + what still blocks it.
    struct Status {
        Phase   phase     = Phase::Running;
        int32_t remaining = -1;   ///< partitions still led / ISRs still joined here (-1 = unknown)
    };

    /// AlterIsr loss-recovery cadence (PartitionReplica::kAlterIsrResendNs).
    static constexpr int64_t kResendNs = 50'000'000LL;

    ControlledShutdown() = default;
    ControlledShutdown(const ControlledShutdown&)            = delete;
    ControlledShutdown& operator=(const ControlledShutdown&) = delete;

    /// Startup thread, before the control thread runs.  `transport` null (a
    /// single node / no replication transport) = nothing to hand off: a begin
    /// completes at once.
    void bind(int32_t self, int64_t incarnation, rune::replication::AeronPartitionTransport* transport,
              MetadataService* metadata, rune::consensus::Election* election, std::vector<int32_t> members) {
        self_ = self; incarnation_ = incarnation; transport_ = transport;
        metadata_ = metadata; election_ = election; members_ = std::move(members);
        if (active_()) state_ = Registering{};
    }

    /// Startup-immutable after bind(): false = a single node / no replication
    /// transport — nothing to hand off.
    [[nodiscard]] bool active() const noexcept { return active_(); }

    // ------------------------------------------------------------- any thread
    /// Ask the control thread to start the hand-off.  false = ring full.
    [[nodiscard]] bool begin() noexcept { return cmd_.offer(kBeginMsg); }

    // --------------------------------------------------------- observer thread
    Status observe() noexcept {
        status_.poll([this](std::int32_t type, std::int64_t a, std::int64_t b) noexcept {
            if (type != kStatusMsg) return;
            observed_.phase     = static_cast<Phase>(a);
            observed_.remaining = static_cast<int32_t>(b);
        });
        return observed_;
    }

    // ---------------------------------------------------------- control thread
    /// One step (the runtime's slow tick).  Returns work done.
    int doWork(int64_t nowNs) {
        int work = 0;
        bool beginNow = false;
        work += cmd_.poll([&beginNow](std::int32_t type, std::int64_t, std::int64_t) noexcept {
            if (type == kBeginMsg) beginNow = true;
        });
        if (beginNow && !std::holds_alternative<Draining>(state_) &&
            !std::holds_alternative<Resigning>(state_) && !std::holds_alternative<Done>(state_)) {
            if (!active_()) {
                state_ = Done{};
            } else {
                state_ = Draining{nowNs};
                // From now on this node never stands for consensus leadership.
                if (election_) election_->withdrawCandidacy();
                spdlog::info("ControlledShutdown[{}]: begin — asking the controller to move this "
                             "node's partition leadership (incarnation {})", self_, incarnation_);
            }
            ++work;
        }
        std::visit([&](auto& st) { work += step_(st, nowNs); }, state_);
        publishStatus_();
        return work;
    }

    /// CONTROL thread: the controller's ack of a NODE-level request.
    void onResponse(const rune::replication::AlterIsrResponseMsg& r) noexcept {
        if (r.leaderId != self_ || incarnationOf_(r) != incarnation_) return;   // not ours / stale
        const bool    complete  = r.errorCode == rune::replication::kAlterIsrOk;
        const int32_t responder = r.partition;   // the answering controller
        std::visit([complete, responder](auto& st) {
            if constexpr (requires { st.req; }) {
                st.req.ackedBy    = responder;
                st.req.completeBy = complete ? responder : -2;
            }
        }, state_);
    }
    static void onResponseThunk(void* self, const rune::replication::AlterIsrResponseMsg& r) noexcept {
        static_cast<ControlledShutdown*>(self)->onResponse(r);
    }

    /// Control thread / tests: current phase.
    [[nodiscard]] Phase phaseOnControl() const noexcept {
        return std::visit([](const auto& st) { return std::decay_t<decltype(st)>::kPhase; }, state_);
    }

    /// Pure: what still keeps `self` from stopping in `map` — partitions in
    /// whose ISR it is, except as the leader of a partition it is the SOLE
    /// in-sync replica of (cannot move cleanly; stays).  `userOnly` restricts
    /// to user topics (topicId >= 0): the controller's coordinator partitions
    /// follow the consensus and move after it resigned.
    [[nodiscard]] static int32_t blocking(const rune::metadata::AssignmentMap& map, int32_t self,
                                          bool userOnly) noexcept {
        return MetadataService::controlledShutdownBlocking(map, self, userOnly);
    }

private:
    // One in-flight NODE-level request: re-sent until the CURRENT controller
    // acked it (a new controller holds no marks — it must hear it again).
    struct Request {
        int64_t lastSendNs = 0;
        int32_t sentTo     = -1;   ///< controller the last send was routed to
        int32_t ackedBy    = -2;   ///< controller that acked it (-2 = none)
        int32_t completeBy = -2;   ///< controller that answered COMPLETE (shutdown; -2 = none)
        bool    clearSent  = false;   ///< re-asked at once when this node's own map cleared
    };
    struct Registering { static constexpr Phase kPhase = Phase::Registering; Request req; };
    struct Running     { static constexpr Phase kPhase = Phase::Running; int32_t registeredWith = -1; };
    struct Draining {
        static constexpr Phase kPhase = Phase::Draining;
        explicit Draining(int64_t now = 0) noexcept : startedNs(now) {}
        int64_t startedNs;
        Request req;
        int32_t remaining = -1;
    };
    struct Resigning {
        static constexpr Phase kPhase = Phase::Resigning;
        int64_t startedNs = 0;
        Request req;
        int32_t remaining = -1;
    };
    struct Done { static constexpr Phase kPhase = Phase::Done; };
    using State = std::variant<Running, Registering, Draining, Resigning, Done>;

    static constexpr std::int32_t kBeginMsg  = 1;
    static constexpr std::int32_t kStatusMsg = 1;

    [[nodiscard]] bool active_() const noexcept {
        return transport_ != nullptr && metadata_ != nullptr && election_ != nullptr && self_ >= 0;
    }
    [[nodiscard]] int32_t controller_() const noexcept {
        return election_ ? static_cast<int32_t>(election_->leaderMemberId()) : -1;
    }
    [[nodiscard]] bool leadsConsensus_() const noexcept {
        return election_ && election_->isClosed() && election_->leaderMemberId() == self_;
    }
    static int64_t incarnationOf_(const rune::replication::AlterIsrResponseMsg& r) noexcept {
        return static_cast<int64_t>((static_cast<uint64_t>(static_cast<uint32_t>(r.partitionEpoch)) << 32) |
                                    static_cast<uint32_t>(r.leaderEpoch));
    }

    /// (Re-)send the request — to EVERY member (each records the mark; the
    /// controller answers) — until the CURRENT controller answered what we
    /// wait for (register: any ack; shutdown: COMPLETE): at once on a
    /// controller change or when `force`d (an event: this node's own map just
    /// cleared), else on the loss-recovery cadence.
    int send_(Request& q, int32_t flags, int64_t nowNs, bool force = false) {
        const int32_t ctl = controller_();
        if (ctl < 0) return 0;
        const bool shutdown = (flags & rune::replication::AlterIsrMsg::kNodeShutdown) != 0;
        if (shutdown ? q.completeBy == ctl : q.ackedBy == ctl) return 0;
        if (!force && q.sentTo == ctl && nowNs - q.lastSendNs < kResendNs) return 0;
        rune::replication::AlterIsrMsg m{};
        m.topicId   = rune::replication::AlterIsrMsg::kNodeControlTopicId;
        m.partition = 0;
        m.leaderId  = self_;
        m.leaderEpoch = 0;
        m.partitionEpoch = 0;
        m.isrCount  = rune::replication::AlterIsrMsg::kLeaderEpochBump;
        m.isr[1]    = flags;
        m.isr[3]    = static_cast<int32_t>(static_cast<uint32_t>(incarnation_));
        m.isr[4]    = static_cast<int32_t>(static_cast<uint32_t>(static_cast<uint64_t>(incarnation_) >> 32));
        q.lastSendNs = nowNs;   // before the sends: a local answer arrives synchronously
        q.sentTo     = ctl;
        for (int32_t n : members_) (void)transport_->sendNodeControl(n, m);   // ring full: re-sent later
        return 1;
    }
    /// The shutdown request again, at once, the first time this node's own
    /// committed map shows nothing left here (the controller's COMPLETE
    /// answer is what ends the wait).
    int askCompletion_(Request& q, int32_t remaining, int64_t nowNs, bool userOnly = false) {
        const bool force = remaining == 0 && !q.clearSent;
        if (force) q.clearSent = true;
        if (remaining != 0) q.clearSent = false;
        namespace rr = rune::replication;
        return send_(q, rr::AlterIsrMsg::kNodeShutdown | (userOnly ? rr::AlterIsrMsg::kNodeShutdownUserOnly : 0),
                     nowNs, force);
    }

    int step_(Running& st, int64_t) {
        // A new controller may hold a mark an earlier incarnation of this node
        // left (it was a member then): register again on every controller change.
        if (!active_()) return 0;
        const int32_t ctl = controller_();
        if (ctl >= 0 && ctl != st.registeredWith) { state_ = Registering{}; return 1; }
        return 0;
    }
    int step_(Done&, int64_t) { return 0; }
    int step_(Registering& st, int64_t nowNs) {
        if (st.req.ackedBy >= 0 && st.req.ackedBy == controller_()) {
            Running r;
            r.registeredWith = st.req.ackedBy;
            state_ = r;
            return 1;
        }
        return send_(st.req, rune::replication::AlterIsrMsg::kNodeRegister, nowNs);
    }
    int step_(Draining& st, int64_t nowNs) {
        const auto& map = metadata_->committedMap();
        const int32_t ctl = controller_();
        if (leadsConsensus_()) {
            // The controller: its user partitions first, then the consensus.
            st.remaining = blocking(map, self_, /*userOnly=*/true);
            const int work = askCompletion_(st.req, st.remaining, nowNs, /*userOnly=*/true);
            if (st.remaining == 0 && st.req.completeBy == self_ && election_->resign(nowNs)) {
                spdlog::info("ControlledShutdown[{}]: partitions handed off — resigning the consensus "
                             "leadership ({} ms)", self_, (nowNs - st.startedNs) / 1'000'000);
                Resigning next;
                next.startedNs = st.startedNs;
                next.req       = Request{};   // the successor must hear the request
                state_ = next;
                return work + 1;
            }
            return work;
        }
        st.remaining = blocking(map, self_, /*userOnly=*/false);
        const int work = askCompletion_(st.req, st.remaining, nowNs);
        if (st.remaining == 0 && ctl >= 0 && st.req.completeBy == ctl) {
            spdlog::info("ControlledShutdown[{}]: every movable partition is led elsewhere, this node left "
                         "every ISR and the controller confirmed every live member routes around it — "
                         "ready to stop ({} ms)", self_, (nowNs - st.startedNs) / 1'000'000);
            state_ = Done{};
            return work + 1;
        }
        return work;
    }
    int step_(Resigning& st, int64_t nowNs) {
        const int32_t ctl = controller_();
        st.remaining = blocking(metadata_->committedMap(), self_, /*userOnly=*/false);
        const int work = (ctl >= 0 && ctl != self_) ? askCompletion_(st.req, st.remaining, nowNs) : 0;
        if (ctl >= 0 && ctl != self_ && st.remaining == 0 && st.req.completeBy == ctl) {
            spdlog::info("ControlledShutdown[{}]: node {} leads the consensus, every movable partition is "
                         "led elsewhere and confirmed — ready to stop ({} ms)", self_, ctl,
                         (nowNs - st.startedNs) / 1'000'000);
            state_ = Done{};
            return work + 1;
        }
        return work;
    }

    void publishStatus_() noexcept {
        Status now;
        now.phase = phaseOnControl();
        std::visit([&now](const auto& st) {
            if constexpr (requires { st.remaining; }) now.remaining = st.remaining;
        }, state_);
        if (now.phase == published_.phase && now.remaining == published_.remaining) return;
        if (status_.offer(kStatusMsg, static_cast<std::int64_t>(now.phase), now.remaining)) published_ = now;
        // else: ring full — retried next tick with the then-current status.
    }

    int32_t                                     self_        = -1;
    int64_t                                     incarnation_ = 0;
    rune::replication::AeronPartitionTransport* transport_   = nullptr;
    MetadataService*                            metadata_    = nullptr;
    rune::consensus::Election*                  election_    = nullptr;
    std::vector<int32_t>                        members_;               ///< every cluster member (incl. self)

    State  state_{Running{}};                       // control thread
    Status published_{Phase::Running, -1};          // control thread
    rune::consensus::ControlRing<1024> cmd_;        // any → control
    rune::consensus::ControlRing<4096> status_;     // control → observer
    Status observed_{};                             // observer thread
};

} // namespace rune::node
