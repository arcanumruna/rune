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
#include <chrono>
#include <random>
#include <type_traits>
#include <variant>
#include <vector>

namespace rune::node {

class ControlledShutdown {
public:
    enum class Phase : int32_t { Registering = 1, Running = 2, Draining = 3, Resigning = 4, Done = 5, Halted = 6 };

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
    using SendFn = bool (*)(void* ctx, int32_t to, const rune::replication::AlterIsrMsg& m) noexcept;
    void bind(int32_t self, int64_t incarnation, SendFn send, void* sendCtx,
              MetadataService* metadata, rune::consensus::Election* election, std::vector<int32_t> members) {
        self_ = self; incarnation_ = incarnation; send_fn_ = send; sendCtx_ = sendCtx;
        metadata_ = metadata; election_ = election; members_ = std::move(members);
        if (active_()) state_ = Registering{};
    }
    /// Pure — the rolling-restart readiness gate: partitions `self`
    /// replicates but is NOT in the committed ISR of (0 = fully in sync).
    [[nodiscard]] static int32_t outOfSync(const rune::metadata::AssignmentMap& map, int32_t self) noexcept {
        int32_t n = 0;
        map.forEach([&](int32_t, int32_t, const rune::metadata::PartitionAssignment& pa) {
            if (std::find(pa.replicas.begin(), pa.replicas.end(), self) == pa.replicas.end()) return;
            if (std::find(pa.isr.begin(), pa.isr.end(), self) == pa.isr.end()) ++n;
        });
        return n;
    }

    /// Startup-immutable after bind(): false = a single node / no replication
    /// transport — nothing to hand off.
    [[nodiscard]] bool active() const noexcept { return active_(); }

    // ------------------------------------------------------------- any thread
    /// Ask the control thread to start the hand-off.  false = ring full.
    [[nodiscard]] bool begin() noexcept { return cmd_.offer(kBeginMsg); }
    /// Fix round 2 (N2): a plain stop() is starting — stop deciding now.
    [[nodiscard]] bool halt() noexcept { return cmd_.offer(kHaltMsg); }
    /// Catalog history Task 11 fix round 0 — CONTROL thread: this process's
    /// CatalogApplier halted.  Every node-control request from now on carries
    /// kNodeCatalogHalted, and the node registers again at once (to EVERY member,
    /// re-sent until the current controller acked it): the controller never
    /// places a new partition here, and every member knows this node is no
    /// healthy controller candidate.  Idempotent.
    int reportCatalogHalted() {
        if (catalogHalted_) return 0;
        catalogHalted_ = true;
        if (!active_()) return 0;
        if (std::holds_alternative<Running>(state_) || std::holds_alternative<Registering>(state_))
            state_ = Registering{};   // a fresh request: re-broadcast at once
        return 1;
    }
    /// ISR v3 (Step 9): this node's storage id and origin wire code (AlterIsrMsg::kStorageOrigin*), carried in slots
    /// 7/8/9 of every node-control frame.  Startup thread / control thread.  `bind()` is unchanged.
    void setStorage(int64_t id, uint8_t origin) noexcept { storageId_ = id; storageOrigin_ = origin; }
    /// ISR v3: is this node's storage CONFIRMED (the gate is open)?  While not, and while the controller has no
    /// committed storage for this node, a Running node re-registers every kStorageReRegisterNs (the controller
    /// ignores a register until it is a ready level >= 2 controller; an event or a lost frame must not stall it).
    using StorageOpenFn = bool (*)(void* ctx) noexcept;
    void setStorageOpenFn(StorageOpenFn fn, void* ctx) noexcept { storageOpenFn_ = fn; storageOpenCtx_ = ctx; }
    static constexpr int64_t kStorageReRegisterNs = 5'000'000'000LL;
    /// TEST ONLY (control thread): pretend the Running registration was acked in consensus term `t`.
    void setRegisteredTermForTest(int64_t t) noexcept {
        if (auto* r = std::get_if<Running>(&state_)) r->registeredTerm = t;
    }
    /// CONTROL thread: the storage gate decided (founded / joined): register again at once, with the decided
    /// origin (the pattern of reportCatalogHalted).  Idempotent.
    int reportStorageDecided() {
        if (!active_()) return 0;
        if (std::holds_alternative<Running>(state_) || std::holds_alternative<Registering>(state_)) {
            state_ = Registering{};
            return 1;
        }
        return 0;
    }
    /// Control thread / tests: this process's current identity (nonce).
    [[nodiscard]] int64_t nonceOnControl() const noexcept { return incarnation_; }

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
        bool beginNow = false, haltNow = false;
        work += cmd_.poll([&beginNow, &haltNow](std::int32_t type, std::int64_t, std::int64_t) noexcept {
            if (type == kBeginMsg) beginNow = true;
            if (type == kHaltMsg) haltNow = true;
        });
        if (haltNow && !std::holds_alternative<Halted>(state_)) {
            work += halt_(nowNs);
            publishStatus_();
            return work;
        }
        if (std::holds_alternative<Halted>(state_)) { publishStatus_(); return work; }
        if (rotateNow_) {   // N1: a member answered kNodeSuperseded — a fresh identity, re-sent at once
            rotateNow_ = false;
            work += rotate_(nowNs);
        }
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
        if (r.errorCode == rune::replication::AlterIsrMsg::kNodeSuperseded) { rotateNow_ = true; return; }
        const bool    complete  = r.errorCode == rune::replication::kAlterIsrOk;
        const int32_t responder = r.partition;   // the answering controller
        std::visit([complete, responder](auto& st) {
            if constexpr (requires { st.req; }) {
                st.req.ackedBy    = responder;
                st.req.unanswered = 0;
                if (complete) st.req.completeBy = responder;   // a pushed COMPLETE is final for it
                else if (st.req.completeBy == responder) st.req.completeBy = -2;
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
        int32_t unanswered = 0;       ///< sends to `sentTo` with no answer (backoff, review L2)
        bool    silentLogged = false;
    };
    /// Review L2: after this many unanswered sends to one controller (an older
    /// binary never answers) the re-send interval doubles, up to kMaxBackoffShift.
    static constexpr int32_t kFastSends       = 20;
    static constexpr int32_t kMaxBackoffShift = 7;   // 50 ms << 7 = 6.4 s
    struct Registering { static constexpr Phase kPhase = Phase::Registering; Request req; };   // readiness: -1
    struct Running {
        static constexpr Phase kPhase = Phase::Running;
        int32_t  registeredWith = -1;
        int64_t  registeredTerm = -1;   ///< ISR v3: the consensus term the registration was acked in
        int64_t  lastRegisterNs = 0;    ///< ISR v3: the last (re-)register, for the storage re-send cadence
        int32_t  remaining      = -1;   ///< readiness gate: partitions not in sync here (outOfSync)
        uint64_t mapVersion     = ~0ull;
        int32_t  gateCtl        = -2;   ///< controller the gate was computed under
    };
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
    struct Halted { static constexpr Phase kPhase = Phase::Halted; };
    using State = std::variant<Running, Registering, Draining, Resigning, Done, Halted>;

    static constexpr std::int32_t kBeginMsg  = 1;
    static constexpr std::int32_t kHaltMsg   = 2;
    static constexpr std::int32_t kStatusMsg = 1;

    [[nodiscard]] bool active_() const noexcept {
        return send_fn_ != nullptr && metadata_ != nullptr && election_ != nullptr && self_ >= 0;
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
        if (q.sentTo != ctl) q.unanswered = 0;                  // a new controller: fast again
        else if (q.ackedBy == ctl) q.unanswered = 0;            // it answers (PENDING)
        const int32_t shift = std::clamp(q.unanswered - kFastSends, 0, kMaxBackoffShift);
        if (!force && q.sentTo == ctl && nowNs - q.lastSendNs < (kResendNs << shift)) return 0;
        if (q.unanswered == kFastSends && !q.silentLogged) {
            q.silentLogged = true;
            spdlog::warn("ControlledShutdown[{}]: controller {} does not answer node-control requests (an "
                         "older version?) — backing off", self_, ctl);
        }
        q.sentTo = ctl;
        ++q.unanswered;
        return sendTo_(q, flags, nowNs);
    }
    /// Broadcast one NODE-level request to every member (incl. self).
    int sendTo_(Request& q, int32_t flags, int64_t nowNs) {
        rune::replication::AlterIsrMsg m{};
        m.topicId   = rune::replication::AlterIsrMsg::kNodeControlTopicId;
        m.partition = 0;
        m.leaderId  = self_;
        m.leaderEpoch = 0;
        m.partitionEpoch = 0;
        m.isrCount  = rune::replication::AlterIsrMsg::kLeaderEpochBump;
        m.isr[1]    = flags | (catalogHalted_ ? rune::replication::AlterIsrMsg::kNodeCatalogHalted : 0);
        // D15: the metadata feature level this binary supports.
        m.isr[rune::replication::AlterIsrMsg::kNodeFeatureLevelSlot] = rune::replication::MetadataFeature::kLatest;
        m.isr[3]    = static_cast<int32_t>(static_cast<uint32_t>(incarnation_));
        m.isr[4]    = static_cast<int32_t>(static_cast<uint32_t>(static_cast<uint64_t>(incarnation_) >> 32));
        // ISR v3: the storage this process runs on (slots 7/8 = id low/high, 9 = origin).
        m.isr[rune::replication::AlterIsrMsg::kNodeStorageLoSlot] =
            static_cast<int32_t>(static_cast<uint32_t>(static_cast<uint64_t>(storageId_)));
        m.isr[rune::replication::AlterIsrMsg::kNodeStorageHiSlot] =
            static_cast<int32_t>(static_cast<uint32_t>(static_cast<uint64_t>(storageId_) >> 32));
        m.isr[rune::replication::AlterIsrMsg::kNodeStorageOriginSlot] = storageOrigin_;
        q.lastSendNs = nowNs;   // before the sends: a local answer arrives synchronously
        for (int32_t n : members_) (void)send_fn_(sendCtx_, n, m);   // ring full: re-sent later
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

    int step_(Running& st, int64_t nowNs) {
        // A new controller may hold a mark an earlier process of this node
        // left (it was a member then): register again on every controller change.
        // ISR v3: and on a new TERM of the same controller (its soft registration state died with the old term).
        if (!active_()) return 0;
        const int32_t ctl = controller_();
        if (ctl >= 0 && (ctl != st.registeredWith || election_->leadershipTermId() > st.registeredTerm)) {
            state_ = Registering{};
            return 1;
        }
        // Readiness gate (review L5): recomputed only on a committed-map change.
        const uint64_t v = metadata_->routingVersion();
        if (v != st.mapVersion || ctl != st.gateCtl) {   // N4: a lost controller → unknown
            st.mapVersion = v;
            st.gateCtl    = ctl;
            st.remaining  = ctl >= 0 ? outOfSync(metadata_->committedMap(), self_) : -1;
        }
        // ISR v3: until the storage is confirmed (gate open AND a committed record), keep telling the controller.
        const bool open = storageOpenFn_ == nullptr || storageOpenFn_(storageOpenCtx_);
        const bool committed = metadata_->featureLevel() < rune::replication::MetadataFeature::kStorageIdentity ||
                               metadata_->msm().nodeStorage(self_) != 0;
        if ((!open || !committed) && nowNs - st.lastRegisterNs >= kStorageReRegisterNs) {
            st.lastRegisterNs = nowNs;
            Request q;
            return sendTo_(q, rune::replication::AlterIsrMsg::kNodeRegister, nowNs);
        }
        return 0;
    }
    int step_(Done&, int64_t) { return 0; }
    int step_(Halted&, int64_t) { return 0; }

    /// Fix round 2 (N2): a plain stop() is starting.  Decide nothing more
    /// (the data plane is about to go), never stand again, hand the consensus
    /// off at once if leading (StandDown), and tell every member this node is
    /// going (best effort, once — they avoid it as a leader right away).
    int halt_(int64_t nowNs) {
        if (metadata_) metadata_->halt();
        if (election_) {
            election_->withdrawCandidacy();
            (void)election_->resign(nowNs);
        }
        if (active_()) {
            Request q;
            (void)sendTo_(q, rune::replication::AlterIsrMsg::kNodeShutdown, nowNs);
        }
        state_ = Halted{};
        spdlog::info("ControlledShutdown[{}]: halted for stop — no further decisions, consensus handed off", self_);
        return 1;
    }
    /// N1: a fresh process identity; the current request is re-sent at once.
    int rotate_(int64_t nowNs) {
        const int64_t old = incarnation_;
        incarnation_ = freshNonce();
        spdlog::warn("ControlledShutdown[{}]: identity {} was superseded by a stale frame — re-registering as {}",
                     self_, old, incarnation_);
        return std::visit([&](auto& st) -> int {
            if constexpr (requires { st.req; }) {
                st.req = Request{};   // immediate re-send with the new identity
                return std::visit([&](auto& s2) { return step_(s2, nowNs); }, state_);
            } else if constexpr (std::is_same_v<std::decay_t<decltype(st)>, Running>) {
                state_ = Registering{};   // re-register under the new identity
                return 1;
            } else {
                return 0;
            }
        }, state_);
    }

public:
    /// A random non-zero 64-bit process identity (cold).
    static int64_t freshNonce() {
        std::random_device rd;
        uint64_t v = (static_cast<uint64_t>(rd()) << 32) ^ static_cast<uint64_t>(rd());
        v ^= static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count()) *
             0x9E3779B97F4A7C15ull;
        return v == 0 ? 1 : static_cast<int64_t>(v);
    }
private:
    int step_(Registering& st, int64_t nowNs) {
        if (st.req.ackedBy >= 0 && st.req.ackedBy == controller_()) {
            Running r;
            r.registeredWith = st.req.ackedBy;
            r.registeredTerm = election_->leadershipTermId();
            r.lastRegisterNs = nowNs;
            state_ = r;
            return 1;
        }
        return send_(st.req, rune::replication::AlterIsrMsg::kNodeRegister, nowNs);
    }
    int step_(Draining& st, int64_t nowNs) {
        const auto& map = metadata_->committedMap();
        const int32_t ctl = controller_();
        if (ctl < 0 && !leadsMovable_(map)) {
            // Review L3: no controller known and nothing this node leads could
            // move — nobody to hand anything to; stop now.
            spdlog::info("ControlledShutdown[{}]: no controller known and no movable partition led here — "
                         "ready to stop", self_);
            state_ = Done{};
            return 1;
        }
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

    [[nodiscard]] bool leadsMovable_(const rune::metadata::AssignmentMap& map) const noexcept {
        bool any = false;
        map.forEach([&](int32_t, int32_t, const rune::metadata::PartitionAssignment& pa) {
            any = any || (pa.leaderNode == self_ && pa.isr.size() > 1);
        });
        return any;
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
    SendFn                                      send_fn_     = nullptr;
    void*                                       sendCtx_     = nullptr;
    MetadataService*                            metadata_    = nullptr;
    rune::consensus::Election*                  election_    = nullptr;
    bool                                        rotateNow_   = false;   ///< N1 (control thread)
    /// Task 11 fix round 0: a latched process attribute (orthogonal to the
    /// hand-off state machine): every request carries kNodeCatalogHalted.
    bool                                        catalogHalted_ = false;
    int64_t                                     storageId_     = 0;     ///< ISR v3: slots 7/8 of every frame
    uint8_t                                     storageOrigin_ = 0;     ///< ISR v3: slot 9 (0 = none)
    StorageOpenFn                               storageOpenFn_  = nullptr;
    void*                                       storageOpenCtx_ = nullptr;
    std::vector<int32_t>                        members_;               ///< every cluster member (incl. self)

    State  state_{Running{}};                       // control thread
    Status published_{Phase::Running, -1};          // control thread
    rune::consensus::ControlRing<1024> cmd_;        // any → control
    rune::consensus::ControlRing<4096> status_;     // control → observer
    Status observed_{};                             // observer thread
};

} // namespace rune::node
