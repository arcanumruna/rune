#pragma once
// CatalogStressDriver.h - catalog history Task 21: the seeded, model-checked stress driver.
//
// Split out of CatalogClusterHarness.h (Task 21 fix round 1, R0) as a pure move: the harness keeps the 3-broker
// cluster and its primitives; the driver owns the stress's own state (step counter, in-doubt resolution, tamper
// and isolation bookkeeping) and drives the harness through a friend declaration.  Single-threaded: the test
// thread owns the harness and the driver.

#include "support/CatalogClusterHarness.h"
#include "support/CatalogModel.h"

#include <random>
#include <span>

namespace rune::node::test {

// ── Task 21: the seeded stress (action / answer types) ───────────────────────
/// One step of the stress.  Everything random is drawn in randomAction (from the seed's rng); perform() is a pure
/// function of the cluster state and this value, so a seed replays exactly.
struct StressAction {
    enum class Kind : uint8_t { Ddl, PairDdl, MoveLeadership, RestartBroker, IsolateBroker, FreshBroker3, TamperBroker3, Pump,
                                LeaderMoveMidDdl, LeaderCrashMidDdl, FollowerStopMidDdl };
    Kind      kind = Kind::Pump;
    int       brokerA = 0, brokerB = 0;   // the entry brokers of the DDL(s)
    uint32_t  roll = 0;                   // which broker a fault picks among the eligible ones
    bool      flag = false;               // RestartBroker: mid-apply variant; IsolateBroker: fenced by the controller
    bool      withDdl = false;            // a DDL while the fault is on
    uint32_t  cut = 0;                    // *MidDdl: how many passes the DDL runs before the fault cuts it (0..40)
    cmodel::Request reqA, reqB;
};
/// One DDL's TERMINAL answer (after the in-doubt resolution), with the broker that gave it.
struct StressAnswer {
    cmodel::Request request;
    int      code = -1;
    int      broker = 0;
    bool     brokerHalted = false;     // the answering broker's applier was halted when the answer arrived
    bool     resolvedInDoubt = false;  // the first terminal answer was NotLeader / Redirect / Timeout; this is the resolved one
    int64_t  eventOffset = -1;         // the log offset of the event the request produced (OK only)
};
struct StepResult {
    std::vector<StressAnswer> answers;
    std::string               what;       // the action, for failure messages
    bool                      skipped = false;
    uint64_t                  trace = 0xcbf29ce484222325ull;   // FNV-1a of (action, every answer) - the determinism witness
    void fold(uint64_t v) noexcept { trace = (trace ^ v) * 0x100000001b3ull; }
    void fold(const std::string& s) noexcept { for (const unsigned char c : s) fold(static_cast<uint64_t>(c)); fold(uint64_t{0xFF}); }
};

class CatalogStressDriver {
public:
    static constexpr int32_t kBrokers = CatalogClusterHarness::kBrokers;
    static constexpr int     kCap     = CatalogClusterHarness::kCap;
    static constexpr int64_t kSession = CatalogClusterHarness::kSession;

    explicit CatalogStressDriver(CatalogClusterHarness& h) : h_(h) {}
    CatalogStressDriver(const CatalogStressDriver&)            = delete;
    CatalogStressDriver& operator=(const CatalogStressDriver&) = delete;

    // ═════════════════════════════════════════════════════════════════════════
    // Task 21: the seeded, model-checked stress
    // ═════════════════════════════════════════════════════════════════════════
    // The stress is SEQUENTIAL at the model level: every step's DDLs end in a terminal answer before the next
    // step.  In-doubt answers (C1): a terminal NotLeader / Redirect / Timeout means the DDL MAY have committed.
    // The step resolves it IMMEDIATELY: healAll() (every fault ends), then the SAME request (same requestId) is
    // re-sent via the leader until a definitive answer - the dedupe returns the original result when the event
    // committed, a fresh decision otherwise - so the model always sees the DDL's true outcome before the next
    // DDL on its name.  A bounded cap (fail_ naming the stage) guards every wait; no wall clock decides anything.

    /// A DDL requestId, stable across the retries of that DDL.
    [[nodiscard]] rune::protocol::RequestId freshRequestId() { return h_.freshRid_(); }

    /// Draws the next action (weights: DDL 55, concurrent pair 10, move leadership 8, restart 8, isolate 8,
    /// fresh broker 3 3, tamper broker 3 2, pump 6).  The model supplies the DDL contents (versions, live names).
    [[nodiscard]] StressAction randomAction(std::mt19937& rng, const cmodel::CatalogModel& model) {
        StressAction a;
        const uint32_t roll = rng() % 100;
        auto entry = [&] { return static_cast<int>(1 + rng() % static_cast<uint32_t>(kBrokers)); };
        auto withRid = [&](cmodel::Request r) { r.requestId = h_.freshRid_(); return r; };
        // Weights (of 100): Ddl 47, pair 10, move leadership 8, restart 8, isolate 8, fresh broker 3, tamper 2,
        // leader move mid-DDL 4, leader crash mid-DDL 2, follower stop mid-DDL 2, pump 6.
        auto cutDdl = [&](StressAction::Kind k) {
            a.kind = k;
            a.brokerA = entry();
            a.reqA = withRid(model.generate(rng));
            a.roll = rng();
            a.cut = rng() % 41;
        };
        if (roll < 47) {
            a.kind = StressAction::Kind::Ddl;
            a.brokerA = entry();
            a.reqA = withRid(model.generate(rng));
        } else if (roll < 57) {
            a.kind = StressAction::Kind::PairDdl;
            a.brokerA = entry();
            a.brokerB = 1 + (a.brokerA - 1 + 1 + static_cast<int>(rng() % 2)) % kBrokers;   // a different broker
            a.reqA = withRid(model.generate(rng));
            a.reqB = withRid(model.generate(rng, &a.reqA.name));
        } else if (roll < 65) {
            a.kind = StressAction::Kind::MoveLeadership;
            a.roll = rng();
        } else if (roll < 73) {
            a.kind = StressAction::Kind::RestartBroker;
            a.roll = rng();
            a.flag = rng() % 2 == 0;
            a.withDdl = rng() % 100 < 60;
            a.reqA = withRid(model.generate(rng));
        } else if (roll < 81) {
            a.kind = StressAction::Kind::IsolateBroker;
            a.roll = rng();
            a.flag = rng() % 2 == 0;
            a.withDdl = rng() % 100 < 70;
            a.brokerA = entry();
            a.reqA = withRid(model.generate(rng));
        } else if (roll < 84) {
            a.kind = StressAction::Kind::FreshBroker3;
            a.withDdl = rng() % 100 < 60;
            a.reqA = withRid(model.generate(rng));
        } else if (roll < 86) {
            a.kind = StressAction::Kind::TamperBroker3;
            a.reqA = withRid(model.generateAccepted(rng));
            a.reqB = withRid(model.generate(rng));
        } else if (roll < 90) {
            cutDdl(StressAction::Kind::LeaderMoveMidDdl);
        } else if (roll < 92) {
            cutDdl(StressAction::Kind::LeaderCrashMidDdl);
        } else if (roll < 94) {
            cutDdl(StressAction::Kind::FollowerStopMidDdl);
        } else {
            a.kind = StressAction::Kind::Pump;
        }
        return a;
    }

    /// Performs one step.  Every DDL of the step ends in a terminal answer (returned in `answers`, in the order
    /// the step issued them).
    StepResult perform(const StressAction& a) {
        ++step_;
        touched_.fill(false);
        StepResult out;
        out.fold(static_cast<uint64_t>(a.kind));
        out.what = describeAction_(a);
        restartHaltedIfDue_();
        switch (a.kind) {
        case StressAction::Kind::Ddl:
            runDdls_(out, {{a.reqA, a.brokerA}});
            break;
        case StressAction::Kind::PairDdl:
            runDdls_(out, {{a.reqA, a.brokerA}, {a.reqB, a.brokerB}});
            break;
        case StressAction::Kind::MoveLeadership: {
            const std::vector<int> cands = leaderCandidates_(h_.cur_.leaderNode);
            if (cands.empty()) { out.skipped = true; break; }
            h_.moveCatalogLeadership(cands[a.roll % cands.size()]);
            for (int i = 0; i < 5; ++i) h_.pump();
            break;
        }
        case StressAction::Kind::RestartBroker: {
            const int victim = 1 + static_cast<int>(a.roll % static_cast<uint32_t>(kBrokers));
            if (!vacateLeadership_(victim)) { out.skipped = true; break; }
            if (a.flag) {   // restart MID-APPLY: the process dies after the checkpoint, before the side effects
                h_.armSwallowEffects(victim);
                const std::size_t before = out.answers.size();
                if (a.withDdl) runDdls_(out, {{a.reqA, h_.cur_.leaderNode}});
                if (!h_.applierHalted(victim)) {
                    for (std::size_t i = before; i < out.answers.size(); ++i) {
                        if (out.answers[i].code != 0) continue;
                        const int64_t off = out.answers[i].eventOffset;
                        h_.until_([&] { return !h_.broker_(victim).alive || h_.applierHalted(victim) || h_.checkpointReaches_(victim, off); },
                               "restart mid-apply: the victim never persisted the event's checkpoint");
                    }
                }
                stopS_(victim);
                restartS_(victim);
            } else {
                stopS_(victim);
                if (a.withDdl) runDdls_(out, {{a.reqA, h_.cur_.leaderNode}});
                for (int i = 0; i < 3; ++i) h_.pump();
                restartS_(victim);
            }
            for (int i = 0; i < 3; ++i) h_.pump();
            break;
        }
        case StressAction::Kind::IsolateBroker: {
            const int victim = 1 + static_cast<int>(a.roll % static_cast<uint32_t>(kBrokers));
            awaitLeaderServing_();
            h_.ft_.isolate(victim);
            touched_[static_cast<std::size_t>(victim - 1)] = true;
            const bool fence = a.flag && victim != h_.cur_.leaderNode;
            if (fence) h_.shrinkIsr_(victim);
            isolated_[victim] = fence;
            if (a.withDdl) runDdls_(out, {{a.reqA, a.brokerA}});
            for (int i = 0; i < 20; ++i) h_.pump();
            healIsolation_();
            break;
        }
        case StressAction::Kind::FreshBroker3: {
            if (!vacateLeadership_(3)) { out.skipped = true; break; }
            stopS_(3);
            h_.wipeBrokerDir(3);
            if (a.withDdl) runDdls_(out, {{a.reqA, h_.cur_.leaderNode}});
            restartS_(3);
            for (int i = 0; i < 3; ++i) h_.pump();
            break;
        }
        case StressAction::Kind::TamperBroker3: {
            if (!h_.broker_(3).alive || h_.applierHalted(3) || tamperArmed_ || h_.cur_.leaderNode == 3) { out.skipped = true; break; }
            awaitLeaderServing_();
            h_.admin(3).applierForTest().tamperNextFingerprintForTest();
            tamperArmed_ = true;
            const std::size_t before = out.answers.size();
            runDdls_(out, {{a.reqA, h_.cur_.leaderNode}});
            bool applied = false;
            for (std::size_t i = before; i < out.answers.size(); ++i) applied = applied || out.answers[i].code == 0;
            // The event applies on broker 3 and its fingerprint no longer matches: broker 3 halts.  (A refused
            // DDL appends nothing: the tamper stays armed for the next event; an in-doubt resolution that healed
            // the cluster restarted broker 3 with a clear seam: nothing to wait for.)
            if (applied && tamperArmed_)
                h_.until_([&] { return h_.applierHalted(3); }, "tamper: broker 3 never halted on the tampered event");
            if (h_.applierHalted(3)) runDdls_(out, {{a.reqB, 3}});   // a DDL sent to the halted broker: Internal, never a redirect
            break;
        }
        case StressAction::Kind::LeaderMoveMidDdl: {
            // The DDL is cut by a leadership move after `cut` passes: append not yet replicated, appended and
            // replicated, committed but unanswered ... the cut point is the seed's.
            const std::vector<int> cands = leaderCandidates_(h_.cur_.leaderNode);
            if (cands.empty()) {
                out.skipped = true;
                runDdls_(out, {{a.reqA, a.brokerA}});
                break;
            }
            runDdlsCut_(out, a, [&] { h_.moveCatalogLeadership(cands[a.roll % cands.size()]); }, [] {});
            break;
        }
        case StressAction::Kind::LeaderCrashMidDdl: {
            const int old = h_.cur_.leaderNode;
            const std::vector<int> cands = leaderCandidates_(old);
            if (cands.empty()) {
                out.skipped = true;
                runDdls_(out, {{a.reqA, a.brokerA}});
                break;
            }
            // The leadership moves and the old leader dies with NO pass between: it never answers what it had
            // in flight (those DDLs are counted in crashedUnanswered and resolved by the in-doubt path).
            runDdlsCut_(out, a,
                        [&] {
                            h_.moveCatalogLeadership(cands[a.roll % cands.size()]);
                            stopS_(old, /*crash=*/true);
                        },
                        [&] { restartS_(old); });
            break;
        }
        case StressAction::Kind::FollowerStopMidDdl: {
            std::vector<int> followers;
            for (int b = 1; b <= kBrokers; ++b)
                if (b != h_.cur_.leaderNode && h_.broker_(b).alive) followers.push_back(b);
            if (followers.empty()) {
                out.skipped = true;
                runDdls_(out, {{a.reqA, a.brokerA}});
                break;
            }
            const int victim = followers[a.roll % followers.size()];
            runDdlsCut_(out, a, [&] { stopS_(victim); }, [&] { restartS_(victim); });
            break;
        }
        case StressAction::Kind::Pump:
            for (int i = 0; i < 50; ++i) h_.pump();
            break;
        }
        for (const StressAnswer& s : out.answers) {
            out.fold(static_cast<uint64_t>(s.code));
            out.fold(static_cast<uint64_t>(s.broker));
            out.fold(static_cast<uint64_t>(s.eventOffset + 1));
            out.fold(cmodel::toString(s.request));
        }
        out.fold(static_cast<uint64_t>(h_.cur_.leaderNode));
        out.fold(static_cast<uint64_t>(out.skipped));
        return out;
    }

    /// Every fault ends: holds released, isolations healed, dead and halted brokers restarted (a restart is the
    /// operator's fix and clears a tamper seam), and every broker converged on one applied offset and digest.
    void healAll() {
        h_.releaseHighWatermark();
        h_.holdPlacement(false);
        healIsolation_();
        for (int b = 1; b <= kBrokers; ++b) if (!h_.broker_(b).alive) restartS_(b);
        for (int b = 1; b <= kBrokers; ++b) {
            if (!h_.applierHalted(b)) continue;
            stopS_(b);
            restartS_(b);
        }
        if (!h_.awaitDigestsEqual(/*excludeHalted=*/false)) h_.fail_("heal", "the brokers never converged on one applied offset and digest");
    }

    /// Invariant 1: brokers at the same applied offset hold the same digest (halted brokers are excluded: they
    /// stopped applying on purpose).
    [[nodiscard]] bool digestsEqualAtEqualOffsets() {
        std::map<int64_t, uint64_t> byOffset;
        for (const auto& br : h_.brokers_) {
            if (!br->alive || br->admin->applier().halted()) continue;
            const int64_t a = br->admin->applier().state().appliedOffset();
            const uint64_t d = br->admin->applier().state().digest();
            const auto [it, fresh] = byOffset.emplace(a, d);
            if (!fresh && it->second != d) return false;
        }
        return true;
    }

    /// Invariant 4: between a create and the drop of one name no other create of that name; an object id is
    /// never reused; every alter / drop acts on the live object of its name.  Read from the leader's log.
    [[nodiscard]] bool noNameHadTwoConcurrentLiveObjects(std::string* why = nullptr) {
        using K = rune::catalog::EventKind;
        std::map<std::string, uint64_t> liveId;
        std::set<uint64_t> usedIds;
        for (const auto& r : h_.readLog_(*h_.broker_(h_.cur_.leaderNode).log)) {
            const auto& e = r.event;
            auto bad = [&](const std::string& m) { if (why) *why = "offset " + std::to_string(r.offset) + " '" + e.objectName + "': " + m; return false; };
            const auto it = liveId.find(e.objectName);
            switch (e.kind) {
            case K::CreateTopic: case K::CreateView:
                if (it != liveId.end()) return bad("created while object " + std::to_string(it->second) + " is live");
                if (!usedIds.insert(e.objectId).second) return bad("objectId " + std::to_string(e.objectId) + " reused");
                liveId[e.objectName] = e.objectId;
                break;
            case K::DropTopic: case K::DropView:
                if (it == liveId.end() || it->second != e.objectId) return bad("dropped an object that is not the live one");
                liveId.erase(it);
                break;
            default:
                if (it == liveId.end() || it->second != e.objectId) return bad("altered an object that is not the live one");
                break;
            }
        }
        return true;
    }

    /// The leader's applied catalog, mapped to the model's shapes (after the leader serves, i.e. has applied
    /// every committed event).
    [[nodiscard]] cmodel::Snapshot catalogAsModelSnapshot() {
        awaitLeaderServing_();
        const auto& st = h_.leaderState_();
        cmodel::Snapshot out;
        for (const auto* t : st.topics()) {
            cmodel::ModelObject o;
            o.objectId = t->objectId;
            o.version = t->version;
            o.transient = t->persistence == rune::catalog::Persistence::Transient;
            o.config = t->config;
            o.topicId = t->topicId;
            o.replicationFactor = t->replicationFactor;
            o.partitions = t->partitions;
            o.keyField = t->schema ? t->schema->keyField : std::string{};
            if (t->schema) {
                o.typed = true;
                o.schemaVersion = t->schema->schemaVersion;
                o.compat = t->schema->compat == rune::codec::CompatMode::Forward ? cmodel::Compat::Forward : cmodel::Compat::Backward;
                const rune::codec::Descriptor d = rune::codec::deserialize(
                    std::span<const uint8_t>(t->schema->evolvingBlob.data(), t->schema->evolvingBlob.size()));
                for (const auto& f : d.fields) if (f.untilVersion == 0xFFFF) o.fields.insert(f.name);
            }
            out.emplace(t->name, std::move(o));
        }
        for (const auto* v : st.views()) {
            cmodel::ModelObject o;
            o.isView = true;
            o.objectId = v->objectId;
            o.version = v->version;
            o.transient = v->def.persistence == rune::catalog::Persistence::Transient;
            o.source = v->def.sourceNames.empty() ? std::string{} : v->def.sourceNames.front();
            o.topicId = v->def.viewTopicId;
            o.keyField = v->def.keyField;
            {
                const rune::codec::Descriptor d = rune::codec::deserialize(
                    std::span<const uint8_t>(v->def.outputDescriptorBlob.data(), v->def.outputDescriptorBlob.size()));
                for (const auto& f : d.fields) o.viewColumns.push_back(f.name);
            }
            out.emplace(v->name, std::move(o));
        }
        return out;
    }

    /// Every live, non-halted broker SERVES exactly the model's live names (applied + placed), each with the
    /// persistence the model holds (topics and view sinks).  nullopt = it does; else the first difference.
    [[nodiscard]] std::optional<std::string> servingDiffersFromModel(const cmodel::Snapshot& want) {
        if (!h_.awaitDigestsEqual(/*excludeHalted=*/true)) return "the brokers never converged on one applied offset before the serving check";
        h_.settleControl_();
        for (const auto& br : h_.brokers_) {
            if (!br->alive || br->admin->applier().halted()) continue;
            std::map<std::string, bool> got;   // name -> transient
            for (int32_t id = 0; id < br->ctl.reg->topicCount(); ++id) {
                if (!h_.serves_(*br, id)) continue;
                const rune::node::TopicSpec* spec = br->ctl.reg->topic(id);
                if (spec == nullptr) continue;
                if (!got.emplace(spec->name, spec->persistence == PersistenceMode::Transient).second)
                    return "broker " + std::to_string(br->id) + " serves two topics named '" + spec->name + "'";
            }
            for (const auto& [name, o] : want) {
                const auto it = got.find(name);
                if (it == got.end()) {
                    std::string d = "broker " + std::to_string(br->id) + " does not serve '" + name + "'";
                    const auto id = h_.appliedTopicId_(br->id, name);
                    d += id ? "; applied topicId " + std::to_string(*id) + " registry-state=" +
                                  std::to_string(static_cast<int>(br->ctl.reg->state(*id))) + " placed=" + std::to_string(h_.placed_.contains(*id))
                            : std::string("; NOT in its applied catalog");
                    d += " pending=" + std::to_string(br->ctl.pending.size()) + " retiring=" + std::to_string(br->ctl.retiring.size());
                    return d;
                }
                if (it->second != o.transient) return "broker " + std::to_string(br->id) + " serves '" + name + "' with the wrong persistence";
                if (auto why = projectionDiffers_(*br, name, o)) return why;
            }
            std::size_t modelViews = 0;
            for (const auto& [name, o] : want) { (void)name; modelViews += o.isView ? 1u : 0u; }
            if (br->views.all().size() != modelViews)
                return "broker " + std::to_string(br->id) + " holds " + std::to_string(br->views.all().size()) +
                       " view records, the model has " + std::to_string(modelViews);
            for (const auto& [name, transient] : got) {
                (void)transient;
                if (!want.contains(name)) return "broker " + std::to_string(br->id) + " still serves the dropped '" + name + "'";
            }
        }
        return std::nullopt;
    }

    /// The broker's PROJECTIONS of one model object (what the control side and the registries hold, not the applied
    /// state): a topic's config and (typed) schema version / compatibility; a view's sink id, output columns and
    /// persistence.  nullopt = they equal the model.
    [[nodiscard]] std::optional<std::string> projectionDiffers_(CatalogClusterHarness::Broker& br, const std::string& name,
                                                               const cmodel::ModelObject& o) {
        const std::string who = "broker " + std::to_string(br.id) + " '" + name + "': ";
        if (!o.isView) {
            const auto cfg = br.ctl.configs.find(o.topicId);
            if (cfg == br.ctl.configs.end()) return who + "holds no config for topic id " + std::to_string(o.topicId);
            if (!(cfg->second == o.config))
                return who + "holds config retention.ms=" + std::to_string(cfg->second.retentionMs) + " delete.retention.ms=" +
                       std::to_string(cfg->second.deleteRetentionMs) + ", the model has retention.ms=" + std::to_string(o.config.retentionMs) +
                       " delete.retention.ms=" + std::to_string(o.config.deleteRetentionMs);
            if (o.typed) {
                const auto* e = br.schemas.latest(o.topicId);
                if (e == nullptr) return who + "the schema registry holds no schema";
                if (e->version != o.schemaVersion)
                    return who + "schema registry version " + std::to_string(e->version) + ", the model has " + std::to_string(o.schemaVersion);
                const bool fwd = e->compatMode == rune::codec::CompatMode::Forward;
                if (fwd != (o.compat == cmodel::Compat::Forward)) return who + "schema registry compatibility differs";
            }
            return std::nullopt;
        }
        const auto* rec = br.views.byName(name);
        if (rec == nullptr) return who + "the view registry holds no record";
        if (rec->viewTopicId != o.topicId)
            return who + "view sink id " + std::to_string(rec->viewTopicId) + ", the model has " + std::to_string(o.topicId);
        std::vector<std::string> cols;
        const rune::codec::Descriptor d = rune::codec::deserialize(
            std::span<const uint8_t>(rec->outputDescriptorBlob.data(), rec->outputDescriptorBlob.size()));
        for (const auto& f : d.fields) cols.push_back(f.name);
        if (cols != o.viewColumns) return who + "view output columns differ from the model";
        if (rec->persistence != (o.transient ? 1 : 0)) return who + "view persistence differs from the model";
        return std::nullopt;
    }

    /// requestId -> how many events carry it.  Every live broker's log must hold the same events: nullopt when
    /// they differ (a divergence).
    [[nodiscard]] std::optional<std::map<rune::protocol::RequestId, std::size_t>> requestLedger() {
        std::optional<std::map<rune::protocol::RequestId, std::size_t>> first;
        for (const auto& br : h_.brokers_) {
            if (!br->alive) continue;
            std::map<rune::protocol::RequestId, std::size_t> mine;
            for (const auto& r : h_.readLog_(*br->log)) ++mine[r.event.principal.requestId];
            if (first && *first != mine) return std::nullopt;
            first = std::move(mine);
        }
        return first;
    }
    [[nodiscard]] std::size_t leaderEventCount() { return h_.readLog_(*h_.broker_(h_.cur_.leaderNode).log).size(); }
    /// How many DDLs ended in a terminal in-doubt answer (and were resolved) so far.
    [[nodiscard]] int inDoubtResolved() const noexcept { return inDoubtResolved_; }
    [[nodiscard]] int stepsDone() const noexcept { return step_; }
    /// DDLs a crashed catalog leader had started and never answered (summed over restarts).
    [[nodiscard]] uint64_t crashedUnanswered(int b) { return h_.broker_(b).crashedUnanswered; }
    /// True: a terminal in-doubt answer is resolved inside the step (C1).  The stress asserts none remains.
    [[nodiscard]] bool resolvesInDoubt() const noexcept { return resolveInDoubt_; }

private:
    // ── Task 21 helpers ──────────────────────────────────────────────────────
    [[nodiscard]] static bool inDoubtCode_(int code) noexcept {
        return code == static_cast<int>(rune::protocol::client::ErrorCode::NotLeader) ||
               code == static_cast<int>(rune::protocol::client::ErrorCode::Redirect) || code == kClientTimeout;
    }
    [[nodiscard]] std::string describeAction_(const StressAction& a) const {
        static constexpr const char* n[] = {"Ddl", "PairDdl", "MoveLeadership", "RestartBroker", "IsolateBroker", "FreshBroker3",
                                            "TamperBroker3", "Pump", "LeaderMoveMidDdl", "LeaderCrashMidDdl", "FollowerStopMidDdl"};
        std::string s = std::string(n[static_cast<int>(a.kind)]) + " (leader " + std::to_string(h_.cur_.leaderNode) + ")";
        switch (a.kind) {
        case StressAction::Kind::Ddl: s += " via " + std::to_string(a.brokerA) + ": " + cmodel::toString(a.reqA); break;
        case StressAction::Kind::PairDdl:
            s += " via " + std::to_string(a.brokerA) + ": " + cmodel::toString(a.reqA) + " || via " + std::to_string(a.brokerB) + ": " +
                 cmodel::toString(a.reqB);
            break;
        case StressAction::Kind::RestartBroker: case StressAction::Kind::IsolateBroker:
            s += std::string(a.flag ? " flag" : "") + (a.withDdl ? " +ddl " + cmodel::toString(a.reqA) : "");
            break;
        case StressAction::Kind::LeaderMoveMidDdl: case StressAction::Kind::LeaderCrashMidDdl: case StressAction::Kind::FollowerStopMidDdl:
            s += " via " + std::to_string(a.brokerA) + ": " + cmodel::toString(a.reqA) + " cut=" + std::to_string(a.cut);
            break;
        case StressAction::Kind::FreshBroker3: s += a.withDdl ? " +ddl " + cmodel::toString(a.reqA) : ""; break;
        case StressAction::Kind::TamperBroker3: s += " " + cmodel::toString(a.reqA); break;
        default: break;
        }
        return s;
    }

    /// The brokers the leadership may move to from `from`: alive, in the ISR, not the current leader, not halted
    /// and not carrying an armed tamper (a halted leader could never answer DDL).
    [[nodiscard]] std::vector<int> leaderCandidates_(int from) const {
        std::vector<int> out;
        for (int b = 1; b <= kBrokers; ++b) {
            if (b == from || !h_.broker_(b).alive || h_.broker_(b).admin->applier().halted()) continue;
            if (tamperArmed_ && b == 3) continue;
            if (std::find(h_.cur_.isr.begin(), h_.cur_.isr.end(), b) == h_.cur_.isr.end()) continue;
            out.push_back(b);
        }
        return out;
    }
    /// `b` stops leading (a broker is stopped only as a follower).  False when no broker can take over.
    bool vacateLeadership_(int b) {
        if (h_.cur_.leaderNode != b) return true;
        const std::vector<int> cands = leaderCandidates_(b);
        if (cands.empty()) return false;
        h_.moveCatalogLeadership(cands.front());
        return true;
    }
    void awaitLeaderServing_() {
        h_.until_([&] { return h_.broker_(h_.cur_.leaderNode).admin->roleName() == "LeaderServing"; },
               "the catalog leader never served DDL (applied up to its high watermark)");
    }
    /// `crash`: the catalog leader dies with DDLs in flight - what it started and never answered is recorded in
    /// crashedUnanswered (the final started == answered + crashedUnanswered).  Every other stop is QUIESCENT.
    void stopS_(int b, bool crash = false) {
        CatalogClusterHarness::Broker& br = h_.broker_(b);
        if (br.alive && br.admin) {
            const uint64_t pending = br.admin->startedCount() - br.admin->answeredCount();
            if (crash) br.crashedUnanswered += pending;
            else if (pending != 0)
                h_.fail_("stress", "broker " + std::to_string(b) + " is stopped with " + std::to_string(pending) + " unanswered DDL(s) (not a crash)");
        }
        touched_[static_cast<std::size_t>(b - 1)] = true;
        h_.stopBroker(b);
        if (b == 3) tamperArmed_ = false;
        haltedSinceStep_[static_cast<std::size_t>(b - 1)] = -1;
    }
    void restartS_(int b) {
        h_.restartBroker(b);
        if (b == 3) tamperArmed_ = false;   // a restarted applier has no tamper seam
        haltedSinceStep_[static_cast<std::size_t>(b - 1)] = -1;
    }
    /// A halted broker stays halted a few steps (the cluster keeps working around it), then the operator
    /// restarts it.
    void restartHaltedIfDue_() {
        for (int b = 1; b <= kBrokers; ++b) {
            int& since = haltedSinceStep_[static_cast<std::size_t>(b - 1)];
            if (!h_.applierHalted(b)) { since = -1; continue; }
            if (since < 0) { since = step_; continue; }
            if (step_ - since < 4) continue;
            stopS_(b);
            restartS_(b);
        }
    }
    void healIsolation_() {
        for (const auto& [b, fenced] : isolated_) {
            h_.ft_.deisolate(b);
            if (fenced) h_.admitWhenCaughtUp_(b, /*awaitApplied=*/!h_.applierHalted(b));
        }
        isolated_.clear();
    }

    [[nodiscard]] uint64_t progressSig_() const {
        uint64_t h = static_cast<uint64_t>(h_.recs_.size()) * 0x9E3779B97F4A7C15ull;
        for (const auto& br : h_.brokers_) {
            if (!br->alive) continue;
            h = h * 1099511628211ull + static_cast<uint64_t>(br->replica->logEndOffset());
            h = h * 1099511628211ull + static_cast<uint64_t>(br->admin->applier().nextOffset());
        }
        return h;
    }
    /// runOps_ with an idle clock: when no log, applier or answer moved for 16 passes (a leader waiting out its
    /// 10 s commit timeout), the virtual clock jumps 250 ms.  Capped like every wait.
    void runStressOps_(const std::vector<ClientOp*>& ops, const std::string& stage) {
        int idle = 0;
        uint64_t last = ~uint64_t{0};
        for (int i = 0; i < kCap; ++i) {
            bool allDone = true, onlyResends = true;
            int64_t earliest = INT64_MAX;
            for (const ClientOp* op : ops) {
                if (op->done()) continue;
                allDone = false;
                if (const auto* r = std::get_if<ClientOp::AwaitingResend>(&op->st)) earliest = std::min(earliest, r->atNs);
                else onlyResends = false;
            }
            if (allDone) return;
            if (onlyResends && earliest > h_.now_ + 2'000'000) {
                h_.now_ = earliest - 1'000'000;
                idle = 0;
            } else {
                const uint64_t sig = progressSig_();
                if (sig == last) {
                    if (++idle >= 16) { h_.now_ += 250'000'000; idle = 0; }
                } else {
                    idle = 0;
                    last = sig;
                }
            }
            h_.pump();
        }
        h_.fail_(stage, "the client ops never finished (pass cap " + std::to_string(kCap) + " hit)");
    }

    /// The DDL the model request states, built against the leader's applied catalog only for the view source's
    /// topic id (the client resolves a name to an id when it authors a plan).
    [[nodiscard]] DdlRequest toDdl_(const cmodel::Request& r) {
        using cmodel::Op;
        const std::string statement = std::string("STRESS ") + cmodel::opName(r.op) + " " + r.name + " rid=" +
                                      std::to_string(r.requestId.lo);
        const DdlMeta meta = h_.metaFor_(statement, r.requestId);
        const uint8_t persistence = r.transient ? 1 : 0;
        switch (r.op) {
        case Op::DefineTopic: {
            DefineTopicMsg m;
            m.session = kSession; m.name = r.name; m.partitionCount = 1;
            m.keyField = r.typed ? "id" : "";
            if (r.typed) m.blob = CatalogDdlRig::descriptor(r.name);
            m.persistence = persistence;
            m.replicationFactor = r.replicationFactor;
            m.meta = meta;
            return DdlRequest{std::move(m)};
        }
        case Op::DropTopic: {
            DropTopicMsg m; m.session = kSession; m.name = r.name; m.meta = meta;
            return DdlRequest{std::move(m)};
        }
        case Op::AlterConfig: {
            AlterTopicConfigMsg m; m.session = kSession; m.name = r.name; m.meta = meta;
            m.expectedVersion = r.expectedVersion;
            m.changes = {{r.configKey == cmodel::ConfigKey::RetentionMs ? rune::catalog::ConfigKey::RetentionMs
                                                                        : rune::catalog::ConfigKey::DeleteRetentionMs,
                          r.configValue}};
            return DdlRequest{std::move(m)};
        }
        case Op::AlterSchema: case Op::SetCompat: {
            AlterTopicMsg m; m.session = kSession; m.topicName = r.name; m.meta = meta;
            m.expectedVersion = static_cast<uint16_t>(r.expectedVersion);
            rune::codec::AlterOp op{};
            if (r.op == Op::AlterSchema) {
                op.type = r.schemaOp == cmodel::SchemaOp::AddField ? rune::codec::AlterOpType::Add : rune::codec::AlterOpType::Drop;
                op.name = r.field;
                op.primType = rune::codec::PrimType::Int32;
            } else {
                op.type = rune::codec::AlterOpType::SetCompatibility;
                op.mode = r.mode == cmodel::Compat::Forward ? rune::codec::CompatMode::Forward : rune::codec::CompatMode::Backward;
            }
            m.opsBlob = rune::codec::encodeAlterOps({op});
            return DdlRequest{std::move(m)};
        }
        case Op::CreateView: {
            CreateViewMsg m; m.session = kSession; m.viewName = r.name; m.keyField = "id"; m.persistence = persistence;
            m.planBlob = stressPlan_(r.name, r.source, r.extraColumn);
            m.meta = meta;
            return DdlRequest{std::move(m)};
        }
        case Op::AlterView: {
            AlterViewMsg m; m.session = kSession; m.name = r.name; m.keyField = "id"; m.persistence = persistence;
            m.expectedVersion = r.expectedVersion;
            m.planBlob = stressPlan_(r.name, r.source, r.extraColumn);
            m.meta = meta;
            return DdlRequest{std::move(m)};
        }
        case Op::DropView: {
            DropViewMsg m; m.session = kSession; m.name = r.name; m.meta = meta;
            return DdlRequest{std::move(m)};
        }
        }
        throw std::logic_error("toDdl_: unreachable");
    }
    /// An authored plan (id + hp [+ hp2]) over `source`, whose id is what a client reads from the catalog: a
    /// topic's id, or a view's sink id; a name that is neither gets an id no topic has.
    [[nodiscard]] std::vector<uint8_t> stressPlan_(const std::string& view, const std::string& source, bool extraColumn) {
        namespace rv = rune::view;
        const auto& st = h_.leaderState_();
        int32_t id = 999'999;
        if (const auto* t = st.topicByName(source)) id = t->topicId;
        else if (const auto* v = st.viewByName(source)) id = v->def.viewTopicId;
        rv::ViewPlan p;
        p.viewName = view;
        p.key      = "id";
        p.sources.push_back(rv::SourceSpec{id, "id"});
        p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::Int64, 0, "id", rv::MergeRule::Replace});
        p.output.push_back(rv::AuthoredField{"hp", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
        if (extraColumn)
            p.output.push_back(rv::AuthoredField{"hp2", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
        return rv::serializeAuthored(p);
    }

    /// Sends each request via its entry broker, runs the client model to the terminal answers, resolves the
    /// in-doubt ones (C1) and appends the terminal answers.
    void runDdls_(StepResult& out, const std::vector<std::pair<cmodel::Request, int>>& reqs) {
        awaitLeaderServing_();
        std::vector<ClientOp*> ops;
        for (const auto& [r, b] : reqs) ops.push_back(&h_.startOp_(toDdl_(r), b, cmodel::toString(r)));
        runStressOps_(ops, "stress: the DDLs");
        finishDdls_(out, reqs, ops, haltedTargets_(ops));
    }

    /// One DDL cut by a fault: sent, `a.cut` passes run, `fault` fires, the client model runs to its terminal
    /// answer, `after` ends the fault (a restart), then the shared tail resolves an in-doubt answer.
    template <class Fault, class After>
    void runDdlsCut_(StepResult& out, const StressAction& a, Fault&& fault, After&& after) {
        awaitLeaderServing_();
        std::vector<ClientOp*> ops{&h_.startOp_(toDdl_(a.reqA), a.brokerA, cmodel::toString(a.reqA))};
        for (uint32_t i = 0; i < a.cut; ++i) h_.pump();
        // A halted flag is read when the answer ARRIVES: an op already answered before the fault is sampled now (the fault
        // may stop the halted broker), the others once they finish (`after` may restart - and so clear - a halted one).
        std::vector<char> halted = haltedTargets_(ops);
        std::vector<char> answered;
        for (const ClientOp* op : ops) answered.push_back(op->done() ? 1 : 0);
        fault();
        runStressOps_(ops, "stress: the cut DDL");
        const std::vector<char> late = haltedTargets_(ops);
        for (std::size_t i = 0; i < ops.size(); ++i) if (answered[i] == 0) halted[i] = late[i];
        after();
        finishDdls_(out, {{a.reqA, a.brokerA}}, ops, halted);
    }

    /// Whether each op's last target had a halted applier (read when the answers arrived).
    [[nodiscard]] std::vector<char> haltedTargets_(const std::vector<ClientOp*>& ops) {
        std::vector<char> out;
        for (const ClientOp* op : ops) out.push_back(h_.applierHalted(op->target) ? 1 : 0);
        return out;
    }

    /// The terminal answers of `ops` (one per request); an in-doubt one is resolved (C1) and appended to `out`.
    void finishDdls_(StepResult& out, const std::vector<std::pair<cmodel::Request, int>>& reqs, const std::vector<ClientOp*>& ops,
                     const std::vector<char>& haltedAtAnswer) {
        std::vector<StressAnswer> res(reqs.size());
        std::vector<std::size_t> doubt;
        for (std::size_t i = 0; i < reqs.size(); ++i) {
            res[i].request = reqs[i].first;
            res[i].code = ops[i]->finalCode();
            res[i].broker = ops[i]->target;
            res[i].brokerHalted = haltedAtAnswer[i] != 0;
            if (inDoubtCode_(res[i].code)) doubt.push_back(i);
            // M7: a Timeout is explained only by a fault on the op's last target in this step (stopped, isolated or
            // crashed); from a healthy broker it is a DDL answered later than its deadline - a failure.
            if (res[i].code == kClientTimeout && !touched_[static_cast<std::size_t>(ops[i]->target - 1)])
                h_.fail_("stress", "Timeout from a healthy broker " + std::to_string(ops[i]->target) + " for '" +
                                       cmodel::toString(reqs[i].first) + "': no fault touched it in this step");
        }
        if (!doubt.empty() && resolveInDoubt_) {
            healAll();
            for (const std::size_t i : doubt) {
                // The SAME request (same requestId, same statement fingerprint) through the leader.
                awaitLeaderServing_();
                ClientOp& retry = h_.startOp_(DdlRequest(ops[i]->base), h_.cur_.leaderNode, cmodel::toString(reqs[i].first) + " (resolve)");
                runStressOps_({&retry}, "stress: resolving an in-doubt DDL");
                res[i].code = retry.finalCode();
                res[i].broker = retry.target;
                res[i].brokerHalted = h_.applierHalted(retry.target);
                res[i].resolvedInDoubt = true;
                ++inDoubtResolved_;
                if (inDoubtCode_(res[i].code))
                    h_.fail_("stress", "the in-doubt DDL '" + cmodel::toString(reqs[i].first) + "' was still unanswered (code " +
                                        std::to_string(res[i].code) + ") after the cluster healed");
            }
        }
        for (StressAnswer& s : res) {
            if (s.code == 0) s.eventOffset = h_.eventOffsetOf_(s.request.requestId);
            out.answers.push_back(std::move(s));
        }
    }

    bool                  resolveInDoubt_ = true;
    std::array<bool, 3>   touched_{false, false, false};   // brokers stopped / isolated / crashed in the current step (M7)
    int                   step_ = 0;
    int                   inDoubtResolved_ = 0;
    bool                  tamperArmed_ = false;
    std::map<int, bool>   isolated_;                         // broker -> fenced from the ISR by the controller
    std::array<int, 3>    haltedSinceStep_{-1, -1, -1};

    CatalogClusterHarness& h_;
};

} // namespace rune::node::test
