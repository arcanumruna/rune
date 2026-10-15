#pragma once
// CatalogClusterHarness.h — catalog history Task 20: the 3-broker harness of the
// interaction matrix (it promotes Task 11's CatalogTrio).
//
// Each broker b (ids 1..3) owns what its admin thread owns in production (a real
// disk __catalog Log, a PartitionReplica over it with the production catalog
// config, its CatalogApplier, SchemaRegistry, ViewRegistry and AdminWorker) plus
// a FAKE CONTROL SIDE: a real gateway TopicRegistry that the applier's effects
// build into (the control thread's reserve-behind-drop / create / drop-retire
// rules of BrokerNode::onCatalogTopicApplied_ and dropRuntimeTopic_), and the
// cluster-wide placement the controller commits.  A topic SERVES on b iff its
// create is applied there (registry Serving) AND its placement is committed.
//
// The replicas of the three brokers talk over ONE shared FilteringTransport
// (rune/replication/testing/FilteringTransport.h) through a thin per-broker
// HoldingLink: the link adds the high-watermark hold seam (a held leader's
// responses are parked, requests to it are lost) and nothing else.
//
// The CONTROLLER is scripted: leadership changes go through the production
// MetadataService::decideCoordinatorPartition (the controller is the broker the
// leadership moves to, exactly as the controller prefers itself when it is in the
// ISR); ISR changes are committed AlterIsrs (cur_.isr + partitionEpoch).
//
// The FAKE GATEWAY applies the DDL route of a broker (a mirror of
// BrokerNode::publishCatalogRoute_ + ClientGateway::ddlRouted_, BrokerNode.cpp /
// ClientGateway.cpp "Catalog history Task 11"): this broker leads -> submit to the
// admin worker; another leads -> ErrorResponse{Redirect 3, its client endpoint};
// none -> NotLeader 2; this broker's applier halted -> Internal 6 (never a
// redirect).  The DDL answer gate (DdlAnswerGate: a topic-creating OK leaves only
// once the topic serves) is mirrored per broker, FIFO.
//
// The CLIENT MODEL (ClientOp) follows the rune-client-cpp rules of Task 5, mirrored
// in ONE place (kDdl* below) with the source lines named: Redirect is followed up
// to kMaxDdlRedirects hops, NotLeader is retried with a 100 ms backoff doubling to
// 1 s until the ddlRetryBudget (10 s, bounded by the 15 s DDL deadline) runs out,
// then it fails explicitly with NotLeader; every other answer is terminal.
//
// Single-threaded: driven from the test thread, a deterministic clock (one pump =
// 1 ms; idle waits jump it), no wall-clock sleeps.  EVERY wait is capped and throws
// std::logic_error naming the cell and the stage (gtest reports it as a failure of
// that cell) — never a hang.

#include "support/CatalogDdlRig.h"            // Answer / descriptor()
#include "support/CatalogMatrixExpectations.h"

#include <rune/gateway/TopicRegistry.h>
#include <rune/node/AdminChannels.h>
#include <rune/node/AdminWorker.h>
#include <rune/node/CatalogApplier.h>
#include <rune/node/CatalogCheckpoint.h>
#include <rune/node/CatalogPartition.h>
#include <rune/node/MetadataService.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/TopicDirReconcile.h>
#include <rune/node/ViewRegistry.h>

#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogQueries.h>
#include <rune/catalog/TopicName.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/testing/FilteringTransport.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>
#include <rune/view/ViewPlan.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <random>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <unistd.h>

namespace rune::node::test {

namespace cm = rune::node::test::cmatrix;
namespace rr = rune::replication;

namespace cluster_detail {
inline int gSeq = 0;   // single-threaded test binaries
}

// ── The client rules (Task 5), mirrored in one place ─────────────────────────
// Source: rune-client-cpp/src/Client.cpp — kDdlInitialBackoffNs / kDdlMaxBackoffNs
// (the anonymous namespace above Client::onDdlRedirect_), Client::onDdlNotLeader_
// (budget = min(DDL deadline, first NotLeader + ddlRetryBudget); backoff doubles
// from the initial to the max; a NotLeader at or past the budget end fails with
// NotLeader), Client::onDdlRedirect_ (> kMaxDdlRedirects hops fails with Redirect);
// rune-client-cpp/include/rune/client/Client.h — ClientConfig::ddlRetryBudget
// (10 s), ClientConfig::ddlTimeoutMs (15 s), Client::kMaxDdlRedirects (3).
// Pinned against these documented values by CatalogInteractionMatrixMirrorRegression.
inline constexpr int64_t kDdlInitialBackoffNs = 100'000'000;      // 100 ms
inline constexpr int64_t kDdlMaxBackoffNs     = 1'000'000'000;    // 1 s
inline constexpr int64_t kDdlRetryBudgetNs    = 10'000'000'000;   // ClientConfig::ddlRetryBudget
inline constexpr int64_t kDdlDeadlineNs       = 15'000'000'000;   // ClientConfig::ddlTimeoutMs
inline constexpr int     kMaxDdlRedirects     = 3;               // Client::kMaxDdlRedirects
/// The code a client op ends with when its DDL deadline passes with no answer
/// (OpStatus::Timeout) — not a protocol code; no cell expects it.
inline constexpr int     kClientTimeout       = 1000;

/// One answer as a client sees it (a broker's reply or the gateway's route answer).
struct ClusterAnswer {
    CatalogDdlRig::Answer a;
    int                   broker = 0;
    int32_t               gateTopic = -1;   // the topic its OK waits for (DdlAnswerGate); -1 = none
    bool                  visible = false;
    bool                  servedAtRelease = false;
    bool                  consumed = false;   // the client model read it
};

/// The error code (kErrorRedirect...) names the matrix uses.
inline constexpr uint16_t kCodeOk = 0, kCodeNotLeader = 2, kCodeRedirect = 3, kCodeInternal = 6;

/// One DDL on the client side: a variant of states, each with what it needs (rule 3).
struct ClientOp {
    struct Unsent {};
    struct InFlight { int broker; int64_t corr; };
    struct AwaitingResend { int64_t atNs; };
    struct Finished { int code; };

    int                       id = 0;
    rune::protocol::RequestId rid{};
    DdlRequest                base;            // the same request on every (re)send
    std::string               statement;
    int                       target = 0;      // the endpoint (broker) the next send goes to
    int                       redirects = 0;
    int64_t                   budgetEndNs = 0; // 0 = no NotLeader seen yet
    int64_t                   backoffNs = 0;
    int64_t                   deadlineNs = 0;
    int                       firstCode = -1;
    CatalogDdlRig::Answer::Kind firstKind = CatalogDdlRig::Answer::Kind::Error;
    std::string               firstMessage;
    int                       afterRedirectCode = -1;
    bool                      captureNext = false;   // the first answer was a Redirect: record the next
    std::vector<int>          codes;                 // every answer seen, in order
    std::function<void()>     beforeFollowRedirect;  // a hook: runs before EVERY Redirect hop is followed
    std::variant<Unsent, InFlight, AwaitingResend, Finished> st{Unsent{}};
    int                       terminalResults = 0;   // must end at exactly 1

    [[nodiscard]] bool done() const noexcept { return std::holds_alternative<Finished>(st); }
    [[nodiscard]] int finalCode() const noexcept { return done() ? std::get<Finished>(st).code : -1; }
};

/// What a cell recorded (see CatalogInteractionMatrixRegressionTest.cpp).
struct CellAnswerFirst { int code = -1; std::string message; CatalogDdlRig::Answer::Kind kind = CatalogDdlRig::Answer::Kind::Error; };
struct CellRun {
    CellAnswerFirst first;
    int afterRedirectCode = -1, finalCode = -1, otherFinalCode = -1, viaLeaderCode = -1, retriedCode = -1;
    bool     produceOnExistingTopicSucceeded = false;
    int      okCount = 0;
    uint64_t objectIdBefore = 0, objectIdAfter = 0;
    uint32_t newVersionAfter = 0, firstVersionOfNewObject = 0;
    int      produceInWindowCode = -1;
    bool     okReleasedOnlyAfterServing = false;
    bool     haltedBrokerStillServesAppliedTopics = false;
    bool     haltOffsetIsTheTamperedEvent = false;   // R2: the halt names the offset of the event the victim refused
    bool     appliedEverywhereBeforePlacement = false;   // R7: every live broker's catalog holds the topic, no placement yet
    int      produceAfterPlacementCode = -1;         // R7 (CreateTopic): a produce on the placed topic
    bool     historyKeepsBothObjects = false;        // R6: history(name) holds events of the old AND the new objectId
    bool     transientEmptyAfterRestart = false;     // RestartMidApply x Transient
    bool     everyRequestHadOneTerminalResult = false;
    std::vector<rune::protocol::RequestId> okRequestIds, refusedRequestIds;
    std::string statement;
};

/// A per-broker link over the shared FilteringTransport with the HW-hold seam.
struct ClusterHold {
    rr::NodeId held = -1;   // the leader whose high watermark cannot move
    struct Parked { rr::NodeId to; rr::FetchResponseMsg resp; std::vector<uint8_t> records; };
    std::vector<Parked> parked;
};
class HoldingLink final : public rr::PartitionTransport {
public:
    HoldingLink(rr::test::FilteringTransport& ft, ClusterHold& hold, rr::NodeId self) : ft_(ft), hold_(hold), self_(self) {}
    bool sendFetchRequest(const rr::FetchRequestMsg& req) override {
        if (hold_.held >= 0 && hold_.held == ft_.currentLeaderId()) return true;   // lost: the held leader hears nothing
        return ft_.sendFetchRequest(req);
    }
    bool sendFetchResponse(rr::NodeId to, const rr::FetchResponseMsg& resp) override {
        if (hold_.held >= 0 && self_ == hold_.held) {   // parked until released to chosen followers
            ClusterHold::Parked p;
            p.to = to; p.resp = resp;
            p.records.assign(resp.records.begin(), resp.records.end());
            hold_.parked.push_back(std::move(p));
            return true;
        }
        return ft_.sendFetchResponse(to, resp);
    }
    bool sendOffsetsForLeaderEpochRequest(rr::NodeId to, const rr::OffsetsForLeaderEpochReqMsg& r) override {
        return ft_.sendOffsetsForLeaderEpochRequest(to, r);
    }
    bool sendOffsetsForLeaderEpochResponse(rr::NodeId to, const rr::OffsetsForLeaderEpochRespMsg& r) override {
        return ft_.sendOffsetsForLeaderEpochResponse(to, r);
    }
    bool sendReconcileRequest(rr::NodeId to, const rr::FetchRequestMsg& r) override { return ft_.sendReconcileRequest(to, r); }
    bool sendReconcileResponse(rr::NodeId to, const rr::FetchResponseMsg& r) override { return ft_.sendReconcileResponse(to, r); }
    bool sendInternalIsr(rr::NodeId, const rr::InternalIsrMsg&) override { return true; }
    bool sendInternalIsrAck(rr::NodeId, const rr::InternalIsrAckMsg&) override { return true; }

private:
    rr::test::FilteringTransport& ft_;
    ClusterHold&                  hold_;
    rr::NodeId                    self_;
};

/// The AdminIsrChannel stand-in (production: BrokerNode.cpp setIsrChannel(&catalogIsrChannel()); the proposal
/// travels admin -> control ring -> controller).  The harness's controller step drains the queue.
class HarnessIsrChannel final : public rr::PartitionTransport {
public:
    explicit HarnessIsrChannel(std::deque<rr::AlterIsrMsg>& q) : q_(q) {}
    bool sendAlterIsr(const rr::AlterIsrMsg& m) override { q_.push_back(m); return true; }
    // Every other PartitionTransport method is unused on an ISR channel.
    bool sendFetchRequest(const rr::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rr::NodeId, const rr::FetchResponseMsg&) override { return true; }

private:
    std::deque<rr::AlterIsrMsg>& q_;
};


class CatalogStressDriver;   // support/CatalogStressDriver.h: the Task 21 stress, a friend of the harness

class CatalogClusterHarness {
    friend class CatalogStressDriver;

public:
    using Answer = CatalogDdlRig::Answer;
    static constexpr int32_t kBrokers = 3;
    static constexpr int64_t kSession = 4242;
    static constexpr int     kCap     = 20'000;   // every drive loop's hard pass cap (1 ms each; idle waits jump)
    static constexpr const char* kTopic = "t";
    static constexpr const char* kBase  = "base";
    static constexpr const char* kSource = "s";
    static constexpr const char* kView  = "v";

    explicit CatalogClusterHarness(const std::string& cellName, int brokers = kBrokers)
        : tag_(cellName),
          root_(std::filesystem::temp_directory_path() /
                ("rune_cluster_" + std::to_string(::getpid()) + "_" + std::to_string(cluster_detail::gSeq++))) {
        if (brokers != kBrokers) fail_("ctor", "the matrix harness is 3 brokers");
        {
            std::error_code ec;
            std::filesystem::remove_all(root_, ec);   // a stale tree of a crashed run with the same pid
        }
        for (int32_t b = 1; b <= kBrokers; ++b) brokers_.push_back(open_(b));
        if (failAfterOpenForTest) fail_("ctor", "test seam: failAfterOpenForTest");
        // The controller forms __catalog exactly as MetadataService does for a fresh cluster.
        const std::vector<int32_t> replicas{1, 2, 3};
        const auto first = MetadataService::decideCoordinatorPartition(
            kCatalogTopicId, kCatalogPartition, /*self=*/1, nullptr, replicas, nullptr, /*bootstrapEpoch=*/1, replicas);
        if (!first) fail_("ctor", "the controller formed no __catalog assignment");
        cur_ = *first;
        commitAssignment_();
        awaitRole(1, "LeaderServing");
    }
    ~CatalogClusterHarness() {   // rootGuard_ removes the tree after the brokers are gone
        clients_.clear();
        brokers_.clear();
    }
    /// Test seam (TempDirHygieneRegression): the ctor throws after the brokers' directories exist, so the
    /// exception-safety of the root guard is observable.  Single-threaded test binaries.
    inline static bool failAfterOpenForTest = false;
    CatalogClusterHarness(const CatalogClusterHarness&)            = delete;
    CatalogClusterHarness& operator=(const CatalogClusterHarness&) = delete;

    // ═════════════════════════════════════════════════════════════════════════
    // The cell API
    // ═════════════════════════════════════════════════════════════════════════
    /// The "Precondition" column of the plan's ops table, built through the
    /// leader by DDL (the client model, no faults).  Every cell also gets the
    /// baseline topic `base` (untyped, disk) that produce/consume run on.
    void buildPrecondition(cm::Kind kind, cm::Persist persist) {
        persist_ = persist;
        doDdl_(makeRequest_(DdlSpec{DdlWhat::DefineTopic, kBase, false, cm::Persist::Disk}, freshRid_()), "precondition base");
        buildTarget_(kind, persist, /*typed=*/true);
        awaitServing_(kBase);
    }

    CellRun runCell(cm::Kind kind, cm::Entity ent, cm::Timing timing) {
        kind_ = kind; ent_ = ent; timing_ = timing;
        persist_ = ent.persist;
        const bool typedCreate = (static_cast<int>(ent.via) + static_cast<int>(ent.persist)) % 2 == 0;
        CellRun run;
        run.statement = std::string(cm::name(kind)) + " " + targetNameOf_(kind);
        endpointLeader_ = cur_.leaderNode;
        const int leader = cur_.leaderNode;
        const int via = ent.via == cm::Via::Leader ? leader : followerOf_(leader, 0);
        const int otherFollower = followerOf_(leader, 1);
        auto cellOp = [&](int target) -> ClientOp& {
            return startOp_(makeRequest_(specOf_(kind, typedCreate), freshRid_(), run.statement), target, run.statement);
        };
        std::vector<ClientOp*> all;

        switch (timing) {
        case cm::Timing::Steady: {
            ClientOp& op = cellOp(via);
            runOps_({&op}, "steady");
            all = {&op};
            fillCommon_(run, op);
            break;
        }
        case cm::Timing::RestartMidApply: {
            armSwallowEffects(otherFollower);   // the victim: checkpoint persisted, side effects never run
            ClientOp& op = cellOp(via);
            runOps_({&op}, "restart: the op");
            const int64_t offset = eventOffsetOf_(op.rid);
            until_([&] { return checkpointReaches_(otherFollower, offset); }, "restart: broker never persisted the checkpoint");
            if (ent.persist == cm::Persist::Transient) (void)produce(placementLeader_, kTopic, {1, 2, 3});   // data to lose
            stopBroker(otherFollower);
            restartBroker(otherFollower);
            // I3: the reconcile at this boot quarantined nothing (a dropped incarnation's dir is retired, never
            // quarantined; a live topic's dir is kept).
            if (!broker_(otherFollower).lastReconcile.quarantined.empty())
                fail_("restart", "the boot reconcile quarantined a topic dir: " +
                                     broker_(otherFollower).lastReconcile.quarantined.front().string());
            if (ent.persist == cm::Persist::Transient) {
                settleControl_();
                run.transientEmptyAfterRestart = transientEmptyAfterRestart_(otherFollower);
            }
            all = {&op};
            fillCommon_(run, op);
            break;
        }
        case cm::Timing::NewBrokerJoins: {
            stopBroker(otherFollower);
            wipeBrokerDir(otherFollower);
            ClientOp& op = cellOp(via);
            runOps_({&op}, "join: the op with {1,2}");
            startFreshBroker(otherFollower);
            all = {&op};
            fillCommon_(run, op);
            break;
        }
        case cm::Timing::LeaderChangeMidDdl: {
            const int next  = followerOf_(leader, 0);
            const int third = followerOf_(leader, 1);
            holdHighWatermark(leader);
            const int64_t before = replica(leader).logEndOffset();
            ClientOp& op = cellOp(via);
            until_([&] { return replica(leader).logEndOffset() > before; }, "leader change: the leader never appended");
            const int64_t appendedAt = replica(leader).logEndOffset() - 1;
            replicateTo({next, third});                    // the event reaches 2 and 3 ...
            if (replica(next).logEndOffset() <= appendedAt)
                fail_("leader change", "the event never reached the next leader before the move");
            moveCatalogLeadership(next);                   // ... then the leader loses leadership before answering
            releaseHighWatermark();
            holdHighWatermark(next);                       // the new leader cannot commit its inherited tail yet
            // Drive the client until its retry reached `next` AND `next` answered NotLeader (the catch-up gate).
            until_([&] { return answeredByWith_(op, next, kCodeNotLeader); },
                   "leader change: the retry never met the catch-up gate");
            if (admin(next).roleName() != "LeaderCatchingUp")
                fail_("leader change", "the new leader served before catching up");
            releaseHighWatermark();                        // now the tail commits
            runOps_({&op}, "leader change: the retry");
            if (!okAnswerMatchesLog_(op)) fail_("leader change", "the retry's OK is not the original event's result");
            if (eventOffsetOf_(op.rid) != appendedAt) fail_("leader change", "the OK'd event is not the original append");
            all = {&op};
            fillCommon_(run, op);
            break;
        }
        case cm::Timing::CatalogBelowMinIsr: {
            const int64_t logEndBefore = replica(leader).logEndOffset();
            const int returning = followerOf_(leader, 0);
            const int staying   = followerOf_(leader, 1);
            auto stopFollowers = [&] { stopBroker(returning); stopBroker(staying); };
            ClientOp* op = nullptr;
            if (ent.via == cm::Via::Leader) {
                stopFollowers();
                op = &cellOp(leader);
            } else {
                // Take the follower's Redirect, then stop both followers (the via broker
                // included) BEFORE the followed request reaches the leader.
                op = &cellOp(via);
                op->beforeFollowRedirect = stopFollowers;
            }
            runOps_({op}, "below minIsr: the op until the client budget ends");
            {   // data-plane isolation from the catalog: the base topic's data replica on the placement leader
                // appends, and the same pass leaves the __catalog log end untouched.  (The real data path is T22's.)
                const int64_t catalogLeoBefore = replica(leader).logEndOffset();
                const ProduceResult pr = produce(placementLeader_, kBase, {1, 2, 3});
                run.produceOnExistingTopicSucceeded =
                    pr.code == 0 && pr.offset >= 0 && replica(leader).logEndOffset() == catalogLeoBefore;
            }
            if (replica(leader).logEndOffset() != logEndBefore)
                fail_("below minIsr", "an event was appended below minIsr (log end " + std::to_string(logEndBefore) + " -> " +
                                          std::to_string(replica(leader).logEndOffset()) + ") codes=" + [&] {
                                          std::string c; for (int x : op->codes) c += std::to_string(x) + ","; return c; }() + " isr=" + std::to_string(replica(leader).isr().size()));
            restartBroker(returning);                       // one broker returns ...
            ClientOp& retried = startOp_(op->base, leader, run.statement);   // ... and the op is retried (same requestId)
            retried.rid = op->rid;
            runOps_({&retried}, "below minIsr: the retry");
            if (!okAnswerMatchesLog_(retried)) fail_("below minIsr", "the retry's OK is not the logged event's result");
            restartBroker(staying);
            all = {op, &retried};
            fillCommon_(run, *op);
            run.retriedCode = retried.finalCode();
            break;
        }
        case cm::Timing::ConcurrentDdlDifferentBrokers: {
            // The leader applies first: the DDL sent to the leader is the one that wins.
            // Via Leader: the cell's DDL goes to the leader, the OTHER one to a follower (its Redirect
            // lands behind).  Via Follower: the cell's DDL is redirected, the OTHER one goes to the leader.
            const int cellTo  = ent.via == cm::Via::Leader ? leader : via;
            const int otherTo = ent.via == cm::Via::Leader ? followerOf_(leader, 0) : leader;
            // Both are built from the same applied state (same names / same expectedVersion), different requestIds.
            const DdlSpec spec = specOf_(kind, typedCreate);
            // Ops send in creation order: the one meant for the leader is created (and so sent) first.
            ClientOp* first  = &startOp_(makeRequest_(spec, freshRid_(), run.statement), ent.via == cm::Via::Leader ? cellTo : otherTo, run.statement);
            ClientOp* second = &startOp_(makeRequest_(spec, freshRid_(), run.statement), ent.via == cm::Via::Leader ? otherTo : cellTo, run.statement);
            ClientOp& mine  = ent.via == cm::Via::Leader ? *first : *second;
            ClientOp& other = ent.via == cm::Via::Leader ? *second : *first;
            runOps_({&mine, &other}, "concurrent: both ops");
            all = {&mine, &other};
            fillCommon_(run, mine);
            run.otherFinalCode = other.finalCode();
            break;
        }
        case cm::Timing::DropAndReuseName: {
            // create -> drop -> create the same name, then the op on the new object.
            if (isCreationKind_(kind)) buildExisting_(kind, ent.persist, typedCreate);   // the first incarnation
            run.objectIdBefore = targetObjectId_(kind);
            doDdl_(makeRequest_(dropSpecOf_(kind), freshRid_()), "reuse: drop");
            if (!isCreationKind_(kind)) buildExisting_(kind, ent.persist, typedCreate);   // the new incarnation
            const uint32_t versionBeforeOp = isCreationKind_(kind) ? 0u : targetVersion_(kind);
            ClientOp& op = cellOp(via);
            runOps_({&op}, "reuse: the op");
            all = {&op};
            fillCommon_(run, op);
            run.firstVersionOfNewObject = isCreationKind_(kind) ? 1u : versionBeforeOp + 1u;
            if (!isCreationKind_(kind) && versionBeforeOp != 1u) fail_("reuse", "the new object's version chain did not restart at 1");
            run.objectIdAfter = objectIdOfOp_(op);
            run.historyKeepsBothObjects = historyHoldsObjects_(targetNameOf_(kind), run.objectIdBefore, run.objectIdAfter);
            break;
        }
        case cm::Timing::CreateRacesPlacement: {
            holdPlacement(true);
            ClientOp& op = cellOp(via);
            const std::string probeName = kind == cm::Kind::CreateTopic ? kTopic : kView;
            // The window: the topic is applied on the leader's broker, its placement is not committed yet.
            until_([&] {
                const auto id = appliedTopicId_(leader, probeName);
                return id.has_value() && registryHolds_(leader, *id) && !placed_.contains(*id);
            }, "placement: the topic was never applied on the leader's broker");
            const int32_t id = *appliedTopicId_(leader, probeName);
            run.produceInWindowCode = probe(leader, id);
            // Every live broker's catalog holds the topic while no placement committed (applied != placed).
            until_([&] {
                for (const auto& br : brokers_)
                    if (br->alive && !appliedTopicId_(br->id, probeName).has_value()) return false;
                return !placed_.contains(id);
            }, "placement: not every broker applied the topic before its placement");
            run.appliedEverywhereBeforePlacement = !placed_.contains(id);
            if (op.done()) fail_("placement", "the DDL answered before the topic's placement committed");
            const bool visibleInWindow = anyVisibleAnswerFor_(op);
            holdPlacement(false);
            runOps_({&op}, "placement: the op after the placement commits");
            run.okReleasedOnlyAfterServing = !visibleInWindow && okServedAtRelease_(op);
            if (probe(leader, id) != 0) fail_("placement", "the topic does not serve after its placement committed");
            if (kind == cm::Kind::CreateTopic) run.produceAfterPlacementCode = produce(placementLeader_, kTopic, {1}).code;
            all = {&op};
            fillCommon_(run, op);
            break;
        }
        case cm::Timing::FingerprintMismatch: {
            const int victim = otherFollowerForHalt_(leader);
            admin(victim).applierForTest().tamperNextFingerprintForTest();
            ClientOp& viaLeaderOp = cellOp(leader);
            runOps_({&viaLeaderOp}, "mismatch: the op via the leader");
            until_([&] { return applierHalted(victim); }, "mismatch: the broker never halted");
            // A DDL sent to the halted broker is answered Internal — never redirected.
            ClientOp& toHalted = cellOp(victim);
            runOps_({&toHalted}, "mismatch: the op to the halted broker");
            run.viaLeaderCode = viaLeaderOp.finalCode();
            all = {&viaLeaderOp, &toHalted};
            fillCommon_(run, toHalted);
            {   // the halted broker keeps serving what it applied: the catalog state, the registry AND the placement
                const auto* base = admin(victim).applier().state().topicByName(kBase);
                run.haltedBrokerStillServesAppliedTopics =
                    serving(victim, kBase) && base != nullptr && registryHolds_(victim, base->topicId);
                const auto* he = admin(victim).applier().haltError();
                run.haltOffsetIsTheTamperedEvent = he != nullptr && he->offset == eventOffsetOf_(viaLeaderOp.rid);
            }
            // The objectId / okRequestIds come from the op that succeeded.
            run.objectIdAfter = objectIdOfOp_(viaLeaderOp);
            break;
        }
        }
        finish_(run, all);
        return run;
    }

    /// Task 20 fix round 1 (I2): the catalog falls below its minIsr AFTER the leader appended the DDL and BEFORE
    /// it committed.  Both followers die with the event only in the leader's log; the controller fences the
    /// ISR.  The client must get the retriable NotLeader (never a Timeout) until its budget ends; once one
    /// broker returns the same requestId is answered by the ORIGINAL event, never a second one.
    struct AfterAppendRun {
        std::vector<int> codes;
        int      finalCode = -1, retriedCode = -1;
        int64_t  appendedAt = -1, leaderLogEndAtFence = -1, leaderLogEndAfterBudget = -1;
        int64_t  firstNotLeaderAfterNs = -1;   // virtual time from the fence to the first NotLeader the client saw
        uint64_t leaderStarted = 0, leaderAnswered = 0;
        bool     retryMatchesLog = false;
        std::size_t eventsWithRid = 0;
        int64_t  eventOffset = -1;
    };
    AfterAppendRun runBelowMinIsrAfterAppend(cm::Kind kind, cm::Via via) {
        kind_ = kind;
        const bool typedCreate = (static_cast<int>(via) + static_cast<int>(persist_)) % 2 == 0;
        const int leader = cur_.leaderNode;
        const int returning = followerOf_(leader, 0);
        const int staying   = followerOf_(leader, 1);
        const std::string statement = std::string(cm::name(kind)) + " " + targetNameOf_(kind);
        AfterAppendRun run;
        holdHighWatermark(leader);   // the event cannot commit before the fence
        const int64_t before = replica(leader).logEndOffset();
        ClientOp& op = startOp_(makeRequest_(specOf_(kind, typedCreate), freshRid_(), statement),
                                via == cm::Via::Leader ? leader : returning, statement);
        until_([&] { return replica(leader).logEndOffset() > before; }, "after-append: the leader never appended");
        run.appendedAt = replica(leader).logEndOffset() - 1;
        releaseHighWatermark();      // the followers never saw the event: nothing was parked for them
        stopBroker(returning);
        stopBroker(staying);         // the ISR is the leader alone: below minIsr with the event in flight
        run.leaderLogEndAtFence = replica(leader).logEndOffset();
        const int64_t fenceAtNs = now_;
        until_([&] { return std::find(op.codes.begin(), op.codes.end(), static_cast<int>(kCodeNotLeader)) != op.codes.end(); },
               "after-append: the in-flight DDL was never answered NotLeader");
        run.firstNotLeaderAfterNs = now_ - fenceAtNs;
        runOps_({&op}, "after-append: the op until the client budget ends");
        run.codes = op.codes;
        run.finalCode = op.finalCode();
        run.leaderLogEndAfterBudget = replica(leader).logEndOffset();
        run.leaderStarted = admin(leader).startedCount();
        run.leaderAnswered = admin(leader).answeredCount();
        restartBroker(returning);    // one broker returns: the event commits and applies
        ClientOp& retried = startOp_(op.base, leader, statement);   // the same request, the same requestId
        retried.rid = op.rid;
        runOps_({&retried}, "after-append: the retry");
        run.retriedCode = retried.finalCode();
        run.retryMatchesLog = okAnswerMatchesLog_(retried);
        run.eventsWithRid = countEventsWithRequestId(op.rid);
        run.eventOffset = eventOffsetOf_(op.rid);
        restartBroker(staying);
        (void)awaitDigestsEqual(/*excludeHalted=*/true);
        return run;
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Faults (the controller's and the network's)
    // ═════════════════════════════════════════════════════════════════════════
    /// The catalog leadership moves to `to`: the controller (= `to`, which prefers itself when it
    /// is in the ISR) runs the PRODUCTION decision and commits the result; every live broker gets
    /// its role and its route changes with it.
    void moveCatalogLeadership(int32_t to) {
        std::vector<int32_t> eligible;
        for (const auto& br : brokers_) if (br->alive) eligible.push_back(br->id);
        const auto next = MetadataService::decideCoordinatorPartition(
            kCatalogTopicId, kCatalogPartition, /*self=*/to, &cur_, eligible, nullptr, /*bootstrapEpoch=*/cur_.leaderEpoch + 1,
            {1, 2, 3});
        if (!next || next->leaderNode != to)
            fail_("leadership", "the controller would not move the catalog leadership to broker " + std::to_string(to));
        cur_ = *next;
        commitAssignment_();
    }
    /// `b` stops fetching and is destroyed (its dirs are kept); the controller drops it from the ISR.
    void stopBroker(int32_t b) {
        if (!broker_(b).alive) return;
        ft_.registerReplica(b, nullptr);
        ft_.isolate(b);
        brokers_[static_cast<std::size_t>(b - 1)]->alive = false;
        brokers_[static_cast<std::size_t>(b - 1)]->tearDown();
        shrinkIsr_(b);
    }
    /// A broker whose dir survived: the log recovers, the applier boot-replays [0, checkpoint], the rest
    /// catches up live; once caught up the controller admits it to the ISR (AlterIsr).
    void restartBroker(int32_t b) {
        if (broker_(b).alive) return;
        const uint64_t startedBefore = broker_(b).startedTotal, answeredBefore = broker_(b).answeredTotal;   // M5: kept
        const uint64_t crashedBefore = broker_(b).crashedUnanswered;
        brokers_[static_cast<std::size_t>(b - 1)] = open_(b);
        broker_(b).startedTotal  = startedBefore;
        broker_(b).answeredTotal = answeredBefore;
        broker_(b).crashedUnanswered = crashedBefore;
        ft_.deisolate(b);
        commitAssignment_();   // its role (follower of the current leader)
        admitWhenCaughtUp_(b);
    }
    /// A broker with an EMPTY data dir (after wipeBrokerDir): it replays from offset 0 and joins the ISR.
    void startFreshBroker(int32_t b) { restartBroker(b); }
    void wipeBrokerDir(int32_t b) {
        if (broker_(b).alive) fail_("wipe", "a running broker's dir is not wiped");
        std::error_code ec;
        std::filesystem::remove_all(root_ / ("b" + std::to_string(b)), ec);
    }
    /// The broker's next live effects are dropped (the process dies after the checkpoint and before the side
    /// effects run: stopBroker + restartBroker then re-runs them through the BOOT sink).
    void armSwallowEffects(int32_t b) { broker_(b).swallowEffects = true; }
    void holdHighWatermark(int32_t b) { hold_.held = b; }
    void replicateTo(std::set<int32_t> followers) {
        std::vector<ClusterHold::Parked> keep;
        for (auto& p : hold_.parked) {
            if (followers.contains(p.to)) {
                p.resp.records = std::span<const uint8_t>(p.records.data(), p.records.size());
                (void)ft_.sendFetchResponse(p.to, p.resp);
            } else {
                keep.push_back(std::move(p));
            }
        }
        hold_.parked.swap(keep);
        for (int i = 0; i < 8; ++i) pump();
    }
    void releaseHighWatermark() { hold_.held = -1; hold_.parked.clear(); }
    /// The controller's placement commits are held (CreateRacesPlacement) or flow.
    void holdPlacement(bool on) { placementHeld_ = on; }
    void holdApply(int32_t b, bool on) { broker_(b).admin->holdApplyForTest(on); }

    // ═════════════════════════════════════════════════════════════════════════
    // Observation
    // ═════════════════════════════════════════════════════════════════════════
    [[nodiscard]] AdminWorker& admin(int32_t b) { return *broker_(b).admin; }
    [[nodiscard]] rr::PartitionReplica& replica(int32_t b) { return *broker_(b).replica; }
    [[nodiscard]] int32_t leader() const noexcept { return cur_.leaderNode; }
    /// The leader's client endpoint as a Redirect named it at the cell's first send.
    [[nodiscard]] std::string leaderClientEndpoint() const { return clientEndpoint_(endpointLeader_ > 0 ? endpointLeader_ : cur_.leaderNode); }
    [[nodiscard]] bool applierHalted(int32_t b) { return broker_(b).alive && broker_(b).admin->applier().halted(); }
    [[nodiscard]] int haltCounter(int32_t b) const { return broker_(b).halts; }

    /// Pump until every live, non-excluded broker has applied its whole log and all agree on the digest
    /// and the applied offset.  Capped.
    bool awaitDigestsEqual(bool excludeHalted) {
        for (int i = 0; i < kCap; ++i) {
            if (digestsEqual_(excludeHalted)) return true;
            pump();
        }
        return digestsEqual_(excludeHalted);
    }
    /// Events carrying `rid` in EVERY live broker's log: the count when they agree, SIZE_MAX when they do not.
    [[nodiscard]] std::size_t countEventsWithRequestId(const rune::protocol::RequestId& rid) {
        std::optional<std::size_t> n;
        for (const auto& br : brokers_) {
            if (!br->alive) continue;
            std::size_t mine = 0;
            for (const auto& r : readLog_(*br->log)) mine += r.event.principal.requestId == rid ? 1 : 0;
            if (n && *n != mine) return SIZE_MAX;
            n = mine;
        }
        return n.value_or(0);
    }
    /// The shared library's history() over the leader's log shows the op: kind, objectId, version step,
    /// statement, requestId (128-bit) and the request fingerprint.
    [[nodiscard]] bool historyShows(cm::Kind kind, uint64_t objectId, const std::string& statement,
                                    const std::vector<rune::protocol::RequestId>& rids) {
        const auto log = readLog_(*broker_(cur_.leaderNode).log);
        for (const auto& rid : rids) {
            const rune::catalog::RecordedEvent* hit = nullptr;
            for (const auto& r : log) if (r.event.principal.requestId == rid) hit = &r;
            if (hit == nullptr) return false;
            const auto& e = hit->event;
            if (static_cast<int>(e.kind) != static_cast<int>(kind) + 1) return false;
            if (e.newVersion != e.prevVersion + 1 && !(e.kind == rune::catalog::EventKind::DropTopic || e.kind == rune::catalog::EventKind::DropView)) return false;
            if (e.statement != statement || e.principal.requestFingerprint == 0) return false;
            // objectId: R5 / R6 name the object; a drop event keeps it too.
            if (objectId != 0 && e.objectId != objectId) return false;
            // The library's own history() must list it (by the object's name).
            rune::catalog::HistoryFilter f;
            f.name = e.objectName;
            bool listed = false;
            for (const auto* h : rune::catalog::history(std::span<const rune::catalog::RecordedEvent>(log), f)) listed = listed || h == hit;
            if (!listed) return false;
        }
        return true;
    }
    /// Invariant 6 (C1 generalized): a topic serves on a broker iff its create is applied there and its
    /// placement is committed — for every live broker, every id of its applied catalog and its registry.
    [[nodiscard]] bool servingMatchesAppliedAndPlaced() {
        settleControl_();
        for (const auto& br : brokers_) {
            if (!br->alive) continue;
            const auto& st = br->admin->applier().state();
            std::set<int32_t> live;
            for (const auto* t : st.topics()) live.insert(t->topicId);
            for (const auto* v : st.views()) live.insert(v->def.viewTopicId);
            for (int32_t id = 0; id < br->ctl.reg->topicCount(); ++id) {
                const bool should = live.contains(id) && placed_.contains(id);
                if (serves_(*br, id) != should) return false;
            }
            for (int32_t id : live)
                if (serves_(*br, id) != placed_.contains(id)) return false;
        }
        return true;
    }

    // The data plane (produce / probe), through the placement leader (broker 1) with acks=leader.
    struct ProduceResult { int code = -1; int64_t offset = -1; };
    /// UnknownTopic 7: not in this broker's applied catalog and not placed; NotLeader 2: applied or placed but
    /// not serving yet (or this broker is not the partition leader); 0: serves.
    [[nodiscard]] int probe(int32_t b, int32_t topicId) {
        Broker& br = broker_(b);
        if (serves_(br, topicId)) return b == placementLeader_ ? 0 : 2;
        if (placed_.contains(topicId) || registryHolds_(b, topicId)) return 2;
        return 7;
    }
    ProduceResult produce(int32_t b, const std::string& topic, std::vector<uint8_t> bytes) {
        Broker& br = broker_(b);
        const rune::catalog::TopicObject* t = br.admin->applier().state().topicByName(topic);
        if (t == nullptr) return {7, -1};
        const int code = probe(b, t->topicId);
        if (code != 0) return {code, -1};
        rr::PartitionReplica& r = dataReplica_(br, *t);
        const rr::Offset off = r.appendKeyed(std::span<const uint8_t>{}, std::span<const uint8_t>(bytes), /*acksAll=*/false);
        if (off < 0) return {6, -1};
        r.onLeaderLogAppended();
        return {0, off};
    }
    /// A topic of `b`'s applied catalog serves on `b` (applied + placed).
    [[nodiscard]] bool serving(int32_t b, const std::string& topic) {
        Broker& br = broker_(b);
        const auto* t = br.admin->applier().state().topicByName(topic);
        return t != nullptr && serves_(br, t->topicId);
    }

    // ── driving ──────────────────────────────────────────────────────────────
    void pump() {
        now_ += 1'000'000;
        for (auto& br : brokers_) if (br->alive) (void)br->admin->doWork(now_);
        (void)ft_.pump(now_);
        controllerStep_();
        controlStep_();
        for (std::size_t k = 0; k < clients_.size(); ++k) stepClient_(*clients_[k]);
    }
    bool awaitRole(int32_t b, std::string_view role) {
        for (int i = 0; i < kCap && broker_(b).admin->roleName() != role; ++i) pump();
        if (broker_(b).admin->roleName() != role)
            fail_("role", "broker " + std::to_string(b) + " never became " + std::string(role));
        return true;
    }
    /// The clock jumps (idle waits: a client backoff); one pass follows.
    void advanceTime(int64_t ns) {
        now_ += ns;
        pump();
    }
    [[nodiscard]] int64_t now() const noexcept { return now_; }

    /// DDLs a broker started / answered over its whole life (the workers torn down by stop / restart included).
    [[nodiscard]] uint64_t startedTotal(int32_t b) { return broker_(b).startedTotal + (broker_(b).admin ? broker_(b).admin->startedCount() : 0); }
    [[nodiscard]] uint64_t answeredTotal(int32_t b) { return broker_(b).answeredTotal + (broker_(b).admin ? broker_(b).admin->answeredCount() : 0); }

    /// I4: the persistence the DDL asked for is what every live non-halted broker APPLIED and what its
    /// TopicRegistry holds.  Topic kinds: topic `t`; view kinds: view `v`, its sink and the source `s`; drop
    /// kinds: the object is absent.
    [[nodiscard]] bool persistenceMatches(cm::Kind kind, cm::Persist persist) {
        settleControl_();
        const auto want = persist == cm::Persist::Transient ? rune::catalog::Persistence::Transient : rune::catalog::Persistence::Disk;
        const auto wantMode = persist == cm::Persist::Transient ? PersistenceMode::Transient : PersistenceMode::Disk;
        for (const auto& br : brokers_) {
            if (!br->alive || br->admin->applier().halted()) continue;
            const auto& st = br->admin->applier().state();
            auto topicOk = [&](const char* name) {
                const auto* t = st.topicByName(name);
                if (t == nullptr || t->persistence != want) return false;
                const rune::node::TopicSpec* spec = br->ctl.reg->topic(t->topicId);
                return spec != nullptr && spec->persistence == wantMode;
            };
            auto viewOk = [&] {
                const auto* v = st.viewByName(kView);
                if (v == nullptr || v->def.persistence != want) return false;
                const rune::node::TopicSpec* spec = br->ctl.reg->topic(v->def.viewTopicId);
                return spec != nullptr && spec->persistence == wantMode;
            };
            switch (kind) {
            case cm::Kind::DropTopic: if (st.topicByName(kTopic) != nullptr) return false; break;
            case cm::Kind::DropView:  if (st.viewByName(kView) != nullptr) return false; break;
            case cm::Kind::CreateView: case cm::Kind::AlterView:
                if (!viewOk() || !topicOk(kSource)) return false;
                break;
            default: if (!topicOk(kTopic)) return false; break;
            }
        }
        return true;
    }

    // ── for the harness's own unit tests ─────────────────────────────────────
    /// While held, a dropped topic's dir is not renamed away, so a create of its (case-folded) name waits behind it.
    void holdRetireForTest(bool on) noexcept { holdRetire_ = on; }
    [[nodiscard]] bool retireHeldForTest() const noexcept { return holdRetire_; }
    [[nodiscard]] std::size_t pendingCountForTest(int b) const { return broker_(b).ctl.pending.size(); }
    [[nodiscard]] std::size_t retiringCountForTest(int b) const { return broker_(b).ctl.retiring.size(); }
    ClientOp& newOpForTest(DdlRequest req, int target) { return startOp_(std::move(req), target, "test"); }
    void runOpsForTest(std::vector<ClientOp*> ops) { runOps_(std::move(ops), "test"); }
    [[nodiscard]] DdlRequest requestForTest(cm::Kind k, cm::Persist p, bool typed) {
        return makeRequest_(specOf_(k, typed), freshRid_());
    }
    [[nodiscard]] const rune::catalog::CatalogState& stateOf(int32_t b) { return broker_(b).admin->applier().state(); }
    /// How many times the op was sent (its original send and every resend / followed Redirect).
    [[nodiscard]] std::size_t sendsOfForTest(const ClientOp& op) const {
        std::size_t n = 0;
        for (const Submission& s : subs_) n += s.op == op.id ? 1u : 0u;
        return n;
    }

private:
    // ═════════════════════════════════════════════════════════════════════════
    // Pieces
    // ═════════════════════════════════════════════════════════════════════════
    enum class DdlWhat : uint8_t { DefineTopic, DropTopic, AlterConfig, AlterSchema, SetCompat, CreateView, AlterView, DropView };
    struct DdlSpec { DdlWhat what; std::string name; bool typed; cm::Persist persist; };

    struct DataPartition {
        std::unique_ptr<rune::storage::InMemoryLog> mem;
        std::unique_ptr<rr::PartitionReplica>       replica;
    };
    struct Effect {
        enum class Kind : uint8_t { Create, Drop, Config } kind = Kind::Create;
        rune::node::TopicSpec spec;     // Create
        rune::catalog::TopicConfig config;   // Create: the topic's full config (a view sink: the defaults)
        int32_t               id = -1;  // Drop, Config
        std::string           name;     // Drop
        std::vector<rune::catalog::ConfigChange> changes;   // Config
    };
    /// The fake control side of one broker (the control thread's TopicRegistry + its create / drop rules).
    struct Control {
        std::unique_ptr<rune::gateway::TopicRegistry> reg;
        std::deque<Effect>                            queue;     // posted by the applier, drained on the control pass
        std::vector<rune::node::TopicSpec>            pending;   // PendingCreateBehindDrop (FIFO)
        struct Retiring { int32_t id; std::string name; int passesLeft; };
        std::vector<Retiring>                         retiring;  // dropped topics whose dir is not renamed away yet
        std::string                                   dataDir;
        /// The control side's view of every topic's config (built from Create effects, changed by Config effects).
        std::map<int32_t, rune::catalog::TopicConfig> configs;
    };
    struct Broker {
        int32_t                               id = 0;
        bool                                  alive = true;
        bool                                  swallowEffects = false;
        int                                   halts = 0;
        std::unique_ptr<rune::storage::Log>   log;
        std::unique_ptr<HoldingLink>          link;
        std::unique_ptr<HarnessIsrChannel>    isrChannel;
        std::unique_ptr<rr::PartitionReplica> replica;
        SchemaRegistry                        schemas;
        ViewRegistry                          views;
        std::unique_ptr<AdminWorker>          admin;
        Control                               ctl;
        rr::NullPartitionTransport            dataLink;
        std::map<int32_t, std::unique_ptr<DataPartition>> data;   // topicId -> partition 0's data replica
        std::deque<std::size_t>               held;               // gated answers (indices into recs_), FIFO
        rune::node::TopicDirReconciliation    lastReconcile;      // the boot reconcile's result (open_)
        uint64_t                              startedTotal = 0, answeredTotal = 0;   // of the workers torn down so far
        uint64_t                              crashedUnanswered = 0;   // started by a crashed leader, never answered (kept across restarts)
        void tearDown() {
            if (admin) { startedTotal += admin->startedCount(); answeredTotal += admin->answeredCount(); }
            admin.reset();
            data.clear();
            replica.reset();
            isrChannel.reset();
            log.reset();
            ctl.reg.reset();
            ctl.queue.clear(); ctl.pending.clear(); ctl.retiring.clear(); ctl.configs.clear();
            held.clear();
        }
        ~Broker() { tearDown(); }
    };

    [[noreturn]] void fail_(const std::string& stage, const std::string& what) const {
        throw std::logic_error("[" + tag_ + "] " + stage + ": " + what);
    }
    Broker& broker_(int32_t b) { return *brokers_.at(static_cast<std::size_t>(b - 1)); }
    const Broker& broker_(int32_t b) const { return *brokers_.at(static_cast<std::size_t>(b - 1)); }

    template <class Pred>
    void until_(Pred&& p, const std::string& what, int cap = kCap) {
        for (int i = 0; i < cap; ++i) {
            if (p()) return;
            pump();
        }
        if (!p()) fail_("wait", what + " (pass cap " + std::to_string(cap) + " hit)");
    }

    [[nodiscard]] static rune::protocol::RequestId freshRidFor_(uint64_t& counter) { return {0xC1A5, ++counter}; }
    [[nodiscard]] rune::protocol::RequestId freshRid_() { return freshRidFor_(ridCounter_); }

    [[nodiscard]] std::string clientEndpoint_(int32_t b) const {
        return "aeron:udp?endpoint=broker-" + std::to_string(b) + "-client:9090";
    }
    [[nodiscard]] int brokerOfEndpoint_(const std::string& ep) const {
        for (int b = 1; b <= kBrokers; ++b) if (ep == clientEndpoint_(b)) return b;
        return -1;
    }
    /// The k-th broker that is not `leader` (ascending ids).
    [[nodiscard]] int followerOf_(int leader, int k) const {
        int n = 0;
        for (int b = 1; b <= kBrokers; ++b) {
            if (b == leader) continue;
            if (n++ == k) return b;
        }
        return -1;
    }
    [[nodiscard]] int otherFollowerForHalt_(int leader) const { return leader == 3 ? 2 : 3; }

    // ── the controller ───────────────────────────────────────────────────────
    /// Deliver the committed assignment as each live broker's role, and the route follows it.
    void commitAssignment_() {
        ft_.setCurrentLeader(cur_.leaderNode);
        for (auto& br : brokers_) {
            if (!br->alive) continue;
            AdminRoleMsg r;
            r.epoch = cur_.leaderEpoch;
            r.leaderId = cur_.leaderNode;
            r.partitionEpoch = cur_.partitionEpoch;
            if (br->id == cur_.leaderNode) {
                r.leader = true;
                r.isrCount = static_cast<int32_t>(cur_.isr.size());
                for (std::size_t i = 0; i < cur_.isr.size(); ++i) r.isr[i] = cur_.isr[i];
            }
            br->admin->onCatalogRole(r);
        }
    }
    /// A committed AlterIsr (the leader proposes, the controller commits): same leader epoch, a new partitionEpoch.
    void alterIsr_(std::vector<int32_t> isr) {
        cur_.isr = std::move(isr);
        ++cur_.partitionEpoch;
        commitAssignment_();
    }
    /// The controller's fencing pass (MetadataService::fenceIsrs_): the dead replica leaves the COMMITTED ISR.
    /// The catalog leader learns it as a committed assignment (same leader epoch, a new partitionEpoch) - it
    /// is never told by the harness directly.
    void shrinkIsr_(int32_t b) {
        if (std::find(cur_.isr.begin(), cur_.isr.end(), b) == cur_.isr.end()) return;
        if (b == cur_.leaderNode) fail_("isr", "the leader's own death: the cell moves the leadership first");
        std::vector<int32_t> isr;
        for (int32_t x : cur_.isr) if (x != b) isr.push_back(x);
        alterIsr_(std::move(isr));
    }
    /// The controller's AlterIsr handler (MetadataService::onAlterIsr, MetadataService.h "onAlterIsr"): the
    /// proposals the catalog leader queued are validated against the committed assignment and committed or
    /// answered with the production reject codes.  Bump / report requests never occur in the matrix (every
    /// role the harness commits is at a fresh epoch or a follower of a known leader) - they fail the cell.
    void controllerStep_() {
        while (!alterIsrQ_.empty()) {
            const rr::AlterIsrMsg m = alterIsrQ_.front();
            alterIsrQ_.pop_front();
            if (m.topicId != kCatalogTopicId || m.partition != kCatalogPartition) continue;
            if (m.leaderId < 1 || m.leaderId > kBrokers || !broker_(m.leaderId).alive) continue;   // its sender is gone
            if (m.isrCount == rr::AlterIsrMsg::kLeaderEpochBump)
                fail_("controller", "unexpected epoch-bump / report request from broker " + std::to_string(m.leaderId));
            auto reply = [&](int16_t code) {
                rr::AlterIsrResponseMsg r;
                r.topicId = m.topicId; r.partition = m.partition; r.leaderId = m.leaderId; r.leaderEpoch = m.leaderEpoch;
                r.partitionEpoch = cur_.partitionEpoch; r.errorCode = code;
                broker_(m.leaderId).admin->onCatalogAlterIsrResponse(r);
            };
            if (cur_.leaderNode != m.leaderId || cur_.leaderEpoch != static_cast<int32_t>(m.leaderEpoch)) {
                reply(rr::kAlterIsrFencedLeaderEpoch);
                continue;
            }
            if (m.partitionEpoch != cur_.partitionEpoch) { reply(rr::kAlterIsrInvalidUpdateVersion); continue; }
            std::vector<int32_t> isr;
            bool hasLeader = false, droppedIneligible = false, malformed = false;
            for (int i = 0; i < m.isrCount; ++i) {
                const int32_t n = m.isr[static_cast<std::size_t>(i)];
                if (n < 1 || n > kBrokers) { malformed = true; break; }
                if (n == m.leaderId) hasLeader = true;
                const bool added = std::find(cur_.isr.begin(), cur_.isr.end(), n) == cur_.isr.end();
                if (added && n != m.leaderId && !broker_(n).alive) { droppedIneligible = true; continue; }   // KIP-841
                isr.push_back(n);
            }
            if (malformed || !hasLeader) { reply(rr::kAlterIsrInvalidRequest); continue; }
            bool same = isr.size() == cur_.isr.size();
            for (int32_t n : isr) same = same && std::find(cur_.isr.begin(), cur_.isr.end(), n) != cur_.isr.end();
            if (same) { reply(droppedIneligible ? rr::kAlterIsrIneligibleReplica : rr::kAlterIsrOk); continue; }
            alterIsr_(std::move(isr));   // committed: every broker's role message carries the new ISR
        }
    }
    /// A returning broker re-enters the ISR the production way: it fetches until it holds the leader's log,
    /// the LEADER proposes it (validated in this leadership at >= the join floor), the controller commits the
    /// AlterIsr - the harness never commits the admission itself.  Only then can the committed prefix (which
    /// needs the ISR >= minIsr) reach it and its applier catch up.
    void admitWhenCaughtUp_(int32_t b, bool awaitApplied = true) {
        const int32_t leader = cur_.leaderNode;
        until_([&] { return broker_(b).replica->logEndOffset() >= broker_(leader).replica->logEndOffset(); },
               "broker " + std::to_string(b) + " never fetched up to the catalog leader's log end");
        until_([&] { return std::find(cur_.isr.begin(), cur_.isr.end(), b) != cur_.isr.end(); },
               "the leader never proposed the returning broker " + std::to_string(b) + " into the ISR");
        if (!awaitApplied) return;   // a halted applier applies nothing: only its replica rejoins the ISR
        until_([&] { return broker_(b).admin->applier().nextOffset() >= broker_(leader).replica->logEndOffset(); },
               "broker " + std::to_string(b) + " never applied the catalog leader's log");
    }

    // ── the fake gateway ─────────────────────────────────────────────────────
    enum class Route : uint8_t { LocalLeader, Remote, Unknown, Halted };
    [[nodiscard]] Route routeOf_(int32_t b) {
        if (broker_(b).admin->applier().halted()) return Route::Halted;
        if (cur_.leaderNode == b) return Route::LocalLeader;
        if (cur_.leaderNode >= 0) return Route::Remote;
        return Route::Unknown;
    }
    /// Returns the corr of the submission (>= 0), or -1 when the broker is down (nothing answers).
    int64_t gatewaySubmit_(int32_t b, DdlRequest req) {
        const int64_t corr = ++corr_;
        std::visit([&](auto& m) { m.corr = corr; m.session = kSession; }, req);
        if (!broker_(b).alive) return -1;
        switch (routeOf_(b)) {
        case Route::LocalLeader: broker_(b).admin->submit(std::move(req)); break;
        case Route::Remote:
            record_(b, corr, Answer{kCodeRedirect, -1, 0, clientEndpoint_(cur_.leaderNode), 0, -1, 0, Answer::Kind::Error});
            break;
        case Route::Unknown: record_(b, corr, Answer{kCodeNotLeader, -1, 0, {}, 0, -1, 0, Answer::Kind::Error}); break;
        case Route::Halted:
            record_(b, corr, Answer{kCodeInternal, -1, 0, "catalog apply halted", 0, -1, 0, Answer::Kind::Error});
            break;
        }
        return corr;
    }
    /// An answer leaves a broker: through its DDL answer gate (FIFO), mirrored from DdlAnswerGate.
    void record_(int32_t b, int64_t corr, Answer a) {
        ClusterAnswer r;
        r.broker = b;
        if (a.code == 0) {
            switch (a.kind) {
            case Answer::Kind::Define: case Answer::Kind::CreateView: r.gateTopic = a.topicId; break;
            case Answer::Kind::Ddl: r.gateTopic = a.gateTopicId; break;
            default: break;
            }
        }
        r.a = std::move(a);
        recs_.push_back(std::move(r));
        const std::size_t idx = recs_.size() - 1;
        byCorr_[corr].push_back(idx);
        Broker& br = broker_(b);
        if (br.held.empty() && (recs_[idx].gateTopic < 0 || serves_(br, recs_[idx].gateTopic))) {
            recs_[idx].visible = true;
            recs_[idx].servedAtRelease = recs_[idx].gateTopic < 0 || serves_(br, recs_[idx].gateTopic);
        } else {
            br.held.push_back(idx);
        }
    }
    void releaseGated_(Broker& br) {
        while (!br.held.empty()) {
            ClusterAnswer& r = recs_[br.held.front()];
            if (r.gateTopic >= 0 && !serves_(br, r.gateTopic)) break;
            r.visible = true;
            r.servedAtRelease = r.gateTopic < 0 || serves_(br, r.gateTopic);
            br.held.pop_front();
        }
    }

    // ── the client model ─────────────────────────────────────────────────────
    ClientOp& startOp_(DdlRequest req, int target, const std::string& statement) {
        auto op = std::make_unique<ClientOp>();
        op->id = static_cast<int>(clients_.size());
        op->rid = std::visit(
            [](auto& m) -> rune::protocol::RequestId {
                if constexpr (requires { m.meta; }) return m.meta.requestId;
                else return {};
            },
            req);
        op->base = std::move(req);
        op->statement = statement;
        op->target = target;
        op->deadlineNs = now_ + kDdlDeadlineNs;
        clients_.push_back(std::move(op));
        return *clients_.back();
    }
    void stepClient_(ClientOp& op) {
        if (std::holds_alternative<ClientOp::AwaitingResend>(op.st)) {
            if (now_ < std::get<ClientOp::AwaitingResend>(op.st).atNs) return;
            op.st = ClientOp::Unsent{};
        }
        if (std::holds_alternative<ClientOp::Unsent>(op.st)) {
            const int64_t corr = gatewaySubmit_(op.target, op.base);
            subs_.push_back(Submission{op.id, op.target, corr});
            op.st = ClientOp::InFlight{op.target, corr};
            return;   // the answer is read on a later pass
        }
        if (const auto* f = std::get_if<ClientOp::InFlight>(&op.st)) {
            const auto it = byCorr_.find(f->corr);
            if (it != byCorr_.end()) {
                for (std::size_t idx : it->second) {
                    if (!recs_[idx].visible || recs_[idx].consumed) continue;
                    recs_[idx].consumed = true;
                    onAnswer_(op, recs_[idx].a.code, recs_[idx].a.detail, recs_[idx].a.kind);
                    return;
                }
            }
            if (now_ >= op.deadlineNs) { op.st = ClientOp::Finished{kClientTimeout}; ++op.terminalResults; }
        }
    }
    void onAnswer_(ClientOp& op, int code, const std::string& message, Answer::Kind kind) {
        op.codes.push_back(code);
        if (op.firstCode == -1) {
            op.firstCode = code;
            op.firstKind = kind;
            op.firstMessage = message;
            op.captureNext = code == kCodeRedirect;
        } else if (op.captureNext) {
            op.afterRedirectCode = code;
            op.captureNext = false;
        }
        auto finish = [&](int c) { op.st = ClientOp::Finished{c}; ++op.terminalResults; };
        if (code == kCodeRedirect) {
            if (message.empty()) { onNotLeader_(op); return; }   // nothing to follow: retriable as NotLeader
            if (++op.redirects > kMaxDdlRedirects) { finish(kCodeRedirect); return; }
            if (op.beforeFollowRedirect) op.beforeFollowRedirect();   // the hook is idempotent or re-decides each hop
            op.target = brokerOfEndpoint_(message);
            op.st = ClientOp::Unsent{};
            return;
        }
        if (code == kCodeNotLeader) { onNotLeader_(op); return; }
        finish(code);
    }
    void onNotLeader_(ClientOp& op) {
        if (op.budgetEndNs == 0) {
            op.budgetEndNs = std::min(op.deadlineNs, now_ + kDdlRetryBudgetNs);
            op.backoffNs = kDdlInitialBackoffNs;
        } else {
            op.backoffNs = std::min(op.backoffNs * 2, kDdlMaxBackoffNs);
        }
        if (now_ >= op.budgetEndNs) { op.st = ClientOp::Finished{kCodeNotLeader}; ++op.terminalResults; return; }
        op.st = ClientOp::AwaitingResend{std::min(now_ + op.backoffNs, op.budgetEndNs)};
    }
    /// Pump until every op finished; idle waits (only resends pending) jump the clock.
    void runOps_(std::vector<ClientOp*> ops, const std::string& stage) {
        for (int i = 0; i < kCap; ++i) {
            bool allDone = true;
            int64_t earliest = INT64_MAX;
            bool onlyResends = true;
            for (ClientOp* op : ops) {
                if (op->done()) continue;
                allDone = false;
                if (const auto* r = std::get_if<ClientOp::AwaitingResend>(&op->st)) earliest = std::min(earliest, r->atNs);
                else onlyResends = false;
            }
            if (allDone) return;
            if (onlyResends && earliest > now_ + 2'000'000) now_ = earliest - 1'000'000;   // jump the idle wait
            pump();
        }
        fail_(stage, "the client ops never finished (pass cap " + std::to_string(kCap) + " hit)");
    }
    /// A DDL through the leader with the client model; fails the cell unless it ends Ok.
    void doDdl_(DdlRequest req, const std::string& stage) {
        ClientOp& op = startOp_(std::move(req), cur_.leaderNode, stage);
        runOps_({&op}, stage);
        if (op.finalCode() != 0) fail_(stage, "the DDL ended " + std::to_string(op.finalCode()));
    }

    // ── requests ─────────────────────────────────────────────────────────────
    [[nodiscard]] DdlMeta metaFor_(const std::string& statement, const rune::protocol::RequestId& rid) const {
        DdlMeta m;
        m.statement = statement;
        m.sessionId = kSession;
        m.sourceAddress = "aeron:udp?endpoint=matrix-client:40123";
        m.clientName = "matrix";
        m.requestId = rid;
        return m;
    }
    [[nodiscard]] const rune::catalog::CatalogState& leaderState_() { return broker_(cur_.leaderNode).admin->applier().state(); }
    [[nodiscard]] static bool isViewKind_(cm::Kind k) {
        return k == cm::Kind::CreateView || k == cm::Kind::AlterView || k == cm::Kind::DropView;
    }
    [[nodiscard]] static bool isCreationKind_(cm::Kind k) { return k == cm::Kind::CreateTopic || k == cm::Kind::CreateView; }
    [[nodiscard]] static std::string targetNameOf_(cm::Kind k) { return isViewKind_(k) ? kView : kTopic; }
    [[nodiscard]] DdlSpec specOf_(cm::Kind k, bool typedCreate) const {
        const cm::Persist p = persist_;
        switch (k) {
        case cm::Kind::CreateTopic:       return {DdlWhat::DefineTopic, kTopic, typedCreate, p};
        case cm::Kind::DropTopic:         return {DdlWhat::DropTopic, kTopic, false, p};
        case cm::Kind::AlterTopicConfig:  return {DdlWhat::AlterConfig, kTopic, false, p};
        case cm::Kind::CreateSchema:      return {DdlWhat::DefineTopic, kTopic, true, p};
        case cm::Kind::AlterSchema:       return {DdlWhat::AlterSchema, kTopic, true, p};
        case cm::Kind::SetCompatibility:  return {DdlWhat::SetCompat, kTopic, true, p};
        case cm::Kind::CreateView:        return {DdlWhat::CreateView, kView, true, p};
        case cm::Kind::AlterView:         return {DdlWhat::AlterView, kView, true, p};
        case cm::Kind::DropView:          return {DdlWhat::DropView, kView, true, p};
        }
        return {DdlWhat::DefineTopic, kTopic, false, p};
    }
    [[nodiscard]] DdlSpec dropSpecOf_(cm::Kind k) const {
        return isViewKind_(k) ? DdlSpec{DdlWhat::DropView, kView, true, persist_} : DdlSpec{DdlWhat::DropTopic, kTopic, false, persist_};
    }
    [[nodiscard]] std::vector<uint8_t> plan_(const std::string& view, const std::string& source, bool extraColumn) {
        namespace rv = rune::view;
        rv::ViewPlan p;
        p.viewName = view;
        p.key      = "id";
        const rune::catalog::TopicObject* t = leaderState_().topicByName(source);
        p.sources.push_back(rv::SourceSpec{t ? t->topicId : 999'999, "id"});
        p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::Int64, 0, "id", rv::MergeRule::Replace});
        p.output.push_back(rv::AuthoredField{"hp", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
        if (extraColumn)
            p.output.push_back(rv::AuthoredField{"hp2", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
        return rv::serializeAuthored(p);
    }
    /// The request of `spec`, built from the CURRENT applied state of the catalog leader (expectedVersions).
    [[nodiscard]] DdlRequest makeRequest_(const DdlSpec& spec, const rune::protocol::RequestId& rid,
                                          const std::string& statement = {}) {
        const DdlMeta meta = metaFor_(statement.empty() ? stmtOf_(spec) : statement, rid);
        const uint8_t persistence = spec.persist == cm::Persist::Transient ? 1 : 0;
        const auto& st = leaderState_();
        switch (spec.what) {
        case DdlWhat::DefineTopic: {
            DefineTopicMsg m;
            m.session = kSession; m.name = spec.name; m.partitionCount = 1;
            m.keyField = spec.typed ? "id" : "";
            if (spec.typed) m.blob = CatalogDdlRig::descriptor(spec.name);
            m.persistence = persistence;
            m.meta = meta;
            return DdlRequest{std::move(m)};
        }
        case DdlWhat::DropTopic: {
            DropTopicMsg m; m.session = kSession; m.name = spec.name; m.meta = meta;
            return DdlRequest{std::move(m)};
        }
        case DdlWhat::AlterConfig: {
            AlterTopicConfigMsg m; m.session = kSession; m.name = spec.name; m.meta = meta;
            const auto* t = st.topicByName(spec.name);
            m.expectedVersion = t != nullptr ? t->version : 0;
            m.changes = {{rune::catalog::ConfigKey::RetentionMs, 60'000}};
            return DdlRequest{std::move(m)};
        }
        case DdlWhat::AlterSchema: case DdlWhat::SetCompat: {
            AlterTopicMsg m; m.session = kSession; m.topicName = spec.name; m.meta = meta;
            const auto* t = st.topicByName(spec.name);
            m.expectedVersion = t != nullptr && t->schema ? t->schema->schemaVersion : 0;
            rune::codec::AlterOp op{};
            if (spec.what == DdlWhat::AlterSchema) {
                op.type = rune::codec::AlterOpType::Add;
                op.name = "extra";
                op.primType = rune::codec::PrimType::Int32;
            } else {
                op.type = rune::codec::AlterOpType::SetCompatibility;
                op.mode = rune::codec::CompatMode::Forward;
            }
            m.opsBlob = rune::codec::encodeAlterOps({op});
            return DdlRequest{std::move(m)};
        }
        case DdlWhat::CreateView: {
            CreateViewMsg m; m.session = kSession; m.viewName = spec.name; m.keyField = "id"; m.persistence = persistence;
            m.planBlob = plan_(spec.name, kSource, false);
            m.meta = meta;
            return DdlRequest{std::move(m)};
        }
        case DdlWhat::AlterView: {
            AlterViewMsg m; m.session = kSession; m.name = spec.name; m.keyField = "id"; m.persistence = persistence;
            const auto* v = st.viewByName(spec.name);
            m.expectedVersion = v != nullptr ? v->version : 0;
            m.planBlob = plan_(spec.name, kSource, true);
            m.meta = meta;
            return DdlRequest{std::move(m)};
        }
        case DdlWhat::DropView: {
            DropViewMsg m; m.session = kSession; m.name = spec.name; m.meta = meta;
            return DdlRequest{std::move(m)};
        }
        }
        throw std::logic_error("makeRequest_: unreachable");
    }
    [[nodiscard]] std::string stmtOf_(const DdlSpec& spec) const {
        static constexpr const char* n[] = {"CREATE TOPIC", "DROP TOPIC", "ALTER TOPIC CONFIG", "ALTER TOPIC", "SET COMPATIBILITY",
                                            "CREATE VIEW", "ALTER VIEW", "DROP VIEW"};
        return std::string(n[static_cast<int>(spec.what)]) + " " + spec.name;
    }
    /// The target object the kind's precondition builds (excluding the baseline topic).
    void buildTarget_(cm::Kind kind, cm::Persist persist, bool typedCreate) {
        auto topic = [&](bool typed) {
            doDdl_(makeRequest_(DdlSpec{DdlWhat::DefineTopic, kTopic, typed, persist}, freshRid_()), "precondition topic");
        };
        auto source = [&] {
            doDdl_(makeRequest_(DdlSpec{DdlWhat::DefineTopic, kSource, true, persist}, freshRid_()), "precondition source");
        };
        auto view = [&] {
            doDdl_(makeRequest_(DdlSpec{DdlWhat::CreateView, kView, true, persist}, freshRid_()), "precondition view");
        };
        switch (kind) {
        case cm::Kind::CreateTopic:       break;                                            // the name is free
        case cm::Kind::DropTopic:         topic(false); break;
        case cm::Kind::AlterTopicConfig:  topic(false); break;
        case cm::Kind::CreateSchema:      topic(false); break;
        case cm::Kind::AlterSchema:       topic(true); break;
        case cm::Kind::SetCompatibility:  topic(true); break;
        case cm::Kind::CreateView:        if (!leaderState_().topicByName(kSource)) source(); break;
        case cm::Kind::AlterView:
        case cm::Kind::DropView:          if (!leaderState_().topicByName(kSource)) source(); view(); break;
        }
        (void)typedCreate;
    }
    /// The target object EXISTING (DropAndReuseName's incarnations): the precondition for the kinds that
    /// act on an object, the object itself for the kinds that create it.
    void buildExisting_(cm::Kind kind, cm::Persist persist, bool typedCreate) {
        if (kind == cm::Kind::CreateTopic) {
            doDdl_(makeRequest_(DdlSpec{DdlWhat::DefineTopic, kTopic, typedCreate, persist}, freshRid_()), "incarnation topic");
        } else if (kind == cm::Kind::CreateView) {
            doDdl_(makeRequest_(DdlSpec{DdlWhat::CreateView, kView, true, persist}, freshRid_()), "incarnation view");
        } else {
            buildTarget_(kind, persist, typedCreate);
        }
    }
    [[nodiscard]] uint64_t targetObjectId_(cm::Kind k) {
        if (k == cm::Kind::CreateTopic) { const auto* t = leaderState_().topicByName(kTopic); return t ? t->objectId : 0; }
        if (k == cm::Kind::CreateView) { const auto* v = leaderState_().viewByName(kView); return v ? v->objectId : 0; }
        if (isViewKind_(k)) { const auto* v = leaderState_().viewByName(kView); return v ? v->objectId : 0; }
        const auto* t = leaderState_().topicByName(kTopic);
        return t ? t->objectId : 0;
    }
    [[nodiscard]] uint32_t targetVersion_(cm::Kind k) {
        if (isViewKind_(k)) { const auto* v = leaderState_().viewByName(kView); return v ? v->version : 0; }
        const auto* t = leaderState_().topicByName(kTopic);
        return t ? t->version : 0;
    }

    // ── results ──────────────────────────────────────────────────────────────
    [[nodiscard]] int64_t eventOffsetOf_(const rune::protocol::RequestId& rid) {
        for (const auto& r : readLog_(*broker_(cur_.leaderNode).log))
            if (r.event.principal.requestId == rid) return r.offset;
        return -1;
    }
    [[nodiscard]] bool checkpointReaches_(int32_t b, int64_t offset) {
        const auto c = readCatalogCheckpoint(broker_(b).admin->applier().catalogDir());
        return c && c->appliedOffset >= offset;
    }
    [[nodiscard]] std::optional<rune::catalog::RecordedEvent> eventFor_(const rune::protocol::RequestId& rid) {
        for (const auto& r : readLog_(*broker_(cur_.leaderNode).log))
            if (r.event.principal.requestId == rid) return r;
        return std::nullopt;
    }
    /// A retry that is answered from the dedupe record returns the ORIGINAL result: the OK it got names exactly
    /// what the logged event did (objectId + version for a DdlResponse; the topic / sink id for the rest).
    [[nodiscard]] bool okAnswerMatchesLog_(const ClientOp& op) {
        const auto e = eventFor_(op.rid);
        if (!e) return false;
        for (const Submission& s : subs_) {
            if (s.op != op.id || s.corr < 0) continue;
            const auto it = byCorr_.find(s.corr);
            if (it == byCorr_.end()) continue;
            for (std::size_t idx : it->second) {
                const Answer& a = recs_[idx].a;
                if (!recs_[idx].visible || a.code != 0) continue;
                if (a.kind == Answer::Kind::Ddl) return a.objectId == e->event.objectId && a.version == e->event.newVersion;
                const auto& st = leaderState_();
                if (const auto* t = st.topicByObjectId(e->event.objectId)) return a.topicId == t->topicId;
                if (const auto* v = st.viewByObjectId(e->event.objectId)) return a.topicId == v->def.viewTopicId;
                return true;   // a drop answered by a non-Ddl template cannot occur
            }
        }
        return false;
    }
    [[nodiscard]] uint64_t objectIdOfOp_(const ClientOp& op) {
        const auto e = eventFor_(op.rid);
        return e ? e->event.objectId : 0;
    }
    void fillCommon_(CellRun& run, ClientOp& op) {
        run.first = CellAnswerFirst{op.firstCode, op.firstMessage, op.firstKind};
        run.afterRedirectCode = op.afterRedirectCode;
        run.finalCode = op.finalCode();
    }
    void finish_(CellRun& run, const std::vector<ClientOp*>& cellOps) {
        // Whatever a cell stopped, returns; holds end; placement flows; then everything settles.
        releaseHighWatermark();
        holdPlacement(false);
        for (int b = 1; b <= kBrokers; ++b) if (!broker_(b).alive) restartBroker(b);
        (void)awaitDigestsEqual(/*excludeHalted=*/true);
        // Request ledgers: a requestId is OK when any of its sends was answered OK, else refused.
        std::set<std::pair<uint64_t, uint64_t>> ok, refused;
        bool single = true;
        for (const ClientOp* op : cellOps) {
            const auto key = std::make_pair(op->rid.hi, op->rid.lo);
            if (op->terminalResults != 1) single = false;   // one terminal result per client op
            if (std::find(op->codes.begin(), op->codes.end(), 0) != op->codes.end()) { ok.insert(key); refused.erase(key); }
            else if (!ok.contains(key)) refused.insert(key);
        }
        // every submission got exactly ONE answer (a broker's or the gateway's)
        for (const Submission& s : subs_) {
            if (s.corr < 0) { single = false; continue; }
            const auto it = byCorr_.find(s.corr);
            if (it == byCorr_.end() || it->second.size() != 1) single = false;
        }
        for (const auto& [hi, lo] : ok) run.okRequestIds.push_back({hi, lo});
        for (const auto& [hi, lo] : refused) run.refusedRequestIds.push_back({hi, lo});
        run.everyRequestHadOneTerminalResult = single;
        for (const ClientOp* op : cellOps) run.okCount += op->finalCode() == 0 ? 1 : 0;
        // The object the OK'd op acted on, from the event the log recorded for its requestId.
        for (const auto& rid : run.okRequestIds) {
            if (const auto e = eventFor_(rid)) {
                run.objectIdAfter = e->event.objectId;
                run.newVersionAfter = e->event.newVersion;
                break;
            }
        }
    }
    /// RestartMidApply x Transient, for the restarted broker `b`: every transient topic / view sink of its applied
    /// catalog is registered and serving, holds no data (a transient log is rebuilt empty after a restart), and a
    /// transient view's definition is back in its ViewRegistry (the re-fold source).  The restarted broker is never
    /// the placement leader here, so its data partition is absent (nothing was produced to it) or empty.
    [[nodiscard]] bool transientEmptyAfterRestart_(int32_t b) {
        Broker& br = broker_(b);
        const auto& st = br.admin->applier().state();
        auto emptyServing = [&](int32_t id) {
            if (!serves_(br, id)) return false;
            const auto it = br.data.find(id);
            return it == br.data.end() || it->second->replica->logEndOffset() == 0;
        };
        for (const auto* t : st.topics())
            if (t->persistence == rune::catalog::Persistence::Transient && !emptyServing(t->topicId)) return false;
        for (const auto* v : st.views()) {
            if (v->def.persistence != rune::catalog::Persistence::Transient) continue;
            if (!emptyServing(v->def.viewTopicId) || br.views.byName(v->name) == nullptr) return false;
        }
        return true;
    }
    /// R6: over the leader's log, history(name) holds events of BOTH objectIds.
    [[nodiscard]] bool historyHoldsObjects_(const std::string& name, uint64_t before, uint64_t after) {
        const auto log = readLog_(*broker_(cur_.leaderNode).log);
        rune::catalog::HistoryFilter f;
        f.name = name;
        bool sawBefore = false, sawAfter = false;
        for (const auto* h : rune::catalog::history(std::span<const rune::catalog::RecordedEvent>(log), f)) {
            sawBefore = sawBefore || h->event.objectId == before;
            sawAfter  = sawAfter  || h->event.objectId == after;
        }
        return before != after && sawBefore && sawAfter;
    }
    /// `broker` answered one of `op`'s sends with `code` (a visible answer).
    [[nodiscard]] bool answeredByWith_(const ClientOp& op, int broker, int code) {
        for (const Submission& s : subs_) {
            if (s.op != op.id || s.broker != broker || s.corr < 0) continue;
            const auto it = byCorr_.find(s.corr);
            if (it == byCorr_.end()) continue;
            for (std::size_t idx : it->second)
                if (recs_[idx].visible && recs_[idx].a.code == code) return true;
        }
        return false;
    }
    [[nodiscard]] bool anyVisibleAnswerFor_(const ClientOp& op) {
        for (const Submission& s : subs_) {
            if (s.op != op.id) continue;
            const auto it = byCorr_.find(s.corr);
            if (it == byCorr_.end()) continue;
            for (std::size_t idx : it->second) if (recs_[idx].visible && recs_[idx].a.code == 0) return true;
        }
        return false;
    }
    [[nodiscard]] bool okServedAtRelease_(const ClientOp& op) {
        for (const Submission& s : subs_) {
            if (s.op != op.id) continue;
            const auto it = byCorr_.find(s.corr);
            if (it == byCorr_.end()) continue;
            for (std::size_t idx : it->second) if (recs_[idx].visible && recs_[idx].a.code == 0) return recs_[idx].servedAtRelease;
        }
        return false;
    }

    // ── the fake control side ────────────────────────────────────────────────
    [[nodiscard]] static rune::node::TopicSpec specFrom_(const std::string& name, int32_t id, int32_t partitions,
                                                        rune::catalog::Persistence p) {
        rune::node::TopicSpec s;
        s.name = name;
        s.topicId = id;
        s.partitionCount = partitions;
        s.persistence = p == rune::catalog::Persistence::Transient ? PersistenceMode::Transient : PersistenceMode::Disk;
        s.segmentRollBytes = 1 << 20;   // a small segment: a cell opens several logs
        return s;
    }
    void post_(Broker& br, bool live, Effect e) {
        if (live && br.swallowEffects) return;   // the process died after the checkpoint, before this side effect
        br.ctl.queue.push_back(std::move(e));
    }
    [[nodiscard]] CatalogEffects effectsFor_(Broker& br, bool live) {
        CatalogEffects fx;
        Broker* self = &br;
        fx.topicApplied = [this, self, live](const rune::catalog::TopicObject& t) {
            Effect e; e.kind = Effect::Kind::Create;
            e.spec = specFrom_(t.name, t.topicId, t.partitions, t.persistence);
            e.config = t.config;
            post_(*self, live, std::move(e));
        };
        fx.topicConfig = [this, self, live](int32_t id, std::span<const rune::catalog::ConfigChange> changes) {
            Effect e; e.kind = Effect::Kind::Config; e.id = id;
            e.changes.assign(changes.begin(), changes.end());
            post_(*self, live, std::move(e));
        };
        fx.sinkApplied = [this, self, live](const SinkSpec& s) {
            Effect e; e.kind = Effect::Kind::Create;
            e.spec = specFrom_(s.name, s.topicId, s.partitions, s.persistence);
            post_(*self, live, std::move(e));
        };
        if (live) {
            fx.topicDropped = [this, self](int32_t id, const std::string& name) {
                Effect e; e.kind = Effect::Kind::Drop; e.id = id; e.name = name;
                post_(*self, true, std::move(e));
            };
            fx.halted = [self](const rune::catalog::ApplyError&) { ++self->halts; };
        } else {
            fx.halted = [self](const rune::catalog::ApplyError&) { ++self->halts; };
        }
        return fx;
    }
    [[nodiscard]] bool nameHeldByRetiring_(const Control& c, const std::string& name) const {
        // ASCII-folded, as BrokerNode::catalogNameHeldByDrop_ (Task 8 fix round 1 M4): on a case-insensitive
        // filesystem a case-variant name shares dataDir/topics/<name>/ with the dropped one.
        for (const auto& r : c.retiring) if (r.name == name || rune::catalog::topicNamesFoldEqual(r.name, name)) return true;
        return false;
    }
    void runCreate_(Broker& br, const rune::node::TopicSpec& spec) {
        (void)br.ctl.reg->createTopicAt(spec.topicId, spec);   // idempotent (same id + name); refused: logged
    }
    /// The control pass of one broker: BrokerNode::onCatalogTopicApplied_ / dropRuntimeTopic_ in miniature.
    void controlPass_(Broker& br) {
        Control& c = br.ctl;
        while (!c.queue.empty()) {
            Effect e = std::move(c.queue.front());
            c.queue.pop_front();
            if (e.kind == Effect::Kind::Config) {
                const auto cfg = c.configs.find(e.id);
                if (cfg == c.configs.end()) fail_("control", "a config effect for topic id " + std::to_string(e.id) + " the control side never created");
                for (const auto& ch : e.changes) {
                    switch (ch.key) {
                    case rune::catalog::ConfigKey::RetentionMs:       cfg->second.retentionMs = ch.value; break;
                    case rune::catalog::ConfigKey::DeleteRetentionMs: cfg->second.deleteRetentionMs = ch.value; break;
                    default: fail_("control", "the stress never sends config key " + std::to_string(static_cast<int>(ch.key)));
                    }
                }
                continue;
            }
            if (e.kind == Effect::Kind::Create) {
                c.configs[e.spec.topicId] = e.config;   // also for a create that waits behind a drop
                // Step 3 of the real handler: a create whose name a dropped topic still holds (its dir not
                // renamed away yet) waits — FIFO per name, behind any create of that name already waiting.
                bool behind = nameHeldByRetiring_(c, e.spec.name);
                if (!behind) {
                    const auto holder = c.reg->topicIdForName(e.spec.name);
                    const auto folded = c.reg->topicIdForFoldedName(e.spec.name);
                    behind = (holder && *holder != e.spec.topicId) || (folded && *folded != e.spec.topicId);
                }
                bool duplicate = false;
                for (const auto& p : c.pending) {
                    duplicate = duplicate || p.topicId == e.spec.topicId;   // the same effect again: idempotent
                    behind = behind || p.name == e.spec.name || rune::catalog::topicNamesFoldEqual(p.name, e.spec.name);
                }
                if (duplicate) continue;
                if (behind) {
                    (void)c.reg->reserveTopicAt(e.spec.topicId, e.spec.name);
                    c.pending.push_back(e.spec);
                } else {
                    runCreate_(br, e.spec);
                }
            } else {
                c.configs.erase(e.id);
                const auto before = c.pending.size();
                std::erase_if(c.pending, [&](const rune::node::TopicSpec& p) { return p.topicId == e.id; });
                if (c.pending.size() != before) { (void)c.reg->dropTopic(e.id); continue; }
                const rune::node::TopicSpec* spec = c.reg->topic(e.id);
                if (spec == nullptr) { (void)c.reg->dropTopic(e.id); continue; }   // unknown / gap / already dropped
                const std::string name = spec->name;
                br.data.erase(e.id);
                (void)c.reg->dropTopic(e.id);
                c.retiring.push_back(Control::Retiring{e.id, name, 2});
            }
        }
        // The retire sequence: the dropped topic's dir is renamed away (after the replicas let go), then
        // the creates that waited on its name run — FIFO.
        if (!holdRetire_) for (auto& r : c.retiring) if (r.passesLeft > 0) --r.passesLeft;   // held: the dir is never renamed away
        for (auto it = c.retiring.begin(); it != c.retiring.end();) {
            if (it->passesLeft > 0) { ++it; continue; }
            (void)c.reg->releaseDroppedLogs(it->id);
            const auto dir = std::filesystem::path(c.dataDir) / "topics" / it->name;
            std::error_code ec;
            if (std::filesystem::exists(dir, ec))
                std::filesystem::rename(dir, std::filesystem::path(c.dataDir) / "topics" / (".retired-" + std::to_string(it->id)), ec);
            it = c.retiring.erase(it);
        }
        for (auto it = c.pending.begin(); it != c.pending.end();) {
            bool held = nameHeldByRetiring_(c, it->name);
            for (auto jt = c.pending.begin(); jt != it && !held; ++jt)
                held = jt->name == it->name || rune::catalog::topicNamesFoldEqual(jt->name, it->name);
            if (held) { ++it; continue; }
            runCreate_(br, *it);
            it = c.pending.erase(it);
        }
    }
    void controlStep_() {
        for (auto& br : brokers_) if (br->alive) controlPass_(*br);
        // The controller commits placements for topics some broker applied (held while a cell races them).
        if (!placementHeld_) {
            for (auto& br : brokers_) {
                if (!br->alive || !br->ctl.reg) continue;
                for (int32_t id = 0; id < br->ctl.reg->topicCount(); ++id)
                    if (br->ctl.reg->state(id) == rune::gateway::TopicRegistry::TopicState::Serving) placed_.insert(id);
            }
        }
        for (auto& br : brokers_) if (br->alive) releaseGated_(*br);
    }
    void settleControl_() {
        for (int i = 0; i < kCap; ++i) {
            bool quiet = true;
            for (auto& br : brokers_)
                if (br->alive && (!br->ctl.queue.empty() || !br->ctl.retiring.empty() || !br->ctl.pending.empty())) quiet = false;
            if (quiet) return;
            pump();
        }
        fail_("settle", "the control side never went quiet");
    }
    [[nodiscard]] bool serves_(const Broker& br, int32_t topicId) const {
        return br.ctl.reg && br.ctl.reg->state(topicId) == rune::gateway::TopicRegistry::TopicState::Serving &&
               placed_.contains(topicId);
    }
    [[nodiscard]] bool regServes_(int32_t b, int32_t topicId) const { return serves_(broker_(b), topicId); }
    [[nodiscard]] bool registryHolds_(int32_t b, int32_t topicId) const {
        const Broker& br = broker_(b);
        return br.ctl.reg && br.ctl.reg->state(topicId) == rune::gateway::TopicRegistry::TopicState::Serving;
    }
    [[nodiscard]] std::optional<int32_t> appliedTopicId_(int32_t b, const std::string& name) {
        const auto& st = broker_(b).admin->applier().state();
        if (const auto* t = st.topicByName(name)) return t->topicId;
        if (const auto* v = st.viewByName(name)) return v->def.viewTopicId;
        return std::nullopt;
    }
    void awaitServing_(const std::string& topic) {
        until_([&] { return serving(cur_.leaderNode, topic); }, "the topic '" + topic + "' never served on the leader's broker");
    }
    rr::PartitionReplica& dataReplica_(Broker& br, const rune::catalog::TopicObject& t) {
        auto& slot = br.data[t.topicId];
        if (slot) return *slot->replica;
        slot = std::make_unique<DataPartition>();
        rr::PartitionConfig c;
        c.topicId = t.topicId; c.partitionId = 0; c.selfId = br.id; c.minIsr = 1;
        const bool leads = br.id == placementLeader_;
        const std::vector<rr::NodeId> replicas{1, 2, 3};
        const std::vector<rr::NodeId> isr{placementLeader_};
        if (t.persistence == rune::catalog::Persistence::Transient) {
            slot->mem = std::make_unique<rune::storage::InMemoryLog>(1 << 20, 1024);
            slot->replica = std::make_unique<rr::PartitionReplica>(c, leads ? rr::Role::Leader : rr::Role::Follower, 1,
                                                                   replicas, isr, slot->mem.get(), &br.dataLink);
        } else {
            rune::storage::Log* log = br.ctl.reg->servableLog(t.topicId, 0);
            if (log == nullptr) fail_("data", "a serving disk topic has no log");
            slot->replica = std::make_unique<rr::PartitionReplica>(c, leads ? rr::Role::Leader : rr::Role::Follower, 1,
                                                                   replicas, isr, log, &br.dataLink);
        }
        return *slot->replica;
    }

    // ── digests ──────────────────────────────────────────────────────────────
    [[nodiscard]] bool digestsEqual_(bool excludeHalted) {
        std::optional<int64_t> leo;
        std::optional<uint64_t> digest;
        std::optional<int64_t> applied;
        for (const auto& br : brokers_) {
            if (!br->alive) continue;
            if (excludeHalted && br->admin->applier().halted()) continue;
            const int64_t l = br->replica->logEndOffset();
            if (leo && *leo != l) return false;
            leo = l;
            if (br->admin->applier().nextOffset() != l) return false;
            const uint64_t d = br->admin->applier().state().digest();
            const int64_t a = br->admin->applier().state().appliedOffset();
            if (digest && *digest != d) return false;
            if (applied && *applied != a) return false;
            digest = d; applied = a;
        }
        return digest.has_value();
    }

    static std::vector<rune::catalog::RecordedEvent> readLog_(rune::storage::Log& log) {
        std::vector<rune::catalog::RecordedEvent> out;
        int64_t next = 0;
        const int64_t leo = log.logEndOffset();
        while (next < leo) {
            const std::vector<uint8_t> data = log.read(next, 1 << 22);
            if (data.empty()) break;
            std::size_t pos = 0;
            const int64_t before = next;
            while (pos < data.size()) {
                rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
                if (!reader.ensureValid()) break;
                const auto tl = static_cast<std::size_t>(reader.totalLength());
                reader.forEachRecord([&](std::string_view, std::string_view val, int64_t off, int64_t) {
                    if (off < next) return;
                    auto ev = rune::catalog::decode(
                        std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()), val.size()));
                    if (!ev) throw std::logic_error("CatalogClusterHarness: a logged event does not decode");
                    out.push_back(rune::catalog::RecordedEvent{off, std::move(*ev)});
                    next = off + 1;
                });
                if (tl == 0) break;
                pos += tl;
            }
            if (next == before) break;
        }
        return out;
    }

    std::unique_ptr<Broker> open_(int32_t id) {
        auto br = std::make_unique<Broker>();
        br->id = id;
        const auto brokerDir = root_ / ("b" + std::to_string(id));
        const auto dir = brokerDir / "__catalog";
        std::filesystem::create_directories(dir);
        br->log = std::make_unique<rune::storage::Log>(dir.string());
        br->log->recover();
        br->link = std::make_unique<HoldingLink>(ft_, hold_, id);
        NodeConfig nc;
        nc.nodeId = id;
        // ISR changes are the controller's (controllerStep_), never a lag shrink across the clock jumps (the
        // lag rule is out of scope here: the fencing commit is the controller's, see shrinkIsr_).
        nc.replicaLagMaxMs = 3'600'000;
        nc.fetchIntervalNs = 2'000'000;
        const std::vector<rr::NodeId> all{1, 2, 3};
        br->replica = std::make_unique<rr::PartitionReplica>(makeCatalogPartitionConfig(nc, kBrokers), rr::Role::Follower, 0,
                                                             all, all, br->log.get(), br->link.get());
        // Controller-managed ISR exactly like production (BrokerNode.cpp: setIsrChannel before any drive): the
        // replica PROPOSES ISR changes; the harness's controller step commits them (controllerStep_).
        br->isrChannel = std::make_unique<HarnessIsrChannel>(alterIsrQ_);
        br->replica->setIsrChannel(br->isrChannel.get());
        ft_.registerReplica(id, br->replica.get());

        br->ctl.dataDir = brokerDir.string();

        Broker* self = br.get();
        auto record = [this, self](int64_t corr, Answer a) { record_(self->id, corr, std::move(a)); };
        AdminDeps deps;
        deps.brokerCount = kBrokers;
        deps.topicMinIsr = 1;
        deps.respondDefine = [record](int64_t, int64_t corr, int32_t topicId, uint16_t version, uint16_t code) {
            record(corr, Answer{code, topicId, version, {}, 0, -1, 0, Answer::Kind::Define});
        };
        deps.respondAlter = [record](int64_t, int64_t corr, int32_t topicId, uint16_t version, uint16_t code) {
            record(corr, Answer{code, topicId, version, {}, 0, -1, 0, Answer::Kind::Alter});
        };
        deps.respondCreateView = [record](int64_t, int64_t corr, int32_t viewTopicId, uint64_t fp, uint16_t code) {
            record(corr, Answer{code, viewTopicId, 0, {}, 0, -1, fp, Answer::Kind::CreateView});
        };
        deps.respondDdl = [record](int64_t, int64_t corr, uint16_t code, uint64_t objectId, uint32_t newVersion, int32_t gate,
                                   std::string_view detail) {
            record(corr, Answer{code, -1, newVersion, std::string(detail), objectId, gate, 0, Answer::Kind::Ddl});
        };
        deps.respondError = [record](int64_t, int64_t corr, uint16_t code, std::string_view message) {
            record(corr, Answer{code, -1, 0, std::string(message), 0, -1, 0, Answer::Kind::Error});
        };
        deps.respondGet = [](int64_t, int64_t, int32_t, uint16_t, int32_t, const std::vector<uint8_t>&, uint16_t) {};
        br->admin = std::make_unique<AdminWorker>(br->schemas, id, std::move(deps), &br->views, br->replica.get(), nullptr);
        (void)br->admin->bootCatalog(BootEffects{effectsFor_(*br, /*live=*/false)}, LiveEffects{effectsFor_(*br, /*live=*/true)});
        // BrokerNode's boot order: the topic-dir reconcile (the production reconcileTopicDirs) settles the dirs
        // against the booted catalog — a dropped incarnation's dir is retired, a live topic's dir kept — and
        // only then is the TopicRegistry built; the live topics were posted by the coalesced boot sink, and every
        // id the catalog dropped (no live topic or view sink holds it) is tombstoned after, never refilled.
        const auto& booted = br->admin->applier().state();
        const std::vector<int32_t> droppedIds = droppedTopicIds(booted);
        std::vector<rune::node::TopicSpec> catalogTopics;
        for (const auto* t : booted.topics()) catalogTopics.push_back(specFrom_(t->name, t->topicId, t->partitions, t->persistence));
        for (const auto* v : booted.views())
            catalogTopics.push_back(specFrom_(v->name, v->def.viewTopicId, v->def.sinkPartitions, v->def.persistence));
        br->lastReconcile = reconcileTopicDirs(brokerDir.string(), catalogTopics, droppedIds, /*nowUnixSeconds=*/1);
        br->ctl.reg = std::make_unique<rune::gateway::TopicRegistry>(std::vector<rune::node::TopicSpec>{}, brokerDir.string());
        for (const int32_t dropped : droppedIds) {
            Effect e; e.kind = Effect::Kind::Drop; e.id = dropped;
            br->ctl.queue.push_back(std::move(e));
        }
        return br;
    }

    struct Submission { int op; int broker; int64_t corr; };

    std::deque<rr::AlterIsrMsg>          alterIsrQ_;   // the catalog leader's AlterIsr proposals (first: brokers hold a channel to it)
    std::string                          tag_;
    std::filesystem::path                root_;
    /// Owns the harness's directory tree: declared BEFORE brokers_, so it is destroyed AFTER them, and it also
    /// runs when the ctor body throws (the destructor of a half-built object never runs).
    struct RootGuard {
        std::filesystem::path p;
        explicit RootGuard(std::filesystem::path path) : p(std::move(path)) {}
        RootGuard(const RootGuard&)            = delete;
        RootGuard& operator=(const RootGuard&) = delete;
        ~RootGuard() { std::error_code ec; std::filesystem::remove_all(p, ec); }
    } rootGuard_{root_};
    rr::test::FilteringTransport         ft_;
    ClusterHold                          hold_;
    std::vector<std::unique_ptr<Broker>> brokers_;
    rune::metadata::PartitionAssignment  cur_;
    std::set<int32_t>                    placed_;
    bool                                 placementHeld_ = false;
    bool                                 holdRetire_ = false;   // test seam (like BrokerNode::holdDroppedDirPurgeForTest): retiring dirs stay
    int32_t                              placementLeader_ = 1;
    int64_t                              now_ = 1'000'000'000;
    int64_t                              corr_ = 0;
    uint64_t                             ridCounter_ = 0;
    std::vector<ClusterAnswer>           recs_;
    std::map<int64_t, std::vector<std::size_t>> byCorr_;
    std::vector<Submission>              subs_;
    std::vector<std::unique_ptr<ClientOp>> clients_;
    cm::Kind                             kind_ = cm::Kind::CreateTopic;
    cm::Entity                           ent_{cm::Via::Leader, cm::Persist::Disk};
    cm::Timing                           timing_ = cm::Timing::Steady;
    cm::Persist                          persist_ = cm::Persist::Disk;
    int32_t                              endpointLeader_ = -1;
};

} // namespace rune::node::test
