// CatalogStressRegressionTest.cpp — catalog history Task 21: seeded, model-checked catalog stress.
//
// 64 seeds x 400 steps over the 3-broker CatalogClusterHarness (Task 20).  Each step is a random DDL (via a
// random broker), a concurrent pair of DDLs on one name, a catalog-leader move, a broker stop+restart (also
// mid-apply), a broker isolation, a fresh broker 3 on an empty dir, a tampered fingerprint on broker 3 (it
// halts; the operator restarts it later) or idle passes.  Every DDL's terminal answer is checked against an
// INDEPENDENT catalog model (support/CatalogModel.h: it never calls rune::catalog::apply).
//
// In-doubt answers (a terminal NotLeader / Redirect / Timeout: the DDL MAY have committed) are resolved in the
// same step by the harness (CatalogClusterHarness::runDdls_, controller ruling C1): the cluster heals and the
// SAME request (same requestId) is re-sent through the leader until it gets a definitive answer - the dedupe
// returns the original result when the event committed - so the model stays sequential.
//
// Invariants: identical digest at the same applied offset on every non-halted broker (every step); the
// leader's catalog equals the model (every step); every broker serves exactly the model's names (every 20
// steps and at the end); no DDL lost or duplicated (the request ledger of every log); every started DDL
// answered; two creates on one name never both live; Internal (6) only from a halted broker.
#include "support/CatalogClusterHarness.h"
#include "support/CatalogModel.h"
#include "support/CatalogStressDriver.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <cstdlib>
#include <iostream>
#include <map>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <unistd.h>

namespace {

using rune::node::test::CatalogClusterHarness;
using rune::node::test::CatalogStressDriver;
using rune::node::test::StepResult;
using rune::node::test::StressAction;
using rune::node::test::StressAnswer;
namespace cmodel = rune::node::test::cmodel;
using ErrorCode = rune::protocol::client::ErrorCode;

constexpr int kSteps = 400;

constexpr int code(ErrorCode c) noexcept { return static_cast<int>(c); }

struct SeedReport {
    std::vector<uint64_t> traces;          // one per step
    std::map<int, int>    codes;           // terminal answer code -> count
    int                   okDdls = 0, inDoubtResolved = 0, skippedSteps = 0;
    std::size_t           events = 0;
    uint64_t              finalDigest = 0;
    double                seconds = 0;
};

/// Applies `answers` (in the order tried) to a COPY of `model`; true when every answer is what the model says.
bool matches(cmodel::CatalogModel& model, const std::vector<const StressAnswer*>& order) {
    for (const StressAnswer* a : order) {
        const cmodel::ModelAnswer m = model.accept(a->request);
        if (a->code == 0) {
            if (!m.accepted) return false;
            model.apply(m);
        } else if (m.accepted || a->code != static_cast<int>(m.refusalCode)) {
            return false;
        }
    }
    return true;
}

std::string codesOf(const std::vector<const StressAnswer*>& v) {
    std::ostringstream o;
    for (const StressAnswer* a : v) o << " [" << cmodel::toString(a->request) << " -> " << a->code << " via broker " << a->broker << "]";
    return o.str();
}

void runSeed(uint32_t seed, int steps, SeedReport& rep) {
    const auto t0 = std::chrono::steady_clock::now();
    std::mt19937 rng(seed);
    CatalogClusterHarness h("stress_" + std::to_string(seed), 3);
    CatalogStressDriver d(h);
    cmodel::CatalogModel model;
    std::set<rune::protocol::RequestId> okRequests, otherRequests;
    int step = -1;
    const bool trace = std::getenv("RUNE_STRESS_TRACE") != nullptr;
    const std::string where = "seed " + std::to_string(seed);
    try {
        for (step = 0; step < steps; ++step) {
            const std::string at = where + " step " + std::to_string(step);
            const StressAction action = d.randomAction(rng, model);
            const StepResult res = d.perform(action);
            rep.traces.push_back(res.trace);
            if (trace) {
                std::ostringstream o;
                o << "[trace] " << at << ": " << res.what << (res.skipped ? " SKIPPED" : "") << " =>";
                for (const StressAnswer& a : res.answers) o << " " << a.code << (a.resolvedInDoubt ? "*" : "") << "@" << a.broker;
                std::cerr << o.str() << "\n";
            }
            if (res.skipped) ++rep.skippedSteps;

            // ── answers vs the model ─────────────────────────────────────────────────────────────────
            std::vector<const StressAnswer*> judged;
            for (const StressAnswer& a : res.answers) {
                ++rep.codes[a.code];
                if (a.code == code(ErrorCode::Internal)) {
                    // F30: Internal only from a broker the model knows is halted (a tampered fingerprint) - never
                    // from a healthy broker (a commit timeout must be the retriable NotLeader, F13).
                    ASSERT_TRUE(a.brokerHalted) << at << ": Internal (6) from healthy broker " << a.broker << " for "
                                                << cmodel::toString(a.request) << " [" << res.what << "]";
                    otherRequests.insert(a.request.requestId);   // never processed: no event
                    continue;
                }
                if (a.code == code(ErrorCode::NotLeader) || a.code == code(ErrorCode::Redirect) ||
                    a.code == rune::node::test::kClientTimeout) {
                    ASSERT_FALSE(d.resolvesInDoubt()) << at << ": code " << a.code << " is still in doubt after the resolution: "
                                                      << cmodel::toString(a.request) << " [" << res.what << "]";
                    continue;   // unresolved (RED d only): nothing known about the DDL
                }
                judged.push_back(&a);
            }
            if (judged.size() == 1) {
                cmodel::CatalogModel next = model;
                ASSERT_TRUE(matches(next, judged))
                    << at << ": broker answer differs from the model:" << codesOf(judged) << " model says "
                    << (model.accept(judged[0]->request).accepted ? std::string("OK") : std::to_string(model.accept(judged[0]->request).refusalCode))
                    << " [" << res.what << "]" << cmodel::describe(model.snapshot());
                model = next;
            } else if (judged.size() == 2) {
                // Two DDLs on one name raced; the leader serialized them in some order.  Try both; if both fit and
                // both were accepted, the order of their events in the log decides.
                cmodel::CatalogModel ab = model, ba = model;
                const bool okAB = matches(ab, {judged[0], judged[1]});
                const bool okBA = matches(ba, {judged[1], judged[0]});
                ASSERT_TRUE(okAB || okBA) << at << ": neither order of the concurrent pair matches the model:" << codesOf(judged)
                                          << " [" << res.what << "]" << cmodel::describe(model.snapshot());
                if (okAB && okBA && judged[0]->code == 0 && judged[1]->code == 0)
                    model = judged[0]->eventOffset < judged[1]->eventOffset ? ab : ba;
                else
                    model = okAB ? ab : ba;
            } else if (judged.size() > 2) {
                FAIL() << at << ": more than two judged answers in one step";
            }
            for (const StressAnswer& a : res.answers) {
                if (a.code == 0) { okRequests.insert(a.request.requestId); ++rep.okDdls; }
                else otherRequests.insert(a.request.requestId);
            }

            // ── invariants after the step ────────────────────────────────────────────────────────────
            ASSERT_TRUE(d.digestsEqualAtEqualOffsets()) << at << ": two brokers at one applied offset hold different digests [" << res.what << "]";
            const cmodel::Snapshot got = d.catalogAsModelSnapshot();
            model.calibrateTopicIds(got);   // the first topic id is not spec'd: learned once from the first shared object
            ASSERT_TRUE(got == model.snapshot()) << at << ": the leader's catalog differs from the model [" << res.what << "]\nbroker:"
                                                 << cmodel::describe(got) << "\nmodel:" << cmodel::describe(model.snapshot());
            if (step % 20 == 19) {
                const auto diff = d.servingDiffersFromModel(model.snapshot());
                ASSERT_FALSE(diff.has_value()) << at << ": " << diff.value_or("") << " [" << res.what << "]";
            }
        }
        step = steps;
        d.healAll();
        ASSERT_TRUE(h.awaitDigestsEqual(/*excludeHalted=*/false)) << where << ": the brokers never converged after healing";
        // Invariant: every DDL answered.
        for (int b = 1; b <= 3; ++b)
            EXPECT_EQ(h.startedTotal(b), h.answeredTotal(b)) << where << ": broker " << b << " started DDLs it never answered";
        // Invariant: no DDL lost or duplicated - the ledger of every log agrees, an OK'd request has exactly one
        // event, a refused one none, and the log holds nothing else.
        const auto ledger = d.requestLedger();
        ASSERT_TRUE(ledger.has_value()) << where << ": the brokers' logs hold different events";
        for (const auto& rid : okRequests) {
            const auto it = ledger->find(rid);
            EXPECT_TRUE(it != ledger->end() && it->second == 1u)
                << where << ": OK'd request " << rid.lo << " has " << (it == ledger->end() ? 0 : it->second) << " events";
        }
        for (const auto& rid : otherRequests)
            if (!okRequests.contains(rid))
                EXPECT_EQ(ledger->count(rid), 0u) << where << ": a refused request " << rid.lo << " left an event in the log";
        EXPECT_EQ(d.leaderEventCount(), okRequests.size()) << where << ": the log holds events no OK'd request explains";
        // Invariant: two creates on one name never both live.
        std::string why;
        EXPECT_TRUE(d.noNameHadTwoConcurrentLiveObjects(&why)) << where << ": " << why;
        // The broker catalog equals the independent model, on the leader and on what every broker serves.
        const cmodel::Snapshot got = d.catalogAsModelSnapshot();
        model.calibrateTopicIds(got);
        EXPECT_TRUE(got == model.snapshot()) << where << ": final catalog differs from the model\nbroker:" << cmodel::describe(got)
                                             << "\nmodel:" << cmodel::describe(model.snapshot());
        const auto diff = d.servingDiffersFromModel(model.snapshot());
        EXPECT_FALSE(diff.has_value()) << where << ": " << diff.value_or("");
        rep.events = d.leaderEventCount();
        rep.inDoubtResolved = d.inDoubtResolved();
        rep.finalDigest = h.stateOf(h.leader()).digest();
    } catch (const std::exception& e) {
        FAIL() << where << " step " << step << ": " << e.what();
    }
    rep.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    if (std::getenv("RUNE_STRESS_STATS") != nullptr) {
        std::ostringstream o;
        o << "[stress] seed " << seed << " steps " << steps << " events " << rep.events << " okDdls " << rep.okDdls << " inDoubt "
          << rep.inDoubtResolved << " skipped " << rep.skippedSteps << " secs " << rep.seconds << " codes";
        for (const auto& [c, n] : rep.codes) o << " " << c << "x" << n;
        std::cerr << o.str() << "\n";
    }
}

/// I5 leak check: after the whole binary ran, no rune_cluster_<pid>_* tree of this process remains.  The harness
/// owns its tree through RAII, so even a seed that failed an ASSERT (unwinding through runSeed) must leave none.
class NoLeakedClusterTreesEnvironment : public testing::Environment {
public:
    void TearDown() override {
        const std::string prefix = "rune_cluster_" + std::to_string(::getpid()) + "_";
        for (const auto& e : std::filesystem::directory_iterator(std::filesystem::temp_directory_path()))
            if (e.path().filename().string().rfind(prefix, 0) == 0)
                ADD_FAILURE() << "a CatalogClusterHarness left its tree behind: " << e.path();
    }
};
[[maybe_unused]] testing::Environment* const kLeakCheck = testing::AddGlobalTestEnvironment(new NoLeakedClusterTreesEnvironment);

} // namespace

// 64 seeds x 400 steps.  The client model follows Redirect and retries NotLeader with the budget (the harness's
// ClientOp), the leader moves, restarts, isolations, a fresh broker 3 and a tampered fingerprint are the faults.
class CatalogStressRegression : public testing::TestWithParam<uint32_t> {};

TEST_P(CatalogStressRegression, RandomDdlUnderChurnMatchesTheModel) {
    SeedReport rep;
    runSeed(GetParam(), kSteps, rep);
    RecordProperty("seconds", std::to_string(rep.seconds));
    RecordProperty("events", std::to_string(rep.events));
    RecordProperty("inDoubtResolved", std::to_string(rep.inDoubtResolved));
}

INSTANTIATE_TEST_SUITE_P(Seeds, CatalogStressRegression, testing::Range(1u, 65u),
                         [](const testing::TestParamInfo<uint32_t>& info) { return "Seed" + std::to_string(info.param); });

// C5: the same seed is the same run - the action sequence, every answer, the leader and the final digest.
TEST(CatalogStressRegression, SameSeedSameTrace) {
    SeedReport a, b;
    runSeed(7, 150, a);
    runSeed(7, 150, b);
    ASSERT_EQ(a.traces.size(), b.traces.size());
    for (std::size_t i = 0; i < a.traces.size(); ++i)
        ASSERT_EQ(a.traces[i], b.traces[i]) << "seed 7 diverged from itself at step " << i;
    EXPECT_EQ(a.finalDigest, b.finalDigest);
    EXPECT_EQ(a.codes, b.codes);
    EXPECT_GT(a.okDdls, 0);
}

// Fix-on-sight in the harness's fake control side: the stress (seed 2, step 99 of the final weights) found that a create whose name differs only
// in ASCII case from a dropped topic still retiring ('B0' behind the dropped 'b0') ran at once, as the harness
// compared names exactly; BrokerNode::catalogNameHeldByDrop_ folds (Task 8 fix round 1, M4) because on a
// case-insensitive filesystem the two share one directory and the registry refuses to open it as another
// incarnation - the topic never served on that broker.  The harness now mirrors the folded rule; this pins the
// seed and the step that exposed it (the first 100 steps of seed 2; 30 of the 64 seeds failed before the fix).
TEST(CatalogStressRegression, Seed2CaseVariantCreateBehindADropServesRegression) {
    SeedReport rep;
    runSeed(2, 100, rep);
    EXPECT_GT(rep.okDdls, 0);
}
